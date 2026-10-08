// loadgen enqueues N jobs and executes them with an in-process worker fleet,
// recording throughput and enqueue-to-start latency percentiles.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/pavan-dangeti/Conveyor/internal/worker"
)

type benchResult struct {
	Workers      int     `json:"workers"`
	Batch        int     `json:"batch"`
	PayloadBytes int     `json:"payload_bytes"`
	Jobs         int     `json:"jobs"`
	Throughput   float64 `json:"throughput_jobs_per_sec"`
	P50Ms        float64 `json:"enqueue_to_start_p50_ms"`
	P95Ms        float64 `json:"enqueue_to_start_p95_ms"`
	P99Ms        float64 `json:"enqueue_to_start_p99_ms"`
	Errors       int     `json:"errors"`
}

func main() {
	dsn := env("DATABASE_URL", "postgres://conveyor:conveyor@127.0.0.1:55433/conveyor?sslmode=disable")
	jobs := envInt("JOBS", 200)
	workersN := envInt("WORKERS", 4)
	batch := envInt("BATCH", 4)
	payload := envInt("PAYLOAD_BYTES", 128)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := migrate.Up(ctx, db); err != nil {
		fatal(err)
	}
	_, _ = db.Exec(`INSERT INTO queues(name) VALUES ('bench') ON CONFLICT DO NOTHING`)
	_, _ = db.Exec(`DELETE FROM jobs WHERE queue='bench'`)

	argsTemplate := `{"enqueued_unix_ms":%d,"data":"%s"}`
	var latencies []float64
	var mu sync.Mutex
	var completed atomic.Int64
	var errors atomic.Int64

	handler := func(ctx context.Context, j queue.Job) error {
		var a struct {
			Enqueued int64 `json:"enqueued_unix_ms"`
		}
		if err := json.Unmarshal(j.Args, &a); err != nil {
			return err
		}
		mu.Lock()
		latencies = append(latencies, float64(time.Since(time.UnixMilli(a.Enqueued)).Milliseconds()))
		mu.Unlock()
		completed.Add(1)
		return nil
	}

	data := string(make([]byte, payload))
	for i := range data {
		data = data[:i] + "x" + data[i+1:]
	}
	start := time.Now()
	enqDone := make(chan struct{})
	go func() {
		for i := 0; i < jobs; i++ {
			args := fmt.Sprintf(argsTemplate, time.Now().UnixMilli(), data)
			if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "bench", Kind: "noop", Args: json.RawMessage(args)}); err != nil {
				errors.Add(1)
			}
		}
		close(enqDone)
	}()
	var wg sync.WaitGroup
	for i := 0; i < workersN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := worker.New(db, dsn, "bench", fmt.Sprintf("loadgen-%d", i), worker.WithLease(5*time.Second), worker.WithBatch(batch))
			w.Handle("noop", handler)
			wctx, cancel := context.WithCancel(ctx)
			defer cancel()
			_ = w.Run(wctx)
		}(i)
		time.Sleep(100 * time.Millisecond)
	}
	go func() {
		for completed.Load() < int64(jobs) && time.Since(start) < 8*time.Minute {
			time.Sleep(200 * time.Millisecond)
		}
		cancel()
	}()
	<-enqDone
	for completed.Load() < int64(jobs) && err == nil {
		time.Sleep(200 * time.Millisecond)
		if time.Since(start) > 8*time.Minute {
			break
		}
	}
	wg.Wait()
	res := benchResult{Workers: workersN, Batch: batch, PayloadBytes: payload, Jobs: jobs, Errors: int(errors.Load())}
	elapsed := time.Since(start).Seconds()
	res.Throughput = float64(completed.Load()) / elapsed
	sort.Float64s(latencies)
	if len(latencies) > 0 {
		res.P50Ms = latencies[len(latencies)/2]
		res.P95Ms = latencies[len(latencies)*95/100]
		res.P99Ms = latencies[len(latencies)*99/100]
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		return n
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

var _ = io.Discard
var _ = slog.Default()
