// riverbench runs the same workload as cmd/loadgen against River on the same
// hardware/database, so the numbers are directly comparable.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

type NoopArgs struct {
	Enqueued int64 `json:"enqueued_unix_ms"`
}

func (NoopArgs) Kind() string { return "noop" }

type NoopWorker struct {
	river.WorkerDefaults[NoopArgs]
	latencies *[]float64
	mu        *sync.Mutex
	done      *atomic.Int64
}

func (w *NoopWorker) Work(ctx context.Context, job *river.Job[NoopArgs]) error {
	w.mu.Lock()
	*w.latencies = append(*w.latencies, float64(time.Since(time.UnixMilli(job.Args.Enqueued)).Milliseconds()))
	w.mu.Unlock()
	w.done.Add(1)
	return nil
}

type benchResult struct {
	Name         string  `json:"name"`
	MaxWorkers   int     `json:"max_workers"`
	PayloadBytes int     `json:"payload_bytes"`
	Jobs         int     `json:"jobs"`
	Throughput   float64 `json:"throughput_jobs_per_sec"`
	P50Ms        float64 `json:"enqueue_to_start_p50_ms"`
	P95Ms        float64 `json:"enqueue_to_start_p95_ms"`
	P99Ms        float64 `json:"enqueue_to_start_p99_ms"`
}

func main() {
	dsn := env("DATABASE_URL", "postgres://conveyor:conveyor@127.0.0.1:55433/river?sslmode=disable")
	jobs := envInt("JOBS", 500)
	maxWorkers := envInt("WORKERSU", 32)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fatal(err)
	}
	defer pool.Close()
	driver := riverpgxv5.New(pool)
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{})
	if err != nil {
		fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		fatal(err)
	}
	var latencies []float64
	var mu sync.Mutex
	var done atomic.Int64
	workers := river.NewWorkers()
	river.AddWorker(workers, &NoopWorker{latencies: &latencies, mu: &mu, done: &done})
	client, err := river.NewClient(driver, &river.Config{
		Queues:        map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: maxWorkers}},
		Workers:       workers,
		FetchCooldown: 1 * time.Millisecond,
	})
	if err != nil {
		fatal(err)
	}
	go func() { _ = client.Start(ctx) }()
	start := time.Now()
	enqStart := time.Now()
	var errCount atomic.Int64
	for i := 0; i < jobs; i++ {
		if _, err := client.Insert(ctx, NoopArgs{Enqueued: time.Now().UnixMilli()}, &river.InsertOpts{}); err != nil {
			errCount.Add(1)
		}
	}
	_ = enqStart
	for done.Load() < int64(jobs) && time.Since(start) < 8*time.Minute {
		time.Sleep(200 * time.Millisecond)
	}
	elapsed := time.Since(start).Seconds()
	res := benchResult{Name: "river", MaxWorkers: maxWorkers, Jobs: jobs}
	res.Throughput = float64(done.Load()) / elapsed
	sort.Float64s(latencies)
	if len(latencies) > 0 {
		res.P50Ms = latencies[len(latencies)/2]
		res.P95Ms = latencies[len(latencies)*95/100]
		res.P99Ms = latencies[len(latencies)*99/100]
	}
	_ = client.Stop(ctx)
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
	fmt.Fprintln(os.Stderr, "enqueue errors:", errCount.Load())
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
