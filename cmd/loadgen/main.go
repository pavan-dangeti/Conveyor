// loadgen measures Conveyor's throughput and enqueue-to-start latency for
// one configuration, repeated -reps times, and appends the result to a JSON
// file. scripts/bench.sh drives the full sweep.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/bench"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/pavan-dangeti/Conveyor/worker"
)

const producers = 4

func main() {
	dsn := flag.String("dsn", "postgres://conveyor:conveyor@127.0.0.1:55433/conveyor?sslmode=disable", "Postgres DSN")
	name := flag.String("name", "", "config name in the results file (default derived from flags)")
	out := flag.String("out", "", "results file to append to (default: print the summary only)")
	jobs := flag.Int("jobs", 2000, "jobs per rep")
	reps := flag.Int("reps", 5, "repetitions")
	workers := flag.Int("workers", 4, "worker loops")
	batch := flag.Int("batch", 8, "concurrent jobs per worker loop")
	payload := flag.Int("payload", 128, "bytes of padding in each job's args")
	debounce := flag.Duration("notify-debounce", 10*time.Millisecond, "minimum gap between pg_notify wake-ups; 0 = one per enqueue")
	batched := flag.Bool("batched-complete", true, "group completions into one fenced UPDATE per worker")
	flag.Parse()
	if *name == "" {
		*name = fmt.Sprintf("conveyor w=%d b=%d payload=%d", *workers, *batch, *payload)
	}
	queue.SetNotifyDebounce(*debounce)

	ctx := context.Background()
	db, err := store.Open(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(*workers*2 + producers + 4)
	if err := migrate.Up(ctx, db); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO queues (name, concurrency) VALUES ('bench', 100000)
ON CONFLICT (name) DO UPDATE SET concurrency = 100000, rate_limit_per_sec = NULL`); err != nil {
		log.Fatal(err)
	}

	cfg := bench.Config{Name: *name, System: "loadgen", Params: map[string]any{
		"jobs": *jobs, "workers": *workers, "batch": *batch, "payload_bytes": *payload,
		"notify_debounce": debounce.String(), "batched_complete": *batched, "producers": producers,
	}}
	padding := strings.Repeat("x", *payload)
	for r := range *reps {
		if _, err := db.Exec(`DELETE FROM jobs WHERE queue = 'bench'`); err != nil {
			log.Fatal(err)
		}
		rep := runRep(ctx, db, *dsn, *jobs, *workers, *batch, padding, *batched)
		log.Printf("%s rep %d: %.0f jobs/s p50=%.1fms p95=%.1fms p99=%.1fms errors=%d",
			*name, r+1, rep.Throughput, rep.P50Ms, rep.P95Ms, rep.P99Ms, rep.Errors)
		cfg.Reps = append(cfg.Reps, rep)
	}
	if *out == "" {
		b, _ := json.MarshalIndent(bench.Summarise(cfg.Reps), "", "  ")
		fmt.Println(string(b))
		return
	}
	if err := bench.Append(*out, cfg, db); err != nil {
		log.Fatal(err)
	}
}

func runRep(ctx context.Context, db *sql.DB, dsn string, jobs, workers, batch int, padding string, batched bool) bench.Rep {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lat bench.Latencies
	var started, errs atomic.Int64
	done := make(chan struct{})
	handler := func(_ context.Context, j queue.Job) error {
		var a struct {
			At int64 `json:"enqueued_unix_us"`
		}
		if err := json.Unmarshal(j.Args, &a); err != nil {
			return err
		}
		lat.Add(time.Since(time.UnixMicro(a.At)))
		if started.Add(1) == int64(jobs) {
			close(done)
		}
		return nil
	}
	var wg sync.WaitGroup
	for i := range workers {
		opts := []worker.Option{worker.WithBatch(batch), worker.WithLease(30 * time.Second), worker.WithBudget(5 * time.Second)}
		if !batched {
			opts = append(opts, worker.WithoutBatchedCompletion())
		}
		w := worker.New(db, dsn, "bench", fmt.Sprintf("loadgen-%d", i), opts...)
		w.Handle("noop", handler)
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(ctx) }()
	}
	time.Sleep(300 * time.Millisecond) // let LISTEN connections come up before the first enqueue

	waitCtx, stopWaits := context.WithCancel(ctx)
	waitsCh := make(chan map[string]int, 1)
	go func() { waitsCh <- bench.SampleWaits(waitCtx, db) }()

	start := time.Now()
	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := p; i < jobs; i += producers {
				args := fmt.Sprintf(`{"enqueued_unix_us":%d,"pad":%q}`, time.Now().UnixMicro(), padding)
				if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "bench", Kind: "noop", Args: json.RawMessage(args)}); err != nil {
					errs.Add(1)
				}
			}
		}()
	}
	pwg.Wait()
	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		log.Printf("timeout: %d of %d jobs started", started.Load(), jobs)
	}
	elapsed := time.Since(start)
	stopWaits()
	waits := <-waitsCh
	cancel()
	wg.Wait()
	return lat.Finish(started.Load(), elapsed, errs.Load(), waits)
}
