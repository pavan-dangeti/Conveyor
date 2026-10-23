// riverbench runs loadgen's workload against River on the same database
// server, timed and summarised by the same internal/bench code, so the two
// systems are directly comparable.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/pavan-dangeti/Conveyor/internal/bench"
)

const producers = 4

type noopArgs struct {
	EnqueuedUS int64  `json:"enqueued_unix_us"`
	Pad        string `json:"pad"`
}

func (noopArgs) Kind() string { return "noop" }

type noopWorker struct {
	river.WorkerDefaults[noopArgs]
	lat     *bench.Latencies
	started *atomic.Int64
	jobs    int64
	done    chan struct{}
}

func (w *noopWorker) Work(_ context.Context, job *river.Job[noopArgs]) error {
	w.lat.Add(time.Since(time.UnixMicro(job.Args.EnqueuedUS)))
	if w.started.Add(1) == w.jobs {
		close(w.done)
	}
	return nil
}

func main() {
	dsn := flag.String("dsn", "postgres://conveyor:conveyor@127.0.0.1:55433/river?sslmode=disable", "Postgres DSN (a separate database from Conveyor's)")
	name := flag.String("name", "", "config name in the results file")
	out := flag.String("out", "", "results file to append to (default: print the summary only)")
	jobs := flag.Int("jobs", 2000, "jobs per rep")
	reps := flag.Int("reps", 5, "repetitions")
	maxWorkers := flag.Int("max-workers", 32, "River MaxWorkers (match loadgen workers*batch)")
	payload := flag.Int("payload", 128, "bytes of padding in each job's args")
	cooldown := flag.Duration("fetch-cooldown", 100*time.Millisecond, "River FetchCooldown (River's default is 100ms)")
	flag.Parse()
	if *name == "" {
		*name = fmt.Sprintf("river max_workers=%d fetch_cooldown=%s", *maxWorkers, *cooldown)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	driver := riverpgxv5.New(pool)
	migrator, err := rivermigrate.New(driver, nil)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		log.Fatal(err)
	}
	sqlDB, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sqlDB.Close() }()

	cfg := bench.Config{Name: *name, System: "riverbench", Params: map[string]any{
		"jobs": *jobs, "max_workers": *maxWorkers, "payload_bytes": *payload,
		"fetch_cooldown": cooldown.String(), "producers": producers, "river": "v0.49.0",
	}}
	padding := strings.Repeat("x", *payload)
	for r := range *reps {
		if _, err := pool.Exec(ctx, `DELETE FROM river_job`); err != nil {
			log.Fatal(err)
		}
		rep, err := runRep(ctx, driver, sqlDB, *jobs, *maxWorkers, *cooldown, padding)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s rep %d: %.0f jobs/s p50=%.1fms p95=%.1fms p99=%.1fms errors=%d",
			*name, r+1, rep.Throughput, rep.P50Ms, rep.P95Ms, rep.P99Ms, rep.Errors)
		cfg.Reps = append(cfg.Reps, rep)
	}
	if *out == "" {
		fmt.Printf("%+v\n", bench.Summarise(cfg.Reps))
		return
	}
	if err := bench.Append(*out, cfg, sqlDB); err != nil {
		log.Fatal(err)
	}
}

func runRep(ctx context.Context, driver *riverpgxv5.Driver, sqlDB *sql.DB, jobs, maxWorkers int, cooldown time.Duration, padding string) (bench.Rep, error) {
	var lat bench.Latencies
	var started, errs atomic.Int64
	done := make(chan struct{})
	workers := river.NewWorkers()
	river.AddWorker(workers, &noopWorker{lat: &lat, started: &started, jobs: int64(jobs), done: done})
	client, err := river.NewClient(driver, &river.Config{
		Queues:        map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: maxWorkers}},
		Workers:       workers,
		FetchCooldown: cooldown,
	})
	if err != nil {
		return bench.Rep{}, err
	}
	if err := client.Start(ctx); err != nil {
		return bench.Rep{}, err
	}
	time.Sleep(300 * time.Millisecond)

	waitCtx, stopWaits := context.WithCancel(ctx)
	waitsCh := make(chan map[string]int, 1)
	go func() { waitsCh <- bench.SampleWaits(waitCtx, sqlDB) }()

	start := time.Now()
	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := p; i < jobs; i += producers {
				if _, err := client.Insert(ctx, noopArgs{EnqueuedUS: time.Now().UnixMicro(), Pad: padding}, nil); err != nil {
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
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = client.Stop(stopCtx)
	return lat.Finish(started.Load(), elapsed, errs.Load(), waits), nil
}
