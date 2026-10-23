// harness-worker executes "work" jobs and applies each job's effect exactly
// once: the ledger row and the completion commit in one transaction guarded
// by the worker's fencing token, so a zombie that lost its lease commits
// neither. It is the worker process used by the fault-injection harness and
// the live demo.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/pavan-dangeti/Conveyor/internal/telemetry"
	"github.com/pavan-dangeti/Conveyor/worker"
)

type workArgs struct {
	MS       int     `json:"ms"`
	FailRate float64 `json:"fail_rate"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL required")
		os.Exit(2)
	}
	owner := env("OWNER", fmt.Sprintf("worker-%d", os.Getpid()))
	lease := duration("LEASE", 2*time.Second)
	// HEARTBEAT_INTERVAL > LEASE simulates a worker whose clock runs slow:
	// its leases lapse mid-job and the fencing token must reject it.
	heartbeat := duration("HEARTBEAT_INTERVAL", lease/3)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdown, err := telemetry.Setup(ctx, "conveyor-worker")
	if err != nil {
		logger.Error("telemetry", "err", err)
		os.Exit(1)
	}
	defer func() { _ = shutdown(context.Background()) }()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		logger.Error("open store", "err", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	if addr := os.Getenv("METRICS_ADDR"); addr != "" {
		srv := &http.Server{Addr: addr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { logger.Info("metrics listening", "addr", addr, "err", srv.ListenAndServe()) }()
	}

	w := worker.New(db, dsn, env("QUEUE", "q1"), owner,
		worker.WithLease(lease), worker.WithHeartbeatInterval(heartbeat), worker.WithBatch(2),
		worker.WithLogger(logger), worker.WithHandlerCommitsOutcome())
	w.Handle("work", func(ctx context.Context, j worker.Job) error {
		a := workArgs{MS: 100}
		if err := json.Unmarshal(j.Args, &a); err != nil {
			return fmt.Errorf("args: %w", err)
		}
		select {
		case <-time.After(time.Duration(a.MS) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		if rand.Float64() < a.FailRate {
			return errors.New("simulated handler failure")
		}
		return applyEffect(ctx, db, w, j)
	})
	if err := w.Run(ctx); err != nil {
		logger.Error("worker", "err", err)
		os.Exit(1)
	}
}

// applyEffect has no ON CONFLICT on the ledger insert on purpose: a second
// effect for the same job must fail loudly, not be silently absorbed.
func applyEffect(ctx context.Context, db *sql.DB, w *worker.Worker, j worker.Job) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ledger (job_id, effect_id, fencing_token) VALUES ($1, 'effect', $2)`,
		j.ID, j.LeaseVersion); err != nil {
		return err
	}
	if err := w.CompleteTx(ctx, tx, j); err != nil {
		return err
	}
	return tx.Commit()
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func duration(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil && d > 0 {
		return d
	}
	return def
}
