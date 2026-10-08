// harness-worker executes "work" jobs and applies each effect exactly once:
// the ledger row and the completion share one transaction guarded by the
// worker's (owner, fencing token) lease. A zombie worker that lost its
// lease cannot commit.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/pavan-dangeti/Conveyor/internal/worker"
)

var done atomic.Int64

func main() {
	dsn := env("DATABASE_URL", "")
	queueName := env("QUEUE", "q1")
	owner := env("OWNER", fmt.Sprintf("worker-%d", os.Getpid()))
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL required")
		os.Exit(2)
	}
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	w := worker.New(db, dsn, queueName, owner,
		worker.WithLease(2*time.Second),
		worker.WithBatch(2),
		worker.WithHandlerCommitsOutcome(),
	)
	w.Handle("work", func(ctx context.Context, j queue.Job) error {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		return applyEffect(db, j, owner)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done.Add(1)
	_ = w.Run(ctx)
}

func applyEffect(db *sql.DB, j queue.Job, owner string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ledger (job_id, effect_id, fencing_token) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		j.ID, "effect", j.LeaseVersion); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET state='completed', lease_expiry=NULL, updated_at=now()
		 WHERE id=$1 AND state='running' AND lease_owner=$2 AND lease_version=$3`,
		j.ID, owner, j.LeaseVersion)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("stale lease")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	done.Add(1)
	_ = json.Valid // keep encoding/json import meaningful for future payloads
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
