package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

func TestRunRequiresHandlers(t *testing.T) {
	if err := New(nil, "", "q1", "w").Run(context.Background()); err == nil {
		t.Fatal("want an error with no handlers")
	}
}

func TestUnknownKindIsFailedNotDropped(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id, _, _ := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "mystery", Args: json.RawMessage(`{}`), MaxAttempts: 1})
	w := New(db, dsn, "q1", "w1", WithLease(2*time.Second))
	w.Handle("known", func(context.Context, queue.Job) error { return nil })
	go func() { _ = w.Run(ctx) }()
	pgtest.Eventually(t, "unknown kind dead-lettered", func() bool { return pgtest.State(t, db, id) == "discarded" })
	var msg string
	_ = db.QueryRow(`SELECT error FROM jobs WHERE id = $1`, id).Scan(&msg)
	if !strings.Contains(msg, "no handler") {
		t.Fatalf("error: %q", msg)
	}
}

func TestUnbatchedCompletionAndGracefulShutdown(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	for range 3 {
		if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "echo", Args: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	release := make(chan struct{})
	w := New(db, dsn, "q1", "w1", WithLease(2*time.Second), WithBatch(3), WithoutBatchedCompletion(), WithBudget(5*time.Second))
	w.Handle("echo", func(context.Context, queue.Job) error {
		<-release
		return nil
	})
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()
	pgtest.Eventually(t, "three jobs running", func() bool { return count(t, db, "running") == 3 })
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "completed"); n != 3 {
		t.Fatalf("in-flight jobs must finish within the shutdown budget, %d completed", n)
	}
}

// Cancelling a running job is noticed on the next heartbeat: the handler's
// context ends and the job stays cancelled instead of completing.
func TestCancelStopsRunningHandler(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id, _, _ := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "slow", Args: json.RawMessage(`{}`)})
	stopped := make(chan struct{})
	w := New(db, dsn, "q1", "w1", WithLease(300*time.Millisecond))
	w.Handle("slow", func(ctx context.Context, _ queue.Job) error {
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	go func() { _ = w.Run(ctx) }()
	pgtest.Eventually(t, "job running", func() bool { return pgtest.State(t, db, id) == "running" })
	if err := queue.Cancel(ctx, db, id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was not cancelled")
	}
	if s := pgtest.State(t, db, id); s != "cancelled" {
		t.Fatalf("want cancelled, got %s", s)
	}
}

func count(t *testing.T, db *sql.DB, state string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE state = $1`, state).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The exactly-once path: a handler commits its effect and the completion in
// one transaction. When the fencing token is stale, neither commits.
func TestCompleteTxCommitsEffectAndCompletionTogether(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fresh, _, _ := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "pay", Args: json.RawMessage(`{}`)})
	stale, _, _ := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "pay", Args: json.RawMessage(`{}`)})
	w := New(db, dsn, "q1", "w1", WithLease(time.Minute), WithHandlerCommitsOutcome())
	w.Handle("pay", func(ctx context.Context, j Job) error {
		if j.ID == stale {
			j.LeaseVersion-- // as if another worker had since claimed the job
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (job_id, effect_id, fencing_token) VALUES ($1, 'charge', $2)`, j.ID, j.LeaseVersion); err != nil {
			return err
		}
		if err := w.CompleteTx(ctx, tx, j); err != nil {
			return err
		}
		return tx.Commit()
	})
	go func() { _ = w.Run(ctx) }()
	pgtest.Eventually(t, "fresh job completed", func() bool { return pgtest.State(t, db, fresh) == "completed" })
	pgtest.Eventually(t, "stale attempt rejected", func() bool { return pgtest.State(t, db, stale) == "retryable" })
	var effects int
	_ = db.QueryRow(`SELECT count(*) FROM ledger WHERE job_id = $1`, stale).Scan(&effects)
	if effects != 0 {
		t.Fatalf("a fenced-out attempt committed %d effects", effects)
	}
}
