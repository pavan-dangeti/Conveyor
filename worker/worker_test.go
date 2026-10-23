package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

func setup(t *testing.T) (*sql.DB, string) { return pgtest.Start(t) }

func TestWorkerProcessesJobs(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{
			Queue: "q1", Kind: "echo", Args: json.RawMessage(`{"n":1}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	var done atomic.Int32
	w := New(db, dsn, "q1", "worker-1", WithLease(2*time.Second), WithBatch(2))
	w.Handle("echo", func(ctx context.Context, j queue.Job) error {
		done.Add(1)
		return nil
	})
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	go func() { _ = w.Run(wctx) }()
	deadline := time.After(15 * time.Second)
	for done.Load() < 5 {
		select {
		case <-deadline:
			t.Fatalf("only %d jobs completed", done.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	var completed int
	for i := 0; i < 60; i++ {
		if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE state='completed'`).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 5 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("want 5 completed, got %d", completed)
}

func TestFailingJobGoesRetryableThenDiscarded(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{
		Queue: "q1", Kind: "boom", Args: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	w := New(db, dsn, "q1", "worker-1", WithLease(2*time.Second))
	w.Handle("boom", func(ctx context.Context, j queue.Job) error {
		return fmt.Errorf("always fails")
	})
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	go func() { _ = w.Run(wctx) }()
	deadline := time.After(20 * time.Second)
	for {
		var state string
		if err := db.QueryRow(`SELECT state FROM jobs WHERE id=$1`, id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "discarded" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("state=%s, want discarded", state)
		case <-time.After(200 * time.Millisecond):
		}
		if state == "retryable" {
			// retryable jobs need promotion; simulate maintenance tick
			if _, err := queue.PromoteDue(ctx, db); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease := 300 * time.Millisecond
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{
		Queue: "q1", Kind: "slow", Args: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	w := New(db, dsn, "q1", "worker-1", WithLease(lease), WithBatch(1))
	w.Handle("slow", func(ctx context.Context, j queue.Job) error {
		<-time.After(1 * time.Second)
		return nil
	})
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	go func() { _ = w.Run(wctx) }()

	var state string
	deadline := time.After(15 * time.Second)
	for {
		if err := db.QueryRow(`SELECT state FROM jobs WHERE queue='q1'`).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "completed" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("lease was reaped while the handler was running, state=%s", state)
		case <-time.After(250 * time.Millisecond):
			if n, err := queue.ReapExpiredLeases(ctx, db); err != nil {
				t.Fatal(err)
			} else if n > 0 {
				t.Fatalf("lease expired after %dms", lease)
			}
		}
	}
}

func TestLeaseExpiryRecoveryByAnotherWorker(t *testing.T) {
	db, dsn := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease := 300 * time.Millisecond
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{
		Queue: "q1", Kind: "slow", Args: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	w1 := New(db, dsn, "q1", "worker-1", WithLease(lease), WithBatch(1))
	w1.Handle("slow", func(ctx context.Context, j queue.Job) error {
		<-held
		return nil
	})
	w1ctx, w1cancel := context.WithCancel(ctx)
	defer w1cancel()
	go func() { _ = w1.Run(w1ctx) }()

	waitForState(t, db, "running")
	// Simulate a crashed worker whose lease is already gone. Its fencing token
	// is stale forever, exactly as after a kill -9.
	if _, err := db.Exec(`UPDATE jobs SET lease_expiry = now() - interval '1 second' WHERE state='running'`); err != nil {
		t.Fatal(err)
	}
	if n, err := queue.ReapExpiredLeases(ctx, db); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("want 1 reaped, got %d", n)
	}
	// The reaper schedules the retry 1 s out; skip the backoff.
	if _, err := db.Exec(`UPDATE jobs SET run_at = now() WHERE state = 'retryable'`); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PromoteDue(ctx, db); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM jobs WHERE queue='q1'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "available" {
		t.Fatalf("want available after promotion, got %s", state)
	}

	w2 := New(db, dsn, "q1", "worker-2", WithLease(lease), WithBatch(1))
	w2.Handle("slow", func(ctx context.Context, j queue.Job) error {
		return nil
	})
	w2ctx, w2cancel := context.WithCancel(ctx)
	defer w2cancel()
	go func() { _ = w2.Run(w2ctx) }()

	deadline := time.After(15 * time.Second)
	for {
		if err := db.QueryRow(`SELECT state FROM jobs WHERE queue='q1'`).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "completed" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("no worker completed the recovered job, state=%s", state)
		case <-time.After(100 * time.Millisecond):
		}
	}
	close(held)
}

func waitForState(t *testing.T, db *sql.DB, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		if err := db.QueryRow(`SELECT state FROM jobs WHERE queue='q1'`).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job never reached %s", want)
}
