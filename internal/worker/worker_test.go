package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setup(t *testing.T) (*sql.DB, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	c, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("conveyor"),
		postgres.WithUsername("conveyor"),
		postgres.WithPassword("conveyor"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO queues(name) VALUES ('q1')`); err != nil {
		t.Fatal(err)
	}
	return db, dsn
}

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
