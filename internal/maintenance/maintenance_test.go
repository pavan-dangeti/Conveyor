package maintenance_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/maintenance"
	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

func TestLeaderPromotesAndReaps(t *testing.T) {
	ctx := context.Background()
	db, _ := pgtest.Start(t)
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{
		Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), Delay: 200 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{
		Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Dequeue(ctx, db, "q1", "w1", 1*time.Second, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mctx, mcancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer mcancel()
	_ = maintenance.RunElectedLoop(mctx, db, logger, 300*time.Millisecond)

	var running, available int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE state='running'`).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE state='available'`).Scan(&available); err != nil {
		t.Fatal(err)
	}
	if running != 0 {
		t.Fatalf("want 0 running (expired lease reaped), got %d", running)
	}
	if available != 2 {
		t.Fatalf("want 2 available (delayed promoted, reaped re-promoted), got %d", available)
	}
}

// TestLeaderFailover runs two candidates: only one leads at a time, and when
// the leader's context ends (as when its process dies and its session's
// advisory lock is released) the standby takes over and keeps reaping.
func TestLeaderFailover(t *testing.T) {
	db, _ := pgtest.Start(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneA := make(chan struct{})
	go func() { _ = maintenance.RunElectedLoop(ctxA, db, logger, 100*time.Millisecond); close(doneA) }()
	pgtest.Eventually(t, "first leader", func() bool { return leaders(t, db) == 1 })
	go func() { _ = maintenance.RunElectedLoop(ctxB, db, logger, 100*time.Millisecond) }()
	time.Sleep(500 * time.Millisecond)
	if n := leaders(t, db); n != 1 {
		t.Fatalf("want exactly one leader, got %d", n)
	}
	stopA()
	<-doneA

	id, _, err := queue.Enqueue(context.Background(), db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Dequeue(context.Background(), db, "q1", "w", time.Millisecond, 1); err != nil {
		t.Fatal(err)
	}
	pgtest.Eventually(t, "standby to reap the expired lease", func() bool { return pgtest.State(t, db, id) != "running" })
	if n := leaders(t, db); n != 1 {
		t.Fatalf("want the standby as the only leader, got %d", n)
	}
}

func leaders(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
