package maintenance_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/maintenance"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestLeaderPromotesAndReaps(t *testing.T) {
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
