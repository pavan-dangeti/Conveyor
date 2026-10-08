package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
)

// Demo workload: enqueue ~60 "work" jobs per minute so the dashboard shows
// a live flow. Exit with an error code only if the database is unreachable.
func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://conveyor:conveyor@127.0.0.1:55433/conveyor?sslmode=disable"
	}
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	if err := migrate.Up(context.Background(), db); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := db.Exec(`INSERT INTO queues(name) VALUES ('default') ON CONFLICT DO NOTHING`); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for range tick.C {
		for i := 0; i < 10; i++ {
			if _, _, err := queue.Enqueue(context.Background(), db, queue.EnqueueParams{
				Queue: "default", Kind: "work", Args: []byte(`{}`),
			}); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}
}
