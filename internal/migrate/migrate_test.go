package migrate_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestUpAppliesMigrationsIdempotently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("conveyor"),
		postgres.WithUsername("conveyor"),
		postgres.WithPassword("conveyor"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Terminate(context.Background()) }()
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := migrate.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrate.Up(ctx, db); err != nil {
		t.Fatalf("second run must be a no-op: %v", err)
	}
	for _, table := range []string{"jobs", "queues", "jobs_archive", "cron_schedules", "job_dependencies", "ledger", "schema_migrations"} {
		var exists bool
		if err := db.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=$1)`, table).Scan(&exists); err != nil || !exists {
			t.Fatalf("table %s missing: %v", table, err)
		}
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO queues(name) VALUES ('default')`); err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO jobs(queue, kind, args, state) VALUES ('default','email','{}','available')`
	if _, err := db.ExecContext(ctx, insert); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO jobs(queue, kind, args, state, idempotency_key) VALUES ('default','k','{}','available','a')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO jobs(queue, kind, args, state, idempotency_key) VALUES ('default','k','{}','available','a')`); err == nil {
		t.Fatal("expected unique violation on (queue, idempotency_key)")
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("want 2 jobs, got %d (%v)", n, err)
	}
}
