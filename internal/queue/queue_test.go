package queue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setup(t *testing.T) *sql.DB {
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
	return db
}

func TestIdempotentEnqueue(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	key := "abc"
	id1, dedup1, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), IdempotencyKey: &key})
	if err != nil || dedup1 {
		t.Fatal(id1, dedup1, err)
	}
	id2, dedup2, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), IdempotencyKey: &key})
	if err != nil || !dedup2 || id2 != id1 {
		t.Fatalf("want dedup with same id, got %v %v %v", id2, dedup2, err)
	}
}

func TestPriorityOrderAndStaleToken(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	for _, p := range []int{0, 5, 3} {
		if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), Priority: p}); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := queue.Dequeue(ctx, db, "q1", "w1", 30*time.Second, 3)
	if err != nil || len(jobs) != 3 {
		t.Fatal(len(jobs), err)
	}
	if jobs[0].Priority != 5 || jobs[1].Priority != 3 || jobs[2].Priority != 0 {
		t.Fatalf("priority order wrong: %+v", []int{jobs[0].Priority, jobs[1].Priority, jobs[2].Priority})
	}
	j := jobs[0]
	if err := queue.Complete(ctx, db, j.ID, "w1", j.LeaseVersion+1); err == nil {
		t.Fatal("stale fencing token must be rejected")
	}
	if err := queue.Complete(ctx, db, j.ID, "w1", j.LeaseVersion); err != nil {
		t.Fatal(err)
	}
}

func TestReapExpiredLeases(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	jobs, _ := queue.Dequeue(ctx, db, "q1", "w1", 1*time.Second, 1)
	if len(jobs) != 1 {
		t.Fatal("want 1 job")
	}
	time.Sleep(1100 * time.Millisecond)
	n, err := queue.ReapExpiredLeases(ctx, db)
	if err != nil || n != 1 {
		t.Fatalf("want 1 reaped, got %d %v", n, err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM jobs WHERE id=$1`, jobs[0].ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "retryable" {
		t.Fatalf("want retryable, got %s", state)
	}
	if err := queue.Complete(ctx, db, jobs[0].ID, "w1", jobs[0].LeaseVersion); err == nil {
		t.Fatal("zombie worker must not complete after lease expired")
	}
}

func TestCancelAndRetry(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	id, _, _ := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`)})
	if err := queue.Cancel(ctx, db, id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET state='discarded' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := queue.Retry(ctx, db, id); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM jobs WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "available" {
		t.Fatalf("want available, got %s", state)
	}
}

func TestRateLimitAndDependencyGate(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	if _, err := db.Exec(`INSERT INTO queues(name, rate_limit_per_sec) VALUES ('limited', 1)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "limited", Kind: "k", Args: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	j1, err := queue.Dequeue(ctx, db, "limited", "w1", 30*time.Second, 2)
	if err != nil || len(j1) != 2 {
		t.Fatal(len(j1), err)
	}
	j2, err := queue.Dequeue(ctx, db, "limited", "w1", 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(j2) != 0 {
		t.Fatalf("rate limit should block second batch, got %d", len(j2))
	}
	// dependency gate
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "dep", Args: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var parent int64
	_ = parent
	if _, err := db.Exec(`INSERT INTO jobs(queue, kind, args, state) VALUES ('q1','parent','{}','available')`); err != nil {
		t.Fatal(err)
	}
	var pid int64
	if err := db.QueryRow(`SELECT id FROM jobs WHERE kind='parent'`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	var cid int64
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "child", Args: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT id FROM jobs WHERE kind='child'`).Scan(&cid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_dependencies(job_id, depends_on) VALUES ($1, $2)`, cid, pid); err != nil {
		t.Fatal(err)
	}
	kids, err := queue.Dequeue(ctx, db, "q1", "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range kids {
		if j.Kind == "child" {
			t.Fatal("child dequeued before parent completed")
		}
	}
	if _, err := db.Exec(`UPDATE jobs SET state='completed' WHERE id=$1`, pid); err != nil {
		t.Fatal(err)
	}
	kids, err = queue.Dequeue(ctx, db, "q1", "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, j := range kids {
		if j.Kind == "child" {
			found = true
		}
	}
	if !found {
		t.Fatal("child should be dequeued after parent completed")
	}
}

func TestExpandDueCron(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO cron_schedules(queue, kind, args, spec, next_run_at) VALUES ('q1','tick','{}','*/5 * * * *', now() - interval '1 minute')`)
	if err != nil {
		t.Fatal(err)
	}
	n, err := queue.ExpandDueCron(ctx, db, time.Now())
	if err != nil || n != 1 {
		t.Fatalf("want 1 expanded, got %d %v", n, err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM jobs WHERE queue='q1'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "available" {
		t.Fatalf("want available, got %s", state)
	}
	// Second run must not duplicate (idempotency key on cron occurrence).
	n, err = queue.ExpandDueCron(ctx, db, time.Now())
	if err != nil || n != 0 {
		t.Fatalf("want 0 expanded second time, got %d %v", n, err)
	}
	var c int
	_ = db.QueryRow(`SELECT count(*) FROM jobs`).Scan(&c)
	if c != 1 {
		t.Fatalf("want exactly 1 job, got %d", c)
	}
}
