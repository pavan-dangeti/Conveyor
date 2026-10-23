package queue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"pgregory.net/rapid"

	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

func enqueueN(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	for range n {
		if _, _, err := queue.Enqueue(context.Background(), db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompleteManyRejectsStaleTokens(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	enqueueN(t, db, 3)
	jobs, err := queue.Dequeue(ctx, db, "q1", "w1", time.Minute, 3)
	if err != nil || len(jobs) != 3 {
		t.Fatal(len(jobs), err)
	}
	ids := []int64{jobs[0].ID, jobs[1].ID, jobs[2].ID}
	versions := []int64{jobs[0].LeaseVersion, jobs[1].LeaseVersion + 1, jobs[2].LeaseVersion}
	done, err := queue.CompleteMany(ctx, db, "w1", ids, versions)
	if err != nil || len(done) != 2 {
		t.Fatalf("want 2 completed, got %v %v", done, err)
	}
	if s := pgtest.State(t, db, jobs[1].ID); s != "running" {
		t.Fatalf("stale entry must not complete, got %s", s)
	}
	if done, _ := queue.CompleteMany(ctx, db, "intruder", ids[1:2], []int64{jobs[1].LeaseVersion}); len(done) != 0 {
		t.Fatal("another owner must not complete the job")
	}
}

func TestCancellingARunningJobFencesItsWorker(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	enqueueN(t, db, 1)
	jobs, _ := queue.Dequeue(ctx, db, "q1", "w1", time.Minute, 1)
	j := jobs[0]
	if err := queue.Cancel(ctx, db, j.ID); err != nil {
		t.Fatal(err)
	}
	if err := queue.Heartbeat(ctx, db, j.ID, "w1", j.LeaseVersion, time.Minute); !errors.Is(err, queue.ErrStaleLease) {
		t.Fatalf("heartbeat after cancel: %v", err)
	}
	if err := queue.Complete(ctx, db, j.ID, "w1", j.LeaseVersion); !errors.Is(err, queue.ErrStaleLease) {
		t.Fatalf("complete after cancel: %v", err)
	}
	if err := queue.Cancel(ctx, db, j.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("cancelling a cancelled job: %v", err)
	}
	if err := queue.Discard(ctx, db, j.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("discarding a cancelled job: %v", err)
	}
}

func TestFailRetriesWithBackoffThenDeadLetters(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	id, _, _ := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), MaxAttempts: 2})
	jobs, _ := queue.Dequeue(ctx, db, "q1", "w1", time.Minute, 1)
	if err := queue.Fail(ctx, db, id, "w1", jobs[0].LeaseVersion, "boom", jobs[0].Attempt, jobs[0].MaxAttempts); err != nil {
		t.Fatal(err)
	}
	if s := pgtest.State(t, db, id); s != "retryable" {
		t.Fatalf("first failure: %s", s)
	}
	if _, err := db.Exec(`UPDATE jobs SET run_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PromoteDue(ctx, db); err != nil {
		t.Fatal(err)
	}
	jobs, _ = queue.Dequeue(ctx, db, "q1", "w1", time.Minute, 1)
	if err := queue.Fail(ctx, db, id, "w1", jobs[0].LeaseVersion, "boom again", jobs[0].Attempt, jobs[0].MaxAttempts); err != nil {
		t.Fatal(err)
	}
	var state string
	var history int
	if err := db.QueryRow(`SELECT state, jsonb_array_length(error_history) FROM jobs WHERE id = $1`, id).Scan(&state, &history); err != nil {
		t.Fatal(err)
	}
	if state != "discarded" || history != 2 {
		t.Fatalf("want discarded with 2 errors, got %s %d", state, history)
	}
	if err := queue.Fail(ctx, db, id, "w1", jobs[0].LeaseVersion, "zombie", 2, 2); !errors.Is(err, queue.ErrStaleLease) {
		t.Fatalf("failing a dead-lettered job: %v", err)
	}
}

func TestEnqueueScheduleAndDependencies(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	at := time.Now().Add(time.Hour).Truncate(time.Second)
	id, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), RunAt: at})
	if err != nil {
		t.Fatal(err)
	}
	var runAt time.Time
	if err := db.QueryRow(`SELECT run_at FROM jobs WHERE id = $1`, id).Scan(&runAt); err != nil || !runAt.Equal(at) {
		t.Fatalf("run_at %v, want %v (%v)", runAt, at, err)
	}
	if s := pgtest.State(t, db, id); s != "scheduled" {
		t.Fatalf("future run_at must be scheduled, got %s", s)
	}
	if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "k", Args: json.RawMessage(`{}`), DependsOn: []int64{424242}}); err == nil {
		t.Fatal("a dependency on a missing job must be rejected")
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM jobs`).Scan(&n)
	if n != 1 {
		t.Fatalf("the rejected enqueue must leave no row behind, got %d jobs", n)
	}
}

func TestStatsAndArchive(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	enqueueN(t, db, 4)
	jobs, _ := queue.Dequeue(ctx, db, "q1", "w1", time.Minute, 4)
	for _, j := range jobs[:3] {
		if err := queue.Complete(ctx, db, j.ID, "w1", j.LeaseVersion); err != nil {
			t.Fatal(err)
		}
	}
	st, err := queue.ReadStats(ctx, db, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if st.CompletedPerSec != 3.0/60 || st.StartP99 < st.StartP50 || st.DeadLetter != 0 {
		t.Fatalf("stats: %+v", st)
	}
	n, err := queue.ArchiveOld(ctx, db, 0)
	if err != nil || n != 3 {
		t.Fatalf("archive: %d %v", n, err)
	}
	var hot, cold int
	_ = db.QueryRow(`SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM jobs_archive)`).Scan(&hot, &cold)
	if hot != 1 || cold != 3 {
		t.Fatalf("hot=%d archived=%d", hot, cold)
	}
}

// A burst of enqueues inside one debounce window must still end in a
// wake-up after the window, not only the one at its start.
func TestNotifyDebounceHasTrailingEdge(t *testing.T) {
	db, dsn := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN jobs_available"); err != nil {
		t.Fatal(err)
	}
	queue.SetNotifyDebounce(300 * time.Millisecond)
	defer queue.SetNotifyDebounce(10 * time.Millisecond)
	time.Sleep(350 * time.Millisecond)
	enqueueN(t, db, 5)
	start := time.Now()
	for i := range 2 {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			t.Fatalf("notification %d: %v", i+1, err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("trailing wake-up took %v", elapsed)
	}
}

// Property: backoff is full jitter in [0, min(10m, 1s * 2^(attempt-1))) for
// any attempt count, including ones where the doubling would overflow.
func TestBackoffIsBoundedFullJitter(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		attempt := rapid.IntRange(1, 1000).Draw(t, "attempt")
		ceiling := 10 * time.Minute
		if attempt <= 10 {
			ceiling = min(ceiling, time.Second<<(attempt-1))
		}
		if d := queue.Backoff(attempt); d < 0 || d >= ceiling {
			t.Fatalf("Backoff(%d) = %v, want [0, %v)", attempt, d, ceiling)
		}
	})
}
