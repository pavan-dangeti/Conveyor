// Package queue implements a durable, Postgres-backed job queue with
// leases, fencing tokens, retries, dead-lettering and workflow dependencies.
// Every state write takes its source and target states from
// internal/statemachine.
package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/pavan-dangeti/Conveyor/internal/metrics"
	sm "github.com/pavan-dangeti/Conveyor/internal/statemachine"
)

type Job struct {
	ID             int64
	Queue          string
	Kind           string
	Args           json.RawMessage
	State          string
	Priority       int
	Attempt        int
	MaxAttempts    int
	RunAt          time.Time
	LeaseOwner     sql.NullString
	LeaseExpiry    sql.NullTime
	LeaseVersion   int64
	IdempotencyKey sql.NullString
	Error          sql.NullString
	TraceContext   json.RawMessage
	CreatedAt      time.Time
}

const (
	DefaultMaxAttempts = 3
	DefaultLease       = 30 * time.Second
	backoffBase        = 1 * time.Second
	backoffCap         = 10 * time.Minute
)

// Backoff is exponential with full jitter: uniform in [0, min(cap, base*2^(attempt-1))).
func Backoff(attempt int) time.Duration {
	d := backoffBase << (attempt - 1)
	if d <= 0 || d > backoffCap {
		d = backoffCap
	}
	return time.Duration(rand.Float64() * float64(d))
}

var ErrStaleLease = errors.New("stale lease token or not running")

// ErrNotFound means no job matched in a state the requested transition
// accepts.
var ErrNotFound = errors.New("job not found in a state that allows this transition")

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Debouncing pg_notify was the throughput fix measured in Phase 5: one
// NOTIFY per insert serialises every enqueue on the notification queue lock,
// while idle workers only need one wake-up per burst.
var (
	notifyDebounce atomic.Int64
	lastNotifyAt   atomic.Int64
)

func init() { notifyDebounce.Store(int64(50 * time.Millisecond)) }

// SetNotifyDebounce changes the minimum gap between wake-up notifications;
// 0 notifies on every enqueue. Used by the benchmark to reproduce the
// before/after comparison.
func SetNotifyDebounce(d time.Duration) { notifyDebounce.Store(int64(d)) }

func notifyListeners(ctx context.Context, db execer) {
	now := time.Now().UnixNano()
	prev := lastNotifyAt.Load()
	if now-prev < notifyDebounce.Load() || !lastNotifyAt.CompareAndSwap(prev, now) {
		return
	}
	_, _ = db.ExecContext(ctx, `SELECT pg_notify('jobs_available', '')`)
}

type EnqueueParams struct {
	Queue       string
	Kind        string
	Args        json.RawMessage
	Priority    int
	MaxAttempts int
	// Delay and RunAt both schedule the job for later; RunAt wins when set.
	Delay          time.Duration
	RunAt          time.Time
	IdempotencyKey *string
	TraceContext   json.RawMessage
	// DependsOn lists jobs that must complete before this one is dequeued.
	DependsOn []int64
}

// Enqueue returns the new job ID. A duplicate (queue, idempotency_key)
// returns the existing job's ID and deduped=true without creating anything.
func Enqueue(ctx context.Context, db *sql.DB, p EnqueueParams) (id int64, deduped bool, err error) {
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	var runAt *time.Time
	if !p.RunAt.IsZero() {
		runAt = &p.RunAt
	}
	state := sm.Initial(p.Delay > 0 || (runAt != nil && runAt.After(time.Now())))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback() }()
	err = tx.QueryRowContext(ctx, `
INSERT INTO jobs (queue, kind, args, state, priority, max_attempts, run_at, idempotency_key, trace_context)
VALUES ($1, $2, $3, $4, $5, $6,
        COALESCE($7::timestamptz, now() + ($8 * interval '1 millisecond')), $9, $10)
ON CONFLICT (queue, idempotency_key) DO NOTHING
RETURNING id`,
		p.Queue, p.Kind, p.Args, string(state), p.Priority, maxAttempts, runAt,
		p.Delay.Milliseconds(), p.IdempotencyKey, p.TraceContext,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) && p.IdempotencyKey != nil {
		err = tx.QueryRowContext(ctx,
			`SELECT id FROM jobs WHERE queue=$1 AND idempotency_key=$2`, p.Queue, *p.IdempotencyKey).Scan(&id)
		return id, err == nil, err
	}
	if err != nil {
		return 0, false, err
	}
	for _, dep := range p.DependsOn {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_dependencies (job_id, depends_on) VALUES ($1, $2) ON CONFLICT DO NOTHING`, id, dep); err != nil {
			return 0, false, fmt.Errorf("dependency %d: %w", dep, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	if state == sm.Available {
		notifyListeners(ctx, db)
	}
	return id, false, nil
}

var (
	dequeueFrom, dequeueTo             = sm.Edge(sm.Dequeue)
	completeFrom, completeTo           = sm.Edge(sm.Complete)
	retryFrom, retryTo                 = sm.Edge(sm.FailRetry)
	failDiscardFrom, failDiscardTo     = sm.Edge(sm.FailDiscard)
	dueFrom, dueTo                     = sm.Edge(sm.Due)
	retryDueFrom, retryDueTo           = sm.Edge(sm.RetryDue)
	expRetryFrom, expRetryTo           = sm.Edge(sm.LeaseExpiredRetry)
	_, expDiscardTo                    = sm.Edge(sm.LeaseExpiredDiscard)
	cancelFrom, cancelTo               = sm.Edge(sm.Cancel)
	manualRetryFrom, manualRetryTo     = sm.Edge(sm.ManualRetry)
	manualDiscardFrom, manualDiscardTo = sm.Edge(sm.ManualDiscard)
)

// Dequeue claims up to batch due jobs in priority order, setting lease
// owner, expiry and a new fencing token (lease_version + 1). It skips jobs
// with unfinished dependencies and respects the queue's concurrency and
// rate limits. ponytail: the concurrency check counts running rows without
// locking the queue row, so concurrent dequeuers can briefly over-admit by
// up to one batch each; lock the queues row if that ever matters.
func Dequeue(ctx context.Context, db *sql.DB, queueName, owner string, lease time.Duration, batch int) ([]Job, error) {
	if lease <= 0 {
		lease = DefaultLease
	}
	start := time.Now()
	rows, err := db.QueryContext(ctx, `
WITH picked AS (
  SELECT id FROM jobs j
  WHERE state = ANY($5) AND queue = $1 AND run_at <= now()
    AND NOT EXISTS (
      SELECT 1 FROM job_dependencies d JOIN jobs dep ON dep.id = d.depends_on
      WHERE d.job_id = j.id AND dep.state <> $7)
    AND (SELECT count(*) FROM jobs r WHERE r.queue = $1 AND r.state = $6)
        < (SELECT concurrency FROM queues WHERE name = $1)
    AND (
      (SELECT rate_limit_per_sec FROM queues WHERE name = $1) IS NULL
      OR COALESCE((SELECT last_dispatch FROM queue_rate WHERE queue = $1), '-infinity'::timestamptz)
         + make_interval(secs => $4::float / (SELECT rate_limit_per_sec FROM queues WHERE name = $1))
         <= now()
    )
  ORDER BY priority DESC, run_at, id
  LIMIT $4
  FOR UPDATE SKIP LOCKED
), dispatch AS (
  INSERT INTO queue_rate (queue, last_dispatch)
  SELECT $1, now() WHERE EXISTS (SELECT 1 FROM picked)
  ON CONFLICT (queue) DO UPDATE SET last_dispatch = now()
)
UPDATE jobs SET state = $6, attempt = attempt + 1,
  lease_owner = $2, lease_expiry = now() + $3 * interval '1 millisecond',
  lease_version = lease_version + 1, started_at = now(), updated_at = now()
FROM picked WHERE jobs.id = picked.id
RETURNING jobs.id, jobs.queue, jobs.kind, jobs.args, jobs.state, jobs.priority,
          jobs.attempt, jobs.max_attempts, jobs.run_at, jobs.lease_owner,
          jobs.lease_expiry, jobs.lease_version, jobs.idempotency_key, jobs.error,
          COALESCE(jobs.trace_context, 'null'::jsonb), jobs.created_at,
          extract(epoch FROM now() - jobs.run_at)`,
		queueName, owner, lease.Milliseconds(), batch, dequeueFrom, string(dequeueTo), string(completeTo))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Job
	for rows.Next() {
		var j Job
		var waited float64
		if err := rows.Scan(&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
			&j.Attempt, &j.MaxAttempts, &j.RunAt, &j.LeaseOwner, &j.LeaseExpiry,
			&j.LeaseVersion, &j.IdempotencyKey, &j.Error, &j.TraceContext, &j.CreatedAt, &waited); err != nil {
			return nil, err
		}
		metrics.StartLatency.Observe(max(waited, 0))
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	metrics.Observe(string(sm.Dequeue), start, int64(len(out)))
	// UPDATE ... RETURNING does not preserve the CTE's ORDER BY.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if !a.RunAt.Equal(b.RunAt) {
			return a.RunAt.Before(b.RunAt)
		}
		return a.ID < b.ID
	})
	return out, nil
}

// Heartbeat extends the lease. ErrStaleLease means the caller no longer
// holds the current fencing token and must stop working on the job.
func Heartbeat(ctx context.Context, db *sql.DB, id int64, owner string, version int64, lease time.Duration) error {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET lease_expiry = now() + $4 * interval '1 millisecond', updated_at = now()
WHERE id = $1 AND state = $5 AND lease_owner = $2 AND lease_version = $3`,
		id, owner, version, lease.Milliseconds(), string(dequeueTo))
	return fenced(res, err)
}

// Complete marks a job completed, guarded by the fencing token.
func Complete(ctx context.Context, db *sql.DB, id int64, owner string, version int64) error {
	return CompleteTx(ctx, db, id, owner, version)
}

// CompleteTx is Complete on any executor, typically a *sql.Tx that also
// holds the job's side effect: the effect and the completion then commit
// together or not at all, and a zombie holding a stale token commits
// neither. This is the exactly-once-effect path.
func CompleteTx(ctx context.Context, tx execer, id int64, owner string, version int64) error {
	start := time.Now()
	res, err := tx.ExecContext(ctx, `
UPDATE jobs SET state = $4, lease_expiry = NULL, updated_at = now()
WHERE id = $1 AND state = ANY($5) AND lease_owner = $2 AND lease_version = $3`,
		id, owner, version, string(completeTo), completeFrom)
	if err = fenced(res, err); err == nil {
		metrics.Observe(string(sm.Complete), start, 1)
	}
	return err
}

// Fail records the error and moves the job to retryable with backoff, or to
// discarded (the dead-letter queue) once attempts are exhausted.
func Fail(ctx context.Context, db *sql.DB, id int64, owner string, version int64, cause string, attempt, maxAttempts int) error {
	ev, from, to, delay := sm.FailRetry, retryFrom, retryTo, Backoff(attempt)
	if attempt >= maxAttempts {
		ev, from, to, delay = sm.FailDiscard, failDiscardFrom, failDiscardTo, 0
	}
	start := time.Now()
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET state = $5, error = $4::text,
  error_history = error_history || jsonb_build_array(jsonb_build_object('at', now(), 'attempt', attempt, 'error', $4::text)),
  run_at = now() + $6 * interval '1 millisecond', lease_expiry = NULL, updated_at = now()
WHERE id = $1 AND state = ANY($7) AND lease_owner = $2 AND lease_version = $3`,
		id, owner, version, cause, string(to), delay.Milliseconds(), from)
	if err = fenced(res, err); err == nil {
		metrics.Observe(string(ev), start, 1)
	}
	return err
}

func fenced(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleLease
	}
	return nil
}

// PromoteDue moves due scheduled and retryable jobs to available and wakes
// workers if anything moved.
func PromoteDue(ctx context.Context, db *sql.DB) (int64, error) {
	var total int64
	for _, t := range []struct {
		ev   sm.Event
		from []string
		to   sm.State
	}{{sm.Due, dueFrom, dueTo}, {sm.RetryDue, retryDueFrom, retryDueTo}} {
		start := time.Now()
		res, err := db.ExecContext(ctx,
			`UPDATE jobs SET state = $1, updated_at = now() WHERE state = ANY($2) AND run_at <= now()`,
			string(t.to), t.from)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		metrics.Observe(string(t.ev), start, n)
		total += n
	}
	if total > 0 {
		notifyListeners(ctx, db)
	}
	return total, nil
}

// ReapExpiredLeases moves running jobs whose lease expired to retryable, or
// to discarded when attempts are exhausted. The old holder's token stays
// stale forever, so if it wakes up its writes are rejected.
func ReapExpiredLeases(ctx context.Context, db *sql.DB) (int64, error) {
	start := time.Now()
	var retried, discarded int64
	err := db.QueryRowContext(ctx, `
WITH reaped AS (
  UPDATE jobs SET
    state = CASE WHEN attempt >= max_attempts THEN $2 ELSE $3 END,
    error = 'lease expired',
    error_history = error_history || jsonb_build_array(jsonb_build_object('at', now(), 'attempt', attempt, 'error', 'lease expired')),
    run_at = now() + interval '1 second',
    lease_expiry = NULL,
    updated_at = now()
  WHERE state = ANY($1) AND lease_expiry < now()
  RETURNING state
), counted AS (
  UPDATE counters SET value = value + (SELECT count(*) FROM reaped)
  WHERE name = 'lease_expirations' AND EXISTS (SELECT 1 FROM reaped)
)
SELECT count(*) FILTER (WHERE state = $3), count(*) FILTER (WHERE state = $2) FROM reaped`,
		expRetryFrom, string(expDiscardTo), string(expRetryTo)).Scan(&retried, &discarded)
	if err != nil {
		return 0, err
	}
	metrics.Observe(string(sm.LeaseExpiredRetry), start, retried)
	metrics.Observe(string(sm.LeaseExpiredDiscard), start, discarded)
	return retried + discarded, nil
}

// CancelOrphans cancels waiting jobs whose workflow dependency was
// discarded or cancelled: they could otherwise never become runnable.
func CancelOrphans(ctx context.Context, db *sql.DB) (int64, error) {
	start := time.Now()
	res, err := db.ExecContext(ctx, `
UPDATE jobs j SET state = $1, error = 'dependency did not complete', updated_at = now()
WHERE j.state = ANY($2) AND j.state <> $3 AND EXISTS (
  SELECT 1 FROM job_dependencies d JOIN jobs dep ON dep.id = d.depends_on
  WHERE d.job_id = j.id AND dep.state IN ($4, $1))`,
		string(cancelTo), cancelFrom, string(dequeueTo), string(sm.Discarded))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	metrics.Observe(string(sm.Cancel), start, n)
	return n, nil
}

// ArchiveOld moves terminal jobs older than the given age into jobs_archive,
// at most 10k per call, so the hot table and its indexes stay small.
func ArchiveOld(ctx context.Context, db *sql.DB, olderThan time.Duration) (int64, error) {
	res, err := db.ExecContext(ctx, `
WITH moved AS (
  DELETE FROM jobs WHERE id IN (
    SELECT id FROM jobs
    WHERE state = ANY($1) AND updated_at < now() - $2 * interval '1 millisecond'
    LIMIT 10000)
  RETURNING id, queue, kind, args, state, priority, attempt, max_attempts, run_at,
            idempotency_key, error, error_history, trace_context, created_at, updated_at, started_at
)
INSERT INTO jobs_archive
  (id, queue, kind, args, state, priority, attempt, max_attempts, run_at,
   idempotency_key, error, error_history, trace_context, created_at, updated_at, started_at)
SELECT * FROM moved`, sm.TerminalStates(), olderThan.Milliseconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Cancel stops a job. A running job's holder notices on its next heartbeat
// (the guarded UPDATE no longer matches) and its outcome write is rejected.
func Cancel(ctx context.Context, db *sql.DB, id int64) error {
	return manual(ctx, db, sm.Cancel, cancelFrom, cancelTo, id, ``)
}

// Retry sends a dead-lettered job back to available with a fresh attempt budget.
func Retry(ctx context.Context, db *sql.DB, id int64) error {
	return manual(ctx, db, sm.ManualRetry, manualRetryFrom, manualRetryTo, id,
		`, attempt = 0, error = NULL, run_at = now()`)
}

// Discard moves a waiting job straight to the dead-letter queue.
func Discard(ctx context.Context, db *sql.DB, id int64) error {
	return manual(ctx, db, sm.ManualDiscard, manualDiscardFrom, manualDiscardTo, id,
		`, error = 'discarded by operator'`)
}

func manual(ctx context.Context, db *sql.DB, ev sm.Event, from []string, to sm.State, id int64, extraSet string) error {
	start := time.Now()
	res, err := db.ExecContext(ctx,
		`UPDATE jobs SET state = $2, lease_expiry = NULL, updated_at = now()`+extraSet+`
		 WHERE id = $1 AND state = ANY($3)`, id, string(to), from)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("job %d: %w (%s)", id, ErrNotFound, ev)
	}
	metrics.Observe(string(ev), start, 1)
	if to == sm.Available {
		notifyListeners(ctx, db)
	}
	return nil
}

// ExpandDueCron enqueues one job per due schedule and advances next_run_at.
// The idempotency key is derived from (schedule, occurrence), so re-running
// after a crash or leader failover cannot create the occurrence twice.
// Missed occurrences while no leader ran are skipped, not replayed.
func ExpandDueCron(ctx context.Context, db *sql.DB, now time.Time) (int64, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, queue, kind, args, spec, next_run_at FROM cron_schedules WHERE enabled AND next_run_at <= $1`, now)
	if err != nil {
		return 0, err
	}
	type sched struct {
		id                int64
		queue, kind, spec string
		args              []byte
		next              time.Time
	}
	var due []sched
	for rows.Next() {
		var s sched
		if err := rows.Scan(&s.id, &s.queue, &s.kind, &s.args, &s.spec, &s.next); err != nil {
			_ = rows.Close()
			return 0, err
		}
		due = append(due, s)
	}
	_ = rows.Close()
	var n int64
	for _, s := range due {
		cs, err := cron.ParseStandard(s.spec)
		if err != nil {
			return n, fmt.Errorf("schedule %d: %w", s.id, err)
		}
		key := fmt.Sprintf("cron:%d:%d", s.id, s.next.Unix())
		if _, _, err := Enqueue(ctx, db, EnqueueParams{Queue: s.queue, Kind: s.kind, Args: s.args, IdempotencyKey: &key}); err != nil {
			return n, err
		}
		n++
		if _, err := db.ExecContext(ctx, `UPDATE cron_schedules SET next_run_at = $2 WHERE id = $1`, s.id, cs.Next(now)); err != nil {
			return n, err
		}
	}
	return n, nil
}

type Stats struct {
	Window                       time.Duration
	CompletedPerSec              float64
	StartP50, StartP95, StartP99 time.Duration
	Retries                      int64
	DeadLetter                   int64
	LeaseExpirations             int64
}

// ReadStats summarises the last window from the jobs table, so every API
// replica reports the same numbers without shared in-memory state.
// Start latency is started_at - run_at on the Postgres clock.
func ReadStats(ctx context.Context, db *sql.DB, window time.Duration) (Stats, error) {
	s := Stats{Window: window}
	var completed int64
	var p50, p95, p99 sql.NullFloat64
	err := db.QueryRowContext(ctx, `
SELECT
  (SELECT count(*) FROM jobs WHERE state = $2 AND updated_at > now() - $1 * interval '1 millisecond'),
  (SELECT count(*) FROM jobs WHERE state = $3),
  (SELECT value FROM counters WHERE name = 'lease_expirations'),
  count(*) FILTER (WHERE attempt > 1),
  percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - run_at)),
  percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - run_at)),
  percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - run_at))
FROM jobs WHERE started_at > now() - $1 * interval '1 millisecond'`,
		window.Milliseconds(), string(completeTo), string(sm.Discarded)).
		Scan(&completed, &s.DeadLetter, &s.LeaseExpirations, &s.Retries, &p50, &p95, &p99)
	if err != nil {
		return s, err
	}
	s.CompletedPerSec = float64(completed) / window.Seconds()
	toDur := func(v sql.NullFloat64) time.Duration { return time.Duration(max(v.Float64, 0) * float64(time.Second)) }
	s.StartP50, s.StartP95, s.StartP99 = toDur(p50), toDur(p95), toDur(p99)
	return s, nil
}
