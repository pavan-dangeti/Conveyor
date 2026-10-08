// Package queue implements a durable, Postgres-backed job queue with
// leases, fencing tokens, retries, and dead-lettering.
package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"math/rand/v2"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/statemachine"
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

func Backoff(attempt int) time.Duration {
	d := backoffBase << (attempt - 1)
	if d <= 0 || d > backoffCap {
		d = backoffCap
	}
	return time.Duration(rand.Float64() * float64(d))
}

var ErrStaleLease = errors.New("stale lease token or not running")

// lastNotifyAt debounces pg_notify: at high enqueue rates one notification
// per insert triples notification dispatch cost with no delivery benefit,
// because every listener would just wake up and re-dequeue anyway. A 50 ms
// cap preserves sub-p99-500ms wakeups while letting throughput scale.
var lastNotifyAt atomic.Int64

func notifyListeners(ctx context.Context, db *sql.DB) {
	now := time.Now().UnixMilli()
	prev := lastNotifyAt.Load()
	if now-prev < 50 {
		return
	}
	if !lastNotifyAt.CompareAndSwap(prev, now) {
		return
	}
	_, _ = db.ExecContext(ctx, `SELECT pg_notify('jobs_available', '')`)
}

type EnqueueParams struct {
	Queue          string
	Kind           string
	Args           json.RawMessage
	Priority       int
	MaxAttempts    int
	Delay          time.Duration
	IdempotencyKey *string
	TraceContext   json.RawMessage
}

// Enqueue returns the new job ID. Duplicate (queue, idempotency_key) returns
// the existing job's ID and deduped=true.
func Enqueue(ctx context.Context, db *sql.DB, p EnqueueParams) (id int64, deduped bool, err error) {
	max := p.MaxAttempts
	if max <= 0 {
		max = DefaultMaxAttempts
	}
	state := statemachine.Available
	delay := p.Delay
	if delay > 0 {
		state = statemachine.Scheduled
	}
	err = db.QueryRowContext(ctx, `
INSERT INTO jobs (queue, kind, args, state, priority, max_attempts, run_at, idempotency_key, trace_context)
VALUES ($1,$2,$3,$4,$5,$6, now() + ($7 * interval '1 millisecond'), $8, $9)
ON CONFLICT (queue, idempotency_key) DO NOTHING
RETURNING id`,
		p.Queue, p.Kind, p.Args, string(state), p.Priority, max, delay.Milliseconds(), p.IdempotencyKey, p.TraceContext,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		if p.IdempotencyKey == nil {
			return 0, false, fmt.Errorf("insert produced no id and no idempotency conflict")
		}
		err = db.QueryRowContext(ctx,
			`SELECT id FROM jobs WHERE queue=$1 AND idempotency_key=$2`, p.Queue, *p.IdempotencyKey).Scan(&id)
		if err != nil {
			return 0, false, err
		}
		return id, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	if state == statemachine.Available {
		notifyListeners(ctx, db)
	}
	return id, false, nil
}

// Dequeue claims up to batch jobs in priority order, setting lease owner,
// expiry and a new fencing token. Skips jobs whose queue is at its
// concurrency budget. ponytail: concurrency check is a soft cap (counted
// without locking); strictly-at-most needs SELECT ... FOR UPDATE on a queue
// row — upgrade if contention metrics show over-admission.
func Dequeue(ctx context.Context, db *sql.DB, queueName, owner string, lease time.Duration, batch int) ([]Job, error) {
	if lease <= 0 {
		lease = DefaultLease
	}
	rows, err := db.QueryContext(ctx, `
WITH picked AS (
  SELECT id FROM jobs j
  WHERE state = 'available'
    AND queue = $1 AND run_at <= now()
    AND NOT EXISTS (
      SELECT 1 FROM job_dependencies d JOIN jobs dep ON dep.id = d.depends_on
      WHERE d.job_id = j.id AND dep.state <> 'completed')
    AND (SELECT count(*) FROM jobs r WHERE r.queue=$1 AND r.state='running')
        < (SELECT concurrency FROM queues WHERE name=$1)
    AND (
      (SELECT rate_limit_per_sec FROM queues WHERE name=$1) IS NULL
      OR COALESCE((SELECT last_dispatch FROM queue_rate WHERE queue=$1), '-infinity'::timestamptz)
         + make_interval(secs => $4::float / (SELECT rate_limit_per_sec FROM queues WHERE name=$1))
         <= now()
    )
  ORDER BY priority DESC, run_at, id
  LIMIT $4
  FOR UPDATE SKIP LOCKED
), dispatch AS (
  INSERT INTO queue_rate(queue, last_dispatch)
  SELECT $1, now() WHERE (SELECT count(*) FROM picked) > 0
  ON CONFLICT (queue) DO UPDATE SET last_dispatch = now()
  RETURNING queue
)
UPDATE jobs SET state='running', attempt=attempt+1,
  lease_owner=$2, lease_expiry=now()+$3::interval, lease_version=lease_version+1,
  updated_at=now()
FROM picked WHERE jobs.id = picked.id
RETURNING jobs.id, jobs.queue, jobs.kind, jobs.args, jobs.state, jobs.priority,
          jobs.attempt, jobs.max_attempts, jobs.run_at, jobs.lease_owner,
          jobs.lease_expiry, jobs.lease_version, jobs.idempotency_key, jobs.error,
          COALESCE(jobs.trace_context, 'null'::jsonb), jobs.created_at`,
		queueName, owner, fmt.Sprintf("%d seconds", int(lease.Seconds())), batch)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
			&j.Attempt, &j.MaxAttempts, &j.RunAt, &j.LeaseOwner, &j.LeaseExpiry,
			&j.LeaseVersion, &j.IdempotencyKey, &j.Error, &j.TraceContext, &j.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
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
	return out, rows.Err()
}

// Heartbeat extends the lease. Returns ErrStaleLease when the caller no
// longer holds the current fencing token.
func Heartbeat(ctx context.Context, db *sql.DB, id int64, owner string, version int64, lease time.Duration) error {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET lease_expiry = now() + $4::interval, updated_at=now()
WHERE id=$1 AND state='running' AND lease_owner=$2 AND lease_version=$3`,
		id, owner, version, fmt.Sprintf("%d seconds", int(lease.Seconds())))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleLease
	}
	return nil
}

// Complete marks a job completed. Guarded by the current fencing token;
// a stale worker gets ErrStaleLease and must not apply effects.
func Complete(ctx context.Context, db *sql.DB, id int64, owner string, version int64) error {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET state='completed', lease_expiry=NULL, updated_at=now()
WHERE id=$1 AND state='running' AND lease_owner=$2 AND lease_version=$3`,
		id, owner, version)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleLease
	}
	return nil
}

// Fail records the error and moves the job to retryable with backoff, or
// discarded when attempts are exhausted.
func Fail(ctx context.Context, db *sql.DB, id int64, owner string, version int64, cause string, attempt, maxAttempts int) error {
	var state string
	var runAtExpr string
	if attempt < maxAttempts {
		state = string(statemachine.Retryable)
		runAtExpr = fmt.Sprintf("now() + interval '%d milliseconds'", Backoff(attempt).Milliseconds())
	} else {
		state = string(statemachine.Discarded)
		runAtExpr = "now()"
	}
	res, err := db.ExecContext(ctx, fmt.Sprintf(`
UPDATE jobs SET state='%s', error=$4::text,
  error_history = error_history || jsonb_build_array(jsonb_build_object('at', now(), 'error', $4::text)),
  run_at = %s, lease_expiry=NULL, updated_at=now()
WHERE id=$1 AND state='running' AND lease_owner=$2 AND lease_version=$3`, state, runAtExpr),
		id, owner, version, cause)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleLease
	}
	return nil
}

// PromoteDue moves scheduled and due-retryable jobs to available.
// Returns the number of jobs promoted.
func PromoteDue(ctx context.Context, db *sql.DB) (int64, error) {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET state='available', updated_at=now()
WHERE state IN ('scheduled','retryable') AND run_at <= now()`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		notifyListeners(ctx, db)
	}
	return n, nil
}

// ReapExpiredLeases finds running jobs whose lease expired and moves them to
// retryable (with backoff) or discarded (attempts exhausted). Fencing means
// even if the old worker later wakes up, its writes are rejected.
func ReapExpiredLeases(ctx context.Context, db *sql.DB) (int64, error) {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET
  state = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END,
  error = 'lease expired',
  error_history = error_history || jsonb_build_array(jsonb_build_object('at', now(), 'error', 'lease expired')),
  run_at = now() + interval '1 second',
  lease_expiry = NULL,
  updated_at = now()
WHERE state='running' AND lease_expiry < now()`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ArchiveOld moves terminal jobs (completed/discarded/cancelled) older than
// the given age into jobs_archive so the hot table stays small.
func ArchiveOld(ctx context.Context, db *sql.DB, olderThan time.Duration) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO jobs_archive
  (id, queue, kind, args, state, priority, attempt, max_attempts, run_at,
   idempotency_key, error, error_history, trace_context, created_at, updated_at)
SELECT id, queue, kind, args, state, priority, attempt, max_attempts, run_at,
       idempotency_key, error, error_history, trace_context, created_at, updated_at
FROM jobs
WHERE state IN ('completed','discarded','cancelled')
  AND updated_at < now() - interval '%d seconds'`, int(olderThan.Seconds())))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
DELETE FROM jobs
WHERE state IN ('completed','discarded','cancelled')
  AND updated_at < now() - interval '%d seconds'`, int(olderThan.Seconds()))); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// Cancel marks a non-running job cancelled.
func Cancel(ctx context.Context, db *sql.DB, id int64) error {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET state='cancelled', updated_at=now()
WHERE id=$1 AND state IN ('scheduled','available','retryable')`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("job %d not cancellable (running or terminal)", id)
	}
	return nil
}

// Retry moves a discarded job back to available and resets its attempt.
func Retry(ctx context.Context, db *sql.DB, id int64) error {
	res, err := db.ExecContext(ctx, `
UPDATE jobs SET state='available', attempt=0, error=NULL, run_at=now(), updated_at=now()
WHERE id=$1 AND state='discarded'`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("job %d not in discarded state", id)
	}
	return nil
}

// ExpandDueCron enqueues jobs for every enabled schedule whose next_run_at
// is due. Idempotency keys derived from (id, next_run_at) make it safe to
// run on every leader tick and across failovers.
func ExpandDueCron(ctx context.Context, db *sql.DB, now time.Time) (int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, queue, kind, args, spec, next_run_at FROM cron_schedules WHERE enabled AND next_run_at <= $1`, now)
	if err != nil {
		return 0, err
	}
	type sched struct {
		id    int64
		queue string
		kind  string
		args  []byte
		spec  string
		next  time.Time
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
		if _, _, err := Enqueue(ctx, db, EnqueueParams{
			Queue: s.queue, Kind: s.kind, Args: s.args, IdempotencyKey: &key,
		}); err != nil {
			return n, err
		}
		n++
		if _, err := db.ExecContext(ctx, `UPDATE cron_schedules SET next_run_at=$2 WHERE id=$1`, s.id, cs.Next(now)); err != nil {
			return n, err
		}
	}
	return n, nil
}
