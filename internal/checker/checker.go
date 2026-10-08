// Package checker reads the committed job history and ledger and proves:
//   - every enqueued job reached a terminal or healthy in-flight state
//   - every completed job applied its effect exactly once (ledger)
//   - no ledger row exists whose fencing token does not match the job's
//     final lease version (i.e. no stale worker committed an effect)
package checker

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Report struct {
	Total      int            `json:"total"`
	ByState    map[string]int `json:"by_state"`
	LedgerRows int            `json:"ledger_rows"`
	Violations []string       `json:"violations"`
}

func (r *Report) OK() bool {
	return len(r.Violations) == 0
}

func Run(ctx context.Context, db *sql.DB) (*Report, error) {
	r := &Report{ByState: map[string]int{}}
	rows, err := db.QueryContext(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		r.ByState[state] = n
		r.Total += n
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM ledger`).Scan(&r.LedgerRows); err != nil {
		return nil, err
	}
	// Completed jobs with missing or duplicate ledger rows.
	bad, err := db.QueryContext(ctx, `
SELECT j.id, count(l.job_id) FROM jobs j
LEFT JOIN ledger l ON l.job_id = j.id
WHERE j.state IN ('completed','discarded','cancelled')
GROUP BY j.id HAVING (j.state='completed' AND count(l.job_id) <> 1) OR (j.state <> 'completed' AND count(l.job_id) <> 0)`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = bad.Close() }()
	for bad.Next() {
		var id int64
		var n int
		if err := bad.Scan(&id, &n); err != nil {
			return nil, err
		}
		r.Violations = append(r.Violations, fmt.Sprintf("job %d: %d ledger rows for its terminal state", id, n))
	}
	// Fencing mismatch: ledger token must equal final lease_version for
	// completed jobs.
	v, err := db.QueryContext(ctx, `
SELECT l.job_id, l.fencing_token, j.lease_version
FROM ledger l JOIN jobs j ON j.id = l.job_id
WHERE l.fencing_token <> j.lease_version`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = v.Close() }()
	for v.Next() {
		var id, tok, ver int64
		if err := v.Scan(&id, &tok, &ver); err != nil {
			return nil, err
		}
		r.Violations = append(r.Violations, fmt.Sprintf("job %d: ledger token %d != final lease_version %d", id, tok, ver))
	}
	// Effect count never exceeds 1 per job (PK already enforces, verify).
	var nd int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT job_id FROM ledger GROUP BY job_id HAVING count(*) > 1) x`).Scan(&nd); err != nil {
		return nil, err
	}
	if nd > 0 {
		r.Violations = append(r.Violations, fmt.Sprintf("%d jobs have >1 ledger row", nd))
	}
	// Jobs stuck running with an expired lease.
	var ns int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE state='running' AND lease_expiry < now() - interval '30 seconds'`).Scan(&ns); err != nil {
		return nil, err
	}
	if ns > 0 {
		r.Violations = append(r.Violations, fmt.Sprintf("%d jobs running with lease long past expiry", ns))
	}
	return r, nil
}

// WaitForTerminal polls the database until every enqueued job is in a
// terminal state or the timeout expires.
func WaitForTerminal(ctx context.Context, db *sql.DB, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		// Tolerate transient errors: the database may be mid-restart.
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM jobs WHERE state IN ('scheduled','available','running','retryable')`).Scan(&n); err != nil {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		if n == 0 {
			return 0, nil
		}
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	var n int
	_ = db.QueryRowContext(ctx,
		`SELECT count(*) FROM jobs WHERE state IN ('scheduled','available','running','retryable')`).Scan(&n)
	return n, nil
}
