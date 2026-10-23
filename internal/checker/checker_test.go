package checker_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/checker"
	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
)

func job(t *testing.T, db *sql.DB, state string, version int64) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO jobs (queue, kind, args, state, lease_version) VALUES ('q1', 'k', '{}', $1, $2) RETURNING id`,
		state, version).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func effect(t *testing.T, db *sql.DB, id, token int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO ledger (job_id, effect_id, fencing_token) VALUES ($1, 'effect', $2)`, id, token); err != nil {
		t.Fatal(err)
	}
}

func TestCleanHistoryPasses(t *testing.T) {
	db, _ := pgtest.Start(t)
	effect(t, db, job(t, db, "completed", 3), 3)
	job(t, db, "discarded", 2)
	rep, err := checker.Run(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.Total != 2 || rep.LedgerRows != 1 || rep.ByState["completed"] != 1 {
		t.Fatalf("unexpected report: %+v", rep)
	}
}

func TestViolationsAreReported(t *testing.T) {
	db, _ := pgtest.Start(t)
	job(t, db, "completed", 1)                   // lost effect
	effect(t, db, job(t, db, "completed", 4), 2) // committed by a zombie holding token 2
	effect(t, db, job(t, db, "discarded", 1), 1) // effect without completion
	stuck := job(t, db, "running", 1)
	if _, err := db.Exec(`UPDATE jobs SET lease_expiry = now() - interval '1 minute' WHERE id = $1`, stuck); err != nil {
		t.Fatal(err)
	}
	rep, err := checker.Run(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(rep.Violations, "\n")
	for _, want := range []string{"0 ledger rows", "ledger token 2 != final lease_version 4", "1 ledger rows", "lease long past expiry"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing violation %q in:\n%s", want, all)
		}
	}
}

func TestWaitForTerminal(t *testing.T) {
	db, _ := pgtest.Start(t)
	id := job(t, db, "available", 0)
	if n, err := checker.WaitForTerminal(context.Background(), db, 300*time.Millisecond); err != nil || n != 1 {
		t.Fatalf("want 1 pending after timeout, got %d %v", n, err)
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = db.Exec(`UPDATE jobs SET state = 'completed' WHERE id = $1`, id)
	}()
	if n, err := checker.WaitForTerminal(context.Background(), db, 5*time.Second); err != nil || n != 0 {
		t.Fatalf("want 0 pending, got %d %v", n, err)
	}
}
