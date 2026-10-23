package fault_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/pavan-dangeti/Conveyor/internal/checker"
	"github.com/pavan-dangeti/Conveyor/internal/maintenance"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
)

const (
	jobsPerRun  = 200
	lease       = 2 * time.Second
	maxAttempts = 25
	// Worker w3 runs with a clock 4x slow relative to the lease: it
	// heartbeats every 8 s against a 2 s lease, so every job longer than
	// the lease is reaped from under it while it is still working.
	skewedHeartbeat = 4 * lease
)

type env struct {
	pg      *postgres.PostgresContainer
	workers *toxiproxy.Proxy
	procs   []*exec.Cmd
}

// fault is one injected failure at an offset from the start of a run. The
// schedule is identical in every run; what varies is the interleaving.
type fault struct {
	at     time.Duration
	name   string
	detail string
	do     func(*env) error
}

var schedule = []fault{
	{0, "latency+drops", "worker link, whole run: 30 ms ± 10 ms latency each way; 5% of connections reset by peer after 500 ms",
		func(e *env) error {
			for _, stream := range []string{"upstream", "downstream"} {
				if _, err := e.workers.AddToxic("latency_"+stream, "latency", stream, 1, toxiproxy.Attributes{"latency": 30, "jitter": 10}); err != nil {
					return err
				}
			}
			_, err := e.workers.AddToxic("reset", "reset_peer", "downstream", 0.05, toxiproxy.Attributes{"timeout": 500})
			return err
		}},
	{2 * time.Second, "postgres-restart", "Postgres container stopped, down for 2 s, started again",
		func(e *env) error {
			if err := e.pg.Stop(context.Background(), nil); err != nil {
				return err
			}
			time.Sleep(2 * time.Second)
			return e.pg.Start(context.Background())
		}},
	{8 * time.Second, "kill-9", "worker w0 receives SIGKILL mid-batch and never returns",
		func(e *env) error { return e.procs[0].Process.Signal(syscall.SIGKILL) }},
	{10 * time.Second, "sigstop", "worker w1 paused with SIGSTOP for 5 s (2.5 leases), then SIGCONT: it resumes as a zombie holding stale fencing tokens",
		func(e *env) error {
			if err := e.procs[1].Process.Signal(syscall.SIGSTOP); err != nil {
				return err
			}
			time.Sleep(5 * time.Second)
			return e.procs[1].Process.Signal(syscall.SIGCONT)
		}},
	{17 * time.Second, "partition", "worker link cut for 4 s (toxiproxy disabled: open connections closed, new ones refused), then healed",
		func(e *env) error {
			if err := e.workers.Disable(); err != nil {
				return err
			}
			time.Sleep(4 * time.Second)
			return e.workers.Enable()
		}},
}

type runReport struct {
	Run              int             `json:"run"`
	Seconds          float64         `json:"seconds_to_drain"`
	Check            *checker.Report `json:"check"`
	LeaseExpirations int64           `json:"lease_expirations"`
	Reclaimed        int64           `json:"jobs_claimed_more_than_once"`
	MaxFencingToken  int64           `json:"max_fencing_token"`
	FaultErrors      []string        `json:"fault_errors,omitempty"`
}

// TestFaultInjection runs real worker processes through toxiproxy, injects
// every fault in schedule plus a clock-skewed worker, and lets the checker
// prove that no job was lost and no effect was applied twice. FAULT_RUNS
// repeats the run; FAULT_RESULTS=1 writes results/fault-<date>.json.
func TestFaultInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("fault-injection harness skipped in -short mode")
	}
	runs, _ := strconv.Atoi(os.Getenv("FAULT_RUNS"))
	runs = max(runs, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(runs)*5*time.Minute)
	t.Cleanup(cancel)

	nw, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("conveyor"), postgres.WithUsername("conveyor"), postgres.WithPassword("conveyor"),
		tcnetwork.WithNetwork([]string{"pg"}, nw),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	tp, err := testcontainers.Run(ctx, "ghcr.io/shopify/toxiproxy:2.12.0",
		testcontainers.WithExposedPorts("8474/tcp", "8666/tcp", "8667/tcp"),
		tcnetwork.WithNetwork([]string{"toxiproxy"}, nw),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/version").WithPort("8474/tcp")),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Terminate(context.Background()) })
	endpoint := func(port string) string {
		hp, err := tp.PortEndpoint(ctx, port, "")
		if err != nil {
			t.Fatal(err)
		}
		return hp
	}
	tox := toxiproxy.NewClient("http://" + endpoint("8474/tcp"))
	// The checker gets a clean path so it observes, rather than suffers,
	// the faults; workers go through the faulty one.
	if _, err := tox.CreateProxy("admin", "0.0.0.0:8666", "pg:5432"); err != nil {
		t.Fatal(err)
	}
	workersProxy, err := tox.CreateProxy("workers", "0.0.0.0:8667", "pg:5432")
	if err != nil {
		t.Fatal(err)
	}
	dsn := func(port string) string {
		return fmt.Sprintf("postgres://conveyor:conveyor@%s/conveyor?sslmode=disable", endpoint(port))
	}
	workerDSN := dsn("8667/tcp")

	db, err := store.Open(ctx, dsn("8666/tcp"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO queues (name, concurrency) VALUES ('q1', 100)`); err != nil {
		t.Fatal(err)
	}
	go func() {
		quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
		for ctx.Err() == nil {
			if maintenance.RunElectedLoop(ctx, db, quiet, 250*time.Millisecond) != nil {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	bin := filepath.Join(t.TempDir(), "harness-worker")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/harness-worker").CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}

	var reports []runReport
	for run := 1; run <= runs; run++ {
		rep := runOnce(ctx, t, db, &env{pg: pg, workers: workersProxy}, bin, workerDSN, run)
		reports = append(reports, rep)
		for _, v := range rep.Check.Violations {
			t.Errorf("run %d: %s", run, v)
		}
		if completed := rep.Check.ByState["completed"]; rep.Check.Total != jobsPerRun || completed != jobsPerRun || rep.Check.LedgerRows != jobsPerRun {
			t.Errorf("run %d: want %d completed jobs and ledger rows, got total=%d completed=%d ledger=%d",
				run, jobsPerRun, rep.Check.Total, completed, rep.Check.LedgerRows)
		}
		if rep.LeaseExpirations == 0 {
			t.Errorf("run %d: no lease expired, so the faults never exercised recovery", run)
		}
		t.Logf("run %d: drained in %.1fs; %d lease expirations; %d jobs claimed more than once; max fencing token %d",
			run, rep.Seconds, rep.LeaseExpirations, rep.Reclaimed, rep.MaxFencingToken)
	}
	if os.Getenv("FAULT_RESULTS") == "1" {
		writeResults(t, reports)
	}
}

func runOnce(ctx context.Context, t *testing.T, db *sql.DB, e *env, bin, workerDSN string, run int) runReport {
	t.Helper()
	if _, err := db.ExecContext(ctx, `TRUNCATE jobs, ledger, queue_rate CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE counters SET value = 0`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"latency_upstream", "latency_downstream", "reset"} {
		_ = e.workers.RemoveToxic(name)
	}
	for i := range jobsPerRun {
		ms := 150
		if i%5 == 0 {
			ms = 3000
		}
		args, _ := json.Marshal(map[string]int{"ms": ms})
		if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "work", Args: args, MaxAttempts: maxAttempts}); err != nil {
			t.Fatal(err)
		}
	}

	for i := range 4 {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), "DATABASE_URL="+workerDSN, "QUEUE=q1", fmt.Sprintf("OWNER=w%d", i), "LEASE="+lease.String())
		if i == 3 {
			cmd.Env = append(cmd.Env, "HEARTBEAT_INTERVAL="+skewedHeartbeat.String())
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		e.procs = append(e.procs, cmd)
	}
	defer func() {
		for _, p := range e.procs {
			_ = p.Process.Signal(syscall.SIGCONT)
			_ = p.Process.Kill()
			_ = p.Wait()
		}
		e.procs = nil
	}()

	start := time.Now()
	faultErrs := make(chan string, len(schedule))
	go func() {
		defer close(faultErrs)
		for _, f := range schedule {
			time.Sleep(time.Until(start.Add(f.at)))
			t.Logf("run %d t=%v: %s", run, f.at, f.name)
			if err := f.do(e); err != nil {
				faultErrs <- fmt.Sprintf("%s: %v", f.name, err)
			}
		}
	}()

	remaining, err := checker.WaitForTerminal(ctx, db, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if remaining > 0 {
		t.Fatalf("run %d: %d jobs not terminal after 3 minutes", run, remaining)
	}
	rep := runReport{Run: run, Seconds: time.Since(start).Seconds()}
	for msg := range faultErrs {
		rep.FaultErrors = append(rep.FaultErrors, msg)
		t.Errorf("run %d fault: %s", run, msg)
	}
	if rep.Check, err = checker.Run(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT
  (SELECT value FROM counters WHERE name = 'lease_expirations'),
  (SELECT count(*) FROM jobs WHERE lease_version > 1),
  (SELECT COALESCE(max(lease_version), 0) FROM jobs)`).Scan(&rep.LeaseExpirations, &rep.Reclaimed, &rep.MaxFencingToken); err != nil {
		t.Fatal(err)
	}
	return rep
}

func writeResults(t *testing.T, reports []runReport) {
	t.Helper()
	faults := make([]map[string]any, len(schedule))
	for i, f := range schedule {
		faults[i] = map[string]any{"at_seconds": f.at.Seconds(), "name": f.name, "detail": f.detail}
	}
	host, _ := exec.Command("sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize").Output()
	out, _ := json.MarshalIndent(map[string]any{
		"date":     time.Now().UTC().Format("2006-01-02"),
		"command":  fmt.Sprintf("FAULT_RUNS=%d FAULT_RESULTS=1 go test -run TestFaultInjection -count=1 -v ./internal/fault", len(reports)),
		"host":     fmt.Sprintf("%s/%s %q", runtime.GOOS, runtime.GOARCH, string(host)),
		"versions": map[string]string{"go": runtime.Version(), "postgres": "postgres:17-alpine", "toxiproxy": "2.12.0"},
		"setup": map[string]any{
			"jobs_per_run":  jobsPerRun,
			"workers":       4,
			"lease":         lease.String(),
			"max_attempts":  maxAttempts,
			"job_mix":       "80% sleep 150 ms, 20% sleep 3000 ms (longer than the lease)",
			"skewed_worker": fmt.Sprintf("w3 heartbeats every %s against a %s lease for the whole run", skewedHeartbeat, lease),
			"faults":        faults,
			"effect":        "harness-worker inserts one ledger row and completes the job in one transaction guarded by (owner, fencing token)",
			"checker":       "every job terminal; each completed job has exactly one ledger row whose fencing token equals its final lease_version; non-completed jobs have none",
			"topology":      "workers -> toxiproxy:8667 (faulty) -> postgres; checker -> toxiproxy:8666 (clean) -> postgres",
		},
		"runs": reports,
	}, "", "  ")
	path := filepath.Join("..", "..", "results", "fault-"+time.Now().UTC().Format("2006-01-02")+".json")
	if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", path)
}
