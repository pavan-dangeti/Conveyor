package fault_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"

	"github.com/pavan-dangeti/Conveyor/internal/checker"
	"github.com/pavan-dangeti/Conveyor/internal/faultproxy"
	"github.com/pavan-dangeti/Conveyor/internal/maintenance"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestFaultInjection runs real worker processes against Postgres and injects
// kill -9, SIGSTOP/SIGCONT, a Postgres restart, and proxy drops+latency. The
// checker then proves no job was lost and no effect applied twice.
func TestFaultInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault-injection harness in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixedPort := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()
	c, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("conveyor"),
		postgres.WithUsername("conveyor"),
		postgres.WithPassword("conveyor"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("5432/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: fixedPort}}}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn := fmt.Sprintf("postgres://conveyor:conveyor@127.0.0.1:%s/conveyor?sslmode=disable", fixedPort)
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

	logDiscard := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		for ctx.Err() == nil {
			if err := maintenance.RunElectedLoop(ctx, db, logDiscard, 300*time.Millisecond); err != nil {
				time.Sleep(time.Second)
			}
		}
	}()

	proxy, err := faultproxy.New("127.0.0.1:0", "127.0.0.1:"+fixedPort)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = proxy.Close() }()
	defer proxy.SetDropRate(0)
	proxy.SetDropRate(0.05)
	proxy.SetLatency(30 * time.Millisecond)
	proxyDSN := fmt.Sprintf("postgres://conveyor:conveyor@%s/conveyor?sslmode=disable", proxy.Addr())

	bin := "/tmp/harness-worker-bin"
	build := exec.Command("go", "build", "-o", bin, "../../cmd/harness-worker")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	var procs []*exec.Cmd
	for i := 0; i < 3; i++ {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"DATABASE_URL="+proxyDSN, "QUEUE=q1", fmt.Sprintf("OWNER=w%d", i))
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		procs = append(procs, cmd)
	}
	defer func() {
		for _, p := range procs {
			if p.Process != nil {
				_ = p.Process.Signal(syscall.SIGTERM)
			}
		}
		time.Sleep(500 * time.Millisecond)
		for _, p := range procs {
			if p.Process != nil {
				_ = p.Process.Kill()
			}
		}
	}()

	const totalJobs = 60
	for i := 0; i < totalJobs; i++ {
		if _, _, err := queue.Enqueue(ctx, db, queue.EnqueueParams{Queue: "q1", Kind: "work", Args: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}

	// Fault injection timeline.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		t.Log("restarting postgres")
		_ = c.Stop(ctx, nil)
		time.Sleep(2 * time.Second)
		_ = c.Start(ctx)

		time.Sleep(2500 * time.Millisecond)
		t.Log("SIGKILL worker 1")
		_ = procs[1].Process.Kill()

		time.Sleep(1500 * time.Millisecond)
		t.Log("SIGSTOP worker 2")
		_ = procs[2].Process.Signal(syscall.SIGSTOP)
		time.Sleep(3 * time.Second)
		t.Log("SIGCONT worker 2")
		_ = procs[2].Process.Signal(syscall.SIGCONT)
	}()

	remaining, err := checker.WaitForTerminal(ctx, db, 300*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if remaining > 0 {
		t.Fatalf("%d jobs not terminal after timeout", remaining)
	}
	// A brief settle window for any in-flight completion bookkeeping.
	time.Sleep(time.Second)
	rep, err := checker.Run(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range rep.Violations {
		t.Error(v)
	}
	out, _ := json.MarshalIndent(rep, "", "  ")
	t.Logf("report: %s", out)
	if err := os.MkdirAll("../../results", 0o755); err == nil {
		_ = os.WriteFile(fmt.Sprintf("../../results/fault-%s.json", time.Now().Format("2006-01-02")), out, 0o644)
	}
	if !rep.OK() || rep.Total != totalJobs {
		t.Fatalf("invariants violated: %s", out)
	}
	var completed, ledger int
	_ = db.QueryRow(`SELECT count(*) FROM jobs WHERE state='completed'`).Scan(&completed)
	_ = db.QueryRow(`SELECT count(*) FROM ledger`).Scan(&ledger)
	if completed != totalJobs || ledger != totalJobs {
		t.Fatalf("want %d completed and %d ledger rows, got %d/%d", totalJobs, totalJobs, completed, ledger)
	}
}
