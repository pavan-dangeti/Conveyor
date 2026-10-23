// Package demo runs the public demo inside the API process: a synthetic
// workload, a supervised fleet of real worker processes, and a chaos action
// that SIGKILLs one of them so visitors can watch its jobs recover.
package demo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/checker"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/telemetry"
)

const (
	Queue         = "default"
	chaosCooldown = 5 * time.Second
	restartDelay  = 8 * time.Second
	checkCacheTTL = 5 * time.Second
)

type worker struct {
	Name     string `json:"name"`
	PID      int    `json:"pid"`
	Alive    bool   `json:"alive"`
	Restarts int    `json:"restarts"`
	Kills    int    `json:"kills"`
	cmd      *exec.Cmd
	// metricsPort is where this worker serves /metrics (9101, 9102, ...).
	metricsPort int
}

type Demo struct {
	db     *sql.DB
	dsn    string
	bin    string
	rate   int
	logger *slog.Logger

	mu        sync.Mutex
	workers   []*worker
	lastChaos time.Time
	check     *checker.Report
	checkedAt time.Time
}

// New prepares a demo fleet of n copies of the worker binary at bin,
// enqueueing about rate jobs per second.
func New(db *sql.DB, dsn, bin string, n, rate int, logger *slog.Logger) *Demo {
	d := &Demo{db: db, dsn: dsn, bin: bin, rate: rate, logger: logger}
	for i := range n {
		d.workers = append(d.workers, &worker{Name: fmt.Sprintf("worker-%d", i+1), metricsPort: 9101 + i})
	}
	return d
}

// Run supervises the fleet and generates load until ctx ends.
func (d *Demo) Run(ctx context.Context) {
	for _, w := range d.workers {
		go d.supervise(ctx, w)
	}
	go d.prune(ctx)
	d.load(ctx)
}

func (d *Demo) supervise(ctx context.Context, w *worker) {
	for ctx.Err() == nil {
		cmd := exec.CommandContext(ctx, d.bin)
		cmd.Env = append(os.Environ(), "DATABASE_URL="+d.dsn, "QUEUE="+Queue, "OWNER="+w.Name,
			fmt.Sprintf("METRICS_ADDR=:%d", w.metricsPort))
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			d.logger.Error("demo worker start", "worker", w.Name, "err", err)
		} else {
			d.mu.Lock()
			w.cmd, w.PID, w.Alive = cmd, cmd.Process.Pid, true
			d.mu.Unlock()
			_ = cmd.Wait()
			d.mu.Lock()
			w.Alive = false
			d.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(restartDelay):
		}
		d.mu.Lock()
		w.Restarts++
		d.mu.Unlock()
	}
}

// load enqueues a mix that exercises the interesting paths: priorities,
// delays, flaky handlers that retry and dead-letter, and two-step workflows.
func (d *Demo) load(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for range d.rate {
			p := queue.EnqueueParams{Queue: Queue, Kind: "work", Priority: rand.IntN(3), MaxAttempts: 3}
			failRate := 0.0
			if rand.Float64() < 0.15 {
				failRate = 0.6
			}
			if rand.Float64() < 0.1 {
				p.Delay = time.Duration(2+rand.IntN(8)) * time.Second
			}
			p.Args, _ = json.Marshal(map[string]any{"ms": 100 + rand.IntN(500), "fail_rate": failRate})
			spanCtx, span := telemetry.Tracer.Start(ctx, "demo.enqueue")
			p.TraceContext = telemetry.Inject(spanCtx)
			id, _, err := queue.Enqueue(spanCtx, d.db, p)
			span.End()
			if err != nil {
				if ctx.Err() == nil {
					d.logger.Error("demo enqueue", "err", err)
				}
				continue
			}
			if rand.Float64() < 0.05 {
				p := queue.EnqueueParams{Queue: Queue, Kind: "work", Args: json.RawMessage(`{"ms":200}`), DependsOn: []int64{id}}
				if _, _, err := queue.Enqueue(ctx, d.db, p); err != nil && ctx.Err() == nil {
					d.logger.Error("demo enqueue child", "err", err)
				}
			}
		}
	}
}

// prune keeps a long-running demo's ledger bounded; the jobs it refers to
// are archived by maintenance after a day.
func (d *Demo) prune(ctx context.Context) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := d.db.ExecContext(ctx, `DELETE FROM ledger WHERE occurred_at < now() - interval '1 day'`); err != nil {
				d.logger.Error("demo prune", "err", err)
			}
		}
	}
}

var errCooldown = errors.New("chaos cooldown: one kill every 5 seconds")

// Chaos SIGKILLs a random live worker.
func (d *Demo) Chaos() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.lastChaos) < chaosCooldown {
		return "", errCooldown
	}
	var alive []*worker
	for _, w := range d.workers {
		if w.Alive && w.cmd != nil {
			alive = append(alive, w)
		}
	}
	if len(alive) == 0 {
		return "", errors.New("no live workers")
	}
	w := alive[rand.IntN(len(alive))]
	if err := w.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		return "", err
	}
	w.Kills++
	d.lastChaos = time.Now()
	d.logger.Warn("chaos: SIGKILL", "worker", w.Name, "pid", w.PID)
	return w.Name, nil
}

// Register mounts the demo endpoints on mux.
func (d *Demo) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /demo/workers", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		out := make([]worker, len(d.workers))
		for i, wk := range d.workers {
			out[i] = *wk
		}
		d.mu.Unlock()
		writeJSON(w, http.StatusOK, out)
	})
	mux.HandleFunc("POST /demo/chaos", func(w http.ResponseWriter, r *http.Request) {
		name, err := d.Chaos()
		if errors.Is(err, errCooldown) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"killed": name})
	})
	mux.HandleFunc("GET /demo/check", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		rep, fresh := d.check, time.Since(d.checkedAt) < checkCacheTTL
		d.mu.Unlock()
		if !fresh {
			var err error
			if rep, err = checker.Run(r.Context(), d.db); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			d.mu.Lock()
			d.check, d.checkedAt = rep, time.Now()
			d.mu.Unlock()
		}
		writeJSON(w, http.StatusOK, rep)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
