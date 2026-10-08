// Package worker provides the worker library: register handlers by job kind,
// dequeue with leasing, heartbeats, and graceful shutdown.
package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

type Handler func(ctx context.Context, job queue.Job) error

type Worker struct {
	db       *sql.DB
	dsn      string
	queue    string
	owner    string
	lease    time.Duration
	batch    int
	pollMin  time.Duration
	pollMax  time.Duration
	budget   time.Duration
	logger   *slog.Logger
	handlers map[string]Handler
}

type Option func(*Worker)

func WithLease(d time.Duration) Option  { return func(w *Worker) { w.lease = d } }
func WithBatch(n int) Option            { return func(w *Worker) { w.batch = n } }
func WithBudget(d time.Duration) Option { return func(w *Worker) { w.budget = d } }
func WithLogger(l *slog.Logger) Option  { return func(w *Worker) { w.logger = l } }
func WithPoll(min, max time.Duration) Option {
	return func(w *Worker) { w.pollMin, w.pollMax = min, max }
}

func New(db *sql.DB, dsn, queueName, owner string, opts ...Option) *Worker {
	w := &Worker{
		db: db, dsn: dsn, queue: queueName, owner: owner,
		lease: queue.DefaultLease, batch: 4, budget: 25 * time.Second,
		pollMin: 200 * time.Millisecond, pollMax: 1 * time.Second,
		logger: slog.Default(), handlers: map[string]Handler{},
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

func (w *Worker) Handle(kind string, h Handler) { w.handlers[kind] = h }

// Run dequeues and executes until ctx is cancelled, then drains in-flight
// jobs within the shutdown budget. In-flight jobs that do not finish before
// the budget keep their leases, which the reaper will expire.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 {
		return errors.New("no handlers registered")
	}
	notify := make(chan struct{}, 1)
	go w.listen(ctx, notify)
	poll := w.pollMin
	var wg sync.WaitGroup
	for {
		jobs, err := queue.Dequeue(ctx, w.db, w.queue, w.owner, w.lease, w.batch)
		if err != nil {
			if ctx.Err() != nil {
				return w.drain(&wg)
			}
			w.logger.Error("dequeue", "err", err)
		} else {
			for _, j := range jobs {
				if h, ok := w.handlers[j.Kind]; ok {
					wg.Add(1)
					go func(j queue.Job, h Handler) {
						defer wg.Done()
						w.execute(ctx, j, h)
					}(j, h)
				} else {
					w.logger.Error("no handler for kind", "kind", j.Kind)
					_ = queue.Fail(ctx, w.db, j.ID, w.owner, j.LeaseVersion,
						fmt.Sprintf("no handler for kind %q", j.Kind), j.Attempt, j.MaxAttempts)
				}
			}
		}
		if ctx.Err() != nil {
			return w.drain(&wg)
		}
		if len(jobs) == w.batch {
			poll = w.pollMin
			continue
		}
		select {
		case <-ctx.Done():
			return w.drain(&wg)
		case <-notify:
			poll = w.pollMin
		case <-time.After(poll):
			poll = min(poll*2, w.pollMax)
		}
	}
}

func (w *Worker) drain(wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.budget):
		w.logger.Warn("shutdown budget exceeded; in-flight jobs will be reaped")
	}
	return nil
}

func (w *Worker) execute(ctx context.Context, j queue.Job, h Handler) {
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.heartbeat(jobCtx, cancel, j)
	err := h(jobCtx, j)
	if jobCtx.Err() != nil {
		return
	}
	if err != nil {
		if ferr := queue.Fail(context.Background(), w.db, j.ID, w.owner, j.LeaseVersion, err.Error(), j.Attempt, j.MaxAttempts); ferr != nil {
			w.logger.Error("fail", "err", ferr)
		}
		return
	}
	if cerr := queue.Complete(context.Background(), w.db, j.ID, w.owner, j.LeaseVersion); cerr != nil {
		w.logger.Error("complete", "err", cerr)
	}
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, j queue.Job) {
	t := time.NewTicker(w.lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := queue.Heartbeat(context.Background(), w.db, j.ID, w.owner, j.LeaseVersion, w.lease); err != nil {
				if errors.Is(err, queue.ErrStaleLease) {
					w.logger.Warn("lost lease", "job", j.ID)
					cancel()
					return
				}
				w.logger.Error("heartbeat", "err", err)
			}
		}
	}
}

// listen wakes dequeue on jobs_available NOTIFY. Any listen error falls back
// to pure polling.
func (w *Worker) listen(ctx context.Context, notify chan<- struct{}) {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		w.logger.Error("listen connect", "err", err)
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN jobs_available"); err != nil {
		w.logger.Error("listen", "err", err)
		return
	}
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("wait notification", "err", err)
			return
		}
		select {
		case notify <- struct{}{}:
		default:
		}
	}
}
