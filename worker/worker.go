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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/pavan-dangeti/Conveyor/internal/metrics"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/telemetry"
)

// Job is a claimed job: its arguments, attempt number and the fencing token
// (LeaseVersion) that guards every write about it.
type Job = queue.Job

// Handler runs one job. Its context is cancelled when the worker shuts down
// or loses the job's lease; a handler must then stop without applying
// effects, because another worker may already own the job.
type Handler func(ctx context.Context, job Job) error

type Worker struct {
	db             *sql.DB
	dsn            string
	queue          string
	owner          string
	lease          time.Duration
	heartbeatEvery time.Duration
	slots          int
	handlerCommits bool
	pollMin        time.Duration
	pollMax        time.Duration
	budget         time.Duration
	logger         *slog.Logger
	handlers       map[string]Handler
	unbatched      bool
	completions    chan completion
}

type completion struct{ id, version int64 }

const (
	completeBatchMax  = 64
	completeBatchWait = 2 * time.Millisecond
)

type Option func(*Worker)

func WithLease(d time.Duration) Option  { return func(w *Worker) { w.lease = d } }
func WithBudget(d time.Duration) Option { return func(w *Worker) { w.budget = d } }
func WithLogger(l *slog.Logger) Option  { return func(w *Worker) { w.logger = l } }

// WithBatch sets how many jobs run concurrently; dequeue claims at most the
// number of free slots, so no job sits leased while waiting for a slot.
func WithBatch(n int) Option { return func(w *Worker) { w.slots = max(n, 1) } }

// WithHeartbeatInterval overrides the default of lease/3.
func WithHeartbeatInterval(d time.Duration) Option { return func(w *Worker) { w.heartbeatEvery = d } }

// WithHandlerCommitsOutcome is for handlers that commit their side effect
// and the completion in one transaction via queue.CompleteTx; the worker
// then never writes the outcome itself on success.
func WithHandlerCommitsOutcome() Option { return func(w *Worker) { w.handlerCommits = true } }

// WithoutBatchedCompletion commits each completion on its own instead of
// grouping completions that finish within 2 ms into one fenced UPDATE.
// It exists to reproduce the benchmark's before/after comparison.
func WithoutBatchedCompletion() Option { return func(w *Worker) { w.unbatched = true } }

func New(db *sql.DB, dsn, queueName, owner string, opts ...Option) *Worker {
	w := &Worker{
		db: db, dsn: dsn, queue: queueName, owner: owner,
		lease: queue.DefaultLease, slots: 4, budget: 25 * time.Second,
		pollMin: 200 * time.Millisecond, pollMax: 1 * time.Second,
		logger: slog.Default(), handlers: map[string]Handler{},
	}
	for _, o := range opts {
		o(w)
	}
	if w.heartbeatEvery <= 0 {
		w.heartbeatEvery = w.lease / 3
	}
	return w
}

func (w *Worker) Handle(kind string, h Handler) { w.handlers[kind] = h }

// CompleteTx marks j completed inside tx, guarded by this worker's fencing
// token. A handler that writes its side effect in tx and then calls
// CompleteTx gets exactly-once effects: the effect and the completion commit
// together, and a worker that has lost the lease commits neither. Use it
// with WithHandlerCommitsOutcome.
func (w *Worker) CompleteTx(ctx context.Context, tx *sql.Tx, j Job) error {
	return queue.CompleteTx(ctx, tx, j.ID, w.owner, j.LeaseVersion)
}

// Run dequeues and executes until ctx is cancelled, then stops dequeuing
// and lets in-flight jobs finish for up to the shutdown budget. Jobs still
// running after that are cancelled; they keep their leases until the
// reaper expires them and another worker retries them.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 {
		return errors.New("no handlers registered")
	}
	notify := make(chan struct{}, 1)
	go w.listen(ctx, notify)
	free := make(chan struct{}, w.slots)
	for range w.slots {
		free <- struct{}{}
	}
	var wg sync.WaitGroup
	completerDone := make(chan struct{})
	if w.handlerCommits || w.unbatched {
		close(completerDone)
	} else {
		w.completions = make(chan completion, w.slots)
		go w.completer(completerDone)
	}
	// Handlers outlive a shutdown signal: they are cancelled only when the
	// shutdown budget runs out or their lease is lost, so in-flight work
	// finishes instead of being thrown away and redone after lease expiry.
	jobsCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()
	drain := func() error { return w.drain(&wg, completerDone, cancelJobs) }
	poll := w.pollMin
	for {
		n := w.acquire(ctx, free)
		if n == 0 {
			return drain()
		}
		jobs, err := queue.Dequeue(ctx, w.db, w.queue, w.owner, w.lease, n)
		for range n - len(jobs) {
			free <- struct{}{}
		}
		if err != nil && ctx.Err() == nil {
			w.logger.Error("dequeue", "err", err)
		}
		for _, j := range jobs {
			wg.Add(1)
			go func(j queue.Job) {
				defer wg.Done()
				defer func() { free <- struct{}{} }()
				w.execute(jobsCtx, j)
			}(j)
		}
		if ctx.Err() != nil {
			return drain()
		}
		if len(jobs) == n {
			poll = w.pollMin
			continue
		}
		select {
		case <-ctx.Done():
			return drain()
		case <-notify:
			poll = w.pollMin
		case <-time.After(poll):
			poll = min(poll*2, w.pollMax)
		}
	}
}

// acquire blocks for at least one free slot and then takes every other
// slot that is free right now. It returns 0 when ctx is cancelled.
func (w *Worker) acquire(ctx context.Context, free chan struct{}) int {
	select {
	case <-ctx.Done():
		return 0
	case <-free:
	}
	n := 1
	for {
		select {
		case <-free:
			n++
		default:
			return n
		}
	}
}

// drain waits for in-flight handlers, then for the completer to flush.
// Past the budget it cancels the handlers and gives up; their jobs are
// reaped and retried, and the fencing token rejects any late write.
func (w *Worker) drain(wg *sync.WaitGroup, completerDone chan struct{}, cancelJobs context.CancelFunc) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		if w.completions != nil {
			close(w.completions)
		}
		<-completerDone
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(w.budget):
		cancelJobs()
		w.logger.Warn("shutdown budget exceeded; in-flight jobs will be reaped")
	}
	return nil
}

// completer groups completions that arrive within completeBatchWait into a
// single CompleteMany, so N finished jobs cost one commit instead of N.
func (w *Worker) completer(done chan struct{}) {
	defer close(done)
	for c := range w.completions {
		ids, versions := []int64{c.id}, []int64{c.version}
		timer := time.NewTimer(completeBatchWait)
	collect:
		for len(ids) < completeBatchMax {
			select {
			case c, ok := <-w.completions:
				if !ok {
					break collect
				}
				ids, versions = append(ids, c.id), append(versions, c.version)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		completed, err := queue.CompleteMany(ctx, w.db, w.owner, ids, versions)
		cancel()
		if err != nil {
			w.logger.Error("complete batch", "jobs", len(ids), "err", err)
		} else if len(completed) < len(ids) {
			w.logger.Warn("stale completions rejected by fencing", "rejected", len(ids)-len(completed))
		}
	}
}

func (w *Worker) execute(ctx context.Context, j queue.Job) {
	ctx, span := telemetry.Tracer.Start(telemetry.Extract(ctx, j.TraceContext), "conveyor.execute "+j.Kind,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.Int64("conveyor.job_id", j.ID), attribute.Int("conveyor.attempt", j.Attempt),
			attribute.Int64("conveyor.fencing_token", j.LeaseVersion)))
	defer span.End()

	h, ok := w.handlers[j.Kind]
	if !ok {
		w.fail(j, fmt.Errorf("no handler for kind %q", j.Kind))
		return
	}
	start := time.Now()
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.heartbeat(jobCtx, cancel, j)
	err := h(jobCtx, j)
	outcome := "ok"
	if err != nil {
		outcome = "error"
		span.SetStatus(codes.Error, err.Error())
	}
	metrics.JobSeconds.WithLabelValues(j.Kind, outcome).Observe(time.Since(start).Seconds())
	switch {
	case jobCtx.Err() != nil:
		// Shutdown or lost lease: the job is no longer ours to finish.
	case err != nil:
		w.fail(j, err)
	case w.handlerCommits:
	case w.completions != nil:
		w.completions <- completion{j.ID, j.LeaseVersion}
	default:
		if cerr := queue.Complete(context.Background(), w.db, j.ID, w.owner, j.LeaseVersion); cerr != nil {
			w.logger.Error("complete", "job", j.ID, "err", cerr)
		}
	}
}

func (w *Worker) fail(j queue.Job, cause error) {
	if err := queue.Fail(context.Background(), w.db, j.ID, w.owner, j.LeaseVersion, cause.Error(), j.Attempt, j.MaxAttempts); err != nil {
		w.logger.Error("fail", "job", j.ID, "err", err)
	}
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, j queue.Job) {
	t := time.NewTicker(w.heartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := queue.Heartbeat(context.Background(), w.db, j.ID, w.owner, j.LeaseVersion, w.lease)
			if errors.Is(err, queue.ErrStaleLease) {
				w.logger.Warn("lost lease", "job", j.ID, "fencing_token", j.LeaseVersion)
				cancel()
				return
			}
			if err != nil {
				w.logger.Error("heartbeat", "job", j.ID, "err", err)
			}
		}
	}
}

// listen wakes the dequeue loop on NOTIFY jobs_available and reconnects
// after connection loss; polling covers any notification missed meanwhile.
func (w *Worker) listen(ctx context.Context, notify chan<- struct{}) {
	for ctx.Err() == nil {
		if err := w.listenOnce(ctx, notify); err != nil && ctx.Err() == nil {
			w.logger.Warn("listen", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func (w *Worker) listenOnce(ctx context.Context, notify chan<- struct{}) error {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN jobs_available"); err != nil {
		return err
	}
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		select {
		case notify <- struct{}{}:
		default:
		}
	}
}
