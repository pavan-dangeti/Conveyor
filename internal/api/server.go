// Package api implements the gRPC service; the HTTP/JSON gateway is
// generated from the same proto.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/telemetry"
)

const (
	MaxArgsBytes  = 64 * 1024
	maxDependsOn  = 100
	maxNameLength = 128
)

type Server struct {
	conveyorv1.UnimplementedConveyorServer
	db *sql.DB
}

func NewServer(db *sql.DB) *Server { return &Server{db: db} }

func (s *Server) Enqueue(ctx context.Context, r *conveyorv1.EnqueueRequest) (*conveyorv1.EnqueueResponse, error) {
	switch {
	case r.Queue == "" || r.Kind == "":
		return nil, status.Error(codes.InvalidArgument, "queue and kind are required")
	case len(r.Queue) > maxNameLength || len(r.Kind) > maxNameLength || len(r.IdempotencyKey) > maxNameLength:
		return nil, status.Error(codes.InvalidArgument, "queue, kind and idempotency_key are limited to 128 bytes")
	case len(r.Args) > MaxArgsBytes:
		return nil, status.Error(codes.InvalidArgument, "args exceed 64 KiB")
	case r.MaxAttempts < 0 || r.MaxAttempts > 64:
		return nil, status.Error(codes.InvalidArgument, "max_attempts must be 0..64")
	case r.DelaySeconds < 0:
		return nil, status.Error(codes.InvalidArgument, "negative delay")
	case len(r.DependsOn) > maxDependsOn:
		return nil, status.Error(codes.InvalidArgument, "at most 100 dependencies")
	}
	args := r.Args
	if args == "" {
		args = "{}"
	}
	if !json.Valid([]byte(args)) {
		return nil, status.Error(codes.InvalidArgument, "args must be valid JSON")
	}
	var runAt time.Time
	if r.RunAt != "" {
		t, err := time.Parse(time.RFC3339, r.RunAt)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "run_at must be RFC 3339")
		}
		runAt = t
	}
	var key *string
	if r.IdempotencyKey != "" {
		key = &r.IdempotencyKey
	}
	ctx, span := telemetry.Tracer.Start(ctx, "conveyor.enqueue")
	defer span.End()
	id, deduped, err := queue.Enqueue(ctx, s.db, queue.EnqueueParams{
		Queue: r.Queue, Kind: r.Kind, Args: json.RawMessage(args), Priority: int(r.Priority),
		MaxAttempts: int(r.MaxAttempts), Delay: time.Duration(r.DelaySeconds) * time.Second,
		RunAt: runAt, IdempotencyKey: key, DependsOn: r.DependsOn,
		TraceContext: telemetry.Inject(ctx),
	})
	if err != nil {
		span.RecordError(err)
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}
	span.SetAttributes(attribute.Int64("conveyor.job_id", id), attribute.Bool("conveyor.deduped", deduped))
	return &conveyorv1.EnqueueResponse{Id: id, Deduped: deduped}, nil
}

const jobColumns = `id, queue, kind, args, state, priority, attempt, max_attempts, run_at,
  lease_version, created_at, COALESCE(error, ''), COALESCE(lease_owner, ''),
  ARRAY(SELECT depends_on FROM job_dependencies WHERE job_id = jobs.id ORDER BY depends_on)`

func scanJob(row interface{ Scan(...any) error }) (*conveyorv1.Job, error) {
	var j conveyorv1.Job
	var runAt, createdAt time.Time
	var deps []int64
	if err := row.Scan(&j.Id, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority, &j.Attempt, &j.MaxAttempts,
		&runAt, &j.LeaseVersion, &createdAt, &j.Error, &j.LeaseOwner, &deps); err != nil {
		return nil, err
	}
	j.RunAt = runAt.UTC().Format(time.RFC3339)
	j.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	j.DependsOn = deps
	return &j, nil
}

func (s *Server) GetJob(ctx context.Context, r *conveyorv1.GetJobRequest) (*conveyorv1.Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, r.Id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return j, nil
}

func (s *Server) ListJobs(ctx context.Context, r *conveyorv1.ListJobsRequest) (*conveyorv1.ListJobsResponse, error) {
	limit := r.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs
WHERE ($1::text = '' OR queue = $1) AND ($2::text = '' OR state = $2)
ORDER BY id DESC LIMIT $3`, r.Queue, r.State, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := &conveyorv1.ListJobsResponse{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "scan: %v", err)
		}
		out.Jobs = append(out.Jobs, j)
	}
	return out, rows.Err()
}

func (s *Server) CancelJob(ctx context.Context, r *conveyorv1.CancelJobRequest) (*conveyorv1.StatusResponse, error) {
	return s.operator(ctx, queue.Cancel, r.Id)
}

func (s *Server) RetryJob(ctx context.Context, r *conveyorv1.RetryJobRequest) (*conveyorv1.StatusResponse, error) {
	return s.operator(ctx, queue.Retry, r.Id)
}

func (s *Server) DiscardJob(ctx context.Context, r *conveyorv1.DiscardJobRequest) (*conveyorv1.StatusResponse, error) {
	return s.operator(ctx, queue.Discard, r.Id)
}

func (s *Server) operator(ctx context.Context, op func(context.Context, *sql.DB, int64) error, id int64) (*conveyorv1.StatusResponse, error) {
	if err := op(ctx, s.db, id); err != nil {
		if errors.Is(err, queue.ErrNotFound) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &conveyorv1.StatusResponse{Ok: true}, nil
}

func (s *Server) ListQueues(ctx context.Context, _ *conveyorv1.ListQueuesRequest) (*conveyorv1.ListQueuesResponse, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT q.name, q.concurrency, COALESCE(q.rate_limit_per_sec, 0), j.state, count(j.id)
FROM queues q LEFT JOIN jobs j ON j.queue = q.name
GROUP BY q.name, q.concurrency, q.rate_limit_per_sec, j.state
ORDER BY q.name`)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list queues: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := &conveyorv1.ListQueuesResponse{}
	var cur *conveyorv1.QueueInfo
	for rows.Next() {
		var name string
		var state sql.NullString
		var conc int32
		var rate float64
		var n int64
		if err := rows.Scan(&name, &conc, &rate, &state, &n); err != nil {
			return nil, status.Errorf(codes.Internal, "scan: %v", err)
		}
		if cur == nil || cur.Name != name {
			cur = &conveyorv1.QueueInfo{Name: name, Concurrency: conc, RateLimitPerSec: rate, DepthByState: map[string]int64{}}
			out.Queues = append(out.Queues, cur)
		}
		if state.Valid {
			cur.DepthByState[state.String] = n
		}
	}
	return out, rows.Err()
}

func (s *Server) CreateCron(ctx context.Context, r *conveyorv1.CreateCronRequest) (*conveyorv1.CronSchedule, error) {
	if r.Queue == "" || r.Kind == "" || r.Spec == "" {
		return nil, status.Error(codes.InvalidArgument, "queue, kind, spec required")
	}
	args := r.Args
	if args == "" {
		args = "{}"
	}
	if len(args) > MaxArgsBytes || !json.Valid([]byte(args)) {
		return nil, status.Error(codes.InvalidArgument, "args must be valid JSON under 64 KiB")
	}
	cs, err := cron.ParseStandard(r.Spec)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad spec: %v", err)
	}
	next := cs.Next(time.Now())
	var id int64
	err = s.db.QueryRowContext(ctx,
		`INSERT INTO cron_schedules (queue, kind, args, spec, next_run_at) VALUES ($1,$2,$3,$4,$5) RETURNING id`,
		r.Queue, r.Kind, []byte(args), r.Spec, next).Scan(&id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create cron: %v", err)
	}
	return &conveyorv1.CronSchedule{Id: id, Queue: r.Queue, Kind: r.Kind, Args: args, Spec: r.Spec,
		NextRunAt: next.UTC().Format(time.RFC3339), Enabled: true}, nil
}

// GetStats reports throughput and enqueue-to-start latency over a window
// (default 60 s, max 1 h).
func (s *Server) GetStats(ctx context.Context, r *conveyorv1.GetStatsRequest) (*conveyorv1.Stats, error) {
	window := time.Duration(r.WindowSeconds) * time.Second
	if r.WindowSeconds <= 0 || r.WindowSeconds > 3600 {
		window = time.Minute
	}
	st, err := queue.ReadStats(ctx, s.db, window)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stats: %v", err)
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return &conveyorv1.Stats{
		WindowSeconds:     int32(window.Seconds()),
		CompletedPerSec:   st.CompletedPerSec,
		StartLatencyP50Ms: ms(st.StartP50),
		StartLatencyP95Ms: ms(st.StartP95),
		StartLatencyP99Ms: ms(st.StartP99),
		Retries:           st.Retries,
		DeadLetter:        st.DeadLetter,
		LeaseExpirations:  st.LeaseExpirations,
	}, nil
}
