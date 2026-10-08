// Package api implements the gRPC service and HTTP gateway.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

const maxArgsBytes = 64 * 1024

type Server struct {
	conveyorv1.UnimplementedConveyorServer
	db *sql.DB
}

func NewServer(db *sql.DB) *Server { return &Server{db: db} }

func (s *Server) Enqueue(ctx context.Context, r *conveyorv1.EnqueueRequest) (*conveyorv1.EnqueueResponse, error) {
	if r.Queue == "" || r.Kind == "" {
		return nil, status.Error(codes.InvalidArgument, "queue and kind are required")
	}
	if len(r.Args) > maxArgsBytes {
		return nil, status.Error(codes.InvalidArgument, "args too large")
	}
	if len(r.Args) > 0 && !json.Valid(r.Args) {
		return nil, status.Error(codes.InvalidArgument, "args must be valid JSON")
	}
	if r.MaxAttempts > 64 {
		return nil, status.Error(codes.InvalidArgument, "max_attempts too large")
	}
	if r.DelaySeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "negative delay")
	}
	var key *string
	if r.IdempotencyKey != "" {
		key = &r.IdempotencyKey
	}
	id, deduped, err := queue.Enqueue(ctx, s.db, queue.EnqueueParams{
		Queue: r.Queue, Kind: r.Kind, Args: r.Args, Priority: int(r.Priority),
		MaxAttempts: int(r.MaxAttempts), Delay: time.Duration(r.DelaySeconds) * time.Second,
		IdempotencyKey: key,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}
	return &conveyorv1.EnqueueResponse{Id: id, Deduped: deduped}, nil
}

func (s *Server) GetJob(ctx context.Context, r *conveyorv1.GetJobRequest) (*conveyorv1.Job, error) {
	var j conveyorv1.Job
	var runAt, createdAt time.Time
	var priority int32
	err := s.db.QueryRowContext(ctx,
		`SELECT id, queue, kind, args, state, priority, attempt, max_attempts, run_at, lease_version, created_at FROM jobs WHERE id=$1`, r.Id,
	).Scan(&j.Id, &j.Queue, &j.Kind, &j.Args, &j.State, &priority, &j.Attempt, &j.MaxAttempts, &runAt, &j.LeaseVersion, &createdAt)
	j.Priority = priority
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	j.RunAt = runAt.UTC().Format(time.RFC3339)
	j.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	return &j, nil
}

func (s *Server) ListJobs(ctx context.Context, r *conveyorv1.ListJobsRequest) (*conveyorv1.ListJobsResponse, error) {
	limit := r.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT id, queue, kind, COALESCE(args, '{}'::jsonb), state, priority, attempt, max_attempts, run_at, lease_version, created_at FROM jobs WHERE 1=1`
	var args []any
	i := 1
	if r.Queue != "" {
		q += fmt.Sprintf(` AND queue=$%d`, i)
		args = append(args, r.Queue)
		i++
	}
	if r.State != "" {
		q += fmt.Sprintf(` AND state=$%d`, i)
		args = append(args, r.State)
		i++
	}
	q += fmt.Sprintf(` ORDER BY id DESC LIMIT $%d`, i)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := &conveyorv1.ListJobsResponse{}
	for rows.Next() {
		var j conveyorv1.Job
		var runAt, createdAt time.Time
		if err := rows.Scan(&j.Id, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority, &j.Attempt, &j.MaxAttempts, &runAt, &j.LeaseVersion, &createdAt); err != nil {
			return nil, status.Errorf(codes.Internal, "scan: %v", err)
		}
		j.RunAt = runAt.UTC().Format(time.RFC3339)
		j.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		out.Jobs = append(out.Jobs, &j)
	}
	return out, rows.Err()
}

func (s *Server) CancelJob(ctx context.Context, r *conveyorv1.CancelJobRequest) (*conveyorv1.StatusResponse, error) {
	if err := queue.Cancel(ctx, s.db, r.Id); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &conveyorv1.StatusResponse{Ok: true}, nil
}

func (s *Server) RetryJob(ctx context.Context, r *conveyorv1.RetryJobRequest) (*conveyorv1.StatusResponse, error) {
	if err := queue.Retry(ctx, s.db, r.Id); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &conveyorv1.StatusResponse{Ok: true}, nil
}

func (s *Server) ListQueues(ctx context.Context, _ *conveyorv1.ListQueuesRequest) (*conveyorv1.ListQueuesResponse, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT q.name, q.concurrency, COALESCE(q.rate_limit_per_sec, 0), j.state, count(*)
FROM queues q LEFT JOIN jobs j ON j.queue = q.name
GROUP BY q.name, q.concurrency, q.rate_limit_per_sec, j.state
ORDER BY q.name`)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list queues: %v", err)
	}
	defer func() { _ = rows.Close() }()
	byName := map[string]*conveyorv1.QueueInfo{}
	for rows.Next() {
		var name, state sql.NullString
		var conc int32
		var rate float64
		var n int64
		if err := rows.Scan(&name, &conc, &rate, &state, &n); err != nil {
			return nil, status.Errorf(codes.Internal, "scan: %v", err)
		}
		qi, ok := byName[name.String]
		if !ok {
			qi = &conveyorv1.QueueInfo{Name: name.String, Concurrency: conc, RateLimitPerSec: rate, DepthByState: map[string]int64{}}
			byName[name.String] = qi
		}
		if state.Valid {
			qi.DepthByState[state.String] = n
		}
	}
	out := &conveyorv1.ListQueuesResponse{}
	for _, qi := range byName {
		out.Queues = append(out.Queues, qi)
	}
	return out, rows.Err()
}

func (s *Server) CreateCron(ctx context.Context, r *conveyorv1.CreateCronRequest) (*conveyorv1.CronSchedule, error) {
	if r.Queue == "" || r.Kind == "" || r.Spec == "" {
		return nil, status.Error(codes.InvalidArgument, "queue, kind, spec required")
	}
	cs, err := cron.ParseStandard(r.Spec)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad spec: %v", err)
	}
	next := cs.Next(time.Now())
	var id int64
	err = s.db.QueryRowContext(ctx,
		`INSERT INTO cron_schedules (queue, kind, args, spec, next_run_at) VALUES ($1,$2,$3,$4,$5) RETURNING id`,
		r.Queue, r.Kind, r.Args, r.Spec, next).Scan(&id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create cron: %v", err)
	}
	return &conveyorv1.CronSchedule{Id: id, Queue: r.Queue, Kind: r.Kind, Args: r.Args, Spec: r.Spec, NextRunAt: next.UTC().Format(time.RFC3339), Enabled: true}, nil
}
