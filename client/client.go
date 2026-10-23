// Package client is the typed Go SDK for the Conveyor API.
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
)

type Client struct {
	c     conveyorv1.ConveyorClient
	conn  *grpc.ClientConn
	token string
}

type Option func(*config)

type config struct {
	token string
	tls   *tls.Config
}

// WithToken sends "authorization: Bearer <token>" on every call.
func WithToken(token string) Option { return func(c *config) { c.token = token } }

// WithTLS enables transport security; without it the connection is plaintext.
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.tls = cfg } }

// New dials addr. Calls carry the caller's trace context, so the enqueue
// span and the job's execution span join one trace.
func New(addr string, opts ...Option) (*Client, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	creds := insecure.NewCredentials()
	if cfg.tls != nil {
		creds = credentials.NewTLS(cfg.tls)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		return nil, err
	}
	return &Client{c: conveyorv1.NewConveyorClient(conn), conn: conn, token: cfg.token}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) ctx(ctx context.Context) context.Context {
	if c.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

// EnqueueOptions are the optional fields of a job; the zero value enqueues
// an immediately available job with default priority and attempts.
type EnqueueOptions struct {
	Priority       int32
	MaxAttempts    int32
	Delay          time.Duration
	RunAt          time.Time
	IdempotencyKey string
	DependsOn      []int64
}

// Enqueue marshals args to JSON and creates a job. deduped reports that the
// idempotency key already existed in this queue and no job was created.
func (c *Client) Enqueue(ctx context.Context, queueName, kind string, args any, o EnqueueOptions) (id int64, deduped bool, err error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return 0, false, err
	}
	req := &conveyorv1.EnqueueRequest{
		Queue: queueName, Kind: kind, Args: string(raw), Priority: o.Priority, MaxAttempts: o.MaxAttempts,
		DelaySeconds: int32(o.Delay / time.Second), IdempotencyKey: o.IdempotencyKey, DependsOn: o.DependsOn,
	}
	if !o.RunAt.IsZero() {
		req.RunAt = o.RunAt.UTC().Format(time.RFC3339)
	}
	r, err := c.c.Enqueue(c.ctx(ctx), req)
	if err != nil {
		return 0, false, err
	}
	return r.Id, r.Deduped, nil
}

func (c *Client) GetJob(ctx context.Context, id int64) (*conveyorv1.Job, error) {
	return c.c.GetJob(c.ctx(ctx), &conveyorv1.GetJobRequest{Id: id})
}

func (c *Client) ListJobs(ctx context.Context, queueName, state string, limit int32) ([]*conveyorv1.Job, error) {
	r, err := c.c.ListJobs(c.ctx(ctx), &conveyorv1.ListJobsRequest{Queue: queueName, State: state, Limit: limit})
	if err != nil {
		return nil, err
	}
	return r.Jobs, nil
}

func (c *Client) CancelJob(ctx context.Context, id int64) error {
	_, err := c.c.CancelJob(c.ctx(ctx), &conveyorv1.CancelJobRequest{Id: id})
	return err
}

func (c *Client) RetryJob(ctx context.Context, id int64) error {
	_, err := c.c.RetryJob(c.ctx(ctx), &conveyorv1.RetryJobRequest{Id: id})
	return err
}

func (c *Client) DiscardJob(ctx context.Context, id int64) error {
	_, err := c.c.DiscardJob(c.ctx(ctx), &conveyorv1.DiscardJobRequest{Id: id})
	return err
}

func (c *Client) ListQueues(ctx context.Context) ([]*conveyorv1.QueueInfo, error) {
	r, err := c.c.ListQueues(c.ctx(ctx), &conveyorv1.ListQueuesRequest{})
	if err != nil {
		return nil, err
	}
	return r.Queues, nil
}

func (c *Client) CreateCron(ctx context.Context, queueName, kind string, args any, spec string) (*conveyorv1.CronSchedule, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return c.c.CreateCron(c.ctx(ctx), &conveyorv1.CreateCronRequest{Queue: queueName, Kind: kind, Args: string(raw), Spec: spec})
}

func (c *Client) Stats(ctx context.Context, window time.Duration) (*conveyorv1.Stats, error) {
	return c.c.GetStats(c.ctx(ctx), &conveyorv1.GetStatsRequest{WindowSeconds: int32(window / time.Second)})
}
