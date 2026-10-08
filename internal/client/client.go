// Package client is the typed Go SDK for the Conveyor API.
package client

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
)

type Client struct {
	c     conveyorv1.ConveyorClient
	conn  *grpc.ClientConn
	token string
}

func New(addr, token string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{c: conveyorv1.NewConveyorClient(conn), conn: conn, token: token}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) ctx(ctx context.Context) context.Context {
	if c.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

func (c *Client) Enqueue(ctx context.Context, queueName, kind string, args []byte, priority int32, maxAttempts int32, delay time.Duration, idempotencyKey string) (int64, bool, error) {
	r, err := c.c.Enqueue(c.ctx(ctx), &conveyorv1.EnqueueRequest{
		Queue: queueName, Kind: kind, Args: string(args), Priority: priority,
		MaxAttempts: maxAttempts, DelaySeconds: int32(delay.Seconds()), IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return 0, false, err
	}
	return r.Id, r.Deduped, nil
}

func (c *Client) GetJob(ctx context.Context, id int64) (*conveyorv1.Job, error) {
	return c.c.GetJob(c.ctx(ctx), &conveyorv1.GetJobRequest{Id: id})
}

func (c *Client) ListJobs(ctx context.Context, queueName, state string, limit int32) (*conveyorv1.ListJobsResponse, error) {
	return c.c.ListJobs(c.ctx(ctx), &conveyorv1.ListJobsRequest{Queue: queueName, State: state, Limit: limit})
}

func (c *Client) CancelJob(ctx context.Context, id int64) error {
	_, err := c.c.CancelJob(c.ctx(ctx), &conveyorv1.CancelJobRequest{Id: id})
	return err
}

func (c *Client) RetryJob(ctx context.Context, id int64) error {
	_, err := c.c.RetryJob(c.ctx(ctx), &conveyorv1.RetryJobRequest{Id: id})
	return err
}

func (c *Client) ListQueues(ctx context.Context) (*conveyorv1.ListQueuesResponse, error) {
	return c.c.ListQueues(c.ctx(ctx), &conveyorv1.ListQueuesRequest{})
}

func (c *Client) CreateCron(ctx context.Context, queueName, kind string, args []byte, spec string) (*conveyorv1.CronSchedule, error) {
	return c.c.CreateCron(c.ctx(ctx), &conveyorv1.CreateCronRequest{Queue: queueName, Kind: kind, Args: string(args), Spec: spec})
}
