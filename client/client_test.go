package client_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/client"
	"github.com/pavan-dangeti/Conveyor/internal/api"
	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
	"github.com/pavan-dangeti/Conveyor/internal/telemetry"
)

const token = "s3cret"

func serve(t *testing.T, cfg api.InterceptorConfig) (*sql.DB, string) {
	t.Helper()
	db, _ := pgtest.Start(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(api.Interceptor(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))))
	conveyorv1.RegisterConveyorServer(srv, api.NewServer(db))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return db, lis.Addr().String()
}

func dial(t *testing.T, addr string, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.New(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSDKEndToEnd(t *testing.T) {
	shutdown, err := telemetry.Setup(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()
	db, addr := serve(t, api.InterceptorConfig{Token: token})
	c := dial(t, addr, client.WithToken(token))
	ctx := context.Background()

	parent, deduped, err := c.Enqueue(ctx, "q1", "email", map[string]string{"to": "a@example.com"},
		client.EnqueueOptions{Priority: 5, MaxAttempts: 2, IdempotencyKey: "welcome-1"})
	if err != nil || deduped {
		t.Fatalf("enqueue: %v %v", err, deduped)
	}
	again, deduped, err := c.Enqueue(ctx, "q1", "email", map[string]string{"to": "a@example.com"},
		client.EnqueueOptions{IdempotencyKey: "welcome-1"})
	if err != nil || !deduped || again != parent {
		t.Fatalf("idempotent enqueue: id=%d deduped=%v err=%v", again, deduped, err)
	}
	child, _, err := c.Enqueue(ctx, "q1", "receipt", nil, client.EnqueueOptions{DependsOn: []int64{parent}})
	if err != nil {
		t.Fatal(err)
	}
	later, _, err := c.Enqueue(ctx, "q1", "reminder", nil, client.EnqueueOptions{RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	delayed, _, err := c.Enqueue(ctx, "q1", "reminder", nil, client.EnqueueOptions{Delay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	j, err := c.GetJob(ctx, child)
	if err != nil || len(j.DependsOn) != 1 || j.DependsOn[0] != parent {
		t.Fatalf("child dependencies: %+v %v", j, err)
	}
	for _, id := range []int64{later, delayed} {
		if j, err := c.GetJob(ctx, id); err != nil || j.State != "scheduled" {
			t.Fatalf("job %d should be scheduled: %+v %v", id, j, err)
		}
	}
	if _, err := c.GetJob(ctx, 999999); status.Code(err) != codes.NotFound {
		t.Fatalf("missing job: %v", err)
	}
	jobs, err := c.ListJobs(ctx, "q1", "available", 10)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("list available: %d %v", len(jobs), err)
	}

	// Discarding the parent strands the child, which is then cancelled.
	if err := c.DiscardJob(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.CancelOrphans(ctx, db); err != nil {
		t.Fatal(err)
	}
	if j, _ := c.GetJob(ctx, child); j.State != "cancelled" {
		t.Fatalf("orphaned child should be cancelled, got %s", j.State)
	}
	if err := c.RetryJob(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if err := c.RetryJob(ctx, parent); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retrying an available job must fail: %v", err)
	}
	if err := c.CancelJob(ctx, later); err != nil {
		t.Fatal(err)
	}

	queues, err := c.ListQueues(ctx)
	if err != nil || len(queues) != 1 || queues[0].Name != "q1" || queues[0].DepthByState["cancelled"] != 2 {
		t.Fatalf("queues: %+v %v", queues, err)
	}
	cs, err := c.CreateCron(ctx, "q1", "digest", map[string]int{"n": 1}, "*/5 * * * *")
	if err != nil || cs.NextRunAt == "" {
		t.Fatalf("cron: %+v %v", cs, err)
	}
	if _, err := c.CreateCron(ctx, "q1", "digest", nil, "every tuesday"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad cron spec: %v", err)
	}
	if _, err := c.Stats(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}

	// The enqueue span's context is stored on the job for the worker to continue.
	var tc json.RawMessage
	if err := db.QueryRow(`SELECT trace_context FROM jobs WHERE id = $1`, parent).Scan(&tc); err != nil {
		t.Fatal(err)
	}
	sc := trace.SpanContextFromContext(telemetry.Extract(ctx, tc))
	if !sc.IsValid() || !sc.IsRemote() {
		t.Fatalf("trace context not stored: %s", tc)
	}
}

func TestEnqueueValidation(t *testing.T) {
	_, addr := serve(t, api.InterceptorConfig{})
	c := dial(t, addr)
	ctx := context.Background()
	big := make([]byte, api.MaxArgsBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	for name, call := range map[string]func() error{
		"missing kind":   func() error { _, _, err := c.Enqueue(ctx, "q1", "", nil, client.EnqueueOptions{}); return err },
		"args too large": func() error { _, _, err := c.Enqueue(ctx, "q1", "k", string(big), client.EnqueueOptions{}); return err },
		"bad attempts": func() error {
			_, _, err := c.Enqueue(ctx, "q1", "k", nil, client.EnqueueOptions{MaxAttempts: 65})
			return err
		},
		"negative delay": func() error {
			_, _, err := c.Enqueue(ctx, "q1", "k", nil, client.EnqueueOptions{Delay: -time.Second})
			return err
		},
		"too many deps": func() error {
			_, _, err := c.Enqueue(ctx, "q1", "k", nil, client.EnqueueOptions{DependsOn: make([]int64, 101)})
			return err
		},
	} {
		if err := call(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: want InvalidArgument, got %v", name, err)
		}
	}
	if _, _, err := c.Enqueue(ctx, "no-such-queue", "k", nil, client.EnqueueOptions{}); status.Code(err) != codes.Internal {
		t.Errorf("unknown queue: want Internal, got %v", err)
	}
}

func TestAuthPublicReadAndRateLimit(t *testing.T) {
	_, addr := serve(t, api.InterceptorConfig{Token: token, PublicRead: true, RatePerSec: 1, Burst: 4})
	ctx := context.Background()
	anon := dial(t, addr)
	if _, err := anon.ListQueues(ctx); err != nil {
		t.Fatalf("public read should be allowed: %v", err)
	}
	if _, _, err := anon.Enqueue(ctx, "q1", "k", nil, client.EnqueueOptions{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("anonymous write: want Unauthenticated, got %v", err)
	}
	wrong := dial(t, addr, client.WithToken("nope"))
	if err := wrong.CancelJob(ctx, 1); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: want Unauthenticated, got %v", err)
	}
	limited := 0
	for range 4 {
		if _, err := anon.ListQueues(ctx); status.Code(err) == codes.ResourceExhausted {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("burst of 4 at 1/s should have been rate limited")
	}
}

func TestRequestIDIsEchoed(t *testing.T) {
	_, addr := serve(t, api.InterceptorConfig{})
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	rpc := conveyorv1.NewConveyorClient(conn)
	var header metadata.MD
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-request-id", "req-42")
	if _, err := rpc.ListQueues(ctx, &conveyorv1.ListQueuesRequest{}, grpc.Header(&header)); err != nil {
		t.Fatal(err)
	}
	if got := header.Get("x-request-id"); len(got) != 1 || got[0] != "req-42" {
		t.Fatalf("request id header: %v", got)
	}
	if _, err := rpc.ListQueues(context.Background(), &conveyorv1.ListQueuesRequest{}, grpc.Header(&header)); err != nil {
		t.Fatal(err)
	}
	if got := header.Get("x-request-id"); len(got) != 1 || len(got[0]) != 16 {
		t.Fatalf("generated request id: %v", got)
	}
}
