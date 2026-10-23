// conveyor is the command-line client for the Conveyor API.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pavan-dangeti/Conveyor/client"
)

const usage = `usage: conveyor <command> [flags]

commands:
  enqueue  --queue Q --kind K [--args JSON] [--priority N] [--max-attempts N]
           [--delay 5s | --run-at RFC3339] [--idempotency-key K] [--depends-on 1,2]
  jobs     [--queue Q] [--state S] [--limit N]
  get      <id>
  cancel   <id>
  retry    <id>
  discard  <id>
  queues
  cron     --queue Q --kind K --spec "*/5 * * * *" [--args JSON]
  stats    [--window 60s]

environment:
  CONVEYOR_ADDR       gRPC address (default localhost:9090)
  CONVEYOR_API_TOKEN  bearer token
  CONVEYOR_TLS=1      use TLS with the system roots`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err := run(context.Background(), os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, args []string) error {
	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = "localhost:9090"
	}
	opts := []client.Option{client.WithToken(os.Getenv("CONVEYOR_API_TOKEN"))}
	if os.Getenv("CONVEYOR_TLS") == "1" {
		opts = append(opts, client.WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	c, err := client.New(addr, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)

	switch cmd {
	case "enqueue":
		queueName := fs.String("queue", "", "queue name")
		kind := fs.String("kind", "", "job kind")
		rawArgs := fs.String("args", "{}", "JSON arguments")
		var o client.EnqueueOptions
		priority := fs.Int("priority", 0, "higher runs first")
		maxAttempts := fs.Int("max-attempts", 0, "attempts before dead-lettering (default 3)")
		fs.DurationVar(&o.Delay, "delay", 0, "run after this delay")
		runAt := fs.String("run-at", "", "run at this RFC 3339 time")
		fs.StringVar(&o.IdempotencyKey, "idempotency-key", "", "dedupe key, unique per queue")
		deps := fs.String("depends-on", "", "comma-separated job IDs that must complete first")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if !json.Valid([]byte(*rawArgs)) {
			return errors.New("--args must be valid JSON")
		}
		o.Priority, o.MaxAttempts = int32(*priority), int32(*maxAttempts)
		if *runAt != "" {
			if o.RunAt, err = time.Parse(time.RFC3339, *runAt); err != nil {
				return fmt.Errorf("--run-at: %w", err)
			}
		}
		if o.DependsOn, err = parseIDs(*deps); err != nil {
			return err
		}
		id, deduped, err := c.Enqueue(ctx, *queueName, *kind, json.RawMessage(*rawArgs), o)
		if err != nil {
			return err
		}
		fmt.Printf("id=%d deduped=%v\n", id, deduped)
	case "jobs":
		queueName := fs.String("queue", "", "filter by queue")
		state := fs.String("state", "", "filter by state")
		limit := fs.Int("limit", 50, "max rows")
		if err := fs.Parse(args); err != nil {
			return err
		}
		jobs, err := c.ListJobs(ctx, *queueName, *state, int32(*limit))
		if err != nil {
			return err
		}
		for _, j := range jobs {
			fmt.Printf("%d\t%s\t%s\t%s\tattempt=%d/%d\t%s\n", j.Id, j.Queue, j.Kind, j.State, j.Attempt, j.MaxAttempts, j.Error)
		}
	case "get", "cancel", "retry", "discard":
		if len(args) != 1 {
			return fmt.Errorf("%s takes exactly one job id", cmd)
		}
		id, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("job id: %w", err)
		}
		switch cmd {
		case "get":
			j, err := c.GetJob(ctx, id)
			if err != nil {
				return err
			}
			out, _ := json.MarshalIndent(j, "", "  ")
			fmt.Println(string(out))
			return nil
		case "cancel":
			err = c.CancelJob(ctx, id)
		case "retry":
			err = c.RetryJob(ctx, id)
		case "discard":
			err = c.DiscardJob(ctx, id)
		}
		if err != nil {
			return err
		}
		fmt.Println("ok")
	case "queues":
		queues, err := c.ListQueues(ctx)
		if err != nil {
			return err
		}
		for _, q := range queues {
			states := make([]string, 0, len(q.DepthByState))
			for s, n := range q.DepthByState {
				states = append(states, fmt.Sprintf("%s=%d", s, n))
			}
			sort.Strings(states)
			fmt.Printf("%s\tconcurrency=%d\trate=%.1f/s\t%s\n", q.Name, q.Concurrency, q.RateLimitPerSec, strings.Join(states, " "))
		}
	case "cron":
		queueName := fs.String("queue", "", "queue name")
		kind := fs.String("kind", "", "job kind")
		rawArgs := fs.String("args", "{}", "JSON arguments")
		spec := fs.String("spec", "", "standard 5-field cron spec")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if !json.Valid([]byte(*rawArgs)) {
			return errors.New("--args must be valid JSON")
		}
		s, err := c.CreateCron(ctx, *queueName, *kind, json.RawMessage(*rawArgs), *spec)
		if err != nil {
			return err
		}
		fmt.Printf("id=%d next=%s\n", s.Id, s.NextRunAt)
	case "stats":
		window := fs.Duration("window", time.Minute, "window for throughput and latency")
		if err := fs.Parse(args); err != nil {
			return err
		}
		s, err := c.Stats(ctx, *window)
		if err != nil {
			return err
		}
		fmt.Printf("completed/s=%.1f start_p50=%.1fms p95=%.1fms p99=%.1fms retries=%d dead_letter=%d lease_expirations=%d\n",
			s.CompletedPerSec, s.StartLatencyP50Ms, s.StartLatencyP95Ms, s.StartLatencyP99Ms, s.Retries, s.DeadLetter, s.LeaseExpirations)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
	}
	return nil
}

func parseIDs(s string) ([]int64, error) {
	if s == "" {
		return nil, nil
	}
	var ids []int64
	for _, p := range strings.Split(s, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("--depends-on: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
