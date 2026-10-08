package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = "localhost:9090"
	}
	token := os.Getenv("CONVEYOR_API_TOKEN")
	c, err := client.New(addr, token)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	cmd, rest := os.Args[1], os.Args[2:]
	switch cmd {
	case "enqueue":
		var queueName, kind, args, key string
		var maxAttempts, priority int64
		var delay time.Duration
		for i := 0; i < len(rest); i++ {
			switch rest[i] {
			case "--queue":
				queueName = rest[i+1]
				i++
			case "--kind":
				kind = rest[i+1]
				i++
			case "--args":
				args = rest[i+1]
				i++
			case "--idempotency-key":
				key = rest[i+1]
				i++
			case "--max-attempts":
				maxAttempts, _ = strconv.ParseInt(rest[i+1], 10, 32)
				i++
			case "--priority":
				priority, _ = strconv.ParseInt(rest[i+1], 10, 32)
				i++
			case "--delay":
				delay, _ = time.ParseDuration(rest[i+1])
				i++
			}
		}
		id, deduped, err := c.Enqueue(ctx, queueName, kind, []byte(args), int32(priority), int32(maxAttempts), delay, key)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("id=%d deduped=%v\n", id, deduped)
	case "jobs":
		var queueName, state string
		var limit int64 = 50
		for i := 0; i < len(rest); i++ {
			switch rest[i] {
			case "--queue":
				queueName = rest[i+1]
				i++
			case "--state":
				state = rest[i+1]
				i++
			case "--limit":
				limit, _ = strconv.ParseInt(rest[i+1], 10, 32)
				i++
			}
		}
		r, err := c.ListJobs(ctx, queueName, state, int32(limit))
		if err != nil {
			fatal(err)
		}
		for _, j := range r.Jobs {
			fmt.Printf("%d\t%s\t%s\t%s\tattempt=%d\n", j.Id, j.Queue, j.Kind, j.State, j.Attempt)
		}
	case "cancel":
		id, _ := strconv.ParseInt(rest[1], 10, 64)
		if err := c.CancelJob(ctx, id); err != nil {
			fatal(err)
		}
		fmt.Println("ok")
	case "retry":
		id, _ := strconv.ParseInt(rest[1], 10, 64)
		if err := c.RetryJob(ctx, id); err != nil {
			fatal(err)
		}
		fmt.Println("ok")
	case "queues":
		r, err := c.ListQueues(ctx)
		if err != nil {
			fatal(err)
		}
		for _, q := range r.Queues {
			fmt.Printf("%s\tconcurrency=%d\trate=%.1f\t%v\n", q.Name, q.Concurrency, q.RateLimitPerSec, q.DepthByState)
		}
	case "cron":
		var queueName, kind, args, spec string
		for i := 0; i < len(rest); i++ {
			switch rest[i] {
			case "--queue":
				queueName = rest[i+1]
				i++
			case "--kind":
				kind = rest[i+1]
				i++
			case "--args":
				args = rest[i+1]
				i++
			case "--spec":
				spec = rest[i+1]
				i++
			}
		}
		s, err := c.CreateCron(ctx, queueName, kind, []byte(args), spec)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("id=%d next=%s\n", s.Id, s.NextRunAt)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`usage: conveyor <command> [flags]
commands:
  enqueue --queue --kind --args '{...}' [--priority N] [--max-attempts N] [--delay 5s] [--idempotency-key K]
  jobs [--queue Q] [--state S] [--limit N]
  cancel <id>
  retry <id>
  queues
  cron --queue --kind --args '{...}' --spec "*/5 * * * *"`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
