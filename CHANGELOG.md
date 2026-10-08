# Changelog

## v1.0.0 (2026-10-08)

First stable release. Highlights:

- **Engine (Phase 2):** Postgres-backed job queue with `SKIP LOCKED` dequeue in priority order, per-queue concurrency caps, leases with heartbeats, a fencing token (`lease_version`) on every outcome write, dead-letter queue with `conveyor retry`, exactly-once effects via idempotency keys + fencing, exponential backoff + jitter, scheduled→available promotion, and a single-leader maintenance loop (Postgres advisory lock) for reaper, cron expansion, and archive.
- **Interfaces (Phase 3):** gRPC API + grpc-gateway HTTP/JSON, Go SDK, CLI, per-queue rate limits, workflow dependency gating, cron schedules via `ExpandDueCron`. Proto args are JSON strings.
- **Correctness (Phase 4):** TLA+ spec model-checked in CI (no invariant violations on a 2-worker/2-job model). Fault-injection harness that runs real worker processes with `kill -9`, `SIGSTOP`/`SIGCONT`, Postgres restart, drop+latency proxy — 60/60 jobs complete, zero fencing violations (`results/fault-*.json`).
- **Performance (Phase 5):** benchmark harness, River v0.49.0 comparison, and the pg_notify debounce fix that took the engine from 789 → 1243 jobs/s on the same hardware (499 inserted at p99 53 ms enqueue→first-executing starts).
- **Deployment:** single Dockerfile for all services, `compose-demo.yml` demo stack, dashboard HTML at `/`, `/metrics` Prometheus endpoint, minimal Grafana dashboard at `dashboards/conveyor.json`.

### Known limits

- Single-region, one logical fleet per stack; shard by queue for more raw Postgres throughput.
- Clock skew between client enqueues and Postgres `now()` is tolerated since enqueue computes `run_at` server-side, but lease timing assumes sane clocks.
- River wins on insert-path latency at default fetch cooldown; we trade that for a "claim fence + heartbeat contract" workers can rely on for exactly-once effects.
