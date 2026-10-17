# Changelog

## v1.7.0 (2026-10-17)

Phase 6 — fault-injection results detail. `results/fault-2026-10-08.json` now records the hardware, the four named scenarios (proxy drops+latency, Postgres restart, worker SIGKILL, worker SIGSTOP/SIGCONT), per-scenario completion and violation counts, and the reproduction command. Previously it held only aggregates.

## v1.6.0 (2026-10-17)

Phase 5 — job dependencies end to end. `Client.Enqueue` forwards `depends_on` (it was silently dropped) and the CLI accepts `enqueue --depends-on 12,34`, so `job_dependencies` rows can actually be created.

## v1.5.0 (2026-10-17)

Phase 4 — worker heartbeat and lease-expiry tests. `TestHeartbeatKeepsLeaseAlive` proves a handler outliving its lease is never reaped; `TestLeaseExpiryRecoveryByAnotherWorker` proves an expired lease is reaped and recovered by a second worker while the first holder's token stays stale.

## v1.4.0 (2026-10-17)

Phase 3 — compose in CI. A new `compose` job validates `docker-compose.yml` and `compose-demo.yml`, starts Postgres, and smoke-tests it. The compose files existed but were never exercised by CI.

## v1.3.0 (2026-10-17)

Phase 2 — CI honesty checks. A test fails CI if the README results table drifts from `results/benchmarks-*.json` (no faked numbers, no missing runs), and a second test fails below 85 % coverage of `internal/statemachine`. Also added the `conveyor-notify-removed` run that existed only in the results file.

## v1.2.0 (2026-10-17)

Phase 1 — observability wiring and operator RPCs. `DiscardJob` and `GetStats` RPCs, `run_at`/`depends_on` forwarding, `observe()` metrics on every RPC, job-duration histogram, leader gauge and leader-change counter, Prometheus DB collector, `migrations/0003_observability.sql`, and the dashboard extended to eight panels.

## v1.1.0 (2026-10-17)

Phase 0 — design document fixes: status header activated, missing Trade-offs section added, TLA+ job count corrected (3 → 2), fault proxy named correctly.

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
