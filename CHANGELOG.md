# Changelog

## v1.1.0 (2026-10-23)

### Added
- **Fault injection through Toxiproxy**: every run now adds a 4 s network partition, connection
  resets, and a worker whose clock runs 4x slow relative to its lease, on top of `kill -9`,
  `SIGSTOP` and a Postgres restart. The harness writes `results/fault-<date>.json` itself and
  repeats with `FAULT_RUNS`; 5 runs × 200 jobs, 0 violations.
- **TLA+ model** with idempotent enqueue, crash without lease loss, the reaper, and liveness
  (`EventuallyTerminal`) under explicit fairness, plus a mutation check in CI that removes the
  fencing guard and requires TLC to catch the zombie. TLA+ tools pinned to v1.7.4 by checksum.
- **Benchmarks** that sweep workers, batch and payload with 5 reps and 95% confidence intervals,
  sample Postgres wait events, and compare against River through the same measurement code
  (`scripts/bench.sh`, `results/bench-2026-10-23.json`).
- **Batched, fenced completions** in the worker library (ADR 0004): 2024 ± 138 → 2697 ± 107 jobs/s.
- **OpenTelemetry tracing** across HTTP, gRPC, enqueue and job execution, with trace context stored
  in each job; request IDs and one JSON access-log line per RPC; worker `/metrics`; a Grafana
  dashboard with rates and percentiles; Prometheus, Grafana and Jaeger in the
  `observability` profile.
- **Security**: per-IP rate limiting, constant-time token check, public-read mode for the demo,
  field-level input validation, least-privilege Postgres roles, TLS through Caddy, a distroless
  non-root image, and a CSP on the dashboard.
- **Live-demo mode**: synthetic workload, supervised worker processes, a chaos endpoint that
  sends SIGKILL, and a live exactly-once check (ADR 0005).
- **Interfaces**: `client` and `worker` moved out of `internal/` so other modules can import them;
  `worker.Job`, `(*Worker).CompleteTx`; SDK options for TLS and enqueue fields; `DiscardJob` and
  `GetStats` RPCs; CLI `get`, `discard`, `stats` and `--run-at`.
- **CI**: gofmt, an 85% core coverage gate, Trivy image and repository scans, and an
  end-to-end smoke test of the demo stack.

### Fixed
- Every state write now takes its source and target states from `internal/statemachine`, which
  was previously defined but bypassed by hand-written SQL.
- The NOTIFY debounce dropped wake-ups for enqueues inside its window; it now has a trailing edge.
- Graceful shutdown cancelled in-flight handlers immediately; they now finish within the
  shutdown budget.
- The worker's LISTEN connection never reconnected after a Postgres restart.
- Workers claimed jobs they had no free slot for, holding leases while waiting.
- Jobs whose workflow dependency was discarded or cancelled waited forever; they are cancelled.
- Running jobs could not be cancelled.
- The dashboard rendered job kinds and errors as HTML.
- `/metrics` appended a hand-written copy of the queue-depth gauges after the Prometheus output.
- The Go toolchain and `golang.org/x/net` were bumped to fix vulnerabilities reported by
  govulncheck.

### Corrections to v1.0.0
- The v1.0.0 benchmark figures (789 → 1243 jobs/s, River 298 and 643 jobs/s) came from single
  runs whose "before" configurations could not be rebuilt from the released code. They are
  replaced by `results/bench-2026-10-23.json`.
- The v1.0.0 fault result covered one run of 60 jobs without partitions or clock skew, and its
  per-scenario breakdown and hardware description were added to the file by hand afterwards.
  They are replaced by `results/fault-2026-10-23.json`, which the harness writes itself.
- The coverage gate added after v1.0.0 read a profile that did not exist yet when it ran, so it
  always skipped; CI now enforces coverage directly.

## v1.0.0 (2026-10-08)

First release.

- **Engine:** Postgres-backed job queue with `SKIP LOCKED` dequeue in priority order, per-queue
  concurrency caps, leases with heartbeats, a fencing token (`lease_version`) on every outcome
  write, a dead-letter queue, idempotency keys, exponential backoff with jitter, and a
  single-leader maintenance loop (Postgres advisory lock) for promotion, reaping, cron and
  archiving.
- **Interfaces:** gRPC API with an HTTP/JSON gateway, Go SDK, CLI, per-queue rate limits,
  workflow dependency gating and cron schedules.
- **Correctness:** TLA+ model checked in CI; fault-injection harness with real worker processes.
- **Performance:** load generator and River comparison.
- **Deployment:** Dockerfile, demo Compose stack, embedded dashboard, `/metrics`, Grafana
  dashboard.
