# Conveyor — Design Document

> Status: Implemented (v1.0). Last updated: 2026-10-23. Measured results are in §8 and `results/`.

## 1. Context

Companies run large volumes of background work (email, payments, ML batch jobs,
notifications) that must survive crashes and restarts. They need a queue that
neither loses jobs nor executes a job's externally visible effect twice.

Conveyor is a durable, distributed job queue and simple workflow engine built
around one idea: **PostgreSQL is the single source of truth**, and exactly-once
*effects* are achieved with idempotency keys and fencing tokens — because
exactly-once *delivery* is impossible.

### 1.1 Why this problem is interesting

A queue looks trivial until a worker is killed mid-job, a network partition
splits workers from the database, or a "zombie" worker wakes up after losing
its lease. Each of these breaks naive designs. The interesting systems work is
in the recovery and fencing paths, and that is what this project exists to
demonstrate.

## 2. Goals and non-goals

### Goals

1. At-least-once delivery with exactly-once effects via idempotency keys and
   fencing tokens.
2. Priorities, delayed/scheduled jobs, cron schedules, retries with
   exponential backoff + jitter, a dead-letter queue, cancellation, per-queue
   concurrency and rate limits, and simple dependency workflows
   (job B runs after job A succeeds).
3. Leases with heartbeats; crashed workers' jobs are recovered automatically.
4. Horizontal scaling of API servers and workers.
5. A formal model (TLA+) of the core protocol, model-checked against stated
   invariants, plus fault-injection tests with an automatic checker.

### Non-goals

- Multi-region replication or conflict resolution.
- A custom storage engine; we build on PostgreSQL.
- Multi-tenant hosted service, RBAC, or billing.
- Exactly-once delivery (see §5.4).

## 3. Requirements and SLOs

### Functional requirements

| ID | Requirement |
|----|-------------|
| F1 | Producers enqueue jobs with kind, arguments (JSON), priority, optional delay, schedule time, max attempts, idempotency key, and queue name. |
| F2 | Workers dequeue jobs matching their registered kinds, respecting per-queue concurrency and rate limits and priority order. |
| F3 | A dequeued job moves to `running` with a lease (owner, expiry, fencing token); workers heartbeat to extend. |
| F4 | A worker completes or fails a job only with the current fencing token; stale tokens are rejected. |
| F5 | Failed jobs retry with exponential backoff and jitter up to max attempts, then go to `discarded`. |
| F6 | Scheduled and cron jobs become available at/after their scheduled time. |
| F7 | Cancelled jobs (only while non-terminal) stop before execution and never run after cancellation. |
| F8 | An idempotency key creates at most one job per queue. |
| F9 | Workflows: job B is only dequeued after its dependency A completes. |
| F10 | All state transitions are defined and enforced in exactly one place. |

### Non-functional requirements / SLOs

| ID | Requirement |
|----|-------------|
| N1 | No job lost: every enqueued job eventually reaches a terminal state or remains retryable. |
| N2 | No double effect: a job's side effect is applied at most once. |
| N3 | Crash recovery time: a job from a crashed worker is picked up again within `2 × lease duration` (default lease 30 s → recovery < 60 s + backoff). |
| N4 | Throughput on one Postgres (Apple M5, 16 GB): target ≥ 5k jobs/s end to end for no-op jobs. **Not met:** measured 2697 ± 107 jobs/s (§8). The limit is WAL flushing, three durable commits per job; see §8 for what would move it. |
| N5 | Enqueue-to-start latency when workers keep up: p50 < 50 ms, p99 < 500 ms. **Met:** p50 5.0 ms, p99 12.4 ms at 2.7k jobs/s (§8). |
| N6 | Graceful shutdown: an in-flight job runs to completion or its lease is released within the worker's shutdown budget (default 25 s). |
| N7 | Durability: acknowledged enqueues survive Postgres restart (synchronous commit). |

### Error budget note

N2/N1 are hard invariants (zero tolerance), verified by the checker in every
fault-injection run and by TLC in CI. N3–N5 are SLOs; §8 reports the measurements.

## 4. System overview

```
 producers                    ┌──────────────────────────┐
 ┌────────┐  gRPC/HTTP ─────▶│  API servers (stateless) │
 └────────┘                  └────────────┬─────────────┘
                                         │ SQL
 ┌────────┐  heartbeats/        ┌────────▼─────────────┐
 │ Worker │◀────NOTIFY──────────│      PostgreSQL      │
 │ fleet  │────complete/fail───▶│ jobs, queues, cron,  │
 └────────┘                     │ workflows, ledger     │
                                └────────▲─────────────┘
                                         │ advisory lock (leader only)
                                ┌────────┴─────────────┐
                                │ Maintenance worker   │
                                │ scheduler, reaper,   │
                                │ retention            │
                                └──────────────────────┘
```

- **API servers** are stateless and scale horizontally; all correctness
  guarantees live in SQL transactions and constraints.
- **Workers** are separate processes registered against queues; a worker
  library provides handler registration, graceful shutdown, and context
  cancellation.
- **Maintenance leader** is a single elected process (Postgres advisory lock)
  running: the scheduler (delayed → available, cron → enqueue), the lease
  reaper (expired leases → retryable or discarded), and retention cleanup.
- **LISTEN/NOTIFY** wakes idle workers; polling is the fallback when NOTIFY
  is missed (e.g., connection drop).

## 5. Core design

### 5.1 Job state machine

```
            enqueue(delay?)                 lease granted
   ───────▶ scheduled ──────────────┐     ┌──────────────▶ running ──────────────▶ completed
   │              │                │     │   ▲    │                              └▶ cancelled
   │ cancel       │ time due       │ avail│   │    │ fail(retry)                 │
   │              ▼                ▼     │   │    ▼                              │
   │          available ◄──────────┴─────┘   │  retryable ──(backoff due)──┐    │
   │              │                          │      │                       │    │
   │              │ lease lost / cancel      │      │ attempts exhausted    │    │
   │              ▼                          │      ▼                       │    │
   └──────▶ cancelled                        └── discarded (dead letter) ◀──┘    │
                                                                                   ▼
                                                                            cancelled
```

States: `scheduled`, `available`, `running`, `completed`, `retryable`,
`discarded`, `cancelled`.

Every transition is a table entry `(from, event, to, guard)` in one package
(`internal/statemachine`), enforced by a single `Transition` function that all
call sites use. No SQL outside that package may write `state` directly.

### 5.2 Data model (draft)

```sql
CREATE TABLE jobs (
  id              BIGSERIAL PRIMARY KEY,
  queue           TEXT NOT NULL REFERENCES queues(name),
  kind            TEXT NOT NULL,
  args            JSONB NOT NULL,
  state           TEXT NOT NULL,          -- state machine enum
  priority        INT NOT NULL DEFAULT 0, -- higher dequeued first
  attempt         INT NOT NULL DEFAULT 0,
  max_attempts    INT NOT NULL DEFAULT 3,
  run_at          TIMESTAMPTZ NOT NULL,   -- available at/after
  lease_owner     TEXT,
  lease_expiry    TIMESTAMPTZ,
  lease_version   BIGINT NOT NULL DEFAULT 0,  -- fencing token, monotonic per job
  idempotency_key TEXT,
  error           TEXT,
  error_history   JSONB NOT NULL DEFAULT '[]',
  trace_context   JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (queue, idempotency_key)       -- F8
);
CREATE INDEX jobs_dequeue_idx ON jobs (queue, priority DESC, run_at)
  WHERE state IN ('available', 'retryable');

CREATE TABLE queues (name TEXT PRIMARY KEY, concurrency INT, rate_limit_per_sec DOUBLE PRECISION);
CREATE TABLE cron_schedules (id BIGSERIAL PRIMARY KEY, queue, kind, args, spec TEXT, next_run_at TIMESTAMPTZ);
CREATE TABLE job_dependencies (job_id BIGINT REFERENCES jobs(id), depends_on BIGINT REFERENCES jobs(id), PRIMARY KEY (job_id, depends_on));
CREATE TABLE ledger (job_id BIGINT, effect_id TEXT, occurred_at TIMESTAMPTZ, PRIMARY KEY (job_id, effect_id)); -- fault-injection checker
```

Hot-table discipline: completed/cancelled/discarded rows are archived to
`jobs_archive` by the maintenance worker (or partitioned by month once volume
warrants it) so the working set stays small. The partial index above keeps
dequeue scans off completed rows.

### 5.3 Dequeue, leases, fencing

Dequeue is one statement (`queue.Dequeue`): a CTE picks due `available`
rows in priority order with `FOR UPDATE SKIP LOCKED`, skipping jobs whose
dependencies are not all completed and honouring the queue's concurrency and
rate limits, and the outer `UPDATE` claims them:

```sql
WITH picked AS (
  SELECT id FROM jobs
  WHERE state = 'available' AND queue = $1 AND run_at <= now()
    AND <dependencies completed> AND <concurrency and rate limit allow>
  ORDER BY priority DESC, run_at, id
  LIMIT $batch
  FOR UPDATE SKIP LOCKED)
UPDATE jobs SET state = 'running', attempt = attempt + 1,
  lease_owner = $worker, lease_expiry = now() + $lease,
  lease_version = lease_version + 1, started_at = now()
FROM picked WHERE jobs.id = picked.id
RETURNING ...;
```

The source and target states in this and every other state-changing
statement come from `statemachine.Edge`, so no SQL can perform a transition
the table in §5.1 does not list.

`SELECT ... FOR UPDATE SKIP LOCKED` guarantees at most one worker claims a
row per queue shard; the maintenance leader's reaper uses the same locking
idiom, so claim vs. expiry never interleave destructively.

**Fencing.** Every state write from a worker (complete / fail / heartbeat) is
guarded:

```sql
UPDATE jobs SET state='completed', ...
 WHERE id=$id AND lease_owner=$w AND lease_version=$v AND state='running';
-- 0 rows affected ⇒ stale; worker must not retry with same token
```

`lease_version` is monotonically increasing per job and is attached to the
effect write in the job handler (and to the ledger insert in tests). A zombie
whose lease expired has an old version, so its completion is rejected and it
cannot overwrite a newer worker's result. When a job is retried, a new lease
version is issued, so an effect from the previous attempt with a lower token
cannot commit twice (the handler must check before applying effects).

**Heartbeats.** Workers extend `lease_expiry` every `lease/3` via a guarded
update with the same (owner, version) check; heartbeat failure (0 rows) means
the worker lost the lease and must abort the job's side effects and discard
the result.

### 5.4 Why exactly-once delivery is impossible, and what we guarantee

A guarantee that a message is *delivered and processed* exactly once requires
the worker and the broker to atomically commit "message processed" together
with the side effect. Over an unreliable network, a worker cannot distinguish
"the broker never saw my ack" from "the broker saw it and its reply was lost"
(the Two Generals problem). So delivery is at-least-once (or at-most-once),
never exactly-once.

What Conveyor guarantees instead:

1. **At-least-once delivery**: every accepted job is eventually leased to
   some worker or remains retryable.
2. **Exactly-once effects**: the job handler applies its side effect at most
   once, because (a) the idempotency key makes enqueue dedupe at the queue
   level, and (b) the fencing token makes a stale worker's completion (and
   any effect write it tries to commit transactionally with that token)
   impossible. Handlers that call external non-transactional systems must
   carry the fencing token / idempotency key in the external request so the
   external system can dedupe too; Conveyor documents this contract.

### 5.5 Retries, backoff, dead letter

Backoff: uniform in `[0, min(cap, base * 2^(attempt-1)))` (full jitter),
`base=1s`, `cap=10m`; a property test checks the bound for any attempt count. After `max_attempts`, the job moves to `discarded` and is visible
in the dashboard with its error history. Operators can retry or discard from
the dashboard/CLI.

### 5.6 Scheduler and cron

The maintenance leader scans `cron_schedules` and due `scheduled` jobs every
500 ms (or on NOTIFY), inserts new `available` jobs, and advances
`next_run_at` using the standard cron parser. All of this is idempotent
(enqueue respects the idempotency key derived from `(cron_id, scheduled
occurrence)`), so a leader failover mid-scan cannot duplicate jobs beyond
the idempotency-key guarantee.

### 5.7 Rate limits and concurrency

Per-queue concurrency: dequeue claims nothing while
`running_jobs(queue) >= queues.concurrency`. The count is not taken under a
lock, so concurrent dequeuers can over-admit by up to one batch each; the
code marks this ceiling.

Per-queue rate limit: `queue_rate.last_dispatch` records the last claim, and
a dequeue of `n` jobs is allowed only once `n / rate_limit_per_sec` seconds
have passed since then. The row update is part of the dequeue statement.

API rate limit: a token bucket per client IP in each API process
(`CONVEYOR_RATE_LIMIT`, default 50 requests/s, burst 100).

### 5.8 Workflows

`job_dependencies` rows gate dequeue: a job with unmet dependencies is not
`available`-listed until all dependencies are `completed`. Listing joins
against the table in the dequeue query; dependency satisfaction is
monotonic, so no locking is needed beyond the row lock on the dependency.

### 5.9 Wake-ups

Enqueue and every transition to `available` emit `NOTIFY jobs_available`,
debounced to at most one per 10 ms with a trailing edge: a burst costs one
notification at its start and one at the end of the window, and no job waits
for a poll. Workers `LISTEN` on a dedicated connection, reconnect after
losing it, and fall back to polling (200 ms doubling to 1 s) meanwhile.

### 5.10 Trade-offs

- **Testcontainers for integration tests.** It keeps CI hermetic and parallel;
  the cost is slower tests and a Docker daemon requirement.
- **Prometheus pull-based scraping.** The API process stays free of client
  wiring; the cost is an extra component to operate.
- **A session-level advisory lock for leader election.** One lock per
  maintenance process resolves failover without a scheduler; its cost is that
  only one maintenance process runs at a time, and a crashed leader's lock is
  released by the database itself.

## 6. Interfaces

- **gRPC API** (`api/proto/conveyor/v1`): `Enqueue`, `GetJob`, `ListJobs`,
  `CancelJob`, `RetryJob`, `DiscardJob`, `ListQueues`, `CreateCron`,
  `GetStats`, plus the standard gRPC health service; the same proto
  generates the HTTP/JSON gateway under `/v1/`.
- **Go SDK** (`internal/client`): typed options, bearer token, optional
  TLS, trace-context propagation.
- **Worker library** (`internal/worker`): handlers by kind; dequeue only into
  free slots; heartbeats every lease/3; context cancelled on lost lease;
  batched, fenced completions; graceful shutdown that stops claiming at once
  and lets in-flight jobs finish within a budget; `CompleteTx` for handlers
  that commit their effect and the completion in one transaction.
  Workers talk to Postgres directly, not through the API, so the API is not
  on the hot path.
- **CLI** (`cmd/conveyor`): `enqueue`, `jobs`, `get`, `cancel`, `retry`,
  `discard`, `queues`, `cron`, `stats`.
- **Dashboard** (`/`): throughput, enqueue-to-start p50/p95/p99, retries,
  dead-letter queue with retry, queue depth by state, recent jobs with
  cancel; in demo mode also the worker fleet, the chaos button and the live
  exactly-once check.

## 7. Correctness

### 7.1 Invariants

1. A job reaches a terminal state or stays retryable (no loss, no stuck
   running beyond lease expiry + reaper interval).
2. At most one worker holds a valid lease on a job at any time.
3. Only the current lease holder can change a running job's outcome.
4. An idempotency key creates at most one job per queue.
5. A job's effect is applied at most once.

### 7.2 Formal model

`spec/tla/Conveyor.tla` models enqueue with idempotency keys, dequeue,
heartbeat, lease loss, lease expiry, the reaper, completion with its effect,
failure and retry, and worker crash and restart. TLC checks it on every PR
(`MCConveyor.cfg`: three enqueue attempts, two sharing a key; two workers;
two attempts; one crash) for invariants 2–5 as state invariants or an action
property, and for invariant 1 as a liveness property under weak fairness.
`spec/tla/README.md` lists what the model leaves out.

### 7.3 Fault injection

`internal/fault` runs four real worker processes against Postgres through
Toxiproxy and, in every run, injects: 30 ms ± 10 ms latency and 5%
connection resets for the whole run, a Postgres restart, `kill -9` of one
worker, `SIGSTOP` of another for 2.5 leases (it resumes as a zombie), a 4 s
network partition, and a fourth worker whose clock runs 4x slow (it
heartbeats every 8 s against a 2 s lease). Each job's effect is a `ledger`
row committed in the same transaction as its completion. Afterwards
`internal/checker` reads the history and fails the run unless every job is
terminal, every completed job has exactly one ledger row carrying its final
fencing token, and no other job has one. The test also requires that leases
actually expired, so a run in which the faults did nothing fails. Results:
`results/fault-*.json` and the README.

## 8. Performance results

Method, raw data and commands: `results/bench-2026-10-23.json`, produced by
`scripts/bench.sh` (5 reps of 2000 no-op jobs per configuration, Postgres
17.11 in Docker on the same Apple M5 / 16 GB laptop, 95% Student-t
intervals). Summary:

- **Bottleneck:** WAL flushes. Each job needs three durable commits (enqueue,
  claim, completion); sampling `pg_stat_activity` during runs shows
  `LWLock:WALWrite` and `IO:WalSync` as the most common waits after CPU.
- **Change that moved it:** batching completions per worker into one fenced
  `UPDATE ... FROM unnest(...)` (ADR 0004): 2024 ± 138 → 2697 ± 107 jobs/s.
  It relieved contention for the WAL write lock (27.4% → 16.4% of wait
  samples), but WAL flushing is still about 40% of samples: the bottleneck
  moved down, not away.
- **Wake-ups:** one NOTIFY per enqueue costs throughput (1545 ± 86 jobs/s);
  the debounced, trailing-edge NOTIFY keeps p99 at 12.4 ms.
- **Scaling:** 2–8 worker loops are within noise of each other; 16 loops
  drop to 1230 ± 124 jobs/s (more claim statements competing for CPU and
  WAL), and one loop with 8 slots cannot keep up (backlog: p50 279 ms).
  Payload size costs WAL bandwidth: 64 KiB args reach 1061 ± 22 jobs/s.
- **River v0.49.0, same host and workload:** 321 jobs/s with its default
  100 ms fetch cooldown, 1921 ± 918 jobs/s with 1 ms. River does more per
  job (a richer row: metadata, attempt history) and its default deliberately
  trades latency for fewer fetch queries.
- **What would move it next:** batching enqueues (`EnqueueMany`), claiming
  and completing in fewer round trips, `synchronous_commit = off` for queues
  that can tolerate losing the last few hundred milliseconds of enqueues
  after a crash, and sharding queues across Postgres instances.

## 9. Observability, security, operations

- Prometheus metrics: queue depth by state, transition counts, per-transition
  latency histograms, retries, dead-letter count, lease expirations, leader
  changes, request rate/errors/duration.
  API processes export these plus queue depth read from Postgres at scrape
  time; workers export theirs on `METRICS_ADDR`.
- OpenTelemetry: the HTTP handler, gRPC server and client, enqueue and job
  execution create spans; the enqueue span's W3C context is stored in
  `jobs.trace_context`, and the worker continues it, so one trace covers the
  request and the job's execution in another process (OTLP/HTTP export).
- Structured JSON logs; one access line per RPC with a request ID (taken
  from `X-Request-Id` or generated, and echoed back). `/healthz`, `/readyz`
  and the gRPC health service.
- Grafana dashboard in `dashboards/conveyor.json`, provisioned with
  Prometheus and Jaeger by `compose-demo.yml --profile observability`.
- Security: bearer token compared in constant time; optional public read
  for the demo; per-IP rate limit; 64 KiB argument limit plus a body limit
  on the gateway; input validation on every field; TLS via Caddy (HTTP and
  gRPC); Postgres roles split into a schema owner that runs migrations and
  `conveyor_app` with DML rights only (`deploy/initdb/01-roles.sh`);
  distroless non-root image; CSP on the dashboard.
- CI: gofmt, vet, staticcheck, golangci-lint, govulncheck, race-detector
  tests with an 85% coverage gate on core packages, TLC, Trivy image and
  repository scans, and an end-to-end smoke test of the demo stack.

## 10. Alternatives considered

| Option | Why not (vs. Postgres-backed) |
|--------|-------------------------------|
| **Redis-backed queue** | Fast, but durability semantics weaker; needs blocking commands for dequeue at scale; we'd reimplement leasing/fencing and still do idempotency elsewhere. Keeping one store avoids a cross-system atomicity problem. |
| **Kafka as the queue** | High throughput, but consumer groups reprocess on rebalance; exactly-once effects still need an external store; leases/fencing and per-job retry/DLQ semantics are awkward. Better fit for event streaming than per-job delivery. |
| **Database-agnostic design** (pluggable storage) | We would dilute correctness guarantees to the lowest common denominator and double the test matrix. Postgres-specific features (`SKIP LOCKED`, advisory locks, `LISTEN/NOTIFY`) are exactly what make the invariants tractable. Postgres is already deployed nearly everywhere. |
| **River (adopt as-is)** | River is a strong reference; we take its design ideas (leases, leader election, maintenance loop) as the baseline for the comparison in §8. It is MIT-licensed, so borrowing ideas with attribution is fine; the project goal is to demonstrate the SDE correctness work, which a wrapper would hide. |

## 11. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Fencing gaps where handlers call external systems without dedupe | Document the contract: handlers must pass idempotency key + fencing token to external calls; provide helper wrappers. |
| Postgres becomes bottleneck at high enqueue rates | Batch dequeue, archived table, partition by queue; horizontal sharding by queue (documented, not implemented in v1). |
| TLA+ model diverges from implementation | The state machine package is the single source of transitions; model-checked properties are also asserted by integration tests; fault-injection checker catches drift empirically. |
| Zombie worker flapping (lease loss → retry success → effect double-apply) | Fencing + idempotency in the effect path; tested by the SIGSTOP fault case. |
| Clock skew breaks lease expiry | Leases use Postgres `now()` (server clock) consistently, never worker clock; documented in the runbook. |

## 12. Hosting plan (free tier only)

- **Plan of record:** one Oracle Cloud Always Free Ampere A1 VM (4 OCPU,
  24 GB) running `compose-demo.yml`: Postgres, the API in demo mode with
  three supervised worker processes, and Caddy for automatic HTTPS. The
  observability profile adds Prometheus, Grafana and Jaeger on loopback.
  Steps are in `docs/runbook.md`.
- **Fallback:** Neon free Postgres with the API on a free container host;
  the demo's child-process workers need no extra services.

## 13. Threat model (summary)

- **Unauthenticated writes**: every write needs the bearer token, compared
  in constant time; tokens come from the environment and are never logged.
  The token is shared, not per client, and TLS is what stops replay on the
  wire.
- **Oversized payloads**: request size limit 64 KiB at the gateway, enforced
  again in the API layer.
- **Malicious job args**: arguments are JSON, executed by user-registered
  handlers only; Conveyor never evals args server-side.
- **DoS via queue flooding**: per-IP token bucket, payload and body limits,
  and HTTP timeouts. The limiter keys on the first `X-Forwarded-For` hop,
  which Caddy sets; a direct gRPC client could forge it, so the gRPC port is
  bound to loopback in the demo. A determined attacker can still exhaust a
  free VM.
- **Chaos endpoint**: unauthenticated by design in the demo, limited to one
  kill every 5 s, and it can only signal the demo's own child processes.
- **Dashboard XSS**: job kinds and errors are caller-controlled, so the page
  renders them as text, never HTML, under a restrictive CSP.
- **DB credential theft**: the runtime role cannot alter the schema;
  Postgres is not published outside the Docker network; secrets live in
  `.env`, which is gitignored.
- **Supply chain**: pinned dependencies, `govulncheck`, container scanning,
  no bot commits.

## 14. Milestones

Phase 0 design (this doc) → Phase 1 foundation (module layout, migrations,
CI, Compose, testcontainers) → Phase 2 engine (state machine, dequeue,
leases, fencing, heartbeats, retries, DLQ, worker library) → Phase 3
interfaces (gRPC/HTTP, SDK, CLI, idempotency, rate limits, cron, workflows)
→ Phase 4 correctness (TLC in CI, fault injection, checker, published
results) → Phase 5 performance/ops (benchmarks, River comparison,
observability, dashboard) → Phase 6 launch (hardening, demo deploy, docs,
release). One PR per phase.
