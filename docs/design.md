# Conveyor — Design Document

> Status: Draft for review (Phase 0). Last updated: 2026-10-08.

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
| N4 | Throughput (target, stated hardware M-series, 16 GB, single Postgres): ≥ 5k jobs/s sustained enqueue+execute round trip for tiny no-op jobs. (Re-baselined after Phase 5 benchmarks.) |
| N5 | Enqueue-to-start latency for available jobs on an idle system: p50 < 50 ms (wake-up path), p99 < 500 ms (poll fallback bound). |
| N6 | Graceful shutdown: an in-flight job runs to completion or its lease is released within the worker's shutdown budget (default 25 s). |
| N7 | Durability: acknowledged enqueues survive Postgres restart (synchronous commit). |

### Error budget note

N2/N1 are hard invariants (zero tolerance), verified by the checker in every
fault-injection run and by TLC in CI. N3–N5 are SLOs measured in Phase 5.

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

Dequeue (single transaction):

```sql
SELECT id FROM jobs
 WHERE state IN ('available','retryable')
   AND queue = $1 AND run_at <= now()
 ORDER BY priority DESC, run_at, id
 LIMIT $batch
 FOR UPDATE SKIP LOCKED;
-- then for each row:
UPDATE jobs SET state='running', attempt=attempt+1,
  lease_owner=$w, lease_expiry=now()+$lease, lease_version=lease_version+1
 WHERE id=$id AND lease_version IS NOT NULL;  -- always succeeds under the row lock
```

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

Backoff: `min(cap, base * 2^attempt)` with full jitter, `base=1s`,
`cap=10m`. After `max_attempts`, the job moves to `discarded` and is visible
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

Per-queue concurrency: the worker's dequeue is bounded by
`queues.concurrency - running_jobs(queue)`, enforced in the dequeue
transaction. Per-queue rate limit: a token-bucket row per queue in Postgres
(or in-process when one worker owns the queue), consumed transactionally at
dequeue time.

### 5.8 Workflows

`job_dependencies` rows gate dequeue: a job with unmet dependencies is not
`available`-listed until all dependencies are `completed`. Listing joins
against the table in the dequeue query; dependency satisfaction is
monotonic, so no locking is needed beyond the row lock on the dependency.

### 5.9 Wake-ups

Enqueue and state transitions to `available` emit `NOTIFY jobs_available`.
Workers `LISTEN` and re-run dequeue immediately. Polling (1 s idle backoff,
capped) is the fallback for missed notifications and for NOTIFY-suppressed
maintenance sweeps.

## 6. Interfaces

- **gRPC API** (`Enqueue`, `Dequeue`-internal, `GetJob`, `ListJobs`,
  `CancelJob`, `RetryJob`, heartbeats, completion) with a grpc-gateway
  HTTP/JSON layer.
- **Go client SDK**: typed wrapper, automatic heartbeat management, context
  cancellation, retry on transient API errors.
- **Worker library**: register handlers by kind, graceful shutdown (drain
  with budget, then abort), context cancellation per job.
- **CLI**: `conveyor enqueue|jobs|cancel|retry|queues|workers|schedule`.
- **Dashboard**: small web UI (queue depths, throughput, latency
  percentiles, retries, DLQ, retry/discard actions) served by the API server.

## 7. Correctness

### 7.1 Invariants

1. A job reaches a terminal state or stays retryable (no loss, no stuck
   running beyond lease expiry + reaper interval).
2. At most one worker holds a valid lease on a job at any time.
3. Only the current lease holder can change a running job's outcome.
4. An idempotency key creates at most one job per queue.
5. A job's effect is applied at most once.

### 7.2 Formal model

`spec/tla/Conveyor.tla` models enqueue, dequeue, heartbeat, lease expiry,
completion, retry, worker crash, and reaper. It is model-checked with TLC
against invariants 1–5 on a small configuration (3 jobs, 2 workers, bounded
lease versions). The model intentionally abstracts: SQL implementation
details, priorities, cron, workflows, rate limits, and the network between
API and workers (treated as atomic RPC). See §10 limitations.

### 7.3 Fault injection

A harness runs real API servers and workers as processes and injects:
`kill -9` on workers, `SIGSTOP` (lease-limbo), network faults via Toxiproxy
(latency, drops, partitions) between workers and Postgres, Postgres restarts,
and clock skew. Each job's side effect appends to `ledger` with the fencing
token. A checker reads the committed history and asserts: no job lost, no
duplicate effect, every completion carried the then-current fencing token.

## 8. Performance plan

- Load generator (`cmd/loadgen`) measuring enqueue→start latency (p50/p95/
  p99) and throughput, sweeping worker count, batch size, payload size.
- Comparison against River (Postgres-backed Go queue) on the same hardware,
  explaining differences.
- One bottleneck identified and fixed (candidate: `FOR UPDATE SKIP LOCKED`
  contention; mitigation candidates: batched dequeue, advisory-lock keyed
  shards, `NOTIFY`-driven thinning), reported with before/after numbers.

## 9. Observability, security, operations

- Prometheus metrics: queue depth by state, transition counts, per-transition
  latency histograms, retries, dead-letter count, lease expirations, leader
  changes, request rate/errors/duration.
- OpenTelemetry trace context carried in `jobs.trace_context` so a trace
  spans enqueue → dequeue → execution.
- Structured JSON logs with request IDs; health (`/healthz`) and readiness
  (`/readyz`) endpoints.
- Grafana dashboards committed under `dashboards/`.
- Auth: API tokens (Bearer), payload size limits (default 64 KiB), TLS in
  deployment, least-privilege DB roles (app role with row-level grants on the
  tables it needs; maintenance role separate).
- Dependency and container scanning in CI; `govulncheck` on every PR.

## 10. Alternatives considered

| Option | Why not (vs. Postgres-backed) |
|--------|-------------------------------|
| **Redis-backed queue** | Fast, but durability semantics weaker; needs blocking commands for dequeue at scale; we'd reimplement leasing/fencing and still do idempotency elsewhere. Keeping one store avoids a cross-system atomicity problem. |
| **Kafka as the queue** | High throughput, but consumer groups reprocess on rebalance; exactly-once effects still need an external store; leases/fencing and per-job retry/DLQ semantics are awkward. Better fit for event streaming than per-job delivery. |
| **Database-agnostic design** (pluggable storage) | We would dilute correctness guarantees to the lowest common denominator and double the test matrix. Postgres-specific features (`SKIP LOCKED`, advisory locks, `LISTEN/NOTIFY`) are exactly what make the invariants tractable. Postgres is already deployed nearly everywhere. |
| **River (adopt as-is)** | River is a strong reference; we take its design ideas (leases, leader election, maintenance loop) as a baseline for comparison in Phase 5. It is MIT-licensed, so borrowing ideas with attribution is fine; the project goal is to demonstrate the SDE correctness work, which a wrapper would hide. |

## 11. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Fencing gaps where handlers call external systems without dedupe | Document the contract: handlers must pass idempotency key + fencing token to external calls; provide helper wrappers. |
| Postgres becomes bottleneck at high enqueue rates | Batch dequeue, archived table, partition by queue; horizontal sharding by queue (documented, not implemented in v1). |
| TLA+ model diverges from implementation | The state machine package is the single source of transitions; model-checked properties are also asserted by integration tests; fault-injection checker catches drift empirically. |
| Zombie worker flapping (lease loss → retry success → effect double-apply) | Fencing + idempotency in the effect path; tested by the SIGSTOP fault case. |
| Clock skew breaks lease expiry | Leases use Postgres `now()` (server clock) consistently, never worker clock; documented in the runbook. |

## 12. Hosting plan (free tier only)

- **Primary**: Oracle Cloud Always Free VM (ARM) running Docker Compose:
  Postgres, API server, dashboard, maintenance worker, Grafana, worker fleet.
  Public demo with AUTH token for visitors; chaos button implemented as a
  guarded endpoint that SIGKILLs one synthetic worker process.
- **Fallback/static assets**: Netlify or Vercel for the marketing/docs page
  only (not the live system).
- **DB (if VM proves unreliable)**: Neon free tier Postgres + API on Render
  free web service. Evaluate in Phase 6; the VM is the plan of record.

## 13. Threat model (summary)

- **Spoofed/replayed API calls**: require Bearer token on every write; tokens
  from env, never logged.
- **Oversized payloads**: request size limit 64 KiB at the gateway, enforced
  again in the API layer.
- **Malicious job args**: arguments are JSON, executed by user-registered
  handlers only; Conveyor never evals args server-side.
- **DoS via queue flooding**: per-token rate limit and payload limits; still
  out of scope to defend fully on free hosting.
- **DB credential theft**: least-privilege roles; TLS to Postgres in
  deployment; secrets in environment, `.env` gitignored.
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
