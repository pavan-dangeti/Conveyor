# ADR 0001: PostgreSQL as the single source of truth

## Status
Accepted (2026-10-08)

## Context
Conveyor needs durable job state, leases, heartbeats, retries, cron, and
idempotent enqueue. Options: Redis, Kafka, Postgres, or a pluggable storage
layer.

## Decision
Use PostgreSQL for all of it: `jobs`, `queues`, `cron_schedules`,
`job_dependencies`, and the fault-injection `ledger`.

## Consequences
- Correctness uses `SELECT ... FOR UPDATE SKIP LOCKED`, advisory locks, and
  `LISTEN/NOTIFY` natively — no second store to keep atomic with.
- One operational dependency; Postgres restart is a well-tested failure path.
- Throughput ceiling lower than Redis/Kafka; mitigated with batched dequeue,
  partial indexes, and archiving the hot table. Sharding by queue is the
  documented escape hatch, not used in v1.
