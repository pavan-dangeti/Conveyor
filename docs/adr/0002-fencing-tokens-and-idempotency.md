# ADR 0002: Fencing tokens + idempotency keys for exactly-once effects

## Status
Accepted (2026-10-08)

## Context
At-least-once delivery means a job can be executed by more than one worker
(lease expiry, zombie). Effects must still happen at most once.

## Decision
- Every dequeue issues a monotonically increasing `lease_version` (fencing
  token). All outcome writes are guarded by `(owner, version)`.
- `(queue, idempotency_key)` is unique; enqueue is idempotent.
- Handlers and external calls must propagate the idempotency key / fencing
  token; documented contract.

## Consequences
- A zombie's completion is a no-op (0 rows) — it cannot overwrite a newer
  result.
- Effects in external non-transactional systems are the handler's
  responsibility; Conveyor provides helpers but cannot enforce.
