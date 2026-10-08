# Conveyor TLA+ model

## What it models
Enqueue availability, Dequeue (dequeue sets owner, fencing token++, attempts++), Heartbeat, ExpireLease (reaper), Complete (guarded by owner+version+leaseValid), Fail, RequeueBackoffElapsed, Crash (invalidates the crashed worker's leases, mirroring the reaper), Restart (worker loses in-memory version map).

## Invariants checked
- EffectOnce: a job's effect is applied at most once.
- EffectImpliesCompleted: an effect implies completion.
- RunningImpliesValidLease: a running job has a valid lease and a live owner.
- RunningLeaseHolderAgrees: a job's running holder agrees with its current fencing token.

## How to run
```
curl -sLfO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
java -cp tla2tools.jar tlc2.TLC Conveyor.tla -config Conveyor.cfg -deadlock
```
Current result: 4082/14817 distinct states, no violations. CI runs this on every PR.

## What it does not cover
- Priorities, delayed/scheduled state, cron, workflow dependencies, rate limits (implementation-level).
- SQL details (SKIP LOCKED semantics, transaction isolation).
- Networks between workers and Postgres; heartbeat timing.
- Liveness (eventual completion): requires fairness assumptions about crashes;
  deferred to a follow-up because a realistic model must allow infinitely many
  crashes, which makes unconditional liveness false.
