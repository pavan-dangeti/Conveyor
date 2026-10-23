# TLA+ model of the lease and fencing protocol

`Conveyor.tla` is the protocol; `MCConveyor.tla` and `MCConveyor.cfg` are the
bounded model TLC checks on every pull request.

## What it models

| action | implementation |
|---|---|
| `Enqueue(j)` | `INSERT … ON CONFLICT (queue, idempotency_key) DO NOTHING` |
| `Dequeue(w, j)` | claim with `SKIP LOCKED`, `lease_version + 1`, `attempt + 1` |
| `Heartbeat(w, j)` | lease extension guarded by owner and token |
| `LoseLease(w, j)` | a guarded write matches no row; the worker drops the job |
| `Complete(w, j)` | effect and completion in one guarded transaction (`CompleteTx`) |
| `Fail(w, j)` | to `retryable`, or `discarded` once attempts are spent |
| `Expire(j)` | time passes without a heartbeat (allowed at any moment) |
| `Reap(j)` | the maintenance leader moves a lapsed `running` job on |
| `RetryDue(j)` | backoff elapsed |
| `Crash(w)`, `Restart(w)` | `kill -9`: tokens lost, leases stay live until they lapse |

## What TLC checks

| property | kind | meaning |
|---|---|---|
| `IdempotentEnqueue` | invariant | one job per idempotency key |
| `SingleHolder` | invariant | at most one worker holds a running job's current token |
| `EffectOnce`, `EffectIffCompleted` | invariant | an effect is applied once, exactly when the job completes |
| `OnlyHolderDecides` | action property | a running job's outcome changes only through its current holder, or the reaper after the lease lapsed |
| `EventuallyTerminal` | liveness | every created job reaches `completed` or `discarded` |

Configuration: three enqueue attempts (two share a key), two workers, two
attempts per job, at most one crash. Liveness assumes weak fairness for
processing steps and for lease expiry, but none for heartbeats, failures or
crashes. Without fairness on `Expire`, TLC finds a real counterexample: a
worker crashes while holding a lease and the job stays `running` forever.
That is why liveness rests on leases always lapsing eventually.

`scripts/tlc.sh` (`make tlc`, and CI) runs the model and then a mutant whose
write guard ignores the fencing token. The mutant must fail, and does: TLC
shows a zombie deciding a job's outcome (`OnlyHolderDecides`). That keeps
the model from passing vacuously. Latest output: `results/tlc-2026-10-23.txt`
(19,530 distinct states, no error).

## What it does not cover

- SQL semantics: `SKIP LOCKED`, isolation levels and the atomicity of each
  statement are assumed, not modelled.
- Priorities, delays, cron, workflow dependencies, rate and concurrency
  limits, archiving, and batched completion. These are covered by
  integration tests and the fault-injection harness.
- Real time. Lease expiry is an arbitrary event, which is strictly more
  adversarial than real clocks for safety.
- Effects outside the guarded transaction, for example calls to external
  APIs. Those need the external system to deduplicate on the fencing token
  or idempotency key; the model cannot help there.
- Larger configurations, which are tractable (each run here takes about a
  second) but are not part of CI.
