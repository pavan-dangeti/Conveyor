# ADR 0004: Batch completions to amortise WAL flushes

## Status
Accepted (2026-10-23)

## Context
Every job costs at least three commits: enqueue, claim, and completion.
With `synchronous_commit = on`, each commit waits for a WAL flush. The
benchmark samples `pg_stat_activity` every 10 ms while it runs; on the
baseline configuration with one completion per commit, the most sampled
waits after CPU were `LWLock:WALWrite` and `IO:WalSync`, i.e. backends
queueing to flush the WAL (`results/bench-2026-10-23.json`, config
"conveyor before: completion per commit").

The same sweep exposed a second, unrelated problem: the 50 ms NOTIFY
debounce was leading-edge only, so enqueues inside a window produced no
wake-up and idle workers slept until their 200 ms poll.
`TestNotifyDebounceHasTrailingEdge` reproduces it.

## Decision
- The worker library sends successful completions to a per-worker
  completer that groups those finishing within 2 ms (at most 64) into one
  `UPDATE ... FROM unnest(ids, tokens)`. Every row keeps its own fencing
  guard, so batching changes how many commits happen, not who may commit.
  Handlers that commit their own effect transaction (`CompleteTx`) are
  unaffected.
- The NOTIFY debounce gains a trailing edge and a 10 ms window, so a burst
  costs at most one extra notification and no enqueue waits for a poll.
- Durability is not traded: `synchronous_commit` stays on, and every
  enqueue is still acknowledged only after its own commit.

## Consequences
- Measured on the same host, 5 reps of 2000 jobs: throughput went from
  2024 ± 138 to 2697 ± 107 jobs/s (95% CI); p99 enqueue-to-start latency
  was unchanged within noise (12.7 → 12.4 ms).
- What moved is contention for the WAL write lock: `LWLock:WALWrite` fell
  from 27.4% to 16.4% of wait samples. The flush itself did not go away:
  `IO:WalSync` rose from 14.4% to 23.3%, so WAL is still about 40% of
  samples and still the bottleneck. Fewer, larger flushes is the expected
  shape; the next step is fewer commits per job (see design §8).
- A completion can be acknowledged up to 2 ms later than before. If the
  worker dies in that window the job's lease expires and it runs again,
  which at-least-once delivery already allows.
- `worker.WithoutBatchedCompletion` and `loadgen -batched-complete=false`
  keep the old path so the comparison stays reproducible.
