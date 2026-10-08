# Conveyor runbook

## Deploy / rollback

One host (compose):

```
docker compose -p conveyor-demo -f compose-demo.yml up --build -d
docker compose -p conveyor-demo -f compose-demo.yml ps
docker compose -p conveyor-demo -f compose-demo.yml down        # rollback = previous image tag
```

For a pinned rollout on a VM: build `conveyor-api:<git-sha>` from the Dockerfile, deploy by image tag (compose or systemd), rollback by redeploying the previous tag. State is all in Postgres, so rollback is a binary change, not a schema change. Migrations are versioned and forward-only; never edit an already-applied migration.

Health checks: `GET /healthz` (liveness), `GET /readyz` (Postgres reachability, 503 when down).

## Common symptoms and diagnosis

| symptom | what it means | what to check/do |
|---|---|---|
| API 503 `readyz` fails | Postgres unreachable | container health, network policy, toxiproxy in dev |
| Jobs stuck in `running`, others completing | isolated zombie (`SIGSTOP`), or lease long past expiry | reaper runs every 500 ms under advisory lock; check `conveyor_jobs_running_expired_lease` — should always be 0 |
| All jobs pile up in `available`, p99 latency spiked | dequeue loop broken on one queue shard | leader-lock: query `pg_stat_activity` the advisory-cookie SELECT 1; restart API which re-runs `maintenance.RunElectedLoop` |
| API returns `code 16 UNAUTHENTICATED` | wrong/missing token | set `CONVEYOR_API_TOKEN` consistently on API and clients |
| `NOTIFY` misses after Postgres restart | worker reconnect | normal: workers poll with exponential backoff capped at 1 s until new connections come up |
| Backlog of `discarded` rows | poison job failing every attempt | inspect `jobs.error_history`, throttle enqueue, `conveyor retry <id>` after the upstream bug is fixed |

## Data integrity checks

- `SELECT count(*) FROM jobs WHERE state='running' AND lease_expiry < now()` must be ~0.
- `SELECT state, count(*) FROM jobs GROUP BY 1` should show a healthy mix; chronic `discarded` points at a handler bug.
- `SELECT count(*), count(DISTINCT job_id) FROM ledger` must be equal (no job applied its effect twice).

## Rotating secrets

`CONVEYOR_API_TOKEN` rotates by redeploying the API after updating env; workers are unaffected (they do not use HTTP auth). `.env` is gitignored; never commit it.

## Metrics to watch

`conveyor_jobs{state=...}`, `conveyor_jobs_running_expired_lease` → alert > 0 for > 5m. Leaders: watch `dc conveyor-api` logs for a second "maintenance leader acquired" line (split-brain should never happen: Postgres advisory lock).
