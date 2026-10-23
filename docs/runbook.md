# Conveyor runbook

Everything Conveyor knows lives in Postgres. API servers and workers are
stateless, so most incidents are fixed by restarting a process, and none
needs a data repair as long as Postgres is healthy.

## Deploy

### First deploy on a VM (the demo host)

```bash
# Ubuntu 24.04 on an Oracle Cloud Always Free Ampere A1 instance
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker "$USER" && newgrp docker
git clone https://github.com/pavan-dangeti/Conveyor.git && cd Conveyor
cp .env.example .env
# Replace every change-me with: openssl rand -hex 24
# Set DOMAIN to the host's DNS name (Caddy then obtains a Let's Encrypt certificate).
docker compose -f compose-demo.yml up -d --build
```

Open ports 80 and 443 in both the instance's security list and its host
firewall (`sudo iptables -I INPUT -p tcp -m multiport --dports 80,443 -j ACCEPT`
on Oracle's Ubuntu image). Ports 8080 and 9090 bind to 127.0.0.1 only.

### Routine deploy

```bash
git pull
docker compose -f compose-demo.yml up -d --build api
```

The API runs migrations at start-up as the schema owner
(`MIGRATIONS_DATABASE_URL`) and then serves as `conveyor_app`, which can read
and write rows but not change the schema. Migrations are forward-only and
must stay compatible with the previous binary: add columns and indexes, and
never drop or rename in the same release that stops using them.

### Rollback

```bash
git checkout <previous-tag>
docker compose -f compose-demo.yml up -d --build api
```

A rollback is a binary change only, because migrations are additive. Jobs in
flight during the switch are protected by leases: a worker killed mid-job
leaves its lease to expire, and the next worker retries the job.

## Health

| check | healthy | meaning when not |
|---|---|---|
| `GET /healthz` | 200 | process is down or wedged |
| `GET /readyz` | 200 | Postgres unreachable from the API (503) |
| `conveyor_running_expired_lease` | 0 most of the time | the reaper is not running: no leader |
| `sum(conveyor_is_leader)` | exactly 1 | 0: maintenance stopped; more than 1 cannot happen with one Postgres |
| `conveyor_queue_depth{state="available"}` | flat or falling | workers can't keep up, or none are running |
| `/demo/check` (demo only) | `"violations": null` | an exactly-once invariant failed: treat as a sev-1 |

The Grafana dashboard (`dashboards/conveyor.json`, provisioned by
`--profile observability`) has a panel for each.

## Common failures

| symptom | likely cause | diagnose | fix |
|---|---|---|---|
| `/readyz` returns 503 `database unreachable` | Postgres down or out of connections | `docker compose -f compose-demo.yml ps postgres`; `SELECT count(*) FROM pg_stat_activity` | restart Postgres; jobs resume, and leases that lapsed meanwhile are reaped and retried |
| `available` grows, `running` is 0 | no workers, or workers can't reach Postgres | worker logs for `dequeue` errors; `SELECT lease_owner, count(*) FROM jobs WHERE state='running' GROUP BY 1` | restart workers; check the DSN and role grants |
| jobs stay `scheduled` or `retryable` past `run_at` | no maintenance leader | `sum(conveyor_is_leader)`; `SELECT pid FROM pg_locks WHERE locktype='advisory'` | restart one API replica; the next candidate takes the advisory lock within a tick |
| `running` with expired leases does not drain | same as above | `conveyor_running_expired_lease` stays above 0 | as above |
| `discarded` grows | a handler fails every attempt | `conveyor jobs --state discarded`; `SELECT error_history FROM jobs WHERE id = …` | fix the handler, then `conveyor retry <id>` |
| `stale completions rejected by fencing` in worker logs | a worker paused longer than its lease (GC pause, SIGSTOP, partition) | count the warnings; compare handler time with the lease | expected and safe; if frequent, raise `WithLease` above the handler's p99 |
| `RESOURCE_EXHAUSTED: rate limit exceeded` | one client above `CONVEYOR_RATE_LIMIT` requests per second | API access logs, field `client` | raise the limit or slow the client |
| `UNAUTHENTICATED` | missing or wrong bearer token | the client must send `authorization: Bearer …` | align `CONVEYOR_API_TOKEN` |
| enqueue-to-start p99 rises with no backlog | LISTEN connection lost, workers on the poll fallback | worker logs for `listen` warnings | recovers on reconnect (1 s); if it persists, check Postgres connection limits |

Every API request is logged as one JSON line with `request_id`, `method`,
`code`, `client` and `duration_ms`. Callers may send `X-Request-Id`; it is
echoed back, so a client-side error can be matched to its server log line.

## Tracing

Run with `--profile observability` and
`OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`, then open Jaeger on
`127.0.0.1:16686`. One trace covers the HTTP request, the Enqueue RPC and the
job's execution on whichever worker ran it, because the trace context is
stored in `jobs.trace_context`.

## Data checks

```sql
-- Should be 0 outside the reaper's 500 ms tick.
SELECT count(*) FROM jobs WHERE state = 'running' AND lease_expiry < now() - interval '5 seconds';
-- Exactly-once: no job has two effects (the primary key enforces it; this verifies it).
SELECT job_id FROM ledger GROUP BY job_id HAVING count(*) > 1;
-- Hot-table size: terminal rows older than a day are archived every tick.
SELECT state, count(*) FROM jobs GROUP BY 1;
```

## Rotating secrets

- **API token:** update `CONVEYOR_API_TOKEN` in `.env`, then
  `docker compose -f compose-demo.yml up -d api`. Workers don't use it.
- **Database passwords:** run `ALTER ROLE conveyor_app PASSWORD '…'` as the
  owner, update `APP_DB_PASSWORD`, and restart `api`. The init script only
  runs on an empty data directory, so changing `.env` alone changes nothing.

`.env` is gitignored. Tokens are never logged and never appear in metrics.
