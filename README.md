# Conveyor

**Problem:** background jobs (email, payments, ML batches) must survive worker crashes, network partitions, and Postgres restarts — no job lost, every job's effect applied exactly once.

**Live demo:** `docker compose -p conveyor-demo -f compose-demo.yml up` on a free-tier host, then open `http://<host>:8080/`. (Hosted deployment recipe is below.)

![Conveyor dashboard returning work to jobs](docs/demo.gif)

## Why it exists

Exactly-once *delivery* over an unreliable network is provably impossible; Conveyor instead guarantees:

1. every accepted job is eventually executed at least once; and
2. every job's *effect* (ledger insert + state transition) commits at most once, even when a "zombie" worker completes after losing its lease.

The distinction — and the mechanism (idempotency keys + fencing tokens) — is in `docs/design.md` §5.4.

## Results

| system | workers | jobs | throughput | p50 enqueue→start | p95 | p99 |
|---|---|---|---|---|---|---|
| Conveyor (debounced NOTIFY) | 4 loops × 8 in-flight | 500 | **1243/s** | 23 ms | 48 ms | 53 ms |
| Conveyor (NOTIFY per enqueue) | same | 500 | 789/s | 1 ms | 7 ms | 13 ms |
| River v0.49.0 (FetchCooldown 100ms) | MaxWorkers 32 | 500 | 298/s | 591 ms | 1088 ms | 1165 ms |
| River v0.49.0 (FetchCooldown 1ms) | MaxWorkers 32 | 500 | 643/s | 2 ms | 3 ms | 5 ms |

All numbers on the same Mac (16 GB, Docker Desktop, Postgres 17-alpine on the same Docker host). Reproduce:

```
# Terminal 1
docker compose up -d postgres          # if running locally; demo stack uses POSTGRES_DB conveyor
# Terminal 2
JOBS=500 WORKERS=4 BATCH=8 \
  DATABASE_URL='postgres://conveyor:conveyor@127.0.0.1:55433/conveyor?sslmode=disable' \
  go run ./cmd/loadgen
JOBS=500 DATABASE_URL='postgres://conveyor:conveyor@127.0.0.1:55433/river?sslmode=disable' \
  go run ./cmd/riverbench
```

Raw outputs: `results/benchmarks-2026-10-08.json`. Phase 4 publishes fault-injection results in `results/fault-*.json` — all 60 jobs completed under `kill -9`, `SIGSTOP/SIGCONT`, a Postgres stop/start, and 5% connection drops at 30 ms latency, with zero fencing-token violations.

## Architecture

```mermaid
flowchart LR
  producer((Producer)) -->|gRPC/JSON| api[conveyor-api: gRPC :9090, HTTP :8080]
  api --> pg[(Postgres 17)]
  workers[workers: kill -9 safe] -->|SKIP LOCKED + leases| pg
  workers -->|heartbeat + guarded outcome writes| pg
  api -->|pg advisory lock: leader| maintenance[maintenance: promote scheduled, reap leases, archive, expand cron]
  maintenance --> pg
```

Invariants and the TLA+ model of the protocol are in `docs/design.md` §7.

## Development

| command | what |
|---|---|
| `make build` | compile all |
| `make test` | `go test -race ./...` (integration tests spin real Postgres containers) |
| `make vet` / `make staticcheck` / `golangci-lint run` | static analysis (all enforced in CI) |
| `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` | vulnerability scan (wired in CI) |

Fault injection (full, incl. kill -9/SIGSTOP/Postgres restart/drops) runs as part of `go test ./internal/fault`.

Formal model check (CI aswell):

```
curl -sLfO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
cd spec/tla && java -cp ../../tla2tools.jar tlc2.TLC Conveyor.tla -config Conveyor.cfg -deadlock
```

## Run the demo stack

```
docker compose -p conveyor-demo -f compose-demo.yml up --build
# dashboard: http://localhost:8080/
# chaos:     docker compose -p conveyor-demo -f compose-demo.yml stop worker-2
```

One VM host (e.g. Oracle Always Free Ampere instance): install Docker, copy this repo, run the same two commands, open port 8080, put a reverse proxy in front. Every service runs in containers.

## Limitations

- Single-region; no multi-tenant resource isolation.
- All leases/deadlines use host-side Postgres `now()` — clocks must be roughly sane; Docker Compose co-located deployments are the primary target.
- Zombie worker re-entry can only be fully guarded by the fencing checks *inside the effect commit*; handlers submitting raw side-effects without the guarded transaction (or where an external API has no Idempotency-Key support) need to propagate the fencing token as an idempotency key (`cmd/harness-worker` is the reference).
- Throughput ceiling on a single Postgres is measured ~1.2k jobs/s on the dev Mac; scale by sharding workloads across one Conveyor stack per queue rather than across regions. River does inserts faster at default fetch settings (see the config-dependent numbers) — we trade that to gain the explicit fencing/heartbeat contract.
- Prometheus metrics are a minimal set; no tracing backend is provisioned (trace_context is stored per job and rendered in a job view, not yet exported to a collector).

## Security & operations

Threat model in `docs/design.md` §13; compact runbook at `docs/runbook.md`. HTTP + gRPC transports use a shared Bearer token set via `CONVEYOR_API_TOKEN` at startup; only the local compose runs trusted profile. Payload cap 64 KiB per request. Token is never logged nor surfaced in `/metrics`.

## Layout

```
api/proto/conveyor/v1    gRPC defn + generated code
cmd/                     conveyor-api, conveyor (CLI), loadgen, demoload,
                         harness-worker, riverbench
internal/               api, client, fault, faultproxy, maintenance, migrate,
                         queue, statemachine, store, worker, checker, ui
migrations/             versioned SQL embedded at build time
spec/tla/               TLA+ model + CI-ready cfg
dashboards/             Grafana dashboard (Phase 5 addition)
results/                published benchmark + fault results
docs/                   design, ADRs, demo GIF, runbook
```
