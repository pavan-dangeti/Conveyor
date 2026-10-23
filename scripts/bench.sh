#!/usr/bin/env bash
# Reproduces results/bench-<date>.json: a fresh Postgres 17 container, then
# every Conveyor and River configuration with REPS repetitions each.
#   scripts/bench.sh                     # full sweep
#   REPS=3 JOBS=1000 scripts/bench.sh    # quicker, noisier
set -euo pipefail

REPS=${REPS:-5}
JOBS=${JOBS:-2000}
PORT=${PORT:-55499}
OUT=${OUT:-results/bench-$(date -u +%F).json}
NAME=conveyor-bench-pg
BIN=$(mktemp -d)

cd "$(dirname "$0")/.."
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:5432" \
  -e POSTGRES_USER=conveyor -e POSTGRES_PASSWORD=conveyor -e POSTGRES_DB=conveyor \
  postgres:17-alpine -c max_connections=300 >/dev/null
trap 'docker rm -f "$NAME" >/dev/null; rm -rf "$BIN"' EXIT
until docker exec "$NAME" pg_isready -U conveyor -d conveyor >/dev/null 2>&1; do sleep 0.5; done
sleep 1
docker exec "$NAME" psql -qU conveyor -d conveyor -c 'CREATE DATABASE river' >/dev/null

DSN="postgres://conveyor:conveyor@127.0.0.1:$PORT/conveyor?sslmode=disable"
RIVER_DSN="postgres://conveyor:conveyor@127.0.0.1:$PORT/river?sslmode=disable"
rm -f "$OUT"
go build -o "$BIN/loadgen" ./cmd/loadgen
go build -o "$BIN/riverbench" ./cmd/riverbench

conveyor() { "$BIN/loadgen" -dsn "$DSN" -out "$OUT" -reps "$REPS" -jobs "$JOBS" "$@"; }
river() { "$BIN/riverbench" -dsn "$RIVER_DSN" -out "$OUT" -reps "$REPS" -jobs "$JOBS" "$@"; }

conveyor -name "conveyor baseline" -workers 4 -batch 8 -payload 128
conveyor -name "conveyor before: completion per commit" -workers 4 -batch 8 -payload 128 -batched-complete=false
conveyor -name "conveyor notify per enqueue" -workers 4 -batch 8 -payload 128 -notify-debounce 0
for w in 1 2 8 16; do conveyor -name "conveyor workers=$w" -workers "$w" -batch 8; done
for b in 1 4 16 32; do conveyor -name "conveyor batch=$b" -workers 4 -batch "$b"; done
for p in 1024 16384 65536; do conveyor -name "conveyor payload=$p" -workers 4 -batch 8 -payload "$p"; done
river -name "river default (fetch cooldown 100ms)" -max-workers 32 -fetch-cooldown 100ms
river -name "river fetch cooldown 1ms" -max-workers 32 -fetch-cooldown 1ms

echo "wrote $OUT"
