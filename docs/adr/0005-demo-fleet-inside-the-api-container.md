# ADR 0005: Run the public demo's workers as supervised child processes

## Status
Accepted (2026-10-23)

## Context
The live demo needs a "chaos" button that kills a worker so visitors can
watch its jobs recover. A browser cannot reach the Docker API, and giving
the API container the Docker socket would hand a public endpoint root on
the host.

## Decision
With `CONVEYOR_DEMO=1` the API process starts N `harness-worker` child
processes, restarts any that exit after 8 s, generates a synthetic workload,
and exposes `POST /demo/chaos`, which sends SIGKILL to a random child (at
most once every 5 s). The children are the same binary the fault-injection
harness uses and talk to Postgres like any other worker, so a kill exercises
the real lease-expiry and fencing path. `/demo/check` runs the same
history checker as the harness against the live tables.

The demo also sets `CONVEYOR_PUBLIC_READ=1`: read-only RPCs need no token,
so visitors see the dashboard, while enqueue, cancel, retry and discard
still require it.

## Consequences
- One container to deploy; no host privileges for the public endpoint.
- The demo's workers share a container with the API, so it shows recovery
  from process death, not from host or network failure. The fault harness
  covers those.
- Demo code is compiled into the API binary but inert unless enabled.
