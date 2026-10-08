# ADR 0003: TLA+ model checked in CI on a small configuration

## Status
Accepted (2026-10-08)

## Context
The lease/fencing protocol has subtle interleavings (crash, expiry, zombie).
Unit tests cannot enumerate them.

## Decision
Commit `spec/tla/Conveyor.tla` and a small `.cfg`; run TLC in CI on every PR
with a bounded configuration (3 jobs, 2 workers, bounded versions).

## Consequences
- Protocol regressions fail CI before merge.
- The model abstracts SQL, priorities, cron, workflows, and the network;
  those are covered by integration and fault-injection tests instead.
- CI time is bounded by the small configuration; larger runs are manual.
