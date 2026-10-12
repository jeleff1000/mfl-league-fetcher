# Live Ops Clock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Dispatch the public Live NFL Ops workflow at 03:00 America/New_York and verify actual runner-start latency.

**Architecture:** Use the existing public repository's `FLY_API_TOKEN` in a manual provisioning workflow. First run a disposable clock/runner timing probe, then deploy a clock which dispatches the existing worker. Preserve the current job until the replacement trigger is proven.

**Tech Stack:** Python standard library, Fly Machines REST API, GitHub REST API, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-12-3am-live-ops-dispatch-design.md`

## Global Constraints

- All code and executing workflows are in public `jeleff1000/mfl-league-fetcher`, `main`.
- Clock/probe have no database credentials, volume mounts, or exposed services.
- Probe resources are destroyed in `finally`; exact app/machine identifiers are recorded before cleanup.
- Pilot startup deadline is 40 seconds; dispatch-to-start SLA is 60 seconds.
- Do not modify existing Fly database machines.
- The production Ops job remains unchanged until runtime capacity is verified.

## Review Focus

- DST transitions: calculate 03:00 with `America/New_York`, never a fixed UTC offset.
- Ambiguous dispatch acknowledgment: reuse one date key and inspect existing runs before retrying.
- Clock restart: persist successful dispatch date, and catch up a missed date.
- Runner capacity: the current baseline is 6.5 GB; a 512 MB runner proves only no-op timing.
- Token scope: existence of a secret does not prove permission to create a separate Fly app.

### Task 1: Credential and app permission probe

**Files:** Create `.github/workflows/probe_live_ops_scheduler.yml`, `scripts/probe_live_ops_scheduler.py`.

**Interfaces:** `python scripts/probe_live_ops_scheduler.py inspect` consumes `FLY_API_TOKEN`; prints only nonsecret app/org and status information.

- [ ] Read the existing app through `GET https://api.machines.dev/v1/apps/league-history-duckdb` with a 10-second request limit.
- [ ] Expose only the organization slug and HTTP status, never the credential or raw machine environment.
- [ ] Dispatch the manual workflow on `main`, verify the access receipt, and identify whether a separate disposable app can be created.

### Task 2: Disposable clock-to-runner probe

**Files:** Extend `scripts/probe_live_ops_scheduler.py`, create `.github/workflows/live_ops_runner_probe.yml`.

**Interfaces:** The control workflow consumes a short-lived GitHub runner registration token; the no-op workflow consumes timestamps and a unique runner label.

- [ ] Register an ephemeral runner with an exclusive probe label and no database secrets.
- [ ] Create one isolated Fly app/machine using the official Actions runner image, 512 MB, no volumes/services.
- [ ] Wait up to 40 seconds for the runner to listen, send a timed dispatch, and record actual job start.
- [ ] Verify the probe job has no checkout, database environment, or publication step.
- [ ] Destroy the exact probe app/machine and remove any leftover GitHub runner registration.

### Task 3: Production clock and handoff

**Files:** Create `scripts/live_ops_clock.py`, `tests/test_live_ops_clock.py`, a manual clock provisioning action; modify the existing workflow only when the probe supports it.

**Interfaces:** The clock sends `repository_dispatch` `live-nfl-ops-refresh` with a date key and explicit `game_date`.

- [ ] Unit-test DST-aware next-run calculation, duplicate date suppression, restart catch-up, and dispatch acknowledgment handling.
- [ ] Deploy one 256 MB clock with a scoped GitHub dispatch credential and durable date marker.
- [ ] Verify a no-op timed dispatch from that machine, including clock and runner timestamps.
- [ ] Select the production runner from measured start latency and actual job requirements; record any deviation from the initially proposed 512 MB runner.
- [ ] Remove the original schedule only after the new trigger is operational; verify remote `main`, deployed source, and next scheduled time.

## Execution record

- Ruling: a 512 MB machine cannot run the existing Ops data job because its baseline alone is 6.5 GB. That capacity is used only for the disposable timing probe; production runner selection depends on the evidence.
- Approved execution: inline, using the public repository's existing deployment secret.
