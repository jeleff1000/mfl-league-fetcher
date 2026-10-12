# Exact 3 AM Eastern Live Ops Dispatch

## Objective

Start the existing public `Refresh Live NFL Ops` workflow at 03:00
America/New_York every day during the NFL season without relying on GitHub's
best-effort `schedule` event. The clock must not query or write DuckDB, alter
league data, or replace the existing Ops pipeline.

## Design

One small always-on Fly app, `league-history-ops-runner`, has two processes in
the same 512 MB machine:

1. A timezone-aware clock wakes at 03:00 America/New_York and sends one
   `repository_dispatch` event to `jeleff1000/mfl-league-fetcher` with event
   type `live-nfl-ops-refresh` and a `scheduled_for` date key.
2. A single repo-scoped GitHub Actions self-hosted runner labeled
   `live-nfl-ops` remains connected and accepts only the overnight Ops job.

The existing workflow retains all data fetching, validation, atomic
publication, and idempotency logic. Only its live-Ops job is pinned to
`[self-hosted, live-nfl-ops]`; all import and league-refresh workflows remain
on the existing GitHub-hosted runners.

## Safety Contract

- The clock process has no Fly DuckDB credential and cannot access league
  tables directly. The runner receives the existing GitHub Actions workflow
  secrets only when GitHub starts the approved Ops job; no database credential
  is stored in the clock configuration.
- It receives two least-privilege GitHub credentials: one to register the
  runner and one to dispatch this single workflow event. A GitHub App is
  preferred; a fine-grained repository token is acceptable only if it is
  scoped to `mfl-league-fetcher` and Actions administration/dispatch.
- `scheduled_for` is the idempotency key. The workflow records and rejects a
  duplicate key before its data phase.
- A clock retry is permitted only when GitHub did not acknowledge the dispatch;
  it must reuse the same key.
- The legacy GitHub `schedule` trigger is removed after the Fly clock's first
  successful production run, so there is exactly one overnight authority.
- Failure to register, dispatch, or start a runner raises an alert and does not
  fall back to a second data writer.

## Timing and Observability

The clock emits `scheduled_at`, `dispatch_acknowledged_at`, and the daily key.
The workflow emits `runner_started_at` and `publication_completed_at`. The
target is dispatch at 03:00:00 Eastern and runner start within 60 seconds.
The publication may complete later; its duration remains independently
measured.

## Validation Sequence

1. Provision the machine with the no-op `ops-runner-probe` workflow only.
2. Dispatch that probe manually and verify the public workflow starts on the
   labeled Fly runner without touching Fly DuckDB.
3. Trigger the clock once with a future/test key and verify the full timestamp
   chain and duplicate-key behavior.
4. Run the existing Ops workflow with `apply=false`; verify the runner uses
   the public checkout and creates no database write.
5. Enable the 03:00 Eastern clock, retain the first day's evidence, then
   remove the GitHub `schedule` trigger.

## Required Authority

Provisioning requires a Fly deployment credential and a GitHub App or
fine-grained token capable of registering a repository-scoped Actions runner
and sending `repository_dispatch`. Neither credential is present in this
session.
