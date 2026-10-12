# Exact 3 AM Eastern Live Ops Dispatch

## Objective

Start the existing public `Refresh Live NFL Ops` workflow at 03:00
America/New_York every day during the NFL season without relying on GitHub's
best-effort `schedule` event. The clock must not query or write DuckDB, alter
league data, or replace the existing Ops pipeline.

## Design

One 256 MB always-on Fly app, `league-history-ops-clock`, wakes at 03:00
America/New_York and sends one `repository_dispatch` event to public
`jeleff1000/mfl-league-fetcher`, with event type `live-nfl-ops-refresh`, its
original intended timestamp, explicit game date, and daily correlation key.
A 1 GB Fly volume (the minimum volume allocation) stores only the small
dispatch acknowledgment file. The existing workflow continues on its current
GitHub-hosted runner and retains all data processing and publication logic.

The approved disposable self-hosted runner was tested and removed. Its 9.231
second start latency was essentially identical to the hosted runner's 9.159
seconds. The existing data job reconstructs a 6.5 GB baseline, so a permanent
512 MB runner would be undersized. The measured delay is scheduled-event
delivery, addressed by the external clock; the retained hosted runner's queue
delay is measured separately and is not guaranteed by the clock.

## Safety Contract

- The clock has no Fly DuckDB credential and its app has a separate private
  network. The data runner receives its existing workflow secrets from GitHub.
- The clock stores the app's existing GitHub dispatch credential as an
  encrypted Fly secret. Its runtime API target is fixed to the public worker
  repository. Temporary runner registration tokens are used only for probes.
- `scheduled_for` is the idempotency key. The workflow records and rejects a
  duplicate key before its data phase.
- A clock retry is permitted only when GitHub did not acknowledge the dispatch;
  it must reuse the same key.
- The legacy GitHub `schedule` trigger is removed after the real Fly clock's
  end-to-end no-write dispatch has passed and the production clock is running.
- Dispatch failures are logged with a retry interval; worker failures retain
  the existing GitHub Actions status and notifications.

## Timing and Observability

The clock emits `scheduled_at`, `dispatch_acknowledged_at`, and the daily key.
The workflow emits `runner_started_at` and `publication_completed_at`. The
target is dispatch at 03:00:00 Eastern and runner start within 60 seconds.
The publication may complete later; its duration remains independently
measured.

## Validation Sequence

1. Provision the disposable machine with the no-op runner probe only.
2. Dispatch that probe manually and verify the public workflow starts on the
   labeled Fly runner without touching Fly DuckDB.
3. Trigger the clock once with a future/test key and verify the full timestamp
   chain and duplicate-key behavior.
4. Compare the same clock dispatch with a hosted runner, then remove all
   disposable probe resources.
5. Enable the 03:00 Eastern clock, seed today's marker only from an already
   successful scheduled Ops run, and remove the original `schedule` trigger.
6. The first real overnight data publication remains a separate production
   observation; a no-write timing probe does not prove that publication.

## Required Authority

The public worker repository has `FLY_API_TOKEN` as an Actions secret. A usable
local Fly login is also present in the user's Fly configuration; local
provisioning mints a one-hour organization token from that login. No token
values are printed or committed. The local app's GitHub token can register
temporary runners and dispatch public worker events.
