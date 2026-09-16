# Update League verification follow-up — 2026-09-16

Goal remains active. This is evidence, not a completion declaration.

## Production results on public main 40778c937

- ESPN afi_data: run 35098482421 COMMITTED. Gray's week-1 score changed
  from 151.66 to 152.66; Will's reciprocal opponent score is 152.66.
  Elizabeth + Joe, Meg + Sammie, and RJ + Abdulai remain the published aliases.
  Three manager_overrides rows retain fingerprint 2224674148746225747;
  league_context retains 1624854446757961055.
  Worker phases 44.328s; dispatch 12:53:31 UTC to cache completion 12:55:04:
  **93 seconds, outside the target**. Manual dispatch, not UI evidence.
- Multiplatform mawhinney_s_vixens: run 35098485348 COMMITTED.
  Provider chain resolved Sleeper 2026 ID 1389710321509232641 without a manual ID.
  Added 12 current-season team-weeks; Yahoo 2014–2018 and Sleeper 2019–2025
  remain. Historical fingerprints unchanged:
  matchup 2268/9552066871079800204; player_fantasy 75863/4821979747357022621;
  draft 2136/14352613929664409412; transactions 7984/3938958896113955166;
  league_settings 12/1861406561020092916.
  league_context 1/13199551947667447998 and manager_overrides
  33/4229662928155799528 unchanged.
  Worker phases 57.649s; dispatch 12:53:32 to cache completion 12:55:14:
  **102 seconds, outside the target**. Manual dispatch, not UI evidence.
- ESPN tfl_of_extraordinary_gentleman: run 35098145142 COMMITTED.
  Earlier dispatch-to-cache measurement 79s; worker phases 43.823s.
  Current Fly reconciliation: 15 franchise careers / 2396 games; zero
  discrepancies from season games, wins, losses, points, and latest aliases.
- Current Fly reconciliation also found zero manager-career discrepancies for
  afi_data (12 franchises/12 games) and mawhinney_s_vixens (16/1884 games).
  Player-career weekly totals were checked for games_rostered, fantasy_points,
  manager_lamar and started-player clutch: 432/432 regular/all players in afi,
  2061/2063 in mawhinney, 2750/2753 in tfl. No discrepancies were returned.
  Follow-up should retain explicit null checks as well as numeric tolerances.

## Whole-day recovery inventory

Captured all 53 completed runs returned for the three incremental workflows
with created >= 2026-09-16 (UTC), limit 100 per workflow; each workflow returned
fewer than 100. Parsed the actual INPUT_DB_NAME from each run log.
The full run inventory is update-league-cohort-2026-09-16.json.

10 distinct leagues attempted updates:
- Sleeper: mawhinney_s_vixens, nyu_ffl, agusta_fantasy_league,
  the_fucking_catalina_wine_mixer.
- Yahoo: the_league, fight_club_except_we_do_talk, pass_interferance.
- ESPN: afi_data, tfl_of_extraordinary_gentleman, live_draft_beer_league.

Unrecovered/unverified: agusta, Catalina, fight_club, pass_interferance,
live_draft_beer_league. Catalina's older success predates the historical PPG fix;
green status is not sufficient. Agusta and live_draft_beer have stale
dispatched registry rows with no workflow_run_id, while their actual runs
35052703365 and 35042426172 terminated with claim-ownership errors.
Fight_club run 35058034929 failed transformed provider score validation.
Do not classify any of these as missing credentials without new evidence.

## Atomic homepage finalization implementation

The worker homepage_refresh path still copies full historical league inputs
to the runner. New opt-in fleet-partition-v3 capability reuses the existing
homepage_summary.compute_homepage_frames and canonical scoped writer on the
same Fly publication connection, after shared career aggregations and before
COMMIT. No replacement formulas, synthetic career view or null restoration.

Tests prove historical records, aliases, unrelated-league isolation, caller
rollback, HTTP replay, and rollback of season + career outputs if homepage
generation fails. Missing franchise output and nulling an existing summary
value fail before publication. Older v1/v2 contracts remain unchanged.
V3 bundles omit uploaded scratch career/homepage tables; older servers reject
the unsupported contract.

Validation so far: 50 aggregation/bundle/homepage tests passed; 22 HTTP/fleet
tests passed; refreshed 16 career/homepage tests passed after fixture change.
Legacy pandas fragmentation warnings remain and are not called clean output.
No independent review agent is available in this session; independent review
and production canary for this new capability remain outstanding.

**Not enabled in workers or deployed at this checkpoint.**
Next: deployment dependency check, server deployment, v3 worker wiring with
matching deferred-validation contract, full-chain canaries, and remaining
cohort recovery. Actual UI canaries, watermark/manual parity, partial-week and
older-correction proof, complete ranking validation, speed, and original goal
acceptance requirements remain open.

Repository identity checked through GitHub API: jeleff1000/league-history-workers
redirects to jeleff1000/mfl-league-fetcher; it is public and defaults to main.

## Server deployment and worker activation checkpoint

Server revision 3eb3ced3722c1d9df8a5184a00a9cd0140fb5bac deployed successfully
in public workflow 35100693579. Docker context 7.14 MB; homepage module import
passed in the actual image. Image deployment-01M2N5PM82QF5S9P8PPC42TRKN.
After deployment /ready reports serving, accepting queries, zero active reads,
OPS writes or delta publications.

Yahoo/ESPN/Sleeper worker changes now request v3 and no longer call the
remote-history homepage helper. They stage only active partitions/identity
outputs, while the server returns career and homepage publication counts.
The preflight checks local provider and career outputs but explicitly reports
homepage validation as atomic_fly (not a fabricated local summary count).
160 validation/refresh/worker tests passed; Ruff passed. Production execution
of the activated worker revisions remains to be verified.

Root application checkout predates all three weekly scripts and both weekly
core modules. Server/shared capability edits were mirrored without overwriting
unrelated root edits; do not invent a partial copy of the missing worker stack.
App-main/caller alignment is still an explicit audit item.

## Live v3 canary handle and remaining evidence

Worker revision 20f71e035bcd7ce8e878235063b26eeb910a7ae7 is pushed to public
main and uses the deployed server capability. Manual Sleeper canary:
35101194997 (nyu_ffl), dispatched 2026-09-16T13:19:39Z; refresh began 13:20:31Z.
Poll this handle to terminal state; do not redispatch because observation times out.

Browser checks returned no enabled browsers, and getBrowser for the live
nyu_ffl URL returned "No browser is available". Actual UI verification remains
unavailable; no API call or manual workflow is being counted as a UI test.

A combined historical fingerprint query across five unrecovered leagues timed
out with HTTP 504. Its local process exited; no snapshot result was produced.
Do not repeat that combined query or claim preservation from it. Use smaller,
single-league checks outside the timed canary window. The audit read overlapped
the canary and can affect timing; report raw elapsed time regardless.

## Live v3 result and quiet repeat

nyu_ffl run 35101194997 committed all five homepage outputs and careers on Fly.
Worker total 110.585s: player_ops_cache 58.655s, publication 21.88s,
shared transformations 8.279s. Dispatch 13:19:39 to cache completion 13:22:26:
167s, misses target; the overlapping timed-out diagnostic is recorded above.

After the run, single-league historical checks each took 0.5–0.8s and all
pre-2026 fingerprints matched the prior baseline exactly. League context and
empty overrides were unchanged. All 12 homepage managers show 9 seasons,
rather than one current week. Nine weekly PPG nulls are zero-point bench/IR
players; their historical PPG availability still needs a semantic audit.
Do not call every ranking column verified.

Quiet same-revision repeat 35101677473:
- dispatch 13:24:10; worker 13:24:31–13:25:10; cache completed 13:25:14 UTC.
- 64s dispatch-to-cache; 38.447s processing; 7.987s shared transforms.
- server careers 2.9628s; full-history homepage 8.2523s.
- COMMITTED, 12 manager rankings/profiles/current standings, one summary,
  20 rivalries. This is manual evidence, not UI-click or complete semantic
  idempotence proof (an exact before/after current-output value hash is pending).

Current credential evidence: pass_interferance has no row in
___ops.main.league_credentials. The latest attempted run 35048669674 logged
"pass_interferance not found in Yahoo credentials". Fight Club DOES have an
encrypted refresh-token record; its earlier score-validation failure must not
be labeled an owner credential blocker.

Successful single-league recovery baselines for the five pending targets are
saved verbatim in update-league-recovery-baseline-2026-09-16.ndjson (integer
fingerprints preserved without JSON float round-tripping).
Agusta has 3188 historical draft rows but no pre-2026 matchup/player/settings
rows in this baseline; do not claim it already has complete played history.

Live recovery handle: ESPN live_draft_beer_league run 35101976587, public
20f71e035, dispatched 13:26:57 UTC. Poll the same handle to completion.

## ESPN rollover rejection and repeated validation retries

35101976587 terminated FAILED at 13:34:42 UTC. Shared transformations finished
13:28:03 (29 seconds after refresh start); the remaining delay was six identical
publication attempts. Every attempt returned HTTP 500 with
"Homepage summary lost populated value: season_best_pickup_player".
Backoffs were 10, 20, 40, 80, and 120 seconds. No successful publication claimed.

Scoped Fly read confirmed the persisted summary was data_year=2025, with
season_best_pickup_player=Rico Dowdle and alltime_best_pickup_player=Justin Herbert.
The guard incorrectly required an old-season highlight to remain populated
when the shared builder advanced the summary to 2026. Current-season highlight
fields now may reset only on a forward data_year transition. Same-season and
all-time populated-value checks remain fail-closed; no stale values are copied.

Deterministic homepage validation now maps to HTTP 422 after atomic rollback,
not transient HTTP 500. Real FlyTarget-to-TestClient integration proves one
attempt, unchanged stored summary, and no retry for lost homepage values.
Transport/attachment failures retain their separate transient handling.

Red/green evidence: rollover first failed on season_best_pickup_player;
HTTP rollback test first failed with 500 instead of 422. After implementation:
18 shared career/homepage tests passed; three focused rollover/same-season/
all-time cases passed; 22 HTTP/fleet tests passed; expanded eight HTTP/caller
tests passed. Ruff and git diff --check passed. Existing pandas fragmentation
warnings remain. Production changes mirrored to root main without staging
unrelated work. Deployment and retry evidence follow separately.
