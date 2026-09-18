# September Update League closure ledger

State: active. Production completion is unproven. This ledger is for league updates, not SuperTable SOTA work.

## Current checkpoint - 2026-09-18 03:00 UTC

**The storage defect is OPEN. This checkpoint supersedes earlier claims that
reaggregating the five tables established durable recovery.**

ESPN full import `35287522419`, attempt 4, for
`i_95_gridlock_league_2k27` failed at publication on public-worker revision
`83cd41dbd1cc0d7c90fcd1ff045206577d5f4dfb`. Provider fetching, expected
records, playoff odds and aggregate processing completed. The delta contained
38 tables / 6.6 MB and took 5.1 seconds to build. Fly rejected publication with
a fatal checkpoint checksum mismatch at physical block 90714112 (computed
5168518579405463287, stored 18392342689821271652). This is the same known
physical block, not a new ESPN fetch defect or a full-database upload.

Read-only reconciliation found the exact bundle
`i_95_gridlock_league_2k27-35287522419-4-280a80409000-b77fa581`
still `VALIDATED`, with `committed_at=null`. League-filtered checks of matchup,
league_settings and player_fantasy_season each returned no rows (about 1 second
per query). Cache finalization was skipped. No unchanged rerun was dispatched.
Fly `/ready` subsequently returned serving, but that is not durability evidence.

A related startup defect is reproduced locally: an exception opening/replaying
the WAL caused automatic WAL quarantine and startup against the older database;
a checkpoint exception was separately swallowed. Existing diagnostic run
`35295551400` lists multiple preserved league WAL quarantines, including 111 MB,
563 MB and 65 MB files. These prove quarantine has occurred, not which specific
publications each file contains. The latest restart has not yet been reconciled.
The pending patch removes automatic quarantine, preserves the WAL in place,
propagates startup checkpoint errors, and prevents pool initialization afterward.
Three regressions failed before the fix; all 9 targeted startup/checkpoint tests
pass in 4.65 seconds afterward. This prevents silent rollback; it does not repair
the block. Deployment is held because fail-closed startup could leave the current
unrepaired production database unavailable. No WAL was moved or deleted here.

Other pending, reviewed weekly changes scope identity reapplication and the six
season rollups to the selected year(s), retain complete history for career and
homepage outputs, reject missing historical rollup keys, and atomically advance
publication generations after the existing five-table scoped recovery. The 56
HTTP/transformation regressions pass (116.33 seconds locally). Related tests
previously passed 35/35. Ruff and diff checks pass. These are not deployed and
their production latency is unverified. The retained-key guard checks coverage,
not numeric correctness; its same-connection production cost is still unmeasured.

Fresh bounded live checks contradict the older recovery baseline:
`tfl_of_extraordinary_gentleman` is missing player-season keys for 2012-2025;
`nyu_ffl` is missing them for 2018-2025. NYU's 2025 season-player table is empty
although its weekly source still includes, for example, Christian McCaffrey's
386.1 points. Yahoo `monsters_of_the_midway` passed retained-key coverage.
Do not deploy the strict guard as if these missing baselines were recovered.

No production writes, database/volume copies, full downloads, snapshots, restarts,
or deployments were performed during this checkpoint. The physical corruption,
retained-history recovery, complete cohort verification, live changed-data
canaries and under-90-second end-to-end target remain open. A proven bounded
storage remedy is still required; retrying the import or rebuilding aggregates
alone does not supply one.

## Current checkpoint - 2026-09-17 09:02 UTC

The stale/unknown-freshness control defect is fixed and live. App main
`48ef0a5db66a16758841c8bd1c20455459cb80d7` makes the existing Update League
component run the guarded, leased production probe before rendering an update
action. It renders no action when the resulting manifest matches publication
and still exposes the action when a real delta exists. The focused real-browser
suite passed 4/4, including both outcomes. Production deployment
`dpl_6HGtAzA3QLUmR4szC73se1fXwMYi` reached Ready and owns the
`leaguehistory.app` aliases. A production browser check now reports no Update
League control for current `agusta_fantasy_league`.

Direct workflow executions now capture the same production source manifest as
UI dispatches instead of publishing without a freshness record. Public worker
main `dd4aec32e58b743b6a339277b39849b643796ca7` adds one stdlib helper and one
pre-claim step shared by Yahoo OAuth, ESPN, and Sleeper; app mirror
`2ac5fcdf6464aec7e3ead2200ae8710995db624d` contains byte-identical copies.
The focused workflow/helper suite passed 69 tests and all three YAML documents
parsed.

Actual production UI canary `35202162447` used the exact public SHA above. It
probed, dispatched, refreshed Sleeper week 1, committed generation 6, warmed
the required cache endpoints, promoted the exact manifest, and reached
`succeeded:hidden`. The receipt reports complete provider validation,
historical/configuration preservation, and all required homepage/season/career
outputs. Worker processing was 43.207s, but click-to-visible was 98.772s, so the
strict under-90-second requirement FAILED for this run.

The overage was isolated to setup: a redundant 4-second `requests` install plus
a 23-second full dependency install. Public main
`71503619fdbaf362fa3c166b68c5daf7b4a6e2ad` now installs once, before claim
handling, with no ephemeral pip cache or bytecode compilation; app mirror is
`b56f18396ec0c8eba52ee72a07129fefe3b4ca54`. Regression tests remain 69/69.
Production no-change/manual canary `35202656669` finished in 43s, captured the
same digest as the UI, made no publication, and reduced dependency setup to
13s--a measured 14-second setup saving. A full changed-data click is therefore
expected under 90 seconds, but that exact post-optimization case is not yet
production-verified and remains open.

Also live on public main: active-week rank materialization repair `961b5c904`,
active-only Sleeper lineage proof `a30cb92bc`, and incremental authoritative
player-position correction `5437dce24`. Augusta's prior recovery run
`35199008631` remains the validated five-aggregate/historical-preservation
witness. The player-position code is shipped but the canonical NFL weekly
source has not yet naturally republished that correction; no large NFL rebuild
was launched.

Still open before completion: post-optimization changed-data UI canaries for
Yahoo OAuth, ESPN, Sleeper, and multiplatform; the credential-blocked leagues
`league_of_snakes`, `the_chulent_bowl`, and `pass_interferance`; and publication
of the pending canonical NFL position correction. No claim of full completion
is made.

## Current checkpoint - 2026-09-16 19:27 UTC

Yahoo Update League is OAuth-only. The production caller maps Yahoo to
`yahoo_incremental_refresh_worker.yml`; that workflow supplies the Yahoo OAuth
client credentials and encrypted credential key, and
`refresh_yahoo_active_season.py` obtains the provider session through
`LeagueContext.get_oauth_session()`. No Yahoo cookie credential, cookie jar, or
cookie fallback was used by the production canary below. The separate legacy
cookie-import workflow is not an Update League caller and is outside this
worker's execution path.

Actual UI-dispatched KMFFL OAuth retry `35140405563` ran on public main
`6f94712e87c5c04948708b13311e4c7d0daebd6b`, refreshed an expired stored OAuth
access token, resolved the saved 2015-2026 Yahoo renewal chain, committed one
generation (`10 -> 11`), finalized the cache, and completed successfully. The
browser remained on the production page without a manual reload. Worker phases
totaled 45.189s; the GitHub job ran for 79s; click-to-visible was approximately
101s. Correctness passed for this canary, but the strict under-90-second
acceptance requirement FAILED.

All compact pre-2026/configuration witnesses were unchanged after publication.
The 2026 week-1 partition contains 10 team rows, 10 nonblank franchise IDs, and
1298.14 total points. Ten manager careers reconcile exactly to the complete
season history (1,490 games; zero games/wins/losses/seasons mismatches), and the
homepage now reports `data_year=2026`, `data_week=1` rather than the stale week
17 value.

The initial player-career audit used the wrong grain and its mismatch count is
withdrawn. The exact production aggregate groups all regular-season
`player_fantasy` rows by `NFL_player_id`, including the expanded unrostered
population. Re-running that exact contract against Fly returned 2,177 source
players, 2,177 target careers, zero rows missing in either direction, and zero
mismatches across first/last year, years active, fantasy points, player/manager
LAMAR, started-player clutch, starts, rostered games, wins, and losses.

The preceding KMFFL attempt `35139531195` failed before publication because the
active snapshot treated legacy server-generated `keeper_config.created_at` as
an unregistered canonical field. Public main `6f94712e8` drops only that legacy
metadata from the local non-published transform frame and makes existing
`___ops` attachment detection idempotent. Generation remained 10 on the failed
attempt. The focused weekly suite passed 361 tests, workflow/writer boundary
suite passed 110 tests, and Ruff passed before the successful retry.

### Sleeper current-state UI no-op - 2026-09-16 19:43 UTC

The production NYU page displayed `Update League` even though its observed and
published source manifests were identical and its last dispatch was healthy.
The status API reported `probe_stale=true` solely because the successful probe
was older than five minutes. One actual UI click ran the existing leased
Sleeper freshness probe and returned `already_current=true`; no worker was
dispatched, the prior run ID `35130659510`, claim version 5, publication bundle,
and source fingerprint remained unchanged, and the control hid after 13.061s.

Whole-row before/after fingerprints were identical for all pre-2026 matchup
(1,596 rows), player_fantasy (61,442), draft (1,452), transactions (9,281),
league_settings (8), league_context (1), and empty manager_overrides. This is a
verified no-op, not a Sleeper publication canary. It confirms a frontend
freshness-state defect: expiring a matching manifest exposes an update control
that only rediscovers the league is current. The proposed bounded fix is to run
the already leased/rate-limited probe off-control and expose the action only
when that probe finds a changed manifest; implementation remains gated on the
requested design approval.

### Sleeper publication canary and full-chain season dependency defect - 2026-09-16 19:52 UTC

The production UI dispatched exactly one Sleeper run for `the_real_ff_league`:
`35142909254`, public-main SHA `5909d3abc6e0b3e5db54b628f03b3eca4e74b83e`.
The persisted renewal chain resolved 2023-2026 and the provider returned ten
final week-1 team rows, 175 roster rows, 198 draft picks, and 52 transaction
perspectives. Publication committed generation 8 to bundle
`fleet-4f868dddc88a163eb681355f021bd205892febe98bf7fa921800da63f0d1bcb5`.
Worker phases totaled 43.688s, the GitHub job took 94s, and click-to-visible was
116.118s. Correctness and the strict under-90-second requirement therefore
remain unaccepted.

Independent Fly witnesses show the source merge itself was scoped correctly.
All pre-2026 whole-row hashes remained unchanged: matchup 510 /
18436341627321814689; player_fantasy 23,666 / 14420224626798944037;
draft 520 / 17283269705718972665; transactions 2,193 /
4263570373966355587; league_settings 3 / 17032997245871009279. League context
also remained unchanged at 13167564389908315858. The new 2026 matchup scope has
10 rows, 10 franchises, zero blank franchise IDs, week 1, and 1,434.64 points.
Caleb Williams is no longer mislabeled best-ever: his week-1 QB all-time rank is
216, while Josh Allen is 17.

The full validator then exposed a real dependency-order defect hidden by the
green workflow. The atomic v3 merge rebuilt career and homepage tables from the
complete weekly chain but retained historical season aggregate rows made under
older semantics. Consequently 398 player careers disagreed with season totals;
the mismatches were confined to win/loss/playoff outcome fields in 2023-2025,
while fantasy points, player/manager LAMAR, clutch, starts, rostered games, and
year coverage reconciled. The same stale-boundary issue left eight matchup
career/homepage win checks and 23 transaction report-card checks inconsistent.
The direct homepage calculation itself was correct for this H2H+median league;
the stale `matchup_season` dependency made the career comparison wrong.

A local TDD fix now reuses the existing matchup, fantasy, draft, and transaction
season aggregators on Fly's already-open complete-chain connection, inside the
same v3 publication transaction, before career and homepage aggregation. It
does not refetch provider history or rewrite matchup/player/draft/transaction
source facts. The publication receipt now records season tables and timing for
Yahoo OAuth, Sleeper, and ESPN. Regression coverage proves stale historical
season rows and transaction report cards are corrected, careers then reconcile,
source hashes are unchanged, and failures remain transactionally rollback-safe.
Expanded server/worker/receipt/ownership/aggregate suites: 509 passed; Ruff and
`git diff --check` passed. This fix is local and uncommitted at this checkpoint; deployment and a
second production canary are still required. Draft warnings, one missing PPG,
and three historical sacko-placement failures remain independently unclosed.

## Disk-blocked continuation checkpoint

D: again reports zero free bytes after three consecutive goal turns with this
same capacity blocker. The current worker HEAD remains a36d876fda916bd0817b7c5479ca117625e3ec0d;
the tested lineage guard is local/uncommitted. No canary or lineage-test process
remains running. Earlier exact temporary-test cleanup was denied; no alternate
deletion mechanism was used. Small C: fixtures allowed the Python regressions,
but the frontend runner still requires its D:/temp module directory and fails
with ENOSPC. Commit/build/deployment cannot proceed safely with zero headroom.

Resume when the user frees D: space: run the pending frontend regression RED,
finish the same ownership check in the existing freshness reader, verify the
worker/frontend suites, then commit only this scoped fix and deploy through
the existing main paths. Recover the_league through the original ESPN/Sleeper
import identity, not its incorrect Yahoo leg. NYU latency/rank issues, remaining
platform canaries, cohort recovery and every other open acceptance item remain
in scope; this is not completion or a narrower replacement objective.

## Latest checkpoint - 2026-09-16 17:50 UTC

The six-file traded-pick/homepage fix is now committed and pushed to public main
as a36d876fda916bd0817b7c5479ca117625e3ec0d. Deployment 35129686647 succeeded
on that exact SHA at 17:42:47 UTC. Fly /ready is serving and accepting queries.
The final transaction/homepage/career/HTTP-publication suite passed 81 tests
in 48.49s using small C: test fixtures. Dirty ownership/planner experiments were
excluded; no workflow or replacement pipeline was introduced.

Second actual NYU UI canary: clicked Retry Update once at 17:50:12.851 UTC;
POST acknowledgment took 17.873s, dispatching run 35130659510. Publication,
rendered results and under-90-second latency remain unverified while it runs.
Read-only observer processes were closed before this single click. D: has no
free space; the browser uses a small C: profile, not another checkout or lake.

NYU UI run 35130659510 subsequently COMMITTED bundle
fleet-83ca13699546f4303aa27e20f09f8724628f6afc2a7a5b367b7056351f180c3b.
Browser observed committed at 150.062s, succeeded/hidden at 153.003s, and the
updated rendered page at 154.911s. The false +13 current-season trade card is
gone; the 2025 +815 all-time trade card, all nine-year manager careers and the
weekly/season/career clutch cards remain visible. No manual page reload.
Worker phases total 103.344s: NFL-week cache patch 54.255s (fetch 51.944s),
shared transformations 8.001s, Fly publication 29.422s. Server career rebuild
3.26s and homepage rebuild 9.0593s are within that publication phase.
The under-90-second requirement FAILED; success is not fleet completion.
Independent Fly career reconciliation: all 12 franchises agree with all-season
games/wins/losses totals (1,400 career games), zero discrepancies. Matchup
identities are nonempty and the same 12 display aliases appear in 2018-2026.
Further independent score/rank checks and remaining platform canaries are open.

Independent post-publication NYU checks: all 12 week-1 team scores match live
Sleeper exactly. All eight mirrored pick perspectives now name the four correct
conveyed players, each with zero trade-asset LAMAR. The old Rams DST mapping is
absent. Fly's all-time trade winner remains KrispyChris at 814.65.
All 430 active player rows match live OPS season/career PPG. Caleb Williams is
QB rank 216 with standard-scoring career PPG 17.49, not rank 1.
One position-rank discrepancy remains: unrostered Bo Melton is displayed WR
but carries DB rank 3699; current Fly WR rank is null, DB rank is 3699. Nine
rostered, nonstarted zero-point players lack a week-1 OPS row. Do not silently
count these as fully verified. Cache-bio synchronization currently refreshes
only missing identities, so stale cached position metadata is a concrete
candidate; source-position/eligibility consistency still requires resolution.

### Yahoo canary paused before dispatch: conflicting active-chain ownership

At 17:59 UTC the actual the_league page offered Update League. No click was
made; the read-only browser was closed. Fly context says platform=sleeper and
league_id=1385696375349448704, while its league_ids_json contains a Yahoo chain
ending 470.l.164172. Persisted settings contain ESPN 110800 for 2011-2024,
Sleeper 1223059438311575552 for 2025, and Yahoo 470.l.164172 for 2026.
The live Sleeper API independently confirms 1385696375349448704 is the 2026
successor of 1223059438311575552. Yahoo 2026 has a different manager identity
set. This is an unresolved ownership/provenance conflict, not proof of an
intentional platform transition. The prior manual Yahoo success is therefore
not a chain-correctness receipt. Do not refresh that Yahoo target again until
the intended active leg is established from import provenance or owner input.
Current frontend and worker selection prefer persisted current-year settings
over the conflicting context, so merely repeating the same canary cannot
resolve the ambiguity. Reuse original import-chain ownership; do not create
another name-based resolver or directly rewrite Fly rows.

### Ownership provenance and local regression fix - 2026-09-16 18:08 UTC

The existing accounts.pending_paid_imports record dated 2026-09-09 03:55:08
resolves the_league's intended import: platform=multi-platform, target index=1,
segments ESPN 110800 then Sleeper 1385696375349448704. No Yahoo segment is in
that saved plan. The league_context platform/ID agree with the Sleeper target;
its updated_at is 2026-09-11 05:42:37. Thus the 2026 Yahoo leg is not supported
by the original import plan. Recovery must retain the ESPN/Sleeper history and
use the established Sleeper continuation, not another Yahoo canary.

Added three real worker regression cases for context/provider contradictions.
All three failed before the fix (wrong provider was accepted). The existing
resolve_active_update_segment now rejects active ownership that contradicts
the saved import target. The lineage + three platform-worker suites pass
40 tests in 1.62s. This two-file worker change is LOCAL ONLY, not committed or
deployed. Unrelated ownership/planner experiments remain excluded.

Frontend regression added in the existing freshness-chain test, but execution
is environment-blocked: Vitest cannot mkdir D:/temp/<run>/ssr (ENOSPC). Its
configuration loader issue was isolated using a small C: test config, without
installing packages or moving the repository. TEMP/TMP remain D:/temp. No
frontend implementation was changed before a valid RED test can run.
D: remains at zero free bytes. Need user to free disk before frontend tests,
commit/deployment and the verified existing-path recovery can continue safely.

## Current sequential checkpoint — 2026-09-16 16:58 UTC

One implementation lane; no new checkout, branch, worktree, or parallel agent.
App UI revision 84cb66f5af4a7be6917104affca26228da355bf3 was deployed in the
preceding continuation; this is not evidence of worker completion. Public worker
checkout remains dc3f3e0221a1bcb730413d90c52b5b698e932c35. Existing modified
ownership experiments are excluded from this work and must not be staged.

Recovery inventory: `update-league-cohort-all-attempts-2026-09-16.json` is the
supporting all-attempts evidence for this ledger (not a second tracker).
All pages of the three incremental-refresh workflow histories returned 219 runs:
113 Sleeper, 51 Yahoo, 55 ESPN, starting 2026-09-10. Run logs resolve 212 runs to
36 unique league database identities. Every league remains unverified for the
full recovery contract, regardless of previous green runs.

Three unmatched runs (35031859653, 35031862539, 35031858907) are push-triggered
workflow-definition failures with no jobs, not league update dispatches.
Four actual dispatches remain identity-unresolved: 34863624106, 34895614878,
34876171739, 34861484048. Three failed before any steps; Yahoo 34895614878 failed
restoring the ops cache before the worker. Do not silently drop these attempts.
The older offseason-update workflow and persisted dispatch registry still need
scope reconciliation before declaring the recovery inventory exhaustive.

Next active task: finish identity/scope reconciliation and compact before-update
witnesses, then trace a real Sleeper UI update. Browser connector returns no
available browser; existing Playwright Chromium is being checked as the browser
test fallback. No new production update has been dispatched in this continuation.

### Live Sleeper UI canary failed — 17:08 UTC

Compact before-witnesses for 36 identified leagues are saved in
`update-league-cohort-before-witnesses-2026-09-16.json`: selected value hashes by
season, configuration hashes, and stable before/after publication generations.
The first whole-row player_fantasy hash timed out without writes; the successful
witness hashes explicit identity/score/alias/LAMAR/clutch columns instead.
No league-history rows or NFL lake were downloaded.

Playwright Chromium loaded the actual production NYU page, with no mocked HTTP
routes. One click at 17:06:51.439Z dispatched public run 35126173284 on exact
worker main dc3f3e0221a1bcb730413d90c52b5b698e932c35. Acknowledgment took 18.802s.
The workflow's paid UI claim succeeded; manual-claim steps were skipped.
NYU before publication generation is 3. The UI reported failure at 99.406s;
the refresh step ran about 59s. Fly rejected publication with HTTP 422:
`Homepage summary lost populated value: season_trade_winner`. Generation remains
3 and the previous +12.93 season trade highlight remains. No successful or
under-90-second publication occurred. This additional attempt belongs to the
same NYU recovery entry; NYU is not recovered.

Source verification found a real shared traded-pick mapping defect: Sleeper's
`pick_{season}_{round}_{roster_id}` was matched to draft slot for startup drafts.
NYU's original roster 4 owns draft slot 6, not slot 4. The old mapping borrowed
an unrelated Rams DST's -12.925 LAMAR and manufactured the +12.93 trade winner.
The four actual conveyed bench assets currently have zero manager LAMAR.

Local fix uses the existing original-roster mapping for every draft category.
The full transaction-enrichment test file passed 26 tests. Homepage correction
uses the existing DDL trade fields and shared calculation: valid no-winner
results are explicit nulls; SQL failures propagate; the existing mirrored
trade-asset contract is checked before calculating the highlight. Tests first
reproduced six failures, then the 72 transaction/homepage regressions passed
within the wider run (95 passing tests total before the failures below).
No replacement pipeline, new checkout, or production write has been introduced.

Read-only cohort inspection found invalid mirrored trade assets in 10 leagues,
across 15 league/year partitions. Examples include 2026 sent-only trade rows in
franchise_mode_fantasy and the_dfb_league_ii, and missing historical sent values
in bethany_beach_league. These are open source/enrichment recovery defects, not
credential blockers. NYU's complete 2018–2026 trade assets pass this check.

Broader verification found an unchanged baseline test disagreement:
`test_draft_manager_season_pk_excludes_draft_category` expects category absent,
but the existing DDL explicitly includes category in the key. Neither file was
changed in this fix. HTTP merge tests could not complete because D: filled with
temporary test databases (2 failures/7 errors total in the 104-test run, with
the remaining failures/errors attributable to disk exhaustion). Cleanup of
only this run's disposable pytest-2419 directory was denied by tool policy;
no alternative deletion mechanism was attempted. User was asked to free space.
The same 8 HTTP publication tests were subsequently rerun with small disposable
fixtures on C: (`--basetemp=C:/Users/joeye/AppData/Local/Temp/league-update-trade-http-20260916-1733`):
8 passed in 16.66s, including atomic rollback and replay. TEMP/TMP remained D:/temp.
No repository, NFL lake, or league-history dataset was moved to C:.
Remaining D: space is approximately 5.5 MiB, insufficient headroom to safely
stage/commit/deploy. The cleanup request remains outstanding.

Review also found pre-existing ambiguous traded-pick fallback branches when
original-roster metadata is absent; these still require a source-backed fix and
regressions. The current mapping fix proves only the metadata-present branch.
Application and public-worker Python files have diverged; do not overwrite the
app's files wholesale. The executing incremental workflow and Fly Dockerfile
both read the canonical public-worker checkout. No workflow was changed here.
The local changes have NOT been committed, deployed, or production-verified.

### Follow-up read-only isolation while deployment is disk-blocked

The next continuation rechecked D: at 4 MiB free; pytest-2419 still exists.
No cleanup retry, new checkout, production rerun, or deployment was attempted.

For the_real_ff_league, Fly contains only 5 sent perspectives for 2026
transaction 1401414684367687680 in league 1389755141288124416. The live Sleeper
transactions/1 response still contains all five assets (one player/four picks).
The current real parser and canonical normalizer produce 5 received + 5 sent
rows, and a second normalization preserves all 10. This is not evidence of a
current provider or normalizer omission.

A subsequent in-memory DuckDB replay loaded only those five live Fly rows into
the canonical DDL, then called the actual merge_provider_refresh_table with the
provider-parsed assets. It explicitly loaded committed HEAD ownership code to
exclude the dirty local ownership experiments. First merge and identical repeat
both yielded 5 received + 5 sent, with zero duplicate transaction/sequence keys.
No real league data was written. Thus the committed merge can restore these
missing perspectives; do not add a speculative replacement normalizer. Full
enrichment/publication of this league remains unverified and must follow the
sequential NYU canary after the publication fix is deployed.

The four identity-unresolved old dispatch IDs have no matches in the current
Fly dispatch registry. They remain unresolved, not credential-blocked/recovered.
September predecessor audit enumerated 93 offseason-draft workflow runs; their
logs invoke check_offseason_drafts.py, not any active-season refresh script.
They are not silently added as weekly-update recoveries: indirect-call/scope
reconciliation remains open, and names require the predecessor's DB_NAMES field.

## Objective and acceptance

Use the existing September shared quick-import transformations for Yahoo OAuth,
ESPN, Sleeper, and multiplatform updates. Fetch changes since the last committed
publication, merge them into the established renewal chain, rebuild derived
outputs with complete historical context, and publish atomically through Fly.
No replacement pipeline, MotherDuck, local full-history hydration, or NFL-lake
download. Actual UI-dispatched updates must visibly complete in under 90 seconds.

All original requirements remain open unless independently evidenced below:

- Full history, franchise identities, shared-team aliases, settings and user configuration.
- Partial-week updates, final scores, transactions, roster/settings changes, missed weeks and older corrections.
- Full-context career/NFL rankings, clutch, simulations, player aggregates and homepage outputs.
- Fail-closed provider completeness; atomic publication, generation checks, idempotence, cancellation and ambiguous-commit recovery.
- Stored Yahoo OAuth; established ESPN/Sleeper credential paths; reconnect-and-resume when required.
- Paid/grandfathered gating in API and worker; existing unpaid upgrade path; agreed availability/abuse limits.
- Shared freshness contract, resilient polling, post-publication UI refresh, compact Almanac control and intact branding/Buy Me a Coffee.
- Canonical public main for every caller/workflow and matching deployed revisions.
- Recover the complete attempted-update cohort, with credential-only blockers supported by evidence.

## Confirmed defects and work

### Career tables computed from active-season-only scratch data â€” OPEN, production defect

Read-only Fly check on 2026-09-16, `___leagues`, filtered to `db_name='the_league'`:

```sql
SELECT year, count(*) AS rows, sum(games) AS games
FROM public.matchup_season
WHERE db_name='the_league'
GROUP BY year ORDER BY year;

SELECT count(*) AS rows, sum(games) AS games
FROM public.matchup_career WHERE db_name='the_league';
```

Observed: each 2011â€“2025 season has 12 rows/168 games; 2026 has 12 rows/12 games.
Season total: 2,532 games. Career: 12 rows/12 games. Historical season rows remain
present, but career totals do not represent that history.

Public worker SHA inspected: `0bfe8122c8d31c094bffb1872e1a62651368e6d7`.
`scripts/refresh_yahoo_active_season.py` hydrates active-year sources but calls
career builders. `active_refresh_publish_tables` includes those whole-league
tables. Existing career-preservation checks skip tables absent from the input
witness map. Therefore a green run and the historical-preservation boolean do
not establish correct career outputs.

Earlier Yahoo run `35057508667` is evidence of publication, not successful
full-history preservation. Do not dispatch further canaries with this defect.

Local changes on `D:\yahoo_oauth`, branch `main`:

- Existing `aggregate_matchup_h2h` accepts `season_years`; an empty set rebuilds
  careers from all persisted matchup years without rewriting season summaries.
- `aggregation_utils.aggregate_career_rollups` directly invokes the existing
  eight career builders on the complete `___leagues` connection. It does not
  open a connection or commit. It rejects a scratch catalog and missing source
  tables before deleting outputs.
- These changes are NOT yet wired into the server merge or deployed.

Evidence: `test_career_rollup_refresh.py`, `test_aggregate_matchup_context.py`,
`test_aggregate_draft_context.py`, `test_aggregate_transaction_context.py`:
17 passed in 3.45s. Tests cover two-year career totals, shared alias, clutch,
quarter-point corrections, repeat execution, another league's isolation,
rollback, missing source rejection, and unchanged historical season summaries.
Draft/transaction fixture coverage is not a production/fleet proof.

Next implementation boundary: call the SAME shared career routines on Fly's
existing merge connection after changed source and season partitions are merged,
before generation/publication-state commit. Package canonical shared modules in
the server image; do not duplicate aggregation SQL or fetch historical raw data.
Verify real merge rollback on aggregation failure, exact season-to-career totals,
unchanged historical partition fingerprints and authoritative publication receipts.
Remove active-only career outputs from the worker contract once server capability
is enforced; an older server must fail closed, not silently skip recomputation.

### Provider score preservation skipped bonus enrichment â€” LOCAL FIX, not deployed

`populate_fantasy_points` scoped its entire UPDATE to rows eligible for score
recomputation. Thus rostered modern ESPN/Sleeper rows with authoritative scores
kept null bonus/premium columns. This explains the null-bonus publication failure
in ESPN runs `35057665071` and `35058032294` more directly than the earlier bio
cache hypothesis.

Change: apply the preservation condition only to `fantasy_points`; enrich bonus
and premium components separately in the same UPDATE. Scope the staging SELECT
to the intended league. No extra points are added to the provider's total.

Regression evidence: six initial cases failed before the fix with null bonus
fields. Expanded 12-case matrix passes for ESPN, Sleeper and Fleaflicker,
zero/nonzero bonuses, TE premiums, retained authoritative score/alias, and repeats.

Existing full `test_sql_player_enrichments.py` has two additional failures:
`applies_custom_offense_corrections` and `preserves_yahoo_rostered_api_points`.
Both were independently reproduced by loading the unmodified HEAD module into
the test process. They are pre-existing, remain open, and must not be hidden by
claiming the full scoring suite passes.

## Production verification still required

No production writes, workflow reruns, or deployments were performed in this
continuation. No new UI canary or under-90-second claim is justified. Root changes
are not yet mirrored to the public worker checkout. Its two existing modified
ownership files were left untouched; the abandoned null-value restoration helper
must not be staged as a fix because it can resurrect invalid all-time ranks.

After the atomic career repair: finish completeness/freshness/watermark and
concurrency audits, reproduce Gray's exact ESPN correction, reconcile aliases
and historical/NFL ranking values, dispatch the real UI path for each platform
and multiplatform support, recover the full attempted-update cohort, and record
worker processing versus click-to-visible latency separately.

## Atomic publication integration follow-up

The shared career routine is now integrated into the existing fleet merge as
`fleet-partition-v2`. V2 omits worker-built career tables and recomputes all eight
careers after source/season partition merge, before publication-state and
generation commit. V1 remains supported for existing callers. Older servers
reject V2 rather than silently omitting its career work.

Tests exercise the actual merge and HTTP publication endpoint, not only helper
functions. Verified locally: historical plus new season totals, untouched prior
season summaries, idempotent HTTP replay, stale generation rejection, exact
uploaded-league/generation scope match, transaction rollback on missing sources
and query timeout, and FAILED_MERGE status on NFL attachment failure.

The HTTP test initially failed because ___ops was not attached on the merge
connection. The fix reuses the server's reference-counted read-only attachment,
releasing it on success or failure. A second injected attachment failure exposed
a VALIDATED status left behind; it now records FAILED_MERGE.

The server image copies canonical Python aggregation modules with an explicit
Fly --ignorefile allowlist. No database, Parquet, NFL lake or credential files
are included in the build context. A Docker build import check verifies the
shared aggregation dependencies before deployment.

Public worker changes are prepared for Yahoo, ESPN and Sleeper: require V2,
exclude scratch careers from publication, and retain the server's career
counts/timings in receipts. Enable them only after server deployment succeeds.
Local worker regression group: 122 passed. Client/merge group: 22 passed.
The two pre-existing scoring tests above still fail; do not claim a wholly
green scoring suite or production recovery.

## Deployment and first canary, 2026-09-16

Server revision `e42b94ec8bc290d67ef6dd61e2affbb9fb30f14d` deployed successfully
via public run `35095334443`. Build context was 7.13 MB. Worker invocation changes
were activated on public main at `cffa425bf98f61cb918b222e919c1d818732c9a9`.

Yahoo manual canary `35095696367` FAILED before publication. Its preflight still
required `matchup_career`, `player_fantasy_career`, and `player_fantasy_career_all`
inside the upload, despite V2 rebuilding them atomically on Fly. No recovery is
claimed. Worker step took 84 seconds; whole job took 121 seconds, so the latency
target also remains unmet.

The validator now accepts the explicit fleet schema contract. V1 still requires
uploaded careers. V2 requires Fly-built careers, prohibits uploading scratch
careers, and retains source identity, local derived-value, and homepage checks.
All three callers declare the same V2 contract used by their bundle builder.
Regression cases reproduced the missing-contract failure before implementation.
The expanded validation suite passes 46 tests; the preceding six-file worker,
career, and validator run passed 166 tests before two provider variants were added.

Actual UI verification remains unverified: the browser surface reported no
connected browsers. Backend/manual canaries do not satisfy the UI criterion.
## Yahoo recovery canary verified, 2026-09-16 12:34 UTC

Public main revision: `372ee4e106c22cbd4a8a3353edf9d1fad1205dc8`.
Manual run: https://github.com/jeleff1000/mfl-league-fetcher/actions/runs/35096470346
COMMITTED bundle: `fleet-0c2397c55cda84e10acb0715b75a5563358cde17a416df2d72017629d283ef99`.
Dispatch 12:32:56; refresh step 12:33:25–12:34:16; cache finalized 12:34:20.
Manual dispatch-to-cache: 84 seconds; worker phases: 49.435 seconds.
Server full-chain career aggregation: 4.4958 seconds. This is NOT UI-click evidence.

Independent Fly checks for `the_league`:
- Career games recovered from 12 to 2,532, exactly matching season totals.
- All 37 franchise careers match season games, wins, losses, points and latest
  manager/alias; zero discrepancies.
- All 4,316 regular-season and 4,319 all-games player careers match persisted
  weekly games, fantasy points, manager LAMAR and started-player clutch sums.
  Zero discrepancies at 1e-5 tolerance. The first audit incorrectly summed
  bench clutch; corrected audit follows the established started-player metric.
- Historical rows and row fingerprints unchanged:
  matchup 2940 / 11727621620591406241;
  player_fantasy 186017 / 13727482781365112600;
  draft 2577 / 12085463288980457930;
  transactions 12512 / 14259586135160535248;
  league_settings 15 / 11640897616840183947.
- league_context fingerprint unchanged: 13149953313368572827; manager_overrides,
  keeper_config, league_rules and standings_config remain empty.
- Receipt downloaded: D:/temp/update-evidence-35096470346/yahoo_active_season_refresh.json.

Additional confirmed open gap: `homepage_refresh._load_homepage_source_frames`
still downloads skinny all-history matchup, draft, transaction and started-player
rows to rebuild homepage outputs locally. Although bounded by league, it does
not satisfy the no-local-history-hydration objective. The existing
`homepage_summary.compute_homepage_frames` accepts a complete connection and is
the reuse target; do not duplicate its formulas or declare this requirement done.

ESPN canary now queued/running: `35096876781` for
`tfl_of_extraordinary_gentleman`, same public main revision.
## Sleeper verification and ESPN remaining failure

Sleeper manual canary `35097064614` on public `372ee4e10` COMMITTED for `nyu_ffl`.
Dispatch 12:39:06; worker 12:39:28–12:40:12; cache finalized 12:40:18 UTC.
Manual dispatch-to-cache: 72 seconds. Processing phases: 42.855 seconds;
server career rebuild: 3.0701 seconds. Not an actual UI-click canary.

Independent Fly reconciliation:
- 12 franchise careers, 1,400 career games; every franchise's games, wins,
  losses, points and latest manager/alias matches its retained season summaries.
- 1,686 regular-season and 1,689 all-games player careers match weekly games,
  points, manager LAMAR and started-player clutch; zero mismatches at 1e-5.
- All pre-2026 row counts and fingerprints unchanged:
  matchup 1596 / 13524715644287174441;
  player_fantasy 61442 / 7588907656652896125;
  draft 1452 / 17688804873191818872;
  transactions 9281 / 5272688981035236134;
  league_settings 8 / 1549569747058823479.
- league_context unchanged: 13373334383361378998. Empty user-configuration
  tables remain empty.
- Receipt: D:/temp/update-evidence-35097064614/sleeper_active_season_refresh.json.

ESPN manual canary `35096876781` FAILED before publication on the same revision.
The bonus-points failure no longer occurs. Preservation now rejects null
`position_alltime_rank` for `00-0034160_2026_1`, Michael Dickson, position P.
Persisted legacy rank is 7; shared position-rank mapping supports QB/RB/WR/TE/K/
DEF/LB/DL/DB but no P source column. Do NOT blindly restore rank 7 or relax
all rank validation. Next: prove the unsupported-position semantics with the
actual shared rank function and distinguish legitimate non-applicability from
missing supported-position source data.

Additional audit finding: `optimal_lineup.position_rank` uses OPS for position
all-time rank but still computes `alltime_ppg` from local active-year rows.
That remains an open complete-history violation; successful career rollup
checks do not validate all weekly NFL comparison fields.

No workers from these three canaries remain running. No all-platform completion
claim is justified. UI verification, all attempted-league recovery, multiplatform
canary, exact Gray correction, full source-watermark reconciliation and the
remaining audit items stay open.

## Fly corruption recovery and post-snapshot import preservation

The live `league_settings` checksum failure was isolated to a damaged physical
block. Validated source snapshot `vs_PAq3Q9kRqKnfnAJNXyOb` predates 33 leagues
that were imported or updated afterward. Preservation snapshot
`vs_Avgke1o8gO9fwXw7p9bw` is created and healthy. The guarded settings swap
preserved all 215 post-snapshot settings rows: live remains 6,255 rows, 6,255
distinct `(db_name, year)` keys, and 1,782 leagues. The generation ledger's
latest publication is 2026-09-16 20:04:35 UTC, before the independently captured
overlay at 21:03:10 UTC. No interim league loss is observed.

Five derived tables share the damaged block: `homepage_manager_rankings`,
`matchup_h2h_career`, `player_fantasy_season`, `player_fantasy_season_all`, and
`standings_by_year`. An isolated restore proved all five readable and key-unique
in the source snapshot. Recovery now exports those exact tables, overlays every
post-snapshot league from live at each atomic swap, rejects a stale overlay
cohort or row count, and rebuilds changed leagues through the existing shared
full-chain aggregators. A post-commit checkpoint failure is reported explicitly
instead of becoming an ambiguous 500; same-run retries are idempotent.

Local evidence: 62/62 DuckDB server integration tests, 3/3 recovery-workflow
contract tests, YAML parse, Ruff, compileall, and diff checks pass. Production
recovery has not yet run. The live source tables remain intact, but the five
derived tables still require the guarded recovery before update canaries resume.

The attempted in-place empty-shell repair was rejected by DuckDB at commit and
rolled back. Live source counts remained unchanged afterward: 6,255 settings,
1,769 contexts, 1,015,510 matchups, 911,103 draft rows, and 3,867,484
transactions; the latest generation timestamp remained 2026-09-16 20:04:35
UTC. A temporary zero-row repair table was removed and the catalog has no
`__repair_*` or `__corrupt_*` leftovers. This path must not be retried.

Follow-up evidence narrowed that statement: the failed path attempted to drop
the damaged objects. Direct reaggregation run `35169976934` proved the first
canonical calculation completes, then `DELETE FROM player_fantasy_season`
fails on physical block 90714112 (computed checksum 5168518579405463287,
stored checksum 18392342689821271652). The fail-fast runner stopped on
`a_circle_of_jerks` with zero completed leagues; it did not mutate production.

Isolated run `35170289421` then tested a different, non-destructive catalog
operation against retained recovery volume `vol_4919j2m0wzg0xw5r`: atomically
rename each of the five damaged objects to `__corrupt_recovery_*` and install
an empty canonical shell without reading or dropping the damaged block. All
five swaps committed successfully in 1m33s including Fly machine startup.
Production remained untouched. This proves the safe fast recovery boundary is
catalog quarantine followed by the already-built snapshot seed and changed-
league reaggregation; no full logical database copy is required.

The next recovery stage is isolated and fail-closed: restore the preservation
snapshot to a new Fly volume, copy every healthy table from its exact catalog
DDL, and recreate only the five proven-damaged derived tables empty. The helper
rejects unknown catalog-object types, absent exclusions, row-count mismatches,
or a retained WAL. It cannot stop, clone, deploy, replace, or otherwise mutate
the primary machine. Local helper/contract evidence: 7/7 tests, YAML parse,
Ruff, compileall, and diff checks pass. Isolated production-volume evidence is
still pending and no cutover is authorized yet.

## Shared historical PPG and rank-applicability corrections

Regression evidence: with a single hydrated week scoring 18.76, the old shared
position-rank function returned both season and career PPG as 18.76. Three
scoring variants failed before the fix. It now directly maps the existing
precomputed OPS PPG columns using the existing scoring-column helper; no local
career AVG or NFL-history download. Missing selected PPG columns fail before
outputs are cleared. Valid zero PPG is retained.

Read-only live OPS witness: Caleb Williams 2026 week 1 has half-PPR/4pt season
PPG 37.26 and career PPG 17.52; all-time QB rank 216. Michael Dickson has
position P, season PPG 0 and career PPG -0.02, with no QB rank. All 1,149 source
rows in that week contain the selected season and career PPG values.

ESPN's legacy non-applicable all-time rank is handled only for unchanged,
recognized nonranked NFL positions (punters, long snappers, offensive line).
Supported, unknown, blank or changed positions remain fail-closed. This
exception affects only position_alltime_rank, not points, clutch or PPG.
Tests exercise the real shared rank function and the preservation gate.
Eight applicability regressions failed before implementation.
Combined worker/validation/rank suite: 185 passed; focused preservation/rank
group: 53 passed. Ruff passes. No production result for these new changes yet.

## Five-table production reaggregation

The snapshot-derived recovery was not used. Run `35171039263` waited 17m22s
for Fly snapshot infrastructure and then failed closed before any production
write because the live overlay advanced from the prepared 33 leagues / 215
settings rows to 35 leagues / 239 rows. That proved the stale-overlay gate,
but also proved this was the wrong-granularity mechanism for five derived
tables. It was not a 17-minute SQL validation.

Production was instead repaired directly from intact persisted facts and
healthy intermediate aggregates in one atomic transaction. The transaction
rebuilt exactly `homepage_manager_rankings`, `matchup_h2h_career`,
`player_fantasy_season`, `player_fantasy_season_all`, and
`standings_by_year`; it did not fetch provider data, replace settings, rewrite
source facts, copy the database, or use MotherDuck. End-to-end database work
took 122.98 seconds, dominated by the two required scans of 50,785,407 weekly
player rows.

Production result and canonical-key validation (6.46 seconds total):

- `homepage_manager_rankings`: 21,856 rows / 21,856 distinct
  `(db_name, franchise_id)` keys.
- `matchup_h2h_career`: 248,797 rows / 248,797 distinct
  `(db_name, franchise_id, opponent_franchise_id)` keys.
- `player_fantasy_season`: 4,372,442 rows / 4,372,442 distinct
  `(db_name, NFL_player_id, year)` keys.
- `player_fantasy_season_all`: 4,380,213 rows / 4,380,213 distinct
  `(db_name, NFL_player_id, year)` keys.
- `standings_by_year`: 62,041 rows / 62,041 distinct
  `(db_name, franchise_id, year)` keys.

Source reconciliation across Yahoo `the_league`, Sleeper `nyu_ffl`, ESPN
`tfl_of_extraordinary_gentleman`, and `kmffl` found zero manager-ranking
mismatches, zero player points/games mismatches, and zero standings
wins/losses/points mismatches. The `the_league` projection counts matched the
pre-write witness exactly: rankings 37, H2H 298, regular player seasons 16,020,
all-games player seasons 16,043, and standings 192. KMFFL has 2,145 rebuilt
season-player rows with nonzero clutch equity across 2015-2026, so the repair
did not null the clutch aggregate.

The five damaged original objects remain quarantined under
`__corrupt_recovery_*`; they were not read, dropped, or treated as fallbacks.
This closes the live five-table outage only. It does not close the broader
Update League goal, UI-dispatched verification, or the remaining
complete-history audit items above.

## Sequential production canaries after five-table repair

All runs below executed public-worker `main` at exact SHA
`793ebe8547155fe0710369343234c14f7d21d281`.

- Sleeper `nyu_ffl`: run `35172946763` succeeded in 24 seconds and correctly
  returned `NO_FINALIZED_WEEKS` without publishing. Source planning took
  0.273 seconds. The persisted renewal chain remained complete for 2018-2026;
  rankings retained 108 manager-seasons, standings retained 2018-2026, and
  player season output retained 1,911 nonzero clutch rows.
- A deliberately incorrect Yahoo worker dispatch for multiplatform
  `the_league` (run `35173026092`) failed closed before provider fetch or
  publication because its active 2026 segment is Sleeper. This confirms the
  active-segment guard; it must not be bypassed by forcing a historical
  platform worker.
- Yahoo OAuth `kmffl`: run `35173145823` succeeded in 41 seconds and correctly
  returned `NO_FINALIZED_WEEKS` without publishing. Source planning took
  0.186 seconds. The persisted Yahoo renewal chain remained complete for
  2015-2026, manager aliases remained present, and the repaired player-season
  output retained 2,145 nonzero clutch rows.
- ESPN `tfl_of_extraordinary_gentleman`: run `35173226958` committed week 1
  and published the refreshed cache. Worker processing was 51.176 seconds:
  provider fetch 4.614, shared transformations 8.103, full season rollups
  7.143, homepage outputs 8.570, and atomic Fly publication 20.787 seconds.
  Cache publication and hot verification took about 1.3 seconds. The workflow
  job took 95 seconds because setup/claim/cache-restore consumed roughly 39
  seconds before the 51-second worker; run creation to verified hot cache was
  about 94 seconds, so the strict under-90-second click-to-visible criterion is
  not yet closed by this manual canary.

Post-publication validation for the ESPN canary was limited to the five
repaired derived tables. Every canonical key was unique. Rankings and H2H had
15/15 and 178/178 rows/keys; regular and all-game player season tables had
10,043/10,043 and 10,058/10,058; standings had 180/180. Every table spans
2012-2026. The regular player-season table retains 3,097 nonzero clutch rows.
The live overview API reports `last_updated=2026-09-17T02:09:17.619718` and
serves full-history leaders from 2013, 2017, 2019, and 2022 alongside 2026
week 1, rather than treating the refreshed week as the entire history.

These canaries close the five-table reaggregation defect on all three platform
paths. They do not yet close actual UI-dispatched verification, the
multiplatform dispatch canary, full recovery-cohort verification, or the final
under-90-second click-to-visible requirement.
