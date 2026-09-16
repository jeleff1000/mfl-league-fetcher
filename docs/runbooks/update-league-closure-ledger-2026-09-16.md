# September Update League closure ledger

State: active. Production completion is unproven. This ledger is for league updates, not SuperTable SOTA work.

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
