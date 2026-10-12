# Transaction Week Protection Implementation Plan

> For agentic workers: implement inline with superpowers:executing-plans. Checkboxes and the evidence section are the durable ledger.

**Goal:** Protect every future Yahoo, ESPN, and Sleeper import/update from fabricated transaction weeks.

**Architecture:** Retain provider-native Sleeper/ESPN periods. Yahoo requires exact dated league windows. One local DuckDB validation helper protects existing full, quick, fleet, and legacy publication boundaries before any upload; no additional network calls, persistent tables, or pipeline.

**Tech stack:** Python, pandas, DuckDB, existing public GitHub workers.

**Spec:** The active user goal in this thread: protect future imports and determine ESPN/Sleeper exposure.

## Global Constraints

- Public `jeleff1000/mfl-league-fetcher`, `main`; preserve unrelated app/stat work.
- No Fly restart, table rebuild, lake download, new workflow, or fleet-wide repair.
- Preserve valid native week zero/preseason periods and explicitly estimated pre-2019 ESPN moves.
- Missing/ambiguous Yahoo dates must fail rather than be extrapolated; valid preseason Yahoo events retain the existing first-week convention.
- Validation is local and scoped to only leagues whose transactions are being published.

## Review Focus

- Timestamp/date boundaries must not depend on the worker host timezone.
- Missing provider fields must not silently become 1 or 0, nor be swallowed into partial success.
- Yahoo duplicate/null/overlapping/gapped windows must not produce a guessed date.
- Native offseason/draft-pick transactions and estimated historical ESPN moves must remain valid.
- Cached data, manual imports, quick/full/multiplatform imports, and direct fleet publication must all reach the guard.

## Task 1: Eliminate provider defaults

Files: Yahoo/Sleeper/ESPN transaction fetchers; `core/transaction_periods.py`; provider regression tests.

Interface: `TransactionWeekError(ValueError)` and `native_transaction_week(value, *, platform, transaction_id) -> int` reject absent/non-integer/out-of-range periods; 0..22 are valid. Yahoo mapping rejects invalid timestamps and uncovered/ambiguous dates.

- [x] Add RED tests: missing Yahoo windows, invalid timestamp, calendar gap, overlapping windows; Sleeper missing leg for player/pick moves; ESPN missing period in both quick and full paths.
- [x] Run `python -m pytest fantasy_football_data_scripts/tests/unit/data_fetchers/test_yahoo_transactions.py fantasy_football_data_scripts/tests/unit/data_fetchers/test_sleeper_transactions.py fantasy_football_data_scripts/tests/unit/data_fetchers/test_espn_transactions_legacy.py -q` and inspect expected failures.
- [x] Implement explicit mapping errors and rethrow them through catch-all handlers. Keep valid provider periods untouched. Run the same command to GREEN.

## Task 2: Guard every publication lane

Files: `core/transaction_periods.py`, `core/delta_publish.py`, `core/fleet_publish.py`, legacy branch of `core/local_db.py`; `tests/unit/core/test_transaction_periods.py`.

Interface: `validate_transaction_weeks(conn, db_names: list[str]) -> None`, reads only staged local `public.transactions` and Yahoo dated `public.matchup`; raises `TransactionWeekError` with bounded offending IDs.

- [x] Add RED tests that call the real full/quick/fleet builders with bad transaction weeks and assert failure before bundle creation; validate legitimate native/estimated/preseason data, missing Yahoo windows, and mixed-league scoping.
- [x] Implement a shared local check at the existing builders and legacy upload path. No upload occurs when it fails. Avoid duplicate quick/fleet validation.
- [x] Run the provider, transaction enrichment, delta, quick, fleet, and local-upload tests to GREEN. Inspect all production caller paths and the canonical normalizer.

## Task 3: Review and deploy

- [x] Run representative no-write input pilots for each platform; record guard timing and exact acceptance/rejection. No new full imports are needed to prove a pre-publication gate.
- [x] Independent review of the diff, then test any substantive findings RED/GREEN.
- [x] Run the project test command with a bounded first-error check; name any environment/checkout blocker separately from targeted results.
- [x] Push verified fixes to public `main`, verify remote SHA and boundary guard. Confirm Fly remains ready with no writes from this goal.
- [x] Completion audit: all three platforms, quick/full/multiplatform/refresh/manual/legacy paths; absent/invalid periods fail, valid periods survive, no added provider calls.

## Evidence

- Baseline `e18aad0`: Yahoo fallback returns week 1 with absent windows; Sleeper uses `txn.get('leg', 1)` twice; ESPN uses `scoringPeriodId` default 0 and catches parsing errors on full imports.
- Read-only QA samples: KMFFL 2024/2025 trades now cover weeks 4..13; NYU native trades cover weeks 1..13; TFL native trades cover weeks 5..12, including valid rows with no timestamp. Native period validation must not require a timestamp.
- Execution ruling: reuse the existing public main checkout; user explicitly requires main and no branch/workspace duplication. The plan is also the progress ledger.
- Initial provider + gate GREEN: 51 passed. Existing delta/fleet/normalizer/enrichment/retransform tests: 74 passed. Quick/legacy/UTC/preseason checks: 49 passed (overlapping sets; not a summed unique count).
- Read-only Fly samples copied into an in-memory local DB: 10 KMFFL Yahoo trades accepted in 82.92 ms, 4 NYU Sleeper trades in 11.57 ms, 8 TFL ESPN trades in 11.06 ms. An injected cached Yahoo week-1 regression was rejected locally. No live writes or provider calls.
- Independent review found swallowed errors in full imports/recovery, an exported-league scope bypass, and a blank-platform bypass. New tests reproduced the bypasses; five publication/orchestrator regressions and four Yahoo recovery regressions now pass. Two additional offseason/outer recovery handler tests pass.
- Yahoo ordinary and recovery loaders now share the same dated-window filter; null synthetic rows cannot hide real dates, and conflicting real dates remain visible. UTC midnight, valid native zero, draft-pick moves, and estimated ESPN history are covered.
- Full project collection was attempted with `python -m pytest --collect-only --maxfail=1 -q`: 651 tests collected, then blocked by the unrelated sparse-checkout omission `multi_league.data_fetchers.fleaflicker`. No full-suite success is claimed; no bulk checkout or install was performed to mask that limitation.
- Public worker boundary validator passed; new shared helper/tests pass Ruff. Fly `/ready` was healthy, serving, with zero active queries/publishes and zero league WAL bytes. No Fly writes, restarts, or table/schema changes were performed for this goal.
- Broad targeted run: 189 passed. After the final provider-scope refinement, all 81 publication/guard tests passed on the final implementation (29 period-contract tests plus 52 existing publication tests). Together these cover 192 distinct targeted cases; overlapping runs are not added together. Provider/fetch/recovery/enrichment code was unchanged by that final refinement. Two outer/offseason handler cases also passed independently.
- The guard applies new period constraints only to Yahoo/ESPN/Sleeper; a regression proves unrelated MFL archived contracts are unchanged.
- Final-implementation cold input pilot under concurrent local test load: Yahoo 5344.47 ms, Sleeper 10.90 ms, ESPN 6.11 ms; all accepted, and injected Yahoo week 1 rejected. Do not misreport only the earlier faster sample as a latency guarantee. An isolated conversion probe measured UTC-zone vs UTC-epoch conversion at 2.68 vs 2.22 ms, so there is no evidence for changing the date algorithm; no speculative optimization was made.
- No new full production import/update was launched. Verification uses real read-only input samples, real local publication builders, injected parser/recovery failures, and existing regression tests. Already-running jobs pinned to older SHAs cannot acquire this change retroactively.
- Final code commit `39356b9fbe978e74980a365d46e0ee79d7489084` was pushed and independently verified against remote `refs/heads/main`. Public Worker Boundary Guard [38311977121](https://github.com/jeleff1000/mfl-league-fetcher/actions/runs/38311977121) completed successfully on that exact SHA. All three quick workers and the multiplatform worker check `refs/heads/main` and checkout the public repo; all three refreshes use the guarded fleet builder, while multiplatform publication uses guarded `upload_to_fly`.
- Repeated final local input pilot accepted the same real examples at Yahoo 1974.77 ms, Sleeper 15.32 ms, ESPN 16.08 ms. These are small local samples, not a full-import latency guarantee. No added verification API calls occur in the production helper.
- Final Fly readiness: serving/healthy, zero active queries, zero publishes, zero league WAL bytes, unchanged league file hash `sha256:b0ac1d0767212f61` throughout this goal. No live write/restart/rebuild/capacity change occurred.
- Closed: all listed tasks complete for this transaction-week bug. Limitation: the complete repository suite and new end-to-end production imports were not run; the explicit targeted tests, no-write production-input pilots, local builder tests, and exact-SHA boundary guard are the completion evidence.
