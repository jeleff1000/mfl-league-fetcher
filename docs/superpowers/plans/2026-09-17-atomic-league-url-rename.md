# Atomic League URL Rename Implementation Plan

> **For Codex:** Execute sequentially with `superpowers:executing-plans`. Do not create a branch, clone, replacement pipeline, or parallel implementation lane.

**Goal:** Make the Settings URL rename move one league identity everywhere, repair the split Agusta state through that same path, and prove weekly updates resolve the renamed URL and credentials.

**Architecture:** Replace request-time SQL fan-out with a public-worker dispatch to one specialized Fly rename endpoint. The endpoint mutates only canonical registry tables, reconciles partial prior renames deterministically, rebuilds derived tables using existing shared functions, and retargets control-plane identities with a durable idempotency receipt.

**Tech stack:** Python/FastAPI/DuckDB server, GitHub Actions public worker, Next.js/TypeScript settings API, Vitest and pytest.

---

## Task 1: Specify and test the canonical rename engine

- [ ] Add failing unit tests in `duckdb-server/tests/test_league_rename.py` for a normal rename, Agusta-style split years, target-only draft history, stable-identity conflict, retries, and canonical-table allowlisting.
- [ ] Add the narrow rename engine in `duckdb-server/league_rename.py`, consuming `canonical_table_registry()` rather than information-schema table discovery.
- [ ] Classify year-scoped, identity/config, and generated tables from the registry; make target partitions win only where they overlap.
- [ ] Record deterministic operation receipts and before/after year manifests.
- [ ] Verify the target holds the union of history and the source has zero canonical rows.
- [ ] Run the focused rename-engine tests.

## Task 2: Add one idempotent Fly API operation

- [ ] Add failing endpoint tests in `duckdb-server/tests/test_integration.py` for auth, validation, successful resume, conflict, and redacted responses.
- [ ] Add `POST /rename-league` in `duckdb-server/main.py`, protected by the existing admin token and `_merge_lock`.
- [ ] Drain readers once, close the pool once, execute the narrow server-local operation, checkpoint, and reopen once.
- [ ] Rebuild generated outputs with the existing shared derived-table functions before marking the operation complete.
- [ ] Ensure no credential values are logged or returned.
- [ ] Run focused server tests.

## Task 3: Retarget credentials and control-plane state

- [ ] Add fixture-backed tests for inventory, paid/grandfathered eligibility, Yahoo OAuth credentials, ESPN credentials, Sleeper registry, update jobs/status, and URL aliases.
- [ ] Implement stable-identity collision checks before mutation.
- [ ] Retarget all supported `___ops` records to the new key, merging only same-identity partial target rows.
- [ ] Create/update a durable old-to-new URL alias and collapse duplicate inventory rows to one canonical target row.
- [ ] Make each phase safe to repeat after an ambiguous response or process restart.
- [ ] Run focused control-plane tests.

## Task 4: Add the public rename worker

- [ ] Add failing CLI tests in `fantasy_football_data_scripts/tests/unit/scripts/test_rename_league_admin.py`.
- [ ] Add `scripts/rename_league_admin.py` to validate/decode the payload and call `FlyTarget.rename_league()`.
- [ ] Add `FlyTarget.rename_league()` with bounded timeout and ambiguous-response reconciliation.
- [ ] Add `.github/workflows/league_admin_rename_worker.yml` using public `main`, the shared league lock, and no provider fetch or database download.
- [ ] Mirror the workflow in both the app and public-worker repositories as required by project rules.
- [ ] Run focused CLI/client tests.

## Task 5: Replace direct Settings writes with dispatch

- [ ] Add failing Vitest coverage showing rename dispatches `league_admin_rename` and never calls `runQueryRW` or information-schema discovery.
- [ ] Refactor `frontend/src/lib/league-management.ts` to validate the rename, derive a deterministic operation key, dispatch the public workflow, and return queued state/run metadata.
- [ ] Update `frontend/src/app/api/league/[db]/league-management/route.ts` and `frontend/src/hooks/use-manager-settings.ts` for pending/completed semantics.
- [ ] Resolve old URL aliases to a redirect and route to the new URL only after committed completion.
- [ ] Preserve existing branding/settings behavior and actionable errors.
- [ ] Run focused frontend tests.

## Task 6: Regression verification before production repair

- [ ] Run all focused rename, lineage, weekly-refresh, Fly server, and frontend suites.
- [ ] Run the unrelated-test baseline and record any pre-existing failure separately.
- [ ] Confirm workflow copies are byte-identical and every executing reference uses the canonical public repository `main`.
- [ ] Push worker/server changes and app changes to their respective public `main` branches without creating another branch.
- [ ] Verify deployed SHAs before mutating Agusta.

## Task 7: Recover Agusta through the production rename path

- [ ] Capture compact pre-operation manifests for both `agustafantasyleague` and `agusta_fantasy_league` without exposing credentials.
- [ ] Dispatch the same rename operation used by Settings.
- [ ] Verify `agusta_fantasy_league` contains 2009–2026, including target-only draft and current 2026 rows.
- [ ] Verify `agustafantasyleague` has zero canonical rows and redirects to the target.
- [ ] Verify one paid inventory identity, preserved Yahoo OAuth credentials, preserved Sleeper registry, aliases, and franchise identities.
- [ ] Verify the public page shows complete history and correct all-time aggregates.

## Task 8: Prove weekly update works after rename

- [ ] Trigger an update through the real production UI-dispatched path for `agusta_fantasy_league`.
- [ ] Verify the worker resolves the renamed key, current Sleeper identity, and retained credential/control-plane records.
- [ ] Verify publication preserves 2009–2025 and updates only the required current data.
- [ ] Verify homepage/career/player aggregates remain full-history and no aliases split.
- [ ] Record run links, processing time, click-to-visible time, deployed SHAs, and any unverified condition in the closure ledger.

