# witness_audit_v2 -- the 2026-07-16 exhaustive witness/column audit tooling

Analysis lanes (write to `D:/league-history-data/nfl/derived/validation/witness_audit_2026_07_16`).
Nothing here mutates the super table; the fix builders live in `scripts/sota_recon/build_*.py`.

Run order:
1. `super_column_census.py`   -- every column of weekly + season/career artifacts, classified
2. `witness_contracts_v2.py`  -- EXHAUSTIVE witness scan (158 contracts; no silent drops)
3. `master_matrix.py`         -- column x witness verdicts (direct / text_derived / derived_witnessed / UNMAPPED)
4. `gap_ledgers.py`           -- unused-atom ADD ledger + era-floor table

Standalone probes:
- `era_rescue.py`        -- ACTUAL populated era per column vs ACHIEVABLE floor (witness / pbp / formula).
                            ⚠ read the HEURISTIC LIMITATION note in its docstring before trusting
                            `interior_zero_years` (rare events produce false "holes").
- `missing_mirrors.py`   -- measures every double-entry identity NOT yet in the recon lattice.
                            This is what caught the `pick6` defect AND the bad first fix for it.
- `residual_probe.py`    -- one-unknown RESIDUAL_SOLVER feasibility (season witness - known weeks).
- `build_defect_queues.py` -- emits receipted CSV queues for classes that must NOT be auto-fixed
                            (targets>attempts; season overcount n>g).

Curated outputs in `docs/`: `witness-contracts-v2.json`, `witness-column-master-matrix.json`.
Report: `docs/runbooks/supertable-witness-master-audit-2026-07-16.md` (§9 = the fix wave).
Execution routing/stop-conditions: `docs/runbooks/witness-audit-execution-handoff-2026-07-16.md`.

Supersedes the name-match-only catalog run in `scripts/sota_recon/witness_contracts.py` (51 sources).

## Hard-won lessons encoded here
- A witness column existing != the atom being attributable. PBP records the PASSER on 100% of
  interceptions but the intended RECEIVER on 0.0-0.5% of them in 2003-08. Check attribution
  completeness per year before defaulting a cell to 0.
- "Known" means the cell IS NOT NULL. Never COALESCE-0 an unknown into a fact.
- Always validate a fill against its mirror AND an untouched control era.
