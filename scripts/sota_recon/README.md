# SOTA Reconciliation System — Master Index

The single source of truth for how the v26 NFL super table is audited, corrected, and
proven complete. **Everything lives on the `D:` data lake** (`D:\league-history-data\nfl`);
this directory holds only the code that reads/audits/corrects it.

## Prove everything is present (run this first)

```
python -m scripts.sota_recon.inventory
```

Writes `D:\league-history-data\nfl\derived\validation\sota_recon_master\DATA_LAKE_INVENTORY.{md,json}`
— a catalog of every source with **present? / coverage / purpose**, plus the v26
correction provenance. If it says `present: N/N ✓ all present`, the data lake is intact.

## The data lake (catalogued in DATA_LAKE_INVENTORY.md)

| role | source | coverage |
|------|--------|----------|
| subject | v26 release `nfl_player_stats_all.parquet` | 1.18M rows, 1920–2025 |
| anchor | `schedule_master`, `pfr_team_games` | 1920–2025 (year-aware franchise_id) |
| **scoring** | `pfr boxscore tables/scoring` | **11,445 games 1957–2026, per-play w/ running score** |
| pbp | `pfr boxscore tables/pbp` | 11,192 games 1981–2026 |
| boxscore | player_offense / kicking / returns / player_defense | ~11,400 games 1957–2026 |
| oracle | `pbp_player_week_rollup` | 731K rows 1978–2025 (independent corroboration) |
| oracle | `ancient_ready_bundle` (PFR/PFA/LOC) | pre-1980 |
| legacy | MotherDuck super_table backup | diff context only |
| identity | `player_bio`, `pfr_player_index` | — |

`sources.py` is the pinned registry every audit reads; nothing touches a path not listed there.

## The audit (read-only)

```
python -m scripts.sota_recon.run_all          # all lanes -> RECON_SUMMARY.md + manifest
python -m scripts.sota_recon.run_all --only oracle,scoring
```

Lanes (each writes per-lane CSVs + manifest under `sota_recon_master/<ts>_v26/`):
- **oracle** — second-source corroboration vs PBP (73 stats, 52.7M cells, 99.69%)
- **scoring** — reconcile each team-game to the scoreboard (independent witness)
- **schedule** — independent calendar anchor (existence/date/score), franchise-keyed
- **team_total** — sum-of-players vs independent team totals
- **internal** — physical impossibilities + cross-side identities + composition/bounds/era-gate
- **seam** — year-over-year discontinuity (transform-artifact) detection

## The corrections (idempotent, replayable, provenance-tagged)

```
python -m scripts.sota_recon.corrections.run_corrections [--dry-run]
```

Every change is a deterministic wave, re-applied cleanly each run, tagged in the
`recon_correction_log` column. Lock-safe streaming-pyarrow write, single-threaded,
row-count + provenance verified after write.

| wave | what |
|------|------|
| wave1 | physical impossibilities → 0 (targets/attempts/FG identities) |
| wave2 | DEF int/sack identity-enforced + game_date/home_away (franchise joins) |
| wave3 | DEF dedup coalesce-merge (preserve DST atoms, keep doubleheaders) |
| wave4a/b/c | `*_allowed` + def-returns + adjusted `dst_points_allowed` |
| wave5 | structural impossibilities (recv-bucket/punt_long/pat) |
| wave6 | added offensive `passer_rating` (verified exact) |
| wave7 | added 9 efficiency ratios (completion%, Y/A, YPC, …) |

## What's documented vs what's real error

`asymmetry_registry.py` holds every **documented residual** — known, characterized,
accepted (subjective tackles, sourcing-limited eras, name collisions, scoring-decomposition
gaps). The discipline: a flag is either fixed or has a written reason there. Nothing
unexplained.

## Honest state

- Non-derived stats: 99.69% corroborated vs independent PBP across 52.7M cells; identities
  enforced all eras; structural impossibilities = 0; season rollups exact on landmarks.
- **Open frontier**: integrate the `scoring` tables (1957–2026) to close the scoreboard-
  reconciliation gap; verify the 451 derived columns (`pts_*`/`rank_*`/`lamar`) by recompute;
  the 21 charted advanced metrics are modern-only (not extendable — by design, never fabricated).
- We do **not** claim 100% / "best on earth as fact." We claim: every discrepancy is fixed
  or documented, corroborated by independent witnesses, and re-verifiable from two commands.
## External historical witnesses

`external_witness_intake.py` validates ff-assets artifact manifests and copies raw
evidence plus normalized records into immutable local intake. It never writes to
Fly or the super table. `recon_external_witnesses.py` then evaluates unresolved
newspaper identity tasks using fuzzy player name + team + season and emits a
candidate ledger. Existing-bio identities and separately gated bio additions are
reported distinctly; bio is changed only with explicit `--apply-bio`.
