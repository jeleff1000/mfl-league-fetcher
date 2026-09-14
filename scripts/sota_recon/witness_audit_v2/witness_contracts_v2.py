"""witness_contracts_v2 -- EXHAUSTIVE local witness scan (audit build, 2026-07-16).

Fixes vs scripts/sota_recon/witness_contracts.py:
  1. NO SILENT DROPS: every registered source yields a contract OR an explicit error entry.
  2. Registers the missing witness families: pfr context tables (accuracy, air_yards, all_pro,
     pro_bowl, voting_*, pressure, play_type, advanced_*), nflcom player_logs / player_career /
     player_splits / player_situational (shard DIRS -- v1 pointed at nonexistent single files),
     nflcom logs_targeted, pbp_backfill twins, second pbp weekly rollup, awards features,
     team-game catalog, master schedule, scoring_summary, newspaper duckdb (pending),
     legacy supertable backup (subject_history), ancient bundle.
  3. Career-grain sources without a year column are cataloged (single-row aggregate) not dropped.
  4. Single-pass per witness (chunked wide aggregates) instead of one query per column.
  5. Casts strip thousands-separators; coverage counts use <>0 (negative-valued stats count).
  6. NFL.com shard sets are split into per-category contracts via the _table column.
  7. min_rows threshold lowered 20 -> 3 (rare stats like safeties get true era floors);
     per-year nonzero counts are RETAINED so any threshold can be re-derived later.
"""
from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

import duckdb

sys.path.insert(0, r"d:\yahoo_oauth")
from scripts.sota_recon.witness_contracts import (  # noqa: E402
    ATOM_MAP, NFLCOM_CATEGORY_ATOMS, _NFLCOM_GENERIC, _META_EXACT, _META_NFLCOM, _META_SUFFIX,
)

LAKE = Path("D:/league-history-data/nfl")
SCRATCH = Path(r"D:\league-history-data\nfl\derived\validation\witness_audit_2026_07_16")
OUT = SCRATCH / "witness_contracts_v2.json"
MIN_ROWS = 3
CHUNK = 180  # stat cols per aggregate pass

_META_EXTRA = {"team_code", "opponent_code", "team_fid", "opponent_fid", "wk", "is_home",
               "is_away", "is_neutral", "week_label", "season_week", "stathead_id",
               "bundle_source", "upsert_state", "already_in_supertable", "team_game_key",
               "source_positions", "identity_resolution", "data_source", "known_stat_cols",
               "coverage_statuses", "known_missing_context", "stat_completeness_class",
               "requires_multi_game_suffix", "derivation_rules", "source_files"}
_YEAR_CANDIDATES = ("year_id", "season", "year", "season_year", "yr")


def is_stat_col(col: str, dtype: str) -> bool:
    if col in _META_EXACT or col in _META_NFLCOM or col in _META_EXTRA or col.startswith("_"):
        return False
    low = col.lower()
    if any(low.endswith(s) for s in _META_SUFFIX) or low.endswith("_code"):
        return False
    t = dtype.upper()
    numeric = any(k in t for k in ("INT", "DOUBLE", "DECIMAL", "FLOAT", "REAL", "HUGE", "SMALL", "TINY"))
    return numeric or t.startswith("VARCHAR")


def canonical(col: str, witness: str) -> str:
    base = witness.split(":", 1)
    if witness.startswith(("nflcom_season:", "nflcom_logs:", "nflcom_career:", "nflcom_splits:", "nflcom_situational:")):
        cat = base[1] if len(base) > 1 else ""
        cmap = NFLCOM_CATEGORY_ATOMS.get(cat, {})
        if col in cmap:
            return cmap[col]
        if col in _NFLCOM_GENERIC or witness.startswith(("nflcom_logs:", "nflcom_career:", "nflcom_splits:", "nflcom_situational:")):
            # namespace EVERYTHING unmapped in category-scoped long tables (fum/lost/ff are safe names though)
            if col in {"fum", "lost", "ff", "fr", "own_fr", "opp_fr", "sck", "scky", "sfty", "pdef",
                       "solo", "ast", "tkl", "combined", "int", "rate", "fgm", "fg_att", "xpm",
                       "xp_att", "punts", "rec", "tgts"}:
                simple = {"fum": "fumbles", "lost": "fumbles_lost", "ff": "def_fumbles_forced",
                          "fr": "fum_rec", "own_fr": "fumble_recovery_own", "opp_fr": "fum_rec",
                          "sck": "def_sacks" if "defen" in cat or cat in ("tackles",) else "sacks_suffered",
                          "scky": "sack_yards_suffered", "sfty": "def_safeties",
                          "pdef": "def_pass_defended", "solo": "def_tackles_solo",
                          "ast": "def_tackle_assists", "tkl": "def_tackles_solo",
                          "combined": "def_tackles_with_assist", "int": "def_interceptions" if "defen" in cat or cat in ("interceptions",) else "passing_interceptions",
                          "rate": "passer_rating", "fgm": "fg_made", "fg_att": "fg_att",
                          "xpm": "pat_made", "xp_att": "pat_att", "punts": "punts",
                          "rec": "receptions", "tgts": "targets"}
                return simple[col]
            return f"{cat}__{col}" if cat else col
    return ATOM_MAP.get(col, col)


def _witnesses() -> list[dict]:
    W: list[dict] = []

    def add(name, path, grain, cls="primary", split_by=None):
        W.append(dict(name=name, path=str(path), grain=grain, cls=cls, split_by=split_by))

    # 1/2: PFR player + boxscore tables (as v1, via glob so nothing hand-picked)
    for d in sorted((LAKE / "raw/pfr/players/tables").glob("*")):
        p = d / "_combined.parquet"
        if p.exists():
            add(f"pfr_season:{d.name}", p, "season_post" if d.name.endswith("_post") else "season")
    for d in sorted((LAKE / "raw/pfr/boxscores/tables").glob("*")):
        p = d / "_combined.parquet"
        if p.exists():
            add(f"pfr_box:{d.name}", p, "game")
    # 3: PFR context tables (NEW -- previously unregistered)
    for d in sorted((LAKE / "raw/pfr/context/tables").glob("*")):
        p = d / "_combined.parquet"
        if p.exists():
            add(f"pfr_context:{d.name}", p, "season")
    # 4: NFL.com -- bulk category season pages + team stats (per-file), shard dirs (split by _table)
    nflcom = LAKE / "raw/nflcom/tables"
    for p in sorted((nflcom / "player_season").glob("*.parquet")):
        add(f"nflcom_season:{p.stem}", p, "season")
    for p in sorted((nflcom / "team_stats").glob("*.parquet")):
        add(f"nflcom_team:{p.stem}", p, "season")
    add("nflcom_logs", nflcom / "player_logs" / "*.parquet", "game", split_by="_table")
    add("nflcom_career", nflcom / "player_career" / "*.parquet", "career", split_by="_table")
    add("nflcom_splits", nflcom / "player_splits" / "*.parquet", "season", split_by="_table")
    add("nflcom_situational", nflcom / "player_situational" / "*.parquet", "season", split_by="_table")
    for p in sorted((nflcom / "player_logs_targeted").glob("*.parquet")):
        add(f"nflcom_logs_targeted:{p.stem}", p, "game")
    # 5: PBP corpus + rollups + backfill twins + team defense
    sh = LAKE / "raw/stathead/generated"
    add("pbp_merged", sh / "pbp_merged_1978_2025/nfl_pbp_1978_2025_merged.parquet", "game")
    add("pbp_rollup_audit", sh / "pbp_supertable_audit_1978_2025/pbp_player_week_rollup.parquet", "game", cls="derived")
    add("pbp_weekly_rollup", sh / "pbp_weekly_rollup_1978_2025/pbp_player_week_rollup.parquet", "game", cls="derived")
    add("pbp_backfill_raw", sh / "pbp_backfill_1978_1998/stathead_pbp_1978_1998_raw.parquet", "game")
    add("pbp_backfill_twin", sh / "pbp_backfill_1978_1998/stathead_pbp_1978_1998_nflverse_twin.parquet", "game")
    add("pbp_team_defense", sh / "pbp_team_defense_1978_2025/pbp_team_defense_week.parquet", "game", cls="derived")
    # 6: NGS
    add("ngs_weekly", LAKE / "raw/nextgen_stats/ngs_weekly_2016_2025.parquet", "game")
    add("ngs_season", LAKE / "raw/nextgen_stats/ngs_season_2016_2025.parquet", "season")
    # 7: ancient bundle (canonical col names + row lineage)
    add("ancient_bundle", LAKE / "curated/ancient_source_recovery/ready_upsert_bundles/through_1979_pfr_loc/weekly_stat_upsert_rows.parquet", "game")
    # 8: awards / honors features
    add("awards_prior_by_year", LAKE / "derived/player_features/player_prior_awards_by_year.parquet", "season", cls="derived")
    # 9: team-game catalog + schedule + scoring summary
    add("team_games_all", LAKE / "raw/pfr/boxscores/nfl_team_games_all.parquet", "game")
    add("master_schedule", LAKE / "raw/pfr/cache/pfr_excel/_master_schedule_1920_2025.parquet", "game")
    add("scoring_summary", LAKE / "derived/scoring_summary/scoring_summary.parquet", "game", cls="derived")
    # 10: legacy supertable snapshot (diff context ONLY, never votes)
    add("legacy_supertable", LAKE / "raw/legacy_supertable_backup_sources/motherduck_legacy_super_table/nfl_super_table_backup_20251226_1825.parquet", "game", cls="subject_history")
    return W


def year_col(cols: dict[str, str]) -> str | None:
    for c in _YEAR_CANDIDATES:
        if c in cols:
            return c
    return None


# strip thousands separators AND percent signs: PFR/NFL.com serve rate columns as '62.5%' strings, which
# a bare TRY_CAST silently nulls -> whole witness families (snap_pct, cmp_pct, fg_pct...) read as absent.
# NULLIF 'nan': pandas-written parquets carry literal 'nan' strings; TRY_CAST('nan') = NaN and
# NaN <> 0 is TRUE in DuckDB, so without this a 0-coverage column reads as populated (caught
# 2026-07-16: "2,457 pre-1978 lost-fumble games" was 2,298 nan + 159 real).
NUM = ("TRY_CAST(NULLIF(LOWER(TRIM(REPLACE(REPLACE(NULLIF(CAST({c} AS VARCHAR), ''), ',', ''), '%', ''))), 'nan') AS DOUBLE)")


def scan_witness(con, w) -> dict:
    src = f"read_parquet('{Path(w['path']).as_posix()}', union_by_name=true)"
    desc = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    cols = {r[0]: r[1] for r in desc}
    stat_cols = [c for c, t in cols.items() if is_stat_col(c, t)]
    meta_cols = [c for c in cols if c not in stat_cols]
    yc = year_col(cols)
    n_rows = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    split = w.get("split_by") if w.get("split_by") in cols else None

    ykey = (f"TRY_CAST({yc} AS INT)" if yc else "NULL")
    yfilter = (f"regexp_matches(CAST({yc} AS VARCHAR), '^[0-9]{{4}}(\\.0)?$')" if yc else "TRUE")
    groups = ([f"COALESCE(LOWER(CAST({split} AS VARCHAR)),'?') AS grp"] if split else ["'' AS grp"]) + [f"{ykey} AS y"]

    def nz_expr(c: str) -> str:
        if cols[c].upper() == "BOOLEAN":
            return f'COUNT(*) FILTER (WHERE "{c}" = TRUE) AS "{c}"'
        return f'COUNT(*) FILTER (WHERE {NUM.format(c=f"\"{c}\"")} <> 0) AS "{c}"'

    # chunked single-pass aggregates
    per: dict[tuple, dict[str, int]] = {}
    for i in range(0, len(stat_cols), CHUNK):
        chunk = stat_cols[i:i + CHUNK]
        aggs = ", ".join(nz_expr(c) for c in chunk)
        q = f"SELECT {', '.join(groups)}, {aggs} FROM {src} WHERE {yfilter} GROUP BY 1, 2"
        for row in con.execute(q).fetchall():
            grp, y = row[0], row[1]
            d = per.setdefault((grp, y), {})
            for c, v in zip(chunk, row[2:]):
                d[c] = (d.get(c, 0) + int(v or 0))

    # fold into contracts (split_by -> multiple witnesses)
    out: dict[str, dict] = {}
    for (grp, y), counts in per.items():
        wname = f"{w['name']}:{grp}" if split else w["name"]
        c = out.setdefault(wname, {"grain": w["grain"], "cls": w["cls"], "path": w["path"],
                                   "n_rows": n_rows, "meta_cols": meta_cols, "atoms": {}, "by_year": {}})
        for col, n in counts.items():
            if n <= 0:
                continue
            atom = canonical(col, wname)
            a = c["atoms"].setdefault(atom, {"col": col, "era_min": None, "era_max": None, "total_nonzero": 0})
            a["total_nonzero"] += n
            if y is not None and n >= MIN_ROWS:
                a["era_min"] = y if a["era_min"] is None else min(a["era_min"], y)
                a["era_max"] = y if a["era_max"] is None else max(a["era_max"], y)
            c["by_year"].setdefault(str(y), {})[col] = n
    return out


def scan_newspaper(con) -> dict:
    db = LAKE / "derived/newspaper_atoms/databases/newspaper_atoms.duckdb"
    out = {}
    con.execute(f"ATTACH '{db.as_posix()}' AS np (READ_ONLY)")
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name='np' AND schema_name='newspaper_promoted'").fetchall()]
    for t in tables:
        try:
            desc = con.execute(f"DESCRIBE SELECT * FROM np.newspaper_promoted.{t}").fetchall()
            cols = {r[0]: r[1] for r in desc}
            n = con.execute(f"SELECT COUNT(*) FROM np.newspaper_promoted.{t}").fetchone()[0]
            stat_cols = [c for c, ty in cols.items() if is_stat_col(c, ty)]
            yc = year_col(cols)
            era = None
            if yc:
                era = con.execute(f"SELECT MIN(TRY_CAST({yc} AS INT)), MAX(TRY_CAST({yc} AS INT)) FROM np.newspaper_promoted.{t}").fetchone()
            out[f"newspaper_promoted:{t}"] = {
                "grain": "game", "cls": "pending_conflict_resolution", "path": str(db),
                "n_rows": n, "meta_cols": [c for c in cols if c not in stat_cols],
                "atoms": {canonical(c, "newspaper"): {"col": c, "era_min": era[0] if era else None,
                                                      "era_max": era[1] if era else None,
                                                      "total_nonzero": None} for c in stat_cols},
                "by_year": {}}
        except Exception as e:  # noqa: BLE001
            out[f"newspaper_promoted:{t}"] = {"error": str(e)}
    return out


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA threads=6")
    contracts: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for w in _witnesses():
        try:
            got = scan_witness(con, w)
            if not got:
                errors[w["name"]] = "no rows / no year groups"
            contracts.update(got)
            print(f"OK   {w['name']:44} -> {len(got)} contract(s)", flush=True)
        except Exception as e:  # noqa: BLE001
            errors[w["name"]] = f"{type(e).__name__}: {e}"
            print(f"FAIL {w['name']:44} {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(limit=1)
    try:
        got = scan_newspaper(con)
        contracts.update(got)
        print(f"OK   newspaper_promoted -> {len(got)} table contract(s)", flush=True)
    except Exception as e:  # noqa: BLE001
        errors["newspaper_promoted"] = str(e)
        print(f"FAIL newspaper_promoted {e}", flush=True)

    n_atoms = len({a for c in contracts.values() for a in c.get("atoms", {})})
    n_inst = sum(len(c.get("atoms", {})) for c in contracts.values())
    summary = {"n_contracts": len(contracts), "n_column_instances": n_inst,
               "n_distinct_atoms": n_atoms, "errors": errors, "min_rows": MIN_ROWS}
    OUT.write_text(json.dumps({"summary": summary, "contracts": contracts}, indent=1, sort_keys=True))
    print(json.dumps(summary, indent=2))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
