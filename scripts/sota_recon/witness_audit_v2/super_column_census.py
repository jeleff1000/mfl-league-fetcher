"""Census every column of the local v26 super table at all grains (weekly/season/career).

Classifies each column into a family so the witness matrix can treat them differently:
  identity/meta -> not stats, never need witnesses
  atom          -> raw stat atoms, need DIRECT witnesses
  scoring       -> pts_*/fpts_* computed, need FORMULA lineage -> witnessed inputs
  rank/ppg/agg  -> computed over atoms, need lineage
  research      -> lamar/percentile layer
  flag/prov     -> provenance & QA flags
"""
import glob
import json
import re
from pathlib import Path

import duckdb

LAKE = Path("D:/league-history-data/nfl")
OUT = Path(r"D:\league-history-data\nfl\derived\validation\witness_audit_2026_07_16\super_column_census.json")


def latest_v26() -> Path:
    files = sorted(glob.glob(str(LAKE / "releases" / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
                   key=lambda p: Path(p).stat().st_mtime, reverse=True)
    return Path(files[0])


IDENTITY = {
    "NFL_player_id", "player", "player_week", "year", "week", "season_type", "team", "opponent",
    "nfl_team", "opponent_nfl_team", "position", "nfl_position", "primary_position", "age",
    "headshot_url", "gsis_id", "pfr_id", "boxscore_id", "game_date", "home_away",
    "nfl_franchise_number", "opponent_franchise_number", "player_id", "first_name", "last_name",
    "full_name", "birth_date", "college", "draft_year", "draft_round", "draft_pick", "height",
    "weight", "years", "managers", "games",
}


def classify(col: str) -> str:
    c = col.lower()
    if col in IDENTITY or c.endswith(("_id", "_url", "_name", "_key", "_date", "_slug")):
        return "identity"
    if c.startswith(("is_", "has_", "flag_", "qa_", "prov_", "src_", "source_")) or c.endswith(("_source", "_provenance", "_flag", "_imputed", "_is_estimated")):
        return "flag_provenance"
    if c.startswith(("pts_", "fpts_")) or c.endswith(("_fpts", "_pts_std", "_pts_ppr", "_pts_half")):
        return "scoring"
    if c.startswith("rank_") or c.endswith(("_rank", "_rank_overall", "_pctl", "_percentile")):
        return "rank"
    if "lamar" in c or c.startswith("research_") or "replacement" in c:
        return "research"
    if c.endswith(("_ppg", "_per_game", "_avg", "_pct", "_rate", "_share", "_ratio", "_per_att", "_per_carry", "_per_target", "_per_touch", "_wmean")) or c.startswith(("avg_", "pct_")):
        return "rate_agg"
    return "atom"


def census(path: Path, grain: str, con) -> dict:
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')").fetchall()
    cols = {}
    fam_counts = {}
    for name, dtype, *_ in desc:
        fam = classify(name)
        cols[name] = {"dtype": dtype, "family": fam}
        fam_counts[fam] = fam_counts.get(fam, 0) + 1
    n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]
    return {"path": str(path), "grain": grain, "n_rows": n_rows, "n_cols": len(cols),
            "family_counts": fam_counts, "columns": cols}


def main():
    wk = latest_v26()
    rel = wk.parent  # .../tables
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    tables = {"weekly": wk}
    sc_dir = rel / "season_career_v26"
    if sc_dir.exists():
        for p in sorted(sc_dir.glob("*.parquet")):
            key = p.stem  # player_nfl_season, player_nfl_career, *_all
            tables[key] = p
    result = {}
    for key, p in tables.items():
        grain = "weekly" if key == "weekly" else ("season" if "season" in key else "career")
        result[key] = census(p, grain, con)
        fc = result[key]["family_counts"]
        print(f"{key:28} rows={result[key]['n_rows']:>10,} cols={result[key]['n_cols']:>5}  " +
              "  ".join(f"{k}={v}" for k, v in sorted(fc.items())))
    OUT.write_text(json.dumps(result, indent=1))
    print(f"\nwrote {OUT}")
    # print the weekly atom list (the columns that need DIRECT witnesses)
    atoms = sorted(c for c, m in result["weekly"]["columns"].items() if m["family"] == "atom")
    print(f"\nWEEKLY 'atom' columns ({len(atoms)}):")
    for i in range(0, len(atoms), 6):
        print("   " + ", ".join(atoms[i:i + 6]))


if __name__ == "__main__":
    main()
