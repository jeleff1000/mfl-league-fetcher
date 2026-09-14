"""Measure Opponents-by-Group/L0 defensive witnesses."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_splits_opponents_group_l0_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_splits"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)
MAPPINGS = {
    "ast": "def_tackle_assists",
    "int": "def_interceptions",
    "pdef": "def_pass_defended",
    "sck": "def_sacks",
    "sfty": "def_safeties",
    "solo": "def_tackles_solo",
    "tds": "def_int_ret_td",
    "total": "def_tackles_combined",
    "yds": "def_interception_yards",
}


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _compare(pairs):
    info = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a or b)]
    agree = sum(int(a == b) for a, b in info)
    return {
        "joined_n": len(pairs),
        "informative_n": len(info),
        "agree_n": agree,
        "agree_pct": round(100 * agree / len(info), 2) if info else None,
        "source_exceeds_target": sum(int(a > b) for a, b in info),
        "target_exceeds_source": sum(int(a < b) for a, b in info),
    }


def _measure(con, source_field: str, canonical: str, v26: str):
    sql = f"""
    WITH s AS (
      SELECT nflcom_slug, TRY_CAST(season AS INTEGER) season_year,
             SUM(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value
      FROM src WHERE TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL
      GROUP BY 1, 2
    ), v AS (
      SELECT i.nflcom_slug, v.year season_year,
             SUM(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) target_value
      FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
      WHERE v.season_type='REG' AND v.\"{canonical}\" IS NOT NULL
      GROUP BY 1, 2
    ) SELECT s.source_value, v.target_value FROM s JOIN v USING(nflcom_slug, season_year)
    """
    return {"source_field": source_field, "canonical": canonical, "witness_form": "SUM", **_compare(con.execute(sql).fetchall())}


def _identity(con):
    rows = con.execute("SELECT TRY_CAST(total AS DOUBLE),TRY_CAST(solo AS DOUBLE),TRY_CAST(ast AS DOUBLE) FROM src").fetchall()
    complete = [(a, b, c) for a, b, c in rows if None not in (a, b, c)]
    return {
        "complete_n": len(complete),
        "total_eq_solo_plus_ast": sum(int(a == b + c) for a, b, c in complete),
        "total_exceeds_parts": sum(int(a > b + c) for a, b, c in complete),
        "parts_exceed_total": sum(int(a < b + c) for a, b, c in complete),
    }


def measure():
    con = duckdb.connect()
    source = _q(SOURCE / "**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE raw_src AS SELECT * FROM read_parquet('{source}',union_by_name=true) WHERE _table='Opponents by Group' AND _layout='player_splits_L0'""")
    con.execute("""CREATE TEMP TABLE src AS SELECT DISTINCT nflcom_slug,season,split_value,_lost_column,ast,int,pdef,sck,sfty,solo,tds,total,yds,lng,avg FROM raw_src WHERE split_value IN ('vs AFC Teams','vs NFC Teams')""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(latest_v26())
    fields = [_measure(con, s, c, v26) for s, c in MAPPINGS.items()]
    candidates = {}
    candidate_targets = {"solo": ("def_tackles_solo", "def_tackles_combined", "def_tackle_assists", "def_tackles_with_assist"), "total": ("def_tackles_combined", "def_tackles_solo", "def_tackle_assists", "def_tackles_with_assist"), "ast": ("def_tackle_assists", "def_tackles_solo", "def_tackles_combined", "def_tackles_with_assist")}
    for source_field, canonicals in candidate_targets.items():
        candidates[source_field] = []
        for canonical in canonicals:
            row = _measure(con, source_field, canonical, v26)
            row["candidate_role"] = "natural" if canonical == MAPPINGS[source_field] else "family contrast"
            candidates[source_field].append(row)
    loss = con.execute("SELECT _lost_column,COUNT(*) n FROM src GROUP BY 1 ORDER BY 1").fetchall()
    return {
        "measurement_only": True,
        "source_table": "Opponents by Group",
        "layout": "player_splits_L0",
        "subject_plane": "sum four opponent-group rows to player-season",
        "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),
        "source_rows_raw": int(con.execute("SELECT COUNT(*) FROM raw_src").fetchone()[0]),
        "excluded_nested_rows": int(con.execute("SELECT COUNT(*) FROM raw_src WHERE split_value NOT IN ('vs AFC Teams','vs NFC Teams')").fetchone()[0]),
        "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),
        "source_split_values": int(con.execute("SELECT COUNT(DISTINCT split_value) FROM src").fetchone()[0]),
        "lost_columns": [{"column": str(a) if a is not None else None, "rows": int(b)} for a, b in loss],
        "identity": _identity(con),
        "fields": fields,
        "candidate_matrix": candidates,
        "target_aggregation": "SUM weekly REG by (NFL_player_id, year)",
        "no_state_change": True,
    }


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
