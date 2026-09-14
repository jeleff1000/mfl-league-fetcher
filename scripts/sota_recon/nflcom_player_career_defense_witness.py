"""Measure the NFL.com Defense Career identity and v26 witness candidates.

This is an audit-only lane.  The source contains repeated/team rows, so exact
duplicate source rows are removed before the career aggregation.  The identity
check is deliberately separate from the v26 comparison: it determines whether
``tkl`` is the unassisted count or whether NFL.com's ``solo`` cell is safe to
use.  Unknown or unreconciled values remain in the receipt; nothing is licensed
or backfilled here.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry, v26_plane


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_defense_witness.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def identity_counts(rows: list[tuple[float, float, float, float]]) -> dict[str, int]:
    """Return complete-row counts for the two competing tackle identities."""
    combined_eq_tkl_plus_ast = sum(int(combined == tkl + ast) for tkl, ast, solo, combined in rows)
    combined_eq_solo_plus_ast = sum(int(combined == solo + ast) for tkl, ast, solo, combined in rows)
    return {
        "complete_n": len(rows),
        "combined_eq_tkl_plus_ast": combined_eq_tkl_plus_ast,
        "combined_eq_solo_plus_ast": combined_eq_solo_plus_ast,
    }


def _candidate_measurement(
    con: duckdb.DuckDBPyConnection, canonical: str, source_field: str, v26: str
) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug, SUM(TRY_CAST(tkl AS DOUBLE)) AS tkl,
               SUM(TRY_CAST(ast AS DOUBLE)) AS ast,
               SUM(TRY_CAST(combined AS DOUBLE)) AS combined,
               SUM(TRY_CAST(solo AS DOUBLE)) AS solo
        FROM src
        GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug, MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) AS target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        GROUP BY 1
      ), p AS (
        SELECT s.nflcom_slug, s.tkl, s.ast, s.combined, s.solo, v.target_value
        FROM s JOIN v USING (nflcom_slug)
      )
      SELECT COUNT(*) AS paired_n,
             SUM(CASE WHEN target_value IS NOT NULL THEN 1 ELSE 0 END) AS informative_n,
             SUM(CASE WHEN {source_field} IS NOT NULL AND target_value IS NOT NULL AND {source_field}=target_value THEN 1 ELSE 0 END) AS agree_n,
             SUM(CASE WHEN {source_field} IS NOT NULL AND target_value IS NOT NULL AND {source_field}>target_value THEN 1 ELSE 0 END) AS source_exceeds_target,
             SUM(CASE WHEN {source_field} IS NOT NULL AND target_value IS NOT NULL AND {source_field}<target_value THEN 1 ELSE 0 END) AS target_exceeds_source
      FROM p"""
    row = con.execute(sql).fetchone()
    keys = ("paired_n", "informative_n", "agree_n", "source_exceeds_target", "target_exceeds_source")
    result = {key: int(value or 0) for key, value in zip(keys, row)} | {
        "canonical": canonical,
        "source_field": source_field,
        "target_aggregation": "MAX",
    }
    result["agree_pct"] = round(100.0 * result["agree_n"] / result["informative_n"], 2) if result["informative_n"] else None
    return result


def _count_measurement(
    con: duckdb.DuckDBPyConnection, source_field: str, canonical: str, v26: str
) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug, SUM(TRY_CAST("{source_field}" AS DOUBLE)) source_value
        FROM src WHERE TRY_CAST("{source_field}" AS DOUBLE) IS NOT NULL GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug, MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v."{canonical}" IS NOT NULL GROUP BY 1
      ) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug)"""
    pairs = con.execute(sql).fetchall()
    info = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a != 0 or b != 0)]
    agree = sum(int(a == b) for a, b in info)
    return {
        "source_field": source_field, "canonical": canonical, "witness_form": "SUM",
        "paired_n": len(pairs), "informative_n": len(info), "agree_n": agree,
        "agree_pct": round(100.0 * agree / len(info), 2) if info else None,
        "source_exceeds_target": sum(int(a > b) for a, b in info),
        "target_exceeds_source": sum(int(a < b) for a, b in info),
    }


def measure() -> dict:
    con = duckdb.connect()
    src_glob = _q(SOURCE / "**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS
        SELECT DISTINCT nflcom_slug, season, team, tkl, ast, combined, solo, g, gs, int, ff,
                        opp_fr, pdef, sck, sfty, tds, yds
        FROM read_parquet('{src_glob}', union_by_name=true)
        WHERE _table='Defense Career'""")
    con.execute(f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")

    identity_rows = con.execute("""SELECT TRY_CAST(tkl AS DOUBLE), TRY_CAST(ast AS DOUBLE),
            TRY_CAST(solo AS DOUBLE), TRY_CAST(combined AS DOUBLE)
        FROM src
        WHERE TRY_CAST(tkl AS DOUBLE) IS NOT NULL
          AND TRY_CAST(ast AS DOUBLE) IS NOT NULL
          AND TRY_CAST(solo AS DOUBLE) IS NOT NULL
          AND TRY_CAST(combined AS DOUBLE) IS NOT NULL""").fetchall()
    identity = identity_counts([(float(a), float(b), float(c), float(d)) for a, b, c, d in identity_rows])

    v26 = _q(v26_plane("career"))
    counts = [
        _count_measurement(con, source_field, canonical, v26)
        for source_field, canonical in (
            ("g", "games_played"), ("gs", "games_started"),
            ("int", "def_interceptions"), ("ff", "def_fumbles_forced"),
            ("opp_fr", "fumble_recovery_opp"), ("pdef", "def_pass_defended"),
            ("sck", "def_sacks"), ("sfty", "def_safeties"),
            ("tds", "def_int_ret_td"), ("yds", "def_interception_yards"),
            ("ast", "def_tackle_assists"), ("combined", "def_tackles_combined"),
            ("tkl", "def_tackles_solo"),
        )
    ]
    candidate_map = {
        "def_tackles_solo": "tkl",
        "def_tackles_combined": "combined",
        "def_tackles_with_assist": "combined",
        "def_tackle_assists": "ast",
    }
    candidates = [
        _candidate_measurement(con, canonical, source_field, v26)
        for canonical, source_field in candidate_map.items()
    ]
    return {
        "measurement_only": True,
        "source_table": "Defense Career",
        "source_deduplication": "SELECT DISTINCT source rows before aggregation",
        "source_grain": "nflcom_slug with repeated/team rows",
        "subject_plane": "player_nfl_career",
        "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),
        "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),
        "identity": identity,
        "identity_denominator": "complete rows with numeric tkl, ast, solo, combined",
        "counts": counts,
        "candidates": candidates,
        "interpretation": {
            "tkl": "candidate unassisted tackle count; selected only because combined=tkl+ast identity is measured",
            "solo": "raw NFL.com cell retained as residual; not presumed equivalent to v26 solo",
            "target_denominator": "paired player IDs with non-null v26 career target",
        },
    }


def main() -> None:
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
