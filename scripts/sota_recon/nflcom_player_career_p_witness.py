"""Measure NFL.com P Career witnesses, including the net-punt demand candidate."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry, v26_plane


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_p_witness.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)
NET_SCHEMA_NAMES = ("punt_net_yards", "punt_net_avg", "net_punt_yards", "net_punt_avg")


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def close_rate(left: float | None, right: float | None, tolerance: float = 0.11) -> bool:
    return left is not None and right is not None and abs(left - right) <= tolerance


def rate_from_counts(yards: float | None, punts: float | None) -> float | None:
    if yards is None or punts is None or punts == 0:
        return None
    return yards / punts


def _count_measurement(con: duckdb.DuckDBPyConnection, source_field: str, canonical: str, form: str, v26: str) -> dict:
    aggregate = "MAX" if form == "MAX" else "SUM"
    sql = f"""WITH s AS (
        SELECT nflcom_slug, {aggregate}(TRY_CAST("{source_field}" AS DOUBLE)) source_value
        FROM src WHERE TRY_CAST("{source_field}" AS DOUBLE) IS NOT NULL GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug, MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v."{canonical}" IS NOT NULL GROUP BY 1
      ) SELECT s.source_value, v.target_value FROM s JOIN v USING (nflcom_slug)"""
    pairs = con.execute(sql).fetchall()
    informative = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a != 0 or b != 0)]
    agree = sum(int(a == b) for a, b in informative)
    return {
        "source_field": source_field,
        "canonical": canonical,
        "witness_form": form,
        "paired_n": len(pairs),
        "informative_n": len(informative),
        "agree_n": agree,
        "agree_pct": round(100.0 * agree / len(informative), 2) if informative else None,
        "source_exceeds_target": sum(int(a > b) for a, b in informative),
        "target_exceeds_source": sum(int(a < b) for a, b in informative),
    }


def _rate_measurement(con: duckdb.DuckDBPyConnection, v26: str) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug,
               SUM(TRY_CAST(yds AS DOUBLE)) yds,
               SUM(TRY_CAST(punts AS DOUBLE)) punts,
               MAX(TRY_CAST(avg AS DOUBLE)) published_avg
        FROM src GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug,
               MAX(TRY_CAST(v.punt_yards AS DOUBLE)) yds,
               MAX(TRY_CAST(v.punts AS DOUBLE)) punts,
               MAX(TRY_CAST(v.punt_yards_per_punt AS DOUBLE)) stored_avg
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        GROUP BY 1
      ) SELECT s.published_avg, s.yds, s.punts, v.yds, v.punts, v.stored_avg
        FROM s JOIN v USING (nflcom_slug)"""
    rows = con.execute(sql).fetchall()
    informative = []
    for published, yds, punts, v_yds, v_punts, stored in rows:
        source_expected = rate_from_counts(yds, punts)
        target_expected = rate_from_counts(v_yds, v_punts)
        if source_expected is None and target_expected is None:
            continue
        informative.append((published, source_expected, target_expected, stored))
    stored_pairs = [(s, t) for _, s, _, t in informative if s is not None and t is not None]
    expected_pairs = [(s, t) for _, s, t, _ in informative if s is not None and t is not None]
    return {
        "source_field": "avg",
        "canonical": "punt_yards_per_punt",
        "witness_form": "RECOMPUTE(SUM(yds)/SUM(punts))",
        "paired_n": len(rows),
        "informative_n": len(informative),
        "source_published_vs_source_operands_agree_n": sum(int(close_rate(p, s)) for p, s, _, _ in informative if p is not None and s is not None),
        "source_operands_vs_v26_operands_agree_n": sum(int(close_rate(s, t)) for s, t in expected_pairs),
        "source_operands_vs_v26_operands_agree_pct": round(100.0 * sum(int(close_rate(s, t)) for s, t in expected_pairs) / len(expected_pairs), 2) if expected_pairs else None,
        "source_operands_vs_v26_stored_rate_agree_n": sum(int(close_rate(s, t)) for s, t in stored_pairs),
        "source_operands_vs_v26_stored_rate_agree_pct": round(100.0 * sum(int(close_rate(s, t)) for s, t in stored_pairs) / len(informative), 2) if informative else None,
        "source_operands_exceeds_v26_stored_rate": sum(int(s > t) for s, t in stored_pairs),
        "v26_stored_rate_exceeds_source_operands": sum(int(s < t) for s, t in stored_pairs),
        "rate_denominator": "player-level SUM(yds)/SUM(punts); zero/zero omitted",
        "rate_units": "yards per punt",
        "stored_target_status": "SUM-shaped career rate residual; not the desired witness",
        "preferred_target_form": "RECOMPUTE(v26 career punt_yards / v26 career punts)",
    }


def measure() -> dict:
    con = duckdb.connect()
    src_glob = _q(SOURCE / "**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS
        SELECT DISTINCT nflcom_slug, season, team, g, gs, punts, yds, avg, lng, blk,
                        net_avg, net_yds, dn, in_20, fc, ret, rety, tb, td
        FROM read_parquet('{src_glob}', union_by_name=true)
        WHERE _table='P Career'""")
    con.execute(f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(v26_plane("career"))
    counts = [
        _count_measurement(con, "g", "games_played", "SUM", v26),
        _count_measurement(con, "gs", "games_started", "SUM", v26),
        _count_measurement(con, "punts", "punts", "SUM", v26),
        _count_measurement(con, "yds", "punt_yards", "SUM", v26),
        _count_measurement(con, "lng", "punt_long", "MAX", v26),
        _count_measurement(con, "blk", "punts_blocked", "SUM", v26),
    ]
    schema_by_plane = {}
    for plane in ("weekly", "season", "career", "season_team"):
        path = _q(v26_plane(plane))
        schema_by_plane[plane] = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    return {
        "measurement_only": True,
        "source_table": "P Career",
        "subject_plane": "player_nfl_career",
        "source_deduplication": "SELECT DISTINCT including season/team before aggregation",
        "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),
        "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),
        "counts": counts,
        "rate": _rate_measurement(con, v26),
        "net_rate_candidate": {
            "source_fields": ["net_avg", "net_yds", "punts"],
            "source_identity_form": "net_avg compared with net_yds/punts; target unavailable",
            "schema_search_names": list(NET_SCHEMA_NAMES),
            "schema_search_found_by_plane": {plane: any(name in cols for name in NET_SCHEMA_NAMES) for plane, cols in schema_by_plane.items()},
            "status": "NO_CURRENT_V26_CANONICAL" if not any(any(name in cols for name in NET_SCHEMA_NAMES) for cols in schema_by_plane.values()) else "CURRENT_V26_CANONICAL_FOUND",
        },
        "no_state_change": True,
    }


def main() -> None:
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
