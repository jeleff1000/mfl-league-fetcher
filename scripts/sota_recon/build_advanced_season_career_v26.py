"""
sota_recon/build_advanced_season_career_v26.py  --  aggregate the new advanced weekly stats
onto the 4 NFL season/career tables (local only, additive; small player-grain tables so fast).

For the 39 SUM-able advanced atoms/composites/team-DST cols: season = Sigma(weekly) with the
grain each table already uses (verified exact on passing_epa):
  player_nfl_season       = Sigma weekly REG  by (NFL_player_id, year)
  player_nfl_season_all   = Sigma weekly ALL  by (NFL_player_id, year)
  player_nfl_career       = Sigma weekly REG  by  NFL_player_id
  player_nfl_career_all   = Sigma weekly ALL  by  NFL_player_id
EP-gated atoms (WPA/success) stay NULL pre-1999 automatically (SUM of all-NULL -> NULL).

For NGS (18 cols): season tables get nflverse's PUBLISHED season value directly --
our weekly->season weighted average does NOT reproduce it (validated), so we ingest the exact
public number. Career values are derived from those published season values using the explicit
NGS denominator map below; additive rush totals sum, while direct rates are denominator-weighted.

Each table: backup + atomic swap. Idempotent (re-derives via EXCLUDE). Ships nothing to Fly.

    python -m scripts.sota_recon.build_advanced_season_career_v26            # dry-run
    python -m scripts.sota_recon.build_advanced_season_career_v26 --validate # build temp outputs, do not swap
    python -m scripts.sota_recon.build_advanced_season_career_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from .build_team_dst_advanced_v26 import DEF_COLS
from .recon_common import utc_stamp
from .sources import latest_v26

OFF_SUMABLE = [
    "passing_wpa", "rushing_wpa", "receiving_wpa",
    "pass_success", "pass_success_plays", "rush_success", "rush_success_plays",
    "rec_success", "rec_success_plays",
    "pass_explosive_20", "rush_explosive_10", "rec_explosive_20",
    "rz_pass_att", "rz_pass_td", "rz_carries", "rz_rush_td", "rz_targets", "rz_rec_td",
    "total_epa", "total_wpa", "scrimmage_yards", "total_tds_accounted_for", "total_touches",
]
# wave52/54 weekly columns -> season/career (Joe 2026-07-12: "season and career need
# new columns as appropriate"). scrimmage_yards/total_tds_accounted_for already ride OFF_SUMABLE.
W52_SUMS = [
    "touches", "opportunities", "turnovers", "scrimmage_tds", "total_return_yards",
    "def_tackles_combined", "all_purpose_yards", "total_points_scored", "dropbacks",
    "fg_made_60plus", "passing_completed_air_yards", "receiving_completed_air_yards",
]
# rates NEVER sum (the NGS lesson): season value = ratio of summed components
RATE_ADDS = ["yards_per_touch", "adjusted_yards_per_attempt",
             "net_yards_per_attempt", "adjusted_net_yards_per_attempt"]
# game-context aggregates: wins while playing + games started (starter-list witnessed)
CTX_ADDS = ["wins", "games_started"]

SUMABLE = OFF_SUMABLE + DEF_COLS + W52_SUMS  # 23 + 16 + 12 = 51

NGS_SEASON = Path("D:/league-history-data/nfl/raw/nextgen_stats/ngs_season_2016_2025.parquet")
PLAYER_BIO = Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")
SC_DIR = Path(latest_v26()).parent / "season_career_v26"

NGS_ADDITIVE = {"ngs_expected_rush_yards", "ngs_rush_yards_over_expected"}
NGS_COMPONENT_DERIVED = {
    "ngs_pct_share_intended_air_yards": "team_receiving_air_yards",
    "ngs_avg_air_yards_differential": "passing_air_yards_components",
}
NGS_DENOMINATOR = {
    "ngs_avg_cushion": "targets",
    "ngs_avg_separation": "targets",
    "ngs_avg_yac": "receptions",
    "ngs_avg_expected_yac": "receptions",
    "ngs_avg_yac_above_expectation": "receptions",
    "ngs_rush_efficiency": "rushing_yards",
    "ngs_pct_att_gte_8_defenders": "carries",
    "ngs_avg_time_to_los": "carries",
    "ngs_rush_pct_over_expected": "carries",
    "ngs_avg_time_to_throw": "attempts",
    "ngs_aggressiveness": "attempts",
    "ngs_avg_air_yards_to_sticks": "attempts",
    "ngs_expected_completion_pct": "attempts",
    "ngs_completion_pct_above_expectation": "attempts",
}

# (filename, group key, season_type filter, add NGS season values)
TABLES = [
    ("player_nfl_season.parquet", ["NFL_player_id", "year"], "season_type='REG'", True, "published"),
    ("player_nfl_season_all.parquet", ["NFL_player_id", "year"], "1=1", True, "published"),
    ("player_nfl_career.parquet", ["NFL_player_id"], "season_type='REG'", True, "career"),
    ("player_nfl_career_all.parquet", ["NFL_player_id"], "1=1", True, "career_all"),
]


def _ngs_cols(con) -> list[str]:
    return [c for c in (r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{NGS_SEASON.as_posix()}')").fetchall()) if c.startswith("ngs_")]


def _validate_ngs_bio_identity(con: duckdb.DuckDBPyConnection) -> dict[str, int | bool]:
    """Require every NGS player identity to resolve through the canonical bio spine."""
    row = con.execute(
        f"""
        WITH ngs AS (SELECT DISTINCT NFL_player_id FROM read_parquet('{NGS_SEASON.as_posix()}')),
        bio AS (SELECT DISTINCT NFL_player_id FROM read_parquet('{PLAYER_BIO.as_posix()}'))
        SELECT COUNT(*) AS source_ids,
               COUNT(b.NFL_player_id) AS source_ids_in_bio,
               COUNT(*) FILTER (WHERE b.NFL_player_id IS NULL) AS source_ids_missing_bio,
               (SELECT COUNT(*) FROM bio b2 JOIN ngs n2 USING (NFL_player_id)) AS bio_ids_with_ngs
        FROM ngs n LEFT JOIN bio b USING (NFL_player_id)
        """
    ).fetchone()
    source_ids, source_ids_in_bio, source_ids_missing_bio, bio_ids_with_ngs = (int(v) for v in row)
    bio_duplicate_ids = int(con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT NFL_player_id
            FROM read_parquet('{PLAYER_BIO.as_posix()}')
            GROUP BY NFL_player_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0])
    return {
        "source_ids": source_ids,
        "source_ids_in_bio": source_ids_in_bio,
        "source_ids_missing_bio": source_ids_missing_bio,
        "bio_ids_with_ngs": bio_ids_with_ngs,
        "bio_duplicate_ids": bio_duplicate_ids,
        "passed": source_ids_missing_bio == 0 and bio_duplicate_ids == 0,
    }


def _create_career_ngs_aggregate(
    con: duckdb.DuckDBPyConnection,
    season_path: Path,
    ngs_cols: list[str],
) -> None:
    """Materialize the explicit season->career NGS denominator-weighted rollup."""
    denominator_check = _validate_ngs_denominators(con, season_path, ngs_cols)
    invalid = {
        column: values for column, values in denominator_check.items()
        if values["invalid_rows"]
    }
    if invalid:
        raise ValueError(f"NGS career denominator coverage failed: {invalid}")
    expressions = []
    for column in ngs_cols:
        if column in NGS_ADDITIVE:
            expressions.append(f"SUM(g.{column}) AS {column}")
            continue
        if column == "ngs_pct_share_intended_air_yards":
            expressions.append(
                f"SUM(g.{column} * d.team_receiving_air_yards) / "
                f"NULLIF(SUM(CASE WHEN g.{column} IS NOT NULL "
                f"THEN d.team_receiving_air_yards ELSE 0 END), 0) AS {column}"
            )
            continue
        if column == "ngs_avg_air_yards_differential":
            expressions.append(
                "SUM(CASE WHEN g.ngs_avg_air_yards_differential IS NOT NULL "
                "THEN d.passing_completed_air_yards ELSE 0 END) / "
                "NULLIF(SUM(CASE WHEN g.ngs_avg_air_yards_differential IS NOT NULL "
                "THEN d.completions ELSE 0 END), 0) - "
                "SUM(CASE WHEN g.ngs_avg_air_yards_differential IS NOT NULL "
                "THEN d.passing_air_yards ELSE 0 END) / "
                "NULLIF(SUM(CASE WHEN g.ngs_avg_air_yards_differential IS NOT NULL "
                "THEN d.attempts ELSE 0 END), 0) AS ngs_avg_air_yards_differential"
            )
            continue
        denominator = NGS_DENOMINATOR[column]
        expressions.append(
            f"SUM(g.{column} * d.{denominator}) / "
            f"NULLIF(SUM(CASE WHEN g.{column} IS NOT NULL THEN d.{denominator} ELSE 0 END), 0) AS {column}"
        )
    con.execute("DROP TABLE IF EXISTS ngs_career_aggregate")
    con.execute(
        f"""
        CREATE TEMP TABLE ngs_career_aggregate AS
        SELECT g.NFL_player_id,
               {', '.join(expressions)}
        FROM read_parquet('{NGS_SEASON.as_posix()}') g
        JOIN (
            SELECT d.*,
                   SUM(COALESCE(d.receiving_air_yards, 0)) OVER (
                       PARTITION BY CAST(d.year AS INTEGER), d.nfl_team
                   ) AS team_receiving_air_yards
            FROM read_parquet('{season_path.as_posix()}') d
        ) d
          ON d.NFL_player_id = g.NFL_player_id
         AND CAST(d.year AS INTEGER) = g.year
        GROUP BY g.NFL_player_id
        """
    )


def _validate_ngs_denominators(
    con: duckdb.DuckDBPyConnection,
    season_path: Path,
    ngs_cols: list[str],
) -> dict[str, dict[str, int]]:
    """Check that every published direct-rate cell has a positive career denominator."""
    result: dict[str, dict[str, int]] = {}
    for column in ngs_cols:
        if column in NGS_ADDITIVE:
            continue
        if column == "ngs_pct_share_intended_air_yards":
            expected, invalid = con.execute(
                f"""
                SELECT COUNT(*) AS expected_rows,
                       COUNT(*) FILTER (
                           WHERE d.NFL_player_id IS NULL
                              OR d.team_receiving_air_yards <= 0
                       ) AS invalid_rows
                FROM read_parquet('{NGS_SEASON.as_posix()}') g
                LEFT JOIN (
                    SELECT d.*,
                           SUM(COALESCE(d.receiving_air_yards, 0)) OVER (
                               PARTITION BY CAST(d.year AS INTEGER), d.nfl_team
                           ) AS team_receiving_air_yards
                    FROM read_parquet('{season_path.as_posix()}') d
                ) d
                  ON d.NFL_player_id = g.NFL_player_id
                 AND CAST(d.year AS INTEGER) = g.year
                WHERE g.{column} IS NOT NULL
                """
            ).fetchone()
            result[column] = {"expected_rows": int(expected), "invalid_rows": int(invalid)}
            continue
        if column == "ngs_avg_air_yards_differential":
            expected, invalid = con.execute(
                f"""
                SELECT COUNT(*) AS expected_rows,
                       COUNT(*) FILTER (
                           WHERE d.NFL_player_id IS NULL
                              OR d.completions <= 0 OR d.attempts <= 0
                              OR d.passing_completed_air_yards IS NULL
                              OR d.passing_air_yards IS NULL
                       ) AS invalid_rows
                FROM read_parquet('{NGS_SEASON.as_posix()}') g
                LEFT JOIN read_parquet('{season_path.as_posix()}') d
                  ON d.NFL_player_id = g.NFL_player_id
                 AND CAST(d.year AS INTEGER) = g.year
                WHERE g.{column} IS NOT NULL
                """
            ).fetchone()
            result[column] = {"expected_rows": int(expected), "invalid_rows": int(invalid)}
            continue
        denominator = NGS_DENOMINATOR[column]
        expected, invalid = con.execute(
            f"""
            SELECT COUNT(*) AS expected_rows,
                   COUNT(*) FILTER (
                       WHERE d.NFL_player_id IS NULL OR COALESCE(d.{denominator}, 0) <= 0
                   ) AS invalid_rows
            FROM read_parquet('{NGS_SEASON.as_posix()}') g
            LEFT JOIN read_parquet('{season_path.as_posix()}') d
              ON d.NFL_player_id = g.NFL_player_id
             AND CAST(d.year AS INTEGER) = g.year
            WHERE g.{column} IS NOT NULL
            """
        ).fetchone()
        result[column] = {"expected_rows": int(expected), "invalid_rows": int(invalid)}
    return result


def _validate_career_ngs_values(
    con: duckdb.DuckDBPyConnection,
    target_path: str,
    ngs_cols: list[str],
) -> dict[str, int | bool]:
    """Require career NGS cells to equal the denominator-mapped rollup."""
    mismatch = " OR ".join(
        f"NOT (t.{column} IS NOT DISTINCT FROM e.{column})" for column in ngs_cols
    )
    row = con.execute(
        f"""
        SELECT COUNT(*) AS expected_rows,
               COUNT(t.NFL_player_id) AS matched_rows,
               COUNT(*) FILTER (WHERE t.NFL_player_id IS NULL) AS missing_rows,
               COUNT(*) FILTER (
                   WHERE t.NFL_player_id IS NOT NULL AND ({mismatch})
               ) AS mismatch_rows
        FROM ngs_career_aggregate e
        LEFT JOIN read_parquet('{target_path}') t USING (NFL_player_id)
        """
    ).fetchone()
    expected_rows, matched_rows, missing_rows, mismatch_rows = (int(value) for value in row)
    return {
        "expected_rows": expected_rows,
        "matched_rows": matched_rows,
        "missing_rows": missing_rows,
        "mismatch_rows": mismatch_rows,
        "passed": missing_rows == 0 and mismatch_rows == 0,
    }


def _validate_published_ngs_values(
    con: duckdb.DuckDBPyConnection,
    target_path: str,
    published_path: str,
    ngs_cols: list[str],
) -> dict[str, int | bool]:
    """Require season materialization to preserve the published NGS values exactly."""
    expected = ", ".join(["NFL_player_id", "CAST(year AS INTEGER) AS yy", *ngs_cols])
    actual = ", ".join(["NFL_player_id", "CAST(year AS INTEGER) AS yy", *ngs_cols])
    mismatch = " OR ".join(
        f"NOT (a.{column} IS NOT DISTINCT FROM e.{column})" for column in ngs_cols
    )
    row = con.execute(
        f"""
        WITH expected AS (
            SELECT {expected}
            FROM read_parquet('{published_path}')
        ), actual AS (
            SELECT {actual}
            FROM read_parquet('{target_path}')
        )
        SELECT
            COUNT(*) AS expected_rows,
            COUNT(a.NFL_player_id) AS matched_rows,
            COUNT(*) FILTER (WHERE a.NFL_player_id IS NULL) AS missing_rows,
            COUNT(*) FILTER (
                WHERE a.NFL_player_id IS NOT NULL AND ({mismatch})
            ) AS mismatch_rows
        FROM expected e
        LEFT JOIN actual a
          ON a.NFL_player_id = e.NFL_player_id
         AND a.yy = e.yy
        """
    ).fetchone()
    expected_rows, matched_rows, missing_rows, mismatch_rows = (int(value) for value in row)
    return {
        "expected_rows": expected_rows,
        "matched_rows": matched_rows,
        "missing_rows": missing_rows,
        "mismatch_rows": mismatch_rows,
        "passed": missing_rows == 0 and mismatch_rows == 0,
    }


def _process(con, fname, key, filt, add_ngs, ngs_mode, apply, validate, ngs_cols) -> dict:
    path = SC_DIR / fname
    v26 = Path(latest_v26()).as_posix()
    tcols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')").fetchall()]
    want = SUMABLE + RATE_ADDS + CTX_ADDS + (ngs_cols if add_ngs else [])
    existing = [c for c in want if c in tcols]

    grp = ", ".join(key)
    yr_key = "year" in key
    # weekly SUM aggregate at this table's grain (+ rate/context components, _c-suffixed
    # so they never collide with canonical season columns)
    sums = ", ".join(f"SUM({c}) AS {c}" for c in SUMABLE)
    comps = ("SUM(CAST(is_win AS INT)) AS wins_c, "
             "SUM(CAST(is_starter AS INT)) AS gs_c, "
             "SUM(passing_yards) AS pyds_c, SUM(passing_tds) AS ptds_c, "
             "SUM(passing_interceptions) AS pint_c, SUM(sack_yards_lost) AS syds_c, "
             "SUM(attempts) AS att_c, SUM(sacks_suffered) AS sk_c")
    agg_key = "NFL_player_id" + (", CAST(year AS INTEGER) AS year" if yr_key else "")
    con.execute("DROP TABLE IF EXISTS agg")
    con.execute(
        f"CREATE TEMP TABLE agg AS SELECT {agg_key}, {sums}, {comps} "
        f"FROM read_parquet('{v26}') WHERE {filt} AND NFL_player_id IS NOT NULL GROUP BY "
        + ("NFL_player_id, CAST(year AS INTEGER)" if yr_key else "NFL_player_id")
    )
    join_on = " AND ".join(
        (f"t.NFL_player_id = a.NFL_player_id" if k == "NFL_player_id" else f"CAST(t.year AS INTEGER) = a.year") for k in key
    )
    exclude = list(dict.fromkeys(existing))
    star = "t.*" if not exclude else f"t.* EXCLUDE ({', '.join(exclude)})"
    sum_sel = ",\n            ".join(f"a.{c}" for c in SUMABLE)
    # rates = ratio of summed components (never averaged weekly ratios); context counts
    rate_sel = """
            a.scrimmage_yards * 1.0 / NULLIF(a.touches, 0) AS yards_per_touch,
            (a.pyds_c + 20 * a.ptds_c - 45 * a.pint_c) * 1.0
                / NULLIF(a.att_c, 0) AS adjusted_yards_per_attempt,
            (a.pyds_c - a.syds_c) * 1.0
                / NULLIF(a.att_c + a.sk_c, 0) AS net_yards_per_attempt,
            (a.pyds_c - a.syds_c + 20 * a.ptds_c - 45 * a.pint_c) * 1.0
                / NULLIF(a.att_c + a.sk_c, 0) AS adjusted_net_yards_per_attempt,
            a.wins_c AS wins,
            a.gs_c AS games_started"""
    select = f"""
        SELECT {star},
            {sum_sel},{rate_sel}"""
    from_join = f"FROM read_parquet('{path.as_posix()}') t LEFT JOIN agg a ON {join_on}"
    if add_ngs and ngs_mode == "published":
        ngs_sel = ",\n            ".join(f"g.{c}" for c in ngs_cols)
        select += f",\n            {ngs_sel}"
        from_join += (f" LEFT JOIN read_parquet('{NGS_SEASON.as_posix()}') g "
                      f"ON t.NFL_player_id = g.NFL_player_id AND CAST(t.year AS INTEGER) = g.year")
    elif add_ngs and ngs_mode in ("career", "career_all"):
        season_name = "player_nfl_season_all.parquet" if ngs_mode == "career_all" else "player_nfl_season.parquet"
        _create_career_ngs_aggregate(con, SC_DIR / season_name, ngs_cols)
        ngs_sel = ",\n            ".join(f"g.{c}" for c in ngs_cols)
        select += f",\n            {ngs_sel}"
        from_join += " LEFT JOIN ngs_career_aggregate g ON t.NFL_player_id = g.NFL_player_id"
    sql = select + "\n        " + from_join

    before = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]
    if not apply and not validate:
        return {"table": fname, "rows": before, "adds": len(want), "existing": len(existing)}

    stamp = utc_stamp()
    tmp = path.with_name(path.stem + "_advagg.parquet")
    con.execute(f"COPY ({sql}) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    after = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}')").fetchone()[0]
    out_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{tmp.as_posix()}')").fetchall()}
    missing = [c for c in want if c not in out_cols]
    assert after == before, f"{fname}: row count changed {before}->{after}"
    assert not missing, f"{fname}: missing {missing}"
    ngs_validation = None
    if add_ngs and ngs_mode == "published":
        ngs_validation = _validate_published_ngs_values(
            con, tmp.as_posix(), NGS_SEASON.as_posix(), ngs_cols
        )
        if not ngs_validation["passed"]:
            return {
                "table": fname,
                "rows": after,
                "adds": len(want),
                "existing": len(existing),
                "ngs_validation": ngs_validation,
                "swapped": False,
            }
    elif add_ngs and ngs_mode in ("career", "career_all"):
        ngs_validation = _validate_career_ngs_values(
            con, tmp.as_posix(), ngs_cols
        )
        if not ngs_validation["passed"]:
            return {
                "table": fname,
                "rows": after,
                "adds": len(want),
                "existing": len(existing),
                "ngs_validation": ngs_validation,
                "swapped": False,
            }
    if validate:
        tmp.unlink(missing_ok=True)
        return {
            "table": fname,
            "rows": after,
            "adds": len(want),
            "existing": len(existing),
            "ngs_validation": ngs_validation,
            "validated": True,
            "swapped": False,
        }
    bk = path.with_name(path.stem + f"_preadvagg_{stamp}.parquet")
    shutil.copy2(path, bk)
    os.replace(tmp, path)
    return {
        "table": fname,
        "rows": after,
        "adds": len(want),
        "existing": len(existing),
        "ngs_validation": ngs_validation,
        "swapped": True,
    }


def run(apply: bool = False, validate: bool = False) -> list[dict]:
    if not NGS_SEASON.exists():
        raise FileNotFoundError(NGS_SEASON)
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='1500MB'")
    _tmp = Path("D:/league-history-data/nfl/tmp/duckdb"); _tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{_tmp.as_posix()}'")
    ngs_cols = _ngs_cols(con)
    bio_validation = _validate_ngs_bio_identity(con)
    if not bio_validation["passed"]:
        raise ValueError(f"NGS player identity coverage failed: {bio_validation}")
    results = [_process(con, *cfg, apply, validate, ngs_cols) for cfg in TABLES]
    for result in results:
        result["ngs_bio_validation"] = bio_validation
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--validate", action="store_true", help="materialize and gate temp outputs without swapping")
    a = ap.parse_args()
    if a.apply and a.validate:
        ap.error("--apply and --validate are mutually exclusive")
    for r in run(a.apply, a.validate):
        tag = "SWAPPED" if r.get("swapped") else ("VALIDATED" if a.validate else ("DRY" if not a.apply else "?"))
        print(f"  [{tag}] {r['table']:<32} rows={r['rows']:>7,} +{r['adds']} cols (existing {r['existing']})")
