"""Measure NFL.com player-season REG mappings at the player-season grain.

This is a measurement lane only.  Counts use season sums, long fields use a MAX,
and passer rating is recomputed from its operands.  The NFL.com tackles ``solo``
cell is deliberately witnessed as ``comb - asst``: the raw cell is retained as a
residual, while the source's internally reconstructible value is tested against
the v26 solo canonical.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .nflcom_player_logs_weekly_witness import _close_to_published, _passer_rating
from .sources import registry, v26_plane


LEDGER = Path(__file__).with_name("witness_gate") / "contracts" / "column_dispositions.v1.json"
OUT = (
    Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
    / "nflcom_player_season_regular_witness_2025.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_season"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


# These are the season fields whose first-pass natural mapping is not a clean
# value witness.  The alternatives are restricted to existing v26 columns with
# the same statistic family; this is a contrast set, not a license to choose a
# numerically flattering column.  The generated artifact retains the full
# denominator and both error directions for every candidate.
CANDIDATE_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = {
    ("fumbles", "fr"): (
        "fumble_recovery_opp", "fumble_recovery_own", "fumbles", "fumbles_lost",
        "rushing_fumbles", "receiving_fumbles", "sack_fumbles", "def_fumbles",
        "def_fumbles_forced",
    ),
    ("passing", "1st"): (
        "passing_first_downs", "receiving_first_downs", "rushing_first_downs",
        "completions", "attempts",
    ),
    ("punts", "punts"): ("punts", "punt_returns", "punt_yards", "punts_blocked"),
    ("receiving", "20"): (
        "rec_explosive_20", "receptions_20_29", "receptions_30_39",
        "receptions_40plus", "completions_40plus", "pass_explosive_20",
    ),
    ("receiving", "40"): (
        "receptions_40plus", "completions_40plus", "receiving_tds_40plus",
        "rushing_40plus",
    ),
    ("receiving", "rec_1st"): (
        "receiving_first_downs", "passing_first_downs", "rushing_first_downs",
        "receptions",
    ),
    ("receiving", "rec_fum"): (
        "receiving_fumbles", "rushing_fumbles", "sack_fumbles", "fumbles",
        "fumbles_lost", "def_fumbles",
    ),
    ("rushing", "40"): (
        "rushing_40plus", "receptions_40plus", "completions_40plus",
        "pass_explosive_20", "rec_explosive_20",
    ),
    ("rushing", "rush_1st"): (
        "rushing_first_downs", "passing_first_downs", "receiving_first_downs",
        "carries",
    ),
    ("rushing", "rush_fum"): (
        "rushing_fumbles", "receiving_fumbles", "sack_fumbles", "fumbles",
        "fumbles_lost", "def_fumbles",
    ),
    ("tackles", "asst"): (
        "def_tackle_assists", "def_tackles_solo", "def_tackles_combined",
        "def_tackles_with_assist",
    ),
}


def _q(path: str | Path) -> str:
    return str(path).replace("'", "''")


def mapped_regular_columns() -> list[tuple[str, str, str]]:
    raw = json.loads(LEDGER.read_text(encoding="utf-8"))
    decisions = raw["decisions"] if isinstance(raw, dict) else raw
    out: list[tuple[str, str, str]] = []
    for row in decisions:
        key = row.get("key", "")
        if (
            row.get("disposition") == "MAPPED_TO_CANONICAL"
            and key.startswith("nflcom_player_season|")
            and "|reg|" in key
        ):
            _, category, _, source_column = key.split("|", 3)
            canonical = row.get("canonical")
            if canonical:
                out.append((category, source_column, canonical))
    return sorted(set(out))


def witness_spec(category: str, source_column: str) -> tuple[str, str | None]:
    if (category, source_column) == ("tackles", "solo"):
        return "RECOMPUTE", "comb - asst"
    if (category, source_column) == ("passing", "rate"):
        return "RECOMPUTE", "passer_rating(completions, attempts, passing_yards, passing_tds, passing_interceptions)"
    return "MAX" if source_column == "lng" else "EXACT", None


def _candidate_measurement(
    con: duckdb.DuckDBPyConnection,
    category: str,
    source_column: str,
    canonical: str,
    v26: str,
) -> dict:
    """Compare one same-family candidate using the season witness denominator."""
    sql = f"""WITH s AS (
            SELECT _player_slug, SUM(TRY_CAST(\"{source_column}\" AS DOUBLE)) source_value
            FROM src
            WHERE _category='{category.replace("'", "''")}'
              AND TRY_CAST(\"{source_column}\" AS DOUBLE) IS NOT NULL
            GROUP BY 1
          ), v AS (
            SELECT i.nflcom_slug, MAX(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) target_value
            FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
            WHERE v.year=2025
              AND v.\"{canonical}\" IS NOT NULL
            GROUP BY 1
          )
          SELECT COUNT(*) AS paired_n,
                 SUM(CASE WHEN s.source_value IS NOT DISTINCT FROM v.target_value
                          THEN 1 ELSE 0 END) AS agree_n,
                 SUM(CASE WHEN s.source_value > v.target_value THEN 1 ELSE 0 END)
                    AS source_exceeds_target,
                 SUM(CASE WHEN s.source_value < v.target_value THEN 1 ELSE 0 END)
                    AS target_exceeds_source
          FROM s JOIN v ON v.nflcom_slug=s._player_slug"""
    paired, agree, source_hi, target_hi = con.execute(sql).fetchone()
    paired = int(paired or 0)
    agree = int(agree or 0)
    return {
        "canonical": canonical,
        "paired_n": paired,
        "agree_n": agree,
        "agree_pct": round(100.0 * agree / paired, 2) if paired else None,
        "source_exceeds_target": int(source_hi or 0),
        "target_exceeds_source": int(target_hi or 0),
    }


def measure_2025() -> dict:
    con = duckdb.connect()
    con.execute(
        f"""CREATE TEMP TABLE src AS
        SELECT * FROM read_parquet('{_q(SOURCE)}/**/*.parquet', union_by_name=true)
        WHERE TRY_CAST(season AS INTEGER) = 2025
          AND LOWER(CAST(season_type AS VARCHAR)) = 'reg'"""
    )
    con.execute(
        f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id = x.pfr_id"""
    )
    # This source is player-season grain. Use the season plane as the authoritative
    # regular-season witness; weekly aggregation is a separate cross-check, not the
    # desired subject for this lane.
    v26 = _q(v26_plane("season"))
    rows: list[dict] = []
    for category, source_column, canonical in mapped_regular_columns():
        witness, expression = witness_spec(category, source_column)
        # A rate has no safe season SUM.  Recompute it from the v26 operands.
        if (category, source_column) == ("passing", "rate"):
            sql = f"""WITH s AS (
                SELECT _player_slug, TRY_CAST(rate AS DOUBLE) source_value
                FROM src WHERE _category='passing' AND TRY_CAST(rate AS DOUBLE) IS NOT NULL
              ), v AS (
                SELECT i.nflcom_slug, SUM(completions) comp, SUM(attempts) att,
                       SUM(passing_yards) yds, SUM(passing_tds) td,
                       SUM(passing_interceptions) ints
                FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
                WHERE v.year=2025
                GROUP BY 1
              ) SELECT s.source_value, v.comp, v.att, v.yds, v.td, v.ints
              FROM s JOIN v ON v.nflcom_slug=s._player_slug"""
            values = con.execute(sql).fetchall()
            informative = agree = source_hi = target_hi = 0
            for raw, comp, att, yds, td, ints in values:
                if any(x is None for x in (raw, comp, att, yds, td, ints)) or not att:
                    continue
                expected = _passer_rating(comp, att, yds, td, ints)
                informative += 1
                agree += int(_close_to_published(str(raw), expected))
                source_hi += int(raw > expected)
                target_hi += int(raw < expected)
            rows.append({
                "category": category, "source_column": source_column,
                "canonical": canonical, "witness": witness,
                "source_expression": expression, "joined": len(values),
                "informative_n": informative, "agree_n": agree,
                "source_exceeds_expected": source_hi,
                "expected_exceeds_source": target_hi,
            })
            continue

        aggregate = "MAX" if witness == "MAX" else "SUM"
        if expression == "comb - asst":
            source_expr = "TRY_CAST(comb AS DOUBLE) - TRY_CAST(asst AS DOUBLE)"
            raw_expr = "TRY_CAST(solo AS DOUBLE)"
        else:
            source_expr = f'TRY_CAST("{source_column}" AS DOUBLE)'
            raw_expr = source_expr
        sql = f"""WITH s AS (
            SELECT _player_slug, {aggregate}({source_expr}) source_value,
                   {aggregate}({raw_expr}) raw_value
            FROM src WHERE _category='{category.replace("'", "''")}'
              AND {source_expr} IS NOT NULL GROUP BY 1
          ), v AS (
            SELECT i.nflcom_slug, MAX(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) target_value
            FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
            WHERE v.year=2025 AND v.\"{canonical}\" IS NOT NULL
            GROUP BY 1
          ) SELECT s.source_value, s.raw_value, v.target_value
          FROM s JOIN v ON v.nflcom_slug=s._player_slug"""
        values = con.execute(sql).fetchall()
        informative = agree = source_hi = target_hi = raw_agree = 0
        for source_value, raw_value, target_value in values:
            if source_value is None or target_value is None:
                continue
            informative += 1
            agree += int(source_value == target_value)
            source_hi += int(source_value > target_value)
            target_hi += int(source_value < target_value)
            raw_agree += int(raw_value == target_value) if raw_value is not None else 0
        rows.append({
            "category": category, "source_column": source_column,
            "canonical": canonical, "witness": witness,
            "source_expression": expression, "joined": len(values),
            "informative_n": informative, "agree_n": agree,
            "raw_agree_n": raw_agree if expression else None,
            "source_exceeds_target": source_hi,
            "target_exceeds_source": target_hi,
        })
    for row in rows:
        row["agree_pct"] = round(100.0 * row["agree_n"] / row["informative_n"], 2) if row["informative_n"] else None
        if row.get("raw_agree_n") is not None:
            row["raw_agree_pct"] = round(100.0 * row["raw_agree_n"] / row["informative_n"], 2) if row["informative_n"] else None
    v26 = _q(v26_plane("season"))
    candidate_matrix: dict[str, list[dict]] = {}
    for (category, source_column), candidates in CANDIDATE_COLUMNS.items():
        key = f"{category}.{source_column}"
        candidate_matrix[key] = [
            _candidate_measurement(con, category, source_column, candidate, v26)
            for candidate in candidates
        ]
    result = {
        "source": "nflcom_player_season", "season": 2025,
        "season_type": "REG", "subject_plane": "player_nfl_season",
        "source_rows": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),
        "mapped_fields": len(rows), "rows": rows,
        "candidate_matrix": candidate_matrix,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> int:
    result = measure_2025()
    measured = [r for r in result["rows"] if r["informative_n"]]
    print(f"measurement -> {OUT}")
    print("regular", result["source_rows"], result["mapped_fields"], min((r["agree_pct"] for r in measured), default=None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
