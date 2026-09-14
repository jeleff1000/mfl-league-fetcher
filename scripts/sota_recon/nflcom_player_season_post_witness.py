"""Measure whether the NFL.com player-season POST capture is a real witness.

The NFL.com player-season harvest can expose both ``reg`` and ``post`` labels. A
label is not evidence of a postseason value: if the two rows are identical, the
surface is a duplicated regular-season capture. The v26 season plane is regular
season-only, so this measurement uses the weekly POST aggregation only as a
phase-availability check; it cannot substitute for a true season-level POST subject.

This module is measurement-only.  It does not edit the disposition ledger, v26, or
MAPPING_LICENSES.  Its output is written beside the other SOTA measurements.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .nflcom_player_logs_weekly_witness import _close_to_published, _passer_rating
from .sources import latest_v26, registry


LEDGER = Path(__file__).with_name("witness_gate") / "contracts" / "column_dispositions.v1.json"
OUT = (
    Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
    / "nflcom_player_season_post_witness_2025.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_season"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(path: str | Path) -> str:
    return str(path).replace("'", "''")


def mapped_post_columns() -> list[tuple[str, str, str]]:
    raw = json.loads(LEDGER.read_text(encoding="utf-8"))
    decisions = raw["decisions"] if isinstance(raw, dict) else raw
    out: list[tuple[str, str, str]] = []
    for row in decisions:
        key = row.get("key", "")
        if (
            row.get("disposition") == "MAPPED_TO_CANONICAL"
            and key.startswith("nflcom_player_season|")
            and "|post|" in key
        ):
            _, category, _, source_column = key.split("|", 3)
            canonical = row.get("canonical")
            if canonical:
                out.append((category, source_column, canonical))
    return sorted(set(out))


def classify_capture(field_results: list[dict]) -> str:
    informative = [r for r in field_results if r["paired_n"]]
    if not informative:
        return "NO_PAIRED_ROWS"
    if all(r["same_n"] == r["paired_n"] for r in informative):
        return "DUPLICATES_REGULAR"
    return "NOT_IDENTICAL"


def classify_post_witness(capture_classification: str, target_fields: list[dict]) -> str:
    informative = [row for row in target_fields if row["informative_n"]]
    if capture_classification == "DUPLICATES_REGULAR":
        return "PROOF_PENDING_DUPLICATE_REGULAR_CAPTURE"
    if not informative:
        return "PROOF_PENDING_NO_POST_TARGET_DENOMINATOR"
    if all(row["agree_n"] == row["informative_n"] for row in informative):
        return "WITNESSABLE_POST"
    return "PROOF_PENDING_POST_VALUE_MISMATCH"


def _target_fields(
    con: duckdb.DuckDBPyConnection,
    *,
    source_phase: str = "post",
    target_phase: str = "POST",
) -> list[dict]:
    """Compare one captured phase to one subject phase at player-season grain.

    The harvest contains both REG and POST rows. The source phase must be filtered
    before aggregation; summing both captures doubles every count.
    """
    v26 = _q(latest_v26())
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id"""
    )
    fields = []
    for category, source_column, canonical in mapped_post_columns():
        if (category, source_column) == ("passing", "rate"):
            values = con.execute(
                f"""WITH s AS (
                    SELECT _player_slug, TRY_CAST(rate AS DOUBLE) source_value
                    FROM src WHERE _category='passing'
                      AND LOWER(CAST(season_type AS VARCHAR)) = '{source_phase}'
                      AND TRY_CAST(rate AS DOUBLE) IS NOT NULL
                  ), v AS (
                    SELECT i.nflcom_slug, SUM(completions) comp, SUM(attempts) att,
                           SUM(passing_yards) yds, SUM(passing_tds) td,
                           SUM(passing_interceptions) ints
                    FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
                    WHERE v.year=2025 AND v.season_type='{target_phase}'
                    GROUP BY 1
                  ) SELECT s.source_value, v.comp, v.att, v.yds, v.td, v.ints
                  FROM s JOIN v ON v.nflcom_slug=s._player_slug"""
            ).fetchall()
            informative = agree = source_hi = target_hi = 0
            for raw, comp, att, yds, td, ints in values:
                if any(value is None for value in (raw, comp, att, yds, td, ints)) or not att:
                    continue
                expected = _passer_rating(comp, att, yds, td, ints)
                if expected is None:
                    continue
                informative += 1
                agree += int(_close_to_published(str(raw), expected))
                source_hi += int(raw > expected)
                target_hi += int(raw < expected)
        else:
            aggregate = "MAX" if source_column == "lng" else "SUM"
            values = con.execute(
                f"""WITH s AS (
                    SELECT _player_slug, {aggregate}(TRY_CAST(\"{source_column}\" AS DOUBLE)) source_value
                    FROM src WHERE _category='{category.replace("'", "''")}'
                      AND LOWER(CAST(season_type AS VARCHAR)) = '{source_phase}'
                      AND TRY_CAST(\"{source_column}\" AS DOUBLE) IS NOT NULL
                    GROUP BY 1
                  ), v AS (
                    SELECT i.nflcom_slug, {aggregate}(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) target_value
                    FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
                    WHERE v.year=2025 AND v.season_type='{target_phase}'
                      AND v.\"{canonical}\" IS NOT NULL
                    GROUP BY 1
                  ) SELECT s.source_value, v.target_value
                  FROM s JOIN v ON v.nflcom_slug=s._player_slug"""
            ).fetchall()
            informative = sum(int(a is not None and b is not None) for a, b in values)
            agree = sum(int(a is not None and b is not None and a == b) for a, b in values)
            source_hi = sum(int(a is not None and b is not None and a > b) for a, b in values)
            target_hi = sum(int(a is not None and b is not None and a < b) for a, b in values)
        fields.append(
            {
                "category": category,
                "source_column": source_column,
                "canonical": canonical,
                "informative_n": informative,
                "agree_n": agree,
                "agree_pct": round(100.0 * agree / informative, 2) if informative else None,
                "source_exceeds_target": source_hi,
                "target_exceeds_source": target_hi,
            }
        )
    return fields


def measure_2025() -> dict:
    con = duckdb.connect()
    con.execute(
        f"""CREATE TEMP TABLE src AS
        SELECT *
        FROM read_parquet('{_q(SOURCE)}/**/*.parquet', union_by_name=true)
        WHERE TRY_CAST(season AS INTEGER) = 2025
          AND LOWER(CAST(season_type AS VARCHAR)) IN ('post', 'reg')"""
    )
    fields: list[dict] = []
    for category, source_column, _canonical in mapped_post_columns():
        q = f"""WITH x AS (
                    SELECT _player_slug,
                           LOWER(CAST(season_type AS VARCHAR)) AS season_type,
                           TRY_CAST(\"{source_column}\" AS VARCHAR) AS value
                    FROM src
                    WHERE _category = '{category.replace("'", "''")}'
                  ),
                  post AS (SELECT _player_slug, value FROM x WHERE season_type = 'post'),
                  reg AS (SELECT _player_slug, value FROM x WHERE season_type = 'reg')
                  SELECT COUNT(*) AS paired_n,
                         SUM(CASE WHEN post.value IS NOT DISTINCT FROM reg.value
                                  THEN 1 ELSE 0 END) AS same_n
                  FROM post JOIN reg USING (_player_slug)"""
        paired_n, same_n = con.execute(q).fetchone()
        fields.append(
            {
                "category": category,
                "source_column": source_column,
                "paired_n": int(paired_n or 0),
                "same_n": int(same_n or 0),
                "same_pct": round(100.0 * same_n / paired_n, 2) if paired_n else None,
            }
        )
    post_rows = con.execute("SELECT COUNT(*) FROM src WHERE LOWER(CAST(season_type AS VARCHAR))='post'").fetchone()[0]
    reg_rows = con.execute("SELECT COUNT(*) FROM src WHERE LOWER(CAST(season_type AS VARCHAR))='reg'").fetchone()[0]
    target_fields = _target_fields(con, source_phase="post", target_phase="POST")
    regular_target_fields = _target_fields(con, source_phase="post", target_phase="REG")
    result = {
        "source": "nflcom_player_season",
        "season": 2025,
        "subject_plane": "weekly_post_aggregation_for_phase_check",
        "post_rows": int(post_rows),
        "regular_rows": int(reg_rows),
        "mapped_post_fields": len(fields),
        "capture_classification": classify_capture(fields),
        "post_witness_classification": classify_post_witness(
            classify_capture(fields), target_fields
        ),
        "post_target_fields": target_fields,
        "post_source_against_regular_target_fields": regular_target_fields,
        "fields": fields,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> int:
    result = measure_2025()
    print(
        f"{result['source']} 2025: {result['capture_classification']} "
        f"({result['mapped_post_fields']} mapped post fields) -> {OUT}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
