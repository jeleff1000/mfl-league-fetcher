"""Normalize Stathead 1978-1998 PBP score fields to nflverse semantics.

The parser can shape Stathead play rows into the nflverse schema, but Stathead's
exported Score column is a running score that is post-play on scoring rows.
This pass uses the local schedule workbook for final scores and reconstructs
pre/post score columns in a way that matches the 1999+ nflverse PBP files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TWIN = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\stathead_generated\pbp_backfill_1978_1998\stathead_pbp_1978_1998_nflverse_twin.parquet"
)
DEFAULT_REFERENCE = REPO_ROOT / "fantasy_football_data" / "cache" / "nflverse" / "nflverse_pbp_1999.parquet"
DEFAULT_SCHEDULE = Path(r"C:\Users\joeye\OneDrive\Documents\Docs_DELETE\Pipeline_Data\nfl_sched.xlsx")
DEFAULT_AUDIT_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\_catalog\pbp_stathead_nflverse_twin_parse_20260507"
)


TEAM_NAME_TO_NFLVERSE = {
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Colts": "IND",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Houston Oilers": "TEN",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC",
    "Los Angeles Raiders": "LV",
    "Los Angeles Rams": "LA",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NE",
    "New Orleans Saints": "NO",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Oakland Raiders": "LV",
    "Philadelphia Eagles": "PHI",
    "Phoenix Cardinals": "ARI",
    "Pittsburgh Steelers": "PIT",
    "San Diego Chargers": "LAC",
    "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA",
    "St. Louis Cardinals": "ARI",
    "St. Louis Rams": "LA",
    "Tampa Bay Buccaneers": "TB",
    "Tennessee Oilers": "TEN",
    "Washington Redskins": "WAS",
}

SPREADSHEET_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def read_xlsx_first_sheet(path: Path) -> list[list[str]]:
    """Read cell values from the first sheet using only the xlsx XML package."""
    with ZipFile(path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", SPREADSHEET_NS):
                shared_strings.append(
                    "".join(
                        t.text or "" for t in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                    )
                )

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels.findall("rel:Relationship", REL_NS)}
        first_sheet = workbook.find("a:sheets", SPREADSHEET_NS)[0]
        rel_id = first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        target = rel_map[rel_id]
        sheet_path = f"xl/{target}" if not target.startswith("/") else target[1:]
        sheet = ET.fromstring(zf.read(sheet_path))

        def cell_value(cell: ET.Element) -> str:
            cell_type = cell.attrib.get("t")
            value = cell.find("a:v", SPREADSHEET_NS)
            if value is None:
                inline = cell.find("a:is", SPREADSHEET_NS)
                if inline is None:
                    return ""
                return "".join(
                    t.text or "" for t in inline.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                )
            raw = value.text or ""
            if cell_type == "s" and raw.isdigit():
                index = int(raw)
                return shared_strings[index] if index < len(shared_strings) else raw
            return raw

        def column_index(cell_ref: str) -> int:
            letters = "".join(ch for ch in cell_ref if ch.isalpha())
            index = 0
            for letter in letters:
                index = index * 26 + ord(letter.upper()) - 64
            return index - 1

        sparse_rows: list[dict[int, str]] = []
        max_cols = 0
        for row in sheet.findall(".//a:sheetData/a:row", SPREADSHEET_NS):
            sparse: dict[int, str] = {}
            row_max = -1
            for cell in row.findall("a:c", SPREADSHEET_NS):
                idx = column_index(cell.attrib["r"])
                sparse[idx] = cell_value(cell)
                row_max = max(row_max, idx)
            sparse_rows.append(sparse)
            max_cols = max(max_cols, row_max + 1)

    return [[row.get(idx, "") for idx in range(max_cols)] for row in sparse_rows]


def excel_serial_date(value: Any) -> str | None:
    try:
        serial = float(value)
    except (TypeError, ValueError):
        return None
    return (datetime(1899, 12, 30) + timedelta(days=serial)).date().isoformat()


def load_schedule_scores(schedule_path: Path, min_season: int, max_season: int) -> pd.DataFrame:
    rows = read_xlsx_first_sheet(schedule_path)
    if not rows:
        raise ValueError(f"Schedule workbook has no rows: {schedule_path}")

    records: list[dict[str, Any]] = []
    unknown_teams: set[str] = set()
    for row in rows[1:]:
        if len(row) < 10:
            continue
        date = excel_serial_date(row[2])
        if not date:
            continue
        year = int(date[:4])
        month = int(date[5:7])
        # NFL regular seasons occasionally run into January. The schedule file
        # stores calendar dates, while Stathead/nflverse use season years.
        season = year - 1 if month <= 2 else year
        if season < min_season or season > max_season:
            continue

        winner_name = row[4]
        loser_name = row[6]
        winner_code = TEAM_NAME_TO_NFLVERSE.get(winner_name)
        loser_code = TEAM_NAME_TO_NFLVERSE.get(loser_name)
        if not winner_code:
            unknown_teams.add(winner_name)
        if not loser_code:
            unknown_teams.add(loser_name)
        if not winner_code or not loser_code:
            continue

        try:
            winner_score = int(float(row[8]))
            loser_score = int(float(row[9]))
        except (TypeError, ValueError):
            continue

        records.append(
            {
                "season": season,
                "game_date": date,
                "winner_code": winner_code,
                "loser_code": loser_code,
                "winner_score": winner_score,
                "loser_score": loser_score,
                "week_label": row[0],
            }
        )

    if unknown_teams:
        raise ValueError(f"Unmapped schedule teams: {sorted(unknown_teams)}")
    if not records:
        raise ValueError(f"No schedule rows loaded from {schedule_path}")

    return pd.DataFrame(records)


def cast_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    arrays = []
    for field in schema:
        if field.name in table.column_names:
            column = table[field.name]
            if column.type != field.type:
                column = column.cast(field.type)
        else:
            column = pa.nulls(table.num_rows, type=field.type)
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=schema)


def normalize_scores(
    source_path: Path,
    reference_path: Path,
    schedule_path: Path,
    out_path: Path,
    audit_dir: Path,
) -> dict[str, Any]:
    reference_schema = pq.ParquetFile(reference_path).schema_arrow
    source_meta = pq.ParquetFile(source_path).metadata
    audit_dir.mkdir(parents=True, exist_ok=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    schedule = load_schedule_scores(schedule_path, 1978, 1998)
    con = duckdb.connect()
    con.register("schedule_scores", schedule)

    source_sql = f"parquet_scan('{str(source_path).replace(chr(39), chr(39) + chr(39))}')"
    select_exprs: list[str] = []
    replacements = {
        "play_id": "CAST(game_play_number AS DOUBLE)",
        "home_score": "CAST(final_home_score AS INTEGER)",
        "away_score": "CAST(final_away_score AS INTEGER)",
        "posteam_score": "CAST(pre_posteam_score AS DOUBLE)",
        "defteam_score": "CAST(pre_defteam_score AS DOUBLE)",
        "score_differential": "CAST(pre_posteam_score - pre_defteam_score AS DOUBLE)",
        "posteam_score_post": "CAST(post_posteam_score AS DOUBLE)",
        "defteam_score_post": "CAST(post_defteam_score AS DOUBLE)",
        "score_differential_post": "CAST(post_posteam_score - post_defteam_score AS DOUBLE)",
        "total_home_score": "CAST(post_home_score AS DOUBLE)",
        "total_away_score": "CAST(post_away_score AS DOUBLE)",
    }
    for field in reference_schema:
        expr = replacements.get(field.name, f"o.{qident(field.name)}")
        select_exprs.append(f"{expr} AS {qident(field.name)}")

    query = f"""
    WITH base AS (
        SELECT
            t.*,
            t.play_id AS original_play_id,
            TRY_CAST(regexp_extract(t.nfl_api_id, ':(\\d+)$', 1) AS INTEGER) AS source_row_index,
            CASE
                WHEN t.posteam = t.home_team THEN t.posteam_score
                ELSE t.defteam_score
            END AS observed_home_score,
            CASE
                WHEN t.posteam = t.home_team THEN t.defteam_score
                ELSE t.posteam_score
            END AS observed_away_score
        FROM {source_sql} t
    ),
    clocked AS (
        SELECT
            b.*,
            LAST_VALUE(b.quarter_seconds_remaining IGNORE NULLS) OVER (
                PARTITION BY b.game_id, b.posteam, b.qtr
                ORDER BY b.original_play_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS filled_qtr_seconds
        FROM base b
    ),
    joined AS (
        SELECT
            c.*,
            CASE
                WHEN c.home_team = s.winner_code THEN s.winner_score
                WHEN c.home_team = s.loser_code THEN s.loser_score
            END AS final_home_score,
            CASE
                WHEN c.away_team = s.winner_code THEN s.winner_score
                WHEN c.away_team = s.loser_code THEN s.loser_score
            END AS final_away_score
        FROM clocked c
        LEFT JOIN schedule_scores s
            ON c.season = s.season
           AND c.game_date = s.game_date
           AND (
                (c.home_team = s.winner_code AND c.away_team = s.loser_code)
                OR (c.home_team = s.loser_code AND c.away_team = s.winner_code)
           )
    ),
    ordered AS (
        SELECT
            j.*,
            ROW_NUMBER() OVER (
                PARTITION BY j.game_id
                ORDER BY
                    j.qtr ASC NULLS LAST,
                    j.filled_qtr_seconds DESC NULLS LAST,
                    j.original_play_id ASC
            ) AS game_play_number
        FROM joined j
    ),
    scored_pre AS (
        SELECT
            o.*,
            COALESCE(
                LAST_VALUE(o.observed_home_score IGNORE NULLS) OVER (
                    PARTITION BY o.game_id
                    ORDER BY o.game_play_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
            ) AS pre_home_score,
            COALESCE(
                LAST_VALUE(o.observed_away_score IGNORE NULLS) OVER (
                    PARTITION BY o.game_id
                    ORDER BY o.game_play_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
            ) AS pre_away_score,
        FROM ordered o
    ),
    scored AS (
        SELECT
            sp.*,
            COALESCE(sp.observed_home_score, sp.pre_home_score) AS post_home_score,
            COALESCE(sp.observed_away_score, sp.pre_away_score) AS post_away_score
        FROM scored_pre sp
    ),
    relative_scores AS (
        SELECT
            s.*,
            CASE WHEN s.posteam = s.home_team THEN s.pre_home_score ELSE s.pre_away_score END AS pre_posteam_score,
            CASE WHEN s.posteam = s.home_team THEN s.pre_away_score ELSE s.pre_home_score END AS pre_defteam_score,
            CASE WHEN s.posteam = s.home_team THEN s.post_home_score ELSE s.post_away_score END AS post_posteam_score,
            CASE WHEN s.posteam = s.home_team THEN s.post_away_score ELSE s.post_home_score END AS post_defteam_score
        FROM scored s
    )
    SELECT
        {", ".join(select_exprs)}
    FROM relative_scores o
    ORDER BY o.season, o.week, o.game_id, o.game_play_number
    """

    table = con.execute(query).to_arrow_table()
    casted = cast_table_to_schema(table, reference_schema)

    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    pq.write_table(casted, temp_path, compression="zstd")
    if out_path.resolve() == source_path.resolve():
        os.replace(temp_path, out_path)
    else:
        if out_path.exists():
            out_path.unlink()
        temp_path.replace(out_path)

    con = duckdb.connect()
    con.register("schedule_scores", schedule)
    out_sql = f"parquet_scan('{str(out_path).replace(chr(39), chr(39) + chr(39))}')"
    game_validation = (
        con.execute(
            f"""
        WITH games AS (
            SELECT DISTINCT game_id, season, week, game_date, home_team, away_team, home_score, away_score
            FROM {out_sql}
        ),
        expected AS (
            SELECT
                g.*,
                CASE
                    WHEN g.home_team = s.winner_code THEN s.winner_score
                    WHEN g.home_team = s.loser_code THEN s.loser_score
                END AS expected_home_score,
                CASE
                    WHEN g.away_team = s.winner_code THEN s.winner_score
                    WHEN g.away_team = s.loser_code THEN s.loser_score
                END AS expected_away_score
            FROM games g
            LEFT JOIN schedule_scores s
                ON g.season = s.season
               AND g.game_date = s.game_date
               AND (
                    (g.home_team = s.winner_code AND g.away_team = s.loser_code)
                    OR (g.home_team = s.loser_code AND g.away_team = s.winner_code)
               )
        )
        SELECT
            COUNT(*) AS games,
            SUM(CASE WHEN expected_home_score IS NOT NULL THEN 1 ELSE 0 END) AS schedule_matches,
            SUM(CASE WHEN home_score = expected_home_score AND away_score = expected_away_score THEN 1 ELSE 0 END)
                AS final_score_matches,
            SUM(CASE WHEN expected_home_score IS NULL THEN 1 ELSE 0 END) AS unmatched_schedule_games
        FROM expected
        """
        )
        .fetchdf()
        .iloc[0]
        .to_dict()
    )
    row_validation = (
        con.execute(
            f"""
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN posteam_score IS NOT NULL THEN 1 ELSE 0 END) AS pre_score_rows,
            SUM(CASE WHEN posteam_score_post IS NOT NULL THEN 1 ELSE 0 END) AS post_score_rows,
            SUM(CASE WHEN total_home_score IS NOT NULL THEN 1 ELSE 0 END) AS total_score_rows,
            SUM(CASE WHEN play_id IS NOT NULL THEN 1 ELSE 0 END) AS play_id_rows
        FROM {out_sql}
        """
        )
        .fetchdf()
        .iloc[0]
        .to_dict()
    )
    validation = {**game_validation, **row_validation}

    mismatch_df = con.execute(
        f"""
        WITH games AS (
            SELECT DISTINCT game_id, season, week, game_date, home_team, away_team, home_score, away_score
            FROM {out_sql}
        ),
        expected AS (
            SELECT
                g.*,
                CASE
                    WHEN g.home_team = s.winner_code THEN s.winner_score
                    WHEN g.home_team = s.loser_code THEN s.loser_score
                END AS expected_home_score,
                CASE
                    WHEN g.away_team = s.winner_code THEN s.winner_score
                    WHEN g.away_team = s.loser_code THEN s.loser_score
                END AS expected_away_score
            FROM games g
            LEFT JOIN schedule_scores s
                ON g.season = s.season
               AND g.game_date = s.game_date
               AND (
                    (g.home_team = s.winner_code AND g.away_team = s.loser_code)
                    OR (g.home_team = s.loser_code AND g.away_team = s.winner_code)
               )
        )
        SELECT *
        FROM expected
        WHERE expected_home_score IS NULL
           OR home_score != expected_home_score
           OR away_score != expected_away_score
        ORDER BY season, week, game_id
        """
    ).fetchdf()
    mismatch_path = audit_dir / "stathead_score_twin_final_score_mismatches.csv"
    mismatch_df.to_csv(mismatch_path, index=False)

    summary = {
        "source": str(source_path),
        "output": str(out_path),
        "reference_schema": str(reference_path),
        "schedule": str(schedule_path),
        "source_rows": source_meta.num_rows,
        "output_rows": pq.ParquetFile(out_path).metadata.num_rows,
        "schema_equal_reference": pq.ParquetFile(out_path).schema_arrow == reference_schema,
        "validation": {key: int(value) for key, value in validation.items()},
        "mismatch_csv": str(mismatch_path),
        "notes": [
            "home_score and away_score now store final game scores from nfl_sched.xlsx.",
            "posteam_score and defteam_score now store reconstructed pre-play scores.",
            "posteam_score_post, defteam_score_post, score_differential_post, total_home_score, and total_away_score are populated from reconstructed post-play scores.",
            "play_id is now a chronological per-game row number derived from quarter/clock chunks, closer to nflverse ordering semantics than the prior global Stathead scrape sequence.",
        ],
    }
    summary_path = audit_dir / "stathead_score_twin_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_TWIN)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--schedule", type=Path, default=DEFAULT_SCHEDULE)
    parser.add_argument("--out", type=Path, default=DEFAULT_TWIN)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    args = parser.parse_args()

    summary = normalize_scores(
        source_path=args.source,
        reference_path=args.reference,
        schedule_path=args.schedule,
        out_path=args.out,
        audit_dir=args.audit_dir,
    )
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
