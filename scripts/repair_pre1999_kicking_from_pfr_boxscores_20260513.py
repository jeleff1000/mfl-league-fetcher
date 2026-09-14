#!/usr/bin/env python3
"""Repair pre-1999 weekly kicking atoms from local PFR boxscores.

The 1978-1998 Stathead/PFR merge has a small but important corruption pattern:
made-FG distance buckets sometimes landed on the wrong kicker/week or remained
populated while `fg_made` was zero.  This script uses the local PFR boxscore
`kicking` table for official FGM/FGA/XPM/XPA counts and the PFR `scoring`
table for made-FG distances.

Default mode is an audit/manifest write.  `--apply` updates only high-confidence
rows:
  * matched PFR kicker rows whose scoring-table FG distance count equals FGM
  * live kicker rows with kicking atoms but no PFR kicking row in that team/game
    (their FG/PAT atoms are zeroed)

Derived `pts_*`/`fpts_*` columns are intentionally not touched here; run
`recompute_weekly_scoring_surface_20260513.py --recompute` after applying.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from scripts.recompute_weekly_scoring_surface_20260513 import (  # noqa: E402
    LongFlyWriter,
    load_env,
    q_ident,
    q_lit,
)


SUPER_TABLE = "___ops.nfl_historical.nfl_player_stats_all"
STAGE_TABLE = "___ops.public.pfr_kicking_repair_stage_20260513"
BACKUP_TABLE = "___ops.public.pfr_kicking_raw_backup_20260513"
SENTINEL_COL = "pfr_kicking_atoms_repaired_at_20260513"

PFR_ROOT_DEFAULT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\pfr_boxscores")

FG_DISTANCE_RE = re.compile(r"(\d+)\s+yard\s+field\s+goal", re.IGNORECASE)

UPDATE_COLUMNS = [
    "fg_made",
    "fgm",
    "fg_att",
    "fg_missed",
    "pat_made",
    "pat_att",
    "pat_missed",
    "fg_made_distance",
    "fg_yards",
    "fg_yards_canonical",
    "fg_yds_over_30",
    "fg_yards_over_30_canonical",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60_plus_canonical",
    "fg_long",
    "fg_pct",
    "pat_pct",
]

LIVE_SELECT_COLUMNS = [
    "player_week",
    "player",
    "NFL_player_id",
    "year",
    "week",
    "nfl_team",
    "opponent_nfl_team",
    "position",
    *UPDATE_COLUMNS,
]

ZERO_COLUMNS = [
    "fg_made",
    "fgm",
    "fg_att",
    "fg_missed",
    "pat_made",
    "pat_att",
    "pat_missed",
    "fg_made_distance",
    "fg_yards",
    "fg_yards_canonical",
    "fg_yds_over_30",
    "fg_yards_over_30_canonical",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60_plus_canonical",
    "fg_long",
]


def norm_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace(".", "").replace("'", "")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def canon_team(code: Any, year: int) -> str:
    value = "" if code is None else str(code).upper().strip()
    simple = {
        "GNB": "GB",
        "KAN": "KC",
        "NOR": "NO",
        "NWE": "NE",
        "SFO": "SF",
        "TAM": "TB",
        "JAC": "JAX",
        "SD": "SDG",
        "LAC": "SDG" if year <= 2016 else "LAC",
        "LV": "RAIDERS",
        "LVR": "RAIDERS",
        "OAK": "RAIDERS",
        "RAI": "RAIDERS",
    }
    value = simple.get(value, value)

    if value in {"BAL", "CLT", "IND"} and year <= 1983:
        return "IND_COLTS"
    if value in {"IND", "CLT"}:
        return "IND"
    if value == "BAL":
        return "BAL"

    if value in {"CRD", "PHO", "ARI"}:
        return "CARDINALS"
    if value == "STL" and year <= 1987:
        return "CARDINALS"
    if value == "STL" and year >= 1995:
        return "RAMS"
    if value in {"RAM", "LAR", "LA"}:
        return "RAMS"

    if value in {"OTI", "TEN"}:
        return "OILERS_TITANS"
    if value == "HOU" and year <= 1998:
        return "OILERS_TITANS"
    if value == "HOU":
        return "HOU"

    return value


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return 0.0
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def q_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return q_lit(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(float(value))


def split_first(value: Any) -> str:
    if value is None:
        return ""
    return str(value).split(";")[0].strip()


def bucket_counts(distances: list[int]) -> dict[str, float]:
    buckets = {
        "fg_made_0_19": 0.0,
        "fg_made_20_29": 0.0,
        "fg_made_30_39": 0.0,
        "fg_made_40_49": 0.0,
        "fg_made_50_59": 0.0,
        "fg_made_60_plus_canonical": 0.0,
    }
    for distance in distances:
        if distance <= 19:
            buckets["fg_made_0_19"] += 1
        elif distance <= 29:
            buckets["fg_made_20_29"] += 1
        elif distance <= 39:
            buckets["fg_made_30_39"] += 1
        elif distance <= 49:
            buckets["fg_made_40_49"] += 1
        elif distance <= 59:
            buckets["fg_made_50_59"] += 1
        else:
            buckets["fg_made_60_plus_canonical"] += 1
    return buckets


def pct(made: float, attempts: float) -> float | None:
    if attempts <= 0:
        return None
    return made / attempts


def pfr_payload(row: dict[str, Any], distances: list[int]) -> dict[str, Any]:
    fgm = to_float(row["fgm"])
    fga = to_float(row["fga"])
    xpm = to_float(row["xpm"])
    xpa = to_float(row["xpa"])
    total_distance = float(sum(distances))
    over_30 = float(sum(max(distance - 30, 0) for distance in distances))
    payload = {
        "fg_made": fgm,
        "fgm": fgm,
        "fg_att": fga,
        "fg_missed": max(fga - fgm, 0.0),
        "pat_made": xpm,
        "pat_att": xpa,
        "pat_missed": max(xpa - xpm, 0.0),
        "fg_made_distance": total_distance,
        "fg_yards": total_distance,
        "fg_yards_canonical": total_distance,
        "fg_yds_over_30": over_30,
        "fg_yards_over_30_canonical": over_30,
        "fg_long": float(max(distances) if distances else 0),
        "fg_pct": pct(fgm, fga),
        "pat_pct": pct(xpm, xpa),
    }
    payload.update(bucket_counts(distances))
    return payload


def zero_payload() -> dict[str, Any]:
    payload = {col: 0.0 for col in ZERO_COLUMNS}
    payload["fg_pct"] = None
    payload["pat_pct"] = None
    return payload


def has_live_kicking(row: dict[str, Any]) -> bool:
    return any(to_float(row.get(col)) != 0.0 for col in ZERO_COLUMNS)


def material_diff(live: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    changed = []
    for col, new_value in payload.items():
        old_value = live.get(col)
        if new_value is None:
            if old_value not in (None, ""):
                changed.append(col)
            continue
        if abs(to_float(old_value) - float(new_value)) > 1e-9:
            changed.append(col)
    return changed


def load_pfr_tables(pfr_root: Path, year_min: int, year_max: int) -> tuple[list[dict], list[dict], list[dict]]:
    team_games_path = pfr_root / "team_games_raw.parquet"
    kicking_path = pfr_root / "tables" / "kicking" / "_combined.parquet"
    scoring_path = pfr_root / "tables" / "scoring" / "_combined.parquet"
    for path in (team_games_path, kicking_path, scoring_path):
        if not path.exists():
            raise FileNotFoundError(path)

    con = duckdb.connect()
    team_games = con.execute(
        f"""
        SELECT season, TRY_CAST(week_num AS INTEGER) AS week, team_name_abbr,
               opp_name_abbr, boxscore_id, game_date
        FROM read_parquet('{team_games_path.as_posix()}')
        WHERE season BETWEEN ? AND ?
          AND TRY_CAST(week_num AS INTEGER) IS NOT NULL
        """,
        [year_min, year_max],
    ).fetchdf()
    kicking = con.execute(
        f"""
        SELECT boxscore_id, season, game_date, player, player_link_ids, team,
               xpm, xpa, fgm, fga
        FROM read_parquet('{kicking_path.as_posix()}')
        WHERE season BETWEEN ? AND ?
        """,
        [year_min, year_max],
    ).fetchdf()
    scoring = con.execute(
        f"""
        SELECT boxscore_id, season, team, description,
               description_link_texts, description_link_ids
        FROM read_parquet('{scoring_path.as_posix()}')
        WHERE season BETWEEN ? AND ?
          AND LOWER(description) LIKE '%field goal%'
        """,
        [year_min, year_max],
    ).fetchdf()
    return (
        team_games.to_dict("records"),
        kicking.to_dict("records"),
        scoring.to_dict("records"),
    )


def fetch_live_rows(
    writer: LongFlyWriter, existing_cols: set[str], year_min: int, year_max: int
) -> list[dict[str, Any]]:
    cols = [col for col in LIVE_SELECT_COLUMNS if col in existing_cols]
    select_list = ", ".join(q_ident(col) for col in cols)
    sql = f"""
        SELECT {select_list}
        FROM {SUPER_TABLE}
        WHERE year BETWEEN {year_min} AND {year_max}
          AND (
            UPPER(TRIM(COALESCE(position, nfl_position, ''))) = 'K'
            OR COALESCE(fg_made, 0) != 0
            OR COALESCE(fg_att, 0) != 0
            OR COALESCE(pat_made, 0) != 0
            OR COALESCE(pat_att, 0) != 0
          )
    """
    return writer.execute(sql, database="___ops")


def column_set(writer: LongFlyWriter) -> set[str]:
    rows = writer.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        """,
        database="___ops",
    )
    return {row["column_name"] for row in rows}


def build_manifests(
    writer: LongFlyWriter,
    pfr_root: Path,
    year_min: int,
    year_max: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    existing_cols = column_set(writer)
    team_games, kicking_rows, scoring_rows = load_pfr_tables(pfr_root, year_min, year_max)
    live_rows = fetch_live_rows(writer, existing_cols, year_min, year_max)

    team_week_to_box: dict[tuple[int, int, str], str] = {}
    for row in team_games:
        year = int(row["season"])
        week = int(row["week"])
        team = canon_team(row["team_name_abbr"], year)
        key = (year, week, team)
        previous = team_week_to_box.get(key)
        if previous and previous != row["boxscore_id"]:
            raise RuntimeError(f"Ambiguous team/week map for {key}: {previous} vs {row['boxscore_id']}")
        team_week_to_box[key] = row["boxscore_id"]

    fg_distances: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in scoring_rows:
        description = "" if row["description"] is None else str(row["description"])
        match = FG_DISTANCE_RE.search(description)
        if not match:
            continue
        kicker_name = norm_name(split_first(row["description_link_texts"]))
        if not kicker_name:
            kicker_name = norm_name(description.split(match.group(0), 1)[0])
        if not kicker_name:
            continue
        fg_distances[(row["boxscore_id"], kicker_name)].append(int(match.group(1)))

    pfr_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    pfr_team_keys: set[tuple[str, str]] = set()
    pfr_team_any_keys: set[tuple[str, str]] = set()
    pfr_kicker_keys: set[tuple[str, str, str]] = set()
    for row in kicking_rows:
        year = int(row["season"])
        fgm = to_float(row["fgm"])
        fga = to_float(row["fga"])
        xpm = to_float(row["xpm"])
        xpa = to_float(row["xpa"])
        pfr_team_any_keys.add((row["boxscore_id"], canon_team(row["team"], year)))
        if fgm == 0 and fga == 0 and xpm == 0 and xpa == 0:
            continue
        key = (row["boxscore_id"], canon_team(row["team"], year), norm_name(row["player"]))
        pfr_by_key[key] = row
        pfr_team_keys.add((row["boxscore_id"], canon_team(row["team"], year)))
        pfr_kicker_keys.add(key)

    live_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    live_rows_with_box = []
    for row in live_rows:
        year = int(to_float(row["year"]))
        week = int(to_float(row["week"]))
        team = canon_team(row.get("nfl_team"), year)
        boxscore_id = team_week_to_box.get((year, week, team))
        enriched = dict(row)
        enriched["year"] = year
        enriched["week"] = week
        enriched["canon_team"] = team
        enriched["boxscore_id"] = boxscore_id or ""
        live_rows_with_box.append(enriched)
        if boxscore_id:
            live_by_key[(boxscore_id, team, norm_name(row.get("player")))] = enriched

    repair_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for live in live_rows_with_box:
        boxscore_id = live["boxscore_id"]
        key = (boxscore_id, live["canon_team"], norm_name(live.get("player")))
        if not boxscore_id:
            if has_live_kicking(live):
                review_rows.append({**live, "issue": "no_boxscore_mapping"})
            continue
        pfr = pfr_by_key.get(key)
        if pfr is None:
            if has_live_kicking(live):
                if (boxscore_id, live["canon_team"]) in pfr_team_any_keys:
                    payload = zero_payload()
                    changed = material_diff(live, payload)
                    if changed:
                        repair_rows.append(
                            {
                                **{col: payload.get(col) for col in UPDATE_COLUMNS},
                                "player_week": live["player_week"],
                                "player": live["player"],
                                "NFL_player_id": live.get("NFL_player_id", ""),
                                "year": live["year"],
                                "week": live["week"],
                                "nfl_team": live.get("nfl_team", ""),
                                "opponent_nfl_team": live.get("opponent_nfl_team", ""),
                                "boxscore_id": boxscore_id,
                                "repair_type": "zero_absent_from_pfr",
                                "changed_columns": ";".join(changed),
                                "pfr_player": "",
                                "pfr_team": "",
                                "fg_distances": "",
                            }
                        )
                else:
                    review_rows.append({**live, "issue": "no_pfr_team_kicking_rows"})
            continue

        distances = fg_distances.get((boxscore_id, norm_name(pfr["player"])), [])
        fgm = int(to_float(pfr["fgm"]))
        if len(distances) != fgm:
            review_rows.append(
                {
                    **live,
                    "issue": "distance_count_mismatch",
                    "pfr_player": pfr["player"],
                    "pfr_team": pfr["team"],
                    "pfr_fgm": fgm,
                    "distance_count": len(distances),
                    "fg_distances": ";".join(str(d) for d in distances),
                }
            )
            continue

        payload = pfr_payload(pfr, distances)
        changed = material_diff(live, payload)
        if not changed:
            continue
        repair_rows.append(
            {
                **{col: payload.get(col) for col in UPDATE_COLUMNS},
                "player_week": live["player_week"],
                "player": live["player"],
                "NFL_player_id": live.get("NFL_player_id", ""),
                "year": live["year"],
                "week": live["week"],
                "nfl_team": live.get("nfl_team", ""),
                "opponent_nfl_team": live.get("opponent_nfl_team", ""),
                "boxscore_id": boxscore_id,
                "repair_type": "pfr_exact",
                "changed_columns": ";".join(changed),
                "pfr_player": pfr["player"],
                "pfr_team": pfr["team"],
                "fg_distances": ";".join(str(d) for d in distances),
            }
        )

    missing_pfr_rows = []
    for key, pfr in pfr_by_key.items():
        if key not in live_by_key:
            year = int(pfr["season"])
            # Ignore non-K/P emergency rows only when every scoring count is zero;
            # those were already excluded from pfr_by_key.
            missing_pfr_rows.append(
                {
                    "boxscore_id": pfr["boxscore_id"],
                    "year": year,
                    "game_date": pfr["game_date"],
                    "pfr_team": pfr["team"],
                    "canon_team": canon_team(pfr["team"], year),
                    "pfr_player": pfr["player"],
                    "player_link_ids": pfr.get("player_link_ids", ""),
                    "xpm": to_float(pfr["xpm"]),
                    "xpa": to_float(pfr["xpa"]),
                    "fgm": to_float(pfr["fgm"]),
                    "fga": to_float(pfr["fga"]),
                    "fg_distances": ";".join(
                        str(d) for d in fg_distances.get((pfr["boxscore_id"], norm_name(pfr["player"])), [])
                    ),
                }
            )

    return repair_rows, review_rows, missing_pfr_rows, existing_cols


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def ensure_sentinel(writer: LongFlyWriter, existing_cols: set[str]) -> None:
    if SENTINEL_COL not in existing_cols:
        writer.execute(f"ALTER TABLE {SUPER_TABLE} ADD COLUMN {q_ident(SENTINEL_COL)} TIMESTAMP", database="___ops")
        existing_cols.add(SENTINEL_COL)


def apply_repairs(writer: LongFlyWriter, rows: list[dict[str, Any]], existing_cols: set[str]) -> None:
    if not rows:
        print("No repair rows to apply.")
        return
    ensure_sentinel(writer, existing_cols)

    writer.execute(f"DROP TABLE IF EXISTS {STAGE_TABLE}", database="___ops")
    stage_cols = ["player_week", *[col for col in UPDATE_COLUMNS if col in existing_cols], "repair_type"]
    col_defs = ["player_week VARCHAR"]
    for col in stage_cols[1:-1]:
        col_defs.append(f"{q_ident(col)} DOUBLE")
    col_defs.append("repair_type VARCHAR")
    writer.execute(f"CREATE TABLE {STAGE_TABLE} ({', '.join(col_defs)})", database="___ops")

    chunk_size = 250
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        values = []
        for row in chunk:
            values.append("(" + ", ".join(q_value(row.get(col)) for col in stage_cols) + ")")
        writer.execute(
            f"INSERT INTO {STAGE_TABLE} ({', '.join(q_ident(col) for col in stage_cols)}) VALUES {', '.join(values)}",
            database="___ops",
        )
        print(f"Inserted stage rows {start + 1}-{start + len(chunk)}")

    writer.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {BACKUP_TABLE} AS
        SELECT t.*
        FROM {SUPER_TABLE} AS t
        INNER JOIN {STAGE_TABLE} AS s
          ON t.player_week = s.player_week
        """,
        database="___ops",
    )

    set_parts = [f"{q_ident(col)} = s.{q_ident(col)}" for col in stage_cols[1:-1]]
    set_parts.append(f"{q_ident(SENTINEL_COL)} = CURRENT_TIMESTAMP")
    writer.execute(
        f"""
        UPDATE {SUPER_TABLE} AS t
        SET {", ".join(set_parts)}
        FROM {STAGE_TABLE} AS s
        WHERE t.player_week = s.player_week
        """,
        database="___ops",
    )
    print(f"Applied {len(rows)} raw kicking repair rows.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pfr-root", type=Path, default=PFR_ROOT_DEFAULT)
    parser.add_argument("--year-min", type=int, default=1978)
    parser.add_argument("--year-max", type=int, default=1998)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "tmp")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env()
    writer = LongFlyWriter()
    started = time.time()
    repair_rows, review_rows, missing_pfr_rows, existing_cols = build_manifests(
        writer,
        args.pfr_root,
        args.year_min,
        args.year_max,
    )
    stem = f"pfr_kicking_repair_{args.year_min}_{args.year_max}_20260513"
    repair_path = args.out_dir / f"{stem}_candidates.csv"
    review_path = args.out_dir / f"{stem}_review.csv"
    missing_path = args.out_dir / f"{stem}_missing_pfr_rows.csv"
    write_csv(repair_path, repair_rows)
    write_csv(review_path, review_rows)
    write_csv(missing_path, missing_pfr_rows)

    by_type: dict[str, int] = defaultdict(int)
    for row in repair_rows:
        by_type[str(row["repair_type"])] += 1
    print(f"candidate_repairs={len(repair_rows)} by_type={dict(sorted(by_type.items()))}")
    print(f"review_rows={len(review_rows)}")
    print(f"missing_pfr_rows={len(missing_pfr_rows)}")
    print(f"wrote {repair_path}")
    print(f"wrote {review_path}")
    print(f"wrote {missing_path}")

    if args.apply:
        apply_repairs(writer, repair_rows, existing_cols)
    else:
        print("Dry run only. Re-run with --apply after reviewing the manifests.")
    print(f"elapsed_seconds={time.time() - started:.1f}")


if __name__ == "__main__":
    main()
