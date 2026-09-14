"""Build a Stathead play-by-play backfill plan from the local NFL schedule.

The plan intentionally groups games by unordered matchup-season. Stathead's
Play Finder returns one selected offense at a time, so each matchup-season gets
two scrape URLs: A offense vs B and B offense vs A. That avoids per-game
duplicate querying while still allowing full-game reconstruction.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.parse
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


DEFAULT_SCHEDULE = Path(r"C:\Users\joeye\OneDrive\Documents\Docs_DELETE\Pipeline_Data\nfl_sched.xlsx")
DEFAULT_NFLVERSE_PBP = Path("fantasy_football_data/cache/nflverse/nflverse_pbp_1999.parquet")
DEFAULT_OUT_DIR = Path("tmp/stathead_pbp_backfill_plan")

PLAY_FINDER_BASE = "https://www.sports-reference.com/stathead/football/play_finder.cgi"

XML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


STATHEAD_FRANCHISES = {
    "crd": "Ari (StL/Chi) Cardinals",
    "atl": "Atlanta Falcons",
    "rav": "Baltimore Ravens",
    "buf": "Buffalo Bills",
    "car": "Carolina Panthers",
    "chi": "Chicago Bears",
    "cin": "Cincinnati Bengals",
    "cle": "Cleveland Browns",
    "dal": "Dallas Cowboys",
    "den": "Denver Broncos",
    "det": "Detroit Lions",
    "gnb": "Green Bay Packers",
    "htx": "Houston Texans",
    "clt": "Indianapolis (Bal) Colts",
    "jax": "Jacksonville Jaguars",
    "kan": "Kansas City Chiefs",
    "rai": "Las Vegas/Oakland/LA Raiders",
    "sdg": "LA/ San Diego Chargers",
    "ram": "Los Angeles (SL/Cle) Rams",
    "mia": "Miami Dolphins",
    "min": "Minnesota Vikings",
    "nwe": "New England Patriots",
    "nor": "New Orleans Saints",
    "nyg": "New York Giants",
    "nyj": "New York Jets",
    "phi": "Philadelphia Eagles",
    "pit": "Pittsburgh Steelers",
    "sfo": "San Francisco 49ers",
    "sea": "Seattle Seahawks",
    "tam": "Tampa Bay Buccaneers",
    "oti": "Ten Titans/Hou Oilers",
    "was": "Washington Football Team",
}


TEAM_NAME_TO_STATHEAD = {
    "Arizona Cardinals": "crd",
    "Atlanta Falcons": "atl",
    "Baltimore Colts": "clt",
    "Baltimore Ravens": "rav",
    "Buffalo Bills": "buf",
    "Carolina Panthers": "car",
    "Chicago Bears": "chi",
    "Cincinnati Bengals": "cin",
    "Cleveland Browns": "cle",
    "Dallas Cowboys": "dal",
    "Denver Broncos": "den",
    "Detroit Lions": "det",
    "Green Bay Packers": "gnb",
    "Houston Oilers": "oti",
    "Houston Texans": "htx",
    "Indianapolis Colts": "clt",
    "Jacksonville Jaguars": "jax",
    "Kansas City Chiefs": "kan",
    "Las Vegas Raiders": "rai",
    "Los Angeles Chargers": "sdg",
    "Los Angeles Raiders": "rai",
    "Los Angeles Rams": "ram",
    "Miami Dolphins": "mia",
    "Minnesota Vikings": "min",
    "New England Patriots": "nwe",
    "New Orleans Saints": "nor",
    "New York Giants": "nyg",
    "New York Jets": "nyj",
    "Oakland Raiders": "rai",
    "Philadelphia Eagles": "phi",
    "Phoenix Cardinals": "crd",
    "Pittsburgh Steelers": "pit",
    "San Diego Chargers": "sdg",
    "San Francisco 49ers": "sfo",
    "Seattle Seahawks": "sea",
    "St. Louis Cardinals": "crd",
    "St. Louis Rams": "ram",
    "Tampa Bay Buccaneers": "tam",
    "Tennessee Oilers": "oti",
    "Tennessee Titans": "oti",
    "Washington Commanders": "was",
    "Washington Football Team": "was",
    "Washington Redskins": "was",
}


STATHEAD_TO_NFLVERSE_MODERN = {
    "crd": "ARI",
    "atl": "ATL",
    "rav": "BAL",
    "buf": "BUF",
    "car": "CAR",
    "chi": "CHI",
    "cin": "CIN",
    "cle": "CLE",
    "dal": "DAL",
    "den": "DEN",
    "det": "DET",
    "gnb": "GB",
    "htx": "HOU",
    "clt": "IND",
    "jax": "JAX",
    "kan": "KC",
    "rai": "LV",
    "sdg": "LAC",
    "ram": "LA",
    "mia": "MIA",
    "min": "MIN",
    "nwe": "NE",
    "nor": "NO",
    "nyg": "NYG",
    "nyj": "NYJ",
    "phi": "PHI",
    "pit": "PIT",
    "sfo": "SF",
    "sea": "SEA",
    "tam": "TB",
    "oti": "TEN",
    "was": "WAS",
}


POSTSEASON_ROUND_ORDER = {
    "WildCard": 1,
    "Division": 2,
    "ConfChamp": 3,
    "SuperBowl": 4,
}


STATHEAD_PLAY_COLUMNS = [
    ("Date", "game_date"),
    ("Tm", "team"),
    ("Opp", "opp"),
    ("Quarter", "quarter"),
    ("Time", "qtr_time_remain"),
    ("Down", "down"),
    ("ToGo", "distance"),
    ("Location", "location"),
    ("Score", "score"),
    ("Detail", "description"),
    ("Yds", "yards"),
    ("EPB", "exp_pts_before"),
    ("EPA", "exp_pts_after"),
    ("Diff", "exp_pts_diff"),
]


SCHEMA_BRIDGE = [
    ("Date", "game_date", "direct", "Stathead date filters returned rows back onto exact scheduled games."),
    (
        "Tm",
        "posteam",
        "lookup",
        "Map Stathead franchise label/id to team code; selected team is offensive/possessing side.",
    ),
    ("Opp", "defteam", "lookup", "Map opponent franchise label/id to team code."),
    ("Quarter", "qtr", "direct", "Integer quarter."),
    (
        "Time",
        "time",
        "partial_direct",
        "Stathead omits repeated timestamps on some rows; preserve raw and fill only with care.",
    ),
    ("Down", "down", "direct", "Blank for kickoffs, PATs, and some penalty rows."),
    ("ToGo", "ydstogo", "direct", "Blank/0 on non-scrimmage plays."),
    ("Location", "yrdln", "direct", "Raw location string, e.g. CRD 24."),
    (
        "Location",
        "side_of_field,yardline_100",
        "derive",
        "Derive from side abbreviation, yardline, and offense/opponent.",
    ),
    (
        "Score",
        "posteam_score,defteam_score",
        "derive",
        "Score is from selected team's perspective in the Stathead row.",
    ),
    ("Detail", "desc", "direct", "Use the full play description as the raw canonical text."),
    (
        "Detail",
        "play_type",
        "derive_parse",
        "Parse pass/rush/punt/kickoff/FG/XP/penalty/fumble/interception from text.",
    ),
    (
        "Detail",
        "player name/id columns",
        "derive_parse",
        "Parse names from text; preserve HTML links if scraper captures them.",
    ),
    (
        "Yds",
        "yards_gained/kick_distance/return_yards",
        "derive_context",
        "Meaning depends on play type; keep raw yards too.",
    ),
    ("EPB", "ep", "direct", "Expected points before the play from selected offense perspective."),
    ("EPA", "post_play_ep", "direct", "Expected points after the play from selected offense perspective."),
    ("Diff", "epa", "direct", "Stathead Diff is EPA for the selected offense."),
    (
        "schedule row",
        "game_id/week/season_type",
        "derive",
        "Attach from the schedule map after filtering rows by date and matchup.",
    ),
]


def col_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + ord(ch.upper()) - 64
    return idx - 1


def excel_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return (datetime(1899, 12, 30) + timedelta(days=float(value))).date()
    except ValueError:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None


def read_xlsx_rows(path: Path) -> list[dict[str, str]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings = []
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
        for si in root.findall("a:si", XML_NS):
            shared_strings.append("".join((t.text or "") for t in si.findall(".//a:t", XML_NS)))

        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        rows = []
        for row in sheet.findall(".//a:sheetData/a:row", XML_NS):
            values = [""] * 14
            for cell in row.findall("a:c", XML_NS):
                idx = col_index(cell.attrib.get("r", "A1"))
                if idx >= len(values):
                    values.extend([""] * (idx - len(values) + 1))
                node = cell.find("a:v", XML_NS)
                if node is None:
                    value = ""
                elif cell.attrib.get("t") == "s":
                    value = shared_strings[int(node.text or "0")]
                else:
                    value = node.text or ""
                values[idx] = value
            rows.append({"xlsx_row": row.attrib.get("r", ""), "values": values})
    return rows


def infer_season(game_date: date, week_label: str) -> int:
    # NFL schedules can put both postseason games and a few regular-season
    # finales in January. For this backfill, Jan/Feb belongs to the prior NFL
    # season regardless of whether the workbook week label is numeric.
    if game_date.month <= 2:
        return game_date.year - 1
    return game_date.year


def stathead_url(season: int, team_id: str, opp_id: str) -> str:
    params = {
        "request": "1",
        "order_by_asc": "1",
        "order_by": "game_date",
        "year_min": str(season),
        "year_max": str(season),
        "game_type": "E",
        "team_id": team_id,
        "opp_id": opp_id,
    }
    return PLAY_FINDER_BASE + "?" + urllib.parse.urlencode(params)


def safe_int(value: str) -> int | None:
    if value == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def build_games(schedule_path: Path, season_min: int, season_max: int) -> tuple[list[dict[str, Any]], list[str]]:
    rows = read_xlsx_rows(schedule_path)
    raw_games = []
    missing_names = set()

    for raw in rows[1:]:
        values = raw["values"]
        week_label = str(values[0]).strip()
        game_date = excel_date(values[2])
        winner = str(values[4]).strip()
        loser = str(values[6]).strip()
        if not game_date or not week_label or not winner or not loser:
            continue

        season = infer_season(game_date, week_label)
        if season < season_min or season > season_max:
            continue

        winner_id = TEAM_NAME_TO_STATHEAD.get(winner)
        loser_id = TEAM_NAME_TO_STATHEAD.get(loser)
        if not winner_id:
            missing_names.add(winner)
        if not loser_id:
            missing_names.add(loser)

        marker = str(values[5]).strip()
        if marker == "@":
            home_name, home_id = loser, loser_id
            away_name, away_id = winner, winner_id
            neutral_site = 0
        elif marker == "N":
            home_name, home_id = "", ""
            away_name, away_id = "", ""
            neutral_site = 1
        else:
            home_name, home_id = winner, winner_id
            away_name, away_id = loser, loser_id
            neutral_site = 0

        team_ids = sorted([x for x in [winner_id, loser_id] if x])
        pair_key = f"{season}_{team_ids[0]}_{team_ids[1]}" if len(team_ids) == 2 else ""
        raw_games.append(
            {
                "season": season,
                "week_label": week_label,
                "season_type": "REG" if week_label.isdigit() else "POST",
                "game_date": game_date.isoformat(),
                "day": values[1],
                "time": values[3],
                "location_marker": marker,
                "neutral_site": neutral_site,
                "winner_name": winner,
                "loser_name": loser,
                "winner_stathead_id": winner_id or "",
                "loser_stathead_id": loser_id or "",
                "winner_points": safe_int(values[8]),
                "loser_points": safe_int(values[9]),
                "home_team_name": home_name,
                "away_team_name": away_name,
                "home_stathead_id": home_id or "",
                "away_stathead_id": away_id or "",
                "team_a_stathead_id": team_ids[0] if len(team_ids) == 2 else "",
                "team_b_stathead_id": team_ids[1] if len(team_ids) == 2 else "",
                "team_a_label": STATHEAD_FRANCHISES.get(team_ids[0], "") if len(team_ids) == 2 else "",
                "team_b_label": STATHEAD_FRANCHISES.get(team_ids[1], "") if len(team_ids) == 2 else "",
                "unordered_pair_key": pair_key,
                "source_xlsx_row": raw["xlsx_row"],
            }
        )

    max_regular_week = defaultdict(int)
    for game in raw_games:
        if game["week_label"].isdigit():
            max_regular_week[game["season"]] = max(max_regular_week[game["season"]], int(game["week_label"]))

    for game in raw_games:
        if game["week_label"].isdigit():
            week_num = int(game["week_label"])
            phase_sort = week_num
        else:
            week_num = max_regular_week[game["season"]] + POSTSEASON_ROUND_ORDER.get(game["week_label"], 9)
            phase_sort = week_num
        game["week_nflverse_like"] = week_num
        game["phase_sort"] = phase_sort
        away_modern = STATHEAD_TO_NFLVERSE_MODERN.get(game["away_stathead_id"], "")
        home_modern = STATHEAD_TO_NFLVERSE_MODERN.get(game["home_stathead_id"], "")
        game["nflverse_game_id_candidate_modern_teams"] = (
            f'{game["season"]}_{week_num:02d}_{away_modern}_{home_modern}'
            if away_modern and home_modern and not game["neutral_site"]
            else ""
        )

    raw_games.sort(
        key=lambda g: (
            g["season"],
            g["phase_sort"],
            g["game_date"],
            g["time"],
            g["team_a_stathead_id"],
            g["team_b_stathead_id"],
        )
    )
    return raw_games, sorted(missing_names)


def build_pair_rows(games: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped = defaultdict(list)
    for game in games:
        if game["unordered_pair_key"]:
            grouped[game["unordered_pair_key"]].append(game)

    pair_rows = []
    query_rows = []
    for pair_key, group in sorted(grouped.items()):
        group.sort(key=lambda g: (g["phase_sort"], g["game_date"], g["time"]))
        season = group[0]["season"]
        team_a = group[0]["team_a_stathead_id"]
        team_b = group[0]["team_b_stathead_id"]
        dates = ";".join(g["game_date"] for g in group)
        weeks = ";".join(str(g["week_nflverse_like"]) for g in group)
        labels = ";".join(g["week_label"] for g in group)
        game_refs = ";".join(f'{g["game_date"]}:{g["week_label"]}' for g in group)
        contains_post = int(any(g["season_type"] == "POST" for g in group))
        team_a_url = stathead_url(season, team_a, team_b)
        team_b_url = stathead_url(season, team_b, team_a)
        pair_rows.append(
            {
                "query_group_key": pair_key,
                "season": season,
                "team_a_id": team_a,
                "team_a_label": STATHEAD_FRANCHISES.get(team_a, ""),
                "team_b_id": team_b,
                "team_b_label": STATHEAD_FRANCHISES.get(team_b, ""),
                "game_count": len(group),
                "game_dates": dates,
                "week_labels": labels,
                "nflverse_like_weeks": weeks,
                "contains_postseason": contains_post,
                "team_a_offense_url": team_a_url,
                "team_b_offense_url": team_b_url,
                "offense_queries_needed_for_full_pbp": 2,
                "game_refs": game_refs,
            }
        )
        for team_id, opp_id in [(team_a, team_b), (team_b, team_a)]:
            query_rows.append(
                {
                    "query_key": f"{season}_{team_id}_vs_{opp_id}",
                    "query_group_key": pair_key,
                    "season": season,
                    "team_id": team_id,
                    "opp_id": opp_id,
                    "offense_team_label": STATHEAD_FRANCHISES.get(team_id, ""),
                    "defense_team_label": STATHEAD_FRANCHISES.get(opp_id, ""),
                    "game_count": len(group),
                    "game_dates": dates,
                    "week_labels": labels,
                    "contains_postseason": contains_post,
                    "url": stathead_url(season, team_id, opp_id),
                }
            )

    return pair_rows, query_rows


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        seen = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    fieldnames.append(key)
                    seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_nflverse_schema(path: Path, parquet_path: Path) -> int:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return 0

    if not parquet_path.exists():
        return 0

    pf = pq.ParquetFile(parquet_path)
    rows = [{"ordinal": i, "column": field.name, "type": str(field.type)} for i, field in enumerate(pf.schema_arrow)]
    write_csv(path, rows, ["ordinal", "column", "type"])
    return len(rows)


def write_nflverse_1999_games(path: Path, parquet_path: Path) -> int:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return 0

    if not parquet_path.exists():
        return 0

    table = pq.read_table(
        parquet_path,
        columns=["game_id", "old_game_id", "home_team", "away_team", "season_type", "week", "game_date"],
    )
    df = table.to_pandas().drop_duplicates().sort_values(["game_date", "game_id"])
    rows = df.astype(str).to_dict("records")
    write_csv(path, rows, ["game_id", "old_game_id", "home_team", "away_team", "season_type", "week", "game_date"])
    return len(rows)


def write_static_maps(out_dir: Path) -> None:
    team_rows = []
    for name, stathead_id in sorted(TEAM_NAME_TO_STATHEAD.items()):
        team_rows.append(
            {
                "schedule_team_name": name,
                "stathead_id": stathead_id,
                "stathead_label": STATHEAD_FRANCHISES.get(stathead_id, ""),
                "nflverse_modern_team_code": STATHEAD_TO_NFLVERSE_MODERN.get(stathead_id, ""),
            }
        )
    write_csv(out_dir / "stathead_pbp_backfill_team_name_map.csv", team_rows)

    play_col_rows = [
        {"stathead_csv_header": header, "data_stat": data_stat} for header, data_stat in STATHEAD_PLAY_COLUMNS
    ]
    write_csv(out_dir / "stathead_play_finder_individual_play_columns.csv", play_col_rows)

    bridge_rows = [
        {
            "stathead_source": source,
            "nflverse_target": target,
            "mapping_status": status,
            "notes": notes,
        }
        for source, target, status, notes in SCHEMA_BRIDGE
    ]
    write_csv(out_dir / "stathead_to_nflverse_schema_bridge.csv", bridge_rows)


def write_summary(
    path: Path,
    games: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
    query_rows: list[dict[str, Any]],
    missing_names: list[str],
    schema_cols: int,
    nflverse_games: int,
) -> None:
    season_counts = Counter(g["season"] for g in games)
    max_games_per_pair = max((int(r["game_count"]) for r in pair_rows), default=0)
    multi_game_pairs = sum(1 for r in pair_rows if int(r["game_count"]) > 1)
    post_pairs = sum(1 for r in pair_rows if int(r["contains_postseason"]))
    lines = [
        "# Stathead PBP Backfill Plan",
        "",
        "This plan maps schedule games to Stathead Play Finder URLs without per-game duplicate pulls.",
        "",
        "Important behavior: one Play Finder URL returns one selected team's offensive/special-teams plays against the opponent. Full game reconstruction therefore needs two oriented URLs per unordered matchup-season.",
        "",
        "## Counts",
        "",
        f"- Seasons covered: {min(season_counts) if season_counts else ''}-{max(season_counts) if season_counts else ''}",
        f"- Schedule games: {len(games):,}",
        f"- Unique unordered matchup-season groups: {len(pair_rows):,}",
        f"- Oriented offense scrape URLs: {len(query_rows):,}",
        f"- Multi-game matchup-season groups: {multi_game_pairs:,}",
        f"- Groups containing postseason games: {post_pairs:,}",
        f"- Max games in one matchup-season group: {max_games_per_pair}",
        f"- 1999 nflverse PBP schema columns captured: {schema_cols:,}"
        if schema_cols
        else "- 1999 nflverse PBP schema columns captured: skipped",
        f"- 1999 nflverse unique games captured: {nflverse_games:,}"
        if nflverse_games
        else "- 1999 nflverse unique games captured: skipped",
        "",
        "## Files",
        "",
        "| file | purpose |",
        "|---|---|",
        "| `stathead_pbp_backfill_games_1978_1998.csv` | One row per schedule game with Stathead IDs and matchup group key. |",
        "| `stathead_pbp_backfill_query_pairs_1978_1998.csv` | One row per unordered matchup-season, with both oriented URLs. |",
        "| `stathead_pbp_backfill_offense_queries_1978_1998.csv` | One row per actual Stathead scrape URL. |",
        "| `stathead_pbp_backfill_team_name_map.csv` | Schedule team name to Stathead franchise ID. |",
        "| `stathead_play_finder_individual_play_columns.csv` | Current Individual Plays CSV columns. |",
        "| `stathead_to_nflverse_schema_bridge.csv` | First-pass source-to-target schema bridge. |",
        "| `nflverse_pbp_1999_schema_columns.csv` | Target PBP schema from the local 1999 parquet. |",
        "| `nflverse_pbp_1999_games.csv` | Unique 1999 games from the local nflverse parquet for validation. |",
        "",
        "## Notes",
        "",
        "- The URL cap should be safe with this grouping: the observed maximum in the plan is shown above, and each URL is one offense across that season's meetings.",
        "- The schedule workbook did not expose worksheet hyperlink relationship files, so this plan uses a local team-name alias map rather than embedded links.",
        "- Date plus unordered team pair is the exact-game join key after a matchup-season query returns multiple games.",
    ]
    if missing_names:
        lines.extend(["", "## Missing Team Name Mappings", ""])
        lines.extend(f"- {name}" for name in missing_names)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule-xlsx", type=Path, default=DEFAULT_SCHEDULE)
    parser.add_argument("--nflverse-pbp", type=Path, default=DEFAULT_NFLVERSE_PBP)
    parser.add_argument("--season-min", type=int, default=1978)
    parser.add_argument("--season-max", type=int, default=1998)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    games, missing_names = build_games(args.schedule_xlsx, args.season_min, args.season_max)
    pair_rows, query_rows = build_pair_rows(games)

    suffix = f"{args.season_min}_{args.season_max}"
    games_path = out_dir / f"stathead_pbp_backfill_games_{suffix}.csv"
    pairs_path = out_dir / f"stathead_pbp_backfill_query_pairs_{suffix}.csv"
    queries_path = out_dir / f"stathead_pbp_backfill_offense_queries_{suffix}.csv"
    write_csv(games_path, games)
    write_csv(pairs_path, pair_rows)
    write_csv(queries_path, query_rows)
    write_static_maps(out_dir)
    schema_cols = write_nflverse_schema(out_dir / "nflverse_pbp_1999_schema_columns.csv", args.nflverse_pbp)
    nflverse_games = write_nflverse_1999_games(out_dir / "nflverse_pbp_1999_games.csv", args.nflverse_pbp)
    write_summary(
        out_dir / "README.md",
        games,
        pair_rows,
        query_rows,
        missing_names,
        schema_cols,
        nflverse_games,
    )

    manifest = {
        "schedule_xlsx": str(args.schedule_xlsx),
        "nflverse_pbp": str(args.nflverse_pbp),
        "season_min": args.season_min,
        "season_max": args.season_max,
        "games": len(games),
        "query_pairs": len(pair_rows),
        "offense_queries": len(query_rows),
        "missing_team_names": missing_names,
        "files": {
            "games": str(games_path),
            "query_pairs": str(pairs_path),
            "offense_queries": str(queries_path),
            "summary": str(out_dir / "README.md"),
        },
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
