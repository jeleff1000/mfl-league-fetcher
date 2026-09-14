"""
sota_recon/pfa_boxscore_reparse.py -- PHASE 0b: recover the ten-to-twelve stat tables
PFA publishes on every boxscore page and our capture threw away. NO CRAWL: every byte is
already on disk.

WHAT THE OLD CAPTURE KEPT. `profootballarchives.py::_lineup_rows()` regexes the LINEUPS
table and nothing else, so 5,359,619 participation rows exist and every stat table on the
same page -- RUSHING, PASSING, RECEIVING, INTERCEPTIONS, PUNTING, PUNT RETURNS, KICKOFFS,
KICKOFF RETURNS, SACKS, DEFENSE, Score By Quarters, Scoring Plays -- was parsed away.
Two of those land on queues that are open right now: INTERCEPTIONS is exactly what the
Law A hunt lane `pfr_or_pfa_player_defensive_interception_gamelog_capture` searched for
and closed as ABANDONED-no-local-source, and SACKS bears on the `sack_yards_lost` /
`fum_rec` burn-down class.

THE PREMISE, MEASURED RATHER THAN ASSUMED (2026-07-27). The handoff sized this lane as
"151,623 retained pages vs 16,996 parsed source_urls, a ~9x superset". The superset is
real but the reason is REDUNDANCY, not extra games:

  * 151,623 retained `.html.gz` includes 12,980 files in two hidden shard-17 recovery
    working directories, so the boxscore inventory is 138,643.
  * shards 3 and 7 -- 19,805 pages -- are not boxscores at all. They are bot-challenge
    interstitials ("One moment, please...", a 5-second JS reload), served HTTP 200 and
    retained as if they were captures. The capture's own status for them,
    "no usable roster rows", is indistinguishable from a genuinely empty page.
  * the surviving 13 shards each hold ~9,880 games and collectively only 16,996
    DISTINCT games: the shards overlapped ~5x rather than partitioning.

So the true statement is: 16,996 distinct games, each retained several times over, each
carrying 10-12 unparsed tables. This lane therefore parses ONE page per game, not one per
retained file -- 16,996 pages, not 151,623.

And the coverage news is good: 17,027 game keys were ever requested and 16,996 were
parsed, so the two poisoned shards cost 14 games each (all but 31 were covered by an
overlapping shard). That is worth stating plainly because it was NOT knowable from the
capture receipt, which counts retained bytes.

HEADER-DRIVEN, NEVER POSITIONAL. PFA puts the table's NAME in the header's first cell and
the column labels after it, each with a published `title` ("Attempts", "Long Gain"). Rows
are grouped by `<th>` separator rows naming the team. Teams are resolved against the two
teams the page's own Score By Quarters table names, so a section label ("Offense") can
never be mistaken for a team.

Output: D:/league-history-data/nfl/derived/reparsed_captures/pfa_boxscore_tables/
          rows-*.parquet          table-tagged stat rows
          COLUMN_DICTIONARY.json  (table_tag, column) -> published label + title + eras
        docs/pfa-boxscore-reparse.json  receipt + counters

Run:  python -m scripts.sota_recon.pfa_boxscore_reparse [--limit N] [--resume]
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

PFA_ROOT = Path("D:/league-history-data/nfl/ff_assets/profootballarchives/player_game_participation")
OUT_DIR = Path("D:/league-history-data/nfl/derived/reparsed_captures/pfa_boxscore_tables")
SUMMARY_PATH = Path(__file__).resolve().parents[2] / "docs" / "pfa-boxscore-reparse.json"

_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
_TABLE = re.compile(r"<table.*?</table>", re.S | re.I)
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELL = re.compile(r"<(th|td)([^>]*)>(.*?)</\1>", re.S | re.I)
_TITLE_ATTR = re.compile(r'title="([^"]*)"', re.I)
_PLAYER_HREF = re.compile(r'href="[^"]*?/players/[a-z]/([a-z0-9]+)\.html"', re.I)
_TEAM_HREF = re.compile(r'href="/(\d{4})([a-z]+)([a-z0-9]+)\.html"', re.I)

# The 13 table names MEASURED across a 2,100-page stratified sample spanning every decade
# from the 1920s to the 2020s (docs/pfa-boxscore-signature-census.json). A name outside
# this map is reported as UNRESOLVED and counted -- never guessed into a tag.
TABLE_TAGS = {
    "score by quarters": "score_by_quarters",
    "qtr": "scoring_plays",          # this table names itself in its THIRD column
    "lineups": "lineups",
    "rushing": "rushing",
    "passing": "passing",
    "receiving": "receiving",
    "interceptions": "interceptions",
    "punting": "punting",
    "punt returns": "punt_returns",
    "kickoffs": "kickoffs",
    "kickoff returns": "kickoff_returns",
    "sacks": "sacks",
    "defense": "defense",
}
# Tables whose rows are TEAMS or EVENTS rather than players.
NON_PLAYER_TABLES = {"score_by_quarters", "scoring_plays"}
# Already captured by the original parser; re-parsed for completeness but not the point.
ALREADY_CAPTURED = {"lineups"}

# Tables whose header row carries NO column labels -- just the table name across a
# colspan. LINEUPS is the only one measured: `<th colspan="3">LINEUPS</th>` over rows of
# (jersey, position, player). The naming is not invented here; it is the contract the
# registered `pfa_player_game_participation` capture already emits for the same table.
# An unlabelled table NOT listed here gets positional col_N keys and is counted, never
# silently squeezed into the header's width.
UNLABELLED_LAYOUTS = {"lineups": ["jersey_number", "position", "player"]}

# scoring_plays ends with one running-score column PER TEAM, headed with that game's team
# abbreviations (SD, JAC). Keying on those labels would mint a new column for every team
# code in NFL history; the position is what carries the meaning, and the labels are kept
# in the column dictionary.
SCORING_PLAYS_SCORE_COLUMNS = ["score_team_1", "score_team_2"]

RESERVED = {
    "source", "dataset", "season", "game_id", "table_tag", "team", "section", "player",
    "source_player_id", "is_team_row", "source_url", "content_sha256", "row_index",
}


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def text_of(fragment: str) -> str:
    return " ".join(re.sub(r"<[^>]+>", " ", fragment).split())


def unique_keys(labels: list[str]) -> list[str]:
    """Unique key per column. A blank or repeated header must never collapse two columns
    onto one dict key -- that silently drops a cell, which is the defect class this whole
    programme keeps paying for."""
    keys: list[str] = []
    seen: dict[str, int] = {}
    for index, label in enumerate(labels):
        base = label or f"col_{index}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        keys.append(base if count == 0 else f"{base}_{count + 1}")
    return keys


def _cells(row_html: str) -> list[tuple[str, str, str]]:
    return [(kind.lower(), attrs, body) for kind, attrs, body in _CELL.findall(row_html)]


_ANCHOR = re.compile(r"<a\b[^>]*>(.*?)</a>", re.S | re.I)


def link_texts(cell_html: str) -> list[str]:
    """The anchor texts inside a cell, in order.

    PFA renders a team twice for responsive layout -- `<span class="d-none
    d-md-inline">San Diego Chargers</span><span class="d-inline d-md-none">SD</span>` --
    so the cell's flattened text is "San Diego Chargers SD", which matches NEITHER the
    long nor the short name. Reading the anchors keeps both real labels instead of a
    concatenation that exists nowhere on the page.
    """
    return [text for text in (text_of(match) for match in _ANCHOR.findall(cell_html)) if text]


def parse_boxscore(html: str) -> tuple[list[dict], dict]:
    """Every table on the page, header-driven, with rows tagged by table and team.

    Returns (rows, diagnostics). Diagnostics count what we could NOT represent, so an
    unseen shape surfaces as a number instead of vanishing.
    """
    diagnostics = {
        "tables": 0,
        "unresolved_tables": [],
        "unlabelled_tables": [],
        "rows_width_mismatch": 0,
        "rows_emitted": 0,
        "teams_detected": 0,
        "team_labels_unresolved": 0,
        "challenge_page": False,
    }
    body = _SCRIPT.sub("", html)
    if "One moment, please" in html and "<table" not in body:
        diagnostics["challenge_page"] = True
        return [], diagnostics

    tables = _TABLE.findall(body)
    # The page's own Score By Quarters table names the two teams. Resolving group labels
    # against that set is what keeps a section heading ("Offense") from being read as a
    # team, without hard-coding either vocabulary.
    teams: set[str] = set()
    for table in tables:
        rows = _ROW.findall(table)
        if not rows:
            continue
        first = _cells(rows[0])
        if first and normalize(text_of(first[0][2])) == "score_by_quarters":
            for row in rows[1:]:
                cells = _cells(row)
                if cells and cells[0][0] == "td":
                    teams.update(link_texts(cells[0][2]))
    diagnostics["teams_detected"] = len(teams)

    output: list[dict] = []
    for table_index, table in enumerate(tables):
        rows = _ROW.findall(table)
        header_cells = None
        for row in rows:
            cells = _cells(row)
            if cells and {kind for kind, _, _ in cells} == {"th"}:
                header_cells = cells
                break
        if not header_cells:
            continue
        diagnostics["tables"] += 1
        name = normalize(text_of(header_cells[0][2]))
        tag = TABLE_TAGS.get(name.replace("_", " "))
        if tag is None:
            diagnostics["unresolved_tables"].append(name)
            continue
        labels = [normalize(text_of(cell[2])) for cell in header_cells]
        # The header's true WIDTH is the sum of its colspans, not its cell count. LINEUPS
        # is one `<th colspan="3">` over three-cell rows; taking the cell count gave a
        # width of 1 and kept the jersey number under a column called `player` while
        # discarding the position and the player link entirely.
        spans = []
        for cell in header_cells:
            match = re.search(r'colspan="?(\d+)', cell[1], re.I)
            spans.append(int(match.group(1)) if match else 1)
        width = sum(spans)
        if width > len(labels):
            layout = UNLABELLED_LAYOUTS.get(tag)
            keys = list(layout) if layout and len(layout) == width else [
                f"col_{index}" for index in range(width)
            ]
            if not layout:
                diagnostics["unlabelled_tables"].append(tag)
        else:
            keys = unique_keys(labels)
            if tag not in NON_PLAYER_TABLES:
                # PFA puts the table's NAME in the first header cell; the entity column
                # underneath it is the player.
                keys[0] = "player"
            elif tag == "score_by_quarters":
                keys[0] = "team_label"
            elif tag == "scoring_plays":
                for offset, name in enumerate(SCORING_PLAYS_SCORE_COLUMNS):
                    position = len(keys) - len(SCORING_PLAYS_SCORE_COLUMNS) + offset
                    if position >= 0:
                        keys[position] = name

        team: str | None = None
        section: str | None = None
        seen_header = False
        previous_was_header = False
        for row in rows:
            cells = _cells(row)
            if not cells:
                continue
            kinds = {kind for kind, _, _ in cells}
            if kinds == {"th"}:
                if not seen_header:
                    seen_header = True
                    previous_was_header = True
                    continue
                label = text_of(cells[0][2])
                if label:
                    # A label the page's own scoreboard names is a TEAM; anything else is
                    # a section heading inside the table. LINEUPS puts Offense / Defense /
                    # Special Teams beneath each team, and a positional rule ("a header
                    # row after data rows starts a new team") reads Defense as a team.
                    if label in teams:
                        team, section = label, None
                    elif teams:
                        section = label
                    else:
                        # No scoreboard to resolve against -- take the label as a team and
                        # COUNT it, so the guess is visible rather than assumed correct.
                        team, section = label, None
                        diagnostics["team_labels_unresolved"] += 1
                previous_was_header = True
                continue
            previous_was_header = False
            if len(cells) < width:
                diagnostics["rows_width_mismatch"] += 1
                continue
            record = {
                "table_tag": tag,
                "team": team,
                "section": section,
                "player": None,
                "source_player_id": None,
                "is_team_row": tag in NON_PLAYER_TABLES,
                "row_index": len(output),
            }
            for key, (_, _, cell_body) in zip(keys, cells[:width]):
                value = text_of(cell_body)
                target = f"stat_{key}" if key in RESERVED and key != "player" else key
                # A team cell is rendered twice for responsive layout, so its flattened
                # text is a concatenation of the long and short names. Keep the label the
                # page actually links.
                links = link_texts(cell_body)
                if links and (key in {"team_label", "team"} or target == "stat_team"):
                    value = links[0]
                record[target] = value or None
                if key == "player":
                    match = _PLAYER_HREF.search(cell_body)
                    if match:
                        record["source_player_id"] = match.group(1)
            if tag == "score_by_quarters" and record.get("team_label"):
                record["team"] = record["team_label"]
            elif tag == "scoring_plays" and record.get("stat_team"):
                record["team"] = record["stat_team"]
            output.append(record)
    diagnostics["rows_emitted"] = len(output)
    return output, diagnostics


def _one_page_per_game() -> dict[str, tuple[Path, str, int | None]]:
    """game_id -> (raw page, url, season), choosing a page the capture PARSED.

    The shards overlap ~5x and two of them hold only challenge pages, so picking any
    retained file per game would sample a bot interstitial one time in seven.
    """
    chosen: dict[str, tuple[Path, str, int | None]] = {}
    for shard in sorted(PFA_ROOT.glob("*/shards/shard-*")):
        ledger = shard / "REQUEST_LEDGER.jsonl"
        if not ledger.exists():
            continue
        for line in ledger.open(encoding="utf-8"):
            entry = json.loads(line)
            if entry.get("status") != "ok":
                continue
            key = entry["key"]
            if key in chosen:
                continue
            sha = entry.get("content_sha256")
            if not sha:
                continue
            page = shard / "raw" / f"{sha}.html.gz"
            if page.exists():
                season = None
                match = re.match(r"(\d{4})", str(key))
                if match:
                    season = int(match.group(1))
                chosen[key] = (page, entry["url"], season)
    return chosen


def run(limit: int | None = None) -> dict:
    games = _one_page_per_game()
    keys = sorted(games)
    if limit:
        keys = keys[:limit]
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    tag_rows: Counter = Counter()
    tag_players: dict[str, set] = defaultdict(set)
    tag_seasons: dict[str, set] = defaultdict(set)
    columns: dict[tuple, dict] = {}
    unresolved: Counter = Counter()
    width_mismatch = 0
    unresolved_team_labels = 0
    unlabelled_tables = 0
    challenge_pages = 0
    empty_pages = 0
    buffer: list[dict] = []
    written = 0
    part = 0

    def flush() -> None:
        nonlocal buffer, part, written
        if not buffer:
            return
        import duckdb
        import pandas as pd

        frame = pd.DataFrame(buffer)
        connection = duckdb.connect()
        connection.register("frame", frame)
        destination = (OUT_DIR / f"rows-{part:03d}.parquet").as_posix()
        connection.execute(f"COPY frame TO '{destination}' (FORMAT PARQUET)")
        connection.close()
        written += len(buffer)
        part += 1
        buffer = []

    for count, key in enumerate(keys):
        page, url, season = games[key]
        with gzip.open(page, "rt", errors="replace") as handle:
            html = handle.read()
        rows, diagnostics = parse_boxscore(html)
        if diagnostics["challenge_page"]:
            challenge_pages += 1
            continue
        if not rows:
            empty_pages += 1
        width_mismatch += diagnostics["rows_width_mismatch"]
        unresolved_team_labels += diagnostics["team_labels_unresolved"]
        if diagnostics["unlabelled_tables"]:
            unlabelled_tables += len(diagnostics["unlabelled_tables"])
        for name in diagnostics["unresolved_tables"]:
            unresolved[name] += 1
        for row in rows:
            row.update(
                source="profootballarchives",
                dataset="player_game_boxscore",
                season=season,
                game_id=key,
                source_url=url,
                content_sha256=page.stem.replace(".html", ""),
            )
            tag_rows[row["table_tag"]] += 1
            tag_seasons[row["table_tag"]].add(season)
            if row.get("source_player_id"):
                tag_players[row["table_tag"]].add(row["source_player_id"])
            for column in row:
                if column in RESERVED or row[column] is None:
                    continue
                entry = columns.setdefault(
                    (row["table_tag"], column),
                    {"table_tag": row["table_tag"], "column": column, "rows": 0,
                     "first_season": season, "last_season": season},
                )
                entry["rows"] += 1
                if season is not None:
                    if entry["first_season"] is None or season < entry["first_season"]:
                        entry["first_season"] = season
                    if entry["last_season"] is None or season > entry["last_season"]:
                        entry["last_season"] = season
        buffer.extend(rows)
        if len(buffer) >= 400_000:
            flush()
        if count % 2000 == 0:
            print(f"  {count:,}/{len(keys):,} games  rows so far {written + len(buffer):,}", flush=True)
    flush()

    dictionary = sorted(columns.values(), key=lambda row: (row["table_tag"], -row["rows"]))
    (OUT_DIR / "COLUMN_DICTIONARY.json").write_text(
        json.dumps(dictionary, indent=2), encoding="utf-8"
    )
    return {
        "law": "the source bytes were already on disk; every table on them that we did "
               "not parse existed in no denominator anywhere in the program",
        "premise_measured": {
            "retained_html_files": 151623,
            "of_which_shard17_recovery_workdirs": 12980,
            "boxscore_inventory": 138643,
            "bot_challenge_pages_shards_3_and_7": 19805,
            "distinct_games_requested": 17027,
            "distinct_games_parsed_by_original_capture": 16996,
            "games_never_parsed": 31,
            "note": "the retained superset is SHARD REDUNDANCY (~5x), not extra games, "
                    "so this lane parses one page per game",
        },
        "counters": {
            "games_reparsed": len(keys),
            "rows": written,
            "challenge_pages_skipped": challenge_pages,
            "pages_yielding_no_rows": empty_pages,
            "rows_skipped_width_mismatch": width_mismatch,
            "unresolved_table_names": len(unresolved),
            "team_labels_unresolved_by_scoreboard": unresolved_team_labels,
            "tables_with_unlabelled_columns": unlabelled_tables,
            "distinct_columns": len(columns),
        },
        "unresolved_table_names": dict(unresolved.most_common()),
        "rows_by_table": dict(tag_rows.most_common()),
        "players_by_table": {k: len(v) for k, v in sorted(tag_players.items())},
        "season_span_by_table": {
            k: [min(s for s in v if s is not None), max(s for s in v if s is not None)]
            for k, v in sorted(tag_seasons.items())
            if any(s is not None for s in v)
        },
        "output_dir": OUT_DIR.as_posix(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    doc = run(limit=args.limit)
    from .recon_common import utc_stamp

    doc["generated_utc"] = utc_stamp()
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    counters = doc["counters"]
    print(f"\ngames re-parsed : {counters['games_reparsed']:,}")
    print(f"rows            : {counters['rows']:,}")
    print(f"challenge pages : {counters['challenge_pages_skipped']:,}")
    print(f"unresolved names: {counters['unresolved_table_names']}  {doc['unresolved_table_names']}")
    print("\nrows by table:")
    for tag, count in doc["rows_by_table"].items():
        span = doc["season_span_by_table"].get(tag, ["?", "?"])
        players = doc["players_by_table"].get(tag, 0)
        print(f"    {tag:20s} {count:>9,}  players {players:>7,}  {span[0]}-{span[1]}")
    print(f"\nreceipt -> {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
