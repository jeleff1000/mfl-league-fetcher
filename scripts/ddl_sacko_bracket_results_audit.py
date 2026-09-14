#!/usr/bin/env python3
"""Audit DDL consolation/Sacko results against live Sacko flags.

This intentionally does NOT trust the ``sacko`` column when deriving the
expected result.

For each league-season, the audit compares:

1. DDL Sacko: the loser who survives the consolation bracket's loser path,
   derived only from flattened DDL matchup rows and scores/win flags.
2. Current Sacko: the franchise currently marked with ``sacko = 1``.

The report is meant to catch both stale data and pipeline regressions where
consolation games were re-arranged or a lower seed could not become Sacko.
"""

from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import ddl_bracket_results_audit as bracket_audit

from multi_league.core.db_reader import get_reader

ROOT = Path(__file__).resolve().parent.parent
REPORT_DEFAULT = ROOT / "reports" / "ddl_sacko_bracket_results_audit.md"

SACKO_ROUND_NAMES = {
    "consolation_final",
    "consolation_championship",
    "sacko",
    "sacko_bowl",
    "last_place_game",
}

MATCHUP_COLUMNS = [
    *bracket_audit.MATCHUP_COLUMNS,
    "champion",
    "sacko",
    "consolation_round",
    "placement_game",
]

SETTINGS_SELECT_COLUMNS = [
    *(f"ls.{col}" for col in bracket_audit.SETTINGS_COLUMNS),
    "COALESCE(lr.sacko_mode, 'consolation_bracket') AS sacko_mode",
]


@dataclass(frozen=True)
class ConsolationGame:
    game: bracket_audit.GameResult
    round_key: str

    @property
    def winner(self) -> str | None:
        return self.game.winner

    @property
    def loser(self) -> str | None:
        return self.game.loser

    @property
    def weeks(self) -> tuple[int, ...]:
        return self.game.weeks


@dataclass(frozen=True)
class SackoAudit:
    db_name: str
    year: int
    platform: str
    status: str
    ddl_sacko: str | None
    current_sacko: str | None
    ddl_sacko_name: str | None
    current_sacko_name: str | None
    opponent_name: str | None
    sacko_score: str | None
    final_weeks: str | None
    source: str
    notes: str

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    @property
    def mismatch(self) -> bool:
        return self.status == "MISMATCH"


def _load_env() -> None:
    for env_file in (ROOT / ".env", ROOT / "frontend" / ".env.local"):
        if not env_file.exists():
            continue
        for raw in env_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _round_name(row: dict[str, Any]) -> str:
    return bracket_audit._round_name(row.get("consolation_round")) or bracket_audit._round_name(
        row.get("playoff_round")
    )


def is_consolation_evidence(row: dict[str, Any]) -> bool:
    return (
        bracket_audit._truthy(row.get("is_consolation"))
        or bool(bracket_audit._round_name(row.get("consolation_round")))
        or bracket_audit._truthy(row.get("consolation_final"))
    )


def is_placement_game(row: dict[str, Any]) -> bool:
    round_key = _round_name(row)
    return bracket_audit._truthy(row.get("placement_game")) or "place_game" in round_key


def collapse_consolation_games_by_round(rows: list[dict[str, Any]]) -> list[ConsolationGame]:
    accumulators: dict[tuple[str, tuple[str, str]], bracket_audit.GameAccumulator] = {}
    for row in rows:
        pair = bracket_audit._pair_key(row)
        if pair is None:
            continue
        round_key = _round_name(row) or f"week_{bracket_audit._to_int(row.get('week')) or 0}"
        key = (round_key, pair)
        acc = accumulators.setdefault(
            key,
            bracket_audit.GameAccumulator(teams=pair, source=round_key),
        )
        acc.add_row(row)

    return [ConsolationGame(acc.result(), round_key) for (round_key, _pair), acc in accumulators.items()]


def trace_ddl_sacko(rows: list[dict[str, Any]]) -> tuple[ConsolationGame | None, str]:
    """Derive the Sacko from actual consolation games, never sacko flags.

    In a Sacko bracket, the loser advances toward last place and the winner
    exits the Sacko path. Tracking prior consolation wins lets us ignore
    placement games that should not decide last place.
    """

    consolation_rows = [
        row
        for row in rows
        if bracket_audit.is_real_played_row(row) and is_consolation_evidence(row) and not is_placement_game(row)
    ]
    games = [game for game in collapse_consolation_games_by_round(consolation_rows) if game.winner and game.loser]
    if not games:
        return None, "no_consolation_games"

    labeled_finals = [game for game in games if game.round_key in SACKO_ROUND_NAMES]
    if len(labeled_finals) == 1:
        return labeled_finals[0], "single_sacko_final_label"

    def sort_key(game: ConsolationGame) -> tuple[int, int, str, str]:
        return (
            min(game.weeks) if game.weeks else 0,
            max(game.weeks) if game.weeks else 0,
            game.game.team_a,
            game.game.team_b,
        )

    wins: dict[str, int] = defaultdict(int)
    alive_games: list[ConsolationGame] = []
    for game in sorted(games, key=sort_key):
        both_alive = wins.get(game.game.team_a, 0) == 0 and wins.get(game.game.team_b, 0) == 0
        if both_alive:
            alive_games.append(game)
        if game.winner:
            wins[game.winner] += 1

    candidates = alive_games or games
    latest_week = max(max(game.weeks) if game.weeks else 0 for game in candidates)
    latest_candidates = [game for game in candidates if (max(game.weeks) if game.weeks else 0) == latest_week]

    if len(latest_candidates) == 1:
        source = "ddl_loser_path" if alive_games else "latest_consolation_game_fallback"
        return latest_candidates[0], source

    latest_labeled = [game for game in latest_candidates if game.round_key in SACKO_ROUND_NAMES]
    if len(latest_labeled) == 1:
        return latest_labeled[0], "latest_sacko_final_label"

    return None, f"ambiguous_latest_consolation_week_{latest_week}_{len(latest_candidates)}_games"


def current_sacko(rows: list[dict[str, Any]]) -> tuple[str | None, str]:
    sacko_ids = {
        bracket_audit.identity_key(row)
        for row in rows
        if bracket_audit._truthy(row.get("sacko")) and bracket_audit.identity_key(row)
    }
    if not sacko_ids:
        return None, "current_no_sacko_flag"
    if len(sacko_ids) > 1:
        return None, "current_multiple_sacko_flags:" + ",".join(sorted(sacko_ids))
    return next(iter(sacko_ids)), "current_sacko_flag"


def audit_season(settings: dict[str, Any], rows: list[dict[str, Any]]) -> SackoAudit:
    db_name = str(settings["db_name"])
    year = int(settings["year"])
    platform = bracket_audit._clean(settings.get("platform"))
    names = bracket_audit.display_name_lookup(rows)

    playoff_teams = bracket_audit._to_int(settings.get("playoff_teams")) or 0
    if playoff_teams < 2:
        return SackoAudit(
            db_name,
            year,
            platform,
            "SKIP",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "settings",
            "no playoffs",
        )

    ddl_game, ddl_source = trace_ddl_sacko(rows)
    if ddl_game is None or ddl_game.loser is None:
        return SackoAudit(
            db_name,
            year,
            platform,
            "INCOMPLETE",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            ddl_source,
            "no unambiguous Sacko-deciding consolation game in DDL",
        )

    expected = ddl_game.loser
    current, current_source = current_sacko(rows)
    status = "PASS" if current and current == expected else "MISMATCH"
    notes = current_source
    if settings.get("sacko_mode") and settings.get("sacko_mode") != "consolation_bracket":
        notes = f"{notes}; league_rules.sacko_mode={settings.get('sacko_mode')}"

    return SackoAudit(
        db_name=db_name,
        year=year,
        platform=platform,
        status=status,
        ddl_sacko=expected,
        current_sacko=current,
        ddl_sacko_name=names.get(expected, expected),
        current_sacko_name=names.get(current or "", current),
        opponent_name=names.get(ddl_game.winner or "", ddl_game.winner),
        sacko_score=format_sacko_score(ddl_game.game),
        final_weeks=bracket_audit.format_weeks(ddl_game.weeks),
        source=ddl_source,
        notes=notes,
    )


def format_sacko_score(game: bracket_audit.GameResult | None) -> str | None:
    if game is None or game.winner is None or game.loser is None:
        return None
    if game.loser == game.team_a:
        sacko_pts, opponent_pts = game.points_a, game.points_b
    else:
        sacko_pts, opponent_pts = game.points_b, game.points_a
    if sacko_pts is None or opponent_pts is None:
        return None
    return f"{sacko_pts:.2f} - {opponent_pts:.2f}"


def load_settings(reader, db_names: list[str] | None, years: list[int] | None) -> list[dict[str, Any]]:
    where = ["ls.playoff_teams IS NOT NULL", "ls.year IS NOT NULL"]
    if db_names:
        where.append(f"ls.db_name IN ({bracket_audit._quote_list(db_names)})")
    if years:
        where.append(f"ls.year IN ({', '.join(str(int(y)) for y in years)})")
    sql = f"""
        SELECT {", ".join(SETTINGS_SELECT_COLUMNS)}
        FROM public.league_settings ls
        LEFT JOIN public.league_rules lr ON lr.db_name = ls.db_name
        WHERE {" AND ".join(where)}
        ORDER BY ls.db_name, ls.year
    """
    return reader.query(sql, database="___leagues")


def load_matchups(reader, db_names: list[str] | None, years: list[int] | None) -> list[dict[str, Any]]:
    where = ["year IS NOT NULL"]
    if db_names:
        where.append(f"db_name IN ({bracket_audit._quote_list(db_names)})")
    if years:
        where.append(f"year IN ({', '.join(str(int(y)) for y in years)})")
    sql = f"""
        SELECT {", ".join(dict.fromkeys(MATCHUP_COLUMNS))}
        FROM public.matchup
        WHERE {" AND ".join(where)}
        ORDER BY db_name, year, week, manager
    """
    return reader.query(sql, database="___leagues")


def run_audit(db_names: list[str] | None = None, years: list[int] | None = None) -> list[SackoAudit]:
    _load_env()
    reader = get_reader()
    settings_rows = load_settings(reader, db_names, years)
    matchup_rows = load_matchups(reader, db_names, years)

    rows_by_season: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in matchup_rows:
        year = bracket_audit._to_int(row.get("year"))
        db_name = bracket_audit._clean(row.get("db_name"))
        if db_name and year is not None:
            rows_by_season[(db_name, year)].append(row)

    audits: list[SackoAudit] = []
    for settings in settings_rows:
        key = (bracket_audit._clean(settings.get("db_name")), int(settings["year"]))
        audits.append(audit_season(settings, rows_by_season.get(key, [])))

    return audits


def write_markdown_report(audits: list[SackoAudit], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = len(audits)
    passed = sum(1 for audit in audits if audit.passed)
    mismatches = [audit for audit in audits if audit.mismatch]
    incomplete = [audit for audit in audits if audit.status == "INCOMPLETE"]
    skipped = [audit for audit in audits if audit.status == "SKIP"]

    lines = [
        "# DDL Sacko Bracket Results Audit",
        "",
        f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "This audit derives the expected Sacko from DDL matchup rows and does not trust the `sacko` column.",
        "",
        "- DDL Sacko: loser who survives the consolation loser path by score/win flags.",
        "- Current Sacko: franchise currently marked `sacko = 1` in Fly.",
        "",
        "## Summary",
        "",
        f"- Seasons checked: {total}",
        f"- Pass: {passed}",
        f"- Mismatch: {len(mismatches)}",
        f"- Incomplete/ambiguous: {len(incomplete)}",
        f"- Skipped no-playoff seasons: {len(skipped)}",
        "",
    ]

    if mismatches:
        lines.extend(["## Mismatches", "", _table_header()])
        for audit in mismatches:
            lines.append(_table_row(audit))
        lines.append("")

    if incomplete:
        lines.extend(["## Incomplete Or Ambiguous", "", _table_header()])
        for audit in incomplete:
            lines.append(_table_row(audit))
        lines.append("")

    lines.extend(["## Season Results", "", _table_header()])
    for audit in sorted(audits, key=lambda a: (a.db_name, a.year)):
        if audit.status == "SKIP":
            continue
        lines.append(_table_row(audit))
    lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_csv_report(audits: list[SackoAudit], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SackoAudit.__dataclass_fields__.keys()))
        writer.writeheader()
        for audit in audits:
            writer.writerow({field: getattr(audit, field) for field in writer.fieldnames})


def _table_header() -> str:
    return (
        "| Status | League | Year | Platform | DDL Sacko | Current Sacko | Opponent | Sacko score | Weeks | Source | Notes |\n"
        "|---|---|---:|---|---|---|---|---|---|---|---|"
    )


def _table_row(audit: SackoAudit) -> str:
    return (
        f"| {audit.status} | {_md(audit.db_name)} | {audit.year} | {_md(audit.platform)} | "
        f"{_md(audit.ddl_sacko_name)} | {_md(audit.current_sacko_name)} | {_md(audit.opponent_name)} | "
        f"{_md(audit.sacko_score)} | {_md(audit.final_weeks)} | {_md(audit.source)} | {_md(audit.notes)} |"
    )


def _md(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit DDL-derived Sacko results against live Sacko flags")
    parser.add_argument("--db", action="append", dest="db_names", help="Database name to audit; repeatable")
    parser.add_argument("--year", action="append", type=int, dest="years", help="Season year to audit; repeatable")
    parser.add_argument(
        "--out", type=Path, default=REPORT_DEFAULT, help=f"Markdown output path (default: {REPORT_DEFAULT})"
    )
    parser.add_argument("--csv", type=Path, default=None, help="Optional CSV output path")
    parser.add_argument("--fail-on-mismatch", action="store_true", help="Exit nonzero if any mismatch is found")
    parser.add_argument("--fail-on-incomplete", action="store_true", help="Exit nonzero if any season is incomplete")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audits = run_audit(args.db_names, args.years)
    write_markdown_report(audits, args.out)
    if args.csv:
        write_csv_report(audits, args.csv)

    mismatches = [audit for audit in audits if audit.mismatch]
    incomplete = [audit for audit in audits if audit.status == "INCOMPLETE"]

    print(f"Wrote {args.out}")
    if args.csv:
        print(f"Wrote {args.csv}")
    print(
        f"checked={len(audits)} pass={sum(1 for audit in audits if audit.passed)} "
        f"mismatch={len(mismatches)} incomplete={len(incomplete)}"
    )

    if args.fail_on_mismatch and mismatches:
        return 1
    if args.fail_on_incomplete and incomplete:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
