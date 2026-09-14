#!/usr/bin/env python3
"""Audit DDL playoff bracket inference against final-game results.

This intentionally does NOT read or trust the ``champion`` column from Fly.

For each league-season, the audit compares:

1. "Think" winner: the championship bracket tracer run against the flattened
   DDL matchup rows with champion flags forced to zero. The default engine is
   a fast in-memory DDL tracer; ``--engine sql`` runs the slower production SQL
   tracer for deep spot checks.
2. "Did" winner: the winner of the championship/final game in the DDL, derived
   from actual scores or win flags on played matchup rows.

The output is a markdown report suitable for checking into a QA artifact or
posting in an investigation thread.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

from multi_league.core.db_reader import get_reader
from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
    trace_championship_bracket_sql,
)

REPORT_DEFAULT = ROOT / "reports" / "ddl_bracket_results_audit.md"

REAL_OPPONENT_SENTINELS = {"", "bye", "none", "null", "nan"}
CHAMPIONSHIP_ROUND_NAMES = {"championship", "championship_game", "final", "finals"}

MATCHUP_COLUMNS = [
    "db_name",
    "year",
    "week",
    "manager",
    "franchise_id",
    "opponent",
    "opponent_franchise_id",
    "team_name",
    "team_points",
    "opponent_points",
    "win",
    "loss",
    "tie",
    "is_playoffs",
    "is_consolation",
    "is_championship",
    "postseason",
    "playoff_round",
    "final_playoff_seed",
    "playoff_seed",
    "placement_rank",
    "is_bye_week",
]

SETTINGS_COLUMNS = [
    "db_name",
    "year",
    "platform",
    "league_key",
    "num_teams",
    "playoff_teams",
    "bye_teams",
    "playoff_start_week",
    "regular_season_weeks",
    "end_week",
    "uses_median",
    "has_multiweek_championship",
    "uses_playoff_reseeding",
    "sleeper_playoff_type",
]


@dataclass(frozen=True)
class GameResult:
    """Collapsed result for one matchup pair, possibly across multiple weeks."""

    team_a: str
    team_b: str
    winner: str | None
    loser: str | None
    points_a: float | None
    points_b: float | None
    weeks: tuple[int, ...]
    source: str
    row_count: int


@dataclass
class GameAccumulator:
    """Accumulates reciprocal rows without double-counting team/week points."""

    teams: tuple[str, str]
    source: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    scores: dict[tuple[str, int], tuple[float, bool]] = field(default_factory=dict)
    wins: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    weeks: set[int] = field(default_factory=set)

    def add_row(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        week = _to_int(row.get("week"))
        if week is not None:
            self.weeks.add(week)

        manager_id = identity_key(row)
        opponent_id = opponent_identity_key(row)
        if manager_id:
            self._record_score(manager_id, week, row.get("team_points"), direct=True)
            if _to_int(row.get("win")) == 1:
                self.wins[manager_id] += 1
        if opponent_id:
            self._record_score(opponent_id, week, row.get("opponent_points"), direct=False)

    def _record_score(self, team_id: str, week: int | None, value: Any, *, direct: bool) -> None:
        score = _to_float(value)
        if week is None or score is None:
            return
        key = (team_id, week)
        existing = self.scores.get(key)
        if existing is None or (direct and not existing[1]):
            self.scores[key] = (score, direct)

    def result(self) -> GameResult:
        team_a, team_b = self.teams
        points_a = _sum_scores(self.scores, team_a)
        points_b = _sum_scores(self.scores, team_b)
        winner: str | None = None
        loser: str | None = None

        if points_a is not None and points_b is not None:
            if points_a > points_b:
                winner, loser = team_a, team_b
            elif points_b > points_a:
                winner, loser = team_b, team_a

        if winner is None:
            wins_a = self.wins.get(team_a, 0)
            wins_b = self.wins.get(team_b, 0)
            if wins_a > wins_b:
                winner, loser = team_a, team_b
            elif wins_b > wins_a:
                winner, loser = team_b, team_a

        return GameResult(
            team_a=team_a,
            team_b=team_b,
            winner=winner,
            loser=loser,
            points_a=points_a,
            points_b=points_b,
            weeks=tuple(sorted(self.weeks)),
            source=self.source,
            row_count=len(self.rows),
        )


@dataclass(frozen=True)
class SeasonAudit:
    db_name: str
    year: int
    platform: str
    status: str
    think_winner: str | None
    did_winner: str | None
    think_winner_name: str | None
    did_winner_name: str | None
    runner_up_name: str | None
    final_score: str | None
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


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, int | float):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _round_name(value: Any) -> str:
    return _clean(value).lower().replace(" ", "_").replace("-", "_")


def _sum_scores(scores: dict[tuple[str, int], tuple[float, bool]], team_id: str) -> float | None:
    values = [score for (tid, _week), (score, _direct) in scores.items() if tid == team_id]
    if not values:
        return None
    return round(sum(values), 2)


def identity_key(row: dict[str, Any]) -> str:
    return _clean(row.get("franchise_id")) or _clean(row.get("manager"))


def opponent_identity_key(row: dict[str, Any]) -> str:
    return _clean(row.get("opponent_franchise_id")) or _clean(row.get("opponent"))


def display_name_lookup(rows: list[dict[str, Any]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for row in sorted(rows, key=lambda r: (_to_int(r.get("week")) or 0)):
        key = identity_key(row)
        manager = _clean(row.get("manager"))
        if key and manager:
            lookup[key] = manager
    return lookup


def is_real_played_row(row: dict[str, Any]) -> bool:
    manager_id = identity_key(row)
    opponent_id = opponent_identity_key(row)
    opponent_name = _clean(row.get("opponent")).lower()
    if not manager_id or not opponent_id or opponent_name in REAL_OPPONENT_SENTINELS:
        return False
    if _truthy(row.get("is_bye_week")):
        return False
    has_score = _to_float(row.get("team_points")) is not None or _to_float(row.get("opponent_points")) is not None
    has_result = _to_int(row.get("win")) is not None or _to_int(row.get("loss")) is not None
    return has_score or has_result


def _pair_key(row: dict[str, Any]) -> tuple[str, str] | None:
    a = identity_key(row)
    b = opponent_identity_key(row)
    if not a or not b:
        return None
    return tuple(sorted((a, b)))


def collapse_games(rows: list[dict[str, Any]], source: str) -> list[GameResult]:
    accumulators: dict[tuple[str, str], GameAccumulator] = {}
    for row in rows:
        key = _pair_key(row)
        if key is None:
            continue
        acc = accumulators.setdefault(key, GameAccumulator(teams=key, source=source))
        acc.add_row(row)
    return [acc.result() for acc in accumulators.values()]


def collapse_games_by_round(rows: list[dict[str, Any]], source: str) -> list[GameResult]:
    accumulators: dict[tuple[str, tuple[str, str]], GameAccumulator] = {}
    for row in rows:
        pair = _pair_key(row)
        if pair is None:
            continue
        round_key = _round_name(row.get("playoff_round")) or f"week_{_to_int(row.get('week')) or 0}"
        key = (round_key, pair)
        acc = accumulators.setdefault(key, GameAccumulator(teams=pair, source=source))
        acc.add_row(row)
    return [acc.result() for acc in accumulators.values()]


def actual_championship_result(rows: list[dict[str, Any]]) -> tuple[GameResult | None, str]:
    """Derive the actual title-game winner from DDL rows, never champion flags."""

    championship_rows = [
        row
        for row in rows
        if is_real_played_row(row)
        and not _truthy(row.get("is_consolation"))
        and (_truthy(row.get("is_championship")) or _round_name(row.get("playoff_round")) in CHAMPIONSHIP_ROUND_NAMES)
    ]
    if championship_rows:
        games = collapse_games(championship_rows, "championship_label")
        games.sort(key=lambda g: (max(g.weeks) if g.weeks else -1, g.row_count), reverse=True)
        return games[0], "championship_label"

    playoff_rows = [
        row
        for row in rows
        if is_real_played_row(row) and _truthy(row.get("is_playoffs")) and not _truthy(row.get("is_consolation"))
    ]
    if not playoff_rows:
        return None, "no_playoff_rows"

    latest_week = max(_to_int(row.get("week")) or 0 for row in playoff_rows)
    latest_rows = [row for row in playoff_rows if (_to_int(row.get("week")) or 0) == latest_week]
    games = collapse_games(latest_rows, "latest_playoff_week")
    if len(games) == 1:
        return games[0], "latest_playoff_week"

    return None, f"ambiguous_latest_playoff_week_{latest_week}_{len(games)}_games"


def trace_fast_think_winner(rows: list[dict[str, Any]], _settings: dict[str, Any]) -> tuple[str | None, str]:
    """Infer the bracket survivor from DDL played rows without champion flags.

    This is intentionally conservative:
    - only real non-consolation games participate;
    - teams with previous championship-bracket losses are not considered alive
      finalists, so third-place/placement rows do not displace the title game;
    - the winner is taken from the latest alive game.
    """

    bracket_rows = [
        row
        for row in rows
        if is_real_played_row(row)
        and not _truthy(row.get("is_consolation"))
        and (
            _truthy(row.get("is_playoffs"))
            or _truthy(row.get("is_championship"))
            or _round_name(row.get("playoff_round"))
            in {"wildcard", "quarterfinal", "semifinal", "championship", "final"}
        )
    ]
    games = [game for game in collapse_games_by_round(bracket_rows, "fast_ddl_trace") if game.winner]
    if not games:
        return None, "fast_trace_no_games"

    def sort_key(game: GameResult) -> tuple[int, int, str, str]:
        return (
            min(game.weeks) if game.weeks else 0,
            max(game.weeks) if game.weeks else 0,
            game.team_a,
            game.team_b,
        )

    losses: dict[str, int] = defaultdict(int)
    alive_games: list[GameResult] = []
    all_games: list[GameResult] = []
    for game in sorted(games, key=sort_key):
        all_games.append(game)
        both_alive = losses.get(game.team_a, 0) == 0 and losses.get(game.team_b, 0) == 0
        if both_alive:
            alive_games.append(game)
        if game.loser:
            losses[game.loser] += 1

    candidates = alive_games or all_games
    candidates.sort(key=lambda game: (max(game.weeks) if game.weeks else 0, min(game.weeks) if game.weeks else 0))
    winner = candidates[-1].winner
    note = "fast_trace_alive_path" if alive_games else "fast_trace_latest_game_fallback"
    return winner, note


def trace_sql_think_winner(rows: list[dict[str, Any]], settings: dict[str, Any]) -> tuple[str | None, str]:
    """Run the existing generic tracer with champion flags forced off."""

    if not rows:
        return None, "no_rows"
    if not any(identity_key(row) and opponent_identity_key(row) for row in rows):
        return None, "missing_identity"

    df = pd.DataFrame(rows).copy()
    for col in MATCHUP_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df["champion"] = 0
    df["championship"] = 0
    for bool_col in ("is_championship", "is_playoffs", "is_consolation", "is_bye_week"):
        df[bool_col] = df[bool_col].map(_truthy)

    conn = duckdb.connect()
    try:
        conn.register("_audit_matchup_rows", df)
        conn.execute("CREATE TABLE matchup AS SELECT * FROM _audit_matchup_rows")
        trace_settings = build_trace_settings(rows, settings)
        result = trace_championship_bracket_sql(
            conn,
            int(settings["year"]),
            trace_settings,
            table="matchup",
            id_col="franchise_id",
            write_back=False,
        )
    except Exception as exc:
        return None, f"trace_error:{exc}"
    finally:
        conn.close()

    champion = result.get("champion")
    note = "trace_complete" if result.get("is_complete") else "trace_incomplete"
    return (str(champion) if champion else None), note


def trace_think_winner(
    rows: list[dict[str, Any]], settings: dict[str, Any], *, engine: str = "fast"
) -> tuple[str | None, str]:
    if engine == "sql":
        return trace_sql_think_winner(rows, settings)
    return trace_fast_think_winner(rows, settings)


def build_trace_settings(rows: list[dict[str, Any]], settings: dict[str, Any]) -> dict[str, Any]:
    playoff_start = _to_int(settings.get("playoff_start_week")) or _infer_playoff_start(rows)
    end_week = _to_int(settings.get("end_week")) or _infer_end_week(rows)
    regular_weeks = _to_int(settings.get("regular_season_weeks"))
    if regular_weeks and (not playoff_start or playoff_start <= 1):
        playoff_start = regular_weeks + 1

    playoff_teams = _to_int(settings.get("playoff_teams")) or _infer_playoff_teams(rows)
    bye_teams = _to_int(settings.get("bye_teams"))
    if bye_teams is None:
        bye_teams = max(0, _next_power_of_two(playoff_teams) - playoff_teams) if playoff_teams else 0

    return {
        "playoff_teams": playoff_teams or 0,
        "bye_teams": bye_teams,
        "playoff_start_week": playoff_start or 14,
        "end_week": end_week or max((_to_int(r.get("week")) or 0 for r in rows), default=17),
        "num_teams": _to_int(settings.get("num_teams")) or 0,
        "uses_median": _truthy(settings.get("uses_median")),
        "uses_median_score": _truthy(settings.get("uses_median")),
        "uses_playoff_reseeding": _truthy(settings.get("uses_playoff_reseeding")),
        "playoff_round_type": _to_int(settings.get("sleeper_playoff_type")) or 0,
        "has_multiweek_championship": _truthy(settings.get("has_multiweek_championship")),
    }


def _infer_playoff_start(rows: list[dict[str, Any]]) -> int | None:
    weeks = [_to_int(r.get("week")) for r in rows if _truthy(r.get("is_playoffs")) or _truthy(r.get("is_consolation"))]
    weeks = [week for week in weeks if week is not None]
    return min(weeks) if weeks else None


def _infer_end_week(rows: list[dict[str, Any]]) -> int | None:
    weeks = [_to_int(r.get("week")) for r in rows if is_real_played_row(r)]
    weeks = [week for week in weeks if week is not None]
    return max(weeks) if weeks else None


def _infer_playoff_teams(rows: list[dict[str, Any]]) -> int | None:
    seeds = {
        _to_int(row.get("final_playoff_seed")) or _to_int(row.get("playoff_seed"))
        for row in rows
        if _truthy(row.get("is_playoffs"))
    }
    seeds = {seed for seed in seeds if seed is not None}
    return max(seeds) if seeds else None


def _next_power_of_two(value: int) -> int:
    n = 1
    while n < value:
        n *= 2
    return n


def audit_season(settings: dict[str, Any], rows: list[dict[str, Any]], *, engine: str = "fast") -> SeasonAudit:
    db_name = str(settings["db_name"])
    year = int(settings["year"])
    platform = _clean(settings.get("platform"))
    names = display_name_lookup(rows)

    playoff_teams = _to_int(settings.get("playoff_teams")) or 0
    if playoff_teams < 2:
        return SeasonAudit(
            db_name, year, platform, "SKIP", None, None, None, None, None, None, None, "settings", "no playoffs"
        )

    did, did_source = actual_championship_result(rows)
    if did is None:
        return SeasonAudit(
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
            did_source,
            "no unambiguous championship game in DDL",
        )
    if did.winner is None:
        return SeasonAudit(
            db_name,
            year,
            platform,
            "INCOMPLETE",
            None,
            None,
            None,
            None,
            None,
            format_score(did),
            format_weeks(did.weeks),
            did_source,
            "championship game exists but winner is not decidable from scores/win flags",
        )

    think_winner, trace_note = trace_think_winner(rows, settings, engine=engine)
    did_winner = did.winner
    status = "PASS" if think_winner and did_winner and think_winner == did_winner else "MISMATCH"
    if not think_winner:
        status = "INCOMPLETE"
    notes = trace_note
    if status == "MISMATCH":
        first_final_week = min(did.weeks) if did.weeks else None
        did_prior_losses = count_prior_bracket_losses(rows, did_winner, first_final_week)
        think_prior_losses = count_prior_bracket_losses(rows, think_winner, first_final_week)
        notes = (
            f"{trace_note}; did_prior_losses_before_final={did_prior_losses}; "
            f"think_prior_losses_before_final={think_prior_losses}"
        )

    return SeasonAudit(
        db_name=db_name,
        year=year,
        platform=platform,
        status=status,
        think_winner=think_winner,
        did_winner=did_winner,
        think_winner_name=names.get(think_winner or "", think_winner),
        did_winner_name=names.get(did_winner or "", did_winner),
        runner_up_name=names.get(did.loser or "", did.loser),
        final_score=format_score(did),
        final_weeks=format_weeks(did.weeks),
        source=did_source,
        notes=notes,
    )


def count_prior_bracket_losses(rows: list[dict[str, Any]], team_id: str | None, before_week: int | None) -> int:
    if not team_id or before_week is None:
        return 0
    prior_rows = [
        row
        for row in rows
        if is_real_played_row(row)
        and not _truthy(row.get("is_consolation"))
        and (_to_int(row.get("week")) or 0) < before_week
        and (
            _truthy(row.get("is_playoffs"))
            or _truthy(row.get("is_championship"))
            or _round_name(row.get("playoff_round"))
            in {"wildcard", "quarterfinal", "semifinal", "championship", "final"}
        )
    ]
    losses = 0
    for game in collapse_games_by_round(prior_rows, "prior_loss_check"):
        if game.loser == team_id:
            losses += 1
    return losses


def format_score(game: GameResult | None) -> str | None:
    if game is None or game.winner is None:
        return None
    if game.winner == game.team_a:
        champ_pts, runner_pts = game.points_a, game.points_b
    else:
        champ_pts, runner_pts = game.points_b, game.points_a
    if champ_pts is None or runner_pts is None:
        return None
    return f"{champ_pts:.2f} - {runner_pts:.2f}"


def format_weeks(weeks: tuple[int, ...]) -> str | None:
    if not weeks:
        return None
    if len(weeks) == 1:
        return str(weeks[0])
    if weeks == tuple(range(min(weeks), max(weeks) + 1)):
        return f"{min(weeks)}-{max(weeks)}"
    return ",".join(str(w) for w in weeks)


def load_settings(reader, db_names: list[str] | None, years: list[int] | None) -> list[dict[str, Any]]:
    where = ["playoff_teams IS NOT NULL", "year IS NOT NULL"]
    if db_names:
        where.append(f"db_name IN ({_quote_list(db_names)})")
    if years:
        where.append(f"year IN ({', '.join(str(int(y)) for y in years)})")
    sql = f"""
        SELECT {", ".join(SETTINGS_COLUMNS)}
        FROM public.league_settings
        WHERE {" AND ".join(where)}
        ORDER BY db_name, year
    """
    return reader.query(sql, database="___leagues")


def load_matchups(reader, db_names: list[str] | None, years: list[int] | None) -> list[dict[str, Any]]:
    where = ["year IS NOT NULL"]
    if db_names:
        where.append(f"db_name IN ({_quote_list(db_names)})")
    if years:
        where.append(f"year IN ({', '.join(str(int(y)) for y in years)})")
    sql = f"""
        SELECT {", ".join(MATCHUP_COLUMNS)}
        FROM public.matchup
        WHERE {" AND ".join(where)}
        ORDER BY db_name, year, week, manager
    """
    return reader.query(sql, database="___leagues")


def _quote_list(values: list[str]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def run_audit(
    db_names: list[str] | None = None, years: list[int] | None = None, *, engine: str = "fast"
) -> list[SeasonAudit]:
    _load_env()
    reader = get_reader()
    settings_rows = load_settings(reader, db_names, years)
    matchup_rows = load_matchups(reader, db_names, years)

    rows_by_season: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in matchup_rows:
        year = _to_int(row.get("year"))
        db_name = _clean(row.get("db_name"))
        if db_name and year is not None:
            rows_by_season[(db_name, year)].append(row)

    audits: list[SeasonAudit] = []
    for settings in settings_rows:
        key = (_clean(settings.get("db_name")), int(settings["year"]))
        audits.append(audit_season(settings, rows_by_season.get(key, []), engine=engine))

    return audits


def write_markdown_report(audits: list[SeasonAudit], output_path: Path, *, engine: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = len(audits)
    passed = sum(1 for audit in audits if audit.passed)
    mismatches = [audit for audit in audits if audit.mismatch]
    incomplete = [audit for audit in audits if audit.status == "INCOMPLETE"]
    skipped = [audit for audit in audits if audit.status == "SKIP"]

    lines = [
        "# DDL Bracket Results Audit",
        "",
        f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "This audit does not read or trust the `champion` column.",
        "",
        f"- THINK: `{engine}` championship tracer result from flattened DDL with champion flags forced off.",
        "- DID: winner of the DDL championship/final game by score or win flag.",
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


def write_csv_report(audits: list[SeasonAudit], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SeasonAudit.__dataclass_fields__.keys()))
        writer.writeheader()
        for audit in audits:
            writer.writerow({field: getattr(audit, field) for field in writer.fieldnames})


def _table_header() -> str:
    return (
        "| Status | League | Year | Platform | THINK winner | DID winner | Runner-up | Final score | Weeks | Source | Notes |\n"
        "|---|---|---:|---|---|---|---|---|---|---|---|"
    )


def _table_row(audit: SeasonAudit) -> str:
    return (
        f"| {audit.status} | {_md(audit.db_name)} | {audit.year} | {_md(audit.platform)} | "
        f"{_md(audit.think_winner_name)} | {_md(audit.did_winner_name)} | {_md(audit.runner_up_name)} | "
        f"{_md(audit.final_score)} | {_md(audit.final_weeks)} | {_md(audit.source)} | {_md(audit.notes)} |"
    )


def _md(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit DDL-derived bracket winners against DDL final results")
    parser.add_argument("--db", action="append", dest="db_names", help="Database name to audit; repeatable")
    parser.add_argument("--year", action="append", type=int, dest="years", help="Season year to audit; repeatable")
    parser.add_argument(
        "--out", type=Path, default=REPORT_DEFAULT, help=f"Markdown output path (default: {REPORT_DEFAULT})"
    )
    parser.add_argument("--csv", type=Path, default=None, help="Optional CSV output path")
    parser.add_argument(
        "--engine",
        choices=["fast", "sql"],
        default="fast",
        help="Trace engine. fast is fleet-safe; sql runs the slower production SQL tracer.",
    )
    parser.add_argument("--fail-on-mismatch", action="store_true", help="Exit nonzero if any mismatch is found")
    parser.add_argument("--fail-on-incomplete", action="store_true", help="Exit nonzero if any season is incomplete")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audits = run_audit(args.db_names, args.years, engine=args.engine)
    write_markdown_report(audits, args.out, engine=args.engine)
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
