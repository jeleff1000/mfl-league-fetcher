"""Build guarded NFL operations refresh inputs from live NFLverse releases.

The action runner uses these guards to build its local ``___ops_nfl`` artifact.
Ordinary refreshes also use the finite mapping hook on their disposable local
bio cache. This module contains no remote publication adapter.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from io import StringIO
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb
import pandas as pd
import requests


class RefreshGateError(RuntimeError):
    """The source does not prove that a live NFL refresh is safe to publish."""


def load_nflverse_identity_roster(year: int) -> pd.DataFrame:
    """Read the small current-season identity directory with a bounded timeout."""
    response = requests.get(
        "https://github.com/nflverse/nflverse-data/releases/download/rosters/"
        f"roster_{int(year)}.csv", timeout=15,
    )
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text), dtype=str)


_SCHEDULE_COLUMNS = {
    "game_id",
    "season",
    "week",
    "game_type",
    "gameday",
    "away_team",
    "away_score",
    "home_team",
    "home_score",
}

_WEEKLY_COLUMNS = {
    "NFL_player_id",
    "year",
    "week",
    "game_id",
    "nfl_team",
    "opponent_nfl_team",
}

CANONICAL_FULL_KEY = (
    "NFL_player_id",
    "game_date",
    "week",
    "season_type",
    "nfl_team",
    "opponent_nfl_team",
)

REQUIRED_OPS_TABLES = (
    "nfl_player_stats_all",
    "player_nfl_season",
    "player_nfl_season_all",
    "player_nfl_career",
    "player_nfl_career_all",
    "player_bio",
    "player_nfl_season_team",
    "player_nfl_season_team_all",
)

VALID_LIVE_GAME_TYPES = frozenset({"REG", "POST"})

# The live fetch plane converts NFLverse codes to the historical/stathead
# vocabulary.  Schedule releases retain NFLverse's codes, so normalize only
# for matching a DST row that does not carry its own game_id.
_STATHEAD_TO_NFLVERSE_TEAM = {
    "GNB": "GB",
    "KAN": "KC",
    "LAR": "LA",
    "NOR": "NO",
    "NWE": "NE",
    "SFO": "SF",
    "TAM": "TB",
}
_NFLVERSE_TO_STATHEAD_TEAM = {value: key for key, value in _STATHEAD_TO_NFLVERSE_TEAM.items()}


def _team_match_code(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().replace(_STATHEAD_TO_NFLVERSE_TEAM)


def _historical_team(value: object) -> object:
    if value is None or pd.isna(value):
        return pd.NA
    token = str(value).strip().upper()
    return _NFLVERSE_TO_STATHEAD_TEAM.get(token, token) if token else pd.NA


def _blank(series: pd.Series) -> pd.Series:
    return series.isna() | (series.astype(str).str.strip() == "")


def _qident(identifier: str) -> str:
    """Quote a dotted DuckDB relation name from trusted application input."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in identifier.split("."))


def _parse_refresh_game_date(game_date: str) -> str:
    """Return one ISO game date or reject an unsafe scheduler input."""
    parsed = pd.to_datetime(game_date, errors="coerce")
    if pd.isna(parsed):
        raise RefreshGateError(f"invalid live refresh game date: {game_date!r}")
    return pd.Timestamp(parsed).strftime("%Y-%m-%d")


def _normalized_live_game_types(season_types: tuple[str, ...]) -> frozenset[str]:
    allowed = frozenset(str(value).strip().upper() for value in season_types if str(value).strip())
    if not allowed or not allowed <= VALID_LIVE_GAME_TYPES:
        raise RefreshGateError(f"unsupported live game types: {sorted(allowed)}")
    return allowed


def finalized_game_scope(
    schedule: pd.DataFrame,
    *,
    year: int,
    week: int,
    season_types: tuple[str, ...] = ("REG",),
    game_date: str | None = None,
) -> pd.DataFrame:
    """Return completed games safe to refresh for one exact season/week scope.

    A schedule row is final only when it has scores for both sides.  This avoids
    publishing live or future games merely because NFLverse has already emitted
    their schedule row.  Manual callers retain regular-season-only behavior;
    the scheduler may explicitly request a single final postseason game date.
    """
    missing = sorted(_SCHEDULE_COLUMNS - set(schedule.columns))
    if missing:
        raise RefreshGateError(f"schedule missing required columns: {', '.join(missing)}")
    allowed_types = _normalized_live_game_types(season_types)
    target_date = _parse_refresh_game_date(game_date) if game_date is not None else None

    scoped = schedule.loc[
        (pd.to_numeric(schedule["season"], errors="coerce") == int(year))
        & (pd.to_numeric(schedule["week"], errors="coerce") == int(week))
        & (schedule["game_type"].astype(str).str.strip().str.upper().isin(allowed_types))
    ].copy()
    scoped["away_score"] = pd.to_numeric(scoped["away_score"], errors="coerce")
    scoped["home_score"] = pd.to_numeric(scoped["home_score"], errors="coerce")
    scoped = scoped.loc[scoped["away_score"].notna() & scoped["home_score"].notna()].copy()

    dates = pd.to_datetime(scoped["gameday"], errors="coerce")
    if dates.isna().any():
        raise RefreshGateError("finalized schedule has an invalid game date")
    scoped["game_date"] = dates.dt.strftime("%Y-%m-%d")
    if target_date is not None:
        scoped = scoped.loc[scoped["game_date"] == target_date].copy()

    if scoped["game_id"].isna().any() or (scoped["game_id"].astype(str).str.strip() == "").any():
        raise RefreshGateError("finalized schedule has a blank game_id")
    if scoped["game_id"].duplicated().any():
        raise RefreshGateError("duplicate finalized game_id")

    return (
        scoped[["game_id", "game_date", "away_team", "away_score", "home_team", "home_score"]]
        .sort_values(["game_date", "game_id"], kind="stable")
        .reset_index(drop=True)
    )


def discover_scheduled_refresh_scope(
    schedule: pd.DataFrame,
    *,
    season: int,
    game_date: str,
) -> dict[str, object] | None:
    """Discover exactly one final REG/POST work item for a scheduler game date.

    A no-game date is an intentional no-op.  Two weekly/postseason work-item
    keys on one date are ambiguous to the one-work-item refresh runner and
    fail closed rather than broadening a replacement scope.
    """
    missing = sorted(_SCHEDULE_COLUMNS - set(schedule.columns))
    if missing:
        raise RefreshGateError(f"schedule missing required columns: {', '.join(missing)}")
    target_date = _parse_refresh_game_date(game_date)
    scoped = schedule.loc[
        (pd.to_numeric(schedule["season"], errors="coerce") == int(season))
        & (schedule["game_type"].astype(str).str.strip().str.upper().isin(VALID_LIVE_GAME_TYPES))
    ].copy()
    scoped["away_score"] = pd.to_numeric(scoped["away_score"], errors="coerce")
    scoped["home_score"] = pd.to_numeric(scoped["home_score"], errors="coerce")
    scoped = scoped.loc[scoped["away_score"].notna() & scoped["home_score"].notna()].copy()
    if scoped.empty:
        return None

    dates = pd.to_datetime(scoped["gameday"], errors="coerce")
    if dates.isna().any():
        raise RefreshGateError("finalized schedule has an invalid game date")
    scoped["game_date"] = dates.dt.strftime("%Y-%m-%d")
    scoped["season_type"] = scoped["game_type"].astype(str).str.strip().str.upper()
    scoped = scoped.loc[scoped["game_date"] == target_date].copy()
    if scoped.empty:
        return None

    scoped["_week"] = pd.to_numeric(scoped["week"], errors="coerce")
    if scoped["_week"].isna().any():
        raise RefreshGateError("finalized schedule has an invalid week")
    work_items = scoped[["_week", "season_type"]].drop_duplicates().sort_values(
        ["_week", "season_type"], kind="stable"
    )
    if len(work_items) != 1:
        raise RefreshGateError("multiple finalized live refresh work items on one game date")

    week = int(work_items.iloc[0]["_week"])
    season_type = str(work_items.iloc[0]["season_type"])
    games = finalized_game_scope(
        schedule,
        year=int(season),
        week=week,
        season_types=(season_type,),
        game_date=target_date,
    )
    if games.empty:
        raise RefreshGateError("scheduled refresh scope lost all finalized games")
    return {
        "year": int(season),
        "week": week,
        "season_type": season_type,
        "game_date": target_date,
        "game_ids": sorted(games["game_id"].astype(str).tolist()),
    }


def prepare_weekly_facts(
    fetched: pd.DataFrame,
    games: pd.DataFrame,
    *,
    year: int,
    week: int,
    season_type: str = "REG",
) -> pd.DataFrame:
    """Normalize one live fetch into full-keyed rows for completed games only.

    NFLverse's player release contains a blank-ID team summary row, while its
    synthesized DST rows can lack ``game_id``.  This boundary removes the
    former and derives the latter from the finalized schedule before enforcing
    the real canonical identity.
    """
    missing = sorted(_WEEKLY_COLUMNS - set(fetched.columns))
    if missing:
        raise RefreshGateError(f"weekly source missing required columns: {', '.join(missing)}")
    game_missing = sorted({"game_id", "game_date", "away_team", "home_team"} - set(games.columns))
    if game_missing:
        raise RefreshGateError(f"finalized game scope missing columns: {', '.join(game_missing)}")
    if games["game_id"].duplicated().any():
        raise RefreshGateError("duplicate finalized game_id")

    facts = fetched.copy()
    off_scope = (
        pd.to_numeric(facts["year"], errors="coerce").ne(int(year))
        | pd.to_numeric(facts["week"], errors="coerce").ne(int(week))
    )
    if off_scope.any():
        raise RefreshGateError(f"weekly source contains {int(off_scope.sum())} rows outside year={year}, week={week}")

    # Never create a synthetic canonical identity for the provider's blank
    # aggregate row.  All actual players and DEF rows must have a stable ID.
    facts = facts.loc[~_blank(facts["NFL_player_id"])].copy()
    if facts.empty:
        raise RefreshGateError("weekly source has no rows with an NFL_player_id")

    game_pairs = pd.concat(
        [
            games[["game_id", "away_team", "home_team"]].rename(
                columns={"away_team": "_team", "home_team": "_opponent"}
            ),
            games[["game_id", "away_team", "home_team"]].rename(
                columns={"home_team": "_team", "away_team": "_opponent"}
            ),
        ],
        ignore_index=True,
    )
    game_pairs["_team_match"] = _team_match_code(game_pairs["_team"])
    game_pairs["_opponent_match"] = _team_match_code(game_pairs["_opponent"])
    if game_pairs.duplicated(["_team_match", "_opponent_match"]).any():
        raise RefreshGateError("finalized schedule has an ambiguous team/opponent game identity")

    facts["_team_match"] = _team_match_code(facts["nfl_team"])
    facts["_opponent_match"] = _team_match_code(facts["opponent_nfl_team"])
    scope_game_ids = set(games["game_id"].astype(str).str.strip())
    missing_game_id = _blank(facts["game_id"])
    direct_scope = (~missing_game_id) & facts["game_id"].astype(str).str.strip().isin(scope_game_ids)
    scope_pairs = set(map(tuple, game_pairs[["_team_match", "_opponent_match"]].to_numpy()))
    fact_pairs = pd.Series(
        list(zip(facts["_team_match"], facts["_opponent_match"])),
        index=facts.index,
    )
    inferred_scope = missing_game_id & fact_pairs.isin(scope_pairs)
    facts = facts.loc[direct_scope | inferred_scope].copy()
    if facts.empty:
        raise RefreshGateError("weekly source has no rows in the finalized game scope")

    # A weekly provider release can contain more than one already-final game.
    # Restrict it to the exact schedule scope before assigning game IDs to DST
    # rows that lack them, so a prior game's blank DST row cannot block or
    # broaden this game's replacement.
    missing_game_id = _blank(facts["game_id"])
    if missing_game_id.any():
        inferred = facts.loc[missing_game_id, ["_team_match", "_opponent_match"]].merge(
            game_pairs[["game_id", "_team_match", "_opponent_match"]],
            on=["_team_match", "_opponent_match"],
            how="left",
            validate="many_to_one",
        )
        if inferred["game_id"].isna().any():
            raise RefreshGateError("cannot derive game_id for a blank-game-id weekly row")
        facts.loc[missing_game_id, "game_id"] = inferred["game_id"].to_numpy()

    facts = facts.merge(
        games[["game_id", "game_date"]],
        on="game_id",
        how="left",
        validate="many_to_one",
    )
    if facts["game_date"].isna().any():
        unknown = sorted(facts.loc[facts["game_date"].isna(), "game_id"].astype(str).unique())
        raise RefreshGateError(f"weekly source has rows outside the finalized game scope: {unknown}")
    if _blank(facts["nfl_team"]).any() or _blank(facts["opponent_nfl_team"]).any():
        raise RefreshGateError("weekly source has a blank team or opponent")

    facts["year"] = int(year)
    facts["week"] = int(week)
    normalized_season_type = str(season_type).strip().upper()
    if normalized_season_type not in VALID_LIVE_GAME_TYPES:
        raise RefreshGateError(f"unsupported live game type: {season_type!r}")
    facts["season_type"] = normalized_season_type
    facts["player_week"] = (
        facts["NFL_player_id"].astype(str).str.strip()
        + f"_{int(year)}_{int(week)}"
    )

    for column in CANONICAL_FULL_KEY:
        if _blank(facts[column]).any():
            raise RefreshGateError(f"weekly source has a blank canonical key value in {column}")
    if facts.duplicated(list(CANONICAL_FULL_KEY)).any():
        raise RefreshGateError("duplicate canonical full key")

    return facts.drop(columns=["_team_match", "_opponent_match"], errors="ignore").sort_values(
        ["game_date", "game_id", "NFL_player_id"], kind="stable"
    ).reset_index(drop=True)


def align_facts_to_wide_schema(
    facts: pd.DataFrame,
    schema: list[tuple[str, str]],
) -> pd.DataFrame:
    """Project live facts onto the existing wide-table contract without inventing values.

    NFLverse evolves independently from the stored SuperTable.  Missing provider
    fields are represented as typed SQL NULLs when the frame is registered with
    DuckDB; silently dropping established columns would make the replacement
    artifact unsafe.  Calculated fields (LAMAR, ranks, rolling metrics) are
    supplied by their dedicated local recomputation stages, not fabricated here.
    """
    columns = [column for column, _ in schema]
    out = facts.copy()
    missing = [column for column in columns if column not in out.columns]
    if missing:
        # Construct missing columns as one frame.  Repeated DataFrame inserts
        # fragment the small (one-game) frame hundreds of times and bury the
        # worker receipt in pandas warnings.
        additions = pd.DataFrame({column: pd.NA for column in missing}, index=out.index)
        out = pd.concat([out, additions], axis=1)
    return out.reindex(columns=columns)


def _provider_identity(value: object, *, numeric: bool = True) -> str | None:
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    token = str(value).strip()
    if not numeric:
        return token
    try:
        number = Decimal(token)
        if number.is_finite() and number > 0 and number == number.to_integral_value():
            return str(int(number))
    except InvalidOperation:
        pass
    raise RefreshGateError("player bio crosswalk contains an invalid provider ID")


def build_sleeper_bio_mapping_rows(
    players: pd.DataFrame, sleeper_players: pd.DataFrame,
) -> pd.DataFrame:
    """Build only requested Sleeper mappings from agreeing, unique stable IDs.

    ``players`` is the NFLverse roster; ``sleeper_players`` is an explicitly
    selected provider subset, not a nickname/league-specific override. ESPN,
    Rotowire and GSIS anchors must be unambiguous and must not contradict one
    another. Names, teams and birthdays never establish an identity here.
    The existing population hook emits only the two mapping columns.
    """
    columns = [("NFL_player_id", "VARCHAR"), ("sleeper_player_id", "DOUBLE")]
    if "gsis_id" not in players or "player_id" not in sleeper_players:
        raise RefreshGateError("player bio crosswalk is missing identity columns")
    roster = players.copy(deep=True)
    roster["gsis_id"] = roster["gsis_id"].map(lambda v: _provider_identity(v, numeric=False))
    roster = roster[roster["gsis_id"].notna()].copy()
    anchors = ("gsis_id", "espn_id", "rotowire_id")
    indexes: dict[str, dict[str, set[str]]] = {}
    for anchor in (*anchors, "sleeper_id"):
        if anchor not in roster:
            continue
        roster[anchor] = roster[anchor].map(
            lambda v: _provider_identity(v, numeric=anchor != "gsis_id")
        )
        index: dict[str, set[str]] = {}
        for value, gsis in zip(roster[anchor], roster["gsis_id"]):
            if value is not None:
                index.setdefault(value, set()).add(gsis)
        indexes[anchor] = index
    resolved: dict[str, str] = {}
    seen_provider_ids: set[str] = set()
    for provider in sleeper_players.to_dict("records"):
        provider_id = _provider_identity(provider.get("player_id"))
        if provider_id is None or provider_id in seen_provider_ids:
            raise RefreshGateError("player bio crosswalk requires unique nonempty provider IDs")
        seen_provider_ids.add(provider_id)
        candidates: set[str] = set()
        supplied: dict[str, str] = {}
        for anchor in anchors:
            value = _provider_identity(provider.get(anchor), numeric=anchor != "gsis_id")
            if value is None:
                continue
            supplied[anchor] = value
            matches = indexes.get(anchor, {}).get(value, set())
            if len(matches) > 1:
                raise RefreshGateError(f"ambiguous player bio crosswalk: {anchor}")
            candidates.update(matches)
        if len(candidates) != 1:
            raise RefreshGateError(
                "player bio crosswalk has missing or disagreeing stable anchors "
                f"for provider_id={provider_id}; supplied={supplied}; "
                f"candidate_gsis={sorted(candidates)}"
            )
        (gsis,) = candidates
        if gsis in resolved:
            raise RefreshGateError("multiple Sleeper players claim one NFL identity")
        target = roster[roster["gsis_id"] == gsis]
        for anchor, value in supplied.items():
            if anchor in target and any(v != value for v in target[anchor].dropna()):
                raise RefreshGateError(f"conflicting player bio crosswalk: {anchor}")
        if "sleeper_id" in target and any(v != provider_id for v in target["sleeper_id"].dropna()):
            raise RefreshGateError("NFL roster already has a conflicting Sleeper ID")
        if indexes.get("sleeper_id", {}).get(provider_id, set()) - {gsis}:
            raise RefreshGateError("Sleeper ID already belongs to another NFL identity")
        resolved[gsis] = provider_id
    selected = roster[roster["gsis_id"].isin(resolved)].drop_duplicates("gsis_id").copy()
    selected["sleeper_id"] = selected["gsis_id"].map(resolved)
    facts = pd.DataFrame(columns=["NFL_player_id", "player", "position", "nfl_team"])
    return build_player_bio_rows(facts, selected, columns)


def build_player_bio_rows(
    facts: pd.DataFrame,
    players: pd.DataFrame,
    bio_schema: list[tuple[str, str]],
) -> pd.DataFrame:
    """Map NFLverse's player directory into the stored player-bio contract.

    This function supplies rows for finalized-game players and every player on
    the current NFLverse roster. Rostered rookies must resolve before they
    appear in a finalized stat feed. The caller upserts authoritative directory
    values while retaining an established value when NFLverse does not publish
    a replacement.
    """
    required_facts = {"NFL_player_id", "player", "position", "nfl_team"}
    missing = sorted(required_facts - set(facts.columns))
    if missing:
        raise RefreshGateError(f"facts missing player bio columns: {', '.join(missing)}")
    if "gsis_id" not in players.columns:
        raise RefreshGateError("NFLverse player directory has no gsis_id")

    columns = [name for name, _ in bio_schema]
    source = players.dropna(subset=["gsis_id"]).drop_duplicates("gsis_id", keep="last").copy()
    source["gsis_id"] = source["gsis_id"].astype(str)
    lookup = source.set_index("gsis_id", drop=False)
    fact_ids = facts.dropna(subset=["NFL_player_id"]).drop_duplicates("NFL_player_id", keep="last")
    fact_lookup = fact_ids.set_index("NFL_player_id", drop=False)
    player_ids = list(dict.fromkeys([*source["gsis_id"].tolist(), *fact_lookup.index.tolist()]))
    rows: list[dict[str, object]] = []
    source_map = {
        "player": ("full_name", "display_name"),
        "nfl_position": ("position",),
        "headshot_url": ("headshot_url", "headshot"),
        "height": ("height",),
        "weight": ("weight",),
        "college": ("college", "college_name"),
        "conference": ("conference", "college_conference"),
        "draft_year": ("draft_year",),
        "draft_round": ("draft_round",),
        "draft_overall": ("draft_number", "draft_pick"),
        "status": ("status",),
        "rookie_year": ("rookie_year", "rookie_season", "entry_year"),
        "first_year": ("rookie_year", "rookie_season", "entry_year"),
        "last_year": ("last_year", "last_season"),
        "pfr_id": ("pfr_id",),
        "birth_date": ("birth_date",),
        "sleeper_player_id": ("sleeper_id",),
        "yahoo_player_id": ("yahoo_id",),
    }
    for raw_player_id in player_ids:
        player_id = str(raw_player_id).strip()
        if not player_id:
            continue
        directory = lookup.loc[player_id] if player_id in lookup.index else None
        fact = fact_lookup.loc[player_id] if player_id in fact_lookup.index else None
        row = {column: pd.NA for column in columns}
        if "NFL_player_id" in row:
            row["NFL_player_id"] = player_id
        if "player" in row and fact is not None:
            row["player"] = fact["player"]
        if "nfl_position" in row and fact is not None:
            row["nfl_position"] = fact["position"]
        if "latest_team" in row and fact is not None:
            row["latest_team"] = _historical_team(fact["nfl_team"])
        if "primary_team" in row and fact is not None:
            row["primary_team"] = _historical_team(fact["nfl_team"])
        if directory is not None:
            for target, source_columns in source_map.items():
                if target not in row:
                    continue
                for source_column in source_columns:
                    if source_column in directory.index and pd.notna(directory[source_column]):
                        value = directory[source_column]
                        if target in {"sleeper_player_id", "yahoo_player_id"}:
                            numeric_value = pd.to_numeric(value, errors="coerce")
                            if pd.isna(numeric_value):
                                continue
                            value = int(numeric_value)
                        row[target] = value
                        break
            for target in ("latest_team", "nfl_draft_team", "primary_team"):
                source_columns = ("team", "latest_team") if target in {"latest_team", "primary_team"} else ("draft_team",)
                if target not in row:
                    continue
                for source_column in source_columns:
                    if source_column in directory.index and pd.notna(directory[source_column]):
                        row[target] = _historical_team(directory[source_column])
                        break
            if "espn_id" in row and "espn_id" in directory.index and pd.notna(directory["espn_id"]):
                row["espn_id"] = str(int(directory["espn_id"]))
            if "position_category" in row and "position_group" in directory.index:
                row["position_category"] = directory["position_group"]
            if "years_active" in row and "years_of_experience" in directory.index and pd.notna(directory["years_of_experience"]):
                row["years_active"] = float(directory["years_of_experience"]) + 1.0
            if "is_undrafted" in row and "draft_year" in directory.index:
                row["is_undrafted"] = float(pd.isna(directory["draft_year"]))
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def replace_completed_game_rows(
    con: duckdb.DuckDBPyConnection,
    table: str,
    facts: pd.DataFrame,
    games: pd.DataFrame,
    *,
    year: int,
    week: int,
) -> int:
    """Atomically replace only the completed game pairs represented by ``facts``.

    The baseline SuperTable predates a durable ``game_id`` column, so deletion
    is intentionally keyed by the full team/opponent game identity plus the
    requested year/week.  This means an opener rerun cannot erase another game
    in the same week.  Inputs must already have passed ``prepare_weekly_facts``.
    """
    target = _qident(table)
    required = set(CANONICAL_FULL_KEY) | {"year"}
    missing = sorted(required - set(facts.columns))
    if missing:
        raise RefreshGateError(f"facts missing required replacement columns: {', '.join(missing)}")
    if facts.empty:
        raise RefreshGateError("refusing to replace a completed-game scope with zero facts")
    if facts.duplicated(list(CANONICAL_FULL_KEY)).any():
        raise RefreshGateError("duplicate canonical full key")

    expected_pairs = pd.concat(
        [
            games[["away_team", "home_team"]].rename(columns={"away_team": "nfl_team", "home_team": "opponent_nfl_team"}),
            games[["away_team", "home_team"]].rename(columns={"home_team": "nfl_team", "away_team": "opponent_nfl_team"}),
        ],
        ignore_index=True,
    )
    expected_pairs["_team_match"] = _team_match_code(expected_pairs["nfl_team"])
    expected_pairs["_opponent_match"] = _team_match_code(expected_pairs["opponent_nfl_team"])
    observed_pairs = facts[["nfl_team", "opponent_nfl_team"]].drop_duplicates().copy()
    observed_pairs["_team_match"] = _team_match_code(observed_pairs["nfl_team"])
    observed_pairs["_opponent_match"] = _team_match_code(observed_pairs["opponent_nfl_team"])
    expected_keys = set(map(tuple, expected_pairs[["_team_match", "_opponent_match"]].to_numpy()))
    observed_keys = set(map(tuple, observed_pairs[["_team_match", "_opponent_match"]].to_numpy()))
    if expected_keys != observed_keys:
        raise RefreshGateError("facts do not cover exactly the finalized game team/opponent scope")

    target_columns = [row[0] for row in con.execute(f"DESCRIBE {target}").fetchall()]
    if set(facts.columns) != set(target_columns):
        extra = sorted(set(facts.columns) - set(target_columns))
        missing_target = sorted(set(target_columns) - set(facts.columns))
        raise RefreshGateError(
            f"facts do not exactly match target schema: extra={extra[:8]}, missing={missing_target[:8]}"
        )
    ordered_facts = facts.reindex(columns=target_columns)
    scope = observed_pairs[["nfl_team", "opponent_nfl_team"]]
    con.register("_live_refresh_facts", ordered_facts)
    con.register("_live_refresh_scope", scope)
    columns_sql = ", ".join(_qident(column) for column in target_columns)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(
            f"""
            DELETE FROM {target} AS current
            WHERE current.year = ?
              AND current.week = ?
              AND EXISTS (
                SELECT 1
                FROM _live_refresh_scope AS scope
                WHERE scope.nfl_team = current.nfl_team
                  AND scope.opponent_nfl_team = current.opponent_nfl_team
              )
            """,
            [int(year), int(week)],
        )
        con.execute(f"INSERT INTO {target} ({columns_sql}) SELECT {columns_sql} FROM _live_refresh_facts")
        inserted = int(con.execute("SELECT COUNT(*) FROM _live_refresh_facts").fetchone()[0])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("_live_refresh_facts")
        con.unregister("_live_refresh_scope")
    return inserted


def rebuild_wide_rank_surface(
    con: duckdb.DuckDBPyConnection,
    *,
    year: int,
    week: int,
    target_schema: str = "nfl_historical",
) -> None:
    """Rebuild the wide table's derived rank families from its full history.

    The live fetch plane has only the current NFL week in memory.  Its
    all-time fields are therefore not publishable directly: a one-week frame
    makes the week's leader rank first in history.  Reuse the canonical
    sidecar rules against the complete local artifact, then materialize their
    compatibility view back into the wide table before the artifact can be
    promoted.  This runs once per NFL ops refresh, never per league update.
    """
    from multi_league.data_fetchers.build_full_ops import (
        COMPAT_VIEW,
        SIDECAR_TABLES,
        create_compatibility_view,
        rebuild_career_sidecar,
        rebuild_season_partitions,
        rebuild_weekly_rank_partition,
        split_wide_to_sidecars,
    )

    wide = f"{target_schema}.nfl_player_stats_all"
    # Vertical sidecars deliberately preserve every source row, including
    # historic duplicate player_week records that cannot safely be collapsed
    # while an active weekly correction is being prepared.
    split_wide_to_sidecars(con, source_ref=wide, target_schema=target_schema, grain="vertical")
    rebuild_weekly_rank_partition(con, int(year), int(week), target_schema=target_schema)
    # Season scoped fields include the prior season's next-year values.  Match
    # the canonical weekly repair scope so a live 2026 update cannot leave
    # 2025's ``avg_pts_next_year_*`` family stale.
    base = f'{target_schema}.{SIDECAR_TABLES["base"]}'
    available_years = {
        int(row[0])
        for row in con.execute(f"SELECT DISTINCT CAST(year AS INTEGER) FROM {base}").fetchall()
        if row[0] is not None
    }
    season_years = [candidate for candidate in (int(year) - 1, int(year)) if candidate in available_years]
    if not season_years:
        raise RefreshGateError(f"rank rebuild found no base rows for {year}")
    rebuild_season_partitions(con, season_years, target_schema=target_schema)
    rebuild_career_sidecar(con, target_schema=target_schema)
    create_compatibility_view(
        con,
        source_ref=wide,
        target_schema=target_schema,
        view_name=COMPAT_VIEW,
        grain="vertical",
    )
    compat = f"{target_schema}.{COMPAT_VIEW}"
    try:
        con.execute(f"CREATE OR REPLACE TABLE {wide} AS SELECT * FROM {compat}")
    finally:
        con.execute(f"DROP VIEW IF EXISTS {compat}")
        for table in SIDECAR_TABLES.values():
            con.execute(f"DROP TABLE IF EXISTS {target_schema}.{table}")


def assert_complete_ops_artifact(path: Path | str, *, schema: str = "nfl_historical") -> dict[str, int]:
    """Prove a replacement file contains every live public NFL operations table."""
    artifact = Path(path)
    if not artifact.is_file():
        raise RefreshGateError(f"ops artifact does not exist: {artifact}")
    con = duckdb.connect(str(artifact), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ? AND table_type = 'BASE TABLE'
            """,
            [schema],
        ).fetchall()
        available = {str(row[0]) for row in rows}
        missing = sorted(set(REQUIRED_OPS_TABLES) - available)
        if missing:
            raise RefreshGateError(f"missing required ops tables: {', '.join(missing)}")
        counts = {
            name: int(con.execute(f"SELECT COUNT(*) FROM {_qident(schema)}.{_qident(name)}").fetchone()[0])
            for name in REQUIRED_OPS_TABLES
        }
        empty = sorted(name for name, count in counts.items() if count <= 0)
        if empty:
            raise RefreshGateError(f"required ops tables are empty: {', '.join(empty)}")
        return counts
    finally:
        con.close()


def assert_ops_schema_compatible(
    reference_artifact: Path | str,
    candidate_artifact: Path | str,
    *,
    schema: str = "nfl_historical",
) -> dict[str, int]:
    """Reject a candidate that cannot replace the live artifact's contract.

    The worker replaces one standalone DuckDB database, so a rebuilt rollup
    may not silently shed a historical column even when its row count looks
    reasonable.  Candidate-only columns are allowed here; the local rollup
    restore step removes them before promotion.  The important fail-closed
    condition is that every established column retains its DuckDB type.
    """
    assert_complete_ops_artifact(reference_artifact, schema=schema)
    assert_complete_ops_artifact(candidate_artifact, schema=schema)
    reference = duckdb.connect(str(reference_artifact), read_only=True)
    candidate = duckdb.connect(str(candidate_artifact), read_only=True)
    try:
        compatibility: dict[str, int] = {}
        for table in REQUIRED_OPS_TABLES:
            expected = dict(_schema_columns(reference, f"{schema}.{table}"))
            actual = dict(_schema_columns(candidate, f"{schema}.{table}"))
            missing = sorted(set(expected) - set(actual))
            changed = sorted(
                column
                for column in set(expected) & set(actual)
                if expected[column] != actual[column]
            )
            if missing:
                raise RefreshGateError(
                    f"{table} would drop existing columns: {', '.join(missing)}"
                )
            if changed:
                details = ", ".join(
                    f"{column} ({expected[column]} -> {actual[column]})" for column in changed
                )
                raise RefreshGateError(f"{table} would change existing column types: {details}")
            compatibility[table] = len(expected)
        return compatibility
    finally:
        candidate.close()
        reference.close()


def _schema_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return [(str(row[0]), str(row[1])) for row in con.execute(f"DESCRIBE {_qident(table)}").fetchall()]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _add_franchise_numbers(facts: pd.DataFrame) -> pd.DataFrame:
    """Populate stable franchise identities before team-scoped aggregates are built."""
    from nfl_data.nfl_franchises import get_nfl_franchise_number

    out = facts.copy()
    for team_col, target_col in (
        ("nfl_team", "nfl_franchise_number"),
        ("opponent_nfl_team", "opponent_nfl_franchise_number"),
    ):
        out[target_col] = [
            get_nfl_franchise_number(str(team), int(year)) if pd.notna(team) and pd.notna(year) else None
            for team, year in zip(out[team_col], out["year"], strict=True)
        ]
        if out[target_col].isna().any():
            raise RefreshGateError(f"could not derive {target_col} for every live fact")
    return out


def upsert_player_bio_rows(
    con: duckdb.DuckDBPyConnection,
    rows: pd.DataFrame,
    *,
    table: str = "nfl_historical.player_bio",
    mapping_only: bool = False,
) -> tuple[int, int]:
    """Refresh finalized-game bios without erasing fields absent from NFLverse.

    The directory, rather than the weekly stat fetch, is the authoritative
    source for player metadata.  Updating identities represented in a final
    game keeps ``player_bio`` and the aggregate headshot/name fields coherent;
    it also means a rookie's first appearance does not require a separate
    backfill.  Null source fields intentionally retain their existing value.

    ``mapping_only`` instead accepts just NFL_player_id/sleeper_player_id from
    ``build_sleeper_bio_mapping_rows``. It fills absent links on existing bios,
    rejects both directions of identity conflict, and never changes metadata.
    """
    if mapping_only:
        return _upsert_sleeper_bio_mappings(con, rows, table=table)
    if rows.empty:
        return 0, 0
    target = _qident(table)
    schema = _schema_columns(con, table)
    columns = [name for name, _ in schema]
    if list(rows.columns) != columns:
        raise RefreshGateError("player bio rows do not match the existing bio schema")
    source = rows.dropna(subset=["NFL_player_id"]).copy()
    source["NFL_player_id"] = source["NFL_player_id"].astype(str).str.strip()
    source = source[source["NFL_player_id"] != ""].copy()
    if source["NFL_player_id"].duplicated().any():
        raise RefreshGateError("player bio refresh contains duplicate NFL_player_id values")
    if source.empty:
        return 0, 0
    con.register("_live_refresh_bio", source)
    try:
        updated = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM _live_refresh_bio AS source
                JOIN {target} AS existing
                  ON existing."NFL_player_id" = source."NFL_player_id"
                """
            ).fetchone()[0]
        )
        inserted = len(source) - updated
        quoted = ", ".join(_qident(column) for column in columns)
        select_sql = []
        for column, data_type in schema:
            field = _qident(column)
            if column == "NFL_player_id":
                value = f"CAST(source.{field} AS {data_type})"
            else:
                value = f"COALESCE(CAST(source.{field} AS {data_type}), existing.{field})"
            select_sql.append(f"{value} AS {field}")
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _live_refresh_bio_merged AS
            SELECT {', '.join(select_sql)}
            FROM _live_refresh_bio AS source
            LEFT JOIN {target} AS existing
              ON existing."NFL_player_id" = source."NFL_player_id"
            """
        )
        merged = int(con.execute("SELECT COUNT(*) FROM _live_refresh_bio_merged").fetchone()[0])
        if merged != len(source):
            raise RefreshGateError("player bio upsert lost source identities during merge")
        con.execute("BEGIN TRANSACTION")
        con.execute(
            f"""
            DELETE FROM {target}
            WHERE "NFL_player_id" IN (
                SELECT "NFL_player_id" FROM _live_refresh_bio_merged
            )
            """
        )
        con.execute(
            f"INSERT INTO {target} ({quoted}) SELECT {quoted} FROM _live_refresh_bio_merged"
        )
        con.execute("COMMIT")
        return inserted, updated
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.execute("DROP TABLE IF EXISTS _live_refresh_bio_merged")
        con.unregister("_live_refresh_bio")


def _upsert_sleeper_bio_mappings(
    con: duckdb.DuckDBPyConnection, rows: pd.DataFrame, *, table: str,
) -> tuple[int, int]:
    """Fill absent IDs in the local bio cache using one guarded transaction."""
    sql = _sleeper_bio_mapping_sql(rows, table=table)
    if not sql:
        return 0, 0
    try:
        for statement in sql.split(";"):
            if statement.strip():
                result = con.execute(statement)
        inserted, updated = result.fetchone()
        return int(inserted), int(updated)
    except duckdb.Error as exc:
        try:
            con.execute("ROLLBACK")
        except duckdb.TransactionException:
            pass
        raise RefreshGateError(f"mapping-only transaction rejected: {exc}") from exc


def _sleeper_bio_mapping_sql(rows: pd.DataFrame, *, table: str) -> str:
    """Compile the guarded local-cache mapping transaction."""
    if list(rows.columns) != ["NFL_player_id", "sleeper_player_id"]:
        raise RefreshGateError("mapping-only rows must contain exactly the two identity columns")
    source = rows.copy(deep=True)
    source["NFL_player_id"] = source["NFL_player_id"].map(lambda v: _provider_identity(v, numeric=False))
    source["sleeper_player_id"] = source["sleeper_player_id"].map(_provider_identity)
    if source.isna().any().any() or any(source[c].duplicated().any() for c in source):
        raise RefreshGateError("mapping-only rows must be complete and one-to-one")
    if source.empty:
        return ""
    if ";" in table or source["NFL_player_id"].str.contains(";", regex=False).any():
        raise RefreshGateError("mapping identity is incompatible with the SQL script transport")
    target = _qident(table)
    values = ", ".join(
        "('" + nfl_id.replace("'", "''") + "', " + provider_id + ")"
        for nfl_id, provider_id in source.itertuples(index=False, name=None)
    )
    return f"""
        BEGIN TRANSACTION;
        CREATE OR REPLACE TEMP TABLE _live_refresh_bio_mappings AS
            SELECT * FROM (VALUES {values}) AS v(NFL_player_id, sleeper_player_id);
        SELECT CASE WHEN EXISTS (
            SELECT source.NFL_player_id
            FROM _live_refresh_bio_mappings source
            LEFT JOIN {target} existing USING (NFL_player_id)
            GROUP BY source.NFL_player_id
            HAVING COUNT(existing.NFL_player_id) <> 1
        ) THEN error('mapping-only target is missing or ambiguous') ELSE true END;
        SELECT CASE WHEN EXISTS (
            SELECT source.NFL_player_id
            FROM _live_refresh_bio_mappings source
            JOIN {target} existing
              ON existing.NFL_player_id = source.NFL_player_id
              OR existing.sleeper_player_id = CAST(source.sleeper_player_id AS DOUBLE)
            WHERE (existing.NFL_player_id IS DISTINCT FROM source.NFL_player_id)
               OR (existing.sleeper_player_id IS NOT NULL
                   AND existing.sleeper_player_id <> CAST(source.sleeper_player_id AS DOUBLE))
        ) THEN error('mapping-only update conflicts with existing provider identities') ELSE true END;
        CREATE OR REPLACE TEMP TABLE _live_refresh_bio_receipt AS
            SELECT 0 AS inserted, COUNT(*) AS updated
            FROM {target} existing JOIN _live_refresh_bio_mappings source USING (NFL_player_id)
            WHERE existing.sleeper_player_id IS NULL;
            UPDATE {target} AS existing
            SET sleeper_player_id = CAST(source.sleeper_player_id AS DOUBLE)
            FROM _live_refresh_bio_mappings source
            WHERE existing.NFL_player_id = source.NFL_player_id
              AND existing.sleeper_player_id IS NULL
            ;
        COMMIT;
        SELECT inserted, updated FROM _live_refresh_bio_receipt;
    """


def _hydrate_primary_position(
    facts: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
    *,
    bio_table: str = "nfl_historical.player_bio",
) -> pd.DataFrame:
    """Use the bio dimension for stable player position, with weekly position as a fallback."""
    out = facts.copy()
    bio = con.execute(
        f"SELECT \"NFL_player_id\", nfl_position FROM {_qident(bio_table)} WHERE \"NFL_player_id\" IS NOT NULL"
    ).fetchdf()
    positions = bio.drop_duplicates("NFL_player_id", keep="last").set_index("NFL_player_id")["nfl_position"]
    out["primary_position"] = out["NFL_player_id"].astype(str).map(positions).fillna(out["position"])
    if _blank(out["primary_position"]).any():
        raise RefreshGateError("cannot establish primary_position for every live fact")
    return out


_ROLLUP_CONTRACT_KEYS = {
    "player_nfl_season": ("NFL_player_id", "year"),
    "player_nfl_season_all": ("NFL_player_id", "year"),
    "player_nfl_career": ("NFL_player_id",),
    "player_nfl_career_all": ("NFL_player_id",),
    "player_nfl_season_team": (
        "NFL_player_id",
        "year",
        "nfl_team",
        "nfl_franchise_number",
        "season_type",
    ),
    "player_nfl_season_team_all": (
        "NFL_player_id",
        "year",
        "nfl_team",
        "nfl_franchise_number",
        "season_type",
    ),
}


def _assert_unique_rollup_keys(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    keys: tuple[str, ...],
) -> None:
    key_sql = ", ".join(_qident(key) for key in keys)
    duplicate_count = int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {key_sql}
                FROM {_qident(relation)}
                GROUP BY {key_sql}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_count:
        raise RefreshGateError(
            f"{relation} has {duplicate_count} duplicate rollup key(s); cannot preserve its contract"
        )


def _restore_reference_rollup_contract(
    con: duckdb.DuckDBPyConnection,
    *,
    stage_schema: str,
    baseline_database: str,
) -> None:
    """Restore omitted historical fields while retaining the newly built rollups.

    The general aggregate builder intentionally creates only fields it can
    derive from the weekly fact table.  The deployed historical tables also
    carry presentation/ranking fields outside that generic builder.  For old
    keys we retain those values from the baseline; for 2026-only keys they are
    null until a dedicated derivation owns them.  This preserves both the
    exact table schema and the historical values a full-file replacement must
    not erase.
    """
    candidate_prefix = "___ops.nfl_historical"
    baseline_prefix = f"{baseline_database}.nfl_historical"
    stage_prefix = f"___ops.{stage_schema}"
    for table, keys in _ROLLUP_CONTRACT_KEYS.items():
        candidate_relation = f"{candidate_prefix}.{table}"
        baseline_relation = f"{baseline_prefix}.{table}"
        stage_relation = f"{stage_prefix}.restore_{table}"
        reference_schema = _schema_columns(con, baseline_relation)
        candidate_columns = {name for name, _ in _schema_columns(con, candidate_relation)}
        reference_columns = {name for name, _ in reference_schema}
        required_keys = set(keys)
        if not required_keys <= candidate_columns or not required_keys <= reference_columns:
            missing_candidate = sorted(required_keys - candidate_columns)
            missing_baseline = sorted(required_keys - reference_columns)
            raise RefreshGateError(
                f"{table} is missing rollup keys "
                f"(candidate={missing_candidate}, baseline={missing_baseline})"
            )
        _assert_unique_rollup_keys(con, candidate_relation, keys)
        _assert_unique_rollup_keys(con, baseline_relation, keys)
        join_sql = " AND ".join(
            f"new.{_qident(key)} IS NOT DISTINCT FROM old.{_qident(key)}" for key in keys
        )
        select_sql = []
        for column, data_type in reference_schema:
            output_column = _qident(column)
            if column in candidate_columns:
                value = f"CAST(new.{output_column} AS {data_type})"
            else:
                value = f"old.{output_column}"
            select_sql.append(f"{value} AS {output_column}")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {_qident(stage_relation)} AS
            SELECT {', '.join(select_sql)}
            FROM {_qident(candidate_relation)} AS new
            LEFT JOIN {_qident(baseline_relation)} AS old
              ON {join_sql}
            """
        )
        restored_rows = int(con.execute(f"SELECT COUNT(*) FROM {_qident(stage_relation)}").fetchone()[0])
        candidate_rows = int(con.execute(f"SELECT COUNT(*) FROM {_qident(candidate_relation)}").fetchone()[0])
        if restored_rows != candidate_rows:
            raise RefreshGateError(
                f"{table} contract restore changed its row count ({candidate_rows} -> {restored_rows})"
            )
        con.execute(
            f"CREATE OR REPLACE TABLE {_qident(candidate_relation)} AS "
            f"SELECT * FROM {_qident(stage_relation)}"
        )
        con.execute(f"DROP TABLE {_qident(stage_relation)}")


def _rebuild_local_rollups(artifact: Path, baseline_artifact: Path) -> dict[str, int]:
    """Run the production aggregate SQL against the local candidate artifact only."""
    from multi_league.data_fetchers import aggregate_nfl_stats_fly as aggregate
    from multi_league.data_fetchers.aggregate_nfl_stats_fly import LocalDuckDBWriter
    from scripts.build_research_team_aggregates import build_table_into_relation

    con = duckdb.connect()
    stage_schema = "_live_refresh_stage"
    artifact_sql = artifact.as_posix().replace("'", "''")
    baseline_sql = baseline_artifact.as_posix().replace("'", "''")
    baseline_database = "_live_refresh_baseline"
    con.execute(f"ATTACH '{artifact_sql}' AS ___ops")
    con.execute(f"ATTACH '{baseline_sql}' AS {_qident(baseline_database)} (READ_ONLY)")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS ___ops.{_qident(stage_schema)}")
    original_public = aggregate.PUBLIC
    original_rollup_date = aggregate.ROLLUP_DATE
    aggregate.PUBLIC = f"___ops.{stage_schema}"
    aggregate.ROLLUP_DATE = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    try:
        counts = aggregate.update_aggregates(
            writer=LocalDuckDBWriter(con),
            make_backups=False,
            rebuild_all_years=True,
        )
        weekly_columns = {
            row[0]
            for row in con.execute("DESCRIBE ___ops.nfl_historical.nfl_player_stats_all").fetchall()
        }
        season_columns = con.execute("DESCRIBE ___ops.nfl_historical.player_nfl_season").fetchall()
        counts["season_team"] = build_table_into_relation(
            con,
            source_ref="___ops.nfl_historical.nfl_player_stats_all",
            target_ref="___ops.nfl_historical.player_nfl_season_team",
            include_postseason=False,
            weekly_columns=weekly_columns,
            season_columns=season_columns,
        )
        counts["season_team_all"] = build_table_into_relation(
            con,
            source_ref="___ops.nfl_historical.nfl_player_stats_all",
            target_ref="___ops.nfl_historical.player_nfl_season_team_all",
            include_postseason=True,
            weekly_columns=weekly_columns,
            season_columns=season_columns,
        )
        _restore_reference_rollup_contract(
            con,
            stage_schema=stage_schema,
            baseline_database=baseline_database,
        )
        con.execute(f"DROP SCHEMA ___ops.{_qident(stage_schema)} CASCADE")
        return {name: int(count) for name, count in counts.items()}
    finally:
        aggregate.PUBLIC = original_public
        aggregate.ROLLUP_DATE = original_rollup_date
        con.close()


def refresh_local_ops_artifact(
    *,
    base_artifact: Path | str,
    output_artifact: Path | str,
    facts: pd.DataFrame,
    games: pd.DataFrame,
    players: pd.DataFrame,
    year: int,
    week: int,
) -> dict[str, object]:
    """Build and verify a complete replacement artifact without contacting Fly.

    The only mutation is the output DuckDB file.  It is safe to run this step
    on a GitHub runner because Fly promotion is intentionally a separate,
    explicit step after all eight local tables have been rebuilt and checked.
    """
    base = Path(base_artifact)
    output = Path(output_artifact)
    assert_complete_ops_artifact(base)
    if base.resolve() != output.resolve():
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(base, output)

    con = duckdb.connect(str(output))
    try:
        wide_schema = _schema_columns(con, "nfl_historical.nfl_player_stats_all")
        bio_schema = _schema_columns(con, "nfl_historical.player_bio")
        enriched = _add_franchise_numbers(facts)
        bio_rows = build_player_bio_rows(enriched, players, bio_schema)
        bios_inserted, bios_updated = upsert_player_bio_rows(con, bio_rows)
        enriched = _hydrate_primary_position(enriched, con)
        aligned = align_facts_to_wide_schema(enriched, wide_schema)
        rows_replaced = replace_completed_game_rows(
            con,
            "nfl_historical.nfl_player_stats_all",
            aligned,
            games,
            year=year,
            week=week,
        )
        rebuild_wide_rank_surface(con, year=year, week=week)
        # Recompute every supported live-week LAMAR rule over the final local
        # weekly facts.  The regular rank/PPG fetch-plane enrichments are
        # already on ``facts``; LAMAR must use the merged artifact's schema.
        from multi_league.data_fetchers.research_lamar import enrich_research_lamar

        enrich_research_lamar(con, table_name="nfl_historical.nfl_player_stats_all", year=year)
    finally:
        con.close()

    rollups = _rebuild_local_rollups(output, base)
    counts = assert_complete_ops_artifact(output)
    schema_columns = assert_ops_schema_compatible(base, output)
    digest = _sha256_file(output)
    return {
        "artifact": str(output),
        "sha256": digest,
        "year": int(year),
        "week": int(week),
        "weekly_rows_replaced": rows_replaced,
        "player_bio_rows_inserted": bios_inserted,
        "player_bio_rows_updated": bios_updated,
        "table_counts": counts,
        "schema_columns": schema_columns,
        "rollups": rollups,
        "built_at": datetime.now(UTC).isoformat(),
    }


def write_refresh_manifest(result: dict[str, object], path: Path | str) -> None:
    """Write a small portable receipt alongside the candidate artifact."""
    manifest = Path(path)
    manifest.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
