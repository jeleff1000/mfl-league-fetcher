"""
Playoff Bracket Utilities

Shared utilities for playoff bracket simulation.
Contains settings loading, bracket validation, and helper functions.
"""

import math
import json
from pathlib import Path
import pandas as pd

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from core.data_normalization import find_league_settings_directory


REQUIRED_PLAYOFF_FIELDS = {
    "playoff_start_week": ("playoff_start_week",),
    "num_playoff_teams": ("num_playoff_teams", "playoff_teams"),
    "bye_teams": ("bye_teams",),
    "num_teams": ("num_teams",),
}


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _get_first_present(values, keys):
    for key in keys:
        if key in values:
            value = values.get(key)
            if not _is_missing(value):
                return value
    return None


def _coerce_required_int(value, field: str, year: int, source: str) -> int:
    if _is_missing(value):
        raise ValueError(f"Missing required playoff setting '{field}' for year {year} in {source}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid playoff setting '{field}' for year {year} in {source}: {value!r}") from exc


def _coerce_optional_int(value, field: str, year: int, source: str, default: int = 0) -> int:
    if _is_missing(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid playoff setting '{field}' for year {year} in {source}: {value!r}") from exc


def _coerce_optional_bool(value, field: str, year: int, source: str, default: bool = False) -> bool:
    if _is_missing(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
    raise ValueError(f"Invalid playoff setting '{field}' for year {year} in {source}: {value!r}")


def _normalize_playoff_settings(values, year: int, source: str) -> dict:
    playoff_start = _coerce_required_int(
        _get_first_present(values, REQUIRED_PLAYOFF_FIELDS["playoff_start_week"]),
        "playoff_start_week",
        year,
        source,
    )
    num_playoff = _coerce_required_int(
        _get_first_present(values, REQUIRED_PLAYOFF_FIELDS["num_playoff_teams"]),
        "num_playoff_teams",
        year,
        source,
    )
    bye_teams = _coerce_required_int(
        _get_first_present(values, REQUIRED_PLAYOFF_FIELDS["bye_teams"]),
        "bye_teams",
        year,
        source,
    )
    num_teams = _coerce_required_int(
        _get_first_present(values, REQUIRED_PLAYOFF_FIELDS["num_teams"]),
        "num_teams",
        year,
        source,
    )

    has_multiweek = _coerce_optional_int(
        _get_first_present(values, ("has_multiweek_championship",)),
        "has_multiweek_championship",
        year,
        source,
        default=0,
    )
    playoff_round_type = _coerce_optional_int(
        _get_first_present(values, ("playoff_round_type", "sleeper_playoff_type")),
        "playoff_round_type",
        year,
        source,
        default=0,
    )
    if playoff_round_type in (1, 2):
        has_multiweek = 1
    elif playoff_round_type == 0 and has_multiweek == 1:
        playoff_round_type = 2

    stored_end_week = _get_first_present(values, ("end_week",))
    if _is_missing(stored_end_week):
        playoff_rounds = calculate_playoff_rounds_from_bracket(num_playoff)
        if playoff_round_type == 1:
            end_week = playoff_start + (2 * playoff_rounds) - 1
        elif playoff_round_type == 2:
            end_week = playoff_start + playoff_rounds
        else:
            end_week = playoff_start + playoff_rounds - 1
    else:
        end_week = _coerce_required_int(stored_end_week, "end_week", year, source)

    config = {
        "playoff_start_week": playoff_start,
        "num_playoff_teams": num_playoff,
        "bye_teams": bye_teams,
        "has_multiweek_championship": has_multiweek,
        "playoff_round_type": playoff_round_type,
        "uses_playoff_reseeding": _coerce_optional_int(
            _get_first_present(values, ("uses_playoff_reseeding",)),
            "uses_playoff_reseeding",
            year,
            source,
            default=0,
        ),
        "num_teams": num_teams,
        "end_week": end_week,
        "uses_median": _coerce_optional_bool(
            _get_first_present(values, ("uses_median",)),
            "uses_median",
            year,
            source,
            default=False,
        ),
    }

    playoff_seeding_rule = _get_first_present(values, ("playoff_seeding_rule", "playoffSeedingRule"))
    if not _is_missing(playoff_seeding_rule):
        config["playoff_seeding_rule"] = str(playoff_seeding_rule)

    playoff_seeding_rule_by = _get_first_present(values, ("playoff_seeding_rule_by", "playoffSeedingRuleBy"))
    if not _is_missing(playoff_seeding_rule_by):
        config["playoff_seeding_rule_by"] = _coerce_optional_int(
            playoff_seeding_rule_by,
            "playoff_seeding_rule_by",
            year,
            source,
            default=0,
        )

    is_valid, error_msg = validate_bracket_structure(
        config["num_playoff_teams"], config["bye_teams"], config["num_teams"]
    )
    if not is_valid:
        raise ValueError(f"Invalid playoff settings for year {year} in {source}: {error_msg}")

    return config


def _require_setting_int(settings: dict, field: str) -> int:
    if field not in settings or _is_missing(settings.get(field)):
        raise ValueError(f"Missing required playoff setting '{field}'")
    try:
        return int(settings[field])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid playoff setting '{field}': {settings[field]!r}") from exc


def validate_bracket_structure(num_playoff_teams: int, bye_teams: int, num_teams: int) -> tuple[bool, str]:
    """
    Validate that playoff settings produce a valid bracket structure.

    Args:
        num_playoff_teams: Number of teams making playoffs
        bye_teams: Number of first-round byes
        num_teams: Total teams in league

    Returns:
        (is_valid, error_message) - error_message is empty string if valid
    """
    # Basic sanity checks
    if num_playoff_teams == 0:
        if bye_teams != 0:
            return False, f"bye_teams ({bye_teams}) must be 0 when playoffs are disabled"
        return True, ""

    if num_playoff_teams < 0:
        return False, f"num_playoff_teams ({num_playoff_teams}) cannot be negative"

    if num_playoff_teams > num_teams:
        return False, f"num_playoff_teams ({num_playoff_teams}) exceeds total teams ({num_teams})"

    if bye_teams < 0:
        return False, f"bye_teams ({bye_teams}) cannot be negative"

    if bye_teams >= num_playoff_teams:
        return False, f"bye_teams ({bye_teams}) must be less than num_playoff_teams ({num_playoff_teams})"

    if num_playoff_teams == 2:
        if bye_teams != 0:
            return False, "bye_teams must be 0 for a two-team championship"
        return True, ""

    # Check if first round is valid
    teams_playing_round1 = num_playoff_teams - bye_teams

    if teams_playing_round1 < 0:
        return False, f"Invalid: {teams_playing_round1} teams would play in round 1 (negative)"

    # Teams playing round 1 must be even (pairs of matchups)
    if teams_playing_round1 % 2 != 0:
        return False, f"Invalid: {teams_playing_round1} teams in round 1 (must be even for matchups)"

    # Check if round 2 makes sense
    winners_round1 = teams_playing_round1 // 2
    teams_round2 = winners_round1 + bye_teams

    if teams_round2 < 2:
        return False, f"Invalid: Only {teams_round2} teams would reach round 2 (need at least 2 for championship)"

    # Ideally round 2 should also be even (unless it's exactly the finals)
    if teams_round2 > 2 and teams_round2 % 2 != 0:
        # This is a warning, not an error - some leagues have 3-way finals or other structures
        print(f"  [WARN] Round 2 has {teams_round2} teams (odd number, bracket may be unusual)")

    return True, ""


def calculate_playoff_rounds_from_bracket(num_playoff_teams: int) -> int:
    """
    Calculate expected number of playoff rounds from bracket size.

    This is purely based on bracket structure - no data dependency.
    Uses ceil(log2(n)) which gives the number of rounds needed to
    determine a winner from n teams.

    Args:
        num_playoff_teams: Number of teams in playoffs

    Returns:
        Number of rounds (weeks) in playoffs

    Examples:
        2 teams -> 1 round (finals only)
        4 teams -> 2 rounds (semis -> finals)
        6 teams -> 3 rounds (wild card -> semis -> finals)
        8 teams -> 3 rounds (quarters -> semis -> finals)
        12 teams -> 4 rounds
    """
    if num_playoff_teams <= 1:
        return 0
    return math.ceil(math.log2(num_playoff_teams))


def get_playoff_round_label(round_index: int, total_rounds: int) -> str:
    """Return the canonical label for a playoff round index."""
    offset_from_end = total_rounds - round_index
    if offset_from_end == 0:
        return "championship"
    if offset_from_end == 1:
        return "semifinal"
    if offset_from_end == 2:
        return "quarterfinal"
    if offset_from_end == 3:
        return "first_round"
    return f"round_{round_index}"


def get_playoff_round_windows(settings: dict) -> list[tuple[int, int]]:
    """Return postseason round windows as ``[(week_start, week_end), ...]``."""
    playoff_start = _require_setting_int(settings, "playoff_start_week")
    num_playoff_teams = _require_setting_int(settings, "num_playoff_teams")
    total_rounds = calculate_playoff_rounds_from_bracket(num_playoff_teams)
    round_type = _coerce_optional_int(
        settings.get("playoff_round_type"), "playoff_round_type", 0, "settings", default=0
    )
    end_week = settings.get("end_week")
    end_week = int(end_week) if not _is_missing(end_week) else None

    windows: list[tuple[int, int]] = []
    week = playoff_start
    for round_index in range(1, total_rounds + 1):
        span = 2 if (round_type == 1 or (round_type == 2 and round_index == total_rounds)) else 1
        week_end = week + span - 1
        if end_week is not None:
            week_end = min(week_end, end_week)
        windows.append((week, week_end))
        week = week_end + 1
    return windows


def get_playoff_round_priority(round_value, num_playoff_teams: int) -> int | None:
    """Return placement priority for a playoff elimination round."""
    if _is_missing(round_value):
        return None

    total_rounds = calculate_playoff_rounds_from_bracket(num_playoff_teams)

    if isinstance(round_value, str):
        normalized = round_value.strip().lower()
        if not normalized:
            return None
        if normalized == "championship":
            return None
        if normalized == "semifinal":
            return 1
        if normalized == "quarterfinal":
            return 2
        if normalized == "first_round":
            return 3
        if normalized.startswith("round_"):
            suffix = normalized.split("_", 1)[1]
            try:
                round_index = int(suffix)
            except ValueError:
                return None
        else:
            try:
                round_index = int(normalized)
            except ValueError:
                return None
    else:
        try:
            round_index = int(round_value)
        except (TypeError, ValueError):
            return None

    if round_index <= 0 or round_index >= total_rounds:
        return None
    return total_rounds - round_index


def get_bracket_side(seed: int, num_playoff_teams: int) -> str:
    """
    Determine which bracket side a seed is on (A or B) for fixed bracket.

    Standard bracket structure assigns seeds to sides so that the #1 seed
    can only meet the #2 seed in the finals:
    - 4-team: Side A = 1,4; Side B = 2,3
    - 6-team: Side A = 1,4,5; Side B = 2,3,6
    - 8-team: Side A = 1,4,5,8; Side B = 2,3,6,7

    Args:
        seed: The playoff seed (1 = best)
        num_playoff_teams: Total number of playoff teams

    Returns:
        'A' or 'B' indicating bracket side
    """
    if num_playoff_teams <= 4:
        return "A" if seed in [1, 4] else "B"
    elif num_playoff_teams <= 6:
        return "A" if seed in [1, 4, 5] else "B"
    elif num_playoff_teams <= 8:
        return "A" if seed in [1, 4, 5, 8] else "B"
    else:
        # For larger brackets, use standard tournament seeding pattern
        # Side A: 1, 4, 5, 8, 9, 12, 13, 16...
        # Side B: 2, 3, 6, 7, 10, 11, 14, 15...
        bracket_group = ((seed - 1) // 4) % 2
        position_in_group = (seed - 1) % 4
        if bracket_group == 0:
            return "A" if position_in_group in [0, 3] else "B"
        else:
            return "A" if position_in_group in [0, 3] else "B"


def create_round_matchups(
    teams_playing: list[str], seeds_map: dict[str, int], use_reseeding: bool, round_num: int, num_playoff_teams: int
) -> list[tuple[str, str]]:
    """
    Create matchup pairings for a playoff round.

    Args:
        teams_playing: List of team names playing this round (excludes bye teams)
        seeds_map: Dict mapping manager name -> seed number
        use_reseeding: Whether bracket reseeds each round
        round_num: Which round (0 = wild card/first round)
        num_playoff_teams: Total playoff teams (for bracket structure)

    Returns:
        List of (teamA, teamB) tuples representing matchups
    """
    if len(teams_playing) < 2:
        return []

    # Sort by seed (best seed first)
    teams_sorted = sorted(teams_playing, key=lambda m: seeds_map.get(m, 999))
    num_games = len(teams_sorted) // 2
    matchups = []

    # Round 0: Both reseeding and fixed bracket use highest seed vs lowest seed
    # (e.g., 3v6, 4v5 for 6-team wild card; 1v4, 2v3 for 4-team semis)
    if round_num == 0 or use_reseeding:
        for i in range(num_games):
            higher_seed = teams_sorted[i]
            lower_seed = teams_sorted[-(i + 1)]
            matchups.append((higher_seed, lower_seed))
    else:
        # Fixed bracket (after round 0): Use bracket position based on original seed
        # Teams on the same side of the bracket face each other until the finals
        side_a = [t for t in teams_sorted if get_bracket_side(seeds_map.get(t, 99), num_playoff_teams) == "A"]
        side_b = [t for t in teams_sorted if get_bracket_side(seeds_map.get(t, 99), num_playoff_teams) == "B"]

        # Sort each side by seed (best first)
        side_a.sort(key=lambda m: seeds_map.get(m, 999))
        side_b.sort(key=lambda m: seeds_map.get(m, 999))

        # If we have one team from each side, it's the finals
        if len(side_a) == 1 and len(side_b) == 1:
            matchups.append((side_a[0], side_b[0]))
        else:
            # Within each side, pair highest vs lowest
            for side in [side_a, side_b]:
                side_games = len(side) // 2
                for i in range(side_games):
                    matchups.append((side[i], side[-(i + 1)]))

    return matchups


def _calculate_bye_teams(num_playoff_teams: int) -> int:
    """
    Calculate expected bye teams based on playoff bracket structure.

    Standard bracket structures:
    - 4 teams: No byes (semis in round 1)
    - 6 teams: 2 byes (top 2 seeds skip round 1)
    - 8 teams: No byes (quarterfinals in round 1)
    - 12 teams: 4 byes (top 4 seeds skip round 1)

    For non-standard sizes, calculate to make bracket work:
    - Power of 2: No byes needed
    - Otherwise: Calculate to fill bracket
    """
    if num_playoff_teams <= 0:
        return 0

    # Standard cases
    if num_playoff_teams == 4:
        return 0
    elif num_playoff_teams == 6:
        return 2  # 4 teams in round 1, 2 byes
    elif num_playoff_teams == 8:
        return 0
    elif num_playoff_teams == 12:
        return 4  # 8 teams in round 1, 4 byes

    # For other sizes: calculate byes to make bracket work
    # Find next power of 2 that can accommodate the bracket
    next_power = 1
    while next_power < num_playoff_teams:
        next_power *= 2

    # Byes = next_power - num_playoff_teams
    return next_power - num_playoff_teams


def get_expected_championship_week(settings: dict) -> int:
    """
    Get expected championship week from settings.

    PRIORITY:
    1. Calculate from bracket structure (most reliable)
    2. Use end_week only as a sanity fallback when playoff_start_week is
       clearly broken
    3. Fallback: playoff_start_week + 2

    This approach uses num_playoff_teams (which Yahoo always provides correctly)
    rather than relying on end_week (which may be missing or incorrect for
    older leagues).

    Args:
        settings: Dict with league settings

    Returns:
        Expected week number for championship game
    """
    playoff_start = _require_setting_int(settings, "playoff_start_week")
    num_playoff_teams = _require_setting_int(settings, "num_playoff_teams")

    # PREFERRED: Calculate from bracket structure, respecting playoff_round_type
    expected_rounds = calculate_playoff_rounds_from_bracket(num_playoff_teams)
    round_type = _coerce_optional_int(
        settings.get("playoff_round_type"), "playoff_round_type", 0, "settings", default=0
    )

    if round_type == 0:
        # Single-week rounds: each round is 1 week
        calculated_championship = playoff_start + expected_rounds - 1
    elif round_type == 1:
        # All 2-week rounds: each round spans 2 weeks
        calculated_championship = playoff_start + (2 * expected_rounds) - 1
    else:
        # Type 2: 2-week championship only (rounds 1..N-1 are 1 week, final is 2)
        calculated_championship = playoff_start + expected_rounds

    # Use end_week only as a sanity fallback when playoff_start_week is
    # obviously wrong. We intentionally do NOT stretch the championship week
    # up to end_week for padded late-consolation seasons.
    end_week = settings.get("end_week")
    if end_week:
        end_week = int(end_week)
        # SANITY CHECK: If calculated week is unreasonably early (before week 13),
        # but end_week is normal (>= 15), the playoff_start_week in settings is wrong.
        # Trust end_week from Yahoo settings in this case.
        if calculated_championship < 13 and end_week >= 15:
            return end_week

    return calculated_championship


def get_expected_playoff_rounds(settings: dict) -> int:
    """
    Get expected number of playoff rounds from bracket structure.

    Uses ceil(log2(num_playoff_teams)) which is purely based on
    bracket math - no data dependency.

    Args:
        settings: Dict with num_playoff_teams

    Returns:
        Number of expected playoff weeks/rounds
    """
    num_playoff_teams = _require_setting_int(settings, "num_playoff_teams")
    return calculate_playoff_rounds_from_bracket(num_playoff_teams)


def get_championship_week_from_data(df: pd.DataFrame, year: int) -> int | None:
    """
    Get championship week from actual data (champion flag).

    This function only returns a value for COMPLETED seasons where a champion
    has been crowned. For mid-season or incomplete data, it returns None so
    callers can fall back to settings-based calculation.

    Args:
        df: DataFrame with matchup data (must have 'year', 'week' columns)
        year: Year to find championship week for

    Returns:
        Championship week number if season is complete (has champion), None otherwise
    """
    year_df = df[df["year"] == year]
    if year_df.empty:
        return None

    # Only return championship week if we have a confirmed champion
    # This ensures we don't return incorrect values mid-season
    if "champion" in year_df.columns:
        champ_rows = year_df[year_df["champion"] == 1]
        if not champ_rows.empty:
            return int(champ_rows["week"].max())

    # No champion found - season is incomplete or data doesn't have champion flag
    # Return None so callers use settings-based calculation instead
    return None


def _load_from_local_db(data_directory: Path, year: int) -> dict | None:
    """
    Try to load settings from local DuckDB league_settings table.

    Checks for a local .duckdb file in the data directory.

    Args:
        data_directory: Directory containing the league data
        year: Year to load settings for

    Returns:
        Settings dict or None if not found
    """

    settings_df = None

    # Try local DuckDB files first
    duckdb_files = list(data_directory.glob("*.duckdb"))
    if duckdb_files:
        import duckdb as _ddb

        try:
            _conn = _ddb.connect(str(duckdb_files[0]), read_only=True)
            _exists = (
                _conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'league_settings'"
                ).fetchone()[0]
                > 0
            )
            if _exists:
                settings_df = _conn.execute("SELECT * FROM public.league_settings").fetchdf()
            _conn.close()
        except Exception:
            pass

    if settings_df is None or settings_df.empty:
        return None

    year_settings = settings_df[settings_df["year"] == year]
    if year_settings.empty:
        return None

    return _normalize_flat_settings_row(year_settings.iloc[0])


def _normalize_flat_settings_row(row) -> dict | None:
    """Normalize one flat canonical league_settings row to playoff config."""
    year_raw = _get_first_present(row, ("year",))
    year = _coerce_required_int(year_raw, "year", 0, "league_settings row") if not _is_missing(year_raw) else 0
    return _normalize_playoff_settings(row, year, "league_settings row")


def _load_from_flat_settings_map(settings_by_year: dict[int, dict] | None, year: int) -> dict | None:
    """Load playoff config from in-memory flat league_settings rows keyed by year."""
    if not settings_by_year:
        return None

    row = settings_by_year.get(int(year)) or settings_by_year.get(str(year))
    if not row:
        return None

    if isinstance(row, pd.Series):
        return _normalize_flat_settings_row(row)

    return _normalize_flat_settings_row(pd.Series(row))


def _infer_settings_from_data(df: pd.DataFrame, year: int) -> dict | None:
    """
    Infer playoff settings from data structure when no settings file exists.

    Detects playoffs by finding weeks with fewer games than regular season.

    Args:
        df: DataFrame with matchup data
        year: Year to infer settings for

    Returns:
        Settings dict or None if unable to infer
    """
    year_df = df[df["year"] == year]
    if year_df.empty:
        return None

    weeks = sorted(year_df["week"].dropna().unique())
    if len(weeks) < 2:
        return None

    # Count games per week
    games_per_week = {}
    for week in weeks:
        week_games = len(year_df[year_df["week"] == week]) // 2
        games_per_week[week] = week_games

    # Find regular season game count (mode of first 10 weeks)
    early_weeks = [w for w in weeks if w <= 10]
    if early_weeks:
        game_counts = [games_per_week[w] for w in early_weeks]
        regular_game_count = max(set(game_counts), key=game_counts.count)
    else:
        regular_game_count = max(games_per_week.values())

    # Detect playoff start: first week with fewer games than regular season
    playoff_start = None
    for week in weeks:
        if games_per_week[week] < regular_game_count:
            playoff_start = int(week)
            break

    if playoff_start is None:
        return None

    # Count unique teams in league for num_teams
    num_teams = len(year_df["franchise_id"].unique())

    # IMPROVED INFERENCE: Count actual playoff participants across ALL playoff weeks
    # This handles byes correctly - teams on bye appear in later weeks but not week 1
    playoff_df = year_df[year_df["week"] >= playoff_start]

    # Find teams in championship bracket (is_playoffs=1, not consolation)
    if "is_playoffs" in playoff_df.columns:
        champ_bracket = playoff_df[playoff_df["is_playoffs"] == 1]
        if not champ_bracket.empty:
            num_playoff_teams = len(champ_bracket["franchise_id"].unique())
        else:
            # Fallback: use first playoff week games
            first_playoff_games = games_per_week.get(playoff_start, 2)
            num_playoff_teams = first_playoff_games * 2
    else:
        # No is_playoffs flag - use first week games as rough estimate
        first_playoff_games = games_per_week.get(playoff_start, 2)
        num_playoff_teams = first_playoff_games * 2

    # Infer bye_teams from playoff structure
    # If 6 teams but only 2 games in week 1 → 2 byes
    # If 4 teams and 2 games in week 1 → 0 byes
    first_playoff_games = games_per_week.get(playoff_start, 2)
    teams_playing_week1 = first_playoff_games * 2
    bye_teams = max(0, num_playoff_teams - teams_playing_week1)

    # Sanity check: byes should be reasonable (0, 2, or 4 typically)
    if bye_teams > num_playoff_teams // 2:
        bye_teams = 0  # Likely a data issue, default to no byes

    # Infer end_week from data (max week in the year)
    end_week = int(max(weeks))

    config = {
        "playoff_start_week": playoff_start,
        "num_playoff_teams": num_playoff_teams,
        "bye_teams": bye_teams,  # Now properly inferred
        "has_multiweek_championship": 0,
        "uses_playoff_reseeding": 0,
        "num_teams": num_teams,
        "end_week": end_week,  # Last week in data = last week of season
    }

    # Settings inferred from data structure

    return config


def load_league_settings(
    year: int,
    settings_dir: str | None = None,
    df: pd.DataFrame | None = None,
    data_directory: str | None = None,
    settings_by_year: dict[int, dict] | None = None,
) -> dict:
    """
    Load league settings for a specific year.

    Tries multiple canonical sources in order:
    1. In-memory flat league_settings rows
    2. Local DuckDB league_settings table
    3. Flat settings JSON files

    Args:
        year: Season year
        settings_dir: Directory containing league_settings JSON files (auto-detected if None)
        df: DataFrame with league_id (used for auto-detection if settings_dir is None)
        data_directory: Path to league data directory (for finding league settings)

    Returns:
        Dictionary with playoff_start_week, num_playoff_teams, bye_teams, has_multiweek_championship, uses_playoff_reseeding
    """
    # A source that HAS a row for the year but with missing/invalid playoff fields must fall
    # through to the next source, not raise -- otherwise the whole cascade below is unreachable
    # exactly when it is needed (observed: MFL league_settings rows with NULL num_playoff_teams
    # killed the sim while data-derived JSON fallbacks sat untried). The final raise keeps the
    # fail-closed contract when NO source can answer, and carries the first source error.
    first_err: Exception | None = None
    try:
        config = _load_from_flat_settings_map(settings_by_year, year)
        if config:
            return config
    except ValueError as exc:
        first_err = exc

    # Try local DuckDB first if data_directory is provided (most reliable for imports)
    if data_directory:
        try:
            db_config = _load_from_local_db(Path(data_directory), year)
            if db_config:
                return db_config
        except ValueError as exc:
            first_err = first_err or exc

    # Helper function to load and validate JSON settings
    def _try_load_json_settings(search_path: Path) -> dict | None:
        """Try to load JSON settings from a directory."""
        # Try multiple naming patterns:
        # 1. league_settings_{year}.json (standard format)
        # 2. league_settings_{year}_*.json (versioned format)
        # 3. settings_{year}.json (ESPN format)
        settings_files = list(search_path.glob(f"league_settings_{year}.json"))
        if not settings_files:
            settings_files = list(search_path.glob(f"league_settings_{year}_*.json"))
        if not settings_files:
            settings_files = list(search_path.glob(f"settings_{year}.json"))
        if not settings_files:
            return None
        with open(settings_files[0], encoding="utf-8") as f:
            settings = json.load(f)
            metadata = settings.get("metadata", settings)
        return _normalize_playoff_settings(metadata, year, str(settings_files[0]))

    # Try JSON files from settings_dir (explicit or auto-detected)
    if settings_dir is None:
        # Use centralized league-agnostic discovery function
        if data_directory:
            settings_path = find_league_settings_directory(data_directory=Path(data_directory), df=df)
        else:
            settings_path = find_league_settings_directory(df=df)
        if settings_path:
            settings_dir = str(settings_path)

    if settings_dir:
        config = _try_load_json_settings(Path(settings_dir))
        if config:
            return config
        # Also check league_settings/ subdirectory
        subdir = Path(settings_dir) / "league_settings"
        if subdir.exists():
            config = _try_load_json_settings(subdir)
            if config:
                return config

    # FALLBACK: Try JSON files directly in data_directory (for league imports)
    if data_directory:
        config = _try_load_json_settings(Path(data_directory))
        if config:
            return config
        # Also check league_settings/ subdirectory
        subdir = Path(data_directory) / "league_settings"
        if subdir.exists():
            config = _try_load_json_settings(subdir)
            if config:
                return config

    raise ValueError(
        f"Missing canonical playoff settings for year {year}. "
        "Checked in-memory settings, local DuckDB league_settings, and flat settings JSON files."
        + (f" First source error: {first_err}" if first_err else "")
    )
