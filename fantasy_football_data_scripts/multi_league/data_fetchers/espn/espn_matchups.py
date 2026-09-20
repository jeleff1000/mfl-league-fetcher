"""
ESPN Matchups Fetcher

Fetches weekly matchup data from ESPN Fantasy API.

Two paths depending on era:
- 2019+: box_scores(week) provides full player-level data with scores
- Pre-2019 or provider-archived seasons: scoreboard(week) provides team-level scores

Output columns match CanonicalMatchupColumns for downstream pipeline compatibility.
"""

import logging

import pandas as pd

from multi_league.core.league_refresh import espn_schedule_is_final

logger = logging.getLogger(__name__)


def log(msg: str):
    logger.info(msg)
    print(msg)


def _classify_matchup_type(matchup_type_str, is_playoff_api, week, playoff_start):
    """
    Classify a matchup based on ESPN's playoffTierType string.

    Returns: (is_playoffs, is_consolation)
    RULE: is_consolation=1 implies is_playoffs=0 (mutually exclusive)

    ESPN matchup_type values:
    - 'NONE': regular season
    - 'WINNERS_BRACKET': championship bracket (true playoffs)
    - 'WINNERS_CONSOLATION_LADDER': teams eliminated from winner's bracket
    - 'LOSERS_CONSOLATION_LADDER': teams that didn't make playoffs
    - 'LOSERS_BRACKET': sacko/punishment bracket
    """
    if matchup_type_str and str(matchup_type_str).upper() != "NONE":
        mt = str(matchup_type_str).upper()
        if mt == "WINNERS_BRACKET":
            return True, False  # True playoffs (championship bracket)
        elif "CONSOLATION" in mt or "LOSERS" in mt:
            return False, True  # Consolation bracket
        else:
            # Unknown bracket type - treat as playoff
            return True, False
    elif is_playoff_api:
        # ESPN says it's a playoff but no specific type
        return True, False
    elif week >= playoff_start:
        # Fallback: week-based inference (shouldn't happen if matchup_type available)
        return True, False
    else:
        return False, False


def _get_max_weeks(year: int) -> int:
    """Get maximum regular + playoff weeks for a season."""
    # NFL expanded to 18 weeks (17 games) starting 2021
    if year >= 2021:
        return 18
    return 17


def _normalize_raw_numeric(value) -> float | None:
    """Normalize raw ESPN numeric payloads to rounded floats."""
    if value in (None, ""):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _normalize_raw_team_id(value) -> int | None:
    """Normalize raw ESPN team IDs to ints for matchup lookup."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _period_points(side: dict, scoring_period: int | None) -> float | None:
    """Read side points for the requested scoring period from raw ESPN JSON."""
    if scoring_period is None:
        return None
    points_by_period = side.get("pointsByScoringPeriod")
    if not isinstance(points_by_period, dict):
        return None
    value = points_by_period.get(str(scoring_period))
    if value is None:
        value = points_by_period.get(scoring_period)
    return _normalize_raw_numeric(value)


def _normalize_raw_winner(value) -> str | None:
    """Normalize ESPN winner strings to HOME/AWAY/TIE when present."""
    if value in (None, ""):
        return None
    winner = str(value).upper()
    if winner in {"HOME", "AWAY", "TIE"}:
        return winner
    return None


def _api_outcome(winner: str | None, side: str) -> str | None:
    """Return win/loss/tie for a side from ESPN's raw winner field."""
    normalized = _normalize_raw_winner(winner)
    if normalized is None:
        return None
    if normalized == "TIE":
        return "tie"
    return "win" if normalized == side.upper() else "loss"


def _raw_score(raw_meta: dict, side: str, fallback) -> float:
    """Prefer raw ESPN totalPoints when available, with library score fallback."""
    raw_value = raw_meta.get(f"{side}_total_points")
    if raw_value is not None:
        return raw_value
    return fallback or 0


def _effective_box_scores(box, raw_schedule_lookup: dict) -> tuple[float, float]:
    """Prefer ESPN's raw finalized totals when its BoxScore totals lag at zero."""
    home = getattr(box, "home_team", None)
    away = getattr(box, "away_team", None)
    raw_meta = raw_schedule_lookup.get(
        (
            _normalize_raw_team_id(getattr(home, "team_id", None)) if home else None,
            _normalize_raw_team_id(getattr(away, "team_id", None)) if away else None,
        ),
        {},
    )

    def score(side: str, fallback) -> float:
        period_value = raw_meta.get(f"{side}_period_points")
        if period_value is not None:
            return period_value
        return _raw_score(raw_meta, side, fallback)

    return (
        score("home", getattr(box, "home_score", 0)),
        score("away", getattr(box, "away_score", 0)),
    )


def _index_raw_schedule(
    schedule: list[dict] | None, scoring_period: int | None = None
) -> dict[tuple[int | None, int | None], dict[str, float | str | None]]:
    """Index raw ESPN schedule payload by oriented team pair."""
    indexed: dict[tuple[int | None, int | None], dict[str, float | str | None]] = {}
    for matchup in schedule or []:
        if not isinstance(matchup, dict):
            continue
        home = matchup.get("home") or {}
        away = matchup.get("away") or {}
        key = (
            _normalize_raw_team_id(home.get("teamId")),
            _normalize_raw_team_id(away.get("teamId")),
        )
        indexed[key] = {
            "home_adjustment": _normalize_raw_numeric(home.get("adjustment")),
            "away_adjustment": _normalize_raw_numeric(away.get("adjustment")),
            "home_tiebreak": _normalize_raw_numeric(home.get("tiebreak")),
            "away_tiebreak": _normalize_raw_numeric(away.get("tiebreak")),
            "home_total_points": _normalize_raw_numeric(home.get("totalPoints")),
            "away_total_points": _normalize_raw_numeric(away.get("totalPoints")),
            "home_period_points": _period_points(home, scoring_period),
            "away_period_points": _period_points(away, scoring_period),
            "winner": _normalize_raw_winner(matchup.get("winner")),
            "matchup_type": matchup.get("playoffTierType"),
        }
    return indexed


def _raw_schedule_matches_period(schedule: list[dict] | None, scoring_period: int) -> bool | None:
    """Validate an ESPN response's period when the payload exposes it.

    ``None`` means the response has no period markers and therefore cannot be
    used for validation.  ``False`` is significant: ESPN occasionally returns
    the current schedule for a requested unplayed/historical period.
    """
    periods = {
        int(raw_period)
        for matchup in schedule or []
        if isinstance(matchup, dict)
        for raw_period in [matchup.get("matchupPeriodId")]
        if raw_period is not None and str(raw_period).strip().isdigit()
    }
    if not periods:
        return None
    return scoring_period in periods


def _matchup_period_is_final(schedule: list[dict]) -> bool:
    """Respect explicit outcomes without changing legacy missing-winner fallback."""
    if not any(str(row.get("winner") or "").strip() for row in schedule):
        return True
    return espn_schedule_is_final(schedule)


def _box_score_snapshot_signature(box_scores: list, raw_schedule_lookup: dict | None = None) -> tuple:
    """Return a stable signature for detecting a repeated ESPN snapshot."""
    raw_schedule_lookup = raw_schedule_lookup or {}
    rows = []
    for box in box_scores:
        home = getattr(box, "home_team", None)
        away = getattr(box, "away_team", None)
        home_score, away_score = _effective_box_scores(box, raw_schedule_lookup)
        rows.append(
            (
                getattr(home, "team_id", None) if home else None,
                getattr(away, "team_id", None) if away else None,
                round(home_score, 2),
                round(away_score, 2),
                getattr(box, "matchup_type", None),
            )
        )
    return tuple(sorted(rows, key=repr))


def _settings_float(settings: dict | None, *keys: str) -> float:
    """Read a numeric setting from any of several known key names."""
    for key in keys:
        if settings and settings.get(key) not in (None, ""):
            normalized = _normalize_raw_numeric(settings.get(key))
            if normalized is not None:
                return normalized
    return 0.0


def _apply_legacy_away_winner_bonus_repair(
    home_score: float,
    away_score: float,
    raw_meta: dict,
    is_playoff: bool,
    playoff_home_team_bonus: float,
) -> tuple[float, float]:
    """
    Repair a narrow pre-2019 ESPN playoff bonus quirk.

    Legacy ESPN scoreboards can expose a tied playoff final while the raw
    schedule also says the away side won. When the league has a playoff home
    bonus, the winner's period score is higher than the home side's period score,
    and the winner's total has not already received the bonus, carry that
    platform bonus into the flattened matchup score.
    """
    if not is_playoff or playoff_home_team_bonus <= 0:
        return home_score, away_score
    if raw_meta.get("winner") != "AWAY":
        return home_score, away_score
    if abs(float(home_score) - float(away_score)) > 0.01:
        return home_score, away_score

    home_period = raw_meta.get("home_period_points")
    away_period = raw_meta.get("away_period_points")
    if home_period is None or away_period is None:
        return home_score, away_score
    if float(away_period) <= float(home_period) + 0.01:
        return home_score, away_score
    if abs(float(away_score) - float(away_period)) > 0.01:
        return home_score, away_score

    return home_score, round(float(away_score) + playoff_home_team_bonus, 2)


def _detect_playoff_start(ctx: "ESPNContext", year: int) -> int:
    """
    Detect which week playoffs start for a given year.

    Args:
        ctx: ESPNContext
        year: NFL season year

    Returns:
        First playoff week number (e.g., 15 means weeks 15-17 are playoffs)
    """
    # Try context first
    if ctx.playoff_start_week:
        return ctx.playoff_start_week

    # Try league settings
    from .espn_league_settings import load_espn_settings

    settings = load_espn_settings(ctx, year)
    if settings and settings.get("playoff_start_week"):
        return settings["playoff_start_week"]

    # Default based on era
    if year >= 2021:
        return 15  # 14 regular + 4 playoff (18 total)
    return 14  # 13 regular + 4 playoff (17 total)


def _get_playoff_seed_map(client, year: int) -> dict[int, int]:
    """Best-effort ESPN playoffSeed map from the raw API."""
    get_seed_map = getattr(client, "get_raw_team_playoff_seed_map", None)
    if get_seed_map is None:
        return {}
    try:
        return get_seed_map(year) or {}
    except Exception as exc:  # noqa: BLE001 - fetcher should still emit matchup rows
        logger.warning(f"  [MATCHUPS] {year}: failed to fetch ESPN playoff seeds: {exc}")
        return {}


def fetch_espn_matchups_modern(
    ctx: "ESPNContext",
    year: int,
    weeks: list[int] | None = None,
    *,
    client=None,
    league=None,
    box_scores_by_week: dict[int, list] | None = None,
    raw_schedules_by_week: dict[int, list[dict]] | None = None,
) -> pd.DataFrame | None:
    """
    Fetch matchups for 2019+ using box_scores.

    box_scores provides BoxScore objects with:
    - home_team, away_team
    - home_score, away_score
    - home_lineup, away_lineup (player details)
    - is_playoff, matchup_type
    """
    from .espn_api_client import ESPNAPIClient

    if client is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [MATCHUPS] Failed to load league for {year}: {e}")
        return None

    if getattr(league, "_uses_league_history", False):
        return fetch_espn_matchups_legacy(ctx, year, weeks=weeks, client=client, league=league)

    max_weeks = _get_max_weeks(year)
    playoff_start = _detect_playoff_start(ctx, year)
    playoff_seed_map = _get_playoff_seed_map(client, year)

    # Respect end_week from league settings to avoid fetching consolation weeks
    # beyond the championship
    from .espn_league_settings import load_espn_settings

    settings = load_espn_settings(ctx, year)
    end_week = settings.get("end_week") if settings else None
    if end_week:
        max_weeks = min(max_weeks, int(end_week))
        log(f"  [MATCHUPS] {year}: capping at week {max_weeks} (end_week from settings)")
    from multi_league.core.league_refresh import provider_weeks_to_fetch

    weeks_to_fetch = provider_weeks_to_fetch(max_week=max_weeks, requested_weeks=weeks)

    # 2-week playoff matchups: ESPN returns CUMULATIVE scores for both weeks of
    # a 2-week matchup period. Week 1 shows week-1-only scores. Week 2 shows
    # the combined total. We detect the continuation week (same teams, scores
    # only went up or stayed equal) and compute per-week deltas:
    #   week1_score = cumulative_at_week1
    #   week2_score = cumulative_at_week2 - cumulative_at_week1
    playoff_matchup_len = int(settings.get("playoff_matchup_period_length", 1) or 1) if settings else 1
    # {(home_team_id, away_team_id): (home_cumulative, away_cumulative)} from previous week
    prev_week_cumulative: dict[tuple, tuple[float, float]] = {}

    rows = []
    consecutive_empty = 0
    MAX_CONSECUTIVE_EMPTY = 3
    previous_snapshot_signature = None

    for week in weeks_to_fetch:
        try:
            box_scores = (
                box_scores_by_week[int(week)]
                if box_scores_by_week is not None and int(week) in box_scores_by_week
                else league.box_scores(week)
            )
        except Exception:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            continue

        if not box_scores:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            continue

        raw_schedule = (
            raw_schedules_by_week[int(week)]
            if raw_schedules_by_week is not None and int(week) in raw_schedules_by_week
            else client.get_raw_schedule(year, week)
        )
        log(
            f"  [MATCHUPS] {year} week {week}: "
            f"box_scores={len(box_scores)}, raw_pairs={len(raw_schedule or [])}"
        )
        period_matches = _raw_schedule_matches_period(raw_schedule, week)
        if period_matches is False:
            log(
                f"  [MATCHUPS] {year}: ESPN returned a stale schedule for "
                f"requested week {week}; skipping it"
            )
            continue
        if not _matchup_period_is_final(raw_schedule):
            previous_snapshot_signature = snapshot_signature
            log(f"  [MATCHUPS] {year} week {week}: fantasy outcomes not final; holding matchup rows")
            continue
        raw_schedule_lookup = _index_raw_schedule(raw_schedule, week)
        has_scores = any(
            home_score > 0 or away_score > 0
            for home_score, away_score in (
                _effective_box_scores(box, raw_schedule_lookup) for box in box_scores
            )
        )
        if not has_scores:
            # A raw schedule with no score witness is still a preseason/live
            # snapshot. Do not materialize phantom 0-0 matchups.
            box_pairs = [
                (
                    _normalize_raw_team_id(getattr(getattr(box, "home_team", None), "team_id", None)),
                    _normalize_raw_team_id(getattr(getattr(box, "away_team", None), "team_id", None)),
                )
                for box in box_scores
            ]
            log(
                f"  [MATCHUPS] {year} week {week}: no score-bearing pair join; "
                f"box_pairs={box_pairs}, raw_pairs={list(raw_schedule_lookup)}"
            )
            if raw_schedule:
                first_raw = raw_schedule[0]
                score_fields = {}
                for side_name in ("home", "away"):
                    side = first_raw.get(side_name) or {}
                    score_fields[side_name] = {
                        key: value
                        for key, value in side.items()
                        if "score" in str(key).lower() or "point" in str(key).lower()
                    }
                log(
                    f"  [MATCHUPS] {year} week {week}: raw score fields "
                    f"{repr(score_fields)[:2000]}"
                )
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            continue

        consecutive_empty = 0
        snapshot_signature = _box_score_snapshot_signature(box_scores, raw_schedule_lookup)
        if previous_snapshot_signature is not None and snapshot_signature == previous_snapshot_signature:
            log(
                f"  [MATCHUPS] {year}: ESPN repeated the prior box-score "
                f"snapshot for requested week {week}; skipping it"
            )
            continue
        previous_snapshot_signature = snapshot_signature

        # 2-week matchup delta: if this is a continuation week in a 2-week
        # playoff round, the API scores are cumulative. We detect this by
        # checking if the same team pairs exist and scores only went up,
        # then subtract last week's scores to get per-week values.
        is_continuation = False
        delta_scores: dict[tuple, tuple[float, float]] = {}  # {(ht_id, at_id): (home_delta, away_delta)}
        if playoff_matchup_len >= 2 and week >= playoff_start and prev_week_cumulative:
            current_pairs: dict[tuple, tuple[float, float]] = {}
            for bs in box_scores:
                ht = getattr(bs, "home_team", None)
                at = getattr(bs, "away_team", None)
                ht_id = getattr(ht, "team_id", None) if ht else None
                at_id = getattr(at, "team_id", None) if at else None
                hs, as_ = (round(score, 2) for score in _effective_box_scores(bs, raw_schedule_lookup))
                if ht_id is not None and at_id is not None:
                    current_pairs[(ht_id, at_id)] = (hs, as_)

            # Check if ALL playoff matchup pairs from last week reappear with
            # scores >= last week's (cumulative pattern)
            if current_pairs and set(current_pairs.keys()) == set(prev_week_cumulative.keys()):
                all_cumulative = all(
                    current_pairs[k][0] >= prev_week_cumulative[k][0] - 0.01
                    and current_pairs[k][1] >= prev_week_cumulative[k][1] - 0.01
                    for k in current_pairs
                )
                if all_cumulative:
                    is_continuation = True
                    for k in current_pairs:
                        h_prev, a_prev = prev_week_cumulative[k]
                        h_curr, a_curr = current_pairs[k]
                        delta_scores[k] = (round(h_curr - h_prev, 2), round(a_curr - a_prev, 2))
                    log(f"  [MATCHUPS] {year}: week {week} is 2-week continuation — computing per-week deltas")

        # Track cumulative scores for next week's delta computation
        if playoff_matchup_len >= 2 and week >= playoff_start:
            prev_week_cumulative = {}
            for bs in box_scores:
                ht = getattr(bs, "home_team", None)
                at = getattr(bs, "away_team", None)
                ht_id = getattr(ht, "team_id", None) if ht else None
                at_id = getattr(at, "team_id", None) if at else None
                hs, as_ = (round(score, 2) for score in _effective_box_scores(bs, raw_schedule_lookup))
                if ht_id is not None and at_id is not None:
                    prev_week_cumulative[(ht_id, at_id)] = (hs, as_)
            if is_continuation:
                # Reset so the NEXT week starts fresh (don't chain deltas across 3+ weeks)
                prev_week_cumulative = {}
        else:
            prev_week_cumulative = {}

        # Collect all scores for median calculation
        week_scores = []
        matchup_data = []

        for matchup_idx, bs in enumerate(box_scores):
            home_team = getattr(bs, "home_team", None)
            away_team = getattr(bs, "away_team", None)
            home_score, away_score = _effective_box_scores(bs, raw_schedule_lookup)
            is_playoff_api = getattr(bs, "is_playoff", False)
            matchup_type = getattr(bs, "matchup_type", None)
            raw_meta = raw_schedule_lookup.get(
                (
                    _normalize_raw_team_id(getattr(home_team, "team_id", None)) if home_team else None,
                    _normalize_raw_team_id(getattr(away_team, "team_id", None)) if away_team else None,
                ),
                {},
            )

            # For continuation weeks, replace cumulative scores with per-week deltas
            if is_continuation:
                ht_id = getattr(home_team, "team_id", None) if home_team else None
                at_id = getattr(away_team, "team_id", None) if away_team else None
                key = (ht_id, at_id)
                if key in delta_scores:
                    home_score, away_score = delta_scores[key]
                    # Skip the row entirely when the continuation week added no
                    # score. ESPN reports cumulative=week-1 (delta=0) when a
                    # league sets playoff_matchup_period_length=2 but actually
                    # plays single-week playoffs. Writing a 0-score row here
                    # creates a phantom matchup that poisons bracket_tracer
                    # (inconsistent playoff_round labels), power_rating
                    # (NULL on zero-score rows), and luck/sim calcs.
                    if abs(home_score) < 0.01 and abs(away_score) < 0.01:
                        continue

            # Classify using matchup_type (WINNERS_BRACKET = playoffs, CONSOLATION/LOSERS = consolation)
            is_playoff, is_consolation = _classify_matchup_type(matchup_type, is_playoff_api, week, playoff_start)
            is_championship = False  # Detected downstream by mark_playoff_rounds

            matchup_data.append(
                {
                    "matchup_idx": matchup_idx,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "home_adjustment": raw_meta.get("home_adjustment"),
                    "away_adjustment": raw_meta.get("away_adjustment"),
                    "home_tiebreak": raw_meta.get("home_tiebreak"),
                    "away_tiebreak": raw_meta.get("away_tiebreak"),
                    "winner": raw_meta.get("winner"),
                    "is_playoff": is_playoff,
                    "is_consolation": is_consolation,
                    "is_championship": is_championship,
                }
            )

            if home_team:
                week_scores.append(home_score)
            if away_team:
                week_scores.append(away_score)

        # Calculate league mean for median scoring
        league_mean = sum(week_scores) / len(week_scores) if week_scores else 0

        # Build rows (one per manager per week)
        for md in matchup_data:
            home = md["home_team"]
            away = md["away_team"]

            # Home team row
            if home:
                home_id = home.team_id
                away_id = away.team_id if away else None
                margin = md["home_score"] - md["away_score"]

                home_row = _build_matchup_row(
                    ctx=ctx,
                    year=year,
                    week=week,
                    team=home,
                    team_id=home_id,
                    team_score=md["home_score"],
                    opp=away,
                    opp_id=away_id,
                    opp_score=md["away_score"],
                    margin=margin,
                    matchup_id=md["matchup_idx"],
                    is_playoff=md["is_playoff"],
                    is_consolation=md["is_consolation"],
                    is_championship=md["is_championship"],
                    adjustment=md["home_adjustment"],
                    tiebreak=md["home_tiebreak"],
                    api_outcome=_api_outcome(md.get("winner"), "HOME"),
                    week_scores=week_scores,
                    league_mean=league_mean,
                    final_playoff_seed=playoff_seed_map.get(home_id),
                )
                rows.append(home_row)

            # Away team row
            if away:
                away_id_val = away.team_id
                home_id_val = home.team_id if home else None
                margin = md["away_score"] - md["home_score"]

                away_row = _build_matchup_row(
                    ctx=ctx,
                    year=year,
                    week=week,
                    team=away,
                    team_id=away_id_val,
                    team_score=md["away_score"],
                    opp=home,
                    opp_id=home_id_val,
                    opp_score=md["home_score"],
                    margin=margin,
                    matchup_id=md["matchup_idx"],
                    is_playoff=md["is_playoff"],
                    is_consolation=md["is_consolation"],
                    is_championship=md["is_championship"],
                    adjustment=md["away_adjustment"],
                    tiebreak=md["away_tiebreak"],
                    api_outcome=_api_outcome(md.get("winner"), "AWAY"),
                    week_scores=week_scores,
                    league_mean=league_mean,
                    final_playoff_seed=playoff_seed_map.get(away_id_val),
                )
                rows.append(away_row)

    if not rows:
        return None

    df = pd.DataFrame(rows)
    log(f"  [MATCHUPS] {year}: {len(df)} matchup rows ({len(df)//2} matchups)")
    return df


def fetch_espn_matchups_legacy(
    ctx: "ESPNContext",
    year: int,
    weeks: list[int] | None = None,
    *,
    client=None,
    league=None,
) -> pd.DataFrame | None:
    """
    Fetch pre-2019 or provider-archived matchups using scoreboard.

    scoreboard(week) provides Matchup objects with team scores.
    matchup_type (from playoffTierType) IS available on Matchup objects for all years.
    """
    from .espn_api_client import ESPNAPIClient

    if client is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [MATCHUPS] Failed to load league for {year}: {e}")
        return None

    max_weeks = _get_max_weeks(year)
    playoff_start = _detect_playoff_start(ctx, year)
    playoff_seed_map = _get_playoff_seed_map(client, year)

    # Respect end_week from league settings to avoid fetching consolation weeks
    # beyond the championship
    from .espn_league_settings import load_espn_settings

    settings = load_espn_settings(ctx, year)
    end_week = settings.get("end_week") if settings else None
    if end_week:
        max_weeks = min(max_weeks, int(end_week))
        log(f"  [MATCHUPS] {year}: capping at week {max_weeks} (end_week from settings)")
    from multi_league.core.league_refresh import provider_weeks_to_fetch

    weeks_to_fetch = provider_weeks_to_fetch(max_week=max_weeks, requested_weeks=weeks)
    playoff_home_team_bonus = _settings_float(
        settings,
        "playoff_home_team_bonus",
        "playoffHomeTeamBonus",
        "scoring_playoff_home_team_bonus",
    )
    if playoff_home_team_bonus <= 0:
        try:
            raw_settings = client.get_league_settings_raw(year)
            raw_scoring_settings = raw_settings.get("scoringSettings") if isinstance(raw_settings, dict) else {}
            playoff_home_team_bonus = _settings_float(raw_scoring_settings, "playoffHomeTeamBonus")
        except Exception:
            playoff_home_team_bonus = 0.0

    rows = []

    for week in weeks_to_fetch:
        try:
            scoreboard = league.scoreboard(week)
        except Exception as e:
            log(f"  [MATCHUPS] No scoreboard for {year} week {week}: {e}")
            break

        if not scoreboard:
            break

        # Collect week scores for median
        week_scores = []
        matchup_data = []
        raw_schedule = client.get_raw_schedule(year, week)
        period_matches = _raw_schedule_matches_period(raw_schedule, week)
        if period_matches is False:
            log(
                f"  [MATCHUPS] {year}: ESPN returned a stale schedule for "
                f"requested week {week}; skipping it"
            )
            continue
        if not _matchup_period_is_final(raw_schedule):
            log(f"  [MATCHUPS] {year} week {week}: fantasy outcomes not final; holding matchup rows")
            continue
        raw_schedule_lookup = _index_raw_schedule(raw_schedule, week)

        for matchup_idx, matchup in enumerate(scoreboard):
            home_team = getattr(matchup, "home_team", None)
            away_team = getattr(matchup, "away_team", None)
            raw_meta = raw_schedule_lookup.get(
                (
                    _normalize_raw_team_id(getattr(home_team, "team_id", None)) if home_team else None,
                    _normalize_raw_team_id(getattr(away_team, "team_id", None)) if away_team else None,
                ),
                {},
            )
            home_score = _raw_score(raw_meta, "home", getattr(matchup, "home_score", 0))
            away_score = _raw_score(raw_meta, "away", getattr(matchup, "away_score", 0))

            # Read matchup_type from Matchup object (available even for pre-2019)
            matchup_type = raw_meta.get("matchup_type") or getattr(matchup, "matchup_type", None)
            is_playoff_api = getattr(matchup, "is_playoff", False)
            is_playoff, is_consolation = _classify_matchup_type(matchup_type, is_playoff_api, week, playoff_start)
            home_score, away_score = _apply_legacy_away_winner_bonus_repair(
                home_score,
                away_score,
                raw_meta,
                is_playoff,
                playoff_home_team_bonus,
            )

            matchup_data.append(
                {
                    "matchup_idx": matchup_idx,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "home_adjustment": raw_meta.get("home_adjustment"),
                    "away_adjustment": raw_meta.get("away_adjustment"),
                    "home_tiebreak": raw_meta.get("home_tiebreak"),
                    "away_tiebreak": raw_meta.get("away_tiebreak"),
                    "winner": raw_meta.get("winner"),
                    "is_playoff": is_playoff,
                    "is_consolation": is_consolation,
                    "is_championship": False,  # Detected downstream
                }
            )

            if home_team:
                week_scores.append(home_score)
            if away_team:
                week_scores.append(away_score)

        # Check for end of season
        has_scores = any(md["home_score"] > 0 or md["away_score"] > 0 for md in matchup_data)
        if not has_scores and week > 1:
            break

        league_mean = sum(week_scores) / len(week_scores) if week_scores else 0

        for md in matchup_data:
            home = md["home_team"]
            away = md["away_team"]

            if home:
                margin = md["home_score"] - md["away_score"]
                rows.append(
                    _build_matchup_row(
                        ctx=ctx,
                        year=year,
                        week=week,
                        team=home,
                        team_id=home.team_id,
                        team_score=md["home_score"],
                        opp=away,
                        opp_id=away.team_id if away else None,
                        opp_score=md["away_score"],
                        margin=margin,
                        matchup_id=md["matchup_idx"],
                        is_playoff=md["is_playoff"],
                        is_consolation=md["is_consolation"],
                        is_championship=md["is_championship"],
                        adjustment=md["home_adjustment"],
                        tiebreak=md["home_tiebreak"],
                        api_outcome=_api_outcome(md.get("winner"), "HOME"),
                        week_scores=week_scores,
                        league_mean=league_mean,
                        final_playoff_seed=playoff_seed_map.get(home.team_id),
                    )
                )

            if away:
                margin = md["away_score"] - md["home_score"]
                rows.append(
                    _build_matchup_row(
                        ctx=ctx,
                        year=year,
                        week=week,
                        team=away,
                        team_id=away.team_id,
                        team_score=md["away_score"],
                        opp=home,
                        opp_id=home.team_id if home else None,
                        opp_score=md["home_score"],
                        margin=margin,
                        matchup_id=md["matchup_idx"],
                        is_playoff=md["is_playoff"],
                        is_consolation=md["is_consolation"],
                        is_championship=md["is_championship"],
                        adjustment=md["away_adjustment"],
                        tiebreak=md["away_tiebreak"],
                        api_outcome=_api_outcome(md.get("winner"), "AWAY"),
                        week_scores=week_scores,
                        league_mean=league_mean,
                        final_playoff_seed=playoff_seed_map.get(away.team_id),
                    )
                )

    if not rows:
        return None

    df = pd.DataFrame(rows)
    log(f"  [MATCHUPS] {year} (legacy): {len(df)} matchup rows")
    return df


def _build_matchup_row(
    ctx,
    year,
    week,
    team,
    team_id,
    team_score,
    opp,
    opp_id,
    opp_score,
    margin,
    matchup_id,
    is_playoff,
    is_consolation,
    is_championship,
    adjustment,
    tiebreak,
    api_outcome,
    week_scores,
    league_mean,
    final_playoff_seed=None,
) -> dict:
    """Build a single matchup row dict."""
    is_bye = not opp_id

    # BYE weeks: match Yahoo behavior — no win/loss, null scores/opponent,
    # not counted as a playoff game. Prevents phantom wins inflating records.
    if is_bye:
        team_name_val = getattr(team, "team_name", f"Team {team_id}")
        return {
            "year": year,
            "week": week,
            "manager": ctx.get_manager_name(team_id, team_name=team_name_val, year=year),
            "manager_guid": ctx.get_manager_guid(team_id, year=year),
            "team_key": str(team_id),
            "team_name": team_name_val,
            "franchise_id": ctx.get_franchise_id(team_id, year=year),
            "team_points": None,
            "opponent": None,
            "opponent_guid": "",
            "opponent_franchise_id": "",
            "opponent_team_name": "",
            "opponent_points": None,
            "margin": None,
            "win": None,
            "loss": None,
            "tie": None,
            "matchup_id": matchup_id,
            "is_playoffs": None,
            "is_consolation": None,
            "is_championship": False,
            "is_bye_week": True,
            "teams_beat_this_week": None,
            "league_weekly_mean": round(league_mean, 2),
            "above_league_median": None,
            "grade": None,
            "gpa": None,
            "matchup_recap_url": None,
            "matchup_recap_title": None,
            "team_projected_points": None,
            "opponent_projected_points": None,
            "adjustment": None,
            "tiebreak": None,
            "final_playoff_seed": final_playoff_seed,
            "platform": "espn",
            "league_id": str(ctx.get_league_id_for_year(year)),
        }

    if api_outcome == "win":
        win, loss, tie = 1, 0, 0
    elif api_outcome == "loss":
        win, loss, tie = 0, 1, 0
    elif api_outcome == "tie":
        win, loss, tie = 0, 0, 1 if team_score > 0 else 0
    else:
        win = 1 if margin > 0 else 0
        loss = 1 if margin < 0 else 0
        tie = 1 if margin == 0 and team_score > 0 else 0

    # Teams beat this week (how many other teams scored lower)
    teams_beat = sum(1 for s in week_scores if team_score > s)
    above_median = 1 if team_score > league_mean else 0

    team_name_val = getattr(team, "team_name", f"Team {team_id}")
    opp_name = getattr(opp, "team_name", "") if opp else ""

    return {
        "year": year,
        "week": week,
        "manager": ctx.get_manager_name(team_id, team_name=team_name_val, year=year),
        "manager_guid": ctx.get_manager_guid(team_id, year=year),
        "team_key": str(team_id),
        "team_name": team_name_val,
        "franchise_id": ctx.get_franchise_id(team_id, year=year),
        "team_points": round(team_score, 2),
        "opponent": ctx.get_manager_name(opp_id, team_name=opp_name, year=year),
        "opponent_guid": ctx.get_manager_guid(opp_id, year=year),
        "opponent_franchise_id": ctx.get_franchise_id(opp_id, year=year),
        "opponent_team_name": opp_name,
        "opponent_points": round(opp_score, 2),
        "margin": round(margin, 2),
        "win": win,
        "loss": loss,
        "tie": tie,
        "matchup_id": matchup_id,
        "is_playoffs": is_playoff,
        "is_consolation": is_consolation,
        "is_championship": is_championship,
        "is_bye_week": False,
        "teams_beat_this_week": teams_beat,
        "league_weekly_mean": round(league_mean, 2),
        "above_league_median": above_median,
        "grade": None,
        "gpa": None,
        "matchup_recap_url": None,
        "matchup_recap_title": None,
        "team_projected_points": None,
        "opponent_projected_points": None,
        "adjustment": adjustment,
        "tiebreak": tiebreak,
        "final_playoff_seed": final_playoff_seed,
        "platform": "espn",
        "league_id": str(ctx.get_league_id_for_year(year)),
    }


def fetch_espn_matchups(
    ctx: "ESPNContext",
    year: int,
    weeks: list[int] | None = None,
    *,
    client=None,
    league=None,
    box_scores_by_week: dict[int, list] | None = None,
    raw_schedules_by_week: dict[int, list[dict]] | None = None,
) -> pd.DataFrame | None:
    """
    Fetch matchups for a single year, choosing modern or legacy path.

    Args:
        ctx: ESPNContext
        year: NFL season year

    Returns:
        DataFrame with matchup data
    """
    if year >= 2019:
        return fetch_espn_matchups_modern(
            ctx,
            year,
            weeks=weeks,
            client=client,
            league=league,
            box_scores_by_week=box_scores_by_week,
            raw_schedules_by_week=raw_schedules_by_week,
        )
    else:
        return fetch_espn_matchups_legacy(
            ctx,
            year,
            weeks=weeks,
            client=client,
            league=league,
        )


def fetch_all_espn_matchups(ctx: "ESPNContext") -> pd.DataFrame:
    """
    Fetch matchup data for all years.

    Args:
        ctx: ESPNContext with year range

    Returns:
        Combined DataFrame for all years
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    years = list(ctx.get_year_range())
    log(f"\n{'='*60}")
    log(f"Fetching ESPN matchup data ({len(years)} years)")
    log(f"{'='*60}")

    all_dfs = []

    with ThreadPoolExecutor(max_workers=min(3, len(years))) as executor:
        futures = {executor.submit(fetch_espn_matchups, ctx, year): year for year in years}
        for future in as_completed(futures):
            year = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                log(f"  [MATCHUPS] {year}: FAILED - {e}")

    if not all_dfs:
        log("[MATCHUPS] No matchup data found for any year")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    log(f"[MATCHUPS] Total: {len(combined)} matchup rows across {len(all_dfs)} years")

    return combined
