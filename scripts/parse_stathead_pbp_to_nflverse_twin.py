"""Parse 1978-1998 Stathead play finder rows into an nflverse-shaped PBP file.

The output is intentionally *not* merged with nflverse. It is a strict schema
twin that lets us harden the pre-1999 parsing rules first, especially defensive
and special-teams atoms that matter for fantasy scoring.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATHEAD = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\stathead_generated\pbp_backfill_1978_1998\stathead_pbp_1978_1998_raw.parquet"
)
DEFAULT_REFERENCE = REPO_ROOT / "fantasy_football_data" / "cache" / "nflverse" / "nflverse_pbp_1999.parquet"
DEFAULT_OUT = DEFAULT_STATHEAD.with_name("stathead_pbp_1978_1998_nflverse_twin.parquet")
DEFAULT_AUDIT_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\_catalog\pbp_stathead_nflverse_twin_parse_20260507"
)


STATHEAD_TO_NFLVERSE = {
    "atl": "ATL",
    "buf": "BUF",
    "car": "CAR",
    "chi": "CHI",
    "cin": "CIN",
    "cle": "CLE",
    "clt": "IND",
    "crd": "ARI",
    "dal": "DAL",
    "den": "DEN",
    "det": "DET",
    "gnb": "GB",
    "htx": "HOU",
    "jax": "JAX",
    "kan": "KC",
    "mia": "MIA",
    "min": "MIN",
    "nor": "NO",
    "nwe": "NE",
    "nyg": "NYG",
    "nyj": "NYJ",
    "oti": "TEN",
    "phi": "PHI",
    "pit": "PIT",
    "rai": "LV",
    "ram": "LA",
    "rav": "BAL",
    "sdg": "LAC",
    "sea": "SEA",
    "sfo": "SF",
    "tam": "TB",
    "was": "WAS",
}

LOCATION_TO_NFLVERSE = {
    **{key.upper(): value for key, value in STATHEAD_TO_NFLVERSE.items()},
    # PFR/Stathead historical location strings that appear inside older text.
    "BALC": "IND",
    "CP": "ARI",
    "CRD": "ARI",
    "HOIL": "TEN",
    "LARD": "LV",
    "LARM": "LA",
    "MD": "MIA",
    "STLC": "ARI",
}

POSTSEASON_ROUND_OFFSET = {
    "WildCard": 1,
    "Division": 2,
    "ConfChamp": 3,
    "SuperBowl": 4,
}

FLOAT_DEFAULT_ZERO = {
    "quarter_end",
    "sp",
    "shotgun",
    "no_huddle",
    "qb_dropback",
    "qb_kneel",
    "qb_spike",
    "qb_scramble",
    "timeout",
    "punt_blocked",
    "first_down_rush",
    "first_down_pass",
    "first_down_penalty",
    "third_down_converted",
    "third_down_failed",
    "fourth_down_converted",
    "fourth_down_failed",
    "incomplete_pass",
    "touchback",
    "interception",
    "punt_inside_twenty",
    "punt_in_endzone",
    "punt_out_of_bounds",
    "punt_downed",
    "punt_fair_catch",
    "kickoff_inside_twenty",
    "kickoff_in_endzone",
    "kickoff_out_of_bounds",
    "kickoff_downed",
    "kickoff_fair_catch",
    "fumble_forced",
    "fumble_not_forced",
    "fumble_out_of_bounds",
    "solo_tackle",
    "safety",
    "penalty",
    "tackled_for_loss",
    "fumble_lost",
    "own_kickoff_recovery",
    "own_kickoff_recovery_td",
    "qb_hit",
    "rush_attempt",
    "pass_attempt",
    "sack",
    "touchdown",
    "pass_touchdown",
    "rush_touchdown",
    "return_touchdown",
    "extra_point_attempt",
    "two_point_attempt",
    "field_goal_attempt",
    "kickoff_attempt",
    "punt_attempt",
    "fumble",
    "complete_pass",
    "assist_tackle",
    "lateral_reception",
    "lateral_rush",
    "lateral_return",
    "lateral_recovery",
    "tackle_with_assist",
    "defensive_two_point_attempt",
    "defensive_two_point_conv",
    "defensive_extra_point_attempt",
    "defensive_extra_point_conv",
    "replay_or_challenge",
    "play_deleted",
    "special_teams_play",
    "aborted_play",
    "success",
    "pass",
    "rush",
    "first_down",
    "special",
    "play",
    "out_of_bounds",
    "home_opening_kickoff",
}

INT_DEFAULT_ZERO = {
    "away_score",
    "home_score",
    "result",
    "total",
    "div_game",
    "temp",
    "wind",
    "passer_jersey_number",
    "rusher_jersey_number",
    "receiver_jersey_number",
    "jersey_number",
}

NO_PLAY_MARKERS = ("(no play)",)

PASS_COMPLETE_RE = re.compile(
    r"^(?P<passer>.+?) pass complete to (?P<receiver>.+?) for (?:(?P<yards>-?\d+) yards?|no gain)",
    re.IGNORECASE,
)
PASS_INCOMPLETE_RE = re.compile(
    r"^(?P<passer>.+?) pass incomplete(?: intended for (?P<receiver>.+?))?(?:\.|$)",
    re.IGNORECASE,
)
INTERCEPTION_RE = re.compile(
    r"^(?P<passer>.+?) pass(?: intended for (?P<receiver>.+?))? is intercepted by (?P<interceptor>.+?)(?: at | and |\.|$)",
    re.IGNORECASE,
)
SACK_RE = re.compile(
    r"^(?P<passer>.+?) sacked by\s*(?P<sackers>.*?)(?: for (?P<yards>\d+) yards?)?(?:,|\.|$)",
    re.IGNORECASE,
)
FG_RE = re.compile(
    r"^(?P<kicker>.+?) (?P<distance>\d+) yard field goal (?P<result>good|no good|blocked)",
    re.IGNORECASE,
)
XP_RE = re.compile(
    r"^(?P<kicker>.+?) kicks extra point (?P<result>good|no good|failed|blocked)",
    re.IGNORECASE,
)
PUNT_RE = re.compile(r"^(?P<punter>.+?) punts (?P<distance>-?\d+) yards?", re.IGNORECASE)
BLOCKED_PUNT_RE = re.compile(r"^(?P<punter>.+?) punts blocked by (?P<blocker>.+?)(?:,|\.|$)", re.IGNORECASE)
PUNT_NODIST_RE = re.compile(r"^(?P<punter>.+?) punts(?:,|$)", re.IGNORECASE)
KICKOFF_RE = re.compile(r"^(?P<kicker>.+?) kicks off (?P<distance>\d+) yards?", re.IGNORECASE)
KICKOFF_NODIST_RE = re.compile(r"^(?P<kicker>.+?) kicks off no gain", re.IGNORECASE)
RETURN_RE = re.compile(
    r"returned by (?P<returner>.+?) for (?:(?P<yards>-?\d+) yards?|no gain)",
    re.IGNORECASE,
)
PENALTY_RE = re.compile(
    r"Penalty on (?P<who>[^:]+): (?P<ptype>[^,()]+)(?:, (?P<yards>\d+) yards?)? \((?P<status>accepted|declined)\)",
    re.IGNORECASE,
)
TACKLE_RE = re.compile(r"\(tackle by (?P<tacklers>[^)]+)\)", re.IGNORECASE)
FORCED_FUMBLE_RE = re.compile(r"forced by (?P<players>.+?)(?: for \d+ yards?|,|\))", re.IGNORECASE)
RECOVERY_RE = re.compile(r"recovered by (?P<player>.+?)(?: at | and |\.|$)", re.IGNORECASE)
BLOCK_RE = re.compile(r"blocked by (?P<player>.+?)(?:,|\.|$)", re.IGNORECASE)
RUSH_RE = re.compile(
    r"^(?P<rusher>.+?) (?P<where>left end|left tackle|left guard|right end|right tackle|right guard|up the middle|middle|left|right) for (?:(?P<yards>-?\d+) yards?|no gain)",
    re.IGNORECASE,
)
GENERIC_RUSH_RE = re.compile(
    r"^(?P<rusher>.+?) for (?:(?P<yards>-?\d+) yards?|no gain)(?:\s+\(|\s+Penalty|,|\.|$)",
    re.IGNORECASE,
)
ABORTED_SNAP_RE = re.compile(r"^(?P<player>.+?) aborted snap", re.IGNORECASE)
SIMPLE_FUMBLE_RE = re.compile(r"^(?P<player>.+?) fumbles(?:\s|\(|,|\.|$)", re.IGNORECASE)
PASS_FRAGMENT_RE = re.compile(r"^(?P<passer>.+?) pass(?: complete)?(?:\s+\(|$)", re.IGNORECASE)
INTENDED_FRAGMENT_RE = re.compile(r"^intended for (?P<receiver>.+?)(?:\s+\(|\.|$)", re.IGNORECASE)
SCRAMBLE_RE = re.compile(
    r"^(?P<rusher>.+?) scrambles(?: [a-z ]+?)? for (?:(?P<yards>-?\d+) yards?|no gain)",
    re.IGNORECASE,
)
KNEEL_RE = re.compile(r"^(?P<rusher>.+?) kneels?(?: for (?P<yards>-?\d+) yards?)?", re.IGNORECASE)
SPIKE_RE = re.compile(r"^(?P<passer>.+?) spikes?", re.IGNORECASE)


@dataclass
class PlayerRef:
    name: str
    player_id: str | None


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def to_float(value: Any) -> float | None:
    text = clean_str(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: Any) -> int | None:
    value_float = to_float(value)
    if value_float is None:
        return None
    return int(value_float)


def nfl_code(stathead_id: Any) -> str | None:
    return STATHEAD_TO_NFLVERSE.get(clean_str(stathead_id).lower())


def parse_week(
    season: int | None,
    game_date: str,
    dates_expected: str,
    labels_expected: str,
    regular_week_max_by_season: dict[int, int],
) -> tuple[int | None, str]:
    labels = [part.strip() for part in clean_str(labels_expected).split(";") if part.strip()]
    dates = [part.strip() for part in clean_str(dates_expected).split(";") if part.strip()]
    label = ""
    if len(labels) == 1:
        label = labels[0]
    elif clean_str(game_date) in dates:
        idx = dates.index(clean_str(game_date))
        if idx < len(labels):
            label = labels[idx]

    if not label:
        return None, "REG"
    if label.isdigit():
        return int(label), "REG"
    regular_max = regular_week_max_by_season.get(season or 0, 17)
    offset = POSTSEASON_ROUND_OFFSET.get(label)
    return (regular_max + offset if offset is not None else None), "POST"


def parse_clock(clock: Any) -> int | None:
    text = clean_str(clock)
    if not text or ":" not in text:
        return None
    minutes, seconds = text.split(":", 1)
    try:
        return int(minutes) * 60 + int(seconds)
    except ValueError:
        return None


def split_score(score: Any) -> tuple[int | None, int | None]:
    text = clean_str(score)
    if "-" not in text:
        return None, None
    left, right = text.split("-", 1)
    try:
        return int(left), int(right)
    except ValueError:
        return None, None


def parse_players(names_text: Any, ids_text: Any) -> list[PlayerRef]:
    names = [part.strip() for part in clean_str(names_text).split(";") if part.strip()]
    ids = [part.strip() for part in clean_str(ids_text).split(";")]
    players: list[PlayerRef] = []
    for idx, name in enumerate(names):
        raw_id = ids[idx].strip() if idx < len(ids) else ""
        players.append(PlayerRef(name=name, player_id=f"pfr:{raw_id}" if raw_id else None))
    return players


def find_player(name: str | None, players: list[PlayerRef]) -> PlayerRef:
    clean_name = clean_str(name)
    if not clean_name:
        return PlayerRef("", None)
    for player in players:
        if player.name == clean_name:
            return player
    return PlayerRef(clean_name, None)


def split_people(text: str) -> list[str]:
    cleaned = re.sub(r"\s+and\s+", ";", text.strip())
    cleaned = cleaned.replace(",", ";")
    return [part.strip() for part in cleaned.split(";") if part.strip()]


def set_player(out: dict[str, Any], prefix: str, player: PlayerRef) -> None:
    if not player.name:
        return
    out[f"{prefix}_player_name"] = player.name
    out[f"{prefix}_player_id"] = player.player_id


def parse_yard_words(value: str | None) -> float:
    if value is None or clean_str(value).lower() == "no gain":
        return 0.0
    return float(value)


def parse_location(location: Any, posteam: str | None) -> tuple[str | None, float | None, str | None]:
    text = clean_str(location)
    if not text:
        return None, None, None
    match = re.match(r"^(?P<side>[A-Za-z]+)\s*(?P<yard>\d+)$", text)
    if not match:
        return text, None, None
    side_raw = match.group("side").upper()
    yard = int(match.group("yard"))
    side = LOCATION_TO_NFLVERSE.get(side_raw, side_raw)
    if yard == 50:
        yardline_100 = 50.0
    elif posteam and side == posteam:
        yardline_100 = float(100 - yard)
    else:
        yardline_100 = float(yard)
    return f"{side} {yard}", yardline_100, side


def base_row(schema_names: list[str]) -> dict[str, Any]:
    out = {name: None for name in schema_names}
    for name in FLOAT_DEFAULT_ZERO:
        if name in out:
            out[name] = 0.0
    for name in INT_DEFAULT_ZERO:
        if name in out:
            out[name] = 0
    return out


def is_no_play(desc_lower: str) -> bool:
    return any(marker in desc_lower for marker in NO_PLAY_MARKERS)


def apply_tackles(
    out: dict[str, Any],
    desc: str,
    players: list[PlayerRef],
    default_team: str | None,
) -> None:
    match = TACKLE_RE.search(desc)
    if not match:
        return
    tacklers = [find_player(name, players) for name in split_people(match.group("tacklers"))]
    tacklers = [player for player in tacklers if player.name]
    if not tacklers:
        return
    if len(tacklers) == 1:
        out["solo_tackle"] = 1.0
        set_player(out, "solo_tackle_1", tacklers[0])
        out["solo_tackle_1_team"] = default_team
        return

    out["assist_tackle"] = 1.0
    out["tackle_with_assist"] = 1.0
    set_player(out, "tackle_with_assist_1", tacklers[0])
    out["tackle_with_assist_1_team"] = default_team
    set_player(out, "assist_tackle_1", tacklers[1])
    out["assist_tackle_1_team"] = default_team
    for idx, tackler in enumerate(tacklers[2:5], start=2):
        set_player(out, f"assist_tackle_{idx}", tackler)
        out[f"assist_tackle_{idx}_team"] = default_team


def apply_penalty(
    out: dict[str, Any], desc: str, players: list[PlayerRef], posteam: str | None, defteam: str | None
) -> None:
    match = PENALTY_RE.search(desc)
    if not match:
        return
    out["penalty"] = 1.0
    out["penalty_type"] = match.group("ptype").strip()
    out["penalty_yards"] = float(match.group("yards") or 0)
    who = match.group("who").strip()
    player = find_player(who, players)
    if player.player_id or player.name in {p.name for p in players}:
        set_player(out, "penalty", player)

    upper_who = who.upper()
    if upper_who in LOCATION_TO_NFLVERSE:
        out["penalty_team"] = LOCATION_TO_NFLVERSE[upper_who]
    elif out["penalty_type"].lower().startswith("offensive"):
        out["penalty_team"] = posteam
    elif out["penalty_type"].lower().startswith("defensive"):
        out["penalty_team"] = defteam


def apply_fumble(
    out: dict[str, Any],
    desc: str,
    players: list[PlayerRef],
    fumbler_team: str | None,
    opposite_team: str | None,
) -> None:
    lowered = desc.lower()
    if "fumble" not in lowered and "muffed" not in lowered:
        return
    out["fumble"] = 1.0

    fumbler = None
    for player in players:
        if f"{player.name} fumbles" in desc or f"muffed catch by {player.name}" in desc:
            fumbler = player
            break
    if fumbler:
        set_player(out, "fumbled_1", fumbler)
        out["fumbled_1_team"] = fumbler_team

    forced = FORCED_FUMBLE_RE.search(desc)
    if forced:
        forced_players = [find_player(name, players) for name in split_people(forced.group("players"))]
        forced_players = [player for player in forced_players if player.name]
        if forced_players:
            out["fumble_forced"] = 1.0
            set_player(out, "forced_fumble_player_1", forced_players[0])
            out["forced_fumble_player_1_team"] = opposite_team
        if len(forced_players) > 1:
            set_player(out, "forced_fumble_player_2", forced_players[1])
            out["forced_fumble_player_2_team"] = opposite_team
    else:
        out["fumble_not_forced"] = 1.0

    recovery = RECOVERY_RE.search(desc)
    if recovery:
        recovered = find_player(recovery.group("player"), players)
        set_player(out, "fumble_recovery_1", recovered)
        if recovered.name and fumbler and recovered.name == fumbler.name:
            out["fumble_recovery_1_team"] = fumbler_team
        elif "returned for" in lowered or "touchdown" in lowered:
            out["fumble_recovery_1_team"] = opposite_team
        else:
            # Conservative default: unknown-team recovery still preserves player.
            out["fumble_recovery_1_team"] = None
        ret = re.search(r"recovered by .+? returned for (?P<yards>-?\d+) yards?", desc, re.IGNORECASE)
        if ret:
            out["fumble_recovery_1_yards"] = float(ret.group("yards"))


def parse_play(
    row: pd.Series,
    schema_names: list[str],
    global_index: int,
    regular_week_max_by_season: dict[int, int],
) -> dict[str, Any]:
    out = base_row(schema_names)
    desc = clean_str(row.get("description"))
    desc_lower = desc.lower()
    players = parse_players(row.get("description_player_names"), row.get("description_pfr_player_ids"))

    season = to_int(row.get("season"))
    posteam = nfl_code(row.get("team_stathead_id"))
    defteam = nfl_code(row.get("opp_stathead_id"))
    boxscore_id = clean_str(row.get("boxscore_id"))
    home_stathead = boxscore_id[9:] if len(boxscore_id) > 9 else ""
    home_team = nfl_code(home_stathead)
    away_team = defteam if clean_str(row.get("team_stathead_id")).lower() == home_stathead else posteam
    week, season_type = parse_week(
        season,
        clean_str(row.get("game_date")),
        clean_str(row.get("game_dates_expected")),
        clean_str(row.get("week_labels_expected")),
        regular_week_max_by_season,
    )
    game_id = None
    if season and week and away_team and home_team:
        game_id = f"{season}_{week:02d}_{away_team}_{home_team}"

    qtr = to_int(row.get("quarter"))
    raw_time = clean_str(row.get("qtr_time_remain"))
    qtr_seconds = parse_clock(raw_time)
    half_seconds = None
    game_seconds = None
    game_half = None
    if qtr is not None and qtr_seconds is not None:
        if qtr <= 2:
            half_seconds = float((2 - qtr) * 900 + qtr_seconds)
            game_half = "Half1"
        elif qtr <= 4:
            half_seconds = float((4 - qtr) * 900 + qtr_seconds)
            game_half = "Half2"
        else:
            half_seconds = float(qtr_seconds)
            game_half = "Overtime"
        game_seconds = float(max(0, (4 - qtr) * 900 + qtr_seconds)) if qtr <= 4 else float(qtr_seconds)

    yrdln, yardline_100, side = parse_location(row.get("location"), posteam)
    down = to_int(row.get("down"))
    ydstogo = to_float(row.get("distance"))
    posteam_score, defteam_score = split_score(row.get("score"))
    home_score = None
    away_score = None
    if posteam_score is not None and defteam_score is not None:
        if posteam == home_team:
            home_score, away_score = posteam_score, defteam_score
        else:
            away_score, home_score = posteam_score, defteam_score

    out.update(
        {
            "play_id": float(global_index + 1),
            "game_id": game_id,
            "old_game_id": boxscore_id,
            "home_team": home_team,
            "away_team": away_team,
            "season_type": season_type,
            "week": week,
            "posteam": posteam,
            "posteam_type": "home" if posteam and posteam == home_team else "away",
            "defteam": defteam,
            "side_of_field": side,
            "yardline_100": yardline_100,
            "game_date": clean_str(row.get("game_date")),
            "quarter_seconds_remaining": float(qtr_seconds) if qtr_seconds is not None else None,
            "half_seconds_remaining": half_seconds,
            "game_seconds_remaining": game_seconds,
            "game_half": game_half,
            "qtr": float(qtr) if qtr is not None else None,
            "down": float(down) if down is not None else None,
            "goal_to_go": 1.0 if yardline_100 is not None and ydstogo is not None and yardline_100 <= ydstogo else 0.0,
            "time": raw_time or None,
            "yrdln": yrdln,
            "ydstogo": ydstogo,
            "desc": desc,
            "yards_gained": 0.0,
            "posteam_score": float(posteam_score) if posteam_score is not None else None,
            "defteam_score": float(defteam_score) if defteam_score is not None else None,
            "score_differential": float(posteam_score - defteam_score)
            if posteam_score is not None and defteam_score is not None
            else None,
            "ep": to_float(row.get("exp_pts_before")),
            "epa": to_float(row.get("exp_pts_diff")),
            "season": season,
            "order_sequence": float(global_index + 1),
            "away_score": int(away_score) if away_score is not None else 0,
            "home_score": int(home_score) if home_score is not None else 0,
            "nfl_api_id": f"stathead:{boxscore_id}:{clean_str(row.get('team_stathead_id'))}:{clean_str(row.get('row_index_in_query'))}",
        }
    )

    # success is DEFINED as epa > 0, so it is only knowable where epa is. It shares the
    # zero-default field list above, which meant a play with no scraped exp_pts_diff kept
    # success = 0.0 -- indistinguishable from a genuinely unsuccessful play. Stathead
    # returned no exp_pts_diff at all for 1993 (39,029 plays, the only such season in
    # 1978-2025), so the whole season shipped as "zero successful plays" and the
    # wave62 rollup's `success IS NOT NULL` guard could not see it. No evidence and
    # zero successes are different claims: absent epa must yield NULL, not 0.
    out["success"] = (1.0 if out["epa"] > 0 else 0.0) if out["epa"] is not None else None

    raw_yards = to_float(row.get("yards")) or 0.0
    no_play = is_no_play(desc_lower)
    if no_play:
        out["play_type"] = "no_play"

    fg = FG_RE.search(desc)
    xp = XP_RE.search(desc)
    punt = PUNT_RE.search(desc)
    blocked_punt = BLOCKED_PUNT_RE.search(desc)
    punt_nodist = PUNT_NODIST_RE.search(desc)
    kickoff = KICKOFF_RE.search(desc)
    kickoff_nodist = KICKOFF_NODIST_RE.search(desc)
    interception = INTERCEPTION_RE.search(desc)
    sack = SACK_RE.search(desc)
    complete = PASS_COMPLETE_RE.search(desc)
    incomplete = PASS_INCOMPLETE_RE.search(desc)
    scramble = SCRAMBLE_RE.search(desc)
    kneel = KNEEL_RE.search(desc)
    spike = SPIKE_RE.search(desc)
    rush = RUSH_RE.search(desc)
    generic_rush = GENERIC_RUSH_RE.search(desc)
    aborted_snap = ABORTED_SNAP_RE.search(desc)
    simple_fumble = SIMPLE_FUMBLE_RE.search(desc)
    pass_fragment = PASS_FRAGMENT_RE.search(desc)
    intended_fragment = INTENDED_FRAGMENT_RE.search(desc)

    fumbler_team = posteam
    fumbler_opposite = defteam
    return_team = None

    if xp:
        out["play_type"] = "extra_point"
        out["special_teams_play"] = 1.0
        out["special"] = 1.0
        out["extra_point_attempt"] = 1.0
        set_player(out, "kicker", find_player(xp.group("kicker"), players))
        result = xp.group("result").lower()
        out["extra_point_result"] = "good" if result == "good" else ("blocked" if result == "blocked" else "failed")
    elif fg:
        out["play_type"] = "field_goal"
        out["special_teams_play"] = 1.0
        out["special"] = 1.0
        out["field_goal_attempt"] = 1.0
        out["kick_distance"] = float(fg.group("distance"))
        set_player(out, "kicker", find_player(fg.group("kicker"), players))
        result = fg.group("result").lower()
        out["field_goal_result"] = "made" if result == "good" else ("blocked" if result == "blocked" else "missed")
    elif punt or blocked_punt or punt_nodist:
        out["play_type"] = "punt"
        out["special_teams_play"] = 1.0
        out["special"] = 1.0
        out["punt_attempt"] = 1.0
        if punt:
            out["kick_distance"] = float(punt.group("distance"))
            set_player(out, "punter", find_player(punt.group("punter"), players))
        elif blocked_punt:
            out["punt_blocked"] = 1.0
            set_player(out, "punter", find_player(blocked_punt.group("punter"), players))
            set_player(out, "blocked", find_player(blocked_punt.group("blocker"), players))
        else:
            set_player(out, "punter", find_player(punt_nodist.group("punter"), players))
        return_match = RETURN_RE.search(desc)
        if return_match:
            return_team = defteam
            out["return_team"] = return_team
            out["return_yards"] = parse_yard_words(return_match.group("yards"))
            set_player(out, "punt_returner", find_player(return_match.group("returner"), players))
            fumbler_team, fumbler_opposite = defteam, posteam
        out["touchback"] = 1.0 if "touchback" in desc_lower else 0.0
        out["punt_fair_catch"] = 1.0 if "fair catch" in desc_lower else 0.0
        out["punt_out_of_bounds"] = 1.0 if "out of bounds" in desc_lower else 0.0
    elif kickoff or kickoff_nodist:
        out["play_type"] = "kickoff"
        out["special_teams_play"] = 1.0
        out["special"] = 1.0
        out["kickoff_attempt"] = 1.0
        if kickoff:
            out["kick_distance"] = float(kickoff.group("distance"))
            set_player(out, "kicker", find_player(kickoff.group("kicker"), players))
        else:
            out["kick_distance"] = 0.0
            set_player(out, "kicker", find_player(kickoff_nodist.group("kicker"), players))
        return_match = RETURN_RE.search(desc)
        if return_match:
            return_team = defteam
            out["return_team"] = return_team
            out["return_yards"] = parse_yard_words(return_match.group("yards"))
            set_player(out, "kickoff_returner", find_player(return_match.group("returner"), players))
            fumbler_team, fumbler_opposite = defteam, posteam
        out["touchback"] = 1.0 if "touchback" in desc_lower else 0.0
        out["kickoff_fair_catch"] = 1.0 if "fair catch" in desc_lower else 0.0
        out["kickoff_out_of_bounds"] = 1.0 if "out of bounds" in desc_lower else 0.0
    elif interception:
        out["play_type"] = "pass"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        out["pass_attempt"] = 1.0
        out["interception"] = 1.0
        set_player(out, "passer", find_player(interception.group("passer"), players))
        if interception.group("receiver"):
            set_player(out, "receiver", find_player(interception.group("receiver"), players))
        set_player(out, "interception", find_player(interception.group("interceptor"), players))
        ret = re.search(r"returned for (?:(?P<yards>-?\d+) yards?|no gain)", desc, re.IGNORECASE)
        out["return_team"] = defteam
        out["return_yards"] = parse_yard_words(ret.group("yards") if ret else None)
        return_team = defteam
    elif sack:
        out["play_type"] = "pass"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        out["sack"] = 1.0
        out["yards_gained"] = raw_yards
        set_player(out, "passer", find_player(sack.group("passer"), players))
        sackers = [find_player(name, players) for name in split_people(sack.group("sackers"))]
        sackers = [player for player in sackers if player.name]
        if len(sackers) == 1:
            set_player(out, "sack", sackers[0])
        elif sackers:
            set_player(out, "half_sack_1", sackers[0])
            if len(sackers) > 1:
                set_player(out, "half_sack_2", sackers[1])
    elif complete:
        out["play_type"] = "pass"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        out["pass_attempt"] = 1.0
        out["complete_pass"] = 1.0
        out["yards_gained"] = raw_yards
        out["passing_yards"] = raw_yards
        out["receiving_yards"] = raw_yards
        set_player(out, "passer", find_player(complete.group("passer"), players))
        set_player(out, "receiver", find_player(complete.group("receiver"), players))
    elif incomplete:
        out["play_type"] = "pass"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        out["pass_attempt"] = 1.0
        out["incomplete_pass"] = 1.0
        set_player(out, "passer", find_player(incomplete.group("passer"), players))
        if incomplete.group("receiver"):
            set_player(out, "receiver", find_player(incomplete.group("receiver"), players))
    elif intended_fragment:
        out["play_type"] = "pass"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        out["pass_attempt"] = 1.0
        out["incomplete_pass"] = 1.0
        set_player(out, "receiver", find_player(intended_fragment.group("receiver"), players))
    elif pass_fragment:
        out["play_type"] = "pass"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        set_player(out, "passer", find_player(pass_fragment.group("passer"), players))
        if "pass complete" in desc_lower:
            out["complete_pass"] = 1.0
        else:
            out["incomplete_pass"] = 1.0
    elif spike:
        out["play_type"] = "qb_spike"
        out["pass"] = 1.0
        out["qb_dropback"] = 1.0
        out["qb_spike"] = 1.0
        set_player(out, "passer", find_player(spike.group("passer"), players))
    elif kneel:
        out["play_type"] = "qb_kneel"
        out["rush"] = 1.0
        out["rush_attempt"] = 1.0
        out["qb_kneel"] = 1.0
        out["yards_gained"] = raw_yards
        out["rushing_yards"] = raw_yards
        set_player(out, "rusher", find_player(kneel.group("rusher"), players))
    elif scramble:
        out["play_type"] = "run"
        out["rush"] = 1.0
        out["pass"] = 1.0
        out["rush_attempt"] = 1.0
        out["qb_dropback"] = 1.0
        out["qb_scramble"] = 1.0
        out["yards_gained"] = raw_yards
        out["rushing_yards"] = raw_yards
        set_player(out, "rusher", find_player(scramble.group("rusher"), players))
    elif rush:
        out["play_type"] = "run"
        out["rush"] = 1.0
        out["rush_attempt"] = 1.0
        out["yards_gained"] = raw_yards
        out["rushing_yards"] = raw_yards
        out["run_location"] = (
            "middle"
            if "middle" in rush.group("where").lower()
            else (
                "left"
                if rush.group("where").lower().startswith("left")
                else ("right" if rush.group("where").lower().startswith("right") else None)
            )
        )
        set_player(out, "rusher", find_player(rush.group("rusher"), players))
    elif generic_rush:
        out["play_type"] = "run"
        out["rush"] = 1.0
        out["rush_attempt"] = 1.0
        out["yards_gained"] = raw_yards
        out["rushing_yards"] = raw_yards
        set_player(out, "rusher", find_player(generic_rush.group("rusher"), players))
    elif aborted_snap:
        out["play_type"] = "run"
        out["rush"] = 1.0
        out["rush_attempt"] = 1.0
        out["aborted_play"] = 1.0
        out["yards_gained"] = raw_yards
        out["rushing_yards"] = raw_yards
        set_player(out, "rusher", find_player(aborted_snap.group("player"), players))
    elif simple_fumble:
        out["play_type"] = "run"
        out["rush"] = 1.0
        out["rush_attempt"] = 1.0
        out["yards_gained"] = raw_yards
        out["rushing_yards"] = raw_yards
        set_player(out, "rusher", find_player(simple_fumble.group("player"), players))
    elif desc_lower.startswith("penalty on "):
        out["play_type"] = "no_play"

    if no_play:
        out["play_type"] = "no_play"
        out["play"] = 0.0
    elif out["play_type"]:
        out["play"] = 1.0

    if out["play_type"] in {"punt", "kickoff", "field_goal", "extra_point"}:
        out["yards_gained"] = 0.0

    apply_penalty(out, desc, players, posteam, defteam)

    block = BLOCK_RE.search(desc)
    if block:
        set_player(out, "blocked", find_player(block.group("player"), players))
        if out["play_type"] == "punt":
            out["punt_blocked"] = 1.0

    tackle_team = defteam
    if return_team:
        tackle_team = posteam if return_team == defteam else defteam
    apply_tackles(out, desc, players, tackle_team)

    apply_fumble(out, desc, players, fumbler_team, fumbler_opposite)

    if out["play_type"] == "run" and raw_yards < 0 and out.get("solo_tackle_1_player_name"):
        out["tackled_for_loss"] = 1.0
        out["tackle_for_loss_1_player_name"] = out["solo_tackle_1_player_name"]
        out["tackle_for_loss_1_player_id"] = out["solo_tackle_1_player_id"]

    if "touchdown" in desc_lower and not no_play:
        out["touchdown"] = 1.0
        out["sp"] = 1.0
        if out["play_type"] == "pass" and out.get("complete_pass") == 1.0:
            out["pass_touchdown"] = 1.0
            out["td_team"] = posteam
            out["td_player_name"] = out.get("receiver_player_name")
            out["td_player_id"] = out.get("receiver_player_id")
        elif out["play_type"] in {"run", "qb_kneel"}:
            out["rush_touchdown"] = 1.0
            out["td_team"] = posteam
            out["td_player_name"] = out.get("rusher_player_name")
            out["td_player_id"] = out.get("rusher_player_id")
        elif out.get("interception") == 1.0:
            out["return_touchdown"] = 1.0
            out["td_team"] = defteam
            out["td_player_name"] = out.get("interception_player_name")
            out["td_player_id"] = out.get("interception_player_id")
        elif out["play_type"] in {"punt", "kickoff"}:
            out["return_touchdown"] = 1.0
            out["td_team"] = defteam
            if out["play_type"] == "punt":
                out["td_player_name"] = out.get("punt_returner_player_name")
                out["td_player_id"] = out.get("punt_returner_player_id")
            else:
                out["td_player_name"] = out.get("kickoff_returner_player_name")
                out["td_player_id"] = out.get("kickoff_returner_player_id")
        elif out.get("fumble_recovery_1_player_name"):
            out["return_touchdown"] = 1.0
            out["td_player_name"] = out.get("fumble_recovery_1_player_name")
            out["td_player_id"] = out.get("fumble_recovery_1_player_id")
            out["td_team"] = out.get("fumble_recovery_1_team")

    first_down = False
    if (
        down
        and ydstogo is not None
        and out.get("yards_gained") is not None
        and out["play_type"] not in {"no_play", None}
    ):
        first_down = bool(out["touchdown"] or float(out["yards_gained"]) >= ydstogo)
    if first_down:
        out["first_down"] = 1.0
        if out["play_type"] == "pass":
            out["first_down_pass"] = 1.0
        elif out["play_type"] in {"run", "qb_kneel"}:
            out["first_down_rush"] = 1.0
    if down == 3 and out["play_type"] not in {"no_play", None}:
        out["third_down_converted" if first_down else "third_down_failed"] = 1.0
    if down == 4 and out["play_type"] not in {"no_play", None}:
        out["fourth_down_converted" if first_down else "fourth_down_failed"] = 1.0

    out["passer"] = out.get("passer_player_name")
    out["passer_id"] = out.get("passer_player_id")
    out["rusher"] = out.get("rusher_player_name")
    out["rusher_id"] = out.get("rusher_player_id")
    out["receiver"] = out.get("receiver_player_name")
    out["receiver_id"] = out.get("receiver_player_id")
    out["fantasy_player_name"] = out.get("receiver_player_name") or out.get("rusher_player_name")
    out["fantasy_player_id"] = out.get("receiver_player_id") or out.get("rusher_player_id")
    out["fantasy"] = out["fantasy_player_name"]
    out["fantasy_id"] = out["fantasy_player_id"]

    return out


def cast_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    arrays = []
    for field in schema:
        series = df[field.name] if field.name in df.columns else pd.Series([None] * len(df))
        if pa.types.is_integer(field.type):
            series = pd.to_numeric(series, errors="coerce").astype("Int64")
        elif pa.types.is_floating(field.type):
            series = pd.to_numeric(series, errors="coerce").astype("float64")
        elif pa.types.is_string(field.type):
            series = series.astype("string")
        arrays.append(pa.array(series, type=field.type, from_pandas=True))
    return pa.Table.from_arrays(arrays, schema=schema)


def build_regular_week_max_by_season(stathead_path: Path, batch_size: int) -> dict[int, int]:
    source = pq.ParquetFile(stathead_path)
    max_by_season: dict[int, int] = {}
    for batch in source.iter_batches(batch_size=batch_size, columns=["season", "week_labels_expected"]):
        df = batch.to_pandas()
        for _, row in df.drop_duplicates().iterrows():
            season = to_int(row.get("season"))
            if season is None:
                continue
            labels = [part.strip() for part in clean_str(row.get("week_labels_expected")).split(";") if part.strip()]
            for label in labels:
                if label.isdigit():
                    max_by_season[season] = max(max_by_season.get(season, 0), int(label))
    return max_by_season


def summarize_batch(rows: list[dict[str, Any]], counts: Counter[str]) -> None:
    for row in rows:
        counts["rows"] += 1
        counts[f"play_type:{row.get('play_type') or 'null'}"] += 1
        for field in (
            "pass_attempt",
            "complete_pass",
            "rush_attempt",
            "sack",
            "interception",
            "fumble",
            "fumble_forced",
            "solo_tackle",
            "assist_tackle",
            "punt_attempt",
            "kickoff_attempt",
            "field_goal_attempt",
            "extra_point_attempt",
            "penalty",
            "touchdown",
            "return_touchdown",
        ):
            if row.get(field) == 1.0:
                counts[field] += 1
        for prefix in (
            "passer",
            "receiver",
            "rusher",
            "sack",
            "half_sack_1",
            "interception",
            "solo_tackle_1",
            "assist_tackle_1",
            "forced_fumble_player_1",
            "fumble_recovery_1",
            "punter",
            "kicker",
            "punt_returner",
            "kickoff_returner",
        ):
            if row.get(f"{prefix}_player_name"):
                counts[f"{prefix}_name_populated"] += 1
            if row.get(f"{prefix}_player_id"):
                counts[f"{prefix}_id_populated"] += 1


def parse_to_twin(
    stathead_path: Path,
    reference_path: Path,
    out_path: Path,
    audit_dir: Path,
    batch_size: int,
    sample_rows: int | None,
) -> dict[str, Any]:
    reference_schema = pq.ParquetFile(reference_path).schema_arrow
    schema_names = reference_schema.names
    audit_dir.mkdir(parents=True, exist_ok=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    source = pq.ParquetFile(stathead_path)
    regular_week_max_by_season = build_regular_week_max_by_season(stathead_path, batch_size)
    writer = pq.ParquetWriter(out_path, reference_schema, compression="zstd")
    counts: Counter[str] = Counter()
    global_index = 0
    try:
        for batch in source.iter_batches(batch_size=batch_size):
            df = batch.to_pandas()
            if sample_rows is not None:
                remaining = sample_rows - global_index
                if remaining <= 0:
                    break
                df = df.head(remaining)

            parsed_rows = []
            for _, row in df.iterrows():
                parsed_rows.append(parse_play(row, schema_names, global_index, regular_week_max_by_season))
                global_index += 1

            summarize_batch(parsed_rows, counts)
            parsed_df = pd.DataFrame(parsed_rows, columns=schema_names)
            writer.write_table(cast_table(parsed_df, reference_schema))
            print(f"parsed {global_index:,} rows", flush=True)
    finally:
        writer.close()

    manifest = {
        "source": str(stathead_path),
        "reference_schema": str(reference_path),
        "output": str(out_path),
        "rows_written": global_index,
        "schema_columns": len(schema_names),
        "sample_rows": sample_rows,
        "regular_week_max_by_season": regular_week_max_by_season,
        "counts": dict(sorted(counts.items())),
        "notes": [
            "Player id columns use pfr:<id> namespace from Stathead links, not nflverse GSIS ids.",
            "Unsupported nflverse modeling columns remain null/zero until a later enrichment pass.",
            "This file is intentionally not merged with 1999-2025 nflverse PBP yet.",
        ],
    }
    manifest_path = audit_dir / "parse_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stathead", type=Path, default=DEFAULT_STATHEAD)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--sample-rows", type=int, default=None)
    args = parser.parse_args()

    manifest = parse_to_twin(
        stathead_path=args.stathead,
        reference_path=args.reference,
        out_path=args.out,
        audit_dir=args.audit_dir,
        batch_size=args.batch_size,
        sample_rows=args.sample_rows,
    )
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
