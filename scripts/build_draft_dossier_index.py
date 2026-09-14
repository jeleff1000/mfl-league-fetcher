#!/usr/bin/env python3
"""Compile draft intelligence discovery artifacts into product indexes.

The fleet miners intentionally overproduce. This script turns those discovery
files into compact, deterministic lookup tables that the app can lazy-load for
manager and league dossier pages without scanning every raw signal per request.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone, UTC
import json
import math
from pathlib import Path
import statistics
from typing import Any


MODEL_VERSION = "draft-dossier-index-v0.2"
TEAM_FEATURES = {"nfl_draft_team", "nfl_team_at_draft", "current_nfl_team"}
ENTITY_FEATURES = TEAM_FEATURES | {"college", "conference"}
ALLOCATION_FEATURES = {
    "draft_phase",
    "capital_bucket",
    "capital_tier",
    "position_capital_bucket",
    "position_capital_tier",
    "position_draft_phase",
    "format_position",
    "format_capital_tier",
    "position_slot",
    "position_slot_tier",
    "position_slot_capital_tier",
    "position_slot_phase",
    "position_draft_label",
    "drafted_as_starter",
}

TEAM_LABELS = {
    "ARI": "Cardinals-drafted players",
    "ATL": "Falcons-drafted players",
    "BAL": "Ravens-drafted players",
    "BUF": "Bills-drafted players",
    "CAR": "Panthers-drafted players",
    "CHI": "Bears-drafted players",
    "CIN": "Bengals-drafted players",
    "CLE": "Browns-drafted players",
    "DAL": "Cowboys-drafted players",
    "DEN": "Broncos-drafted players",
    "DET": "Lions-drafted players",
    "GB": "Packers-drafted players",
    "HOU": "Texans-drafted players",
    "IND": "Colts-drafted players",
    "JAX": "Jaguars-drafted players",
    "KC": "Chiefs-drafted players",
    "LAC": "Chargers-drafted players",
    "LAR": "Rams-drafted players",
    "LV": "Raiders-drafted players",
    "MIA": "Dolphins-drafted players",
    "MIN": "Vikings-drafted players",
    "NE": "Patriots-drafted players",
    "NO": "Saints-drafted players",
    "NYG": "Giants-drafted players",
    "NYJ": "Jets-drafted players",
    "PHI": "Eagles-drafted players",
    "PIT": "Steelers-drafted players",
    "SEA": "Seahawks-drafted players",
    "SF": "49ers-drafted players",
    "TB": "Buccaneers-drafted players",
    "TEN": "Titans-drafted players",
    "WAS": "Commanders-drafted players",
}

VALUE_LABELS = {
    "age_23_under": "age-23-and-under players",
    "age_24_26": "prime-age players",
    "age_27_29": "older-prime players",
    "age_30_plus": "30-plus players",
    "experience_rookie": "rookies",
    "experience_year_2": "second-year players",
    "experience_year_3_4": "year-3/4 players",
    "experience_veteran": "veterans",
    "phase_early": "premium/early draft shelf",
    "phase_middle": "middle draft shelf",
    "phase_late": "late draft/cheap shelf",
    "no_recent_same_position": "after no position run",
    "same_position_recent": "after a small position run",
    "same_position_run": "inside an active position run",
    "ras_9_plus": "elite athletic testers",
    "ras_8_9": "strong athletic testers",
    "ras_6_8": "average athletic testers",
    "ras_under_6": "lower athletic testers",
    "height_77_plus": "very tall players",
    "height_73_76": "taller players",
    "height_69_72": "shorter players",
    "height_under_69": "very short players",
    "weight_under_200": "sub-200 lb players",
    "weight_200_219": "200-219 lb players",
    "weight_220_249": "220-249 lb players",
    "weight_250_plus": "250-plus lb players",
    "nfl_round_1": "first-round NFL picks",
    "nfl_round_2_3": "Day 2 NFL picks",
    "nfl_round_4_7": "late-round NFL picks",
    "undrafted": "undrafted players",
    "auction_50_plus": "$50-plus auction shelf",
    "auction_30_49": "$30-49 auction shelf",
    "auction_16_29": "$16-29 auction shelf",
    "auction_6_15": "$6-15 auction shelf",
    "auction_1_5": "$1-5 auction shelf",
    "snake_r1_2": "round 1-2 shelf",
    "snake_r3_5": "round 3-5 shelf",
    "snake_r6_9": "round 6-9 shelf",
    "snake_r10_plus": "late-round shelf",
    "capital_premium": "premium capital",
    "capital_core": "core capital",
    "capital_depth": "depth capital",
    "capital_flyer": "flyer capital",
    "prior_allpro": "players with prior All-Pro seasons",
    "prior_multi_allpro": "multi All-Pro players",
    "prior_probowl": "players with prior Pro Bowls",
    "prior_major_award": "prior award winners",
    "prior_award_mention": "players with prior award votes",
    "prev_allpro": "previous-season All-Pro players",
    "prev_probowl": "previous-season Pro Bowl players",
    "prev_major_award": "reigning award winners",
    "prev_award_mention": "players with award votes last season",
    "no_prior_awards": "players without prior awards",
    "decorated_veteran": "decorated veterans",
    "undecorated_veteran": "veterans without major awards",
    "mobile_qb": "mobile QBs",
    "pocket_or_low_rush_qb": "pocket/low-rush QBs",
    "pass_catching_rb": "pass-catching RBs",
    "non_receiving_rb": "RBs without a pass-catching resume",
    "workhorse_rb": "workhorse RBs",
    "committee_or_low_carry_rb": "committee/low-carry RBs",
    "WR_target_earner": "target-earning WRs",
    "WR_low_volume_profile": "lower-volume WRs",
    "TE_target_earner": "target-earning TEs",
    "TE_low_volume_profile": "lower-volume TEs",
    "prior_injury_discount": "players coming off missed time",
    "partial_prior_season": "players from partial prior seasons",
    "full_prior_season": "players from full prior seasons",
}

PLAYER_ARCHETYPE_LABELS = {
    "QB_mobile": "mobile QBs",
    "RB_rookie": "rookie RBs",
    "RB_pass_catcher": "pass-catching RBs",
    "RB_workhorse": "workhorse RBs",
    "WR_target_earner": "target-earning WRs",
    "TE_target_earner": "target-earning TEs",
    "TE_elite_athlete": "elite-athlete TEs",
    "prior_injury_discount": "prior-injury discounts",
}

POSITION_NAMES = {
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "K": "K",
    "DEF": "DST",
}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _median(values: list[float]) -> float:
    clean = [value for value in values if not math.isnan(value)]
    return statistics.median(clean) if clean else 0.0


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _titleize(value: Any) -> str:
    return " ".join(part.capitalize() for part in str(value or "").replace("_", " ").split())


def _plural_position(value: Any) -> str:
    normalized = str(value or "").upper()
    if normalized == "DEF":
        return "DSTs"
    if normalized == "K":
        return "kickers"
    if normalized in POSITION_NAMES:
        return f"{POSITION_NAMES[normalized]}s"
    return f"{_titleize(value)}s"


def _position_draft_label(value: Any) -> str:
    raw = str(value or "").strip()
    letters = "".join(char for char in raw if char.isalpha())
    digits = "".join(char for char in raw if char.isdigit())
    if not letters or not digits:
        return _titleize(raw)
    pos = POSITION_NAMES.get(letters.upper(), letters.upper())
    slot = _int(digits)
    if slot == 1:
        return f"{pos} anchor slot"
    if slot == 2:
        return f"{pos} second slot"
    if slot >= 6:
        return f"{pos} depth slots"
    return f"{pos}{slot} slot"


def _friendly_position_value(value: Any) -> str | None:
    raw = str(value or "")
    parts = raw.split("_")
    if not parts:
        return None
    pos = POSITION_NAMES.get(parts[0].upper())
    if not pos:
        return None
    rest = "_".join(parts[1:])
    if not rest:
        return None
    return f"{pos} {VALUE_LABELS.get(rest, _titleize(rest))}"


def _position_compound_label(value: Any) -> str | None:
    raw = str(value or "")
    parts = raw.split("_")
    if not parts:
        return None
    pos = POSITION_NAMES.get(parts[0].upper())
    if not pos:
        return None
    rest = "_".join(parts[1:])
    if not rest:
        return None
    return f"{pos} {VALUE_LABELS.get(rest, _titleize(rest))}"


def _slot_compound_label(value: Any) -> str | None:
    raw = str(value or "")
    parts = raw.split("_")
    if len(parts) < 2:
        return None
    slot = _position_draft_label(parts[0])
    rest = "_".join(parts[1:])
    return f"{slot}, {VALUE_LABELS.get(rest, _titleize(rest))}"


def _format_compound_label(value: Any) -> str:
    raw = str(value or "")
    parts = raw.split("_")
    if len(parts) < 2:
        return _titleize(raw)
    return f"{parts[0].lower()} {VALUE_LABELS.get('_'.join(parts[1:]), _titleize('_'.join(parts[1:])))}"


def _feature_category(feature_type: Any) -> str:
    categories = {
        "age_bucket": "Age",
        "experience_bucket": "Experience",
        "position_age": "Position Age",
        "position_experience": "Position Experience",
        "position": "Position",
        "position_draft_label": "Roster Construction",
        "drafted_as_starter": "Starter Pattern",
        "draft_age_grade": "Age Value",
        "draft_phase": "Draft Phase",
        "nfl_draft_capital": "Draft Pedigree",
        "nfl_draft_team": "NFL Draft Team",
        "nfl_team_at_draft": "NFL Team Context",
        "current_nfl_team": "NFL Team Context",
        "conference": "College Conference",
        "college": "College",
        "ras_bucket": "RAS",
        "player_archetype": "Player Archetype",
        "qb_mobility_profile": "Prior Production",
        "rb_receiving_profile": "Prior Production",
        "rb_workload_profile": "Prior Production",
        "receiving_volume_profile": "Prior Production",
        "availability_profile": "Prior Production",
        "award_history": "Prior Awards",
        "bio_profile": "Player Profile",
        "weight_bucket": "Size",
        "height_bucket": "Size",
        "position_height": "Position Size",
        "position_weight": "Position Size",
        "position_ras": "Position RAS",
        "capital_bucket": "Draft Capital",
        "capital_tier": "Draft Capital",
        "position_capital_bucket": "Capital Allocation",
        "position_capital_tier": "Capital Allocation",
        "position_draft_phase": "Draft Phase",
        "format_position": "Format Allocation",
        "format_capital_tier": "Format Allocation",
        "position_slot": "Roster Slot",
        "position_slot_tier": "Roster Slot",
        "position_slot_capital_tier": "Slot Capital",
        "position_slot_phase": "Slot Timing",
        "draft_state": "Draft State",
    }
    raw = str(feature_type or "")
    return categories.get(raw, _titleize(raw))


def _feature_label(feature_type: Any, feature_value: Any) -> str:
    raw_type = str(feature_type or "")
    raw_value = str(feature_value or "")
    if raw_type in TEAM_FEATURES:
        return TEAM_LABELS.get(raw_value, f"{raw_value}-drafted players")
    if raw_type == "position":
        return _plural_position(raw_value)
    if raw_type == "position_draft_label" or raw_type == "position_slot":
        return _position_draft_label(raw_value)
    if raw_type in {"position_age", "position_experience", "position_height", "position_weight", "position_ras"}:
        return _friendly_position_value(raw_value) or _titleize(raw_value)
    if raw_type in {"capital_bucket", "capital_tier", "draft_phase"}:
        return VALUE_LABELS.get(raw_value, _titleize(raw_value))
    if raw_type in {"position_capital_bucket", "position_capital_tier", "position_draft_phase"}:
        return _position_compound_label(raw_value) or _titleize(raw_value)
    if raw_type in {"format_position", "format_capital_tier"}:
        return _format_compound_label(raw_value)
    if raw_type in {"position_slot_tier", "position_slot_capital_tier", "position_slot_phase"}:
        return _slot_compound_label(raw_value) or _titleize(raw_value)
    if raw_type == "drafted_as_starter":
        return "players drafted into starting slots" if raw_value == "starter" else "bench/depth picks"
    if raw_type == "player_archetype":
        return PLAYER_ARCHETYPE_LABELS.get(raw_value, _titleize(raw_value))
    if raw_type == "conference":
        return str(feature_value or "").replace(" Conference", "")
    if raw_type == "college":
        return f"{feature_value} players"
    if raw_type == "draft_state":
        if raw_value.startswith("phase_"):
            parts = raw_value.split("_")
            if len(parts) > 2:
                phase = "_".join(parts[:2])
                rest = "_".join(parts[2:])
                return f"{VALUE_LABELS.get(phase, _titleize(phase))}, {VALUE_LABELS.get(rest, _titleize(rest))}"
        return VALUE_LABELS.get(raw_value, _titleize(raw_value))
    return VALUE_LABELS.get(raw_value, _titleize(raw_value))


def _format_count(value: Any) -> str:
    number = _float(value)
    if abs(number - round(number)) < 0.05:
        return str(int(round(number)))
    return f"{number:.1f}"


def _format_pct(value: Any) -> str:
    pct = _float(value) * 100
    if 0 < abs(pct) < 1:
        return f"{pct:.1f}%"
    return f"{round(pct):.0f}%"


def _season_text(value: Any) -> str:
    seasons = _int(value)
    return f"{seasons} season{'s' if seasons != 1 else ''}"


def _confidence_label(score: Any) -> str:
    value = _float(score)
    if value >= 85:
        return "High confidence"
    if value >= 75:
        return "Solid read"
    if value >= 65:
        return "Directional read"
    return "Watch list"


def _direction_label(metric: str, direction: str, *, league: bool = False) -> str:
    if metric == "absence_affinity":
        return "Rare profile"
    if league:
        return "Beats market" if direction == "overweight" else "Lags market"
    return "Above room" if direction == "overweight" else "Below room"


def _manager_signal_read(signal: dict[str, Any]) -> str:
    metric = str(signal.get("signal_metric") or "capital_affinity")
    direction = str(signal.get("signal_direction") or "overweight")
    label = str(signal.get("feature_label") or _feature_label(signal.get("feature_type"), signal.get("feature_value")))
    if metric == "absence_affinity":
        return f"Has almost never drafted {label}."
    if metric == "pick_affinity":
        preposition = "in" if any(token in label for token in ("shelf", "capital", "slot")) else "on"
        return (
            f"Uses more picks {preposition} {label} than the room."
            if direction == "overweight"
            else f"Uses fewer picks {preposition} {label} than the room."
        )
    preposition = "in" if any(token in label for token in ("shelf", "capital", "slot")) else "to"
    return (
        f"Commits more draft capital {preposition} {label} than the room."
        if direction == "overweight"
        else f"Commits less draft capital {preposition} {label} than the room."
    )


def _manager_signal_evidence(signal: dict[str, Any]) -> str:
    metric = str(signal.get("signal_metric") or "capital_affinity")
    seasons = _season_text(signal.get("years_seen"))
    if metric == "absence_affinity":
        return f"No picks; about {_format_count(signal.get('expected_count'))} would be expected over {seasons}."
    if metric == "pick_affinity":
        return f"{_format_count(signal.get('picks'))} picks vs about {_format_count(signal.get('expected_count'))} expected over {seasons}."
    return (
        f"{_format_pct(signal.get('observed_share'))} of draft capital vs "
        f"{_format_pct(signal.get('expected_share'))} room average; "
        f"{_format_count(signal.get('picks'))} picks over {seasons}."
    )


def _manager_signal_why_it_matters(signal: dict[str, Any]) -> str:
    metric = str(signal.get("signal_metric") or "capital_affinity")
    direction = str(signal.get("signal_direction") or "overweight")
    feature_type = str(signal.get("feature_type") or "")
    if metric == "absence_affinity":
        return "Use as a fade tiebreaker, not a hard rule."
    if feature_type in TEAM_FEATURES or feature_type in {"college", "conference"}:
        return "Low-priority fingerprint for nominations after stronger player-profile reads."
    if metric == "pick_affinity":
        family = str(signal.get("signal_family") or "")
        context = "when the draft reaches this zone" if family == "allocation" else "when players fit this profile"
        return (
            f"Expect this manager to show up {context}."
            if direction == "overweight"
            else "Lower volume means less pressure, not a full fade."
        )
    return (
        "Treat as price pressure when this manager is active."
        if direction == "overweight"
        else "Treat as a discount check, not a ban."
    )


def _decorate_manager_signal(signal: dict[str, Any]) -> dict[str, Any]:
    metric = str(signal.get("signal_metric") or "capital_affinity")
    direction = str(signal.get("signal_direction") or "overweight")
    label = _feature_label(signal.get("feature_type"), signal.get("feature_value"))
    signal.update(
        {
            "category": _feature_category(signal.get("feature_type")),
            "feature_label": label,
            "direction": direction,
            "direction_label": _direction_label(metric, direction),
            "confidence_label": _confidence_label(signal.get("confidence")),
        }
    )
    signal["read"] = _manager_signal_read(signal)
    signal["evidence"] = _manager_signal_evidence(signal)
    signal["why_it_matters"] = _manager_signal_why_it_matters(signal)
    return signal


def _league_signal_read(signal: dict[str, Any]) -> str:
    label = str(signal.get("feature_label") or _feature_label(signal.get("feature_type"), signal.get("feature_value")))
    direction = str(signal.get("signal_direction") or "overweight")
    if direction == "overweight":
        return f"This league has gotten more value than expected from {label}."
    return f"This league has gotten less value than expected from {label}."


def _league_signal_evidence(signal: dict[str, Any]) -> str:
    value_delta = _float(signal.get("value_delta"))
    pick_delta = _float(signal.get("pick_score_delta"))
    pick_text = ""
    if abs(pick_delta) >= 0.05 and (pick_delta == 0 or math.copysign(1, pick_delta) == math.copysign(1, value_delta)):
        pick_text = f"; pick quality {pick_delta:+.2f}"
    return (
        f"{value_delta:+.1f} LAMAR vs expected{pick_text} "
        f"across {_format_count(signal.get('picks'))} picks and {_season_text(signal.get('years_seen'))}."
    )


def _league_signal_why_it_matters(signal: dict[str, Any]) -> str:
    if str(signal.get("signal_direction") or "overweight") == "overweight":
        return "Use as a value pocket when the board creates a similar choice."
    return "Use as a price check; similar buys need a discount before following the room."


def _decorate_league_signal(signal: dict[str, Any]) -> dict[str, Any]:
    direction = str(signal.get("signal_direction") or "overweight")
    confidence = _float(signal.get("confidence") or signal.get("validation_score"))
    signal.update(
        {
            "category": _feature_category(signal.get("feature_type")),
            "feature_label": _feature_label(signal.get("feature_type"), signal.get("feature_value")),
            "direction": direction,
            "direction_label": _direction_label("value_inefficiency", direction, league=True),
            "confidence": round(confidence, 3),
            "confidence_label": _confidence_label(confidence),
        }
    )
    signal["read"] = _league_signal_read(signal)
    signal["evidence"] = _league_signal_evidence(signal)
    signal["why_it_matters"] = _league_signal_why_it_matters(signal)
    return signal


def _archetype_metric(row: dict[str, Any]) -> str:
    try:
        evidence = json.loads(str(row.get("evidence_json") or "[]"))
    except json.JSONDecodeError:
        evidence = []
    if evidence and isinstance(evidence[0], dict):
        return str(evidence[0].get("signal_metric") or "capital_affinity")
    return "capital_affinity"


def _archetype_label(row: dict[str, Any]) -> str:
    metric = _archetype_metric(row)
    direction = str(row.get("primary_signal_direction") or "overweight")
    label = _feature_label(row.get("primary_feature_type"), row.get("primary_feature_value"))
    if metric == "absence_affinity":
        return f"Rarely drafted: {label}"
    prefix = "Above-room" if direction == "overweight" else "Below-room"
    kind = "pick volume" if metric == "pick_affinity" else "draft capital"
    return f"{prefix} {kind}: {label}"


def _archetype_summary(row: dict[str, Any]) -> str:
    label = _feature_label(row.get("primary_feature_type"), row.get("primary_feature_value"))
    direction = str(row.get("primary_signal_direction") or "overweight")
    direction_text = "above-room" if direction == "overweight" else "below-room"
    return (
        f"Fleet comparison cluster for {direction_text} {label}; use only as support behind the direct manager signals."
    )


def _signal_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        _norm(row.get("signal_metric") or "capital_affinity"),
        _norm(row.get("feature_type")),
        _norm(row.get("feature_value")),
        _norm(row.get("signal_direction")),
    )


def _assignment_scope_key(row: dict[str, Any]) -> tuple[str, str, tuple[str, str, str, str]]:
    return (
        _norm(row.get("db_name")),
        _norm(row.get("scope_label")),
        _signal_key(row),
    )


def _confidence(row: dict[str, Any], assignment: dict[str, Any] | None = None) -> float:
    score = _float(
        (assignment or {}).get("validation_score") or (assignment or {}).get("assignment_score"), float("nan")
    )
    if not math.isnan(score):
        return round(score, 3)
    z_score = abs(_float(row.get("signal_z_score") or row.get("capital_z_score")))
    repeatability = _float(row.get("repeatability"))
    years = min(_int(row.get("years_seen")), 12)
    return round(min(99.0, 50.0 + z_score * 5.0 + repeatability * 12.0 + years * 0.7), 3)


def _signal_family(feature_type: str) -> str:
    if feature_type in TEAM_FEATURES:
        return "team_origin"
    if feature_type in {"college", "conference"}:
        return "college_origin"
    if feature_type in ALLOCATION_FEATURES:
        return "allocation"
    if feature_type in {
        "ras_bucket",
        "height_bucket",
        "weight_bucket",
        "position_ras",
        "position_height",
        "position_weight",
    }:
        return "measurables"
    if feature_type in {"age_bucket", "experience_bucket", "position_age", "position_experience", "draft_age_grade"}:
        return "age_experience"
    if feature_type in {
        "player_archetype",
        "archetype",
        "nfl_draft_capital",
        "award_history",
        "bio_profile",
        "qb_mobility_profile",
        "rb_receiving_profile",
        "rb_workload_profile",
        "receiving_volume_profile",
        "availability_profile",
    }:
        return "player_profile"
    if feature_type == "position":
        return "position"
    return "other"


def _keep_manager_signal(row: dict[str, Any]) -> bool:
    metric = str(row.get("signal_metric") or "capital_affinity")
    direction = str(row.get("signal_direction") or "overweight")
    feature_type = str(row.get("feature_type") or "")
    if feature_type in TEAM_FEATURES and direction == "underweight":
        return False
    if _int(row.get("years_seen")) < 3:
        return False
    if direction == "overweight" and _int(row.get("picks")) < 3:
        return False
    if metric == "absence_affinity" and _float(row.get("expected_count")) < 5:
        return False
    return True


def _signal_priority(signal: dict[str, Any]) -> float:
    score = _float(signal.get("confidence"))
    z = abs(_float(signal.get("z_score")))
    years = min(_int(signal.get("years_seen")), 12)
    repeatability = _float(signal.get("repeatability"))
    metric = str(signal.get("signal_metric") or "")
    feature_type = str(signal.get("feature_type") or "")
    priority = score + z * 2.0 + years * 0.4 + repeatability * 5.0
    if metric == "absence_affinity":
        priority -= 4.0
    if feature_type in {
        "player_archetype",
        "award_history",
        "bio_profile",
        "qb_mobility_profile",
        "rb_receiving_profile",
        "rb_workload_profile",
        "receiving_volume_profile",
        "availability_profile",
    }:
        priority += 3.0
    if feature_type in TEAM_FEATURES:
        priority -= 3.0
    if feature_type in ENTITY_FEATURES:
        priority -= 1.5
    return priority


def _compact_signal(row: dict[str, Any], assignment: dict[str, Any] | None = None) -> dict[str, Any]:
    metric = str(row.get("signal_metric") or "capital_affinity")
    z_score = _float(row.get("signal_z_score") or row.get("capital_z_score"))
    confidence = _confidence(row, assignment)
    feature_type = str(row.get("feature_type") or "")
    signal = {
        "id": ":".join(str(value) for value in _signal_key(row)),
        "manager": row.get("manager"),
        "manager_key": row.get("manager_key"),
        "feature_type": feature_type,
        "feature_value": row.get("feature_value"),
        "signal_metric": metric,
        "signal_direction": row.get("signal_direction") or ("overweight" if z_score >= 0 else "underweight"),
        "signal_family": _signal_family(feature_type),
        "confidence": confidence,
        "promotion_level": (assignment or {}).get("promotion_level")
        or ("state_ready" if confidence >= 84 else "briefing_watch"),
        "picks": _int(row.get("picks")),
        "expected_count": round(_float(row.get("expected_count")), 3),
        "excess_count": round(_float(row.get("excess_count")), 3),
        "years_seen": _int(row.get("years_seen")),
        "earliest_year": row.get("earliest_year"),
        "latest_year": row.get("latest_year"),
        "repeatability": round(_float(row.get("repeatability")), 4),
        "observed_share": round(_float(row.get("observed_share")), 4),
        "expected_share": round(_float(row.get("expected_share")), 4),
        "lift": round(_float(row.get("lift")), 4),
        "z_score": round(z_score, 3),
        "observed_capital": round(_float(row.get("observed_capital")), 3),
        "expected_capital": round(_float(row.get("expected_capital")), 3),
        "excess_capital": round(_float(row.get("excess_capital")), 3),
        "priority": round(
            _signal_priority(
                {
                    "confidence": confidence,
                    "z_score": z_score,
                    "years_seen": row.get("years_seen"),
                    "repeatability": row.get("repeatability"),
                    "signal_metric": metric,
                    "feature_type": feature_type,
                }
            ),
            3,
        ),
    }
    return _decorate_manager_signal(signal)


def _semantic_bucket(signal: dict[str, Any]) -> tuple[str, str, str]:
    feature_type = str(signal.get("feature_type") or "")
    feature_value = str(signal.get("feature_value") or "")
    direction = str(signal.get("signal_direction") or "")

    value = feature_value.lower()
    if feature_type in {"capital_bucket", "capital_tier", "format_capital_tier"}:
        if any(token in value for token in ("50_plus", "r1_2", "premium")):
            return ("capital_allocation", "premium", direction)
        if any(token in value for token in ("30_49", "16_29", "r3_5", "core")):
            return ("capital_allocation", "core", direction)
        if any(token in value for token in ("6_15", "r6_9", "depth")):
            return ("capital_allocation", "depth", direction)
        if any(token in value for token in ("1_5", "r10_plus", "flyer")):
            return ("capital_allocation", "flyer", direction)
    if feature_type in {"position_capital_bucket", "position_capital_tier"}:
        parts = value.split("_")
        position = parts[0] if parts else ""
        if any(token in value for token in ("50_plus", "r1_2", "premium")):
            return ("position_capital", f"{position}_premium", direction)
        if any(token in value for token in ("30_49", "16_29", "r3_5", "core")):
            return ("position_capital", f"{position}_core", direction)
        if any(token in value for token in ("6_15", "r6_9", "depth")):
            return ("position_capital", f"{position}_depth", direction)
        if any(token in value for token in ("1_5", "r10_plus", "flyer")):
            return ("position_capital", f"{position}_flyer", direction)
    if feature_type in {"position_slot", "position_slot_tier", "position_slot_capital_tier", "position_slot_phase"}:
        slot = value.split("_")[0]
        return ("position_slot", slot, direction)
    return (feature_type, feature_value, direction)


def _select_headlines(signals: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    family_counts: Counter[str] = Counter()
    semantic_seen: set[tuple[str, str, str]] = set()
    for signal in sorted(
        signals, key=lambda row: (_float(row.get("priority")), _float(row.get("confidence"))), reverse=True
    ):
        family = str(signal.get("signal_family") or "other")
        semantic = _semantic_bucket(signal)
        if semantic in semantic_seen and signal.get("signal_metric") != "absence_affinity":
            continue
        if family_counts[family] >= {"team_origin": 1, "college_origin": 1, "allocation": 3}.get(family, 2):
            continue
        selected.append(signal)
        family_counts[family] += 1
        semantic_seen.add(semantic)
        if len(selected) >= limit:
            break
    return selected


def _select_support(signals: list[dict[str, Any]], headline_ids: set[str], *, limit: int = 18) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    semantic_seen = {_semantic_bucket(signal) for signal in signals if signal.get("id") in headline_ids}
    for signal in signals:
        if signal.get("id") in headline_ids:
            continue
        semantic = _semantic_bucket(signal)
        if semantic in semantic_seen:
            continue
        selected.append(signal)
        semantic_seen.add(semantic)
        if len(selected) >= limit:
            break
    return selected


def _build_bucket_registry(assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in assignments:
        grouped[(str(row.get("signal_metric") or "unknown"), str(row.get("feature_type") or "unknown"))].append(row)

    registry = []
    for (metric, feature_type), rows in grouped.items():
        scopes = {(row.get("db_name"), row.get("scope_key")) for row in rows}
        leagues = {row.get("db_name") for row in rows if row.get("db_name")}
        ready = sum(
            1
            for row in rows
            if row.get("profile_status") == "catalog_ready" or row.get("promotion_level") == "state_ready"
        )
        scores = [_float(row.get("validation_score") or row.get("assignment_score")) for row in rows]
        values = Counter(str(row.get("feature_value") or "") for row in rows)
        coverage = min(1.0, math.log1p(len(scopes)) / math.log1p(220))
        diversity = min(1.0, len(leagues) / 60.0)
        ready_share = ready / max(len(rows), 1)
        score_component = min(1.0, _median(scores) / 90.0)
        penalty = 0.84 if feature_type in TEAM_FEATURES else 0.92 if feature_type in ENTITY_FEATURES else 1.0
        priority = 100.0 * (0.30 * coverage + 0.25 * diversity + 0.25 * ready_share + 0.20 * score_component) * penalty
        registry.append(
            {
                "bucket_key": f"{metric}:{feature_type}",
                "signal_metric": metric,
                "feature_type": feature_type,
                "signal_family": _signal_family(feature_type),
                "priority": round(priority, 3),
                "scope_count": len(scopes),
                "league_count": len(leagues),
                "signal_count": len(rows),
                "ready_count": ready,
                "ready_share": round(ready_share, 4),
                "median_score": round(_median(scores), 3),
                "top_values": [
                    {"feature_value": value, "count": count} for value, count in values.most_common(10) if value
                ],
            }
        )
    registry.sort(key=lambda row: (row["priority"], row["scope_count"], row["median_score"]), reverse=True)
    for rank, row in enumerate(registry, start=1):
        row["rank"] = rank
        row["dossier_tier"] = "core" if rank <= 18 else "support" if rank <= 45 else "deep"
    return registry


def _build_manager_dossiers(
    signals: list[dict[str, Any]],
    assignments: list[dict[str, Any]],
    archetype_assignments: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    assignment_by_key = {_assignment_scope_key(row): row for row in assignments}
    archetypes_by_scope: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in archetype_assignments:
        archetypes_by_scope[(_norm(row.get("db_name")), _norm(row.get("scope_label")))].append(row)

    by_manager: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in signals:
        if not _keep_manager_signal(row):
            continue
        manager = str(row.get("manager") or "").strip()
        db_name = str(row.get("db_name") or "").strip()
        if not manager or not db_name:
            continue
        assignment = assignment_by_key.get((_norm(db_name), _norm(manager), _signal_key(row)))
        if assignment and assignment.get("promotion_level") == "explore_only":
            continue
        by_manager[(db_name, manager)].append(_compact_signal(row, assignment))

    dossiers: dict[str, dict[str, Any]] = defaultdict(dict)
    for (db_name, manager), rows in by_manager.items():
        ordered = sorted(
            rows, key=lambda row: (_float(row.get("priority")), _float(row.get("confidence"))), reverse=True
        )
        headline = _select_headlines(ordered)
        headline_ids = {row["id"] for row in headline}
        support = _select_support(ordered, headline_ids)
        metric_counts = Counter(str(row.get("signal_metric")) for row in ordered)
        family_counts = Counter(str(row.get("signal_family")) for row in ordered)
        archetypes = sorted(
            archetypes_by_scope.get((_norm(db_name), _norm(manager)), []),
            key=lambda row: (_float(row.get("archetype_score")), -_int(row.get("archetype_rank"), 999)),
            reverse=True,
        )[:4]
        dossiers[db_name][manager] = {
            "db_name": db_name,
            "manager": manager,
            "manager_key": ordered[0].get("manager_key") if ordered else None,
            "signal_count": len(ordered),
            "metrics": dict(metric_counts),
            "families": dict(family_counts),
            "headline_signals": headline,
            "support_signals": support,
            "archetypes": [
                {
                    "id": row.get("archetype_id"),
                    "label": _archetype_label(row),
                    "summary": _archetype_summary(row),
                    "score": round(_float(row.get("archetype_score")), 3),
                    "rank": _int(row.get("archetype_rank")),
                    "status": row.get("archetype_status"),
                }
                for row in archetypes
            ],
        }
    return {db: dict(managers) for db, managers in sorted(dossiers.items())}


def _league_signal_priority(row: dict[str, Any]) -> float:
    return (
        abs(_float(row.get("shrunk_z_score") or row.get("value_z_score"))) * 8
        + _float(row.get("validation_score")) * 0.7
        + min(_int(row.get("years_seen")), 12) * 0.5
        + _float(row.get("repeatability")) * 5
    )


def _build_league_dossiers(run_dir: Path) -> dict[str, Any]:
    candidate_path = run_dir / "league_state_candidates.json"
    if not candidate_path.exists():
        siblings = sorted(
            run_dir.parent.glob("*/league_state_candidates.json"),
            key=lambda path: path.parent.name,
            reverse=True,
        )
        candidate_path = siblings[0] if siblings else candidate_path
    raw = _read_json(candidate_path, [])
    by_league: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw:
        db_name = str(row.get("db_name") or row.get("scope_key") or "").strip()
        if not db_name:
            continue
        level = str(row.get("promotion_level") or "")
        if level == "explore_only":
            continue
        signal = {
            "feature_type": row.get("feature_type"),
            "feature_value": row.get("feature_value"),
            "signal_metric": row.get("signal_metric") or "value_inefficiency",
            "signal_direction": row.get("signal_direction"),
            "promotion_level": level,
            "validation_score": row.get("validation_score"),
            "picks": row.get("picks"),
            "years_seen": row.get("years_seen"),
            "repeatability": row.get("repeatability"),
            "value_delta": row.get("value_delta"),
            "pick_score_delta": row.get("pick_score_delta"),
            "z_score": row.get("shrunk_z_score") or row.get("value_z_score"),
            "priority": round(_league_signal_priority(row), 3),
        }
        by_league[db_name].append(_decorate_league_signal(signal))

    dossiers = {}
    for db_name, rows in by_league.items():
        ordered = sorted(rows, key=lambda row: _float(row.get("priority")), reverse=True)
        headline = []
        semantic_seen: set[tuple[str, str, str]] = set()
        for row in ordered:
            semantic = _semantic_bucket(row)
            if semantic in semantic_seen:
                continue
            headline.append(row)
            semantic_seen.add(semantic)
            if len(headline) >= 6:
                break
        headline_ids = {
            (row.get("feature_type"), row.get("feature_value"), row.get("signal_direction")) for row in headline
        }
        support = []
        for row in ordered:
            identity = (row.get("feature_type"), row.get("feature_value"), row.get("signal_direction"))
            if identity in headline_ids:
                continue
            semantic = _semantic_bucket(row)
            if semantic in semantic_seen:
                continue
            support.append(row)
            semantic_seen.add(semantic)
            if len(support) >= 12:
                break
        dossiers[db_name] = {
            "db_name": db_name,
            "signal_count": len(ordered),
            "headline_signals": headline,
            "support_signals": support,
        }
    return dict(sorted(dossiers.items()))


def build_index(run_dir: Path) -> dict[str, Any]:
    manager_signals = _read_json(run_dir / "manager_signals.json", [])
    assignments = _read_json(run_dir / "profile_assignments.json", [])
    archetype_assignments = _read_json(run_dir / "archetype_assignments.json", [])
    bucket_registry = _build_bucket_registry(assignments)
    manager_dossiers = _build_manager_dossiers(manager_signals, assignments, archetype_assignments)
    league_dossiers = _build_league_dossiers(run_dir)
    manager_count = sum(len(managers) for managers in manager_dossiers.values())
    return {
        "model_version": MODEL_VERSION,
        "source_run": str(run_dir),
        "generated_at": datetime.now(UTC).isoformat(),
        "summary": {
            "league_count": len(manager_dossiers),
            "manager_count": manager_count,
            "manager_signal_count": len(manager_signals),
            "indexed_league_count": len(league_dossiers),
            "bucket_count": len(bucket_registry),
        },
        "bucket_registry": bucket_registry,
        "manager_dossiers": manager_dossiers,
        "league_dossiers": league_dossiers,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build final draft dossier lazy-load index.")
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    run_dir = args.run_dir
    payload = build_index(run_dir)
    _write_json(run_dir / "dossier_index.json", payload)
    _write_json(run_dir / "bucket_registry.json", payload["bucket_registry"])
    _write_json(run_dir / "manager_dossiers.json", payload["manager_dossiers"])
    _write_json(run_dir / "league_dossiers.json", payload["league_dossiers"])
    print(json.dumps(payload["summary"], indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
