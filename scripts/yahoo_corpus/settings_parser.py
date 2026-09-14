"""Parse Yahoo settings XML into the canonical research cohort shape."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Mapping
from typing import Any

from multi_league.core.yahoo_league_settings import (
    _build_roster_position_counts,
    _build_scoring_rules,
    _build_scoring_settings_dict,
    _parse_league_metadata,
    _parse_roster_positions,
    _parse_stat_categories,
    _parse_stat_modifier_bonuses,
    _parse_stat_modifiers,
)


IDP_SLOTS = {
    "DL",
    "DE",
    "DT",
    "LB",
    "ILB",
    "OLB",
    "EDGE",
    "DB",
    "CB",
    "S",
    "SS",
    "FS",
    "D",
    "DP",
    "IDP",
    "IDP_FLEX",
}
SUPERFLEX_SLOTS = {"Q/W/R/T", "SUPER_FLEX", "SF", "OP"}
FLEX_SLOTS = {"W/R/T", "W/R", "R/W/T", "FLEX"}


def strip_namespaces(xml_text: str) -> ET.Element:
    """Return an ElementTree root with namespace prefixes removed from tags."""
    root = ET.fromstring(xml_text)
    for element in root.iter():
        if "}" in element.tag:
            element.tag = element.tag.split("}", 1)[1]
    return root


def parse_settings_xml(xml_text: str, league_key: str) -> dict[str, Any]:
    """Parse one raw Yahoo settings response with the production helpers."""
    root = strip_namespaces(xml_text)
    metadata = _parse_league_metadata(root)
    if metadata.get("num_teams"):
        metadata["num_teams"] = int(metadata["num_teams"])
    roster_positions = _parse_roster_positions(root)
    stat_map = _parse_stat_categories(root)
    modifiers = _parse_stat_modifiers(root)
    scoring_rules = _build_scoring_rules(stat_map, modifiers)
    scoring_rules.extend(_parse_stat_modifier_bonuses(root, stat_map))
    return {
        "league_key": league_key,
        "metadata": metadata,
        "roster_positions": roster_positions,
        "roster_position_counts": _build_roster_position_counts(roster_positions),
        "scoring_settings": _build_scoring_settings_dict(scoring_rules),
    }


def extract_renewal_keys(xml_text: str) -> list[str]:
    """Return normalized previous/next Yahoo league keys from settings XML."""
    root = strip_namespaces(xml_text)
    league = root.find("league")
    if league is None:
        return []
    keys: list[str] = []
    for field in ("renew", "renewed"):
        raw = (league.findtext(field) or "").strip()
        game_key, separator, league_id = raw.partition("_")
        if separator and game_key and league_id:
            key = f"{game_key}.l.{league_id}"
            if key not in keys:
                keys.append(key)
    return keys


def _slot_count(counts: Mapping[str, Any], names: set[str]) -> int:
    return sum(int(counts.get(name, 0) or 0) for name in names)


def classify_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    """Classify canonical Yahoo settings using the production cohort buckets."""
    metadata = settings.get("metadata") or {}
    counts = settings.get("roster_position_counts") or {}
    scoring = settings.get("scoring_settings") or {}
    num_teams = metadata.get("num_teams")

    missing: list[str] = []
    if not num_teams:
        missing.append("num_teams")
    if not counts:
        missing.append("roster_positions")
    if missing:
        return {
            "classification_status": "incomplete",
            "classification_reason": ",".join(missing),
            "cohort_slug": None,
        }

    if str(metadata.get("draft_status") or "").lower() == "predraft":
        # Renewed/created shell whose season never happened -- importing it
        # yields an empty database that fails pre-upload validation.
        return {
            "classification_status": "unplayed",
            "classification_reason": "draft_status=predraft",
            "cohort_slug": None,
        }

    num_teams = int(num_teams)
    rec = float(scoring.get("rec", 0) or 0)
    pass_td = float(scoring.get("pass_td", 4) or 4)
    has_idp = _slot_count(counts, IDP_SLOTS) > 0
    has_superflex = _slot_count(counts, SUPERFLEX_SLOTS) > 0

    teams = "10t" if num_teams <= 11 else "12t"
    roster = "idp" if has_idp else "sflx" if has_superflex else "flx"
    ppr = "std" if rec == 0 else "half" if rec < 0.75 else "ppr"
    td = "6pt" if pass_td >= 5 else "4pt"
    flex_count = _slot_count(counts, FLEX_SLOTS)
    superflex_count = _slot_count(counts, SUPERFLEX_SLOTS)

    return {
        "classification_status": "classified",
        "classification_reason": None,
        "format": "single_season",
        "teams": teams,
        "roster": roster,
        "ppr": ppr,
        "td": td,
        "cohort_slug": f"{teams}_{roster}_{ppr}_{td}",
        "num_teams": num_teams,
        "roster_shape": (
            f"QB{int(counts.get('QB', 0) or 0)}_"
            f"RB{int(counts.get('RB', 0) or 0)}_"
            f"WR{int(counts.get('WR', 0) or 0)}_"
            f"TE{int(counts.get('TE', 0) or 0)}_"
            f"FLX{flex_count}_SF{superflex_count}"
        ),
    }
