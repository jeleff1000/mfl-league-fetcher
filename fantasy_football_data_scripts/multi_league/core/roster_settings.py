from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from multi_league.core.roster_slots import is_flex
from multi_league.core.roster_slots import resolve as resolve_position

logger = logging.getLogger(__name__)
_verbose = "--verbose" in sys.argv


def load_roster_settings_from_json(settings_dir: Path, league_id: str = "") -> dict[int, dict[str, int]]:
    """
    Load roster settings from year-specific settings JSON files.

    Historical seasons may use different league IDs across years, so this loader
    reads every settings file in the target directory and keys the result by year.
    """
    roster_by_year: dict[int, dict[str, int]] = {}
    settings_dir = Path(settings_dir)

    if not settings_dir.exists():
        logger.warning("[roster] Settings directory not found: %s", settings_dir)
        return roster_by_year

    files = (
        list(settings_dir.glob("yahoo_roster_*.json"))
        + list(settings_dir.glob("league_settings_*.json"))
        + list(settings_dir.glob("settings_*.json"))
    )

    for file_path in sorted(files):
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("[roster] Could not load %s: %s", file_path, exc)
            continue

        year = data.get("year") or data.get("season")
        if not year:
            continue

        position_counts: dict[str, int] = {}

        if "roster_position_counts" in data and isinstance(data["roster_position_counts"], dict):
            for position, count in data["roster_position_counts"].items():
                if position not in ("BN", "IR") and count > 0:
                    position_counts[position] = int(count)
        elif "roster_positions" in data:
            roster_positions = data["roster_positions"]
            if roster_positions and isinstance(roster_positions[0], dict):
                for pos_info in roster_positions:
                    position = pos_info.get("position", "")
                    count = pos_info.get("count", 0)
                    if position not in ("BN", "IR") and position and count > 0:
                        position_counts[position] = int(count)
            elif roster_positions and isinstance(roster_positions[0], str):
                from collections import Counter

                pos_counter = Counter(roster_positions)
                for position, count in pos_counter.items():
                    if position not in ("BN", "IR") and count > 0:
                        position_counts[position] = int(count)

        if not position_counts:
            continue

        normalized_counts: dict[str, int] = {}
        for position, count in position_counts.items():
            resolved = resolve_position(position)
            normalized_counts[resolved] = normalized_counts.get(resolved, 0) + int(count)

        roster_by_year[int(year)] = normalized_counts

        if _verbose:
            if any(is_flex(pos) for pos in normalized_counts):
                flex_slots = [f"{pos}:{normalized_counts[pos]}" for pos in normalized_counts if is_flex(pos)]
                logger.info(
                    "[roster] Loaded %s%s: %s (Flex: %s)",
                    year,
                    f" [{league_id}]" if league_id else "",
                    normalized_counts,
                    ", ".join(flex_slots),
                )
            else:
                logger.info("[roster] Loaded %s%s: %s", year, f" [{league_id}]" if league_id else "", normalized_counts)

    if roster_by_year:
        years = sorted(roster_by_year)
        logger.info("[roster] Loaded settings for %s years (%s-%s)", len(years), years[0], years[-1])

    return roster_by_year
