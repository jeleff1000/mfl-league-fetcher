#!/usr/bin/env python3
"""Build the deterministic, precomputed Research player SEO registry."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unicodedata
from typing import Any, Iterable, Mapping, Sequence

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "fantasy_football_data_scripts"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))


def load_repo_environment(path: Path = ROOT / ".env") -> None:
    load_dotenv(path, override=False)


load_repo_environment()

from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


LEGACY_SLUG_PATH = ROOT / "frontend" / "public" / "player-slugs.json"
OUTPUT_PATH = ROOT / "frontend" / "public" / "player-seo-registry.json"
MAX_INDEXABLE_ENTRIES = 50_000
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

# Covers offensive skill players, kickers, team defenses, and common IDP slots.
FANTASY_POSITIONS = (
    "QB",
    "RB",
    "FB",
    "WR",
    "TE",
    "K",
    "PK",
    "DEF",
    "DST",
    "DL",
    "DE",
    "ED",
    "EDGE",
    "DT",
    "NT",
    "LB",
    "ILB",
    "MLB",
    "OLB",
    "DB",
    "CB",
    "S",
    "SAF",
    "FS",
    "SS",
)

FANTASY_POSITION_PATTERN = (
    rf"(^|[^A-Z0-9])({'|'.join(FANTASY_POSITIONS)})([^A-Z0-9]|$)"
)


def is_fantasy_position(position: str) -> bool:
    return bool(re.search(FANTASY_POSITION_PATTERN, position.upper()))


PLAYER_SOURCE_SQL = f"""
WITH player_source AS (
    SELECT
        NFL_player_id,
        player,
        position,
        player_week,
        year
    FROM nfl_historical.nfl_player_stats_all
    WHERE NFL_player_id IS NOT NULL
      AND TRIM(CAST(NFL_player_id AS VARCHAR)) <> ''
      AND player IS NOT NULL
      AND TRIM(player) <> ''
),
name_coverage AS (
    SELECT
        NFL_player_id,
        player,
        COUNT(DISTINCT player_week) AS name_games,
        MAX(year) AS name_latest_year
    FROM player_source
    GROUP BY NFL_player_id, player
),
preferred_name AS (
    SELECT
        NFL_player_id,
        player,
        ROW_NUMBER() OVER (
            PARTITION BY NFL_player_id
            ORDER BY name_games DESC, name_latest_year DESC, player
        ) AS name_rank
    FROM name_coverage
),
identity_coverage AS (
    SELECT
        NFL_player_id,
        COUNT(DISTINCT CASE
            WHEN REGEXP_MATCHES(
                UPPER(TRIM(COALESCE(position, ''))),
                '{FANTASY_POSITION_PATTERN}'
            )
            THEN player_week
        END) AS fantasy_games,
        COUNT(DISTINCT player_week) AS total_games,
        MAX(year) AS latest_year,
        BOOL_OR(REGEXP_MATCHES(
            UPPER(TRIM(COALESCE(position, ''))),
            '{FANTASY_POSITION_PATTERN}'
        ))
            AS fantasy_relevant
    FROM player_source
    GROUP BY NFL_player_id
)
SELECT
    CAST(identity_coverage.NFL_player_id AS VARCHAR) AS nfl_player_id,
    preferred_name.player AS name,
    identity_coverage.fantasy_games,
    identity_coverage.total_games,
    identity_coverage.latest_year,
    identity_coverage.fantasy_relevant
FROM identity_coverage
INNER JOIN preferred_name
    ON identity_coverage.NFL_player_id = preferred_name.NFL_player_id
   AND preferred_name.name_rank = 1
ORDER BY identity_coverage.NFL_player_id
""".strip()


class RegistryValidationError(ValueError):
    """Raised when registry invariants are violated."""


@dataclass(frozen=True)
class PlayerSource:
    nfl_player_id: str
    name: str
    fantasy_games: int
    total_games: int
    latest_year: int
    indexable: bool


def _preference(player: PlayerSource) -> tuple[int, int, int, str]:
    return (
        -player.fantasy_games,
        -player.total_games,
        -player.latest_year,
        player.nfl_player_id,
    )


PREFERENCE = _preference


def normalize_player_name(name: str) -> str:
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(char for char in decomposed if not unicodedata.combining(char))
    words = re.sub(r"[^a-z0-9]+", " ", ascii_name.lower()).strip()
    return re.sub(r"\s+", " ", words)


def slugify_player_name(name: str) -> str:
    return normalize_player_name(name).replace(" ", "-")


def canonical_suffix(nfl_player_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9-]+", "-", nfl_player_id).strip("-").lower()


def _previous_entries(previous: Any) -> list[Mapping[str, object]]:
    if isinstance(previous, Mapping):
        if type(previous.get("version")) is not int or previous["version"] != 1:
            raise RegistryValidationError("previous registry version must be 1")
        if "entries" not in previous:
            raise RegistryValidationError("previous registry is missing entries field")
        entries = previous["entries"]
    else:
        entries = previous
    if not isinstance(entries, list):
        raise RegistryValidationError("previous registry entries must be a list")
    return entries


def validate_previous_registry(previous: Any) -> list[Mapping[str, object]]:
    entries = _previous_entries(previous)
    validate_entries(entries)
    return entries


def build_registry_entries(
    rows: Sequence[PlayerSource],
    previous: Any,
    legacy_slugs: Mapping[str, str] | None = None,
) -> list[dict[str, object]]:
    """Assign stable canonical slugs without changing existing ownership."""

    by_id: dict[str, PlayerSource] = {}
    for row in rows:
        if row.nfl_player_id in by_id:
            raise RegistryValidationError(
                f"duplicate NFL player ID in source rows: {row.nfl_player_id}"
            )
        if not slugify_player_name(row.name):
            raise RegistryValidationError(
                f"player name cannot produce a slug: {row.nfl_player_id}"
            )
        by_id[row.nfl_player_id] = row

    previous_entries = validate_previous_registry(previous)
    previous_by_id = {
        str(item["nflPlayerId"]): str(item["canonicalSlug"])
        for item in previous_entries
    }
    assigned_by_id: dict[str, str] = {}
    slug_owner = {
        str(item["canonicalSlug"]): str(item["nflPlayerId"])
        for item in previous_entries
    }

    # Keep every extant assignment first. In particular this freezes the owner
    # of short slugs even if a future collision has stronger coverage.
    for nfl_player_id in sorted(by_id):
        prior_slug = previous_by_id.get(nfl_player_id)
        if not prior_slug:
            continue
        assigned_by_id[nfl_player_id] = prior_slug

    grouped: dict[str, list[PlayerSource]] = defaultdict(list)
    for row in rows:
        if row.nfl_player_id not in assigned_by_id:
            grouped[slugify_player_name(row.name)].append(row)

    for base_slug in sorted(grouped):
        candidates = sorted(grouped[base_slug], key=PREFERENCE)
        base_available = base_slug not in slug_owner
        for index, row in enumerate(candidates):
            if index == 0 and base_available:
                candidate_slug = base_slug
            else:
                suffix = canonical_suffix(row.nfl_player_id)
                if not suffix:
                    raise RegistryValidationError(
                        f"NFL player ID cannot produce a slug suffix: {row.nfl_player_id}"
                    )
                candidate_slug = f"{base_slug}-{suffix}"
                collision_index = 2
                while candidate_slug in slug_owner:
                    candidate_slug = f"{base_slug}-{suffix}-{collision_index}"
                    collision_index += 1
            assigned_by_id[row.nfl_player_id] = candidate_slug
            slug_owner[candidate_slug] = row.nfl_player_id

    compatibility = legacy_slugs or {}
    entries = [
        {
            "nflPlayerId": row.nfl_player_id,
            "name": row.name,
            "normalizedName": normalize_player_name(row.name),
            "canonicalSlug": assigned_by_id[row.nfl_player_id],
            "legacySlug": compatibility.get(
                row.nfl_player_id,
                f"{slugify_player_name(row.name)}-{canonical_suffix(row.nfl_player_id)}",
            ),
            "indexable": bool(row.indexable and row.total_games > 0),
        }
        for row in rows
    ]
    entries.sort(key=lambda item: (str(item["canonicalSlug"]), str(item["nflPlayerId"])))
    validate_entries(entries)
    return entries


def validate_entries(entries: Iterable[Mapping[str, object]]) -> None:
    seen_ids: set[str] = set()
    seen_canonical: set[str] = set()
    seen_legacy: set[str] = set()
    slug_owners: dict[str, str] = {}
    indexable_count = 0

    required_fields = {
        "nflPlayerId",
        "name",
        "normalizedName",
        "canonicalSlug",
        "legacySlug",
        "indexable",
    }

    for item in entries:
        if not isinstance(item, Mapping):
            raise RegistryValidationError("registry entry must be an object")
        missing_fields = sorted(required_fields - set(item))
        if missing_fields:
            raise RegistryValidationError(
                f"registry entry has missing fields: {', '.join(missing_fields)}"
            )
        if isinstance(item["legacySlug"], str) and not item["legacySlug"].strip():
            raise RegistryValidationError(
                f"missing legacy slug reverse mapping: {item['nflPlayerId']}"
            )
        string_fields = required_fields - {"indexable"}
        malformed_fields = sorted(
            field
            for field in string_fields
            if not isinstance(item[field], str) or not str(item[field]).strip()
        )
        if malformed_fields:
            raise RegistryValidationError(
                f"registry entry has malformed fields: {', '.join(malformed_fields)}"
            )
        if not isinstance(item["indexable"], bool):
            raise RegistryValidationError(
                "registry entry indexable field must be boolean"
            )

        nfl_player_id = str(item["nflPlayerId"])
        name = str(item["name"])
        normalized_name = str(item["normalizedName"])
        canonical_slug = str(item["canonicalSlug"])
        legacy_slug = str(item["legacySlug"])
        if normalized_name != normalize_player_name(name):
            raise RegistryValidationError(
                f"inconsistent normalized name for NFL player ID: {nfl_player_id}"
            )
        if nfl_player_id in seen_ids:
            raise RegistryValidationError(f"duplicate NFL player ID: {nfl_player_id}")
        if not SLUG_PATTERN.fullmatch(canonical_slug):
            raise RegistryValidationError(f"invalid canonical slug: {canonical_slug!r}")
        if canonical_slug in seen_canonical:
            raise RegistryValidationError(
                f"duplicate canonical slug: {canonical_slug}"
            )
        if not legacy_slug:
            raise RegistryValidationError(
                f"missing legacy slug reverse mapping: {nfl_player_id}"
            )
        if legacy_slug in seen_legacy:
            raise RegistryValidationError(f"duplicate legacy slug: {legacy_slug}")
        for slug in (canonical_slug, legacy_slug):
            owner = slug_owners.get(slug)
            if owner is not None and owner != nfl_player_id:
                raise RegistryValidationError(
                    f"slug ownership collision: {slug!r} belongs to both "
                    f"{owner!r} and {nfl_player_id!r}"
                )
            slug_owners[slug] = nfl_player_id
        seen_ids.add(nfl_player_id)
        seen_canonical.add(canonical_slug)
        seen_legacy.add(legacy_slug)
        indexable_count += int(item.get("indexable") is True)

    if indexable_count > MAX_INDEXABLE_ENTRIES:
        raise RegistryValidationError(
            f"indexable registry contains {indexable_count:,} entries; maximum is 50,000"
        )


def load_legacy_slugs(
    path: Path,
    current_names: Mapping[str, str],
) -> dict[str, str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    candidates: dict[str, list[tuple[bool, str]]] = defaultdict(list)
    for slug, item in raw.items():
        if not isinstance(item, Mapping) or not item.get("id"):
            continue
        nfl_player_id = str(item["id"])
        exact_name = str(item.get("name", "")) == current_names.get(nfl_player_id)
        candidates[nfl_player_id].append((exact_name, str(slug)))

    result: dict[str, str] = {}
    for nfl_player_id, options in candidates.items():
        exact = sorted(slug for is_exact, slug in options if is_exact)
        fallback = sorted(slug for _, slug in options)
        result[nfl_player_id] = exact[0] if exact else fallback[0]
    return result


def load_previous_registry(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"version": 1, "entries": []}
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_player_sources(reader: FlyReader | None = None) -> list[PlayerSource]:
    records = (reader or FlyReader()).query(PLAYER_SOURCE_SQL, database="___ops")
    by_id: dict[str, PlayerSource] = {}
    for record in records:
        total_games = int(record.get("total_games") or 0)
        fantasy_relevant = bool(record.get("fantasy_relevant"))
        source = PlayerSource(
            nfl_player_id=str(record["nfl_player_id"]),
            name=str(record["name"]),
            fantasy_games=int(record.get("fantasy_games") or 0),
            total_games=total_games,
            latest_year=int(record.get("latest_year") or 0),
            indexable=fantasy_relevant and total_games > 0,
        )
        current = by_id.get(source.nfl_player_id)
        if current is None or PREFERENCE(source) < PREFERENCE(current):
            by_id[source.nfl_player_id] = source
    return [by_id[nfl_player_id] for nfl_player_id in sorted(by_id)]


def write_registry(path: Path, entries: Sequence[Mapping[str, object]]) -> None:
    validate_entries(entries)
    payload = {"version": 1, "entries": list(entries)}
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def build_registry(output_path: Path = OUTPUT_PATH) -> list[dict[str, object]]:
    sources = fetch_player_sources()
    current_names = {source.nfl_player_id: source.name for source in sources}
    legacy_slugs = load_legacy_slugs(LEGACY_SLUG_PATH, current_names)
    previous = load_previous_registry(output_path)
    entries = build_registry_entries(sources, previous, legacy_slugs)
    write_registry(output_path, entries)
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()
    entries = build_registry(args.output)
    indexable_count = sum(item["indexable"] is True for item in entries)
    print(
        f"Wrote {len(entries):,} identities ({indexable_count:,} indexable) "
        f"to {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
