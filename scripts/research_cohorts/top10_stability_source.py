"""Source and approved-pool boundaries for annual top-10 stability studies."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Iterable, Mapping

from cohort_format_sql import cohort_league_settings_sql


@dataclass(frozen=True)
class ApprovedPool:
    metric: str
    position: str
    members: tuple[str, ...]
    pooled_n_2025: int | None
    display_ok: bool
    sort_ok: bool


@dataclass(frozen=True)
class LeagueYear:
    db_name: str
    year: int
    cohort_member: str


@dataclass(frozen=True)
class StudySourceManifest:
    file_sha256: dict[str, str]
    git_commit: str


_COHORT_MEMBER = re.compile(
    r"^(10t|12t)/(flx|sflx|idp)/(std|half|ppr)/(4pt|6pt)$"
)
_COHORT_SLUG = re.compile(
    r"^(10t|12t)_(flx|sflx|idp)_(std|half|ppr)_(4pt|6pt)$"
)


def cohort_member_from_slug(slug: str) -> str:
    """Convert a serving slug to the cluster-map member representation."""
    value = str(slug)
    if not _COHORT_SLUG.fullmatch(value):
        raise ValueError(f"invalid cohort slug: {slug!r}")
    return value.replace("_", "/")


def cohort_slug_from_member(member: str) -> str:
    """Convert a validated cluster-map member to a serving slug."""
    value = str(member)
    if not _COHORT_MEMBER.fullmatch(value):
        raise ValueError(f"invalid cohort member: {member!r}")
    return value.replace("/", "_")


def _parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def load_approved_pools(
    path: Path | str,
    *,
    valid_metrics: Iterable[str],
) -> tuple[ApprovedPool, ...]:
    """Load the previously approved metric/position cohort components."""

    valid = set(valid_metrics)
    pools: list[ApprovedPool] = []
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"metric", "pos", "members", "pooled_n_2025", "display_ok", "sort_ok"}
        missing = sorted(required - set(reader.fieldnames or ()))
        if missing:
            raise ValueError(f"cluster map missing columns: {', '.join(missing)}")
        for line_number, row in enumerate(reader, start=2):
            metric = str(row["metric"]).strip()
            if metric not in valid:
                raise ValueError(f"unknown metric {metric!r} at line {line_number}")
            members = tuple(
                member.strip() for member in str(row["members"]).split("|") if member.strip()
            )
            if (
                not members
                or len(members) != len(set(members))
                or any(not _COHORT_MEMBER.fullmatch(member) for member in members)
            ):
                raise ValueError(f"invalid pool members at line {line_number}")
            pooled_raw = str(row["pooled_n_2025"]).strip()
            pools.append(
                ApprovedPool(
                    metric=metric,
                    position=str(row["pos"]).strip().upper(),
                    members=members,
                    pooled_n_2025=int(float(pooled_raw)) if pooled_raw else None,
                    display_ok=_parse_bool(row["display_ok"]),
                    sort_ok=_parse_bool(row["sort_ok"]),
                )
            )
    return tuple(pools)


def pool_for_base_cohort(
    pools: Iterable[ApprovedPool],
    metric: str,
    position: str,
    base_cohort: str,
) -> ApprovedPool:
    """Return the unique approved component containing a requested base cohort."""

    matches = [
        pool
        for pool in pools
        if pool.metric == metric
        and pool.position == position.upper()
        and base_cohort in pool.members
    ]
    if not matches:
        raise KeyError(f"no approved pool for {metric}/{position}/{base_cohort}")
    if len(matches) != 1:
        raise ValueError(f"overlapping approved pools for {metric}/{position}/{base_cohort}")
    return matches[0]


def eligible_leagues(
    reader: object,
    *,
    year: int,
    members: Iterable[str],
) -> tuple[LeagueYear, ...]:
    """Enumerate unique eligible leagues from one year and approved member slugs."""

    member_list = tuple(dict.fromkeys(str(member) for member in members))
    if not member_list:
        raise ValueError("members cannot be empty")
    placeholders = ",".join("?" for _ in member_list)
    classified = cohort_league_settings_sql(year=int(year))
    rows = reader.con.execute(
        f"""
        WITH classified AS ({classified}), labeled AS (
          SELECT DISTINCT db_name, year,
                 teams || '/' || roster || '/' || ppr || '/' || td AS cohort_member
          FROM classified
        )
        SELECT db_name, year, cohort_member
        FROM labeled
        WHERE year = ? AND cohort_member IN ({placeholders})
        ORDER BY db_name
        """,
        [int(year), *member_list],
    ).fetchall()
    result = tuple(LeagueYear(str(db_name), int(row_year), str(member)) for db_name, row_year, member in rows)
    identities = [(row.db_name, row.year) for row in result]
    if len(identities) != len(set(identities)):
        raise ValueError("eligible league population contains duplicate league-years")
    if any(row.year != int(year) for row in result):
        raise ValueError("eligible league population crossed year boundary")
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_source_manifest(
    files: Mapping[str, Path | str],
    *,
    git_commit: str,
) -> StudySourceManifest:
    """Hash source contents so evidence cannot outlive the data it measured."""

    hashes: dict[str, str] = {}
    for name, raw_path in sorted(files.items()):
        path = Path(raw_path)
        if not path.is_file():
            raise FileNotFoundError(f"manifest source not found: {path}")
        hashes[str(name)] = _sha256_file(path)
    if not git_commit.strip():
        raise ValueError("git_commit cannot be empty")
    return StudySourceManifest(file_sha256=hashes, git_commit=git_commit.strip())
