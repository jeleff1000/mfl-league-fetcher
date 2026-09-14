"""Shared contract for playoff evidence produced by platform adapters."""

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class EvidenceRow:
    db_name: str
    year: int
    week: int
    NFL_player_id: str
    made_po_bf: int
    is_playoffs_bf: int
    source_platform: str
    evidence_kind: str
    source_id: str
    generated_at: str


def dedupe_key(row: EvidenceRow) -> tuple[str, int, int, str]:
    return (row.db_name, int(row.year), int(row.week), row.NFL_player_id)


def validate_rows(rows: Iterable[EvidenceRow]) -> list[str]:
    violations: list[str] = []
    seen: set[tuple[str, int, int, str]] = set()
    for row in rows:
        key = dedupe_key(row)
        if key in seen:
            violations.append(f"duplicate evidence key: {key}")
        seen.add(key)
        if not row.db_name or not row.NFL_player_id:
            violations.append(f"missing identity for evidence key: {key}")
        if int(row.made_po_bf) not in (0, 1) or int(row.is_playoffs_bf) not in (0, 1):
            violations.append(f"non-boolean playoff flags for evidence key: {key}")
        if int(row.is_playoffs_bf) and not int(row.made_po_bf):
            violations.append(f"is_playoffs_bf requires made_po_bf: {key}")
        if row.evidence_kind.strip().lower() == "consolation" and int(row.is_playoffs_bf):
            violations.append(f"consolation evidence cannot set is_playoffs_bf: {key}")
    return violations
