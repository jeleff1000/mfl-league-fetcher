"""Read/write external_source_config.json + external_alias_mappings field of franchise_config.json."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class SourceMapping:
    """Configuration for mapping an external data source to a table."""

    source_signature: str  # column_set_hash for matching
    filename_glob: str  # human-legible fallback
    table: str
    column_map: dict[str, str]  # slot_name -> source_file_column
    decided_at: str = ""
    decided_by: str = "user"


@dataclass
class IdentityMapping:
    """Configuration for mapping an external alias to a franchise or ignoring it."""

    franchise_id: str | None = None  # None if create-new or ignore
    new_franchise_seed: dict | None = None  # populated when "create new"
    ignore: bool = False
    decided_at: str = ""
    decided_by: str = "user"
    source: str = "manual_wizard"  # "manual_wizard" | "auto_suggested_confirmed"
    # Snapshot fields — populated ONLY for ignore decisions:
    row_count_at_decision: int | None = None
    tables_at_decision: list[str] | None = None
    years_at_decision: list[int] | None = None


def _now() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(UTC).isoformat()


def read_external_source_config(path: Path) -> list[SourceMapping]:
    """Read external_source_config.json; return empty list if missing."""
    if not Path(path).exists():
        return []
    payload = json.loads(Path(path).read_text())
    return [SourceMapping(**s) for s in payload.get("sources", [])]


def write_external_source_config(path: Path, mappings: list[SourceMapping]) -> None:
    """Write external_source_config.json with sources array."""
    payload = {
        "version": "1.0",
        "sources": [asdict(m) for m in mappings],
    }
    for s in payload["sources"]:
        if not s.get("decided_at"):
            s["decided_at"] = _now()
    Path(path).write_text(json.dumps(payload, indent=2))


def read_external_alias_mappings(
    franchise_config_path: Path,
) -> dict[str, IdentityMapping]:
    """Read external_alias_mappings from franchise_config.json; return empty dict if missing."""
    if not Path(franchise_config_path).exists():
        return {}
    payload = json.loads(Path(franchise_config_path).read_text())
    raw = payload.get("external_alias_mappings", {})
    out: dict[str, IdentityMapping] = {}
    for ext_str, m in raw.items():
        out[ext_str] = IdentityMapping(
            franchise_id=m.get("franchise_id"),
            new_franchise_seed=m.get("new_franchise_seed"),
            ignore=bool(m.get("ignore", False)),
            decided_at=m.get("decided_at", ""),
            decided_by=m.get("decided_by", "user"),
            source=m.get("source", "manual_wizard"),
            row_count_at_decision=m.get("row_count_at_decision"),
            tables_at_decision=m.get("tables_at_decision"),
            years_at_decision=m.get("years_at_decision"),
        )
    return out


def write_external_alias_mappings(franchise_config_path: Path, mappings: dict[str, IdentityMapping]) -> None:
    """Update external_alias_mappings IN PLACE on franchise_config.json; bump version to 1.2."""
    path = Path(franchise_config_path)
    payload = json.loads(path.read_text()) if path.exists() else {}
    payload["version"] = "1.2"
    serialized = {}
    # Fields that must be preserved verbatim even when 0/falsy (snapshot data
    # for ignore decisions — row_count_at_decision could legitimately be 0,
    # though unlikely in practice).
    PRESERVE_ZERO = {"row_count_at_decision"}
    for ext_str, m in mappings.items():
        d = {}
        for k, v in asdict(m).items():
            if v is None or v == "":
                continue
            if v is False and k not in PRESERVE_ZERO:
                continue
            d[k] = v
        # Always include these even if defaults; readability:
        if m.ignore:
            d["ignore"] = True
        if not d.get("decided_at"):
            d["decided_at"] = _now()
        serialized[ext_str] = d
    payload["external_alias_mappings"] = serialized
    path.write_text(json.dumps(payload, indent=2))
