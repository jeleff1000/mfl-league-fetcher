"""Private, resumable storage for the Yahoo settings census."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


PRIVATE_LEAGUE_FIELDS = {"league_name", "grant_id", "account_hash", "source_database"}
CREDENTIAL_STATES = {"success", "duplicate_grant", "failure"}


@dataclass(frozen=True)
class XMLWriteResult:
    path: Path
    sha256: str


class CensusStore:
    """Checkpoint league/settings progress and emit secret-free aggregate output."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.raw_root = self.root / "raw"
        self.state_path = self.root / "state.json"
        self.root.mkdir(parents=True, exist_ok=True)
        self.league_rows: dict[str, dict[str, Any]] = {}
        self.credential_rows: dict[str, dict[str, Any]] = {}
        self._load_state()

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        data = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.league_rows = {row["league_key"]: row for row in data.get("league_rows", [])}
        self.credential_rows = {row["grant_id"]: row for row in data.get("credential_rows", [])}

    @staticmethod
    def _atomic_json(path: Path, value: Any) -> None:
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8")
        temp.replace(path)

    @staticmethod
    def _atomic_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
        temp = path.with_suffix(path.suffix + ".tmp")
        pq.write_table(pa.Table.from_pylist(rows), temp)
        temp.replace(path)

    def write_xml(self, league_key: str, xml_text: str) -> XMLWriteResult:
        game_key, separator, league_id = league_key.partition(".l.")
        if not separator or not game_key or not league_id:
            raise ValueError(f"Invalid Yahoo league_key: {league_key}")
        destination = self.raw_root / game_key / f"{league_id}.xml"
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = xml_text.encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        if not destination.exists() or destination.read_bytes() != payload:
            temp = destination.with_suffix(".xml.tmp")
            temp.write_bytes(payload)
            temp.replace(destination)
        return XMLWriteResult(path=destination, sha256=digest)

    def record_league(self, row: dict[str, Any]) -> None:
        league_key = str(row.get("league_key") or "")
        if not league_key:
            raise ValueError("league row requires league_key")
        self.league_rows[league_key] = dict(row)

    def record_credential(self, row: dict[str, Any]) -> None:
        status = row.get("status")
        if status not in CREDENTIAL_STATES:
            raise ValueError(f"Invalid credential status: {status}")
        grant_id = str(row.get("grant_id") or "")
        if not grant_id:
            raise ValueError("credential row requires grant_id")
        self.credential_rows[grant_id] = dict(row)

    def is_complete(self, league_key: str) -> bool:
        return self.league_rows.get(league_key, {}).get("fetch_status") == "success"

    def summary(self) -> dict[str, Any]:
        credentials = list(self.credential_rows.values())
        leagues = list(self.league_rows.values())
        seasons = [int(row["season"]) for row in leagues if row.get("season")]
        cohorts = Counter(
            str(row["cohort_slug"])
            for row in leagues
            if row.get("classification_status") == "classified" and row.get("cohort_slug")
        )
        return {
            "credential_rows": sum(int(row.get("credential_rows", 1) or 1) for row in credentials),
            "unique_grants": len(credentials),
            "successful_grants": sum(row.get("status") == "success" for row in credentials),
            "failed_grants": sum(row.get("status") == "failure" for row in credentials),
            "unique_league_years": len(leagues),
            "season_min": min(seasons) if seasons else None,
            "season_max": max(seasons) if seasons else None,
            "cohort_counts": dict(sorted(cohorts.items())),
            "fetch_success": sum(row.get("fetch_status") == "success" for row in leagues),
            "fetch_failure": sum(row.get("fetch_status") == "failure" for row in leagues),
            "classification_incomplete": sum(
                row.get("classification_status") != "classified" for row in leagues
            ),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def flush(self) -> None:
        league_rows = sorted(
            self.league_rows.values(),
            key=lambda row: (int(row.get("season") or 0), str(row.get("league_key") or "")),
        )
        credential_rows = sorted(self.credential_rows.values(), key=lambda row: row["grant_id"])
        self._atomic_json(
            self.state_path,
            {"league_rows": league_rows, "credential_rows": credential_rows},
        )
        if league_rows:
            self._atomic_parquet(self.root / "league_year_manifest_private.parquet", league_rows)
            public_rows = [
                {key: value for key, value in row.items() if key not in PRIVATE_LEAGUE_FIELDS}
                for row in league_rows
            ]
            self._atomic_parquet(self.root / "league_year_manifest.parquet", public_rows)
        if credential_rows:
            self._atomic_parquet(self.root / "credential_status_private.parquet", credential_rows)
        self._atomic_json(self.root / "summary.json", self.summary())
