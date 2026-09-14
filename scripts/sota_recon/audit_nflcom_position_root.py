"""Exhaustive audit of NFL.com position page kinds before witness registration."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

LAKE = Path(r"D:\league-history-data\nfl")
OUT = Path(__file__).resolve().parents[2] / "docs/audits/sota-recon/position/nflcom-root-receipt.json"
POSITIONS = LAKE / "derived/validation/sota_recon_master/nflcom_player_page_positions.parquet"
CROSSWALK = LAKE / "derived/entity_universes/nflcom_slug_pfrid.parquet"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_receipt() -> dict:
    con = duckdb.connect()
    p = POSITIONS.as_posix(); x = CROSSWALK.as_posix()
    total, distinct_pairs, conflict_pairs, page_kind_rows = con.execute(f"""
      WITH a AS (SELECT * FROM read_parquet('{p}')),
      b AS (SELECT nflcom_slug,season,COUNT(DISTINCT position_raw) AS raw_n,
                   COUNT(DISTINCT page_kind) AS page_kind_n,COUNT(*) AS rows
            FROM a GROUP BY 1,2)
      SELECT COUNT(*), COUNT(DISTINCT (nflcom_slug,season)),
             COUNT(*) FILTER (WHERE raw_n > 1), SUM(rows) FROM b
    """).fetchone()
    by_kind = con.execute(f"SELECT page_kind,COUNT(*) AS row_count,COUNT(DISTINCT (nflcom_slug,season)) AS pair_count,COUNT(DISTINCT position_raw) AS position_count FROM read_parquet('{p}') GROUP BY 1 ORDER BY 1").fetchdf().to_dict("records")
    crosswalk = con.execute(f"""
      SELECT COUNT(DISTINCT a.nflcom_slug) AS parsed_slugs,
             COUNT(DISTINCT x.nflcom_slug) AS crosswalk_slugs,
             COUNT(DISTINCT a.nflcom_slug) FILTER (WHERE x.pfr_id IS NOT NULL) AS mapped_slugs,
             COUNT(DISTINCT a.nflcom_slug) FILTER (WHERE x.pfr_id IS NULL) AS unmapped_slugs
      FROM read_parquet('{p}') a LEFT JOIN read_parquet('{x}') x USING(nflcom_slug)
    """).fetchone()
    return {
        "schema_version": "nflcom-position-root-receipt.v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source_root": "nflcom",
        "source_artifact": str(POSITIONS),
        "source_artifact_sha256": sha256(POSITIONS),
        "crosswalk": str(CROSSWALK),
        "crosswalk_sha256": sha256(CROSSWALK),
        "within_root": {"rows": int(page_kind_rows), "distinct_slug_seasons": int(distinct_pairs), "conflicting_slug_seasons": int(conflict_pairs), "one_root_vote_required": True},
        "page_kind_coverage": by_kind,
        "expected_coverage": {"parsed_slugs": int(crosswalk[0]), "crosswalk_slugs": int(crosswalk[1]), "mapped_slugs": int(crosswalk[2]), "unmapped_slugs": int(crosswalk[3])},
        "registration_status": "PASS" if conflict_pairs == 0 else "WITHHELD_FROM_CERTIFIED_TABLE",
        "lineage_rule": "logs, splits, and situational are duplicate page kinds inside one NFL.com root and collapse to one slug-season vote",
    }


def main() -> None:
    receipt = build_receipt(); OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "status": receipt["registration_status"], "conflicts": receipt["within_root"]["conflicting_slug_seasons"]}, indent=2))


if __name__ == "__main__":
    main()
