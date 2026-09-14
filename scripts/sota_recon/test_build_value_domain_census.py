import json
from pathlib import Path

import duckdb

from scripts.sota_recon.build_value_domain_census import build_census, scan_source


def _fixture(tmp_path: Path) -> tuple[Path, dict]:
    parquet = tmp_path / "fixture.parquet"
    duckdb.connect().execute(
        """COPY (SELECT * FROM (VALUES
            (1978, 'passing_yards', '12', '{\"a\": 1}'),
            (1979, 'rushing_yards', '0', '{\"a\": 2, \"b\": [3]}'),
            (NULL, NULL, 'bad', NULL)
        ) AS t(year, stat_name, stat_value, payload)) TO ? (FORMAT PARQUET)""",
        [str(parquet)],
    )
    return parquet, {
        "sources": [
            {
                "source_id": "fixture",
                "path": str(parquet),
                "value_fields": ["stat_name", "stat_value", "payload"],
                "label_fields": ["stat_name"],
                "json_fields": ["payload"],
                "year_field": "year",
            }
        ]
    }


def test_scan_source_profiles_eav_and_json(tmp_path):
    _, source = _fixture(tmp_path)
    rows = scan_source(source["sources"][0])
    labels = {row["raw_label"] for row in rows if row["value_path"] == "$.stat_name"}
    assert labels == {"passing_yards", "rushing_yards"}
    json_paths = {row["value_path"] for row in rows if row["field_name"] == "payload"}
    assert json_paths == {"$.a", "$.b[0]"}


def test_build_census_writes_json_and_csv(tmp_path):
    _, inventory = _fixture(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
    out_json = tmp_path / "out.json"
    out_csv = tmp_path / "out.csv"
    result = build_census(inventory_path, out_json, out_csv)
    assert result["record_count"] >= 4
    assert out_json.exists() and out_csv.exists()
