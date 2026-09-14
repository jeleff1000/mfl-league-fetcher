from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.sota_recon.witness_gate.position_validation import validate_position_frame


def test_detailed_position_in_broad_column_fails_candidate_gate() -> None:
    frame = pd.DataFrame(
        [
            {
                "source": "fixture",
                "dataset": "player_bio",
                "source_player_id": "p1",
                "position": "CB",
                "nfl_position": "CB",
            }
        ]
    )

    findings = validate_position_frame(
        frame, position_col="position", nfl_position_col="nfl_position"
    )

    assert [item.code for item in findings] == ["DETAILED_POSITION_IN_BROAD_COLUMN"]
    assert findings[0].scope["gate_plane"] == "candidate_promotion"
    assert findings[0].scope["lineage"]["source_player_id"] == "p1"


def test_unknown_position_fails_source_health_loudly() -> None:
    frame = pd.DataFrame(
        [{"source": "fixture", "row_id": "bad-1", "position": "XYZ", "nfl_position": "XYZ"}]
    )

    findings = validate_position_frame(
        frame, position_col="position", nfl_position_col="nfl_position"
    )

    assert {item.code for item in findings} == {"UNKNOWN_POSITION_TOKEN"}
    assert all(item.severity == "fail" for item in findings)
    assert findings[0].scope["gate_plane"] == "source_health"
    assert findings[0].scope["lineage"] == {"source": "fixture", "row_id": "bad-1"}


@pytest.mark.parametrize(
    ("position", "nfl_position", "code"),
    [("QB", "CB", "POSITION_PAIR_INCOMPATIBLE"), ("LS", "LS", "LONG_SNAPPER_IN_BROAD_POSITION")],
)
def test_invalid_position_pair_returns_structured_finding(
    position: str, nfl_position: str, code: str
) -> None:
    findings = validate_position_frame(
        pd.DataFrame([{"position": position, "nfl_position": nfl_position}]),
        position_col="position",
        nfl_position_col="nfl_position",
    )

    assert [item.code for item in findings] == [code]
    assert findings[0].scope["position"] == position
    assert findings[0].scope["nfl_position"] == nfl_position


def test_findings_are_stable_under_row_reordering() -> None:
    frame = pd.DataFrame(
        [
            {"source": "b", "row_id": "2", "position": "QB", "nfl_position": "CB"},
            {"source": "a", "row_id": "1", "position": "CB", "nfl_position": "CB"},
        ]
    )

    forward = validate_position_frame(
        frame, position_col="position", nfl_position_col="nfl_position"
    )
    reverse = validate_position_frame(
        frame.iloc[::-1], position_col="position", nfl_position_col="nfl_position"
    )

    assert [item.to_dict() for item in forward] == [item.to_dict() for item in reverse]


def test_missing_contract_column_fails_loudly() -> None:
    with pytest.raises(ValueError, match="missing position contract columns.*nfl_position"):
        validate_position_frame(
            pd.DataFrame([{"position": "QB"}]),
            position_col="position",
            nfl_position_col="nfl_position",
        )


def test_duplicate_broad_position_column_is_rejected_before_record_conversion() -> None:
    frame = pd.DataFrame(
        [["CB", "DB", "CB"]], columns=["position", "position", "nfl_position"]
    )

    with pytest.raises(ValueError, match="duplicate DataFrame columns.*position"):
        validate_position_frame(frame, position_col="position", nfl_position_col="nfl_position")


def test_duplicate_lineage_column_is_rejected() -> None:
    frame = pd.DataFrame(
        [["p1", "p2", "QB", "QB"]],
        columns=["source_player_id", "source_player_id", "position", "nfl_position"],
    )

    with pytest.raises(ValueError, match="duplicate DataFrame columns.*source_player_id"):
        validate_position_frame(frame, position_col="position", nfl_position_col="nfl_position")


def test_parquet_lineage_leaves_are_preserved_and_order_independent(tmp_path) -> None:
    leaves = [
        {"shard_id": 2, "manifest_sha256": "b" * 64},
        {"shard_id": 1, "manifest_sha256": "a" * 64},
    ]

    def findings_for(raw_leaves, name):
        path = tmp_path / name
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "source": "profootballarchives",
                        "dataset": "player_game_participation",
                        "position": "CB",
                        "nfl_position": "CB",
                        "lineage_leaves": raw_leaves,
                    }
                ]
            ),
            path,
        )
        return validate_position_frame(
            pq.read_table(path).to_pandas(),
            position_col="position",
            nfl_position_col="nfl_position",
        )

    forward = findings_for(leaves, "forward.parquet")
    reverse = findings_for(list(reversed(leaves)), "reverse.parquet")

    assert [item.to_dict() for item in forward] == [item.to_dict() for item in reverse]
    assert forward[0].scope["lineage"]["lineage_leaves"] == list(reversed(leaves))
    assert set(forward[0].evidence_refs[1:]) == {"a" * 64, "b" * 64}


@pytest.mark.parametrize(
    "lineage_leaves",
    ["not-a-list", [{"shard_id": 1}], [{"manifest_sha256": "a" * 64}]],
)
def test_malformed_lineage_leaves_fail_loudly(lineage_leaves) -> None:
    frame = pd.DataFrame(
        [{"position": "CB", "nfl_position": "CB", "lineage_leaves": lineage_leaves}]
    )

    with pytest.raises(ValueError, match="lineage_leaves"):
        validate_position_frame(frame, position_col="position", nfl_position_col="nfl_position")


@pytest.mark.parametrize(
    "shard_id",
    [True, False, 1.0, 1.5, "1", "01", "1.0", float("nan"), float("inf"), -1],
)
def test_lineage_shard_id_rejects_coercible_non_integral_values(shard_id) -> None:
    leaves = np.array(
        [{"shard_id": shard_id, "manifest_sha256": "a" * 64}], dtype=object
    )
    frame = pd.DataFrame(
        [{"position": "CB", "nfl_position": "CB", "lineage_leaves": leaves}]
    )

    with pytest.raises(ValueError, match="shard_id must be a nonnegative integer scalar"):
        validate_position_frame(frame, position_col="position", nfl_position_col="nfl_position")


def test_lineage_shard_id_accepts_numpy_int64() -> None:
    leaves = np.array(
        [{"shard_id": np.int64(7), "manifest_sha256": "a" * 64}], dtype=object
    )
    frame = pd.DataFrame(
        [{"position": "CB", "nfl_position": "CB", "lineage_leaves": leaves}]
    )

    findings = validate_position_frame(
        frame, position_col="position", nfl_position_col="nfl_position"
    )

    assert findings[0].scope["lineage"]["lineage_leaves"][0]["shard_id"] == 7
