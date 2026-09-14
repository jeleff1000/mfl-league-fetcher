"""Regression tests for schema-conform binding passes."""

from __future__ import annotations

import pandas as pd

from multi_league.external_ingest.schema_conform import _passes


def test_run_passes_skips_non_candidate_hungarian_assignment(monkeypatch):
    """Hungarian output can include zero-score cells outside a slot's candidates."""

    def fake_hungarian_assign(_score_matrix):
        return {
            "timestamp": "manager_guid",
            "manager_guid_source": "transaction_id",
        }

    def fake_cooccurrence_filter(candidates, anchor_col, df, mi_threshold=0.0, min_rows=0):
        if anchor_col == "manager":
            return ["manager_guid_source"]
        return list(candidates)

    monkeypatch.setattr(_passes, "cooccurrence_filter", fake_cooccurrence_filter)
    monkeypatch.setattr(_passes, "hungarian_assign", fake_hungarian_assign)

    src_df = pd.DataFrame(
        {
            "timestamp": ["2026-01-01", "2026-01-02"],
            "manager_guid_source": ["guid-a", "guid-b"],
        }
    )
    ref_columns = {
        "manager_guid": pd.Series(["guid-a", "guid-b"]),
        "transaction_id": pd.Series(["tx-a", "tx-b"]),
    }

    bindings, ledger = _passes.run_passes(
        src_df=src_df,
        ref_columns=ref_columns,
        pass_slots={3: ["manager_guid", "transaction_id"]},
        ctx=object(),
        cooccurrence_anchors={"manager_guid": "manager"},
    )

    assert bindings == {"manager_guid_source": "transaction_id"}
    assert all(row["source_col"] != "timestamp" for row in ledger)
