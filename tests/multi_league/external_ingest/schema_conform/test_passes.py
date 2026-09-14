import pandas as pd
from types import SimpleNamespace
from multi_league.external_ingest.schema_conform._passes import (
    cooccurrence_filter,
    hungarian_assign,
    run_passes,
)
from multi_league.external_ingest.schema_conform._config import CONFIDENCE_FLOOR, COLLISION_GAP


def test_cooccurrence_filter_keeps_matched_column():
    """When manager_guid co-varies with manager, filter keeps it."""
    df = pd.DataFrame(
        {
            "manager": ["A", "A", "B", "B", "C", "C"] * 10,
            "manager_guid": ["g_a", "g_a", "g_b", "g_b", "g_c", "g_c"] * 10,
            "opponent_guid": ["g_x", "g_y", "g_x", "g_z", "g_y", "g_z"] * 10,
        }
    )
    candidates = ["manager_guid", "opponent_guid"]
    kept = cooccurrence_filter(candidates, anchor_col="manager", df=df, mi_threshold=0.5)
    assert "manager_guid" in kept


def test_cooccurrence_filter_skipped_below_min_rows():
    """With < 50 rows, filter passes everything through."""
    df = pd.DataFrame({"manager": ["a"] * 10, "x": [1] * 10})
    kept = cooccurrence_filter(["x"], anchor_col="manager", df=df, mi_threshold=0.5, min_rows=50)
    assert kept == ["x"]


def test_hungarian_assigns_one_to_one():
    """Score matrix: 2 source cols, 2 slots. Hungarian gives 1:1."""
    score_matrix = pd.DataFrame(
        [[0.95, 0.10], [0.20, 0.85]],
        index=["src_a", "src_b"],
        columns=["slot_x", "slot_y"],
    )
    bindings = hungarian_assign(score_matrix)
    assert bindings == {"src_a": "slot_x", "src_b": "slot_y"}


def test_hungarian_with_uneven_matrix():
    """3 sources, 2 slots — only 2 winners."""
    score_matrix = pd.DataFrame(
        [[0.9, 0.1], [0.5, 0.8], [0.4, 0.3]],
        index=["src_a", "src_b", "src_c"],
        columns=["slot_x", "slot_y"],
    )
    bindings = hungarian_assign(score_matrix)
    assert len(bindings) == 2


def test_run_passes_structural_first():
    """Year/week bind in pass 1 by numeric range alone (high name + value match)."""
    src_df = pd.DataFrame(
        {
            "year": [2013, 2014, 2015, 2014],
            "week": [1, 2, 3, 4],
            "manager": ["A", "B", "C", "A"],
        }
    )
    ref_columns = {
        "year": pd.Series([2014, 2015, 2016, 2017, 2018]),
        "week": pd.Series([1, 2, 3, 4, 5, 6]),
        "manager": pd.Series(["X", "Y", "Z"]),
    }
    pass_slots = {1: ["year", "week"], 2: ["manager"], 3: [], 4: []}
    bindings, ledger = run_passes(src_df, ref_columns, pass_slots, ctx=SimpleNamespace(df=src_df))
    assert bindings.get("year") == "year"
    assert bindings.get("week") == "week"
    assert bindings.get("manager") == "manager"
    # Ledger has pass_number recorded
    pass_for_year = next(r for r in ledger if r["source_col"] == "year")
    assert pass_for_year["pass_number"] == 1


def test_cross_source_collision_detected():
    """Gap F: two source cols both scoring >= CONFIDENCE_FLOOR for the same slot triggers
    a _cross_source_collision ledger row.

    'mgr_guid' and 'owner_guid' both token-match to 'manager_guid' (score 1.0) AND
    contain identical value distributions from the canonical reference set,
    so both achieve top scores for the manager_guid slot triggering cross-source collision.
    """
    import string
    import random

    rng = random.Random(42)

    def rand_base32(n=26):
        chars = string.ascii_uppercase + "2345678"  # rough Yahoo GUID charset
        return "".join(rng.choice(chars) for _ in range(n))

    # 4 canonical manager_guids; 100 rows cycling through them
    ref_guids = [rand_base32() for _ in range(4)]
    n_rows = 100

    manager_col = ["Adin", "Marc", "Tani", "Dave"] * (n_rows // 4)
    guid_values = [ref_guids[i % 4] for i in range(n_rows)]

    # Both 'mgr_guid' and 'owner_guid' token-match 'manager_guid' (MI=1 with manager)
    # and carry identical canonical guid distributions → both score ~1.0 for the slot.
    src_df = pd.DataFrame(
        {
            "manager": manager_col,
            "mgr_guid": guid_values,
            "owner_guid": guid_values,  # identical distribution
        }
    )

    ref_columns = {
        "manager_guid": pd.Series(ref_guids * 25),  # thick reference
    }

    pass_slots = {3: ["manager_guid"]}

    _, ledger = run_passes(
        src_df,
        ref_columns,
        pass_slots,
        ctx=SimpleNamespace(df=src_df),
        cooccurrence_anchors={"manager_guid": "manager"},
    )

    # At least one _cross_source_collision row must be present for manager_guid
    collision_rows = [r for r in ledger if r.get("_cross_source_collision") and r["ddl_slot"] == "manager_guid"]
    assert len(collision_rows) >= 1, (
        "Expected at least one _cross_source_collision ledger row for manager_guid "
        "when mgr_guid and owner_guid both score >= CONFIDENCE_FLOOR for the same slot"
    )
    cr = collision_rows[0]
    assert cr["score"] >= CONFIDENCE_FLOOR
    assert cr["second_best_score"] >= CONFIDENCE_FLOOR
    assert (cr["score"] - cr["second_best_score"]) < COLLISION_GAP
