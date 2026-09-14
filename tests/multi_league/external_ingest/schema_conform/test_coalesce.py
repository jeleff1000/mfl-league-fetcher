import pandas as pd
from multi_league.external_ingest.schema_conform._coalesce import (
    apply_coalesce,
    COALESCE_INPUT_COLS,
)


def test_is_keeper_status_filled():
    df = pd.DataFrame(
        {
            "is_keeper_status": ["keeper", "", ""],
            "is_keeper_cost": [0, 0, 0],
        }
    )
    out = apply_coalesce(df, "is_keeper")
    assert out.tolist() == [1, 0, 0]


def test_is_keeper_cost_only():
    df = pd.DataFrame(
        {
            "is_keeper_status": ["", "", ""],
            "is_keeper_cost": [12.0, 0, 5.5],
        }
    )
    out = apply_coalesce(df, "is_keeper")
    assert out.tolist() == [1, 0, 1]


def test_is_keeper_either_truthy():
    df = pd.DataFrame(
        {
            "is_keeper_status": ["keeper", "", "keeper"],
            "is_keeper_cost": [0, 5.0, 12.0],
        }
    )
    out = apply_coalesce(df, "is_keeper")
    assert out.tolist() == [1, 1, 1]


def test_is_keeper_missing_column_uses_default():
    df = pd.DataFrame({"is_keeper_status": ["keeper", ""]})  # no is_keeper_cost
    out = apply_coalesce(df, "is_keeper")
    assert out.tolist() == [1, 0]


def test_coalesce_input_cols_exposes_consumed():
    assert "is_keeper_status" in COALESCE_INPUT_COLS["is_keeper"]
    assert "is_keeper_cost" in COALESCE_INPUT_COLS["is_keeper"]
