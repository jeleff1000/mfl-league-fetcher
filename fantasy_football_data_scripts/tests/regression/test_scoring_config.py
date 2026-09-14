"""
Regression tests for get_scoring_columns() across all scoring combo fixtures.

For each combo schema in the regression DuckDB:
1. Load league_settings
2. Reconstruct a Sleeper-style scoring dict (strip 'scoring_' prefix)
3. Call get_scoring_columns()
4. If baselines exist: compare column selections, corrections, and DEF multipliers exactly
5. If no baselines: smoke test only (must not raise)
"""

import math

import pytest

from multi_league.transformations.player.modules.scoring_calculator import get_scoring_columns


# ── helpers ──────────────────────────────────────────────────────────────────


def _load_settings_row(conn, schema: str) -> dict:
    """Return the first row of league_settings as a dict, stripping None values."""
    rows = conn.execute(f"SELECT * FROM {schema}.league_settings LIMIT 1").fetchdf()
    if rows.empty:
        return {}
    return {k: v for k, v in rows.iloc[0].items() if v is not None}


def _build_scoring_dict(row: dict) -> dict:
    """
    Build a Sleeper-style scoring dict from a flat league_settings row.

    Strips the 'scoring_' prefix so the result looks like:
        {'pass_td': 4.0, 'pass_int': -2.0, 'rec': 0.5, ...}

    get_scoring_columns() detects this as Sleeper format when it sees keys
    like 'rec' or 'pass_td' at the top level.

    NaN values are excluded because the fixture stores NULL columns as NaN
    after pandas fetchdf(). Keeping NaN would cause key-presence checks
    inside get_scoring_columns() to misfire (e.g. 'fgm_yds' in settings).
    """
    scoring = {}
    for col, val in row.items():
        if col.startswith("scoring_"):
            stat_key = col[len("scoring_") :]
            try:
                fval = float(val)
            except (TypeError, ValueError):
                continue
            if math.isnan(fval):
                continue
            scoring[stat_key] = fval
    return scoring


def _format_mismatch(schema: str, key: str, expected, actual) -> str:
    return f"  [{schema}] {key}: expected={expected!r}, actual={actual!r}"


# ── main test ─────────────────────────────────────────────────────────────────


def test_scoring_config_all_combos(regression_db_readonly, available_schemas, baselines):
    """
    Test get_scoring_columns() produces correct results for every combo.

    With baselines: validates column selections, corrections, and DEF multipliers.
    Without baselines (baselines fixture skipped): smoke test — must not raise.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    # baselines may be None if the fixture was skipped (pytest.skip raises, so
    # if we reach here with baselines, it's a real dict or the fixture resolved).
    has_baselines = baselines is not None and isinstance(baselines, dict)

    failures = []

    for schema in available_schemas:
        row = _load_settings_row(regression_db_readonly, schema)
        if not row:
            failures.append(f"  [{schema}] No rows in league_settings — skipping.")
            continue

        scoring_dict = _build_scoring_dict(row)

        # Smoke: get_scoring_columns must not raise
        try:
            result = get_scoring_columns(scoring_dict)
        except Exception as exc:
            failures.append(f"  [{schema}] get_scoring_columns() raised {type(exc).__name__}: {exc}")
            continue

        # Basic structural checks (always run)
        if not isinstance(result, dict):
            failures.append(f"  [{schema}] get_scoring_columns() returned non-dict: {type(result)}")
            continue
        if "corrections" not in result:
            failures.append(f"  [{schema}] missing 'corrections' key in result")
            continue
        if not isinstance(result["corrections"], list):
            failures.append(f"  [{schema}] 'corrections' is not a list: {type(result['corrections'])}")
            continue

        # Baseline comparisons (only when baselines file exists)
        if not has_baselines:
            continue

        combo_baseline = baselines.get(schema)
        if combo_baseline is None:
            # Schema exists in DB but not in baselines — new combo, skip comparison
            continue

        # Baselines store scoring config under the 'scoring_config' key
        baseline = combo_baseline.get("scoring_config")
        if baseline is None:
            # No scoring_config section in baseline — skip comparison for this combo
            continue

        # Compare precalc column selections (all keys except 'corrections' and 'def_multipliers')
        baseline_cols = {k: v for k, v in baseline.items() if k not in ("corrections", "def_multipliers")}
        result_cols = {k: v for k, v in result.items() if k not in ("corrections", "def_multipliers")}

        for key, expected_val in baseline_cols.items():
            actual_val = result_cols.get(key)
            if actual_val != expected_val:
                failures.append(_format_mismatch(schema, key, expected_val, actual_val))

        # Do NOT flag extra keys in result that baseline doesn't track.
        # The baseline only records the keys the capture script chose to save;
        # get_scoring_columns() may return additional keys (e.g. idp_multipliers,
        # def_corrections) that were added after the baseline was captured.

        # Compare corrections list (order-independent, tuple comparison)
        baseline_corrections = set(tuple(c) for c in baseline.get("corrections", []))
        result_corrections = set(tuple(c) for c in result.get("corrections", []))
        if baseline_corrections != result_corrections:
            missing = baseline_corrections - result_corrections
            extra = result_corrections - baseline_corrections
            if missing:
                failures.append(f"  [{schema}] corrections missing: {sorted(missing)}")
            if extra:
                failures.append(f"  [{schema}] corrections extra: {sorted(extra)}")

        # Compare DEF multipliers (if present in baseline)
        if "def_multipliers" in baseline:
            baseline_def = baseline["def_multipliers"]
            result_def = result.get("def_multipliers", {})
            for comp_key, expected_mult in baseline_def.items():
                actual_mult = result_def.get(comp_key)
                if actual_mult != expected_mult:
                    failures.append(_format_mismatch(schema, f"def_multipliers.{comp_key}", expected_mult, actual_mult))

    if failures:
        pytest.fail(f"{len(failures)} combos failed:\n" + "\n".join(failures))
