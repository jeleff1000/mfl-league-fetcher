"""Ordered binding passes with co-occurrence filter + Hungarian assignment.

Pass 1: STRUCTURAL    — year, week, round, pick
Pass 2: UNIQUE-STRING — manager, opponent, player, team_name, transaction_type
Pass 3: ANCHORED-IDENTITY — manager_guid, opponent_guid, team_key, yahoo_player_id, transaction_id
Pass 4: REMAINING — cost, faab_bid, points, fantasy_position, etc.
"""

from __future__ import annotations

import pandas as pd
from scipy.optimize import linear_sum_assignment
from sklearn.metrics import normalized_mutual_info_score

from ._config import COOCCURRENCE_MI_FLOOR, COOCCURRENCE_MIN_ROWS, CONFIDENCE_FLOOR, COLLISION_GAP


def cooccurrence_filter(
    candidates: list[str],
    anchor_col: str,
    df: pd.DataFrame,
    mi_threshold: float = COOCCURRENCE_MI_FLOOR,
    min_rows: int = COOCCURRENCE_MIN_ROWS,
) -> list[str]:
    """Keep candidates whose normalized mutual information with anchor_col is ≥ threshold.
    Skipped (passes everything) when df has fewer than min_rows rows.
    """
    if len(df) < min_rows or anchor_col not in df.columns:
        return list(candidates)

    anchor = df[anchor_col].astype(str).fillna("")
    kept = []
    for c in candidates:
        if c not in df.columns:
            continue
        col = df[c].astype(str).fillna("")
        try:
            mi = normalized_mutual_info_score(anchor, col)
        except Exception:
            mi = 0.0
        if mi >= mi_threshold:
            kept.append(c)
    return kept


def hungarian_assign(score_matrix: pd.DataFrame) -> dict[str, str]:
    """One-to-one assignment maximizing total score.

    Args:
        score_matrix: DataFrame indexed by source col, columns are slots.

    Returns:
        {source_col: slot} for the optimal assignment.
    """
    if score_matrix.empty:
        return {}
    cost = -score_matrix.values  # linear_sum_assignment minimizes
    row_ind, col_ind = linear_sum_assignment(cost)
    bindings = {}
    for r, c in zip(row_ind, col_ind):
        bindings[score_matrix.index[r]] = score_matrix.columns[c]
    return bindings


def run_passes(
    src_df: pd.DataFrame,
    ref_columns: dict[str, pd.Series],
    pass_slots: dict[int, list[str]],
    ctx,
    cooccurrence_anchors: dict[str, str] | None = None,
) -> tuple[dict[str, str], list[dict]]:
    """Run the 4 ordered binding passes.

    Args:
        src_df: source rows (post-normalization).
        ref_columns: {slot: ref_series} for every DDL slot considered.
        pass_slots: {1: [...], 2: [...], 3: [...], 4: [...]}.
        ctx: context object with .df (source df, for invariants/cooc).
        cooccurrence_anchors: {ambiguous_slot: anchor_col} for pass 3.
                              e.g., {"manager_guid": "manager", "opponent_guid": "opponent"}.

    Returns:
        (bindings, ledger_rows)
        bindings: {source_col: slot}
        ledger_rows: list of dicts with {source_col, ddl_slot, score, second_best_score,
                                          second_best_slot, pass_number, score_components}
    """
    cooccurrence_anchors = cooccurrence_anchors or {}
    bound_sources: set[str] = set()
    bound_slots: set[str] = set()
    bindings: dict[str, str] = {}
    ledger: list[dict] = []

    for pass_num in sorted(pass_slots.keys()):
        slots_this_pass = [s for s in pass_slots[pass_num] if s not in bound_slots and s in ref_columns]
        if not slots_this_pass:
            continue

        # Available source columns (excluding already-bound ones)
        available_sources = [c for c in src_df.columns if c not in bound_sources]
        if not available_sources:
            break

        # Pass 3 — apply co-occurrence filter per slot
        slot_to_candidates: dict[str, list[str]] = {}
        for slot in slots_this_pass:
            if pass_num == 3 and slot in cooccurrence_anchors:
                anchor = cooccurrence_anchors[slot]
                # anchor must already be bound (Pass 2)
                anchor_src = next((s for s, sl in bindings.items() if sl == anchor), anchor)
                slot_to_candidates[slot] = cooccurrence_filter(
                    available_sources,
                    anchor_src,
                    src_df,
                )
            else:
                slot_to_candidates[slot] = list(available_sources)

        # Build score matrix for THIS pass: rows = union of all candidates, cols = slots
        all_candidates = sorted({c for cands in slot_to_candidates.values() for c in cands})
        if not all_candidates:
            continue

        score_matrix = pd.DataFrame(0.0, index=all_candidates, columns=slots_this_pass)
        components: dict[tuple, dict] = {}
        for slot in slots_this_pass:
            ref_series = ref_columns[slot]
            ref_distinct = ref_series.dropna().nunique()
            for src_col in slot_to_candidates[slot]:
                src_series = src_df[src_col]
                comp = _compute_score_with_components(src_col, src_series, slot, ref_series, ref_distinct)
                score_matrix.loc[src_col, slot] = comp["total"]
                components[(src_col, slot)] = comp

        # Cross-source collision check: for each slot, if top-2 source cols both
        # score >= CONFIDENCE_FLOOR and gap < COLLISION_GAP, emit a collision ledger row.
        for slot in slots_this_pass:
            if slot not in score_matrix.columns:
                continue
            scores_for_slot = score_matrix[slot].sort_values(ascending=False)
            if len(scores_for_slot) < 2:
                continue
            top = float(scores_for_slot.iloc[0])
            runner_up = float(scores_for_slot.iloc[1])
            if top >= CONFIDENCE_FLOOR and runner_up >= CONFIDENCE_FLOOR and (top - runner_up) < COLLISION_GAP:
                top_src = str(scores_for_slot.index[0])
                runner_src = str(scores_for_slot.index[1])
                ledger.append(
                    {
                        "source_col": top_src,
                        "ddl_slot": slot,
                        "score": top,
                        "score_components": components.get((top_src, slot), {}),
                        "second_best_score": runner_up,
                        "second_best_slot": runner_src,  # repurposed: competing source col
                        "pass_number": pass_num,
                        "_cross_source_collision": True,
                    }
                )

        winners = hungarian_assign(score_matrix)

        # Record ledger + book bindings
        for src_col, slot in winners.items():
            if (src_col, slot) not in components:
                continue
            score = score_matrix.loc[src_col, slot]
            # Find runner-up slot for THIS source col
            other_scores = score_matrix.loc[src_col].drop(slot).sort_values(ascending=False)
            second_best = float(other_scores.iloc[0]) if len(other_scores) else 0.0
            second_slot = str(other_scores.index[0]) if len(other_scores) else None

            bindings[src_col] = slot
            bound_sources.add(src_col)
            bound_slots.add(slot)
            ledger.append(
                {
                    "source_col": src_col,
                    "ddl_slot": slot,
                    "score": float(score),
                    "score_components": components[(src_col, slot)],
                    "second_best_score": second_best,
                    "second_best_slot": second_slot,
                    "pass_number": pass_num,
                }
            )

    return bindings, ledger


def _compute_score_with_components(src_col_name, src_values, slot, ref_values, ref_distinct):
    """Wrapper that exposes individual score components for the ledger."""
    from ._scoring import token_jaccard, value_jaccard, dtype_match, shape_cosine, weights_for

    w = weights_for(slot, ref_distinct)
    n = token_jaccard(src_col_name, slot)
    v = value_jaccard(set(src_values.dropna().astype(str).unique()), set(ref_values.dropna().astype(str).unique()))
    d = dtype_match(src_values, ref_values)
    s = shape_cosine(src_values, ref_values)
    total = w["name"] * n + w["value"] * v + w["dtype"] * d + w["shape"] * s
    return {"name": n, "value": v, "dtype": d, "shape": s, "total": total}
