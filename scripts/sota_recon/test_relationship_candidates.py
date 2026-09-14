"""Contract tests for the O.6 R1-R9 candidate generator (§19.3).

Run:  python -m pytest scripts/sota_recon/test_relationship_candidates.py -q
"""

from __future__ import annotations

from .relationship_candidates import Candidate, by_class, generate, scope_columns

R_CLASSES = {"R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9"}


def test_generation_is_deterministic():
    a, b = generate(), generate()
    assert a == b


def test_candidate_ids_unique():
    ids = [c.cand_id for c in generate()]
    assert len(ids) == len(set(ids))


def test_all_nine_classes_proposed():
    assert set(by_class()) == R_CLASSES


def test_components_stay_in_contract_scope():
    scope = set(scope_columns())
    for c in generate():
        if c.test_kind == "team_witness_join":
            continue  # rhs names a foreign witness column, not a v26 column
        assert c.lhs in scope, c.cand_id
        for comp in c.rhs:
            assert comp in scope, c.cand_id


def test_weights_align_with_rhs():
    for c in generate():
        assert not c.weights or len(c.weights) == len(c.rhs), c.cand_id


def test_ops_and_vocabulary():
    for c in generate():
        assert c.op in ("eq", "le"), c.cand_id
        assert c.r_class in R_CLASSES, c.cand_id


def test_newspaper_crossing_always_escalated():
    for c in generate():
        crosses = "newspaper" in c.lhs or any("newspaper" in r for r in c.rhs) \
            or "newspaper" in c.note
        if crosses:
            assert c.escalation == "newspaper_root_crossing", c.cand_id


def test_known_composites_present():
    ids = {c.cand_id for c in generate()}
    assert "R1:player_week:touches=carries+receptions" in ids
    assert "R1:player_week:fg_missed=fg_att+fg_made" in ids  # weights (1,-1)
    assert "R7:player_week:fg_made=fg_made_0_19+fg_made_20_29+fg_made_30_39+fg_made_40_49+fg_made_50_59+fg_made_60_" in ids


def test_fg_missed_is_a_difference():
    c = next(x for x in generate()
             if x.cand_id == "R1:player_week:fg_missed=fg_att+fg_made")
    assert c.weights == (1.0, -1.0)


def test_r6_edges_only_for_sum_or_max():
    for c in generate():
        if c.r_class == "R6":
            assert c.agg in ("SUM", "MAX"), c.cand_id


def test_population_vocabulary():
    for c in generate():
        assert c.population in ("all", "team_def_row", "idp_rows"), c.cand_id
        if c.population != "all":
            assert c.cand_id.endswith(f"@{c.population}"), c.cand_id


def test_idp_team_defense_planes_never_summed_together():
    """IDP and team-defense are dual bookkeeping. Every def-side conservation rhs
    is population-scoped, and every defense/fumbles-family stat gets the
    IDP -> team-DEF vertical reconciliation edge."""
    cands = generate()
    scoped = {c.lhs for c in cands if c.test_kind == "team_idp_vertical"}
    from .relationship_candidates import IDP_TEAM_VERTICAL_FAMILIES, scope_columns
    expected = {sid for sid, s in scope_columns().items()
                if s["family"] in IDP_TEAM_VERTICAL_FAMILIES}
    assert scoped == expected
    for c in cands:
        if c.test_kind == "team_idp_vertical":
            assert c.population == "team_def_row", c.cand_id
    # the def-side league conservations exist in BOTH population scopes
    ids = {c.cand_id for c in cands}
    assert "R8:league_year:passing_interceptions=def_interceptions@team_def_row" in ids
    assert "R8:league_year:passing_interceptions=def_interceptions@idp_rows" in ids
    assert "R8:league_year:passing_interceptions=def_interceptions" not in ids
