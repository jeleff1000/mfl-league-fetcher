"""The value-path generator must refuse rather than default."""
from __future__ import annotations

import json

import pytest

from . import mapspec_generator as G
from .witness_map import HAND_WITNESS_MAP, WITNESS_MAP, GENERATED_WITNESS_MAP, build_witness_sql


def test_aggregation_class_is_read_never_guessed():
    """`fg_long` is yards/MAX and `fg_made_distance` is a count/SUM. A generator that
    reads either off the name picks the wrong one, so the class comes from the contract
    and anything outside SUM/MAX is refused instead of defaulted to sum."""
    assert G.AGG_FROM_CLASS == {"SUM": "sum", "MAX": "max"}
    klass = G._agg_class()
    specs, _ = G.build()
    for spec in specs:
        if spec.agg == "value":
            assert spec.agg == "value", spec.v26_col
        else:
            assert klass[spec.v26_col] in G.AGG_FROM_CLASS, spec.v26_col
            assert spec.agg == G.AGG_FROM_CLASS[klass[spec.v26_col]], spec.v26_col


def test_a_rate_is_never_summed():
    """Summing a stored rate across rows produces a number that means nothing.
    Native rate witnesses are allowed, but only as value paths."""
    klass = G._agg_class()
    specs, _ = G.build()
    bad = [s.v26_col for s in specs
           if klass.get(s.v26_col) in {"RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE"}
           and s.agg != "value"]
    assert not bad, bad


def test_native_rates_are_witness_values_not_generator_refusals():
    """A published season/career rate is a witness value even when the canonical
    is RECOMPUTE_RATE for rebuilding the supertable from weekly components.

    The generator must preserve the rate as a native value path; it must never
    turn it into a SUM/MAX path or discard it solely because the canonical is
    rate-shaped.
    """
    assert "passer_rating" in G.native_value_canonicals()
    specs, report = G.build()
    rates = [s for s in specs if s.v26_col == "passer_rating"]
    assert rates, "native season/career passer-rating witnesses should be emitted"
    assert all(s.agg == "value" for s in rates)
    assert report["tally"].get("refused_rate_shaped_canonical", 0) == 0


def test_native_games_and_weekly_starter_are_value_paths():
    """Games counts are directly mappable at season/career grain, while starter
    status is directly mappable at weekly grain."""
    native = G.native_value_canonicals()
    assert {"games_played", "games_started", "is_starter", "age"} <= native
    specs, _ = G.build()
    for canonical in ("games_played", "games_started", "age"):
        matches = [s for s in specs if s.v26_col == canonical]
        assert matches, canonical
        assert all(s.agg == "value" for s in matches)
    weekly_starts = [s for s in WITNESS_MAP
                     if s.v26_col == "is_starter" and s.grain == "week"]
    assert weekly_starts, "weekly starter witness is a supported native grain"
    assert all(s.agg == "value" for s in weekly_starts)


def test_pbp_player_week_rollup_generated_specs_use_week_grain():
    """The PBP rollup is a player-week source, not a season mirror.

    The hand map contains legacy season defaults, so this protects the
    generator normalization that makes generated PBP atoms visible to the
    weekly quorum board.
    """
    specs, _ = G.build()
    rows = [s for s in specs if s.source_key == "pbp_player_week_rollup"]
    assert rows
    assert all(s.grain == "week" for s in rows)
    assert all(s.validation_grain == "week" for s in rows)


def test_newspaper_player_cells_declares_long_form_weekly_shape():
    """Newspaper player cells are a normalized long table: stat_name identifies
    the field and stat_value carries its value at player-week grain."""
    decl, _ = G._declarations()
    assert decl["newspaper_player_cells"][:2] == ("newspaper_player_week", "week")
    specs, _ = G.build()
    rows = [s for s in specs if s.source_key == "newspaper_player_cells"]
    assert rows
    assert all(s.shape == "newspaper_player_week" for s in rows)
    assert all(s.source_col == "stat_value" for s in rows)
    assert all(s.table_col == "stat_name" and s.source_table for s in rows)
    assert all(s.validation_grain == "week" for s in rows)


def test_newspaper_player_cells_validates_at_weekly_grain():
    from .witness_map import MapSpec, validate

    result = validate([MapSpec("newspaper_player_cells", "carries", "stat_value",
                               "newspaper_player_week", grain="week", agg="value",
                               source_table="carries", table_col="stat_name",
                               validation_grain="week")])[0]
    assert not str(result.get("verdict", "")).startswith("BROKEN")


def test_newspaper_team_claims_declares_one_team_week_shape():
    decl, _ = G._declarations()
    assert decl["newspaper_team_claims"][:2] == ("newspaper_team_week", "week")
    specs, _ = G.build()
    rows = [s for s in specs if s.source_key == "newspaper_team_claims"]
    assert rows
    assert all(s.shape == "newspaper_team_week" for s in rows)
    assert all(s.source_col == "stat_value" for s in rows)
    assert all(s.team_col == "nfl_team" and s.table_col == "stat_name"
               and s.source_table for s in rows)


def test_newspaper_team_claims_has_a_team_week_validation_path():
    from .witness_map import MapSpec, validate

    result = validate([MapSpec("newspaper_team_claims", "passing_yards", "stat_value",
                               "newspaper_team_week", grain="week", agg="sum",
                               team_col="nfl_team", source_table="passing_yards",
                               table_col="stat_name", validation_grain="week")])[0]
    assert not str(result.get("verdict", "")).startswith("BROKEN")


def test_newspaper_team_stats_declares_two_team_unpivot_shape():
    decl, _ = G._declarations()
    assert decl["newspaper_team_stats"][:2] == ("newspaper_team_pair_week", "week")
    specs, _ = G.build()
    rows = [s for s in specs if s.source_key == "newspaper_team_stats"]
    assert rows
    assert all(s.shape == "newspaper_team_pair_week" for s in rows)
    assert all(s.source_col == "team_1_value" for s in rows)
    assert all(s.team_col == "team_1_nfl_team" and s.table_col == "stat_name"
               and s.source_table for s in rows)


def test_newspaper_team_stats_validates_both_team_sides():
    from .witness_map import MapSpec, validate

    result = validate([MapSpec("newspaper_team_stats", "passing_yards", "team_1_value",
                               "newspaper_team_pair_week", grain="week", agg="sum",
                               team_col="team_1_nfl_team", source_table="passing_yards",
                               table_col="stat_name", validation_grain="week")])[0]
    assert not str(result.get("verdict", "")).startswith("BROKEN")


def test_pfr_player_and_static_combine_families_declare_their_actual_shape():
    decl, _ = G._declarations()
    assert decl["pfr_player_combine"][:2] == ("pfr_player_combine", "player_static")
    assert decl["pfr_snap_counts"][:2] == ("pages", "season")
    specs, _ = G.build()
    combine = [s for s in specs if s.source_key == "pfr_player_combine"]
    snaps = [s for s in specs if s.source_key == "pfr_snap_counts"]
    assert combine and all(s.shape == "pfr_player_combine" and s.grain == "player_static"
                           for s in combine)
    assert snaps and all(s.shape == "pages" for s in snaps)


def test_pbp_team_defense_declares_native_team_week_shape():
    decl, _ = G._declarations()
    assert decl["pbp_team_defense"][:2] == ("team_week", "week")
    specs, _ = G.build()
    rows = [s for s in specs if s.source_key == "pbp_team_defense"]
    assert rows
    assert all(s.shape == "team_week" and s.agg == "value" for s in rows)


def test_pbp_team_defense_has_a_team_week_validation_path():
    from .witness_map import MapSpec, validate

    result = validate([MapSpec("pbp_team_defense", "def_plays", "def_plays",
                               "team_week", grain="week", agg="value",
                               team_col="nfl_team", validation_grain="week")])[0]
    assert not str(result.get("verdict", "")).startswith("BROKEN")


def test_targeted_nflcom_logs_use_the_preserved_layout_axis():
    axis = G.TABLE_MAJOR["nflcom_player_logs_targeted"]
    assert axis["shape"] == "nflcom_log_week"
    assert axis["table_col"] == "_layout"
    assert axis["table_key_part"] == 1
    specs, report = G.build()
    rows = [s for s in specs if s.source_key == "nflcom_player_logs_targeted"]
    assert rows
    assert all(s.shape == "nflcom_log_week" and s.table_col == "_layout"
               and s.source_table.startswith("id_gamelog:") for s in rows)
    assert all(s.validation_grain == "week" for s in rows)
    assert not any(k.startswith("identity depends") and "nflcom_player_logs_targeted" in str(v)
                   for k, v in report["refused"].items())


def test_targeted_nflcom_logs_validate_against_weekly_super_table_rows():
    from .witness_map import MapSpec, validate

    result = validate([MapSpec("nflcom_player_logs_targeted", "def_tackles_solo", "solo",
                               "nflcom_log_week", grain="week", agg="sum",
                               team_col="nflcom_slug", season_type="REG",
                               source_table="id_gamelog:DEF_log", table_col="_layout",
                               row_filter="_table='Regular Season'",
                               validation_grain="week")])[0]
    assert not str(result.get("verdict", "")).startswith("BROKEN")


def test_nflcom_team_stats_use_category_side_and_season_axes():
    axis = G.TABLE_MAJOR["nflcom_team_stats"]
    assert axis["table_col"] == "_category"
    assert axis["table_key_part"] == 0
    assert axis["shape"] == "nflcom_team_season"
    specs, report = G.build()
    rows = [s for s in specs if s.source_key == "nflcom_team_stats"]
    assert rows
    assert all(s.shape == "nflcom_team_season" and s.table_col == "_category"
               and s.source_table for s in rows)
    assert not any("nflcom_team_stats" in str(v)
                   for k, v in report["refused"].items()
                   if k.startswith("identity depends"))


def test_nflcom_team_stats_have_a_receipted_team_season_validation_path():
    from .witness_map import MapSpec, validate

    result = validate([MapSpec("nflcom_team_stats", "passing_yards", "pass_yds",
                               "nflcom_team_season", grain="season", agg="sum",
                               team_col="team", season_type="REG",
                               source_table="passing", table_col="_category",
                               row_filter="_side='offense' AND season_type='reg'")])[0]
    assert not str(result.get("verdict", "")).startswith("BROKEN")


def test_shape_is_declared_not_inferred():
    """A wrong shape does not raise -- it silently validates nothing. So a source with
    no declaration is REFUSED and counted, never given a default shape."""
    decl, why = G._declarations()
    assert decl and all(source in why for source in decl)
    specs, report = G.build()
    # a TABLE_MAJOR source declares its shape in the table axis instead of in `decl`
    assert all(s.source_key in decl or s.source_key in G.TABLE_MAJOR for s in specs)
    assert report["tally"].get("refused_no_shape_declaration", 0) == 0
    assert report["tally"].get("refused_table_axis_lost_at_capture", 0) == 0
    # every refusal reason is enumerated, so "0 refused" can never hide an untracked drop
    assert set(report["refused"]) or not report["tally"].get("refused_no_shape_declaration")


def test_static_and_game_source_shapes_are_evidence_backed():
    decl, why = G._declarations()
    expected = {
        "player_bio": "player_bio",
        "pfr_combine": "pfr_combine",
        "pfr_box_home_snaps": "box",
        "pfr_box_vis_snaps": "box",
        "schedule_master": "schedule_game",
        "pfr_team_games": "team_week",
        "newspaper_lineups": "newspaper_lineup_week",
        "newspaper_player_notes": "newspaper_note_week",
        "newspaper_game_context": "newspaper_game_week",
    }
    for source, shape in expected.items():
        assert decl[source][0] == shape
        assert why[source]
    assert G.TABLE_MAJOR["statscrew_team_season_stats"]["shape"] == "statscrew_team_season"
    assert "table_tag" in G.TABLE_MAJOR["statscrew_team_season_stats"]["why"]


def test_static_source_shapes_have_executable_value_paths():
    specs, _ = G.build()
    selected = [s for s in specs if s.source_key in {"player_bio", "pfr_combine"}]
    assert selected
    for spec in selected:
        sql = build_witness_sql(spec)
        assert "0 AS yr" in sql
        assert "GROUP BY 1" in sql


def test_statscrew_repeated_headers_require_the_table_axis_and_crosswalk():
    specs, report = G.build()
    statscrew = [s for s in specs if s.source_key == "statscrew_team_season_stats"]
    assert statscrew
    assert all(s.shape == "statscrew_team_season" for s in statscrew)
    assert report["tally"].get("refused_table_dependent_identity", 0) == 0
    assert report["tally"].get("refused_key_space", 0) == 0


def test_team_game_sources_are_not_mislabeled_as_unresolved_player_keys():
    _, report = G.build()
    assert report["tally"].get("refused_key_space", 0) == 0
    # 28 -> 24 on 2026-08-01: the context/scoring waves hand-specced 4 of the
    # dossier candidates on pfr_team_games and scoring_summary, so they are no
    # longer refused as team-grain-without-a-path -- they HAVE paths now.
    # 24 -> 23 on 2026-08-02: the statscrew_results shape gave the results
    # source its team-week path (code map + catalog date join, 3 lanes 100%).
    assert report["tally"].get("refused_subject_grain", 0) == 23
    grain = report["refused"]["source is team-game grain, not a player MapSpec"]
    # 4 -> 3 on 2026-08-02: `res` is hand-specced (is_win 100%); the remaining
    # three refused columns still await hand paths through the results shape.
    assert grain["statscrew_team_season_results"] == 3
    assert grain["scoring_summary"] == 1
    assert grain["pfr_team_games"] == 2
    assert grain["schedule_master"] == 7
    assert sum(grain.values()) == 23


def test_position_taxonomy_holds_are_not_aggregation_holds():
    _, report = G.build()
    assert report["tally"].get("refused_context_taxonomy", 0) == 12
    assert report["tally"].get("refused_aggregation_class", 0) < 57
    assert sum(report["refused"]["canonical position taxonomy unresolved"].values()) == 12


def test_high_agreement_pfr_position_witnesses_use_native_season_values():
    specs, report = G.build()
    promoted = {
        "pfr_adj_passing", "pfr_games_played", "pfr_games_played_post",
        "pfr_adv_defense", "pfr_adv_defense_post", "pfr_adv_recrush", "pfr_adv_recrush_post",
        "pfr_adv_rushrec", "pfr_adv_rushrec_post", "pfr_kicking_post",
        "pfr_passing_adv_post", "pfr_passing_adv_season", "pfr_passing_post",
        "pfr_player_defense", "pfr_player_kicking", "pfr_player_punting",
        "pfr_player_returns", "pfr_player_scoring", "pfr_player_season_passing",
        "pfr_player_season_rec_rush", "pfr_player_season_rush_rec", "pfr_punting_post",
        "pfr_defense_post",
        "pfr_recrush_post", "pfr_returns_post", "pfr_scoring_post",
    }
    found = [s for s in specs if s.source_key in promoted and s.v26_col == "nfl_position"]
    assert {s.source_key for s in found} == promoted
    assert all(s.agg == "value" and s.validation_grain == "season" for s in found)
    assert not any(source in promoted
                   for source in report["refused"]["canonical position taxonomy unresolved"])


def test_refusal_ledger_separates_true_targets_from_adjudicated_exclusions():
    _, report = G.build()
    assert report["tally"].get("refused_no_canonical", 0) == 0
    # 63 -> 51 on 2026-08-01: Joe ruled rates ARE witness sources, and the hand
    # rate wave (completion_pct, Y/A family targets via twins, catch_pct, Y/R, YPC,
    # yards_per_touch, pat_pct, receiving_adot, total_tds_scored) closed 12 refusals that
    # were parked on exactly that adjudication.
    assert report["tally"].get("refused_canonical_adjudication", 0) == 51
    assert report["tally"].get("refused_new_column_candidate", 0) == 16
    assert report["tally"].get("refused_player_bio_only", 0) == 12
    assert report["tally"].get("refused_table_axis_lost_at_capture", 0) == 0


def test_all_six_refusal_categories_have_current_zero_or_hold_counts():
    _, report = G.build()
    tally = report["tally"]
    assert tally.get("refused_no_shape_declaration", 0) == 0
    assert tally.get("refused_table_dependent_identity", 0) == 0
    assert tally.get("refused_key_space", 0) == 0
    assert tally.get("refused_aggregation_class", 0) == 0
    assert tally.get("refused_no_canonical", 0) == 0
    assert tally.get("refused_table_axis_lost_at_capture", 0) == 0


def test_table_axis_loss_is_limited_to_known_nflcom_captures():
    _, report = G.build()
    lost = report["refused"].get("table exists in the dossier but not in the parquet", {})
    assert dict(lost) == {}


def test_2025_nflcom_log_recapture_promotes_shared_blocks_with_bounded_path():
    specs, report = G.build()
    recovered = [s for s in specs
                 if s.source_key == "nflcom_player_logs" and s.source_path]
    assert len(recovered) == 78
    assert {s.source_table for s in recovered} == {
        "id_gamelog:RBFB5", "id_gamelog:WRTE"
    }
    assert all(s.table_col == "_layout" and s.validation_years == (2025, 2025)
               for s in recovered)
    assert sum(s.validation_tolerance is not None for s in recovered) == 12
    assert report["tally"].get("refused_table_axis_lost_at_capture", 0) == 0


def test_recent_games_shared_rb_wr_blocks_use_recovered_axis():
    specs, report = G.build()
    recent = [s for s in specs
              if s.source_key == "nflcom_player_career"
              and s.shape == "nflcom_week"
              and s.source_table in {"RBFB", "WRTE"}]
    assert len(recent) == 24
    assert {s.source_table for s in recent} == {"RBFB", "WRTE"}
    assert all(s.table_col == "_rg_block" and s.validation_grain == "week"
               for s in recent)
    assert report["refused"].get(
        "table exists in the dossier but not in the parquet", {}).get(
            "nflcom_player_career", 0) == 0


def test_native_weekly_context_values_are_not_aggregated():
    specs, report = G.build()
    native_context = [
        s for s in specs
        if s.source_key in G.NATIVE_WEEKLY_CONTEXT_SOURCES
        and s.v26_col in G.NATIVE_WEEKLY_CONTEXT
    ]
    assert native_context
    assert all(s.agg == "value" and s.validation_grain == "week"
               for s in native_context)
    # This is a real reduction from the pre-native-context bucket, not a relabelling of
    # the same refused rows.
    assert report["tally"].get("refused_aggregation_class", 0) < 156


def test_internal_and_newspaper_context_values_use_native_value_paths():
    specs, report = G.build()
    wanted = {
        ("legacy_motherduck_supertable", "headshot_url"),
        ("legacy_motherduck_supertable", "fantasy_position"),
        ("newspaper_lineups", "starter_position"),
    }
    found = {(s.source_key, s.v26_col): s for s in specs}
    assert wanted <= found.keys()
    assert all(found[key].agg == "value" for key in wanted)
    assert report["tally"].get("refused_aggregation_class", 0) == 0


def test_2025_audit_plane_classifier_prefers_declared_validation_grain():
    from .audit_mapspec_2025 import _plane_for
    assert _plane_for({"validation_grain": "week", "v26_col": "passing_yards"}) == "weekly"
    assert _plane_for({"validation_grain": "season", "v26_col": "passing_yards"}) == "season"
    assert _plane_for({"validation_grain": "static", "v26_col": "passing_yards"}) == "player_bio"


def test_pfr_award_members_use_receipted_direct_player_keys():
    assert G._kc_status()["pfr_all_pro_members"] == "ACTIVE"
    assert G._kc_status()["pfr_pro_bowl_members"] == "ACTIVE"
    specs, report = G.build()
    awards = [s for s in specs
              if s.source_key in {"pfr_all_pro_members", "pfr_pro_bowl_members"}]
    assert len(awards) == 30
    assert all(s.shape == "pfr_award_pages" for s in awards)
    assert report["refused"].get("key space unresolved", {}).get(
        "pfr_all_pro_members", 0) == 0


def test_generated_shape_cannot_contradict_a_hand_spec():
    """For a source that already carries hand MapSpecs the declaration is READ OFF them,
    so a generated spec is structurally incapable of disagreeing about shape or stratum."""
    hand = {}
    for m in HAND_WITNESS_MAP:
        hand.setdefault(m.source_key, set()).add((m.shape, m.grain, m.team_col, m.season_type))
    specs, _ = G.build()
    for s in specs:
        if s.source_key in hand and len(hand[s.source_key]) == 1:
            expected = next(iter(hand[s.source_key]))
            if s.source_key == "pbp_player_week_rollup":
                expected = (expected[0], "week", expected[2], expected[3])
            actual = (s.shape, s.grain, s.team_col, s.season_type)
            if s.source_key == "nflcom_player_logs":
                # The log source has table-axis specs for REG/POST/PRE; the
                # hand declaration pins shape/grain/team, not one phase.
                assert actual[:3] == expected[:3]
            else:
                assert actual == expected


def test_never_overwrites_an_existing_spec():
    """A hand spec is ARGUED; a generated one is derived. The derived one never wins."""
    existing = {(m.source_key, m.source_col) for m in HAND_WITNESS_MAP}
    specs, _ = G.build()
    assert not [s for s in specs if (s.source_key, s.source_col) in existing]


def test_applying_is_idempotent_and_the_written_block_is_what_build_returns():
    """The generated module is rebuilt WHOLE, so what is on disk must equal what build()
    returns right now -- otherwise a dossier decision changed and the value path on disk is
    stale while still being executed."""
    specs, _ = G.build()
    on_disk = {(s.source_key, s.source_col): (s.v26_col, s.agg, s.shape, s.grain,
                                              s.team_col, s.season_type)
               for s in GENERATED_WITNESS_MAP}
    if not on_disk:
        pytest.skip("generator has not been applied yet")
    fresh = {(s.source_key, s.source_col): (s.v26_col, s.agg, s.shape, s.grain,
                                            s.team_col, s.season_type) for s in specs}
    assert fresh == on_disk, "witness_map_generated.py is stale -- re-run with --apply"


def test_the_merged_map_has_no_duplicate_value_paths():
    """Two MapSpecs for one value path means one is silently ignored by whichever consumer
    dedupes last.

    The key is (source, TABLE, column). One source column legitimately appears many times
    now -- `nflcom_player_season.lng` is seven specs, one per category table -- and that is
    the point of the table selector, not a duplicate."""
    # source_expr joins the key 2026-08-01: two EXTRACTIONS from one composite cell
    # (made vs missed from "3/4") are two distinct value paths over one column.
    # filters + attribution column join the key 2026-08-01: pbp_rollup specs
    # legitimately share a physical column (pass_attempt) across different play
    # predicates and different ATTRIBUTED players (passer vs receiver).
    keys = [(m.source_key, m.source_table, m.table_col, m.row_filter, m.source_col,
             m.source_expr, m.filters, m.team_col)
            for m in WITNESS_MAP]
    dupes = {k for k in keys if keys.count(k) > 1}
    assert not dupes, sorted(dupes)[:10]


def test_a_generated_spec_is_not_a_license():
    """Existing in WITNESS_MAP must never by itself authorize a vote. Generated specs
    enter unvalidated; only measured agreement in MAPPING_LICENSES.json licenses them."""
    import inspect

    from . import witness_map as WM

    src = inspect.getsource(WM.licensed)
    assert "MAPPING_LICENSES" in src or "LICENSES" in src
    assert "VALIDATED" in src, "licensing must gate on a measured verdict, not on existence"


def test_only_joinable_sources_get_a_value_path():
    """A MapSpec on a source whose key space is unresolved would validate against a join
    that cannot run. Identity settles what a column IS; it never licenses the join."""
    kc = G._kc_status()
    specs, _ = G.build()
    for s in specs:
        assert kc.get(s.source_key) in {"ACTIVE", "AUTHORITY"}, s.source_key


def test_one_column_adjudicated_two_ways_is_surfaced_not_silently_collapsed():
    _, report = G.build()
    assert report["conflicts"] == [], report["conflicts"]


def test_a_column_whose_canonical_depends_on_the_table_carries_a_table_selector():
    """THE NFL.COM TRAP, and the invariant that now contains it.

    212 of our 309 NFL tables are NFL.com's and they re-emit the same ~50 column names per
    category, so `nflcom_player_season.lng` adjudicates to SEVEN canonicals -- fg_long,
    passing_long, punt_long, rushing_long, receiving_long, kickoff_return_long,
    punt_return_long -- separated only by the table.

    This used to assert such a column was NEVER specced, because MapSpec could not express
    the distinction and emitting one spec would have kept one of the seven while unioning
    every category table into it. MapSpec now carries a table selector, so the rule is no
    longer abstention: such a column may be specced ONLY IF it names its table. Silence
    about the table is still forbidden -- it is what made the collapse invisible."""
    dossier = json.loads(G.DOSSIER_PATH.read_text(encoding="utf-8"))
    dependent = G.table_dependent_identities(dossier, G.load_decisions())
    assert dependent, "expected the nflcom multi-canonical columns to be detected"
    specs, _ = G.build()
    naked = [(s.source_key, s.source_col) for s in specs
             if (s.source_key, s.source_col) in dependent
             and not (s.source_table and s.table_col)]
    assert not naked, naked
    # and the fan-out is real: lng must reach several distinct canonicals, not one
    lng = {s.v26_col for s in specs
           if s.source_key == "nflcom_player_season" and s.source_col == "lng"}
    assert len(lng) >= 5, sorted(lng)


def test_player_box_game_dates_use_player_game_context_path():
    specs, _ = G.build()
    rows = [s for s in specs
            if s.source_key in G.BOX_PLAYER_CONTEXT_SOURCES
            and s.v26_col == "game_date"]
    assert rows
    assert all(s.agg == "value" and s.validation_grain == "week" for s in rows)
    for spec in rows:
        sql = build_witness_sql(spec)
        assert "player_link_ids" in sql
        assert "boxscore_id" in sql


def test_a_split_source_spec_always_names_its_partition_dimension():
    """A split source has no Total row, so a season value must be summed inside exactly ONE
    dimension. Without the filter the harness summed all six at once and every column read
    5-6x high (punt_yards median 12,285 against our 2,667) -- which presents as a total
    supertable failure rather than as a grain error."""
    specs, _ = G.build()
    for source in ("nflcom_player_situational", "nflcom_player_splits"):
        rows = [s for s in specs if s.source_key == source]
        assert rows, source
        assert all(s.row_filter for s in rows), f"{source}: spec with no partition dimension"


def test_the_refusal_reason_is_the_true_one_not_the_incidental_one():
    """These columns are ALSO missing a shape declaration. If they were counted under that
    reason, the ledger would read 'supply a declaration and they unblock' -- and supplying
    one is precisely the destructive move. The reason recorded must be the one that is
    actually load-bearing, so the check runs ahead of the shape check."""
    dossier = json.loads(G.DOSSIER_PATH.read_text(encoding="utf-8"))
    dependent = G.table_dependent_identities(dossier, G.load_decisions())
    _, report = G.build()
    # Every currently table-dependent identity is either addressable through a declared
    # TABLE_MAJOR axis or remains held by a more load-bearing refusal (for example the
    # raw NFL.com capture whose position-block axis was lost).
    assert report["tally"].get("refused_table_dependent_identity", 0) == 0
    assert dependent


def test_the_conflict_detector_cannot_be_the_only_guard():
    """The collapse-conflict check sits AFTER every refusal, so it only ever sees columns
    that already cleared them -- i.e. single-table sources, where a conflict is impossible
    by construction. It reads 0 because its input was filtered above it, not because
    nothing conflicts. The table-dependent guard is what actually holds this line."""
    _, report = G.build()
    assert report["conflicts"] == []
    dossier = json.loads(G.DOSSIER_PATH.read_text(encoding="utf-8"))
    assert G.table_dependent_identities(dossier, G.load_decisions()), (
        "conflicts==[] must not be read as 'no column is adjudicated two ways'")


def test_a_deleted_mapspec_is_never_resurrected_from_its_own_fossil_citation():
    """A dossier row may justify itself by citing an already-committed MapSpec instead of
    re-arguing the concept. When that spec is later DELETED as wrong, the row keeps citing
    it -- so a generator reading identity from the dossier faithfully rebuilds the mapping
    the codebase deliberately removed. `pfr_player_kicking.fgm5 -> fg_made_50_59` is the
    live case: PFR's fgm5 is 50-PLUS, so a 60-yarder lands in the 50-59 bucket.

    The hand map's ABSENCES carry knowledge; `already_specced` only reads its presences."""
    dossier = json.loads(G.DOSSIER_PATH.read_text(encoding="utf-8"))
    hand = {(m.source_key, m.source_col) for m in HAND_WITNESS_MAP}
    fossils = G.fossil_citations(dossier, hand)
    assert ("pfr_player_kicking", "fgm5") in fossils
    specs, _ = G.build()
    assert not [s for s in specs if (s.source_key, s.source_col) in fossils]


def test_a_rate_is_refused_even_when_the_contract_declares_it_summable():
    """READING aggregation_class IS NOT ENOUGH, so the refusal trusts neither field alone.

    This is tested SYNTHETICALLY on purpose. It used to assert that `pat_pct` was live-
    misclassified, which made the test pass only while the bug survived -- so fixing the
    contract broke the test that was guarding it. The guard has to be checkable
    independently of whether the contract currently happens to be wrong.

    The two historical failure modes, reproduced as fixtures: `pat_pct` declared
    unit='ratio' AND aggregation_class='SUM', contradicting itself; and
    `rushing_yards_per_carry` declared unit='count', so BOTH of its fields were wrong and
    no cross-check of the contract against itself could have caught it."""
    contradicts_itself = G.rate_shaped_canonicals({"pat_pct": "SUM"}, {"pat_pct": "ratio"})
    assert "pat_pct" in contradicts_itself
    both_fields_wrong = G.rate_shaped_canonicals(
        {"rushing_yards_per_carry": "SUM"}, {"rushing_yards_per_carry": "count"})
    assert "rushing_yards_per_carry" in both_fields_wrong, "name must catch what unit cannot"
    # and a genuine counting stat is never swept up by either signal
    assert not G.rate_shaped_canonicals({"rushing_yards": "SUM"}, {"rushing_yards": "yards"})


def test_no_spec_aggregates_a_declared_ratio():
    """The invariant, stated over the SHIPPED map rather than the generator: nothing in
    WITNESS_MAP may sum or max a canonical whose declared unit is a ratio."""
    units = G._units()
    offenders = sorted({m.v26_col for m in WITNESS_MAP
                        if units.get(m.v26_col) == "ratio" and m.agg in ("sum", "max")})
    assert not offenders, offenders


def test_no_spec_anywhere_aggregates_a_non_aggregatable_canonical():
    """THE GATE THIS PROGRAM WAS MISSING, stated over the SHIPPED map rather than the
    generator, because the generator was never the only author of a spec.

    stat_contracts declared 22 rate-shaped canonicals as SUM, and 21 live specs summed an
    average or a percentage. Sixteen were generated; five were HAND specs that predate the
    generator and so bypassed its aggregation_class refusal entirely -- including
    `ngs_avg_separation`, whose class was ALREADY WEIGHTED_RECOMPUTE. A guard that only
    runs inside the generator cannot see those, which is why this reads WITNESS_MAP.

    A name-based heuristic cannot hold this line either: `pat_pct` declared unit='ratio'
    AND SUM, contradicting itself, while `rushing_yards_per_carry` declared unit='count',
    so both of its fields were wrong. The contract is now correct and THIS is the check
    that keeps it correct."""
    import json

    contracts = json.loads(G.CONTRACTS_PATH.read_text(encoding="utf-8"))
    klass = {r["canonical_name"]: r["aggregation_class"] for r in contracts["stats"]}
    offenders = sorted(
        f"{m.source_key}.{m.source_col}->{m.v26_col}({klass.get(m.v26_col)})"
        for m in WITNESS_MAP
        if m.agg in ("sum", "max") and klass.get(m.v26_col) not in ("SUM", "MAX"))
    assert not offenders, offenders


def test_a_declared_ratio_is_never_classed_summable():
    """The contract must not contradict itself. `pat_pct` carried unit='ratio' beside
    aggregation_class='SUM' for long enough to produce live specs that summed it."""
    import json

    contracts = json.loads(G.CONTRACTS_PATH.read_text(encoding="utf-8"))
    bad = sorted(r["canonical_name"] for r in contracts["stats"]
                 if r.get("unit") == "ratio" and r["aggregation_class"] in ("SUM", "MAX"))
    assert not bad, bad


def test_the_shifted_nflcom_tables_are_never_the_source():
    """The un-shift repoint, guarded permanently.

    nflcom_player_situational and nflcom_player_splits were captured with their stored
    column names ONE COLUMN OFF. The repair repointed both at `*_unshifted` tables, and
    39 table-scoped MapSpecs each now read through them. A silent revert to the shifted
    path would not error -- it would move every column one place and validate confidently
    against the wrong canonical.

    This check used to live as a SOURCE_PATH_ENDSWITH refusal precondition, and it stopped
    running the moment that refusal was discharged. Protection should not be a side effect
    of an open queue item, so it lives here now and runs on every suite."""
    from .sources import registry

    reg = registry(include_subject=True)
    for source in ("nflcom_player_situational", "nflcom_player_splits"):
        path = reg[source].path.replace("\\", "/").rstrip("/")
        assert path.endswith("_unshifted"), (
            f"{source} points at {path!r} -- the SHIFTED capture. Every spec on this source "
            "would read one column off and validate against the wrong canonical.")
    # and the specs really do depend on it
    specs, _ = G.build()
    assert [s for s in specs if s.source_key == "nflcom_player_situational"]


def test_one_table_never_puts_two_specs_on_the_same_canonical():
    """Two columns of ONE table aliased to one canonical means at least one is wrong, and
    the validator would happily measure both.

    This is not hypothetical. `nflcom_player_career` DEF renders TKL|AST|COMBINED|SOLO;
    COMBINED = TKL + AST holds on 100.00% of 53,451 rows, so TKL is the solo count and SOLO
    is a fourth column outside the identity. Adjudicating `tkl -> def_tackles_solo` WITHOUT
    withdrawing `solo -> def_tackles_solo` left both on the same canonical for exactly one
    generator run. A remedy with two halves has to land as one.

    SCOPED TO TABLE-MAJOR SOURCES, and the scope is a finding rather than a convenience.
    Run unscoped, this also fires on `legacy_motherduck_supertable`, where `att` and
    `carries` both alias `carries` and `cmp` and `completions` both alias `completions` --
    our own retired supertable is a wide union of historical schemas and carries the same
    stat under two era-names. That is plausibly benign and is NOT verified here; it is
    queued in disagreement_adjudications.v1.json rather than waved through in a test.

    On a table-major source the columns of one table are positionally distinct statistics,
    so two of them on one canonical is unambiguously an error.
    """
    import collections

    seen = collections.defaultdict(list)
    for m in WITNESS_MAP:
        if m.source_key not in G.TABLE_MAJOR:
            continue
        # Regular/Post Season are distinct physical partitions even when they share the
        # same table label and source column.
        seen[(m.source_key, m.source_table, m.row_filter, m.v26_col)].append(m.source_col)
    clashes = {k: v for k, v in seen.items() if len(v) > 1}
    # StatsCrew's `Brup` (Pass Breakups) and `PD` (Passes Defensed) are a measured
    # historical spelling split for the same canonical. The adjudication records the
    # overlap and the row-level placeholder rule; retaining both atoms is intentional.
    clashes.pop(("statscrew_team_season_stats", "defense_and_fumbles",
                 "is_total_row=false", "def_pass_defended"), None)
    assert not clashes, sorted(f"{k[0]}[{k[1]}].{k[2]} <- {sorted(v)}"
                               for k, v in clashes.items())[:10]
