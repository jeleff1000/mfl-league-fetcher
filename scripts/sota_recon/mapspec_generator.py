"""Turn adjudicated column identities into MapSpecs -- the missing value path.

WHY THIS EXISTS. A witness column needs three things before it can audit a supertable
cell, and they fail independently:

    1. IDENTITY   the dossier says what the column IS            -> column_dossier
    2. KEY        a receipted crosswalk lands it on our key      -> kc_planes + receipts
    3. VALUE      a validator can pull the number through it     -> WITNESS_MAP  <-- here

Measured 2026-07-29: 2,989 witness columns clear (1), and after the nflcom slug receipt
was wired into kc_planes most of them clear (2) -- but only 255 had a MapSpec, so 96.7%
of our witness material could not audit anything. The identity work was done; the value
path simply had never been written.

THE MAPPING IS NOT INVENTED HERE. `source_col -> v26_col` comes from the dossier's own
MAPPED_TO_CANONICAL decisions, which were argued per concept and receipted. This module
only supplies the three MECHANICAL fields a MapSpec needs on top of that pair:

    agg          from stat_contracts.aggregation_class -- NEVER from the name.
                 `fg_long` is yards/MAX and `fg_made_distance` is a count/SUM; reading
                 either off its name picks the wrong one. A canonical whose class is not
                 SUM or MAX is REFUSED, not defaulted: a rate cannot be summed across
                 rows and a FIRST/NON_AGGREGATABLE cell is not a measurement to add up.
    shape        DECLARED per source, never inferred. Shape decides which SQL the
                 validator emits, and a wrong shape does not error -- it silently
                 validates nothing, which is worse than failing.
    grain/team_col/season_type
                 same: declared per source.

WHERE THE DECLARATION COMES FROM. For a source that ALREADY carries hand MapSpecs the
declaration is READ OFF THOSE SPECS, so a generated spec structurally cannot disagree
with a hand-written one about shape or stratum. For a source with no spec yet, it must
appear in NEW_SOURCE_SHAPES below with the evidence for the call. A source in neither
place is REFUSED and counted -- silence is never a default.

Run:  python -m scripts.sota_recon.mapspec_generator [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

from .column_dossier import load_decisions, row_key
from .witness_map import MapSpec, HAND_WITNESS_MAP

ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = Path(__file__).resolve().parent / "witness_map_generated.py"
DOSSIER_PATH = ROOT / "docs" / "column-dossier.json"
KC_PATH = Path(__file__).resolve().parent / "witness_gate" / "contracts" / "kc_planes.v1.json"
CONTRACTS_PATH = Path(__file__).resolve().parent / "witness_gate" / "contracts" / "stat_contracts.v1.json"
GENERATOR = "scripts.sota_recon.mapspec_generator"

# aggregation_class -> MapSpec.agg for values that are safe to aggregate from lower
# grain rows. Native-grain values are handled explicitly below; they must never be
# silently coerced into one of these reducers.
AGG_FROM_CLASS = {"SUM": "sum", "MAX": "max"}

# Sources with no hand MapSpec yet. Each entry is (shape, grain, team_col, season_type)
# plus the evidence for the call -- a declaration without a reason is not a declaration.
NEW_SOURCE_SHAPES: dict[str, tuple[tuple[str, str, str, str], str]] = {
    # The three ancient recovery bundles are the SAME 106-column schema as
    # `ancient_pfa_gamelog`, which already carries 13 hand MapSpecs at
    # ('flat','season','team_name_abbr','REG'). They were split out of one physical
    # bundle by OQ-LR-5 on lineage, not on shape, so the shape is inherited by
    # construction rather than guessed.
    "ancient_pfr_recovery": (("flat", "season", "team_name_abbr", "REG"),
                             "same physical schema as ancient_pfa_gamelog (OQ-LR-5 split "
                             "one bundle by lineage; shape was never per-lineage)"),
    "ancient_pbp1978_recovery": (("flat", "season", "team_name_abbr", "REG"),
                                 "same physical schema as ancient_pfa_gamelog"),
    "ancient_newspaper_ocr": (("flat", "season", "team_name_abbr", "REG"),
                              "same physical schema as ancient_pfa_gamelog; NON-VOTING "
                              "by lineage policy, so the spec exists to be validated, "
                              "not to vote"),
    # The current newspaper player witness is normalized long-form: one row per
    # (player, week, stat_name), with the number in stat_value. The dossier's
    # column is the controlled stat_name, not a physical value column.
    "newspaper_player_cells": (("newspaper_player_week", "week", "nfl_team", "REG"),
                               "normalized long-form player-week witness with explicit "
                               "NFL_player_id/year/week keys; stat_name selects the "
                               "canonical and stat_value carries the measured value"),
    "newspaper_team_claims": (("newspaper_team_week", "week", "nfl_team", "REG"),
                              "normalized one-team-per-row team-week witness with "
                              "stat_name selector and stat_value payload"),
    "newspaper_team_stats": (("newspaper_team_pair_week", "week", "team_1_nfl_team", "REG"),
                             "normalized two-team-per-row team-week witness; each row "
                             "must be unpivoted from team_1/team_2 before comparison"),
    "pfr_player_combine": (("pfr_player_combine", "player_static", "pfr_id", "REG"),
                           "PFR combine player capture is a static-player table keyed by "
                           "direct pfr_id; measurements compare to the player-bio static "
                           "plane rather than a season row"),
    "pfr_snap_counts": (("pages", "season", "team", "REG"),
                        "PFR player page table with pfr_id/year_id and a team column; "
                        "Career rows are excluded by the pages year parser"),
    # These are not player-stat tables, but their dossier columns are real mapped
    # measurements/axes. Declaring the physical grain moves them out of the misleading
    # no-shape bucket; NON_AGGREGATABLE fields then stop at the aggregation-class gate.
    "pfr_box_home_snaps": (("box", "game", "boxscore_id", "REG"),
                           "PFR home snap-count rows are one player per boxscore; the "
                           "capture carries boxscore_id, player_link_ids, and snap fields"),
    "pfr_box_vis_snaps": (("box", "game", "boxscore_id", "REG"),
                          "PFR visitor snap-count rows have the identical one-player per "
                          "boxscore schema; home/visitor is a physical table split"),
    "schedule_master": (("schedule_game", "game", "nfl_team", "REG"),
                        "PFR schedule master is one team-game row keyed by year/week/team/"
                        "opponent, with explicit season_phase and score fields"),
    "pfr_team_games": (("team_game", "game", "team_code", "REG"),
                       "PFR team-games capture is one team-game row with explicit year, "
                       "week, season_type, team_fid, opponent_fid, and score fields"),
    "newspaper_lineups": (("newspaper_lineup_week", "week", "nfl_team", "REG"),
                          "newspaper lineup sidecar is one resolved player-week row with "
                          "NFL_player_id/player_week, team/opponent, position, and starter "
                          "participation fields; atoms remain immutable"),
    "newspaper_player_notes": (("newspaper_note_week", "week", "nfl_team", "REG"),
                               "newspaper note sidecar carries explicit resolved player, "
                               "game date/year/week, team, and opponent axes; note text is "
                               "context evidence, not a numeric stat"),
    "newspaper_game_context": (("newspaper_game_week", "week", "team_1_resolved", "REG"),
                               "newspaper game-context sidecar carries one game key, date, "
                               "year/week, season_type, both resolved teams, and scores"),
    "statscrew_team_season_roster": (("statscrew_roster_season", "season", "source_player_id", "REG"),
                                      "StatsCrew roster rows carry source_player_id, season, "
                                      "team, and player position; source IDs use the receipted "
                                      "StatsCrew-to-PFR crosswalk"),
    "pfr_box_home_starters": (("box", "game", "boxscore_id", "REG"),
                              "PFR home starter rows are one linked player per boxscore "
                              "with explicit boxscore_id, player_link_ids, game date, and "
                              "position"),
    "pfr_box_vis_starters": (("box", "game", "boxscore_id", "REG"),
                             "PFR visitor starter rows have the identical one-linked-player "
                             "per-boxscore schema; home/visitor is the physical split"),
    "pfr_box_expected_points": (("box", "game", "boxscore_id", "REG"),
                                "PFR expected-points capture is a boxscore-keyed table; "
                                "the mapped game_date is a locator/context field"),
    "pfr_box_game_info": (("box", "game", "boxscore_id", "REG"),
                          "PFR game-info capture is keyed by boxscore_id and game date; "
                          "the mapped date is context, not a player aggregate"),
    "pfr_box_home_drives": (("box", "game", "boxscore_id", "REG"),
                            "PFR home-drive capture is keyed by boxscore_id and game "
                            "date; the mapped date is context"),
    "pfr_box_officials": (("box", "game", "boxscore_id", "REG"),
                          "PFR officials capture is keyed by boxscore_id and game date; "
                          "the mapped date is context"),
    "pfr_box_pbp": (("box", "game", "boxscore_id", "REG"),
                    "PFR play-by-play capture is keyed by boxscore_id and game date; "
                    "the mapped date is context"),
    "pfr_box_scoring": (("box", "game", "boxscore_id", "REG"),
                        "PFR scoring capture is keyed by boxscore_id and game date; "
                        "the mapped date is context"),
    "pfr_box_team_stats": (("box", "game", "boxscore_id", "REG"),
                           "PFR team-stat capture is keyed by boxscore_id and game date; "
                           "the mapped team/game field is context"),
    "pfr_box_vis_drives": (("box", "game", "boxscore_id", "REG"),
                           "PFR visitor-drive capture is keyed by boxscore_id and game "
                           "date; the mapped drive context is boxscore-grain"),
    "pfr_adj_passing": (("pages", "season", "team_name_abbr", "REG"),
                        "PFR adjusted-passing player pages carry pfr_id/year_id and a "
                        "team column; the year parser excludes Career rows"),
    "pfr_games_played_post": (("pages", "season", "team", "POST"),
                              "PFR playoff games-played player pages carry pfr_id/year_id "
                              "and team; the table is explicitly postseason"),
    "pfr_all_pro_members": (("pfr_award_pages", "season", "player_link_ids", "REG"),
                            "PFR All-Pro membership rows carry a scalar player_link_ids PFR "
                            "key, year, and season-stat columns; the award text is a sparse "
                            "player-season witness"),
    "pfr_pro_bowl_members": (("pfr_award_pages", "season", "player_link_ids", "REG"),
                             "PFR Pro Bowl membership rows carry a scalar player_link_ids "
                             "PFR key, year, and season-stat columns; the award is sparse "
                             "player-season evidence"),
    "pfr_combine": (("pfr_combine", "player_static", "pfr_id", "REG"),
                    "PFR combine rows carry the direct player-link id, combine year, and "
                    "physical measurements; the direct link id is the static-player key"),
    "player_bio": (("player_bio", "player_static", "pfr_id", "REG"),
                   "player_bio is the canonical static-player identity plane: pfr_id and "
                   "NFL_player_id crosswalk the row and remaining fields are native static "
                   "values, not season aggregates"),
    "pbp_team_defense": (("team_week", "week", "nfl_team", "REG"),
                         "canonical team-week PBP defense table with explicit team, year, "
                         "week, and season_type axes; values are already team-level"),
    # Our own retired supertable: one row per player-week, same column vocabulary family
    # as pbp_player_week_rollup which declares ('flat','season',...). Its crosswalk into
    # pfr space is the receipted bio one, already ACTIVE in kc_planes.
    "legacy_motherduck_supertable": (("flat", "season", "team_name_abbr", "REG"),
                                     "player-week rows keyed by NFL_player_id, crosswalked "
                                     "via the receipted bio crosswalk exactly as "
                                     "pbp_player_week_rollup is"),
}

# The static plane is a real target for bio/combine measurements, but it is not a mirror
# of every operational identifier in player_bio.  Keep those identifiers in the bio
# table/crosswalk lane instead of emitting MapSpecs that cannot name a canonical target.
PLAYER_STATIC_TARGET_COLUMNS = frozenset({
    "age_at_draft", "allpro", "bench", "birth_date", "birth_place", "broad_jump",
    "college", "cone", "conference", "dr_av", "draft_overall", "draft_round",
    "draft_year", "first_year", "forty", "headshot_url", "height", "high_school",
    "hof", "is_undrafted", "last_year", "nfl_draft_team", "nfl_position", "probowls",
    "ras_score", "rookie_year", "seasons_started", "shuttle", "vertical", "w_av",
    "weight", "years_active",
})

# Context fields are not aggregatable, but these six sources publish them natively on
# the same player-week key as the weekly plane.  They are eligible for `value`, never
# SUM/MAX; sources without NFL_player_id+year+week remain refused.
NATIVE_WEEKLY_CONTEXT = frozenset({
    "game_date", "nfl_position", "season_type", "nfl_team",
    "opponent_nfl_team", "player_week", "data_source", "position",
    "headshot_url", "fantasy_position", "starter_position",
})
NATIVE_WEEKLY_CONTEXT_SOURCES = frozenset({
    "ancient_pfa_gamelog", "ancient_pfr_recovery", "ancient_pbp1978_recovery",
    "ancient_newspaper_ocr", "pbp_player_week_rollup", "legacy_motherduck_supertable",
    "newspaper_player_notes", "nflcom_player_logs", "nflcom_player_logs_targeted",
})
NATIVE_SEASON_CONTEXT = frozenset({"position", "nfl_position"})
NATIVE_SEASON_CONTEXT_SOURCES = frozenset({
    "statscrew_team_season_roster", "pfr_adj_passing", "pfr_games_played",
    "pfr_games_played_post",
    # These PFR season/page families publish `pos` with >=99.5% agreement on the
    # 2025 overlap against the canonical nfl_position vocabulary.  They are native
    # context witnesses, never aggregations; lower-agreement awards/combine/snap
    # vocabularies remain fail-closed for an explicit taxonomy crosswalk.
    "pfr_adv_defense", "pfr_adv_defense_post", "pfr_adv_recrush", "pfr_adv_recrush_post",
    "pfr_adv_rushrec", "pfr_adv_rushrec_post", "pfr_kicking_post",
    "pfr_passing_adv_post", "pfr_passing_adv_season", "pfr_passing_post",
    "pfr_player_defense", "pfr_player_kicking", "pfr_player_punting",
    "pfr_player_returns", "pfr_player_scoring", "pfr_player_season_passing",
    "pfr_player_season_rec_rush", "pfr_player_season_rush_rec", "pfr_punting_post",
    "pfr_defense_post",
    "pfr_recrush_post", "pfr_returns_post", "pfr_scoring_post",
})

# These captures have valid team-game keys, but their dossier rows are not player
# observations. Keeping them out of the player MapSpec lane is a grain decision, not an
# unresolved identity crosswalk. A future team-game witness lane can license them without
# pretending they belong in weekly/season/career/player-bio player planes.
TEAM_GAME_ONLY_SOURCES = frozenset({
    "scoring_summary", "statscrew_team_season_results",
})
BOX_PLAYER_CONTEXT_SOURCES = frozenset({
    "pfr_box_defense_advanced", "pfr_box_home_snaps", "pfr_box_home_starters",
    "pfr_box_kicking", "pfr_box_passing_advanced", "pfr_box_receiving_advanced",
    "pfr_box_returns", "pfr_box_rushing_advanced", "pfr_box_vis_snaps",
    "pfr_box_vis_starters", "pfr_player_defense_box", "pfr_player_offense_box",
})
NFLCOM_PLAYER_WEEK_CONTEXT_SOURCES = frozenset({
    "nflcom_player_logs", "nflcom_player_logs_targeted",
})


#: TABLE-MAJOR SOURCES: the ones where (source_key, source_col) is not unique to a
#: statistic. Each entry declares how to reach the physical table, and every field is a
#: DECLARATION with evidence -- none of it is inferable from the parquet alone.
#:
#:   table_col       the physical column holding the table key
#:   table_key_part  which segment of the DOSSIER's table_key names that table. It is not
#:                   always the first: player_season keys read `field-goals|reg` (part 0),
#:                   but the layout-major sources read `Field Position|player_situational_L0`
#:                   where part 0 is the SPLIT DIMENSION and part 1 is the layout that
#:                   actually determines column identity.
#:   row_filter      for a split source, the PARTITION DIMENSION. Load-bearing: these
#:                   sources have no Total row, so summing across all dimensions multiplies
#:                   the value 5-6x. Which dimensions partition was MEASURED, not assumed --
#:                   see the reason string on each.
#:   slug_col        the key column (nflcom uses two different names for it)
TABLE_MAJOR: dict[str, dict] = {
    "nflcom_player_season": {
        "table_col": "_category", "table_key_part": 0, "slug_col": "_player_slug",
        "row_filter": "season_type='reg'",
        "why": "11 stat categories x {reg, post}, but reg and post are 100.00% identical on "
               "every joined (slug, season) pair in all 11 categories -- season_type carries "
               "no information here, so the post half is excluded rather than validated as "
               "postseason it is not",
    },
    "nflcom_player_career": {
        "table_col": "_table", "table_key_part": 0, "slug_col": "nflcom_slug",
        "row_filter": "",
        # Recent Games lost its explicit id_recent selector, but four blocks retain
        # mutually exclusive field signatures. The selectors below were measured on the
        # parquet: DEF=9,998 rows, K=599, P=530, QB=630, with zero row overlap. RBFB5 and
        # WRTE share the same physical columns with reversed semantics and remain held.
        "exclude_tables": {"Recent Games"},
        "signature_filters": {
            "Recent Games|id_recent:DEF_career": (
                "NULLIF(TRIM(CAST(tkl AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(ast AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(combined AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(solo AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(sfty AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(pdef AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(ff AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(opp_fr AS VARCHAR)), '') IS NOT NULL"),
            "Recent Games|id_recent:K": (
                "NULLIF(TRIM(CAST(fg_att AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(fgm AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(xp_att AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(xpm AS VARCHAR)), '') IS NOT NULL"),
            "Recent Games|id_recent:P": (
                "NULLIF(TRIM(CAST(punts AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(net_yds AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(net_avg AS VARCHAR)), '') IS NOT NULL"),
            "Recent Games|id_recent:QB": (
                "NULLIF(TRIM(CAST(comp AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(rate AS VARCHAR)), '') IS NOT NULL OR "
                "NULLIF(TRIM(CAST(scky AS VARCHAR)), '') IS NOT NULL"),
            # Recent Games was independently re-parsed into the retained local
            # recovery frame. Its explicit _rg_block resolves the two shared
            # REC/ATT/YDS layouts; `1=1` is intentional because the recovered
            # frame's block selector, not raw-column occupancy, is load-bearing.
            "Recent Games|id_recent:RBFB5": "1=1",
            "Recent Games|id_recent:WRTE": "1=1",
        },
        "signature_shapes": {
            "Recent Games|id_recent:RBFB5": "nflcom_week",
            "Recent Games|id_recent:WRTE": "nflcom_week",
        },
        "signature_grains": {
            "Recent Games|id_recent:RBFB5": "week",
            "Recent Games|id_recent:WRTE": "week",
        },
        "signature_validation_grains": {
            "Recent Games|id_recent:RBFB5": "week",
            "Recent Games|id_recent:WRTE": "week",
        },
        "signature_source_tables": {
            "Recent Games|id_recent:RBFB5": "RBFB",
            "Recent Games|id_recent:WRTE": "WRTE",
        },
        "signature_table_cols": {
            "Recent Games|id_recent:RBFB5": "_rg_block",
            "Recent Games|id_recent:WRTE": "_rg_block",
        },
        "why": "Career blocks retain _table. Recent Games DEF/K/P/QB are recovered by "
               "disjoint measured row signatures, while RBFB5/WRTE use the independently "
               "re-parsed retained recovery frame's explicit _rg_block selector",
    },
    "nflcom_player_logs": {
        "table_col": "_table", "table_key_part": 0, "slug_col": "nflcom_slug",
        "row_filter": "", "shape": "nflcom_log_week", "grain": "week",
        "validation_grain": "week",
        "exclude_tables": {"Regular Season", "Post Season", "Preseason"},
        # The retained parquet lost _layout, but five position blocks remain recoverable
        # from mutually exclusive physical signatures. These filters were measured over
        # all 1,549,161 rows: DEF=428,779, K=38,386, P=31,294, QB=51,344, OL=53,782,
        # with zero pairwise overlap. RBFB5 and WRTE deliberately remain held because
        # they share the same REC/ATT/YDS columns with reversed semantics.
        "signature_filters": {
            **{
                f"{phase}|id_gamelog:DEF_log": (
                    "NULLIF(TRIM(CAST(total AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(solo AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(ast AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(sfty AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(pdef AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(ff AS VARCHAR)), '') IS NOT NULL")
                for phase in ("Regular Season", "Post Season", "Preseason")
            },
            **{
                f"{phase}|id_gamelog:K_log": (
                    "NULLIF(TRIM(CAST(fg_att AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(fgm AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(xp_att AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(xpm AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(pct AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(pct_2 AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(blk_2 AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(ko AS VARCHAR)), '') IS NOT NULL")
                for phase in ("Regular Season", "Post Season", "Preseason")
            },
            **{
                f"{phase}|id_gamelog:P": (
                    "NULLIF(TRIM(CAST(punts AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(net_yds AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(net_avg AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(oob AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(dn AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(in_20 AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(fc AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(rety AS VARCHAR)), '') IS NOT NULL")
                for phase in ("Regular Season", "Post Season", "Preseason")
            },
            **{
                f"{phase}|id_gamelog:QB": (
                    "NULLIF(TRIM(CAST(comp AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(scky AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(rate AS VARCHAR)), '') IS NOT NULL")
                for phase in ("Regular Season", "Post Season", "Preseason")
            },
            **{
                f"{phase}|id_gamelog:OL": (
                    "NULLIF(TRIM(CAST(g AS VARCHAR)), '') IS NOT NULL OR "
                    "NULLIF(TRIM(CAST(gs AS VARCHAR)), '') IS NOT NULL")
                for phase in ("Regular Season", "Post Season", "Preseason")
            },
            **{
                f"{phase}|id_gamelog:{block}": f"_table='{phase}'"
                for phase in ("Regular Season", "Post Season", "Preseason")
                for block in ("RBFB5", "WRTE")
            },
        },
        # Complete 2025-only recapture with a real _layout discriminator. This
        # override is bounded to the recovered artifact; historical raw logs stay
        # preserved and fail closed when this artifact is not in scope.
        "signature_source_paths": {
            f"{phase}|id_gamelog:{block}": (
                "D:/league-history-data/nfl/derived/validation/sota_recon_master/"
                "nflcom_player_logs_2025_axis_recovered.parquet")
            for phase in ("Regular Season", "Post Season", "Preseason")
            for block in ("RBFB5", "WRTE")
        },
        "signature_source_tables": {
            f"{phase}|id_gamelog:{block}": f"id_gamelog:{block}"
            for phase in ("Regular Season", "Post Season", "Preseason")
            for block in ("RBFB5", "WRTE")
        },
        "signature_table_cols": {
            f"{phase}|id_gamelog:{block}": "_layout"
            for phase in ("Regular Season", "Post Season", "Preseason")
            for block in ("RBFB5", "WRTE")
        },
        "signature_validation_years": {
            f"{phase}|id_gamelog:{block}": (2025, 2025)
            for phase in ("Regular Season", "Post Season", "Preseason")
            for block in ("RBFB5", "WRTE")
        },
        "signature_season_types": {
            f"{phase}|id_gamelog:{block}": {
                "Regular Season": "REG", "Post Season": "POST", "Preseason": "PRE"
            }[phase]
            for phase in ("Regular Season", "Post Season", "Preseason")
            for block in ("RBFB5", "WRTE")
        },
        # The capture retained the phase label but lost the id_gamelog position-block
        # discriminator.  Register the physical phase axis so the refusal is truthfully
        # reported as TABLE_AXIS_LOST_AT_CAPTURE, not as an absent source shape.  No value
        # path is emitted until the missing block axis is recovered.
        "why": "raw logs retain _table phase and nflcom_slug. Five blocks are recovered "
               "by disjoint measured signatures (DEF/K/P/QB/OL); RBFB5 and WRTE remain "
               "held because their shared REC/ATT/YDS columns reverse meaning",
    },
    "statscrew_team_season_stats": {
        "table_col": "table_tag", "table_key_part": 0,
        "slug_col": "source_player_id", "row_filter": "is_total_row=false",
        "shape": "statscrew_team_season", "grain": "season",
        # The table axis is present and resolves the repeated short headers (`no`, `yds`,
        # `long`, etc.). source_player_id is now receipted into PFR space by the
        # fail-closed StatsCrew crosswalk; 690 source IDs remain unmatched/ambiguous and
        # therefore contribute no witness rows.
        "why": "StatsCrew preserves table_tag for eleven titled stat families; the same "
               "physical column is a different statistic only across that declared axis. "
               "source_player_id is joined through statscrew_player_pfr_crosswalk_v1; "
               "unmatched or ambiguous IDs are excluded",
    },
    "nflcom_player_situational": {
        "table_col": "_layout", "table_key_part": 1, "slug_col": "nflcom_slug",
        "row_filter": "_table='Home vs Road'",
        "why": "8 split dimensions x 8 layouts, but ZERO (layout, column) pairs disagree "
               "across dimensions, so identity is per LAYOUT. Home vs Road is partition-valid "
               "by measurement: it agrees 98.2-99.5% with Attempts, Stadium Surfaces and "
               "Field Position, while Quarters (~44%), Game Halves (~37%), Margin of Victory "
               "(49%) and Point Differential (18-28%) fail -- Point Differential last because "
               "`Behind` contains `Behind by 1-8` and `Behind by 9-16`, so summing double-counts",
    },
    "nflcom_player_splits": {
        "table_col": "_layout", "table_key_part": 1, "slug_col": "nflcom_slug",
        "row_filter": "_table='Months'",
        "why": "same layout-major structure. Days, Months, Opponents by Team, Outcomes and "
               "Stadiums mutually agree at 97.8-99.4% so each reconstructs the season total; "
               "`Opponents by Group` fails at ~36% because its buckets overlap. Months carries "
               "the highest mutual agreement (99.1% mean)",
    },
    "nflcom_player_logs_targeted": {
        "table_col": "_layout", "table_key_part": 1, "slug_col": "nflcom_slug",
        "row_filter": "", "shape": "nflcom_log_week", "grain": "week",
        "validation_grain": "week", "season_type_from_table_key_part": 0,
        "why": "targeted capture preserves the id_gamelog position block in _layout and "
               "the Regular/Post Season axis in _table; each block is addressable",
    },
    "nflcom_team_stats": {
        "table_col": "_category", "table_key_part": 0, "slug_col": "team",
        "row_filter": "", "shape": "nflcom_team_season", "grain": "season",
        "validation_grain": "season", "side_table_key_part": 1,
        "season_type_from_table_key_part": 2,
        "why": "team stats preserve category, side, season, and season_type; the "
               "receipted doubled-nickname plus season crosswalk resolves team to "
               "franchise space before comparison",
    },
}


def _kc_status() -> dict[str, str]:
    doc = json.loads(KC_PATH.read_text(encoding="utf-8"))
    return {x["source_id"]: x["k"].get("status") for x in doc["contracts"]}


def _agg_class() -> dict[str, str]:
    doc = json.loads(CONTRACTS_PATH.read_text(encoding="utf-8"))
    return {r["canonical_name"]: r["aggregation_class"] for r in doc["stats"]}


def _units() -> dict[str, str]:
    doc = json.loads(CONTRACTS_PATH.read_text(encoding="utf-8"))
    return {r["canonical_name"]: r.get("unit", "") for r in doc["stats"]}


def _natural_grains() -> dict[str, str]:
    doc = json.loads(CONTRACTS_PATH.read_text(encoding="utf-8"))
    return {r["canonical_name"]: r.get("natural_grain", "") for r in doc["stats"]}


#: Names that DENOTE a ratio whatever the contract claims about them. This list exists
#: because reading aggregation_class alone is not enough: `pat_pct` is declared
#: unit="ratio" AND aggregation_class="SUM" -- the row contradicts itself -- while
#: `rushing_yards_per_carry` is declared unit="count", so BOTH of its fields are wrong and
#: no amount of cross-checking the contract against itself would catch it. 22 rate-shaped
#: canonicals are classed SUM, and before this guard they produced 45 generated specs that
#: summed a rate across rows. Summing a rate yields a number that means nothing; the
#: measured symptom is a v26 median of 589 for a statistic bounded near 158.
RATE_SHAPED = re.compile(r"(_pct$|_rate$|_avg$|^avg_|_per_|_share$|rating|percentage)", re.I)


def rate_shaped_canonicals(agg_class: dict[str, str], units: dict[str, str]) -> set[str]:
    """Canonicals no spec may aggregate, by unit OR by name -- neither field is trusted alone."""
    return {name for name, klass in agg_class.items()
            if klass in AGG_FROM_CLASS
            and (units.get(name) == "ratio" or RATE_SHAPED.search(name))}


def native_value_canonicals() -> set[str]:
    """Canonicals that may be witnessed as a value at their declared native grain.

    A rate is not additive, but a source-published season/career rate is still a
    legitimate witness for the corresponding season/career cell. The same applies
    to counts whose derivation is event-cardinality: a native season/career
    ``games`` value is direct, while counting weekly rows is a different path.

    This set controls only ``MapSpec.agg='value'``. It does not license a source,
    resolve its key space, or invent its grain/shape; those gates remain separate.
    """
    classes = _agg_class()
    units = _units()
    grains = _natural_grains()
    return {
        name for name, klass in classes.items()
        if name in {"games_played", "games_started", "is_starter", "age"}
        or klass in {"RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE"}
        or units.get(name) == "ratio"
        or RATE_SHAPED.search(name)
        # FIRST/ANY and identity attributes are direct values when the contract
        # says their native axis is season/career/static. This is deliberately
        # grain-limited: player-game dimensions still need a weekly/table path.
        or klass in {"FIRST", "ANY"}
        or grains.get(name) in {"player_static", "player_season", "player_career"}
    }


def _declarations() -> tuple[dict[str, tuple], dict[str, str]]:
    """Per-source (shape, grain, team_col, season_type), and where it came from."""
    decl: dict[str, tuple] = {}
    why: dict[str, str] = {}
    seen: dict[str, set] = collections.defaultdict(set)
    # HAND specs only. Reading the merged map would let a generated spec supply the very
    # declaration that authorized it -- the shape would become self-justifying.
    for m in HAND_WITNESS_MAP:
        seen[m.source_key].add((m.shape, m.grain, m.team_col, m.season_type))
    for source, tuples in seen.items():
        # The retained PBP rollup is natively one row per player-week.  Its
        # early hand specs predate the weekly witness gate and used the
        # season validation default, which made every generated rollup spec
        # look season-only even though the source carries year/week keys.
        # Normalize the declaration at this boundary so generated specs use
        # the source's actual grain without changing the value path.
        if source == "pbp_player_week_rollup":
            tuples = {(shape, "week", team_col, season_type)
                      for shape, _grain, team_col, season_type in tuples}
        if len(tuples) != 1:
            # a source that declares two shapes cannot be fanned out safely
            continue
        decl[source] = next(iter(tuples))
        why[source] = "read off this source's existing hand MapSpecs"
    for source, (tup, reason) in NEW_SOURCE_SHAPES.items():
        if source in decl:      # a hand spec always wins over a declaration here
            continue
        decl[source] = tup
        why[source] = reason
    return decl, why


def table_dependent_identities(dossier: dict, decisions: dict) -> dict[tuple[str, str], set]:
    """(source, column) pairs whose CANONICAL depends on which table the column came from.

    MapSpec is keyed (source_key, source_col) and carries no table selector. On a
    single-table source that key is unique to a statistic. On NFL.com it is not:
    `nflcom_player_season.lng` is `fg_long` under `field-goals|reg`, `passing_long` under
    `passing|reg`, `punt_long` under `punts|reg` -- SEVEN canonicals behind one column
    name, discriminated only by the table key. 212 of our 309 NFL tables are NFL.com's,
    and they are the same ~50 columns re-emitted per category/split/season-type.

    Such a column cannot be expressed as a MapSpec AT ALL, so it is refused here, ahead of
    the shape check, for the reason that is actually true. This ordering is the point:
    these rows would otherwise be refused as "no shape declaration", which reads as
    "supply a declaration and they are unblocked" -- and supplying one would emit a single
    spec per (source, column), silently keeping ONE of the seven adjudications and
    unioning every category table into it.
    """
    canonical_of: dict[tuple[str, str], set] = collections.defaultdict(set)
    for row in dossier["rows"]:
        if row["source"] == "v26_release" or row["disposition"] != "MAPPED_TO_CANONICAL":
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        canonical = (decisions.get(key) or {}).get("canonical")
        if canonical:
            canonical_of[(row["source"], row["column"])].add(canonical)
    return {k: v for k, v in canonical_of.items() if len(v) > 1}


#: A dossier row may justify itself by citing an already-committed MapSpec rather than
#: re-arguing the concept ("Imported, not re-argued -- the decision predates this pass and
#: lives in witness_map"). That citation is only as good as the spec it names. When a spec
#: is later DELETED as wrong, the dossier row keeps citing it, and a generator that reads
#: identity from the dossier will faithfully resurrect the deleted mapping.
#:
#: This is exactly what happened to `pfr_player_kicking.fgm5 -> fg_made_50_59`. PFR's fgm5
#: is "50+", not "50--59", so a 60-yarder lands in the 50--59 bucket; the hand spec was
#: removed and a regression test pins it out. The dossier still records it as decided.
#:
#: The hand map's ABSENCES therefore carry knowledge, and `already_specced` only sees its
#: PRESENCES. A row whose stated authority no longer exists has no authority.
CITES_A_COMMITTED_MAPSPEC = "a committed MapSpec already decides this column"


def fossil_citations(dossier: dict, hand: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Rows justified by a MapSpec that has since been deleted -- the citation is void."""
    return {(row["source"], row["column"]) for row in dossier["rows"]
            if CITES_A_COMMITTED_MAPSPEC in (row.get("reason") or "")
            and (row["source"], row["column"]) not in hand}


def build() -> tuple[list[MapSpec], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    decisions = load_decisions()
    table_dependent = table_dependent_identities(dossier, decisions)
    fossils = fossil_citations(dossier, {(m.source_key, m.source_col)
                                         for m in HAND_WITNESS_MAP})
    kc = _kc_status()
    agg_class = _agg_class()
    rate_shaped = rate_shaped_canonicals(agg_class, _units())
    native_values = native_value_canonicals()
    decl, why = _declarations()
    # HAND specs only, so the generated block is rebuilt WHOLE on every run. If the
    # already-written generated specs counted as "existing", a dossier decision that later
    # changed its canonical would leave the stale spec in place and emit nothing.
    existing = {(m.source_key, m.source_table, m.source_col)
                for m in HAND_WITNESS_MAP}

    specs: list[MapSpec] = []
    tally = collections.Counter()
    refused: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)

    for row in dossier["rows"]:
        source, column = row["source"], row["column"]
        if source == "v26_release" or row["disposition"] != "MAPPED_TO_CANONICAL":
            continue
        # A table-major hand spec is identified by its physical table selector,
        # not by the dossier's complete table_key. Resolve that selector before
        # deciding whether the generated row would overwrite a hand path.
        existing_table = ""
        if source in TABLE_MAJOR:
            axis = TABLE_MAJOR[source]
            parts = (row["table_key"] or "").split("|")
            if len(parts) > axis["table_key_part"]:
                existing_table = parts[axis["table_key_part"]]
        if (source, existing_table, column) in existing:
            tally["already_specced"] += 1
            continue
        if (source, column) in fossils:
            tally["refused_fossil_citation"] += 1
            refused["justified by a MapSpec that has since been deleted"][source] += 1
            continue
        if (source, column) in table_dependent and source not in TABLE_MAJOR:
            # ahead of the shape check ON PURPOSE -- see table_dependent_identities().
            # Sources in TABLE_MAJOR are now addressable: MapSpec carries a table selector,
            # so they are fanned out per table below instead of refused.
            tally["refused_table_dependent_identity"] += 1
            refused["identity depends on the table, and the source declares no table axis"][source] += 1
            continue
        if source in TABLE_MAJOR:
            axis = TABLE_MAJOR[source]
            parts = (row["table_key"] or "").split("|")
            signature_filter = axis.get("signature_filters", {}).get(row["table_key"] or "")
            if (len(parts) > axis["table_key_part"] and
                    parts[axis["table_key_part"]] in axis.get("exclude_tables", ()) and
                    not signature_filter):
                # The captured partition is not addressable. This is load-bearing even
                # for NON_AGGREGATABLE context columns: report the lost physical axis,
                # not the downstream aggregation symptom.
                tally["refused_table_axis_lost_at_capture"] += 1
                refused["table exists in the dossier but not in the parquet"][source] += 1
                continue
        if source in TEAM_GAME_ONLY_SOURCES or source in {
            "pfr_team_games", "schedule_master", "newspaper_game_context",
            "pfr_box_expected_points", "pfr_box_game_info", "pfr_box_home_drives",
            "pfr_box_officials", "pfr_box_pbp", "pfr_box_scoring",
            "pfr_box_team_stats", "pfr_box_vis_drives",
        }:
            # These sources resolve to team-game grain, not player grain. Their keys are
            # valid for a separate team-game lane, so do not mislabel them as unresolved
            # player key space.
            tally["refused_subject_grain"] += 1
            refused["source is team-game grain, not a player MapSpec"][source] += 1
            continue
        if kc.get(source) not in {"ACTIVE", "AUTHORITY"}:
            tally["refused_key_space"] += 1
            refused["key space unresolved"][source] += 1
            continue
        if source not in decl and source not in TABLE_MAJOR:
            tally["refused_no_shape_declaration"] += 1
            refused["no shape declaration"][source] += 1
            continue
        decision = decisions.get(row_key(source, row["table_key"], column)) or {}
        canonical = decision.get("canonical")
        if not canonical:
            if row.get("canonical") and decision.get("disposition") == "EXCLUDED_WITH_REASON":
                tally["refused_canonical_adjudication"] += 1
                refused["canonical mapping rejected by adjudication"][source] += 1
            elif decision.get("disposition") == "NEW_SUPERTABLE_COLUMN_CANDIDATE":
                # Preserve a real schema opportunity as a candidate. It is not a
                # failed mapping: there is deliberately no existing canonical target
                # to which a MapSpec could point.
                tally["refused_new_column_candidate"] += 1
                refused["new supertable column candidate"][source] += 1
            else:
                tally["refused_no_canonical"] += 1
                refused["no canonical target"][source] += 1
            continue
        if source == "player_bio" and canonical not in PLAYER_STATIC_TARGET_COLUMNS:
            # The dossier correctly records these fields as bio atoms, but the current
            # super-table career/static plane has no corresponding canonical column.
            # That is a target adjudication, not a shape defect.
            tally["refused_player_bio_only"] += 1
            refused["player-bio-only field (not a supertable target)"][canonical] += 1
            continue
        if (canonical in {"nfl_position", "position"} and not (
                canonical in native_values
                or (canonical in NATIVE_WEEKLY_CONTEXT
                    and source in NATIVE_WEEKLY_CONTEXT_SOURCES)
                or (canonical in NATIVE_SEASON_CONTEXT
                    and source in NATIVE_SEASON_CONTEXT_SOURCES))):
            # PFR award/page `pos`, newspaper listed/starter labels, and bio position
            # values use different role/taxonomy vocabularies (direct validation found
            # only 54.13% agreement for player_bio.nfl_position). They are native context
            # values, but not safely comparable canonical position values without an
            # explicit crosswalk; do not misclassify this as an aggregation failure.
            tally["refused_context_taxonomy"] += 1
            refused["canonical position taxonomy unresolved"][source] += 1
            continue
        klass = agg_class.get(canonical)
        if (canonical in native_values
            or (canonical in NATIVE_WEEKLY_CONTEXT
                and source in NATIVE_WEEKLY_CONTEXT_SOURCES)
            or (canonical in NATIVE_SEASON_CONTEXT
                and source in NATIVE_SEASON_CONTEXT_SOURCES)
            or (source == "player_bio" and canonical == "headshot_url")
            or (source == "newspaper_lineups"
                and canonical in {"nfl_team", "opponent_nfl_team", "player_week",
                                  "starter_position"})
            or (canonical == "game_date" and source in BOX_PLAYER_CONTEXT_SOURCES)
            or source == "pbp_team_defense"):
            # Native season/career (or weekly) values are witnesses. They are not
            # being reconstructed from lower-grain rows, so RECOMPUTE_RATE,
            # WEIGHTED_RECOMPUTE, FIRST, and EVENT_CARDINALITY remain valid source
            # identities when the source declaration supplies that native grain.
            agg = "value"
            tally["emitted_native_value"] += 1
        else:
            agg = AGG_FROM_CLASS.get(klass)
        if agg is None:
            tally["refused_aggregation_class"] += 1
            refused[f"aggregation_class={klass}"][canonical] += 1
            continue
        if source in TABLE_MAJOR:
            axis = TABLE_MAJOR[source]
            parts = (row["table_key"] or "").split("|")
            if len(parts) <= axis["table_key_part"]:
                tally["refused_unparseable_table_key"] += 1
                continue
            table = parts[axis["table_key_part"]]
            signature_filter = axis.get("signature_filters", {}).get(row["table_key"] or "")
            if table in axis.get("exclude_tables", ()) and not signature_filter:
                tally["refused_table_axis_lost_at_capture"] += 1
                refused["table exists in the dossier but not in the parquet"][source] += 1
                continue
            signature_key = row["table_key"] or ""
            shape = axis.get("signature_shapes", {}).get(signature_key,
                                                         axis.get("shape", "nflcom"))
            grain = axis.get("signature_grains", {}).get(signature_key,
                                                          axis.get("grain", "season"))
            season_type = axis.get("signature_season_types", {}).get(signature_key, "REG")
            row_filter = signature_filter or axis["row_filter"]
            source_table = axis.get("signature_source_tables", {}).get(signature_key, table)
            table_col = axis.get("signature_table_cols", {}).get(
                signature_key, axis["table_col"])
            source_path = axis.get("signature_source_paths", {}).get(signature_key, "")
            validation_tolerance = (
                0.055 if source_path and canonical in {
                    "rushing_yards_per_carry",
                    "receiving_yards_per_reception",
                } else None)
            phase_part = axis.get("season_type_from_table_key_part")
            if phase_part is not None and len(parts) > phase_part:
                phase = parts[phase_part]
                season_type = {"Regular Season": "REG", "Post Season": "POST",
                               "Preseason": "PRE"}.get(phase, phase.upper())
                if "side_table_key_part" in axis:
                    side = parts[axis["side_table_key_part"]]
                    row_filter = f"_side='{side}' AND season_type='{phase.lower()}'"
                else:
                    row_filter = f"_table='{phase}'"
            specs.append(MapSpec(
                source_key=source, v26_col=canonical, source_col=column,
                shape=shape, agg=agg, grain=grain,
                team_col=axis["slug_col"], season_type=season_type,
                source_table=source_table, table_col=table_col,
                row_filter=row_filter,
                validation_grain=axis.get("signature_validation_grains", {}).get(
                    signature_key, axis.get("validation_grain", "season")),
                validation_years=axis.get("signature_validation_years", {}).get(
                    signature_key),
                source_path=source_path,
                validation_tolerance=validation_tolerance,
                quirks=f"GENERATED by {GENERATOR}: identity from the column dossier "
                       f"({row['table_key']}), aggregation_class={klass} from "
                       f"stat_contracts. Table selector {table_col}={source_table!r}; "
                       f"axis declared because {axis['why']}"))
            tally["emitted_table_scoped"] += 1
            continue
        shape, grain, team_col, season_type = decl[source]
        source_col = column
        filters = ""
        source_table = ""
        table_col = ""
        if source in {"newspaper_player_cells", "newspaper_team_claims"}:
            source_col = "stat_value"
            # Use the existing table selector fields to keep one value path per
            # controlled stat_name; the long table has no physical wide column
            # for each canonical.
            source_table = column
            table_col = "stat_name"
        elif source == "newspaper_team_stats":
            source_col = "team_1_value"
            source_table = column
            table_col = "stat_name"
        specs.append(MapSpec(
            source_key=source, v26_col=canonical, source_col=source_col,
            shape=shape, agg=agg, grain=grain, team_col=team_col,
            season_type=season_type, filters=filters,
            source_table=source_table, table_col=table_col,
            validation_grain="week" if source in {"newspaper_player_cells", "newspaper_team_claims",
                                                  "newspaper_team_stats", "newspaper_lineups",
                                                  "pbp_team_defense", "pbp_player_week_rollup"}
            or (canonical == "game_date" and source in BOX_PLAYER_CONTEXT_SOURCES)
            or (canonical in NATIVE_WEEKLY_CONTEXT
                and source in NATIVE_WEEKLY_CONTEXT_SOURCES)
            else "season",
            quirks=f"GENERATED by {GENERATOR}: identity from the column dossier "
                   f"({row['table_key'] or '*'}), aggregation_class={klass} from "
                   f"stat_contracts, shape {why[source]}"))
        tally["emitted"] += 1

    # one source column can be adjudicated under several table keys (nflcom captions);
    # the MapSpec is per (source, column), so collapse and surface any disagreement
    # THE KEY INCLUDES THE TABLE. Collapsing on (source, column) alone is exactly the
    # defect this fan-out exists to fix: it would keep one of `lng`'s seven canonicals.
    # The physical table selector can have a partition predicate as a second
    # axis. NFL.com team_stats uses (category, side, season_type), so category
    # alone is not a complete value-path key.
    by_key: dict[tuple[str, str, str, str, str], MapSpec] = {}
    conflicts: list[str] = []
    for s in specs:
        k = (s.source_key, s.source_table, s.table_col, s.row_filter, s.source_col)
        prior = by_key.get(k)
        if prior and (prior.v26_col, prior.agg) != (s.v26_col, s.agg):
            conflicts.append(f"{s.source_key}[{s.source_table};{s.row_filter}].{s.source_col}: "
                             f"{prior.v26_col}/{prior.agg} vs {s.v26_col}/{s.agg}")
        by_key[k] = s
    return sorted(by_key.values(),
                  key=lambda m: (m.source_key, m.source_table, m.table_col,
                                  m.row_filter, m.source_col)), {
        "tally": dict(tally),
        "conflicts": sorted(set(conflicts)),
        # Keep the complete refusal ledger. Truncating to eight source keys made the
        # visible counts disagree with tally whenever a ninth source was held for the
        # same reason, which is unacceptable for an audit receipt.
        "refused": {k: dict(v) for k, v in refused.items()},
    }


# The fields the generator is entitled to set. Everything else on MapSpec (scale, filters,
# v26_expr, blank_zero, validation_*) is a JUDGEMENT about the source that this module has
# no evidence for, so it is left at its dataclass default rather than written out. A
# generated spec that needs one of them is a spec that needs a person.
EMITTED_FIELDS = ("source_key", "v26_col", "source_col", "shape", "agg",
                  "grain", "team_col", "season_type", "source_table",
                  "table_col", "row_filter", "validation_grain", "validation_years",
                  "source_path", "validation_tolerance", "quirks")

MODULE_HEADER = '''"""GENERATED by {generator} -- do not edit by hand.

Regenerate with::

    python -m scripts.sota_recon.mapspec_generator --apply

Every spec below is a VALUE PATH, not a vote. It says "this source column can be pulled
through to this canonical at this aggregation"; it does not say the numbers agree. Nothing
here may vouch for anything until `witness_map.validate()` has measured it and written the
verdict into MAPPING_LICENSES.json.

Provenance of the three mechanical fields, per spec, in `quirks`:
  * the (source_col -> v26_col) pair is the column dossier's receipted
    MAPPED_TO_CANONICAL decision -- it is not invented here;
  * `agg` is stat_contracts.aggregation_class, never the column name;
  * `shape`/`grain`/`team_col`/`season_type` are DECLARED per source, either read off that
    source's hand-written MapSpecs or declared with evidence in NEW_SOURCE_SHAPES.

Counts at generation time: {counts}
"""
from __future__ import annotations

GENERATED_SPECS: list[dict] = [
'''


def render(specs: list[MapSpec], report: dict) -> str:
    counts = ", ".join(f"{k}={v:,}" for k, v in sorted(report["tally"].items()))
    body = []
    for spec in specs:
        fields = ", ".join(f"{name}={getattr(spec, name)!r}" for name in EMITTED_FIELDS)
        body.append(f"    dict({fields}),")
    return MODULE_HEADER.format(generator=GENERATOR, counts=counts) + "\n".join(body) + "\n]\n"


def apply(specs: list[MapSpec], report: dict) -> str:
    """Write the generated block, refusing on anything the generator could not settle."""
    if report["conflicts"]:
        raise SystemExit(
            f"REFUSING to write: {len(report['conflicts'])} source columns are adjudicated "
            "two ways. A collapsed conflict is a silently wrong value path -- resolve the "
            "dossier decision first.")
    OUT_PATH.write_text(render(specs, report), encoding="utf-8", newline="\n")
    return str(OUT_PATH)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    specs, report = build()
    print(f"generated MapSpecs: {len(specs):,}")
    for k, v in sorted(report["tally"].items()):
        print(f"   {k:34s} {v:6,}")
    if report["conflicts"]:
        print("\nCONFLICTS -- one source column adjudicated two ways, not written:")
        for c in report["conflicts"][:20]:
            print("   -", c)
    print("\nrefused, by reason:")
    for reason, who in sorted(report["refused"].items()):
        print(f"   {reason}")
        for name, n in who.items():
            print(f"      {name[:44]:44s} {n:5,}")
    by_source = collections.Counter(s.source_key for s in specs)
    print("\nemitted per source:")
    for s, n in by_source.most_common():
        print(f"   {s[:40]:40s} {n:5,}")
    if not args.apply:
        print("\n(dry run -- pass --apply to write the generated spec module)")
        return 0
    path = apply(specs, report)
    print(f"\nwrote {len(specs):,} generated value paths -> {path}")
    print("   these are UNVALIDATED. Run `python -m scripts.sota_recon.witness_map` to "
          "measure them before any lane may read them as a vote.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
