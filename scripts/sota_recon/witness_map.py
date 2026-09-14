"""
sota_recon/witness_map.py  --  the executable WITNESS MAP: exactly how each witness vouches.

Answers "for each witness, do we know exactly how it maps/aggregates onto our super-table
columns?" with a declarative registry instead of SQL scattered through lanes. One MapSpec
per (source, v26_column):

    shape   how the source is keyed/parsed:
              pages : PFR player-page season tables (pfr_id, year_id regex, exclude nTM
                      combined rows, MAX-per-year dedup then SUM across stints)
              box   : PFR per-game boxscore tables (player_link_ids unnest, year from
                      boxscore_id, REG filter via nfl_team_games_all)
              flat  : direct columns (rollup / NGS: id + year (+week), season_type filter)
    agg     how source rows aggregate to OUR grain (sum / max / value)
    scale   unit conversion INTO v26's unit (units are per-column, never assumed)
    filters extra SQL predicates (quirks live here, e.g. pbp "(no play)" exclusion)

Every spec is VALIDATED before it may vouch in the legacy compatibility report: measured agreement vs v26 on a modern
known-good stratum. A wrong mapping (wrong column / unit / missing filter) shows up as
systematic disagreement -> the MAPPING is flagged SUSPECT. Promotion is now controlled
only by typed witness_gate admissibility contracts; this module cannot authorize it.

    python -m scripts.sota_recon.witness_map            # validate all mappings
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from . import sources as S
from .pbp_taxonomy import fumble_mentions_sql, official_play_sql

BOX_PLAYER_CONTEXT_SOURCES = frozenset({
    "pfr_box_defense_advanced", "pfr_box_home_snaps", "pfr_box_home_starters",
    "pfr_box_kicking", "pfr_box_passing_advanced", "pfr_box_receiving_advanced",
    "pfr_box_returns", "pfr_box_rushing_advanced", "pfr_box_vis_snaps",
    "pfr_box_vis_starters", "pfr_player_defense_box", "pfr_player_offense_box",
})
NFLCOM_PLAYER_WEEK_CONTEXT_SOURCES = frozenset({"nflcom_player_logs_targeted"})

VALIDATION_YEARS = (2005, 2019)   # known-good modern stratum for mapping validation
VALIDATION_MIN_AGREE = 0.95       # below this, the MAPPING is suspect (not the data)
DEPRECATED_FOR_PROMOTION = True


@dataclass(frozen=True)
class MapSpec:
    source_key: str          # sources.py registry key
    v26_col: str             # our super-table column
    source_col: str          # the witness's column
    shape: str               # pages | box | flat
    agg: str = "sum"         # sum | max | value
    # THE TWO SIDES CAN NEED DIFFERENT AGGREGATES. `agg` drives both, which is right almost
    # everywhere -- but not for a SEASON-GRAIN canonical read off a PER-TEAM source page.
    # games_played lives only at season/career grain, so the v26 side must read the STORED
    # season value (agg="value"), while the source side must SUM a traded player's team rows:
    # nflcom's career pages carry one row per team, and ANY_VALUE would silently take one
    # team's games as the season total. Leave None to keep `agg` governing both.
    witness_agg: str | None = None
    scale: float = 1.0       # witness value * scale == v26 unit
    filters: str = ""        # extra SQL predicate on source rows
    grain: str = "season"    # grain at which this mapping vouches
    quirks: str = ""         # documented oddities
    team_col: str = "team_name_abbr"  # pages shape: column holding team abbrev (games uses 'team')
    v26_expr: str = ""       # override v26-side aggregate (e.g. COUNT(*) for games)
    blank_zero: bool = False # box shape: blank cell on an EXISTING row counts as 0 (PFR
                             # renders 0 as '' in some box columns; measured per spec,
                             # never assumed -- a row must exist for the 0 to vouch)
    season_type: str = "REG" # which v26 stratum this mapping vouches for (the POST pages
                             # tables carry playoff season totals; votes stay REG-only)
    validation_years: tuple | None = None  # override the default known-good stratum
                             # (PFA has no modern overlap; advanced tables start 2018;
                             # 2025 excluded for pbp-derived cols BY MEASUREMENT --
                             # wk14+ air/EPA empty pending re-aggregation)
    validation_referee: str = ""  # source_key of a LICENSED pages spec for the same
                             # v26_col: validate against IT instead of v26 (the
                             # cross-era design -- PFA graded by pages on 1932-75)
    validation_grain: str = "season"  # "week": compare weekly values directly (NGS
                             # weekly raw; load-fidelity check at the native grain)
    validation_tolerance: float | None = None  # bounded source-display precision override
    source_table: str = ""   # WHICH TABLE inside a multi-table source this column came
                             # from. Blank for a single-table source. This field exists
                             # because (source_key, source_col) is NOT unique to a
                             # statistic on NFL.com: `nflcom_player_season.lng` is fg_long
                             # under field-goals, passing_long under passing, punt_long
                             # under punts -- SEVEN canonicals behind one column name,
                             # separated only by the table. 212 of our 309 NFL tables are
                             # NFL.com's, so without this field 746 adjudicated columns
                             # could not be expressed as specs at all.
    table_col: str = ""      # the PHYSICAL column holding that table key (_category,
                             # _layout, _table). Declared per source, never guessed: the
                             # dossier's table_key and the parquet's discriminator are
                             # different vocabularies on several sources.
    source_path: str = ""    # optional bounded recovered surface; raw registry lineage
                             # remains untouched.
    row_filter: str = ""     # a source-level predicate applied before aggregation. For
                             # split sources this is the PARTITION DIMENSION, and it is
                             # load-bearing: a split source has no Total row, so summing
                             # across every dimension multiplies the value 5-6x.
    source_expr: str = ""    # SQL expression over s.* replacing the raw column read.
                             # THE COMPOSITE-CELL LANE (2026-08-01): nflcom field-goal
                             # buckets publish one cell "made/attempted" ("3/4", named
                             # backwards a_m; order MEASURED -- first<second on all
                             # 2,038 unequal cells, so first=made). Two extractions
                             # from one cell are two DISTINCT value paths; source_col
                             # stays the physical column for reach/receipts.


HAND_WITNESS_MAP: list[MapSpec] = [
    # ---- PFR player pages (season grain) ----
    MapSpec("pfr_player_season_passing", "passing_yards", "pass_yds", "pages"),
    MapSpec("pfr_player_season_passing", "passing_tds", "pass_td", "pages"),
    MapSpec("pfr_player_season_passing", "passing_interceptions", "pass_int", "pages"),
    MapSpec("pfr_player_season_passing", "completions", "pass_cmp", "pages"),
    MapSpec("pfr_player_season_passing", "attempts", "pass_att", "pages"),
    MapSpec("pfr_player_season_rush_rec", "rushing_yards", "rush_yds", "pages"),
    MapSpec("pfr_player_season_rush_rec", "receiving_yards", "rec_yds", "pages"),
    MapSpec("pfr_player_season_rec_rush", "rushing_yards", "rush_yds", "pages"),
    MapSpec("pfr_player_season_rec_rush", "receiving_yards", "rec_yds", "pages"),
    MapSpec("pfr_player_kicking", "fg_made", "fgm", "pages"),
    MapSpec("pfr_player_kicking", "fg_att", "fga", "pages"),
    MapSpec("pfr_player_kicking", "pat_made", "xpm", "pages"),
    MapSpec("pfr_player_kicking", "fg_long", "fg_long", "pages", agg="max",
            quirks="season long = MAX of weekly longs, never SUM (the fg_long=109 class)"),
    MapSpec("pfr_player_defense", "def_interceptions", "def_int", "pages"),
    MapSpec("pfr_player_defense", "def_sacks", "sacks", "pages",
            quirks="half-sacks + official-vs-stathead noise ~2-3% (asymmetry registry)"),
    # ---- Phase-2 expansion: passing pages ----
    MapSpec("pfr_player_season_passing", "sacks_suffered", "pass_sacked", "pages",
            quirks="PFR sacked tracked 1969+ (blank before; pages NULL-sum abstains)"),
    MapSpec("pfr_player_season_passing", "sack_yards_lost", "pass_sacked_yds", "pages"),
    # ---- Phase-2 expansion: rush/rec pages (both table variants) ----
    MapSpec("pfr_player_season_rush_rec", "carries", "rush_att", "pages"),
    MapSpec("pfr_player_season_rush_rec", "rushing_tds", "rush_td", "pages"),
    MapSpec("pfr_player_season_rush_rec", "receptions", "rec", "pages"),
    MapSpec("pfr_player_season_rush_rec", "targets", "targets", "pages",
            quirks="targets tracked 1992+ on PFR pages"),
    MapSpec("pfr_player_season_rush_rec", "receiving_tds", "rec_td", "pages"),
    MapSpec("pfr_player_season_rush_rec", "fumbles", "fumbles", "pages",
            quirks="PFR pages fumbles = ALL fumbles (not just lost); semantics measured"),
    MapSpec("pfr_player_season_rec_rush", "carries", "rush_att", "pages"),
    MapSpec("pfr_player_season_rec_rush", "rushing_tds", "rush_td", "pages"),
    MapSpec("pfr_player_season_rec_rush", "receptions", "rec", "pages"),
    MapSpec("pfr_player_season_rec_rush", "targets", "targets", "pages"),
    MapSpec("pfr_player_season_rec_rush", "receiving_tds", "rec_td", "pages"),
    MapSpec("pfr_player_season_rec_rush", "fumbles", "fumbles", "pages"),
    # ---- Phase-2 expansion: kicking pages (incl. distance buckets) ----
    MapSpec("pfr_player_kicking", "pat_att", "xpa", "pages"),
    MapSpec("pfr_player_kicking", "fg_made_0_19", "fgm1", "pages"),
    MapSpec("pfr_player_kicking", "fg_made_20_29", "fgm2", "pages"),
    MapSpec("pfr_player_kicking", "fg_made_30_39", "fgm3", "pages"),
    MapSpec("pfr_player_kicking", "fg_made_40_49", "fgm4", "pages"),
    # ---- 2026-08-01 rate/derived wave (Joe: "Rates are still witness sources") ----
    # Season-grain rates read the STORED season value (agg="value"); the season plane
    # currently SUMS weekly rates (completion_pct median 343 in 2025), so these specs
    # are expected to CONVICT the season builder, not to validate. Units checked
    # per-column: PFR percents are 0-100 like our weekly plane, except pat_pct which
    # we store 0-1 (scale=0.01). pass/rush/rec_success NOT mapped: PFR publishes a
    # RATE, we store a COUNT -- different aggregation class (equation lane).
    MapSpec("pfr_player_season_passing", "completion_pct", "pass_cmp_pct", "pages",
            agg="value", validation_tolerance=0.06),
    # passing_{,adjusted_,net_,adjusted_net_}yards_per_attempt NOT specced here: the
    # generator already licenses pass_*_per_att to their TWIN canonicals
    # ({adjusted_,net_}yards_per_attempt -- sparse, qualified-passers, CORRECT ~6.4)
    # and one value path may vouch for one canonical only. The dense passing_* twins
    # are broken weekly-rate SUMS (median 34.7 in 2025); twin consolidation is a
    # vocabulary adjudication queued for Joe, after which the licence follows the
    # surviving column.
    MapSpec("pfr_player_season_rush_rec", "rushing_yards_per_carry",
            "rush_yds_per_att", "pages", agg="value", validation_tolerance=0.06),
    MapSpec("pfr_player_season_rec_rush", "rushing_yards_per_carry",
            "rush_yds_per_att", "pages", agg="value", validation_tolerance=0.06),
    MapSpec("pfr_player_season_rush_rec", "receiving_yards_per_reception",
            "rec_yds_per_rec", "pages", agg="value", validation_tolerance=0.06),
    MapSpec("pfr_player_season_rec_rush", "receiving_yards_per_reception",
            "rec_yds_per_rec", "pages", agg="value", validation_tolerance=0.06),
    MapSpec("pfr_player_season_rush_rec", "catch_pct", "catch_pct", "pages",
            agg="value", validation_tolerance=0.06,
            quirks="PFR catch% tracked with targets (1992+)"),
    MapSpec("pfr_player_season_rec_rush", "catch_pct", "catch_pct", "pages",
            agg="value", validation_tolerance=0.06),
    MapSpec("pfr_player_season_rush_rec", "yards_per_touch", "yds_per_touch", "pages",
            agg="value", validation_tolerance=0.06),
    MapSpec("pfr_player_season_rec_rush", "yards_per_touch", "yds_per_touch", "pages",
            agg="value", validation_tolerance=0.06),
    # total_touches NOT specced: measured 100% identical to `touches` on both planes
    # (2025), and the generator already licenses touches -> touches. Exact-duplicate
    # column; dedup adjudication queued.
    MapSpec("pfr_player_scoring", "total_tds_scored", "total_td", "pages",
            # SOURCE-SIDE NAME (2026-08-04): PFR's scoring table column is
            # literally "total_td" -- the repo rename swept source_col too,
            # and the scoped gate caught the lane hunting a column that does
            # not exist upstream. Their names are theirs; only OUR side renames.
            quirks="PFR scoring-page All TD = rush+rec+returns+fumble+int+other"),
    MapSpec("pfr_player_kicking", "pat_pct", "xp_pct", "pages", agg="value",
            scale=0.01, validation_tolerance=0.001,
            quirks="we store 0-1, PFR publishes 0-100"),
    MapSpec("pfr_adv_recrush", "receiving_adot", "rec_adot", "pages", agg="value",
            validation_tolerance=0.06,
            quirks="advanced tables 2018+; PFR charting vs our PBP-derived ADOT "
                   "disagrees ~30% at 0.06 tol with MATCHING medians -- method "
                   "asymmetry queued for adjudication, not a mapping defect"),
    MapSpec("pfr_adv_rushrec", "receiving_adot", "rec_adot", "pages", agg="value",
            validation_tolerance=0.06),
    # rec_air_yds -> receiving_air_yards REMOVED 2026-08-01: PFR counts air yards on
    # RECEPTIONS, our canonical counts air yards on ALL TARGETS (2025 medians 96 vs
    # 210) -- same name, different concept; the audit caught it before it licensed.
    # ---- 2026-08-01 NFL.com wave (formats verified: cmp_2=70.6 pct, yds_att=7.5,
    # rec_yac_r=787 on 127 rec = TOTAL yac despite the _r suffix) ----
    MapSpec("nflcom_player_season", "completion_pct", "cmp_2", "nflcom", agg="value",
            grain="season", team_col="_player_slug", source_table="passing",
            table_col="_category", row_filter="season_type='reg'",
            validation_tolerance=0.06,
            quirks="second lineage root against the summed-rate season defect"),
    MapSpec("nflcom_player_season", "yards_per_attempt", "yds_att", "nflcom",
            agg="value", grain="season", team_col="_player_slug",
            source_table="passing", table_col="_category",
            row_filter="season_type='reg'", validation_tolerance=0.06),
    # ---- 2026-08-05 NFL.com rushing witness wave. ----
    MapSpec("nflcom_player_season", "carries", "att", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'"),
    MapSpec("nflcom_player_season", "rushing_yards", "rush_yds", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'"),
    MapSpec("nflcom_player_season", "rushing_tds", "td", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'"),
    MapSpec("nflcom_player_season", "rushing_long", "lng", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'", agg="max"),
    MapSpec("nflcom_player_season", "rushing_first_downs", "rush_1st", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'"),
    MapSpec("nflcom_player_season", "rushing_fumbles", "rush_fum", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'",
            quirks="NFL.com publishes rushing fumbles, not rushing fumbles lost"),
    MapSpec("nflcom_player_season", "rushing_yards_per_carry", "rush_yds", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'",
            source_expr="TRY_CAST(s.rush_yds AS DOUBLE) / NULLIF(TRY_CAST(s.att AS DOUBLE), 0)",
            agg="value", v26_expr="SUM(TRY_CAST(t.rushing_yards AS DOUBLE)) / NULLIF(SUM(TRY_CAST(t.carries AS DOUBLE)), 0)",
            validation_tolerance=0.06),
    MapSpec("nflcom_player_season", "rush_explosive_10", "20", "nflcom",
            grain="season", team_col="_player_slug", source_table="rushing",
            table_col="_category", row_filter="season_type='reg'",
            quirks="legacy canonical name; locked definition is 20+ yards, matching NFL.com 20 bucket"),
    MapSpec("nflcom_player_season", "receiving_yards_after_catch", "rec_yac_r",
            "nflcom", grain="season", team_col="_player_slug",
            source_table="receiving", table_col="_category",
            row_filter="season_type='reg'",
            quirks="_r suffix is a lie: measured TOTAL yac, not per-reception. "
                   "VALIDATED 96% at 2023; 2025 reads low = the known stale "
                   "wk14-18 pbp stratum (second receipt); 2019 shows ~3% "
                   "systematic charting offset (era asymmetry, adjudication)"),
    # (wave 3a withdrawn before commit: passing_int_pct / passing_td_pct turned out
    # already generated from pfr_player_season_passing AND pfr_passing_post -- the
    # list of unwitnessed columns was stale, not the map.)
    # ---- 2026-08-01 wave 3b: nflcom situational splits. The situational surface has
    # NO Total row; 'Home Games' + 'Road Games' is the complementary partition for a
    # season total, and the field-position dimension is Own 1-20 | Own 21-50 |
    # Opp 49-20 | Opp 19-1. SEMANTIC PIN: 'Opp 19-1' EXCLUDES snaps at exactly the
    # opp 20, our rz_* (pbp inside-20) includes them -- ours is expected >= theirs by
    # that sliver; the audit measures the gap rather than assuming it away. ----
    MapSpec("nflcom_player_situational", "fumble_recovery_own", "own_fr", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L1", table_col="_layout",
            row_filter="_table='Home vs Road'",
            quirks="the ONLY published own-recovery split anywhere"),
    MapSpec("nflcom_player_situational", "fumble_recovery_opp", "opp_fr", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L1", table_col="_layout",
            row_filter="_table='Home vs Road'",
            quirks="also TESTS the fumbles.fr -> fumble_recovery_opp licence: if fr "
                   "= own_fr + opp_fr that licence is mis-pinned"),
    MapSpec("nflcom_player_situational", "rz_pass_att", "att", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L5", table_col="_layout",
            row_filter="split_value = 'Opp 19-1 - by Yard Line'"),
    MapSpec("nflcom_player_situational", "rz_pass_td", "td", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L5", table_col="_layout",
            row_filter="split_value = 'Opp 19-1 - by Yard Line'"),
    MapSpec("nflcom_player_situational", "rz_carries", "att", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L3", table_col="_layout",
            row_filter="split_value = 'Opp 19-1 - by Yard Line'"),
    MapSpec("nflcom_player_situational", "rz_rush_td", "td", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L3", table_col="_layout",
            row_filter="split_value = 'Opp 19-1 - by Yard Line'"),
    MapSpec("nflcom_player_situational", "rz_rec_td", "td", "nflcom",
            grain="season", team_col="nflcom_slug",
            source_table="player_situational_L2", table_col="_layout",
            row_filter="split_value = 'Opp 19-1 - by Yard Line'",
            quirks="rz_targets has no witness here: L2 publishes no targets column"),
    # ---- 2026-08-01 wave 3c: nflcom composite FG buckets via source_expr.
    # Cell format "made/attempted" (order measured, first<second on every unequal
    # cell); missed = attempted - made. 12 value paths off 6 physical columns. ----
    *[MapSpec("nflcom_player_season", made_col, comp, "nflcom", grain="season",
              team_col="_player_slug", source_table="field-goals",
              table_col="_category", row_filter="season_type='reg'",
              source_expr=f"TRY_CAST(regexp_extract(s.\"{comp}\", '^(\\d+)', 1) AS DOUBLE)")
      for comp, made_col in [("1_19_a_m", "fg_made_0_19"), ("20_29_a_m", "fg_made_20_29"),
                             ("30_39_a_m", "fg_made_30_39"), ("40_49_a_m", "fg_made_40_49"),
                             ("50_59_a_m", "fg_made_50_59"), ("60_a_m", "fg_made_60_")]],
    *[MapSpec("nflcom_player_season", miss_col, comp, "nflcom", grain="season",
              team_col="_player_slug", source_table="field-goals",
              table_col="_category", row_filter="season_type='reg'",
              source_expr=(f"TRY_CAST(regexp_extract(s.\"{comp}\", '/(\\d+)$', 1) AS DOUBLE)"
                           f" - TRY_CAST(regexp_extract(s.\"{comp}\", '^(\\d+)', 1) AS DOUBLE)"),
              quirks="nflcom bucket ATTEMPTS include blocked kicks, our fg_att does "
                     "not (2025: att-made=157=missed exactly, blocked=23 separate), "
                     "so source misses run HIGH by blocks-in-bucket; the excess "
                     "derives fg_blocked_distance, which nobody publishes directly")
      for comp, miss_col in [("1_19_a_m", "fg_missed_0_19"), ("20_29_a_m", "fg_missed_20_29"),
                             ("30_39_a_m", "fg_missed_30_39"), ("40_49_a_m", "fg_missed_40_49"),
                             ("50_59_a_m", "fg_missed_50_59"), ("60_a_m", "fg_missed_60_")]],
    # ---- 2026-08-01 wave 4: the Scoring log (Joe: "Scoring log" x9). One row per
    # scoring play 1920-2025, scorer = first link id. Made-FG distances are the
    # only published per-kick distances anywhere (nflcom/pfr publish buckets). ----
    MapSpec("pfr_box_scoring", "fg_made_distance", "description", "pfr_scoring_log",
            grain="season",
            source_expr="TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard field goal', 1) AS DOUBLE)",
            quirks="sum of made-FG distances; fg_yards/fg_yards_canonical are "
                   "twins pending consolidation. VALIDATED 100% at 2023 AND 2019; "
                   "2025 reads 28% low on OUR side while fg_yds_over_30 (same "
                   "parsed distances) reads clean -- the wk14-18 2025 kicking "
                   "repair was PARTIAL, fixed over_30 and left this column stale"),
    MapSpec("pfr_box_scoring", "fg_made_60plus", "description", "pfr_scoring_log",
            grain="season",
            source_expr="CASE WHEN TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard field goal', 1) AS DOUBLE) >= 60 THEN 1 END",
            quirks="counts 60+ yard makes; fg_made_60_ carries the nflcom bucket "
                   "witness -- twin columns, each now witnessed by a different root"),
    MapSpec("pfr_box_scoring", "fg_yds_over_30", "description", "pfr_scoring_log",
            grain="season",
            source_expr="CASE WHEN TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard field goal', 1) AS DOUBLE) > 30 THEN "
                        "TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard field goal', 1) AS DOUBLE) - 30 END",
            quirks="candidate semantics sum(dist-30 | dist>30); the audit IS the "
                   "definition test -- drop on failure, never force"),
    # ---- 2026-08-01 wave 5a: the game catalog (pfr_team_games was registered all
    # along; the deferral was chasing an imaginary registration hazard) ----
    MapSpec("pfr_team_games", "team_points", "team_points", "team_week", agg="value",
            grain="week", validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'"),
    MapSpec("pfr_team_games", "opponent_points", "opponent_points", "team_week",
            agg="value", grain="week", validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'"),
    MapSpec("pfr_team_games", "is_overtime", "is_overtime", "team_week", agg="value",
            grain="week", validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'"),
    MapSpec("pfr_team_games", "is_win", "result", "team_week", agg="value",
            grain="week", validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'",
            source_expr="CASE WHEN r.result='W' THEN 1.0 "
                        "WHEN r.result IN ('L','T') THEN 0.0 END"),
    MapSpec("pfr_team_games", "game_margin", "team_points", "team_week", agg="value",
            grain="week", validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'",
            source_expr="TRY_CAST(r.team_points AS DOUBLE) "
                        "- TRY_CAST(r.opponent_points AS DOUBLE)"),
    # ---- 2026-08-01 wave 5b: scoring_summary (Joe sign-off in-session) ----
    MapSpec("scoring_summary", "team_points", "points", "team_week", agg="value",
            grain="week", validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'",
            quirks="internal lineage: consistency witness, not an external root"),
    MapSpec("scoring_summary", "total_tds_scored", "td", "team_week", grain="week",
            validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'",
            quirks="team-summed player total_tds_scored vs the summary's TD count"),
    MapSpec("scoring_summary", "fg_made", "fg", "team_week", grain="week",
            validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'"),
    MapSpec("scoring_summary", "pat_made", "pat_made", "team_week", grain="week",
            validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'"),
    MapSpec("scoring_summary", "def_safeties", "safeties", "team_week", grain="week",
            validation_grain="week", team_col="team_code",
            row_filter="season_type='REG'",
            quirks="two_pt column NOT specced: split-at-admission disposition "
                   "PENDING in the signoff ledger"),
    # ---- 2026-08-01 wave 5c: raw pbp_merged, the registered oracle with zero
    # specs. Attribution via gsis id columns; filters = play predicate. ----
    MapSpec("pbp_merged_1978_2025", "rz_pass_att", "pass_attempt", "pbp_rollup",
            grain="season", team_col="passer_player_id",
            filters="r.pass_attempt=1 AND r.yardline_100<=20", source_expr="1",
            quirks="2025: medians IDENTICAL, 33% cell-exact -- +-1 edge-case predicate class (spikes/sacks/2pt); definition adjudication, not a wrong map"),
    MapSpec("pbp_merged_1978_2025", "rz_pass_td", "pass_touchdown", "pbp_rollup",
            grain="season", team_col="passer_player_id",
            filters="r.pass_touchdown=1 AND r.yardline_100<=20", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rz_carries", "rush_attempt", "pbp_rollup",
            grain="season", team_col="rusher_player_id",
            filters="r.rush_attempt=1 AND r.yardline_100<=20", source_expr="1",
            quirks="2025: 86% with identical medians -- kneel-down class"),
    MapSpec("pbp_merged_1978_2025", "rz_rush_td", "rush_touchdown", "pbp_rollup",
            grain="season", team_col="rusher_player_id",
            filters="r.rush_touchdown=1 AND r.yardline_100<=20", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rz_targets", "pass_attempt", "pbp_rollup",
            grain="season", team_col="receiver_player_id",
            filters="r.pass_attempt=1 AND r.yardline_100<=20", source_expr="1",
            quirks="the ONLY witness for rz_targets anywhere"),
    MapSpec("pbp_merged_1978_2025", "rz_rec_td", "pass_touchdown", "pbp_rollup",
            grain="season", team_col="receiver_player_id",
            filters="r.pass_touchdown=1 AND r.yardline_100<=20", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "pass_explosive_20", "complete_pass", "pbp_rollup",
            grain="season", team_col="passer_player_id",
            filters="r.complete_pass=1 AND r.yards_gained>=20", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rec_explosive_20", "complete_pass", "pbp_rollup",
            grain="season", team_col="receiver_player_id",
            filters="r.complete_pass=1 AND r.yards_gained>=20", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rush_explosive_10", "rush_attempt", "pbp_rollup",
            grain="season", team_col="rusher_player_id",
            filters="r.rush_attempt=1 AND r.yards_gained>=10", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "dropbacks", "qb_dropback", "pbp_rollup",
            grain="season", team_col="passer_player_id",
            filters="r.qb_dropback=1", source_expr="1",
            quirks="2025: 59% cell-exact, medians identical -- nullified-play class"),
    MapSpec("pbp_merged_1978_2025", "passing_2pt_conversions", "two_point_conv_result",
            "pbp_rollup", grain="season", team_col="passer_player_id",
            filters="r.two_point_conv_result='success' AND r.pass=1",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rushing_2pt_conversions", "two_point_conv_result",
            "pbp_rollup", grain="season", team_col="rusher_player_id",
            filters="r.two_point_conv_result='success' AND r.rush=1",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "receiving_2pt_conversions", "two_point_conv_result",
            "pbp_rollup", grain="season", team_col="receiver_player_id",
            filters="r.two_point_conv_result='success' AND r.pass=1",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "penalties", "penalty_player_id", "pbp_rollup",
            grain="season", team_col="penalty_player_id",
            filters="r.penalty=1 AND r.penalty_player_id IS NOT NULL", source_expr="1",
            quirks="source counts DOUBLE ours (med 2 vs 1) even accepted-only -- our penalties definition is narrower; adjudication before licence"),
    MapSpec("pbp_merged_1978_2025", "penalty_yards", "penalty_yards", "pbp_rollup",
            grain="season", team_col="penalty_player_id",
            filters="r.penalty=1 AND r.penalty_player_id IS NOT NULL",
            source_expr="TRY_CAST(r.penalty_yards AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "pass_success_plays", "success", "pbp_rollup",
            grain="season", team_col="passer_player_id",
            filters="r.pass_attempt=1 AND r.success=1", source_expr="1",
            quirks="OUR success_plays run ~2.7x nflverse success=1 -- different success definition entirely; NOT licensable until the definition is pinned"),
    MapSpec("pbp_merged_1978_2025", "rush_success_plays", "success", "pbp_rollup",
            grain="season", team_col="rusher_player_id",
            filters="r.rush_attempt=1 AND r.success=1", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rec_success_plays", "success", "pbp_rollup",
            grain="season", team_col="receiver_player_id",
            filters="r.pass_attempt=1 AND r.success=1", source_expr="1"),
    # ---- 2026-08-01 wave 5d: drive logs. Offensive drives credit the DEFENSE
    # that forced them; home drives witness visitor three-and-outs and vice
    # versa, so each spec covers half the league-weeks. ----
    MapSpec("pfr_box_home_drives", "three_out", "end_event", "pfr_drives",
            grain="week", validation_grain="week", source_table="home",
            filters="r.end_event = 'Punt' AND TRY_CAST(r.play_count_tip AS INT) <= 3",
            quirks="2025: ~21% cell-exact with IDENTICAL medians across four "
                   "definition variants -- +-1 drive-segmentation scatter between "
                   "PFR's drive log and our pbp segmentation; adjudication, "
                   "not licensable at exact tolerance"),
    MapSpec("pfr_box_vis_drives", "three_out", "end_event", "pfr_drives",
            grain="week", validation_grain="week", source_table="vis",
            filters="r.end_event = 'Punt' AND TRY_CAST(r.play_count_tip AS INT) <= 3"),
    # ---- 2026-08-01 wave 6: pfr_box_team_stats under Joe's re-key ruling
    # (selector re-key + dual-licensing mirror). Each spec = (label, extraction,
    # credit); 'opp' credit reads the mirror -- the home cell as the visitor's
    # *_allowed. Composite cells split on '-'; single-value labels cast whole. ----
    # 2025 audit: 12/20 VALIDATED 95-100% on 544 team-weeks. Measured-not-licensed:
    # total_yds_allowed (PFR Total = NET pass, ours gross -- definitional pin);
    # def_sacks/def_sack_yards/def_interceptions (our team-week SUM double-counts:
    # DST aggregate row + player rows both carry the stat -- v26-side accounting,
    # note def_carries_allowed, DST-row-only, reads 94.7%); penalties (player-sum
    # narrower than team totals, same class as the pbp finding).
    *[MapSpec("pfr_box_team_stats", v26, "home_stat", "pfr_team_stats",
              grain="week", validation_grain="week", source_table=label,
              team_col=credit, source_expr=expr)
      for v26, label, credit, expr in [
        # own side
        ("sacks_suffered", "Sacked-Yards", "own",
         "TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
        ("sack_yards_lost", "Sacked-Yards", "own",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
        ("penalties", "Penalties-Yards", "own",
         "TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
        ("penalty_yards", "Penalties-Yards", "own",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
        ("turnovers", "Turnovers", "own", "TRY_CAST({col} AS DOUBLE)"),
        ("rushing_yards", "Rush-Yds-TDs", "own",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
        ("passing_yards", "Cmp-Att-Yd-TD-INT", "own",
         "TRY_CAST(split_part({col}, '-', 3) AS DOUBLE)"),
        # the mirror: every cell is also the OTHER team's allowed
        ("def_carries_allowed", "Rush-Yds-TDs", "opp",
         "TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
        ("rushing_yds_allowed", "Rush-Yds-TDs", "opp",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
        ("rushing_tds_allowed", "Rush-Yds-TDs", "opp",
         "TRY_CAST(split_part({col}, '-', 3) AS DOUBLE)"),
        ("passing_yds_allowed", "Cmp-Att-Yd-TD-INT", "opp",
         "TRY_CAST(split_part({col}, '-', 3) AS DOUBLE)"),
        ("passing_tds_allowed", "Cmp-Att-Yd-TD-INT", "opp",
         "TRY_CAST(split_part({col}, '-', 4) AS DOUBLE)"),
        ("def_interceptions", "Cmp-Att-Yd-TD-INT", "opp",
         "TRY_CAST(split_part({col}, '-', 5) AS DOUBLE)"),
        # total_yds_allowed REMOVED from this path 2026-08-01: the twin pin
        # measured def_yards_allowed 91% (net-based, matches PFR Total) vs
        # total_yds_allowed 14% (gross) -- the path belongs to the net twin,
        # and the gross column's conviction receipt lives in wave 6.
        ("def_sacks", "Sacked-Yards", "opp",
         "TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
        ("def_sack_yards", "Sacked-Yards", "opp",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
        ("def_third_down_allowed", "Third Down Conv.", "opp",
         "TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
        ("def_third_down_faced", "Third Down Conv.", "opp",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
        ("def_fourth_down_allowed", "Fourth Down Conv.", "opp",
         "TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
        ("def_fourth_down_faced", "Fourth Down Conv.", "opp",
         "TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
      ]],
    # ---- 2026-08-01 wave 7a: the equation lane on pages ("we have yards and
    # attempts and sack yards"). These target the BROKEN dense season twins, so
    # like completion_pct they are expected to CONVICT, not validate. ----
    MapSpec("pfr_player_season_passing", "passing_yards_per_attempt", "pass_yds",
            "pages", agg="value", validation_tolerance=0.06,
            source_expr="TRY_CAST(pass_yds AS DOUBLE)"
                        "/NULLIF(TRY_CAST(pass_att AS DOUBLE),0)"),
    MapSpec("pfr_player_season_passing", "passing_adjusted_yards_per_attempt",
            "pass_yds", "pages", agg="value", validation_tolerance=0.06,
            source_expr="(TRY_CAST(pass_yds AS DOUBLE)+20*TRY_CAST(pass_td AS DOUBLE)"
                        "-45*TRY_CAST(pass_int AS DOUBLE))"
                        "/NULLIF(TRY_CAST(pass_att AS DOUBLE),0)"),
    MapSpec("pfr_player_season_passing", "passing_net_yards_per_attempt",
            "pass_yds", "pages", agg="value", validation_tolerance=0.06,
            source_expr="(TRY_CAST(pass_yds AS DOUBLE)"
                        "-TRY_CAST(pass_sacked_yds AS DOUBLE))"
                        "/NULLIF(TRY_CAST(pass_att AS DOUBLE)"
                        "+TRY_CAST(pass_sacked AS DOUBLE),0)"),
    MapSpec("pfr_player_season_passing", "passing_adjusted_net_yards_per_attempt",
            "pass_yds", "pages", agg="value", validation_tolerance=0.06,
            source_expr="(TRY_CAST(pass_yds AS DOUBLE)+20*TRY_CAST(pass_td AS DOUBLE)"
                        "-45*TRY_CAST(pass_int AS DOUBLE)"
                        "-TRY_CAST(pass_sacked_yds AS DOUBLE))"
                        "/NULLIF(TRY_CAST(pass_att AS DOUBLE)"
                        "+TRY_CAST(pass_sacked AS DOUBLE),0)"),
    MapSpec("pfr_player_season_rush_rec", "opportunities", "rush_att", "pages",
            source_expr="COALESCE(TRY_CAST(rush_att AS DOUBLE),0)"
                        "+COALESCE(TRY_CAST(targets AS DOUBLE),0)",
            quirks="carries + targets (Joe's definition)"),
    MapSpec("pfr_player_season_rec_rush", "opportunities", "rush_att", "pages",
            source_expr="COALESCE(TRY_CAST(rush_att AS DOUBLE),0)"
                        "+COALESCE(TRY_CAST(targets AS DOUBLE),0)"),
    MapSpec("pfr_player_returns", "total_return_yards", "punt_ret_yds", "pages",
            source_expr="CASE WHEN punt_ret_yds IS NOT NULL OR kick_ret_yds IS NOT "
                        "NULL THEN COALESCE(TRY_CAST(punt_ret_yds AS DOUBLE),0)"
                        "+COALESCE(TRY_CAST(kick_ret_yds AS DOUBLE),0) END",
            quirks="a row must publish at least one side before 0 can vouch"),
    # ---- 2026-08-01 wave 7b: scoring log round 2 (player-credited only; the
    # team-credited allowed family waits on the nickname crosswalk with EP) ----
    MapSpec("pfr_box_scoring", "rz_rush_td", "description", "pfr_scoring_log",
            grain="season",
            source_expr="CASE WHEN TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard rush', 1) AS DOUBLE) <= 20 THEN 1 END",
            quirks="TD distance <= 20 <=> snap inside our rz; second root beside "
                   "situational and pbp"),
    MapSpec("pfr_box_scoring", "rz_rec_td", "description", "pfr_scoring_log",
            grain="season",
            source_expr="CASE WHEN TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard pass', 1) AS DOUBLE) <= 20 THEN 1 END"),
    MapSpec("pfr_box_scoring", "rz_pass_td", "description", "pfr_scoring_log",
            grain="season", table_col="2",
            source_expr="CASE WHEN TRY_CAST(regexp_extract(s.description, "
                        "'(\\d+) yard pass from', 1) AS DOUBLE) <= 20 THEN 1 END",
            quirks="passer is the SECOND link id on 'A pass from B' scores"),
    MapSpec("pfr_box_scoring", "total_tds_accounted_for", "description", "pfr_scoring_log",
            grain="season",
            source_expr="CASE WHEN s.description LIKE '% yard %' AND "
                        "s.description NOT LIKE '%field goal%' THEN 1 END",
            quirks="THE SEMANTIC PIN for total_tds_accounted_for (68.6% equal to the validated "
                   "total_tds_scored): measured 75.4% vs all-scoring-log TDs 2025 -- "
                   "closer to all-TDs than total_tds_scored is; adjudication has its "
                   "numbers now"),
    MapSpec("pfr_box_scoring", "rushing_2pt_conversions", "description",
            "pfr_scoring_log", grain="season", table_col="-1",
            filters="regexp_matches(s.description, '\\([^)]* run\\)$')",
            source_expr="1",
            quirks="the 2pt runner is always the LAST link id. ~75% -- the SAME rate "
                   "the independent pbp witness reads, so the two surfaces agree "
                   "with each other against US: our 2pt attribution is the "
                   "supertable-fault candidate"),
    MapSpec("pfr_box_scoring", "passing_2pt_conversions", "description",
            "pfr_scoring_log", grain="season", table_col="-1",
            filters="regexp_matches(s.description, '\\([^)]*pass from [^)]*\\)$')",
            source_expr="1",
            quirks="the 2pt passer is always the LAST link id"),
    MapSpec("pfr_box_scoring", "receiving_2pt_conversions", "description",
            "pfr_scoring_log", grain="season", table_col="-2",
            filters="regexp_matches(s.description, '\\([^)]*pass from [^)]*\\)$')",
            source_expr="1",
            quirks="the 2pt receiver is second-to-last"),
    # ---- 2026-08-01 wave 7c: mirrors that need no parsing ----
    # points_allowed NOT specced: it is opponent_points on the same row -- the
    # 100% receipt proves the ALIAS, and one value path licenses one canonical
    # (opponent_points holds it). Alias consolidation queued with touches/
    # total_touches and the fg canonical family.
    MapSpec("pfr_box_team_stats", "def_yards_allowed", "home_stat",
            "pfr_team_stats", grain="week", validation_grain="week",
            source_table="Total Yards", team_col="opp",
            source_expr="TRY_CAST({col} AS DOUBLE)",
            quirks="TWIN PIN MEASURED 2025: 91% here vs 14% on total_yds_allowed -- "
                   "def_yards_allowed is NET-based (PFR Total = rush + net pass), "
                   "total_yds_allowed is GROSS; consolidation now has its receipt"),
    # ---- 2026-08-01 wave 8: Joe's column ruling. total_points was NEVER a new
    # column -- total_points_scored exists and was simply unwitnessed (the exact
    # candidates-we-already-have trap he warned about). kicking_points IS the one
    # approved new column: measured here against its witnesses via a v26_expr
    # computed from components until the plane carries it. fga buckets are
    # DERIVABLE (made+missed), never new columns. ----
    # total_points_scored <- pfr scoring: ALREADY SPECCED by the concurrent
    # session (the wave52 block below) -- one value path, one licence; Joe's
    # ruling (ball-carrier TDs + 2pts count toward total) is recorded there.
    # Its 93.4% with identical medians ties the residual to the known 2pt
    # attribution defect.
    MapSpec("statscrew_team_season_stats", "total_points_scored", "pts",
            "statscrew_team_season", source_table="total_scoring",
            table_col="table_tag", team_col="source_player_id",
            quirks="second root for total points"),
    MapSpec("statscrew_team_season_stats", "kicking_points", "pts",
            "statscrew_team_season", source_table="kicking",
            table_col="table_tag", team_col="source_player_id",
            agg="value", validation_tolerance=0.4,
            v26_expr="SUM(3*COALESCE(TRY_CAST(t.fg_made AS DOUBLE),0)"
                     "+COALESCE(TRY_CAST(t.pat_made AS DOUBLE),0))",
            quirks="THE APPROVED NEW COLUMN (Joe 2026-08-01): kicking_points = "
                   "3*FG + XP, measured against components until the plane "
                   "carries the column"),
    *[MapSpec("pfr_player_kicking", att_label, src, "pages",
              agg="value", validation_tolerance=0.4, v26_expr=expr,
              quirks="fga buckets are DERIVABLE from made+missed -- mis-adjudicated "
                     "as new-column candidates. Agreement degrades with distance "
                     "(100/93/79/63/66%): PFR attempts INCLUDE blocked kicks, our "
                     "buckets do not -- the SECOND independent derivation surface "
                     "for fg_blocked_distance beside the nflcom bucket excess")
      for att_label, src, expr in [
        ("fg_att_0_19", "fga1",
         "SUM(COALESCE(TRY_CAST(t.fg_made_0_19 AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_missed_0_19 AS DOUBLE),0))"),
        ("fg_att_20_29", "fga2",
         "SUM(COALESCE(TRY_CAST(t.fg_made_20_29 AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_missed_20_29 AS DOUBLE),0))"),
        ("fg_att_30_39", "fga3",
         "SUM(COALESCE(TRY_CAST(t.fg_made_30_39 AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_missed_30_39 AS DOUBLE),0))"),
        ("fg_att_40_49", "fga4",
         "SUM(COALESCE(TRY_CAST(t.fg_made_40_49 AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_missed_40_49 AS DOUBLE),0))"),
        ("fg_att_50plus", "fga5",
         "SUM(COALESCE(TRY_CAST(t.fg_made_50_59 AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_missed_50_59 AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_made_60_ AS DOUBLE),0)"
         "+COALESCE(TRY_CAST(t.fg_missed_60_ AS DOUBLE),0))"),
      ]],
    # ---- 2026-08-01 wave 9: expected_points UNBLOCKED by the self-built
    # nickname crosswalk. PFR's EP model is not nflverse EPA -- these measure the
    # cross-model relationship; licensing is what the numbers earn. ----
    MapSpec("pfr_box_expected_points", "def_epa_allowed", "pbp_exp_points_def_tot",
            "pfr_box_team_ep", agg="value", grain="week", validation_grain="week",
            scale=-1.0, validation_tolerance=1.5,
            quirks="PFR EP model vs nflverse EPA: same concept, different model; "
                   "sign flipped (their defense-positive = our offense-take)"),
    MapSpec("pfr_box_expected_points", "def_pass_epa_allowed",
            "pbp_exp_points_def_pass", "pfr_box_team_ep", agg="value", grain="week",
            validation_grain="week", scale=-1.0, validation_tolerance=1.5),
    MapSpec("pfr_box_expected_points", "def_rush_epa_allowed",
            "pbp_exp_points_def_rush", "pfr_box_team_ep", agg="value", grain="week",
            validation_grain="week", scale=-1.0, validation_tolerance=1.5),
    # ---- 2026-08-01 wave 9b: the denominator beneath the team_stats counter.
    # The specced labels read only ~2/3 of published rows IN EVERY ERA; these
    # close the closable third (Fumbles-Lost composite + the era-variant single-
    # value labels). First Downs / Net Pass Yards / Time of Possession stay
    # blocked on unapproved canonicals. ----
    MapSpec("pfr_box_team_stats", "fumbles", "home_stat", "pfr_team_stats",
            grain="week", validation_grain="week", source_table="Fumbles-Lost",
            team_col="own",
            source_expr="TRY_CAST(split_part({col}, '-', 1) AS DOUBLE)"),
    MapSpec("pfr_box_team_stats", "fumbles_lost", "home_stat", "pfr_team_stats",
            grain="week", validation_grain="week", source_table="Fumbles-Lost",
            team_col="own",
            source_expr="TRY_CAST(split_part({col}, '-', 2) AS DOUBLE)"),
    MapSpec("pfr_box_team_stats", "sack_yards_lost", "home_stat", "pfr_team_stats",
            grain="week", validation_grain="week", source_table="Sack Yds Lost",
            team_col="own", source_expr="TRY_CAST({col} AS DOUBLE)",
            validation_years=(1952, 1963),
            quirks="era-variant single-value label extends the composite-label "
                   "licence into the mid-century strata; label exists ONLY "
                   "1947-1963, so validation runs in-era (re-span 08-02)"),
    MapSpec("pfr_box_team_stats", "penalty_yards", "home_stat", "pfr_team_stats",
            grain="week", validation_grain="week", source_table="Penalty Yds",
            team_col="own", source_expr="TRY_CAST({col} AS DOUBLE)",
            validation_years=(1933, 1945),
            quirks="label exists ONLY 1920-1945 but the plane's penalty_yards "
                   "starts 1978: NO-OVERLAP is structural -- this label is a "
                   "pure BACKFILL witness (1,242 team-game values for an era "
                   "we hold nothing). Licence follows the sibling "
                   "Penalties-Yards label; backfill lane consumes this one"),
    MapSpec("pfr_box_team_stats", "fumbles_lost", "home_stat", "pfr_team_stats",
            grain="week", validation_grain="week", source_table="Fumbles Lost",
            team_col="own", source_expr="TRY_CAST({col} AS DOUBLE)"),
    # ---- 2026-08-01 Stage 0d: the allowed family already sitting as columns on
    # pbp_team_defense (same root as v26 -- load-fidelity witnesses, ngs-style) ----
    *[MapSpec("pbp_team_defense", c, c, "team_week", agg="value", grain="week",
              validation_grain="week", team_col="nfl_team")
      for c in ("def_attempts_allowed", "def_carries_allowed",
                "passing_first_downs_allowed", "rushing_first_downs_allowed",
                "receiving_first_downs_allowed")],
    # Stage 0b RETIRED same-session: the roster's game_id column is 100% NULL
    # (144,568 rows, 0 populated) -- schema presence masqueraded as population.
    # The surface is SEASON-grain appearance (its crosswalk-seed role stands);
    # games_played gets no witness here. The PFA participation lane carries the
    # SAME schema family and its game_id population is UNVERIFIED until measured.
    # ---- Stage 0c: total_epa / total_wpa via union attribution ----
    MapSpec("pbp_merged_1978_2025", "total_epa", "epa", "pbp_total_role_union",
            grain="season",
            quirks="passer(qb_epa) + rusher(epa) + receiver(epa) role union -- "
                   "the same three-family sum the pipeline computes"),
    MapSpec("pbp_merged_1978_2025", "total_wpa", "wpa", "pbp_total_role_union",
            grain="season"),
    # ---- Stage 0e: missed-FG distances from the pbp detail text ----
    MapSpec("pfr_box_pbp", "fg_missed_distance", "detail", "pfr_pbp_detail",
            grain="season",
            source_expr="CASE WHEN s.detail LIKE '%no good%' "
                        "AND s.detail NOT LIKE '%blocked%' THEN "
                        "TRY_CAST(regexp_extract(s.detail, "
                        "'(\d+) yard field goal', 1) AS DOUBLE) END",
            quirks="the only published missed-distance surface anywhere. "
                   "VALIDATED 100% at 2023; 2025 low on OUR side = the FOURTH "
                   "column-precise receipt on the stale wk14-18 kicking strata "
                   "(made_distance + missed_distance stale, over_30 repaired); "
                   "2019 ~79% = pre-2020 blocked-classification edge, adjudicate"),
    # ---- 2026-08-01 pbp derivation wave (Joe: pbp derives ~99% to '78, 2pt '94).
    # Player-attributed batch; longs are MAX-class; fumbles attribute the FUMBLER. ----
    *[MapSpec("pbp_merged_1978_2025", v26, sc, "pbp_rollup", grain="season",
              team_col=attr, filters=flt, source_expr=expr, agg=agg)
      for v26, sc, attr, flt, expr, agg in [
        ("passing_air_yards", "air_yards", "passer_player_id",
         "r.pass_attempt=1", "TRY_CAST(r.air_yards AS DOUBLE)", "sum"),
        ("passing_completed_air_yards", "air_yards", "passer_player_id",
         "r.complete_pass=1", "TRY_CAST(r.air_yards AS DOUBLE)", "sum"),
        ("passing_first_downs", "first_down_pass", "passer_player_id",
         "r.first_down_pass=1", "1", "sum"),
        ("passing_long", "yards_gained", "passer_player_id",
         "r.complete_pass=1", "TRY_CAST(r.yards_gained AS DOUBLE)", "max"),
        ("passing_yards_after_catch", "yards_after_catch", "passer_player_id",
         "r.complete_pass=1", "TRY_CAST(r.yards_after_catch AS DOUBLE)", "sum"),
        ("rushing_first_downs", "first_down_rush", "rusher_player_id",
         "r.first_down_rush=1 AND LOWER(COALESCE(CAST(r.\"desc\" AS VARCHAR), '')) NOT LIKE '%(no play)%' AND NOT (COALESCE(CAST(r.pbp_source_system AS VARCHAR), '')='nflverse' AND COALESCE(CAST(r.play_type AS VARCHAR), '')='no_play')", "1", "sum"),
        ("rushing_long", "yards_gained", "rusher_player_id",
         "r.rush_attempt=1", "TRY_CAST(r.yards_gained AS DOUBLE)", "max"),
        ("rushing_scrambles", "qb_scramble", "rusher_player_id",
         "r.qb_scramble=1", "1", "sum"),
        ("rushing_40plus", "yards_gained", "rusher_player_id",
         "r.rush_attempt=1 AND r.yards_gained>=40", "1", "sum"),
        ("rushing_tds_40plus", "rush_touchdown", "rusher_player_id",
         "r.rush_touchdown=1 AND r.yards_gained>=40", "1", "sum"),
        ("rushing_tds_50plus", "rush_touchdown", "rusher_player_id",
         "r.rush_touchdown=1 AND r.yards_gained>=50", "1", "sum"),
        ("rushing_fumbles", "fumbled_1_player_id", "rusher_player_id",
         "r.rush_attempt=1 AND (r.fumbled_1_player_id=r.rusher_player_id OR r.fumbled_2_player_id=r.rusher_player_id) AND LOWER(COALESCE(CAST(r.\"desc\" AS VARCHAR), '')) NOT LIKE '%(no play)%' AND NOT (COALESCE(CAST(r.pbp_source_system AS VARCHAR), '')='nflverse' AND COALESCE(CAST(r.play_type AS VARCHAR), '')='no_play')", "1", "sum"),
        ("rushing_fumbles_lost", "fumble_lost", "rusher_player_id",
         "r.rush_attempt=1 AND r.fumble_lost=1 AND (r.fumbled_1_player_id=r.rusher_player_id OR r.fumbled_2_player_id=r.rusher_player_id) AND LOWER(COALESCE(CAST(r.\"desc\" AS VARCHAR), '')) NOT LIKE '%(no play)%' AND NOT (COALESCE(CAST(r.pbp_source_system AS VARCHAR), '')='nflverse' AND COALESCE(CAST(r.play_type AS VARCHAR), '')='no_play')", "1", "sum"),
        ("receiving_completed_air_yards", "air_yards", "receiver_player_id",
         "r.complete_pass=1", "TRY_CAST(r.air_yards AS DOUBLE)", "sum"),
        ("receiving_yards_after_catch", "yards_after_catch", "receiver_player_id",
         "r.complete_pass=1", "TRY_CAST(r.yards_after_catch AS DOUBLE)", "sum"),
        ("receiving_first_downs", "first_down_pass", "receiver_player_id",
         "r.first_down_pass=1", "1", "sum"),
        ("receiving_long", "yards_gained", "receiver_player_id",
         "r.complete_pass=1", "TRY_CAST(r.yards_gained AS DOUBLE)", "max"),
        ("fg_blocked", "field_goal_result", "kicker_player_id",
         "r.field_goal_result='blocked'", "1", "sum"),
        ("fg_missed_distance", "kick_distance", "kicker_player_id",
         "r.field_goal_result='missed'",
         "TRY_CAST(r.kick_distance AS DOUBLE)", "sum"),
        ("pat_blocked", "extra_point_result", "kicker_player_id",
         "r.extra_point_result='blocked'", "1", "sum"),
      ]],
    # Fumble partition witnesses.  The event map deliberately counts both
    # fumble slots and partitions each event by the credited offensive role.
    *[MapSpec("pbp_merged_1978_2025", c, "fumble_event", "pbp_fumble_events",
              grain="season", validation_tolerance=0.0,
              quirks="PBP-DISPOSITION-NO-PLAY-001; fumble slots 1+2; mutually exclusive role partition")
      for c in ("fumbles", "fumbles_lost", "rushing_fumbles", "rushing_fumbles_lost",
                "receiving_fumbles", "receiving_fumbles_lost", "sack_fumbles", "sack_fumbles_lost")],
    # ---- team-DST derivation batch (defteam attribution). 2025 AUDIT VERDICT:
    # these four read ~2x pbp while def_attempts_allowed matches EXACTLY --
    # FOUR MORE RECEIPTS for repair 2c (DST aggregate row + defensive player
    # rows both carry per-target stats; attempts live on the DST row alone).
    # The specs are the repair's verification harness, not licensable yet. ----
    *[MapSpec("pbp_merged_1978_2025", v26, sc, "pbp_team_rollup", grain="week",
              validation_grain="week", filters=flt, source_expr=expr)
      for v26, sc, flt, expr in [
        ("def_completions_allowed", "complete_pass", "r.complete_pass=1", "1"),
        ("def_completion_yards_allowed", "yards_gained", "r.complete_pass=1",
         "TRY_CAST(r.yards_gained AS DOUBLE)"),
        ("def_air_yards_allowed", "air_yards", "r.pass_attempt=1",
         "TRY_CAST(r.air_yards AS DOUBLE)"),
        ("def_yards_after_catch_allowed", "yards_after_catch", "r.complete_pass=1",
         "TRY_CAST(r.yards_after_catch AS DOUBLE)"),
        ("def_targets_allowed", "pass_attempt", "r.pass_attempt=1", "1"),
      ]],
    # ---- 2026-08-01 rate dissolution (Joe: rates ARE pbp-mappable): the
    # product-facing rates, computed as true ratios from raw pbp. The pct/ypc/ypr
    # specs double as 2a verification instruments (stored season rates are the
    # convicted sums). ----
    MapSpec("pbp_merged_1978_2025", "completion_pct", "complete_pass", "pbp_ratio",
            agg="value", grain="season", team_col="passer_player_id",
            validation_tolerance=0.06,
            source_expr="100.0*COUNT(*) FILTER (WHERE r.complete_pass=1 AND r.pass_attempt=1)"
                        "/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "catch_pct", "complete_pass", "pbp_ratio",
            agg="value", grain="season", team_col="receiver_player_id",
            validation_tolerance=0.06,
            source_expr="100.0*COUNT(*) FILTER (WHERE r.complete_pass=1 AND r.pass_attempt=1)"
                        "/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "rushing_yards_per_carry", "yards_gained",
            "pbp_ratio", agg="value", grain="season", team_col="rusher_player_id",
            validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.rush_attempt=1)"
                        "/NULLIF(COUNT(*) FILTER (WHERE r.rush_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "receiving_yards_per_reception", "yards_gained",
            "pbp_ratio", agg="value", grain="season", team_col="receiver_player_id",
            validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.complete_pass=1)"
                        "/NULLIF(COUNT(*) FILTER (WHERE r.complete_pass=1),0)"),
    MapSpec("pbp_merged_1978_2025", "passing_cpoe", "cpoe", "pbp_ratio",
            agg="value", grain="season", team_col="passer_player_id",
            validation_tolerance=0.6,
            source_expr="AVG(TRY_CAST(r.cpoe AS DOUBLE))",
            quirks="mean cpoe over charted attempts; formula measured not assumed"),
    # ---- 2026-08-01 gwfg (Joe: 'why is gwfg not mapped to pbp?'): definition
    # reverse-engineered against stored -- go-ahead FG in final 2:00 of Q4 or OT
    # matches 18/24 kickers exactly at 2023; the 6-kicker residual is the
    # definition-pin adjudication, documented not hidden. ----
    MapSpec("pbp_merged_1978_2025", "gwfg_att", "field_goal_attempt", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_attempt=1 AND (r.qtr>=5 OR (r.qtr=4 AND r.game_seconds_remaining<=120)) AND r.score_differential BETWEEN -2 AND 0", source_expr="1",
            quirks="18/24 exact at 2023; residual = edge-class pin"),
    MapSpec("pbp_merged_1978_2025", "gwfg_made", "field_goal_result", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='made' AND (r.qtr>=5 OR (r.qtr=4 AND r.game_seconds_remaining<=120)) AND r.score_differential BETWEEN -2 AND 0", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "gwfg_missed", "field_goal_result", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND (r.qtr>=5 OR (r.qtr=4 AND r.game_seconds_remaining<=120)) AND r.score_differential BETWEEN -2 AND 0", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "gwfg_distance", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='made' AND (r.qtr>=5 OR (r.qtr=4 AND r.game_seconds_remaining<=120)) AND r.score_differential BETWEEN -2 AND 0",
            source_expr="TRY_CAST(r.kick_distance AS DOUBLE)"),
    # ---- 2026-08-01 careful pass (Joe: passer rating, net Y/A, hits, poor
    # throws): three of four had their pbp ingredients all along. ----
    MapSpec("pbp_merged_1978_2025", "passer_rating", "complete_pass", "pbp_ratio",
            agg="value", grain="season", team_col="passer_player_id",
            validation_tolerance=0.6,
            source_expr="((LEAST(GREATEST((COUNT(*) FILTER (WHERE r.complete_pass=1 AND r.pass_attempt=1)*1.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)-0.3)*5,0),2.375))+(LEAST(GREATEST((SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)-3)*0.25,0),2.375))+(LEAST(GREATEST(COUNT(*) FILTER (WHERE r.pass_touchdown=1)*20.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0),0),2.375))+(LEAST(GREATEST(2.375-COUNT(*) FILTER (WHERE r.interception=1 AND r.pass_attempt=1)*25.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0),0),2.375)))/6*100",
            quirks="the NFL formula with clamps, computed from raw plays. CONVICTION: ours reads ~86 vs true 73-78 at BOTH strata -- the season builder stores MEAN-OF-WEEKLY ratings (small-sample skew), a second distinct rate-aggregation defect beside the summed rates"),
    MapSpec("pbp_merged_1978_2025", "passing_net_yards_per_attempt",
            "yards_gained", "pbp_ratio", agg="value", grain="season",
            team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="(SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1 OR r.sack=1))/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1 OR r.sack=1),0)",
            quirks="(pass yds + negative sack yds)/(att+sacks); also a 2a "
                   "instrument against the summed season twin"),
    MapSpec("pbp_merged_1978_2025", "passing_hits", "qb_hit", "pbp_rollup",
            grain="season", team_col="passer_player_id",
            filters="r.qb_hit=1 AND r.qb_dropback=1 AND r.sack=0", source_expr="1",
            quirks="qb_hit runs from 1999 in pbp -- DEEPER than PFR charting (2018+); was mis-filed as charting-CANNOT until Joe asked. Definition pinned: hits EXCLUDE sacks (medians 16=16 at 2023); residual scatter = charting-vs-pbp per-game class"),
    # ---- 2026-08-01 THE COMB: burn the OWED queue (Joe: no gaps without a
    # documented reason; three_out should never have lasted). ----
    MapSpec("pbp_merged_1978_2025", "fg_missed_0_19", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND TRY_CAST(r.kick_distance AS DOUBLE) BETWEEN 0 AND 19",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "fg_missed_20_29", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND TRY_CAST(r.kick_distance AS DOUBLE) BETWEEN 20 AND 29",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "fg_missed_30_39", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND TRY_CAST(r.kick_distance AS DOUBLE) BETWEEN 30 AND 39",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "fg_missed_40_49", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND TRY_CAST(r.kick_distance AS DOUBLE) BETWEEN 40 AND 49",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "fg_missed_50_59", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND TRY_CAST(r.kick_distance AS DOUBLE) BETWEEN 50 AND 59",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "fg_missed_60_", "kick_distance", "pbp_rollup",
            grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='missed' AND TRY_CAST(r.kick_distance AS DOUBLE) BETWEEN 60 AND 99",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "fg_made_distance", "kick_distance",
            "pbp_rollup", grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='made'", source_expr="TRY_CAST(r.kick_distance AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "fg_yds_over_30", "kick_distance",
            "pbp_rollup", grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='made' AND TRY_CAST(r.kick_distance AS DOUBLE) > 30",
            source_expr="TRY_CAST(r.kick_distance AS DOUBLE) - 30"),
    MapSpec("pbp_merged_1978_2025", "pick6", "td_player_id", "pbp_rollup",
            grain="season", team_col="td_player_id",
            filters="r.interception=1 AND r.touchdown=1", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "total_tds_scored", "td_player_id", "pbp_rollup",
            grain="season", team_col="td_player_id",
            filters="r.touchdown=1", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "timeouts", "timeout", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="timeout_team",
            filters="r.timeout=1", source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "three_out", "drive_play_count",
            "pbp_drive_rollup", grain="week", validation_grain="week",
            source_expr="TRY_CAST(r.drive_play_count AS INT) <= 3 "
                        "AND r.drive_end_transition = 'PUNT'",
            quirks="never had an excuse: drive_play_count + drive_end_transition "
                   "sat in pbp the whole time (Joe)"),
    MapSpec("pbp_merged_1978_2025", "def_sack_yards", "yards_gained",
            "pbp_team_rollup", grain="week", validation_grain="week",
            filters="r.sack=1", source_expr="-TRY_CAST(r.yards_gained AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "target_share", "pass_attempt", "pbp_share",
            agg="value", grain="season", team_col="receiver_player_id",
            validation_tolerance=0.01,
            source_expr="COUNT(*) FILTER (WHERE r.pass_attempt=1)"
                        "|DEN|COUNT(*) FILTER (WHERE r.pass_attempt=1)"),
    MapSpec("pbp_merged_1978_2025", "air_yards_share", "air_yards", "pbp_share",
            agg="value", grain="season", team_col="receiver_player_id",
            validation_tolerance=0.01,
            source_expr="SUM(TRY_CAST(r.air_yards AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)"
                        "|DEN|SUM(TRY_CAST(r.air_yards AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)"),
    MapSpec("pbp_merged_1978_2025", "passing_yds_allowed", "yards_gained", "pbp_team_rollup",
            grain="week", validation_grain="week", filters="r.pass_attempt=1",
            source_expr="TRY_CAST(r.yards_gained AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "rushing_yds_allowed", "yards_gained", "pbp_team_rollup",
            grain="week", validation_grain="week", filters="r.rush_attempt=1",
            source_expr="TRY_CAST(r.yards_gained AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "passing_tds_allowed", "pass_touchdown", "pbp_team_rollup",
            grain="week", validation_grain="week", filters="r.pass_touchdown=1",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "rushing_tds_allowed", "rush_touchdown", "pbp_team_rollup",
            grain="week", validation_grain="week", filters="r.rush_touchdown=1",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "scrimmage_yards", "scrimmage_yards",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "scrimmage_tds", "scrimmage_tds",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "total_touches", "touches",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "opportunities", "opportunities",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "turnovers", "turnovers",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "total_return_yards", "return_yards",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "passing_int_pct", "interception", "pbp_ratio", agg="value",
            grain="season", team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="COUNT(*) FILTER (WHERE r.interception=1 AND r.pass_attempt=1)*1.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "passing_td_pct", "pass_touchdown", "pbp_ratio", agg="value",
            grain="season", team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="COUNT(*) FILTER (WHERE r.pass_touchdown=1)*1.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "passing_adjusted_yards_per_attempt", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="(SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)+20*COUNT(*) FILTER (WHERE r.pass_touchdown=1)-45*COUNT(*) FILTER (WHERE r.interception=1 AND r.pass_attempt=1))/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "passing_adjusted_net_yards_per_attempt", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="(SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1 OR r.sack=1)+20*COUNT(*) FILTER (WHERE r.pass_touchdown=1)-45*COUNT(*) FILTER (WHERE r.interception=1 AND r.pass_attempt=1))/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1 OR r.sack=1),0)"),
    MapSpec("pbp_merged_1978_2025", "receiving_yards_per_target", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="receiver_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.complete_pass=1)/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "receiving_adot", "air_yards", "pbp_ratio", agg="value",
            grain="season", team_col="receiver_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.air_yards AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "pacr", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.complete_pass=1)/NULLIF(SUM(TRY_CAST(r.air_yards AS DOUBLE)) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "racr", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="receiver_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.complete_pass=1)/NULLIF(SUM(TRY_CAST(r.air_yards AS DOUBLE)) FILTER (WHERE r.pass_attempt=1),0)"),
    # receiving_tds_allowed / yds_from_scrimmage / rush_receive_td: the comb
    # PROVED them aliases of passing_tds_allowed / scrimmage_yards /
    # scrimmage_tds (identical value paths) -- one path one licence; the
    # pairs join the consolidation slate.
    # ---- 2026-08-01 FINAL SWEEP: pbp fully combed (Joe). ----
    MapSpec("pbp_merged_1978_2025", "all_purpose_yards", "all_purpose",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "total_points_scored", "points_scored",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "def_tackles_combined", "tackles_combined",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "def_tackles_for_loss_yards", "tfl_yards",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "def_fumbles", "def_fumbles",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "fumble_recovery_own", "fum_rec_own",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "fumble_recovery_opp", "fum_rec_opp",
            "pbp_total_role_union", grain="season"),
    MapSpec("pbp_merged_1978_2025", "passing_yards_per_attempt", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="passer_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "fg_pct", "field_goal_result", "pbp_ratio", agg="value",
            grain="season", team_col="kicker_player_id", validation_tolerance=0.001,
            source_expr="COUNT(*) FILTER (WHERE r.field_goal_result='made')/NULLIF(COUNT(*) FILTER (WHERE r.field_goal_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "pat_pct", "extra_point_result", "pbp_ratio", agg="value",
            grain="season", team_col="kicker_player_id", validation_tolerance=0.001,
            source_expr="COUNT(*) FILTER (WHERE r.extra_point_result='good')*1.0/NULLIF(COUNT(*) FILTER (WHERE r.extra_point_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "yards_per_touch", "yards_gained", "pbp_ratio", agg="value",
            grain="season", team_col="rusher_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.rush_attempt=1 OR r.complete_pass=1)/NULLIF(COUNT(*) FILTER (WHERE r.rush_attempt=1 OR r.complete_pass=1),0)"),
    MapSpec("pbp_merged_1978_2025", "punt_yards_per_punt", "kick_distance", "pbp_ratio", agg="value",
            grain="season", team_col="punter_player_id", validation_tolerance=0.06,
            source_expr="SUM(TRY_CAST(r.kick_distance AS DOUBLE)) FILTER (WHERE r.punt_attempt=1)/NULLIF(COUNT(*) FILTER (WHERE r.punt_attempt=1),0)"),
    MapSpec("pbp_merged_1978_2025", "fourth_down_stop", "fourth_down_failed", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="defteam",
            filters="r.fourth_down_failed=1", source_expr="1", agg="sum"),
    MapSpec("pbp_merged_1978_2025", "gwfg_blocked", "field_goal_result", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="posteam",
            filters="r.field_goal_result='blocked' AND (r.qtr>=5 OR (r.qtr=4 AND r.game_seconds_remaining<=120)) AND r.score_differential BETWEEN -2 AND 0", source_expr="1", agg="sum"),
    MapSpec("pbp_merged_1978_2025", "points_allowed", "defteam_score", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="defteam",
            filters="TRUE", source_expr="TRY_CAST(r.posteam_score AS DOUBLE)",
            agg="value", witness_agg="max"),
    MapSpec("pbp_merged_1978_2025", "team_points", "posteam_score", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="posteam",
            filters="TRUE", source_expr="TRY_CAST(r.posteam_score AS DOUBLE)",
            agg="value", witness_agg="max"),
    MapSpec("pbp_merged_1978_2025", "opponent_points", "defteam_score", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="posteam",
            filters="TRUE", source_expr="TRY_CAST(r.defteam_score AS DOUBLE)",
            agg="value", witness_agg="max"),
    MapSpec("pbp_merged_1978_2025", "is_overtime", "qtr", "pbp_team_rollup",
            grain="week", validation_grain="week", team_col="posteam",
            filters="TRUE", source_expr="CASE WHEN TRY_CAST(r.qtr AS INT)>4 THEN 1 ELSE 0 END",
            agg="value", witness_agg="max"),
    # ---- 2026-08-01 the LAST FOUR: OWED reaches zero. ----
    MapSpec("pbp_merged_1978_2025", "receiving_pass_rating", "complete_pass",
            "pbp_ratio", agg="value", grain="season",
            team_col="receiver_player_id", validation_tolerance=0.6,
            source_expr="((LEAST(GREATEST((COUNT(*) FILTER (WHERE r.complete_pass=1 AND r.pass_attempt=1)*1.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)-0.3)*5,0),2.375))+(LEAST(GREATEST((SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)-3)*0.25,0),2.375))+(LEAST(GREATEST(COUNT(*) FILTER (WHERE r.pass_touchdown=1)*20.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0),0),2.375))+(LEAST(GREATEST(2.375-COUNT(*) FILTER (WHERE r.interception=1 AND r.pass_attempt=1)*25.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0),0),2.375)))/6*100"),
    MapSpec("pbp_merged_1978_2025", "def_passer_rating_allowed", "complete_pass",
            "pbp_team_ratio", agg="value", grain="week", validation_grain="week",
            validation_tolerance=0.6, source_expr="((LEAST(GREATEST((COUNT(*) FILTER (WHERE r.complete_pass=1 AND r.pass_attempt=1)*1.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)-0.3)*5,0),2.375))+(LEAST(GREATEST((SUM(TRY_CAST(r.yards_gained AS DOUBLE)) FILTER (WHERE r.pass_attempt=1)/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0)-3)*0.25,0),2.375))+(LEAST(GREATEST(COUNT(*) FILTER (WHERE r.pass_touchdown=1)*20.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0),0),2.375))+(LEAST(GREATEST(2.375-COUNT(*) FILTER (WHERE r.interception=1 AND r.pass_attempt=1)*25.0/NULLIF(COUNT(*) FILTER (WHERE r.pass_attempt=1),0),0),2.375)))/6*100"),
    MapSpec("pbp_merged_1978_2025", "total_yds_allowed", "yards_gained",
            "pbp_team_rollup", grain="week", validation_grain="week",
            team_col="defteam",
            filters="r.pass_attempt=1 OR r.rush_attempt=1",
            source_expr="TRY_CAST(r.yards_gained AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "dst_return_yards", "return_yards",
            "pbp_team_return", grain="week", validation_grain="week",
            team_col="return_team"),
    # ---- 2026-08-01 Joe's challenge round: two misclassifications fall ----
    # RAW vs FANTASY-ELIGIBLE points allowed -- LOCKED 2026-08-01 (Joe).
    # points_allowed = RAW final opponent score. dst_points_allowed = raw MINUS
    # scores the DST is not blamed for: opponent INT-return TDs, opponent
    # fumble-return TDs, and safeties (all conceded by OUR OFFENSE, so the
    # events group by posteam). Relationship gate: stored points_allowed minus
    # stored dst_points_allowed == 6*(retTDs) + 2*safeties held 407/408
    # team-weeks at 2023. NEVER map dst_points_allowed to a raw score.
    MapSpec("pbp_merged_1978_2025", "dst_points_allowed", "defteam_score",
            "pbp_team_ratio", agg="value", grain="week",
            validation_grain="week", team_col="posteam",
            source_expr="MAX(TRY_CAST(r.defteam_score AS DOUBLE))"
                        " - 6*COUNT(*) FILTER (WHERE r.touchdown=1"
                        " AND (r.interception=1 OR r.fumble_lost=1))"
                        " - 2*COUNT(*) FILTER (WHERE r.safety=1)",
            quirks="fantasy-eligible contraction; residual vs stored is the "
                   "running-score final-state proxy, not the formula (the "
                   "relationship gate isolates that at 407/408)"),
    # ---- 2026-08-02 STATSCREW results root (Joe: "team-code for statscrew") --
    MapSpec("statscrew_team_season_results", "is_win", "res",
            "statscrew_results", grain="week", validation_grain="week",
            agg="value", source_expr="CASE WHEN r.res='W' THEN 1 ELSE 0 END"),
    MapSpec("statscrew_team_season_results", "team_points", "game",
            "statscrew_results", grain="week", validation_grain="week",
            agg="value",
            source_expr="CASE WHEN r.res='W' THEN GREATEST(TRY_CAST(regexp_extract(r.game, ' (\d+) at ', 1) AS DOUBLE), TRY_CAST(regexp_extract(r.game, ' (\d+)$', 1) AS DOUBLE)) "
                        "WHEN r.res='L' THEN LEAST(TRY_CAST(regexp_extract(r.game, ' (\d+) at ', 1) AS DOUBLE), TRY_CAST(regexp_extract(r.game, ' (\d+)$', 1) AS DOUBLE)) "
                        "ELSE TRY_CAST(regexp_extract(r.game, ' (\d+) at ', 1) AS DOUBLE) END"),
    MapSpec("statscrew_team_season_results", "opponent_points", "game",
            "statscrew_results", grain="week", validation_grain="week",
            agg="value",
            source_expr="CASE WHEN r.res='W' THEN LEAST(TRY_CAST(regexp_extract(r.game, ' (\d+) at ', 1) AS DOUBLE), TRY_CAST(regexp_extract(r.game, ' (\d+)$', 1) AS DOUBLE)) "
                        "WHEN r.res='L' THEN GREATEST(TRY_CAST(regexp_extract(r.game, ' (\d+) at ', 1) AS DOUBLE), TRY_CAST(regexp_extract(r.game, ' (\d+)$', 1) AS DOUBLE)) "
                        "ELSE TRY_CAST(regexp_extract(r.game, ' (\d+)$', 1) AS DOUBLE) END"),
    # ---- 2026-08-02 PFA crosswalk landed (Joe: "do the PFA crosswalk") ----
    MapSpec("pfa_player_game_participation", "games_played", "game_id",
            "pfa_participation_games", grain="season", agg="value",
            validation_years=(1948, 1958),
            quirks="games-per-season appearance witness through the CORROBORATED "
                   "pfa crosswalk (16,362 matched; twins and span-violators "
                   "abstain); compares the season plane's real games_played, "
                   "in-era. MEASURED 48.3% exact (n=2,547) with we-exceed "
                   "dominating 993/325 median +2: PFA's partial boxscore "
                   "coverage under-counts games -- a LOWER-BOUND corroborator, "
                   "never an equality voter; equality licence stays withheld "
                   "per the pfa_licensing per-statistic coverage scope"),
    # ---- 2026-08-02 the defensive WEEK lanes, SPECCED (Joe's law) ----
    MapSpec("pbp_merged_1978_2025", "def_pass_defended", "pass_defense_1_player_id",
            "pbp_rollup_week", grain="week", validation_grain="week",
            team_col="pass_defense_1_player_id",
            filters="r.pass_defense_1_player_id IS NOT NULL", source_expr="1",
            quirks="WEEK-grain validated directly 98.3% n=27,047 (2005-19); "
                   "validator route for pbp_rollup_week owed"),
    MapSpec("pbp_merged_1978_2025", "def_fumbles_forced",
            "forced_fumble_player_1_player_id", "pbp_rollup_week",
            grain="week", validation_grain="week",
            team_col="forced_fumble_player_1_player_id",
            filters="r.forced_fumble_player_1_player_id IS NOT NULL",
            source_expr="1",
            quirks="WEEK-grain validated directly 97.8% n=6,735"),
    MapSpec("pbp_merged_1978_2025", "def_sacks", "sack_player_id",
            "pbp_rollup_week", grain="week", validation_grain="week",
            team_col="sack_player_id",
            filters="r.sack_player_id IS NOT NULL", source_expr="1",
            quirks="full sacks only: 92.9% n=13,678 at week grain -- the "
                   "missing 7% IS the half-sack class; licence withheld until "
                   "the half-sack union lane lands"),
    # ---- 2026-08-02 nflcom roster wired (Joe: "do the NFL.com glob") ----
    MapSpec("nflcom_team_season_roster", "nfl_team", "team",
            "appearance_games", grain="season", agg="value",
            v26_expr="COUNT(DISTINCT t.nfl_team)",
            quirks="MEMBERSHIP witness: season team-stint count, not "
                   "games_played (game_id is 100% NULL; grain is one row per "
                   "player-team-season -- measured 08-02, 0b retirement "
                   "confirmed). Corroborates missing/extra team stints"),
    # ---- 2026-08-01 FUMBLE GRAMMAR (Joe: "do the PFR fumble grammars") ----
    # 32,594 events 1978-2025 from the pbp detail sentences; grammar receipt in
    # the lake. fum_rec/fum_rec_yds NOT specced here: opp-recovery is the same
    # value path as fumble_recovery_opp (dup-path law) -- consolidation slate.
    MapSpec("pfr_box_pbp", "rushing_fumbles", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.context='rushing'"),
    MapSpec("pfr_box_pbp", "rushing_fumbles_lost", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.context='rushing' AND e.lost=1"),
    MapSpec("pfr_box_pbp", "receiving_fumbles", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.context='receiving'"),
    MapSpec("pfr_box_pbp", "receiving_fumbles_lost", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.context='receiving' AND e.lost=1"),
    MapSpec("pfr_box_pbp", "sack_fumbles", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.context='sack'"),
    MapSpec("pfr_box_pbp", "sack_fumbles_lost", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.context='sack' AND e.lost=1"),
    MapSpec("pfr_box_pbp", "fumbles", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id"),
    MapSpec("pfr_box_pbp", "fumbles_lost", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="fumbler_id",
            filters="e.lost=1"),
    MapSpec("pfr_box_pbp", "fumble_recovery_own", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="recoverer_id",
            filters="e.rec_own=1"),
    MapSpec("pfr_box_pbp", "fumble_recovery_opp", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="recoverer_id",
            filters="e.rec_own=0"),
    MapSpec("pfr_box_pbp", "def_fumbles_forced", "detail", "pfr_fumble_events",
            grain="season", source_table="fumble_events",
            team_col="forcer_id"),
    # ---- 2026-08-01 challenge round 2 (Joe): five more pbp lanes. ----
    MapSpec("pbp_merged_1978_2025", "kickoff_return_long", "return_yards",
            "pbp_rollup", grain="season", agg="max", witness_agg="max",
            team_col="kickoff_returner_player_id",
            filters="r.kickoff_attempt=1",
            source_expr="TRY_CAST(r.return_yards AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "punt_return_long", "return_yards",
            "pbp_rollup", grain="season", agg="max", witness_agg="max",
            team_col="punt_returner_player_id",
            filters="r.punt_attempt=1",
            source_expr="TRY_CAST(r.return_yards AS DOUBLE)"),
    MapSpec("pbp_merged_1978_2025", "fg_made_60_", "field_goal_result",
            "pbp_rollup", grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='made'"
                    " AND TRY_CAST(r.kick_distance AS INT)>=60",
            source_expr="1"),
    MapSpec("pbp_merged_1978_2025", "sack_yards_lost", "yards_gained",
            "pbp_rollup", grain="season", team_col="passer_player_id",
            filters="r.sack=1",
            source_expr="-TRY_CAST(r.yards_gained AS DOUBLE)",
            quirks="stored positive; pbp yards_gained is negative on sacks. "
                   "93.2% exact 2005-2019 (n=985, median 104=104); residual is "
                   "a BALANCED two-sided scatter (39 up / 28 down, median 5) = "
                   "strip-sack spot-of-fumble yardage, not a supertable zero "
                   "fault -- queued for arbitration, licence withheld"),
    MapSpec("pbp_merged_1978_2025", "home_away", "home_team",
            "pbp_team_rollup", grain="week", validation_grain="week",
            team_col="posteam", filters="TRUE",
            source_expr="CASE WHEN r.posteam=r.home_team THEN 1 ELSE 0 END",
            agg="value", witness_agg="max",
            v26_expr="MAX(CASE WHEN LOWER(t.home_away)='home'"
                     " THEN 1 ELSE 0 END)",
            quirks="text column bridged to 1/0 on both sides via v26_expr; "
                   "the FACT (side of the field) is what pbp witnesses"),
    MapSpec("pbp_merged_1978_2025", "fg_blocked_distance", "kick_distance",
            "pbp_rollup", grain="season", team_col="kicker_player_id",
            filters="r.field_goal_result='blocked'",
            source_expr="TRY_CAST(r.kick_distance AS DOUBLE)",
            quirks="my NO_PLANE_COLUMN claim was FALSE (probe error); Joe caught it"),
    MapSpec("pbp_merged_1978_2025", "receiving_target_interceptions",
            "interception", "pbp_rollup", grain="season",
            team_col="receiver_player_id",
            filters="r.interception=1 AND r.pass_attempt=1", source_expr="1",
            quirks="was charting-CANNOT; pbp has int + targeted receiver on the "
                   "same play -- Joe caught it"),
    MapSpec("pbp_merged_1978_2025", "game_margin", "posteam_score",
            "pbp_team_rollup", grain="week", validation_grain="week",
            team_col="posteam", filters="TRUE",
            source_expr="TRY_CAST(r.posteam_score AS DOUBLE)"
                        "-TRY_CAST(r.defteam_score AS DOUBLE)",
            agg="value", witness_agg="max"),
    MapSpec("pbp_merged_1978_2025", "is_win", "posteam_score",
            "pbp_team_rollup", grain="week", validation_grain="week",
            team_col="posteam", filters="TRUE",
            source_expr="CASE WHEN TRY_CAST(r.posteam_score AS DOUBLE)"
                        ">TRY_CAST(r.defteam_score AS DOUBLE) THEN 1 ELSE 0 END",
            agg="value", witness_agg="max",
            quirks="MEASURED PROXY ONLY (74%): MAX works for scores (monotonic) "
                   "but not margins -- a team can lead and lose; the final-state "
                   "aggregate needs arg_max machinery; catalog remains the 100% "
                   "witness. Same caveat on game_margin (19%)"),
    # ---- Phase-2 expansion: punting pages ----
    MapSpec("pfr_player_punting", "punts", "punt", "pages"),
    MapSpec("pfr_player_punting", "punt_yards", "punt_yds", "pages"),
    MapSpec("pfr_player_punting", "punt_long", "punt_long", "pages", agg="max",
            quirks="season long = MAX of weekly longs, never SUM"),
    # ---- Phase-2 expansion: defense pages ----
    MapSpec("pfr_player_defense", "def_interception_yards", "def_int_yds", "pages"),
    # 2026-08-04: the defense table's fumbles column is the ALL-PHASES total
    # (matches our column's definition) -- the rush_rec surface is offense-
    # scoped and undercounted return men, which is exactly why 474 ancient
    # player-seasons failed conservation (measured 2026-08-03)
    MapSpec("pfr_player_defense", "fumbles", "fumbles", "pages",
            quirks="all-phases fumble total; the second conservation witness "
                   "for the ancient weekly backfill"),
    MapSpec("pfr_player_defense", "def_int_ret_td", "def_int_td", "pages"),
    MapSpec("pfr_player_defense", "def_pass_defended", "pass_defended", "pages"),
    MapSpec("pfr_player_defense", "def_fumbles_forced", "fumbles_forced", "pages"),
    MapSpec("pfr_player_defense", "def_tackles_solo", "tackles_solo", "pages"),
    MapSpec("pfr_player_defense", "def_tackle_assists", "tackles_assists", "pages"),
    MapSpec("pfr_player_defense", "def_tackles_for_loss", "tackles_loss", "pages"),
    MapSpec("pfr_player_defense", "def_qb_hits", "qb_hits", "pages"),
    MapSpec("pfr_player_defense", "def_safeties", "safety_md", "pages"),
    MapSpec("pfr_player_defense", "fum_rec", "fumbles_rec", "pages",
            quirks="def_fumbles semantics pin PENDING (refinement queue 0b) -- own vs "
                   "opponent recovery; verdict is the measurement"),
    # ---- Phase-2 expansion: returns pages ----
    MapSpec("pfr_player_returns", "kickoff_returns", "kick_ret", "pages"),
    MapSpec("pfr_player_returns", "kickoff_return_yards", "kick_ret_yds", "pages"),
    MapSpec("pfr_player_returns", "kickoff_return_tds", "kick_ret_td", "pages"),
    MapSpec("pfr_player_returns", "punt_returns", "punt_ret", "pages"),
    MapSpec("pfr_player_returns", "punt_return_yards", "punt_ret_yds", "pages"),
    MapSpec("pfr_player_returns", "punt_return_tds", "punt_ret_td", "pages"),
    # ---- wave52 composites: the pages tables CARRY these -- derivation graded by witness ----
    MapSpec("pfr_player_season_rush_rec", "touches", "touches", "pages"),
    MapSpec("pfr_player_season_rec_rush", "touches", "touches", "pages"),
    MapSpec("pfr_player_season_rush_rec", "scrimmage_yards", "yds_from_scrimmage", "pages"),
    MapSpec("pfr_player_season_rec_rush", "scrimmage_yards", "yds_from_scrimmage", "pages"),
    MapSpec("pfr_player_season_rush_rec", "scrimmage_tds", "rush_receive_td", "pages"),
    MapSpec("pfr_player_season_rec_rush", "scrimmage_tds", "rush_receive_td", "pages"),
    MapSpec("pfr_player_returns", "all_purpose_yards", "all_purpose_yds", "pages",
            quirks="PFR AP = rush+rec+KR+PR+int-ret+fum-ret yards; coverage limited to "
                   "players with a returns page row; verdict grades OUR definition"),
    MapSpec("pfr_player_defense", "def_tackles_combined", "tackles_combined", "pages",
            quirks="tackle family is the known pages-vs-box intra-lineage split; measured"),
    MapSpec("pfr_player_scoring", "total_points_scored", "scoring", "pages",
            quirks="grades the wave52 points formula (scored TDs only -- total_tds_accounted_for "
                   "includes passing TDs and must not be used)"),
    MapSpec("pfr_player_fantasy", "fpts_4pt_0ppr", "fantasy_points", "pages",
            team_col="player",
            quirks="PFR FantPt = 4pt-pass-TD/0-PPR: EXTERNAL witness for the fantasy "
                   "formula stack 1970+; fantasy table has no team column (team_col "
                   "points at player so the nTM filter is a no-op); stint handling "
                   "imperfect -- verdict decides"),
    # ---- Phase-2 expansion: scoring pages (2nd pages stream -> intra-lineage checks) ----
    MapSpec("pfr_player_scoring", "rushing_tds", "rush_td", "pages"),
    MapSpec("pfr_player_scoring", "receiving_tds", "rec_td", "pages"),
    MapSpec("pfr_player_scoring", "fg_made", "fgm", "pages"),
    MapSpec("pfr_player_scoring", "pat_made", "xpm", "pages"),
    MapSpec("pfr_player_scoring", "def_safeties", "safety_md", "pages"),
    MapSpec("pfr_games_played", "games_played", "g", "pages", grain="season",
            agg="value", team_col="team",
            quirks="appearance witness (PPG denominators); the canonical lives only at "
                   "season/career grain, so validate against the season table. PARTIAL "
                   "scrape: 5,815 players; year_id='Career' rows excluded by the "
                   "4-digit regex"),
    # ---- NFL.com career pages: the SECOND appearance witness, at season grain ----
    # These two were EXCLUDED for a year on the reason "a games COUNT is a season aggregate;
    # the v26 subject is player-WEEK grain and has no such column by construction". False on
    # both counts: player_nfl_season carries games_played over 112,632 rows 1920-2025, and
    # the pfr_games_played spec directly above had been validating against that very table
    # the whole time. Joe: "spec is great for season and for career. those are numbers we
    # want. not sure the problem" -- there was none.
    #
    # witness_agg="sum" is load-bearing. agg="value" points the v26 side at the STORED season
    # value; the career page carries ONE ROW PER TEAM, so the source side must SUM a traded
    # player's rows. ANY_VALUE would take one team's games as the season total.
    MapSpec("nflcom_player_career", "games_played", "g", "nflcom", grain="season",
            agg="value", witness_agg="sum", source_table="Defense Career",
            table_col="_table", team_col="nflcom_slug",
            quirks="g/gs are FRAME columns of the shared wide career schema -- published on "
                   "~100% of rows of all 7 position tables with identical values, so one "
                   "spec measures them; per-table specs would re-measure the same numbers. "
                   "Measured 2026-07-31: 2025 alone 100.00%, 2010+ 93.16%, ALL 62.95%. The "
                   "residual is one-sided (nflcom higher on 30,296 rows, v26 on 167) because "
                   "v26 holds a row only for weeks it captured, so its games_played counts "
                   "weeks-we-have, not games-played"),
    MapSpec("nflcom_player_career", "games_started", "gs", "nflcom", grain="season",
            agg="value", witness_agg="sum", source_table="Defense Career",
            table_col="_table", team_col="nflcom_slug",
            quirks="same frame column and same grain argument as `g`. Measured 2026-07-31: "
                   "2025 99.69%, 2010+ 99.70%, 2000-09 93.29%, ALL 81.41%. games_started is "
                   "73.6% populated at season grain, so the denominator is the populated "
                   "subset, not every player-season"),
    # ---- PFR per-game boxscores (game grain, summed to season for votes) ----
    MapSpec("pfr_player_offense_box", "passing_yards", "pass_yds", "box"),
    MapSpec("pfr_player_offense_box", "rushing_yards", "rush_yds", "box"),
    MapSpec("pfr_player_offense_box", "receiving_yards", "rec_yds", "box"),
    MapSpec("pfr_player_offense_box", "passing_interceptions", "pass_int", "box"),
    MapSpec("pfr_player_defense_box", "def_interceptions", "def_int", "box", blank_zero=True,
            quirks="pre-2016 boxes render 0 INT as blank on rows with populated tackle/sack "
                   "cells (24,700 blanks 2005-15, one after); blank row-cell = 0"),
    MapSpec("pfr_box_kicking", "fg_made", "fgm", "box", blank_zero=True,
            quirks="kicking box mixes kickers+punters; punter rows leave fg cells blank = 0 "
                   "(the 50.4% class: every disagreement was witness-NULL vs v26 0)"),
    MapSpec("pfr_box_kicking", "punt_yards", "punt_yds", "box"),
    # ---- Phase-2 expansion: offense box ----
    MapSpec("pfr_player_offense_box", "passing_tds", "pass_td", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "completions", "pass_cmp", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "attempts", "pass_att", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "carries", "rush_att", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "rushing_tds", "rush_td", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "receptions", "rec", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "targets", "targets", "box", blank_zero=True,
            quirks="era guard abstains pre-targets years (all-blank column)"),
    MapSpec("pfr_player_offense_box", "receiving_tds", "rec_td", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "fumbles", "fumbles", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "fumbles_lost", "fumbles_lost", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "sacks_suffered", "pass_sacked", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "sack_yards_lost", "pass_sacked_yds", "box", blank_zero=True),
    MapSpec("pfr_player_offense_box", "passing_long", "pass_long", "box", agg="max",
            blank_zero=True,
            quirks="game long is a direct value; season long = MAX, never SUM. Measured: "
                   "blank long on an existing row = no qualifying play = 0 (869 w-NULL "
                   "atoms vs v26 0, zero value diffs beyond 3) -- same convention as "
                   "punter fg cells, era-guarded"),
    MapSpec("pfr_player_offense_box", "rushing_long", "rush_long", "box", agg="max",
            blank_zero=True),
    MapSpec("pfr_player_offense_box", "receiving_long", "rec_long", "box", agg="max",
            blank_zero=True),
    # ---- Phase-2 expansion: defense box ----
    MapSpec("pfr_player_defense_box", "def_sacks", "sacks", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_interception_yards", "def_int_yds", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_int_ret_td", "def_int_td", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_pass_defended", "pass_defended", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_fumbles_forced", "fumbles_forced", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_tackles_solo", "tackles_solo", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_tackle_assists", "tackles_assists", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_tackles_for_loss", "tackles_loss", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "def_qb_hits", "qb_hits", "box", blank_zero=True),
    MapSpec("pfr_player_defense_box", "fum_rec", "fumbles_rec", "box", blank_zero=True,
            quirks="def_fumbles semantics pin PENDING (refinement queue 0b)"),
    # ---- Phase-2 expansion: kicking box ----
    MapSpec("pfr_box_kicking", "fg_att", "fga", "box", blank_zero=True),
    MapSpec("pfr_box_kicking", "pat_made", "xpm", "box", blank_zero=True),
    MapSpec("pfr_box_kicking", "pat_att", "xpa", "box", blank_zero=True),
    MapSpec("pfr_box_kicking", "punts", "punt", "box", blank_zero=True),
    MapSpec("pfr_box_kicking", "punt_long", "punt_long", "box", agg="max", blank_zero=True,
            quirks="season long = MAX of game longs"),
    # ---- Phase-2 expansion: returns box ----
    MapSpec("pfr_box_returns", "kickoff_returns", "kick_ret", "box", blank_zero=True),
    MapSpec("pfr_box_returns", "kickoff_return_yards", "kick_ret_yds", "box", blank_zero=True),
    MapSpec("pfr_box_returns", "kickoff_return_tds", "kick_ret_td", "box", blank_zero=True),
    MapSpec("pfr_box_returns", "punt_returns", "punt_ret", "box", blank_zero=True),
    MapSpec("pfr_box_returns", "punt_return_yards", "punt_ret_yds", "box", blank_zero=True),
    MapSpec("pfr_box_returns", "punt_return_tds", "punt_ret_td", "box", blank_zero=True),
    # ---- PBP rollup (week grain, flat) ----
    MapSpec("pbp_player_week_rollup", "passing_yards", "passing_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "rushing_yards", "rushing_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "receiving_yards", "receiving_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "def_interceptions", "def_interceptions", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_made", "fg_made", "flat"),
    # ---- Phase-2 expansion: PBP rollup (identity names; independent lineage 1999+) ----
    MapSpec("pbp_player_week_rollup", "passing_tds", "passing_tds", "flat"),
    MapSpec("pbp_player_week_rollup", "completions", "completions", "flat"),
    MapSpec("pbp_player_week_rollup", "attempts", "attempts", "flat"),
    MapSpec("pbp_player_week_rollup", "sacks_suffered", "sacks_suffered", "flat"),
    MapSpec("pbp_player_week_rollup", "carries", "carries", "flat"),
    MapSpec("pbp_player_week_rollup", "rushing_tds", "rushing_tds", "flat"),
    MapSpec("pbp_player_week_rollup", "receptions", "receptions", "flat"),
    MapSpec("pbp_player_week_rollup", "targets", "targets", "flat"),
    MapSpec("pbp_player_week_rollup", "receiving_tds", "receiving_tds", "flat"),
    MapSpec("pbp_player_week_rollup", "fumbles", "fumbles", "flat"),
    MapSpec("pbp_player_week_rollup", "fumbles_lost", "fumbles_lost", "flat"),
    MapSpec("pbp_player_week_rollup", "fum_rec", "fum_rec", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_att", "fg_att", "flat"),
    MapSpec("pbp_player_week_rollup", "pat_made", "pat_made", "flat"),
    MapSpec("pbp_player_week_rollup", "pat_att", "pat_att", "flat"),
    MapSpec("pbp_player_week_rollup", "punts", "punts", "flat"),
    MapSpec("pbp_player_week_rollup", "punt_yards", "punt_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "punt_long", "punt_long", "flat", agg="max"),
    MapSpec("pbp_player_week_rollup", "def_sacks", "def_sacks", "flat"),
    MapSpec("pbp_player_week_rollup", "def_interception_yards", "def_interception_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "def_int_ret_td", "def_int_ret_td", "flat"),
    MapSpec("pbp_player_week_rollup", "def_fumbles_forced", "def_fumbles_forced", "flat"),
    MapSpec("pbp_player_week_rollup", "def_tackles_solo", "def_tackles_solo", "flat"),
    MapSpec("pbp_player_week_rollup", "def_tackle_assists", "def_tackle_assists", "flat"),
    MapSpec("pbp_player_week_rollup", "def_pass_defended", "def_pass_defended", "flat"),
    MapSpec("pbp_player_week_rollup", "def_qb_hits", "def_qb_hits", "flat"),
    MapSpec("pbp_player_week_rollup", "def_safeties", "def_safeties", "flat"),
    MapSpec("pbp_player_week_rollup", "kickoff_returns", "kickoff_returns", "flat"),
    MapSpec("pbp_player_week_rollup", "kickoff_return_yards", "kickoff_return_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "kickoff_return_tds", "kickoff_return_tds", "flat"),
    MapSpec("pbp_player_week_rollup", "punt_returns", "punt_returns", "flat"),
    MapSpec("pbp_player_week_rollup", "punt_return_yards", "punt_return_yards", "flat"),
    MapSpec("pbp_player_week_rollup", "punt_return_tds", "punt_return_tds", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_made_0_19", "fg_made_0_19", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_made_20_29", "fg_made_20_29", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_made_30_39", "fg_made_30_39", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_made_40_49", "fg_made_40_49", "flat"),
    MapSpec("pbp_player_week_rollup", "fg_made_50_59", "fg_made_50_59", "flat"),
    # ---- POST pages: playoff season totals (v26 POST stratum, first licensing) ----
    MapSpec("pfr_passing_post", "passing_yards", "pass_yds", "pages", season_type="POST"),
    MapSpec("pfr_passing_post", "passing_tds", "pass_td", "pages", season_type="POST"),
    MapSpec("pfr_passing_post", "passing_interceptions", "pass_int", "pages", season_type="POST"),
    MapSpec("pfr_passing_post", "completions", "pass_cmp", "pages", season_type="POST"),
    MapSpec("pfr_passing_post", "attempts", "pass_att", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "carries", "rush_att", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "rushing_yards", "rush_yds", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "rushing_tds", "rush_td", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "receptions", "rec", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "receiving_yards", "rec_yds", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "receiving_tds", "rec_td", "pages", season_type="POST"),
    MapSpec("pfr_rushrec_post", "targets", "targets", "pages", season_type="POST"),
    MapSpec("pfr_kicking_post", "fg_made", "fgm", "pages", season_type="POST"),
    MapSpec("pfr_kicking_post", "fg_att", "fga", "pages", season_type="POST"),
    MapSpec("pfr_kicking_post", "pat_made", "xpm", "pages", season_type="POST"),
    MapSpec("pfr_kicking_post", "pat_att", "xpa", "pages", season_type="POST"),
    MapSpec("pfr_defense_post", "def_interceptions", "def_int", "pages", season_type="POST"),
    MapSpec("pfr_defense_post", "def_sacks", "sacks", "pages", season_type="POST"),
    MapSpec("pfr_defense_post", "def_tackles_solo", "tackles_solo", "pages", season_type="POST"),
    # ================================================================================
    # O.8 (2026-07-26): the 13 MAPPING_PENDING value-witnesses get their MapSpecs.
    # Every one of these was registered, unmapped, and therefore silently unutilized
    # -- a licensed authority that could never vouch for anything. All are PFR
    # player-page tables (pfr_id + year_id), so they take the "pages" shape; the POST
    # stratum grades against v26 season_type='POST' and the advanced tables grade on
    # 2018-2024 (2025 excluded BY MEASUREMENT -- wk14+ air/EPA pending re-aggregation,
    # same carve-out the advanced BOX specs already use).
    #
    # INTRA-LINEAGE, NOT NEW ROOTS: these are all pfr-root. They add derivation-path
    # and grain redundancy plus POST-stratum coverage; they do NOT move root diversity.
    # ---- POST-stratum authorities: punting / returns / scoring / rec-rush ----
    MapSpec("pfr_punting_post", "punts", "punt", "pages", season_type="POST"),
    MapSpec("pfr_punting_post", "punt_yards", "punt_yds", "pages", season_type="POST"),
    MapSpec("pfr_punting_post", "punt_long", "punt_long", "pages", agg="max",
            season_type="POST",
            quirks="season long = MAX of weekly longs, never SUM (mirrors the REG spec)"),
    MapSpec("pfr_returns_post", "kickoff_returns", "kick_ret", "pages", season_type="POST"),
    MapSpec("pfr_returns_post", "kickoff_return_yards", "kick_ret_yds", "pages",
            season_type="POST"),
    MapSpec("pfr_returns_post", "kickoff_return_tds", "kick_ret_td", "pages",
            season_type="POST"),
    MapSpec("pfr_returns_post", "punt_returns", "punt_ret", "pages", season_type="POST"),
    MapSpec("pfr_returns_post", "punt_return_yards", "punt_ret_yds", "pages",
            season_type="POST"),
    MapSpec("pfr_returns_post", "punt_return_tds", "punt_ret_td", "pages",
            season_type="POST"),
    MapSpec("pfr_scoring_post", "rushing_tds", "rush_td", "pages", season_type="POST"),
    MapSpec("pfr_scoring_post", "receiving_tds", "rec_td", "pages", season_type="POST"),
    MapSpec("pfr_scoring_post", "fg_made", "fgm", "pages", season_type="POST"),
    MapSpec("pfr_scoring_post", "pat_made", "xpm", "pages", season_type="POST"),
    MapSpec("pfr_scoring_post", "pat_att", "xpa", "pages", season_type="POST"),
    MapSpec("pfr_scoring_post", "def_safeties", "safety_md", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "receptions", "rec", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "receiving_yards", "rec_yds", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "receiving_tds", "rec_td", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "targets", "targets", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "carries", "rush_att", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "rushing_yards", "rush_yds", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "rushing_tds", "rush_td", "pages", season_type="POST"),
    MapSpec("pfr_recrush_post", "fumbles", "fumbles", "pages", season_type="POST"),
    # ---- advanced SEASON tables (2018+): season-grain twins of the advanced BOX specs
    MapSpec("pfr_passing_adv_season", "passing_completed_air_yards", "pass_air_yds",
            "pages", validation_years=(2018, 2024),
            quirks="PFR air yds = COMPLETED air yards (wave54 split the column); "
                   "season-grain twin of the pfr_box_passing_advanced spec"),
    MapSpec("pfr_passing_adv_season", "passing_drops", "pass_drops", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_passing_adv_season", "passing_blitzed", "pass_blitzed", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_passing_adv_season", "passing_hurried", "pass_hurried", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_passing_adv_season", "passing_pressured", "pass_pressured", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_recrush", "receiving_completed_air_yards", "rec_air_yds", "pages",
            validation_years=(2018, 2024), quirks="COMPLETED air yards (see passing note)"),
    MapSpec("pfr_adv_recrush", "receiving_drops", "rec_drops", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_recrush", "receiving_broken_tackles", "rec_broken_tackles", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_rushrec", "rushing_broken_tackles", "rush_broken_tackles", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense", "def_pressures", "pressures", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense", "def_blitzes", "blitzes", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense", "def_hurries", "qb_hurry", "pages",
            validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense", "def_air_yards_allowed", "def_air_yds", "pages",
            validation_years=(2018, 2024),
            quirks="definition-version question open on this column (burn-down cell, "
                   "Joe's queue) -- the mapping is graded regardless"),
    # ---- advanced POST tables (2018+) ----
    MapSpec("pfr_passing_adv_post", "passing_completed_air_yards", "pass_air_yds",
            "pages", season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_passing_adv_post", "passing_drops", "pass_drops", "pages",
            season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_adv_recrush_post", "receiving_drops", "rec_drops", "pages",
            season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_adv_recrush_post", "receiving_broken_tackles", "rec_broken_tackles",
            "pages", season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_adv_rushrec_post", "rushing_broken_tackles", "rush_broken_tackles",
            "pages", season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense_post", "def_pressures", "pressures", "pages",
            season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense_post", "def_blitzes", "blitzes", "pages",
            season_type="POST", validation_years=(2018, 2024)),
    MapSpec("pfr_adv_defense_post", "def_hurries", "qb_hurry", "pages",
            season_type="POST", validation_years=(2018, 2024)),
    # ---- adjusted-passing indices: PLAUSIBILITY oracle, not a stat copy ----
    # pfr_adj_passing carries era-adjusted INDEX columns (pass_rating_idx etc.), which
    # no canonical v26 column mirrors -- there is nothing to alias. It is licensed as a
    # lane witness (index-vs-raw monotonicity) rather than via a MapSpec; see
    # LANE_WITNESSED in test_mapping_obligation.py.
    # ---- PFA gamelog stream (ancient_pfa_gamelog: the pfa_player_gamelog rows of the
    # through_1978 composite bundle -- OQ-LR-5 per-stream registration, 2026-07-26):
    # cross-era licensing -- graded against LICENSED pages mappings on the 1932-54
    # overlap, because no modern stratum exists. Independent pfa_loc lineage: takes
    # pre-1957 from depth-1 to depth-2 wherever it overlaps PFR-sourced rows. The
    # bundle_source filter is MANDATORY (source-definition row scope). ----
    MapSpec("ancient_pfa_gamelog", "passing_yards", "passing_yards", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_passing", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "passing_tds", "passing_tds", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_passing", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "passing_interceptions", "passing_interceptions", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_passing", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "completions", "completions", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_passing", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "attempts", "attempts", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_passing", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "rushing_yards", "rushing_yards", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_rec_rush", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "carries", "carries", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_rec_rush", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "rushing_tds", "rushing_tds", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_rec_rush", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "receiving_yards", "receiving_yards", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_rec_rush", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "receptions", "receptions", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_rec_rush", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "receiving_tds", "receiving_tds", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_season_rec_rush", validation_years=(1932, 1975)),
    MapSpec("ancient_pfa_gamelog", "fg_made", "fg_made", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_kicking", validation_years=(1938, 1975)),
    MapSpec("ancient_pfa_gamelog", "pat_made", "pat_made", "flat",
            filters="r.bundle_source LIKE '%pfa_player_gamelog%' AND r.season_type = 'REG'",
            validation_referee="pfr_player_kicking", validation_years=(1938, 1975)),
    # ---- PFR advanced box (2018+): the INDEPENDENT witnesses for the advanced family
    # (nflverse weekly is pbp-lineage = substantially self-agreement). 2025 excluded
    # from validation BY MEASUREMENT (wk14+ air/EPA empty pending re-aggregation). ----
    MapSpec("pfr_box_passing_advanced", "passing_completed_air_yards", "pass_air_yds",
            "box", validation_years=(2018, 2024),
            quirks="PFR box air yds = COMPLETED air yards (the 16.8% mismatch vs "
                   "intended was definitional; wave54 split the column)"),
    MapSpec("pfr_box_passing_advanced", "passing_drops", "pass_drops", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_passing_advanced", "passing_blitzed", "pass_blitzed", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_passing_advanced", "passing_hurried", "pass_hurried", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_passing_advanced", "passing_pressured", "pass_pressured", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_receiving_advanced", "receiving_completed_air_yards", "rec_air_yds",
            "box", validation_years=(2018, 2024),
            quirks="COMPLETED air yards (see passing note)"),
    MapSpec("pfr_box_receiving_advanced", "receiving_drops", "rec_drops", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_receiving_advanced", "receiving_broken_tackles", "rec_broken_tackles",
            "box", validation_years=(2018, 2024)),
    MapSpec("pfr_box_rushing_advanced", "rushing_broken_tackles", "rush_broken_tackles",
            "box", validation_years=(2018, 2024)),
    MapSpec("pfr_box_defense_advanced", "def_pressures", "pressures", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_defense_advanced", "def_blitzes", "blitzes", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_defense_advanced", "def_hurries", "qb_hurry", "box",
            validation_years=(2018, 2024)),
    MapSpec("pfr_box_defense_advanced", "def_air_yards_allowed", "def_air_yds", "box",
            validation_years=(2018, 2024),
            quirks="coverage air yards allowed; semantics measured, not assumed"),
    # ---- NGS weekly raw (2016+): SAME lineage as the v26 ngs_* columns -- this is a
    # LOAD-FIDELITY check at native grain (catches load corruption + the staleness
    # class), never independent testimony. Week-grain compare. ----
    *[
        MapSpec("ngs_weekly_raw", column, column, "flat", agg="value",
                validation_grain="week", validation_years=(2016, 2024))
        for column in (
            "ngs_avg_cushion", "ngs_avg_separation", "ngs_avg_yac",
            "ngs_avg_expected_yac", "ngs_avg_yac_above_expectation",
            "ngs_pct_share_intended_air_yards", "ngs_rush_efficiency",
            "ngs_pct_att_gte_8_defenders", "ngs_avg_time_to_los",
            "ngs_expected_rush_yards", "ngs_rush_yards_over_expected",
            "ngs_rush_pct_over_expected", "ngs_avg_time_to_throw",
            "ngs_aggressiveness", "ngs_avg_air_yards_to_sticks",
            "ngs_expected_completion_pct", "ngs_completion_pct_above_expectation",
            "ngs_avg_air_yards_differential",
        )
    ],
    # ---- NGS season (flat, identity: columns already share v26 names + units) ----
    *[
        MapSpec("ngs_season_published", column, column, "flat", agg="value",
                quirks="published season value: direct NGS value, never re-aggregated")
        for column in (
            "ngs_avg_cushion", "ngs_avg_separation", "ngs_avg_yac",
            "ngs_avg_expected_yac", "ngs_avg_yac_above_expectation",
            "ngs_pct_share_intended_air_yards", "ngs_rush_efficiency",
            "ngs_pct_att_gte_8_defenders", "ngs_avg_time_to_los",
            "ngs_expected_rush_yards", "ngs_rush_yards_over_expected",
            "ngs_rush_pct_over_expected", "ngs_avg_time_to_throw",
            "ngs_aggressiveness", "ngs_avg_air_yards_to_sticks",
            "ngs_expected_completion_pct", "ngs_completion_pct_above_expectation",
            "ngs_avg_air_yards_differential",
        )
    ],
]

# THE SAME g/gs PAIR ON EVERY REMAINING CAREER TABLE, built in a loop rather than typed
# seven times. Defense Career's pair is written out above with the full argument; these are
# the identical mapping on the identical frame columns, and the ONLY thing that differs is
# which page the row came from.
#
# THEY ARE MEASURED PER TABLE AND NOT SHARED, and that is deliberate. g/gs look like frame
# columns of the shared wide career schema -- published on ~100% of rows of all seven
# position tables -- so one spec looked sufficient. Measured: of 80,828 (slug, season) pairs
# appearing on more than one career table, only 97.43% carry IDENTICAL g AND gs. 2,079
# diverge, so a single spec would silently speak for values it never saw. The divergence is
# itself an open question (the per-table g may count games AT THAT POSITION), and per-table
# licences are what will localise it.
HAND_WITNESS_MAP += [
    MapSpec("nflcom_player_career", canon, col, "nflcom", grain="season",
            agg="value", witness_agg="sum", source_table=tbl,
            table_col="_table", team_col="nflcom_slug",
            quirks=f"appearance witness on {tbl}. agg='value' points the v26 side at the "
                   f"STORED season plane (player_nfl_season carries {canon} over 112,632 "
                   f"rows, 1920-2025); witness_agg='sum' because the career page is "
                   f"PER-TEAM and a traded player's rows must be summed -- ANY_VALUE would "
                   f"take one team's games as the season total. Measured per table rather "
                   f"than shared with Defense Career: g/gs agree across career tables on "
                   f"only 97.43% of player-seasons")
    for tbl in ("QB Career", "RBFB Career", "WRTE Career", "K Career", "P Career",
                "Offensive Line Career")
    for col, canon in (("g", "games_played"), ("gs", "games_started"))
]

# ---- NFL.com weekly passing-rate witnesses (2026-08-05) -------------------
# The regular-season QB gamelog has one row per player-week and publishes the
# component fields needed for these rates.  These are explicit expressions,
# not aliases of the season pages; the row filter is the measured QB passing
# block used by the existing weekly Y/A specs.
_NFLCOM_QB_LOG_FILTER = (
    "NULLIF(TRIM(CAST(comp AS VARCHAR)), '') IS NOT NULL OR "
    "NULLIF(TRIM(CAST(rate AS VARCHAR)), '') IS NOT NULL OR "
    "NULLIF(TRIM(CAST(scky AS VARCHAR)), '') IS NOT NULL"
)
_NFLCOM_QB_RATE_SPECS = {
    "passing_adjusted_yards_per_attempt":
        "(TRY_CAST(r.yds AS DOUBLE)+20*TRY_CAST(r.td AS DOUBLE)"
        "-45*TRY_CAST(r.int AS DOUBLE))/NULLIF(TRY_CAST(r.att AS DOUBLE),0)",
    "passing_adjusted_net_yards_per_attempt":
        "(TRY_CAST(r.yds AS DOUBLE)+20*TRY_CAST(r.td AS DOUBLE)"
        "-45*TRY_CAST(r.int AS DOUBLE)-TRY_CAST(r.scky AS DOUBLE))"
        "/NULLIF(TRY_CAST(r.att AS DOUBLE)+TRY_CAST(r.sck AS DOUBLE),0)",
    "passing_net_yards_per_attempt":
        "(TRY_CAST(r.yds AS DOUBLE)-TRY_CAST(r.scky AS DOUBLE))"
        "/NULLIF(TRY_CAST(r.att AS DOUBLE)+TRY_CAST(r.sck AS DOUBLE),0)",
    "passing_td_pct":
        "ROUND(100.0*TRY_CAST(r.td AS DOUBLE)/NULLIF(TRY_CAST(r.att AS DOUBLE),0), 2)",
}
HAND_WITNESS_MAP += [
    # The synthetic source_col keeps this expression path distinct from the
    # physical `yds` MapSpecs already generated for the same log source.
    MapSpec("nflcom_player_logs", canonical, "__qb_rate_expression__", "nflcom_log_week",
            agg="value", grain="week", validation_grain="week",
            source_table="Regular Season", table_col="_table",
            row_filter=_NFLCOM_QB_LOG_FILTER, source_expr=expression,
            validation_years=(1999, 2024),
            team_col="nflcom_slug",
            quirks="explicit QB gamelog rate expression; weekly NFL.com root; "
                    "component columns are restricted to the measured passing block")
    for canonical, expression in _NFLCOM_QB_RATE_SPECS.items()
]

# ---- independent PFR play-by-play player-week witness ---------------------
# This lane is rebuilt from the local PFR box-score PBP capture.  It is a PFR
# root witness, not a second name for the nflverse/PBP rollup.
HAND_WITNESS_MAP += [
    MapSpec("pfr_pbp_player_week_witness", canonical, canonical,
            "pfr_pbp_player_week", agg="value", grain="week",
            validation_grain="week", validation_years=(1999, 2024),
            source_expr=f"TRY_CAST(r.{canonical} AS DOUBLE)",
            quirks="derived from independent PFR play-by-play capture; "
                    "validation is modern REG only")
    for canonical in (
        "pass_explosive_20", "pass_success", "pass_success_plays",
        "passing_epa", "rz_pass_att",
    )
]
# The PFR box-score player table has a cleaner denominator than PBP text
# parsing: published pass attempts plus published sacks suffered.
HAND_WITNESS_MAP += [
    MapSpec("pfr_pbp_player_week_witness", "pass_success_plays",
            "pass_success_plays_box", "pfr_pbp_player_week", agg="value",
            grain="week", validation_grain="week",
            validation_years=(1999, 2024),
            source_expr="TRY_CAST(r.pass_success_plays_box AS DOUBLE)",
            quirks="PFR player-offense box denominator: pass_att + pass_sacked; "
                    "preferred over the PBP-text denominator")
]

# ---- NFL.com player-page position root (2026-08-05) -----------------------
# One source root, one slug-season vote. The parsed artifact has three page kinds
# (logs/splits/situational); audit_nflcom_position_root.py proves they agree before
# this lane is allowed to load. NFL.com emits one position token and cannot witness
# fantasy dual eligibility.
HAND_WITNESS_MAP += [
    MapSpec("nflcom_player_page_positions", "nfl_position", "position_raw", "nflcom_position",
            agg="value", grain="season", source_expr="position_raw",
            quirks="single-position page witness; page kinds collapse within nflcom root; no fantasy eligibility authority"),
    MapSpec("nflcom_player_page_positions", "position", "position_raw", "nflcom_position",
            agg="value", grain="season", source_expr="broad(position_raw)",
            quirks="single-position broad comparison only; source does not declare dual eligibility"),
]

# THE ONE PLACE RECENT GAMES IS NOT REDUNDANT, and my own supplementary ruling could not see
# it. That ruling downgraded the caption because "of the 39 canonicals this caption witnesses,
# ZERO have no other witness" -- but it counted only canonicals ALREADY MAPPED from the
# caption, and `gs` had only ever been mapped to games_started at SEASON grain. So a
# redundancy claim was measured against what the source was WIRED to rather than what it COULD
# witness, and it buried the one canonical with no witness at all: `is_starter` had ZERO specs
# and ZERO licence rows.
#
# On Recent Games g/gs are FLAGS, not counts -- g is 1.0 on 856 rows and 0.0 on 16; gs is 1.0
# on 195 and 0.0 on 677 -- so `gs` is a WEEK-GRAIN STARTED FLAG, a different statistic from
# the career tables' season counts. Measured 0.9670 on n=698, VALIDATED. OL block only, which
# is why it survived: offensive linemen have no other stats, so g/gs is all that block
# publishes.
#
# agg="value" is load-bearing twice: is_starter is NON_AGGREGATABLE so a summing spec is
# refused outright, and the shape's aggregate is now derived from it rather than hardcoded.
HAND_WITNESS_MAP += [
    MapSpec("nflcom_player_career", "is_starter", "gs", "nflcom_week", grain="week",
            agg="value", source_table="OL", table_col="_rg_block", team_col="nflcom_slug",
            quirks="week-grain STARTED FLAG (0/1), not a count. The ONLY witness for "
                   "is_starter, which had none. 0.9670 on n=698; the 23 disagreements are "
                   "rows where nflcom publishes a flag and v26 holds NULL -- ours, not "
                   "theirs, and witness_map deliberately refuses to filter them out"),
]

# ---------------------------------------------------------------------------------------
# GENERATED value paths.
#
# `HAND_WITNESS_MAP` above is ARGUED: every spec there was written by a person who looked
# at the source. The generated block is DERIVED: its (source_col -> v26_col) pairs come
# from the column dossier's receipted MAPPED_TO_CANONICAL decisions, its `agg` from
# stat_contracts.aggregation_class, and its shape from a per-source declaration. See
# scripts/sota_recon/mapspec_generator.py for why each of those three refuses rather than
# defaults.
#
# THE TWO LISTS MUST STAY SEPARATE. mapspec_generator reads its per-source shape
# declaration off the HAND specs. If generated specs fed back into that base, a source's
# shape would become self-justifying -- the generator would "read off an existing hand
# spec" that it had itself written. Importers that want the argued base ask for
# HAND_WITNESS_MAP; importers that want every executable value path ask for WITNESS_MAP.
#
# A spec existing here is NOT a license to vouch. Licensing is `licensed()` below, which
# reads measured agreement out of MAPPING_LICENSES.json. Generated specs enter UNVALIDATED
# and stay mute until they are measured.
try:
    from .witness_map_generated import GENERATED_SPECS
except ImportError:      # not yet applied -- the generator has never been run with --apply
    GENERATED_SPECS: list[dict] = []

# ADJUDICATED SPEC OVERRIDES (2026-08-02 tackle ruling, executable form).
# nflcom's own `total` != solo+ast on ~49% of its rows in EVERY log table
# (ST tackles folded in), so def_tackles_combined maps to the COMPUTED SUM
# solo+ast, never to `total`. Applied at LOAD time so the ruling survives
# every regeneration of witness_map_generated. Blank handling follows the
# empty-string adjudication: both components blank => ABSTAIN (NULL), one
# blank => 0, mirroring the surface's own identity where published.
def _solo_ast(alias: str) -> str:
    solo = f"NULLIF(TRIM(CAST({alias}.solo AS VARCHAR)), '')"
    ast = f"NULLIF(TRIM(CAST({alias}.ast AS VARCHAR)), '')"
    return (f"CASE WHEN {solo} IS NULL AND {ast} IS NULL THEN NULL "
            f"ELSE COALESCE(TRY_CAST({solo} AS DOUBLE), 0) "
            f"+ COALESCE(TRY_CAST({ast} AS DOUBLE), 0) END")


# EMPTY 2026-08-03: the tackle override was REFUTED by its own re-vouch.
# Computed solo+ast scored 0.17-0.74 against the plane where raw `total`
# held 0.9995 -- because OUR tackle family (pbp credit arrays) counts every
# play type INCLUDING special teams, and so does nflcom's gamelog `total`;
# nflcom's solo/ast are defense-phase-only. DEFINITION_DIFFERS by phase
# scope, not a mapping defect. total -> def_tackles_combined STANDS.
# The mechanism stays: it is how the wrong ruling was caught executably.
# 2026-08-03 scanner re-maps -- MEASURED, not name-guessed (the wrong-column
# scanner tested every candidate plane column against each near-zero lane):
#   rush_success_plays lane matches rush_success at 0.9897 (n=1750)
#   rec_success_plays lane matches rec_success at 0.9889 (n=3336)
#   total_yds_allowed lane matches def_yards_allowed at 1.0000 (n=408)
ADJUDICATED_SPEC_OVERRIDES: dict[tuple[str, str, str], dict] = {
    # 2026-08-04 TD-family mapspec re-audit (Joe: "have we re-mapspecced the
    # columns we changed?"). Direction-tested against every TD column:
    # pfr_box_scoring's parsed description measures TDs SCORED (total_tds_scored
    # 0.9766) not TDs accounted-for (total_tds_accounted_for 0.7481, its declared target).
    ("pfr_box_scoring", "total_tds_accounted_for", "description"):
        {"v26_col": "total_tds_scored"},
    ("pbp_merged_1978_2025", "rush_success_plays", "success"):
        {"v26_col": "rush_success"},
    ("pbp_merged_1978_2025", "rec_success_plays", "success"):
        {"v26_col": "rec_success"},
    ("pbp_merged_1978_2025", "total_yds_allowed", "yards_gained"):
        {"v26_col": "def_yards_allowed"},
}


def _adjudicate(spec: dict) -> dict:
    ov = ADJUDICATED_SPEC_OVERRIDES.get(
        (spec.get("source_key"), spec.get("v26_col"), spec.get("source_col")))
    return {**spec, **ov} if ov else spec


GENERATED_WITNESS_MAP: list[MapSpec] = [
    MapSpec(**_adjudicate(spec)) for spec in GENERATED_SPECS]

# adjudicated overrides apply to the WHOLE map -- hand specs carry mapping
# defects too (the 2026-08-03 scanner re-maps were all hand pbp specs)
import dataclasses as _dc


def _adjudicate_spec(sp: MapSpec) -> MapSpec:
    ov = ADJUDICATED_SPEC_OVERRIDES.get(
        (sp.source_key, sp.v26_col, sp.source_col))
    return _dc.replace(sp, **ov) if ov else sp


WITNESS_MAP: list[MapSpec] = [
    _adjudicate_spec(sp)
    for sp in [*HAND_WITNESS_MAP, *GENERATED_WITNESS_MAP]]


# TEXT COLUMNS CANNOT BE SCALED (2026-08-05). Every lane generator ended
# its value with `* {spec.scale}` -- a numeric coercion baked in because the
# harness was built for numeric stats. For a STRING column that is
# `VARCHAR * 1.0`, a binder error, so SIX position witnesses (PFA,
# newspaper OCR, the 1978 pbp recovery, ancient PFR, legacy supertable)
# raised on load and were silently counted as "no data". Position is our
# thickest-witnessed stat and most of its roots were dark for this reason.
STRING_V26_COLS = {"position", "nfl_position", "fantasy_position", "nfl_team",
                   "opponent_nfl_team", "season_type", "player", "data_source"}


def _scaled(expr: str, spec) -> str:
    """Apply the lane's scale, except on text columns where it is nonsense."""
    return expr + _scale_suffix(spec)


def _scale_suffix(spec) -> str:
    """` * <scale>`, or nothing at all when the column holds text."""
    if getattr(spec, "v26_col", None) in STRING_V26_COLS:
        return ""
    return f" * {spec.scale}"


def _q(key: str) -> str:
    p = Path(S.registry()[key].path)
    # sharded harvest sources register a DIRECTORY; DuckDB needs the glob
    # (Stage 0b: nflcom_team_season_roster and the ff_assets family)
    return f"{p.as_posix()}/**/*.parquet" if p.is_dir() else p.as_posix()


def outer_for(spec: MapSpec) -> str:
    """The SQL aggregate for a spec's declared agg. A rate never reaches here: the
    generator refuses any canonical the contract does not class SUM or MAX."""
    a = spec.witness_agg or spec.agg
    return "MAX" if a == "max" else ("ANY_VALUE" if a == "value" else "SUM")


def build_witness_sql(spec: MapSpec) -> str:
    """One generator for every lane: (pfr_id, yr, val) at season grain from a MapSpec."""
    f = f"AND ({spec.filters})" if spec.filters else ""
    _a = spec.witness_agg or spec.agg
    inner_agg = "MAX" if _a == "max" else ("ANY_VALUE" if _a == "value" else "SUM")
    if spec.shape == "pages":
        if spec.source_col == "pos":
            return f"""
            SELECT pfr_id,
                   TRY_CAST(regexp_extract(CAST(year_id AS VARCHAR), '(\\d{{4}})', 1) AS INT) AS yr,
                   ANY_VALUE(NULLIF(TRIM(CAST(pos AS VARCHAR)), '')) AS val
            FROM '{_q(spec.source_key)}'
            WHERE pfr_id IS NOT NULL AND pos IS NOT NULL
            GROUP BY 1, 2 HAVING yr IS NOT NULL"""
        outer = "MAX" if spec.agg in ("max", "value") else "SUM"
        # the EQUATION lane (Joe, 2026-08-01: "we have yards and attempts and sack
        # yards"): a pages spec may declare an expression over several columns of
        # the same row -- Y/A families, opportunities, return-yard sums.
        pg_val = spec.source_expr or f"TRY_CAST({spec.source_col} AS DOUBLE)"
        return f"""
        SELECT pfr_id,
               TRY_CAST(regexp_extract(CAST(year_id AS VARCHAR), '(\\d{{4}})', 1) AS INT) AS yr,
               {outer}(x) {_scale_suffix(spec)} AS val FROM (
          SELECT pfr_id, year_id, MAX({pg_val}) AS x
          FROM '{_q(spec.source_key)}'
          WHERE COALESCE(regexp_matches(CAST({spec.team_col} AS VARCHAR), '^[0-9]TM$'), FALSE) = FALSE
            AND ({pg_val}) IS NOT NULL {f}
          GROUP BY pfr_id, year_id)
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "box":
        if spec.v26_col == "game_date" and spec.source_key in BOX_PLAYER_CONTEXT_SOURCES:
            return f"""
            SELECT bio.pfr_id,
                   TRY_CAST(g.year AS INT) AS yr,
                   ANY_VALUE(CAST(TRY_CAST(r.game_date AS DATE) AS VARCHAR)) AS val
            FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) r
            JOIN read_parquet('{Path(S.PLAYER_BIO.path).as_posix()}') bio
              ON bio.pfr_id = regexp_extract(CAST(r.player_link_ids AS VARCHAR), '^([^,]+)', 1)
            JOIN read_parquet('{Path(S.TEAM_GAMES.path).as_posix()}') g
              ON g.boxscore_id = r.boxscore_id AND g.season_type = 'REG'
            WHERE r.player_link_ids IS NOT NULL AND r.game_date IS NOT NULL
            GROUP BY 1, 2 HAVING yr IS NOT NULL"""
        if spec.v26_col == "nfl_position":
            return f"""
            SELECT regexp_extract(CAST(player_link_ids AS VARCHAR), '^([^,]+)', 1) AS pfr_id,
                   TRY_CAST(g.year AS INT) AS yr,
                   ANY_VALUE(NULLIF(TRIM(CAST(pos AS VARCHAR)), '')) AS val
            FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) r
            JOIN read_parquet('{Path(S.TEAM_GAMES.path).as_posix()}') g
              ON g.boxscore_id = r.boxscore_id AND g.season_type = 'REG'
            WHERE r.player_link_ids IS NOT NULL AND r.pos IS NOT NULL
            GROUP BY 1, 2 HAVING yr IS NOT NULL"""
        # SEASON year comes from team_games, NEVER from the boxscore_id date prefix --
        # January games belong to the previous season (the Moss/Wilford class: one-game
        # yard shuffles across every season boundary, both directions).
        outer = "MAX" if spec.agg == "max" else "SUM"
        # blank_zero: PFR renders 0 as '' in some box columns (punter fg cells, pre-2016
        # def_int). The row's existence is the witness; a blank cell on it vouches 0.
        # ERA GUARD: a year whose column is blank on EVERY row means the column wasn't
        # tracked that year -- the witness ABSTAINS instead of fabricating zeros.
        x_expr = (f"COALESCE(TRY_CAST({spec.source_col} AS DOUBLE), 0)" if spec.blank_zero
                  else f"TRY_CAST({spec.source_col} AS DOUBLE)")
        notnull = "" if spec.blank_zero else f"AND {spec.source_col} IS NOT NULL"
        era_guard = ""
        if spec.blank_zero:
            era_guard = f"""
        JOIN (SELECT g2.yr FROM '{_q(spec.source_key)}' s2
              JOIN (SELECT DISTINCT boxscore_id, year AS yr
                    FROM '{Path(S.TEAM_GAMES.path).as_posix()}'
                    WHERE season_type = 'REG') g2 USING (boxscore_id)
              GROUP BY 1
              HAVING COUNT(TRY_CAST(s2.{spec.source_col} AS DOUBLE)) > 0) tracked
          ON tracked.yr = g.yr"""
        return f"""
        SELECT b.pid AS pfr_id, g.yr, {outer}(b.x) {_scale_suffix(spec)} AS val FROM (
          SELECT regexp_extract(player_link_ids, '^([^,]+)', 1) AS pid, boxscore_id,
                 {x_expr} AS x
          FROM '{_q(spec.source_key)}'
          WHERE player_link_ids IS NOT NULL {notnull} {f}) b
        JOIN (SELECT DISTINCT boxscore_id, year AS yr
              FROM '{Path(S.TEAM_GAMES.path).as_posix()}'
              WHERE season_type = 'REG') g USING (boxscore_id){era_guard}
        GROUP BY 1, 2"""
    if (spec.shape == "nflcom_log_week" and spec.v26_col == "game_date"
            and spec.source_key in NFLCOM_PLAYER_WEEK_CONTEXT_SOURCES):
        table_pred = (f"AND r.{spec.table_col} = '{spec.source_table.replace(chr(39), chr(39) * 2)}'"
                      if spec.table_col and spec.source_table else "")
        row_pred = f"AND ({spec.row_filter})" if spec.row_filter else ""
        xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
        return f"""
        SELECT bio.pfr_id, TRY_CAST(r.season AS INT) AS yr,
               ANY_VALUE(CAST(TRY_CAST(r.game_date AS DATE) AS VARCHAR)) AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) r
        JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
        JOIN read_parquet('{Path(S.PLAYER_BIO.path).as_posix()}') bio ON bio.pfr_id = x.pfr_id
        WHERE r.game_date IS NOT NULL {table_pred} {row_pred}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "nflcom_position":
        xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
        return f"""
        SELECT x.pfr_id, TRY_CAST(p.season AS INT) AS yr,
               ANY_VALUE(NULLIF(TRIM(CAST(p.position_raw AS VARCHAR)), '')) AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) p
        JOIN read_parquet('{xw.replace(chr(39), chr(39) * 2)}') x ON x.nflcom_slug = p.nflcom_slug
        WHERE p.position_raw IS NOT NULL
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "nflcom":
        # NFL.com keys on a url slug, not NFL_player_id, so it reaches pfr_id through the
        # receipted nflcom_slug_pfrid crosswalk rather than through player_bio.
        #
        # SELECT DISTINCT is not optional. nflcom_player_career carries 3,304 exact
        # duplicate rows (3.6%); summing over them depressed a CORRECT column's measured
        # agreement from 73.28% to 47.59%, which reads exactly like a wrong mapping.
        #
        # PUBLICATION IS A CAST QUESTION, NOT A NULL ONE (2026-07-31). NFL.com writes ''
        # where a column does not apply to a row -- Defense Career renders a tackle block
        # and an interception block into ONE physical schema and blanks the half that does
        # not apply, so publication runs 15.9% to 100% across its columns. `col IS NOT NULL`
        # passes those rows, SUM(TRY_CAST('')) over an all-blank group is NULL, and a NULL
        # witness value then scores as a DISAGREEMENT rather than as an abstention. That
        # alone read `opp_fr` down to 46.9%. 107 of 145 column-instances in player_career
        # carry the placeholder, so this is the rule for the shape, not a special case.
        #
        # The table selector and the row filter are both applied HERE, before aggregation.
        table_pred = (f"AND s.{spec.table_col} = '{spec.source_table}'"
                      if spec.source_table and spec.table_col else "")
        row_pred = f"AND ({spec.row_filter})" if spec.row_filter else ""
        slug = spec.team_col or "nflcom_slug"     # team_col carries the slug column name
        # composite cells: the raw column does not TRY_CAST (a "3/4" cell casts NULL,
        # which would filter EVERY row), so both the value and the abstention guard
        # must run through the declared extraction.
        val_expr = spec.source_expr or f'TRY_CAST(s."{spec.source_col}" AS DOUBLE)'
        xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
        return f"""
        SELECT x.pfr_id, TRY_CAST(s.season AS INT) AS yr,
               {outer_for(spec)}({val_expr}) {_scale_suffix(spec)} AS val
        FROM (SELECT DISTINCT * FROM read_parquet('{_q(spec.source_key)}', union_by_name=True)) s
        JOIN read_parquet('{xw}') x ON x.nflcom_slug = s.{slug}
        WHERE ({val_expr}) IS NOT NULL {table_pred} {row_pred} {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "appearance_games":
        # RETIRED TARGET, HONEST REPLACEMENT (2026-08-02): games_played can
        # NEVER come from this source -- its game_id is 100% NULL and the grain
        # measured one row per (player, team, season): a MEMBERSHIP table. What
        # membership witnesses is the season TEAM-STINT COUNT: roster team slug
        # -> nickname (last slug token) -> team code via the self-built nickname
        # crosswalk; player via the receipted slug crosswalk. Pair with
        # v26_expr COUNT(DISTINCT t.nfl_team) on the plane side.
        xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
        nick = (r"D:/league-history-data/nfl/derived/validation"
                r"/sota_recon_master/nickname_team_code_crosswalk.parquet")
        return f"""
        SELECT x.pfr_id, TRY_CAST(s.season AS INT) AS yr,
               {_scaled('COUNT(DISTINCT n.team_code)', spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) s
        JOIN read_parquet('{xw}') x ON x.nflcom_slug = s.source_player_id
        JOIN read_parquet('{nick}') n
          ON (LOWER(n.nickname) = regexp_extract(s.team, '([^-]+)$', 1)
              OR LOWER(REPLACE(n.nickname, ' ', '-')) =
                 regexp_extract(s.team, '([^-]+-[^-]+)$', 1))
         AND n.season = TRY_CAST(s.season AS INT) AND n.confidence >= 0.95
        WHERE s.team IS NOT NULL {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pfr_pbp_detail":
        # The pfr play-by-play `detail` text, scoring-log idiom: first link id is
        # the acting player, extraction declared per spec, REG scoped via the
        # catalog. Opens missed-FG distances ('52 yard field goal no good') and,
        # later, the fumble grammar.
        assert spec.source_expr, "pfr_pbp_detail specs require source_expr"
        return f"""
        SELECT regexp_extract(CAST(s.detail_link_ids AS VARCHAR), '^([^;,]+)', 1) AS pfr_id,
               TRY_CAST(s.season AS INT) AS yr,
               {outer_for(spec)}({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) s
        JOIN (SELECT DISTINCT boxscore_id FROM '{Path(S.TEAM_GAMES.path).as_posix()}'
              WHERE season_type = 'REG') g USING (boxscore_id)
        WHERE ({spec.source_expr}) IS NOT NULL
          AND s.detail_link_ids IS NOT NULL {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pfa_participation_games":
        # PFA per-game participation -> games per (player, season), keyed
        # through the 08-02 pfa crosswalk (90.2% profiles, twin-abstaining).
        # Unlike the nflcom roster, PFA game_ids are REAL, so this is the
        # genuine games lane the roster could never be.
        xw = (r"D:/league-history-data/nfl/derived/validation"
              r"/sota_recon_master/pfa_pfrid_crosswalk.parquet")
        return f"""
        SELECT x.pfr_id, TRY_CAST(s.season AS INT) AS yr,
               {_scaled('COUNT(DISTINCT s.game_id)', spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) s
        JOIN read_parquet('{xw}') x
          ON x.pfa_id = s.source_player_id AND x.status = 'MATCHED'
        WHERE s.game_id IS NOT NULL {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pbp_rollup_week":
        # pbp_rollup at WEEK grain: same attribution idiom (team_col carries
        # the credited-id column, filters the play predicate, source_expr the
        # per-play value) but keyed (pfr_id, yr, wk) so the correction engine
        # and the arbitration executor can place cells in specific weeks.
        # Added 2026-08-02 after hand-rolled executor SQL exposed that these
        # lanes were never specced (Joe: "THEY'RE ALL SUPPOSED TO BE
        # MAPSPECCED"). A lane that is not a MapSpec does not exist.
        assert spec.team_col and spec.source_expr, "needs attribution + expr"
        return f"""
        SELECT bio.pfr_id, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(r.week AS INT) AS wk,
               {outer_for(spec)}({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio
          ON bio.NFL_player_id = r.{spec.team_col}
        WHERE bio.pfr_id IS NOT NULL AND r.season_type = 'REG'
          AND ({spec.source_expr}) IS NOT NULL {f}
        GROUP BY 1, 2, 3 HAVING yr IS NOT NULL AND wk IS NOT NULL"""
    if spec.shape == "pfr_fumble_events":
        # The fumble grammar's derived event table (build_pfr_fumble_events.py):
        # one row per `X fumbles` sentence with typed context and box-derived
        # team sides. team_col names the credited actor id column; filters the
        # event predicate. Unknown sides leave `lost`/`rec_own` NULL and the
        # event abstains from lost-split specs (never COALESCE-0).
        assert spec.team_col, "pfr_fumble_events specs require team_col (actor)"
        ev_path = (r"D:/league-history-data/nfl/derived/validation"
                   r"/sota_recon_master/pfr_fumble_events.parquet")
        return f"""
        SELECT e.{spec.team_col} AS pfr_id, TRY_CAST(e.season AS INT) AS yr,
               {_scaled('COUNT(*)', spec)} AS val
        FROM read_parquet('{ev_path}') e
        WHERE e.{spec.team_col} IS NOT NULL {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pfr_scoring_log":
        # The boxscore Scoring log: one row per scoring play, "Kicker 32 yard field
        # goal", link ids semicolon-separated with the SCORER first. Every spec on
        # this shape declares its extraction via source_expr (there is no raw
        # numeric column); the TEAM_GAMES join scopes to REG and supplies nothing
        # else -- season comes from the row.
        assert spec.source_expr, "pfr_scoring_log specs require source_expr"
        # link-position attribution (round 2): table_col carries which link id is
        # the credited player -- '' / '1' = the scorer (first), '2' = second
        # (2pt runner after a TD), '-1' = last ('pass from Y' passer).
        idx = spec.table_col or "1"
        if idx == "-1":
            attr = ("regexp_extract(CAST(s.description_link_ids AS VARCHAR), "
                    "'([^;]+)$', 1)")
        elif idx == "-2":
            attr = ("regexp_extract(CAST(s.description_link_ids AS VARCHAR), "
                    "'([^;]+);[^;]+$', 1)")
        elif idx == "2":
            attr = ("regexp_extract(CAST(s.description_link_ids AS VARCHAR), "
                    "'^[^;]+;([^;]+)', 1)")
        else:
            attr = ("regexp_extract(CAST(s.description_link_ids AS VARCHAR), "
                    "'^([^;]+)', 1)")
        return f"""
        SELECT {attr} AS pfr_id,
               TRY_CAST(s.season AS INT) AS yr,
               {outer_for(spec)}({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) s
        JOIN (SELECT DISTINCT boxscore_id FROM '{Path(S.TEAM_GAMES.path).as_posix()}'
              WHERE season_type = 'REG') g USING (boxscore_id)
        WHERE ({spec.source_expr}) IS NOT NULL
          AND s.description_link_ids IS NOT NULL {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pbp_ratio":
        # Joe, 2026-08-01: 'why is completion percentage not mapped to pbp?' --
        # because RATE_VIA_COMPONENTS was an illegal CANNOT. A ratio of witnessed
        # components is still a pbp-computable value; source_expr here is the FULL
        # aggregate expression (SUM/COUNT FILTER ratio), no outer wrap.
        assert spec.source_expr and spec.agg == "value"
        return f"""
        SELECT bio.pfr_id, TRY_CAST(r.season AS INT) AS yr,
               ({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio
          ON bio.NFL_player_id = r.{spec.team_col}
        WHERE bio.pfr_id IS NOT NULL AND r.season_type = 'REG' {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL AND val IS NOT NULL"""
    if spec.shape == "statscrew_results":
        # StatsCrew game-by-game results: own team codes canonicalized via the
        # 08-02 code map (caption nickname -> catalog code, 98.75% coverage,
        # all cells 1.0 confidence); week resolved by catalog DATE match. The
        # score parse needs no side detection: W -> our points = max of the
        # two, L -> min, T -> equal either way.
        assert spec.source_expr, "statscrew_results specs require source_expr"
        scm = (r"D:/league-history-data/nfl/derived/validation"
               r"/sota_recon_master/statscrew_team_code_map.parquet")
        return f"""
        SELECT m.team_code AS team, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(g.week AS INT) AS wk,
               ANY_VALUE({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) r
        JOIN read_parquet('{scm}') m
          ON m.sc_code = str_split(r.team, ' ')[1]
         AND m.season = TRY_CAST(r.season AS INT) AND m.confidence >= 0.95
        JOIN '{Path(S.TEAM_GAMES.path).as_posix()}' g
          ON g.team_code = m.team_code AND g.year = TRY_CAST(r.season AS INT)
         AND g.game_date = strptime(r.date, '%B %d, %Y')
         AND g.season_type = 'REG'
        WHERE r.row_class = 'reg_season' AND ({spec.source_expr}) IS NOT NULL
        GROUP BY 1, 2, 3 HAVING val IS NOT NULL"""
    if spec.shape == "pbp_team_ratio":
        # team-grain ratio (def_passer_rating_allowed class), full aggregate
        # expression grouped by team_col (defteam unless the contraction events
        # live on the OTHER side of the ball -- dst_points_allowed groups by
        # posteam because a pick-six is thrown while we are the offense).
        assert spec.source_expr and spec.agg == "value"
        tr_team = (spec.team_col
                   if spec.team_col not in ("", "team_name_abbr") else "defteam")
        return f"""
        SELECT r.{tr_team} AS team, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(r.week AS INT) AS wk, ({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        WHERE r.season_type = 'REG' AND r.{tr_team} IS NOT NULL
        GROUP BY 1, 2, 3 HAVING val IS NOT NULL"""
    if spec.shape == "pbp_team_return":
        # dst return yards: the RETURNING team is defteam on punts, posteam on
        # kickoffs -- a two-branch team union.
        return f"""
        SELECT team, yr, wk, SUM(v) {_scale_suffix(spec)} AS val FROM (
          SELECT r.defteam AS team, TRY_CAST(r.season AS INT) AS yr,
                 TRY_CAST(r.week AS INT) AS wk,
                 TRY_CAST(r.return_yards AS DOUBLE) AS v
          FROM '{_q(spec.source_key)}' r
          WHERE r.season_type='REG' AND r.punt_attempt=1 AND r.return_yards IS NOT NULL
          UNION ALL
          SELECT r.posteam, TRY_CAST(r.season AS INT), TRY_CAST(r.week AS INT),
                 TRY_CAST(r.return_yards AS DOUBLE)
          FROM '{_q(spec.source_key)}' r
          WHERE r.season_type='REG' AND r.kickoff_attempt=1 AND r.return_yards IS NOT NULL
        ) u GROUP BY 1, 2, 3"""
    if spec.shape == "pbp_team_rollup":
        # Raw pbp rolled to (defteam, year, week) -- the team-DST derivation lane
        # (Joe: pbp derives ~everything to '78). Routes through the team_week
        # compare; nflverse team codes are v26's own vocabulary.
        assert spec.source_expr, "pbp_team_rollup needs source_expr"
        tcol = spec.team_col if spec.team_col not in ("", "team_name_abbr") else "defteam"
        return f"""
        SELECT r.{tcol} AS team, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(r.week AS INT) AS wk,
               {outer_for(spec)}({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        WHERE r.season_type = 'REG' AND r.{tcol} IS NOT NULL
          AND ({spec.source_expr}) IS NOT NULL {f}
        GROUP BY 1, 2, 3"""
    if spec.shape == "pbp_drive_rollup":
        # Drive-grain team stats (three_out class): dedupe plays to
        # (game, fixed_drive), credit the DEFENSE that forced the outcome.
        # source_expr = a drive-level predicate over the drive_* columns.
        assert spec.source_expr
        return f"""
        SELECT team, yr, wk, SUM(hit) {_scale_suffix(spec)} AS val FROM (
          SELECT r.game_id, r.fixed_drive,
                 ANY_VALUE(r.defteam) AS team,
                 ANY_VALUE(TRY_CAST(r.season AS INT)) AS yr,
                 ANY_VALUE(TRY_CAST(r.week AS INT)) AS wk,
                 MAX(CASE WHEN {spec.source_expr} THEN 1 ELSE 0 END) AS hit
          FROM '{_q(spec.source_key)}' r
          WHERE r.season_type = 'REG' AND r.defteam IS NOT NULL
            AND r.fixed_drive IS NOT NULL
          GROUP BY 1, 2) d
        GROUP BY 1, 2, 3"""
    if spec.shape == "pbp_share":
        # player numerator over TEAM denominator per season (target/air shares).
        # source_expr = numerator aggregate; filters unused; source_col names the
        # denominator aggregate via {den}.
        num, den = spec.source_expr.split("|DEN|")
        return f"""
        WITH per AS (
          SELECT bio.pfr_id, ANY_VALUE(r.posteam) AS team,
                 TRY_CAST(r.season AS INT) AS yr, {num} AS numer
          FROM '{_q(spec.source_key)}' r
          JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio
            ON bio.NFL_player_id = r.{spec.team_col}
          WHERE r.season_type = 'REG' GROUP BY 1, 3),
        tm AS (
          SELECT r.posteam AS team, TRY_CAST(r.season AS INT) AS yr, {den} AS denom
          FROM '{_q(spec.source_key)}' r
          WHERE r.season_type = 'REG' GROUP BY 1, 2)
        SELECT per.pfr_id, per.yr, per.numer / NULLIF(tm.denom, 0) {_scale_suffix(spec)} AS val
        FROM per JOIN tm USING (team, yr)"""
    if spec.shape == "pbp_total_role_union":
        # total_epa/total_wpa: a player's total = his passing (qb_epa on his
        # dropbacks) + rushing (epa on his rushes) + receiving (epa on his
        # targets). One witness, three UNION ALL role branches. source_col picks
        # the metric: 'epa' uses qb_epa on the passer branch, 'wpa' uses wpa
        # everywhere (measured, not assumed -- the audit grades the formula).
        METRICS = {
            "epa":   [("passer_player_id", "TRY_CAST(r.qb_epa AS DOUBLE)", "TRUE"),
                      ("rusher_player_id", "TRY_CAST(r.epa AS DOUBLE)", "TRUE"),
                      ("receiver_player_id", "TRY_CAST(r.epa AS DOUBLE)", "TRUE")],
            "wpa":   [("passer_player_id", "TRY_CAST(r.wpa AS DOUBLE)", "TRUE"),
                      ("rusher_player_id", "TRY_CAST(r.wpa AS DOUBLE)", "TRUE"),
                      ("receiver_player_id", "TRY_CAST(r.wpa AS DOUBLE)", "TRUE")],
            "scrimmage_yards": [
                ("rusher_player_id", "TRY_CAST(r.yards_gained AS DOUBLE)", "r.rush_attempt=1"),
                ("receiver_player_id", "TRY_CAST(r.yards_gained AS DOUBLE)", "r.complete_pass=1")],
            "scrimmage_tds": [
                ("rusher_player_id", "1", "r.rush_touchdown=1"),
                ("receiver_player_id", "1", "r.pass_touchdown=1")],
            "touches": [
                ("rusher_player_id", "1", "r.rush_attempt=1"),
                ("receiver_player_id", "1", "r.complete_pass=1")],
            "opportunities": [
                ("rusher_player_id", "1", "r.rush_attempt=1"),
                ("receiver_player_id", "1", "r.pass_attempt=1")],
            "turnovers": [
                ("passer_player_id", "1", "r.interception=1"),
                ("fumbled_1_player_id", "1", "r.fumble_lost=1")],
            "return_yards": [
                ("punt_returner_player_id", "TRY_CAST(r.return_yards AS DOUBLE)", "TRUE"),
                ("kickoff_returner_player_id", "TRY_CAST(r.return_yards AS DOUBLE)", "TRUE")],
            "all_purpose": [
                ("rusher_player_id", "TRY_CAST(r.yards_gained AS DOUBLE)", "r.rush_attempt=1"),
                ("receiver_player_id", "TRY_CAST(r.yards_gained AS DOUBLE)", "r.complete_pass=1"),
                ("punt_returner_player_id", "TRY_CAST(r.return_yards AS DOUBLE)", "TRUE"),
                ("kickoff_returner_player_id", "TRY_CAST(r.return_yards AS DOUBLE)", "TRUE")],
            "points_scored": [
                ("td_player_id", "6", "r.touchdown=1"),
                ("kicker_player_id", "3", "r.field_goal_result='made'"),
                ("kicker_player_id", "1", "r.extra_point_result='good'"),
                ("fumbled_1_player_id", "0", "FALSE")],
            "tackles_combined": [
                ("solo_tackle_1_player_id", "1", "TRUE"),
                ("solo_tackle_2_player_id", "1", "TRUE"),
                ("assist_tackle_1_player_id", "1", "TRUE"),
                ("assist_tackle_2_player_id", "1", "TRUE"),
                ("assist_tackle_3_player_id", "1", "TRUE"),
                ("assist_tackle_4_player_id", "1", "TRUE"),
                ("tackle_with_assist_1_player_id", "1", "TRUE"),
                ("tackle_with_assist_2_player_id", "1", "TRUE")],
            "tfl_yards": [
                ("tackle_for_loss_1_player_id",
                 "GREATEST(-TRY_CAST(r.yards_gained AS DOUBLE),0)", "TRUE"),
                ("tackle_for_loss_2_player_id",
                 "GREATEST(-TRY_CAST(r.yards_gained AS DOUBLE),0)", "TRUE")],
            "def_fumbles": [
                ("fumbled_1_player_id", "1", "r.fumbled_1_team = r.defteam")],
            "fum_rec_own": [
                ("fumble_recovery_1_player_id", "1",
                 "r.fumble_recovery_1_team = r.fumbled_1_team")],
            "fum_rec_opp": [
                ("fumble_recovery_1_player_id", "1",
                 "r.fumble_recovery_1_team <> r.fumbled_1_team")],
        }
        branches = " UNION ALL ".join(
            f"""SELECT r.{role} AS pid, TRY_CAST(r.season AS INT) AS yr, {val} AS v
                FROM '{_q(spec.source_key)}' r
                WHERE r.season_type = 'REG' AND r.{role} IS NOT NULL
                  AND ({flt}) AND ({val}) IS NOT NULL"""
            for role, val, flt in METRICS[spec.source_col])
        return f"""
        SELECT bio.pfr_id, u.yr, SUM(u.v) {_scale_suffix(spec)} AS val
        FROM ({branches}) u
        JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio ON bio.NFL_player_id = u.pid
        WHERE bio.pfr_id IS NOT NULL
        GROUP BY 1, 2"""
    if spec.shape == "pbp_rollup":
        # Raw pbp_merged rolled to season at query time. team_col carries the
        # ATTRIBUTION id column (passer/rusher/receiver/penalty player id -- the
        # nflcom-slug precedent for reusing the field), filters carry the play
        # predicate, source_expr the per-play value. gsis ids join bio.NFL_player_id
        # directly (measured 113/113 on 2024 passers).
        assert spec.team_col and spec.source_expr, "pbp_rollup needs attribution + expr"
        # Every PBP player rollup inherits the locked play-disposition taxonomy.
        # Individual maps may add narrower predicates, but none may count a
        # nullified down.
        f = f"{f} AND {official_play_sql('r')}"
        return f"""
        SELECT bio.pfr_id, TRY_CAST(r.season AS INT) AS yr,
               {outer_for(spec)}({spec.source_expr}) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio
          ON bio.NFL_player_id = r.{spec.team_col}
        WHERE bio.pfr_id IS NOT NULL AND r.season_type = 'REG'
          AND ({spec.source_expr}) IS NOT NULL {f}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pbp_fumble_events":
        # Fumble attribution is an event partition, not a single source
        # player column: both fumble slots are legal, and the fumbler must be
        # the rusher, receiver, or passer for the corresponding subtype.
        kind = {
            "fumbles": "1=1",
            "fumbles_lost": "r.fumble_lost=1",
            "rushing_fumbles": "r.rush_attempt=1 AND r.fumbler_id=r.rusher_player_id",
            "rushing_fumbles_lost": "r.rush_attempt=1 AND r.fumbler_id=r.rusher_player_id AND r.fumble_lost=1",
            "receiving_fumbles": "r.complete_pass=1 AND r.fumbler_id=r.receiver_player_id",
            "receiving_fumbles_lost": "r.complete_pass=1 AND r.fumbler_id=r.receiver_player_id AND r.fumble_lost=1",
            "sack_fumbles": "r.sack=1 AND r.fumbler_id=r.passer_player_id",
            "sack_fumbles_lost": "r.sack=1 AND r.fumbler_id=r.passer_player_id AND r.fumble_lost=1",
        }[spec.v26_col]
        branches = " UNION ALL ".join(
            f"SELECT r.fumbled_{i}_player_id AS fumbler_id, "
            f"CASE WHEN {i}=1 AND r.fumbled_2_player_id IS NULL THEN {fumble_mentions_sql('r')} ELSE 1 END AS event_count, r.* FROM '{_q(spec.source_key)}' r "
            f"WHERE r.fumble=1 AND r.fumbled_{i}_player_id IS NOT NULL "
            f"AND {official_play_sql('r')}"
            for i in (1, 2)
        )
        value = "e.event_count" if spec.v26_col == "fumbles" else "1"
        return f"""
        SELECT bio.pfr_id, TRY_CAST(e.season AS INT) AS yr,
               SUM(CASE WHEN {kind.replace('r.', 'e.')} THEN {value} ELSE 0 END) AS val
        FROM ({branches}) e
        JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio
          ON bio.NFL_player_id = e.fumbler_id
        WHERE bio.pfr_id IS NOT NULL AND e.season_type='REG'
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pfr_team_stats":
        # THE RE-KEYED LONG TABLE (Joe's regime ruling, 2026-08-01): statistic
        # identity lives in the `stat` ROW LABEL, team identity in the COLUMN
        # (vis_stat/home_stat), and every cell speaks TWICE -- the home value is
        # simultaneously the visitor's *_allowed. Key = (label, extraction, credit).
        #   source_table = the stat label (selector on r.stat)
        #   source_expr  = extraction with {col} placeholder, substituted per side
        #   team_col     = 'own' (credit the side itself) | 'opp' (the mirror)
        assert spec.source_expr and "{col}" in spec.source_expr, \
            "pfr_team_stats needs a {col} extraction"
        assert spec.team_col in ("own", "opp"), "credit must be own|opp"
        label = spec.source_table.replace("'", "''")
        home_val = spec.source_expr.replace("{col}", "r.home_stat")
        vis_val = spec.source_expr.replace("{col}", "r.vis_stat")
        home_team = "g.team_code" if spec.team_col == "own" else "g.opponent_code"
        vis_team = "g.opponent_code" if spec.team_col == "own" else "g.team_code"
        tg = Path(S.TEAM_GAMES.path).as_posix()
        games = (f"(SELECT DISTINCT boxscore_id, year, week, team_code, opponent_code "
                 f"FROM '{tg}' WHERE season_type = 'REG' AND is_home) g")
        return f"""
        SELECT team, yr, wk, ANY_VALUE(val) {_scale_suffix(spec)} AS val FROM (
          SELECT {home_team} AS team, TRY_CAST(g.year AS INT) AS yr,
                 TRY_CAST(g.week AS INT) AS wk, {home_val} AS val
          FROM '{_q(spec.source_key)}' r JOIN {games} USING (boxscore_id)
          WHERE r.stat = '{label}' AND ({home_val}) IS NOT NULL
          UNION ALL
          SELECT {vis_team}, TRY_CAST(g.year AS INT), TRY_CAST(g.week AS INT), {vis_val}
          FROM '{_q(spec.source_key)}' r JOIN {games} USING (boxscore_id)
          WHERE r.stat = '{label}' AND ({vis_val}) IS NOT NULL)
        GROUP BY 1, 2, 3"""
    if spec.shape == "pfr_box_team_ep":
        # expected_points names teams by NICKNAME; the self-built crosswalk
        # (scoring-log running scores x catalog sides, 7 contested cells of 2,403)
        # translates. PFR's def EP is signed FOR the defense; our *_allowed columns
        # are the offense's take, so specs declare scale=-1.
        xw = ("D:/league-history-data/nfl/derived/validation/sota_recon_master/"
              "nickname_team_code_crosswalk.parquet")
        tg = Path(S.TEAM_GAMES.path).as_posix()
        return f"""
        SELECT xw.team_code AS team, TRY_CAST(g.year AS INT) AS yr,
               TRY_CAST(g.week AS INT) AS wk,
               ANY_VALUE(TRY_CAST(r."{spec.source_col}" AS DOUBLE)) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        JOIN read_parquet('{xw}') xw
          ON xw.nickname = r.team_name AND xw.season = TRY_CAST(r.season AS INT)
         AND xw.confidence >= 0.95
        JOIN (SELECT DISTINCT boxscore_id, year, week FROM '{tg}'
              WHERE season_type = 'REG') g USING (boxscore_id)
        WHERE TRY_CAST(r."{spec.source_col}" AS DOUBLE) IS NOT NULL {f}
        GROUP BY 1, 2, 3"""
    if spec.shape == "pfr_drives":
        # Drive logs are OFFENSIVE drives; the DEFENSE that forced the outcome is
        # the game's OTHER team, so home drives credit the visitor and vice versa.
        # source_table carries 'home'|'vis'; filters carry the drive predicate.
        side = spec.source_table
        assert side in ("home", "vis"), "pfr_drives needs source_table home|vis"
        credit = "opponent_code" if side == "home" else "team_code"
        tg = Path(S.TEAM_GAMES.path).as_posix()
        return f"""
        SELECT g.{credit} AS team,
               TRY_CAST(g.year AS INT) AS yr, TRY_CAST(g.week AS INT) AS wk,
               SUM(1) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        JOIN (SELECT DISTINCT boxscore_id, year, week, team_code, opponent_code
              FROM '{tg}' WHERE season_type = 'REG' AND is_home) g
          USING (boxscore_id)
        WHERE TRUE {f}
        GROUP BY 1, 2, 3"""
    if spec.shape == "flat":
        outer = "MAX" if spec.agg == "max" else ("ANY_VALUE" if spec.agg == "value" else "SUM")
        st = ("AND r.season_type = 'REG'"
              if spec.source_key == "pbp_player_week_rollup" else "")
        source_col = f'r."{spec.source_col.replace(chr(34), chr(34) * 2)}"'
        text_value = spec.v26_col in {
            "game_date", "nfl_position", "season_type", "nfl_team",
            "opponent_nfl_team", "player_week", "data_source", "position",
            "headshot_url", "fantasy_position", "starter_position",
        }
        value_expr = source_col if spec.agg == "value" and text_value \
            else f"TRY_CAST({source_col} AS DOUBLE)"
        return f"""
        SELECT bio.pfr_id, r."year" AS yr, {outer}({value_expr}) {_scale_suffix(spec)} AS val
        FROM '{_q(spec.source_key)}' r
        JOIN '{Path(S.PLAYER_BIO.path).as_posix()}' bio USING (NFL_player_id)
        WHERE bio.pfr_id IS NOT NULL AND {source_col} IS NOT NULL {st} {f}
        GROUP BY 1, 2"""
    if spec.shape in {"player_bio", "pfr_combine", "pfr_player_combine"}:
        # Static-player value paths are keyed by the direct PFR id and have no season
        # axis.  Use yr=0 as the explicit static sentinel; validate() routes these specs
        # to the career/static plane rather than pretending a bio cell is a season total.
        if spec.shape == "player_bio":
            src = _q(spec.source_key)
            pid = "pfr_id"
        elif spec.shape == "pfr_player_combine":
            src = _q(spec.source_key)
            pid = "pfr_id"
        else:
            src = _q(spec.source_key)
            # PFR combine capture stores the direct Stathead id in player_link_ids.  It is
            # a scalar in this table (not the comma-separated box-score form).
            pid = "player_link_ids"
        if spec.shape in {"pfr_combine", "pfr_player_combine"} and spec.source_col == "height":
            # Combine pages publish feet-inches (6-4); the static plane stores inches.
            source_expr = ("CAST((TRY_CAST(split_part(height, '-', 1) AS DOUBLE) * 12 "
                           "+ TRY_CAST(split_part(height, '-', 2) AS DOUBLE)) AS VARCHAR)")
        elif spec.shape in {"pfr_combine", "pfr_player_combine"} and spec.source_col in {
                "weight", "forty_yd", "vertical", "bench_reps", "broad_jump",
                "cone", "shuttle"}:
            source_expr = f"CAST(TRY_CAST({spec.source_col} AS DOUBLE) AS VARCHAR)"
        else:
            source_expr = f"CAST({spec.source_col} AS VARCHAR)"
        return f"""
        SELECT CAST({pid} AS VARCHAR) AS pfr_id, 0 AS yr,
               ANY_VALUE({source_expr}) AS val
        FROM read_parquet('{src}', union_by_name=true)
        WHERE {pid} IS NOT NULL AND NULLIF(TRIM(CAST({spec.source_col} AS VARCHAR)), '') IS NOT NULL
        GROUP BY 1"""
    if spec.shape == "newspaper_lineup_week":
        # Long-form lineup sidecar is already one resolved player-week row.  The season
        # witness projection is only for shared tooling; the licensing path uses the
        # native weekly branch above and parses the same explicit year/week suffix.
        return f"""
        SELECT bio.pfr_id,
               TRY_CAST(regexp_extract(r.player_week, '_(\\d{{4}})_', 1) AS INT) AS yr,
               ANY_VALUE(TRY_CAST(r.{spec.source_col} AS DOUBLE)) AS val
        FROM read_parquet('{_q(spec.source_key)}') r
        JOIN read_parquet('{Path(S.PLAYER_BIO.path).as_posix()}') bio
          ON bio.NFL_player_id = r.NFL_player_id
        WHERE r.NFL_player_id IS NOT NULL AND r.{spec.source_col} IS NOT NULL
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "newspaper_note_week":
        return f"""
        SELECT CAST(NFL_player_id AS VARCHAR) AS pfr_id,
               TRY_CAST(year AS INT) AS yr,
               ANY_VALUE(CAST(\"{spec.source_col}\" AS VARCHAR)) AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true)
        WHERE NFL_player_id IS NOT NULL
          AND NULLIF(TRIM(CAST(\"{spec.source_col}\" AS VARCHAR)), '') IS NOT NULL
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "statscrew_team_season":
        # StatsCrew rows are per player-team-season and repeat short physical headers
        # across table_tag families.  The table selector is applied before aggregation;
        # source_player_id reaches PFR/NFL space only through the receipted, fail-closed
        # crosswalk built by build_statscrew_player_crosswalk.py.
        xw = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
                  r"statscrew_player_pfr_crosswalk.parquet").as_posix()
        table_pred = (f"AND s.table_tag = '{spec.source_table.replace(chr(39), chr(39) * 2)}'"
                      if spec.source_table else "")
        row_pred = f"AND ({spec.row_filter})" if spec.row_filter else ""
        outer = "MAX" if spec.agg == "max" else ("ANY_VALUE" if spec.agg == "value" else "SUM")
        return f"""
        SELECT x.pfr_id, TRY_CAST(s.season AS INT) AS yr,
               {outer}(TRY_CAST(s."{spec.source_col}" AS DOUBLE)) {_scale_suffix(spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) s
        JOIN read_parquet('{xw}') x ON x.source_player_id=s.source_player_id
        WHERE TRY_CAST(s."{spec.source_col}" AS DOUBLE) IS NOT NULL
          {table_pred} {row_pred}
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "statscrew_roster_season":
        xw = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
                  r"statscrew_player_pfr_crosswalk.parquet").as_posix()
        return f"""
        SELECT x.pfr_id, TRY_CAST(s.season AS INT) AS yr,
               ANY_VALUE(NULLIF(TRIM(CAST(s."{spec.source_col}" AS VARCHAR)), '')) AS val
        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) s
        JOIN read_parquet('{xw}') x ON x.source_player_id=CAST(s.source_player_id AS VARCHAR)
        WHERE s."{spec.source_col}" IS NOT NULL
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    if spec.shape == "pfr_award_pages":
        if spec.source_col == "pos":
            return f"""
            SELECT CAST(player_link_ids AS VARCHAR) AS pfr_id,
                   TRY_CAST(year AS INT) AS yr,
                   ANY_VALUE(NULLIF(TRIM(CAST(pos AS VARCHAR)), '')) AS val
            FROM read_parquet('{_q(spec.source_key)}')
            WHERE player_link_ids IS NOT NULL AND pos IS NOT NULL
            GROUP BY 1, 2 HAVING yr IS NOT NULL"""
        outer = "MAX" if spec.agg == "max" else ("ANY_VALUE" if spec.agg == "value" else "SUM")
        return f"""
        SELECT CAST(player_link_ids AS VARCHAR) AS pfr_id,
               TRY_CAST(year AS INT) AS yr,
               {outer}(TRY_CAST({spec.source_col} AS DOUBLE)) {_scale_suffix(spec)} AS val
        FROM read_parquet('{_q(spec.source_key)}')
        WHERE player_link_ids IS NOT NULL
          AND TRY_CAST({spec.source_col} AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2 HAVING yr IS NOT NULL"""
    raise ValueError(f"unknown shape {spec.shape}")


# =======================================================================================
# THE VERDICT, SPLIT.
#
# The old vocabulary had five values -- VALIDATED, MAPPING-SUSPECT, BROKEN, NO-OVERLAP,
# NO-MODERN-STRATUM -- and four of them blame our PLUMBING while the fifth passes. There
# was no sentence in the language for "the value path is right and OUR NUMBER is wrong".
# So when PFR and we disagreed on solo tackles across 23,178 player-seasons, the ledger
# recorded that our mapping was suspect. That is why "fix the supertable" never moved: a
# disagreement could not convict the supertable.
#
# Two second-order defects came with it:
#
#   1. MAPPING-SUSPECT also DE-LICENSED the witness. The worse our data was, the fewer
#      witnesses were permitted to speak about it. A self-sealing instrument.
#   2. The agreement denominator was the INNER JOIN of rows where BOTH sides are non-null.
#      Rows where the source holds a value and we hold nothing were dropped from numerator
#      AND denominator alike, so a column we are 95% missing could read VALIDATED at 100%.
#      The instrument could not see our gaps, so it could never report one.
#
# A validation result is now THREE independent facts:
#
#   path       is the value path well-formed?    BROKEN / NO_STRATUM / NO_OVERLAP / WELL_FORMED
#   agreement  where both sides have a number, do they match?      AGREES / DISAGREES
#   fault      when they do not, WHO is wrong?   -- ADJUDICATED, never inferred
#
# `fault` is deliberately not computed. Per the program's escalate-don't-improvise law the
# code MEASURES (coverage, sign split, magnitude) and a recorded decision ATTRIBUTES. An
# unattributed disagreement defaults to UNADJUDICATED, which is a queue, not a pass.
# =======================================================================================

PATH_WELL_FORMED = "WELL_FORMED"

#: fault attributions. SUPERTABLE_* faults are the queue that feeds supertable repair.
FAULTS = {
    "UNADJUDICATED": "measured, attribution not yet made -- this is a QUEUE, not a pass",
    "SUPERTABLE_GAP": "the source holds values on keys where we hold nothing; not "
                      "attributable to the mapping once the key plane is receipted",
    "SUPERTABLE_VALUE": "we hold a value and it is wrong -- concentrated and/or one-sided",
    "SOURCE_DEFECT": "we are right and the source is the outlier",
    "NAME_COLLISION": "two different statistics share a name; neither side is wrong and "
                      "the mapping must be withdrawn rather than repaired",
    "MAPPING_DEFECT": "the value path itself is wrong -- unit, filter, grain or column",
}

#: faults that leave the MAPPING sound, so the witness keeps its licence to vouch. A
#: supertable defect must never silence the witness that detected it.
FAULTS_LEAVING_THE_MAPPING_SOUND = {"SUPERTABLE_GAP", "SUPERTABLE_VALUE", "SOURCE_DEFECT"}

ADJUDICATIONS_PATH = (Path(__file__).resolve().parent / "witness_gate" / "contracts"
                      / "disagreement_adjudications.v1.json")


def load_adjudications() -> dict[tuple[str, str], dict]:
    """Recorded fault attributions, keyed (source_key, v26_col). Absent = UNADJUDICATED."""
    import json
    if not ADJUDICATIONS_PATH.exists():
        return {}
    doc = json.loads(ADJUDICATIONS_PATH.read_text(encoding="utf-8"))
    return {(r["source"], r["v26_col"]): r for r in doc["adjudications"]}


def validate(specs: list[MapSpec] | None = None) -> list[dict]:
    """Measure each mapping against v26 on the known-good stratum.

    Reports COVERAGE (does the source hold keys we do not?) beside AGREEMENT (where we both
    hold a number, does it match?). Attribution of fault is read from the adjudication
    contract, never inferred here.

    TOLERANCE COMES FROM THE CONTRACT, NEVER FROM THIS FILE (2026-07-31). §17.1 retired the
    ad-hoc +/-1.5-yard / +/-0.5-count rule on 2026-07-26 and made `tolerance_of()` the only
    legal reader -- "a +/-1 disagreement between roots is a conflict, not noise". That law
    was applied in the VOTING lane and left standing here, in the LICENSING lane, where it
    matters more: a licence is what lets a witness speak at all. Under +/-1.5 the nflcom
    `sfty` mapping licenses at ~100% while contradicting us on 143 of 143 rows in 1982-93,
    because a band of 1.5 around a count whose median is 1 accepts every value the statistic
    takes. Same defect the column audit carried; same fix.
    """
    # function-local: witness_votes imports THIS module, so a module-level import of the
    # tolerance reader would be circular. The reader still has to be the single source.
    from .witness_votes import tolerance_of

    adjudicated = load_adjudications()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    # The partitioned weekly parts are canonical whenever the release carries
    # its PARTITIONED marker. Reading the legacy monolith here can report false
    # witness disagreements after a supported weekly rewrite.
    v26 = Path(S.weekly_read_path()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    out = []
    for spec in specs or WITNESS_MAP:
        # the DECLARED tolerance for the canonical this spec claims to witness
        tol = (spec.validation_tolerance if spec.validation_tolerance is not None
               else tolerance_of(spec.v26_col))
        vy = spec.validation_years or VALIDATION_YEARS
        lo = max(vy[0], S.registry()[spec.source_key].year_min)
        hi = min(vy[1], S.registry()[spec.source_key].year_max)
        if lo > hi:
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                            agree_pct=None, verdict="NO-MODERN-STRATUM"))
            continue
        if spec.shape == "pfr_pbp_player_week":
            # The PFR witness artifact is already reduced to pfr_id/year/week;
            # identity is resolved only here through the canonical bio anchor.
            src = Path(S.registry()[spec.source_key].path).as_posix()
            source_value = spec.source_expr or f"TRY_CAST(r.{spec.source_col} AS DOUBLE)"
            bio_q = Path(S.PLAYER_BIO.path).as_posix()
            try:
                n, agree = con.execute(f"""
                    WITH w AS (
                        SELECT bio.NFL_player_id AS pid,
                               r.year AS yr, r.week AS wk,
                               ANY_VALUE({source_value}) AS val
                        FROM read_parquet('{src}', union_by_name=true) r
                        JOIN read_parquet('{bio_q}') bio ON bio.pfr_id = r.pfr_id
                        WHERE r.season_type = '{spec.season_type}'
                          AND ({source_value}) IS NOT NULL
                          AND r.year BETWEEN {lo} AND {hi}
                        GROUP BY 1, 2, 3
                    ), v AS (
                        SELECT t.NFL_player_id AS pid, t.year AS yr, t.week AS wk,
                               ANY_VALUE(TRY_CAST(t.{spec.v26_col} AS DOUBLE)) AS val
                        FROM read_parquet('{v26}', union_by_name=true) t
                        WHERE t.season_type = '{spec.season_type}'
                          AND t.{spec.v26_col} IS NOT NULL
                          AND t.year BETWEEN {lo} AND {hi}
                        GROUP BY 1, 2, 3
                    )
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol})
                    FROM w JOIN v USING (pid, yr, wk)""").fetchone()
            except Exception as e:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                agree_pct=None,
                                verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                continue
            pct = agree / n if n else None
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                            agree_pct=None if pct is None else round(pct, 4),
                            verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                     if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
            continue
        if spec.shape == "nflcom_week":
            # THE SHAPE HONOURS THE DECLARED AGGREGATE, ON BOTH SIDES. It hardcoded SUM,
            # which is right for a count and wrong for a flag: `is_starter` is
            # NON_AGGREGATABLE / dtype bool / natural_grain player_game, and
            # test_no_spec_anywhere_aggregates_a_non_aggregatable_canonical exists to stop a
            # spec summing one. Relabelling the spec while the SQL still summed would satisfy
            # the test with a declaration that LIED about the query.
            #
            # AND THE SUBJECT SIDE IS CAST. SUM() silently coerced a BOOLEAN to numeric;
            # ANY_VALUE preserves it, and `DOUBLE - BOOLEAN` has no `-` operator, so the spec
            # returned "BROKEN: Binder Error" the instant the aggregate stopped being SUM.
            # TRY_CAST is a no-op for every numeric column.
            _wagg = outer_for(spec)
            # WEEK-GRAIN NFL.com, against the RECOVERED Recent Games table. That caption
            # arrived with no season and its seven position blocks merged into one `_table`
            # value; nflcom_recent_games_recovery restored both -- the block from column
            # occupancy, the season from the scoreboard against the game catalog -- so this
            # reads `_rg_block` / `_rg_season` / `_rg_wk` rather than the raw columns.
            #
            # It is the grain v26 is NATIVE to, and a season total cannot substitute: a
            # season rollup cannot catch a within-season distribution error.
            rec = ("D:/league-history-data/nfl/raw/nflcom/tables/"
                   "player_career_recent_recovered/recent_games_recovered.parquet")
            xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
            bio_q = Path(S.PLAYER_BIO.path).as_posix()
            try:
                n, agree = con.execute(f"""
                    WITH ident AS (SELECT b.NFL_player_id pid, x.nflcom_slug slug
                                   FROM read_parquet('{xw}') x
                                   JOIN read_parquet('{bio_q}') b ON b.pfr_id = x.pfr_id),
                         w AS (SELECT i.pid, r._rg_season yr, r._rg_wk wk,
                                      {_wagg}(TRY_CAST(r."{spec.source_col}" AS DOUBLE)) v
                               FROM read_parquet('{rec}') r
                               JOIN ident i ON i.slug = r.nflcom_slug
                               WHERE r._rg_block = '{spec.source_table}'
                                 AND r._rg_season IS NOT NULL
                                 AND TRY_CAST(r."{spec.source_col}" AS DOUBLE) IS NOT NULL
                               GROUP BY 1, 2, 3),
                         t AS (SELECT NFL_player_id pid, year yr, week wk,
                                      {_wagg}(TRY_CAST({spec.v26_col} AS DOUBLE)) v
                               FROM read_parquet('{v26}')
                               WHERE season_type = '{spec.season_type}' GROUP BY 1, 2, 3)
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.v - t.v) <= {tol})
                    FROM w JOIN t USING (pid, yr, wk)""").fetchone()
            except Exception as e:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col,
                                source_table=spec.source_table, validation_grain="week", n=0,
                                agree_pct=None,
                                verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                continue
            pct = agree / n if n else None
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col,
                            source_table=spec.source_table, validation_grain="week", n=n,
                            agree_pct=None if pct is None else round(pct, 4),
                            verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                     if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
            continue
        if spec.validation_grain == "week":
            # native-grain compare: weekly value vs v26 weekly value, joined on
            # (player, year, week). Tolerance is the canonical's DECLARED policy (17.1).
            src = _q(spec.source_key)
            bio_q = Path(S.PLAYER_BIO.path).as_posix()
            f = f"AND ({spec.filters})" if spec.filters else ""
            if (spec.shape == "box" and spec.v26_col == "game_date"
                    and spec.source_key in BOX_PLAYER_CONTEXT_SOURCES):
                try:
                    n, agree = con.execute(f"""
                        WITH w AS (
                            SELECT bio.NFL_player_id AS pid,
                                   TRY_CAST(g.year AS INT) AS yr,
                                   TRY_CAST(g.week AS INT) AS wk,
                                   ANY_VALUE(CAST(TRY_CAST(r.game_date AS DATE) AS VARCHAR)) AS val
                            FROM read_parquet('{src}', union_by_name=true) r
                            JOIN read_parquet('{bio_q}') bio
                              ON bio.pfr_id = regexp_extract(CAST(r.player_link_ids AS VARCHAR), '^([^,]+)', 1)
                            JOIN read_parquet('{Path(S.TEAM_GAMES.path).as_posix()}') g
                              ON g.boxscore_id = r.boxscore_id AND g.season_type = 'REG'
                            WHERE r.player_link_ids IS NOT NULL AND r.game_date IS NOT NULL
                            GROUP BY 1,2,3
                        ), v AS (
                            SELECT t.NFL_player_id AS pid, t.year AS yr, t.week AS wk,
                                   ANY_VALUE(CAST(TRY_CAST(t.game_date AS DATE) AS VARCHAR)) AS val
                            FROM read_parquet('{v26}') t
                            WHERE t.season_type = 'REG' AND t.game_date IS NOT NULL
                            GROUP BY 1,2,3
                        )
                        SELECT COUNT(*), COUNT(*) FILTER (
                            WHERE LOWER(TRIM(w.val)) = LOWER(TRIM(v.val)))
                        FROM w JOIN v USING (pid,yr,wk)
                        WHERE yr BETWEEN {lo} AND {hi}""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None, verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if not n else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else
                                         "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                         "MAPPING-SUSPECT")))
                continue
            if spec.shape in {"newspaper_team_week", "team_week", "pfr_drives",
                              "pfr_team_stats", "pfr_box_team_ep",
                              "pbp_team_rollup", "pbp_drive_rollup",
                              "pbp_team_ratio", "pbp_team_return",
                              "statscrew_results"}:
                # Team claims have no player key. Additive canonicals are compared
                # to the team/week SUM of the supertable; native team rates use the
                # published value already present on the team row.
                stat_pred = (f"AND r.{spec.table_col} = '{spec.source_table.replace(chr(39), chr(39) * 2)}'"
                             if spec.shape == "newspaper_team_week" and spec.table_col
                             and spec.source_table else "")
                # 2026-08-01: the shape honours row_filter (scoring_summary carries
                # POST rows the REG compare must exclude) and source_expr (is_win
                # from a 'W'/'L' result column has no raw numeric to cast).
                row_pred = f"AND ({spec.row_filter})" if spec.row_filter else ""
                tw_val = spec.source_expr or f"TRY_CAST(r.{spec.source_col} AS DOUBLE)"
                target_agg = ("ANY_VALUE" if spec.agg == "value" else "SUM")
                # pfr_drives / pfr_team_stats build their own (team, yr, wk, val)
                # witness -- both lack year/week columns and both credit teams the
                # raw row does not name (the defense; the mirrored side).
                w_cte = (build_witness_sql(spec)
                         if spec.shape in ("pfr_drives", "pfr_team_stats",
                                           "pfr_box_team_ep", "pbp_team_rollup", "pbp_drive_rollup",
                                           "pbp_team_ratio", "pbp_team_return", "statscrew_results")
                         else f"""
                            SELECT r.{spec.team_col} AS team, TRY_CAST(r.year AS INT) AS yr,
                                   TRY_CAST(r.week AS INT) AS wk,
                                   ANY_VALUE({tw_val}) AS val
                            FROM read_parquet('{src}', union_by_name=true) r
                            WHERE ({tw_val}) IS NOT NULL
                              {stat_pred} {row_pred} AND r.year BETWEEN {lo} AND {hi}
                            GROUP BY 1, 2, 3""")
                try:
                    n, agree = con.execute(f"""
                        WITH w AS ({w_cte}
                        ), v AS (
                            SELECT t.nfl_team AS team, t.year AS yr, t.week AS wk,
                                   {spec.v26_expr or f"{target_agg}(TRY_CAST(t.{spec.v26_col} AS DOUBLE))"} AS val
                            FROM read_parquet('{v26}', union_by_name=true) t
                            WHERE t.season_type = '{spec.season_type}'
                              AND t.{spec.v26_col} IS NOT NULL
                              AND t.year BETWEEN {lo} AND {hi}
                            GROUP BY 1, 2, 3
                        )
                        SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol})
                        FROM w JOIN v USING (team, yr, wk)""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None,
                                    verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if pct is None else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                         if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
                continue
            if spec.shape == "newspaper_team_pair_week":
                # Each physical newspaper row contains both teams. Unpivot both
                # sides before grouping so one team is never silently discarded.
                stat_pred = (f"AND r.{spec.table_col} = '{spec.source_table.replace(chr(39), chr(39) * 2)}'"
                             if spec.table_col and spec.source_table else "")
                target_agg = ("ANY_VALUE" if spec.agg == "value" else "SUM")
                try:
                    n, agree = con.execute(f"""
                        WITH w AS (
                            SELECT team, yr, wk, ANY_VALUE(val) AS val FROM (
                                SELECT r.team_1_nfl_team AS team,
                                       TRY_CAST(r.year AS INT) AS yr,
                                       TRY_CAST(r.week AS INT) AS wk,
                                       TRY_CAST(r.team_1_value AS DOUBLE) AS val
                                FROM '{src}' r
                                WHERE TRY_CAST(r.team_1_value AS DOUBLE) IS NOT NULL
                                  {stat_pred} AND r.year BETWEEN {lo} AND {hi}
                                UNION ALL
                                SELECT r.team_2_nfl_team AS team,
                                       TRY_CAST(r.year AS INT) AS yr,
                                       TRY_CAST(r.week AS INT) AS wk,
                                       TRY_CAST(r.team_2_value AS DOUBLE) AS val
                                FROM '{src}' r
                                WHERE TRY_CAST(r.team_2_value AS DOUBLE) IS NOT NULL
                                  {stat_pred} AND r.year BETWEEN {lo} AND {hi}
                            ) u GROUP BY 1, 2, 3
                        ), v AS (
                            SELECT t.nfl_team AS team, t.year AS yr, t.week AS wk,
                                   {spec.v26_expr or f"{target_agg}(TRY_CAST(t.{spec.v26_col} AS DOUBLE))"} AS val
                            FROM '{v26}' t
                            WHERE t.season_type = '{spec.season_type}'
                              AND t.{spec.v26_col} IS NOT NULL
                              AND t.year BETWEEN {lo} AND {hi}
                            GROUP BY 1, 2, 3
                        )
                        SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol})
                        FROM w JOIN v USING (team, yr, wk)""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None,
                                    verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if pct is None else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                         if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
                continue
            if (spec.shape == "flat" and spec.agg == "value"
                    and spec.v26_col in {
                        "game_date", "nfl_position", "season_type", "nfl_team",
                        "opponent_nfl_team", "player_week", "data_source", "position",
                        "headshot_url", "fantasy_position", "starter_position",
                    }):
                # Native player-week context is compared as text, not passed through ABS.
                # This is limited to flat sources with explicit player/year/week axes.
                try:
                    n, agree = con.execute(f"""
                        SELECT COUNT(*), COUNT(*) FILTER (
                            WHERE LOWER(TRIM(CAST(r.{spec.source_col} AS VARCHAR))) =
                                  LOWER(TRIM(CAST(t.{spec.v26_col} AS VARCHAR))))
                        FROM '{src}' r
                        JOIN '{v26}' t ON t.NFL_player_id=r.NFL_player_id
                            AND t.year=r.year AND t.week=r.week
                            AND t.season_type='{spec.season_type}'
                        WHERE r.{spec.source_col} IS NOT NULL
                          AND t.{spec.v26_col} IS NOT NULL
                          AND r.year BETWEEN {lo} AND {hi}""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None,
                                    verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if not n else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else
                                         "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                         "MAPPING-SUSPECT")))
                continue
            if spec.shape == "nflcom_log_week":
                src = (Path(spec.source_path).as_posix() if spec.source_path
                       else _q(spec.source_key))
                table_pred = (f"AND r.{spec.table_col} = '{spec.source_table.replace(chr(39), chr(39) * 2)}'"
                              if spec.table_col and spec.source_table else "")
                row_pred = f"AND ({spec.row_filter})" if spec.row_filter else ""
                xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
                if spec.v26_col == "game_date":
                    try:
                        n, agree = con.execute(f"""
                            WITH w AS (
                                SELECT bio.NFL_player_id AS pid,
                                       TRY_CAST(r.season AS INT) AS yr,
                                       TRY_CAST(r.wk AS INT) AS wk,
                                       ANY_VALUE(CAST(TRY_CAST(r.game_date AS DATE) AS VARCHAR)) AS val
                                FROM read_parquet('{src}', union_by_name=true) r
                                JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
                                JOIN read_parquet('{bio_q}') bio ON bio.pfr_id = x.pfr_id
                                WHERE r.game_date IS NOT NULL {table_pred} {row_pred}
                                  AND TRY_CAST(r.season AS INT) BETWEEN {lo} AND {hi}
                                GROUP BY 1,2,3
                            ), v AS (
                                SELECT t.NFL_player_id AS pid, t.year AS yr, t.week AS wk,
                                       ANY_VALUE(CAST(TRY_CAST(t.game_date AS DATE) AS VARCHAR)) AS val
                                FROM read_parquet('{v26}') t
                                WHERE t.season_type = '{spec.season_type}' AND t.game_date IS NOT NULL
                                  AND t.year BETWEEN {lo} AND {hi}
                                GROUP BY 1,2,3
                            )
                            SELECT COUNT(*), COUNT(*) FILTER (
                                WHERE LOWER(TRIM(w.val)) = LOWER(TRIM(v.val)))
                            FROM w JOIN v USING (pid,yr,wk)""").fetchone()
                    except Exception as e:
                        out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                        agree_pct=None, verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                        continue
                    pct = agree / n if n else None
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                    agree_pct=None if not n else round(pct, 4),
                                    verdict=("NO-OVERLAP" if not n else
                                             "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                             "MAPPING-SUSPECT")))
                    continue
                log_val = (spec.source_expr or
                           f'TRY_CAST(r."{spec.source_col}" AS DOUBLE)')
                try:
                    n, agree = con.execute(f"""
                        WITH w AS (
                            SELECT bio.NFL_player_id AS pid,
                                   TRY_CAST(r.season AS INT) AS yr,
                                   TRY_CAST(r.wk AS INT) AS wk,
                                   ANY_VALUE({log_val}) AS val
                            FROM read_parquet('{src}', union_by_name=true) r
                            JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
                            JOIN read_parquet('{bio_q}') bio ON bio.pfr_id = x.pfr_id
                            WHERE bio.NFL_player_id IS NOT NULL
                              AND ({log_val}) IS NOT NULL
                              {table_pred} {row_pred}
                              AND TRY_CAST(r.season AS INT) BETWEEN {lo} AND {hi}
                            GROUP BY 1, 2, 3
                        ), v AS (
                            SELECT t.NFL_player_id AS pid, t.year AS yr, t.week AS wk,
                                   ANY_VALUE(TRY_CAST(t.{spec.v26_col} AS DOUBLE)) AS val
                            FROM read_parquet('{v26}', union_by_name=true) t
                            WHERE t.season_type = '{spec.season_type}'
                              AND t.{spec.v26_col} IS NOT NULL
                              AND t.year BETWEEN {lo} AND {hi}
                            GROUP BY 1, 2, 3
                        )
                        SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol})
                        FROM w JOIN v USING (pid, yr, wk)""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None,
                                    verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if pct is None else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                         if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
                continue
            if spec.shape == "newspaper_player_week":
                # Long-form newspaper cells use stat_name as the table selector,
                # stat_value as the numeric payload, and do not carry season_type.
                f += (f" AND r.{spec.table_col} = '{spec.source_table.replace(chr(39), chr(39) * 2)}'"
                      if spec.table_col and spec.source_table else "")
                source_value = f"TRY_CAST(r.{spec.source_col} AS DOUBLE)"
            else:
                source_col = f'r."{spec.source_col.replace(chr(34), chr(34) * 2)}"'
                text_value = spec.v26_col in {
                    "game_date", "nfl_position", "season_type", "nfl_team",
                    "opponent_nfl_team", "player_week", "data_source", "position",
                    "headshot_url", "fantasy_position", "starter_position",
                }
                source_value = source_col if spec.agg == "value" and text_value \
                    else f"TRY_CAST({source_col} AS DOUBLE)"
            if spec.shape in {"newspaper_lineup_week", "newspaper_note_week"}:
                # The lineup sidecar stores player_week as <pfr_id>_<year>_<week> and
                # keeps the resolved NFL_player_id.  This is a native weekly witness;
                # never roll it up to a season before checking it.
                text_witness = (spec.shape == "newspaper_note_week" or
                                spec.v26_col in {"nfl_team", "opponent_nfl_team", "player_week",
                                                 "starter_position"})
                if text_witness and spec.v26_col == "game_date":
                    source_expr = f'CAST(TRY_CAST(r."{spec.source_col}" AS DATE) AS VARCHAR)'
                    target_expr = f'CAST(TRY_CAST(t."{spec.v26_col}" AS DATE) AS VARCHAR)'
                else:
                    source_expr = (f'CAST(r."{spec.source_col}" AS VARCHAR)' if text_witness
                               else f'TRY_CAST(r."{spec.source_col}" AS DOUBLE)')
                    target_expr = (f'CAST(t."{spec.v26_col}" AS VARCHAR)' if text_witness
                               else f'TRY_CAST(t."{spec.v26_col}" AS DOUBLE)')
                agreement_expr = ("LOWER(TRIM(w.val)) = LOWER(TRIM(v.val))"
                                  if text_witness else f"ABS(w.val-v.val) <= {tol}")
                try:
                    n, agree = con.execute(f"""
                        WITH w AS (
                            SELECT r.NFL_player_id AS pid,
                                   TRY_CAST(regexp_extract(r.player_week, '_(\\d{{4}})_', 1) AS INT) AS yr,
                                   TRY_CAST(regexp_extract(r.player_week, '_(\\d+)$', 1) AS INT) AS wk,
                                   ANY_VALUE({source_expr}) AS val
                            FROM '{src}' r
                            WHERE r.NFL_player_id IS NOT NULL
                              AND {source_expr} IS NOT NULL
                            GROUP BY 1,2,3
                        ), v AS (
                            SELECT t.NFL_player_id AS pid, t.year AS yr, t.week AS wk,
                                   ANY_VALUE({target_expr}) AS val
                            FROM '{v26}' t
                            WHERE t.season_type='{spec.season_type}'
                              AND t.{spec.v26_col} IS NOT NULL
                            GROUP BY 1,2,3
                        )
                        SELECT COUNT(*), COUNT(*) FILTER (WHERE {agreement_expr})
                        FROM w JOIN v USING (pid,yr,wk)
                        WHERE w.yr BETWEEN {lo} AND {hi}""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None,
                                    verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if n == 0 else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else
                                         "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                         "MAPPING-SUSPECT")))
                continue
            try:
                n, agree = con.execute(f"""
                    SELECT COUNT(*),
                           COUNT(*) FILTER (WHERE ABS({source_value} - t.{spec.v26_col}) <= {tol})
                    FROM '{src}' r
                    JOIN '{v26}' t ON t.NFL_player_id = r.NFL_player_id
                        AND t.year = r.year AND t.week = r.week
                        AND t.season_type = '{spec.season_type}'
                    WHERE {source_value} IS NOT NULL AND t.{spec.v26_col} IS NOT NULL
                      AND r.year BETWEEN {lo} AND {hi} {f}""").fetchone()
            except Exception as e:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                agree_pct=None,
                                verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                continue
            pct = agree / n if n else None
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                            agree_pct=None if pct is None else round(pct, 4),
                            verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                     if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
            continue
        if (spec.shape == "box" and spec.v26_col == "game_date"
                and spec.source_key in BOX_PLAYER_CONTEXT_SOURCES):
            try:
                n, agree = con.execute(f"""
                    WITH w AS (
                        SELECT bio.NFL_player_id AS pid,
                               TRY_CAST(g.year AS INT) AS yr,
                               TRY_CAST(g.week AS INT) AS wk,
                               ANY_VALUE(CAST(TRY_CAST(r.game_date AS DATE) AS VARCHAR)) AS val
                        FROM read_parquet('{_q(spec.source_key)}', union_by_name=true) r
                        JOIN read_parquet('{bio}') bio
                          ON bio.pfr_id = regexp_extract(CAST(r.player_link_ids AS VARCHAR), '^([^,]+)', 1)
                        JOIN read_parquet('{Path(S.TEAM_GAMES.path).as_posix()}') g
                          ON g.boxscore_id = r.boxscore_id AND g.season_type = 'REG'
                        WHERE r.player_link_ids IS NOT NULL AND r.game_date IS NOT NULL
                        GROUP BY 1,2,3
                    ), v AS (
                        SELECT t.NFL_player_id AS pid, t.year AS yr, t.week AS wk,
                               ANY_VALUE(CAST(TRY_CAST(t.game_date AS DATE) AS VARCHAR)) AS val
                        FROM read_parquet('{v26}') t
                        WHERE t.season_type = 'REG' AND t.game_date IS NOT NULL
                        GROUP BY 1,2,3
                    )
                    SELECT COUNT(*), COUNT(*) FILTER (
                        WHERE LOWER(TRIM(w.val)) = LOWER(TRIM(v.val)))
                    FROM w JOIN v USING (pid,yr,wk)
                    WHERE yr BETWEEN {lo} AND {hi}""").fetchone()
            except Exception as e:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                agree_pct=None, verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                continue
            pct = agree / n if n else None
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                            agree_pct=None if not n else round(pct, 4),
                            verdict=("NO-OVERLAP" if not n else
                                     "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                     "MAPPING-SUSPECT")))
            continue
        if spec.shape == "nflcom_team_season":
            # NFL.com team names are doubled nicknames. Resolve (nickname, season)
            # to franchise space using the receipted passing-yard fingerprint before
            # comparing any other category/side statistic.
            src = _q(spec.source_key)
            row_pred = f"AND ({spec.row_filter})" if spec.row_filter else ""
            target_agg = ("MAX" if spec.agg == "max" else
                          "ANY_VALUE" if spec.agg == "value" else "SUM")
            try:
                n, agree = con.execute(f"""
                    WITH nick AS (
                        SELECT CASE WHEN length(team) % 2 = 1
                                     AND substr(team, 1, CAST((length(team)-1)/2 AS BIGINT))
                                         = substr(team, CAST((length(team)+3)/2 AS BIGINT))
                                    THEN substr(team, 1, CAST((length(team)-1)/2 AS BIGINT))
                                    ELSE team END AS nick,
                               TRY_CAST(season AS INT) AS yr,
                               SUM(TRY_CAST(pass_yds AS DOUBLE)) AS pass_yards
                        FROM read_parquet('{src}', union_by_name=True)
                        WHERE _category='passing' AND _side='offense'
                          AND season_type='reg'
                        GROUP BY 1, 2
                    ), mine AS (
                        SELECT CAST(nfl_franchise_number AS INT) AS fid, year AS yr,
                               SUM(TRY_CAST(passing_yards AS DOUBLE)) AS pass_yards
                        FROM read_parquet('{v26}', union_by_name=True)
                        WHERE season_type='REG' AND nfl_franchise_number IS NOT NULL
                        GROUP BY 1, 2
                    ), xw AS (
                        SELECT nick, yr, fid FROM (
                            SELECT n.nick, n.yr, m.fid,
                                   ABS(n.pass_yards - m.pass_yards) AS delta,
                                   ROW_NUMBER() OVER (PARTITION BY n.nick, n.yr
                                                      ORDER BY ABS(n.pass_yards - m.pass_yards)) AS rn
                            FROM nick n JOIN mine m USING (yr)
                            WHERE n.pass_yards IS NOT NULL AND m.pass_yards IS NOT NULL
                        ) q WHERE rn=1 AND delta <= 1
                    ), w AS (
                        SELECT x.fid, TRY_CAST(s.season AS INT) AS yr,
                               ANY_VALUE(TRY_CAST(s."{spec.source_col}" AS DOUBLE)) AS val
                        FROM read_parquet('{src}', union_by_name=True) s
                        JOIN xw x ON x.nick = CASE WHEN length(s.team) % 2 = 1
                                                   AND substr(s.team, 1, CAST((length(s.team)-1)/2 AS BIGINT))
                                                       = substr(s.team, CAST((length(s.team)+3)/2 AS BIGINT))
                                                  THEN substr(s.team, 1, CAST((length(s.team)-1)/2 AS BIGINT))
                                                  ELSE s.team END
                                      AND x.yr = TRY_CAST(s.season AS INT)
                        WHERE s."{spec.source_col}" IS NOT NULL {row_pred}
                        GROUP BY 1, 2
                    ), v AS (
                        SELECT CAST(nfl_franchise_number AS INT) AS fid, year AS yr,
                               {target_agg}(TRY_CAST({spec.v26_col} AS DOUBLE)) AS val
                        FROM read_parquet('{v26}', union_by_name=True)
                        WHERE season_type = '{spec.season_type}'
                          AND nfl_franchise_number IS NOT NULL
                          AND {spec.v26_col} IS NOT NULL
                        GROUP BY 1, 2
                    )
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol})
                    FROM w JOIN v USING (fid, yr)""").fetchone()
            except Exception as e:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                agree_pct=None,
                                verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                continue
            pct = agree / n if n else None
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                            agree_pct=None if pct is None else round(pct, 4),
                            verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                     if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
            continue
        if spec.validation_referee:
            # cross-era referee: grade against a LICENSED pages mapping for the same
            # column instead of v26 (PFA-class sources have no modern stratum)
            ref = next((m for m in WITNESS_MAP
                        if m.source_key == spec.validation_referee
                        and m.v26_col == spec.v26_col and m.season_type == "REG"), None)
            if ref is None:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                agree_pct=None, verdict="BROKEN: no referee spec"))
                continue
            v_cte = build_witness_sql(ref)
            try:
                n, agree = con.execute(f"""
                    WITH w AS ({build_witness_sql(spec)}),
                         v AS ({v_cte})
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol})
                    FROM w JOIN v USING (pfr_id, yr)
                    WHERE yr BETWEEN {lo} AND {hi}""").fetchone()
            except Exception as e:
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                agree_pct=None,
                                verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                continue
            pct = agree / n if n else None
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                            agree_pct=None if pct is None else round(pct, 4),
                            verdict=("NO-OVERLAP" if not n else "VALIDATED"
                                     if pct >= VALIDATION_MIN_AGREE else "MAPPING-SUSPECT")))
            continue
        # NOTE the missing `AND t.{col} IS NOT NULL`. It used to be here, and with the
        # INNER JOIN below it silently deleted every row where the source has a number and
        # we have none -- from the numerator AND the denominator. That is the one class of
        # disagreement that can only ever be OUR fault, so excluding it made the
        # instrument structurally incapable of convicting the supertable.
        if spec.agg == "value":
            if spec.shape in {"pages", "box"} and spec.v26_col == "nfl_position":
                v_cte = (f"SELECT bio.pfr_id, t.year AS yr, "
                         f"ANY_VALUE(CAST(t.nfl_position AS VARCHAR)) AS val "
                         f"FROM '{Path(S.v26_plane('season')).as_posix()}' t "
                         f"JOIN '{bio}' bio USING (NFL_player_id) "
                         f"WHERE bio.pfr_id IS NOT NULL AND t.nfl_position IS NOT NULL "
                         f"GROUP BY 1, 2")
                try:
                    n, agree = con.execute(f"""
                        WITH w AS ({build_witness_sql(spec)}), v AS ({v_cte})
                        SELECT COUNT(*), COUNT(*) FILTER (
                            WHERE LOWER(TRIM(CAST(w.val AS VARCHAR))) =
                                  LOWER(TRIM(CAST(v.val AS VARCHAR))))
                        FROM w JOIN v USING (pfr_id, yr)
                        WHERE yr BETWEEN {lo} AND {hi}""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None, verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if not n else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else
                                         "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                         "MAPPING-SUSPECT")))
                continue
            if (spec.shape == "pfr_award_pages" and spec.v26_col == "nfl_position"):
                v_cte = (f"SELECT bio.pfr_id, t.year AS yr, "
                         f"ANY_VALUE(CAST(t.nfl_position AS VARCHAR)) AS val "
                         f"FROM '{Path(S.v26_plane('season')).as_posix()}' t "
                         f"JOIN '{bio}' bio USING (NFL_player_id) "
                         f"WHERE bio.pfr_id IS NOT NULL AND t.nfl_position IS NOT NULL "
                         f"GROUP BY 1, 2")
                try:
                    n, agree = con.execute(f"""
                        WITH w AS ({build_witness_sql(spec)}), v AS ({v_cte})
                        SELECT COUNT(*), COUNT(*) FILTER (
                            WHERE LOWER(TRIM(CAST(w.val AS VARCHAR))) =
                                  LOWER(TRIM(CAST(v.val AS VARCHAR))))
                        FROM w JOIN v USING (pfr_id, yr)
                        WHERE yr BETWEEN {lo} AND {hi}""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None, verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if not n else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else
                                         "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                         "MAPPING-SUSPECT")))
                continue
            if spec.shape == "statscrew_roster_season":
                v_cte = (f"SELECT bio.pfr_id, t.year AS yr, "
                         f"ANY_VALUE(CAST(t.{spec.v26_col} AS VARCHAR)) AS val "
                         f"FROM '{Path(S.v26_plane('season')).as_posix()}' t "
                         f"JOIN '{bio}' bio USING (NFL_player_id) "
                         f"WHERE bio.pfr_id IS NOT NULL AND t.{spec.v26_col} IS NOT NULL "
                         f"GROUP BY 1, 2")
                try:
                    n, agree = con.execute(f"""
                        WITH w AS ({build_witness_sql(spec)}), v AS ({v_cte})
                        SELECT COUNT(*), COUNT(*) FILTER (
                            WHERE LOWER(TRIM(CAST(w.val AS VARCHAR))) =
                                  LOWER(TRIM(CAST(v.val AS VARCHAR))))
                        FROM w JOIN v USING (pfr_id, yr)
                        WHERE yr BETWEEN {lo} AND {hi}""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None, verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n if n else None
                out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=n,
                                agree_pct=None if not n else round(pct, 4),
                                verdict=("NO-OVERLAP" if not n else
                                         "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                                         "MAPPING-SUSPECT")))
                continue
            if spec.grain == "player_static":
                # Static witnesses compare against the career/static plane at the same
                # player key.  The static sentinel yr=0 keeps the existing witness tuple
                # shape without inventing a season for bio or combine measurements.
                v_cte = (f"SELECT bio.pfr_id, 0 AS yr, "
                         f"ANY_VALUE(CAST(t.{spec.v26_col} AS VARCHAR)) AS val "
                         f"FROM '{Path(S.v26_plane('career')).as_posix()}' t "
                         f"JOIN '{bio}' bio USING (NFL_player_id) "
                         f"WHERE bio.pfr_id IS NOT NULL GROUP BY 1")
                try:
                    n_src, n_cmp, n_no_row, n_we_null, n_src_null, agree = con.execute(f"""
                        WITH w AS ({build_witness_sql(spec)}), v AS ({v_cte})
                        SELECT COUNT(*), COUNT(v.val),
                               COUNT(*) FILTER (WHERE v.pfr_id IS NULL),
                               COUNT(*) FILTER (WHERE v.pfr_id IS NOT NULL AND v.val IS NULL),
                               COUNT(*) FILTER (WHERE w.val IS NULL),
                               COUNT(*) FILTER (WHERE w.val = v.val)
                        FROM w LEFT JOIN v ON v.pfr_id=w.pfr_id AND v.yr=w.yr
                        WHERE w.yr=0""").fetchone()
                except Exception as e:
                    out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                                    agree_pct=None, path="BROKEN",
                                    verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
                    continue
                pct = agree / n_cmp if n_cmp else None
                record = adjudicated.get((spec.source_key, spec.v26_col))
                out.append(dict(
                    source=spec.source_key, v26_col=spec.v26_col,
                    source_table=spec.source_table or "", validation_grain="static",
                    n=n_cmp, agree_pct=None if pct is None else round(pct, 4),
                    path="NO_OVERLAP" if not n_src else PATH_WELL_FORMED,
                    agreement=None if not n_cmp else ("AGREES" if pct >= VALIDATION_MIN_AGREE
                                                       else "DISAGREES"),
                    n_source_keys=n_src, n_compared=n_cmp,
                    coverage_pct=round(n_cmp / n_src, 4) if n_src else None,
                    n_we_hold_no_row=n_no_row, n_we_hold_null=n_we_null,
                    source_null_or_blank=n_src_null,
                    source_exceeds_us=0, we_exceed_source=0,
                    zero_vs_zero=0, zero_share=None, informative_n=n_cmp,
                    fault=(record or {}).get("fault", "UNADJUDICATED"),
                    fault_basis=(record or {}).get("basis", ""),
                    verdict=("NO-OVERLAP" if not n_cmp else
                             "VALIDATED" if pct >= VALIDATION_MIN_AGREE else
                             "MAPPING-SUSPECT")))
                continue
            # value-grain specs compare against the SEASON table, not weekly re-aggregation.
            # v26_expr is honoured here too (2026-08-01): measurement specs for a column
            # the plane does not yet carry (kicking_points) or a derivable (fga buckets =
            # made+missed) compute their subject from components instead of binding a
            # nonexistent column.
            from .recon_rate_fingerprint import season_parquet
            v_val = spec.v26_expr or f"ANY_VALUE(t.{spec.v26_col})"
            v_cte = (f"SELECT bio.pfr_id, t.year AS yr, {v_val} AS val "
                     f"FROM '{Path(season_parquet()).as_posix()}' t "
                     f"JOIN '{bio}' bio USING (NFL_player_id) "
                     f"WHERE bio.pfr_id IS NOT NULL "
                     f"GROUP BY 1, 2")
        else:
            v26_agg = spec.v26_expr or f"{'MAX' if spec.agg == 'max' else 'SUM'}(t.{spec.v26_col})"
            v_cte = (f"SELECT bio.pfr_id, t.year AS yr, {v26_agg} AS val "
                     f"FROM '{v26}' t JOIN '{bio}' bio USING (NFL_player_id) "
                     f"WHERE t.season_type = '{spec.season_type}' AND bio.pfr_id IS NOT NULL "
                     + "GROUP BY 1, 2")
        try:
            # LEFT JOIN, so the denominator is the SOURCE's keys -- what the witness can
            # speak to -- rather than the intersection of what we both already have.
            (n_src, n_cmp, n_no_row, n_we_null, n_src_null, agree,
             src_gt, src_lt, med_abs, n_both_zero, med_src, med_v26,
             n_src_zero, n_v26_zero, n_src_zero_v26_nonzero,
             n_src_nonzero_v26_zero) = con.execute(f"""
                WITH w AS ({build_witness_sql(spec)}),
                     v AS ({v_cte})
                SELECT COUNT(*),
                       COUNT(v.val),
                       COUNT(*) FILTER (WHERE v.pfr_id IS NULL),
                       COUNT(*) FILTER (WHERE v.pfr_id IS NOT NULL AND v.val IS NULL),
                       COUNT(*) FILTER (WHERE w.val IS NULL),
                       COUNT(*) FILTER (WHERE ABS(w.val - v.val) <= {tol}),
                       COUNT(*) FILTER (WHERE w.val > v.val + {tol}),
                       COUNT(*) FILTER (WHERE w.val < v.val - {tol}),
                       MEDIAN(ABS(w.val - v.val)) FILTER (WHERE ABS(w.val - v.val) > {tol}),
                       -- ZERO SHARE. An agreement rate is only worth its non-zero base:
                       -- nflcom `sfty` reads 98.5% agreement on rows that are 99.0%
                       -- zero-vs-zero, i.e. it agrees about nothing, while `solo` reads
                       -- 51.9% on a base only 3% zero and is a real disagreement. Reading
                       -- the first as the better witness is the "equal counts prove
                       -- nothing" trap with the control left out.
                       COUNT(*) FILTER (WHERE w.val = 0 AND v.val = 0),
                       MEDIAN(w.val) FILTER (WHERE w.val <> 0 OR v.val <> 0),
                       MEDIAN(v.val) FILTER (WHERE w.val <> 0 OR v.val <> 0),
                       COUNT(*) FILTER (WHERE w.val = 0),
                       COUNT(*) FILTER (WHERE v.val = 0),
                       COUNT(*) FILTER (WHERE w.val = 0 AND v.val <> 0),
                       COUNT(*) FILTER (WHERE w.val <> 0 AND v.val = 0)
                FROM w LEFT JOIN v ON v.pfr_id = w.pfr_id AND v.yr = w.yr
                WHERE w.yr BETWEEN {lo} AND {hi}""").fetchone()
        except Exception as e:  # bad column name etc. = broken mapping, report don't crash
            out.append(dict(source=spec.source_key, v26_col=spec.v26_col, n=0,
                            agree_pct=None, path="BROKEN", agreement=None,
                            fault="MAPPING_DEFECT", fault_basis="SQL did not execute",
                            verdict=f"BROKEN: {str(e).splitlines()[0][:70]}"))
            continue
        pct = agree / n_cmp if n_cmp else None
        agreement = (None if not n_cmp else
                     "AGREES" if pct >= VALIDATION_MIN_AGREE else "DISAGREES")
        record = adjudicated.get((spec.source_key, spec.v26_col))
        fault = (record or {}).get("fault", "UNADJUDICATED")
        if agreement == "AGREES" and not n_we_null and not n_no_row and not record:
            fault = ""          # nothing to attribute: full coverage, full agreement
        out.append(dict(
            source=spec.source_key, v26_col=spec.v26_col,
            # THE KEY WAS NOT UNIQUE. On a table-major source one canonical is produced by
            # several tables -- QB/RBFB/WRTE Career all emit `fumbles`, `carries`,
            # `rushing_yards` -- so whichever validated last silently owned the licence and
            # a column with no spec of its own READ as licensed. Recorded per row here;
            # licensed_at_table() below is the precise reader.
            source_table=spec.source_table or "",
            validation_grain=spec.validation_grain or "season",
            n=n_cmp, agree_pct=None if pct is None else round(pct, 4),
            path="NO_OVERLAP" if not n_src else PATH_WELL_FORMED,
            agreement=agreement,
            # --- coverage: what the witness can speak to that we cannot answer ---
            n_source_keys=n_src, n_compared=n_cmp,
            coverage_pct=round(n_cmp / n_src, 4) if n_src else None,
            n_we_hold_no_row=n_no_row, n_we_hold_null=n_we_null,
            source_null_or_blank=n_src_null,
            # --- shape of the disagreement, for attribution ---
            source_exceeds_us=src_gt, we_exceed_source=src_lt,
            median_abs_delta=None if med_abs is None else round(med_abs, 3),
            # an agree_pct is only as good as its non-zero base -- see the SQL comment
            zero_vs_zero=n_both_zero,
            zero_share=round(n_both_zero / n_cmp, 4) if n_cmp else None,
            informative_n=n_cmp - n_both_zero,
            median_source=None if med_src is None else round(med_src, 2),
            median_v26=None if med_v26 is None else round(med_v26, 2),
            # Explicit zero direction.  These are separate from ordinary greater-than
            # counts because a source-published zero against a positive target is not the
            # same adjudication as a positive residual: it can be a source blank/zero
            # convention, while a positive source against our zero is a candidate
            # SUPERTABLE_VALUE/GAP.  Keep both counts in the receipt for adjudication.
            source_zero=n_src_zero,
            supertable_zero=n_v26_zero,
            source_zero_supertable_nonzero=n_src_zero_v26_nonzero,
            source_nonzero_supertable_zero=n_src_nonzero_v26_zero,
            fault=fault, fault_basis=(record or {}).get("basis", ""),
            verdict=("NO-OVERLAP" if not n_cmp else
                     "VALIDATED" if agreement == "AGREES" else "MAPPING-SUSPECT")))
    con.close()
    return out


LICENSES = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
            r"\MAPPING_LICENSES.json")


def write_licenses(rows: list[dict]) -> str:
    """Persist validation verdicts: the 'license to vouch'. Lanes that vote MUST read this
    and use only VALIDATED mappings -- an unvalidated mapping cannot vouch for anything."""
    import json
    from decimal import Decimal
    with open(LICENSES, "w", encoding="utf-8") as f:
        json.dump({"generated": True, "rows": rows}, f, indent=1,
                  default=lambda o: float(o) if isinstance(o, Decimal) else str(o))
    return LICENSES


def licensed() -> set[tuple[str, str]]:
    """Mappings permitted to vouch.

    A disagreement whose fault has been attributed to US -- SUPERTABLE_GAP,
    SUPERTABLE_VALUE -- or to the source's own data leaves the VALUE PATH sound, so the
    witness keeps its licence. Only a fault in the path itself (MAPPING_DEFECT) or a
    withdrawn identity (NAME_COLLISION) removes it.

    This inversion is the point. Under the old rule any disagreement de-licensed the
    witness, so a supertable defect silenced the very witness that had detected it, and the
    worse our data was the fewer witnesses were allowed to speak about it."""
    import json, os
    if not os.path.exists(LICENSES):
        raise FileNotFoundError(
            f"{LICENSES} missing -- run `python -m scripts.sota_recon.witness_map` first")
    rows = json.load(open(LICENSES, encoding="utf-8"))["rows"]
    adjudicated = load_adjudications()
    out = set()
    for r in rows:
        key = (r["source"], r["v26_col"])
        if r["verdict"] == "VALIDATED":
            out.add(key)
            continue
        fault = (adjudicated.get(key) or {}).get("fault", "UNADJUDICATED")
        if fault in FAULTS_LEAVING_THE_MAPPING_SOUND:
            out.add(key)      # our defect, not the witness's -- it may still vouch
    return out


def licensed_at_table() -> set[tuple[str, str, str]]:
    """(source, source_table, v26_col) -- the PRECISE licence.

    `licensed()` above answers "may this SOURCE vouch for this canonical", which is the
    right granularity for a VOTE: a source speaks with one voice. This answers "does THIS
    TABLE have its own validated value path", which is what a floor and any per-table
    accounting need. Keeping both is deliberate -- collapsing them is what let a column with
    no spec of its own read as licensed because a sibling caption had one.
    """
    import json, os
    if not os.path.exists(LICENSES):
        raise FileNotFoundError(f"{LICENSES} missing")
    rows = json.load(open(LICENSES, encoding="utf-8"))["rows"]
    adjudicated = load_adjudications()
    out = set()
    for r in rows:
        key = (r["source"], r.get("source_table", ""), r["v26_col"])
        if r["verdict"] == "VALIDATED":
            out.add(key)
            continue
        fault = (adjudicated.get((r["source"], r["v26_col"])) or {}).get(
            "fault", "UNADJUDICATED")
        if fault in FAULTS_LEAVING_THE_MAPPING_SOUND:
            out.add(key)
    return out


def supertable_defect_queue() -> list[dict]:
    """Disagreements attributed to US. THIS is the queue that feeds supertable repair --
    the output the old vocabulary was structurally incapable of producing."""
    return [record for record in load_adjudications().values()
            if record.get("fault", "").startswith("SUPERTABLE")
            or record.get("fault") == "MAPPING_DEFECT"]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.parse_args()
    rows = validate()
    write_licenses(rows)
    bad = [r for r in rows if r["verdict"] not in ("VALIDATED",)]
    print(f"{'source':32s} {'v26 column':26s} {'n':>7s} {'agree':>7s}  verdict")
    for r in rows:
        a = f"{r['agree_pct']:.1%}" if r["agree_pct"] is not None else "-"
        print(f"{r['source']:32s} {r['v26_col']:26s} {r['n']:>7,} {a:>7s}  {r['verdict']}")
    print(f"\nmappings: {len(rows)}  validated: {len(rows) - len(bad)}  flagged: {len(bad)}")
