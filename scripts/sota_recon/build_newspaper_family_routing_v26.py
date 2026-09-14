"""
sota_recon/build_newspaper_family_routing_v26.py -- route ALL newspaper sidecar families
to their canonical destinations (the step after recon; the step before the ONE rebuild).

The G1-G10 recon lane (recon_newspaper_sidecar) grades the bundle; this lane ROUTES it:
every row of every family gets a disposition against its canonical table, and the
promotable weekly subset is assembled into an ancient ready-bundle SOURCE PACKAGE so the
supertable ingests newspaper atoms through the pipeline (never cell pokes -- Lesson 1).

Families -> canonical destinations (per BUNDLE_MANIFEST):
  weekly_player_stat_cells -> v26 weekly grain      (fill/insert candidates -> package)
  scoring_events           -> scoring_summary        (count-grain corroboration; G4)
  play_by_play_events      -> pfr boxscore pbp       (additive sidecar lane pre-PBP era)
  team_game_stats(+claims) -> nfl_team_games_all/team_stats (score claims via G3)
  lineup_participation     -> pfr home/vis_starters  (G9; newspaper-only starters)
  game_context             -> game catalog           (G3 context corroboration)
  player_game_notes        -> none (historical-record sidecar ledger)

Identity: the 380 ledger-resolved held atoms (RESOLUTION_LEDGER, twin-safe) are NOT in the
bundle tables (holds were never materialized); their payloads are recovered here from the
pilot's expanded_ready_witness_rows.csv by (target_table, boxscore_id, role raw name) and
routed with identity_resolution='ledger_resolved_pfr_corroborated'.

Promotability gate (wave58 doctrine, game-level): a game is CLEAN iff it has no G1 catalog
miss, no G3 confirmed score conflict, no G4 over-claim, no G6 over-claim, no G8
cell-below-events, no G5 unresolvable-defense events, and no REVIEWER holds. The
overlay-unmatched hold file is deliberately NOT a game gate: those rows ARE the insert
lane (wave58 deferred inserts; the pipeline apply protects them with NOT EXISTS + the
overwrite guard here). Newspaper-vs-v26 disagreements are NEVER settled here (Lesson 2:
candidate PFR errors -> image queue).

Outputs (report lane -- always safe):
  derived/validation/sota_recon_master/newspaper_routing/
    ROUTING_SUMMARY.json, weekly_settled_cells.csv, <family>_routed.csv,
    image_queue_inputs.csv, weekly_upsert_package_rows.csv
With --stage additionally writes the ready-bundle source package:
  curated/ancient_source_recovery/newspaper_ocr_recovery/<stamp>/
    newspaper_weekly_stat_upsert_rows.csv + PACKAGE_MANIFEST.json
(Registration into build_nfl_recovery_ready_upsert_bundle.SOURCE_REL_PATHS is a separate,
reviewed edit on the tools drive; this script never mutates canon, the bundle, or v26.)

    python -m scripts.sota_recon.build_newspaper_family_routing_v26           # report only
    python -m scripts.sota_recon.build_newspaper_family_routing_v26 --stage
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .newspaper_witness_common import (
    ATOM_TO_V26_COL, EVENT_TYPE_MAP, STAT_CELL_ATOM_MAP, NEWSPAPER_BUNDLE_DIR,
    sidecar_path)
from .sources import DATA_LAKE, latest_v26

OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_routing"
SIDE = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_sidecar"
IDENT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_identity"
STAGE_ROOT = Path(DATA_LAKE) / "curated" / "ancient_source_recovery" / "newspaper_ocr_recovery"

TG = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "nfl_team_games_all.parquet").replace("\\", "/")
SS = os.path.join(DATA_LAKE, "derived", "scoring_summary", "scoring_summary.parquet").replace("\\", "/")
PBP = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", "pbp", "_combined.parquet").replace("\\", "/")
STARTERS = [os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", t, "_combined.parquet").replace("\\", "/")
            for t in ("home_starters", "vis_starters")]
EXPANDED = os.path.join(
    DATA_LAKE, "derived", "newspaper_atoms", "supertable_witness_handoffs",
    "20260715T170438Z_newspaper_weekly_pbp_lineup_pilot_materialization_v1",
    "expanded_ready_witness_rows.csv").replace("\\", "/")
LEDGER = Path(os.environ.get("NEWSPAPER_IDENTITY_LEDGER", IDENT / "RESOLUTION_LEDGER.csv")).as_posix()
EVENT_OVERLAY_DIR = Path(os.environ.get(
    "NEWSPAPER_RESOLVED_EVENT_OVERLAY_DIR", IDENT / "resolved_event_overlays"
))
HOLDS_DIR = (Path(NEWSPAPER_BUNDLE_DIR) / "holds").as_posix()

DATA_SOURCE = "newspapers_com_ocr"

# v26 stores return TDs GRANULARLY (Lesson 2); def_tds / special_teams_tds are ROLLUPS.
# Rollup cells are only promotable when clean scoring events witness the granular split
# exactly; otherwise they route to needs_granular_split review.
ROLLUP_ATOMS = {"def_tds", "special_teams_tds"}
EVENT_GRANULAR_SPLIT = {
    "interception_return_touchdown": ("def_tds", "def_int_ret_td"),
    "fumble_return_touchdown": ("def_tds", "fum_ret_td"),
    "kickoff_return_touchdown": ("special_teams_tds", "kickoff_return_tds"),
    "punt_return_touchdown": ("special_teams_tds", "punt_return_tds"),
}
GRANULAR_ATOMS = sorted({g for _, g in EVENT_GRANULAR_SPLIT.values()})
# full promotable atom -> v26/package column map (atom == column for all of these)
PROMOTABLE_ATOM_COL = {**{a: c for a, c in ATOM_TO_V26_COL.items() if a not in ROLLUP_ATOMS},
                       **{g: g for g in GRANULAR_ATOMS}}

# expanded_ready_witness_rows wide weekly columns -> canonical atoms (only atoms with a
# v26 column; eff_touchdowns is ambiguous rush/rec/other -> deliberately NOT routed).
# Intersected with the CSV's real columns at runtime (the pilot only materialized the
# stat surface its games actually witnessed).
EFF_WEEKLY_ATOMS = {f"eff_{a}": a for a in ATOM_TO_V26_COL if a not in ("fumbles", "fumbles_lost")}

# full ancient ready-bundle package stat surface (superset; unmapped stay empty)
PACKAGE_STAT_COLS = [
    "attempts", "completions", "passing_yards", "passing_tds", "passing_interceptions",
    "sacks_suffered", "sack_yards_lost", "carries", "rushing_yards", "rushing_tds",
    "targets", "receptions", "receiving_yards", "receiving_tds",
    "completions_40plus", "completions_50plus", "passing_tds_40plus", "passing_tds_50plus",
    "receptions_0_4", "receptions_5_9", "receptions_10_19", "receptions_20_29",
    "receptions_30_39", "receptions_40plus", "receiving_tds_40plus", "receiving_tds_50plus",
    "fumbles", "fumbles_lost", "fum_rec", "fum_rec_yds", "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp", "fumble_recovery_yards", "fum_ret_td",
    "fg_att", "fg_made", "fg_missed", "fg_blocked", "fg_yards", "fg_long",
    "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49", "fg_made_50_59",
    "fg_made_60_", "pat_att", "pat_made", "pat_missed", "pat_blocked",
    "punts", "punt_yards", "punt_long", "punts_blocked",
    "kickoff_returns", "kickoff_return_yards", "kickoff_return_tds", "kickoff_return_long",
    "punt_returns", "punt_return_yards", "punt_return_tds", "punt_return_long",
    "special_teams_tds", "def_sacks", "def_interceptions", "def_interception_yards",
    "def_int_ret_td", "def_tds", "def_fumbles_forced", "def_tackles_solo",
    "def_tackle_assists", "def_tackles_with_assist", "def_tackles_for_loss",
    "def_pass_defended", "def_qb_hits", "def_safeties", "def_blk_kick",
    "special_teams_tackles_solo",
]
PACKAGE_META_COLS = [
    "bundle_source", "upsert_state", "already_in_supertable", "player_week",
    "NFL_player_id", "player", "year", "week", "season_type", "game_date",
    "team_game_key", "nfl_team", "opponent_nfl_team", "source_positions",
    "identity_resolution", "data_source", "known_stat_cols", "nonzero_known_stat_cols",
    "coverage_statuses", "known_missing_context", "stat_completeness_class",
    "source_fact_cells", "nonzero_source_fact_cells", "requires_multi_game_suffix",
    "source_player_url", "source_boxscore_url", "source_files", "derivation_rules",
]


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _csv(con, sql: str, name: str) -> int:
    path = (OUT / f"{name}.csv").as_posix()
    con.execute(f"COPY ({sql}) TO '{path}' (HEADER, DELIMITER ',')")
    n, = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
    return n


def _case_map(col: str, mapping: dict[str, str]) -> str:
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in mapping.items() if v)
    return f"CASE {col} {whens} ELSE NULL END"


def run(stage: bool) -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    subject = latest_v26()
    if "_local_only" in subject:
        raise SystemExit(f"GUARD: subject resolved to a _local_only overlay: {subject}")
    subject_sha = _sha256(subject)

    con = duckdb.connect()
    con.execute(f"CREATE VIEW tg AS SELECT * FROM read_parquet('{TG}')")
    con.execute(f"CREATE VIEW ss AS SELECT * FROM read_parquet('{SS}')")
    for t in ("weekly_player_stat_cells", "scoring_events", "team_game_stats",
              "team_game_stat_claims", "game_context", "lineup_participation",
              "play_by_play_events", "player_game_notes"):
        con.execute(f"CREATE VIEW np_{t} AS SELECT * FROM read_parquet('{sidecar_path('newspaper_' + t)}')")
    scoring_overlay = EVENT_OVERLAY_DIR / "resolved_scoring_events.parquet"
    pbp_overlay = EVENT_OVERLAY_DIR / "resolved_play_by_play_events.parquet"
    if scoring_overlay.exists():
        con.execute(f"""
            CREATE VIEW scoring_all AS
            SELECT * FROM np_scoring_events
            UNION ALL BY NAME
            SELECT o.* FROM read_parquet('{scoring_overlay.as_posix()}') o
            WHERE NOT EXISTS (
                SELECT 1 FROM np_scoring_events n
                WHERE n.target_entity_key = o.target_entity_key)""")
    else:
        con.execute("CREATE VIEW scoring_all AS SELECT * FROM np_scoring_events")
    if pbp_overlay.exists():
        con.execute(f"""
            CREATE VIEW pbp_all AS
            SELECT * FROM np_play_by_play_events
            UNION ALL BY NAME
            SELECT o.* FROM read_parquet('{pbp_overlay.as_posix()}') o
            WHERE NOT EXISTS (
                SELECT 1 FROM np_play_by_play_events n
                WHERE n.target_entity_key = o.target_entity_key)""")
    else:
        con.execute("CREATE VIEW pbp_all AS SELECT * FROM np_play_by_play_events")
    for name in ("g7_cells_vs_subject", "g4_scoring_bijection_by_team_game",
                 "g3_final_score_mismatches", "g3_context_score_mismatches",
                 "g1_boxscores_missing_from_catalog", "g6_player_team_over_violations",
                 "g8_event_cell_coverage", "g9_lineup_vs_pfr_starters",
                 "g5_events_with_unresolvable_defense"):
        con.execute(f"CREATE VIEW {name.split('_')[0]}_{name} AS SELECT * FROM read_csv_auto('{(SIDE / (name + '.csv')).as_posix()}', sample_size=-1)")
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_csv_auto('{(SIDE / (name + '.csv')).as_posix()}', sample_size=-1)")
    con.execute(f"CREATE VIEW ledger AS SELECT * FROM read_csv_auto('{LEDGER}', sample_size=-1)")
    con.execute(f"""
        CREATE TEMP TABLE holdq AS
        SELECT * FROM read_csv_auto('{HOLDS_DIR}/remaining_identity_hardhold_queue.csv', sample_size=-1)""")
    con.execute(f"CREATE VIEW expanded AS SELECT * FROM read_csv_auto('{EXPANDED}', sample_size=-1)")

    summary: dict = {"lane": "newspaper_routing", "subject": subject,
                     "subject_sha256": subject_sha,
                     "bundle": NEWSPAPER_BUNDLE_DIR}

    # --- clean-game gate (wave58 doctrine, game grain) --------------------------------
    con.execute(f"""
        CREATE TEMP TABLE dirty_games AS
        SELECT DISTINCT boxscore_id, STRING_AGG(DISTINCT reason, ',') reasons FROM (
            SELECT boxscore_id, 'g1_no_catalog_game' reason FROM g1_boxscores_missing_from_catalog
            UNION ALL SELECT boxscore_id, 'g3_final_score_conflict' FROM g3_final_score_mismatches
            UNION ALL SELECT boxscore_id, 'g3_context_confirmed_conflict' FROM g3_context_score_mismatches WHERE confirmed_class
            UNION ALL SELECT boxscore_id, 'g4_over_claim' FROM g4_scoring_bijection_by_team_game WHERE verdict = 'over_violation'
            UNION ALL SELECT boxscore_id, 'g6_over_claim' FROM g6_player_team_over_violations
            UNION ALL SELECT boxscore_id, 'g8_cell_below_events' FROM g8_event_cell_coverage WHERE verdict = 'cell_below_events'
            UNION ALL SELECT boxscore_id, 'g5_unresolvable_defense' FROM g5_events_with_unresolvable_defense
            UNION ALL SELECT boxscore_id, 'reviewer_hard_hold' FROM read_csv_auto('{HOLDS_DIR}/reviewer_hold_hard_hold_audit.csv', sample_size=-1)
        ) GROUP BY 1""")
    n_dirty, = con.execute("SELECT COUNT(*) FROM dirty_games").fetchone()
    summary["dirty_games"] = n_dirty

    # --- ledger-resolved payload recovery (the 380) -----------------------------------
    # hold queue -> ledger verdicts; payload rejoined from the expanded witness universe
    # on (target_table, boxscore_id, role raw name).
    con.execute("""
        CREATE TEMP TABLE resolved AS
        SELECT h.*, l.resolved_nfl_id, l.resolved_player, l.verdict identity_verdict
        FROM holdq h JOIN ledger l USING (identity_task_id)
        WHERE l.verdict IN ('resolved_pfr_corroborated',
                            'resolved_external_witness_corroborated')""")

    # --- family: weekly player stat cells ---------------------------------------------
    subj_atom_cols = ", ".join(
        f"s.{c}" for c in sorted(set(ATOM_TO_V26_COL.values()) | set(GRANULAR_ATOMS)))
    con.execute(f"""
        CREATE TEMP TABLE subj AS
        SELECT s.player_week, s.NFL_player_id, {subj_atom_cols}
        FROM read_parquet('{Path(subject).as_posix()}') s""")
    atom_case = " ".join(f"WHEN '{a}' THEN s.{c}" for a, c in ATOM_TO_V26_COL.items())

    # canonical id crosswalk: the pilot resolved identities in the PFR-index id space, but
    # v26 keys ancient players by HIST-* ids; bio.pfr_id is the bridge. Classifying on the
    # raw pilot id would call an EXISTING player-week "missing" and mint a twin (Jim Thorpe
    # ThorJi20 vs HIST-99602082). Every supertable-facing lane re-keys through this map.
    bio_path = os.path.join(DATA_LAKE, "ops_data", "nfl_historical", "player_bio.parquet").replace("\\", "/")
    con.execute(f"""
        CREATE TEMP TABLE canon AS
        SELECT pfr_id raw_id, NFL_player_id cid
        FROM read_parquet('{bio_path}')
        WHERE pfr_id IS NOT NULL AND pfr_id <> NFL_player_id""")
    summary["canon_crosswalk_rows"] = con.execute("SELECT COUNT(*) FROM canon").fetchone()[0]

    # lane A: native-id cells, recomputed from the bundle on CANONICAL ids (the recon
    # lane's G7 CSV predates the crosswalk; kept only for the delta report below)
    cell_case = _case_map("stat_name", STAT_CELL_ATOM_MAP)
    con.execute(f"""
        CREATE TEMP TABLE weekly_a AS
        WITH atoms AS (
            SELECT n.boxscore_id, COALESCE(c.cid, n.NFL_player_id) cid,
                   {cell_case} atom, MAX(TRY_CAST(n.stat_value AS DOUBLE)) v
            FROM np_weekly_player_stat_cells n
            LEFT JOIN canon c ON c.raw_id = n.NFL_player_id
            WHERE n.NFL_player_id IS NOT NULL AND n.NFL_player_id <> ''
              AND {cell_case} IS NOT NULL
            GROUP BY 1, 2, 3),
        keyed AS (
            SELECT a.*, a.cid || '_' || CAST(CAST(t.year AS INT) AS VARCHAR)
                     || '_' || CAST(CAST(t.week AS INT) AS VARCHAR) player_week
            FROM atoms a JOIN (SELECT DISTINCT boxscore_id, year, week FROM tg) t USING (boxscore_id))
        SELECT k.player_week, k.cid NFL_player_id, k.boxscore_id, k.atom,
               k.v np_value, CASE k.atom {atom_case} ELSE NULL END v26_value,
               CASE WHEN s.player_week IS NULL THEN 'missing_row'
                    WHEN CASE k.atom {atom_case} ELSE NULL END IS NULL THEN 'fill_signal'
                    WHEN TRY_CAST(CASE k.atom {atom_case} ELSE NULL END AS DOUBLE) = k.v
                         THEN 'witnessed_equal'
                    ELSE 'witnessed_diff' END verdict,
               'native' identity_resolution
        FROM keyed k LEFT JOIN subj s ON s.player_week = k.player_week""")
    # crosswalk impact: how many raw-id "missing rows" were really existing HIST rows
    summary["rekey_delta_vs_g7"] = dict(con.execute("""
        SELECT g.verdict || ' -> ' || a.verdict, COUNT(*)
        FROM g7_cells_vs_subject g
        JOIN canon c ON c.raw_id = g.NFL_player_id
        JOIN weekly_a a ON a.boxscore_id = g.boxscore_id AND a.atom = g.atom
             AND a.NFL_player_id = c.cid
        WHERE g.verdict <> a.verdict GROUP BY 1 ORDER BY 2 DESC""").fetchall())

    # lane B: ledger-resolved cell subjects; payload from expanded player_game_box_score
    # rows (wide) -> unpivot to atoms, dedup MAX per (player, atom, game).
    expanded_cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM expanded").fetchall()}
    eff_weekly_atoms = {c: a for c, a in EFF_WEEKLY_ATOMS.items() if c in expanded_cols}
    eff_unpivot = " UNION ALL ".join(
        f"SELECT r.resolved_nfl_id, r.resolved_player, e.boxscore_id, '{atom}' atom, "
        f"TRY_CAST(e.{col} AS DOUBLE) v, e.eff_source_document_id sdoc "
        f"FROM expanded e JOIN resolved r ON r.boxscore_id = e.boxscore_id "
        f"AND r.source_surface = 'weekly_player_stat_cell' "
        f"AND LOWER(TRIM(COALESCE(e.eff_player_raw, ''))) = LOWER(TRIM(r.raw_player)) "
        f"WHERE e.target_table = 'player_game_box_score' AND e.{col} IS NOT NULL"
        for col, atom in eff_weekly_atoms.items())
    con.execute(f"""
        CREATE TEMP TABLE weekly_b AS
        WITH atoms AS (
            SELECT resolved_nfl_id, resolved_player, boxscore_id, atom,
                   MAX(v) v, STRING_AGG(DISTINCT sdoc, '|') sdocs
            FROM ({eff_unpivot}) GROUP BY 1, 2, 3, 4),
        canoned AS (
            SELECT COALESCE(c.cid, a.resolved_nfl_id) resolved_nfl_id,
                   a.resolved_player, a.boxscore_id, a.atom, a.v, a.sdocs
            FROM atoms a LEFT JOIN canon c ON c.raw_id = a.resolved_nfl_id),
        keyed AS (
            SELECT a.*, t.year, t.week,
                   a.resolved_nfl_id || '_' || CAST(CAST(t.year AS INT) AS VARCHAR)
                     || '_' || CAST(CAST(t.week AS INT) AS VARCHAR) player_week
            FROM canoned a JOIN (SELECT DISTINCT boxscore_id, year, week FROM tg) t USING (boxscore_id))
        SELECT k.player_week, k.resolved_nfl_id NFL_player_id, k.boxscore_id, k.atom,
               k.v np_value, CASE k.atom {atom_case} ELSE NULL END v26_value,
               CASE WHEN s.player_week IS NULL THEN 'missing_row'
                    WHEN CASE k.atom {atom_case} ELSE NULL END IS NULL THEN 'fill_signal'
                    WHEN TRY_CAST(CASE k.atom {atom_case} ELSE NULL END AS DOUBLE) = k.v
                         THEN 'witnessed_equal'
                    ELSE 'witnessed_diff' END verdict,
               CASE WHEN EXISTS (
                        SELECT 1 FROM resolved r
                        WHERE r.boxscore_id = k.boxscore_id
                          AND r.resolved_nfl_id = k.resolved_nfl_id
                          AND r.identity_verdict = 'resolved_external_witness_corroborated')
                    THEN 'ledger_resolved_external_witness_corroborated'
                    ELSE 'ledger_resolved_pfr_corroborated' END identity_resolution
        FROM keyed k LEFT JOIN subj s ON s.player_week = k.player_week""")

    weekly_sql = """
        SELECT w.*, d.boxscore_id IS NOT NULL AS in_dirty_game,
               CASE WHEN w.verdict = 'witnessed_diff' THEN 'disagreement_queue'
                    WHEN w.verdict = 'witnessed_equal' THEN 'corroborated'
                    WHEN d.boxscore_id IS NOT NULL THEN 'gated_out_dirty_game'
                    WHEN w.verdict = 'fill_signal' THEN 'fill_candidate'
                    WHEN w.verdict = 'missing_row' THEN 'insert_candidate'
                    END route
        FROM (SELECT * FROM weekly_a UNION ALL SELECT * FROM weekly_b) w
        LEFT JOIN dirty_games d USING (boxscore_id)"""
    con.execute(f"CREATE TEMP TABLE weekly_routed AS {weekly_sql}")
    _csv(con, "SELECT * FROM weekly_routed ORDER BY boxscore_id, player_week, atom",
         "weekly_settled_cells")
    summary["weekly"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM weekly_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- family: scoring events -------------------------------------------------------
    bucket_case = _case_map("event_type", {k: v[0] for k, v in EVENT_TYPE_MAP.items()})
    con.execute(f"""
        CREATE TEMP TABLE scoring_routed AS
        SELECT e.boxscore_id, e.scoring_team, e.event_type, {bucket_case} bucket,
               e.scoring_NFL_player_id, e.passer_NFL_player_id, e.receiver_NFL_player_id,
               e.source_document_id, e.confidence_bar,
               COALESCE(g4.verdict, 'no_canonical_scoring') team_game_verdict,
               CASE WHEN g4.verdict = 'over_violation' THEN 'queue_pfr_omission_candidate'
                    WHEN d.boxscore_id IS NOT NULL THEN 'held_dirty_game'
                    WHEN g4.verdict = 'witnessed_equal' THEN 'corroborated_exact'
                    WHEN g4.verdict = 'partial_under' THEN 'corroborated_partial'
                    ELSE 'insert_candidate_no_canonical_scoring' END route
        FROM scoring_all e
        LEFT JOIN g4_scoring_bijection_by_team_game g4
               ON g4.boxscore_id = e.boxscore_id AND g4.team_code = e.scoring_team
        LEFT JOIN dirty_games d ON d.boxscore_id = e.boxscore_id""")
    _csv(con, "SELECT * FROM scoring_routed ORDER BY boxscore_id", "scoring_events_routed")
    summary["scoring_events"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM scoring_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- family: play-by-play events ---------------------------------------------------
    con.execute(f"""
        CREATE TEMP TABLE pbp_routed AS
        SELECT p.boxscore_id, p.event_order, p.play_type, p.possession_team,
               p.primary_NFL_player_id, p.secondary_NFL_player_id,
               p.source_document_id, p.confidence_bar,
               c.boxscore_id IS NOT NULL AS canonical_pbp_exists,
               CASE WHEN d.boxscore_id IS NOT NULL THEN 'held_dirty_game'
                    WHEN c.boxscore_id IS NULL THEN 'insert_candidate_no_canonical_pbp'
                    ELSE 'sidecar_overlaps_canonical_pbp' END route
        FROM pbp_all p
        LEFT JOIN (SELECT DISTINCT boxscore_id FROM read_parquet('{PBP}')) c USING (boxscore_id)
        LEFT JOIN dirty_games d ON d.boxscore_id = p.boxscore_id""")
    _csv(con, "SELECT * FROM pbp_routed ORDER BY boxscore_id, event_order", "pbp_events_routed")
    summary["pbp_events"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM pbp_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- family: team game stat claims -------------------------------------------------
    con.execute("""
        CREATE TEMP TABLE claims_routed AS
        SELECT c.boxscore_id, c.nfl_team, c.stat_name, c.stat_value,
               c.source_document_id, c.confidence_bar,
               CASE WHEN c.stat_name IN ('final_score', 'score_by_periods', 'score_by_period',
                                         'line_score_by_period', 'score_by_periods_from_narrative')
                         THEN 'score_claim_g3_lane'
                    WHEN c.stat_name = 'attendance' THEN 'game_context_fill_lane'
                    WHEN c.stat_name = 'league_standings_record_snapshot'
                         THEN 'historical_record_sidecar'
                    ELSE 'team_stat_witness_pending_label_map' END route
        FROM np_team_game_stat_claims c""")
    _csv(con, "SELECT * FROM claims_routed ORDER BY boxscore_id", "team_claims_routed")
    summary["team_claims"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM claims_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- family: lineups (native G9 + ledger-resolved re-check) -------------------------
    starters_union = " UNION ALL ".join(
        f"SELECT boxscore_id, player_link_ids FROM read_parquet('{p}')" for p in STARTERS)
    con.execute(f"""
        CREATE TEMP TABLE lineups_routed AS
        WITH native AS (
            SELECT boxscore_id, NFL_player_id, starter_position, verdict,
                   'native' identity_resolution
            FROM g9_lineup_vs_pfr_starters),
        led AS (
            SELECT r.boxscore_id, r.resolved_nfl_id NFL_player_id,
                   NULL AS starter_position,
                   CASE WHEN EXISTS (SELECT 1 FROM ({starters_union}) st
                                     WHERE st.boxscore_id = r.boxscore_id
                                       AND st.player_link_ids LIKE '%' || r.resolved_nfl_id || '%')
                        THEN 'matched' ELSE 'not_in_pfr_starters' END verdict,
                   CASE WHEN r.identity_verdict = 'resolved_external_witness_corroborated'
                        THEN 'ledger_resolved_external_witness_corroborated'
                        ELSE 'ledger_resolved_pfr_corroborated' END identity_resolution
            FROM resolved r WHERE r.source_surface = 'lineup_participation'),
        u AS (SELECT * FROM native UNION ALL SELECT * FROM led)
        SELECT u.*, CASE WHEN u.verdict = 'matched' THEN 'corroborated'
                         WHEN d.boxscore_id IS NOT NULL THEN 'held_dirty_game'
                         ELSE 'starter_insert_candidate' END route
        FROM u LEFT JOIN dirty_games d USING (boxscore_id)""")
    _csv(con, "SELECT * FROM lineups_routed ORDER BY boxscore_id", "lineups_routed")
    summary["lineups"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM lineups_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- family: game context -----------------------------------------------------------
    con.execute("""
        CREATE TEMP TABLE context_routed AS
        SELECT g.boxscore_id, g.team_1_resolved, g.team_1_score, g.team_2_resolved,
               g.team_2_score, g.reconciliation_status, g.confidence_bar,
               CASE WHEN m.boxscore_id IS NOT NULL THEN 'confirmed_score_conflict_queue'
                    WHEN x.boxscore_id IS NOT NULL THEN 'no_catalog_game_queue'
                    ELSE 'corroborates_catalog' END route
        FROM np_game_context g
        LEFT JOIN (SELECT DISTINCT boxscore_id FROM g3_context_score_mismatches
                   WHERE confirmed_class) m USING (boxscore_id)
        LEFT JOIN (SELECT DISTINCT boxscore_id FROM g1_boxscores_missing_from_catalog) x USING (boxscore_id)""")
    _csv(con, "SELECT * FROM context_routed ORDER BY boxscore_id", "game_context_routed")
    summary["game_context"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM context_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- family: player game notes ------------------------------------------------------
    con.execute("""
        CREATE TEMP TABLE notes_routed AS
        SELECT n.boxscore_id, n.player_week, n.NFL_player_id, n.note_type, n.note_text,
               n.confidence_bar, n.source_document_id,
               CASE WHEN n.NFL_player_id IS NOT NULL AND n.NFL_player_id <> ''
                    THEN 'historical_notes_sidecar' ELSE 'hold_no_identity' END route
        FROM np_player_game_notes n""")
    _csv(con, "SELECT * FROM notes_routed ORDER BY boxscore_id", "notes_routed")
    summary["notes"] = dict(con.execute(
        "SELECT route, COUNT(*) FROM notes_routed GROUP BY 1 ORDER BY 2 DESC").fetchall())

    # --- unified image-queue inputs (feeds the cross-workstream queue) ------------------
    queue_sql = """
        SELECT 'weekly_disagreement' queue_family, boxscore_id,
               player_week || ':' || atom entity, np_value::VARCHAR np_value,
               v26_value::VARCHAR canon_value, NULL confidence_bar
        FROM weekly_routed WHERE route = 'disagreement_queue'
        UNION ALL
        SELECT 'scoring_over_claim', boxscore_id,
               scoring_team || ':' || event_type, NULL, NULL, confidence_bar
        FROM scoring_routed WHERE route = 'queue_pfr_omission_candidate'
        UNION ALL
        SELECT 'context_score_conflict', boxscore_id,
               COALESCE(team_1_resolved, '') || ' v ' || COALESCE(team_2_resolved, ''),
               team_1_score::VARCHAR || '-' || team_2_score::VARCHAR, NULL, confidence_bar
        FROM context_routed WHERE route = 'confirmed_score_conflict_queue'
        UNION ALL
        SELECT 'no_catalog_game', boxscore_id, 'game', NULL, NULL, confidence_bar
        FROM context_routed WHERE route = 'no_catalog_game_queue'"""
    n_queue = _csv(con, queue_sql, "image_queue_inputs")
    summary["image_queue_inputs"] = n_queue

    # --- rollup -> granular split (Lesson 2: v26 return TDs are granular) ---------------
    # Deduped granular TD events per scorer in clean games (one play reported by many
    # papers is ONE play: count within doc, MAX across docs).
    split_case = " ".join(f"WHEN '{et}' THEN '{g}'" for et, (_, g) in EVENT_GRANULAR_SPLIT.items())
    con.execute(f"""
        CREATE TEMP TABLE ev_granular AS
        SELECT e.boxscore_id, COALESCE(c.cid, e.pid) pid, e.rollup_atom, e.granular_atom,
               MAX(e.n_in_doc) n FROM (
            SELECT boxscore_id, scoring_NFL_player_id pid,
                   CASE event_type {" ".join(f"WHEN '{et}' THEN '{r}'" for et, (r, _) in EVENT_GRANULAR_SPLIT.items())} END rollup_atom,
                   CASE event_type {split_case} END granular_atom,
                   source_document_id, COUNT(*) n_in_doc
            FROM scoring_routed
            WHERE route IN ('corroborated_exact', 'corroborated_partial',
                            'insert_candidate_no_canonical_scoring')
              AND scoring_NFL_player_id IS NOT NULL AND scoring_NFL_player_id <> ''
            GROUP BY 1, 2, 3, 4, 5) e
        LEFT JOIN canon c ON c.raw_id = e.pid
        WHERE e.granular_atom IS NOT NULL GROUP BY 1, 2, 3, 4""")
    granular_subj_case = " ".join(f"WHEN '{g}' THEN s.{g}" for g in GRANULAR_ATOMS)
    con.execute(f"""
        CREATE TEMP TABLE rollup_split_ledger AS
        WITH r AS (
            SELECT * FROM weekly_routed
            WHERE route IN ('fill_candidate', 'insert_candidate') AND atom IN ('def_tds', 'special_teams_tds')),
        agg AS (
            SELECT r.player_week, r.NFL_player_id, r.boxscore_id, r.atom, r.np_value,
                   r.identity_resolution, r.route,
                   COALESCE(SUM(e.n), 0) split_total
            FROM r LEFT JOIN ev_granular e
                 ON e.boxscore_id = r.boxscore_id AND e.pid = r.NFL_player_id
                AND e.rollup_atom = r.atom
            GROUP BY ALL),
        split_atoms AS (
            SELECT a.player_week, a.NFL_player_id, a.boxscore_id, a.identity_resolution,
                   a.route orig_route, e.granular_atom atom, CAST(e.n AS DOUBLE) np_value
            FROM agg a JOIN ev_granular e
                 ON e.boxscore_id = a.boxscore_id AND e.pid = a.NFL_player_id
                AND e.rollup_atom = a.atom
            WHERE a.split_total = a.np_value)
        SELECT sa.*, CASE sa.atom {granular_subj_case} ELSE NULL END v26_granular,
               CASE WHEN s.player_week IS NULL THEN sa.orig_route
                    WHEN CASE sa.atom {granular_subj_case} ELSE NULL END IS NULL THEN 'fill_candidate'
                    WHEN TRY_CAST(CASE sa.atom {granular_subj_case} ELSE NULL END AS DOUBLE) = sa.np_value
                         THEN 'corroborated_granular'
                    ELSE 'disagreement_queue' END route
        FROM split_atoms sa LEFT JOIN subj s ON s.player_week = sa.player_week""")
    _csv(con, "SELECT * FROM rollup_split_ledger ORDER BY boxscore_id", "rollup_split_ledger")
    con.execute("""
        CREATE TEMP TABLE rollup_unsplittable AS
        SELECT w.* FROM weekly_routed w
        WHERE w.route IN ('fill_candidate', 'insert_candidate')
          AND w.atom IN ('def_tds', 'special_teams_tds')
          AND NOT EXISTS (SELECT 1 FROM rollup_split_ledger s
                          WHERE s.player_week = w.player_week AND s.boxscore_id = w.boxscore_id
                            AND s.NFL_player_id = w.NFL_player_id)""")
    summary["rollup_split"] = {
        "split_atoms": dict(con.execute(
            "SELECT route, COUNT(*) FROM rollup_split_ledger GROUP BY 1").fetchall()),
        "needs_granular_split_review": con.execute(
            "SELECT COUNT(*) FROM rollup_unsplittable").fetchone()[0],
    }

    # promotable cell set: non-rollup fills/inserts + exact granular splits
    con.execute("""
        CREATE TEMP TABLE weekly_promotable AS
        SELECT player_week, NFL_player_id, boxscore_id, atom, np_value,
               identity_resolution, route
        FROM weekly_routed
        WHERE route IN ('fill_candidate', 'insert_candidate')
          AND atom NOT IN ('def_tds', 'special_teams_tds')
        UNION ALL
        SELECT player_week, NFL_player_id, boxscore_id, atom, np_value,
               identity_resolution, route
        FROM rollup_split_ledger WHERE route IN ('fill_candidate', 'insert_candidate')""")

    # --- assemble the weekly upsert package (fills + inserts from clean games) ----------
    # position context for inserts: newspaper lineup listing, else empty
    stat_pivot = ", ".join(
        f"MAX(CASE WHEN atom = '{a}' THEN np_value END) AS {c}"
        for a, c in PROMOTABLE_ATOM_COL.items())
    empty_stats = ", ".join(f"NULL AS {c}" for c in PACKAGE_STAT_COLS
                            if c not in PROMOTABLE_ATOM_COL.values())
    package_sql = f"""
        WITH cells AS (SELECT * FROM weekly_promotable),
        pivoted AS (
            SELECT player_week, NFL_player_id, boxscore_id,
                   MAX(identity_resolution) identity_resolution,
                   CASE WHEN MAX(CASE WHEN route = 'insert_candidate' THEN 1 ELSE 0 END) = 1
                        THEN 'insert_candidate' ELSE 'existing_player_week_review' END upsert_state,
                   {stat_pivot}
            FROM cells GROUP BY 1, 2, 3),
        ctx AS (SELECT DISTINCT boxscore_id, year, week, season_type, game_date,
                       team_code, opponent_code FROM tg),
        team_of AS (
            SELECT DISTINCT boxscore_id, NFL_player_id, FIRST_VALUE(nfl_team) OVER (
                     PARTITION BY boxscore_id, NFL_player_id ORDER BY nfl_team) nfl_team
            FROM (SELECT u.boxscore_id, COALESCE(c.cid, u.NFL_player_id) NFL_player_id, u.nfl_team
                  FROM (SELECT boxscore_id, NFL_player_id, nfl_team FROM np_weekly_player_stat_cells
                        WHERE NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL
                        UNION ALL
                        SELECT boxscore_id, resolved_nfl_id, nfl_team FROM resolved
                        WHERE nfl_team IS NOT NULL) u
                  LEFT JOIN canon c ON c.raw_id = u.NFL_player_id)),
        lineup_pos AS (
            SELECT l.boxscore_id, COALESCE(c.cid, l.NFL_player_id) NFL_player_id,
                   STRING_AGG(DISTINCT COALESCE(l.starter_position, l.listed_position_raw), ',') pos
            FROM np_lineup_participation l
            LEFT JOIN canon c ON c.raw_id = l.NFL_player_id
            WHERE l.NFL_player_id IS NOT NULL GROUP BY 1, 2),
        np_names AS (
            SELECT NFL_player_id, ANY_VALUE(np_name) np_name
            FROM (SELECT COALESCE(c.cid, u.NFL_player_id) NFL_player_id, u.np_name
                  FROM (SELECT NFL_player_id, resolved_player np_name FROM np_weekly_player_stat_cells
                        WHERE resolved_player IS NOT NULL
                        UNION ALL
                        SELECT resolved_nfl_id, resolved_player FROM resolved
                        WHERE resolved_player IS NOT NULL) u
                  LEFT JOIN canon c ON c.raw_id = u.NFL_player_id)
            GROUP BY 1)
        SELECT
          '{DATA_SOURCE}' bundle_source,
          p.upsert_state,
          p.upsert_state = 'existing_player_week_review' already_in_supertable,
          p.player_week, p.NFL_player_id,
          COALESCE(b.player, nn.np_name, p.NFL_player_id) player,
          c.year, c.week, c.season_type, c.game_date,
          p.boxscore_id || '_' || t.nfl_team team_game_key,
          t.nfl_team,
          CASE WHEN t.nfl_team = c.team_code THEN c.opponent_code
               WHEN t.nfl_team = c.opponent_code THEN c.team_code END opponent_nfl_team,
          lp.pos source_positions,
          p.identity_resolution,
          '{DATA_SOURCE}' data_source,
          NULL known_stat_cols, NULL nonzero_known_stat_cols, NULL coverage_statuses,
          NULL known_missing_context, NULL stat_completeness_class,
          NULL source_fact_cells, NULL nonzero_source_fact_cells,
          FALSE requires_multi_game_suffix,
          NULL source_player_url, NULL source_boxscore_url,
          p.boxscore_id source_files, NULL derivation_rules,
          {", ".join("p." + c for c in PROMOTABLE_ATOM_COL.values())},
          {empty_stats}
        FROM pivoted p
        JOIN ctx c ON c.boxscore_id = p.boxscore_id
        LEFT JOIN team_of t ON t.boxscore_id = p.boxscore_id AND t.NFL_player_id = p.NFL_player_id
        LEFT JOIN lineup_pos lp ON lp.boxscore_id = p.boxscore_id AND lp.NFL_player_id = p.NFL_player_id
        LEFT JOIN (SELECT NFL_player_id, ANY_VALUE(player) player
                   FROM read_parquet('{os.path.join(DATA_LAKE, "ops_data", "nfl_historical", "player_bio.parquet").replace(chr(92), "/")}')
                   GROUP BY 1) b ON b.NFL_player_id = p.NFL_player_id
        LEFT JOIN np_names nn ON nn.NFL_player_id = p.NFL_player_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY p.player_week ORDER BY p.boxscore_id) = 1"""
    n_package = _csv(con, package_sql, "weekly_upsert_package_rows")
    summary["package_rows"] = n_package
    summary["package_states"] = dict(con.execute(
        f"SELECT upsert_state, COUNT(*) FROM ({package_sql}) GROUP BY 1").fetchall())

    # identity sidecar: package players ABSENT from bio need synthetic-stub candidates
    # (routed through the ancient identity lane, never direct bio writes here).
    identity_sql = f"""
        WITH pkg AS ({package_sql}),
        agg AS (
            SELECT NFL_player_id, ANY_VALUE(player) player,
                   STRING_AGG(DISTINCT identity_resolution, ',') identity_resolutions,
                   STRING_AGG(DISTINCT source_positions, ',') source_positions,
                   MIN(CAST(year AS INT)) first_year, MAX(CAST(year AS INT)) last_year,
                   COUNT(*) weekly_bundle_rows,
                   COUNT(*) FILTER (WHERE upsert_state = 'insert_candidate') insert_candidate_rows,
                   COUNT(*) FILTER (WHERE upsert_state = 'existing_player_week_review') existing_player_week_review_rows
            FROM pkg GROUP BY 1)
        SELECT a.NFL_player_id, a.player,
               b.NFL_player_id IS NOT NULL already_in_player_bio,
               CASE WHEN b.NFL_player_id IS NOT NULL THEN 'existing_player_bio_ok'
                    ELSE 'insert_synthetic_player_bio_candidate' END player_bio_upsert_state,
               a.identity_resolutions, a.source_positions, a.first_year, a.last_year,
               NULL source_player_urls, a.weekly_bundle_rows, a.insert_candidate_rows,
               a.existing_player_week_review_rows,
               a.first_year first_stat_year, a.last_year last_stat_year,
               NULL promoted_stat_atoms, NULL source_url, NULL source_boxscore_url,
               'newspaper_ocr_recovery' source_identity_bundles,
               '{NEWSPAPER_BUNDLE_DIR.replace(chr(92), "/")}' source_identity_paths
        FROM agg a
        LEFT JOIN (SELECT DISTINCT NFL_player_id FROM read_parquet('{bio_path}')) b USING (NFL_player_id)"""
    _csv(con, identity_sql, "player_identity_package_rows")
    summary["identity_rows"] = dict(con.execute(
        f"SELECT player_bio_upsert_state, COUNT(*) FROM ({identity_sql}) GROUP BY 1").fetchall())
    summary["bio_missing_ids"] = [r[0] for r in con.execute(
        f"SELECT NFL_player_id FROM ({identity_sql}) WHERE NOT already_in_player_bio").fetchall()]

    # sanity: no package row may target a non-NULL differing subject cell (Lesson 1/2)
    viols, = con.execute(f"""
        SELECT COUNT(*) FROM ({package_sql}) p
        JOIN subj s USING (player_week)
        WHERE {" OR ".join(
            f"(p.{c} IS NOT NULL AND s.{c} IS NOT NULL AND TRY_CAST(s.{c} AS DOUBLE) <> TRY_CAST(p.{c} AS DOUBLE))"
            for c in PROMOTABLE_ATOM_COL.values())}""").fetchone()
    summary["package_overwrite_violations"] = viols
    if viols:
        raise SystemExit(f"GUARD: {viols} package rows would overwrite non-NULL subject cells")

    # --- stage the source package -------------------------------------------------------
    if stage:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        pkg_dir = STAGE_ROOT / f"{stamp}_pilot_v1"
        pkg_dir.mkdir(parents=True, exist_ok=False)
        pkg_csv = (pkg_dir / "newspaper_weekly_stat_upsert_rows.csv").as_posix()
        con.execute(f"COPY ({package_sql}) TO '{pkg_csv}' (HEADER, DELIMITER ',')")
        idn_csv = (pkg_dir / "newspaper_player_identity_upsert_rows.csv").as_posix()
        con.execute(f"COPY ({identity_sql}) TO '{idn_csv}' (HEADER, DELIMITER ',')")
        manifest = {
            "package": "newspaper_ocr_recovery", "created_at_utc": stamp,
            "bundle_source": DATA_SOURCE,
            "witness_bundle": NEWSPAPER_BUNDLE_DIR,
            "subject_sha256_at_stage": subject_sha,
            "rows": n_package, "states": summary["package_states"],
            "routing_summary": (OUT / "ROUTING_SUMMARY.json").as_posix(),
            "registration_note": (
                "Register in tools/build/build_nfl_recovery_ready_upsert_bundle.py "
                "SOURCE_REL_PATHS as 'newspaper_ocr_recovery' -> this CSV, then re-run the "
                "ready bundle + local apply + derived recompute chain."),
            "write_guarantee": "package dir only; no canon/bundle/v26 writes",
        }
        (pkg_dir / "PACKAGE_MANIFEST.json").write_text(json.dumps(manifest, indent=2))
        summary["staged_package"] = pkg_csv

    con.close()
    (OUT / "ROUTING_SUMMARY.json").write_text(json.dumps(summary, indent=2, default=str))
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stage", action="store_true",
                    help="also write the ready-bundle source package under curated/")
    args = ap.parse_args()
    summary = run(stage=args.stage)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
