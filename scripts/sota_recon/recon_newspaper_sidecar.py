"""
sota_recon/recon_newspaper_sidecar.py -- golden-gate recon of the newspaper sidecar
witness bundle against the canonical families it lands on.

Newspaper is a PARTIAL witness (a paper reports what it reports), so the gates are
one-sided where physics demands it:
  * a newspaper claim EXCEEDING canonical truth is a VIOLATION (over-claim)
  * a newspaper claim BELOW canonical truth is partial capture -> fill signal, never a fail
  * reflexive identities must hold BOTH ways inside the bundle (Nagurski runs for a TD vs
    Green Bay => Green Bay is charged a rushing TD allowed; passer TD => receiver TD;
    player sums roll up to team claims; team pairs unpivot losslessly)

Gates
  G1  game_existence      every boxscore_id resolves in nfl_team_games_all (NON_NFL exempt)
  G2  pair_integrity      team_game_stats pair rows == 2 matching unpivoted claims
  G3  final_score         newspaper final_score claims + game_context scores == team_points
  G4  scoring_bijection   per team-game TD/FG/PAT/safety event counts vs scoring_summary
  G5  offense_defense     every scoring event charges a resolvable opponent; emits the
                          defense-allowed rollup; def_interceptions(T) == passing_interceptions(opp)
  G6  player_team_sum     player cell sums roll up to scoring_summary + team claims
  G7  cells_vs_subject    identity-resolved cells vs CURRENT latest_v26() weekly cells
  G8  events_vs_cells     TD/FG/PAT scorers in events have matching player cells (coverage)
  G9  lineups_vs_pfr      newspaper starters vs PFR home/vis_starters (coverage)
  G10 subject_integrity   latest_v26() byte-identical before/after; bundle contains no
                          supertable parquet; subject is never a *_local_only overlay

Outputs: D:\\league-history-data\\nfl\\derived\\validation\\sota_recon_master\\newspaper_sidecar\\
  NEWSPAPER_SIDECAR_RECON_SUMMARY.json + per-gate violation/rollup CSVs.

    python -m scripts.sota_recon.recon_newspaper_sidecar
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import duckdb

from .newspaper_witness_common import (
    ATOM_TO_V26_COL, EVENT_TYPE_MAP, NEWSPAPER_BUNDLE_DIR, STAT_CELL_ATOM_MAP,
    sidecar_path)
from .sources import DATA_LAKE, latest_v26

OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_sidecar"
TG = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "nfl_team_games_all.parquet").replace("\\", "/")
SS = os.path.join(DATA_LAKE, "derived", "scoring_summary", "scoring_summary.parquet").replace("\\", "/")
STARTERS = [os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", t, "_combined.parquet").replace("\\", "/")
            for t in ("home_starters", "vis_starters")]
FORBIDDEN = "nfl_player_stats_all.parquet"


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _csv(con, sql: str, name: str) -> int:
    """Write a query to OUT/name.csv, return row count."""
    n = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
    con.execute(f"COPY ({sql}) TO '{(OUT / (name + '.csv')).as_posix()}' (HEADER)")
    return n


def _case_map(col: str, mapping: dict[str, str]) -> str:
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in mapping.items())
    return f"CASE {col} {whens} ELSE NULL END"


def run() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    subject = latest_v26()
    if "_local_only" in subject:
        raise SystemExit(f"GUARD: subject resolved to a _local_only overlay: {subject}")
    subject_sha_before = _sha256(subject)

    bundle_tables = os.listdir(os.path.join(NEWSPAPER_BUNDLE_DIR, "tables"))
    if FORBIDDEN in bundle_tables or any(not t.startswith("newspaper_") for t in bundle_tables):
        raise SystemExit(f"GUARD: bundle contains non-sidecar files: {bundle_tables}")

    con = duckdb.connect()
    con.execute(f"CREATE VIEW tg AS SELECT * FROM read_parquet('{TG}')")
    con.execute(f"CREATE VIEW ss AS SELECT * FROM read_parquet('{SS}')")
    for t in ("weekly_player_stat_cells", "scoring_events", "team_game_stats",
              "team_game_stat_claims", "game_context", "lineup_participation",
              "play_by_play_events"):
        con.execute(f"CREATE VIEW np_{t} AS SELECT * FROM read_parquet('{sidecar_path('newspaper_' + t)}')")

    gates: dict[str, dict] = {}

    # --- G1 game existence ------------------------------------------------------------
    holds_dir = (Path(NEWSPAPER_BUNDLE_DIR) / "holds").as_posix()
    con.execute(f"""
        CREATE TEMP TABLE held_boxes AS
        SELECT DISTINCT boxscore_id FROM (
            SELECT boxscore_id FROM read_csv_auto('{holds_dir}/remaining_overlay_unmatched_hardhold.csv')
            UNION SELECT boxscore_id FROM read_csv_auto('{holds_dir}/remaining_identity_hardhold_queue.csv')
            UNION SELECT boxscore_id FROM read_csv_auto('{holds_dir}/reviewer_hold_hard_hold_audit.csv'))""")
    con.execute("""
        CREATE TEMP TABLE np_boxes AS
        SELECT boxscore_id, STRING_AGG(DISTINCT src, ',') AS sources FROM (
            SELECT boxscore_id, 'cells' src FROM np_weekly_player_stat_cells
            UNION ALL SELECT boxscore_id, 'events' FROM np_scoring_events
            UNION ALL SELECT boxscore_id, 'pbp' FROM np_play_by_play_events
            UNION ALL SELECT boxscore_id, 'team' FROM np_team_game_stat_claims
            UNION ALL SELECT boxscore_id, 'lineup' FROM np_lineup_participation
            UNION ALL SELECT boxscore_id, 'context' FROM np_game_context
        ) GROUP BY 1""")
    non_nfl = "(SELECT DISTINCT boxscore_id FROM np_game_context WHERE team_1_resolved = 'NON_NFL' OR team_2_resolved = 'NON_NFL')"
    miss_sql = f"""
        SELECT b.boxscore_id, b.sources,
               b.boxscore_id IN {non_nfl} AS non_nfl_exempt,
               b.boxscore_id IN (SELECT boxscore_id FROM held_boxes) AS already_held
        FROM np_boxes b LEFT JOIN (SELECT DISTINCT boxscore_id FROM tg) t USING (boxscore_id)
        WHERE t.boxscore_id IS NULL ORDER BY 1"""
    n_missing = _csv(con, miss_sql, "g1_boxscores_missing_from_catalog")
    n_boxes, = con.execute("SELECT COUNT(*) FROM np_boxes").fetchone()
    n_exempt, n_held = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE non_nfl_exempt),
               COUNT(*) FILTER (WHERE NOT non_nfl_exempt AND already_held)
        FROM ({miss_sql})""").fetchone()
    gates["G1_game_existence"] = {"boxscores": n_boxes, "missing": n_missing,
                                  "non_nfl_exempt": n_exempt, "already_held": n_held,
                                  "queue": n_missing - n_exempt - n_held}

    # --- G2 pair integrity (wide pair rows unpivot losslessly to claims) --------------
    # a claim matches its pair side on raw string, numeric value, or the sum of a
    # comma-separated period line (claims may carry a derived total, e.g. '7,10,0,13' -> 30)
    period_sum = ("list_aggregate(list_transform(string_split({raw}, ','), "
                  "x -> COALESCE(TRY_CAST(x AS DOUBLE), 0)), 'sum')")
    pair_sql = f"""
        WITH pairs AS (
            SELECT pilot_team_stat_key AS pair_key, boxscore_id, stat_name,
                   team_1_nfl_team, team_1_value raw1, TRY_CAST(team_1_value AS DOUBLE) v1,
                   {period_sum.format(raw='team_1_value')} ps1,
                   team_2_nfl_team, team_2_value raw2, TRY_CAST(team_2_value AS DOUBLE) v2,
                   {period_sum.format(raw='team_2_value')} ps2
            FROM np_team_game_stats),
        cl AS (
            SELECT source_pair_key AS pair_key, nfl_team, stat_value raw,
                   TRY_CAST(stat_value AS DOUBLE) v
            FROM np_team_game_stat_claims)
        SELECT p.pair_key, p.boxscore_id, p.stat_name,
               COUNT(c.nfl_team) AS claim_rows,
               COUNT(*) FILTER (WHERE
                    (c.nfl_team = p.team_1_nfl_team AND
                     (c.raw IS NOT DISTINCT FROM p.raw1 OR c.v IS NOT DISTINCT FROM p.v1
                      OR c.v = p.ps1))
                 OR (c.nfl_team = p.team_2_nfl_team AND
                     (c.raw IS NOT DISTINCT FROM p.raw2 OR c.v IS NOT DISTINCT FROM p.v2
                      OR c.v = p.ps2))) AS matched
        FROM pairs p LEFT JOIN cl c USING (pair_key)
        GROUP BY 1, 2, 3"""
    bad_pairs = f"SELECT * FROM ({pair_sql}) WHERE claim_rows > 0 AND matched <> claim_rows"
    n_bad = _csv(con, bad_pairs, "g2_pair_integrity_violations")
    n_pairs, n_paired = con.execute(
        f"SELECT COUNT(*), COUNT(*) FILTER (WHERE claim_rows > 0) FROM ({pair_sql})").fetchone()
    gates["G2_pair_integrity"] = {"pairs": n_pairs, "with_claims": n_paired, "violations": n_bad}

    # --- G3 final score vs canonical --------------------------------------------------
    score_sql = """
        SELECT c.boxscore_id, c.nfl_team, TRY_CAST(c.stat_value AS DOUBLE) np_points,
               t.team_points, t.opponent_code
        FROM np_team_game_stat_claims c
        JOIN tg t ON t.boxscore_id = c.boxscore_id AND t.team_code = c.nfl_team
        WHERE c.stat_name = 'final_score' AND TRY_CAST(c.stat_value AS DOUBLE) IS NOT NULL"""
    n_diff = _csv(con, f"SELECT * FROM ({score_sql}) WHERE np_points <> team_points",
                  "g3_final_score_mismatches")
    n_checked, = con.execute(f"SELECT COUNT(*) FROM ({score_sql})").fetchone()
    # game_context holds CANDIDATE rows too (bootstrap/ocr-followup claims). Only rows whose
    # reconciliation_status marks them confirmed/corroborated/resolved are score-gated; the
    # rest are unadjudicated claims and are reported informationally, never as mismatches.
    confirmed_re = "confirmed|corrobor|resolved|direct_visual|game_mapped|accepted|source_final_score"
    ctx_sql = f"""
        SELECT g.boxscore_id, side.team, side.score np_score, t.team_points,
               g.reconciliation_status,
               COALESCE(REGEXP_MATCHES(g.reconciliation_status, '{confirmed_re}'), FALSE) AS confirmed_class
        FROM np_game_context g
        CROSS JOIN LATERAL (VALUES (g.team_1_resolved, TRY_CAST(g.team_1_score AS DOUBLE)),
                                   (g.team_2_resolved, TRY_CAST(g.team_2_score AS DOUBLE))) side(team, score)
        JOIN tg t ON t.boxscore_id = g.boxscore_id AND t.team_code = side.team
        WHERE side.score IS NOT NULL"""
    _csv(con, f"SELECT * FROM ({ctx_sql}) WHERE np_score <> team_points",
         "g3_context_score_mismatches")
    n_ctx, n_ctx_conf, n_ctx_conf_diff, n_ctx_cand_diff = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE confirmed_class),
               COUNT(*) FILTER (WHERE confirmed_class AND np_score <> team_points),
               COUNT(*) FILTER (WHERE NOT confirmed_class AND np_score <> team_points)
        FROM ({ctx_sql})""").fetchone()
    gates["G3_final_score"] = {"claim_scores_checked": n_checked, "claim_mismatches": n_diff,
                               "context_scores_checked": n_ctx,
                               "context_confirmed_checked": n_ctx_conf,
                               "context_confirmed_mismatches": n_ctx_conf_diff,
                               "context_candidate_diffs_informational": n_ctx_cand_diff}

    # --- G4 scoring bijection vs scoring_summary --------------------------------------
    # Corroboration dedup: multiple newspapers reporting the same play are ONE play. Per
    # (game, team, bucket, scorer) the deduped count is the MAX count within any single
    # source document (a generic 'touchdown' and a typed 'rushing_touchdown' for the same
    # scorer from different papers collapse; two TDs by one scorer inside one paper survive).
    bucket_case = _case_map("event_type", {k: v[0] for k, v in EVENT_TYPE_MAP.items()})
    atom_case = _case_map("event_type", {k: v[1] for k, v in EVENT_TYPE_MAP.items() if v[1]})
    con.execute(f"""
        CREATE TEMP TABLE np_events_deduped AS
        SELECT boxscore_id, scoring_team, bucket, scorer,
               MAX(n_in_doc) AS n,
               MAX(atom) AS atom
        FROM (
            SELECT boxscore_id, scoring_team, bucket,
                   COALESCE(NULLIF(scoring_NFL_player_id, ''), '<unattributed>') scorer,
                   COALESCE(source_documents_json, '<nodoc>') doc,
                   COUNT(*) n_in_doc,
                   MAX(atom) atom
            FROM (SELECT *, {bucket_case} AS bucket, {atom_case} AS atom
                  FROM np_scoring_events)
            GROUP BY 1, 2, 3, 4, 5)
        GROUP BY 1, 2, 3, 4""")
    con.execute("""
        CREATE TEMP TABLE np_team_scoring AS
        SELECT boxscore_id, scoring_team AS team_code,
               COALESCE(SUM(n) FILTER (WHERE bucket = 'td'), 0) td,
               COALESCE(SUM(n) FILTER (WHERE bucket = 'fg'), 0) fg,
               COALESCE(SUM(n) FILTER (WHERE bucket = 'pat'), 0) pat_made,
               COALESCE(SUM(n) FILTER (WHERE bucket = 'safety'), 0) safeties,
               COALESCE(SUM(n) FILTER (WHERE bucket IS NULL), 0) unclassified
        FROM np_events_deduped
        GROUP BY 1, 2""")
    bij_sql = """
        SELECT n.boxscore_id, n.team_code,
               n.td np_td, s.td ss_td, n.fg np_fg, s.fg ss_fg,
               n.pat_made np_pat, s.pat_made ss_pat,
               n.safeties np_safeties, s.safeties ss_safeties, n.unclassified,
               CASE WHEN s.boxscore_id IS NULL THEN 'no_canonical_scoring'
                    WHEN n.td > s.td OR n.fg > s.fg OR n.pat_made > s.pat_made
                         OR n.safeties > s.safeties THEN 'over_violation'
                    WHEN n.td = s.td AND n.fg = s.fg AND n.pat_made = s.pat_made
                         AND n.safeties = s.safeties THEN 'witnessed_equal'
                    ELSE 'partial_under' END verdict
        FROM np_team_scoring n
        LEFT JOIN ss s ON s.boxscore_id = n.boxscore_id AND s.team_code = n.team_code"""
    _csv(con, bij_sql, "g4_scoring_bijection_by_team_game")
    g4 = dict(con.execute(f"SELECT verdict, COUNT(*) FROM ({bij_sql}) GROUP BY 1").fetchall())
    _csv(con, f"SELECT * FROM ({bij_sql}) WHERE verdict = 'over_violation'",
         "g4_scoring_over_violations")
    gates["G4_scoring_bijection"] = {**{k: g4.get(k, 0) for k in
                                        ("witnessed_equal", "partial_under", "over_violation",
                                         "no_canonical_scoring")},
                                     "team_games": sum(g4.values())}

    # --- G5 offense<->defense reflexive (the Nagurski gate) ---------------------------
    allowed_sql = """
        SELECT t.opponent_code AS defense_team, e.boxscore_id, t.year, t.week,
               COALESCE(SUM(n) FILTER (WHERE atom = 'rushing_tds'), 0) rushing_tds_allowed,
               COALESCE(SUM(n) FILTER (WHERE atom = 'receiving_tds'), 0) passing_tds_allowed,
               COALESCE(SUM(n) FILTER (WHERE atom = 'fg_made'), 0) fg_allowed,
               COALESCE(SUM(n) FILTER (WHERE atom = 'pat_made'), 0) pat_allowed,
               COALESCE(SUM(n) FILTER (WHERE atom IN ('def_tds', 'special_teams_tds')), 0) return_tds_allowed
        FROM np_events_deduped e
        JOIN tg t ON t.boxscore_id = e.boxscore_id AND t.team_code = e.scoring_team
        GROUP BY 1, 2, 3, 4"""
    n_allowed = _csv(con, allowed_sql, "g5_defense_allowed_rollup")
    orphan_sql = """
        SELECT e.boxscore_id, e.scoring_team, COUNT(*) events
        FROM np_scoring_events e
        LEFT JOIN tg t ON t.boxscore_id = e.boxscore_id AND t.team_code = e.scoring_team
        WHERE t.boxscore_id IS NULL AND e.scoring_team IS NOT NULL AND e.scoring_team <> ''
        GROUP BY 1, 2"""
    n_orphans = _csv(con, orphan_sql, "g5_events_with_unresolvable_defense")
    mirror_sql = """
        WITH c AS (SELECT boxscore_id, nfl_team, stat_name,
                          MAX(TRY_CAST(stat_value AS DOUBLE)) v
                   FROM np_team_game_stat_claims
                   WHERE stat_name IN ('def_interceptions', 'passing_interceptions',
                                       'interceptions')
                   GROUP BY 1, 2, 3)
        SELECT d.boxscore_id, d.nfl_team defense_team, t.opponent_code offense_team,
               d.v def_interceptions, o.v opp_passing_interceptions
        FROM c d
        JOIN tg t ON t.boxscore_id = d.boxscore_id AND t.team_code = d.nfl_team
        JOIN c o ON o.boxscore_id = d.boxscore_id AND o.nfl_team = t.opponent_code
               AND o.stat_name = 'passing_interceptions'
        WHERE d.stat_name IN ('def_interceptions', 'interceptions')"""
    n_int_pairs, = con.execute(f"SELECT COUNT(*) FROM ({mirror_sql})").fetchone()
    n_int_diff = _csv(con, f"SELECT * FROM ({mirror_sql}) WHERE def_interceptions <> opp_passing_interceptions",
                      "g5_interception_mirror_mismatches")
    gates["G5_offense_defense"] = {"defense_allowed_rows": n_allowed,
                                   "unresolvable_defense_events": n_orphans,
                                   "interception_mirror_pairs": n_int_pairs,
                                   "interception_mirror_mismatches": n_int_diff}

    # --- G6 player -> team sum --------------------------------------------------------
    cell_case = _case_map("stat_name", STAT_CELL_ATOM_MAP)
    con.execute(f"""
        CREATE TEMP TABLE np_player_atoms AS
        SELECT boxscore_id, nfl_team, NFL_player_id, atom,
               MAX(TRY_CAST(stat_value AS DOUBLE)) v
        FROM (SELECT *, COALESCE({cell_case},
                        CASE stat_name WHEN 'touchdowns' THEN 'td_total' END) AS atom
              FROM np_weekly_player_stat_cells)
        WHERE atom IS NOT NULL AND TRY_CAST(stat_value AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2, 3, 4""")
    rollup_sql = """
        WITH team_cells AS (
            SELECT boxscore_id, nfl_team, atom, SUM(v) player_sum
            FROM np_player_atoms
            WHERE atom IN ('rushing_tds', 'receiving_tds', 'fg_made', 'pat_made', 'td_total')
            GROUP BY 1, 2, 3)
        SELECT c.boxscore_id, c.nfl_team, c.atom, c.player_sum,
               CASE c.atom WHEN 'fg_made' THEN s.fg WHEN 'pat_made' THEN s.pat_made
                           WHEN 'td_total' THEN s.td ELSE s.td END canonical_ceiling,
               CASE WHEN s.boxscore_id IS NULL THEN 'no_canonical_scoring'
                    WHEN c.atom IN ('rushing_tds', 'receiving_tds')
                         THEN CASE WHEN c.player_sum > s.td THEN 'over_violation' ELSE 'within_ceiling' END
                    WHEN c.atom = 'td_total'
                         THEN CASE WHEN c.player_sum > s.td THEN 'over_violation'
                                   WHEN c.player_sum = s.td THEN 'witnessed_equal'
                                   ELSE 'partial_under' END
                    WHEN c.atom = 'fg_made'
                         THEN CASE WHEN c.player_sum > s.fg THEN 'over_violation'
                                   WHEN c.player_sum = s.fg THEN 'witnessed_equal'
                                   ELSE 'partial_under' END
                    ELSE CASE WHEN c.player_sum > s.pat_made THEN 'over_violation'
                              WHEN c.player_sum = s.pat_made THEN 'witnessed_equal'
                              ELSE 'partial_under' END END verdict
        FROM team_cells c
        JOIN tg t ON t.boxscore_id = c.boxscore_id AND t.team_code = c.nfl_team
        LEFT JOIN ss s ON s.boxscore_id = c.boxscore_id AND s.team_code = c.nfl_team"""
    _csv(con, rollup_sql, "g6_player_team_rollup")
    g6 = dict(con.execute(f"SELECT verdict, COUNT(*) FROM ({rollup_sql}) GROUP BY 1").fetchall())
    _csv(con, f"SELECT * FROM ({rollup_sql}) WHERE verdict = 'over_violation'",
         "g6_player_team_over_violations")
    pr_sql = """
        WITH s AS (SELECT boxscore_id, nfl_team, atom, SUM(v) v FROM np_player_atoms
                   WHERE atom IN ('passing_tds', 'receiving_tds') GROUP BY 1, 2, 3)
        SELECT p.boxscore_id, p.nfl_team, p.v passing_tds, r.v receiving_tds
        FROM s p JOIN s r ON r.boxscore_id = p.boxscore_id AND r.nfl_team = p.nfl_team
                        AND r.atom = 'receiving_tds'
        WHERE p.atom = 'passing_tds'"""
    n_pr, = con.execute(f"SELECT COUNT(*) FROM ({pr_sql})").fetchone()
    n_pr_diff = _csv(con, f"SELECT * FROM ({pr_sql}) WHERE passing_tds <> receiving_tds",
                     "g6_passing_receiving_mirror_diffs")
    gates["G6_player_team_sum"] = {**{k: g6.get(k, 0) for k in
                                      ("witnessed_equal", "partial_under", "within_ceiling",
                                       "over_violation", "no_canonical_scoring")},
                                   "passing_receiving_pairs": n_pr,
                                   "passing_receiving_diffs": n_pr_diff}

    # --- G7 cells vs current v26 subject ----------------------------------------------
    v26_cols = ", ".join(f"s.{c}" for c in ATOM_TO_V26_COL.values())
    con.execute(f"""
        CREATE TEMP TABLE subj AS
        SELECT s.player_week, {v26_cols}
        FROM read_parquet('{Path(subject).as_posix()}') s
        SEMI JOIN (SELECT DISTINCT player_week FROM np_weekly_player_stat_cells) c
             ON s.player_week = c.player_week""")
    atom_values = " ".join(f"WHEN '{a}' THEN s.{c}" for a, c in ATOM_TO_V26_COL.items())
    subj_sql = f"""
        SELECT c.player_week, c.NFL_player_id, c.boxscore_id, c.atom,
               c.v np_value, CASE c.atom {atom_values} ELSE NULL END v26_value,
               CASE WHEN s.player_week IS NULL THEN 'missing_row'
                    WHEN CASE c.atom {atom_values} ELSE NULL END IS NULL THEN 'fill_signal'
                    WHEN TRY_CAST(CASE c.atom {atom_values} ELSE NULL END AS DOUBLE) = c.v
                         THEN 'witnessed_equal'
                    ELSE 'witnessed_diff' END verdict
        FROM (SELECT p.*, w.player_week FROM np_player_atoms p
              JOIN (SELECT DISTINCT boxscore_id, NFL_player_id, player_week
                    FROM np_weekly_player_stat_cells) w
                   USING (boxscore_id, NFL_player_id)
              WHERE p.atom IN ({', '.join(repr(a) for a in ATOM_TO_V26_COL)})) c
        LEFT JOIN subj s USING (player_week)"""
    _csv(con, subj_sql, "g7_cells_vs_subject")
    g7 = dict(con.execute(f"SELECT verdict, COUNT(*) FROM ({subj_sql}) GROUP BY 1").fetchall())
    _csv(con, f"SELECT * FROM ({subj_sql}) WHERE verdict = 'witnessed_diff'",
         "g7_witnessed_diffs")
    gates["G7_cells_vs_subject"] = {k: g7.get(k, 0) for k in
                                    ("witnessed_equal", "witnessed_diff", "fill_signal",
                                     "missing_row")}

    # --- G8 events -> cells internal coverage -----------------------------------------
    ev_cell_sql = """
        WITH ev AS (
            SELECT boxscore_id, scorer AS pid, atom, SUM(n) n
            FROM np_events_deduped
            WHERE atom IN ('rushing_tds', 'receiving_tds', 'fg_made', 'pat_made')
              AND scorer <> '<unattributed>'
            GROUP BY 1, 2, 3)
        SELECT ev.*, p.v cell_value,
               CASE WHEN p.v IS NULL THEN 'no_cell'
                    WHEN p.v >= ev.n THEN 'covered'
                    ELSE 'cell_below_events' END verdict
        FROM ev LEFT JOIN np_player_atoms p
             ON p.boxscore_id = ev.boxscore_id AND p.NFL_player_id = ev.pid
            AND (p.atom = ev.atom OR (p.atom = 'td_total'
                 AND ev.atom IN ('rushing_tds', 'receiving_tds')))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ev.boxscore_id, ev.pid, ev.atom
                                   ORDER BY CASE WHEN p.atom = ev.atom THEN 0 ELSE 1 END) = 1"""
    _csv(con, ev_cell_sql, "g8_event_cell_coverage")
    g8 = dict(con.execute(f"SELECT verdict, COUNT(*) FROM ({ev_cell_sql}) GROUP BY 1").fetchall())
    gates["G8_events_vs_cells"] = {k: g8.get(k, 0) for k in
                                   ("covered", "no_cell", "cell_below_events")}

    # --- G9 lineups vs PFR starters ---------------------------------------------------
    starters_union = " UNION ALL ".join(
        f"SELECT boxscore_id, player_link_ids FROM read_parquet('{p}')" for p in STARTERS)
    lineup_sql = f"""
        WITH st AS (
            SELECT s.boxscore_id, s.player_link_ids
            FROM ({starters_union}) s
            SEMI JOIN (SELECT DISTINCT boxscore_id FROM np_lineup_participation) l
                 ON s.boxscore_id = l.boxscore_id)
        SELECT l.boxscore_id, l.NFL_player_id, l.starter_position,
               CASE WHEN NOT EXISTS (SELECT 1 FROM st WHERE st.boxscore_id = l.boxscore_id)
                    THEN 'pfr_has_no_starters'
                    WHEN EXISTS (SELECT 1 FROM st WHERE st.boxscore_id = l.boxscore_id
                                 AND st.player_link_ids LIKE '%' || l.NFL_player_id || '%')
                    THEN 'matched' ELSE 'not_in_pfr_starters' END verdict
        FROM np_lineup_participation l
        WHERE l.NFL_player_id IS NOT NULL AND l.NFL_player_id <> ''"""
    _csv(con, lineup_sql, "g9_lineup_vs_pfr_starters")
    g9 = dict(con.execute(f"SELECT verdict, COUNT(*) FROM ({lineup_sql}) GROUP BY 1").fetchall())
    gates["G9_lineups_vs_pfr"] = {k: g9.get(k, 0) for k in
                                  ("matched", "not_in_pfr_starters", "pfr_has_no_starters")}
    con.close()

    # --- G10 subject integrity --------------------------------------------------------
    subject_sha_after = _sha256(subject)
    gates["G10_subject_integrity"] = {
        "subject": subject,
        "sha256_before": subject_sha_before,
        "sha256_after": subject_sha_after,
        "unchanged": subject_sha_before == subject_sha_after,
        "bundle_forbidden_file_present": FORBIDDEN in bundle_tables,
    }
    if not gates["G10_subject_integrity"]["unchanged"]:
        raise SystemExit("GUARD: subject supertable changed during recon run")

    # Structural failures break the witness architecture itself; adjudication items are
    # newspaper-vs-canonical conflicts that queue for image verification (Joe's wave56
    # doctrine: extraction never overrides, humans/models read the page). A partial witness
    # with open adjudication queues is "review", never "fail".
    structural = (gates["G2_pair_integrity"]["violations"]
                  + (0 if gates["G10_subject_integrity"]["unchanged"] else 1)
                  + (1 if gates["G10_subject_integrity"]["bundle_forbidden_file_present"] else 0))
    adjudication = {
        "g1_unmatched_games": gates["G1_game_existence"]["queue"],
        "g3_confirmed_score_conflicts": (gates["G3_final_score"]["claim_mismatches"]
                                         + gates["G3_final_score"]["context_confirmed_mismatches"]),
        "g4_scoring_over_claims": gates["G4_scoring_bijection"]["over_violation"],
        "g5_unresolvable_defense_events": gates["G5_offense_defense"]["unresolvable_defense_events"],
        "g5_interception_mirror": gates["G5_offense_defense"]["interception_mirror_mismatches"],
        "g6_player_team_over_claims": gates["G6_player_team_sum"]["over_violation"],
        "g6_passing_receiving_diffs": gates["G6_player_team_sum"]["passing_receiving_diffs"],
        "g7_subject_disagreements": gates["G7_cells_vs_subject"]["witnessed_diff"],
        "g8_cell_below_events": gates["G8_events_vs_cells"]["cell_below_events"],
    }
    n_adjudication = sum(adjudication.values())
    summary = {
        "lane": "newspaper_sidecar",
        "bundle": NEWSPAPER_BUNDLE_DIR,
        "subject": subject,
        "status": "fail" if structural else ("review" if n_adjudication else "pass"),
        "structural_violations": structural,
        "adjudication_queue_total": n_adjudication,
        "adjudication_queue": adjudication,
        "gates": gates,
        "cloud_write_performed": False,
        "destructive_actions_performed": False,
        "write_guarantee": "Recon artifacts only under sota_recon_master/newspaper_sidecar; "
                           "subject supertable verified byte-identical before/after.",
    }
    with open(OUT / "NEWSPAPER_SIDECAR_RECON_SUMMARY.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
