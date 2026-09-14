"""
sota_recon/corrections/wave3_dedup_def.py

Wave 3: deduplicate DEF team-game rows WITHOUT losing data.

Two DEF lineages coexist for 2014-2025 modern games:
  - pfr_team_games_team_def_source_reconstruction : authoritative score
        (pts_def_team_pts, points_allowed, dst_points_allowed, pts_allow tiers)
        but MISSING the DST component atoms (def_tds, fum_ret_td, def_safeties,
        special_teams_tds, total_yds_allowed, passing/rushing_tds_allowed).
  - legacy_annual_merged_player_weeks : carries ALL the DST component atoms,
        but its points_allowed disagrees with PFR ~79% (unreliable score).

A blind drop of legacy would destroy the DST scoring components the per-league
Fly settings wiring depends on. So this is a PFR-PRIORITY COALESCE-MERGE:

  for each duplicate (franchise, opp-franchise, year, week) group that is exactly
  one PFR row + one legacy row:
     keep the PFR row; for every stat column, set value = COALESCE(pfr, legacy)
     so PFR wins where present and legacy fills PFR's NULLs (the components);
     then DELETE the legacy row.

The 36 pre-1932 doubleheader groups (two PFR rows, distinct game-ids) are NOT
duplicates and are left untouched. Year-aware franchise number is the dedup key,
so BAL-Colts vs BAL-Ravens never collide.
"""

from __future__ import annotations

from .framework import PROV_COL

PFR_SRC = "pfr_team_games_team_def_source_reconstruction"
LEGACY_SRC = "legacy_annual_merged_player_weeks"

# columns NEVER coalesced (identity / keys / lineage / the score we trust from PFR)
_NO_COALESCE = {
    "player_week", "NFL_player_id", "player", "position",
    "nfl_team", "opponent_nfl_team", "year", "week", "season_type",
    "nfl_franchise_number", "opponent_nfl_franchise_number",
    "data_source", PROV_COL,
    # PFR is authoritative for these — do NOT let legacy overwrite/fill
    "points_allowed", "dst_points_allowed", "pts_def_team_pts",
}


def _dedup_def(con) -> int:
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    # Only coalesce columns the legacy twin actually contributes: where, across the
    # duplicate DEF pairs, the PFR row is NULL but the legacy row has a value. This
    # keeps the UPDATE light (a handful of component cols) instead of all ~730.
    candidate = [c for c in cols if c not in _NO_COALESCE]
    contrib = con.execute(f"""
        SELECT { ', '.join(f'SUM(CASE WHEN P."{c}" IS NULL AND L."{c}" IS NOT NULL THEN 1 ELSE 0 END) AS "{c}"' for c in candidate) }
        FROM st P JOIN st L
          ON P.position='DEF' AND P.data_source='{PFR_SRC}'
         AND L.position='DEF' AND L.data_source='{LEGACY_SRC}'
         AND P.nfl_franchise_number=L.nfl_franchise_number
         AND P.opponent_nfl_franchise_number=L.opponent_nfl_franchise_number
         AND P.year=L.year AND CAST(P.week AS INTEGER)=CAST(L.week AS INTEGER)
    """).fetchone()
    coalesce_cols = [c for c, n in zip(candidate, contrib) if n and n > 0]

    # count legacy rows that have a PFR twin (the rows we will merge+drop)
    n = con.execute(f"""
        SELECT COUNT(*) FROM st L
        WHERE L.position='DEF' AND L.data_source='{LEGACY_SRC}'
          AND EXISTS (
            SELECT 1 FROM st P
            WHERE P.position='DEF' AND P.data_source='{PFR_SRC}'
              AND P.nfl_franchise_number = L.nfl_franchise_number
              AND P.opponent_nfl_franchise_number = L.opponent_nfl_franchise_number
              AND P.year = L.year AND CAST(P.week AS INTEGER) = CAST(L.week AS INTEGER)
          )
    """).fetchone()[0]
    if not n:
        return 0

    # 1) coalesce-fill the PFR row's NULLs from its legacy twin
    #    (identifiers double-quoted: some columns contain special chars like 'xp%')
    set_clause = ",\n            ".join(
        f'"{c}" = COALESCE(P."{c}", L."{c}")' for c in coalesce_cols
    )
    con.execute(f"""
        UPDATE st AS P
        SET {set_clause},
            {PROV_COL} = CASE WHEN P.{PROV_COL} IS NULL OR P.{PROV_COL}=''
                              THEN 'wave3.def_dedup_merge' ELSE P.{PROV_COL} || ',wave3.def_dedup_merge' END
        FROM st AS L
        WHERE P.position='DEF' AND P.data_source='{PFR_SRC}'
          AND L.position='DEF' AND L.data_source='{LEGACY_SRC}'
          AND P.nfl_franchise_number = L.nfl_franchise_number
          AND P.opponent_nfl_franchise_number = L.opponent_nfl_franchise_number
          AND P.year = L.year AND CAST(P.week AS INTEGER) = CAST(L.week AS INTEGER)
    """)

    # 2) delete the legacy twins now that their components are merged in
    con.execute(f"""
        DELETE FROM st AS L
        WHERE L.position='DEF' AND L.data_source='{LEGACY_SRC}'
          AND EXISTS (
            SELECT 1 FROM st P
            WHERE P.position='DEF' AND P.data_source='{PFR_SRC}'
              AND P.nfl_franchise_number = L.nfl_franchise_number
              AND P.opponent_nfl_franchise_number = L.opponent_nfl_franchise_number
              AND P.year = L.year AND CAST(P.week AS INTEGER) = CAST(L.week AS INTEGER)
          )
    """)
    return n


from .framework import Correction

CORRECTIONS = [
    Correction("wave3.def_dedup_merge",
               "merge legacy DST components into PFR DEF twin (PFR-priority coalesce), drop legacy",
               _dedup_def,
               # accurate dry-run count: legacy DEF rows that actually have a PFR twin
               f"""position='DEF' AND data_source='{LEGACY_SRC}'
                   AND EXISTS (SELECT 1 FROM st P WHERE P.position='DEF'
                       AND P.data_source='{PFR_SRC}'
                       AND P.nfl_franchise_number = st.nfl_franchise_number
                       AND P.opponent_nfl_franchise_number = st.opponent_nfl_franchise_number
                       AND P.year = st.year AND CAST(P.week AS INTEGER) = CAST(st.week AS INTEGER))"""),
]
