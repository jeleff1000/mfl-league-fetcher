"""The PFR fumble grammar (Joe, 2026-08-01): every `X fumbles (forced by F),
recovered by R` sentence in the boxscore play-by-play `detail` text becomes one
typed event row.

The pbp table carries aligned `detail_link_texts` / `detail_link_ids` arrays
(semicolon-separated, same order), so a name extracted from the sentence maps
to its pfr_id by list position -- the scoring-log idiom generalized to
mid-sentence actors. Team sides come from the game's own box participant
tables (offense + defense + returns + kicking), which lets lost-vs-kept be
decided per event instead of assumed.

Output: pfr_fumble_events.parquet in the receipts lake. Specs read it via the
`pfr_fumble_events` shape in witness_map.py.

Context classes (priority order -- a sack fumble is not also a rush fumble):
  sack      -- 'sacked by' before the fumble sentence
  aborted   -- 'aborted snap' (PFR omits the word 'fumbles' entirely;
               nflverse counts these as fumbles -- Lamar Jackson 2018
               has 6 of 12 this way)
  receiving -- fumbler was the completed-pass receiver in the same detail
  return    -- punt/kickoff/interception return context
  rushing   -- everything else with a rush-grammar or bare-yardage lead-in
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master"
           ) / "pfr_fumble_events.parquet"

# the '' is SQL quote-doubling: names like Le'Veon carry a literal apostrophe
NAME = r"([A-Z][\w.''-]*(?: [A-Z][\w.''-]*)+)"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    reg = S.registry()
    pbp = Path(reg["pfr_box_pbp"].path).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()
    off = Path(reg["pfr_player_offense_box"].path).as_posix()
    dfb = Path(reg["pfr_player_defense_box"].path).as_posix()

    # (boxscore_id, pfr_id) -> team, from both box sides. Returners/kickers who
    # touched no scrimmage stat can be absent; events with an unknown side keep
    # NULL and the lost flag abstains (proof-or-pending, never COALESCE-0).
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW sides AS
    SELECT boxscore_id, pfr_id, ANY_VALUE(team) AS team FROM (
      SELECT s.boxscore_id,
             UNNEST(str_split(CAST(s.player_link_ids AS VARCHAR), ';')) AS pfr_id,
             s.team
      FROM read_parquet('{off}', union_by_name=true) s
      WHERE s.player_link_ids IS NOT NULL AND s.team IS NOT NULL
      UNION ALL
      SELECT s.boxscore_id,
             UNNEST(str_split(CAST(s.player_link_ids AS VARCHAR), ';')) AS pfr_id,
             s.team
      FROM read_parquet('{dfb}', union_by_name=true) s
      WHERE s.player_link_ids IS NOT NULL AND s.team IS NOT NULL
    ) GROUP BY 1, 2""")

    # A play can contain SEVERAL fumble sentences (strip, recovery, re-fumble):
    # extract every clause, one event each. Clause = 'NAME fumbles ...' to the
    # end of its sentence; the recover/force actors are read from the clause,
    # never the whole play, so multi-fumble plays pair actors correctly.
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW ev AS
    WITH plays AS (
      SELECT s.boxscore_id, TRY_CAST(s.season AS INT) AS season, s.detail,
             str_split(CAST(s.detail_link_texts AS VARCHAR), ';') AS names,
             str_split(CAST(s.detail_link_ids AS VARCHAR), ';') AS ids,
             list_concat(
               regexp_extract_all(s.detail, '{NAME} fumbles[^.]*'),
               regexp_extract_all(s.detail, '{NAME} aborted snap[^.]*')
             ) AS clauses
      FROM read_parquet('{pbp}', union_by_name=true) s
      JOIN (SELECT DISTINCT boxscore_id FROM '{games}'
            WHERE season_type = 'REG') g USING (boxscore_id)
      WHERE (s.detail LIKE '%fumbles%' OR s.detail LIKE '%aborted snap%')
        AND s.detail_link_ids IS NOT NULL
    ), clauses AS (
      SELECT boxscore_id, season, detail, names, ids,
             UNNEST(clauses) AS clause
      FROM plays
    ), parsed AS (
      SELECT boxscore_id, season, detail, names, ids, clause,
             regexp_extract(clause, '^{NAME} (?:fumbles|aborted snap)', 1)
                 AS fumbler_name,
             regexp_extract(clause, 'recovered by {NAME} at', 1) AS rec_name,
             regexp_extract(clause, 'forced by {NAME}', 1) AS forcer_name
      FROM clauses
    )
    SELECT boxscore_id, season,
      ids[list_position(names, fumbler_name)] AS fumbler_id,
      ids[list_position(names, rec_name)] AS recoverer_id,
      ids[list_position(names, forcer_name)] AS forcer_id,
      CASE
        WHEN clause LIKE '%aborted snap%' THEN 'aborted'
        WHEN detail LIKE '%sacked by%' THEN 'sack'
        WHEN regexp_matches(detail,
             'pass complete[^.]* to ' || regexp_escape(fumbler_name))
          THEN 'receiving'
        WHEN detail LIKE '%punt%' OR detail LIKE '%kicks off%'
          OR detail LIKE '%kicks onside%' OR detail LIKE '%intercepted%'
          THEN 'return'
        ELSE 'rushing'
      END AS context
    FROM parsed
    WHERE fumbler_name IS NOT NULL AND fumbler_name <> ''
      AND list_position(names, fumbler_name) IS NOT NULL""")

    con.execute(f"""
    COPY (
      SELECT e.boxscore_id, e.season, e.fumbler_id, e.recoverer_id,
             e.forcer_id, e.context,
             fs.team AS fumbler_team, rs.team AS recoverer_team,
             CASE WHEN fs.team IS NULL OR rs.team IS NULL THEN NULL
                  WHEN fs.team <> rs.team THEN 1 ELSE 0 END AS lost,
             CASE WHEN e.recoverer_id = e.fumbler_id THEN 1
                  WHEN fs.team IS NULL OR rs.team IS NULL THEN NULL
                  WHEN fs.team = rs.team THEN 1 ELSE 0 END AS rec_own
      FROM ev e
      LEFT JOIN sides fs ON fs.boxscore_id = e.boxscore_id
                        AND fs.pfr_id = e.fumbler_id
      LEFT JOIN sides rs ON rs.boxscore_id = e.boxscore_id
                        AND rs.pfr_id = e.recoverer_id
    ) TO '{OUT.as_posix()}' (FORMAT parquet)""")

    stats = con.execute(f"""
    SELECT COUNT(*), MIN(season), MAX(season),
           COUNT(*) FILTER (WHERE recoverer_id IS NOT NULL),
           COUNT(*) FILTER (WHERE lost IS NOT NULL),
           COUNT(*) FILTER (WHERE forcer_id IS NOT NULL)
    FROM read_parquet('{OUT.as_posix()}')""").fetchone()
    ctx = dict(con.execute(f"""
    SELECT context, COUNT(*) FROM read_parquet('{OUT.as_posix()}')
    GROUP BY 1""").fetchall())
    return {"rows": stats[0], "span": [stats[1], stats[2]],
            "with_recoverer": stats[3], "lost_decidable": stats[4],
            "with_forcer": stats[5], "contexts": ctx, "out": str(OUT)}


if __name__ == "__main__":
    import json
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2))
