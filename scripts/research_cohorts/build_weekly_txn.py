"""Build the canonical weekly transaction research table from local lake snapshots.

The source and inclusive year window use the same environment contract as the season
builders (``RESEARCH_*_PATH`` and ``RESEARCH_YEAR_START/END``).  Exact format rows and
all-format rollups are both retained so best-ball, dynasty, and keeper leagues remain
available for analysis without changing the legacy all-format research page.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pyarrow as pa

try:
    from .build_research_txn_cohort import _ADD, _TXN_EXTRAS, pruned_psv_sql
    from .cohort_format_sql import (
        FORMAT_SELECT,
        cohort_league_settings_sql,
        grouping_sets_with_formats,
    )
    from .year_config import configured_years, year_predicate
except ImportError:
    from build_research_txn_cohort import _ADD, _TXN_EXTRAS, pruned_psv_sql
    from cohort_format_sql import FORMAT_SELECT, cohort_league_settings_sql, grouping_sets_with_formats
    from year_config import configured_years, year_predicate


ROOT = Path(__file__).resolve().parents[2]
CO = Path(os.environ.get(
    "RESEARCH_OUT_DIR",
    "D:/league-history-data/fantasy_leagues/cohort_aggregates",
))
OUT = CO / "research_txn_weekly.parquet"
DB = CO / "research_cohorts.duckdb"
YEARS = configured_years()
YEAR_PREDICATE = year_predicate("year")

MIN_STABLE = 35
MIN_DISPLAY = 10
ADD_MIN = 5

_FORMAT_FIELDS = [
    pa.field("teams", pa.string()), pa.field("roster", pa.string()),
    pa.field("ppr", pa.string()), pa.field("td", pa.string()),
    pa.field("league_type", pa.string()), pa.field("lineup_mode", pa.string()),
    pa.field("keeper_mode", pa.string()),
]
WEEKLY_SCHEMA = pa.schema([
    *_FORMAT_FIELDS,
    pa.field("year", pa.int64()), pa.field("week", pa.int64()),
    pa.field("NFL_player_id", pa.string()), pa.field("pos_grp", pa.string()),
    pa.field("n_add_lg", pa.int64()),
    pa.field("avg_add_lamar", pa.float64()), pa.field("avg_faab_pct", pa.float64()),
    pa.field("avg_faab_bid", pa.float64()),
    pa.field("sum_add_lamar", pa.float64()), pa.field("n_add_lamar", pa.int64()),
    pa.field("sum_faab_pct", pa.float64()), pa.field("n_faab_pct", pa.int64()),
    pa.field("sum_faab_bid", pa.float64()), pa.field("n_faab_bid", pa.int64()),
])
DENOM_SCHEMA = pa.schema([
    *_FORMAT_FIELDS,
    pa.field("year", pa.int64()), pa.field("pos_grp", pa.string()),
    pa.field("n_leagues", pa.int64()),
])

_GS_WEEKLY = grouping_sets_with_formats(
    [("teams", "roster", "ppr", "td")],
    tail=("week", "NFL_player_id"),
)
_GS_DENOM = grouping_sets_with_formats(
    [("teams", "roster", "ppr", "td")],
    tail=("year", "pos_grp"),
)


def typed_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


def weekly_sql(year: int) -> str:
    """Return one year's exact-cohort weekly add primitives plus format rollups."""
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_TXN_EXTRAS)}),
pos AS (
  SELECT NFL_player_id, position, broad_positions
  FROM public.player_position WHERE year = {year}
),
btx AS (
  SELECT DISTINCT ls.teams || '_flx_' || ls.ppr || '_' || ls.td AS slug,
         t.NFL_player_id, t.week
  FROM public.transactions t
  JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year AND ls.roster = 'flx'
  WHERE t.year = {year} AND t.NFL_player_id IS NOT NULL AND t.week IS NOT NULL
    AND t.transaction_type IN {_ADD}
),
psv AS MATERIALIZED ({pruned_psv_sql(year)}),
rosm AS (
  SELECT tx.db_name, tx.NFL_player_id, tx.week, SUM(psv.lamar) AS rosm_lamar
  FROM (
    SELECT DISTINCT t.db_name, t.NFL_player_id, t.week, ls.teams, ls.ppr, ls.td
    FROM public.transactions t
    JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year AND ls.roster = 'flx'
    WHERE t.year = {year} AND t.NFL_player_id IS NOT NULL AND t.week IS NOT NULL
      AND t.transaction_type IN {_ADD}
  ) tx
  JOIN (
    SELECT DISTINCT p.db_name, p.NFL_player_id, p.week
    FROM public.player_fantasy p
    JOIN ls ON ls.db_name = p.db_name AND ls.year = p.year
    WHERE p.year = {year} AND CAST(p.is_rostered AS INT) = 1
      AND p.NFL_player_id IS NOT NULL
  ) pf ON pf.db_name = tx.db_name AND pf.NFL_player_id = tx.NFL_player_id
      AND pf.week > tx.week
  JOIN psv ON psv.slug = tx.teams || '_flx_' || tx.ppr || '_' || tx.td
      AND psv.NFL_player_id = pf.NFL_player_id AND psv.week = pf.week
  GROUP BY 1, 2, 3
),
ms AS (
  SELECT db_name, MAX(spend) AS max_spend FROM (
    SELECT t.db_name, t.franchise_id, SUM(t.faab_bid) AS spend
    FROM public.transactions t JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year
    WHERE t.year = {year} AND t.faab_bid > 0 AND t.franchise_id IS NOT NULL
    GROUP BY 1, 2
  ) GROUP BY 1
),
mb AS (
  SELECT t.db_name, MAX(t.faab_bid) AS max_bid
  FROM public.transactions t JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year
  WHERE t.year = {year} AND t.faab_bid > 0 GROUP BY 1
),
tx AS (
  SELECT t.NFL_player_id, ls.teams, ls.roster, ls.ppr, ls.td,
         ls.league_type, ls.lineup_mode, ls.keeper_mode, t.week, t.db_name,
         rosm.rosm_lamar AS manager_lamar_ros_managed, t.faab_bid,
         100.0 * t.faab_bid / NULLIF(
           CASE WHEN ls.waiver_budget > 0 AND COALESCE(mb.max_bid, 0) <= ls.waiver_budget
                THEN ls.waiver_budget
                ELSE GREATEST(COALESCE(ms.max_spend, 0), COALESCE(mb.max_bid, 0)) END,
           0
         ) AS faab_pct,
         CASE WHEN pos.position IN ('K','DEF') THEN pos.position ELSE 'SKILL' END AS pos_grp
  FROM public.transactions t JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year
  LEFT JOIN mb ON mb.db_name = t.db_name
  LEFT JOIN ms ON ms.db_name = t.db_name
  LEFT JOIN rosm ON rosm.db_name = t.db_name
       AND rosm.NFL_player_id = t.NFL_player_id AND rosm.week = t.week
  LEFT JOIN pos ON pos.NFL_player_id = t.NFL_player_id
  WHERE t.year = {year} AND t.NFL_player_id IS NOT NULL AND t.week IS NOT NULL
    AND t.transaction_type IN {_ADD}
    AND (COALESCE(pos.position,'') <> 'K' OR ls.k_slots > 0)
    AND (COALESCE(pos.position,'') <> 'DEF' OR ls.def_slots > 0)
    AND (list_has_any(pos.broad_positions, ['QB','RB','WR','TE','K','DEF'])
         OR (ls.roster = 'idp' AND list_has_any(pos.broad_positions, ['DL','LB','DB'])))
)
SELECT COALESCE(teams,'ALL') AS teams, COALESCE(roster,'ALL') AS roster,
       COALESCE(ppr,'ALL') AS ppr, COALESCE(td,'ALL') AS td, {FORMAT_SELECT},
       {year} AS year, week, NFL_player_id, MAX(pos_grp) AS pos_grp,
       COUNT(DISTINCT db_name) AS n_add_lg,
       AVG(manager_lamar_ros_managed) AS avg_add_lamar,
       AVG(CASE WHEN faab_pct > 0 THEN faab_pct END) AS avg_faab_pct,
       AVG(CASE WHEN faab_bid > 0 THEN faab_bid END) AS avg_faab_bid,
       SUM(manager_lamar_ros_managed) AS sum_add_lamar,
       COUNT(manager_lamar_ros_managed) AS n_add_lamar,
       SUM(CASE WHEN faab_pct > 0 THEN faab_pct END) AS sum_faab_pct,
       COUNT(CASE WHEN faab_pct > 0 THEN 1 END) AS n_faab_pct,
       SUM(CASE WHEN faab_bid > 0 THEN faab_bid END) AS sum_faab_bid,
       COUNT(CASE WHEN faab_bid > 0 THEN 1 END) AS n_faab_bid
FROM tx GROUP BY {_GS_WEEKLY}
"""


DENOM_SQL = f"""
WITH ls AS ({cohort_league_settings_sql(extra_select=_TXN_EXTRAS)}),
elig AS (
  SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
         'SKILL' AS pos_grp FROM ls
  UNION ALL SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode,
         keeper_mode, 'K' FROM ls WHERE k_slots > 0
  UNION ALL SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode,
         keeper_mode, 'DEF' FROM ls WHERE def_slots > 0
)
SELECT COALESCE(teams,'ALL') AS teams, COALESCE(roster,'ALL') AS roster,
       COALESCE(ppr,'ALL') AS ppr, COALESCE(td,'ALL') AS td, {FORMAT_SELECT},
       year, pos_grp, COUNT(DISTINCT db_name) AS n_leagues
FROM elig WHERE {YEAR_PREDICATE} GROUP BY {_GS_DENOM}
"""


def main() -> None:
    env_path = ROOT / ".env"
    for line in (env_path.read_text(encoding="utf-8") if env_path.exists() else "").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))

    sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))
    from local_reader import LocalReader

    reader = LocalReader()
    CO.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='2000MB'")
    rows: list[dict] = []
    for y in YEARS:
        year_rows = reader.query(weekly_sql(y), "___leagues")
        rows.extend(year_rows)
        print(f"  [weekly-txn] {y}: {len(year_rows):,} player-week-cohort add rows", flush=True)

    con.register("w", typed_table(rows, WEEKLY_SCHEMA))
    con.register("d", typed_table(reader.query(DENOM_SQL, "___leagues"), DENOM_SCHEMA))
    con.execute(f"""
      CREATE TABLE weekly AS
      WITH j AS (
        SELECT w.teams,w.roster,w.ppr,w.td,
          w.league_type,w.lineup_mode,w.keeper_mode,
          4 AS cohort_level,
          (CASE WHEN w.league_type<>'ALL' THEN 1 ELSE 0 END)
          +(CASE WHEN w.lineup_mode<>'ALL' THEN 1 ELSE 0 END)
          +(CASE WHEN w.keeper_mode<>'ALL' THEN 1 ELSE 0 END) AS format_level,
          w.year,w.week,w.NFL_player_id,d.n_leagues,w.n_add_lg,
          ROUND(w.sum_add_lamar/NULLIF(w.n_add_lamar,0),2) AS avg_add_lamar,
          ROUND(w.sum_faab_pct/NULLIF(w.n_faab_pct,0),1) AS avg_faab_pct,
          ROUND(w.sum_faab_bid/NULLIF(w.n_faab_bid,0),1) AS avg_faab_bid,
          w.sum_add_lamar,w.n_add_lamar,w.sum_faab_pct,w.n_faab_pct,
          w.sum_faab_bid,w.n_faab_bid,
          ROUND(100.0*w.n_add_lg/NULLIF(d.n_leagues,0),1) AS add_rate_pct
        FROM w JOIN d USING (
          teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,year,pos_grp
        )
      )
      SELECT j.*,
        CASE WHEN n_add_lg >= {ADD_MIN} THEN ROUND(100.0*PERCENT_RANK() OVER (
          PARTITION BY teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,year,week
          ORDER BY avg_add_lamar),1) END AS add_grade,
        CASE WHEN n_add_lg >= {ADD_MIN} THEN 'graded' ELSE 'thin' END AS add_grade_conf,
        CASE WHEN n_leagues >= {MIN_STABLE} THEN 'confident'
             WHEN n_leagues >= {MIN_DISPLAY} THEN 'mushy' ELSE 'insufficient' END AS confidence
      FROM j
    """)
    con.execute(f"""COPY (
      SELECT * FROM weekly ORDER BY year,week,add_rate_pct DESC
    ) TO '{OUT.as_posix()}'""")
    n = con.execute("""SELECT COUNT(*),
        COUNT(*) FILTER (WHERE confidence='confident'),
        COUNT(*) FILTER (WHERE add_grade IS NOT NULL) FROM weekly""").fetchone()
    con.close()
    reader.close()

    with duckdb.connect(str(DB)) as out_db:
        out_db.execute(
            f"CREATE OR REPLACE TABLE transactions_weekly AS "
            f"SELECT * FROM read_parquet('{OUT.as_posix()}')"
        )
    print(f"weekly txn rows: {n[0]:,} ({n[1]:,} confident; {n[2]:,} graded)")
    print(f"added transactions_weekly to {DB}")


if __name__ == "__main__":
    main()
