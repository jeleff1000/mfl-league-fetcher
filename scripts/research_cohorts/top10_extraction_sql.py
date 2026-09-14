"""Lean production-equivalent event facts for the rank-stability cache.

The production builders enrich Draft and Transactions with expensive roster-window
value joins. Rank resampling needs the behavior/capital primitives independently, so
these queries retain the same cohort, completeness, position, auction, FAAB, and
quarantine rules without scanning ``player_fantasy``. Canonical value facts are joined
from the ops cache in a separate lane.
"""

from __future__ import annotations

from build_research_draft_cohort import _DRAFT_EXTRAS
from build_research_matchup_cohort import _MATCHUP_EXTRAS
from build_research_txn_cohort import _ADD, _TXN_EXTRAS
from cohort_format_sql import cohort_league_settings_sql


def draft_population_sql(year: int) -> str:
    """Every complete-draft league eligible to enter an annual draw."""
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_DRAFT_EXTRAS)}),
dsize AS (
  SELECT db_name,year,COUNT(*) picks,
         COUNT(*) FILTER (WHERE COALESCE(cost,0)>0) paid_picks
  FROM public.draft WHERE year={int(year)} AND pick IS NOT NULL GROUP BY 1,2
)
SELECT ls.db_name,ls.year,ls.teams,ls.roster,ls.ppr,ls.td,ls.league_type,
       ls.lineup_mode,ls.keeper_mode,
       (z.paid_picks >= 0.25*z.picks) AS is_auction,
       1::TINYINT AS skill_eligible,
       (ls.k_slots>0)::TINYINT AS k_eligible,
       (ls.def_slots>0)::TINYINT AS def_eligible
FROM ls JOIN dsize z ON z.db_name=ls.db_name AND z.year=ls.year
WHERE z.picks >= 10*NULLIF(ls.num_teams,0)
"""


def transaction_population_sql(year: int) -> str:
    """All gated leagues, including those with no transaction in the year."""
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_TXN_EXTRAS)})
SELECT db_name,year,teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,
       1::TINYINT AS skill_eligible,
       (k_slots>0)::TINYINT AS k_eligible,
       (def_slots>0)::TINYINT AS def_eligible
FROM ls
"""


def matchup_week_population_sql(year: int) -> str:
    """Per-league active weeks and position/championship eligibility."""
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_MATCHUP_EXTRAS)}),
lw AS (
  SELECT p.db_name,p.year,MAX(p.week) mw
  FROM public.player_fantasy p JOIN ls ON ls.db_name=p.db_name AND ls.year=p.year
  WHERE p.year={int(year)} AND p.week IS NOT NULL GROUP BY 1,2
),
cw AS (
  SELECT DISTINCT p.db_name,p.year,p.week
  FROM public.player_fantasy p JOIN ls ON ls.db_name=p.db_name AND ls.year=p.year
  WHERE p.year={int(year)} AND CAST(p.champion AS INT)=1 AND p.week IS NOT NULL
),
elig AS (
  SELECT ls.*,lw.mw FROM ls JOIN lw USING (db_name,year)
),
wk AS (SELECT CAST(UNNEST(range(1,23)) AS INTEGER) AS "week")
SELECT e.db_name,e.year,e.teams,e.roster,e.ppr,e.td,e.league_type,
       e.lineup_mode,e.keeper_mode,wk.week,
       1::TINYINT AS skill_eligible,
       (e.k_slots>0)::TINYINT AS k_eligible,
       (e.def_slots>0)::TINYINT AS def_eligible,
       (cw.week IS NOT NULL)::TINYINT AS is_champ_week
FROM elig e JOIN wk ON wk.week <= e.mw
LEFT JOIN cw ON cw.db_name=e.db_name AND cw.year=e.year AND cw.week=wk.week
"""


def matchup_active_sql(year: int) -> str:
    """NFL-active player weeks, independent of the sampled fantasy leagues."""
    return f"""
SELECT a.NFL_player_id,a.year,a.week,p.position
FROM public.player_active_week a
LEFT JOIN public.player_position p
  ON p.NFL_player_id=a.NFL_player_id AND p.year=a.year
WHERE a.year={int(year)}
"""


def light_matchup_sql(year: int) -> str:
    """Per-league player/week facts without the redundant window-sort regroup."""
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_MATCHUP_EXTRAS)}),
pos AS (
  SELECT NFL_player_id,position,broad_positions
  FROM public.player_position WHERE year={int(year)}
),
act AS (
  SELECT NFL_player_id,week
  FROM public.player_active_week WHERE year={int(year)}
),
pfd AS (
  SELECT p.db_name,p.year,p.week,p.NFL_player_id,
         1::TINYINT AS rostered_leagues,
         MAX(CASE WHEN CAST(p.is_started AS INT)=1 THEN 1 ELSE 0 END)::TINYINT
           AS started_leagues,
         MAX(CASE WHEN CAST(p.is_started AS INT)=1 AND CAST(p.win AS INT)=1
                  THEN 1 ELSE 0 END)::TINYINT AS wins_started,
         MAX(CASE WHEN CAST(p.is_started AS INT)=1 AND CAST(p.win AS INT)=0
                  THEN 1 ELSE 0 END)::TINYINT AS losses_started,
         MAX(CASE WHEN CAST(p.is_started AS INT)=1 THEN p.fantasy_points END)
           AS points_started,
         MAX(CASE WHEN CAST(p.is_started AS INT)=1 THEN p.clutch_equity END)
           AS clutch_sum,
         MAX(CASE WHEN CAST(p.is_started AS INT)=1 AND CAST(p.champion AS INT)=1
                  THEN 1 ELSE 0 END)::TINYINT AS champ_started
  FROM public.player_fantasy p
  JOIN ls ON ls.db_name=p.db_name AND ls.year=p.year
  WHERE p.year={int(year)} AND CAST(p.is_rostered AS INT)=1
    AND p.NFL_player_id IS NOT NULL AND p.week IS NOT NULL
  GROUP BY p.db_name,p.year,p.week,p.NFL_player_id
)
SELECT p.db_name,ls.teams,ls.roster,ls.ppr,ls.td,
       COALESCE(ls.league_type,'ALL') AS league_type,
       COALESCE(ls.lineup_mode,'ALL') AS lineup_mode,
       COALESCE(ls.keeper_mode,'ALL') AS keeper_mode,
       p.year,p.week,p.NFL_player_id,pos.position,
       CASE WHEN pos.position IN ('K','DEF') THEN pos.position ELSE 'SKILL' END
         AS pos_grp,
       p.rostered_leagues,p.started_leagues,p.wins_started,p.losses_started,
       p.points_started,p.clutch_sum,p.champ_started
FROM pfd p
JOIN ls ON ls.db_name=p.db_name AND ls.year=p.year
JOIN act ON act.NFL_player_id=p.NFL_player_id AND act.week=p.week
LEFT JOIN pos ON pos.NFL_player_id=p.NFL_player_id
WHERE (COALESCE(pos.position,'')<>'K' OR ls.k_slots>0)
  AND (COALESCE(pos.position,'')<>'DEF' OR ls.def_slots>0)
  AND (list_has_any(pos.broad_positions,['QB','RB','WR','TE','K','DEF'])
       OR (ls.roster='idp' AND list_has_any(pos.broad_positions,['DL','LB','DB'])))
"""


def light_draft_sql(year: int) -> str:
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_DRAFT_EXTRAS)}),
pos AS (
  SELECT NFL_player_id, position, broad_positions
  FROM public.player_position WHERE year = {int(year)}
),
dsize AS (
  SELECT d.db_name, COUNT(*) AS picks,
         COUNT(*) FILTER (WHERE COALESCE(d.cost,0) > 0) AS paid_picks
  FROM public.draft d JOIN ls ON ls.db_name=d.db_name AND ls.year=d.year
  WHERE d.year={int(year)} AND d.pick IS NOT NULL GROUP BY 1
),
aspend AS (
  SELECT d.db_name, COALESCE(d.manager,d.franchise_id) AS team, SUM(d.cost) AS spend
  FROM public.draft d JOIN ls ON ls.db_name=d.db_name AND ls.year=d.year
  WHERE d.year={int(year)} AND COALESCE(d.cost,0)>0
    AND COALESCE(d.manager,d.franchise_id) IS NOT NULL GROUP BY 1,2
),
arank AS (
  SELECT db_name,spend,ROW_NUMBER() OVER (PARTITION BY db_name ORDER BY spend DESC) rk
  FROM aspend
),
atop AS (
  SELECT db_name,MAX(CASE WHEN rk=3 THEN spend END) third,
         MAX(CASE WHEN rk=1 THEN spend END) top1 FROM arank GROUP BY 1
),
atot AS (
  SELECT d.db_name,SUM(d.cost) total_cost,MAX(ls.num_teams) nt
  FROM public.draft d JOIN ls ON ls.db_name=d.db_name AND ls.year=d.year
  WHERE d.year={int(year)} AND COALESCE(d.cost,0)>0 GROUP BY 1
),
abud AS (
  SELECT t.db_name,COALESCE(a.third,a.top1,t.total_cost/NULLIF(t.nt,0)) budget
  FROM atot t LEFT JOIN atop a USING (db_name)
),
picks AS (
  SELECT d.db_name,ls.teams,ls.roster,ls.ppr,ls.td,ls.league_type,
         ls.lineup_mode,ls.keeper_mode,d.NFL_player_id,pos.position,d.pick,d.cost,
         COALESCE(d.is_keeper,false) is_keeper,
         z.paid_picks >= 0.25*z.picks AS is_auction,
         CASE WHEN z.paid_picks >= 0.25*z.picks AND COALESCE(d.cost,0)>0
              THEN 100.0*d.cost/NULLIF(ab.budget,0) END AS cost_pct
  FROM public.draft d
  JOIN ls ON ls.db_name=d.db_name AND ls.year=d.year
  JOIN dsize z ON z.db_name=d.db_name
  LEFT JOIN abud ab ON ab.db_name=d.db_name
  LEFT JOIN pos ON pos.NFL_player_id=d.NFL_player_id
  WHERE d.year={int(year)} AND d.pick IS NOT NULL AND d.NFL_player_id IS NOT NULL
    AND z.picks >= 10*NULLIF(ls.num_teams,0)
    AND (COALESCE(pos.position,'')<>'K' OR ls.k_slots>0)
    AND (COALESCE(pos.position,'')<>'DEF' OR ls.def_slots>0)
    AND (list_has_any(pos.broad_positions,['QB','RB','WR','TE','K','DEF'])
         OR (ls.roster='idp' AND list_has_any(pos.broad_positions,['DL','LB','DB'])))
)
SELECT db_name, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
       {int(year)} AS year, NFL_player_id, MAX(position) AS position,
       SUM(CASE WHEN NOT is_auction AND NOT is_keeper THEN pick END) AS sum_adp_pick,
       COUNT(CASE WHEN NOT is_auction AND NOT is_keeper THEN 1 END) AS n_adp_pick,
       1::BIGINT AS n_drafted,
       SUM(CASE WHEN is_auction AND NOT is_keeper THEN cost_pct END) AS sum_auction_cost_pct,
       COUNT(CASE WHEN is_auction AND NOT is_keeper AND cost_pct IS NOT NULL THEN 1 END)
         AS n_auction_cost_pct
FROM picks
GROUP BY db_name, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
         NFL_player_id
"""


def light_transaction_sql(year: int) -> str:
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_TXN_EXTRAS)}),
pos AS (
  SELECT NFL_player_id, position, broad_positions
  FROM public.player_position WHERE year={int(year)}
),
bad AS (
  SELECT DISTINCT d.db_name
  FROM public.draft d JOIN ls ON ls.db_name=d.db_name AND ls.year=d.year
  WHERE d.year={int(year)} AND d.total_fantasy_points>700
),
ms AS (
  SELECT db_name,MAX(spend) max_spend FROM (
    SELECT t.db_name,t.franchise_id,SUM(t.faab_bid) spend
    FROM public.transactions t JOIN ls ON ls.db_name=t.db_name AND ls.year=t.year
    WHERE t.year={int(year)} AND t.faab_bid>0 AND t.franchise_id IS NOT NULL
    GROUP BY 1,2
  ) GROUP BY 1
),
mb AS (
  SELECT t.db_name,MAX(t.faab_bid) max_bid
  FROM public.transactions t JOIN ls ON ls.db_name=t.db_name AND ls.year=t.year
  WHERE t.year={int(year)} AND t.faab_bid>0 GROUP BY 1
),
events AS (
  SELECT t.db_name,ls.teams,ls.roster,ls.ppr,ls.td,ls.league_type,
         ls.lineup_mode,ls.keeper_mode,t.week,t.NFL_player_id,pos.position,
         t.transaction_type IN {_ADD} AS is_add,
         t.transaction_type='drop' AS is_drop,
         CASE WHEN t.faab_bid>0 THEN 100.0*t.faab_bid/NULLIF(
           CASE WHEN ls.waiver_budget>0 AND COALESCE(mb.max_bid,0)<=ls.waiver_budget
                THEN ls.waiver_budget
                ELSE GREATEST(COALESCE(ms.max_spend,0),COALESCE(mb.max_bid,0)) END,0)
         END AS faab_pct,
         CASE WHEN bad.db_name IS NULL THEN t.transaction_score END AS transaction_score
  FROM public.transactions t
  JOIN ls ON ls.db_name=t.db_name AND ls.year=t.year
  LEFT JOIN mb ON mb.db_name=t.db_name
  LEFT JOIN ms ON ms.db_name=t.db_name
  LEFT JOIN bad ON bad.db_name=t.db_name
  LEFT JOIN pos ON pos.NFL_player_id=t.NFL_player_id
  WHERE t.year={int(year)} AND t.week IS NOT NULL AND t.NFL_player_id IS NOT NULL
    AND (t.transaction_type IN {_ADD} OR t.transaction_type='drop')
    AND (COALESCE(pos.position,'')<>'K' OR ls.k_slots>0)
    AND (COALESCE(pos.position,'')<>'DEF' OR ls.def_slots>0)
    AND (list_has_any(pos.broad_positions,['QB','RB','WR','TE','K','DEF'])
         OR (ls.roster='idp' AND list_has_any(pos.broad_positions,['DL','LB','DB'])))
)
SELECT db_name, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
       {int(year)} AS year, week, NFL_player_id, MAX(position) AS position,
       COUNT(CASE WHEN is_add THEN 1 END) AS n_add_events,
       COUNT(CASE WHEN is_drop THEN 1 END) AS n_drop_events,
       MAX(CASE WHEN is_add THEN 1 ELSE 0 END)::BIGINT AS n_add_leagues,
       MAX(CASE WHEN is_drop THEN 1 ELSE 0 END)::BIGINT AS n_drop_leagues,
       SUM(CASE WHEN is_add THEN faab_pct END) AS sum_faab_pct,
       COUNT(CASE WHEN is_add AND faab_pct IS NOT NULL THEN 1 END) AS n_faab_pct,
       SUM(CASE WHEN is_add THEN transaction_score END) AS sum_transaction_score,
       COUNT(CASE WHEN is_add AND transaction_score IS NOT NULL THEN 1 END)
         AS n_transaction_score
FROM events
GROUP BY db_name, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
         week, NFL_player_id
"""
