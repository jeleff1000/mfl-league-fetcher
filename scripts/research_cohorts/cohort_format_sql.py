"""One league-year format contract shared by every research cohort builder."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import combinations

import position_slots_contract as PS


FORMAT_DIMENSIONS = ("league_type", "lineup_mode", "keeper_mode")
MATCHUP_ADAPTIVE_DIMENSIONS = (
    "teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode"
)
MATCHUP_ADAPTIVE_MIN_LEAGUES = 150
COHORT_DIMENSIONS = ("teams", "roster", "ppr", "td", *FORMAT_DIMENSIONS)
# matchup only -- draft/txn keep the 4-dimension key
MATCHUP_COHORT_DIMENSIONS = ("teams", "roster", "ppr", "td", "bracket",
                             *FORMAT_DIMENSIONS)
MATCHUP_FORMAT_DIMENSIONS = ("bracket", *FORMAT_DIMENSIONS)
SEASON_GRAIN = (*COHORT_DIMENSIONS, "cohort_level", "format_level", "year", "NFL_player_id")
CAREER_GRAIN = (*COHORT_DIMENSIONS, "cohort_level", "format_level", "NFL_player_id")
FORMAT_SELECT = ", ".join(f"COALESCE({dim},'ALL') AS {dim}" for dim in FORMAT_DIMENSIONS)


def grouping_sets_with_formats(
    base_dimension_sets: Iterable[Iterable[str]], *, tail: Iterable[str]
) -> str:
    """Emit exact-format and all-format rollups for each existing cohort rung."""
    sets: list[str] = []
    tail_tuple = tuple(tail)
    for base in base_dimension_sets:
        base_tuple = tuple(base)
        sets.append("(" + ", ".join((*base_tuple, *FORMAT_DIMENSIONS, *tail_tuple)) + ")")
        sets.append("(" + ", ".join((*base_tuple, *tail_tuple)) + ")")
    return "GROUPING SETS (\n  " + ",\n  ".join(sets) + "\n)"


def matchup_grouping_sets_with_formats(
    base_dimension_sets: Iterable[Iterable[str]], *, tail: Iterable[str]
) -> str:
    """Emit matchup grouping sets with bracket kept as a first-class dimension.

    The normal helper is intentionally shared by draft/transaction builders, where
    playoff bracket is irrelevant. Matchup cohorts must never silently collapse a
    4-, 6-, and 8-team tournament into the same post-season baseline.
    """
    sets: list[str] = []
    tail_tuple = tuple(tail)
    for base in base_dimension_sets:
        base_tuple = tuple(base)
        sets.append("(" + ", ".join((*base_tuple, *MATCHUP_FORMAT_DIMENSIONS, *tail_tuple)) + ")")
        sets.append("(" + ", ".join((*base_tuple, "bracket", *tail_tuple)) + ")")
    return "GROUPING SETS (\n  " + ",\n  ".join(sets) + "\n)"


_LANE_EXPR = """CASE WHEN COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL,0)+COALESCE(s.roster_LB,0)
              +COALESCE(s.roster_DB,0)+COALESCE(s.roster_DB_LB,0)
              +COALESCE(s.roster_DL_LB,0) > 0 THEN 'idp'
         WHEN COALESCE(s.roster_SUPER_FLEX,0) > 0 THEN 'sflx' ELSE 'flx' END"""
_SCORING_EXPR = """CASE WHEN COALESCE(s.scoring_rec,0) = 0 THEN 'std'
         WHEN COALESCE(s.scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END"""


def cohort_league_settings_sql(
    *, year: int | None = None, extra_select: Iterable[str] = (),
    position_slots: bool = False,
) -> str:
    """Classify every league-year without excluding dynasty, best ball, or keepers.

    Keeper status comes from observed ``draft.is_keeper`` rows.  ``max_keepers`` is not a
    trustworthy classifier because legacy Yahoo captures frequently leave it null/zero.

    ``position_slots`` adds ``teams_QB/RB/WR/TE``.

    TIER CONTRACT v2 (2026-08-02): these are the OBSERVED-CAPACITY tier -- 08tm/10tm/12tm/14tm
    cut from how many roster spots at that position the league ACTUALLY fills, averaged over
    its weeks, NOT from declared roster slots. `league_settings` roster columns understate real
    rosters by 3-5 spots and their bench column swings 20 points between adjacent sizes, so the
    declared-slot tier put 56% of the corpus in one bucket and scattered literal 12-team
    leagues across all four.

    This is an INVISIBLE change -- an un-updated caller would emit the old two-level
    '10t'/'12t' alphabet and be silently wrong -- so `position_slots_contract` is fail-closed
    and every call site must pass the contract token explicitly.

    ``teams`` itself is left untouched, so K/DEF/IDP and any caller that has not opted in keep
    exactly today's behaviour.
    """
    draft_where = f"WHERE d.year = {int(year)}" if year is not None else ""
    settings_where = f"WHERE s.year = {int(year)}" if year is not None else ""
    selects = list(extra_select)
    if position_slots:
        # The historic research band is one mandated all-format cohort.  Its
        # position-slot fields must carry that same ALL key rather than quietly
        # reintroducing a post-2010 roster distinction through teams_QB/RB/WR/TE.
        for pos in PS.TIER_POSITIONS:
          for stat in PS.TIER_STATS:
            # teams_column_name() only knows the four legacy flex positions and returns an
            # empty name for K/DEF/DL/LB/DB, which silently collapsed five columns into one.
            column = f"teams_{pos}" + ("" if stat == "rostered" else "_started")
            # PER-POSITION CUTS. Each position has its own measured ladder -- a QB league
            # runs ~1.8 rostered QBs per team and a WR league ~5.8 receivers, so one set of
            # cuts cannot serve both.
            cap = f"cap_{pos.lower()}.{'spots' if stat == 'rostered' else 'started'}"
            if pos == "QB":
                # QB is LANE-KEYED: superflex is the QB axis (R1). Rostered goes 1.8 -> 3.2
                # per team and every boundary roughly doubles, so a superflex league scored
                # on the single-QB ladder would read 14tm for every team size.
                flx = PS.observed_capacity_tier_sql(cap, cuts=PS.tier_cuts(pos, stat, "flx"))
                sfx = PS.observed_capacity_tier_sql(cap, cuts=PS.tier_cuts(pos, stat, "sflx"))
                expr = (f"CASE WHEN COALESCE(s.roster_SUPER_FLEX,0) > 0 THEN ({sfx}) "
                        f"ELSE ({flx}) END")
            else:
                expr = PS.observed_capacity_tier_sql(cap, cuts=PS.tier_cuts(pos, stat))
            # UNOBSERVED -> FALL BACK TO THE PUBLISHED RULE, never NULL. A league with no
            # player rows has no observed capacity, and emitting NULL would drop it out of
            # every cohort via the rollup's COALESCE(dim,'ALL') -- a coverage loss caused by
            # this change rather than by the data. This mirrors the fallback the declared-slot
            # bucket already used for missing roster columns: fall back to the truth the league
            # is ALREADY published under, mapped into the v2 four-bucket alphabet.
            legacy = ("CASE WHEN s.num_teams <= 11 THEN '10tm' ELSE '12tm' END")
            selects.append(
                f"CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL' "
                f"WHEN {cap} IS NULL THEN ({legacy}) "
                f"ELSE ({expr}) END AS {column}"
            )
    extras = ""
    if selects:
        extras = ",\n    " + ",\n    ".join(selects)
    cap_ctes, cap_joins = "", ""
    if position_slots:
        pf_where = f"AND pf.year = {int(year)}" if year is not None else ""
        for pos in PS.TIER_POSITIONS:
            a = pos.lower()
            # OBSERVED CAPACITY (contract v2): spots the league actually fills at this
            # position, averaged over its weeks. Reads player_fantasy, not roster_* columns.
            cap_ctes += f""",
cap_{a} AS (
  SELECT db_name, year, AVG(nspots) AS spots, AVG(nstarted) AS started FROM (
    SELECT pf.db_name, pf.year, pf.week, COUNT(*) AS nspots,
           SUM(CASE WHEN pf.is_started = 1 THEN 1 ELSE 0 END) AS nstarted
    FROM public.player_fantasy pf
    JOIN (SELECT NFL_player_id, "year", MAX(UPPER(TRIM(position))) AS pos
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE NFL_player_id IS NOT NULL AND position NOT LIKE '%,%'
          GROUP BY 1, 2) np
      ON np.NFL_player_id = pf.NFL_player_id AND np."year" = pf.year
    WHERE np.pos = '{pos}' AND pf.week BETWEEN 1 AND 17 {pf_where}
    GROUP BY 1, 2, 3
  ) GROUP BY 1, 2
)"""
            cap_joins += (f"\nLEFT JOIN cap_{a} ON cap_{a}.db_name = s.db_name"
                          f" AND cap_{a}.year = s.year")
    return f"""
WITH keeper_year AS (
  SELECT d.db_name, d.year,
         BOOL_OR(COALESCE(TRY_CAST(d.is_keeper AS BOOLEAN), false)) AS has_keeper
  FROM public.draft d
  {draft_where}
  GROUP BY 1, 2
){cap_ctes}
SELECT s.db_name, s.year, s.num_teams,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN s.num_teams <= 11 THEN '10t' ELSE '12t' END AS teams,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL,0)+COALESCE(s.roster_LB,0)
              +COALESCE(s.roster_DB,0)+COALESCE(s.roster_DB_LB,0)
              +COALESCE(s.roster_DL_LB,0) > 0 THEN 'idp'
         WHEN COALESCE(s.roster_SUPER_FLEX,0) > 0 THEN 'sflx' ELSE 'flx' END AS roster,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN COALESCE(s.scoring_rec,0) = 0 THEN 'std'
         WHEN COALESCE(s.scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END AS ppr,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN COALESCE(s.scoring_pass_td,4) >= 5 THEN '6pt' ELSE '4pt' END AS td,
    -- playoff bracket, bucketed to the three populated sizes. Playoff and champ rates are
    -- mechanically a function of bracket (4-of-12 vs 6-of-12 are different base rates), so
    -- a cell that mixes brackets averages two denominators. Rare sizes fold to the nearest.
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN s.playoff_teams IS NULL THEN NULL
         WHEN s.playoff_teams <= 5 THEN '4po'
         WHEN s.playoff_teams <= 7 THEN '6po'
         ELSE '8po' END AS bracket,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN COALESCE(s.is_dynasty,false) THEN 'dynasty' ELSE 'redraft' END AS league_type,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN COALESCE(s.sleeper_best_ball,false) THEN 'best_ball' ELSE 'managed' END AS lineup_mode,
    CASE WHEN s.year BETWEEN 2003 AND 2010 THEN 'ALL'
         WHEN COALESCE(k.has_keeper,false) THEN 'keeper' ELSE 'non_keeper' END AS keeper_mode,
    COALESCE(s.scoring_sane, TRUE) AS scoring_sane
    {extras}
FROM public.league_settings s
LEFT JOIN keeper_year k ON k.db_name = s.db_name AND k.year = s.year{cap_joins}
{settings_where}
"""
