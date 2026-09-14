"""build_research_matchup_cohort.py -- matchup/outcome behavior cohort table (local parquet).

Same lattice/gating pattern as draft/txn; source is player_fantasy (heaviest table), read
PER YEAR. Rostered rows, 2015-2025.

Grain: (teams, roster, ppr, td, year, NFL_player_id) with GROUPING-SETS lattice.

ELIGIBILITY GATES (docs/runbooks/research-eligibility-gates-2026-07-16.md) -- every metric is
counted only over league-years where the DDL settings make it possible:

  * Class A (position): a player's weeks only count in leagues where their position has a
    startable slot. roster_K/roster_DEF are 1/2/NULL, and NULL MEANS "no slot", so
    COALESCE(x,0)>0 is a true test. Without it a kicker's ceiling is capped by the share of
    leagues carrying a K slot (76.2% managed vs 43.5% dynasty) -- which masqueraded as a
    -20.3pt "dynasty effect" and collapses to -0.60 once gated.
  * Class B (playoff): is_playoff_week := week >= playoff_start_week, PER LEAGUE-YEAR. Never
    hardcode week 15: it varies (15=74%, 14=19.8%, 16=4.6%), so a fixed week misclassifies 26%
    of leagues -- week 14 is a playoff game in a fifth of them and regular season in the rest.

start_rate (Joe, 2026-07-17): "% started / leagues eligible to start" -- if 100 leagues carry a
K slot and Boswell starts in 10, that's 10% -- AVAILABILITY-SCOPED: a week only enters the
denominator if the player was on an NFL field that week (public.player_active_week: a REG
super-table row, byes/IR/pre-callup excluded) AND the league was still playing that week
(per-league max week, data-derived -- never a hardcoded season length; the previous `x 18`
capped 2019-2020 at 83% and 2021+ at ~90% by construction). Numerator counts started AND
active: starting a bye/IR player is a manager mistake, not player usage. >100% is impossible
by construction; a player with zero active weeks gets NULL, not a fake 0.

History of this column's denominators, each an artifact in turn: /rostered_weeks moved with
roster depth (reported 1/depth; faked an 18.1pt dynasty gap, d=2.45, true gap ~0 at corr
0.988); /(n_leagues x 18) deflated usage 6-17% UNEVENLY by year. start_intensity_legacy_pct
(started/rostered) is retained so before/after stays inspectable.

format: dynasty POOLS with managed (measured corr 0.988 per player, all positions). best_ball
is excluded only from the conditioned-on-started metrics (ppg/lamar/win/clutch WHEN STARTED),
where "started" means "was retroactively optimal" and inflates ppg +66% / lamar +372%; its
usage metrics (start_rate, roster_rate) pool fine at corr 0.923.

NOT a cohort dim: playoff field size. Measured 2026-07-16 -- stratified within league size it
moves nothing (<=0.41x SD, and that on champ_rate whose own corr is 0.21). Playoff share vs
regular-season clutch correlates -0.136 (10t) and +0.076 (12t): opposite signs, both ~0.
league size IS kept: it shifts LAMAR's level ~+1.5 (8.8% of player-weeks cross replacement),
though not its ranking (corr 0.9955).

manager LAMAR (Joe 2026-07-19, ledger D2): avg_lamar_started / total_lamar_started serve the
CANONICAL computation -- value from the super table's slug canon, weight from the observed
weekly start rate; total = per-manager expected total (canon delivered / leagues that started
him). Native league lamar is corrupt in 376 league-years no gate can identify, so it is never
served -- *_native_legacy columns keep it inspectable. Non-flx cells fail closed to NULL
until the calibration contract grows the slug space.

COLUMN DENOMINATORS -- LOCKED (Joe 2026-07-26, docs/runbooks/research-leaderboard-exposure-
policy.md). Usage/outcome rates use the position-eligible league-week exposure (d.n_leagues
for a given week, summed over the player's applicable weeks). Clutch and Champ use the
season's position-eligible leagues with a recorded champion (d.n_leagues_champ). Playoffs
use the position-eligible leagues where playoff evidence exists (d.n_leagues_po); leagues
without identifiable playoff evidence are excluded from that lane's denominator. Never use
rostered-leagues or a fixed season length. Consequences, all live in FINAL_SQL:
  * T6 Win% is wins divided by all started leagues. Expected W-L is the weekly
    start share multiplied by that all-start Win%; unknown/tied results receive no
    W-L credit instead of being removed from the Win% denominator.
  * T7 champ/playoff each split into total (rostered) vs as-starter (in the lineup), four
    columns, all over eligible leagues. Self-flooring: a 1-league stash is 1/thousands ~= 0%.
  * T8 active and inactive weeks ship as SEPARATE columns -- availability sits beside the
    usage rate instead of being folded into it.

Champion & clutch are stored for EVERYONE (mushy middle sits at baseline -- honest raw
material); the lift-over-baseline + shrinkage + value-tier framing is the read-time view.
NOTE champ_rate is intrinsically noisy (corr 0.21-0.45 across strata): one champion per league
per year, mostly team quality and luck. Treat it as a weak column, not a headline.

    py -3 scripts/research_cohorts/build_research_matchup_cohort.py
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from matchup_metric_sql import (
    is_research_week,
    season_expected_outcome_sql,
    season_metric_select,
    weekly_metric_select,
)
try:
    from .cohort_format_sql import (
        FORMAT_DIMENSIONS, FORMAT_SELECT, cohort_league_settings_sql,
        matchup_grouping_sets_with_formats,
    )
    from . import position_slots_contract as PS
except ImportError:
    from cohort_format_sql import (
        FORMAT_DIMENSIONS, FORMAT_SELECT, cohort_league_settings_sql,
        matchup_grouping_sets_with_formats,
    )
    import position_slots_contract as PS
try:
    from .year_config import configured_years, year_predicate
except ImportError:
    from year_config import configured_years, year_predicate

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

OUT_DIR = Path(os.environ.get("RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
OUT = OUT_DIR / "research_matchup_player_season.parquet"
WEEKLY_OUT = OUT_DIR / "research_matchup_weekly.parquet"
MIN_DISPLAY, MIN_STABLE = 10, 35
MIN_POP_ROSTER_RATE_PCT = 2.0
YEARS = configured_years()
YEAR_PREDICATE = year_predicate()

# ---- read cache ----------------------------------------------------------------------
# This builder is the cycle's whale (16.5h on the 13k-league lake). On 2026-07-19 a binder
# error in a LATE query threw away every completed pass. Reads are now cached to parquet,
# keyed by (SQL text, source-data fingerprint): a resume replays finished passes in minutes,
# an EDITED query re-runs automatically because its hash moves, and a re-fold or new slices
# invalidate everything because the fingerprint moves. Set RESEARCH_NO_CACHE=1 to bypass.
CACHE_DIR = OUT_DIR / "_build_cache_matchup"

# Source snapshots are intentionally skinny.  Older snapshots contain only the
# player-level win flag, while refreshed source bundles may also carry loss/tie,
# opponent scores, and matchup outcomes.  Build SQL only against columns that
# actually exist in the attached lake.
_PLAYER_COLUMNS = {"win", "champion", "clutch_equity", "manager_lamar", "team_points"}
_MATCHUP_COLUMNS: set[str] = set()

# ``champion`` is a season/team outcome: it identifies the eventual winner and is
# commonly stamped onto every row for that team's roster.  It is intentionally not
# part of this list.  Champ% as-starter requires a row-level championship-game signal.
_CHAMPIONSHIP_COLUMNS = (
    "is_championship",
    "championship",
    "is_championship_game",
    "championship_game",
)


def championship_signal_sql(player_alias: str = "p", matchup_alias: str | None = "m") -> str:
    """Return an explicit championship-game signal, never the season winner flag."""
    terms = [
        f"CAST({player_alias}.{column} AS INT)"
        for column in _CHAMPIONSHIP_COLUMNS
        if column in _PLAYER_COLUMNS
    ]
    if matchup_alias:
        terms.extend(
            f"CAST({matchup_alias}.{column} AS INT)"
            for column in _CHAMPIONSHIP_COLUMNS
            if column in _MATCHUP_COLUMNS
        )
    return f"COALESCE({', '.join(terms)}, 0)" if terms else "0"


def _denominator_sql_with_explicit_championship_signal(sql: str) -> str:
    """Add row-level title-game evidence to the championship *eligibility* gate.

    ``champion`` remains a valid season-level notation of a championship, but a
    refreshed source may carry only an explicit championship-game marker.  That
    marker should make the league eligible for the Champ/Clutch denominator; it
    must not itself create player credit (the numerator still requires a started,
    active, team-game championship row).

    The base SQL remains fixture-compatible for older snapshots that do not expose
    an explicit championship column.  Main applies this after inspecting the live
    schema, so old cached/test schemas never bind a nonexistent column.
    """
    explicit_terms = [
        f"CAST(p.{column} AS INT)"
        for column in _CHAMPIONSHIP_COLUMNS
        if column in _PLAYER_COLUMNS
    ]
    matchup_terms = [
        f"CAST(m_ch.{column} AS INT)"
        for column in _CHAMPIONSHIP_COLUMNS
        if column in _MATCHUP_COLUMNS
    ]
    if matchup_terms:
        explicit_terms.append(
            "EXISTS (SELECT 1 FROM public.matchup m_ch "
            "WHERE m_ch.db_name = p.db_name AND m_ch.year = p.year "
            f"AND ({' OR '.join(f'{term} = 1' for term in matchup_terms)}))"
        )
    if not explicit_terms:
        return sql
    old = "CAST(p.champion AS INT) = 1"
    new = f"({old} OR {' OR '.join(f'{term} = 1' for term in explicit_terms)})"
    return sql.replace(old, new)


def _data_fingerprint() -> str:
    """Size+mtime of every source the reader unions -- so stale data can never be served."""
    sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))
    import local_reader as lr
    return lr.source_fingerprint()


def cached_rows(fly, con, tag: str, sql: str, fingerprint: str) -> list[dict]:
    """fly.query() with a parquet-backed cache. Returns the same list[dict] as the reader."""
    if os.environ.get("RESEARCH_NO_CACHE") == "1":
        return fly.query(sql, "___leagues")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha1(sql.encode("utf-8")).hexdigest()[:12]
    path = CACHE_DIR / f"{tag}_{fingerprint}_{key}.parquet"
    if path.exists():
        return con.execute(
            f"SELECT * FROM read_parquet('{path.as_posix()}')").fetch_arrow_table().to_pylist()
    rows = fly.query(sql, "___leagues")
    if rows:  # an empty result has no arrow schema to round-trip; just don't cache it
        con.register("_cache_tmp", pa.Table.from_pylist(rows))
        tmp = path.with_suffix(".tmp.parquet")
        con.execute(f"COPY _cache_tmp TO '{tmp.as_posix()}' (FORMAT PARQUET)")
        con.unregister("_cache_tmp")
        os.replace(tmp, path)   # atomic: a killed build never leaves a half-written cache
    return rows


def po_validation_summary(row):
    """Normalize nullable DuckDB aggregate output for validation reporting."""
    year = int(row[0])
    total = int(row[1] or 0)
    agree = int(row[2] or 0)
    pct = 100.0 * agree / total if total else 0.0
    return year, total, agree, pct


def execute_stage(con, label: str, sql: str) -> None:
    """Run a heavy local stage with enough timing detail to diagnose smoke builds."""
    started = time.perf_counter()
    print(f"[matchup-cohort] {label} started", flush=True)
    con.execute(sql)
    print(f"[matchup-cohort] {label} ok ({time.perf_counter() - started:,.1f}s)", flush=True)


def validate_observed_position_index(con) -> None:
    """Fail closed before position-slot cohorts can silently collapse.

    The modern QB/RB/WR/TE team bucket is derived from observed roster capacity
    and therefore needs the immutable NFL position index.  An empty ops cache does
    not produce a SQL error: every capacity tier becomes NULL and the subsequent
    eligibility join simply drops those rows.  That is a much worse failure mode
    than stopping the shard before it emits plausible-looking partial data.
    """
    try:
        total = con.execute("""
          SELECT COUNT(DISTINCT pf.NFL_player_id || ':' || CAST(pf.year AS VARCHAR))
          FROM public.player_fantasy pf
          WHERE pf.year >= 2011 AND pf.NFL_player_id IS NOT NULL
        """).fetchone()[0]
        mapped = con.execute("""
          SELECT COUNT(DISTINCT pf.NFL_player_id || ':' || CAST(pf.year AS VARCHAR))
          FROM public.player_fantasy pf
          JOIN ops.nfl_historical.nfl_player_stats_all np
            ON np.NFL_player_id = pf.NFL_player_id AND np."year" = pf.year
          WHERE pf.year >= 2011
            AND pf.NFL_player_id IS NOT NULL
            AND np.position IS NOT NULL
            AND np.position NOT LIKE '%,%'
        """).fetchone()[0]
    except Exception as exc:
        raise RuntimeError(
            "observed-capacity position index is unavailable; restore "
            "ops_cache.duckdb before building modern matchup cohorts"
        ) from exc
    if total and not mapped:
        raise RuntimeError(
            f"observed-capacity position index mapped 0 of {total:,} modern "
            "player-year records; refusing to emit collapsed position cohorts"
        )
    pct = 100.0 * mapped / total if total else 100.0
    print(f"[position-index] observed capacity coverage {mapped:,}/{total:,} ({pct:.1f}%)",
          flush=True)

_MATCHUP_EXTRAS = (
    "COALESCE(s.playoff_start_week, 15) AS playoff_start_week",
    "s.playoff_teams",
    "COALESCE(s.roster_K, 0) AS k_slots",
    "COALESCE(s.roster_DEF, 0) AS def_slots",
)
_LS = cohort_league_settings_sql(extra_select=_MATCHUP_EXTRAS, position_slots=True)
MATCHUP_FORMAT_SELECT = "COALESCE(bracket,'ALL') AS bracket, " + FORMAT_SELECT
_GS_P = matchup_grouping_sets_with_formats(
    (("teams", "roster", "ppr", "td"), ("teams", "roster", "ppr"),
     ("teams", "roster"), ()), tail=("NFL_player_id", "pos_grp"))
# pos_grp is in EVERY denominator set: it scopes eligibility, it is not a dim that coarsens.
_GS_D = matchup_grouping_sets_with_formats(
    (("teams", "roster", "ppr", "td"), ("teams", "roster", "ppr"),
     ("teams", "roster"), ()), tail=("year", "pos_grp"))


def _ls_for_year(year: int) -> str:
    return cohort_league_settings_sql(year=year, extra_select=_MATCHUP_EXTRAS,
                                      position_slots=True)


def _research_last_week(year: int) -> int:
    """Keep playoff weeks while omitting the final NFL week from research."""
    return 16 if is_research_week(year, 16) and not is_research_week(year, 17) else 17


def player_sql(year: int, *, per_league: bool = False) -> str:
    cohort_select = (
        "teams, roster, ppr, td, bracket, league_type, lineup_mode, keeper_mode"
        if per_league
        else f"COALESCE(teams,'ALL') teams, COALESCE(roster,'ALL') roster, "
             f"COALESCE(ppr,'ALL') ppr, COALESCE(td,'ALL') td, {MATCHUP_FORMAT_SELECT}"
    )
    league_select = "db_name AS db_name, " if per_league else ""
    position_select = "MAX(position) AS position, " if per_league else ""
    group_by = (
        "teams, roster, ppr, td, bracket, league_type, lineup_mode, keeper_mode, "
        "db_name, NFL_player_id"
        if per_league
        else _GS_P
    )
    last_week = _research_last_week(year)
    p = lambda col: f"p.{col}" if col in _PLAYER_COLUMNS else "NULL"
    m = lambda col: f"m.{col}" if col in _MATCHUP_COLUMNS else "NULL"
    p_team_key = "p.team_key" if "team_key" in _PLAYER_COLUMNS else "NULL"
    m_team_key = "m.team_key" if "team_key" in _MATCHUP_COLUMNS else "NULL"
    mfd = ""
    mjoin = ""
    mfd_columns = {"win", "loss", "tie", "team_points", "opponent_points"}
    mfd_columns.update(_CHAMPIONSHIP_COLUMNS)
    if _MATCHUP_COLUMNS.intersection(mfd_columns):
        m_team_key_select = "team_key," if "team_key" in _MATCHUP_COLUMNS else ""
        m_team_key_partition = ", team_key" if "team_key" in _MATCHUP_COLUMNS else ""
        mfd = """
    mfd AS (
      SELECT db_name, year, week, {m_team_key} manager, franchise_id,
             LOWER(TRIM(CAST(manager AS VARCHAR))) AS manager_key,
             {mcols}
      FROM public.matchup
      WHERE year = {year} AND week <= {last_week}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY db_name, year, week,
                     LOWER(TRIM(CAST(manager AS VARCHAR))), franchise_id {m_team_partition}
        ORDER BY CASE WHEN {has_outcome} THEN 0 ELSE 1 END,
                 CASE WHEN {has_scores} THEN 0 ELSE 1 END
      ) = 1
    ),""".format(
            mcols=", ".join(f"{c}" for c in ("win", "loss", "tie", "team_points", "opponent_points", *_CHAMPIONSHIP_COLUMNS) if c in _MATCHUP_COLUMNS),
            m_team_key=m_team_key_select,
            m_team_partition=m_team_key_partition,
            has_outcome=" OR ".join(f"{c} IS NOT NULL" for c in ("win", "loss", "tie") if c in _MATCHUP_COLUMNS) or "FALSE",
            has_scores=" AND ".join(f"{c} IS NOT NULL" for c in ("team_points", "opponent_points") if c in _MATCHUP_COLUMNS) or "FALSE",
            year=year, last_week=last_week,
        )
        # Refreshed source folds can carry a platform-native team_key on the
        # player row while the matchup fold carries a normalized/legacy key.
        # The manager is still the correct fallback identity in that case.  A
        # prior version allowed that fallback only when the player key was NULL,
        # silently dropping championship and outcome signals after a source
        # refold.  The row-number ordering below gives an exact key match
        # precedence when both are available.
        m_identity = (
            "((p.team_key IS NOT NULL AND m.team_key IS NOT NULL "
            "AND m.team_key = p.team_key) "
            "OR (m.manager_key = LOWER(TRIM(CAST(p.manager AS VARCHAR)))))"
            if "team_key" in _PLAYER_COLUMNS and "team_key" in _MATCHUP_COLUMNS
            else "m.manager_key = LOWER(TRIM(CAST(p.manager AS VARCHAR)))"
        )
        mjoin = """
      LEFT JOIN mfd m ON m.db_name = p.db_name AND m.year = p.year AND m.week = p.week
        AND {pmanager} IS NOT NULL
        -- Prefer the stable team key. Manager is only the fallback for old folds where
        -- team_key is absent on the player row.
        AND {identity}
""".format(pmanager="p.manager" if "manager" in _PLAYER_COLUMNS else "NULL",
           identity=m_identity,
           pfranchise="p.franchise_id" if "franchise_id" in _PLAYER_COLUMNS else "NULL")
    pwin = p("win")
    ploss = p("loss")
    ptie = p("tie")
    pteam = p("team_points")
    popp = p("opponent_points")
    mwin, mloss, mtie = m("win"), m("loss"), m("tie")
    mteam, mopp = m("team_points"), m("opponent_points")
    mfranchise = m("franchise_id")
    pmanager = p("manager")
    pfranchise = p("franchise_id")
    score_win = f"CASE WHEN {pteam} IS NOT NULL AND {popp} IS NOT NULL THEN CAST({pteam} > {popp} AS INT) END"
    score_loss = f"CASE WHEN {pteam} IS NOT NULL AND {popp} IS NOT NULL THEN CAST({pteam} < {popp} AS INT) END"
    score_tie = f"CASE WHEN {pteam} IS NOT NULL AND {popp} IS NOT NULL THEN CAST({pteam} = {popp} AS INT) END"
    mscore_win = f"CASE WHEN {mteam} IS NOT NULL AND {mopp} IS NOT NULL THEN CAST({mteam} > {mopp} AS INT) END"
    mscore_loss = f"CASE WHEN {mteam} IS NOT NULL AND {mopp} IS NOT NULL THEN CAST({mteam} < {mopp} AS IN…18780 tokens truncated…dw.bracket=p.bracket AND dw.league_type=p.league_type
             AND dw.lineup_mode=p.lineup_mode AND dw.keeper_mode=p.keeper_mode
             AND dw.year=p.year AND dw.pos_grp=p.pos_grp AND dw.week=tg.week
      LEFT JOIN wp ON wp.teams=p.teams AND wp.roster=p.roster AND wp.ppr=p.ppr AND wp.td=p.td AND wp.bracket=p.bracket
             AND wp.league_type=p.league_type AND wp.lineup_mode=p.lineup_mode
             AND wp.keeper_mode=p.keeper_mode AND wp.year=p.year AND wp.week=tg.week
             AND wp.NFL_player_id=p.NFL_player_id AND wp.pos_grp=p.pos_grp
      LEFT JOIN act a ON a.NFL_player_id=p.NFL_player_id AND a.year=p.year AND a.week=tg.week
      JOIN d ON d.teams=p.teams AND d.roster=p.roster AND d.ppr=p.ppr AND d.td=p.td AND d.bracket=p.bracket
            AND d.league_type=p.league_type AND d.lineup_mode=p.lineup_mode
            AND d.keeper_mode=p.keeper_mode AND d.year=p.year AND d.pos_grp=p.pos_grp
      LEFT JOIN psv ON psv.NFL_player_id=p.NFL_player_id AND psv.year=p.year
             AND psv.week=tg.week AND p.roster='flx'
             AND psv.slug=p.teams || '_flx_' || p.ppr || '_' || p.td
"""

WEEKLY_FINAL_TABLE_SQL = f"""
      CREATE TABLE weekly_final AS
      WITH d_lookup AS MATERIALIZED (
        -- The denominator lattice carries many diagnostic columns that are not needed
        -- here.  Joining the wide relation directly made DuckDB build a spill-sized
        -- hash payload on old seasons even though the lookup key is unique.  Collapse
        -- it to the one served value before the weekly join.
        SELECT teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,
               year,pos_grp,MAX(n_leagues) AS n_leagues
        FROM d
        GROUP BY 1,2,3,4,5,6,7,8,9,10
      ), season_population AS MATERIALIZED (
        -- p is also the season lattice and is intentionally retained for the season
        -- output.  The weekly gate only needs the player/key and roster penetration;
        -- make that relation narrow and unique before joining it.
        SELECT teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,
               year,NFL_player_id,pos_grp,MAX(n_rostered_leagues) AS n_rostered_leagues
        FROM p
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
      )
      SELECT b.teams, b.roster, b.ppr, b.td, b.bracket,
             b.league_type, b.lineup_mode, b.keeper_mode,
             (CASE WHEN b.teams<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN b.roster<>'ALL' THEN 1 ELSE 0 END)
             +(CASE WHEN b.ppr<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN b.td<>'ALL' THEN 1 ELSE 0 END)
               AS cohort_level,
             CASE WHEN b.league_type='ALL' THEN 0 ELSE 3 END AS format_level,
             b.year, b.week, b.NFL_player_id, b.pos_grp,
             {weekly_metric_select('b')},
             100.0 * b.started_leagues / NULLIF(b.team_game_eligible_leagues,0)
                  * b.wins_started / NULLIF(b.started_leagues,0) AS won_pct,
             100.0 * b.started_leagues / NULLIF(b.team_game_eligible_leagues,0)
                  * (1.0 - b.wins_started / NULLIF(b.started_leagues,0)) AS lost_pct,
             b.points_started / NULLIF(b.started_leagues,0) AS ppg_when_started,
             b.lamar_canon AS avg_lamar_started,
             b.lamar_weighted AS total_lamar_started,
             b.clutch_weighted AS avg_clutch_started,
             b.lamar_weighted, b.clutch_weighted,
             b.rostered_leagues, b.started_leagues, b.healthy_started_leagues,
             b.roster_eligible_leagues, b.team_game_eligible_leagues, b.healthy_eligible_leagues,
             b.clutch_eligible_leagues,
             b.wins_started, b.losses_started, b.points_started, b.points_started_leagues, b.clutch_sum,
             b.active_wins_started,
             b.lamar_weighted, b.clutch_weighted,
             b.champ_started, b.champ_eligible,
             d.n_leagues,
             CASE WHEN d.n_leagues >= {MIN_STABLE} THEN 'confident'
                  WHEN d.n_leagues >= {MIN_DISPLAY} THEN 'mushy'
                  ELSE 'insufficient' END AS confidence
      FROM weekly_base b
      JOIN d_lookup d USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,pos_grp)
      -- Season population gate: rostered in at least 2% of leagues where this
      -- position is eligible. d.n_leagues is position-scoped, not total leagues.
      JOIN season_population
        ON season_population.teams=b.teams AND season_population.roster=b.roster
       AND season_population.ppr=b.ppr AND season_population.td=b.td AND season_population.bracket=b.bracket
       AND season_population.league_type=b.league_type
       AND season_population.lineup_mode=b.lineup_mode
       AND season_population.keeper_mode=b.keeper_mode
       AND season_population.year=b.year
       AND season_population.NFL_player_id=b.NFL_player_id
       AND season_population.pos_grp=b.pos_grp
       AND season_population.n_rostered_leagues >= {MIN_POP_ROSTER_RATE_PCT}/100.0*d.n_leagues
"""

WEEKLY_SEASON_SQL = f"""
      CREATE TABLE weekly_season AS
      SELECT w.teams,w.roster,w.ppr,w.td,w.bracket,w.league_type,w.lineup_mode,w.keeper_mode,
             w.year,w.NFL_player_id,w.pos_grp,
             {season_metric_select('w')},
             100.0 * ({season_expected_outcome_sql('w', 'wins_started')})
                 / NULLIF(COUNT(*),0) AS won_pct,
             100.0 * ({season_expected_outcome_sql('w', 'losses_started')})
                 / NULLIF(COUNT(*),0) AS lost_pct,
             {season_expected_outcome_sql('w', 'wins_started')} AS expected_wins,
             {season_expected_outcome_sql('w', 'losses_started')} AS expected_losses,
             SUM(1.0*w.started_leagues/NULLIF(w.team_game_eligible_leagues,0)) AS expected_starts,
             SUM(w.points_started) / NULLIF(SUM(w.points_started_leagues),0) AS ppg_when_started,
             SUM(w.points_started) AS total_points_observed,
             100.0 * SUM(w.champ_started) / NULLIF(SUM(w.champ_eligible),0)
                 AS champ_week_rate_pct,
             SUM(w.champ_started) AS started_champ_active,
             SUM(w.champ_eligible) AS champ_elig_leagues,
             SUM(w.started_leagues) AS started_weeks,
             SUM(w.team_game_eligible_leagues) AS elig_league_weeks,
             COUNT(*) AS active_weeks,
             SUM(100.0 * w.started_leagues / NULLIF(w.team_game_eligible_leagues,0))
                 AS weekly_start_rate_sum,
             SUM(100.0 * w.healthy_started_leagues / NULLIF(w.healthy_eligible_leagues,0))
                 AS weekly_healthy_rate_sum
      FROM weekly_base w
      WHERE w.team_game_eligible_leagues > 0
      GROUP BY ALL
"""

# (label, sql) in execution order. The builder iterates this; nothing splits text.
WEEKLY_STATEMENTS = (
    ("weekly base", WEEKLY_BASE_SQL),
    ("weekly final", WEEKLY_FINAL_TABLE_SQL),
    ("weekly season", WEEKLY_SEASON_SQL),
)
# Kept for the bind tests, which hand the whole blob to DuckDB in one execute().
WEEKLY_FINAL_SQL = ";\n".join(sql.strip() for _, sql in WEEKLY_STATEMENTS) + ";"

FINAL_SQL = f"""
      CREATE TABLE final AS
      SELECT p.teams, p.roster, p.ppr, p.td, p.bracket,
        p.league_type, p.lineup_mode, p.keeper_mode,
        (CASE WHEN p.teams<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN p.roster<>'ALL' THEN 1 ELSE 0 END)
        +(CASE WHEN p.ppr<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN p.td<>'ALL' THEN 1 ELSE 0 END) AS cohort_level,
        CASE WHEN p.league_type='ALL' THEN 0 ELSE 3 END AS format_level,
        p.year, p.NFL_player_id, p.pos_grp,
        -- Roster rate is league membership, not exposure-week volume: a player
        -- rostered in one eligible league for 16 weeks is still rostered in one
        -- league. The numerator and denominator are distinct league-years
        -- scoped to the player's position group.
        ROUND(100.0*p.n_rostered_leagues/NULLIF(d.n_leagues,0),1) AS roster_rate_pct,
        -- start_rate = started-while-active / (eligible leagues still playing, summed over the
        -- player's ACTIVE weeks). NULL when the player never took an NFL field that year --
        -- honest, not a fake 0. <=100 by construction.
        ROUND(100.0*p.started_team_game_weeks/NULLIF(pd.team_game_eligible_league_weeks,0),1) AS start_rate_pct,
        -- healthy_start_rate: started / position-eligible leagues over the player's
        -- active/snap weeks only.  The active-week join excludes team byes, while the
        -- position-eligible denominator remains independent of whether that league
        -- rostered him.
        -- NULL rather than 0 when the weekly lane is absent AND he was never rostered.
        ROUND(100.0*p.started_active_weeks/NULLIF(pd.healthy_eligible_league_weeks,0),1)
              AS healthy_start_rate_pct,
        p.rostered_weeks AS rostered_league_weeks,
        p.started_active_weeks AS started_active_weeks,
        pd.nfl_active_weeks AS nfl_active_weeks,
        pd.team_game_eligible_league_weeks AS elig_league_weeks,
        -- Start-weighted record, bottom-up: season won/lost are sums of weekly
        -- win-rate x start-share contributions. Unknown outcomes stay unknown.
        ROUND(CASE WHEN ws.start_rate_pct IS NOT NULL THEN ws.won_pct
                   ELSE 100.0*p.wins_started/NULLIF(pd.team_game_eligible_league_weeks,0) END,1)
              AS won_pct,
        ROUND(CASE WHEN ws.start_rate_pct IS NOT NULL THEN ws.lost_pct
                   ELSE 100.0*p.losses_started/NULLIF(pd.team_game_eligible_league_weeks,0) END,1)
              AS lost_pct,
        ROUND(COALESCE(ws.win_rate_pct,
              100.0*p.wins_started/NULLIF(p.started_team_game_weeks,0)),1)
              AS win_rate_pct,
        ROUND(ws.expected_wins,4) AS expected_wins,
        ROUND(ws.expected_losses,4) AS expected_losses,
        ROUND(ws.expected_starts,4) AS expected_starts,
        -- kept for diagnostics: the OLD definition, so before/after is inspectable rather
        -- than a claim. Not for display.
        ROUND(100.0*p.started_weeks/NULLIF(p.rostered_weeks,0),1) AS start_intensity_legacy_pct,
        ROUND(COALESCE(ws.ppg_when_started,
              p.sum_pts_started/NULLIF(p.points_started_leagues,0)),2) AS ppg_when_started,
        ROUND(ws.total_points_observed,2) AS total_points_observed,
        -- DIAGNOSTIC, both sides ACTIVE-scoped (fixed 2026-07-26). The old form divided
        -- SUM(win) over ALL started league-weeks by started_weeks, which the weekly lane
        -- resolves ACTIVE-only -- so a player active one week but started all season read
        -- 1,700% (measured: 1,851 rows, all years, max 1700). Numerator and denominator now
        -- share the started-AND-active basis, so it is <=100 by construction.
        ROUND(100.0*p.wins_started/NULLIF(p.wins_started + p.losses_started,0),1)
              AS win_rate_started_pct,
        -- champ_week_rate (Joe 2026-07-17): of the eligible leagues whose championship week
        -- the player PLAYED in, share where he was in the champion's STARTING lineup. The old
        -- sum_champ/started_weeks counted benched champ-roster rows (56% of champ rows) over
        -- an unrelated denominator -> impossible values (Aiyuk 400% at 0.0 start rate).
        -- <=100 by construction; NULL when he played no title weeks. sum_champ retained raw.
        ROUND(100.0*p.started_champ_active/NULLIF(d.n_leagues_champ,0),1)
              AS champ_week_rate_pct,
        p.started_champ_active, d.n_leagues_champ AS champ_elig_leagues,
        po.n_po_resolved_lg AS playoff_eligible_leagues, po.n_po_resolved_lg AS playoff_signal_leagues, p.sum_champ,
        -- CLUTCH: total championship impact per ELIGIBLE LEAGUE (Joe 2026-07-20).
        --
        -- clutch_equity is a change in title odds, so it is inherently ADDITIVE across weeks
        -- and is NOT a per-week rate. Dividing by league-WEEKS would charge a player for how
        -- long he was around -- "clutch doesn't care how long you've been on a team, it cares
        -- how much you impacted your team's champ% that week." A champ-week hero must keep
        -- the full size of that week; only the LEAGUE dimension gets normalised, so being
        -- started in 10 of 226 leagues still drowns out against being started in all 226.
        --
        -- The design's own caveat needs no code: a monster week for an eliminated team moves
        -- no title odds, so clutch_equity is ~0 by construction (Kyle Pitts' week 15). The
        -- metric already declines to reward it.
        ROUND(COALESCE(ws.avg_clutch_started,
              p.sum_clutch_started_active/NULLIF(d.n_leagues_champ,0)),4) AS avg_clutch_started,
        -- the old conditioned-on-started form, kept inspectable, NOT for display
        ROUND(p.sum_clutch_started/NULLIF(p.started_weeks,0),3) AS avg_clutch_when_started_legacy,
        -- CANONICAL manager LAMAR (Joe 2026-07-19, ledger D2): value from the slug canon,
        -- weight = observed weekly start rate. Native league lamar is never served (376
        -- corrupt league-years escape every gate); it stays as *_native_legacy diagnostics.
        ROUND(c.num_sr/NULLIF(c.den_sr,0),3) AS avg_lamar_started,
        -- per-manager expected total (Joe 2026-07-19): total canon LAMAR delivered across
        -- starting managers / distinct leagues that started him.
        ROUND(COALESCE(ws.total_lamar_started,
              c.num_abs/NULLIF(ns.n_lg_started,0)),2) AS total_lamar_started,
        ROUND(p.sum_lamar_started/NULLIF(p.started_weeks,0),3) AS avg_lamar_started_native_legacy,
        ROUND(p.sum_lamar_started,2) AS total_lamar_started_native_legacy,
        -- record (W-L) over started weeks
        p.wins_started, p.losses_started,
        ROUND(100.0*p.wins_started/NULLIF(p.wins_started + p.losses_started,0),1) AS record_win_pct,
        -- playoff vs regular season, split on each league's OWN playoff_start_week
        p.po_started_weeks, p.po_wins_started,
        ROUND(100.0*p.po_wins_started/NULLIF(p.po_started_weeks,0),1) AS playoff_win_pct,
        ROUND(100.0*p.reg_wins_started/NULLIF(p.reg_started_weeks,0),1) AS regular_win_pct,
        ROUND(100.0*p.po_started_active_weeks/NULLIF(p.started_active_weeks,0),1)
              AS pct_starts_in_playoffs,
        -- clutch rate: title-leverage per playoff week started
        ROUND(p.sum_clutch_po/NULLIF(p.po_clutch_started_weeks,0),3) AS avg_clutch_playoff,
        -- playoff rate, ROSTERED basis, three shapes (Joe 2026-07-19 Â§2.4) -- ladder_stab's
        -- po_wkwt/po_final/po_started classes decide which ships. Denominator = leagues that
        -- rostered him with manager attribution (po.n_rost_lg).
        ROUND(100.0*po.wkwt_credit/NULLIF(po.n_po_resolved_lg,0),1) AS playoff_rate_wkwt,
        ROUND(100.0*po.n_final_po/NULLIF(po.n_po_resolved_lg,0),1) AS playoff_rate_final,
        ROUND(100.0*po.n_started_po/NULLIF(po.n_po_resolved_lg,0),1) AS playoff_rate_started,
        po.n_rost_lg AS po_n_rostered_leagues, po.n_started_po,
        -- ===== SERVED playoff/champ rates (T7, LOCKED denominators, Joe 2026-07-26) =====
        -- Playoff rates use d.n_leagues_po: position-eligible leagues where playoff
        -- evidence exists. Roster/start rates continue to use d.n_leagues.
        --
        -- The eligible denominator is what makes these self-flooring: a 1-league stash is
        -- 1/thousands ~= 0%, never 1/1 = 100%. They are compound rates by design --
        -- total <= roster%, as-starter <= start% -- reading "how much did you actually
        -- contribute to deep runs", not "how did the teams that had you do".
        --
        -- total vs as-starter is the roster/lineup split:
        --   playoff_total      rostered on a team that made the playoffs, measured at the
        --                      last regular-season week (the qualification moment)
        --   playoff_as_starter STARTED in one of that team's playoff games
        --   champ_total        rostered on the champion (title-week roster)
        --   champ_as_starter   in the champion's STARTING lineup in the title game
        -- The as-starter lanes never credit a benched player, which is the whole point:
        -- 56% of champion-flagged rows are bench rows.
        -- Missing playoff evidence is excluded from the playoff denominator.
        ROUND(100.0*COALESCE(po.n_final_po,0)/NULLIF(po.n_po_resolved_lg,0),1) AS playoff_total_pct,
        ROUND(100.0*COALESCE(po.n_started_po,0)/NULLIF(po.n_po_resolved_lg,0),1) AS playoff_as_starter_pct,
        -- Diagnostic roster-on-champion rate uses all position-eligible leagues. The
        -- canonical Champ metric is champ_as_starter_pct immediately below.
        ROUND(100.0*COALESCE(p.n_champ_leagues,0)/NULLIF(d.n_leagues_champ,0),1) AS champ_total_pct,
        ROUND(100.0*p.n_champ_start_leagues/NULLIF(d.n_leagues_champ,0),1) AS champ_as_starter_pct,
        p.n_champ_leagues, p.n_champ_start_leagues,
        -- T8: availability follows player NFL-team game weeks. A bye supplies no team-game
        -- row, while injury/scratch weeks remain inactive whether or not he was rostered.
        bit_count(COALESCE(pd.inactive_week_mask, 0)) AS inactive_weeks,
        pd.inactive_week_mask,
        pd.active_week_mask,
        -- additive shard components: local assembly sums these, then recalculates every
        -- displayed rate/mean. Never merge already-rounded percentages.
        p.sum_pts_started, p.sum_clutch_started_active,
        p.wins_started_active, p.sum_clutch_started,
        p.sum_lamar_started, p.reg_started_weeks, p.reg_wins_started,
        p.sum_clutch_po, p.po_started_active_weeks,
        c.num_sr AS canon_num_sr, c.den_sr AS canon_den_sr,
        c.num_abs AS canon_num_abs, ns.n_lg_started,
        po.wkwt_credit AS po_wkwt_credit, po.n_final_po,
        po.n_po_resolved_lg AS po_n_resolved_leagues,
        po.n_started_po AS po_n_started_leagues,
        ws.weekly_start_rate_sum, ws.weekly_healthy_rate_sum, ws.active_weeks,
        p.n_rostered_leagues, p.rostered_weeks, p.rostered_active_weeks, p.started_team_game_weeks,
        pd.roster_eligible_league_weeks, pd.team_game_eligible_league_weeks,
        pd.healthy_eligible_league_weeks,
        COALESCE(ws.started_weeks,p.started_weeks) AS started_weeks, d.n_leagues,
        CASE WHEN d.n_leagues >= {MIN_STABLE} THEN 'confident'
             WHEN d.n_leagues >= {MIN_DISPLAY} THEN 'mushy' ELSE 'insufficient' END AS confidence
      -- pos_grp in the join: each player divides by the leagues that could START them, not by
      -- every league in the cohort. pdenom is LEFT-joined: a player with zero NFL-active
      -- weeks keeps his row (roster_rate etc. are real) with start_rate NULL.
      FROM p JOIN d USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,pos_grp)
      LEFT JOIN pdenom pd USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,NFL_player_id,pos_grp)
      LEFT JOIN canon c USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,NFL_player_id,pos_grp)
      LEFT JOIN nstl ns USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,NFL_player_id,pos_grp)
      LEFT JOIN pol po USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,NFL_player_id,pos_grp)
      LEFT JOIN weekly_season ws USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,year,NFL_player_id,pos_grp)
      -- Exclude season populations below 2% roster penetration using the
      -- position-eligible denominator carried by d.n_leagues.
      WHERE p.n_rostered_leagues >= {MIN_POP_ROSTER_RATE_PCT}/100.0*d.n_leagues
"""


if __name__ == "__main__":
    main()
