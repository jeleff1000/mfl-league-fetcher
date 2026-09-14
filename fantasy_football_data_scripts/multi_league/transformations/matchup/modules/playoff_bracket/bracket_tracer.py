"""
Bracket Tracer — Data-first playoff bracket tracer against DDL.

All data reads (seeding, matchup pairs, winner determination) are SQL
against the matchup and league_settings tables. Python only handles
the iterative alive-set tracking.

Works against:
  - Local DuckDB (conn to a .duckdb file)
  - MotherDuck (conn with ATTACH)
  - Any DuckDB connection with matchup + league_settings tables

Handles:
- Any bracket size (2-64 teams, including odd)
- Byes, reseeding (informational — pairings always from data)
- Multi-week rounds (combined score)
- H2H + Median seeding
- Championship bracket classification, champion

Phase 2 refactor:
- Championship-only write-back (no consolation/sacko/postseason writes)
- Backward-walk from championship to classify bracket edges
- 4-priority championship matchup identification
- Formula-based championship week with data validation
"""

from __future__ import annotations

import logging

from multi_league.transformations.matchup.modules.playoff_config import round_weeks as _round_weeks
from multi_league.transformations.matchup.modules.playoff_helpers import (
    POINTS_FIRST_SEEDING,
    normalize_playoff_seeding_rule,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL Templates
# ---------------------------------------------------------------------------

_SEED_SQL = """
WITH reg AS (
    SELECT * FROM {table}
    WHERE year = {year} AND week < {playoff_start_week}
      AND team_points IS NOT NULL AND opponent_points IS NOT NULL
      AND {db_filter}
),
h2h AS (
    SELECT
        {id_col} AS tid,
        SUM(CASE WHEN CAST(team_points AS DOUBLE) > CAST(opponent_points AS DOUBLE) THEN 1 ELSE 0 END) AS h2h_wins,
        SUM(CAST(team_points AS DOUBLE)) AS total_points
    FROM reg
    WHERE {id_col} IS NOT NULL
    GROUP BY {id_col}
),
weekly_scores AS (
    SELECT DISTINCT {id_col} AS tid, week, CAST(team_points AS DOUBLE) AS pts
    FROM reg WHERE {id_col} IS NOT NULL
),
weekly_median AS (
    SELECT week, MEDIAN(pts) AS league_median
    FROM weekly_scores GROUP BY week
),
median_agg AS (
    SELECT
        ws.tid,
        SUM(CASE WHEN ws.pts > wm.league_median THEN 1 ELSE 0 END) AS med_wins
    FROM weekly_scores ws
    JOIN weekly_median wm ON ws.week = wm.week
    GROUP BY ws.tid
)
SELECT
    h.tid,
    h.h2h_wins,
    COALESCE(m.med_wins, 0) AS median_wins,
    h.h2h_wins + CASE WHEN {uses_median} THEN COALESCE(m.med_wins, 0) ELSE 0 END AS total_wins,
    h.total_points,
    ROW_NUMBER() OVER (
        ORDER BY {seed_order}
    ) AS seed
FROM h2h h
LEFT JOIN median_agg m ON h.tid = m.tid
ORDER BY seed
"""

# Pair structure is determined by id_col + opp_col, not by score presence.
# Externally-imported historical data (e.g. KMFFL 2013 playoffs uploaded
# without scores) has real matchup pairs but NULL points; gating on
# "team_points > 0 OR opponent_points > 0" used to drop those pairs and the
# tracer would tag nothing. Scheduled-but-unplayed weeks have no rows in
# the matchup table at all, so this isn't a regression for in-progress
# seasons.
_PAIRS_SQL = """
WITH raw_pairs AS (
    SELECT {id_col} AS a, {opp_col} AS b
    FROM {table}
    WHERE year = {year}
      AND week >= {wk_start} AND week <= {wk_end}
      AND {id_col} IS NOT NULL AND {opp_col} IS NOT NULL
      AND {db_filter}
)
SELECT DISTINCT
    CASE WHEN r1.a < r1.b THEN r1.a ELSE r1.b END AS team_a,
    CASE WHEN r1.a < r1.b THEN r1.b ELSE r1.a END AS team_b
FROM raw_pairs r1
JOIN raw_pairs r2 ON r1.a = r2.b AND r1.b = r2.a
"""

# Score-based fallback for finding pairs when opponent_franchise_id is NULL.
# Matches rows where team A's points = team B's opponent_points and vice versa.
_PAIRS_SCORE_SQL = """
SELECT DISTINCT
    CASE WHEN m.{id_col} < opp.{id_col} THEN m.{id_col} ELSE opp.{id_col} END AS team_a,
    CASE WHEN m.{id_col} < opp.{id_col} THEN opp.{id_col} ELSE m.{id_col} END AS team_b
FROM {table} m
JOIN {table} opp
    ON m.year = opp.year AND m.week = opp.week
    AND m.{id_col} != opp.{id_col}
    AND ABS(CAST(m.team_points AS DOUBLE) - CAST(opp.opponent_points AS DOUBLE)) < 0.01
    AND ABS(CAST(m.opponent_points AS DOUBLE) - CAST(opp.team_points AS DOUBLE)) < 0.01
WHERE m.year = {year}
  AND m.week >= {wk_start} AND m.week <= {wk_end}
  AND m.{id_col} IS NOT NULL AND opp.{id_col} IS NOT NULL
  AND (CAST(m.team_points AS DOUBLE) > 0 OR CAST(m.opponent_points AS DOUBLE) > 0)
  AND {db_filter_m} AND {db_filter_opp}
"""

_WINNER_SQL = """
SELECT
    {id_col} AS tid,
    SUM(CAST(team_points AS DOUBLE)) AS total_pts,
    SUM(CASE WHEN CAST(win AS INTEGER) = 1 THEN 1 ELSE 0 END) AS win_count
FROM {table}
WHERE year = {year}
  AND week >= {wk_start} AND week <= {wk_end}
  AND {db_filter}
  AND (
    ({id_col} = '{team_a}' AND {opp_col} = '{team_b}')
    OR ({id_col} = '{team_b}' AND {opp_col} = '{team_a}')
  )
GROUP BY {id_col}
ORDER BY total_pts DESC NULLS LAST, win_count DESC
"""

# Score-based fallback for winner determination when opp_col is NULL for some rows.
_WINNER_SCORE_SQL = """
SELECT
    m.{id_col} AS tid,
    SUM(CAST(m.team_points AS DOUBLE)) AS total_pts
FROM {table} m
JOIN {table} opp
    ON m.year = opp.year AND m.week = opp.week
    AND m.{id_col} != opp.{id_col}
    AND ABS(CAST(m.team_points AS DOUBLE) - CAST(opp.opponent_points AS DOUBLE)) < 0.01
    AND ABS(CAST(m.opponent_points AS DOUBLE) - CAST(opp.team_points AS DOUBLE)) < 0.01
WHERE m.year = {year}
  AND m.week >= {wk_start} AND m.week <= {wk_end}
  AND m.{id_col} IN ('{team_a}', '{team_b}')
  AND opp.{id_col} IN ('{team_a}', '{team_b}')
  AND {db_filter_m} AND {db_filter_opp}
GROUP BY m.{id_col}
ORDER BY total_pts DESC
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def trace_championship_bracket_sql(
    conn,
    year: int,
    settings: dict,
    table: str = "matchup",
    id_col: str = "franchise_id",
    write_back: bool = True,
    db_filter: str = "1=1",
) -> dict:
    """Trace championship bracket via SQL. Reads from and writes to DDL.

    Orchestrator that uses the new Phase 2 helpers:
    1. Compute championship week (formula + validation)
    2. Identify championship matchup (4-priority rule)
    3. Backward-walk to classify all championship bracket edges
    4. Write back championship-only columns

    Args:
        conn: DuckDB connection (local or MotherDuck).
        year: Season year.
        settings: Dict with playoff_teams, bye_teams, playoff_start_week,
            end_week, num_teams, uses_playoff_reseeding, plus optional
            playoff_round_type, has_multiweek_championship, uses_median_score.
        table: Qualified table name (e.g. 'the_league.public.matchup').
        id_col: Identity column ('franchise_id' or 'manager').
        write_back: If True, UPDATE the matchup table with results.

    Returns:
        Dict with champion, runner_up, classifications, rounds,
        elimination_round, champ_bracket_teams, etc.
    """
    settings = _normalise_settings(settings)

    playoff_teams = int(settings["playoff_teams"])
    bye_count = int(settings["bye_teams"])
    playoff_start = int(settings["playoff_start_week"])
    end_week = int(settings["end_week"])

    prt = int(settings.get("playoff_round_type") or 0)
    if prt == 0 and settings.get("has_multiweek_championship"):
        prt = 2

    if playoff_teams < 2:
        return _empty_result()

    # Resolve opponent column
    opp_col = _pick_opp_col(conn, table, id_col)

    # -- Seeds via SQL -------------------------------------------------------
    uses_median = bool(settings.get("uses_median_score") or settings.get("uses_median"))
    seeds = _sql_seeds(
        conn,
        table,
        year,
        playoff_start,
        id_col,
        uses_median,
        settings.get("playoff_seeding_rule"),
        settings.get("playoff_seeding_rule_by"),
        db_filter,
    )
    if len(seeds) < 2:
        return _empty_result()

    # -- Round structure -----------------------------------------------------
    rounds = _round_weeks(playoff_teams, playoff_start, end_week, prt)

    # -- Capture fetcher state (before we overwrite anything) ----------------
    fetcher_state = _capture_fetcher_state(conn, table, year, id_col, playoff_start, db_filter)

    # -- Championship week (formula + validation) ----------------------------
    champ_week = _compute_championship_week_with_validation(
        conn, table, year, id_col, playoff_start, end_week, playoff_teams, prt, rounds, db_filter
    )

    # -- Identify championship matchup (4-priority rule) ---------------------
    champ_matchup = _identify_championship_matchup(
        conn,
        table,
        year,
        id_col,
        opp_col,
        seeds,
        playoff_teams,
        bye_count,
        champ_week,
        rounds,
        fetcher_state,
        db_filter,
    )

    if champ_matchup is None:
        # Could not identify championship matchup — fall back to empty
        return _empty_result()

    team_a, team_b = champ_matchup

    # -- Backward walk from championship to classify edges -------------------
    bw_result = _backward_walk(
        conn, table, year, id_col, opp_col, seeds, team_a, team_b, champ_week, rounds, bye_count, db_filter
    )

    classifications = bw_result["classifications"]
    rounds_log = bw_result["rounds_log"]
    champion = bw_result["champion"]
    runner_up = bw_result["runner_up"]
    elimination_round = bw_result["elimination_round"]
    champ_bracket_teams = bw_result["champ_bracket_teams"]
    is_complete = bw_result["is_complete"]

    # -- Reconcile champion with fetcher state ------------------------------
    champion = _reconcile_champion(champion, fetcher_state, seeds, playoff_teams)

    championship_teams = (team_a, team_b)

    result = {
        "classifications": classifications,
        "championship_week": champ_week,
        "championship_teams": championship_teams,
        "champion": champion,
        "runner_up": runner_up,
        "rounds": rounds_log,
        "is_complete": is_complete,
        "elimination_round": elimination_round,
        "champ_bracket_teams": champ_bracket_teams,
    }

    # -- Write back to DDL ---------------------------------------------------
    if write_back and classifications:
        _write_back_championship_only(
            conn,
            table,
            year,
            id_col,
            playoff_start,
            end_week,
            champ_week,
            classifications,
            championship_teams,
            champion,
            rounds_log,
            seeds,
            db_filter,
        )

    return result


# Alias for backward compatibility
trace_bracket_sql = trace_championship_bracket_sql


# ---------------------------------------------------------------------------
# Legacy DataFrame API (kept for callers that still use it)
# ---------------------------------------------------------------------------


def trace_bracket(
    matchup_df,
    year: int,
    settings: dict,
    id_col: str = "franchise_id",
) -> dict:
    """DataFrame-based bracket tracer (legacy wrapper).

    Loads the DataFrame into a temporary DuckDB, runs trace_championship_bracket_sql,
    and returns the result dict. Does NOT write back — the caller handles
    DataFrame updates.
    """
    import duckdb

    from multi_league.transformations.matchup.modules.playoff_bracket.bracket_common import (
        resolve_id_col,
    )

    id_col = resolve_id_col(matchup_df, id_col)
    conn = duckdb.connect()
    try:
        conn.register("_matchup_view", matchup_df)
        conn.execute("CREATE TABLE matchup AS SELECT * FROM _matchup_view")
        result = trace_championship_bracket_sql(
            conn,
            year,
            settings,
            table="matchup",
            id_col=id_col,
            write_back=False,
        )
    finally:
        conn.close()
    return result


def apply_bracket_results(df, bracket_result, year, id_col="franchise_id"):
    """Apply bracket trace results to a pandas DataFrame (legacy)."""
    import pandas as pd

    from multi_league.transformations.matchup.modules.playoff_bracket.bracket_common import (
        resolve_id_col,
    )

    df = df.copy()
    id_col = resolve_id_col(df, id_col)
    year_mask = df["year"] == year

    for col in ("is_playoffs", "is_consolation", "championship", "champion", "sacko"):
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = df[col].astype(object).fillna(0)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    if "playoff_round" not in df.columns:
        df["playoff_round"] = ""
    else:
        df["playoff_round"] = df["playoff_round"].fillna("").astype(str)

    for col in ("is_playoffs", "is_consolation", "championship", "champion", "sacko"):
        df.loc[year_mask, col] = 0
    df.loc[year_mask, "playoff_round"] = ""

    classifications = bracket_result.get("classifications", {})
    champion = bracket_result.get("champion")
    sacko_id = bracket_result.get("sacko")
    rounds_log = bracket_result.get("rounds", [])
    championship_week = bracket_result.get("championship_week")

    round_lookup: dict[tuple[str, int], int] = {}
    for rd in rounds_log:
        r_num = rd["round"]
        wk = rd["week"]
        if isinstance(wk, str) and "-" in wk:
            wk_s, wk_e = (int(x) for x in wk.split("-"))
        else:
            wk_s = wk_e = int(wk)
        for m in rd["matchups"]:
            for tid in (m["high_seed_id"], m["low_seed_id"]):
                for w in range(wk_s, wk_e + 1):
                    round_lookup[(tid, w)] = r_num

    for idx in df[year_mask].index:
        tid = df.at[idx, id_col]
        wk = df.at[idx, "week"]
        if pd.isna(tid) or pd.isna(wk):
            continue
        key = (tid, int(wk))
        cls = classifications.get(key)
        if cls == "playoff":
            df.at[idx, "is_playoffs"] = 1
            df.at[idx, "is_consolation"] = 0
        elif cls == "consolation":
            df.at[idx, "is_consolation"] = 1
            df.at[idx, "is_playoffs"] = 0
        if key in round_lookup:
            df.at[idx, "playoff_round"] = round_lookup[key]

    # For multi-week championship rounds, mark ALL weeks of the round
    champ_rounds = bracket_result.get("rounds", [])
    champ_weeks = [championship_week] if championship_week else []
    if champ_rounds:
        last_round = champ_rounds[-1]
        rw = last_round.get("week")
        if isinstance(rw, str) and "-" in rw:
            cr_s, cr_e = (int(x) for x in rw.split("-"))
            champ_weeks = list(range(cr_s, cr_e + 1))
        elif rw is not None:
            champ_weeks = [int(rw)]

    if champ_weeks and bracket_result.get("championship_teams"):
        ct = bracket_result["championship_teams"]
        for cw in champ_weeks:
            mask = year_mask & (df["week"] == cw) & (df[id_col].isin(ct))
            df.loc[mask, "championship"] = 1

    if champion:
        if champ_weeks:
            for cw in champ_weeks:
                df.loc[year_mask & (df[id_col] == champion) & (df["week"] == cw), "champion"] = 1
        else:
            df.loc[year_mask & (df[id_col] == champion), "champion"] = 1
    if sacko_id:
        # Sacko game is on the last consolation week (may differ from championship week)
        sacko_weeks = sorted(
            {wk for (tid, wk), cls in classifications.items() if tid == sacko_id and cls == "consolation"}
        )
        sacko_week = sacko_weeks[-1] if sacko_weeks else championship_week
        if sacko_week:
            df.loc[year_mask & (df[id_col] == sacko_id) & (df["week"] == sacko_week), "sacko"] = 1
        else:
            df.loc[year_mask & (df[id_col] == sacko_id), "sacko"] = 1

    return df


# ---------------------------------------------------------------------------
# Phase 2: New helpers
# ---------------------------------------------------------------------------


def _compute_championship_week_with_validation(
    conn, table, year, id_col, playoff_start, end_week, playoff_teams, prt, rounds, db_filter="1=1"
) -> int:
    """Compute championship week via formula, validated against actual data.

    Formula: last round's end week from _round_weeks().
    Actual: max week where >= 2 teams have non-NULL opponent and points.

    Trust actual when off-by-1 from formula.
    Trust formula when deviation > 1.
    """
    # Formula-based championship week
    formula_week = rounds[-1][1] if rounds else end_week

    # Actual: find the max week with at least 2 teams playing
    try:
        rows = conn.execute(
            f"SELECT week, COUNT(DISTINCT {id_col}) AS n_teams "
            f"FROM {table} "
            f"WHERE year = {year} AND week >= {playoff_start} "
            f"AND {id_col} IS NOT NULL "
            f"AND team_points IS NOT NULL AND opponent_points IS NOT NULL "
            f"AND {db_filter} "
            f"GROUP BY week "
            f"HAVING COUNT(DISTINCT {id_col}) >= 2 "
            f"ORDER BY week DESC "
            f"LIMIT 1"
        ).fetchall()
        actual_week = int(rows[0][0]) if rows else formula_week
    except Exception:
        actual_week = formula_week

    deviation = abs(formula_week - actual_week)
    if deviation <= 1:
        # Off-by-1: trust actual ONLY when actual <= formula (handles short
        # seasons where the league truncated before the formula-predicted
        # championship). Never trust actual past formula — the max-week query
        # also picks up consolation games, which can extend the league one
        # week past the actual championship and would push champ_week into
        # post-championship phantom territory (memory:
        # feedback_phantom_rows_no_flags). Concrete fleet repro:
        # dingleberry_derby 2018 has formula=15, actual=16 (W16 consolation),
        # and pre-fix the tracer treated W16 as the championship week and
        # wrote phantom-row classifications.
        return min(formula_week, actual_week)
    else:
        # Large deviation — trust formula (actual data might be incomplete)
        logger.warning(
            "Championship week: formula=%d, actual=%d (deviation=%d). Trusting formula.",
            formula_week,
            actual_week,
            deviation,
        )
        return formula_week


def _capture_fetcher_state(conn, table, year, id_col, playoff_start, db_filter="1=1") -> dict:
    """Capture fetcher-set flags before we overwrite them.

    Returns dict with:
        - championship_flags: set of franchise_ids with is_championship=1
        - champion_flags: set of franchise_ids with champion=1
    """
    championship_flags = set()
    champion_flags = set()
    try:
        rows = conn.execute(
            f"SELECT {id_col}, "
            f"MAX(CASE WHEN CAST(is_championship AS INTEGER) = 1 THEN 1 ELSE 0 END) AS is_champ, "
            f"MAX(CASE WHEN CAST(champion AS INTEGER) = 1 THEN 1 ELSE 0 END) AS is_winner "
            f"FROM {table} "
            f"WHERE year = {year} AND week >= {playoff_start} AND {id_col} IS NOT NULL "
            f"AND {db_filter} "
            f"GROUP BY {id_col}"
        ).fetchall()
        for tid, is_champ, is_winner in rows:
            if is_champ:
                championship_flags.add(str(tid))
            if is_winner:
                champion_flags.add(str(tid))
    except Exception:
        pass

    return {
        "championship_flags": championship_flags,
        "champion_flags": champion_flags,
    }


def _identify_championship_matchup(
    conn,
    table,
    year,
    id_col,
    opp_col,
    seeds,
    playoff_teams,
    bye_count,
    champ_week,
    rounds,
    fetcher_state,
    db_filter="1=1",
) -> tuple[str, str] | None:
    """Identify the championship matchup using 4-priority rule.

    P1: fetcher-set is_championship=1 flag
    P2: no-observed-loss filter (teams that never lost in prior playoff weeks)
    P3: speculative bracket-depth scoring (backward-walk along WINNING paths)
    P4: seed-sum tiebreak (lowest combined seeds)

    Does NOT filter by is_playoffs=1 (on fresh imports, is_playoffs hasn't
    been written yet).

    Returns (team_a, team_b) tuple or None if cannot identify.
    """
    # Get all pairs playing in championship round (may span multiple weeks)
    champ_round_start, champ_round_end = rounds[-1]
    champ_pairs = _sql_pairs(conn, table, year, champ_round_start, champ_round_end, id_col, opp_col, db_filter)
    if not champ_pairs:
        return None

    graph_pair = _identify_championship_matchup_from_winner_graph(
        conn,
        table,
        year,
        id_col,
        opp_col,
        seeds,
        playoff_teams,
        bye_count,
        rounds,
        db_filter,
    )
    if graph_pair is not None:
        return graph_pair

    def _is_championship_seed(team_id: str) -> bool:
        return seeds.get(team_id, 999) <= playoff_teams

    def _both_championship_seeds(pair: tuple[str, str]) -> bool:
        a, b = pair
        return _is_championship_seed(a) and _is_championship_seed(b)

    # P1: fetcher-set is_championship flag. Treat it as advisory only:
    # flags from non-playoff seeds are lower-bracket data, not title finalists.
    fetcher_champ_ids = fetcher_state.get("championship_flags", set())
    fetcher_champ_ids = {team_id for team_id in fetcher_champ_ids if _is_championship_seed(team_id)}
    if len(fetcher_champ_ids) >= 2:
        for a, b in champ_pairs:
            if a in fetcher_champ_ids and b in fetcher_champ_ids:
                return (a, b)

    # P2: no-observed-loss filter
    # Find teams that lost in prior playoff weeks (before championship week)
    losers_in_prior = set()
    for round_num, (wk_start, wk_end) in enumerate(rounds[:-1], 1):
        round_pairs = _sql_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter)
        for a, b in round_pairs:
            winner = _sql_winner(conn, table, year, a, b, wk_start, wk_end, id_col, opp_col, seeds, db_filter)
            if winner:
                loser = b if winner == a else a
                losers_in_prior.add(loser)

    undefeated_pairs = [(a, b) for a, b in champ_pairs if a not in losers_in_prior and b not in losers_in_prior]

    # P2.5: Filter to championship-eligible seeds (≤ playoff_teams).
    # Consolation bracket teams (seeds > playoff_teams) cannot be championship finalists.
    champ_eligible = [(a, b) for a, b in undefeated_pairs if _both_championship_seeds((a, b))]
    if champ_eligible:
        undefeated_pairs = champ_eligible

    if len(undefeated_pairs) == 1:
        return undefeated_pairs[0]

    # P3: speculative bracket-depth scoring
    # Walk backward along WINNING paths; consolation chains break when
    # winner != survivor. Score = number of rounds a team won consecutively.
    if len(undefeated_pairs) > 1:
        candidate_pairs = undefeated_pairs
    elif undefeated_pairs:
        candidate_pairs = undefeated_pairs
    else:
        candidate_pairs = champ_pairs

    seed_eligible_candidates = [(a, b) for a, b in candidate_pairs if _both_championship_seeds((a, b))]
    if seed_eligible_candidates:
        candidate_pairs = seed_eligible_candidates

    def _bracket_depth(team_id: str) -> int:
        """Count consecutive wins backward from championship week."""
        depth = 0
        for round_num in range(len(rounds) - 2, -1, -1):
            wk_start, wk_end = rounds[round_num]
            round_pairs = _sql_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter)
            found = False
            for a, b in round_pairs:
                if team_id in (a, b):
                    winner = _sql_winner(conn, table, year, a, b, wk_start, wk_end, id_col, opp_col, seeds, db_filter)
                    if winner == team_id:
                        depth += 1
                        found = True
                    break
            if not found:
                # Bye round — count as depth for bye-eligible seeds so that
                # bye teams aren't penalised vs consolation teams who played
                # (and won) every round.
                if seeds.get(team_id, 999) <= bye_count:
                    depth += 1
                continue
        return depth

    scored_pairs = []
    for a, b in candidate_pairs:
        score = _bracket_depth(a) + _bracket_depth(b)
        scored_pairs.append((score, a, b))

    if scored_pairs:
        max_score = max(s for s, _, _ in scored_pairs)
        if max_score > 0:
            # Filter to pairs with the best bracket depth
            top_pairs = [(a, b) for s, a, b in scored_pairs if s == max_score]
            if len(top_pairs) == 1:
                return top_pairs[0]
            # Multiple ties on P3 — fall through to P4 with narrowed set
            candidate_pairs = top_pairs

    # P4: seed-sum tiebreak (lowest combined seeds = highest-seeded pair)
    best_pair = None
    best_seed_sum = 9999
    for a, b in candidate_pairs:
        seed_sum = seeds.get(a, 999) + seeds.get(b, 999)
        if seed_sum < best_seed_sum:
            best_seed_sum = seed_sum
            best_pair = (a, b)

    return best_pair


def _identify_championship_matchup_from_winner_graph(
    conn,
    table,
    year,
    id_col,
    opp_col,
    seeds,
    playoff_teams,
    bye_count,
    rounds,
    db_filter="1=1",
) -> tuple[str, str] | None:
    """Identify the title game by following platform winner keys.

    Yahoo exposes actual scoreboard pairings plus winner_team_key. The raw
    playoff/consolation flags are not canonical enough to store directly, but
    the winner graph is the safest way to handle custom opponent choices,
    non-standard bracket ordering, and older Yahoo seasons.
    """
    if not _table_has_columns(conn, table, {"team_key", "winner_team_key"}):
        return None

    alive = {team_id for team_id, seed in seeds.items() if seed <= playoff_teams}
    if not alive:
        return None

    last_round_pairs: list[tuple[str, str]] = []
    for round_index, (wk_start, wk_end) in enumerate(rounds, 1):
        pairs = _sql_direct_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter)
        if not pairs:
            return None

        candidate_pairs = [(a, b) for a, b in pairs if a in alive and b in alive]
        if not candidate_pairs:
            candidate_pairs = [(a, b) for a, b in pairs if a in alive or b in alive]
        if not candidate_pairs:
            return None

        winners: set[str] = set()
        played: set[str] = set()
        for a, b in candidate_pairs:
            played.update((a, b))
            # Per-week winner key is authoritative only for single-week rounds;
            # multi-week rounds are decided by combined score (handled in _sql_winner).
            winner = None
            if wk_start == wk_end:
                winner = _sql_winner_from_team_key(
                    conn, table, year, a, b, wk_start, wk_end, id_col, opp_col, db_filter
                )
            if winner is None:
                winner = _sql_winner(conn, table, year, a, b, wk_start, wk_end, id_col, opp_col, seeds, db_filter)
            if winner in (a, b):
                winners.add(winner)

        if not winners:
            return None

        # First-round byes are alive even though they do not appear in a pair.
        bye_survivors = {
            team_id for team_id in alive - played if round_index == 1 and seeds.get(team_id, 999) <= bye_count
        }
        alive = winners | bye_survivors
        last_round_pairs = candidate_pairs

    if len(last_round_pairs) == 1:
        return last_round_pairs[0]
    return None


def _backward_walk(
    conn, table, year, id_col, opp_col, seeds, team_a, team_b, champ_week, rounds, bye_count, db_filter="1=1"
) -> dict:
    """Walk backward from championship classifying game edges.

    Start with survivor_set = {both championship finalists}.
    For each round from championship back to round 1, find each
    survivor's opponent, classify both as "playoff", track elimination.

    Handles byes: true bye = row where opponent IS NULL AND team_points
    IS NULL AND opponent_points IS NULL. Survivor propagates backward
    without classification.

    Returns dict with classifications, rounds_log, champion, runner_up,
    elimination_round, champ_bracket_teams, is_complete.
    """
    num_rounds = len(rounds)
    classifications: dict[tuple[str, int], str] = {}
    rounds_log: list[dict] = []
    elimination_round: dict[str, int] = {}
    is_complete = True

    # Determine champion/runner_up from the full championship round range
    # (for 2-week rounds, sum scores across both weeks)
    champ_round_start, champ_round_end = rounds[-1]
    champion = _sql_winner(
        conn, table, year, team_a, team_b, champ_round_start, champ_round_end, id_col, opp_col, seeds, db_filter
    )
    if champion:
        runner_up = team_b if champion == team_a else team_a
    else:
        champion = None
        runner_up = None
        is_complete = False

    # Start: survivor_set = championship finalists
    # We'll build round info from championship backward, then reverse
    survivor_set = {team_a, team_b}
    champ_bracket_teams = {team_a, team_b}

    # Classify championship week
    for tid in (team_a, team_b):
        classifications[(tid, champ_week)] = "playoff"

    # Handle multi-week championship
    champ_round_start, champ_round_end = rounds[-1]
    for wk in range(champ_round_start, champ_round_end + 1):
        for tid in (team_a, team_b):
            classifications[(tid, wk)] = "playoff"

    # Build championship round log entry
    s_a, s_b = seeds.get(team_a, 99), seeds.get(team_b, 99)
    champ_matchup_entry = {
        "high_seed_id": team_a if s_a <= s_b else team_b,
        "low_seed_id": team_b if s_a <= s_b else team_a,
        "high_seed": min(s_a, s_b),
        "low_seed": max(s_a, s_b),
        "winner": champion,
    }
    champ_round_log = {
        "round": num_rounds,
        "week": champ_round_start if champ_round_start == champ_round_end else f"{champ_round_start}-{champ_round_end}",
        "matchups": [champ_matchup_entry],
    }

    # Track runner_up elimination
    if runner_up:
        elimination_round[runner_up] = num_rounds

    # Backward walk: from round (num_rounds - 1) down to round 1
    backward_rounds_log = [champ_round_log]

    for round_idx in range(num_rounds - 2, -1, -1):
        round_num = round_idx + 1
        wk_start, wk_end = rounds[round_idx]

        # For each survivor, find their opponent in this round
        all_pairs = _sql_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter)

        round_matchups = []
        new_survivors = set()

        for survivor in list(survivor_set):
            # Find the pair involving this survivor
            opponent = None
            for a, b in all_pairs:
                if a == survivor:
                    opponent = b
                    break
                elif b == survivor:
                    opponent = a
                    break

            if opponent is None:
                # Check for true bye: row with NULL opponent and NULL points
                is_bye = _is_true_bye(conn, table, year, id_col, survivor, wk_start, wk_end, db_filter)
                if is_bye:
                    # Bye team: propagate backward without classification.
                    # Per project policy (memory: feedback_phantom_rows_no_flags)
                    # bye rows must not get is_playoffs / playoff_round set —
                    # those flags only belong on weeks the team actually played.
                    new_survivors.add(survivor)
                    continue
                else:
                    # No game found and not a bye — might be data gap
                    logger.debug(
                        "No opponent found for %s in round %d (weeks %d-%d). Treating as data gap.",
                        survivor,
                        round_num,
                        wk_start,
                        wk_end,
                    )
                    new_survivors.add(survivor)
                    continue

            # Symmetry check: if A->B exists, B->A must exist
            reverse_found = False
            for a, b in all_pairs:
                if (a == opponent and b == survivor) or (b == opponent and a == survivor):
                    reverse_found = True
                    break
            if not reverse_found:
                raise ValueError(
                    f"Symmetry violation: {survivor}->{opponent} exists but "
                    f"reverse pair not found in week {wk_start}-{wk_end}"
                )

            # Classify both teams for this round
            for wk in range(wk_start, wk_end + 1):
                classifications[(survivor, wk)] = "playoff"
                classifications[(opponent, wk)] = "playoff"

            champ_bracket_teams.add(opponent)

            # Determine winner
            winner = _sql_winner(
                conn, table, year, survivor, opponent, wk_start, wk_end, id_col, opp_col, seeds, db_filter
            )

            if winner:
                loser = opponent if winner == survivor else survivor
                # The survivor should be the winner going forward
                if winner != survivor:
                    # This means the survivor actually lost this round — shouldn't
                    # happen in a backward walk from the championship. But handle
                    # gracefully.
                    logger.warning(
                        "Backward walk: %s was expected to win round %d but %s won. Data inconsistency.",
                        survivor,
                        round_num,
                        winner,
                    )
                # The loser was eliminated in this round
                elimination_round[loser] = round_num
                # Walk further back: BOTH teams played in the prior round
                # (or had byes).  We must trace both paths backward so all
                # QF games are discovered in 6-team brackets where bye
                # teams and non-bye teams co-exist.
                new_survivors.add(survivor)
                new_survivors.add(opponent)
            else:
                is_complete = False
                new_survivors.add(survivor)
                new_survivors.add(opponent)

            s_surv, s_opp = seeds.get(survivor, 99), seeds.get(opponent, 99)
            round_matchups.append(
                {
                    "high_seed_id": survivor if s_surv <= s_opp else opponent,
                    "low_seed_id": opponent if s_surv <= s_opp else survivor,
                    "high_seed": min(s_surv, s_opp),
                    "low_seed": max(s_surv, s_opp),
                    "winner": winner,
                }
            )

        survivor_set = new_survivors

        backward_rounds_log.append(
            {
                "round": round_num,
                "week": wk_start if wk_start == wk_end else f"{wk_start}-{wk_end}",
                "matchups": round_matchups,
            }
        )

    # Reverse to get chronological order
    rounds_log = list(reversed(backward_rounds_log))

    return {
        "classifications": classifications,
        "rounds_log": rounds_log,
        "champion": champion,
        "runner_up": runner_up,
        "is_complete": is_complete,
        "elimination_round": elimination_round,
        "champ_bracket_teams": champ_bracket_teams,
    }


def _is_true_bye(conn, table, year, id_col, team_id, wk_start, wk_end, db_filter="1=1") -> bool:
    """Check if a team has a true bye (NULL opponent AND NULL points) in the given weeks."""
    try:
        rows = conn.execute(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE year = {year} AND week >= {wk_start} AND week <= {wk_end} "
            f"AND {id_col} = '{_esc(team_id)}' "
            f"AND {db_filter} "
            f"AND opponent IS NULL AND team_points IS NULL AND opponent_points IS NULL"
        ).fetchall()
        return rows[0][0] > 0
    except Exception:
        return False


def _forfeit_tiebreak(seeds: dict[str, int], team_a: str, team_b: str) -> str:
    """When both teams have equal scores (including 0-0 forfeit), lower seed wins.

    'Lower seed' means numerically lower seed number = better seed = wins.
    """
    seed_a = seeds.get(team_a, 999)
    seed_b = seeds.get(team_b, 999)
    return team_a if seed_a <= seed_b else team_b


def _reconcile_champion(
    tracer_champion: str | None,
    fetcher_state: dict,
    seeds: dict[str, int],
    playoff_teams: int | None = None,
) -> str | None:
    """Reconcile tracer's champion with fetcher's champion flag.

    If both agree, return the champion.
    If fetcher says someone else won, trust tracer (data-first).
    If tracer couldn't determine (None), use fetcher as fallback.
    """
    fetcher_champions = fetcher_state.get("champion_flags", set())

    if tracer_champion:
        return tracer_champion

    # Tracer couldn't determine — use fetcher as fallback
    if len(fetcher_champions) == 1:
        candidate = next(iter(fetcher_champions))
        if playoff_teams is not None and seeds.get(candidate, 999) > playoff_teams:
            return None
        return candidate

    return None


# ---------------------------------------------------------------------------
# Internal SQL helpers
# ---------------------------------------------------------------------------


def _normalise_settings(s: dict) -> dict:
    out = dict(s)
    if "playoff_teams" not in out and "num_playoff_teams" in out:
        out["playoff_teams"] = out["num_playoff_teams"]
    return out


def _pick_opp_col(conn, table: str, id_col: str) -> str:
    """Determine best opponent column available in the table."""
    if id_col != "franchise_id":
        raise ValueError(f"{table} requires franchise_id identity; got {id_col!r}")

    try:
        cols = [r[0] for r in conn.execute(f"DESCRIBE {table}").fetchall()]
        if "opponent_franchise_id" in cols:
            return "opponent_franchise_id"
    except Exception:
        pass
    raise ValueError(f"{table} is missing opponent_franchise_id; refusing mixed franchise_id/manager matching")


def _sql_seeds(
    conn,
    table,
    year,
    playoff_start,
    id_col,
    uses_median,
    seeding_rule=None,
    seeding_rule_by=None,
    db_filter="1=1",
) -> dict[str, int]:
    total_wins_expr = (
        f"h.h2h_wins + CASE WHEN {'TRUE' if uses_median else 'FALSE'} THEN COALESCE(m.med_wins, 0) ELSE 0 END"
    )
    if normalize_playoff_seeding_rule(seeding_rule, seeding_rule_by) == POINTS_FIRST_SEEDING:
        seed_order = f"h.total_points DESC, {total_wins_expr} DESC, h.tid ASC"
    else:
        seed_order = f"{total_wins_expr} DESC, h.total_points DESC, h.tid ASC"
    sql = _SEED_SQL.format(
        table=table,
        year=year,
        playoff_start_week=playoff_start,
        id_col=id_col,
        uses_median="TRUE" if uses_median else "FALSE",
        seed_order=seed_order,
        db_filter=db_filter,
    )
    rows = conn.execute(sql).fetchall()
    return {str(r[0]): int(r[5]) for r in rows}


def _sql_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter="1=1") -> list[tuple[str, str]]:
    pairs = _sql_direct_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter)

    # Score-based fallback: find additional pairs missed due to NULL opp_col
    if id_col == "franchise_id" and opp_col == "opponent_franchise_id":
        fallback_sql = _PAIRS_SCORE_SQL.format(
            table=table,
            year=year,
            wk_start=wk_start,
            wk_end=wk_end,
            id_col=id_col,
            db_filter_m=db_filter.replace("db_name", "m.db_name") if "db_name" in db_filter else db_filter,
            db_filter_opp=db_filter.replace("db_name", "opp.db_name") if "db_name" in db_filter else db_filter,
        )
        try:
            fallback = [(str(r[0]), str(r[1])) for r in conn.execute(fallback_sql).fetchall()]
            existing = {(min(a, b), max(a, b)) for a, b in pairs}
            for a, b in fallback:
                key = (min(a, b), max(a, b))
                if key not in existing:
                    pairs.append((a, b))
                    existing.add(key)
        except Exception:
            pass

    return pairs


def _sql_direct_pairs(conn, table, year, wk_start, wk_end, id_col, opp_col, db_filter="1=1") -> list[tuple[str, str]]:
    sql = _PAIRS_SQL.format(
        table=table,
        year=year,
        wk_start=wk_start,
        wk_end=wk_end,
        id_col=id_col,
        opp_col=opp_col,
        db_filter=db_filter,
    )
    return [(str(r[0]), str(r[1])) for r in conn.execute(sql).fetchall()]


def _sql_winner(
    conn, table, year, team_a, team_b, wk_start, wk_end, id_col, opp_col, seeds, db_filter="1=1"
) -> str | None:
    """Determine winner from actual H2H data. Returns None if they didn't play.

    The platform winner key (Yahoo ``winner_team_key``) is per-week, so it is only
    authoritative for single-week rounds. Multi-week rounds are decided by COMBINED
    score across the round, so for those we skip the per-week key and use the
    summed-points logic below.
    """
    if wk_start == wk_end:
        platform_winner = _sql_winner_from_team_key(
            conn, table, year, team_a, team_b, wk_start, wk_end, id_col, opp_col, db_filter
        )
        if platform_winner is not None:
            return platform_winner

    sql = _WINNER_SQL.format(
        table=table,
        year=year,
        wk_start=wk_start,
        wk_end=wk_end,
        id_col=id_col,
        opp_col=opp_col,
        team_a=_esc(team_a),
        team_b=_esc(team_b),
        db_filter=db_filter,
    )
    rows = conn.execute(sql).fetchall()

    # Score-based fallback when opp_col is NULL for some rows
    if not rows and id_col == "franchise_id" and opp_col == "opponent_franchise_id":
        fallback_sql = _WINNER_SCORE_SQL.format(
            table=table,
            year=year,
            wk_start=wk_start,
            wk_end=wk_end,
            id_col=id_col,
            team_a=_esc(team_a),
            team_b=_esc(team_b),
            db_filter_m=db_filter.replace("db_name", "m.db_name") if "db_name" in db_filter else db_filter,
            db_filter_opp=db_filter.replace("db_name", "opp.db_name") if "db_name" in db_filter else db_filter,
        )
        try:
            rows = conn.execute(fallback_sql).fetchall()
        except Exception:
            pass

    if not rows:
        return None

    # SUM over NULL points returns NULL; treat as "points-unavailable" and fall
    # back to the per-row `win` flag. Externally-imported data (KMFFL 2013
    # playoffs) ships with win=1/loss=0 even when scores are missing, so
    # win_count carries the truth. When both points and win flags are NULL
    # for both teams we genuinely can't tell — return None so the tracer
    # leaves the champion unset.
    pts: dict[str, float] = {}
    wins: dict[str, int] = {}
    for r in rows:
        tid = str(r[0])
        if r[1] is not None:
            pts[tid] = float(r[1])
        if len(r) > 2 and r[2] is not None:
            wins[tid] = int(r[2])

    pa = pts.get(team_a)
    pb = pts.get(team_b)

    # Primary path: scores available for both teams.
    if pa is not None and pb is not None:
        if pa > pb:
            return team_a
        if pb > pa:
            return team_b
        # Tied score with an explicit platform result — trust the fetcher/API
        # outcome before falling back to seed rules.
        wa = wins.get(team_a, 0)
        wb = wins.get(team_b, 0)
        if wa > wb:
            return team_a
        if wb > wa:
            return team_b
        # True tie with no platform winner — lower seed wins.
        return team_a if seeds.get(team_a, 999) < seeds.get(team_b, 999) else team_b

    # Fallback path: scores missing — use win flag(s).
    wa = wins.get(team_a, 0)
    wb = wins.get(team_b, 0)
    if wa > wb:
        return team_a
    if wb > wa:
        return team_b

    return None


def _table_has_columns(conn, table: str, columns: set[str]) -> bool:
    try:
        existing = {str(row[0]) for row in conn.execute(f"DESCRIBE {table}").fetchall()}
    except Exception:
        return False
    return columns.issubset(existing)


def _sql_winner_from_team_key(
    conn,
    table,
    year,
    team_a,
    team_b,
    wk_start,
    wk_end,
    id_col,
    opp_col,
    db_filter="1=1",
) -> str | None:
    """Resolve the platform winner key to the tracer identity column."""
    if not _table_has_columns(conn, table, {"team_key", "winner_team_key"}):
        return None

    rows = conn.execute(
        f"""
        SELECT week, {id_col} AS tid, team_key, winner_team_key
        FROM {table}
        WHERE year = {year}
          AND week >= {wk_start} AND week <= {wk_end}
          AND {db_filter}
          AND (
            ({id_col} = '{_esc(team_a)}' AND {opp_col} = '{_esc(team_b)}')
            OR ({id_col} = '{_esc(team_b)}' AND {opp_col} = '{_esc(team_a)}')
          )
        ORDER BY week DESC
        """
    ).fetchall()
    if not rows:
        return None

    by_week: dict[int, list[tuple[str, str | None, str | None]]] = {}
    for week, tid, team_key, winner_team_key in rows:
        if week is None or tid is None:
            continue
        by_week.setdefault(int(week), []).append(
            (
                str(tid),
                str(team_key).strip() if team_key is not None else None,
                str(winner_team_key).strip() if winner_team_key is not None else None,
            )
        )

    for week in sorted(by_week, reverse=True):
        week_rows = by_week[week]
        key_to_id = {
            team_key: tid
            for tid, team_key, _winner_team_key in week_rows
            if team_key and team_key.lower() not in {"none", "nan", "--"}
        }
        winner_keys = [
            winner_team_key
            for _tid, _team_key, winner_team_key in week_rows
            if winner_team_key and winner_team_key.lower() not in {"none", "nan", "--"}
        ]
        for winner_key in winner_keys:
            winner_id = key_to_id.get(winner_key)
            if winner_id in {str(team_a), str(team_b)}:
                return winner_id

    return None


def _write_back_championship_only(
    conn,
    table,
    year,
    id_col,
    playoff_start,
    end_week,
    champ_week,
    classifications,
    championship_teams,
    champion,
    rounds_log,
    seeds,
    db_filter="1=1",
):
    """Write championship bracket results back.

    Resets: is_playoffs, is_consolation, is_championship, champion, playoff_round,
    final_playoff_seed for the year. Then writes championship-specific values.
    is_consolation is reset here so the consolation tracer starts from clean state;
    the consolation tracer re-derives is_consolation from is_playoffs afterward.
    """
    num_rounds = len(rounds_log)

    # -- Reset championship-owned columns for this year --------------------
    # Also reset is_consolation so the consolation tracer starts clean.
    # Without this, enforce_postseason_flags may have pre-labeled championship
    # games as consolation (e.g. ESPN partial flags), leaving stale state.
    conn.execute(
        f"UPDATE {table} SET "
        f"is_playoffs = 0, is_consolation = 0, is_championship = FALSE, "
        f"champion = 0, playoff_round = NULL, final_playoff_seed = NULL "
        f"WHERE year = {year} AND week >= {playoff_start} AND {db_filter}"
    )

    # -- is_playoffs --------------------------------------------------------
    playoff_keys = []
    for (tid, wk), label in classifications.items():
        if label == "playoff":
            playoff_keys.append(f"({year}, '{_esc(tid)}', {wk})")

    if playoff_keys:
        vals = ", ".join(playoff_keys)
        update_sql = f"UPDATE {table} SET is_playoffs = 1 WHERE year = {year} AND {db_filter} AND (year, {id_col}, week) IN ({vals})"
        conn.execute(update_sql)
        # Verify the update actually took effect
        verify = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE year = {year} AND {db_filter} AND CAST(is_playoffs AS INTEGER) = 1"
        ).fetchone()[0]
        print(
            f"[BRACKET DIAG] {year}: wrote {len(playoff_keys)} playoff keys, verified {verify} rows with is_playoffs=1"
        )
        if verify == 0:
            print(f"[BRACKET DIAG] {year}: UPDATE SQL: {update_sql[:300]}")

    # -- playoff_round (string label for championship bracket games) ---------
    for rnd in rounds_log:
        r_num = rnd["round"]
        wk = rnd["week"]
        if isinstance(wk, str) and "-" in wk:
            wk_s, wk_e = (int(x) for x in wk.split("-"))
        else:
            wk_s = wk_e = int(wk)

        offset_from_end = num_rounds - r_num
        if offset_from_end == 0:
            label = "championship"
        elif offset_from_end == 1:
            label = "semifinal"
        elif offset_from_end == 2:
            label = "quarterfinal"
        else:
            label = f"round_{r_num}"

        # Collect team IDs that played championship bracket this round.
        # Bye rows are intentionally excluded — per project policy
        # (memory: feedback_phantom_rows_no_flags) phantom/bye rows must not
        # carry playoff_round or is_playoffs flags.
        round_teams = set()
        for m in rnd["matchups"]:
            round_teams.add(m["high_seed_id"])
            round_teams.add(m["low_seed_id"])

        if round_teams:
            team_list = ", ".join(f"'{_esc(t)}'" for t in round_teams)
            for wk in range(wk_s, wk_e + 1):
                conn.execute(
                    f"UPDATE {table} SET playoff_round = '{label}' "
                    f"WHERE year = {year} AND week = {wk} AND {id_col} IN ({team_list}) "
                    f"AND {db_filter}"
                )

    # -- final_playoff_seed --------------------------------------------------
    if seeds:
        for tid, seed in seeds.items():
            conn.execute(
                f"UPDATE {table} SET final_playoff_seed = {seed} WHERE year = {year} AND {id_col} = '{_esc(tid)}' AND {db_filter}"
            )

    # -- is_championship flag --------------------------------------------------
    # For multi-week championship rounds, set on ALL weeks of the round
    if championship_teams and champ_week:
        team_list = ", ".join(f"'{_esc(t)}'" for t in championship_teams)
        # Find the championship round range from rounds_log
        champ_round_entry = rounds_log[-1] if rounds_log else None
        if champ_round_entry:
            rw = champ_round_entry["week"]
            if isinstance(rw, str) and "-" in rw:
                cr_s, cr_e = (int(x) for x in rw.split("-"))
            else:
                cr_s = cr_e = int(rw)
            for wk in range(cr_s, cr_e + 1):
                conn.execute(
                    f"UPDATE {table} SET is_championship = 1 "
                    f"WHERE year = {year} AND week = {wk} AND {id_col} IN ({team_list}) AND {db_filter}"
                )
        else:
            conn.execute(
                f"UPDATE {table} SET is_championship = 1 "
                f"WHERE year = {year} AND week = {champ_week} AND {id_col} IN ({team_list}) AND {db_filter}"
            )

    # -- champion flag -------------------------------------------------------
    # For multi-week championship rounds, set on ALL weeks of the round
    if champion:
        champ_round_entry = rounds_log[-1] if rounds_log else None
        if champ_round_entry:
            rw = champ_round_entry["week"]
            if isinstance(rw, str) and "-" in rw:
                cr_s, cr_e = (int(x) for x in rw.split("-"))
            else:
                cr_s = cr_e = int(rw)
            for wk in range(cr_s, cr_e + 1):
                conn.execute(
                    f"UPDATE {table} SET champion = 1 "
                    f"WHERE year = {year} AND week = {wk} AND {id_col} = '{_esc(champion)}' AND {db_filter}"
                )
        elif champ_week:
            conn.execute(
                f"UPDATE {table} SET champion = 1 "
                f"WHERE year = {year} AND week = {champ_week} AND {id_col} = '{_esc(champion)}' AND {db_filter}"
            )
        else:
            conn.execute(
                f"UPDATE {table} SET champion = 1 WHERE year = {year} AND {id_col} = '{_esc(champion)}' AND {db_filter}"
            )


def _esc(s: str) -> str:
    """Escape single quotes for SQL literals."""
    return str(s).replace("'", "''")


def _empty_result() -> dict:
    return {
        "classifications": {},
        "championship_week": None,
        "championship_teams": None,
        "champion": None,
        "runner_up": None,
        "rounds": [],
        "is_complete": False,
        "elimination_round": {},
        "champ_bracket_teams": set(),
    }
