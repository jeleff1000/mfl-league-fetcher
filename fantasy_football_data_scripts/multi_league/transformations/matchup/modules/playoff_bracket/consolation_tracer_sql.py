"""
Consolation Tracer (SQL) -- traces consolation games and assigns placement ranks.

Works alongside bracket_tracer.py (championship bracket). Reads championship
bracket state (is_playoffs, champion, final_playoff_seed) from the matchup DDL,
then labels consolation games, placement ranks, and sacko via SQL UPDATEs.

Owns these columns (championship tracer does NOT write them):
  - is_consolation
  - consolation_round
  - placement_game
  - placement_rank
  - sacko
  - postseason

All data reads and writes are SQL against the matchup table.
Python handles the iterative logic for placement rank assignment.
"""

from __future__ import annotations

import logging
import re

from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
    _esc,
    _round_weeks,
    _pick_opp_col,
    _sql_winner_from_team_key,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rank extraction from consolation_round labels
# ---------------------------------------------------------------------------

_WORD_TO_NUM = {
    "third": 3,
    "3rd": 3,
    "fifth": 5,
    "5th": 5,
    "seventh": 7,
    "7th": 7,
    "ninth": 9,
    "9th": 9,
    "eleventh": 11,
    "11th": 11,
    "thirteenth": 13,
    "13th": 13,
}

_ORDINAL_RE = re.compile(r"(\d+)(?:st|nd|rd|th)")


def _rank_from_label(label: str) -> int | None:
    """Extract numeric rank from a consolation_round label.

    Examples:
        'third_place_game' -> 3
        '5th_place_game'   -> 5
        'consolation_final' -> None (not a placement game)
    """
    if not label or "place" not in label.lower():
        return None
    low = label.lower().replace("_", " ")
    for word, num in _WORD_TO_NUM.items():
        if word in low:
            return num
    m = _ORDINAL_RE.search(low)
    if m:
        return int(m.group(1))
    return None


def _consolation_round_winner(
    conn,
    table: str,
    year: int,
    id_col: str,
    opp_col: str,
    team_a: str,
    team_b: str,
    wk_start: int,
    wk_end: int,
    db_filter: str = "1=1",
) -> tuple[str, str]:
    """Determine winner of a consolation matchup.

    For SINGLE-week rounds, prefers the platform-provided winner (Yahoo
    ``winner_team_key``) so placement games trust the API rather than re-deriving
    from points — consistent with the championship tracer. For MULTI-week rounds the
    platform key is per-week (not combined), so we keep the combined ``team_points``
    logic, which is the correct way to decide a multi-week matchup. Falls back to
    combined points when no winner key is available (ESPN/Sleeper, older data), and
    to seed as a final tiebreak.
    """
    # Platform winner key is per-week; only authoritative for single-week rounds.
    if wk_start == wk_end:
        platform_winner = _sql_winner_from_team_key(
            conn, table, year, team_a, team_b, wk_start, wk_end, id_col, opp_col, db_filter
        )
        if platform_winner in (team_a, team_b):
            loser = team_b if platform_winner == team_a else team_a
            return platform_winner, loser

    try:
        rows = conn.execute(
            f"SELECT {id_col}, SUM(CAST(team_points AS DOUBLE)) AS total "
            f"FROM {table} "
            f"WHERE year = {year} AND {db_filter} "
            f"AND week >= {wk_start} AND week <= {wk_end} "
            f"AND {id_col} IN ('{_esc(team_a)}', '{_esc(team_b)}') "
            f"AND {opp_col} IN ('{_esc(team_a)}', '{_esc(team_b)}') "
            f"GROUP BY {id_col}"
        ).fetchall()
    except Exception:
        rows = []

    scores: dict[str, float] = {}
    for row in rows:
        scores[str(row[0])] = float(row[1] or 0)

    score_a = scores.get(team_a, 0)
    score_b = scores.get(team_b, 0)

    if score_a > score_b:
        return team_a, team_b
    if score_b > score_a:
        return team_b, team_a

    # Tie: use seed
    seed_a = _get_seed(conn, table, year, id_col, team_a, db_filter)
    seed_b = _get_seed(conn, table, year, id_col, team_b, db_filter)
    if seed_a <= seed_b:
        return team_a, team_b
    return team_b, team_a


def _ordinal(n: int) -> str:
    """Return ordinal string: 1->1st, 2->2nd, 3->3rd, etc."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def trace_consolation_sql(
    conn,
    year: int,
    settings: dict,
    table: str = "matchup",
    id_col: str = "franchise_id",
    db_filter: str = "1=1",
) -> dict:
    """Trace consolation bracket and assign placement ranks for all teams.

    Reads championship bracket state (is_playoffs, champion, final_playoff_seed)
    already written by the championship tracer. Then:
      1. Resets consolation-owned columns
      2. Sweeps non-championship postseason rows as consolation
      3. Labels consolation rounds
      4. Computes placement ranks for all teams
      5. Writes placement_rank, sacko, postseason

    Args:
        conn: DuckDB connection.
        year: Season year.
        settings: Dict with playoff_teams, bye_teams, playoff_start_week,
            end_week, num_teams, uses_median, playoff_round_type, etc.
        table: Qualified table name.
        id_col: Identity column ('franchise_id' or 'manager').

    Returns:
        Dict with placements, sacko, etc.
    """
    settings = _normalise_settings(settings)

    playoff_teams = int(settings["playoff_teams"])
    playoff_start = int(settings["playoff_start_week"])
    end_week = int(settings["end_week"])
    num_teams = int(settings["num_teams"])

    prt = int(settings.get("playoff_round_type") or 0)
    if prt == 0 and settings.get("has_multiweek_championship"):
        prt = 2

    opp_col = _pick_opp_col(conn, table, id_col)
    uses_median = bool(settings.get("uses_median_score") or settings.get("uses_median"))
    preserve_consolation_flags = bool(settings.get("preserve_consolation_flags"))

    # Step 0: Season-complete guard
    season_complete = _has_champion(conn, table, year, db_filter)

    # Step 1: Reset consolation-owned columns
    if preserve_consolation_flags:
        _reset_consolation_derived_columns(conn, table, year, db_filter)
    else:
        _reset_consolation_columns(conn, table, year, db_filter)

    # Step 2: Sweep consolation rows
    if not preserve_consolation_flags:
        _sweep_consolation(conn, table, year, playoff_start, db_filter)

    # Step 3: Label consolation rounds
    # `rounds` is the championship bracket (used for elimination round
    # detection + placement rank formula). `consolation_rounds` is derived
    # from actual consolation rows so brackets that extend past the
    # championship (e.g. 4-team championship + 8-team consolation finishing
    # one week later) get every week labeled.
    rounds = _round_weeks(playoff_teams, playoff_start, end_week, prt)
    consolation_rounds = _consolation_round_weeks_from_data(conn, table, year, playoff_start, prt, db_filter)
    _label_consolation_rounds(
        conn,
        table,
        year,
        id_col,
        opp_col,
        rounds,
        consolation_rounds,
        settings,
        uses_median,
        db_filter,
    )

    # Step 4-8: Placement ranks, sacko, postseason
    placements = {}
    sacko_id = None

    if season_complete:
        placements = _compute_placement_ranks(
            conn,
            table,
            year,
            settings,
            id_col,
            opp_col,
            rounds,
            consolation_rounds,
            uses_median,
            db_filter,
        )
        if placements:
            sacko_id = _write_sacko_from_loser_path(conn, table, year, id_col, opp_col, db_filter)
            if sacko_id:
                placements = _align_placements_with_sacko(placements, sacko_id)
            _write_placement_ranks(conn, table, year, id_col, placements, db_filter)
            if not sacko_id:
                sacko_id = _write_sacko(conn, table, year, id_col, placements, num_teams, db_filter)

    # Step 8.5: Enforce mutual exclusivity — championship tracer wins
    conn.execute(
        f"UPDATE {table} SET is_consolation = 0 "
        f"WHERE year = {year} AND {db_filter} "
        f"AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1 "
        f"AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1"
    )

    # Step 9: Derive postseason
    _derive_postseason(conn, table, year, db_filter)

    return {
        "placements": placements,
        "sacko": sacko_id,
        "season_complete": season_complete,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_settings(s: dict) -> dict:
    out = dict(s)
    if "playoff_teams" not in out and "num_playoff_teams" in out:
        out["playoff_teams"] = out["num_playoff_teams"]
    if "num_playoff_teams" not in out and "playoff_teams" in out:
        out["num_playoff_teams"] = out["playoff_teams"]
    return out


def _has_champion(conn, table: str, year: int, db_filter: str = "1=1") -> bool:
    """Check if champion=1 exists for this year."""
    try:
        rows = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE year = {year} AND {db_filter} AND COALESCE(CAST(champion AS INTEGER), 0) = 1"
        ).fetchall()
        return rows[0][0] > 0
    except Exception:
        return False


def _reset_consolation_columns(conn, table: str, year: int, db_filter: str = "1=1"):
    """Reset all consolation-owned columns for the year."""
    conn.execute(
        f"UPDATE {table} SET "
        f"is_consolation = 0, "
        f"consolation_round = NULL, "
        f"placement_game = 0, "
        f"placement_rank = NULL, "
        f"sacko = 0 "
        f"WHERE year = {year} AND {db_filter}"
    )


def _reset_consolation_derived_columns(conn, table: str, year: int, db_filter: str = "1=1"):
    """Reset derived consolation columns while preserving platform API flags."""
    conn.execute(
        f"UPDATE {table} SET "
        f"consolation_round = NULL, "
        f"placement_game = 0, "
        f"placement_rank = NULL, "
        f"sacko = 0 "
        f"WHERE year = {year} AND {db_filter}"
    )


def _sweep_consolation(conn, table: str, year: int, playoff_start: int, db_filter: str = "1=1"):
    """Mark is_consolation=1 for postseason rows NOT claimed by championship tracer.

    A consolation row is one where:
      - week >= playoff_start
      - is_playoffs = 0 (not claimed by championship tracer)
      - has a real opponent and either real points OR a recorded result
    """
    cols = {row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()}

    evidence_clauses: list[str] = []
    if {"team_points", "opponent_points"}.issubset(cols):
        evidence_clauses.append(
            "(team_points IS NOT NULL AND opponent_points IS NOT NULL "
            "AND (CAST(team_points AS DOUBLE) > 0 OR CAST(opponent_points AS DOUBLE) > 0))"
        )
    for col in ("win", "loss", "tie"):
        if col in cols:
            evidence_clauses.append(f"COALESCE(CAST({col} AS INTEGER), 0) = 1")

    if not evidence_clauses:
        return

    conn.execute(
        f"UPDATE {table} SET is_consolation = 1 "
        f"WHERE year = {year} AND {db_filter} "
        f"AND week >= {playoff_start} "
        f"AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 0 "
        f"AND opponent IS NOT NULL AND TRIM(opponent) != '' "
        f"AND ({' OR '.join(evidence_clauses)})"
    )


def _consolation_round_weeks_from_data(
    conn, table: str, year: int, playoff_start: int, prt: int, db_filter: str = "1=1"
) -> list[tuple[int, int]]:
    """Compute (wk_start, wk_end) windows for consolation rounds from actual data.

    Queries weeks where a real consolation game exists (after `_sweep_consolation`
    has run) and groups them into round windows according to `prt`:
      - prt=0 or prt=2 (championship-only multi-week): each consolation week
        is its own single-week round.
      - prt=1 (all multi-week): consecutive weeks are paired into 2-week rounds.

    Returns [] when no consolation games exist for the year. Bye / phantom rows
    are excluded by the `opponent IS NOT NULL` filter, preserving the
    phantom-row policy (see `feedback_phantom_rows_no_flags`).
    """
    try:
        rows = conn.execute(
            f"SELECT DISTINCT week FROM {table} "
            f"WHERE year = {year} AND {db_filter} "
            f"AND week >= {playoff_start} "
            f"AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1 "
            f"AND opponent IS NOT NULL AND TRIM(opponent) != '' "
            f"ORDER BY week"
        ).fetchall()
    except Exception:
        return []

    weeks = [int(r[0]) for r in rows]
    if not weeks:
        return []

    if prt != 1:
        return [(w, w) for w in weeks]

    grouped: list[tuple[int, int]] = []
    i = 0
    while i < len(weeks):
        if i + 1 < len(weeks) and weeks[i + 1] == weeks[i] + 1:
            grouped.append((weeks[i], weeks[i + 1]))
            i += 2
        else:
            grouped.append((weeks[i], weeks[i]))
            i += 1
    return grouped


def _label_consolation_rounds(
    conn,
    table,
    year,
    id_col,
    opp_col,
    rounds,
    consolation_rounds,
    settings,
    uses_median,
    db_filter="1=1",
):
    """Label consolation_round for each consolation row.

    Logic:
      - Identify which championship round each team was eliminated in
      - Pair eliminated teams for placement games
      - Recognize placement-game patterns and label them

    `rounds` is the championship bracket structure — used to detect which
    championship round each team was eliminated in and to compute placement
    ranks (a team eliminated in the championship semifinal plays for 3rd).

    `consolation_rounds` is the consolation bracket structure (derived from
    actual data) — used as the iteration backbone for labeling so brackets
    that extend past the championship still get every week labeled.
    """
    playoff_teams = int(settings["playoff_teams"])
    playoff_start = int(settings["playoff_start_week"])
    num_teams = int(settings["num_teams"])
    num_champ_rounds = len(rounds)
    num_consolation_rounds = len(consolation_rounds)

    # Get champ bracket teams (those with is_playoffs=1)
    champ_teams = set()
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {id_col} FROM {table} "
            f"WHERE year = {year} AND {db_filter} AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1 "
            f"AND {id_col} IS NOT NULL"
        ).fetchall()
        champ_teams = {str(r[0]) for r in rows}
    except Exception:
        pass

    # Get elimination round for each champ bracket team
    # A team is eliminated in round R if they have is_playoffs=1 rows in round R
    # but NOT in round R+1 (and they're not the champion)
    champion_id = None
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {id_col} FROM {table} "
            f"WHERE year = {year} AND {db_filter} AND COALESCE(CAST(champion AS INTEGER), 0) = 1 "
            f"AND {id_col} IS NOT NULL"
        ).fetchall()
        if rows:
            champion_id = str(rows[0][0])
    except Exception:
        pass

    elimination_round = {}
    for team in champ_teams:
        if team == champion_id:
            continue
        last_playoff_round = 0
        for round_idx, (wk_start, wk_end) in enumerate(rounds, 1):
            try:
                cnt = conn.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE year = {year} AND {db_filter} "
                    f"AND week >= {wk_start} AND week <= {wk_end} "
                    f"AND {id_col} = '{_esc(team)}' "
                    f"AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1"
                ).fetchall()[0][0]
                if cnt > 0:
                    last_playoff_round = round_idx
            except Exception:
                pass
        if last_playoff_round > 0:
            elimination_round[team] = last_playoff_round

    # Label consolation rounds based on position in postseason
    # For each consolation week, determine the label
    _label_map = {0: "championship", 1: "semifinal", 2: "quarterfinal"}

    for round_idx, (wk_start, wk_end) in enumerate(consolation_rounds, 1):
        offset_from_end = num_consolation_rounds - round_idx

        # Determine consolation label for this round window
        if offset_from_end == 0:
            base_label = "consolation_final"
        elif offset_from_end == 1:
            base_label = "consolation_semifinal"
        else:
            base_label = f"consolation_round_{round_idx}"

        for wk in range(wk_start, wk_end + 1):
            # Get all consolation pairs in this week
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT {id_col}, {opp_col} FROM {table} "
                    f"WHERE year = {year} AND week = {wk} AND {db_filter} "
                    f"AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1 "
                    f"AND {id_col} IS NOT NULL AND {opp_col} IS NOT NULL"
                ).fetchall()
            except Exception:
                rows = []

            for row in rows:
                team_a, team_b = str(row[0]), str(row[1])

                # Check if both participants were eliminated in the same champ round
                elim_a = elimination_round.get(team_a)
                elim_b = elimination_round.get(team_b)

                label = base_label
                is_placement = 0

                if elim_a is not None and elim_b is not None and elim_a == elim_b:
                    # Both eliminated in same round -> placement game.
                    # Placement rank uses CHAMPIONSHIP bracket depth, not
                    # consolation depth — a team eliminated in the championship
                    # semifinal plays for 3rd regardless of how deep the
                    # consolation bracket runs.
                    elim_offset = num_champ_rounds - elim_a
                    if elim_offset == 1:
                        rank = 3
                    elif elim_offset == 2:
                        rank = 5
                    else:
                        rank = 2 ** (num_champ_rounds - elim_a) + 1

                    label = f"{_ordinal(rank).lower()}_place_game"
                    is_placement = 1

                conn.execute(
                    f"UPDATE {table} SET "
                    f"consolation_round = '{_esc(label)}', "
                    f"placement_game = {is_placement} "
                    f"WHERE year = {year} AND week = {wk} AND {db_filter} "
                    f"AND {id_col} = '{_esc(team_a)}' "
                    f"AND {opp_col} = '{_esc(team_b)}'"
                )


def _compute_placement_ranks(
    conn,
    table,
    year,
    settings,
    id_col,
    opp_col,
    rounds,
    consolation_rounds,
    uses_median,
    db_filter="1=1",
) -> dict[str, int]:
    """Compute placement_rank for every rostered franchise in the year.

    `rounds` is the championship bracket structure (used for elimination
    detection in Pass B). `consolation_rounds` is the consolation bracket
    structure (used in Pass A for placement-game-driven ranks and in the
    duplicate-rank tiebreaker so post-championship h2h games count).

    Returns {franchise_id: placement_rank} mapping.
    """
    num_teams = int(settings["num_teams"])
    playoff_teams = int(settings["playoff_teams"])
    playoff_start = int(settings["playoff_start_week"])
    num_rounds = len(rounds)

    # Step 6a: Preconditions
    # Get champion
    champion_id = None
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {id_col} FROM {table} "
            f"WHERE year = {year} AND {db_filter} AND COALESCE(CAST(champion AS INTEGER), 0) = 1 "
            f"AND {id_col} IS NOT NULL"
        ).fetchall()
        if len(rows) == 1:
            champion_id = str(rows[0][0])
        elif len(rows) > 1:
            logger.warning(f"[consolation] {year}: Multiple champions found, using first")
            champion_id = str(rows[0][0])
    except Exception:
        pass

    if not champion_id:
        logger.warning(f"[consolation] {year}: No champion found, skipping placement ranks")
        return {}

    # Get runner-up (other team in championship matchup)
    runner_up = None
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {id_col} FROM {table} "
            f"WHERE year = {year} AND {db_filter} "
            f"AND COALESCE(CAST(is_championship AS INTEGER), 0) = 1 "
            f"AND {id_col} IS NOT NULL "
            f"AND {id_col} != '{_esc(champion_id)}'"
        ).fetchall()
        if rows:
            runner_up = str(rows[0][0])
    except Exception:
        pass

    if not runner_up:
        # Fallback: try to find opponent of champion in championship week
        try:
            rows = conn.execute(
                f"SELECT {opp_col} FROM {table} "
                f"WHERE year = {year} AND {db_filter} "
                f"AND COALESCE(CAST(champion AS INTEGER), 0) = 1 "
                f"AND {id_col} = '{_esc(champion_id)}' "
                f"AND {opp_col} IS NOT NULL "
                f"LIMIT 1"
            ).fetchall()
            if rows:
                runner_up = str(rows[0][0])
        except Exception:
            pass

    placements: dict[str, int] = {}
    placements[champion_id] = 1
    if runner_up:
        placements[runner_up] = 2

    # Step 6b: Pass A -- placement-game-driven ranks (round-aware)
    #
    # Iterates the CONSOLATION round structure (not championship) so post-
    # championship placement games are visited. For multi-week rounds (prt=1),
    # this correctly:
    #   - Collapses 2-week matchups into one pair (dedup per round, not week)
    #   - Sums scores across weeks for winner determination
    #   - Distinguishes winners' finals from losers' finals when both share
    #     the same consolation_round label (e.g. "5th_place_game")
    for _round_idx, (wk_start, wk_end) in enumerate(consolation_rounds):
        try:
            round_rows = conn.execute(
                f"SELECT {id_col}, {opp_col}, consolation_round "
                f"FROM {table} "
                f"WHERE year = {year} AND {db_filter} "
                f"AND week >= {wk_start} AND week <= {wk_end} "
                f"AND COALESCE(placement_game, 0) = 1 "
                f"AND consolation_round IS NOT NULL "
                f"AND {id_col} IS NOT NULL AND {opp_col} IS NOT NULL"
            ).fetchall()
        except Exception:
            round_rows = []

        # Deduplicate to unique matchup pairs per label within this round
        seen = set()
        pairs_by_label: dict[str, list[tuple[str, str]]] = {}
        for row in round_rows:
            a, b, label = str(row[0]), str(row[1]), str(row[2])
            pair_key = (tuple(sorted((a, b))), label)
            if pair_key in seen:
                continue
            seen.add(pair_key)
            pairs_by_label.setdefault(label, []).append((a, b))

        for label, pairs in pairs_by_label.items():
            rank = _rank_from_label(label)
            if rank is None:
                continue

            for team_a, team_b in pairs:
                winner, loser = _consolation_round_winner(
                    conn,
                    table,
                    year,
                    id_col,
                    opp_col,
                    team_a,
                    team_b,
                    wk_start,
                    wk_end,
                    db_filter,
                )

                w_placed = winner in placements
                l_placed = loser in placements

                if w_placed and l_placed:
                    # Both already placed from a prior round — finals game.
                    # If both have the LOSER rank from semis (rank+1), this
                    # is the losers' final (for rank+2 / rank+3).
                    prior_w = placements[winner]
                    prior_l = placements[loser]
                    if prior_w == rank + 1 and prior_l == rank + 1:
                        placements[winner] = rank + 2
                        placements[loser] = rank + 3
                    else:
                        placements[winner] = rank
                        placements[loser] = rank + 1
                else:
                    placements[winner] = rank
                    placements[loser] = rank + 1

    # Step 6b.2: Resolve duplicate ranks from cross-bracket finals
    #
    # After Pass A, multiple teams may share the same rank if the consolation
    # bracket re-seeds (e.g. Sleeper cross-bracket finals: semi winner vs
    # semi loser from different matchups). Resolve iteratively: breaking one
    # tie can cascade (pushing a team down creates a new tie at the next rank).
    from collections import Counter

    for _pass in range(num_teams):  # bounded iterations
        rank_counts = Counter(placements.values())
        dups = {r: c for r, c in rank_counts.items() if c > 1 and r > 2}
        if not dups:
            break
        for dup_rank in sorted(dups):
            tied_teams = [t for t, r in placements.items() if r == dup_rank]
            resolved = False
            for _ri, (wk_start, wk_end) in reversed(list(enumerate(consolation_rounds))):
                try:
                    h2h_rows = conn.execute(
                        f"SELECT DISTINCT {id_col}, {opp_col} "
                        f"FROM {table} "
                        f"WHERE year = {year} AND {db_filter} "
                        f"AND week >= {wk_start} AND week <= {wk_end} "
                        f"AND {id_col} IN ({','.join(repr(_esc(t)) for t in tied_teams)}) "
                        f"AND {opp_col} IN ({','.join(repr(_esc(t)) for t in tied_teams)})"
                    ).fetchall()
                except Exception:
                    h2h_rows = []

                h2h_pairs = set()
                for row in h2h_rows:
                    a, b = str(row[0]), str(row[1])
                    pair = tuple(sorted((a, b)))
                    if pair in h2h_pairs:
                        continue
                    h2h_pairs.add(pair)
                    winner, loser = _consolation_round_winner(
                        conn,
                        table,
                        year,
                        id_col,
                        opp_col,
                        a,
                        b,
                        wk_start,
                        wk_end,
                        db_filter,
                    )
                    placements[loser] = dup_rank + 1

                if h2h_pairs:
                    resolved = True
                    break
            if not resolved:
                # No h2h game found — break tie by seed (higher seed = better rank)
                tied_with_seeds = sorted(
                    tied_teams,
                    key=lambda t: _get_seed(conn, table, year, id_col, t, db_filter),
                )
                for offset, team in enumerate(tied_with_seeds):
                    placements[team] = dup_rank + offset
                resolved = True
            if resolved:
                break  # Restart outer loop to re-check all ranks

    # Step 6c: Pass B -- tier-stratified fill for remaining
    # Get all rostered franchises for this year
    all_franchises = set()
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {id_col} FROM {table} "
            f"WHERE year = {year} AND {db_filter} AND {id_col} IS NOT NULL "
            f"AND team_points IS NOT NULL"
        ).fetchall()
        all_franchises = {str(r[0]) for r in rows}
    except Exception:
        pass

    unassigned = all_franchises - set(placements.keys())

    if unassigned:
        # Get elimination round for each champ bracket team
        elimination_round = {}
        champ_bracket_teams = set()
        try:
            rows = conn.execute(
                f"SELECT DISTINCT {id_col} FROM {table} "
                f"WHERE year = {year} AND {db_filter} AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1 "
                f"AND {id_col} IS NOT NULL"
            ).fetchall()
            champ_bracket_teams = {str(r[0]) for r in rows}
        except Exception:
            pass

        for team in champ_bracket_teams:
            if team == champion_id:
                continue
            last_round = 0
            for round_idx, (wk_start, wk_end) in enumerate(rounds, 1):
                try:
                    cnt = conn.execute(
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE year = {year} AND {db_filter} "
                        f"AND week >= {wk_start} AND week <= {wk_end} "
                        f"AND {id_col} = '{_esc(team)}' "
                        f"AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1"
                    ).fetchall()[0][0]
                    if cnt > 0:
                        last_round = round_idx
                except Exception:
                    pass
            if last_round > 0:
                elimination_round[team] = last_round

        # Tier 1: eliminated playoff teams (have is_playoffs=1 loss row)
        tier1 = []
        for team in unassigned:
            if team in elimination_round:
                seed = _get_seed(conn, table, year, id_col, team, db_filter)
                tier1.append((team, elimination_round[team], seed))

        # Sort by elimination round DESC (later = better), then seed ASC
        tier1.sort(key=lambda x: (-x[1], x[2]))

        # Tier 2: non-playoff teams, sorted by seed ASC
        tier2 = []
        for team in unassigned:
            if team not in elimination_round:
                seed = _get_seed(conn, table, year, id_col, team, db_filter)
                tier2.append((team, seed))
        tier2.sort(key=lambda x: x[1])

        # Walk unassigned ranks lowest-to-highest
        assigned_ranks = set(placements.values())
        next_rank = 1
        for team, _, _ in tier1:
            while next_rank in assigned_ranks:
                next_rank += 1
            if next_rank <= num_teams:
                placements[team] = next_rank
                assigned_ranks.add(next_rank)
                next_rank += 1

        for team, _ in tier2:
            while next_rank in assigned_ranks:
                next_rank += 1
            if next_rank <= num_teams:
                placements[team] = next_rank
                assigned_ranks.add(next_rank)
                next_rank += 1

    # Step 7: Invariant check
    actual_teams = len(all_franchises)
    expected = num_teams
    # Use actual team count if it differs from settings (some leagues have
    # fewer active teams than settings say)
    if actual_teams < expected:
        expected = actual_teams

    assigned_ranks = sorted(placements.values())
    if len(assigned_ranks) != expected:
        logger.warning(f"[consolation] {year}: {len(assigned_ranks)} ranks assigned but expected {expected} teams")
    else:
        # Check for duplicates or gaps
        if assigned_ranks != list(range(1, expected + 1)):
            logger.warning(f"[consolation] {year}: Ranks not contiguous 1..{expected}: {assigned_ranks}")

    # Check exactly 1 franchise at rank num_teams (sacko)
    sacko_count = sum(1 for r in placements.values() if r == expected)
    if sacko_count != 1 and expected > 0:
        logger.warning(f"[consolation] {year}: Expected 1 franchise at rank {expected}, found {sacko_count}")

    return placements


def _get_seed(conn, table, year, id_col, team_id, db_filter="1=1") -> int:
    """Get final_playoff_seed for a team. Fallback to 999."""
    try:
        rows = conn.execute(
            f"SELECT final_playoff_seed FROM {table} "
            f"WHERE year = {year} AND {db_filter} AND {id_col} = '{_esc(team_id)}' "
            f"AND final_playoff_seed IS NOT NULL "
            f"LIMIT 1"
        ).fetchall()
        if rows:
            return int(rows[0][0])
    except Exception:
        pass
    return 999


def _write_placement_ranks(conn, table, year, id_col, placements: dict[str, int], db_filter: str = "1=1"):
    """Write placement_rank on EVERY matchup row for each franchise-year."""
    for team_id, rank in placements.items():
        conn.execute(
            f"UPDATE {table} SET placement_rank = {rank} WHERE year = {year} AND {db_filter} AND {id_col} = '{_esc(team_id)}'"
        )


def _align_placements_with_sacko(placements: dict[str, int], sacko_id: str) -> dict[str, int]:
    """Move the loser-path Sacko to the worst placement rank.

    ESPN-style consolation ladders can have multiple rows labeled
    ``consolation_final`` in the same week. Placement ranks are first inferred
    from the whole bracket shape, but the Sacko is the team that survives the
    DDL loser path. Keep the rank list contiguous by shifting only the teams
    between the Sacko's old rank and the worst rank.
    """
    if sacko_id not in placements:
        return placements

    worst_rank = max(placements.values()) if placements else 0
    sacko_rank = placements[sacko_id]
    if worst_rank <= 0 or sacko_rank == worst_rank:
        return placements

    adjusted: dict[str, int] = {}
    for team_id, rank in placements.items():
        if team_id == sacko_id:
            adjusted[team_id] = worst_rank
        elif sacko_rank < rank <= worst_rank:
            adjusted[team_id] = rank - 1
        else:
            adjusted[team_id] = rank
    return adjusted


def _round_key(label: object, week: int) -> str:
    text = str(label or "").strip().lower().replace(" ", "_").replace("-", "_")
    return text or f"week_{week}"


def _write_sacko_from_loser_path(conn, table, year, id_col, opp_col, db_filter: str = "1=1") -> str | None:
    """Set sacko=1 from the DDL consolation loser path.

    Sleeper's losers bracket advances the lower-scoring team toward last place.
    Once a team wins a non-placement consolation game, they have exited the
    Sacko path. Placement games such as 3rd/5th place are explicitly excluded
    so they cannot overwrite the bottom-bracket final.
    """

    try:
        rows = conn.execute(
            f"""
            SELECT week, {id_col}, {opp_col}, team_points, win, loss, consolation_round
            FROM {table}
            WHERE year = {year}
              AND {db_filter}
              AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1
              AND COALESCE(CAST(placement_game AS INTEGER), 0) = 0
              AND LOWER(REPLACE(COALESCE(consolation_round, ''), ' ', '_')) NOT LIKE '%place_game%'
              AND {id_col} IS NOT NULL
              AND {opp_col} IS NOT NULL
            ORDER BY week, {id_col}
            """
        ).fetchall()
    except Exception as exc:
        logger.warning(f"[consolation] {year}: failed reading loser-path rows for sacko: {exc}")
        return None

    if not rows:
        return None

    games: dict[tuple[str, tuple[str, str]], dict] = {}
    for week_raw, team_raw, opp_raw, points_raw, win_raw, loss_raw, label_raw in rows:
        try:
            week = int(week_raw)
        except (TypeError, ValueError):
            continue
        team = str(team_raw)
        opp = str(opp_raw)
        if not team or not opp or team == opp:
            continue
        pair = tuple(sorted((team, opp)))
        key = (_round_key(label_raw, week), pair)
        game = games.setdefault(
            key,
            {
                "pair": pair,
                "weeks": set(),
                "scores": {},
                "wins": {},
                "losses": {},
            },
        )
        game["weeks"].add(week)
        try:
            points = float(points_raw) if points_raw is not None else None
        except (TypeError, ValueError):
            points = None
        if points is not None:
            game["scores"][(team, week)] = points
        try:
            if int(win_raw or 0) == 1:
                game["wins"][team] = game["wins"].get(team, 0) + 1
            if int(loss_raw or 0) == 1:
                game["losses"][team] = game["losses"].get(team, 0) + 1
        except (TypeError, ValueError):
            pass

    collapsed: list[dict] = []
    for game in games.values():
        team_a, team_b = game["pair"]
        scores = game["scores"]
        points_a_values = [score for (team, _week), score in scores.items() if team == team_a]
        points_b_values = [score for (team, _week), score in scores.items() if team == team_b]
        points_a = sum(points_a_values) if points_a_values else None
        points_b = sum(points_b_values) if points_b_values else None
        winner = loser = None
        if points_a is not None and points_b is not None:
            if points_a > points_b:
                winner, loser = team_a, team_b
            elif points_b > points_a:
                winner, loser = team_b, team_a

        if winner is None:
            wins_a = game["wins"].get(team_a, 0)
            wins_b = game["wins"].get(team_b, 0)
            if wins_a > wins_b:
                winner, loser = team_a, team_b
            elif wins_b > wins_a:
                winner, loser = team_b, team_a

        if winner and loser:
            collapsed.append(
                {
                    "team_a": team_a,
                    "team_b": team_b,
                    "winner": winner,
                    "loser": loser,
                    "weeks": tuple(sorted(game["weeks"])),
                }
            )

    if not collapsed:
        return None

    def sort_key(game: dict) -> tuple[int, int, str, str]:
        weeks = game["weeks"]
        return (min(weeks), max(weeks), game["team_a"], game["team_b"])

    wins: dict[str, int] = {}
    alive_games: list[dict] = []
    for game in sorted(collapsed, key=sort_key):
        team_a = game["team_a"]
        team_b = game["team_b"]
        both_alive = wins.get(team_a, 0) == 0 and wins.get(team_b, 0) == 0
        if both_alive:
            alive_games.append(game)
        wins[game["winner"]] = wins.get(game["winner"], 0) + 1

    if not alive_games:
        return None

    latest_week = max(max(game["weeks"]) for game in alive_games)
    latest = [game for game in alive_games if max(game["weeks"]) == latest_week]
    if len(latest) != 1:
        logger.warning(
            f"[consolation] {year}: ambiguous loser-path sacko candidate in week {latest_week}: {len(latest)} games"
        )
        return None

    sacko_id = latest[0]["loser"]
    try:
        sacko_week_rows = conn.execute(
            f"""
            SELECT MAX(week)
            FROM {table}
            WHERE year = {year}
              AND {db_filter}
              AND {id_col} = '{_esc(sacko_id)}'
              AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1
              AND COALESCE(CAST(placement_game AS INTEGER), 0) = 0
            """
        ).fetchall()
        sacko_week = int(sacko_week_rows[0][0]) if sacko_week_rows and sacko_week_rows[0][0] is not None else None
        if sacko_week is None:
            return None
        conn.execute(
            f"""
            UPDATE {table}
            SET sacko = 1
            WHERE year = {year}
              AND {db_filter}
              AND {id_col} = '{_esc(sacko_id)}'
              AND week = {sacko_week}
            """
        )
    except Exception as exc:
        logger.warning(f"[consolation] {year}: failed writing loser-path sacko={sacko_id}: {exc}")
        return None

    return sacko_id


def _write_sacko(
    conn, table, year, id_col, placements: dict[str, int], num_teams: int, db_filter: str = "1=1"
) -> str | None:
    """Set sacko=1 on the franchise with the worst placement_rank.

    The sacko flag goes on their LAST postseason row (max week where
    is_consolation=1 or is_playoffs=1).
    """
    # Find franchise with worst rank
    worst_rank = max(placements.values()) if placements else 0
    sacko_ids = [tid for tid, rank in placements.items() if rank == worst_rank]

    if not sacko_ids:
        return None

    sacko_id = sacko_ids[0]

    # Find their last postseason row
    try:
        rows = conn.execute(
            f"SELECT MAX(week) FROM {table} "
            f"WHERE year = {year} AND {db_filter} AND {id_col} = '{_esc(sacko_id)}' "
            f"AND (COALESCE(CAST(is_consolation AS INTEGER), 0) = 1 "
            f"     OR COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1)"
        ).fetchall()
        if rows and rows[0][0] is not None:
            sacko_week = int(rows[0][0])
            conn.execute(
                f"UPDATE {table} SET sacko = 1 "
                f"WHERE year = {year} AND {db_filter} "
                f"AND {id_col} = '{_esc(sacko_id)}' "
                f"AND week = {sacko_week}"
            )
        else:
            logger.warning(f"[consolation] {year}: no postseason row found for sacko={sacko_id}; leaving unset")
            return None
    except Exception as exc:
        logger.warning(f"[consolation] Failed to write sacko for {sacko_id}: {exc}")

    return sacko_id


def _derive_postseason(conn, table, year, db_filter="1=1"):
    """Set postseason = 1 where is_playoffs=1 OR is_consolation=1."""
    conn.execute(
        f"UPDATE {table} SET postseason = "
        f"CASE WHEN COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1 "
        f"       OR COALESCE(CAST(is_consolation AS INTEGER), 0) = 1 "
        f"  THEN 1 ELSE 0 END "
        f"WHERE year = {year} AND {db_filter}"
    )
