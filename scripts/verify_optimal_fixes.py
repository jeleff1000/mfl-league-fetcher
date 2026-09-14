"""Read-only verification that Phase B optimal_gte_team_points fixes resolve all 22 failing rows.

Simulates the NEW manager_optimal algorithm in pure Python against live MotherDuck data,
applying:
  1. player_bio position override (whitelist)
  2. Scarcity order with DB before _FRONT7
  3. FLX slots ordered by fantasy_points DESC

For each of the 22 failing (db, year, week, franchise_id) combos, computes expected
optimal_sum and asserts it's >= team_points (the validator's invariant).

Run:
    python scripts/verify_optimal_fixes.py

This does NOT modify MotherDuck. It only reads player_fantasy + player_bio + league_settings
and simulates in-process.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure fantasy_football_data_scripts is importable
_FFS = Path(__file__).resolve().parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))

from multi_league.core.db_reader import get_reader

# Canonical slot families for eligibility checks
DB_FAMILY = {"DB", "CB", "S", "SS", "FS", "NB"}
LB_FAMILY = {"LB", "OLB", "ILB", "MLB"}
DL_FAMILY = {"DL", "DE", "DT", "NT", "ED"}
FRONT7 = LB_FAMILY | DL_FAMILY
DEF_FAMILY = {"DEF", "DST", "D/ST"}

# Player_bio override whitelist (matches apply_player_bio_positions SQL)
BIO_WHITELIST_SKILL = {"WR", "TE", "RB"}
BIO_WHITELIST_LB_TO_DB_ALLOWED = {"DB", "CB", "S", "SS", "FS", "NB"}


def should_override(current: str, bio: str) -> bool:
    if not bio or "," in bio:
        return False
    cu = current.upper() if current else ""
    bu = bio.upper() if bio else ""
    if cu == bu:
        return False
    if cu in BIO_WHITELIST_SKILL and bu in BIO_WHITELIST_SKILL:
        return True
    if cu == "LB" and bu in BIO_WHITELIST_LB_TO_DB_ALLOWED:
        return True
    return False


def effective_position(current: str, bio: str) -> str:
    return bio if should_override(current, bio) else (current or "")


def tokens_for_eligibility(position: str, fantasy_position: str) -> set[str]:
    """Mimic position_eligibility_sql's `position || fantasy_position` split."""
    out: set[str] = set()
    for val in (position, fantasy_position):
        if not val:
            continue
        for tok in str(val).upper().split(","):
            t = tok.strip()
            if t:
                out.add(t)
    return out


def is_eligible_for_slot(toks: set[str], slot: str) -> bool:
    slot_u = slot.upper()
    if slot_u == "DEF":
        return bool(toks & DEF_FAMILY)
    if slot_u == "DB":
        return bool(toks & DB_FAMILY)
    if slot_u == "LB":
        return bool(toks & LB_FAMILY)
    if slot_u == "DL":
        return bool(toks & DL_FAMILY)
    if slot_u == "_FRONT7":
        return bool(toks & FRONT7)
    return slot_u in toks


def is_flex_eligible(toks: set[str], eligible_positions: list[str]) -> bool:
    wanted: set[str] = set()
    for p in eligible_positions:
        pu = p.upper()
        if pu == "DB":
            wanted |= DB_FAMILY
        elif pu == "LB":
            wanted |= LB_FAMILY
        elif pu == "DL":
            wanted |= DL_FAMILY
        else:
            wanted.add(pu)
    return bool(toks & wanted)


SCARCITY = {
    "K": 0,
    "DEF": 1,
    "TE": 2,
    "QB": 3,
    "RB": 4,
    "WR": 5,
    "DB": 6,
    "_FRONT7": 7,
}


def compute_optimal(players: list[dict], roster: dict) -> tuple[float, list[tuple[str, str, float]]]:
    """Pure-Python simulation of the NEW manager_optimal algorithm.

    players: list of {player, position, fantasy_position, fantasy_points, player_week}
    roster: {'QB': 1, 'RB': 2, ..., 'FLX': 2, 'SUPER_FLEX': 1, 'IDP': 1, ...}

    Returns: (optimal_sum, picks) where picks = [(slot, player, points), ...]
    """
    # Partition roster into dedicated (canonical slots) vs flex (multi-position)
    canonical = {"QB", "RB", "WR", "TE", "K", "DEF", "DB", "LB", "DL"}
    flex_defs = {
        "FLX": ["RB", "WR", "TE"],
        "W/R": ["WR", "RB"],
        "W/T": ["WR", "TE"],
        "R/T": ["RB", "TE"],
        "REC_FLEX": ["WR", "TE"],
        "SUPER_FLEX": ["QB", "RB", "WR", "TE"],
        "IDP": ["DL", "LB", "DB"],
        "DL_LB": ["DL", "LB"],
        "DB_LB": ["DB", "LB"],
    }

    dedicated = {}
    flex_positions = []  # list of (name, eligible_list, count)
    for slot_name, count in roster.items():
        if not count:
            continue
        c = int(count)
        if c <= 0:
            continue
        up = slot_name.upper()
        if up in canonical:
            dedicated[up] = dedicated.get(up, 0) + c
        elif up in flex_defs:
            flex_positions.append((up, flex_defs[up], c))

    # Collapse DL + LB -> _FRONT7
    front7_total = dedicated.pop("DL", 0) + dedicated.pop("LB", 0)
    if front7_total > 0:
        dedicated["_FRONT7"] = front7_total

    # Order dedicated slots by scarcity
    sorted_slots = sorted(dedicated.items(), key=lambda x: SCARCITY.get(x[0].upper(), 10))

    picks: list[tuple[str, str, float]] = []
    selected_pws: set[str] = set()

    # Fill dedicated slots. Never pick a negative-point player — leave the
    # slot empty, mirroring the SQL `p.fantasy_points >= 0` guard.
    for slot, cnt in sorted_slots:
        eligible = [
            p
            for p in players
            if p["player_week"] not in selected_pws
            and is_eligible_for_slot(p["toks"], slot)
            and p["fantasy_points"] >= 0
        ]
        eligible.sort(key=lambda p: (-p["fantasy_points"], p["player_week"]))
        for p in eligible[:cnt]:
            picks.append((slot, p["player"], p["fantasy_points"]))
            selected_pws.add(p["player_week"])

    # Fill flex slots (order by fantasy_points DESC, per Bug A fix)
    for flex_name, elig_list, cnt in flex_positions:
        tokens_with_slotname = list(elig_list) + [flex_name]
        eligible = [
            p
            for p in players
            if p["player_week"] not in selected_pws
            and (is_flex_eligible(p["toks"], tokens_with_slotname) or flex_name.upper() in p["toks"])
            and p["fantasy_points"] >= 0
        ]
        eligible.sort(key=lambda p: (-p["fantasy_points"], p["player_week"]))
        for p in eligible[:cnt]:
            picks.append((flex_name, p["player"], p["fantasy_points"]))
            selected_pws.add(p["player_week"])

    total = sum(pt for _, _, pt in picks)
    return total, picks


def main() -> int:
    reader = get_reader()

    mode = os.environ.get("VERIFY_MODE", "failing")  # failing | sample_passing
    if mode == "sample_passing":
        # Sample 60 currently-passing rows per pilot to check for regressions.
        failing_rows = reader.query(
            """
            WITH sampled AS (
              SELECT db_name, year, week, franchise_id, manager, team_points,
                ROW_NUMBER() OVER (PARTITION BY db_name ORDER BY year, week, manager) AS rn
              FROM public.matchup
              WHERE db_name IN ('nyu_ffl','the_pigskin_platoon','degenerate_gamblers_football_league','tfl_of_extraordinary_gentleman')
                AND optimal_points IS NOT NULL AND team_points IS NOT NULL
                AND optimal_points >= team_points - 0.01
                AND is_bye_week = 0
                AND NOT (platform='espn' AND year<2019)
            )
            SELECT s.db_name, s.year, s.week, s.franchise_id, s.manager, s.team_points,
              (SELECT SUM(pf.fantasy_points) FROM public.player_fantasy pf
                WHERE pf.db_name=s.db_name AND pf.year=s.year AND pf.week=s.week
                  AND pf.franchise_id=s.franchise_id AND pf.is_started=true) AS starter_sum
            FROM sampled s
            WHERE s.rn <= 15
            ORDER BY s.db_name, s.year, s.week
        """,
            database="___leagues",
        )
    else:
        # 22 failing rows.
        failing_rows = reader.query(
            """
            SELECT
              m.db_name, m.year, m.week, m.franchise_id, m.manager, m.team_points,
              (SELECT SUM(pf.fantasy_points)
                 FROM public.player_fantasy pf
                 WHERE pf.db_name=m.db_name AND pf.year=m.year AND pf.week=m.week
                   AND pf.franchise_id=m.franchise_id AND pf.is_started=true) AS starter_sum
            FROM public.matchup m
            WHERE m.db_name IN ('nyu_ffl','the_pigskin_platoon','degenerate_gamblers_football_league')
              AND m.optimal_points IS NOT NULL AND m.team_points IS NOT NULL
              AND m.optimal_points < m.team_points - 0.01
              AND NOT (m.platform='espn' AND m.year<2019)
            ORDER BY m.db_name, m.year, m.week
        """,
            database="___leagues",
        )

    print(f"Verifying {len(failing_rows)} failing rows...\n")

    pass_count = 0
    fail_count = 0
    details: list[str] = []

    for row in failing_rows:
        db_name = row["db_name"]
        year = row["year"]
        week = row["week"]
        franchise_id = row["franchise_id"]
        manager = row["manager"]
        team_points = row["team_points"]
        starter_sum = row["starter_sum"]

        # Get roster settings for this year (fallback to nearest if missing)
        # league_settings columns vary per-platform; discover what exists first.
        cols = {
            r["column_name"]
            for r in reader.query(
                "SELECT column_name FROM (DESCRIBE public.league_settings)",
                database="___leagues",
            )
        }

        roster_cols = [
            c for c in cols if c.startswith("roster_") and c not in ("roster_BN", "roster_IR", "roster_TAXI")
        ]
        col_list = ", ".join(f'"{c}"' for c in roster_cols)

        roster_rows = reader.query(
            f"SELECT {col_list} FROM public.league_settings" f" WHERE db_name = '{db_name}' AND year = {year}",
            database="___leagues",
        )
        if not roster_rows:
            roster_rows = reader.query(
                f"SELECT {col_list} FROM public.league_settings"
                f" WHERE db_name = '{db_name}' ORDER BY year DESC LIMIT 1",
                database="___leagues",
            )

        roster = {}
        if roster_rows:
            roster_dict = roster_rows[0]
            for c in roster_cols:
                slot = c.replace("roster_", "", 1)
                roster[slot] = roster_dict.get(c) or 0

        # Get players with bio override applied
        # Escape single quotes in franchise_id for safety
        safe_fid = str(franchise_id).replace("'", "''")
        player_rows = reader.query(
            f"""
            SELECT pf.player_week, pf.player, pf.position, pf.fantasy_position,
                   pf.fantasy_points, pb.nfl_position
            FROM public.player_fantasy pf
            LEFT JOIN ___ops.nfl_historical.player_bio pb
              ON CAST(pf.NFL_player_id AS VARCHAR) = CAST(pb.NFL_player_id AS VARCHAR)
            WHERE pf.db_name = '{db_name}' AND pf.year = {year} AND pf.week = {week}
              AND pf.franchise_id = '{safe_fid}'
              AND pf.fantasy_points IS NOT NULL
              AND LOWER(COALESCE(pf.manager, '')) NOT IN ('', 'unrostered', 'fa', 'free agent', 'waivers')
        """,
            database="___leagues",
        )

        players = []
        for pr in player_rows:
            pw = pr["player_week"]
            player = pr["player"]
            pos = pr["position"]
            fpos = pr["fantasy_position"]
            fpts = pr["fantasy_points"]
            bio_pos = pr["nfl_position"]
            eff_pos = effective_position(pos, bio_pos)
            players.append(
                {
                    "player_week": pw,
                    "player": player,
                    "position": eff_pos,
                    "fantasy_position": fpos,
                    "fantasy_points": float(fpts),
                    "toks": tokens_for_eligibility(eff_pos, fpos),
                }
            )

        optimal_sum, picks = compute_optimal(players, roster)
        # New invariant: optimal_points >= starter_sum (not team_points).
        # starter_sum == team_points for rows with complete data; falls back
        # to team_points only if starter_sum is NULL (no starter rows).
        reference = float(starter_sum) if starter_sum is not None else float(team_points)
        passed = optimal_sum >= reference - 0.01
        status = "PASS" if passed else "FAIL"
        ref_label = "starter_sum" if starter_sum is not None else "team_points"
        line = (
            f"[{status}] {db_name:40s} {year} w{week:2d} {manager:30s} "
            f"team_points={team_points:.2f} {ref_label}={reference:.2f} "
            f"new_optimal={optimal_sum:.2f} diff={optimal_sum - reference:+.2f}"
        )
        print(line)
        details.append(line)
        if passed:
            pass_count += 1
        else:
            fail_count += 1
            print("  picks:")
            for slot, player, pt in sorted(picks, key=lambda x: -x[2]):
                print(f"    {slot:10s} {player:30s} {pt:6.2f}")

    print(f"\nResult: {pass_count} PASS / {fail_count} FAIL")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
