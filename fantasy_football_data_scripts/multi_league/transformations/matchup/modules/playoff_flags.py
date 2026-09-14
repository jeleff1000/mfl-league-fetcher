"""
Playoff Flags Module

Normalizes playoff and consolation flags with proper mutual exclusivity.

CRITICAL RULE: is_consolation=1 MUST imply is_playoffs=0 (mutually exclusive)
"""

from functools import wraps
from pathlib import Path
import sys
import math
import json
import pandas as pd
from typing import Any

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from core.data_normalization import normalize_numeric_columns, ensure_league_id


def _is_utf8_console() -> bool:
    """Check if console supports UTF-8 encoding."""
    enc = getattr(sys.stdout, "encoding", None)
    return enc and "utf" in enc.lower()


def safe_print(*args: Any, **kwargs: Any) -> None:
    """Print with ASCII fallback for non-UTF8 consoles."""
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        ascii_args = []
        for a in args:
            if isinstance(a, str):
                ascii_args.append(a.encode("ascii", errors="replace").decode("ascii"))
            else:
                ascii_args.append(a)
        print(*ascii_args, **kwargs)


def ensure_normalized(func):
    """Decorator to ensure data normalization"""

    @wraps(func)
    def wrapper(df: pd.DataFrame, *args, **kwargs):
        # Normalize input
        df = normalize_numeric_columns(df)

        # Run transformation
        result = func(df, *args, **kwargs)

        # Normalize output
        result = normalize_numeric_columns(result)

        # Ensure league_id present
        if "league_id" in df.columns:
            league_id = df["league_id"].iloc[0] if len(df) > 0 else None
            if league_id:
                result = ensure_league_id(result, league_id)

        return result

    return wrapper


@ensure_normalized
def normalize_playoff_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize is_playoffs and is_consolation flags.

    CRITICAL RULES:
    1. is_consolation=1 → is_playoffs=0 (mutually exclusive)
    2. Either can be 0, but both cannot be 1
    3. postseason = 1 if either playoffs or consolation

    Returns:
        DataFrame with normalized playoff flags
    """
    df = df.copy()

    # Ensure flags exist
    for col in ["is_playoffs", "is_consolation"]:
        if col not in df.columns:
            df[col] = 0

    # Convert to int, handling NaN and boolean dtypes from parquet/MotherDuck
    df["is_playoffs"] = df["is_playoffs"].astype(object).fillna(0).astype(int)
    df["is_consolation"] = df["is_consolation"].astype(object).fillna(0).astype(int)

    # CRITICAL: Enforce is_consolation=1 → is_playoffs=0
    df.loc[df["is_consolation"] == 1, "is_playoffs"] = 0

    # Create postseason flag
    df["postseason"] = ((df["is_playoffs"] == 1) | (df["is_consolation"] == 1)).astype(int)

    # Count violations (should be 0 after fix)
    violations = ((df["is_playoffs"] == 1) & (df["is_consolation"] == 1)).sum()
    if violations > 0:
        safe_print(f"  [WARN] Found {violations} mutual exclusivity violations (forcing consolation)")

    playoff_count = (df["is_playoffs"] == 1).sum()
    consolation_count = (df["is_consolation"] == 1).sum()
    postseason_count = (df["postseason"] == 1).sum()

    safe_print(f"  Playoffs: {playoff_count} games")
    safe_print(f"  Consolation: {consolation_count} games")
    safe_print(f"  Total postseason: {postseason_count} games")
    safe_print("  [OK] Verified rule: is_consolation=1 -> is_playoffs=0")

    return df


def _repair_bracket_pairings(df, year_mask, manager_seeds, num_playoff, bye_teams, playoff_start, year):
    """
    Repair missing bracket pairings for the first playoff round.

    Yahoo API sometimes returns consolation opponents for playoff-seeded teams
    instead of their correct bracket opponent (e.g., seed 4 plays a consolation
    team instead of seed 5). This detects and fixes mismatched pairings using
    standard bracket seeding rules.

    In fantasy football, scores are independent of opponent (based on roster),
    so we can safely re-pair teams using their actual scores from that week.

    Returns the DataFrame with corrected pairings (or unchanged if no repair needed).
    """
    first_bracket_week = playoff_start
    first_week_mask = year_mask & (df["week"] == first_bracket_week)

    if not first_week_mask.any():
        return df

    # Non-bye qualifying teams that should play in the first bracket round
    non_bye_qualifiers = {mgr: seed for mgr, seed in manager_seeds.items() if bye_teams < seed <= num_playoff}

    if len(non_bye_qualifiers) < 2:
        return df

    # Determine expected pairings based on standard bracket seeding
    # Formula: seed X plays seed (bye_teams + 1 + num_playoff - X)
    expected_pairs = []
    paired = set()
    for mgr, seed in sorted(non_bye_qualifiers.items(), key=lambda x: x[1]):
        if mgr in paired:
            continue
        opp_seed = bye_teams + 1 + num_playoff - seed
        opp_mgr = next((m for m, s in non_bye_qualifiers.items() if s == opp_seed), None)
        if opp_mgr and opp_mgr not in paired:
            expected_pairs.append((mgr, seed, opp_mgr, opp_seed))
            paired.add(mgr)
            paired.add(opp_mgr)

    if not expected_pairs:
        return df

    # Check which expected pairs are mismatched in the data
    first_week_df = df[first_week_mask].copy()

    # manager_seeds is franchise_id-keyed (see _detect_bracket_mismatch caller), so
    # every mgr_* / actual_opp_* variable in this function is a franchise_id. Mask on
    # franchise_id / opponent_franchise_id throughout; only resolve back to manager
    # display names when writing the opponent column or composite keys.
    fid_to_mgr = {
        fid: mgr for fid, mgr in zip(first_week_df["franchise_id"], first_week_df["manager"]) if pd.notna(fid)
    }

    # Build the re-pairing map: franchise_id -> new_opponent franchise_id
    new_opponents = {}

    for mgr_a, seed_a, mgr_b, seed_b in expected_pairs:
        a_games = first_week_df[first_week_df["franchise_id"] == mgr_a]
        b_games = first_week_df[first_week_df["franchise_id"] == mgr_b]

        if a_games.empty or b_games.empty:
            continue

        actual_opp_a = a_games.iloc[0].get("opponent_franchise_id")

        if actual_opp_a == mgr_b:
            continue  # Already correctly paired

        # Skip if either team has a bye/placeholder (None opponent or 0 points)
        # These indicate missing data — we can't repair without real scores
        a_pts = a_games.iloc[0].get("team_points", 0)
        b_pts = b_games.iloc[0].get("team_points", 0)
        if pd.isna(actual_opp_a) or str(actual_opp_a).strip().lower() in ("none", ""):
            continue
        actual_opp_b = b_games.iloc[0].get("opponent_franchise_id")
        if pd.isna(actual_opp_b) or str(actual_opp_b).strip().lower() in ("none", ""):
            continue
        if pd.isna(a_pts) or a_pts == 0 or pd.isna(b_pts) or b_pts == 0:
            safe_print(
                f"    [REPAIR] {year} W{playoff_start}: Seed {seed_a} ({fid_to_mgr.get(mgr_a, mgr_a)}) vs Seed {seed_b} ({fid_to_mgr.get(mgr_b, mgr_b)}) - SKIPPED (missing scores)"
            )
            continue

        # Verify displaced teams also have real game data before swapping
        displaced_a = first_week_df[first_week_df["franchise_id"] == actual_opp_a]
        displaced_b = first_week_df[first_week_df["franchise_id"] == actual_opp_b]
        if displaced_a.empty or displaced_b.empty:
            safe_print(
                f"    [REPAIR] {year} W{playoff_start}: Seed {seed_a} ({fid_to_mgr.get(mgr_a, mgr_a)}) vs Seed {seed_b} ({fid_to_mgr.get(mgr_b, mgr_b)}) - SKIPPED (displaced team missing)"
            )
            continue

        # Don't displace another playoff qualifier whose own pair couldn't be repaired
        # e.g., if seed 3's pair was skipped (opponent missing), don't displace seed 3
        # from their current game when repairing a different pair
        displaced_qualifiers = [
            t for t in [actual_opp_a, actual_opp_b] if t in non_bye_qualifiers and t not in new_opponents
        ]
        if displaced_qualifiers:
            names = ", ".join(f"{fid_to_mgr.get(t, t)} (seed {non_bye_qualifiers[t]})" for t in displaced_qualifiers)
            safe_print(
                f"    [REPAIR] {year} W{playoff_start}: Seed {seed_a} ({fid_to_mgr.get(mgr_a, mgr_a)}) vs Seed {seed_b} ({fid_to_mgr.get(mgr_b, mgr_b)}) - SKIPPED (would displace qualifier: {names})"
            )
            continue

        safe_print(
            f"    [REPAIR] {year} W{playoff_start}: Seed {seed_a} ({fid_to_mgr.get(mgr_a, mgr_a)}) should play Seed {seed_b} ({fid_to_mgr.get(mgr_b, mgr_b)})"
        )
        safe_print(
            f"             Actual: {fid_to_mgr.get(mgr_a, mgr_a)} vs {fid_to_mgr.get(actual_opp_a, actual_opp_a)}, "
            f"{fid_to_mgr.get(mgr_b, mgr_b)} vs {fid_to_mgr.get(actual_opp_b, actual_opp_b)}"
        )

        # Schedule the bracket correction
        new_opponents[mgr_a] = mgr_b
        new_opponents[mgr_b] = mgr_a

        # Displaced teams play each other (if not already remapped by another pair)
        if actual_opp_a not in new_opponents and actual_opp_b not in new_opponents:
            new_opponents[actual_opp_a] = actual_opp_b
            new_opponents[actual_opp_b] = actual_opp_a

    if not new_opponents:
        return df

    # Collect team data for all affected teams (keyed on franchise_id)
    team_data = {}
    for mgr in new_opponents:
        mgr_games = first_week_df[first_week_df["franchise_id"] == mgr]
        if not mgr_games.empty:
            row = mgr_games.iloc[0]
            team_data[mgr] = {
                "team_points": row.get("team_points", 0),
                "team_projected_points": row.get("team_projected_points"),
                "franchise_id": row.get("franchise_id"),
                "manager": row.get("manager"),
            }

    # Apply all swaps simultaneously
    for mgr, new_opp in new_opponents.items():
        mask = first_week_mask & (df["franchise_id"] == mgr)
        if not mask.any() or new_opp not in team_data:
            continue

        new_opp_data = team_data[new_opp]
        pts = team_data.get(mgr, {}).get("team_points", 0)
        new_opp_pts = new_opp_data["team_points"]

        # Core columns — write the opponent manager display name, not the franchise_id
        df.loc[mask, "opponent"] = new_opp_data.get("manager")
        df.loc[mask, "opponent_points"] = new_opp_pts

        # Recalculate win/loss/tie
        if pts > new_opp_pts:
            df.loc[mask, "win"] = 1
            df.loc[mask, "loss"] = 0
            df.loc[mask, "tie"] = 0
        elif pts < new_opp_pts:
            df.loc[mask, "win"] = 0
            df.loc[mask, "loss"] = 1
            df.loc[mask, "tie"] = 0
        else:
            df.loc[mask, "win"] = 0
            df.loc[mask, "loss"] = 0
            df.loc[mask, "tie"] = 1

        # Recalculate margin
        df.loc[mask, "margin"] = pts - new_opp_pts

        # Optional columns
        if "total_matchup_score" in df.columns:
            df.loc[mask, "total_matchup_score"] = pts + new_opp_pts

        if "opponent_projected_points" in df.columns and new_opp_data.get("team_projected_points") is not None:
            df.loc[mask, "opponent_projected_points"] = new_opp_data["team_projected_points"]

        if "opponent_franchise_id" in df.columns and new_opp_data.get("franchise_id") is not None:
            df.loc[mask, "opponent_franchise_id"] = new_opp_data["franchise_id"]

        # Recompute composite keys using manager display names (the historical format)
        year_val = int(df.loc[mask, "year"].iloc[0]) if mask.any() else None
        week_val = int(df.loc[mask, "week"].iloc[0]) if mask.any() else None
        mgr_name = team_data.get(mgr, {}).get("manager") or ""
        new_opp_name = new_opp_data.get("manager") or ""
        opp_clean = str(new_opp_name).replace(" ", "")

        if "cumulative_week" in df.columns and mask.any():
            cw = int(df.loc[mask, "cumulative_week"].iloc[0])
            if "opponent_week" in df.columns:
                df.loc[mask, "opponent_week"] = f"{opp_clean}{cw}"

        if "opponent_year" in df.columns and year_val is not None:
            df.loc[mask, "opponent_year"] = f"{opp_clean}{year_val}"

        if "matchup_key" in df.columns and year_val is not None and week_val is not None:
            team1, team2 = sorted([mgr_name, new_opp_name])
            new_key = f"{team1}__vs__{team2}__{year_val}__{week_val}"
            df.loc[mask, "matchup_key"] = new_key
            if "matchup_sort_key" in df.columns:
                df.loc[mask, "matchup_sort_key"] = 1 if mgr_name < new_opp_name else 2

    repair_count = len(new_opponents) // 2
    safe_print(f"    [REPAIR] Fixed {repair_count} bracket pairing(s) in {year} W{playoff_start}")
    safe_print("             Displaced teams re-paired for consolation")

    return df


def _repair_alive_pairings(df, year_mask, week, alive_teams, year):
    """
    General bracket repair for any playoff week.

    Yahoo API sometimes pairs alive (championship-bracket) teams against
    consolation opponents instead of each other. This detects misplaced
    alive teams and re-pairs them together.

    For example, in a semifinal week with 4 alive teams {A, B, C, D}:
    - Yahoo might show: A vs B (correct), C vs X (wrong), D vs Y (wrong)
    - This repairs to: A vs B, C vs D, X vs Y

    `alive_teams` contains franchise_ids (see playoff_flags caller), so masks use
    franchise_id / opponent_franchise_id throughout and only surface manager display
    names when writing the opponent column or composite keys.

    Returns the DataFrame with corrected pairings (or unchanged if no repair needed).
    """
    week_mask = year_mask & (df["week"] == week)
    if not week_mask.any():
        return df

    week_df = df[week_mask].copy()

    fid_to_mgr = {fid: mgr for fid, mgr in zip(week_df["franchise_id"], week_df["manager"]) if pd.notna(fid)}

    # Find alive teams whose opponent is NOT alive (misplaced into consolation matchups)
    misplaced = []
    for mgr in alive_teams:
        mgr_rows = week_df[week_df["franchise_id"] == mgr]
        if mgr_rows.empty:
            continue
        opp = mgr_rows.iloc[0].get("opponent_franchise_id")
        if pd.isna(opp) or str(opp).strip().lower() in ("none", ""):
            continue
        if opp not in alive_teams:
            pts = mgr_rows.iloc[0].get("team_points", 0)
            if pd.notna(pts) and pts > 0:
                misplaced.append(mgr)

    # Need at least 2 misplaced alive teams to re-pair, and must be even count
    if len(misplaced) < 2 or len(misplaced) % 2 != 0:
        return df

    # Build re-pairing map: pair misplaced alive teams together
    # Use seed-based pairing if available, otherwise pair by order (highest vs lowest seed)
    new_opponents = {}

    # Collect the consolation opponents being displaced (keyed on franchise_id)
    displaced = {}
    for mgr in misplaced:
        mgr_rows = week_df[week_df["franchise_id"] == mgr]
        displaced[mgr] = mgr_rows.iloc[0].get("opponent_franchise_id")

    # Pair misplaced alive teams together (pair them in order: first with last, etc.)
    # This preserves bracket structure where higher seed plays lower seed
    sorted_misplaced = sorted(misplaced)
    for i in range(len(sorted_misplaced) // 2):
        mgr_a = sorted_misplaced[i]
        mgr_b = sorted_misplaced[-(i + 1)]

        new_opponents[mgr_a] = mgr_b
        new_opponents[mgr_b] = mgr_a

    # Pair displaced consolation teams together
    displaced_teams = [displaced[mgr] for mgr in misplaced]
    for i in range(len(displaced_teams) // 2):
        team_a = displaced_teams[i]
        team_b = displaced_teams[-(i + 1)]
        if team_a not in new_opponents and team_b not in new_opponents:
            new_opponents[team_a] = team_b
            new_opponents[team_b] = team_a

    if not new_opponents:
        return df

    safe_print(f"    [REPAIR] {year} W{week}: Re-pairing {len(misplaced)} alive teams scattered into consolation")
    for mgr in misplaced:
        mgr_name = fid_to_mgr.get(mgr, mgr)
        old_name = fid_to_mgr.get(displaced[mgr], displaced[mgr])
        new_name = fid_to_mgr.get(new_opponents.get(mgr), new_opponents.get(mgr, "?"))
        safe_print(f"             {mgr_name}: {old_name} -> {new_name}")

    # Collect team data for all affected teams (keyed on franchise_id)
    team_data = {}
    for mgr in new_opponents:
        mgr_games = week_df[week_df["franchise_id"] == mgr]
        if not mgr_games.empty:
            row = mgr_games.iloc[0]
            team_data[mgr] = {
                "team_points": row.get("team_points", 0),
                "team_projected_points": row.get("team_projected_points"),
                "franchise_id": row.get("franchise_id"),
                "manager": row.get("manager"),
            }

    # Apply all swaps simultaneously (same logic as _repair_bracket_pairings)
    for mgr, new_opp in new_opponents.items():
        mask = week_mask & (df["franchise_id"] == mgr)
        if not mask.any() or new_opp not in team_data:
            continue

        new_opp_data = team_data[new_opp]
        pts = team_data.get(mgr, {}).get("team_points", 0)
        new_opp_pts = new_opp_data["team_points"]

        # Core columns — write the opponent manager display name, not the franchise_id
        df.loc[mask, "opponent"] = new_opp_data.get("manager")
        df.loc[mask, "opponent_points"] = new_opp_pts

        # Recalculate win/loss/tie
        if pts > new_opp_pts:
            df.loc[mask, "win"] = 1
            df.loc[mask, "loss"] = 0
            df.loc[mask, "tie"] = 0
        elif pts < new_opp_pts:
            df.loc[mask, "win"] = 0
            df.loc[mask, "loss"] = 1
            df.loc[mask, "tie"] = 0
        else:
            df.loc[mask, "win"] = 0
            df.loc[mask, "loss"] = 0
            df.loc[mask, "tie"] = 1

        # Recalculate margin
        df.loc[mask, "margin"] = pts - new_opp_pts

        # Optional columns
        if "total_matchup_score" in df.columns:
            df.loc[mask, "total_matchup_score"] = pts + new_opp_pts

        if "opponent_projected_points" in df.columns and new_opp_data.get("team_projected_points") is not None:
            df.loc[mask, "opponent_projected_points"] = new_opp_data["team_projected_points"]

        if "opponent_franchise_id" in df.columns and new_opp_data.get("franchise_id") is not None:
            df.loc[mask, "opponent_franchise_id"] = new_opp_data["franchise_id"]

        # Recompute composite keys using manager display names (the historical format)
        year_val = int(df.loc[mask, "year"].iloc[0]) if mask.any() else None
        week_val = int(df.loc[mask, "week"].iloc[0]) if mask.any() else None
        mgr_name = team_data.get(mgr, {}).get("manager") or ""
        new_opp_name = new_opp_data.get("manager") or ""
        opp_clean = str(new_opp_name).replace(" ", "")

        if "cumulative_week" in df.columns and mask.any():
            cw = int(df.loc[mask, "cumulative_week"].iloc[0])
            if "opponent_week" in df.columns:
                df.loc[mask, "opponent_week"] = f"{opp_clean}{cw}"

        if "opponent_year" in df.columns and year_val is not None:
            df.loc[mask, "opponent_year"] = f"{opp_clean}{year_val}"

        if "matchup_key" in df.columns and year_val is not None and week_val is not None:
            team1, team2 = sorted([mgr_name, new_opp_name])
            new_key = f"{team1}__vs__{team2}__{year_val}__{week_val}"
            df.loc[mask, "matchup_key"] = new_key
            if "matchup_sort_key" in df.columns:
                df.loc[mask, "matchup_sort_key"] = 1 if mgr_name < new_opp_name else 2

    return df


@ensure_normalized
def detect_playoffs_by_seed(df: pd.DataFrame, settings_dir: str = None) -> pd.DataFrame:
    """
    Correctly detect is_playoffs and is_consolation based on league settings and game outcomes.

    CRITICAL LOGIC:
    - is_playoffs=1: ONLY teams still competing for 1st place (championship)
    - is_consolation=1: ALL other postseason games (including 3rd place, 5th place, etc.)

    Once a team loses in the championship bracket, all subsequent games are consolation games.

    Uses final_playoff_seed (end of regular season standings) to determine which teams
    qualified for the championship bracket vs consolation bracket.

    IMPORTANT: For Sleeper leagues, the fetcher already sets these flags correctly using
    the winners_bracket API. We detect Sleeper data by checking if is_playoffs/is_consolation
    are already meaningfully set (not all zeros during playoff weeks), and preserve those values.

    Args:
        df: DataFrame with matchup data
        settings_dir: Directory containing league_settings JSON files (optional, auto-detected if None)

    Returns:
        DataFrame with corrected is_playoffs and is_consolation flags
    """
    df = df.copy()

    required_cols = ["week", "year", "manager"]
    for col in required_cols:
        if col not in df.columns:
            safe_print(f"  [WARN] {col} column not found, skipping playoff detection")
            if "is_playoffs" not in df.columns:
                df["is_playoffs"] = 0
            if "is_consolation" not in df.columns:
                df["is_consolation"] = 0
            return df

    # SLEEPER DATA DETECTION: Check if playoff flags are already set by fetcher.
    # The Sleeper fetcher uses the winners_bracket API which is authoritative and
    # handles commissioner overrides (e.g., custom seedings based on house rules).
    # We preserve fetcher flags UNLESS a known bug is detected (bye team misclassification).
    #
    # BYE TEAM SANITY CHECK: If top seeds (seed <= bye_teams) have 0 playoff games
    # but DO have consolation games, the fetcher likely has the bye team bug where
    # teams with first-round byes are misclassified as consolation. In that case,
    # we fall through to seed-based recalculation which handles byes correctly.
    if "is_playoffs" in df.columns and "is_consolation" in df.columns:
        # Check for any postseason week (week >= 14 is a safe heuristic)
        postseason_weeks = df[df["week"] >= 14]
        if not postseason_weeks.empty:
            has_playoff_flags = (postseason_weeks["is_playoffs"] == 1).any()
            has_consolation_flags = (postseason_weeks["is_consolation"] == 1).any()

            if has_playoff_flags:
                # Playoff flags are authoritatively set — check for bye team bug before preserving
                # NOTE: We only enter preservation when is_playoffs=1 exists. If ONLY
                # is_consolation=1 exists (e.g., from external/staged data on Yahoo),
                # that's incomplete data — fall through to seed-based recalculation.
                bye_team_bug_detected = False

                if "final_playoff_seed" in df.columns:
                    for year in df["year"].dropna().unique():
                        year_int = int(year)
                        year_df = df[df["year"] == year]

                        # Load bye_teams count for this year
                        bye_teams_count = 0
                        if settings_dir:
                            data_dir_check = Path(settings_dir)
                            # Try JSON settings for this year
                            for pattern in [f"league_settings_{year_int}_*.json", f"league_settings_{year_int}.json"]:
                                settings_files = list(data_dir_check.glob(pattern))
                                if not settings_files and data_dir_check.parent.exists():
                                    settings_files = list(data_dir_check.parent.glob(pattern))
                                if settings_files:
                                    try:
                                        with open(settings_files[0], encoding="utf-8") as f:
                                            s = json.load(f)
                                            meta = s.get("metadata", s)
                                            bye_teams_count = int(meta.get("bye_teams", 0))
                                    except Exception:  # noqa: broad-except
                                        pass
                                    break

                        if bye_teams_count > 0:
                            # Check if any bye-eligible team (seed <= bye_teams) has
                            # consolation games but 0 playoff games
                            for seed in range(1, bye_teams_count + 1):
                                seed_df = year_df[year_df["final_playoff_seed"] == seed]
                                if seed_df.empty:
                                    continue
                                postseason_rows = seed_df[seed_df["week"] >= 14]
                                has_playoff = (postseason_rows["is_playoffs"] == 1).any()
                                has_consolation = (postseason_rows["is_consolation"] == 1).any()
                                if not has_playoff and has_consolation:
                                    mgr = seed_df["manager"].iloc[0]
                                    safe_print(
                                        f"  [BYE BUG] {year_int}: Seed {seed} ({mgr}) has 0 playoff games but consolation games — bye team bug detected"
                                    )
                                    bye_team_bug_detected = True

                if bye_team_bug_detected:
                    safe_print("  [RECALC] Bye team bug detected in fetcher flags — recalculating from seeds")
                else:
                    # Flags look correct — preserve them (handles commissioner overrides)
                    safe_print("  [PRESERVE] Playoff flags set by fetcher (Sleeper bracket API) — preserving")

                    df["is_playoffs"] = df["is_playoffs"].astype(object).fillna(0).astype(int)
                    df["is_consolation"] = df["is_consolation"].astype(object).fillna(0).astype(int)

                    # Create postseason flag
                    df["postseason"] = ((df["is_playoffs"] == 1) | (df["is_consolation"] == 1)).astype(int)

                    playoff_count = (df["is_playoffs"] == 1).sum()
                    consolation_count = (df["is_consolation"] == 1).sum()
                    safe_print(
                        f"  [PRESERVED] is_playoffs=1: {playoff_count} games, is_consolation=1: {consolation_count} games"
                    )

                    return df

            elif has_consolation_flags and not has_playoff_flags:
                consolation_only = (postseason_weeks["is_consolation"] == 1).sum()
                safe_print(
                    f"  [SKIP PRESERVE] Only is_consolation flags found ({consolation_only} games), no is_playoffs — recalculating from seeds"
                )

    # Ensure win/loss columns exist
    for col in ["win", "loss"]:
        if col not in df.columns:
            df[col] = 0

    df["win"] = df["win"].astype(object).fillna(0).astype(int)
    df["loss"] = df["loss"].astype(object).fillna(0).astype(int)

    # Load settings from local DuckDB or JSON files per year
    from core.data_normalization import find_league_settings_directory

    if settings_dir is None:
        # Use centralized utility to find settings directory (league-agnostic)
        settings_path = find_league_settings_directory(df=df)
        if settings_path:
            settings_dir = str(settings_path)
            safe_print(f"  [SETTINGS] Auto-discovered settings directory: {settings_path}")
        else:
            safe_print("  [WARN] Could not find league_settings directory, using defaults")
    else:
        # Use provided settings_dir (data_directory from LeagueContext)
        settings_path = find_league_settings_directory(data_directory=Path(settings_dir), df=df)
        if settings_path:
            settings_dir = str(settings_path)
            safe_print(f"  [SETTINGS] Using settings directory: {settings_path}")

    # Load settings per year - try local DuckDB first, then JSON
    settings_by_year = {}
    data_dir = Path(settings_dir) if settings_dir else None

    # Track settings sources for consolidated logging
    settings_sources = {"local_db": [], "json": [], "inferred": []}

    # Try loading from local DuckDB (league_settings table)
    if data_dir:
        settings_df = None
        duckdb_files = list(data_dir.glob("*.duckdb"))
        if duckdb_files:
            import duckdb as _ddb

            try:
                _conn = _ddb.connect(str(duckdb_files[0]), read_only=True)
                _exists = (
                    _conn.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = 'league_settings'"
                    ).fetchone()[0]
                    > 0
                )
                if _exists:
                    settings_df = _conn.execute("SELECT * FROM public.league_settings").fetchdf()
                _conn.close()
            except Exception:
                pass

        if settings_df is not None and not settings_df.empty:
            try:
                for year in settings_df["year"].unique():
                    year_int = int(year)
                    row = settings_df[settings_df["year"] == year].iloc[0]

                    # Helper to safely get int setting with NULL handling
                    def _safe_int(val, default):
                        return int(val) if pd.notna(val) else default

                    # Get playoff settings first (needed for end_week calculation)
                    playoff_start = _safe_int(row.get("playoff_start_week"), 15)
                    num_playoff = _safe_int(row.get("num_playoff_teams"), _safe_int(row.get("playoff_teams"), 6))

                    # Prefer the flat end_week column and otherwise derive it
                    # from canonical bracket geometry.
                    end_week = None
                    if "end_week" in row.index and pd.notna(row.get("end_week")):
                        end_week = int(row.get("end_week"))

                    # Calculate end_week dynamically if not stored (instead of hardcoding 17)
                    if end_week is None:
                        playoff_rounds = math.ceil(math.log2(num_playoff)) if num_playoff > 1 else 1
                        end_week = playoff_start + playoff_rounds - 1

                    settings_by_year[year_int] = {
                        "playoff_start_week": playoff_start,
                        "num_playoff_teams": num_playoff,
                        "bye_teams": _safe_int(row.get("bye_teams"), 0),
                        "end_week": end_week,  # Last week of fantasy season
                    }
                    settings_sources["local_db"].append(year_int)
            except Exception as e:
                safe_print(f"  [WARN] Failed to load from local DB: {e}")

    # Try loading from JSON files for any years not in local DB
    if data_dir and data_dir.exists():
        years = sorted(df["year"].dropna().unique())
        for year in years:
            year_int = int(year)
            if year_int in settings_by_year:
                continue  # Already loaded from local DB

            # Try both naming conventions:
            # Yahoo: league_settings_YEAR_LEAGUEID.json
            # Sleeper: league_settings_YEAR.json
            settings_files = list(data_dir.glob(f"league_settings_{year_int}_*.json"))
            if not settings_files:
                settings_files = list(data_dir.glob(f"league_settings_{year_int}.json"))
            if settings_files:
                try:
                    with open(settings_files[0], encoding="utf-8") as f:
                        settings = json.load(f)
                        metadata = settings.get("metadata", settings)

                        # Get playoff settings first (needed for end_week calculation)
                        playoff_start = int(metadata.get("playoff_start_week", 15))
                        num_playoff = int(metadata.get("num_playoff_teams", metadata.get("playoff_teams", 6)))

                        # Calculate end_week dynamically if not stored (instead of hardcoding 17)
                        stored_end_week = metadata.get("end_week")
                        if stored_end_week is not None:
                            end_week = int(stored_end_week)
                        else:
                            playoff_rounds = math.ceil(math.log2(num_playoff)) if num_playoff > 1 else 1
                            end_week = playoff_start + playoff_rounds - 1

                        settings_by_year[year_int] = {
                            "playoff_start_week": playoff_start,
                            "num_playoff_teams": num_playoff,
                            "bye_teams": int(metadata.get("bye_teams", 0)),
                            "end_week": end_week,  # Last week of fantasy season
                        }
                        settings_sources["json"].append(year_int)
                except Exception as e:
                    safe_print(f"  [WARN] Failed to load JSON settings for {year_int}: {e}")

    # For years with no settings, try to INFER playoffs from data structure
    # Playoffs are detected by: reduced games per week (fewer than regular season)
    for year in df["year"].dropna().unique():
        year_int = int(year)
        if year_int in settings_by_year:
            continue

        year_df = df[df["year"] == year]
        weeks = sorted(year_df["week"].unique())

        if len(weeks) < 2:
            continue

        # Count games per week
        games_per_week = {}
        for week in weeks:
            week_games = len(year_df[year_df["week"] == week]) // 2
            games_per_week[week] = week_games

        # Find regular season game count (mode of first 10 weeks)
        early_weeks = [w for w in weeks if w <= 10]
        if early_weeks:
            regular_game_count = max(
                set([games_per_week[w] for w in early_weeks]), key=[games_per_week[w] for w in early_weeks].count
            )
        else:
            regular_game_count = max(games_per_week.values())

        # Detect playoff start: first week with fewer games than regular season
        playoff_start = None
        for week in weeks:
            if games_per_week[week] < regular_game_count:
                playoff_start = week
                break

        if playoff_start:
            # Infer num_playoff_teams from first playoff week games
            first_playoff_games = games_per_week.get(playoff_start, 2)
            # First round games * 2 = teams playing, might have byes
            num_playoff_teams = first_playoff_games * 2
            # Add 2 if there are bye teams (common for 6-team with 2 byes)
            if len([w for w in weeks if w >= playoff_start]) >= 3:
                num_playoff_teams = max(num_playoff_teams, 4)

            # For inferred settings, end_week is the max week in the data
            inferred_end_week = int(max(weeks))
            settings_by_year[year_int] = {
                "playoff_start_week": int(playoff_start),
                "num_playoff_teams": num_playoff_teams,
                "bye_teams": 0,
                "end_week": inferred_end_week,  # Last week in data
            }
            settings_sources["inferred"].append(year_int)
        else:
            # No playoffs detected - use defaults but warn
            max_week = int(max(weeks))
            settings_by_year[year_int] = {
                "playoff_start_week": max_week + 1,  # Beyond last week = no playoffs
                "num_playoff_teams": 0,
                "bye_teams": 0,
                "end_week": max_week,  # Last week in data
            }
            safe_print(
                f"  [WARN] {year_int}: No playoff structure detected (all weeks have {regular_game_count} games)"
            )

    if not settings_by_year:
        safe_print("  [SETTINGS] No settings found, no years to process")
    else:
        # Log consolidated settings summary
        all_years = sorted(settings_by_year.keys())
        year_range = f"{min(all_years)}-{max(all_years)}" if len(all_years) > 1 else str(all_years[0])
        sources_summary = []
        if settings_sources["local_db"]:
            sources_summary.append(f"{len(settings_sources['local_db'])} from local DB")
        if settings_sources["json"]:
            sources_summary.append(f"{len(settings_sources['json'])} from json")
        if settings_sources["inferred"]:
            sources_summary.append(f"{len(settings_sources['inferred'])} inferred")
        safe_print(
            f"  [SETTINGS] Loaded settings for {len(all_years)} years ({year_range}): {', '.join(sources_summary)}"
        )

    # Initialize flags
    df["is_playoffs"] = 0
    df["is_consolation"] = 0

    # Process each year separately
    for year, year_settings in settings_by_year.items():
        year_mask = df["year"] == year

        if not year_mask.any():
            continue

        playoff_start_config = year_settings["playoff_start_week"]
        num_playoff = year_settings["num_playoff_teams"]
        bye_teams = year_settings["bye_teams"]

        # SETTINGS-DRIVEN PLAYOFF START:
        #
        # playoff_start_week from settings means: the first week ANY playoff games occur
        #
        # Example (6-team playoffs, 2 byes):
        #   - playoff_start_week: 15
        #   - bye_teams: 2
        #
        # This means:
        #   - Week 15: Seeds 3-6 play (first playoff games)
        #   - Week 16: Seeds 1-2 enter and play winners (semifinals)
        #
        # NO ADJUSTMENT NEEDED - trust the settings file directly.

        playoff_start = playoff_start_config

        # Per-year settings already logged in consolidated summary above

        postseason_mask = year_mask & (df["week"] >= playoff_start)

        if not postseason_mask.any():
            continue

        # Determine playoff qualifiers based on final_playoff_seed
        if "final_playoff_seed" in df.columns:
            year_df = df[year_mask].copy()

            # Get playoff qualifiers (teams with seed <= num_playoff_teams)
            manager_seeds = {}
            for fid in year_df["franchise_id"].unique():
                fid_data = year_df[year_df["franchise_id"] == fid]
                seed_values = fid_data["final_playoff_seed"].dropna()
                if not seed_values.empty:
                    manager_seeds[fid] = int(seed_values.iloc[0])

            playoff_qualifiers = {fid for fid, seed in manager_seeds.items() if seed <= num_playoff}
            consolation_teams = {fid for fid, seed in manager_seeds.items() if seed > num_playoff}
            # Bracket info included in per-year playoff summary below
        else:
            # Fallback: use first playoff week to determine qualifiers
            safe_print(f"  [WARN] No final_playoff_seed found for year {year}, using fallback logic")
            first_week = int(df[postseason_mask]["week"].min())
            playoff_qualifiers = set(
                df[(df["year"] == year) & (df["week"] == first_week) & (df["is_playoffs"] == 1)][
                    "franchise_id"
                ].unique()
            )
            consolation_teams = set(df[postseason_mask]["franchise_id"].unique()) - playoff_qualifiers

        # Track which teams are STILL ALIVE for 1st place (can win championship)
        # Once a team loses, they can only play consolation games
        alive_for_championship = playoff_qualifiers.copy()
        initial_alive = len(alive_for_championship)

        # REPAIR: Fix missing bracket pairings before processing weeks
        # Yahoo API sometimes returns consolation opponents for playoff-seeded
        # teams instead of their correct bracket opponent in the first round.
        df = _repair_bracket_pairings(df, year_mask, manager_seeds, num_playoff, bye_teams, playoff_start, year)

        # Process each playoff week in order
        playoff_weeks = sorted(df[postseason_mask]["week"].unique())

        for week in playoff_weeks:
            week_mask = postseason_mask & (df["week"] == week)
            week_games = df[week_mask].copy()

            if week_games.empty:
                continue

            # REPAIR: Fix alive teams scattered into consolation matchups
            # Yahoo API sometimes pairs championship-bracket teams against
            # consolation opponents instead of each other (e.g., in semifinals
            # when the commissioner uses custom bracket ordering).
            df = _repair_alive_pairings(df, year_mask, week, alive_for_championship, year)

            # Mark championship bracket games (ONLY teams still alive for 1st place)
            # is_playoffs=1 means: "This team can still win the championship"
            championship_games_mask = (
                week_mask
                & df["franchise_id"].isin(alive_for_championship)
                & df["opponent_franchise_id"].isin(alive_for_championship)
            )
            df.loc[championship_games_mask, "is_playoffs"] = 1

            # Mark ALL other postseason games as consolation
            # This includes:
            # - Teams that never made playoffs (seeds > num_playoff)
            # - Teams that lost in championship bracket (playing for 3rd, 5th, etc.)
            # Exclude bye rows (no opponent / NaN points) — they aren't real games
            bye_row_mask = (
                df["opponent"].isna()
                | (df["opponent"].astype(str).str.strip() == "")
                | (df["opponent"].astype(str).str.lower() == "none")
            )
            non_championship_mask = week_mask & ~championship_games_mask & ~bye_row_mask
            df.loc[non_championship_mask, "is_consolation"] = 1

            # Refresh week_games after potential repair
            week_games = df[week_mask].copy()

            # Find teams that LOST in championship bracket this week
            # These teams are eliminated from 1st place contention
            championship_losers = week_games[
                (week_games["franchise_id"].isin(alive_for_championship))
                & (week_games["opponent_franchise_id"].isin(alive_for_championship))
                & (week_games["loss"] == 1)
            ]["franchise_id"].unique()

            # Remove losers from alive_for_championship for next week
            # They can no longer win 1st place, so all future games are consolation
            alive_for_championship = alive_for_championship - set(championship_losers)

        # Log summary instead of per-week
        if playoff_weeks:
            safe_print(
                f"    Playoff weeks {int(min(playoff_weeks))}-{int(max(playoff_weeks))}: {initial_alive} teams -> {len(alive_for_championship)} remaining"
            )

    # Create postseason flag
    df["postseason"] = ((df["is_playoffs"] == 1) | (df["is_consolation"] == 1)).astype(int)

    # Final verification - ensure mutual exclusivity
    violations = ((df["is_playoffs"] == 1) & (df["is_consolation"] == 1)).sum()
    if violations > 0:
        safe_print(f"  [ERROR] Found {violations} games with both flags set!")
        df.loc[(df["is_playoffs"] == 1) & (df["is_consolation"] == 1), "is_playoffs"] = 0

    playoff_count = (df["is_playoffs"] == 1).sum()
    consolation_count = (df["is_consolation"] == 1).sum()
    postseason_count = (df["postseason"] == 1).sum()

    safe_print(f"  [FINAL] is_playoffs=1 (competing for 1st): {playoff_count} games")
    safe_print(f"  [FINAL] is_consolation=1 (all other postseason): {consolation_count} games")
    safe_print(f"  [FINAL] Total postseason: {postseason_count}")
    safe_print("  [OK] Verified: is_playoffs and is_consolation are mutually exclusive")

    return df


@ensure_normalized
def mark_playoff_rounds(df: pd.DataFrame, data_directory: str = None) -> pd.DataFrame:
    """
    Mark playoff rounds (quarterfinal, semifinal, championship) and consolation rounds.

    FULLY GENERIC - works with any bracket configuration:
    - Dynamically detects placement games (3rd, 5th, 7th, 9th, etc.)
    - Handles winner-advances AND loser-advances brackets
    - No hardcoded assumptions about bracket structure

    Adds columns for BOTH championship and consolation brackets:
      - playoff_week_index (1, 2, 3, ...)
      - playoff_round_num (same as index)
      - playoff_round (string label: "quarterfinal", "semifinal", "championship", "third_place_game")
      - consolation_round (string label: "consolation_round_1", "consolation_final", "fifth_place_game")
      - quarterfinal, semifinal, championship (binary flags)
      - placement_game (binary flag for ANY placement game)
      - placement_rank (3, 5, 7, 9, etc. - which place this game determines)

    Args:
        df: DataFrame with matchup data
        data_directory: Path to league data directory (for finding league settings, currently unused)

    Returns:
        DataFrame with playoff round columns
    """
    df = df.copy()

    # Initialize round columns
    for col in [
        "playoff_week_index",
        "playoff_round_num",
        "playoff_round",
        "consolation_round",
        "quarterfinal",
        "semifinal",
        "championship",
        "placement_game",
        "placement_rank",
        "consolation_semifinal",
        "consolation_final",
    ]:
        if col not in df.columns:
            df[col] = 0 if col not in ["playoff_round", "consolation_round"] else ""

    # Ensure playoff flags exist
    if "is_playoffs" not in df.columns:
        df["is_playoffs"] = 0
    if "is_consolation" not in df.columns:
        df["is_consolation"] = 0

    df["is_playoffs"] = df["is_playoffs"].astype(object).fillna(0).astype(int)
    df["is_consolation"] = df["is_consolation"].astype(object).fillna(0).astype(int)

    # Enforce mutual exclusivity
    df.loc[df["is_consolation"] == 1, "is_playoffs"] = 0

    # CRITICAL: Clear all playoff round labels for regular season games
    # Regular season = is_playoffs=0 AND is_consolation=0
    regular_season_mask = (df["is_playoffs"] == 0) & (df["is_consolation"] == 0)
    df.loc[regular_season_mask, "playoff_round"] = ""
    df.loc[regular_season_mask, "consolation_round"] = ""
    df.loc[regular_season_mask, "playoff_week_index"] = 0
    df.loc[regular_season_mask, "playoff_round_num"] = 0
    df.loc[regular_season_mask, "quarterfinal"] = 0
    df.loc[regular_season_mask, "semifinal"] = 0
    df.loc[regular_season_mask, "championship"] = 0
    df.loc[regular_season_mask, "consolation_semifinal"] = 0
    df.loc[regular_season_mask, "consolation_final"] = 0
    df.loc[regular_season_mask, "placement_game"] = 0
    df.loc[regular_season_mask, "placement_rank"] = 0

    # ==========================================================================
    # LOAD SETTINGS FOR EXPECTED PLAYOFF ROUNDS
    # ==========================================================================
    # We need end_week from settings to know the EXPECTED championship week,
    # rather than inferring from actual data (which may be incomplete).
    # Uses the central utils.load_league_settings which tries local DB then JSON.
    # ==========================================================================
    # Import bracket_utils with fallbacks for different import contexts
    try:
        from .playoff_bracket import utils as bracket_utils
    except ImportError:
        try:
            from playoff_bracket import utils as bracket_utils
        except ImportError:
            from multi_league.transformations.matchup.modules.playoff_bracket import utils as bracket_utils

    settings_by_year = {}
    data_dir = Path(data_directory) if data_directory else None

    # Load settings for each year using the central loader
    years_in_data = sorted(df["year"].dropna().unique().astype(int))
    years_loaded = []

    for year_int in years_in_data:
        try:
            # Use the central settings loader which tries local DB, then JSON, then infers from data
            settings = bracket_utils.load_league_settings(
                year=year_int,
                settings_dir=str(data_dir) if data_dir else None,
                df=df,
                data_directory=str(data_dir) if data_dir else None,
            )
            settings_by_year[year_int] = {
                "playoff_start_week": settings.get("playoff_start_week", 15),
                "num_playoff_teams": settings.get("num_playoff_teams", 6),
                "end_week": settings.get("end_week", 17),
            }
            years_loaded.append(year_int)
        except Exception as e:
            safe_print(f"  [WARN] Failed to load settings for year {year_int}: {e}")

    if years_loaded:
        year_range = f"{min(years_loaded)}-{max(years_loaded)}" if len(years_loaded) > 1 else str(years_loaded[0])
        safe_print(
            f"  [SETTINGS] Loaded settings for {len(years_loaded)} years ({year_range}): {len([y for y in years_loaded if y in settings_by_year])} from json"
        )

    def _label_for_index(
        idx_from_start: int, total_rounds: int, actual_week: int, expected_championship_week: int
    ) -> str:
        """
        Map playoff week index to round label.

        CRITICAL FIX: Only label as "championship" if we're at the EXPECTED championship week,
        not just the last week with data. This prevents premature championship marking
        when playoffs are incomplete.
        """
        offset_from_end = total_rounds - idx_from_start

        # Guard: If we haven't reached the expected championship week yet,
        # don't mark this as championship even if it's the last week with data
        if actual_week < expected_championship_week and offset_from_end == 0:
            # This week would be championship based on data, but we haven't reached expected week
            # Label it based on what round it SHOULD be in the expected structure
            safe_print(
                f"      [GUARD] Week {actual_week} is last in data but expected championship is week {expected_championship_week}"
            )
            return "semifinal" if total_rounds >= 2 else f"round_{idx_from_start}"

        if offset_from_end == 0:
            return "championship"
        if offset_from_end == 1:
            return "semifinal"
        if offset_from_end == 2:
            return "quarterfinal"
        return f"round_{idx_from_start}"

    # Process each year
    years = sorted(df["year"].dropna().unique().astype(int))

    # Memoize championship week warnings to avoid spammy logs
    _championship_week_warnings_printed = set()

    for yr in years:
        # Get expected playoff structure from settings
        yr_settings = settings_by_year.get(yr, {})
        playoff_start = yr_settings.get("playoff_start_week", 15)
        num_playoff_teams = yr_settings.get("num_playoff_teams", yr_settings.get("playoff_teams", 6))
        end_week = yr_settings.get("end_week", 17)
        playoff_round_type = int(yr_settings.get("playoff_round_type", yr_settings.get("sleeper_playoff_type", 0)) or 0)
        if playoff_round_type == 0 and yr_settings.get("has_multiweek_championship"):
            playoff_round_type = 2

        normalized_settings = {
            "playoff_start_week": int(playoff_start),
            "num_playoff_teams": int(num_playoff_teams),
            "playoff_round_type": playoff_round_type,
            "end_week": int(end_week) if end_week is not None else None,
        }
        expected_rounds = int(bracket_utils.get_expected_playoff_rounds(normalized_settings))
        expected_championship_week = int(bracket_utils.get_expected_championship_week(normalized_settings))

        # ====== CHAMPIONSHIP BRACKET ======
        mask_y = (df["year"] == yr) & (df["is_playoffs"] == 1)
        actual_weeks = sorted(df.loc[mask_y, "week"].dropna().unique().astype(int))

        # CRITICAL: Check if bracket API already set championship=1 for any rows
        # If so, trust the bracket API over calculated expected_championship_week
        bracket_championship_mask = mask_y & (df["championship"] == 1)
        has_bracket_championship = bracket_championship_mask.any()
        bracket_championship_week = None
        if has_bracket_championship:
            bracket_championship_week = int(df.loc[bracket_championship_mask, "week"].iloc[0])
            if bracket_championship_week != expected_championship_week:
                # Memoize to avoid spammy logs when multiple years have same mismatch
                warn_key = (bracket_championship_week, expected_championship_week)
                if warn_key not in _championship_week_warnings_printed:
                    safe_print(
                        f"  [BRACKET] Year {yr}: Trusting bracket API championship week {bracket_championship_week} (settings said {expected_championship_week})"
                    )
                    _championship_week_warnings_printed.add(warn_key)
                expected_championship_week = bracket_championship_week

        if actual_weeks:
            # CRITICAL FIX: Use EXPECTED rounds for labeling, not actual data length
            # This prevents marking the last week with data as championship
            # when more playoff weeks are expected but not yet played
            total = expected_rounds  # Use expected, NOT len(actual_weeks)

            # Create week-to-index mapping based on playoff_start_week
            # Index 1 = playoff_start_week, Index 2 = playoff_start_week + 1, etc.
            w2idx = {w: w - playoff_start + 1 for w in actual_weeks}

            # Assign indices
            df.loc[mask_y, "playoff_week_index"] = df.loc[mask_y, "week"].map(w2idx).astype(int)
            df.loc[mask_y, "playoff_round_num"] = df.loc[mask_y, "playoff_week_index"].astype(int)

            # Assign labels using expected structure
            labels = {w: _label_for_index(w2idx[w], total, w, expected_championship_week) for w in actual_weeks}
            df.loc[mask_y, "playoff_round"] = df.loc[mask_y, "week"].map(labels)

            # Set binary flags
            for w in actual_weeks:
                lab = labels[w]
                if lab in ("quarterfinal", "semifinal", "championship"):
                    week_mask = mask_y & (df["week"] == w)
                    df.loc[week_mask, lab] = 1

            # PRESERVE bracket-set championship: if bracket API set championship=1,
            # ensure those rows have championship=1 and playoff_round="championship"
            if has_bracket_championship and bracket_championship_week:
                df.loc[bracket_championship_mask, "championship"] = 1
                df.loc[bracket_championship_mask, "playoff_round"] = "championship"

        # ====== CONSOLATION BRACKET ======
        cons_mask_y = (df["year"] == yr) & (df["is_consolation"] == 1)
        cons_weeks = sorted(df.loc[cons_mask_y, "week"].dropna().unique().astype(int))

        if cons_weeks:
            # Use EXPECTED rounds for consolation too (same postseason length)
            total_cons = expected_rounds
            # Create week-to-index mapping based on playoff_start_week
            cons_w2idx = {w: w - playoff_start + 1 for w in cons_weeks}

            # Assign consolation round labels
            for w in cons_weeks:
                idx = cons_w2idx[w]
                week_mask = cons_mask_y & (df["week"] == w)

                # Label based on position from expected end
                offset_from_end = total_cons - idx

                # Guard: Don't mark as consolation_final if we haven't reached expected end week
                if w < expected_championship_week and offset_from_end == 0:
                    df.loc[week_mask, "consolation_round"] = "consolation_semifinal"
                    df.loc[week_mask, "consolation_semifinal"] = 1
                elif offset_from_end == 0:
                    df.loc[week_mask, "consolation_round"] = "consolation_final"
                    df.loc[week_mask, "consolation_final"] = 1
                elif offset_from_end == 1:
                    df.loc[week_mask, "consolation_round"] = "consolation_semifinal"
                    df.loc[week_mask, "consolation_semifinal"] = 1
                else:
                    df.loc[week_mask, "consolation_round"] = f"consolation_round_{idx}"

        # ====== DYNAMIC PLACEMENT GAME DETECTION ======
        # Track all teams through postseason to detect placement games
        # FULLY GENERIC: Works for any bracket size (2-32 teams)
        #
        # Strategy:
        # 1. Track team paths through brackets (championship vs consolation)
        # 2. Pair teams that lost/won in same week from same bracket
        # 3. Calculate placement rank based on:
        #    - Their playoff seeds
        #    - How many teams are competing for that placement
        #    - Bracket tree structure

        postseason_mask = (df["year"] == yr) & ((df["is_playoffs"] == 1) | (df["is_consolation"] == 1))
        postseason_weeks = sorted(df.loc[postseason_mask, "week"].dropna().unique().astype(int))

        if not postseason_weeks:
            continue

        # Build a history of who each team has played and their results
        team_history = {}  # {manager: [(week, opponent, won, bracket_type), ...]}

        # Track team seeds for placement calculations
        team_seeds = {}  # {manager: final_playoff_seed}

        for week in postseason_weeks:
            week_mask = (df["year"] == yr) & (df["week"] == week)
            week_df = df[week_mask]

            for _, row in week_df.iterrows():
                mgr = row["manager"]
                opp = row["opponent"]
                won = row["win"] == 1
                bracket_type = "championship" if row["is_playoffs"] == 1 else "consolation"

                if mgr not in team_history:
                    team_history[mgr] = []
                team_history[mgr].append((week, opp, won, bracket_type))

                # Store team seed for later calculations
                if mgr not in team_seeds and "final_playoff_seed" in row.index:
                    seed = row["final_playoff_seed"]
                    if pd.notna(seed):
                        team_seeds[mgr] = int(seed)

        # Track which teams are still alive in each bracket for dynamic placement calculation
        # This is the KEY to making placement detection work for any bracket size
        alive_by_week = {}  # {week: {'championship': set(), 'consolation': set()}}

        for week in postseason_weeks:
            week_mask = (df["year"] == yr) & (df["week"] == week)
            week_df = df[week_mask]

            # Count teams still alive in each bracket
            champ_alive = set(week_df[week_df["is_playoffs"] == 1]["franchise_id"].unique())
            cons_alive = set(week_df[week_df["is_consolation"] == 1]["franchise_id"].unique())

            alive_by_week[week] = {"championship": champ_alive, "consolation": cons_alive}

        # NOTE: Placement game detection is now handled by simulate_playoff_brackets()
        # which properly assigns unique sequential ranks (3rd, 5th, 7th, 9th...)
        # See: playoff_bracket/placement_games.py

    # ====== ENFORCE MUTUAL EXCLUSIVITY ======
    # CRITICAL: Ensure playoff and consolation labels are mutually exclusive
    # is_playoffs=1 → ONLY playoff_round can be set, consolation_round MUST be empty
    # is_consolation=1 → ONLY consolation_round can be set, playoff_round MUST be empty
    #
    # This runs LAST to override any mislabeling from placement game detection

    playoff_only_mask = df["is_playoffs"] == 1
    consolation_only_mask = df["is_consolation"] == 1

    # Count violations before fixing
    violations_before = 0
    for _, row in df.iterrows():
        if row["is_playoffs"] == 1 and pd.notna(row.get("consolation_round")) and row.get("consolation_round") != "":
            violations_before += 1
        if row["is_consolation"] == 1 and pd.notna(row.get("playoff_round")) and row.get("playoff_round") != "":
            violations_before += 1

    # Clear consolation labels from playoff games
    df.loc[playoff_only_mask, "consolation_round"] = ""
    df.loc[playoff_only_mask, "consolation_semifinal"] = 0
    df.loc[playoff_only_mask, "consolation_final"] = 0
    df.loc[playoff_only_mask, "placement_game"] = 0
    df.loc[playoff_only_mask, "placement_rank"] = 0

    # Clear playoff labels from consolation games
    df.loc[consolation_only_mask, "playoff_round"] = ""
    df.loc[consolation_only_mask, "playoff_week_index"] = 0
    df.loc[consolation_only_mask, "playoff_round_num"] = 0
    df.loc[consolation_only_mask, "quarterfinal"] = 0
    df.loc[consolation_only_mask, "semifinal"] = 0
    df.loc[consolation_only_mask, "championship"] = 0

    if violations_before > 0:
        safe_print(f"\n  [ENFORCE] Fixed {violations_before} conflicting round labels")
    safe_print(f"            Playoff games: {playoff_only_mask.sum()} (only playoff_round)")
    safe_print(f"            Consolation games: {consolation_only_mask.sum()} (only consolation_round)")

    # Convert types
    df["playoff_week_index"] = pd.to_numeric(df["playoff_week_index"], errors="coerce").fillna(0).astype("Int64")
    df["playoff_round_num"] = pd.to_numeric(df["playoff_round_num"], errors="coerce").fillna(0).astype("Int64")
    df["placement_game"] = df["placement_game"].astype(object).fillna(0).astype(int)
    df["placement_rank"] = pd.to_numeric(df["placement_rank"], errors="coerce").fillna(0).astype("Int64")

    for col in ["quarterfinal", "semifinal", "championship", "consolation_semifinal", "consolation_final"]:
        df[col] = df[col].astype(object).fillna(0).astype(int)

    # Summary stats
    qf_count = (df["quarterfinal"] == 1).sum()
    sf_count = (df["semifinal"] == 1).sum()
    champ_count = (df["championship"] == 1).sum()
    placement_count = (df["placement_game"] == 1).sum()
    cons_sf_count = (df["consolation_semifinal"] == 1).sum()
    cons_final_count = (df["consolation_final"] == 1).sum()

    return df


@ensure_normalized
def mark_champions_and_sackos(df: pd.DataFrame, settings_dir: str = None) -> pd.DataFrame:
    """
    DEPRECATED: This function is now a no-op.

    Champion and sacko detection is now handled by:
        simulate_playoff_brackets() in playoff_bracket/__init__.py

    This function exists only for backward compatibility with existing callers.
    It returns the DataFrame unchanged.

    The new approach properly:
    - Assigns unique placement ranks (3rd, 5th, 7th, 9th...)
    - Detects champion as winner of final championship game
    - Detects sacko as loser of worst placement game (e.g., 9th place for 10-team league)
    - Scales to any league size (8, 10, 12+ teams)
    """
    # NO-OP: simulate_playoff_brackets() is called by sql_matchup_enrichments.py
    # after normalize_playoff_flags completes
    return df


@ensure_normalized
def add_season_result(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add season_result column combining win/loss outcome with game type.

    GENERIC & DYNAMIC: Works for any postseason structure.

    season_result format: "{won|lost} {game_type}"

    Examples:
    - "won championship"
    - "lost championship"
    - "won third place game"
    - "lost third place game"
    - "won fifth place game"
    - "lost seventh place game"
    - "won consolation semifinal"
    - etc.

    IMPORTANT: The season_result is propagated to ALL rows with the same
    manager_year, so the final result is visible regardless of week filters.

    Args:
        df: DataFrame with playoff/consolation round labels and win/loss flags

    Returns:
        DataFrame with season_result column added (propagated to all rows per manager_year)
    """
    df = df.copy()

    # Initialize season_result column
    df["season_result"] = ""

    # Ensure required columns exist
    required = ["year", "week", "win", "loss"]
    for col in required:
        if col not in df.columns:
            safe_print(f"  [WARN] Missing column {col}, cannot create season_result")
            return df

    # Ensure manager_year column exists for propagation (use int year to avoid float formatting)
    if "manager_year" not in df.columns and "manager" in df.columns:
        df["manager_year"] = df["manager"].astype(str) + "_" + df["year"].fillna(0).astype(int).astype(str)
    elif "manager_year" in df.columns:
        # Ensure consistent formatting (may have been created with float year)
        df["manager_year"] = df["manager"].astype(str) + "_" + df["year"].fillna(0).astype(int).astype(str)

    # Dictionary to store final result per manager_year
    final_results = {}

    # Process each year to find all postseason results
    # Priority: Championship > Semifinal > Quarterfinal > Consolation
    years = sorted(df["year"].dropna().unique())

    for year in years:
        year = int(year)
        year_mask = df["year"] == year

        # Find all postseason games
        postseason_mask = year_mask & (
            (df.get("is_playoffs", pd.Series(0)) == 1) | (df.get("is_consolation", pd.Series(0)) == 1)
        )

        if not postseason_mask.any():
            continue

        # Get ALL postseason games, not just championship week
        # This captures semifinal/quarterfinal losers who don't play in championship week
        postseason_df = df[postseason_mask]

        # Track managers who already have results (higher priority rounds override lower)
        managers_with_results = set()

        # Process games in reverse week order (championship week first)
        # so that championship results take priority over earlier rounds
        for week in sorted(postseason_df["week"].unique(), reverse=True):
            week_df = postseason_df[postseason_df["week"] == week]

            for _, row in week_df.iterrows():
                manager = row.get("manager", "")
                manager_year_key = f"{manager}_{year}"

                # Skip if this manager already has a result from a later round
                if manager_year_key in managers_with_results:
                    continue

                # Determine outcome (won or lost)
                if row["win"] == 1:
                    outcome = "won"
                elif row["loss"] == 1:
                    outcome = "lost"
                else:
                    # Tie or unknown
                    outcome = "tied in"

                # Determine game type
                game_type = None

                # Championship game (is_playoffs=1, championship=1)
                if row.get("is_playoffs") == 1 and row.get("championship") == 1:
                    game_type = "championship"

                # Playoff semifinal
                elif row.get("is_playoffs") == 1 and row.get("semifinal") == 1:
                    game_type = "semifinal"

                # Playoff quarterfinal
                elif row.get("is_playoffs") == 1 and row.get("quarterfinal") == 1:
                    game_type = "quarterfinal"

                # Placement games (consolation with placement_rank)
                elif row.get("is_consolation") == 1 and row.get("placement_game") == 1:
                    # Use consolation_round label if available
                    cons_round = str(row.get("consolation_round", ""))
                    if cons_round and cons_round != "":
                        # Remove 'game' suffix if present (to avoid "won third place game game")
                        game_type = cons_round.replace("_game", " game").replace("_", " ")
                    else:
                        # Fallback to placement_rank
                        placement_rank = row.get("placement_rank", 0)
                        if placement_rank > 0:
                            rank_suffix = {1: "st", 2: "nd", 3: "rd"}.get(
                                placement_rank % 10 if placement_rank not in [11, 12, 13] else 0, "th"
                            )
                            game_type = f"{placement_rank}{rank_suffix} place game"

                # Consolation round (not a placement game)
                elif row.get("is_consolation") == 1:
                    cons_round = str(row.get("consolation_round", ""))
                    if "final" in cons_round:
                        game_type = "consolation final"
                    elif "semifinal" in cons_round:
                        game_type = "consolation semifinal"
                    else:
                        game_type = "consolation round"

                # Store final result for this manager_year and mark as processed
                if game_type:
                    final_results[manager_year_key] = f"{outcome} {game_type}"
                    managers_with_results.add(manager_year_key)

    # Propagate season_result to ALL rows with the same manager_year
    if "manager_year" in df.columns and final_results:
        df["season_result"] = df["manager_year"].map(final_results).fillna("")
    elif final_results:
        # Fallback: create manager_year key on the fly (use int year to match key format)
        df["season_result"] = (
            (df["manager"].astype(str) + "_" + df["year"].fillna(0).astype(int).astype(str))
            .map(final_results)
            .fillna("")
        )

    safe_print("  [OK] Added season_result column (propagated to all rows)")

    # Count unique manager_years with results
    unique_results = len(final_results)
    total_rows_with_result = (df["season_result"] != "").sum()
    safe_print(f"       {unique_results} unique manager_year results propagated to {total_rows_with_result} rows")

    return df
