"""
ESPN Roster/Player Fetcher

Fetches weekly roster and player data from ESPN Fantasy API.

Two paths depending on era:
- 2019+: box_scores(week) provides BoxPlayer objects with weekly points
- Pre-2019 or provider-archived seasons: final roster only (no weekly lineup)

Output columns match CanonicalPlayerColumns for downstream pipeline compatibility.
"""

import logging
from collections import defaultdict

import pandas as pd

from multi_league.core.canonical_roster import normalize_roster_df

logger = logging.getLogger(__name__)


def log(msg: str):
    logger.info(msg)
    print(msg)


# ESPN lineup slot ID -> canonical fantasy_position
# fantasy_position = where the manager placed the player (lineup slot)
# NOT the player's actual NFL position
# Full ESPN slot ID reference: https://github.com/cwendt94/espn-api
ESPN_SLOT_MAP = {
    0: "QB",
    1: "TQB",  # Team QB (rare)
    2: "RB",
    3: "RB/WR",  # RB/WR flex
    4: "WR",
    5: "WR/TE",  # WR/TE flex
    6: "TE",
    7: "OP",  # QB/RB/WR/TE (SUPER_FLEX)
    8: "DT",  # IDP
    9: "DE",  # IDP
    10: "LB",  # IDP
    11: "DL",  # IDP DL flex
    12: "CB",  # IDP
    13: "S",  # IDP Safety
    14: "DB",  # IDP DB flex
    15: "DP",  # IDP flex (any defensive player)
    16: "DEF",
    17: "K",
    18: "P",  # Punter
    19: "HC",  # Head Coach
    20: "BN",  # Bench
    21: "IR",  # Injured Reserve
    22: "BN",  # Bench (alternate)
    23: "FLEX",  # RB/WR/TE
}

# ESPN sometimes returns lineupSlot as a string name instead of int ID
# Map string values to canonical positions
ESPN_STRING_SLOT_MAP = {
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "K": "K",
    "DEF": "DEF",
    "D/ST": "DEF",
    "DST": "DEF",
    "FLEX": "FLEX",
    "RB/WR/TE": "FLEX",
    "RB/WR": "FLEX",
    "WR/TE": "FLEX",
    "BE": "BN",
    "BN": "BN",
    "BENCH": "BN",
    "IR": "IR",
    "ER": "IR",
    "OP": "OP",
    "SUPER_FLEX": "OP",
    # IDP slots
    "DT": "DT",
    "DE": "DE",
    "LB": "LB",
    "DL": "DL",
    "CB": "CB",
    "S": "S",
    "DB": "DB",
    "DP": "DP",
    # Misc
    "TQB": "TQB",
    "P": "P",
    "HC": "HC",
}

# Non-starter positions (bench/IR) - both int slot IDs and string names
NON_STARTER_SLOTS = {20, 21, 22}  # BN, IR, BN (alternate)
NON_STARTER_NAMES = {"BN", "IR", "BE", "ER", "BENCH", "TAXI"}  # String equivalents

# ESPN position ID -> position abbreviation (for player's actual position)
ESPN_POSITION_ID_MAP = {
    1: "QB",
    2: "RB",
    3: "WR",
    4: "TE",
    5: "K",
    16: "DEF",
}


def _get_max_weeks(year: int) -> int:
    """Get maximum weeks for a season."""
    if year >= 2021:
        return 18
    return 17


def _get_player_position(player) -> str:
    """Extract player's actual NFL position from ESPN player object."""
    # Try position attribute directly
    pos = getattr(player, "position", None)
    if pos and pos != "Unknown":
        return pos

    # Try eligibleSlots
    eligible = getattr(player, "eligibleSlots", []) or []
    for slot_id in eligible:
        if slot_id in ESPN_POSITION_ID_MAP:
            return ESPN_POSITION_ID_MAP[slot_id]

    return "Unknown"


def _normalize_dst_name(name: str, pro_team: str = None) -> str:
    """
    Normalize D/ST player names to canonical format.

    ESPN uses "{Mascot} D/ST" (e.g., "Buccaneers D/ST").
    Our canonical format is "{Mascot} DST" (e.g., "Buccaneers DST").
    """
    if not name:
        return name
    # Convert "X D/ST" -> "X DST"
    if " D/ST" in name:
        return name.replace(" D/ST", " DST")
    return name


def _build_ownership_map(ctx, year: int, db, max_weeks: int = None) -> dict:
    """Build player ownership map: {espn_player_id: {week: team_key}} from draft + transactions.

    Reads from local DuckDB (draft and transactions tables must already be populated).

    Returns dict where ownership[player_id][week] = team_key of the owning team.
    Forward-fills ownership: once a player is on a team, they stay until the next transaction.
    """
    if db is None:
        raise ValueError("db (LocalLeagueDB) is required for ESPN roster ownership resolution")

    if max_weeks is None:
        max_weeks = _get_max_weeks(year)

    # ownership[player_id] = {week: team_key}
    ownership = defaultdict(dict)

    # Load draft → initial owners (week 0, will forward-fill to week 1+)
    draft_df = None
    try:
        draft_df = db.read_table("draft", year=year)
    except Exception:
        pass

    if draft_df is not None and not draft_df.empty:
        for _, row in draft_df.iterrows():
            pid = row.get("espn_player_id")
            team = row.get("team_key")
            if pid and team:
                ownership[pid][0] = str(team)  # Week 0 = pre-season draft

    # Load transactions → ownership changes
    txn_df = None
    try:
        txn_df = db.read_table("transactions", year=year)
    except Exception:
        pass

    if txn_df is not None and not txn_df.empty:
        # Sort by week, then timestamp for within-week ordering
        sort_cols = ["week"]
        if "timestamp" in txn_df.columns:
            sort_cols.append("timestamp")
        txn_df = txn_df.sort_values(sort_cols, na_position="last")

        for _, row in txn_df.iterrows():
            pid = row.get("espn_player_id")
            week = row.get("week")
            txn_type = str(row.get("transaction_type", "")).lower()
            team = row.get("team_key")

            if not pid or pd.isna(week):
                continue
            week = int(week)

            if txn_type in ("add", "trade"):
                ownership[pid][week] = str(team)
            elif txn_type == "drop":
                ownership[pid][week] = None  # Unrostered

    # Forward-fill: propagate ownership across weeks
    for pid in list(ownership.keys()):
        week_changes = sorted(ownership[pid].keys())
        if not week_changes:
            continue

        filled = {}
        current_owner = None
        for wk in range(0, max_weeks + 1):
            if wk in ownership[pid]:
                current_owner = ownership[pid][wk]
            if current_owner is not None:
                filled[wk] = current_owner
        ownership[pid] = filled

    return dict(ownership)


def _pick_preferred_roster_row(candidates: list[dict]) -> dict:
    """Prefer the real started copy when ESPN emits duplicate roster rows."""
    if len(candidates) == 1:
        return candidates[0]

    def _score(row: dict) -> tuple[int, int, int, float]:
        fantasy_position = str(row.get("fantasy_position") or "").upper()
        is_started = bool(row.get("is_started"))
        has_starter_slot = bool(fantasy_position) and fantasy_position not in NON_STARTER_NAMES
        has_projection = row.get("projected_points") is not None
        fantasy_points = float(row.get("fantasy_points") or 0)
        return (
            1 if is_started else 0,
            1 if has_starter_slot else 0,
            1 if has_projection else 0,
            fantasy_points,
        )

    return max(candidates, key=_score)


def _resolve_ghost_rosters(rows: list, ctx, year: int, db, max_weeks: int = None) -> list:
    """Resolve ghost roster entries using transaction history as source of truth.

    ESPN box_scores() returns stale data — traded/dropped players still appear
    on their old team's bench. Instead of heuristic dedup (keep started), use
    the draft + transaction timeline to determine true ownership each week.

    Falls back to heuristic (keep started) when ownership can't be determined
    (e.g., undrafted free agents with no transaction record, pre-2019 estimated data).
    """
    # Step 1: Find duplicates (same player, same week, different teams)
    pw_rows = defaultdict(list)
    for r in rows:
        pid = r.get("espn_player_id")
        if pid:
            key = (pid, r["year"], r["week"])
        else:
            key = (r.get("player"), r["year"], r["week"])
        pw_rows[key].append(r)

    # Quick exit if no duplicates
    dups = {k: v for k, v in pw_rows.items() if len(v) > 1}
    if not dups:
        return rows

    # Step 2: Build ownership map from draft + transactions
    ownership = _build_ownership_map(ctx, year, db=db, max_weeks=max_weeks)

    # Step 3: Resolve each duplicate
    resolved_rows = []
    ghost_count = 0
    txn_resolved = 0
    heuristic_resolved = 0

    for key, group in pw_rows.items():
        if len(group) == 1:
            resolved_rows.append(group[0])
            continue

        pid, yr, wk = key
        owner_team = ownership.get(pid, {}).get(wk)

        if owner_team:
            # Transaction says who owns this player — keep that row
            match = [r for r in group if str(r.get("team_key")) == str(owner_team)]
            if match:
                resolved_rows.append(_pick_preferred_roster_row(match))
                ghost_count += len(group) - 1
                txn_resolved += 1
                continue

        # Fallback: keep started version (heuristic)
        started = [r for r in group if r.get("is_started")]
        if started:
            resolved_rows.append(_pick_preferred_roster_row(started))
        else:
            resolved_rows.append(_pick_preferred_roster_row(group))
        ghost_count += len(group) - 1
        heuristic_resolved += 1

    if ghost_count:
        log(
            f"  [ROSTERS] Removed {ghost_count} ghost roster entries "
            f"({txn_resolved} via transactions, {heuristic_resolved} via heuristic)"
        )

    return resolved_rows


def fetch_espn_rosters_modern(
    ctx: "ESPNContext",
    year: int,
    db=None,
    max_weeks: int = None,
    weeks: list[int] | None = None,
    *,
    client=None,
    league=None,
    box_scores_out: dict[int, list] | None = None,
) -> pd.DataFrame | None:
    """
    Fetch roster/player data for 2019+ using box_scores.

    Each BoxPlayer has: name, playerId, position, eligibleSlots,
    points, projected_points, lineupSlot, proTeam
    """
    from .espn_api_client import ESPNAPIClient

    if client is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [ROSTERS] Failed to load league for {year}: {e}")
        return None

    if getattr(league, "_uses_league_history", False):
        return fetch_espn_rosters_legacy(
            ctx, year, max_weeks=max_weeks, weeks=weeks, client=client, league=league
        )

    if max_weeks is None:
        max_weeks = _get_max_weeks(year)
    from multi_league.core.league_refresh import provider_weeks_to_fetch

    weeks_to_fetch = provider_weeks_to_fetch(max_week=max_weeks, requested_weeks=weeks)
    rows = []
    consecutive_empty = 0
    MAX_CONSECUTIVE_EMPTY = 3

    for week in weeks_to_fetch:
        try:
            box_scores = league.box_scores(week)
            if box_scores_out is not None:
                box_scores_out[int(week)] = box_scores
        except Exception:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            continue

        if not box_scores:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            continue

        week_has_data = False

        for bs in box_scores:
            home_team = getattr(bs, "home_team", None)
            if home_team and hasattr(bs, "home_lineup"):
                for player in bs.home_lineup or []:
                    row = _build_player_row(ctx, year, week, home_team, player)
                    if row:
                        rows.append(row)
                        if row.get("fantasy_points", 0) and row["fantasy_points"] > 0:
                            week_has_data = True

            away_team = getattr(bs, "away_team", None)
            if away_team and hasattr(bs, "away_lineup"):
                for player in bs.away_lineup or []:
                    row = _build_player_row(ctx, year, week, away_team, player)
                    if row:
                        rows.append(row)
                        if row.get("fantasy_points", 0) and row["fantasy_points"] > 0:
                            week_has_data = True

        if not week_has_data and week > 1:
            rows = [r for r in rows if not (r["year"] == year and r["week"] == week)]
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            continue
        else:
            consecutive_empty = 0

    # --- Post-collection deduplication ---

    if rows:
        # Fix G: Resolve ghost roster entries using transaction history as source of truth.
        # ESPN box_scores() returns stale data — traded/dropped players still appear
        # on their old team's bench. Use draft + transaction timeline to determine
        # true ownership each week, falling back to heuristic (keep started) when
        # ownership can't be determined.
        rows = _resolve_ghost_rosters(rows, ctx, year, db=db, max_weeks=max(weeks_to_fetch, default=0))

        # Fix F: Detect and remove duplicate consecutive weeks — ESPN API bug where
        # box_scores() returns same data for week 17 and 18 in some years (18-week transition)
        week_fingerprints = defaultdict(set)
        for r in rows:
            key = (r.get("espn_player_id") or r.get("player"), r.get("fantasy_points"))
            week_fingerprints[(r["year"], r["week"])].add(key)

        weeks_to_remove = set()
        year_weeks = sorted(week_fingerprints.keys())
        for i in range(len(year_weeks) - 1):
            curr = year_weeks[i]
            nxt = year_weeks[i + 1]
            if curr[0] == nxt[0] and nxt[1] == curr[1] + 1:  # Same year, consecutive
                if week_fingerprints[curr] == week_fingerprints[nxt] and len(week_fingerprints[curr]) > 5:
                    log(
                        f"  [ROSTERS] WARNING: Week {nxt[1]} has identical data to week {curr[1]} in {curr[0]} — removing duplicate week {nxt[1]}"
                    )
                    weeks_to_remove.add(nxt)

        if weeks_to_remove:
            rows = [r for r in rows if (r["year"], r["week"]) not in weeks_to_remove]

    if not rows:
        return None

    df = pd.DataFrame(rows)
    log(f"  [ROSTERS] {year}: {len(df)} player-week rows")
    return df


def fetch_espn_rosters_legacy(
    ctx: "ESPNContext",
    year: int,
    max_weeks: int = None,
    weeks: list[int] | None = None,
    *,
    client=None,
    league=None,
) -> pd.DataFrame | None:
    """
    Fetch final roster data for pre-2019 or provider-archived seasons.

    LIMITATION: These payloads contain the FINAL roster only.
    Weekly lineup slots and points are NOT available.
    All players show same roster regardless of week.
    """
    from .espn_api_client import ESPNAPIClient

    if client is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [ROSTERS] Failed to load league for {year}: {e}")
        return None

    if max_weeks is None:
        max_weeks = _get_max_weeks(year)
    from multi_league.core.league_refresh import provider_weeks_to_fetch

    weeks_to_fetch = provider_weeks_to_fetch(max_week=max_weeks, requested_weeks=weeks)
    rows = []

    # Pre-2019: We get final rosters from league.teams
    # We replicate this across all weeks since we can't get weekly snapshots
    for team in league.teams:
        team_id = team.team_id
        roster = getattr(team, "roster", []) or []

        if not roster:
            continue

        for week in weeks_to_fetch:
            for player in roster:
                player_name = getattr(player, "name", "Unknown")
                player_id = getattr(player, "playerId", None)
                position = _get_player_position(player)
                pro_team = getattr(player, "proTeam", None)

                # Normalize D/ST names
                if position in ("DEF", "D/ST", "DST"):
                    player_name = _normalize_dst_name(player_name, pro_team)
                    position = "DEF"

                row = {
                    "year": year,
                    "week": week,
                    "manager": ctx.get_manager_name(
                        team_id, team_name=getattr(team, "team_name", f"Team {team_id}"), year=year
                    ),
                    "manager_guid": ctx.get_manager_guid(team_id, year=year),
                    "team_key": str(team_id),
                    "team_name": getattr(team, "team_name", f"Team {team_id}"),
                    "franchise_id": ctx.get_franchise_id(team_id, year=year),
                    "espn_player_id": player_id,
                    "player": player_name,
                    "nfl_team": pro_team,
                    "position": position,
                    "fantasy_position": None,  # Can't determine lineup slot pre-2019
                    "eligible_positions": None,
                    # PENDING: Pre-2019 weekly player points unavailable. Season totals only.
                    "fantasy_points": None,
                    "projected_points": None,
                    "is_started": None,  # Can't determine pre-2019
                    "platform": "espn",
                    "league_id": str(ctx.get_league_id_for_year(year)),
                }
                rows.append(row)

    if not rows:
        return None

    df = pd.DataFrame(rows)
    log(f"  [ROSTERS] {year} (legacy): {len(df)} player-week rows (season totals only, no weekly points)")
    return df


def _build_player_row(ctx, year: int, week: int, team, player) -> dict | None:
    """Build a single player row from a BoxPlayer object."""
    player_name = getattr(player, "name", None) or "Unknown"
    player_id = getattr(player, "playerId", None)
    points = getattr(player, "points", 0) or 0
    projected = getattr(player, "projected_points", None)
    lineup_slot = getattr(player, "lineupSlot", None)
    pro_team = getattr(player, "proTeam", None)

    # Get player's actual position
    position = _get_player_position(player)

    # Map lineup slot to fantasy_position (handle both int and string values)
    if lineup_slot is not None:
        if isinstance(lineup_slot, int):
            fantasy_position = ESPN_SLOT_MAP.get(lineup_slot, None)
        else:
            fantasy_position = ESPN_STRING_SLOT_MAP.get(str(lineup_slot).upper(), None)
    else:
        fantasy_position = None

    # Determine is_started from fantasy_position (the mapped slot name).
    # Only players in actual starter slots (QB, RB, WR, TE, FLEX, K, DEF, OP)
    # are started. BN/IR are not.
    if fantasy_position is not None:
        is_started = fantasy_position not in NON_STARTER_NAMES
    else:
        is_started = None

    # Build eligible positions list
    eligible = getattr(player, "eligibleSlots", []) or []
    eligible_positions = (
        ",".join(
            filter(
                None,
                (
                    ESPN_SLOT_MAP.get(s) if isinstance(s, int) else ESPN_STRING_SLOT_MAP.get(str(s).upper())
                    for s in eligible
                ),
            )
        )
        if eligible
        else None
    )

    # Normalize D/ST names
    if position in ("DEF", "D/ST", "DST") or (lineup_slot == 16):
        player_name = _normalize_dst_name(player_name, pro_team)
        position = "DEF"

    team_id = team.team_id
    team_name = getattr(team, "team_name", f"Team {team_id}")

    return {
        "year": year,
        "week": week,
        "manager": ctx.get_manager_name(team_id, team_name=team_name, year=year),
        "manager_guid": ctx.get_manager_guid(team_id, year=year),
        "team_key": str(team_id),
        "team_name": team_name,
        "franchise_id": ctx.get_franchise_id(team_id, year=year),
        "espn_player_id": player_id,
        "player": player_name,
        "nfl_team": pro_team,
        "position": position,
        "fantasy_position": fantasy_position,
        "eligible_positions": eligible_positions,
        "fantasy_points": round(points, 2) if points else 0,
        # `if projected is not None`, NOT `if projected` — a legitimate
        # 0.0 projection (bye weeks, inactive / injured / cut players) is
        # truthy-false in Python, and using truthiness here silently maps
        # ESPN's correct 0.0 to None. That downstream-nulls K / DEF / TE
        # projections on NFL bye weeks, breaks the 75% coverage threshold
        # in populate_team_projected_points, and nulls out the whole
        # team_projected_points row — the root of the
        # sim_proj_wins_populated / sim_projected_points_populated
        # residuals (tfl 2020 w5/w9, 2023 w14, pigskin 2024 w14).
        "projected_points": round(projected, 2) if projected is not None else None,
        "is_started": is_started,
        "platform": "espn",
        "league_id": str(ctx.get_league_id_for_year(year)),
    }


def fetch_espn_rosters(
    ctx: "ESPNContext",
    year: int,
    db=None,
    weeks: list[int] | None = None,
) -> pd.DataFrame | None:
    """
    Fetch rosters for a single year, choosing modern or legacy path.

    Args:
        ctx: ESPNContext
        year: NFL season year
        db: LocalLeagueDB instance (required for ghost resolution via draft/transactions)

    Returns:
        DataFrame with player/roster data
    """
    # Read end_week from league_settings in local DB to cap the fetch range
    max_weeks = None
    if db is not None:
        try:
            conn = db.connect()
            row = conn.execute("SELECT end_week FROM public.league_settings WHERE year = ?", [year]).fetchone()
            if row and row[0]:
                max_weeks = int(row[0])
                log(f"  [ROSTERS] {year}: capping at week {max_weeks} (from league_settings)")
        except Exception:
            pass  # Fall back to hardcoded default

    if year >= 2019:
        return fetch_espn_rosters_modern(ctx, year, db=db, max_weeks=max_weeks, weeks=weeks)
    else:
        return fetch_espn_rosters_legacy(ctx, year, max_weeks=max_weeks, weeks=weeks)


def fetch_all_espn_rosters(ctx: "ESPNContext", db=None) -> pd.DataFrame:
    """
    Fetch roster data for all years.

    When db is provided, normalizes to canonical schema and writes directly to
    LocalLeagueDB (no per-year parquets needed — Phase 2 merge is skipped).

    Args:
        ctx: ESPNContext with year range
        db: LocalLeagueDB instance. When provided, saves normalized data
            to DuckDB per-year. Also required for ghost resolution (reads
            draft + transactions from DB).

    Returns:
        Combined DataFrame for all years
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    years = list(ctx.get_year_range())
    log(f"\n{'='*60}")
    log(f"Fetching ESPN roster/player data ({len(years)} years)")
    log(f"{'='*60}")

    # Parallel API fetch (each year is independent HTTP calls)
    year_dfs: dict[int, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=min(3, len(years))) as executor:
        futures = {executor.submit(fetch_espn_rosters, ctx, year, db=db): year for year in years}
        for future in as_completed(futures):
            year = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    year_dfs[year] = df
            except Exception as e:
                log(f"  [ROSTERS] {year}: FAILED - {e}")

    # Sequential normalize + DuckDB save (single-writer)
    all_dfs = []
    for year in sorted(year_dfs.keys()):
        df = year_dfs[year]
        if db is not None:
            league_id = str(ctx.get_league_id_for_year(year))
            normalized = normalize_roster_df(df, platform="espn", league_id=league_id)
            db.save_table("player_fantasy", normalized, year=year)
            log(f"  [LocalDB] player_fantasy year={year}: {len(normalized):,} rows (canonical)")
            all_dfs.append(normalized)
        else:
            all_dfs.append(df)

    if not all_dfs:
        log("[ROSTERS] No roster data found for any year")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    log(f"[ROSTERS] Total: {len(combined)} player-week rows across {len(all_dfs)} years")

    return combined
