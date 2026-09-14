"""
ESPN Transactions Fetcher

Fetches transaction history from ESPN Fantasy API.

Three paths depending on era and transaction type:
1. 2019+ Waivers/FA: Library's transactions() method per scoring period
2. 2019+ Trades: Raw ESPN API (mTransactions2 view) because library crashes
   on TRADE_ACCEPT records missing 'status' key
3. Pre-2019: Roster-diff workaround (compare draft vs final rosters)
   All pre-2019 transactions get is_estimated=True

Output columns match CanonicalTransactionColumns for pipeline compatibility.
"""

import logging

import pandas as pd

import sys

logger = logging.getLogger(__name__)
_verbose = "--verbose" in sys.argv


def log(msg: str):
    logger.info(msg)
    print(msg)


def _get_max_weeks(year: int) -> int:
    if year >= 2021:
        return 18
    return 17


def _normalize_dst_name(name: str) -> str:
    """Normalize D/ST names: 'X D/ST' -> 'X DST'."""
    if not name:
        return name
    if " D/ST" in name:
        return name.replace(" D/ST", " DST")
    return name


def _build_player_lookup(league) -> dict[int, str]:
    """Build player ID -> name lookup from league roster and free agents."""
    player_lookup = {}

    # From team rosters
    for team in league.teams or []:
        for player in getattr(team, "roster", []) or []:
            pid = getattr(player, "playerId", None)
            name = getattr(player, "name", None)
            if pid and name:
                player_lookup[pid] = _normalize_dst_name(name)

    # Also try free_agents for broader coverage
    try:
        free_agents = getattr(league, "free_agents", None)
        if callable(free_agents):
            for player in free_agents(size=200):
                pid = getattr(player, "playerId", None)
                name = getattr(player, "name", None)
                if pid and name:
                    player_lookup[pid] = _normalize_dst_name(name)
    except Exception:
        pass  # free_agents may not be available for historical years

    return player_lookup


def fetch_espn_transactions_modern(
    ctx: "ESPNContext", year: int, max_week: int | None = None, *, client=None, league=None
) -> pd.DataFrame | None:
    """
    Fetch transactions for 2019+ (waivers/FA + trades via raw API).

    Uses raw ESPN API for both waivers and trades because the library's
    transactions() method only returns current scoring period data.
    """
    from .espn_api_client import ESPNAPIClient

    if client is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [TRANSACTIONS] Failed to load league for {year}: {e}")
        return None

    season_max_weeks = _get_max_weeks(year)
    if max_week is None:
        max_weeks = season_max_weeks
    else:
        max_weeks = int(max_week)
        if not 1 <= max_weeks <= season_max_weeks:
            raise ValueError(f"max_week must be between 1 and {season_max_weeks}, got {max_weeks}")
    rows = []

    # Build player ID -> name lookup from roster + free agents
    player_lookup = _build_player_lookup(league)
    if _verbose:
        log(f"  [TRANSACTIONS] Built player lookup with {len(player_lookup)} players for {year}")

    # === Part 1: Waivers and Free Agent pickups via raw API ===
    # The library's transactions() method is broken for historical years —
    # it only returns current scoring period data. Use raw API instead.
    if _verbose:
        log(f"  [TRANSACTIONS] Fetching waivers/FA via raw API for {year}...")
    try:
        raw_waivers = client.get_raw_waivers(year, max_weeks=max_weeks)
        for txn in raw_waivers:
            txn_id = txn.get("id")
            txn_type_raw = txn.get("type", "unknown")
            scoring_period = txn.get("scoringPeriodId", 0)
            items = txn.get("items", [])
            txn_date = txn.get("processDate") or txn.get("proposalDate")
            faab_bid = txn.get("bidAmount", 0) or 0

            for item in items:
                player_id = item.get("playerId")
                item_type = item.get("type", "").upper()  # 'ADD' or 'DROP'
                from_team = item.get("fromTeamId")
                to_team = item.get("toTeamId")

                # Determine which team this item belongs to
                team_id = to_team if item_type == "ADD" else from_team

                # Resolve player name from lookup
                player_name = player_lookup.get(player_id, "Unknown")

                # Map to canonical transaction_type
                if item_type == "ADD":
                    txn_type = "add"
                    source = "waivers" if txn_type_raw == "WAIVER" else "freeagents"
                elif item_type == "DROP":
                    txn_type = "drop"
                    source = "team"
                else:
                    txn_type = item_type.lower() or "unknown"
                    source = "unknown"

                # FAAB only on the add side
                faab = faab_bid if item_type == "ADD" else 0

                row = {
                    "transaction_id": str(txn_id) if txn_id else None,
                    "year": year,
                    "week": scoring_period,
                    "timestamp": txn_date,
                    # Source/destination fields (None for non-trade rows)
                    "source_manager": None,
                    "source_manager_guid": None,
                    "source_team_name": None,
                    "source_franchise_id": None,
                    "destination_manager": None,
                    "destination_manager_guid": None,
                    "destination_team_name": None,
                    "destination_franchise_id": None,
                    "manager": ctx.get_manager_name(team_id, team_name=ctx.get_team_name(team_id, year), year=year)
                    if team_id
                    else "Unknown",
                    "manager_guid": ctx.get_manager_guid(team_id, year=year) if team_id else "",
                    "team_key": str(team_id) if team_id else "",
                    "team_name": ctx.get_team_name(team_id, year) if team_id else "",
                    "franchise_id": ctx.get_franchise_id(team_id, year=year) if team_id else None,
                    "player": player_name,
                    "espn_player_id": player_id,
                    "position": None,
                    "nfl_team": None,
                    "transaction_type": txn_type,
                    "faab_bid": faab,
                    "source_type": source,
                    "destination": "team" if txn_type == "add" else "waivers",
                    "is_estimated": False,
                    "platform": "espn",
                    "league_id": str(ctx.get_league_id_for_year(year)),
                }
                rows.append(row)

        if _verbose:
            log(f"  [TRANSACTIONS] Found {len(raw_waivers)} waiver/FA transactions for {year} ({len(rows)} rows)")
    except Exception as e:
        log(f"  [TRANSACTIONS] Error fetching waivers for {year}: {e}")

    # === Part 2: Trades via raw API ===
    if _verbose:
        log(f"  [TRANSACTIONS] Fetching trades via raw API for {year}...")
    try:
        trades = client.get_raw_trades(year, max_weeks=max_weeks)
        for trade in trades:
            trade_id = trade.get("id")
            scoring_period = trade.get("scoringPeriodId", 0)
            items = trade.get("items", [])
            accept_date = trade.get("acceptDate")

            for item in items:
                player_id = item.get("playerId")
                # Resolve player name: try raw API fields first, then lookup table
                player_name = item.get("playerName", item.get("player", {}).get("fullName", None))
                if not player_name or player_name == "Unknown":
                    player_name = player_lookup.get(player_id, "Unknown")
                from_team = item.get("fromTeamId")
                to_team = item.get("toTeamId")

                # One row per traded player with source/destination fields.
                # duplicate_trade_rows() in the normalizer will split into
                # sent+received pairs.
                if to_team:
                    rows.append(
                        {
                            "transaction_id": str(trade_id) if trade_id else None,
                            "year": year,
                            "week": scoring_period,
                            "timestamp": accept_date,
                            # Source = who sent the player (from_team)
                            "source_manager": ctx.get_manager_name(
                                from_team, team_name=ctx.get_team_name(from_team, year), year=year
                            )
                            if from_team
                            else None,
                            "source_manager_guid": ctx.get_manager_guid(from_team, year=year) if from_team else None,
                            "source_team_name": ctx.get_team_name(from_team, year) if from_team else None,
                            "source_franchise_id": ctx.get_franchise_id(from_team, year=year) if from_team else None,
                            # Destination = who received the player (to_team)
                            "destination_manager": ctx.get_manager_name(
                                to_team, team_name=ctx.get_team_name(to_team, year), year=year
                            ),
                            "destination_manager_guid": ctx.get_manager_guid(to_team, year=year),
                            "destination_team_name": ctx.get_team_name(to_team, year),
                            "destination_franchise_id": ctx.get_franchise_id(to_team, year=year),
                            # Keep manager for backward compat (gets overwritten by duplicate_trade_rows)
                            "manager": ctx.get_manager_name(
                                to_team, team_name=ctx.get_team_name(to_team, year), year=year
                            ),
                            "manager_guid": ctx.get_manager_guid(to_team, year=year),
                            "team_key": str(to_team),
                            "team_name": ctx.get_team_name(to_team, year),
                            "franchise_id": ctx.get_franchise_id(to_team, year=year),
                            # Player info
                            "player": _normalize_dst_name(player_name) if player_name else None,
                            "espn_player_id": player_id,
                            "position": item.get("position"),
                            "nfl_team": item.get("proTeam"),
                            "transaction_type": "trade",
                            "faab_bid": 0,
                            "source_type": "trade",
                            "destination": "team",
                            "is_estimated": False,
                            "platform": "espn",
                            "league_id": str(ctx.get_league_id_for_year(year)),
                        }
                    )

        if _verbose:
            log(f"  [TRANSACTIONS] Found {len(trades)} trades for {year}")
    except Exception as e:
        log(f"  [TRANSACTIONS] Error fetching trades for {year}: {e}")

    if not rows:
        return None

    df = pd.DataFrame(rows)
    log(f"  [TRANSACTIONS] {year}: {len(df)} transaction rows")
    return df


def fetch_espn_transactions_legacy(ctx: "ESPNContext", year: int) -> pd.DataFrame | None:
    """
    Pre-2019 transaction estimation via roster-diff workaround.

    Compare draft rosters vs final rosters:
    - Players on different team than drafted = "trade" (week 6)
    - Undrafted players on final roster = "add" (week 6)
    - Drafted players not on any final roster = "drop" (week 6)

    All transactions marked is_estimated=True.

    PENDING FUTURE IMPROVEMENT: Pre-2019 transaction data unavailable via API.
    These are estimated from draft vs final roster comparison.
    """
    from .espn_api_client import ESPNAPIClient

    client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        league = client.get_league(year)
    except Exception as e:
        log(f"  [TRANSACTIONS] Failed to load league for {year}: {e}")
        return None

    # Build draft map: player_id -> team_id
    draft_map = {}  # player_id -> drafting_team_id
    if league.draft:
        for pick in league.draft:
            pid = getattr(pick, "playerId", None)
            team = getattr(pick, "team", None)
            if pid and team:
                draft_map[pid] = team.team_id

    # Build final roster map: player_id -> team_id
    final_map = {}  # player_id -> final_team_id
    for team in league.teams:
        roster = getattr(team, "roster", []) or []
        for player in roster:
            pid = getattr(player, "playerId", None)
            if pid:
                final_map[pid] = team.team_id

    rows = []
    txn_counter = 0
    estimate_week = 6  # Attribute all pre-2019 transactions to week 6

    # Players on a different team than drafted = drop + add.
    # We cannot infer trades from roster snapshots alone — bidirectional
    # movement between two teams could be two independent waiver pickups
    # rather than an actual trade. All movements are recorded as drop/add.
    for pid, draft_team_id in draft_map.items():
        if pid in final_map and final_map[pid] != draft_team_id:
            final_team_id = final_map[pid]
            txn_counter += 1
            rows.append(
                _build_estimated_row(
                    ctx,
                    year,
                    estimate_week,
                    txn_counter,
                    team_id=draft_team_id,
                    player_id=pid,
                    txn_type="drop",
                    source="team",
                    destination="waivers",
                    league=league,
                )
            )
            txn_counter += 1
            rows.append(
                _build_estimated_row(
                    ctx,
                    year,
                    estimate_week,
                    txn_counter,
                    team_id=final_team_id,
                    player_id=pid,
                    txn_type="add",
                    source="waivers",
                    destination="team",
                    league=league,
                )
            )

    # Undrafted players on final roster = add
    for pid, final_team_id in final_map.items():
        if pid not in draft_map:
            txn_counter += 1
            rows.append(
                _build_estimated_row(
                    ctx,
                    year,
                    estimate_week,
                    txn_counter,
                    team_id=final_team_id,
                    player_id=pid,
                    txn_type="add",
                    source="freeagents",
                    destination="team",
                    league=league,
                )
            )

    # Drafted players not on any final roster = drop
    for pid, draft_team_id in draft_map.items():
        if pid not in final_map:
            txn_counter += 1
            rows.append(
                _build_estimated_row(
                    ctx,
                    year,
                    estimate_week,
                    txn_counter,
                    team_id=draft_team_id,
                    player_id=pid,
                    txn_type="drop",
                    source="team",
                    destination="waivers",
                    league=league,
                )
            )

    if not rows:
        return None

    df = pd.DataFrame(rows)
    log(
        f"  [TRANSACTIONS] {year} (estimated): {len(df)} transaction rows "
        f"(trades: {(df['transaction_type']=='trade').sum()}, "
        f"adds: {(df['transaction_type']=='add').sum()}, "
        f"drops: {(df['transaction_type']=='drop').sum()})"
    )
    return df


def _build_estimated_row(ctx, year, week, txn_id, team_id, player_id, txn_type, source, destination, league) -> dict:
    """Build a single estimated transaction row for pre-2019 data."""
    # Try to find player info from league roster data
    player_name = None
    position = None
    nfl_team = None

    for team in league.teams:
        for player in getattr(team, "roster", []) or []:
            if getattr(player, "playerId", None) == player_id:
                player_name = getattr(player, "name", "Unknown")
                position = getattr(player, "position", None)
                nfl_team = getattr(player, "proTeam", None)
                break
        if player_name:
            break

    # Also check draft for player info
    if not player_name and league.draft:
        for pick in league.draft:
            if getattr(pick, "playerId", None) == player_id:
                player_name = getattr(pick, "playerName", "Unknown")
                break

    if player_name:
        player_name = _normalize_dst_name(player_name)

    return {
        "transaction_id": f"est_{year}_{txn_id}",
        "year": year,
        "week": week,
        "timestamp": None,
        # Source/destination fields (None for non-trade rows)
        "source_manager": None,
        "source_manager_guid": None,
        "source_team_name": None,
        "source_franchise_id": None,
        "destination_manager": None,
        "destination_manager_guid": None,
        "destination_team_name": None,
        "destination_franchise_id": None,
        "manager": ctx.get_manager_name(team_id, team_name=ctx.get_team_name(team_id, year), year=year),
        "manager_guid": ctx.get_manager_guid(team_id, year=year),
        "team_key": str(team_id),
        "team_name": ctx.get_team_name(team_id, year),
        "franchise_id": ctx.get_franchise_id(team_id, year=year),
        "player": player_name or f"Player_{player_id}",
        "espn_player_id": player_id,
        "position": position,
        "nfl_team": nfl_team,
        "transaction_type": txn_type,
        "faab_bid": 0,
        "source_type": source,
        "destination": destination,
        "is_estimated": True,
        "platform": "espn",
        "league_id": str(ctx.get_league_id_for_year(year)),
    }


def fetch_espn_transactions(
    ctx: "ESPNContext", year: int, max_week: int | None = None, *, client=None, league=None
) -> pd.DataFrame | None:
    """
    Fetch transactions for a single year, choosing modern or legacy path.
    """
    if year >= 2019:
        return fetch_espn_transactions_modern(ctx, year, max_week=max_week, client=client, league=league)
    else:
        return fetch_espn_transactions_legacy(ctx, year)


def fetch_all_espn_transactions(ctx: "ESPNContext") -> pd.DataFrame:
    """
    Fetch transaction data for all years.

    Args:
        ctx: ESPNContext with year range
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    years = list(ctx.get_year_range())
    log(f"\n{'='*60}")
    log(f"Fetching ESPN transaction data ({len(years)} years)")
    log(f"{'='*60}")

    all_dfs = []

    with ThreadPoolExecutor(max_workers=min(3, len(years))) as executor:
        futures = {executor.submit(fetch_espn_transactions, ctx, year): year for year in years}
        for future in as_completed(futures):
            year = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                log(f"  [TRANSACTIONS] {year}: FAILED - {e}")

    if not all_dfs:
        log("[TRANSACTIONS] No transaction data found for any year")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)

    # Deduplicate by (transaction_id, espn_player_id, manager, transaction_type) --
    # ESPN transactions can appear in multiple scoring periods, causing duplicates.
    # Including transaction_type prevents collapsing add/drop pairs from the same txn.
    before_dedup = len(combined)
    dedup_cols = ["transaction_id", "espn_player_id", "manager", "transaction_type"]
    if all(c in combined.columns for c in dedup_cols):
        # Split: rows with valid espn_player_id use full key, NULL ids use name fallback
        mask_null_id = combined["espn_player_id"].isna()
        if mask_null_id.any():
            combined_with_id = combined[~mask_null_id].drop_duplicates(subset=dedup_cols, keep="first")
            combined_no_id = combined[mask_null_id].drop_duplicates(
                subset=["transaction_id", "player", "manager"], keep="first"
            )
            combined = pd.concat([combined_with_id, combined_no_id], ignore_index=True)
        else:
            combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
        removed = before_dedup - len(combined)
        if removed > 0:
            log(f"[TRANSACTIONS] Removed {removed:,} duplicate rows")

    log(f"[TRANSACTIONS] Total: {len(combined)} transaction rows across {len(all_dfs)} years")

    return combined
