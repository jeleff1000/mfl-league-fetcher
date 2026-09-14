"""
Sleeper Transaction Data Fetcher

Fetches transaction data (waivers, trades, free agent pickups, drops) from Sleeper.
Output schema matches transactions_v2.py for downstream compatibility.

Output columns:
- transaction_id
- year, week
- timestamp, human_readable_timestamp
- status (complete, failed, pending)
- transaction_type (waiver, free_agent, trade)
- manager, manager_guid, team_name
- player (player name)
- sleeper_player_id
- faab_bid
- source_type (waivers, free_agent, team, trade)
- destination (team, waivers)

Sleeper transaction types:
- waiver: Player claimed off waivers
- free_agent: Free agent pickup
- trade: Trade between teams
- commissioner: Commissioner action

Usage:
    from sleeper_transactions import fetch_sleeper_transactions
    from sleeper_context import SleeperContext

    ctx = SleeperContext.load("path/to/sleeper_context.json")
    df = fetch_sleeper_transactions(ctx, year=2024)
"""

import logging
import sys
from pathlib import Path
from typing import Any
from datetime import datetime

import pandas as pd

_verbose = "--verbose" in sys.argv

from .sleeper_api_client import SleeperAPIClient
from .sleeper_player_cache import SleeperPlayerCache
from .sleeper_context import SleeperContext, YearFilter, resolve_years_to_fetch
from .sleeper_roster_identity import build_year_roster_map
from multi_league.core.date_utils import get_current_nfl_season_year

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function matching Yahoo script pattern."""
    logger.info(msg)
    print(msg)


def convert_timestamp(ts_millis: int) -> str:
    """
    Convert Sleeper timestamp (milliseconds since epoch) to human readable format.

    Args:
        ts_millis: Unix timestamp in milliseconds

    Returns:
        Human readable timestamp string
    """
    try:
        dt = datetime.fromtimestamp(ts_millis / 1000)
        return dt.strftime("%b %d %Y %I:%M:%S %p").upper()
    except (ValueError, TypeError, OSError):
        return "UNKNOWN"


def map_transaction_type(sleeper_type: str, adds: dict, drops: dict) -> str:
    """
    Map Sleeper transaction type to Yahoo-compatible type.

    Args:
        sleeper_type: Sleeper transaction type
        adds: Dict of added players
        drops: Dict of dropped players

    Returns:
        Transaction type string
    """
    if sleeper_type == "trade":
        return "trade"
    elif sleeper_type == "waiver":
        return "add"  # Waiver claim
    elif sleeper_type == "free_agent":
        return "add"  # Free agent pickup
    elif sleeper_type == "commissioner":
        return "commish"

    # Infer from adds/drops
    if adds and not drops:
        return "add"
    elif drops and not adds:
        return "drop"
    elif adds and drops:
        return "add/drop"

    return "unknown"


def map_source_type(sleeper_type: str) -> str:
    """Map Sleeper transaction type to source_type."""
    if sleeper_type == "waiver":
        return "waivers"
    elif sleeper_type == "free_agent":
        return "freeagents"
    elif sleeper_type == "trade":
        return "trade"
    else:
        return sleeper_type


class SleeperTransactionFetcher:
    """
    Fetch transaction data from Sleeper API.

    Transactions include waiver claims, free agent pickups, trades, and drops.
    Output matches Yahoo format for downstream compatibility.
    """

    def __init__(
        self,
        ctx: SleeperContext,
        client: SleeperAPIClient | None = None,
        player_cache: SleeperPlayerCache | None = None,
    ):
        """
        Initialize the transaction fetcher.

        Args:
            ctx: SleeperContext with league configuration
            client: Optional pre-configured API client
            player_cache: Optional pre-loaded player cache
        """
        self.ctx = ctx
        self.client = client or SleeperAPIClient()
        self.player_cache = player_cache or SleeperPlayerCache(ctx.cache_directory)

        # Ensure player cache is loaded
        self.player_cache.refresh_if_stale(self.client)

    def _normalize_def_player(self, player_id: str, player_name: str, nfl_team: str) -> tuple:
        """
        Normalize DEF/DST player names to "Team DST" format.

        DEF normalization triggers when:
        1. player_id is a valid NFL team abbreviation (e.g., "KC", "SF")
        2. Position from cache is DEF/DST/D/ST
        3. player_name already contains "DST"
        4. player_name is a full team name (e.g., "Kansas City Chiefs")

        Args:
            player_id: Sleeper player_id
            player_name: Player name from cache
            nfl_team: NFL team abbreviation

        Returns:
            Tuple of (normalized_player_name, normalized_nfl_team)
        """
        player_id_str = str(player_id).upper()

        # Check multiple conditions for DEF detection
        is_def_player = False

        # 1. player_id is a valid NFL team abbreviation
        if player_id_str in self.player_cache.VALID_NFL_TEAMS:
            is_def_player = True
            nfl_team = player_id_str

        # 2. Position from cache is DEF/DST
        if not is_def_player:
            position = self.player_cache.get_player_position(str(player_id))
            if position and position.upper() in ("DEF", "DST", "D/ST"):
                is_def_player = True

        # 3. player_name already contains "DST" - already normalized
        if player_name and "DST" in player_name.upper():
            # Already in DST format, but ensure consistency
            is_def_player = True

        # 4. player_name is a full team name (e.g., "Kansas City Chiefs")
        if not is_def_player and player_name:
            if player_name in self.player_cache.NFL_TEAM_SHORT_NAMES:
                is_def_player = True

        if is_def_player:
            # Normalize to "Team DST" format
            if player_id_str in self.player_cache.VALID_NFL_TEAMS:
                # Use player_id as team abbrev
                team_full_name = self.player_cache.TEAM_ABBREV_TO_NAME.get(player_id_str, player_id_str)
                short_name = self.player_cache.NFL_TEAM_SHORT_NAMES.get(team_full_name, player_id_str)
                return f"{short_name} DST", player_id_str

            if player_name and player_name in self.player_cache.NFL_TEAM_SHORT_NAMES:
                # player_name is full team name (e.g., "Kansas City Chiefs")
                short_name = self.player_cache.NFL_TEAM_SHORT_NAMES[player_name]
                return f"{short_name} DST", nfl_team

            # If already has DST suffix, ensure proper format
            if player_name and "DST" in player_name.upper():
                # Already formatted, return as-is
                return player_name, nfl_team

        return player_name, nfl_team

    def _get_roster_mappings(
        self,
        league_id: str,
        year: int | None = None,
        db=None,
        required_roster_ids: set[int] | None = None,
    ) -> dict[int, dict[str, str]]:
        """Backward-compatible wrapper retained for direct callers."""
        return build_year_roster_map(
            self.ctx,
            self.client,
            league_id,
            year=year or get_current_nfl_season_year(),
            db=db,
            required_roster_ids=required_roster_ids,
            log_fn=log,
        )

    @staticmethod
    def _collect_referenced_roster_ids(txn: dict[str, Any]) -> set[int]:
        """Collect every roster_id reference a transaction can carry."""
        roster_ids: set[int] = set()

        def _add(value: Any) -> None:
            try:
                if value is not None and str(value).strip() != "":
                    roster_ids.add(int(value))
            except (TypeError, ValueError):
                return

        for value in (txn.get("adds") or {}).values():
            _add(value)
        for value in (txn.get("drops") or {}).values():
            _add(value)
        for value in txn.get("roster_ids") or []:
            _add(value)
        for pick in txn.get("draft_picks") or []:
            _add(pick.get("roster_id"))
            _add(pick.get("owner_id"))
            _add(pick.get("previous_owner_id"))

        return roster_ids

    @staticmethod
    def _fallback_manager_info(roster_map: dict[int, dict[str, str]], roster_id: int | None) -> dict[str, str]:
        if roster_id is None:
            return {}
        if roster_id in roster_map:
            return roster_map[roster_id]
        synthetic_name = f"Team {int(roster_id)}"
        return {
            "manager_name": synthetic_name,
            "manager_guid": f"orp{int(roster_id):04d}00",
            "team_name": synthetic_name,
        }

    def _parse_transaction(
        self, txn: dict[str, Any], year: int, roster_map: dict[int, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Parse a single Sleeper transaction into output rows.

        One transaction can involve multiple players (trades, add/drops).
        Returns one row per player involved.

        Args:
            txn: Raw transaction from API
            year: Season year
            roster_map: roster_id -> manager info mapping

        Returns:
            List of transaction rows (one per player)
        """
        rows = []

        def _franchise_id(info: dict[str, Any]) -> str | None:
            guid = str(info.get("manager_guid") or "").strip()
            return guid if guid else None

        txn_id = txn.get("transaction_id", "")
        txn_type = txn.get("type", "unknown")
        status = txn.get("status", "unknown")
        created_ts = txn.get("created", 0)  # Milliseconds
        leg = txn.get("leg", 1)  # Week/round number

        # Get adds and drops
        adds = txn.get("adds") or {}  # player_id -> roster_id
        drops = txn.get("drops") or {}  # player_id -> roster_id

        # Get FAAB info if present
        waiver_budget = txn.get("waiver_budget", []) or []
        faab_bid = 0
        if waiver_budget:
            # Sum of all FAAB spent in this transaction
            faab_bid = sum(abs(wb.get("amount", 0)) for wb in waiver_budget)

        # Get roster IDs involved
        roster_ids = txn.get("roster_ids", []) or []

        # Map transaction type
        transaction_type = map_transaction_type(txn_type, adds, drops)
        source_type = map_source_type(txn_type)

        # Convert timestamp
        human_ts = convert_timestamp(created_ts)
        timestamp = str(created_ts // 1000) if created_ts else ""

        # Process adds
        for player_id, roster_id in adds.items():
            manager_info = self._fallback_manager_info(roster_map, roster_id)

            player_name = self.player_cache.get_player_name(str(player_id))
            nfl_team = self.player_cache.get_player_team(str(player_id))
            position = self.player_cache.get_player_position(str(player_id))

            # Handle DEF/DST transactions - normalize to "Team DST" format
            player_name, nfl_team = self._normalize_def_player(player_id, player_name, nfl_team)

            # Build source/destination fields for trade rows
            if txn_type == "trade":
                # The sender is whoever dropped this player
                sender_roster_id = drops.get(player_id)
                sender_info = self._fallback_manager_info(roster_map, sender_roster_id)
                source_mgr = sender_info.get("manager_name") or None
                source_guid = sender_info.get("manager_guid") or None
                source_team = sender_info.get("team_name") or None
                source_fid = _franchise_id(sender_info)
                dest_mgr = manager_info.get("manager_name", "Unknown")
                dest_guid = manager_info.get("manager_guid", "")
                dest_team = manager_info.get("team_name", "")
                dest_fid = _franchise_id(manager_info)
            else:
                source_mgr = None
                source_guid = None
                source_team = None
                source_fid = None
                dest_mgr = manager_info.get("manager_name", "Unknown")
                dest_guid = manager_info.get("manager_guid", "")
                dest_team = manager_info.get("team_name", "")
                dest_fid = _franchise_id(manager_info)

            rows.append(
                {
                    "transaction_id": txn_id,
                    "year": year,
                    "week": leg,
                    "timestamp": timestamp,
                    "human_readable_timestamp": human_ts,
                    "status": status,
                    "transaction_type": transaction_type,
                    "manager": manager_info.get("manager_name", "Unknown"),
                    "manager_guid": manager_info.get("manager_guid", ""),
                    "team_name": manager_info.get("team_name", ""),
                    "player": player_name,
                    "sleeper_player_id": str(player_id),
                    "nfl_team": nfl_team,
                    "position": position,
                    "faab_bid": faab_bid if txn_type == "waiver" else 0,
                    "source_type": source_type,
                    "destination": "team",
                    "source_manager": source_mgr,
                    "source_manager_guid": source_guid,
                    "source_team_name": source_team,
                    "source_franchise_id": source_fid,
                    "destination_manager": dest_mgr,
                    "destination_manager_guid": dest_guid,
                    "destination_team_name": dest_team,
                    "destination_franchise_id": dest_fid,
                }
            )

        # Process drops - but skip for trades and commissioner transactions
        # For trades, the adds dict already shows who received each player
        # Processing drops for trades would create duplicate rows with the OLD owner
        # Commissioner transactions also have drops that duplicate the adds with NULL identity fields
        if txn_type not in ("trade", "commissioner"):
            for player_id, roster_id in drops.items():
                manager_info = self._fallback_manager_info(roster_map, roster_id)

                player_name = self.player_cache.get_player_name(str(player_id))
                nfl_team = self.player_cache.get_player_team(str(player_id))
                position = self.player_cache.get_player_position(str(player_id))

                # Handle DEF/DST transactions - normalize to "Team DST" format
                player_name, nfl_team = self._normalize_def_player(player_id, player_name, nfl_team)

                # For add/drop transactions, we already have the add - check if this is a paired drop
                is_paired = player_id in adds or any(d_pid in adds for d_pid in drops.keys())

                rows.append(
                    {
                        "transaction_id": txn_id,
                        "year": year,
                        "week": leg,
                        "timestamp": timestamp,
                        "human_readable_timestamp": human_ts,
                        "status": status,
                        "transaction_type": "drop" if not is_paired else transaction_type,
                        "manager": manager_info.get("manager_name", "Unknown"),
                        "manager_guid": manager_info.get("manager_guid", ""),
                        "team_name": manager_info.get("team_name", ""),
                        "player": player_name,
                        "sleeper_player_id": str(player_id),
                        "nfl_team": nfl_team,
                        "position": position,
                        "faab_bid": 0,
                        "source_type": "team",
                        "destination": "waivers",
                        "source_manager": None,
                        "source_manager_guid": None,
                        "source_team_name": None,
                        "source_franchise_id": None,
                        "destination_manager": None,
                        "destination_manager_guid": None,
                        "destination_team_name": None,
                        "destination_franchise_id": None,
                    }
                )

        # Process draft picks in trades
        if txn_type == "trade":
            draft_pick_rows = self._parse_trade_picks(txn, year, roster_map)
            rows.extend(draft_pick_rows)

        return rows

    def _parse_trade_picks(
        self, txn: dict[str, Any], year: int, roster_map: dict[int, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Parse draft picks from a trade transaction.

        Sleeper stores draft pick trades in the 'draft_picks' field of trade transactions.
        Each pick has: season, round, roster_id (receiver), previous_owner_id (sender).

        Args:
            txn: Raw trade transaction from API
            year: Season year when the trade occurred
            roster_map: roster_id -> manager info mapping

        Returns:
            List of transaction rows (one per draft pick traded)
        """
        rows = []
        draft_picks = txn.get("draft_picks") or []

        if not draft_picks:
            return rows

        txn_id = txn.get("transaction_id", "")
        created_ts = txn.get("created", 0)
        leg = txn.get("leg", 1)

        # Convert timestamp
        human_ts = convert_timestamp(created_ts)
        timestamp = str(created_ts // 1000) if created_ts else ""

        for pick in draft_picks:
            pick_season = pick.get("season", str(year + 1))
            pick_round = pick.get("round", 1)
            # Sleeper API field meanings:
            # - roster_id: The ORIGINAL draft slot owner (whose pick it is)
            # - owner_id: Who CURRENTLY owns the pick (after the trade)
            # - previous_owner_id: Who owned the pick BEFORE the trade
            original_slot_roster_id = pick.get("roster_id")
            new_owner_roster_id = pick.get("owner_id")
            previous_owner_roster_id = pick.get("previous_owner_id")

            receiver_info = self._fallback_manager_info(roster_map, new_owner_roster_id)
            sender_info = self._fallback_manager_info(roster_map, previous_owner_roster_id)
            original_slot_info = self._fallback_manager_info(roster_map, original_slot_roster_id)

            # Descriptive player name: "2026 1st (from OriginalSlotOwner)"
            # This shows whose draft slot it is, making it clear which pick changed hands
            round_ord = {1: "1st", 2: "2nd", 3: "3rd"}.get(pick_round, f"{pick_round}th")
            original_slot_owner = original_slot_info.get("manager_name", "Unknown")
            player_desc = f"{pick_season} {round_ord} (from {original_slot_owner})"
            sender_guid = sender_info.get("manager_guid") or ""
            receiver_guid = receiver_info.get("manager_guid") or ""

            rows.append(
                {
                    "transaction_id": txn_id,
                    "year": year,
                    "week": leg,
                    "timestamp": timestamp,
                    "human_readable_timestamp": human_ts,
                    "status": "complete",
                    "transaction_type": "trade_pick",
                    "manager": receiver_info.get("manager_name", "Unknown"),
                    "manager_guid": receiver_info.get("manager_guid", ""),
                    "team_name": receiver_info.get("team_name", ""),
                    "player": player_desc,
                    "position": "PICK",
                    "sleeper_player_id": f"pick_{pick_season}_{pick_round}_{original_slot_roster_id}",
                    "nfl_team": None,
                    "faab_bid": 0,
                    "source_type": "trade",
                    "destination": "team",
                    "source_manager": sender_info.get("manager_name") or None,
                    "source_manager_guid": sender_guid or None,
                    "source_team_name": sender_info.get("team_name") or None,
                    "source_franchise_id": sender_guid if sender_guid else None,
                    "destination_manager": receiver_info.get("manager_name", "Unknown"),
                    "destination_manager_guid": receiver_guid,
                    "destination_team_name": receiver_info.get("team_name", ""),
                    "destination_franchise_id": receiver_guid if receiver_guid else None,
                    "traded_pick_season": int(pick_season) if pick_season is not None else None,
                    "traded_pick_round": int(pick_round) if pick_round is not None else None,
                    "traded_pick_original_owner": original_slot_owner,
                }
            )

        return rows

    def fetch_transactions_for_year(self, year: int, db=None, max_week: int | None = None) -> pd.DataFrame:
        """
        Fetch all transactions for a season.

        Args:
            year: Season year
            max_week: Optional inclusive transaction-week ceiling. Full imports
                retain the 22-week default; active-season refreshes pass their
                latest materialized week to avoid polling future weeks.

        Returns:
            DataFrame with transaction data
        """
        log(f"\nFetching Sleeper transactions for {year}")

        # Get league ID for year
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            log(f"No league ID found for year {year}")
            return pd.DataFrame()

        if _verbose:
            log(f"League ID: {league_id}")

        # Fetch up to 22 weeks to cover regular season (18) + playoffs (4).
        # Transactions are event-based, so an active refresh only needs the
        # season through its latest completed week.
        if max_week is None:
            max_week = 22
        else:
            max_week = int(max_week)
            if not 1 <= max_week <= 22:
                raise ValueError(f"max_week must be between 1 and 22, got {max_week}")

        log(f"Fetching weeks 1-{max_week}")

        # Fetch all weeks
        all_transactions: list[dict[str, Any]] = []
        referenced_roster_ids: set[int] = set()
        consecutive_empty_weeks = 0

        for week in range(1, max_week + 1):
            try:
                txns = self.client.get_league_transactions(league_id, week)

                if not txns:
                    consecutive_empty_weeks += 1
                    if consecutive_empty_weeks >= 5:
                        log("  5+ consecutive weeks with no transactions - stopping")
                        break
                    continue

                consecutive_empty_weeks = 0
                for txn in txns:
                    all_transactions.append(txn)
                    referenced_roster_ids.update(self._collect_referenced_roster_ids(txn))

                if _verbose:
                    log(f"  Week {week}: {len(txns)} transactions")

            except Exception as e:
                log(f"  Error fetching week {week}: {e}")
                continue

        if not all_transactions:
            log("No transactions found")
            return pd.DataFrame()

        roster_map = build_year_roster_map(
            self.ctx,
            self.client,
            league_id,
            year,
            db=db,
            required_roster_ids=referenced_roster_ids,
            log_fn=log,
        )
        log(f"Found {len(roster_map)} teams")

        parsed_transactions = []
        for txn in all_transactions:
            parsed_transactions.extend(self._parse_transaction(txn, year, roster_map))

        # Create DataFrame
        df = pd.DataFrame(parsed_transactions)

        # CRITICAL: Ensure sleeper_player_id is clean string without .0 suffix
        # Float-to-string conversion adds .0 suffix (e.g., 10790962.0 -> "10790962.0")
        if "sleeper_player_id" in df.columns:
            df["sleeper_player_id"] = df["sleeper_player_id"].astype(str).str.replace(r"\.0$", "", regex=True)

        # Sort by timestamp (newest first), then by transaction_id
        df = df.sort_values(["timestamp", "transaction_id"], ascending=[False, True])

        # Add composite keys for downstream compatibility
        df["player_year"] = df["sleeper_player_id"] + "_" + df["year"].astype(str)
        df["manager_year"] = df["manager"] + "_" + df["year"].astype(str)
        df["manager_week"] = df["manager"] + "_" + df["year"].astype(str) + "_" + df["week"].astype(str)
        df["player_week"] = df["sleeper_player_id"] + "_" + df["year"].astype(str) + "_" + df["week"].astype(str)

        log(f"[OK] {year}: {len(df)} transactions")
        if _verbose:
            log(f"  Adds: {(df['destination'] == 'team').sum()}")
            log(f"  Drops: {(df['destination'] == 'waivers').sum()}")
            log(f"  Trades: {(df['transaction_type'] == 'trade').sum()}")
            log(f"  Draft pick trades: {(df['transaction_type'] == 'trade_pick').sum()}")

        return df


def fetch_sleeper_transactions(
    ctx: SleeperContext,
    year: int,
    client: SleeperAPIClient | None = None,
    player_cache: SleeperPlayerCache | None = None,
    db=None,
    max_week: int | None = None,
) -> pd.DataFrame:
    """
    Main entry point for fetching Sleeper transaction data.

    Args:
        ctx: SleeperContext with league configuration
        year: Season year to fetch
        client: Optional pre-configured API client
        player_cache: Optional pre-loaded player cache
        db: LocalLeagueDB instance (required).

    Returns:
        DataFrame with transaction data
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    fetcher = SleeperTransactionFetcher(ctx, client, player_cache)

    df = fetcher.fetch_transactions_for_year(year, db=db, max_week=max_week)

    if df.empty:
        return df

    # Save output
    league_id = ctx.get_league_id_for_year(year) or ctx.league_id
    db.save_table("transactions", df, year=year, platform="sleeper", league_id=str(league_id))
    if _verbose:
        log(f"  [LocalDB] transactions year={year}: {len(df):,} rows")

    return df


def fetch_all_sleeper_transactions(
    ctx: SleeperContext,
    client: SleeperAPIClient | None = None,
    player_cache: SleeperPlayerCache | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> dict[int, pd.DataFrame]:
    """
    Fetch transaction data for all years in context range.

    Args:
        ctx: SleeperContext with league configuration
        client: Optional pre-configured API client
        player_cache: Optional pre-loaded player cache
        year_filter: If set, only fetch this year or these years (for quick imports)
        db: LocalLeagueDB instance (required).

    Returns:
        Dict mapping year to DataFrame
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    results = {}

    # If year_filter is set, only fetch that year/those years (quick import mode).
    # Otherwise prefer discovered league_ids over the payload start/end window.
    years_to_fetch = resolve_years_to_fetch(ctx, year_filter)

    for year in years_to_fetch:
        df = fetch_sleeper_transactions(
            ctx=ctx,
            year=year,
            client=client,
            player_cache=player_cache,
            db=db,
        )

        if not df.empty:
            results[year] = df

    return results


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper Transaction Data Fetcher")
    parser.add_argument("--context", required=True, help="Path to sleeper_context.json")
    parser.add_argument("--year", type=int, help="Specific year to fetch")
    parser.add_argument("--all-years", action="store_true", help="Fetch all years")

    args = parser.parse_args()

    ctx = SleeperContext.load(Path(args.context))

    if args.all_years:
        results = fetch_all_sleeper_transactions(ctx)
        for year, df in results.items():
            print(f"Year {year}: {len(df)} transactions")
    elif args.year:
        df = fetch_sleeper_transactions(ctx, args.year)
        print(f"\nFetched {len(df)} transactions")
        if not df.empty:
            print(df.head())
    else:
        # Default to current NFL season year
        current_year = get_current_nfl_season_year()
        df = fetch_sleeper_transactions(ctx, current_year)
        print(f"\nFetched {len(df)} transactions")
