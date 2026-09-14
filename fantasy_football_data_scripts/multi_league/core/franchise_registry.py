"""
Franchise Registry - Track Manager Sub-Franchises Across Years

Solves two problems:
1. Different people with same display name (e.g., two "David"s with different GUIDs)
2. Same person with multiple teams (e.g., Jay Dog owns "King Ads" AND "GreyWolf")

Key concepts:
- franchise_id: Stable identifier for career stat grouping (never changes)
- franchise_name: Display name, updates to most recent team name, only disambiguates when needed

Usage:
    from multi_league.core.franchise_registry import FranchiseRegistry

    # Discover franchises from data
    registry = FranchiseRegistry.from_data(matchup_df, draft_df)
    registry.save(data_dir / "franchise_config.json")

    # Apply to dataframes
    df = registry.apply_franchise_columns(df)
"""

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path

_verbose = "--verbose" in sys.argv

import pandas as pd

from .manager_identity import hidden_manager_guid_mask, hidden_manager_owner_id, is_hidden_manager_guid


def _safe_int_year(value) -> int:
    """
    Safely convert year value to integer.

    Handles various formats: int, float, '2013', '2013.0', 2013.0, etc.
    Returns 0 if conversion fails (shouldn't happen with valid data).
    """
    if pd.isna(value):
        return 0
    try:
        # Convert to float first (handles '2013.0'), then to int
        return int(float(value))
    except (ValueError, TypeError):
        return 0


def _names_are_similar(name_a: str, name_b: str) -> bool:
    """
    Check if two manager names plausibly belong to the same person.

    Returns True if:
    - One name contains the other (e.g., "armwood" / "armwood109")
    - SequenceMatcher ratio >= 0.6 (e.g., "Dave" / "David")

    Returns False for clearly different people (e.g., "John" / "Sarah").
    """
    from difflib import SequenceMatcher

    a = str(name_a).lower().strip()
    b = str(name_b).lower().strip()

    if not a or not b or a == "?" or b == "?":
        # Can't determine similarity — allow merge (benefit of the doubt)
        return True

    # Substring check: "armwood" in "armwood109" or vice versa
    if a in b or b in a:
        return True

    # Fuzzy similarity
    return SequenceMatcher(None, a, b).ratio() >= 0.6


@dataclass
class TeamSeason:
    """A single team-year instance within a franchise."""

    year: int
    team_name: str
    team_key: str = ""
    team_slot: int = 0
    wins: int = 0
    losses: int = 0
    points_for: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "TeamSeason":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Franchise:
    """
    A franchise = one continuous team lineage.

    One owner (manager_guid) can have multiple franchises (sub-franchises).
    The franchise_id stays stable; franchise_name updates to current team name.
    """

    franchise_id: str
    franchise_name: str  # Display name - updates to most recent
    owner_guid: str
    owner_name: str  # Yahoo display name (e.g., "David", "Jay Dog")
    team_history: list[TeamSeason] = field(default_factory=list)
    years_active: list[int] = field(default_factory=list)

    def add_season(self, season: TeamSeason):
        """Add a team-season to this franchise's history."""
        # Update existing season if same year
        for i, s in enumerate(self.team_history):
            if s.year == season.year:
                self.team_history[i] = season
                return

        self.team_history.append(season)
        if season.year not in self.years_active:
            self.years_active.append(season.year)
            self.years_active.sort()

    def get_current_team_name(self) -> str:
        """Get the most recent team name."""
        if not self.team_history:
            return self.owner_name
        most_recent = max(self.team_history, key=lambda s: s.year)
        return most_recent.team_name or self.owner_name

    def get_career_stats(self) -> dict:
        """Calculate career totals from team history."""
        return {
            "career_wins": sum(s.wins for s in self.team_history),
            "career_losses": sum(s.losses for s in self.team_history),
            "career_points": sum(s.points_for for s in self.team_history),
            "years_played": len(self.years_active),
        }

    def to_dict(self) -> dict:
        return {
            "franchise_id": self.franchise_id,
            "franchise_name": self.franchise_name,
            "owner_guid": self.owner_guid,
            "owner_name": self.owner_name,
            "years_active": self.years_active,
            "team_history": [s.to_dict() for s in self.team_history],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Franchise":
        history = [TeamSeason.from_dict(s) for s in d.pop("team_history", [])]
        years = d.pop("years_active", [])
        franchise = cls(
            franchise_id=d.get("franchise_id", ""),
            franchise_name=d.get("franchise_name", ""),
            owner_guid=d.get("owner_guid", ""),
            owner_name=d.get("owner_name", ""),
        )
        franchise.team_history = history
        franchise.years_active = years
        return franchise


class FranchiseRegistry:
    """
    Registry for tracking franchises across a league.

    Handles:
    - Different people with same display name (different GUIDs)
    - Same person with multiple teams (same GUID, different teams)
    - Team name changes across years (same franchise, updated name)
    """

    def __init__(self):
        self.franchises: dict[str, Franchise] = {}  # franchise_id -> Franchise
        self._config_path: Path | None = None

        # Lookup indices
        self._guid_team_to_franchise: dict[tuple[str, str], str] = {}  # (guid, team_name) -> franchise_id
        self._year_guid_team_to_franchise: dict[
            tuple[int, str, str], str
        ] = {}  # (year, guid, team_name) -> franchise_id

        # Hidden GUID merges: maps hidden_* pseudo-GUIDs to real GUIDs
        # This allows lookup by hidden GUID to find the merged franchise
        self._hidden_guid_merges: dict[str, str] = {}  # hidden_guid -> real_guid

        # Account-switch merges: maps real GUIDs to canonical GUIDs
        # When a person switches accounts (e.g., Sleeper armwood -> armwood109),
        # the old GUID maps to the new canonical GUID
        self._guid_merges: dict[str, str] = {}  # real_guid -> canonical_guid

        # External alias mappings persisted in franchise_config v1.2
        # Maps external manager string → {"franchise_id": str, "ignore": bool, ...}
        self.external_alias_mappings: dict = {}

    @classmethod
    def from_data(
        cls,
        matchup_df: pd.DataFrame,
        draft_df: pd.DataFrame = None,
        existing_config: Path = None,
        franchise_merges: list[dict] | None = None,
    ) -> "FranchiseRegistry":
        """
        Create/update registry from matchup and draft data.

        Args:
            matchup_df: DataFrame with manager_guid, manager, team_name, year columns
            draft_df: Optional DataFrame with team_key for slot extraction
            existing_config: Optional path to existing franchise_config.json to preserve manual edits
            franchise_merges: Optional list of {"display_name": str, "owner_ids": [str, ...]}
                merges from the wizard payload. Each entry folds extra owner_ids into the first owner_id,
                populating _guid_merges BEFORE row-scanning so duplicates collapse naturally.
        """
        registry = cls()

        # Load existing config if present (preserves manual linkages)
        if existing_config and existing_config.exists():
            try:
                registry = cls.load(existing_config)
                print(f"[Franchise] Loaded existing config with {len(registry.franchises)} franchises")
            except Exception as e:
                print(f"[Franchise] Could not load existing config: {e}")
                registry = cls()

        # Apply payload-driven merges before scanning rows
        if franchise_merges:
            for merge in franchise_merges:
                owner_ids = merge.get("owner_ids", [])
                if len(owner_ids) >= 2:
                    canonical = owner_ids[0]
                    for other in owner_ids[1:]:
                        registry._guid_merges[other] = canonical

        # Process data to discover/update franchises
        registry._discover_from_data(matchup_df, draft_df)

        return registry

    def _discover_from_data(self, matchup_df: pd.DataFrame, draft_df: pd.DataFrame = None):
        """Discover franchises from data."""
        if matchup_df is None or matchup_df.empty:
            print("[Franchise] No matchup data provided")
            return

        required_cols = ["manager_guid", "manager", "year"]
        if not all(col in matchup_df.columns for col in required_cols):
            print(f"[Franchise] Missing required columns: {required_cols}")
            return

        stale_placeholder_fids = [
            fid for fid, franchise in self.franchises.items() if is_hidden_manager_guid(franchise.owner_guid)
        ]
        if stale_placeholder_fids:
            for fid in stale_placeholder_fids:
                del self.franchises[fid]
            self._build_indices()
            print(
                f"[Franchise] Dropped {len(stale_placeholder_fids)} stale placeholder-GUID franchise(s) "
                "from existing config"
            )

        # Build team_key lookup from draft data if available
        team_key_lookup = {}  # (year, manager_guid, team_name) -> team_key
        if draft_df is not None and "team_key" in draft_df.columns:
            for _, row in draft_df[["year", "manager_guid", "team_key"]].drop_duplicates().iterrows():
                if pd.notna(row["manager_guid"]) and pd.notna(row["team_key"]):
                    # We need to correlate team_key to team_name
                    # For now, store by (year, guid, slot)
                    slot = self._extract_team_slot(row["team_key"])
                    team_key_lookup[(_safe_int_year(row["year"]), str(row["manager_guid"]), slot)] = str(
                        row["team_key"]
                    )

        # Step 1: Find all unique (manager_guid, team_name) combinations per year
        # Note: For hidden managers (GUID = '--'/'--hidden--'), we use manager name as the grouping key,
        # but disambiguate by team_name when multiple distinct teams share the same name.
        team_instances = []
        cols = ["manager_guid", "manager", "year"]
        if "team_name" in matchup_df.columns:
            cols.append("team_name")
        if "team_key" in matchup_df.columns:
            cols.append("team_key")

        deduped = matchup_df[cols].drop_duplicates()

        # Pre-scan: detect hidden managers that need team_name disambiguation.
        # If the same manager name has multiple distinct team_names in the same year
        # AND the GUID is missing, they're different people sharing a display name.
        _needs_team_disambig: set[str] = set()
        if "team_name" in deduped.columns:
            hidden_rows = deduped[hidden_manager_guid_mask(deduped["manager_guid"])]
            if not hidden_rows.empty:
                for mgr_name, grp in hidden_rows.groupby("manager"):
                    # Multiple team names in the same year = different people
                    for yr in grp["year"].dropna().unique():
                        yr_teams = grp[grp["year"] == yr]["team_name"].dropna().unique()
                        if len(yr_teams) > 1:
                            _needs_team_disambig.add(str(mgr_name))
                            break
            if _needs_team_disambig:
                print(f"[Franchise] Hidden managers needing team_name disambiguation: {_needs_team_disambig}")

        for _, row in deduped.iterrows():
            guid = row.get("manager_guid")
            manager_name = str(row.get("manager", ""))

            # Skip rows with truly empty/invalid manager names
            # NOTE: 'Unknown' is allowed through - it's a valid placeholder for deleted accounts
            # and will be assigned a hidden_unknown franchise ID to preserve historical data
            if not manager_name or manager_name in ("", "None", "N/A"):
                continue

            # For hidden managers (GUID = '--' or empty), use manager name as pseudo-GUID
            # This allows grouping the same person across years even without a real GUID
            if is_hidden_manager_guid(guid):
                if manager_name in _needs_team_disambig:
                    # Different people share this display name — include team_name
                    team = str(row.get("team_name", "")).strip()
                    guid = hidden_manager_owner_id(manager_name, team)
                else:
                    guid = hidden_manager_owner_id(manager_name)
            elif manager_name.lower() == "unknown" and not is_hidden_manager_guid(guid):
                # Sleeper "Unknown" managers: owner_id is valid but user account is deleted.
                # Use the full owner_id to create distinct franchise_ids so different unknown
                # managers don't get conflated into a single "hidden_unknown" franchise.
                guid = f"hidden_u_{str(guid)}"

            team_instances.append(
                {
                    "manager_guid": str(guid),
                    "manager": manager_name,
                    "year": _safe_int_year(row.get("year")),
                    "team_name": str(row.get("team_name", "")) if "team_name" in row else "",
                    "team_key": str(row.get("team_key", "")) if "team_key" in row else "",
                }
            )

        # Step 1.5: Auto-merge hidden GUIDs with real GUIDs
        # When external data (no manager_guid) is merged with Yahoo data (has manager_guid),
        # the same person gets different GUIDs. If a manager name has BOTH a hidden GUID
        # AND exactly one real GUID, merge them so they're treated as the same franchise.
        print("[Franchise] Checking for hidden GUID merges...")

        # Build map: manager_name -> (hidden_guids, real_guids)
        name_to_guids: dict[str, tuple[set[str], set[str]]] = defaultdict(lambda: (set(), set()))
        for inst in team_instances:
            manager_name = inst["manager"]
            guid = inst["manager_guid"]
            if guid.startswith("hidden_"):
                name_to_guids[manager_name][0].add(guid)
            else:
                name_to_guids[manager_name][1].add(guid)

        # Find merge candidates: manager names with hidden GUIDs AND exactly one real GUID
        guid_replacements: dict[str, str] = {}  # hidden_guid -> real_guid
        for manager_name, (hidden_guids, real_guids) in name_to_guids.items():
            if hidden_guids and len(real_guids) == 1:
                real_guid = next(iter(real_guids))
                for hidden_guid in hidden_guids:
                    guid_replacements[hidden_guid] = real_guid
                    if _verbose:
                        print(f"  [MERGE] '{manager_name}': {hidden_guid} -> {real_guid}")

        # Apply replacements to team_instances and save merge mapping
        if guid_replacements:
            merged_count = 0
            for inst in team_instances:
                old_guid = inst["manager_guid"]
                if old_guid in guid_replacements:
                    inst["manager_guid"] = guid_replacements[old_guid]
                    merged_count += 1
            # Save merge mapping for later lookup by hidden GUID
            self._hidden_guid_merges.update(guid_replacements)
            print(f"[Franchise] Merged {merged_count} instances across {len(guid_replacements)} hidden GUIDs")
        elif _verbose:
            print("[Franchise] No hidden GUID merges needed")

        # Step 1.5b: Apply payload-driven GUID merges
        # The wizard may have identified external data GUIDs to merge with real GUIDs
        if self._guid_merges:
            payload_merged_count = 0
            for inst in team_instances:
                old_guid = inst["manager_guid"]
                if old_guid in self._guid_merges:
                    inst["manager_guid"] = self._guid_merges[old_guid]
                    payload_merged_count += 1
            if payload_merged_count:
                print(
                    f"[Franchise] Merged {payload_merged_count} instances across {len(self._guid_merges)} payload GUIDs"
                )
            elif _verbose:
                print("[Franchise] No payload GUID merges needed")

        # Step 1.6: Chain hidden franchises across consecutive years
        # When a league has exactly 1 hidden franchise per year across consecutive years,
        # it's very likely the same person rotating through different team names (Yahoo
        # doesn't expose GUIDs for old/inactive accounts). Merge these into one franchise.
        hidden_per_year: dict[int, list[str]] = defaultdict(list)  # year -> [hidden_guids]
        for inst in team_instances:
            if inst["manager_guid"].startswith("hidden_"):
                if inst["manager_guid"] not in hidden_per_year[inst["year"]]:
                    hidden_per_year[inst["year"]].append(inst["manager_guid"])

        if hidden_per_year:
            # Find consecutive year ranges where each year has exactly 1 hidden franchise
            sorted_years = sorted(hidden_per_year.keys())
            chains: list[list[int]] = []
            current_chain: list[int] = []

            for y in sorted_years:
                if len(hidden_per_year[y]) == 1:
                    if not current_chain or y == current_chain[-1] + 1:
                        current_chain.append(y)
                    else:
                        if len(current_chain) >= 2:
                            chains.append(current_chain)
                        current_chain = [y]
                else:
                    if len(current_chain) >= 2:
                        chains.append(current_chain)
                    current_chain = []
            if len(current_chain) >= 2:
                chains.append(current_chain)

            # Merge each chain: all hidden GUIDs in the chain -> first year's hidden GUID
            # BUT only if names are plausibly the same person (prevents merging
            # different people who just happen to both have hidden GUIDs)
            chain_merges: dict[str, str] = {}
            for chain in chains:
                anchor_guid = hidden_per_year[chain[0]][0]
                anchor_name = next(
                    (
                        inst["manager"]
                        for inst in team_instances
                        if inst["manager_guid"] == anchor_guid and inst["year"] == chain[0]
                    ),
                    "?",
                )
                chain_names = [f"{chain[0]}:{anchor_name}"]
                for y in chain[1:]:
                    hg = hidden_per_year[y][0]
                    name = next(
                        (
                            inst["manager"]
                            for inst in team_instances
                            if inst["manager_guid"] == hg and inst["year"] == y
                        ),
                        "?",
                    )
                    chain_names.append(f"{y}:{name}")
                    # Name similarity guard: only chain if names are plausibly the same person
                    if _names_are_similar(anchor_name, name):
                        if hg != anchor_guid:
                            chain_merges[hg] = anchor_guid
                    else:
                        print(
                            f"[Franchise] Skipped chaining '{name}' ({y}) -> "
                            f"'{anchor_name}' (names too different, likely different people)"
                        )
                print(f"[Franchise] Chaining hidden franchises: {' -> '.join(chain_names)} (anchor: {anchor_guid})")

            if chain_merges:
                merged_count = 0
                for inst in team_instances:
                    if inst["manager_guid"] in chain_merges:
                        inst["manager_guid"] = chain_merges[inst["manager_guid"]]
                        merged_count += 1
                self._hidden_guid_merges.update(chain_merges)
                print(f"[Franchise] Chained {merged_count} instances across {len(chain_merges)} hidden GUIDs")

        # Step 1.7: Chain isolated single-year franchises to multi-year franchises
        # When a franchise appears in only 1 year, find which multi-year franchise
        # was absent that year and merge them (if unambiguous).
        # This handles both hidden managers and real-GUID managers that only appear once.
        guid_years: dict[str, set[int]] = defaultdict(set)
        for inst in team_instances:
            guid_years[inst["manager_guid"]].add(inst["year"])

        _all_league_years = sorted(set(inst["year"] for inst in team_instances))  # noqa: F841
        multi_year_guids = {g for g, yrs in guid_years.items() if len(yrs) > 1}
        single_year_guids = {g for g, yrs in guid_years.items() if len(yrs) == 1}

        if single_year_guids:
            linked_count = 0
            # Iterate over a copy since we modify single_year_guids during iteration
            for single_guid in list(single_year_guids):
                single_year = next(iter(guid_years[single_guid]))

                # Find multi-year franchises that are MISSING in single_year
                # but present in adjacent years (year-1 or year+1)
                candidates = []
                for multi_guid in multi_year_guids:
                    multi_years = guid_years[multi_guid]
                    if single_year not in multi_years:
                        # Check adjacency: was this franchise active in year-1 or year+1?
                        if (single_year - 1) in multi_years or (single_year + 1) in multi_years:
                            candidates.append(multi_guid)

                if len(candidates) == 1:
                    # Unambiguous slot match - but verify names are plausibly the same person
                    # to avoid false merges when a roster slot is reused by a different person
                    target_guid = candidates[0]
                    target_name = next(
                        (inst["manager"] for inst in team_instances if inst["manager_guid"] == target_guid), "?"
                    )
                    single_name = next(
                        (inst["manager"] for inst in team_instances if inst["manager_guid"] == single_guid), "?"
                    )

                    # Name similarity guard: skip merge if names are clearly different people
                    # Catches account switches like "armwood" -> "armwood109" while blocking
                    # roster slot reuse like "John" -> "Sarah"
                    names_compatible = _names_are_similar(single_name, target_name)

                    if not names_compatible:
                        print(
                            f"[Franchise] Skipped linking '{single_name}' ({single_year}) -> "
                            f"'{target_name}' (names too different, likely different people)"
                        )
                    else:
                        for inst in team_instances:
                            if inst["manager_guid"] == single_guid:
                                inst["manager_guid"] = target_guid

                        # Store the merge so get_franchise_id() can resolve old GUIDs
                        if not single_guid.startswith("hidden_"):
                            self._guid_merges[single_guid] = target_guid
                        else:
                            self._hidden_guid_merges[single_guid] = target_guid

                        guid_years[target_guid].add(single_year)
                        single_year_guids.discard(single_guid)

                        print(
                            f"[Franchise] Linked single-year '{single_name}' ({single_year}) -> "
                            f"'{target_name}' (unambiguous slot match, names compatible)"
                        )
                        linked_count += 1
                elif len(candidates) > 1:
                    single_name = next(
                        (inst["manager"] for inst in team_instances if inst["manager_guid"] == single_guid), "?"
                    )
                    cand_names = [
                        next((inst["manager"] for inst in team_instances if inst["manager_guid"] == c), "?")
                        for c in candidates
                    ]
                    print(
                        f"[Franchise] Could not auto-link '{single_name}' ({single_year}) - "
                        f"multiple candidates: {cand_names}. Use franchise_config.json to resolve."
                    )

            if linked_count:
                print(f"[Franchise] Step 1.7: Linked {linked_count} single-year franchise(s) to multi-year franchises")

        # Step 2: Determine which managers need disambiguation
        # - Same manager name, different GUIDs
        # - Same GUID, multiple team names (in same year)
        manager_to_guids = defaultdict(set)
        guid_year_to_teams = defaultdict(set)  # (guid, year) -> set of team_names

        for inst in team_instances:
            manager_to_guids[inst["manager"]].add(inst["manager_guid"])
            guid_year_to_teams[(inst["manager_guid"], inst["year"])].add(inst["team_name"])

        # Managers needing disambiguation: multiple GUIDs OR any GUID has multiple teams in a year
        guids_needing_disambiguation = set()
        for _manager, guids in manager_to_guids.items():
            if len(guids) > 1:
                # Different people with same name
                guids_needing_disambiguation.update(guids)

        for (guid, _year), teams in guid_year_to_teams.items():
            if len(teams) > 1:
                # Same person with multiple teams
                guids_needing_disambiguation.add(guid)

        if _verbose or guids_needing_disambiguation:
            print(f"[Franchise] GUIDs needing disambiguation: {len(guids_needing_disambiguation)}")

        # Step 3: Group instances to form franchises
        # - For hidden managers (pseudo-GUID starts with "hidden_"): group by GUID only
        #   This ensures same person with different team names = ONE franchise
        # - For real GUIDs with MULTIPLE SIMULTANEOUS TEAMS: group by (guid, team_name)
        #   This handles one person owning multiple teams at the same time
        # - For real GUIDs with ONE TEAM (name may change mid-season): group by GUID only
        #   This ensures team name changes within a year = ONE franchise (not separate)

        # First, identify which (GUID, year) combinations have SIMULTANEOUS teams
        # CRITICAL: A team name change mid-season is NOT two teams - it's one team
        # that renamed. We only split into separate franchises if two teams played
        # in the SAME WEEK for the same manager.
        #
        # Example: Ezra 2013 had "Big Macho Me" (weeks 1-10, 13-14) and
        # "Demaryius Tacktheritrix" (weeks 11-12). These never played the same week,
        # so it's ONE team that renamed mid-season, not two teams.
        guid_years_with_simultaneous_teams = set()  # Set of (guid, year) tuples

        # Query matchup_df directly for week-level data (team_instances doesn't have weeks)
        has_week_data = "week" in matchup_df.columns and "team_name" in matchup_df.columns

        if has_week_data:
            for (guid, year), teams in guid_year_to_teams.items():
                if len(teams) > 1:
                    # Multiple team names for this (guid, year) - check if simultaneous or name change
                    year_data = matchup_df[(matchup_df["manager_guid"] == guid) & (matchup_df["year"] == year)]
                    # Group by week and count distinct team names per week
                    week_team_counts = year_data.groupby("week")["team_name"].nunique()
                    if (week_team_counts > 1).any():
                        # Found a week with multiple teams - truly simultaneous teams
                        guid_years_with_simultaneous_teams.add((guid, year))
                        if _verbose:
                            print(f"  [SIMULTANEOUS] {guid[:12]} year {year}: {teams} (same week)")
                    elif _verbose:
                        # Different team names never appeared in same week - name change
                        print(f"  [NAME CHANGE] {guid[:12]} year {year}: {teams} (different weeks)")
        else:
            # No week data available - fall back to year-level (assume all multi-team = simultaneous)
            if _verbose:
                print("[Franchise] Warning: No week column - cannot distinguish name changes from simultaneous teams")
            for (guid, year), teams in guid_year_to_teams.items():
                if len(teams) > 1:
                    guid_years_with_simultaneous_teams.add((guid, year))

        if guid_years_with_simultaneous_teams and _verbose:
            print(f"[Franchise] Simultaneous multi-team years: {guid_years_with_simultaneous_teams}")

        # For years with multiple team names but NO simultaneous teams, log as name changes
        name_change_years = set()
        for (guid, year), teams in guid_year_to_teams.items():
            if len(teams) > 1 and (guid, year) not in guid_years_with_simultaneous_teams:
                name_change_years.add((guid, year))
        if name_change_years and _verbose:
            print(f"[Franchise] Mid-season name changes (treating as same team): {name_change_years}")

        guid_team_instances = defaultdict(list)
        for inst in team_instances:
            guid = inst["manager_guid"]
            year = inst["year"]

            # Hidden managers: group by GUID only (manager name is already encoded in the pseudo-GUID)
            if guid.startswith("hidden_"):
                key = (guid, "")  # Empty team_name groups all years together
            # ONLY for years where this person had SIMULTANEOUS teams: group by (guid, team_name)
            # This keeps true multi-team situations separate
            elif (guid, year) in guid_years_with_simultaneous_teams:
                key = (guid, inst["team_name"])
            # Single team (may have renamed mid-season): group by GUID only
            # Mid-season name changes = same franchise
            else:
                key = (guid, "")  # Empty team_name groups all years together
            guid_team_instances[key].append(inst)

        # Step 4: Create/update franchises
        # Track which franchises we've seen for this GUID to assign indices
        guid_to_franchise_count = defaultdict(int)

        for (guid, team_name), instances in guid_team_instances.items():
            # Check if this team already exists in registry
            existing_fid = self._guid_team_to_franchise.get((guid, team_name))

            # If lookup failed for a (guid, '') group, check if ANY franchise
            # already exists for this GUID. This prevents creating a spurious _2
            # franchise when the config has (guid, 'OldTeamName') but not (guid, '').
            if not existing_fid and team_name == "":
                for (g, _t), fid in self._guid_team_to_franchise.items():
                    if g == guid and fid in self.franchises:
                        existing_fid = fid
                        break

            if existing_fid and existing_fid in self.franchises:
                # Update existing franchise
                franchise = self.franchises[existing_fid]
            else:
                # Create new franchise
                guid_to_franchise_count[guid] += 1
                team_index = guid_to_franchise_count[guid]

                # Use full GUID to eliminate collision risk
                franchise_id = f"{guid}_{team_index}"
                owner_name = instances[0]["manager"]

                # Determine franchise_name based on disambiguation needs
                if guid in guids_needing_disambiguation:
                    # Get most recent team name for display
                    most_recent = max(instances, key=lambda x: x["year"])
                    current_team = most_recent["team_name"] or owner_name
                    franchise_name = f"{owner_name} - {current_team}"
                else:
                    franchise_name = owner_name

                franchise = Franchise(
                    franchise_id=franchise_id, franchise_name=franchise_name, owner_guid=guid, owner_name=owner_name
                )
                self.franchises[franchise_id] = franchise

            # Add seasons to franchise
            for inst in instances:
                team_key = inst.get("team_key", "")
                slot = self._extract_team_slot(team_key) if team_key else 0

                # Try to get season stats from matchup data
                year_mask = (matchup_df["manager_guid"] == guid) & (matchup_df["year"] == inst["year"])
                if "team_name" in matchup_df.columns and inst["team_name"]:
                    year_mask &= matchup_df["team_name"] == inst["team_name"]

                year_data = matchup_df[year_mask]

                wins = int(year_data["win"].sum()) if "win" in year_data.columns else 0
                losses = int(year_data["loss"].sum()) if "loss" in year_data.columns else 0
                points = float(year_data["team_points"].sum()) if "team_points" in year_data.columns else 0.0

                season = TeamSeason(
                    year=inst["year"],
                    team_name=inst["team_name"],
                    team_key=team_key,
                    team_slot=slot,
                    wins=wins,
                    losses=losses,
                    points_for=points,
                )
                franchise.add_season(season)

            # Update franchise_name to most recent (in case team name changed)
            if guid in guids_needing_disambiguation:
                current_team = franchise.get_current_team_name()
                franchise.franchise_name = f"{franchise.owner_name} - {current_team}"

        # Merge franchises for the same GUID that were spuriously split
        self._merge_same_guid_franchises()

        # Rebuild indices
        self._build_indices()

        print(f"[Franchise] Total franchises: {len(self.franchises)}")

    def _merge_same_guid_franchises(self):
        """Merge franchises sharing the same GUID when they don't have overlapping years.

        This catches cases where the same person got two franchise_ids due to
        team name changes across years. If they never had simultaneous teams
        (no year overlap), they should be one franchise.

        For multi-team owners (e.g., Exp with 2 teams in 2005-2006), we do
        PAIRWISE merges: the "main" franchise (most years) absorbs non-overlapping
        sub-franchises, while sub-franchises that overlap with each other stay separate.
        """
        guid_to_fids = defaultdict(list)
        for fid, f in self.franchises.items():
            guid_to_fids[f.owner_guid].append(fid)

        merged_count = 0
        for guid, fids in guid_to_fids.items():
            if len(fids) <= 1:
                continue

            # Sort by number of seasons (most first = "primary" franchise)
            sorted_fids = sorted(fids, key=lambda f: len(self.franchises[f].team_history), reverse=True)

            primary = sorted_fids[0]
            to_merge = []

            for secondary in sorted_fids[1:]:
                if secondary not in self.franchises:
                    continue  # Already merged
                primary_years = {s.year for s in self.franchises[primary].team_history}
                secondary_years = {s.year for s in self.franchises[secondary].team_history}

                if not (primary_years & secondary_years):
                    # No overlap with primary — safe to merge
                    to_merge.append(secondary)

            if to_merge:
                for secondary in to_merge:
                    self.merge_franchises(primary, secondary)
                merged_count += 1
                remaining = sum(1 for f in self.franchises.values() if f.owner_guid == guid)
                print(
                    f"  [MERGE] Merged {len(to_merge)+1} of {len(fids)} franchises for GUID {guid[:12]} -> {primary} ({remaining} remaining)"
                )

        if merged_count > 0:
            print(f"[Franchise] Merged {merged_count} GUID groups (pairwise, preserving simultaneous teams)")

    @staticmethod
    def _extract_team_slot(team_key: str) -> int:
        """Extract team slot from team_key (e.g., 461.l.836761.t.3 -> 3)."""
        if not team_key:
            return 0
        match = re.search(r"\.t\.(\d+)$", str(team_key))
        return int(match.group(1)) if match else 0

    def _build_indices(self):
        """Build lookup indices for fast franchise resolution."""
        self._guid_team_to_franchise.clear()
        self._year_guid_team_to_franchise.clear()

        # Count franchises per GUID to decide if (guid, '') fallback is safe
        guid_franchise_count = defaultdict(int)
        for _fid, franchise in self.franchises.items():
            guid_franchise_count[franchise.owner_guid] += 1

        for franchise_id, franchise in self.franchises.items():
            guid = franchise.owner_guid

            # Only set (guid, '') fallback for GUIDs with a SINGLE franchise.
            # GUIDs with multiple franchises (true simultaneous teams) must
            # require team_name for lookup — otherwise the last one iterated
            # wins non-deterministically and causes duplicate rows.
            if guid_franchise_count[guid] == 1:
                self._guid_team_to_franchise[(guid, "")] = franchise_id

            for season in franchise.team_history:
                # (guid, team_name) -> franchise_id
                key = (guid, season.team_name)
                self._guid_team_to_franchise[key] = franchise_id

                # (year, guid, team_name) -> franchise_id
                year_key = (season.year, guid, season.team_name)
                self._year_guid_team_to_franchise[year_key] = franchise_id

                # Only set year-level fallback for single-franchise GUIDs
                if guid_franchise_count[guid] == 1:
                    year_key_empty = (season.year, guid, "")
                    self._year_guid_team_to_franchise[year_key_empty] = franchise_id

    def get_franchise_id(
        self, manager_guid: str, team_name: str = None, year: int = None, manager_name: str = None
    ) -> str | None:
        """
        Look up franchise_id for a given manager/team/year.

        Args:
            manager_guid: Yahoo manager GUID
            team_name: Team name (optional but recommended)
            year: Year (optional, helps with year-specific lookup)
            manager_name: Manager name (used for hidden managers with placeholder GUIDs)
        """
        guid = str(manager_guid) if manager_guid and pd.notna(manager_guid) else ""
        team = str(team_name) if team_name and pd.notna(team_name) else ""

        # For hidden managers (GUID = '--'/'--hidden--' or empty), use manager name as pseudo-GUID
        if is_hidden_manager_guid(manager_guid):
            if manager_name and pd.notna(manager_name):
                hidden_guid = hidden_manager_owner_id(str(manager_name))
                # Check if this hidden GUID was merged with a real GUID
                if hidden_guid in self._hidden_guid_merges:
                    guid = self._hidden_guid_merges[hidden_guid]
                else:
                    # Try with team_name slug appended (used when multiple hidden
                    # managers share the same name and need disambiguation)
                    if team:
                        disambig_guid = hidden_manager_owner_id(str(manager_name), team)
                        if disambig_guid in self._hidden_guid_merges:
                            guid = self._hidden_guid_merges[disambig_guid]
                        else:
                            # Check if disambiguated version exists as a franchise owner
                            for fid, f in self.franchises.items():
                                if f.owner_guid == disambig_guid:
                                    guid = disambig_guid
                                    break
                            else:
                                guid = hidden_guid
                    else:
                        guid = hidden_guid
            else:
                return None
        elif manager_name and str(manager_name).lower() == "unknown" and not is_hidden_manager_guid(guid):
            # Sleeper "Unknown" managers with valid owner_id - use same hidden_u_ prefix
            # as in _discover_from_data to match the franchise_id that was created
            guid = f"hidden_u_{guid}"

        # Resolve account-switch merges (real GUID -> canonical GUID)
        if guid in self._guid_merges:
            guid = self._guid_merges[guid]

        # Try year-specific lookup first
        if year:
            safe_year = _safe_int_year(year)
            key = (safe_year, guid, team)
            if key in self._year_guid_team_to_franchise:
                return self._year_guid_team_to_franchise[key]
            # For hidden managers, also try with empty team_name
            if guid.startswith("hidden_"):
                key = (safe_year, guid, "")
                if key in self._year_guid_team_to_franchise:
                    return self._year_guid_team_to_franchise[key]

        # Fall back to (guid, team_name) lookup
        key = (guid, team)
        if key in self._guid_team_to_franchise:
            return self._guid_team_to_franchise[key]

        # For hidden managers, try with empty team_name
        if guid.startswith("hidden_"):
            key = (guid, "")
            if key in self._guid_team_to_franchise:
                return self._guid_team_to_franchise[key]

        # Last resort: find any franchise with this GUID
        for fid, franchise in self.franchises.items():
            if franchise.owner_guid == guid:
                return fid

        # FUZZY FALLBACK: Try fuzzy matching on manager name if provided
        # This catches cases where manager name has slight spelling variations
        if manager_name and pd.notna(manager_name):
            from difflib import SequenceMatcher

            best_match = None
            best_score = 0.0
            manager_lower = str(manager_name).lower().strip()

            for fid, franchise in self.franchises.items():
                owner_lower = str(franchise.owner_name).lower().strip() if franchise.owner_name else ""
                if owner_lower:
                    score = SequenceMatcher(None, manager_lower, owner_lower).ratio()
                    if score > 0.85 and score > best_score:  # 85% similarity threshold
                        best_match = fid
                        best_score = score

            if best_match:
                return best_match

        return None

    def get_franchise_name(self, franchise_id: str) -> str | None:
        """Get display name for a franchise."""
        if franchise_id and franchise_id in self.franchises:
            return self.franchises[franchise_id].franchise_name
        return None

    def apply_franchise_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add franchise_id and franchise_name columns to a DataFrame.

        Args:
            df: DataFrame with manager_guid OR manager column
                (plus optionally team_name, year for disambiguation)

        Strategy:
            1. If manager_guid available: use it as primary lookup key
            2. If only manager name: use fuzzy matching via get_franchise_id
            3. Fallback: leave franchise_id as None
        """
        if df.empty:
            return df

        has_guid = "manager_guid" in df.columns and df["manager_guid"].notna().any()
        has_manager = "manager" in df.columns and df["manager"].notna().any()

        if not has_guid and not has_manager:
            print("[Franchise] Warning: neither manager_guid nor manager column found")
            return df

        df = df.copy()

        # Normalize merged GUIDs so downstream code sees one consistent GUID
        if self._guid_merges and "manager_guid" in df.columns:
            df["manager_guid"] = (
                df["manager_guid"].astype(str).map(lambda g: self._guid_merges.get(g, g) if g and g != "nan" else g)
            )

        def lookup_franchise_id(row):
            return self.get_franchise_id(
                manager_guid=row.get("manager_guid"),
                team_name=row.get("team_name"),
                year=row.get("year"),
                manager_name=row.get("manager"),  # Used for fuzzy matching fallback
            )

        df["franchise_id"] = df.apply(lookup_franchise_id, axis=1)
        df["franchise_name"] = df["franchise_id"].map(lambda fid: self.get_franchise_name(fid))

        return df

    def get_orphan_teams(self) -> list[dict]:
        """
        Find teams that couldn't be linked to existing franchises.

        Returns list of (year, manager_guid, team_name) that need manual linking.
        """
        # TODO: Implement orphan detection for cross-year linking
        return []

    def calculate_roster_continuity(
        self,
        player_df: pd.DataFrame,
        year1: int,
        team1_guid: str,
        team1_name: str,
        year2: int,
        team2_guid: str,
        team2_name: str,
    ) -> float:
        """
        Calculate roster overlap between two team-years.

        Used to suggest linkages when team names change.
        Returns score 0.0 to 1.0 (1.0 = perfect overlap).
        """
        if player_df is None or player_df.empty:
            return 0.0

        # Get rosters for each year (week 1 or any week)
        team1_players = set()
        team2_players = set()

        if "yahoo_player_id" in player_df.columns:
            mask1 = (player_df["year"] == year1) & (player_df["manager_guid"] == team1_guid)
            if "team_name" in player_df.columns:
                mask1 &= player_df["team_name"] == team1_name
            team1_players = set(player_df[mask1]["yahoo_player_id"].dropna().unique())

            mask2 = (player_df["year"] == year2) & (player_df["manager_guid"] == team2_guid)
            if "team_name" in player_df.columns:
                mask2 &= player_df["team_name"] == team2_name
            team2_players = set(player_df[mask2]["yahoo_player_id"].dropna().unique())

        if not team1_players or not team2_players:
            return 0.0

        overlap = len(team1_players & team2_players)
        total = len(team1_players | team2_players)

        return overlap / total if total > 0 else 0.0

    def get_summary_df(self) -> pd.DataFrame:
        """Get summary DataFrame of all franchises."""
        rows = []
        for franchise in self.franchises.values():
            stats = franchise.get_career_stats()
            rows.append(
                {
                    "franchise_id": franchise.franchise_id,
                    "franchise_name": franchise.franchise_name,
                    "owner_name": franchise.owner_name,
                    "owner_guid": franchise.owner_guid[:16] + "...",
                    "years_active": len(franchise.years_active),
                    "first_year": min(franchise.years_active) if franchise.years_active else None,
                    "last_year": max(franchise.years_active) if franchise.years_active else None,
                    "career_wins": stats["career_wins"],
                    "career_losses": stats["career_losses"],
                    "career_points": round(stats["career_points"], 1),
                    "team_names": [s.team_name for s in franchise.team_history],
                }
            )
        return pd.DataFrame(rows)

    def save(self, path: Path):
        """Save franchise config to JSON."""
        path = Path(path)

        config = {
            "version": "1.2",
            "generated_at": pd.Timestamp.now().isoformat(),
            "total_franchises": len(self.franchises),
            "franchises": [f.to_dict() for f in self.franchises.values()],
            "hidden_guid_merges": self._hidden_guid_merges,  # Persist hidden merge mapping
            "guid_merges": self._guid_merges,  # Persist account-switch merge mapping
            "external_alias_mappings": self.external_alias_mappings,  # Persist external alias mappings
        }

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, default=str)

        self._config_path = path
        print(f"[Franchise] Saved config to {path}")

    @classmethod
    def load(cls, path: Path) -> "FranchiseRegistry":
        """Load franchise config from JSON."""
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Franchise config not found: {path}")

        with open(path, encoding="utf-8") as f:
            config = json.load(f)

        registry = cls()
        registry._config_path = path

        for f_dict in config.get("franchises", []):
            franchise = Franchise.from_dict(f_dict)
            registry.franchises[franchise.franchise_id] = franchise

        # Restore hidden GUID merge mapping (if present in config)
        registry._hidden_guid_merges = config.get("hidden_guid_merges", {})

        # Restore account-switch GUID merge mapping (if present in config)
        registry._guid_merges = config.get("guid_merges", {})

        # Restore external alias mappings (v1.2+)
        registry.external_alias_mappings = config.get("external_alias_mappings", {})

        registry._build_indices()
        print(f"[Franchise] Loaded {len(registry.franchises)} franchises from {path.name}")
        return registry

    def merge_franchises(self, source_id: str, target_id: str):
        """
        Manually merge two franchises (for correcting auto-discovery mistakes).

        Args:
            source_id: Franchise to merge FROM (will be deleted)
            target_id: Franchise to merge INTO
        """
        if source_id not in self.franchises:
            raise ValueError(f"Source franchise not found: {source_id}")
        if target_id not in self.franchises:
            raise ValueError(f"Target franchise not found: {target_id}")

        source = self.franchises[source_id]
        target = self.franchises[target_id]

        # Move all seasons to target
        for season in source.team_history:
            target.add_season(season)

        # Repoint any external_alias_mappings entries that targeted the deleted
        # source_id at the surviving target. Without this, persisted aliases
        # become orphans and identity_match.build_alias_set() silently drops
        # them — re-scans of the same external file then fall back to fuzzy
        # matching or unresolved instead of using the user's prior decisions.
        for ext_str, mapping in self.external_alias_mappings.items():
            if isinstance(mapping, dict) and mapping.get("franchise_id") == source_id:
                mapping["franchise_id"] = target_id

        # Remove source
        del self.franchises[source_id]

        # Rebuild indices
        self._build_indices()

        print(f"[Franchise] Merged {source.franchise_name} into {target.franchise_name}")

    def rename_franchise(self, franchise_id: str, new_name: str):
        """Manually override franchise display name."""
        if franchise_id not in self.franchises:
            raise ValueError(f"Franchise not found: {franchise_id}")

        self.franchises[franchise_id].franchise_name = new_name
        print(f"[Franchise] Renamed {franchise_id} to '{new_name}'")
