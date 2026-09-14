"""
ESPN-NFL Data Merger

Merges ESPN roster data with NFLverse player stats.
Similar to sleeper_nfl_merge.py but uses ESPN-specific player IDs.

Two-layer matching strategy:
1. ESPN ID lookup via sleeper_nfl_player_map.espn_id (47% coverage)
2. Name + position match in super_table (50% additional)
3. Last-name-only 1:1 match for remaining ~3%

D/ST handled separately: match via proTeam abbreviation -> DEF-{franchise_id}

Usage:
    merged = merge_espn_nfl(espn_df, nfl_df, config=MergeConfig())
"""

import logging
from dataclasses import dataclass, field

from multi_league.data_fetchers.shared.name_utils import normalize_name as _shared_normalize_name, apply_name_aliases

import pandas as pd

logger = logging.getLogger(__name__)


def log(msg: str):
    logger.info(msg)
    print(msg)


# Reuse ABBREV_TO_FRANCHISE_ID from normalizer
from ..sleeper.sleeper_data_normalizer import ABBREV_TO_FRANCHISE_ID


@dataclass
class MergeConfig:
    """Configuration for the ESPN-NFL merge process."""

    fantasy_positions: set[str] = field(
        default_factory=lambda: {
            "QB",
            "RB",
            "WR",
            "TE",
            "K",
            "DEF",
            "FLEX",
            "OP",
        }
    )

    flex_positions: dict[str, list[str]] = field(
        default_factory=lambda: {
            "FLEX": ["RB", "WR", "TE"],
            "OP": ["QB", "RB", "WR", "TE"],
        }
    )

    name_aliases: dict[str, str] = field(
        default_factory=lambda: {
            "hollywood brown": "marquise brown",
            "scotty miller": "scott miller",
            "mitch trubisky": "mitchell trubisky",
            "bill belichick": "william belichick",
            "gabe davis": "gabriel davis",
        }
    )


# Cache for ESPN ID -> NFL_player_id mapping
_ESPN_NFL_MAP_CACHE: dict[str, str] | None = None


def get_espn_nfl_map() -> dict[str, str]:
    """
    Load ESPN ID -> NFL_player_id mapping.

    Uses the shared unified mapping (player_bio + legacy maps).

    Returns:
        Dict: espn_id -> NFL_player_id
    """
    global _ESPN_NFL_MAP_CACHE

    if _ESPN_NFL_MAP_CACHE is not None:
        return _ESPN_NFL_MAP_CACHE

    from multi_league.data_fetchers.shared.nfl_player_mapping import get_espn_to_nfl_map

    _ESPN_NFL_MAP_CACHE = get_espn_to_nfl_map()
    log(f"[NFL MERGE] Loaded {len(_ESPN_NFL_MAP_CACHE):,} ESPN->NFL mappings (player_bio + legacy)")
    return _ESPN_NFL_MAP_CACHE


def normalize_name(name: str, config: MergeConfig | None = None) -> str:
    """Normalize a player name for matching, with optional alias application.

    Delegates to the shared normalize_name in name_utils, then applies
    ESPN-specific name aliases from config.
    """
    result = _shared_normalize_name(name)
    if config and hasattr(config, "name_aliases") and config.name_aliases:
        result = apply_name_aliases(result, config.name_aliases)
    return result


def extract_last_name(name: str) -> str:
    """Extract last name, handling compound surnames."""
    if not name:
        return ""

    parts = name.split()
    if not parts:
        return ""

    compound_prefixes = {"st", "de", "la", "van", "von", "mc", "mac", "o"}

    if len(parts) >= 3 and parts[-2].lower() in compound_prefixes:
        return f"{parts[-2]} {parts[-1]}"

    return parts[-1]


def merge_espn_nfl(
    espn_df: pd.DataFrame,
    nfl_df: pd.DataFrame,
    config: MergeConfig | None = None,
) -> pd.DataFrame:
    """
    Merge ESPN roster data with NFLverse player stats.

    Args:
        espn_df: DataFrame from fetch_espn_rosters (player-week data)
        nfl_df: DataFrame from load_nfl_from_super_table (NFL stats)
        config: Optional merge configuration

    Returns:
        Merged DataFrame with NFL_player_id and player_week
    """
    if config is None:
        config = MergeConfig()

    if espn_df.empty:
        log("[NFL MERGE] ESPN data is empty, nothing to merge")
        return espn_df

    log(f"\n{'='*60}")
    log(f"[NFL MERGE] Merging {len(espn_df):,} ESPN rows with {len(nfl_df):,} NFL rows")
    log(f"{'='*60}")

    result = espn_df.copy()

    # Ensure NFL_player_id column exists
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    # === Separate D/ST from individual players ===
    def_mask = result["position"].fillna("").str.upper().isin(["DEF", "DST", "D/ST"]) | result[
        "fantasy_position"
    ].fillna("").str.upper().isin(["DEF", "D/ST"])

    individual_df = result[~def_mask].copy()
    def_df = result[def_mask].copy()

    # === Layer 1: ESPN ID direct lookup ===
    espn_map = get_espn_nfl_map()
    layer1_matched = 0

    if espn_map and "espn_player_id" in individual_df.columns:
        mask = individual_df["NFL_player_id"].isna()
        if mask.any():
            individual_df.loc[mask, "NFL_player_id"] = (
                individual_df.loc[mask, "espn_player_id"].astype(str).map(lambda x: espn_map.get(x))
            )
            layer1_matched = individual_df["NFL_player_id"].notna().sum()
            log(f"  [Layer 1] ESPN ID lookup: {layer1_matched:,} matched")

    # === Layer 2: Name + position match (vectorized) ===
    unmatched_mask = individual_df["NFL_player_id"].isna()
    layer2_matched = 0

    if unmatched_mask.any() and not nfl_df.empty:
        # Build NFL lookup DataFrame with normalized names
        nfl_keys = nfl_df[["player", "position", "year", "week", "NFL_player_id"]].copy()
        nfl_keys["_norm_name"] = nfl_keys["player"].fillna("").apply(lambda x: normalize_name(str(x), config))
        nfl_keys["_norm_pos"] = nfl_keys["position"].fillna("").str.upper()
        nfl_keys = nfl_keys[nfl_keys["_norm_name"].ne("") & nfl_keys["NFL_player_id"].notna()]
        nfl_keys = nfl_keys.drop_duplicates(subset=["_norm_name", "_norm_pos", "year", "week"], keep="first")

        # Build ESPN unmatched DataFrame with normalized names
        unmatched = individual_df.loc[unmatched_mask].copy()
        unmatched["_norm_name"] = unmatched["player"].fillna("").apply(lambda x: normalize_name(str(x), config))
        unmatched["_norm_pos"] = unmatched["position"].fillna("").str.upper()

        # Vectorized merge (note: merge resets index, so save original)
        original_idx = unmatched.index
        merged = unmatched.merge(
            nfl_keys[["_norm_name", "_norm_pos", "year", "week", "NFL_player_id"]].rename(
                columns={"NFL_player_id": "_nfl_id_layer2"}
            ),
            on=["_norm_name", "_norm_pos", "year", "week"],
            how="left",
        )

        # Apply matched IDs back using original index positions
        matched_mask = merged["_nfl_id_layer2"].notna()
        if matched_mask.any():
            layer2_matched = matched_mask.sum()
            individual_df.loc[original_idx[matched_mask.values], "NFL_player_id"] = merged.loc[
                matched_mask, "_nfl_id_layer2"
            ].values

        log(f"  [Layer 2] Name+position match: {layer2_matched:,} matched")

    # === Layer 3: Last name 1:1 fallback (vectorized) ===
    unmatched_mask = individual_df["NFL_player_id"].isna()
    layer3_matched = 0

    if unmatched_mask.any() and not nfl_df.empty:
        # Build NFL lastname lookup
        nfl_ln = nfl_df[["player", "position", "year", "week", "NFL_player_id"]].copy()
        nfl_ln["_norm_name"] = nfl_ln["player"].fillna("").apply(lambda x: normalize_name(str(x), config))
        nfl_ln["_last_name"] = nfl_ln["_norm_name"].apply(extract_last_name)
        nfl_ln["_norm_pos"] = nfl_ln["position"].fillna("").str.upper()
        nfl_ln = nfl_ln[nfl_ln["_last_name"].ne("") & nfl_ln["NFL_player_id"].notna()]

        # Count unique NFL_player_ids per (lastname, pos, year, week) - only keep 1:1 matches
        group_cols = ["_last_name", "_norm_pos", "year", "week"]
        nfl_ln_counts = nfl_ln.groupby(group_cols)["NFL_player_id"].nunique().reset_index()
        nfl_ln_counts = nfl_ln_counts[nfl_ln_counts["NFL_player_id"] == 1]

        if not nfl_ln_counts.empty:
            # Get the actual IDs for 1:1 matches
            nfl_ln_unique = nfl_ln.drop_duplicates(subset=group_cols, keep="first")
            nfl_ln_1to1 = nfl_ln_unique.merge(nfl_ln_counts[group_cols], on=group_cols, how="inner")

            # Build ESPN unmatched with last names
            unmatched = individual_df.loc[unmatched_mask].copy()
            unmatched["_norm_name"] = unmatched["player"].fillna("").apply(lambda x: normalize_name(str(x), config))
            unmatched["_last_name"] = unmatched["_norm_name"].apply(extract_last_name)
            unmatched["_norm_pos"] = unmatched["position"].fillna("").str.upper()

            # Vectorized merge (note: merge resets index, so save original)
            original_idx = unmatched.index
            merged = unmatched.merge(
                nfl_ln_1to1[["_last_name", "_norm_pos", "year", "week", "NFL_player_id"]].rename(
                    columns={"NFL_player_id": "_nfl_id_layer3"}
                ),
                on=["_last_name", "_norm_pos", "year", "week"],
                how="left",
            )

            matched_mask = merged["_nfl_id_layer3"].notna()
            if matched_mask.any():
                layer3_matched = matched_mask.sum()
                individual_df.loc[original_idx[matched_mask.values], "NFL_player_id"] = merged.loc[
                    matched_mask, "_nfl_id_layer3"
                ].values

        log(f"  [Layer 3] Last name 1:1 fallback: {layer3_matched:,} matched")

    # === Handle D/ST ===
    def_matched = 0
    if not def_df.empty:
        def_df = _merge_dst(def_df)
        def_matched = def_df["NFL_player_id"].notna().sum()
        log(f"  [D/ST] Matched {def_matched:,} defense records")

    # === Recombine ===
    result = pd.concat([individual_df, def_df], ignore_index=True)

    # === Build fallback NFL_player_id for unmatched ===
    still_missing = result["NFL_player_id"].isna()
    if still_missing.any():
        result.loc[still_missing, "NFL_player_id"] = "ESPN-" + result.loc[still_missing, "espn_player_id"].astype(str)
        log(f"  [Fallback] {still_missing.sum():,} unmatched -> ESPN-{{id}} format")

    # === Build player_week composite key ===
    result["player_week"] = (
        result["NFL_player_id"].fillna("").astype(str).str.strip()
        + "_"
        + result["year"].astype(str)
        + "_"
        + result["week"].astype(str)
    )

    # === Build player_year composite key ===
    result["player_year"] = (
        result["NFL_player_id"].fillna("").astype(str).str.strip() + "_" + result["year"].astype(str)
    )

    # === Summary ===
    total = len(result)
    real_matched = total - (result["NFL_player_id"].astype(str).str.startswith("ESPN-")).sum()
    pct = (real_matched / total * 100) if total > 0 else 0
    log("\n[NFL MERGE] Summary:")
    log(f"  Total rows: {total:,}")
    log(f"  Real NFL matches: {real_matched:,} ({pct:.1f}%)")
    log(f"  Layer 1 (ESPN ID): {layer1_matched:,}")
    log(f"  Layer 2 (Name+Pos): {layer2_matched:,}")
    log(f"  Layer 3 (Last name): {layer3_matched:,}")
    log(f"  D/ST: {def_matched:,}")
    log(f"  Fallback (ESPN-X): {(total - real_matched):,}")

    return result


def _merge_dst(def_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge D/ST records using team abbreviation -> franchise ID.

    ESPN D/ST player names are "{Mascot} D/ST" or "{Mascot} DST".
    We match by proTeam abbreviation -> DEF-{franchise_id}.
    """
    if def_df.empty:
        return def_df

    result = def_df.copy()

    # Ensure NFL_player_id column
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    # Match by nfl_team (proTeam abbreviation)
    team_col = "nfl_team" if "nfl_team" in result.columns else None
    if team_col is None:
        return result

    for idx, row in result.iterrows():
        team_abbr = str(row.get(team_col, "")).upper().strip()
        franchise_id = ABBREV_TO_FRANCHISE_ID.get(team_abbr)
        if franchise_id:
            result.at[idx, "NFL_player_id"] = f"DEF-{franchise_id}"
            result.at[idx, "position"] = "DEF"

    return result
