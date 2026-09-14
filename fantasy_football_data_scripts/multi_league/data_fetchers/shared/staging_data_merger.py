"""
Staging Data Merger

Merges externally uploaded data from Fly staging tables with API-fetched data.
This ensures data uploaded via the web UI flows through all transformations properly.

Usage:
    from multi_league.data_fetchers.staging_data_merger import merge_staging_data

    merge_staging_data(
        ctx=league_context,
        db=local_league_db,
        harmonize_dtypes_func=harmonize_dtypes,
        log_func=print
    )
"""

import os
import re
from collections.abc import Callable
import pandas as pd

from .clean_names import TEAM_ABBREV_TO_FRANCHISE_ID


def _blankish_mask(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip().str.lower()
    return series.isna() | text.isin({"", "none", "nan", "<na>"})


def _valid_key_mask(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for key in keys:
        if key not in df.columns:
            return pd.Series(False, index=df.index)
        mask &= ~_blankish_mask(df[key])
    return mask


def _drop_duplicates_preserve_null_keys(df: pd.DataFrame, subset: list[str], keep: str = "first") -> pd.DataFrame:
    """Drop duplicate populated keys without collapsing every NULL key together."""
    if not subset or df.empty or not all(key in df.columns for key in subset):
        return df

    order_col = "__staging_merge_order"
    while order_col in df.columns:
        order_col = f"_{order_col}"

    working = df.copy()
    working[order_col] = range(len(working))
    valid_mask = _valid_key_mask(working, subset)
    with_key = working[valid_mask].drop_duplicates(subset=subset, keep=keep)
    without_key = working[~valid_mask]
    return (
        pd.concat([with_key, without_key], ignore_index=True)
        .sort_values(order_col)
        .drop(columns=[order_col])
        .reset_index(drop=True)
    )


def _canonical_player_key(value) -> str:
    from multi_league.core.canonical_draft import external_draft_player_match_key

    return external_draft_player_match_key(value)


def _backfill_draft_player_identity_from_existing(
    staging_df: pd.DataFrame,
    existing_df: pd.DataFrame | None,
    log_func: Callable = print,
) -> pd.DataFrame:
    """Let external draft rows keep their CSV values while inheriting stable player IDs."""
    if (
        staging_df.empty
        or existing_df is None
        or existing_df.empty
        or "year" not in staging_df.columns
        or "player" not in staging_df.columns
        or "year" not in existing_df.columns
        or "player" not in existing_df.columns
    ):
        return staging_df

    # External draft sheets often carry the user-facing auction fields
    # (manager/player/cost) but omit stable slot keys such as round/pick.
    # Fly's delta merge uses year+round+pick as the draft identity, so backfill
    # those slot columns from the matched API row when the upload left them blank.
    backfill_cols = [
        "round",
        "pick",
        "pick_in_round",
        "draft_slot",
        "draft_slot_roster_id",
        "NFL_player_id",
        "yahoo_player_id",
        "sleeper_player_id",
        "espn_player_id",
        "position",
        "nfl_team_api",
    ]
    available_backfill_cols = [col for col in backfill_cols if col in existing_df.columns]
    if not available_backfill_cols:
        return staging_df

    out = staging_df.copy()
    existing = existing_df.copy()
    out["_identity_year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    existing["_identity_year"] = pd.to_numeric(existing["year"], errors="coerce").astype("Int64")
    out["_identity_player"] = out["player"].map(_canonical_player_key)
    existing["_identity_player"] = existing["player"].map(_canonical_player_key)

    source_cols = ["_identity_year", "_identity_player", "player", *available_backfill_cols]
    identity = existing[source_cols].dropna(subset=["_identity_year", "_identity_player"]).copy()
    counts = identity.groupby(["_identity_year", "_identity_player"], dropna=False).size().reset_index(name="_ct")
    identity = identity.merge(counts, on=["_identity_year", "_identity_player"], how="left")
    identity = identity[identity["_ct"] == 1].drop_duplicates(["_identity_year", "_identity_player"], keep="first")
    identity = identity.rename(columns={col: f"_existing_{col}" for col in ["player", *available_backfill_cols]})

    merged = out.merge(identity, on=["_identity_year", "_identity_player"], how="left")
    matched_mask = (
        merged["_existing_player"].notna()
        if "_existing_player" in merged.columns
        else pd.Series(False, index=merged.index)
    )
    matched = int(matched_mask.sum())
    if matched:
        for col in available_backfill_cols:
            existing_col = f"_existing_{col}"
            if existing_col not in merged.columns:
                continue
            if col not in merged.columns:
                merged[col] = None
            missing = _blankish_mask(merged[col])
            merged.loc[missing, col] = merged.loc[missing, existing_col]

        # Use the canonical display name from the already-linked API row. This
        # fixes external typos/aliases such as "Patrick Mahomes II" and
        # "Jacobi Myers" before downstream name-based joins run.
        merged["player"] = merged["_existing_player"].where(merged["_existing_player"].notna(), merged["player"])
        log_func(f"[STAGING] draft: backfilled player identity for {matched:,} row(s) from existing draft rows")

    drop_cols = [col for col in merged.columns if col.startswith("_existing_") or col.startswith("_identity_")]
    return merged.drop(columns=drop_cols)


def _read_external_rows(conn, table: str, db_name: str):
    """Prefer staging.conformed_<table> (DDL-shaped, post-schema-conform).
    Fall back to staging.staging_<table> for leagues that haven't gone through
    PHASE 1.7 yet (legacy path).

    Operates on a local DuckDB connection — used by PHASE 2.2 when reading
    schema_conformed external rows during in-process merge.
    """
    conformed = f"staging.conformed_{table}"
    raw = f"staging.staging_{table}"

    try:
        n = conn.execute(f"SELECT count(*) FROM {conformed} WHERE db_name=?", [db_name]).fetchone()[0]
    except Exception:
        n = 0
    if n > 0:
        return conn.execute(f"SELECT * FROM {conformed} WHERE db_name=?", [db_name]).df()
    try:
        return conn.execute(f"SELECT * FROM {raw} WHERE db_name=?", [db_name]).df()
    except Exception:
        return pd.DataFrame()


def _get_franchise_id_from_abbrev(abbrev: str) -> str:
    """
    Convert team abbreviation to franchise ID for DEF player_week format.

    Args:
        abbrev: Team abbreviation (e.g., "JAX", "LV", "BUF")

    Returns:
        Franchise ID as string (e.g., "27", "31", "17") or original abbrev if not found
    """
    if not abbrev or pd.isna(abbrev):
        return str(abbrev)

    abbrev_upper = str(abbrev).upper().strip()
    if abbrev_upper in TEAM_ABBREV_TO_FRANCHISE_ID:
        return str(TEAM_ABBREV_TO_FRANCHISE_ID[abbrev_upper])

    # Return original if not found (will be caught by validator)
    return abbrev_upper


def _normalize_def_records(df: pd.DataFrame, log_func: Callable = print) -> pd.DataFrame:
    """
    Normalize DEF (defense) records by generating NFL_player_id and player_week.

    Staging data bypasses yahoo_nfl_merge normalization, so DEF records may be missing
    these critical fields. This function generates them based on nfl_team.

    Uses franchise ID format (DEF-{franchise_id}) instead of abbreviation format
    (DEF-{abbrev}) for consistency with super_table joins.

    Args:
        df: Player DataFrame
        log_func: Logging function

    Returns:
        DataFrame with normalized DEF records
    """
    if df.empty:
        return df

    # Find position column - check multiple possible names
    position_col = None
    for col_name in ["position", "fantasy_position", "primary_position", "yahoo_position", "nfl_position"]:
        if col_name in df.columns:
            position_col = col_name
            break

    # Check required columns (position is flexible)
    required_cols = {"nfl_team", "year", "week"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        log_func(f"[STAGING-DEF] Missing columns for DEF normalization: {missing}")
        return df

    if position_col is None:
        log_func("[STAGING-DEF] No position column found, skipping DEF normalization")
        return df

    # Find DEF rows missing NFL_player_id
    def_mask = df[position_col].astype(str).str.upper().isin(["DEF", "DST", "D/ST"])

    if "NFL_player_id" not in df.columns:
        df["NFL_player_id"] = None

    missing_id_mask = def_mask & (
        df["NFL_player_id"].isna() | (df["NFL_player_id"].astype(str).isin(["", "None", "nan"]))
    )

    if not missing_id_mask.any():
        return df

    count = missing_id_mask.sum()
    log_func(f"[STAGING-DEF] Normalizing {count:,} DEF records with missing NFL_player_id...")

    # Generate NFL_player_id using franchise ID (e.g., "DEF-27" for JAX, "DEF-31" for LV)
    df.loc[missing_id_mask, "NFL_player_id"] = "DEF-" + df.loc[missing_id_mask, "nfl_team"].apply(
        _get_franchise_id_from_abbrev
    )

    # Generate player_week if missing
    if "player_week" not in df.columns:
        df["player_week"] = None

    missing_pw_mask = def_mask & (df["player_week"].isna() | (df["player_week"].astype(str).isin(["", "None", "nan"])))

    if missing_pw_mask.any():
        # Use pd.to_numeric to safely handle float strings like '2014.0'
        df.loc[missing_pw_mask, "player_week"] = (
            df.loc[missing_pw_mask, "NFL_player_id"].astype(str)
            + "_"
            + pd.to_numeric(df.loc[missing_pw_mask, "year"], errors="coerce").fillna(0).astype(int).astype(str)
            + "_"
            + pd.to_numeric(df.loc[missing_pw_mask, "week"], errors="coerce").fillna(0).astype(int).astype(str)
        )
        log_func(f"[STAGING-DEF] Generated player_week for {missing_pw_mask.sum():,} DEF records")

    # Fix existing DEF-{abbrev} player_week values to use franchise_id format
    # Pattern: DEF-{letters}_year_week -> DEF-{franchise_id}_year_week

    if "player_week" in df.columns:
        legacy_mask = df["player_week"].astype(str).str.match(r"^DEF-[A-Za-z]+_\d+_\d+$", na=False)
        if legacy_mask.any():
            legacy_count = legacy_mask.sum()

            def fix_legacy_player_week(pw):
                if pd.isna(pw):
                    return pw
                match = re.match(r"^DEF-([A-Za-z]+)_(\d+)_(\d+)$", str(pw))
                if match:
                    abbrev = match.group(1).upper()
                    year = match.group(2)
                    week = match.group(3)
                    fid = _get_franchise_id_from_abbrev(abbrev)
                    return f"DEF-{fid}_{year}_{week}"
                return pw

            df.loc[legacy_mask, "player_week"] = df.loc[legacy_mask, "player_week"].apply(fix_legacy_player_week)

            # Also fix NFL_player_id if it has legacy format
            if "NFL_player_id" in df.columns:
                legacy_id_mask = df["NFL_player_id"].astype(str).str.match(r"^DEF-[A-Za-z]+$", na=False)
                if legacy_id_mask.any():

                    def fix_legacy_nfl_id(nfl_id):
                        if pd.isna(nfl_id):
                            return nfl_id
                        match = re.match(r"^DEF-([A-Za-z]+)$", str(nfl_id))
                        if match:
                            abbrev = match.group(1).upper()
                            fid = _get_franchise_id_from_abbrev(abbrev)
                            return f"DEF-{fid}"
                        return nfl_id

                    df.loc[legacy_id_mask, "NFL_player_id"] = df.loc[legacy_id_mask, "NFL_player_id"].apply(
                        fix_legacy_nfl_id
                    )

            log_func(f"[STAGING-DEF] Fixed {legacy_count:,} legacy DEF player_week formats (abbrev -> franchise_id)")

    return df


def _backfill_nfl_player_ids(
    df: pd.DataFrame, log_func: Callable = print, motherduck_token: str | None = None
) -> pd.DataFrame:
    """
    Backfill missing NFL_player_id values using yahoo_player_id mappings.

    Staging data often has yahoo_player_id but missing NFL_player_id because
    it bypassed the Yahoo-NFL merge process. This function:
    1. Builds a mapping from yahoo_player_id -> NFL_player_id using rows that have both
    2. Also queries the global yahoo_nfl_player_map table in MotherDuck
    3. Applies mappings to fill in missing NFL_player_id values
    4. Regenerates player_week for newly matched rows

    Args:
        df: Player DataFrame with yahoo_player_id and possibly missing NFL_player_id
        log_func: Logging function
        motherduck_token: MotherDuck token for querying global map

    Returns:
        DataFrame with NFL_player_id backfilled where possible
    """
    if "yahoo_player_id" not in df.columns:
        return df

    # Ensure NFL_player_id column exists
    if "NFL_player_id" not in df.columns:
        df["NFL_player_id"] = None

    # Find rows that have valid NFL_player_id
    nfl_id_str = df["NFL_player_id"].astype(str)
    has_nfl_id = df["NFL_player_id"].notna() & ~nfl_id_str.isin(["None", "", "nan", "<NA>"])

    # Helper to safely convert to int (handles float strings like '5479.0')
    def safe_int(x):
        try:
            return int(float(str(x).replace(".0", "")))
        except (ValueError, TypeError):
            return None

    # Build mapping from yahoo_player_id -> NFL_player_id (local data)
    # Normalize keys to int for consistent lookup across all sources
    mapping_df = df.loc[has_nfl_id, ["yahoo_player_id", "NFL_player_id"]].drop_duplicates()
    mapping_df = mapping_df[mapping_df["yahoo_player_id"].notna()]
    yahoo_to_nfl = {
        safe_int(k): v
        for k, v in zip(mapping_df["yahoo_player_id"], mapping_df["NFL_player_id"])
        if safe_int(k) is not None
    }
    log_func(f"[STAGING-BACKFILL] Built {len(yahoo_to_nfl):,} yahoo->NFL mappings from local data")

    # Also query the global yahoo_nfl_player_map table in MotherDuck
    token = motherduck_token or os.environ.get("MOTHERDUCK_TOKEN", "")
    if token:
        try:
            from multi_league.core.db_reader import get_reader

            reader = get_reader()
            global_map = reader.query_df(
                """
                SELECT yahoo_player_id, NFL_player_id
                FROM public.yahoo_nfl_player_map
                """,
                database="___ops",
            )

            # Convert yahoo_player_id to int for consistent lookup (uses safe_int defined above)
            global_map["yahoo_player_id_int"] = global_map["yahoo_player_id"].apply(safe_int)
            global_map = global_map[global_map["yahoo_player_id_int"].notna()]

            # Merge with local mappings (local takes priority)
            global_dict = dict(zip(global_map["yahoo_player_id_int"], global_map["NFL_player_id"]))
            new_from_global = {k: v for k, v in global_dict.items() if k not in yahoo_to_nfl}
            yahoo_to_nfl.update(new_from_global)
            log_func(f"[STAGING-BACKFILL] Added {len(new_from_global):,} mappings from global yahoo_nfl_player_map")
        except Exception as e:
            log_func(f"[STAGING-BACKFILL] Could not query global map: {e}")

    # Find rows that need NFL_player_id filled
    missing_nfl = ~has_nfl_id & df["yahoo_player_id"].notna()
    missing_count = missing_nfl.sum()

    if missing_count == 0:
        log_func("[STAGING-BACKFILL] No rows need NFL_player_id backfill")
        return df

    # Apply mapping - convert yahoo_player_id to int for consistent lookup (uses safe_int)
    df.loc[missing_nfl, "NFL_player_id"] = df.loc[missing_nfl, "yahoo_player_id"].apply(
        lambda x: yahoo_to_nfl.get(safe_int(x)) if pd.notna(x) else None
    )

    # Count how many were filled
    still_missing_mask = df["NFL_player_id"].isna() | df["NFL_player_id"].astype(str).isin(["None", "", "nan", "<NA>"])
    filled = missing_count - (still_missing_mask & missing_nfl).sum()
    log_func(f"[STAGING-BACKFILL] Filled {filled:,}/{missing_count:,} missing NFL_player_id values from mappings")

    # For players still missing, try to match by name from nfl_player_stats_all
    still_need_match = missing_nfl & still_missing_mask
    if still_need_match.any() and token and "player" in df.columns:
        try:
            import duckdb

            backend = os.environ.get("DATABASE_BACKEND", "fly")

            # Get unique players that still need matching
            unmatched = df.loc[still_need_match, ["yahoo_player_id", "player", "position"]].drop_duplicates(
                subset=["yahoo_player_id"]
            )
            log_func(
                f"[STAGING-BACKFILL] Attempting name-match for {len(unmatched)} unmatched players from NFL data..."
            )

            new_mappings = []

            if backend == "fly":
                from multi_league.core.db_reader import get_reader

                reader = get_reader()
                for _, row in unmatched.iterrows():
                    player_name = str(row["player"]).strip()
                    position = str(row.get("position", "")).strip()

                    query = f"""
                        SELECT DISTINCT NFL_player_id, player, position
                        FROM nfl_historical.nfl_player_stats_all
                        WHERE player = '{player_name.replace("'", "''")}'
                    """
                    if position and position not in ["", "None", "nan"]:
                        query += f" AND position = '{position}'"
                    query += " LIMIT 1"

                    result = reader.query(query, database="___ops")
                    if result:
                        nfl_id = result[0]["NFL_player_id"]
                        yahoo_id = row["yahoo_player_id"]
                        df.loc[df["yahoo_player_id"] == yahoo_id, "NFL_player_id"] = nfl_id
                        new_mappings.append(
                            {
                                "yahoo_player_id": str(yahoo_id),
                                "NFL_player_id": nfl_id,
                                "yahoo_name": player_name,
                                "nfl_name": result[0]["player"],
                                "position": position,
                            }
                        )
            else:
                conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

                for _, row in unmatched.iterrows():
                    player_name = str(row["player"]).strip()
                    position = str(row.get("position", "")).strip()

                    # Search nfl_player_stats_all by exact name match
                    query = """
                        SELECT DISTINCT NFL_player_id, player, position
                        FROM nfl_historical.nfl_player_stats_all
                        WHERE player = ?
                    """
                    params = [player_name]

                    # Add position filter if available
                    if position and position not in ["", "None", "nan"]:
                        query += " AND position = ?"
                        params.append(position)

                    query += " LIMIT 1"

                    result = conn.execute(query, params).fetchdf()
                    if len(result) > 0:
                        nfl_id = result["NFL_player_id"].iloc[0]
                        yahoo_id = row["yahoo_player_id"]
                        df.loc[df["yahoo_player_id"] == yahoo_id, "NFL_player_id"] = nfl_id
                        new_mappings.append(
                            {
                                "yahoo_player_id": str(yahoo_id),
                                "NFL_player_id": nfl_id,
                                "yahoo_name": player_name,
                                "nfl_name": result["player"].iloc[0],
                                "position": position,
                            }
                        )

                conn.close()

            if new_mappings:
                log_func(f"[STAGING-BACKFILL] Matched {len(new_mappings)} players from NFL data by name")

                # Insert new mappings into yahoo_nfl_player_map for future use
                # Use DO NOTHING to never overwrite existing mappings (prevents name collision issues)
                try:
                    ops_conn = duckdb.connect(f"md:___ops?motherduck_token={token}")
                    for mapping in new_mappings:
                        ops_conn.execute(
                            """
                            INSERT INTO public.yahoo_nfl_player_map
                            (yahoo_player_id, NFL_player_id, match_layer, match_confidence, yahoo_name, nfl_name, position)
                            VALUES (?, ?, 3, 95.0, ?, ?, ?)
                            ON CONFLICT (yahoo_player_id) DO NOTHING
                        """,
                            [
                                mapping["yahoo_player_id"],
                                mapping["NFL_player_id"],
                                mapping["yahoo_name"],
                                mapping["nfl_name"],
                                mapping["position"],
                            ],
                        )
                    ops_conn.close()
                    log_func(f"[STAGING-BACKFILL] Saved {len(new_mappings)} new mappings to yahoo_nfl_player_map")
                except Exception as e:
                    log_func(f"[STAGING-BACKFILL] Could not save new mappings: {e}")

            conn.close()
        except Exception as e:
            log_func(f"[STAGING-BACKFILL] NFL data name-match failed: {e}")

    # ADDITIONAL FALLBACK: Position+Team inference for remaining unmatched (DEF, etc.)
    # This helps with edge cases where name matching fails but position/team is unique
    still_missing = df["NFL_player_id"].isna() | df["NFL_player_id"].astype(str).isin(["None", "", "nan", "<NA>"])
    if still_missing.any() and "position" in df.columns and "nfl_team" in df.columns and "year" in df.columns:
        # Try to infer from position+team combinations (especially useful for DEF)
        for idx in df[still_missing].index:
            pos = df.loc[idx, "position"]
            team = df.loc[idx, "nfl_team"]
            year = df.loc[idx, "year"]

            # DEF positions should use franchise ID format
            if pos in ("DEF", "DST", "D/ST"):
                if pd.notna(team) and team:
                    franchise_id = _get_franchise_id_from_abbrev(str(team))
                    df.loc[idx, "NFL_player_id"] = f"DEF-{franchise_id}"
                    df.loc[idx, "yahoo_player_id"] = f"DEF-{franchise_id}"
                    log_func(f"[STAGING-BACKFILL] Inferred DEF ID for {team} -> DEF-{franchise_id}")

    # Final count
    final_missing = df["NFL_player_id"].isna() | df["NFL_player_id"].astype(str).isin(["None", "", "nan", "<NA>"])
    total_filled = missing_count - (final_missing & missing_nfl).sum()
    if total_filled > filled:
        log_func(f"[STAGING-BACKFILL] Total filled: {total_filled:,}/{missing_count:,} (including name matches)")

    # Regenerate player_week for all newly filled rows
    if total_filled > 0 and "year" in df.columns and "week" in df.columns:
        newly_filled = (
            missing_nfl
            & df["NFL_player_id"].notna()
            & ~df["NFL_player_id"].astype(str).isin(["None", "", "nan", "<NA>"])
        )
        if newly_filled.any():
            # Use pd.to_numeric to safely handle float strings like '2014.0'
            df.loc[newly_filled, "player_week"] = (
                df.loc[newly_filled, "NFL_player_id"].astype(str)
                + "_"
                + pd.to_numeric(df.loc[newly_filled, "year"], errors="coerce").fillna(0).astype(int).astype(str)
                + "_"
                + pd.to_numeric(df.loc[newly_filled, "week"], errors="coerce").fillna(0).astype(int).astype(str)
            )
            log_func(f"[STAGING-BACKFILL] Regenerated player_week for {newly_filled.sum():,} rows")

    # For rows STILL missing NFL_player_id, regenerate player_week with YAHOO- prefix
    # This ensures a consistent format even for unmatched players
    if "year" in df.columns and "week" in df.columns and "yahoo_player_id" in df.columns:
        still_missing = df["NFL_player_id"].isna() | df["NFL_player_id"].astype(str).isin(["None", "", "nan", "<NA>"])
        has_yahoo_id = df["yahoo_player_id"].notna()
        needs_fallback = still_missing & has_yahoo_id

        if needs_fallback.any():
            # Use pd.to_numeric to safely handle float strings like '2014.0'
            df.loc[needs_fallback, "player_week"] = (
                "YAHOO-"
                + df.loc[needs_fallback, "yahoo_player_id"].astype(str).str.replace(".0", "", regex=False)
                + "_"
                + pd.to_numeric(df.loc[needs_fallback, "year"], errors="coerce").fillna(0).astype(int).astype(str)
                + "_"
                + pd.to_numeric(df.loc[needs_fallback, "week"], errors="coerce").fillna(0).astype(int).astype(str)
            )
            log_func(
                f"[STAGING-BACKFILL] Generated fallback player_week (YAHOO- prefix) for {needs_fallback.sum():,} unmatched rows"
            )

    return df


def merge_staging_data(
    ctx,
    db,
    harmonize_dtypes_func: Callable,
    log_func: Callable = print,
) -> dict[str, int | str]:
    """
    Merge staging data from Fly with API-fetched data in the local DB.

    Loads data uploaded via the web UI (stored in Fly staging tables at
    `${db_name}.staging.staging_*`) and merges it with API data already in
    the LocalLeagueDB. Merged data then flows through all transformations
    (LAMAR, keeper economics, etc.).

    Args:
        ctx: LeagueContext with paths to canonical files.
        db: LocalLeagueDB instance (must have .save_table(), .read_table(),
            .table_exists(), .row_count(), .execute_sql()).
        harmonize_dtypes_func: Function to harmonize DataFrame dtypes before concat.
        log_func: Logging function (default: print).

    Returns:
        - ``{"status": "no_staging"}`` when no staging tables exist on Fly.
        - ``{"<table_type>": <row_count or status_string>, ...}`` on success.

    Raises:
        RuntimeError: if reading staging data from Fly fails. Callers that set
            ``has_external_data=True`` should propagate; otherwise they may log
            and continue.
    """
    from multi_league.core.runtime_mode import is_corpus_mode

    if is_corpus_mode():
        return {"status": "no_staging"}

    from multi_league.data_fetchers.shared.staging_reader import read_staging_data

    stats: dict[str, int | str] = {}

    # Load staging data from Fly. Any failure here is a real error — raise.
    try:
        staging_data = read_staging_data(ctx.league_name)
    except Exception as e:
        raise RuntimeError(f"Staging read failed: {e}") from e

    if not staging_data:
        log_func("[STAGING] No staging tables found on Fly")
        return {"status": "no_staging"}

    log_func(f"[STAGING] Found staging data: {list(staging_data.keys())}")

    # Map staging table types to canonical local DB table names
    table_name_map = {
        "matchup": "matchup",
        "draft": "draft",
        "transactions": "transactions",
        "schedule": "schedule",
        "player": "player_fantasy",
    }

    # Define deduplication keys per table type
    # NOTE: transactions uses (transaction_id, yahoo_player_id) because a single
    # transaction can have multiple players (add/drop combos, trades with multiple players)
    # NOTE: matchup does preliminary dedup on manager name here, then more accurate
    # dedup using franchise_id happens in discover_franchises.py
    dedup_keys_map = {
        # NOTE: matchup uses team_name to distinguish when one manager owns multiple teams
        # (e.g., "Michael" owns both "Tig Ole Bitties" and "VD and CRABtree")
        "matchup": ["year", "week", "manager", "team_name"],
        "draft": ["year", "round", "pick"],
        "transactions": ["year", "transaction_id", "yahoo_player_id"],
        "schedule": ["year", "week", "team_id"],
        # CRITICAL: player_week is the primary dedup key, not yahoo_player_id
        # Historical NFL data (e.g., 1970 kickers) has no yahoo_player_id
        # Fallback to year+week+yahoo_player_id+manager if player_week doesn't exist
        "player": ["player_week"],  # Primary key - unique per player per week
    }

    # Infer platform from ctx for save_table normalization
    platform = getattr(ctx, "platform", None) or "yahoo"

    # Process each table type
    for table_type, table_name in table_name_map.items():
        if table_type not in staging_data or staging_data[table_type] is None:
            stats[table_type] = "not_in_staging"
            continue

        staging_df = staging_data[table_type]

        if staging_df.empty:
            log_func(f"[STAGING] {table_type}: Empty DataFrame in staging, skipping")
            stats[table_type] = "empty"
            continue

        log_func(f"[STAGING] Processing {table_type}: {len(staging_df):,} staging rows")

        # Coerce VARCHAR staging data to proper types BEFORE any merge/write.
        # Fly staging tables store everything as VARCHAR. Without this,
        # ArrowTypeError crashes when combining with API data (proper types).
        try:
            from multi_league.core.data_normalization import coerce_staging_dtypes

            staging_df = coerce_staging_dtypes(staging_df)
        except ImportError:
            pass

        # league_id reconciliation.
        #
        # Two legitimate cases for staging data carrying a foreign league_id:
        #   1. The user uploaded external CSV/Parquet through the wizard
        #      (ctx.has_external_data=True). The whole point of that flow is
        #      to ingest data from a different source; we normalize silently.
        #   2. The user is doing a same-league reimport — staging carries
        #      ctx.league_id and there's nothing to reconcile.
        #
        # The dangerous case is when staging data has a DIFFERENT league_id
        # AND has_external_data is False. That almost always means the user
        # accidentally uploaded league B's export while importing league A —
        # silently relabelling those rows to league A would corrupt league
        # A's history. Refuse and surface the mismatch.
        old_league_ids = staging_df["league_id"].unique().tolist() if "league_id" in staging_df.columns else []
        foreign_ids = [
            lid for lid in old_league_ids if pd.notna(lid) and str(lid).strip() and str(lid) != str(ctx.league_id)
        ]
        if foreign_ids and not getattr(ctx, "has_external_data", False):
            raise RuntimeError(
                f"Staging {table_type} contains foreign league_id(s) {foreign_ids} "
                f"that do not match this import's league_id ({ctx.league_id}). "
                "If this is intentional cross-league data, set has_external_data=True "
                "(the wizard does this automatically when you upload external files). "
                "Otherwise, the wrong export is most likely staged — clear staging "
                "and re-upload."
            )
        staging_df["league_id"] = ctx.league_id
        if foreign_ids:
            log_func(
                f"[STAGING] Normalized league_id in {table_type}: {foreign_ids} → {ctx.league_id} "
                f"(has_external_data=True)"
            )

        # Add data_source tag
        if "data_source" not in staging_df.columns:
            staging_df["data_source"] = "staging"

        # Normalize DEF records - generate NFL_player_id and player_week for defense positions
        # This is critical for staging data which bypasses yahoo_nfl_merge normalization
        if table_type == "player":
            staging_df = _normalize_def_records(staging_df, log_func)
        elif table_type == "draft":
            try:
                from multi_league.core.canonical_draft import clean_external_draft_player_fields

                staging_df = clean_external_draft_player_fields(staging_df)
            except Exception as e:
                log_func(f"[STAGING] draft: player field cleanup skipped: {e}")

        # External ingest transformations: column rename, manager_guid synthesis,
        # ignored-row drop. Only activates when ctx has external mappings.
        if getattr(ctx, "external_column_maps", None) or getattr(ctx, "external_identity_maps", None):
            from multi_league.external_ingest.apply_mappings import (
                ColumnMap,
                IdentityDecision,
                apply_mappings,
            )

            column_maps = [
                ColumnMap(table=cm["table"], column_map=cm["column_map"])
                for cm in (ctx.external_column_maps or [])
                if cm.get("table") == table_type
            ]
            identity_decisions = {
                mgr: IdentityDecision(
                    franchise_id=d.get("franchise_id"),
                    owner_guid=d.get("owner_guid"),
                    display_name=d.get("display_name") or d.get("manager_name"),
                    create_new=d.get("create_new", False),
                    ignore=d.get("ignore", False),
                )
                for mgr, d in (ctx.external_identity_maps or {}).items()
            }
            result = apply_mappings(
                staged_dfs={table_type: staging_df},
                column_maps=column_maps,
                identity_decisions=identity_decisions,
                # league_id is the platform-unique league key (Yahoo league_key /
                # Sleeper league_id / ESPN league_id). Using league_name here
                # would risk synthetic-guid collisions when two distinct leagues
                # share a slug. platform discriminates further across providers.
                league_db=ctx.league_id,
                platform=platform,
            )
            staging_df = result.dfs[table_type]
            if result.ignored_rows_dropped:
                log_func(f"[EXTERNAL_INGEST] {table_type}: dropped ignored rows {result.ignored_rows_dropped}")
            if result.unmapped_managers:
                log_func(f"[EXTERNAL_INGEST] {table_type}: unmapped managers {result.unmapped_managers}")

        # Determine which years staging actually touches. Other years stay in
        # local DB untouched — re-saving them through save_table+normalizer
        # would silently strip post-quick-import enrichment columns (e.g.,
        # player_lamar, manager_lamar, optimal_player) that only the canonical
        # raw schema's RAW_COLUMNS survive. For KMFFL specifically, the quick
        # import populates 2025 with full enrichments and PHASE 0.4 keeps it
        # in local DuckDB across the quick→full handoff; this scoping ensures
        # the merger doesn't blow that work away just because staging carries
        # 2013/2014 rows.
        staging_years: list = []
        if "year" in staging_df.columns:
            staging_years = pd.to_numeric(staging_df["year"], errors="coerce").dropna().astype(int).unique().tolist()
            staging_years = sorted(set(staging_years))
        if not staging_years:
            log_func(
                f"[STAGING] {table_type}: no resolvable year column in staging — falling back to whole-table merge"
            )

        # Read existing data from local DB, scoped to staging-touched years when possible.
        existing_df = None
        if db.table_exists(table_name) and db.row_count(table_name) > 0:
            try:
                if staging_years:
                    years_csv = ", ".join(str(y) for y in staging_years)
                    existing_df = (
                        db.connect().execute(f"SELECT * FROM public.{table_name} WHERE year IN ({years_csv})").fetchdf()
                    )
                    log_func(
                        f"[STAGING] {table_type}: scoped read to staging years {staging_years} "
                        f"({len(existing_df):,} existing rows; other years untouched)"
                    )
                else:
                    existing_df = db.read_table(table_name)
            except Exception as e:
                log_func(f"[STAGING] Error reading existing {table_type} from local DB: {e}")
                stats[table_type] = f"read_error: {e}"
                continue

        def _persist_merged(result_df: pd.DataFrame, _table_name=table_name, _years=tuple(staging_years)):
            """Delete and re-save only staging-touched years; leave other years intact."""
            if _years:
                years_csv = ", ".join(str(y) for y in _years)
                db.execute_sql(f"DELETE FROM public.{_table_name} WHERE year IN ({years_csv})")
                # Filter result_df to only the staging-touched years, in case
                # earlier dedup pulled in stray rows from another year.
                if "year" in result_df.columns:
                    year_int = pd.to_numeric(result_df["year"], errors="coerce")
                    result_df = result_df[year_int.isin(_years)].copy()
            else:
                db.execute_sql(f"DELETE FROM public.{_table_name}")
            db.save_table(_table_name, result_df, platform=platform, league_id=ctx.league_id)
            log_func(f"  [STAGING] Saved {_table_name}: {len(result_df):,} rows into local DB")

        # Merge with existing fetched data if it exists
        if existing_df is not None and not existing_df.empty:
            try:
                yahoo_df = existing_df
                log_func(f"[STAGING] Existing {table_type}: {len(yahoo_df):,} rows from local DB")

                # Add data_source tag to Yahoo data
                if "data_source" not in yahoo_df.columns:
                    yahoo_df["data_source"] = "yahoo"

                if table_type == "draft":
                    staging_df = _backfill_draft_player_identity_from_existing(staging_df, yahoo_df, log_func)

                # Harmonize dtypes before concat
                yahoo_df, staging_df = harmonize_dtypes_func(yahoo_df, staging_df)

                # Concatenate - filter out empty DataFrames to avoid FutureWarning
                # Staging data comes FIRST so it takes priority in deduplication
                dfs_to_concat = [df for df in [staging_df, yahoo_df] if not df.empty]
                if not dfs_to_concat:
                    log_func(f"[STAGING] {table_type}: Both DataFrames are empty, skipping")
                    stats[table_type] = "both_empty"
                    continue
                combined_df = pd.concat(dfs_to_concat, ignore_index=True)

                # Deduplicate based on table-specific keys
                # None means dedup is deferred to a later pipeline stage (e.g., discover_franchises)
                dedup_keys_config = dedup_keys_map.get(table_type)

                if dedup_keys_config is None:
                    # Dedup deferred to later stage - skip here
                    log_func(f"[STAGING] {table_type}: Dedup deferred to transformation stage (franchise_id needed)")
                    dedup_keys = []
                else:
                    # Filter to keys that actually exist in the DataFrame
                    dedup_keys = [k for k in dedup_keys_config if k in combined_df.columns]

                if dedup_keys:
                    before_len = len(combined_df)
                    # Prefer staging data over Yahoo (keep='first', staging data is first)
                    # This ensures externally uploaded data takes precedence.
                    #
                    # CRITICAL: when the dedup key column has NULL values (e.g.,
                    # `player_week` is NULL on freshly-fetched Yahoo rosters
                    # because resolve_all_nfl_player_ids/ensure_player_week
                    # haven't run yet), pandas.drop_duplicates treats every NULL
                    # as equal and collapses all those rows to one. Split the
                    # frame so rows with a populated key dedup normally, and
                    # rows with a NULL key fall back to the documented
                    # composite (year+week+yahoo_player_id+manager).
                    primary_key = dedup_keys[0] if len(dedup_keys) == 1 else None
                    if table_type == "draft":
                        combined_df = _drop_duplicates_preserve_null_keys(combined_df, dedup_keys, keep="first")
                    elif primary_key is not None and primary_key in combined_df.columns:
                        key_str = combined_df[primary_key].astype(str).str.strip()
                        valid_mask = combined_df[primary_key].notna() & (key_str != "") & (key_str.str.lower() != "nan")
                        with_key = combined_df[valid_mask]
                        without_key = combined_df[~valid_mask]
                        with_key = with_key.drop_duplicates(subset=dedup_keys, keep="first")
                        fallback_keys = [
                            k for k in ["year", "week", "yahoo_player_id", "manager"] if k in without_key.columns
                        ]
                        if fallback_keys and not without_key.empty:
                            without_key = without_key.drop_duplicates(subset=fallback_keys, keep="first")
                        combined_df = pd.concat(
                            [df for df in [with_key, without_key] if not df.empty],
                            ignore_index=True,
                        )
                    else:
                        combined_df = combined_df.drop_duplicates(subset=dedup_keys, keep="first")
                    removed = before_len - len(combined_df)
                    if removed > 0:
                        log_func(f"[STAGING] Removed {removed:,} duplicate rows from {table_type} (kept staging data)")

                    # Secondary dedup for draft: a player can only be drafted once per year.
                    # Position-based dedup (year+round+pick) catches exact slot dupes,
                    # but if staging data has the same player at a different position
                    # (e.g., different round/pick numbering), both survive.
                    if table_type == "draft":
                        for dedup_col, label in [
                            ("NFL_player_id", "NFL_player_id"),
                            ("yahoo_player_id", "yahoo_player_id"),
                            ("player", "player name"),
                        ]:
                            if dedup_col not in combined_df.columns:
                                continue
                            before_sec = len(combined_df)
                            combined_df = _drop_duplicates_preserve_null_keys(
                                combined_df,
                                ["year", dedup_col],
                                keep="first",
                            )
                            removed_sec = before_sec - len(combined_df)
                            if removed_sec > 0:
                                log_func(
                                    f"[STAGING] Removed {removed_sec:,} draft duplicates by {label} (same player, different position)"
                                )

                    # Secondary dedup for player: same player-week-manager can have
                    # different player_week values between staging and Yahoo data
                    # (staging constructs player_week differently).
                    if table_type == "player":
                        sec_keys = ["yahoo_player_id", "year", "week", "manager"]
                        if all(k in combined_df.columns for k in sec_keys):
                            before_sec = len(combined_df)
                            combined_df = combined_df.drop_duplicates(subset=sec_keys, keep="first")
                            removed_sec = before_sec - len(combined_df)
                            if removed_sec > 0:
                                log_func(
                                    f"[STAGING] Removed {removed_sec:,} player duplicates by yahoo_player_id+year+week+manager"
                                )

                # For player data: backfill NFL_player_id from existing mappings
                # This matches staging players to NFL IDs using yahoo_player_id
                if table_type == "player" and "yahoo_player_id" in combined_df.columns:
                    combined_df = _backfill_nfl_player_ids(combined_df, log_func, None)

                # Write merged data
                _persist_merged(combined_df)
                stats[table_type] = len(combined_df)
                log_func(f"[STAGING] Merged {table_type}: {len(combined_df):,} total rows (existing + staging)")

            except Exception as e:
                log_func(f"[STAGING] Error merging {table_type}: {e}")
                stats[table_type] = f"merge_error: {e}"
        else:
            # No Yahoo data, just write staging data
            # But still run normalization and backfill to populate NFL_player_id
            if table_type == "player":
                staging_df = _normalize_def_records(staging_df, log_func)
                if "yahoo_player_id" in staging_df.columns:
                    staging_df = _backfill_nfl_player_ids(staging_df, log_func, None)

            try:
                _persist_merged(staging_df)
                stats[table_type] = len(staging_df)
                log_func(f"[STAGING] Wrote {table_type}: {len(staging_df):,} staging rows (no Yahoo data)")
            except Exception as e:
                log_func(f"[STAGING] Error writing {table_type}: {e}")
                stats[table_type] = f"write_error: {e}"

    log_func("[STAGING] All staging data merged successfully")
    return stats
