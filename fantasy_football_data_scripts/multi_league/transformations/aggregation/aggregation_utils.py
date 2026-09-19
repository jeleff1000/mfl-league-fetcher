"""Shared utilities for aggregation modules.

Consolidates duplicated helpers (column detection, db name resolution,
logging, centralized-DB table refs) that were previously copy-pasted
across 6+ aggregation files.

Centralized model
-----------------
All aggregate tables now live in ``___leagues.public.*`` with a ``db_name``
column identifying the league. Helper functions ``central_table()`` and
``league_db_filter()`` produce the scoped SQL fragments used across
every aggregation module.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def make_logger(prefix: str):
    """Create a prefixed logger for an aggregation module.

    Usage:
        log = make_logger("FANTASY-AGG")
        log("Processing 10 managers...")
        # => [18:42:13] [FANTASY-AGG] Processing 10 managers...
    """

    def _log(msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] [{prefix}] {msg}", flush=True)

    return _log


# Module-level logger for this file
log = make_logger("AGG-UTILS")


# ---------------------------------------------------------------------------
# Centralized Database Helpers
# ---------------------------------------------------------------------------

CENTRAL_DB_NAME = "___leagues"
CAREER_ROLLUP_TABLES = (
    "matchup_career", "matchup_h2h_career",
    "player_fantasy_career", "player_fantasy_career_all",
    "draft_manager_career", "draft_player_career",
    "transaction_manager_career", "transaction_player_career",
)
COMPLETE_CHAIN_SEASON_ROLLUP_TABLES = (
    "matchup_season",
    "player_fantasy_season", "player_fantasy_season_all",
    "draft_manager_season",
    "transaction_manager_season", "transaction_report_card",
)
HOMEPAGE_ROLLUP_TABLES = (
    "homepage_league_summary", "homepage_manager_rankings",
    "homepage_current_standings", "homepage_top_rivalries", "homepage_manager_profiles",
)


class HomepageValidationError(RuntimeError):
    """Deterministic invalid homepage output; retrying the same input cannot fix it."""

# Active table catalog — starts at the centralized database and is rebound
# to ``current_database()`` by ``configure_table_catalog()`` when an aggregation
# script runs on a local DuckDB scratch connection (e.g. ``memory``).
_ACTIVE_TABLE_CATALOG = CENTRAL_DB_NAME


def get_active_catalog() -> str:
    """Return the currently-active target catalog for aggregate tables."""
    return _ACTIVE_TABLE_CATALOG


def set_active_catalog(catalog: str) -> str:
    """Override the active target catalog and return the previous value."""
    global _ACTIVE_TABLE_CATALOG
    previous_catalog = _ACTIVE_TABLE_CATALOG
    _ACTIVE_TABLE_CATALOG = catalog
    return previous_catalog


def current_catalog(conn) -> str:
    """Return the active DuckDB catalog for a connection."""
    return conn.execute("SELECT current_database()").fetchone()[0]


def configure_table_catalog(conn) -> None:
    """Point the active catalog at this connection's current_database().

    If the connection is a local in-memory scratch pad (``memory``) but the
    module has already been configured against the centralized database,
    leave the catalog alone — some callers swap connections mid-run.
    """
    global _ACTIVE_TABLE_CATALOG
    catalog = current_catalog(conn)
    if catalog == "memory" and _ACTIVE_TABLE_CATALOG not in {CENTRAL_DB_NAME, "memory"}:
        return
    _ACTIVE_TABLE_CATALOG = catalog


def central_table(table_name: str) -> str:
    """Return the centralized-DB qualified name for a table."""
    return f"{_ACTIVE_TABLE_CATALOG}.public.{table_name}"


def league_db_filter(db_name: str, alias: str = "", *, year: int | None = None) -> str:
    """Scope one league, optionally restricting both reads and writes to a season."""
    prefix = f"{alias}." if alias else ""
    predicate = f"{prefix}db_name = '{db_name}'"
    if year is not None:
        if type(year) is not int or year <= 0:
            raise ValueError("year must be a positive integer")
        predicate += f" AND {prefix}year = {year}"
    return predicate


def reapply_persisted_manager_identities(conn, db_name: str, *, season_years: set[int] | None = None) -> int:
    """Reapply saved aliases and franchise merges before rebuilding rollups."""
    if not table_exists_in_catalog(conn, "league_context"):
        return 0
    columns = get_available_columns(conn, "league_context")
    required = {"db_name", "manager_name_overrides_json", "franchise_merges_json"}
    if not required.issubset(columns):
        return 0

    rows = conn.execute(
        f"SELECT manager_name_overrides_json, franchise_merges_json "
        f"FROM {central_table('league_context')} WHERE db_name = ?",
        [db_name],
    ).fetchall()
    if not rows:
        return 0
    if len(rows) != 1:
        raise RuntimeError(f"Expected one league_context row for {db_name}, found {len(rows)}")

    try:
        manager_name_overrides = json.loads(rows[0][0]) if rows[0][0] else {}
        franchise_merges = json.loads(rows[0][1]) if rows[0][1] else []
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Invalid saved manager identity settings for {db_name}") from error
    if not isinstance(manager_name_overrides, dict) or not isinstance(franchise_merges, list):
        raise RuntimeError(f"Invalid saved manager identity settings for {db_name}")
    if not manager_name_overrides and not franchise_merges:
        return 0

    from multi_league.transformations.sql_enrichments import SQLEnrichments

    enricher = SQLEnrichments(
        db_name=db_name,
        quick=True,
        conn=conn,
        manager_name_overrides=manager_name_overrides,
        franchise_merges=franchise_merges,
    )
    try:
        return enricher.reapply_saved_identity_settings(season_years=season_years)
    finally:
        enricher.close()


def aggregate_complete_chain_season_rollups(
    conn, db_name: str, *, season_years: set[int] | None = None,
) -> dict[str, int]:
    """Rebuild season-derived dependencies from the complete persisted chain.

    ``season_years`` limits season outputs to affected seasons. Game ranks must
    first see the complete league history: a new high game can move an older
    game's all-time rank. Careers and homepage outputs also consume the
    complete persisted chain on the same connection.

    This function never fetches provider data. Saved identity settings are
    reapplied only within the requested source seasons before aggregation.
    The caller owns the transaction, so any failure rolls the source partition
    merge and every derived table back together.
    """
    from multi_league.core.aggregate_ddl import ensure_aggregate_table
    from multi_league.core.sql_utils import validate_db_name
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_season,
    )
    from multi_league.transformations.aggregation.aggregate_fantasy_context import (
        aggregate_fantasy_season, aggregate_fantasy_season_all,
    )
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        aggregate_matchup_season,
    )
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_season, aggregate_transaction_report_card,
    )

    validate_db_name(db_name)
    if season_years is not None:
        for year in season_years:
            league_db_filter(db_name, year=year)
        if not season_years:
            return {}
    if current_catalog(conn) != CENTRAL_DB_NAME:
        raise RuntimeError(
            "Complete-chain season publication requires the complete ___leagues connection, "
            "not worker scratch data"
        )
    configure_table_catalog(conn)
    for source in ("matchup", "league_settings", "player_fantasy", "draft", "transactions"):
        if not table_exists_in_catalog(conn, source):
            raise RuntimeError(f"Complete-chain season publication source is missing: {source}")
    reapply_persisted_manager_identities(conn, db_name, season_years=season_years)
    for table in COMPLETE_CHAIN_SEASON_ROLLUP_TABLES:
        ensure_aggregate_table(conn, get_active_catalog(), table)

    from multi_league.transformations.aggregation.modules.optimal_lineup import refresh_position_game_ranks

    refresh_position_game_ranks(conn, central_table("player_fantasy"), db_name=db_name)

    result = dict.fromkeys(COMPLETE_CHAIN_SEASON_ROLLUP_TABLES, 0)
    builders = (
        aggregate_matchup_season, aggregate_fantasy_season,
        aggregate_fantasy_season_all, aggregate_draft_manager_season,
        aggregate_transaction_manager_season, aggregate_transaction_report_card,
    )
    for year in ([None] if season_years is None else sorted(season_years)):
        for table, builder in zip(COMPLETE_CHAIN_SEASON_ROLLUP_TABLES, builders, strict=True):
            result[table] += builder(conn, db_name, year=year)
    return result


def find_missing_retained_season_rollup_years(
    conn, db_name: str, *, season_years: set[int],
) -> dict[str, set[int]]:
    """Return historical aggregate years that are missing source-backed keys.

    The check reads only one league and returns years, not rows.  Callers can
    fold those years into the normal scoped season aggregation before commit,
    avoiding a separate recovery run or any whole-database work.
    """
    from multi_league.transformations.aggregation.aggregate_draft_context import _build_keeper_expr
    from multi_league.transformations.aggregation.aggregate_transaction_context import _ADD_DROP_TRANSACTION_TYPES_SQL

    configure_table_catalog(conn)
    source_scope = league_db_filter(db_name, 'd')
    for year in season_years:
        league_db_filter(db_name, year=year)
    if season_years:
        source_scope += f" AND d.year NOT IN ({', '.join(map(str, sorted(season_years)))})"
    draft_columns = get_available_columns(conn, 'draft')
    keeper = _build_keeper_expr(draft_columns)
    draft_category = "COALESCE(CAST(d.draft_category AS VARCHAR), 'standard')" if 'draft_category' in draft_columns else "'standard'"
    matchup_columns = get_available_columns(conn, 'matchup')
    placeholder_filter = 'AND COALESCE(d.is_placeholder, 0)=0' if 'is_placeholder' in matchup_columns else ''
    regular_nfl = '((d.year >= 2021 AND d.week <= 18) OR (d.year < 2021 AND d.week <= 17))'
    named_franchise = "d.franchise_id IS NOT NULL AND NULLIF(TRIM(d.manager), '') IS NOT NULL"
    checks = (
        ('matchup_season', 'matchup', 'year, franchise_id', 'd.year, d.franchise_id', f"""
            d.franchise_id IS NOT NULL AND d.manager IS NOT NULL AND d.opponent IS NOT NULL
            AND COALESCE(d.is_playoffs, 0)=0 AND COALESCE(d.is_consolation, 0)=0
            AND COALESCE(d.is_bye_week, 0)=0 {placeholder_filter}
            AND (COALESCE(d.team_points, 0)>0 OR COALESCE(d.opponent_points, 0)>0
                 OR COALESCE(d.win, 0)=1 OR COALESCE(d.loss, 0)=1 OR COALESCE(d.tie, 0)=1)
            AND d.week < COALESCE((SELECT MAX(ls.playoff_start_week)
                FROM {central_table('league_settings')} ls
                WHERE ls.db_name=d.db_name AND ls.year=d.year), 99)
        """),
        ('player_fantasy_season', 'player_fantasy', 'year, NFL_player_id', 'd.year, d.NFL_player_id',
         f'd.NFL_player_id IS NOT NULL AND {regular_nfl}'),
        ('player_fantasy_season_all', 'player_fantasy', 'year, NFL_player_id', 'd.year, d.NFL_player_id',
         'd.NFL_player_id IS NOT NULL'),
        ('draft_manager_season', 'draft', 'year, franchise_id, draft_category',
         f"d.year, d.franchise_id, {draft_category}",
         f"{named_franchise} AND TRIM(d.franchise_id)<>'' AND NOT ({keeper})"),
        ('transaction_manager_season', 'transactions', 'year, franchise_id', 'd.year, d.franchise_id',
         f'{named_franchise} AND d.transaction_type IN {_ADD_DROP_TRANSACTION_TYPES_SQL}'),
        ('transaction_report_card', 'transaction_manager_season', 'year, franchise_id', 'd.year, d.franchise_id',
         "d.franchise_id IS NOT NULL AND TRIM(d.franchise_id)<>''"),
    )
    missing_years: dict[str, set[int]] = {}
    for target, source, target_keys, source_keys, predicate in checks:
        missing = conn.execute(f"""
            SELECT DISTINCT CAST(missing.year AS INTEGER)
            FROM (
                SELECT {source_keys} FROM {central_table(source)} d
                WHERE {source_scope} AND {predicate}
                EXCEPT
                SELECT {target_keys} FROM {central_table(target)} WHERE {league_db_filter(db_name)}
            ) missing
            WHERE missing.year IS NOT NULL
            ORDER BY 1
        """).fetchall()
        if missing:
            missing_years[target] = {int(row[0]) for row in missing}
    return missing_years


def assert_retained_season_rollup_coverage(conn, db_name: str, *, season_years: set[int]) -> None:
    """Reject retained historical aggregate gaps after scoped repair."""
    missing_by_table = find_missing_retained_season_rollup_years(
        conn, db_name, season_years=season_years,
    )
    if missing_by_table:
        target = next(iter(missing_by_table))
        years = sorted(missing_by_table[target])[:5]
        raise HomepageValidationError(
            f"Incomplete retained {target} for {db_name}: missing historical years {years}. "
            "Scoped aggregate repair did not restore source-backed coverage."
        )


def aggregate_career_rollups(
    conn,
    db_name: str,
    *,
    refresh_game_ranks: bool = True,
) -> dict[str, int]:
    """Run the normal career aggregations on a complete, merged league connection.

    Weekly publication must call this on Fly after merging changed partitions,
    inside the publication transaction. It neither commits nor opens another
    connection, and it never rewrites historical season tables. Supplying a
    current-season-only scratch database is not a valid use of this function.
    """
    from multi_league.core.aggregate_ddl import ensure_aggregate_table
    from multi_league.core.sql_utils import validate_db_name
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_career, aggregate_draft_player_career,
    )
    from multi_league.transformations.aggregation.aggregate_fantasy_context import (
        aggregate_fantasy_career, aggregate_fantasy_career_all,
    )
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        aggregate_matchup_career, aggregate_matchup_h2h,
    )
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_career, aggregate_transaction_player_career,
    )

    validate_db_name(db_name)
    if current_catalog(conn) != CENTRAL_DB_NAME:
        raise RuntimeError("Career publication requires the complete ___leagues connection, not worker scratch data")
    configure_table_catalog(conn)
    for source in (
        "matchup", "matchup_season", "league_settings", "player_fantasy",
        "draft", "draft_manager_season", "transactions", "transaction_manager_season",
    ):
        if not table_exists_in_catalog(conn, source):
            raise RuntimeError(f"Career publication source is missing: {source}")
    aggregations = {
        "matchup_career": aggregate_matchup_career,
        "player_fantasy_career": aggregate_fantasy_career,
        "player_fantasy_career_all": aggregate_fantasy_career_all,
        "draft_manager_career": aggregate_draft_manager_career,
        "draft_player_career": aggregate_draft_player_career,
        "transaction_manager_career": aggregate_transaction_manager_career,
        "transaction_player_career": aggregate_transaction_player_career,
    }
    # Validate the centralized shell before the first destructive operation.
    for table in (*aggregations, "matchup_h2h_career", "matchup_h2h_season"):
        ensure_aggregate_table(conn, get_active_catalog(), table)
    # V2 publication has no season pass and still owns this refresh. V3 calls
    # the season builder immediately beforehand, so repeating the all-history
    # rank scan adds write latency without changing a cell.
    if refresh_game_ranks:
        from multi_league.transformations.aggregation.modules.optimal_lineup import refresh_position_game_ranks

        refresh_position_game_ranks(conn, central_table("player_fantasy"), db_name=db_name)
    result = {table: aggregate(conn, db_name) for table, aggregate in aggregations.items()}
    _, result["matchup_h2h_career"] = aggregate_matchup_h2h(conn, db_name, season_years=set())
    return result


def aggregate_homepage_rollups(
    conn,
    db_name: str,
    *,
    manager_profile_franchise_ids: set[str] | None = None,
) -> dict[str, int]:
    """Reuse the import homepage builder on Fly's uncommitted full chain.

    Publication owns the transaction. No remote reader, worker history copy,
    synthetic career table, or null-value restoration is involved here.
    """
    from multi_league.core.sql_utils import validate_db_name
    from multi_league.transformations.aggregation.homepage_summary import compute_homepage_frames
    from multi_league.core.aggregate_ddl import HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES
    import pandas as pd

    validate_db_name(db_name)
    if current_catalog(conn) != CENTRAL_DB_NAME:
        raise HomepageValidationError("Homepage publication requires the complete ___leagues connection")
    configure_table_catalog(conn)
    for source in (
        "matchup", "matchup_season", "player_fantasy", "player_fantasy_season",
        "player_fantasy_career", "draft", "transactions", "league_settings", "league_context",
    ):
        if not table_exists_in_catalog(conn, source):
            raise HomepageValidationError(f"Homepage publication source is missing: {source}")
    if manager_profile_franchise_ids is None:
        frames = compute_homepage_frames(conn, db_name)
    else:
        manager_profile_franchise_ids = {
            str(value) for value in manager_profile_franchise_ids if str(value).strip()
        }
        frames = compute_homepage_frames(
            conn,
            db_name,
            manager_profile_franchise_ids=manager_profile_franchise_ids,
        )
        expected_profile_ids = {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT franchise_id FROM public.matchup WHERE db_name = ? "
                "AND franchise_id IS NOT NULL AND team_points IS NOT NULL "
                "AND COALESCE(CAST(is_bye_week AS INTEGER), 0) = 0",
                [db_name],
            ).fetchall()
        }
        if table_exists_in_catalog(conn, "homepage_manager_profiles"):
            previous_profiles = conn.execute(
                "SELECT * FROM public.homepage_manager_profiles WHERE db_name = ?",
                [db_name],
            ).fetchdf()
            if not previous_profiles.empty:
                previous_ids = previous_profiles["franchise_id"].astype(str)
                preserved = previous_profiles[
                    previous_ids.isin(expected_profile_ids)
                    & ~previous_ids.isin(manager_profile_franchise_ids)
                ].copy()
                frames["homepage_manager_profiles"] = pd.concat(
                    [frames["homepage_manager_profiles"], preserved],
                    ignore_index=True,
                    sort=False,
                ).drop_duplicates(subset=["franchise_id"], keep="first")
    if set(frames) != set(HOMEPAGE_ROLLUP_TABLES):
        raise HomepageValidationError("Homepage builder did not return all canonical outputs")
    if len(frames["homepage_league_summary"]) != 1:
        raise HomepageValidationError("Homepage builder must return exactly one league summary")
    for table in ("homepage_manager_rankings", "homepage_manager_profiles", "homepage_current_standings"):
        # Match the homepage builders' played-game scope. Historical imports
        # can contain placeholder/bye-only franchise rows used to preserve an
        # identity without inventing scores; those identities must not force a
        # zero-game ranking row. Current standings also use the latest season
        # containing a positive score, not a future placeholder season.
        played_scope = (
            " AND team_points IS NOT NULL "
            "AND COALESCE(CAST(is_bye_week AS INTEGER), 0) = 0"
        )
        year_scope = ""
        params = [db_name]
        if table == "homepage_current_standings":
            year_scope = (
                " AND TRY_CAST(year AS INTEGER) = ("
                "SELECT MAX(TRY_CAST(year AS INTEGER)) FROM public.matchup "
                "WHERE db_name = ? AND team_points > 0)"
            )
            params.append(db_name)
        expected = {str(row[0]) for row in conn.execute(
            "SELECT DISTINCT franchise_id FROM public.matchup WHERE db_name = ? "
            "AND franchise_id IS NOT NULL" + played_scope + year_scope,
            params,
        ).fetchall()}
        frame = frames[table]
        if "franchise_id" not in frame:
            raise HomepageValidationError(f"{table} lacks franchise identities")
        ids = frame["franchise_id"]
        if ids.isna().any() or ids.duplicated().any() or set(ids.astype(str)) != expected:
            raise HomepageValidationError(f"{table} franchise coverage differs from persisted history")
    # A swallowed query error in a legacy homepage calculation must not erase
    # a previously populated summary. Reject it; do not restore stale values.
    if table_exists_in_catalog(conn, "homepage_league_summary"):
        previous = conn.execute(
            "SELECT * FROM public.homepage_league_summary WHERE db_name = ?", [db_name],
        ).fetchdf()
        if len(previous) > 1:
            raise HomepageValidationError("Persisted homepage summary has duplicate league identity")
        if not previous.empty:
            summary = frames["homepage_league_summary"].iloc[0]
            old_year = previous.iloc[0].get("data_year")
            new_year = summary.get("data_year")
            season_advanced = pd.notna(old_year) and pd.notna(new_year) and new_year > old_year
            # Older partial publishers could advance data_year while retaining
            # a prior year's individual season highlight. Its own year is the
            # authoritative scope; do not force the corrected builder to keep it.
            stale_highlight_prefixes = tuple(
                column[:-5] + "_"
                for column, value in previous.iloc[0].items()
                if column.startswith("season_") and column.endswith("_year")
                and pd.notna(value) and pd.notna(new_year) and value < new_year
            )
            nullable_trade_columns = {
                f"{scope}_trade_{field}"
                for scope in ("season", "alltime")
                for field in HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES
                if field != "db_name"
            }
            highlight_identity_columns = {
                **{
                    f"{scope}_{kind}": ("player", "manager", "year", "round", "pick")
                    for scope in ("season", "alltime")
                    for kind in ("best_pick", "worst_pick")
                },
                **{
                    f"{scope}_{kind}": ("player", "manager", "year", "week")
                    for scope in ("season", "alltime")
                    for kind in ("best_pickup", "worst_drop")
                },
            }

            def highlight_identity_changed(column: str) -> bool:
                """Allow optional attributes to clear only for a new winner."""
                for prefix, identity_suffixes in highlight_identity_columns.items():
                    marker = f"{prefix}_"
                    if not column.startswith(marker) or column == f"{prefix}_player":
                        continue
                    old_identity = tuple(
                        previous.iloc[0].get(f"{prefix}_{suffix}")
                        for suffix in identity_suffixes
                    )
                    new_identity = tuple(
                        summary.get(f"{prefix}_{suffix}")
                        for suffix in identity_suffixes
                    )
                    new_player = summary.get(f"{prefix}_player")
                    return pd.notna(new_player) and old_identity != new_identity
                return False

            for column, old_value in previous.iloc[0].items():
                if column in {"db_name", "last_updated"} or pd.isna(old_value):
                    continue
                # The shared trade calculation validates its inputs and raises
                # query failures. Its explicit nullable DDL result may clear a
                # previous winner after a score/value correction. Omitted fields
                # still fail below; do not manufacture or restore old values.
                if column in summary and column in nullable_trade_columns:
                    continue
                # Current-season highlights change scope at rollover. An empty
                # new-season pickup/trade is valid; last year's winner is not
                # a current-season value. All-time and same-season guards stay.
                if season_advanced and column.startswith("season_"):
                    continue
                if stale_highlight_prefixes and column.startswith(stale_highlight_prefixes):
                    continue
                # A new winner can legitimately lack an optional attribute
                # held by the prior winner (auction cost vs. snake pick is the
                # common cross-season case). The new anchor player must still
                # exist; losing the whole highlight remains a hard rejection.
                if (column not in summary or pd.isna(summary[column])) and highlight_identity_changed(column):
                    continue
                if column not in summary or pd.isna(summary[column]):
                    raise HomepageValidationError(f"Homepage summary lost populated value: {column}")
    counts = {}
    for table in HOMEPAGE_ROLLUP_TABLES:
        replace_scoped_aggregate_table_from_dataframe(conn, db_name, table, frames[table])
        counts[table] = len(frames[table])
    return counts


def table_exists_in_catalog(conn, table_name: str) -> bool:
    """Check if ``_ACTIVE_TABLE_CATALOG.public.<table_name>`` exists."""
    configure_table_catalog(conn)
    try:
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' "
            f"  AND table_name = '{table_name}' "
            f"  AND table_catalog = '{_ACTIVE_TABLE_CATALOG}' "
            "LIMIT 1"
        ).fetchone()
        if row:
            return True
    except Exception:
        pass
    try:
        conn.execute(f"DESCRIBE {central_table(table_name)}").fetchone()
        return True
    except Exception:
        return False


def get_available_columns(conn, table_name: str) -> set[str]:
    """Return the set of columns defined on ``_ACTIVE_TABLE_CATALOG.public.<table_name>``."""
    configure_table_catalog(conn)
    try:
        rows = conn.execute(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            f"  AND table_name = '{table_name}' "
            f"  AND table_catalog = '{_ACTIVE_TABLE_CATALOG}'"
        ).fetchall()
        return {row[0].lower() for row in rows}
    except Exception:
        try:
            sample = conn.execute(f"SELECT * FROM {central_table(table_name)} LIMIT 0").description
            return {col[0].lower() for col in sample}
        except Exception:
            return set()


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _empty_player_lookup_df():
    import pandas as pd

    return pd.DataFrame(columns=["player_week", "player", "headshot_url", "NFL_player_id"])


def _fetch_player_lookup(remote_conn, player_weeks, *, chunk_size: int = 500, max_inline_values: int = 1000):
    """Fetch the compact player_week lookup without requiring connection-local registration."""
    import pandas as pd

    if player_weeks.empty:
        return _empty_player_lookup_df()

    if hasattr(remote_conn, "register") and hasattr(remote_conn, "unregister"):
        try:
            remote_conn.register("_needed_pws", player_weeks)
            try:
                return remote_conn.execute("""
                    SELECT DISTINCT s.player_week, s.player, s.headshot_url, s.NFL_player_id
                    FROM ___ops.nfl_historical.nfl_player_stats_all s
                    INNER JOIN _needed_pws p ON s.player_week = p.player_week
                    WHERE s.player IS NOT NULL
                """).fetchdf()
            finally:
                remote_conn.unregister("_needed_pws")
        except Exception:
            pass

    values = [
        str(value) for value in player_weeks["player_week"].dropna().drop_duplicates().tolist() if str(value).strip()
    ]
    if not values:
        return _empty_player_lookup_df()
    if len(values) > max_inline_values:
        return _empty_player_lookup_df()

    frames = []
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        in_list = ", ".join(_sql_literal(value) for value in chunk)
        frames.append(
            remote_conn.execute(f"""
                SELECT DISTINCT s.player_week, s.player, s.headshot_url, s.NFL_player_id
                FROM ___ops.nfl_historical.nfl_player_stats_all s
                WHERE s.player IS NOT NULL
                  AND s.player_week IN ({in_list})
            """).fetchdf()
        )

    if not frames:
        return _empty_player_lookup_df()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def _create_local_table_from_df(local_conn, table_name: str, df):
    register_name = f"_{table_name}_df"
    local_conn.register(register_name, df)
    try:
        local_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {register_name}")
    finally:
        local_conn.unregister(register_name)


def replace_scoped_aggregate_table_from_dataframe(
    conn,
    db_name: str,
    table_name: str,
    df: pd.DataFrame,
) -> None:
    """Replace one league's rows in a shared aggregate table.

    This is the canonical way to write per-league aggregate data in the
    centralized model: ``DELETE WHERE db_name = '{db_name}'`` followed by
    ``INSERT`` with the ``db_name`` column populated from the caller.

    The target table must be registered in
    :data:`multi_league.core.aggregate_ddl.AGGREGATE_TABLE_SPECS`.
    """
    import pandas as pd

    from multi_league.core.aggregate_ddl import (
        AGGREGATE_TABLE_SPECS,
        aggregate_insert_columns,
        ensure_aggregate_table,
    )
    from multi_league.core.sql_utils import execute_scoped

    configure_table_catalog(conn)

    prepared = df.copy()
    prepared.columns = [str(column) for column in prepared.columns]
    if prepared.columns.duplicated().any():
        dupes = prepared.columns[prepared.columns.duplicated()].tolist()
        raise ValueError(f"{table_name} aggregate dataframe has duplicate columns: {dupes}")

    expected_columns = list(AGGREGATE_TABLE_SPECS[table_name].column_types.keys())
    expected_types = AGGREGATE_TABLE_SPECS[table_name].column_types
    data_columns = [column for column in expected_columns if column != "db_name"]

    extra = sorted(set(prepared.columns) - set(data_columns) - {"db_name"})
    if extra:
        raise ValueError(f"{table_name} aggregate dataframe has non-canonical columns: {extra}")

    if "db_name" in prepared.columns:
        prepared = prepared.drop(columns=["db_name"])
    for column in data_columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[data_columns]

    ensure_aggregate_table(conn, _ACTIVE_TABLE_CATALOG, table_name)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table(table_name)} WHERE {league_db_filter(db_name)}",
        db_name,
        label=f"{table_name}:delete",
    )

    register_name = "_aggregate_upload"
    conn.register(register_name, prepared)
    try:
        cast_select = ", ".join(
            f"CAST({_quote_identifier(column)} AS {expected_types[column]}) AS {_quote_identifier(column)}"
            for column in data_columns
        )
        insert_cols = aggregate_insert_columns(table_name, expected_columns)
        execute_scoped(
            conn,
            f"INSERT INTO {central_table(table_name)} ({insert_cols}) "
            f"SELECT '{db_name}' AS db_name, {cast_select} FROM {register_name}",
            db_name,
            label=f"{table_name}:insert",
        )
    finally:
        conn.unregister(register_name)


# ---------------------------------------------------------------------------
# DB Name Resolution
# ---------------------------------------------------------------------------


def resolve_db_name(args) -> tuple[str, dict]:
    """Resolve database name from --db or --context CLI args.

    Returns (db_name, ctx_data). ctx_data is the parsed context JSON,
    or empty dict if --db was used directly.

    Replaces the copy-pasted sanitize blocks in aggregate_draft_context,
    aggregate_fantasy_context, and aggregate_transaction_context.
    """
    if getattr(args, "db", None):
        return args.db, {}

    if not getattr(args, "context", None):
        raise SystemExit("Must provide --context or --db")

    with open(args.context) as f:
        ctx_data = json.load(f)

    # Always sanitize from league_name — do NOT use motherduck_db_name
    # from context, it may have wrong format (see existing code comments).
    league_name = ctx_data.get("league_name", "")
    if not league_name:
        raise SystemExit("No league_name in context JSON")

    from multi_league.core.db_utils import sanitize_database_name

    return sanitize_database_name(league_name), ctx_data


# ---------------------------------------------------------------------------
# Column Detection
# ---------------------------------------------------------------------------


def _fetch_columns(conn, db_name: str, schema: str, table_name: str) -> set[str]:
    """Fetch column names for a table. Returns empty set if table doesn't exist.

    Uses DESCRIBE as primary method — works identically for both MotherDuck
    (DESCRIBE db.public.table) and local DuckDB (DESCRIBE table).
    """
    refs = [f"{db_name}.{schema}.{table_name}"]
    if db_name == "memory":
        refs = [f"{schema}.{table_name}", table_name]

    for ref in refs:
        try:
            result = conn.execute(f"DESCRIBE {ref}").fetchall()
            return {row[0].lower() for row in result}
        except Exception:
            continue
    return set()


class ColumnCache:
    """Cache column metadata to avoid repeated DESCRIBE queries.

    Works with both MotherDuck connections (schema='public', catalog=db_name)
    and local in-memory DuckDB (schema='main', catalog='memory').

    Usage:
        # MotherDuck
        cache = ColumnCache(conn, db_name)
        cols = cache.columns("matchup")

        # Local DuckDB
        cache = ColumnCache(local_conn, "memory", schema="main")
        if cache.exists("matchup_season"):
            ...
    """

    def __init__(self, conn, db_name: str, schema: str = "public"):
        self._conn = conn
        self._db_name = db_name
        self._schema = schema
        self._columns: dict[str, set[str]] = {}
        self._exists: dict[str, bool] = {}

    def columns(self, table: str) -> set[str]:
        """Get column names for a table (cached after first call)."""
        if table not in self._columns:
            self._columns[table] = _fetch_columns(self._conn, self._db_name, self._schema, table)
            self._exists[table] = bool(self._columns[table])
        return self._columns[table]

    def exists(self, table: str) -> bool:
        """Check if a table exists (cached after first call)."""
        if table not in self._exists:
            self.columns(table)  # populates both caches
        return self._exists[table]

    def invalidate(self, table: str | None = None):
        """Clear cache for a table (or all tables)."""
        if table:
            self._columns.pop(table, None)
            self._exists.pop(table, None)
        else:
            self._columns.clear()
            self._exists.clear()


# ---------------------------------------------------------------------------
# Year Counting (for career skip logic)
# ---------------------------------------------------------------------------


def count_distinct_years(conn, db_name: str, table: str) -> int:
    """Count distinct years for one league in a centralized aggregate table.

    For a local DuckDB scratch pad, pass ``db_name="memory"`` — the function
    will query ``public.<table>`` without any db_name filter.
    """
    try:
        if db_name == "memory":
            sql = f"SELECT COUNT(DISTINCT year) FROM {table}"
        else:
            configure_table_catalog(conn)
            sql = f"SELECT COUNT(DISTINCT year) FROM {central_table(table)} " f"WHERE {league_db_filter(db_name)}"
        row = conn.execute(sql).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Local DuckDB Scratch Pad
# ---------------------------------------------------------------------------

import time

try:
    import duckdb as _duckdb
except ImportError:
    _duckdb = None  # type: ignore[assignment]


def _pull_table(remote_conn, remote_table: str, local_conn, local_name: str) -> bool:
    """Pull a remote table into local DuckDB. Returns True on success."""
    try:
        df = remote_conn.execute(f"SELECT * FROM {remote_table}").fetchdf()
        local_conn.execute(f"CREATE TABLE {local_name} AS SELECT * FROM df")
        return True
    except Exception:
        return False


def _pull_query(remote_conn, sql: str, local_conn, local_name: str) -> bool:
    """Pull a remote query result into local DuckDB. Returns True on success."""
    try:
        df = remote_conn.execute(sql).fetchdf()
        local_conn.execute(f"CREATE TABLE {local_name} AS SELECT * FROM df")
        return True
    except Exception:
        return False


def _pull_if_exists(remote_conn, db_name: str, table: str, local_conn) -> bool:
    """Pull a league's rows from the centralized table if it exists.

    Queries ``central_table(table) WHERE db_name = '{db_name}'``; falls back to
    a plain DESCRIBE check when information_schema is not available.
    """
    if not table_exists_in_catalog(remote_conn, table):
        return False
    return _pull_query(
        remote_conn,
        f"SELECT * FROM {central_table(table)} WHERE {league_db_filter(db_name)}",
        local_conn,
        table,
    )


class LocalProfileContext:
    """Holds local DuckDB copies of centralized tables for profile computation.

    Pulls a single league's rows from ``___leagues.public.*`` into an
    in-memory DuckDB once, then all profile queries run locally (instant)
    instead of making ~300 remote round-trips.

    Uses CREATE TABLE AS SELECT (not register()) so DESCRIBE works.
    All reads are scoped by ``db_name`` — no cross-league contamination.
    """

    def __init__(self, remote_conn, db_name: str, platform: str = "yahoo"):
        if _duckdb is None:
            raise ImportError("duckdb is required for LocalProfileContext")
        self.local = _duckdb.connect(":memory:")
        self._log = make_logger("PROFILE-LOCAL")
        self.db_name = db_name
        self._pull_tables(remote_conn, db_name, platform)
        self.cache = ColumnCache(self.local, "memory", schema="main")

    def _pull_tables(self, remote_conn, db_name: str, platform: str):
        """Pull one league's rows from centralized tables into local memory."""
        t0 = time.perf_counter()
        configure_table_catalog(remote_conn)

        # 1. Core league tables (small, pull scoped by db_name)
        for table in ["matchup", "draft", "transactions"]:
            _pull_query(
                remote_conn,
                f"SELECT * FROM {central_table(table)} WHERE {league_db_filter(db_name)}",
                self.local,
                table,
            )

        # 2. player_fantasy (filtered to started players with LAMAR)
        #    clutch_equity may not exist yet (calculated later in pipeline),
        #    so only filter on columns that are guaranteed to exist.
        pf_pulled = _pull_query(
            remote_conn,
            f"SELECT * FROM {central_table('player_fantasy')} "
            f"WHERE {league_db_filter(db_name)} "
            "AND is_started = 1 "
            "AND manager_lamar IS NOT NULL",
            self.local,
            "player_fantasy",
        )
        if not pf_pulled:
            # Fallback: try without LAMAR filter (still scoped by db_name)
            self._log("[WARN] Filtered player_fantasy pull failed, trying league-only pull")
            _pull_query(
                remote_conn,
                f"SELECT * FROM {central_table('player_fantasy')} WHERE {league_db_filter(db_name)}",
                self.local,
                "player_fantasy",
            )

        # 3. Headshot lookup tables
        self._build_headshot_lookups(remote_conn)

        # 4. Pre-aggregated tables (if they exist)
        for table in ["matchup_season", "player_fantasy_season"]:
            _pull_if_exists(remote_conn, db_name, table, self.local)

        # 5. Settings tables (for H2H+Median detection)
        for table in ["league_settings", "league_context"]:
            _pull_if_exists(remote_conn, db_name, table, self.local)

        elapsed = time.perf_counter() - t0
        self._log(f"Pulled tables to local DuckDB in {elapsed:.1f}s")

    def _build_headshot_lookups(self, remote_conn):
        """Build compact headshot lookup tables locally.

        Two tables:
        - player_lookup: player_week -> (player, headshot_url, NFL_player_id)
        - nfl_id_headshots: NFL_player_id -> headshot_url
        """
        # a) player_week-based lookup
        try:
            needed_pws = self.local.execute(
                "SELECT DISTINCT player_week FROM player_fantasy WHERE player_week IS NOT NULL"
            ).fetchdf()
            headshots_df = _fetch_player_lookup(remote_conn, needed_pws)
            _create_local_table_from_df(self.local, "player_lookup", headshots_df)
        except Exception as e:
            self._log(f"[WARN] Failed to build player_lookup: {e}")

        # b) NFL_player_id-based lookup (for draft/txn headshots).  Restrict
        # the OPS read to players actually present in this league; scanning the
        # global bio and weekly-stat tables on every league refresh is both
        # unnecessary and the dominant homepage setup cost.
        try:
            id_queries: list[str] = []
            for table in ("player_lookup", "player_fantasy", "draft", "transactions"):
                try:
                    columns = {
                        str(row[0])
                        for row in self.local.execute(f"DESCRIBE {table}").fetchall()
                    }
                except Exception:
                    continue
                if "NFL_player_id" in columns:
                    id_queries.append(
                        f"SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id FROM {table} "
                        "WHERE NFL_player_id IS NOT NULL"
                    )
            wanted_ids = (
                [str(row[0]) for row in self.local.execute(
                    "SELECT DISTINCT NFL_player_id FROM (" + " UNION ALL ".join(id_queries) + ") "
                    "WHERE NULLIF(TRIM(NFL_player_id), '') IS NOT NULL ORDER BY 1"
                ).fetchall()]
                if id_queries
                else []
            )
            nfl_headshots_df = remote_conn.execute("""
                WITH wanted AS (
                    SELECT UNNEST(?::VARCHAR[]) AS NFL_player_id
                ), candidates AS (
                    SELECT bio.NFL_player_id, bio.headshot_url, 0 AS source_priority
                    FROM ___ops.nfl_historical.player_bio AS bio
                    JOIN wanted USING (NFL_player_id)
                    WHERE bio.headshot_url IS NOT NULL
                    UNION ALL
                    SELECT stats.NFL_player_id, ANY_VALUE(stats.headshot_url) AS headshot_url,
                           1 AS source_priority
                    FROM ___ops.nfl_historical.nfl_player_stats_all AS stats
                    JOIN wanted USING (NFL_player_id)
                    WHERE stats.headshot_url IS NOT NULL
                    GROUP BY stats.NFL_player_id
                )
                SELECT NFL_player_id, headshot_url FROM candidates
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY NFL_player_id ORDER BY source_priority, headshot_url
                ) = 1
            """, [wanted_ids]).fetchdf()
            _create_local_table_from_df(self.local, "nfl_id_headshots", nfl_headshots_df)
        except Exception as e:
            self._log(f"[WARN] Failed to build nfl_id_headshots: {e}")

    def setup_aliases(self, db_name: str):
        """Attach per-league views so legacy f-string SQL keeps working.

        Historically each aggregation script emitted SQL like
        ``SELECT * FROM <db>.public.matchup`` (per-league MotherDuck databases).
        After calling this method, those f-strings resolve to the local
        in-memory scratch pad, and ``___ops.nfl_historical.nfl_player_stats_all``
        resolves to the compact ``player_lookup`` view.

        New code should prefer :func:`central_table` + :func:`league_db_filter`
        directly and skip this compatibility shim entirely.
        """
        # 1. Alias league tables: legacy per-league prefix → local scratch tables
        try:
            self.local.execute(f"ATTACH ':memory:' AS \"{db_name}\"")
            self.local.execute(f'CREATE SCHEMA IF NOT EXISTS "{db_name}".public')
        except Exception:
            pass

        # Discover all local tables and create views.
        # Use fully qualified memory.main.{table} to avoid recursive view binding.
        try:
            tables = self.local.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_catalog = 'memory'"
            ).fetchall()
            for (tbl,) in tables:
                try:
                    self.local.execute(
                        f'CREATE OR REPLACE VIEW "{db_name}".public.{tbl} ' f"AS SELECT * FROM memory.main.{tbl}"
                    )
                except Exception:
                    pass
        except Exception as e:
            self._log(f"[WARN] Could not create league table aliases: {e}")

        # 2. Alias super_table / player_bio references for headshot lookups
        #    ___ops.nfl_historical.nfl_player_stats_all → player_lookup
        #    ___ops.nfl_historical.player_bio → nfl_id_headshots
        try:
            self.local.execute("ATTACH ':memory:' AS \"___ops\"")
            self.local.execute('CREATE SCHEMA "___ops".nfl_historical')
        except Exception:
            pass

        # player_lookup → nfl_player_stats_all alias
        try:
            self.local.execute(
                'CREATE OR REPLACE VIEW "___ops".nfl_historical.nfl_player_stats_all '
                "AS SELECT * FROM memory.main.player_lookup"
            )
        except Exception:
            pass

        # nfl_id_headshots → player_bio alias
        # Include stub columns for platform-specific IDs and player name so
        # that headshot_subquery() SQL (which references bio.yahoo_player_id,
        # bio.sleeper_player_id, bio.espn_id, bio.player) parses without
        # binder errors.  Only the NFL_player_id lookup will actually match;
        # the fallback subqueries will simply return no rows.
        try:
            self.local.execute(
                'CREATE OR REPLACE VIEW "___ops".nfl_historical.player_bio AS '
                "SELECT NFL_player_id, headshot_url, "
                "NULL::VARCHAR AS yahoo_player_id, "
                "NULL::VARCHAR AS sleeper_player_id, "
                "NULL::VARCHAR AS espn_id, "
                "NULL::VARCHAR AS player "
                "FROM memory.main.nfl_id_headshots"
            )
        except Exception:
            pass

    def close(self):
        """Close the local DuckDB connection."""
        self.local.close()
