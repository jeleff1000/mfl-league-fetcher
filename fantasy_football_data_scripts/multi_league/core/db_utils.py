"""
Database Utility Functions

Canonical location for database name sanitization, collision detection,
pipeline connections, and player ID resolution.

All database operations route to Fly.io DuckDB server.

Usage:
    from multi_league.core.db_utils import (
        sanitize_database_name,
        resolve_database_name,
        validate_centralized_database_name,
    )
"""

from __future__ import annotations

import hashlib
import logging
import os
import re

from multi_league.core.fetch_runtime import runtime_from_source

log = logging.getLogger(__name__)


def sanitize_database_name(name: str) -> str:
    """Sanitize a league name into a valid database name.

    Converts to lowercase, replaces non-alphanumeric chars with underscores,
    ensures it doesn't start with a digit, and limits to 63 chars.

    This is the CANONICAL version -- all other copies should import from here.
    NOTE: For new imports, prefer resolve_database_name() which handles collisions.
    """
    if not name:
        return "league_db"

    db = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not db:
        db = "league_db"
    if db[0].isdigit():
        db = "l_" + db
    return db[:63]


def get_db_name(ctx) -> str:
    """Get the target database name from a context."""
    return runtime_from_source(ctx).db_name


def register_league_early(ctx, platform: str) -> None:
    """Register a league in the ___ops registry at the START of import.

    Call this right after get_db_name() to ensure the registry entry exists
    before any data is written. This prevents orphaned databases if the
    import crashes mid-way -- collision detection will still work on retry.

    The full credential store (with encrypted tokens) runs at the END of
    import and updates this entry with sensitive data.
    """
    runtime = runtime_from_source(ctx)
    db_name = runtime.db_name
    league_id = runtime.league_id
    league_name = runtime.league_name

    if not db_name or not league_id or not league_name:
        log.warning("Cannot register league early: missing db_name, league_id, or league_name")
        return

    try:
        from multi_league.utils.credential_store import register_league

        register_league(
            platform=platform,
            league_id=str(league_id),
            league_name=league_name,
            database_name=db_name,
        )
    except Exception as e:
        # Non-fatal -- import can proceed without early registration
        log.warning(f"Early registration failed (non-fatal): {e}")


def _league_identity_key(platform: str, league_id: str) -> str:
    """Build a canonical identity string for a league (platform + league_id)."""
    return f"{platform.lower().strip()}:{league_id.strip()}"


def _short_hash(identity: str) -> str:
    """Generate a short 4-char hash suffix from a league identity string."""
    return hashlib.md5(identity.encode()).hexdigest()[:4]


def resolve_database_name(
    league_name: str,
    platform: str,
    league_id: str,
    token: str | None = None,
) -> str:
    """Resolve a unique database name via ``league_inventory``.

    Rules:
      1. Sanitize league_name -> base ``db_name``
      2. Look up ``db_name`` in ``___ops.accounts.league_inventory``
      3. **Not found** -> new league, use base name
      4. **Found, same platform+league_id** -> re-import, return existing name
      5. **Found, different league** -> append ``_`` + 4-char hash of
         ``platform:league_id`` to disambiguate

    Falls back to the plain sanitized name if the database server is
    unreachable (non-fatal -- the import can proceed and early registration
    will claim the name later).
    """
    base = sanitize_database_name(league_name)
    identity = _league_identity_key(platform, league_id)

    try:
        from multi_league.core.readers.fly_reader import FlyReader

        reader = FlyReader()
    except Exception as e:
        log.warning(f"FlyReader unavailable (non-fatal, using base name): {e}")
        return base

    try:
        owner = _find_db_owner_fly(reader, base)
        if owner is None:
            return base

        if owner["platform"] == platform.lower().strip() and owner["league_id"] == str(league_id).strip():
            return base

        suffix = _short_hash(identity)
        hashed = f"{base}_{suffix}"[:63]

        hashed_owner = _find_db_owner_fly(reader, hashed)
        if hashed_owner is None:
            return hashed
        if hashed_owner["platform"] == platform.lower().strip() and hashed_owner["league_id"] == str(league_id).strip():
            return hashed

        suffix_long = hashlib.md5(identity.encode()).hexdigest()[:8]
        return f"{base}_{suffix_long}"[:63]
    except Exception as e:
        log.warning(f"Collision check failed (non-fatal, using base name): {e}")
        return base


def _find_db_owner_fly(reader, db_name: str) -> dict | None:
    """Look up the owner of a database_name via Fly reader."""
    try:
        rows = reader.query(
            f"SELECT platform, league_id, league_name "
            f"FROM accounts.league_inventory "
            f"WHERE database_name = '{db_name}'",
            database="___ops",
        )
        if rows:
            r = rows[0]
            return {
                "platform": r["platform"],
                "league_id": str(r["league_id"]) if r.get("league_id") else "",
                "league_name": r.get("league_name"),
            }
    except Exception:
        pass
    return None


_ALLOWED_CENTRALIZED_DBS = {"___leagues", "___ops"}


def validate_centralized_database_name(db_name: str) -> None:
    """Validate that ``db_name`` is one of the centralized targets.

    Databases are created and managed server-side by Fly. This function does
    not create, connect, or check existence — it only enforces that callers
    pass an allowed centralized name (``___leagues`` or ``___ops``) and raises
    ``ValueError`` otherwise.
    """
    if db_name not in _ALLOWED_CENTRALIZED_DBS:
        raise ValueError(
            f"validate_centralized_database_name({db_name!r}) is not allowed in the "
            f"centralized model -- only {sorted(_ALLOWED_CENTRALIZED_DBS)} are valid targets."
        )


def get_pipeline_connection(
    db_name: str, data_dir: str | None = None, attach_ops: bool = False, qualified: bool = False
):
    """Get a DuckDB connection for pipeline scripts -- local DuckDB files only.

    Args:
        db_name: Database / catalog name (e.g., 'kmffl')
        data_dir: Local data directory containing the DuckDB file (required)
        attach_ops: If True, also ATTACH ___ops read-only (for super_table queries)
        qualified: If True, ATTACH local file AS the league so that
                   legacy f-string SQL with the per-league prefix still
                   resolves. Default False preserves existing
                   direct-connection behavior for callers that use
                   unqualified ``public.table`` syntax.

    Returns:
        duckdb.DuckDBPyConnection scoped to the right database
    """
    import duckdb
    from pathlib import Path

    if not data_dir:
        raise RuntimeError("get_pipeline_connection() requires data_dir. " "All pipeline work uses local DuckDB files.")

    local_path = Path(data_dir) / f"{db_name}.duckdb"
    if not local_path.exists():
        raise FileNotFoundError(
            f"Local DuckDB file not found at {local_path}. "
            f"data_dir={data_dir} was provided but the file does not exist."
        )

    if qualified:
        # Callers use {db_name}.public.table syntax
        conn = duckdb.connect()  # in-memory hub
        conn.execute(f"ATTACH '{local_path}' AS \"{db_name}\"")
        conn.execute(f'USE "{db_name}"')
    else:
        # Existing callers use public.table syntax (direct connection)
        conn = duckdb.connect(str(local_path))
    # Create schema if needed (matches LocalLeagueDB.connect)
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")

    # Standalone local runners (including the Yahoo cookie track) use the
    # same local ops cache as the production worker. Keep the default
    # unchanged for OAuth/Sleeper/ESPN callers, but allow the runner to opt
    # into the attachment for subprocesses that do not pass attach_ops.
    if not attach_ops and os.environ.get("LOCAL_PIPELINE_ATTACH_OPS") == "1":
        attach_ops = True

    if attach_ops:
        ops_cache = os.environ.get("OPS_CACHE_PATH", "")
        if ops_cache and Path(ops_cache).exists():
            try:
                attach_ops_cache(conn, ops_cache)
                log.info(f"[___ops] Attached local cache: {ops_cache}")
            except Exception as e:
                log.warning(f"Could not attach local ops cache: {e}")
        else:
            log.warning(
                "[___ops] No OPS_CACHE_PATH set -- ___ops tables will not be "
                "available for SQL JOINs. Set OPS_CACHE_PATH to a local "
                "___ops .duckdb file, or run build_ops_cache.py first."
            )

    return conn


def attach_ops_cache(conn, ops_cache: str) -> None:
    """Attach a local ops cache and bridge modern public tables to legacy namespaces.

    Deployed ops bundles expose ``public.player_bio`` and
    ``public.nfl_player_stats_all``. The shared import SQL intentionally uses
    the stable legacy names ``___ops.nfl_historical.*``. Keep the source file
    read-only and create only in-memory views when the modern layout is found.
    """
    from pathlib import Path

    path = Path(ops_cache).expanduser().resolve()
    conn.execute(f"ATTACH '{path}' AS \"___ops_raw\" (READ_ONLY)")
    objects = {
        (row[1], row[2])
        for row in conn.execute("SHOW ALL TABLES").fetchall()
        if row[0] == "___ops_raw"
    }
    if ("nfl_historical", "player_bio") in objects or ("nfl_historical", "nfl_player_stats_all") in objects:
        conn.execute(f"ATTACH '{path}' AS \"___ops\" (READ_ONLY)")
        conn.execute('DETACH "___ops_raw"')
        return

    public_tables = {table for schema, table in objects if schema == "public"}
    if not public_tables:
        raise RuntimeError(f"OPS cache has no supported public or nfl_historical tables: {path}")

    conn.execute("ATTACH ':memory:' AS \"___ops\"")
    conn.execute('CREATE SCHEMA "___ops".public')
    conn.execute('CREATE SCHEMA "___ops".nfl_historical')
    conn.execute('CREATE SCHEMA "___ops".yahoo_historical')
    for table in sorted(public_tables):
        quoted = '"' + table.replace('"', '""') + '"'
        conn.execute(
            f'CREATE OR REPLACE VIEW "___ops".public.{quoted} '
            f'AS SELECT * FROM "___ops_raw".public.{quoted}'
        )

    # The shared pipeline's NFL namespace is a compatibility view over the
    # modern public bundle. Alias every matching historical table so future
    # shared SQL additions also work without another cookie-specific branch.
    for table in sorted(public_tables):
        quoted = '"' + table.replace('"', '""') + '"'
        conn.execute(
            f'CREATE OR REPLACE VIEW "___ops".nfl_historical.{quoted} '
            f'AS SELECT * FROM "___ops_raw".public.{quoted}'
        )


# ---------------------------------------------------------------------------
# Player ID resolution -- single source of truth
# ---------------------------------------------------------------------------

# Platform ID column -> player_bio column + cast type
_PLATFORM_BIO_MAP = {
    "espn_player_id": ("espn_id", "VARCHAR"),
    "espn_player_id_original": ("espn_id", "VARCHAR"),
    "sleeper_player_id": ("sleeper_player_id", "DOUBLE"),  # canonical column name
    "sleeper_player_id_original": ("sleeper_player_id", "DOUBLE"),  # legacy column name
    "yahoo_player_id": ("yahoo_player_id", "DOUBLE"),
}

# Priority order -- check platform-specific columns first to avoid cross-platform collisions
# Lists both canonical and legacy column names so resolution works on old and new tables
_PLATFORM_PRIORITY = {
    "espn": ["espn_player_id", "espn_player_id_original"],
    "sleeper": ["sleeper_player_id", "sleeper_player_id_original"],
    "yahoo": ["yahoo_player_id"],
    # Fleaflicker does not have a player_bio mapping column yet. Fetchers
    # preserve native IDs and any direct NFL IDs; unresolved rows fall through
    # to the existing name/position resolver below.
    "fleaflicker": [],
    # MFL IDs are retained on roster rows.  Most ops bundles do not yet carry
    # a native mfl_player_id column in player_bio, so name/position fallback
    # remains the safe resolver until that map is available.
    "mfl": [],
}

# Legacy ESPN ID overrides: pre-2019 ESPN leagues use short legacy player IDs
# that don't match player_bio.espn_id (modern 7-digit IDs).  This is a
# surgical mapping for the handful of players affected.  If this grows past
# ~20 entries, promote to an ___ops table.
_LEGACY_ESPN_ID_MAP = {
    "13213": "00-0027325",  # LeGarrette Blount  (modern espn_id 3166800)
    "3042435": "00-0031545",  # Kevin White       (modern espn_id 28395)
}


def _normalized_name_sql(expr: str) -> str:
    """Return a SQL expression that roughly matches shared name normalization."""
    varchar_expr = f"COALESCE(CAST({expr} AS VARCHAR), '')"
    no_punct = f"REGEXP_REPLACE({varchar_expr}, '[^a-zA-Z0-9 ]', '', 'g')"
    no_suffix = f"REGEXP_REPLACE({no_punct}, ' (iii|iv|ii|jr|sr|v)$', '', 'i')"
    collapsed = f"REGEXP_REPLACE({no_suffix}, '\\\\s+', ' ', 'g')"
    return f"LOWER(TRIM({collapsed}))"


def _position_family_sql(expr: str) -> str:
    """Return a SQL expression that maps platform/bio positions to a common family."""
    upper_expr = f"UPPER(TRIM(COALESCE(CAST({expr} AS VARCHAR), '')))"
    return f"""
        CASE
            WHEN {upper_expr} IN ('HB', 'FB', 'RB') THEN 'RB'
            WHEN {upper_expr} = 'WR' THEN 'WR'
            WHEN {upper_expr} = 'TE' THEN 'TE'
            WHEN {upper_expr} = 'QB' THEN 'QB'
            WHEN {upper_expr} = 'K' THEN 'K'
            WHEN {upper_expr} IN ('DEF', 'DST', 'D/ST') THEN 'DEF'
            WHEN {upper_expr} IN ('DB', 'CB', 'S', 'SS', 'FS', 'SAF', 'NB') THEN 'DB'
            WHEN {upper_expr} IN ('DL', 'DE', 'DT', 'NT') THEN 'DL'
            WHEN {upper_expr} IN ('LB', 'MLB', 'ILB', 'OLB') THEN 'LB'
            ELSE {upper_expr}
        END
    """


def _resolve_unique_platform_ids_sql(
    qualified: str,
    id_col: str,
    bio_col: str,
    cast_type: str,
    *,
    player_col: str | None = None,
) -> str:
    """Resolve unique platform IDs, requiring a matching name when available.

    Historical providers can reuse player IDs.  A platform ID that is unique in
    today's ``player_bio`` is therefore not sufficient when the fetched row names
    a different player; leave that row unresolved so the name fallback can map it.
    """
    if player_col:
        source_name = _normalized_name_sql(f"t.{player_col}")
        name_guard = f"""
          AND (
              NULLIF(TRIM(CAST(t.{player_col} AS VARCHAR)), '') IS NULL
              OR {source_name} = pb.norm_name
          )
        """
        mapping_sql = f"""
            WITH unique_ids AS (
                SELECT {bio_col} AS platform_id, MIN(NFL_player_id) AS NFL_player_id
                FROM ___ops.nfl_historical.player_bio
                WHERE {bio_col} IS NOT NULL
                  AND NFL_player_id IS NOT NULL
                GROUP BY {bio_col}
                HAVING COUNT(DISTINCT NFL_player_id) = 1
            )
            SELECT DISTINCT
                ids.platform_id,
                ids.NFL_player_id,
                {_normalized_name_sql("bio.player")} AS norm_name
            FROM unique_ids ids
            JOIN ___ops.nfl_historical.player_bio bio
              ON bio.{bio_col} = ids.platform_id
             AND bio.NFL_player_id = ids.NFL_player_id
            WHERE bio.player IS NOT NULL
        """
    else:
        name_guard = ""
        mapping_sql = f"""
            SELECT {bio_col} AS platform_id, MIN(NFL_player_id) AS NFL_player_id
            FROM ___ops.nfl_historical.player_bio
            WHERE {bio_col} IS NOT NULL
              AND NFL_player_id IS NOT NULL
            GROUP BY {bio_col}
            HAVING COUNT(DISTINCT NFL_player_id) = 1
        """

    return f"""
        UPDATE {qualified} t
        SET NFL_player_id = pb.NFL_player_id
        FROM (
            {mapping_sql}
        ) pb
        WHERE TRY_CAST(t.{id_col} AS {cast_type}) = pb.platform_id
          {name_guard}
          AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
    """


def _resolve_ambiguous_platform_ids_sql(
    qualified: str,
    id_col: str,
    bio_col: str,
    cast_type: str,
    *,
    player_col: str | None,
    position_col: str | None,
) -> list[str]:
    """Build safe disambiguation SQL for platform IDs with multiple bio matches."""
    statements: list[str] = []

    if player_col:
        norm_player = _normalized_name_sql(f"t.{player_col}")
        name_only_sql = f"""
            UPDATE {qualified} t
            SET NFL_player_id = pb.NFL_player_id
            FROM (
                SELECT platform_id, norm_name, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                    SELECT
                        {bio_col} AS platform_id,
                        NFL_player_id,
                        {_normalized_name_sql("player")} AS norm_name
                    FROM ___ops.nfl_historical.player_bio
                    WHERE {bio_col} IS NOT NULL
                      AND NFL_player_id IS NOT NULL
                      AND player IS NOT NULL
                ) matched
                GROUP BY platform_id, norm_name
                HAVING COUNT(DISTINCT NFL_player_id) = 1
            ) pb
            WHERE TRY_CAST(t.{id_col} AS {cast_type}) = pb.platform_id
              AND {norm_player} = pb.norm_name
              AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
        """
        statements.append(name_only_sql)

        if position_col:
            pos_family = _position_family_sql(f"t.{position_col}")
            name_pos_sql = f"""
                UPDATE {qualified} t
                SET NFL_player_id = pb.NFL_player_id
                FROM (
                    SELECT platform_id, norm_name, pos_family, MIN(NFL_player_id) AS NFL_player_id
                    FROM (
                        SELECT
                            {bio_col} AS platform_id,
                            NFL_player_id,
                            {_normalized_name_sql("player")} AS norm_name,
                            {_position_family_sql("nfl_position")} AS pos_family
                        FROM ___ops.nfl_historical.player_bio
                        WHERE {bio_col} IS NOT NULL
                          AND NFL_player_id IS NOT NULL
                          AND player IS NOT NULL
                          AND nfl_position IS NOT NULL
                    ) matched
                    GROUP BY platform_id, norm_name, pos_family
                    HAVING COUNT(DISTINCT NFL_player_id) = 1
                ) pb
                WHERE TRY_CAST(t.{id_col} AS {cast_type}) = pb.platform_id
                  AND {norm_player} = pb.norm_name
                  AND {pos_family} = pb.pos_family
                  AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
            """
            # Name+position is the safest disambiguator, so run it before name-only.
            statements.insert(0, name_pos_sql)

    return statements


def _resolve_name_based_fallback_sql(
    qualified: str,
    *,
    player_col: str | None,
    position_col: str | None,
    year_col: str | None = None,
) -> list[str]:
    """Build safe player_bio fallback SQL for rows with no usable platform-ID match."""
    statements: list[str] = []

    if not player_col:
        return statements

    norm_player = _normalized_name_sql(f"t.{player_col}")

    if year_col:
        year_expr = f"TRY_CAST(t.{year_col} AS INTEGER)"

        year_name_sql = f"""
            UPDATE {qualified} t
            SET NFL_player_id = st.NFL_player_id
            FROM (
                SELECT year, norm_name, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                    SELECT
                        TRY_CAST(year AS INTEGER) AS year,
                        NFL_player_id,
                        {_normalized_name_sql("player")} AS norm_name
                    FROM ___ops.nfl_historical.nfl_player_stats_all
                    WHERE NFL_player_id IS NOT NULL
                      AND player IS NOT NULL
                      AND year IS NOT NULL
                ) matched
                GROUP BY year, norm_name
                HAVING COUNT(DISTINCT NFL_player_id) = 1
            ) st
            WHERE {year_expr} = st.year
              AND {norm_player} = st.norm_name
              AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
        """
        statements.append(year_name_sql)

        if position_col:
            pos_family = _position_family_sql(f"t.{position_col}")
            year_name_pos_sql = f"""
                UPDATE {qualified} t
                SET NFL_player_id = st.NFL_player_id
                FROM (
                    SELECT year, norm_name, pos_family, MIN(NFL_player_id) AS NFL_player_id
                    FROM (
                        SELECT
                            TRY_CAST(year AS INTEGER) AS year,
                            NFL_player_id,
                            {_normalized_name_sql("player")} AS norm_name,
                            {_position_family_sql("nfl_position")} AS pos_family
                        FROM ___ops.nfl_historical.nfl_player_stats_all
                        WHERE NFL_player_id IS NOT NULL
                          AND player IS NOT NULL
                          AND year IS NOT NULL
                          AND nfl_position IS NOT NULL
                    ) matched
                    GROUP BY year, norm_name, pos_family
                    HAVING COUNT(DISTINCT NFL_player_id) = 1
                ) st
                WHERE {year_expr} = st.year
                  AND {norm_player} = st.norm_name
                  AND {pos_family} = st.pos_family
                  AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
            """
            # Year+name+position is safest; run before year+name.
            statements.insert(0, year_name_pos_sql)

    name_only_sql = f"""
        UPDATE {qualified} t
        SET NFL_player_id = pb.NFL_player_id
        FROM (
            SELECT norm_name, MIN(NFL_player_id) AS NFL_player_id
            FROM (
                SELECT
                    NFL_player_id,
                    {_normalized_name_sql("player")} AS norm_name
                FROM ___ops.nfl_historical.player_bio
                WHERE NFL_player_id IS NOT NULL
                  AND player IS NOT NULL
            ) matched
            GROUP BY norm_name
            HAVING COUNT(DISTINCT NFL_player_id) = 1
        ) pb
        WHERE {norm_player} = pb.norm_name
          AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
    """
    statements.append(name_only_sql)

    if position_col:
        pos_family = _position_family_sql(f"t.{position_col}")
        name_pos_sql = f"""
            UPDATE {qualified} t
            SET NFL_player_id = pb.NFL_player_id
            FROM (
                SELECT norm_name, pos_family, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                    SELECT
                        NFL_player_id,
                        {_normalized_name_sql("player")} AS norm_name,
                        {_position_family_sql("nfl_position")} AS pos_family
                    FROM ___ops.nfl_historical.player_bio
                    WHERE NFL_player_id IS NOT NULL
                      AND player IS NOT NULL
                      AND nfl_position IS NOT NULL
                ) matched
                GROUP BY norm_name, pos_family
                HAVING COUNT(DISTINCT NFL_player_id) = 1
            ) pb
            WHERE {norm_player} = pb.norm_name
              AND {pos_family} = pb.pos_family
              AND (t.NFL_player_id IS NULL OR TRY_CAST(t.NFL_player_id AS VARCHAR) = '')
        """
        statements.insert(0, name_pos_sql)

    return statements


def detect_platform(conn, db_name: str, schema: str = "public") -> str:
    """Detect platform from league_settings.platform column.

    Canonical DDL includes all platform ID columns on every table, so
    column-sniffing no longer works. The authoritative source is the
    flat ``platform`` column written at fetch/normalize time.

    Args:
        conn: DuckDB connection
        db_name: Database name
        schema: Schema name (default: public)

    Returns:
        'espn', 'sleeper', 'fleaflicker', or 'yahoo'
    """
    queries = [
        (f"SELECT platform FROM {schema}.league_settings WHERE db_name = ? LIMIT 1", [db_name]),
        (f'SELECT platform FROM "{db_name}".{schema}.league_settings LIMIT 1', None),
        (f"SELECT platform FROM {schema}.league_settings LIMIT 1", None),
    ]

    for sql, params in queries:
        try:
            row = conn.execute(sql, params or []).fetchone()
            if row and row[0] in ("espn", "sleeper", "fleaflicker", "mfl", "yahoo"):
                return row[0]
        except Exception:
            continue
    return "yahoo"


def resolve_nfl_ids(conn, db_name: str, table_name: str, schema: str = "public", platform: str | None = None) -> int:
    """Resolve platform player IDs -> NFL_player_id via player_bio.

    Uses platform IDs as the primary key, but guards against ambiguous
    ``player_bio`` mappings. Some Yahoo IDs are attached to multiple NFL
    players in ``player_bio`` (for example same-name collisions like
    Chris Johnson RB vs DB). Blind joins on those IDs are nondeterministic
    and can silently corrupt ``player_week`` / scoring. The resolver now:

      1. resolves IDs that map to exactly one NFL player
      2. for unresolved ambiguous IDs, uses the row's own ``player`` name
         and ``position`` to disambiguate when that combination is unique
      3. otherwise leaves the row unresolved instead of guessing

    **Strict DDL compliance**: when running against the centralized
    ``___leagues`` catalog (``db_name == "___leagues"``), the
    ``NFL_player_id`` column is guaranteed to exist (canonical DDL
    pre-creates it as VARCHAR). Any ALTER TABLE fallback is disabled in
    that mode -- if the column is missing, the canonical DDL is wrong and
    the import should fail loudly.

    In local DuckDB mode (any other ``db_name``, typically the scratch
    file for one import), the ALTER TABLE fallback is retained for
    backwards compatibility with older local files.

    Args:
        conn: DuckDB connection
        db_name: Database name (e.g., 'the_league' or '___leagues')
        table_name: Table to resolve (e.g., 'player_fantasy', 'draft', 'transactions')
        schema: Schema name (default: 'public')
        platform: Platform override. If None, auto-detected from table columns.

    Returns:
        Number of rows resolved (or -1 if unknown / DuckDB doesn't report rowcount)
    """
    qualified = f"{db_name}.{schema}.{table_name}"
    is_centralized = db_name == "___leagues"

    try:
        cols = {r[0] for r in conn.execute(f"DESCRIBE {qualified}").fetchall()}
    except Exception:
        return 0

    if "NFL_player_id" not in cols:
        if is_centralized:
            raise RuntimeError(
                f"[STRICT DDL] {qualified} is missing the NFL_player_id column. "
                f"Fix the canonical DDL in canonical_{table_name}.py."
            )
        try:
            conn.execute(f"ALTER TABLE {qualified} ADD COLUMN NFL_player_id VARCHAR")
        except Exception:
            pass
    elif not is_centralized:
        # Fix columns that were incorrectly typed as DOUBLE/BIGINT (can't
        # hold DEF-* IDs). In the centralized model the canonical DDL
        # already declares NFL_player_id VARCHAR, so this check is
        # local-only.
        try:
            col_type = [r[1] for r in conn.execute(f"DESCRIBE {qualified}").fetchall() if r[0] == "NFL_player_id"]
            if col_type and col_type[0].upper() in ("DOUBLE", "BIGINT", "INTEGER", "FLOAT", "HUGEINT", "UBIGINT"):
                conn.execute(f"""
                    ALTER TABLE {qualified} ALTER COLUMN NFL_player_id
                    SET DATA TYPE VARCHAR USING CAST(NFL_player_id AS VARCHAR)
                """)
        except Exception:
            pass

    # The active-season worker hydrates already-enriched rows from Fly and
    # appends only the newly-finalized provider rows.  Do not scan player_bio
    # or the super table when this table has no unresolved IDs at all.
    unresolved_rows = conn.execute(
        f"SELECT COUNT(*) FROM {qualified} "
        "WHERE NFL_player_id IS NULL OR TRY_CAST(NFL_player_id AS VARCHAR) = ''"
    ).fetchone()[0]
    if int(unresolved_rows or 0) == 0:
        return 0

    # Detect platform if not provided
    if platform is None:
        platform = detect_platform(conn, db_name, schema)

    # Use only the platform's own ID columns (avoids cross-platform ID collisions)
    id_cols = _PLATFORM_PRIORITY.get(platform, [])
    total = 0

    for id_col in id_cols:
        if id_col not in cols:
            continue
        bio_col, cast_type = _PLATFORM_BIO_MAP[id_col]
        player_col = "player" if "player" in cols else None
        position_col = "position" if "position" in cols else None
        conn.execute(
            _resolve_unique_platform_ids_sql(
                qualified,
                id_col,
                bio_col,
                cast_type,
                player_col=player_col,
            )
        )
        for sql in _resolve_ambiguous_platform_ids_sql(
            qualified,
            id_col,
            bio_col,
            cast_type,
            player_col=player_col,
            position_col=position_col,
        ):
            conn.execute(sql)
        total += 1  # DuckDB doesn't return rowcount for UPDATE
        break  # Only need one platform column

    player_col = "player" if "player" in cols else None
    position_col = "position" if "position" in cols else None
    year_col = "year" if "year" in cols else None
    for sql in _resolve_name_based_fallback_sql(
        qualified,
        player_col=player_col,
        position_col=position_col,
        year_col=year_col,
    ):
        conn.execute(sql)

    # Extract DEF NFL_player_id from player_week (e.g., "DEF-29_2025_18" -> "DEF-29")
    if "player_week" in cols:
        try:
            conn.execute(f"""
                UPDATE {qualified}
                SET NFL_player_id = SPLIT_PART(player_week, '_', 1)
                WHERE (NFL_player_id IS NULL OR TRY_CAST(NFL_player_id AS VARCHAR) = '')
                  AND player_week LIKE 'DEF-%'
            """)
        except Exception:
            pass

    # Legacy ESPN ID override: pre-2019 ESPN used short legacy IDs that don't
    # match player_bio.espn_id. Surgical mapping for a handful of known players.
    if platform == "espn":
        for legacy_id, nfl_id in _LEGACY_ESPN_ID_MAP.items():
            for espn_col in ("espn_player_id", "espn_player_id_original"):
                if espn_col not in cols:
                    continue
                try:
                    conn.execute(f"""
                        UPDATE {qualified}
                        SET NFL_player_id = '{nfl_id}'
                        WHERE CAST({espn_col} AS VARCHAR) = '{legacy_id}'
                          AND (NFL_player_id IS NULL
                               OR NFL_player_id = ''
                               OR NFL_player_id LIKE 'ESPN-%')
                    """)
                except Exception:
                    pass

    # Sleeper DEF/DST ID resolution: Sleeper stores DEF/DST adds/drops/trades
    # with sleeper_player_id = 2-3 char NFL team abbreviation ('LV', 'CIN',
    # 'WAS') instead of a numeric player id. These never match player_bio
    # because player_bio.sleeper_player_id is DOUBLE, so TRY_CAST('LV' AS
    # DOUBLE) yields NULL. Resolve them by joining against the super_table's
    # canonical nfl_team -> DEF-{id} mapping. Handles all transaction types,
    # including drops and trades, which don't have player_week to extract
    # from. OAK -> LV alias covers pre-2020 Raiders rows.
    if platform == "sleeper":
        for sleeper_col in ("sleeper_player_id", "sleeper_player_id_original"):
            if sleeper_col not in cols:
                continue
            try:
                conn.execute(f"""
                    UPDATE {qualified} t
                    SET NFL_player_id = m.def_id
                    FROM (
                        SELECT DISTINCT NFL_player_id AS def_id, nfl_team
                        FROM ___ops.nfl_historical.nfl_player_stats_all
                        WHERE NFL_player_id LIKE 'DEF-%'
                          AND nfl_team IS NOT NULL
                    ) m
                    WHERE (
                        UPPER(CAST(t.{sleeper_col} AS VARCHAR)) = m.nfl_team
                        OR (UPPER(CAST(t.{sleeper_col} AS VARCHAR)) = 'OAK' AND m.nfl_team = 'LV')
                    )
                    AND LENGTH(CAST(t.{sleeper_col} AS VARCHAR)) BETWEEN 2 AND 3
                    AND (t.NFL_player_id IS NULL OR CAST(t.NFL_player_id AS VARCHAR) = '')
                """)
            except Exception:
                pass
            break

    return total
