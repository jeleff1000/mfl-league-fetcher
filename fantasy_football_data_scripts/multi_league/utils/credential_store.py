"""
Credential Store for Weekly Updates

Stores encrypted OAuth refresh tokens for automatic weekly updates.
Backend-aware: Fly writes go through the Fly query writer; MotherDuck is
kept for legacy local workflows.
"""

import json
import os
from datetime import datetime, timezone

from multi_league.utils.yahoo_auth_mode import validate_cookie_jar_payload


def _is_fly_backend() -> bool:
    return os.environ.get("DATABASE_BACKEND", "fly") == "fly"


def _sql_literal(value: object) -> str:
    """Return a DuckDB SQL literal for FlyWriter statements."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


try:
    from cryptography.fernet import Fernet

    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    Fernet = None

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


def get_encryption_key() -> str | None:
    """Get encryption key from environment."""
    return os.environ.get("CREDENTIAL_ENCRYPTION_KEY") or os.environ.get("CREDENTIAL_ENCRYPTION_KEY_NEW")


def encrypt_token(token: str, key: str) -> str:
    """Encrypt a token using Fernet symmetric encryption."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography package not installed")

    f = Fernet(key.encode() if isinstance(key, str) else key)
    return f.encrypt(token.encode()).decode()


def _pad_base64url(value: str) -> str:
    """Add omitted base64url padding for Fernet keys/tokens.

    Node's base64url encoder strips "=" padding, while Python cryptography's
    Fernet decoder expects padded urlsafe base64. Accept both forms so existing
    frontend-written credential rows remain readable.
    """
    stripped = value.strip()
    return stripped + ("=" * (-len(stripped) % 4))


def decrypt_token(encrypted_token: str, key: str) -> str:
    """Decrypt a token using Fernet symmetric encryption."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography package not installed")

    normalized_key = _pad_base64url(key) if isinstance(key, str) else key
    normalized_token = _pad_base64url(encrypted_token)
    f = Fernet(normalized_key.encode() if isinstance(normalized_key, str) else normalized_key)
    return f.decrypt(normalized_token.encode()).decode()


def decrypt_token_with_key_rotation(encrypted_token: str, keys: list[str]) -> str:
    """Decrypt with the first valid Fernet key.

    Supports CREDENTIAL_ENCRYPTION_KEY plus CREDENTIAL_ENCRYPTION_KEY_NEW during
    rotations and accepts frontend-written unpadded Fernet tokens.
    """
    last_error: Exception | None = None
    for key in keys:
        if not key:
            continue
        try:
            return decrypt_token(encrypted_token, key)
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise ValueError("No Fernet keys provided")


def _upsert_league_inventory(
    conn,
    database_name: str,
    platform: str,
    league_name: str,
    league_id: str,
) -> None:
    """Upsert a row in ___ops.accounts.league_inventory.

    Called during early registration and credential storage to keep the
    centralized inventory in sync. New leagues default to tier='free'.
    Non-fatal — logs warnings but never blocks imports.
    """
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ___ops.accounts.league_inventory (
                database_name       VARCHAR NOT NULL PRIMARY KEY,
                platform            VARCHAR NOT NULL,
                league_name         VARCHAR,
                league_id           VARCHAR,
                tier                VARCHAR DEFAULT 'free',
                entitled_mode       VARCHAR DEFAULT 'quick',
                last_import_mode    VARCHAR,
                last_import_at      TIMESTAMP,
                in_centralized      BOOLEAN DEFAULT FALSE,
                num_teams           INTEGER,
                first_year          INTEGER,
                last_year           INTEGER,
                scoring_variant     VARCHAR,
                has_credentials     BOOLEAN DEFAULT FALSE,
                created_at          TIMESTAMP DEFAULT current_timestamp,
                updated_at          TIMESTAMP DEFAULT current_timestamp
            )
        """)
        existing = conn.execute(
            "SELECT 1 FROM ___ops.accounts.league_inventory WHERE database_name = ?",
            [database_name],
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE ___ops.accounts.league_inventory
                SET platform = ?, league_name = COALESCE(?, league_name),
                    league_id = ?, updated_at = current_timestamp
                WHERE database_name = ?
            """,
                [platform, league_name, league_id, database_name],
            )
        else:
            conn.execute(
                """
                INSERT INTO ___ops.accounts.league_inventory
                (database_name, platform, league_name, league_id, tier, entitled_mode,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, 'free', 'quick', current_timestamp, current_timestamp)
            """,
                [database_name, platform, league_name, league_id],
            )
    except Exception as e:
        print(f"Warning: league_inventory upsert failed (non-fatal): {e}")


def register_league(
    platform: str,
    league_id: str,
    league_name: str,
    database_name: str,
    motherduck_token: str | None = None,
) -> bool:
    """Register a league in the ___ops registry BEFORE import starts.

    Creates a minimal registry entry so collision detection works even if
    the import crashes. The full credential store (with encrypted tokens)
    runs at the end of a successful import and updates this entry.

    Also upserts ___ops.accounts.league_inventory for centralized tracking.

    This is intentionally lightweight — no encryption needed, just
    platform + league_id + database_name.

    Args:
        platform: "yahoo", "sleeper", or "espn"
        league_id: Platform-specific league ID
        league_name: Human-readable name
        database_name: Resolved MotherDuck database name
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        True if successful, False otherwise
    """
    if _is_fly_backend():
        print(f"[credential_store] Fly backend — skipping register_league for {database_name} (non-fatal)")
        return True

    if not DUCKDB_AVAILABLE:
        print("Warning: duckdb not available - cannot register league")
        return False

    md_token = motherduck_token or os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not md_token:
        print("Warning: No database token set - cannot register league")
        return False

    try:
        conn = duckdb.connect(f"md:?motherduck_token={md_token}")

        platform_lower = platform.lower().strip()

        if platform_lower == "yahoo":
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ___ops.main.league_credentials (
                    league_id TEXT PRIMARY KEY,
                    league_name TEXT,
                    database_name TEXT,
                    encrypted_refresh_token TEXT,
                    updated_at TIMESTAMP DEFAULT current_timestamp
                )
            """)
            # Check if exists first, then insert or update (DuckDB compat)
            existing = conn.execute(
                "SELECT encrypted_refresh_token FROM ___ops.main.league_credentials WHERE league_id = ?", [league_id]
            ).fetchall()
            if existing:
                # Update but preserve encrypted_refresh_token
                conn.execute(
                    "UPDATE ___ops.main.league_credentials SET league_name = ?, database_name = ?, updated_at = current_timestamp WHERE league_id = ?",
                    [league_name, database_name, league_id],
                )
            else:
                conn.execute(
                    "INSERT INTO ___ops.main.league_credentials (league_id, league_name, database_name, updated_at) VALUES (?, ?, ?, current_timestamp)",
                    [league_id, league_name, database_name],
                )

        elif platform_lower == "sleeper":
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ___ops.main.sleeper_leagues (
                    sleeper_league_id TEXT PRIMARY KEY,
                    league_name TEXT,
                    database_name TEXT,
                    created_at TIMESTAMP DEFAULT current_timestamp,
                    updated_at TIMESTAMP DEFAULT current_timestamp
                )
            """)
            existing = conn.execute(
                "SELECT 1 FROM ___ops.main.sleeper_leagues WHERE sleeper_league_id = ?", [league_id]
            ).fetchall()
            if existing:
                conn.execute(
                    "UPDATE ___ops.main.sleeper_leagues SET league_name = ?, database_name = ?, updated_at = current_timestamp WHERE sleeper_league_id = ?",
                    [league_name, database_name, league_id],
                )
            else:
                conn.execute(
                    "INSERT INTO ___ops.main.sleeper_leagues (sleeper_league_id, league_name, database_name, updated_at) VALUES (?, ?, ?, current_timestamp)",
                    [league_id, league_name, database_name],
                )

        elif platform_lower == "espn":
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ___ops.main.espn_leagues (
                    espn_league_id INTEGER PRIMARY KEY,
                    league_name TEXT,
                    database_name TEXT,
                    encrypted_espn_s2 TEXT,
                    encrypted_swid TEXT,
                    created_at TIMESTAMP DEFAULT current_timestamp,
                    updated_at TIMESTAMP DEFAULT current_timestamp
                )
            """)
            existing = conn.execute(
                "SELECT 1 FROM ___ops.main.espn_leagues WHERE espn_league_id = ?", [int(league_id)]
            ).fetchall()
            if existing:
                conn.execute(
                    "UPDATE ___ops.main.espn_leagues SET league_name = ?, database_name = ?, updated_at = current_timestamp WHERE espn_league_id = ?",
                    [league_name, database_name, int(league_id)],
                )
            else:
                conn.execute(
                    "INSERT INTO ___ops.main.espn_leagues (espn_league_id, league_name, database_name, updated_at) VALUES (?, ?, ?, current_timestamp)",
                    [int(league_id), league_name, database_name],
                )

        else:
            print(f"Warning: unknown platform '{platform}' - cannot register")
            conn.close()
            return False

        # Also upsert centralized league_inventory
        _upsert_league_inventory(conn, database_name, platform_lower, league_name, league_id)

        conn.close()
        print(f"Registered {platform} league {league_id} -> {database_name}")
        return True

    except Exception as e:
        print(f"Warning: Failed to register league: {e}")
        return False


def _store_league_credentials_fly(
    league_id: str,
    league_name: str,
    refresh_token: str,
    database_name: str,
    encryption_key: str | None = None,
) -> bool:
    """Store Yahoo credentials in the Fly ___ops registry."""
    if not CRYPTO_AVAILABLE:
        print("Warning: cryptography not available - cannot encrypt credentials")
        return False

    key = encryption_key or get_encryption_key()
    if not key:
        print("Warning: No encryption key available")
        return False

    try:
        encrypted = encrypt_token(refresh_token, key)
    except Exception as e:
        print(f"Error encrypting credentials: {e}")
        return False

    try:
        from multi_league.core.fly_writer import FlyWriter

        safe_league_id = _sql_literal(league_id)
        safe_league_name = _sql_literal(league_name)
        safe_database_name = _sql_literal(database_name)
        safe_encrypted = _sql_literal(encrypted)
        sql = f"""
        BEGIN TRANSACTION;

        DELETE FROM main.league_credentials
        WHERE league_id = {safe_league_id}
           OR database_name = {safe_database_name};

        INSERT INTO main.league_credentials
            (league_id, league_name, database_name, encrypted_refresh_token, updated_at)
        VALUES
            ({safe_league_id}, {safe_league_name}, {safe_database_name}, {safe_encrypted}, current_timestamp);

        UPDATE accounts.league_inventory
        SET platform = 'yahoo',
            league_name = COALESCE(NULLIF({safe_league_name}, ''), league_name),
            league_id = {safe_league_id},
            has_credentials = TRUE,
            last_import_at = current_timestamp,
            updated_at = current_timestamp
        WHERE database_name = {safe_database_name};

        INSERT INTO accounts.league_inventory
            (database_name, platform, league_name, league_id, tier, entitled_mode,
             has_credentials, last_import_at, created_at, updated_at)
        SELECT {safe_database_name}, 'yahoo', {safe_league_name}, {safe_league_id},
               'free', 'quick', TRUE, current_timestamp, current_timestamp, current_timestamp
        WHERE NOT EXISTS (
            SELECT 1 FROM accounts.league_inventory
            WHERE database_name = {safe_database_name}
        );

        COMMIT;
        """
        FlyWriter().execute(
            sql,
            database="___ops",
            timeout_seconds=3,
            max_retries=1,
        )
        print(f"Stored Fly credentials for {league_name} ({league_id}) -> {database_name}")
        return True
    except Exception as e:
        print(f"Error storing Fly credentials: {e}")
        return False


def _store_yahoo_cookie_credentials_fly(
    league_id: str,
    league_name: str,
    cookie_payload: dict,
    database_name: str,
    encryption_key: str | None = None,
    cookie_format: str = "json",
    captured_at: str | None = None,
    expires_at: str | None = None,
    status: str = "active",
) -> bool:
    """Store an encrypted Yahoo browser-session payload in Fly."""
    if not CRYPTO_AVAILABLE:
        print("Warning: cryptography not available - cannot encrypt Yahoo cookie credentials")
        return False
    metadata = validate_cookie_jar_payload(cookie_payload)
    key = encryption_key or get_encryption_key()
    if not key:
        print("Warning: No encryption key available")
        return False
    try:
        encrypted = encrypt_token(json.dumps(cookie_payload, sort_keys=True, separators=(",", ":")), key)
        from multi_league.core.fly_writer import FlyWriter

        values = {
            "league_id": league_id,
            "league_name": league_name,
            "database_name": database_name,
            "encrypted": encrypted,
            "cookie_format": cookie_format or metadata["format"],
            "captured_at": captured_at,
            "expires_at": expires_at,
            "status": status or "active",
        }
        safe = {name: _sql_literal(value) for name, value in values.items()}
        captured_expr = safe["captured_at"] if captured_at else "current_timestamp"
        expires_expr = safe["expires_at"] if expires_at else "NULL"
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS main;
        CREATE TABLE IF NOT EXISTS main.yahoo_web_credentials (
            league_id TEXT PRIMARY KEY,
            league_name TEXT,
            database_name TEXT UNIQUE,
            encrypted_cookie_jar TEXT NOT NULL,
            cookie_format TEXT NOT NULL DEFAULT 'json',
            captured_at TIMESTAMP DEFAULT current_timestamp,
            expires_at TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'active',
            updated_at TIMESTAMP DEFAULT current_timestamp
        );
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS league_name TEXT;
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS database_name TEXT;
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS encrypted_cookie_jar TEXT;
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS cookie_format TEXT DEFAULT 'json';
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP;
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active';
        ALTER TABLE main.yahoo_web_credentials ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
        DELETE FROM main.yahoo_web_credentials
        WHERE league_id = {safe['league_id']}
           OR database_name = {safe['database_name']};
        INSERT INTO main.yahoo_web_credentials
            (league_id, league_name, database_name, encrypted_cookie_jar,
             cookie_format, captured_at, expires_at, status, updated_at)
        VALUES
            ({safe['league_id']}, {safe['league_name']}, {safe['database_name']},
             {safe['encrypted']}, {safe['cookie_format']}, {captured_expr},
             {expires_expr}, {safe['status']}, current_timestamp);

        CREATE SCHEMA IF NOT EXISTS accounts;
        CREATE TABLE IF NOT EXISTS accounts.league_inventory (
            database_name       VARCHAR PRIMARY KEY,
            platform            VARCHAR,
            league_name         VARCHAR,
            league_id           VARCHAR,
            tier                VARCHAR DEFAULT 'free',
            entitled_mode       VARCHAR DEFAULT 'quick',
            last_import_mode    VARCHAR,
            last_import_at      TIMESTAMP,
            in_centralized      BOOLEAN DEFAULT FALSE,
            num_teams           INTEGER,
            first_year          INTEGER,
            last_year           INTEGER,
            scoring_variant     VARCHAR,
            has_credentials     BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP DEFAULT current_timestamp,
            updated_at          TIMESTAMP DEFAULT current_timestamp
        );
        ALTER TABLE accounts.league_inventory ADD COLUMN IF NOT EXISTS platform VARCHAR;
        ALTER TABLE accounts.league_inventory ADD COLUMN IF NOT EXISTS league_name VARCHAR;
        ALTER TABLE accounts.league_inventory ADD COLUMN IF NOT EXISTS league_id VARCHAR;
        ALTER TABLE accounts.league_inventory ADD COLUMN IF NOT EXISTS has_credentials BOOLEAN DEFAULT FALSE;
        ALTER TABLE accounts.league_inventory ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT current_timestamp;
        UPDATE accounts.league_inventory
        SET platform = 'yahoo', league_name = {safe['league_name']}, league_id = {safe['league_id']},
            has_credentials = TRUE, updated_at = current_timestamp
        WHERE database_name = {safe['database_name']};
        INSERT INTO accounts.league_inventory
            (database_name, platform, league_name, league_id, tier, entitled_mode,
             has_credentials, created_at, updated_at)
        SELECT {safe['database_name']}, 'yahoo', {safe['league_name']}, {safe['league_id']},
               'free', 'quick', TRUE, current_timestamp, current_timestamp
        WHERE NOT EXISTS (
            SELECT 1 FROM accounts.league_inventory
            WHERE database_name = {safe['database_name']}
        );
        """
        FlyWriter().execute(sql, database="___ops")
        print(f"Stored encrypted Yahoo web credentials for {league_name} ({league_id}) -> {database_name}")
        return True
    except Exception as exc:
        print(f"Error storing Yahoo web credentials: {exc}")
        return False


def store_yahoo_cookie_credentials(
    league_id: str,
    league_name: str,
    cookie_payload: dict,
    database_name: str,
    encryption_key: str | None = None,
    cookie_format: str = "json",
    captured_at: str | None = None,
    expires_at: str | None = None,
    status: str = "active",
) -> bool:
    """Persist encrypted Yahoo browser cookies in the Fly backend."""
    validate_cookie_jar_payload(cookie_payload)
    if not _is_fly_backend():
        raise RuntimeError("Yahoo cookie credential storage requires DATABASE_BACKEND=fly")
    return _store_yahoo_cookie_credentials_fly(
        league_id=league_id,
        league_name=league_name,
        cookie_payload=cookie_payload,
        database_name=database_name,
        encryption_key=encryption_key,
        cookie_format=cookie_format,
        captured_at=captured_at,
        expires_at=expires_at,
        status=status,
    )


def retrieve_yahoo_cookie_credentials(reader, database_name: str, encryption_key: str | None = None) -> dict | None:
    """Read and decrypt one active Yahoo cookie credential from Fly.

    The returned mapping is for the worker boundary and contains the payload
    only in memory. Callers must not serialize it into logs or manifests.
    """
    safe_db = str(database_name).replace("'", "''")
    rows = reader.query(
        "SELECT league_id, league_name, database_name, encrypted_cookie_jar, "
        "cookie_format, captured_at, expires_at, status "
        f"FROM main.yahoo_web_credentials WHERE database_name = '{safe_db}'",
        database="___ops",
    )
    if not rows:
        return None
    row = rows[0]
    status = str(row.get("status") or "active").lower()
    if status != "active":
        raise ValueError(f"Yahoo web credential for {database_name} is not active")
    expires_at = row.get("expires_at")
    if expires_at:
        expiry = expires_at if isinstance(expires_at, datetime) else datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry <= datetime.now(timezone.utc):
            raise ValueError(f"Yahoo web credential for {database_name} is expired")
    key = encryption_key or get_encryption_key()
    if not key:
        raise ValueError("CREDENTIAL_ENCRYPTION_KEY is required to decrypt Yahoo web credentials")
    payload = json.loads(decrypt_token(str(row["encrypted_cookie_jar"]), key))
    metadata = validate_cookie_jar_payload(payload)
    return {
        "league_id": row.get("league_id"),
        "league_name": row.get("league_name"),
        "database_name": row.get("database_name"),
        "cookie_format": row.get("cookie_format") or metadata["format"],
        "captured_at": row.get("captured_at"),
        "expires_at": expires_at,
        "status": status,
        "cookie_payload": payload,
    }


def store_league_credentials(
    league_id: str,
    league_name: str,
    refresh_token: str,
    database_name: str,
    encryption_key: str | None = None,
    motherduck_token: str | None = None,
) -> bool:
    """
    Store encrypted credentials for a league.

    Args:
        league_id: Yahoo league ID
        league_name: League name
        refresh_token: OAuth refresh token to store
        database_name: Database name
        encryption_key: Fernet encryption key (from env if not provided)
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        True if successful, False otherwise
    """
    if _is_fly_backend():
        return _store_league_credentials_fly(
            league_id=league_id,
            league_name=league_name,
            refresh_token=refresh_token,
            database_name=database_name,
            encryption_key=encryption_key,
        )

    if not DUCKDB_AVAILABLE:
        print("Warning: duckdb not available - cannot store credentials")
        return False

    if not CRYPTO_AVAILABLE:
        print("Warning: cryptography not available - cannot encrypt credentials")
        return False

    key = encryption_key or get_encryption_key()
    if not key:
        print("Warning: No encryption key available")
        return False

    md_token = motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
    if not md_token:
        print("Warning: No database token available")
        return False

    try:
        # Encrypt the refresh token
        encrypted = encrypt_token(refresh_token, key)

        # Store in database
        os.environ["MOTHERDUCK_TOKEN"] = md_token
        con = duckdb.connect("md:")

        # Create credentials table if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS ___ops.league_credentials (
                league_id TEXT PRIMARY KEY,
                league_name TEXT,
                database_name TEXT,
                encrypted_refresh_token TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Delete existing row (if any) then insert
        con.execute(
            """
            DELETE FROM ___ops.league_credentials WHERE league_id = ?
        """,
            [league_id],
        )
        con.execute(
            """
            INSERT INTO ___ops.league_credentials
            (league_id, league_name, database_name, encrypted_refresh_token, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
            [league_id, league_name, database_name, encrypted],
        )

        # Update league_inventory: mark has_credentials and last_import_at
        try:
            _upsert_league_inventory(con, database_name, "yahoo", league_name, league_id)
            con.execute(
                """
                UPDATE ___ops.accounts.league_inventory
                SET has_credentials = TRUE, last_import_at = current_timestamp
                WHERE database_name = ?
            """,
                [database_name],
            )
        except Exception:
            pass

        con.close()
        print(f"Stored credentials for {league_name} ({league_id})")
        return True

    except Exception as e:
        print(f"Error storing credentials: {e}")
        return False


def store_sleeper_league(
    sleeper_league_id: str, league_name: str, database_name: str, motherduck_token: str | None = None
) -> bool:
    """
    Store Sleeper league info in MotherDuck (no encryption needed - API is public).

    Args:
        sleeper_league_id: Sleeper league ID
        league_name: League name
        database_name: MotherDuck database name
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        True if successful, False otherwise
    """
    if _is_fly_backend():
        print(f"[credential_store] Fly backend — skipping store_sleeper_league for {database_name} (non-fatal)")
        return True

    if not DUCKDB_AVAILABLE:
        print("Warning: duckdb not available - cannot store Sleeper league")
        return False

    md_token = motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
    if not md_token:
        print("Warning: No database token available")
        return False

    try:
        os.environ["MOTHERDUCK_TOKEN"] = md_token
        con = duckdb.connect("md:")

        # Create Sleeper leagues table if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS ___ops.sleeper_leagues (
                sleeper_league_id TEXT PRIMARY KEY,
                league_name TEXT,
                database_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Delete existing row (if any) then insert
        con.execute(
            """
            DELETE FROM ___ops.sleeper_leagues WHERE sleeper_league_id = ?
        """,
            [sleeper_league_id],
        )
        con.execute(
            """
            INSERT INTO ___ops.sleeper_leagues
            (sleeper_league_id, league_name, database_name, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """,
            [sleeper_league_id, league_name, database_name],
        )

        # Update league_inventory: Sleeper always has credentials (public API)
        try:
            _upsert_league_inventory(con, database_name, "sleeper", league_name, sleeper_league_id)
            con.execute(
                """
                UPDATE ___ops.accounts.league_inventory
                SET has_credentials = TRUE, last_import_at = current_timestamp
                WHERE database_name = ?
            """,
                [database_name],
            )
        except Exception:
            pass

        con.close()
        print(f"Stored Sleeper league: {league_name} ({sleeper_league_id})")
        return True

    except Exception as e:
        print(f"Error storing Sleeper league: {e}")
        return False


def get_sleeper_league(sleeper_league_id: str, motherduck_token: str | None = None) -> dict | None:
    """
    Retrieve Sleeper league info.

    Args:
        sleeper_league_id: Sleeper league ID
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        Dict with league_name, database_name or None if not found
    """
    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        safe_id = str(sleeper_league_id).replace("'", "''")
        rows = reader.query(
            f"SELECT league_name, database_name FROM main.sleeper_leagues WHERE sleeper_league_id = '{safe_id}'",
            database="___ops",
        )
        if rows:
            return {"league_name": rows[0]["league_name"], "database_name": rows[0]["database_name"]}
        return None

    except Exception as e:
        print(f"Error retrieving Sleeper league: {e}")
        return None


def store_espn_league(
    espn_league_id: int,
    league_name: str,
    database_name: str,
    espn_s2: str = "",
    swid: str = "",
    encryption_key: str | None = None,
    motherduck_token: str | None = None,
) -> bool:
    """
    Store ESPN league info in MotherDuck, with optional encrypted cookies.

    Public leagues are stored without cookies. Private leagues have their
    espn_s2 and SWID cookies encrypted with Fernet.

    Args:
        espn_league_id: ESPN league ID (numeric)
        league_name: League name
        database_name: MotherDuck database name
        espn_s2: ESPN S2 cookie value (empty for public leagues)
        swid: ESPN SWID cookie value (empty for public leagues)
        encryption_key: Fernet encryption key (from env if not provided)
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        True if successful, False otherwise
    """
    if _is_fly_backend():
        print(f"[credential_store] Fly backend — skipping store_espn_league for {database_name} (non-fatal)")
        return True

    if not DUCKDB_AVAILABLE:
        print("Warning: duckdb not available - cannot store ESPN league")
        return False

    md_token = motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
    if not md_token:
        print("Warning: No database token available")
        return False

    # Encrypt cookies if present
    encrypted_s2 = None
    encrypted_swid = None
    if espn_s2 and swid:
        if not CRYPTO_AVAILABLE:
            print("Warning: cryptography not available - storing league without cookies")
        else:
            key = encryption_key or get_encryption_key()
            if key:
                encrypted_s2 = encrypt_token(espn_s2, key)
                encrypted_swid = encrypt_token(swid, key)
            else:
                print("Warning: No encryption key - storing league without cookies")

    try:
        os.environ["MOTHERDUCK_TOKEN"] = md_token
        con = duckdb.connect("md:")

        # Create ESPN leagues table if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS ___ops.espn_leagues (
                espn_league_id INTEGER PRIMARY KEY,
                league_name TEXT,
                database_name TEXT,
                encrypted_espn_s2 TEXT,
                encrypted_swid TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        existing = con.execute(
            """
            SELECT encrypted_espn_s2, encrypted_swid
            FROM ___ops.espn_leagues
            WHERE espn_league_id = ?
        """,
            [espn_league_id],
        ).fetchone()

        # Preserve previously stored cookies when this call only updates
        # metadata for a public import or a re-import without cookies.
        final_s2 = encrypted_s2 if encrypted_s2 is not None else (existing[0] if existing else None)
        final_swid = encrypted_swid if encrypted_swid is not None else (existing[1] if existing else None)

        if existing:
            con.execute(
                """
                UPDATE ___ops.espn_leagues
                SET league_name = ?, database_name = ?, encrypted_espn_s2 = ?, encrypted_swid = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE espn_league_id = ?
            """,
                [league_name, database_name, final_s2, final_swid, espn_league_id],
            )
        else:
            con.execute(
                """
                INSERT INTO ___ops.espn_leagues
                (espn_league_id, league_name, database_name, encrypted_espn_s2, encrypted_swid, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                [espn_league_id, league_name, database_name, final_s2, final_swid],
            )

        # Update league_inventory: mark credentials if cookies stored
        try:
            has_creds = final_s2 is not None and final_s2 != ""
            _upsert_league_inventory(con, database_name, "espn", league_name, str(espn_league_id))
            if has_creds:
                con.execute(
                    """
                    UPDATE ___ops.accounts.league_inventory
                    SET has_credentials = TRUE, last_import_at = current_timestamp
                    WHERE database_name = ?
                """,
                    [database_name],
                )
        except Exception:
            pass

        con.close()
        print(f"Stored ESPN league: {league_name} ({espn_league_id})")
        return True

    except Exception as e:
        print(f"Error storing ESPN league: {e}")
        return False


def get_espn_league(espn_league_id: int, motherduck_token: str | None = None) -> dict | None:
    """
    Retrieve ESPN league info (without decrypting cookies).

    Args:
        espn_league_id: ESPN league ID
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        Dict with league_name, database_name or None if not found
    """
    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        rows = reader.query(
            f"SELECT league_name, database_name FROM main.espn_leagues WHERE espn_league_id = {int(espn_league_id)}",
            database="___ops",
        )
        if rows:
            return {"league_name": rows[0]["league_name"], "database_name": rows[0]["database_name"]}
        return None

    except Exception as e:
        print(f"Error retrieving ESPN league: {e}")
        return None


def retrieve_espn_credentials(
    database_name: str,
    encryption_key: str | None = None,
    motherduck_token: str | None = None,
    reader=None,
) -> dict | None:
    """
    Retrieve and decrypt ESPN credentials for a league by database_name.

    Args:
        database_name: MotherDuck database name
        encryption_key: Fernet encryption key (from env if not provided)
        motherduck_token: MotherDuck token (from env if not provided)
        reader: Optional existing Fly reader for a caller that has already
            opened the canonical database connection.

    Returns:
        Dict with league_id, league_name, espn_s2, swid or None if not found
    """
    if not CRYPTO_AVAILABLE:
        return None

    key = encryption_key or get_encryption_key()
    if not key:
        return None

    try:
        if reader is None:
            from multi_league.core.db_reader import get_reader

            reader = get_reader()
        safe_db = str(database_name).replace("'", "''")
        rows = reader.query(
            f"SELECT espn_league_id, league_name, database_name, "
            f"encrypted_espn_s2, encrypted_swid "
            f"FROM main.espn_leagues WHERE database_name = '{safe_db}'",
            database="___ops",
        )

        if rows:
            return _decrypt_espn_credentials_row(rows[0], key)
        return None

    except Exception as e:
        print(f"Error retrieving ESPN credentials for {database_name}: {e}")
        return None


def retrieve_espn_credentials_by_league_id(
    espn_league_id: int | str, encryption_key: str | None = None, motherduck_token: str | None = None
) -> dict | None:
    """Retrieve and decrypt ESPN credentials by ESPN league id."""
    if not CRYPTO_AVAILABLE:
        return None

    key = encryption_key or get_encryption_key()
    if not key:
        return None

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        rows = reader.query(
            "SELECT espn_league_id, league_name, database_name, "
            "encrypted_espn_s2, encrypted_swid "
            f"FROM main.espn_leagues WHERE espn_league_id = {int(espn_league_id)} "
            "ORDER BY encrypted_espn_s2 IS NULL, encrypted_swid IS NULL, updated_at DESC "
            "LIMIT 1",
            database="___ops",
        )

        if rows:
            return _decrypt_espn_credentials_row(rows[0], key)
        return None

    except Exception as e:
        print(f"Error retrieving ESPN credentials for league id {espn_league_id}: {e}")
        return None


def _decrypt_espn_credentials_row(row: dict, key: str) -> dict | None:
    encrypted_s2 = row.get("encrypted_espn_s2")
    encrypted_swid = row.get("encrypted_swid")
    if not encrypted_s2 or not encrypted_swid:
        return None
    return {
        "league_id": row["espn_league_id"],
        "league_name": row["league_name"],
        "database_name": row["database_name"],
        "espn_s2": decrypt_token(encrypted_s2, key),
        "swid": decrypt_token(encrypted_swid, key),
    }


def get_league_credentials(
    league_id: str, encryption_key: str | None = None, motherduck_token: str | None = None
) -> str | None:
    """
    Retrieve and decrypt credentials for a league.

    Args:
        league_id: Yahoo league ID
        encryption_key: Fernet encryption key (from env if not provided)
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        Decrypted refresh token or None if not found
    """
    if not CRYPTO_AVAILABLE:
        return None

    key = encryption_key or get_encryption_key()
    if not key:
        return None

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        safe_id = str(league_id).replace("'", "''")
        rows = reader.query(
            f"SELECT encrypted_refresh_token FROM main.league_credentials WHERE league_id = '{safe_id}'",
            database="___ops",
        )

        if rows:
            return decrypt_token(rows[0]["encrypted_refresh_token"], key)
        return None

    except Exception as e:
        print(f"Error retrieving credentials: {e}")
        return None


def retrieve_league_credentials(
    database_name: str, encryption_key: str | None = None, motherduck_token: str | None = None
) -> dict | None:
    """
    Retrieve and decrypt credentials for a league by database_name.

    This is the function used by weekly_update_worker.yml to look up
    stored credentials for existing leagues.

    Args:
        database_name: MotherDuck database name (e.g., 'balla_s', 'kmffl')
        encryption_key: Fernet encryption key (from env if not provided)
        motherduck_token: MotherDuck token (from env if not provided)

    Returns:
        Dict with league_id, league_name, database_name, refresh_token
        or None if not found
    """
    if not CRYPTO_AVAILABLE:
        return None

    key = encryption_key or get_encryption_key()
    if not key:
        return None

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        safe_db = str(database_name).replace("'", "''")
        rows = reader.query(
            f"SELECT league_id, league_name, database_name, encrypted_refresh_token "
            f"FROM main.league_credentials WHERE database_name = '{safe_db}'",
            database="___ops",
        )

        if rows:
            row = rows[0]
            return {
                "league_id": row["league_id"],
                "league_name": row["league_name"],
                "database_name": row["database_name"],
                "refresh_token": decrypt_token(row["encrypted_refresh_token"], key),
            }
        return None

    except Exception as e:
        print(f"Error retrieving credentials for {database_name}: {e}")
        return None
