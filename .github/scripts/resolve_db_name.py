#!/usr/bin/env python3
"""
Resolve the database name for a league.

Single source of truth for database name resolution across all import workflows.
Prints the resolved name to stdout (last line).

Usage:
    python .github/scripts/resolve_db_name.py \
        --league-id <id> --league-name <name> --platform <yahoo|sleeper|espn> \
        [--database-name <pre-computed>]
"""

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.request


def fly_query(sql, database="___ops"):
    """Query via Fly.io database server."""
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/") + "/query"
    token = os.environ["DATABASE_READ_TOKEN"]
    data = json.dumps({"sql": sql, "database": database}).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


_DB_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_YAHOO_LEAGUE_ID_RE = re.compile(r"^\d+\.l\.\d+$|^\d+$")
_SLEEPER_LEAGUE_ID_RE = re.compile(r"^[a-zA-Z0-9_]{1,64}$")


def _safe_db_name(name: str) -> str:
    """Validate a database name matches sanitize_database_name() output. Raises on bad input."""
    if not name or not _DB_NAME_RE.match(name):
        raise ValueError(f"Invalid database name (must match {_DB_NAME_RE.pattern}): {name!r}")
    return name


def _safe_league_id(league_id: str, platform: str) -> str:
    """Validate platform-specific league_id format. Raises on bad input."""
    s = str(league_id)
    if platform == "yahoo":
        if not _YAHOO_LEAGUE_ID_RE.match(s):
            raise ValueError(f"Invalid Yahoo league_id: {s!r}")
    elif platform == "sleeper":
        if not _SLEEPER_LEAGUE_ID_RE.match(s):
            raise ValueError(f"Invalid Sleeper league_id: {s!r}")
    elif platform == "espn":
        if not s.isdigit():
            raise ValueError(f"Invalid ESPN league_id (must be numeric): {s!r}")
    else:
        raise ValueError(f"Unknown platform: {platform!r}")
    return s


def slugify(name: str) -> str:
    """Convert league name to a valid database name.
    Matches sanitize_database_name() in db_utils.py."""
    x = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not x:
        return "league"
    if x[0].isdigit():
        x = f"l_{x}"
    return x[:63]


def extract_yahoo_league_number(yahoo_league_key: str) -> str | None:
    """Extract stable numeric suffix from Yahoo league key.
    '449.l.198278' -> '198278', '198278' -> '198278', else None."""
    if not yahoo_league_key:
        return None
    key_str = str(yahoo_league_key).strip()
    m = re.match(r"^\d+\.l\.(\d+)$", key_str)
    if m:
        return m.group(1)
    if key_str.isdigit():
        return key_str
    return None


def check_db_exists(db_name: str) -> bool:
    """Check if a database exists via Fly.io."""
    try:
        safe_db = _safe_db_name(db_name)
        result = fly_query(
            f"SELECT 1 FROM information_schema.schemata WHERE catalog_name = '{safe_db}' LIMIT 1",
            database=safe_db,
        )
        return len(result) > 0
    except Exception:
        return False


def check_league_id_in_db(db_name: str, league_id: str, platform: str) -> bool:
    """Check if a league_id exists in a database's matchup table."""
    try:
        safe_db = _safe_db_name(db_name)
        safe_id = _safe_league_id(league_id, platform)
        if platform == "yahoo":
            yahoo_number = extract_yahoo_league_number(safe_id)
            if yahoo_number:
                result = fly_query(
                    f"SELECT 1 FROM public.matchup "
                    f"WHERE CAST(league_id AS VARCHAR) LIKE '%.l.{yahoo_number}' "
                    "LIMIT 1",
                    database=safe_db,
                )
                return len(result) > 0

        result = fly_query(
            f"SELECT 1 FROM public.matchup WHERE CAST(league_id AS VARCHAR) = '{safe_id}' LIMIT 1",
            database=safe_db,
        )
        return len(result) > 0
    except Exception:
        return False


def lookup_mapping_table(league_id: str, platform: str) -> str | None:
    """Check the ___ops mapping table for an existing database name."""
    try:
        safe_id = _safe_league_id(league_id, platform)
        if platform == "yahoo":
            yahoo_number = extract_yahoo_league_number(safe_id)
            if yahoo_number:
                result = fly_query(
                    "SELECT database_name FROM main.league_credentials "
                    f"WHERE CAST(league_id AS VARCHAR) LIKE '%.l.{yahoo_number}' "
                    "LIMIT 1"
                )
                if result and result[0].get("database_name"):
                    return result[0]["database_name"]
        elif platform == "sleeper":
            result = fly_query(
                f"SELECT database_name FROM main.sleeper_leagues WHERE sleeper_league_id = '{safe_id}' LIMIT 1"
            )
            if result and result[0].get("database_name"):
                return result[0]["database_name"]
        elif platform == "espn":
            result = fly_query(
                f"SELECT database_name FROM main.espn_leagues WHERE espn_league_id = {int(safe_id)} LIMIT 1"
            )
            if result and result[0].get("database_name"):
                return result[0]["database_name"]
    except Exception as e:
        print(f"[resolve] Mapping table lookup failed for {platform}: {e}", file=sys.stderr)
    return None


def lookup_inventory_owner(db_name: str) -> dict | None:
    """Return league_inventory ownership for a db name, if present."""
    try:
        safe_db = _safe_db_name(db_name)
        result = fly_query(
            "SELECT database_name, platform, league_id, league_name "
            "FROM accounts.league_inventory "
            f"WHERE database_name = '{safe_db}' "
            "LIMIT 1",
        )
        if result:
            return result[0]
    except Exception as e:
        print(f"[resolve] league_inventory owner lookup failed for {db_name}: {e}", file=sys.stderr)
    return None


def lookup_verified_renewal_ids(db_name: str, platform: str) -> set[str]:
    """Return exact league IDs already proven to belong to this canonical DB."""
    safe_db = _safe_db_name(db_name)
    ids: set[str] = set()
    try:
        rows = fly_query(
            "SELECT observed_manifest_json FROM accounts.league_update_manifests "
            f"WHERE database_name = '{safe_db}' LIMIT 1",
        )
        if rows and rows[0].get("observed_manifest_json"):
            manifest = json.loads(rows[0]["observed_manifest_json"])
            for segment in manifest.get("segments") or []:
                if str(segment.get("provider") or "").lower() != platform:
                    continue
                active_id = str(segment.get("active_league_id") or "").strip()
                if active_id:
                    ids.add(active_id)
                for pair in segment.get("renewal_chain") or []:
                    if isinstance(pair, (list, tuple)) and len(pair) == 2 and pair[1]:
                        ids.add(str(pair[1]).strip())
    except Exception as e:
        print(f"[resolve] Verified update-chain lookup failed for {db_name}: {e}", file=sys.stderr)
    try:
        rows = fly_query(
            "SELECT league_id, league_ids_json FROM public.league_context "
            f"WHERE db_name = '{safe_db}' LIMIT 1",
            database="___leagues",
        )
        if rows:
            current_id = str(rows[0].get("league_id") or "").strip()
            if current_id:
                ids.add(current_id)
            encoded = rows[0].get("league_ids_json")
            if encoded:
                values = json.loads(encoded)
                if isinstance(values, dict):
                    ids.update(str(value).strip() for value in values.values() if value)
    except Exception as e:
        print(f"[resolve] Saved league-chain lookup failed for {db_name}: {e}", file=sys.stderr)
    return ids


def same_or_verified_inventory_owner(owner: dict | None, league_id: str, platform: str) -> bool:
    if same_inventory_owner(owner, league_id, platform):
        return True
    if not owner or str(owner.get("platform") or "").lower() != platform:
        return False
    db_name = str(owner.get("database_name") or "").strip()
    return bool(db_name and str(league_id) in lookup_verified_renewal_ids(db_name, platform))


def lookup_inventory_identity(base_name: str, league_id: str, platform: str) -> dict | None:
    """Return an existing base or hashed db for this exact platform league identity."""
    try:
        safe_base = _safe_db_name(base_name)
        safe_id = _safe_league_id(league_id, platform)
        prefix = f"{safe_base}_"
        result = fly_query(
            "SELECT database_name, platform, league_id, league_name "
            "FROM accounts.league_inventory "
            f"WHERE LOWER(platform) = '{platform}' "
            f"AND CAST(league_id AS VARCHAR) = '{safe_id}' "
            "AND ("
            f"database_name = '{safe_base}' "
            f"OR substr(database_name, 1, {len(prefix)}) = '{prefix}'"
            ") "
            "ORDER BY "
            f"CASE WHEN database_name = '{safe_base}' THEN 0 ELSE 1 END, "
            "updated_at DESC NULLS LAST, database_name "
            "LIMIT 1",
        )
        if result:
            return result[0]
    except Exception as e:
        print(f"[resolve] league_inventory identity lookup failed for {platform}:{league_id}: {e}", file=sys.stderr)
    return None


def same_inventory_owner(owner: dict | None, league_id: str, platform: str) -> bool:
    if not owner:
        return False
    return str(owner.get("platform", "")).lower() == platform and str(owner.get("league_id", "")) == str(league_id)


def provisional_inventory_owner(owner: dict | None, platform: str) -> bool:
    """Return True for same-platform inventory rows that do not yet carry identity.

    Checkout and payment flows can create a league_inventory reservation before the
    import has written its platform league_id. Those rows should not force a
    hash-suffixed database name when a platform registry or payload already knows
    the canonical slug.
    """
    if not owner:
        return False
    owner_platform = str(owner.get("platform") or "").strip().lower()
    owner_league_id = str(owner.get("league_id") or "").strip()
    return owner_platform == platform and not owner_league_id


def choose_hashed_name(base_name: str, league_id: str, platform: str) -> str:
    """Return a stable hash-suffixed db name that preserves the suffix."""
    digest = hashlib.md5(f"{platform}:{league_id}".encode()).hexdigest()
    for length in (4, 6, 8, 12):
        suffix = digest[:length]
        base_room = max(1, 63 - len(suffix) - 1)
        candidate = f"{base_name[:base_room]}_{suffix}"
        owner = lookup_inventory_owner(candidate)
        if (
            not owner
            or same_inventory_owner(owner, league_id, platform)
            or provisional_inventory_owner(owner, platform)
        ):
            return candidate
    raise RuntimeError("Unable to resolve a unique database name")


def check_registry_collision(base_name: str, league_id: str, platform: str) -> bool:
    """Check if base_name is registered to a DIFFERENT league in ANY platform's registry.

    Returns True if another league owns the name (collision), False if free.
    """
    safe_base = _safe_db_name(base_name)
    registry_queries = [
        (
            "yahoo",
            f"SELECT league_id FROM main.league_credentials WHERE database_name = '{safe_base}' LIMIT 1",
        ),
        (
            "sleeper",
            f"SELECT sleeper_league_id as league_id FROM main.sleeper_leagues WHERE database_name = '{safe_base}' LIMIT 1",
        ),
        (
            "espn",
            f"SELECT espn_league_id as league_id FROM main.espn_leagues WHERE database_name = '{safe_base}' LIMIT 1",
        ),
    ]
    for reg_platform, query in registry_queries:
        try:
            result = fly_query(query)
            if result and result[0].get("league_id"):
                registered_id = str(result[0]["league_id"])
                # Same platform + same league = reimport, not collision
                if reg_platform == platform:
                    if platform == "yahoo":
                        own_number = extract_yahoo_league_number(league_id)
                        reg_number = extract_yahoo_league_number(registered_id)
                        if own_number and reg_number and own_number == reg_number:
                            continue
                    elif registered_id == str(league_id):
                        continue
                # Different league or different platform owns this name
                print(
                    f"[resolve] Registry collision: {base_name} owned by " f"{reg_platform} league {registered_id}",
                    file=sys.stderr,
                )
                return True
        except Exception:
            pass
    return False


def resolve(league_id: str, league_name: str, platform: str, pre_computed_db: str = "") -> str:
    """Resolve the database name. Returns the name string."""

    server_url = os.environ.get("DATABASE_SERVER_URL", "")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not server_url or not read_token:
        print("[resolve] WARNING: No DATABASE_SERVER_URL/DATABASE_READ_TOKEN, using slugified name", file=sys.stderr)
        return slugify(league_name)

    try:
        base_name = slugify(league_name)

        base_owner = lookup_inventory_owner(base_name)
        if base_owner:
            if same_or_verified_inventory_owner(base_owner, league_id, platform):
                print(f"[resolve] league_inventory owns base name for this league: {base_name}", file=sys.stderr)
                return base_name
            if provisional_inventory_owner(base_owner, platform):
                print(
                    f"[resolve] league_inventory has provisional base row without league_id: {base_name}",
                    file=sys.stderr,
                )
            else:
                hashed_name = choose_hashed_name(base_name, league_id, platform)
                owner_label = base_owner.get("league_name") or base_owner.get("database_name")
                print(
                    f"[resolve] league_inventory collision: {base_name} owned by {owner_label}, using: {hashed_name}",
                    file=sys.stderr,
                )
                return hashed_name

        # 1. An explicit target is the caller's canonical name, so honor it
        # before a temporary credential/inventory mapping.  It remains subject
        # to the same live inventory and registry collision checks as every
        # automatically resolved name.
        if pre_computed_db:
            pre_owner = lookup_inventory_owner(pre_computed_db)
            if (
                pre_owner
                and not same_or_verified_inventory_owner(pre_owner, league_id, platform)
                and not provisional_inventory_owner(pre_owner, platform)
            ):
                hashed_name = choose_hashed_name(base_name, league_id, platform)
                print(
                    f"[resolve] Pre-computed '{pre_computed_db}' collides in league_inventory, "
                    f"using: {hashed_name}",
                    file=sys.stderr,
                )
                return hashed_name
            if provisional_inventory_owner(pre_owner, platform):
                print(
                    f"[resolve] Pre-computed '{pre_computed_db}' has provisional inventory owner; trusting registry/payload",
                    file=sys.stderr,
                )
            if check_registry_collision(pre_computed_db, league_id, platform):
                print(
                    f"[resolve] Pre-computed '{pre_computed_db}' collides with another league, resolving fresh",
                    file=sys.stderr,
                )
            else:
                if check_db_exists(pre_computed_db):
                    print(f"[resolve] Using pre-computed database_name: {pre_computed_db}", file=sys.stderr)
                else:
                    print(
                        f"[resolve] Trusting pre-computed database_name without catalog: {pre_computed_db}",
                        file=sys.stderr,
                    )
                return pre_computed_db

        # 2. Fly league_inventory is the live source of truth. This catches
        # cross-platform collisions even when legacy credential registries are stale.
        inventory_identity = lookup_inventory_identity(base_name, league_id, platform)
        if inventory_identity and inventory_identity.get("database_name"):
            mapped = inventory_identity["database_name"]
            print(f"[resolve] Found in league_inventory: {mapped}", file=sys.stderr)
            return mapped

        # 3. Check mapping table for THIS league, but do not let stale
        # mappings override an inventory owner for another league.
        mapped = lookup_mapping_table(league_id, platform)
        if mapped:
            mapped_owner = lookup_inventory_owner(mapped)
            if (
                not mapped_owner
                or same_or_verified_inventory_owner(mapped_owner, league_id, platform)
                or provisional_inventory_owner(mapped_owner, platform)
            ):
                print(f"[resolve] Found in {platform} mapping table: {mapped}", file=sys.stderr)
                return mapped
            print(
                f"[resolve] Ignoring stale {platform} mapping table db '{mapped}' "
                f"owned by {mapped_owner.get('platform')}:{mapped_owner.get('league_id')}",
                file=sys.stderr,
            )

        # 4. Slugify and check for legacy collisions
        print(f"[resolve] Not in mapping table, checking base name: {base_name}", file=sys.stderr)

        # 4a. Check ALL registry tables for ownership (catches deleted-but-registered DBs)
        if check_registry_collision(base_name, league_id, platform):
            hashed_name = choose_hashed_name(base_name, league_id, platform)
            print(f"[resolve] Registry collision, using: {hashed_name}", file=sys.stderr)
            return hashed_name

        if not check_db_exists(base_name):
            print(f"[resolve] Database {base_name} does not exist, will create", file=sys.stderr)
            return base_name

        # Base DB exists - check if it's the same league (reimport)
        if check_league_id_in_db(base_name, league_id, platform):
            print(f"[resolve] League {league_id} found in {base_name}, reusing (reimport)", file=sys.stderr)
            return base_name

        # Different league owns the base name - collision
        hashed_name = choose_hashed_name(base_name, league_id, platform)
        print(f"[resolve] Collision detected, using: {hashed_name}", file=sys.stderr)
        return hashed_name

    except Exception as e:
        print(f"[resolve] Error during resolution: {e}", file=sys.stderr)
        return slugify(league_name)


def main():
    parser = argparse.ArgumentParser(description="Resolve database name for a league")
    parser.add_argument("--league-id", required=True, help="Platform-specific league ID")
    parser.add_argument("--league-name", required=True, help="Human-readable league name")
    parser.add_argument("--platform", required=True, choices=["yahoo", "sleeper", "espn"])
    parser.add_argument("--database-name", default="", help="Pre-computed database name (trust if DB exists)")
    args = parser.parse_args()

    resolved = resolve(args.league_id, args.league_name, args.platform, args.database_name)
    print(resolved)


if __name__ == "__main__":
    main()
