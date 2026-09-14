"""
Seed the Fly.io DuckDB server with the ___ops database from MotherDuck.

Usage:
    python scripts/seed_fly_ops.py

Requires env vars (from .env at project root):
    MOTHERDUCK_TOKEN
    DATABASE_SERVER_URL
    DATABASE_ADMIN_TOKEN
"""

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_SERVER_URL = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
DATABASE_ADMIN_TOKEN = os.environ.get("DATABASE_ADMIN_TOKEN")
LOCAL_PATH = str(Path(os.environ.get("TEMP", "C:/Temp")) / "___ops_export.duckdb")


def check_env():
    missing = []
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        missing.append("MOTHERDUCK_TOKEN")
    if not DATABASE_SERVER_URL:
        missing.append("DATABASE_SERVER_URL")
    if not DATABASE_ADMIN_TOKEN:
        missing.append("DATABASE_ADMIN_TOKEN")
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)


def export_ops():
    """Run the export in a subprocess so DuckDB file handles are fully released."""
    print("[1/5] Exporting ___ops from MotherDuck (in subprocess)...")

    # Clean up old files
    for f in (LOCAL_PATH, LOCAL_PATH + ".wal"):
        if os.path.exists(f):
            os.remove(f)

    script = f"""
import duckdb, os

local_conn = duckdb.connect(r"{LOCAL_PATH}")
local_conn.execute("INSTALL motherduck")
local_conn.execute("LOAD motherduck")
local_conn.execute("SET motherduck_token = '" + os.environ["MOTHERDUCK_TOKEN"] + "'")
local_conn.execute("ATTACH 'md:___ops' AS md_ops (READ_ONLY)")

all_tables = local_conn.execute(
    "SELECT database, schema, name FROM (SHOW ALL TABLES) WHERE database = 'md_ops'"
).fetchall()

schema_tables = {{}}
for _db, schema, table in all_tables:
    schema_tables.setdefault(schema, []).append(table)

print(f"  Found schemas: {{list(schema_tables.keys())}}")
print(f"  Total tables: {{sum(len(t) for t in schema_tables.values())}}")

for schema_name, tables in schema_tables.items():
    print(f"  Processing schema: {{schema_name}}")
    if schema_name != "main":
        local_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{{schema_name}}"')
    for table_name in tables:
        src = f'md_ops."{{schema_name}}"."{{table_name}}"'
        dst = f'"{{schema_name}}"."{{table_name}}"'
        print(f"    Copying {{schema_name}}.{{table_name}}...", end=" ", flush=True)
        local_conn.execute(f"CREATE TABLE {{dst}} AS SELECT * FROM {{src}}")
        count = local_conn.execute(f"SELECT COUNT(*) FROM {{dst}}").fetchone()[0]
        print(f"({{count:,}} rows)")

local_conn.execute("DETACH md_ops")
local_conn.execute("CHECKPOINT")
local_conn.close()
print("EXPORT_DONE")
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=False,
        timeout=600,
    )

    if result.returncode != 0:
        print("ERROR: Export subprocess failed")
        sys.exit(1)

    # Clean WAL after subprocess exits (handles are released)
    wal_path = LOCAL_PATH + ".wal"
    if os.path.exists(wal_path):
        os.remove(wal_path)

    file_size = os.path.getsize(LOCAL_PATH)
    print(f"\n  Export complete: {file_size / (1024*1024):.1f} MB")


def compute_sha256() -> str:
    """Compute SHA-256 hex digest of the exported file."""
    print("[4/5] Computing SHA-256 checksum...")
    h = hashlib.sha256()
    with open(LOCAL_PATH, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    digest = h.hexdigest()
    print(f"  SHA-256: {digest}")
    return digest


def upload_to_fly(checksum: str):
    """Upload the DuckDB file to the Fly.io server via POST /replace-db."""
    print("[5/5] Uploading to Fly.io server...")

    url = f"{DATABASE_SERVER_URL}/replace-db"
    headers = {
        "Authorization": f"Bearer {DATABASE_ADMIN_TOKEN}",
        "X-Db-Name": "___ops",
        "X-Content-SHA256": checksum,
    }

    file_size = os.path.getsize(LOCAL_PATH)
    print(f"  Target: {url}")
    print(f"  File size: {file_size / (1024*1024):.1f} MB")

    with open(LOCAL_PATH, "rb") as f:
        response = requests.post(
            url,
            headers=headers,
            files={"file": ("___ops.duckdb", f, "application/octet-stream")},
            timeout=600,
        )

    print(f"  Response: {response.status_code}")
    if response.status_code == 200:
        print(f"  Body: {response.text}")
    else:
        print(f"  ERROR: {response.status_code} - {response.text}")
        sys.exit(1)


def verify_server():
    """Hit GET /ready to verify the server is healthy after upload."""
    print("\nVerifying server health...")
    url = f"{DATABASE_SERVER_URL}/ready"
    try:
        resp = requests.get(url, timeout=30)
        print(f"  GET /ready -> {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"  WARNING: Could not reach server: {e}")


def main():
    check_env()
    export_ops()
    checksum = compute_sha256()
    upload_to_fly(checksum)

    # Clean up local export
    if os.path.exists(LOCAL_PATH):
        os.remove(LOCAL_PATH)
        print("Cleaned up local export file.")

    verify_server()
    print("\nDone!")


if __name__ == "__main__":
    main()
