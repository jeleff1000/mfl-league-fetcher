"""Capture a Fly league publication generation before an import reads sources.

This dependency-minimal step runs before provider parsing or large cache
restoration. Its receipt is bound to the eventual resolved import lock key.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


_DB_NAME = re.compile(r"[A-Za-z0-9_]{1,128}\Z")
_TRANSIENT_HTTP = {429, 500, 502, 503, 504}


def parse_generation(db_name: str, rows: Any) -> int:
    if not isinstance(db_name, str) or not _DB_NAME.fullmatch(db_name):
        raise ValueError("db_name must be a canonical single-league lock key")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise ValueError("generation query must return one row")
    generation = rows[0].get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise ValueError("generation query returned no nonnegative integer")
    return generation


def read_generation(db_name: str, *, url: str, token: str) -> int:
    parse_generation(db_name, [{"generation": 0}])
    if not url or not token:
        raise RuntimeError("Fly read URL/token are required for import generation capture")
    sql = (
        "SELECT COALESCE(MAX(generation), 0) AS generation "
        "FROM merge_admin.league_publish_generations "
        f"WHERE db_name = '{db_name}'"
    )
    request = urllib.request.Request(
        url.rstrip("/") + "/query",
        data=json.dumps({"sql": sql, "database": "___leagues"}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                if response.status != 200:
                    raise RuntimeError(f"Fly generation query returned HTTP {response.status}")
                return parse_generation(db_name, json.loads(response.read()))
        except urllib.error.HTTPError as exc:
            if exc.code not in _TRANSIENT_HTTP or attempt == 2:
                raise RuntimeError(f"Fly generation query returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == 2:
                raise RuntimeError("Fly generation query was unavailable after three attempts") from exc
        time.sleep(1 << attempt)
    raise RuntimeError("Fly generation query exhausted retries")


def record_generation(
    generation: int,
    *,
    github_env: Path | None,
    github_output: Path | None,
) -> None:
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise ValueError("generation must be a nonnegative integer")
    if github_env is not None:
        with github_env.open("a", encoding="utf-8") as receipt:
            receipt.write(f"LEAGUE_IMPORT_BASE_GENERATION={generation}\n")
    if github_output is not None:
        with github_output.open("a", encoding="utf-8") as receipt:
            receipt.write(f"base_generation={generation}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-name", required=True)
    args = parser.parse_args()
    generation = read_generation(
        args.db_name,
        url=os.environ.get("DATABASE_SERVER_URL", ""),
        token=os.environ.get("DATABASE_READ_TOKEN", ""),
    )
    record_generation(
        generation,
        github_env=Path(os.environ["GITHUB_ENV"]) if os.environ.get("GITHUB_ENV") else None,
        github_output=Path(os.environ["GITHUB_OUTPUT"]) if os.environ.get("GITHUB_OUTPUT") else None,
    )
    print(f"Captured import snapshot generation {generation} for {args.db_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
