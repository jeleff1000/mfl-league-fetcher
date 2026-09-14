#!/usr/bin/env python3
"""Crawl every Yahoo settings document reachable from the Fly credential bank."""

from __future__ import annotations

import argparse
import gc
import json
import os
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
FFS_ROOT = ROOT / "fantasy_football_data_scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(FFS_ROOT) not in sys.path:
    sys.path.insert(0, str(FFS_ROOT))

from multi_league.utils.credential_store import decrypt_token_with_key_rotation

from scripts.yahoo_corpus.client import RequestBudgetExceeded, YahooCensusClient
from scripts.yahoo_corpus.settings_parser import (
    classify_settings,
    extract_renewal_keys,
    parse_settings_xml,
)
from scripts.yahoo_corpus.store import CensusStore


DEFAULT_OUTPUT = Path("D:/league-history-data/fantasy_leagues/yahoo_settings_census")
CREDENTIAL_SQL = """
SELECT database_name, league_id, league_name, encrypted_refresh_token
FROM main.league_credentials
WHERE encrypted_refresh_token IS NOT NULL
  AND length(encrypted_refresh_token) > 0
ORDER BY database_name
"""


@dataclass(frozen=True)
class CredentialGrant:
    grant_id: str
    refresh_token: str = field(repr=False)
    source_databases: tuple[str, ...]

    @property
    def credential_rows(self) -> int:
        return len(self.source_databases)


@dataclass(frozen=True)
class CredentialLoadResult:
    grants: tuple[CredentialGrant, ...]
    total_rows: int
    failed_rows: int


def load_env() -> None:
    """Load a local repo environment without overriding the invoking shell."""
    candidates = [ROOT / ".env"]
    candidates.extend(parent / ".env" for parent in ROOT.parents if (parent / ".git").exists())
    for path in candidates:
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))
        return


def load_credential_groups(
    reader: Any,
    encryption_keys: list[str],
    *,
    decryptor: Callable[[str, list[str]], str] = decrypt_token_with_key_rotation,
) -> CredentialLoadResult:
    """Read Fly credentials and collapse identical plaintext grants in memory."""
    rows = reader.query(CREDENTIAL_SQL, database="___ops")
    grouped: dict[str, list[str]] = {}
    failures = 0
    for row in rows:
        try:
            plaintext = decryptor(str(row["encrypted_refresh_token"]), encryption_keys)
        except Exception:
            failures += 1
            continue
        grouped.setdefault(plaintext, []).append(str(row.get("database_name") or ""))
    grants = tuple(
        CredentialGrant(
            grant_id=f"grant-{index:04d}",
            refresh_token=token,
            source_databases=tuple(databases),
        )
        for index, (token, databases) in enumerate(grouped.items(), start=1)
    )
    return CredentialLoadResult(grants=grants, total_rows=len(rows), failed_rows=failures)


def _linked_league(key: str, settings: dict[str, Any]) -> dict[str, Any]:
    metadata = settings.get("metadata") or {}
    game_key, _, league_id = key.partition(".l.")
    return {
        "league_key": key,
        "league_id": league_id,
        "name": str(metadata.get("name") or ""),
        "season": str(metadata.get("season") or ""),
        "num_teams": int(metadata.get("num_teams") or 0),
        "game_key": game_key,
    }


def crawl(
    grants: list[CredentialGrant] | tuple[CredentialGrant, ...],
    store: CensusStore,
    client_factory: Callable[[CredentialGrant], Any],
    *,
    force_refetch: bool = False,
) -> dict[str, Any]:
    """Crawl grants serially, checkpointing every league and credential."""
    for grant in grants:
        existing_credential = store.credential_rows.get(grant.grant_id)
        if existing_credential and existing_credential.get("status") == "success" and not force_refetch:
            continue
        client = client_factory(grant)
        discovered_count = 0
        try:
            client.refresh()
            games = client.discover_games()
            direct_leagues: list[dict[str, Any]] = []
            season_failures = 0
            for game in games:
                try:
                    direct_leagues.extend(client.discover_leagues(game))
                except RequestBudgetExceeded:
                    raise
                except Exception:
                    season_failures += 1

            queue: deque[dict[str, Any]] = deque(direct_leagues)
            attempted: set[str] = set()
            while queue:
                league = queue.popleft()
                league_key = str(league.get("league_key") or "")
                if not league_key or league_key in attempted:
                    continue
                attempted.add(league_key)
                discovered_count += 1

                if store.is_complete(league_key) and not force_refetch:
                    for linked_key in store.league_rows[league_key].get("renewal_keys", []):
                        if linked_key not in attempted:
                            queue.append(_linked_league(linked_key, {}))
                    continue

                try:
                    xml_text = client.fetch_settings_xml(league_key)
                    settings = parse_settings_xml(xml_text, league_key)
                    classification = classify_settings(settings)
                    renewal_keys = extract_renewal_keys(xml_text)
                    xml_result = store.write_xml(league_key, xml_text)
                    metadata = settings.get("metadata") or {}
                    row = {
                        "league_key": league_key,
                        "league_id": str(league.get("league_id") or league_key.partition(".l.")[2]),
                        "game_key": str(league.get("game_key") or league_key.partition(".l.")[0]),
                        "season": int(league.get("season") or metadata.get("season") or 0) or None,
                        "league_name": str(league.get("name") or metadata.get("name") or ""),
                        "num_teams": classification.get("num_teams") or metadata.get("num_teams"),
                        "fetch_status": "success",
                        "fetch_error": None,
                        "settings_sha256": xml_result.sha256,
                        "settings_path": str(xml_result.path),
                        "grant_id": grant.grant_id,
                        "renewal_keys": renewal_keys,
                        **classification,
                    }
                    store.record_league(row)
                    for linked_key in renewal_keys:
                        if linked_key not in attempted:
                            queue.append(_linked_league(linked_key, settings))
                except RequestBudgetExceeded:
                    raise
                except Exception as exc:
                    store.record_league(
                        {
                            "league_key": league_key,
                            "league_id": str(league.get("league_id") or league_key.partition(".l.")[2]),
                            "game_key": str(league.get("game_key") or league_key.partition(".l.")[0]),
                            "season": int(league.get("season") or 0) or None,
                            "league_name": str(league.get("name") or ""),
                            "fetch_status": "failure",
                            "fetch_error": type(exc).__name__,
                            "classification_status": "incomplete",
                            "classification_reason": "settings_fetch_or_parse_failed",
                            "cohort_slug": None,
                            "grant_id": grant.grant_id,
                            "renewal_keys": [],
                        }
                    )
                finally:
                    store.flush()

            store.record_credential(
                {
                    "grant_id": grant.grant_id,
                    "status": "success",
                    "credential_rows": grant.credential_rows,
                    "source_databases": list(grant.source_databases),
                    "games": len(games),
                    "league_keys_seen": discovered_count,
                    "season_failures": season_failures,
                    "requests": client.request_count,
                }
            )
        except RequestBudgetExceeded:
            store.record_credential(
                {
                    "grant_id": grant.grant_id,
                    "status": "failure",
                    "credential_rows": grant.credential_rows,
                    "source_databases": list(grant.source_databases),
                    "error_class": "RequestBudgetExceeded",
                    "league_keys_seen": discovered_count,
                    "requests": client.request_count,
                }
            )
            store.flush()
            raise
        except Exception as exc:
            store.record_credential(
                {
                    "grant_id": grant.grant_id,
                    "status": "failure",
                    "credential_rows": grant.credential_rows,
                    "source_databases": list(grant.source_databases),
                    "error_class": type(exc).__name__,
                    "league_keys_seen": discovered_count,
                    "requests": client.request_count,
                }
            )
        finally:
            store.flush()
            del client
            gc.collect()
    return store.summary()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credential-cap", type=int, default=None)
    parser.add_argument("--request-budget", type=int, default=25_000)
    parser.add_argument("--delay-ms", type=int, default=100)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--force-refetch", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env()
    required = {
        "YAHOO_CLIENT_ID": os.environ.get("YAHOO_CLIENT_ID"),
        "YAHOO_CLIENT_SECRET": os.environ.get("YAHOO_CLIENT_SECRET"),
    }
    missing = [name for name, value in required.items() if not value]
    keys = [
        key
        for key in (
            os.environ.get("CREDENTIAL_ENCRYPTION_KEY"),
            os.environ.get("CREDENTIAL_ENCRYPTION_KEY_NEW"),
        )
        if key
    ]
    if missing or not keys:
        print("Missing required Yahoo or credential-encryption environment configuration.")
        return 2

    from multi_league.core.db_reader import get_reader

    loaded = load_credential_groups(get_reader(), keys)
    grants = list(loaded.grants[: args.credential_cap]) if args.credential_cap else list(loaded.grants)
    store = CensusStore(args.output)
    if store.state_path.exists() and not args.resume and store.league_rows:
        print(f"Existing census state at {args.output}; rerun with --resume or use a new --output.")
        return 2

    for index in range(loaded.failed_rows):
        store.record_credential(
            {
                "grant_id": f"decrypt-failure-{index + 1:04d}",
                "status": "failure",
                "credential_rows": 1,
                "error_class": "CredentialDecryptFailure",
            }
        )
    store.flush()

    def client_factory(grant: CredentialGrant) -> YahooCensusClient:
        return YahooCensusClient(
            required["YAHOO_CLIENT_ID"],
            required["YAHOO_CLIENT_SECRET"],
            grant.refresh_token,
            request_budget=args.request_budget,
            delay_ms=args.delay_ms,
        )

    print(
        f"Yahoo census: {loaded.total_rows} credential rows, "
        f"{len(loaded.grants)} unique grants, {loaded.failed_rows} decrypt failures."
    )
    try:
        summary = crawl(grants, store, client_factory, force_refetch=args.force_refetch)
    except RequestBudgetExceeded:
        print("Request budget reached; progress is checkpointed. Rerun with --resume.")
        return 3
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Private census output: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
