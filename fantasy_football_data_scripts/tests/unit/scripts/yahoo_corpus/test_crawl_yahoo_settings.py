from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.yahoo_corpus.crawl_yahoo_settings import (
    CredentialGrant,
    build_parser,
    crawl,
    load_credential_groups,
)
from scripts.yahoo_corpus.store import CensusStore


class FakeReader:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.calls: list[tuple[str, str]] = []

    def query(self, sql: str, *, database: str):
        self.calls.append((sql, database))
        return self.rows


class FakeClient:
    def __init__(self, leagues: list[dict], xml: str, *, fail_refresh: bool = False):
        self.leagues = leagues
        self.xml = xml
        self.fail_refresh = fail_refresh
        self.request_count = 0
        self.fetches: list[str] = []

    def refresh(self) -> None:
        self.request_count += 1
        if self.fail_refresh:
            raise RuntimeError("sanitized auth failure")

    def discover_games(self) -> list[dict]:
        self.request_count += 1
        return [{"game_key": "461", "season": "2025"}]

    def discover_leagues(self, game: dict) -> list[dict]:
        self.request_count += 1
        return self.leagues

    def fetch_settings_xml(self, league_key: str) -> str:
        self.request_count += 1
        self.fetches.append(league_key)
        return self.xml


SETTINGS_XML = """<fantasy_content><league><league_key>461.l.12345</league_key>
<league_id>12345</league_id><name>Private</name><season>2025</season><num_teams>12</num_teams>
<settings><roster_positions>
<roster_position><position>QB</position><count>1</count></roster_position>
<roster_position><position>RB</position><count>2</count></roster_position>
<roster_position><position>WR</position><count>2</count></roster_position>
<roster_position><position>TE</position><count>1</count></roster_position>
<roster_position><position>W/R/T</position><count>1</count></roster_position>
</roster_positions><stat_categories><stats>
<stat><stat_id>4</stat_id><name>Passing Touchdowns</name><display_name>Passing Touchdowns</display_name></stat>
<stat><stat_id>10</stat_id><name>Receptions</name><display_name>Receptions</display_name></stat>
</stats></stat_categories><stat_modifiers><stats>
<stat><stat_id>4</stat_id><value>4</value></stat><stat><stat_id>10</stat_id><value>1</value></stat>
</stats></stat_modifiers></settings></league></fantasy_content>"""


def _credential_rows() -> list[dict]:
    return [
        {
            "database_name": "one",
            "league_id": "461.l.1",
            "league_name": "One",
            "encrypted_refresh_token": "cipher-a",
        },
        {
            "database_name": "two",
            "league_id": "461.l.2",
            "league_name": "Two",
            "encrypted_refresh_token": "cipher-b",
        },
        {
            "database_name": "three",
            "league_id": "461.l.3",
            "league_name": "Three",
            "encrypted_refresh_token": "cipher-c",
        },
    ]


def test_load_credential_groups_queries_fly_and_collapses_plaintext_tokens() -> None:
    reader = FakeReader(_credential_rows())
    plaintext = {"cipher-a": "same-token", "cipher-b": "same-token", "cipher-c": "other-token"}

    result = load_credential_groups(
        reader,
        ["old-key", "new-key"],
        decryptor=lambda ciphertext, keys: plaintext[ciphertext],
    )

    assert reader.calls[0][1] == "___ops"
    assert "FROM main.league_credentials" in reader.calls[0][0]
    assert result.total_rows == 3
    assert len(result.grants) == 2
    assert result.grants[0].credential_rows == 2
    assert "same-token" not in repr(result.grants[0])


def test_crawl_deduplicates_overlapping_leagues_and_resumes(tmp_path) -> None:
    league = {
        "league_key": "461.l.12345",
        "league_id": "12345",
        "name": "Private League",
        "season": "2025",
        "num_teams": 12,
        "game_key": "461",
    }
    clients: list[FakeClient] = []

    def factory(grant: CredentialGrant):
        client = FakeClient([league], SETTINGS_XML)
        clients.append(client)
        return client

    grants = [
        CredentialGrant("grant-0001", "token-a", ("one",)),
        CredentialGrant("grant-0002", "token-b", ("two",)),
    ]
    store = CensusStore(tmp_path)

    crawl(grants, store, factory)
    crawl(grants, CensusStore(tmp_path), factory)

    assert len(store.league_rows) == 1
    assert sum(len(client.fetches) for client in clients) == 1
    assert (tmp_path / "raw" / "461" / "12345.xml").exists()


def test_crawl_continues_after_failed_grant(tmp_path) -> None:
    grants = [
        CredentialGrant("grant-0001", "bad", ("one",)),
        CredentialGrant("grant-0002", "good", ("two",)),
    ]
    league = {
        "league_key": "461.l.12345",
        "league_id": "12345",
        "name": "Private League",
        "season": "2025",
        "num_teams": 12,
        "game_key": "461",
    }

    def factory(grant: CredentialGrant):
        return FakeClient([league], SETTINGS_XML, fail_refresh=grant.refresh_token == "bad")

    store = CensusStore(tmp_path)
    crawl(grants, store, factory)

    assert store.credential_rows["grant-0001"]["status"] == "failure"
    assert store.credential_rows["grant-0002"]["status"] == "success"
    assert store.summary()["unique_league_years"] == 1


def test_cli_supports_safety_and_resume_controls() -> None:
    args = build_parser().parse_args(
        [
            "--credential-cap",
            "3",
            "--request-budget",
            "100",
            "--output",
            str(Path("D:/private")),
            "--resume",
            "--force-refetch",
        ]
    )

    assert args.credential_cap == 3
    assert args.request_budget == 100
    assert args.resume is True
    assert args.force_refetch is True


def test_cli_file_runs_standalone() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/yahoo_corpus/crawl_yahoo_settings.py", "--help"],
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--credential-cap" in result.stdout
