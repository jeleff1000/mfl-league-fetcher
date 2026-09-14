from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PARSE_ACTION = ROOT / ".github" / "actions" / "parse-league-data" / "action.yml"


def test_shared_yahoo_credential_owner_is_exposed_to_workers():
    """A clone can read another league's credential without claiming ownership of it."""
    action = PARSE_ACTION.read_text(encoding="utf-8")

    assert "credential_database_name:" in action
    assert "credential_owner_db" in action
    assert 'f.write(f"credential_database_name={credential_owner_db}\\n")' in action
    assert "credential_owner_league_name" in action
    assert 'f.write(f"credential_league_name={credential_owner_league_name}\\n")' in action
