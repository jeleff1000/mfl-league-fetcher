import pandas as pd

from scripts.refresh_yahoo_active_season import normalize_yahoo_roster_provider_identity


def test_raw_yahoo_player_id_is_promoted_to_canonical_provider_identity() -> None:
    raw = pd.DataFrame(
        {
            "player_id": ["33477", "33501"],
            "player_name": ["Player One", "Player Two"],
            "year": [2026, 2026],
            "week": [1, 1],
        }
    )

    normalized = normalize_yahoo_roster_provider_identity(raw)

    assert normalized["yahoo_player_id"].tolist() == ["33477", "33501"]
    assert "yahoo_player_id" not in raw.columns


def test_existing_canonical_identity_is_preserved_and_only_blanks_are_filled() -> None:
    raw = pd.DataFrame(
        {
            "player_id": ["raw-1", "raw-2"],
            "yahoo_player_id": ["canonical-1", None],
        }
    )

    normalized = normalize_yahoo_roster_provider_identity(raw)

    assert normalized["yahoo_player_id"].tolist() == ["canonical-1", "raw-2"]
