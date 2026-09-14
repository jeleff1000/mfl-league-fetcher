"""Generate the negative-test fixtures listed in the spec.
Run once: python tests/fixtures/schema_conform/build_fixtures.py
"""

from pathlib import Path
import pandas as pd

OUT = Path(__file__).parent

BASE_MATCHUP = pd.DataFrame(
    {
        "year": [2014] * 10,
        "week": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        "manager": ["Adin", "Marc"] * 5,
        "manager_guid": ["3FJVUGAHEWQKCU2Z2TQQQMPWEA", "FYW5PMKZ23OGUAONDX6AGROCZQ"] * 5,
        "team_name": ["TeamA", "TeamB"] * 5,
        "opponent": ["Marc", "Adin"] * 5,
        "team_points": [100.0, 110.0] * 5,
        "opponent_points": [95.0, 105.0] * 5,
    }
)

# 1. mangled_manager_guid: rename + truncate values
mangled = BASE_MATCHUP.copy()
mangled = mangled.rename(columns={"manager_guid": "mystery_id"})
mangled["mystery_id"] = mangled["mystery_id"].str[:10]
mangled.to_parquet(OUT / "mangled_manager_guid.parquet")

# 2. whitespace_manager: trailing spaces / mixed case
whitespace = BASE_MATCHUP.copy()
whitespace["manager"] = ["  Adin ", "MARC"] * 5
whitespace.to_parquet(OUT / "whitespace_manager.parquet")

# 3. swapped_guid_columns: manager/opponent guid swapped
swapped = BASE_MATCHUP.copy()
swapped["opponent_guid"] = swapped["manager_guid"].iloc[::-1].values  # reversed
swapped.to_parquet(OUT / "swapped_guid_columns.parquet")

# 4. missing_year: year column dropped entirely
missing = BASE_MATCHUP.drop(columns=["year"])
missing.to_parquet(OUT / "missing_year.parquet")

# 5. collision_dual_guids: two guid-shape columns
collision = BASE_MATCHUP.copy()
collision["another_guid"] = collision["manager_guid"].str[::-1]  # reversed Yahoo guid
collision.to_parquet(OUT / "collision_dual_guids.parquet")

# 6. ambiguous_fuzzy_name: 'Dan' when canonical has Daniel + Dave, NO guid column present.
# With no manager_guid column the orchestrator must derive it from name via fuzzy lookup.
# 'Dan' scores 66 vs 'daniel' and 57 vs 'adin' — both below FUZZY_CUTOFF=90, so
# derivation returns None, Dan gets a synthetic external_<hash> id, not a canonical one.
ambiguous = BASE_MATCHUP.drop(columns=["manager_guid"]).copy()
ambiguous["manager"] = ["Dan", "Marc"] * 5  # Dan is below fuzzy cutoff, Marc resolves fine
ambiguous.to_parquet(OUT / "ambiguous_fuzzy_name.parquet")

# 7. inconsistent_team_key: draft uses different team_key format
inconsistent = pd.DataFrame(
    {
        "year": [2014] * 5,
        "round": [1, 2, 3, 4, 5],
        "pick": [1, 2, 3, 4, 5],
        "manager": ["Adin"] * 5,
        "team_key": ["FOREIGN_X", "FOREIGN_Y", "FOREIGN_Z", "FOREIGN_W", "FOREIGN_V"],
        "yahoo_player_id": ["1001", "1002", "1003", "1004", "1005"],
        "player": ["P1", "P2", "P3", "P4", "P5"],
        "cost": [10.0] * 5,
    }
)
inconsistent.to_parquet(OUT / "inconsistent_team_key_draft.parquet")

# 8. aliased_columns: mgr_id, pts, txn_id naming
aliased = BASE_MATCHUP.copy()
aliased = aliased.rename(
    columns={
        "manager_guid": "mgr_id",
        "team_points": "team_pts",
        "opponent_points": "opp_pts",
    }
)
aliased.to_parquet(OUT / "aliased_columns.parquet")

print("Fixtures generated:", sorted(p.name for p in OUT.glob("*.parquet")))
