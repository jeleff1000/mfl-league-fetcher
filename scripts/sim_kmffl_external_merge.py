"""End-to-end simulation: KMFFL parquets → apply_mappings → normalize → disambiguation → final franchise_ids.

Validates that 2013/2014 external matchup data ends up with the SAME franchise_id
as the canonical 2015+ Yahoo years for managers who exist in both eras (Ezra, Marc, etc.).

Run: python scripts/sim_kmffl_external_merge.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

from multi_league.core.canonical_matchup import normalize_matchup_df
from multi_league.external_ingest.apply_mappings import (
    ColumnMap,
    IdentityDecision,
    apply_mappings,
)
from multi_league.external_ingest.auto_map import auto_map
from multi_league.external_ingest.manifests import get_manifest

PARQUET_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\_cleanup\recommend_to_delete\fantasy_football_data_downloads_4.6GB\fantasy_football_data\KMFFL\kmffl_import"
)

# Canonical 2015+ franchise_ids per manager_guid (pulled from live Fly DB).
CANONICAL_FIDS = {
    "3FJVUGAHEWQKCU2Z2TQQQMPWEA": ("Adin", "3FJVUGAHEWQKCU2Z2TQQQMPWEA"),
    "KM4FIU3EKJKO4RYTLCNIPAIMWI": ("Daniel", "KM4FIU3EKJKO4RYTLCNIPAIMWI"),
    "QAWPKGQTG3HQVP4BPUGBG5SY2U": ("Eleff", "QAWPKGQTG3HQVP4BPUGBG5SY2U"),
    "SAQHTT6JGGOWN3ROP6QMGS6KTU": ("Ezra", "SAQHTT6JGGOWN3ROP6QMGS6KTU"),
    "GHZOUGTBZIYGQ6QOMLOD4FZLGA": ("Gavi", "GHZOUGTBZIYGQ6QOMLOD4FZLGA"),
    "46SSFIBGNE3ZZFEDG7GYGV3HN4": ("Jason", "46SSFIBGNE3ZZFEDG7GYGV3HN4"),
    "5KVNMK2CWFQTDQTVYQLOPGLXFQ": ("Jesse", "5KVNMK2CWFQTDQTVYQLOPGLXFQ"),
    "FYW5PMKZ23OGUAONDX6AGROCZQ": ("Marc", "FYW5PMKZ23OGUAONDX6AGROCZQ"),
    "JKFP2XZZK4L36MF2DI7CEWS3J4": ("Tani", "JKFP2XZZK4L36MF2DI7CEWS3J4"),
    "2OUWRYUIY4HHEW72H5YVBHPILA": ("Yaacov", "2OUWRYUIY4HHEW72H5YVBHPILA"),
}

# Identity decisions: maps (display_name_in_2014_parquet) -> canonical franchise_id
# Production wizard persists this dict; here we hardcode the canonical map.
IDENTITY = {
    # 2013 managers
    "Ezra": IdentityDecision(franchise_id="SAQHTT6JGGOWN3ROP6QMGS6KTU"),
    "Gavi": IdentityDecision(franchise_id="GHZOUGTBZIYGQ6QOMLOD4FZLGA"),
    "Jason": IdentityDecision(franchise_id="46SSFIBGNE3ZZFEDG7GYGV3HN4"),
    "Jesse": IdentityDecision(franchise_id="5KVNMK2CWFQTDQTVYQLOPGLXFQ"),
    "Marc": IdentityDecision(franchise_id="FYW5PMKZ23OGUAONDX6AGROCZQ"),
    "Tani": IdentityDecision(franchise_id="JKFP2XZZK4L36MF2DI7CEWS3J4"),
    "Yaacov": IdentityDecision(franchise_id="2OUWRYUIY4HHEW72H5YVBHPILA"),
    # 2013-only / 2014-only
    "Rubinstein": IdentityDecision(create_new=True),
    # 2014 only
    "Adin": IdentityDecision(franchise_id="3FJVUGAHEWQKCU2Z2TQQQMPWEA"),
    "Daniel": IdentityDecision(franchise_id="KM4FIU3EKJKO4RYTLCNIPAIMWI"),
    "Ilan": IdentityDecision(create_new=True),
}


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def simulate(scenario: str, drop_manager_guid_from_column_map: bool) -> pd.DataFrame:
    """Run apply_mappings + normalize_matchup_df + return the combined frame."""
    section(f"SCENARIO: {scenario}")

    out_frames = []
    for year in (2013, 2014):
        path = PARQUET_DIR / f"matchup_data_week_all_year_{year}.parquet"
        df = pd.read_parquet(path)
        df["league_id"] = "kmffl_test"

        manifest = get_manifest("matchup")
        am = auto_map(list(df.columns), manifest)
        cmap = dict(am.column_map)
        if drop_manager_guid_from_column_map:
            cmap.pop("manager_guid", None)
            cmap.pop("team_name", None)
            cmap.pop("team_key", None)
        print(f"[{year}] column_map slots: {sorted(cmap)}")

        result = apply_mappings(
            staged_dfs={"matchup": df},
            column_maps=[ColumnMap(table="matchup", column_map=cmap)],
            identity_decisions=IDENTITY,
            league_db="kmffl",
            platform="yahoo",
        )
        applied = result.dfs["matchup"]
        applied["league_id"] = "kmffl_test"
        guid_count = applied.get("manager_guid", pd.Series([], dtype=object)).notna().sum()
        print(f"[{year}] after apply_mappings: {len(applied)} rows, manager_guid populated: {guid_count}")

        normalized = normalize_matchup_df(applied, platform="yahoo", league_id="kmffl_test")
        fid_count = normalized["franchise_id"].notna().sum()
        hidden_count = normalized["franchise_id"].astype(str).str.startswith("hidden_").sum()
        print(
            f"[{year}] after normalize_matchup_df: {len(normalized)} rows, "
            f"franchise_id populated: {fid_count}, hidden_*: {hidden_count}"
        )
        normalized["year"] = year
        out_frames.append(normalized)

    return pd.concat(out_frames, ignore_index=True)


def verify(combined: pd.DataFrame, scenario: str) -> None:
    section(f"VERIFICATION: {scenario}")
    # For each manager that exists in both 2013/2014 + 2015+, check franchise_id matches canonical.
    by_mgr = (
        combined.groupby("manager")
        .agg(
            year_min=("year", "min"),
            year_max=("year", "max"),
            distinct_fids=("franchise_id", lambda s: sorted(s.dropna().unique().tolist())),
            row_count=("year", "count"),
        )
        .reset_index()
    )
    print(by_mgr.to_string(index=False))

    print()
    matches = 0
    misses = 0
    for _, row in by_mgr.iterrows():
        mgr = row["manager"]
        # Look up canonical fid by manager
        canonical_fid = next(
            (fid for fid, (canonical_mgr, _) in CANONICAL_FIDS.items() if canonical_mgr == mgr),
            None,
        )
        if canonical_fid is None:
            print(f"  [SKIP]  {mgr}: not in 2015+ canonical map")
            continue
        actual_fids = row["distinct_fids"]
        if actual_fids == [canonical_fid]:
            print(f"  [OK]    {mgr}: franchise_id = {canonical_fid} (matches 2015+ canonical)")
            matches += 1
        else:
            print(f"  [FAIL]  {mgr}: expected [{canonical_fid}], got {actual_fids}")
            misses += 1

    print(f"\nResult: {matches} matched / {misses} mismatched")
    if misses > 0:
        print("BUG: 2013/2014 managers will NOT merge with 2015+ canonical franchises.")
    else:
        print("OK: all 2013/2014 known managers will merge with their 2015+ canonical franchises.")


if __name__ == "__main__":
    # Scenario A: wizard column_map INCLUDES manager_guid/team_name/team_key (current auto_map output)
    combined_a = simulate(
        "A — wizard column_map INCLUDES manager_guid (current auto_map default)",
        drop_manager_guid_from_column_map=False,
    )
    verify(combined_a, "A")

    # Scenario B: wizard column_map MISSING manager_guid/team_name/team_key (simulating legacy state
    # where the deployed run had a column_map without these slots, which produced the
    # observed Fly state where 2014 manager_guid is NULL and franchise_id = hidden_<slug>)
    combined_b = simulate(
        "B — wizard column_map MISSING manager_guid/team_name/team_key (legacy)",
        drop_manager_guid_from_column_map=True,
    )
    verify(combined_b, "B")
