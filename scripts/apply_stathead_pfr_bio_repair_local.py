"""Apply safe Stathead PFR bio repairs to a local player_bio parquet copy.

This intentionally does not write to Fly. It consumes
stathead_pfr_bio_repair_candidates.csv and applies only:

- update_existing_pfr_id
- insert_minimal_stub

Rows marked review_* are left untouched for manual resolution.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PLAYER_BIO = REPO_ROOT / "ops_data" / "nfl_historical" / "player_bio.parquet"
DEFAULT_REPAIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\_catalog\pbp_stathead_nflverse_twin_parse_20260507"
    r"\stathead_pfr_bio_repair_candidates.csv"
)
DEFAULT_AUDIT_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\_catalog\pbp_stathead_nflverse_twin_parse_20260507"
)
DEFAULT_OUT = (
    Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
    / "nfl_historical_repair_artifacts_20260508"
    / "player_bio_stathead_pfr_repaired.parquet"
)


def value_or_none(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def infer_position_from_roles(roles: str) -> tuple[str | None, str | None, str | None]:
    role_set = {part.strip() for part in str(roles or "").split(",") if part.strip()}
    if "passer" in role_set:
        return "QB", "offense", "QB"
    if "receiver" in role_set:
        return "WR", "offense", "WR/TE"
    if "rusher" in role_set:
        return "RB", "offense", "RB"
    if "kicker" in role_set:
        return "K", "special_teams", "K"
    if "punter" in role_set:
        return "P", "special_teams", "P"
    if role_set & {
        "solo_tackle_1",
        "assist_tackle_1",
        "tackle_with_assist_1",
        "tackle_for_loss_1",
        "sack",
        "half_sack_1",
        "half_sack_2",
        "interception",
        "forced_fumble_player_1",
        "blocked",
    }:
        return "DEF", "defense", "IDP"
    return None, None, None


def make_stub(row: pd.Series, columns: list[str]) -> dict[str, Any]:
    position, position_side, position_category = infer_position_from_roles(row.get("stathead_roles", ""))
    first_year = float(row["stathead_first_year"])
    last_year = float(row["stathead_last_year"])

    stub = {col: None for col in columns}
    stub.update(
        {
            "NFL_player_id": row["pfr_id"],
            "player": row["player_name"],
            "nfl_position": position,
            "status": "historical_stathead_stub",
            "first_year": first_year,
            "last_year": last_year,
            "years_active": max(1.0, last_year - first_year + 1.0),
            "position_category": position_category,
            "position_side": position_side,
            "pfr_id": row["pfr_id"],
        }
    )
    return stub


def apply_repair(player_bio_path: Path, repair_path: Path, out_path: Path, audit_dir: Path) -> dict[str, Any]:
    audit_dir.mkdir(parents=True, exist_ok=True)
    original_schema = pq.ParquetFile(player_bio_path).schema_arrow
    bio = pd.read_parquet(player_bio_path)
    repair = pd.read_csv(repair_path)

    updates = repair[repair["action"] == "update_existing_pfr_id"].copy()
    stubs = repair[repair["action"] == "insert_minimal_stub"].copy()
    review = repair[~repair["action"].isin(["update_existing_pfr_id", "insert_minimal_stub"])].copy()

    applied_updates = []
    skipped_updates = []
    for _, row in updates.iterrows():
        target_id = value_or_none(row.get("target_NFL_player_id"))
        pfr_id = value_or_none(row.get("pfr_id"))
        if not target_id or not pfr_id:
            skipped_updates.append({**row.to_dict(), "skip_reason": "missing_target_or_pfr_id"})
            continue

        mask = bio["NFL_player_id"].astype(str) == str(target_id)
        if int(mask.sum()) != 1:
            skipped_updates.append({**row.to_dict(), "skip_reason": f"target_row_count={int(mask.sum())}"})
            continue
        existing = value_or_none(bio.loc[mask, "pfr_id"].iloc[0])
        if existing and existing != pfr_id:
            skipped_updates.append({**row.to_dict(), "skip_reason": f"existing_pfr_id={existing}"})
            continue
        bio.loc[mask, "pfr_id"] = pfr_id
        applied_updates.append(row.to_dict())

    stub_rows = [make_stub(row, list(bio.columns)) for _, row in stubs.iterrows()]
    if stub_rows:
        stub_df = pd.DataFrame(stub_rows, columns=bio.columns)
        # Avoid pandas' all-NA concat warning while preserving the original
        # player_bio columns from the left-hand frame.
        stub_df = stub_df.dropna(axis=1, how="all")
        bio = pd.concat([bio, stub_df], ignore_index=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(bio, schema=original_schema, preserve_index=False)
    pq.write_table(table, out_path, compression="zstd")

    applied_updates_path = audit_dir / "stathead_pfr_bio_applied_updates.csv"
    inserted_stubs_path = audit_dir / "stathead_pfr_bio_inserted_stubs.csv"
    skipped_updates_path = audit_dir / "stathead_pfr_bio_skipped_updates.csv"
    review_queue_path = audit_dir / "stathead_pfr_bio_review_queue.csv"
    pd.DataFrame(applied_updates).to_csv(applied_updates_path, index=False)
    pd.DataFrame(stub_rows, columns=bio.columns).to_csv(inserted_stubs_path, index=False)
    pd.DataFrame(skipped_updates).to_csv(skipped_updates_path, index=False)
    review.to_csv(review_queue_path, index=False)

    summary = {
        "input_player_bio": str(player_bio_path),
        "repair_candidates": str(repair_path),
        "output_player_bio": str(out_path),
        "input_rows": int(len(pd.read_parquet(player_bio_path, columns=["NFL_player_id"]))),
        "output_rows": int(len(bio)),
        "applied_update_rows": int(len(applied_updates)),
        "inserted_stub_rows": int(len(stub_rows)),
        "skipped_update_rows": int(len(skipped_updates)),
        "manual_review_rows": int(len(review)),
        "audit_outputs": {
            "applied_updates": str(applied_updates_path),
            "inserted_stubs": str(inserted_stubs_path),
            "skipped_updates": str(skipped_updates_path),
            "review_queue": str(review_queue_path),
        },
    }
    summary_path = audit_dir / "stathead_pfr_bio_local_apply_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-bio", type=Path, default=DEFAULT_PLAYER_BIO)
    parser.add_argument("--repair", type=Path, default=DEFAULT_REPAIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    args = parser.parse_args()

    summary = apply_repair(args.player_bio, args.repair, args.out, args.audit_dir)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
