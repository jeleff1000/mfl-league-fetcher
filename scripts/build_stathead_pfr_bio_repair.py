"""Build repair candidates for Stathead PFR IDs missing from player_bio.

The 1978-1998 Stathead PBP twin preserves PFR-linked player IDs for offensive,
defensive, and special-teams actors. This script compares those IDs to the local
player_bio cache and writes a small repair candidate file.

Important: this does not write to Fly. It classifies each missing PFR identity as
an existing player_bio row that should receive pfr_id, a row needing review, or a
minimal stub candidate.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TWIN = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\stathead_generated\pbp_backfill_1978_1998\stathead_pbp_1978_1998_nflverse_twin.parquet"
)
DEFAULT_PLAYER_BIO = REPO_ROOT / "ops_data" / "nfl_historical" / "player_bio.parquet"
DEFAULT_OUT_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\_catalog\pbp_stathead_nflverse_twin_parse_20260507"
)

PLAYER_ROLE_PREFIXES = [
    "passer",
    "receiver",
    "rusher",
    "interception",
    "punt_returner",
    "kickoff_returner",
    "punter",
    "kicker",
    "blocked",
    "tackle_for_loss_1",
    "tackle_for_loss_2",
    "forced_fumble_player_1",
    "forced_fumble_player_2",
    "solo_tackle_1",
    "solo_tackle_2",
    "assist_tackle_1",
    "assist_tackle_2",
    "assist_tackle_3",
    "assist_tackle_4",
    "tackle_with_assist_1",
    "tackle_with_assist_2",
    "fumbled_1",
    "fumbled_2",
    "fumble_recovery_1",
    "fumble_recovery_2",
    "sack",
    "half_sack_1",
    "half_sack_2",
    "safety",
]

PFR_ID_RE = r"^[A-Za-z]{4}[A-Za-z\.]{2}\d{2}$|^[a-z]{5,}[a-z]{3}\d{2}$"


def path_sql(path: Path) -> str:
    return str(path).replace("\\", "/")


def normalize_name(name: str | None) -> str:
    if not name:
        return ""
    text = name.lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def available_roles(con: duckdb.DuckDBPyConnection, twin_path: Path) -> list[str]:
    twin = path_sql(twin_path)
    roles = []
    for prefix in PLAYER_ROLE_PREFIXES:
        try:
            con.execute(f"SELECT {prefix}_player_name, {prefix}_player_id FROM read_parquet('{twin}') LIMIT 0")
            roles.append(prefix)
        except duckdb.Error:
            continue
    return roles


def build_role_union_sql(twin_path: Path, roles: list[str]) -> str:
    twin = path_sql(twin_path)
    pieces = []
    for role in roles:
        pieces.append(
            f"""
            SELECT
              '{role}' AS role_name,
              {role}_player_name AS player_name,
              regexp_replace({role}_player_id, '^pfr:', '') AS pfr_id_raw,
              lower(regexp_replace({role}_player_id, '^pfr:', '')) AS pfr_id_norm,
              season
            FROM read_parquet('{twin}')
            WHERE {role}_player_name IS NOT NULL
              AND {role}_player_id IS NOT NULL
            """
        )
    return "\nUNION ALL\n".join(pieces)


def build_unmatched(con: duckdb.DuckDBPyConnection, twin_path: Path, player_bio_path: Path) -> pd.DataFrame:
    roles = available_roles(con, twin_path)
    role_union = build_role_union_sql(twin_path, roles)
    bio = path_sql(player_bio_path)
    bio_identity_sql = f"""
    SELECT DISTINCT
      lower(
        COALESCE(
          NULLIF(pfr_id, ''),
          CASE
            WHEN regexp_matches(NFL_player_id, '{PFR_ID_RE}') THEN NFL_player_id
          END
        )
      ) AS pfr_id_norm
    FROM read_parquet('{bio}')
    WHERE COALESCE(
      NULLIF(pfr_id, ''),
      CASE
        WHEN regexp_matches(NFL_player_id, '{PFR_ID_RE}') THEN NFL_player_id
      END
    ) IS NOT NULL
    """

    return con.execute(
        f"""
        WITH ids AS ({role_union}),
        unique_ids AS (
          SELECT
            player_name,
            pfr_id_norm,
            min(pfr_id_raw) AS pfr_id_raw,
            min(season) AS stathead_first_year,
            max(season) AS stathead_last_year,
            count(*) AS stathead_role_rows,
            string_agg(DISTINCT role_name, ', ' ORDER BY role_name) AS stathead_roles
          FROM ids
          GROUP BY 1,2
        ),
        bio_ids AS ({bio_identity_sql})
        SELECT u.*
        FROM unique_ids u
        LEFT JOIN bio_ids b USING (pfr_id_norm)
        WHERE b.pfr_id_norm IS NULL
        ORDER BY stathead_role_rows DESC, player_name, pfr_id_raw
        """
    ).fetchdf()


def build_bio_candidates(player_bio_path: Path) -> pd.DataFrame:
    bio = pd.read_parquet(player_bio_path)
    bio = bio.copy()
    bio["player_norm"] = bio["player"].map(normalize_name)
    bio["has_pfr_identity"] = bio.apply(
        lambda row: bool(row.get("pfr_id")) or bool(re.match(PFR_ID_RE, str(row.get("NFL_player_id") or ""))),
        axis=1,
    )
    return bio


def classify(unmatched: pd.DataFrame, bio: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, missing in unmatched.iterrows():
        player_norm = normalize_name(str(missing["player_name"]))
        candidates = bio[bio["player_norm"] == player_norm].copy()
        first_year = int(missing["stathead_first_year"])
        last_year = int(missing["stathead_last_year"])

        def overlaps(
            row: pd.Series,
            *,
            first_year: int = first_year,
            last_year: int = last_year,
        ) -> bool:
            bio_first = row.get("first_year")
            bio_last = row.get("last_year")
            if pd.isna(bio_first) or pd.isna(bio_last):
                return False
            return int(bio_first) <= last_year and int(bio_last) >= first_year

        candidates["year_overlap"] = candidates.apply(overlaps, axis=1) if not candidates.empty else []
        update_candidates = candidates[(~candidates["has_pfr_identity"]) & (candidates["year_overlap"])]
        exact_no_pfr_candidates = candidates[~candidates["has_pfr_identity"]]

        if len(update_candidates) == 1:
            action = "update_existing_pfr_id"
            picked = update_candidates.iloc[0]
            candidate_count = len(candidates)
        elif len(update_candidates) > 1:
            action = "review_multiple_existing_overlap_candidates"
            picked = update_candidates.iloc[0]
            candidate_count = len(update_candidates)
        elif len(exact_no_pfr_candidates) == 1:
            action = "review_existing_name_no_year_overlap"
            picked = exact_no_pfr_candidates.iloc[0]
            candidate_count = len(candidates)
        elif len(candidates) > 0:
            action = "review_exact_name_candidates_all_have_pfr_or_no_overlap"
            picked = candidates.iloc[0]
            candidate_count = len(candidates)
        else:
            action = "insert_minimal_stub"
            picked = pd.Series(dtype=object)
            candidate_count = 0

        rows.append(
            {
                "action": action,
                "player_name": missing["player_name"],
                "pfr_id": missing["pfr_id_raw"],
                "stathead_first_year": first_year,
                "stathead_last_year": last_year,
                "stathead_role_rows": int(missing["stathead_role_rows"]),
                "stathead_roles": missing["stathead_roles"],
                "candidate_count": candidate_count,
                "target_NFL_player_id": picked.get("NFL_player_id"),
                "target_existing_pfr_id": picked.get("pfr_id"),
                "target_player": picked.get("player"),
                "target_position": picked.get("nfl_position"),
                "target_first_year": picked.get("first_year"),
                "target_last_year": picked.get("last_year"),
                "target_has_pfr_identity": bool(picked.get("has_pfr_identity", False)),
                "suggested_insert_NFL_player_id": missing["pfr_id_raw"] if action == "insert_minimal_stub" else "",
                "suggested_insert_pfr_id": missing["pfr_id_raw"] if action == "insert_minimal_stub" else "",
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--twin", type=Path, default=DEFAULT_TWIN)
    parser.add_argument("--player-bio", type=Path, default=DEFAULT_PLAYER_BIO)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    unmatched = build_unmatched(con, args.twin, args.player_bio)
    bio = build_bio_candidates(args.player_bio)
    repair = classify(unmatched, bio)
    if not repair.empty:
        update_mask = repair["action"] == "update_existing_pfr_id"
        duplicate_targets = set(
            repair.loc[update_mask, "target_NFL_player_id"].dropna().loc[lambda series: series.duplicated(keep=False)]
        )
        if duplicate_targets:
            repair.loc[
                update_mask & repair["target_NFL_player_id"].isin(duplicate_targets),
                "action",
            ] = "review_duplicate_update_target"

    unmatched_path = args.out_dir / "stathead_pfr_bio_unmatched_ids.csv"
    repair_path = args.out_dir / "stathead_pfr_bio_repair_candidates.csv"
    summary_path = args.out_dir / "stathead_pfr_bio_repair_summary.json"
    unmatched.to_csv(unmatched_path, index=False)
    repair.to_csv(repair_path, index=False)

    summary = {
        "twin": str(args.twin),
        "player_bio": str(args.player_bio),
        "unmatched_unique_pfr_name_ids": int(len(unmatched)),
        "repair_candidates": int(len(repair)),
        "action_counts": repair["action"].value_counts().to_dict() if not repair.empty else {},
        "outputs": {
            "unmatched_csv": str(unmatched_path),
            "repair_candidates_csv": str(repair_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
