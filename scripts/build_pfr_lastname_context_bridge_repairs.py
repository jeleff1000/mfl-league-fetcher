#!/usr/bin/env python3
"""Build high-confidence PFR bridge repairs from same-game surname matches.

This is for the post exact-name pass. It finds PFR rows still missing an
identity bridge, joins to supertable rows by game/team/opponent context and
surname, and only promotes low-delta unique matches into a derived local bio.

No Fly writes are performed.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from audit_pfr_boxscore_retabs_vs_fly_supertable import STAT_MAP  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_RETAB_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_retabs_20260512T134853Z"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_supertable_audit_bridge2_20260512T151709Z"
DEFAULT_BIO = (
    ORGANIZED_ROOT
    / "_catalog"
    / "pfr_identity_bridge_repairs_20260512T151649Z"
    / "player_bio_stathead_pfr_bridge_repaired.parquet"
)
DEFAULT_OUTPUT_ROOT = ORGANIZED_ROOT / "_catalog"


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def pfr_blank(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    return text.eq("") | text.str.lower().eq("none")


def build_candidates(
    con: duckdb.DuckDBPyConnection,
    retab_dir: Path,
    audit_dir: Path,
) -> pd.DataFrame:
    remaining = pd.read_csv(audit_dir / "pfr_ids_missing_identity_bridge.csv")
    con.register("remaining_ids", remaining[["pfr_id_norm"]])

    fact = sql_path(retab_dir / "pfr_player_game_fact.parquet")
    hist = sql_path(audit_dir / "fly_nfl_franchise_history.parquet")
    super_glob = sql_path(audit_dir / "fly_supertable_weekly_selected" / "*.parquet")

    stat_sum_expr = " + ".join(f"abs(COALESCE({q_ident(col)}, 0))" for col, _ in STAT_MAP)
    super_stat_sum_expr = " + ".join(f"abs(COALESCE(s.{q_ident(super_col)}, 0))" for _, super_col in STAT_MAP)
    delta_expr = " + ".join(
        f"abs(COALESCE(p.{q_ident(pfr_col)}, 0) - COALESCE(s.{q_ident(super_col)}, 0))"
        for pfr_col, super_col in STAT_MAP
    )
    pfr_last_expr = (
        "lower(regexp_extract("
        "regexp_replace(CAST(player AS VARCHAR), "
        "'\\s+(Jr\\.?|Sr\\.?|II|III|IV)$', '', 'i'), "
        "'([A-Za-z0-9]+)$', 1))"
    )
    super_last_expr = (
        "lower(regexp_extract("
        "regexp_replace(CAST(s.player AS VARCHAR), "
        "'\\s+(Jr\\.?|Sr\\.?|II|III|IV)$', '', 'i'), "
        "'([A-Za-z0-9]+)$', 1))"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE pfr AS
        WITH hist_aug AS (
            SELECT * FROM read_parquet('{hist}')
            UNION ALL SELECT 31 AS nfl_franchise_number, 'LVR' AS abbrev, 2020 AS start_year, 9999 AS end_year, true AS is_canonical
            UNION ALL SELECT 14 AS nfl_franchise_number, 'RAM' AS abbrev, 1936 AS start_year, 1945 AS end_year, true AS is_canonical
            UNION ALL SELECT 148 AS nfl_franchise_number, 'BOS' AS abbrev, 1944 AS start_year, 1945 AS end_year, true AS is_canonical
        ),
        base AS (
            SELECT
                *,
                lower(pfr_id) AS pfr_id_norm,
                {pfr_last_expr} AS last_norm,
                {stat_sum_expr} AS pfr_activity
            FROM read_parquet('{fact}')
            WHERE lower(pfr_id) IN (SELECT pfr_id_norm FROM remaining_ids)
        ),
        mapped AS (
            SELECT
                b.*,
                th.nfl_franchise_number AS team_franchise_number,
                oh.nfl_franchise_number AS opponent_franchise_number
            FROM base b
            LEFT JOIN hist_aug th
              ON b.team = th.abbrev
             AND b.season BETWEEN th.start_year AND th.end_year
            LEFT JOIN hist_aug oh
              ON b.opponent = oh.abbrev
             AND b.season BETWEEN oh.start_year AND oh.end_year
            QUALIFY row_number() OVER (
                PARTITION BY b.boxscore_id, b.team, b.pfr_player_game_key
                ORDER BY COALESCE(th.is_canonical, false) DESC,
                         COALESCE(oh.is_canonical, false) DESC,
                         th.nfl_franchise_number,
                         oh.nfl_franchise_number
            ) = 1
        )
        SELECT *
        FROM mapped
        WHERE pfr_activity > 1e-9
          AND last_norm <> ''
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE super AS
        SELECT
            s.*,
            {super_last_expr} AS last_norm,
            TRY_CAST(year AS INTEGER) AS season_i,
            TRY_CAST(week AS INTEGER) AS week_i,
            {super_stat_sum_expr} AS super_activity
        FROM read_parquet('{super_glob}', union_by_name=true) s
        WHERE player IS NOT NULL
          AND NFL_player_id IS NOT NULL
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE last_candidates AS
        SELECT
            p.pfr_id_norm,
            p.pfr_id,
            p.player AS pfr_player,
            p.season,
            p.pfr_week_num,
            p.team,
            p.opponent,
            p.last_norm,
            s.NFL_player_id AS super_NFL_player_id,
            s.player AS super_player,
            s.position AS super_position,
            {delta_expr} AS total_abs_delta,
            p.pfr_activity,
            s.super_activity
        FROM pfr p
        JOIN super s
          ON p.season = s.season_i
         AND p.pfr_week_num = s.week_i
         AND p.team_franchise_number = s.nfl_franchise_number
         AND (
             p.opponent_franchise_number = s.opponent_nfl_franchise_number
             OR p.opponent_franchise_number IS NULL
             OR s.opponent_nfl_franchise_number IS NULL
         )
         AND p.last_norm = s.last_norm
        """
    )
    return con.execute(
        """
        WITH best_row AS (
            SELECT *
            FROM last_candidates
            QUALIFY row_number() OVER (
                PARTITION BY pfr_id_norm, season, pfr_week_num, team
                ORDER BY total_abs_delta ASC, super_NFL_player_id
            ) = 1
        )
        SELECT
            pfr_id_norm,
            min(pfr_id) AS pfr_id,
            min(pfr_player) AS pfr_player,
            count(*) AS candidate_game_rows,
            count(DISTINCT super_NFL_player_id) AS unique_super_ids,
            string_agg(DISTINCT super_NFL_player_id, ';' ORDER BY super_NFL_player_id) AS super_ids,
            string_agg(DISTINCT super_player, ';' ORDER BY super_player) AS super_players,
            sum(CASE WHEN total_abs_delta <= 1e-9 THEN 1 ELSE 0 END) AS exact_rows,
            min(total_abs_delta) AS min_delta,
            median(total_abs_delta) AS median_delta,
            max(total_abs_delta) AS max_delta,
            min(season) AS first_season,
            max(season) AS last_season
        FROM best_row
        GROUP BY pfr_id_norm
        ORDER BY unique_super_ids, candidate_game_rows DESC
        """
    ).df()


def build_repairs(
    retab_dir: Path,
    audit_dir: Path,
    bio_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='3GB'")
    con.execute("SET preserve_insertion_order=false")

    candidates = build_candidates(con, retab_dir, audit_dir)
    candidates.to_csv(output_dir / "pfr_bridge_review_lastname_context_candidates.csv", index=False)

    bio = pd.read_parquet(bio_path)
    bio["pfr_id_blank"] = pfr_blank(bio["pfr_id"])
    enriched = candidates[candidates["unique_super_ids"].eq(1)].merge(
        bio[
            [
                "NFL_player_id",
                "player",
                "nfl_position",
                "first_year",
                "last_year",
                "pfr_id",
                "pfr_id_blank",
            ]
        ],
        left_on="super_ids",
        right_on="NFL_player_id",
        how="left",
        suffixes=("_pfr", "_bio"),
    )
    if "pfr_id_pfr" in enriched.columns:
        enriched = enriched.rename(columns={"pfr_id_pfr": "pfr_id"})
    high_confidence = enriched[
        enriched["pfr_id_blank"].fillna(False)
        & enriched["candidate_game_rows"].ge(2)
        & (
            enriched["exact_rows"].ge(1)
            | enriched["median_delta"].le(2)
            | (enriched["candidate_game_rows"].ge(10) & enriched["median_delta"].le(5))
        )
        & enriched["max_delta"].le(15)
    ].copy()
    high_confidence["repair_action"] = "fill_existing_bio_pfr_id"
    high_confidence["repair_confidence"] = "high"
    high_confidence["repair_reason"] = "unique same-game surname-context match with low stat delta"
    high_confidence = high_confidence.drop_duplicates(["pfr_id_norm", "NFL_player_id"], keep="first")
    target_counts = high_confidence.groupby("NFL_player_id")["pfr_id_norm"].transform("nunique")
    high_confidence = high_confidence[target_counts.eq(1)].copy()
    high_confidence.sort_values(
        ["candidate_game_rows", "pfr_id"],
        ascending=[False, True],
    ).to_csv(
        output_dir / "pfr_bridge_fill_existing_lastname_context_high_confidence.csv",
        index=False,
    )

    repaired = bio.drop(columns=["pfr_id_blank"]).copy()
    fill_map = high_confidence.set_index("NFL_player_id")["pfr_id"].to_dict()
    repaired["pfr_id"] = repaired.apply(
        lambda row: fill_map.get(row["NFL_player_id"], row["pfr_id"]),
        axis=1,
    )
    repaired_bio_path = output_dir / "player_bio_stathead_pfr_bridge_repaired.parquet"
    repaired.to_parquet(repaired_bio_path, index=False)

    counts = {
        "remaining_bridge_ids_input": int(pd.read_csv(audit_dir / "pfr_ids_missing_identity_bridge.csv").shape[0]),
        "lastname_context_candidate_ids": int(len(candidates)),
        "high_confidence_fill_ids": int(high_confidence["pfr_id_norm"].nunique()),
        "high_confidence_candidate_game_rows": int(high_confidence["candidate_game_rows"].sum()),
        "repaired_bio_path": str(repaired_bio_path),
    }
    manifest = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "retab_dir": str(retab_dir),
        "audit_dir": str(audit_dir),
        "bio_path": str(bio_path),
        "output_dir": str(output_dir),
        "counts": counts,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    top = high_confidence.sort_values("candidate_game_rows", ascending=False).head(15)
    lines = [
        "# PFR Lastname Context Bridge Repair",
        "",
        f"Generated: {manifest['generated_at_utc']}",
        f"Audit dir: `{audit_dir}`",
        f"Base bio: `{bio_path}`",
        "",
        "## Summary",
        "",
        f"- Remaining bridge IDs entering pass: {counts['remaining_bridge_ids_input']:,}",
        f"- Lastname-context candidate IDs: {counts['lastname_context_candidate_ids']:,}",
        f"- High-confidence fills: {counts['high_confidence_fill_ids']:,}",
        f"- Candidate game rows covered: {counts['high_confidence_candidate_game_rows']:,}",
        "",
        "## Top Fills",
        "",
        top[
            [
                "pfr_id",
                "pfr_player",
                "candidate_game_rows",
                "exact_rows",
                "median_delta",
                "max_delta",
                "NFL_player_id",
                "player",
                "first_year",
                "last_year",
            ]
        ].to_csv(index=False),
        "",
        f"Derived repaired bio: `{repaired_bio_path}`",
    ]
    (output_dir / "PFR_LASTNAME_CONTEXT_BRIDGE_REPAIR.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retab-dir", type=Path, default=DEFAULT_RETAB_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--bio-path", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir or (args.output_root / f"pfr_lastname_context_bridge_repairs_{now_stamp()}")
    manifest = build_repairs(args.retab_dir, args.audit_dir, args.bio_path, output_dir)
    print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
