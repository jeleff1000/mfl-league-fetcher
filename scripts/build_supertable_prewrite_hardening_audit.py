"""Build local pre-write audit files for the NFL supertable refresh.

This does not mutate Fly or any source parquet.  It only profiles the local
PFR/PBP artifacts into priority queues so we can decide what to resolve before a
table edit.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_PFR_CATALOG = ORGANIZED_ROOT / "_catalog"
DEFAULT_PBP_ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"


STAT_FAMILIES = {
    "core_offense": {
        "completions",
        "attempts",
        "passing_yards",
        "passing_tds",
        "passing_interceptions",
        "sacks_suffered",
        "sack_yards_lost",
        "carries",
        "rushing_yards",
        "rushing_tds",
        "receptions",
        "receiving_yards",
        "receiving_tds",
        "fumbles",
        "fumbles_lost",
    },
    "kicking": {
        "fg_att",
        "fg_made",
        "fg_missed",
        "fg_blocked",
        "fg_long",
        "fg_yards",
        "pat_att",
        "pat_made",
        "pat_missed",
        "pat_blocked",
    },
    "returns": {
        "kickoff_returns",
        "kickoff_return_yards",
        "kickoff_return_tds",
        "punt_returns",
        "punt_return_yards",
        "punt_return_tds",
        "special_teams_tds",
    },
    "idp": {
        "def_interceptions",
        "def_interception_yards",
        "def_int_ret_td",
        "def_tds",
        "def_sacks",
        "def_tackles_with_assist",
        "def_tackles_solo",
        "def_tackle_assists",
        "fum_rec",
        "fum_rec_yds",
        "fum_ret_td",
        "def_fumbles_forced",
        "def_pass_defended",
        "def_tackles_for_loss",
        "def_qb_hits",
        "def_safeties",
    },
    "punting": {"punts", "punt_yards", "punt_long", "punts_blocked"},
    "long_bonus": {"passing_long", "rushing_long", "receiving_long"},
    "volume_only": {"targets"},
}

HIGH_VALUE_FAMILIES = {"core_offense", "kicking", "returns", "idp", "punting"}
WATCHLIST_PLAYERS = {"Jordan Thomas", "Spencer Havner"}


def resolve_latest_pfr_package(catalog_dir: Path) -> Path:
    packages = sorted(
        catalog_dir.glob("pfr_supertable_update_package_*"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for package in packages:
        if (package / "pfr_supertable_review_rows.parquet").exists():
            return package
    raise FileNotFoundError(f"No PFR update package with review rows found under {catalog_dir}")


def split_diff_cols(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    for sep in ("|", ","):
        text = text.replace(sep, ";")
    return [part.strip() for part in text.split(";") if part.strip()]


def stat_families(diff_cols: list[str]) -> list[str]:
    families = []
    col_set = set(diff_cols)
    for family, cols in STAT_FAMILIES.items():
        if col_set & cols:
            families.append(family)
    return sorted(families)


def era_label(year: object) -> str:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return "unknown"
    if y < 1950:
        return "pre1950"
    if y < 1970:
        return "1950-1969"
    if y < 1978:
        return "1970-1977"
    if y < 1999:
        return "1978-1998"
    return "1999-2025"


def pfr_priority(row: pd.Series) -> str:
    year = int(row["year"]) if pd.notna(row.get("year")) else 0
    bucket = str(row.get("weekly_bucket") or "")
    families = set(row.get("stat_families_list") or [])
    player = str(row.get("player") or "")

    if player in WATCHLIST_PLAYERS:
        return "P0_watchlist"
    if year >= 1999 and bucket in {"weekly_identity_manual_review", "weekly_update_context_review"}:
        return "P0_nflverse_era_review"
    if year >= 1978 and (families & HIGH_VALUE_FAMILIES):
        return "P0_scoring_relevant_1978plus"
    if year >= 1978 and bucket == "weekly_identity_manual_review":
        return "P0_identity_1978plus"
    if year >= 1978:
        return "P1_context_1978plus"
    if year >= 1950 and (families & HIGH_VALUE_FAMILIES):
        return "P1_scoring_relevant_1950_1977"
    if bucket in {"weekly_insert_multi_game_review", "weekly_update_multi_game_review"}:
        return "P3_pre1978_multigame"
    return "P2_older_or_low_impact"


def build_pfr_priority_outputs(package_dir: Path, out_dir: Path) -> dict[str, int]:
    review = pd.read_parquet(package_dir / "pfr_supertable_review_rows.parquet")
    review = review.copy()
    review["era"] = review["year"].map(era_label)
    review["diff_col_list"] = review["diff_cols"].map(split_diff_cols)
    review["stat_families_list"] = review["diff_col_list"].map(stat_families)
    review["stat_families"] = review["stat_families_list"].map(lambda values: ";".join(values))
    review["priority"] = review.apply(pfr_priority, axis=1)
    review["diff_col_count_parsed"] = review["diff_col_list"].map(len)

    export = review.drop(columns=["diff_col_list", "stat_families_list"])
    export.to_csv(out_dir / "pfr_review_priority_rows.csv", index=False)

    summary = (
        export.groupby(["priority", "weekly_bucket", "era"], dropna=False)
        .agg(rows=("player_week", "size"), stat_diff_count=("stat_diff_count", "sum"))
        .reset_index()
        .sort_values(["priority", "rows"], ascending=[True, False])
    )
    summary.to_csv(out_dir / "pfr_review_priority_summary.csv", index=False)

    top_players = (
        export.groupby(["priority", "player", "NFL_player_id", "position"], dropna=False)
        .agg(
            rows=("player_week", "size"),
            min_year=("year", "min"),
            max_year=("year", "max"),
            buckets=("weekly_bucket", lambda s: ";".join(sorted(set(map(str, s))))),
            families=(
                "stat_families",
                lambda s: ";".join(sorted({x for item in s for x in str(item).split(";") if x})),
            ),
        )
        .reset_index()
        .sort_values(["priority", "rows"], ascending=[True, False])
    )
    top_players.to_csv(out_dir / "pfr_review_top_players.csv", index=False)

    priority_counts = Counter(export["priority"])
    return {key: int(value) for key, value in sorted(priority_counts.items())}


def role_priority(row: pd.Series) -> str:
    min_year = int(row["min_year"]) if pd.notna(row.get("min_year")) else 0
    rows = int(row["player_week_rows"]) if pd.notna(row.get("player_week_rows")) else 0
    roles = str(row.get("event_roles") or "").lower()
    high_value_role = any(
        token in roles
        for token in [
            "receiver",
            "rusher",
            "passer",
            "kickoff_returner",
            "punt_returner",
            "solo_tackle",
            "assist_tackle",
            "sack",
            "interceptor",
            "fumble_recovery",
            "forced_fumble",
        ]
    )
    if min_year >= 1978 and rows >= 25 and high_value_role:
        return "P0_bridge_candidate_1978plus"
    if min_year >= 1978 and high_value_role:
        return "P1_small_bridge_candidate_1978plus"
    if rows >= 50 and high_value_role:
        return "P1_large_older_bridge_candidate"
    return "P2_low_volume_or_low_impact"


def build_pbp_priority_outputs(pbp_dir: Path, out_dir: Path) -> dict[str, int]:
    path = pbp_dir / "unmapped_pbp_players.csv"
    if not path.exists():
        return {}
    unmapped = pd.read_csv(path)
    unmapped = unmapped.copy()
    unmapped["priority"] = unmapped.apply(role_priority, axis=1)
    unmapped.to_csv(out_dir / "pbp_unmapped_priority_rows.csv", index=False)

    summary = (
        unmapped.groupby(["priority", "pbp_source_systems"], dropna=False)
        .agg(
            players=("pbp_player_id", "size"),
            player_week_rows=("player_week_rows", "sum"),
            event_rows=("event_rows", "sum"),
        )
        .reset_index()
        .sort_values(["priority", "player_week_rows"], ascending=[True, False])
    )
    summary.to_csv(out_dir / "pbp_unmapped_priority_summary.csv", index=False)
    priority_counts = unmapped.groupby("priority", dropna=False)["player_week_rows"].sum().astype(int).to_dict()
    return {str(key): int(value) for key, value in sorted(priority_counts.items())}


def write_live_query_templates(out_dir: Path) -> None:
    (out_dir / "prewrite_live_gate_queries.sql").write_text(
        """-- Read-only Fly/DuckDB gates to run before any supertable write.

-- 1. Offensive points leaking through IDP positions.
SELECT NFL_player_id, player, position, nfl_team, MIN(year) AS min_year, MAX(year) AS max_year,
       COUNT(*) AS rows, MAX(fpts_4pt_half) AS max_half_ppr, SUM(COALESCE(pts_idp_std, 0)) AS idp_points
FROM ___ops.nfl_historical.nfl_player_stats_all
WHERE position IN ('LB', 'DL', 'DB')
  AND COALESCE(pts_idp_std, 0) = 0
  AND COALESCE(fpts_4pt_half, 0) > 0
GROUP BY 1, 2, 3, 4
ORDER BY max_half_ppr DESC, rows DESC;

-- 2. Duplicate player_week keys.
SELECT player_week, COUNT(*) AS rows
FROM ___ops.nfl_historical.nfl_player_stats_all
WHERE player_week IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY rows DESC, player_week
LIMIT 200;

-- 3. Implausible IDP best-game check after rebuild/write candidate.
SELECT NFL_player_id, player, position, year, week, nfl_team, opponent_nfl_team, pts_idp_std,
       def_tackles_solo, def_tackle_assists, def_sacks, def_interceptions, def_fumbles_forced, fum_rec, def_tds
FROM ___ops.nfl_historical.nfl_player_stats_all
WHERE pts_idp_std > 0
ORDER BY pts_idp_std DESC
LIMIT 50;
""",
        encoding="utf-8",
    )


def write_markdown_summary(
    out_dir: Path,
    *,
    package_dir: Path,
    pbp_dir: Path,
    pfr_counts: dict[str, int],
    pbp_counts: dict[str, int],
) -> None:
    lines = [
        "# Supertable Pre-write Hardening Audit",
        "",
        f"Generated: {datetime.now(UTC).isoformat()}",
        f"PFR package: `{package_dir}`",
        f"PBP rollup dir: `{pbp_dir}`",
        "",
        "## Priority Counts",
        "",
        "### PFR held-out rows",
        "",
    ]
    for key, value in pfr_counts.items():
        lines.append(f"- {key}: {value:,} rows")
    lines.extend(["", "### PBP unmapped player-week rows", ""])
    for key, value in pbp_counts.items():
        lines.append(f"- {key}: {value:,} player-week rows")
    lines.extend(
        [
            "",
            "## Output Files",
            "",
            "- `pfr_review_priority_rows.csv`",
            "- `pfr_review_priority_summary.csv`",
            "- `pfr_review_top_players.csv`",
            "- `pbp_unmapped_priority_rows.csv`",
            "- `pbp_unmapped_priority_summary.csv`",
            "- `prewrite_live_gate_queries.sql`",
            "",
            "## Before Fly Write",
            "",
            "1. Resolve or explicitly defer every P0 row.",
            "2. Run a local no-write rebuild with the selected PFR/PBP artifacts.",
            "3. Run the live/read-only gate queries against the rebuilt candidate or live table comparison.",
            "4. Recompute weekly, season, career, ranks, PPG, and LAMAR from the rebuilt weekly table.",
        ]
    )
    (out_dir / "PREWRITE_HARDENING_AUDIT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pfr-package-dir", type=Path, help="Explicit PFR package directory")
    parser.add_argument("--pbp-rollup-dir", type=Path, default=DEFAULT_PBP_ROLLUP_DIR)
    parser.add_argument("--output-dir", type=Path, help="Output directory")
    args = parser.parse_args()

    package_dir = args.pfr_package_dir or resolve_latest_pfr_package(DEFAULT_PFR_CATALOG)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = args.output_dir or (DEFAULT_PFR_CATALOG / f"supertable_prewrite_hardening_{stamp}")
    out_dir.mkdir(parents=True, exist_ok=True)

    pfr_counts = build_pfr_priority_outputs(package_dir, out_dir)
    pbp_counts = build_pbp_priority_outputs(args.pbp_rollup_dir, out_dir)
    write_live_query_templates(out_dir)
    write_markdown_summary(
        out_dir,
        package_dir=package_dir,
        pbp_dir=args.pbp_rollup_dir,
        pfr_counts=pfr_counts,
        pbp_counts=pbp_counts,
    )
    (out_dir / "manifest.json").write_text(
        json.dumps(
            {
                "created_at_utc": datetime.now(UTC).isoformat(),
                "pfr_package_dir": str(package_dir),
                "pbp_rollup_dir": str(args.pbp_rollup_dir),
                "pfr_priority_counts": pfr_counts,
                "pbp_priority_player_week_counts": pbp_counts,
                "output_dir": str(out_dir),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(out_dir)
    print("PFR priority counts:", pfr_counts)
    print("PBP priority player-week counts:", pbp_counts)


if __name__ == "__main__":
    main()
