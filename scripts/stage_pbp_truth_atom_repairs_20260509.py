#!/usr/bin/env python3
"""Stage guarded PBP truth atom repairs and safe missing fantasy rows.

This stage is compatible with apply_combined_identity_promotion_20260509.py.
It includes:
  - existing-row overlays for pre-1999 FG distance, returns, and long-play atoms
  - missing context-complete pre-1999 kicker/returner/offense rows
  - optional missing context-complete punter rows
  - optional missing context-complete IDP/ST defensive rows

Targets, IDP/defense-only rows, fumble-only rows, and context-missing rows are
intentionally excluded from the default pass. Punters and IDP rows are opt-in so
their promotions can be audited separately.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
NFL_DATA_ROOT = SCRIPTS_ROOT / "nfl_data"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_ROOT))
sys.path.insert(0, str(NFL_DATA_ROOT))

ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_DIR = (
    ORGANIZED_ROOT
    / "_catalog"
    / "pbp_supertable_gap_audit_20260507"
    / "after_postseason_official_repair_20260509T2105Z"
)
ROLLUP_PATH = (
    ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025" / "pbp_player_week_rollup.parquet"
)
OUT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507" / "pbp_truth_atom_repair_stage_20260509"
SUPER_TABLE = "nfl_historical.nfl_player_stats_all"


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: list[str]) -> str:
    return "(" + ",".join(q_lit(value) for value in values) + ")" if values else "('')"


def chunks(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_schema(writer: Any) -> pd.DataFrame:
    rows = writer.execute(
        """
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        ORDER BY ordinal_position
        """,
        database="___ops",
    )
    return pd.DataFrame(rows)


def fetch_live_rows(writer: Any, keys: list[str], columns: list[str]) -> pd.DataFrame:
    select_cols = ", ".join(q_ident(col) for col in columns)
    rows: list[dict[str, Any]] = []
    for chunk_no, chunk in enumerate(chunks(keys, 1000), start=1):
        part = writer.execute(
            f"""
            SELECT {select_cols}
            FROM {SUPER_TABLE}
            WHERE player_week IN {sql_in(chunk)}
            """,
            database="___ops",
        )
        rows.extend(part)
        print(f"[fetch] existing overlay chunk {chunk_no}: {len(part)} rows")
    return pd.DataFrame(rows)


def clean_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = out[col].map(
                lambda value: None if value is None or (isinstance(value, float) and math.isnan(value)) else value
            )
    return out


def changed_keys_and_summary(
    before: pd.DataFrame, after: pd.DataFrame, cols: list[str]
) -> tuple[list[str], pd.DataFrame]:
    changed = pd.Series(False, index=after.index)
    rows = []
    for col in cols:
        if col not in before.columns or col not in after.columns:
            continue
        old = pd.to_numeric(before[col], errors="coerce").fillna(0.0)
        new = pd.to_numeric(after[col], errors="coerce").fillna(0.0)
        delta = new - old
        changed |= delta.ne(0)
        if delta.abs().sum() > 0:
            rows.append(
                {
                    "repair_type": "existing_overlay",
                    "stat": col,
                    "rows_changed": int(delta.ne(0).sum()),
                    "old_total": float(old[delta.ne(0)].sum()),
                    "new_total": float(new[delta.ne(0)].sum()),
                    "delta_total": float(delta.sum()),
                    "abs_delta_total": float(delta.abs().sum()),
                }
            )
    keys = after.loc[changed, "player_week"].dropna().astype(str).drop_duplicates().tolist()
    summary = pd.DataFrame(rows)
    if not summary.empty:
        summary = summary.sort_values(["repair_type", "abs_delta_total"], ascending=[True, False])
    return keys, summary


def find_existing_overlay_candidates(
    audit_dir: Path,
    rollup_path: Path,
    *,
    min_year: int = 1978,
    max_year: int = 1998,
) -> tuple[list[str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    import pyarrow.parquet as pq

    from multi_league.data_fetchers.pbp_schema_backfill import (
        PBP_TRUTH_ATOM_SOURCE_MAP,
        apply_pbp_truth_atom_overlays,
    )

    weekly_path = audit_dir / "fly_supertable_derived_weekly.parquet"
    available = set(pq.read_schema(weekly_path).names)
    needed = [
        "player_week",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        *PBP_TRUTH_ATOM_SOURCE_MAP.keys(),
    ]
    weekly = pd.read_parquet(weekly_path, columns=[col for col in needed if col in available])
    before = weekly.copy()
    repaired = apply_pbp_truth_atom_overlays(
        weekly,
        rollup_path=rollup_path,
        min_year=min_year,
        max_year=max_year,
    )
    cols = [col for col in PBP_TRUTH_ATOM_SOURCE_MAP if col in before.columns and col in repaired.columns]
    keys, stat_summary = changed_keys_and_summary(before, repaired, cols)
    annotated_cols = [
        col
        for col in ["player_week", "player", "position", "nfl_team", "opponent_nfl_team", "year", "week", "season_type"]
        if col in repaired.columns
    ]
    annotated = repaired.loc[repaired["player_week"].astype(str).isin(keys), annotated_cols].copy()
    annotated["repair_type"] = "existing_overlay"
    weekly_keys = pd.read_parquet(weekly_path, columns=["player_week"])
    return keys, stat_summary, annotated, weekly_keys


def recompute_rows(rows: pd.DataFrame) -> pd.DataFrame:
    from build_nfl_super_table import (
        populate_fg_made_60_plus_canonical,
        populate_fg_yards_canonical,
        populate_fg_yards_over_30_canonical,
    )
    from multi_league.data_fetchers.fantasy_points_calculator import (
        calculate_all_fantasy_points,
        calculate_composite_fantasy_points,
    )

    out = populate_fg_made_60_plus_canonical(rows.copy())
    out = populate_fg_yards_canonical(out)
    out = populate_fg_yards_over_30_canonical(out)
    out = calculate_all_fantasy_points(out)
    out = calculate_composite_fantasy_points(out)
    return out


def build_missing_rows(
    weekly_existing: pd.DataFrame,
    columns: list[str],
    type_by_col: dict[str, str],
    rollup_path: Path,
    *,
    min_year: int = 1978,
    max_year: int = 1998,
    include_punters: bool = False,
    punters_only: bool = False,
    include_idp: bool = False,
    idp_only: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from multi_league.data_fetchers.pbp_schema_backfill import (
        build_pbp_safe_missing_rows,
        pbp_safe_missing_source_map,
    )

    source_map = pbp_safe_missing_source_map(include_punters=include_punters, include_idp=include_idp)
    raw_missing = build_pbp_safe_missing_rows(
        weekly_existing,
        rollup_path=rollup_path,
        columns=[*columns, "safe_missing_bucket"],
        schema_types=type_by_col,
        min_year=min_year,
        max_year=max_year,
        include_punters=include_punters,
        punters_only=punters_only,
        include_idp=include_idp,
        idp_only=idp_only,
    )
    if raw_missing.empty:
        return pd.DataFrame(columns=columns), pd.DataFrame(), pd.DataFrame()

    annotated_cols = [
        col
        for col in [
            "player_week",
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
            "safe_missing_bucket",
        ]
        if col in raw_missing.columns
    ]
    annotated = raw_missing[annotated_cols].copy()
    annotated["repair_type"] = "safe_missing_insert"

    stat_rows = []
    for col in source_map:
        if col in raw_missing.columns:
            values = pd.to_numeric(raw_missing[col], errors="coerce").fillna(0.0)
            if values.abs().sum() > 0:
                stat_rows.append(
                    {
                        "repair_type": "safe_missing_insert",
                        "stat": col,
                        "rows_changed": int(values.ne(0).sum()),
                        "old_total": 0.0,
                        "new_total": float(values.sum()),
                        "delta_total": float(values.sum()),
                        "abs_delta_total": float(values.abs().sum()),
                    }
                )
    stat_summary = pd.DataFrame(stat_rows)
    if not stat_summary.empty:
        stat_summary = stat_summary.sort_values("abs_delta_total", ascending=False)
    return raw_missing.reindex(columns=columns), stat_summary, annotated


def build_metric_scope(promotion: pd.DataFrame) -> pd.DataFrame:
    fpts_cols = [
        "fpts_4pt_0ppr",
        "fpts_4pt_half",
        "fpts_4pt_ppr",
        "fpts_5pt_0ppr",
        "fpts_5pt_half",
        "fpts_5pt_ppr",
        "fpts_6pt_0ppr",
        "fpts_6pt_half",
        "fpts_6pt_ppr",
        "fpts_4pt_tep",
        "fpts_5pt_tep",
        "fpts_6pt_tep",
    ]
    ppg_cols = []
    for td in ("4pt", "5pt", "6pt"):
        for ppr in ("0ppr", "half", "ppr", "tep"):
            ppg_cols.extend(
                [
                    f"ppg_season_{td}_{ppr}",
                    f"ppg_alltime_{td}_{ppr}",
                    f"rolling_3_{td}_{ppr}",
                    f"rolling_5_{td}_{ppr}",
                    f"rolling_total_{td}_{ppr}",
                    f"weighted_ppg_{td}_{ppr}",
                    f"consistency_{td}_{ppr}",
                    f"avg_pts_next_year_{td}_{ppr}",
                ]
            )
    ppg_cols.extend(["rolling_total_def", "rolling_total_k"])

    rows = []
    if "NFL_player_id" not in promotion.columns:
        return pd.DataFrame(rows)
    for player_id, group in promotion.groupby("NFL_player_id", dropna=True):
        rows.append(
            {
                "NFL_player_id": player_id,
                "players": ",".join(sorted(group["player"].dropna().astype(str).unique())) if "player" in group else "",
                "years_touched": ",".join(
                    str(int(v)) for v in sorted(pd.to_numeric(group["year"], errors="coerce").dropna().unique())
                ),
                "weeks_touched": ",".join(
                    str(int(v)) for v in sorted(pd.to_numeric(group["week"], errors="coerce").dropna().unique())
                ),
                "fpts_columns": ",".join(fpts_cols),
                "ppg_columns": ",".join(ppg_cols),
                "reason": "guarded PBP truth atom repair or safe pre-1999 missing row insert",
            }
        )
    return pd.DataFrame(rows)


def build_rank_scope(promotion: pd.DataFrame, schema_cols: set[str]) -> pd.DataFrame:
    from scripts.apply_combined_identity_promotion_20260509 import (
        aggregate_rank_specs,
        weekly_rank_specs,
    )

    weekly_cols = [spec.col for spec in weekly_rank_specs() if spec.col in schema_cols]
    season_cols = [spec.col for spec in aggregate_rank_specs("rank_season") if spec.col in schema_cols]
    alltime_cols = [spec.col for spec in aggregate_rank_specs("rank_alltime") if spec.col in schema_cols]

    rows = []
    touched_weeks = promotion[["year", "week"]].dropna().drop_duplicates().sort_values(["year", "week"])
    for row in touched_weeks.itertuples(index=False):
        rows.append(
            {
                "scope": "weekly",
                "year": int(row.year),
                "week": int(row.week),
                "positions": "ALL",
                "rank_columns": ",".join(weekly_cols),
                "reason": "affected raw/fantasy rows can change weekly ranks",
            }
        )

    for year in sorted(pd.to_numeric(promotion["year"], errors="coerce").dropna().astype(int).unique()):
        rows.append(
            {
                "scope": "season",
                "year": int(year),
                "week": "",
                "positions": "ALL",
                "rank_columns": ",".join(season_cols),
                "reason": "affected rows can change season ranks",
            }
        )

    rows.append(
        {
            "scope": "alltime",
            "year": "",
            "week": "",
            "positions": "ALL",
            "rank_columns": ",".join(alltime_cols),
            "reason": "affected rows can change all-time ranks",
        }
    )
    return pd.DataFrame(rows)


def write_summary(
    out_dir: Path,
    manifest: dict[str, Any],
    stat_summary: pd.DataFrame,
    annotated: pd.DataFrame,
) -> None:
    lines = [
        "# PBP Truth Atom Repair Stage",
        "",
        "Guarded PBP atom repairs and safe missing row inserts.",
        "",
        "## Manifest",
        "",
        json.dumps(manifest, indent=2, sort_keys=True),
        "",
        "## Stat Delta Summary",
        "",
        stat_summary.to_csv(index=False) if not stat_summary.empty else "(none)",
        "",
        "## Sample Rows",
        "",
        annotated.head(80).to_csv(index=False) if not annotated.empty else "(none)",
        "",
    ]
    (out_dir / "PBP_TRUTH_ATOM_REPAIR_STAGE.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--audit-dir", type=Path, default=AUDIT_DIR)
    parser.add_argument("--rollup-path", type=Path, default=ROLLUP_PATH)
    parser.add_argument("--min-year", type=int, default=1978)
    parser.add_argument("--max-year", type=int, default=1998)
    parser.add_argument("--skip-overlays", action="store_true")
    parser.add_argument("--include-punters", action="store_true")
    parser.add_argument("--punters-only", action="store_true")
    parser.add_argument("--include-idp", action="store_true")
    parser.add_argument("--idp-only", action="store_true")
    args = parser.parse_args()

    load_env()
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.data_fetchers.pbp_schema_backfill import apply_pbp_truth_atom_overlays

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_overlays:
        weekly_path = args.audit_dir / "fly_supertable_derived_weekly.parquet"
        weekly_existing = pd.read_parquet(weekly_path, columns=["player_week"])
        keys = []
        overlay_summary = pd.DataFrame()
        overlay_annotated = pd.DataFrame()
    else:
        keys, overlay_summary, overlay_annotated, weekly_existing = find_existing_overlay_candidates(
            args.audit_dir,
            args.rollup_path,
            min_year=args.min_year,
            max_year=args.max_year,
        )
    print(f"[stage] existing overlay candidate player_weeks: {len(keys)}")

    writer = FlyWriter()
    schema = fetch_schema(writer)
    schema.to_csv(out_dir / "live_schema_columns.csv", index=False)
    columns = schema["column_name"].astype(str).tolist()
    type_by_col = dict(zip(schema["column_name"].astype(str), schema["data_type"].astype(str)))

    if keys:
        live = fetch_live_rows(writer, keys, columns)
        if len(live) != len(keys):
            missing = sorted(set(keys) - set(live["player_week"].dropna().astype(str)))
            raise RuntimeError(f"Fetched {len(live)} live rows for {len(keys)} keys; missing={missing[:20]}")
        repaired_live = apply_pbp_truth_atom_overlays(
            live,
            rollup_path=args.rollup_path,
            min_year=args.min_year,
            max_year=args.max_year,
        )
        repaired_live = recompute_rows(repaired_live).reindex(columns=columns)
    else:
        repaired_live = pd.DataFrame(columns=columns)

    missing_rows, missing_summary, missing_annotated = build_missing_rows(
        weekly_existing,
        columns,
        type_by_col,
        args.rollup_path,
        min_year=args.min_year,
        max_year=args.max_year,
        include_punters=args.include_punters or args.punters_only,
        punters_only=args.punters_only,
        include_idp=args.include_idp or args.idp_only,
        idp_only=args.idp_only,
    )
    print(f"[stage] safe missing insert rows: {len(missing_rows)}")
    if not missing_rows.empty:
        missing_rows = recompute_rows(missing_rows).reindex(columns=columns)

    promotion = pd.concat([repaired_live, missing_rows], ignore_index=True, sort=False)
    if promotion.empty:
        raise SystemExit("No PBP truth atom repair rows found")
    duplicated = promotion["player_week"].dropna().astype(str).duplicated()
    if duplicated.any():
        dupes = promotion.loc[duplicated, "player_week"].astype(str).head(20).tolist()
        raise RuntimeError(f"Promotion rows contain duplicate player_week keys: {dupes}")
    promotion = clean_for_parquet(promotion.reindex(columns=columns))

    annotated = pd.concat([overlay_annotated, missing_annotated], ignore_index=True, sort=False)
    stat_summary = pd.concat([overlay_summary, missing_summary], ignore_index=True, sort=False)
    if not stat_summary.empty:
        stat_summary = stat_summary.sort_values(["repair_type", "abs_delta_total"], ascending=[True, False])

    promotion.to_parquet(out_dir / "combined_promotion_rows_full.parquet", index=False)
    annotated.to_parquet(out_dir / "combined_promotion_rows_annotated.parquet", index=False)
    annotated.to_csv(out_dir / "pbp_truth_atom_repair_rows.csv", index=False)
    stat_summary.to_csv(out_dir / "pbp_truth_atom_repair_stat_summary.csv", index=False)
    pd.DataFrame({"player_week": []}).to_csv(out_dir / "delete_player_weeks.csv", index=False)

    metric_scope = build_metric_scope(promotion)
    metric_scope.to_csv(out_dir / "metric_recompute_scope.csv", index=False)
    rank_scope = build_rank_scope(promotion, set(columns))
    rank_scope.to_csv(out_dir / "rank_recompute_scope.csv", index=False)

    manifest = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "promotion_rows": int(len(promotion)),
        "existing_overlay_rows": int(len(repaired_live)),
        "safe_missing_insert_rows": int(len(missing_rows)),
        "delete_player_weeks": 0,
        "affected_player_ids": int(promotion["NFL_player_id"].dropna().astype(str).nunique()),
        "rank_scope_rows": int(len(rank_scope)),
        "stat_summary_rows": int(len(stat_summary)),
        "source_audit_dir": str(args.audit_dir),
        "source_rollup_path": str(args.rollup_path),
        "min_year": int(args.min_year),
        "max_year": int(args.max_year),
        "skip_overlays": bool(args.skip_overlays),
        "include_punters": bool(args.include_punters or args.punters_only),
        "punters_only": bool(args.punters_only),
        "include_idp": bool(args.include_idp or args.idp_only),
        "idp_only": bool(args.idp_only),
        "promotion_rows_path": str(out_dir / "combined_promotion_rows_full.parquet"),
        "exclusions": [
            "targets",
            *([] if (args.include_punters or args.punters_only) else ["punters"]),
            *([] if (args.include_idp or args.idp_only) else ["IDP/defense-only rows"]),
            *([] if (args.include_idp or args.idp_only) else ["blocked-kick rows"]),
            "fumble-only rows",
            "context-missing rows",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    write_summary(out_dir, manifest, stat_summary, annotated)

    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
