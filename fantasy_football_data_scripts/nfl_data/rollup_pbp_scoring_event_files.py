"""Roll up local PBP scoring event files to weekly, season, and career grain.

Input files are produced by ``build_pbp_scoring_event_files.py``.  This script is
purely local and deterministic: it reads the auditable event-grain parquets and
writes compact derived rollups that can later be joined into the super table.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVENT_DIR = ROOT / "fantasy_football_data" / "cache" / "nflverse" / "scoring_events"
DEFAULT_OUTPUT_DIR = DEFAULT_EVENT_DIR / "rollups"

EVENT_FILES = {
    "pass_completions_50plus": "pbp_pass_completions_50plus.parquet",
    "reception_yardage_buckets": "pbp_reception_yardage_buckets.parquet",
    "special_teams_solo_tackles": "pbp_special_teams_solo_tackles.parquet",
    "fumble_recovery_yards": "pbp_fumble_recovery_yards.parquet",
}

ROLLUP_COLUMNS = [
    "completions_50plus",
    "receptions_0_4",
    "receptions_5_9",
    "receptions_10_19",
    "receptions_20_29",
    "receptions_30_39",
    "special_teams_tackles_solo",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
]


@dataclass(frozen=True)
class RollupResult:
    name: str
    path: str
    rows: int


def _read_event(event_dir: Path, name: str) -> pd.DataFrame:
    path = event_dir / EVENT_FILES[name]
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _base_week_index(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "NFL_player_id",
        "player_week",
        "season",
        "season_type",
        "week",
        "player",
        "nfl_team",
        "opponent_nfl_team",
    ]
    existing = [col for col in cols if col in df.columns]
    if not existing:
        return pd.DataFrame(columns=cols)
    base = df[existing].copy()
    for col in cols:
        if col not in base.columns:
            base[col] = pd.NA
    return base.drop_duplicates(["NFL_player_id", "player_week", "season", "season_type", "week"])


def _count_rollup(df: pd.DataFrame, value_col: str, *, filter_default_candidate: bool = False) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    if filter_default_candidate and "is_default_rollup_candidate" in work.columns:
        work = work[work["is_default_rollup_candidate"].fillna(False).astype(bool)].copy()
    if work.empty:
        return pd.DataFrame()
    group_cols = ["NFL_player_id", "player_week", "season", "season_type", "week"]
    return (
        work.groupby(group_cols, dropna=False)["event_count"]
        .sum()
        .reset_index()
        .rename(columns={"event_count": value_col})
    )


def _pivot_rollup(df: pd.DataFrame, *, values: str, aggfunc: str = "sum") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    group_cols = ["NFL_player_id", "player_week", "season", "season_type", "week"]
    pivot = df.pivot_table(
        index=group_cols,
        columns="super_table_column",
        values=values,
        aggfunc=aggfunc,
        fill_value=0,
    )
    pivot.columns = [str(col) for col in pivot.columns]
    return pivot.reset_index()


def build_weekly_rollup(event_dir: Path) -> pd.DataFrame:
    events = {name: _read_event(event_dir, name) for name in EVENT_FILES}
    base_parts = [_base_week_index(df) for df in events.values() if not df.empty]
    if not base_parts:
        return pd.DataFrame()
    weekly = pd.concat(base_parts, ignore_index=True).drop_duplicates(
        ["NFL_player_id", "player_week", "season", "season_type", "week"]
    )

    parts = [
        _count_rollup(events["pass_completions_50plus"], "completions_50plus"),
        _pivot_rollup(events["reception_yardage_buckets"], values="event_count"),
        _count_rollup(
            events["special_teams_solo_tackles"],
            "special_teams_tackles_solo",
            filter_default_candidate=True,
        ),
        _pivot_rollup(events["fumble_recovery_yards"], values="event_yards"),
    ]
    for part in parts:
        if part.empty:
            continue
        weekly = weekly.merge(part, on=["NFL_player_id", "player_week", "season", "season_type", "week"], how="left")

    for col in ROLLUP_COLUMNS:
        if col not in weekly.columns:
            weekly[col] = 0
        weekly[col] = pd.to_numeric(weekly[col], errors="coerce").fillna(0)

    weekly["fumble_recovery_yards"] = weekly["fumble_recovery_yards_own"] + weekly["fumble_recovery_yards_opp"]
    weekly["year"] = weekly["season"]
    sort_cols = ["year", "week", "NFL_player_id", "player_week"]
    return weekly.sort_values(sort_cols).reset_index(drop=True)


def _rollup_to_period(weekly: pd.DataFrame, *, include_postseason: bool, career: bool) -> pd.DataFrame:
    if weekly.empty:
        return weekly
    work = weekly.copy()
    if not include_postseason:
        work = work[work["season_type"].fillna("REG").eq("REG")].copy()
    group_cols = ["NFL_player_id"] if career else ["NFL_player_id", "year"]
    numeric_cols = [*ROLLUP_COLUMNS, "fumble_recovery_yards"]
    meta = (
        work.sort_values(["year", "week"])
        .groupby(group_cols, dropna=False)
        .agg(
            player=("player", "last"),
            first_year=("year", "min"),
            last_year=("year", "max"),
        )
        .reset_index()
    )
    sums = work.groupby(group_cols, dropna=False)[numeric_cols].sum().reset_index()
    return meta.merge(sums, on=group_cols, how="left").sort_values(group_cols).reset_index(drop=True)


def _write(df: pd.DataFrame, output_dir: Path, name: str, write_csv: bool) -> RollupResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.parquet"
    df.to_parquet(path, index=False)
    if write_csv:
        df.to_csv(path.with_suffix(".csv"), index=False)
    return RollupResult(name=name, path=str(path), rows=len(df))


def build_rollups(event_dir: Path, output_dir: Path, write_csv: bool) -> list[RollupResult]:
    weekly = build_weekly_rollup(event_dir)
    outputs = [
        _write(weekly, output_dir, "pbp_scoring_player_week_rollup", write_csv),
        _write(
            _rollup_to_period(weekly, include_postseason=False, career=False),
            output_dir,
            "pbp_scoring_player_season_rollup",
            write_csv,
        ),
        _write(
            _rollup_to_period(weekly, include_postseason=True, career=False),
            output_dir,
            "pbp_scoring_player_season_all_rollup",
            write_csv,
        ),
        _write(
            _rollup_to_period(weekly, include_postseason=False, career=True),
            output_dir,
            "pbp_scoring_player_career_rollup",
            write_csv,
        ),
        _write(
            _rollup_to_period(weekly, include_postseason=True, career=True),
            output_dir,
            "pbp_scoring_player_career_all_rollup",
            write_csv,
        ),
    ]
    manifest = {"event_dir": str(event_dir), "output_dir": str(output_dir), "outputs": [o.__dict__ for o in outputs]}
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description="Roll up local PBP scoring event files.")
    parser.add_argument("--event-dir", type=Path, default=DEFAULT_EVENT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--csv", action="store_true", help="Also write CSV copies next to the parquet files.")
    args = parser.parse_args()

    outputs = build_rollups(args.event_dir, args.output_dir, args.csv)
    print("[outputs]", flush=True)
    for output in outputs:
        print(f"  {output.name}: {output.rows:,} rows -> {output.path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
