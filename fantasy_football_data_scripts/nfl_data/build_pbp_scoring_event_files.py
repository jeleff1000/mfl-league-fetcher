"""Build event-level local files for scoring rules sourced from play-by-play.

These files are intentionally event-grain, not already folded into the super
table.  They preserve enough play detail to audit every future rollup:

* pass completions of 50+ yards
* reception yardage bucket counts
* special-teams solo tackles
* fumble recovery return yards, split own-team vs opponent recovery

Outputs are written under ``fantasy_football_data/cache/nflverse/scoring_events``
by default.  That path is gitignored and is meant to be regenerated locally.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE_DIR = ROOT / "fantasy_football_data" / "cache" / "nflverse"
DEFAULT_OUTPUT_DIR = DEFAULT_CACHE_DIR / "scoring_events"
PBP_URL = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet"

BASE_COLUMNS = [
    "season",
    "season_type",
    "week",
    "game_id",
    "old_game_id",
    "play_id",
    "qtr",
    "game_seconds_remaining",
    "posteam",
    "defteam",
    "return_team",
    "play_type",
    "desc",
]

PASS_COLUMNS = [
    "complete_pass",
    "pass_attempt",
    "passer_player_id",
    "passer_player_name",
    "receiver_player_id",
    "receiver_player_name",
    "passing_yards",
    "receiving_yards",
    "yards_gained",
]

RECEPTION_COLUMNS = [
    "complete_pass",
    "receiver_player_id",
    "receiver_player_name",
    "passer_player_id",
    "passer_player_name",
    "receiving_yards",
    "yards_gained",
    "lateral_receiver_player_id",
    "lateral_receiver_player_name",
    "lateral_receiving_yards",
]

SPECIAL_TEAMS_COLUMNS = [
    "special_teams_play",
    "kickoff_attempt",
    "punt_attempt",
    "punt_blocked",
    "solo_tackle",
    "solo_tackle_1_team",
    "solo_tackle_1_player_id",
    "solo_tackle_1_player_name",
    "solo_tackle_2_team",
    "solo_tackle_2_player_id",
    "solo_tackle_2_player_name",
    "kickoff_returner_player_id",
    "kickoff_returner_player_name",
    "punt_returner_player_id",
    "punt_returner_player_name",
    "return_yards",
]

FUMBLE_COLUMNS = [
    "fumble",
    "fumble_lost",
    "fumbled_1_team",
    "fumbled_1_player_id",
    "fumbled_1_player_name",
    "fumbled_2_team",
    "fumbled_2_player_id",
    "fumbled_2_player_name",
    "fumble_recovery_1_team",
    "fumble_recovery_1_yards",
    "fumble_recovery_1_player_id",
    "fumble_recovery_1_player_name",
    "fumble_recovery_2_team",
    "fumble_recovery_2_yards",
    "fumble_recovery_2_player_id",
    "fumble_recovery_2_player_name",
]

OUTPUTS = {
    "pass_completions_50plus": "pbp_pass_completions_50plus.parquet",
    "reception_yardage_buckets": "pbp_reception_yardage_buckets.parquet",
    "special_teams_solo_tackles": "pbp_special_teams_solo_tackles.parquet",
    "fumble_recovery_yards": "pbp_fumble_recovery_yards.parquet",
}


@dataclass(frozen=True)
class BuildResult:
    name: str
    path: str
    rows: int


def _existing_columns(path: Path) -> list[str]:
    """Read a parquet schema without loading the full file."""
    try:
        import pyarrow.parquet as pq

        return list(pq.ParquetFile(path).schema_arrow.names)
    except Exception:
        # pandas fallback: slower, but keeps the script usable with minimal deps.
        return list(pd.read_parquet(path).columns)


def _download_pbp(year: int, cache_file: Path) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    url = PBP_URL.format(year=year)
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            tmp_path = Path(tmp.name)
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
    tmp_path.replace(cache_file)


def _read_pbp_year(year: int, cache_dir: Path, download_missing: bool) -> pd.DataFrame | None:
    cache_file = cache_dir / f"nflverse_pbp_{year}.parquet"
    if not cache_file.exists():
        if not download_missing:
            return None
        print(f"[download] {year} play-by-play", flush=True)
        _download_pbp(year, cache_file)

    wanted = set(BASE_COLUMNS + PASS_COLUMNS + RECEPTION_COLUMNS + SPECIAL_TEAMS_COLUMNS + FUMBLE_COLUMNS)
    existing = set(_existing_columns(cache_file))
    columns = sorted(wanted & existing)
    return pd.read_parquet(cache_file, columns=columns)


def _num(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def _text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str)


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def _standardize_base(df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_columns(df.copy(), BASE_COLUMNS)
    if "season" in df.columns:
        df["season"] = pd.to_numeric(df["season"], errors="coerce").astype("Int64")
    if "week" in df.columns:
        df["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
    return df


def _player_week(player_id: pd.Series, season: pd.Series, week: pd.Series) -> pd.Series:
    return _text(player_id) + "_" + _text(season.astype("Int64")) + "_" + _text(week.astype("Int64"))


def pass_completion_50plus_events(pbp: pd.DataFrame) -> pd.DataFrame:
    pbp = _ensure_columns(_standardize_base(pbp), PASS_COLUMNS)
    passing_yards = _num(pbp["passing_yards"], 0.0)
    mask = (
        (_num(pbp["complete_pass"], 0) == 1)
        & passing_yards.ge(50)
        & pbp["passer_player_id"].notna()
        & _text(pbp["passer_player_id"]).ne("")
    )
    out = pbp.loc[mask, BASE_COLUMNS + PASS_COLUMNS].copy()
    out["category"] = "pass_cmp_50p"
    out["super_table_column"] = "completions_50plus"
    out["player_role"] = "passer"
    out["NFL_player_id"] = out["passer_player_id"]
    out["player"] = out["passer_player_name"]
    out["nfl_team"] = out["posteam"]
    out["opponent_nfl_team"] = out["defteam"]
    out["event_count"] = 1
    out["event_yards"] = passing_yards.loc[mask].astype(float)
    out["player_week"] = _player_week(out["NFL_player_id"], out["season"], out["week"])
    return out


def reception_bucket_events(pbp: pd.DataFrame) -> pd.DataFrame:
    pbp = _ensure_columns(_standardize_base(pbp), RECEPTION_COLUMNS)
    receiving_yards = _num(pbp["receiving_yards"], 0.0)
    mask = (
        (_num(pbp["complete_pass"], 0) == 1)
        & pbp["receiver_player_id"].notna()
        & _text(pbp["receiver_player_id"]).ne("")
        & receiving_yards.ge(0)
        & receiving_yards.le(39)
    )
    out = pbp.loc[mask, BASE_COLUMNS + RECEPTION_COLUMNS].copy()
    yards = receiving_yards.loc[mask].astype(float)
    bucket_defs = [
        (0, 4, "receptions_0_4"),
        (5, 9, "receptions_5_9"),
        (10, 19, "receptions_10_19"),
        (20, 29, "receptions_20_29"),
        (30, 39, "receptions_30_39"),
    ]
    out["super_table_column"] = pd.NA
    out["bucket_min"] = pd.NA
    out["bucket_max"] = pd.NA
    for low, high, col in bucket_defs:
        bucket_mask = yards.ge(low) & yards.le(high)
        out.loc[bucket_mask, "super_table_column"] = col
        out.loc[bucket_mask, "bucket_min"] = low
        out.loc[bucket_mask, "bucket_max"] = high
    out["category"] = "reception_yardage_bucket"
    out["player_role"] = "receiver"
    out["NFL_player_id"] = out["receiver_player_id"]
    out["player"] = out["receiver_player_name"]
    out["nfl_team"] = out["posteam"]
    out["opponent_nfl_team"] = out["defteam"]
    out["event_count"] = 1
    out["event_yards"] = yards
    out["player_week"] = _player_week(out["NFL_player_id"], out["season"], out["week"])
    return out


def special_teams_solo_tackle_events(pbp: pd.DataFrame) -> pd.DataFrame:
    pbp = _ensure_columns(_standardize_base(pbp), SPECIAL_TEAMS_COLUMNS)
    st_mask = (_num(pbp["special_teams_play"], 0) == 1) & (_num(pbp["solo_tackle"], 0) == 1)
    rows: list[pd.DataFrame] = []
    for slot in (1, 2):
        id_col = f"solo_tackle_{slot}_player_id"
        name_col = f"solo_tackle_{slot}_player_name"
        team_col = f"solo_tackle_{slot}_team"
        slot_mask = st_mask & pbp[id_col].notna() & _text(pbp[id_col]).ne("")
        if not slot_mask.any():
            continue
        part = pbp.loc[slot_mask, BASE_COLUMNS + SPECIAL_TEAMS_COLUMNS].copy()
        part["category"] = "st_tkl_solo"
        part["super_table_column"] = "special_teams_tackles_solo"
        part["player_role"] = "special_teams_tackler"
        part["tackle_slot"] = slot
        part["NFL_player_id"] = part[id_col]
        part["player"] = part[name_col]
        part["nfl_team"] = part[team_col]
        part["opponent_nfl_team"] = part["return_team"]
        part["is_default_rollup_candidate"] = (
            part["return_team"].isna()
            | _text(part["return_team"]).eq("")
            | _text(part[team_col]).ne(_text(part["return_team"]))
        )
        part["event_count"] = 1
        part["event_yards"] = _num(part["return_yards"], 0.0)
        part["player_week"] = _player_week(part["NFL_player_id"], part["season"], part["week"])
        rows.append(part)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def fumble_recovery_yard_events(pbp: pd.DataFrame) -> pd.DataFrame:
    pbp = _ensure_columns(_standardize_base(pbp), FUMBLE_COLUMNS)
    rows: list[pd.DataFrame] = []
    for slot in (1, 2):
        id_col = f"fumble_recovery_{slot}_player_id"
        name_col = f"fumble_recovery_{slot}_player_name"
        team_col = f"fumble_recovery_{slot}_team"
        yards_col = f"fumble_recovery_{slot}_yards"
        slot_mask = pbp[id_col].notna() & _text(pbp[id_col]).ne("")
        if not slot_mask.any():
            continue
        part = pbp.loc[slot_mask, BASE_COLUMNS + FUMBLE_COLUMNS].copy()
        fumbled_team = part["fumbled_1_team"].combine_first(part["fumbled_2_team"])
        is_own = _text(part[team_col]).eq(_text(fumbled_team))
        part["category"] = "fum_ret_yd"
        part["recovery_slot"] = slot
        part["super_table_column"] = "fumble_recovery_yards_own"
        part.loc[~is_own, "super_table_column"] = "fumble_recovery_yards_opp"
        part["player_role"] = "fumble_recoverer"
        part["NFL_player_id"] = part[id_col]
        part["player"] = part[name_col]
        part["nfl_team"] = part[team_col]
        part["opponent_nfl_team"] = part["defteam"]
        part["fumbled_team"] = fumbled_team
        part["is_own_team_recovery"] = is_own
        part["event_count"] = 1
        part["event_yards"] = _num(part[yards_col], 0.0)
        part["player_week"] = _player_week(part["NFL_player_id"], part["season"], part["week"])
        rows.append(part)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _write_output(name: str, df: pd.DataFrame, output_dir: Path, write_csv: bool) -> BuildResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / OUTPUTS[name]
    df = df.sort_values(
        [c for c in ("season", "week", "game_id", "play_id", "NFL_player_id", "super_table_column") if c in df.columns]
    ).reset_index(drop=True)
    df.to_parquet(path, index=False)
    if write_csv:
        df.to_csv(path.with_suffix(".csv"), index=False)
    return BuildResult(name=name, path=str(path), rows=len(df))


def build_event_files(
    years: Iterable[int],
    *,
    cache_dir: Path,
    output_dir: Path,
    download_missing: bool,
    write_csv: bool,
) -> list[BuildResult]:
    frames: dict[str, list[pd.DataFrame]] = {name: [] for name in OUTPUTS}
    missing_years: list[int] = []
    processed_years: list[int] = []

    for year in years:
        print(f"[year] {year}", flush=True)
        pbp = _read_pbp_year(year, cache_dir, download_missing)
        if pbp is None:
            print(f"  [skip] missing cached play-by-play for {year}", flush=True)
            missing_years.append(year)
            continue
        processed_years.append(year)
        frames["pass_completions_50plus"].append(pass_completion_50plus_events(pbp))
        frames["reception_yardage_buckets"].append(reception_bucket_events(pbp))
        frames["special_teams_solo_tackles"].append(special_teams_solo_tackle_events(pbp))
        frames["fumble_recovery_yards"].append(fumble_recovery_yard_events(pbp))

    results: list[BuildResult] = []
    for name, parts in frames.items():
        df = pd.concat([part for part in parts if not part.empty], ignore_index=True) if parts else pd.DataFrame()
        results.append(_write_output(name, df, output_dir, write_csv))

    manifest = {
        "processed_years": processed_years,
        "missing_years": missing_years,
        "cache_dir": str(cache_dir),
        "output_dir": str(output_dir),
        "outputs": [result.__dict__ for result in results],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Build local event files for PBP-derived scoring columns.")
    parser.add_argument("--start-year", type=int, default=1999)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--download-missing", action="store_true")
    parser.add_argument("--csv", action="store_true", help="Also write CSV copies next to the parquet files.")
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1)
    results = build_event_files(
        years,
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
        download_missing=args.download_missing,
        write_csv=args.csv,
    )
    print("\n[outputs]", flush=True)
    for result in results:
        print(f"  {result.name}: {result.rows:,} rows -> {result.path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
