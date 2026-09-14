"""Known player identity repair guards for NFL supertable source data.

These guards repair confirmed same-name contamination and duplicate-ID bridges
before player_week keys are generated. They are intentionally data-driven so
the exact touched rows are auditable.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPAIR_MAP = REPO_ROOT / "ops_data" / "nfl_historical" / "known_identity_player_week_repair_map.csv"
DEFAULT_PBP_SPLIT_ROLLUP = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\stathead_generated\pbp_supertable_audit_1978_2025"
    r"\pbp_player_week_rollup.parquet"
)
DEFAULT_PFR_CACHE = REPO_ROOT / "fantasy_football_data" / "cache" / "pfr_excel"

# Confirmed same-name collisions where one source row can contain stat families
# from two different people.  These need row splitting, not a whole-row ID swap.
KNOWN_STAT_FAMILY_SPLITS = [
    {
        "player": "Mark Carrier",
        "source_id": "00-0002688",
        "target_ids": ("00-0002687", "00-0002688"),
        "target_positions": {"00-0002687": "WR", "00-0002688": "DB"},
        "target_teams": {"00-0002687": {"TB", "CLE", "CAR"}, "00-0002688": {"CHI", "DET", "WAS"}},
        "offense_ids": {"00-0002687"},
        "defense_ids": {"00-0002688"},
        "min_year": 1987,
        "max_year": 1998,
        "truth_reason": "Mark Carrier WR and Mark Carrier DB collided under DB gsis id",
    },
    {
        "player": "Kevin Williams",
        "source_id": "00-0017861",
        "target_ids": ("WillKe00", "00-0017861"),
        "target_positions": {"WillKe00": "WR", "00-0017861": "DB"},
        "target_teams": {"WillKe00": {"DAL", "ARI", "BUF"}, "00-0017861": {"NYJ", "MIA", "HOU"}},
        "offense_ids": {"WillKe00"},
        "defense_ids": {"00-0017861"},
        "min_year": 1993,
        "max_year": 1998,
        "truth_reason": "Kevin Williams WR/KR and Kevin Williams DB collided under DB gsis id",
    },
]

# Confirmed whole-row context/stat rebuilds from PFR game logs. These are not
# simple ID bridges: old rows had the correct display name with another same-name
# player's team/stat line partially grafted on.
KNOWN_CONTEXT_IDENTITY_REBUILDS = [
    {
        "source_file": "flx1980s.parquet",
        "pfr_player_id": "JoneKe00",
        "canonical_id": "JoneKe00",
        "player": "Keith Jones",
        "year": 1989,
        "position": "RB",
    },
    {
        "source_file": "flx1980s.parquet",
        "pfr_player_id": "JoneKe01",
        "canonical_id": "JoneKe01",
        "player": "Keith Jones",
        "year": 1989,
        "position": "RB",
    },
    {
        "source_file": "flx 70s.parquet",
        "pfr_player_id": "WashGe00",
        "canonical_id": "WashGe00",
        "player": "Gene Washington",
        "year": 1979,
        "position": "WR",
    },
    {
        "source_file": "flx 70s.parquet",
        "pfr_player_id": "WashGe20",
        "canonical_id": "WashGe20",
        "player": "Gene Washington",
        "year": 1979,
        "position": "WR",
    },
    {
        "source_file": "kickers.parquet",
        "pfr_player_id": "davisgre01",
        "canonical_id": "00-0003985",
        "player": "Greg Davis",
        "year": 1989,
        "position": "K",
    },
]

KNOWN_CONTEXT_IDENTITY_DELETE_ONLY_KEYS = {
    "davisgre01_1989_10",
    "00-0003985_1989_10",
}

PFR_GAMELOG_TO_SUPER_COLUMNS = {
    "Passing_Cmp": "completions",
    "Passing_Att": "attempts",
    "Passing_Yds": "passing_yards",
    "Passing_TD": "passing_tds",
    "Passing_Int": "passing_interceptions",
    "Passing_Sk": "sacks_suffered",
    "Rushing_Att": "carries",
    "Rushing_Yds": "rushing_yards",
    "Rushing_TD": "rushing_tds",
    "Receiving_Tgt": "targets",
    "Receiving_Rec": "receptions",
    "Receiving_Yds": "receiving_yards",
    "Receiving_TD": "receiving_tds",
    "Kick Returns_Ret": "kickoff_returns",
    "Kick Returns_Yds": "kickoff_return_yards",
    "Kick Returns_KRTD": "kickoff_return_tds",
    "Punt Returns_Ret": "punt_returns",
    "Punt Returns_Yds": "punt_return_yards",
    "Punt Returns_PRTD": "punt_return_tds",
    "XPM": "pat_made",
    "XPA": "pat_att",
    "FGM.1": "fg_made",
    "FGA": "fg_att",
}

OFFENSE_STAT_COLUMNS = {
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "sack_yards_lost",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "rushing_2pt_conversions",
    "rushing_first_downs",
    "rushing_epa",
    "rushing_long",
    "rushing_40plus",
    "rushing_tds_40plus",
    "rushing_tds_50plus",
    "targets",
    "target_share",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "receiving_2pt_conversions",
    "receiving_first_downs",
    "receiving_epa",
    "receiving_air_yards",
    "receiving_yards_after_catch",
    "receiving_long",
    "receptions_0_4",
    "receptions_5_9",
    "receptions_10_19",
    "receptions_20_29",
    "receptions_30_39",
    "receptions_40plus",
    "receiving_tds_40plus",
    "receiving_tds_50plus",
    "completions_40plus",
    "completions_50plus",
    "passing_tds_40plus",
    "passing_tds_50plus",
    "fumbles",
    "fumbles_lost",
    "rushing_fumbles",
    "rushing_fumbles_lost",
    "receiving_fumbles",
    "receiving_fumbles_lost",
    "sack_fumbles",
    "sack_fumbles_lost",
}

RETURN_STAT_COLUMNS = {
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "special_teams_tds",
    "special_teams_tackles_solo",
}

KICKING_STAT_COLUMNS = {
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_blocked",
    "fg_yards",
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
    "fg_made_60_",
    "fg_made_60_plus",
    "fg_made_60_plus_canonical",
    "fg_yards_over_30_canonical",
    "fg_yds_over_30_canonical",
    "fg_made_distance",
    "fg_missed_distance",
    "fg_blocked_distance",
    "fg_missed_0_19",
    "fg_missed_20_29",
    "fg_missed_30_39",
    "fg_missed_40_49",
    "fg_missed_50_59",
    "fg_missed_60_",
    "gwfg_att",
    "gwfg_made",
    "gwfg_missed",
    "gwfg_blocked",
    "gwfg_distance",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
    "pat_pct",
}

DEFENSE_STAT_COLUMNS = {
    "def_sacks",
    "def_sack_yards",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_fumbles_forced",
    "def_fumbles",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_tackles_for_loss",
    "def_tackles_for_loss_yards",
    "def_pass_defended",
    "def_qb_hits",
    "def_safeties",
    "def_blk_kick",
    "fum_rec",
    "fum_rec_yds",
    "fum_ret_td",
    "fumble_recovery_own",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
}


def _log(log_fn: Callable[[str], None] | None, message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def load_identity_player_week_repair_map(path: Path | None = None) -> pd.DataFrame:
    repair_path = path or DEFAULT_REPAIR_MAP
    if not repair_path.exists():
        return pd.DataFrame(
            columns=[
                "from_player_week",
                "to_player_week",
                "from_NFL_player_id",
                "to_NFL_player_id",
                "year",
                "week",
                "season_type",
                "repair_type",
                "truth_confidence",
                "truth_reason",
            ]
        )
    repair = pd.read_csv(repair_path, dtype=str).fillna("")
    for col in ["year", "week"]:
        if col in repair.columns:
            repair[col] = pd.to_numeric(repair[col], errors="coerce")
    return repair


def apply_known_identity_repairs(
    df: pd.DataFrame,
    *,
    repair_map_path: Path | None = None,
    id_col: str = "NFL_player_id",
    year_col: str = "year",
    week_col: str = "week",
    season_type_col: str = "season_type",
    player_week_col: str = "player_week",
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Apply confirmed identity repairs to an NFL stats frame.

    The map is keyed by player_week, but this function can synthesize the key
    from id/year/week when player_week has not been generated yet.
    """

    if id_col not in df.columns or year_col not in df.columns or week_col not in df.columns:
        return df

    repair = load_identity_player_week_repair_map(repair_map_path)
    if repair.empty:
        return df

    out = df.copy()
    had_player_week = player_week_col in out.columns
    if not had_player_week:
        out[player_week_col] = (
            out[id_col].astype(str)
            + "_"
            + pd.to_numeric(out[year_col], errors="coerce").astype("Int64").astype(str)
            + "_"
            + pd.to_numeric(out[week_col], errors="coerce").astype("Int64").astype(str)
        )

    repair_by_key = repair.drop_duplicates("from_player_week", keep="first").set_index("from_player_week")
    matched = out[player_week_col].astype(str).isin(repair_by_key.index)
    if not matched.any():
        if not had_player_week:
            out = out.drop(columns=[player_week_col])
        return out

    matched_keys = out.loc[matched, player_week_col].astype(str)
    out.loc[matched, id_col] = matched_keys.map(repair_by_key["to_NFL_player_id"]).to_numpy()
    out.loc[matched, player_week_col] = matched_keys.map(repair_by_key["to_player_week"]).to_numpy()

    _log(log_fn, f"  Applied known identity repairs to {int(matched.sum()):,} rows")

    if not had_player_week:
        out = out.drop(columns=[player_week_col])
    return out


def _norm_int_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _build_player_week(id_value: object, year_value: object, week_value: object) -> str:
    year = pd.to_numeric(pd.Series([year_value]), errors="coerce").astype("Int64").iloc[0]
    week = pd.to_numeric(pd.Series([week_value]), errors="coerce").astype("Int64").iloc[0]
    return f"{id_value}_{year}_{week}"


def _load_split_pbp_rollup(
    *,
    rollup_path: Path | None = None,
    needed_ids: set[str] | None = None,
    needed_columns: set[str] | None = None,
) -> pd.DataFrame:
    path = rollup_path or DEFAULT_PBP_SPLIT_ROLLUP
    base_cols = [
        "NFL_player_id",
        "player_week",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
    ]
    if not path.exists():
        return pd.DataFrame(columns=base_cols)

    try:
        import pyarrow.parquet as pq

        available_cols = set(pq.read_schema(path).names)
    except Exception:
        available_cols = set(pd.read_parquet(path).columns)
    cols = [c for c in base_cols if c in available_cols]
    for col in sorted(needed_columns or set()):
        if col in available_cols and col not in cols:
            cols.append(col)

    pbp = pd.read_parquet(path, columns=cols)
    if needed_ids and "NFL_player_id" in pbp.columns:
        pbp = pbp[pbp["NFL_player_id"].astype(str).isin(needed_ids)].copy()
    if pbp.empty:
        return pbp

    pbp["__split_year"] = _norm_int_series(pbp["year"])
    pbp["__split_week"] = _norm_int_series(pbp["week"])
    pbp["NFL_player_id"] = pbp["NFL_player_id"].astype(str)
    for col in needed_columns or set():
        if col in pbp.columns:
            pbp[col] = pd.to_numeric(pbp[col], errors="coerce").fillna(0.0)
    return pbp


def _zero_columns(row: pd.Series, columns: set[str], existing_cols: set[str]) -> pd.Series:
    for col in columns:
        if col in existing_cols:
            row[col] = 0.0
    return row


def _fill_zero_family_values_from_pbp(
    row: pd.Series,
    pbp_row: pd.Series,
    columns: set[str],
    existing_cols: set[str],
) -> pd.Series:
    for col in columns:
        if col not in existing_cols or col not in pbp_row.index:
            continue
        current = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
        pbp_value = pd.to_numeric(pd.Series([pbp_row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
        if current == 0 and pbp_value != 0:
            row[col] = pbp_value
    return row


def _set_family_values_from_pbp(
    row: pd.Series,
    pbp_row: pd.Series,
    columns: set[str],
    existing_cols: set[str],
) -> pd.Series:
    for col in columns:
        if col not in existing_cols or col not in pbp_row.index:
            continue
        pbp_value = pd.to_numeric(pd.Series([pbp_row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
        row[col] = pbp_value
    return row


def _row_family_total(row: pd.Series, columns: set[str], existing_cols: set[str]) -> float:
    total = 0.0
    for col in columns:
        if col in existing_cols:
            value = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
            total += abs(float(value))
    return total


def _build_year_context_fallbacks(pbp: pd.DataFrame) -> dict[tuple[str, int], tuple[object, object]]:
    if pbp.empty or "nfl_team" not in pbp.columns:
        return {}

    fallbacks: dict[tuple[str, int], tuple[object, object]] = {}
    concrete = pbp[pbp["nfl_team"].notna()].copy()
    if concrete.empty:
        return fallbacks

    for (player_id, year), group in concrete.groupby(["NFL_player_id", "__split_year"], dropna=True):
        team_counts = group["nfl_team"].astype(str).value_counts()
        if team_counts.empty:
            continue
        team = team_counts.index[0]
        team_group = group[group["nfl_team"].astype(str).eq(team)]
        opp = None
        if "opponent_nfl_team" in team_group.columns:
            opp_values = team_group["opponent_nfl_team"].dropna().astype(str)
            if not opp_values.empty:
                opp = opp_values.value_counts().index[0]
        fallbacks[(str(player_id), int(year))] = (team, opp)
    return fallbacks


def _numeric_value(value: object) -> float:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return 0.0
    return float(number)


def _age_years(value: object) -> int | pd.NA:
    if pd.isna(value):
        return pd.NA
    text = str(value)
    if "-" in text:
        text = text.split("-", 1)[0]
    number = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    return pd.NA if pd.isna(number) else int(number)


def _super_team_code(value: object) -> object:
    if pd.isna(value):
        return value
    code = str(value).strip()
    replacements = {
        "GNB": "GB",
        "KAN": "KC",
        "NWE": "NE",
        "NOR": "NO",
        "SFO": "SF",
        "TAM": "TB",
    }
    return replacements.get(code, code)


def _pfr_position(row: pd.Series, fallback: str) -> str:
    for col in ("Pos.", "Punt Returns_Pos.", "Rushing_Pos.", "Receiving_Pos.", "Pos"):
        value = row.get(col)
        if pd.notna(value) and str(value).strip():
            return str(value).strip()
    return fallback


def _load_context_rebuild_source_rows(
    *,
    pfr_cache_dir: Path,
    specs: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in specs or KNOWN_CONTEXT_IDENTITY_REBUILDS:
        source_path = pfr_cache_dir / str(spec["source_file"])
        if not source_path.exists():
            continue
        source = pd.read_parquet(source_path)
        if "pfr_player_id" not in source.columns or "Date" not in source.columns or "Week" not in source.columns:
            continue
        source = source[source["pfr_player_id"].astype(str).eq(str(spec["pfr_player_id"]))].copy()
        source["__context_year"] = pd.to_datetime(source["Date"], errors="coerce").dt.year
        source["__context_week"] = pd.to_numeric(source["Week"], errors="coerce").astype("Int64")
        source = source[source["__context_year"].eq(int(spec["year"]))].copy()
        for _, row in source.iterrows():
            if pd.isna(row["__context_week"]):
                continue
            item = row.to_dict()
            item.update(
                {
                    "__canonical_id": spec["canonical_id"],
                    "__truth_player": spec["player"],
                    "__truth_position": spec["position"],
                    "__truth_source_file": spec["source_file"],
                }
            )
            rows.append(item)
    return pd.DataFrame(rows)


def _context_rebuild_template(
    df: pd.DataFrame,
    *,
    player_week: str,
    player: str,
    player_id: str,
    year: int,
    week: int,
    id_col: str,
    player_col: str,
    week_col: str,
) -> pd.Series:
    if "player_week" in df.columns:
        exact = df[df["player_week"].astype(str).eq(player_week)]
        if not exact.empty:
            return exact.iloc[0].copy()

    ids = df.get(id_col, pd.Series("", index=df.index)).astype(str)
    same_id = df[ids.eq(player_id)]
    if not same_id.empty:
        weeks = pd.to_numeric(same_id.get(week_col), errors="coerce")
        idx = (weeks - week).abs().sort_values().index[0]
        return same_id.loc[idx].copy()

    players = df.get(player_col, pd.Series("", index=df.index)).astype(str)
    same_player = df[players.eq(player)]
    if not same_player.empty:
        weeks = pd.to_numeric(same_player.get(week_col), errors="coerce")
        idx = (weeks - week).abs().sort_values().index[0]
        return same_player.loc[idx].copy()

    return pd.Series({col: pd.NA for col in df.columns})


def apply_known_context_identity_rebuilds(
    df: pd.DataFrame,
    *,
    pfr_cache_dir: Path | None = None,
    rollup_path: Path | None = None,
    id_col: str = "NFL_player_id",
    year_col: str = "year",
    week_col: str = "week",
    season_type_col: str = "season_type",
    player_week_col: str = "player_week",
    player_col: str = "player",
    position_col: str = "position",
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Rebuild confirmed same-name rows from PFR game logs.

    This handles residual cases where the row key/name survived but team/opponent
    and stat lines drifted across two different people with the same display
    name. PFR game logs are authoritative for box-score counts; PBP rollup is
    used only for derived atoms PFR game logs do not carry.
    """

    required = {id_col, year_col, week_col, player_col}
    if not required.issubset(df.columns):
        return df

    pfr_dir = pfr_cache_dir or DEFAULT_PFR_CACHE
    truth = _load_context_rebuild_source_rows(pfr_cache_dir=pfr_dir)
    if truth.empty:
        _log(log_fn, "  Skipping context identity rebuilds: PFR source rows unavailable")
        return df

    out = df.copy()
    had_player_week = player_week_col in out.columns
    if not had_player_week:
        out[player_week_col] = (
            out[id_col].astype(str)
            + "_"
            + _norm_int_series(out[year_col]).astype(str)
            + "_"
            + _norm_int_series(out[week_col]).astype(str)
        )

    existing_cols = set(out.columns)
    stat_cols = (
        OFFENSE_STAT_COLUMNS
        | RETURN_STAT_COLUMNS
        | KICKING_STAT_COLUMNS
        | DEFENSE_STAT_COLUMNS
        | {"punts", "punt_yards", "punt_long", "punts_blocked", "fg_missed", "fantasy_points_ppr"}
    ) & existing_cols
    truth_keys = {
        _build_player_week(row["__canonical_id"], row["__context_year"], row["__context_week"])
        for _, row in truth.iterrows()
    }
    pbp = _load_split_pbp_rollup(
        rollup_path=rollup_path,
        needed_ids={str(row["__canonical_id"]) for _, row in truth.iterrows()},
        needed_columns=stat_cols,
    )
    pbp_by_key = (
        pbp.drop_duplicates(player_week_col, keep="first").set_index(player_week_col) if not pbp.empty else None
    )

    rebuilt_rows: list[pd.Series] = []
    for _, source in truth.iterrows():
        player_id = str(source["__canonical_id"])
        player = str(source["__truth_player"])
        year = int(source["__context_year"])
        week = int(source["__context_week"])
        player_week = _build_player_week(player_id, year, week)
        row = _context_rebuild_template(
            out,
            player_week=player_week,
            player=player,
            player_id=player_id,
            year=year,
            week=week,
            id_col=id_col,
            player_col=player_col,
            week_col=week_col,
        )

        for col in stat_cols:
            if col in row.index:
                row[col] = 0.0

        row[id_col] = player_id
        row[player_week_col] = player_week
        row[player_col] = player
        row[year_col] = year
        row[week_col] = week
        if season_type_col in existing_cols:
            row[season_type_col] = "REG"
        if "nfl_team" in existing_cols:
            row["nfl_team"] = _super_team_code(source.get("Team"))
        if "opponent_nfl_team" in existing_cols:
            row["opponent_nfl_team"] = _super_team_code(source.get("Opp"))
        position = _pfr_position(source, str(source["__truth_position"]))
        for pos_col in (position_col, "nfl_position", "fantasy_position"):
            if pos_col in existing_cols:
                row[pos_col] = position
        if "age" in existing_cols:
            row["age"] = _age_years(source.get("Age"))
        if "pts" in existing_cols and pd.notna(source.get("Pts")):
            row["pts"] = str(int(_numeric_value(source.get("Pts"))))

        pfr_targets: set[str] = set()
        for pfr_col, target_col in PFR_GAMELOG_TO_SUPER_COLUMNS.items():
            if pfr_col in source.index and target_col in existing_cols:
                row[target_col] = _numeric_value(source.get(pfr_col))
                pfr_targets.add(target_col)
        if "fg_att" in existing_cols and "fg_missed" in existing_cols:
            row["fg_missed"] = max(0.0, _numeric_value(row.get("fg_att")) - _numeric_value(row.get("fg_made")))
            pfr_targets.add("fg_missed")
        if "pat_att" in existing_cols and "pat_missed" in existing_cols:
            row["pat_missed"] = max(0.0, _numeric_value(row.get("pat_att")) - _numeric_value(row.get("pat_made")))
            pfr_targets.add("pat_missed")

        if pbp_by_key is not None and player_week in pbp_by_key.index:
            pbp_row = pbp_by_key.loc[player_week]
            for col in stat_cols:
                if col in pfr_targets or col not in row.index or col not in pbp_row.index:
                    continue
                row[col] = _numeric_value(pbp_row.get(col))

        rebuilt_rows.append(row)

    if not rebuilt_rows:
        if not had_player_week:
            out = out.drop(columns=[player_week_col], errors="ignore")
        return out

    delete_keys = truth_keys | KNOWN_CONTEXT_IDENTITY_DELETE_ONLY_KEYS
    keep = ~out[player_week_col].astype(str).isin(delete_keys)
    rebuilt = pd.DataFrame(rebuilt_rows)
    final_cols = list(dict.fromkeys([*out.columns, *rebuilt.columns]))
    out = pd.concat(
        [out.loc[keep].dropna(axis=1, how="all"), rebuilt.dropna(axis=1, how="all")],
        ignore_index=True,
        sort=False,
    ).reindex(columns=final_cols)

    if not had_player_week:
        out = out.drop(columns=[player_week_col], errors="ignore")

    _log(
        log_fn,
        "  Applied context identity rebuilds: "
        f"{len(rebuilt_rows):,} truth rows, {int((~keep).sum()):,} existing rows replaced/deleted",
    )
    return out.reset_index(drop=True)


def _choose_context_value(
    *,
    pbp_value: object,
    source_value: object,
    source_team: object,
    target_teams: set[str],
    fallback_value: object,
    use_source_if_team_matches: bool,
) -> object:
    if pd.notna(pbp_value) and str(pbp_value) != "":
        return pbp_value
    if use_source_if_team_matches and pd.notna(source_team) and str(source_team) in target_teams:
        return source_value
    if pd.notna(fallback_value) and str(fallback_value) != "":
        return fallback_value
    return pd.NA


def apply_known_stat_family_splits(
    df: pd.DataFrame,
    *,
    rollup_path: Path | None = None,
    id_col: str = "NFL_player_id",
    year_col: str = "year",
    week_col: str = "week",
    season_type_col: str = "season_type",
    player_week_col: str = "player_week",
    player_col: str = "player",
    position_col: str = "position",
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Split confirmed same-name rows that contain mixed stat families.

    Whole-row repair maps cannot handle cases like Mark Carrier WR/DB where a
    single weekly row has WR box-score atoms and DB IDP atoms. This function
    replaces only confirmed collision rows with one row per true PBP identity.
    Existing official stat-family values are preserved; PBP rollup values are
    used only when the counterpart row is otherwise missing.
    """

    required = {id_col, year_col, week_col, player_col}
    if not required.issubset(df.columns):
        return df

    existing_cols = set(df.columns)
    family_cols = (OFFENSE_STAT_COLUMNS | RETURN_STAT_COLUMNS | DEFENSE_STAT_COLUMNS) & existing_cols
    if not family_cols:
        return df

    needed_ids: set[str] = set()
    for spec in KNOWN_STAT_FAMILY_SPLITS:
        needed_ids.update(spec["target_ids"])
    pbp = _load_split_pbp_rollup(
        rollup_path=rollup_path,
        needed_ids=needed_ids,
        needed_columns=family_cols,
    )
    if pbp.empty:
        _log(log_fn, "  Skipping stat-family identity splits: PBP rollup missing or empty")
        return df
    context_fallbacks = _build_year_context_fallbacks(pbp)

    out = df.copy()
    had_player_week = player_week_col in out.columns
    if not had_player_week:
        out[player_week_col] = (
            out[id_col].astype(str)
            + "_"
            + _norm_int_series(out[year_col]).astype(str)
            + "_"
            + _norm_int_series(out[week_col]).astype(str)
        )

    out["__split_year"] = _norm_int_series(out[year_col])
    out["__split_week"] = _norm_int_series(out[week_col])

    rows_to_keep = pd.Series(True, index=out.index)
    replacement_rows: list[pd.Series] = []
    touched_source_rows = 0

    for spec in KNOWN_STAT_FAMILY_SPLITS:
        source_id = spec["source_id"]
        target_ids = set(spec["target_ids"])
        source_mask = (
            out[id_col].astype(str).eq(source_id)
            & out[player_col].astype(str).eq(spec["player"])
            & out["__split_year"].between(spec["min_year"], spec["max_year"])
        )
        source_idx = out.index[source_mask]
        if len(source_idx) == 0:
            continue

        pbp_spec = pbp[
            pbp["NFL_player_id"].isin(target_ids) & pbp["__split_year"].between(spec["min_year"], spec["max_year"])
        ]
        if pbp_spec.empty:
            continue

        pbp_by_week = {
            (int(year), int(week)): group
            for (year, week), group in pbp_spec.groupby(["__split_year", "__split_week"], dropna=True)
        }

        for idx in source_idx:
            source_row = out.loc[idx]
            year = source_row["__split_year"]
            week = source_row["__split_week"]
            if pd.isna(year) or pd.isna(week):
                continue
            role_rows = pbp_by_week.get((int(year), int(week)))
            if role_rows is None or role_rows.empty:
                continue

            role_rows = role_rows[role_rows["NFL_player_id"].isin(target_ids)]
            if role_rows.empty:
                continue

            source_offense = _row_family_total(source_row, OFFENSE_STAT_COLUMNS | RETURN_STAT_COLUMNS, existing_cols)
            source_defense = _row_family_total(source_row, DEFENSE_STAT_COLUMNS, existing_cols)
            source_has_stats = source_offense > 0 or source_defense > 0
            if not source_has_stats:
                continue

            rows_to_keep.loc[idx] = False
            touched_source_rows += 1

            for _, pbp_row in role_rows.iterrows():
                target_id = str(pbp_row["NFL_player_id"])
                new_row = source_row.copy()
                new_row[id_col] = target_id
                new_row[player_week_col] = _build_player_week(target_id, source_row[year_col], source_row[week_col])
                if player_col in existing_cols and pd.notna(pbp_row.get("player")):
                    new_row[player_col] = pbp_row["player"]
                target_position = spec["target_positions"].get(target_id) or pbp_row.get("position")
                for pos_col in (position_col, "nfl_position", "fantasy_position"):
                    if pos_col in existing_cols and target_position:
                        new_row[pos_col] = target_position
                target_teams = spec.get("target_teams", {}).get(target_id, set())
                fallback_team, fallback_opp = context_fallbacks.get(
                    (target_id, int(year)),
                    (pd.NA, pd.NA),
                )
                source_team = source_row.get("nfl_team", pd.NA)
                if "nfl_team" in existing_cols:
                    new_row["nfl_team"] = _choose_context_value(
                        pbp_value=pbp_row.get("nfl_team"),
                        source_value=source_row.get("nfl_team"),
                        source_team=source_team,
                        target_teams=target_teams,
                        fallback_value=fallback_team,
                        use_source_if_team_matches=True,
                    )
                if "opponent_nfl_team" in existing_cols:
                    new_row["opponent_nfl_team"] = _choose_context_value(
                        pbp_value=pbp_row.get("opponent_nfl_team"),
                        source_value=source_row.get("opponent_nfl_team"),
                        source_team=source_team,
                        target_teams=target_teams,
                        fallback_value=fallback_opp,
                        use_source_if_team_matches=True,
                    )

                if target_id in spec["offense_ids"]:
                    new_row = _zero_columns(new_row, DEFENSE_STAT_COLUMNS, existing_cols)
                    new_row = _set_family_values_from_pbp(
                        new_row,
                        pbp_row,
                        OFFENSE_STAT_COLUMNS | RETURN_STAT_COLUMNS,
                        existing_cols,
                    )
                elif target_id in spec["defense_ids"]:
                    new_row = _zero_columns(new_row, OFFENSE_STAT_COLUMNS, existing_cols)
                    # Return columns on a copied offensive row belong to the
                    # offensive/returner identity.  For the DB row, use PBP
                    # returns unless the original row already had DB context.
                    source_team = str(source_row.get("nfl_team", ""))
                    pbp_team = str(pbp_row.get("nfl_team", ""))
                    if source_team != pbp_team:
                        new_row = _zero_columns(new_row, RETURN_STAT_COLUMNS, existing_cols)
                    new_row = _fill_zero_family_values_from_pbp(
                        new_row,
                        pbp_row,
                        DEFENSE_STAT_COLUMNS | RETURN_STAT_COLUMNS,
                        existing_cols,
                    )
                else:
                    continue

                replacement_rows.append(new_row.drop(labels=["__split_year", "__split_week"], errors="ignore"))

    out = out.loc[rows_to_keep].drop(columns=["__split_year", "__split_week"], errors="ignore")
    if replacement_rows:
        replacement_df = pd.DataFrame(replacement_rows)
        final_cols = list(dict.fromkeys([*out.columns, *replacement_df.columns]))
        out_for_concat = out.dropna(axis=1, how="all")
        replacement_for_concat = replacement_df.dropna(axis=1, how="all")
        out = pd.concat([out_for_concat, replacement_for_concat], ignore_index=True, sort=False).reindex(
            columns=final_cols
        )
        _log(
            log_fn,
            "  Applied stat-family identity splits to "
            f"{touched_source_rows:,} source rows -> {len(replacement_rows):,} repaired rows",
        )
    elif not had_player_week:
        out = out.drop(columns=[player_week_col], errors="ignore")
        return out

    if not had_player_week:
        out = out.drop(columns=[player_week_col], errors="ignore")
    return out.reset_index(drop=True)


def apply_known_special_teams_identity_repairs(
    df: pd.DataFrame,
    *,
    rollup_path: Path | None = None,
    id_col: str = "NFL_player_id",
    year_col: str = "year",
    week_col: str = "week",
    season_type_col: str = "season_type",
    player_week_col: str = "player_week",
    player_col: str = "player",
    position_col: str = "position",
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Repair confirmed narrow special-teams identity collisions.

    Currently handles the 1987 Steve Jordan collision:
    - `00-0008927` is the Vikings TE and should not carry kicking stats.
    - `JOR577792` is the Colts replacement kicker.

    Mike Michel and Dave Green are not handled here because they are dual P/K
    aliases for the same person and need a broader canonical-ID policy.
    """

    required = {id_col, year_col, week_col, player_col}
    if not required.issubset(df.columns):
        return df

    existing_cols = set(df.columns)
    kicking_cols = KICKING_STAT_COLUMNS & existing_cols
    if not kicking_cols:
        return df

    pbp = _load_split_pbp_rollup(
        rollup_path=rollup_path,
        needed_ids={"JOR577792"},
        needed_columns=kicking_cols,
    )
    if pbp.empty:
        _log(log_fn, "  Skipping special-teams identity repairs: PBP rollup missing or empty")
        return df

    out = df.copy()
    had_player_week = player_week_col in out.columns
    if not had_player_week:
        out[player_week_col] = (
            out[id_col].astype(str)
            + "_"
            + _norm_int_series(out[year_col]).astype(str)
            + "_"
            + _norm_int_series(out[week_col]).astype(str)
        )

    out["__st_year"] = _norm_int_series(out[year_col])
    out["__st_week"] = _norm_int_series(out[week_col])

    player = out[player_col].astype(str)
    ids = out[id_col].astype(str)
    weeks = out["__st_week"]
    years = out["__st_year"]
    positions = out.get(position_col, pd.Series("", index=out.index)).astype(str)
    player_weeks = out[player_week_col].astype(str)

    leak_mask = (
        player.eq("Steve Jordan") & ids.eq("00-0008927") & positions.eq("TE") & years.eq(1987) & weeks.isin([1, 2])
    )
    for col in kicking_cols:
        out.loc[leak_mask, col] = 0.0

    pbp_steve = pbp[
        pbp["NFL_player_id"].astype(str).eq("JOR577792")
        & pbp["__split_year"].eq(1987)
        & pbp["__split_week"].isin([4, 5, 6])
    ].copy()
    pbp_by_week = {int(row["__split_week"]): row for _, row in pbp_steve.iterrows()}

    source_mask = (
        player.eq("Steve Jordan")
        & years.eq(1987)
        & (
            positions.eq("K")
            | player_weeks.str.startswith("JOR577792_")
            | (ids.eq("00-0008927") & weeks.isin([4, 5, 6]))
        )
    )
    touched = int(leak_mask.sum())

    for idx in out.index[source_mask]:
        week = out.at[idx, "__st_week"]
        if pd.isna(week) or int(week) not in pbp_by_week:
            continue
        pbp_row = pbp_by_week[int(week)]
        out.at[idx, id_col] = "JOR577792"
        out.at[idx, player_week_col] = _build_player_week("JOR577792", out.at[idx, year_col], out.at[idx, week_col])
        if player_col in existing_cols and pd.notna(pbp_row.get("player")):
            out.at[idx, player_col] = pbp_row["player"]
        for pos_col in (position_col, "nfl_position", "fantasy_position"):
            if pos_col in existing_cols:
                out.at[idx, pos_col] = "K"
        for context_col in ("nfl_team", "opponent_nfl_team", season_type_col):
            if context_col in existing_cols and context_col in pbp_row.index:
                out.at[idx, context_col] = pbp_row[context_col]
        for col in (OFFENSE_STAT_COLUMNS | RETURN_STAT_COLUMNS | DEFENSE_STAT_COLUMNS) & existing_cols:
            out.at[idx, col] = 0.0
        for col in kicking_cols:
            if col in pbp_row.index:
                out.at[idx, col] = pd.to_numeric(pd.Series([pbp_row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
        touched += 1

    existing_player_weeks = set(out[player_week_col].dropna().astype(str))
    template = out.loc[source_mask].iloc[0].copy() if source_mask.any() else None
    replacement_rows: list[pd.Series] = []
    if template is not None:
        for week, pbp_row in pbp_by_week.items():
            key = f"JOR577792_1987_{week}"
            if key in existing_player_weeks:
                continue
            new_row = template.copy()
            new_row[id_col] = "JOR577792"
            new_row[year_col] = 1987
            new_row[week_col] = week
            new_row[player_week_col] = key
            if player_col in existing_cols:
                new_row[player_col] = pbp_row.get("player", "Steve Jordan")
            for pos_col in (position_col, "nfl_position", "fantasy_position"):
                if pos_col in existing_cols:
                    new_row[pos_col] = "K"
            for context_col in ("nfl_team", "opponent_nfl_team", season_type_col):
                if context_col in existing_cols and context_col in pbp_row.index:
                    new_row[context_col] = pbp_row[context_col]
            for col in (
                OFFENSE_STAT_COLUMNS | RETURN_STAT_COLUMNS | DEFENSE_STAT_COLUMNS | kicking_cols
            ) & existing_cols:
                new_row[col] = 0.0
            for col in kicking_cols:
                if col in pbp_row.index:
                    new_row[col] = pd.to_numeric(pd.Series([pbp_row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
            replacement_rows.append(new_row.drop(labels=["__st_year", "__st_week"], errors="ignore"))

    out = out.drop(columns=["__st_year", "__st_week"], errors="ignore")
    if replacement_rows:
        replacement_df = pd.DataFrame(replacement_rows)
        final_cols = list(dict.fromkeys([*out.columns, *replacement_df.columns]))
        out = pd.concat(
            [out.dropna(axis=1, how="all"), replacement_df.dropna(axis=1, how="all")],
            ignore_index=True,
            sort=False,
        ).reindex(columns=final_cols)
        touched += len(replacement_rows)

    if touched:
        _log(log_fn, f"  Applied special-teams identity repairs to {touched:,} Steve Jordan rows")

    if not had_player_week:
        out = out.drop(columns=[player_week_col], errors="ignore")
    return out.reset_index(drop=True)
