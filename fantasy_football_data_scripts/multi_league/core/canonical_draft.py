"""Canonical draft schema - all platforms normalized.

Fetchers return raw API data + join keys. LAMAR, pick quality, grades
are all SQL enrichments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from multi_league.core.manager_identity import hidden_manager_guid_mask


DRAFT_SCHEMA: list[tuple[str, str, str]] = [
    ("db_name", "VARCHAR", "system"),
    # === Identity ===
    ("year", "INTEGER", "api"),
    ("round", "INTEGER", "api"),
    ("pick", "INTEGER", "api"),  # overall pick number
    ("pick_in_round", "INTEGER", "api"),  # pick within round (ESPN)
    ("draft_slot", "INTEGER", "api"),  # draft order position (snake)
    ("draft_slot_roster_id", "INTEGER", "api"),  # Sleeper roster id whose slot produced this pick
    ("manager", "VARCHAR", "api"),
    ("manager_guid", "VARCHAR", "api"),
    ("franchise_id", "VARCHAR", "join_key"),
    ("team_key", "VARCHAR", "api"),
    ("team_name", "VARCHAR", "api"),
    ("platform", "VARCHAR", "api"),
    ("league_id", "VARCHAR", "api"),
    # === Player identity ===
    ("player", "VARCHAR", "api"),
    ("position", "VARCHAR", "api"),  # NFL position
    ("nfl_team_api", "VARCHAR", "api"),  # raw from API (unreliable for Yahoo)
    ("yahoo_player_id", "VARCHAR", "api"),
    ("sleeper_player_id", "VARCHAR", "api"),
    ("espn_player_id", "VARCHAR", "api"),
    ("fleaflicker_player_id", "VARCHAR", "api"),
    # === Draft details ===
    ("cost", "DOUBLE", "api"),  # auction cost (0 or NULL for snake)
    ("draft_type", "VARCHAR", "api"),  # "auction" | "snake"
    ("is_keeper", "INTEGER", "api"),  # 1 if keeper pick, 0 otherwise
    # === ADP data removed - lives in ___ops.yahoo_historical.yahoo_draft_analysis ===
    # Frontend joins at query time: draft.yahoo_player_id + draft.year -> ops table
    # === Sleeper-specific ===
    ("draft_id", "VARCHAR", "api"),  # Sleeper draft ID
    ("draft_category", "VARCHAR", "api"),  # rookie/startup/veteran
    # === ESPN-specific ===
    ("auto_drafted", "INTEGER", "api"),  # 1 if autopick, 0 if manual
    ("trade_locked", "INTEGER", "api"),  # 1 if pick was traded
    ("nominated_by", "VARCHAR", "api"),  # auction: who nominated (team_id)
    # === SQL enrichments: identity ===
    ("NFL_player_id", "VARCHAR", "sql"),
    ("nfl_team", "VARCHAR", "sql"),  # from super_table
    ("franchise_name", "VARCHAR", "sql"),
    # === SQL enrichments: core draft metrics ===
    ("manager_lamar", "DOUBLE", "sql"),
    ("player_lamar", "DOUBLE", "sql"),
    ("pick_quality_zscore", "DOUBLE", "sql"),
    ("draft_value_zscore", "DOUBLE", "sql"),
    ("expected_lamar", "DOUBLE", "sql"),
    ("draft_grade", "VARCHAR", "sql"),
    ("manager_draft_score", "DOUBLE", "sql"),
    ("manager_draft_grade", "VARCHAR", "sql"),
    ("keeper_draft_score", "DOUBLE", "sql"),
    ("keeper_draft_grade", "VARCHAR", "sql"),
    ("pick_score", "DOUBLE", "sql"),
    ("cost_bucket", "INTEGER", "sql"),
    ("position_draft_rank", "INTEGER", "sql"),
    ("position_draft_label", "VARCHAR", "sql"),
    ("position_percentile", "DOUBLE", "sql"),
    # === SQL enrichments: player season stats (from player_fantasy aggregation) ===
    ("total_fantasy_points", "DOUBLE", "sql"),
    ("season_ppg", "DOUBLE", "sql"),
    ("games_played", "INTEGER", "sql"),
    ("games_started", "INTEGER", "sql"),
    ("games_eligible", "INTEGER", "sql"),
    ("weeks_rostered", "INTEGER", "sql"),
    ("weeks_started", "INTEGER", "sql"),
    ("season_position_rank", "INTEGER", "sql"),
    # === SQL enrichments: manager aggregates (broadcast to each pick) ===
    ("manager_total_lamar", "DOUBLE", "sql"),
    ("manager_avg_lamar", "DOUBLE", "sql"),
    ("manager_picks_count", "INTEGER", "sql"),
    ("manager_hit_rate", "DOUBLE", "sql"),
    ("manager_draft_percentile_alltime", "DOUBLE", "sql"),
    # === SQL enrichments: age ===
    ("draft_age", "INTEGER", "sql"),
    ("expected_age", "DOUBLE", "sql"),
    ("draft_age_zscore", "DOUBLE", "sql"),
    ("draft_age_grade", "VARCHAR", "sql"),
    ("manager_weighted_age", "DOUBLE", "sql"),
    # === SQL enrichments: bench/starter analysis ===
    ("starter_slots_available", "INTEGER", "sql"),
    ("drafted_as_starter", "INTEGER", "sql"),
    ("drafted_as_backup", "INTEGER", "sql"),
    ("position_activation_rate", "DOUBLE", "sql"),
    ("bench_insurance_discount", "DOUBLE", "sql"),
    ("bench_lamar", "DOUBLE", "sql"),
    ("position_failure_rate", "DOUBLE", "sql"),
    ("failure_rate", "DOUBLE", "sql"),
    ("bench_value_by_rank", "DOUBLE", "sql"),
]

RAW_COLUMNS = [name for name, _, src in DRAFT_SCHEMA if src in ("api", "join_key")]
SQL_COLUMNS = [name for name, _, src in DRAFT_SCHEMA if src == "sql"]
ALL_COLUMNS = [name for name, _, _ in DRAFT_SCHEMA]
COLUMN_TYPES = {name: dtype for name, dtype, _ in DRAFT_SCHEMA}

_NFL_TEAM_MARKERS = {
    "ARI",
    "ATL",
    "BAL",
    "BUF",
    "CAR",
    "CHI",
    "CIN",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GB",
    "HOU",
    "IND",
    "JAC",
    "JAX",
    "KC",
    "LA",
    "LAC",
    "LAR",
    "LV",
    "MIA",
    "MIN",
    "NE",
    "NO",
    "NYG",
    "NYJ",
    "OAK",
    "PHI",
    "PIT",
    "SD",
    "SEA",
    "SF",
    "STL",
    "TB",
    "TEN",
    "WAS",
    "WSH",
}
_NON_NFL_TEAM_MARKERS = {"FA", "SDSU"}
_POSITION_MARKERS = {"QB", "RB", "WR", "TE", "K", "DEF", "DST", "D/ST", "DB", "DL", "LB"}
_EXTERNAL_PLAYER_NAME_ALIASES = {
    "alshon jeffries": "Alshon Jeffery",
    "chigoziem okonkwo": "Chig Okonkwo",
    "derick carr": "Derek Carr",
    "gabriel davis": "Gabe Davis",
    "hollywood brown": "Marquise Brown",
    "jacobi myers": "Jakobi Meyers",
    "joshua palmer": "Josh Palmer",
    "ken walker": "Kenneth Walker III",
    "maquise brown": "Marquise Brown",
    "mitch trubisky": "Mitchell Trubisky",
    "robbie anderson": "Robbie Chosen",
    "robby anderson": "Robbie Chosen",
    "jesper horset": "Jesper Horsted",
}


def split_external_draft_player(value) -> tuple[str, str | None, str | None]:
    """Split external draft strings like ``Player, Team POS`` into stable fields."""
    if value is None:
        return "", None, None

    raw = str(value).strip()
    if not raw or raw.lower() in {"none", "nan", "<na>"}:
        return "", None, None

    def parse_tail(tail: str) -> tuple[str | None, str | None] | None:
        tokens = tail.strip().split()
        if not tokens:
            return None
        marker = tokens[0].upper()
        if marker not in _NFL_TEAM_MARKERS and marker not in _NON_NFL_TEAM_MARKERS:
            return None
        team = "JAX" if marker == "JAC" else "WAS" if marker == "WSH" else marker
        if team in _NON_NFL_TEAM_MARKERS:
            team = None
        position = next((tok.upper() for tok in tokens[1:] if tok.upper() in _POSITION_MARKERS), None)
        if position in {"DST", "D/ST"}:
            position = "DEF"
        return team, position

    if "," in raw:
        player_part, tail = raw.rsplit(",", 1)
        parsed = parse_tail(tail)
        if parsed:
            team, position = parsed
            return player_part.strip(), team, position

    tokens = raw.split()
    if len(tokens) > 1:
        marker = tokens[-1].upper()
        if marker in _NFL_TEAM_MARKERS or marker in _NON_NFL_TEAM_MARKERS:
            team = "JAX" if marker == "JAC" else "WAS" if marker == "WSH" else marker
            if team in _NON_NFL_TEAM_MARKERS:
                team = None
            return " ".join(tokens[:-1]).strip(), team, None

    return raw, None, None


def canonicalize_external_draft_player_name(value) -> str:
    """Return a canonical display name for known external draft aliases."""
    from multi_league.data_fetchers.shared.name_utils import normalize_name

    name, _, _ = split_external_draft_player(value)
    normalized = normalize_name(name)
    return _EXTERNAL_PLAYER_NAME_ALIASES.get(normalized, name)


def external_draft_player_match_key(value) -> str:
    """Return the normalized player key used for external draft identity joins."""
    from multi_league.data_fetchers.shared.name_utils import normalize_name

    return normalize_name(canonicalize_external_draft_player_name(value))


def clean_external_draft_player_fields(df):
    """Normalize uploaded draft player strings while preserving team/position hints."""
    if df is None or df.empty or "player" not in df.columns:
        return df

    import pandas as pd

    out = df.copy()
    if "position" not in out.columns:
        out["position"] = None
    if "nfl_team_api" not in out.columns:
        out["nfl_team_api"] = None

    for idx in out.index:
        player_name, team, position = split_external_draft_player(out.at[idx, "player"])
        if player_name:
            out.at[idx, "player"] = canonicalize_external_draft_player_name(player_name)
        if team:
            current_team = out.at[idx, "nfl_team_api"]
            if current_team is None or pd.isna(current_team) or str(current_team).strip() == "":
                out.at[idx, "nfl_team_api"] = team
        if position:
            current_position = out.at[idx, "position"]
            if current_position is None or pd.isna(current_position) or str(current_position).strip() == "":
                out.at[idx, "position"] = position

    return out


def normalize_draft_df(df, platform: str, league_id: str | None = None) -> pd.DataFrame:
    """Normalize a draft DataFrame to canonical schema."""
    import pandas as pd

    db_name_cols = [c for c in ["db_name"] if df is not None and c in df.columns]
    if df is None or df.empty:
        return pd.DataFrame(columns=db_name_cols + RAW_COLUMNS)

    def _blank_string_mask(series: pd.Series) -> pd.Series:
        text = series.astype("string").str.strip()
        return text.isna() | text.isin({"", "None", "nan", "<NA>"})

    def _coerce_keeper_flag(series: pd.Series) -> pd.Series:
        text = series.astype("string").str.strip().str.lower()
        text = text.replace(
            {
                "": "0",
                "none": "0",
                "nan": "0",
                "<na>": "0",
                "true": "1",
                "t": "1",
                "yes": "1",
                "y": "1",
                "false": "0",
                "f": "0",
                "no": "0",
                "n": "0",
            }
        )
        return pd.to_numeric(text, errors="coerce").fillna(0)

    df = df.copy()
    df["platform"] = platform
    if league_id:
        if "league_id" not in df.columns:
            df["league_id"] = league_id
        else:
            missing_league_id = _blank_string_mask(df["league_id"])
            df.loc[missing_league_id, "league_id"] = league_id

    platform_id_col = {
        "sleeper": "sleeper_player_id",
        "espn": "espn_player_id",
        "fleaflicker": "fleaflicker_player_id",
    }.get((platform or "").lower(), "yahoo_player_id")

    rename_map = {
        "player_id": platform_id_col,
        "yahoo_position": "position",
        "nfl_position": "position",
        "nfl_team": "nfl_team_api",
        "is_keeper_status": "is_keeper",
        "bid_amount": "cost",
    }
    for old, new in rename_map.items():
        if old not in df.columns:
            continue
        if new not in df.columns:
            df = df.rename(columns={old: new})
            continue
        missing_new = _blank_string_mask(df[new])
        if missing_new.any():
            df.loc[missing_new, new] = df.loc[missing_new, old]

    if "nfl_team" in df.columns and "nfl_team_api" not in df.columns:
        df = df.rename(columns={"nfl_team": "nfl_team_api"})

    for col in ["yahoo_player_id", "sleeper_player_id", "espn_player_id", "fleaflicker_player_id"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace(r"\.0$", "", regex=True).replace({"None": None, "nan": None, "": None})
            )

    if "position" not in df.columns:
        df["position"] = None
    df = clean_external_draft_player_fields(df)
    for source_col in ["yahoo_position", "nfl_position", "primary_position"]:
        if source_col not in df.columns:
            continue
        missing_position = _blank_string_mask(df["position"])
        if missing_position.any():
            df.loc[missing_position, "position"] = df.loc[missing_position, source_col]
    df["position"] = df["position"].astype("string").str.strip()
    df["position"] = df["position"].replace(
        {
            "": pd.NA,
            "None": pd.NA,
            "nan": pd.NA,
            "DST": "DEF",
            "D/ST": "DEF",
            "Defense": "DEF",
            "DEFENSE": "DEF",
        }
    )
    df["position"] = df["position"].str.upper()

    if "franchise_id" not in df.columns:
        df["franchise_id"] = None
    else:
        hidden_franchise = hidden_manager_guid_mask(df["franchise_id"])
        if hidden_franchise.any():
            df.loc[hidden_franchise, "franchise_id"] = None
    if "manager_guid" in df.columns:
        missing_franchise = _blank_string_mask(df["franchise_id"])
        valid_guid = ~hidden_manager_guid_mask(df["manager_guid"])
        fill_mask = missing_franchise & valid_guid
        df.loc[fill_mask, "franchise_id"] = (
            df.loc[fill_mask, "manager_guid"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .replace({"None": None, "nan": None, "": None})
        )

    keeper_flag = pd.Series(0, index=df.index, dtype="float64")
    if "is_keeper" in df.columns:
        keeper_flag = keeper_flag.combine(_coerce_keeper_flag(df["is_keeper"]), max)
    if "is_keeper_status" in df.columns:
        keeper_flag = keeper_flag.combine(_coerce_keeper_flag(df["is_keeper_status"]), max)
    if "is_keeper_cost" in df.columns:
        keeper_cost = pd.to_numeric(df["is_keeper_cost"], errors="coerce").fillna(0)
        keeper_flag = keeper_flag.combine((keeper_cost > 0).astype(int), max)

    if (platform or "").lower() == "yahoo" and {"year", "round"}.issubset(df.columns):
        round_num = pd.to_numeric(df["round"], errors="coerce")
        keeper_cost_signal = (
            pd.to_numeric(df["is_keeper_cost"], errors="coerce").fillna(0)
            if "is_keeper_cost" in df.columns
            else pd.Series(0, index=df.index, dtype="float64")
        )
        for _year, idx in df.groupby("year", dropna=False).groups.items():
            if not len(idx):
                continue
            full_draft = round_num.loc[idx].max(skipna=True) >= 10
            all_marked_keeper = (keeper_flag.loc[idx].fillna(0) > 0).all()
            no_cost_signal = (keeper_cost_signal.loc[idx].fillna(0) <= 0).all()
            if full_draft and all_marked_keeper and no_cost_signal:
                keeper_flag.loc[idx] = 0
    df["is_keeper"] = keeper_flag.astype("Int64")

    if "player" in df.columns:
        from multi_league.data_fetchers.shared.clean_names import normalize_def_player_name

        def_aliases = {"DEF", "DST", "D/ST", "DEFENSE"}
        for idx in df.index:
            player_name = str(df.at[idx, "player"]).strip()
            if not player_name or player_name.lower() in {"none", "nan"}:
                continue
            normalized, _ = normalize_def_player_name(player_name)
            position_value = str(df.at[idx, "position"]).strip().upper()
            if normalized and (position_value in def_aliases or position_value in {"", "<NA>"}):
                df.at[idx, "player"] = normalized
                df.at[idx, "position"] = "DEF"

    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    preserved_sql_cols = [c for c in ["NFL_player_id"] if c in df.columns]
    cols_to_drop = [c for c in SQL_COLUMNS if c in df.columns and c not in preserved_sql_cols]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    for col in ["year", "round", "pick", "pick_in_round", "draft_slot", "draft_slot_roster_id", "is_keeper"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "draft_id" in df.columns:
        missing_draft_id = _blank_string_mask(df["draft_id"])
        if missing_draft_id.any():
            default_league_id = league_id or "unknown_league"
            league_series = (
                df["league_id"].astype("string").str.strip()
                if "league_id" in df.columns
                else pd.Series(default_league_id, index=df.index, dtype="string")
            )
            league_series = league_series.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
            league_series = league_series.fillna(default_league_id)
            year_series = df["year"].astype("string").replace({"<NA>": "unknown_year"}).fillna("unknown_year")
            platform_prefix = (platform or "unknown").lower().strip() or "unknown"
            df.loc[missing_draft_id, "draft_id"] = (
                platform_prefix
                + "_"
                + league_series[missing_draft_id]
                + "_"
                + year_series[missing_draft_id]
                + "_default"
            )

    if "cost" in df.columns:
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    return df[db_name_cols + [c for c in RAW_COLUMNS if c in df.columns] + preserved_sql_cols]


def create_draft_table_sql(database_name: str) -> str:
    cols = [f'    "{name}" {dtype}' for name, dtype, _ in DRAFT_SCHEMA]
    cols[0] = '    "db_name" VARCHAR NOT NULL'
    return f"""
CREATE TABLE IF NOT EXISTS "{database_name}".public.draft (
{','.join(chr(10) + c for c in cols)}
)
"""


def upload_drafts(database_name: str, df, conn=None) -> bool:
    """Upload canonical draft DataFrame through the active DuckDB connection."""
    import logging

    logger = logging.getLogger(__name__)
    if df is None or df.empty:
        return False
    df = df.copy()
    if "db_name" not in df.columns:
        df["db_name"] = database_name

    close_conn = conn is None
    if conn is None:
        raise RuntimeError("conn is required - all pipeline work uses local DuckDB files.")

    try:
        conn.execute(f'CREATE DATABASE IF NOT EXISTS "{database_name}"')

        # Drop old-format table
        try:
            old_cols = {
                r[0]
                for r in conn.execute(
                    f"SELECT column_name FROM duckdb_columns() "
                    f"WHERE database_name = '{database_name}' AND table_name = 'draft'"
                ).fetchall()
            }
            if old_cols and ("nfl_team_api" not in old_cols or "auto_drafted" not in old_cols):
                conn.execute(f'DROP TABLE "{database_name}".public.draft')
                logger.info(f"[DRAFT] Dropped old-format draft table in {database_name}")
        except Exception:
            pass

        conn.execute(create_draft_table_sql(database_name))

        years = df["year"].dropna().unique().tolist()
        for yr in years:
            conn.execute(f'DELETE FROM "{database_name}".public.draft WHERE year = ?', [int(yr)])

        conn.register("_draft_upload", df)
        insert_cols = ["db_name"] + [c for c in RAW_COLUMNS if c in df.columns]
        col_list = ", ".join([f'"{c}"' for c in insert_cols])
        conn.execute(f'INSERT INTO "{database_name}".public.draft ({col_list}) SELECT {col_list} FROM _draft_upload')
        conn.unregister("_draft_upload")

        logger.info(f"[DRAFT] Uploaded {len(df)} rows to {database_name} ({len(years)} year(s))")
        return True
    except Exception as e:
        logger.error(f"[DRAFT] Upload failed: {e}")
        return False
    finally:
        if close_conn:
            conn.close()
