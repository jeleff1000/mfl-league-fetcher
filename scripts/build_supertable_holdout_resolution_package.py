#!/usr/bin/env python3
"""Build a local package for the final PFR/supertable holdouts.

This script does not write to Fly. It turns the small manual-review tail into
explicit staged actions:

* old-era PFR context/stat overwrites
* post-1999 identity retargets and same-key context repairs
* James Stewart Vikings split inserts plus the contaminated live key to remove
* deferred recompute scopes for after raw atoms land
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_OUTPUT_ROOT = ORGANIZED_ROOT / "_catalog"
DEFAULT_AUDIT_DIR = DEFAULT_OUTPUT_ROOT / "pfr_boxscore_supertable_audit_zero_bridge_20260512T154117Z"


STAT_COLS = [
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "sack_yards_lost",
    "passing_long",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "rushing_long",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "receiving_long",
    "targets",
    "fumbles",
    "fumbles_lost",
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
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "kickoff_return_long",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "punt_return_long",
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "punts",
    "punt_yards",
    "punt_long",
]


OUTPUT_BASE_COLS = [
    "player_week",
    "NFL_player_id",
    "player",
    "position",
    "nfl_position",
    "nfl_team",
    "opponent_nfl_team",
    "nfl_franchise_number",
    "opponent_nfl_franchise_number",
    "year",
    "week",
    "season_type",
    "data_source",
    *STAT_COLS,
    "pfr_id",
    "pfr_id_norm",
    "bridge_classification",
    "source_tables",
    "boxscore_id",
    "game_date_min",
    "game_date_max",
    "stat_diff_count",
    "diff_cols",
    "holdout_action",
    "holdout_reason",
    "source_player_week",
    "source_NFL_player_id",
]


RETARGETS: dict[str, dict[str, str]] = {
    # 1999+ live rows already use these player_week roots, but several had the
    # NFL_player_id column contaminated by a same-name player.
    "browch25": {"target_id": "00-0021457", "player": "Chris Brown", "position": "DB"},
    "davich03": {"target_id": "00-0025794", "player": "Chris Davis", "position": "WR"},
    "willch06": {"target_id": "00-0026691", "player": "Chris Williams", "position": "WR"},
    "carttj00": {"target_id": "00-0037052", "player": "T.J. Carter", "position": "DB"},
    "martad00": {"target_id": "00-0038658", "player": "Adrian Martinez", "position": "QB"},
    "joneja16": {"target_id": "00-0040317", "player": "Jacoby Jones", "position": "WR"},
    "whitre20": {"target_id": "00-0020125", "player": "Reggie White", "position": "RB"},
    # The local PFR bio bridge had Jonah Williams' PFR IDs swapped.
    "willjo16": {"target_id": "00-0035944", "player": "Jonah Williams", "position": "DL"},
    "willjo10": {"target_id": "00-0035629", "player": "Jonah Williams", "position": "OT"},
}

IDENTITY_FIELD_ASSERT_PFR_IDS = {
    # These target player_week roots exist in live, but the NFL_player_id column
    # has been observed under a same-name identity. Some rows are stat no-ops, so
    # they need an explicit identity-field assertion outside the safe stat delta.
    "browch25",
    "davich03",
    "willch06",
    "whitre20",
}


SAME_KEY_POST_CONTEXT_REPAIRS = {
    "gibsda00": "PFR exact boxscore shows Jaguars context; live nflverse row is opponent-sided.",
    "mccrma20": "PFR exact boxscore shows Jaguars context; live nflverse row is opponent-sided.",
    "tayltr01": "PFR exact boxscore supplies the 2007 Rams row; live row is a stale Raiders context.",
}


DISTINCT_INSERT_PFR_IDS = {
    "johndo99": "Post-1999 PFR-only IDP rows for D.J. Johnson.",
    "browjo03": "Post-1999 PFR-only kicker row for Jonathan Brown.",
    "stewja20": "Distinct 1995 Vikings RB James Stewart split from 00-0015698.",
}


BIO_PFR_ID_REPAIRS = [
    {
        "NFL_player_id": "00-0035629",
        "player": "Jonah Williams",
        "position": "OT",
        "correct_pfr_id": "WillJo10",
        "old_pfr_id": "WillJo16",
        "reason": "PFR WillJo10 is the Bengals/Cardinals OT; WillJo16 is the Rams/Saints DL.",
    },
    {
        "NFL_player_id": "00-0035944",
        "player": "Jonah Williams",
        "position": "DL",
        "correct_pfr_id": "WillJo16",
        "old_pfr_id": "WillJo10",
        "reason": "PFR WillJo16 is the Rams/Saints DL; WillJo10 is the Bengals/Cardinals OT.",
    },
]


HEADSHOT_URL_REPAIRS = [
    {
        "NFL_player_id": "00-0009599",
        "player": "Fred Lane",
        "position": "RB",
        "correct_headshot_url": "https://www.sportscasting.com/wp-content/uploads/2020/07/Fred-Lane-pose-768x512.jpeg",
        "source": "sportscasting",
        "reason": "Replace placeholder NFL.com image with user-confirmed Fred Lane headshot.",
    },
    {
        "NFL_player_id": "00-0008300",
        "player": "Patrick Jeffers",
        "position": "WR",
        "correct_headshot_url": "https://www.statspros.com/wp-content/smush-webp/2023/11/Patrick-Jeffers-Stats.jpg.webp",
        "source": "statspros",
        "reason": "Replace placeholder NFL.com image with user-confirmed Patrick Jeffers headshot.",
    },
    {
        "NFL_player_id": "00-0011035",
        "player": "Freeman McNeil",
        "position": "RB",
        "correct_headshot_url": "https://i.pinimg.com/736x/4e/8a/47/4e8a47f102f60a21bb6ccb05a5169a7c.jpg",
        "source": "pinimg",
        "reason": "Replace stale Freeman McNeil headshot with user-confirmed URL.",
    },
]


DELETE_OR_REKEY_ROWS = [
    {
        "player_week": "00-0015698_1995_10",
        "NFL_player_id": "00-0015698",
        "player": "James Stewart",
        "year": 1995,
        "week": 10,
        "action": "delete_after_insert_or_rekey_to_StewJa20_1995_10",
        "reason": "Live row is Vikings James Stewart under the Jaguars/Lions James Stewart identity.",
    }
]


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def latest_reconciliation_dir() -> Path:
    dirs = sorted(
        DEFAULT_OUTPUT_ROOT.glob("pfr_weekly_reconciliation_*"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in dirs:
        if (path / "pfr_weekly_reconciliation.parquet").exists():
            return path
    raise FileNotFoundError("No pfr_weekly_reconciliation_* directory found")


def available_columns(path: Path) -> set[str]:
    return set(pq.read_schema(path).names)


def load_reconciliation(reconciliation_dir: Path) -> pd.DataFrame:
    path = reconciliation_dir / "pfr_weekly_reconciliation.parquet"
    cols = [
        "weekly_bucket",
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "year",
        "week",
        "season_type",
        "season_type_source",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "pfr_team_list",
        "pfr_opponent_list",
        "pfr_id",
        "pfr_id_norm",
        "bridge_classification",
        "source_tables",
        "boxscore_id",
        "game_date_min",
        "game_date_max",
        "live_player_week",
        "live_NFL_player_id",
        "live_player",
        "live_position",
        "live_nfl_team",
        "live_opponent_nfl_team",
        "live_data_source",
        "stat_diff_count",
        "diff_cols",
        *STAT_COLS,
    ]
    cols = [col for col in cols if col in available_columns(path)]
    rec = pd.read_parquet(path, columns=cols)
    rec["pfr_id_norm"] = rec["pfr_id_norm"].astype("string").fillna("").str.lower()
    rec["year"] = pd.to_numeric(rec["year"], errors="coerce").astype("Int64")
    rec["week"] = pd.to_numeric(rec["week"], errors="coerce").astype("Int64")
    for col in STAT_COLS:
        if col not in rec.columns:
            rec[col] = 0.0
        rec[col] = pd.to_numeric(rec[col], errors="coerce").fillna(0)
    return rec


def latest_franchise_history(audit_dir: Path) -> pd.DataFrame | None:
    path = audit_dir / "fly_nfl_franchise_history.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def normalize_abbrev(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    code = str(value).strip().upper()
    aliases = {
        "GNB": "GB",
        "KAN": "KC",
        "NWE": "NE",
        "NOR": "NO",
        "SFO": "SF",
        "TAM": "TB",
        "LVR": "LV",
        "RAI": "LV",
        "OAK": "LV",
        "CRD": "ARI",
        "PHO": "ARI",
        "RAM": "LA",
        "LAR": "LA",
        "SDG": "LAC",
        "NYT": "NYJ",
    }
    return aliases.get(code, code)


def enrich_franchise_numbers(rows: pd.DataFrame, hist: pd.DataFrame | None) -> pd.DataFrame:
    out = rows.copy()
    if hist is None or hist.empty:
        for col in ["nfl_franchise_number", "opponent_nfl_franchise_number"]:
            if col not in out.columns:
                out[col] = pd.NA
        return out

    hist = hist.copy()
    hist["abbrev_norm"] = hist["abbrev"].map(normalize_abbrev)
    hist["start_year"] = pd.to_numeric(hist["start_year"], errors="coerce").fillna(0).astype(int)
    hist["end_year"] = pd.to_numeric(hist["end_year"], errors="coerce").fillna(9999).astype(int)
    hist["is_canonical"] = hist["is_canonical"].fillna(False).astype(bool)
    hist = hist.sort_values(["is_canonical", "nfl_franchise_number"], ascending=[False, True])

    def lookup(team: Any, year: Any) -> int | None:
        code = normalize_abbrev(team)
        if not code or pd.isna(year):
            return None
        season = int(year)
        candidates = hist[hist["abbrev_norm"].eq(code) & hist["start_year"].le(season) & hist["end_year"].ge(season)]
        if candidates.empty:
            return None
        return int(candidates.iloc[0]["nfl_franchise_number"])

    out["nfl_franchise_number"] = [lookup(team, year) for team, year in zip(out["nfl_team"], out["year"], strict=False)]
    out["opponent_nfl_franchise_number"] = [
        lookup(team, year) for team, year in zip(out["opponent_nfl_team"], out["year"], strict=False)
    ]
    out["nfl_franchise_number"] = pd.to_numeric(out["nfl_franchise_number"], errors="coerce").astype("Int64")
    out["opponent_nfl_franchise_number"] = pd.to_numeric(out["opponent_nfl_franchise_number"], errors="coerce").astype(
        "Int64"
    )
    return out


def output_rows(rows: pd.DataFrame, hist: pd.DataFrame | None) -> pd.DataFrame:
    rows = rows.copy()
    if "source_player_week" not in rows.columns:
        rows["source_player_week"] = rows["player_week"]
    if "source_NFL_player_id" not in rows.columns:
        rows["source_NFL_player_id"] = rows["NFL_player_id"]
    rows = enrich_franchise_numbers(rows, hist)
    for col in OUTPUT_BASE_COLS:
        if col not in rows.columns:
            rows[col] = None
    return rows[OUTPUT_BASE_COLS].copy()


def build_context_overwrites(rec: pd.DataFrame, hist: pd.DataFrame | None) -> pd.DataFrame:
    pre_1999 = rec[
        rec["weekly_bucket"].eq("weekly_update_context_review")
        & rec["year"].between(1978, 1998)
        & pd.to_numeric(rec["stat_diff_count"], errors="coerce").fillna(0).gt(0)
    ].copy()
    pre_1999["holdout_action"] = "same_key_pfr_context_stat_overwrite"
    pre_1999["holdout_reason"] = (
        "PFR exact boxscore row corrects old supertable context/stat row at the same player_week key."
    )

    post_same_key = rec[
        rec["weekly_bucket"].eq("weekly_update_context_review") & rec["pfr_id_norm"].isin(SAME_KEY_POST_CONTEXT_REPAIRS)
    ].copy()
    post_same_key["holdout_action"] = "same_key_pfr_context_stat_overwrite"
    post_same_key["holdout_reason"] = post_same_key["pfr_id_norm"].map(SAME_KEY_POST_CONTEXT_REPAIRS)

    rows = pd.concat([pre_1999, post_same_key], ignore_index=True)
    rows["data_source"] = "pfr_boxscore_holdout_context_repair"
    return output_rows(rows, hist)


def target_player_week(target_id: str, year: Any, week: Any) -> str:
    return f"{target_id}_{int(year)}_{int(week)}"


def build_retarget_rows(rec: pd.DataFrame, hist: pd.DataFrame | None) -> pd.DataFrame:
    rows = rec[
        rec["pfr_id_norm"].isin(RETARGETS)
        & rec["weekly_bucket"].isin(["weekly_identity_manual_review", "weekly_update_context_review"])
    ].copy()
    if rows.empty:
        return output_rows(rows, hist)

    rows["source_player_week"] = rows["player_week"]
    rows["source_NFL_player_id"] = rows["NFL_player_id"]
    rows["target_NFL_player_id"] = rows["pfr_id_norm"].map(lambda value: RETARGETS[value]["target_id"])
    rows["player_week"] = [
        target_player_week(target_id, year, week)
        for target_id, year, week in zip(rows["target_NFL_player_id"], rows["year"], rows["week"], strict=False)
    ]
    rows["NFL_player_id"] = rows["target_NFL_player_id"]
    rows["player"] = rows["pfr_id_norm"].map(lambda value: RETARGETS[value]["player"])
    rows["position"] = rows["pfr_id_norm"].map(lambda value: RETARGETS[value]["position"])
    rows["nfl_position"] = rows["position"]
    rows["data_source"] = "pfr_boxscore_holdout_identity_retarget"
    rows["holdout_action"] = "retarget_existing_or_upsert"
    rows["holdout_reason"] = "PFR ID maps to a different canonical live player key than the current bridge selected."
    return output_rows(rows, hist)


def build_insert_rows(rec: pd.DataFrame, hist: pd.DataFrame | None) -> pd.DataFrame:
    rows = rec[
        rec["pfr_id_norm"].isin(DISTINCT_INSERT_PFR_IDS) & rec["weekly_bucket"].eq("weekly_identity_manual_review")
    ].copy()
    if rows.empty:
        return output_rows(rows, hist)
    rows["data_source"] = "pfr_boxscore_holdout_identity_insert"
    rows["holdout_action"] = "insert_missing_distinct_identity_row"
    rows["holdout_reason"] = rows["pfr_id_norm"].map(DISTINCT_INSERT_PFR_IDS)
    return output_rows(rows, hist)


def build_identity_field_repairs(rec: pd.DataFrame) -> pd.DataFrame:
    rows = rec[rec["pfr_id_norm"].isin(IDENTITY_FIELD_ASSERT_PFR_IDS)].copy()
    if rows.empty:
        return pd.DataFrame(
            columns=[
                "player_week",
                "correct_NFL_player_id",
                "player",
                "position",
                "year",
                "week",
                "reason",
            ]
        )

    rows["source_player_week"] = rows["player_week"]
    rows["source_NFL_player_id"] = rows["NFL_player_id"]
    rows["correct_NFL_player_id"] = rows["pfr_id_norm"].map(lambda value: RETARGETS[value]["target_id"])
    rows["player_week"] = [
        target_player_week(target_id, year, week)
        for target_id, year, week in zip(rows["correct_NFL_player_id"], rows["year"], rows["week"], strict=False)
    ]
    rows["player"] = rows["pfr_id_norm"].map(lambda value: RETARGETS[value]["player"])
    rows["position"] = rows["pfr_id_norm"].map(lambda value: RETARGETS[value]["position"])
    repairs = rows[
        [
            "player_week",
            "correct_NFL_player_id",
            "player",
            "position",
            "year",
            "week",
            "source_player_week",
            "source_NFL_player_id",
            "pfr_id",
        ]
    ].copy()
    repairs["reason"] = (
        "Assert canonical NFL_player_id after PFR retarget; several live rows had same-name contamination."
    )
    return repairs.drop_duplicates(["player_week", "correct_NFL_player_id"]).sort_values(
        ["year", "week", "player_week"]
    )


def build_headshot_url_repairs() -> pd.DataFrame:
    return pd.DataFrame(HEADSHOT_URL_REPAIRS)


def build_recompute_scope(*frames: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for frame in frames:
        if frame.empty:
            continue
        cols = [
            col for col in ["NFL_player_id", "player", "year", "week", "season_type", "holdout_action"] if col in frame
        ]
        parts.append(frame[cols].copy())
    if not parts:
        return pd.DataFrame(columns=["NFL_player_id", "player", "min_year", "max_year", "touched_weeks"])
    scope = pd.concat(parts, ignore_index=True)
    scope["year"] = pd.to_numeric(scope["year"], errors="coerce")
    scope = scope[scope["NFL_player_id"].notna() & scope["year"].notna()].copy()
    return (
        scope.groupby(["NFL_player_id", "player"], dropna=False)
        .agg(
            min_year=("year", "min"),
            max_year=("year", "max"),
            touched_weeks=("week", "nunique"),
            raw_actions=("holdout_action", lambda values: ";".join(sorted(set(map(str, values))))),
        )
        .reset_index()
        .sort_values(["min_year", "NFL_player_id"])
    )


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    text_df = df.copy().astype("string").fillna("")
    headers = list(text_df.columns)
    rows = text_df.values.tolist()
    widths = [max(len(str(header)), *(len(str(row[i])) for row in rows)) for i, header in enumerate(headers)]

    def fmt(values: list[Any]) -> str:
        return "| " + " | ".join(str(value).ljust(widths[i]) for i, value in enumerate(values)) + " |"

    return "\n".join(
        [
            fmt(headers),
            "| " + " | ".join("-" * width for width in widths) + " |",
            *[fmt(row) for row in rows],
        ]
    )


def sql_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_headshot_repair_sql(output_dir: Path, headshot_url_repairs: pd.DataFrame) -> None:
    lines = [
        "-- Local staging SQL for reviewed headshot URL repairs.",
        "-- Review connection/transaction handling before live execution.",
        "",
    ]
    for row in headshot_url_repairs.to_dict("records"):
        lines.extend(
            [
                f"-- {row['player']} ({row['NFL_player_id']})",
                "UPDATE ___ops.nfl_historical.nfl_player_stats_all",
                f"SET headshot_url = {sql_quote(row['correct_headshot_url'])}",
                f"WHERE NFL_player_id = {sql_quote(row['NFL_player_id'])};",
                "",
            ]
        )
    (output_dir / "pfr_holdout_headshot_url_repairs.sql").write_text(
        "\n".join(lines).rstrip() + "\n",
        encoding="utf-8",
    )


def write_outputs(
    output_dir: Path,
    reconciliation_dir: Path,
    context_overwrites: pd.DataFrame,
    retarget_rows: pd.DataFrame,
    insert_rows: pd.DataFrame,
    identity_field_repairs: pd.DataFrame,
    headshot_url_repairs: pd.DataFrame,
    recompute_scope: pd.DataFrame,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    context_overwrites.to_parquet(output_dir / "pfr_holdout_context_overwrite_rows.parquet", index=False)
    context_overwrites.head(200).to_csv(output_dir / "pfr_holdout_context_overwrite_rows_sample.csv", index=False)
    retarget_rows.to_parquet(output_dir / "pfr_holdout_identity_retarget_rows.parquet", index=False)
    retarget_rows.to_csv(output_dir / "pfr_holdout_identity_retarget_rows.csv", index=False)
    insert_rows.to_parquet(output_dir / "pfr_holdout_identity_insert_rows.parquet", index=False)
    insert_rows.to_csv(output_dir / "pfr_holdout_identity_insert_rows.csv", index=False)
    identity_field_repairs.to_csv(output_dir / "pfr_holdout_identity_field_repairs.csv", index=False)
    pd.DataFrame(BIO_PFR_ID_REPAIRS).to_csv(output_dir / "pfr_holdout_bio_pfr_id_repairs.csv", index=False)
    headshot_url_repairs.to_csv(output_dir / "pfr_holdout_headshot_url_repairs.csv", index=False)
    write_headshot_repair_sql(output_dir, headshot_url_repairs)
    pd.DataFrame(DELETE_OR_REKEY_ROWS).to_csv(output_dir / "pfr_holdout_delete_or_rekey_rows.csv", index=False)
    recompute_scope.to_csv(output_dir / "pfr_holdout_deferred_recompute_scope.csv", index=False)

    action_summary = pd.DataFrame(
        [
            {"action": "context_overwrite", "rows": len(context_overwrites)},
            {"action": "identity_retarget", "rows": len(retarget_rows)},
            {"action": "identity_insert", "rows": len(insert_rows)},
            {"action": "identity_field_assert", "rows": len(identity_field_repairs)},
            {"action": "bio_pfr_id_repair", "rows": len(BIO_PFR_ID_REPAIRS)},
            {"action": "headshot_url_repair", "rows": len(headshot_url_repairs)},
            {"action": "delete_or_rekey", "rows": len(DELETE_OR_REKEY_ROWS)},
            {"action": "deferred_recompute_players", "rows": len(recompute_scope)},
        ]
    )
    action_summary.to_csv(output_dir / "pfr_holdout_resolution_summary.csv", index=False)

    retarget_summary = (
        retarget_rows.groupby(["pfr_id", "NFL_player_id", "player", "position"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "pfr_id"], ascending=[False, True])
    )
    insert_summary = (
        insert_rows.groupby(["pfr_id", "NFL_player_id", "player", "position"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "pfr_id"], ascending=[False, True])
    )

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "reconciliation_dir": str(reconciliation_dir),
        "output_dir": str(output_dir),
        "context_overwrite_rows": int(len(context_overwrites)),
        "identity_retarget_rows": int(len(retarget_rows)),
        "identity_insert_rows": int(len(insert_rows)),
        "identity_field_repairs": int(len(identity_field_repairs)),
        "bio_pfr_id_repairs": int(len(BIO_PFR_ID_REPAIRS)),
        "headshot_url_repairs": int(len(headshot_url_repairs)),
        "delete_or_rekey_rows": int(len(DELETE_OR_REKEY_ROWS)),
        "deferred_recompute_players": int(len(recompute_scope)),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    markdown = [
        "# PFR Holdout Resolution Package",
        "",
        f"Created: {manifest['created_at_utc']}",
        f"Source reconciliation: `{reconciliation_dir}`",
        "",
        "Local-only staging package. No Fly writes were performed.",
        "",
        "## Action Counts",
        "",
        markdown_table(action_summary),
        "",
        "## Identity Retargets",
        "",
        markdown_table(retarget_summary),
        "",
        "## Distinct Inserts",
        "",
        markdown_table(insert_summary),
        "",
        "## Notes",
        "",
        "- The old-era context rows are staged as same-key PFR context/stat overwrites, not new duplicate player_week rows.",
        "- James Stewart `StewJa20` is staged as a distinct Vikings identity; `00-0015698_1995_10` is staged for delete/rekey after the insert lands.",
        "- Jonah Williams needs a bio PFR-ID swap before rebuilding the bridge: `00-0035629` -> `WillJo10`, `00-0035944` -> `WillJo16`.",
        "- Fred Lane and Patrick Jeffers have user-confirmed headshot URL repairs staged in `pfr_holdout_headshot_url_repairs.csv`.",
        "- Derived points, ranks, season aggregates, and career aggregates remain deferred until raw atom writes land.",
    ]
    (output_dir / "PFR_HOLDOUT_RESOLUTION_PACKAGE.md").write_text(
        "\n".join(markdown) + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reconciliation-dir", type=Path, default=latest_reconciliation_dir())
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reconciliation_dir = args.reconciliation_dir
    output_dir = args.output_dir or (DEFAULT_OUTPUT_ROOT / f"supertable_holdout_resolution_{now_stamp()}")
    rec = load_reconciliation(reconciliation_dir)
    hist = latest_franchise_history(args.audit_dir)

    context_overwrites = build_context_overwrites(rec, hist)
    retarget_rows = build_retarget_rows(rec, hist)
    insert_rows = build_insert_rows(rec, hist)
    identity_field_repairs = build_identity_field_repairs(rec)
    headshot_url_repairs = build_headshot_url_repairs()
    recompute_scope = build_recompute_scope(context_overwrites, retarget_rows, insert_rows)

    write_outputs(
        output_dir,
        reconciliation_dir,
        context_overwrites,
        retarget_rows,
        insert_rows,
        identity_field_repairs,
        headshot_url_repairs,
        recompute_scope,
    )

    print(f"output_dir={output_dir}")
    print(f"context_overwrite_rows={len(context_overwrites):,}")
    print(f"identity_retarget_rows={len(retarget_rows):,}")
    print(f"identity_insert_rows={len(insert_rows):,}")
    print(f"identity_field_repairs={len(identity_field_repairs):,}")
    print(f"headshot_url_repairs={len(headshot_url_repairs):,}")
    print(f"deferred_recompute_players={len(recompute_scope):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
