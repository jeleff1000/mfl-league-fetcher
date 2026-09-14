#!/usr/bin/env python3
"""Build draft-safe prior NFL award features from local PFR context parquet.

The live player_bio allpro/probowls/hof fields are career totals. They are
useful for research, but not safe for draft intelligence because a 2016 draft
cannot know a player's 2020 Pro Bowl. This script converts season-stamped PFR
All-Pro, Pro Bowl, and awards-voting tables into prior counts by draft year.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone, UTC
import json
from pathlib import Path
import re

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONTEXT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\pfr_context")
DEFAULT_BIO = ROOT / "ops_data" / "nfl_historical" / "player_bio.parquet"
DEFAULT_OUT = ROOT / "artifacts" / "player_prior_awards_by_year.parquet"

AWARD_VOTE_DIRS = [
    "voting_apmvp",
    "voting_apopoy",
    "voting_apdpoy",
    "voting_apaoroy",
    "voting_apadroy",
    "voting_aparoy",
    "voting_apdroy",
    "voting_aporoy",
    "voting_aproy",
    "voting_appoy",
    "voting_apcpoy",
    "voting_apaflcoy",
    "voting_apcoy",
    "voting_upimvp",
    "voting_upipoy",
    "voting_upiaroy",
    "voting_upiroy",
    "voting_upiaflcoy",
]


def parquet_files(root: Path, table: str) -> list[Path]:
    table_dir = root / "tables" / table
    if not table_dir.exists():
        return []
    return sorted(path for path in table_dir.rglob("*.parquet") if path.name != "compact_manifest.parquet")


def read_table(root: Path, table: str, columns: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in parquet_files(root, table):
        try:
            available = set(pd.read_parquet(path).columns)
            read_cols = [col for col in columns if col in available]
            if not {"year", "player_link_ids"}.issubset(read_cols):
                continue
            frames.append(pd.read_parquet(path, columns=read_cols))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def extract_pfr_id(value: object, urls: object = None) -> str | None:
    text = "" if value is None else str(value)
    for token in re.split(r"[\s,;|]+", text):
        token = token.strip().strip('"').strip("'")
        if re.match(r"^[A-Za-z][A-Za-z0-9.]{5,}$", token):
            return token
    url_text = "" if urls is None else str(urls)
    match = re.search(r"/players/[A-Z]/([^/]+)\.htm", url_text)
    return match.group(1) if match else None


def clean_year(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def build_events(context_root: Path) -> pd.DataFrame:
    events: list[pd.DataFrame] = []

    all_pro = read_table(context_root, "all_pro", ["year", "player", "player_link_ids", "player_urls"])
    if not all_pro.empty:
        all_pro["pfr_id"] = [
            extract_pfr_id(ids, urls) for ids, urls in zip(all_pro.get("player_link_ids"), all_pro.get("player_urls"))
        ]
        all_pro["award_year"] = clean_year(all_pro["year"])
        all_pro["allpro_season"] = 1
        events.append(all_pro[["pfr_id", "award_year", "allpro_season"]].dropna())

    pro_bowl = read_table(context_root, "pro_bowl", ["year", "player", "player_link_ids", "player_urls"])
    if not pro_bowl.empty:
        pro_bowl["pfr_id"] = [
            extract_pfr_id(ids, urls) for ids, urls in zip(pro_bowl.get("player_link_ids"), pro_bowl.get("player_urls"))
        ]
        pro_bowl["award_year"] = clean_year(pro_bowl["year"])
        pro_bowl["probowl_season"] = 1
        events.append(pro_bowl[["pfr_id", "award_year", "probowl_season"]].dropna())

    vote_frames: list[pd.DataFrame] = []
    for table in AWARD_VOTE_DIRS:
        votes = read_table(
            context_root, table, ["year", "ranker", "player", "player_link_ids", "player_urls", "votes", "share"]
        )
        if votes.empty:
            continue
        votes["pfr_id"] = [
            extract_pfr_id(ids, urls) for ids, urls in zip(votes.get("player_link_ids"), votes.get("player_urls"))
        ]
        votes["award_year"] = clean_year(votes["year"])
        votes["award_vote_mention"] = 1
        votes["major_award_win"] = pd.to_numeric(votes.get("ranker"), errors="coerce").eq(1).astype(int)
        vote_frames.append(votes[["pfr_id", "award_year", "award_vote_mention", "major_award_win"]].dropna())

    if vote_frames:
        events.append(pd.concat(vote_frames, ignore_index=True))

    if not events:
        return pd.DataFrame(columns=["pfr_id", "award_year"])

    frame = pd.concat(events, ignore_index=True).fillna(0)
    for col in ["allpro_season", "probowl_season", "award_vote_mention", "major_award_win"]:
        if col not in frame.columns:
            frame[col] = 0
    frame["pfr_id_norm"] = frame["pfr_id"].astype("string").str.lower()
    return (
        frame.groupby(["pfr_id_norm", "award_year"], dropna=False)
        .agg(
            pfr_id=("pfr_id", "first"),
            allpro_seasons=("allpro_season", "max"),
            probowl_seasons=("probowl_season", "max"),
            award_vote_mentions=("award_vote_mention", "sum"),
            major_award_wins=("major_award_win", "sum"),
        )
        .reset_index()
    )


def bio_bridge(player_bio: Path) -> pd.DataFrame:
    bio = pd.read_parquet(player_bio, columns=["NFL_player_id", "pfr_id"])
    bio = bio.dropna(subset=["NFL_player_id"]).copy()
    bio["pfr_id"] = bio["pfr_id"].fillna("")
    missing_pfr = bio["pfr_id"].eq("") & bio["NFL_player_id"].astype(str).str.match(r"^[A-Za-z][A-Za-z0-9.]{5,}$")
    bio.loc[missing_pfr, "pfr_id"] = bio.loc[missing_pfr, "NFL_player_id"].astype(str)
    bio = bio[bio["pfr_id"].astype(str).str.strip().ne("")].copy()
    bio["pfr_id_norm"] = bio["pfr_id"].astype("string").str.lower()
    return bio[["pfr_id_norm", "pfr_id", "NFL_player_id"]].drop_duplicates()


def cumulative_prior(events: pd.DataFrame, bridge: pd.DataFrame, max_draft_year: int) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    award_cols = [
        "allpro_seasons",
        "probowl_seasons",
        "award_vote_mentions",
        "major_award_wins",
    ]
    rows: list[dict[str, object]] = []
    for pfr_id_norm, grp in events.sort_values("award_year").groupby("pfr_id_norm", dropna=False):
        first_year = int(grp["award_year"].min())
        totals = {
            "prior_allpro_seasons": 0,
            "prior_probowl_seasons": 0,
            "prior_award_vote_mentions": 0,
            "prior_major_award_wins": 0,
        }
        by_year = {int(year): year_grp for year, year_grp in grp.groupby("award_year")}
        for draft_year in range(first_year + 1, max_draft_year + 1):
            award_year = draft_year - 1
            prev = {
                "prev_allpro_seasons": 0,
                "prev_probowl_seasons": 0,
                "prev_award_vote_mentions": 0,
                "prev_major_award_wins": 0,
            }
            if award_year in by_year:
                year_grp = by_year[award_year]
                sums = {col: int(year_grp[col].sum()) if col in year_grp.columns else 0 for col in award_cols}
                prev["prev_allpro_seasons"] = sums["allpro_seasons"]
                prev["prev_probowl_seasons"] = sums["probowl_seasons"]
                prev["prev_award_vote_mentions"] = sums["award_vote_mentions"]
                prev["prev_major_award_wins"] = sums["major_award_wins"]
                totals["prior_allpro_seasons"] += prev["prev_allpro_seasons"]
                totals["prior_probowl_seasons"] += prev["prev_probowl_seasons"]
                totals["prior_award_vote_mentions"] += prev["prev_award_vote_mentions"]
                totals["prior_major_award_wins"] += prev["prev_major_award_wins"]
            if sum(totals.values()) <= 0:
                continue
            rows.append(
                {
                    "pfr_id_norm": pfr_id_norm,
                    "draft_year": draft_year,
                    **prev,
                    **totals,
                }
            )

    prior = pd.DataFrame(rows)
    if prior.empty:
        return prior
    out = prior.merge(bridge, on="pfr_id_norm", how="inner")
    out["prior_award_reputation"] = (
        out["prior_allpro_seasons"] + out["prior_probowl_seasons"] + out["prior_major_award_wins"]
    )
    return out[
        [
            "NFL_player_id",
            "pfr_id",
            "draft_year",
            "prev_allpro_seasons",
            "prev_probowl_seasons",
            "prev_major_award_wins",
            "prev_award_vote_mentions",
            "prior_allpro_seasons",
            "prior_probowl_seasons",
            "prior_major_award_wins",
            "prior_award_vote_mentions",
            "prior_award_reputation",
        ]
    ].sort_values(["NFL_player_id", "draft_year"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Build draft-safe prior player award features.")
    parser.add_argument("--pfr-context-root", type=Path, default=DEFAULT_CONTEXT)
    parser.add_argument("--player-bio", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--max-draft-year", type=int, default=datetime.now(UTC).year)
    args = parser.parse_args()

    events = build_events(args.pfr_context_root)
    bridge = bio_bridge(args.player_bio)
    out = cumulative_prior(events, bridge, args.max_draft_year)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, index=False)

    summary = {
        "rows": int(len(out)),
        "players": int(out["NFL_player_id"].nunique()) if not out.empty else 0,
        "min_draft_year": int(out["draft_year"].min()) if not out.empty else None,
        "max_draft_year": int(out["draft_year"].max()) if not out.empty else None,
        "source_award_rows": int(len(events)),
        "output": str(args.out),
    }
    args.out.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
