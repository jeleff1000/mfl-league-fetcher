"""
sota_recon/nflcom_targeted_logs.py -- targeted NFL.com game-log fetch for the pre-1978 fumbles_lost fill.

The main harvester (nflcom_harvest) crawls the full 21k-player universe sequentially; the fumbles_lost
fill only needs ~50 specific (player, season) log pages. This fetches EXACTLY those pages through the same
disk cache + parser, but writes to its own parquet and NEVER touches harvest_state.json (safe to run while
the main crawl is grinding; the main crawl later hits the warm cache for these URLs).

    python -m scripts.sota_recon.nflcom_targeted_logs            # fetch + write targeted parquet
    python -m scripts.sota_recon.nflcom_targeted_logs --show     # print the fetched log rows for the targets
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

from .nflcom_harvest import BASE, TABLES, http_get, parse_all_tables

SUPER = "D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables/nfl_player_stats_all.parquet"
OUT = TABLES / "player_logs_targeted" / "fumbles_pre1978.parquet"


def _slugify(name: str) -> str:
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    s = re.sub(r"[.']", "", s.lower())
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def build_targets() -> pd.DataFrame:
    """ALL pre-1978 player-weeks with fumbles>0 (the fumbles_lost audit population -- both the NULLs
    to fill AND the existing values to verify, which skew default-0), mapped to every era-matching
    NFL.com slug candidate (ambiguous names keep ALL candidates; disambiguate at fill time)."""
    con = duckdb.connect()
    tgt = con.execute(
        f"""SELECT NFL_player_id, player, CAST(year AS INT) AS season, CAST(week AS INT) AS wk, fumbles
            FROM read_parquet('{SUPER}')
            WHERE year < 1978 AND fumbles > 0
            ORDER BY season, player"""
    ).fetchdf()
    con.close()
    uni = json.loads((BASE / "player_universe.json").read_text())
    rows = []
    for _, r in tgt.iterrows():
        base = _slugify(r["player"])
        cands = [s for s in [base] + [f"{base}-{i}" for i in range(2, 6)]
                 if s in uni and uni[s] and min(uni[s]) <= r["season"] <= max(uni[s])]
        for slug in cands or [None]:
            rows.append({**r, "nflcom_slug": slug, "n_candidates": len(cands)})
    return pd.DataFrame(rows)


def fetch(targets: pd.DataFrame) -> pd.DataFrame:
    pages = targets.dropna(subset=["nflcom_slug"])[["nflcom_slug", "season"]].drop_duplicates()
    all_rows = []
    for i, (slug, season) in enumerate(pages.itertuples(index=False), 1):
        html = http_get(f"https://www.nfl.com/players/{slug}/stats/logs/{season}/")
        tabs = parse_all_tables(html or "")
        n = 0
        for t in tabs:
            for r in t["rows"]:
                r["_view"] = "logs"; r["_table"] = t["caption"]
                r["nflcom_slug"] = slug; r["season"] = season
                if "fum" in t["keys"] and "lost" in t["keys"]:
                    r["fumbles"] = r.get("fum"); r["fumbles_lost"] = r.get("lost")
                all_rows.append(r); n += 1
        print(f"  [{i}/{len(pages)}] {slug} {season}: {len(tabs)} tables, {n} rows")
    return pd.DataFrame(all_rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true")
    a = ap.parse_args()
    if a.show:
        con = duckdb.connect()
        print(con.execute(f"SELECT * FROM read_parquet('{OUT.as_posix()}') ORDER BY season, nflcom_slug, _table").fetchdf().to_string())
        raise SystemExit
    targets = build_targets()
    unmapped = targets[targets.nflcom_slug.isna()]
    if len(unmapped):
        print(f"UNMAPPED targets ({len(unmapped)}):\n{unmapped.to_string()}")
    df = fetch(targets)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.astype(str).to_parquet(OUT, index=False)
    print(f"wrote {len(df)} log rows -> {OUT}")
