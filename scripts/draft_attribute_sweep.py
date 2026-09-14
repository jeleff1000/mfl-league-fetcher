#!/usr/bin/env python3
"""Unbiased manager-tell discovery: let the data rank the attributes.

Instead of hand-defining archetypes with arbitrary cutoffs, this sweeps EVERY
advanced attribute from the local v26 super table, percentile-normalizes it
within (position, season) — so no cutoffs, and a stat means the same thing
across positions/eras — and ranks the columns by how much a manager's OWN prior
picks predict where their next pick lands on that axis, OUT OF SAMPLE, versus a
shuffled-manager NULL. The null is the guard against "so many columns": with ~50
axes some will look predictive by chance, so a real tell must clear the null.

  feature value   = pick's prior-season stat, percentile within (position, year)
  persistence     = corr( manager_train_mean - league_mean , actual - league_mean )
                    on held-out years (train year < cutoff, test on/after)
  z_vs_null       = (persistence - shuffled_mean) / shuffled_sd  (manager labels
                    permuted K times); a tell must clear ~+2
  style_corr      = correlation with the production axis (prior-yr scrimmage),
                    to flag "this is just the proven-vs-upside style again"

Reads picks from the built candidate dataset; features from the local v26
weekly parquet (aggregated to prior season). No preconceived archetype list.

    python scripts/draft_attribute_sweep.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
import glob
import json
import os
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

# Curated to genuine PLAYER-TYPE / USAGE / EFFICIENCY / ADVANCED signals — not a
# preconceived archetype, just "exclude leaky outcomes (fantasy pts, ranks,
# lamar), ids and metadata." Everything here is prior-season, pre-draft-known.
FEATURES = [
    # NGS (2016+): athletic / separation / expectation
    "ngs_avg_separation", "ngs_avg_cushion", "ngs_avg_time_to_throw",
    "ngs_avg_time_to_los", "ngs_rush_yards_over_expected", "ngs_rush_pct_over_expected",
    "ngs_rush_efficiency", "ngs_avg_yac_above_expectation", "ngs_avg_expected_yac",
    "ngs_aggressiveness", "ngs_avg_air_yards_to_sticks", "ngs_pct_share_intended_air_yards",
    # usage / role
    "target_share", "air_yards_share", "offense_snap_pct", "carries", "receptions",
    "targets", "rushing_scrambles", "rz_carries", "rz_targets", "rz_pass_att",
    # efficiency / play-style
    "receiving_adot", "receiving_yards_per_target", "receiving_yards_per_reception",
    "rushing_yards_per_carry", "rushing_yards_before_contact", "rushing_yards_after_contact",
    "receiving_yards_after_catch", "rushing_broken_tackles", "receiving_broken_tackles",
    "passing_cpoe",
    # advanced value
    "passing_epa", "rushing_epa", "receiving_epa", "total_epa",
    "passing_wpa", "rushing_wpa", "receiving_wpa",
    "pass_success", "rush_success", "rec_success",
    "pass_explosive_20", "rush_explosive_10", "rec_explosive_20",
    # production (reference — expect these to top the board as the style axis)
    "receiving_yards", "rushing_yards", "receiving_tds", "rushing_tds",
    "receiving_first_downs", "rushing_first_downs",
]


def latest_v26_weekly() -> str:
    paths = sorted(glob.glob(
        r"D:\league-history-data\nfl\releases\*_v26\tables\nfl_player_stats_all.parquet"))
    if not paths:
        raise SystemExit("no v26 release found")
    return paths[-1]


def load_season_features(weekly_path: str) -> pd.DataFrame:
    con = duckdb.connect()
    have = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{weekly_path}')").fetchall()}
    feats = [f for f in FEATURES if f in have]
    missing = [f for f in FEATURES if f not in have]
    if missing:
        print(f"  (skipping {len(missing)} absent cols: {missing[:6]}...)", flush=True)
    aggs = ",\n".join(f"AVG({f}) AS {f}" for f in feats)
    sql = f"""
    SELECT NFL_player_id, year, ANY_VALUE(position) AS position,
           SUM(CASE WHEN COALESCE(offense_snaps,0) > 0 THEN 1 ELSE 0 END) AS games,
           {aggs}
    FROM read_parquet('{weekly_path}')
    WHERE NFL_player_id IS NOT NULL AND year IS NOT NULL
    GROUP BY NFL_player_id, year
    """
    df = con.execute(sql).fetchdf()
    con.close()
    return df, feats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True, help="draft_candidates.parquet")
    ap.add_argument("--test-from", type=int, default=2021)
    ap.add_argument("--null-iters", type=int, default=20)
    ap.add_argument("--min-test", type=int, default=1500)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    weekly = latest_v26_weekly()
    print(f"v26 weekly: {weekly}\naggregating season features...", flush=True)
    feat, feats = load_season_features(weekly)
    print(f"  {len(feat):,} player-seasons, {len(feats)} features", flush=True)

    picks = pd.read_parquet(args.data, columns=["chosen", "year", "manager_key",
                                                "cand_nflid", "cand_pos"])
    picks = picks[picks["chosen"] == 1].rename(columns={"cand_nflid": "NFL_player_id"})
    # pick in year Y reacts to the player's year Y-1 season
    feat = feat.rename(columns={"year": "season"})
    picks["season"] = picks["year"] - 1
    m = picks.merge(feat.drop(columns=["position"]), on=["NFL_player_id", "season"], how="left")
    print(f"  {len(m):,} picks joined to prior-season features", flush=True)

    # percentile-normalize each feature within (position, season) — no cutoffs
    for f in feats:
        m[f] = m.groupby(["cand_pos", "season"])[f].rank(pct=True)
    # production/style reference axis = scrimmage-yards percentile (rush+rec yds)
    ref = m[["receiving_yards", "rushing_yards"]].mean(axis=1)
    m["_style"] = ref

    train_mask = m["year"] < args.test_from
    rng = np.random.default_rng(0)
    rows = []
    for f in feats:
        d = m[["manager_key", "year", f, "_style"]].dropna(subset=[f])
        tr = d[d["year"] < args.test_from]
        te = d[d["year"] >= args.test_from]
        if len(te) < args.min_test or tr.empty:
            continue
        mgr_mean = tr.groupby("manager_key")[f].mean()
        league_mean = tr[f].mean()
        te = te[te["manager_key"].isin(mgr_mean.index)]
        if len(te) < args.min_test:
            continue
        pred = te["manager_key"].map(mgr_mean).to_numpy() - league_mean
        target = te[f].to_numpy() - league_mean
        if pred.std() < 1e-9:
            continue
        persistence = float(np.corrcoef(pred, target)[0, 1])
        # shuffle-null: permute which manager profile each manager gets
        nulls = []
        keys = mgr_mean.index.to_numpy()
        vals = mgr_mean.to_numpy()
        for _ in range(args.null_iters):
            perm = dict(zip(keys, rng.permutation(vals)))
            p = te["manager_key"].map(perm).to_numpy() - league_mean
            if p.std() < 1e-9:
                continue
            nulls.append(np.corrcoef(p, target)[0, 1])
        null_mu, null_sd = (float(np.mean(nulls)), float(np.std(nulls))) if nulls else (0.0, 1.0)
        z = (persistence - null_mu) / null_sd if null_sd > 1e-9 else 0.0
        style_corr = float(np.corrcoef(te[f].to_numpy(), te["_style"].dropna().reindex(te.index).fillna(0.5).to_numpy())[0, 1])
        rows.append({"feature": f, "n_test": int(len(te)), "persistence": round(persistence, 4),
                     "z_vs_null": round(z, 2), "style_corr": round(style_corr, 3)})

    lb = pd.DataFrame(rows).sort_values("z_vs_null", ascending=False).reset_index(drop=True)
    print("\n" + "=" * 78)
    print("ATTRIBUTE SWEEP — which prior-season axes does manager identity persist on?")
    print("(percentile-normalized within position/season; out-of-sample; vs shuffle-null)")
    print("=" * 78)
    print(f"{'feature':32} {'n_test':>7} {'persist':>8} {'z_null':>7} {'style_r':>8}")
    for _, r in lb.iterrows():
        star = "  <--" if r["z_vs_null"] >= 2 and abs(r["style_corr"]) < 0.5 else ""
        print(f"{r['feature']:32} {r['n_test']:>7,} {r['persistence']:>8.3f} "
              f"{r['z_vs_null']:>7.1f} {r['style_corr']:>8.3f}{star}")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    lb.to_json(out_dir / "draft_attribute_sweep.json", orient="records", indent=2)
    print(f"\n<-- = clears null (z>=2) AND not just the production axis (|style_r|<0.5)")
    print(f"wrote {out_dir / 'draft_attribute_sweep.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
