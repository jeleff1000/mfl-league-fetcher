"""Analysis over the light extract.  Pure pandas on a small parquet -- seconds, no DB.

Variance comes from the sampled leagues; the finite-population correction uses the TRUE
population count, so sampling costs nothing in the final required-league number.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd

Z = NormalDist().inv_cdf(0.975)

# metric -> margins in the metric's own units (a count is never asked for +/-0.03 starts)
METRICS = {
    "start_pct":   ((0.01, 0.03, 0.05), "all"),
    "playoff_pct": ((0.01, 0.03, 0.05), "po"),
    "champ_pct":   ((0.01, 0.03, 0.05), "all"),
    "exp_starts":  ((0.25, 0.5, 1.0),   "all"),
    "clutch":      ((0.25, 0.5, 1.0),   "started"),
}


def req(var, L_sample, L_true, e):
    """Leagues needed for margin e: variance from the sample, FPC against the truth."""
    if not np.isfinite(var) or var < 0 or not L_true or L_true < 2:
        return None
    if var <= 0:
        return 2.0
    naive = Z * Z * var / (e * e)
    return naive / (1.0 + naive / L_true)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cells", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    cells = pd.read_parquet(a.cells)
    pop = pd.read_parquet(a.cells.with_suffix(".pop.parquet"))
    samp = pd.read_parquet(a.cells.with_suffix(".samp.parquet"))

    cells["start_pct"] = (cells.starts / cells.wks.replace(0, np.nan)).clip(upper=1.0)
    cells["champ_pct"] = cells.champ.astype(float)
    cells["playoff_pct"] = cells.po.astype(float)
    cells["exp_starts"] = cells.starts.astype(float)

    # sampled denominators per (cohort, position); absence from a league is a real zero
    den = samp.groupby(["cohort", "position"]).db_name.nunique().rename("L_samp").reset_index()
    den_po = (samp.merge(cells.loc[cells.po_sig == 1, ["db_name"]].drop_duplicates(), on="db_name")
                  .groupby(["cohort", "position"]).db_name.nunique().rename("L_samp_po").reset_index())
    pop = pop.merge(den, on=["cohort", "position"], how="left").merge(
        den_po, on=["cohort", "position"], how="left")

    rows = []
    for (cohort, position), leagues in samp.groupby(["cohort", "position"]):
        c = cells[cells.position == position].merge(leagues[["db_name"]], on="db_name")
        if c.empty:
            continue
        p = pop[(pop.cohort == cohort) & (pop.position == position)]
        if p.empty:
            continue
        p = p.iloc[0]
        for metric, (margins, policy) in METRICS.items():
            src = c[c.po_sig == 1] if policy == "po" else (c[c.starts > 0] if policy == "started" else c)
            if src.empty:
                continue
            L_s = p.L_samp_po if policy == "po" else p.L_samp
            L_t = p.population_leagues * (p.L_samp_po / p.L_samp if policy == "po" and p.L_samp else 1.0)
            g = src.groupby("player").agg(s=(metric, "sum"),
                                          ss=(metric, lambda v: float(np.square(v).sum())),
                                          present=(metric, "size"),
                                          rate=("start_pct", "mean")).reset_index()
            base = g.present.astype(float) if policy == "started" else float(L_s)
            g["mean"] = g.s / base
            g["var"] = ((g.ss - g.s ** 2 / base) / (np.asarray(base) - 1)).clip(lower=0)
            g = g[g["var"].notna() & g.rate.notna()]
            if g.empty:
                continue
            g["decile"] = g.rate.apply(lambda r: int(min(10, max(1, math.ceil(r * 10) if r > 0 else 1))) * 10)
            for e in margins:
                g[f"req_{e:g}"] = [req(v, L_s, L_t, e) for v in g["var"]]
            agg = g.groupby("decile", as_index=False).agg(
                players=("mean", "size"), mean_value=("mean", "median"),
                **{f"req_{e:g}": (f"req_{e:g}", lambda s: s.quantile(0.90)) for e in margins})
            agg["metric"], agg["cohort"], agg["position"] = metric, cohort, position
            agg["population_leagues"] = p.population_leagues
            agg["sampled_leagues"] = L_s
            rows.append(agg)

    out = pd.concat(rows, ignore_index=True)
    out.to_parquet(a.out, index=False)
    print(f"wrote {len(out):,} rows -> {a.out}\n")
    core = out[out.position.isin(["QB", "RB", "WR", "TE"]) & (out.cohort == "ALL")]
    for metric in METRICS:
        d = core[core.metric == metric]
        if d.empty:
            continue
        cols = [c for c in d.columns if c.startswith("req_")]
        t = d.pivot_table(index="decile", values=cols + ["mean_value", "players"], aggfunc="median")
        print(f"--- {metric} (cohort ALL, QB/RB/WR/TE, pop={int(d.population_leagues.iloc[0]):,}) ---")
        print(t.round(2).to_string(), "\n")


if __name__ == "__main__":
    main()
