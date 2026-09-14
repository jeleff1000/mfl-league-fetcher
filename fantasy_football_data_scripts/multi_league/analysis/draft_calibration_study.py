#!/usr/bin/env python3
"""
Draft Value Calibration Study (Phase 0a of the draft optimizer roadmap).

Walk-forward evaluation of the optimizer's value model against live fleet
data (centralized ___leagues database on Fly): for each league-year, fit
per-position cost/pick -> realized manager LAMAR curves on PRIOR years only
(mirroring the serving route's isotonic + recency weighting), predict the
target year, and score calibration + rank accuracy.

Variants under test (each maps to a measured baseline defect):
  baseline    - current serving behavior (raw axis, no floor, no shrink)
  floor       - floor predictions at the format's empirical late-pick mean
                (fixes: bottom decile predicted negative, realized +5..+8)
  shrink      - tail-stabilized isotonic: terminal blocks merged inward until
                they reach a minimum effective weight, so the extreme ends of
                the curve are never set by a couple of recent outliers
                (fixes: snake top decile +17 overshoot)
  percentile  - snake curves fit on board percentile instead of raw pick
                (league-size invariance; prerequisite for pooling)
  combined    - floor + shrink + percentile
  pooled_all  - combined blended with fleet format-cluster curves
                (superflex/IDP-conditional, temporal walk-forward pooling)

Populations:
  A - league-years with >=min-train-years of same-format local history
      (all variants comparable head-to-head)
  B - league-years with NO usable local history (the 56% thin fleet):
      scored with pure pooled curves vs the raw-capital baseline

Usage:
    python -m multi_league.analysis.draft_calibration_study
    python -m multi_league.analysis.draft_calibration_study --db the_league
    python -m multi_league.analysis.draft_calibration_study --export picks.csv
"""

import argparse
import sys

import numpy as np
import pandas as pd

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.db_reader import get_reader  # noqa: E402
from multi_league.core.logging_config import get_logger  # noqa: E402

logger = get_logger(__name__)

# Mirror the serving route: recency downweighting and isotonic minimums.
RECENCY_DECAY = 0.7
POOLED_RECENCY_DECAY = 0.8
MIN_ISOTONIC_POINTS = 15
TAIL_MIN_WEIGHT = 3.0  # terminal isotonic blocks must carry this much weight
POOLED_BLEND_FULL_TRUST = 40.0
STREAMING_POSITIONS = {"K", "DEF"}
KEEPER_COLUMNS = ("is_keeper", "is_keeper_status", "is_keeper_cost")
IDP_POSITIONS = {
    "LB", "DL", "DB", "IDP", "DB/LB", "DL/LB", "S", "CB", "DE", "DT",
    "EDGE", "ILB", "OLB", "MLB", "SS", "FS", "NT",
}

VARIANTS = ["baseline", "floor", "shrink", "percentile", "combined", "pooled_all"]

QA_LEAGUES = [
    "the_league",
    "nyu_ffl",
    "tfl_of_extraordinary_gentleman",
    "keeper_league",
    "zootown_dynasty",
    "degenerate_gamblers_football_league",
    "l_14_big_booms",
    "the_pigskin_platoon",
    "rock_hill_fantasy_league",
    "champions_branch_out",
]


# ---------------------------------------------------------------------------
# Data access — centralized ___leagues database on Fly
# (per-league catalogs are not cross-queryable on the Fly query server;
#  the merged public.draft / public.league_settings tables carry db_name)
# ---------------------------------------------------------------------------

CENTRAL_DB = "___leagues"
LEAGUE_BATCH_SIZE = 150


def quote_sql(value):
    return "'" + str(value).replace("'", "''") + "'"


def get_central_columns(reader, table):
    desc = reader.query_df(f"DESCRIBE public.{table}", database=CENTRAL_DB)
    name_col = "column_name" if "column_name" in desc.columns else desc.columns[0]
    return set(desc[name_col].astype(str).tolist())


def discover_league_databases(reader):
    """Leagues in the centralized draft table with manager_lamar outcomes."""
    df = reader.query_df(
        """
        SELECT DISTINCT db_name
        FROM public.draft
        WHERE manager_lamar IS NOT NULL AND year IS NOT NULL
        """,
        database=CENTRAL_DB,
    )
    valid = sorted(str(name) for name in df["db_name"].dropna().tolist())
    logger.info(f"Discovered {len(valid)} leagues with draft LAMAR data in {CENTRAL_DB}")
    return valid


def extract_fleet_draft(reader, db_names):
    """Pull non-keeper draft picks with realized LAMAR for the given leagues."""
    cols = get_central_columns(reader, "draft")
    keeper_col = next((c for c in KEEPER_COLUMNS if c in cols), None)
    keeper_filter = f"AND COALESCE({keeper_col}, 0) = 0" if keeper_col else ""
    select_parts = [
        "db_name",
        "year",
        "position",
        "manager_lamar",
        "cost" if "cost" in cols else "NULL AS cost",
        "pick" if "pick" in cols else "NULL AS pick",
        "round" if "round" in cols else "NULL AS round",
        "draft_type" if "draft_type" in cols else "NULL AS draft_type",
        "manager" if "manager" in cols else "NULL AS manager",
    ]

    frames = []
    for start in range(0, len(db_names), LEAGUE_BATCH_SIZE):
        batch = db_names[start : start + LEAGUE_BATCH_SIZE]
        in_list = ", ".join(quote_sql(name) for name in batch)
        sql = f"""
        SELECT {", ".join(select_parts)}
        FROM public.draft
        WHERE db_name IN ({in_list})
          AND year IS NOT NULL
          AND manager_lamar IS NOT NULL
          {keeper_filter}
        """
        try:
            frames.append(reader.query_df(sql, database=CENTRAL_DB))
        except Exception as e:
            logger.warning(f"Draft batch starting at {start} failed: {e}")

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df
    df["position"] = df["position"].map(normalize_position)
    df = df[df["position"] != ""].copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df["manager_lamar"] = pd.to_numeric(df["manager_lamar"], errors="coerce")
    df = df.dropna(subset=["manager_lamar"])
    return df


def extract_fleet_sizes(reader):
    """{db_name: {year: num_teams}} from centralized league_settings."""
    try:
        df = reader.query_df(
            "SELECT db_name, year, num_teams FROM public.league_settings",
            database=CENTRAL_DB,
        )
    except Exception as e:
        logger.warning(f"league_settings pull failed: {e}")
        return {}
    sizes = {}
    for _, row in df.dropna(subset=["db_name", "year"]).iterrows():
        sizes.setdefault(str(row["db_name"]), {})[int(row["year"])] = (
            float(row["num_teams"]) if pd.notna(row["num_teams"]) else np.nan
        )
    return sizes


def extract_keeper_leagues(reader):
    """Leagues where any draft pick is flagged as a keeper."""
    try:
        df = reader.query_df(
            "SELECT DISTINCT db_name FROM public.draft WHERE COALESCE(is_keeper, 0) = 1",
            database=CENTRAL_DB,
        )
        return set(str(name) for name in df["db_name"].dropna().tolist())
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# League typing (data-driven flags for cluster keys + segment reporting)
# ---------------------------------------------------------------------------


def normalize_position(raw):
    pos = str(raw or "").strip().upper()
    if pos in ("D/ST", "DST", "D"):
        return "DEF"
    # Multi-eligibility labels ("LB,DE", "WR/RB"): first part, v1 simplification.
    for sep in (",", "|", "/"):
        if sep in pos and pos not in ("DB/LB", "DL/LB"):
            pos = pos.split(sep)[0].strip()
    return pos


def detect_year_format(year_df):
    """Auction vs snake for one league-year (matches CLAUDE.md rule)."""
    draft_types = (
        year_df["draft_type"].dropna().astype(str).str.lower().unique().tolist()
        if "draft_type" in year_df.columns
        else []
    )
    if any(t in ("live", "auction", "offline") for t in draft_types):
        return "auction"
    costs = pd.to_numeric(year_df["cost"], errors="coerce").fillna(0)
    if len(year_df) > 0 and (costs > 0).mean() >= 0.25:
        return "auction"
    return "snake"


def build_league_flags(fleet_df):
    """Data-driven per-league flags: superflex proxy, IDP presence."""
    flags = {}
    for db_name, league in fleet_df.groupby("db_name"):
        with_mgr = league.dropna(subset=["manager"])
        manager_years = with_mgr.groupby(["year", "manager"]).ngroups
        qb_picks = int((with_mgr["position"] == "QB").sum())
        # 2.0 QB picks per manager-year: 1QB leagues drafting backup QBs sit
        # around 1.3-1.8; true superflex/2QB leagues sit at 2.0+.
        superflex = manager_years > 0 and qb_picks / manager_years >= 2.0
        idp_share = float(league["position"].isin(IDP_POSITIONS).mean())
        flags[db_name] = {"superflex": bool(superflex), "idp": idp_share > 0.02}
    return flags


def history_bucket(n_years):
    if n_years <= 2:
        return "history 1-2y"
    if n_years <= 4:
        return "history 3-4y"
    if n_years <= 7:
        return "history 5-7y"
    return "history 8y+"


def size_bucket(num_teams):
    if pd.isna(num_teams):
        return "unknown"
    if num_teams <= 10:
        return "small (<=10)"
    if num_teams <= 12:
        return "medium (10-12)"
    return "large (12+)"


# ---------------------------------------------------------------------------
# Curve fitting (mirrors frontend/src/lib/isotonic.ts semantics + variants)
# ---------------------------------------------------------------------------


def _pav_blocks(sy, sw):
    """Pool adjacent violators over pre-sorted values. Returns block arrays."""
    sum_wy, sum_w, rights = [], [], []
    for i in range(len(sy)):
        sum_wy.append(sw[i] * sy[i])
        sum_w.append(sw[i])
        rights.append(i)
        while len(sum_w) >= 2 and sum_wy[-2] / sum_w[-2] > sum_wy[-1] / sum_w[-1]:
            wy, w, r = sum_wy.pop(), sum_w.pop(), rights.pop()
            sum_wy[-1] += wy
            sum_w[-1] += w
            rights[-1] = r
    return sum_wy, sum_w, rights


class StepCurve:
    """Monotone step function y(x) with optional floor. Vectorized predict."""

    __slots__ = ("step_x", "step_y", "floor")

    def __init__(self, step_x, step_y, floor=None):
        self.step_x = np.asarray(step_x, dtype=float)
        self.step_y = np.asarray(step_y, dtype=float)
        self.floor = floor

    def predict(self, xs):
        xs = np.asarray(xs, dtype=float)
        if len(self.step_x) == 0:
            out = np.zeros(len(xs))
        else:
            idx = np.clip(
                np.searchsorted(self.step_x, xs, side="right") - 1,
                0,
                len(self.step_y) - 1,
            )
            out = self.step_y[idx]
        if self.floor is not None:
            out = np.maximum(out, self.floor)
        return out

    def with_floor(self, floor):
        return StepCurve(self.step_x, self.step_y, floor)


def fit_curve(xs, ys, ws, increasing, tail_min_weight=0.0):
    """Weighted PAV isotonic regression -> StepCurve.

    tail_min_weight > 0 merges terminal blocks inward until the first and
    last block each carry at least that much effective weight. The interior
    of the curve is untouched and monotonicity is preserved (a merged tail
    mean always stays on the correct side of its interior neighbor), so rank
    order between picks cannot change — only unstable tail levels contract.
    """
    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)
    ws = np.asarray(ws, dtype=float)
    mask = ws > 0
    xs, ys, ws = xs[mask], ys[mask], ws[mask]
    if len(xs) == 0:
        return StepCurve([], [])

    order = np.argsort(xs, kind="stable")
    sx, sy, sw = xs[order], ys[order], ws[order]
    if not increasing:
        sy = -sy

    sum_wy, sum_w, rights = _pav_blocks(sy, sw)

    if tail_min_weight > 0:
        # Top tail: merge the last block into its neighbor until credible.
        while len(sum_w) >= 2 and sum_w[-1] < tail_min_weight:
            sum_wy[-2] += sum_wy[-1]
            sum_w[-2] += sum_w[-1]
            rights[-2] = rights[-1]
            sum_wy.pop(), sum_w.pop(), rights.pop()
        # Bottom tail: merge the first block into its neighbor.
        while len(sum_w) >= 2 and sum_w[0] < tail_min_weight:
            sum_wy[1] += sum_wy[0]
            sum_w[1] += sum_w[0]
            del sum_wy[0], sum_w[0], rights[0]

    means = [wy / w for wy, w in zip(sum_wy, sum_w)]
    block_rights = rights

    step_x, step_y = [], []
    left = 0
    for mean, right in zip(means, block_rights):
        step_x.append(sx[left])
        step_y.append(mean if increasing else -mean)
        left = right + 1

    return StepCurve(step_x, step_y)


def fit_mean_curve(ys, ws):
    """Fallback: recency-weighted position mean as a constant curve."""
    ws = np.asarray(ws, dtype=float)
    ys = np.asarray(ys, dtype=float)
    if ws.sum() <= 0:
        return None
    return StepCurve([0.0], [float(np.average(ys, weights=ws))])


def fit_binned_curve(x, y, w, increasing, tail_min_weight):
    """Bin x to integer units (x is on a 0-100 normalized scale), then fit —
    keeps pooled fits fast on hundreds of thousands of picks."""
    df = pd.DataFrame({"xb": np.round(np.asarray(x, dtype=float)), "y": y, "w": w})
    df["wy"] = df["y"] * df["w"]
    g = df.groupby("xb")[["wy", "w"]].sum()
    g = g[g["w"] > 0]
    if g.empty:
        return None
    return fit_curve(
        g.index.to_numpy(), (g["wy"] / g["w"]).to_numpy(), g["w"].to_numpy(),
        increasing, tail_min_weight,
    )


# ---------------------------------------------------------------------------
# Fleet-pooled curves (percentile/budget-share axis, format-cluster keyed)
# ---------------------------------------------------------------------------


def normalize_fleet_axes(fleet_df, sizes):
    """Attach format, x_norm (0-100), and cluster keys to every pick."""
    frames = []
    for (db_name, year), grp in fleet_df.groupby(["db_name", "year"]):
        fmt = detect_year_format(grp)
        g = grp.copy()
        g["fmt"] = fmt
        # Per-league-year LAMAR magnitude: leagues score LAMAR on different
        # scales (scoring settings, depth), so pooled curves are fit on
        # scale-free values and rescaled per league at predict time.
        core = g[~g["position"].isin(STREAMING_POSITIONS)]
        scale = float(core["manager_lamar"].abs().mean()) if len(core) else np.nan
        g["y_scale"] = max(scale, 1e-6) if pd.notna(scale) else np.nan
        if fmt == "auction":
            cost = pd.to_numeric(g["cost"], errors="coerce")
            g["x_raw"] = cost
            g = g[g["x_raw"] >= 1]
            n_teams = sizes.get(db_name, {}).get(int(year), np.nan)
            if pd.isna(n_teams):
                n_teams = g["manager"].nunique() or 12
            per_team = g["x_raw"].sum() / max(float(n_teams), 1.0)
            g["x_norm"] = 100.0 * g["x_raw"] / max(per_team, 1.0)
        else:
            pick = pd.to_numeric(g["pick"], errors="coerce")
            if pick.isna().all() and "round" in g.columns:
                pick = pd.to_numeric(g["round"], errors="coerce")
            g["x_raw"] = pick
            g = g.dropna(subset=["x_raw"])
            max_pick = g["x_raw"].max()
            g["x_norm"] = 100.0 * g["x_raw"] / max(float(max_pick), 1.0)
        frames.append(g)
    out = pd.concat(frames, ignore_index=True)
    return out


def cluster_keys(flags):
    """Cluster lookup chain, most-specific first."""
    sf = "SF1" if flags.get("superflex") else "SF0"
    idp = "IDP1" if flags.get("idp") else "IDP0"
    return [f"{sf}_{idp}", sf, "ALL"]


def build_pooled_curves(norm_df, league_flags):
    """Fleet curves in scale-free LAMAR units, keyed by format cluster.

    Returns (curves, floors, median_scales):
      curves[(fmt, cluster, target_year, position)] -> StepCurve (y in
        scale-free units; multiply by a league scale at predict time)
      floors[(fmt, position, target_year)] -> scale-free late-pick floor
      median_scales[(fmt, target_year)] -> fleet median league-year scale
        (the rescale factor of last resort for leagues with no history)

    For each target year, trains only on picks from strictly earlier years
    (temporal walk-forward — no outcome leakage from the scored season).
    """
    work = norm_df[~norm_df["position"].isin(STREAMING_POSITIONS)].copy()
    work = work.dropna(subset=["y_scale"])
    work["y"] = work["manager_lamar"].astype(float) / work["y_scale"]
    sf = work["db_name"].map(lambda d: league_flags.get(d, {}).get("superflex", False))
    idp = work["db_name"].map(lambda d: league_flags.get(d, {}).get("idp", False))
    work["cl_sf"] = np.where(sf, "SF1", "SF0")
    work["cl_full"] = work["cl_sf"] + "_" + np.where(idp, "IDP1", "IDP0")

    curves, floors, median_scales = {}, {}, {}
    for fmt, fmt_df in work.groupby("fmt"):
        increasing = fmt == "auction"
        years = sorted(fmt_df["year"].unique())
        top_positions = fmt_df["position"].value_counts().head(10).index.tolist()
        year_scales = fmt_df.groupby(["db_name", "year"])["y_scale"].first().reset_index()
        for target_year in years:
            train = fmt_df[fmt_df["year"] < target_year]
            if len(train) < 200:
                continue
            prior_scales = year_scales[year_scales["year"] < target_year]["y_scale"]
            if len(prior_scales) > 0:
                median_scales[(fmt, target_year)] = float(prior_scales.median())
            w = np.power(POOLED_RECENCY_DECAY, (target_year - 1) - train["year"])
            train = train.assign(w=w)

            for pos in top_positions:
                pos_train = train[train["position"] == pos]
                if len(pos_train) < 50:
                    continue
                # Late-pick empirical floor (bottom of the board), scale-free.
                late = (
                    pos_train[pos_train["x_norm"] <= 3]
                    if fmt == "auction"
                    else pos_train[pos_train["x_norm"] >= 85]
                )
                if len(late) >= 20:
                    floors[(fmt, pos, target_year)] = max(
                        0.0, float(np.average(late["y"], weights=late["w"]))
                    )

                for cluster_col in (None, "cl_sf", "cl_full"):
                    if cluster_col is None:
                        subsets = [("ALL", pos_train)]
                    else:
                        subsets = list(pos_train.groupby(cluster_col))
                    for cname, sub in subsets:
                        if len(sub) < 50:
                            continue
                        curve = fit_binned_curve(
                            sub["x_norm"], sub["y"], sub["w"], increasing, TAIL_MIN_WEIGHT
                        )
                        if curve is not None:
                            curves[(fmt, str(cname), target_year, pos)] = curve
    return curves, floors, median_scales


def lookup_pooled(curves, fmt, flags, target_year, pos):
    for key in cluster_keys(flags):
        curve = curves.get((fmt, key, target_year, pos))
        if curve is not None:
            return curve
    return None


# ---------------------------------------------------------------------------
# Walk-forward evaluation
# ---------------------------------------------------------------------------


def fit_local_predictors(train, fmt, train_max_year):
    """Per-position local curve sets for all variants sharing this training.

    Returns {pos: {"base": curve, "base_shrunk": ..., "pct": ...,
                   "pct_shrunk": ..., "n": int}} where pct axes exist only
    for snake (auction pct == base).
    """
    increasing = fmt == "auction"
    x_col = "x_raw" if increasing else "x_raw"
    work = train.dropna(subset=[x_col]).copy()
    work["w"] = np.power(RECENCY_DECAY, train_max_year - work["year"])
    out = {}
    for pos, pos_df in work.groupby("position"):
        n = len(pos_df)
        entry = {"n": n}
        ys = pos_df["manager_lamar"].to_numpy(dtype=float)
        ws = pos_df["w"].to_numpy(dtype=float)
        if n >= MIN_ISOTONIC_POINTS:
            xs_raw = pos_df["x_raw"].to_numpy(dtype=float)
            xs_pct = pos_df["x_norm"].to_numpy(dtype=float)
            entry["base"] = fit_curve(xs_raw, ys, ws, increasing)
            entry["base_shrunk"] = fit_curve(xs_raw, ys, ws, increasing, TAIL_MIN_WEIGHT)
            if increasing:
                entry["pct"] = entry["base"]
                entry["pct_shrunk"] = entry["base_shrunk"]
            else:
                entry["pct"] = fit_curve(xs_pct, ys, ws, increasing)
                entry["pct_shrunk"] = fit_curve(xs_pct, ys, ws, increasing, TAIL_MIN_WEIGHT)
        elif n >= 3:
            mean_curve = fit_mean_curve(ys, ws)
            if mean_curve is None:
                continue
            for key in ("base", "base_shrunk", "pct", "pct_shrunk"):
                entry[key] = mean_curve
        else:
            continue
        out[pos] = entry
    return out


def local_floor_fallback(train, fmt, pos):
    """League-local late-pick mean when no pooled floor exists."""
    pos_train = train[train["position"] == pos]
    late = (
        pos_train[pos_train["x_norm"] <= 3]
        if fmt == "auction"
        else pos_train[pos_train["x_norm"] >= 85]
    )
    if len(late) < 8:
        return None
    return max(0.0, float(late["manager_lamar"].mean()))


def walk_forward_league(
    league_df, db_name, sizes, flags, pooled, min_train_years
):
    """Score every year of one league. Returns pick-level rows with a
    prediction column per variant (population A) or pooled-only (B)."""
    pooled_curves, pooled_floors, median_scales = pooled
    year_formats = {
        int(year): grp["fmt"].iloc[0] for year, grp in league_df.groupby("year")
    }
    year_scales = {
        int(year): grp["y_scale"].iloc[0] for year, grp in league_df.groupby("year")
    }
    years = sorted(year_formats)
    n_history = len(years)
    rows = []

    for target_year in years:
        fmt = year_formats[target_year]
        train_years = [y for y in years if y < target_year and year_formats[y] == fmt]
        target = league_df[league_df["year"] == target_year]
        target = target[~target["position"].isin(STREAMING_POSITIONS)]
        target = target.dropna(subset=["x_raw"])
        if target.empty:
            continue

        has_local = len(train_years) >= min_train_years
        train = (
            league_df[league_df["year"].isin(train_years)] if train_years else league_df.iloc[0:0]
        )
        local = (
            fit_local_predictors(train, fmt, max(train_years)) if has_local else {}
        )

        # Rescale factor for scale-free pooled curves: the league's own prior
        # years when available (no leakage), else the fleet median.
        train_scales = [year_scales[y] for y in train_years if pd.notna(year_scales.get(y))]
        league_scale = (
            float(np.mean(train_scales))
            if train_scales
            else median_scales.get((fmt, target_year))
        )

        for pos, pos_target in target.groupby("position"):
            xs_raw = pos_target["x_raw"].to_numpy(dtype=float)
            xs_pct = pos_target["x_norm"].to_numpy(dtype=float)
            realized = pos_target["manager_lamar"].to_numpy(dtype=float)
            capital = xs_raw if fmt == "auction" else -xs_raw
            n_picks = len(pos_target)

            floor_free = pooled_floors.get((fmt, pos, target_year))
            floor = (
                floor_free * league_scale
                if (floor_free is not None and league_scale is not None)
                else None
            )
            if floor is None and has_local:
                floor = local_floor_fallback(train, fmt, pos)

            pooled_curve = lookup_pooled(pooled_curves, fmt, flags, target_year, pos)
            pooled_pred = None
            if pooled_curve is not None and league_scale is not None:
                pooled_pred = pooled_curve.predict(xs_pct) * league_scale
                if floor is not None:
                    pooled_pred = np.maximum(pooled_pred, floor)

            preds = {}
            population = "A" if has_local else "B"
            entry = local.get(pos)
            if population == "A" and entry is not None:
                base = entry["base"].predict(xs_raw)
                preds["baseline"] = base
                preds["floor"] = np.maximum(base, floor) if floor is not None else base
                preds["shrink"] = entry["base_shrunk"].predict(xs_raw)
                pct_x = xs_raw if fmt == "auction" else xs_pct
                preds["percentile"] = entry["pct"].predict(pct_x)
                combined = entry["pct_shrunk"].predict(pct_x)
                if floor is not None:
                    combined = np.maximum(combined, floor)
                preds["combined"] = combined
                if pooled_pred is not None:
                    cred = min(1.0, entry["n"] / POOLED_BLEND_FULL_TRUST)
                    preds["pooled_all"] = cred * combined + (1 - cred) * pooled_pred
                else:
                    preds["pooled_all"] = combined
            elif population == "A":
                continue  # position unseen in training — skip like baseline did
            else:
                if pooled_pred is None:
                    continue
                preds["pooled_pure"] = pooled_pred

            for i in range(n_picks):
                row = {
                    "db_name": db_name,
                    "year": target_year,
                    "format": fmt,
                    "position": pos,
                    "population": population,
                    "capital": float(capital[i]),
                    "realized": float(realized[i]),
                    "num_teams": sizes.get(target_year, np.nan),
                    "history_years": n_history,
                    "superflex": flags.get("superflex", False),
                    "idp": flags.get("idp", False),
                }
                for name, arr in preds.items():
                    row[f"pred_{name}"] = float(arr[i])
                rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def value_calibration(pairs, pred_col, n_bins=10):
    work = pairs.dropna(subset=[pred_col, "realized"]).copy()
    if len(work) < n_bins * 2:
        return {
            "wmae": np.nan, "bias": np.nan, "n": len(work),
            "top_err": np.nan, "bottom_err": np.nan,
        }
    try:
        work["bin"] = pd.qcut(work[pred_col], q=n_bins, duplicates="drop")
    except ValueError:
        return {
            "wmae": np.nan, "bias": np.nan, "n": len(work),
            "top_err": np.nan, "bottom_err": np.nan,
        }
    grouped = (
        work.groupby("bin", observed=True)
        .agg(predicted_avg=(pred_col, "mean"), realized_avg=("realized", "mean"), count=("realized", "count"))
        .reset_index()
    )
    grouped["abs_error"] = (grouped["predicted_avg"] - grouped["realized_avg"]).abs()
    total = grouped["count"].sum()
    return {
        "wmae": float((grouped["abs_error"] * grouped["count"]).sum() / total),
        "bias": float((work[pred_col] - work["realized"]).mean()),
        "n": int(total),
        "top_err": float(grouped["abs_error"].iloc[-1]),
        "bottom_err": float(grouped["abs_error"].iloc[0]),
    }


def mean_spearman(pairs, pred_col, min_picks=20):
    """Mean pooled Spearman per league-year."""
    values = []
    for _, grp in pairs.groupby(["db_name", "year"]):
        grp = grp.dropna(subset=[pred_col, "realized"])
        if len(grp) < min_picks:
            continue
        rho = grp[pred_col].corr(grp["realized"], method="spearman")
        if pd.notna(rho):
            values.append(rho)
    return (float(np.mean(values)), len(values)) if values else (np.nan, 0)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_variant_comparison(pop_a):
    print("=" * 78)
    print("POPULATION A — head-to-head variants (leagues with local history)")
    print(f"  {pop_a['db_name'].nunique()} leagues, "
          f"{pop_a.groupby(['db_name', 'year']).ngroups} league-years, "
          f"{len(pop_a):,} picks (non-K/DEF)")
    print("=" * 78)

    for fmt in ("auction", "snake"):
        fmt_pairs = pop_a[pop_a["format"] == fmt]
        if fmt_pairs.empty:
            continue
        base_cal = value_calibration(fmt_pairs, "pred_baseline")
        base_sp, _ = mean_spearman(fmt_pairs, "pred_baseline")
        cap_sp, _ = mean_spearman(fmt_pairs, "capital")

        print(f"\n{fmt.upper()}  (raw-capital Spearman baseline: {cap_sp:.3f})")
        header = (
            f"  {'variant':<11s} {'WMAE':>6s} {'dWMAE':>7s} {'Spear':>6s} {'dSp':>7s} "
            f"{'topErr':>7s} {'botErr':>7s} {'bias':>7s}  verdict"
        )
        print(header)
        print("  " + "-" * (len(header) - 2))
        for variant in VARIANTS:
            col = f"pred_{variant}"
            cal = value_calibration(fmt_pairs, col)
            sp, _ = mean_spearman(fmt_pairs, col)
            if variant == "baseline":
                verdict = "(reference)"
            elif pd.isna(cal["wmae"]) or pd.isna(sp):
                verdict = "n/a"
            else:
                improved = cal["wmae"] < base_cal["wmae"] - 0.01
                rank_ok = sp >= base_sp - 0.005
                verdict = "PASS" if (improved and rank_ok) else (
                    "rank-loss" if not rank_ok else "no-gain"
                )
            d_wmae = cal["wmae"] - base_cal["wmae"] if pd.notna(cal["wmae"]) else np.nan
            d_sp = sp - base_sp if pd.notna(sp) else np.nan
            print(
                f"  {variant:<11s} {cal['wmae']:>6.2f} {d_wmae:>+7.2f} {sp:>6.3f} {d_sp:>+7.3f} "
                f"{cal['top_err']:>7.1f} {cal['bottom_err']:>7.1f} {cal['bias']:>+7.2f}  {verdict}"
            )


def print_segments(pop_a, keeper_leagues, winner="pooled_all"):
    print("\n" + "=" * 78)
    print(f"SEGMENT VALUE-ADD — baseline vs {winner} (population A)")
    print("=" * 78)
    work = pop_a.copy()
    work["seg_size"] = work["num_teams"].map(size_bucket)
    work["seg_history"] = work["history_years"].map(history_bucket)
    work["seg_sf"] = np.where(work["superflex"], "superflex", "1QB")
    work["seg_idp"] = np.where(work["idp"], "IDP", "no-IDP")
    work["seg_keeper"] = np.where(
        work["db_name"].isin(keeper_leagues), "keeper", "redraft"
    )

    header = (
        f"  {'segment':<18s} {'n':>8s} {'WMAE base':>10s} {'WMAE new':>9s} "
        f"{'Sp base':>8s} {'Sp new':>7s}"
    )
    for seg_col in ("format", "seg_size", "seg_sf", "seg_idp", "seg_keeper", "seg_history"):
        print(f"\n  by {seg_col.replace('seg_', '')}:")
        print(header)
        print("  " + "-" * (len(header) - 2))
        for seg_val, grp in sorted(work.groupby(seg_col), key=lambda kv: str(kv[0])):
            base_cal = value_calibration(grp, "pred_baseline")
            new_cal = value_calibration(grp, f"pred_{winner}")
            base_sp, _ = mean_spearman(grp, "pred_baseline")
            new_sp, _ = mean_spearman(grp, f"pred_{winner}")
            print(
                f"  {str(seg_val):<18s} {len(grp):>8,d} {base_cal['wmae']:>10.2f} "
                f"{new_cal['wmae']:>9.2f} {base_sp:>8.3f} {new_sp:>7.3f}"
            )


def print_population_b(pop_b):
    print("\n" + "=" * 78)
    print("POPULATION B — thin-history leagues (no local model possible)")
    print("  Scored with pure fleet-pooled curves; nothing to compare against")
    print("  except the raw-capital ordering these leagues get implicitly today.")
    print("=" * 78)
    if pop_b.empty:
        print("  (no population B rows)")
        return
    print(
        f"  {pop_b['db_name'].nunique()} leagues, "
        f"{pop_b.groupby(['db_name', 'year']).ngroups} league-years, {len(pop_b):,} picks"
    )
    for fmt, grp in pop_b.groupby("format"):
        cal = value_calibration(grp, "pred_pooled_pure")
        sp, n_ly = mean_spearman(grp, "pred_pooled_pure")
        cap_sp, _ = mean_spearman(grp, "capital")
        print(
            f"  {fmt:8s} n={len(grp):>8,d}  pooled WMAE: {cal['wmae']:5.2f}  "
            f"bias: {cal['bias']:+5.2f}  Spearman: {sp:.3f} vs raw-capital {cap_sp:.3f} "
            f"({n_ly} league-years)"
        )


def print_qa_leagues(all_pairs, winner="pooled_all"):
    print("\n" + "=" * 78)
    print(f"NAMED QA LEAGUES — baseline vs {winner}")
    print("=" * 78)
    header = (
        f"  {'league':<38s} {'pop':>3s} {'picks':>6s} {'Sp base':>8s} {'Sp new':>7s} "
        f"{'WMAE base':>10s} {'WMAE new':>9s}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for db in QA_LEAGUES:
        grp = all_pairs[all_pairs["db_name"] == db]
        if grp.empty:
            print(f"  {db:<38s}  (no scored picks)")
            continue
        pop = "A" if (grp["population"] == "A").any() else "B"
        if pop == "A":
            grp = grp[grp["population"] == "A"]
            base_sp, _ = mean_spearman(grp, "pred_baseline")
            new_sp, _ = mean_spearman(grp, f"pred_{winner}")
            base_cal = value_calibration(grp, "pred_baseline", n_bins=5)
            new_cal = value_calibration(grp, f"pred_{winner}", n_bins=5)
            print(
                f"  {db:<38s} {pop:>3s} {len(grp):>6,d} {base_sp:>8.3f} {new_sp:>7.3f} "
                f"{base_cal['wmae']:>10.2f} {new_cal['wmae']:>9.2f}"
            )
        else:
            sp, _ = mean_spearman(grp, "pred_pooled_pure")
            cal = value_calibration(grp, "pred_pooled_pure", n_bins=5)
            print(
                f"  {db:<38s} {pop:>3s} {len(grp):>6,d} {'—':>8s} {sp:>7.3f} "
                f"{'—':>10s} {cal['wmae']:>9.2f}"
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Draft value calibration study - walk-forward variant comparison",
    )
    parser.add_argument("--db", type=str, default=None, help="Single league database to evaluate")
    parser.add_argument("--max-leagues", type=int, default=None, help="Limit league count (testing)")
    parser.add_argument(
        "--min-train-years",
        type=int,
        default=2,
        help="Minimum prior same-format years for a league-local model (default 2)",
    )
    parser.add_argument("--export", type=str, default=None, help="Export pick-level pairs to CSV")
    args = parser.parse_args()

    logger.info("Connecting to Fly...")
    reader = get_reader()

    databases = discover_league_databases(reader)
    scored_dbs = [args.db] if args.db else (
        databases[: args.max_leagues] if args.max_leagues else databases
    )

    # Pooled curves always train on the FULL fleet (temporal split handles
    # leakage); --db/--max-leagues only limit which leagues get scored.
    logger.info(f"Pulling draft picks for {len(databases)} leagues from {CENTRAL_DB}...")
    fleet_draft = extract_fleet_draft(reader, databases)
    if fleet_draft.empty:
        logger.error("No draft data returned.")
        sys.exit(1)
    fleet_sizes = extract_fleet_sizes(reader)
    keeper_leagues = extract_keeper_leagues(reader)
    logger.info(f"Pulled {len(fleet_draft):,} non-keeper picks; normalizing axes...")

    norm = normalize_fleet_axes(fleet_draft, fleet_sizes)
    league_flags = build_league_flags(norm)
    logger.info("Fitting fleet-pooled curves (temporal walk-forward)...")
    pooled = build_pooled_curves(norm, league_flags)
    logger.info(f"Fitted {len(pooled[0]):,} pooled curves, {len(pooled[1]):,} floors")

    all_rows = []
    scored_set = set(scored_dbs)
    league_groups = [
        (db, grp) for db, grp in norm.groupby("db_name") if db in scored_set
    ]
    for i, (db_name, league_df) in enumerate(league_groups, 1):
        if i % 200 == 0:
            logger.info(f"[{i}/{len(league_groups)}] walk-forward...")
        all_rows.extend(
            walk_forward_league(
                league_df,
                db_name,
                fleet_sizes.get(db_name, {}),
                league_flags.get(db_name, {}),
                pooled,
                args.min_train_years,
            )
        )

    if not all_rows:
        logger.error("No walk-forward pairs produced.")
        sys.exit(1)

    all_pairs = pd.DataFrame(all_rows)
    logger.info(
        f"Scored {len(all_pairs):,} picks across "
        f"{all_pairs.groupby(['db_name', 'year']).ngroups} league-years"
    )

    if args.export:
        all_pairs.to_csv(args.export, index=False)
        logger.info(f"Pick-level pairs exported to {args.export}")

    pop_a = all_pairs[all_pairs["population"] == "A"]
    pop_b = all_pairs[all_pairs["population"] == "B"]

    if not pop_a.empty:
        print_variant_comparison(pop_a)
        print_segments(pop_a, keeper_leagues)
    print_population_b(pop_b)
    print_qa_leagues(all_pairs)


if __name__ == "__main__":
    main()
