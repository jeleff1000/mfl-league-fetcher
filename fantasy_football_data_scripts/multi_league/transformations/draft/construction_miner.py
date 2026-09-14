#!/usr/bin/env python3
"""Roster-construction miner — how a manager BUILDS a team, not just who they
pick. The wide miner works at pick grain; construction lives at the
manager-draft grain (and cross-year), so it gets its own pass and feeds the
same surfacing/rendering contract.

Signals:
  archetype        Zero-RB / Robust-RB / Hero-RB / early-QB / late-QB / TE-early
  spend_shape      stars-and-scrubs vs balanced (auction)
  opening          "almost always opens with a running back"
  run_reactivity   chases position runs vs fades them (snake)
  repeat_player    "keeps going back to <player> — <k> seasons"
  consistency      runs the same playbook every year (meta)

Each emits a nugget row shaped like the wide miner's (scope_type=manager,
nugget_headline/evidence, surfaced, importance, ...) so the QA harness and
downstream treat them identically.
"""

from __future__ import annotations

from typing import Any

MODEL_VERSION = "construction-miner-v1"

# Minimum drafts to call a per-manager construction pattern a tendency.
MIN_DRAFTS = 3
EARLY_PICKS = 6  # "early" = the manager's top-N picks by draft capital


def _fetch_sql(db_name: str) -> str:
    safe = db_name.replace("'", "''")
    return f"""
    SELECT COALESCE(franchise_id, manager) AS manager_key,
           manager, year,
           UPPER(COALESCE(position, '')) AS pos,
           COALESCE(cost, 0) AS cost,
           pick, round, NFL_player_id, player
    FROM public.draft
    WHERE db_name = '{safe}'
      AND COALESCE(is_keeper, 0) = 0
      AND year IS NOT NULL
      AND manager IS NOT NULL
      AND NFL_player_id IS NOT NULL
    """


# ---------------------------------------------------------------------------
# Per-manager-year construction labels
# ---------------------------------------------------------------------------


def _label_one_draft(picks) -> set[str]:
    """picks: DataFrame for one (manager, year), ordered by draft capital
    (best pick first). Returns the set of construction labels it earns."""
    import pandas as pd  # noqa: F401

    labels: set[str] = set()
    positions = list(picks["pos"])
    n = len(positions)
    if n < 4:
        return labels
    early = positions[:EARLY_PICKS]

    rb_early = early.count("RB")
    wr_early = early.count("WR")
    if rb_early == 0:
        labels.add("zero_rb")
    if early[:3].count("RB") >= 2:
        labels.add("robust_rb")
    if rb_early == 1 and positions[0] == "RB":
        labels.add("hero_rb")
    if wr_early >= 3:
        labels.add("wr_heavy_start")

    # QB timing: capital rank of the first QB relative to draft length.
    qb_rank = next((i for i, p in enumerate(positions) if p == "QB"), None)
    if qb_rank is not None:
        if qb_rank <= 2:
            labels.add("early_qb")
        elif qb_rank >= max(9, int(n * 0.6)):
            labels.add("late_qb")
    else:
        labels.add("late_qb")  # never took one until the very end / streamed

    if "TE" in early[:4]:
        labels.add("te_early")

    # Opening pick.
    labels.add(f"opens_{positions[0].lower()}")

    # Spend concentration (auction only).
    costs = picks["cost"].tolist()
    if sum(1 for c in costs if c > 0) >= max(6, int(n * 0.5)):
        total = sum(costs)
        if total > 0:
            top3 = sum(sorted(costs, reverse=True)[:3]) / total
            if top3 >= 0.5:
                labels.add("stars_and_scrubs")
            elif top3 <= 0.35:
                labels.add("balanced_build")
    return labels


def _capital_order(group):
    """Order one draft's picks best-first by capital (cost for auction,
    pick for snake)."""
    is_auction = (group["cost"] > 0).mean() >= 0.25
    if is_auction:
        return group.sort_values("cost", ascending=False)
    return group.sort_values("pick", ascending=True, na_position="last")


def manager_year_labels(df):
    """Returns {(manager_key, year): set[label]} and per-(manager,year) meta."""
    labels: dict[tuple, set[str]] = {}
    for (mk, yr), group in df.groupby(["manager_key", "year"], sort=False):
        labels[(mk, yr)] = _label_one_draft(_capital_order(group))
    return labels


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

# Each label: importance, a STRONG identity phrase (rate >= 0.7), a SOFT verb
# phrase (used with "usually"/"often" at lower rates), and the unit word.
_LABEL_META: dict[str, dict[str, Any]] = {
    "zero_rb": {"imp": 3, "strong": "drafts Zero-RB — no early running back", "soft": "goes Zero-RB"},
    "robust_rb": {"imp": 3, "strong": "is a Robust-RB drafter — loads up on backs early", "soft": "loads up on backs early"},
    "hero_rb": {"imp": 3, "strong": "plays Hero-RB — one big back, then receivers", "soft": "plays Hero-RB"},
    "wr_heavy_start": {"imp": 3, "strong": "opens WR-heavy — stacks receivers early", "soft": "opens WR-heavy"},
    "early_qb": {"imp": 3, "strong": "grabs their quarterback early — a top-3 pick", "soft": "grabs their quarterback early"},
    "late_qb": {"imp": 3, "strong": "waits on quarterback — no QB until the late rounds", "soft": "waits on quarterback"},
    "te_early": {"imp": 3, "strong": "prioritizes tight end — one in the first few rounds", "soft": "takes an early tight end"},
    "stars_and_scrubs": {"imp": 3, "strong": "drafts stars-and-scrubs — most of the budget on a few players", "soft": "leans stars-and-scrubs", "unit": "auctions"},
    "balanced_build": {"imp": 3, "strong": "spreads the money around — a balanced build", "soft": "leans toward a balanced build", "unit": "auctions"},
    "opens_rb": {"imp": 2, "strong": "almost always opens with a running back", "soft": "opens with a running back"},
    "opens_wr": {"imp": 2, "strong": "almost always opens with a receiver", "soft": "opens with a receiver"},
    "opens_qb": {"imp": 2, "strong": "almost always opens with a quarterback", "soft": "opens with a quarterback"},
    "opens_te": {"imp": 2, "strong": "almost always opens with a tight end", "soft": "opens with a tight end"},
}


def _construction_headline(name: str, meta: dict, obs: float, n_with: int, n_total: int) -> str | None:
    """Rate-matched framing. An identity claim ('is a Zero-RB drafter') needs
    a real absolute rate, not just above-baseline — 2-of-16 is not a story."""
    unit = meta.get("unit", "drafts")
    if obs >= 0.70:
        return f"{name} {meta['strong']} in {n_with} of {n_total} {unit}."
    if obs >= 0.50:
        return f"{name} usually {meta['soft']} — {n_with} of {n_total} {unit}."
    if obs >= 0.35:
        return f"{name} often {meta['soft']} — {n_with} of {n_total} {unit}."
    return None  # below a third of their drafts is not a tendency


def _binom_z(n_with: int, n_total: int, base_rate: float) -> float:
    import math

    expected = n_total * base_rate
    var = n_total * base_rate * (1 - base_rate)
    if var <= 0:
        return 0.0
    return (n_with - expected) / math.sqrt(var)


def score_construction(labels: dict[tuple, set[str]], df) -> list[dict[str, Any]]:
    from collections import defaultdict

    from multi_league.transformations.draft.nugget_renderer import surface_score

    mgr_of = {mk for mk, _ in labels}
    label_names = {"zero_rb", "robust_rb", "hero_rb", "wr_heavy_start", "early_qb",
                   "late_qb", "te_early", "stars_and_scrubs", "balanced_build",
                   "opens_rb", "opens_wr", "opens_qb", "opens_te"}
    name_of = dict(zip(df["manager_key"], df["manager"]))

    # Per manager: total drafts + label counts. League: base rate per label.
    mgr_total: dict[str, int] = defaultdict(int)
    mgr_label: dict[tuple, int] = defaultdict(int)
    league_total = 0
    league_label: dict[str, int] = defaultdict(int)
    for (mk, _yr), labs in labels.items():
        mgr_total[mk] += 1
        league_total += 1
        for lab in labs:
            if lab in label_names:
                mgr_label[(mk, lab)] += 1
                league_label[lab] += 1

    rows: list[dict[str, Any]] = []
    for (mk, lab), n_with in mgr_label.items():
        n_total = mgr_total[mk]
        if n_total < MIN_DRAFTS or n_with < 2:
            continue
        base = league_label[lab] / league_total if league_total else 0
        if base <= 0 or base >= 0.999:
            continue
        obs = n_with / n_total
        if obs < base * 1.3:  # must exceed the league's own base rate
            continue
        meta = _LABEL_META[lab]
        headline = _construction_headline(str(name_of.get(mk, mk)), meta, obs, n_with, n_total)
        if headline is None:  # real absolute rate required, not just above base
            continue
        z = _binom_z(n_with, n_total, base)
        row = {
            "db_name": None,
            "scope_type": "manager",
            "scope_key": str(mk),
            "scope_label": str(name_of.get(mk, mk)),
            "feature_type": f"construction.{lab}",
            "feature_value": lab,
            "feature_label": "Roster Construction",
            "legibility": "A",
            "importance": meta["imp"],
            "count_z_score": round(z, 3),
            "count_lift": round(obs / base, 3),
            "count_repeatability": round(n_with / n_total, 3),
            "years_seen": n_total,
            "picks": n_with,
            "nugget_headline": headline,
            "nugget_evidence": f"{n_with} of {n_total} {meta.get('unit', 'drafts')}",
            "confidence": "wide",
            "evidence_level": "explore",
            "model_version": MODEL_VERSION,
        }
        row.update(surface_score(row))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Repeat-player loyalty
# ---------------------------------------------------------------------------


def score_repeat_players(df, *, min_seasons: int = 4) -> list[dict[str, Any]]:
    from multi_league.transformations.draft.nugget_renderer import surface_score

    rows: list[dict[str, Any]] = []
    grp = (
        df.dropna(subset=["player"])
        .groupby(["manager_key", "manager", "NFL_player_id", "player"], sort=False)["year"]
        .nunique()
        .reset_index(name="seasons")
    )
    grp = grp[grp["seasons"] >= min_seasons]
    # One nugget per manager: their most-repeated player.
    for (mk, name), sub in grp.groupby(["manager_key", "manager"], sort=False):
        top = sub.sort_values("seasons", ascending=False).iloc[0]
        k = int(top["seasons"])
        row = {
            "db_name": None,
            "scope_type": "manager",
            "scope_key": str(mk),
            "scope_label": str(name),
            "feature_type": "construction.repeat_player",
            "feature_value": str(top["player"]),
            "feature_label": "Player Loyalty",
            "legibility": "A",
            "importance": 2,
            "count_z_score": 3.0 + (k - min_seasons),  # more seasons = louder
            "count_lift": float(k),
            "count_repeatability": 1.0,
            "years_seen": k,
            "picks": k,
            "nugget_headline": f"{name} keeps going back to {top['player']} — drafted in {k} different seasons.",
            "nugget_evidence": f"{k} seasons",
            "confidence": "wide",
            "evidence_level": "explore",
            "model_version": MODEL_VERSION,
        }
        row.update(surface_score(row))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Run reactivity (snake) — does the manager pick INTO position runs?
# ---------------------------------------------------------------------------


def score_run_reactivity(df, *, window: int = 4) -> list[dict[str, Any]]:
    import pandas as pd

    from multi_league.transformations.draft.nugget_renderer import surface_score

    rows: list[dict[str, Any]] = []
    # Snake years only: pick order must be meaningful (mostly $0).
    snake_pick_flags = []
    for yr, g in df.groupby("year", sort=False):
        if (g["cost"] > 0).mean() < 0.25:
            snake_pick_flags.append(yr)
    snake = df[df["year"].isin(snake_pick_flags)].dropna(subset=["pick"]).copy()
    if snake.empty:
        return rows

    into_run = {}
    total = {}
    league_into = 0
    league_total = 0
    for yr, g in snake.groupby("year", sort=False):
        g = g.sort_values("pick")
        seq = list(zip(g["manager_key"], g["pos"]))
        for i, (mk, pos) in enumerate(seq):
            prior = [p for _, p in seq[max(0, i - window):i]]
            is_into = prior.count(pos) >= 2  # position already flying off the board
            total[mk] = total.get(mk, 0) + 1
            league_total += 1
            if is_into:
                into_run[mk] = into_run.get(mk, 0) + 1
                league_into += 1
    if league_total == 0:
        return rows
    base = league_into / league_total
    name_of = dict(zip(df["manager_key"], df["manager"]))
    mgr_years = df.groupby("manager_key")["year"].nunique().to_dict()
    for mk, tot in total.items():
        if tot < 20 or mgr_years.get(mk, 0) < MIN_DRAFTS:
            continue
        obs = into_run.get(mk, 0) / tot
        if base <= 0 or base >= 1:
            continue
        import math
        z = (into_run.get(mk, 0) - tot * base) / math.sqrt(max(tot * base * (1 - base), 1e-9))
        if obs >= base * 1.25 and z >= 2.0:
            verdict = "chases position runs — jumps in when a position starts flying off the board"
        elif obs <= base * 0.75 and z <= -2.0:
            verdict = "fades position runs — stays away when everyone's piling into a position"
        else:
            continue
        row = {
            "db_name": None,
            "scope_type": "manager",
            "scope_key": str(mk),
            "scope_label": str(name_of.get(mk, mk)),
            "feature_type": "construction.run_reactivity",
            "feature_value": "chase" if obs >= base else "fade",
            "feature_label": "Run Reactivity",
            "legibility": "A",
            "importance": 2,
            "count_z_score": round(z, 3),
            "count_lift": round(obs / base, 3),
            "count_repeatability": round(mgr_years.get(mk, 0) / mgr_years.get(mk, 1), 3),
            "years_seen": int(mgr_years.get(mk, 0)),
            "picks": tot,
            "nugget_headline": f"{name_of.get(mk, mk)} {verdict}.",
            "nugget_evidence": f"across {int(mgr_years.get(mk, 0))} snake drafts",
            "confidence": "wide",
            "evidence_level": "explore",
            "model_version": MODEL_VERSION,
        }
        row.update(surface_score(row))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Consistency (meta)
# ---------------------------------------------------------------------------


def score_consistency(labels: dict[tuple, set[str]], df, *, min_drafts: int = 4) -> list[dict[str, Any]]:
    from collections import Counter, defaultdict

    from multi_league.transformations.draft.nugget_renderer import surface_score

    name_of = dict(zip(df["manager_key"], df["manager"]))
    # Opening position per draft = the strategy signature we test for stability.
    opens_by_mgr: dict[str, list[str]] = defaultdict(list)
    for (mk, _yr), labs in labels.items():
        opener = next((lab for lab in labs if lab.startswith("opens_")), None)
        if opener:
            opens_by_mgr[mk].append(opener)

    rows: list[dict[str, Any]] = []
    for mk, opens in opens_by_mgr.items():
        n = len(opens)
        if n < min_drafts:
            continue
        top, cnt = Counter(opens).most_common(1)[0]
        share = cnt / n
        if share >= 0.8:
            pos = top.replace("opens_", "").upper()
            headline = f"{name_of.get(mk, mk)} is utterly predictable — opens with a {pos} in {cnt} of {n} drafts."
            fv = "predictable"
        elif len(set(opens)) == n:  # a different opener every single year
            headline = f"{name_of.get(mk, mk)} reinvents their draft every year — a different opening position every time."
            fv = "chaotic"
        else:
            continue
        row = {
            "db_name": None,
            "scope_type": "manager",
            "scope_key": str(mk),
            "scope_label": str(name_of.get(mk, mk)),
            "feature_type": "construction.consistency",
            "feature_value": fv,
            "feature_label": "Draft Consistency",
            "legibility": "A",
            "importance": 2,
            "count_z_score": 3.5 if fv == "predictable" else 3.0,
            "count_lift": 2.0,
            "count_repeatability": round(share, 3),
            "years_seen": n,
            "picks": n,
            "nugget_headline": headline,
            "nugget_evidence": f"{n} drafts",
            "confidence": "wide",
            "evidence_level": "explore",
            "model_version": MODEL_VERSION,
        }
        row.update(surface_score(row))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


def mine_construction(df) -> list[dict[str, Any]]:
    if df is None or len(df) == 0:
        return []
    labels = manager_year_labels(df)
    signals = []
    signals += score_construction(labels, df)
    signals += score_repeat_players(df)
    signals += score_run_reactivity(df)
    signals += score_consistency(labels, df)
    return signals


def run_construction_miner_fly(db_name: str) -> list[dict[str, Any]]:
    import pandas as pd
    from multi_league.core.readers.fly_reader import FlyReader

    rows = FlyReader().query(_fetch_sql(db_name), database="___leagues")
    df = pd.DataFrame(rows)
    return mine_construction(df)
