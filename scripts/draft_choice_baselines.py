#!/usr/bin/env python3
"""Step 1 of manager-level draft prediction: candidate-pool dataset + baselines.

The pick a manager makes = market + roster-need + a small manager residual.
Before we try to model the residual we have to know how well the *market*
predicts a pick with no manager identity at all — that number is the floor
every later "manager-aware" lift is measured against.

This script frames each pick as a DISCRETE CHOICE: at the moment a manager is
on the clock, the candidate pool is every player still available (reconstructed
as the players drafted at a later sequence position in that same draft, minus
keepers). We then score four position/player predictors on every pick,
out-of-sample, and report top-1/top-3 accuracy:

  adp          best available by FLEET ADP           (market baseline #1)
  roster_need  best available at a position of need   (market baseline #2)
  static       manager's modal position by round      (the manager NULL)
  naive        league-wide modal position by round    (market-blind NULL)

`static` and `naive` are trained leave-future-out (years < Y predict year Y).
`adp`/`roster_need` use contemporaneous fleet ADP — a deliberately STRONG,
hard-to-beat market baseline, so any later manager lift is honest.

Reads Fly read-only via FlyReader. Writes a per-pick eval parquet + a JSON
summary to the scratchpad (nothing committed until the floor is reviewed).

    python scripts/draft_choice_baselines.py                 # full fleet
    python scripts/draft_choice_baselines.py --years 2020-2024
    python scripts/draft_choice_baselines.py --limit-leagues 200 --out /tmp
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

# --- make `multi_league` importable and load Fly creds from repo-root .env ---
_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "fantasy_football_data_scripts"))
_ENV = _REPO / ".env"
if _ENV.exists():
    for _line in _ENV.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k, _v.strip())

from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402

POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF")
_POS_SET = set(POSITIONS)
#: Rough overall draft frequency — the fallback ranking a position-only
#: predictor uses to fill out a top-3 when history is thin/absent, so its
#: top-3 is always three genuine guesses (and always a superset of its top-1).
DEFAULT_POS_ORDER = ("RB", "WR", "QB", "TE", "DEF", "K")
DEFAULT_SLOTS = {"qb": 1, "rb": 2, "wr": 2, "te": 1, "k": 1, "def": 1,
                 "flex": 1, "rec_flex": 0, "superflex": 0}


def canon_pos(raw: object) -> str:
    """Draft position string -> one of QB/RB/WR/TE/K/DEF/OTHER."""
    if raw is None:
        return "OTHER"
    tok = str(raw).upper().split(",", 1)[0].strip()
    if tok in ("RB", "FB", "HB"):
        return "RB"
    if tok in ("DEF", "DST", "D/ST", "D"):
        return "DEF"
    if tok in ("K", "PK"):
        return "K"
    if tok in _POS_SET:
        return tok
    return "OTHER"


def round_bucket(rnd: object) -> str:
    try:
        r = int(rnd)
    except (TypeError, ValueError):
        return "r9plus"
    if r <= 3:
        return f"r{r}"
    if r <= 5:
        return "r4_5"
    if r <= 8:
        return "r6_8"
    return "r9plus"


# --------------------------------------------------------------------------- #
# Data pull
# --------------------------------------------------------------------------- #

def fetch_year(reader: FlyReader, year: int) -> pd.DataFrame:
    """Every non-keeper pick in `year`, in draft order, with fleet ADP attached.

    Fleet ADP (adp_stage) = avg(pick / max_pick) for the player across every
    league that drafted them that year (>=5 leagues required for stability).
    Lower = drafted earlier by the market.
    """
    sql = f"""
    WITH fleet AS (
        SELECT NFL_player_id, pick,
               MAX(pick) OVER (PARTITION BY db_name) AS mx
        FROM public.draft
        WHERE year = {year} AND NFL_player_id IS NOT NULL
          AND COALESCE(is_keeper,0)=0 AND pick IS NOT NULL
    ),
    adp AS (
        SELECT NFL_player_id,
               AVG(CASE WHEN pick>0 AND mx>1 THEN pick*1.0/mx END) AS adp_stage,
               COUNT(*) AS fleet_n
        FROM fleet GROUP BY NFL_player_id HAVING COUNT(*) >= 5
    ),
    picks AS (
        SELECT d.db_name,
               COALESCE(d.franchise_id, d.manager) AS manager_key,
               d.NFL_player_id, d.player, d.position, COALESCE(d.cost,0) AS cost,
               d.pick, d.round,
               ROW_NUMBER() OVER (PARTITION BY d.db_name ORDER BY d.pick, d.round) AS seq,
               AVG(CASE WHEN COALESCE(d.cost,0)>0 THEN 1.0 ELSE 0.0 END)
                   OVER (PARTITION BY d.db_name) AS auction_share
        FROM public.draft d
        WHERE d.year = {year} AND d.NFL_player_id IS NOT NULL
          AND COALESCE(d.is_keeper,0)=0 AND d.pick IS NOT NULL AND d.manager IS NOT NULL
    )
    SELECT p.db_name, p.manager_key, p.NFL_player_id, p.player, p.position,
           p.cost, p.pick, p.round, p.seq, p.auction_share,
           a.adp_stage, a.fleet_n
    FROM picks p LEFT JOIN adp a ON p.NFL_player_id = a.NFL_player_id
    ORDER BY p.db_name, p.seq
    """
    return pd.DataFrame(reader.query(sql, database="___leagues"))


def fetch_slots(reader: FlyReader) -> dict[tuple[str, int], dict[str, int]]:
    """Starter-slot counts per (db_name, year) from league_settings."""
    sql = """
    SELECT db_name, year,
           COALESCE("roster_QB",0) qb, COALESCE("roster_RB",0) rb,
           COALESCE("roster_WR",0) wr, COALESCE("roster_TE",0) te,
           COALESCE("roster_K",0) k,  COALESCE("roster_DEF",0) def,
           COALESCE("roster_FLX",0)+COALESCE("roster_W/R",0)
             +COALESCE("roster_R/T",0) AS flex,
           COALESCE("roster_REC_FLEX",0) AS rec_flex,
           COALESCE("roster_SUPER_FLEX",0) AS superflex
    FROM public.league_settings WHERE year IS NOT NULL
    """
    out: dict[tuple[str, int], dict[str, int]] = {}
    for r in reader.query(sql, database="___leagues"):
        out[(str(r["db_name"]), int(r["year"]))] = {
            k: int(r.get(k) or 0) for k in DEFAULT_SLOTS
        }
    return out


# --------------------------------------------------------------------------- #
# Roster-need heuristic
# --------------------------------------------------------------------------- #

def needed_positions(have: Counter, slots: dict[str, int]) -> set[str]:
    """Positions this manager still has an unfilled STARTER slot for.

    Heuristic: direct slots first; a flex slot keeps RB/WR/TE "needed" until the
    manager's surplus (beyond direct slots) fills the flex count; rec_flex keeps
    WR/TE needed; superflex keeps QB needed. Approximate (flex pools scored
    independently) but that is all a best-available-of-need heuristic needs.
    """
    needed: set[str] = set()
    for pos, key in (("QB", "qb"), ("RB", "rb"), ("WR", "wr"),
                     ("TE", "te"), ("K", "k"), ("DEF", "def")):
        if have[pos] < slots.get(key, 0):
            needed.add(pos)
    if slots.get("flex", 0) > 0:
        surplus = sum(max(0, have[p] - slots.get(k, 0))
                      for p, k in (("RB", "rb"), ("WR", "wr"), ("TE", "te")))
        if surplus < slots["flex"]:
            needed.update(("RB", "WR", "TE"))
    if slots.get("rec_flex", 0) > 0:
        surplus = sum(max(0, have[p] - slots.get(k, 0))
                      for p, k in (("WR", "wr"), ("TE", "te")))
        if surplus < slots["rec_flex"]:
            needed.update(("WR", "TE"))
    if slots.get("superflex", 0) > 0 and max(0, have["QB"] - slots.get("qb", 0)) < slots["superflex"]:
        needed.add("QB")
    return needed


# --------------------------------------------------------------------------- #
# Evaluation
# --------------------------------------------------------------------------- #

class Tally:
    """hits / total accumulator, split by an arbitrary key."""

    def __init__(self) -> None:
        self.hit: dict[tuple, int] = defaultdict(int)
        self.tot: dict[tuple, int] = defaultdict(int)

    def add(self, splits: list[tuple], hit: bool) -> None:
        for s in splits:
            self.tot[s] += 1
            if hit:
                self.hit[s] += 1

    def rate(self, key: tuple) -> tuple[float, int]:
        t = self.tot.get(key, 0)
        return (self.hit.get(key, 0) / t if t else float("nan"), t)


def _distinct_positions(pos_seq, k: int = 3) -> set[str]:
    """First `k` distinct positions scanning a ranked candidate list — the
    'k most likely positions' for a player-ranking predictor, comparable to a
    position-only predictor's top-k."""
    seen: list[str] = []
    for p in pos_seq:
        if p not in seen:
            seen.append(p)
            if len(seen) == k:
                break
    return set(seen)


def _top3_positions(counts: dict[str, int]) -> list[str]:
    """Three position guesses: most-frequent first, padded by overall
    frequency so a thin/empty history still yields a full, top-1-consistent
    top-3."""
    ranked = [p for p, _ in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)]
    for p in DEFAULT_POS_ORDER:
        if p not in ranked:
            ranked.append(p)
    return ranked[:3]


def evaluate(reader: FlyReader, years: list[int], slots_map: dict,
             limit_leagues: int | None) -> tuple[pd.DataFrame, dict]:
    """One ascending-year pass: predict each year with only prior-year history,
    then fold the year into history. Returns (per-pick eval frame, summary)."""
    tallies = {p: Tally() for p in ("adp", "roster_need", "static", "naive")}
    ptallies = {p: Tally() for p in ("adp", "roster_need")}   # player-level
    # manager history: manager_key -> round_bucket -> Counter(pos), prior years only
    hist: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    ghist: dict[str, Counter] = defaultdict(Counter)          # league-wide null
    rows: list[dict] = []
    keep_dbs: set[str] | None = None

    for year in years:
        df = fetch_year(reader, year)
        if df.empty:
            continue
        if limit_leagues is not None:
            if keep_dbs is None:
                keep_dbs = set(sorted(df["db_name"].unique())[:limit_leagues])
            df = df[df["db_name"].isin(keep_dbs)]
            if df.empty:
                continue
        df["cpos"] = df["position"].map(canon_pos)
        df["adp"] = pd.to_numeric(df["adp_stage"], errors="coerce")
        year_updates: list[tuple[str, str, str]] = []   # (mkey, rbucket, pos) to fold in AFTER the year

        for db_name, g in df.groupby("db_name", sort=False):
            g = g.sort_values("seq")
            pos = g["cpos"].to_numpy()
            adp = g["adp"].to_numpy(dtype=float)
            adp_inf = np.where(np.isnan(adp), np.inf, adp)   # unranked -> worst
            mkey = g["manager_key"].to_numpy()
            player = g["NFL_player_id"].to_numpy()
            pname = g["player"].to_numpy()
            rnd = g["round"].to_numpy()
            is_auction = float(g["auction_share"].iloc[0]) >= 0.25
            fmt = "auction" if is_auction else "snake"
            slots = slots_map.get((db_name, year), DEFAULT_SLOTS)
            n = len(g)
            have: dict[str, Counter] = defaultdict(Counter)   # manager -> pos counts so far

            for i in range(n):
                mk = mkey[i]
                chosen = pos[i]
                rb = round_bucket(rnd[i])
                cand_pos = pos[i:]
                cand_adp = adp_inf[i:]
                cand_player = player[i:]

                # ---- market baseline #1: best available by fleet ADP ----
                order = np.argsort(cand_adp, kind="stable")
                sorted_pos = cand_pos[order]
                t3 = order[:3]
                adp_pos1 = sorted_pos[0]
                adp_pos3 = _distinct_positions(sorted_pos)
                adp_pl1_hit = bool(cand_player[t3[0]] == player[i])
                adp_pl3_hit = bool(player[i] in set(cand_player[t3].tolist()))

                # ---- market baseline #2: best available at a position of need ----
                need = needed_positions(have[mk], slots)
                need_mask = np.array([p in need for p in cand_pos]) if need else np.zeros(len(cand_pos), bool)
                if need_mask.any():
                    idx = np.where(need_mask)[0]
                    idx = idx[np.argsort(cand_adp[idx], kind="stable")]
                else:
                    idx = order          # no need -> best player available
                sorted_pos_rn = cand_pos[idx]
                r3 = idx[:3]
                rn_pos1 = sorted_pos_rn[0]
                rn_pos3 = _distinct_positions(sorted_pos_rn)
                rn_pl1_hit = bool(cand_player[r3[0]] == player[i])
                rn_pl3_hit = bool(player[i] in set(cand_player[r3].tolist()))

                # ---- manager null: modal position at this round from prior years ----
                mcounts = hist.get(mk, {}).get(rb) if mk in hist else None
                has_prior = bool(mcounts)
                gc = ghist.get(rb)
                counts = dict(mcounts) if has_prior else (dict(gc) if gc else {})
                st3 = _top3_positions(counts)
                static_pos1, static_pos3 = st3[0], set(st3)

                # ---- market-blind null: league-wide modal position at this round ----
                nv3 = _top3_positions(dict(gc) if gc else {})
                naive_pos1, naive_pos3 = nv3[0], set(nv3)

                splits = [("all",), ("fmt", fmt), ("round", rb)]
                if has_prior:
                    splits.append(("prior", "yes"))
                tallies["adp"].add(splits, chosen == adp_pos1)
                tallies["adp"].add([("all_t3",), ("fmt_t3", fmt)], chosen in adp_pos3)
                tallies["roster_need"].add(splits, chosen == rn_pos1)
                tallies["roster_need"].add([("all_t3",), ("fmt_t3", fmt)], chosen in rn_pos3)
                tallies["static"].add(splits, chosen == static_pos1)
                tallies["static"].add([("all_t3",), ("fmt_t3", fmt)], chosen in static_pos3)
                tallies["naive"].add(splits, chosen == naive_pos1)
                tallies["naive"].add([("all_t3",), ("fmt_t3", fmt)], chosen in naive_pos3)
                ptallies["adp"].add(splits, adp_pl1_hit)
                ptallies["adp"].add([("all_t3",), ("fmt_t3", fmt)], adp_pl3_hit)
                ptallies["roster_need"].add(splits, rn_pl1_hit)
                ptallies["roster_need"].add([("all_t3",), ("fmt_t3", fmt)], rn_pl3_hit)

                rows.append({
                    "db_name": db_name, "year": year, "manager_key": mk,
                    "seq": int(g["seq"].iloc[i]), "round_bucket": rb, "fmt": fmt,
                    "chosen_pos": chosen, "chosen_player": pname[i], "has_prior": has_prior,
                    "pool_size": int(n - i),
                    "adp_pos1_hit": chosen == adp_pos1, "adp_pos3_hit": chosen in adp_pos3,
                    "adp_player1_hit": adp_pl1_hit, "adp_player3_hit": adp_pl3_hit,
                    "rn_pos1_hit": chosen == rn_pos1, "rn_pos3_hit": chosen in rn_pos3,
                    "rn_player1_hit": rn_pl1_hit, "rn_player3_hit": rn_pl3_hit,
                    "static_pos1_hit": chosen == static_pos1, "static_pos3_hit": chosen in static_pos3,
                    "naive_pos1_hit": chosen == naive_pos1,
                })
                have[mk][chosen] += 1
                if chosen in _POS_SET:
                    year_updates.append((mk, rb, chosen))

        # fold this year into history AFTER predicting it (leave-future-out)
        for mk, rb, p in year_updates:
            hist[mk][rb][p] += 1
            ghist[rb][p] += 1
        print(f"  year {year}: {len(df):>6} picks  (cum eval rows {len(rows):,})", flush=True)

    evaldf = pd.DataFrame(rows)
    summary = _summarize(tallies, ptallies, evaldf)
    return evaldf, summary


def _summarize(tallies: dict, ptallies: dict, evaldf: pd.DataFrame) -> dict:
    def rate(t: Tally, key: tuple) -> float:
        v, _ = t.rate(key)
        return round(v, 4) if v == v else None

    out: dict = {"n_picks": int(len(evaldf)), "position": {}, "player": {}, "by_split": {}}
    for name, t in tallies.items():
        out["position"][name] = {
            "top1_all": rate(t, ("all",)),
            "top3_all": rate(t, ("all_t3",)),
            "top1_snake": rate(t, ("fmt", "snake")),
            "top1_auction": rate(t, ("fmt", "auction")),
            "top1_managers_with_history": rate(t, ("prior", "yes")),
        }
    for name, t in ptallies.items():
        out["player"][name] = {
            "top1_all": rate(t, ("all",)),
            "top3_all": rate(t, ("all_t3",)),
            "top1_snake": rate(t, ("fmt", "snake")),
            "top1_auction": rate(t, ("fmt", "auction")),
        }
    # position top-1 by round bucket (adp vs naive vs static)
    for rb in ("r1", "r2", "r3", "r4_5", "r6_8", "r9plus"):
        out["by_split"][rb] = {
            n: rate(t, ("round", rb)) for n, t in tallies.items()
        }
    return out


def _print_report(summary: dict) -> None:
    print("\n" + "=" * 66)
    print(f"DRAFT-CHOICE BASELINES  -  {summary['n_picks']:,} pick decisions")
    print("=" * 66)
    print("\nPOSITION prediction (chosen position == predicted?)")
    print(f"{'predictor':14} {'top1':>7} {'top3':>7} {'snake':>7} {'auction':>8} {'w/hist':>8}")
    for name, m in summary["position"].items():
        print(f"{name:14} {_f(m['top1_all']):>7} {_f(m['top3_all']):>7} "
              f"{_f(m['top1_snake']):>7} {_f(m['top1_auction']):>8} "
              f"{_f(m['top1_managers_with_history']):>8}")
    print("\nPLAYER prediction (chosen player == predicted?)")
    print(f"{'predictor':14} {'top1':>7} {'top3':>7} {'snake':>7} {'auction':>8}")
    for name, m in summary["player"].items():
        print(f"{name:14} {_f(m['top1_all']):>7} {_f(m['top3_all']):>7} "
              f"{_f(m['top1_snake']):>7} {_f(m['top1_auction']):>8}")
    print("\nPOSITION top-1 by round bucket")
    hdr = "round " + " ".join(f"{n:>12}" for n in summary["by_split"]["r1"])
    print(hdr)
    for rb, m in summary["by_split"].items():
        print(f"{rb:6} " + " ".join(f"{_f(v):>12}" for v in m.values()))


def _f(v: object) -> str:
    return f"{v:.3f}" if isinstance(v, float) else "  -  "


def _parse_years(spec: str | None) -> list[int]:
    if not spec:
        return list(range(2003, 2027))
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in spec.split(",")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--years", help="e.g. 2020-2024 or 2019,2021 (default all)")
    ap.add_argument("--limit-leagues", type=int, help="cap distinct leagues (quick runs)")
    ap.add_argument("--out", default=None, help="output dir (default: scratchpad)")
    args = ap.parse_args()

    out_dir = Path(args.out) if args.out else Path(
        os.environ.get("CLAUDE_SCRATCHPAD",
                       r"C:\Users\joeye\AppData\Local\Temp\claude\d--yahoo-oauth"
                       r"\525cbdca-267e-4fd9-9215-ef9148ff3e9b\scratchpad"))
    out_dir.mkdir(parents=True, exist_ok=True)

    reader = FlyReader()
    years = _parse_years(args.years)
    print(f"fetching slots + drafting {years[0]}-{years[-1]} "
          f"(leagues cap={args.limit_leagues})...", flush=True)
    slots_map = fetch_slots(reader)
    print(f"  loaded roster slots for {len(slots_map):,} (league,year) pairs", flush=True)

    evaldf, summary = evaluate(reader, years, slots_map, args.limit_leagues)
    _print_report(summary)

    pq = out_dir / "draft_choice_eval.parquet"
    js = out_dir / "draft_choice_baselines.json"
    evaldf.to_parquet(pq, index=False)
    js.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nwrote {pq}\nwrote {js}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
