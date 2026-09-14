#!/usr/bin/env python3
"""Step 1.5: the enriched candidate-grain dataset for the choice model.

The baselines (draft_choice_baselines.py) established the floor. This builds the
model's training input: one row per (pick decision x candidate player), with a
`chosen` label and the features that could explain WHY a manager took the player
they took over the rest of the board — the manager residual.

Grain: for each pick decision we keep the chosen player plus the top-K available
players by fleet ADP (the realistic board), each carried as a candidate row and
tagged with the same `pick_id`. Features fall in four groups:

  market     fleet ADP stage, reach-vs-current-pick, ADP rank on the board
  player     prior-season production tier, rookie flag, experience, age,
             NFL draft pedigree, RAS, college
  context    pick stage, round, roster-need flag, QB/pass-catcher STACK flag,
             format, superflex, league size
  manager    prior-years-only (leave-future-out) tendencies that let the model
             learn identity: this manager's rate of taking this position at this
             round, their mean reach-vs-ADP, their number of prior drafts

The manager_* columns are what separates the "market model" from the
"manager-aware model" downstream — an ablation is just dropping those columns.
All manager features use ONLY prior seasons, so the file is leak-free for a
leave-future-out split on `year`.

    python scripts/draft_choice_dataset.py --years 2018-2026
    python scripts/draft_choice_dataset.py --limit-leagues 200 --out /path
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# draft_choice_baselines lives beside this file; importing it also sets up the
# multi_league path + loads the repo-root .env (shared preamble). Reuse its
# pure helpers rather than duplicating them (CLAUDE.md: maximize common utils).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_choice_baselines import (  # noqa: E402
    DEFAULT_SLOTS, _POS_SET, canon_pos, needed_positions, round_bucket,
)
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402

TOPK_DEFAULT = 20
_ROOKIE_PPG = -1.0   # sentinel: no prior season (kept distinct from a true 0)


def fetch_year(reader: FlyReader, year: int) -> pd.DataFrame:
    """Non-keeper picks in `year`, draft order, joined to fleet ADP + prior
    season production + player bio (cross-db join runs on the ___leagues conn)."""
    sql = f"""
    WITH fleet AS (
        SELECT NFL_player_id, pick, MAX(pick) OVER (PARTITION BY db_name) AS mx
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
               d.pick, d.round, UPPER(NULLIF(d.nfl_team,'')) AS nfl_team,
               ROW_NUMBER() OVER (PARTITION BY d.db_name ORDER BY d.pick, d.round) AS seq,
               MAX(d.pick) OVER (PARTITION BY d.db_name) AS max_pick,
               AVG(CASE WHEN COALESCE(d.cost,0)>0 THEN 1.0 ELSE 0.0 END)
                   OVER (PARTITION BY d.db_name) AS auction_share
        FROM public.draft d
        WHERE d.year = {year} AND d.NFL_player_id IS NOT NULL
          AND COALESCE(d.is_keeper,0)=0 AND d.pick IS NOT NULL AND d.manager IS NOT NULL
    )
    SELECT p.db_name, p.manager_key, p.NFL_player_id, p.player, p.position,
           p.cost, p.pick, p.round, p.nfl_team, p.seq, p.max_pick, p.auction_share,
           a.adp_stage,
           ps.ppg_season_4pt_ppr AS prior_ppg, ps.games_played AS prior_games,
           pb.rookie_year, pb.birth_date, pb.college,
           pb.draft_round AS nfl_draft_round, pb.draft_overall AS nfl_draft_overall,
           pb.ras_score
    FROM picks p
    LEFT JOIN adp a ON p.NFL_player_id = a.NFL_player_id
    LEFT JOIN ___ops.nfl_historical.player_nfl_season ps
        ON p.NFL_player_id = ps.NFL_player_id AND ps.year = {year - 1}
    LEFT JOIN ___ops.nfl_historical.player_bio pb
        ON p.NFL_player_id = pb.NFL_player_id
    ORDER BY p.db_name, p.seq
    """
    return pd.DataFrame(reader.query(sql, database="___leagues"))


def fetch_settings(reader: FlyReader) -> dict[tuple[str, int], dict]:
    sql = """
    SELECT db_name, year, num_teams,
           COALESCE("roster_QB",0) qb, COALESCE("roster_RB",0) rb,
           COALESCE("roster_WR",0) wr, COALESCE("roster_TE",0) te,
           COALESCE("roster_K",0) k, COALESCE("roster_DEF",0) def,
           COALESCE("roster_FLX",0)+COALESCE("roster_W/R",0)
             +COALESCE("roster_R/T",0) AS flex,
           COALESCE("roster_REC_FLEX",0) AS rec_flex,
           COALESCE("roster_SUPER_FLEX",0) AS superflex
    FROM public.league_settings WHERE year IS NOT NULL
    """
    out: dict[tuple[str, int], dict] = {}
    for r in reader.query(sql, database="___leagues"):
        d = {k: int(r.get(k) or 0) for k in DEFAULT_SLOTS}
        d["num_teams"] = int(r.get("num_teams") or 0)
        d["superflex_flag"] = 1 if (d["superflex"] > 0 or d["qb"] >= 2) else 0
        out[(str(r["db_name"]), int(r["year"]))] = d
    return out


SCHEMA = pa.schema([
    ("pick_id", pa.string()), ("db_name", pa.string()), ("year", pa.int32()),
    ("manager_key", pa.string()), ("seq", pa.int32()), ("fmt", pa.string()),
    ("cand_nflid", pa.string()), ("chosen", pa.int8()),
    ("cand_pos", pa.string()),
    # market
    ("adp_stage", pa.float32()), ("reach", pa.float32()), ("adp_rank", pa.int32()),
    ("adp_known", pa.int8()),
    # player
    ("prior_pos_pctl", pa.float32()), ("is_rookie", pa.int8()),
    ("experience", pa.float32()), ("age", pa.float32()),
    ("nfl_draft_overall", pa.float32()), ("ras_score", pa.float32()),
    # context
    ("pick_stage", pa.float32()), ("round_bucket", pa.string()),
    ("pos_is_need", pa.int8()),
    # NOTE: QB/pass-catcher STACK dropped from v1 — draft.nfl_team is 0% populated;
    # a stack flag needs a player_nfl_season(year=Y).nfl_team join (future v2).
    ("superflex", pa.int8()), ("num_teams", pa.int32()),
    ("roster_have_pos", pa.int32()),
    # manager (prior-years only)
    ("mgr_pos_rate", pa.float32()), ("mgr_mean_reach", pa.float32()),
    ("mgr_n_prior", pa.int32()),
])


def _year_pctl(df: pd.DataFrame) -> pd.Series:
    """Within (canon position) for this year, percentile of prior-season ppg.
    Rookies / no-prior-season rows stay NaN (flagged separately)."""
    ppg = pd.to_numeric(df["prior_ppg"], errors="coerce")
    tmp = pd.DataFrame({"pos": df["cpos"].values, "ppg": ppg.values})
    return tmp.groupby("pos")["ppg"].rank(pct=True)


def build(reader: FlyReader, years: list[int], topk: int,
          include_auction: bool, limit_leagues: int | None, out_path: Path) -> dict:
    settings = fetch_settings(reader)
    writer = pq.ParquetWriter(out_path, SCHEMA)
    # prior-years-only manager accumulators (leave-future-out)
    hist: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    mreach: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])   # [sum, n]
    mdrafts: dict[str, set] = defaultdict(set)                          # years seen
    keep_dbs: set[str] | None = None
    stats = {"pick_decisions": 0, "cand_rows": 0, "feat_nonnull": Counter()}

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
        df["prior_pctl"] = _year_pctl(df)
        birth_yr = pd.to_datetime(df["birth_date"], errors="coerce").dt.year
        df["age_yr"] = (year - birth_yr).astype(float)
        rookie_yr = pd.to_numeric(df["rookie_year"], errors="coerce")
        df["exp_yr"] = (year - rookie_yr).astype(float)
        year_updates: list[tuple] = []
        batch: list[dict] = []

        for db_name, g in df.groupby("db_name", sort=False):
            g = g.sort_values("seq")
            n = len(g)
            pos = g["cpos"].to_numpy()
            adp = g["adp"].to_numpy(dtype=float)
            adp_inf = np.where(np.isnan(adp), np.inf, adp)
            mkey = g["manager_key"].to_numpy()
            nflid = g["NFL_player_id"].to_numpy()
            seq = g["seq"].to_numpy()
            max_pick = float(g["max_pick"].iloc[0]) or float(n)
            is_auction = float(g["auction_share"].iloc[0]) >= 0.25
            if is_auction and not include_auction:
                continue
            fmt = "auction" if is_auction else "snake"
            st = settings.get((db_name, year), DEFAULT_SLOTS)
            superflex = st.get("superflex_flag", 0)
            num_teams = st.get("num_teams", 0)
            prior_pctl = g["prior_pctl"].to_numpy(dtype=float)
            age = g["age_yr"].to_numpy(dtype=float)
            exp = g["exp_yr"].to_numpy(dtype=float)
            ndo = pd.to_numeric(g["nfl_draft_overall"], errors="coerce").to_numpy(dtype=float)
            ras = pd.to_numeric(g["ras_score"], errors="coerce").to_numpy(dtype=float)
            prior_ppg = pd.to_numeric(g["prior_ppg"], errors="coerce").to_numpy(dtype=float)

            have: dict[str, Counter] = defaultdict(Counter)          # mgr -> pos counts

            for i in range(n):
                mk = mkey[i]
                chosen_pos = pos[i]
                rb = round_bucket(g["round"].iloc[i])
                stage_i = seq[i] / max_pick
                # candidate pool = available (seq >= i); sample top-K by ADP + chosen(0)
                cand_adp = adp_inf[i:]
                order = np.argsort(cand_adp, kind="stable")
                board_rank = np.empty(len(order), dtype=int)
                board_rank[order] = np.arange(len(order))          # 0 = best available
                sample_local = sorted(set(order[:topk].tolist()) | {0})

                need = needed_positions(have[mk], st)
                # manager prior-year features
                mrows = hist.get(mk, {})
                mrb = mrows.get(rb) if mrows else None
                mrb_tot = sum(mrb.values()) if mrb else 0
                rsum, rn = mreach[mk]
                mgr_mean_reach = (rsum / rn) if rn else np.nan
                mgr_n_prior = len(mdrafts[mk])

                for loc in sample_local:
                    j = i + loc
                    cpos = pos[j]
                    a = adp[j]
                    pos_rate = (mrb.get(cpos, 0) / mrb_tot) if mrb_tot else np.nan
                    batch.append({
                        "pick_id": f"{db_name}|{year}|{seq[i]}",
                        "db_name": db_name, "year": year, "manager_key": mk,
                        "seq": int(seq[i]), "fmt": fmt,
                        "cand_nflid": nflid[j], "chosen": 1 if loc == 0 else 0,
                        "cand_pos": cpos,
                        "adp_stage": a, "reach": (a - stage_i) if not np.isnan(a) else np.nan,
                        "adp_rank": int(board_rank[loc]), "adp_known": 0 if np.isnan(a) else 1,
                        "prior_pos_pctl": prior_pctl[j],
                        "is_rookie": 1 if np.isnan(prior_ppg[j]) else 0,
                        "experience": exp[j], "age": age[j],
                        "nfl_draft_overall": ndo[j], "ras_score": ras[j],
                        "pick_stage": stage_i, "round_bucket": rb,
                        "pos_is_need": 1 if cpos in need else 0,
                        "superflex": superflex, "num_teams": num_teams,
                        "roster_have_pos": int(have[mk].get(cpos, 0)),
                        "mgr_pos_rate": pos_rate, "mgr_mean_reach": mgr_mean_reach,
                        "mgr_n_prior": mgr_n_prior,
                    })
                stats["pick_decisions"] += 1
                # advance roster state with the ACTUAL pick
                have[mk][chosen_pos] += 1
                if chosen_pos in _POS_SET:
                    reach_actual = (adp[i] - stage_i) if not np.isnan(adp[i]) else None
                    year_updates.append((mk, rb, chosen_pos, reach_actual, year))

        if batch:
            _write_batch(writer, batch)
            stats["cand_rows"] += len(batch)
            bdf = pd.DataFrame(batch)
            for c in ("adp_stage", "prior_pos_pctl", "age", "experience",
                      "ras_score", "nfl_draft_overall", "mgr_pos_rate"):
                stats["feat_nonnull"][c] += int(bdf[c].notna().sum())
        # fold this year into prior-years history AFTER building it
        for mk, rb, p, reach_actual, yr in year_updates:
            hist[mk][rb][p] += 1
            mdrafts[mk].add(yr)
            if reach_actual is not None:
                mreach[mk][0] += reach_actual
                mreach[mk][1] += 1
        print(f"  year {year}: {stats['pick_decisions']:>7,} decisions  "
              f"{stats['cand_rows']:>9,} cand rows", flush=True)

    writer.close()
    return stats


def _write_batch(writer: pq.ParquetWriter, batch: list[dict]) -> None:
    bdf = pd.DataFrame(batch)
    # coerce to schema (float32 nullable via pyarrow from_pandas)
    tbl = pa.Table.from_pandas(bdf, schema=SCHEMA, preserve_index=False)
    writer.write_table(tbl)


def _parse_years(spec: str | None) -> list[int]:
    if not spec:
        return list(range(2003, 2027))
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in spec.split(",")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--years", help="e.g. 2018-2026 (default all 2003-2026)")
    ap.add_argument("--topk", type=int, default=TOPK_DEFAULT, help="negatives per pick")
    ap.add_argument("--include-auction", action="store_true", help="also emit auction drafts")
    ap.add_argument("--limit-leagues", type=int)
    ap.add_argument("--out", default=None, help="output dir (default scratchpad)")
    args = ap.parse_args()

    out_dir = Path(args.out) if args.out else Path(
        r"C:\Users\joeye\AppData\Local\Temp\claude\d--yahoo-oauth"
        r"\525cbdca-267e-4fd9-9215-ef9148ff3e9b\scratchpad\dcb_dataset")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "draft_candidates.parquet"

    reader = FlyReader()
    years = _parse_years(args.years)
    print(f"building candidate dataset {years[0]}-{years[-1]}  topk={args.topk}  "
          f"auction={args.include_auction}  leagues<={args.limit_leagues}", flush=True)
    stats = build(reader, years, args.topk, args.include_auction,
                  args.limit_leagues, out_path)

    print(f"\n{stats['pick_decisions']:,} pick decisions -> "
          f"{stats['cand_rows']:,} candidate rows  ({out_path})")
    if stats["cand_rows"]:
        print("feature coverage (non-null share of candidate rows):")
        for c, nn in stats["feat_nonnull"].most_common():
            print(f"  {c:20} {nn / stats['cand_rows']:.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
