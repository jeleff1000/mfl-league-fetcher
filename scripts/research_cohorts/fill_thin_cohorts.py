"""fill_thin_cohorts.py -- borrow a player's own value from a fatter cohort into a thin one.

A thin cell is not a small number, it is a NOISY one: the ladder's own reliability curve puts
po_final at r=0.155 when n=50 and r=0.708 at n=3,200. Serving the unfilled cell is serving
noise; the fill moves the cell along that curve using the SAME PLAYER's measurement elsewhere.

WHAT MAY BE BORROWED, and why each is safe -- all measured paired-within-player, same player,
same season, bracket held fixed, 10 teams vs 12 teams (n=2,332..5,833 player-years per cell):

    start_pct  1.037 / 1.058 / 1.036 across brackets 4/6/8   -> straight copy
    win_pct    0.976 / 1.012 / 1.001                          -> straight copy
    clutch     -0.000 / 0.000 / -0.019 (level -0.04..-0.12)   -> straight copy (additive)
    champ      0.923 / 1.013 / 0.848, x(1/N)-scaled           -> scale by teams ratio
    playoff    carries the structural term                    -> scale by E[s] ratio

No tier guards, deliberately. A metric either translates uniformly at the player level or it
is not borrowed at all; special-casing a tier is how a fill starts encoding the analyst's
priors instead of the population's behaviour. Tier checks were run on an INDEPENDENT variable
(usage) precisely because tiering on the metric under test manufactures regression-to-mean --
that artifact produced two wrong verdicts before it was caught.

RATE SPACE, not odds space: averaging commutes with rate = lift x s but not with logits, and
the cohort-level moves here are 1.15-1.32, well short of the 1.5-2.4 jumps where the rate rule
over-predicted elite players. The support gate covers the remainder.

Filled values are written IN PLACE and are NOT distinguishable in the served payload (Joe,
2026-07-29). Provenance (n_own / n_borrowed / donor cell) is retained in a sidecar for audit.

    py -3 scripts/research_cohorts/fill_thin_cohorts.py \
        --season <assembled season parquet> --struct <struct lattice> \
        --thresholds <ladder_thresholds.json> --out <dir>
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

BASE = ["teams", "roster", "ppr", "td", "league_type", "lineup_mode", "keeper_mode"]

# (column, how) -- how in {"copy", "teams_ratio", "struct_ratio"}
#
# Start% is deliberately absent.  The native paired audit showed that copying it from
# the slot-preserving donor still leaves 21.17% of supported cells more than 10 points
# away and a 4.31-point 10t/12t bias.  It needs the position/slot model (or remains NULL),
# not generic proliferation.
FILLABLE = [
    ("won_pct", "copy"),
    ("win_rate_pct", "copy"),
    ("avg_clutch_started", "copy"),
    ("champ_total_pct", "teams_ratio"),
    ("champ_as_starter_pct", "teams_ratio"),
    ("playoff_total_pct", "struct_ratio"),
    ("playoff_as_starter_pct", "struct_ratio"),
]

# ladder metric -> the served columns it governs; target n is the curve's plateau, not a
# target r: po_final never reaches r=0.75 at any tested n, so gating on r would fill nothing.
METRIC_FOR = {
    "start_rate_pct": "start_pct", "won_pct": "win_pct", "win_rate_pct": "win_pct",
    "avg_clutch_started": "clutch", "champ_total_pct": "champ",
    "champ_as_starter_pct": "champ", "playoff_total_pct": "po_final",
    "playoff_as_starter_pct": "po_started",
}


def plateau_n(thresholds: dict, metric: str, default: int = 3200) -> int:
    """n at which the reliability curve stops improving -- past it, filling buys nothing."""
    curve = thresholds.get("metrics", {}).get(metric, {}).get("curve")
    if not curve:
        return default
    pts = sorted((int(k), float(v)) for k, v in curve.items())
    best_n, best_r = pts[0]
    for n, r in pts:
        if r > best_r + 1e-9:
            best_n, best_r = n, r
    return best_n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=Path, required=True)
    ap.add_argument("--struct", type=Path, required=True)
    ap.add_argument("--thresholds", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--min-donor-leagues", type=int, default=50,
                    help="a donor thinner than this is not worth borrowing from")
    ap.add_argument("--min-struct-leagues", type=int, default=25,
                    help="refuse a multiplier resting on fewer league-years than this")
    ap.add_argument("--pos-map", type=Path, default=None,
                    help="(NFL_player_id, year, pos) parquet; enables the QB slot formula")
    ap.add_argument("--slot-width", type=float, default=9.0,
                    help="logistic width w for the slot projection (median fitted w=9.0; "
                         "p10-p90 was 5-20, so this is the one-parameter simplification)")
    args = ap.parse_args()

    thresholds = json.loads(args.thresholds.read_text(encoding="utf-8"))
    targets = {col: plateau_n(thresholds, METRIC_FOR[col]) for col, _ in FILLABLE}
    print("[fill] plateau targets:", targets)

    con = duckdb.connect()
    con.execute("SET memory_limit='6000MB'; SET threads=4; SET preserve_insertion_order=false;")
    con.execute(f"CREATE VIEW s AS SELECT * FROM read_parquet('{args.season.as_posix()}')")
    con.execute(f"CREATE VIEW st AS SELECT * FROM read_parquet('{args.struct.as_posix()}')")

    have = {r[0] for r in con.execute("DESCRIBE s").fetchall()}
    fillable = [(c, how) for c, how in FILLABLE if c in have]
    missing = [c for c, _ in FILLABLE if c not in have]
    if missing:
        print(f"[fill] not present in season parquet, skipped: {missing}")
    if not fillable:
        raise SystemExit("[fill] no fillable columns present -- refusing to write a no-op")

    join_st = " AND ".join(f"st.{c}=s.{c}" for c in BASE) + " AND st.year=s.year"
    con.execute(f"""
      CREATE TABLE cells AS
      SELECT s.*, st.mean_s, st.max_s, st.n_leagues AS struct_lg,
             TRY_CAST(REPLACE(s.teams,'t','') AS INTEGER) AS teams_n
      FROM s LEFT JOIN st ON {join_st}
    """)

    n_all, n_s = con.execute(
        "SELECT COUNT(*), COUNT(mean_s) FROM cells").fetchone()
    print(f"[fill] {n_all:,} season rows; structural rate resolved for {n_s:,} "
          f"({100.0*n_s/max(n_all,1):.1f}%)")

    # donor = the same player-year's fattest cohort cell that is itself above the floor
    exprs, prov = [], []
    for col, how in fillable:
        tgt = targets[col]
        if how == "copy":
            borrowed = f"d.{col}"
        elif how == "teams_ratio":
            borrowed = f"d.{col} * (d.teams_n * 1.0 / NULLIF(c.teams_n,0))"
        else:
            borrowed = f"d.{col} * (c.mean_s / NULLIF(d.mean_s,0))"
        # support gate: never publish past what the target cohort has actually shown
        cap = "100.0" if col.endswith("_pct") else "1e30"
        exprs.append(f"""
          CASE WHEN c.n_leagues < {tgt} AND d.{col} IS NOT NULL
                    AND ({borrowed}) IS NOT NULL AND ({borrowed}) <= {cap}
                    AND (c.mean_s IS NOT NULL OR '{how}' <> 'struct_ratio')
                    AND (c.struct_lg >= {args.min_struct_leagues} OR '{how}' = 'copy')
               THEN {borrowed} ELSE c.{col} END AS {col}""")
        prov.append(f"""
          CASE WHEN c.n_leagues < {tgt} AND d.{col} IS NOT NULL THEN 1 ELSE 0 END
            AS filled_{col}""")

    keep = [c for c in have if c not in {col for col, _ in fillable}]
    con.execute(f"""
      CREATE TABLE filled AS
      WITH donor AS (
        -- The SLOT-PRESERVING pool (Joe 2026-07-29). cohort_level=2 / format_level=0 keeps
        -- `teams` and `roster` resolved while rolling ppr / td / league_type / keeper_mode
        -- to ALL. That is deliberate: the cohort key already stratifies 10t/12t and flx/sflx
        -- BECAUSE those set the position's starting-slot count, and slots are what a metric
        -- actually scales with -- total QB starts per league-week measured 12.18 / 11.92 /
        -- 12.16 across dynasty / keeper / redraft at 12 teams x 1 QB, i.e. conserved. Within
        -- a fixed slot count a player is portable across the other dimensions (2024 QBs,
        -- half vs full PPR on 1,273 / 2,264 leagues each: 0.87-0.97 for every established
        -- starter). Pooling from cohort_level=0 instead would mix 10-, 12- and 24-QB-slot
        -- leagues into one donor and destroy exactly the normalisation the stratification
        -- exists to provide.
        SELECT * FROM cells
        WHERE cohort_level = 2 AND format_level = 0
          AND n_leagues >= {args.min_donor_leagues}
      )
      SELECT {', '.join('c.'+c for c in keep)},
             {', '.join(exprs)},
             {', '.join(prov)},
             c.n_leagues AS n_own,
             COALESCE(d.n_leagues, 0) AS n_donor
      FROM cells c
      LEFT JOIN donor d ON d.NFL_player_id=c.NFL_player_id AND d.year=c.year
                        AND d.teams=c.teams AND d.roster=c.roster
                        AND NOT (c.cohort_level = 2 AND c.format_level = 0)
    """)

    # ---- QB start%: project along the SLOT axis instead of copying ---------------------
    # Managed-lineup QB start% is a logistic in total league-wide QB slots:
    #     start%(S) = 1 / (1 + exp(-(S - r)/w))
    # so with w fixed the projection is a plain logit shift and needs no per-player fit:
    #     start%_target = logistic( logit(start%_donor) + (S_target - S_donor)/w )
    # S is exact for QB from the cohort key -- 10t/12t x flx(1 QB)/sflx(2 QB) -- which is
    # why QB is wired first: every other position needs the fitted flex share before S is
    # even defined. Fitted per player-year on 261 curves: median RMSE 0.0295, median w 9.0.
    # w varies p10-p90 5-20, so this trades per-player width accuracy for the ability to
    # place a player from a SINGLE observation.
    if args.pos_map and "start_rate_pct" in have:
        w = args.slot_width
        con.execute(f"CREATE VIEW pm AS SELECT * FROM read_parquet('{args.pos_map.as_posix()}')")
        con.execute(f"""
          CREATE OR REPLACE TABLE filled AS
          WITH slots AS (
            SELECT *,
                   TRY_CAST(REPLACE(teams,'t','') AS INTEGER)
                     * (CASE WHEN roster='sflx' THEN 2 ELSE 1 END) AS qb_slots
            FROM filled
          ),
          d AS (
            SELECT NFL_player_id, year, qb_slots AS donor_slots,
                   start_rate_pct AS donor_start
            FROM slots WHERE cohort_level = 2 AND format_level = 0
          )
          SELECT s.* EXCLUDE (start_rate_pct, qb_slots),
                 CASE
                   WHEN pm.pos = 'QB' AND d.donor_start IS NOT NULL
                        AND s.qb_slots IS NOT NULL AND d.donor_slots IS NOT NULL
                        AND d.donor_start > 0.05 AND d.donor_start < 99.95
                   THEN 100.0 / (1.0 + exp(-(
                          ln((d.donor_start/100.0)/(1.0 - d.donor_start/100.0))
                          + (s.qb_slots - d.donor_slots) / {w}.0 )))
                   ELSE s.start_rate_pct
                 END AS start_rate_pct
          FROM slots s
          LEFT JOIN pm ON pm.NFL_player_id = s.NFL_player_id AND pm.year = s.year
          LEFT JOIN d  ON d.NFL_player_id  = s.NFL_player_id AND d.year  = s.year
        """)
        n_qb = con.execute("""SELECT COUNT(*) FROM filled f JOIN pm ON
                              pm.NFL_player_id=f.NFL_player_id AND pm.year=f.year
                              WHERE pm.pos='QB'""").fetchone()[0]
        print(f"[fill] QB slot projection applied over {n_qb:,} QB rows (w={w})")

    args.out.mkdir(parents=True, exist_ok=True)
    out = args.out / "research_matchup_player_season_filled.parquet"
    drop = ", ".join(f"filled_{c}" for c, _ in fillable) + ", n_own, n_donor"
    con.execute(f"COPY (SELECT * EXCLUDE ({drop}) FROM filled) TO '{out.as_posix()}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)")
    sidecar = args.out / "research_matchup_fill_provenance.parquet"
    con.execute(f"""COPY (SELECT NFL_player_id, year, {', '.join(BASE)},
                   n_own, n_donor, {', '.join('filled_'+c for c,_ in fillable)}
                   FROM filled) TO '{sidecar.as_posix()}' (FORMAT PARQUET)""")

    print(f"[fill] -> {out}")
    print(f"[fill] provenance -> {sidecar}")
    print(con.execute(f"""
      SELECT {', '.join(f'SUM(filled_{c}) AS {c}' for c,_ in fillable)},
             COUNT(*) AS rows
      FROM filled""").fetchdf().to_string(index=False))
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
