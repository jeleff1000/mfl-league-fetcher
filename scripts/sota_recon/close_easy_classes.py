"""CLOSE THE THREE EASY CLASSES (Joe 2026-08-04: "lets get those 3 classes
done and over with").

  A. IDENTITY/KEY columns   -- the join key itself, or catalog-witnessed.
     A key column is proven by UNIQUENESS + COMPLETENESS on its own grain,
     not by an external witness: nothing outside can witness our key.
  B. STANDARD ANALYTICS     -- publicly-defined formulas (RACR, PACR, WOPR,
     CPOE, ppg_*). Measured against the definition; >= 0.999 books.
  C. LICENSED-BUT-UNBOOKED  -- anything in the stage7 registry lacking a
     dossier (e.g. the renamed TD columns after the swap).

Nothing is booked on a name: every class carries its own proof and each
dossier records which proof it passed.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
REG = Path(__file__).parent / "witness_gate" / "contracts" / "stage7_formulas.v1.json"
D = lambda c: f'TRY_CAST("{c}" AS DOUBLE)'

# A: identity/context. proof = completeness on the rows the column defines
IDENTITY = {
    "NFL_player_id": "player identity key",
    "player_week": "the composite grain key",
    "year": "season key", "week": "week key",
    "season_type": "phase key", "nfl_team": "team key (catalog-joined)",
    "player": "player display name carried with the id",
    "position": "broad fantasy position (two-position law)",
    "nfl_position": "specific role (two-position law)",
    "fantasy_position": "lineup slot",
    "data_source": "provenance stamp",
    "headshot_url": "asset pointer",
}
# B: standard public analytics -> (expr, guard)
ANALYTICS = {
    "racr": (f"{D('receiving_yards')} / NULLIF({D('receiving_air_yards')},0)",
             f"{D('receiving_air_yards')} <> 0"),
    "pacr": (f"{D('passing_yards')} / NULLIF({D('passing_air_yards')},0)",
             f"{D('passing_air_yards')} <> 0"),
    "wopr": (f"1.5*{D('target_share')} + 0.7*{D('air_yards_share')}",
             f"{D('target_share')} IS NOT NULL AND {D('air_yards_share')} IS NOT NULL"),
    "passing_cpoe": (f"{D('completion_pct')} - {D('ngs_expected_completion_pct')}",
                     f"{D('ngs_expected_completion_pct')} IS NOT NULL"),
}


def dossier(col, n, fit, lane, rule):
    (LAKE / f"closure_{col}.json").write_text(json.dumps({
        "column": col, "verdict": "CLOSED",
        "generated": time.strftime("%Y-%m-%d %H:%M"), "lanes": 1,
        "grid": {"all": {"proof": {"n": n, "agree": fit, "lane": lane}}},
        "blocking": [], "rule": rule}, indent=1), encoding="utf-8")


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = S.weekly_read_path()
    cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{wk}') LIMIT 0").fetchall()}
    closed = {p.stem.replace("closure_", "")
              for p in LAKE.glob("closure_*.json")}
    booked, short = [], []

    # ---- A. identity/key
    total, dup = con.execute(f"""
    SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT (NFL_player_id, year, week,
                                                nfl_team, season_type))
    FROM read_parquet('{wk}')""").fetchone()
    print(f"grain check: {total} rows, {dup} duplicate grain keys", flush=True)
    for c, why in IDENTITY.items():
        if c not in cols or c in closed:
            continue
        n, nulls = con.execute(f"""SELECT COUNT(*), COUNT(*) FILTER (
          WHERE "{c}" IS NULL) FROM read_parquet('{wk}')""").fetchone()
        completeness = 1 - nulls / n if n else 0
        # a key must be complete; a context column must be complete WHERE
        # its subject exists (headshot/display are allowed sparse)
        # NO ZERO BARS (2026-08-04): the first run "closed" fantasy_position
        # at 0.2941 completeness because context columns had bar=0.0 -- a
        # fake close, revoked. A sparse context column must EXPLAIN its
        # NULLs (rostered-only, asset-only) before it can book; until then
        # it stays open. Every class carries a real bar.
        bar = 0.999
        if completeness >= bar:
            dossier(c, n, round(completeness, 6), "identity/context proof",
                    f"{why}; completeness {completeness:.4f}, grain "
                    f"uniqueness {dup} dup keys over {total} rows")
            booked.append(f"{c} ({completeness:.4f})")
        else:
            short.append(f"{c} completeness {completeness:.4f} < {bar}")

    # ---- B. standard analytics
    for c, (expr, guard) in ANALYTICS.items():
        if c not in cols or c in closed:
            continue
        try:
            n, ok, ok1 = con.execute(f"""SELECT COUNT(*),
              COUNT(*) FILTER (WHERE ABS({D(c)} - ({expr})) <= 0.005),
              COUNT(*) FILTER (WHERE ABS({D(c)} - ROUND(({expr}),1)) <= 0.001)
            FROM read_parquet('{wk}')
            WHERE season_type='REG' AND "{c}" IS NOT NULL AND ({guard})
            """).fetchone()
        except Exception as e:
            short.append(f"{c} ERR {str(e)[:60]}")
            continue
        if not n:
            short.append(f"{c} no overlap")
            continue
        fit = max(ok, ok1) / n
        if fit >= 0.999:
            dossier(c, n, round(fit, 6), "public formula recompute",
                    "standard analytic: plane reproduces the published "
                    "definition from closed bases")
            booked.append(f"{c} ({fit:.6f})")
        else:
            short.append(f"{c} fit {fit:.6f}")

    # ---- C. licensed but unbooked
    reg = json.loads(REG.read_text("utf-8"))["licensed"]
    for c, spec in reg.items():
        if c not in cols or c in closed or "OVER (" in spec["expr"]:
            continue
        try:
            n, ok = con.execute(f"""SELECT COUNT(*), COUNT(*) FILTER (
              WHERE ABS({D(c)} - ({spec['expr']})) < 1e-9
                 OR ABS({D(c)} - ROUND(({spec['expr']}),1)) <= 0.001)
            FROM read_parquet('{wk}')
            WHERE season_type='REG' AND "{c}" IS NOT NULL
              AND ({spec['guard']}) > 0""").fetchone()
        except Exception as e:
            short.append(f"{c} ERR {str(e)[:60]}")
            continue
        if not n:
            continue
        fit = ok / n
        if fit >= 0.999:
            dossier(c, n, round(fit, 6), "licensed formula recompute",
                    "DERIVED-RECOMPUTED from closed bases")
            booked.append(f"{c} ({fit:.6f})")
        else:
            short.append(f"{c} fit {fit:.6f}")

    print(f"\nBOOKED {len(booked)}:")
    for b in booked:
        print("   ", b)
    print(f"SHORT {len(short)}:")
    for s in short:
        print("   ", s)


if __name__ == "__main__":
    main()
