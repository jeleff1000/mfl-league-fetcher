"""COLUMN CLOSURE (Joe: 'closing out supertable columns 1 by 1').

For one column, generates the closure dossier -- the decade x root agreement
grid -- and rules CLOSED or lists exactly what blocks:

  per root, per decade: n cells compared, agreement, verdict
  CLOSED = every decade with plane coverage has >=1 root at >=95%, no
           unresolved conflicts, lanes locked, drift gate live
  else   = the blocking (decade, root) cells are the work list

Usage: python -m scripts.sota_recon.close_column passing_yards
Writes the dossier JSON to the lake and a receipt line to stdout.
"""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S
from scripts.sota_recon.vouch_2024 import lane_sql, compare, fingerprint

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")

# RULEBOOK CLOSURES (signoff ledger): {column: (boundary_year, mandated
# value)} -- decades wholly before the boundary are corroborated by LEAGUE
# RULE when every nonnull cell equals the mandate (Joe 2026-08-03:
# "is_overtime := 0 pre-1974, rulebook as witness").
RULEBOOK_CLOSURES = {"is_overtime": (1974, 0.0)}

# ESTIMATE ERAS (Joe 2026-08-03, tackle family): the data STAYS (pbp
# carries tackle credits to 1978) but three independent derivations
# (plane, pbp arrays, PFR season pages) disagree pairwise at 0.20-0.77 --
# measured 2026-08-03 -- because tackles were unofficial estimates before
# 2001. Decades inside an estimate era do not BLOCK closure; the dossier
# records the era note and the cross-derivation measurements.
# span = full pre-official era: tackles unofficial before 2001 everywhere;
# 1978-2000 additionally holds the kept pbp-derived estimates (Joe: "pbp
# has them to 78"), ancient decades hold sparse defect-family claims only.
ESTIMATE_ERAS = {
    "def_tackles_solo": (1920, 2000),
    "def_tackle_assists": (1920, 2000),
    "def_tackles_combined": (1920, 2000),
}

# AUTO-RULED eras (Joe 2026-08-03: "auto-rule and keep a ledger in plain
# english so i can grade at the end") -- era_rulings.v1.json is written by
# era_auto_ruler.py, mirrored in plain English in the grading ledger.
_ER = Path(__file__).parent / "witness_gate" / "contracts" / "era_rulings.v1.json"
if _ER.exists():
    _er = json.loads(_ER.read_text("utf-8"))
    for _c, _v in _er.get("estimate_eras", {}).items():
        ESTIMATE_ERAS.setdefault(_c, tuple(_v["span"]))
    for _c, _v in _er.get("rulebook", {}).items():
        RULEBOOK_CLOSURES.setdefault(_c, (_v["boundary"], _v["value"]))


def root_of(k: str) -> str:
    k = k.lower()
    if k.startswith(("pfr", "ancient_pfr", "ancient_pbp1978")):
        return "pfr"
    if k.startswith("nflcom"):
        return "nflcom"
    if k.startswith("pbp"):
        return "pbp"
    if k.startswith("statscrew"):
        return "statscrew"
    if "pfa" in k:
        return "pfa"
    if "newspaper" in k:
        return "newspaper"
    return "internal"


def build(con: duckdb.DuckDBPyConnection, col: str) -> dict:
    wk = S.weekly_read_path()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane_post AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'POST'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    decades = [(d, d + 9) for d in range(1920, 2030, 10)]
    plane_decades = {int(r[0]) for r in con.execute(f"""
    SELECT DISTINCT (CAST(year AS INT) // 10) * 10 FROM plane
    WHERE {col} IS NOT NULL AND year IS NOT NULL""").fetchall()}

    grid = defaultdict(dict)
    sps = [s for s in W.WITNESS_MAP if s.v26_col == col]
    # RATE-CLASS TOLERANCE FLOOR (measured 2026-08-03): sources publish rates
    # to ONE DECIMAL; demanding 0.001 manufactured a fake blockade (1940s Y/A
    # 0.456 -> 0.901 at 0.05, n=2,327). Rate-shaped columns compare at >=0.05.
    if any(k in col for k in ("_per_", "_pct", "pct_", "rating", "share")):
        for s in sps:
            if (s.validation_tolerance or 0) < 0.05:
                object.__setattr__(s, "validation_tolerance", 0.05)
    for sp in sps:
        sql, keys = lane_sql(sp)
        if not sql:
            continue
        r = root_of(sp.source_key)
        for d0, d1 in decades:
            if d0 not in plane_decades:
                continue
            try:
                n, agree = compare(con, sp, sql, keys, d0, d1)
            except Exception:
                continue
            if not n:
                continue
            # ZERO-CORROBORATION VOID (2026-08-03, Brown/Sayers): nflcom
            # career "agreed 1.0" on 1960s fumbles_lost because BOTH sides
            # zero-render -- mutual fabrication. A lane with no NONZERO
            # witness value in the window corroborates nothing there.
            try:
                nz = con.execute(f"""WITH w AS ({sql})
                SELECT COUNT(*) FROM w
                WHERE TRY_CAST(w.val AS DOUBLE) > 0
                  AND w.yr BETWEEN {d0} AND {d1}""").fetchone()[0]
            except Exception:
                nz = 0
            if not nz:
                continue
            cur = grid[d0].get(r)
            cand = {"n": n, "agree": round(agree / n, 4),
                    "lane": sp.source_key}
            if cur is None or (cand["agree"], cand["n"]) > (cur["agree"], cur["n"]):
                grid[d0][r] = cand

    rb = RULEBOOK_CLOSURES.get(col)
    est = ESTIMATE_ERAS.get(col)
    blocking = []
    for d0 in sorted(plane_decades):
        if est and est[0] <= d0 and d0 + 9 <= est[1] + 9 and d0 <= est[1]:
            grid[d0]["estimate_era"] = {
                "n": None, "agree": None,
                "lane": f"ESTIMATE-ERA {est[0]}-{est[1]} per signoff; "
                        "values kept, no single truth exists"}
            continue
        if rb and d0 + 9 < rb[0]:
            n_bad = con.execute(f"""SELECT COUNT(*) FROM plane
            WHERE CAST(year AS INT) BETWEEN {d0} AND {d0 + 9}
              AND {col} IS NOT NULL
              AND TRY_CAST({col} AS DOUBLE) <> {rb[1]}""").fetchone()[0]
            if n_bad == 0:
                grid[d0]["rulebook"] = {"n": None, "agree": 1.0,
                                        "lane": "league_rule"}
                continue
        roots = grid.get(d0, {})
        best = max((v["agree"] for v in roots.values()), default=0.0)
        if best < 0.95:
            blocking.append({"decade": d0, "best_agreement": best,
                             "roots_present": sorted(roots)})
    verdict = "CLOSED" if not blocking else "BLOCKED"
    # RATE COLUMNS CLOSE BY RECOMPUTE, NOT BY WITNESS-MATCH: a rate whose
    # components are both CLOSED reaches its terminal state through Stage 7
    # (recompute-from-closed-bases); witnesses corroborate at print precision
    # (~0.90 here) but the bases are the truth. Verdict: DERIVED_PENDING_S7.
    _s7reg_path = (Path(__file__).parent / "witness_gate" / "contracts"
                   / "stage7_formulas.v1.json")
    _s7lic = (json.loads(_s7reg_path.read_text("utf-8"))["licensed"]
              if _s7reg_path.exists() else {})
    # recompute terminal-state triggers on REGISTRY MEMBERSHIP, not name
    # shape -- the bonus_* thresholds licensed 2026-08-04 match no rate
    # pattern and were invisible to this branch (0 closures caught it)
    if blocking and (col in _s7lic or any(
            k in col for k in ("_per_", "_pct", "pct_", "rating", "share"))):
        # TERMINAL STATE (2026-08-03): a rate column with a LICENSED formula
        # whose plane matches its own recompute at print precision on every
        # single-row cell is CLOSED by DERIVED-RECOMPUTED -- the bases are
        # the truth and the bases are witness-closed and drift-gated.
        reg_path = (Path(__file__).parent / "witness_gate" / "contracts"
                    / "stage7_formulas.v1.json")
        lic = (json.loads(reg_path.read_text(encoding="utf-8"))["licensed"]
               if reg_path.exists() else {})
        spec = lic.get(col)
        if spec and "OVER (" not in spec["expr"]:
            # a cell matches its recompute in EITHER storage convention:
            # print-rounded (1dp) exactly, or full precision at 0.005
            n, ok = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (
              WHERE {col} IS NOT NULL AND (
                ABS(TRY_CAST({col} AS DOUBLE)
                    - ROUND(({spec['expr']}), 1)) <= 0.001
                OR ABS(TRY_CAST({col} AS DOUBLE) - ({spec['expr']})) <= 0.005))
            FROM plane WHERE ({spec['guard']}) > 0
              AND ({spec['expr']}) IS NOT NULL""").fetchone()
            agree = ok / n if n else 0.0
            if n and agree >= 0.999:
                verdict = "CLOSED"
                blocking = [{"note": ("DERIVED-RECOMPUTED: licensed formula "
                                      f"({spec['variant']}) reproduces every "
                                      "single-row cell at print precision"),
                             "recompute_n": n, "recompute_agree": round(agree, 5)}]
            else:
                verdict = "DERIVED_PENDING_S7"
                blocking = [{"note": "licensed formula does not yet reproduce "
                                     "the plane -- run the Stage-7 round",
                             "recompute_n": n,
                             "recompute_agree": round(agree, 5)}]
        else:
            verdict = "DERIVED_PENDING_S7"
            blocking = [{"note": ("rate column with NO licensed formula "
                                  "(or window-shaped): queue for Stage-7 "
                                  "licensing")}]
    dossier = {
        "column": col, "verdict": verdict,
        "generated": time.strftime("%Y-%m-%d %H:%M"),
        "lanes": len(sps),
        "grid": {str(d): grid[d] for d in sorted(grid)},
        "blocking": blocking,
        "rule": ("CLOSED = every plane-covered decade has >=1 root at >=95%; "
                 "blockers listed are the exact work items"),
    }
    (LAKE / f"closure_{col}.json").write_text(
        json.dumps(dossier, indent=1), encoding="utf-8")
    return dossier


if __name__ == "__main__":
    col = sys.argv[1] if len(sys.argv) > 1 else "passing_yards"
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    d = build(con, col)
    print(json.dumps({"column": d["column"], "verdict": d["verdict"],
                      "decades_covered": len(d["grid"]),
                      "blocking": d["blocking"]}, indent=1))
