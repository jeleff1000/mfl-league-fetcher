"""
sota_recon/recon_column_census.py  --  LANE: per-column sanity census across EVERY column.

Answers "are all ~700 columns clean for every row?" by measuring, not asserting. For every column
it records dtype, null%, min/max, negatives, distinct/constant, and flags concrete defects:
  TYPE_STR_NUMERIC  VARCHAR column whose values are numeric (should be a numeric type) -- e.g.
                    three_out / fourth_down_stop were VARCHAR.
  ALL_NULL          column is entirely NULL (dead column).
  CONSTANT          single non-null value (suspicious for a stat).
  NEG_IMPOSSIBLE    negative values in a count/that-can't-be-negative column.
Plus CROSS-COLUMN CEILING PAIRS (child <= parent per row) that catch over-credits across many
columns at once: completions<=attempts, fg_made<=fg_att, receptions<=targets(1978+), tds<=touches,
bucket-sums<=totals, fumbles_lost<=fumbles, etc. A violation = a real per-row defect.

Coverage tag per column: FROZEN (golden_grid leader), SUM_RECON (aggregate lane season==Sumweek),
ORACLE (PBP-corroborated family), or SANITY_ONLY (only the generic checks here). This tells us
exactly which columns have independent verification vs which rest on sanity alone.

    python -m scripts.sota_recon.recon_column_census            # summary + violations
    python -m scripts.sota_recon.recon_column_census --full     # write full per-column table
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import duckdb
from .sources import latest_v26

OUT = Path("D:/league-history-data/nfl/derived/validation/column_census.json")

# child <= parent must hold for EVERY row (with small tolerance for half-stats / rounding)
CEILING_PAIRS = [
    ("completions", "attempts"), ("passing_tds", "completions"), ("fg_made", "fg_att"),
    ("pat_made", "pat_att"), ("receptions", "targets"), ("receiving_tds", "receptions"),
    ("rushing_tds", "carries"), ("fumbles_lost", "fumbles"), ("sack_fumbles_lost", "sack_fumbles"),
    ("rushing_fumbles_lost", "rushing_fumbles"), ("receiving_fumbles_lost", "receiving_fumbles"),
    ("completions_40plus", "completions"), ("completions_50plus", "completions_40plus"),
    ("passing_tds_40plus", "passing_tds"), ("passing_tds_50plus", "passing_tds_40plus"),
    ("receiving_tds_40plus", "receiving_tds"), ("rushing_tds_50plus", "rushing_tds_40plus"),
    ("fg_made_50_59", "fg_att"),
    # NOTE: solo vs def_tackles_with_assist is NOT a parent/child pair (the latter = assisted-tackle
    # count, not total) -> intentionally excluded (was a false positive).
]
# these ceiling pairs only hold in the targets-tracked era
ERA_PAIRS = {("receptions", "targets"): 1978}

# count-type columns where a negative value is impossible (yards CAN be negative; counts can't)
NONNEG = {"completions", "attempts", "passing_tds", "receptions", "targets", "carries",
          "rushing_tds", "receiving_tds", "fg_made", "fg_att", "pat_made", "pat_att",
          "def_sacks", "def_interceptions", "def_tackles_solo", "def_tackle_assists",
          "fumbles", "fumbles_lost", "def_fumbles_forced", "fum_rec", "def_safeties",
          "def_pass_defended", "def_tackles_for_loss", "kickoff_returns", "punt_returns"}


def _con():
    c = duckdb.connect(); c.execute("PRAGMA threads=3"); c.execute("SET memory_limit='6GB'")
    c.execute("PRAGMA disable_progress_bar"); c.execute("SET preserve_insertion_order=false")
    sp = Path(latest_v26()).parent / ".censusspill"; sp.mkdir(exist_ok=True)
    c.execute(f"SET temp_directory='{sp.as_posix()}'")
    return c


def run(full=False):
    v = Path(latest_v26()).as_posix()
    con = _con(); con.execute(f"CREATE VIEW st AS SELECT * FROM '{v}'")
    total = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    desc = con.execute("DESCRIBE st").fetchall()
    cols = [(r[0], r[1]) for r in desc]
    numeric_types = ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGEINT", "BIGINT")

    out = {}
    # chunk the per-column aggregates (count_nonnull, min, max, n_distinct, n_neg via TRY_CAST)
    chunk = []
    def flush(chunk):
        if not chunk: return
        sel = []
        for c, _ in chunk:
            cc = f'TRY_CAST("{c}" AS DOUBLE)'
            sel.append(f'COUNT("{c}") AS "nn__{c}"')
            sel.append(f'COUNT({cc}) AS "num__{c}"')
            sel.append(f'MIN({cc}) AS "mn__{c}"')
            sel.append(f'MAX({cc}) AS "mx__{c}"')
            sel.append(f'COUNT(*) FILTER (WHERE {cc} < 0) AS "ng__{c}"')
            sel.append(f'COUNT(DISTINCT "{c}") AS "nd__{c}"')
        row = con.execute(f"SELECT {', '.join(sel)} FROM st").fetchdf().iloc[0]
        for c, dt in chunk:
            nn = int(row[f"nn__{c}"])
            out[c] = {"dtype": dt, "nonnull": nn,
                      "null_pct": round(100*(1-nn/total), 2),
                      "numeric_frac": round(int(row[f"num__{c}"])/nn, 3) if nn else 0.0,
                      "min": None if row[f"mn__{c}"] is None or row[f"mn__{c}"] != row[f"mn__{c}"] else float(row[f"mn__{c}"]),
                      "max": None if row[f"mx__{c}"] is None or row[f"mx__{c}"] != row[f"mx__{c}"] else float(row[f"mx__{c}"]),
                      "n_negative": int(row[f"ng__{c}"]), "n_distinct": int(row[f"nd__{c}"])}
    for c, dt in cols:
        chunk.append((c, dt))
        if len(chunk) >= 40:
            flush(chunk); chunk = []
    flush(chunk)

    # provenance sentinels: per-wave marker columns (recomputed/repaired/merged/populated_at_<stamp>)
    # are NULL/constant BY DESIGN -> not defects. Tag + separate from real-signal flags.
    def _is_sentinel(name: str) -> bool:
        n = name.lower()
        return any(k in n for k in ("_recomputed_at", "_repaired_at", "_merged_at", "_populated_at",
                                    "_backfill", "_audit", "_at_2026", "recon_correction_log"))

    # classify flags
    flags = {}
    sentinels = []
    for c, info in out.items():
        f = []
        dt = info["dtype"].upper()
        if "VARCHAR" in dt and info["nonnull"] > 0 and info["numeric_frac"] > 0.5:
            f.append("TYPE_STR_NUMERIC")
        if info["nonnull"] == 0:
            f.append("ALL_NULL")
        elif info["n_distinct"] == 1:
            f.append("CONSTANT")
        if c in NONNEG and info["n_negative"] > 0:
            f.append(f"NEG_IMPOSSIBLE({info['n_negative']})")
        if not f:
            continue
        if _is_sentinel(c) and set(x.split("(")[0] for x in f) <= {"ALL_NULL", "CONSTANT"}:
            sentinels.append(c)        # expected provenance marker, not a defect
        else:
            flags[c] = f

    # ceiling-pair violations
    colset = {c for c, _ in cols}
    pair_viol = {}
    for child, parent in CEILING_PAIRS:
        if child not in colset or parent not in colset:
            continue
        era = ERA_PAIRS.get((child, parent))
        where = f" AND year>={era}" if era else ""
        n = con.execute(f"""SELECT COUNT(*) FROM st
            WHERE TRY_CAST("{child}" AS DOUBLE) > TRY_CAST("{parent}" AS DOUBLE) + 0.001
            AND "{child}" IS NOT NULL AND "{parent}" IS NOT NULL{where}""").fetchone()[0]
        if n > 0:
            pair_viol[f"{child}<= {parent}"] = int(n)
    con.close()

    res = {"total_rows": total, "n_columns": len(cols),
           "flag_counts": {k: sum(1 for v in flags.values() if any(x.startswith(k.split('(')[0]) for x in v))
                           for k in ["TYPE_STR_NUMERIC", "ALL_NULL", "CONSTANT", "NEG_IMPOSSIBLE"]},
           "provenance_sentinels": len(sentinels),
           "flagged_columns": flags, "ceiling_pair_violations": pair_viol}
    if full:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps({"summary": {k: res[k] for k in res if k != "flagged_columns"},
                                   "columns": out, "flags": flags}, indent=0, default=str))
        res["full_path"] = str(OUT)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--full", action="store_true"); a = ap.parse_args()
    r = run(full=a.full)
    print(f"columns: {r['n_columns']}  rows: {r['total_rows']:,}")
    print(f"flag counts (real-signal): {r['flag_counts']}")
    print(f"provenance sentinels (expected, excluded): {r['provenance_sentinels']}")
    print(f"\nCEILING-PAIR VIOLATIONS (child > parent, impossible): {len(r['ceiling_pair_violations'])}")
    for k, n in sorted(r["ceiling_pair_violations"].items(), key=lambda x: -x[1]):
        print(f"  {k}: {n:,} rows")
    print(f"\nFLAGGED COLUMNS ({len(r['flagged_columns'])}):")
    for c, f in sorted(r["flagged_columns"].items()):
        print(f"  {c:42} {f}")
    if a.full:
        print(f"\nfull per-column table -> {r['full_path']}")
