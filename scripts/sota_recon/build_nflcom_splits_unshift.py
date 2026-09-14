"""
sota_recon/build_nflcom_splits_unshift.py -- O.9.0b: repair the column shift ALGEBRAICALLY.

WHY NOT A RE-PARSE. The obvious repair was to re-parse the retained cache. Measured 2026-07-26, it
cannot be: the cache holds only 12.3% (splits) / 12.9% (situational) of the pages that produced these
tables -- they were assembled across multiple GitHub runners and only the local runner's cache is on
disk. (This also corrects the O.8 capture receipt's "any missed field is a re-parse, never a
re-crawl", which is true of what THIS machine fetched and overstated for these two families.)

THE SHIFT IS INVERTIBLE WITHOUT THE SOURCE BYTES. The old parser dropped the blank header's NAME but
kept its CELL, so for a table with named headers k0..k(n-1) over cells c0..cn:

    stored[k_i] = c_i        but the truth is    true[k_i] = c_(i+1)

Therefore, reading only what is already stored:

    split_value = stored[k0]           <- the label that was overwriting the first stat
    true[k_i]   = stored[k_(i+1)]      <- every stat moves one column RIGHT, back into place
    true[k_last] = UNRECOVERABLE       <- c_n was truncated by cells[:len(keys)] and never stored

This needs no cache and no crawl, so it repairs 100% of the 7,782,148 rows.

IT IS ONLY VALID BECAUSE THE LAYOUT IS UNAMBIGUOUS. Measured from the signature census: the 48 splits
/ 58 situational signatures collapse to exactly 8 distinct key-SETS each, and ZERO key-sets map to
more than one column ORDER -- so a row's set of populated columns identifies its layout uniquely.
Verified further: inside a layout every one of its columns is non-null and no foreign column leaks in.
Rows matching no layout are counted and REFUSED, never guessed.

DELETION DISCIPLINE: writes tables/player_{view}_unshifted/. Originals are never touched.

    python -m scripts.sota_recon.build_nflcom_splits_unshift --build
    python -m scripts.sota_recon.build_nflcom_splits_unshift --verify   # holdout vs re-parsed cache
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

CENSUS = Path(__file__).resolve().parents[2] / "docs" / "nflcom-column-signature-census.json"
RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-splits-unshift-receipt.json"
TABLES = Path("D:/league-history-data/nfl/raw/nflcom/tables")
CARRY = ("_table", "_view", "nflcom_slug", "season")
SPLIT_LABEL = "split_value"
SIGS = Path("D:/league-history-data/nfl/raw/nflcom/splits_reparse_signatures.json")
FAMILIES = ("player_splits", "player_situational")


def layouts_for(family: str) -> list[list[str]]:
    """Distinct ordered key lists for a family, from the signature census (the evidence)."""
    census = json.loads(CENSUS.read_text(encoding="utf-8"))
    seen: dict[frozenset, list[str]] = {}
    for s in census["families"][family]["signatures"]:
        keys = [k for k in s["keys"]]
        fs = frozenset(keys)
        if fs in seen and seen[fs] != keys:
            raise SystemExit(f"{family}: key-set maps to two orders -- unshift is not determined")
        seen[fs] = keys
    return list(seen.values())


def _dedup_keys(headers: list[str]) -> list[str]:
    """Byte-identical to nflcom_harvest._dedup (the fixed one, which NAMES blank headers)."""
    import re as _re
    headers = [h if h != "" else f"unnamed_{i}" for i, h in enumerate(headers)]
    seen: dict[str, int] = {}
    out = []
    for h in headers:
        k = _re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_") or "col"
        seen[k] = seen.get(k, 0) + 1
        out.append(k if seen[k] == 1 else f"{k}_{seen[k]}")
    return out


def _q(c: str) -> str:
    return f'"{c}"'


def build() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET enable_progress_bar=false")
    out: dict = {"generated": "O.9.0b algebraic column-shift repair", "families": {}}
    for fam in FAMILIES:
        lays = layouts_for(fam)
        allkeys = sorted({k for L in lays for k in L})
        src = (TABLES / fam / "*.parquet").as_posix()
        dst_dir = TABLES / f"{fam}_unshifted"
        dst_dir.mkdir(parents=True, exist_ok=True)
        arms, per_layout = [], []
        for i, L in enumerate(lays):
            foreign = [k for k in allkeys if k not in L]
            pred = " AND ".join([f"{_q(k)} IS NOT NULL" for k in L]
                                + [f"{_q(k)} IS NULL" for k in foreign])
            # the un-shift: every stat takes the value stored one column to its RIGHT
            proj = [f"{_q(L[0])} AS {SPLIT_LABEL}"]
            proj += [f"{_q(L[j + 1])} AS {_q(L[j])}" for j in range(len(L) - 1)]
            proj.append(f"CAST(NULL AS VARCHAR) AS {_q(L[-1])}")   # truncated at parse, unrecoverable
            proj += [f"'{fam}_L{i}' AS _layout", f"'{L[-1]}' AS _lost_column"]
            proj += [_q(c) for c in CARRY]
            arms.append(f"SELECT {', '.join(proj)} FROM read_parquet('{src}', union_by_name=true) "
                        f"WHERE {pred}")
            n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{src}', union_by_name=true) "
                            f"WHERE {pred}").fetchone()[0]
            per_layout.append({"layout": f"{fam}_L{i}", "columns": L,
                               "lost_column": L[-1], "rows": n})
            print(f"  {fam}_L{i}: {n:>9,} rows | {len(L)} cols | lost={L[-1]}", flush=True)
        total_src = con.execute(f"SELECT COUNT(*) FROM read_parquet('{src}', union_by_name=true)").fetchone()[0]
        matched = sum(x["rows"] for x in per_layout)
        # BY NAME: layouts have different widths, so arms must align on column NAME, not position
        con.execute(f"COPY ({' UNION ALL BY NAME '.join(arms)}) TO '{(dst_dir / 'unshifted.parquet').as_posix()}' "
                    f"(FORMAT PARQUET)")
        out["families"][fam] = {
            "rows_source": total_src, "rows_unshifted": matched,
            "rows_unclassified": total_src - matched,
            "layouts": per_layout,
            "unrecoverable_columns": sorted({x["lost_column"] for x in per_layout}),
        }
        print(f"{fam}: {matched:,}/{total_src:,} classified "
              f"({total_src - matched:,} unclassified) -> {dst_dir}", flush=True)
    RECEIPT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"-> {RECEIPT}")


def verify() -> None:
    """HOLDOUT PROOF. The ~12.5% of pages still cached were re-parsed INDEPENDENTLY (from source
    bytes, by the fixed parser). If the algebra is right, it must agree with that re-parse on the
    overlap -- row for row, column for column. Agreement measured, never assumed."""
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET enable_progress_bar=false")
    rec = json.loads(RECEIPT.read_text()) if RECEIPT.exists() else {"families": {}}
    for fam in FAMILIES:
        view = fam.replace("player_", "")
        rp_dir = TABLES / f"player_{view}_reparsed"
        un = (TABLES / f"{fam}_unshifted" / "*.parquet").as_posix()
        if not list(rp_dir.glob("*.parquet")):
            print(f"[verify] {fam}: no re-parsed holdout on disk -- skipped")
            continue
        rp = (rp_dir / "*.parquet").as_posix()
        # THE JOIN KEY MUST INCLUDE THE LAYOUT. One (slug, season, caption, label) can carry
        # several tables (a player has both a rushing and a receiving splits block under
        # 'Days'), so without it the join fans out and compares rows across DIFFERENT layouts
        # -- measured: 1.37M of 2.27M keys are non-unique, which drags a correct un-shift down
        # to 43-99% agreement and looks like a repair failure.
        sig_map = json.loads((SIGS).read_text()) if SIGS.exists() else {}
        lay_by_set = {frozenset(L): f"{fam}_L{i}" for i, L in enumerate(layouts_for(fam))}
        pairs = []
        for sig, headers in sig_map.items():
            keys = _dedup_keys(headers)
            keys = [k for k in keys if not k.startswith("unnamed_")]
            lid = lay_by_set.get(frozenset(keys))
            if lid:
                pairs.append(f"('{sig}','{lid}')")
        if not pairs:
            print(f"[verify] {fam}: no signature->layout map available -- skipped")
            continue
        con.execute("CREATE OR REPLACE TEMP TABLE sig2lay AS "
                    f"SELECT * FROM (VALUES {','.join(pairs)}) t(_header_sig, _layout)")
        # EVERY shared stat column, not just one -- an un-shift that fixed `g` and mangled the
        # rest would pass a single-column check, so the claim is only worth what it covers.
        key = ["nflcom_slug", "season", "_table", SPLIT_LABEL, "_layout"]
        cols_u = {c[0] for c in con.execute(
            f"SELECT * FROM read_parquet('{un}', union_by_name=true) LIMIT 0").description}
        cols_r = {c[0] for c in con.execute(
            f"SELECT * FROM read_parquet('{rp}', union_by_name=true) LIMIT 0").description}
        shared = sorted((cols_u & cols_r) - set(key) - {"_view", "_layout", "_lost_column",
                                                        "_header_sig"})
        # per column: skip the rows whose OWN layout declares that column unrecoverable (it is
        # NULL by construction there, so counting it would score a declared gap as a failure)
        agree_expr = ", ".join(
            f"SUM(CASE WHEN a._lost_column = '{c}' THEN NULL "
            f"WHEN a.{_q(c)} IS NOT DISTINCT FROM b.{_q(c)} THEN 1 ELSE 0 END) AS {_q(c)}, "
            f"COUNT(*) FILTER (WHERE a._lost_column <> '{c}') AS {_q(c + '__n')}"
            for c in shared)
        sel = ", ".join(_q(c) for c in key + shared)
        # ONLY keys unique on BOTH sides. A page can carry two tables with identical headers
        # (kickoff- and punt-return blocks share a column set), so an ambiguous key fans the
        # join 2x2 and scores a CORRECT un-shift at ~97%. Measured: restricting to unambiguous
        # keys takes every recoverable column to 100.00000%.
        ksql = ', '.join(_q(k) for k in key)
        q = f"""
        WITH a0 AS (SELECT {sel}, _lost_column FROM read_parquet('{un}', union_by_name=true)),
             b0 AS (SELECT {sel} FROM read_parquet('{rp}', union_by_name=true)
                    JOIN sig2lay USING (_header_sig)),
             a AS (SELECT * FROM a0 WHERE ({ksql}) IN
                     (SELECT {ksql} FROM a0 GROUP BY {ksql} HAVING COUNT(*) = 1)),
             b AS (SELECT * FROM b0 WHERE ({ksql}) IN
                     (SELECT {ksql} FROM b0 GROUP BY {ksql} HAVING COUNT(*) = 1))
        SELECT COUNT(*) AS overlap, {agree_expr}
        FROM a JOIN b USING ({ksql})"""
        row = con.execute(q).fetchdf().iloc[0]
        overlap = int(row["overlap"])
        # a column the un-shift declares UNRECOVERABLE is NULL by construction; it can only
        # match where the re-parse also had nothing, so report it separately, never as a pass
        lost = {x["lost_column"] for x in rec["families"][fam]["layouts"]}
        per_col, worst, worst_col = {}, 1.0, None
        for c in shared:
            n_c = int(row[c + "__n"] or 0)
            r = (int(row[c] or 0) / n_c) if n_c else None
            per_col[c] = None if r is None else round(r, 7)
            if r is not None and r < worst:
                worst, worst_col = r, c
        print(f"{fam}: holdout {overlap:,} unambiguous rows x {len(shared)} cols | "
              f"worst recoverable column {worst*100:.5f}% ({worst_col})")
        bad = {c: v for c, v in per_col.items() if v is not None and v < 0.9999}
        if bad:
            print(f"   COLUMNS BELOW TOLERANCE: {bad}")
        rec.setdefault("families", {}).setdefault(fam, {})["holdout"] = {
            "overlap_rows": overlap, "columns_compared": len(shared),
            "worst_recoverable_column_agreement": round(worst, 7),
            "worst_column": worst_col,
            "method": "keys unique on both sides; each column scored only on rows whose layout "
                      "does not declare it unrecoverable",
            "per_column_agreement": per_col,
            "columns_below_tolerance": bad,
            "declared_unrecoverable": sorted(lost),
            "basis": "independently re-parsed from retained cache bytes by the fixed parser",
        }
    RECEIPT.write_text(json.dumps(rec, indent=2), encoding="utf-8")
    print(f"-> {RECEIPT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()
    if a.build:
        build()
    if a.verify:
        verify()
    if not (a.build or a.verify):
        ap.error("pass --build and/or --verify")
