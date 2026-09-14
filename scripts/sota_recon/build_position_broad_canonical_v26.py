"""
sota_recon/build_position_broad_canonical_v26.py  --  collapse `position` to the broad fantasy enum.

THE TWO-COLUMN CONTRACT (Joe, 2026-07-20): `position` keeps the BROAD fantasy position (the 11-value
enum), `nfl_position` keeps the GRANULAR/official position. One column per job. This wave enforces the
first half; `nfl_position` is never touched here.

WHAT WAS WRONG: no super-table builder ever called the taxonomy, so granular codes were copied verbatim
into `position` instead of being mapped. Measured on the live table (1,219,987 rows):

    position broad + nfl_position granular   648,583   correct by design
    position broad + nfl_position broad      538,292   fine -- source position IS broad (79% of it 2010+,
                                                       where rosters record plain QB/RB/WR; no lost detail)
    position granular (single token)          27,775   THE DEFECT -- 84% pre-1970 (LT/RG/LDT/RCB/...)
    position compound ('QB,K', 'RB,DB')        5,256   left alone -- see below

CONSEQUENCE OF THE DEFECT (why this is not cosmetic): league scoring routes on position via a CASE that
matches a fixed list ('LB','DL','DB','ILB','OLB','MLB','DE','DT','ED','CB','S','SS','FS','D'). `RCB`,
`LCB` and `RLB` are not in it, so those players fell through to ELSE and were scored as OFFENSIVE
players. 114 IDP player-weeks are mis-scored today purely because `position` was never collapsed.

COMPOUNDS ARE DELIBERATELY UNTOUCHED. Every compound value was verified to consist entirely of broad
tokens already (0 rows contain a granular token inside a comma list), so there is nothing to map. They
also carry real multi-eligibility that downstream ranking consumes via
`list_has_any(string_split(position, ','), ...)` -- collapsing 'QB,K' to a single token would destroy a
two-way player's second position. Do not "fix" them here.

SINGLE SOURCE OF TRUTH: the CASE below is GENERATED from witness_gate/contracts/position_taxonomy.v1.json
(118 detailed -> 11 broad). It is not hand-maintained. The repo already carries three hand-written copies
of this mapping (build_nfl_position_from_pfr_v26._canonical_fantasy_pos_case,
build_nfl_position_complete_v26.POS_CASE, position_tokens.FANTASY_POSITION_ORDER); this wave deliberately
does not add a fourth. Consolidating those three onto the contract is follow-up work.

GATES (all must hold or the wave aborts before swapping):
  * row count unchanged
  * `nfl_position` byte-identical on every row
  * every emitted single-token `position` is in the 11-value broad enum
  * compound `position` values byte-identical
  * changed-row count == the exact pre-measured expectation (no silent over-reach)
  * backup written before os.replace

    python -m scripts.sota_recon.build_position_broad_canonical_v26            # dry-run
    python -m scripts.sota_recon.build_position_broad_canonical_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26

PROV = "wave61.position_broad_canonical"
TAXONOMY = Path(__file__).resolve().parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_taxonomy(path: Path = TAXONOMY) -> tuple[list[str], dict[str, str]]:
    body = json.loads(path.read_text(encoding="utf-8"))
    broad = list(body["broad_positions"])
    detailed = {str(k).strip().upper(): str(v).strip().upper() for k, v in body["detailed_to_broad"].items()}
    unknown = sorted({v for v in detailed.values()} - set(broad))
    if unknown:
        raise ValueError(f"taxonomy maps to non-broad targets: {unknown}")
    return broad, detailed


def build_position_case(broad: list[str], detailed: dict[str, str]) -> str:
    """Generate the collapse CASE from the contract.

    Only single-token, non-broad, non-blank values are mapped. Anything the contract does not
    know is left EXACTLY as-is rather than nulled -- an unmapped code is a taxonomy gap to fix in
    the contract, not a row to silently destroy.
    """
    broad_list = ", ".join(_sql_literal(b) for b in broad)
    # group detailed codes by target so the CASE stays compact and readable
    by_target: dict[str, list[str]] = {}
    for code, target in sorted(detailed.items()):
        if code in set(broad):
            continue  # identity entries need no branch
        by_target.setdefault(target, []).append(code)
    branches = []
    for target in sorted(by_target):
        codes = ", ".join(_sql_literal(c) for c in sorted(by_target[target]))
        branches.append(f"    WHEN UPPER(TRIM(position)) IN ({codes}) THEN {_sql_literal(target)}")
    branch_sql = "\n".join(branches)
    return f"""
CASE
    WHEN position IS NULL THEN position
    WHEN TRIM(position) = '' THEN position
    WHEN position LIKE '%,%' THEN position
    WHEN UPPER(TRIM(position)) IN ({broad_list}) THEN UPPER(TRIM(position))
{branch_sql}
    ELSE position
END
""".strip()


def run(apply: bool = False) -> dict:
    broad, detailed = load_taxonomy()
    case_sql = build_position_case(broad, detailed)
    v26 = latest_v26()
    vq = Path(v26).as_posix()

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    expected_changed = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' WHERE ({case_sql}) IS DISTINCT FROM position"
    ).fetchone()[0]
    residual = con.execute(
        f"""SELECT COUNT(*) FROM '{vq}'
            WHERE position IS NOT NULL AND TRIM(position) <> '' AND position NOT LIKE '%,%'
              AND ({case_sql}) NOT IN ({", ".join(_sql_literal(b) for b in broad)})"""
    ).fetchone()[0]
    unmapped = con.execute(
        f"""SELECT position, COUNT(*) AS n FROM '{vq}'
            WHERE position IS NOT NULL AND TRIM(position) <> '' AND position NOT LIKE '%,%'
              AND ({case_sql}) NOT IN ({", ".join(_sql_literal(b) for b in broad)})
            GROUP BY 1 ORDER BY n DESC LIMIT 25"""
    ).fetchall()

    if not apply:
        con.close()
        return {
            "rows": before_rows,
            "would_change": expected_changed,
            "residual_non_broad": residual,
            "unmapped_codes": unmapped,
        }

    if residual:
        con.close()
        raise SystemExit(
            f"ABORT: {residual} rows would still hold a non-broad single-token position. "
            f"Add these codes to position_taxonomy.v1.json first: {unmapped}"
        )

    stamp = utc_stamp()
    spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(spill, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{spill}'")

    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = [f"({case_sql}) AS position"]
    if "recon_correction_log" in cols:
        repl.append(
            "CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
            f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END AS recon_correction_log"
        )
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_posbroad.parquet")
    reader = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), reader.schema)
    for batch in reader:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()

    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]

    def _multiset_delta(expr: str, where: str = "TRUE") -> int:
        """Rows whose (expr) value-distribution differs between old and new.

        Deliberately NOT a join on player_week: that key is NOT unique. 219 player_week
        values cover 442 rows -- the 1920s-30s doubleheaders (two games, distinct opponents,
        one week) that this codebase preserves on purpose. Joining on it fans those rows out
        and compares mismatched pairs, which reported a phantom "nfl_position changed on 2
        rows" and aborted a run whose transform was correct.

        Comparing the multiset of values is order-independent and duplicate-safe: any real
        change to a value, or to how many rows hold it, still shows up.
        """
        sql = f"""
            SELECT COALESCE(SUM(ABS(d)), 0) FROM (
                SELECT COALESCE(a.v, b.v) AS v, COALESCE(a.c, 0) - COALESCE(b.c, 0) AS d
                FROM (SELECT {expr} AS v, COUNT(*) c FROM '{vq}' WHERE {where} GROUP BY 1) a
                FULL OUTER JOIN
                     (SELECT {expr} AS v, COUNT(*) c FROM '{tq}' WHERE {where} GROUP BY 1) b
                  ON a.v IS NOT DISTINCT FROM b.v
            ) WHERE d <> 0
        """
        return int(con.execute(sql).fetchone()[0])

    nflpos_changed = _multiset_delta("nfl_position")
    compound_changed = _multiset_delta("position", "position LIKE '%,%'")
    pos_changed = _multiset_delta("position") // 2  # each move = one -1 and one +1
    bad = con.execute(
        f"""SELECT COUNT(*) FROM '{tq}' WHERE position IS NOT NULL AND TRIM(position) <> ''
            AND position NOT LIKE '%,%'
            AND position NOT IN ({", ".join(_sql_literal(b) for b in broad)})"""
    ).fetchone()[0]

    problems = []
    if after_rows != before_rows:
        problems.append(f"row count moved {before_rows} -> {after_rows}")
    if nflpos_changed:
        problems.append(f"nfl_position changed on {nflpos_changed} rows (must be 0)")
    if compound_changed:
        problems.append(f"compound position changed on {compound_changed} rows (must be 0)")
    if bad:
        problems.append(f"{bad} rows still hold a non-broad single-token position")
    if pos_changed != expected_changed:
        problems.append(f"changed {pos_changed} rows, expected exactly {expected_changed}")
    if problems:
        tmp.unlink(missing_ok=True)
        con.close()
        raise SystemExit("ABORT (no swap): " + "; ".join(problems))

    backup = vp.with_name(vp.stem + f"_prew61_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    con.close()
    return {
        "rows": after_rows,
        "position_changed": pos_changed,
        "nfl_position_changed": nflpos_changed,
        "compound_changed": compound_changed,
        "backup": str(backup),
        "provenance": PROV,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write the collapsed position column")
    args = ap.parse_args()
    print(json.dumps(run(apply=args.apply), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
