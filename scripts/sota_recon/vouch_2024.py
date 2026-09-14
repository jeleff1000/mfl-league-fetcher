"""THE VOUCH HARNESS v2 (Joe: 'column by column... vouching every column
means what you think it means... backfills ready FOR EVERY column').

v2 adds ERA-AWARE vouching: a lane that cannot see 2024 (ancient witnesses --
newspaper, PFA, mid-century era labels) is not a failure; it vouches against
its OWN densest era, exactly like the 1947 'Sack Yds Lost' label validated
in-era at 97.65%. Ladder per lane:
    1. 2024 exact          (the modern gold stratum)
    2. 2020-2024 window    (thin modern samples)
    3. in-era peak window  (peak witness year -2 .. peak) -- era lanes
Each lock records WHICH window vouched it; the gate re-verifies in the same
window. Locks are machine-written only.
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
from scripts.sota_recon.correction_engine import week_witness_sql

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
LOCKFILE = Path(__file__).parent / "witness_gate" / "contracts" / "witness_locks.v1.json"
LEDGER = LAKE / "vouch_2024_ledger.json"

MIN_N, MIN_AGREE = 25, 0.95


def _stage(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def lane_sql(sp):
    adapted = week_witness_sql(sp)
    if adapted:
        sql, kind = adapted
        return sql, ("pid", "yr", "wk") if kind == "player" else ("team", "yr", "wk")
    try:
        sql = W.build_witness_sql(sp)
    except Exception:
        return None, None
    cols = ("team", "yr", "wk") if any(
        s in sql for s in ("AS team", "r.defteam AS", "r.posteam AS")) else (
        "pfr_id", "yr")
    return sql, cols


def fingerprint(sp) -> str:
    """The dup-path key: uniquely identifies ONE value path. The v1 lockfile
    keyed (source, col, shape) and collided -- the gate re-vouched sibling
    specs and read the mismatch as drift (caught by its own first run)."""
    return "|".join([sp.source_key, sp.v26_col, sp.shape, sp.source_table,
                     sp.table_col, sp.row_filter, sp.source_col,
                     sp.source_expr, sp.filters, sp.team_col])


# CONTEXT/STRING COLUMNS (2026-08-04, Joe: "ALL OUR COLUMN RULES FOR
# POSITION AND FANTASY POSITION ARE FULLY SPECCED AND QUORUM AND LOCKED AND
# GATED"). compare() was numeric-only -- ABS(w.val - CAST(t.col)) -- so a
# string column could never vouch, never lock, and never gate. That is why
# 34 position mapspecs existed and none of them held a lock. Position-class
# columns compare on the BROAD axis through position_taxonomy, which is the
# vocabulary alignment their comparison always needed.
_TAXP = Path(__file__).parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json"
STRING_COLS = {"position", "nfl_position", "fantasy_position", "nfl_team",
               "opponent_nfl_team", "season_type", "player", "data_source"}
TAXONOMY_COLS = {"position", "nfl_position", "fantasy_position"}


def _broad_sql(expr: str) -> str:
    """Map a detailed role to its broad position.

    DUAL-ALIGNMENT NOTATION (2026-08-04): sources write "played both spots"
    with a slash -- LCB/RCB, LDE/RDE, LT/RT, SS/FS, RG/C. Measured, these
    were ~85% of the PFR disagreement: every one maps to a SINGLE broad
    position (both corners are DB, both tackles are OL), so the mismatch was
    a taxonomy gap, never a source dispute.

    HYPHEN IS THE SAME NOTATION (Joe 2026-08-05: "take a sample of
    mismatches and see if its just aliases" -- much of it was). StatsCrew
    writes multi-spot labels with hyphens instead: SE-FL, FB-HB, LOH-ROH,
    DB-E, LDE-MLB-LL. Splitting only on / and , left those as raw literals
    that could never equal anything, so they scored as source disputes.
    Adding '-' moved statscrew vs our position on 1950-69 from 0.7489 to
    0.8550 -- the lane was never wrong, the split was.

    AND AN UNMAPPABLE LABEL IS NOT A CLAIM. The old fallback returned the
    raw string, so 'FL/LE' (a genuine two-way label spanning WR and DL) came
    back as the literal 'FL/LE' and counted as a contradiction -- it even
    failed the write-time position law on year=1954. A label that resolves
    to more than one broad, or to none, means the source expressed NO SINGLE
    OPINION: return NULL and let the caller skip it.
    """
    if not _TAXP.exists():
        return f"UPPER(TRIM({expr}))"
    d2b = json.loads(_TAXP.read_text(encoding="utf-8"))["detailed_to_broad"]
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in d2b.items())
    direct = f"CASE UPPER(TRIM({expr})) {whens} END"
    # token-wise map over slash/comma/HYPHEN splits, then demand unanimity
    toks = (f"list_distinct(list_filter(list_transform("
            f"str_split_regex(UPPER(TRIM({expr})), '[/,-]'), x -> "
            f"CASE TRIM(x) {whens} END), y -> y IS NOT NULL))")
    unanimous = f"CASE WHEN len({toks}) = 1 THEN {toks}[1] END"
    return f"COALESCE({direct}, {unanimous})"


def compare(con, sp, sql, keys, lo, hi):
    col, tol = sp.v26_col, (sp.validation_tolerance or 0.001)
    if col in STRING_COLS:
        w_expr = (_broad_sql("CAST(w.val AS VARCHAR)") if col in TAXONOMY_COLS
                  else "UPPER(TRIM(CAST(w.val AS VARCHAR)))")
        t_expr = (_broad_sql(f"t.{col}") if col in TAXONOMY_COLS
                  else f"UPPER(TRIM(t.{col}))")
        if keys == ("pid", "yr", "wk"):
            q = f"""WITH w AS ({sql})
            SELECT COUNT(*), COUNT(*) FILTER (WHERE {w_expr} = {t_expr})
            FROM w JOIN plane t ON t.NFL_player_id = w.pid
              AND t.year = w.yr AND t.week = w.wk
            WHERE w.yr BETWEEN {lo} AND {hi} AND t.{col} IS NOT NULL
              AND w.val IS NOT NULL"""
        elif keys == ("pfr_id", "yr"):
            q = f"""WITH w AS ({sql})
            SELECT COUNT(*), COUNT(*) FILTER (WHERE {w_expr} = {t_expr})
            FROM w JOIN ids b USING (pfr_id)
            JOIN plane t ON t.NFL_player_id = b.NFL_player_id
              AND t.year = w.yr
            WHERE w.yr BETWEEN {lo} AND {hi} AND t.{col} IS NOT NULL
              AND w.val IS NOT NULL"""
        else:
            return 0, 0
        return con.execute(q).fetchone()
    # SEASON-TYPE ROUTING (2026-08-03): a POST lane compared against the
    # REG plane view can never agree -- 196 of 741 "conflicts" were exactly
    # this category error, plus 33 Preseason lanes (no plane rows -> the
    # join is empty and reads NO-OVERLAP, which is the honest verdict).
    plane = ("plane_post"
             if (sp.season_type or "REG").upper() == "POST"
             or "post" in (sp.source_table or "").lower() else "plane")
    if keys == ("pid", "yr", "wk"):
        # tolerance honored here too -- the player-week branch compared EXACT,
        # which made rate lanes unfixable by spec tolerance (found 2026-08-03)
        q = f"""WITH w AS ({sql})
        SELECT COUNT(*), COUNT(*) FILTER (
          WHERE ABS(w.val - TRY_CAST(t.{col} AS DOUBLE)) <= {tol})
        FROM w JOIN {plane} t ON t.NFL_player_id = w.pid
          AND t.year = w.yr AND t.week = w.wk
        WHERE w.yr BETWEEN {lo} AND {hi} AND t.{col} IS NOT NULL"""
    elif keys == ("team", "yr", "wk"):
        q = f"""WITH w AS ({sql})
        SELECT COUNT(*), COUNT(*) FILTER (
          WHERE ABS(w.val - TRY_CAST(t.{col} AS DOUBLE)) <= {tol})
        FROM w JOIN {plane} t ON t.nfl_team = w.team AND t.year = w.yr
          AND t.week = w.wk AND t.position = 'DEF'
        WHERE w.yr BETWEEN {lo} AND {hi} AND t.{col} IS NOT NULL"""
    else:
        q = f"""WITH w AS ({sql}),
        v AS (SELECT b.pfr_id, t.year AS yr,
                     SUM(TRY_CAST(t.{col} AS DOUBLE)) AS sv
              FROM {plane} t JOIN ids b USING (NFL_player_id)
              WHERE t.{col} IS NOT NULL
                AND t.year BETWEEN {lo} AND {hi} GROUP BY 1, 2)
        SELECT COUNT(*), COUNT(*) FILTER (
          WHERE ABS(w.val - v.sv) <= {tol})
        FROM w JOIN v USING (pfr_id, yr)
        WHERE w.yr BETWEEN {lo} AND {hi}"""
    return con.execute(q).fetchone()


def peak_year(con, sql, keys):
    try:
        r = con.execute(f"""WITH w AS ({sql})
        SELECT w.yr FROM w GROUP BY 1 ORDER BY COUNT(*) DESC, w.yr DESC
        LIMIT 1""").fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except Exception:
        return None


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = S.weekly_read_path()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    # VIEW, never a materialized table: v2's first run loaded the whole
    # 1,082-column plane into 1.3GB and OOM'd. A view pushes each compare's
    # year predicate down into the parquet scan -- slower per lane, immune
    # to the plane's width.
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")
    plane_cols = {r[0] for r in con.execute(
        "DESCRIBE SELECT * FROM plane LIMIT 0").fetchall()}

    locks, queue, skipped = [], [], defaultdict(int)
    lanes = [sp for sp in W.WITNESS_MAP if sp.v26_col in plane_cols]
    _stage(f"{len(lanes)} lanes")
    done_src = None
    for sp in sorted(lanes, key=lambda s: s.source_key):
        if sp.source_key != done_src:
            done_src = sp.source_key
            _stage(f"{done_src} ({len(locks)} locked so far)")
        sql, keys = lane_sql(sp)
        if not sql:
            # ECOSYSTEM-FIRST FALLBACK: shapes without a lane generator
            # (newspaper long tables, nflcom team-season) already have inline
            # compare support in W.validate() -- use the validator itself as
            # the compare engine instead of skipping 360 lanes.
            try:
                res = W.validate([sp])[0]
            except Exception as e:
                queue.append({"fingerprint": fingerprint(sp),
                              "source": sp.source_key, "column": sp.v26_col,
                              "shape": sp.shape,
                              "error": str(e).splitlines()[0][:90]})
                continue
            n2 = res.get("n") or 0
            a2 = res.get("agree_pct")
            rec = {"fingerprint": fingerprint(sp), "source": sp.source_key,
                   "column": sp.v26_col, "shape": sp.shape,
                   "window": "native-validate", "n": n2, "agree": a2,
                   "grain": sp.validation_grain or sp.grain}
            if n2 >= MIN_N and (a2 or 0) >= MIN_AGREE:
                locks.append(rec)
            else:
                rec["why"] = (f"native verdict {res.get('verdict')}"
                              f" agree={a2} n={n2}")
                queue.append(rec)
            continue
        rec = {"fingerprint": fingerprint(sp), "source": sp.source_key,
               "column": sp.v26_col, "shape": sp.shape}
        try:
            windows = [("2024", 2024, 2024), ("2020-2024", 2020, 2024)]
            n = agree = 0
            used = None
            for name, lo, hi in windows:
                n, agree = compare(con, sp, sql, keys, lo, hi)
                used = name
                if n >= MIN_N:
                    break
            if n < MIN_N:
                py = peak_year(con, sql, keys)
                if py:
                    n, agree = compare(con, sp, sql, keys, py - 2, py)
                    used = f"in-era {py - 2}-{py}"
        except Exception as e:
            rec["error"] = str(e).splitlines()[0][:90]
            queue.append(rec)
            continue
        rec.update({"window": used, "n": n,
                    "agree": round(agree / n, 4) if n else None,
                    "grain": "week" if keys and len(keys) == 3 else "season"})
        if n >= MIN_N and agree / n >= MIN_AGREE:
            locks.append(rec)
        else:
            rec["why"] = ("no overlap in any window" if not n else
                          f"n={n} below {MIN_N}" if n < MIN_N else
                          f"CONFLICT {agree/n:.3f} in {used} -> arbitration")
            queue.append(rec)

    lock_doc = {
        "version": "v2", "law": (
            "every locked lane re-verifies IN ITS VOUCHED WINDOW on every CI "
            "run; unwitnessed atom changes or mapping drift FAIL the suite. "
            "Machine-written only; keys are full lane fingerprints."),
        "thresholds": {"min_n": MIN_N, "min_agree": MIN_AGREE},
        "locks": sorted(locks, key=lambda r: (r["column"], r["fingerprint"])),
    }
    LOCKFILE.write_text(json.dumps(lock_doc, indent=1), encoding="utf-8")
    LEDGER.write_text(json.dumps({
        "generated": time.strftime("%Y-%m-%d %H:%M"),
        "locked": len(locks), "queued": len(queue),
        "skipped": dict(skipped), "queue": queue}, indent=1), encoding="utf-8")
    return {"lanes": len(lanes), "locked_lanes": len(locks),
            "locked_columns": len({r['column'] for r in locks}),
            "conflicts_to_arbitration": sum(
                1 for q in queue if "CONFLICT" in q.get("why", "")),
            "fix_queue": len(queue), "skipped": dict(skipped)}


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    print(json.dumps(build(con), indent=2))
