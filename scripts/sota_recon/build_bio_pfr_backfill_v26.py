"""
sota_recon/build_bio_pfr_backfill_v26.py -- wave59: close the standing player_bio gap
against PFR's player index (superset guarantee).

recon_bio_vs_pfr partitions the forward gap into: mintable (absent, appearance-witnessed),
relink (bio row exists under NFL_player_id with pfr_id NULL -- same player, link only),
twin (bio row exists but linked to a DIFFERENT pfr_id -- conflict, review only), and
index_only_ghost (no appearance witness -- skipped). This wave applies the first two as
two separately-gated phases; twins and ghosts are never auto-applied.

  MINT   append one identity bio row per mintable pfr_id (NFL_player_id=pfr_id; identity
         + career fields from PFR; all stat/measurable fields NULL) -> row_add fact
  RELINK set pfr_id = NFL_player_id on the 47 existing rows where pfr_id IS NULL and the
         PFR id matches -> cell_override fact (old NULL -> pfr_id)

Gates: after == before + minted; every untouched row byte-stable (order-independent hash
of rows outside mint/relink identical before/after); relinked rows differ ONLY in pfr_id;
minted ids all new & unique; timestamped backup; facts emitted.

    python -m scripts.sota_recon.build_bio_pfr_backfill_v26            # DRY RUN
    python -m scripts.sota_recon.build_bio_pfr_backfill_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import facts
from .recon_bio_vs_pfr import OUT as RECON_OUT
from .sources import PLAYER_BIO

WAVE = "wave59.bio_pfr_backfill"
BIO = Path(PLAYER_BIO.path)
MINTABLE = (RECON_OUT / "forward_gap_mintable.csv").as_posix()
RELINK = (RECON_OUT / "forward_gap_relink.csv").as_posix()

# minted identity fields sourced from PFR; everything else in bio -> typed NULL
MINT_EXPR = {
    "NFL_player_id": "m.pfr_id",
    "player": "m.player",
    "nfl_position": "split_part(m.index_position, '-', 1)",
    "rookie_year": "TRY_CAST(m.first_year AS DOUBLE)",
    "first_year": "TRY_CAST(m.first_year AS DOUBLE)",
    "last_year": "TRY_CAST(m.last_year AS DOUBLE)",
    "years_active": "TRY_CAST(m.last_year AS DOUBLE) - TRY_CAST(m.first_year AS DOUBLE) + 1",
    "pfr_id": "m.pfr_id",
    "primary_team_source": "'pfr_index_backfill_w59'",
}


def _bio_schema(con) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{BIO.as_posix()}')").fetchall()]


def _mint_select(schema) -> str:
    cols = []
    for name, typ in schema:
        expr = MINT_EXPR.get(name, f"CAST(NULL AS {typ})")
        cols.append(f"{expr} AS {name}")
    return ("SELECT " + ", ".join(cols) +
            f" FROM read_csv_auto('{MINTABLE}') m")


def run(apply: bool) -> dict:
    con = duckdb.connect()
    bio_p = BIO.as_posix()
    schema = _bio_schema(con)

    mint_ids = [r[0] for r in con.execute(f"SELECT pfr_id FROM read_csv_auto('{MINTABLE}')").fetchall()]
    relink_ids = [r[0] for r in con.execute(f"SELECT bio_nflid FROM read_csv_auto('{RELINK}')").fetchall()]
    mint_lit = ", ".join(f"('{i}')" for i in mint_ids)
    relink_lit = ", ".join(f"('{i}')" for i in relink_ids) if relink_ids else "(NULL)"

    # guards: minted ids must be wholly absent; relink ids must exist with pfr_id NULL
    clash = con.execute(f"""
        SELECT COUNT(*) FROM (VALUES {mint_lit}) t(id)
        WHERE id IN (SELECT pfr_id FROM read_parquet('{bio_p}') WHERE pfr_id IS NOT NULL
                     UNION SELECT NFL_player_id FROM read_parquet('{bio_p}'))""").fetchone()[0]
    if clash:
        raise SystemExit(f"GUARD: {clash} minted ids already present -- rerun recon_bio_vs_pfr")
    if len(set(mint_ids)) != len(mint_ids):
        raise SystemExit("GUARD: duplicate mint ids")
    bad_relink = con.execute(f"""
        SELECT COUNT(*) FROM (VALUES {relink_lit}) t(id)
        WHERE id IS NOT NULL AND id NOT IN
              (SELECT NFL_player_id FROM read_parquet('{bio_p}') WHERE pfr_id IS NULL)""").fetchone()[0]
    if bad_relink:
        raise SystemExit(f"GUARD: {bad_relink} relink ids not in NULL-pfr_id state -- rerun recon")

    before_n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{bio_p}')").fetchone()[0]
    # order-independent hash of rows OUTSIDE mint+relink -- must be identical after
    stable_pred = (f"NFL_player_id NOT IN (SELECT id FROM (VALUES {mint_lit}) t(id)) "
                   f"AND NFL_player_id NOT IN (SELECT id FROM (VALUES {relink_lit}) t(id))")
    stable_hash = con.execute(
        f"SELECT bit_xor(hash(b::VARCHAR)) FROM read_parquet('{bio_p}') b WHERE {stable_pred}"
    ).fetchone()[0]

    res = {"wave": WAVE, "mode": "APPLY" if apply else "DRY-RUN", "bio": bio_p,
           "before_rows": before_n, "mint": len(mint_ids), "relink": len(relink_ids),
           "after_rows_expected": before_n + len(mint_ids)}
    if not apply:
        con.close()
        with open(RECON_OUT / "BIO_MINT_DRY_RUN.json", "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2, default=str)
        return res

    # transform: relink pfr_id on existing rows, then append minted rows
    tmp = bio_p + ".tmp_w59"
    con.execute(f"""COPY (
        SELECT * REPLACE (
            CASE WHEN NFL_player_id IN (SELECT id FROM (VALUES {relink_lit}) t(id))
                      AND pfr_id IS NULL THEN NFL_player_id ELSE pfr_id END AS pfr_id)
        FROM read_parquet('{bio_p}')
        UNION ALL BY NAME
        {_mint_select(schema)}
    ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)""")

    after_n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0]
    after_stable = con.execute(
        f"SELECT bit_xor(hash(b::VARCHAR)) FROM read_parquet('{tmp}') b WHERE {stable_pred}"
    ).fetchone()[0]
    new_present = con.execute(f"""
        SELECT COUNT(DISTINCT pfr_id) FROM read_parquet('{tmp}')
        WHERE pfr_id IN (SELECT id FROM (VALUES {mint_lit}) t(id))""").fetchone()[0]
    relinked = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{tmp}')
        WHERE NFL_player_id IN (SELECT id FROM (VALUES {relink_lit}) t(id))
          AND pfr_id = NFL_player_id""").fetchone()[0]
    # relinked rows must differ from before ONLY in pfr_id (compare all other cols via hash)
    other_cols = ", ".join(n for n, _ in schema if n != "pfr_id")
    relink_stable = con.execute(f"""
        WITH a AS (SELECT {other_cols} FROM read_parquet('{bio_p}')
                   WHERE NFL_player_id IN (SELECT id FROM (VALUES {relink_lit}) t(id))),
             c AS (SELECT {other_cols} FROM read_parquet('{tmp}')
                   WHERE NFL_player_id IN (SELECT id FROM (VALUES {relink_lit}) t(id)))
        SELECT (SELECT bit_xor(hash(a::VARCHAR)) FROM a) = (SELECT bit_xor(hash(c::VARCHAR)) FROM c)
    """).fetchone()[0] if relink_ids else True

    gates = {"rows": after_n == before_n + len(mint_ids),
             "untouched_stable": after_stable == stable_hash,
             "mints_present": new_present == len(mint_ids),
             "relinks_applied": relinked == len(relink_ids),
             "relink_only_pfrid_changed": bool(relink_stable)}
    res.update(after_rows=after_n, gates=gates)
    if not all(gates.values()):
        os.remove(tmp)
        res.update(swapped=False, aborted="gate_failure")
        con.close()
        return res

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bk = bio_p.replace(".parquet", f"_prew59_{stamp}.parquet")
    shutil.copy2(bio_p, bk)
    os.replace(tmp, bio_p)

    fcon = facts.connect()
    for r in con.execute(f"""SELECT pfr_id, player, first_year, last_year, index_position
                             FROM read_csv_auto('{MINTABLE}')""").fetchall():
        facts.emit_fact("row_add", fcon, wave_id=WAVE,
            reason=f"player_bio superset gap vs PFR index; appearance-witnessed ({r[2]}-{r[3]})",
            witness="pfr_player_index", source_snapshot_id=f"pfr_index|recon_bio_vs_pfr:{stamp}",
            table_name="player_bio", target_key=r[0],
            row_json=json.dumps({"NFL_player_id": r[0], "pfr_id": r[0], "player": r[1],
                                 "first_year": r[2], "last_year": r[3], "nfl_position": r[4]}))
    for r in con.execute(f"SELECT bio_nflid FROM read_csv_auto('{RELINK}')").fetchall():
        facts.emit_fact("cell_override", fcon, wave_id=WAVE,
            reason="link existing bio row to its PFR id (was unlinked)",
            witness="pfr_player_index", source_snapshot_id=f"pfr_index|recon_bio_vs_pfr:{stamp}",
            table_name="player_bio", target_key=r[0], column_name="pfr_id",
            old_value=None, new_value=r[0])
    fcon.close()

    res.update(swapped=True, backup=bk, facts_emitted=len(mint_ids) + len(relink_ids))
    con.close()
    with open(RECON_OUT / f"BIO_MINT_RUN_{stamp}.json", "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, default=str)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
