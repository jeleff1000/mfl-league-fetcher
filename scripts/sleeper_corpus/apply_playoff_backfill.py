"""apply_playoff_backfill.py -- overlay the Sleeper playoff-backfill sidecars onto the lake.

Downloads/merges the playoff_backfill_worker shard parquets and stamps two NEW columns onto
corpus_snapshot.player_fantasy -- made_po_bf, is_playoffs_bf -- for slice-delivered rows only
(final_playoff_seed IS NULL). The builder prefers made_po_bf over the (absent) native seed, so
final_playoff_seed keeps its real meaning for the 2,785 local leagues that carry it.

    py -3 scripts/sleeper_corpus/apply_playoff_backfill.py --sidecar-dir <dir> [--no-backup]
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

SNAP = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sidecar-dir", help="dir of playoff_backfill_shard*.parquet")
    ap.add_argument("--snapshot", default=str(SNAP))
    ap.add_argument("--no-backup", action="store_true")
    ap.add_argument("--tag", default="pre-po-overlay", help="backup filename tag")
    ap.add_argument("--resolve-only", action="store_true",
                    help="skip the sidecar merge; just (re)derive made_po/has_po_signal from "
                         "columns already in the lake. Safe on a lake whose overlay already ran.")
    args = ap.parse_args()

    snap = Path(args.snapshot)
    if not args.resolve_only and not args.sidecar_dir:
        raise SystemExit("--sidecar-dir is required unless --resolve-only")
    glob = None
    if not args.resolve_only:
        shards = sorted(Path(args.sidecar_dir).rglob("playoff_backfill_shard*.parquet"))
        if not shards:
            raise SystemExit(f"no shard parquets under {args.sidecar_dir}")
        glob = f"{args.sidecar_dir}/**/playoff_backfill_shard*.parquet"

    if not args.no_backup:
        bkp = snap.with_name(f"{snap.stem}.{args.tag}.duckdb")
        print(f"[overlay] backing up -> {bkp.name}", flush=True)
        shutil.copy2(snap, bkp)

    con = duckdb.connect(str(snap))
    con.execute("SET memory_limit='7000MB'; SET threads=4; SET preserve_insertion_order=false;")

    if not args.resolve_only:
        # 1) collect the sidecar (shards are disjoint by db, dedup is a cheap safety net)
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE sc AS
          SELECT db_name, year, week, NFL_player_id,
                 MAX(made_po) AS made_po, MAX(is_playoffs) AS is_playoffs
          FROM read_parquet('{glob}')
          GROUP BY 1,2,3,4
        """)
        n_sc, n_lg = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT db_name) FROM sc").fetchone()
        print(f"[overlay] sidecar: {n_sc:,} rows across {n_lg:,} leagues", flush=True)

        # 2) add the overlay columns (idempotent)
        have = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
        for col in ("made_po_bf", "is_playoffs_bf"):
            if col not in have:
                con.execute(f"ALTER TABLE public.player_fantasy ADD COLUMN {col} TINYINT")

        # 3) stamp onto slice rows ONLY (never clobber the native seed on local leagues)
        con.execute("""
          UPDATE public.player_fantasy p
          SET made_po_bf = sc.made_po, is_playoffs_bf = sc.is_playoffs
          FROM sc
          WHERE p.db_name = sc.db_name AND p.year = sc.year
            AND p.week = sc.week AND p.NFL_player_id = sc.NFL_player_id
            AND p.final_playoff_seed IS NULL
        """)
    else:
        print("[overlay] --resolve-only: skipping sidecar merge", flush=True)

    # 4) resolve the precedence chain ONCE, into canonical columns.
    # Every consumer was re-implementing "native final_playoff_seed, then made_po_bf, then
    # champion" by hand, and a reader that checks only the unsuffixed names sees an empty
    # column (the public lake carries no native seed at all) and concludes the backfill never
    # ran. made_po / has_po_signal are the names to read; the _bf pair stays for provenance.
    have = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    for col in ("made_po", "has_po_signal"):
        if col not in have:
            con.execute(f"ALTER TABLE public.player_fantasy ADD COLUMN {col} TINYINT")

    con.execute("""
      UPDATE public.player_fantasy p
      SET has_po_signal = CASE
              WHEN p.final_playoff_seed IS NOT NULL
                OR p.made_po_bf IS NOT NULL
                OR CAST(p.champion AS INT) = 1 THEN 1 ELSE 0 END,
          made_po = CASE
              -- the seed test is exact, but only when the bracket size is known
              WHEN p.final_playoff_seed IS NOT NULL AND ls.playoff_teams IS NOT NULL
                   THEN CASE WHEN p.final_playoff_seed <= ls.playoff_teams THEN 1 ELSE 0 END
              WHEN p.made_po_bf IS NOT NULL THEN CAST(p.made_po_bf AS TINYINT)
              -- a champion flag is itself playoff evidence
              WHEN CAST(p.champion AS INT) = 1 THEN 1
              ELSE NULL END
      FROM public.league_settings ls
      WHERE ls.db_name = p.db_name AND ls.year = p.year
    """)
    resolved, signal = con.execute("""
      SELECT COUNT(DISTINCT db_name) FILTER (WHERE made_po IS NOT NULL),
             COUNT(DISTINCT db_name) FILTER (WHERE has_po_signal = 1)
      FROM public.player_fantasy
    """).fetchone()
    print(f"[overlay] canonical made_po resolved for {resolved:,} leagues "
          f"({signal:,} carry a playoff signal)", flush=True)

    # 5) verify coverage jump
    print("[overlay] playoff-signal coverage by year (rostered rows):", flush=True)
    print(con.execute("""
      SELECT year,
             COUNT(DISTINCT db_name) AS leagues,
             COUNT(DISTINCT db_name) FILTER (
               WHERE final_playoff_seed IS NOT NULL OR made_po_bf IS NOT NULL) AS lgs_with_signal
      FROM public.player_fantasy
      WHERE CAST(is_rostered AS INT)=1 AND year BETWEEN 2019 AND 2025
      GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False), flush=True)
    con.close()
    print("[overlay] done", flush=True)


if __name__ == "__main__":
    main()
