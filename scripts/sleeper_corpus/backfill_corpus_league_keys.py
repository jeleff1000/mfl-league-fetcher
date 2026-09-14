"""backfill_corpus_league_keys.py -- one-shot: add the league_key column to an existing
corpus_snapshot.duckdb whose league_settings was folded before the column joined the
contract (build_corpus_snapshot.py). Re-reads only league_settings (1 row/league-year)
from each already-folded smpl db. Safe to re-run; no-op once the column is fully populated.

    py -3 scripts/sleeper_corpus/backfill_corpus_league_keys.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

CORPUS = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
OUT = CORPUS / "corpus_snapshot.duckdb"


def main() -> None:
    con = duckdb.connect(str(OUT))
    cols = {r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()}
    if "league_key" not in cols:
        con.execute("ALTER TABLE public.league_settings ADD COLUMN league_key VARCHAR")
    todo = [r[0] for r in con.execute(
        "SELECT DISTINCT db_name FROM public.league_settings WHERE league_key IS NULL").fetchall()]
    print(f"[backfill] {len(todo):,} leagues missing league_key")
    filled = skipped = 0
    for name in todo:
        db = CORPUS / "leagues" / name / f"{name}.duckdb"
        if not db.exists():
            db = CORPUS / name / f"{name}.duckdb"  # stray root-level ingest dirs
        if not db.exists():
            skipped += 1
            continue
        try:
            con.execute(f"ATTACH '{db.as_posix()}' AS src (READ_ONLY)")
            con.execute("""
                UPDATE public.league_settings t
                SET league_key = s.league_key
                FROM (SELECT db_name, year, CAST(league_key AS VARCHAR) league_key
                      FROM src.public.league_settings) s
                WHERE t.db_name = s.db_name AND t.year = s.year AND t.db_name = ?""", [name])
            filled += 1
        except Exception as e:
            print(f"  [skip] {name}: {str(e).splitlines()[0][:100]}")
            skipped += 1
        finally:
            try:
                con.execute("DETACH src")
            except Exception:
                pass
    left = con.execute(
        "SELECT COUNT(*) FROM public.league_settings WHERE league_key IS NULL").fetchone()[0]
    print(f"[backfill] filled {filled:,}, skipped {skipped:,}; league-years still NULL: {left:,}")
    con.close()


if __name__ == "__main__":
    main()
