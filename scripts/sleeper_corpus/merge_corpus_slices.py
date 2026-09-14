"""merge_corpus_slices.py -- merge crawl slices downloaded from GH Actions into D:'s central
corpus snapshot.

The crawl worker (corpus_crawl_worker.yml) ships one `corpus_slice_N.duckdb.gpg` per matrix
batch -- encrypted, because the workers repo is PUBLIC and slices carry Sleeper league ids
(G10, 2026-07-06). Download them (`gh run download <run-id> -D <dir>`) and point this at the
directory; `.gpg` files are decrypted with CORPUS_ARTIFACT_PASSPHRASE, then each slice is
folded in and deduped by the `_sources` ledger -- so re-merging an already-merged slice is a
no-op and a partial/failed slice can be re-merged safely.

    gh run download <run-id> -D D:/tmp/slices
    set CORPUS_ARTIFACT_PASSPHRASE=...
    py -3 scripts/sleeper_corpus/merge_corpus_slices.py D:/tmp/slices
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

from build_corpus_snapshot import OUT, TABLES, folded_set, open_snapshot


def decrypt(enc: Path, outdir: Path) -> Path:
    """Decrypt a .gpg slice with CORPUS_ARTIFACT_PASSPHRASE into outdir."""
    passphrase = os.environ.get("CORPUS_ARTIFACT_PASSPHRASE")
    if not passphrase:
        raise SystemExit(
            "CORPUS_ARTIFACT_PASSPHRASE is unset -- needed to decrypt the crawl slices "
            "(they are encrypted because the workers repo is public; see G10).")
    out = outdir / enc.name[: -len(".gpg")]
    subprocess.run(
        ["gpg", "--batch", "--yes", "--quiet", "--decrypt", "--passphrase", passphrase,
         "--output", str(out), str(enc)],
        check=True, capture_output=True)
    return out


def merge_slice(central: duckdb.DuckDBPyConnection, slice_path: Path, already: set[str]) -> tuple[int, int]:
    """Fold one slice's leagues into the central snapshot. Returns (merged, skipped)."""
    central.execute(f"ATTACH '{slice_path.as_posix()}' AS sl (READ_ONLY)")
    try:
        incoming = [r[0] for r in central.execute("SELECT db_name FROM sl._sources").fetchall()]
        new = [d for d in incoming if d not in already]
        if not new:
            return 0, len(incoming)
        central.execute("BEGIN")
        # Filter by db_name so a slice that overlaps another (shouldn't happen with disjoint
        # --offset slices, but a re-run or a re-merge can) contributes each league exactly once.
        params = ", ".join("?" for _ in new)
        sl_tables = {r[0] for r in central.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='sl' AND table_schema='public'").fetchall()}
        for t, (cols, _) in TABLES.items():
            # a table the slice predates (e.g. matchup, added 2026-07-19) is skipped, same as
            # missing columns are NULL-filled -- old slices must keep merging
            if t not in sl_tables:
                print(f"    [schema] {t}: absent from slice, skipped")
                continue
            # Name columns on BOTH sides: INSERT ... SELECT * binds by position, so a slice
            # built before a schema migration (or by an older worker) would silently shift
            # values into the wrong columns rather than error. Columns the slice predates
            # (e.g. waiver_budget, added 2026-07-17) are NULL-filled -- patch_waiver_budget.py
            # is the designed backfill for those, not the merge.
            have = {r[0] for r in central.execute(f"DESCRIBE sl.public.{t}").fetchall()}
            collist = ", ".join(f'"{n}"' for n, _ in cols)
            sellist = ", ".join(
                f'CAST("{n}" AS {typ}) AS "{n}"' if n in have else f'CAST(NULL AS {typ}) AS "{n}"'
                for n, typ in cols
            )
            missing = [n for n, _ in cols if n not in have]
            if missing:
                print(f"    [schema] {t}: NULL-filling {len(missing)} column(s) the slice "
                      f"predates: {', '.join(missing)}")
            central.execute(
                f"INSERT INTO public.{t} ({collist}) SELECT {sellist} FROM sl.public.{t} "
                f"WHERE db_name IN ({params})", new)
        central.execute(
            f"INSERT INTO _sources SELECT * FROM sl._sources WHERE db_name IN ({params})", new)
        central.execute("COMMIT")
        already.update(new)
        return len(new), len(incoming) - len(new)
    except Exception:
        try:
            central.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        try:
            central.execute("DETACH sl")
        except Exception:
            pass


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: merge_corpus_slices.py <dir-of-downloaded-slices>")
    src = Path(sys.argv[1])
    encrypted = sorted(src.rglob("corpus_slice_*.duckdb.gpg"))
    plain = sorted(src.rglob("corpus_slice_*.duckdb"))
    if not encrypted and not plain:
        raise SystemExit(f"no corpus_slice_*.duckdb[.gpg] found under {src}")

    central = open_snapshot(OUT)
    already = folded_set(central)
    print(f"[merge] central has {len(already):,} leagues; "
          f"{len(encrypted) + len(plain)} slice(s) to merge")
    tot_new = tot_dup = 0
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        # decrypted plaintext lives only in a temp dir for the life of the merge
        slices = [decrypt(e, tmp) for e in encrypted] + plain
        for s in slices:
            try:
                new, dup = merge_slice(central, s, already)
            except Exception as e:
                print(f"  [FAIL] {s.name}: {str(e).splitlines()[0][:140]}")
                failures.append(s.name)
                continue
            tot_new += new
            tot_dup += dup
            print(f"  {s.name}: +{new} new, {dup} already present")

    print(f"[merge] +{tot_new:,} leagues ({tot_dup:,} dups skipped) -> {OUT}")
    n = central.execute("SELECT COUNT(*) FROM _sources").fetchone()[0]
    print(f"[merge] central now holds {n:,} leagues")
    for t in TABLES:
        rows = central.execute(f"SELECT COUNT(*) FROM public.{t}").fetchone()[0]
        print(f"   {t}: {rows:,} rows")
    central.close()
    if failures:
        raise SystemExit(f"[merge] {len(failures)} slice(s) failed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
