"""
sota_recon/corrections/framework.py

The correction-layer engine. Properties every correction inherits:

  idempotent  - each correction's WHERE clause IS its detection predicate, so once
                applied the rows no longer match and re-running is a no-op. Safe to
                replay every rebuild.
  provenance  - every changed row gets its correction key appended to a dedicated
                `recon_correction_log` column (additive, never touches data_source).
  composable  - all corrections run against one in-memory table, then ONE lock-safe
                write (temp parquet + os.replace), so we never half-write the release.
  auditable   - a corrections manifest records per-step detected/changed counts.

A Correction is just an object with .key, .description, and .apply(con) -> int
(rows changed). Use `simple_update()` for the common set/where case; corrections that
need a join (e.g. oracle backfill) implement .apply directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from ..recon_common import utc_stamp, write_manifest
from ..sources import latest_v26

PROV_COL = "recon_correction_log"


def ensure_prov_column(con: duckdb.DuckDBPyConnection, table: str = "st") -> None:
    cols = [c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {PROV_COL} VARCHAR")


def _count(con: duckdb.DuckDBPyConnection, where: str, table: str = "st") -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0])


def simple_update(con: duckdb.DuckDBPyConnection, key: str, set_clause: str,
                  where: str, table: str = "st") -> int:
    """Apply SET to rows matching WHERE, tag provenance, return rows changed."""
    n = _count(con, where, table)
    if n:
        con.execute(f"""
            UPDATE {table}
            SET {set_clause},
                {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL} = ''
                                  THEN '{key}' ELSE {PROV_COL} || ',{key}' END
            WHERE {where}
        """)
    return n


@dataclass
class Correction:
    key: str
    description: str
    apply_fn: "callable"          # (con) -> int rows changed
    detect_where: str | None = None  # optional, for dry-run counting

    def detect(self, con) -> int:
        if self.detect_where is None:
            return -1
        return _count(con, self.detect_where)

    def apply(self, con) -> int:
        return self.apply_fn(con)


@dataclass
class CorrectionRun:
    corrections: list[Correction] = field(default_factory=list)

    def add(self, c: Correction) -> None:
        self.corrections.append(c)

    def execute(self, dry_run: bool = False) -> dict:
        v26 = latest_v26()
        stamp = utc_stamp()
        # unique temp dir per run so concurrent/leftover DuckDB processes never collide
        # on the shared .tmp spill directory (the silent failure cause in background runs)
        tmp_spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
        os.makedirs(tmp_spill, exist_ok=True)
        con = duckdb.connect()
        # single-threaded + no insertion-order preservation: avoids the DuckDB GIL/memory
        # crash on large COPY writes of the full 1.18M x 730-col table.
        con.execute("PRAGMA threads=1")
        con.execute("PRAGMA disable_progress_bar")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory='{tmp_spill}'")
        con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
        ensure_prov_column(con)

        before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
        results = []
        for c in self.corrections:
            if dry_run:
                detected = c.detect(con)
                results.append({"key": c.key, "description": c.description,
                                "detected": detected, "changed": 0, "applied": False})
                continue
            changed = c.apply(con)
            results.append({"key": c.key, "description": c.description,
                            "changed": int(changed), "applied": True})

        # provenance hygiene: dedup the correction log so re-runs don't accumulate
        # duplicate tags (idempotent corrections re-tag each run). Keeps the audit trail clean.
        if not dry_run:
            con.execute(f"""
                UPDATE st
                SET {PROV_COL} = array_to_string(list_distinct(string_split({PROV_COL}, ',')), ',')
                WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'
            """)

        after_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
        # corrections may DELETE rows (dedup) but must never ADD rows
        assert after_rows <= before_rows, "corrections must not add rows"

        manifest = {
            "generated_at_utc": stamp,
            "subject_release": v26,
            "dry_run": dry_run,
            "row_count": int(after_rows),
            "corrections": results,
            "total_rows_changed": int(sum(r.get("changed", 0) for r in results)),
        }

        if not dry_run:
            # lock-safe write via STREAMING pyarrow batches (DuckDB's bulk COPY of this
            # 1.18M x 730-col table intermittently crashes the process; streaming record
            # batches keeps memory flat and the writer stable).
            import pyarrow.parquet as pq
            vpath = Path(v26)
            tmp = vpath.with_name(vpath.stem + "_corrtmp.parquet")
            reader = con.execute("SELECT * FROM st").fetch_record_batch(50000)
            writer = pq.ParquetWriter(str(tmp), reader.schema)
            for batch in reader:
                writer.write_batch(batch)
            writer.close()
            con.close()
            os.replace(tmp, vpath)
            # verify the swap actually landed (guards against a silent/partial write):
            # re-open the live file and assert the provenance column is present.
            vcon = duckdb.connect()
            vcon.execute("PRAGMA threads=1")
            vcon.execute("PRAGMA disable_progress_bar")
            live_cols = [c[0] for c in vcon.execute(f"DESCRIBE SELECT * FROM '{vpath}'").fetchall()]
            live_rows = vcon.execute(f"SELECT COUNT(*) FROM '{vpath}'").fetchone()[0]
            vcon.close()
            if PROV_COL not in live_cols:
                raise RuntimeError(
                    f"Correction write did not land: {PROV_COL} missing from {vpath}. "
                    f"Corrected data is in {tmp} if it still exists."
                )
            if live_rows != after_rows:
                raise RuntimeError(
                    f"Correction write did not land: live row count {live_rows} != "
                    f"expected {after_rows}. Corrected data is in {tmp} if it still exists."
                )
            # manifest goes next to the recon master outputs
            out_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(v26)))),
                "derived", "validation", "sota_recon_master", f"{stamp}_corrections",
            )
            os.makedirs(out_dir, exist_ok=True)
            write_manifest(os.path.join(out_dir, "corrections_manifest.json"), manifest)
            manifest["manifest_path"] = os.path.join(out_dir, "corrections_manifest.json")
        else:
            con.close()
        import shutil
        shutil.rmtree(tmp_spill, ignore_errors=True)
        return manifest
