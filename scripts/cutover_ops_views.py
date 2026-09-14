"""
cutover_ops_views.py -- view-refresh step of the ___ops split (docs/runbooks/ops-nfl-split-migration.md).

Recreates the Fly ___ops.nfl_historical VIEWS over the standalone ___ops_nfl.duckdb so the
logical name ___ops.nfl_historical.<t> keeps resolving with ZERO read-reference repoint.
Idempotent + type-aware (DROP VIEW errors on a Table and vice-versa, so we read the current type
first and drop the right kind). Safe to re-run.

Views use EXPLICIT COLUMN LISTS read live from ___ops_nfl, never SELECT *: DuckDB (v1.5.1, the
Fly engine) mis-binds star expansion of the ~1,068-col super table inside nested plans
(CTE + window-dedup + outer window -> InternalException "inequal types VARCHAR != DOUBLE"),
which killed every research-mode derived-lane query on 2026-07-09. Explicit projection through
the view is verified safe. Consequence: **re-run this script after every /replace-db ___ops_nfl**
so new columns become visible through the views (build_ops_nfl_and_replace.py invokes it).

  *** WRITES TO PRODUCTION ___ops when --apply is given. ***
  Prereqs (rollout order): (1) ___ops_nfl.duckdb built + placed on the volume; (2) the server code
  that attaches ___ops_nfl is deployed (so CREATE VIEW can bind it, incl. the admin-write path).

    python scripts/cutover_ops_views.py            # dry-run: show current types + the exact DDL
    python scripts/cutover_ops_views.py --apply --i-understand-this-writes-prod
    python scripts/cutover_ops_views.py --verify   # confirm all resolve as views over ___ops_nfl
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

SCHEMA = "nfl_historical"
TABLES = [
    "nfl_player_stats_all", "player_nfl_season", "player_nfl_season_all",
    "player_nfl_career", "player_nfl_career_all", "player_bio",
    "player_nfl_season_team", "player_nfl_season_team_all",
]


def load_env() -> None:
    import os
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"'))


def current_types(writer) -> dict[str, str]:
    """table_name -> 'VIEW' | 'BASE TABLE' | 'MISSING' for the 6 tables in ___ops (this db only).

    Filter on current_database() so the identically-named ___ops_nfl.nfl_historical tables (attached
    on the same connection) are never matched. TABLES/SCHEMA are hardcoded constants -> safe to inline.
    """
    tlist = ",".join(f"'{t}'" for t in TABLES)
    rows = writer.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        f"WHERE table_catalog = current_database() AND table_schema='{SCHEMA}' "
        f"AND table_name IN ({tlist})",
        database="___ops",
    )
    got = {r["table_name"]: r["table_type"] for r in rows}
    return {t: got.get(t, "MISSING") for t in TABLES}


def source_columns(writer) -> dict[str, list[str]]:
    """table_name -> ordered column list, read live from ___ops_nfl so the views can never
    drift from the file that was just placed on the volume."""
    tlist = ",".join(f"'{t}'" for t in TABLES)
    rows = writer.execute(
        "SELECT table_name, column_name, ordinal_position FROM information_schema.columns "
        f"WHERE table_catalog = '___ops_nfl' AND table_schema='{SCHEMA}' "
        f"AND table_name IN ({tlist}) ORDER BY table_name, ordinal_position",
        database="___ops",
    )
    cols: dict[str, list[str]] = {t: [] for t in TABLES}
    for r in rows:
        cols[r["table_name"]].append(r["column_name"])
    missing = [t for t in TABLES if not cols[t]]
    if missing:
        raise SystemExit(f"ABORT: no columns found in ___ops_nfl for {missing} -- is the file attached?")
    return cols


def _select_items(table: str, physical_cols: list[str]) -> str:
    """Quoted physical columns, plus -- for the super table only -- any DST bracket column that
    is no longer physically stored, re-derived from its scalar so every reader of ___ops sees it
    exactly as before. The derived one-hot equals the old stored column on 100% of DEF rows
    (measured), so this is value-identical. Idempotent: while the bracket is still physical it is
    emitted from the column list and NOT re-derived (avoids a duplicate)."""
    items = ['"' + c.replace('"', '""') + '"' for c in physical_cols]
    if table == "nfl_player_stats_all":
        from multi_league.transformations.common.dst_brackets import (
            DROPPABLE_BRACKETS,
            bracket_onehot_sql,
        )
        physical = set(physical_cols)
        for b in DROPPABLE_BRACKETS:
            if b not in physical:
                items.append(f'{bracket_onehot_sql(b)} AS "{b}"')
    return ", ".join(items)


def build_ddl(types: dict[str, str], cols: dict[str, list[str]]) -> list[str]:
    # One transaction per table: each view is atomically dropped+recreated (a failed CREATE can
    # never leave a table dropped with no view behind it), and per-table statements keep each
    # admin-write payload modest (the super table alone lists 1,000+ quoted columns).
    ddls: list[str] = []
    for t in TABLES:
        cur = types[t]
        stmts = ["BEGIN TRANSACTION;"]
        if cur == "VIEW":
            stmts.append(f'DROP VIEW IF EXISTS {SCHEMA}."{t}";')
        elif cur != "MISSING":  # BASE TABLE
            stmts.append(f'DROP TABLE IF EXISTS {SCHEMA}."{t}";')
        col_list = _select_items(t, cols[t])
        stmts.append(
            f'CREATE VIEW {SCHEMA}."{t}" AS SELECT {col_list} FROM "___ops_nfl".{SCHEMA}."{t}";'
        )
        stmts.append("COMMIT;")
        ddls.append("\n".join(stmts))
    return ddls


def verify(writer) -> bool:
    """Confirm each of the 6 is a VIEW and resolves through the production READ path (/query pool,
    where ___ops_nfl is permanently attached) -- not just the admin-write connection."""
    from multi_league.core.db_reader import get_reader
    reader = get_reader()
    ok = True
    types = current_types(writer)
    for t in TABLES:
        is_view = types[t] == "VIEW"
        try:
            df = reader.query_df(f'SELECT count(*) AS n FROM {SCHEMA}."{t}"', database="___ops")
            n = int(df["n"].iloc[0])
        except Exception as e:
            n, is_view, ok = f"ERR {e}", False, False
        status = "OK" if is_view else "NOT-A-VIEW"
        ok = ok and is_view
        print(f"[verify] {t}: type={types[t]} read-path rows={n} -> {status}")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-understand-this-writes-prod", action="store_true")
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()
    load_env()
    from multi_league.core.fly_writer import FlyWriter
    writer = FlyWriter()

    if a.verify:
        raise SystemExit(0 if verify(writer) else "VERIFY FAIL: not all 6 are views over ___ops_nfl")

    types = current_types(writer)
    print("[cutover] current ___ops.nfl_historical types:")
    for t in TABLES:
        print(f"          {t}: {types[t]}")
    cols = source_columns(writer)
    ddls = build_ddl(types, cols)
    print("\n[cutover] DDL (idempotent, type-aware, explicit columns):")
    for t, ddl in zip(TABLES, ddls):
        print(f"          {t}: {len(cols[t])} columns, {len(ddl)} chars DDL")

    if not (a.apply and a.i_understand_this_writes_prod):
        print("\nDRY-RUN -- no write. Re-run with --apply --i-understand-this-writes-prod")
        return

    # One admin-write connection (server pre-attaches ___ops_nfl there), one transaction per table.
    for t, ddl in zip(TABLES, ddls):
        writer.execute(ddl, database="___ops")
        print(f"[cutover] refreshed view {SCHEMA}.{t} ({len(cols[t])} columns)")
    print("\n[cutover] applied. verifying...")
    if not verify(writer):
        raise SystemExit("VERIFY FAIL after apply -- inspect ___ops.nfl_historical")
    print("[cutover] DONE -- all promoted tables now resolve as views over ___ops_nfl.")


if __name__ == "__main__":
    main()
