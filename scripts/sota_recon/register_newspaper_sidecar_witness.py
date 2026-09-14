"""
sota_recon/register_newspaper_sidecar_witness.py -- register the immutable newspaper
sidecar bundle in the curated witness docs.

Updates (idempotent; prior newspaper_sidecar:* entries are replaced):
  docs/witness-contracts-v2.json        -> contracts["newspaper_sidecar:<table>"] entries
                                           (cls=sidecar_registered_non_voting, atom census
                                           with era ranges from the bundle itself)
  docs/witness-column-master-matrix.json -> appends "newspaper_sidecar:*" to the witnesses
                                           list of each weekly column the bundle witnesses
                                           (verdicts untouched -- those belong to the audit
                                           pipeline, not registration)

Never touches any supertable parquet. Read-only against the bundle.

    python -m scripts.sota_recon.register_newspaper_sidecar_witness
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .newspaper_witness_common import (
    ATOM_TO_V26_COL, EVENT_TYPE_MAP, NEWSPAPER_BUNDLE_DIR, SIDECAR_TABLES,
    STAT_CELL_ATOM_MAP, sidecar_path)

DOCS = Path(__file__).resolve().parents[2] / "docs"
CONTRACTS_JSON = DOCS / "witness-contracts-v2.json"
MATRIX_JSON = DOCS / "witness-column-master-matrix.json"
PREFIX = "newspaper_sidecar:"
CLS = "sidecar_registered_non_voting"


def _year_expr(cols: list[str]) -> str:
    if "year" in cols:
        return "TRY_CAST(year AS INT)"
    if "boxscore_id" in cols:
        return "TRY_CAST(SUBSTR(boxscore_id, 1, 4) AS INT)"
    return "NULL"


def _atoms_long(con, table: str, name_col: str, value_col: str,
                mapping: dict[str, str], yexpr: str) -> tuple[dict, dict, int]:
    """Atom census for long-format tables: canonical atoms + raw-vocab count + by_year."""
    rows = con.execute(f"""
        SELECT {name_col} AS raw_name,
               MIN({yexpr}) AS era_min,
               MAX({yexpr}) AS era_max,
               COUNT(*) FILTER (WHERE TRY_CAST({value_col} AS DOUBLE) IS NOT NULL
                                  AND TRY_CAST({value_col} AS DOUBLE) <> 0) AS nonzero
        FROM read_parquet('{sidecar_path(table)}') GROUP BY 1""").fetchall()
    atoms: dict[str, dict] = {}
    raw_unmapped = 0
    for raw, lo, hi, nz in rows:
        atom = mapping.get(raw)
        if atom is None:
            raw_unmapped += 1
            continue
        cur = atoms.setdefault(atom, {"col": atom, "variants": [], "era_min": None,
                                      "era_max": None, "total_nonzero": 0})
        cur["variants"].append(raw)
        if lo is not None:
            cur["era_min"] = lo if cur["era_min"] is None else min(cur["era_min"], lo)
        if hi is not None:
            cur["era_max"] = hi if cur["era_max"] is None else max(cur["era_max"], hi)
        cur["total_nonzero"] += nz
    for a in atoms.values():
        a["variants"] = sorted(a["variants"])
    by_year = dict(con.execute(f"""
        SELECT {yexpr}, COUNT(*) FROM read_parquet('{sidecar_path(table)}')
        GROUP BY 1 ORDER BY 1""").fetchall())
    return atoms, {str(k): v for k, v in by_year.items() if k is not None}, raw_unmapped


def _entry(con, table: str) -> dict:
    p = sidecar_path(table)
    n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}')").fetchone()[0]
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()]
    e = {"grain": "game", "cls": CLS, "path": p, "n_rows": n_rows,
         "meta_cols": len(cols), "atoms": {}, "by_year": {}}
    yexpr = _year_expr(cols)
    if table == "newspaper_weekly_player_stat_cells":
        e["atoms"], e["by_year"], e["raw_vocab_unmapped"] = _atoms_long(
            con, table, "stat_name", "stat_value", STAT_CELL_ATOM_MAP, yexpr)
    elif table in ("newspaper_team_game_stats",):
        e["atoms"], e["by_year"], e["raw_vocab_unmapped"] = _atoms_long(
            con, table, "stat_name", "team_1_value", STAT_CELL_ATOM_MAP, yexpr)
    elif table == "newspaper_team_game_stat_claims":
        e["atoms"], e["by_year"], e["raw_vocab_unmapped"] = _atoms_long(
            con, table, "stat_name", "stat_value", STAT_CELL_ATOM_MAP, yexpr)
    elif table == "newspaper_scoring_events":
        ev_map = {k: v[1] for k, v in EVENT_TYPE_MAP.items() if v[1] is not None}
        e["atoms"], e["by_year"], e["raw_vocab_unmapped"] = _atoms_long(
            con, table, "event_type", "points", ev_map, yexpr)
    else:
        e["by_year"] = {str(k): v for k, v in con.execute(f"""
            SELECT {yexpr}, COUNT(*) FROM read_parquet('{p}')
            GROUP BY 1 ORDER BY 1""").fetchall() if k is not None}
    return e


def run() -> dict:
    con = duckdb.connect()
    contracts_doc = json.loads(CONTRACTS_JSON.read_text(encoding="utf-8"))
    contracts = contracts_doc["contracts"]
    for k in [k for k in contracts if k.startswith(PREFIX)]:
        del contracts[k]
    added = {}
    for table in SIDECAR_TABLES:
        key = PREFIX + table.removeprefix("newspaper_")
        contracts[key] = _entry(con, table)
        contracts[key]["bundle"] = NEWSPAPER_BUNDLE_DIR
        added[key] = {"n_rows": contracts[key]["n_rows"],
                      "atoms": sorted(contracts[key]["atoms"])}
    contracts_doc["summary"]["n_contracts"] = len(contracts)
    contracts_doc["summary"]["newspaper_sidecar_bundle"] = {
        "bundle": NEWSPAPER_BUNDLE_DIR, "cls": CLS, "n_tables": len(SIDECAR_TABLES),
        "note": "sidecar-only witness; wide 203-col overlay deprecated review-only; "
                "non-voting until identity/overlay/reviewer hard-holds clear",
    }
    CONTRACTS_JSON.write_text(json.dumps(contracts_doc, indent=1, sort_keys=True),
                              encoding="utf-8")

    # matrix: append witness names to weekly columns actually witnessed by the bundle
    matrix = json.loads(MATRIX_JSON.read_text(encoding="utf-8"))
    weekly = matrix["tables"]["weekly"]
    cell_atoms = set(contracts[PREFIX + "weekly_player_stat_cells"]["atoms"])
    event_atoms = set(contracts[PREFIX + "scoring_events"]["atoms"])
    touched = {}
    for atom, col in ATOM_TO_V26_COL.items():
        if col not in weekly:
            continue
        names = []
        if atom in cell_atoms:
            names.append(PREFIX + "weekly_player_stat_cells")
        if atom in event_atoms:
            names.append(PREFIX + "scoring_events")
        if not names:
            continue
        wl = weekly[col].setdefault("witnesses", [])
        new = [n for n in names if n not in wl]
        if new:
            wl.extend(new)
            touched[col] = new
    MATRIX_JSON.write_text(json.dumps(matrix, indent=1, sort_keys=True),
                           encoding="utf-8")
    con.close()
    return {"contracts_added": added, "matrix_columns_touched": touched,
            "write_guarantee": "docs JSON registration only; no parquet writes"}


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
