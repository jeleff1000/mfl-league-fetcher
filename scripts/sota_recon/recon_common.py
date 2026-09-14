"""Shared plumbing for SOTA reconciliation scripts."""

from __future__ import annotations

import datetime as _dt
import json
import os

import duckdb

from .sources import DATA_LAKE, registry

MASTER_ROOT = os.path.join(DATA_LAKE, "derived", "validation", "sota_recon_master")


def utc_stamp() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute("PRAGMA disable_progress_bar")
    return con


def new_run_dir(stamp: str | None = None) -> str:
    stamp = stamp or utc_stamp()
    d = os.path.join(MASTER_ROOT, f"{stamp}_v26")
    os.makedirs(d, exist_ok=True)
    return d


def lane_dir(run_dir: str, lane: str) -> str:
    d = os.path.join(run_dir, lane)
    os.makedirs(d, exist_ok=True)
    return d


def write_manifest(path: str, payload: dict) -> None:
    base = {
        "generated_at_utc": utc_stamp(),
        "cloud_write_performed": False,
        "destructive_actions_performed": False,
    }
    base.update(payload)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(base, f, indent=2, default=str)


def dump_csv(con: duckdb.DuckDBPyConnection, sql: str, out_csv: str) -> int:
    con.execute(f"COPY ({sql}) TO '{out_csv}' (HEADER, DELIMITER ',')")
    return int(con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0])


TEAM_CODE_CANON = {
    "OAK": "RAI",
    "LV": "RAI",
    "LVR": "RAI",
    "STL": "RAM",
    "LA": "RAM",
    "LAR": "RAM",
    "SD": "SDG",
    "LAC": "SDG",
    "SDG": "SDG",
    "HOU_OILERS": "OTI",
    "TEN": "OTI",
    "OTI": "OTI",
    "BAL_COLTS": "CLT",
    "IND": "CLT",
    "CLT": "CLT",
    "PHX": "CRD",
    "ARI": "CRD",
    "CRD": "CRD",
    "BOS_PATS": "NWE",
    "NE": "NWE",
    "NWE": "NWE",
    "TAM": "TAM",
    "TB": "TAM",
    "GB": "GNB",
    "GNB": "GNB",
    "KC": "KAN",
    "KAN": "KAN",
    "NO": "NOR",
    "NOR": "NOR",
    "SF": "SFO",
    "SFO": "SFO",
    "JAC": "JAX",
    "JAX": "JAX",
    "WAS": "WAS",
    "WSH": "WAS",
}


def canon_team_sql(col: str) -> str:
    whens = "\n".join(f"        WHEN UPPER(TRIM({col})) = '{k}' THEN '{v}'" for k, v in TEAM_CODE_CANON.items())
    return f"(CASE\n{whens}\n        ELSE UPPER(TRIM({col}))\n      END)"


def era_of(year_col: str) -> str:
    return f"""(CASE
        WHEN {year_col} < 1933 THEN 'early_box'
        WHEN {year_col} < 1950 THEN 'mid_box'
        WHEN {year_col} < 1978 THEN 'pre_pbp'
        ELSE 'modern'
      END)"""


def source_pin() -> dict:
    from .sources import manifest_block

    reg = registry()
    return {
        "subject_release": reg["v26_release"].path,
        "sources": manifest_block(),
    }


def safe_replace(src, dst, spin_seconds: float = 60.0, slow_minutes: float = 30.0) -> None:
    """os.replace with retry: long-running local readers (research app, corpus ingest) hold the
    release parquet almost continuously on Windows; the free gaps between their queries are
    SUB-SECOND, so a slow poll systematically lands inside a hold (2026-07-17: 120 x 5s retries
    never hit a gap; a 100ms spin wins). Fast-spin first, then back off."""
    import os as _os
    import time as _time
    deadline_fast = _time.monotonic() + spin_seconds
    deadline_slow = _time.monotonic() + slow_minutes * 60
    while True:
        try:
            _os.replace(src, dst)
            return
        except PermissionError:
            now = _time.monotonic()
            if now < deadline_fast:
                _time.sleep(0.1)
            elif now < deadline_slow:
                _time.sleep(1.0)
            else:
                raise
