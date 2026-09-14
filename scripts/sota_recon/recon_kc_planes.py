"""
sota_recon/recon_kc_planes.py  --  LANE: K/C plane gate execution (O.4)

Executes every runnable kc_planes.v1.json contract against the entity universes:

K gates (per contract, D1):
  n_rows / null_key_rows          rows whose declared key is unparseable (recorded, excluded)
  n_distinct_keys / dup_key_count source-key uniqueness; a dup under declared '1:1' is a
                                  CARDINALITY VIOLATION, under 'controlled 1:N' it is fanout
  unmatched_keys (+rate)          anti-join: distinct source keys absent from the universe
                                  (AUTHORITY contracts expect exactly 0 -- self-consistency)

C gates (per contract, D2 set arithmetic):
  expected_keys_in_window         universe keys inside the contract's temporal validity
                                  (+ season-type policy for season-grain joins)
  matched_in_window               observed ∩ expected
  expected_only                   expected − observed; a MISSING finding only under DENSE,
                                  recorded coverage under every other declared model

Zero-counters emitted for the closure scoreboard (§0 / §25.13):
  one_to_one_cardinality_violations, authority_anti_join_failures,
  dense_sources_with_missing_keys, contracts_errored, contracts_pending (typed)

Outputs:
  {MASTER_ROOT}/{stamp}_v26/kc_planes/   per-source receipts CSV + manifest (regenerable)
  docs/kc-planes-summary.json            compact committed summary (per-source gate scalars)

Run:  python -m scripts.sota_recon.recon_kc_planes
"""

from __future__ import annotations

import json
import os

import duckdb

from .entity_universes import OUT_DIR as UNIVERSES_DIR
from .entity_universes import UNIVERSE_KEYS
from .kc_planes import load as load_contracts
from .recon_common import canon_team_sql, connect, lane_dir, new_run_dir, utc_stamp
from .sources import registry

LANE = "kc_planes"
SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "kc-planes-summary.json")

# (universe, logical key component) -> universe column
UNIVERSE_COL = {
    ("games", "boxscore_id"): "boxscore_id",
    ("team_games", "boxscore_id"): "boxscore_id",
    ("team_games", "team"): "team_code",  # canon_team contracts remap to team_canon at run time
    ("team_games", "team_game_key"): "team_game_key",
    ("team_games", "year"): "year",
    ("team_games", "week"): "week",
    ("player_game_presence", "boxscore_id"): "boxscore_id",
    ("player_game_presence", "pfr_id"): "pfr_id",
    ("player_seasons", "pfr_id"): "pfr_id",
    ("player_seasons", "year"): "year",
    ("players", "pfr_id"): "pfr_id",
    ("plays", "game_id"): "game_id",
    ("plays", "play_id"): "play_id",
}
# universe year column for temporal windows (players has none: identity grain)
UNIVERSE_YEAR = {u: "year" for u in UNIVERSE_KEYS} | {"players": None}


def _register_universes(con: duckdb.DuckDBPyConnection, universes_dir: str) -> None:
    for name in UNIVERSE_KEYS:
        p = os.path.join(universes_dir, f"{name}.parquet").replace("'", "''")
        con.execute(f"CREATE OR REPLACE VIEW u_{name} AS SELECT * FROM '{p}'")
    con.execute("CREATE OR REPLACE VIEW u_players AS SELECT DISTINCT pfr_id FROM u_player_seasons WHERE pfr_id IS NOT NULL")


def _source_key_expr(logical: str, col: str, canon_team: bool) -> str:
    if logical == "year":
        return f"TRY_CAST({col} AS INTEGER)"
    if logical == "week":
        return f"TRY_CAST({col} AS INTEGER)"
    if logical == "pfr_id":
        return f"NULLIF(TRIM({col}), '')"
    if logical == "team" and canon_team:
        return canon_team_sql(col)
    return col


def _scan(path: str) -> str:
    """A quoted SQL scan target for a source path.

    DuckDB reads a quoted string in FROM as a FILE only when it looks like one; a bare
    directory resolves as a TABLE NAME and raises CatalogException. Every source that
    reached this lane before 2026-07-29 happened to be a single .parquet, so the bare
    quoting worked; the nflcom stat families are SHARDED DIRECTORIES and are the first
    to need the glob. Emitted as read_parquet(union_by_name) because shards written by
    different harvest runs do not all carry the same columns.
    """
    if os.path.isdir(path):
        glob = os.path.join(path, "**", "*.parquet").replace("\\", "/").replace("'", "''")
        return f"read_parquet('{glob}', union_by_name=true)"
    return "'" + path.replace("'", "''") + "'"


def run_contract(con: duckdb.DuckDBPyConnection, entry: dict, src_path: str,
                 xwalk_path: str | None = None) -> dict:
    k = entry["k"]
    model = entry["c"]["coverage_model"]
    universe = k["canonical_universe"]
    logical = k["logical_key"]
    canon = bool(k.get("canon_team"))
    if k.get("source_key_exprs"):
        # contract-declared SQL expressions (e.g. composite-key splits); canon still applies to team
        raw = k["source_key_exprs"]
        exprs = [
            f"{_source_key_expr(lg, expr, canon)} AS k{i}"
            for i, (lg, expr) in enumerate(zip(logical, raw))
        ]
    else:
        exprs = [
            f"{_source_key_expr(lg, col, canon)} AS k{i}"
            for i, (lg, col) in enumerate(zip(logical, k["source_key_columns"]))
        ]
    kcols = ", ".join(f"k{i}" for i in range(len(logical)))
    notnull = " AND ".join(f"k{i} IS NOT NULL" for i in range(len(logical)))
    base = _scan(src_path)
    cw = k.get("crosswalk")
    if cw:
        # receipted ID-space crosswalk (crosswalk_receipts.v1.json): LEFT JOIN so rows
        # whose id has no crosswalk keep a NULL key and are COUNTED as null_key_rows —
        # typed coverage loss, never a silent drop.
        if not xwalk_path:
            raise ValueError(f"crosswalk contract without a path for {cw['via']!r}")
        xp_scan = _scan(xwalk_path)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW src_xw AS
            SELECT s.*, x.{cw['adds']} AS xw_{cw['adds']}
            FROM {base} s
            LEFT JOIN (SELECT DISTINCT {cw['on']}, {cw['adds']} FROM {xp_scan}
                       WHERE {cw['on']} IS NOT NULL AND {cw['adds']} IS NOT NULL) x
              ON s.{cw['on']} = x.{cw['on']}""")
        base = "src_xw"
    con.execute(f"CREATE OR REPLACE TEMP VIEW src_keys AS SELECT {', '.join(exprs)} FROM {base}")

    n_rows = con.execute("SELECT COUNT(*) FROM src_keys").fetchone()[0]
    null_key_rows = con.execute(f"SELECT COUNT(*) FROM src_keys WHERE NOT ({notnull})").fetchone()[0]
    con.execute(f"CREATE OR REPLACE TEMP VIEW src_valid AS SELECT {kcols} FROM src_keys WHERE {notnull}")
    n_keys = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT {kcols} FROM src_valid)").fetchone()[0]
    dup_keys = con.execute(
        f"SELECT COUNT(*) FROM (SELECT {kcols} FROM src_valid GROUP BY {kcols} HAVING COUNT(*) > 1)"
    ).fetchone()[0]

    ucols = [
        "team_canon" if (universe == "team_games" and lg == "team" and canon)
        else UNIVERSE_COL[(universe, lg)]
        for lg in logical
    ]
    ujoin = " AND ".join(f"s.k{i} = u.{c}" for i, c in enumerate(ucols))
    unmatched = con.execute(
        f"""
        SELECT COUNT(*) FROM (SELECT DISTINCT {kcols} FROM src_valid) s
        LEFT JOIN (SELECT DISTINCT {', '.join(ucols)} FROM u_{universe}) u ON {ujoin}
        WHERE u.{ucols[0]} IS NULL
        """
    ).fetchone()[0]

    # C plane: expected universe keys inside the contract window (+ season policy)
    tv = k.get("temporal_validity") or {}
    ycol = UNIVERSE_YEAR[universe]
    where = ["1=1"]
    if ycol and tv.get("year_min") is not None:
        where.append(f"{ycol} >= {int(tv['year_min'])} AND {ycol} <= {int(tv['year_max'])}")
    stp = k.get("season_type_policy", "ALL")
    if stp != "ALL" and universe == "player_seasons":
        where.append(f"season_type = '{stp}'")
    uwin = f"(SELECT DISTINCT {', '.join(ucols)} FROM u_{universe} WHERE {' AND '.join(where)})"
    expected = con.execute(f"SELECT COUNT(*) FROM {uwin}").fetchone()[0]
    matched_in_window = con.execute(
        f"""
        SELECT COUNT(*) FROM (SELECT DISTINCT {kcols} FROM src_valid) s
        JOIN {uwin} u ON {ujoin}
        """
    ).fetchone()[0]
    expected_only = expected - matched_in_window

    is_1to1 = k.get("expected_cardinality") == "1:1"
    return {
        "status": k["status"],
        "universe": universe,
        "coverage_model": model,
        "expected_cardinality": k.get("expected_cardinality"),
        "n_rows": int(n_rows),
        "null_key_rows": int(null_key_rows),
        "n_distinct_keys": int(n_keys),
        "dup_key_count": int(dup_keys),
        "cardinality_violation": bool(is_1to1 and dup_keys > 0),
        "unmatched_keys": int(unmatched),
        "unmatched_rate": round(unmatched / n_keys, 6) if n_keys else None,
        "authority_anti_join_failure": bool(k["status"] == "AUTHORITY" and unmatched > 0),
        "expected_keys_in_window": int(expected),
        "matched_in_window": int(matched_in_window),
        "expected_only": int(expected_only),
        "missing_if_dense": int(expected_only) if model == "DENSE" else 0,
    }


def run(universes_dir: str | None = None,
        contracts: dict | None = None,
        paths: dict[str, str] | None = None,
        write_receipts: bool = True) -> dict:
    con = connect()
    _register_universes(con, universes_dir or UNIVERSES_DIR)
    doc = contracts or load_contracts()
    paths = paths or {sid: s.path for sid, s in registry(include_subject=True).items()}

    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    pending: dict[str, str] = {}
    for entry in doc["contracts"]:
        sid = entry["source_id"]
        st = entry["k"]["status"]
        if st not in {"ACTIVE", "AUTHORITY"}:
            pending[sid] = st
            continue
        try:
            xw = (entry["k"].get("crosswalk") or {}).get("via")
            results[sid] = run_contract(con, entry, paths[sid],
                                        xwalk_path=paths.get(xw) if xw else None)
        except Exception as exc:  # per-source isolation: one bad parquet never kills the lane
            errors[sid] = f"{type(exc).__name__}: {exc}"

    counters = {
        "contracts_total": len(doc["contracts"]),
        "contracts_executed": len(results),
        "contracts_errored": len(errors),
        "contracts_pending_typed": len(pending),
        "one_to_one_cardinality_violations": sum(r["cardinality_violation"] for r in results.values()),
        "authority_anti_join_failures": sum(r["authority_anti_join_failure"] for r in results.values()),
        "dense_sources_with_missing_keys": sum(1 for r in results.values() if r["missing_if_dense"] > 0),
    }
    summary = {
        "generated_utc": utc_stamp(),
        "counters": counters,
        "pending": pending,
        "errors": errors,
        "sources": results,
    }
    if write_receipts:
        out = lane_dir(new_run_dir(), LANE)
        with open(os.path.join(out, "kc_planes_receipts.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        summary["receipts_dir"] = out
    return summary


def main() -> int:
    summary = run()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print("counters:", json.dumps(summary["counters"], indent=2))
    worst = sorted(
        ((sid, r) for sid, r in summary["sources"].items() if r["unmatched_rate"]),
        key=lambda x: -x[1]["unmatched_rate"],
    )[:10]
    for sid, r in worst:
        print(f"  unmatched {sid:36s} {r['unmatched_keys']:>8,} / {r['n_distinct_keys']:>9,} keys "
              f"({r['unmatched_rate']:.2%}) -> {r['universe']}")
    if summary["errors"]:
        print("errors:")
        for sid, e in summary["errors"].items():
            print(f"  {sid}: {e[:140]}")
    print(f"summary -> {os.path.abspath(SUMMARY_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
