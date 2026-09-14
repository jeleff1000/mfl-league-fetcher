"""Contract-driven row relationship gates + PBP relationship checks.

O.7 slice 4: the row-identity section is driven by the COMMITTED edge contract
(witness_gate/contracts/relationship_edges.v1.json via relationship_edges.
row_gate_edges / row_gate_sql) -- every enforceable row-grain edge runs as a
gate WITHIN its proven era/definition scope. The ad-hoc relationship_registry
rows this lane used to carry are RETIRED with parity receipts
(relationship_registry.RETIRED); the registry keeps only witness-lane rows the
grid cannot express.

No values are promoted here.  The output is an evidence lane: exact formula
failures, PBP coverage, and samples are kept distinct so a disagreement cannot
silently become a repair.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import duckdb

from . import relationship_edges as RE
from .sources import PBP_MERGED, PBP_ROLLUP, latest_v26
from .witness_votes import tolerance_of

PBP_COMPARE = ("attempts", "completions", "passing_yards", "passing_tds",
               "passing_interceptions", "sacks_suffered", "carries", "rushing_yards",
               "rushing_tds", "targets", "receptions", "receiving_yards",
               "receiving_tds", "passing_epa", "rushing_epa", "receiving_epa")


def _q(value) -> str:
    return Path(getattr(value, "path", value)).as_posix()


def _columns(con, path: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()}


def _checks(con, release: str) -> tuple[dict, list[dict]]:
    """Run every enforceable row-grain edge from the committed contract as a gate,
    batched into a single release scan (one FILTER pair per edge)."""
    cols = _columns(con, release)
    results: dict[str, dict] = {}
    samples: list[dict] = []
    gates: list[tuple[dict, str, str]] = []
    for e in RE.row_gate_edges():
        needed = {e["lhs"], *e["rhs"]}
        if not needed <= cols:
            results[e["edge_id"]] = {"status": "not_run",
                                     "missing_columns": sorted(needed - cols)}
            continue
        rendered = RE.row_gate_sql(e, tolerance_of(e["lhs"]))
        if rendered is None:
            results[e["edge_id"]] = {"status": "not_run",
                                     "reason": "gate renderer cannot express edge"}
            continue
        gates.append((e, *rendered))
    if gates:
        sel = []
        for i, (e, guard, viol) in enumerate(gates):
            sel.append(f"COUNT(*) FILTER (WHERE {guard}) AS n_{i}")
            sel.append(f"COUNT(*) FILTER (WHERE ({guard}) AND ({viol})) AS v_{i}")
        row = con.execute(
            f"SELECT {', '.join(sel)} FROM '{release}'").fetchone()
        for i, (e, guard, viol) in enumerate(gates):
            checked, bad = int(row[2 * i]), int(row[2 * i + 1])
            results[e["edge_id"]] = {
                "status": "run", "cells_checked": checked, "mismatches": bad,
                "verdict": e["verdict"], "era_scope": e["era_scope"],
            }
            if not bad:
                continue
            lhs = e["lhs"]
            for a, b, c in con.execute(f"""
                    SELECT player_week, year, {lhs} FROM '{release}'
                    WHERE ({guard}) AND ({viol}) LIMIT 10""").fetchall():
                samples.append(dict(check=e["edge_id"], player_week=a, year=b,
                                    stored=c, derived=None))
    return results, samples


def _pbp(con, release: str) -> dict:
    pbp, roll = _q(PBP_MERGED), _q(PBP_ROLLUP)
    rcols = _columns(con, release)
    pcols = _columns(con, _q(PBP_ROLLUP))
    comparisons = {}
    for col in PBP_COMPARE:
        if col not in rcols or col not in pcols:
            continue
        n, agree = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(p.{col} - r.{col}) <= 0.5)
            FROM '{_q(PBP_ROLLUP)}' p JOIN '{release}' r USING (player_week)
            WHERE p.{col} IS NOT NULL AND r.{col} IS NOT NULL
        """).fetchone()
        comparisons[col] = {"coverage_rows": n, "agreements": agree,
                            "density": round(agree / n, 6) if n else None}
    required = {"NFL_player_id", "year", "week", "season_type", "receiving_yards",
                "receiving_completed_air_yards", "receiving_yards_after_catch"}
    if not required <= rcols:
        return {"status": "not_run", "missing_release_columns": sorted(required - rcols)}
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pbp_recv AS
        SELECT receiver_player_id AS gsis, season AS year, week, season_type,
               SUM(CASE WHEN complete_pass = 1 THEN receiving_yards END) AS pbp_rec_yards,
               SUM(CASE WHEN complete_pass = 1 THEN air_yards END) AS pbp_completed_air,
               SUM(CASE WHEN complete_pass = 1 THEN yards_after_catch END) AS pbp_yac,
               COUNT(*) FILTER (WHERE receiver_player_id IS NOT NULL) AS pbp_targets
        FROM '{pbp}'
        WHERE receiver_player_id IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """)
    n, component_n, agree_yards, agree_parts = con.execute(f"""
        SELECT COUNT(*),
          COUNT(*) FILTER (WHERE p.pbp_completed_air IS NOT NULL AND p.pbp_yac IS NOT NULL),
          COUNT(*) FILTER (WHERE ABS(p.pbp_rec_yards - r.receiving_yards) <= 0.5),
          COUNT(*) FILTER (WHERE p.pbp_completed_air IS NOT NULL AND p.pbp_yac IS NOT NULL
            AND ABS(r.receiving_yards - (p.pbp_completed_air + p.pbp_yac)) <= 0.5)
        FROM pbp_recv p JOIN '{release}' r
          ON r.NFL_player_id = p.gsis AND r.year = p.year AND r.week = p.week
         AND r.season_type = p.season_type
        WHERE r.receiving_yards IS NOT NULL
    """).fetchone()
    return {
        "status": "run",
        "coverage_rows": n,
        "receiving_yards_agreement": agree_yards,
        "receiving_component_rows": component_n,
        "receiving_component_agreement": agree_parts,
        "receiving_yards_density": round(agree_yards / n, 6) if n else None,
        "component_density": round(agree_parts / component_n, 6) if component_n else None,
        "rollup_comparisons": comparisons,
        "source": "pbp_merged_1978_2025",
    }


def run(src: str | None = None, csv_path: str | None = None) -> dict:
    release = _q(src or latest_v26())
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    row, samples = _checks(con, release)
    pbp = _pbp(con, release)
    con.close()
    if csv_path and samples:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(samples[0]))
            w.writeheader()
            w.writerows(samples)
    ran = [k for k, v in row.items() if v.get("status") == "run"]
    return {"release": release,
            "edge_contract": "witness_gate/contracts/relationship_edges.v1.json",
            "row_gates_run": len(ran),
            "row_gates_violated": sorted(k for k in ran if row[k]["mismatches"]),
            "row_relationships": row,
            "pbp_relationships": pbp,
            "samples": samples}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src")
    ap.add_argument("--csv")
    args = ap.parse_args()
    print(json.dumps(run(args.src, args.csv), indent=2, default=str))
