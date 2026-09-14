"""Measure, but do not apply, a target-side RBFB5/WRTE layout candidate matrix.

The raw capture loses the position block.  This diagnostic asks whether the exact
date/opponent weekly target would distinguish the two layouts.  It is deliberately not
fed back into capture recovery: using the target to assign a layout and then scoring the
same target would be circular.  Only source-side equations may recover a layout.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import duckdb

from .build_pre1978_fumbles_lost_v26 import NICKNAME_CODES
from .nflcom_player_logs_capture_inventory import SHARED_SCHEMA_TIE, mirror_layout_recovery_query
from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_logs_shared_layout_candidate_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_logs"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def score_row(row):
    """Return RB/WR exact scores without turning missing targets into zeros."""
    pairs = {
        "RBFB5": ((row[1], row[7]), (row[2], row[8]), (row[3], row[9]), (row[4], row[10]), (row[5], row[11]), (row[6], row[12])),
        "WRTE": ((row[1], row[8]), (row[2], row[7]), (row[3], row[10]), (row[4], row[9]), (row[5], row[12]), (row[6], row[11])),
    }
    result = {}
    for layout, values in pairs.items():
        informative = [(a, b) for a, b in values if a is not None and b is not None and (a or b)]
        result[layout] = {"informative_n": len(informative), "agree_n": sum(a == b for a, b in informative)}
    rb, wr = result["RBFB5"], result["WRTE"]
    result["winner"] = "RBFB5" if rb["agree_n"] > wr["agree_n"] else "WRTE" if wr["agree_n"] > rb["agree_n"] else "TIE"
    result["min_informative_n"] = min(rb["informative_n"], wr["informative_n"])
    return result


def _query_rows(con, v26, layout_filter):
    source = str(SOURCE / "**/*.parquet").replace("'", "''")
    raw = f"SELECT * FROM read_parquet('{source}', union_by_name=true) WHERE _table IN ('Regular Season','Post Season')"
    con.execute("CREATE OR REPLACE TEMP TABLE codes(label VARCHAR, code VARCHAR)")
    codes = [(label, code) for label, values in NICKNAME_CODES.items() for code in values] + [("Braves", "BOS")]
    con.executemany("INSERT INTO codes VALUES (?, ?)", codes)
    con.execute("CREATE OR REPLACE TEMP TABLE src AS " + mirror_layout_recovery_query(raw))
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug,b.NFL_player_id
        FROM read_parquet('{str(SLUGS).replace("'", "''")}') x
        JOIN read_parquet('{str(BIO).replace("'", "''")}') b ON b.pfr_id=x.pfr_id""")
    return con.execute(f"""SELECT s.recovered_layout,
        TRY_CAST(s.yds AS DOUBLE),TRY_CAST(s.yds_2 AS DOUBLE),
        TRY_CAST(s.lng AS DOUBLE),TRY_CAST(s.lng_2 AS DOUBLE),
        TRY_CAST(s.td AS DOUBLE),TRY_CAST(s.td_2 AS DOUBLE),
        v.rushing_yards,v.receiving_yards,v.rushing_long,v.receiving_long,
        v.rushing_tds,v.receiving_tds
      FROM src s JOIN ids i ON i.nflcom_slug=s.nflcom_slug
      JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
       AND v.year=TRY_CAST(s.season AS INT)
       AND CAST(v.game_date AS DATE)=TRY_STRPTIME(s.game_date,'%m/%d/%Y')
       AND v.season_type=CASE WHEN s._table='Post Season' THEN 'POST' ELSE 'REG' END
       AND v.opponent_nfl_team IN
           (SELECT code FROM codes WHERE label=regexp_replace(s.opp,'^@',''))
      WHERE {layout_filter}""").fetchall()


def _summarize(rows, known=False):
    counts = Counter()
    eligible = 0
    for row in rows:
        score = score_row(row)
        if score["min_informative_n"] >= 2:
            eligible += 1
            counts[(row[0] if known else "UNRESOLVED", score["winner"])] += 1
    return {"joined_n": len(rows), "eligible_min_informative_2_n": eligible, "confusion": [{"actual": a, "candidate": b, "rows": n} for (a, b), n in sorted(counts.items())]}


def measure():
    con = duckdb.connect()
    v26 = str(latest_v26()).replace("'", "''")
    known = _query_rows(con, v26, "s.recovered_layout IN ('RBFB5','WRTE') AND (" + SHARED_SCHEMA_TIE + ")")
    unresolved = _query_rows(con, v26, "s.recovered_layout IS NULL AND (" + SHARED_SCHEMA_TIE + ")")
    candidate_counts = Counter()
    for row in unresolved:
        score = score_row(row)
        candidate_counts[score["winner"]] += 1
    aggregate = {}
    for label, index in (("RBFB5", 0), ("WRTE", 1)):
        pairs = []
        for row in unresolved:
            score = score_row(row)[label]
            aggregate[label] = aggregate.get(label, {"informative_n": 0, "agree_n": 0})
            aggregate[label]["informative_n"] += score["informative_n"]
            aggregate[label]["agree_n"] += score["agree_n"]
    for value in aggregate.values():
        value["agree_pct"] = round(100 * value["agree_n"] / value["informative_n"], 2) if value["informative_n"] else None
    return {"measurement_only": True, "source": "nflcom_player_logs", "candidate_layouts": ["RBFB5", "WRTE"], "calibration_known_shared": _summarize(known, known=True), "unresolved_shared": _summarize(unresolved), "unresolved_candidate_counts": dict(candidate_counts), "aggregate_candidate_scores": aggregate, "circularity_guard": "TARGET_SIDE_DIAGNOSTIC_ONLY; never assign recovered_layout from this matrix", "no_state_change": True}


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
