"""Measure Opponents-by-Team/L5 passing witnesses, including recomputed rates."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_splits_opponents_team_l5_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_splits"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)
MAPPINGS = {"1st": "passing_first_downs", "20": "pass_explosive_20", "att": "attempts", "comp": "completions", "int": "passing_interceptions", "lng": "passing_long", "sck": "sacks_suffered", "scky": "sack_yards_lost", "td": "passing_tds", "yds": "passing_yards"}


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _compare(pairs):
    info = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a or b)]
    agree = sum(int(a == b) for a, b in info)
    return {"joined_n": len(pairs), "informative_n": len(info), "agree_n": agree, "agree_pct": round(100 * agree / len(info), 2) if info else None, "source_exceeds_target": sum(int(a > b) for a, b in info), "target_exceeds_source": sum(int(a < b) for a, b in info)}


def _measure(con, source_field, canonical, v26):
    aggregation = "MAX" if source_field == "lng" else "SUM"
    sql = f"""WITH s AS (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,{aggregation}(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value FROM src WHERE TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2),v AS (SELECT i.nflcom_slug,v.year season_year,{aggregation}(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) target_value FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id WHERE v.season_type='REG' AND v.\"{canonical}\" IS NOT NULL GROUP BY 1,2) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug,season_year)"""
    return {"source_field": source_field, "canonical": canonical, "witness_form": aggregation, **_compare(con.execute(sql).fetchall())}


def _ratio(con, v26, source_field, numerator, denominator, target_numerator, target_denominator, scale, label):
    sql = f"""WITH s AS (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,SUM(TRY_CAST(\"{numerator}\" AS DOUBLE)) n,SUM(TRY_CAST(\"{denominator}\" AS DOUBLE)) d FROM src WHERE TRY_CAST(\"{numerator}\" AS DOUBLE) IS NOT NULL AND TRY_CAST(\"{denominator}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2),v AS (SELECT i.nflcom_slug,v.year season_year,SUM(TRY_CAST(v.\"{target_numerator}\" AS DOUBLE)) n,SUM(TRY_CAST(v.\"{target_denominator}\" AS DOUBLE)) d FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id WHERE v.season_type='REG' AND v.\"{target_numerator}\" IS NOT NULL AND v.\"{target_denominator}\" IS NOT NULL GROUP BY 1,2) SELECT s.n,s.d,v.n,v.d FROM s JOIN v USING(nflcom_slug,season_year)"""
    rows = con.execute(sql).fetchall()
    return {"source_field": source_field, "source_expression": f"SUM({numerator}) / SUM({denominator})" + (" * 100" if scale == 100 else ""), "canonical_expression": f"SUM({target_numerator}) / SUM({target_denominator})" + (" * 100" if scale == 100 else ""), "canonical": label, "witness_form": "RECOMPUTE", **_compare([(a / b * scale, c / d * scale) for a, b, c, d in rows if b and d])}


def _denominator(con, v26):
    source = con.execute("SELECT COUNT(*) n,SUM(CASE WHEN TRY_CAST(comp AS DOUBLE)<=TRY_CAST(att AS DOUBLE) THEN 1 ELSE 0 END) comp_ok,SUM(CASE WHEN TRY_CAST(\"1st\" AS DOUBLE)<=TRY_CAST(att AS DOUBLE) THEN 1 ELSE 0 END) first_ok FROM src WHERE TRY_CAST(comp AS DOUBLE) IS NOT NULL AND TRY_CAST(att AS DOUBLE) IS NOT NULL AND TRY_CAST(\"1st\" AS DOUBLE) IS NOT NULL").fetchone()
    target = con.execute(f"SELECT COUNT(*) n,SUM(CASE WHEN completions<=attempts THEN 1 ELSE 0 END) comp_ok,SUM(CASE WHEN passing_first_downs<=attempts THEN 1 ELSE 0 END) first_ok FROM read_parquet('{v26}') WHERE season_type='REG' AND completions IS NOT NULL AND attempts IS NOT NULL AND passing_first_downs IS NOT NULL").fetchone()
    return {"source": {"rows": int(source[0]), "satisfy_comp_le_att": int(source[1] or 0), "satisfy_first_downs_le_att": int(source[2] or 0)}, "target_weekly": {"rows": int(target[0]), "satisfy_completions_le_attempts": int(target[1] or 0), "satisfy_passing_first_downs_le_attempts": int(target[2] or 0)}}


def measure():
    con = duckdb.connect()
    source = _q(SOURCE / "**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS SELECT DISTINCT nflcom_slug,season,split_value,_lost_column,\"1st\",\"1st_2\",\"20\",att,avg,comp,int,lng,pct,rate,sck,scky,td,yds FROM read_parquet('{source}',union_by_name=true) WHERE _table='Opponents by Team' AND _layout='player_splits_L5'""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(latest_v26())
    fields = [_measure(con, s, c, v26) for s, c in MAPPINGS.items()]
    candidates = {}
    for source_field, canonicals in {"1st": ("passing_first_downs", "receiving_first_downs", "rushing_first_downs"), "20": ("pass_explosive_20", "rec_explosive_20", "receptions_20_29"), "lng": ("passing_long", "receiving_long", "rushing_long")}.items():
        candidates[source_field] = []
        for canonical in canonicals:
            row = _measure(con, source_field, canonical, v26)
            row["candidate_role"] = "natural" if canonical == MAPPINGS[source_field] else "family contrast"
            candidates[source_field].append(row)
    rates = [_ratio(con, v26, "avg", "yds", "att", "passing_yards", "attempts", 1, "passing_yards / attempts"), _ratio(con, v26, "pct", "comp", "att", "completions", "attempts", 100, "completions / attempts"), _ratio(con, v26, "1st_2", "1st", "att", "passing_first_downs", "attempts", 100, "passing_first_downs / attempts")]
    loss = con.execute("SELECT _lost_column,COUNT(*) n FROM src GROUP BY 1 ORDER BY 1").fetchall()
    rate = con.execute("SELECT COUNT(*) row_n,SUM(CASE WHEN TRY_CAST(rate AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) populated FROM src").fetchone()
    return {"measurement_only": True, "source_table": "Opponents by Team", "layout": "player_splits_L5", "subject_plane": "sum 48 disjoint opponent-team rows to player-season", "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]), "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]), "source_split_values": int(con.execute("SELECT COUNT(DISTINCT split_value) FROM src").fetchone()[0]), "lost_columns": [{"column": str(a) if a is not None else None, "rows": int(b)} for a, b in loss], "fields": fields, "candidate_matrix": candidates, "rates": rates, "denominator_checks": _denominator(con, v26), "excluded_rate": {"rows": int(rate[0]), "populated_n": int(rate[1] or 0), "status": "CAPTURE_LOST"}, "target_aggregation": "SUM weekly REG by (NFL_player_id, year); MAX for long; recompute rates", "no_state_change": True}


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
