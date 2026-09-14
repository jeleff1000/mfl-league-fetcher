"""Measurement-only witness audit for the NFL.com player-splits Outcomes table."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_splits_outcomes_witness.json"
SOURCE = Path(r"D:/league-history-data/nfl/raw/nflcom/tables/player_splits_unshifted/unshifted.parquet")
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)

MAPPINGS = {
    "L0": {"ast": "def_tackle_assists", "int": "def_interceptions", "pdef": "def_pass_defended", "sck": "def_sacks", "sfty": "def_safeties", "solo": "def_tackles_solo", "tds": "def_int_ret_td", "total": "def_tackles_combined", "yds": "def_interception_yards"},
    "L1": {"ff": "def_fumbles_forced", "fum": "fumbles", "lost": "fumbles_lost", "opp_fr": "fumble_recovery_opp", "own_fr": "fumble_recovery_own"},
    "L2": {"1st": "receiving_first_downs", "20": "rec_explosive_20", "lng": "receiving_long", "rec": "receptions", "td": "receiving_tds", "yds": "receiving_yards"},
    "L3": {"att": "carries", "lng": "rushing_long", "td": "rushing_tds", "yds": "rushing_yards"},
    "L4": {"ret": "kickoff_returns + punt_returns", "yds": "kickoff_return_yards + punt_return_yards", "td": "kickoff_return_tds + punt_return_tds", "lng": "GREATEST(kickoff_return_long, punt_return_long)"},
    "L5": {"1st": "passing_first_downs", "20": "pass_explosive_20", "att": "attempts", "comp": "completions", "int": "passing_interceptions", "lng": "passing_long", "sck": "sacks_suffered", "scky": "sack_yards_lost", "td": "passing_tds", "yds": "passing_yards"},
    "L6": {"blk": "punts_blocked", "lng": "punt_long", "punts": "punts", "yds": "punt_yards"},
    "L7": {"fg_att": "fg_att", "fgm": "fg_made"},
}
TARGETS = {layout: tuple(mapping.values()) for layout, mapping in MAPPINGS.items()}
LOST = {"L0": "lng", "L1": "td", "L2": "40", "L3": "1st", "L4": "fum", "L5": "rate", "L6": "net_avg", "L7": "pct"}


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _compare(rows):
    pairs = [(float(a), float(b)) for a, b in rows if a is not None and b is not None and (a or b)]
    return {"joined_n": len(rows), "informative_n": len(pairs), "agree_n": sum(a == b for a, b in pairs), "agree_pct": round(100 * sum(a == b for a, b in pairs) / len(pairs), 2) if pairs else None, "source_exceeds_target": sum(a > b for a, b in pairs), "target_exceeds_source": sum(a < b for a, b in pairs)}


def _measure(con, source_field, canonical, layout):
    agg = "MAX" if source_field == "lng" else "SUM"
    rows = con.execute(f"SELECT s.source_value,t.target_value FROM (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,{agg}(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value FROM src WHERE _layout='player_splits_{layout}' AND TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2) s JOIN (SELECT nflcom_slug,year season_year,{agg}(TRY_CAST(\"{canonical}\" AS DOUBLE)) target_value FROM target WHERE \"{canonical}\" IS NOT NULL GROUP BY 1,2) t USING(nflcom_slug,season_year)").fetchall()
    return {"source_field": source_field, "canonical": canonical, "witness_form": agg, **_compare(rows)}


def _measure_l4(con, source_field, canonical):
    agg = "MAX" if source_field == "lng" else "SUM"
    if source_field == "lng" and canonical.startswith("GREATEST"):
        target_expr = "GREATEST(MAX(TRY_CAST(\"kickoff_return_long\" AS DOUBLE)), MAX(TRY_CAST(\"punt_return_long\" AS DOUBLE)))"
    elif " + " in canonical:
        left, right = canonical.split(" + ")
        target_expr = f"SUM(TRY_CAST(\"{left}\" AS DOUBLE)) + SUM(TRY_CAST(\"{right}\" AS DOUBLE))"
    else:
        target_expr = f"{agg}(TRY_CAST(\"{canonical}\" AS DOUBLE))"
    rows = con.execute(f"SELECT s.source_value,t.target_value FROM (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,{agg}(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value FROM src WHERE _layout='player_splits_L4' AND TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2) s JOIN (SELECT nflcom_slug,year season_year,{target_expr} target_value FROM target GROUP BY 1,2) t USING(nflcom_slug,season_year)").fetchall()
    return {"source_field": source_field, "canonical": canonical, "witness_form": agg, **_compare(rows)}


def _ratio(con, layout, source_num, source_den, target_num, target_den, scale=1.0):
    rows = con.execute(f"SELECT s.n,s.d,t.n,t.d FROM (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,SUM(TRY_CAST(\"{source_num}\" AS DOUBLE)) n,SUM(TRY_CAST(\"{source_den}\" AS DOUBLE)) d FROM src WHERE _layout='player_splits_{layout}' AND TRY_CAST(\"{source_num}\" AS DOUBLE) IS NOT NULL AND TRY_CAST(\"{source_den}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2) s JOIN (SELECT nflcom_slug,year season_year,SUM(TRY_CAST(\"{target_num}\" AS DOUBLE)) n,SUM(TRY_CAST(\"{target_den}\" AS DOUBLE)) d FROM target WHERE \"{target_num}\" IS NOT NULL AND \"{target_den}\" IS NOT NULL GROUP BY 1,2) t USING(nflcom_slug,season_year)").fetchall()
    pairs = [(scale * a / b, scale * c / d) for a, b, c, d in rows if b and d]
    return {"source_expression": f"SUM({source_num}) / SUM({source_den})", "canonical_expression": f"SUM({target_num}) / SUM({target_den})", "joined_n": len(rows), "informative_n": len(pairs), "agree_n": sum(abs(a - b) <= 0.05 for a, b in pairs), "agree_pct": round(100 * sum(abs(a - b) <= 0.05 for a, b in pairs) / len(pairs), 2) if pairs else None, "source_exceeds_target": sum(a - b > 0.05 for a, b in pairs), "target_exceeds_source": sum(b - a > 0.05 for a, b in pairs)}


def _denominators(con):
    checks = []
    for name, source_num, source_den, target_num, target_den in [
        ("L0 total identity", "total", "solo + ast", None, None),
        ("L1 lost <= fum", "lost", "fum", "fumbles_lost", "fumbles"),
        ("L2 first downs <= receptions", "1st", "rec", "receiving_first_downs", "receptions"),
        ("L3 rushing TD <= carries", "td", "att", "rushing_tds", "carries"),
        ("L4 return TD <= returns", "td", "ret", None, None),
        ("L5 completions <= attempts", "comp", "att", "completions", "attempts"),
        ("L6 blocked punts <= punts", "blk", "punts", "punts_blocked", "punts"),
        ("L7 made FG <= attempts", "fgm", "fg_att", "fg_made", "fg_att"),
    ]:
        if name == "L0 total identity":
            row = con.execute("SELECT COUNT(*),SUM(TRY_CAST(total AS DOUBLE)=TRY_CAST(solo AS DOUBLE)+TRY_CAST(ast AS DOUBLE)) FROM src WHERE _layout='player_splits_L0' AND total IS NOT NULL AND solo IS NOT NULL AND ast IS NOT NULL").fetchone()
            checks.append({"check": name, "source_rows": int(row[0]), "satisfy": int(row[1] or 0)})
        elif name == "L4 return TD <= returns":
            row = con.execute("SELECT COUNT(*),SUM(TRY_CAST(td AS DOUBLE)<=TRY_CAST(ret AS DOUBLE)) FROM src WHERE _layout='player_splits_L4' AND td IS NOT NULL AND ret IS NOT NULL").fetchone()
            checks.append({"check": name, "source_rows": int(row[0]), "satisfy": int(row[1] or 0)})
        else:
            row = con.execute(f"SELECT COUNT(*),SUM(TRY_CAST(\"{source_num}\" AS DOUBLE)<=TRY_CAST(\"{source_den}\" AS DOUBLE)) FROM src WHERE _layout='player_splits_{name[:2].replace(' ','')}' AND \"{source_num}\" IS NOT NULL AND \"{source_den}\" IS NOT NULL").fetchone()
            target = con.execute(f"SELECT COUNT(*),SUM(\"{target_num}\"<=\"{target_den}\") FROM target WHERE \"{target_num}\" IS NOT NULL AND \"{target_den}\" IS NOT NULL").fetchone()
            checks.append({"check": name, "source_rows": int(row[0]), "source_satisfy": int(row[1] or 0), "target_weekly_rows": int(target[0]), "target_satisfy": int(target[1] or 0)})
    return checks


def measure(source_table="Outcomes", split_filter="split_value IN ('Wins','Losses','Ties')", partition="Wins + Losses + Ties"):
    con = duckdb.connect()
    source = _q(SOURCE)
    source_columns = sorted({"nflcom_slug", "season", "split_value", "_layout", "_lost_column"} | {field for mapping in MAPPINGS.values() for field in mapping} | {"g", "fc", "20", "40", "rate", "pct", "avg", "1st_2"})
    selected_source = ",".join(f'"{column}"' for column in source_columns)
    con.execute(f"CREATE TEMP TABLE src AS SELECT DISTINCT {selected_source} FROM read_parquet('{source}') WHERE _table='{source_table}' AND {split_filter}")
    v26 = _q(latest_v26())
    con.execute(f"CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id")
    target_columns = sorted({
        canonical
        for values in TARGETS.values()
        for canonical in values
        if " + " not in canonical and "GREATEST" not in canonical
    } | {"kickoff_returns", "punt_returns", "kickoff_return_yards", "punt_return_yards", "kickoff_return_tds", "punt_return_tds", "kickoff_return_long", "punt_return_long"})
    selected = ",".join(f'v."{column}"' for column in target_columns)
    con.execute(f"CREATE TEMP TABLE target AS SELECT i.nflcom_slug,v.year,{selected} FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id WHERE v.season_type='REG'")
    layouts = {}
    for layout, mapping in MAPPINGS.items():
        measure_fn = _measure_l4 if layout == "L4" else lambda c, s, k: _measure(c, s, k, layout)
        fields = [measure_fn(con, source_field, canonical) for source_field, canonical in mapping.items()]
        candidate_targets_for = {
            "lng": ("GREATEST(kickoff_return_long, punt_return_long)", "kickoff_return_long", "punt_return_long"),
            "ret": ("kickoff_returns + punt_returns", "kickoff_returns", "punt_returns"),
            "yds": ("kickoff_return_yards + punt_return_yards", "kickoff_return_yards", "punt_return_yards"),
            "td": ("kickoff_return_tds + punt_return_tds", "kickoff_return_tds", "punt_return_tds"),
        } if layout == "L4" else {source_field: TARGETS[layout] for source_field in mapping}
        candidates = {source_field: sorted([{**measure_fn(con, source_field, candidate), "is_natural_mapping": candidate == canonical} for candidate in candidate_targets_for[source_field]], key=lambda x: (-(x["agree_pct"] or -1), x["canonical"])) for source_field, canonical in mapping.items()}
        ratios = []
        specs = {"L0": [("yds", "int", "def_interception_yards", "def_interceptions")], "L2": [("yds", "rec", "receiving_yards", "receptions"), ("1st", "rec", "receiving_first_downs", "receptions")], "L3": [("yds", "att", "rushing_yards", "carries")], "L5": [("yds", "att", "passing_yards", "attempts"), ("1st", "att", "passing_first_downs", "attempts")], "L6": [("yds", "punts", "punt_yards", "punts")]}.get(layout, [])
        for source_num, source_den, target_num, target_den in specs:
            ratios.append({"source_field": "avg_or_pct", **_ratio(con, layout, source_num, source_den, target_num, target_den, 100.0 if source_num == "1st" else 1.0)})
        layouts[layout] = {"fields": fields, "candidate_matrix": candidates, "ratios": ratios}
    loss = con.execute("SELECT _layout,_lost_column,COUNT(*) FROM src GROUP BY 1,2 ORDER BY 1").fetchall()
    return {"measurement_only": True, "source_table": source_table, "partition": partition, "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]), "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]), "source_split_values": int(con.execute("SELECT COUNT(DISTINCT split_value) FROM src").fetchone()[0]), "layouts": layouts, "lost_columns": [{"layout": a, "column": b, "rows": int(c)} for a, b, c in loss], "denominator_checks": _denominators(con), "unmapped_fields": {"L0.g": {"status": "NO_EXISTING_CANONICAL_FOUND", "schema_search": ["games_played", "games", "g"], "schema_matches": []}, "L4.fc": {"status": "NO_CURRENT_V26_CANONICAL"}, "L4.20": {"status": "NO_CURRENT_V26_CANONICAL"}, "L4.40": {"status": "NO_CURRENT_V26_CANONICAL"}}, "target_aggregation": "SUM weekly REG by (NFL_player_id, year); MAX for long; recompute rate forms", "no_state_change": True}


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
