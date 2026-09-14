"""Verify named player/team identity rows in the frozen research cache."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def describe(con: duckdb.DuckDBPyConnection, relation: str) -> list[tuple[str, str]]:
    return [(row[0], row[1]) for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def search_relation(con: duckdb.DuckDBPyConnection, relation: str, terms: list[str]) -> dict:
    try:
        schema = describe(con, relation)
    except duckdb.Error:
        return {"relation": relation, "missing": True, "rows": []}
    text_columns = [name for name, typ in schema if "CHAR" in typ.upper() or "TEXT" in typ.upper()]
    if not text_columns:
        return {"relation": relation, "columns": [name for name, _ in schema], "rows": []}
    predicates = []
    for term in terms:
        for column in text_columns:
            predicates.append(
                f"LOWER(CAST(\"{column.replace(chr(34), chr(34) * 2)}\" AS VARCHAR)) LIKE {q('%' + term.lower() + '%')}"
            )
    select_columns = ", ".join(f'"{name.replace(chr(34), chr(34) * 2)}"' for name, _ in schema)
    rows = con.execute(
        f"SELECT {select_columns} FROM {relation} WHERE {' OR '.join(predicates)} LIMIT 200"
    ).fetchall()
    return {
        "relation": relation,
        "columns": [name for name, _ in schema],
        "rows": [dict(zip([name for name, _ in schema], row)) for row in rows],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    con = duckdb.connect()
    corpus = args.root / "corpus_snapshot.duckdb"
    ops = args.root / "ops_cache.duckdb"
    con.execute(f"ATTACH {q(corpus.resolve().as_posix())} AS lake (READ_ONLY)")
    con.execute(f"ATTACH {q(ops.resolve().as_posix())} AS ops (READ_ONLY)")

    result = {
        "cache_root": str(args.root),
        "player_bio_matches": search_relation(
            con, "ops.nfl_historical.player_bio", ["tyler conklin", "rams"]
        ),
    }
    crosswalk = args.root / "native_id_crosswalk.parquet"
    if crosswalk.exists():
        result["native_id_crosswalk_matches"] = search_relation(
            con, f"read_parquet({q(crosswalk.resolve().as_posix())})", ["tyler conklin", "rams"]
        )

    player_columns = {row[0] for row in con.execute("DESCRIBE lake.public.player_fantasy").fetchall()}
    id_columns = [column for column in ("NFL_player_id", "player_id") if column in player_columns]
    result["player_fantasy_columns"] = sorted(player_columns)
    result["player_fantasy_named_rows"] = []
    for match_group in (result["player_bio_matches"], result.get("native_id_crosswalk_matches", {"rows": []})):
        for row in match_group["rows"]:
            player_id = row.get("NFL_player_id")
            if player_id is None or not id_columns:
                continue
            id_column = id_columns[0]
            select = [f"COUNT(*) AS rows", f"COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) AS league_years"]
            for column in ("win", "team_points", "loss", "tie"):
                if column in player_columns:
                    select.append(f"COUNT(*) FILTER (WHERE {column} IS NULL) AS null_{column}")
            result["player_fantasy_named_rows"].append({
                "NFL_player_id": player_id,
                **dict(zip([x.split(" AS ")[-1] for x in select], con.execute(
                    f"SELECT {', '.join(select)} FROM lake.public.player_fantasy WHERE CAST({id_column} AS VARCHAR)={q(str(player_id))}"
                ).fetchone())),
            })

    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))
    con.close()


if __name__ == "__main__":
    main()
