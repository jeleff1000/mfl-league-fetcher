"""Audit native MFL/Fleaflicker identity repairs in a public research cache.

This is intentionally narrower than the matchup builder: it reads only the raw player
identity columns for an explicit target list and reports how many null canonical IDs are
resolvable from an unambiguous year-scoped native-ID map.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def _ident(source: str) -> str:
    return source.replace("'", "''")


def _columns(con: duckdb.DuckDBPyConnection, source: str) -> set[str]:
    return {row[0] for row in con.execute(
        f"DESCRIBE {source}.public.player_fantasy"
    ).fetchall()}


def audit(
    snapshot: Path,
    corpus: Path | None,
    db_names: list[str],
    crosswalk: Path | None = None,
    espn_map: Path | None = None,
    aliases: Path | None = None,
) -> dict:
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{_ident(snapshot.resolve().as_posix())}' AS snap (READ_ONLY)")
        sources = ["snap"]
        if corpus and corpus.is_file():
            con.execute(f"ATTACH '{_ident(corpus.resolve().as_posix())}' AS corp (READ_ONLY)")
            sources.append("corp")
        con.execute("CREATE TEMP TABLE targets(db_name VARCHAR PRIMARY KEY)")
        con.executemany("INSERT INTO targets VALUES (?)", [(name,) for name in sorted(set(db_names))])
        external_rows = 0
        if crosswalk and crosswalk.is_file():
            path = _ident(crosswalk.resolve().as_posix())
            con.execute(f"""CREATE TEMP TABLE external_native_map AS
                SELECT LOWER(TRIM(CAST(platform AS VARCHAR))) AS platform,
                       CAST(year AS INTEGER) AS year,
                       NULLIF(TRIM(CAST(native_id AS VARCHAR)), '') AS pid,
                       NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') AS NFL_player_id
                FROM read_parquet('{path}')
                WHERE platform IS NOT NULL AND year IS NOT NULL
                  AND native_id IS NOT NULL AND NFL_player_id IS NOT NULL""")
            external_rows = con.execute("SELECT COUNT(*) FROM external_native_map").fetchone()[0]

        arms = []
        for source in sources:
            have = _columns(con, source)
            def col(name: str) -> str:
                return f'CAST(p."{name}" AS VARCHAR)' if name in have else "CAST(NULL AS VARCHAR)"
            if not {"db_name", "year", "NFL_player_id"} <= have:
                continue
            arms.append(f"""
                SELECT p.db_name, CAST(p.year AS INTEGER) AS year,
                       {col('NFL_player_id')} AS NFL_player_id,
                       {col('fleaflicker_player_id')} AS fleaflicker_player_id,
                       {col('mfl_player_id')} AS mfl_player_id
                FROM {source}.public.player_fantasy p
                JOIN targets t ON t.db_name=p.db_name
            """)
        if not arms:
            raise RuntimeError("no compatible player_fantasy source found")
        con.execute("CREATE TEMP TABLE raw AS " + " UNION ALL ".join(arms))

        result = {"targets": sorted(set(db_names)), "platforms": {}}
        for native_col, platform in (
            ("fleaflicker_player_id", "fleaflicker"),
            ("mfl_player_id", "mfl"),
        ):
            con.execute("DROP TABLE IF EXISTS native_map")
            external_platform = "fleaflicker" if platform == "fleaflicker" else "mfl"
            external_arm = (
                f" UNION ALL SELECT year, pid, NFL_player_id FROM external_native_map WHERE platform='{external_platform}'"
                if crosswalk and crosswalk.is_file() else ""
            )
            con.execute(f"""CREATE TEMP TABLE native_map AS
                SELECT year, pid, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                    SELECT year, NULLIF(TRIM({native_col}), '') AS pid,
                           NULLIF(TRIM(NFL_player_id), '') AS NFL_player_id
                    FROM raw
                    WHERE {native_col} IS NOT NULL AND NULLIF(TRIM(NFL_player_id), '') IS NOT NULL
                    {external_arm}
                ) ids
                WHERE pid IS NOT NULL AND NFL_player_id IS NOT NULL
                GROUP BY 1, 2
                HAVING COUNT(DISTINCT NFL_player_id) = 1""")
            row = con.execute(f"""
                SELECT
                  COUNT(*) FILTER (WHERE r.{native_col} IS NOT NULL) AS native_rows,
                  COUNT(*) FILTER (WHERE r.{native_col} IS NOT NULL AND NULLIF(TRIM(r.NFL_player_id), '') IS NULL) AS null_native_rows,
                  COUNT(*) FILTER (WHERE r.{native_col} IS NOT NULL AND NULLIF(TRIM(r.NFL_player_id), '') IS NULL
                                   AND m.NFL_player_id IS NOT NULL) AS rescued_rows,
                  COUNT(DISTINCT r.{native_col}) FILTER (WHERE r.{native_col} IS NOT NULL
                                   AND NULLIF(TRIM(r.NFL_player_id), '') IS NULL
                                   AND m.NFL_player_id IS NOT NULL) AS rescued_ids,
                  COUNT(*) FILTER (WHERE r.{native_col} IS NOT NULL AND NULLIF(TRIM(r.NFL_player_id), '') IS NULL
                                   AND m.NFL_player_id IS NULL) AS unresolved_rows,
                  (SELECT COUNT(*) FROM native_map) AS map_rows
                FROM raw r
                LEFT JOIN native_map m ON m.year=r.year AND m.pid=NULLIF(TRIM(r.{native_col}), '')
            """).fetchone()
            result["platforms"][platform] = dict(zip(
                ("native_rows", "null_native_rows", "rescued_rows", "rescued_ids",
                 "unresolved_rows", "map_rows"), row
            ))

        # The folded public snapshot is the authoritative output of the repair.
        # Older MFL/Fleaflicker source rows often have no native ID column, so the
        # native-map audit above cannot prove that fold-time name/position repair
        # was applied.  Count the canonical IDs in the folded population directly.
        folded_have = _columns(con, "snap")
        if {"NFL_player_id", "position", "db_name", "year"} <= folded_have:
            fantasy_positions = "('QB', 'RB', 'WR', 'TE', 'K', 'DEF', 'DST')"
            folded_rows = con.execute(f"""
                SELECT
                  LOWER(TRIM(CAST(ls.platform AS VARCHAR))) AS platform,
                  COUNT(*) AS rows,
                  COUNT(*) FILTER (WHERE NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL) AS identified_rows,
                  COUNT(*) FILTER (
                    WHERE UPPER(TRIM(CAST(position AS VARCHAR))) IN {fantasy_positions}
                  ) AS fantasy_rows,
                  COUNT(*) FILTER (
                    WHERE UPPER(TRIM(CAST(position AS VARCHAR))) IN {fantasy_positions}
                      AND NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL
                  ) AS fantasy_identified_rows,
                  COUNT(*) FILTER (
                    WHERE UPPER(TRIM(CAST(position AS VARCHAR))) IN {fantasy_positions}
                      AND NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NULL
                  ) AS fantasy_unresolved_rows,
                  COUNT(DISTINCT db_name) AS league_count,
                  COUNT(DISTINCT year) AS year_count
                FROM snap.public.player_fantasy p
                JOIN snap.public.league_settings ls USING (db_name, year)
                WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                GROUP BY 1
                ORDER BY 1
            """).fetchall()
            result["folded_population"] = [
                dict(zip(("platform", "rows", "identified_rows", "fantasy_rows",
                          "fantasy_identified_rows", "fantasy_unresolved_rows",
                          "league_count", "year_count"), row))
                for row in folded_rows
            ]
            unresolved = con.execute(f"""
                SELECT
                  LOWER(TRIM(CAST(ls.platform AS VARCHAR))) AS platform,
                  UPPER(TRIM(CAST(p.position AS VARCHAR))) AS position,
                  NULLIF(TRIM(CAST(p.player AS VARCHAR)), '') AS player,
                  COUNT(*) AS rows,
                  COUNT(DISTINCT p.db_name || ':' || CAST(p.year AS VARCHAR)) AS league_years
                FROM snap.public.player_fantasy p
                JOIN snap.public.league_settings ls USING (db_name, year)
                WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                  AND UPPER(TRIM(CAST(p.position AS VARCHAR))) IN {fantasy_positions}
                  AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
                GROUP BY 1, 2, 3
                ORDER BY rows DESC, platform, position, player
                LIMIT 5000
            """).fetchall()
            result["folded_unresolved_examples"] = [
                dict(zip(("platform", "position", "player", "rows", "league_years"), row))
                for row in unresolved
            ]
            if espn_map and espn_map.is_file():
                path = espn_map.resolve().as_posix().replace("'", "''")
                con.execute(f"""
                    CREATE TEMP TABLE audit_espn_map AS
                    SELECT norm_name, pos_family,
                           MIN(NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '')) AS NFL_player_id
                    FROM (
                      SELECT LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                               CAST(espn_name AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'),
                               '\\s+', ' ', 'g'), ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) AS norm_name,
                             CASE UPPER(TRIM(CAST(position AS VARCHAR)))
                               WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                               WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                               ELSE UPPER(TRIM(CAST(position AS VARCHAR))) END AS pos_family,
                             NFL_player_id
                      FROM read_parquet('{path}')
                      UNION ALL
                      SELECT LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                               CAST(nfl_name AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'),
                               '\\s+', ' ', 'g'), ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) AS norm_name,
                             CASE UPPER(TRIM(CAST(position AS VARCHAR)))
                               WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                               WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                               ELSE UPPER(TRIM(CAST(position AS VARCHAR))) END AS pos_family,
                             NFL_player_id
                      FROM read_parquet('{path}')
                    ) x
                    WHERE norm_name IS NOT NULL AND norm_name <> '' AND pos_family IS NOT NULL
                      AND pos_family <> '' AND NFL_player_id IS NOT NULL
                    GROUP BY 1, 2
                    HAVING COUNT(DISTINCT NFL_player_id) = 1
                """)
                if aliases and aliases.is_file():
                    alias_path = aliases.resolve().as_posix().replace("'", "''")
                    con.execute(f"""
                        INSERT INTO audit_espn_map
                        SELECT LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                                 CAST(source_name AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'),
                                 '\\s+', ' ', 'g'), ' (iii|iv|ii|jr|sr|v)$', '', 'i'))),
                               CASE UPPER(TRIM(CAST(position AS VARCHAR)))
                                 WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                                 WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                                 ELSE UPPER(TRIM(CAST(position AS VARCHAR))) END,
                               NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '')
                        FROM read_csv_auto('{alias_path}', header=true)
                        WHERE source_name IS NOT NULL AND position IS NOT NULL AND NFL_player_id IS NOT NULL
                    """)
                espn_rows = con.execute(f"""
                    SELECT LOWER(TRIM(CAST(ls.platform AS VARCHAR))) AS platform,
                           COUNT(*) AS rows,
                           COUNT(DISTINCT p.player) AS players
                    FROM snap.public.player_fantasy p
                    JOIN snap.public.league_settings ls USING (db_name, year)
                    JOIN audit_espn_map e
                      ON e.norm_name = LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                           CAST(p.player AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'), '\\s+', ' ', 'g'),
                           ' (iii|iv|ii|jr|sr|v)$', '', 'i')))
                     AND e.pos_family = CASE UPPER(TRIM(CAST(p.position AS VARCHAR)))
                           WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                           WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                           ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END
                    WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                      AND UPPER(TRIM(CAST(p.position AS VARCHAR))) IN {fantasy_positions}
                      AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
                    GROUP BY 1 ORDER BY 1
                """).fetchall()
                result["espn_resolvable_unresolved"] = [
                    dict(zip(("platform", "rows", "players"), row)) for row in espn_rows
                ]
                residual = con.execute(f"""
                    SELECT LOWER(TRIM(CAST(ls.platform AS VARCHAR))) AS platform,
                           UPPER(TRIM(CAST(p.position AS VARCHAR))) AS position,
                           NULLIF(TRIM(CAST(p.player AS VARCHAR)), '') AS player,
                           COUNT(*) AS rows,
                           COUNT(DISTINCT p.db_name || ':' || CAST(p.year AS VARCHAR)) AS league_years
                    FROM snap.public.player_fantasy p
                    JOIN snap.public.league_settings ls USING (db_name, year)
                    LEFT JOIN audit_espn_map e
                      ON e.norm_name = LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                           CAST(p.player AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'), '\\s+', ' ', 'g'),
                           ' (iii|iv|ii|jr|sr|v)$', '', 'i')))
                     AND e.pos_family = CASE UPPER(TRIM(CAST(p.position AS VARCHAR)))
                           WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                           WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                           ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END
                    WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                      AND UPPER(TRIM(CAST(p.position AS VARCHAR))) IN {fantasy_positions}
                      AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
                      AND e.NFL_player_id IS NULL
                    GROUP BY 1, 2, 3
                    ORDER BY rows DESC, platform, position, player
                """).fetchall()
                result["espn_unresolved_examples"] = [
                    dict(zip(("platform", "position", "player", "rows", "league_years"), row))
                    for row in residual
                ]
        result["external_crosswalk_rows"] = external_rows
        return result
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--corpus", type=Path)
    ap.add_argument("--db-names", required=True, help="comma-separated DB names")
    ap.add_argument("--crosswalk", type=Path)
    ap.add_argument("--espn-map", type=Path)
    ap.add_argument("--aliases", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    names = [x.strip() for x in args.db_names.split(",") if x.strip()]
    if not names:
        raise SystemExit("--db-names is empty")
    result = audit(args.snapshot, args.corpus, names, args.crosswalk, args.espn_map, args.aliases)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
