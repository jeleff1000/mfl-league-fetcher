"""Overlay a processed source snapshot onto the full public corpus snapshot.

Source playoff/backfill packages are intentionally sparse: some platforms carry only
league settings and matchup rows.  Replacing the public corpus with that sparse fold
silently deletes the player_fantasy population.  This merge keeps the base population
and lets non-null overlay values repair matching rows.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


TABLE_KEYS = {
    "league_settings": ("db_name", "year"),
    "draft": ("db_name", "year", "NFL_player_id", "pick"),
    "transactions": ("db_name", "year", "week", "NFL_player_id", "franchise_id", "transaction_type"),
    "player_fantasy": ("db_name", "year", "week", "NFL_player_id"),
    "matchup": ("db_name", "year", "week", "manager", "franchise_id"),
}


def _tables(con: duckdb.DuckDBPyConnection, db: str) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog='{db}' AND table_schema='public'"
        ).fetchall()
    }


def _columns(con: duckdb.DuckDBPyConnection, db: str, table: str) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f"DESCRIBE {db}.public.{table}").fetchall()}


def merge(
    base: Path,
    overlay: Path,
    out: Path,
    crosswalk: Path | None = None,
    espn_map: Path | None = None,
    aliases: Path | None = None,
) -> None:
    if not base.is_file() or not overlay.is_file():
        raise FileNotFoundError(f"base={base} overlay={overlay}")
    if out.exists():
        out.unlink()
    con = duckdb.connect(str(out))
    con.execute(f"ATTACH '{base.resolve().as_posix()}' AS base (READ_ONLY)")
    con.execute(f"ATTACH '{overlay.resolve().as_posix()}' AS ov (READ_ONLY)")
    con.execute("CREATE SCHEMA public")

    for table, keys in TABLE_KEYS.items():
        in_base = table in _tables(con, "base")
        in_overlay = table in _tables(con, "ov")
        if not in_base and not in_overlay:
            continue
        if not in_base:
            con.execute(f"CREATE TABLE public.{table} AS SELECT * FROM ov.public.{table}")
            continue
        if not in_overlay:
            con.execute(f"CREATE TABLE public.{table} AS SELECT * FROM base.public.{table}")
            continue

        base_types = _columns(con, "base", table)
        overlay_types = _columns(con, "ov", table)
        # Keep the full union schema. Backfill overlays can widen matchup with
        # outcome columns that were absent from the older base snapshot.
        base_cols = list(base_types)
        all_cols = base_cols + [c for c in overlay_types if c not in base_types]

        if table == "player_fantasy":
            # NFL_player_id is precisely the field being repaired, so it cannot
            # be part of the replacement key. The overlay is a complete player
            # population for its targeted league-years; replace those pairs
            # atomically and retain the base population everywhere else.
            overlay_scope = "SELECT DISTINCT db_name, year FROM ov.public.player_fantasy"
            base_select = ", ".join(
                f'b."{c}" AS "{c}"' if c in base_types else f'CAST(NULL AS {overlay_types[c]}) AS "{c}"'
                for c in all_cols
            )
            overlay_select = ", ".join(
                f'o."{c}" AS "{c}"' if c in overlay_types else f'CAST(NULL AS {base_types[c]}) AS "{c}"'
                for c in all_cols
            )
            sql = f"""
              CREATE TABLE public.{table} AS
              SELECT {base_select}
              FROM base.public.{table} b
              WHERE NOT EXISTS (
                SELECT 1 FROM ({overlay_scope}) s
                WHERE s.db_name IS NOT DISTINCT FROM b.db_name
                  AND s.year IS NOT DISTINCT FROM b.year
              )
              UNION ALL
              SELECT {overlay_select}
              FROM ov.public.{table} o
            """
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM public.{table}").fetchone()[0]
            print(f"[corpus-merge] {table}: {n:,} rows (league-year replacement)", flush=True)
            continue

        key_pred = " AND ".join(
            f"o.\"{k}\" IS NOT DISTINCT FROM b.\"{k}\"" for k in keys
        )
        select_cols = ", ".join(
            (f'COALESCE(o."{c}", b."{c}") AS "{c}"'
             if c in overlay_types and c in base_types else
             f'b."{c}" AS "{c}"' if c in base_types else
             f'o."{c}" AS "{c}"')
            for c in all_cols
        )
        # Existing rows are retained with non-null overlay repairs; overlay-only rows
        # are appended.  The second arm is deliberately restricted to base columns so
        # the output schema remains the full base contract.
        sql = f"""
          CREATE TABLE public.{table} AS
          SELECT {select_cols}
          FROM base.public.{table} b
          LEFT JOIN ov.public.{table} o ON {key_pred}
          UNION ALL
          SELECT {', '.join(
              f'o."{c}" AS "{c}"' if c in overlay_types
              else f'CAST(NULL AS {base_types[c]}) AS "{c}"'
              for c in all_cols)}
          FROM ov.public.{table} o
          WHERE NOT EXISTS (
            SELECT 1 FROM base.public.{table} b WHERE {key_pred}
          )
        """
        con.execute(sql)
        n = con.execute(f"SELECT COUNT(*) FROM public.{table}").fetchone()[0]
        print(f"[corpus-merge] {table}: {n:,} rows", flush=True)

    # The ledger is operational only; preserve it when present so future local folds
    # remain resumable, while allowing overlay-only source names to be recorded.
    if "_sources" in _tables(con, "base"):
        con.execute("CREATE TABLE public._sources AS SELECT * FROM base.public._sources")

    # Repair the complete merged population, including league-years that came from
    # the immutable base cache rather than the current sparse overlay.  A previous
    # implementation only applied the crosswalk while folding newly downloaded
    # leagues, leaving old MFL/Fleaflicker rows with null canonical IDs forever.
    output_tables = {
        row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        ).fetchall()
    }
    if {"player_fantasy", "league_settings"} <= output_tables:
        con.execute("""
            CREATE TEMP TABLE _identity_anchors AS
            SELECT
              CAST(p.year AS INTEGER) AS year,
              LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(p.player AS VARCHAR),
                '[^a-zA-Z0-9 ]', '', 'g'), '\\s+', ' ', 'g'),
                ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) AS norm_name,
              CASE UPPER(TRIM(CAST(p.position AS VARCHAR)))
                WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END AS pos_family,
              MIN(NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '')) AS NFL_player_id
            FROM public.player_fantasy p
            JOIN public.league_settings ls USING (db_name, year)
            WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
              AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NOT NULL
              AND NULLIF(TRIM(CAST(p.player AS VARCHAR)), '') IS NOT NULL
              AND UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('QB','RB','WR','TE','K','DEF','DST','HB','FB','PK','D/ST')
            GROUP BY 1, 2, 3
            HAVING COUNT(DISTINCT NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '')) = 1
        """)
        if crosswalk and crosswalk.is_file():
            path = crosswalk.resolve().as_posix().replace("'", "''")
            con.execute(f"""
                CREATE TEMP TABLE _identity_external AS
                SELECT year, name_norm AS norm_name, pos_family,
                       MIN(NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '')) AS NFL_player_id
                FROM read_parquet('{path}')
                WHERE LOWER(TRIM(CAST(platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                  AND name_norm IS NOT NULL AND name_norm <> ''
                  AND pos_family IS NOT NULL AND pos_family <> ''
                  AND NFL_player_id IS NOT NULL
                GROUP BY 1, 2, 3
                HAVING COUNT(DISTINCT NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '')) = 1
            """)
            con.execute("""
                CREATE TEMP TABLE _identity_map AS
                SELECT year, norm_name, pos_family, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                  SELECT year, norm_name, pos_family, NFL_player_id FROM _identity_anchors
                  UNION ALL
                  SELECT year, norm_name, pos_family, NFL_player_id FROM _identity_external
                ) x
                WHERE NFL_player_id IS NOT NULL
                GROUP BY 1, 2, 3
                HAVING COUNT(DISTINCT NFL_player_id) = 1
            """)
        else:
            con.execute("CREATE TEMP TABLE _identity_map AS SELECT * FROM _identity_anchors")
        if espn_map and espn_map.is_file():
            path = espn_map.resolve().as_posix().replace("'", "''")
            con.execute(f"""
                CREATE TEMP TABLE _identity_espn AS
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
                WHERE norm_name IS NOT NULL AND norm_name <> ''
                  AND pos_family IS NOT NULL AND pos_family <> ''
                  AND NFL_player_id IS NOT NULL
                GROUP BY 1, 2
                HAVING COUNT(DISTINCT NFL_player_id) = 1
            """)
        if aliases and aliases.is_file():
            path = aliases.resolve().as_posix().replace("'", "''")
            con.execute(f"""
                CREATE TEMP TABLE _identity_aliases AS
                SELECT LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                         CAST(source_name AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'),
                         '\\s+', ' ', 'g'), ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) AS norm_name,
                       CASE UPPER(TRIM(CAST(position AS VARCHAR)))
                         WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                         WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                         ELSE UPPER(TRIM(CAST(position AS VARCHAR))) END AS pos_family,
                       NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') AS NFL_player_id
                FROM read_csv_auto('{path}', header=true)
                WHERE source_name IS NOT NULL AND position IS NOT NULL AND NFL_player_id IS NOT NULL
            """)
        before = con.execute("""
            SELECT COUNT(*) FROM public.player_fantasy p
            JOIN public.league_settings ls USING (db_name, year)
            WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
              AND UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('QB','RB','WR','TE','K','DEF','DST','HB','FB','PK','D/ST')
              AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
        """).fetchone()[0]
        con.execute("""
            UPDATE public.player_fantasy AS p
            SET NFL_player_id = m.NFL_player_id
            FROM _identity_map m
            WHERE NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
              AND CAST(p.year AS INTEGER) = m.year
              AND LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(p.player AS VARCHAR),
                '[^a-zA-Z0-9 ]', '', 'g'), '\\s+', ' ', 'g'),
                ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) = m.norm_name
              AND (CASE UPPER(TRIM(CAST(p.position AS VARCHAR)))
                WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END) = m.pos_family
              AND EXISTS (
                SELECT 1 FROM public.league_settings ls
                WHERE ls.db_name = p.db_name AND ls.year = p.year
                  AND LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                )
        """)
        if espn_map and espn_map.is_file():
            con.execute("""
                UPDATE public.player_fantasy AS p
                SET NFL_player_id = m.NFL_player_id
                FROM _identity_espn m
                WHERE NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
                  AND LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(p.player AS VARCHAR),
                    '[^a-zA-Z0-9 ]', '', 'g'), '\\s+', ' ', 'g'),
                    ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) = m.norm_name
                  AND (CASE UPPER(TRIM(CAST(p.position AS VARCHAR)))
                    WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                    WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                    ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END) = m.pos_family
                  AND EXISTS (
                    SELECT 1 FROM public.league_settings ls
                    WHERE ls.db_name = p.db_name AND ls.year = p.year
                      AND LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                  )
            """)
        if aliases and aliases.is_file():
            con.execute("""
                UPDATE public.player_fantasy AS p
                SET NFL_player_id = m.NFL_player_id
                FROM _identity_aliases m
                WHERE NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
                  AND LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(p.player AS VARCHAR),
                    '[^a-zA-Z0-9 ]', '', 'g'), '\\s+', ' ', 'g'),
                    ' (iii|iv|ii|jr|sr|v)$', '', 'i'))) = m.norm_name
                  AND (CASE UPPER(TRIM(CAST(p.position AS VARCHAR)))
                    WHEN 'HB' THEN 'RB' WHEN 'FB' THEN 'RB' WHEN 'PK' THEN 'K'
                    WHEN 'DST' THEN 'DEF' WHEN 'D/ST' THEN 'DEF' WHEN 'DEF' THEN 'DEF'
                    ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END) = m.pos_family
                  AND EXISTS (
                    SELECT 1 FROM public.league_settings ls
                    WHERE ls.db_name = p.db_name AND ls.year = p.year
                      AND LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
                  )
            """)
        after = con.execute("""
            SELECT COUNT(*) FROM public.player_fantasy p
            JOIN public.league_settings ls USING (db_name, year)
            WHERE LOWER(TRIM(CAST(ls.platform AS VARCHAR))) IN ('mfl', 'fleaflicker')
              AND UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('QB','RB','WR','TE','K','DEF','DST','HB','FB','PK','D/ST')
              AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NULL
        """).fetchone()[0]
        mapped = before - after
        print(f"[corpus-merge] identity repair: {mapped:,} rows mapped; {after:,} fantasy rows remain unresolved", flush=True)
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--overlay", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--crosswalk", type=Path)
    ap.add_argument("--espn-map", type=Path)
    ap.add_argument("--aliases", type=Path)
    args = ap.parse_args()
    merge(args.base, args.overlay, args.out, args.crosswalk, args.espn_map, args.aliases)


if __name__ == "__main__":
    main()
