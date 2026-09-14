"""
sota_recon/build_nfl_position_from_pfr_v26.py

Derive the weekly super-table position columns from PFR player-page position data:

  position     = display/fantasy eligibility set derived from PFR position data plus
                 evidence-based dual-position season stats.
  nfl_position = historical/granular NFL identity from PFR season `pos` when available,
                 else PFR index primary token.

This fixes raw weekly source drift such as:
  - Benny Friedman flipping RB/TB/QB week to week. Display position includes QB; NFL identity is TB.
  - Don Hutson showing as TE. Display position includes WR; NFL identity is LE/E.

The output is stable within a player-season but can change across seasons. Multi-role eligibility
is encoded as a sorted comma-separated set, e.g. K,QB or DB,WR.

    python -m scripts.sota_recon.build_nfl_position_from_pfr_v26
    python -m scripts.sota_recon.build_nfl_position_from_pfr_v26 --apply
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26
from .position_tokens import canonical_position_string_sql

PROV = "wave57.nfl_position_from_pfr"
PFR_ROOT = Path("D:/league-history-data/nfl/raw/pfr/players")
BIO = Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")

PFR_SEASON_TABLES = [
    ("games_played", 100),
    ("passing", 80),
    ("receiving_and_rushing", 80),
    ("rushing_and_receiving", 80),
    ("scoring", 75),
    ("defense", 70),
    ("kicking", 65),
    ("punting", 65),
    ("returns", 60),
]


def _table_path(table: str) -> Path:
    return PFR_ROOT / "tables" / table / "_combined.parquet"


def _existing_pfr_tables() -> list[tuple[str, int, Path]]:
    out: list[tuple[str, int, Path]] = []
    for table, priority in PFR_SEASON_TABLES:
        path = _table_path(table)
        if path.exists():
            out.append((table, priority, path))
    return out


def _year_expr(column: str) -> str:
    return f"TRY_CAST(NULLIF(TRIM(CAST({column} AS VARCHAR)), '') AS INTEGER)"


def _clean_pos_expr(column: str) -> str:
    return f"UPPER(TRIM(CAST({column} AS VARCHAR)))"


def _fantasy_pos_case(raw_pos: str, fallback: str = "w.nfl_position") -> str:
    # The expression is intentionally SQL-only so it can run inside the streaming parquet rewrite.
    # Composite PFR positions like RCB/WR should become DB,WR directly. This keeps real
    # dual-role seasons PFR-led instead of depending on low-volume stat side effects.
    return f"""
    CASE
      WHEN {raw_pos} IS NULL OR TRIM({raw_pos}) = '' OR {raw_pos} = '/' THEN {fallback}
      ELSE COALESCE(NULLIF(array_to_string(list_sort(list_distinct(list_concat(
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(WR|FL|SE|WB|E|LE|RE)([-/, ]|$)') THEN ['WR'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(TE)([-/, ]|$)') THEN ['TE'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(QB|TB|BB)([-/, ]|$)') THEN ['QB'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(RB|HB|FB|B|LH|RH)([-/, ]|$)') THEN ['RB'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(K)([-/, ]|$)') THEN ['K'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(P)([-/, ]|$)') THEN ['P'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(CB|DB|S|FS|SS|SAF|LCB|RCB|LDH|RDH|DH)([-/, ]|$)') THEN ['DB'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(LB|MLB|LLB|RLB|OLB|ILB)([-/, ]|$)') THEN ['LB'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(DE|LDE|RDE|DT|LDT|RDT|NT|DL|EDGE|MG|DG)([-/, ]|$)') THEN ['DL'] ELSE CAST([] AS VARCHAR[]) END,
        CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(OL|C|G|T|OT|OG|LG|RG|LT|RT|LS)([-/, ]|$)') THEN ['OL'] ELSE CAST([] AS VARCHAR[]) END
      ))), ','), ''), {fallback})
    END
    """


def _canonical_fantasy_pos_case(raw_pos: str, fallback: str = "w.nfl_position") -> str:
    mapped_list = f"""
    list_concat(
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(WR|FL|SE|WB|E|LE|RE)([-/, ]|$)') THEN ['WR'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(TE)([-/, ]|$)') THEN ['TE'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(QB|TB|BB)([-/, ]|$)') THEN ['QB'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(RB|HB|FB|B|LH|RH)([-/, ]|$)') THEN ['RB'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(K)([-/, ]|$)') THEN ['K'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(P)([-/, ]|$)') THEN ['P'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(CB|DB|S|FS|SS|SAF|LCB|RCB|LDH|RDH|DH)([-/, ]|$)') THEN ['DB'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(LB|MLB|LLB|RLB|OLB|ILB)([-/, ]|$)') THEN ['LB'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(DE|LDE|RDE|DT|LDT|RDT|NT|DL|EDGE|MG|DG)([-/, ]|$)') THEN ['DL'] ELSE CAST([] AS VARCHAR[]) END,
      CASE WHEN regexp_matches({raw_pos}, '(^|[-/, ])(OL|C|G|T|OT|OG|LG|RG|LT|RT|LS)([-/, ]|$)') THEN ['OL'] ELSE CAST([] AS VARCHAR[]) END
    )
    """
    mapped = canonical_position_string_sql(mapped_list)
    fallback_ordered = canonical_position_string_sql(f"string_split(COALESCE(CAST({fallback} AS VARCHAR), ''), ',')")
    return f"""
    CASE
      WHEN {raw_pos} IS NULL OR TRIM({raw_pos}) = '' OR {raw_pos} = '/' THEN COALESCE({fallback_ordered}, {fallback})
      ELSE COALESCE({mapped}, {fallback_ordered}, {fallback})
    END
    """


_fantasy_pos_case = _canonical_fantasy_pos_case


def _build_pfr_sources(con: duckdb.DuckDBPyConnection, vq: str) -> None:
    pfr_index = (PFR_ROOT / "player_index.parquet").as_posix()
    bio = BIO.as_posix()

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE super_ids AS
        SELECT DISTINCT NFL_player_id
        FROM '{vq}'
        WHERE NFL_player_id IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE bio_bridge AS
        SELECT NFL_player_id, pfr_id
        FROM '{bio}'
        WHERE NFL_player_id IS NOT NULL
          AND pfr_id IS NOT NULL
          AND TRIM(CAST(pfr_id AS VARCHAR)) <> ''
    """)

    union_parts: list[str] = []
    for table, priority, path in _existing_pfr_tables():
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()}
        if "pfr_id" not in cols or "year_id" not in cols or "pos" not in cols:
            continue
        raw_pos = _clean_pos_expr("t.pos")
        union_parts.append(f"""
            SELECT
              COALESCE(sd.NFL_player_id, sn.NFL_player_id, b.NFL_player_id) AS NFL_player_id,
              {_year_expr("t.year_id")} AS year,
              {raw_pos} AS pfr_pos,
              {priority} AS priority,
              '{table}' AS source_table
            FROM '{path.as_posix()}' t
            LEFT JOIN super_ids sd ON sd.NFL_player_id = t.pfr_id
            LEFT JOIN super_ids sn ON sn.NFL_player_id = t.NFL_player_id
            LEFT JOIN bio_bridge b ON b.pfr_id = t.pfr_id
            WHERE COALESCE(sd.NFL_player_id, sn.NFL_player_id, b.NFL_player_id) IS NOT NULL
              AND {_year_expr("t.year_id")} IS NOT NULL
              AND {raw_pos} NOT IN ('', '/', 'NA', 'N/A', 'NULL')
        """)

    if not union_parts:
        raise RuntimeError("No usable PFR season-position tables found.")

    con.execute("CREATE OR REPLACE TEMP TABLE pfr_pos_raw AS " + "\nUNION ALL\n".join(union_parts))
    con.execute("""
        CREATE OR REPLACE TEMP TABLE pfr_season_pos AS
        WITH votes AS (
          SELECT NFL_player_id, year, pfr_pos,
                 COUNT(*) AS votes,
                 MAX(priority) AS max_priority,
                 string_agg(DISTINCT source_table, ',') AS sources
          FROM pfr_pos_raw
          GROUP BY 1,2,3
        )
        SELECT NFL_player_id, year, pfr_pos, sources
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY NFL_player_id, year
                   ORDER BY votes DESC, max_priority DESC, pfr_pos
                 ) AS rn
          FROM votes
        )
        WHERE rn = 1
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pfr_index_pos AS
        WITH idx AS (
          SELECT pfr_id, {_clean_pos_expr("split_part(index_position, '-', 1)")} AS index_pos
          FROM '{pfr_index}'
          WHERE index_position IS NOT NULL
            AND TRIM(CAST(index_position AS VARCHAR)) <> ''
        ),
        matched AS (
          SELECT COALESCE(sd.NFL_player_id, b.NFL_player_id) AS NFL_player_id, idx.index_pos
          FROM idx
          LEFT JOIN super_ids sd ON sd.NFL_player_id = idx.pfr_id
          LEFT JOIN bio_bridge b ON b.pfr_id = idx.pfr_id
          WHERE COALESCE(sd.NFL_player_id, b.NFL_player_id) IS NOT NULL
            AND idx.index_pos NOT IN ('', '/', 'NA', 'N/A', 'NULL')
        )
        SELECT NFL_player_id, MIN(index_pos) AS index_pos
        FROM matched
        GROUP BY NFL_player_id
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE final_pos AS
        WITH current AS (
          SELECT NFL_player_id, year,
                 ANY_VALUE(position) AS position,
                 ANY_VALUE(nfl_position) AS nfl_position,
                 SUM(COALESCE(TRY_CAST(fg_att AS DOUBLE),0)) AS fga,
                 SUM(COALESCE(TRY_CAST(pat_made AS DOUBLE),0)) AS xpm,
                 SUM(COALESCE(TRY_CAST(punts AS DOUBLE),0)) AS punts,
                 SUM(COALESCE(TRY_CAST(def_tackles_solo AS DOUBLE),0)) AS solo,
                 SUM(COALESCE(TRY_CAST(def_tackle_assists AS DOUBLE),0)) AS ast,
                 SUM(COALESCE(TRY_CAST(def_tackles_with_assist AS DOUBLE),0)) AS comb_tackles,
                 SUM(COALESCE(TRY_CAST(def_pass_defended AS DOUBLE),0)) AS pd,
                 SUM(COALESCE(TRY_CAST(def_interceptions AS DOUBLE),0)) AS di
          FROM '{vq}'
          WHERE NFL_player_id IS NOT NULL AND year IS NOT NULL
          GROUP BY 1,2
        ),
        base AS (
          SELECT
            w.*,
            ({_fantasy_pos_case("COALESCE(s.pfr_pos, i.index_pos, w.nfl_position, w.position)", "w.position")}) AS base_position,
            COALESCE(s.pfr_pos, i.index_pos, w.nfl_position, w.position) AS pfr_position
          FROM current w
          LEFT JOIN pfr_season_pos s ON s.NFL_player_id = w.NFL_player_id AND s.year = w.year
          LEFT JOIN pfr_index_pos i ON i.NFL_player_id = w.NFL_player_id
        )
        SELECT
          NFL_player_id,
          year,
          {canonical_position_string_sql("""list_concat(
            CASE WHEN base_position IS NOT NULL AND TRIM(base_position) <> ''
                 THEN string_split(base_position, ',') ELSE CAST([] AS VARCHAR[]) END,
            CASE WHEN fga >= 5 OR xpm >= 5 THEN ['K'] ELSE CAST([] AS VARCHAR[]) END,
            CASE WHEN punts >= 5 THEN ['P'] ELSE CAST([] AS VARCHAR[]) END,
            CASE WHEN list_has_any(string_split(COALESCE(base_position, ''), ','), ['QB','RB','WR','TE'])
                   AND NOT list_has_any(string_split(COALESCE(base_position, ''), ','), ['DB','LB','DL'])
                   AND (
                     (pd >= 3 AND GREATEST(solo + ast, comb_tackles) >= 10)
                     OR di >= 3
                   )
                 THEN ['DB'] ELSE CAST([] AS VARCHAR[]) END
          )""")} AS fantasy_position,
          pfr_position
        FROM base
    """)


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA threads=3")
    _build_pfr_sources(con, vq)

    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    matched_seasons = con.execute("""
        SELECT COUNT(*) FROM pfr_season_pos
    """).fetchone()[0]
    if not apply:
        changed = con.execute(f"""
            SELECT
              SUM(CASE WHEN w.position IS DISTINCT FROM p.fantasy_position THEN 1 ELSE 0 END) AS position_rows_changed,
              SUM(CASE WHEN w.nfl_position IS DISTINCT FROM p.pfr_position THEN 1 ELSE 0 END) AS nfl_position_rows_changed
            FROM '{vq}' w
            JOIN final_pos p ON p.NFL_player_id = w.NFL_player_id AND p.year = w.year
        """).fetchone()
        anchors = _anchors(con, vq)
        con.close()
        return {
            "rows": before_rows,
            "pfr_player_seasons": matched_seasons,
            "position_rows_changed": changed[0],
            "nfl_position_rows_changed": changed[1],
            "anchors_before": anchors,
        }

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'")

    wkcols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = [
        "COALESCE(p.fantasy_position, w.position) AS position",
        "COALESCE(p.pfr_position, w.nfl_position) AS nfl_position",
    ]
    if "recon_correction_log" in wkcols:
        repl.append(f"""
            CASE
              WHEN (p.fantasy_position IS NOT NULL AND p.fantasy_position IS DISTINCT FROM w.position)
                OR (p.pfr_position IS NOT NULL AND p.pfr_position IS DISTINCT FROM w.nfl_position)
              THEN CASE
                WHEN w.recon_correction_log IS NULL OR w.recon_correction_log = '' THEN '{PROV}'
                ELSE w.recon_correction_log || ',{PROV}'
              END
              ELSE w.recon_correction_log
            END AS recon_correction_log
        """)

    out_sql = f"""
        SELECT w.* REPLACE ({", ".join(repl)})
        FROM '{vq}' w
        LEFT JOIN final_pos p ON p.NFL_player_id = w.NFL_player_id AND p.year = w.year
    """
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_pfrpos.parquet")
    rb = con.execute(out_sql).fetch_record_batch(50_000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()

    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    multi_position_seasons = con.execute(f"""
        SELECT COUNT(*)
        FROM (
          SELECT NFL_player_id, year
          FROM '{tq}'
          WHERE NFL_player_id IS NOT NULL AND year IS NOT NULL AND position IS NOT NULL
          GROUP BY 1,2
          HAVING COUNT(DISTINCT position) > 1
        )
    """).fetchone()[0]
    multi_nfl_position_seasons = con.execute(f"""
        SELECT COUNT(*)
        FROM (
          SELECT NFL_player_id, year
          FROM '{tq}'
          WHERE NFL_player_id IS NOT NULL AND year IS NOT NULL AND nfl_position IS NOT NULL
          GROUP BY 1,2
          HAVING COUNT(DISTINCT nfl_position) > 1
        )
    """).fetchone()[0]
    cross_season_changers = con.execute(f"""
        SELECT COUNT(*)
        FROM (
          SELECT NFL_player_id
          FROM '{tq}'
          WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL
          GROUP BY 1
          HAVING COUNT(DISTINCT position) > 1
        )
    """).fetchone()[0]
    anchors = _anchors(con, tq)
    ordered_anchor_ok = all(
        expected in anchors["season_positions"]
        for expected in (
            "Don Hutson 1942=WR,K,DB",
            "George Blanda 1962=QB,K",
            "Cookie Gilchrist 1962=RB,K",
            "Deion Sanders 1993=DB",
            "Deion Sanders 1996=WR,DB",
            "Devontez Walker 2025=WR",
            "Travis Hunter 2025=WR,DB",
        )
    )
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as sources

    old_latest = sources.latest_v26
    sources.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        sources.latest_v26 = old_latest

    gate = (
        golden["failed"] == 0
        and after_rows == before_rows
        and multi_position_seasons == 0
        and multi_nfl_position_seasons == 0
        and cross_season_changers > 0
        and all("QB" in p.split(",") for p in anchors["friedman_position"])
        and anchors["friedman_nfl_position"] == ["TB"]
        and all("WR" in p.split(",") for p in anchors["hutson_position"])
        and "TE" not in anchors["hutson_nfl_position"]
        and any("QB" in p.split(",") for p in anchors["pryor_position"])
        and any("WR" in p.split(",") for p in anchors["pryor_position"])
        and any("DB" in p.split(",") for p in anchors["deion_position"])
        and any("WR" in p.split(",") for p in anchors["deion_position"])
        and anchors["walker_position"] == ["WR"]
        and anchors["walker_nfl_position"] == ["WR"]
        and any("DB" in p.split(",") for p in anchors["hunter_position"])
        and any("WR" in p.split(",") for p in anchors["hunter_position"])
        and ordered_anchor_ok
    )

    result = {
        "rows": after_rows,
        "pfr_player_seasons": matched_seasons,
        "player_seasons_multi_position": multi_position_seasons,
        "player_seasons_multi_nfl_position": multi_nfl_position_seasons,
        "cross_season_changers": cross_season_changers,
        "anchors": anchors,
        "ordered_anchor_ok": ordered_anchor_ok,
        "golden": f"{golden['passed']}/{golden['total']}",
        "gate_pass": bool(gate),
    }
    if gate:
        backup = vp.with_name(vp.stem + f"_prepfrpos_{stamp}.parquet")
        shutil.copy2(vp, backup)
        os.replace(tmp, vp)
        result["backup"] = backup.name
        result["swapped"] = True
    else:
        result["temp"] = str(tmp)
        result["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    return result


def _distinct(con: duckdb.DuckDBPyConnection, table: str, pid_or_name: str, column: str) -> list[str]:
    rows = con.execute(
        f"""
        SELECT DISTINCT {column}
        FROM '{table}'
        WHERE (NFL_player_id = ? OR player = ?)
          AND {column} IS NOT NULL
        ORDER BY 1
    """,
        [pid_or_name, pid_or_name],
    ).fetchall()
    return [str(r[0]) for r in rows]


def _anchors(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, list[str]]:
    anchors = {
        "friedman_position": _distinct(con, table, "FrieBe20", "position"),
        "friedman_nfl_position": _distinct(con, table, "FrieBe20", "nfl_position"),
        "hutson_position": _distinct(con, table, "HutsDo00", "position"),
        "hutson_nfl_position": _distinct(con, table, "HutsDo00", "nfl_position"),
        "pryor_position": _distinct(con, table, "Terrelle Pryor", "position"),
        "pryor_nfl_position": _distinct(con, table, "Terrelle Pryor", "nfl_position"),
        "deion_position": _distinct(con, table, "Deion Sanders", "position"),
        "deion_nfl_position": _distinct(con, table, "Deion Sanders", "nfl_position"),
        "walker_position": _distinct(con, table, "Devontez Walker", "position"),
        "walker_nfl_position": _distinct(con, table, "Devontez Walker", "nfl_position"),
        "blanda_position": _distinct(con, table, "George Blanda", "position"),
        "blanda_nfl_position": _distinct(con, table, "George Blanda", "nfl_position"),
        "hunter_position": _distinct(con, table, "Travis Hunter", "position"),
        "hunter_nfl_position": _distinct(con, table, "Travis Hunter", "nfl_position"),
    }
    season_rows = con.execute(
        f"""
        SELECT player, CAST(year AS INTEGER) AS year, array_to_string(list_sort(list_distinct(array_agg(position))), '|') AS positions
        FROM '{table}'
        WHERE (NFL_player_id IN ('HutsDo00','BLA674817','GilcCo00','00-0039792','00-0040718','00-0014324')
               OR player IN ('Don Hutson','George Blanda','Cookie Gilchrist','Devontez Walker','Travis Hunter','Deion Sanders'))
          AND year IN (1942, 1962, 1993, 1996, 2025)
        GROUP BY player, year
        ORDER BY player, year
        """
    ).fetchall()
    anchors["season_positions"] = [f"{row[0]} {row[1]}={row[2]}" for row in season_rows]
    return anchors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(run(apply=args.apply))
