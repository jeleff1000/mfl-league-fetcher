"""Generate the capture-boundary receipt for raw NFL.com player logs.

This is measurement-only.  It records whether the raw shards retain the two keys needed
for a weekly witness: a game date and the position-group layout discriminator.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .build_pre1978_fumbles_lost_v26 import NICKNAME_CODES
from .nflcom_column_audit import BLOCK_RECOVERY
from .sources import registry


OUT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_capture_inventory.json"
)


# The raw player-log parquet drops the page block label. BLOCK_RECOVERY handles rows
# carrying discriminating header fields; these fallbacks recover partial rows whose
# remaining non-empty fields still identify one header signature. The shared RBFB5/WRTE
# schema stays unresolved when its internal equation is tied. WRTE's rushing-only block
# is separately identifiable because its yards/average/long/TD fields occupy the ``*_2``
# positions, while RBFB5's rushing block occupies the base positions.
AVG_2_RBFB5_FORM = (
    "TRY_CAST(att AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(rec AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(yds AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(avg AS DOUBLE) IS NULL "
    "AND TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL "
    "AND COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) - "
    "TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06, FALSE) "
    "AND NOT COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) - "
    "TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06, FALSE)"
)
AVG_2_WRTE_FORM = (
    "TRY_CAST(att AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(rec AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(yds AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(avg AS DOUBLE) IS NULL "
    "AND TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL "
    "AND COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) - "
    "TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06, FALSE) "
    "AND NOT COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) - "
    "TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06, FALSE)"
)

# Shared RB/WR rows can still carry an independent secondary block rate even when
# the base average satisfies both denominators.  Keep only one-sided forms: a tie is
# not a witness and remains unresolved.
AVG_2_SHARED_RBFB5_FORM = (
    "TRY_CAST(att AS DOUBLE) > 0 AND TRY_CAST(rec AS DOUBLE) > 0 "
    "AND TRY_CAST(avg AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL "
    "AND ABS(TRY_CAST(avg AS DOUBLE) - TRY_CAST(yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(avg AS DOUBLE) - TRY_CAST(yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(avg_2 AS DOUBLE) - TRY_CAST(yds_2 AS DOUBLE) "
    "/ NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06 "
    "AND NOT ABS(TRY_CAST(avg_2 AS DOUBLE) - TRY_CAST(yds_2 AS DOUBLE) "
    "/ NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06"
)
AVG_2_SHARED_WRTE_FORM = (
    "TRY_CAST(att AS DOUBLE) > 0 AND TRY_CAST(rec AS DOUBLE) > 0 "
    "AND TRY_CAST(avg AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL "
    "AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL "
    "AND ABS(TRY_CAST(avg AS DOUBLE) - TRY_CAST(yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(avg AS DOUBLE) - TRY_CAST(yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(avg_2 AS DOUBLE) - TRY_CAST(yds_2 AS DOUBLE) "
    "/ NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06 "
    "AND NOT ABS(TRY_CAST(avg_2 AS DOUBLE) - TRY_CAST(yds_2 AS DOUBLE) "
    "/ NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06"
)

PLAYER_LOG_BLOCK_RECOVERY = f"""CASE
    WHEN ({BLOCK_RECOVERY}) IS NOT NULL THEN ({BLOCK_RECOVERY})
    WHEN ({AVG_2_SHARED_RBFB5_FORM}) THEN 'RBFB5'
    WHEN ({AVG_2_SHARED_WRTE_FORM}) THEN 'WRTE'
    WHEN TRY_CAST(rec AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
     AND TRY_CAST(att AS DOUBLE) IS NULL
     AND TRY_CAST(yds_2 AS DOUBLE) IS NULL THEN 'WRTE'
    WHEN TRY_CAST(rec AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds AS DOUBLE) IS NULL
     AND TRY_CAST(att AS DOUBLE) IS NULL THEN 'RBFB5'
    WHEN TRY_CAST(att AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
     AND TRY_CAST(rec AS DOUBLE) IS NULL
     AND TRY_CAST(yds_2 AS DOUBLE) IS NULL THEN 'RBFB5'
    WHEN TRY_CAST(att AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(lng_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(td_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds AS DOUBLE) IS NULL
     AND TRY_CAST(rec AS DOUBLE) IS NULL THEN 'WRTE'
    WHEN TRY_CAST(att AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(lng_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(td_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(avg_2 AS DOUBLE) IS NULL
     AND TRY_CAST(yds AS DOUBLE) IS NULL
     AND TRY_CAST(rec AS DOUBLE) IS NULL THEN 'WRTE'
    WHEN ({AVG_2_RBFB5_FORM}) THEN 'RBFB5'
    WHEN ({AVG_2_WRTE_FORM}) THEN 'WRTE'
    WHEN TRY_CAST(att_2 AS DOUBLE) IS NOT NULL
     AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL THEN 'QB'
    WHEN TRY_CAST(tds AS DOUBLE) IS NOT NULL
     AND TRY_CAST(int AS DOUBLE) IS NOT NULL THEN 'DEF_log'
    WHEN TRY_CAST(fr AS DOUBLE) IS NOT NULL
     AND TRY_CAST(total AS DOUBLE) IS NULL
     AND TRY_CAST(comp AS DOUBLE) IS NULL THEN 'DEF_log'
    WHEN (TRY_CAST(ko AS DOUBLE) IS NOT NULL
       OR TRY_CAST(tb AS DOUBLE) IS NOT NULL
       OR TRY_CAST(ret AS DOUBLE) IS NOT NULL)
     AND TRY_CAST(fgm AS DOUBLE) IS NULL
     AND TRY_CAST(punts AS DOUBLE) IS NULL THEN 'K_log'
END"""

RAW_VALUE_COLUMNS = (
    "total", "solo", "ast", "sck", "sfty", "pdef", "int", "yds", "avg", "lng",
    "tds", "ff", "fr", "att", "td", "rec", "yds_2", "avg_2", "lng_2", "td_2",
    "fum", "lost", "fumbles", "fumbles_lost", "blk", "fg_att", "fgm", "pct",
    "xp_att", "xpm", "pct_2", "blk_2", "ko", "tb", "ret", "comp", "scky", "rate",
    "att_2", "g", "gs", "punts", "net_yds", "net_avg", "oob", "dn", "in_20", "fc",
    "rety",
)


def raw_numeric_expression(columns: set[str] | None = None) -> str:
    """Return the numeric-occupancy predicate for the available source schema.

    The raw capture carries both short and long fumble aliases, while the retained
    targeted cache carries only the physical short fields.  Callers measuring a
    particular parquet surface may therefore pass its declared columns; omitting
    the argument preserves the raw-capture predicate.
    """
    names = RAW_VALUE_COLUMNS if columns is None else tuple(
        column for column in RAW_VALUE_COLUMNS if column in columns
    )
    return " OR ".join(
        f'TRY_CAST("{column}" AS DOUBLE) IS NOT NULL'
        for column in names
    )


SHARED_SCHEMA_TIE = (
    "TRY_CAST(att AS DOUBLE) > 0 AND TRY_CAST(rec AS DOUBLE) > 0 "
    "AND TRY_CAST(avg AS DOUBLE) IS NOT NULL "
    "AND ABS(TRY_CAST(avg AS DOUBLE) - TRY_CAST(yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(avg AS DOUBLE) - TRY_CAST(yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06"
)
SHARED_SCHEMA_TIE_A = (
    "TRY_CAST(a.att AS DOUBLE) > 0 AND TRY_CAST(a.rec AS DOUBLE) > 0 "
    "AND TRY_CAST(a.avg AS DOUBLE) IS NOT NULL "
    "AND ABS(TRY_CAST(a.avg AS DOUBLE) - TRY_CAST(a.yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(a.att AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(a.avg AS DOUBLE) - TRY_CAST(a.yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(a.rec AS DOUBLE), 0)) <= 0.06"
)
SHARED_SCHEMA_TIE_R = (
    "TRY_CAST(r.att AS DOUBLE) > 0 AND TRY_CAST(r.rec AS DOUBLE) > 0 "
    "AND TRY_CAST(r.avg AS DOUBLE) IS NOT NULL "
    "AND ABS(TRY_CAST(r.avg AS DOUBLE) - TRY_CAST(r.yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(r.att AS DOUBLE), 0)) <= 0.06 "
    "AND ABS(TRY_CAST(r.avg AS DOUBLE) - TRY_CAST(r.yds AS DOUBLE) "
    "/ NULLIF(TRY_CAST(r.rec AS DOUBLE), 0)) <= 0.06"
)

def mirror_layout_recovery_query(source_sql: str, *, deduplicate: bool = True) -> str:
    """Return a source-only layout surface using exact RBFB5/WRTE mirror rows."""
    raw_select = "SELECT DISTINCT" if deduplicate else "SELECT"
    mirror = """TRY_CAST(a.att AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.att AS DOUBLE)
        AND TRY_CAST(a.rec AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.rec AS DOUBLE)
        AND TRY_CAST(a.yds AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.yds_2 AS DOUBLE)
        AND TRY_CAST(a.avg AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.avg_2 AS DOUBLE)
        AND TRY_CAST(a.lng AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.lng_2 AS DOUBLE)
        AND TRY_CAST(a.td AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.td_2 AS DOUBLE)
        AND TRY_CAST(a.yds_2 AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.yds AS DOUBLE)
        AND TRY_CAST(a.avg_2 AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.avg AS DOUBLE)
        AND TRY_CAST(a.lng_2 AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.lng AS DOUBLE)
        AND TRY_CAST(a.td_2 AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.td AS DOUBLE)"""
    return f"""WITH raw AS (
        {raw_select} *, ({PLAYER_LOG_BLOCK_RECOVERY}) AS direct_layout
        FROM ({source_sql})
    ), mirrors AS (
        SELECT a._table, a.nflcom_slug, a.season, a.game_date, a.opp,
               COUNT(DISTINCT k.direct_layout) AS mirror_layout_n,
               MIN(k.direct_layout) AS mirror_layout
        FROM raw a
        JOIN raw k
          ON k._table = a._table
         AND k.nflcom_slug = a.nflcom_slug
         AND k.season = a.season
         AND k.game_date = a.game_date
         AND k.opp = a.opp
         AND k.direct_layout IN ('RBFB5', 'WRTE')
        WHERE a.direct_layout IS NULL
          AND ({SHARED_SCHEMA_TIE_A})
          AND ({mirror})
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT r.* EXCLUDE (direct_layout),
           CASE
             WHEN r.direct_layout IS NOT NULL THEN r.direct_layout
             WHEN m.mirror_layout_n = 1 AND ({SHARED_SCHEMA_TIE_R}) THEN
               CASE m.mirror_layout WHEN 'RBFB5' THEN 'WRTE' ELSE 'RBFB5' END
           END AS recovered_layout,
           CASE
             WHEN r.direct_layout IS NOT NULL THEN 'SIGNATURE'
             WHEN m.mirror_layout_n = 1 AND ({SHARED_SCHEMA_TIE_R}) THEN 'MIRRORED_SOURCE_ROW'
           END AS layout_recovery_source
    FROM raw r
    LEFT JOIN mirrors m
      ON m._table = r._table
     AND m.nflcom_slug = r.nflcom_slug
     AND m.season = r.season
     AND m.game_date = r.game_date
     AND m.opp = r.opp"""


def classify_capture(
    total_rows: int,
    parsed_dates: int,
    has_layout: bool,
    recovered_layout_rows: int = 0,
    unresolved_numeric_rows: int | None = None,
) -> dict:
    blockers = []
    if parsed_dates == 0:
        blockers.append("NO_GAME_DATE")
    if unresolved_numeric_rows is None:
        unresolved_numeric_rows = total_rows - recovered_layout_rows
    if not has_layout and unresolved_numeric_rows > 0:
        blockers.append(
            "PARTIAL_POSITION_LAYOUT"
            if recovered_layout_rows
            else "NO_POSITION_LAYOUT"
        )
    return {
        "status": "WITNESSABLE_KEY_SURFACE" if not blockers else "PROOF_PENDING_CAPTURE",
        "blockers": blockers,
        "weekly_key": "(nflcom_slug, parsed_game_date, recovered_layout)",
        "recovered_layout_rows": int(recovered_layout_rows),
        "unresolved_numeric_rows": int(unresolved_numeric_rows),
    }


def classify_week_translation(unique_buckets: int, ambiguous_buckets: int) -> dict:
    return {
        "status": (
            "WITNESSABLE_WEEK_TRANSLATION"
            if ambiguous_buckets == 0
            else "PROOF_PENDING_WEEK_TRANSLATION"
        ),
        "unique_buckets": int(unique_buckets),
        "ambiguous_buckets": int(ambiguous_buckets),
    }


def measure_week_translation(con: duckdb.DuckDBPyConnection) -> list[dict]:
    target = Path(registry(include_subject=False)["nflcom_player_logs_targeted"].path)
    slug = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
    bio = Path(registry(include_subject=False)["player_bio"].path)
    # v26 is accessed through the same latest-v26 accessor used by the witness runner;
    # keep this import local so importing the pure classification helpers stays cheap.
    from .sources import latest_v26
    v26 = Path(latest_v26())
    con.execute("CREATE OR REPLACE TEMP TABLE targeted_opp_codes(label VARCHAR, code VARCHAR)")
    codes = [(label, code) for label, values in NICKNAME_CODES.items() for code in values]
    codes.append(("Braves", "BOS"))
    con.executemany("INSERT INTO targeted_opp_codes VALUES (?, ?)", codes)
    q = f"""WITH src AS (
          SELECT DISTINCT nflcom_slug, season, wk, game_date, opp, _table
          FROM read_parquet('{target.as_posix()}')
        ), ids AS (
          SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
          FROM read_parquet('{slug.as_posix()}') x
          JOIN read_parquet('{bio.as_posix()}') b ON b.pfr_id=x.pfr_id
        ), joined AS (
          SELECT s._table, s.season, s.wk, s.opp, v.week AS target_week
          FROM src s
          JOIN ids i USING (nflcom_slug)
          JOIN read_parquet('{v26.as_posix()}') v
            ON v.NFL_player_id=i.NFL_player_id
           AND v.year=TRY_CAST(s.season AS INT)
           AND CAST(v.game_date AS DATE)=TRY_STRPTIME(s.game_date, '%m/%d/%Y')
           AND v.season_type=CASE WHEN s._table='Post Season' THEN 'POST' ELSE 'REG' END
           AND v.opponent_nfl_team IN (
             SELECT code FROM targeted_opp_codes
             WHERE label=regexp_replace(s.opp, '^@', '')
           )
        ), buckets AS (
          SELECT _table, season, wk,
                 COUNT(DISTINCT target_week) AS target_week_n
          FROM joined GROUP BY 1, 2, 3
        )
        SELECT _table,
               COUNT(*) AS source_week_buckets,
               COUNT(*) FILTER (WHERE target_week_n=1) AS unique_buckets,
               COUNT(*) FILTER (WHERE target_week_n>1) AS ambiguous_buckets
        FROM buckets GROUP BY 1 ORDER BY 1"""
    out = []
    for table, total, unique, ambiguous in con.execute(q).fetchall():
        row = {
            "table": table,
            "source_week_buckets": int(total),
            **classify_week_translation(int(unique), int(ambiguous)),
        }
        out.append(row)
    return out


def measure_layout_ambiguity(con: duckdb.DuckDBPyConnection) -> list[dict]:
    numeric_expression = raw_numeric_expression()
    rows = con.execute(
        f"""SELECT _table,
            COUNT(*) FILTER (
                WHERE recovered_layout IS NULL AND ({numeric_expression})
            ) AS unresolved_numeric_rows,
            COUNT(*) FILTER (
                WHERE recovered_layout IS NULL
                  AND ({numeric_expression}) AND ({SHARED_SCHEMA_TIE})
            ) AS shared_schema_tie_rows,
            COUNT(*) FILTER (
                WHERE layout_recovery_source = 'MIRRORED_SOURCE_ROW'
            ) AS mirror_recovered_rows
        FROM layout_surface
        GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    return [
        {
            "table": table,
            "unresolved_numeric_rows": int(unresolved_numeric),
            "shared_schema_tie_rows": int(ties),
            "other_numeric_unresolved_rows": int(unresolved_numeric - ties),
            "mirror_recovered_rows": int(mirror_recovered),
            "status": (
                "PROOF_PENDING_SHARED_SCHEMA_TIE"
                if ties
                else "PROOF_PENDING_NO_INDEPENDENT_LAYOUT_KEY"
            ),
        }
        for table, unresolved_numeric, ties, mirror_recovered in rows
    ]


def measure_unresolved_numeric_signature(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Separate shared equation ties, fumble residue, and any missed signature."""
    numeric = f"({raw_numeric_expression()})"
    shared_fumble = (
        "TRY_CAST(fum AS DOUBLE) IS NOT NULL "
        "AND TRY_CAST(lost AS DOUBLE) IS NOT NULL "
        "AND TRY_CAST(fumbles AS DOUBLE) IS NOT NULL "
        "AND TRY_CAST(fg_att AS DOUBLE) IS NULL "
        "AND TRY_CAST(punts AS DOUBLE) IS NULL "
        "AND TRY_CAST(int AS DOUBLE) IS NULL "
        "AND TRY_CAST(att AS DOUBLE) IS NULL "
        "AND TRY_CAST(rec AS DOUBLE) IS NULL"
    )
    zero_only = " AND ".join(
        f'(TRY_CAST("{column}" AS DOUBLE) IS NULL OR TRY_CAST("{column}" AS DOUBLE) = 0)'
        for column in RAW_VALUE_COLUMNS
    )
    rows = con.execute(
        f"""SELECT _table,
            COUNT(*) AS unresolved_numeric_rows,
            COUNT(*) FILTER (WHERE ({SHARED_SCHEMA_TIE})) AS shared_schema_tie_rows,
            COUNT(*) FILTER (WHERE {shared_fumble}) AS fumble_only_shared_rows,
            COUNT(*) FILTER (WHERE {zero_only} AND NOT ({SHARED_SCHEMA_TIE}) AND NOT ({shared_fumble})) AS zero_only_numeric_rows,
            COUNT(*) FILTER (WHERE NOT ({SHARED_SCHEMA_TIE}) AND NOT ({shared_fumble}) AND NOT ({zero_only})) AS other_numeric_rows
        FROM layout_surface
        WHERE recovered_layout IS NULL AND {numeric}
        GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    return [
        {
            "table": table,
            "unresolved_numeric_rows": int(total),
            "shared_schema_tie_rows": int(ties),
            "fumble_only_shared_rows": int(fumbles),
            "zero_only_numeric_rows": int(zeroes),
            "other_numeric_rows": int(other),
            "status": "SHARED_LAYOUT_RESIDUE" if other == 0 else "PROOF_PENDING_OTHER_NUMERIC",
        }
        for table, total, ties, fumbles, zeroes, other in rows
    ]


def measure_shard_structure(con: duckdb.DuckDBPyConnection) -> dict:
    source = Path(registry(include_subject=False)["nflcom_player_logs"].path)
    source_expr = f"{source.as_posix()}/**/*.parquet"
    row = con.execute(
        f"""WITH per_file AS (
            SELECT filename,
                   COUNT(DISTINCT _table) AS table_n,
                   COUNT(DISTINCT ({PLAYER_LOG_BLOCK_RECOVERY})) AS layout_n
            FROM read_parquet('{source_expr}', union_by_name=true, filename=true)
            GROUP BY 1
        )
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE table_n > 1),
               COUNT(*) FILTER (WHERE layout_n > 1)
        FROM per_file"""
    ).fetchone()
    file_count, mixed_table_files, mixed_layout_files = map(int, row)
    return {
        "file_count": file_count,
        "mixed_table_files": mixed_table_files,
        "mixed_recovered_layout_files": mixed_layout_files,
        "status": (
            "NO_SHARD_LAYOUT_DISCRIMINATOR"
            if mixed_table_files == file_count and mixed_layout_files == file_count
            else "PROOF_PENDING_SHARD_STRUCTURE"
        ),
    }


def measure_avg_2_ratio_calibration(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Calibrate the one-sided secondary-average forms on known layouts only."""
    rows = con.execute(
        f"""SELECT known_layout,
            COUNT(*) AS informative_n,
            COUNT(*) FILTER (WHERE
                COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06, FALSE)
                AND NOT COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06, FALSE)
            ) AS att_only_n,
            COUNT(*) FILTER (WHERE
                COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06, FALSE)
                AND NOT COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06, FALSE)
            ) AS rec_only_n,
            COUNT(*) FILTER (WHERE
                COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06, FALSE)
                AND COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06, FALSE)
            ) AS tie_n,
            COUNT(*) FILTER (WHERE NOT COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06, FALSE)
                AND NOT COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE) -
                    TRY_CAST(yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06, FALSE)
            ) AS neither_n
        FROM avg_2_known_layout
        WHERE known_layout IN ('RBFB5', 'WRTE')
          AND TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL
          AND TRY_CAST(yds_2 AS DOUBLE) IS NOT NULL
        GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    out = []
    for layout, informative, att_only, rec_only, tie, neither in rows:
        correct = att_only if layout == "WRTE" else rec_only
        opposite = rec_only if layout == "WRTE" else att_only
        out.append({
            "layout": layout,
            "informative_n": int(informative),
            "att_only_n": int(att_only),
            "rec_only_n": int(rec_only),
            "tie_n": int(tie),
            "neither_n": int(neither),
            "correct_unique_n": int(correct),
            "opposite_unique_n": int(opposite),
        })
    return out


def measure() -> dict:
    source = Path(registry(include_subject=False)["nflcom_player_logs"].path)
    source_expr = f"{source.as_posix()}/**/*.parquet"
    con = duckdb.connect()
    schema = [row[0] for row in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{source_expr}', union_by_name=true)"
    ).fetchall()]
    numeric_expression = raw_numeric_expression()
    con.execute(
        "CREATE OR REPLACE TEMP TABLE layout_surface AS "
        + mirror_layout_recovery_query(
            f"SELECT * FROM read_parquet('{source_expr}', union_by_name=true)",
            deduplicate=False,
        )
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE avg_2_known_layout AS "
        f"SELECT *, ({BLOCK_RECOVERY}) AS known_layout "
        f"FROM read_parquet('{source_expr}', union_by_name=true)"
    )
    total_rows, parsed_dates, recovered_layout_rows, unresolved_numeric_rows = con.execute(
        f"""SELECT COUNT(*),
        COUNT(*) FILTER (
            WHERE COALESCE(TRY_STRPTIME(game_date, '%m/%d/%Y'), TRY_CAST(game_date AS TIMESTAMP)) IS NOT NULL
        ),
        COUNT(*) FILTER (WHERE recovered_layout IS NOT NULL),
        COUNT(*) FILTER (WHERE recovered_layout IS NULL AND ({numeric_expression}))
        FROM layout_surface"""
    ).fetchone()
    table_rows = [
        {
            "table": table,
            "rows": int(rows),
            "parseable_game_date_rows": int(parsed),
            "recovered_layout_rows": int(classified),
            "unresolved_layout_rows": int(rows - classified),
            "unresolved_numeric_rows": int(unresolved_numeric),
            "unresolved_empty_rows": int(rows - classified - unresolved_numeric),
        }
        for table, rows, parsed, classified, unresolved_numeric in con.execute(
            f"""SELECT _table, COUNT(*),
                COUNT(*) FILTER (
                    WHERE COALESCE(TRY_STRPTIME(game_date, '%m/%d/%Y'), TRY_CAST(game_date AS TIMESTAMP)) IS NOT NULL
                ),
                COUNT(*) FILTER (WHERE recovered_layout IS NOT NULL),
                COUNT(*) FILTER (WHERE recovered_layout IS NULL AND ({numeric_expression}))
            FROM layout_surface
            GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    ]
    season_min, season_max = con.execute(
        f"""SELECT MIN(TRY_CAST(season AS INT)), MAX(TRY_CAST(season AS INT))
        FROM read_parquet('{source_expr}', union_by_name=true)"""
    ).fetchone()
    week_translation = measure_week_translation(con)
    layout_ambiguity = measure_layout_ambiguity(con)
    avg_2_ratio_calibration = measure_avg_2_ratio_calibration(con)
    shard_structure = measure_shard_structure(con)
    unresolved_numeric_signature = measure_unresolved_numeric_signature(con)
    con.close()
    result = {
        "source": "nflcom_player_logs",
        "source_path": str(source),
        "raw_rows": int(total_rows),
        "parseable_game_date_rows": int(parsed_dates),
        "recovered_layout_rows": int(recovered_layout_rows),
        "unresolved_numeric_rows": int(unresolved_numeric_rows),
        "unresolved_empty_rows": int(total_rows - recovered_layout_rows - unresolved_numeric_rows),
        "has_position_layout_column": "_layout" in schema,
        "physical_tables": table_rows,
        "season_min": int(season_min),
        "season_max": int(season_max),
        "capture": classify_capture(
            int(total_rows), int(parsed_dates), "_layout" in schema,
            int(recovered_layout_rows), int(unresolved_numeric_rows),
        ),
        "week_translation_calibration": week_translation,
        "avg_2_ratio_calibration": avg_2_ratio_calibration,
        "layout_ambiguity": layout_ambiguity,
        "unresolved_numeric_signature": unresolved_numeric_signature,
        "shard_structure": shard_structure,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    result = measure()
    print(f"measurement -> {OUT}")
    print(result["capture"]["status"], result["capture"]["blockers"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
