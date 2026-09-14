"""Measure source-side layout labels recoverable from the targeted NFL.com capture.

The generic player-log shards lose the position block label.  The targeted cache was
reparsed from retained NFL.com HTML and keeps the uniquely resolved header layout.  This
runner joins the two source captures on their source key *and all shared source values*;
it never consults the v26 subject table and never writes a recovered label into the raw
capture.  A label is usable only when the exact source row has one distinct targeted
layout and no conflicting direct raw layout.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .nflcom_player_logs_capture_inventory import PLAYER_LOG_BLOCK_RECOVERY


RAW = Path(r"D:/league-history-data/nfl/raw/nflcom/tables/player_logs")
TARGETED = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_targeted_normalized.parquet"
)
OUT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_source_label_witness.json"
)

SOURCE_VALUE_COLUMNS = (
    "wk", "game_date", "opp", "result", "total", "solo", "ast", "sck", "sfty",
    "pdef", "int", "yds", "avg", "lng", "tds", "ff", "fr", "att", "td", "rec",
    "yds_2", "avg_2", "lng_2", "td_2", "fum", "lost", "blk", "fg_att", "fgm",
    "pct", "xp_att", "xpm", "pct_2", "blk_2", "ko", "tb", "ret", "comp", "scky",
    "rate", "att_2", "g", "gs", "punts", "net_yds", "net_avg", "oob", "dn", "in_20",
    "fc", "rety",
)

KEY_COLUMNS = ("nflcom_slug", "season", "_table", "game_date", "opp")


def _q(path: Path) -> str:
    return str(path).replace("'", "''")


def _normalized(alias: str, column: str) -> str:
    return f"NULLIF(TRIM(CAST({alias}.{column} AS VARCHAR)), '')"


def _exact_source_match_query() -> str:
    comparisons = " AND ".join(
        f"{_normalized('r', column)} IS NOT DISTINCT FROM "
        f"{_normalized('t', column)}"
        for column in SOURCE_VALUE_COLUMNS
    )
    keys = ", ".join(KEY_COLUMNS)
    return f"""
WITH raw AS (
    SELECT row_number() OVER () AS raw_row_id, *,
           ({PLAYER_LOG_BLOCK_RECOVERY}) AS direct_layout
    FROM parquet_scan('{_q(RAW)}/*.parquet', union_by_name=true)
    WHERE _view = 'logs'
), targeted AS (
    SELECT row_number() OVER () AS targeted_row_id,
           replace(_layout, 'id_gamelog:', '') AS source_layout,
           *
    FROM parquet_scan('{_q(TARGETED)}')
    WHERE _view = 'logs'
), exact AS (
    SELECT r.raw_row_id, r.direct_layout, t.source_layout,
           t.nflcom_slug, t.season, t._table, t.game_date, t.opp
    FROM raw r
    JOIN targeted t USING ({keys})
    WHERE {comparisons}
), labels AS (
    SELECT raw_row_id,
           COUNT(DISTINCT source_layout) AS source_layout_n,
           MIN(source_layout) AS source_layout,
           COUNT(*) AS exact_target_matches,
           COUNT(*) FILTER (
               WHERE direct_layout IS NOT NULL AND direct_layout <> source_layout
           ) AS conflicting_direct_matches,
           MIN(direct_layout) AS direct_layout
    FROM exact
    GROUP BY raw_row_id
)
SELECT * FROM labels
"""


def build() -> dict:
    con = duckdb.connect()
    try:
        raw_rows = con.execute(
            f"SELECT COUNT(*) FROM parquet_scan('{_q(RAW)}/*.parquet', union_by_name=true) "
            "WHERE _view='logs'"
        ).fetchone()[0]
        direct_counts = dict(
            con.execute(
                f"""SELECT COALESCE(({PLAYER_LOG_BLOCK_RECOVERY}), 'UNRESOLVED') AS layout,
                           COUNT(*)
                    FROM parquet_scan('{_q(RAW)}/*.parquet', union_by_name=true)
                    WHERE _view='logs'
                    GROUP BY 1 ORDER BY 1"""
            ).fetchall()
        )
        targeted_rows = con.execute(
            f"SELECT COUNT(*) FROM parquet_scan('{_q(TARGETED)}') WHERE _view='logs'"
        ).fetchone()[0]
        labels = con.execute(_exact_source_match_query()).fetchdf()
    finally:
        con.close()

    safe = labels[
        (labels["source_layout_n"] == 1)
        & (labels["conflicting_direct_matches"] == 0)
    ]
    unresolved = safe[safe["direct_layout"].isna()]
    conflicts = labels[labels["conflicting_direct_matches"] > 0]
    by_layout = []
    for layout, group in safe.groupby("source_layout", dropna=False):
        by_layout.append(
            {
                "source_layout": layout,
                "safe_raw_rows": int(len(group)),
                "unresolved_raw_rows": int(group["direct_layout"].isna().sum()),
                "already_classified_raw_rows": int(group["direct_layout"].notna().sum()),
            }
        )

    receipt = {
        "version": "1",
        "claim": (
            "targeted NFL.com source rows with independently parsed header layouts can "
            "recover matching generic raw rows when source values agree exactly"
        ),
        "source_only": True,
        "subject_table_used": False,
        "raw_rows": int(raw_rows),
        "targeted_labelled_rows": int(targeted_rows),
        "direct_layout_counts": [
            {"layout": layout, "rows": int(rows)} for layout, rows in direct_counts.items()
        ],
        "exact_source_match_rows": int(len(labels)),
        "safe_source_label_rows": int(len(safe)),
        "safe_unresolved_raw_rows": int(len(unresolved)),
        "safe_already_classified_raw_rows": int(len(safe) - len(unresolved)),
        "ambiguous_source_label_rows": int((labels["source_layout_n"] != 1).sum()),
        "conflicting_direct_layout_rows": int(len(conflicts)),
        "by_layout": by_layout,
        "status": "SOURCE_LABEL_RECOVERABLE" if len(unresolved) else "NO_NEW_SOURCE_LABELS",
        "rule": (
            "match on nflcom_slug, season, _table, game_date, opp and every shared source "
            "value; accept only one distinct targeted _layout with no conflicting direct layout"
        ),
        "no_mapping_or_license_change": True,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
