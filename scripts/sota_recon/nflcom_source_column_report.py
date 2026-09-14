"""Local NFL.com-to-supertable column reconciliation.

This is intentionally a report, not a mapper or a writer.  It reads the existing local
NFL.com captures, the local slug/PFR bridge, player bio, and the local v26 supertable.  A
row is emitted for every NFL.com source/table/column decision in the dossier.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from .column_dossier import row_key
from .sources import PLAYER_BIO, registry, latest_v26

ROOT = Path(__file__).resolve().parents[2]
DOSSIER = ROOT / "docs" / "column-dossier.json"
OUT = ROOT / "docs" / "nflcom-source-column-reconciliation.json"
SOURCES = (
    "nflcom_player_logs", "nflcom_player_career", "nflcom_player_season",
    "nflcom_player_splits", "nflcom_player_situational", "nflcom_team_stats",
    "nflcom_player_logs_targeted",
)
SOURCE_REFS: dict[str, str] = {}


def q(path: str | Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def numeric(expr: str) -> str:
    return f"TRY_CAST(NULLIF(LOWER(TRIM(CAST({expr} AS VARCHAR))), 'nan') AS DOUBLE)"


def season_type_from_key(table_key: str) -> str | None:
    tail = table_key.rsplit("|", 1)[0].lower()
    if tail in {"regular season", "reg"} or table_key.lower().endswith("|reg"):
        return "REG"
    if tail in {"post season", "post"} or table_key.lower().endswith("|post"):
        return "POST"
    if tail in {"preseason", "pre"} or table_key.lower().endswith("|pre"):
        return "PRE"
    return None


def layout_expr(source: str) -> str:
    """Classify the seven NFL.com game-log layouts using published fields and bio position."""
    if source == "nflcom_player_logs_targeted":
        return "CAST(r._layout AS VARCHAR)"
    return """CASE
        WHEN r.comp IS NOT NULL THEN 'id_gamelog:QB'
        WHEN r.total IS NOT NULL THEN 'id_gamelog:DEF_log'
        WHEN r.punts IS NOT NULL THEN 'id_gamelog:P'
        WHEN r.fg_att IS NOT NULL THEN 'id_gamelog:K_log'
        WHEN r.g IS NOT NULL THEN 'id_gamelog:OL'
        WHEN r.rec IS NOT NULL AND r.att IS NOT NULL
          AND (UPPER(COALESCE(b.nfl_position, '')) LIKE '%RB%'
               OR UPPER(COALESCE(b.nfl_position, '')) LIKE '%FB%')
          THEN 'id_gamelog:RBFB5'
        WHEN r.rec IS NOT NULL AND r.att IS NOT NULL THEN 'id_gamelog:WRTE'
        ELSE NULL END"""


def source_sql(source: str, table_key: str, column: str, canonical: str, agg: str) -> str | None:
    if source == "nflcom_team_stats":
        return None
    path = SOURCE_REFS.get(source) or f"read_parquet('{q(registry(include_subject=False)[source].path)}', union_by_name=true)"
    cross = SOURCE_REFS.get("nflcom_slug_pfrid") or f"read_parquet('{q(registry(include_subject=False)['nflcom_slug_pfrid'].path)}')"
    bio = SOURCE_REFS.get("player_bio") or f"read_parquet('{q(PLAYER_BIO.path)}')"
    st = season_type_from_key(table_key)
    if source in {"nflcom_player_logs", "nflcom_player_logs_targeted"}:
        if not st:
            return None
        layout = table_key.rsplit("|", 1)[1]
        value = numeric(f"r.{column}")
        aggregate = "MAX" if agg == "MAX" else "SUM"
        return f"""
        SELECT x.pfr_id, TRY_CAST(r.season AS INTEGER) AS year,
               '{st}' AS season_type, {aggregate}(x.value) AS value
        FROM {path} r
        JOIN {cross} cw ON cw.nflcom_slug = r.nflcom_slug
        JOIN {bio} b ON b.pfr_id = cw.pfr_id
        CROSS JOIN LATERAL (SELECT {value} AS value) x
        WHERE r._table = '{table_key.split('|', 1)[0].replace("'", "''")}'
          AND {layout_expr(source)} = '{layout}'
          AND x.value IS NOT NULL
        GROUP BY 1, 2, 3"""
    if source == "nflcom_player_career":
        if table_key.startswith("Recent Games"):
            return None
        caption, layout = table_key.split("|", 1)
        value = numeric(f"r.{column}")
        aggregate = "MAX" if agg == "MAX" else "SUM"
        return f"""
        SELECT cw.pfr_id, TRY_CAST(r.season AS INTEGER) AS year, 'REG' AS season_type,
               {aggregate}({value}) AS value
        FROM {path} r
        JOIN {cross} cw ON cw.nflcom_slug = r.nflcom_slug
        WHERE r._table = '{caption.replace("'", "''")}'
          AND {value} IS NOT NULL
        GROUP BY 1, 2, 3"""
    if source in {"nflcom_player_season", "nflcom_player_splits", "nflcom_player_situational"}:
        parts = table_key.split("|")
        category = parts[0].replace("'", "''")
        layout = parts[1].replace("'", "''") if len(parts) > 1 else None
        value = numeric(f"r.{column}")
        aggregate = "MAX" if agg == "MAX" else "SUM"
        discriminator = "_category" if source == "nflcom_player_season" else "_table"
        filters = [f"r.{discriminator} = '{category}'"]
        if layout:
            if source == "nflcom_player_season":
                filters.append(f"LOWER(r.season_type) = '{layout.lower()}'")
            else:
                filters.append(f"r._layout = '{layout}'")
        if source == "nflcom_player_season":
            slug = "r._player_slug"
        else:
            slug = "r.nflcom_slug"
        return f"""
        SELECT cw.pfr_id, TRY_CAST(r.season AS INTEGER) AS year,
               '{st or 'REG'}' AS season_type, {aggregate}({value}) AS value
        FROM {path} r
        JOIN {cross} cw ON cw.nflcom_slug = {slug}
        WHERE {' AND '.join(filters)} AND {value} IS NOT NULL
        GROUP BY 1, 2, 3"""
    return None


def build(out: Path = OUT, source_keys: tuple[str, ...] = SOURCES) -> dict:
    dossier = json.loads(DOSSIER.read_text(encoding="utf-8"))
    rows = [r for r in dossier["rows"] if r["source"] in source_keys]
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    SOURCE_REFS.clear()
    needed_by_source: dict[str, set[str]] = {source: set() for source in source_keys}
    for row in rows:
        needed_by_source[row["source"]].add(str(row["column"]))
    for source in source_keys:
        ref = f"src_{source}"
        path = q(registry(include_subject=False)[source].path)
        available = {
            str(x[0]) for x in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}', union_by_name=true)"
            ).fetchall()
        }
        carry = sorted((needed_by_source[source] & available) | {
            "nflcom_slug", "_player_slug", "season", "season_type", "_table", "_layout",
            "split_value", "_category", "_side",
        } & available)
        select_cols = ", ".join('"' + c.replace('"', '""') + '"' for c in carry)
        con.execute(
            f"CREATE TEMP TABLE {ref} AS SELECT {select_cols} FROM read_parquet('{path}', union_by_name=true)"
        )
        SOURCE_REFS[source] = ref
    con.execute(
        f"CREATE TEMP TABLE src_nflcom_slug_pfrid AS SELECT * FROM read_parquet('{q(registry(include_subject=False)['nflcom_slug_pfrid'].path)}')"
    )
    con.execute(
        f"CREATE TEMP TABLE src_player_bio AS SELECT * FROM read_parquet('{q(PLAYER_BIO.path)}')"
    )
    SOURCE_REFS["nflcom_slug_pfrid"] = "src_nflcom_slug_pfrid"
    SOURCE_REFS["player_bio"] = "src_player_bio"
    v26_path = q(latest_v26())
    v26_available = {
        str(x[0]) for x in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{v26_path}')"
        ).fetchall()
    }
    canonical_cols = sorted({str(r["canonical"]) for r in rows if r.get("canonical")})
    carry_v26 = [c for c in canonical_cols if c in v26_available]
    v26_select = ", ".join('"' + c.replace('"', '""') + '"' for c in
                             ["NFL_player_id", "year", "season_type"] + carry_v26)
    con.execute(
        f"CREATE TEMP TABLE src_v26 AS SELECT {v26_select} FROM read_parquet('{v26_path}')"
    )
    report: list[dict] = []
    for r in rows:
        if r.get("disposition") != "MAPPED_TO_CANONICAL":
            report.append({
                "row_key": row_key(r["source"], r["table_key"], r["column"]),
                "source": r["source"], "table_key": r["table_key"],
                "source_column": r["column"], "supertable_column": r.get("canonical"),
                "status": r.get("disposition"), "reason": r.get("reason"),
                "overlap_rows": None, "conflict_rows": None,
                "source_only_rows": None, "supertable_only_rows": None,
            })
            continue
        canonical = str(r["canonical"])
        table_key = str(r["table_key"])
        agg = str(r.get("canonical_aggregation") or "SUM")
        sql = source_sql(r["source"], table_key, r["column"], canonical, agg)
        base = {
            "row_key": row_key(r["source"], table_key, r["column"]),
            "source": r["source"], "table_key": table_key,
            "source_column": r["column"], "supertable_column": canonical,
        }
        if sql is None:
            report.append(base | {"status": "NOT_COMPARABLE", "reason":
                "team-grain source or source surface has no safe season/player join",
                "overlap_rows": None, "conflict_rows": None,
                "source_only_rows": None, "supertable_only_rows": None})
            continue
        try:
            # The v26 side is aggregated to the same player/season/season-type grain.
            vagg = "MAX" if agg == "MAX" else "SUM"
            vsql = f"""
            SELECT b.pfr_id, v.year::INTEGER AS year, v.season_type,
                   {vagg}(TRY_CAST(v.{canonical} AS DOUBLE)) AS value
            FROM src_v26 v
            JOIN src_player_bio b USING (NFL_player_id)
            WHERE v.{canonical} IS NOT NULL
            GROUP BY 1,2,3"""
            result = con.execute(f"""
            WITH s AS ({sql}), v AS ({vsql}),
            j AS (
              SELECT s.pfr_id, s.year, s.season_type, s.value source_value,
                     v.value supertable_value
              FROM s LEFT JOIN v USING (pfr_id, year, season_type)
            )
            SELECT
              COUNT(*) FILTER (WHERE supertable_value IS NOT NULL) AS overlap_rows,
              COUNT(*) FILTER (WHERE supertable_value IS NOT NULL
                AND ABS(source_value - supertable_value) > 0.01) AS conflict_rows,
              COUNT(*) FILTER (WHERE supertable_value IS NULL) AS source_only_rows,
              (SELECT COUNT(*) FROM v WHERE NOT EXISTS
                (SELECT 1 FROM s WHERE s.pfr_id=v.pfr_id AND s.year=v.year
                 AND s.season_type=v.season_type)) AS supertable_only_rows
            FROM j""").fetchone()
            report.append(base | {"status": "COMPARED", "reason": None,
                "overlap_rows": int(result[0] or 0), "conflict_rows": int(result[1] or 0),
                "source_only_rows": int(result[2] or 0),
                "supertable_only_rows": int(result[3] or 0)})
        except Exception as exc:
            report.append(base | {"status": "COMPARE_ERROR", "reason": str(exc).splitlines()[0],
                "overlap_rows": None, "conflict_rows": None,
                "source_only_rows": None, "supertable_only_rows": None})
    document = {
        "report": "local NFL.com source columns versus local v26 supertable",
        "read_only": True, "sources": list(source_keys),
        "counts": {"rows": len(report),
                   "compared": sum(x["status"] == "COMPARED" for x in report),
                   "not_comparable": sum(x["status"] == "NOT_COMPARABLE" for x in report),
                   "unresolved": sum(x["status"] not in {"COMPARED", "NOT_COMPARABLE"} for x in report)},
        "rows": report,
    }
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--source", choices=SOURCES)
    args = parser.parse_args()
    selected = (args.source,) if args.source else SOURCES
    document = build(args.out, selected)
    print(json.dumps(document["counts"], indent=2))
    print(f"report -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
