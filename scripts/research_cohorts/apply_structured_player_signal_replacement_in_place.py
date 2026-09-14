"""Apply validated direct-source replacements to exact canonical player rows.

Unlike the null-fill updater, this module may replace a populated canonical
signal only when the delta carries the value observed while building the
candidate (``expected_*``).  That makes the operation fail closed if the cache
changed after candidate generation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY_COLUMNS = ("db_name", "year", "week", "NFL_player_id", "manager")
FIELD_MAP = {
    "source_win": "win",
    "source_loss": "loss",
    "source_tie": "tie",
    "source_team_points": "team_points",
    "source_is_playoffs": "is_playoffs",
}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _schema(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in con.execute("DESCRIBE public.player_fantasy").fetchall()]


def _identity_join(left: str, right: str) -> str:
    return " AND ".join(
        f"{left}.{_q(column)} IS NOT DISTINCT FROM {right}.{_q(column)}"
        for column in KEY_COLUMNS
    )


def apply(*, base: Path, delta: Path, report: Path) -> dict[str, object]:
    con = duckdb.connect(str(base))
    try:
        before_schema = _schema(con)
        missing_player = sorted((set(KEY_COLUMNS) | set(FIELD_MAP.values())) - set(before_schema))
        if missing_player:
            raise ValueError(f"canonical player schema missing: {missing_player}")

        delta_path = str(delta.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW delta_raw AS SELECT * FROM read_parquet('{delta_path}')")
        delta_columns = {row[0] for row in con.execute("DESCRIBE delta_raw").fetchall()}
        required_delta = {"player_rowid", *KEY_COLUMNS, *FIELD_MAP, *("expected_" + target for target in FIELD_MAP.values())}
        missing_delta = sorted(required_delta - delta_columns)
        if missing_delta:
            raise ValueError(f"structured replacement delta schema missing: {missing_delta}")

        for column, label in (("player_rowid", "rowid"), (", ".join(KEY_COLUMNS), "player identity")):
            duplicate_count = int(con.execute(f"SELECT COUNT(*) FROM (SELECT {column} FROM delta_raw GROUP BY {column} HAVING COUNT(*) > 1)").fetchone()[0])
            if duplicate_count:
                raise ValueError(f"structured replacement delta has duplicate {label} values: {duplicate_count}")

        before_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
        canonical_fields = ", ".join(
            f"p.{_q(target)} AS {_q('canonical_' + target)}" for target in FIELD_MAP.values()
        )
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE matched AS
            SELECT p.rowid AS canonical_rowid, {canonical_fields}, d.*
            FROM delta_raw d
            JOIN public.player_fantasy p
              ON p.rowid = d.player_rowid
             AND {_identity_join('p', 'd')}
        """)
        delta_rows = int(con.execute("SELECT COUNT(*) FROM delta_raw").fetchone()[0])
        matched_rows = int(con.execute("SELECT COUNT(*) FROM matched").fetchone()[0])
        if matched_rows != delta_rows:
            raise ValueError(f"structured replacement delta is stale or not exact: delta_rows={delta_rows}, matched_rows={matched_rows}")

        replacement_cells: dict[str, int] = {}
        for source, target in FIELD_MAP.items():
            expected = "expected_" + target
            invalid_expected = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NOT NULL AND {_q(expected)} IS NULL"
            ).fetchone()[0])
            invalid_unused_expected = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NULL AND {_q(expected)} IS NOT NULL"
            ).fetchone()[0])
            if invalid_expected or invalid_unused_expected:
                raise ValueError(f"replacement delta has invalid expected state for {target}: source_without_expected={invalid_expected}, expected_without_source={invalid_unused_expected}")
            stale = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NOT NULL AND {_q('canonical_' + target)} IS DISTINCT FROM {_q(expected)}"
            ).fetchone()[0])
            if stale:
                raise ValueError(f"structured replacement delta is stale for {target}: {stale} cells")
            non_replacements = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NOT NULL AND {_q(source)} IS NOT DISTINCT FROM {_q(expected)}"
            ).fetchone()[0])
            if non_replacements:
                raise ValueError(f"structured replacement delta includes non-conflicting {target} cells: {non_replacements}")
            replacement_cells[target] = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NOT NULL"
            ).fetchone()[0])

        assignments = ", ".join(
            f"{_q(target)} = CASE WHEN m.{_q(source)} IS NOT NULL THEN m.{_q(source)} ELSE p.{_q(target)} END"
            for source, target in FIELD_MAP.items()
        )
        con.execute("BEGIN")
        try:
            con.execute(f"UPDATE public.player_fantasy p SET {assignments} FROM matched m WHERE p.rowid = m.canonical_rowid")
            after_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
            if after_rows != before_rows:
                raise ValueError(f"player row count changed: before={before_rows}, after={after_rows}")
            if _schema(con) != before_schema:
                raise ValueError("player schema changed")
            remaining = {
                target: int(con.execute(
                    f"SELECT COUNT(*) FROM public.player_fantasy p JOIN matched m ON p.rowid=m.canonical_rowid "
                    f"WHERE m.{_q(source)} IS NOT NULL AND p.{_q(target)} IS DISTINCT FROM m.{_q(source)}"
                ).fetchone()[0])
                for source, target in FIELD_MAP.items()
            }
            if any(remaining.values()):
                raise ValueError(f"structured replacement readback mismatch: {remaining}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        result: dict[str, object] = {
            "in_place": True,
            "cache_mutated": True,
            "new_lineage": False,
            "schema_unchanged": True,
            "row_count_unchanged": True,
            "delta_rows": delta_rows,
            "matched_rows": matched_rows,
            "replacement_cells": replacement_cells,
            "readback_remaining": remaining,
            "join_contract": ["player_rowid", *KEY_COLUMNS],
            "source_precedence": "validated_direct_mfl_source",
        }
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--delta", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(apply(base=args.base, delta=args.delta, report=args.report), sort_keys=True))


if __name__ == "__main__":
    main()
