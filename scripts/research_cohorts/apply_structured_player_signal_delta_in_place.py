"""Apply one exact-player structured null-fill delta to the canonical lake.

The delta is produced against the current canonical cache and carries both its
candidate ``player_rowid`` and full logical player identity.  This updater
requires both to match, fills null cells only, and refuses any conflict,
schema change, or row-count change.
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
        required_player = set(KEY_COLUMNS) | set(FIELD_MAP.values())
        missing_player = sorted(required_player - set(before_schema))
        if missing_player:
            raise ValueError(f"canonical player schema missing: {missing_player}")

        delta_path = str(delta.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW delta_raw AS SELECT * FROM read_parquet('{delta_path}')")
        delta_columns = {row[0] for row in con.execute("DESCRIBE delta_raw").fetchall()}
        required_delta = {"player_rowid", *KEY_COLUMNS, *FIELD_MAP}
        missing_delta = sorted(required_delta - delta_columns)
        if missing_delta:
            raise ValueError(f"structured delta schema missing: {missing_delta}")

        duplicate_rowids = int(con.execute("""
            SELECT COUNT(*) FROM (
              SELECT player_rowid FROM delta_raw GROUP BY player_rowid HAVING COUNT(*) > 1
            )
        """).fetchone()[0])
        if duplicate_rowids:
            raise ValueError(f"structured delta has duplicate player_rowid values: {duplicate_rowids}")

        duplicate_identities = int(con.execute("""
            SELECT COUNT(*) FROM (
              SELECT db_name, year, week, NFL_player_id, manager
              FROM delta_raw GROUP BY ALL HAVING COUNT(*) > 1
            )
        """).fetchone()[0])
        if duplicate_identities:
            raise ValueError(f"structured delta has duplicate player identities: {duplicate_identities}")

        before_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE matched AS
            SELECT p.rowid AS canonical_rowid,
                   {', '.join(f'p.{_q(target)} AS {_q("canonical_" + target)}' for target in FIELD_MAP.values())},
                   d.*
            FROM delta_raw d
            JOIN public.player_fantasy p
              ON p.rowid = d.player_rowid
             AND {_identity_join('p', 'd')}
        """)
        delta_rows = int(con.execute("SELECT COUNT(*) FROM delta_raw").fetchone()[0])
        matched_rows = int(con.execute("SELECT COUNT(*) FROM matched").fetchone()[0])
        if matched_rows != delta_rows:
            raise ValueError(
                f"structured delta is stale or not exact: delta_rows={delta_rows}, matched_rows={matched_rows}"
            )

        conflicts: dict[str, int] = {}
        improvements: dict[str, int] = {}
        for source, target in FIELD_MAP.items():
            conflicts[target] = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NOT NULL "
                f"AND {_q('canonical_' + target)} IS NOT NULL "
                f"AND {_q(source)} IS DISTINCT FROM {_q('canonical_' + target)}"
            ).fetchone()[0])
            improvements[target] = int(con.execute(
                f"SELECT COUNT(*) FROM matched WHERE {_q(source)} IS NOT NULL "
                f"AND {_q('canonical_' + target)} IS NULL"
            ).fetchone()[0])
        conflict_total = sum(conflicts.values())
        if conflict_total:
            raise ValueError(f"structured delta conflicts with canonical cells: {conflicts}")

        assignments = ", ".join(
            f"{_q(target)} = CASE WHEN p.{_q(target)} IS NULL THEN m.{_q(source)} ELSE p.{_q(target)} END"
            for source, target in FIELD_MAP.items()
        )
        con.execute("BEGIN")
        try:
            con.execute(f"""
                UPDATE public.player_fantasy p
                SET {assignments}
                FROM matched m
                WHERE p.rowid = m.canonical_rowid
            """)
            after_rows = int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0])
            after_schema = _schema(con)
            if after_rows != before_rows:
                raise ValueError(f"player row count changed: before={before_rows}, after={after_rows}")
            if after_schema != before_schema:
                raise ValueError("player schema changed")
            remaining = {
                target: int(con.execute(
                    f"SELECT COUNT(*) FROM public.player_fantasy p JOIN matched m "
                    f"ON p.rowid=m.canonical_rowid WHERE m.{_q(source)} IS NOT NULL "
                    f"AND p.{_q(target)} IS DISTINCT FROM m.{_q(source)}"
                ).fetchone()[0])
                for source, target in FIELD_MAP.items()
            }
            if any(remaining.values()):
                raise ValueError(f"structured delta readback mismatch: {remaining}")
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
            "improvements_by_field": improvements,
            "readback_remaining": remaining,
            "join_contract": ["player_rowid", *KEY_COLUMNS],
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
