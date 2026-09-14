"""Validate a complete player sidecar without mutating the canonical lake.

The team/matchup rescue artifact cannot create player rows because it has no
player identity. This gate is for a true player sidecar only. It requires the
exact canonical 29-column schema, a unique player-week/team key, and rejects
any disagreement with an already-confirmed canonical value.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


CANONICAL_COLUMNS = (
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar", "manager",
    "team_points", "final_playoff_seed", "is_playoffs", "has_po_signal", "player",
    "position", "fantasy_position", "platform", "team_key", "team_name", "nfl_team_api",
    "yahoo_player_id", "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
)
REQUIRED_IDENTITY_COLUMNS = ("db_name", "year", "week", "platform")
# fantasy_position is part of the fixed canonical row identity because a player can occupy more than one lineup slot.
BASE_IDENTITY_COLUMNS = ("db_name", "year", "week", "platform", "manager", "team_name", "team_key", "fantasy_position")
PLAYER_ID_COLUMNS = (
    "NFL_player_id", "sleeper_player_id", "fleaflicker_player_id", "mfl_player_id",
    "espn_player_id", "yahoo_player_id",
)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _schema(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def _hash_columns(columns: list[str]) -> str:
    return hashlib.sha256(json.dumps(columns).encode()).hexdigest()


def _platform_player_id_expr(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    platform = f"LOWER(NULLIF(TRIM(CAST({prefix}{q('platform')} AS VARCHAR)), ''))"
    ids = {
        "sleeper": "sleeper_player_id",
        "fleaflicker": "fleaflicker_player_id",
        "mfl": "mfl_player_id",
    }
    # Prefer the platform-native ID, but fall back to the canonical NFL ID
    # when the source row does not carry a platform-native identifier.
    fallback = "COALESCE(" + ", ".join(
        f"NULLIF(TRIM(CAST({prefix}{q(column)} AS VARCHAR)), '')"
        for column in PLAYER_ID_COLUMNS
    ) + ")"
    cases = [
        f"WHEN '{platform_name}' THEN COALESCE(NULLIF(TRIM(CAST({prefix}{q(column)} AS VARCHAR)), ''), {fallback})"
        for platform_name, column in ids.items()
    ]
    return "CASE " + platform + " " + " ".join(cases) + f" ELSE {fallback} END"


def validate_player_sidecar(
    base_path: Path,
    sidecar_path: Path,
    out_dir: Path,
    shard_index: int,
    shard_count: int,
) -> dict[str, object]:
    if not 0 <= shard_index < shard_count:
        raise ValueError("invalid shard index")
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(base_path), read_only=True)
    try:
        base_columns = _schema(con, "public.player_fantasy")
        if tuple(base_columns) != CANONICAL_COLUMNS:
            raise ValueError("canonical player_fantasy is not the exact canonical 29-column schema")

        uri = str(sidecar_path.resolve()).replace("'", "''")
        con.execute(f"CREATE OR REPLACE TEMP VIEW source_rows AS SELECT * FROM read_parquet('{uri}')")
        source_columns = _schema(con, "source_rows")
        if tuple(source_columns) != CANONICAL_COLUMNS:
            raise ValueError("player sidecar must contain the exact canonical 29 columns in order")

        null_identity = " OR ".join(f"{q(column)} IS NULL" for column in REQUIRED_IDENTITY_COLUMNS)
        player_id_expr = _platform_player_id_expr()
        key_expr = "CONCAT_WS('|', " + ", ".join(
            [f"CAST({q(column)} AS VARCHAR)" for column in BASE_IDENTITY_COLUMNS] + [player_id_expr]
        ) + ")"
        has_player_id = f"{player_id_expr} IS NOT NULL"
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW source_keyed AS
            SELECT *, {key_expr} AS join_key
            FROM source_rows
            WHERE NOT ({null_identity})
              AND LOWER(NULLIF(TRIM(CAST({q('platform')} AS VARCHAR)), '')) IN ('sleeper', 'fleaflicker', 'mfl')
              AND ({has_player_id})
              AND MOD(ABS(HASH({key_expr})), {int(shard_count)}) = {int(shard_index)}
        """)
        source_rows = int(con.execute("SELECT COUNT(*) FROM source_keyed").fetchone()[0])
        source_keys = int(con.execute("SELECT COUNT(DISTINCT join_key) FROM source_keyed").fetchone()[0])
        duplicate_source_rows = source_rows - source_keys
        if duplicate_source_rows:
            raise ValueError(f"duplicate source player keys: {duplicate_source_rows}")

        con.execute("CREATE OR REPLACE TEMP TABLE source_key_list AS SELECT DISTINCT join_key FROM source_keyed")
        base_player_id_expr = _platform_player_id_expr("p")
        base_key_expr = "CONCAT_WS('|', " + ", ".join(
            [f"CAST(p.{q(column)} AS VARCHAR)" for column in BASE_IDENTITY_COLUMNS] + [base_player_id_expr]
        ) + ")"
        # Restrict the canonical scan to keys actually present in this shard.
        # The canonical table is 269M rows; a full duplicate scan is unnecessary.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE base_keyed AS
            SELECT p.*, {base_key_expr} AS join_key
            FROM public.player_fantasy p
            WHERE {base_key_expr} IN (SELECT join_key FROM source_key_list)
        """)
        base_rows = int(con.execute("SELECT COUNT(*) FROM base_keyed").fetchone()[0])
        duplicate_base_rows = int(con.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT join_key) FROM base_keyed"
        ).fetchone()[0])
        base_conflict_terms = [
            f"COUNT(DISTINCT CASE WHEN {q(column)} IS NOT NULL THEN CAST({q(column)} AS VARCHAR) END) > 1"
            for column in CANONICAL_COLUMNS
        ]
        base_conflict_predicate = " OR ".join(base_conflict_terms)
        canonical_conflicting_keys = int(con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT join_key
                FROM base_keyed
                GROUP BY join_key
                HAVING {base_conflict_predicate}
            )
        """).fetchone()[0])
        if canonical_conflicting_keys:
            raise ValueError(
                f"canonical duplicate keys contain conflicting values: {canonical_conflicting_keys}"
            )
        # Repeated physical rows are safe to compare as one logical key only
        # when every canonical column agrees; ANY_VALUE preserves a non-null
        # value when duplicates contain NULL plus a confirmed value.
        collapsed_select = ", ".join(
            f"ANY_VALUE({q(column)}) AS {q(column)}" for column in CANONICAL_COLUMNS
        )
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW base_collapsed AS
            SELECT join_key, {collapsed_select}
            FROM base_keyed
            GROUP BY join_key
        """)
        matched_rows = int(con.execute(
            "SELECT COUNT(*) FROM source_keyed s JOIN base_collapsed b USING (join_key)"
        ).fetchone()[0])
        new_rows = int(con.execute("""
            SELECT COUNT(*) FROM source_keyed s
            WHERE NOT EXISTS (SELECT 1 FROM base_collapsed b WHERE b.join_key=s.join_key)
        """).fetchone()[0])

        conflict_terms = []
        improvement_terms = []
        conflicts_by_field: dict[str, int] = {}
        improvements_by_field: dict[str, int] = {}
        for column in CANONICAL_COLUMNS:
            source = f"s.{q(column)}"
            base = f"b.{q(column)}"
            conflict_terms.append(
                f"({source} IS NOT NULL AND {base} IS NOT NULL AND CAST({source} AS VARCHAR) <> CAST({base} AS VARCHAR))"
            )
            improvement_terms.append(f"({base} IS NULL AND {source} IS NOT NULL)")
            conflicts_by_field[column] = int(con.execute(
                f"SELECT COUNT(*) FROM source_keyed s JOIN base_collapsed b USING (join_key) WHERE {conflict_terms[-1]}"
            ).fetchone()[0])
            improvements_by_field[column] = int(con.execute(
                f"SELECT COUNT(*) FROM source_keyed s JOIN base_collapsed b USING (join_key) WHERE {improvement_terms[-1]}"
            ).fetchone()[0])
        conflict_predicate = " OR ".join(conflict_terms) or "FALSE"
        improvement_predicate = " OR ".join(improvement_terms) or "FALSE"
        conflict_rows = int(con.execute(f"""
            SELECT COUNT(*) FROM source_keyed s JOIN base_collapsed b USING (join_key)
            WHERE {conflict_predicate}
        """).fetchone()[0])
        # Conflicting source keys are retained in the audit report but are
        # excluded from the promotable delta. They require adjudication and
        # must never overwrite a confirmed canonical value.
        con.execute(f"""
            COPY (
                SELECT
                    s.db_name, s.year, s.week, s.platform, s.manager,
                    s.team_name, s.team_key, s.fantasy_position,
                    s.NFL_player_id, s.player,
                    s.clutch_equity AS source_clutch_equity,
                    b.clutch_equity AS canonical_clutch_equity,
                    s.clutch_equity - b.clutch_equity AS clutch_delta
                FROM source_keyed s
                JOIN base_collapsed b USING (join_key)
                WHERE {conflict_predicate}
            ) TO '{str(out_dir / 'clutch_conflicts.parquet').replace("'", "''")}'
            (FORMAT PARQUET)
        """)
        con.execute(f"""
            COPY (
                SELECT {', '.join(f'COALESCE(b.{q(column)}, s.{q(column)}) AS {q(column)}' for column in CANONICAL_COLUMNS)}
                FROM source_keyed s
                LEFT JOIN base_collapsed b USING (join_key)
                WHERE b.join_key IS NULL OR ({improvement_predicate})
            ) TO '{str(out_dir / 'promotable_player_delta.parquet').replace("'", "''")}'
            (FORMAT PARQUET)
        """)
        promotable_rows = int(con.execute(f"""
            SELECT COUNT(*) FROM source_keyed s
            LEFT JOIN base_collapsed b USING (join_key)
            WHERE b.join_key IS NULL OR ({improvement_predicate})
        """).fetchone()[0])
        report = {
            "read_only": True,
            "source_rows": source_rows,
            "source_keys": source_keys,
            "duplicate_source_rows": duplicate_source_rows,
            "canonical_rows": base_rows,
            "canonical_rows_scoped_to_source_keys": True,
            "duplicate_canonical_rows": duplicate_base_rows,
            "canonical_conflicting_keys": canonical_conflicting_keys,
            "matched_rows": matched_rows,
            "new_rows": new_rows,
            "conflict_rows": conflict_rows,
            "promotable_rows": promotable_rows,
            "improvements_by_field": improvements_by_field,
            "conflicts_by_field": conflicts_by_field,
            "canonical_schema_sha256": _hash_columns(list(CANONICAL_COLUMNS)),
            "source_schema_sha256": _hash_columns(source_columns),
            "schema_unchanged": True,
            "cache_mutated": False,
            "new_lineage": False,
            "join_key": list(BASE_IDENTITY_COLUMNS) + ["coalesced_player_id"],
            "player_id_columns": list(PLAYER_ID_COLUMNS),
            "output_columns": list(CANONICAL_COLUMNS),
        }
        (out_dir / "player_delta_report.json").write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
        return report
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sidecar", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(validate_player_sidecar(
        args.base, args.sidecar, args.out_dir, args.shard_index, args.shard_count
    ), indent=2))


if __name__ == "__main__":
    main()
