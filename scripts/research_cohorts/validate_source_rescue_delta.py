"""Validate a source rescue sidecar against canonical team-week data.

This is a read-only, shardable gate.  It never creates a lake snapshot and it
never changes canonical values.  The source rescue artifact is team/matchup
level; it is propagated one-to-many onto every already-present player row for
the matched manager/team/week.  It cannot create absent player rows because
it contains no player identity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


CANONICAL_PLAYER_FIELDS = ("win", "team_points", "is_playoffs", "champion", "clutch_equity", "platform")
POSITIVE_SIGNAL_FIELDS = frozenset(("is_playoffs", "champion"))


def schema(con: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, str]]:
    return [{"name": r[0], "type": r[1], "null": r[2]} for r in con.execute(f"DESCRIBE {relation}").fetchall()]


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def source_conflict_term(field: str, source: str, base: str) -> str:
    if field in POSITIVE_SIGNAL_FIELDS:
        return f"({source} IS NOT NULL AND CAST({source} AS INTEGER)=1 AND {base} IS NOT NULL AND CAST({base} AS INTEGER)<>1)"
    return f"({base} IS NOT NULL AND {source} IS NOT NULL AND CAST({base} AS VARCHAR) <> CAST({source} AS VARCHAR))"


def source_improvement_term(field: str, source: str, base: str) -> str:
    if field in POSITIVE_SIGNAL_FIELDS:
        return f"({base} IS NULL AND {source} IS NOT NULL AND CAST({source} AS INTEGER)=1)"
    return f"({base} IS NULL AND {source} IS NOT NULL)"


def source_overlay_expr(field: str, source: str, base: str) -> str:
    if field in POSITIVE_SIGNAL_FIELDS:
        return f"CASE WHEN {source} IS NOT NULL AND CAST({source} AS INTEGER)=1 THEN 1 ELSE {base} END"
    return f"COALESCE({source}, {base})"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--sidecar", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--shard-index", type=int, required=True)
    ap.add_argument("--shard-count", type=int, required=True)
    args = ap.parse_args()
    if not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("invalid shard index")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.base), read_only=True)
    p_schema = schema(con, "public.player_fantasy")
    p_cols = {r["name"] for r in p_schema}
    required = {"db_name", "year", "week", "team_key"}
    missing = sorted(required - p_cols)
    if missing:
        raise SystemExit(f"canonical player_fantasy lacks join columns: {missing}")

    sidecar_uri = str(args.sidecar.resolve()).replace("'", "''")
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW source_rows AS SELECT * FROM read_parquet('{sidecar_uri}')"
    )
    s_schema = schema(con, "source_rows")
    s_cols = {r["name"] for r in s_schema}
    missing_source = sorted({"db_name", "year", "week", "team_key"} - s_cols)
    if missing_source:
        raise SystemExit(f"source sidecar lacks required identity columns: {missing_source}")
    source_team_expr = "LOWER(NULLIF(TRIM(CAST(team_name AS VARCHAR)), ''))" if "team_name" in s_cols else "NULL"
    source_manager_expr = "LOWER(NULLIF(TRIM(CAST(manager AS VARCHAR)), ''))"
    source_team_key_expr = "NULLIF(TRIM(CAST(team_key AS VARCHAR)), '')"
    # The sidecar is team-level.  This key is deliberately independent of
    # source-only franchise identifiers, which are not present in the 29-column
    # canonical player schema.
    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW source_keyed AS
      SELECT *,
        CAST(db_name AS VARCHAR) AS k_db_name,
        CAST(year AS INTEGER) AS k_year,
        CAST(week AS INTEGER) AS k_week,
        {source_team_key_expr} AS k_team_key,
        {source_manager_expr} AS k_manager,
        {source_team_expr} AS k_team,
        CONCAT_WS('|', CAST(db_name AS VARCHAR), CAST(year AS INTEGER), CAST(week AS INTEGER),
                  COALESCE({source_team_key_expr}, CONCAT_WS(':', COALESCE({source_manager_expr}, '<NULL>'), COALESCE({source_team_expr}, '<NULL>')))) AS join_key
      FROM source_rows
    """)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW source_shard AS
        SELECT * FROM source_keyed
        WHERE k_db_name IS NOT NULL AND k_year IS NOT NULL AND k_week IS NOT NULL
          AND (k_team_key IS NOT NULL OR k_manager IS NOT NULL OR k_team IS NOT NULL)
          AND MOD(ABS(HASH(join_key)), {args.shard_count}) = {args.shard_index}
        """
    )

    source_fields = [c for c in CANONICAL_PLAYER_FIELDS if c in p_cols and c in s_cols]
    if "is_championship" in s_cols and "champion" in p_cols and "champion" not in source_fields:
        source_fields.append("is_championship")
    if not source_fields:
        raise SystemExit("source sidecar has no canonical fields to promote")

    # A source key is promotable only if every non-null value for each target
    # field agrees.  Conflicts are reported and excluded, never guessed.
    distinct_expr = []
    for field in source_fields:
        if field in POSITIVE_SIGNAL_FIELDS:
            continue
        distinct_expr.append(
            f"COUNT(DISTINCT CAST({ident(field)} AS VARCHAR)) FILTER (WHERE {ident(field)} IS NOT NULL) AS {ident('n_' + field)}"
        )
    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW source_keys AS
      SELECT join_key, k_db_name, k_year, k_week, k_team_key, k_manager, k_team,
             COUNT(*) AS source_rows,
             {', '.join(distinct_expr)}
      FROM source_shard
      GROUP BY 1,2,3,4,5,6,7
    """)
    conflict_pred = " OR ".join(
        f"n_{field} > 1" for field in source_fields if field not in POSITIVE_SIGNAL_FIELDS
    ) or "FALSE"
    source_key_count = con.execute("SELECT COUNT(*) FROM source_keys").fetchone()[0]
    conflict_keys = con.execute(f"SELECT COUNT(*) FROM source_keys WHERE {conflict_pred}").fetchone()[0]

    # Reduce only non-conflicting keys.  MAX is safe after the conflict gate.
    agg_expr = []
    for field in source_fields:
        agg_expr.append(f"MAX({ident(field)}) AS {ident(field)}")
    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW promotable_source AS
      SELECT k_db_name, k_year, k_week, k_team_key, k_manager, k_team, join_key,
             {', '.join(agg_expr)}
      FROM source_shard
      WHERE join_key IN (SELECT join_key FROM source_keys WHERE NOT ({conflict_pred}))
      GROUP BY 1,2,3,4,5,6,7
    """)

    # Join to existing player rows. Prefer team_key when both sides have it;
    # otherwise use normalized manager + team_name. The canonical player table
    # has many legacy rows with null team_key, so a source team_key must not
    # disable the stable fallback identity.
    base_team_key = "NULLIF(TRIM(CAST(p.team_key AS VARCHAR)), '')"
    base_manager = "LOWER(NULLIF(TRIM(CAST(p.manager AS VARCHAR)), ''))"
    base_team = "LOWER(NULLIF(TRIM(CAST(p.team_name AS VARCHAR)), ''))"
    team_key_join = f"(s.k_team_key IS NOT NULL AND {base_team_key} = s.k_team_key)"
    manager_team_join = f"(s.k_manager IS NOT NULL AND s.k_team IS NOT NULL AND {base_manager} = s.k_manager AND {base_team} = s.k_team)"
    # When the canonical row lacks team_key and team_name, manager is still a
    # valid team identity only if the source has exactly one team for that
    # manager in this league/week. Ambiguous multi-team managers are excluded.
    manager_only_join = f"""(
      s.k_manager IS NOT NULL AND {base_manager} = s.k_manager
      AND NOT EXISTS (
        SELECT 1 FROM promotable_source s2
        WHERE s2.k_db_name=s.k_db_name AND s2.k_year=s.k_year AND s2.k_week=s.k_week
          AND s2.k_manager=s.k_manager
          AND s2.k_team_key IS DISTINCT FROM s.k_team_key
      )
    )"""
    join = f"p.db_name=s.k_db_name AND CAST(p.year AS INTEGER)=s.k_year AND CAST(p.week AS INTEGER)=s.k_week AND ({team_key_join} OR {manager_team_join} OR {manager_only_join})"
    matched = con.execute(f"SELECT COUNT(*) FROM promotable_source s JOIN public.player_fantasy p ON {join}").fetchone()[0]
    source_keys_matched = con.execute(f"SELECT COUNT(DISTINCT s.join_key) FROM promotable_source s JOIN public.player_fantasy p ON {join}").fetchone()[0]
    unmatched = con.execute(f"SELECT COUNT(*) FROM promotable_source s WHERE NOT EXISTS (SELECT 1 FROM public.player_fantasy p WHERE {join})").fetchone()[0]
    player_identity = " OR ".join(
        f"NULLIF(TRIM(CAST(p.{ident(column)} AS VARCHAR)), '') IS NOT NULL"
        for column in ("NFL_player_id", "sleeper_player_id", "fleaflicker_player_id", "mfl_player_id", "espn_player_id", "yahoo_player_id")
        if column in p_cols
    ) or "FALSE"
    # This is deliberately a one-to-many team-to-player join.  The rescue
    # source has no player identity; every canonical player row belonging to
    # the matched manager/team/week receives the team-level signals.  Player
    # identity is diagnostic only and must never filter the propagation.
    player_join = join
    player_identity_join = f"{join} AND ({player_identity})"
    matched_player_source_keys = con.execute(
        f"SELECT COUNT(DISTINCT s.join_key) FROM promotable_source s JOIN public.player_fantasy p ON {player_join}"
    ).fetchone()[0]
    player_rows_without_identity = con.execute(
        f"SELECT COUNT(*) FROM public.player_fantasy p JOIN promotable_source s ON {player_join} WHERE NOT ({player_identity})"
    ).fetchone()[0]
    base_player_team_key_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE team_key IS NOT NULL").fetchone()[0]
    base_player_manager_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE manager IS NOT NULL AND TRIM(CAST(manager AS VARCHAR)) <> ''").fetchone()[0]
    base_player_team_name_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE team_name IS NOT NULL AND TRIM(CAST(team_name AS VARCHAR)) <> ''").fetchone()[0]
    base_player_manager_team_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE manager IS NOT NULL AND TRIM(CAST(manager AS VARCHAR)) <> '' AND team_name IS NOT NULL AND TRIM(CAST(team_name AS VARCHAR)) <> ''").fetchone()[0]
    source_manager_team_keys = con.execute("SELECT COUNT(*) FROM promotable_source WHERE k_manager IS NOT NULL AND k_team IS NOT NULL").fetchone()[0]
    source_manager_only_eligible = con.execute("""
      SELECT COUNT(*) FROM promotable_source s
      WHERE s.k_manager IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM promotable_source s2
          WHERE s2.k_db_name=s.k_db_name AND s2.k_year=s.k_year AND s2.k_week=s.k_week
            AND s2.k_manager=s.k_manager
            AND s2.k_team_key IS DISTINCT FROM s.k_team_key
        )
    """).fetchone()[0]

    matchup_metrics = {"matchup_table_present": False}
    table_names = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public'").fetchall()}
    if "matchup" in table_names:
        m_schema = schema(con, "public.matchup")
        m_cols = {r["name"] for r in m_schema}
        matchup_metrics["matchup_table_present"] = True
        matchup_metrics["matchup_schema_sha256"] = hashlib.sha256(json.dumps(m_schema, sort_keys=True).encode()).hexdigest()
        if {"db_name", "year", "week", "team_key"} <= m_cols:
            mjoin = "m.db_name=s.k_db_name AND CAST(m.year AS INTEGER)=s.k_year AND CAST(m.week AS INTEGER)=s.k_week AND CAST(m.team_key AS VARCHAR) IS NOT DISTINCT FROM s.k_team_key"
            matchup_metrics["matchup_join_key"] = ["db_name", "year", "week", "team_key"]
        elif {"db_name", "year", "week", "manager", "team_name"} <= m_cols:
            mjoin = "m.db_name=s.k_db_name AND CAST(m.year AS INTEGER)=s.k_year AND CAST(m.week AS INTEGER)=s.k_week AND m.manager IS NOT DISTINCT FROM s.k_manager AND m.team_name IS NOT DISTINCT FROM s.k_team"
            matchup_metrics["matchup_join_key"] = ["db_name", "year", "week", "manager", "team_name"]
        else:
            mjoin = None
            matchup_metrics["matchup_join_key"] = None
        matchup_metrics["base_matchup_rows"] = int(con.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0])
        if mjoin:
            matchup_metrics["matched_source_matchup_keys"] = int(con.execute(f"SELECT COUNT(DISTINCT s.join_key) FROM promotable_source s JOIN public.matchup m ON {mjoin}").fetchone()[0])
            matchup_metrics["new_source_matchup_keys"] = int(con.execute(f"SELECT COUNT(*) FROM promotable_source s WHERE NOT EXISTS (SELECT 1 FROM public.matchup m WHERE {mjoin})").fetchone()[0])

    improvements = {}
    conflicts = {}
    conflict_terms = []
    for field in source_fields:
        target = "champion" if field == "is_championship" and "champion" in p_cols else field
        if target not in p_cols:
            continue
        improvements[target] = con.execute(
            f"SELECT COUNT(*) FROM public.player_fantasy p JOIN promotable_source s ON {player_join} WHERE {source_improvement_term(target, f's.{ident(field)}', f'p.{ident(target)}')}"
        ).fetchone()[0]
        conflict_terms.append(source_conflict_term(target, f"s.{ident(field)}", f"p.{ident(target)}"))
        conflicts[target] = con.execute(
            f"SELECT COUNT(*) FROM public.player_fantasy p JOIN promotable_source s ON {player_join} WHERE {conflict_terms[-1]}"
        ).fetchone()[0]
    conflict_rows = int(con.execute(
        f"SELECT COUNT(*) FROM public.player_fantasy p JOIN promotable_source s ON {player_join} WHERE {' OR '.join(conflict_terms) or 'FALSE'}"
    ).fetchone()[0])
    conflict_samples = []
    if conflict_rows:
        sample_fields = [
            "s.k_db_name", "s.k_year", "s.k_week", "s.k_team_key", "s.k_manager",
            "s.champion", "p.champion", "p.NFL_player_id", "p.platform", "p.player",
            "p.sleeper_player_id", "p.fleaflicker_player_id", "p.mfl_player_id", "p.manager", "p.team_name",
        ]
        for row in con.execute(
            f"SELECT {', '.join(sample_fields)} FROM public.player_fantasy p JOIN promotable_source s ON {player_join} "
            f"WHERE {' OR '.join(conflict_terms) or 'FALSE'} LIMIT 25"
        ).fetchall():
            conflict_samples.append({
                "db_name": row[0], "year": row[1], "week": row[2], "team_key": row[3],
                "manager": row[4], "source_champion": row[5],
                "canonical_champion": row[6], "NFL_player_id": row[7], "platform": row[8],
                "player": row[9], "sleeper_player_id": row[10],
                "fleaflicker_player_id": row[11], "mfl_player_id": row[12],
                "canonical_manager": row[13], "team_name": row[14],
            })
    # Conflicting source keys are deliberately excluded from promotable_source
    # above.  Do not abort the shard after writing no output: the remaining
    # keys are still independently auditable and promotable, and the conflict
    # counts must survive so the full matrix can be inventoried.  A conflict
    # is never allowed to overwrite a confirmed canonical value.

    # Emit exact canonical player rows. This is intentionally one-to-many:
    # one team/week source row fans out to every canonical player row on that
    # team/week. The source has no player identity, so it must never be used as
    # a player-level join or as an insert source.
    overlay_expr = []
    for field in [r["name"] for r in p_schema]:
        if field in source_fields and field in p_cols:
            overlay_expr.append(
                f"{source_overlay_expr(field, f's.{ident(field)}', f'p.{ident(field)}')} AS {ident(field)}"
            )
        else:
            overlay_expr.append(f"p.{ident(field)} AS {ident(field)}")
    improvement_predicate = " OR ".join(
        source_improvement_term(field, f"s.{ident(field)}", f"p.{ident(field)}")
        for field in source_fields if field in p_cols
    ) or "FALSE"
    con.execute(f"""
      COPY (
        SELECT {', '.join(overlay_expr)}
        FROM public.player_fantasy p
        JOIN promotable_source s ON {player_join}
        WHERE {improvement_predicate}
      ) TO ? (FORMAT PARQUET)
    """, [str(args.out_dir / "promotable_delta.parquet")])

    # This artifact has no NFL_player_id (or any platform player id), so it is
    # categorically incapable of inserting missing player_fantasy rows. Keep
    # this explicit in the report: player_delta_rows are existing player rows
    # updated by propagation, not player rows created by this sidecar.
    team_week_delta_rows = int(con.execute("SELECT COUNT(*) FROM promotable_source").fetchone()[0])
    player_delta_rows = int(con.execute(f"""
      SELECT COUNT(*) FROM public.player_fantasy p
      JOIN promotable_source s ON {player_join}
      WHERE {improvement_predicate}
    """).fetchone()[0])
    report = {
        "validation_status": "conflicts_excluded" if conflict_rows else "clean",
        "read_only": True,
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "base_schema_sha256": hashlib.sha256(json.dumps(p_schema, sort_keys=True).encode()).hexdigest(),
        "source_schema_sha256": hashlib.sha256(json.dumps(s_schema, sort_keys=True).encode()).hexdigest(),
        "base_player_columns": [r["name"] for r in p_schema],
        "source_fields_considered": source_fields,
        "source_rows_in_shard": int(con.execute("SELECT COUNT(*) FROM source_shard").fetchone()[0]),
        "source_keys": int(source_key_count),
        "conflicting_source_keys_excluded": int(conflict_keys),
        "promotable_unique_team_keys": int(con.execute("SELECT COUNT(*) FROM promotable_source").fetchone()[0]),
        "matched_source_team_keys": int(source_keys_matched),
        "unmatched_source_team_keys": int(unmatched),
        "matched_player_rows": int(matched),
        "matched_player_source_keys": int(matched_player_source_keys),
        "matched_player_rows_without_identity": int(player_rows_without_identity),
        "unmapped_team_match_rows_excluded_from_player_delta": 0,
        "base_player_rows_with_nonnull_team_key": int(base_player_team_key_rows),
        "base_player_rows_with_manager": int(base_player_manager_rows),
        "base_player_rows_with_team_name": int(base_player_team_name_rows),
        "base_player_rows_with_manager_and_team_name": int(base_player_manager_team_rows),
        "source_team_keys_with_manager_and_team_name": int(source_manager_team_keys),
        "source_team_keys_with_unambiguous_manager_fallback": int(source_manager_only_eligible),
        "improvements_by_field": {k: int(v) for k, v in improvements.items()},
        "conflict_rows": conflict_rows,
        "conflicts_by_field": {k: int(v) for k, v in conflicts.items()},
        "conflict_samples": conflict_samples,
        "team_week_delta_rows": team_week_delta_rows,
        "player_delta_rows": player_delta_rows,
        "player_identity_columns_present": False,
        "player_rows_insertable": 0,
        "player_row_action": "existing_rows_only_team_overlay",
        "output_columns": [r["name"] for r in p_schema],
        "canonical_schema_unchanged": True,
        "cache_mutated": False,
        "new_lineage": False,
        "join_key": ["db_name", "year", "week", "team_key"],
        "fallback_join_key": ["db_name", "year", "week", "manager", "team_name"],
        **matchup_metrics,
    }
    (args.out_dir / "promotion_delta_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    con.close()


if __name__ == "__main__":
    main()
