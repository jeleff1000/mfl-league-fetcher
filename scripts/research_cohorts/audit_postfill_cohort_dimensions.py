"""Validate cohort-dimension sidecars against the canonical research cache.

This is intentionally read-only.  It never attaches a writable database and
never writes the corpus or ops cache.  It verifies that overlays are null-only,
unique, schema-compatible, and reports the residual population after applying
them in temporary SQL views.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def path_sql(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def count(con: duckdb.DuckDBPyConnection, sql: str, params: list[str] | None = None) -> int:
    return int(con.execute(sql, params or []).fetchone()[0])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--position", type=Path, required=True)
    ap.add_argument("--settings", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"ATTACH {path_sql(args.base)} AS lake (READ_ONLY)")
    try:
        pcols = {r[0] for r in con.execute("DESCRIBE lake.public.player_fantasy").fetchall()}
        scols = {r[0] for r in con.execute("DESCRIBE lake.public.league_settings").fetchall()}
        required_player = {"db_name", "year", "week", "position", "NFL_player_id"}
        required_settings = {"db_name", "year", "scoring_pass_td", "playoff_teams", "roster_FLX", "roster_SUPER_FLEX", "roster_IDP"}
        if not required_player <= pcols:
            raise SystemExit(f"canonical player schema missing: {sorted(required_player - pcols)}")
        if not required_settings <= scols:
            raise SystemExit(f"canonical settings schema missing: {sorted(required_settings - scols)}")

        pos = path_sql(args.position)
        settings = path_sql(args.settings)
        pos_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({pos})").fetchall()}
        settings_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({settings})").fetchall()}
        if not {"nfl_id", "season_year", "position_fill"} <= pos_cols:
            raise SystemExit(f"position sidecar schema missing: {sorted({ 'nfl_id', 'season_year', 'position_fill' } - pos_cols)}")
        if not required_settings - {"db_name", "year"} <= settings_cols:
            raise SystemExit(f"settings overlay schema missing: {sorted((required_settings - {'db_name', 'year'}) - settings_cols)}")

        # Contract checks: one key per sidecar and no impossible values.
        position_rows = count(con, f"SELECT COUNT(*) FROM read_parquet({pos})")
        position_keys = count(con, f"SELECT COUNT(*) FROM (SELECT DISTINCT nfl_id,season_year FROM read_parquet({pos}))")
        settings_rows = count(con, f"SELECT COUNT(*) FROM read_parquet({settings})")
        settings_keys = count(con, f"SELECT COUNT(*) FROM (SELECT DISTINCT db_name,year FROM read_parquet({settings}))")
        if position_rows != position_keys:
            raise SystemExit(f"duplicate position sidecar keys: {position_rows - position_keys}")
        if settings_rows != settings_keys:
            raise SystemExit(f"duplicate settings sidecar keys: {settings_rows - settings_keys}")
        bad_position = count(con, f"SELECT COUNT(*) FROM read_parquet({pos}) WHERE position_fill IS NULL OR TRIM(CAST(position_fill AS VARCHAR))='' ")
        if bad_position:
            raise SystemExit(f"blank position fills: {bad_position}")
        bad_bracket = count(con, f"SELECT COUNT(*) FROM read_parquet({settings}) WHERE playoff_teams IS NOT NULL AND playoff_teams NOT IN (4,6,8)")
        if bad_bracket:
            raise SystemExit(f"invalid playoff-team fills: {bad_bracket}")

        # Sidecar projections are temporary views only.
        con.execute(f"CREATE OR REPLACE TEMP VIEW position_overlay AS SELECT * FROM read_parquet({pos})")
        con.execute(f"CREATE OR REPLACE TEMP VIEW settings_overlay AS SELECT * FROM read_parquet({settings})")

        # A fill may only land in a currently-null cell.  Check every filled
        # canonical settings field and every position row key.
        overwrite = {}
        for field, fill in (("scoring_pass_td", "scoring_pass_td"), ("playoff_teams", "playoff_teams"),
                            ("roster_FLX", "roster_FLX"), ("roster_SUPER_FLEX", "roster_SUPER_FLEX"),
                            ("roster_IDP", "roster_IDP")):
            overwrite[field] = count(con, f"""
              SELECT COUNT(*) FROM lake.public.league_settings s
              JOIN settings_overlay o USING (db_name,year)
              WHERE o.{fill} IS NOT NULL AND s.{field} IS NOT NULL
            """)
        if any(overwrite.values()):
            raise SystemExit(f"non-null settings overwrite candidates: {overwrite}")

        # Position rows are keyed by player/year and intentionally fan out to
        # weekly rows.  The overlay is applied only to currently-null cells;
        # populated canonical positions are never overwritten.  Therefore a
        # player-season key may legitimately join populated rows whose legacy
        # raw label differs from the bio taxonomy (for example DE vs DL).
        # Report those differences for audit, but do not reject a null-only
        # fill for them.  Rejecting them would confuse a non-mutating overlay
        # with a schema/data replacement and would block valid DE->DL/CB->DB
        # normalization.
        canonical_broad = "CASE WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('DST','D/ST','DEFENSE','DEFENCE') THEN 'DEF' WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('DE','DT','NT','DL') THEN 'DL' WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('CB','S','SS','FS','SAF','DB') THEN 'DB' WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('OLB','ILB','MLB','LB') THEN 'LB' WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('FB','HB') THEN 'RB' WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('PK','PLACEKICKER') THEN 'K' ELSE UPPER(TRIM(CAST(p.position AS VARCHAR))) END"
        position_overwrite = count(con, f"""
          SELECT COUNT(*)
          FROM lake.public.player_fantasy p
          JOIN position_overlay o
            ON CAST(p.NFL_player_id AS VARCHAR)=o.nfl_id
           AND CAST(p.year AS INTEGER)=o.season_year
          WHERE o.position_fill IS NOT NULL
            AND p.position IS NOT NULL AND TRIM(CAST(p.position AS VARCHAR))<>''
            AND ({canonical_broad}) <> UPPER(TRIM(CAST(o.position_fill AS VARCHAR)))
        """)
        position_conflicts = [
            {"canonical": r[0], "overlay_position": r[1], "rows": int(r[2])}
            for r in con.execute(f"""
              SELECT ({canonical_broad}) canonical,
                     UPPER(TRIM(CAST(o.position_fill AS VARCHAR))) overlay_position,
                     COUNT(*)
              FROM lake.public.player_fantasy p
              JOIN position_overlay o
                ON CAST(p.NFL_player_id AS VARCHAR)=o.nfl_id
               AND CAST(p.year AS INTEGER)=o.season_year
              WHERE o.position_fill IS NOT NULL
                AND p.position IS NOT NULL AND TRIM(CAST(p.position AS VARCHAR))<>''
                AND ({canonical_broad}) <> UPPER(TRIM(CAST(o.position_fill AS VARCHAR)))
              GROUP BY 1,2 ORDER BY 3 DESC LIMIT 30
            """).fetchall()
        ]
        if position_overwrite:
            canonical_domain = [
                {"position": r[0], "rows": int(r[1])}
                for r in con.execute("""
                  SELECT UPPER(TRIM(CAST(position AS VARCHAR))),COUNT(*)
                  FROM lake.public.player_fantasy
                  WHERE position IS NOT NULL AND TRIM(CAST(position AS VARCHAR))<>''
                  GROUP BY 1 ORDER BY 2 DESC LIMIT 50
                """).fetchall()
            ]
            overlay_domain = [
                {"position": r[0], "rows": int(r[1])}
                for r in con.execute(f"""
                  SELECT UPPER(TRIM(CAST(position_fill AS VARCHAR))),COUNT(*)
                  FROM position_overlay GROUP BY 1 ORDER BY 2 DESC
                """).fetchall()
            ]
            examples = [
                {"nfl_id": r[0], "year": int(r[1]), "canonical": r[2], "overlay_position": r[3]}
                for r in con.execute(f"""
                  SELECT CAST(p.NFL_player_id AS VARCHAR),CAST(p.year AS INTEGER),
                         ({canonical_broad}),
                         UPPER(TRIM(CAST(o.position_fill AS VARCHAR)))
                  FROM lake.public.player_fantasy p
                  JOIN position_overlay o
                    ON CAST(p.NFL_player_id AS VARCHAR)=o.nfl_id
                   AND CAST(p.year AS INTEGER)=o.season_year
                  WHERE p.position IS NOT NULL AND TRIM(CAST(p.position AS VARCHAR))<>''
                    AND ({canonical_broad}) <> UPPER(TRIM(CAST(o.position_fill AS VARCHAR)))
                  LIMIT 50
                """).fetchall()
            ]
            print(json.dumps({"position_conflict_rows_nonblocking": position_overwrite,
                              "position_conflict_pairs": position_conflicts,
                              "canonical_position_domain": canonical_domain, "overlay_position_domain": overlay_domain,
                              "position_conflict_examples": examples}, sort_keys=True))

        # Post-fill position coverage.  The only remaining rows are reported
        # separately when they have no usable identity at all.
        pos_summary = {}
        pos_summary["canonical_null_before"] = count(con, """
          SELECT COUNT(*) FROM lake.public.player_fantasy
          WHERE db_name IS NOT NULL AND (position IS NULL OR TRIM(CAST(position AS VARCHAR))='')
        """)
        pos_summary["fillable_by_nfl_id"] = count(con, """
          SELECT COUNT(*) FROM lake.public.player_fantasy p
          JOIN position_overlay o ON CAST(p.NFL_player_id AS VARCHAR)=o.nfl_id AND CAST(p.year AS INTEGER)=o.season_year
          WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
        """)
        pos_summary["residual_null_after_overlay"] = pos_summary["canonical_null_before"] - pos_summary["fillable_by_nfl_id"]
        if {"player", "team_name", "team_key"} <= pcols:
            pos_summary["residual_empty_identity"] = count(con, """
              SELECT COUNT(*) FROM lake.public.player_fantasy p
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND p.NFL_player_id IS NULL
                AND NULLIF(TRIM(CAST(p.player AS VARCHAR)),'') IS NULL
                AND NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),'') IS NULL
                AND NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),'') IS NULL
            """)

        # Settings residuals after applying the overlay in a temporary view.
        setting_summary = {}
        for field in ("scoring_pass_td", "playoff_teams", "roster_FLX", "roster_SUPER_FLEX", "roster_IDP"):
            setting_summary[field] = {
                "missing_before": count(con, f"SELECT COUNT(*) FROM lake.public.league_settings WHERE {field} IS NULL"),
                "filled_by_overlay": count(con, f"SELECT COUNT(*) FROM lake.public.league_settings s JOIN settings_overlay o USING(db_name,year) WHERE s.{field} IS NULL AND o.{field} IS NOT NULL"),
            }
            setting_summary[field]["missing_after"] = setting_summary[field]["missing_before"] - setting_summary[field]["filled_by_overlay"]
        roster_before = count(con, """
          SELECT COUNT(*) FROM lake.public.league_settings
          WHERE roster_FLX IS NULL AND roster_SUPER_FLEX IS NULL AND roster_IDP IS NULL
            AND roster_DL IS NULL AND roster_LB IS NULL AND roster_DB IS NULL
            AND roster_DB_LB IS NULL AND roster_DL_LB IS NULL
        """)
        roster_filled = count(con, f"""
          SELECT COUNT(*)
          FROM lake.public.league_settings s
          LEFT JOIN settings_overlay o USING (db_name,year)
          WHERE s.roster_FLX IS NULL AND s.roster_SUPER_FLEX IS NULL AND s.roster_IDP IS NULL
            AND s.roster_DL IS NULL AND s.roster_LB IS NULL AND s.roster_DB IS NULL
            AND s.roster_DB_LB IS NULL AND s.roster_DL_LB IS NULL
            AND (o.roster_FLX IS NOT NULL OR o.roster_SUPER_FLEX IS NOT NULL OR o.roster_IDP IS NOT NULL)
        """)
        setting_summary["roster_any_config"] = {
            "missing_before": roster_before,
            "filled_by_overlay": roster_filled,
            "missing_after": roster_before - roster_filled,
        }
        # Best-ball has no universal safe inference.  Report the source
        # overlay if present, but never infer a value from lineup changes here.
        if "sleeper_best_ball" in scols:
            setting_summary["sleeper_best_ball"] = {
                "missing_before": count(con, "SELECT COUNT(*) FROM lake.public.league_settings WHERE sleeper_best_ball IS NULL"),
                "filled_by_overlay": count(con, "SELECT COUNT(*) FROM lake.public.league_settings s JOIN settings_overlay o USING(db_name,year) WHERE s.sleeper_best_ball IS NULL AND o.sleeper_best_ball IS NOT NULL"),
            }
            setting_summary["sleeper_best_ball"]["missing_after"] = setting_summary["sleeper_best_ball"]["missing_before"] - setting_summary["sleeper_best_ball"]["filled_by_overlay"]

        report = {
            "status": "postfill_validation_pass",
            "canonical_mutated": False,
            "new_lineage": False,
            "new_columns": [],
            "position_sidecar_rows": position_rows,
            "settings_sidecar_rows": settings_rows,
            "overwrite_candidates": overwrite,
            "position_overwrite_candidates": position_overwrite,
            "position_conflicts_nonblocking": position_conflicts,
            "position": pos_summary,
            "settings": setting_summary,
        }
        (args.out / "postfill_dimension_validation.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(report, sort_keys=True))
    finally:
        con.close()


if __name__ == "__main__":
    main()
