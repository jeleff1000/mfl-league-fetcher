"""Combine source-settings shards into one null-only settings overlay.

The canonical snapshot is read-only.  This command is a validator/combiner
for downloaded artifacts; it does not replace the snapshot or alter its
schema.  Source and local evidence are normalized to the canonical domains;
invalid values are discarded and disagreements are reported, not silently
used to overwrite canonical values.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

FIELDS = (
    "scoring_pass_td",
    "playoff_teams",
    "roster_FLX",
    "roster_SUPER_FLEX",
    "roster_IDP",
    "sleeper_best_ball",
)


def sql_path(path: str) -> str:
    """Return a safely quoted DuckDB path/glob literal."""
    return "'" + str(path).replace("'", "''") + "'"


def combine_parquet(settings_parquet: Path, sidecars: str, out: Path, local_sidecar: str | None = None) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        settings_cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(settings_parquet)]).fetchall()}
        required = {"db_name", "year"} | set(FIELDS)
        if required - settings_cols:
            raise SystemExit(f"settings parquet missing columns: {sorted(required-settings_cols)}")
        source_cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)", [sidecars]).fetchall()}
        required_source = {"db_name", "year"} | {f + "_fill" for f in FIELDS}
        if required_source - source_cols:
            raise SystemExit(f"source sidecar missing columns: {sorted(required_source-source_cols)}")
        duplicate = con.execute(
            """
            SELECT COUNT(*) - COUNT(DISTINCT (CAST(db_name AS VARCHAR), CAST(year AS INTEGER)))
            FROM read_parquet(?, union_by_name=true)
            """, [sidecars]
        ).fetchone()[0]
        if duplicate:
            raise SystemExit(f"duplicate source sidecar keys: {duplicate}")
        if local_sidecar:
            local_cols = {r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [local_sidecar]).fetchall()}
            required_local = {"db_name", "year", "pass_td_fill", "playoff_teams_fill", "roster_FLX_fill", "roster_SUPER_FLEX_fill", "roster_IDP_fill"}
            if required_local - local_cols:
                raise SystemExit(f"local settings sidecar missing columns: {sorted(required_local-local_cols)}")
            local_duplicate = con.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT (CAST(db_name AS VARCHAR), CAST(year AS INTEGER))) FROM read_parquet(?)", [local_sidecar]
            ).fetchone()[0]
            if local_duplicate:
                raise SystemExit(f"duplicate local settings keys: {local_duplicate}")
            local_relation = f"read_parquet({sql_path(local_sidecar)})"
        else:
            local_relation = "(SELECT NULL::VARCHAR AS db_name, NULL::INTEGER AS year, NULL::INTEGER AS pass_td_fill, NULL::INTEGER AS playoff_teams_fill, NULL::INTEGER AS roster_FLX_fill, NULL::INTEGER AS roster_SUPER_FLEX_fill, NULL::INTEGER AS roster_IDP_fill, NULL::BOOLEAN AS best_ball_fill WHERE FALSE)"

        # Normalize source values before combining.  The API has returned
        # malformed values in the past (for example pass TD=50 and FLX=13),
        # so those values are evidence of a source response but never become
        # canonical fills.
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW source_settings_raw AS
            SELECT
              CAST(db_name AS VARCHAR) AS db_name,
              CAST(year AS INTEGER) AS year,
              CASE WHEN TRY_CAST(scoring_pass_td_fill AS INTEGER) IN (4,6) THEN TRY_CAST(scoring_pass_td_fill AS INTEGER) END AS scoring_pass_td_fill,
              CASE WHEN TRY_CAST(playoff_teams_fill AS INTEGER) IN (4,6,8) THEN TRY_CAST(playoff_teams_fill AS INTEGER) END AS playoff_teams_fill,
              CASE WHEN TRY_CAST(roster_FLX_fill AS INTEGER) BETWEEN 1 AND 5 THEN TRY_CAST(roster_FLX_fill AS INTEGER) END AS roster_FLX_fill,
              TRY_CAST(roster_SUPER_FLEX_fill AS INTEGER) AS roster_SUPER_FLEX_fill,
              TRY_CAST(roster_IDP_fill AS INTEGER) AS roster_IDP_fill,
              TRY_CAST(sleeper_best_ball_fill AS BOOLEAN) AS sleeper_best_ball_fill,
              'source_api' AS evidence_source
            FROM read_parquet({sql_path(sidecars)}, union_by_name=true)
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW local_settings_raw AS
            SELECT
              CAST(db_name AS VARCHAR) AS db_name,
              CAST(year AS INTEGER) AS year,
              CASE WHEN TRY_CAST(pass_td_fill AS INTEGER) IN (4,6) THEN TRY_CAST(pass_td_fill AS INTEGER) END AS scoring_pass_td_fill,
              CASE WHEN TRY_CAST(playoff_teams_fill AS INTEGER) IN (4,6,8) THEN TRY_CAST(playoff_teams_fill AS INTEGER) END AS playoff_teams_fill,
              CASE WHEN TRY_CAST(roster_FLX_fill AS INTEGER) BETWEEN 1 AND 5 THEN TRY_CAST(roster_FLX_fill AS INTEGER) END AS roster_FLX_fill,
              TRY_CAST(roster_SUPER_FLEX_fill AS INTEGER) AS roster_SUPER_FLEX_fill,
              TRY_CAST(roster_IDP_fill AS INTEGER) AS roster_IDP_fill,
              TRY_CAST(best_ball_fill AS BOOLEAN) AS sleeper_best_ball_fill,
              'local_evidence' AS evidence_source
            FROM {local_relation}
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW combined_settings_sources AS
            SELECT * FROM source_settings_raw
            UNION ALL
            SELECT * FROM local_settings_raw
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW source_group AS
            SELECT db_name, year,
              MAX(scoring_pass_td_fill) AS scoring_pass_td_fill,
              MAX(playoff_teams_fill) AS playoff_teams_fill,
              MAX(roster_FLX_fill) AS roster_FLX_fill,
              MAX(roster_SUPER_FLEX_fill) AS roster_SUPER_FLEX_fill,
              MAX(roster_IDP_fill) AS roster_IDP_fill,
              MAX(sleeper_best_ball_fill) AS sleeper_best_ball_fill
            FROM source_settings_raw GROUP BY 1,2
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW local_group AS
            SELECT db_name, year,
              MAX(scoring_pass_td_fill) AS scoring_pass_td_fill,
              MAX(playoff_teams_fill) AS playoff_teams_fill,
              MAX(roster_FLX_fill) AS roster_FLX_fill,
              MAX(roster_SUPER_FLEX_fill) AS roster_SUPER_FLEX_fill,
              MAX(roster_IDP_fill) AS roster_IDP_fill,
              MAX(sleeper_best_ball_fill) AS sleeper_best_ball_fill
            FROM local_settings_raw GROUP BY 1,2
            """
        )
        # Fleaflicker has no sleeper_best_ball field.  Materialize every
        # canonical Fleaflicker null as an explicit managed=false candidate,
        # including keys for which the source worker returned no other
        # settings.  Without this canonical-key scan, only Fleaflicker rows
        # that happened to produce another sidecar field could be closed.
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW non_applicable_best_ball AS
            SELECT CAST(db_name AS VARCHAR) AS db_name,
                   CAST(year AS INTEGER) AS year,
                   FALSE AS sleeper_best_ball_fill
            FROM read_parquet({sql_path(str(settings_parquet))})
            WHERE db_name LIKE 'smpl_ffl_%' AND sleeper_best_ball IS NULL
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW candidate_settings AS
            SELECT COALESCE(s.db_name,z.db_name) AS db_name,
                   CAST(COALESCE(s.year,z.year) AS INTEGER) AS year,
                   COALESCE(s.scoring_pass_td_fill,z.scoring_pass_td_fill) AS scoring_pass_td_fill,
                   COALESCE(s.playoff_teams_fill,z.playoff_teams_fill) AS playoff_teams_fill,
                   COALESCE(s.roster_FLX_fill,z.roster_FLX_fill) AS roster_FLX_fill,
                   COALESCE(s.roster_SUPER_FLEX_fill,z.roster_SUPER_FLEX_fill) AS roster_SUPER_FLEX_fill,
                   COALESCE(s.roster_IDP_fill,z.roster_IDP_fill) AS roster_IDP_fill,
                   COALESCE(s.sleeper_best_ball_fill,z.sleeper_best_ball_fill) AS sleeper_best_ball_fill
            FROM source_group s FULL OUTER JOIN local_group z USING (db_name,year)
            UNION ALL
            SELECT n.db_name,n.year,NULL,NULL,NULL,NULL,NULL,n.sleeper_best_ball_fill
            FROM non_applicable_best_ball n
            WHERE NOT EXISTS (
              SELECT 1 FROM source_group s FULL OUTER JOIN local_group z USING (db_name,year)
              WHERE COALESCE(s.db_name,z.db_name)=n.db_name AND COALESCE(s.year,z.year)=n.year
            )
            """
        )
        unmatched = con.execute(
            f"""
            SELECT COUNT(*) FROM (
              SELECT DISTINCT db_name, year FROM combined_settings_sources
            ) s LEFT JOIN read_parquet({sql_path(str(settings_parquet))}) l USING (db_name,year)
            WHERE l.db_name IS NULL
            """
        ).fetchone()[0]
        if unmatched:
            raise SystemExit(f"source keys absent from canonical settings: {unmatched}")
        conflict_expr = ",".join(
            f"COUNT(DISTINCT CAST({f}_fill AS VARCHAR)) FILTER (WHERE {f}_fill IS NOT NULL) AS {f}_conflicts"
            for f in FIELDS
        )
        conflict_rows = con.execute(
            "SELECT * FROM (SELECT db_name,year," + conflict_expr + " FROM combined_settings_sources GROUP BY 1,2) q WHERE "
            + " OR ".join(f"{f}_conflicts > 1" for f in FIELDS)
        ).fetchall()
        # Fleaflicker does not expose Sleeper's best-ball field.  Its nulls are
        # an inapplicable platform field, not unknown best-ball leagues.  The
        # canonical research population treats those league-years as managed
        # (false) so downstream cohort filters never mistake platform nulls
        # for a missing setting.  This remains a null-only overlay and does
        # not overwrite an existing value.
        select_fields = []
        for f in FIELDS:
            if f == "sleeper_best_ball":
                fill = "c.sleeper_best_ball_fill"
            else:
                fill = f"c.{f}_fill"
            select_fields.append(f"CASE WHEN l.{f} IS NULL THEN {fill} END AS {f}")
        con.execute(
            f"""
            COPY (
              SELECT c.db_name,CAST(c.year AS INTEGER) AS year,
                     {','.join(select_fields)},
                     'platform_settings_api_and_local' AS source
              FROM candidate_settings c
              JOIN read_parquet({sql_path(str(settings_parquet))}) l USING (db_name,year)
              WHERE """ + " OR ".join(f"l.{f} IS NULL AND c.{f}_fill IS NOT NULL" for f in FIELDS) +
            f") TO ? (FORMAT PARQUET)",
            [str(out / "league_settings_updates_combined.parquet")],
        )
        result = {
            "status": "combined_null_only_settings_overlay",
            "canonical_mutated": False,
            "new_lineage": False,
            "source_sidecar_keys": int(con.execute("SELECT COUNT(DISTINCT (db_name,year)) FROM source_settings_raw").fetchone()[0]),
            "local_sidecar_keys": int(con.execute("SELECT COUNT(DISTINCT (db_name,year)) FROM local_settings_raw").fetchone()[0]),
            "conflicting_keys_reported": len(conflict_rows),
            "output_rows": int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out / "league_settings_updates_combined.parquet")]).fetchone()[0]),
            "fills_by_field": {
                f: int(con.execute(f"SELECT COUNT(*) FROM read_parquet(?) WHERE {f} IS NOT NULL", [str(out / "league_settings_updates_combined.parquet")]).fetchone()[0])
                for f in FIELDS
            },
            "best_ball_non_applicable_filled": int(con.execute(
                "SELECT COUNT(*) FROM read_parquet(?) WHERE sleeper_best_ball IS FALSE AND db_name LIKE 'smpl_ffl_%'",
                [str(out / "league_settings_updates_combined.parquet")],
            ).fetchone()[0]),
            "best_ball_non_applicable_canonical_nulls": int(con.execute(
                "SELECT COUNT(*) FROM non_applicable_best_ball",
            ).fetchone()[0]),
        }
    finally:
        con.close()
    (out / "combine_report.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--settings-parquet", type=Path, required=True)
    ap.add_argument("--sidecars", required=True, help="DuckDB read_parquet glob")
    ap.add_argument("--local-sidecar", help="Optional local-evidence settings sidecar")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    combine_parquet(args.settings_parquet, args.sidecars, args.out, args.local_sidecar)


if __name__ == "__main__":
    main()
