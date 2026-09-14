"""Audit and map-spec PBP result fields already represented by canonical stats."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.sota_recon.sources import v26_plane


RAW = Path(r"D:\league-history-data\nfl\raw\stathead\generated\pbp_merged_1978_2025\nfl_pbp_1978_2025_merged.parquet")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pbp-mapped-result-fields-2025.json")


MAPS = {
    "field_goal_result": {
        "disposition": "MAPPED_TO_EXISTING_CANONICAL_STATS",
        "canonical_targets": ["fg_made", "fg_missed", "fg_blocked", "fg_att"],
        "value_mapping": {"made": "fg_made", "missed": "fg_missed", "blocked": ["fg_blocked", "fg_missed"]},
        "definition_note": "Canonical fg_missed is unsuccessful-attempt scope and includes blocked attempts; PBP keeps blocked as a distinct result.",
    },
    "extra_point_result": {
        "disposition": "MAPPED_TO_EXISTING_CANONICAL_STATS",
        "canonical_targets": ["pat_made", "pat_missed", "pat_blocked", "pat_att"],
        "value_mapping": {"good": "pat_made", "failed": "pat_missed", "blocked": ["pat_blocked", "pat_missed"]},
        "definition_note": "Canonical PAT miss scope is broader than the non-null PBP result vocabulary; null/no-play and blocked handling remain source-definition evidence.",
    },
    "two_point_conv_result": {
        "disposition": "MAPPED_AS_CONVERSION_OUTCOME_WITNESS",
        "canonical_targets": ["passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"],
        "value_mapping": {"success": "typed offensive 2-point conversion counters", "failure": "attempt denominator; no made-conversion increment"},
        "definition_note": "The result field supplies success/failure; pass/rush/receiver typing comes from the play description and player-role atoms. Defensive two-point attempts remain separate.",
    },
    "series_result": {
        "disposition": "MAPPED_EVENT_STATE_WITNESS",
        "canonical_targets": [
            "def_fourth_down_faced",
            "def_fourth_down_allowed",
            "def_third_down_faced",
            "def_third_down_allowed",
            "three_out",
            "pts_def_3out",
        ],
        "target_lanes": ["weekly_game_context", "team_season_derivations"],
        "value_mapping": {
            "Turnover on downs": "fourth-down failure/stop sequence witness; def_fourth_down_faced and def_fourth_down_allowed are the canonical defensive opportunity/conversion counters",
            "Punt": "three-and-out sequence derivation mapped to three_out and pts_def_3out when the complete series has zero first downs",
            "First down": "first-down derivations, typed by play/down context",
            "Touchdown/Field goal/Safety/End of half": "drive terminal outcome and team points derivations",
        },
        "sequence_derivations": {
            "three_and_out": "count distinct game_id/posteam/series groups with series_result=Punt and drive_first_downs=0; maps to three_out and pts_def_3out",
            "fourth_down_conversion": "requires fourth-down attempt plus result; no standalone canonical conversion column currently exists",
        },
        "grain_boundary": "fourth_down_stop is a separate defensive/player statistic and must not be substituted for the team/game fourth-down failure count; pts_def_forced_punts is not the three-and-out counter",
    },
    "play_type_nfl": {
        "disposition": "MAPPED_EVENT_TYPE_WITNESS",
        "canonical_targets": [],
        "target_lanes": ["weekly_derivations", "team_season_derivations"],
        "value_mapping": "typed play family used to derive player and team counters; no duplicate canonical text column",
    },
    "fixed_drive_result": {
        "disposition": "MAPPED_DRIVE_OUTCOME_WITNESS",
        "canonical_targets": [],
        "target_lanes": ["weekly_game_context", "team_season_derivations"],
        "value_mapping": "drive terminal outcome retained as a witness for team/game derivations",
    },
}


def main() -> None:
    if not RAW.exists():
        raise FileNotFoundError(RAW)
    weekly = v26_plane("weekly")
    con = duckdb.connect()
    source = RAW.as_posix().replace("'", "''")
    canonical = str(weekly).replace("'", "''")
    fields = {}
    for field, spec in MAPS.items():
        values = con.execute(
            f'''SELECT coalesce("{field}", '<NULL>') AS value, count(*) AS rows
                FROM read_parquet('{source}') WHERE season=2025
                GROUP BY 1 ORDER BY 1'''
        ).fetchall()
        item = dict(spec)
        item["source_column"] = field
        item["source_rows_2025"] = sum(int(n) for _, n in values)
        item["source_values_2025"] = [{"value": str(v), "rows": int(n)} for v, n in values]
        if spec.get("canonical_targets"):
            targets = spec["canonical_targets"]
            expr = ", ".join(f'coalesce(sum("{c}"), 0) AS "{c}"' for c in targets)
            row = con.execute(f"SELECT {expr} FROM read_parquet('{canonical}') WHERE year=2025").fetchone()
            item["canonical_sums_2025"] = {c: float(v) for c, v in zip(targets, row)}
        fields[field] = item
    derived_checks = {}
    for field in ("fourth_down_converted", "fourth_down_failed", "third_down_converted", "third_down_failed"):
        derived_checks[field] = int(con.execute(
            f"SELECT coalesce(sum(CASE WHEN \"{field}\" = 1 THEN 1 ELSE 0 END), 0) FROM read_parquet('{source}') WHERE season=2025"
        ).fetchone()[0])
    derived_checks["series_result_punt_rows"] = int(con.execute(
        f"SELECT count(*) FROM read_parquet('{source}') WHERE season=2025 AND series_result='Punt'"
    ).fetchone()[0])
    derived_checks["three_out_series_groups"] = int(con.execute(
        f'''SELECT count(*) FROM (
              SELECT game_id, posteam, series
              FROM read_parquet('{source}')
              WHERE season=2025 AND series_result='Punt' AND posteam IS NOT NULL
              GROUP BY 1,2,3
              HAVING max(coalesce(drive_first_downs, 0)) = 0
            )'''
    ).fetchone()[0])
    # These are explicit checks, not canonical writes or inferred promotions.
    canonical_def = {}
    for field in ("def_fourth_down_faced", "def_fourth_down_allowed", "def_third_down_faced", "def_third_down_allowed", "fourth_down_stop", "pts_def_forced_punts"):
        canonical_def[field] = float(con.execute(
            f"SELECT coalesce(sum(\"{field}\"), 0) FROM read_parquet('{canonical}') WHERE year=2025 AND position='DEF'"
        ).fetchone()[0])
    weekly_sql = f'''
      WITH p AS (
        SELECT week AS wk,
          sum(CASE WHEN fourth_down_converted=1 THEN 1 ELSE 0 END) AS p_4conv,
          sum(CASE WHEN fourth_down_failed=1 THEN 1 ELSE 0 END) AS p_4fail,
          sum(CASE WHEN third_down_converted=1 THEN 1 ELSE 0 END) AS p_3conv,
          sum(CASE WHEN third_down_failed=1 THEN 1 ELSE 0 END) AS p_3fail
        FROM read_parquet('{source}') WHERE season=2025 GROUP BY 1
      ), c AS (
        SELECT week AS wk,
          sum(def_fourth_down_faced) AS c_4faced,
          sum(def_fourth_down_allowed) AS c_4allowed,
          sum(def_third_down_faced) AS c_3faced,
          sum(def_third_down_allowed) AS c_3allowed
        FROM read_parquet('{canonical}') WHERE year=2025 AND position='DEF' GROUP BY 1
      )
      SELECT coalesce(p.wk, c.wk) AS week,
        c_4allowed-p_4conv AS fourth_conversion_gap,
        c_4faced-(p_4conv+p_4fail) AS fourth_faced_gap,
        c_3allowed-p_3conv AS third_conversion_gap,
        c_3faced-(p_3conv+p_3fail) AS third_faced_gap
      FROM p FULL OUTER JOIN c ON p.wk=c.wk ORDER BY 1
    '''
    weekly_coverage = [
        {k: (int(v) if v is not None else None) for k, v in zip(("week", "fourth_conversion_gap", "fourth_faced_gap", "third_conversion_gap", "third_faced_gap"), row)}
        for row in con.execute(weekly_sql).fetchall()
    ]
    con.close()
    receipt = {
        "source": str(RAW),
        "canonical_weekly": str(weekly),
        "year": 2025,
        "scope": "read-only result-field adjudication; no canonical data writes",
        "fields": fields,
        "derived_2025_checks": {"pbp": derived_checks, "canonical_def_position_sums": canonical_def},
        "weekly_coverage_check_2025": weekly_coverage,
        "status": "MAPPED_AND_SPECIFIED",
        "remaining_work": "Only source-definition reconciliation/backfill remains; no field is an unclassified promotion candidate.",
    }
    OUT.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps({"status": receipt["status"], "fields": list(fields), "output": str(OUT)}, indent=2))


if __name__ == "__main__":
    main()
