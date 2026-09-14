"""Recompute weekly super-table scoring columns from raw atoms.

This is the post-PFR/PBP cleanup operator for the live super table.  It
refreshes calculator-owned weekly `pts_*`, `bonus_*`, `yds_allow_*`, and
`fpts_*` columns in `___ops.nfl_historical.nfl_player_stats_all`.

The later layers still need to be refreshed separately:
  * `scripts/recompute_l3_ppg_season_career_2026_05_04.py`
  * `scripts/recompute_l4_weekly_ranks_2026_05_03.py`
  * `scripts/recompute_l4_research_lamar_2026_05_03.py`
  * `scripts/rebuild_nfl_aggregate_tables.py`
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402
from multi_league.core.scoring_config import get_fpts_variant_columns  # noqa: E402
from multi_league.data_fetchers.fantasy_points_calculator import (  # noqa: E402
    COMPOSITE_POINTS_COLUMNS,
    PRECALC_COLUMNS,
    TEAM_MARGIN_COLUMNS,
)


SUPER_TABLE = "___ops.nfl_historical.nfl_player_stats_all"
STAGE_TABLE = "___ops.public.weekly_scoring_surface_stage_20260513"
BACKUP_TABLE = "___ops.public.weekly_scoring_surface_backup_20260513"
SENTINEL_COL = "weekly_scoring_surface_recomputed_at_20260513"

DEFAULT_RANGES = [
    (1920, 1939),
    (1940, 1959),
    (1960, 1979),
    (1980, 1998),
    (1999, 2009),
    (2010, 2019),
    (2020, 2025),
]

IDP_POSITIONS = ("LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "EDGE", "DB", "CB", "S", "SS", "FS", "SAF")


class LongFlyWriter(FlyWriter):
    """Fly writer with a longer timeout for large DuckDB UPDATE/CTAS jobs."""

    def execute(self, sql: str, database: str = "___leagues") -> list[dict]:
        timeout = int(os.environ.get("FLY_QUERY_TIMEOUT_SECONDS", "360"))
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self.url}/query-rw",
                    json={"sql": sql, "database": database},
                    headers=self._headers(),
                    timeout=timeout,
                )
            except self.RETRY_EXCEPTIONS:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self._retry_delay(attempt))
                    continue
                raise

            if resp.status_code in self.RETRY_STATUS and attempt < self.MAX_RETRIES - 1:
                time.sleep(self._retry_delay(attempt))
                continue

            if resp.status_code != 200:
                raise RuntimeError(f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'}")

            return resp.json()

        raise RuntimeError("Query: exhausted retries")


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def fetch_rows(reader: FlyReader, sql: str) -> list[dict]:
    return reader.query(sql, database="___ops")


def fetch_scalar(reader: FlyReader, sql: str, key: str = "n") -> int:
    rows = fetch_rows(reader, sql)
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        return int(first.get(key) or 0)
    return int(first[0] or 0)


def table_exists(reader: FlyReader, full_name: str) -> bool:
    schema_name, table_name = full_name.split(".")[-2:]
    return bool(
        fetch_scalar(
            reader,
            f"""
            SELECT COUNT(*) AS n
            FROM information_schema.tables
            WHERE table_catalog = '___ops'
              AND table_schema = {q_lit(schema_name)}
              AND table_name = {q_lit(table_name)}
            """,
        )
    )


def column_names(reader: FlyReader) -> set[str]:
    rows = fetch_rows(
        reader,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        """,
    )
    return {row["column_name"] for row in rows}


def ensure_columns(writer: FlyWriter, existing: set[str]) -> set[str]:
    for col in PRECALC_COLUMNS + COMPOSITE_POINTS_COLUMNS:
        if col not in existing:
            writer.execute(f"ALTER TABLE {SUPER_TABLE} ADD COLUMN {q_ident(col)} DOUBLE", database="___ops")
            existing.add(col)
    if SENTINEL_COL not in existing:
        writer.execute(f"ALTER TABLE {SUPER_TABLE} ADD COLUMN {q_ident(SENTINEL_COL)} TIMESTAMP", database="___ops")
        existing.add(SENTINEL_COL)
    return existing


def num(col: str, existing: set[str], alias: str = "b") -> str:
    if col not in existing:
        return "CAST(0 AS DOUBLE)"
    return f"COALESCE(TRY_CAST({alias}.{q_ident(col)} AS DOUBLE), CAST(0 AS DOUBLE))"


def pos_expr(alias: str = "b") -> str:
    return f"UPPER(TRIM(CAST(COALESCE({alias}.nfl_position, {alias}.position, '') AS VARCHAR)))"


def true_dst_expr(alias: str = "b") -> str:
    return (
        f"(COALESCE(CAST({alias}.player_week AS VARCHAR), '') LIKE 'DEF-%' "
        f"OR COALESCE(CAST({alias}.player AS VARCHAR), '') LIKE '% DST')"
    )


def as_def(expr: str, alias: str = "b") -> str:
    return f"CASE WHEN {true_dst_expr(alias)} THEN ({expr}) ELSE NULL END"


def as_non_dst_idp(expr: str, alias: str = "b") -> str:
    return f"CASE WHEN {true_dst_expr(alias)} THEN 0 ELSE ({expr}) END"


def as_non_def(expr: str, alias: str = "b") -> str:
    return f"CASE WHEN {true_dst_expr(alias)} THEN 0 ELSE ({expr}) END"


def bool_double(condition: str) -> str:
    return f"CASE WHEN {condition} THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END"


def formulas(existing: set[str]) -> dict[str, str]:
    def n(col: str) -> str:
        return num(col, existing)

    pos = pos_expr()
    is_te = f"{pos} = 'TE'"
    is_def = f"{pos} = 'DEF'"

    pass_int_n2 = f"{n('passing_interceptions')} * -2"
    pass_int_n1 = f"{n('passing_interceptions')} * -1"
    pass_yds = f"{n('passing_yards')} * 0.04"
    pass_td = n("passing_tds")
    rush_fum_lost = f"({n('rushing_fumbles_lost')} + {n('sack_fumbles_lost')})"
    rec_fum_lost = n("receiving_fumbles_lost")
    rush_yd = n("rushing_yards")
    rush_td = n("rushing_tds")
    rec_yd = n("receiving_yards")
    rec_td = n("receiving_tds")
    rec = n("receptions")
    rush_base = f"({rush_yd} * 0.1 + {rush_td} * 6 + ({rush_fum_lost}) * -2)"
    rec_base = f"({rec_yd} * 0.1 + {rec_td} * 6 + {rec_fum_lost} * -2)"
    two_pt_total = (
        f"({n('passing_2pt_conversions')} + {n('rushing_2pt_conversions')} + {n('receiving_2pt_conversions')})"
    )
    fum_lost_total = f"({n('rushing_fumbles_lost')} + {n('sack_fumbles_lost')} + {n('receiving_fumbles_lost')})"

    fg_0_19 = n("fg_made_0_19")
    fg_20_29 = n("fg_made_20_29")
    fg_30_39 = n("fg_made_30_39")
    fg_40_49 = n("fg_made_40_49")
    fg_50_59 = n("fg_made_50_59")
    fg_60_plus_legacy = f"({n('fg_made_60_')} + {n('fg_made_60_plus')})"
    fg_60_plus_canonical = n("fg_made_60_plus_canonical")
    fg_bucket_sum = f"({fg_0_19} + {fg_20_29} + {fg_30_39} + {fg_40_49} + {fg_50_59} + {fg_60_plus_canonical})"
    fg_bucket_points = f"({fg_0_19} * 3 + {fg_20_29} * 3 + {fg_30_39} * 3 + {fg_40_49} * 4 + {fg_50_59} * 5 + {fg_60_plus_canonical} * 6)"
    fg_made = n("fg_made")
    pat_made = n("pat_made")
    pat_missed = n("pat_missed")
    fg_missed = n("fg_missed")
    fg_actual_distance = f"CASE WHEN {n('fg_made_distance')} > 0 THEN {n('fg_made_distance')} ELSE {n('fg_yards')} END"
    fg_yards_from_buckets = f"({fg_0_19} * 17 + {fg_20_29} * 25 + {fg_30_39} * 35 + {fg_40_49} * 45 + {fg_50_59} * 54 + {fg_60_plus_legacy} * 62)"
    fg_yards_estimated = f"CASE WHEN ({fg_0_19} + {fg_20_29} + {fg_30_39} + {fg_40_49} + {fg_50_59} + {fg_60_plus_legacy}) > 0 THEN {fg_yards_from_buckets} ELSE {fg_made} * 35 END"
    fg_yards_for_pts = f"CASE WHEN {fg_actual_distance} > 0 THEN {fg_actual_distance} ELSE {fg_yards_estimated} END"

    comp_td_sum = f"({n('pts_def_int_ret_td')} + {n('pts_def_fum_ret_td')} + {n('pts_def_blk_kick_td')})"
    def_td = f"CASE WHEN {comp_td_sum} > 0 THEN {comp_td_sum} ELSE {n('def_tds')} END"
    def_fg_block = f"CASE WHEN {n('pts_def_fg_block')} != 0 THEN {n('pts_def_fg_block')} ELSE {n('fg_blocked')} END"
    def_punt_block = n("pts_def_punt_block")
    def_pat_block = n("pts_def_pat_block")
    def_block = f"({def_fg_block} + {def_punt_block} + {def_pat_block})"
    def_base = (
        f"({n('def_sacks')} * 1 + {n('def_interceptions')} * 2 "
        f"+ {n('fum_rec')} * 2 + ({def_td}) * 6 + {n('def_safeties')} * 2 + ({def_block}) * 2)"
    )
    pa_tiers = (
        f"({n('pts_allow_0')} * 10 + {n('pts_allow_1_6')} * 7 + {n('pts_allow_7_13')} * 4 "
        f"+ {n('pts_allow_14_20')} * 1 + {n('pts_allow_21_27')} * 0 + {n('pts_allow_28_34')} * -1 "
        f"+ {n('pts_allow_35_plus')} * -4)"
    )
    total_ya = n("total_yds_allowed")
    ya_300_349 = bool_double(f"{total_ya} BETWEEN 300 AND 349")
    ya_350_399 = bool_double(f"{total_ya} BETWEEN 350 AND 399")
    ya_400_449 = bool_double(f"{total_ya} BETWEEN 400 AND 449")
    ya_450_499 = bool_double(f"{total_ya} BETWEEN 450 AND 499")
    ya_500_549 = bool_double(f"{total_ya} BETWEEN 500 AND 549")
    ya_550_plus = bool_double(f"{total_ya} >= 550")
    ya_tiers = (
        f"({n('yds_allow_neg')} * 5 + {n('yds_allow_0_99')} * 4 + {n('yds_allow_100_199')} * 3 "
        f"+ {n('yds_allow_200_299')} * 2 + ({ya_300_349}) * 0 + ({ya_350_399}) * 0 "
        f"+ ({ya_400_449}) * -2 + ({ya_450_499}) * -2 + ({ya_500_549}) * -4 + ({ya_550_plus}) * -4)"
    )

    idp_tackles = f"({n('def_tackles_solo')} + {n('def_tackle_assists')} * 0.5)"
    idp_sacks = n("def_sacks")
    idp_int = n("def_interceptions")
    idp_ff = n("def_fumbles_forced")
    idp_fr = n("fum_rec")
    idp_tfl = n("def_tackles_for_loss")
    idp_pd = n("def_pass_defended")
    idp_qb_hits = n("def_qb_hits")
    idp_safety = n("def_safeties")
    idp_td = f"({n('def_tds')} + {n('fum_ret_td')})"
    fum_rec_yd_total = f"({n('fumble_recovery_yards_own')} + {n('fumble_recovery_yards_opp')})"
    idp_fum_rec_yd = f"CASE WHEN {fum_rec_yd_total} != 0 THEN {fum_rec_yd_total} ELSE {n('fum_rec_yds')} END"
    idp_tkl_combined = f"CASE WHEN {n('def_tackles_with_assist')} > 0 THEN {n('def_tackles_with_assist')} ELSE {n('def_tackles_solo')} + {n('def_tackle_assists')} END"

    out: dict[str, str] = {
        "pts_pass_4pt": f"{pass_yds} + {pass_td} * 4 + ({pass_int_n2})",
        "pts_pass_5pt": f"{pass_yds} + {pass_td} * 5 + ({pass_int_n2})",
        "pts_pass_6pt": f"{pass_yds} + {pass_td} * 6 + ({pass_int_n2})",
        "pts_pass_4pt_int1": f"{pass_yds} + {pass_td} * 4 + ({pass_int_n1})",
        "pts_pass_5pt_int1": f"{pass_yds} + {pass_td} * 5 + ({pass_int_n1})",
        "pts_pass_6pt_int1": f"{pass_yds} + {pass_td} * 6 + ({pass_int_n1})",
        "pts_rush": rush_base,
        "pts_rec_0ppr": rec_base,
        "pts_rec_half": f"({rec_base}) + {rec} * 0.5",
        "pts_rec_ppr": f"({rec_base}) + {rec}",
        "pts_rec_tep": f"CASE WHEN {is_te} THEN ({rec_base}) + {rec} * 1.5 ELSE ({rec_base}) + {rec} END",
        "pts_ret_yds": f"({n('kickoff_return_yards')} + {n('punt_return_yards')}) / 25.0",
        "pts_misc": as_non_def(f"({two_pt_total}) * 2 + {n('special_teams_tds')} * 6 + {n('fum_ret_td')} * 6"),
        "pts_pass_cmp": f"{n('completions')} * 0.25",
        "pts_rush_att": f"{n('carries')} * 0.1",
        "pts_first_downs": f"({n('rushing_first_downs')} + {n('receiving_first_downs')})",
        "pts_pass_fd": n("passing_first_downs"),
        "pts_sack_taken": f"{n('sacks_suffered')} * -1",
        "pts_pick6": n("pick6"),
        "pts_pass_yd_p04": pass_yds,
        "pts_pass_td_4": f"{pass_td} * 4",
        "pts_pass_td_6": f"{pass_td} * 6",
        "pts_pass_int_n2": pass_int_n2,
        "pts_pass_int_n1": pass_int_n1,
        "pts_rush_yd_p1": f"{rush_yd} * 0.1",
        "pts_rush_td_6": f"{rush_td} * 6",
        "pts_rec_yd_p1": f"{rec_yd} * 0.1",
        "pts_rec_td_6": f"{rec_td} * 6",
        "pts_rec_1": rec,
        "pts_rec_p5": f"{rec} * 0.5",
        "pts_rec_te_bonus_p5": f"CASE WHEN {is_te} THEN {rec} * 0.5 ELSE 0 END",
        "pts_pass_2pt_2": f"{n('passing_2pt_conversions')} * 2",
        "pts_rush_2pt_2": f"{n('rushing_2pt_conversions')} * 2",
        "pts_rec_2pt_2": f"{n('receiving_2pt_conversions')} * 2",
        "pts_fum_lost_n2": f"({fum_lost_total}) * -2",
        "pts_fum_lost_n1": f"({fum_lost_total}) * -1",
        "pts_pass_cmp_p25": f"{n('completions')} * 0.25",
        "pts_pass_cmp_p1": f"{n('completions')} * 0.1",
        "pts_pass_cmp_p5": f"{n('completions')} * 0.5",
        "pts_rush_att_p1": f"{n('carries')} * 0.1",
        "pts_rush_att_p2": f"{n('carries')} * 0.2",
        "pts_rush_att_p25": f"{n('carries')} * 0.25",
        "pts_pass_fd_p5": f"{n('passing_first_downs')} * 0.5",
        "pts_pass_fd_p25": f"{n('passing_first_downs')} * 0.25",
        "pts_rush_fd_p5": f"{n('rushing_first_downs')} * 0.5",
        "pts_rush_fd_p25": f"{n('rushing_first_downs')} * 0.25",
        "pts_rec_fd_p5": f"{n('receiving_first_downs')} * 0.5",
        "pts_rec_fd_p25": f"{n('receiving_first_downs')} * 0.25",
        "pts_pick6_n2": f"{n('pick6')} * -2",
        "pts_pick6_n1": f"{n('pick6')} * -1",
        "pts_sack_taken_n1": f"{n('sacks_suffered')} * -1",
        "pts_sack_taken_np5": f"{n('sacks_suffered')} * -0.5",
        "pts_st_td_6": as_non_def(f"{n('special_teams_tds')} * 6"),
        "pts_fum_ret_td_6": as_non_def(f"{n('fum_ret_td')} * 6"),
        "pts_kr_yd_p04": f"{n('kickoff_return_yards')} * 0.04",
        "pts_pr_yd_p04": f"{n('punt_return_yards')} * 0.04",
        "pts_pass_td_40plus_2": f"{n('passing_tds_40plus')} * 2",
        "pts_pass_td_50plus_1": n("passing_tds_50plus"),
        "pts_rush_td_40plus_2": f"{n('rushing_tds_40plus')} * 2",
        "pts_rush_td_50plus_1": n("rushing_tds_50plus"),
        "pts_rec_td_40plus_1": n("receiving_tds_40plus"),
        "pts_rec_td_50plus_1": n("receiving_tds_50plus"),
        "pts_pass_cmp_40plus_1": n("completions_40plus"),
        "pts_rush_40plus_1": n("rushing_40plus"),
        "pts_rec_40plus_1": n("receptions_40plus"),
        "bonus_pass_300yd": bool_double(f"{n('passing_yards')} >= 300"),
        "bonus_pass_400yd": bool_double(f"{n('passing_yards')} >= 400"),
        "bonus_rush_100yd": bool_double(f"{n('rushing_yards')} >= 100"),
        "bonus_rush_200yd": bool_double(f"{n('rushing_yards')} >= 200"),
        "bonus_rec_100yd": bool_double(f"{n('receiving_yards')} >= 100"),
        "bonus_rec_200yd": bool_double(f"{n('receiving_yards')} >= 200"),
        "bonus_rush_rec_100yd": bool_double(f"({n('rushing_yards')} + {n('receiving_yards')}) >= 100"),
        "bonus_rush_rec_200yd": bool_double(f"({n('rushing_yards')} + {n('receiving_yards')}) >= 200"),
        "bonus_pass_25cmp": bool_double(f"{n('completions')} >= 25"),
        "bonus_rush_20att": bool_double(f"{n('carries')} >= 20"),
        "bonus_rec_10rec": bool_double(f"{n('receptions')} >= 10"),
        "pts_k_std": f"CASE WHEN {fg_bucket_sum} > 0 THEN {fg_bucket_points} ELSE {fg_made} * 3 END + {pat_made} - {fg_missed}",
        "pts_k_yds": f"({fg_yards_for_pts}) * 0.1 + {pat_made}",
        "pts_k_flat": f"{fg_made} * 3 + {pat_made}",
        "pts_k_fgm_0_19_3": f"{fg_0_19} * 3",
        "pts_k_fgm_20_29_3": f"{fg_20_29} * 3",
        "pts_k_fgm_30_39_3": f"{fg_30_39} * 3",
        "pts_k_fgm_40_49_4": f"{fg_40_49} * 4",
        "pts_k_fgm_40_49_3": f"{fg_40_49} * 3",
        "pts_k_fgm_50_59_5": f"{fg_50_59} * 5",
        "pts_k_fgm_60p_6": f"{fg_60_plus_canonical} * 6",
        "pts_k_fgm_60p_5": f"{fg_60_plus_canonical} * 5",
        "pts_k_xpm_1": pat_made,
        "pts_k_xpmiss_n1": f"{pat_missed} * -1",
        "pts_k_fgmiss_n1": f"{fg_missed} * -1",
        "pts_k_fgm_yd_p1": f"{n('fg_yards_canonical')} * 0.1",
        "pts_k_fgm_yd_over30_p1": f"{n('fg_yards_over_30_canonical')} * 0.1",
        "pts_def_sack": as_def(n("def_sacks")),
        "pts_def_int": as_def(n("def_interceptions")),
        "pts_def_ff": as_def(n("def_fumbles_forced")),
        "pts_def_fr": as_def(n("fum_rec")),
        "pts_def_td": as_def(def_td),
        "pts_def_safety": as_def(n("def_safeties")),
        "pts_def_fg_block": as_def(def_fg_block),
        "pts_def_punt_block": as_def(def_punt_block),
        "pts_def_pat_block": as_def(def_pat_block),
        "pts_def_block": as_def(def_block),
        "pts_def_tfl": as_def(n("def_tackles_for_loss")),
        "pts_def_3out": as_def(n("three_out")),
        "pts_def_4stop": as_def(n("fourth_down_stop")),
        "pts_def_ret_yd": as_def("b.team_ret_yds"),
        "pts_def_ret_td": as_def("b.team_ret_tds"),
        "pts_def_std": as_def(f"({def_base}) + ({pa_tiers})"),
        "pts_def_ya": as_def(f"({def_base}) + ({pa_tiers}) + ({ya_tiers})"),
        "yds_allow_300_349": ya_300_349,
        "yds_allow_350_399": ya_350_399,
        "yds_allow_400_449": ya_400_449,
        "yds_allow_450_499": ya_450_499,
        "yds_allow_500_549": ya_500_549,
        "yds_allow_550_plus": ya_550_plus,
        "pts_idp_tackle_solo": as_non_dst_idp(n("def_tackles_solo")),
        "pts_idp_tackle_assist": as_non_dst_idp(n("def_tackle_assists")),
        "pts_idp_sack": as_non_dst_idp(n("def_sacks")),
        "pts_idp_int": as_non_dst_idp(n("def_interceptions")),
        "pts_idp_ff": as_non_dst_idp(n("def_fumbles_forced")),
        "pts_idp_fr": as_non_dst_idp(n("fum_rec")),
        "pts_idp_pd": as_non_dst_idp(n("def_pass_defended")),
        "pts_idp_qb_hit": as_non_dst_idp(n("def_qb_hits")),
        "pts_idp_tfl": as_non_dst_idp(n("def_tackles_for_loss")),
        "pts_idp_safety": as_non_dst_idp(n("def_safeties")),
        "pts_idp_td": as_non_dst_idp(idp_td),
        "pts_idp_blk_kick": as_non_dst_idp(n("def_blk_kick")),
        "pts_idp_int_ret_yd": as_non_dst_idp(n("def_interception_yards")),
        "pts_idp_fum_rec_yd": as_non_dst_idp(idp_fum_rec_yd),
        "pts_idp_tkl_combined": as_non_dst_idp(idp_tkl_combined),
        "pts_idp_blk_kick_td": as_non_dst_idp(n("def_blk_kick_td")),
        "pts_idp_fum_ret_td": as_non_dst_idp(n("fum_ret_td")),
        "pts_idp_xpr": as_non_dst_idp(n("def_xpr")),
        "pts_idp_pass_def_3p": as_non_dst_idp(f"LEAST({n('def_pass_defended')}, CAST(3 AS DOUBLE))"),
        "pts_idp_std": as_non_dst_idp(
            f"({idp_tackles}) * 1.0 + {idp_sacks} * 2.0 + {idp_int} * 3.0 + {idp_ff} * 2.0 "
            f"+ {idp_fr} * 2.0 + {idp_tfl} * 1.0 + {idp_pd} * 1.0 + {idp_qb_hits} * 0.5 "
            f"+ {idp_safety} * 2.0 + ({idp_td}) * 6.0"
        ),
        "pts_idp_premium": as_non_dst_idp(
            f"({idp_tackles}) * 1.5 + {idp_sacks} * 3.0 + {idp_int} * 4.0 + {idp_ff} * 3.0 "
            f"+ {idp_fr} * 3.0 + {idp_tfl} * 1.5 + {idp_pd} * 1.0 + {idp_qb_hits} * 1.0 "
            f"+ {idp_safety} * 3.0 + ({idp_td}) * 6.0"
        ),
        "pts_idp_tackle_heavy": as_non_dst_idp(
            f"({idp_tackles}) * 2.0 + {idp_sacks} * 2.0 + {idp_int} * 3.0 + {idp_ff} * 2.0 "
            f"+ {idp_fr} * 2.0 + {idp_tfl} * 2.0 + {idp_pd} * 1.0 + {idp_qb_hits} * 0.5 "
            f"+ {idp_safety} * 2.0 + ({idp_td}) * 6.0"
        ),
        "pts_idp_big_play": as_non_dst_idp(
            f"({idp_tackles}) * 0.5 + {idp_sacks} * 4.0 + {idp_int} * 6.0 + {idp_ff} * 4.0 "
            f"+ {idp_fr} * 4.0 + {idp_tfl} * 2.0 + {idp_pd} * 1.5 + {idp_qb_hits} * 1.0 "
            f"+ {idp_safety} * 4.0 + ({idp_td}) * 6.0"
        ),
    }
    for team_col in TEAM_MARGIN_COLUMNS:
        out[team_col] = as_def(n(team_col))
    return out


def fpts_formula(col: str, alias: str = "c") -> str:
    parts = []
    if "_4pt_" in col:
        parts.append("pts_pass_4pt")
    elif "_5pt_" in col:
        parts.append("pts_pass_5pt")
    elif "_6pt_" in col:
        parts.append("pts_pass_6pt")
    else:
        raise ValueError(col)

    parts.extend(["pts_rush"])
    if "_0ppr" in col:
        parts.append("pts_rec_0ppr")
    elif "_half" in col:
        parts.append("pts_rec_half")
    elif "_ppr" in col:
        parts.append("pts_rec_ppr")
    elif "_tep" in col:
        parts.append("pts_rec_tep")
    else:
        raise ValueError(col)
    parts.extend(["pts_misc", "pts_k_std", "pts_def_std"])
    if col.endswith("_ret"):
        parts.append("pts_ret_yds")
    return " + ".join(f"COALESCE({alias}.{q_ident(part)}, 0)" for part in parts)


def build_stage_sql(start_year: int, end_year: int, existing: set[str]) -> tuple[str, list[str]]:
    all_formulas = formulas(existing)
    precalc_cols = [col for col in PRECALC_COLUMNS if col in all_formulas]
    fpts_cols = [col for col in get_fpts_variant_columns() if col in COMPOSITE_POINTS_COLUMNS]
    source_cols = sorted(
        {
            "player_week",
            "rowid",
            "year",
            "week",
            "nfl_team",
            "position",
            "nfl_position",
            *existing.intersection(set(PRECALC_COLUMNS)),
            *[
                col
                for col in existing
                if col
                in {
                    "passing_yards",
                    "passing_tds",
                    "passing_interceptions",
                    "rushing_yards",
                    "rushing_tds",
                    "rushing_fumbles_lost",
                    "sack_fumbles_lost",
                    "receiving_yards",
                    "receiving_tds",
                    "receiving_fumbles_lost",
                    "receptions",
                    "passing_2pt_conversions",
                    "rushing_2pt_conversions",
                    "receiving_2pt_conversions",
                    "special_teams_tds",
                    "fum_ret_td",
                    "kickoff_return_yards",
                    "punt_return_yards",
                    "completions",
                    "passing_first_downs",
                    "rushing_first_downs",
                    "receiving_first_downs",
                    "carries",
                    "pick6",
                    "sacks_suffered",
                    "passing_tds_40plus",
                    "passing_tds_50plus",
                    "rushing_tds_40plus",
                    "rushing_tds_50plus",
                    "receiving_tds_40plus",
                    "receiving_tds_50plus",
                    "completions_40plus",
                    "rushing_40plus",
                    "receptions_40plus",
                    "fg_made_0_19",
                    "fg_made_20_29",
                    "fg_made_30_39",
                    "fg_made_40_49",
                    "fg_made_50_59",
                    "fg_made_60_",
                    "fg_made_60_plus",
                    "fg_made_60_plus_canonical",
                    "fg_made",
                    "pat_made",
                    "pat_missed",
                    "fg_missed",
                    "fg_made_distance",
                    "fg_yards",
                    "fg_yards_canonical",
                    "fg_yards_over_30_canonical",
                    "def_sacks",
                    "def_interceptions",
                    "def_fumbles_forced",
                    "fum_rec",
                    "def_tds",
                    "def_safeties",
                    "def_tackles_for_loss",
                    "fg_blocked",
                    "three_out",
                    "fourth_down_stop",
                    "total_yds_allowed",
                    "pts_allow_0",
                    "pts_allow_1_6",
                    "pts_allow_7_13",
                    "pts_allow_14_20",
                    "pts_allow_21_27",
                    "pts_allow_28_34",
                    "pts_allow_35_plus",
                    "yds_allow_neg",
                    "yds_allow_0_99",
                    "yds_allow_100_199",
                    "yds_allow_200_299",
                    "def_tackles_solo",
                    "def_tackle_assists",
                    "def_tackles_with_assist",
                    "def_pass_defended",
                    "def_qb_hits",
                    "def_blk_kick",
                    "def_interception_yards",
                    "fumble_recovery_yards_own",
                    "fumble_recovery_yards_opp",
                    "fum_rec_yds",
                    "def_blk_kick_td",
                    "def_xpr",
                    *TEAM_MARGIN_COLUMNS,
                }
            ],
        }
    )
    select_source = ",\n          ".join(f"s.{q_ident(col)}" if col != "rowid" else "s.rowid" for col in source_cols)
    calc_cols = ",\n          ".join(f"({all_formulas[col]}) AS {q_ident(col)}" for col in precalc_cols)
    fpts_select = ",\n          ".join(f"({fpts_formula(col)}) AS {q_ident(col)}" for col in fpts_cols)
    stage_cols = precalc_cols + fpts_cols
    sql = f"""
    CREATE OR REPLACE TABLE {STAGE_TABLE} AS
    WITH team_returns AS (
      SELECT
        nfl_team,
        CAST(year AS INTEGER) AS year_int,
        CAST(week AS INTEGER) AS week_int,
        SUM(COALESCE(TRY_CAST(kickoff_return_yards AS DOUBLE), 0) + COALESCE(TRY_CAST(punt_return_yards AS DOUBLE), 0)) AS team_ret_yds,
        SUM(COALESCE(TRY_CAST(special_teams_tds AS DOUBLE), 0)) AS team_ret_tds
      FROM {SUPER_TABLE}
      WHERE year BETWEEN {int(start_year)} AND {int(end_year)}
        AND NOT (COALESCE(CAST(player_week AS VARCHAR), '') LIKE 'DEF-%'
          OR COALESCE(CAST(player AS VARCHAR), '') LIKE '% DST')
      GROUP BY nfl_team, CAST(year AS INTEGER), CAST(week AS INTEGER)
    ),
    b AS (
      SELECT
          {select_source},
          COALESCE(tr.team_ret_yds, 0) AS team_ret_yds,
          COALESCE(tr.team_ret_tds, 0) AS team_ret_tds
      FROM {SUPER_TABLE} AS s
      LEFT JOIN team_returns AS tr
        ON tr.nfl_team = s.nfl_team
       AND tr.year_int = CAST(s.year AS INTEGER)
       AND tr.week_int = CAST(s.week AS INTEGER)
      WHERE s.year BETWEEN {int(start_year)} AND {int(end_year)}
    ),
    c AS (
      SELECT
          b.rowid,
          {calc_cols}
      FROM b
    )
    SELECT
          c.rowid,
          {", ".join(f"c.{q_ident(col)}" for col in precalc_cols)},
          {fpts_select}
    FROM c
    """
    return sql, stage_cols


def cmd_backup(reader: FlyReader, writer: FlyWriter, existing: set[str]) -> None:
    print(f"=== Backup weekly scoring surface -> {BACKUP_TABLE} ===")
    if table_exists(reader, BACKUP_TABLE):
        rows = fetch_scalar(reader, f"SELECT COUNT(*) AS n FROM {BACKUP_TABLE}")
        print(f"[backup] exists; rows={rows:,}. Leaving it unchanged.")
        return

    cols = [
        col
        for col in [
            "player_week",
            "NFL_player_id",
            "player",
            "position",
            "nfl_position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
            *PRECALC_COLUMNS,
            *COMPOSITE_POINTS_COLUMNS,
        ]
        if col in existing
    ]
    select_cols = ",\n          ".join(f"s.{q_ident(col)}" for col in cols)
    writer.execute(
        f"""
        CREATE TABLE {BACKUP_TABLE} AS
        SELECT
          s.rowid AS original_rowid,
          {select_cols}
        FROM {SUPER_TABLE} AS s
        """,
        database="___ops",
    )
    rows = fetch_scalar(reader, f"SELECT COUNT(*) AS n FROM {BACKUP_TABLE}")
    print(f"[backup] created rows={rows:,}")


def cmd_recompute(writer: FlyWriter, existing: set[str], ranges: list[tuple[int, int]]) -> None:
    print("=== Recompute weekly scoring surface ===")
    all_formulas = formulas(existing)
    ret_cols = {"pts_def_ret_yd", "pts_def_ret_td"}
    precalc_cols = [col for col in PRECALC_COLUMNS if col in all_formulas and col not in ret_cols]
    fpts_cols = [col for col in get_fpts_variant_columns() if col in COMPOSITE_POINTS_COLUMNS]
    for start_year, end_year in ranges:
        print(f"[points] years {start_year}-{end_year}, cols={len(precalc_cols)}")
        for start in range(0, len(precalc_cols), 14):
            chunk = precalc_cols[start : start + 14]
            set_clause = ",\n              ".join(f"{q_ident(col)} = ({all_formulas[col]})" for col in chunk)
            writer.execute(
                f"""
                UPDATE {SUPER_TABLE} AS b
                SET
                  {set_clause},
                  {q_ident(SENTINEL_COL)} = CURRENT_TIMESTAMP
                WHERE b.year BETWEEN {int(start_year)} AND {int(end_year)}
                """,
                database="___ops",
            )

        print(f"[def returns] years {start_year}-{end_year}")
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE} AS b
            SET
              {q_ident('pts_def_ret_yd')} = NULL,
              {q_ident('pts_def_ret_td')} = NULL
            WHERE b.year BETWEEN {int(start_year)} AND {int(end_year)}
              AND NOT {true_dst_expr('b')}
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE} AS b
            SET
              {q_ident('pts_def_ret_yd')} = 0,
              {q_ident('pts_def_ret_td')} = 0
            WHERE b.year BETWEEN {int(start_year)} AND {int(end_year)}
              AND {true_dst_expr('b')}
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE} AS b
            SET
              {q_ident('pts_def_ret_yd')} = tr.team_ret_yds,
              {q_ident('pts_def_ret_td')} = tr.team_ret_tds
            FROM (
              SELECT
                nfl_team,
                CAST(year AS INTEGER) AS year_int,
                CAST(week AS INTEGER) AS week_int,
                SUM(COALESCE(TRY_CAST(kickoff_return_yards AS DOUBLE), 0)
                  + COALESCE(TRY_CAST(punt_return_yards AS DOUBLE), 0)) AS team_ret_yds,
                SUM(COALESCE(TRY_CAST(special_teams_tds AS DOUBLE), 0)) AS team_ret_tds
              FROM {SUPER_TABLE}
              WHERE year BETWEEN {int(start_year)} AND {int(end_year)}
                AND NOT (COALESCE(CAST(player_week AS VARCHAR), '') LIKE 'DEF-%'
                  OR COALESCE(CAST(player AS VARCHAR), '') LIKE '% DST')
              GROUP BY nfl_team, CAST(year AS INTEGER), CAST(week AS INTEGER)
            ) AS tr
            WHERE b.year BETWEEN {int(start_year)} AND {int(end_year)}
              AND {true_dst_expr('b')}
              AND tr.nfl_team = b.nfl_team
              AND tr.year_int = CAST(b.year AS INTEGER)
              AND tr.week_int = CAST(b.week AS INTEGER)
            """,
            database="___ops",
        )

        print(f"[fpts] years {start_year}-{end_year}, cols={len(fpts_cols)}")
        for start in range(0, len(fpts_cols), 18):
            chunk = fpts_cols[start : start + 18]
            set_clause = ",\n              ".join(f"{q_ident(col)} = ({fpts_formula(col, alias='b')})" for col in chunk)
            writer.execute(
                f"""
                UPDATE {SUPER_TABLE} AS b
                SET
                  {set_clause},
                  {q_ident(SENTINEL_COL)} = CURRENT_TIMESTAMP
                WHERE b.year BETWEEN {int(start_year)} AND {int(end_year)}
                """,
                database="___ops",
            )
        print(f"[update] years {start_year}-{end_year} complete")


def cmd_verify(reader: FlyReader, existing: set[str], ranges: list[tuple[int, int]]) -> bool:
    print("=== Verify weekly scoring surface ===")
    total_rows = fetch_scalar(reader, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE}")
    sentinel_rows = fetch_scalar(
        reader,
        f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE {q_ident(SENTINEL_COL)} IS NOT NULL",
    )
    print(f"[verify] sentinel_rows={sentinel_rows:,} / total_rows={total_rows:,}")

    all_formulas = formulas(existing)
    formula_by_col = {col: expr for col, expr in all_formulas.items() if col in existing}
    for col in get_fpts_variant_columns():
        if col in existing:
            formula_by_col[col] = fpts_formula(col, alias="b")
    verify_cols = [col for col in [*PRECALC_COLUMNS, *COMPOSITE_POINTS_COLUMNS] if col in formula_by_col]

    drift_totals: dict[str, int] = {}
    max_delta: dict[str, float] = {}
    for start_year, end_year in ranges:
        print(f"[verify-direct] years {start_year}-{end_year}")

        def build_verify_sql(
            chunk: list[str],
            start_year: int = start_year,
            end_year: int = end_year,
        ) -> str:
            exprs = []
            needs_team_returns = any("team_ret_" in formula_by_col[col] for col in chunk)
            for col in chunk:
                formula_expr = formula_by_col[col]
                exprs.append(
                    f"""
                    SUM(CASE
                        WHEN (b.{q_ident(col)} IS NULL AND ({formula_expr}) IS NULL)
                          OR (b.{q_ident(col)} IS NOT NULL AND ({formula_expr}) IS NOT NULL
                              AND ABS(CAST(b.{q_ident(col)} AS DOUBLE) - CAST(({formula_expr}) AS DOUBLE)) <= 0.000001)
                        THEN 0 ELSE 1 END) AS {q_ident(col)}
                    """.strip()
                )
                exprs.append(
                    f"""
                    MAX(
                        CASE
                          WHEN b.{q_ident(col)} IS NULL OR ({formula_expr}) IS NULL THEN NULL
                          ELSE ABS(CAST(b.{q_ident(col)} AS DOUBLE) - CAST(({formula_expr}) AS DOUBLE))
                        END
                    ) AS {q_ident(col + '__max_delta')}
                    """.strip()
                )
            if needs_team_returns:
                return f"""
                    WITH team_returns AS (
                      SELECT
                        nfl_team,
                        CAST(year AS INTEGER) AS year_int,
                        CAST(week AS INTEGER) AS week_int,
                        SUM(COALESCE(TRY_CAST(kickoff_return_yards AS DOUBLE), 0) + COALESCE(TRY_CAST(punt_return_yards AS DOUBLE), 0)) AS team_ret_yds,
                        SUM(COALESCE(TRY_CAST(special_teams_tds AS DOUBLE), 0)) AS team_ret_tds
                      FROM {SUPER_TABLE}
                      WHERE year BETWEEN {int(start_year)} AND {int(end_year)}
                        AND NOT (COALESCE(CAST(player_week AS VARCHAR), '') LIKE 'DEF-%'
                          OR COALESCE(CAST(player AS VARCHAR), '') LIKE '% DST')
                      GROUP BY nfl_team, CAST(year AS INTEGER), CAST(week AS INTEGER)
                    ),
                    b AS (
                      SELECT
                        s.*,
                        COALESCE(tr.team_ret_yds, 0) AS team_ret_yds,
                        COALESCE(tr.team_ret_tds, 0) AS team_ret_tds
                      FROM {SUPER_TABLE} AS s
                      LEFT JOIN team_returns AS tr
                        ON tr.nfl_team = s.nfl_team
                       AND tr.year_int = CAST(s.year AS INTEGER)
                       AND tr.week_int = CAST(s.week AS INTEGER)
                      WHERE s.year BETWEEN {int(start_year)} AND {int(end_year)}
                    )
                    SELECT
                      {", ".join(exprs)}
                    FROM b
                    """
            return f"""
                    SELECT
                      {", ".join(exprs)}
                    FROM {SUPER_TABLE} AS b
                    WHERE b.year BETWEEN {int(start_year)} AND {int(end_year)}
                    """

        def record_rows(chunk: list[str], rows: list[dict]) -> None:
            if rows:
                for col in chunk:
                    drift_totals[col] = drift_totals.get(col, 0) + int(rows[0].get(col) or 0)
                    delta_value = rows[0].get(col + "__max_delta")
                    if delta_value is not None:
                        max_delta[col] = max(max_delta.get(col, 0.0), float(delta_value))

        for start in range(0, len(verify_cols), 4):
            chunk = verify_cols[start : start + 4]
            try:
                rows = fetch_rows(reader, build_verify_sql(chunk))
                record_rows(chunk, rows)
            except RuntimeError as exc:
                if len(chunk) == 1:
                    raise
                print(f"  [verify-cols-fallback] chunk failed, retrying individually: {exc}", flush=True)
                for col in chunk:
                    rows = fetch_rows(reader, build_verify_sql([col]))
                    record_rows([col], rows)

    bad = {col: n for col, n in drift_totals.items() if n}
    if bad:
        print("[verify] drift FAIL")
        for col, n in sorted(bad.items()):
            print(f"  {col}: {n:,} (max_delta={max_delta.get(col, 0.0):.6g})")
    else:
        print("[verify] drift PASS: weekly scoring columns match the recompute stage")
    sentinel_ok = int(sentinel_rows) == int(total_rows)
    print(f"[verify] sentinel {'PASS' if sentinel_ok else 'FAIL'}")
    return not bad and sentinel_ok


def parse_ranges(value: str | None) -> list[tuple[int, int]]:
    if not value:
        return DEFAULT_RANGES
    out: list[tuple[int, int]] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            out.append((int(start), int(end)))
        else:
            year = int(part)
            out.append((year, year))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", action="store_true")
    parser.add_argument("--add-columns", action="store_true")
    parser.add_argument("--recompute", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--ranges", help="Comma-separated years/ranges, e.g. 1999-2025 or 1920-1977,1978-1998.")
    args = parser.parse_args()

    if not any((args.backup, args.add_columns, args.recompute, args.verify, args.all)):
        parser.error("choose at least one action or use --all")

    load_env()
    reader = FlyReader()
    writer = LongFlyWriter() if (args.all or args.add_columns or args.backup or args.recompute) else None
    existing = column_names(reader)
    ranges = parse_ranges(args.ranges)

    if args.all or args.add_columns:
        assert writer is not None
        existing = ensure_columns(writer, existing)
    if args.all or args.backup:
        assert writer is not None
        cmd_backup(reader, writer, existing)
    if args.all or args.recompute:
        assert writer is not None
        cmd_recompute(writer, existing, ranges)
    if args.all or args.verify:
        ok = cmd_verify(reader, existing, ranges)
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
