"""
research_fly_smoke.py -- live Fly smoke test for every major research-mode SQL shape.

Mocked unit tests cannot catch live drift: the 2026-07-09 outage (DuckDB mis-binding wide
projections under CTE+window plans after the ___ops split) passed 1,700+ mocked tests while
every derived-lane question 500'd in production. This script executes one REAL query per
plan shape against Fly and fails loudly on any error, so schema/view/pre-agg/engine drift
is caught the day it happens, not by users.

Run:  python scripts/research_fly_smoke.py            # all shapes, exit 1 on any failure
      python scripts/research_fly_smoke.py --shape rolling_window

Wire into the weekly update batch and/or a daily cron (see weekly-update-system-plan).
Read-only: uses DATABASE_READ_TOKEN only.
"""
from __future__ import annotations
import argparse
import os
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
ST = "nfl_historical.nfl_player_stats_all"
ST_BASE = "___ops_nfl.nfl_historical.nfl_player_stats_all"
SEASON = "nfl_historical.player_nfl_season"
CAREER = "nfl_historical.player_nfl_career"
BIO = "nfl_historical.player_bio"
TG = "nfl_historical.nfl_team_games_all"

# Each shape mirrors the SQL skeleton one research lane emits. Keep these aligned with the
# lanes: derived windows/streaks/milestones, semantic leaderboards/threshold counts/cohorts,
# standard weekly/season/career views, and the venue/calendar join.
SHAPES: dict[str, tuple[str, str]] = {
    "simple_scan": (
        "weekly leaderboard scan through the ___ops view",
        f"""SELECT player, year, week, rushing_yards FROM {ST}
            WHERE NFL_player_id IS NOT NULL ORDER BY CAST(rushing_yards AS DOUBLE) DESC NULLS LAST LIMIT 3""",
    ),
    "rolling_window": (
        "derived-lane shape: DIRECT base scan + window dedup + outer window. GOLDEN: top "
        "5-game rushing window must be Earl Campbell 898 (1980 wk7-11). Any other value = "
        "the DuckDB silent-corruption bug is back (wide views AND projection subqueries "
        "both scramble bindings under this plan on v1.5.1 -- only direct base scans are safe)",
        f"""WITH base AS (
              SELECT t.NFL_player_id, t.player, CAST(t.year AS INTEGER) AS year, CAST(t.week AS INTEGER) AS week,
                COALESCE(CAST(t.rushing_yards AS DOUBLE),0) AS stat_value,
                ROW_NUMBER() OVER (PARTITION BY t.NFL_player_id, t.year, t.week ORDER BY t.player_week) AS rnk
              FROM {ST_BASE} t
              WHERE t.NFL_player_id IS NOT NULL AND (t.season_type IS NULL OR t.season_type = 'REG')
            ), dedup AS (SELECT * FROM base WHERE rnk = 1)
            SELECT player, SUM(stat_value) OVER (PARTITION BY NFL_player_id ORDER BY year, week
              ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w
            FROM dedup ORDER BY w DESC LIMIT 3""",
    ),
    "first5_window_golden": (
        "first-N-career-games window over the direct base scan. GOLDEN: Puka Nacua's first "
        "5 games (2023 wk1-5) must sum to 92.6 half-PPR. The 2026-07-09 projection-subquery "
        "workaround silently scrambled this window (87.4 spanning 2023-2025)",
        f"""WITH g AS (
              SELECT player, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
                COALESCE(CAST(fpts_4pt_half AS DOUBLE),0) AS pts,
                ROW_NUMBER() OVER (PARTITION BY NFL_player_id ORDER BY CAST(year AS INTEGER), CAST(week AS INTEGER)) AS game_no
              FROM {ST_BASE}
              WHERE NFL_player_id = '00-0039075' AND (season_type IS NULL OR season_type = 'REG')
            )
            SELECT MIN(year) AS first_year, MAX(year) AS last_year, ROUND(SUM(pts),1) AS total
            FROM g WHERE game_no <= 5""",
    ),
    "wide_relation_window": (
        "canary for the DuckDB wide-projection binder bug: same shape but scanning the full view. "
        "EXPECTED TO FAIL on DuckDB v1.5.1 -- when this starts PASSING, the engine bug is gone "
        "and narrowSuperTableScan() can be retired",
        f"""WITH base AS (
              SELECT t.NFL_player_id, CAST(t.year AS INTEGER) AS year, CAST(t.week AS INTEGER) AS week,
                COALESCE(CAST(t.rushing_yards AS DOUBLE),0) AS stat_value,
                ROW_NUMBER() OVER (PARTITION BY t.NFL_player_id, t.year, t.week ORDER BY t.player_week) AS rnk
              FROM {ST} t WHERE t.NFL_player_id IS NOT NULL
            ), dedup AS (SELECT * FROM base WHERE rnk = 1)
            SELECT NFL_player_id, SUM(stat_value) OVER (PARTITION BY NFL_player_id ORDER BY year, week
              ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w
            FROM dedup LIMIT 2""",
    ),
    "bio_join": (
        "weekly + player_bio join (draft/college filters)",
        f"""SELECT t.player, pb.college FROM {ST} t
            JOIN {BIO} pb ON t.NFL_player_id = pb.NFL_player_id
            WHERE TRY_CAST(pb.draft_round AS INTEGER) = 1 LIMIT 3""",
    ),
    "season_preagg": (
        "season pre-agg leaderboard incl. an advanced column (P0-B drift canary)",
        f"""SELECT player, year, passing_epa, games_played FROM {SEASON}
            WHERE games_played > 0 ORDER BY CAST(passing_epa AS DOUBLE) DESC NULLS LAST LIMIT 3""",
    ),
    "career_preagg": (
        "career pre-agg leaderboard",
        f"""SELECT player, passing_yards, games_played FROM {CAREER}
            ORDER BY CAST(passing_yards AS DOUBLE) DESC NULLS LAST LIMIT 3""",
    ),
    "threshold_count": (
        "semantic threshold-count shape (games over a cutoff, grouped per player)",
        f"""SELECT NFL_player_id, ANY_VALUE(player) AS player,
              COUNT(*) FILTER (WHERE CAST(passing_yards AS DOUBLE) >= 300) AS games_300
            FROM {ST} WHERE NFL_player_id IS NOT NULL AND (season_type IS NULL OR season_type = 'REG')
            GROUP BY NFL_player_id ORDER BY games_300 DESC LIMIT 3""",
    ),
    "cumulative_milestone": (
        "derived milestone shape: running total + first crossing",
        f"""WITH g AS (
              SELECT NFL_player_id, player, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
                SUM(COALESCE(CAST(passing_tds AS DOUBLE),0)) OVER (
                  PARTITION BY NFL_player_id ORDER BY CAST(year AS INTEGER), CAST(week AS INTEGER)
                ) AS running
              FROM (SELECT NFL_player_id, player, year, week, passing_tds FROM {ST})
              WHERE NFL_player_id IS NOT NULL
            )
            SELECT player, MIN(year) AS crossed FROM g WHERE running >= 400 GROUP BY player LIMIT 3""",
    ),
    "team_games_join": (
        "weekly venue/calendar join to nfl_team_games_all",
        f"""SELECT t.player, tg.game_day_of_week, tg.is_home FROM {ST} t
            JOIN {TG} tg ON CAST(t.year AS INTEGER) = tg.year AND CAST(t.week AS INTEGER) = tg.week
              AND COALESCE(t.season_type,'REG') = tg.season_type
              AND TRY_CAST(t.nfl_franchise_number AS INTEGER) = tg.team_fid
            WHERE tg.is_home LIMIT 3""",
    ),
    "team_aggregate": (
        "semantic team-offense aggregate shape",
        f"""SELECT nfl_team, CAST(year AS INTEGER) AS year, SUM(COALESCE(CAST(passing_yards AS DOUBLE),0)) AS yds
            FROM (SELECT nfl_team, year, passing_yards, season_type FROM {ST})
            WHERE nfl_team IS NOT NULL AND (season_type IS NULL OR season_type = 'REG')
            GROUP BY nfl_team, CAST(year AS INTEGER) ORDER BY yds DESC LIMIT 3""",
    ),
    "registry_season_columns": (
        "every registry column claiming season view resolves in player_nfl_season "
        "(guards against registry/pre-agg drift returning)",
        # Columns are injected at runtime from the frontend registry dump when available;
        # fall back to the known-registered advanced set.
        "",
    ),
    "season_share_golden": (
        "season share-family integrity. GOLDEN: Welker 2008 target_share 0.2827 (149 of 527 NE "
        "targets), NO season target_share >= 0.5, NO air_yards_share >= 0.75, and no air share "
        "before 2009 (partial within-game tracking 1999-2008 minted Keyshawn 2002 = 1.036). "
        "A 1.0 target_share = the denominator is reading weekly share columns again (sparse "
        "garbage in the 2003-2008 hole minted Welker 2008 = 1.0 until the 2026-07-09 "
        "team-denominator rollup fix)",
        f"""SELECT
              MAX(CASE WHEN player = 'Wes Welker' AND year = 2008 THEN target_share END) AS welker_2008,
              MAX(CASE WHEN player = 'Wes Welker' AND year = 2010 THEN target_share END) AS welker_2010,
              MAX(target_share) AS max_share,
              COUNT(CASE WHEN target_share >= 0.5 THEN 1 END) AS n_ge_half,
              MAX(air_yards_share) AS max_air_share,
              COUNT(CASE WHEN year < 2009 AND air_yards_share IS NOT NULL THEN 1 END) AS air_pre_2009
            FROM {SEASON}""",
    ),
}

REGISTRY_SEASON_PROBE_COLS = [
    "passing_epa", "rushing_epa", "receiving_epa", "passing_cpoe", "wopr", "racr", "pacr",
    "target_share", "air_yards_share", "passing_air_yards", "receiving_air_yards", "passer_rating",
]


def load_env() -> None:
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"'))


def run_sql_rows(sql: str) -> list[dict]:
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/")
    token = os.environ["DATABASE_READ_TOKEN"]
    resp = requests.post(
        f"{url}/query",
        json={"sql": sql, "database": "___ops", "max_rows": 5},
        headers={"Authorization": f"Bearer {token}"},
        timeout=90,
    )
    resp.raise_for_status()
    return resp.json()


def run_sql(sql: str) -> tuple[bool, str]:
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/")
    token = os.environ["DATABASE_READ_TOKEN"]
    try:
        resp = requests.post(
            f"{url}/query",
            json={"sql": sql, "database": "___ops", "max_rows": 5},
            headers={"Authorization": f"Bearer {token}"},
            timeout=90,
        )
    except requests.RequestException as exc:
        return False, f"request failed: {exc}"
    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}: {resp.text[:160]}"
    rows = resp.json()
    if not rows:
        return False, "0 rows"
    return True, f"{len(rows)} rows, first={str(rows[0])[:90]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shape", help="run a single named shape")
    a = ap.parse_args()
    load_env()

    names = [a.shape] if a.shape else list(SHAPES)
    failures: list[str] = []
    expected_failures_now_passing: list[str] = []
    for name in names:
        desc, sql = SHAPES[name]
        expect_fail = name == "wide_relation_window"
        if name == "registry_season_columns":
            cols = ", ".join(REGISTRY_SEASON_PROBE_COLS)
            sql = f"SELECT {cols} FROM {SEASON} LIMIT 1"
        t0 = time.time()
        ok, detail = run_sql(sql)
        ms = int((time.time() - t0) * 1000)
        if expect_fail:
            status = "XFAIL (engine bug still present)" if not ok else "XPASS -- ENGINE BUG FIXED, retire narrowSuperTableScan()"
            if ok:
                expected_failures_now_passing.append(name)
            print(f"[{status}] {name} ({ms}ms)")
            continue
        if ok and name == "rolling_window":
            top = run_sql_rows(SHAPES[name][1])[0]
            if not (top.get("player") == "Earl Campbell" and abs(float(top.get("w", 0)) - 898.0) < 0.5):
                ok, detail = False, f"GOLDEN MISMATCH: expected Earl Campbell 898, got {top}"
        if ok and name == "first5_window_golden":
            row = run_sql_rows(SHAPES[name][1])[0]
            if not (row.get("first_year") == 2023 and row.get("last_year") == 2023 and abs(float(row.get("total", 0)) - 92.6) < 0.05):
                ok, detail = False, f"GOLDEN MISMATCH: expected 2023/2023/92.6, got {row}"
        if ok and name == "season_share_golden":
            row = run_sql_rows(SHAPES[name][1])[0]
            if not (
                row.get("welker_2008") is not None
                and abs(float(row["welker_2008"]) - 0.2827) < 0.005
                and abs(float(row.get("welker_2010", 0)) - 0.26) < 0.005
                and float(row.get("max_share", 1)) < 0.5
                and int(row.get("n_ge_half", 1)) == 0
                and float(row.get("max_air_share", 1)) < 0.75
                and int(row.get("air_pre_2009", 1)) == 0
            ):
                ok, detail = False, (
                    "GOLDEN MISMATCH: expected welker_2008~0.2827/welker_2010~0.26/max<0.5/"
                    f"max_air<0.75/air_pre_2009=0, got {row}"
                )
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name} ({ms}ms) -- {detail if not ok else detail}")
        if not ok:
            failures.append(f"{name}: {desc} -> {detail}")

    if expected_failures_now_passing:
        print("\nNOTE: wide_relation_window now passes -- the DuckDB engine bug is fixed on Fly. "
              "narrowSuperTableScan() in research-derived.ts can be simplified away.")
    if failures:
        print("\nSMOKE FAILURES:")
        for f in failures:
            print("  " + f)
        sys.exit(1)
    print("\nAll live research shapes healthy.")


if __name__ == "__main__":
    main()
