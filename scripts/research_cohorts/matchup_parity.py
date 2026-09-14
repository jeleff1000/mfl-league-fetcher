"""Local parity validation for research matchup weekly and season artifacts."""

from dataclasses import dataclass
from pathlib import Path

import duckdb

from cohort_format_sql import FORMAT_DIMENSIONS
from matchup_metric_sql import season_metric_select


@dataclass(frozen=True)
class ParityError:
    teams: str
    roster: str
    ppr: str
    td: str
    year: int
    NFL_player_id: str
    metric: str
    weekly_value: float | None
    season_value: float | None


METRICS = (
    "start_rate_pct",
    "healthy_start_rate_pct",
    "expected_wins",
    "expected_losses",
    "expected_starts",
    "total_lamar_started",
    "avg_clutch_started",
)


def _sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def validate_weekly_season_parity(
    weekly_path: Path,
    season_path: Path,
    *,
    tolerance: float = 1e-8,
) -> list[ParityError]:
    """Return field-level differences between weekly rollups and level-4 seasons."""
    con = duckdb.connect()
    try:
        weekly_columns = {
            column[0]
            for column in con.execute(
                f"SELECT * FROM read_parquet('{_sql_path(weekly_path)}') LIMIT 0"
            ).description
        }
        season_columns = {
            column[0]
            for column in con.execute(
                f"SELECT * FROM read_parquet('{_sql_path(season_path)}') LIMIT 0"
            ).description
        }
        format_keys = [
            key for key in (*FORMAT_DIMENSIONS, "format_level")
            if key in weekly_columns and key in season_columns
        ]
        keys = [
            "teams", "roster", "ppr", "td", *format_keys,
            "year", "NFL_player_id",
        ]
        key_sql = ",".join(keys)
        group_sql = ",".join(str(index) for index in range(1, len(keys) + 1))
        rows = con.execute(f"""
          WITH rolled AS (
            SELECT {key_sql},
                   {season_metric_select(
                       'w',
                       roster_denominator=("roster_eligible_leagues"
                                            if "roster_eligible_leagues" in weekly_columns
                                            else "eligible_leagues"),
                       team_game_denominator=("team_game_eligible_leagues"
                                              if "team_game_eligible_leagues" in weekly_columns
                                              else "eligible_leagues"),
                       healthy_denominator=("healthy_eligible_leagues"
                                            if "healthy_eligible_leagues" in weekly_columns
                                            else "eligible_leagues"),
                       healthy_numerator=("healthy_started_leagues"
                                          if "healthy_started_leagues" in weekly_columns
                                          else "started_leagues"))}
            FROM read_parquet('{_sql_path(weekly_path)}') w
            WHERE eligible_leagues > 0
            GROUP BY {group_sql}
          )
          SELECT r.teams,r.roster,r.ppr,r.td,r.year,r.NFL_player_id,
                 {', '.join(f'r.{m} AS weekly_{m}, s.{m} AS season_{m}' for m in METRICS)}
          FROM rolled r
          JOIN read_parquet('{_sql_path(season_path)}') s
            USING ({key_sql})
          WHERE COALESCE(s.cohort_level,4)=4
        """).fetchall()
    finally:
        con.close()

    errors: list[ParityError] = []
    for row in rows:
        identity = row[:6]
        values = row[6:]
        for index, metric in enumerate(METRICS):
            weekly_value = values[index * 2]
            season_value = values[index * 2 + 1]
            if weekly_value is None and season_value is None:
                continue
            mismatch = weekly_value is None or season_value is None
            if not mismatch:
                mismatch = abs(float(weekly_value) - float(season_value)) > tolerance
            if mismatch:
                errors.append(ParityError(
                    *identity,
                    metric=metric,
                    weekly_value=weekly_value,
                    season_value=season_value,
                ))
    return errors
