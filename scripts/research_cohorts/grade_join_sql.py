"""Shared format-safe joins used by research grade builders."""
from __future__ import annotations

from cohort_format_sql import SEASON_GRAIN


def _relation(source: str) -> str:
    if "/" in source or "\\" in source or source.endswith(".parquet"):
        return "'" + source.replace("'", "''") + "'"
    return source


def matchup_clutch_join_sql(source: str, matchup: str) -> str:
    """Attach one matchup clutch value without crossing cohort or format grains."""
    keys = ",".join(SEASON_GRAIN)
    return f"""SELECT x.*, m.avg_clutch_started AS clu
      FROM {_relation(source)} x
      LEFT JOIN (
        SELECT {keys}, avg_clutch_started
        FROM {_relation(matchup)}
      ) m USING ({keys})"""
