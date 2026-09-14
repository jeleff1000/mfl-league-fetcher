"""Inventory guardrails for SQL-bearing frontend query surfaces.

This keeps the query audit explicit: when a new SQL-bearing frontend surface is
added, it must be consciously classified rather than silently drifting outside
our validator/review plan.
"""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
API_ROOT = ROOT / "frontend" / "src" / "app" / "api" / "league" / "[db]"

VALID_CLASSES = {
    "pipeline_contract",
    "preagg_parity",
    "operational_route",
    "drilldown_detail",
    "debug_only",
    "shared_query_family",
}


CONFIG_QUERY_SURFACES: dict[str, str] = {
    "draft/config.ts": "pipeline_contract",
    "dynasty/config.ts": "pipeline_contract",
    "hof/brackets/config.ts": "pipeline_contract",
    "hof/champions/config.ts": "pipeline_contract",
    "hof/games/config.ts": "pipeline_contract",
    "hof/leaderboards/config.ts": "pipeline_contract",
    "hof/player-games/config.ts": "pipeline_contract",
    "hof/player-records/config.ts": "preagg_parity",
    "hof/player-seasons/config.ts": "preagg_parity",
    "hof/records/config.ts": "pipeline_contract",
    "hof/sackos/config.ts": "pipeline_contract",
    "hof/seasons/config.ts": "preagg_parity",
    "keepers/config.ts": "pipeline_contract",
    "luck/config.ts": "pipeline_contract",
    "luck/h2h/config.ts": "preagg_parity",
    "luck/schedule-swap/config.ts": "pipeline_contract",
    "managers/config.ts": "pipeline_contract",
    "matchups/config.ts": "pipeline_contract",
    "matchups/season-extras/config.ts": "preagg_parity",
    "overview/config.ts": "preagg_parity",
    "players/career/config.ts": "preagg_parity",
    "players/season/config.ts": "preagg_parity",
    "players/weekly/config.ts": "pipeline_contract",
    "simulations/config.ts": "preagg_parity",
    "standings/config.ts": "pipeline_contract",
    "team-names/config.ts": "pipeline_contract",
    "team-stats/config.ts": "pipeline_contract",
    "transactions/config.ts": "pipeline_contract",
}


RAW_SQL_QUERY_SURFACES: dict[str, str] = {
    "draft/optimizer/route.ts": "drilldown_detail",
    "draft/route.ts": "preagg_parity",
    "features/route.ts": "operational_route",
    "keeper-config/apply/route.ts": "operational_route",
    "keeper-config/route.ts": "operational_route",
    "league-rules/route.ts": "operational_route",
    "manager-profile/route.ts": "drilldown_detail",
    "manager-settings/route.ts": "operational_route",
    "managers/trends/route.ts": "preagg_parity",
    "matchups/career/route.ts": "preagg_parity",
    "matchups/debug/route.ts": "debug_only",
    "matchups/h2h/route.ts": "preagg_parity",
    "players/card/route.ts": "drilldown_detail",
    "players/consistency-map/route.ts": "drilldown_detail",
    "players/h2h/route.ts": "drilldown_detail",
    "players/heatmap/route.ts": "drilldown_detail",
    "players/optimal/route.ts": "drilldown_detail",
    "players/position-trends/route.ts": "preagg_parity",
    "players/weekly-points/route.ts": "drilldown_detail",
    "schedules/route.ts": "drilldown_detail",
    "standings/route.ts": "preagg_parity",
    "standings-config/route.ts": "operational_route",
    "team-stats/detail/route.ts": "drilldown_detail",
    "transactions/shared.ts": "shared_query_family",
    "transactions/views/aggregation.ts": "preagg_parity",
    "transactions/views/legacy.ts": "drilldown_detail",
    "transactions/views/report-card.ts": "preagg_parity",
    "transactions/views/weekly-add-drop.ts": "drilldown_detail",
    "transactions/views/weekly-trades.ts": "drilldown_detail",
}


SQL_MARKERS = (
    "runQuery(",
    "runQueryRW(",
    "executeQuery(",
    "sql: (",
    "qualifyLeagueTableRef(",
    "scopedLeagueTable(",
)


def _actual_config_files() -> set[str]:
    return {str(path.relative_to(API_ROOT)).replace("\\", "/") for path in API_ROOT.rglob("config.ts")}


def _actual_raw_sql_files() -> set[str]:
    files: set[str] = set()
    for path in API_ROOT.rglob("*.ts"):
        rel = str(path.relative_to(API_ROOT)).replace("\\", "/")
        if rel.endswith("config.ts"):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(marker in text for marker in SQL_MARKERS):
            files.add(rel)
    return files


def test_all_config_query_surfaces_are_explicitly_classified():
    actual = _actual_config_files()
    accounted = set(CONFIG_QUERY_SURFACES)

    assert actual == accounted, (
        "Config query surface inventory drifted.\n"
        f"Missing from inventory: {sorted(actual - accounted)}\n"
        f"Extra in inventory: {sorted(accounted - actual)}"
    )


def test_all_raw_sql_query_surfaces_are_explicitly_classified():
    actual = _actual_raw_sql_files()
    accounted = set(RAW_SQL_QUERY_SURFACES)

    assert actual == accounted, (
        "Raw SQL query surface inventory drifted.\n"
        f"Missing from inventory: {sorted(actual - accounted)}\n"
        f"Extra in inventory: {sorted(accounted - actual)}"
    )


def test_query_surface_classes_are_valid():
    classes = set(CONFIG_QUERY_SURFACES.values()) | set(RAW_SQL_QUERY_SURFACES.values())
    assert classes <= VALID_CLASSES, "Unknown query surface classes: " f"{sorted(classes - VALID_CLASSES)}"
