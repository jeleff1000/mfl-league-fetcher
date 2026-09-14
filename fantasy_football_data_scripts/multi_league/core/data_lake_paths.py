"""Canonical local D-drive paths for league-history data.

Defaults are local-only and can be overridden with ``LEAGUE_HISTORY_DATA_ROOT``.
"""

from __future__ import annotations

import os
from pathlib import Path


def data_root() -> Path:
    return Path(os.environ.get("LEAGUE_HISTORY_DATA_ROOT", r"D:\league-history-data"))


def nfl_root() -> Path:
    return data_root() / "nfl"


def nfl_ops_data_root() -> Path:
    return nfl_root() / "ops_data"


def nfl_ops_historical_root() -> Path:
    return nfl_ops_data_root() / "nfl_historical"


def nfl_ops_cache_root() -> Path:
    return nfl_root() / "ops_cache"


def player_bio_path() -> Path:
    return nfl_ops_historical_root() / "player_bio.parquet"


def known_identity_repair_map_path() -> Path:
    return nfl_ops_historical_root() / "known_identity_player_week_repair_map.csv"


def raw_pfr_root() -> Path:
    return nfl_root() / "raw" / "pfr"


def raw_pfr_boxscores_root() -> Path:
    return raw_pfr_root() / "boxscores"


def raw_pfr_context_root() -> Path:
    return raw_pfr_root() / "context"


def raw_pfr_players_root() -> Path:
    return raw_pfr_root() / "players"


def pfr_excel_zip_path() -> Path:
    return raw_pfr_root() / "cache" / "pfr_excel.zip"


def pfr_excel_cache_root() -> Path:
    return raw_pfr_root() / "cache" / "pfr_excel"


def raw_stathead_generated_root() -> Path:
    return nfl_root() / "raw" / "stathead" / "generated"


def pbp_player_week_rollup_path() -> Path:
    return raw_stathead_generated_root() / "pbp_supertable_audit_1978_2025" / "pbp_player_week_rollup.parquet"


def legacy_catalog_root() -> Path:
    return nfl_root() / "curated" / "legacy_catalog" / "fantasy_football_data_organized_catalog"


def legacy_supertable_backup_sources_root() -> Path:
    return nfl_root() / "raw" / "legacy_supertable_backup_sources"


def pbp_gap_audit_catalog_root() -> Path:
    return legacy_catalog_root() / "pbp_supertable_gap_audit_20260507"


def ancient_apply_output_root() -> Path:
    return nfl_ops_historical_root() / "ancient_apply"


def ancient_upsert_bundle_output_root() -> Path:
    return nfl_ops_historical_root() / "ancient_upsert_bundle"
