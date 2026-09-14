"""
Aggregation Enrichments Mixin — Thin Wrapper

Delegates to standalone modules in aggregation/modules/.
"""

import logging

from .modules import lamar, optimal_lineup, clutch

logger = logging.getLogger(__name__)


class AggregationEnrichmentsMixin:
    """Thin wrapper — delegates to standalone modules for LAMAR, optimal lineup, and clutch."""

    @staticmethod
    def _flex_lamar_suffix(flex_name: str) -> str | None:
        return lamar.flex_lamar_suffix(flex_name)

    def league_wide_optimal_for_all(self, roster_by_year=None):
        return optimal_lineup.league_wide_optimal(
            self._get_connection(),
            self._qualified_name("player_fantasy"),
            roster_by_year or self.roster_by_year,
            self._build_roster_helpers(),
            dry_run=self.dry_run,
            db_name=self.db_name,
        )

    def compute_manager_optimal(self, roster_by_year=None):
        return optimal_lineup.manager_optimal(
            self._get_connection(),
            self._qualified_name("player_fantasy"),
            roster_by_year or self.roster_by_year,
            self._build_roster_helpers(),
            dry_run=self.dry_run,
            db_name=self.db_name,
        )

    def populate_position_rank(self, roster_by_year=None):
        return optimal_lineup.position_rank(
            self._get_connection(),
            self._qualified_name("player_fantasy"),
            roster_by_year or self.roster_by_year,
            self._build_roster_helpers(),
            dry_run=self.dry_run,
            db_name=self.db_name,
        )

    def calculate_lamar_for_all_players(self, roster_by_year=None):
        return lamar.calculate_lamar(
            self._get_connection(),
            self._qualified_name("player_fantasy"),
            roster_by_year or self.roster_by_year,
            dry_run=self.dry_run,
            db_name=self.db_name,
        )

    def calculate_clutch_equity(self):
        return clutch.calculate_clutch_equity(
            self._get_connection(),
            self._qualified_name("player_fantasy"),
            self._qualified_name("matchup"),
            dry_run=self.dry_run,
            db_name=self.db_name,
        )

    def _build_roster_helpers(self):
        """Build helpers dataclass for optimal lineup modules."""
        from .modules.optimal_lineup import RosterHelpers

        return RosterHelpers(
            available_settings_years=self._available_settings_years,
            resolve_settings_year=self._resolve_settings_year,
            get_dedicated_slots=self._get_dedicated_slots,
            identify_flex_positions=self._identify_flex_positions,
            position_eligibility_sql=self._position_eligibility_sql,
            flex_eligibility_sql=self._flex_eligibility_sql,
            preferred_flex_rank_column=self._preferred_flex_rank_column,
            front7_eligibility_sql=self._front7_eligibility_sql,
            primary_position_sql=self._primary_position_sql,
            group_years_by_scoring=self._group_years_by_scoring,
        )
