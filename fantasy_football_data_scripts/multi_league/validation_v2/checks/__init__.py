from .completeness import CHECKS as completeness_checks
from .manager_identity import CHECKS as identity_checks
from .manifest_checks import CHECKS as manifest_checks
from .matchups import CHECKS as matchup_checks
from .standings import CHECKS as standings_checks
from .playoffs import CHECKS as playoff_checks
from .simulations import CHECKS as simulation_checks
from .luck import CHECKS as luck_checks
from .players_weekly import CHECKS as players_weekly_checks
from .players_agg import CHECKS as players_agg_checks
from .optimal_lineup import CHECKS as optimal_lineup_checks
from .draft import CHECKS as draft_checks
from .keepers import CHECKS as keeper_checks
from .transactions import CHECKS as transaction_checks
from .overview import CHECKS as overview_checks
from .recaps import CHECKS as recap_checks
from .team_names import CHECKS as team_name_checks
from .schedules import CHECKS as schedule_checks
from .league_settings import CHECKS as league_settings_checks
from .system import CHECKS as system_checks
from .pipeline_health import CHECKS as pipeline_health_checks
from .super_table import CHECKS as super_table_checks

ALL_CHECKS = list(
    completeness_checks
    + identity_checks
    + manifest_checks
    + matchup_checks
    + standings_checks
    + playoff_checks
    + simulation_checks
    + luck_checks
    + players_weekly_checks
    + players_agg_checks
    + optimal_lineup_checks
    + draft_checks
    + keeper_checks
    + transaction_checks
    + overview_checks
    + recap_checks
    + team_name_checks
    + schedule_checks
    + league_settings_checks
    + system_checks
    + pipeline_health_checks
    + super_table_checks
)
