"""Playoff Bracket Package — SQL-first tracers.

Modules:
- bracket_tracer: championship tracer (trace_championship_bracket_sql)
- consolation_tracer_sql: consolation tracer (trace_consolation_sql)
- bracket_common: shared utilities
- utils: round-window math, load_league_settings, create_round_matchups
"""

from . import bracket_tracer
from . import consolation_tracer_sql
from . import bracket_common
from . import utils

# Re-export functions that external modules import from this package
from .utils import load_league_settings, create_round_matchups

__all__ = [
    "bracket_tracer",
    "consolation_tracer_sql",
    "bracket_common",
    "utils",
    "load_league_settings",
    "create_round_matchups",
]
