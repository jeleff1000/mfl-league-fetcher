"""
League Validation System

A standalone diagnostic tool for validating fantasy football league data.
Detects data quality issues that could break the UI or cause incorrect displays.

Usage:
    python -m multi_league.validation.cli validate leagues/sleeper/my_league
    python -m multi_league.validation.cli validate-all leagues/
"""

# Lazy imports — avoid pulling in pandas/duckdb at package load time
# (batch_validate only needs validate_motherduck, not the file-based validators)


def __getattr__(name: str):
    _imports = {
        "LeagueValidator": (".league_validator", "LeagueValidator"),
        "ValidationResults": (".league_validator", "ValidationResults"),
        "LeagueType": (".league_type_detector", "LeagueType"),
        "LeagueTypeDetector": (".league_type_detector", "LeagueTypeDetector"),
        "LeagueSettings": (".settings_loader", "LeagueSettings"),
        "SettingsLoader": (".settings_loader", "SettingsLoader"),
        "ValidationReport": (".report_generator", "ValidationReport"),
        "ReportGenerator": (".report_generator", "ReportGenerator"),
        "SyncChecker": (".sync_checker", "SyncChecker"),
        "SyncCheckResults": (".sync_checker", "SyncCheckResults"),
    }
    if name in _imports:
        module_path, attr = _imports[name]
        import importlib

        mod = importlib.import_module(module_path, __package__)
        return getattr(mod, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "LeagueValidator",
    "ValidationResults",
    "LeagueType",
    "LeagueTypeDetector",
    "LeagueSettings",
    "SettingsLoader",
    "ValidationReport",
    "ReportGenerator",
    "SyncChecker",
    "SyncCheckResults",
]
