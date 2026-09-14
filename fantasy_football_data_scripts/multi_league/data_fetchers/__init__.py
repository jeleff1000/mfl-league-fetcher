"""Yahoo, Sleeper, and ESPN data fetcher packages.

Submodules are imported explicitly where needed — NOT eagerly at package
level — to avoid cascading relative-import failures when the package is
loaded via sys.path manipulation (e.g., in subprocess fetcher scripts).

Import submodules directly:
    from multi_league.data_fetchers.yahoo.yahoo_matchups import weekly_matchup_data
    from multi_league.data_fetchers.shared.clean_names import norm_manager
    from multi_league.data_fetchers.shared.nfl_player_mapping import get_yahoo_to_nfl_map
"""
