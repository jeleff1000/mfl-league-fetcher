"""
Consolidated import path setup for transformation scripts.

Call setup_module_path() at module level in any script that needs to import
from multi_league.core, multi_league.data_fetchers, etc.

Replaces the sys.path.insert + try/except ImportError boilerplate
that appears in 100+ scripts across transformations/, data_fetchers/, etc.

Usage (works whether imported as package OR run as standalone script):
    try:
        from multi_league.shared.import_setup import setup_module_path
    except ImportError:
        import sys as _sys; from pathlib import Path as _Path
        _d = _Path(__file__).resolve().parent
        while not (_d / 'shared' / 'import_setup.py').exists() and _d != _d.parent: _d = _d.parent
        _sys.path.insert(0, str(_d.parent)); del _d
        from multi_league.shared.import_setup import setup_module_path
    setup_module_path()

    # Now these imports work regardless of how the script was invoked:
    from multi_league.core.league_context import LeagueContext
    from multi_league.core.import_config import PASS_1
"""

import sys
from pathlib import Path


def setup_module_path(caller_file: str = None) -> Path:
    """Add the correct directories to sys.path for multi_league imports.

    Ensures both `from multi_league.X import Y` and `from core.X import Y`
    import styles work, matching the existing codebase convention.

    Args:
        caller_file: __file__ of the calling script (optional, for logging)

    Returns:
        Path to the fantasy_football_data_scripts directory
    """
    # Walk up from this file to find fantasy_football_data_scripts
    # shared/import_setup.py → multi_league/shared/ → multi_league/ → fantasy_football_data_scripts/
    current = Path(__file__).resolve()
    multi_league_dir = current.parent.parent
    scripts_dir = multi_league_dir.parent

    paths_to_add = [
        str(scripts_dir),  # fantasy_football_data_scripts/ (for `from multi_league.X`)
        str(multi_league_dir),  # multi_league/ (for `from core.X`)
    ]

    for p in paths_to_add:
        if p not in sys.path:
            sys.path.insert(0, p)

    return scripts_dir


def _bootstrap_and_setup(caller_file: str) -> Path:
    """Bootstrap sys.path from a caller's __file__ so setup_module_path() is importable.

    This is used by scripts that may be run as standalone (not as a package),
    e.g. `python multi_league/transformations/matchup/expected_record_v2.py`.
    In that case, `from multi_league.shared.import_setup import ...` would fail
    because Python doesn't know about the multi_league package yet.

    Args:
        caller_file: The __file__ of the calling script.

    Returns:
        Path to the fantasy_football_data_scripts directory.
    """
    caller = Path(caller_file).resolve()
    # Walk up to find multi_league/ directory (contains shared/import_setup.py)
    current = caller.parent
    for _ in range(10):  # Safety limit
        if (current / "shared" / "import_setup.py").exists():
            # Found multi_league/
            scripts_dir = current.parent
            for p in [str(scripts_dir), str(current)]:
                if p not in sys.path:
                    sys.path.insert(0, p)
            return scripts_dir
        current = current.parent
    # Fallback: caller might be in a shallow location, just use setup_module_path
    return setup_module_path(caller_file)
