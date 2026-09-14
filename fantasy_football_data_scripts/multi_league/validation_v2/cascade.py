"""Cascade suppression for validation_v2.

Post-hoc cascade logic: when a root-cause check fails for a league,
downstream checks whose page matches a suppression set are marked
suppressed=True so they don't clutter the report.

Root checks (those whose names appear as keys in CASCADE_RULES)
are never suppressed — they always stay visible.
"""

from __future__ import annotations

from multi_league.validation_v2.models import CheckResult

# Root cause check name -> set of downstream page tags to suppress
CASCADE_RULES: dict[str, set[str]] = {
    # Missing matchup data breaks most of the app
    "completeness_has_matchup": {
        "matchups",
        "standings",
        "playoffs",
        "simulations",
        "overview",
        "recaps",
        "team_names",
        "schedules",
    },
    # Missing player_fantasy breaks player-centric pages
    "completeness_has_player_fantasy": {
        "players",
        "recaps",
        "keepers",
    },
    # Missing draft data breaks draft page
    "completeness_has_draft": {"draft"},
    # Missing transactions data breaks transactions page
    "completeness_has_transactions": {"transactions"},
    # Missing settings breaks league settings page
    "completeness_has_settings": {"league_settings"},
    # LAMAR not computed -> suppress LAMAR-dependent checks
    "pipeline_lamar_nonzero": {"pipeline_health"},
    # Optimal lineup not computed -> suppress optimal-dependent checks
    "pipeline_optimal_calculated": {"players"},
    # Franchise ID NULL -> suppress identity/standings downstream
    "identity_franchise_id_not_null": {"system", "standings"},
}

# Root check names — these are never suppressed themselves
_ROOT_CHECK_NAMES: set[str] = set(CASCADE_RULES.keys())


def apply_cascade(results: list[CheckResult]) -> list[CheckResult]:
    """Apply cascade suppression rules to a list of CheckResults.

    For each (db_name, root_check) pair that failed, find downstream
    results for the same db_name whose check.page is in the suppressed
    set. Mark those downstream results as suppressed.

    Root checks themselves are never suppressed.

    Args:
        results: List of CheckResult objects from all passes.

    Returns:
        The same list (mutated in place) with suppressed flags set.
    """
    # Build set of (db_name, root_check_name) pairs that failed
    failed_roots: set[tuple[str, str]] = set()
    for r in results:
        if r.check.name in CASCADE_RULES and not r.passed and not r.skipped:
            failed_roots.add((r.db_name, r.check.name))

    if not failed_roots:
        return results

    # Build a lookup: db_name -> set of pages to suppress, and which root caused it
    suppress_map: dict[str, list[tuple[set[str], str]]] = {}
    for db_name, root_name in failed_roots:
        suppress_map.setdefault(db_name, []).append((CASCADE_RULES[root_name], root_name))

    # Apply suppression to downstream results
    for r in results:
        # Never suppress root checks
        if r.check.name in _ROOT_CHECK_NAMES:
            continue
        # Only suppress if there's a matching root failure for this league
        entries = suppress_map.get(r.db_name)
        if not entries:
            continue
        for pages_to_suppress, root_name in entries:
            if r.check.page in pages_to_suppress:
                r.suppressed = True
                r.suppressed_by = root_name
                break  # One suppression reason is enough

    return results
