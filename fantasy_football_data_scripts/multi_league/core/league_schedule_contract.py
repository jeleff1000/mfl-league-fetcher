"""Shared SQL witnesses for optional regular-season schedule opponents."""


def declared_regular_season_bye_sql(*, alias: str, latest_week_sql: str) -> str:
    """Accept a native future bye or a completed bye proved by canonical facts.

    Schedule's canonical projection need not retain the optional fetcher flag.
    The corresponding persisted matchup remains an independent completed-bye
    witness. Callers separately enforce complete odd-team inventory and one bye.
    """
    return (
        f"({alias}.opponent = 'BYE' AND {alias}.opponent_franchise_id IS NULL AND ("
        f"COALESCE(TRY_CAST(json_extract_string(to_json({alias}), '$.is_bye_week') AS BOOLEAN), FALSE) "
        f"OR TRY_CAST({alias}.week AS INTEGER) > {latest_week_sql} "
        "OR EXISTS (SELECT 1 FROM public.matchup AS bye_matchup "
        f"WHERE bye_matchup.db_name = {alias}.db_name AND bye_matchup.year = {alias}.year "
        f"AND bye_matchup.week = {alias}.week AND bye_matchup.franchise_id = {alias}.franchise_id "
        "AND COALESCE(TRY_CAST(json_extract_string(to_json(bye_matchup), '$.is_bye_week') AS BOOLEAN), FALSE) "
        "AND bye_matchup.team_points IS NULL AND bye_matchup.opponent_points IS NULL)))"
    )
