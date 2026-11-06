"""Shared SQL witnesses for optional regular-season schedule opponents."""


def native_guillotine_inventory_sql(*, alias: str, latest_week_sql: str) -> str:
    """Validate identities in each present native inventory without making fixtures.

    Historical rows must belong to that completed week. Future rows must belong
    to the latest completed field and agree with the first available future
    inventory; Yahoo can omit any later weeks from its rolling scoreboard.
    """
    return (
        f"(({latest_week_sql} <= 0 OR EXISTS (SELECT 1 FROM public.matchup AS native_fact "
        f"WHERE native_fact.db_name = {alias}.db_name AND native_fact.year = {alias}.year "
        f"AND TRY_CAST(native_fact.week AS INTEGER) = LEAST(TRY_CAST({alias}.week AS INTEGER), {latest_week_sql}) "
        f"AND native_fact.franchise_id = {alias}.franchise_id "
        "AND native_fact.team_points IS NOT NULL AND native_fact.opponent_points IS NOT NULL)) "
        f"AND (TRY_CAST({alias}.week AS INTEGER) <= {latest_week_sql} OR EXISTS ("
        "SELECT 1 FROM public.schedule AS native_future "
        f"WHERE native_future.db_name = {alias}.db_name AND native_future.year = {alias}.year "
        f"AND native_future.franchise_id = {alias}.franchise_id "
        "AND TRY_CAST(native_future.week AS INTEGER) = (SELECT MIN(TRY_CAST(native_first.week AS INTEGER)) "
        "FROM public.schedule AS native_first "
        f"WHERE native_first.db_name = {alias}.db_name AND native_first.year = {alias}.year "
        f"AND TRY_CAST(native_first.week AS INTEGER) > {latest_week_sql} "
        "AND NOT COALESCE(TRY_CAST(json_extract_string(to_json(native_first), '$.is_playoffs') AS BOOLEAN), FALSE)))))"
    )


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
