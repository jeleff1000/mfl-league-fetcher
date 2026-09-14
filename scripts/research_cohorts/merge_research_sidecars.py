"""Apply validated outcome and source-matchup sidecars to a public lake copy.

This is an immutable-copy operation.  It never writes to Fly and never mutates
the restored cache in place.  Outcome rows update only the known player-week
keys; source matchup rows upsert on the canonical matchup key.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


CANONICAL_PLAYER_COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def player_join_key(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    platform_expr = f"lower(nullif(trim(cast({prefix}platform as varchar)), ''))"
    player_id_expr = (
        "case "
        f"when {platform_expr}='sleeper' then nullif(trim(cast({prefix}sleeper_player_id as varchar)), '') "
        f"when {platform_expr}='fleaflicker' then nullif(trim(cast({prefix}fleaflicker_player_id as varchar)), '') "
        f"when {platform_expr}='mfl' then nullif(trim(cast({prefix}mfl_player_id as varchar)), '') "
        f"else coalesce(nullif(trim(cast({prefix}NFL_player_id as varchar)), ''), "
        f"nullif(trim(cast({prefix}sleeper_player_id as varchar)), ''), "
        f"nullif(trim(cast({prefix}fleaflicker_player_id as varchar)), ''), "
        f"nullif(trim(cast({prefix}mfl_player_id as varchar)), '')) end"
    )
    return (
        "concat_ws('|', "
        f"cast({prefix}db_name as varchar), cast({prefix}year as varchar), cast({prefix}week as varchar), "
        f"coalesce({player_id_expr}, '<NO_PLAYER_ID>'), "
        f"coalesce(cast({prefix}manager as varchar), '<NULL_MANAGER>'), "
        f"coalesce(cast({prefix}team_key as varchar), '<NULL_TEAM_KEY>'), "
        f"coalesce(cast({prefix}team_name as varchar), '<NULL_TEAM_NAME>'))"
    )


def tables(con: duckdb.DuckDBPyConnection, db: str) -> list[str]:
    return [r[0] for r in con.execute(
        "select table_name from duckdb_tables() where database_name=? and schema_name='public'",
        [db],
    ).fetchall()]


def cols(con: duckdb.DuckDBPyConnection, db: str, table: str) -> list[str]:
    return [r[0] for r in con.execute(f"describe {db}.public.{qi(table)}").fetchall()]


def resolved_player_select(bcols: list[str], source: str = "b") -> list[str]:
    """Build the single canonical player schema at materialization time."""
    has_bf = "is_playoffs_bf" in bcols
    if "is_playoffs" not in bcols and not has_bf:
        raise SystemExit("player_fantasy has no playoff truth column")
    expressions: list[str] = []
    for column in bcols:
        if column in {"is_playoffs_bf", "made_po_bf", "made_po"}:
            continue
        if column == "is_playoffs" and has_bf:
            expressions.append(
                f"CASE WHEN {source}.is_playoffs IS NOT NULL THEN {source}.is_playoffs "
                f"ELSE {source}.is_playoffs_bf END AS is_playoffs"
            )
        else:
            expressions.append(f"{source}.{qi(column)} AS {qi(column)}")
    if "is_playoffs" not in bcols:
        expressions.append(f"{source}.is_playoffs_bf AS is_playoffs")
    return expressions


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--outcomes", type=Path, required=True)
    ap.add_argument("--matchups", type=Path, required=True)
    args = ap.parse_args()
    if args.out.exists():
        args.out.unlink()
    con = duckdb.connect(str(args.out))
    con.execute(f"attach '{args.base.resolve().as_posix().replace(chr(39), chr(39)*2)}' as base (read_only)")
    con.execute(f"attach '{args.outcomes.resolve().as_posix().replace(chr(39), chr(39)*2)}' as outcomes (read_only)")
    con.execute(f"attach '{args.matchups.resolve().as_posix().replace(chr(39), chr(39)*2)}' as rescues (read_only)")
    con.execute("create schema public")
    base_tables = set(tables(con, "base"))
    out_tables = set(tables(con, "outcomes"))
    rescue_tables = set(tables(con, "rescues"))

    for table in sorted(base_tables):
        if table in ("player_fantasy", "matchup"):
            continue
        con.execute(f"create table public.{qi(table)} as select * from base.public.{qi(table)}")

    if "player_fantasy" in base_tables:
        bcols = cols(con, "base", "player_fantasy")
        base_player_rows = con.execute("select count(*) from base.public.player_fantasy").fetchone()[0]
        outcome_cols = set(cols(con, "outcomes", "outcome_targets")) if "outcome_targets" in out_tables else set()
        if outcome_cols:
            con.execute("""
              create or replace temp table _outcome_agg as
              select db_name, cast(year as integer) as year, cast(week as integer) as week,
                     NFL_player_id, max(cast(win as integer)) as win, max(team_points) as team_points
              from outcomes.public.outcome_targets group by 1,2,3,4
            """)
        else:
            con.execute("""
              create or replace temp table _outcome_agg
              (db_name varchar, year integer, week integer, NFL_player_id varchar,
               win integer, team_points double)
            """)

        source_ready = False
        if "source_matchup_rescue" in rescue_tables:
            rcols = set(cols(con, "rescues", "source_matchup_rescue"))
            required_source = {"db_name", "year", "week", "manager", "team_key", "platform", "win", "team_points", "is_playoffs", "is_championship", "champion"}
            if not required_source <= rcols:
                raise SystemExit(f"source sidecar lacks player overlay fields: {sorted(required_source - rcols)}")
            p_manager = "LOWER(NULLIF(TRIM(CAST(p.manager AS VARCHAR)), ''))" if "manager" in bcols else "NULL"
            p_team = "LOWER(NULLIF(TRIM(CAST(p.team_name AS VARCHAR)), ''))" if "team_name" in bcols else "NULL"
            p_team_key = "NULLIF(TRIM(CAST(p.team_key AS VARCHAR)), '')" if "team_key" in bcols else "NULL"
            con.execute("""
              create or replace temp table _source_team_signals_raw as
              select cast(db_name as varchar) db_name, cast(year as integer) season_year,
                     cast(week as integer) season_week, nullif(trim(cast(team_key as varchar)), '') source_team_key,
                     lower(nullif(trim(cast(manager as varchar)), '')) manager_key,
                     lower(nullif(trim(cast(team_name as varchar)), '')) team_name_key,
                     max(cast(win as integer)) source_win, max(team_points) source_team_points,
                     max(case when cast(is_playoffs as integer)=1 then 1 else 0 end) source_playoffs,
                     max(case when cast(is_championship as integer)=1 and cast(champion as integer)=1 then 1 else 0 end) source_champion,
                     max(nullif(lower(trim(cast(platform as varchar))), '')) source_platform
              from rescues.public.source_matchup_rescue group by 1,2,3,4,5,6
            """)
            con.execute("""
              create or replace temp table _source_team_signals as
              select r.*, count(distinct source_team_key) over
                (partition by db_name, season_year, season_week, manager_key) manager_team_count
              from _source_team_signals_raw r
            """)
            identity = f"""
              (({p_team_key} is not null and s.source_team_key is not null and {p_team_key}=s.source_team_key)
               or ({p_manager}=s.manager_key and {p_team}=s.team_name_key and s.team_name_key is not null)
               or ({p_manager}=s.manager_key and s.manager_key is not null and s.manager_team_count=1))
            """
            con.execute(f"""
              create or replace temp table _source_player_matches as
              select p.rowid player_rowid, count(distinct s.source_team_key) matches,
                     max(s.source_win) source_win, max(s.source_team_points) source_team_points,
                     max(s.source_playoffs) source_playoffs, max(s.source_champion) source_champion,
                     max(s.source_platform) source_platform
              from base.public.player_fantasy p
              join _source_team_signals s
                on s.db_name=p.db_name and s.season_year=cast(p.year as integer) and s.season_week=cast(p.week as integer)
               and {identity}
              group by p.rowid
            """)
            ambiguous = con.execute("select count(*) from _source_player_matches where matches > 1").fetchone()[0]
            if ambiguous:
                raise SystemExit(f"ambiguous source team joins: {ambiguous}")
            source_ready = True
        else:
            con.execute("""
              create or replace temp table _source_player_matches
              (player_rowid bigint, matches integer, source_win integer,
               source_team_points double, source_playoffs integer,
               source_champion integer, source_platform varchar)
            """)

        select_expr = resolved_player_select(bcols)
        replacements = {
            'b."win" AS "win"': 'CASE WHEN b."win" IS NULL THEN COALESCE(o.win, s.source_win) ELSE b."win" END AS "win"',
            'b."team_points" AS "team_points"': 'CASE WHEN b."team_points" IS NULL THEN COALESCE(o.team_points, s.source_team_points) ELSE b."team_points" END AS "team_points"',
            'b."is_playoffs" AS "is_playoffs"': 'CASE WHEN b."is_playoffs" IS NULL AND s.source_playoffs=1 THEN 1 ELSE b."is_playoffs" END AS "is_playoffs"',
            'b."champion" AS "champion"': 'CASE WHEN b."champion" IS NULL AND s.source_champion=1 THEN 1 ELSE b."champion" END AS "champion"',
            'b."platform" AS "platform"': 'CASE WHEN b."platform" IS NULL THEN s.source_platform ELSE b."platform" END AS "platform"',
        }
        select_expr = [replacements.get(expression, expression) for expression in select_expr]
        con.execute(f"""
          create table public.player_fantasy as
          select {', '.join(select_expr)} from base.public.player_fantasy b
          left join _outcome_agg o on o.db_name=b.db_name and o.year=cast(b.year as integer)
           and o.week=cast(b.week as integer) and o.NFL_player_id=b.NFL_player_id
          left join _source_player_matches s on s.player_rowid=b.rowid
        """)

    if "matchup" in base_tables or "source_matchup_rescue" in rescue_tables:
        bcols = cols(con, "base", "matchup") if "matchup" in base_tables else []
        rcols = cols(con, "rescues", "source_matchup_rescue") if "source_matchup_rescue" in rescue_tables else []
        if not rcols:
            if bcols:
                con.execute("create table public.matchup as select * from base.public.matchup")
        else:
            extra = {"target_year", "target_reason_codes"}
            rcols = [c for c in rcols if c not in extra]
            if set(rcols) != set(bcols):
                raise SystemExit(
                    "matchup sidecar schema mismatch: source-only fields cannot enter canonical matchup: "
                    f"base={bcols}, projected_source={rcols}"
                )
            # Source artifacts may serialize the same canonical fields in a
            # different order.  The output must follow the base schema order.
            rcols = list(bcols)
            allcols = list(bcols)
            if {"db_name", "year", "week", "team_key"} <= set(bcols) and "team_key" in rcols:
                keys = ["db_name", "year", "week", "team_key"]
            elif {"db_name", "year", "week", "manager", "team_name"} <= set(bcols):
                keys = ["db_name", "year", "week", "manager", "team_name"]
            elif {"db_name", "year", "week", "manager"} <= set(bcols):
                keys = ["db_name", "year", "week", "manager"]
            else:
                raise SystemExit(f"matchup sidecar lacks canonical merge keys: base={bcols}, rescue={rcols}")
            pred_parts = []
            for key in keys:
                if key in {"manager", "team_name", "team_key"}:
                    pred_parts.append(
                        f"NULLIF(TRIM(CAST(o.{qi(key)} AS VARCHAR)), '') "
                        f"IS NOT DISTINCT FROM NULLIF(TRIM(CAST(b.{qi(key)} AS VARCHAR)), '')"
                    )
                else:
                    pred_parts.append(f"CAST(o.{qi(key)} AS VARCHAR) IS NOT DISTINCT FROM CAST(b.{qi(key)} AS VARCHAR)")
            pred = " and ".join(pred_parts)
            # Use the overlay type for missing base columns; DuckDB can infer the
            # union schema from the explicit SELECT arms.
            rtypes = {r[0]: r[1] for r in con.execute("describe rescues.public.source_matchup_rescue").fetchall()}
            bsel = ", ".join(f"b.{qi(c)} as {qi(c)}" if c in bcols else f"cast(null as {rtypes[c]}) as {qi(c)}" for c in allcols)
            positive_signal_columns = {"is_playoffs", "is_championship", "champion"}
            rsel_parts = []
            for c in allcols:
                if c in bcols and c in rcols:
                    if c in positive_signal_columns:
                        rsel_parts.append(
                            f"CASE WHEN CAST(o.{qi(c)} AS INTEGER)=1 THEN 1 ELSE b.{qi(c)} END AS {qi(c)}"
                        )
                    else:
                        rsel_parts.append(
                            f"CASE WHEN b.{qi(c)} IS NULL THEN o.{qi(c)} ELSE b.{qi(c)} END AS {qi(c)}"
                        )
                elif c in bcols:
                    rsel_parts.append(f"b.{qi(c)} as {qi(c)}")
                else:
                    rsel_parts.append(f"o.{qi(c)} as {qi(c)}")
            rsel = ", ".join(rsel_parts)
            only = ", ".join(f"o.{qi(c)} as {qi(c)}" if c in rcols else f"cast(null as {dict((r[0],r[1]) for r in con.execute('describe base.public.matchup').fetchall())[c]}) as {qi(c)}" for c in allcols)
            if bcols:
                con.execute(f"""
                  create table public.matchup as
                  select {rsel} from base.public.matchup b
                  left join rescues.public.source_matchup_rescue o on {pred}
                  union all
                  select {only} from rescues.public.source_matchup_rescue o
                  where not exists (select 1 from base.public.matchup b where {pred})
                """)
            else:
                con.execute(f"create table public.matchup as select {', '.join(qi(c) for c in rcols)} from rescues.public.source_matchup_rescue")

    output_db = con.execute("select current_database()").fetchone()[0]
    for required in ("player_fantasy", "matchup"):
        if required not in tables(con, output_db):
            raise SystemExit(f"merged lake missing {required}")
    player_cols = set(cols(con, output_db, "player_fantasy"))
    actual_player_columns = cols(con, output_db, "player_fantasy")
    if actual_player_columns != CANONICAL_PLAYER_COLUMNS:
        raise SystemExit(
            "player schema/order changed: "
            f"expected={CANONICAL_PLAYER_COLUMNS}, actual={actual_player_columns}"
        )
    output_player_rows = con.execute("select count(*) from public.player_fantasy").fetchone()[0]
    if "player_fantasy" in base_tables and output_player_rows != base_player_rows:
        raise SystemExit(
            f"player row count changed: base={base_player_rows}, output={output_player_rows}"
        )

    player_key = player_join_key()
    base_key_count = con.execute(
        f"select count(distinct {player_join_key('b')}) from base.public.player_fantasy b"
    ).fetchone()[0] if "player_fantasy" in base_tables else 0
    output_key_count = con.execute(
        f"select count(distinct {player_join_key('p')}) from public.player_fantasy p"
    ).fetchone()[0]
    duplicate_player_keys = con.execute(
        f"select count(*) from (select {player_key} k from public.player_fantasy group by 1 having count(*) > 1)"
    ).fetchone()[0]
    if duplicate_player_keys:
        raise SystemExit(f"duplicate canonical player join keys: {duplicate_player_keys}")
    missing_base_keys = con.execute(
        f"select count(*) from (select distinct {player_join_key('b')} k from base.public.player_fantasy b) b "
        f"where not exists (select 1 from public.player_fantasy p where {player_join_key('p')}=b.k)"
    ).fetchone()[0] if "player_fantasy" in base_tables else 0
    if output_player_rows < base_player_rows or missing_base_keys:
        raise SystemExit(
            "player rows were lost: "
            f"base={base_player_rows}, output={output_player_rows}, missing_base_keys={missing_base_keys}"
        )
    forbidden = {"is_playoffs_bf", "made_po_bf", "made_po"} & player_cols
    if forbidden or "is_playoffs" not in player_cols:
        raise SystemExit(
            f"player schema is not canonical: forbidden={sorted(forbidden)}, "
            f"has_is_playoffs={'is_playoffs' in player_cols}"
        )
    print({
        "player_fantasy_rows": output_player_rows,
        "base_player_fantasy_rows": base_player_rows if "player_fantasy" in base_tables else None,
        "base_unique_player_keys": base_key_count,
        "output_unique_player_keys": output_key_count,
        "new_player_rows": output_player_rows - base_player_rows,
        "missing_base_player_keys": missing_base_keys,
        "duplicate_player_join_keys": duplicate_player_keys,
        "canonical_player_columns": actual_player_columns,
        "matchup_rows": con.execute("select count(*) from public.matchup").fetchone()[0],
        "overlay_matchup_rows": con.execute("select count(*) from rescues.public.source_matchup_rescue").fetchone()[0] if "source_matchup_rescue" in rescue_tables else 0,
    })


if __name__ == "__main__":
    main()
