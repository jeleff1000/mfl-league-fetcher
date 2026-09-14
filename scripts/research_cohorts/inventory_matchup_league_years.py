"""Inventory the GH public research lake at the league-year grain.

This is deliberately a source audit, not a cohort aggregate audit.  Cohort parquet
files normally discard ``db_name`` and therefore cannot establish that a particular
league-year was actually represented.  The public lake is the authority for deciding
which league-years need a focused matchup rescue.

The output is fail-closed: settings-only rows are excluded from the rescue population,
while any populated league-year missing one of the required denominator/outcome lanes is
marked ``needs_rescue`` with explicit reason codes.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import os
from pathlib import Path

import duckdb


def columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall()}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def inventory(root: Path, years: set[int]) -> list[dict]:
    snapshot = root / "corpus_snapshot.duckdb"
    ops = root / "ops_cache.duckdb"
    if not snapshot.is_file() or snapshot.stat().st_size == 0:
        raise FileNotFoundError(snapshot)
    if not ops.is_file() or ops.stat().st_size == 0:
        raise FileNotFoundError(ops)

    # Do not instantiate LocalReader here.  It eagerly materializes the full bye-aware
    # player/team lattice, which is appropriate for a matchup build but defeats the point
    # of a fast inventory.  Read the immutable lake and the ops cache directly and measure
    # source coverage with bounded aggregates.
    con = duckdb.connect()
    con.execute(f"ATTACH '{snapshot.resolve().as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{ops.resolve().as_posix()}' AS ops (READ_ONLY)")
    prefix = "lake.public"

    ls_cols = columns(con, f"{prefix}.league_settings")
    pf_cols = columns(con, f"{prefix}.player_fantasy")
    m_cols = columns(con, f"{prefix}.matchup")
    ops_cols = columns(con, "ops.nfl_historical.nfl_player_stats_all")

    if "db_name" not in ls_cols or "year" not in ls_cols:
        raise RuntimeError("league_settings lacks db_name/year")
    if "db_name" not in pf_cols or "year" not in pf_cols:
        raise RuntimeError("player_fantasy lacks db_name/year")

    ops_year = f"CAST(year AS INTEGER) IN ({','.join(str(y) for y in sorted(years))})" if years else "TRUE"
    ops_reg = "season_type='REG'" if "season_type" in ops_cols else "TRUE"
    snap_terms = [f"COALESCE({c},0) > 0" for c in ("offense_snaps", "special_teams_snaps", "defense_snaps") if c in ops_cols]
    active_signal = " OR ".join(snap_terms)
    active_signal = f"({active_signal} OR COALESCE(fantasy_points_ppr,0) <> 0)" if active_signal and "fantasy_points_ppr" in ops_cols else active_signal or "TRUE"
    active_clause = f"(position='DEF' OR CAST(year AS INTEGER)<2012 OR {active_signal})" if "position" in ops_cols else active_signal
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _ops_pos AS
        SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id, CAST(year AS INTEGER) AS year
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE {ops_year} AND NFL_player_id IS NOT NULL AND position IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _ops_active AS
        SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
               CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE {ops_year} AND {ops_reg} AND NFL_player_id IS NOT NULL AND week IS NOT NULL
          AND {active_clause}""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _ops_team_weeks AS
        SELECT DISTINCT CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE {ops_year} AND {ops_reg} AND week IS NOT NULL AND nfl_team IS NOT NULL""")

    year_pred = f"CAST(year AS INTEGER) IN ({','.join(str(y) for y in sorted(years))})" if years else "TRUE"
    pf_started = "CAST(is_started AS INTEGER)=1" if "is_started" in pf_cols else "FALSE"
    pf_rostered = "CAST(is_rostered AS INTEGER)=1" if "is_rostered" in pf_cols else "FALSE"
    pf_id = "NFL_player_id IS NOT NULL" if "NFL_player_id" in pf_cols else "FALSE"
    pf_clutch = "clutch_equity IS NOT NULL" if "clutch_equity" in pf_cols else "FALSE"
    pf_champ = "CAST(champion AS INTEGER)=1" if "champion" in pf_cols else "FALSE"
    # The matchup table is not the only authoritative outcome source.  The matchup
    # builder deliberately falls back to player_fantasy's canonical win/loss/tie or
    # score fields when a league import did not materialize public.matchup.  The
    # inventory must apply the same fallback or it will schedule tens of thousands of
    # already-usable league-years for pointless rescue work.
    pf_outcome_terms = [f"{c} IS NOT NULL" for c in ("win", "loss", "tie") if c in pf_cols]
    if {"team_points", "opponent_points"} <= pf_cols:
        pf_outcome_terms.append("team_points IS NOT NULL AND opponent_points IS NOT NULL")
    pf_outcome_known = " OR ".join(pf_outcome_terms) or "FALSE"
    pf_playoff_terms = [
        "CAST(is_playoffs AS INTEGER)=1" if "is_playoffs" in pf_cols else None,
        "final_playoff_seed IS NOT NULL" if "final_playoff_seed" in pf_cols else None,
        "playoff_round IS NOT NULL" if "playoff_round" in pf_cols else None,
        "CAST(champion AS INTEGER)=1" if "champion" in pf_cols else None,
        "CAST(championship AS INTEGER)=1" if "championship" in pf_cols else None,
        "CAST(is_championship AS INTEGER)=1" if "is_championship" in pf_cols else None,
    ]
    pf_playoff_signal = " OR ".join(x for x in pf_playoff_terms if x) or "FALSE"
    # Championship eligibility requires a row-level title-game signal.  The
    # `champion` field is a season/team winner marker and is intentionally not
    # accepted here: it is commonly stamped on every roster row for the
    # eventual winner and would hide the exact source gap we are inventorying.
    pf_champ_terms = [
        "CAST(championship AS INTEGER)=1" if "championship" in pf_cols else None,
        "CAST(is_championship AS INTEGER)=1" if "is_championship" in pf_cols else None,
    ]
    pf_champ_signal = " OR ".join(x for x in pf_champ_terms if x) or "FALSE"

    # A number of historical sources store confirmed W/L/T only on matchup,
    # not on player_fantasy.  Materialize that fallback once; a correlated
    # EXISTS over the 360M-row player table exhausts the Actions temp volume.
    matchup_player_outcome_terms = [
        f"{c} IS NOT NULL" for c in ("win", "loss", "tie") if c in m_cols
    ]
    if {"opponent", "opponent_points"} <= m_cols:
        matchup_player_outcome_terms.append(
            "opponent IS NOT NULL AND opponent_points IS NOT NULL"
        )
    matchup_player_outcome = " OR ".join(matchup_player_outcome_terms) or "FALSE"
    matchup_key_cols = ["db_name", "year", "week"]
    # Manager is the stable cross-table key in historical source folds; old
    # imports may carry platform-specific franchise IDs on only one side.
    if "manager" in pf_cols and "manager" in m_cols:
        matchup_key_cols.append("manager")
    elif "franchise_id" in pf_cols and "franchise_id" in m_cols:
        matchup_key_cols.append("franchise_id")
    if len(matchup_key_cols) > 3 and {"db_name", "year", "week"} <= m_cols:
        key_select = ", ".join(
            ["CAST(db_name AS VARCHAR) AS db_name", "CAST(year AS INTEGER) AS year", "CAST(week AS INTEGER) AS week"]
            + [
                f'LOWER(TRIM(CAST("{c}" AS VARCHAR))) AS "{c}"'
                if c == "manager" else f'CAST("{c}" AS VARCHAR) AS "{c}"'
                for c in matchup_key_cols[3:]
            ]
        )
        key_join = " AND ".join(
            ["mo.db_name = f.db_name", "mo.year = CAST(f.year AS INTEGER)", "mo.week = CAST(f.week AS INTEGER)"]
            + [
                f'mo."{c}" IS NOT DISTINCT FROM LOWER(TRIM(CAST(f."{c}" AS VARCHAR)))'
                if c == "manager" else f'mo."{c}" IS NOT DISTINCT FROM f."{c}"'
                for c in matchup_key_cols[3:]
            ]
        )
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _matchup_outcomes AS
            SELECT {key_select}, 1::INTEGER AS has_outcome
            FROM {prefix}.matchup
            WHERE CAST(year AS INTEGER) IN ({','.join(str(y) for y in sorted(years))})
              AND ({matchup_player_outcome})
            GROUP BY ALL""")
        pf_matchup_outcome = "COALESCE(mo.has_outcome, 0) = 1"
        player_outcome_join = f"LEFT JOIN _matchup_outcomes mo ON {key_join}"
    else:
        pf_matchup_outcome = "FALSE"
        player_outcome_join = ""
    pf_outcome_known_with_matchup = f"({pf_outcome_known} OR {pf_matchup_outcome})"

    def qualify_player(expr: str) -> str:
        for column in sorted(pf_cols, key=len, reverse=True):
            expr = re.sub(rf"\b{re.escape(column)}\b", f'f."{column}"', expr)
        return expr

    p_pf_id = qualify_player(pf_id)
    p_pf_rostered = qualify_player(pf_rostered)
    p_pf_started = qualify_player(pf_started)
    p_pf_clutch = qualify_player(pf_clutch)
    p_pf_outcome = qualify_player(pf_outcome_known_with_matchup)
    p_pf_playoff = qualify_player(pf_playoff_signal)
    p_pf_champ_signal = qualify_player(pf_champ_signal)
    p_pf_champ = qualify_player(pf_champ)
    p_year_pred = year_pred.replace("CAST(year", "CAST(f.year")

    # Base population: a settings row with no player rows is not a real denominator
    # population.  Keep it visible, but never schedule it for a rescue.
    # Preserve the platform's native per-season league identifier.  The folded
    # DB name is not sufficient for historical MFL: one DB can contain many
    # seasons, each with a different league ID.  Targeted source rescues must
    # query this value with the target year.
    source_id_expr = (
        "MAX(CAST(league_key AS VARCHAR)) AS source_id"
        if "league_key" in ls_cols else "CAST(NULL AS VARCHAR) AS source_id"
    )
    base = f"""
      SELECT CAST(db_name AS VARCHAR) AS db_name, CAST(year AS INTEGER) AS year,
             COUNT(*) AS settings_rows,
             MAX(CAST(platform AS VARCHAR)) AS platform,
             {source_id_expr},
             MAX(CAST(num_teams AS INTEGER)) AS num_teams,
             MAX(CAST(playoff_teams AS INTEGER)) AS playoff_teams,
             MAX(CAST(playoff_start_week AS INTEGER)) AS playoff_start_week
      FROM {prefix}.league_settings
      WHERE {year_pred}
      GROUP BY 1,2
    """
    player = f"""
      SELECT CAST(f.db_name AS VARCHAR) AS db_name, CAST(f.year AS INTEGER) AS year,
             COUNT(*) AS player_rows,
             COUNT(*) FILTER (WHERE {p_pf_id}) AS identified_player_rows,
             COUNT(*) FILTER (WHERE {p_pf_rostered} AND {p_pf_id}) AS identified_rostered_rows,
             COUNT(DISTINCT CAST(f.week AS INTEGER)) AS player_weeks,
             COUNT(*) FILTER (WHERE {p_pf_started}) AS started_rows,
             COUNT(DISTINCT CAST(f.week AS INTEGER)) FILTER (WHERE {p_pf_started}) AS started_weeks,
             COUNT(*) FILTER (WHERE {p_pf_started} AND {p_pf_clutch}) AS clutch_started_rows,
             COUNT(DISTINCT CAST(f.week AS INTEGER)) FILTER (WHERE {p_pf_started} AND {p_pf_clutch}) AS clutch_started_weeks,
             COUNT(*) FILTER (WHERE {p_pf_started} AND NOT ({p_pf_clutch})) AS started_without_clutch_rows,
             COUNT(DISTINCT CAST(f.week AS INTEGER)) FILTER (WHERE {p_pf_started} AND NOT ({p_pf_clutch})) AS started_without_clutch_weeks,
             COUNT(*) FILTER (WHERE {p_pf_started} AND NOT ({p_pf_outcome})) AS started_without_outcome_rows,
             COUNT(*) FILTER (WHERE {p_pf_started} AND ({p_pf_outcome})) AS player_outcome_started_rows,
             MAX(CAST(f.week AS INTEGER)) AS max_player_week,
             list_sort(list_distinct(list(CAST(f.week AS INTEGER)) FILTER (
                 WHERE {p_pf_started} AND (NOT ({p_pf_clutch}) OR NOT ({p_pf_outcome}))
             ))) AS missing_started_weeks,
             COUNT(*) FILTER (WHERE {p_pf_playoff}) AS player_playoff_signal_rows,
             COUNT(*) FILTER (WHERE {p_pf_champ_signal}) AS player_championship_signal_rows,
             COUNT(*) FILTER (WHERE {p_pf_champ}) AS champion_rows,
             COUNT(DISTINCT CAST(f.week AS INTEGER)) FILTER (WHERE {p_pf_champ}) AS champion_weeks
      FROM {prefix}.player_fantasy f
      {player_outcome_join}
      WHERE {p_year_pred}
      GROUP BY 1,2
    """

    m_year = f"CAST(m.year AS INTEGER) IN ({','.join(str(y) for y in sorted(years))})" if years else "TRUE"
    outcome_parts = [f"m.{c} IS NOT NULL" for c in ("win", "loss", "tie") if c in m_cols]
    if {"opponent", "opponent_points"} <= m_cols:
        outcome_parts.append("m.opponent IS NOT NULL AND m.opponent_points IS NOT NULL")
    m_outcome = " OR ".join(outcome_parts) or "FALSE"
    m_playoff = "m.is_playoffs=1" if "is_playoffs" in m_cols else "FALSE"
    m_final_seed = "m.final_playoff_seed IS NOT NULL" if "final_playoff_seed" in m_cols else "FALSE"
    m_champion = "m.champion=1" if "champion" in m_cols else "FALSE"
    m_champ_signal_terms = [
        "CAST(m.championship AS INTEGER)=1" if "championship" in m_cols else None,
        "CAST(m.is_championship AS INTEGER)=1" if "is_championship" in m_cols else None,
    ]
    m_champ_signal = " OR ".join(x for x in m_champ_signal_terms if x) or "FALSE"
    # Matchup is the independent outcome/playoff lane.  A champion marker is also a
    # playoff signal: starting a championship game necessarily means that playoffs ran.
    matchup = f"""
      SELECT CAST(m.db_name AS VARCHAR) AS db_name, CAST(m.year AS INTEGER) AS year,
             COUNT(*) AS matchup_rows,
             COUNT(*) FILTER (WHERE {m_outcome}) AS matchup_outcome_rows,
             COUNT(*) FILTER (WHERE {m_playoff}) AS playoff_matchup_rows,
             COUNT(DISTINCT m.manager) FILTER (WHERE {m_playoff}) AS playoff_managers,
             COUNT(*) FILTER (WHERE {m_final_seed}) AS final_seed_rows,
             COUNT(*) FILTER (WHERE {m_champion}) AS matchup_champion_rows,
             COUNT(*) FILTER (WHERE {m_champ_signal}) AS matchup_championship_signal_rows
      FROM {prefix}.matchup m
      WHERE {m_year}
      GROUP BY 1,2
    """ if {"db_name", "year"} <= m_cols else """
      SELECT NULL::VARCHAR AS db_name, NULL::INTEGER AS year,
             0::BIGINT AS matchup_rows, 0::BIGINT AS matchup_outcome_rows,
             0::BIGINT AS playoff_matchup_rows, 0::BIGINT AS playoff_managers,
             0::BIGINT AS final_seed_rows, 0::BIGINT AS matchup_champion_rows,
             0::BIGINT AS matchup_championship_signal_rows
      WHERE FALSE
    """

    # Lightweight source checks.  These are intentionally source-level, not a reimplementation
    # of the full builder: a zero means the denominator lane cannot possibly be constructed.
    d_year = f"CAST(year AS INTEGER) IN ({','.join(str(y) for y in sorted(years))})" if years else "TRUE"
    derived = f"""
      WITH ids AS (
        SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name, CAST(year AS INTEGER) AS year,
               CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
        FROM {prefix}.player_fantasy WHERE {d_year} AND NFL_player_id IS NOT NULL
      ), id_cov AS (
        SELECT i.db_name, i.year, COUNT(*) AS identified_source_ids,
               COUNT(*) FILTER (WHERE pos.NFL_player_id IS NOT NULL) AS position_join_ids
        FROM ids i LEFT JOIN _ops_pos pos USING (NFL_player_id,year) GROUP BY 1,2
      ), started AS (
        SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name, CAST(year AS INTEGER) AS year,
               CAST(NFL_player_id AS VARCHAR) AS NFL_player_id, CAST(week AS INTEGER) AS week
        FROM {prefix}.player_fantasy WHERE {d_year} AND NFL_player_id IS NOT NULL
          AND week IS NOT NULL AND {pf_started}
      ), week_cov AS (
        SELECT s.db_name, s.year,
               COUNT(*) FILTER (WHERE a.NFL_player_id IS NOT NULL) AS active_join_started_weeks,
               COUNT(*) FILTER (WHERE g.year IS NOT NULL) AS team_game_join_started_weeks,
               0::BIGINT AS started_without_team_game_rows
        FROM started s
        LEFT JOIN _ops_active a USING (NFL_player_id,year,week)
        LEFT JOIN _ops_team_weeks g USING (year,week)
        GROUP BY 1,2
      )
      SELECT i.db_name, i.year, i.identified_source_ids,
             i.position_join_ids AS position_join_rows,
             COALESCE(w.active_join_started_weeks,0) AS active_join_rows,
             COALESCE(w.team_game_join_started_weeks,0) AS team_game_join_rows,
             COALESCE(w.started_without_team_game_rows,0) AS started_without_team_game_rows
      FROM id_cov i LEFT JOIN week_cov w USING (db_name,year)
    """

    con.execute("CREATE OR REPLACE TEMP TABLE _inv AS " + base)
    con.execute("CREATE OR REPLACE TEMP TABLE _player AS " + player)
    con.execute("CREATE OR REPLACE TEMP TABLE _matchup AS " + matchup)
    con.execute("CREATE OR REPLACE TEMP TABLE _derived AS " + derived)

    rows = []
    query = """
      SELECT b.*, p.* EXCLUDE (db_name,year), m.* EXCLUDE (db_name,year),
             d.* EXCLUDE (db_name,year)
      FROM _inv b
      LEFT JOIN _player p USING (db_name,year)
      LEFT JOIN _matchup m USING (db_name,year)
      LEFT JOIN _derived d USING (db_name,year)
      ORDER BY year, db_name
    """
    names = [r[0] for r in con.execute("DESCRIBE (" + query + ")").fetchall()]
    for values in con.execute(query).fetchall():
        row = dict(zip(names, values))
        reasons = []
        populated = int(row.get("player_rows") or 0) > 0
        identified = int(row.get("identified_rostered_rows") or 0) > 0
        if not populated or not identified:
            reasons.append("settings_only_or_no_identified_rostered_players")
        else:
            if int(row.get("started_without_clutch_rows") or 0) > 0:
                reasons.append("started_rows_missing_clutch")
            if int(row.get("started_without_outcome_rows") or 0) > 0:
                reasons.append("started_rows_missing_win_outcome")
            # A separate matchup row is optional when player_fantasy contains the
            # complete outcome fallback consumed by the builder.  Rescue only when
            # neither source can supply started-league outcomes.
            if (int(row.get("matchup_rows") or 0) == 0
                    and int(row.get("player_outcome_started_rows") or 0) == 0):
                reasons.append("no_matchup_rows")
            if (int(row.get("matchup_championship_signal_rows") or 0)
                    + int(row.get("player_championship_signal_rows") or 0) == 0):
                reasons.append("no_championship_signal")
            if (int(row.get("playoff_matchup_rows") or 0)
                    + int(row.get("final_seed_rows") or 0)
                    + int(row.get("champion_rows") or 0)
                    + int(row.get("matchup_champion_rows") or 0)
                    + int(row.get("player_playoff_signal_rows") or 0) == 0):
                reasons.append("no_playoff_signal")
            if int(row.get("started_without_team_game_rows") or 0) > 0:
                reasons.append("started_rows_missing_bye_aware_team_game")
            if int(row.get("position_join_rows") or 0) == 0:
                reasons.append("no_position_eligibility_source")
            if int(row.get("team_game_join_rows") or 0) == 0:
                reasons.append("no_team_game_denominator_source")
        row["populated_league_year"] = populated and identified
        row["needs_rescue"] = bool(populated and identified and reasons)
        target_weeks = set(int(w) for w in (row.get("missing_started_weeks") or []) if w is not None)
        if "no_playoff_signal" in reasons or "no_championship_signal" in reasons:
            playoff_start = int(row.get("playoff_start_week") or (15 if int(row["year"]) < 2021 else 18))
            max_week = int(row.get("max_player_week") or playoff_start)
            target_weeks.update(range(playoff_start, max_week + 1))
        row["target_weeks"] = sorted(target_weeks)
        row["reason_codes"] = reasons
        rows.append(row)
    con.close()
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=Path("research_public_lake"))
    ap.add_argument("--years", default="")
    ap.add_argument("--out", type=Path, default=Path("out"))
    args = ap.parse_args()
    years = {int(x) for x in args.years.split(",") if x.strip()} if args.years else set()
    rows = inventory(args.root, years)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "matchup_league_year_inventory.json").write_text(
        json.dumps(rows, indent=2, default=str) + "\n", encoding="utf-8"
    )
    fields = sorted({key for row in rows for key in row})
    with (args.out / "matchup_league_year_inventory.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v) if isinstance(v, list) else v for k, v in row.items()})
    rescue = [r for r in rows if r["needs_rescue"]]
    target = [
        {
            "db_name": r["db_name"],
            "year": r["year"],
            "platform": r.get("platform"),
            "source_id": r.get("source_id"),
            "weeks": r.get("target_weeks") or [],
            "reason_codes": r["reason_codes"],
        }
        for r in rescue
    ]
    (args.out / "matchup_rescue_targets.json").write_text(json.dumps(target, indent=2) + "\n", encoding="utf-8")
    print(f"league-years inventoried: {len(rows):,}")
    print(f"populated league-years: {sum(bool(r['populated_league_year']) for r in rows):,}")
    print(f"settings-only/unidentified excluded: {sum(not bool(r['populated_league_year']) for r in rows):,}")
    print(f"targeted rescue league-years: {len(rescue):,}")
    for reason, count in sorted({reason: sum(reason in r['reason_codes'] for r in rescue) for r in rescue for reason in r['reason_codes']}.items()):
        print(f"  {reason}: {count:,}")


if __name__ == "__main__":
    main()
