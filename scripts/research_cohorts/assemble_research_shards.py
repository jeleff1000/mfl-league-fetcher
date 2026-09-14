"""Losslessly assemble disjoint public/private research cohort shards.

Every rate and mean is recalculated from additive sufficient statistics. Inputs must represent
mutually disjoint league populations; year ranges may be split across any number of files.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

import duckdb

try:
    from .matchup_metric_sql import season_expected_outcome_sql
except ImportError:
    from matchup_metric_sql import season_expected_outcome_sql

FORMAT = ["league_type", "lineup_mode", "keeper_mode"]
BASE = ["teams", "roster", "ppr", "td", "bracket", *FORMAT]
SEASON_KEY = [*BASE, "cohort_level", "format_level", "year", "NFL_player_id"]
WEEK_KEY = [*BASE, "cohort_level", "format_level", "year", "week", "NFL_player_id"]
# Matchup is position-scoped.  Keep the generic transaction keys above unchanged, but
# carry pos_grp through every matchup aggregation so QB/RB/WR/TE/K/DEF rows cannot merge
# during shard assembly.
MATCHUP_SEASON_KEY = [*SEASON_KEY, "pos_grp"]
MATCHUP_WEEK_KEY = [*WEEK_KEY, "pos_grp"]

MATCHUP_WEEKLY_STATS = {
    "rostered_leagues", "started_leagues", "healthy_started_leagues",
    "roster_eligible_leagues", "team_game_eligible_leagues", "healthy_eligible_leagues",
    "wins_started",
    "losses_started", "points_started", "clutch_sum", "champ_started",
    "champ_eligible", "n_leagues", "avg_lamar_started",
}
MATCHUP_SEASON_STATS = {
    "n_rostered_leagues", "rostered_weeks", "rostered_active_weeks",
    "started_team_game_weeks", "started_active_weeks",
    "roster_eligible_league_weeks", "team_game_eligible_league_weeks",
    "healthy_eligible_league_weeks", "elig_league_weeks", "wins_started_active", "started_champ_active",
    "champ_elig_leagues", "sum_champ", "sum_pts_started",
    "sum_clutch_started_active", "sum_clutch_started", "sum_lamar_started",
    "canon_num_sr", "canon_den_sr", "canon_num_abs", "n_lg_started",
    "wins_started", "losses_started", "po_started_weeks", "po_wins_started",
    "reg_started_weeks", "reg_wins_started", "sum_clutch_po", "po_wkwt_credit",
    "n_final_po", "po_n_rostered_leagues", "n_started_po", "playoff_eligible_leagues", "started_weeks",
    "n_leagues", "total_points_observed",
    # T7/T8 (2026-07-26): league-counted champ numerators, the active-scoped playoff-start
    # count, and the inactive-week mask. Listing them here is fail-closed on purpose -- a
    # pre-T7 shard is REJECTED rather than union_by_name-NULLed into a silent zero.
    "n_champ_leagues", "n_champ_start_leagues", "po_started_active_weeks",
    "inactive_week_mask", "active_week_mask",
}
# Not SUM-able. A week is one week however many leagues rostered the player, so the
# inactive-week SET is combined with BIT_OR across disjoint league shards and counted once.
MATCHUP_SEASON_BITSETS = {"inactive_week_mask", "active_week_mask"}
TRANSACTION_SEASON_STATS = {
    "n_add_leagues", "n_drop_leagues", "n_leagues", "sum_faab_pct",
    "n_faab_pct", "sum_faab_bid", "n_faab_bid", "sum_transaction_score",
    "n_transaction_score", "sum_add_lamar", "n_add_lamar",
    "sum_drop_regret", "n_drop_regret",
}
TRANSACTION_WEEKLY_STATS = {
    "n_add_lg", "n_leagues", "sum_add_lamar", "n_add_lamar",
    "sum_faab_pct", "n_faab_pct", "sum_faab_bid", "n_faab_bid",
}


def _connection(out_dir: Path) -> duckdb.DuckDBPyConnection:
    """Create a spill-capable assembler connection for full-lake artifacts."""
    temp_dir = out_dir / ".duckdb_tmp_assemble"
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    memory_mb = os.environ.get("RESEARCH_ASSEMBLE_MEMORY_MB", "3500")
    threads = os.environ.get("RESEARCH_ASSEMBLE_THREADS", "2")
    con.execute(f"SET memory_limit='{memory_mb}MB'")
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
    return con


def _quoted(paths: Iterable[Path]) -> str:
    found = sorted({str(Path(p).resolve()).replace("\\", "/") for p in paths if Path(p).is_file()})
    if not found:
        raise ValueError("no shard files supplied")
    return "[" + ",".join("'" + p.replace("'", "''") + "'" for p in found) + "]"


def _read(paths: Iterable[Path]) -> str:
    return f"read_parquet({_quoted(paths)}, union_by_name=true)"


def _require_columns(
    con: duckdb.DuckDBPyConnection,
    paths: Iterable[Path],
    required: Iterable[str],
    label: str,
) -> None:
    """Reject partial-schema shards before union_by_name can NULL-fill them."""
    required_set = set(required)
    failures = []
    for path in sorted({Path(p).resolve() for p in paths if Path(p).is_file()}):
        quoted = path.as_posix().replace("'", "''")
        columns = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{quoted}')"
            ).fetchall()
        }
        missing = sorted(required_set - columns)
        if missing:
            failures.append(f"{', '.join(missing)} in {path.as_posix()}")
    if failures:
        raise ValueError(f"{label} shard missing required columns: {'; '.join(failures)}")


def check_population_cover(
    con: duckdb.DuckDBPyConnection,
    roots: list[Path],
    table: str,
    allow_unmanifested: bool = False,
) -> None:
    """Refuse an input set that does not partition the league population exactly once.

    Every assembled number is a SUM over shards, which is only lossless when the shards are
    disjoint AND complete. Two failure modes, both silent in the output:

      * DOUBLE COVER -- a whole-year shard assembled alongside that year's hash buckets.
        Every count and every denominator doubles. Rates survive (numerator and denominator
        double together), which is exactly why it is so hard to spot: the boards look right
        and n_leagues is nonsense.
      * PARTIAL COVER -- a rescue bucket that failed and nobody noticed. Leagues vanish from
        the denominator, so every rate for that year is computed over the wrong population.

    Shards stamp their coordinates in shard_manifest.parquet (see the builder). Missing
    manifests fail closed: a pre-manifest artifact set cannot be proven disjoint, and
    assuming it is has already cost us once.
    """
    manifests = sorted({p.resolve() for root in roots for p in Path(root).rglob("shard_manifest.parquet")})
    if not manifests:
        if allow_unmanifested:
            print("[cover] WARNING: no shard manifests found; population cover UNVERIFIED "
                  "(--allow-unmanifested). Double-counted years will not be caught.")
            return
        raise ValueError(
            "no shard_manifest.parquet found in the input roots -- cannot prove the shards "
            "partition the population exactly once. Rebuild the shards with a builder that "
            "emits a manifest, or pass --allow-unmanifested to assemble unverified.")
    quoted = "[" + ",".join("'" + p.as_posix().replace("'", "''") + "'" for p in manifests) + "]"
    rows = con.execute(
        f"SELECT \"table\", year_start, year_end, bucket, buckets, "
        f"fingerprint, target_hash "
        f"FROM read_parquet({quoted}, union_by_name=true)").fetchall()

    # A clean bucket cover is not sufficient if the shards were produced from
    # different lake snapshots.  The rates can still look plausible while the
    # numerators and denominators come from different populations.  Every
    # current builder stamps a source fingerprint; fail closed for legacy or
    # hand-created manifests instead of silently mixing them.
    fingerprints = {row[5] for row in rows if row[0] == table}
    if None in fingerprints or "" in fingerprints:
        raise ValueError(
            f"{table} shard manifests are missing source fingerprints; "
            "rebuild the shards from the same canonical lake before assembling"
        )
    if len(fingerprints) > 1:
        raise ValueError(
            f"{table} shard manifests come from mixed source fingerprints: "
            f"{sorted(fingerprints)}"
        )
    target_hashes = {row[6] for row in rows if row[0] == table and row[6]}
    if len(target_hashes) > 1:
        raise ValueError(
            f"{table} targeted shard manifests use mixed target inventories: "
            f"{sorted(target_hashes)}"
        )

    per_year: dict[int, list[tuple]] = {}
    for tbl, y0, y1, bucket, buckets, _fingerprint, _target_hash in rows:
        if tbl != table:
            continue
        for year in range(int(y0), int(y1) + 1):
            per_year.setdefault(year, []).append((bucket, buckets))
    if not per_year:
        raise ValueError(f"shard manifests cover no {table} years")

    problems = []
    for year in sorted(per_year):
        entries = per_year[year]
        whole = [e for e in entries if e[0] is None]
        bucketed = [e for e in entries if e[0] is not None]
        if whole and bucketed:
            problems.append(
                f"{year}: covered BOTH by a whole-population shard and by "
                f"{len(bucketed)} bucket shard(s) -- every count would double")
        elif len(whole) > 1:
            problems.append(f"{year}: {len(whole)} whole-population shards -- duplicated")
        elif bucketed:
            counts = {e[1] for e in bucketed}
            if len(counts) > 1:
                problems.append(f"{year}: mixed bucket counts {sorted(counts)}")
                continue
            expected = counts.pop()
            seen = sorted(e[0] for e in bucketed)
            if len(set(seen)) != len(seen):
                problems.append(f"{year}: repeated bucket ids {seen}")
            elif set(seen) != set(range(expected)):
                missing = sorted(set(range(expected)) - set(seen))
                problems.append(
                    f"{year}: buckets {missing} MISSING of {expected} -- those leagues would "
                    f"drop out of every denominator")
    if problems:
        raise ValueError("shard population cover is not a clean partition:\n  "
                         + "\n  ".join(problems))
    span = f"{min(per_year)}-{max(per_year)}"
    print(f"[cover] {table}: {len(per_year)} years ({span}) each covered exactly once "
          f"across {len(manifests)} shards")


def _confidence(n: str = "SUM(n_leagues)") -> str:
    return f"""CASE WHEN {n} >= 35 THEN 'confident'
                WHEN {n} >= 10 THEN 'mushy' ELSE 'insufficient' END"""


def assemble_matchup(
    season_paths: Iterable[Path], weekly_paths: Iterable[Path], out_dir: Path,
    roots: list[Path] | None = None, allow_unmanifested: bool = False,
    denom_season: Path | None = None, denom_week: Path | None = None,
    pos_grp: Path | None = None,
) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    season_out = out_dir / "research_matchup_player_season.parquet"
    weekly_out = out_dir / "research_matchup_weekly.parquet"
    con = _connection(out_dir)
    if roots:
        check_population_cover(con, roots, "matchup", allow_unmanifested)
    _require_columns(con, weekly_paths, [*MATCHUP_WEEK_KEY, *MATCHUP_WEEKLY_STATS], "weekly matchup")
    _require_columns(con, season_paths, [*MATCHUP_SEASON_KEY, *MATCHUP_SEASON_STATS], "season matchup")
    season_schema = {
        row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM {_read(season_paths)} LIMIT 0"
        ).fetchall()
    }
    con.execute(f"CREATE VIEW sw AS SELECT * FROM {_read(weekly_paths)}")
    weekly_schema = {
        row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM {_read(weekly_paths)} LIMIT 0"
        ).fetchall()
    }
    # Active wins are a legacy diagnostic, but they must use the same active-week scope as
    # the active-start denominator.  Older compatibility fixtures do not carry this field;
    # those fall back to the season primitive below.
    weekly_active_wins_sum = (
        "SUM(active_wins_started) AS weekly_active_wins_started"
        if "active_wins_started" in weekly_schema
        else "CAST(NULL AS DOUBLE) AS weekly_active_wins_started"
    )
    weekly_active_wins_source = (
        "SUM(active_wins_started) AS active_wins_started"
        if "active_wins_started" in weekly_schema
        else "CAST(NULL AS DOUBLE) AS active_wins_started"
    )
    weekly_clutch_eligible_max = (
        "MAX(clutch_eligible_leagues) AS clutch_eligible_leagues"
        if "clutch_eligible_leagues" in weekly_schema
        else "CAST(NULL AS BIGINT) AS clutch_eligible_leagues"
    )
    wk = ",".join(MATCHUP_WEEK_KEY)

    # ---- authoritative eligible-league denominators -------------------------------------
    # A denominator is a property of LEAGUE SETTINGS, not of any player, so it is not
    # shardable. Summing each bucket's own count gave every player a denominator covering
    # only the buckets he happened to be rostered in: measured 2026-07-27, ~51% of rows in
    # every bucketed year, averaging 0.68-0.71 of the true population and bottoming at
    # 0.004 -- which inverts the self-flooring the eligible-leagues denominator exists for.
    # The numerators are unaffected (additive league counts, summed correctly), so only the
    # divisor is replaced here, from build_population_denominators.py.
    use_pop = bool(denom_week and denom_season and pos_grp)
    if use_pop:
        # Older ALL-format years can emit the same grouping-set cell more than once.
        # The denominator is a lookup lattice, so duplicate keys would fan out every
        # shard row during the join and corrupt both runtime and aggregates.
        con.execute(f"CREATE VIEW dweek AS SELECT DISTINCT * FROM read_parquet('{Path(denom_week).as_posix()}')")
        con.execute(f"CREATE VIEW dseason AS SELECT DISTINCT * FROM read_parquet('{Path(denom_season).as_posix()}')")
        con.execute(f"CREATE VIEW pgrp AS SELECT * FROM read_parquet('{Path(pos_grp).as_posix()}')")
    dseason_schema = (
        {row[0] for row in con.execute("DESCRIBE dseason").fetchall()}
        if use_pop else set()
    )
    elig_expr = "d.n_lg" if use_pop else "s.shard_eligible_leagues"
    champ_elig_expr = "d.n_champ_lg" if use_pop else "s.shard_champ_eligible"
    # Clutch uses the season-level champion-observed population.  The weekly
    # `champ_eligible` field is intentionally week-specific and belongs to the
    # championship-start diagnostic, not to Clutch normalization.
    clutch_elig_expr = (
        "ds.n_leagues_champ" if use_pop and "n_leagues_champ" in dseason_schema
        else "s.shard_champ_eligible"
    )
    nlg_expr = "ds.n_leagues" if use_pop else "s.shard_n_leagues"
    # Additive exposure columns are required in current shards.  Keep the legacy
    # league-count fallback only so the small compatibility fixtures can still exercise
    # the assembler; real releases take the weighted branch.
    season_rostered_num = (
        "r.rostered_league_weeks" if "rostered_league_weeks" in season_schema
        else "r.n_rostered_leagues"
    )
    season_rostered_den = (
        "r.roster_eligible_league_weeks" if "roster_eligible_league_weeks" in season_schema
        else "dsx.n_leagues" if use_pop else "r.n_leagues"
    )
    season_start_fallback_num = (
        "r.started_team_game_weeks" if "started_team_game_weeks" in season_schema
        else "r.started_active_weeks"
    )

    dim_join = " AND ".join(f"d.{c}=s.{c}" for c in BASE)
    sdim_join = " AND ".join(f"ds.{c}=s.{c}" for c in BASE)
    # Stage 1: numerators only. These ARE additive -- each shard counts its own disjoint
    # leagues, so summing is exact.
    con.execute(f"""
      CREATE TABLE weekly_sums AS
      SELECT {wk},
        SUM(rostered_leagues) AS rostered_leagues,
        SUM(started_leagues) AS started_leagues,
        SUM(healthy_started_leagues) AS healthy_started_leagues,
        SUM(wins_started) AS wins_started,
        {weekly_active_wins_source},
        SUM(losses_started) AS losses_started,
        SUM(points_started) AS points_started,
        SUM(clutch_sum) AS clutch_sum,
        SUM(champ_started) AS champ_started,
        {weekly_clutch_eligible_max},
        MAX(avg_lamar_started) AS avg_lamar_started,
        -- The corrected shard schema names this denominator by its meaning:
        -- team_game_eligible_leagues.  Keep the assembler's legacy derived alias
        -- `eligible_leagues` below, but source it from the canonical field.
        SUM(team_game_eligible_leagues) AS shard_eligible_leagues,
        MAX(CASE WHEN healthy_eligible_leagues > 0 THEN 1 ELSE 0 END) AS player_active,
        SUM(champ_eligible) AS shard_champ_eligible,
        SUM(n_leagues) AS shard_n_leagues
      FROM sw GROUP BY {wk}""")
    # Stage 2: resolve the denominator, then divide. Kept separate from stage 1 so the
    # divisor is a JOIN against the population, never an aggregate over the shards.
    joins = ""
    if use_pop:
        joins = (
            f" LEFT JOIN dweek d ON {dim_join} AND d.year=s.year AND d.week=s.week"
            "   AND d.pos_grp=s.pos_grp"
            f" LEFT JOIN dseason ds ON {sdim_join} AND ds.year=s.year"
            "   AND ds.pos_grp=s.pos_grp")
    con.execute(f"""
      CREATE TABLE weekly_base AS
      SELECT s.*, {elig_expr} AS eligible_leagues,
             {elig_expr} AS roster_eligible_leagues,
             {elig_expr} AS team_game_eligible_leagues,
             CASE WHEN s.player_active=1 THEN {elig_expr} ELSE 0 END AS healthy_eligible_leagues,
             {champ_elig_expr} AS champ_eligible,
             {clutch_elig_expr} AS clutch_eligible_leagues,
             {nlg_expr} AS n_leagues
      FROM weekly_sums s{joins}""")
    if use_pop:
        miss, total = con.execute("""SELECT
            COUNT(*) FILTER (WHERE eligible_leagues IS NULL), COUNT(*) FROM weekly_base"""
        ).fetchone()
        if miss:
            raise ValueError(
                f"population denominator missing for {miss:,} of {total:,} weekly rows -- "
                "the denominator lattice does not cover the shards' cohort cells; rebuild "
                "it from the same lake before assembling")
        print(f"[denom] weekly: population denominator resolved for all {total:,} rows")
    con.execute(f"""
      CREATE TABLE weekly AS
      SELECT {wk},
        100.0*rostered_leagues/NULLIF(roster_eligible_leagues,0) AS roster_rate_pct,
        100.0*started_leagues/NULLIF(eligible_leagues,0) AS start_rate_pct,
        100.0*healthy_started_leagues/NULLIF(healthy_eligible_leagues,0) AS healthy_start_rate_pct,
        100.0*wins_started/NULLIF(started_leagues,0) AS win_rate_pct,
        1.0*started_leagues/NULLIF(eligible_leagues,0) AS expected_starts,
        1.0*started_leagues/NULLIF(eligible_leagues,0)
             * wins_started/NULLIF(started_leagues,0) AS expected_wins,
        1.0*started_leagues/NULLIF(eligible_leagues,0)
             * (1.0 - wins_started/NULLIF(started_leagues,0)) AS expected_losses,
        100.0*started_leagues/NULLIF(eligible_leagues,0)
             * wins_started/NULLIF(started_leagues,0) AS won_pct,
        100.0*started_leagues/NULLIF(eligible_leagues,0)
             * (1.0 - wins_started/NULLIF(started_leagues,0)) AS lost_pct,
        points_started/NULLIF(started_leagues,0) AS ppg_when_started,
        avg_lamar_started,
        avg_lamar_started*started_leagues/NULLIF(eligible_leagues,0) AS total_lamar_started,
        -- Clutch is normalized by leagues with a player-eligible championship
        -- signal, not by the week-specific Champ-start diagnostic denominator.
        clutch_sum/NULLIF(COALESCE(clutch_eligible_leagues, champ_eligible),0) AS avg_clutch_started,
        avg_lamar_started*started_leagues/NULLIF(eligible_leagues,0) AS lamar_weighted,
        clutch_sum/NULLIF(COALESCE(clutch_eligible_leagues, champ_eligible),0) AS clutch_weighted,
        rostered_leagues::BIGINT AS rostered_leagues,
        started_leagues::BIGINT AS started_leagues,
        healthy_started_leagues::BIGINT AS healthy_started_leagues,
        roster_eligible_leagues::BIGINT AS roster_eligible_leagues,
        team_game_eligible_leagues::BIGINT AS team_game_eligible_leagues,
        healthy_eligible_leagues::BIGINT AS healthy_eligible_leagues,
        eligible_leagues::BIGINT AS eligible_leagues,
        wins_started::BIGINT AS wins_started,
        active_wins_started::BIGINT AS active_wins_started,
        losses_started::BIGINT AS losses_started,
        points_started,
        clutch_sum,
        champ_started::BIGINT AS champ_started,
        champ_eligible::BIGINT AS champ_eligible,
        clutch_eligible_leagues::BIGINT AS clutch_eligible_leagues,
        n_leagues::BIGINT AS n_leagues,
        {_confidence("n_leagues")} AS confidence
      FROM weekly_base
    """)
    con.execute(f"COPY weekly TO '{weekly_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    con.execute(f"CREATE VIEW ss AS SELECT * FROM {_read(season_paths)}")
    # The merged weekly table is authoritative for expected starts/wins/losses: these are
    # sums of combined weekly rates, not sums of independently-normalized shard rates.
    # Roll weekly observations up to the season key (including pos_grp, but not week).
    season_week_key = ",".join(MATCHUP_SEASON_KEY)
    season_wins = season_expected_outcome_sql("weekly", "wins_started")
    season_losses = season_expected_outcome_sql("weekly", "losses_started")
    # Start exposure is position-eligible team-game leagues.  Use the canonical
    # field here instead of the legacy `eligible_leagues` compatibility alias.
    season_starts = (
        "SUM(1.0*weekly.started_leagues/"
        "NULLIF(weekly.team_game_eligible_leagues,0))"
    )
    con.execute(f"""
      CREATE TABLE wseason AS
      SELECT {season_week_key},
        100.0*SUM(started_leagues)
          /NULLIF(SUM(team_game_eligible_leagues),0) AS start_rate_pct,
        -- Diagnostic per-week expected W/L rates.  Expected W/L themselves are
        -- additive season quantities; these bounded lanes are only for rate checks.
        100.0*({season_wins})/NULLIF(COUNT(*),0) AS won_pct,
        100.0*({season_losses})/NULLIF(COUNT(*),0) AS lost_pct,
        100.0*{season_wins}/NULLIF({season_starts},0)
          AS win_rate_pct,
        {season_wins} AS expected_wins,
        {season_losses} AS expected_losses,
        {season_starts} AS expected_starts,
        SUM(total_lamar_started) AS total_lamar_started,
        SUM(clutch_sum)/NULLIF(SUM(COALESCE(clutch_eligible_leagues, champ_eligible)),0) AS avg_clutch_started,
        SUM(start_rate_pct) AS weekly_start_rate_sum,
        SUM(healthy_start_rate_pct) AS weekly_healthy_rate_sum,
        COUNT(*) AS active_weeks,
        SUM(points_started) AS weekly_points_started,
        SUM(started_leagues) AS weekly_started_leagues,
        SUM(team_game_eligible_leagues) AS weekly_eligible_leagues,
        SUM(healthy_started_leagues) AS weekly_healthy_started_leagues,
        {weekly_active_wins_sum},
        SUM(healthy_eligible_leagues) AS weekly_healthy_eligible_leagues,
        SUM(rostered_leagues) AS weekly_rostered_leagues,
        SUM(roster_eligible_leagues) AS weekly_roster_eligible_leagues,
        SUM(team_game_eligible_leagues) AS weekly_team_game_eligible_leagues,
        SUM(champ_started) AS weekly_champ_started,
        SUM(champ_eligible) AS weekly_champ_eligible,
        SUM(clutch_sum) AS weekly_clutch_sum
      FROM weekly WHERE eligible_leagues > 0 GROUP BY {season_week_key}
    """)
    sk = ",".join(MATCHUP_SEASON_KEY)
    active_wins_expr = "COALESCE(w.weekly_active_wins_started,r.wins_started_active)"
    con.execute(f"""
      CREATE TABLE sraw AS
      SELECT {sk},
        SUM(n_rostered_leagues) AS n_rostered_leagues,
        -- Preserve the canonical weighted-exposure name through the intermediate
        -- rollup.  The season query below deliberately uses rostered_league_weeks
        -- (league-weeks, not a distinct-league diagnostic); renaming it here to the
        -- legacy rostered_weeks name makes a real release fail at assembly.
        SUM(rostered_weeks) AS rostered_league_weeks,
        SUM(rostered_active_weeks) AS rostered_active_weeks,
        SUM(started_team_game_weeks) AS started_team_game_weeks,
        SUM(started_active_weeks) AS started_active_weeks,
        SUM(roster_eligible_league_weeks) AS roster_eligible_league_weeks,
        SUM(team_game_eligible_league_weeks) AS team_game_eligible_league_weeks,
        SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks,
        SUM(elig_league_weeks) AS elig_league_weeks,
        SUM(wins_started_active) AS wins_started_active,
        SUM(started_champ_active) AS started_champ_active,
        SUM(champ_elig_leagues) AS champ_elig_leagues,
        SUM(sum_champ) AS sum_champ,
        SUM(sum_pts_started) AS sum_pts_started,
        SUM(sum_clutch_started_active) AS sum_clutch_started_active,
        SUM(sum_clutch_started) AS sum_clutch_started,
        SUM(sum_lamar_started) AS sum_lamar_started,
        SUM(canon_num_sr) AS canon_num_sr,
        SUM(canon_den_sr) AS canon_den_sr,
        SUM(canon_num_abs) AS canon_num_abs,
        SUM(n_lg_started) AS n_lg_started,
        SUM(wins_started) AS wins_started,
        SUM(losses_started) AS losses_started,
        SUM(po_started_weeks) AS po_started_weeks,
        SUM(po_wins_started) AS po_wins_started,
        SUM(reg_started_weeks) AS reg_started_weeks,
        SUM(reg_wins_started) AS reg_wins_started,
        SUM(sum_clutch_po) AS sum_clutch_po,
        SUM(po_wkwt_credit) AS po_wkwt_credit,
        -- Preserve the playoff-membership closure even when a shard's final-week
        -- signal is absent but it contains a verified playoff start.
        SUM(GREATEST(n_final_po, n_started_po, n_champ_start_leagues)) AS n_final_po,
        SUM(po_n_rostered_leagues) AS po_n_rostered_leagues,
        -- A championship start is necessarily a playoff start.  Keep the
        -- league-counted playoff-start numerator closed even when the source
        -- playoff signal is absent on that championship row.
        SUM(GREATEST(n_started_po, n_champ_start_leagues)) AS n_started_po,
        SUM(playoff_eligible_leagues) AS playoff_eligible_leagues,
        SUM(started_weeks) AS started_weeks,
        SUM(n_leagues) AS n_leagues,
        SUM(total_points_observed) AS total_points_observed,
        SUM(n_champ_leagues) AS n_champ_leagues,
        SUM(n_champ_start_leagues) AS n_champ_start_leagues,
        SUM(po_started_active_weeks) AS po_started_active_weeks,
        BIT_OR(COALESCE(inactive_week_mask, 0)) AS inactive_week_mask,
        BIT_OR(COALESCE(active_week_mask, 0)) AS active_week_mask
      FROM ss GROUP BY {sk}
    """)
    using = ",".join(MATCHUP_SEASON_KEY)
    # the season denominator is the population count for this cohort cell and the
    # player's position group -- a JOIN, never a sum over shards
    if use_pop:
        season_nlg = "dsx.n_leagues"
        season_champ_nlg = "dsx.n_leagues_champ"
        # Playoff rates use the position-eligible leagues where playoff evidence
        # exists. The all-league denominator remains `dsx.n_leagues` for roster/start.
        season_po_nlg = "dsx.n_leagues_po"
        season_join = (
            " LEFT JOIN dseason dsx ON "
            + " AND ".join(f"dsx.{c}=r.{c}" for c in BASE)
            + " AND dsx.year=r.year AND dsx.pos_grp=r.pos_grp")
    else:
        season_nlg, season_champ_nlg, season_po_nlg, season_join = (
            "r.n_leagues", "r.champ_elig_leagues", "r.playoff_eligible_leagues", "")
    con.execute(f"""
      CREATE TABLE season AS
      SELECT r.{',r.'.join(MATCHUP_SEASON_KEY)},
         -- Season usage is the weighted roll-up of weekly exposure.  `n_rostered_leagues`
         -- is only a league-count diagnostic; using it here makes a 1-week league count
         -- the same as a full-season league and violates the weekly-to-season contract.
         100.0*COALESCE(w.weekly_rostered_leagues,{season_rostered_num})
           /NULLIF(COALESCE(w.weekly_roster_eligible_leagues,{season_rostered_den}),0) AS roster_rate_pct,
         COALESCE(w.start_rate_pct,
           100.0*{season_start_fallback_num}/NULLIF(r.team_game_eligible_league_weeks,0)) AS start_rate_pct,
        COALESCE(100.0*w.weekly_healthy_started_leagues/
                 NULLIF(w.weekly_healthy_eligible_leagues,0),
          100.0*r.started_active_weeks/NULLIF(r.healthy_eligible_league_weeks,0)) AS healthy_start_rate_pct,
        COALESCE(w.weekly_started_leagues,r.started_team_game_weeks) AS started_team_game_weeks,
        COALESCE(w.weekly_healthy_started_leagues,r.started_active_weeks) AS started_active_weeks,
        bit_count(COALESCE(r.active_week_mask,0)) AS nfl_active_weeks,
        COALESCE(w.weekly_eligible_leagues, r.elig_league_weeks) AS elig_league_weeks,
        -- fallback fires on the whole weekly lane, never per column (see the builder's
        -- note): a per-column COALESCE turned undecided starts into losses and broke
        -- won + lost = start_rate.
        CASE WHEN w.start_rate_pct IS NOT NULL THEN w.won_pct
             ELSE 100.0*r.wins_started/NULLIF(r.team_game_eligible_league_weeks,0) END AS won_pct,
        CASE WHEN w.start_rate_pct IS NOT NULL THEN w.lost_pct
             ELSE 100.0*r.losses_started/NULLIF(r.team_game_eligible_league_weeks,0) END AS lost_pct,
        -- Win% is the weekly weighted wins-in-starts rate: expected wins divided by
        -- expected starts.  Recomputing from raw decided-league counts here gives a
        -- different answer whenever eligible league support changes by week.
        COALESCE(100.0*w.expected_wins/NULLIF(w.expected_starts,0),
          100.0*r.wins_started/NULLIF(r.started_team_game_weeks,0)) AS win_rate_pct,
        w.expected_wins,w.expected_losses,w.expected_starts,
        100.0*COALESCE(w.weekly_started_leagues,r.started_weeks)
          /NULLIF(COALESCE(w.weekly_rostered_leagues,{season_rostered_num}),0) AS start_intensity_legacy_pct,
        r.sum_pts_started/NULLIF(r.started_weeks,0) AS ppg_when_started,
        COALESCE(w.weekly_points_started,r.total_points_observed) AS total_points_observed,
        -- both sides active-scoped (see the builder's note): the all-weeks numerator over
        -- an active-only denominator produced 1,700%.
        100.0*{active_wins_expr}/NULLIF(COALESCE(w.weekly_healthy_started_leagues,r.started_active_weeks),0)
          AS win_rate_started_pct,
        100.0*COALESCE(w.weekly_champ_started,r.started_champ_active)
          /NULLIF(COALESCE(w.weekly_champ_eligible,r.champ_elig_leagues),0) AS champ_week_rate_pct,
        COALESCE(w.weekly_champ_started,r.started_champ_active) AS started_champ_active,
        COALESCE(w.weekly_champ_eligible,r.champ_elig_leagues) AS champ_eligible_league_weeks,
        r.sum_champ,
        -- Clutch is a champion-signal metric.  Use the additive season clutch
        -- numerator over the season's distinct champion-eligible population;
        -- weekly normalization by all position-eligible leagues is not a valid
        -- fallback when champion coverage varies by week.
        COALESCE(r.sum_clutch_started_active/NULLIF({season_champ_nlg},0),
          w.avg_clutch_started) AS avg_clutch_started,
        r.sum_clutch_started/NULLIF(r.started_weeks,0) AS avg_clutch_when_started_legacy,
        r.canon_num_sr/NULLIF(r.canon_den_sr,0) AS avg_lamar_started,
        COALESCE(w.total_lamar_started,
          r.canon_num_abs/NULLIF(r.n_lg_started,0)) AS total_lamar_started,
        r.sum_lamar_started/NULLIF(r.started_weeks,0) AS avg_lamar_started_native_legacy,
        r.sum_lamar_started AS total_lamar_started_native_legacy,
        r.wins_started,r.losses_started,
        100.0*r.wins_started/NULLIF(r.wins_started+r.losses_started,0) AS record_win_pct,
        r.po_started_weeks,r.po_wins_started,
        100.0*r.po_wins_started/NULLIF(r.po_started_weeks,0) AS playoff_win_pct,
        100.0*r.reg_wins_started/NULLIF(r.reg_started_weeks,0) AS regular_win_pct,
        100.0*r.po_started_active_weeks/NULLIF(r.started_active_weeks,0)
          AS pct_starts_in_playoffs,
        r.sum_clutch_po/NULLIF(r.po_started_weeks,0) AS avg_clutch_playoff,
        100.0*r.po_wkwt_credit/NULLIF(r.po_n_rostered_leagues,0) AS playoff_rate_wkwt,
        100.0*r.n_final_po/NULLIF(r.po_n_rostered_leagues,0) AS playoff_rate_final,
        100.0*r.n_started_po/NULLIF(r.po_n_rostered_leagues,0) AS playoff_rate_started,
        r.po_n_rostered_leagues,r.n_started_po,
        -- Signal rates are defined only where the relevant signal exists.
        100.0*r.n_final_po/NULLIF({season_po_nlg},0) AS playoff_total_pct,
        100.0*GREATEST(r.n_started_po, r.n_champ_start_leagues)
          /NULLIF({season_po_nlg},0) AS playoff_as_starter_pct,
        100.0*r.n_champ_leagues/NULLIF({season_champ_nlg},0) AS champ_total_pct,
        100.0*r.n_champ_start_leagues/NULLIF({season_champ_nlg},0) AS champ_as_starter_pct,
        r.n_champ_leagues,r.n_champ_start_leagues,r.po_started_active_weeks,
        bit_count(COALESCE(r.inactive_week_mask,0)) AS inactive_weeks,
        r.inactive_week_mask,
        r.active_week_mask,
        w.weekly_start_rate_sum,w.weekly_healthy_rate_sum,w.active_weeks,
        r.n_rostered_leagues,
        COALESCE(w.weekly_rostered_leagues,r.rostered_league_weeks) AS rostered_league_weeks,
        r.rostered_active_weeks,
        COALESCE(w.weekly_roster_eligible_leagues,r.roster_eligible_league_weeks)
          AS roster_eligible_league_weeks,
        COALESCE(w.weekly_team_game_eligible_leagues,r.team_game_eligible_league_weeks)
          AS team_game_eligible_league_weeks,
        COALESCE(w.weekly_healthy_eligible_leagues,r.healthy_eligible_league_weeks)
          AS healthy_eligible_league_weeks,
        COALESCE(w.weekly_started_leagues,r.started_weeks) AS started_weeks,
        {season_nlg} AS n_leagues,
        {season_champ_nlg} AS champ_elig_leagues,
        {season_po_nlg} AS playoff_eligible_leagues,
        r.sum_pts_started,r.sum_clutch_started_active,{active_wins_expr} AS wins_started_active,
        r.sum_clutch_started,r.sum_lamar_started,r.reg_started_weeks,r.reg_wins_started,
        r.sum_clutch_po,r.canon_num_sr,r.canon_den_sr,r.canon_num_abs,r.n_lg_started,
        r.po_wkwt_credit,r.n_final_po,
        {_confidence(season_nlg)} AS confidence
      FROM sraw r LEFT JOIN wseason w USING ({using}){season_join}
    """)
    con.execute(f"COPY season TO '{season_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    return season_out, weekly_out


def assemble_transactions(paths: Iterable[Path], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "research_txn_player_season.parquet"
    con = _connection(out_dir)
    _require_columns(
        con, paths, [*SEASON_KEY, *TRANSACTION_SEASON_STATS], "season transaction"
    )
    key = ",".join(SEASON_KEY)
    con.execute(f"""
      CREATE TABLE final AS
      SELECT {key},
        100.0*SUM(n_add_leagues)/NULLIF(SUM(n_leagues),0) AS add_rate_pct,
        100.0*SUM(n_drop_leagues)/NULLIF(SUM(n_leagues),0) AS drop_rate_pct,
        100.0*(SUM(n_add_leagues)-SUM(n_drop_leagues))/NULLIF(SUM(n_leagues),0)
          AS net_add_rate_pct,
        SUM(sum_faab_pct)/NULLIF(SUM(n_faab_pct),0) AS avg_faab_pct,
        SUM(sum_faab_bid)/NULLIF(SUM(n_faab_bid),0) AS avg_faab_bid,
        SUM(sum_transaction_score)/NULLIF(SUM(n_transaction_score),0) AS avg_transaction_score,
        SUM(sum_add_lamar)/NULLIF(SUM(n_add_lamar),0) AS avg_add_lamar,
        SUM(sum_drop_regret)/NULLIF(SUM(n_drop_regret),0) AS avg_drop_regret,
        SUM(n_add_leagues)::BIGINT AS n_add_leagues,
        SUM(n_drop_leagues)::BIGINT AS n_drop_leagues,
        SUM(n_leagues)::BIGINT AS n_leagues,
        SUM(sum_faab_pct) AS sum_faab_pct,SUM(n_faab_pct)::BIGINT AS n_faab_pct,
        SUM(sum_faab_bid) AS sum_faab_bid,SUM(n_faab_bid)::BIGINT AS n_faab_bid,
        SUM(sum_transaction_score) AS sum_transaction_score,
        SUM(n_transaction_score)::BIGINT AS n_transaction_score,
        SUM(sum_add_lamar) AS sum_add_lamar,SUM(n_add_lamar)::BIGINT AS n_add_lamar,
        SUM(sum_drop_regret) AS sum_drop_regret,SUM(n_drop_regret)::BIGINT AS n_drop_regret,
        {_confidence()} AS confidence
      FROM {_read(paths)} GROUP BY {key}
    """)
    con.execute(f"COPY final TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    return out


def assemble_transactions_weekly(paths: Iterable[Path], out_dir: Path) -> Path:
    """Losslessly combine disjoint weekly transaction population shards."""
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "research_txn_weekly.parquet"
    con = _connection(out_dir)
    _require_columns(
        con, paths, [*WEEK_KEY, *TRANSACTION_WEEKLY_STATS], "weekly transaction"
    )
    key = ",".join(WEEK_KEY)
    con.execute(f"""
      CREATE TABLE final AS
      WITH merged AS (
        SELECT {key},
          SUM(n_add_lg)::BIGINT AS n_add_lg,
          SUM(n_leagues)::BIGINT AS n_leagues,
          SUM(sum_add_lamar) AS sum_add_lamar,
          SUM(n_add_lamar)::BIGINT AS n_add_lamar,
          SUM(sum_faab_pct) AS sum_faab_pct,
          SUM(n_faab_pct)::BIGINT AS n_faab_pct,
          SUM(sum_faab_bid) AS sum_faab_bid,
          SUM(n_faab_bid)::BIGINT AS n_faab_bid
        FROM {_read(paths)} GROUP BY {key}
      ), calculated AS (
        SELECT *,
          100.0*n_add_lg/NULLIF(n_leagues,0) AS add_rate_pct,
          sum_add_lamar/NULLIF(n_add_lamar,0) AS avg_add_lamar,
          sum_faab_pct/NULLIF(n_faab_pct,0) AS avg_faab_pct,
          sum_faab_bid/NULLIF(n_faab_bid,0) AS avg_faab_bid
        FROM merged
      )
      SELECT *,
        CASE WHEN n_add_lg >= 5 THEN 100.0*PERCENT_RANK() OVER (
          PARTITION BY teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,
                       format_level,year,week
          ORDER BY avg_add_lamar) END AS add_grade,
        CASE WHEN n_add_lg >= 5 THEN 'graded' ELSE 'thin' END AS add_grade_conf,
        CASE WHEN n_leagues >= 35 THEN 'confident'
             WHEN n_leagues >= 10 THEN 'mushy' ELSE 'insufficient' END AS confidence
      FROM calculated
    """)
    con.execute(f"COPY final TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    return out


def _files(roots: list[Path], name: str) -> list[Path]:
    return [p for root in roots for p in root.rglob(name)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", action="append", type=Path, required=True,
                        help="Root containing extracted public or local-private shard artifacts")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--table",
        choices=["matchup", "transactions", "transactions_weekly", "all"],
        default="all",
    )
    parser.add_argument("--denom-season", type=Path, default=None,
                        help="population eligible-league lattice (build_population_denominators.py)")
    parser.add_argument("--denom-week", type=Path, default=None,
                        help="population weekly eligible-league lattice")
    parser.add_argument("--pos-grp", type=Path, default=None,
                        help="player-year -> pos_grp map, the key the denominator splits on")
    parser.add_argument(
        "--allow-unmanifested", action="store_true",
        help="assemble shards that carry no population manifest -- the disjointness of the "
             "input set then rests on the operator, not on a check",
    )
    args = parser.parse_args()
    if args.table in {"matchup", "all"}:
        assemble_matchup(
            _files(args.input, "research_matchup_player_season.parquet"),
            _files(args.input, "research_matchup_weekly.parquet"),
            args.output,
            roots=args.input,
            allow_unmanifested=args.allow_unmanifested,
            denom_season=args.denom_season,
            denom_week=args.denom_week,
            pos_grp=args.pos_grp,
        )
    if args.table in {"transactions", "all"}:
        assemble_transactions(
            _files(args.input, "research_txn_player_season.parquet"), args.output
        )
    if args.table in {"transactions_weekly", "all"}:
        weekly_paths = _files(args.input, "research_txn_weekly.parquet")
        if weekly_paths:
            assemble_transactions_weekly(weekly_paths, args.output)
        elif args.table == "transactions_weekly":
            raise ValueError("no weekly transaction shard files supplied")


if __name__ == "__main__":
    main()
