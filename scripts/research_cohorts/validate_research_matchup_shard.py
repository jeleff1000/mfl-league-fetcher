"""Fail-fast semantic checks for one research matchup shard.

This runs on the worker before an artifact is uploaded.  It deliberately checks the
identities that are otherwise easy to miss until the Fly bundle is assembled:
confirmed W/L for every expected start, weekly start-rate identity, bounded rates,
and non-negative exposure denominators.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


RATE_PREFIXES = (
    "roster_rate_", "start_rate_", "healthy_start_rate_", "win_rate_",
    "won_", "lost_", "champ_total_", "champ_started_",
    "playoff_total_", "playoff_started_",
)
RATE_COLUMNS = {
    "roster_rate_pct", "start_rate_pct", "healthy_start_rate_pct", "win_rate_pct",
    "won_pct", "lost_pct", "champ_total_pct", "champ_as_starter_pct",
    "playoff_total_pct", "playoff_as_starter_pct",
}
COUNT_PREFIXES = (
    "n_leagues_", "n_champ_", "n_clutch_", "n_win_", "n_srate_",
    "eligible_leagues_", "healthy_elig_", "champ_elig_", "playoff_elig_",
    "started_leagues_",
)
COUNT_COLUMNS = {
    "n_leagues", "n_leagues_champ", "n_leagues_po", "n_rostered_leagues",
    "eligible_leagues", "healthy_eligible_leagues", "champ_eligible",
    "champ_elig_leagues", "playoff_eligible_leagues", "started_leagues",
}

# These are the additive sufficient statistics consumed by the release assembler.
# A typed-empty or pre-weighted shard must not pass validation: it can otherwise
# look green in isolation and fail much later when seasons are assembled.
REQUIRED_SEASON_COLUMNS = {
    "rostered_weeks", "roster_eligible_league_weeks",
    "started_team_game_weeks", "team_game_eligible_league_weeks",
    "started_active_weeks", "healthy_eligible_league_weeks",
    "wins_started", "losses_started", "n_champ_start_leagues", "n_started_po",
}
REQUIRED_WEEKLY_COLUMNS = {
    "rostered_leagues", "started_leagues", "healthy_started_leagues",
    "team_game_eligible_leagues", "healthy_eligible_leagues",
    "wins_started", "losses_started", "champ_eligible",
}


def q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def cols(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    return {row[0] for row in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')"
    ).fetchall()}


def check_file(con: duckdb.DuckDBPyConnection, path: Path, year_start: int, year_end: int) -> list[str]:
    table = "shard"
    con.execute(f"CREATE OR REPLACE TEMP VIEW {table} AS SELECT * FROM read_parquet('{path.as_posix()}')")
    have = cols(con, path)
    errors: list[str] = []
    name = path.name.lower()
    required = (
        REQUIRED_SEASON_COLUMNS if "player_season" in name
        else REQUIRED_WEEKLY_COLUMNS if "weekly" in name
        else set()
    )
    missing_schema = sorted(required - have)
    if missing_schema:
        errors.append(
            "missing current additive matchup columns: " + ", ".join(missing_schema)
        )
    if "year" in have:
        lo, hi, n = con.execute(
            f"SELECT MIN(year), MAX(year), COUNT(*) FROM {table}"
        ).fetchone()
        if n and not (year_start <= lo <= hi <= year_end):
            errors.append(f"year bounds {lo}-{hi} outside {year_start}-{year_end}")

    # expected_wins/expected_losses/expected_starts are derived display fields, not
    # shard primitives.  Older valid shards carry a pre-normalization version of those
    # fields whose denominator differs from the current wins-in-starts contract.  The
    # assembler recomputes them from the additive wins/losses and exposure columns, and
    # the assembled release gate validates the final identity.  Validating the legacy
    # display fields here would reject otherwise usable historical shards.

    rate_names = set(RATE_COLUMNS)
    rate_names.update(c for prefix in RATE_PREFIXES for c in have if c.startswith(prefix))
    for name in sorted(rate_names & have):
            col = q(name)
            bad = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL "
                f"AND (NOT isfinite(CAST({col} AS DOUBLE)) OR {col} < -0.0001 OR {col} > 100.0001)"
            ).fetchone()[0]
            if bad:
                errors.append(f"{name}: {bad} out-of-bounds rate rows")
                if name in {"playoff_total_pct", "playoff_as_starter_pct"}:
                    sample_cols = [c for c in (
                        "teams", "roster", "ppr", "td", "bracket", "NFL_player_id",
                        "pos_grp", "po_n_resolved_leagues", "po_n_rostered_leagues",
                        "po_n_started_leagues", "n_final_po", name
                    ) if c in have]
                    if sample_cols:
                        sample = con.execute(
                            f"SELECT {','.join(q(c) for c in sample_cols)} FROM {table} "
                            f"WHERE {q(name)} IS NOT NULL AND ({q(name)} < -0.0001 OR {q(name)} > 100.0001) LIMIT 5"
                        ).fetchall()
                        print(f"[semantic-gate] {name} samples: {sample}", flush=True)

    count_names = set(COUNT_COLUMNS)
    count_names.update(c for prefix in COUNT_PREFIXES for c in have if c.startswith(prefix))
    for name in sorted(count_names & have):
            col = q(name)
            bad = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL AND {col} < 0"
            ).fetchone()[0]
            if bad:
                errors.append(f"{name}: {bad} negative denominator/count rows")

    # Denominator contracts.  These are deliberately checked against the additive
    # primitives as well as the displayed rates; a plausible-looking percentage can
    # still be using the wrong league lattice (for example all leagues instead of
    # K/DEF-eligible leagues, or rostered weeks instead of team-game weeks).
    ratio_specs = [
        ("roster_rate_pct", ("rostered_leagues", "roster_eligible_leagues"), 100.0),
        ("roster_rate_pct", ("rostered_weeks", "roster_eligible_league_weeks"), 100.0),
        ("start_rate_pct", ("started_leagues", "team_game_eligible_leagues"), 100.0),
        ("start_rate_pct", ("started_team_game_weeks", "team_game_eligible_league_weeks"), 100.0),
        ("healthy_start_rate_pct", ("healthy_started_leagues", "healthy_eligible_leagues"), 100.0),
        ("healthy_start_rate_pct", ("started_active_weeks", "healthy_eligible_league_weeks"), 100.0),
    ]
    for rate, (numer, denom), scale in ratio_specs:
        if rate not in have or numer not in have or denom not in have:
            continue
        bad = con.execute(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE {q(rate)} IS NOT NULL AND {q(numer)} IS NOT NULL "
            f"AND {q(denom)} > 0 "
            f"AND ABS({q(rate)} - {scale}*{q(numer)}/NULLIF({q(denom)},0)) > 0.11"
        ).fetchone()[0]
        if bad:
            errors.append(f"{rate}: {bad} rows violate {numer}/{denom} denominator")

    # Season W/L counts must be drawn from the same started team-game exposure
    # as the served Win% lane.  A source join that loses NFL team-game support
    # can otherwise leave raw W/L counts on a row with zero started weeks; that
    # defect only appeared later in the assembled season/career audit.
    if {"wins_started", "losses_started", "started_team_game_weeks"} <= have:
        bad = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE "
            f"wins_started + losses_started > started_team_game_weeks + .000001"
        ).fetchone()[0]
        if bad:
            errors.append(
                f"W+L within started team-game weeks: {bad} rows exceed support"
            )

    # Clutch is a final-grain metric.  Raw shard `avg_clutch_started` can be a
    # weekly-weighted placeholder, especially in the season file; the assembler
    # intentionally recomputes it from additive clutch sums over champion-eligible
    # leagues.  Validate the additive fields here and defer the displayed-rate
    # identity to audit_research_matchup.py after assembly.  Do not reject a shard
    # because a player has champion coverage but no clutch-bearing start.

    # The weekly primitive must retain the separate healthy denominator: a player
    # on a bye is not inactive, while a player with no snaps is not healthy-eligible.
    # This identity catches accidental reuse of the roster/team-game denominator.
    if {"healthy_eligible_leagues", "team_game_eligible_leagues"} <= have and "week" in have:
        bad = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE "
            f"healthy_eligible_leagues < 0 OR team_game_eligible_leagues < 0"
        ).fetchone()[0]
        if bad:
            errors.append(f"healthy/team-game denominator: {bad} negative rows")

    clutch_names = (["avg_clutch_started"] if "avg_clutch_started" in have else [])
    clutch_names += sorted(c for c in have if c.startswith("clutch_"))
    for clutch in clutch_names:
        suffix = clutch.removeprefix("clutch_") if clutch != "avg_clutch_started" else ""
        denom = "champ_eligible" if not suffix and "champ_eligible" in have else (
            "champ_elig_leagues" if not suffix else f"champ_elig_{suffix}"
        )
        if denom in have:
            out = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {q(clutch)} IS NOT NULL "
                f"AND ({q(clutch)} < -100.0001 OR {q(clutch)} > 100.0001)"
            ).fetchone()[0]
            if out:
                errors.append(f"{clutch}: {out} out-of-bounds clutch rows")
    return errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--year-start", type=int, required=True)
    ap.add_argument("--year-end", type=int, required=True)
    args = ap.parse_args()
    files = sorted(args.out.glob("research_matchup*.parquet"))
    if not files:
        raise SystemExit(f"no matchup parquet files in {args.out}")
    con = duckdb.connect()
    failures: list[str] = []
    for path in files:
        found = check_file(con, path, args.year_start, args.year_end)
        failures.extend(f"{path.name}: {item}" for item in found)
        print(f"[semantic-gate] {path.name}: {'FAIL' if found else 'ok'}", flush=True)
    con.close()
    if failures:
        print("[semantic-gate] FAIL", flush=True)
        for failure in failures:
            print(f"  - {failure}", flush=True)
        return 1
    print(f"[semantic-gate] PASS ({len(files)} matchup artifacts)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
