"""build_population_denominators.py -- the TRUE eligible-league denominators, from the lake.

Every research matchup rate divides by "leagues where this player's position could start".
That number is a property of LEAGUE SETTINGS ALONE -- it does not depend on any player, so
it is not shardable and never should have been sharded.

THE DEFECT THIS FIXES (found 2026-07-27 by the audit's cohort-cell probe). Shards partition
the league population by hash(db_name). Each shard computed its denominator over its OWN
bucket's leagues, and the assembler summed them -- so a player's denominator counted only
the buckets he happened to be rostered in. Measured on the 30-shard build: in every
bucketed year ~51% of rows carried a short denominator, averaging 0.68-0.71 of the true
population and bottoming out at 0.004 of it.

That inverts exactly the property the eligible-leagues denominator exists to create. A
thinly-rostered player lands in one bucket, gets ~1/5 the denominator, and his champ% and
playoff% inflate by that factor -- and thin players are precisely who the denominator was
chosen to floor. The numerators were never affected: they are additive league counts and
they summed correctly.

So the denominator is computed ONCE here, over the whole lake, with the builder's OWN
DENOM_SQL / DENOM_WEEK_SQL imported rather than re-typed -- if the eligibility rules ever
change, this file cannot drift from the numerators it will be divided into.

    py -3 scripts/research_cohorts/build_population_denominators.py --output <dir>
    py -3 scripts/research_cohorts/build_population_denominators.py --output <dir> \
        --verify-against <assembled season parquet>
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))

SEASON_OUT = "research_matchup_denom_season.parquet"
WEEK_OUT = "research_matchup_denom_week.parquet"


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def build(out_dir: Path) -> tuple[Path, Path]:
    # The bucket env MUST be clear: this is the whole-population denominator, and a stray
    # RESEARCH_LEAGUE_BUCKET would silently reproduce the very defect being fixed.
    for var in ("RESEARCH_LEAGUE_BUCKET", "RESEARCH_LEAGUE_BUCKETS", "RESEARCH_SAMPLE_LEAGUES"):
        if os.environ.pop(var, None):
            print(f"[denom] cleared {var} -- denominators are population-wide by definition")

    import build_research_matchup_cohort as M
    from local_reader import LocalReader

    out_dir.mkdir(parents=True, exist_ok=True)
    reader = LocalReader()
    con = duckdb.connect()
    con.execute("SET memory_limit='3000MB'; SET threads=2; SET enable_progress_bar=false;")

    for label, sql, name in (
        ("season", M.DENOM_SQL, SEASON_OUT),
        ("weekly", M.DENOM_WEEK_SQL, WEEK_OUT),
    ):
        path = out_dir / name
        print(f"[denom] {label} lattice ...", flush=True)
        if label == "weekly":
            # The weekly lattice is the only denominator stage large enough to exceed
            # DuckDB's reader heap when all seasons are returned as one Arrow list. Query
            # one season at a time and append through DuckDB; the SQL itself is unchanged.
            year_paths = []
            total = 0
            for year in M.YEARS:
                year_sql = sql.replace(M.YEAR_PREDICATE, f"year = {int(year)}")
                rows = reader.query(year_sql, "___leagues")
                if not rows:
                    continue
                con.register("_d", pa.Table.from_pylist(rows))
                year_path = path.with_name(f".{path.stem}_{int(year)}.parquet")
                con.execute(f"COPY _d TO '{year_path.as_posix()}' (FORMAT PARQUET)")
                year_paths.append(year_path)
                total += len(rows)
                con.unregister("_d")
                print(f"[denom] weekly {year}: {len(rows):,} cells", flush=True)
            if not year_paths:
                raise SystemExit("weekly denominator query returned no rows")
            quoted = ",".join("'" + p.as_posix().replace("'", "''") + "'" for p in year_paths)
            con.execute(f"COPY (SELECT * FROM read_parquet([{quoted}], union_by_name=true)) TO '{path.as_posix()}' (FORMAT PARQUET)")
            print(f"[denom] weekly: {total:,} cells -> {path}", flush=True)
        else:
            rows = reader.query(sql, "___leagues")
            if not rows:
                raise SystemExit(f"{label} denominator query returned no rows")
            con.register("_d", pa.Table.from_pylist(rows))
            con.execute(f"COPY _d TO '{path.as_posix()}' (FORMAT PARQUET)")
            con.unregister("_d")
            print(f"[denom] {label}: {len(rows):,} cells -> {path}", flush=True)
    reader.close()
    return out_dir / SEASON_OUT, out_dir / WEEK_OUT


POS_GRP_OUT = "research_matchup_pos_grp.parquet"


def build_pos_grp(out_dir: Path, ops_cache: Path) -> Path:
    """Emit (NFL_player_id, year) -> pos_grp, the key the denominator is split on.

    The shards do not carry pos_grp, so the assembler cannot join a K's denominator to a K.
    It is recoverable exactly rather than guessed: this replays LocalReader's own
    taxonomy-backed player_position view over the same ops cache, and applies the builder's
    own K/DEF/SKILL rule. Cheap on purpose -- it needs the super table only, not the
    league scan that gates league_settings.
    """
    import json

    from local_reader import POSITION_TAXONOMY, player_position_view_sql

    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='3000MB'; SET threads=2; SET enable_progress_bar=false;")
    con.execute(f"ATTACH '{Path(ops_cache).as_posix()}' AS ops (READ_ONLY)")
    con.execute("CREATE SCHEMA public")
    tax = json.loads(Path(POSITION_TAXONOMY).read_text(encoding="utf-8"))
    pairs = ", ".join("('" + k.replace("'", "''") + "','" + v.replace("'", "''") + "')"
                      for k, v in tax["detailed_to_broad"].items())
    con.execute(f"CREATE TEMP TABLE _pos_tax AS SELECT * FROM (VALUES {pairs}) t(detailed, broad)")
    con.execute("""CREATE VIEW public._pos_raw AS
        SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
               CAST(year AS INTEGER) AS year, CAST(position AS VARCHAR) AS position
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL
        UNION ALL
        SELECT CAST(b.NFL_player_id AS VARCHAR), CAST(y.year AS INTEGER),
               CAST(b.nfl_position AS VARCHAR)
        FROM ops.nfl_historical.player_bio b
        CROSS JOIN (SELECT UNNEST(range(1997, 2026)) AS year) y
        WHERE b.NFL_player_id IS NOT NULL AND b.nfl_position IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM ops.nfl_historical.nfl_player_stats_all s
                          WHERE s.NFL_player_id = b.NFL_player_id AND s.year = y.year
                            AND s.position IS NOT NULL)""")
    con.execute(player_position_view_sql())
    path = out_dir / POS_GRP_OUT
    # Match position_slots_contract.pos_grp_sql: singular QB/RB/WR/TE/K/DEF counting
    # groups, with the defensive IDP classes collapsed to IDP.
    con.execute(f"""COPY (
        SELECT NFL_player_id, year,
               CASE WHEN "position" IN ('QB','RB','WR','TE','K','DEF') THEN "position"
                    WHEN "position" IN ('DL','LB','DB') THEN 'IDP'
                    ELSE 'IDP' END AS pos_grp
        FROM public.player_position GROUP BY 1, 2, 3)
      TO '{path.as_posix()}' (FORMAT PARQUET)""")
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]
    con.close()
    print(f"[pos-grp] {n:,} player-years -> {path}", flush=True)
    return path


def verify(season_denom: Path, assembled_season: Path) -> bool:
    """Cross-check the freshly computed denominator against the built shards.

    A widely-rostered player appears in EVERY bucket, so the assembled data's per-cell
    MAX(n_leagues) already equals the true population wherever such a player exists. Where
    the two agree, this proves the local lake and the lake the fleet read are the same
    population -- which is what makes it legitimate to divide fleet-built numerators by
    locally-built denominators.
    """
    con = duckdb.connect()
    con.execute("SET memory_limit='3000MB'; SET threads=2; SET enable_progress_bar=false;")
    rows = con.execute(f"""
      WITH truth AS (
        SELECT teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
               year, pos_grp, n_leagues AS true_n
        FROM read_parquet('{season_denom.as_posix()}')),
      observed AS (
        SELECT teams, roster, ppr, td, league_type, lineup_mode, keeper_mode, year,
               MAX(n_leagues) AS max_seen
        FROM read_parquet('{assembled_season.as_posix()}')
        WHERE cohort_level=4 AND format_level=0 AND year BETWEEN 2019 AND 2025
        GROUP BY ALL)
      SELECT t.year, t.teams, t.roster, t.ppr, t.td, t.true_n, o.max_seen
      FROM truth t JOIN observed o USING (teams,roster,ppr,td,league_type,lineup_mode,
                                          keeper_mode,year)
      WHERE t.pos_grp='SKILL' AND t.roster='flx' AND t.year BETWEEN 2019 AND 2025
      ORDER BY t.year, t.teams, t.ppr, t.td""").fetchall()
    con.close()
    if not rows:
        print("[verify] no comparable cells -- cannot confirm the lakes match")
        return False
    exact = sum(1 for r in rows if r[5] == r[6])
    print(f"[verify] {exact}/{len(rows)} flx SKILL cells match the shards' observed maximum")
    for year, teams, roster, ppr, td, true_n, seen in rows:
        if true_n != seen:
            print(f"           {year} {teams}/{roster}/{ppr}/{td}: "
                  f"lake {true_n} vs shards saw {seen}")
    return exact == len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--verify-against", type=Path, default=None,
                    help="assembled season parquet to cross-check the population against")
    ap.add_argument("--pos-grp-only", action="store_true",
                    help="emit only the player-year -> pos_grp map (skips the league scan)")
    ap.add_argument("--ops-cache", type=Path, default=Path(os.environ.get(
        "RESEARCH_OPS_CACHE_PATH",
        "D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb")))
    args = ap.parse_args()
    _load_env()
    if args.pos_grp_only:
        build_pos_grp(args.output, args.ops_cache)
        return 0
    season_denom, _ = build(args.output)
    build_pos_grp(args.output, args.ops_cache)
    if args.verify_against:
        ok = verify(season_denom, args.verify_against)
        print("[verify] " + ("lakes agree" if ok else "MISMATCH -- do not assemble against these"))
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
