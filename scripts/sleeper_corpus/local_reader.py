"""local_reader.py -- drop-in replacement for FlyReader that reads the LOCAL real-leagues
snapshot (built by snapshot_real_leagues.py) UNIONed with the grind corpus snapshot (built by
build_corpus_snapshot.py), so the cohort build runs offline over real + sampled leagues.
``RESEARCH_SOURCE_MODE=real_delta`` instead exposes only real league-years that are absent
from the corpus, allowing those additive shards to be merged with public-corpus CI artifacts
without double counting.

Same `.query(sql, database=None) -> list[dict]` shape as FlyReader, so the cohort builders swap
`FlyReader()` -> `LocalReader()` with no other change. The builders' SQL references
public.{league_settings,draft,transactions,player_fantasy}; each is exposed as a view over
(snapshot UNION ALL corpus) with both sides cast to a shared column contract. If the corpus
snapshot is absent, falls back to snapshot-only views (the pre-corpus behavior).
"""
from __future__ import annotations
import hashlib
import json
import os
import time
from pathlib import Path
import duckdb

SNAPSHOT = Path(os.environ.get(
    "RESEARCH_SNAPSHOT_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/leagues_snapshot.duckdb"))
CORPUS_SNAPSHOT = Path(os.environ.get(
    "RESEARCH_CORPUS_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb"))
# The super table cache: the ONLY source of `position`, which the Class A eligibility gate
# needs (a kicker's weeks only count in leagues with a K slot). Without it the gate cannot be
# expressed at all.
OPS_CACHE = Path(os.environ.get(
    "RESEARCH_OPS_CACHE_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb"))
# Optional compact identity asset.  It is intentionally separate from the large
# research snapshot so a repaired map can be replaced without rebuilding the lake.
NATIVE_ID_CROSSWALK = Path(os.environ.get(
    "RESEARCH_NATIVE_ID_CROSSWALK",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/native_id_crosswalk.parquet"))
# Versioned position taxonomy (position-taxonomy.v1): maps the super table's detailed
# positions (RDE, RG, OLB, LOT, ...) onto broad ones (QB/RB/WR/TE/OL/DL/LB/DB/K/P/DEF).
# Eligibility is a BROAD-position question -- a punter is never startable, an IDP is
# startable only in an IDP league -- so every builder gates off this one contract rather
# than its own hand-written list.
POSITION_TAXONOMY = Path(__file__).resolve().parents[2] / (
    "scripts/sota_recon/witness_gate/contracts/position_taxonomy.v1.json")

# Shared column contract per table (superset of what the cohort/weekly builders read).
# Both snapshot sides are cast to these types so the UNION resolves cleanly.
_CONTRACT: dict[str, list[tuple[str, str]]] = {
    "league_settings": [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("num_teams", "INTEGER"),
        ("is_dynasty", "BOOLEAN"), ("max_keepers", "INTEGER"), ("sleeper_best_ball", "BOOLEAN"),
        ("roster_QB", "INTEGER"), ("roster_RB", "INTEGER"), ("roster_WR", "INTEGER"),
        ("roster_TE", "INTEGER"), ("roster_FLX", "INTEGER"), ("roster_SUPER_FLEX", "INTEGER"),
        ("roster_IDP", "INTEGER"), ("roster_DL", "INTEGER"), ("roster_LB", "INTEGER"),
        ("roster_DB", "INTEGER"), ("roster_DB_LB", "INTEGER"), ("roster_DL_LB", "INTEGER"),
        ("scoring_rec", "DOUBLE"), ("scoring_pass_td", "DOUBLE"), ("league_key", "VARCHAR"),
        # --- eligibility-gate inputs (docs/runbooks/research-eligibility-gates-2026-07-16.md)
        ("playoff_start_week", "INTEGER"), ("playoff_teams", "INTEGER"),
        ("regular_season_weeks", "INTEGER"), ("end_week", "INTEGER"),
        ("has_multiweek_championship", "BOOLEAN"),
        ("roster_K", "INTEGER"), ("roster_DEF", "INTEGER"),
        ("roster_BN", "INTEGER"), ("roster_TAXI", "INTEGER"), ("roster_IR", "INTEGER"),
        ("platform", "VARCHAR"),
        # FAAB normalization inputs (bids are only comparable as % of budget)
        ("waiver_budget", "INTEGER"), ("waiver_type", "VARCHAR"),
        # NOTE: `scoring_sane` is NOT in this contract -- it is computed from the data and
        # appended to the league_settings view (see the values/rates split below).
    ],
    "draft": [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("NFL_player_id", "VARCHAR"),
        ("pick", "INTEGER"), ("cost", "DOUBLE"), ("draft_value_zscore", "DOUBLE"),
        ("pick_quality_zscore", "DOUBLE"), ("manager_lamar", "DOUBLE"),
        ("total_fantasy_points", "DOUBLE"),
        # keeper% gate: is_keeper is the only trustworthy signal (100% populated on every
        # platform); league_settings.max_keepers is a Yahoo capture gap at 26.5%.
        ("is_keeper", "BOOLEAN"), ("round", "INTEGER"), ("pick_in_round", "INTEGER"),
        # auction-budget derivation: per-team spend (Joe 2026-07-20, 3rd-highest rule)
        ("franchise_id", "VARCHAR"), ("manager", "VARCHAR"),
    ],
    "transactions": [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
        ("NFL_player_id", "VARCHAR"), ("franchise_id", "VARCHAR"),
        ("transaction_type", "VARCHAR"), ("faab_bid", "DOUBLE"),
        ("transaction_score", "DOUBLE"), ("manager_lamar_ros_managed", "DOUBLE"),
        ("player_lamar_ros_total", "DOUBLE"),
    ],
    "player_fantasy": [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
        ("NFL_player_id", "VARCHAR"), ("player", "VARCHAR"), ("position", "VARCHAR"),
        ("fantasy_position", "VARCHAR"), ("platform", "VARCHAR"),
        ("team_key", "VARCHAR"), ("team_name", "VARCHAR"), ("nfl_team_api", "VARCHAR"),
        ("yahoo_player_id", "VARCHAR"), ("sleeper_player_id", "VARCHAR"),
        ("espn_player_id", "VARCHAR"), ("fleaflicker_player_id", "VARCHAR"),
        ("mfl_player_id", "VARCHAR"),
        ("is_started", "INTEGER"), ("is_rostered", "INTEGER"),
        ("fantasy_points", "DOUBLE"), ("win", "INTEGER"), ("champion", "INTEGER"),
        ("clutch_equity", "DOUBLE"), ("manager_lamar", "DOUBLE"),
        # playoff-rate capture (Joe 2026-07-19): standings-derivation fallback inputs
        ("manager", "VARCHAR"), ("team_points", "DOUBLE"),
        # row-level playoff outcome (2026-07-26): final_playoff_seed stamped by
        # matchup_to_player; made_po_bf/is_playoffs_bf overlaid by apply_playoff_backfill.py
        # for slice leagues. po_sql prefers these over the manager/matchup lane. _proj
        # NULL-fills any source that predates them, so this is a safe contract widening.
        ("final_playoff_seed", "INTEGER"), ("made_po_bf", "TINYINT"),
        ("is_playoffs_bf", "TINYINT"),
    ],
    # playoff ground truth (ledger D15: 94% of crawled leagues carry it fully populated)
    "matchup": [
        ("db_name", "VARCHAR"), ("year", "INTEGER"), ("week", "INTEGER"),
        ("manager", "VARCHAR"), ("franchise_id", "VARCHAR"), ("team_points", "DOUBLE"),
        ("is_playoffs", "INTEGER"), ("playoff_seed", "INTEGER"),
        ("final_playoff_seed", "INTEGER"), ("champion", "INTEGER"),
    ],
}


def player_position_view_sql() -> str:
    """One taxonomy row per player-year, even when source labels change mid-season."""
    priority = ("QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB", "OL", "P")
    canonical = "\n".join(
        f"WHEN list_contains(broad_positions, '{pos}') THEN '{pos}'" for pos in priority
    )
    return f"""CREATE VIEW public.player_position AS
        WITH ex AS (
          SELECT r.NFL_player_id, r.year, UPPER(TRIM(tok)) AS tok
          FROM public._pos_raw r,
               UNNEST(str_split(r.position, ',')) AS g(tok)
        ), mapped AS (
          SELECT ex.NFL_player_id, ex.year, COALESCE(tx.broad, ex.tok) AS broad
          FROM ex LEFT JOIN _pos_tax tx ON tx.detailed = ex.tok
        ), agg AS (
          SELECT NFL_player_id, year, list_sort(list_distinct(list(broad))) AS broad_positions
          FROM mapped GROUP BY 1, 2
        )
        SELECT NFL_player_id, year,
               CASE {canonical}
                    ELSE list_extract(broad_positions, 1) END AS position,
               CASE {canonical}
                    ELSE list_extract(broad_positions, 1) END AS broad_position,
               broad_positions
        FROM agg"""


def configured_reader_memory_mb() -> int:
    """Reader cap: conservative locally, larger for isolated high-volume CI shards."""
    return max(512, min(int(os.environ.get("RESEARCH_READER_MEMORY_MB", "3000")), 6000))


def configured_league_bucket() -> tuple[int, int] | None:
    """Return ``(bucket, bucket_count)`` for an exhaustive hash partition of leagues."""
    raw_count = os.environ.get("RESEARCH_LEAGUE_BUCKETS")
    raw_bucket = os.environ.get("RESEARCH_LEAGUE_BUCKET")
    if raw_count is None and raw_bucket is None:
        return None
    if raw_count is None or raw_bucket is None:
        raise ValueError(
            "RESEARCH_LEAGUE_BUCKETS and RESEARCH_LEAGUE_BUCKET must be set together"
        )
    count, bucket = int(raw_count), int(raw_bucket)
    if count < 1 or not 0 <= bucket < count:
        raise ValueError(
            "research league partition requires buckets >= 1 and 0 <= bucket < buckets"
        )
    return bucket, count


def configured_league_batch() -> tuple[int, int] | None:
    """Return ``(batch, batch_count)`` for a deterministic league-batch plan.

    Unlike hash buckets, the plan is explicitly materialized because the batches are
    balanced by observed row volume.  The plan path is consumed by ``LocalReader``
    when the reader builds its public views.
    """
    raw_count = os.environ.get("RESEARCH_LEAGUE_BATCHES")
    raw_batch = os.environ.get("RESEARCH_LEAGUE_BATCH")
    if raw_count is None and raw_batch is None:
        return None
    if raw_count is None or raw_batch is None:
        raise ValueError(
            "RESEARCH_LEAGUE_BATCHES and RESEARCH_LEAGUE_BATCH must be set together"
        )
    count, batch = int(raw_count), int(raw_batch)
    if count < 1 or not 0 <= batch < count:
        raise ValueError(
            "research league batch requires batches >= 1 and 0 <= batch < batches"
        )
    plan = os.environ.get("RESEARCH_LEAGUE_BATCH_PLAN")
    if not plan:
        raise ValueError(
            "RESEARCH_LEAGUE_BATCH_PLAN is required when league batch mode is enabled"
        )
    return batch, count


def configured_league_list() -> Path | None:
    """Return an explicit JSON league-year/DB target list for focused rescues."""
    raw = os.environ.get("RESEARCH_LEAGUE_LIST", "").strip()
    return Path(raw) if raw else None


def configured_source_mode() -> str:
    """Select the full local union or a merge-safe private-league delta."""
    mode = os.environ.get("RESEARCH_SOURCE_MODE", "full").strip().lower()
    if mode not in {"full", "real_delta"}:
        raise ValueError(
            "RESEARCH_SOURCE_MODE must be 'full' or 'real_delta', "
            f"got {mode!r}"
        )
    return mode


def source_fingerprint(
    snapshot: Path | str = SNAPSHOT,
    corpus: Path | str | None = CORPUS_SNAPSHOT,
    ops_cache: Path | str | None = OPS_CACHE,
) -> str:
    """Fingerprint every source and the union mode for cache invalidation."""
    parts = [f"source_mode:{configured_source_mode()}"]
    for raw in (snapshot, corpus, ops_cache):
        if raw is None:
            parts.append("none")
            continue
        path = Path(raw)
        try:
            stat = path.stat()
            parts.append(
                f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"
            )
        except OSError:
            parts.append(f"{path.resolve()}:missing")
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:12]


class LocalReader:
    def __init__(self, snapshot: Path | str = SNAPSHOT, corpus: Path | str | None = CORPUS_SNAPSHOT,
                 ops_cache: Path | str | None = OPS_CACHE) -> None:
        snap = Path(snapshot)
        if not snap.exists():
            raise FileNotFoundError(f"real-leagues snapshot not found: {snap} (run snapshot_real_leagues.py)")
        self.con = duckdb.connect()  # in-memory; sources attached read-only
        self.con.execute(f"SET memory_limit='{configured_reader_memory_mb()}MB'")
        self.con.execute("SET threads=2")
        self.con.execute("SET preserve_insertion_order=false")
        self._attach_retry(snap, "snap")
        corp = Path(corpus) if corpus else None
        self.has_corpus = bool(corp and corp.exists())
        if self.has_corpus:
            self._attach_retry(corp, "corp")
        self.source_mode = configured_source_mode()
        if self.source_mode == "real_delta" and not self.has_corpus:
            raise RuntimeError(
                "RESEARCH_SOURCE_MODE=real_delta requires a corpus snapshot so overlap "
                "protection can fail closed"
            )
        if self.source_mode == "real_delta":
            print(
                "[local_reader] REAL DELTA MODE: real snapshot only; public Sleeper "
                "league-key overlaps excluded",
                flush=True,
            )
        self.con.execute("CREATE SCHEMA public")
        # public.player_position: the Class A gate needs `position`, which lives only in the
        # super table. Exposed as a slim view so builders can gate without knowing where it
        # comes from. Absent ops_cache -> the view is empty and gated builders fail loudly
        # rather than silently counting kickers in leagues that can't start them.
        ops = Path(ops_cache) if ops_cache else None
        self.has_ops = bool(ops and ops.exists())
        if self.has_ops:
            year_start = int(os.environ.get("RESEARCH_YEAR_START", "1997"))
            year_end = int(os.environ.get("RESEARCH_YEAR_END", "2026"))
            year_filter = f"year BETWEEN {year_start} AND {year_end}"
            self.con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
            # broad-position lookup from the versioned taxonomy contract
            tax = json.loads(POSITION_TAXONOMY.read_text(encoding="utf-8"))
            pairs = ", ".join(
                "('" + k.replace("'", "''") + "','" + v.replace("'", "''") + "')"
                for k, v in tax["detailed_to_broad"].items())
            self.con.execute(
                f"CREATE TEMP TABLE _pos_tax AS SELECT * FROM (VALUES {pairs}) AS t(detailed, broad)")
            self.con.execute(f"""CREATE VIEW public._pos_raw AS
                SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                       CAST(year AS INTEGER) AS year, CAST(position AS VARCHAR) AS position
                FROM ops.nfl_historical.nfl_player_stats_all
                WHERE {year_filter} AND NFL_player_id IS NOT NULL AND position IS NOT NULL
                UNION ALL
                -- bio fallback (ledger D7): never-played players (stashed rookies) have no
                -- stat rows; bio carries their true position so eligibility gates see them.
                -- year-less bio applies to every year via the cross join below.
                SELECT CAST(b.NFL_player_id AS VARCHAR), CAST(y.year AS INTEGER), CAST(b.nfl_position AS VARCHAR)
                FROM ops.nfl_historical.player_bio b
                CROSS JOIN (SELECT UNNEST(range({year_start}, {year_end + 1})) AS year) y
                WHERE b.NFL_player_id IS NOT NULL AND b.nfl_position IS NOT NULL
                  -- per-(player, YEAR): a later debut must not erase earlier stat-less years
                  AND NOT EXISTS (
                    SELECT 1 FROM ops.nfl_historical.nfl_player_stats_all s
                    WHERE s.NFL_player_id = b.NFL_player_id AND s.year = y.year
                      AND s.position IS NOT NULL)""")
            # Safe identity repair for folded Fleaflicker/MFL rows. Only unique
            # year/name(/position) matches are accepted; ambiguous names remain NULL.
            self.con.execute(f"""CREATE TEMP TABLE _player_name_map AS
                SELECT year, norm_name, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                  SELECT CAST(year AS INTEGER) AS year,
                         CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                         LOWER(TRIM(REGEXP_REPLACE(
                           REGEXP_REPLACE(CAST(player AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'),
                           '\\s+', ' ', 'g'))) AS norm_name
                  FROM ops.nfl_historical.nfl_player_stats_all
                  WHERE {year_filter} AND NFL_player_id IS NOT NULL AND player IS NOT NULL
                ) x
                GROUP BY 1, 2
                HAVING COUNT(DISTINCT NFL_player_id) = 1""")
            self.con.execute(f"""CREATE TEMP TABLE _player_name_pos_map AS
                SELECT year, norm_name, pos_family, MIN(NFL_player_id) AS NFL_player_id
                FROM (
                  SELECT CAST(year AS INTEGER) AS year,
                         CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                         LOWER(TRIM(REGEXP_REPLACE(
                           REGEXP_REPLACE(CAST(player AS VARCHAR), '[^a-zA-Z0-9 ]', '', 'g'),
                           '\\s+', ' ', 'g'))) AS norm_name,
                         COALESCE(tx.broad, UPPER(TRIM(CAST(position AS VARCHAR)))) AS pos_family
                  FROM ops.nfl_historical.nfl_player_stats_all s
                  LEFT JOIN _pos_tax tx ON tx.detailed = UPPER(TRIM(CAST(s.position AS VARCHAR)))
                  WHERE {year_filter} AND NFL_player_id IS NOT NULL AND player IS NOT NULL
                    AND position IS NOT NULL
                ) x
                GROUP BY 1, 2, 3
                HAVING COUNT(DISTINCT NFL_player_id) = 1""")
            # player_position carries BOTH the raw position and its broad class, so builders
            # can gate on eligibility (a P is startable nowhere; a DL/LB/DB only in an IDP
            # league) without each one re-deriving the mapping. Unmapped detailed positions
            # fall back to the raw value rather than vanishing -- an unknown position must
            # not silently become eligible.
            # COMPOUND positions are real and must not be dropped: the super table carries
            # dual-eligibility as a comma string -- Taysom Hill is 'QB,WR' (2020-21) and
            # 'QB,TE' (2022-23). Those match no taxonomy key, so a scalar lookup falls through
            # to the raw value and an IN (...) eligibility test silently excludes him. Each
            # token is therefore mapped separately and eligibility asks whether ANY of a
            # player's classes can start -- which is exactly what dual-eligibility means.
            self.con.execute(player_position_view_sql())
            # public.player_active_week: a week where the player had a snap opportunity.
            # Modern rows use the snap/production evidence; DEF rows are the exception
            # because DST records are team-game rows and do not carry player snaps. Before
            # snap tracking is populated (the historical pre-2012 band), REG row presence
            # is the explicit fallback. This keeps the all-years denominator honest without
            # silently treating a modern zero-snap inactive as healthy.
            self.con.execute(f"""CREATE VIEW public.player_active_week AS
                SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                       CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
                FROM ops.nfl_historical.nfl_player_stats_all
                WHERE {year_filter} AND NFL_player_id IS NOT NULL AND week IS NOT NULL
                  AND season_type = 'REG'
                  AND (
                    position = 'DEF'
                    OR CAST(year AS INTEGER) < 2012
                    OR (
                      offense_snaps IS NOT NULL
                      OR special_teams_snaps IS NOT NULL
                      OR defense_snaps IS NOT NULL
                    ) AND (
                      COALESCE(offense_snaps, 0) > 0
                      OR COALESCE(special_teams_snaps, 0) > 0
                      OR COALESCE(defense_snaps, 0) > 0
                      OR COALESCE(fantasy_points_ppr, 0) <> 0
                    )
                  )""")
            # The usage board distinguishes a player's NFL-team game from a game the player
            # personally appeared in.  Stats rows identify the team while the player is
            # active; carry that observed team across the player's in-season span and join
            # to the complete team-game grid. This makes an injury absence a zero-start
            # opportunity for Start %, while Healthy Start % uses the snap-based
            # player_active_week above.
            # Compact bye dictionary.  A full team-game grid is unnecessary: for a player's
            # carried NFL team, every regular-season week is a team-game opportunity except
            # the team's bye.  Keeping only the missing week per team-season avoids materializing
            # a large player/team/game lattice in every runner.
            # Some historical source rows have a canonical NFL player ID and
            # ``nfl_team_api`` but no corresponding NFL stats row (the 234-row
            # team-game denominator gap).  Preserve those rows as a fallback
            # team observation; the NFL stats table remains authoritative whenever
            # both sources exist.
            raw_team_arms = []
            raw_team_sources = ["snap"] + (["corp"] if self.has_corpus and self.source_mode == "full" else [])
            for raw_src in raw_team_sources:
                try:
                    raw_cols = {r[0] for r in self.con.execute(
                        f"DESCRIBE {raw_src}.public.player_fantasy").fetchall()}
                except duckdb.CatalogException:
                    continue
                if {"NFL_player_id", "year", "week", "nfl_team_api"} <= raw_cols:
                    raw_team_arms.append(f"""
                        SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                               CAST(year AS INTEGER) AS year,
                               CAST(week AS INTEGER) AS week,
                               UPPER(TRIM(CAST(nfl_team_api AS VARCHAR))) AS team_code
                        FROM {raw_src}.public.player_fantasy
                        WHERE NFL_player_id IS NOT NULL AND year IS NOT NULL
                          AND week IS NOT NULL AND nfl_team_api IS NOT NULL
                          AND TRIM(CAST(nfl_team_api AS VARCHAR)) <> ''
                          AND UPPER(TRIM(CAST(nfl_team_api AS VARCHAR))) NOT IN
                              ('FA', 'F/A', 'FREE AGENT', 'N/A', 'NA')""")
            if raw_team_arms:
                self.con.execute(
                    "CREATE TEMP TABLE _source_player_team_week AS "
                    + " UNION ALL ".join(raw_team_arms))
            else:
                self.con.execute("""CREATE TEMP TABLE _source_player_team_week (
                    NFL_player_id VARCHAR, year INTEGER, week INTEGER, team_code VARCHAR)""")
            self.con.execute("""CREATE TABLE public.team_byes AS
                WITH team_games AS MATERIALIZED (
                   SELECT DISTINCT CAST(year AS INTEGER) AS year,
                          CAST(week AS INTEGER) AS week,
                          CASE WHEN nfl_franchise_number IS NOT NULL
                               THEN 'F:' || CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)
                               ELSE 'T:' || UPPER(TRIM(CAST(nfl_team AS VARCHAR))) END AS team
                   FROM ops.nfl_historical.nfl_player_stats_all
                   WHERE year BETWEEN 1997 AND 2026
                     AND week IS NOT NULL AND nfl_team IS NOT NULL
                     AND season_type = 'REG'
                ), years AS (
                  SELECT CAST(y AS INTEGER) AS year
                  FROM UNNEST(range(1997, 2027)) AS t(y)
                ), teams AS (
                  SELECT DISTINCT year, team FROM team_games
                ), weeks AS (
                  SELECT y.year, CAST(w AS INTEGER) AS week
                  FROM years y
                  CROSS JOIN LATERAL range(1, CASE WHEN y.year <= 2020 THEN 17 ELSE 18 END) t(w)
                )
                SELECT t.year, t.team, w.week AS bye_week
                FROM teams t
                CROSS JOIN weeks w
                WHERE w.year = t.year
                  AND NOT EXISTS (
                    SELECT 1 FROM team_games g
                    WHERE g.year=t.year AND g.week=w.week AND g.team=t.team
                  )""")
            self.con.execute("""CREATE VIEW public.team_bye_dictionary AS
                SELECT year, team, list_sort(list(bye_week)) AS bye_weeks
                FROM public.team_byes GROUP BY 1, 2""")
            self.con.execute(f"""CREATE TABLE public.player_team_week AS
                 WITH observed AS (
                   SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                          CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
                          CASE WHEN nfl_franchise_number IS NOT NULL
                               THEN 'F:' || CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)
                               ELSE 'T:' || UPPER(TRIM(CAST(nfl_team AS VARCHAR))) END AS team
                   FROM ops.nfl_historical.nfl_player_stats_all
                   WHERE {year_filter} AND NFL_player_id IS NOT NULL AND year IS NOT NULL AND week IS NOT NULL
                     AND nfl_team IS NOT NULL AND season_type = 'REG'
                   UNION ALL
                   SELECT s.NFL_player_id, s.year, s.week,
                          CASE WHEN i.nfl_franchise_number IS NOT NULL
                               THEN 'F:' || CAST(CAST(i.nfl_franchise_number AS BIGINT) AS VARCHAR)
                               ELSE 'T:' || s.team_code END AS team
                   FROM _source_player_team_week s
                   LEFT JOIN (
                     SELECT DISTINCT CAST(year AS INTEGER) AS year,
                            UPPER(TRIM(CAST(nfl_team AS VARCHAR))) AS team_code,
                            CAST(nfl_franchise_number AS BIGINT) AS nfl_franchise_number
                     FROM ops.nfl_historical.nfl_player_stats_all
                     WHERE year BETWEEN 1997 AND 2026 AND nfl_team IS NOT NULL
                       AND nfl_franchise_number IS NOT NULL
                   ) i ON i.year=s.year AND i.team_code=s.team_code
                 ), bounds AS (
                  SELECT NFL_player_id, year, MIN(week) AS first_week,
                         -- A missing stat row after an injury is still a team-game
                         -- opportunity. Carry the last observed team through the
                         -- research endpoint instead of stopping at the last row
                         -- containing player production. Week 17 (2003-2020) and
                         -- week 18 (2021+) are intentionally excluded downstream.
                         CASE WHEN year <= 2020 THEN 16 ELSE 17 END AS last_week
                  FROM observed GROUP BY 1, 2
                )
                SELECT b.NFL_player_id, b.year, CAST(w.week AS INTEGER) AS week,
                       (SELECT arg_max(o.team, o.week) FROM observed o
                        WHERE o.NFL_player_id=b.NFL_player_id AND o.year=b.year
                          AND o.week <= w.week) AS team
                FROM bounds b CROSS JOIN LATERAL range(b.first_week, b.last_week + 1) w(week)""")
            self.con.execute("""CREATE TABLE public.player_team_game_week AS
                SELECT p.NFL_player_id, p.year, p.week,
                       p.team AS team, p.team AS nfl_team
                FROM public.player_team_week p
                WHERE NOT EXISTS (
                  SELECT 1 FROM public.team_byes b
                  WHERE b.year=p.year AND b.team=p.team AND b.bye_week=p.week
                )""")
            # public.player_slug_value: the super table's precomputed per-week value for each
            # of the 12 flx cohort slugs, long-form. This is the RECREATION source for
            # lineage-corrupt leagues (Joe 2026-07-17: recreate, don't NULL): a cohort cell IS
            # a slug, so the slug-matched lamar/fpts is the population-comparable replacement
            # for a league's own (corrupted) enrichment.
            slug_arms_lamar, slug_arms_fpts = [], []
            for t in ("10t", "12t"):
                for p_, fp in (("std", "0ppr"), ("half", "half"), ("ppr", "ppr")):
                    for td in ("4pt", "6pt"):
                        s = f"{t}_flx_{p_}_{td}"
                        slug_arms_lamar.append(f"WHEN '{s}' THEN lamar_{s}")
                        slug_arms_fpts.append(f"WHEN '{s}' THEN fpts_{td}_{fp}")
            slugs_sql = ", ".join(f"('{t}_flx_{p_}_{td}')" for t in ("10t", "12t")
                                  for p_ in ("std", "half", "ppr") for td in ("4pt", "6pt"))
            self.con.execute(f"""CREATE VIEW public.player_slug_value AS
                SELECT CAST(o.NFL_player_id AS VARCHAR) AS NFL_player_id,
                       CAST(o.year AS INTEGER) AS year, CAST(o.week AS INTEGER) AS week,
                       s.slug,
                       CAST(CASE s.slug {' '.join(slug_arms_lamar)} END AS DOUBLE) AS lamar,
                       CAST(CASE s.slug {' '.join(slug_arms_fpts)} END AS DOUBLE) AS fpts
                FROM ops.nfl_historical.nfl_player_stats_all o
                CROSS JOIN (VALUES {slugs_sql}) AS s(slug)
                WHERE {year_filter} AND o.NFL_player_id IS NOT NULL AND o.week IS NOT NULL
                  AND o.season_type = 'REG'""")
        else:
            self.con.execute("CREATE TEMP TABLE _pos_tax (detailed VARCHAR, broad VARCHAR)")
            self.con.execute("CREATE TEMP TABLE _player_name_map (year INTEGER, norm_name VARCHAR, NFL_player_id VARCHAR)")
            self.con.execute("CREATE TEMP TABLE _player_name_pos_map (year INTEGER, norm_name VARCHAR, pos_family VARCHAR, NFL_player_id VARCHAR)")
            self.con.execute("CREATE VIEW public.player_position AS "
                             "SELECT NULL::VARCHAR AS NFL_player_id, NULL::INTEGER AS \"year\", "
                             "NULL::VARCHAR AS \"position\" WHERE false")
            self.con.execute("CREATE VIEW public.player_active_week AS "
                             "SELECT NULL::VARCHAR AS NFL_player_id, NULL::INTEGER AS \"year\", "
                             "NULL::INTEGER AS week WHERE false")
            self.con.execute("CREATE VIEW public.player_team_game_week AS "
                             "SELECT NULL::VARCHAR AS NFL_player_id, NULL::INTEGER AS \"year\", "
                             "NULL::INTEGER AS week, NULL::VARCHAR AS team, NULL::VARCHAR AS nfl_team "
                             "WHERE false")
            self.con.execute("CREATE VIEW public.team_byes AS "
                             "SELECT NULL::INTEGER AS \"year\", NULL::VARCHAR AS team, "
                             "NULL::INTEGER AS bye_week WHERE false")
            self.con.execute("CREATE VIEW public.team_bye_dictionary AS "
                             "SELECT NULL::INTEGER AS \"year\", NULL::VARCHAR AS team, "
                             "[]::INTEGER[] AS bye_weeks WHERE false")
            self.con.execute("CREATE VIEW public.player_team_week AS "
                             "SELECT NULL::VARCHAR AS NFL_player_id, NULL::INTEGER AS \"year\", "
                             "NULL::INTEGER AS week, NULL::VARCHAR AS team WHERE false")
            self.con.execute("CREATE VIEW public.player_slug_value AS "
                             "SELECT NULL::VARCHAR AS NFL_player_id, NULL::INTEGER AS \"year\", "
                             "NULL::INTEGER AS week, NULL::VARCHAR AS slug, NULL::DOUBLE AS lamar, "
                             "NULL::DOUBLE AS fpts WHERE false")
        # Double-count guard: a sampled league can ALSO be a real customer league. Corpus
        # league-years whose league_key matches a real Sleeper league_key are dropped from the
        # union (the real side wins). Exclusion happens on league_settings only -- every cohort
        # builder reaches draft/txn/player_fantasy through a JOIN on league_settings, so the
        # excluded league-years never enter any aggregate or denominator.
        corpus_ls_guard = ""
        real_delta_ls_guard = ""
        if self.has_corpus and self.source_mode == "full":
            corpus_ls_guard = ("league_key IS NULL OR CAST(league_key AS VARCHAR) NOT IN ("
                               "SELECT CAST(league_key AS VARCHAR) "
                               "FROM snap.public.league_settings "
                               "WHERE LOWER(COALESCE(platform,'')) = 'sleeper' "
                               "AND league_key IS NOT NULL)")
        elif self.source_mode == "real_delta":
            real_delta_ls_guard = ("LOWER(COALESCE(platform,'')) <> 'sleeper' "
                                   "OR league_key IS NULL OR CAST(league_key AS VARCHAR) NOT IN ("
                                   "SELECT CAST(league_key AS VARCHAR) "
                                   "FROM corp.public.league_settings "
                                   "WHERE LOWER(COALESCE(platform,'')) = 'sleeper' "
                                   "AND league_key IS NOT NULL)")
        # Denominator-ghost gate (2026-07-18): a league-year that sits in league_settings but
        # contributes (almost) no identified rostered player rows deflates every SEASON-grain
        # denominator -- n_leagues counts it, no numerator can come from it (found: ~1.5k
        # crawled never-played shells). Mirror of the fold-time stats gate (gated_fold.py):
        # >=100 rostered player-weeks carrying NFL_player_id AND >=85% of rostered rows
        # identified. Weekly denominators already self-protect (they join player_fantasy);
        # gating the league_settings view closes the season lane for every builder at once.
        # SCORING-SANITY FLAG, not an exclusion (Joe 2026-07-19). Crawled leagues with
        # custom/troll scoring carry weekly player scores to 1e10; their SETTINGS look normal
        # (teams/rec/pass_td all sane) because the absurdity lives in scoring dims the flat
        # capture drops, so only the DATA can flag them. But the ruling is a VALUES/RATES
        # SPLIT, not a blanket ban:
        #   * VALUES (points, ppg, LAMAR magnitude) never come from leagues at all -- they
        #     come from the super table's slug-scored canon, so joke scoring cannot reach them;
        #   * RATES/USAGE (start rate, roster rate, positional start share, add rate) are
        #     TRUE in a joke league -- a manager still started who they started. Those leagues
        #     stay in the usage denominators and in the LAMAR start-cutoff census.
        # So `scoring_sane` ships as a COLUMN on league_settings: builders filter on it only
        # for scoring-DERIVED lanes (win/clutch/champion outcomes, any league-observed points),
        # and ignore it everywhere usage is counted. Bounds from the 19k league-year
        # distribution: real weekly max p90=73.7 with a knee at ~100-120 (highest real NFL
        # fantasy week ~=61 PPR; 6pt-passing + bonuses ~90); real minima p01 ~= -19.
        # Load focused targets before the population/scoring prepass so target workers do
        # not scan the entire player lake just to discard it afterward.
        target_path = configured_league_list()
        if target_path is not None:
            if not target_path.is_file():
                raise FileNotFoundError(f"RESEARCH_LEAGUE_LIST not found: {target_path}")
            payload = json.loads(target_path.read_text(encoding="utf-8"))
            records = payload.get("targets", []) if isinstance(payload, dict) else payload
            normalized = []
            for record in records or []:
                if isinstance(record, str):
                    normalized.append((record.strip(), None))
                elif isinstance(record, dict) and record.get("db_name"):
                    normalized.append((str(record["db_name"]).strip(), record.get("year")))
            normalized = sorted({(db, int(year) if year is not None else None) for db, year in normalized if db})
            if not normalized:
                raise ValueError(f"RESEARCH_LEAGUE_LIST is empty: {target_path}")
            self.con.execute("CREATE OR REPLACE TEMP TABLE _lg_target (db_name VARCHAR, year INTEGER)")
            self.con.executemany("INSERT INTO _lg_target VALUES (?, ?)", normalized)
            print(f"[local_reader] FOCUSED TARGET MODE: {len(normalized):,} target records from {target_path}", flush=True)

        srcs = ["snap"] + (["corp"] if self.has_corpus and self.source_mode == "full" else [])

        # Native platform IDs are stronger than a name-only repair, but older folded
        # schemas often dropped those columns.  Build year-scoped maps from every source
        # row that still carries both the native ID and a canonical NFL ID.  Only
        # one-to-one mappings are admitted; an ID that resolves to multiple NFL players
        # is deliberately left unresolved rather than poisoning denominators.
        native_specs = (
            ("fleaflicker_player_id", "_fleaflicker_id_map"),
            ("mfl_player_id", "_mfl_id_map"),
        )
        external_native_maps: dict[str, str] = {}
        if NATIVE_ID_CROSSWALK.is_file():
            crosswalk_path = NATIVE_ID_CROSSWALK.resolve().as_posix().replace("'", "''")
            self.con.execute(f"""CREATE TEMP TABLE _external_native_id_map AS
                SELECT LOWER(TRIM(CAST(platform AS VARCHAR))) AS platform,
                       CAST(year AS INTEGER) AS year,
                       NULLIF(TRIM(CAST(native_id AS VARCHAR)), '') AS pid,
                       NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') AS NFL_player_id
                FROM read_parquet('{crosswalk_path}')
                WHERE platform IS NOT NULL AND year IS NOT NULL
                  AND native_id IS NOT NULL AND NFL_player_id IS NOT NULL""")
            self.con.execute(f"""CREATE TEMP TABLE _external_name_pos_map AS
                SELECT LOWER(TRIM(CAST(platform AS VARCHAR))) AS platform,
                       CAST(year AS INTEGER) AS year,
                       NULLIF(TRIM(CAST(name_norm AS VARCHAR)), '') AS norm_name,
                       NULLIF(TRIM(CAST(pos_family AS VARCHAR)), '') AS pos_family,
                       MIN(NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '')) AS NFL_player_id
                FROM read_parquet('{crosswalk_path}')
                WHERE platform IS NOT NULL AND year IS NOT NULL
                  AND name_norm IS NOT NULL AND pos_family IS NOT NULL
                  AND NFL_player_id IS NOT NULL
                GROUP BY 1, 2, 3, 4
                HAVING COUNT(DISTINCT NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '')) = 1""")
            external_native_maps = {
                "fleaflicker_player_id": "fleaflicker",
                "mfl_player_id": "mfl",
            }
        else:
            self.con.execute("""CREATE TEMP TABLE _external_name_pos_map(
                platform VARCHAR, year INTEGER, norm_name VARCHAR,
                pos_family VARCHAR, NFL_player_id VARCHAR)""")
        for native_col, map_name in native_specs:
            arms = []
            for src in srcs:
                have = {r[0] for r in self.con.execute(
                    f"DESCRIBE {src}.public.player_fantasy").fetchall()}
                if {native_col, "NFL_player_id", "year"} <= have:
                    target_clause = (
                        " AND EXISTS (SELECT 1 FROM _lg_target t "
                        "WHERE t.db_name=pf_native.db_name "
                        "AND (t.year IS NULL OR t.year=pf_native.year))"
                        if target_path else ""
                    )
                    arms.append(
                        f"SELECT CAST(pf_native.year AS INTEGER) AS year, "
                        f"NULLIF(TRIM(CAST(pf_native.\"{native_col}\" AS VARCHAR)), '') AS pid, "
                        "NULLIF(TRIM(CAST(pf_native.\"NFL_player_id\" AS VARCHAR)), '') AS NFL_player_id "
                        f"FROM {src}.public.player_fantasy pf_native "
                        f"WHERE pf_native.\"{native_col}\" IS NOT NULL "
                        "AND pf_native.\"NFL_player_id\" IS NOT NULL"
                        f"{target_clause}"
                    )
            external_platform = external_native_maps.get(native_col)
            if external_platform:
                arms.append(
                    "SELECT year, pid, NFL_player_id "
                    "FROM _external_native_id_map "
                    f"WHERE platform = '{external_platform}'"
                )
            if arms:
                self.con.execute(f"""CREATE TEMP TABLE {map_name} AS
                    SELECT year, pid, MIN(NFL_player_id) AS NFL_player_id
                    FROM ({' UNION ALL '.join(arms)}) ids
                    WHERE pid IS NOT NULL AND NFL_player_id IS NOT NULL
                    GROUP BY 1, 2
                    HAVING COUNT(DISTINCT NFL_player_id) = 1""")
            else:
                self.con.execute(
                    f"CREATE TEMP TABLE {map_name} (year INTEGER, pid VARCHAR, NFL_player_id VARCHAR)"
                )

        def _source_pf_select(src: str) -> str:
            """Project player rows and repair only safe null-ID name matches."""
            have = {r[0] for r in self.con.execute(
                f"DESCRIBE {src}.public.player_fantasy").fetchall()}

            def col(name: str, typ: str = "VARCHAR") -> str:
                return f'CAST(pf."{name}" AS {typ})' if name in have else f'CAST(NULL AS {typ})'

            norm_name = (
                "LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(pf.player AS VARCHAR), "
                "'[^a-zA-Z0-9 ]', '', 'g'), '\\\\s+', ' ', 'g'), "
                "' (iii|iv|ii|jr|sr|v)$', '', 'i')))"
                if "player" in have else "NULL"
            )
            pos_family = (
                "COALESCE(tx.broad, UPPER(TRIM(CAST(pf.position AS VARCHAR))))"
                if "position" in have else "NULL"
            )
            ff_pid = col("fleaflicker_player_id")
            mf_pid = col("mfl_player_id")
            tx_join = (
                "LEFT JOIN _pos_tax tx ON tx.detailed=UPPER(TRIM(CAST(pf.position AS VARCHAR)))"
                if "position" in have else ""
            )
            canonical_id = (
                "COALESCE(NULLIF(TRIM(CAST(pf.NFL_player_id AS VARCHAR)), ''), "
                "ffid.NFL_player_id, mfid.NFL_player_id, emnp.NFL_player_id, "
                "npp.NFL_player_id, nm.NFL_player_id)"
                if "NFL_player_id" in have else
                "COALESCE(ffid.NFL_player_id, mfid.NFL_player_id, "
                "emnp.NFL_player_id, npp.NFL_player_id, nm.NFL_player_id)"
            )
            parts = []
            for name, typ in _CONTRACT["player_fantasy"]:
                if name == "NFL_player_id":
                    parts.append(f'CAST({canonical_id} AS {typ}) AS "{name}"')
                else:
                    parts.append(f'{col(name, typ)} AS "{name}"')
            target = (
                " WHERE EXISTS (SELECT 1 FROM _lg_target t WHERE t.db_name=pf.db_name "
                "AND (t.year IS NULL OR t.year=pf.year))" if target_path else ""
            )
            return (
                f"SELECT * FROM (SELECT {', '.join(parts)} "
                f"FROM {src}.public.player_fantasy pf "
                f"{tx_join} "
                f"LEFT JOIN _fleaflicker_id_map ffid ON ffid.year=pf.year "
                f"AND ffid.pid=NULLIF(TRIM({ff_pid}), '') "
                f"LEFT JOIN _mfl_id_map mfid ON mfid.year=pf.year "
                f"AND mfid.pid=NULLIF(TRIM({mf_pid}), '') "
                f"LEFT JOIN _external_name_pos_map emnp ON emnp.platform='mfl' "
                f"AND emnp.year=pf.year AND emnp.norm_name={norm_name} AND emnp.pos_family={pos_family} "
                f"LEFT JOIN _player_name_pos_map npp ON npp.year=pf.year "
                f"AND npp.norm_name={norm_name} AND npp.pos_family={pos_family} "
                f"LEFT JOIN _player_name_map nm ON nm.year=pf.year AND nm.norm_name={norm_name}"
                f"{target}) resolved_pf"
            )

        # Install the exhaustive partition before computing _lg_has_data.  The old order
        # built the scoring/population gate over every player row and only applied the
        # batch/hash predicate afterward, forcing every shard to rescan the full lake.
        # A shard is already a complete league-season partition, so restricting this
        # first scan is lossless and removes the dominant repeated cost.
        league_bucket = configured_league_bucket()
        league_batch = configured_league_batch()
        if league_bucket is not None and league_batch is not None:
            raise ValueError("hash bucket and weighted league batch modes are mutually exclusive")
        source_partition_guard = "TRUE"
        planned = None
        if league_bucket is not None:
            bucket, bucket_count = league_bucket
            source_partition_guard = f"hash(db_name) % {bucket_count} = {bucket}"
        elif league_batch is not None:
            batch, batch_count = league_batch
            plan_path = os.environ["RESEARCH_LEAGUE_BATCH_PLAN"]
            self.con.execute(
                "CREATE OR REPLACE TEMP TABLE _lg_batch AS "
                "SELECT db_name, year FROM read_parquet(?) WHERE batch = ? AND batches = ?",
                [plan_path, batch, batch_count],
            )
            planned = self.con.execute("SELECT COUNT(*) FROM _lg_batch").fetchone()[0]
            source_partition_guard = "(db_name, year) IN (SELECT db_name, year FROM _lg_batch)"

        pf_arms = [_source_pf_select(s) for s in srcs]
        pf_union = " UNION ALL ".join(
            f"SELECT db_name, year, is_rostered, NFL_player_id, fantasy_points "
            f"FROM ({arm}) q WHERE {source_partition_guard}"
            for arm in pf_arms)
        delta_data_guard = ""
        if real_delta_ls_guard:
            delta_data_guard = (
                "WHERE (db_name, year) IN (SELECT db_name, year "
                "FROM snap.public.league_settings WHERE " + real_delta_ls_guard + ")"
            )
        self.con.execute(f"""CREATE TEMP TABLE _lg_has_data AS
            SELECT db_name, year,
                   (COALESCE(MAX(fantasy_points), 0) <= 120
                    AND COALESCE(MIN(fantasy_points), 0) >= -40) AS scoring_sane
            FROM ({pf_union})
            {delta_data_guard}
            GROUP BY 1, 2
            HAVING COUNT(NFL_player_id) FILTER (is_rostered = 1) >= 100
               AND COUNT(NFL_player_id) FILTER (is_rostered = 1)
                   >= 0.85 * COUNT(*) FILTER (is_rostered = 1)""")
        data_guard = "(db_name, year) IN (SELECT db_name, year FROM _lg_has_data)"

        # Focused rescue mode: restrict every fact view to the same explicit target list.
        # The list was loaded before _lg_has_data so this predicate is source-bounded.
        if target_path is not None:
            data_guard += " AND (db_name, year) IN ("
            data_guard += "SELECT t.db_name, COALESCE(t.year, s.year) FROM _lg_target t "
            data_guard += "JOIN (SELECT DISTINCT db_name, year FROM _lg_has_data) s "
            data_guard += "ON s.db_name=t.db_name AND (t.year IS NULL OR t.year=s.year))"

        # EXHAUSTIVE SHARD MODE: split whole leagues into disjoint hash buckets. Unlike
        # sample mode, every bucket is production data and the complete bucket set can be
        # recombined losslessly from the builders' additive sufficient statistics.
        if league_bucket is not None:
            bucket, bucket_count = league_bucket
            data_guard += f" AND hash(db_name) % {bucket_count} = {bucket}"
            print(
                f"[local_reader] LEAGUE BUCKET {bucket + 1}/{bucket_count} "
                "(exhaustive production partition)",
                flush=True,
            )
        elif league_batch is not None:
            batch, batch_count = league_batch
            data_guard += " AND (db_name, year) IN (SELECT db_name, year FROM _lg_batch)"
            print(
                f"[local_reader] LEAGUE BATCH {batch + 1}/{batch_count} "
                f"({planned:,} league-seasons; balanced production partition)",
                flush=True,
            )

        # SAMPLE MODE (RESEARCH_SAMPLE_LEAGUES=N): restrict the lake to N leagues so the whole
        # builder chain runs end-to-end in minutes. Iterating on a 16-hour whale to find a
        # plumbing bug is how 2026-07-19 was lost. Sampled by hash of db_name -- deterministic
        # (same N gives the same leagues, so runs are comparable) and whole-LEAGUE, never
        # whole-league-YEAR, because career/multi-year logic needs a league's full history.
        # Pair with RESEARCH_OUT_DIR so sample output can never overwrite production parquets.
        sample_n = int(os.environ.get("RESEARCH_SAMPLE_LEAGUES", "0") or 0)
        if sample_n > 0:
            self.con.execute(f"""CREATE TEMP TABLE _lg_sample AS
                SELECT db_name FROM (
                  SELECT DISTINCT db_name FROM _lg_has_data ORDER BY hash(db_name)
                ) LIMIT {sample_n}""")
            got = self.con.execute("SELECT COUNT(*) FROM _lg_sample").fetchone()[0]
            print(f"[local_reader] SAMPLE MODE: {got:,} leagues "
                  f"(RESEARCH_SAMPLE_LEAGUES={sample_n}) -- output is NOT production data",
                  flush=True)
            data_guard += " AND db_name IN (SELECT db_name FROM _lg_sample)"

        def _proj(src: str, table: str, cols: list[tuple[str, str]]) -> tuple[str, bool]:
            """SELECT arm for one source, NULL-filling contract columns the source doesn't have
            yet (same tolerance as the fold's _select_sql): a snapshot built before a contract
            widening keeps working -- the new lanes read NULL until it is refreshed. A missing
            TABLE (e.g. matchup pre-widening) becomes an empty arm, not a binder error."""
            try:
                have = {r[0] for r in self.con.execute(f"DESCRIBE {src}.public.{table}").fetchall()}
            except duckdb.CatalogException:
                parts = ", ".join(f'CAST(NULL AS {t}) AS "{n}"' for n, t in cols)
                return f"SELECT {parts} WHERE false", False
            parts = ", ".join(
                f'CAST("{n}" AS {t}) AS "{n}"' if n in have else f'CAST(NULL AS {t}) AS "{n}"'
                for n, t in cols)
            return f"SELECT {parts} FROM {src}.public.{table}", True

        for table, cols in _CONTRACT.items():
            if table == "player_fantasy":
                snap_arm, snap_ok = _source_pf_select("snap"), True
                corp_arm, corp_ok = (
                    (_source_pf_select("corp"), True)
                    if self.has_corpus and self.source_mode == "full" else ("", False)
                )
            else:
                snap_arm, snap_ok = _proj("snap", table, cols)
                corp_arm, corp_ok = (
                    _proj("corp", table, cols)
                    if self.has_corpus and self.source_mode == "full" else ("", False)
                )
            if table == "league_settings":
                if not snap_ok:
                    raise RuntimeError("real-leagues snapshot has no league_settings table")
                snap_conditions = [data_guard]
                if real_delta_ls_guard:
                    snap_conditions.insert(0, f"({real_delta_ls_guard})")
                snap_arm += " WHERE " + " AND ".join(snap_conditions)
                if corp_ok:
                    corp_conditions = [data_guard]
                    if corpus_ls_guard:
                        corp_conditions.insert(0, f"({corpus_ls_guard})")
                    corp_arm += " WHERE " + " AND ".join(corp_conditions)
                union = f" UNION ALL {corp_arm}" if corp_ok else ""
                self.con.execute(f"CREATE VIEW public._ls_base AS {snap_arm}{union}")
                # scoring_sane rides as a column so every builder reads ONE settings view and
                # decides per-lane whether the flag applies.
                self.con.execute("""CREATE VIEW public.league_settings AS
                    SELECT b.*, COALESCE(g.scoring_sane, false) AS scoring_sane
                    FROM public._ls_base b
                    LEFT JOIN _lg_has_data g ON g.db_name = b.db_name AND g.year = b.year""")
                continue
            # Every fact view must carry the same population guard as league_settings.
            # This is what makes hash buckets and weighted league batches real
            # partitions rather than merely filtered settings metadata.  Without it,
            # player_fantasy/matchup remain whole-lake views and each nominal shard
            # repeats almost the entire build.
            if snap_ok:
                snap_arm += " WHERE " + data_guard
            if corp_ok:
                corp_arm += " WHERE " + data_guard
            union = f" UNION ALL {corp_arm}" if corp_ok else ""
            self.con.execute(f"CREATE VIEW public.{table} AS {snap_arm}{union}")

    def _attach_retry(self, path: Path, alias: str, attempts: int = 6, wait_s: int = 20) -> None:
        """ATTACH READ_ONLY with retry: a transient writer (a snapshot fold, a one-off script)
        holds an exclusive lock for seconds, and failing a whole cohort build over it is worse
        than waiting out a couple of minutes. Loud on every retry, raises after the last."""
        for i in range(attempts):
            try:
                self.con.execute(f"ATTACH '{path.as_posix()}' AS {alias} (READ_ONLY)")
                return
            except duckdb.IOException:
                if i == attempts - 1:
                    raise
                print(f"[local_reader] {path.name} locked by another process; "
                      f"retry {i + 1}/{attempts - 1} in {wait_s}s", flush=True)
                time.sleep(wait_s)

    def install_league_filter(self, rows: list[tuple[str, int]] | tuple[tuple[str, int], ...]) -> None:
        """Expose read-only filtered views for an explicit league-year population.

        The underlying snapshot views remain unchanged. Stability studies use the
        ``public.filtered_*`` views so an analysis query cannot accidentally cross
        a year boundary or include a league outside its paired sample population.
        """
        normalized = [(str(db_name).strip(), int(year)) for db_name, year in rows]
        if not normalized:
            raise ValueError("research league filter cannot be empty")
        if any(not db_name for db_name, _ in normalized):
            raise ValueError("research league filter contains an empty db_name")
        if len(normalized) != len(set(normalized)):
            raise ValueError("research league filter contains duplicate league-years")

        self.con.execute("DROP TABLE IF EXISTS _research_league_filter")
        self.con.execute(
            "CREATE TEMP TABLE _research_league_filter (db_name VARCHAR, year INTEGER)"
        )
        self.con.executemany(
            "INSERT INTO _research_league_filter VALUES (?, ?)",
            normalized,
        )
        for table in _CONTRACT:
            filtered = f"filtered_{table}"
            self.con.execute(f"DROP VIEW IF EXISTS public.{filtered}")
            self.con.execute(f"""CREATE VIEW public.{filtered} AS
                SELECT source.*
                FROM public.{table} source
                JOIN _research_league_filter chosen
                  ON chosen.db_name = source.db_name AND chosen.year = source.year""")

    def query(self, sql: str, database: str | None = None) -> list[dict]:
        return self.con.execute(sql).to_arrow_table().to_pylist()

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass
