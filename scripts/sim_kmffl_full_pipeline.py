"""Compact end-to-end simulation: fake KMFFL with 2024-2025 canonical + external 2013/2014.

Validates today's three fixes against an in-memory DuckDB:

  1. 8acc7d50 - apply_mappings preserves yahoo_player_id (2014 player rows
                survive dedup instead of collapsing 2,560 to ~160)
  2. 9dad4189 - staging merger only DELETEs+saves staging-touched years
                (2025 quick-import enrichments survive untouched)
  3. 6aff3607 - ensure_player_week regenerates stale YAHOO-prefix once
                NFL_player_id resolves (Bo Nix joins super_table)

The test scope is intentionally tiny: just 2024 + 2025 canonical (simulated
quick-import + Yahoo-fetcher output) plus the real KMFFL 2013/2014 external
parquets. That is the minimum to exercise both quick->full handoff and the
external-merge code paths.

Run: python scripts/sim_kmffl_full_pipeline.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

# Force UTF-8 console (Windows cp1252 chokes on -> characters in log output).
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

PARQUET_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\_cleanup\recommend_to_delete\fantasy_football_data_downloads_4.6GB\fantasy_football_data\KMFFL\kmffl_import"
)

# Canonical 2024-2025 franchise_ids (subset, matches live Fly DB).
CANONICAL = {
    "Adin": "3FJVUGAHEWQKCU2Z2TQQQMPWEA",
    "Daniel": "KM4FIU3EKJKO4RYTLCNIPAIMWI",
    "Eleff": "QAWPKGQTG3HQVP4BPUGBG5SY2U",
    "Ezra": "SAQHTT6JGGOWN3ROP6QMGS6KTU",
    "Gavi": "GHZOUGTBZIYGQ6QOMLOD4FZLGA",
    "Jason": "46SSFIBGNE3ZZFEDG7GYGV3HN4",
    "Jesse": "5KVNMK2CWFQTDQTVYQLOPGLXFQ",
    "Marc": "FYW5PMKZ23OGUAONDX6AGROCZQ",
    "Tani": "JKFP2XZZK4L36MF2DI7CEWS3J4",
    "Yaacov": "2OUWRYUIY4HHEW72H5YVBHPILA",
}


def hr(title: str) -> None:
    print(f"\n{'=' * 75}\n{title}\n{'=' * 75}")


def setup_db() -> duckdb.DuckDBPyConnection:
    """Fresh DuckDB with minimal schemas covering what we test."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")

    conn.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager VARCHAR, manager_guid VARCHAR, franchise_id VARCHAR,
            yahoo_player_id VARCHAR, "NFL_player_id" VARCHAR,
            player VARCHAR, player_week VARCHAR,
            fantasy_position VARCHAR, position VARCHAR, nfl_team VARCHAR,
            fantasy_points DOUBLE, is_started INTEGER,
            player_lamar DOUBLE, manager_lamar DOUBLE,
            league_id VARCHAR, data_source VARCHAR, platform VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager VARCHAR, manager_guid VARCHAR, franchise_id VARCHAR,
            team_name VARCHAR, team_key VARCHAR,
            opponent VARCHAR, opponent_franchise_id VARCHAR,
            team_points DOUBLE, opponent_points DOUBLE,
            win INTEGER, loss INTEGER, tie INTEGER,
            league_id VARCHAR, data_source VARCHAR, platform VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            year INTEGER, week INTEGER, "NFL_player_id" VARCHAR,
            player_week VARCHAR, player VARCHAR, position VARCHAR,
            fpts_4pt_0ppr DOUBLE, fpts_4pt_ppr DOUBLE
        )
    """)
    return conn


def seed_canonical_2024_2025(conn: duckdb.DuckDBPyConnection) -> None:
    """Pre-load 2024 (full-import-already-fetched) and 2025 (quick-imported)
    rows with FULL enrichments. The staging merger must NOT touch these."""

    # 2024 player rows for 3 managers (full-import shape — yahoo fetcher output)
    rows_2024 = []
    for week in (1, 2):
        rows_2024.extend(
            [
                (
                    "kmffl",
                    2024,
                    week,
                    "Adin",
                    CANONICAL["Adin"],
                    CANONICAL["Adin"],
                    "10001",
                    "00-0034796",
                    "Patrick Mahomes",
                    f"00-0034796_2024_{week}",
                    "QB",
                    "QB",
                    25.0,
                    1,
                    8.5,
                    8.5,
                    "kmffl_2024",
                ),
                (
                    "kmffl",
                    2024,
                    week,
                    "Marc",
                    CANONICAL["Marc"],
                    CANONICAL["Marc"],
                    "10002",
                    "00-0033873",
                    "Justin Jefferson",
                    f"00-0033873_2024_{week}",
                    "WR",
                    "WR",
                    22.0,
                    1,
                    9.1,
                    9.1,
                    "kmffl_2024",
                ),
            ]
        )
    # 2025 quick-import: Bo Nix on Adin's roster with canonical player_week
    rows_2025 = [
        (
            "kmffl",
            2025,
            1,
            "Adin",
            CANONICAL["Adin"],
            CANONICAL["Adin"],
            "40875",
            "00-0039732",
            "Bo Nix",
            "00-0039732_2025_1",
            "QB",
            "QB",
            6.84,
            1,
            5.5,
            5.5,
            "kmffl_2025",
        ),
        (
            "kmffl",
            2025,
            2,
            "Adin",
            CANONICAL["Adin"],
            CANONICAL["Adin"],
            "40875",
            "00-0039732",
            "Bo Nix",
            "00-0039732_2025_2",
            "QB",
            "QB",
            20.24,
            1,
            5.5,
            5.5,
            "kmffl_2025",
        ),
    ]
    conn.executemany(
        """
        INSERT INTO public.player_fantasy (
            db_name, year, week, manager, manager_guid, franchise_id,
            yahoo_player_id, "NFL_player_id", player, player_week,
            fantasy_position, position, fantasy_points, is_started,
            player_lamar, manager_lamar, league_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        rows_2024 + rows_2025,
    )

    # Matchup for 2024 + 2025 (Adin vs Marc)
    matchup_rows = []
    for year in (2024, 2025):
        for week in (1, 2):
            matchup_rows.append(
                (
                    "kmffl",
                    year,
                    week,
                    "Adin",
                    CANONICAL["Adin"],
                    CANONICAL["Adin"],
                    f"Adin {year}",
                    "Marc",
                    120.0,
                    100.0,
                    1,
                    0,
                    0,
                    f"kmffl_{year}",
                )
            )
            matchup_rows.append(
                (
                    "kmffl",
                    year,
                    week,
                    "Marc",
                    CANONICAL["Marc"],
                    CANONICAL["Marc"],
                    f"Marc {year}",
                    "Adin",
                    100.0,
                    120.0,
                    0,
                    1,
                    0,
                    f"kmffl_{year}",
                )
            )
    conn.executemany(
        """
        INSERT INTO public.matchup (
            db_name, year, week, manager, manager_guid, franchise_id,
            team_name, opponent, team_points, opponent_points, win, loss, tie, league_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        matchup_rows,
    )

    # super_table fixtures for the 5 distinct (player, year, week) combos.
    conn.execute("""
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES
            (2024, 1, '00-0034796', '00-0034796_2024_1', 'Patrick Mahomes', 'QB', 25.0, 25.0),
            (2024, 2, '00-0034796', '00-0034796_2024_2', 'Patrick Mahomes', 'QB', 25.0, 25.0),
            (2024, 1, '00-0033873', '00-0033873_2024_1', 'Justin Jefferson', 'WR', 22.0, 22.0),
            (2024, 2, '00-0033873', '00-0033873_2024_2', 'Justin Jefferson', 'WR', 22.0, 22.0),
            (2025, 1, '00-0039732', '00-0039732_2025_1', 'Bo Nix', 'QB', 6.84, 6.84),
            (2025, 2, '00-0039732', '00-0039732_2025_2', 'Bo Nix', 'QB', 20.24, 20.24),
            (2025, 3, '00-0039732', '00-0039732_2025_3', 'Bo Nix', 'QB', 13.42, 13.42)
    """)


def make_fake_db(conn):
    """Wrap real DuckDB conn in a LocalLeagueDB-like API for the merger."""
    fake = SimpleNamespace()

    def table_exists(name):
        return bool(
            conn.execute(
                f"SELECT 1 FROM information_schema.tables " f"WHERE table_name='{name}' AND table_schema='public'"
            ).fetchone()
        )

    def row_count(name):
        if not table_exists(name):
            return 0
        return conn.execute(f"SELECT COUNT(*) FROM public.{name}").fetchone()[0]

    def read_table(name):
        return conn.execute(f"SELECT * FROM public.{name}").fetchdf()

    def execute_sql(sql):
        conn.execute(sql)

    # Mirror the real LocalLeagueDB.save_table: run the canonical normalizer
    # for this table type so franchise_id, player_week, etc. get derived
    # correctly. Without this, matchup rows land with manager_guid set but
    # franchise_id=NULL because normalize_matchup_df is what bridges them.
    from multi_league.core.canonical_matchup import normalize_matchup_df
    from multi_league.core.canonical_roster import normalize_roster_df

    _NORMALIZERS = {
        "matchup": normalize_matchup_df,
        "player_fantasy": normalize_roster_df,
    }

    def save_table(name, df, year=None, platform=None, league_id=None):
        normalizer = _NORMALIZERS.get(name)
        if normalizer is not None and df is not None and len(df) > 0:
            try:
                df = normalizer(df, platform=platform or "yahoo", league_id=league_id)
            except Exception as e:
                print(f"  [WARN] normalizer failed for {name}: {e}")

        target_cols = {
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='public' AND table_name='{name}'"
            ).fetchall()
        }
        common = [c for c in df.columns if c in target_cols]
        if not common:
            return
        sub = df[common].copy()
        if "db_name" in target_cols and "db_name" not in sub.columns:
            sub.insert(0, "db_name", "kmffl")
            common = ["db_name"] + common
        if year is not None:
            conn.execute(f"DELETE FROM public.{name} WHERE year = {int(year)}")
        conn.register("__incoming", sub)
        col_list = ", ".join(f'"{c}"' for c in common)
        conn.execute(f"INSERT INTO public.{name} ({col_list}) " f"SELECT {col_list} FROM __incoming")
        conn.unregister("__incoming")

    fake.table_exists = table_exists
    fake.row_count = row_count
    fake.read_table = read_table
    fake.execute_sql = MagicMock(side_effect=execute_sql)
    fake.save_table = MagicMock(side_effect=save_table)
    fake.connect = lambda: conn
    return fake


def run_staging_merger(conn) -> None:
    """Run the REAL merge_staging_data on the KMFFL 2013/2014 parquets."""
    from multi_league.core.data_normalization import harmonize_dtypes
    from multi_league.data_fetchers.shared import staging_data_merger
    import multi_league.data_fetchers.shared.staging_reader as sr_mod

    matchup_2013 = pd.read_parquet(PARQUET_DIR / "matchup_data_week_all_year_2013.parquet")
    matchup_2014 = pd.read_parquet(PARQUET_DIR / "matchup_data_week_all_year_2014.parquet")
    player_2014 = pd.read_parquet(PARQUET_DIR / "yahoo_player_stats_2014_all_weeks.parquet")
    matchup = pd.concat([matchup_2013, matchup_2014], ignore_index=True)
    matchup["league_id"] = "kmffl_2014"

    def fake_read_staging(_):
        return {"matchup": matchup.copy(), "player": player_2014.copy()}

    original = sr_mod.read_staging_data
    sr_mod.read_staging_data = fake_read_staging

    try:
        ctx = SimpleNamespace(
            league_id="kmffl_canonical",
            league_name="kmffl",
            platform="yahoo",
            has_external_data=True,
            external_column_maps=[
                # Deliberately omit manager_guid / team_name / yahoo_player_id
                # from column_map. The franchise_id-alone fallback (0593d422) +
                # PRESERVED_IDENTITY_COLS (8acc7d50) should still produce a
                # correct merged state.
                {
                    "table": "matchup",
                    "column_map": {
                        "year": "year",
                        "week": "week",
                        "manager": "manager",
                        "opponent": "opponent",
                        "team_points": "team_points",
                        "opponent_points": "opponent_points",
                        "win": "win",
                    },
                },
                {
                    "table": "player_fantasy",
                    "column_map": {
                        "year": "year",
                        "week": "week",
                        "manager": "manager",
                        "player": "player",
                        "points": "points",
                        "fantasy_position": "fantasy_position",
                    },
                },
            ],
            external_identity_maps={
                **{m: {"franchise_id": fid} for m, fid in CANONICAL.items()},
                "Rubinstein": {"create_new": True},
                "Ilan": {"create_new": True},
            },
        )
        fake_db = make_fake_db(conn)
        stats = staging_data_merger.merge_staging_data(
            ctx=ctx,
            db=fake_db,
            harmonize_dtypes_func=harmonize_dtypes,
            log_func=print,
        )
        print(f"\n[STAGING] stats: {stats}")
    finally:
        sr_mod.read_staging_data = original


def run_ensure_player_week(conn) -> None:
    """Run the real ensure_player_week enrichment."""
    from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
    from multi_league.transformations.player.sql_player_enrichments import PlayerEnrichmentsMixin

    class _Runner(PlayerEnrichmentsMixin, SQLEnrichmentsBase):
        pass

    # data_dir must be truthy so _qualified_name returns "public.<table>"
    # instead of "___leagues.public.<table>" (which would miss our in-memory
    # tables and silently no-op).
    runner = _Runner(db_name="kmffl", data_dir="local")
    runner._conn = conn
    runner._platform = "yahoo"
    runner.ensure_player_week()


# ----------------------------------------------------------------------------
# Assertions
# ----------------------------------------------------------------------------


def check_2024_untouched(conn):
    """2024 (canonical full-import data) must not have been touched."""
    rows = conn.execute("""
        SELECT yahoo_player_id, fantasy_points, manager_lamar, player_week
        FROM public.player_fantasy
        WHERE year=2024 ORDER BY week, yahoo_player_id
    """).fetchall()
    if len(rows) != 4:
        return False, f"expected 4 rows for 2024, got {len(rows)}"
    for pid, fp, ml, pw in rows:
        if not pw or pw.startswith("YAHOO-") or fp == 0 or ml is None:
            return False, f"2024 row clobbered: pid={pid} fp={fp} ml={ml} pw={pw}"
    return True, "all 4 rows preserved (yahoo_player_id, fp, manager_lamar, canonical player_week)"


def check_2025_quick_enrichments(conn):
    """2025 quick-import enrichments must survive the staging merger."""
    rows = conn.execute("""
        SELECT week, fantasy_points, manager_lamar, player_week
        FROM public.player_fantasy
        WHERE year=2025 AND yahoo_player_id='40875'
        ORDER BY week
    """).fetchall()
    if len(rows) != 2:
        return False, f"expected 2 Bo Nix rows for 2025, got {len(rows)}"
    for week, fp, ml, pw in rows:
        if pw != f"00-0039732_2025_{week}" or fp == 0 or ml is None:
            return False, f"2025 wk{week} clobbered: fp={fp} ml={ml} pw={pw}"
    return True, "Bo Nix's 2 quick-import rows preserved (canonical pw, fp, manager_lamar intact)"


def check_2014_player_survived(conn):
    """8acc7d50: 2014 player rows must not collapse from ~2,560 to ~160."""
    n = conn.execute("SELECT COUNT(*) FROM public.player_fantasy WHERE year=2014").fetchone()[0]
    pids = conn.execute("SELECT COUNT(DISTINCT yahoo_player_id) FROM public.player_fantasy WHERE year=2014").fetchone()[
        0
    ]
    if n < 2000:
        return False, f"only {n} rows / {pids} pids survived (expected ~2,560 / 250+)"
    return True, f"{n} 2014 rows / {pids} distinct yahoo_player_ids survived"


def check_2014_matchup_canonical_franchise_ids(conn):
    """2013/2014 matchup managers map to the canonical 2015+ franchise_ids."""
    rows = conn.execute(
        """
        SELECT manager, franchise_id, COUNT(*) n
        FROM public.matchup WHERE year IN (2013, 2014)
        GROUP BY manager, franchise_id ORDER BY year_min, manager
    """.replace("year_min", "manager")
    ).fetchall()
    misses = []
    for mgr, fid, _n in rows:
        expected = CANONICAL.get(mgr)
        if expected is not None and fid != expected:
            misses.append(f"{mgr}: got {fid} (expected {expected})")
    if misses:
        return False, "; ".join(misses)
    canonical_count = sum(1 for mgr, fid, _ in rows if CANONICAL.get(mgr) == fid)
    return True, f"{canonical_count} (manager, franchise_id) groups matched canonical"


def check_player_week_regenerated(conn):
    """6aff3607: stale YAHOO- prefix regenerates once NFL_player_id is set."""
    # Inject a stale row: NFL_player_id resolved but player_week is YAHOO-.
    conn.execute("""
        INSERT INTO public.player_fantasy (
            db_name, year, week, manager, yahoo_player_id, "NFL_player_id",
            player, player_week, fantasy_position, fantasy_points,
            is_started, league_id
        ) VALUES (
            'kmffl', 2025, 3, 'Adin', '40875', '00-0039732',
            'Bo Nix', 'YAHOO-40875_2025_3', 'BN', 0, 0, 'kmffl_2025'
        )
    """)
    run_ensure_player_week(conn)
    pw = conn.execute("""
        SELECT player_week FROM public.player_fantasy
        WHERE year=2025 AND week=3 AND yahoo_player_id='40875'
    """).fetchone()[0]
    if pw != "00-0039732_2025_3":
        return False, f"player_week stayed at {pw!r}"
    return True, f"YAHOO-40875_2025_3 -> {pw}"


def main() -> int:
    hr("STEP 1: in-memory DuckDB + canonical 2024 + 2025 quick-import seed")
    conn = setup_db()
    seed_canonical_2024_2025(conn)
    n_pre = conn.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    print(f"  seeded {n_pre} canonical player rows (2024 + 2025)")

    hr("STEP 2: REAL staging merger on KMFFL 2013/2014 parquets")
    run_staging_merger(conn)

    hr("STEP 3: assertions")
    checks = [
        ("9dad4189 2024 untouched     ", check_2024_untouched),
        ("9dad4189 2025 enrichments   ", check_2025_quick_enrichments),
        ("8acc7d50 2014 player rows   ", check_2014_player_survived),
        ("matchup 2013/2014 canonical ", check_2014_matchup_canonical_franchise_ids),
        ("6aff3607 player_week regen  ", check_player_week_regenerated),
    ]
    n_pass = 0
    for name, fn in checks:
        try:
            ok, msg = fn(conn)
        except Exception as e:
            ok, msg = False, f"raised: {e}"
        flag = "OK  " if ok else "FAIL"
        print(f"  [{flag}] {name} {msg}")
        n_pass += int(ok)

    hr("RESULT")
    if n_pass == len(checks):
        print(f"all {n_pass}/{len(checks)} checks passed - safe to re-run KMFFL")
        return 0
    print(f"{n_pass}/{len(checks)} passed - investigate before re-running")
    return 1


if __name__ == "__main__":
    import sys as _sys

    if "--schema-conform" not in _sys.argv:
        _sys.exit(main())


# ============================================================================
# PHASE 1.7 / schema_conform sim helpers (Task 16 of schema_conform plan)
# ============================================================================


def _safe_col(c: str) -> str:
    """Mirrors frontend _safeColName."""
    import re

    return re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")


def simulate_upload_route(
    conn: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    table: str,
    db_name: str,
) -> None:
    """Mirrors frontend/src/app/api/import/upload-staging/route.ts:
    CREATE TABLE staging.staging_<table> with EVERY source col as VARCHAR. NO column strip.
    """
    df = pd.read_parquet(parquet_path)
    canonical_cols = [_safe_col(c) for c in df.columns]
    df.columns = canonical_cols
    cols_ddl = ", ".join(f"{c} VARCHAR" for c in canonical_cols)

    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS staging.staging_{table} " f"(db_name VARCHAR, filename VARCHAR, {cols_ddl})"
    )
    df["db_name"] = db_name
    df["filename"] = parquet_path.name
    cols = ["db_name", "filename"] + canonical_cols
    conn.register("upload_view", df.astype(str).where(df.notna(), None)[cols])
    conn.execute(f"INSERT INTO staging.staging_{table} SELECT * FROM upload_view")
    conn.unregister("upload_view")


def simulate_phase_1_7(conn: duckdb.DuckDBPyConnection, db_name: str) -> None:
    from multi_league.external_ingest.schema_conform import run

    run(conn=conn, db_name=db_name, run_id="sim-test")


def assert_no_identity_columns_dropped_at_upload(conn) -> None:
    """The exact assertion that today's sim is missing — would have caught
    the upload-route schema bug for the past N versions of this code."""
    cols = (
        conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='staging' AND table_name='staging_player_fantasy'"
        )
        .df()["column_name"]
        .tolist()
    )
    for required in ("manager_guid", "team_key", "yahoo_player_id", "player_key"):
        assert required in cols, f"upload route stripped {required} (today's bug)"


def assert_kmffl_known_managers_resolve(conn) -> None:
    KNOWN = {
        "ezra": "SAQHTT6JGGOWN3ROP6QMGS6KTU",
        "marc": "FYW5PMKZ23OGUAONDX6AGROCZQ",
        "tani": "JKFP2XZZK4L36MF2DI7CEWS3J4",
        "yaacov": "2OUWRYUIY4HHEW72H5YVBHPILA",
        "jason": "46SSFIBGNE3ZZFEDG7GYGV3HN4",
        "jesse": "5KVNMK2CWFQTDQTVYQLOPGLXFQ",
        "gavi": "GHZOUGTBZIYGQ6QOMLOD4FZLGA",
        "adin": "3FJVUGAHEWQKCU2Z2TQQQMPWEA",
        "daniel": "KM4FIU3EKJKO4RYTLCNIPAIMWI",
    }
    for table in ("matchup", "player_fantasy", "draft", "transactions"):
        try:
            df = conn.execute(
                f"SELECT manager, manager_guid FROM staging.conformed_{table} "
                f"WHERE db_name='kmffl' AND year IN (2013, 2014)"
            ).df()
        except Exception:
            continue
        for src_name, expected_guid in KNOWN.items():
            rows = df[df["manager"].str.lower().str.strip() == src_name]
            if rows.empty:
                continue
            assert (
                rows["manager_guid"] == expected_guid
            ).all(), f"{table}: known manager {src_name} not resolving to canonical guid"


def assert_kmffl_new_managers_get_stable_synthetic_ids(conn) -> None:
    """For new managers (not in canonical) who have NO source guid, schema_conform
    should mint a stable external_<hash>. If the source already has a real guid,
    that's preserved instead — that's the correct pipeline behavior, not a bug.
    """
    # Pull both source and conformed rows
    try:
        raw = conn.execute("SELECT manager, manager_guid FROM staging.staging_matchup " "WHERE db_name='kmffl'").df()
    except Exception:
        raw = pd.DataFrame(columns=["manager", "manager_guid"])

    conformed = conn.execute(
        "SELECT manager, manager_guid FROM staging.conformed_matchup " "WHERE db_name='kmffl' AND year IN (2013, 2014)"
    ).df()

    for new_name in ("rubinstein", "ilan"):
        rows = conformed[conformed["manager"].str.lower().str.strip() == new_name]
        if rows.empty:
            continue
        unique_guids = rows["manager_guid"].dropna().unique()
        assert len(unique_guids) == 1, f"{new_name} got {len(unique_guids)} different guids: {list(unique_guids)}"
        # Was the source guid populated for this manager?
        src_rows = raw[raw["manager"].str.lower().str.strip() == new_name]
        src_guids = src_rows["manager_guid"].dropna().unique() if "manager_guid" in src_rows.columns else []
        if len(src_guids) > 0:
            # Source had a real guid — pipeline should preserve it
            assert unique_guids[0] == src_guids[0], (
                f"{new_name} source had guid {src_guids[0]!r} " f"but conformed has {unique_guids[0]!r}"
            )
        else:
            # Source had no guid — pipeline should have minted external_*
            assert unique_guids[0].startswith(
                "external_"
            ), f"{new_name} had no source guid but got non-synthetic {unique_guids[0]!r}"


def assert_decision_ledger_complete(conn) -> None:
    decisions = conn.execute(
        "SELECT table_name, ddl_slot, status FROM staging.schema_decisions " "WHERE db_name='kmffl'"
    ).df()
    required_per_table = {
        "matchup": ["year", "week", "manager_guid"],
        "player_fantasy": ["year", "week", "manager_guid", "yahoo_player_id"],
        "draft": ["year", "round", "pick", "manager_guid", "yahoo_player_id"],
        "transactions": ["year", "week", "transaction_id", "manager_guid", "yahoo_player_id"],
    }
    for table, slots in required_per_table.items():
        for slot in slots:
            match = decisions[(decisions.table_name == table) & (decisions.ddl_slot == slot)]
            assert not match.empty, f"no ledger entry for {table}.{slot}"
            assert match.iloc[0]["status"] in (
                "bound",
                "derived",
                "coalesced",
                "routed_yahoo_settings_v1",
            ), f"{table}.{slot} ledger status is {match.iloc[0]['status']}"


def assert_idempotent_phase_1_7(conn, db_name: str) -> None:
    """Run schema_conform.run() twice. Conformed rows + ledger rows should match
    (modulo run_id / timestamp)."""
    from multi_league.external_ingest.schema_conform import run as conform_run

    conform_run(conn=conn, db_name=db_name, run_id="idem-1")
    rows1 = (
        conn.execute(
            "SELECT * FROM staging.conformed_matchup WHERE db_name=? ORDER BY year, week, manager_guid",
            [db_name],
        )
        .df()
        .drop(columns=["db_name"], errors="ignore")
    )
    conform_run(conn=conn, db_name=db_name, run_id="idem-2")
    rows2 = (
        conn.execute(
            "SELECT * FROM staging.conformed_matchup WHERE db_name=? ORDER BY year, week, manager_guid",
            [db_name],
        )
        .df()
        .drop(columns=["db_name"], errors="ignore")
    )
    pd.testing.assert_frame_equal(rows1.reset_index(drop=True), rows2.reset_index(drop=True))


def main_schema_conform() -> int:
    """End-to-end sim: upload route + PHASE 1.7 against KMFFL parquets.

    Returns 0 on success, non-zero on assertion failure.
    """
    print("=" * 80)
    print("SCHEMA_CONFORM SIM — exercising upload route + PHASE 1.7")
    print("=" * 80)

    # Check parquets exist before doing anything else
    parquet_map = {
        "matchup": "matchup_data_week_all_year_2014.parquet",  # 2014 only (has both years in practice)
        "player_fantasy": "yahoo_player_stats_2014_all_weeks.parquet",
        "draft": "draft_data_2014.parquet",
        "transactions": "transactions_year_2014.parquet",
    }
    available = {t: PARQUET_DIR / f for t, f in parquet_map.items() if (PARQUET_DIR / f).exists()}
    if not available:
        print(f"\n[SKIP] No KMFFL parquets found at {PARQUET_DIR}")
        print("       Set PARQUET_DIR to your local KMFFL import directory to run this sim.")
        return 0

    print(f"[INFO] Found {len(available)}/{len(parquet_map)} parquet(s): {list(available)}")

    conn = duckdb.connect(":memory:")

    # Seed minimal canonical context so the matcher has reference data
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    # IMPORTANT: do NOT include team_key in the canonical matchup DDL.
    # The matchup parquet has no team_key column, so schema_conform would
    # mistakenly bind team_key to the highest-scoring alternative (e.g. team_points).
    # That produces wrong values in the conformed matchup and causes the cross-table
    # team_key consistency check to fail with 0% overlap against draft/player_fantasy.
    # Omitting team_key from the ref entirely keeps it out of filtered_pass_slots
    # for matchup → no bad binding → cross-table check only compares tables that
    # genuinely have team_key (draft and player_fantasy), which share the same format.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager VARCHAR, manager_guid VARCHAR, team_name VARCHAR,
            opponent VARCHAR, franchise_id VARCHAR,
            team_points DOUBLE, opponent_points DOUBLE,
            win INTEGER, loss INTEGER, tie INTEGER
        )
    """)
    # team_key values for use in draft/player_fantasy seeds (same league format).
    _TEAM_KEYS = {name: f"331.l.381581.t.{i + 1}" for i, name in enumerate(CANONICAL)}
    for name, guid in CANONICAL.items():
        for week in range(1, 18):
            conn.execute(
                "INSERT INTO public.matchup VALUES " "('kmffl', 2024, ?, ?, ?, ?, 'Adin', ?, 100.0, 95.0, 1, 0, 0)",
                [week, name, guid, f"Team {name}", guid],
            )

    # Seed minimal canonical player_fantasy, draft, transactions so _build_ref_cols
    # returns non-empty dicts for those tables. Without this, filtered_pass_slots is
    # empty and no ledger entries are written, causing decision_ledger_complete to fail.
    #
    # Design constraints:
    #  1. Only include manager_guid in tables whose source parquets ALSO have it.
    #     schema_conform treats manager_guid as REQUIRED_IDENTITY when it appears in
    #     ref_cols — seeding it for draft/transactions (whose parquets lack it) would
    #     trigger UNMAPPED_REQUIRED and abort the run.
    #     - player_fantasy parquet HAS manager_guid  → seed WITH it
    #     - draft parquet lacks manager_guid          → seed WITHOUT it
    #     - transactions parquet lacks manager_guid   → seed WITHOUT it
    #  2. Omit team_key from player_fantasy seed. The cross-table consistency check
    #     compares team_key values across conformed tables; if player_fantasy ref has
    #     a different team_key format than matchup ref, the check fails at 0% overlap.
    #     Since team_key is NOT a required identity slot for player_fantasy, omitting
    #     it from ref is safe — player_fantasy.team_key just won't appear in the ledger.
    #  3. Use realistic value formats matching the KMFFL parquet (yahoo_player_id as
    #     numeric strings, team_key as "331.l.XXXXX.t.N", transaction_id as
    #     "331.l.XXXXX.tr.N") so value_jaccard gives meaningful scores.
    #  4. For transactions, the co-occurrence filter for transaction_id uses manager as
    #     anchor (MI≈0.57). We seed 30 actual rows from the parquet so the MI threshold
    #     is comfortably satisfied.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager VARCHAR, manager_guid VARCHAR,
            yahoo_player_id VARCHAR, player VARCHAR,
            fantasy_position VARCHAR, fantasy_points DOUBLE
        )
    """)
    _PF_PIDS = [str(p) for p in (10001, 10002, 10003, 10004, 10005)]
    for name, guid in CANONICAL.items():
        for pid in _PF_PIDS:
            conn.execute(
                "INSERT INTO public.player_fantasy VALUES " "('kmffl', 2024, 1, ?, ?, ?, 'Player', 'WR', 10.5)",
                [name, guid, pid],
            )

    # draft parquet lacks manager_guid — omit it from the reference table so
    # schema_conform does not require an impossible match.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.draft (
            db_name VARCHAR, year INTEGER, round INTEGER, pick INTEGER,
            manager VARCHAR, team_key VARCHAR,
            yahoo_player_id VARCHAR, player VARCHAR, cost DOUBLE
        )
    """)
    _DRAFT_PIDS = [str(p) for p in (3727, 3950, 4256, 4269, 4416, 4682, 5234, 5967, 6760, 7168)]
    for i, (name, _guid) in enumerate(CANONICAL.items()):
        conn.execute(
            "INSERT INTO public.draft VALUES " "('kmffl', 2024, 1, ?, ?, ?, ?, 'Player', 0)",
            [i + 1, name, _TEAM_KEYS[name], _DRAFT_PIDS[i % len(_DRAFT_PIDS)]],
        )

    # transactions parquet lacks manager_guid — same treatment as draft.
    # Seed with actual KMFFL 2014 rows (top 30) so co-occurrence MI is realistic.
    # The co-occurrence anchor for transaction_id is now "manager" (MI≈0.57 > 0.5),
    # fixed in COOCCURRENCE_ANCHORS_PER_TABLE after discovering "transaction_type" had
    # MI≈0.035 and caused the binding to silently fail.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.transactions (
            db_name VARCHAR, year INTEGER, week INTEGER,
            transaction_id VARCHAR, manager VARCHAR,
            yahoo_player_id VARCHAR, transaction_type VARCHAR
        )
    """)
    _txn_parquet = PARQUET_DIR / "transactions_year_2014.parquet"
    if _txn_parquet.exists():
        _df_txn = pd.read_parquet(_txn_parquet)
        for _, _row in _df_txn.head(30).iterrows():
            conn.execute(
                "INSERT INTO public.transactions VALUES (?,?,?,?,?,?,?)",
                [
                    "kmffl",
                    int(_row["year"]),
                    int(_row["week"]),
                    str(_row["transaction_id"]),
                    str(_row["manager"]),
                    str(_row["yahoo_player_id"]),
                    str(_row["transaction_type"]),
                ],
            )

    # Simulate upload route for each available parquet
    for table, path in available.items():
        simulate_upload_route(conn, path, table, db_name="kmffl")
        print(f"[INFO] Uploaded staging.staging_{table} from {path.name}")

    # The killer assertion: would have caught the upload-route schema bug in production
    if "player_fantasy" in available:
        try:
            assert_no_identity_columns_dropped_at_upload(conn)
            print("[OK] assert_no_identity_columns_dropped_at_upload")
        except AssertionError as e:
            print(f"[FAIL] {e}")
            return 1

    # Run PHASE 1.7
    try:
        simulate_phase_1_7(conn, db_name="kmffl")
        print("[OK] simulate_phase_1_7 (no SchemaConformAbort raised)")
    except Exception as e:
        print(f"[FAIL] schema_conform raised: {e}")
        return 1

    # Run downstream assertions (skip those whose table isn't available)
    checks = [
        ("known_managers_resolve", assert_kmffl_known_managers_resolve),
        ("new_managers_stable_synthetic", assert_kmffl_new_managers_get_stable_synthetic_ids),
        ("decision_ledger_complete", assert_decision_ledger_complete),
        ("idempotent_phase_1_7", lambda c: assert_idempotent_phase_1_7(c, "kmffl")),
    ]
    failed = 0
    for name, fn in checks:
        try:
            fn(conn)
            print(f"[OK] {name}")
        except AssertionError as e:
            print(f"[FAIL] {name}: {e}")
            failed += 1

    if failed:
        print(f"\n=== SCHEMA_CONFORM SIM: {failed} assertion(s) failed ===")
        return 1
    print("\n=== SCHEMA_CONFORM SIM: all assertions passed ===")
    return 0


if __name__ == "__main__":
    import sys as _sys_main

    if "--schema-conform" in _sys_main.argv:
        _sys_main.exit(main_schema_conform())
