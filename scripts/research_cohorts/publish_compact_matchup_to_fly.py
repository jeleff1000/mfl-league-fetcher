"""Publish only the final compact Research Matchup serving tables to Fly.

This command never reads the GitHub cache, raw player rows, lane artifacts, or
legacy/adaptive matchup tables.  Its only allowed input is the completed compact
DuckDB artifact.  It stages each serving table separately because `/merge-ops`
has a 5 GB request limit; the UI is deployed only after all three tables verify.

Production writes require both ``--apply`` and
``--i-understand-this-writes-prod``.  ``--build-only`` performs every local
semantic gate and measures the actual upload files without contacting Fly.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
COMPACT_SOURCE_TABLES = [
    "research_matchup_compact_weekly",
    "research_matchup_compact_season",
    "research_matchup_compact_career",
]
# These remain compact one-row-per-player/time projections. They move the
# expensive 576-grade-struct extraction out of the request path without
# materializing a row per request selector.
GRADE_SIDECAR_TABLES = [
    "research_matchup_compact_grade_season",
    "research_matchup_compact_grade_career",
]
COMPACT_TABLES = COMPACT_SOURCE_TABLES + GRADE_SIDECAR_TABLES
SERVING_POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF", "DB", "DL", "LB")
WEEKLY_PARTITIONS = 3
OUTER_KEYS = {
    "research_matchup_compact_weekly": ("NFL_player_id", "year", "week", "position"),
    "research_matchup_compact_season": ("NFL_player_id", "year", "position"),
    "research_matchup_compact_career": ("NFL_player_id", "position"),
    "research_matchup_compact_grade_season": ("NFL_player_id", "year", "position"),
    "research_matchup_compact_grade_career": ("NFL_player_id", "position"),
}
OPS_SCHEMA = "nfl_historical"
# Full immutable-cache RB-position inventory, after the shared synthetic-row
# guard.  The numerator counts are CMC's roster/start/outcome facts; 9,837 is
# the weekly all-league RB eligibility denominator.
CMC_WEEK_ONE_GOLDEN = (9837, 9766, 9600, 9579, 6019.0)
MAX_MERGE_OPS_BYTES = 5_000_000_000
VERCEL_REVALIDATE_TIMEOUT_SECONDS = 30
VERCEL_WARM_TIMEOUT_SECONDS = 90
VERCEL_WARM_CONCURRENCY = 8
RESEARCH_MATCHUP_CONFIGS = tuple(
    f"{teams}t-{roster}-{scoring}-{pass_td}"
    for teams in (10, 12)
    for roster in ("flx", "sflx", "idp")
    for scoring in ("std", "half", "ppr")
    for pass_td in ("4pt", "6pt")
)
RESEARCH_MATCHUP_CORE_COLUMNS = {
    "weekly": "NFL_player_id,player,position,roster_rate,start_rate,win_rate,expected_wins,expected_losses,year,week",
    "season": "NFL_player_id,player,position,roster_rate,start_rate,win_rate,expected_wins,expected_losses,year,healthy_start_rate,expected_starts,playoff_started,champ_started",
    "career": "NFL_player_id,player,position,roster_rate,start_rate,win_rate,expected_wins,expected_losses,year,healthy_start_rate,expected_starts,playoff_started,champ_started,expected_playoffs,expected_champs",
}

def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def weekly_partition_table(index: int) -> str:
    if not 0 <= index < WEEKLY_PARTITIONS:
        raise ValueError(f"weekly partition index out of range: {index}")
    return f"research_matchup_compact_weekly_p{index:02d}"


def serving_units() -> list[tuple[str, str, str]]:
    """(source table, Fly serving table, source predicate) in publish order."""
    weekly = "research_matchup_compact_weekly"
    units = [
        (weekly, weekly_partition_table(index),
         f"WHERE hash(NFL_player_id) % {WEEKLY_PARTITIONS} = {index}")
        for index in range(WEEKLY_PARTITIONS)
    ]
    units.extend((table, table, "") for table in COMPACT_TABLES if table != weekly)
    return units


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def _require_table(con: duckdb.DuckDBPyConnection, table: str) -> int:
    try:
        count = int(con.execute(f"SELECT COUNT(*) FROM {_q(table)}").fetchone()[0])
    except duckdb.CatalogException as exc:
        raise SystemExit(f"PREFLIGHT FAIL: compact artifact is missing {table}") from exc
    if not count:
        raise SystemExit(f"PREFLIGHT FAIL: compact artifact table {table} is empty")
    return count


def _scalar(con: duckdb.DuckDBPyConnection, sql: str, failure: str) -> None:
    bad = int(con.execute(sql).fetchone()[0])
    if bad:
        raise SystemExit(f"PREFLIGHT FAIL: {failure} ({bad:,} rows)")


def _assert_outer_identity(con: duckdb.DuckDBPyConnection, table: str) -> None:
    key = ", ".join(_q(column) for column in OUTER_KEYS[table])
    _scalar(
        con,
        f"SELECT COUNT(*) FROM (SELECT {key} FROM {_q(table)} GROUP BY {key} HAVING COUNT(*) <> 1)",
        f"{table} has duplicate outer identity",
    )


def _assert_serving_positions(con: duckdb.DuckDBPyConnection, table: str) -> None:
    allowed = ", ".join(repr(position) for position in SERVING_POSITIONS)
    _scalar(
        con,
        f"SELECT COUNT(*) FROM {_q(table)} WHERE position NOT IN ({allowed})",
        f"{table} has a non-serving position",
    )


def _assert_core_cells(con: duckdb.DuckDBPyConnection, table: str) -> None:
    key = ", ".join(f"r.{_q(column)}" for column in OUTER_KEYS[table])
    selector = ", ".join(
        f"cell.{field}" for field in (
            "q_teams", "q_roster", "q_scoring", "q_pass_td", "q_dynasty", "q_best_ball"
        )
    )
    _scalar(
        con,
        f"""SELECT COUNT(*) FROM (
              SELECT {key}, {selector}
              FROM {_q(table)} r, UNNEST(r.cohort_cells) u(cell)
              GROUP BY {key}, {selector}
              HAVING COUNT(*) <> 1
            )""",
        f"{table} has duplicate core selector cells",
    )
    _scalar(
        con,
        f"""SELECT COUNT(*)
            FROM {_q(table)} r, UNNEST(r.cohort_cells) u(cell)
            WHERE cell.roster_rate_pct < 0 OR cell.roster_rate_pct > 100
               OR cell.start_rate_pct < 0 OR cell.start_rate_pct > 100
               OR cell.win_rate_pct < 0 OR cell.win_rate_pct > 100""",
        f"{table} has impossible core rate",
    )


def _assert_grade_cells(con: duckdb.DuckDBPyConnection, table: str) -> None:
    if table == "research_matchup_compact_weekly":
        return
    key = ", ".join(f"r.{_q(column)}" for column in OUTER_KEYS[table])
    selector = ", ".join(
        f"cell.{field}" for field in (
            "q_teams", "q_roster", "q_scoring", "q_pass_td", "q_dynasty", "q_best_ball",
            "q_playoff_teams",
        )
    )
    _scalar(
        con,
        f"""SELECT COUNT(*) FROM (
              SELECT {key}, {selector}
              FROM {_q(table)} r, UNNEST(r.grade_cells) u(cell)
              GROUP BY {key}, {selector}
              HAVING COUNT(*) <> 1
            )""",
        f"{table} has duplicate grade selector cells",
    )
    _scalar(
        con,
        f"""SELECT COUNT(*)
            FROM {_q(table)} r, UNNEST(r.grade_cells) u(cell)
            WHERE cell.champ_rate_pct < 0 OR cell.champ_rate_pct > 100
               OR cell.playoff_rate_pct < 0 OR cell.playoff_rate_pct > 100
               OR cell.champ_rate_pct > cell.playoff_rate_pct""",
        f"{table} has champ rate greater than playoff rate or an impossible grade rate",
    )


def _assert_selector_access_paths(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Require complete, in-range build-time selector maps for serving reads."""
    columns = {str(row[0]) for row in con.execute(f"DESCRIBE {_q(table)}").fetchall()}
    required = {"core_selector_indices", "roster_cells", "roster_selector_indices"}
    if table != "research_matchup_compact_weekly":
        required.add("grade_selector_indices")
    missing = sorted(required - columns)
    if missing:
        raise SystemExit(f"PREFLIGHT FAIL: {table} is missing {', '.join(missing)}")
    _scalar(
        con,
        f"SELECT COUNT(*) FROM {_q(table)} WHERE list_count(core_selector_indices) <> 288",
        f"{table} has incomplete core_selector_indices",
    )
    _scalar(
        con,
        f"""SELECT COUNT(*)
            FROM {_q(table)} r, UNNEST(r.core_selector_indices) u(selector_index)
            WHERE selector_index IS NULL OR selector_index < 1
               OR selector_index > list_count(r.cohort_cells)""",
        f"{table} has out-of-range core_selector_indices",
    )
    _scalar(
        con,
        f"SELECT COUNT(*) FROM {_q(table)} WHERE list_count(roster_selector_indices) <> 288",
        f"{table} has incomplete roster_selector_indices",
    )
    _scalar(
        con,
        f"""SELECT COUNT(*)
            FROM {_q(table)} r, UNNEST(r.roster_selector_indices) u(selector_index)
            WHERE selector_index IS NULL OR selector_index < 1
               OR selector_index > list_count(r.roster_cells)""",
        f"{table} has out-of-range roster_selector_indices",
    )
    if table == "research_matchup_compact_weekly":
        return
    _scalar(
        con,
        f"SELECT COUNT(*) FROM {_q(table)} WHERE list_count(grade_selector_indices) <> 864",
        f"{table} has incomplete grade_selector_indices",
    )
    _scalar(
        con,
        f"""SELECT COUNT(*)
            FROM {_q(table)} r, UNNEST(r.grade_selector_indices) u(selector_index)
            WHERE selector_index IS NULL OR selector_index < 1
               OR selector_index > list_count(r.grade_cells)""",
        f"{table} has out-of-range grade_selector_indices",
    )


def cmc_week_one_golden(con: duckdb.DuckDBPyConnection, source: str = "") -> tuple:
    prefix = f"{source}." if source else ""
    row = con.execute(
        f"""SELECT cell.eligible_leagues, cell.rostered_leagues,
                   cell.started_leagues,
                   cell.valid_started_outcomes, cell.win_equivalent
            FROM {prefix}{_q('research_matchup_compact_weekly')} r,
                 UNNEST(r.cohort_cells) u(cell)
            WHERE r.NFL_player_id = '00-0033280' AND r.year = 2025 AND r.week = 1 AND r.position = 'RB'
              AND cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
              AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
              AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'"""
    ).fetchall()
    if len(row) != 1:
        raise SystemExit(f"PREFLIGHT FAIL: expected exactly one CMC 2025 week-one ALL core cell, found {len(row)}")
    return tuple(row[0])


def preflight_bundle(con: duckdb.DuckDBPyConnection, *, require_full_history: bool = False) -> dict[str, dict]:
    """Read-only compact artifact gates.  No Fly access and no source-cache access."""
    summary: dict[str, dict] = {}
    for table in COMPACT_SOURCE_TABLES:
        rows = _require_table(con, table)
        _assert_outer_identity(con, table)
        _assert_serving_positions(con, table)
        _assert_core_cells(con, table)
        _assert_grade_cells(con, table)
        _assert_selector_access_paths(con, table)
        summary[table] = {"rows": rows}
        print(f"[preflight] {table}: {rows:,} outer rows; identities/cells/rates OK")
    golden = cmc_week_one_golden(con)
    if golden != CMC_WEEK_ONE_GOLDEN:
        raise SystemExit(f"PREFLIGHT FAIL: CMC 2025 week-one ALL golden {golden} != {CMC_WEEK_ONE_GOLDEN}")
    print(f"[preflight] CMC 2025 week-one ALL golden: {golden}")
    if require_full_history:
        season_range = con.execute(
            "SELECT MIN(year), MAX(year), COUNT(DISTINCT year) FROM research_matchup_compact_season"
        ).fetchone()
        start_year, end_year, years = season_range
        if start_year is None or end_year != 2025 or years != end_year - start_year + 1:
            raise SystemExit(
                "PREFLIGHT FAIL: compact season history must be continuous through 2025 "
                f"at the 150-league serving floor, found {season_range}"
            )
        print(
            f"[preflight] cache-supported serving history: "
            f"{start_year}-{end_year} ({years} seasons)"
        )
    return summary


def stage_table(
    bundle: Path,
    source_table: str,
    output_dir: Path,
    *,
    target_table: str | None = None,
    source_predicate: str = "",
    enforce_limit: bool = True,
) -> tuple[Path, int]:
    """Copy one final serving table into its own `/merge-ops` file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target_table = target_table or source_table
    staged = output_dir / f"{target_table}.duckdb"
    if staged.exists():
        staged.unlink()
    con = duckdb.connect(str(staged))
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"ATTACH '{bundle.as_posix()}' AS src (READ_ONLY)")
    con.execute(
        f"CREATE TABLE {_q(target_table)} AS "
        f"SELECT * FROM src.{_q(source_table)} {source_predicate}"
    )
    rows = int(con.execute(f"SELECT COUNT(*) FROM {_q(target_table)}").fetchone()[0])
    con.execute("DETACH src")
    con.execute("CHECKPOINT")
    con.close()
    wal = Path(f"{staged}.wal")
    if wal.exists():
        raise SystemExit(f"STAGING FAIL: checkpoint left WAL beside {staged}")
    size = staged.stat().st_size
    print(f"[stage] {target_table}: {rows:,} rows, {size / 1e9:.3f} GB; sha256={_sha256(staged)[:16]}...")
    if enforce_limit and size > MAX_MERGE_OPS_BYTES:
        raise SystemExit(
            f"TRANSFER FAIL: {target_table} staged file is {size / 1e9:.3f} GB, over the "
            f"/merge-ops {MAX_MERGE_OPS_BYTES / 1e9:.1f} GB limit; do not upload it")
    return staged, rows


def _cohort_code_sql() -> str:
    """Pack six finite selector dimensions into a stable 10-bit integer.

    This preserves one outer player/time row and every metric cell.  It only
    removes six repeated strings from each cell; the frontend can map a user
    format to this exact code without changing any denominator or value.
    """
    return """
      CAST(
        (CASE cell.q_teams WHEN '10t' THEN 1 WHEN '12t' THEN 2 ELSE 0 END)
        + (CASE cell.q_roster WHEN 'flx' THEN 1 WHEN 'sflx' THEN 2 WHEN 'idp' THEN 3 ELSE 0 END) * 4
        + (CASE cell.q_scoring WHEN 'std' THEN 1 WHEN 'half' THEN 2 WHEN 'ppr' THEN 3 ELSE 0 END) * 16
        + (CASE cell.q_pass_td WHEN '4pt' THEN 1 WHEN '6pt' THEN 2 ELSE 0 END) * 64
        + (CASE cell.q_dynasty WHEN 'dynasty' THEN 1 ELSE 0 END) * 256
        + (CASE cell.q_best_ball WHEN 'best_ball' THEN 1 ELSE 0 END) * 512
        AS USMALLINT
      )
    """


def stage_weekly_selector_code_probe(bundle: Path, output_dir: Path) -> Path:
    """Materialize a no-data-loss weekly storage probe from the compact artifact.

    This is deliberately a final-artifact transform, never a cache rebuild. It
    retains every weekly value but replaces repeated six-string selectors with
    a stable code and stores numerics at their safe serving precision.
    """
    table = "research_matchup_compact_weekly"
    output_dir.mkdir(parents=True, exist_ok=True)
    staged = output_dir / f"{table}_selector_code_probe.duckdb"
    if staged.exists():
        staged.unlink()
    con = duckdb.connect(str(staged))
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"ATTACH '{bundle.as_posix()}' AS src (READ_ONLY)")
    con.execute(
        f"""
        CREATE TABLE {_q(table)} AS
        SELECT
          r.NFL_player_id, r.year, r.week, r.position,
          list(struct_pack(
            cohort_code := {_cohort_code_sql()},
            eligible_leagues := CAST(cell.eligible_leagues AS UINTEGER),
            rostered_leagues := CAST(cell.rostered_leagues AS UINTEGER),
            started_leagues := CAST(cell.started_leagues AS UINTEGER),
            valid_started_outcomes := CAST(cell.valid_started_outcomes AS UINTEGER),
            win_equivalent := CAST(cell.win_equivalent AS FLOAT),
            roster_rate_pct := CAST(cell.roster_rate_pct AS FLOAT),
            start_rate_pct := CAST(cell.start_rate_pct AS FLOAT),
            win_rate_pct := CAST(cell.win_rate_pct AS FLOAT),
            expected_starts := CAST(cell.expected_starts AS FLOAT),
            expected_wins := CAST(cell.expected_wins AS FLOAT),
            expected_losses := CAST(cell.expected_losses AS FLOAT),
            clutch_weekly_average := CAST(cell.clutch_weekly_average AS FLOAT)
          ) ORDER BY {_cohort_code_sql()}) AS cohort_cells
        FROM src.{_q(table)} r, UNNEST(r.cohort_cells) u(cell)
        GROUP BY r.NFL_player_id, r.year, r.week, r.position
        """
    )
    con.execute("DETACH src")
    con.execute("CHECKPOINT")
    con.close()
    size = staged.stat().st_size
    print(f"[probe] weekly selector-code layout: {size / 1e9:.3f} GB; sha256={_sha256(staged)[:16]}...")
    return staged


def merge_ops(path: Path) -> None:
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/")
    token = os.environ["DATABASE_ADMIN_TOKEN"]
    with path.open("rb") as handle:
        response = requests.post(
            f"{url}/merge-ops",
            headers={"Authorization": f"Bearer {token}"},
            files={"file": (path.name, handle, "application/octet-stream")},
            timeout=1800,
        )
    if response.status_code != 200:
        raise SystemExit(f"MERGE-OPS FAIL ({response.status_code}): {response.text[:400]}")
    print(f"[merge-ops] {path.name}: {response.json()}")


def research_matchup_warm_urls(site_url: str) -> tuple[str, ...]:
    """Canonical first-page reads populated by the research matchup UI."""
    base = site_url.rstrip("/")
    urls: list[str] = []
    for config in RESEARCH_MATCHUP_CONFIGS:
        for grain, display_columns in RESEARCH_MATCHUP_CORE_COLUMNS.items():
            query = urlencode({
                "grain": grain,
                "limit": "100",
                "offset": "0",
                "sort": "start_rate",
                "dir": "desc",
                "include_count": "0",
                "include_meta": "0",
                "display_columns": display_columns,
                "bracket": "6po",
                "dynasty": "0",
                "bestball": "0",
            })
            urls.append(f"{base}/api/research/{config}/matchup?{query}")
    return tuple(urls)


def refresh_vercel_research_matchup_cache(site_url: str) -> None:
    """Expire then prime all canonical public matchup-research reads on Vercel."""
    secret = os.environ.get("REVALIDATION_SECRET", "").strip()
    if not secret:
        raise RuntimeError("REVALIDATION_SECRET is required to refresh the Vercel research cache")
    base = site_url.rstrip("/")
    response = requests.post(
        f"{base}/api/revalidate",
        params={
            "scope": "research-matchup",
            "secret": secret,
            "strategy": "expire",
        },
        timeout=VERCEL_REVALIDATE_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Vercel research cache revalidation failed ({response.status_code}): {response.text[:400]}")
    payload = response.json()
    if not payload.get("revalidated") or payload.get("scope") != "research-matchup":
        raise RuntimeError(f"Vercel research cache revalidation was not confirmed: {payload}")

    urls = research_matchup_warm_urls(base)
    print(f"[vercel-cache] warming {len(urls)} canonical research matchup reads")

    def warm(url: str) -> None:
        warm_response = requests.get(
            url,
            headers={"User-Agent": "leaguehistory-ops-cache-warmer/1.0"},
            timeout=VERCEL_WARM_TIMEOUT_SECONDS,
        )
        warm_response.raise_for_status()

    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=VERCEL_WARM_CONCURRENCY) as executor:
        futures = {executor.submit(warm, url): url for url in urls}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 - report every failed canonical warm URL
                failures.append(f"{futures[future]}: {exc}")
    if failures:
        raise RuntimeError("Vercel research cache warm failed:\n" + "\n".join(failures[:10]))
    print("[vercel-cache] research matchup cache expired and warmed")


def verify_live(reader, expected: dict[str, int], bundle: Path) -> None:
    for table, want in expected.items():
        live = reader.query(
            f"SELECT COUNT(*) AS n FROM {OPS_SCHEMA}.{_q(table)}", database="___ops"
        )
        found = int(live[0]["n"])
        if found != want:
            raise SystemExit(f"VERIFY FAIL: {table} Fly rows {found:,} != compact artifact {want:,}")
        print(f"[verify] {table}: {found:,} rows")
    con = duckdb.connect(str(bundle), read_only=True)
    expected_cmc = cmc_week_one_golden(con)
    con.close()
    # FlyReader evaluates the same compact-cell SQL as the frontend route.
    weekly_union = " UNION ALL ".join(
        f"SELECT * FROM {OPS_SCHEMA}.{_q(weekly_partition_table(index))}"
        for index in range(WEEKLY_PARTITIONS)
    )
    weekly_count = reader.query(
        f"SELECT COUNT(*) AS n FROM ({weekly_union})", database="___ops"
    )
    if int(weekly_count[0]["n"]) != sum(
        expected[weekly_partition_table(index)] for index in range(WEEKLY_PARTITIONS)
    ):
        raise SystemExit("VERIFY FAIL: weekly Fly partitions do not reconstruct the compact weekly population")
    live = reader.query(
        f"""SELECT cell.eligible_leagues, cell.rostered_leagues,
                   cell.started_leagues,
                   cell.valid_started_outcomes, cell.win_equivalent
            FROM ({weekly_union}) r,
                 UNNEST(r.cohort_cells) u(cell)
            WHERE r.NFL_player_id = '00-0033280' AND r.year = 2025 AND r.week = 1 AND r.position = 'RB'
              AND cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
              AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
              AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'""",
        database="___ops",
    )
    found_cmc = tuple(live[0].values()) if len(live) == 1 else ()
    if found_cmc != expected_cmc or found_cmc != CMC_WEEK_ONE_GOLDEN:
        raise SystemExit(f"VERIFY FAIL: Fly CMC week-one golden {found_cmc} != {CMC_WEEK_ONE_GOLDEN}")
    print(f"[verify] Fly CMC 2025 week-one ALL golden: {found_cmc}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--stage-dir", type=Path, default=Path("out/compact-matchup-publish"))
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--layout-probe", action="store_true",
                        help="measure the no-data-loss coded-selector weekly layout; never writes Fly")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--i-understand-this-writes-prod", action="store_true")
    args = parser.parse_args()
    if args.apply and not args.i_understand_this_writes_prod:
        raise SystemExit("--apply requires --i-understand-this-writes-prod")
    if not args.bundle.exists():
        raise SystemExit(f"compact artifact does not exist: {args.bundle}")
    load_env()
    con = duckdb.connect(str(args.bundle), read_only=True)
    summary = preflight_bundle(con, require_full_history=True)
    con.close()
    if args.layout_probe:
        stage_table(args.bundle, "research_matchup_compact_weekly", args.stage_dir, enforce_limit=False)
        stage_weekly_selector_code_probe(args.bundle, args.stage_dir)
        print("[done] layout probe complete; Fly untouched")
        return 0
    staged = [
        stage_table(
            args.bundle, source_table, args.stage_dir,
            target_table=target_table, source_predicate=source_predicate,
        )
        for source_table, target_table, source_predicate in serving_units()
    ]
    if not args.apply:
        print("[done] compact artifact staged and transfer-safe; Fly untouched")
        return 0
    from multi_league.core.readers.fly_reader import FlyReader
    expected_rows = {path.stem: rows for path, rows in staged}
    for path, _rows in staged:
        merge_ops(path)
    verify_live(FlyReader(), expected_rows, args.bundle)
    refresh_vercel_research_matchup_cache(
        os.environ.get("VERCEL_SITE_URL", "https://www.leaguehistory.app")
    )
    print("[done] compact serving tables published and verified on Fly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
