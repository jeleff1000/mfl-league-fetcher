"""NFL.COM POSITION, RECOVERED FROM PAGES WE ALREADY HAVE.

Joe 2026-08-05: "i dont think its a harvest gap. we have player pages for
every player ever and they show position."

He was right and the "capture gap" reading was wrong. NFL.COM'S ONLY
position COLUMN in the registry (NFLCOM_TEAM_SEASON_ROSTER.position) is
0/144,568 populated, because that crawl walked the ROSTER SITEMAP
(/sitemap/html/rosters/1962/houston-oilers), which lists names and slugs
and no positions. But the player-page cache -- 48,719 files already on
disk -- carries the position in the page's own meta description:

    "Logs for 1977 New York Jets MLB John Ebersole"
     ^kind      ^year ^team           ^POS ^name

So this was never a HARVEST gap. It was a PARSE gap: the bytes were
fetched, the field was never read. Nothing needs re-crawling.

IDENTITY comes from the canonical URL, present on 100% of player pages:
    https://www.nfl.com/players/john-ebersole/stats/logs/1977/
which yields the slug and the season directly, and joins to pfr_id through
nflcom_slug_pfrid.

GRAIN: one row per (slug, season, page_kind). A player with logs, splits
and situational pages for a season gives three rows that must agree --
free corroboration within the root.

Positions are DETAILED (MLB, OLB, FS, NT, LS...) and map to broad through
position_taxonomy like any other witness.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

CACHE = Path(r"D:/league-history-data/nfl/raw/nflcom/cache")
LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "nflcom_player_page_positions.parquet"
SHARDS = LAKE / "nflcom_position_shards"
# MUST NOT CHANGE while shards exist: shard N covers files
# [(N-1)*CHUNK, N*CHUNK) of the sorted list, so the resume arithmetic and
# every shard already on disk are tied to this number. Shrinking it to 2000
# with 8 four-thousand-file shards present would resume at 16,000 and write
# overlapping slices.
CHUNK = 4000

DESC = re.compile(r'name="description" content="([^"]{0,200})"')
CANON = re.compile(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"')
# "Logs for 1977 New York Jets MLB John Ebersole"
PARSE = re.compile(r"^[A-Za-z ]{3,20} for (\d{4}) (.+?) "
                   r"([A-Z]{1,3}(?:/[A-Z]{1,3})?) (.+)$")
SLUG_YEAR = re.compile(r"/players/([a-z0-9\-]+)/(?:stats/)?([a-z]+)?/?(\d{4})?/?$")

# NFL.com ships some pages with its own template unrendered
# ("Splits for 1973 player.CurrentTeam player.Position player.DisplayName").
# Those are the SOURCE's defect, recorded as unparseable rather than guessed.
TEMPLATE_BUG = "player.Position"


def rows_from(path: Path) -> dict | None:
    # 12KB covers <head>, where both the description and the canonical link
    # live. The cache sits on a slow external SSD, so bytes read IS the cost.
    try:
        with open(path, "rb") as fh:
            txt = fh.read(12000).decode("utf-8", errors="ignore")
    except Exception:
        return None
    d = DESC.search(txt)
    if not d:
        return None
    desc = d.group(1)
    if TEMPLATE_BUG in desc:
        return {"unparseable": "source_template_bug"}
    g = PARSE.match(desc)
    if not g:
        return None
    c = CANON.search(txt)
    if not c:
        return None
    su = SLUG_YEAR.search(c.group(1).rstrip("/") + "/")
    if not su:
        return None
    return {"nflcom_slug": su.group(1),
            "season": int(g.group(1)),
            "team_name": g.group(2).strip(),
            "position_raw": g.group(3).strip(),
            "player": g.group(4).strip(),
            "page_kind": (su.group(2) or "main"),
            "source_url": c.group(1)}


def build() -> dict:
    SHARDS.mkdir(parents=True, exist_ok=True)
    files = sorted(CACHE.glob("*.html"))
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    kept, skipped, bug = [], 0, 0
    shard, written = 0, 0
    done = {int(p.stem.split("_")[1]) for p in SHARDS.glob("shard_*.parquet")}

    def flush(n):
        nonlocal kept
        if not kept:
            return
        con.execute("CREATE OR REPLACE TEMP TABLE s AS SELECT * FROM "
                    f"read_json_auto('{(SHARDS / 'tmp.json').as_posix()}')")
        con.execute(f"COPY s TO '{(SHARDS / f'shard_{n}.parquet').as_posix()}' "
                    f"(FORMAT parquet)")
        kept = []

    # I/O bound on a slow external SSD: a serial scan of 48k files runs for
    # hours, a thread pool finishes in minutes. Order does not matter here.
    # RESUME (2026-08-05): the first parallel run died at 28,000 files with
    # 7 shards on disk and no final parquet. Shard N covers files
    # [(N-1)*CHUNK, N*CHUNK) of the SORTED list, so a finished shard means
    # that slice is done -- skip straight past it instead of re-reading
    # 28,000 files off a slow external SSD.
    first = 0
    while (SHARDS / f"shard_{first // CHUNK + 1}.parquet").exists():
        first += CHUNK
    if first:
        print(f"resuming: {first:,} files already sharded, "
              f"{len(files) - first:,} to go", flush=True)
        files = files[first:]

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=12) as pool:
        for i, r in enumerate(pool.map(rows_from, files, chunksize=64)):
            if i and i % CHUNK == 0:
                shard = (first + i) // CHUNK
                if kept:
                    (SHARDS / "tmp.json").write_text(
                        "\n".join(json.dumps(x) for x in kept), encoding="utf-8")
                    flush(shard)
                    written += 1
                print(f"  {i:,}/{len(files):,} scanned, {written} shards",
                      flush=True)
            if r is None:
                skipped += 1
            elif "unparseable" in r:
                bug += 1
            else:
                kept.append(r)
    if kept:
        # tail slice: number it past the last full shard so a resumed run
        # never collides with, or silently overwrites, an existing one
        shard = (first + len(files)) // CHUNK + 1
        (SHARDS / "tmp.json").write_text(
            "\n".join(json.dumps(r) for r in kept), encoding="utf-8")
        flush(shard)

    con.execute(f"""COPY (SELECT DISTINCT * FROM
        read_parquet('{(SHARDS / 'shard_*.parquet').as_posix()}'))
        TO '{OUT.as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 20000)""")
    n, players, y0, y1, dual = con.execute(
        f"""SELECT COUNT(*), COUNT(DISTINCT nflcom_slug), MIN(season),
                   MAX(season), COUNT(*) FILTER (WHERE position_raw LIKE '%/%')
            FROM read_parquet('{OUT.as_posix()}')""").fetchone()
    pre = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT.as_posix()}') "
                      f"WHERE season < 1970").fetchone()[0]
    return {"files_scanned": len(files), "rows": n, "players": players,
            "seasons": f"{y0}-{y1}", "pre_1970_rows": pre,
            "slash_positions": dual,
            "non_player_or_no_description": skipped,
            "source_template_bug": bug, "out": str(OUT)}


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
