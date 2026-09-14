"""Plan matchup rollup work into duration-balanced runner buckets."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import duckdb

# Every planned position must have a real research-table UI lane.  P and OL
# are not valid FLEX/Superflex or IDP results and must never consume a build
# lane or become an outer serving row.
GROUPS = ("QB", "RB", "WR", "TE", "K", "DEF", "DB", "DL", "LB")


def pack_weighted_player_tasks(players: list[dict], runners: int) -> list[dict]:
    """Greedily assign indivisible player work by observed cache-row weight.

    Denominator population is never assigned here.  These lists only decide
    which rostered player outer rows a runner emits; every task still scans its
    complete position/year inventory when calculating cohort denominators.
    """

    if runners < 1:
        raise ValueError("runners must be positive")
    if not players:
        return []
    lanes = [{"bucket": index, "rows": 0, "players": []} for index in range(min(runners, len(players)))]
    for player in sorted(
        players,
        key=lambda item: (-int(item["rows"]), item["year"], item["position"], item["NFL_player_id"]),
    ):
        lane = min(lanes, key=lambda item: (item["rows"], item["bucket"]))
        lane["players"].append(player)
        lane["rows"] += int(player["rows"])
    return lanes


def weighted_player_inventory(
    snapshot: Path,
    start: int,
    end: int,
    positions: tuple[str, ...] | None = None,
) -> list[dict]:
    """Read only the cache keys needed to weight emitted outer-player rows."""

    con = duckdb.connect(str(snapshot), read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT
              CAST(year AS INTEGER) AS year,
              {norm_position('position')} AS position,
              CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
              COUNT(*)::BIGINT AS rows
            FROM public.player_fantasy
            WHERE CAST(year AS INTEGER) BETWEEN ? AND ?
              AND week IS NOT NULL
              AND NFL_player_id IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING MAX(CASE WHEN is_rostered IS NULL OR CAST(is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END) = 1
            """,
            [start, end],
        ).fetchall()
    finally:
        con.close()
    selected = set(GROUPS if positions is None else positions)
    return [
        {"year": int(year), "position": str(position), "NFL_player_id": str(player_id), "rows": int(row_count)}
        for year, position, player_id, row_count in rows
        if position in selected
    ]


def norm_position(expr: str) -> str:
    return f"""CASE
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('QB','RB','WR','TE')
        THEN UPPER(TRIM(CAST({expr} AS VARCHAR)))
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) = 'FB' THEN 'RB'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('K','PK') THEN 'K'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) = 'P' THEN 'P'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DEF','DST','D/ST') THEN 'DEF'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('CB','DB','FS','S','SAF','SS') THEN 'DB'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DE','DL','DT','EDGE','NT') THEN 'DL'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DEFENSIVE END','DEFENSIVE TACKLE') THEN 'DL'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('ILB','LB','MLB','OLB') THEN 'LB'
      WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('C','G','LS','OG','OL','OT','T','OFFENSIVE LINE') THEN 'OL'
      ELSE NULL
    END"""


def inventory(
    snapshot: Path, ops_cache: Path, start: int, end: int
) -> dict[tuple[int, str], tuple[int, int]]:
    con = duckdb.connect(str(snapshot), read_only=True)
    try:
        con.execute(f"ATTACH '{ops_cache.resolve().as_posix()}' AS ops (READ_ONLY)")
        rows = con.execute(f"""
          WITH regular_stats AS (
            SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
              CAST(year AS INTEGER) AS year,
              MAX(NULLIF(TRIM(CAST(position AS VARCHAR)),'')) AS stats_position
            FROM ops.nfl_historical.nfl_player_stats_all
            WHERE CAST(year AS INTEGER) BETWEEN {start} AND {end}
              AND week IS NOT NULL AND COALESCE(season_type,'REG')='REG'
            GROUP BY 1,2
          ), raw AS (
            SELECT CAST(f.year AS INTEGER) AS year,
              {norm_position('token.position')} AS position,
              CAST(f.NFL_player_id AS VARCHAR) AS NFL_player_id, f.is_rostered
            FROM public.player_fantasy f
            INNER JOIN regular_stats s
              ON s.NFL_player_id=CAST(f.NFL_player_id AS VARCHAR)
             AND s.year=CAST(f.year AS INTEGER)
            CROSS JOIN UNNEST(string_split(s.stats_position, ',')) AS token(position)
            WHERE CAST(f.year AS INTEGER) BETWEEN {start} AND {end} AND f.week IS NOT NULL
          ), rostered AS (
            SELECT year,position,NFL_player_id
            FROM raw WHERE position IS NOT NULL AND NFL_player_id IS NOT NULL
            GROUP BY 1,2,3
            HAVING MAX(CASE WHEN CAST(is_rostered AS INTEGER)=1 THEN 1 ELSE 0 END)=1
          )
          SELECT raw.year,raw.position,COUNT(*)::BIGINT AS source_rows,
            COUNT(DISTINCT raw.NFL_player_id)::BIGINT AS emitted_players
          FROM raw INNER JOIN rostered USING(year,position,NFL_player_id)
          GROUP BY 1,2
        """).fetchall()
    finally:
        con.close()
    return {
        (int(year), str(position)): (int(row_count), int(player_count))
        for year, position, row_count, player_count in rows
        if str(position) in GROUPS
    }


def capacity_inventory(
    snapshot: Path,
    start: int,
    end: int,
    positions: tuple[str, ...] | None = None,
) -> dict[tuple[int, str], int]:
    """Read the narrow immutable-cache inventory used only by capacity audits.

    The capacity gate materializes its own position/year fanout directly from
    ``player_fantasy``.  It never consumes player identities or NFL stats, so
    attaching the ops lookup here is wasted work and makes a read-only audit
    depend on data it does not use.
    """

    con = duckdb.connect(str(snapshot), read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT
              CAST(year AS INTEGER) AS year,
              {norm_position('position')} AS position,
              COUNT(*)::BIGINT AS rows
            FROM public.player_fantasy
            WHERE CAST(year AS INTEGER) BETWEEN ? AND ?
              AND week IS NOT NULL
              AND NFL_player_id IS NOT NULL
            GROUP BY 1, 2
            """,
            [start, end],
        ).fetchall()
    finally:
        con.close()
    selected = set(GROUPS if positions is None else positions)
    return {
        (int(year), str(position)): int(row_count)
        for year, position, row_count in rows
        if position in selected
    }


def cost(rows: int, year: int, position: str | None, buckets: int) -> float:
    multiplier = 1.35 if position == "RB" else 1.15 if position in ("WR", "DB", "DL", "LB") else 1.0
    if year < 2020:
        multiplier *= 0.75
    return max(0.5, 4.0 + rows / 1_000_000.0 * 9.0 * multiplier / buckets)


def base_tasks(
    snapshot: Path,
    ops_cache: Path,
    start: int,
    end: int,
    positions: tuple[str, ...] | None = None,
) -> list[dict]:
    counts = inventory(snapshot, ops_cache, start, end)
    selected = GROUPS if positions is None else positions
    out=[]
    for year in range(start, end + 1):
        for position in selected:
            rows, players = counts.get((year, position), (0, 0))
            if rows:
                out.append({"year":year,"position":position,"rows":rows,"players":players,
                            "base_key":f"{year}:{position}","buckets":1})
    return out


def expanded(base: list[dict]) -> list[dict]:
    tasks=[]
    for b in base:
        n=b["buckets"]
        for bucket in range(n):
            tasks.append({
                "year":b["year"], "position":b["position"], "players":b["players"],
                "player_buckets":n, "player_bucket":bucket if n > 1 else None,
                "task_id":f"{b['year']}_{b['position']}_b{bucket + 1}of{n}",
                "rows":b["rows"],
                "estimate":cost(b["rows"],b["year"],b["position"],n),
            })
    return tasks


def pack(tasks: list[dict], n: int) -> list[dict]:
    bins=[{"bucket":i,"estimate":0.0,"tasks":[]} for i in range(n)]
    for task in sorted(
        tasks,
        key=lambda x: (-x["estimate"], x["year"], x["position"], x.get("player_bucket") or 0),
    ):
        target=min(bins,key=lambda x:x["estimate"])
        target["tasks"].append(task)
        target["estimate"]+=task["estimate"]
    for b in bins: b["estimate"]=round(b["estimate"],1)
    return bins


def make_plan(
    snapshot: Path,
    ops_cache: Path,
    start: int,
    end: int,
    runners: int,
    positions: tuple[str, ...] | None = None,
) -> list[dict]:
    if positions and any(position not in GROUPS for position in positions):
        raise ValueError(f"unsupported canonical position: {positions}")
    source_tasks = base_tasks(snapshot, ops_cache, start, end, positions)
    if not source_tasks:
        return []
    # A position/year denominator remains a full-cache calculation inside every
    # task.  Only emitted outer-player identities are hash-partitioned. This
    # keeps metrics exact while allowing WR/RB workloads to use all runners.
    target_seconds = max(
        20.0,
        # Use the actual average task cost. Over-splitting RB/WR leaves empty
        # light lanes and forces small positions onto a few long tails.
        sum(cost(task["rows"], task["year"], task["position"], 1) for task in source_tasks) / runners,
    )
    for task in source_tasks:
        estimated = cost(task["rows"], task["year"], task["position"], 1)
        task["buckets"] = min(
            task["players"],
            max(1, math.ceil(estimated / target_seconds)),
        )
    # Integer rounding above can leave otherwise available GitHub runners
    # unused (for example, three equal heavy lanes each round to two shards
    # under a 15-runner request).  Add one shard at a time to the current
    # heaviest splittable task until every requested runner has work.  The
    # denominator remains exact because player buckets only partition emitted
    # outer rows; each task still derives its full position/year denominator.
    # Preserve tiny pilots as a single readable unit; fanning two cache rows
    # across every available runner adds overhead without useful parallelism.
    substantial_build = sum(task["rows"] for task in source_tasks) >= 1_000_000
    desired_task_count = (
        min(runners, sum(task["players"] for task in source_tasks))
        if substantial_build
        else sum(task["buckets"] for task in source_tasks)
    )
    while sum(task["buckets"] for task in source_tasks) < desired_task_count:
        splittable = [
            task for task in source_tasks
            if task["buckets"] < task["players"]
        ]
        if not splittable:
            break
        target = max(
            splittable,
            key=lambda task: (
                cost(task["rows"], task["year"], task["position"], task["buckets"]),
                task["rows"],
                task["year"],
                task["position"],
            ),
        )
        target["buckets"] += 1
    tasks = expanded(source_tasks)
    return pack(tasks, min(runners, len(tasks)))


def make_capacity_audit_plan(
    snapshot: Path,
    ops_cache: Path,
    start: int,
    end: int,
    runners: int,
    positions: tuple[str, ...] | None = None,
) -> list[dict]:
    """Balance immutable-cache capacity checks without duplicating a lane.

    A capacity gate is one full denominator scan per position/year.  Unlike a
    compact-output build, splitting a player identity would repeat that same
    scan and prove nothing additional, so audit tasks remain indivisible.
    """

    if positions and any(position not in GROUPS for position in positions):
        raise ValueError(f"unsupported canonical position: {positions}")
    counts = capacity_inventory(snapshot, start, end, positions)
    selected = GROUPS if positions is None else positions
    source_tasks = [
        {
            "year": year,
            "position": position,
            "rows": rows,
            "players": 0,
            "base_key": f"{year}:{position}",
            "buckets": 1,
        }
        for year in range(start, end + 1)
        for position in selected
        if (rows := counts.get((year, position), 0))
    ]
    if not source_tasks:
        return []
    tasks = [
        {
            **task,
            "player_buckets": 1,
            "player_bucket": None,
            "task_id": f"{task['year']}_{task['position']}_capacity",
            "estimate": cost(task["rows"], task["year"], task["position"], 1),
        }
        for task in source_tasks
    ]
    return pack(tasks, min(runners, len(tasks)))


def main() -> None:
    p=argparse.ArgumentParser()
    p.add_argument("--snapshot",type=Path,required=True)
    p.add_argument("--ops-cache",type=Path,required=True)
    p.add_argument("--year-start",type=int,required=True)
    p.add_argument("--year-end",type=int,required=True)
    p.add_argument("--runners",type=int,default=15)
    p.add_argument("--positions",default="",help="Optional comma-separated canonical positions for an isolated pilot")
    p.add_argument("--capacity-only",action="store_true",help="Plan one cache-capacity audit per position/year without player buckets")
    p.add_argument("--output",type=Path,required=True)
    a=p.parse_args()
    positions=tuple(position.strip().upper() for position in a.positions.split(",") if position.strip()) or None
    if positions and any(position not in GROUPS for position in positions):
        raise SystemExit(f"unsupported canonical position: {positions}")
    planner = make_capacity_audit_plan if a.capacity_only else make_plan
    matrix=planner(a.snapshot,a.ops_cache,a.year_start,a.year_end,a.runners,positions)
    if not matrix: raise SystemExit("no rollup tasks were planned")
    a.output.write_text(json.dumps({"include":matrix},separators=(",",":"))+"\n",encoding="utf-8")
    print(json.dumps({"years":[a.year_start,a.year_end],"tasks":sum(len(x["tasks"]) for x in matrix),
      "runners":a.runners,"bucket_estimates":[x["estimate"] for x in matrix],
      "max_estimate":max(x["estimate"] for x in matrix),
      "min_estimate":min(x["estimate"] for x in matrix)},sort_keys=True))


if __name__=="__main__":
    main()
