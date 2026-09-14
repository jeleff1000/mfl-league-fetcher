"""NO-SPEC INSPECTOR: hunt witnesses for columns that have none.

For each no-spec plane column (from the triage), token-match candidate
columns across every registered source's schema, then MEASURE the most
promising candidates against the plane on modern years (season-sum compare
through the bio crosswalk). A candidate >= 0.95 with n >= 100 is reported
as a SPEC FIND (hand MapSpec to add + vouch); weaker matches are logged.
Names propose, measurement decides -- nothing is specced from a name.

Resumable via incremental JSON. Run: python scripts/sota_recon/nospec_inspector.py [limit]
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "nospec_inspection.json"
TOK = re.compile(r"[a-z]+")
STOP = {"def", "the", "of", "per", "a", "in", "id"}
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 50


def tokens(s: str) -> set:
    return set(TOK.findall(s.lower())) - STOP


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    tri = json.loads((LAKE / "unwitnessed_triage.json").read_text("utf-8"))
    nospec = sorted(c for c, cls in tri["columns"].items()
                    if cls.startswith("no-spec"))
    done = {}
    if OUT.exists():
        done = json.loads(OUT.read_text("utf-8")).get("results", {})
    todo = [c for c in nospec if c not in done][:LIMIT]
    print(f"{len(todo)} no-spec columns this tranche "
          f"({len(done)} already inspected)", flush=True)

    # schema cache over registered player-grain sources
    reg = S.registry()
    schemas = {}
    SELF_SOURCES = {"v26_release", "legacy_motherduck_supertable"}
    for key, ent in reg.items():
        # a source that IS us (release copies, the old supertable) cannot
        # witness us -- the first tranche's 11 "finds" were all self-matches
        if key in SELF_SOURCES or "v26" in key or "legacy" in key:
            continue
        p = Path(ent.path)
        pp = (p.as_posix() + "/**/*.parquet") if p.is_dir() else p.as_posix()
        try:
            cols = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{pp}', "
                f"union_by_name=true) LIMIT 0").fetchall()]
            schemas[key] = (pp, cols)
        except Exception:
            continue
    print(f"{len(schemas)} source schemas cached", flush=True)

    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    for i, col in enumerate(todo):
        want = tokens(col)
        cands = []
        for key, (pp, cols) in schemas.items():
            for sc in cols:
                got = tokens(sc)
                if not got or not want:
                    continue
                j = len(want & got) / len(want | got)
                if j >= 0.5:
                    cands.append((j, key, sc, pp))
        cands.sort(reverse=True)
        row = {"matches": []}
        for j, key, sc, pp in cands[:5]:
            idcol = next((c for c in schemas[key][1]
                          if c in ("pfr_id", "player_link_ids", "NFL_player_id")), None)
            yrcol = next((c for c in schemas[key][1]
                          if c in ("season", "year")), None)
            if not idcol or not yrcol:
                continue
            idexpr = ("regexp_extract(CAST(s.player_link_ids AS VARCHAR), "
                      "'^([^;,]+)', 1)" if idcol == "player_link_ids"
                      else f"s.{idcol}")
            joinkey = ("NFL_player_id" if idcol == "NFL_player_id"
                       else "pfr_id")
            try:
                n, ok = con.execute(f"""
                WITH w AS (
                  SELECT {idexpr} AS k, TRY_CAST(s.{yrcol} AS INT) AS yr,
                         SUM(TRY_CAST(s."{sc}" AS DOUBLE)) AS val
                  FROM read_parquet('{pp}', union_by_name=true) s
                  WHERE TRY_CAST(s."{sc}" AS DOUBLE) IS NOT NULL
                  GROUP BY 1, 2),
                v AS (
                  SELECT {'b.pfr_id' if joinkey == 'pfr_id' else 't.NFL_player_id'} AS k,
                         CAST(t.year AS INT) AS yr,
                         SUM(TRY_CAST(t.{col} AS DOUBLE)) AS sv
                  FROM plane t JOIN ids b USING (NFL_player_id)
                  WHERE t.{col} IS NOT NULL
                    AND CAST(t.year AS INT) BETWEEN 2015 AND 2024
                  GROUP BY 1, 2)
                SELECT COUNT(*), COUNT(*) FILTER (
                  WHERE ABS(w.val - v.sv) <= 0.05)
                FROM w JOIN v USING (k, yr)
                WHERE w.yr BETWEEN 2015 AND 2024""").fetchone()
            except Exception:
                continue
            if n and n >= 25:
                row["matches"].append(
                    {"source": key, "source_col": sc, "n": n,
                     "agree": round(ok / n, 4), "jaccard": round(j, 2)})
        row["matches"].sort(key=lambda m: -m["agree"])
        row["find"] = bool(row["matches"]
                           and row["matches"][0]["agree"] >= 0.95
                           and row["matches"][0]["n"] >= 100)
        done[col] = row
        if i % 10 == 0 or row["find"]:
            print(f"[{time.strftime('%H:%M:%S')}] {i+1}/{len(todo)} {col}"
                  f"{' FIND: ' + str(row['matches'][0]) if row['find'] else ''}",
                  flush=True)
            OUT.write_text(json.dumps(
                {"results": done}, indent=1), encoding="utf-8")
    OUT.write_text(json.dumps({"results": done}, indent=1), encoding="utf-8")
    finds = {c: r["matches"][0] for c, r in done.items() if r.get("find")}
    print(f"DONE: {len(finds)} spec finds of {len(done)} inspected",
          flush=True)
    print(json.dumps(finds, indent=1)[:2000])


if __name__ == "__main__":
    main()
