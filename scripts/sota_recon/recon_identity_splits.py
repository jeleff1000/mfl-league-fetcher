"""
sota_recon/recon_identity_splits.py  --  LANE: systematic identity-split detection.

An identity SPLIT = one real human carrying >1 NFL_player_id, which fragments a career across
ids (and double-counts in any name-based rollup). The Reggie White scare turned out to be the
INVERSE (a name collision: two humans, different DOBs) -- this lane separates the two using
hard identity anchors from player_bio, so we find the true splits and never confuse them with
legitimate same-name different-people cases.

Signals (high -> low confidence):
  PFR_ID     two NFL_player_ids share the same non-null pfr_id  -> PFR itself says one player.
  DOB_NAME   two ids share birth_date + normalized name         -> same human (DOB+name ~unique).
  DOB_COLLEGE two ids share birth_date + college (name spelling differs e.g. Bob/Robert) -> review.
Each candidate id is annotated with its super-table year span + games + fantasy points so we can
see whether BOTH ids actually carry stats (a split that matters) vs a bio-only stub.

A name collision (same name, DIFFERENT birth_date) is explicitly NOT a split and is reported
separately as a sanity counter (so name-based anchors/rollups can avoid them).

    python -m scripts.sota_recon.recon_identity_splits            # summary
    python -m scripts.sota_recon.recon_identity_splits --list     # full candidate list
"""
from __future__ import annotations
import argparse
from pathlib import Path
import duckdb
from .sources import latest_v26, PLAYER_BIO

_NORM = "lower(regexp_replace(regexp_replace(player, '\\s+(jr|sr|ii|iii|iv|v)\\.?$', '', 'i'), '[^a-zA-Z]', '', 'g'))"


def _con():
    c = duckdb.connect(); c.execute("PRAGMA threads=2"); c.execute("SET memory_limit='4GB'")
    c.execute("PRAGMA disable_progress_bar")
    return c


def _diff_team_conflict(con, ids) -> bool:
    """True if any two ids in the group appear in the SAME (year,week) on DIFFERENT teams ->
    physically impossible for one human -> these are DIFFERENT PEOPLE, not a split."""
    if len(ids) < 2:
        return False
    inlist = ",".join(repr(i) for i in ids)
    n = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT year, week, COUNT(DISTINCT nfl_team) t, COUNT(DISTINCT NFL_player_id) d
          FROM v26 WHERE NFL_player_id IN ({inlist}) AND nfl_team IS NOT NULL AND year IS NOT NULL AND week IS NOT NULL
          GROUP BY year, week HAVING COUNT(DISTINCT NFL_player_id)>1 AND COUNT(DISTINCT nfl_team)>1)""").fetchone()[0]
    return n > 0


def run(want_list=False):
    bio = Path(PLAYER_BIO.path).as_posix(); v26 = Path(latest_v26()).as_posix()
    con = _con()
    con.execute(f"CREATE TEMP VIEW v26 AS SELECT * FROM '{v26}'")
    # per-id super-table footprint (does this id carry real stats?)
    con.execute(f"""CREATE TEMP TABLE foot AS
        SELECT NFL_player_id, MIN(year) y0, MAX(year) y1, COUNT(*) nrows,
               COALESCE(SUM(CAST(fpts_4pt_half AS DOUBLE)),0) fpts, any_value(nfl_position) pos
        FROM v26 WHERE NFL_player_id IS NOT NULL GROUP BY NFL_player_id""")
    con.execute(f"""CREATE TEMP TABLE b AS
        SELECT NFL_player_id, player, nfl_position, CAST(birth_date AS VARCHAR) dob, college, pfr_id,
               CAST(substr(CAST(birth_date AS VARCHAR),1,4) AS INT) byr, {_NORM} AS nm FROM '{bio}'""")

    def groups(key_cols, where):
        kc = ", ".join(key_cols)
        return con.execute(f"""
            WITH g AS (SELECT {kc}, COUNT(DISTINCT NFL_player_id) nid,
                              STRING_AGG(DISTINCT NFL_player_id, ',') ids
                       FROM b WHERE {where} GROUP BY {kc} HAVING COUNT(DISTINCT NFL_player_id)>1)
            SELECT * FROM g""").fetchdf()

    pfr = groups(["pfr_id"], "pfr_id IS NOT NULL AND pfr_id<>''")
    dob_name = groups(["dob", "nm", "byr"], "dob IS NOT NULL AND nm<>''")

    def classify(df, kind):
        out = []
        for _, r in df.iterrows():
            ids = [x for x in str(r["ids"]).split(",") if x]
            foot = con.execute(f"""SELECT NFL_player_id, y0, y1, nrows, fpts, pos FROM foot
                WHERE NFL_player_id IN ({','.join(repr(i) for i in ids)})""").fetchdf()
            stat_ids = foot[foot["nrows"] > 0]
            byr = int(r["byr"]) if kind == "DOB_NAME" and r["byr"] == r["byr"] else None
            conflict = _diff_team_conflict(con, ids)               # same wk, diff team -> diff humans
            # impossible age: a stat-bearing id plays before 18 or after 45 vs the shared DOB
            bad_age = byr is not None and any(
                (s.y0 < byr + 18) or (s.y1 > byr + 45) for s in stat_ids.itertuples())
            npos = stat_ids["pos"].nunique()
            if kind == "PFR_ID" and not conflict:
                verdict = "DEFINITE_SPLIT"                          # PFR says one player
            elif conflict:
                verdict = "DIFFERENT_PEOPLE"                        # impossible: two teams same week
            elif len(stat_ids) < 2:
                verdict = "BIO_STUB"                                # only a duplicate bio entry, no stats
            elif bad_age:
                verdict = "DOB_ERROR"                               # mis-assigned DOB / father-son
            elif npos > 1:
                verdict = "DIFF_POS_REVIEW"                         # same DOB+name, different positions
            else:
                verdict = "PROBABLE_SPLIT"                          # one human, stats fragmented
            out.append({"kind": kind, "verdict": verdict,
                        "key": {k: r[k] for k in df.columns if k not in ("nid", "ids", "byr")},
                        "name": (foot["NFL_player_id"].iloc[0] if len(foot) else ids[0]), "ids": ids,
                        "ids_with_stats": int(len(stat_ids)),
                        "footprint": foot.to_dict("records")})
        return out

    cand = classify(pfr, "PFR_ID") + classify(dob_name, "DOB_NAME")
    seen = set(); uniq = []
    for c in cand:
        k = frozenset(c["ids"])
        if k in seen: continue
        seen.add(k); uniq.append(c)
    by = {}
    for c in uniq:
        by.setdefault(c["verdict"], []).append(c)

    collisions = con.execute("""SELECT COUNT(*) FROM (
        SELECT nm FROM b WHERE dob IS NOT NULL AND nm<>'' GROUP BY nm
        HAVING COUNT(DISTINCT dob)>1)""").fetchone()[0]

    # MATERIAL = stat-affecting splits that should be merged (definite or probable)
    material = by.get("DEFINITE_SPLIT", []) + by.get("PROBABLE_SPLIT", [])
    res = {"candidate_groups": len(uniq),
           "material_split_groups": len(material),
           "by_verdict": {k: len(v) for k, v in by.items()},
           "name_collisions_distinct_dob": collisions,
           "material": material, "groups": by}
    if want_list:
        res["all"] = uniq
    con.close()
    return res


def _name_of(con, pid, bio):
    r = con.execute(f"SELECT player FROM '{bio}' WHERE NFL_player_id=? LIMIT 1", [pid]).fetchone()
    return r[0] if r else pid


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--verdict", help="show only this verdict bucket (e.g. BIO_STUB, DOB_ERROR)")
    a = ap.parse_args()
    r = run(want_list=True)
    print(f"candidate groups: {r['candidate_groups']}  | MATERIAL (merge): {r['material_split_groups']}")
    print(f"by verdict: {r['by_verdict']}")
    print(f"name collisions (same name, diff DOB - NOT splits): {r['name_collisions_distinct_dob']}\n")
    import duckdb as _d
    from .sources import PLAYER_BIO as _PB
    _con = _d.connect(); _bio = Path(_PB.path).as_posix()
    buckets = [a.verdict] if a.verdict else ["DEFINITE_SPLIT", "PROBABLE_SPLIT", "DIFF_POS_REVIEW",
                                            "DOB_ERROR", "DIFFERENT_PEOPLE", "BIO_STUB"]
    for vk in buckets:
        grp = r["groups"].get(vk, [])
        if not grp:
            continue
        print(f"== {vk} ({len(grp)}) ==")
        for c in grp:
            keystr = ", ".join(f"{k}={v}" for k, v in c["key"].items())
            print(f"  [{c['kind']}] {keystr}")
            for f in c["footprint"]:
                y0 = int(f['y0']) if f['y0'] == f['y0'] and f['y0'] is not None else '?'
                y1 = int(f['y1']) if f['y1'] == f['y1'] and f['y1'] is not None else '?'
                nr = int(f['nrows']) if f['nrows'] == f['nrows'] else 0
                print(f"      {f['NFL_player_id']:12} {_name_of(_con, f['NFL_player_id'], _bio):22} "
                      f"{y0}-{y1} rows={nr} fpts={f['fpts']:.0f} pos={f['pos']}")
        print()
