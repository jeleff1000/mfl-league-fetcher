"""Recover `Recent Games`: give it back its position block and its season.

WHAT WAS WRONG. The caption was refused on two independent capture defects. Its position
block did not survive capture -- the dossier carries SEVEN table_keys (id_recent:QB, :WRTE,
...) collapsing onto one `_table` value, so `yds` is a union of passing, rushing, receiving
and interception-return yards. And it is KEYLESS: `season` is NULL on all 23,454 rows; the
caption carries `wk`, `opp` and `result` and no year. Both are recoverable from what is
already stored, with no re-crawl.

THE BLOCK, from column occupancy. The same technique that recovered player_logs: the rows
carry only a handful of distinct occupancy signatures and each names its block. Measured
here, the seven separate cleanly -- DEF 9,147 rows, WRTE 2,527, RBFB 1,245, OL 803, QB 628,
P 529, K 504.

  NOTE THE COLLISION THAT BROKE THE FIRST player_logs ATTEMPT and is avoided here: a derived
  column must not reuse a source column name. `SELECT *, CASE ... AS blk` on a table that
  already has `blk` silently read the ORIGINAL column in every later reference. Every
  derived column in this module is prefixed `_rg_`.

THE SEASON, from the scoreboard. `result` ('W 20 - 13') gives the win/loss and both scores,
`opp` ('@Patriots') gives the venue, `wk` gives the week. Matched against the game catalog
that pins a unique season on 63.6% of rows by score alone; adding the constraint that the
PLAYER has a v26 weekly row that season takes it to 93.9%. The answer is overwhelmingly one
season -- 2025 on 99.4% of resolved rows -- which is what "Recent Games" means.

  A ROW THAT DOES NOT RESOLVE IS LEFT NULL, never guessed. Assigning the modal season to
  the residual would manufacture 2025 rows for players who were not in the league.

WHY THIS IS WORTH DOING even though these canonicals are already witnessed by the career
captions: this witness is at WEEK grain, and v26 is natively player-week. A season total
cannot catch a within-season distribution error and a weekly witness can.

    python -m scripts.sota_recon.nflcom_recent_games_recovery [--apply]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from . import sources as S

SRC = "D:/league-history-data/nfl/raw/nflcom/tables/player_career/**/*.parquet"
OUT = Path(r"D:\league-history-data\nfl\raw\nflcom\tables\player_career_recent_recovered")
LEDGER = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\nflcom_recent_games_recovery.json")

#: Occupancy signature -> block. Ordered: the first arm whose columns are present wins, and
#: the arms are disjoint on the measured signatures. `_rg_` prefix so no derived name can
#: collide with a source column.
BLOCK_SQL = """CASE
    WHEN comp IS NOT NULL AND comp <> '' THEN 'QB'
    WHEN (fgm IS NOT NULL AND fgm <> '') OR (fg_att IS NOT NULL AND fg_att <> '')
      OR (xpm IS NOT NULL AND xpm <> '')                            THEN 'K'
    WHEN (punts IS NOT NULL AND punts <> '')
      OR (net_yds IS NOT NULL AND net_yds <> '')                    THEN 'P'
    WHEN (pdef IS NOT NULL AND pdef <> '') OR (sfty IS NOT NULL AND sfty <> '')
      OR (tkl IS NOT NULL AND tkl <> '') OR (combined IS NOT NULL AND combined <> '')
                                                                    THEN 'DEF'
    WHEN (rec IS NOT NULL AND rec <> '') AND (att IS NOT NULL AND att <> '') THEN
      -- WRTE and RBFB share a schema ordered oppositely. Split on the INTERNAL identity,
      -- never on our own bio position -- that would be OUR data deciding which of the
      -- SITE's blocks a row came from.
      CASE WHEN TRY_CAST(att AS DOUBLE) > 0
             AND ABS(TRY_CAST(avg AS DOUBLE)
                     - TRY_CAST(yds AS DOUBLE)/NULLIF(TRY_CAST(att AS DOUBLE),0)) <= 0.06
           THEN 'RBFB'
           WHEN TRY_CAST(rec AS DOUBLE) > 0
             AND ABS(TRY_CAST(avg AS DOUBLE)
                     - TRY_CAST(yds AS DOUBLE)/NULLIF(TRY_CAST(rec AS DOUBLE),0)) <= 0.06
           THEN 'WRTE' END
    WHEN rec IS NOT NULL AND rec <> ''                              THEN 'WRTE'
    WHEN att IS NOT NULL AND att <> ''                              THEN 'RBFB'
    WHEN (g IS NOT NULL AND g <> '') OR (gs IS NOT NULL AND gs <> '') THEN 'OL'
    END"""


def build(con: duckdb.DuckDBPyConnection) -> dict:
    tg = Path(S.TEAM_GAMES.path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()

    con.execute(f"""CREATE OR REPLACE TABLE rg AS
      SELECT *, {BLOCK_SQL} AS _rg_block,
             TRY_CAST(wk AS INT) AS _rg_wk,
             CASE WHEN opp LIKE '@%' THEN 0 ELSE 1 END AS _rg_home,
             TRY_CAST(regexp_extract(result, '([0-9]+)\\s*-', 1) AS INT) AS _rg_pf,
             TRY_CAST(regexp_extract(result, '-\\s*([0-9]+)$', 1) AS INT) AS _rg_pa
      FROM (SELECT DISTINCT * FROM read_parquet('{SRC}', union_by_name=True)
            WHERE _table='Recent Games')""")

    con.execute(f"""CREATE OR REPLACE TABLE g AS SELECT year yr, week wk,
       team_points pf, opponent_points pa, is_home FROM read_parquet('{tg}')
       WHERE season_type='REG' AND year >= 2000""")
    con.execute(f"""CREATE OR REPLACE TABLE pw AS
       SELECT x.nflcom_slug slug, t.year yr, t.week wk FROM read_parquet('{v26}') t
       JOIN read_parquet('{bio}') b ON b.NFL_player_id=t.NFL_player_id
       JOIN read_parquet('{xw}') x ON x.pfr_id=b.pfr_id
       WHERE t.season_type='REG' AND t.year>=2000 GROUP BY 1,2,3""")

    # SEASON = the unique catalog year whose (week, score, venue) matches AND in which this
    # player actually appears. Ambiguous or unmatched -> NULL, never the modal season.
    con.execute("""CREATE OR REPLACE TABLE season_map AS
      WITH cand AS (
        SELECT rg.nflcom_slug slug, rg._rg_wk wk, rg._rg_pf pf, rg._rg_pa pa,
               rg._rg_home home, g.yr
        FROM rg JOIN g ON g.wk=rg._rg_wk AND g.pf=rg._rg_pf AND g.pa=rg._rg_pa
                        AND g.is_home=rg._rg_home),
           kept AS (SELECT c.* FROM cand c
                    JOIN pw ON pw.slug=c.slug AND pw.yr=c.yr AND pw.wk=c.wk)
      SELECT slug, wk, pf, pa, home, MIN(yr) AS yr, COUNT(DISTINCT yr) AS n_yr
      FROM kept GROUP BY 1,2,3,4,5""")

    con.execute("""CREATE OR REPLACE TABLE recovered AS
      SELECT rg.*, CASE WHEN m.n_yr = 1 THEN m.yr END AS _rg_season
      FROM rg LEFT JOIN season_map m
        ON m.slug=rg.nflcom_slug AND m.wk=rg._rg_wk AND m.pf=rg._rg_pf
       AND m.pa=rg._rg_pa AND m.home=rg._rg_home""")

    total = con.execute("SELECT COUNT(*) FROM recovered").fetchone()[0]
    blocks = dict(con.execute(
        "SELECT COALESCE(_rg_block,'(unassigned)'), COUNT(*) FROM recovered "
        "GROUP BY 1 ORDER BY 2 DESC").fetchall())
    seasoned = con.execute(
        "SELECT COUNT(*) FROM recovered WHERE _rg_season IS NOT NULL").fetchone()[0]
    both = con.execute("SELECT COUNT(*) FROM recovered WHERE _rg_season IS NOT NULL "
                       "AND _rg_block IS NOT NULL").fetchone()[0]
    years = dict(con.execute("SELECT _rg_season, COUNT(*) FROM recovered "
                             "WHERE _rg_season IS NOT NULL GROUP BY 1 "
                             "ORDER BY 2 DESC").fetchall())
    return {"rows": total, "blocks": blocks,
            "rows_with_season": seasoned, "rows_with_block_and_season": both,
            "season_distribution": {str(k): v for k, v in years.items()},
            "pct_recovered": round(100.0 * both / total, 2)}


def validate(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Measure the recovered table at WEEK grain -- the grain v26 is native to."""
    v26 = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
    con.execute(f"""CREATE OR REPLACE TABLE ident AS
       SELECT b.NFL_player_id pid, x.nflcom_slug slug FROM read_parquet('{xw}') x
       JOIN read_parquet('{bio}') b ON b.pfr_id=x.pfr_id""")
    # (block, source column, canonical). Block-scoped, because the SAME column name is a
    # different statistic per block -- `yds_2` is rushing yards on QB and receiving yards on
    # RBFB, `lng` is a field-goal long on K and a punt long on P. That is the whole reason
    # the block had to be recovered before anything here could be measured.
    PAIRS = [
        ("QB", "comp", "completions"), ("QB", "att", "attempts"),
        ("QB", "yds", "passing_yards"), ("QB", "td", "passing_tds"),
        ("QB", "int", "passing_interceptions"), ("QB", "scky", "sack_yards_lost"),
        ("QB", "sck", "sacks_suffered"),
        ("QB", "att_2", "carries"), ("QB", "yds_2", "rushing_yards"),
        ("QB", "td_2", "rushing_tds"),
        ("QB", "fum", "fumbles"), ("QB", "lost", "fumbles_lost"),

        ("RBFB", "att", "carries"), ("RBFB", "yds", "rushing_yards"),
        ("RBFB", "td", "rushing_tds"), ("RBFB", "rec", "receptions"),
        ("RBFB", "yds_2", "receiving_yards"), ("RBFB", "td_2", "receiving_tds"),
        # ORDER FOLLOWS THE BLOCK, and I had these two swapped on the first run: RBFB
        # renders RUSHING first (yds -> rushing_yards) so `lng` is the rushing long, while
        # WRTE renders RECEIVING first so its `lng` is the receiving long. The swap scored
        # 0.0778 and 0.1391 -- near zero, which is the malformed-expression end of the law,
        # not a weak witness.
        ("RBFB", "lng", "rushing_long"), ("RBFB", "lng_2", "receiving_long"),
        ("RBFB", "fum", "fumbles"), ("RBFB", "lost", "fumbles_lost"),

        ("WRTE", "rec", "receptions"), ("WRTE", "yds", "receiving_yards"),
        ("WRTE", "td", "receiving_tds"), ("WRTE", "lng", "receiving_long"),
        ("WRTE", "att", "carries"), ("WRTE", "yds_2", "rushing_yards"),
        ("WRTE", "td_2", "rushing_tds"), ("WRTE", "lng_2", "rushing_long"),
        ("WRTE", "fum", "fumbles"), ("WRTE", "lost", "fumbles_lost"),

        ("DEF", "tkl", "def_tackles_solo"), ("DEF", "ast", "def_tackle_assists"),
        ("DEF", "combined", "def_tackles_combined"), ("DEF", "sck", "def_sacks"),
        ("DEF", "int", "def_interceptions"), ("DEF", "pdef", "def_pass_defended"),
        ("DEF", "ff", "def_fumbles_forced"), ("DEF", "sfty", "def_safeties"),
        ("DEF", "yds", "def_interception_yards"), ("DEF", "tds", "def_int_ret_td"),
        ("DEF", "opp_fr", "fumble_recovery_opp"),

        ("K", "fgm", "fg_made"), ("K", "fg_att", "fg_att"), ("K", "xpm", "pat_made"),
        ("K", "xp_att", "pat_att"), ("K", "blk", "fg_blocked"),
        ("K", "xblk", "pat_blocked"), ("K", "lng", "fg_long"),

        ("P", "punts", "punts"), ("P", "yds", "punt_yards"),
        ("P", "blk", "punts_blocked"), ("P", "lng", "punt_long"),
    ]
    # RATES, block-scoped and recomputed from OUR operands at WEEK grain. Same argument
    # as the season-grain equation lane: a rate cannot be SUMMED (its value path is
    # correctly refused) and it can be CHECKED, and checking it audits every operand.
    # `avg` is a different statistic per block, which is why this could not run until the
    # block was recovered.
    RATES = [
        ("QB", "avg", "passing_yards", "attempts", 1.0),
        ("QB", "avg_2", "rushing_yards", "carries", 1.0),
        ("RBFB", "avg", "rushing_yards", "carries", 1.0),
        ("RBFB", "avg_2", "receiving_yards", "receptions", 1.0),
        ("WRTE", "avg", "receiving_yards", "receptions", 1.0),
        ("WRTE", "avg_2", "rushing_yards", "carries", 1.0),
        ("DEF", "avg", "def_interception_yards", "def_interceptions", 1.0),
        ("P", "avg", "punt_yards", "punts", 1.0),
        ("K", "pct", "fg_made", "fg_att", 100.0),
        ("K", "xpct", "pat_made", "pat_att", 100.0),
    ]
    out = []
    for block, col, num, den, scale in RATES:
        try:
            r = con.execute(f"""
              WITH w AS (SELECT i.pid, r._rg_season yr, r._rg_wk wk,
                                ANY_VALUE(TRY_CAST(r."{col}" AS DOUBLE)) rate
                         FROM recovered r JOIN ident i ON i.slug=r.nflcom_slug
                         WHERE r._rg_block='{block}' AND r._rg_season IS NOT NULL
                           AND TRY_CAST(r."{col}" AS DOUBLE) IS NOT NULL
                         GROUP BY 1,2,3),
                   t AS (SELECT NFL_player_id pid, year yr, week wk,
                                SUM({num}) num, SUM({den}) den
                         FROM read_parquet('{v26}') WHERE season_type='REG' GROUP BY 1,2,3)
              SELECT COUNT(*), COUNT(*) FILTER (
                       WHERE ABS(w.rate - (t.num/t.den)*{scale}) <= 0.06)
              FROM w JOIN t USING (pid, yr, wk) WHERE t.den > 0""").fetchone()
            n, ok = r
            out.append({"block": block, "column": col, "canonical": f"{num}/{den}",
                        "kind": "RATE", "n": n,
                        "agree_pct": round(ok / n, 4) if n else None,
                        "zero_vs_zero": 0, "informative_n": n,
                        "informative_agree_pct": round(ok / n, 4) if n else None})
        except Exception as exc:
            out.append({"block": block, "column": col, "kind": "RATE",
                        "error": str(exc).splitlines()[0][:90]})
    # NAMED FORMULA at week grain. The rate arm above takes two-operand ratios; passer
    # rating is a five-operand formula fixed by rule, so it needs the same closed-registry
    # treatment the season-grain lane got -- and the four components clamp to [0, 2.375]
    # BEFORE averaging, without which a 5-for-5 game reads above the 158.3 maximum.
    RATE_FORMULA = ("(GREATEST(0, LEAST(2.375, ((c/a) - 0.3) * 5))"
                    " + GREATEST(0, LEAST(2.375, ((y/a) - 3) * 0.25))"
                    " + GREATEST(0, LEAST(2.375, (td/a) * 20))"
                    " + GREATEST(0, LEAST(2.375, 2.375 - ((i/a) * 25)))) / 6 * 100")
    try:
        r = con.execute(f"""
          WITH w AS (SELECT i.pid, r._rg_season yr, r._rg_wk wk,
                            ANY_VALUE(TRY_CAST(r."rate" AS DOUBLE)) rate
                     FROM recovered r JOIN ident i ON i.slug=r.nflcom_slug
                     WHERE r._rg_block='QB' AND r._rg_season IS NOT NULL
                       AND TRY_CAST(r."rate" AS DOUBLE) IS NOT NULL GROUP BY 1,2,3),
               t AS (SELECT NFL_player_id pid, year yr, week wk, SUM(completions) c,
                            SUM(attempts) a, SUM(passing_yards) y, SUM(passing_tds) td,
                            SUM(passing_interceptions) i
                     FROM read_parquet('{v26}') WHERE season_type='REG' GROUP BY 1,2,3)
          SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(w.rate - {RATE_FORMULA}) <= 0.06)
          FROM w JOIN t USING (pid, yr, wk) WHERE t.a > 0""").fetchone()
        n, ok = r
        out.append({"block": "QB", "column": "rate", "kind": "FORMULA",
                    "canonical": "passer_rating(comp,att,yds,td,int)", "n": n,
                    "agree_pct": round(ok / n, 4) if n else None,
                    "zero_vs_zero": 0, "informative_n": n,
                    "informative_agree_pct": round(ok / n, 4) if n else None})
    except Exception as exc:
        out.append({"block": "QB", "column": "rate", "kind": "FORMULA",
                    "error": str(exc).splitlines()[0][:90]})
    for block, col, canon in PAIRS:
        try:
            r = con.execute(f"""
              WITH w AS (SELECT i.pid, r._rg_season yr, r._rg_wk wk,
                                SUM(TRY_CAST(r."{col}" AS DOUBLE)) v
                         FROM recovered r JOIN ident i ON i.slug=r.nflcom_slug
                         WHERE r._rg_block='{block}' AND r._rg_season IS NOT NULL
                           AND TRY_CAST(r."{col}" AS DOUBLE) IS NOT NULL
                         GROUP BY 1,2,3),
                   t AS (SELECT NFL_player_id pid, year yr, week wk, SUM({canon}) v
                         FROM read_parquet('{v26}') WHERE season_type='REG' GROUP BY 1,2,3)
              SELECT COUNT(*), COUNT(*) FILTER (WHERE w.v=t.v),
                     COUNT(*) FILTER (WHERE w.v=0 AND t.v=0)
              FROM w JOIN t USING (pid, yr, wk)""").fetchone()
            n, ok, zz = r
            out.append({"block": block, "column": col, "canonical": canon, "n": n,
                        "agree_pct": round(ok / n, 4) if n else None,
                        "zero_vs_zero": zz,
                        "informative_n": n - zz,
                        "informative_agree_pct": (round((ok - zz) / (n - zz), 4)
                                                  if n - zz > 0 else None)})
        except Exception as exc:
            out.append({"block": block, "column": col, "canonical": canon,
                        "error": str(exc).splitlines()[0][:90]})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="write the recovered table to disk")
    args = ap.parse_args()
    con = duckdb.connect(); con.execute("SET memory_limit='6GB'")
    report = build(con)
    print(f"  rows                        {report['rows']:,}")
    print(f"  with a block                {sum(v for k, v in report['blocks'].items() if k != '(unassigned)'):,}")
    print(f"  with a season               {report['rows_with_season']:,}")
    print(f"  with BOTH (auditable)       {report['rows_with_block_and_season']:,} "
          f"({report['pct_recovered']}%)")
    print(f"  blocks                      {report['blocks']}")
    print(f"  seasons                     {dict(list(report['season_distribution'].items())[:5])}")
    rows = validate(con)
    report["validation"] = rows
    print("\n  WEEK-GRAIN agreement against v26:")
    for r in sorted(rows, key=lambda x: -(x.get("agree_pct") or 0)):
        if r.get("error"):
            print(f"     {r['block']:5s} {r['column']:9s} ERROR {r['error']}"); continue
        print(f"     {r['block']:5s} {r['column']:9s} -> {r['canonical']:24s} "
              f"{r['agree_pct']} (inf {r['informative_agree_pct']}) n={r['n']:,}")
    if args.apply:
        OUT.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY recovered TO '{OUT.as_posix()}/recent_games_recovered.parquet' "
                    f"(FORMAT PARQUET)")
        print(f"\n  wrote -> {OUT}")
    Path(LEDGER).write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    print(f"  ledger -> {LEDGER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
