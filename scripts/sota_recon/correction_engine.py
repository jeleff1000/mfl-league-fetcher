"""THE CORRECTION ENGINE (Joe, 2026-08-02: 'I thought we were totally set up
to audit vs all our witnesses and correct every atom based on our full
witness coverage ledger for each column 1920-2025').

He was right to expect it. Every piece existed -- the witness map, the
licences, the precedence law, the single-root backfill law, the overlay
applier -- and corrections were still being hand-built one lane at a time.
This engine is the missing assembly: it walks EVERY licensed weekly witness
lane and mechanically emits the complete correction set.

Per licensed weekly-witnessable spec (player-grain, week grain):
  1. materialize the witness cells (build_witness_sql -- the same SQL the
     audit trusts);
  2. join to the plane at the spec's grain;
  3. emit overlay rows under THE WRITE RULES:
       BACKFILL  -- plane NULL + witness value, inside the witness's
                    validated density span (single-root law);
       CORRECT   -- plane value contradicts the witness AND >= 2 roots agree
                    on the same value against stored (precedence law's
                    unanimity bar; single-root corrections of NONNULL cells
                    are NOT emitted -- those go to arbitration);
  4. every row carries repair_id, root(s), ruling, old value.

Output: one overlay parquet per column family in engine_overlays/, plus a
manifest. The resumable applier consumes them exactly like hand-built cars.

Scope note: this pass covers WEEK-grain player and team lanes (the weekly
table, Joe's current scope). Season-grain-only witnesses corroborate but
cannot place a value in a specific week, so they do not emit here.

Run:  python -m scripts.sota_recon.correction_engine            # generate
      python -m scripts.sota_recon.correction_engine --dry-run  # count only
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUTDIR = LAKE / "engine_overlays"
MANIFEST = LAKE / "correction_engine_manifest.json"


def _stage(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def weekly_specs():
    """Licensed, week-grain, player-keyed specs -- the lanes that can place a
    value in a specific week. One spec per (source, column) value path."""
    lic = W.licensed()
    out = []
    for sp in W.WITNESS_MAP:
        if (sp.source_key, sp.v26_col) not in lic:
            continue
        # season-type seal (2026-08-03): a POST/Preseason lane joined to the
        # REG plane is the category error that manufactured 229 conflicts in
        # the vouch queue -- it must never reach the WRITER
        if ((sp.season_type or "REG").upper() != "REG"
                or "post" in (sp.source_table or "").lower()
                or "preseason" in (sp.source_table or "").lower()):
            continue
        # rate columns are Stage-7 jurisdiction (recompute from bases),
        # never the engine's -- a witnessed rate cannot outvote its own
        # closed components
        if any(k in sp.v26_col for k in ("_per_", "_pct", "pct_", "rating",
                                         "share")):
            continue
        week_capable_shapes = {"box", "pbp_rollup", "pbp_rollup_week",
                               "team_week", "nflcom_log_week"}
        # capability comes from the SHAPE (the adapters), not the declared
        # grain -- season-declared box/pbp specs are week-capable by nature
        if (sp.validation_grain != "week" and sp.grain != "week"
                and sp.shape not in week_capable_shapes):
            continue
        out.append(sp)
    return out


def week_witness_sql(sp) -> tuple[str, str] | None:
    """Week-grain witness SQL for shapes build_witness_sql doesn't cover.
    Returns (sql yielding key/yr/wk/val, key_kind) or None. Mirrors the
    validator's own native-grain idioms -- the same SQL the audit trusts."""
    from scripts.sota_recon.witness_map import _q
    if sp.shape == "nflcom_log_week":
        src = (Path(sp.source_path).as_posix() if sp.source_path
               else _q(sp.source_key))
        xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
        bio = Path(S.PLAYER_BIO.path).as_posix()
        table_pred = (f"AND r.{sp.table_col} = "
                      f"'{sp.source_table.replace(chr(39), chr(39)*2)}'"
                      if sp.table_col and sp.source_table else "")
        row_pred = f"AND ({sp.row_filter})" if sp.row_filter else ""
        # source_expr replaces the raw column read (2026-08-02 tackle ruling:
        # def_tackles_combined = computed solo+ast, never nflcom `total`)
        val_expr = sp.source_expr or f'TRY_CAST(r."{sp.source_col}" AS DOUBLE)'
        return (f"""
        SELECT bio.NFL_player_id AS pid, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(r.wk AS INT) AS wk,
               ANY_VALUE({val_expr}) AS val
        FROM read_parquet('{src}', union_by_name=true) r
        JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
        JOIN read_parquet('{bio}') bio ON bio.pfr_id = x.pfr_id
        WHERE ({val_expr}) IS NOT NULL
          {table_pred} {row_pred}
        GROUP BY 1, 2, 3 HAVING yr IS NOT NULL AND wk IS NOT NULL""",
                "player")
    if sp.shape == "flat" and sp.source_key == "pbp_player_week_rollup":
        # the rollup IS per player-week (NFL_player_id/year/week columns);
        # the flat season lane just discarded the week key. Its absence made
        # a LOCKED lane's testimony invisible to arbitration voter panels
        # (rz_targets re-won 90 cells the lock had already countersigned).
        src = _q(sp.source_key)
        return (f"""
        SELECT NFL_player_id AS pid, TRY_CAST(year AS INT) AS yr,
               TRY_CAST(week AS INT) AS wk,
               TRY_CAST("{sp.source_col}" AS DOUBLE) AS val
        FROM read_parquet('{src}', union_by_name=true)
        WHERE season_type = 'REG'
          AND TRY_CAST("{sp.source_col}" AS DOUBLE) IS NOT NULL""",
                "player")
    if sp.shape == "box":
        # BOX SCORES ARE WEEKLY (Joe: 'we mapped them to box scores, that's
        # weekly'). The box specs were always per-game -- boxscore_id ->
        # catalog week; the season generator merely discarded the week key.
        # This adapter restores it for EVERY box-mapped spec at once.
        from scripts.sota_recon.witness_map import _q
        src = _q(sp.source_key)
        games = Path(S.TEAM_GAMES.path).as_posix()
        bio = Path(S.PLAYER_BIO.path).as_posix()
        pred = (f"AND ({sp.row_filter})" if sp.row_filter else "")
        return (f"""
        SELECT b.NFL_player_id AS pid, g.year AS yr,
               TRY_CAST(g.week AS INT) AS wk,
               SUM(TRY_CAST(s."{sp.source_col}" AS DOUBLE)) AS val
        FROM read_parquet('{src}', union_by_name=true) s
        JOIN (SELECT DISTINCT boxscore_id, year, week FROM '{games}'
              WHERE season_type = 'REG') g USING (boxscore_id)
        JOIN (SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
              WHERE pfr_id IS NOT NULL) b
          ON b.pfr_id = regexp_extract(
              CAST(s.player_link_ids AS VARCHAR), '^([^;,]+)', 1)
        WHERE TRY_CAST(s."{sp.source_col}" AS DOUBLE) IS NOT NULL {pred}
        GROUP BY 1, 2, 3 HAVING yr IS NOT NULL AND wk IS NOT NULL""",
                "player")
    if sp.shape == "pbp_rollup":
        # PBP IS PER-PLAY (Joe: 'mapping PBP to season and not week does not
        # make sense'). Every play row carries season AND week; the season
        # shape's GROUP BY simply dropped wk. Same spec, same filters, same
        # expr -- the week key restored.
        from scripts.sota_recon.witness_map import _q
        bio = Path(S.PLAYER_BIO.path).as_posix()
        f = f"AND ({sp.filters})" if sp.filters else ""
        return (f"""
        SELECT bio.NFL_player_id AS pid, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(r.week AS INT) AS wk,
               {'MAX' if (sp.witness_agg or sp.agg) == 'max' else 'SUM'}({sp.source_expr}) AS val
        FROM '{_q(sp.source_key)}' r
        JOIN read_parquet('{bio}') bio
          ON bio.NFL_player_id = r.{sp.team_col}
        WHERE r.season_type = 'REG' AND ({sp.source_expr}) IS NOT NULL {f}
        GROUP BY 1, 2, 3 HAVING yr IS NOT NULL AND wk IS NOT NULL""",
                "player")
    if sp.shape == "team_week":
        src = _q(sp.source_key)
        return (f"""
        SELECT r.{sp.team_col or 'nfl_team'} AS team,
               TRY_CAST(r.year AS INT) AS yr, TRY_CAST(r.week AS INT) AS wk,
               ANY_VALUE(TRY_CAST(r."{sp.source_col}" AS DOUBLE)) AS val
        FROM read_parquet('{src}', union_by_name=true) r
        WHERE TRY_CAST(r."{sp.source_col}" AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2, 3 HAVING yr IS NOT NULL AND wk IS NOT NULL""",
                "team")
    return None


def build(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    OUTDIR.mkdir(exist_ok=True)
    specs = weekly_specs()
    _stage(f"{len(specs)} licensed week-grain lanes")

    manifest, totals = [], {"backfill": 0, "correct": 0, "lanes": 0}
    by_col: dict[str, list] = {}
    for sp in specs:
        by_col.setdefault(sp.v26_col, []).append(sp)

    for col, sps in sorted(by_col.items()):
        lane_rows = []
        for sp in sps:
            adapted = week_witness_sql(sp)
            if adapted:
                wsql, kind = adapted
                if kind == "player":
                    join = ("JOIN plane t ON t.NFL_player_id = w.pid "
                            "AND t.year = w.yr AND t.week = w.wk")
                else:
                    join = ("JOIN plane t ON t.nfl_team = w.team "
                            "AND t.year = w.yr AND t.week = w.wk "
                            "AND t.position = 'DEF'")
            else:
                try:
                    wsql = W.build_witness_sql(sp)
                except Exception as e:
                    manifest.append({"column": col, "source": sp.source_key,
                                     "skipped": str(e)[:80]})
                    continue
                is_team = "team" in (sp.shape or "") or sp.team_col in (
                    "posteam", "defteam", "nfl_team")
                if is_team:
                    join = ("JOIN plane t ON t.nfl_team = w.team "
                            "AND t.year = w.yr AND t.week = w.wk "
                            "AND t.position = 'DEF'")
                else:
                    join = ("JOIN read_parquet('%s') b ON b.pfr_id = w.pfr_id "
                            "JOIN plane t ON t.NFL_player_id = b.NFL_player_id "
                            "AND t.year = w.yr AND t.week = w.wk" % bio)
                    if "wk" not in wsql and " AS yr" in wsql:
                        manifest.append({"column": col, "source": sp.source_key,
                                         "skipped": "season-grain witness sql"})
                        continue
            q = f"""
            WITH w AS ({wsql}),
            plane AS (
              SELECT NFL_player_id, nfl_team, position, year, week,
                     TRY_CAST({col} AS DOUBLE) AS stored
              FROM read_parquet('{wk}')
              WHERE season_type = 'REG')
            SELECT t.NFL_player_id, t.year, t.week,
                   t.stored AS old_value, w.val AS new_value,
                   '{sp.source_key}' AS root
            FROM w {join}
            WHERE w.val IS NOT NULL
              AND (t.stored IS NULL OR t.stored <> w.val)"""
            try:
                con.execute(f"CREATE OR REPLACE TEMP TABLE lane AS {q}")
                n = con.execute("SELECT COUNT(*) FROM lane").fetchone()[0]
            except Exception as e:
                manifest.append({"column": col, "source": sp.source_key,
                                 "skipped": str(e)[:80]})
                continue
            if n:
                con.execute(f"""
                CREATE OR REPLACE TEMP TABLE lane_{len(lane_rows)} AS
                SELECT * FROM lane""")
                lane_rows.append((sp.source_key, f"lane_{len(lane_rows) - 0}"))
        if not lane_rows:
            continue
        union = " UNION ALL ".join(
            f"SELECT *, '{src}' AS src FROM {tbl}" for src, tbl in lane_rows)
        # WRITE RULES: backfill = stored NULL, any licensed root (single-root
        # law). correct = stored NONNULL, requires >= 2 roots agreeing on the
        # same value (precedence unanimity bar); single-root contradictions
        # queue for arbitration and are counted, not written.
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE verdicts AS
        WITH all_claims AS ({union}),
        agg AS (
          SELECT NFL_player_id, year, week, old_value, new_value,
                 COUNT(DISTINCT src) AS roots,
                 STRING_AGG(DISTINCT src, ',') AS root_list
          FROM all_claims GROUP BY 1, 2, 3, 4, 5)
        SELECT NFL_player_id, year, week, '{col}' AS column_name,
               old_value, new_value,
               'engine_' || CASE WHEN old_value IS NULL THEN 'backfill'
                                 ELSE 'correct' END AS repair_id,
               root_list AS root,
               CASE WHEN old_value IS NULL
                    THEN 'SINGLE-ROOT BACKFILL LAW (engine)'
                    ELSE 'precedence unanimity: ' || roots || ' roots agree'
               END AS ruling
        FROM agg
        WHERE old_value IS NULL OR roots >= 2
        QUALIFY COUNT(*) OVER (PARTITION BY NFL_player_id, year, week) = 1""")
        # ZERO-BACKFILL FLOOR (2026-08-03): a witness ZERO in a year before
        # that column's earliest NONZERO claim is renderer fill, not
        # testimony -- nflcom zero-fills ancient DEF gamelog cells, and 130
        # all-zero pre-1957 def_sacks/def_fumbles_forced "backfills" nearly
        # re-manufactured the exact fabrication the Class-A purge removed,
        # laundered through a witness. Law 3 applies to WITNESSES too.
        # ROOT CREDIBILITY LAW (Joe 2026-08-03): "it might have sparser data
        # on earlier years so if it has a 0 or null or blank those values
        # are not necessarily true, but positive values can be trusted."
        # A zero backfill needs SAME-YEAR nonzero evidence from the claiming
        # roots -- a year where the union records no nonzero at all is
        # sparse coverage, and its zeros are fill, not observations.
        con.execute(f"""
        DELETE FROM verdicts v WHERE v.old_value IS NULL AND v.new_value = 0
          AND CAST(v.year AS INT) NOT IN (
            SELECT DISTINCT CAST(z.year AS INT)
            FROM ({union}) z WHERE z.new_value > 0)""")
        # MAX-class columns (longs) can NEVER backfill zero: a "longest of
        # 0" is absence rendered as a number, not an observation
        if any((s.witness_agg or s.agg) == "max" for s in sps):
            con.execute("""DELETE FROM verdicts
            WHERE old_value IS NULL AND new_value = 0""")
        # single-row weeks only: doubleheader-era multi-row keys queue for the
        # game_date-keyed overlay schema v2 (the one-repair-per-cell gate
        # caught 14 escapees in batch 2's first launch)
        nb, nc = con.execute("""
        SELECT COUNT(*) FILTER (WHERE old_value IS NULL),
               COUNT(*) FILTER (WHERE old_value IS NOT NULL)
        FROM verdicts""").fetchone()
        arb = con.execute(f"""
        WITH all_claims AS ({union})
        SELECT COUNT(DISTINCT (NFL_player_id, year, week))
        FROM all_claims WHERE old_value IS NOT NULL""").fetchone()[0] - nc
        totals["backfill"] += nb
        totals["correct"] += nc
        totals["lanes"] += len(lane_rows)
        entry = {"column": col, "lanes": len(lane_rows),
                 "backfill": nb, "correct": nc,
                 "single_root_contradictions_to_arbitration": max(arb, 0)}
        manifest.append(entry)
        if not dry and (nb or nc):
            con.execute(f"""
            COPY (SELECT * FROM verdicts)
            TO '{(OUTDIR / f"{col}.parquet").as_posix()}' (FORMAT parquet)""")
        _stage(f"{col}: backfill={nb} correct={nc} (from {len(lane_rows)} lanes)")

    doc = {"wave": "correction_engine", "date": time.strftime("%Y-%m-%d"),
           "dry_run": dry, "totals": totals, "columns": manifest}
    MANIFEST.write_text(json.dumps(doc, indent=2, default=str),
                        encoding="utf-8")
    return doc


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    r = build(con, a.dry_run)
    print(json.dumps({"totals": r["totals"]}, indent=2))
