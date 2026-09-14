"""Column-by-column audit of every NFL.com table against the supertable.

WHY A HARNESS AND NOT A SCRIPT PER SOURCE. NFL.com is 212 of our 309 NFL tables but only
~50 distinct column names per source, re-emitted per category / split / season-type. Every
question worth asking is the same question 212 times:

    1. does this column have a MAP, and to what?
    2. if not, could it have one -- and what would it alias to?
    3. if it has one, does the supertable actually AGREE, on a base worth counting?
    4. if it disagrees, is that OUR defect or the source's?

This module answers 1-3 mechanically and REFUSES to answer 4. Attribution goes to
`disagreement_adjudications.v1.json` with its crossed control, because who-is-wrong is a
decision and this file only measures.

THE FOUR TRAPS THIS HARNESS HAS ALREADY BEEN BITTEN BY. Each is now enforced in code:

  DUPLICATE ROWS. nflcom_player_career carries 3,304 exact duplicates (3.6%). SUMming over
  them depressed a CORRECT column's agreement from 73.28% to 47.59% -- a 26-point artefact
  that reads exactly like "wrong column". Every read is SELECT DISTINCT first.

  ZERO SHARE. `sfty` reads 98.5% agreement on rows that are 99.0% zero-vs-zero: it agrees
  about nothing. `solo` reads 51.9% on a base 3.0% zero and is a real disagreement. Ranking
  the first above the second inverts the truth, so every rate is reported with its
  informative (non-zero) base beside it.

  RATE AGGREGATION. Summing passing `rate` into passer_rating produced a v26 median of 589
  against a statistic bounded near 158 -- seventeen weekly ratings added up. Aggregation
  comes from stat_contracts.aggregation_class and anything outside SUM/MAX is REFUSED, the
  same refusal mapspec_generator makes.

  GRAIN. Split sources have no Total row, so a season value must be summed within exactly
  ONE split dimension, and only dimensions that provably partition the season may be used.
  Measured, not assumed: `dimension_partition_report` ranks them by mutual agreement.
  (Games are NOT additive across splits even where stats are -- a game appears in every
  bucket it touches while a yard belongs to one -- so `g` cannot serve as this control.)

  THE EMPTY STRING IS NOT A NULL (2026-07-31). NFL.com writes `''`, not NULL, where a
  column is not published for that row -- and `COUNT(col)` counts `''`. So `occupancy`
  read 1.0000 for all 21 Defense Career columns while the TRUE publication rate ran from
  15.9% (`opp_fr`) to 100% (`g`), and the block structure -- a 58.3% tackle block and a
  17.8% interception block sharing one physical table -- was invisible. Worse, the read
  filter `col IS NOT NULL` PASSED those rows, `SUM(TRY_CAST('' AS DOUBLE))` returned NULL,
  and `ABS(NULL - vv) <= 1` is NULL, so every unpublished row scored as a DISAGREEMENT.
  `opp_fr` read 46.9% because NFL.com publishes it in 6 of 15,772 rows in 2000-09 and the
  other 15,766 were counted against us. Publication is now measured with TRY_CAST and is
  reported as `published_share`; a row the source did not publish is not evidence.

  TOLERANCE IS MAGNITUDE-BLIND. `ABS(sv-vv) <= 1` on a count whose median is 1 accepts
  every value in {0,1,2}: `sfty` scored 71.4% under it and 35.5% under equality, and 143
  of 143 "agreeing" 1982-93 rows were src=1 against our 0. A count is now scored on
  EQUALITY, with the +/-1 band retained beside it as `agree_pct_pm1` so the two can be
  compared rather than one silently standing in for the other.

  A BLENDED RATE HIDES A CLEAN ERA. Agreement on these families is era-structured, because
  each statistic has a season at which the league began recording it. `pdef` reads 73.2%
  blended and 99.1% on 2010+ / 0.0% before 1982 -- one number is a defect, the other two
  are a clean mapping plus a coverage floor. Every measured column is stratified.

Run::

    python -m scripts.sota_recon.nflcom_column_audit --source nflcom_player_season
    python -m scripts.sota_recon.nflcom_column_audit --all
"""
from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

import duckdb

from . import sources as S
from .column_dossier import load_decisions, row_key
from .mapspec_generator import rate_shaped_canonicals

ROOT = Path(__file__).resolve().parents[2]
DOSSIER_PATH = ROOT / "docs" / "column-dossier.json"
CONTRACTS_PATH = (Path(__file__).resolve().parent / "witness_gate" / "contracts"
                  / "stat_contracts.v1.json")
#: generated measurement receipt. It lives beside MAPPING_LICENSES.json, OFF-REPO: it is a
#: measurement, not a decision, and the program's law forbids new audit artifacts in docs/.
#: Decisions derived from it go to disagreement_adjudications.v1.json, in-repo.
LEDGER = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\nflcom_column_audit.json")

AGG_FROM_CLASS = {"SUM": "sum", "MAX": "max"}


#: Below this, an agreement rate is noise. logs_targeted post-season rows produced
#: "0.0% agreement" verdicts on bases of 2, 5 and 7 player-seasons.
MIN_INFORMATIVE_N = 30

#: The era strata. These are not arbitrary buckets: each boundary is a season at which the
#: league changed what it RECORDS, so a column's agreement genuinely is a different quantity
#: on either side. 1982 = sacks become official; 1994 = the participation//tackle floor
#: firms up; 2000 = passes-defended and safeties enter the official player record; 2010 =
#: the fully-modern gamebook. A column that reads 99% on 2010+ and 0% on <1982 has a clean
#: mapping and a coverage floor, which is a different finding from "73% blended".
ERA_SQL = ("CASE WHEN yr < 1982 THEN '<1982' WHEN yr < 1994 THEN '1982-93' "
           "WHEN yr < 2000 THEN '1994-99' WHEN yr < 2010 THEN '2000-09' ELSE '2010+' END")


def published(col: str, alias: str = "src") -> str:
    """The rows on which the source ACTUALLY PUBLISHED this column.

    NFL.com writes the empty string, not NULL, where a column does not apply to a row --
    the Defense Career table renders a tackle block and an interception block into one
    physical schema and blanks the half that does not apply. `col IS NOT NULL` passes
    those rows and they then score as disagreements, because SUM(TRY_CAST('')) is NULL and
    every comparison against NULL is NULL. Publication is a CAST question, not a NULL one.
    """
    return f'TRY_CAST({alias}."{col}" AS DOUBLE) IS NOT NULL'


BLOCK_RECOVERY = """CASE
    WHEN TRY_CAST(comp AS DOUBLE) IS NOT NULL
      OR TRY_CAST(scky AS DOUBLE) IS NOT NULL                      THEN 'QB'
    WHEN TRY_CAST(fgm AS DOUBLE) IS NOT NULL
      OR TRY_CAST(fg_att AS DOUBLE) IS NOT NULL
      OR TRY_CAST(xpm AS DOUBLE) IS NOT NULL                       THEN 'K_log'
    WHEN TRY_CAST(punts AS DOUBLE) IS NOT NULL
      OR TRY_CAST(net_yds AS DOUBLE) IS NOT NULL                   THEN 'P'
    WHEN TRY_CAST(total AS DOUBLE) IS NOT NULL
      OR TRY_CAST(pdef AS DOUBLE) IS NOT NULL
      OR TRY_CAST(sfty AS DOUBLE) IS NOT NULL                      THEN 'DEF_log'
    WHEN TRY_CAST(rec AS DOUBLE) IS NOT NULL
     AND TRY_CAST(att AS DOUBLE) IS NOT NULL THEN
      CASE WHEN TRY_CAST(att AS DOUBLE) > 0
             AND ABS(TRY_CAST(avg AS DOUBLE)
                     - TRY_CAST(yds AS DOUBLE)/NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06
             AND NOT (TRY_CAST(rec AS DOUBLE) > 0
                      AND ABS(TRY_CAST(avg AS DOUBLE)
                              - TRY_CAST(yds AS DOUBLE)/NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06)
           THEN 'RBFB5'
           WHEN TRY_CAST(rec AS DOUBLE) > 0
             AND ABS(TRY_CAST(avg AS DOUBLE)
                     - TRY_CAST(yds AS DOUBLE)/NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06
             AND NOT (TRY_CAST(att AS DOUBLE) > 0
                      AND ABS(TRY_CAST(avg AS DOUBLE)
                              - TRY_CAST(yds AS DOUBLE)/NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06)
           THEN 'WRTE' END
    WHEN TRY_CAST(g AS DOUBLE) IS NOT NULL
     AND TRY_CAST(gs AS DOUBLE) IS NOT NULL                         THEN 'OL' END"""


class SourcePlan:
    """How to read one NFL.com source: its table axis, its key, and its grain.

    `table_key_part` says WHICH segment of the dossier's table_key names the physical
    table. It is not always the first: `player_season` keys read `field-goals|reg` (part 0
    is the table) but `player_situational` keys read `Field Position|player_situational_L0`,
    where part 0 is the SPLIT DIMENSION and part 1 is the layout that actually determines
    column identity. Matching on part 0 everywhere silently matched nothing for the two
    layout-major sources, and "nothing matched" presented as "nothing to audit".

    `axis_lost` marks a source whose table discriminator did not survive capture. Its
    columns are physically a UNION of several tables' statistics -- `player_logs`.`yds` is
    passing yards for a QB block, rushing yards for an RB block and interception-return
    yards for a DB block, all merged under one name because the position-group id is in
    the dossier but not in the parquet. Measuring such a column against ANY single
    canonical is meaningless, so the harness refuses instead of reporting a number.
    """

    def __init__(self, source, path, slug_col, table_cols, *, row_filter="",
                 declared_tables=0, note="", table_key_part=0, axis_lost=False,
                 axis_lost_tables=(), source_read_filter=""):
        self.source, self.path, self.slug_col = source, path, slug_col
        self.table_cols, self.row_filter = table_cols, row_filter
        self.declared_tables, self.note = declared_tables, note
        self.table_key_part, self.axis_lost = table_key_part, axis_lost
        #: AXIS LOSS IS PER-TABLE, NOT ONLY PER-SOURCE (2026-07-31). player_career is not
        #: axis-lost as a whole -- 7 of its 8 captions carry exactly one layout -- but its
        #: `Recent Games` caption spans ALL SEVEN position blocks, which the dossier records
        #: as seven table_keys (id_recent:QB, :WRTE, ...) collapsing onto one `_table`
        #: value. Declaring the flag only at source level measured 31 union columns against
        #: one canonical each.
        self.axis_lost_tables = frozenset(axis_lost_tables)
        # Optional predicate pushed into the parquet scan.  This is useful for a
        # dimension-specific audit of a layout-major source: materializing the
        # other dimensions first can exhaust DuckDB memory without changing the
        # audit's declared row filter.
        self.source_read_filter = source_read_filter

    def glob(self):
        p = Path(self.path).as_posix()
        return p if p.endswith(".parquet") else p + "/**/*.parquet"


# The physical table axis per source, with the DECLARED count beside it. Where they differ
# the capture lost a discriminator the dossier still enumerates -- recorded, not smoothed.
PLANS = {
    "nflcom_player_career": SourcePlan(
        "nflcom_player_career",
        r"D:/league-history-data/nfl/raw/nflcom/tables/player_career",
        "nflcom_slug", ["_table"], declared_tables=15,
        axis_lost_tables=("Recent Games",),
        note="declared 15, physical 9: the seven `Recent Games` position-group blocks "
             "(id_recent:QB, :WRTE, ...) are merged into one _table value -- the block id "
             "is in the dossier but NOT in the parquet. That caption is ALSO KEYLESS: "
             "`season` is NULL on all 23,454 of its rows (it carries `wk` and no year), so "
             "a season-grain join returns 0 rows. Both are capture defects, so its 31 "
             "column-instances are REFUSED rather than measured against nothing"),
    "nflcom_player_season": SourcePlan(
        "nflcom_player_season",
        r"D:/league-history-data/nfl/raw/nflcom/tables/player_season",
        "_player_slug", ["_category"], row_filter="season_type='reg'", declared_tables=22,
        note="declared 22, physical 11: reg and post rows are 100.00% identical on every "
             "joined (slug, season) pair in all 11 categories, so season_type carries no "
             "information and the post half is excluded rather than validated as postseason"),
    "nflcom_player_situational": SourcePlan(
        "nflcom_player_situational",
        r"D:/league-history-data/nfl/raw/nflcom/tables/player_situational_unshifted",
        "nflcom_slug", ["_layout"], row_filter="_table='Home vs Road'", declared_tables=58,
        table_key_part=1,
        note="declared 58 = 8 split dimensions x 8 layouts, but ZERO (layout, column) pairs "
             "disagree across dimensions, so identity is per LAYOUT: 129 decisions, not 937. "
             "Home vs Road is partition-valid (98.2-99.5% mutual agreement with Attempts / "
             "Stadium Surfaces / Field Position); Quarters, Game Halves, Margin of Victory "
             "and Point Differential are not"),
    "nflcom_player_splits": SourcePlan(
        "nflcom_player_splits",
        r"D:/league-history-data/nfl/raw/nflcom/tables/player_splits_unshifted",
        "nflcom_slug", ["_layout"], declared_tables=48, table_key_part=1,
        row_filter="_table='Months'",
        note="same layout-major structure as situational. MEASURED partition validity: "
             "Days, Months, Opponents by Team, Outcomes and Stadiums mutually agree at "
             "97.8-99.4%, so each independently reconstructs the season total; `Opponents "
             "by Group` fails at ~36% because its buckets overlap, exactly as Point "
             "Differential does in situational. Months carries the highest mutual "
             "agreement (99.1% mean) and is the declared path. WITHOUT this filter the "
             "harness summed all six dimensions together and every column read 5-6x high "
             "(punt_yards median 12,285 against our 2,667) -- which presents as a total "
             "supertable failure rather than as a grain error."),
    "nflcom_player_logs": SourcePlan(
        "nflcom_player_logs",
        r"D:/league-history-data/nfl/raw/nflcom/tables/player_logs",
        "nflcom_slug", ["_table"], row_filter="_table='Regular Season'", declared_tables=21,
        axis_lost=True,
        note="declared 21, physical 3: _view is the constant 'logs' and the position-group "
             "block (id_gamelog:QB, :WRTE, ...) is NOT in the parquet, so 7 of every 21 "
             "declared tables are unaddressable as captured -- the same drop as player_career"),
    "nflcom_player_logs_targeted": SourcePlan(
        "nflcom_player_logs_targeted",
        S.registry()["nflcom_player_logs_targeted"].path,
        "nflcom_slug", ["_table"], declared_tables=14, axis_lost=True,
        note="declared 14, physical 2: same dropped position-group block as player_logs"),
}


def agg_classes() -> dict[str, str]:
    return {r["canonical_name"]: r["aggregation_class"]
            for r in json.loads(CONTRACTS_PATH.read_text(encoding="utf-8"))["stats"]}


def dossier_for(source: str) -> dict[tuple[str, str], tuple[str, str | None]]:
    """(table_key, column) -> (disposition, canonical) for one source."""
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    decisions = load_decisions()
    out = {}
    for row in dossier["rows"]:
        if row["source"] != source:
            continue
        canonical = (decisions.get(row_key(source, row["table_key"], row["column"])) or {}
                     ).get("canonical")
        out[(row["table_key"], row["column"])] = (row["disposition"], canonical)
    return out


def dimension_partition_report(con, plan, layout_col, dim_col, probe_col) -> list[dict]:
    """Which split dimensions provably partition the season?

    A split source has no Total row, so a season value is the SUM within one dimension. Two
    dimensions that both partition the season must reconstruct the same total, so mutual
    agreement is the receipt. Overlapping vocabularies fail loudly: Point Differential
    carries `Behind` AND `Behind by 1-8` AND `Behind by 9-16`, so summing double-counts.
    """
    con.execute(f"""CREATE OR REPLACE TEMP TABLE dimsum AS
        SELECT {plan.slug_col} slug, TRY_CAST(season AS INT) yr, {dim_col} dim,
               SUM(TRY_CAST("{probe_col}" AS DOUBLE)) v
        FROM src WHERE {layout_col} IS NOT NULL AND "{probe_col}" IS NOT NULL
        GROUP BY 1, 2, 3""")
    rows = con.execute("""
        SELECT a.dim, b.dim, COUNT(*), COUNT(*) FILTER (WHERE ABS(a.v-b.v) <= 1)
        FROM dimsum a JOIN dimsum b USING (slug, yr) WHERE a.dim < b.dim
        GROUP BY 1, 2 HAVING COUNT(*) > 100""").fetchall()
    score = collections.defaultdict(list)
    for a, b, n, agree in rows:
        pct = 100.0 * agree / n
        score[a].append(pct)
        score[b].append(pct)
    return sorted(({"dimension": d, "mean_mutual_agreement": round(sum(v)/len(v), 1),
                    "pairs": len(v)} for d, v in score.items()),
                  key=lambda r: -r["mean_mutual_agreement"])


def audit_source(plan: SourcePlan) -> dict:
    klass = agg_classes()
    units = {r["canonical_name"]: r.get("unit", "")
             for r in json.loads(CONTRACTS_PATH.read_text(encoding="utf-8"))["stats"]}
    rate_shaped = rate_shaped_canonicals(klass, units)
    dos = dossier_for(plan.source)
    xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    # SELECT DISTINCT: duplicate source rows silently double every SUM (3.6% of them in
    # player_career, worth 26 points of measured agreement on a correct column).
    source_where = f" WHERE {plan.source_read_filter}" if plan.source_read_filter else ""
    con.execute(f"CREATE TABLE src AS SELECT DISTINCT * FROM "
                f"read_parquet('{plan.glob()}', union_by_name=True){source_where}")
    con.execute(f"""CREATE TABLE ident AS SELECT b.NFL_player_id pid, x.nflcom_slug slug
                    FROM read_parquet('{xw}') x
                    JOIN read_parquet('{bio}') b ON b.pfr_id = x.pfr_id""")

    physical = [c[0] for c in con.execute("DESCRIBE src").fetchall()]
    axis = ", ".join(plan.table_cols)
    where = f"WHERE {plan.row_filter}" if plan.row_filter else ""
    tables = [r[0] for r in con.execute(
        f"SELECT DISTINCT {axis} FROM src {where} ORDER BY 1").fetchall()]
    stat_cols = [c for c in physical
                 if not c.startswith("_") and c not in {"season", "season_type",
                                                        plan.slug_col, "player"}]

    rows, refusals = [], collections.Counter()
    for table in tables:
        pred = f"{plan.table_cols[0]} = '{table}'"
        if plan.row_filter:
            pred += f" AND {plan.row_filter}"
        n_rows = con.execute(f"SELECT COUNT(*) FROM src WHERE {pred}").fetchone()[0]
        if not n_rows:
            continue
        # PUBLISHED, not merely non-NULL: the empty string is this source's "does not apply"
        # and COUNT() counts it. See published().
        occ = con.execute(
            f"SELECT {', '.join(f'COUNT(TRY_CAST(\"{c}\" AS DOUBLE))' for c in stat_cols)} "
            f"FROM src WHERE {pred}").fetchone()
        nonnull = con.execute(
            f"SELECT {', '.join(f'COUNT(\"{c}\")' for c in stat_cols)} "
            f"FROM src WHERE {pred}").fetchone()
        for col, filled, notnull in zip(stat_cols, occ, nonnull):
            if not filled:
                continue           # never published in this table: nothing to audit
            disp, canonical = next(
                ((d, c) for (tk, cc), (d, c) in dos.items()
                 if cc == col and tk.split("|")[plan.table_key_part] == table),
                ("(not in dossier)", None))
            rec = {"table": table, "column": col, "rows": n_rows,
                   "published": filled,
                   "published_share": round(filled / n_rows, 4),
                   # kept beside it so the gap between them is visible rather than inferred:
                   # where these differ, `''` is doing the work COUNT() reported as data
                   "notnull_share": round(notnull / n_rows, 4),
                   "disposition": disp, "canonical": canonical}
            if disp != "MAPPED_TO_CANONICAL" or not canonical:
                rows.append(rec)
                refusals[disp] += 1
                continue
            if plan.axis_lost or table in plan.axis_lost_tables:
                # the column is a union of several tables' statistics -- see SourcePlan
                rec["audit"] = "REFUSED_TABLE_AXIS_LOST"
                rows.append(rec)
                refusals["refused_table_axis_lost"] += 1
                continue
            if canonical in rate_shaped:
                rec["audit"] = "REFUSED_RATE_SHAPED"
                rows.append(rec)
                refusals["refused_rate_shaped"] += 1
                continue
            agg = AGG_FROM_CLASS.get(klass.get(canonical))
            if agg is None:
                # a rate cannot be aggregated across rows -- summing passer_rating gave a
                # median of 589 against a statistic bounded near 158
                rec["audit"] = "REFUSED_AGGREGATION_CLASS"
                rec["aggregation_class"] = klass.get(canonical)
                rows.append(rec)
                refusals["refused_aggregation_class"] += 1
                continue
            sql_agg = "MAX" if agg == "max" else "SUM"
            # WITH w AS (...) is shared by the blended and the stratified read, and the
            # source filter is published(), never IS NOT NULL -- a row the source left
            # blank is not evidence for or against us.
            base = f"""
                  WITH w AS (SELECT i.pid, TRY_CAST(src.season AS INT) yr,
                                    {sql_agg}(TRY_CAST(src."{col}" AS DOUBLE)) v
                             FROM src JOIN ident i ON i.slug = src.{plan.slug_col}
                             WHERE {pred} AND {published(col)} GROUP BY 1, 2),
                       t AS (SELECT NFL_player_id pid, year yr, {sql_agg}({canonical}) v
                             FROM read_parquet('{v26}') WHERE season_type = 'REG'
                             GROUP BY 1, 2),
                       j AS (SELECT w.yr, w.v sv, t.v vv
                             FROM w JOIN t ON t.pid=w.pid AND t.yr=w.yr)"""
            try:
                r = con.execute(f"""{base}
                  SELECT COUNT(*), COUNT(*) FILTER (WHERE vv > 0 OR sv > 0),
                         COUNT(*) FILTER (WHERE (vv>0 OR sv>0) AND sv = vv),
                         COUNT(*) FILTER (WHERE (vv>0 OR sv>0) AND ABS(sv-vv) <= 1),
                         COUNT(*) FILTER (WHERE vv = 0 AND sv = 0),
                         MEDIAN(sv) FILTER (WHERE vv>0 OR sv>0),
                         MEDIAN(vv) FILTER (WHERE vv>0 OR sv>0),
                         COUNT(*) FILTER (WHERE sv > vv), COUNT(*) FILTER (WHERE sv < vv)
                  FROM j""").fetchone()
                n, nz, exact, pm1, zz, msrc, mv26, gt, lt = r
                if not n:
                    # A MEASUREMENT ON A BASE OF ZERO IS NOT A WEAK RESULT, IT IS NO RESULT.
                    # `Recent Games` returned joined=0 for all 31 of its columns because its
                    # season key is NULL, and every one was filed "MEASURED" with a null
                    # rate -- which reads as a witness that agrees about nothing rather than
                    # as a witness that could not be asked.
                    rec["audit"] = "REFUSED_NO_KEY_OVERLAP"
                    rows.append(rec)
                    refusals["refused_no_key_overlap"] += 1
                    continue
                # EQUALITY is the headline. The +/-1 band is kept beside it because on a
                # count with a median of 1 it accepts {0,1,2} and is not a tolerance at all.
                rec |= {"audit": "MEASURED", "joined": n, "informative_n": nz,
                        "agree_pct": round(100.0*exact/nz, 2) if nz else None,
                        "agree_pct_pm1": round(100.0*pm1/nz, 2) if nz else None,
                        "zero_share": round(zz/n, 4) if n else None,
                        "median_source": msrc, "median_v26": mv26,
                        "source_exceeds_us": gt, "we_exceed_source": lt,
                        # a residual with ONE sign is a coverage story, not a value one
                        "one_sided": (gt == 0) != (lt == 0)}
                rec["by_era"] = [
                    {"era": e, "informative_n": en, "agree_pct": ea,
                     "source_exceeds_us": eg, "we_exceed_source": el}
                    for e, en, ea, eg, el in con.execute(f"""{base}
                      SELECT {ERA_SQL} era, COUNT(*) FILTER (WHERE vv>0 OR sv>0),
                             ROUND(100.0*COUNT(*) FILTER (WHERE (vv>0 OR sv>0) AND sv=vv)
                                   / NULLIF(COUNT(*) FILTER (WHERE vv>0 OR sv>0), 0), 2),
                             COUNT(*) FILTER (WHERE sv > vv), COUNT(*) FILTER (WHERE sv < vv)
                      FROM j GROUP BY 1 ORDER BY 1""").fetchall() if en]
                # the best stratum a column reaches, and on what base -- so a clean mapping
                # under a coverage floor is not filed as a defect
                best = max((e for e in rec["by_era"]
                            if (e["informative_n"] or 0) >= MIN_INFORMATIVE_N),
                           key=lambda e: e["agree_pct"] or 0, default=None)
                rec["best_era"] = best["era"] if best else None
                rec["best_era_agree_pct"] = best["agree_pct"] if best else None
            except Exception as exc:
                rec |= {"audit": "BROKEN", "error": str(exc).splitlines()[0][:90]}
            rows.append(rec)
    con.close()
    return {"source": plan.source, "declared_tables": plan.declared_tables,
            "physical_tables": len(tables), "note": plan.note,
            "row_filter": plan.row_filter, "columns": len(rows),
            "disposition_tally": dict(refusals), "rows": rows}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    targets = list(PLANS) if args.all else [args.source]
    ledger = {}
    if Path(LEDGER).exists():
        ledger = json.loads(Path(LEDGER).read_text(encoding="utf-8"))
    for name in targets:
        if name not in PLANS:
            print(f"unknown source {name}; known: {', '.join(PLANS)}")
            return 1
        print(f"\n=== {name} ===")
        report = audit_source(PLANS[name])
        ledger[name] = report
        if report["declared_tables"] != report["physical_tables"]:
            print(f"  CAPTURE GAP: {report['declared_tables']} declared tables, "
                  f"{report['physical_tables']} physically addressable")
        measured = [r for r in report["rows"] if r.get("audit") == "MEASURED"]
        # an agreement measured on a base of nothing is not a weak result, it is no result
        graded = [r for r in measured
                  if (r.get("informative_n") or 0) >= MIN_INFORMATIVE_N]
        weak = [r for r in graded if (r["agree_pct"] or 0) < 90]
        onesided = [r for r in weak if r.get("one_sided")]
        print(f"  {report['columns']} column-instances | {len(measured)} measured | "
              f"{len(graded)} with a non-zero base | {len(weak)} below 90% | "
              f"{len(onesided)} of those ONE-SIDED")
        for r in sorted(weak, key=lambda x: x["agree_pct"] or 0)[:14]:
            flag = "ONE-SIDED" if r.get("one_sided") else ""
            ms, mv = r.get("median_source"), r.get("median_v26")
            fmt = lambda x: "-" if x is None else f"{float(x):g}"
            # the best stratum is printed beside the blend: a column that is 99% on 2010+
            # and 0% before 1982 is a clean mapping under a coverage floor, not a defect
            print(f"     {str(r['table'])[:20]:20s} {r['column']:12s} -> {r['canonical']:26s} "
                  f"{r['agree_pct'] or 0:5.1f}% n={r['informative_n']:>6,} "
                  f"pub {r.get('published_share', 0):.2f} "
                  f"best {str(r.get('best_era')):8s} {r.get('best_era_agree_pct') or 0:5.1f}% "
                  f"med {fmt(ms)}/{fmt(mv)} {flag}")
        # where the two differ, `''` was being counted as data by COUNT()
        blanks = [r for r in report["rows"]
                  if r.get("notnull_share", 0) - r.get("published_share", 0) > 0.01]
        if blanks:
            print(f"  {len(blanks)} columns carry a NON-NUMERIC placeholder "
                  f"(published_share < notnull_share); lowest: "
                  + ", ".join(f"{r['column']}={r['published_share']:.2f}"
                              for r in sorted(blanks, key=lambda x: x["published_share"])[:6]))
    Path(LEDGER).write_text(json.dumps(ledger, indent=1, default=str), encoding="utf-8")
    print(f"\nledger -> {LEDGER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# =======================================================================================
# THE TWO BLOCKERS, SOLVED. Both were recorded as flat refusals; both were derivable from
# data we already hold. Neither needed a re-harvest or an external dictionary.
# =======================================================================================

#: TEAM KEY. nflcom_team_stats sat PENDING_CROSSWALK on "team-code canonicalization into
#: franchise space". Its `team` column holds a DOUBLED NICKNAME ('Bears Bears'), and all 53
#: values invert cleanly except one truncated before doubling ('Combine (AKA Steagle ...').
#: Undoubling gives a nickname, but a nickname cannot be matched to a franchise by LABEL:
#: every franchise spans the nickname's seasons, so containment is degenerate, and renamed
#: franchises (Redskins -> Football Team -> Commanders) break any name-similarity rule.
#:
#: So the key is fingerprinted on VALUES instead: match each (nickname, season) to the
#: franchise-season whose total passing yards agrees to within a yard. 2,146 of 2,228
#: team-seasons (96.3%) match exactly, and 39 of 42 nicknames resolve to one franchise at
#: >=95% purity. The three that "split" are REAL era transitions and resolve per SEASON --
#: Texans is fid 137 in 1952, fid 30 in 1960-62 (the Dallas Texans that became the Chiefs)
#: and fid 25 from 2002; Titans is fid 20 in 1960-62 (the New York Titans that became the
#: Jets) and fid 28 in Tennessee. Hence the key is (nickname, season) -> fid, never
#: nickname -> fid.
TEAM_KEY_FINGERPRINT = """
  SELECT n.nick, n.yr, m.fid FROM
    (SELECT nick, yr, SUM(TRY_CAST(pass_yds AS DOUBLE)) v FROM ts
     WHERE _category='passing' AND _side='offense' GROUP BY 1, 2) n
  JOIN mine m ON m.yr = n.yr
  WHERE n.v IS NOT NULL AND m.pass_yds IS NOT NULL AND ABS(n.v - m.pass_yds) <= 1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY n.nick, n.yr ORDER BY ABS(n.v - m.pass_yds)) = 1"""

#: BLOCK RECOVERY. player_logs declares 21 tables and physically has 3, because the
#: position-group id (id_gamelog:QB, :WRTE, ...) did not survive capture -- so `yds` is a
#: union of passing, rushing and interception-return yards. It is recoverable WITHOUT a
#: re-parse: the file holds only 6 distinct column-occupancy signatures over 1,324,615
#: rows, and five name their block uniquely.
#:
#: The sixth covers WRTE and RBFB, which share an identical schema ordered oppositely (in
#: RBFB `yds` is rushing, in WRTE it is receiving). That one splits on the INTERNAL
#: identity rather than on our own position data -- using our bio position would be OUR
#: data deciding which of the SITE's blocks a row came from. If both identities are true
#: (for example one rush and one reception for the same yardage), the row is deliberately
#: left unclassified: a tie is not evidence for either block.
#:
#: The recovery proves itself: after assignment, QB.yds -> passing_yards reads 99.6%,
#: QB.int -> passing_interceptions 99.8%, DEF.int -> def_interceptions 99.7% and
#: K.fgm -> fg_made 99.3%. A misassigned block agrees with nothing.
#:
#: NOTE the column-name collision that silently broke the first attempt: `blk` is ALREADY a
#: column in player_logs (field goals blocked), so `SELECT *, CASE ... AS blk` produced a
#: duplicate name and every later reference read the ORIGINAL column. The derived column
#: must not reuse a source column name.
