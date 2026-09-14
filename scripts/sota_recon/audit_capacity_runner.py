"""THE EQUATION LANE: run the audit capacity that excluded columns already carry.

WHY THIS EXISTS. `disposition` answers "is this column ours?" and `audit_capacity` answers
"can it CHECK us?". The second field landed on 2026-07-31 with `avg` proven at 99.98% --
and nothing executed it, so a RECEIPTED capacity was a note rather than a check.

It is not one column. On nflcom_player_career eleven MAPPED columns are refused for
AGGREGATION CLASS -- fg_pct, pat_pct, passer_rating and five *_per_* rates -- and the
refusal is correct: a rate cannot be summed across a season, and summing passer_rating gave
a v26 median of 589 against a statistic bounded near 158. But every one of them is an
EQUATION over operands we store, so the refusal that blocks the VALUE PATH does not block
the AUDIT. A rate that reproduces from our numerator and our denominator checks BOTH.

THE TWO TRAPS THIS LANE IS BUILT AROUND.

  GRAIN. A rate is a per-ROW value, so a traded player's two season rows cannot be summed
  or averaged into one. Only seasons the source renders as a SINGLE row are used, and the
  restriction is declared rather than discovered -- `avg` measured 99.98% on 15,251
  single-row seasons and would be meaningless across two.

  TOLERANCE. These are DISPLAY values, rounded to one or two decimals by the site. The
  O.9.0 pass proved a 0.06 absolute band is the right test for a one-decimal table by
  sweeping it -- agreement cliffs to exactly 100% there -- and that a tighter band produces
  54-58% agreement that reads as a label problem and is a tolerance artefact. The band is
  derived from the DECIMALS the source publishes, not chosen.

    python -m scripts.sota_recon.audit_capacity_runner
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb

from . import sources as S
from .column_dossier import load_decisions

HERE = Path(__file__).resolve().parent
#: measurements go OFF-REPO beside the other measurement ledgers -- the programme's law
#: forbids new audit artifacts in docs/, and a receipt is a measurement not a decision
LEDGER = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\audit_capacity_receipts.json")

#: `col == a / b` with an optional `* k` scale. Deliberately the ONLY form parsed: a
#: general expression evaluator would let a capacity claim mean anything, and every rate on
#: these pages is a two-operand ratio.
#:
#: THE SCALE IS NOT OPTIONAL DECORATION. A percentage column is published as 85.7 while
#: made/att is 0.857, so an unscaled comparison reads 0% and looks like a wrong mapping.
#: This programme has already been bitten from BOTH directions -- `fg%` needed x100 and
#: `xp%` needed /100 in the same sweep, so no blanket rule works and the factor is declared
#: per claim.
RATIO = re.compile(r"^\s*(\w+)\s*==\s*(\w+)\s*/\s*(\w+)\s*(?:\*\s*([0-9.]+)\s*)?$")

#: NAMED FORMULAS -- the second, CLOSED expression form. `passer_rating` is a real audit
#: (its definition is fixed by rule and it consumes five of our columns at once) and it is
#: not a two-operand ratio, so the ratio parser refuses it correctly. A registry keeps the
#: vocabulary closed exactly as the ratio parser does: only names appearing HERE may be
#: claimed, so a capacity claim still cannot mean anything it likes.
#:
#: The NFL passer rating clamps each of its four components to [0, 2.375] BEFORE averaging.
#: Dropping the clamp is the classic error -- it lets a 5-attempt 5-completion game read far
#: above the 158.3 maximum, and the resulting disagreement looks like a supertable defect.
NAMED_FORMULAS = {
    "passer_rating": {
        "operands": ("completions", "attempts", "passing_yards", "passing_tds",
                     "passing_interceptions"),
        "sql": ("(GREATEST(0, LEAST(2.375, (({0}/{1}) - 0.3) * 5))"
                " + GREATEST(0, LEAST(2.375, (({2}/{1}) - 3) * 0.25))"
                " + GREATEST(0, LEAST(2.375, ({3}/{1}) * 20))"
                " + GREATEST(0, LEAST(2.375, 2.375 - (({4}/{1}) * 25)))) / 6 * 100"),
        "guard": "{1} > 0",
    },
}

#: `col == name(a, b, ...)`. Same discipline as RATIO: a closed vocabulary, never a
#: general evaluator.
FORMULA = re.compile(r"^\s*(\w+)\s*==\s*(\w+)\s*\(([^)]*)\)\s*$")


def parse_formula(expression: str):
    """(source_col, formula_name, operands) for a NAMED formula, else None."""
    m = FORMULA.match(expression or "")
    if not m or m.group(2) not in NAMED_FORMULAS:
        return None
    ops = tuple(x.strip() for x in m.group(3).split(",") if x.strip())
    spec = NAMED_FORMULAS[m.group(2)]
    if len(ops) != len(spec["operands"]):
        return None
    return m.group(1), m.group(2), ops


#: derived, not chosen: half a unit in the last published decimal place
TOLERANCE_BY_DECIMALS = {1: 0.06, 2: 0.006, 3: 0.0006}


def parse(expression: str) -> tuple[str, str, str, float] | None:
    m = RATIO.match(expression or "")
    return (m.group(1), m.group(2), m.group(3), float(m.group(4) or 1.0)) if m else None


def decimals_of(con, table: str, col: str, slug_col: str, pred: str) -> int:
    """How many decimals does the SOURCE publish? The tolerance follows from this."""
    row = con.execute(f"""
        SELECT MAX(LENGTH(SPLIT_PART(CAST("{col}" AS VARCHAR), '.', 2)))
        FROM src WHERE {pred} AND TRY_CAST("{col}" AS DOUBLE) IS NOT NULL""").fetchone()
    return int(row[0] or 1)


#: WHICH PLANE THE OPERANDS COME FROM. A published rate is a witness on whatever plane its
#: operands are read from, and until now they were ALWAYS read from the weekly release --
#: so every rate verified our weekly->season ROLLUP and never the STORED season table.
#: Joe, three times: "avg is just another way to witness on the season and career levels."
#: That matters because the stored plane is exactly where the summed-instead-of-maxed
#: punt_long defect lived with nothing watching it.
def operands_for(plane_path: str, numerator: str, denominator: str) -> str:
    """The (pid, yr, num, den) CTE for one subject plane.

    WEEKLY needs SUM + a season_type filter because it is week-grain. The stored SEASON
    plane is ALREADY one row per (player, year) and carries no season_type column, so
    summing it would be wrong twice over -- it would double nothing and reference a column
    that does not exist.
    """
    if plane_path.endswith("player_nfl_season.parquet"):
        return (f"SELECT NFL_player_id pid, year yr, {numerator} num, {denominator} den "
                f"FROM read_parquet('{plane_path}')")
    return (f"SELECT NFL_player_id pid, year yr, SUM({numerator}) num, SUM({denominator}) den "
            f"FROM read_parquet('{plane_path}') WHERE season_type='REG' GROUP BY 1, 2")


def run_one(con, v26: str, ident: str, plan_table: str, table_col: str, slug_col: str,
            source_col: str, numerator: str, denominator: str, scale: float = 1.0,
            row_filter: str = "") -> dict:
    """Measure a published rate against OUR two operands, on single-row seasons only."""
    pred = f"{table_col} = '{plan_table}'" + (f" AND ({row_filter})" if row_filter else "")
    dec = decimals_of(con, plan_table, source_col, slug_col, pred)
    # the band is half a unit in the LAST PUBLISHED DECIMAL, which lives in the same units
    # as the published value -- so a percentage's band is 0.06 PERCENT, not 0.06 of a ratio
    tol = TOLERANCE_BY_DECIMALS.get(dec, 0.06)
    operand_cte = operands_for(v26, numerator, denominator)
    r = con.execute(f"""
      WITH w AS (
        SELECT i.pid, TRY_CAST(src.season AS INT) yr,
               ANY_VALUE(TRY_CAST(src."{source_col}" AS DOUBLE)) rate, COUNT(*) nrow
        FROM src JOIN ident i ON i.slug = src.{slug_col}
        WHERE {pred} AND TRY_CAST(src."{source_col}" AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2),
      t AS ({operand_cte}),
      j AS (SELECT w.rate, t.num, t.den FROM w JOIN t ON t.pid=w.pid AND t.yr=w.yr
            WHERE w.nrow = 1 AND t.den > 0)
      SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(rate - (num/den)*{scale}) <= {tol})
      FROM j""").fetchone()
    n, ok = r
    return {"n": n, "agree_pct": round(ok / n, 4) if n else None,
            "decimals_published": dec, "tolerance": tol,
            "expression": f"{source_col} == {numerator} / {denominator}"
                          + (f" * {scale:g}" if scale != 1.0 else "")}


def _unsum(sums: str) -> str:
    """Strip the SUM() wrapper from a formula's operand list for an already-aggregated plane.

    `sums` is built as "SUM(a) AS a, SUM(b) AS b". On the stored season plane the row IS the
    season, so SUM would be a no-op that still forces a GROUP BY -- and on a plane with no
    season_type column the filter would fail outright. Rewriting is safer than maintaining
    two format strings that must stay in step.
    """
    import re
    return re.sub(r"SUM\(([^)]*)\)", r"\g<1>", sums)


def run_formula(con, v26: str, plan_table: str, table_col: str, slug_col: str,
                source_col: str, name: str, operands: tuple,
                row_filter: str = "") -> dict:
    """Recompute a NAMED formula from OUR columns and compare to the published value."""
    spec = NAMED_FORMULAS[name]
    pred = f"{table_col} = '{plan_table}'" + (f" AND ({row_filter})" if row_filter else "")
    dec = decimals_of(con, plan_table, source_col, slug_col, pred)
    tol = TOLERANCE_BY_DECIMALS.get(dec, 0.06)
    sums = ", ".join(f"SUM({c}) o{i}" for i, c in enumerate(operands))
    refs = [f"o{i}" for i in range(len(operands))]
    expr = spec["sql"].format(*refs)
    guard = spec["guard"].format(*refs)
    # SAME PLANE RULE as operands_for: the stored season plane is already player-year grain
    # and has no season_type column, so it must not be summed or filtered.
    if v26.endswith("player_nfl_season.parquet"):
        formula_cte = (f"SELECT NFL_player_id pid, year yr, {_unsum(sums)} "
                       f"FROM read_parquet('{v26}')")
    else:
        formula_cte = (f"SELECT NFL_player_id pid, year yr, {sums} "
                       f"FROM read_parquet('{v26}') WHERE season_type='REG' GROUP BY 1, 2")
    r = con.execute(f"""
      WITH w AS (SELECT i.pid, TRY_CAST(src.season AS INT) yr,
                        ANY_VALUE(TRY_CAST(src."{source_col}" AS DOUBLE)) rate, COUNT(*) nrow
                 FROM src JOIN ident i ON i.slug = src.{slug_col}
                 WHERE {pred} AND TRY_CAST(src."{source_col}" AS DOUBLE) IS NOT NULL
                 GROUP BY 1, 2),
           t AS ({formula_cte}),
           j AS (SELECT w.rate, {expr} AS ours FROM w JOIN t ON t.pid=w.pid AND t.yr=w.yr
                 WHERE w.nrow = 1 AND {guard})
      SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(rate - ours) <= {tol}) FROM j""").fetchone()
    n, ok = r
    return {"n": n, "agree_pct": round(ok / n, 4) if n else None,
            "decimals_published": dec, "tolerance": tol,
            "expression": f"{source_col} == {name}({', '.join(operands)})"}


#: The runner was welded to player_career: one parquet path, one `_table` discriminator,
#: one slug column. player_season keys on `_category` and `_player_slug` and filters
#: season_type, so its rates could not be measured at all -- `passing.rate` sat refused for
#: aggregation class with no way to make the equation claim run.
SOURCE_PLANS = {
    "nflcom_player_career": {
        "glob": "D:/league-history-data/nfl/raw/nflcom/tables/player_career/**/*.parquet",
        "table_col": "_table", "slug_col": "nflcom_slug", "row_filter": "",
    },
    "nflcom_player_season": {
        "glob": "D:/league-history-data/nfl/raw/nflcom/tables/player_season/**/*.parquet",
        "table_col": "_category", "slug_col": "_player_slug",
        "row_filter": "season_type = 'reg'",
    },
}


def build(plane: str = "weekly") -> dict:
    """Every EQUATION-or-FORMULA capacity, measured, across every planned source.

    `plane` selects WHICH SUBJECT the operands come from. "weekly" (the default, and the only
    behaviour before 2026-07-31) verifies our weekly->season rollup. "season" verifies the
    STORED season table -- the one we actually serve, and the plane where three MAX columns
    were being summed with nothing watching. Joe asked for this three times.
    """
    decisions = load_decisions()
    claims = [(k, e) for k, e in decisions.items()
              if (e.get("audit_capacity") or {}).get("kind") in {"EQUATION", "FORMULA"}]
    v26 = Path(S.v26_plane(plane)).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
    out = []
    con = duckdb.connect(); con.execute("SET memory_limit='6GB'")
    con.execute(f"""CREATE TABLE ident AS SELECT b.NFL_player_id pid, x.nflcom_slug slug
                    FROM read_parquet('{xw}') x
                    JOIN read_parquet('{bio}') b ON b.pfr_id = x.pfr_id""")
    loaded = None
    for key, entry in sorted(claims):
        cap = entry["audit_capacity"]
        parts = key.split("|")
        source, column = parts[0], parts[-1]
        plan_table = parts[1]
        plan = SOURCE_PLANS.get(source)
        if plan is None:
            out.append({"key": key, "status": "NOT_RUN",
                        "why": f"no SOURCE_PLAN for {source}"})
            continue
        if loaded != source:
            con.execute("DROP TABLE IF EXISTS src")
            con.execute(f"CREATE TABLE src AS SELECT DISTINCT * FROM "
                        f"read_parquet('{plan['glob']}', union_by_name=True)")
            loaded = source
        parsed = parse(cap.get("expression", ""))
        formula = None if parsed else parse_formula(cap.get("expression", ""))
        if not parsed and not formula:
            out.append({"key": key, "status": "UNPARSEABLE",
                        "why": f"neither a two-operand ratio nor a NAMED formula: "
                               f"{cap.get('expression')!r}"})
            continue
        try:
            if parsed:
                _, num, den, scale = parsed
                res = run_one(con, v26, "ident", plan_table, plan["table_col"],
                              plan["slug_col"], column, num, den, scale,
                              plan["row_filter"])
            else:
                _, name, ops = formula
                res = run_formula(con, v26, plan_table, plan["table_col"],
                                  plan["slug_col"], column, name, ops,
                                  plan["row_filter"])
        except Exception as exc:
            out.append({"key": key, "status": "BROKEN",
                        "why": str(exc).splitlines()[0][:120]})
            continue
        # RE-MEASURED, not inherited: the contract's receipt is prose, this is the number
        res |= {"key": key, "source": source, "status": "MEASURED",
                "declared_status": cap.get("status"), "audits": cap.get("audits")}
        out.append(res)
    con.close()
    measured = [r for r in out if r["status"] == "MEASURED"]
    return {"counters": {
                "audit_capacity_equations_declared": len(claims),
                "audit_capacity_equations_measured": len(measured),
                "audit_capacity_equations_not_run": len(out) - len(measured)},
            "rows": out}


def main() -> int:
    report = build()
    # THE SECOND PLANE, in the SAME ledger. Its rows are keyed "<row key>@season" so the
    # gate can tell the two apart and neither can satisfy the other's floor -- the same
    # precision lesson as the per-table licence key, where QB Career's `fumbles` floor was
    # being satisfied by whichever of QB/RBFB/WRTE validated last.
    season = build("season")
    for r in season["rows"]:
        report["rows"].append({**r, "key": r["key"] + "@season", "plane": "season"})
    report["counters"]["season_plane_rows"] = len(season["rows"])
    for k, v in report["counters"].items():
        print(f"  {k:46s} {v:4,}")
    print()
    for r in report["rows"]:
        col = r["key"].split("|")[-1]
        tbl = r["key"].split("|")[1].split("|")[0]
        if r["status"] != "MEASURED":
            print(f"   {tbl:22s} {col:8s} {r['status']}: {r.get('why')}")
            continue
        print(f"   {tbl:22s} {col:8s} {r['agree_pct']} on n={r['n']:,} "
              f"(tol {r['tolerance']}, {r['decimals_published']}dp)  {r['expression']}")
    Path(LEDGER).write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    print(f"\nreceipts -> {LEDGER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
