"""The legacy supertable's dead vocabulary: 133 columns, decided by MEASUREMENT.

Joe's method, applied where names are useless: "you take your best guess on meaning for the
column names. you golden sample them vs the supertable. if its apples to apples we know we
have another witness."

This block is where that is the ONLY workable method. `legacy_motherduck_supertable` is our
own retired schema, and its column names are dedup artefacts and 1990s shorthand -- `ret`,
`ret_1`, `ret_2`, `ret_3`, `yds_1`, `yds_4`, `yds_5`, `col_10`, `y/a_1`. Four columns named
`ret*` cannot be told apart by reading, and `col_39` says nothing at all. So every decision
below is a join against the live supertable on `player_week`, 651,440 overlapping rows, and
the agreement rate IS the receipt.

THE MEASUREMENT CAUGHT A TRAP IN OUR OWN VOCABULARY. `y/c` reads as yards per carry and is
yards per COMPLETION:

    y/c == passing_yards / completions   99.6% of 23,721 rows
    y/c == rushing_yards / carries        0.2% of 17,498 rows

That is PFR's convention (Y/C = yards per completion) inherited into our legacy schema, and
it is the same shape as StatsCrew's `td`-is-Touchdown-Percentage. Reading the name would
have mapped it to `rushing_yards_per_carry` and been wrong on every row.

AND IT FOUND A SCALE DIFFERENCE. `fg%` matches `fg_pct` at 99.8% ONLY when multiplied by
100 -- the legacy table stores 0-100, the live one stores 0-1. Mapped, with the scale
recorded, because a witness that disagrees by a factor of 100 would fail every future
tolerance check for a reason nobody would find.

85 OF THE 133 ARE ALL-ZERO across 732,587 rows and are excluded on that measurement rather
than on a reading of their names. `col_10`, `col_39`, `3datt`, `opp3dconv`, `top`, `to` --
the schema declared them and nothing ever wrote a non-zero value. That is the same argument
the StatsCrew CFL columns got: the exclusion discards nothing because there is nothing there.

Run:  python -m scripts.sota_recon.legacy_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.legacy_column_adjudication"
SOURCE = "legacy_motherduck_supertable"

# legacy column -> (canonical, agreement, n). Every one measured on player_week.
MEASURED: dict[str, tuple[str, str, int]] = {
    "ret": ("kickoff_returns", "99.6%", 23647),
    "ret_2": ("kickoff_returns", "99.6%", 23647),
    "ret_1": ("punt_returns", "99.5%", 11729),
    "ret_3": ("punt_returns", "99.5%", 11729),
    "yds_4": ("kickoff_return_yards", "99.5%", 23261),
    "yds_5": ("punt_return_yards", "99.3%", 10686),
    "1d": ("rushing_first_downs", "96.0%", 24221),
    "1d_1": ("receiving_first_downs", "97.2%", 43031),
    "att": ("carries", "99.6%", 79478),
    "cmp": ("completions", "99.5%", 23756),
    "rec": ("receptions", "99.5%", 101724),
    "tgt": ("targets", "99.1%", 68334),
    "sk": ("sacks_suffered", "99.6%", 15528),
    "fgm": ("fg_made", "99.6%", 5159),
    "pts": ("total_points_scored", "99.5%", 7005),
    "krtd": ("kickoff_return_tds", "100.0%", 273),
    "prtd": ("punt_return_tds", "99.2%", 262),
    "fumble_recovery_tds": ("fum_ret_td", "99.9%", 751),
    # rates, confirmed against the canonical rather than against a numerator
    "y/a": ("passing_yards_per_attempt", "99.5%", 23722),
    "y/r": ("receiving_yards_per_reception", "99.6%", 100861),
    "y/tgt": ("receiving_yards_per_target", "99.2%", 58401),
    "int%": ("passing_int_pct", "99.4%", 14006),
    "td%": ("passing_td_pct", "99.4%", 13516),
    "ctch%": ("catch_pct", "99.3%", 58715),
    "rate": ("passer_rating", "93.6%", 24292),
    # These four read as obvious and were still wrong on the FIRST sweep, for two
    # different reasons -- neither of which the name would have surfaced.
    # `cmp%` scored 36.9% at a 0.011 absolute tolerance because the legacy table stores
    # one decimal and the live one stores full precision; the same pair scores 99.6%
    # once the tolerance is relative. `any/a` and `ay/a` scored nothing because the
    # candidate list had no `passing_`-prefixed names in it -- the canonicals were
    # there the whole time. Both are measurement defects, not source defects.
    "cmp%": ("completion_pct", "99.6%", 23749),
    "any/a": ("passing_adjusted_net_yards_per_attempt", "99.5%", 21166),
    "ay/a": ("passing_adjusted_yards_per_attempt", "99.6%", 24211),
    # `yds_1` names nothing at all and turns out to be SACK YARDS LOST -- 99.5% of 17,041
    # rows, against 0.0% for interception-return, punt, fumble-recovery and receiving
    # yards. No reading of "yds_1" could have produced that; only the join could.
    "yds_1": ("sack_yards_lost", "99.5%", 17041),
}

# Confirmed against the OPERANDS rather than against a stored canonical, because the
# canonical either does not exist at weekly grain or the legacy column is the only form.
DERIVED_CONFIRMED: dict[str, tuple[str, str, str]] = {
    "y/c": ("passing_yards / completions", "99.6% of 23,721",
            "PFR's Y/C = yards per COMPLETION, NOT per carry: the rushing reading scores "
            "0.2% on 17,498 rows. Reading the name would have mapped it to "
            "rushing_yards_per_carry and been wrong everywhere"),
    "y/ret": ("kickoff_return_yards / kickoff_returns", "99.8% of 23,087",
              "kickoff return average. v26 has no return-average column, so this is the "
              "only form the value takes"),
    "y/a_1": ("rushing_yards / carries", "99.6% of 76,600",
              "yards per rush attempt. The unsuffixed `y/a` is the PASSING one (99.5% vs "
              "passing_yards_per_attempt), so the dedup suffix is the only thing "
              "separating two different rates -- and it separates them correctly"),
    "y/ret_1": ("punt_return_yards / punt_returns", "99.7% of 10,627",
                "punt return average, mirroring `y/ret` for kickoffs. v26 carries no "
                "return-average column, so this is the only form the value takes"),
    "sk%": ("100 * sacks_suffered / (attempts + sacks_suffered)", "99.6% of 15,509",
            "sack percentage. No stored canonical scores above 0.4% because v26 carries "
            "no `sack_pct`; the denominator is DROPBACKS, and reconstructing it as "
            "attempts+sacks_suffered lands the value. The operands are both present on "
            "the live table, so the legacy column carries nothing they do not"),
    "inc": ("attempts - completions", "99.6% of 24,772",
            "incompletions. Derivable where both operands are present, which they are on "
            "the live table, so the row carries no material they do not"),
}

# `fg%` matches only at x100 -- a genuine SCALE difference between the two tables.
SCALED: dict[str, tuple[str, float, str]] = {
    # NOTE the two scales run in OPPOSITE directions. `fg%` needs x100 and `xp%` needs
    # /100, so the legacy table is not internally consistent about percent storage and
    # a single blanket scale rule would have broken one of them.
    "xp%": ("pat_pct", 0.01, "99.9% of 6,443 rows AFTER dividing by 100. The legacy table "
                             "stores 0-100 here and the live one stores 0-1. The first "
                             "sweep called this 'every candidate scored 0.0%, including "
                             "at x100' -- true, and it was scanning the wrong direction "
                             "against a candidate list that also omitted `pat_pct`. "
                             "The runner-up is fg_pct at 53.4%, so the margin is real"),
    "fg%": ("fg_pct", 100, "99.8% of 5,156 rows AFTER multiplying by 100. The legacy table "
                           "stores the percentage 0-100 and the live one stores 0-1. "
                           "Mapped WITH the scale recorded: a witness that disagrees by a "
                           "factor of 100 would fail every tolerance check for a reason "
                           "nobody would find"),
}

# All-zero across 732,587 rows -- measured, not read off the name.
DEAD = {
    "1std", "1stdopp", "2pa", "3d%", "3datt", "3dconv", "4d%", "4datt", "4datt_1", "4dconv",
    "att_3", "blck", "blck_1", "cmp%_1", "col_10", "col_11", "col_13", "col_14", "col_15",
    "col_23", "col_39", "col_49", "col_59", "col_9", "date", "day_of_week", "dply", "dy/p",
    "frtd", "int_1", "inttd", "inttd_1", "krtd_1", "md", "md_1", "ny/a", "opp3d%",
    "opp3datt", "opp3dconv", "opp4d%", "opp4datt", "opp4dconv", "opp4dconv_1", "oppfrtd",
    "oppinttd", "oppkrtd", "oppkrtd_1", "oppprtd", "othtd", "pass", "pc", "pen", "ply",
    "pnt", "ptdif", "pts_def_high", "pts_k_yahoo", "ptso", "ptso_1", "rate_1", "result",
    "rettd", "rettd_1", "rsh", "rush", "sfty", "sfty_1", "sfty_2", "sk_1", "source_file",
    "td_3", "td_4", "td_5", "td_6", "time", "to", "to_1", "top", "tot", "tot_1", "xpa_1",
    "xpm_1", "y/p", "y/p_1", "yds_6",
}
_DEAD_REASON = (
    "MEASURED ALL-ZERO: not one non-zero value across 732,587 rows. The retired schema "
    "declared the column and nothing ever wrote to it, so excluding it discards nothing -- "
    "the same argument the StatsCrew CFL columns got, and it is a measurement rather than a "
    "reading of the name (`col_39` has no name to read)")

EXCLUDED: dict[str, str] = {
    "rank": "OUR OWN ranking against OUR OWN pool, in a retired schema. v26's rank_* family "
            "is the live form of the same idea; a legacy rank is not a witness for it",
    "fantasy_points": "a league scoring variant we computed. The source publishes the "
                      "inputs, never our weighted sum",
    "game_number": "locates the row within a season; its obligation is the crosswalk lane",
    "player_year": "locates the row; a player-season key",
    "computed": "a pipeline flag recording that the row was derived rather than loaded",
    "side": "offense/defense marker for the row -- a row selector, not a measurement",
    "points": "an all-but-empty twin of `pts`, which is measured and mapped. Kept out "
              "rather than double-mapped to total_points_scored",
    "safeties": "an all-but-empty twin of the measured `sfty` family, all of which are dead",
    "two_pt": "a two-point flag with no surviving definition of WHICH kind (pass, rush or "
              "reception). v26 splits the three, so mapping it would pick one at random",
    "2pm": "two-point conversions made, but only 1 non-zero row in 732,587 -- below any "
           "threshold at which a golden sample could confirm which of the three canonical "
           "two-point columns it is",
    "fg_made_list": "a LIST cell: the made-field-goal distances as one packed string. Same "
                    "split-at-admission shape as the nflcom made-att buckets",
    "fg_missed_list": "a LIST cell of missed field-goal distances",
    "fg_blocked_list": "a LIST cell of blocked field-goal distances",
}

# Left OPEN deliberately: the golden sample refuted every candidate.
# The succ% family is the one place where the right expression EXISTS, is well formed,
# and still does not agree. That is the signature of a DEFINITION mismatch rather than a
# mapping gap, so it escalates instead of resolving -- the numbers below are the argument.
UNRESOLVED: dict[str, str] = {
    "succ%": "DEFINITION MISMATCH, measured. 100*pass_success/pass_success_plays -- the "
             "correctly-formed rate out of our own stored numerator and denominator -- "
             "agrees on only 12.1% of 10,826 rows. A malformed expression scores near "
             "zero and a right one scores near 100; 12% is neither, which is what two "
             "different STATISTICS sharing a name look like. PFR scores a play "
             "successful on yardage-to-go thresholds by down; our *_success columns do "
             "not. SETTLED BY: Joe ruling whether to carry PFR's definition as a "
             "separate canonical, or to leave the legacy column dead",
    "succ%_1": "as succ%: 100*rush_success/rush_success_plays agrees on 45.1% of 27,862 "
               "rows. SETTLED BY the same ruling",
    "succ%_2": "as succ%: 100*rec_success/rec_success_plays agrees on 66.7% of 49,306 "
               "rows. The rising agreement across the three (12% -> 45% -> 67%) tracks "
               "how often the two definitions happen to coincide, not how close the "
               "mapping is. SETTLED BY the same ruling",
}


def build_decisions() -> tuple[list[dict], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    from .column_dossier import load_decisions

    mine = {k for k, e in load_decisions().items() if e.get("generated_by") == GENERATOR}
    decisions: list[dict] = []
    tally = collections.Counter()
    unhandled: set[str] = set()

    for row in dossier["rows"]:
        if row["source"] != SOURCE:
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        name = row["column"]
        if name in UNRESOLVED:
            tally["unresolved_left_open"] += 1
            continue
        if name in MEASURED:
            canon, rate, n = MEASURED[name]
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canon,
                "reason": f"GOLDEN SAMPLE: the legacy column equals {canon!r} on {rate} of "
                          f"{n:,} rows joined on player_week. The name could not have said "
                          f"this -- four columns are called ret*",
                "evidence": f"joined legacy_motherduck_supertable to the live release on "
                            f"player_week, 651,440 overlapping rows; {name!r} vs {canon!r} "
                            f"agrees {rate} on {n:,} non-zero rows"})
            tally["mapped"] += 1
        elif name in SCALED:
            canon, scale, note = SCALED[name]
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canon,
                "reason": f"GOLDEN SAMPLE at scale x{scale}: {note}",
                "evidence": f"legacy {name!r} vs {canon!r} on player_week; {note}"})
            tally["mapped_with_scale"] += 1
        elif name in DERIVED_CONFIRMED:
            expr, rate, note = DERIVED_CONFIRMED[name]
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"a derived rate CONFIRMED against its operands: {name} == {expr} "
                          f"on {rate}. {note}",
                "evidence": f"golden sample on player_week: {name} == {expr}, {rate}"})
            tally["derived_confirmed"] += 1
        elif name in DEAD:
            decisions.append({"key": key, "disposition": "EXCLUDED_WITH_REASON",
                              "reason": _DEAD_REASON,
                              "evidence": "SUM(CASE WHEN value <> 0) = 0 over 732,587 rows"})
            tally["dead"] += 1
        elif name in EXCLUDED:
            decisions.append({"key": key, "disposition": "EXCLUDED_WITH_REASON",
                              "reason": EXCLUDED[name],
                              "evidence": f"legacy_motherduck_supertable.{name}"})
            tally["excluded"] += 1
        else:
            unhandled.add(name)
            tally["unhandled"] += 1
    return decisions, dict(tally) | {"unhandled_names": sorted(unhandled)}


def escalated_row_keys() -> dict[str, str]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    return {row_key(r["source"], r["table_key"], r["column"]): UNRESOLVED[r["column"]]
            for r in dossier["rows"]
            if r["source"] == SOURCE and r["disposition"] == "OPEN"
            and r["column"] in UNRESOLVED}


def _validate(v26: set[str]) -> list[str]:
    problems = [f"{k}: canonical {c!r} does not exist"
                for k, (c, _, _) in MEASURED.items() if c not in v26]
    problems += [f"{k}: canonical {c!r} does not exist"
                 for k, (c, _, _) in SCALED.items() if c not in v26]
    seen: dict[str, str] = {}
    for name, table in (("MEASURED", MEASURED), ("SCALED", SCALED),
                        ("DERIVED", DERIVED_CONFIRMED), ("DEAD", {k: 1 for k in DEAD}),
                        ("EXCLUDED", EXCLUDED), ("UNRESOLVED", UNRESOLVED)):
        for k in table:
            if k in seen:
                problems.append(f"{k}: in {seen[k]} and {name}")
            seen[k] = name
    for q in UNRESOLVED.values():
        # Tightened 2026-07-29. The three alternates that used to satisfy this check
        # ("worth one more test", "does not carry", "differs") were written to let vague
        # entries through, and every entry that leaned on them turned out to be WRONG on
        # re-measurement -- cmp% was a tolerance artifact, any/a and ay/a were candidate-
        # list omissions, xp% was a scale tested in one direction only. An unresolved
        # column now has to name the act that would settle it, with no way around it.
        if "SETTLED BY" not in q:
            problems.append(f"an unresolved column must say what would settle it: {q[:40]}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    problems = _validate(_v26_columns())
    if problems:
        print("CORRESPONDENCE TABLE INVALID:")
        for p in problems:
            print("  -", p)
        return 1
    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for k, v in tally.items():
        if k != "unhandled_names":
            print(f"  {k:22s} {v}")
    if tally["unhandled_names"]:
        print("\nUNHANDLED (left OPEN):", tally["unhandled_names"])
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
