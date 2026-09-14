"""The pfr tail: 91 open rows over 34 concepts, and most of it is already decided elsewhere.

WHY THIS BLOCK IS SMALL AND WHY THAT MATTERS. pfr began the day at 1,130 open. The plumbing
sweep took 187, the MapSpec import took 188 more -- decisions that already existed in
`witness_map` and that the dossier had simply never consulted. What is left is 34 concepts,
and pfr is the single most valuable witness in the program: 176 canonicals reached, more
than any other lineage, and millions of rows to golden-sample against rather than the 1,892
newspaper cells.

THE EVIDENCE IS PFR'S OWN `data-stat` MACHINE IDS. `pass_cmp`, `rec_yds`, `def_int` are not
our reading of an abbreviation -- they are the attribute names PFR ships in its own HTML,
which is the same evidence class as StatsCrew's `<th title=>`. That is why `pass_int` maps
to `passing_interceptions` (the passer's side) while `def_int` maps to `def_interceptions`
(the defender's): PFR itself distinguishes them by prefix, and we are reading its
distinction rather than inventing one.

THREE THINGS ARE REFUSED, each because it is a KIND of problem this program has already
named rather than a column nobody looked at:

  COMPOSITE CELLS   `awards` ("AP MVP-1, AP1, PB"), `all_pro_string`, `draft_info` -- one
                    cell carrying several facts. Fourth lineage to hit the split-at-
                    admission gap after StatsCrew `results.game`, nflcom splits L7 and the
                    nflcom field-goal buckets.
  THE TACKLE TOTAL  `tackles_combined`, refused by every lineage that publishes it.
  LONG-FORMAT CELLS `stat` / `home_stat` / `vis_stat` / `info` -- a stat NAME and its two
                    team values in three columns. Real material, but the dossier keys on
                    physical columns and these three hold hundreds of concepts between
                    them; they need the LONG regime the newspaper sources use.

Run:  python -m scripts.sota_recon.pfr_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.pfr_column_adjudication"

# PFR data-stat id -> canonical. The prefix IS the disambiguator, and it is PFR's, not ours.
MAPPED: dict[str, str] = {
    "pass_att": "attempts",
    "pass_cmp": "completions",
    "pass_yds": "passing_yards",
    "pass_td": "passing_tds",
    "pass_int": "passing_interceptions",       # the PASSER's interceptions
    "rush_att": "carries",
    "rush_yds": "rushing_yards",
    "rush_td": "rushing_tds",
    "rec": "receptions",
    "rec_yds": "receiving_yards",
    "rec_td": "receiving_tds",
    "def_int": "def_interceptions",            # the DEFENDER's -- PFR's own prefix split
    "sacks": "def_sacks",
    "tackles_solo": "def_tackles_solo",
    "age": "age",
    "pos": "position",
    "def_yds_per_target": "receiving_yards_per_target",
}

NEW_CANDIDATES: dict[str, str] = {
    "gs": "games STARTED. v26 registers games_started at bio/season grain but the weekly "
          "release has no column for it, and the start/no-start fact per game is exactly "
          "what the presence lane needs",
    "experience": "seasons of NFL experience at the time of the row. v26 carries "
                  "years_active and rookie_year but no per-season experience counter",
    "conference_id": "AFC/NFC at the time of the row. v26 has no conference column; the "
                     "franchise lane carries team identity but not its conference, which "
                     "moved for several franchises",
    "def_tgt_yds_per_att": "yards per TARGET allowed by this defender. v26 has "
                           "def_targets_allowed and def_yards_after_catch_allowed but no "
                           "per-target rate, and PFR publishes it without the operands on "
                           "some pages",
    "pass_tgt_yds_per_att": "intended air yards per attempt -- PFR's charting-era passing "
                            "depth measure. v26 carries passing_air_yards but no "
                            "per-attempt form",
    "two_pt_md": "two-point conversions MADE, as a single total. v26 splits "
                 "passing_/rushing_/receiving_2pt_conversions and carries no total, so "
                 "this is not a duplicate of any one of them -- and PFR does not say which "
                 "kind it was, so it cannot be routed to one",
}

EXCLUDED: dict[str, str] = {
    "g": "a games COUNT. The v26 subject is player-WEEK grain and has no such column by "
         "construction; its obligation is the presence lane. Same argument the nflcom and "
         "splits passes made for their own `g`",
    "fantasy_rank_overall": "OUR OWN ranking concept applied to PFR's pool. v26's rank_* "
                            "family ranks against OUR universe, so a PFR rank is neither "
                            "the same number nor a witness for it -- it is a different "
                            "population ranked by a different party",
    "fantasy_rank_pos": "as fantasy_rank_overall, by position",
}

ESCALATED: dict[str, str] = {
    "tackles_combined": "THE TACKLE-TOTAL QUESTION, from the lineage that started it. "
                        "O.9.0 raised it for nflcom player_career (TKL|AST|COMBINED|SOLO), "
                        "StatsCrew raised it from 'Total Tackles' where tackle = solo + "
                        "ast holds on 34.6% of rows, and the nflcom splits DEFENSE block "
                        "raised it again. Four surfaces, one concept. Mapping this to "
                        "def_tackles_combined before the identity is partitioned by era "
                        "would double-count or halve every historical tackle total. "
                        "SETTLED BY: the open escalation, applied to all four at once",
    "awards": "a COMPOSITE cell: PFR writes 'AP MVP-1, AP1, PB' in one string, carrying "
              "MVP, All-Pro first team and Pro Bowl as three facts. It maps to at least "
              "six canonicals (mvp, allpro, all_pro_first_team, pro_bowl, probowls, and "
              "the career_* rollups), so no single-column disposition fits. FOURTH "
              "independent instance of the split-at-admission gap after StatsCrew "
              "results.game, nflcom splits L7 and the nflcom field-goal buckets -- four "
              "lineages make it a vocabulary decision, not a quirk. SETTLED BY: a "
              "split-at-admission disposition that names more than one target",
    "all_pro_string": "the same composite in its own column ('AP1', 'AP2', 'PB'), mapping "
                      "to all_pro_first_team, all_pro_second_team and allpro at once. "
                      "SETTLED BY the same split-at-admission disposition as `awards`",
    "draft_info": "a COMPOSITE cell: 'Chicago Bears / 2nd / 34th pick / 1965' carries "
                  "draft team, round, overall pick and year in one string. Four canonicals "
                  "(nfl_draft_team, draft_round, draft_overall, draft_year), all of which "
                  "v26 already has and none of which can receive a string. SETTLED BY the "
                  "same split-at-admission disposition",
    "stat": "a LONG-FORMAT cell: this column holds the stat NAME and `home_stat` / "
            "`vis_stat` hold its two team values. Between them these three columns carry "
            "hundreds of distinct concepts, so adjudicating `stat` as one column would "
            "assign one disposition to all of them. SETTLED BY: typing pfr_box_team_stats "
            "as a LONG source in column_dossier.py, the way the newspaper sources already "
            "are, so the dossier keys on (source, stat_name) and each concept gets a row",
    "home_stat": "the HOME team's value for whatever `stat` names on that row -- one "
                 "column holding hundreds of concepts. SETTLED BY the same LONG-regime "
                 "re-keying as `stat`",
    "vis_stat": "the VISITING team's value, same shape. SETTLED BY the same LONG-regime "
                "re-keying as `stat`",
    "info": "a LONG-FORMAT cell on pfr_box_game_info: stadium, weather, roof, surface and "
            "attendance arrive as name/value pairs under one column. SETTLED BY typing "
            "pfr_box_game_info as a LONG source so each concept gets its own dossier row",
}


def build_decisions() -> tuple[list[dict], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    from .column_dossier import load_decisions

    mine = {k for k, e in load_decisions().items() if e.get("generated_by") == GENERATOR}
    decisions: list[dict] = []
    tally = collections.Counter()
    unhandled: set[str] = set()

    for row in dossier["rows"]:
        if row["lineage"] != "pfr":
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        name = row["column"]
        evidence = (f"PFR's own `data-stat` machine id {name!r} on {row['source']} -- the "
                    "attribute name the site ships in its HTML, not our reading of an "
                    "abbreviation. Same evidence class as StatsCrew's <th title=>")
        if name in ESCALATED:
            tally["escalated_left_open"] += 1
            continue
        if name in MAPPED:
            decisions.append({"key": key, "disposition": "MAPPED_TO_CANONICAL",
                              "canonical": MAPPED[name],
                              "reason": f"PFR's {name!r}; v26 carries the same material as "
                                        f"{MAPPED[name]!r}",
                              "evidence": evidence})
            tally["mapped"] += 1
        elif name in NEW_CANDIDATES:
            decisions.append({"key": key,
                              "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                              "reason": NEW_CANDIDATES[name], "evidence": evidence})
            tally["new"] += 1
        elif name in EXCLUDED:
            decisions.append({"key": key, "disposition": "EXCLUDED_WITH_REASON",
                              "reason": EXCLUDED[name], "evidence": evidence})
            tally["excluded"] += 1
        else:
            unhandled.add(name)
            tally["unhandled"] += 1
    return decisions, dict(tally) | {"unhandled_names": sorted(unhandled)}


def escalated_row_keys() -> dict[str, str]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for row in dossier["rows"]:
        if row["lineage"] == "pfr" and row["disposition"] == "OPEN" \
                and row["column"] in ESCALATED:
            out[row_key(row["source"], row["table_key"], row["column"])] = \
                ESCALATED[row["column"]]
    return out


def _validate(v26: set[str]) -> list[str]:
    problems = [f"{k}: canonical {c!r} does not exist" for k, c in MAPPED.items()
                if c not in v26]
    problems += [f"{k}: proposed NEW but v26 carries it" for k in NEW_CANDIDATES
                 if k in v26]
    seen: dict[str, str] = {}
    for name, table in (("MAPPED", MAPPED), ("NEW", NEW_CANDIDATES),
                        ("EXCLUDED", EXCLUDED), ("ESCALATED", ESCALATED)):
        for k in table:
            if k in seen:
                problems.append(f"{k}: in {seen[k]} and {name}")
            seen[k] = name
    for q in ESCALATED.values():
        if "SETTLED BY" not in q:
            problems.append("an escalation must say what would settle it")
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
