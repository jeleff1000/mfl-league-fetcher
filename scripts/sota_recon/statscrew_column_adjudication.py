"""O.9.3 burn-down: adjudicate the StatsCrew stat families from the SOURCE'S OWN titles.

THE EVIDENCE IS THE SITE'S PUBLISHED COLUMN NAME, not our reading of its abbreviation.
StatsCrew ships the full name in a `title` attribute on every header cell, and the
harvest keeps them in `COLUMN_DICTIONARY.jsonl` next to each shard's records. The short
labels are a MEASURED trap, and two of them would have been decided wrong from the
abbreviation alone:

    passing.tds   = "PassingTouchdowns"        passing.td   = "Touchdown Percentage"
    passing.ints  = "Interceptions"            passing.int  = "Interception Percentage"

`td` and `int` are counts everywhere else in this program. Here they are rates.

12,643 dictionary rows across both datasets resolve to 95 + 8 (table_tag, column) pairs
with ZERO multi-label conflicts -- the same column key never carries two different
published titles, which is what makes the dictionary usable as a decision input rather
than a hint.

THREE THINGS ARE ESCALATED RATHER THAN GUESSED, each with the measurement that stopped
the guess. See ESCALATED below; `defense_and_fumbles.tackle` in particular is the same
label question O.9.0 raised for NFL.com, and it fails the obvious identity here:
tackle = solo + ast holds on only 9,483 of 27,423 rows where all three are present.

Run:  python -m scripts.sota_recon.statscrew_column_adjudication [--apply]
"""

from __future__ import annotations

import argparse
import glob
import json
import os

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger
from .sources import DATA_LAKE

GENERATOR = "scripts.sota_recon.statscrew_column_adjudication"

DICTIONARY_GLOB = os.path.join(
    DATA_LAKE, "ff_assets", "statscrew", "*", "*", "shards", "shard-*",
    "COLUMN_DICTIONARY.jsonl")

SOURCES = {
    "team_season_stats": "statscrew_team_season_stats",
    "team_season_results": "statscrew_team_season_results",
}

# ---------------------------------------------------------------------------
# (table_tag, column) -> canonical. Keyed on the TAG as well as the column because the
# same column key means different things per table: `no` is interceptions, kick returns,
# punt returns, punts, receptions, rush attempts and sacks depending on which of the
# eleven captioned tables it sits under. A column-only table would collapse all seven.
# ---------------------------------------------------------------------------
MAPPED: dict[tuple[str, str], str] = {
    # -- defense_and_fumbles: the pre-1994 IDP surface this source was captured for ----
    # `brup` (Pass Breakups, 1921-2025) and `pd` (Passes Defensed, 2013-2021) are the
    # same concept in two site spellings. MEASURED before mapping both to one canonical:
    # they co-occur on 214 rows, ALL of them `Totals` rows where brup is the literal
    # string '0' and pd carries the real count -- a placeholder, not a second
    # measurement, so no apply-time double count is possible.
    ("defense_and_fumbles", "brup"): "def_pass_defended",
    ("defense_and_fumbles", "pd"): "def_pass_defended",
    ("defense_and_fumbles", "fum"): "fumbles",
    ("defense_and_fumbles", "fyds"): "fumble_recovery_yards",
    ("defense_and_fumbles", "ftd"): "fum_ret_td",
    ("defense_and_fumbles", "ff"): "def_fumbles_forced",
    ("defense_and_fumbles", "solo"): "def_tackles_solo",
    ("defense_and_fumbles", "ast"): "def_tackle_assists",
    ("defense_and_fumbles", "tfl"): "def_tackles_for_loss",
    ("defense_and_fumbles", "tfly"): "def_tackles_for_loss_yards",
    ("defense_and_fumbles", "qbh"): "def_qb_hits",
    # -- interceptions ----------------------------------------------------------------
    ("interceptions", "no"): "def_interceptions",
    ("interceptions", "yds"): "def_interception_yards",
    ("interceptions", "tds"): "def_int_ret_td",
    # -- returns ----------------------------------------------------------------------
    ("kick_returns", "no"): "kickoff_returns",
    ("kick_returns", "yds"): "kickoff_return_yards",
    ("kick_returns", "long"): "kickoff_return_long",
    ("kick_returns", "tds"): "kickoff_return_tds",
    ("punt_returns", "no"): "punt_returns",
    ("punt_returns", "yds"): "punt_return_yards",
    ("punt_returns", "long"): "punt_return_long",
    ("punt_returns", "tds"): "punt_return_tds",
    # -- punting ----------------------------------------------------------------------
    ("punting", "no"): "punts",
    ("punting", "yds"): "punt_yards",
    ("punting", "avg"): "punt_yards_per_punt",
    ("punting", "long"): "punt_long",
    # -- receiving / rushing ----------------------------------------------------------
    ("receiving", "no"): "receptions",
    ("receiving", "yds"): "receiving_yards",
    ("receiving", "avg"): "receiving_yards_per_reception",
    ("receiving", "long"): "receiving_long",
    ("receiving", "tds"): "receiving_tds",
    ("rushing", "no"): "carries",
    ("rushing", "yds"): "rushing_yards",
    ("rushing", "avg"): "rushing_yards_per_carry",
    ("rushing", "long"): "rushing_long",
    ("rushing", "tds"): "rushing_tds",
    # -- sacks ------------------------------------------------------------------------
    ("sacks", "no"): "def_sacks",
    ("sacks", "yds"): "def_sack_yards",
    # -- passing. THE TRAP LIVES HERE: `tds`/`ints` are counts, `td`/`int` are rates ----
    ("passing", "yds"): "passing_yards",
    ("passing", "long"): "passing_long",
    ("passing", "tds"): "passing_tds",
    ("passing", "ints"): "passing_interceptions",
    ("passing", "td"): "passing_td_pct",
    ("passing", "int"): "passing_int_pct",
    ("passing", "att"): "attempts",
    ("passing", "comp"): "completions",
    ("passing", "comp_2"): "completion_pct",
    ("passing", "yds_att"): "passing_yards_per_attempt",
    ("passing", "rating"): "passer_rating",
    ("passing", "sacked"): "sacks_suffered",
    ("passing", "yds_lost"): "sack_yards_lost",
    # -- kicking ----------------------------------------------------------------------
    ("kicking", "x_ca"): "pat_att",
    ("kicking", "x_cm"): "pat_made",
    ("kicking", "x_cp"): "pat_pct",
    ("kicking", "fga"): "fg_att",
    ("kicking", "fgm"): "fg_made",
    ("kicking", "fg"): "fg_pct",
    # -- total_scoring: a SCORING table, so every column is a touchdown count -----------
    ("total_scoring", "fg"): "fg_made",
    ("total_scoring", "fum"): "fum_ret_td",
    ("total_scoring", "rush"): "rushing_tds",
    ("total_scoring", "rec"): "receiving_tds",
    ("total_scoring", "punt"): "punt_return_tds",
    ("total_scoring", "kick"): "kickoff_return_tds",
    ("total_scoring", "int"): "def_int_ret_td",
    ("total_scoring", "saf"): "def_safeties",
    ("total_scoring", "points"): "total_points_scored",
    # Published as "Extra Points, Converts, Action Points, XFL PATs" -- a UNION of four
    # league-specific names, which is why the first pass filed it as a candidate pending
    # a split. MEASURED (2026-07-28), the split is unnecessary IN OUR SCOPE: the capture
    # is NFL-only (49 team codes, /football/stats/t-{NFL TEAM}/), so every value is an
    # NFL extra point. 3,789 non-zero rows, 1921-2025.
    # WHICH extra-point column is a DISCRIMINATING test, not a guess: against the kicking
    # table's own x_cm ("Extra Points/Converts Made") it agrees 3,798 of 3,798 (100%);
    # against x_ca (ATTEMPTED, the rival reading) only 1,665 of 3,576 (47%).
    ("total_scoring", "x_c"): "pat_made",
    # -- team_season_results ----------------------------------------------------------
    ("results", "date"): "game_date",
    ("results", "res"): "result",
    ("results", "col_6"): "is_overtime",
    ("results", "row_class"): "season_type",
    # -- roster -----------------------------------------------------------------------
    ("*", "position"): "position",
}

NEW_CANDIDATES: dict[tuple[str, str], str] = {
    ("interceptions", "long"): "longest interception return ('Interception Return "
                               "Long'). v26 has punt_return_long and "
                               "kickoff_return_long but no equivalent for the "
                               "interception family -- the same gap the nflcom pass "
                               "found independently, which is what a gap rather than a "
                               "parse artefact looks like",
    ("kicking", "yds"): "'Kickoff Yards' -- the kicking side of the kickoff. v26 "
                        "carries kickoff RETURN yards only",
    ("kicking", "avg"): "'Kickoff Average'. No v26 column; and StatsCrew publishes it "
                        "beside Kickoff Yards and Kickoffs, so it is a checkable "
                        "aggregate rather than an orphan rate",
    ("kicking", "long"): "'Kickoff Long'. No v26 column",
    ("kicking", "ko"): "'Kickoffs' taken. No v26 column",
    ("kicking", "pts"): "'Kicking Points' -- points scored by the kicker (FG*3 + PAT). "
                        "Candidate canonical column: kicking_points",
    ("total_scoring", "mfg"): "'Missed Field Goal Return Touchdowns'. No v26 column; a "
                              "genuine scoring lane we do not carry",
    ("total_scoring", "other"): "'Other Touchdowns' -- the site's residual scoring "
                                "bucket. v26 has no residual, so a season whose total "
                                "does not close without it cannot currently be "
                                "reconciled",
    ("total_scoring", "2pt"): "'Two-Point Conversions' as a player TOTAL. v26 carries "
                              "passing_/rushing_/receiving_2pt_conversions separately "
                              "but no total, so this is not a duplicate of any single "
                              "one of them",
    ("results", "col_7"): "THE SECOND BLANK HEADER, and it carries real material: 1,035 "
                          "games hold either a playoff round name ('AFC Divisional "
                          "Playoff', 'NFL Championship') or a NEUTRAL-SITE city "
                          "('Milwaukee'). v26 has neither a round name (season_type is "
                          "REG/POST only) nor a venue. It is a MIXED cell and must be "
                          "split at admission, so it is a candidate rather than a "
                          "mapping. The pre-fix parser dropped this column entirely",
}

# Derived rates whose operands the SAME table publishes -- checked, not asserted.
DERIVED_RATIOS: dict[tuple[str, str], tuple[str, str]] = {
    # The site's Average is yards per event, not events per yard. The old
    # denominator direction made a clean-looking but inverted witness.
    ("interceptions", "avg"): ("yds", "no"),
    ("kick_returns", "avg"): ("yds", "no"),
    ("punt_returns", "avg"): ("yds", "no"),
    ("kicking", "avg"): ("yds", "ko"),
}

# A derived source column is still a witness even when it is not admitted as a
# new stored super-table column.  Keep the source operands and their canonical
# lower-layer counterparts explicit so the ratio can audit the mapping.  The
# first four have no canonical rate column, so `canonical` is intentionally
# None: they witness the numerator/denominator columns, not a duplicate rate.
DERIVED_WITNESSES: dict[tuple[str, str], dict[str, object]] = {
    ("interceptions", "avg"): {
        "canonical": None,
        "numerator": "def_interception_yards",
        "denominator": "def_interceptions",
    },
    ("kick_returns", "avg"): {
        "canonical": None,
        "numerator": "kickoff_return_yards",
        "denominator": "kickoff_returns",
    },
    ("punt_returns", "avg"): {
        "canonical": None,
        "numerator": "punt_return_yards",
        "denominator": "punt_returns",
    },
    ("kicking", "avg"): {
        "canonical": None,
        "numerator": None,
        "denominator": None,
        "note": "kicking.yds and kicking.ko are not canonical v26 columns yet",
    },
}

# These rates already have canonical rate columns, but the rate itself is also
# useful evidence that the two lower-layer counters are mapped correctly.
MAPPED_RATE_OPERANDS: dict[tuple[str, str], tuple[str, str]] = {
    ("punting", "avg"): ("punt_yards", "punts"),
    ("receiving", "avg"): ("receiving_yards", "receptions"),
    ("rushing", "avg"): ("rushing_yards", "carries"),
}

EXCLUDED: dict[tuple[str, str], str] = {
    ("sacks", "avg"):
        "StatsCrew labels this 'Average Yards per Sack', but the captured sacks table has "
        "no material yds operand; retain the raw field as an unverified source witness, "
        "not a derived ratio.",
    ("results", "record"):
        "cumulative W-L-T context string, not a numeric rate; it must not be divided or "
        "used as a denominator equation.",
    # ---- NON-NFL LEAGUE CONCEPTS. Joe's domain ruling, 2026-07-28: "we don't need the
    # CFL stuff." StatsCrew renders every league from ONE table shape, so an NFL page
    # still carries the rouge columns -- and the measurement says the ruling costs us
    # nothing, which is the stronger reason to exclude and is why it was checked before
    # acting. Our capture is NFL-scoped already (49 team codes, all
    # /football/stats/t-{NFL TEAM}/), so these are non-NFL COLUMNS on NFL rows, not
    # non-NFL rows:
    #     kicking.kos       94 non-null,      0 NON-ZERO
    #     total_scoring.single  22,176 non-null, 0 NON-ZERO
    # Every value in our scope is a zero placeholder. There is no material here to
    # decline -- the ruling and the data agree, and if a future capture ever admits CFL
    # rows these become material again and the exclusion must be revisited WITH them.
    ("kicking", "kos"):
        "'Kickoff Singles' -- the rouge, a CFL scoring concept StatsCrew renders on every "
        "league's page from one shared table shape. DOMAIN RULING (Joe, 2026-07-28): the "
        "v26 subject is NFL-scoped and non-NFL-league concepts are out. MEASURED on our "
        "own rows before excluding: 94 non-null and ZERO non-zero across 168,390 rows, so "
        "the ruling discards nothing -- this column carries no material in NFL scope",
    ("total_scoring", "single"):
        "'Singles' (the rouge) -- same CFL concept, same shared table shape, same domain "
        "ruling. MEASURED: 22,176 non-null and ZERO non-zero. The column is present on "
        "every NFL scoring row as a placeholder and carries no value anywhere in our "
        "capture",
    ("*", "table_tag"): "OUR parser's normalisation of the page's <h2> caption. It is "
                        "the dossier's own MULTI_TABLE discriminator, not material the "
                        "site publishes -- the site publishes the caption, which is "
                        "kept as table_caption",
    ("*", "is_total_row"): "parser flag marking the page's own Totals row. A "
                           "row-selection predicate for aggregation lanes, not a "
                           "measured value",
    ("*", "game_team_ids"): "the StatsCrew team codes linked inside the row. It LOCATES "
                            "the row against the team key space; its obligation is the "
                            "team-code -> team_fid crosswalk lane, which is this "
                            "source's declared blocker",
}

ESCALATED: dict[tuple[str, str], str] = {
    ("defense_and_fumbles", "tackle"):
        "published as 'Total Tackles', and the obvious identity FAILS: "
        "tackle = solo + ast holds on only 9,483 of the 27,423 rows where all three are "
        "present (34.6%). So StatsCrew's total is not our def_tackles_combined "
        "definition, or the three columns are populated from different eras' "
        "conventions. This is the SAME question O.9.0 raised for NFL.com "
        "(TKL|AST|COMBINED|SOLO -- four tackle columns where two would do), reached "
        "independently from a second source. Mapping it would double-count or halve "
        "every historical tackle total. SETTLED BY: partitioning the identity test by "
        "era and by Totals-vs-player rows before choosing a canonical",
    ("defense_and_fumbles", "frec"):
        "published as 'Fumbles Recovered' with no indication whose fumble. v26 splits "
        "fumble_recovery_own from fumble_recovery_opp and guessing picks one of two real "
        "canonical columns -- the same ambiguity the nflcom DEF game log's bare `FR` "
        "raised. SETTLED BY: comparing against a source that does distinguish (the "
        "nflcom player_career block publishes OPP FR separately)",
    ("results", "game"):
        "a COMPOSITE cell: 'Pitcairn Quakers 0 at Canton Bulldogs 48' carries the "
        "opponent, both scores and the venue relation in one string. It maps to THREE "
        "canonical facts (opponent_code, team_points, opponent_points) plus is_home, so "
        "no single-column disposition in the vocabulary fits it -- MAPPED_TO_CANONICAL "
        "names one target, EXCLUDED would discard the only place the scores appear, and "
        "NEW would propose storing the string. SETTLED BY: a parse into its "
        "constituents, each of which then gets its own row. This is a genuine limit of "
        "a one-row-per-column dossier and is recorded rather than forced",
    ("results", "home"):
        "cumulative HOME record after each game. Recomputable in principle, but only "
        "once the venue is known -- and the venue lives inside the composite `game` "
        "cell above, which is itself escalated. Blocked on that parse, not independent",
    ("results", "road"):
        "cumulative ROAD record after each game; blocked on the same `game`-cell parse "
        "as `home`",
}


def load_dictionary() -> dict[tuple[str, str, str], dict]:
    """(dataset, table_tag, column) -> the site's published label and title.

    The results dataset tags each table with the PAGE's caption
    ('1922_canton_bulldogs_game_by_game_results'), so it is normalised to the single tag
    the records carry. Conflicting titles for one key are raised, never merged."""
    entries: dict[tuple[str, str, str], dict] = {}
    conflicts: list[str] = []
    for path in glob.glob(DICTIONARY_GLOB):
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                dataset = row["dataset"]
                tag = row["table_tag"] if dataset == "team_season_stats" else "results"
                key = (dataset, tag, row["column_key"])
                published = {"label": row.get("source_label"),
                             "title": row.get("source_title"),
                             "position": row.get("column_position")}
                if key in entries and entries[key] != published:
                    conflicts.append(f"{key}: {entries[key]} vs {published}")
                entries[key] = published
    if conflicts:
        raise SystemExit("the source published two different names for one column:\n  "
                         + "\n  ".join(sorted(set(conflicts))[:20]))
    return entries


def _receipt(dataset: str, tag: str, column: str, published: dict | None) -> str:
    if published is None:
        return (f"MEASURED on the imported lake rows for {dataset}/{tag}/{column} -- "
                "this column is emitted by our parser and has no site-published header, "
                "so there is no COLUMN_DICTIONARY entry to cite")
    return (f"COLUMN_DICTIONARY.jsonl ({dataset}/{tag}): the site publishes column "
            f"{published['position']} as label {published['label']!r}, title "
            f"{published['title']!r}")


def build_decisions() -> tuple[list[dict], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    dictionary = load_dictionary()
    reverse = {value: key for key, value in SOURCES.items()}

    decisions: list[dict] = []
    tally = {"mapped": 0, "new": 0, "excluded": 0, "derived": 0,
             "escalated_left_open": 0, "unhandled_left_open": 0}
    unhandled: list[str] = []

    # OPEN rows, PLUS rows this generator itself already decided. Without the second
    # half a correspondence table can only ever be extended, never REVISED: a row it
    # closed last run is no longer OPEN, so a corrected entry silently does nothing and
    # the run reports "0 written" as if it had agreed with itself. Found 2026-07-28
    # revising three CFL-flavoured columns after a domain ruling.
    #
    # Rule-closed rows stay out deliberately -- the tables name specific (tag, column)
    # pairs, and sweeping in every plumbing column would fill `unhandled` with rows
    # nobody intended to adjudicate.
    from .column_dossier import load_decisions

    mine = {key for key, entry in load_decisions().items()
            if entry.get("generated_by") == GENERATOR}
    for row in dossier["rows"]:
        if row["lineage"] != "statscrew":
            continue
        if row["disposition"] != "OPEN" and row_key(
                row["source"], row["table_key"], row["column"]) not in mine:
            continue
        source, table_key, column = row["source"], row["table_key"], row["column"]
        dataset = reverse.get(source, "team_season_roster")
        tag = table_key if table_key != "*" else (
            "results" if dataset == "team_season_results" else "*")
        published = dictionary.get((dataset, tag, column))
        evidence = _receipt(dataset, tag, column, published)
        key = row_key(source, table_key, column)
        lookup = (tag, column)
        wildcard = ("*", column)

        if lookup in ESCALATED or wildcard in ESCALATED:
            tally["escalated_left_open"] += 1
            continue
        if lookup in DERIVED_RATIOS:
            numerator, denominator = DERIVED_RATIOS[lookup]
            witness = DERIVED_WITNESSES[lookup]
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "witness_kind": "DERIVED_WITNESS",
                "source_numerator": numerator,
                "source_denominator": denominator,
                "canonical_numerator": witness["numerator"],
                "canonical_denominator": witness["denominator"],
                "canonical": witness["canonical"],
                "reason": f"derived rate; its operands {numerator!r}/{denominator!r} are "
                          f"published in the SAME table, so the row carries no material "
                          f"they do not. Retain it as a derived witness against the "
                          f"canonical numerator/denominator, not as a stored cell",
                "evidence": evidence})
            tally["derived"] += 1
        elif lookup in MAPPED or wildcard in MAPPED:
            canonical = MAPPED.get(lookup) or MAPPED[wildcard]
            decision = {
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canonical,
                "reason": f"the site's published {(published or {}).get('title') or column!r} "
                          f"in the {tag!r} table; v26 carries the same material as "
                          f"{canonical!r}",
                "evidence": evidence}
            if lookup in MAPPED_RATE_OPERANDS:
                numerator, denominator = MAPPED_RATE_OPERANDS[lookup]
                decision.update({
                    "witness_kind": "DERIVED_WITNESS",
                    "source_numerator": "yds",
                    "source_denominator": "no",
                    "canonical_numerator": numerator,
                    "canonical_denominator": denominator,
                })
            if lookup == ("total_scoring", "points"):
                decision["candidate_role"] = "EXISTING_CANONICAL_VALIDATION"
                decision["candidate_formula"] = (
                    "6*(rush+rec+punt+kick+int+fum+mfg+other) + 3*fg + "
                    "x_c + single + 2*(saf+2pt)"
                )
            decisions.append(decision)
            tally["mapped"] += 1
        elif lookup in NEW_CANDIDATES:
            decisions.append({
                "key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                "reason": NEW_CANDIDATES[lookup], "evidence": evidence})
            tally["new"] += 1
        elif lookup in EXCLUDED or wildcard in EXCLUDED:
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": EXCLUDED.get(lookup) or EXCLUDED[wildcard],
                "evidence": evidence})
            tally["excluded"] += 1
        else:
            unhandled.append(f"{source}|{tag}|{column} "
                             f"(title={(published or {}).get('title')!r})")
            tally["unhandled_left_open"] += 1
    return decisions, tally | {"unhandled": sorted(unhandled)}


def escalated_row_keys() -> dict[str, str]:
    """Dossier row key -> the question that stopped the decision. Keyed the same way
    `build_decisions` keys its lookups, so the two cannot drift apart."""
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    reverse = {value: key for key, value in SOURCES.items()}
    out: dict[str, str] = {}
    for row in dossier["rows"]:
        if row["lineage"] != "statscrew" or row["disposition"] != "OPEN":
            continue
        source, table_key, column = row["source"], row["table_key"], row["column"]
        dataset = reverse.get(source, "team_season_roster")
        tag = table_key if table_key != "*" else (
            "results" if dataset == "team_season_results" else "*")
        question = ESCALATED.get((tag, column)) or ESCALATED.get(("*", column))
        if question:
            out[row_key(source, table_key, column)] = question
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    v26 = _v26_columns()
    from duckdb import connect

    connection = connect()
    try:
        team_games = {
            row[0] for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet('"
                + os.path.join(DATA_LAKE, "raw", "pfr", "boxscores",
                               "nfl_team_games_all.parquet").replace("\\", "/")
                + "')").fetchall()}
    finally:
        connection.close()
    # a results-family target lives in the team_games universe, not in v26
    known = v26 | team_games
    missing = sorted({canonical for canonical in MAPPED.values() if canonical not in known})
    if missing:
        print("CORRESPONDENCE TABLE INVALID -- canonical columns that do not exist:")
        for name in missing:
            print("  -", name)
        return 1

    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for name, value in tally.items():
        if name != "unhandled":
            print(f"  {name:24s} {value}")
    if tally["unhandled"]:
        print("\nUNHANDLED (left OPEN):")
        for name in tally["unhandled"]:
            print("   -", name)
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
