"""O.9.3 burn-down: adjudicate the nflcom splits + situational columns (1,287 dossier rows).

WHY THIS PASS EXISTS SEPARATELY from `nflcom_column_adjudication`. Those two families were
QUARANTINED by the COLUMN-SHIFT defect and were REPOINTED at the O.9.0b unshifted tables on
2026-07-28, which settled what each cell is CALLED. It did not settle which STAT FAMILY
each block is, and the 2026-07-28 handoff measured the obvious shortcut and refused it:
matching these column sets against O.9.0's RESOLVED gamelog blocks matched **0 of 8**. The
splits blocks carry genuinely different column sets. Reading them off the abbreviations is
exactly the move the StatsCrew pass proved wrong (`tds` is PassingTouchdowns, `td` is
Touchdown Percentage), so it is not the move here either.

TWO INDEPENDENT RECEIPTS, and this module refuses to run without both.

  1. THE SITE'S OWN NAME -- `docs/nflcom-splits-block-census.json`.
     A splits page nests <h3>Passing</h3> over <h3>Days</h3><table>, so NFL.com publishes
     the stat family for every table on every retained page. The census reads it by a
     STRUCTURAL rule that knows no vocabulary, then checks its own reading: the captions it
     derives must be exactly the `_table` values the parquet carries, and the section names
     must be disjoint from them. This is the source's own published name, which is the
     standard the program requires wherever a source publishes one.

  2. THE ARITHMETIC -- `docs/nflcom-splits-layout-identity.json`.
     Each rate column is scored against EVERY ordered pair of other columns in its own
     layout, so the receipt is the MARGIN, not a one-sided "it matches". It fixes the
     relationship BETWEEN columns (which is the count, which the yardage) without knowing
     the family; the <h3> fixes the family without knowing the arithmetic. Neither alone
     licenses a mapping.

Every declaration below is CHECKED against those two at run time (`_validate`). A block
whose census section disagrees with the table, or a derived ratio whose operands the
identity sweep did not confirm, fails the run rather than being written.

WHAT IS ESCALATED RATHER THAN GUESSED -- each with the measurement that stopped the guess:

  * THE WHOLE OF L4. `RET|YDS|AVG|LNG|TD|20+|40+|FC` is ONE header set that NFL.com renders
    under BOTH <h3>Kick Return</h3> and <h3>Punt Return</h3>. Measured in the census, not
    inferred. The un-shift keys a row's layout by its COLUMN SET, so the stored table cannot
    tell the two apart, and every stat-bearing column in L4 maps to a different canonical
    depending on which it is. This is a real residue of the parse, at the same grain as the
    column shift and much smaller; it is recorded, not averaged.
  * `total` in the DEFENSE block -- the tackle-total question, now raised by a THIRD
    independent source.
  * The L7 field-goal DISTANCE BUCKETS -- composite `made-att` cells, the second instance of
    the vocabulary gap StatsCrew's `results.game` opened.

Run:  python -m scripts.sota_recon.nflcom_splits_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.nflcom_splits_column_adjudication"

ROOT = Path(__file__).resolve().parents[2]
BLOCK_CENSUS = ROOT / "docs" / "nflcom-splits-block-census.json"
LAYOUT_IDENTITY = ROOT / "docs" / "nflcom-splits-layout-identity.json"
SIGNATURE_CENSUS = ROOT / "docs" / "nflcom-column-signature-census.json"

SOURCES = {"nflcom_player_splits": "player_splits",
           "nflcom_player_situational": "player_situational"}

# ---------------------------------------------------------------------------
# THE BLOCK DECLARATION. Suffix -> the section name NFL.com's own <h3> carries.
# Asserted here so the census can CONTRADICT it: `_validate` requires the census to have
# RESOLVED every one of these to exactly this name, in BOTH families independently. L4 is
# declared None because the census measured it AMBIGUOUS, and a declaration of None that
# the census resolved would fail just as loudly as the reverse.
# ---------------------------------------------------------------------------
BLOCKS: dict[str, str | None] = {
    "L0": "Defense",
    "L1": "Fumbles",
    "L2": "Receiving",
    "L3": "Rushing",
    "L4": None,          # Kick Return AND Punt Return share this header set
    "L5": "Passing",
    "L6": "Punting",
    "L7": "Kicking",
}

# ---------------------------------------------------------------------------
# (layout suffix, column) -> canonical v26 column. Keyed on the SUFFIX, not the full
# layout name, because the census measured splits_Lx and situational_Lx to carry the same
# section name independently -- so one argument covers both families and cannot drift
# between them. "*" applies to every layout.
# ---------------------------------------------------------------------------
MAPPED: dict[tuple[str, str], str] = {
    # -- L0 DEFENSE. Published: G|Total|Solo|AST|SCK|SFTY|PDEF|INT|TDS|YDS|AVG|LNG(lost).
    # TDS/YDS/AVG sit after INT, and the rival reading -- that they are ALL defensive
    # touchdowns and yards, fumble returns included -- is the one that would quietly wreck
    # every historical defensive total. It is REFUSED on three measurements, not on the
    # column ordering:
    #   avg = yds/int on 99.94% / 99.92% of non-degenerate rows, against 54.9% / 57.6%
    #         for the next best pair of the 90 scored (identity sweep)
    #   tds > int on 45 of 1,650,177 and 41 of 1,065,212 rows (0.003%) -- a TD count that
    #         included fumble returns could not stay bounded by interceptions
    #   yds <> 0 while int = 0 on 538 of 1,548,905 and 481 of 968,818 such rows (0.03%)
    # so TDS/YDS/AVG are the INTERCEPTION-return group.
    ("L0", "solo"): "def_tackles_solo",
    ("L0", "ast"): "def_tackle_assists",
    ("L0", "sck"): "def_sacks",
    ("L0", "sfty"): "def_safeties",
    ("L0", "pdef"): "def_pass_defended",
    ("L0", "int"): "def_interceptions",
    ("L0", "tds"): "def_int_ret_td",
    ("L0", "yds"): "def_interception_yards",
    # -- L1 FUMBLES. Published: G|FUM|LOST|FF|OWN FR|OPP FR|TD(lost). No rate column, so
    # there is no arithmetic identity to have -- the <h3> and the site's own unusually
    # explicit headers are the whole evidence, and they are explicit precisely where the
    # program's open whose-fumble question is not: this source SPLITS own from opponent.
    ("L1", "fum"): "fumbles",
    ("L1", "lost"): "fumbles_lost",
    ("L1", "ff"): "def_fumbles_forced",
    ("L1", "own_fr"): "fumble_recovery_own",
    ("L1", "opp_fr"): "fumble_recovery_opp",
    # -- L2 RECEIVING. Published: G|REC|YDS|AVG|LNG|TD|1ST|1ST%|20+|40+(lost).
    ("L2", "rec"): "receptions",
    ("L2", "yds"): "receiving_yards",
    ("L2", "lng"): "receiving_long",
    ("L2", "td"): "receiving_tds",
    ("L2", "1st"): "receiving_first_downs",
    # `20+` is a CUMULATIVE threshold, and which v26 column that is was MEASURED rather
    # than read off the name: if rec_explosive_20 is "receptions of 20+ yards" then every
    # 40+ reception is also one, so receptions_40plus <= rec_explosive_20 must hold. It
    # does on 731,072 of 731,075 rows (3 violations, 0.0004%). Under the rival reading --
    # a 20-29 yard BUCKET -- that containment would fail routinely, and the separate
    # bucket column receptions_20_29 exceeds rec_explosive_20 on 12 rows, which is what a
    # genuinely different column looks like.
    ("L2", "20"): "rec_explosive_20",
    # -- L3 RUSHING. Published: G|ATT|YDS|AVG|LNG|TD|1ST(lost).
    ("L3", "att"): "carries",
    ("L3", "yds"): "rushing_yards",
    ("L3", "lng"): "rushing_long",
    ("L3", "td"): "rushing_tds",
    # L4's page axis is lost, but YDS has an existing combined canonical. The weekly
    # subject identity is measured in the player_splits and situational receipts:
    # total_return_yards = kickoff_return_yards + punt_return_yards.
    ("L4", "yds"): "total_return_yards",
    # -- L5 PASSING. Published: G|ATT|COMP|PCT|YDS|AVG|LNG|TD|INT|1ST|1ST%|20+|SCK|SCKY|
    #    RATE(lost).
    ("L5", "att"): "attempts",
    ("L5", "comp"): "completions",
    ("L5", "yds"): "passing_yards",
    ("L5", "lng"): "passing_long",
    ("L5", "td"): "passing_tds",
    ("L5", "int"): "passing_interceptions",
    ("L5", "1st"): "passing_first_downs",
    # same containment measurement as the receiving 20+, and it is CLEANER on this side:
    # completions_40plus <= pass_explosive_20 on all 731,116 rows, zero violations.
    ("L5", "20"): "pass_explosive_20",
    ("L5", "sck"): "sacks_suffered",
    ("L5", "scky"): "sack_yards_lost",
    # -- L6 PUNTING. Published: G|PUNTS|YDS|LNG|AVG|BLK|RET|RETY|IN 20|NET AVG(lost).
    ("L6", "punts"): "punts",
    ("L6", "yds"): "punt_yards",
    ("L6", "lng"): "punt_long",
    ("L6", "blk"): "punts_blocked",
    # -- L7 KICKING (field goals). Published: G|1-19|20-29|30-39|40-49|50-59|60+|FGM|FG
    #    ATT|PCT(lost). Only the two totals are single-valued; the buckets are escalated.
    ("L7", "fgm"): "fg_made",
    ("L7", "fg_att"): "fg_att",
}

# Real material v26 does not carry. §24.4: these route through the census + demand join.
NEW_CANDIDATES: dict[tuple[str, str], str] = {
    # RET/RETY sit INSIDE the punting block, so they are returns ALLOWED on this punter's
    # punts, not returns he made. Mapping them to punt_returns / punt_return_yards would
    # credit a punter with the returner's production -- a real hazard, since O.9.0's
    # gamelog pass legitimately maps those same names from a RETURNS block. The reading is
    # MEASURED, not assumed: a punter cannot have more returns against him than he had
    # punts, and ret <= punts holds on 71,813 of 71,816 splits rows (99.996%) and 39,777
    # of 39,777 situational rows. v26 carries no punter-side allowed columns at all.
    ("L6", "ret"): "punt returns ALLOWED on this punter's punts (the site's RET inside "
                   "the Punting block). v26 has punt_returns for the RETURNER only and no "
                   "punter-side coverage column; mapping it to punt_returns would credit "
                   "the punter with the returner's work",
    ("L6", "rety"): "punt return yards ALLOWED on this punter's punts. Same argument as "
                    "RET, and it is the operand the site's own NET AVG is computed from",
    ("L6", "in_20"): "punts placed inside the 20. No v26 column, and it is the standard "
                     "punting placement statistic -- proposed independently by the O.9.0 "
                     "gamelog pass (`punts_in_20`), which is what a genuine schema gap "
                     "reached from two directions looks like",
}

# Derived rates whose OPERANDS the same layout publishes. Excluded because the row carries
# no material the operands do not -- an equation check, not a stored cell. The operand
# claim is not asserted: every entry must be CONFIRMED by the identity sweep at RESOLVED
# status before this module will write anything (see `_validate`).
DERIVED_RATIOS: dict[tuple[str, str], tuple[str, str]] = {
    ("L0", "avg"): ("yds", "int"),
    ("L2", "avg"): ("yds", "rec"),
    ("L2", "1st_2"): ("1st", "rec"),
    ("L3", "avg"): ("yds", "att"),
    ("L5", "avg"): ("yds", "att"),
    ("L5", "pct"): ("comp", "att"),
    ("L5", "1st_2"): ("1st", "att"),
    ("L6", "avg"): ("yds", "punts"),
    # L4's family is unknown, but the ARITHMETIC is not: avg = yds/ret is the same equation
    # whether the block is kick returns or punt returns, so this one column is decidable
    # while the rest of its layout is escalated.
    ("L4", "avg"): ("yds", "ret"),
}

EXCLUDED: dict[tuple[str, str], str] = {
    ("*", "g"): "a games COUNT for the split. The v26 subject is player-WEEK grain and has "
                "no such column by construction; its obligation is the presence lane "
                "(pfr_games_played / appearance witnesses). This is the same argument the "
                "O.9.0 gamelog pass made for `games`, and being a per-split count rather "
                "than a season count does not change it -- it is still a count OF weeks, "
                "on a subject whose row IS a week",
    ("*", "split_value"): "the split LABEL -- 'Sundays', 'vs AFC Teams', 'Chicago Bears', "
                          "'Trailing by 1-8'. It LOCATES the row on its split axis rather "
                          "than witnessing a stat, and the axis it names (`_table`) is "
                          "already excluded as the table discriminator. Its obligation is "
                          "the split-axis crosswalk lane: the opponent labels are team "
                          "codes and the stadium labels are venues, both of which must be "
                          "resolved before these tables can serve as the conservation "
                          "check they were captured for",
    ("*", "_view"): "our harvester's name for the PAGE VIEW ('splits' / 'situational'), "
                    "constant within each source. It is the dossier's own source "
                    "discriminator, not material NFL.com publishes",
    ("*", "_lost_column"): "our REPAIR's declaration of which column the old parser "
                           "truncated away on this row, one of 1st/40/fum/lng/net_avg/pct/"
                           "rate/td. It is provenance about a known loss -- deliberately "
                           "kept so the NULLs are declared rather than silent -- and it "
                           "witnesses nothing about the player",
}

# ---------------------------------------------------------------------------
# ESCALATED -- left OPEN, each with the measurement that stopped the guess and the thing
# that would settle it.
# ---------------------------------------------------------------------------
_L4_ESCALATION = (
    "THE L4 BLOCK IS AMBIGUOUS AT THE SOURCE. NFL.com renders the header set "
    "RET|YDS|AVG|LNG|TD|20+|40+|FC under BOTH <h3>Kick Return</h3> and <h3>Punt Return</h3> "
    "-- MEASURED in docs/nflcom-splits-block-census.json, where L4 is the only layout of "
    "sixteen carrying two section names. The O.9.0b un-shift keys a row's layout by its "
    "COLUMN SET, which is what made the repair invertible, and that same property means "
    "the stored table CANNOT distinguish the two blocks. {column!r} maps to a different "
    "canonical under each ({kick!r} vs {punt!r}), so a mapping here would silently file "
    "kick returns as punt returns for an unknown share of 811,572 rows. SETTLED BY: "
    "recovering the section per row -- either by re-deriving layout from the cached HTML "
    "where the <h3> is present (~12% of pages), or by testing whether the original "
    "player_splits table preserved page order well enough that a player-season's two L4 "
    "blocks are separable by position. Neither is a guess and both are measurable; "
    "choosing between them is a slice, not an improvisation")

_L4_PAIRS = {
    "ret": ("kickoff_returns", "punt_returns"),
    "yds": ("kickoff_return_yards", "punt_return_yards"),
    "lng": ("kickoff_return_long", "punt_return_long"),
    "td": ("kickoff_return_tds", "punt_return_tds"),
    "20": ("kick returns of 20+ yards", "punt returns of 20+ yards"),
    "40": ("kick returns of 40+ yards", "punt returns of 40+ yards"),
    "fc": ("fair catches on kickoffs", "fair catches on punts"),
}

ESCALATED: dict[tuple[str, str], str] = {
    ("L0", "total"):
        "published as 'Total' at the head of the DEFENSE block, and it is the tackle-total "
        "question for the THIRD time from a THIRD independent lineage -- O.9.0 raised it "
        "for the nflcom player_career block (TKL|AST|COMBINED|SOLO, four tackle columns "
        "where two would do) and the StatsCrew pass raised it again from that site's own "
        "'Total Tackles', where tackle = solo + ast holds on only 34.6% of rows. Three "
        "sources asking the same question independently is evidence about the CONCEPT, "
        "not about any one parse. Mapping it to def_tackles_combined would double-count or "
        "halve every historical tackle total. SETTLED BY the escalation already open: "
        "partition the solo+ast identity by era and by row kind before choosing a "
        "canonical, then apply the answer to all three sources at once",
}

# The L7 distance buckets: ONE cell holding TWO facts.
_BUCKET_ESCALATION = (
    "a COMPOSITE cell. MEASURED: 100% of the {n:,} non-null values in this column match "
    "^\\d+-\\d+$ ('3-3', '0-1') -- it is MADE-ATTEMPTED in one cell, and v26 carries both "
    "halves separately as fg_made_{bucket} and fg_missed_{bucket}. No disposition in the "
    "vocabulary fits: MAPPED_TO_CANONICAL names ONE target and would drop the attempts, "
    "EXCLUDED_WITH_REASON would discard the only place NFL.com publishes per-distance "
    "field goals, and NEW_SUPERTABLE_COLUMN_CANDIDATE would propose storing the string. "
    "This is the SECOND independent instance of the gap StatsCrew's `results.game` opened "
    "('Pitcairn Quakers 0 at Canton Bulldogs 48'), and two instances from two lineages "
    "make it a vocabulary question rather than a quirk of one source. SETTLED BY: a "
    "split-at-admission disposition that names more than one target, after which each "
    "half gets its own row")

_BUCKETS = {"1_19": "0_19", "20_29": "20_29", "30_39": "30_39",
            "40_49": "40_49", "50_59": "50_59", "60": "60_"}
# measured non-null counts per family, for the receipt text (splits + situational)
_BUCKET_ROWS = 78641 + 41353


def _load(path: Path, what: str) -> dict:
    if not path.exists():
        raise SystemExit(
            f"{path.name} is missing -- {what}. This module does not decide anything "
            "without it: an adjudication needs evidence, and the evidence for these "
            "layouts is measured, not remembered. Build it first.")
    return json.loads(path.read_text(encoding="utf-8"))


def _validate(v26: set[str]) -> list[str]:
    """Every declaration above must survive contact with the two receipts."""
    problems: list[str] = []
    census = _load(BLOCK_CENSUS, "the site's own <h3> block names")
    identity = _load(LAYOUT_IDENTITY, "the arithmetic layout identities")

    # 1. the canonical targets must exist
    problems += [f"{key}: canonical {canonical!r} is not a v26 column"
                 for key, canonical in MAPPED.items() if canonical not in v26]

    # 2. one column, one bucket
    buckets = [MAPPED, NEW_CANDIDATES, dict.fromkeys(DERIVED_RATIOS), EXCLUDED, ESCALATED]
    seen: dict[tuple[str, str], int] = {}
    for index, bucket in enumerate(buckets):
        for key in bucket:
            if key in seen:
                problems.append(f"{key}: declared in two buckets at once "
                                f"({seen[key]} and {index})")
            seen[key] = index

    # 3. THE BLOCK DECLARATION vs THE SITE'S OWN NAME, per family, independently
    for suffix, declared in BLOCKS.items():
        for family in SOURCES.values():
            record = census["layouts"].get(f"{family}_{suffix}")
            if record is None:
                problems.append(f"{family}_{suffix}: the block census never observed this "
                                f"layout, so its identity is undeclared, not decided")
                continue
            if declared is None:
                if record["status"] != "AMBIGUOUS":
                    problems.append(
                        f"{family}_{suffix}: declared AMBIGUOUS but the census RESOLVED it "
                        f"to {record['section']!r} -- the escalation is now wrong and the "
                        f"columns are decidable")
                continue
            if record["status"] != "RESOLVED":
                problems.append(f"{family}_{suffix}: declared {declared!r} but the census "
                                f"status is {record['status']} {record['sections']}")
            elif record["section"] != declared:
                problems.append(f"{family}_{suffix}: declared {declared!r}, the site "
                                f"publishes {record['section']!r}")

    # 4. every column this pass DECIDES must be one the site publishes a name for. The
    #    only decidable columns without one are our own parser's emissions, which are
    #    excluded on exactly that ground.
    headers = published_headers()
    for bucket, label in ((MAPPED, "MAPPED"), (NEW_CANDIDATES, "NEW"),
                          (dict.fromkeys(DERIVED_RATIOS), "DERIVED")):
        for suffix, column in bucket:
            if (suffix, column) not in headers:
                problems.append(
                    f"{label} {suffix}.{column}: the source publishes no header for this "
                    "column, so there is no source-published name to cite as evidence")

    # 5. EVERY derived ratio must be CONFIRMED by the arithmetic sweep, in both families
    for (suffix, column), (numerator, denominator) in DERIVED_RATIOS.items():
        for family in SOURCES.values():
            record = ((identity["layouts"].get(f"{family}_{suffix}") or {})
                      .get("identities", {}).get(column))
            if record is None:
                problems.append(f"{family}_{suffix}.{column}: declared a derived ratio but "
                                f"the identity sweep produced no result for it")
                continue
            if record.get("status") != "RESOLVED":
                problems.append(f"{family}_{suffix}.{column}: identity sweep status "
                                f"{record.get('status')}, not RESOLVED")
                continue
            best = record["best"]
            if (best["numerator"], best["denominator"]) != (numerator, denominator):
                problems.append(
                    f"{family}_{suffix}.{column}: declared {numerator}/{denominator}, the "
                    f"sweep resolves {best['numerator']}/{best['denominator']} at "
                    f"{best['pct']}%")
    return problems


def published_headers() -> dict[tuple[str, str], str]:
    """(layout suffix, stored column key) -> the RAW header NFL.com printed above it.

    The stored key is our slug of the site's label, and the two differ in ways that matter
    to a reader of the ledger: `1st_2` is the site's '1st%', `20` is '20+', `in_20` is
    'IN 20', `1_19` is '1-19'. The law is that evidence cites the SOURCE'S own published
    name, so the raw string goes in the receipt rather than our normalisation of it."""
    census = json.loads(SIGNATURE_CENSUS.read_text(encoding="utf-8"))
    unshift = json.loads(
        (ROOT / "docs" / "nflcom-splits-unshift-receipt.json").read_text(encoding="utf-8"))
    suffix_of = {(family, tuple(entry["columns"])): entry["layout"].rsplit("_", 1)[-1]
                 for family, body in unshift["families"].items()
                 for entry in body["layouts"]}
    out: dict[tuple[str, str], str] = {}
    for family in SOURCES.values():
        for signature in census["families"][family]["signatures"]:
            suffix = suffix_of.get((family, tuple(signature["keys"])))
            if suffix is None:
                continue
            for key, header in zip(signature["keys"], signature["headers"]):
                out[(suffix, key)] = header
    return out


def _evidence(family: str, suffix: str, column: str, census: dict, identity: dict,
              headers: dict[tuple[str, str], str]) -> str:
    layout = f"{family}_{suffix}"
    record = census["layouts"].get(layout) or {}
    published = headers.get((suffix, column))
    head = (f"NFL.com publishes this column as {published!r} "
            if published else
            f"no site-published header (parser-emitted column {column!r}) ")
    if record.get("status") == "RESOLVED":
        head += (f"(docs/nflcom-column-signature-census.json). "
                 f"docs/nflcom-splits-block-census.json: NFL.com's own <h3> names the "
                 f"block {record['section']!r} on {record['tables']:,} sampled tables, "
                 f"with no competing section name")
    else:
        head += (f"(docs/nflcom-column-signature-census.json). "
                 f"docs/nflcom-splits-block-census.json: NFL.com renders this header set "
                 f"under {record.get('sections')}")
    ratio = ((identity["layouts"].get(layout) or {}).get("identities", {}).get(column))
    if ratio and ratio.get("status") == "RESOLVED":
        best, runner = ratio["best"], ratio.get("runner_up")
        head += (f". docs/nflcom-splits-layout-identity.json: {column} = "
                 f"{best['numerator']}/{best['denominator']} on {best['pct']}% of "
                 f"{best['eligible']:,} eligible rows")
        if runner:
            head += (f", against {runner['pct']}% for the next best pair of the "
                     f"{ratio['candidates_scored']} scored -- the margin is the receipt")
    return head


def build_decisions() -> tuple[list[dict], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    census = _load(BLOCK_CENSUS, "the site's own <h3> block names")
    identity = _load(LAYOUT_IDENTITY, "the arithmetic layout identities")

    headers = published_headers()

    from .column_dossier import load_decisions
    mine = {key for key, entry in load_decisions().items()
            if entry.get("generated_by") == GENERATOR}

    decisions: list[dict] = []
    tally = {"mapped": 0, "new": 0, "excluded": 0, "derived": 0,
             "escalated_left_open": 0, "unhandled_left_open": 0}
    unhandled: set[str] = set()

    for row in dossier["rows"]:
        family = SOURCES.get(row["source"])
        if family is None:
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        # OPEN rows plus this generator's OWN earlier decisions, so a corrected entry
        # actually revises rather than silently doing nothing
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        _, _, layout = row["table_key"].partition("|")
        suffix = layout.rsplit("_", 1)[-1]
        column = row["column"]
        lookup, wildcard = (suffix, column), ("*", column)
        evidence = _evidence(family, suffix, column, census, identity, headers)
        if lookup == ("L4", "yds"):
            evidence += (
                "; D:/league-history-data/nfl/derived/validation/"
                "sota_recon_master/nflcom_player_splits_witness.json and "
                "nflcom_player_situational_return_yards_witness.json: generated "
                "player-season candidate matrices measure the existing combined target "
                "`total_return_yards`; its weekly subject identity is "
                "`total_return_yards = kickoff_return_yards + punt_return_yards`. "
                "The page-axis ambiguity remains for the other L4 fields."
            )

        if lookup in ESCALATED:
            tally["escalated_left_open"] += 1
            continue
        if suffix == "L4" and column in _L4_PAIRS and lookup not in MAPPED:
            tally["escalated_left_open"] += 1
            continue
        if suffix == "L7" and column in _BUCKETS:
            tally["escalated_left_open"] += 1
            continue
        if lookup in DERIVED_RATIOS:
            numerator, denominator = DERIVED_RATIOS[lookup]
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"a derived rate. Its operands {numerator!r}/{denominator!r} are "
                          f"published in the SAME block and the identity sweep confirms "
                          f"the equation against every other ordered pair in the layout, "
                          f"so this row carries no material the operands do not. It is an "
                          f"equation check, not a stored cell",
                "evidence": evidence})
            tally["derived"] += 1
        elif lookup in MAPPED:
            canonical = MAPPED[lookup]
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canonical,
                "reason": f"the site's {headers.get((suffix, column), column)!r} column "
                          f"in its own {BLOCKS[suffix]!r} block; v26 carries the same "
                          f"material as {canonical!r}",
                "evidence": evidence})
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
            unhandled.add(f"{suffix}|{column}")
            tally["unhandled_left_open"] += 1

    return decisions, tally | {"unhandled": sorted(unhandled)}


def escalated_row_keys() -> dict[str, str]:
    """Dossier row key -> the question that stopped the decision.

    An escalated row and an unexamined row are both OPEN, and a queue that cannot tell
    them apart invites the next session to re-derive what this one refused on purpose.
    `column_escalation_census` rolls these up so the open count declares its own
    composition."""
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    from .composite_column_adjudication import owned_row_keys
    composite_owned = owned_row_keys()
    out: dict[str, str] = {}
    for row in dossier["rows"]:
        if row["source"] not in SOURCES or row["disposition"] != "OPEN":
            continue
        _, _, layout = row["table_key"].partition("|")
        suffix, column = layout.rsplit("_", 1)[-1], row["column"]
        key = row_key(row["source"], row["table_key"], column)
        if key in composite_owned:
            continue
        if (suffix, column) in ESCALATED:
            out[key] = ESCALATED[(suffix, column)]
        elif suffix == "L4" and column in _L4_PAIRS and (suffix, column) not in MAPPED:
            kick, punt = _L4_PAIRS[column]
            out[key] = _L4_ESCALATION.format(column=column, kick=kick, punt=punt)
        elif suffix == "L7" and column in _BUCKETS:
            out[key] = _BUCKET_ESCALATION.format(n=_BUCKET_ROWS, bucket=_BUCKETS[column])
    return out


def escalation_report() -> list[str]:
    """The questions this pass refused to answer, printed every run so they stay visible."""
    lines = [f"  L0.total -- {ESCALATED[('L0', 'total')][:88]}..."]
    for column, (kick, punt) in sorted(_L4_PAIRS.items()):
        if ("L4", column) in MAPPED:
            continue
        lines.append(f"  L4.{column} -- ambiguous block: {kick} vs {punt}")
    for column, bucket in sorted(_BUCKETS.items()):
        lines.append(f"  L7.{column} -- composite made-att cell -> fg_made_{bucket} + "
                     f"fg_missed_{bucket}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    problems = _validate(_v26_columns())
    if problems:
        print("CORRESPONDENCE TABLE INVALID -- the receipts contradict the declarations:")
        for problem in problems:
            print("  -", problem)
        return 1

    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for name, value in tally.items():
        if name != "unhandled":
            print(f"  {name:24s} {value}")
    print("\nESCALATED (left OPEN, with what would settle each):")
    for line in escalation_report():
        print(line)
    if tally["unhandled"]:
        print("\nUNHANDLED (left OPEN -- add to the table or escalate):")
        for name in tally["unhandled"]:
            print("   -", name)
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
