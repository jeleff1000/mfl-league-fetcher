"""O.9.3 burn-down: nflcom `team_stats` + `player_season` -- the NAMED families.

WHY THEY WERE LEFT. O.9.0 could RESOLVE the gamelog families because exactly one declared
layout regenerates each header signature, so the block boundaries were receipted. These two
came back NAMED instead: no layout reproduces them, and the handoffs recorded the evidence
as "an uppercased header string and nothing more".

That undersold it. An uppercased header string IS the source's own published name, which is
the standard this program requires everywhere a source publishes one -- it is the same
evidence class as StatsCrew's `<th title=>`. What was actually missing was the JOIN from a
dossier table to its signature. That join is now MEASURED and total: subtract the partition
columns the harvester adds (`_category`, `_side`, `_player_slug`, `season`, `season_type`)
and every one of the 54 dossier tables matches EXACTLY ONE signature key-set -- 32 of 32 for
team_stats, 22 of 22 for player_season, zero ambiguous, zero unmatched. So every column here
carries NFL.com's own header, and the correspondence table is keyed on
`(category, side, column)`.

**SIDE IS THE WHOLE BALLGAME, and it is not uniform.** `_side='defense'` means "what
happened against this team", so `passing|defense.yds` is passing yards ALLOWED and maps to
`passing_yds_allowed`, never to `passing_yards`. But the defense tables are NOT uniformly
allowed-stats: `int` and `sck` in that same block are interceptions and sacks the DEFENSE
MADE -- credits, not concessions. Mapping a whole side one way would file every defensive
takeaway as something conceded. Each column is decided on its own.

TWO MEASUREMENTS THAT CHANGED A DECISION:

1. `player_season.receiving` publishes `1st` with the header **'1st%'** and `rec_1st` with
   'Rec 1st'. The short key is the PERCENTAGE and the long key is the count -- the exact
   inversion StatsCrew's `td`/`tds` trap has, reached independently in a second source.
   Read off the key alone, `1st` becomes `receiving_first_downs` and is wrong by a factor of
   the reception count.

2. `receiving` and `passing` looked like they might republish each other. MEASURED on the
   same team-seasons: `rec == cmp` on 4,432/4,456 defense and 4,444/4,456 offense (99.5%),
   `td == td` on 4,454/4,456 (99.96%), and `pass_yds == rec_yds` on 4,442/4,456 (99.7%).
   They ARE the same quantity computed twice -- but they are NOT a duplicate to discard:
   v26 deliberately carries `completions` and `receptions` separately, and the R3 edge
   already cross-checks them. Each maps to its own canonical, and this run is an independent
   corroboration of that edge on a second lineage.
   (The first pass of that check compared raw VARCHARs and read 44.5% agreement on yards --
   a thousands-separator artifact, not a disagreement. Cast before concluding.)

Run:  python -m scripts.sota_recon.nflcom_category_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import (DOSSIER_PATH, NFLCOM_SEMANTICS, _v26_columns,
                                         apply_to_ledger)

GENERATOR = "scripts.sota_recon.nflcom_category_column_adjudication"

SOURCES = {"nflcom_team_stats": "team_stats", "nflcom_player_season": "player_season"}
# columns the harvester adds around the site's table; not part of any header signature
PARTITION = {"_category", "_side", "_player_slug", "season", "season_type"}

# ---------------------------------------------------------------------------
# (category, side, column) -> canonical. side is '' for player_season (no side axis).
# '*' as the side means "same meaning on every side this category has".
# ---------------------------------------------------------------------------
MAPPED: dict[tuple[str, str, str], str] = {
    # ---- PASSING. offense = the team's own passing; defense = passing ALLOWED --------
    ("passing", "offense", "att"): "attempts",
    ("passing", "offense", "cmp"): "completions",
    ("passing", "offense", "pass_yds"): "passing_yards",
    ("passing", "offense", "td"): "passing_tds",
    ("passing", "offense", "int"): "passing_interceptions",
    ("passing", "offense", "rate"): "passer_rating",
    ("passing", "offense", "lng"): "passing_long",
    ("passing", "offense", "1st"): "passing_first_downs",
    ("passing", "offense", "20"): "pass_explosive_20",
    ("passing", "offense", "40"): "completions_40plus",
    ("passing", "offense", "sck"): "sacks_suffered",
    ("passing", "offense", "scky"): "sack_yards_lost",
    ("passing", "", "att"): "attempts",
    ("passing", "", "cmp"): "completions",
    ("passing", "", "pass_yds"): "passing_yards",
    ("passing", "", "td"): "passing_tds",
    ("passing", "", "int"): "passing_interceptions",
    ("passing", "", "rate"): "passer_rating",
    ("passing", "", "lng"): "passing_long",
    ("passing", "", "1st"): "passing_first_downs",
    ("passing", "", "20"): "pass_explosive_20",
    ("passing", "", "40"): "completions_40plus",
    ("passing", "", "sck"): "sacks_suffered",
    ("passing", "", "scky"): "sack_yards_lost",
    # defense side: CONCESSIONS...
    ("passing", "defense", "cmp"): "def_completions_allowed",
    ("passing", "defense", "att"): "def_attempts_allowed",
    ("passing", "defense", "1st"): "passing_first_downs_allowed",
    ("passing", "defense", "yds"): "passing_yds_allowed",
    ("passing", "defense", "td"): "passing_tds_allowed",
    ("passing", "defense", "rate"): "def_passer_rating_allowed",
    ("passing", "defense", "20"): "def_explosive_pass_allowed",
    # ...and CREDITS in the same block. An interception is not something a defense allows.
    ("passing", "defense", "int"): "def_interceptions",
    ("passing", "defense", "sck"): "def_sacks",
    # ---- RUSHING --------------------------------------------------------------------
    ("rushing", "offense", "att"): "carries",
    ("rushing", "offense", "rush_yds"): "rushing_yards",
    ("rushing", "offense", "td"): "rushing_tds",
    ("rushing", "offense", "lng"): "rushing_long",
    ("rushing", "offense", "rush_1st"): "rushing_first_downs",
    ("rushing", "offense", "rush_fum"): "rushing_fumbles",
    ("rushing", "offense", "40"): "rushing_40plus",
    ("rushing", "", "att"): "carries",
    ("rushing", "", "rush_yds"): "rushing_yards",
    ("rushing", "", "td"): "rushing_tds",
    ("rushing", "", "lng"): "rushing_long",
    ("rushing", "", "rush_1st"): "rushing_first_downs",
    ("rushing", "", "rush_fum"): "rushing_fumbles",
    ("rushing", "", "40"): "rushing_40plus",
    ("rushing", "defense", "rush_yds"): "rushing_yds_allowed",
    ("rushing", "defense", "att"): "def_carries_allowed",
    ("rushing", "defense", "rush_1st"): "rushing_first_downs_allowed",
    ("rushing", "defense", "td"): "rushing_tds_allowed",
    # ---- RECEIVING ------------------------------------------------------------------
    ("receiving", "offense", "rec"): "receptions",
    ("receiving", "offense", "yds"): "receiving_yards",
    ("receiving", "offense", "td"): "receiving_tds",
    ("receiving", "offense", "lng"): "receiving_long",
    ("receiving", "offense", "rec_1st"): "receiving_first_downs",
    ("receiving", "offense", "rec_fum"): "receiving_fumbles",
    ("receiving", "offense", "20"): "rec_explosive_20",
    ("receiving", "offense", "40"): "receptions_40plus",
    ("receiving", "", "rec"): "receptions",
    ("receiving", "", "yds"): "receiving_yards",
    ("receiving", "", "td"): "receiving_tds",
    ("receiving", "", "lng"): "receiving_long",
    ("receiving", "", "rec_1st"): "receiving_first_downs",
    ("receiving", "", "rec_fum"): "receiving_fumbles",
    ("receiving", "", "20"): "rec_explosive_20",
    ("receiving", "", "40"): "receptions_40plus",
    ("receiving", "", "tgts"): "targets",
    ("receiving", "defense", "rec"): "def_completions_allowed",
    ("receiving", "defense", "rec_1st"): "receiving_first_downs_allowed",
    ("receiving", "defense", "yds"): "passing_yds_allowed",
    ("receiving", "defense", "td"): "receiving_tds_allowed",
    ("receiving", "defense", "pdef"): "def_pass_defended",
    ("receiving", "defense", "20"): "def_explosive_pass_allowed",
    # ---- DEFENSIVE / SCORING FAMILIES ------------------------------------------------
    ("tackles", "", "solo"): "def_tackles_solo",
    ("tackles", "", "asst"): "def_tackle_assists",
    ("tackles", "", "sck"): "def_sacks",
    ("interceptions", "", "int"): "def_interceptions",
    ("interceptions", "", "int_td"): "def_int_ret_td",
    ("interceptions", "", "int_yds"): "def_interception_yards",
    ("fumbles", "", "ff"): "def_fumbles_forced",
    ("fumbles", "", "fr_td"): "fum_ret_td",
    ("scoring", "defense", "fr_td"): "fum_ret_td",
    ("scoring", "defense", "int_td"): "def_int_ret_td",
    ("scoring", "defense", "sfty"): "def_safeties",
    ("scoring", "offense", "rec_td"): "receiving_tds",
    ("scoring", "offense", "rsh_td"): "rushing_tds",
    # Tot TD is the franchise's all-touchdown total, not merely rsh_td + rec_td.  The
    # conservation ladder closes the identity against total_tds_scored by contrast; residual
    # value defects remain a witness finding, not a reason to choose a different column.
    ("scoring", "offense", "tot_td"): "total_tds_scored",
    ("scoring", "special-teams", "fgm"): "fg_made",
    ("scoring", "special-teams", "xpm"): "pat_made",
    ("scoring", "special-teams", "xp_pct"): "pat_pct",
    ("scoring", "special-teams", "kret_td"): "kickoff_return_tds",
    ("scoring", "special-teams", "pret_t"): "punt_return_tds",
    # ---- KICKING / RETURNS -----------------------------------------------------------
    ("field-goals", "*", "fgm"): "fg_made",
    ("field-goals", "*", "att"): "fg_att",
    ("field-goals", "*", "lng"): "fg_long",
    ("field-goals", "*", "fg_blk"): "fg_blocked",
    ("kickoff-returns", "*", "ret"): "kickoff_returns",
    ("kickoff-returns", "*", "yds"): "kickoff_return_yards",
    ("kickoff-returns", "*", "lng"): "kickoff_return_long",
    ("kickoff-returns", "*", "kret_td"): "kickoff_return_tds",
    ("punt-returns", "*", "ret"): "punt_returns",
    ("punt-returns", "*", "yds"): "punt_return_yards",
    ("punt-returns", "*", "lng"): "punt_return_long",
    ("punt-returns", "*", "pret_t"): "punt_return_tds",
    ("punts", "", "punts"): "punts",
    ("punts", "", "yds"): "punt_yards",
    ("punts", "", "lng"): "punt_long",
    ("punts", "", "p_blk"): "punts_blocked",
    # ---- DOWNS -----------------------------------------------------------------------
    # The downs page republishes the first-down counts.  Its defense side is opponent
    # production, so it must land on the allowed canonicals; its offense side is our
    # team's production.  The count identity was measured on keyed 2025 team-seasons.
    ("downs", "defense", "rec_1st"): "receiving_first_downs_allowed",
    ("downs", "defense", "rush_1st"): "rushing_first_downs_allowed",
    ("downs", "defense", "3rd_att"): "def_third_down_faced",
    ("downs", "defense", "4th_att"): "def_fourth_down_faced",
    ("downs", "offense", "rec_1st"): "receiving_first_downs",
    ("downs", "offense", "rush_1st"): "rushing_first_downs",
    ("downs", "defense", "3rd_md"): "def_third_down_allowed",
    ("downs", "defense", "4th_md"): "def_fourth_down_allowed",
}

# Real material v26 does not carry. §24.4 -- census + demand join, never straight to schema.
NEW_CANDIDATES: dict[tuple[str, str, str], str] = {
    ("passing", "defense", "lng"): "longest pass ALLOWED. v26 has passing_long for the "
        "passer and no defensive counterpart",
    ("passing", "defense", "40"): "completions of 40+ yards ALLOWED. v26's only defensive "
        "explosive column is def_explosive_pass_allowed at the 20+ threshold",
    ("passing", "*", "40"): "completions of 40+ yards -- see completions_40plus",
    ("rushing", "defense", "lng"): "longest rush ALLOWED. No v26 counterpart",
    ("rushing", "defense", "rush_fum"): "fumbles on runs against this defense. Distinct "
        "from def_fumbles_forced, which credits the forcer",
    ("rushing", "defense", "40"): "rushes of 40+ yards ALLOWED. v26's defensive explosive "
        "column sits at the 20+ threshold only",
    ("rushing", "defense", "20"): "rushes of 20+ yards ALLOWED. v26's existing "
        "def_explosive_rush_allowed is the 10+ threshold (the 2025 comparison is 0.0% "
        "with a median 9 against 47), so this requires a distinct 20+ allowed canonical",
    ("rushing", "*", "20"): "rushes of 20+ yards. v26's rushing explosive threshold is "
        "rush_explosive_10 (10+) and its bucket column is rushing_40plus (40+); NEITHER is "
        "the 20+ count, and mapping to either would silently change the threshold",
    ("receiving", "defense", "lng"): "longest reception ALLOWED. No v26 counterpart",
    ("receiving", "defense", "rec_fum"): "fumbles by receivers against this defense",
    ("receiving", "defense", "40"): "receptions of 40+ yards ALLOWED",
    ("receiving", "*", "rec_yac_r"): "yards after catch PER RECEPTION. v26 carries "
        "receiving_yards_after_catch as a total but no per-reception rate, and NFL.com "
        "publishes no YAC total here to derive it from -- so the rate is the only form the "
        "number appears in",
    ("interceptions", "", "lng"): "longest interception return. v26 has "
        "def_interception_yards and def_int_ret_td but no LNG for the family, while every "
        "other return family has one. Proposed INDEPENDENTLY by the O.9.0 gamelog pass and "
        "by StatsCrew -- three sources reaching the same gap is what a schema hole looks "
        "like rather than a parse artefact",
    ("kickoff-returns", "*", "fc"): "fair catches on kickoffs. No v26 column",
    ("kickoff-returns", "*", "fum"): "fumbles on kickoff returns. No v26 column",
    ("kickoff-returns", "*", "20"): "kickoff returns of 20+ yards. No v26 column",
    ("kickoff-returns", "*", "40"): "kickoff returns of 40+ yards. No v26 column",
    ("kickoff-returns", "*", "xp_blk"): "extra points BLOCKED -- the site files this in the "
        "kickoff-returns table, but it is a special-teams block count. v26 has pat_blocked "
        "for the KICKER's side; this is the blocking team's credit",
    ("kickoff-returns", "special-teams", "fg_blk"): "field goals BLOCKED by this team's defense. "
        "This is a defensive component of the coarse def_blk_kick total, not fg_blocked, "
        "which is the kicker's own field-goal attempt blocked; v26 has no type-specific "
        "field-goal-block canonical",
    ("punt-returns", "*", "fc"): "fair catches on punts. No v26 column, and it is the "
        "standard punt-return volume denominator",
    ("punt-returns", "*", "fum"): "fumbles on punt returns. No v26 column",
    ("punt-returns", "special-teams", "p_blk"): "punts BLOCKED by this team's defense. "
        "This is a defensive component of the coarse def_blk_kick total, not punts_blocked, "
        "which is this team's own punt blocked; v26 has no type-specific punt-block canonical",
    ("punt-returns", "*", "20"): "punt returns of 20+ yards. No v26 column",
    ("punt-returns", "*", "40"): "punt returns of 40+ yards. No v26 column",
    ("punts", "", "net_yds"): "net punt yards (gross minus return and touchback penalty). "
        "v26 carries punt_yards but nothing net, and net is the modern evaluative punting "
        "number. Proposed independently by the O.9.0 gamelog pass",
    ("punts", "", "in_20"): "punts placed inside the 20. No v26 column; proposed "
        "independently by O.9.0 and by StatsCrew",
    ("punts", "", "oob"): "punts out of bounds. No v26 column",
    ("punts", "", "dn"): "punts downed by the coverage team. No v26 column",
    ("punts", "", "tb"): "punt touchbacks. No v26 column",
    ("punts", "", "fc"): "fair catches induced by this punter. No v26 column",
    ("punts", "", "ret"): "punt returns ALLOWED on this punter's punts -- the coverage "
        "side, not the returner's. Mapping it to punt_returns would credit the punter with "
        "the returner's work; the same distinction the splits Punting block forced",
    ("punts", "", "rety"): "punt return yards ALLOWED on this punter's punts",
    ("punts", "", "td"): "touchdowns scored on returns of this punter's punts",
    ("kickoffs", "", "ko"): "kickoffs taken. v26 carries kickoff RETURN columns but nothing "
        "for the kicking side; proposed independently by O.9.0 and StatsCrew",
    ("kickoffs", "", "yds"): "kickoff yards -- the kicking side of the kickoff",
    ("kickoffs", "", "tb"): "kickoff touchbacks. No v26 column",
    ("kickoffs", "", "oob"): "kickoffs out of bounds. No v26 column",
    ("kickoffs", "", "osk"): "onside kicks attempted. No v26 column",
    ("kickoffs", "", "osk_rec"): "onside kicks recovered. No v26 column",
    ("kickoffs", "", "ret"): "returns of this player's kickoffs -- coverage side",
    ("kickoffs", "", "ret_yds"): "return yards allowed on this player's kickoffs",
    ("kickoffs", "", "td"): "touchdowns scored on returns of this player's kickoffs",
    ("scoring", "offense", "2_pt"): "two-point conversions as a TEAM total. v26 carries "
        "passing_/rushing_/receiving_2pt_conversions separately but no total, so this is "
        "not a duplicate of any single one of them",
    ("downs", "offense", "3rd_att"): "third downs faced by this offense. The existing "
        "third-down faced canonical is defense-side only",
    ("downs", "offense", "4th_att"): "fourth downs faced by this offense. The existing "
        "fourth-down faced canonical is defense-side only",
    ("downs", "offense", "3rd_md"): "third downs CONVERTED by this offense. v26's only "
        "third-down column is def_third_down_allowed, which is the defensive side",
    ("downs", "offense", "4th_md"): "fourth downs converted by this offense",
    ("downs", "*", "scrm_plys"): "scrimmage plays run. No v26 column, and it is the "
        "denominator every per-play rate in the program currently lacks",
    # CAUGHT BY THE OPERAND CHECK, not by reading the name. The field-goals table publishes
    # FGM and Att so its 'FG %' is a derivation -- but the SCORING table publishes FGM
    # without Att, so there the percentage is the only form the value appears in. Excluding
    # it as a ratio would lose it. Same shape as O.9.0's kickoff_avg.
    ("scoring", "special-teams", "fg"): "field-goal percentage, and THIS table publishes no "
        "attempts column to recompute it from (unlike the field-goals table, which does). "
        "The rate is the only form the value takes here, so it is material rather than a "
        "derivation",
}

# Derived rates whose OPERANDS the same table publishes. Checked, not asserted: the operand
# columns must be present in the same signature or the entry is refused at validation.
DERIVED_RATIOS: dict[tuple[str, str, str], tuple[str, str]] = {
    ("passing", "offense", "cmp_2"): ("cmp", "att"),
    ("passing", "offense", "yds_att"): ("pass_yds", "att"),
    ("passing", "offense", "1st_2"): ("1st", "att"),
    ("passing", "", "cmp_2"): ("cmp", "att"),
    ("passing", "", "yds_att"): ("pass_yds", "att"),
    ("passing", "", "1st_2"): ("1st", "att"),
    ("passing", "defense", "cmp_2"): ("cmp", "att"),
    ("passing", "defense", "yds_att"): ("yds", "att"),
    ("passing", "defense", "1st_2"): ("1st", "att"),
    ("rushing", "*", "ypc"): ("rush_yds", "att"),
    ("rushing", "*", "rush_1st_2"): ("rush_1st", "att"),
    ("receiving", "*", "yds_rec"): ("yds", "rec"),
    ("receiving", "*", "rec_1st_2"): ("rec_1st", "rec"),
    # THE TRAP: in player_season.receiving the SHORT key is the percentage and the LONG key
    # is the count. Published headers: `1st` = '1st%', `rec_1st` = 'Rec 1st'.
    ("receiving", "", "1st"): ("rec_1st", "rec"),
    ("field-goals", "*", "fg"): ("fgm", "att"),
    ("kickoff-returns", "*", "avg"): ("yds", "ret"),
    ("punt-returns", "*", "avg"): ("yds", "ret"),
    ("punts", "", "avg"): ("yds", "punts"),
    ("punts", "", "net_avg"): ("net_yds", "punts"),
    ("kickoffs", "", "ret_avg"): ("ret_yds", "ret"),
    ("kickoffs", "", "tb_2"): ("tb", "ko"),
}

# The downs page publishes the first-down percentage but omits the denominator that its
# count pages publish.  These are still derived rates: the numerator is the mapped downs
# count and the denominator is the canonical lower-layer volume atom.  Keep them separate
# from DERIVED_RATIOS because the source table does not publish both operands itself.
EXTERNAL_DERIVED_RATIOS: dict[tuple[str, str, str], tuple[str, str, str]] = {
    ("downs", "offense", "rec_1st_2"):
        ("receiving_first_downs", "receptions", "R_DOWNS_RECV_FD_PCT"),
    ("downs", "offense", "rush_1st_2"):
        ("rushing_first_downs", "carries", "R_DOWNS_RUSH_FD_PCT"),
    ("downs", "defense", "rec_1st_2"):
        ("receiving_first_downs_allowed", "def_completions_allowed",
         "R_ALW_DOWNS_RECV_FD_PCT"),
    ("downs", "defense", "rush_1st_2"):
        ("rushing_first_downs_allowed", "def_carries_allowed",
         "R_ALW_DOWNS_RUSH_FD_PCT"),
}

ESCALATED: dict[tuple[str, str, str], str] = {
    ("tackles", "", "comb"): "published as 'Comb'. THE TACKLE-TOTAL QUESTION, now raised by "
        "a FOURTH nflcom surface: O.9.0 found TKL|AST|COMBINED|SOLO in player_career, "
        "StatsCrew's own 'Total Tackles' fails tackle = solo + ast on 65.4% of rows, and "
        "the splits DEFENSE block publishes 'Total'. Mapping this to def_tackles_combined "
        "before that identity is partitioned by era would double-count or halve every "
        "historical tackle total. SETTLED BY the open escalation, applied to all four "
        "surfaces at once",
    ("fumbles", "", "fr"): "published as bare 'FR' with no indication whose fumble was "
        "recovered. v26 splits fumble_recovery_own from fumble_recovery_opp, so guessing "
        "picks one of two real canonicals. The same question StatsCrew's `frec` raised. "
        "SETTLED BY: comparing against a source that DOES distinguish -- the nflcom splits "
        "Fumbles block publishes OWN FR and OPP FR separately, which is a comparison this "
        "program can now actually run",
    ("scoring", "special-teams", "fgm"): "",   # placeholder, replaced below
}
del ESCALATED[("scoring", "special-teams", "fgm")]

# The field-goal DISTANCE BUCKETS: one cell, two facts. NFL.com's own header says so.
_BUCKET_ESCALATION = (
    "a COMPOSITE cell, and the site's own header admits it: {header!r} -- 'A-M' is "
    "ATTEMPTED-MADE in one column. v26 carries both halves separately as fg_made_{bucket} "
    "and fg_missed_{bucket}. No disposition in the vocabulary fits: MAPPED_TO_CANONICAL "
    "names ONE target and drops the attempts, EXCLUDED_WITH_REASON discards the only place "
    "NFL.com publishes per-distance field goals, NEW_SUPERTABLE_COLUMN_CANDIDATE proposes "
    "storing the string. THIRD independent instance of the gap StatsCrew's `results.game` "
    "opened and the nflcom splits L7 buckets repeated -- three lineages make it a "
    "vocabulary question, not a quirk. SETTLED BY: a split-at-admission disposition that "
    "names more than one target")

_BUCKETS = {"1_19_a_m": "0_19", "20_29_a_m": "20_29", "30_39_a_m": "30_39",
            "40_49_a_m": "40_49", "50_59_a_m": "50_59", "60_a_m": "60_"}


def published_headers() -> dict[tuple[str, str, str], str]:
    """(category, side, column) -> the header NFL.com printed, via the MEASURED join from
    each dossier table to exactly one signature key-set."""
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    semantics = json.loads(NFLCOM_SEMANTICS.read_text(encoding="utf-8"))
    out: dict[tuple[str, str, str], str] = {}
    problems: list[str] = []
    for source, family in SOURCES.items():
        columns: dict[str, set[str]] = collections.defaultdict(set)
        for row in dossier["rows"]:
            if row["source"] == source:
                columns[row["table_key"]].add(row["column"])
        signatures: dict[frozenset, list[dict]] = collections.defaultdict(list)
        for signature in semantics["families"][family]["signatures"]:
            signatures[frozenset(signature["keys"])].append(signature)
        for table_key, cols in columns.items():
            found = signatures.get(frozenset(cols - PARTITION))
            if not found:
                problems.append(f"{source}|{table_key}: no signature reproduces this "
                                "table's column set")
                continue
            if len({tuple(s["headers"]) for s in found}) > 1:
                problems.append(f"{source}|{table_key}: two signatures with different "
                                "headers share this key set")
                continue
            parts = table_key.split("|")
            category = parts[0]
            side = parts[1] if len(parts) == 3 else ""
            for key, header in zip(found[0]["keys"], found[0]["headers"]):
                out[(category, side, str(key))] = str(header)
    if problems:
        raise SystemExit("the dossier-to-signature join is not total:\n  "
                         + "\n  ".join(problems[:20]))
    return out


def _lookup(table: dict, category: str, side: str, column: str):
    for key in ((category, side, column), (category, "*", column)):
        if key in table:
            return table[key]
    return None


def _validate(v26: set[str]) -> list[str]:
    problems = [f"{key}: canonical {canonical!r} is not a v26 column"
                for key, canonical in MAPPED.items() if canonical not in v26]
    seen: dict[tuple[str, str, str], str] = {}
    for name, table in (("MAPPED", MAPPED), ("NEW", NEW_CANDIDATES),
                        ("DERIVED", DERIVED_RATIOS),
                        ("EXTERNAL_DERIVED", EXTERNAL_DERIVED_RATIOS),
                        ("ESCALATED", ESCALATED)):
        for key in table:
            if key in seen:
                problems.append(f"{key}: in {seen[key]} and {name}")
            seen[key] = name

    headers = published_headers()
    # every decided column must be one the site publishes a header for
    for name, table in (("MAPPED", MAPPED), ("NEW", NEW_CANDIDATES),
                        ("DERIVED", DERIVED_RATIOS)):
        for category, side, column in table:
            if side == "*":
                continue
            if (category, side, column) not in headers:
                problems.append(f"{name} {category}|{side}|{column}: the source publishes "
                                "no header for this column")
    # every derived ratio's OPERANDS must be published by the SAME table
    published = collections.defaultdict(set)
    for (category, side, column) in headers:
        published[(category, side)].add(column)
    for (category, side, column), (numerator, denominator) in DERIVED_RATIOS.items():
        sides = [s for (c, s) in published if c == category] if side == "*" else [side]
        for actual in sides:
            have = published[(category, actual)]
            if column not in have:
                continue
            missing = {numerator, denominator} - have
            if missing:
                problems.append(
                    f"DERIVED {category}|{actual}|{column}: operands {sorted(missing)} are "
                    "NOT published by this table, so the ratio is the only form the value "
                    "appears in and it is real material, not a derivation")
    for (category, side, column), (numerator, denominator, witness) in \
            EXTERNAL_DERIVED_RATIOS.items():
        if (category, side, column) not in headers:
            problems.append(f"EXTERNAL_DERIVED {category}|{side}|{column}: the source "
                            "publishes no header for this column")
        for canonical in (numerator, denominator):
            if canonical not in v26:
                problems.append(f"EXTERNAL_DERIVED {category}|{side}|{column}: canonical "
                                f"{canonical!r} is not a v26 column")
        if not witness:
            problems.append(f"EXTERNAL_DERIVED {category}|{side}|{column}: missing witness "
                            "family")
    return problems


def build_decisions() -> tuple[list[dict], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    headers = published_headers()

    from .column_dossier import load_decisions
    mine = {key for key, entry in load_decisions().items()
            if entry.get("generated_by") == GENERATOR}

    decisions: list[dict] = []
    tally = {"mapped": 0, "new": 0, "excluded": 0, "derived": 0,
             "escalated_left_open": 0, "unhandled_left_open": 0}
    unhandled: set[str] = set()

    for row in dossier["rows"]:
        if row["source"] not in SOURCES:
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        parts = row["table_key"].split("|")
        category = parts[0]
        side = parts[1] if len(parts) == 3 else ""
        column = row["column"]
        header = headers.get((category, side, column), column)
        evidence = (f"docs/nflcom-column-signature-census.json: NFL.com publishes this "
                    f"column as {header!r} in its {category!r} table"
                    + (f" on the {side!r} side" if side else "")
                    + ". The dossier table joins to EXACTLY ONE header signature (54 of 54 "
                      "tables, 0 ambiguous, 0 unmatched) after subtracting the harvester's "
                      "partition columns")

        if _lookup(ESCALATED, category, side, column) is not None:
            tally["escalated_left_open"] += 1
            continue
        if category == "field-goals" and column in _BUCKETS:
            tally["escalated_left_open"] += 1
            continue
        ratio = _lookup(DERIVED_RATIOS, category, side, column)
        external_ratio = _lookup(EXTERNAL_DERIVED_RATIOS, category, side, column)
        canonical = _lookup(MAPPED, category, side, column)
        candidate = _lookup(NEW_CANDIDATES, category, side, column)
        if external_ratio is not None:
            numerator, denominator, witness = external_ratio
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"a derived rate. NFL.com's {header!r} omits its denominator, "
                          f"but the mapped count and lower-layer denominator are "
                          f"{numerator!r}/{denominator!r}; the conservation lane witnesses "
                          f"the equation as {witness!r}. This is an equation check, not a "
                          "new stored cell",
                "evidence": evidence})
            tally["derived"] += 1
        elif ratio is not None:
            numerator, denominator = ratio
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"a derived rate. The same table publishes its operands "
                          f"{numerator!r}/{denominator!r}, checked against the signature "
                          f"rather than assumed, so this row carries no material they do "
                          f"not. It is an equation check, not a stored cell",
                "evidence": evidence})
            tally["derived"] += 1
        elif canonical is not None:
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canonical,
                "reason": f"the site's {header!r} column in its {category!r} table"
                          + (f" on the {side!r} side, which is "
                             + ("what the defense CONCEDED"
                                if side == "defense" and canonical.endswith("_allowed")
                                else "the credit that side earns")
                             if side == "defense" else "")
                          + f"; v26 carries the same material as {canonical!r}",
                "evidence": evidence})
            tally["mapped"] += 1
        elif candidate is not None:
            decisions.append({
                "key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                "reason": candidate, "evidence": evidence})
            tally["new"] += 1
        else:
            unhandled.add(f"{category}|{side}|{column} ({header!r})")
            tally["unhandled_left_open"] += 1

    return decisions, tally | {"unhandled": sorted(unhandled)}


def escalated_row_keys() -> dict[str, str]:
    """Dossier row key -> the question that stopped the decision."""
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    headers = published_headers()
    out: dict[str, str] = {}
    for row in dossier["rows"]:
        if row["source"] not in SOURCES or row["disposition"] != "OPEN":
            continue
        parts = row["table_key"].split("|")
        category = parts[0]
        side = parts[1] if len(parts) == 3 else ""
        column = row["column"]
        key = row_key(row["source"], row["table_key"], column)
        question = _lookup(ESCALATED, category, side, column)
        if question:
            out[key] = question
        elif category == "field-goals" and column in _BUCKETS:
            out[key] = _BUCKET_ESCALATION.format(
                header=headers.get((category, side, column), column),
                bucket=_BUCKETS[column])
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    problems = _validate(_v26_columns())
    if problems:
        print("CORRESPONDENCE TABLE INVALID:")
        for problem in problems:
            print("  -", problem)
        return 1

    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for name, value in tally.items():
        if name != "unhandled":
            print(f"  {name:24s} {value}")
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
