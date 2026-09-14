"""O.9.3 burn-down: adjudicate the nflcom RESOLVED-layout columns into the ledger.

WHY A MODULE AND NOT A HAND-EDITED JSON. The decisions are the CORRESPONDENCE TABLE
below -- one line per O.9.0 semantic concept, argued once and applied to every
(caption, layout, column) that concept appears under. `yds` alone appears under five
different meanings across the logs layouts; deciding it 21 times by hand is 21 chances to
decide it differently. Writing the table once and letting it fan out is the same
discipline the dossier applies to its own keys.

THE EVIDENCE, and its exact limit. `docs/nflcom-column-semantics.json` records, per
signature, the header sequence NFL.com published and the layout that reproduces it. The
artifact is explicit about what that proves:

    PROVEN      SEGMENTATION only -- exactly one declared layout reproduces the observed
                header sequence, so the BLOCK BOUNDARIES (which stat family a position
                belongs to) are receipted by regeneration.
    NOT PROVEN  the semantic LABELS inside each block are declared from NFL.com's own
                column legend and are not yet value-validated against a witness.

So a decision here says *this column is the site's <name> in a receipted <block>* and
maps that to the canonical cell of the same meaning. It does NOT license the source to
vote -- every nflcom family remains MAPPING_PENDING behind the slug->pfr_id crosswalk,
and the value-validation that turns a declared label into a receipted one happens at
licensing. An adjudication is a statement about what a column IS, not permission to use
it (§19 proof-or-pending is unaffected).

WHAT IS DELIBERATELY NOT DECIDED HERE:
  - `player_situational` and `player_splits`: QUARANTINED by the O.9.0 COLUMN-SHIFT
    defect. Every stored value sits one column left of its name, so the stored name is
    known-wrong and there is nothing to adjudicate until the re-parse lands. Adjudicating
    a name we know is wrong would launder a defect into a decision.
  - `def_tackles` (TKL): the remaining named label question. `def_fum_rec` (FR) is
    settled below by a date-keyed weekly candidate matrix; it is mapped, but that
    does not license the source or invent a preseason subject row.
  - `player_season` and `team_stats`: their signatures are NAMED, not RESOLVED -- no
    layout reproduces them, so the block boundaries are not receipted and the only
    evidence is an uppercased header string. A separate pass.

Run:  python -m scripts.sota_recon.nflcom_column_adjudication [--apply]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .column_dossier import DISPOSITIONS_PATH, NFLCOM_SEMANTICS, row_key

ROOT = Path(__file__).resolve().parents[2]
DOSSIER_PATH = ROOT / "docs" / "column-dossier.json"

# families whose signatures carry a RESOLVED layout -- the only ones this pass touches
FAMILIES = {
    "player_logs": "nflcom_player_logs",
    "player_career": "nflcom_player_career",
    # The targeted surface was re-parsed from retained cache with a real `_layout`.
    # Its header grammar is exactly the player_logs grammar; only its capture population
    # differs.  The reparse receipt is a second required evidence edge.
    "player_logs_targeted": "nflcom_player_logs_targeted",
}


def _signature_family(family: str) -> str:
    return "player_logs" if family == "player_logs_targeted" else family

_RECEIPT = ("docs/nflcom-column-semantics.json -- O.9.0 signature {sig!r}, status "
            "RESOLVED: exactly one declared layout reproduces this header sequence, so "
            "the block boundary is receipted by regeneration. Position {position} of "
            "that block is the site's {semantic!r}")

# ---------------------------------------------------------------------------
# THE CORRESPONDENCE TABLE -- O.9.0 semantic concept -> canonical v26 column.
#
# Every target is checked against the live v26 column list before anything is written
# (see `_validate`): a canonical that does not exist would be a mapping to nowhere, and
# it would pass the ledger's own validator, which only checks that the field is present.
# ---------------------------------------------------------------------------
MAPPED: dict[str, str] = {
    # --- defense: the block NFL.com publishes as Total/Solo/AST/SCK/SFTY/PDEF/INT/... ---
    "def_ast_tackles": "def_tackle_assists",
    "def_combined_tackles": "def_tackles_combined",
    "def_forced_fumbles": "def_fumbles_forced",
    # DEF_log's bare FR is defensive recovery of an opponent fumble. This is not the
    # returner's fumble count and not `fumble_recovery_own`; the all-years date-keyed
    # weekly witness is retained beside the source and cited in the generated receipt.
    "def_fum_rec": "fumble_recovery_opp",
    "def_fum_rec_opp": "fumble_recovery_opp",
    "def_int": "def_interceptions",
    "def_int_ret_td": "def_int_ret_td",
    "def_int_ret_yds": "def_interception_yards",
    "def_pass_defended": "def_pass_defended",
    "def_sacks": "def_sacks",
    "def_safeties": "def_safeties",
    "def_solo_tackles": "def_tackles_solo",
    # --- passing ---
    "pass_att": "attempts",
    "pass_cmp": "completions",
    "pass_int": "passing_interceptions",
    "pass_td": "passing_tds",
    "pass_yds": "passing_yards",
    "pass_yds_per_att": "passing_yards_per_attempt",
    "passer_rating": "passer_rating",
    "sack_yds_lost": "sack_yards_lost",
    "sacks_suffered": "sacks_suffered",
    # --- rushing / receiving ---
    "rec": "receptions",
    "rec_long": "receiving_long",
    "rec_td": "receiving_tds",
    "rec_yds": "receiving_yards",
    "rec_yds_per_rec": "receiving_yards_per_reception",
    "rush_att": "carries",
    "rush_long": "rushing_long",
    "rush_td": "rushing_tds",
    "rush_yds": "rushing_yards",
    "rush_yds_per_att": "rushing_yards_per_carry",
    "fumbles": "fumbles",
    "fumbles_lost": "fumbles_lost",
    # --- kicking. NFL.com calls the extra point XP; v26 canonicalises the family as
    #     pat_* (pat_att/made/pct/blocked/missed all exist). Same material, our spelling.
    "fg_att": "fg_att",
    "fg_blocked": "fg_blocked",
    "fg_long": "fg_long",
    "fg_made": "fg_made",
    "fg_pct": "fg_pct",
    "xp_att": "pat_att",
    "xp_blocked": "pat_blocked",
    "xp_made": "pat_made",
    "xp_pct": "pat_pct",
    # --- punting / returns ---
    "punt_avg": "punt_yards_per_punt",
    "punt_long": "punt_long",
    "punt_ret": "punt_returns",
    "punt_ret_td": "punt_return_tds",
    "punt_ret_yds": "punt_return_yards",
    "punt_yds": "punt_yards",
    "punts": "punts",
    "punts_blocked": "punts_blocked",
    "kickoff_ret": "kickoff_returns",
    # --- row locators the bulk rules do not cover by name ---
    "game_date": "game_date",
    "season": "year",
    "week": "week",
}

# Real material the supertable does not carry. §24.4: these route through the census +
# demand join, never straight into a schema.
NEW_CANDIDATES: dict[str, str] = {
    "def_int_ret_long": "longest interception return. v26 carries def_interception_yards "
                        "and def_int_ret_td but no LNG for the family; every other "
                        "return family in v26 has one (punt_return_long, "
                        "kickoff_return_long), so this is a gap in our schema rather "
                        "than a concept we rejected",
    "punt_fair_catches": "fair catches induced. No v26 column; a punt-coverage concept "
                         "the supertable has never carried",
    "punt_net_yds": "net punt yards (gross minus return and touchback penalty). v26 "
                    "carries punt_yards but nothing net; net is the modern evaluative "
                    "punting number and is not recomputable from what we store",
    "punt_touchbacks": "punts into the end zone. No v26 column",
    "punts_downed": "punts downed by the coverage team. No v26 column",
    "punts_in_20": "punts placed inside the 20. No v26 column, and it is the standard "
                   "punting placement statistic",
    "punts_oob": "punts out of bounds. No v26 column",
    "kickoffs": "kickoffs taken. v26 carries kickoff RETURN columns but nothing for the "
                "kicking side of the kickoff",
    "kickoff_avg": "average kickoff distance. No v26 column, and NOT derivable from what "
                   "NFL.com publishes here -- the signature carries the average without "
                   "a kickoff-yards operand, so excluding it as a ratio would lose the "
                   "only form the number appears in",
    "kickoff_ret_avg": "kickoff return average. No v26 column, and the logs signature "
                       "publishes no kickoff-return-yards operand to recompute it from",
    "touchbacks": "kickoff touchbacks. No v26 column",
}

# Derived ratios whose OPERANDS this same signature publishes. Excluded because the row
# carries no material the operands do not already carry -- it is an equation check
# (equations.v1.json), not a stored cell. The operand claim is CHECKED per signature, not
# asserted: a ratio whose operands are absent is real material in its only available form
# and belongs in NEW_CANDIDATES instead (see kickoff_avg).
DERIVED_RATIOS: dict[str, tuple[str, ...]] = {
    "def_int_ret_avg": ("def_int", "def_int_ret_yds"),
    "punt_net_avg": ("punts", "punt_net_yds"),
}

# Excluded on a grain or ownership argument, one at a time.
EXCLUDED: dict[str, str] = {
    "games": "a games COUNT is a season aggregate; the v26 subject is player-WEEK grain "
             "and has no such column by construction. Its obligation is the presence "
             "lane (pfr_games_played / entity_universes appearance witnesses), which is "
             "receipted separately -- mapping it to a weekly cell would invent one",
    "games_started": "as `games`: a season aggregate against a weekly subject. The "
                     "start/no-start fact per game lives in the presence lane",
    "result": "the game's outcome (W/L 12-20) is TEAM-grain context on a player row. "
              "nfl_team_games_all is the authoritative W/L/T source and the game "
              "catalog is the anchor for it; a per-player copy is neither a player stat "
              "nor an independent team witness",
    "opponent": "locates the row against the game catalog rather than witnessing a "
                "stat; its obligation is the game-key crosswalk lane",
    "team": "locates the row; its obligation is the team_fid canonicalization lane",
}

# Player logs carry `wk` as part of their player-game locator.  Keep it in the key
# plane for this source; mapping it to the weekly stat vocabulary makes a locator look
# like a witnessed statistic.  Other NFL.com surfaces retain their own established
# treatment and are not changed by this source-specific rule.
PLAYER_LOGS_KEY_EXCLUDED: dict[str, str] = {
    "week": "the week number locates the player-game row; it is a key, not a player "
            "statistic. Its obligation is the key/crosswalk plane, not a stat witness",
    "kickoff_ret": "returns of this player's kickoffs -- coverage-side activity on a "
                    "kicker log, not the kicker's own kickoff-returner credit. v26's "
                    "`kickoff_returns` is a returner-grain player stat",
    "punt_ret": "returns of this player's punts -- coverage-side activity on a punter "
                 "log, not the returner's own punt-return credit. v26's `punt_returns` "
                 "is a returner-grain player stat",
    "punt_ret_yds": "return yards allowed on this player's punts -- coverage-side "
                    "activity, not `punt_return_yards` credited to the returner",
    "punt_ret_td": "touchdowns scored on returns of this player's punts -- coverage-side "
                   "activity, not `punt_return_tds` credited to the returner",
}

PLAYER_CAREER_OWNERSHIP_CANDIDATES: dict[str, str] = {
    "kickoff_ret": "kickoff_returns_allowed: returns of this player's kickoffs are "
                    "coverage-side activity, not the returner's own credit; v26 has no "
                    "player-kicker allowed-return canonical",
    "punt_ret": "returns of this player's punts are coverage-side activity, not the "
                 "returner's own `punt_returns`; v26 has no punter allowed-return "
                 "canonical",
    "punt_ret_yds": "return yards allowed on this player's punts are coverage-side "
                    "activity, not `punt_return_yards` credited to the returner",
    "punt_ret_td": "touchdowns scored on returns of this player's punts are coverage-"
                   "side activity, not `punt_return_tds` credited to the returner",
}

# NAMED LABEL QUESTIONS -- left OPEN on purpose, each with the question that would settle
# it. These are the rows an unchecked bulk pass would have swallowed silently.
ESCALATED: dict[str, str] = {
    "def_tackles": "O.9.0 KNOWN_LABEL_QUESTIONS, verbatim: the player_career DEF block "
                   "renders TKL|AST|COMBINED|SOLO -- four tackle columns where two would "
                   "do, and which of TKL/COMBINED is the site's TOTAL is a label question "
                   "the layout test cannot settle. Mapping the wrong one to "
                   "def_tackles_combined would double-count or halve every historical "
                   "tackle total. SETTLED BY: value-validating TKL and COMBINED against "
                   "def_tackles_solo + def_tackle_assists on the same rows",
}


# ---------------------------------------------------------------------------
# THE TEAM_STATS DUPLICATE BLOCK. Not a judgement call -- a receipted measurement that
# already governs this source, restated at column grain.
#
# `sources.py` carries a MANDATORY SOURCE-DEFINITION FILTER on nflcom_team_stats:
# rows WHERE NOT (_side='special-teams' AND _category IN ('kicking','punting')). It was
# measured on 2026-07-26 by content-hash equality over every non-partition column: those
# two families are byte-identical to offense_passing on all 4,456 rows each (16 files ->
# 14 distinct contents), and their column set IS the passing set. NFL.com serves no
# /team-stats/special-teams/{kicking,punting}/ page, so the harvester cached the default
# passing table under a kicking and a punting name.
#
# The rows are already excluded from the source. Their dossier ROWS were not, and a
# column-level queue that leaves them OPEN invites someone to adjudicate `att` as a
# kicking attempt -- which is exactly the mislabelling the row filter exists to stop.
DUPLICATE_TABLE_KEYS = {
    "kicking|special-teams": "passing|offense",
    "punting|special-teams": "passing|offense",
}
_DUPLICATE_SOURCE = "nflcom_team_stats"
_DUPLICATE_EVIDENCE = (
    "sources.py nflcom_team_stats source-definition filter, measured 2026-07-26: "
    "special-teams/{kicking,punting} are byte-identical to offense_passing on all 4,456 "
    "rows each by content hash over every non-partition column, and their column set IS "
    "the passing set (att/cmp/pass_yds/rate/scky). NFL.com serves no such page; the "
    "harvester cached the default passing table under two other names")


# ---- long-alias columns ------------------------------------------------------------
# `nflcom_player_logs` carries the site's short header AND a long alias as two separate
# PHYSICAL parquet columns: `fum`/`fumbles` and `lost`/`fumbles_lost`. Only the short
# form appears in the layout census, so the long form stayed OPEN across all 21
# caption/layout pairs -- 42 dossier columns describing material already adjudicated.
#
# Equal non-empty COUNTS would not have settled this; a one-sided match is not a
# receipt. The pairing was scored against its own crossed control:
#     fum  == fumbles       100.00% of 57,724 non-empty rows
#     lost == fumbles_lost  100.00% of 57,724
#     fum  == fumbles_lost   37.48%   <- control, agrees only where the two coincide
#     lost == fumbles        37.48%   <- control
# 100.00% against 37.48% is the margin, and the margin is what licenses DUPLICATE_OF.
# The four layouts whose header names no fumble column at all. The count is the
# occupancy denominator that was scanned, and the alias was non-empty on 0 of each.
ALIAS_UNPUBLISHED_LAYOUTS = {"DEF_log": 428779, "K_log": 37176, "P": 31294, "OL": 53782}
ALIAS_COLUMNS: dict[tuple[str, str], str] = {
    ("nflcom_player_logs", "fumbles"): "fum",
    ("nflcom_player_logs", "fumbles_lost"): "lost",
}
_ALIAS_EVIDENCE = (
    "value-equality scan over nflcom_player_logs (union_by_name): {alias!r} equals "
    "{short!r} on 100.00% of 57,724 non-empty rows, against 37.48% for the crossed "
    "control pairing. Measured 2026-07-29")


def alias_decisions() -> list[dict]:
    """Long-alias physical columns -> the short column the layout census already names."""
    document = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    keys = {row_key(r["source"], r["table_key"], r["column"]) for r in document["rows"]}
    decisions = []
    for row in document["rows"]:
        short = ALIAS_COLUMNS.get((row["source"], row["column"]))
        if short is None or row["disposition"] != "OPEN":
            continue
        target = row_key(row["source"], row["table_key"], short)
        if target not in keys:
            # The short column is not published under THIS caption. That does NOT make
            # the alias a duplicate -- if it carried material here it would be the only
            # form of it, and excluding it would lose real data. So it is measured
            # rather than assumed: NFL.com's own header for these four layouts ends at
            # FR / Avg / TD / GS and names no fumble column at all, and an occupancy
            # scan confirms the parquet never writes one.
            layout = row["table_key"].rpartition(":")[2]
            scanned = ALIAS_UNPUBLISHED_LAYOUTS.get(layout)
            if scanned is None:
                continue
            decisions.append({
                "key": row_key(row["source"], row["table_key"], row["column"]),
                "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"the {layout} header publishes no fumble column, and "
                          f"{row['column']!r} is empty on every one of the "
                          f"{scanned:,} {layout}-shaped rows. The parquet declares the "
                          f"column because the schema is shared across layouts; this "
                          f"layout never writes it, so the exclusion discards nothing",
                "evidence": f"occupancy scan over nflcom_player_logs restricted to "
                            f"{layout}-shaped rows ({scanned:,} rows): "
                            f"{row['column']!r} non-empty on 0. Measured 2026-07-29",
            })
            continue
        decisions.append({
            "key": row_key(row["source"], row["table_key"], row["column"]),
            "disposition": "DUPLICATE_OF",
            "canonical": target,
            "reason": f"{row['column']!r} is a long alias of {short!r} -- the same "
                      f"physical value, byte for byte, on every non-empty row. NFL.com "
                      f"publishes one header; the parquet carries two columns",
            "evidence": _ALIAS_EVIDENCE.format(alias=row["column"], short=short),
        })
    return decisions


def duplicate_decisions() -> list[dict]:
    document = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    decisions = []
    for row in document["rows"]:
        if row["source"] != _DUPLICATE_SOURCE or row["disposition"] != "OPEN":
            continue
        table_key = row["table_key"]
        family, _, season_type = table_key.rpartition("|")
        target = DUPLICATE_TABLE_KEYS.get(family)
        if target is None:
            continue
        decisions.append({
            "key": row_key(_DUPLICATE_SOURCE, table_key, row["column"]),
            "disposition": "DUPLICATE_OF",
            "canonical": row_key(_DUPLICATE_SOURCE, f"{target}|{season_type}",
                                 row["column"]),
            "reason": f"{family} is a cached copy of passing/offense, not a "
                      f"special-teams table. Left OPEN, this row invites "
                      f"{row['column']!r} to be adjudicated as a "
                      f"{family.split('|')[0]} statistic -- the precise mislabelling "
                      "the row-level source filter already prevents",
            "evidence": _DUPLICATE_EVIDENCE,
        })
    return decisions


def _v26_columns() -> set[str]:
    import duckdb

    from .sources import latest_v26

    connection = duckdb.connect()
    try:
        return {
            row[0] for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{latest_v26()}')"
            ).fetchall()
        }
    finally:
        connection.close()


def _validate_table(v26: set[str]) -> list[str]:
    """A canonical that does not exist is a mapping to nowhere -- and it would pass the
    ledger's validator, which only checks that the field is non-empty."""
    problems = [f"{semantic}: canonical {canonical!r} is not a v26 column"
                for semantic, canonical in MAPPED.items() if canonical not in v26]
    problems += [f"{semantic}: proposed as NEW but v26 already carries it"
                 for semantic in NEW_CANDIDATES if semantic in v26]
    overlap = set(MAPPED) & (set(NEW_CANDIDATES) | set(EXCLUDED) | set(DERIVED_RATIOS)
                             | set(ESCALATED))
    if overlap:
        problems.append(f"a semantic in two buckets at once: {sorted(overlap)}")
    return problems


# ---- the unparsed caption on `nflcom_player_career` --------------------------------
# Measured 2026-07-29 against the parquet, not read off the column names. The three
# plumbing keys (`_table`, `_view`, `nflcom_slug`) are deliberately ABSENT: the dossier's
# own plumbing rule already closes them, and a generated decision must not overwrite it.
BLANK_CAPTION_SOURCE = "nflcom_player_career"
BLANK_CAPTION_SLUG = "blake-hance"
BLANK_CAPTION_ROWS = 48
BLANK_CAPTION_TOTAL = 637134
BLANK_CAPTION_SEMANTICS = {"wk": "week", "opp": "opponent", "result": "result",
                           "g": "games", "gs": "games_started"}
BLANK_CAPTION_OCCUPANCY = {
    "ast": 0, "att": 0, "avg": 0, "avg_2": 0, "blk": 0,
    "combined": 0, "dn": 0, "fc": 0, "ff": 0, "fg_att": 0, "fgm": 0, "fum": 0, "g": 8,
    "gs": 8, "in_20": 0, "int": 0, "ko": 0, "lng": 0, "lng_2": 0, "lost": 0, "net_avg": 0,
    "net_yds": 0, "oob": 0, "opp": 48, "opp_fr": 0, "pct": 0, "pdef": 0,
    "punts": 0, "rec": 0, "result": 48, "ret": 0, "rety": 0, "sck": 0, "sfty": 0, "solo": 0,
    "tb": 0, "td": 0, "td_2": 0, "tds": 0, "tkl": 0, "wk": 48, "xblk": 0, "xp_att": 0,
    "xpct": 0, "xpm": 0, "yds": 0, "yds_2": 0,
}


def build_decisions() -> tuple[list[dict], dict[str, int]]:
    document = json.loads(NFLCOM_SEMANTICS.read_text(encoding="utf-8"))
    dossier_keys = {
        row_key(row["source"], row["table_key"], row["column"])
        for row in json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))["rows"]
    }
    decisions: list[dict] = []
    tally = {"mapped": 0, "new": 0, "excluded": 0, "derived": 0,
             "escalated_left_open": 0, "unhandled_left_open": 0}
    unhandled: set[str] = set()

    for family, source_key in FAMILIES.items():
        signatures = (document.get("families", {}).get(_signature_family(family)) or {}).get("signatures") or []
        for signature in signatures:
            if signature.get("status") != "RESOLVED":
                continue
            caption, layout = signature.get("caption"), signature.get("layout")
            if not caption or not layout:
                continue
            table_key = f"{caption}|{layout}"
            keys = signature.get("keys") or []
            semantics = signature.get("semantics") or []
            published = {str(name) for name in semantics}
            for position, (column, semantic) in enumerate(zip(keys, semantics)):
                semantic = str(semantic)
                key = row_key(source_key, table_key, str(column))
                if key not in dossier_keys:
                    continue
                receipt = _RECEIPT.format(sig=table_key, position=position,
                                          semantic=semantic)
                if semantic == "def_fum_rec":
                    receipt += ("; D:/league-history-data/nfl/derived/validation/"
                                "sota_recon_master/"
                                "nflcom_player_logs_all_years_weekly_witness.json -- "
                                "date-keyed weekly candidate matrix: regular/post "
                                "opponent-recovery wins the crossed own-recovery "
                                "candidate on informative rows; v26 has no PRE "
                                "season_type")
                if family == "player_logs_targeted":
                    receipt += ("; docs/nflcom-targeted-cache-reparse-receipt.json -- "
                                "PASS, local retained-cache reparse, exact _layout")
                if family in {"player_logs", "player_logs_targeted"} \
                        and semantic in PLAYER_LOGS_KEY_EXCLUDED:
                    decisions.append({
                        "key": key, "disposition": "EXCLUDED_WITH_REASON",
                        "reason": PLAYER_LOGS_KEY_EXCLUDED[semantic],
                        "evidence": receipt,
                    })
                    tally["excluded"] += 1
                    continue
                if family in {"player_logs_targeted", "player_career"} and semantic == "week":
                    decisions.append({
                        "key": key, "disposition": "EXCLUDED_WITH_REASON",
                        "reason": PLAYER_LOGS_KEY_EXCLUDED[semantic],
                        "evidence": receipt,
                    })
                    tally["excluded"] += 1
                    continue
                if (family == "player_career" and layout.startswith("id_recent:")
                        and semantic in {"games", "games_started"}):
                    # Existing appearance-field backlog; this correspondence pass only
                    # adjudicates the resolved stat blocks.
                    continue
                if (family == "player_career" and layout.startswith("id_recent:")
                        and semantic in PLAYER_CAREER_OWNERSHIP_CANDIDATES):
                    decisions.append({
                        "key": key,
                        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                        "reason": PLAYER_CAREER_OWNERSHIP_CANDIDATES[semantic],
                        "evidence": receipt,
                    })
                    tally["new"] += 1
                    continue
                if semantic in ESCALATED:
                    tally["escalated_left_open"] += 1
                    continue
                if semantic in MAPPED:
                    decisions.append({
                        "key": key, "disposition": "MAPPED_TO_CANONICAL",
                        "canonical": MAPPED[semantic],
                        "reason": f"the site's {semantic!r} in a receipted "
                                  f"{layout.split(':')[-1]} block; v26 carries the same "
                                  f"material as {MAPPED[semantic]!r}",
                        "evidence": receipt,
                    })
                    tally["mapped"] += 1
                elif semantic in DERIVED_RATIOS:
                    operands = DERIVED_RATIOS[semantic]
                    if not set(operands) <= published:
                        # the operands are NOT on this page, so the ratio is the only
                        # form the number appears in -- real material, not a derivation
                        decisions.append({
                            "key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                            "reason": f"{semantic}: declared derivable from "
                                      f"{operands}, but THIS signature publishes "
                                      f"neither operand, so the ratio is the only form "
                                      f"the value appears in",
                            "evidence": receipt,
                        })
                        tally["new"] += 1
                        continue
                    decisions.append({
                        "key": key, "disposition": "EXCLUDED_WITH_REASON",
                        "reason": f"derived ratio; MEASURED on this signature: its "
                                  f"operands {operands} are both published in the same "
                                  f"header, so the row carries no material they do not. "
                                  f"It is an equation check, not a stored cell",
                        "evidence": receipt,
                    })
                    tally["derived"] += 1
                elif semantic in NEW_CANDIDATES:
                    decisions.append({
                        "key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                        "reason": NEW_CANDIDATES[semantic],
                        "evidence": receipt,
                    })
                    tally["new"] += 1
                elif semantic in EXCLUDED:
                    decisions.append({
                        "key": key, "disposition": "EXCLUDED_WITH_REASON",
                        "reason": EXCLUDED[semantic],
                        "evidence": receipt,
                    })
                    tally["excluded"] += 1
                else:
                    unhandled.add(semantic)
                    tally["unhandled_left_open"] += 1

    # ---- the caption that did not parse ------------------------------------------
    # `nflcom_player_career` emits 48 rows under a BLANK caption, and the dossier keeps
    # them under a bare key on purpose so un-censused material cannot vanish. That is
    # the right default and here it was costing 47 OPEN columns -- 8.7% of the whole
    # remaining queue -- so the counter was clean about coverage while the denominator
    # beneath it was one page.
    #
    # MEASURED, not read off the names:
    #   48 blank-caption rows of 637,134 (0.01%), ALL from one slug, `blake-hance`
    #   an occupancy scan over those 48 rows finds FIVE columns non-empty; 42 are empty
    #   on every row, so excluding them discards nothing
    # The five keep whatever the correspondence table already says, so a blank-caption
    # row can never disagree with the same column under a real caption.
    for column, occupied in sorted(BLANK_CAPTION_OCCUPANCY.items()):
        key = row_key(BLANK_CAPTION_SOURCE, "", column)
        if key not in dossier_keys:
            continue
        semantic = BLANK_CAPTION_SEMANTICS.get(column)
        if semantic in {"games", "games_started", "week"}:
            # The blank-caption table has no receipted layout; these season-grain
            # appearance fields remain the existing backlog.
            continue
        receipt = (f"occupancy scan over the {BLANK_CAPTION_ROWS} blank-caption rows of "
                   f"{BLANK_CAPTION_SOURCE} ({BLANK_CAPTION_TOTAL:,} rows total, one "
                   f"slug {BLANK_CAPTION_SLUG!r}): {column!r} non-empty on {occupied}")
        if occupied == 0:
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"empty on every one of the {BLANK_CAPTION_ROWS} rows that "
                          f"carry this key. The column is DECLARED by the parquet schema "
                          f"and never written under this caption, so the exclusion "
                          f"discards nothing -- the same argument the all-zero legacy "
                          f"columns got, made on occupancy rather than on the name",
                "evidence": receipt})
            tally["excluded"] += 1
        elif semantic in MAPPED:
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL",
                "canonical": MAPPED[semantic],
                "reason": f"the site's {semantic!r}, carried on the unparsed caption. "
                          f"Same disposition as every keyed signature that publishes it",
                "evidence": receipt})
            tally["mapped"] += 1
        elif semantic in EXCLUDED:
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": EXCLUDED[semantic],
                "evidence": receipt})
            tally["excluded"] += 1
        else:
            unhandled.add(str(semantic))
            tally["unhandled_left_open"] += 1

    # DEDUPE: one dossier row can be reached from several signatures (the same caption +
    # layout pair appears once, but player_career's Recent Games captions repeat). Last
    # write would win silently, so a genuine disagreement must surface instead.
    by_key: dict[str, dict] = {}
    conflicts: list[str] = []
    for decision in decisions:
        seen = by_key.get(decision["key"])
        if seen and (seen["disposition"], seen.get("canonical")) != (
                decision["disposition"], decision.get("canonical")):
            conflicts.append(
                f"{decision['key']}: {seen['disposition']}/{seen.get('canonical')} vs "
                f"{decision['disposition']}/{decision.get('canonical')}")
        by_key[decision["key"]] = decision
    if conflicts:
        raise SystemExit("two signatures disagree about the same dossier row:\n  "
                         + "\n  ".join(conflicts))
    return sorted(by_key.values(), key=lambda d: d["key"]), tally | {
        "unhandled_semantics": sorted(unhandled)}


GENERATOR = "scripts.sota_recon.nflcom_column_adjudication"


def escalated_row_keys() -> dict[str, str]:
    """Dossier row key -> the question that stopped the decision. Walks the same RESOLVED
    signatures `build_decisions` walks, so the two cannot drift apart."""
    document = json.loads(NFLCOM_SEMANTICS.read_text(encoding="utf-8"))
    open_rows = {
        row_key(row["source"], row["table_key"], row["column"])
        for row in json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))["rows"]
        if row["disposition"] == "OPEN"}
    out: dict[str, str] = {}
    for family, source_key in FAMILIES.items():
        signatures = (document.get("families", {}).get(_signature_family(family)) or {}).get("signatures") or []
        for signature in signatures:
            if signature.get("status") != "RESOLVED":
                continue
            caption, layout = signature.get("caption"), signature.get("layout")
            if not caption or not layout:
                continue
            for column, semantic in zip(signature.get("keys") or [],
                                        signature.get("semantics") or []):
                if str(semantic) not in ESCALATED:
                    continue
                key = row_key(source_key, f"{caption}|{layout}", str(column))
                if key in open_rows:
                    out[key] = ESCALATED[str(semantic)]
    return out


def apply_to_ledger(decisions: list[dict], generated_by: str = GENERATOR) -> dict[str, int]:
    """Merge into the ledger WITHOUT clobbering a hand decision.

    An adjudication beats a bulk rule, and a hand adjudication beats a generated one --
    otherwise re-running this module would quietly revert a human who looked at a column
    and disagreed with the table."""
    # `__name__` is "__main__" under `python -m`, and a generator that stamps that can
    # never recognise its own earlier output: the ownership check below reads every one
    # of its decisions as HAND-WRITTEN and refuses to update them, while reporting
    # success. That is exactly what happened to the statscrew pass -- it declined to
    # revise 114 of its own rows twice, and printed a clean result both times.
    if not generated_by or "." not in generated_by:
        raise ValueError(
            f"generated_by={generated_by!r} is not a stable module path. Pass the "
            "module's GENERATOR constant, never __name__ -- under `python -m` that is "
            "'__main__', and the ownership check silently stops recognising this "
            "generator's own decisions")
    ledger = json.loads(DISPOSITIONS_PATH.read_text(encoding="utf-8"))
    existing = {entry["key"]: entry for entry in ledger.get("decisions", [])}
    added = kept = 0
    for decision in decisions:
        previous = existing.get(decision["key"])
        if previous is not None and previous.get("generated_by") != generated_by:
            kept += 1
            continue
        # Generated decisions may carry separately receipted audit-capacity fields.
        # A mapping revision must replace the generator-owned adjudication, but must not
        # erase a measurement receipt that another lane attached to that same key.
        merged = dict(previous or {})
        same_core = (previous is not None
                      and previous.get("disposition") == decision.get("disposition")
                      and previous.get("canonical") == decision.get("canonical")
                      # Generated evidence includes the live contract's aggregation,
                      # unit, and grain.  A contract correction must refresh that
                      # receipt even when the disposition/canonical target is unchanged.
                      and previous.get("evidence") == decision.get("evidence"))
        merged.update(decision)
        if not same_core and "canonical" not in decision:
            # A withdrawal is a DELETED row semantically: do not leave the old target
            # attached to an EXCLUDED/CANDIDATE disposition.
            merged.pop("canonical", None)
        if same_core:
            # A rerun may have a shorter generic receipt than a later adjudication
            # attached to this same generated row. Keep the established explanation and
            # evidence unless the semantic disposition actually changed.
            for field in ("reason", "evidence"):
                if field in previous:
                    merged[field] = previous[field]
        merged["generated_by"] = generated_by
        existing[decision["key"]] = merged
        added += 1
    ledger["decisions"] = sorted(existing.values(), key=lambda d: d["key"])
    DISPOSITIONS_PATH.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    return {"written": added, "hand_decisions_preserved": kept,
            "ledger_total": len(ledger["decisions"])}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="write the decisions into the adjudication ledger")
    args = parser.parse_args()

    problems = _validate_table(_v26_columns())
    if problems:
        print("CORRESPONDENCE TABLE INVALID:")
        for problem in problems:
            print("  -", problem)
        return 1

    decisions, tally = build_decisions()
    duplicates = duplicate_decisions()
    aliases = alias_decisions()
    decisions = decisions + duplicates + aliases
    tally["duplicate_of_cached_passing_table"] = len(duplicates)
    tally["duplicate_of_long_alias_column"] = len(aliases)
    print(f"decisions: {len(decisions):,}")
    for name, value in tally.items():
        print(f"  {name:24s} {value}")
    if tally["unhandled_semantics"]:
        print("\nUNHANDLED semantics (left OPEN, add them to the table or escalate):")
        for name in tally["unhandled_semantics"]:
            print("   -", name)
    if args.apply:
        print("\n", apply_to_ledger(decisions))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
