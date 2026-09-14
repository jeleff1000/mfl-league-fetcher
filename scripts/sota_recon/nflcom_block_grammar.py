"""
sota_recon/nflcom_block_grammar.py -- O.9.0 phase 2: turn header SIGNATURES into SEMANTICS.

Phase 1 (`nflcom_column_semantics --census`) recovered, from the retained cache, the ordered raw header
sequence behind every stored nflcom column. This module assigns each position its canonical meaning.

WHY NOT GREEDY MATCHING. NFL.com stat blocks are ambiguous under a bare left-to-right longest-match:
`ATT YDS AVG TD` is a RUSHING block on an RBFB page but sits AFTER the receiving block on a WRTE page,
and `YDS` alone is passing / rushing / receiving / interception-return yards depending only on which
block it fell in. A greedy segmenter would silently pick one and be wrong for a whole position class.

THE RULE INSTEAD: declare candidate LAYOUTS (ordered block sequences) and require that EXACTLY ONE
reproduces the observed header signature EXACTLY. Every signature ends in a terminal disposition:

    RESOLVED     exactly one layout reproduces the signature  -> every column gets a semantic name
    NAMED        family's headers are self-describing ('Pass Yds', 'Yds/Att') and no header repeats
                 inside the signature -- MEASURED, so `_dedup` mints no `_2` and there is no parse
                 artifact to undo. Not the same as mapped: 'Yds' under defense/passing is yards
                 ALLOWED, and picking its canonical target is dossier adjudication (O.9.3).
    QUARANTINED  family carries the O.9.0 COLUMN-SHIFT defect (values sit one column left of their
                 name) -- labelling it would hand out confident names for shifted data
    AMBIGUOUS    two or more layouts reproduce it             -> queued, never guessed
    UNRESOLVED   no layout reproduces it                      -> queued, never guessed

That is proof-or-pending (§19) applied to column semantics: the layout must REGENERATE the evidence,
so a wrong block library fails loudly instead of mislabelling a stat. Nothing here asserts a mapping
into our super-table -- that is the dossier's job (O.9.3); this only says what NFL.com's column IS.

    python -m scripts.sota_recon.nflcom_block_grammar --resolve
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

CENSUS = Path(__file__).resolve().parents[2] / "docs" / "nflcom-column-signature-census.json"
OUT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-column-semantics.json"


def N(h: str) -> str:
    """Normalize a raw header cell for matching (NFL.com varies case/spacing across eras)."""
    return " ".join(h.upper().replace(".", "").split())


# ---------------------------------------------------------------------------------------------
# BLOCK LIBRARY -- each block is (ordered raw headers, ordered semantic names).
# Semantic names are NFL.com-side names (what the site's column means), NOT v26 canonical columns.
# ---------------------------------------------------------------------------------------------
def B(name: str, headers: str, semantics: str) -> tuple:
    h = [N(x) for x in headers.split("|")]
    s = semantics.split("|")
    assert len(h) == len(s), f"block {name}: {len(h)} headers vs {len(s)} semantics"
    return (name, tuple(h), tuple(s))


BLOCKS = [
    # ---- row-identity prefixes (context, never stat values) ----
    B("id_career", "SEASON|TEAM|G|GS", "season|team|games|games_started"),
    B("id_gamelog", "WK|GAME DATE|OPP|RESULT", "week|game_date|opponent|result"),
    # career pages append a 'Recent Games' table -- game grain, but NO Game Date column
    B("id_recent", "WK|OPP|RESULT", "week|opponent|result"),
    B("games_gs", "G|GS", "games|games_started"),
    B("id_split", "G", "games"),
    # ---- offense ----
    B("passing", "COMP|ATT|YDS|AVG|TD|INT|SCK|SCKY|RATE",
      "pass_cmp|pass_att|pass_yds|pass_yds_per_att|pass_td|pass_int|sacks_suffered|sack_yds_lost|passer_rating"),
    B("passing_nosack", "COMP|ATT|YDS|AVG|TD|INT|RATE",
      "pass_cmp|pass_att|pass_yds|pass_yds_per_att|pass_td|pass_int|passer_rating"),
    B("rushing4", "ATT|YDS|AVG|TD", "rush_att|rush_yds|rush_yds_per_att|rush_td"),
    B("rushing5", "ATT|YDS|AVG|LNG|TD", "rush_att|rush_yds|rush_yds_per_att|rush_long|rush_td"),
    B("receiving5", "REC|YDS|AVG|LNG|TD", "rec|rec_yds|rec_yds_per_rec|rec_long|rec_td"),
    B("receiving4", "REC|YDS|AVG|TD", "rec|rec_yds|rec_yds_per_rec|rec_td"),
    B("fumbles2", "FUM|LOST", "fumbles|fumbles_lost"),
    # ---- defense ----
    B("defense_career", "TKL|AST|COMBINED|SOLO|SCK|SFTY|PDEF",
      "def_tackles|def_ast_tackles|def_combined_tackles|def_solo_tackles|def_sacks|def_safeties|def_pass_defended"),
    B("defense_log", "TOTAL|SOLO|AST|SCK|SFTY|PDEF",
      "def_combined_tackles|def_solo_tackles|def_ast_tackles|def_sacks|def_safeties|def_pass_defended"),
    B("int_return", "INT|YDS|AVG|LNG|TDS",
      "def_int|def_int_ret_yds|def_int_ret_avg|def_int_ret_long|def_int_ret_td"),
    B("ff_oppfr", "FF|OPP FR", "def_forced_fumbles|def_fum_rec_opp"),
    B("ff_fr", "FF|FR", "def_forced_fumbles|def_fum_rec"),
    B("fum_own_opp", "FF|OWN FR|OPP FR", "def_forced_fumbles|def_fum_rec_own|def_fum_rec_opp"),
    # ---- kicking / punting ----
    B("kick_fg", "BLK|LNG|FG ATT|FGM|PCT", "fg_blocked|fg_long|fg_att|fg_made|fg_pct"),
    B("kick_xp", "XP ATT|XPM|XPCT|XBLK", "xp_att|xp_made|xp_pct|xp_blocked"),
    # game-log kicker tables spell the same block PCT|BLK where career spells XPCT|XBLK
    B("kick_xp_log", "XP ATT|XPM|PCT|BLK", "xp_att|xp_made|xp_pct|xp_blocked"),
    B("kickoffs", "KO|AVG|TB|RET|AVG", "kickoffs|kickoff_avg|touchbacks|kickoff_ret|kickoff_ret_avg"),
    B("punting", "PUNTS|YDS|NET YDS|LNG|AVG|NET AVG|BLK|OOB|DN|IN 20|TB|FC|RET|RETY|TD",
      "punts|punt_yds|punt_net_yds|punt_long|punt_avg|punt_net_avg|punts_blocked|punts_oob|punts_downed|"
      "punts_in_20|punt_touchbacks|punt_fair_catches|punt_ret|punt_ret_yds|punt_ret_td"),
]

# Candidate LAYOUTS per (family, caption-pattern). A layout is an ordered list of block names.
# A signature resolves only if EXACTLY ONE listed layout reproduces it exactly.
# The STAT sequence is a property of the position block; the PREFIX is a property of the page
# (career table / game log / the 'Recent Games' table career pages append). They compose, so
# declare each once and pair them -- prefixes differ, so pairing adds no ambiguity.
STAT_SEQUENCES = {
    "QB": ["passing", "rushing4", "fumbles2"],
    "QB_nosack": ["passing_nosack", "rushing4", "fumbles2"],
    "RBFB4": ["rushing4", "receiving5", "fumbles2"],
    "RBFB5": ["rushing5", "receiving5", "fumbles2"],
    "WRTE": ["receiving5", "rushing5", "fumbles2"],
    "DEF_career": ["defense_career", "int_return", "ff_oppfr"],
    "DEF_log": ["defense_log", "int_return", "ff_fr"],
    "K": ["kick_fg", "kick_xp", "kickoffs"],
    "K_log": ["kick_fg", "kick_xp_log", "kickoffs"],
    "P": ["punting"],
    "OL": [],
}


def _compose(prefix: str, names: list[str], ol_needs_games: bool) -> dict[str, list[str]]:
    out = {}
    for n in names:
        seq = STAT_SEQUENCES[n]
        if n == "OL":
            seq = ["games_gs"] if ol_needs_games else []
        out[f"{prefix}:{n}"] = [prefix] + seq
    return out


_CAREER_SET = ["QB", "QB_nosack", "RBFB4", "WRTE", "DEF_career", "K", "P", "OL"]
_LOG_SET = ["QB", "QB_nosack", "RBFB5", "WRTE", "DEF_log", "K_log", "K", "P", "OL"]

# career pages carry BOTH the season table (id_career) and a 'Recent Games' table (id_recent)
CAREER_LAYOUTS = {**_compose("id_career", _CAREER_SET, ol_needs_games=False),
                  **_compose("id_recent", _LOG_SET + ["DEF_career"], ol_needs_games=True)}
LOG_LAYOUTS = _compose("id_gamelog", _LOG_SET, ol_needs_games=True)

LAYOUTS: dict[str, dict[str, list[str]]] = {
    "player_career": CAREER_LAYOUTS,
    "player_logs": LOG_LAYOUTS,
    "player_logs_targeted": LOG_LAYOUTS,
}

# Families whose values are MISLABELLED AT REST (O.9.0 column-shift defect): the stored column
# names cannot be resolved to semantics because the values sit one column left of their name.
# Resolving them would hand out confident labels for shifted data -- they stay quarantined until
# the re-parse lands. See sources.py NFLCOM_PLAYER_SPLITS / NFLCOM_PLAYER_SITUATIONAL.
QUARANTINED_FAMILIES = {"player_splits", "player_situational"}

# Families whose headers are SELF-DESCRIBING ('Pass Yds', 'Yds/Att', 'Rush 1st%') rather than the
# repeated short codes that made career/logs positionally ambiguous. No header repeats inside a
# signature, so `_dedup` mints no `_2` suffix and the stored column name already carries the
# meaning -- there is no parse artifact to undo, and a layout grammar would add nothing.
# They are NOT thereby mapped: 'Yds' under defense/passing is yards ALLOWED, and choosing the
# canonical target for each is dossier adjudication (O.9.3), not a naming question.
NAMED_HEADER_FAMILIES = {"player_season", "team_stats"}

_BY_NAME = {b[0]: b for b in BLOCKS}


def try_layout(headers: list[str], layout: list[str]) -> list[str] | None:
    """Reproduce `headers` by concatenating the layout's blocks. -> semantics, or None if it doesn't."""
    want: list[str] = []
    sem: list[str] = []
    for bn in layout:
        _, h, s = _BY_NAME[bn]
        want.extend(h)
        sem.extend(s)
    return sem if want == [N(h) for h in headers] else None


def resolve_signature(family: str, headers: list[str]) -> dict:
    if family in QUARANTINED_FAMILIES:
        return {"status": "QUARANTINED", "layout": None, "semantics": None}
    if family in NAMED_HEADER_FAMILIES:
        # MEASURED, not assumed: the no-parse-artifact claim holds only while no header repeats.
        norm = [N(h) for h in headers]
        if len(set(norm)) == len(norm):
            return {"status": "NAMED", "layout": None, "semantics": norm}
        return {"status": "UNRESOLVED", "layout": None, "semantics": None,
                "why": "repeated header in a family declared self-describing"}
    cands = LAYOUTS.get(family, {})
    hits = {name: sem for name, lay in cands.items() if (sem := try_layout(headers, lay)) is not None}
    if len(hits) == 1:
        name, sem = next(iter(hits.items()))
        return {"status": "RESOLVED", "layout": name, "semantics": sem}
    if len(hits) > 1:
        return {"status": "AMBIGUOUS", "layout": sorted(hits), "semantics": None}
    return {"status": "UNRESOLVED", "layout": None, "semantics": None}


def main() -> None:
    if not CENSUS.exists():
        raise SystemExit(f"missing {CENSUS} -- run nflcom_column_semantics --census --report first")
    census = json.loads(CENSUS.read_text(encoding="utf-8"))
    out: dict = {
        "generated": "O.9.0 phase 2: nflcom block-grammar resolution",
        "PROVEN": "SEGMENTATION only. A RESOLVED signature means exactly one declared layout "
                  "reproduces the observed header sequence exactly -- so the block BOUNDARIES "
                  "(which stat family each position belongs to) are receipted by regeneration.",
        "NOT_PROVEN": "The semantic LABELS inside each block are declared from the NFL.com column "
                      "legend, not yet value-validated against a witness. Labels become receipted "
                      "only when a family is licensed: team_stats via the team key space (O.9.1), "
                      "player families after the slug->pfr_id crosswalk (O.9.2). Until then these "
                      "names may be read but may NOT vote.",
        "KNOWN_LABEL_QUESTIONS": [
            "player_career DEF block renders TKL|AST|COMBINED|SOLO -- four tackle columns where two "
            "would do; which of TKL/COMBINED is the site's total is a label question the layout test "
            "cannot settle. Flagged for validation, not guessed away.",
        ],
        "families": {},
    }
    tot = Counter()
    print(f"{'family':<22} {'RESOLVED':>8} {'NAMED':>6} {'AMBIG':>6} {'UNRES':>6} "
          f"{'QUAR':>6}  {'rows resolved':>15}")
    for fam, blob in sorted(census["families"].items()):
        rows_res = rows_tot = 0
        recs = []
        for sig in blob["signatures"]:
            r = resolve_signature(fam, sig["headers"])
            tot[r["status"]] += 1
            rows_tot += sig["rows"]
            if r["status"] in ("RESOLVED", "NAMED"):
                rows_res += sig["rows"]
            recs.append({"caption": sig["caption"], "headers": sig["headers"], "keys": sig["keys"],
                         "rows": sig["rows"], "pages": sig["pages"], "example": sig["example"], **r})
        c = Counter(r["status"] for r in recs)
        print(f"{fam:<22} {c['RESOLVED']:>8} {c['NAMED']:>6} {c['AMBIGUOUS']:>6} "
              f"{c['UNRESOLVED']:>6} {c['QUARANTINED']:>6}  {rows_res:>10,}/{rows_tot:,}")
        # stored-key -> set of semantics it stands for (the ambiguity receipt)
        keymap: dict[str, Counter] = defaultdict(Counter)
        for r in recs:
            if r["status"] not in ("RESOLVED", "NAMED"):
                continue
            for key, sem in zip(r["keys"], r["semantics"]):
                keymap[key][sem] += r["rows"]
        out["families"][fam] = {
            "signatures": recs,
            "rows_resolved": rows_res, "rows_total": rows_tot,
            "stored_key_semantics": {k: dict(v) for k, v in sorted(keymap.items())},
            "polysemous_keys": {k: dict(v) for k, v in sorted(keymap.items()) if len(v) > 1},
        }
    out["totals"] = dict(tot)
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nsignature dispositions: {dict(tot)}")
    print(f"-> {OUT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--resolve", action="store_true")
    ap.parse_args()
    main()
