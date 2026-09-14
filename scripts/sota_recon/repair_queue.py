"""A DIAGNOSED REPAIR THAT NOTHING COUNTS IS A REPAIR THAT WILL NEVER HAPPEN.

Joe, 2026-07-31, on the 2025 team-DEF gap: "ok so we have that fully wired as something
that needs a backfill?" The honest answer was NO. Every adjudication carried a `remedy`
and a `remedy_applied` flag, and a grep proved NOTHING READ EITHER ONE -- the only two
hits in the whole tree were a docstring and a test comment. The ledger was a filing
cabinet, not a queue.

THE DENOMINATOR WAS ALSO WRONG, in the flattering direction. This programme kept saying
"nine deferred repairs" in conversation, because nine were what had been discussed out
loud. The ledger held FORTY unapplied remedies. A number carried in prose instead of
derived from the ledger drifts toward the part of the work that was memorable.

WHO OWES THE WORK IS THE SPLIT THAT MATTERS. "40 repairs" is not one queue, because the
faults do not describe one kind of labour, and lumping them exaggerates what we owe:

    SUPERTABLE_GAP / SUPERTABLE_VALUE  -> OUR DATA. a backfill or a rebuild. promote-class.
    MAPPING_DEFECT                     -> OUR CODE. a MapSpec/licence fix, cheap, in-repo.
    SOURCE_DEFECT / NAME_COLLISION     -> NOT OURS. the source is wrong or renders another
    DEFINITION_DIFFERS                    page; there is nothing here to backfill.
    UNADJUDICATED                      -> NOT A REPAIR AT ALL. a decision still owed.

REPAIRS THAT GROW ARE A DIFFERENT CLASS. Most of these are settled historical holes and
sitting on them costs nothing new. The 2025 team-DEF tackle gap is a WEEK-9 CUTOVER in
the current season: weeks 1-8 complete, weeks 10-18 absent on every team. Every further
week lands with the same four columns null. A queue that cannot say which of its rows is
still bleeding will let the bleeding one age like the rest, so `growing` is declared per
row and counted separately.

WHY THIS IS A QUEUE AND NOT A GATE. Joe deferred these deliberately -- "as long as its
gated and locked in as we finalize more witnesses we dont haveto backfillyet". Deferral
is a decision, and gating a decision to zero would just force it to be un-made. What is
NOT deferrable is honesty about the count, and one thing IS gated to zero: you may not
claim a remedy landed without a receipt (§19 proof-or-pending). `remedy_applied: true`
with no `remedy_receipt` is how a backlog empties itself without any data changing.

    python -m scripts.sota_recon.repair_queue
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

CONTRACT = Path(__file__).parent / "witness_gate" / "contracts" / \
    "disagreement_adjudications.v1.json"
_R = Path(__file__).parent


def _txt(p: str) -> str:
    return (_R / p).read_text(encoding="utf-8")


def _decisions() -> list[dict]:
    """The dispositions ledger, read by its REAL key.

    This cost a false PASS. The first version of the helper read `dispositions` or `rows`;
    the contract's key is `decisions`. `.get()` returned an EMPTY LIST, so every absence
    check below answered "yes, absent" about a ledger it had never seen, and four
    withdrawal receipts verified against nothing at all. An absence proved over an empty
    denominator is not evidence -- it is the shape of evidence.
    """
    d = json.loads(_txt("witness_gate/contracts/column_dispositions.v1.json"))
    rows = d.get("decisions")
    if not isinstance(rows, list) or not rows:
        raise ValueError("column_dispositions.v1.json: no `decisions` -- an absence "
                         "check over an empty ledger cannot receipt anything")
    return rows


def _matches(source: str, column: str, v26_col: str) -> list[dict]:
    """Decision rows for one (source, column) -> canonical, keyed the way the ledger is.

    SCOPING TO THE SOURCE IS NOT OPTIONAL and the second bug here proves it. The fields
    are `key` ('source|table_key|column') and `canonical` -- not `source_column`/`v26_col`,
    which matched nothing. And `solo -> def_tackles_solo` has NINETEEN live rows, every one
    belonging to nflcom_player_logs / _targeted, a different source still blocked at
    capture. An unscoped absence check would read those and declare a withdrawal that did
    happen to have failed.
    """
    return [r for r in _decisions()
            if r["key"].split("|")[0] == source and r["key"].split("|")[-1] == column
            and r.get("canonical") == v26_col]


def _no_disposition(source: str, column: str, v26_col: str) -> bool:
    """A WITHDRAWAL IS A DELETED ROW, so the receipt is an ABSENCE.

    The ledger holds decisions and OPEN is the absence of one, which means a withdrawn
    mapping cannot be receipted by a row saying "withdrawn" -- that would be a decision.
    It is receipted by proving the decision is GONE and the dossier row has returned to
    OPEN. `_decisions()` raises rather than returning [], so this cannot pass vacuously.
    """
    return not _matches(source, column, v26_col)


def _has_disposition(source: str, column: str, v26_col: str) -> bool:
    """The mirror: a remedy that ADJUDICATED something is receipted by the row EXISTING."""
    return bool(_matches(source, column, v26_col))


#: A RECEIPT THAT IS PROSE ROTS SILENTLY; A RECEIPT THAT IS A PREDICATE CANNOT.
#: Each applied remedy names an id here, and the gate RE-RUNS the check every time. If a
#: fix is reverted the receipt goes false and the gate fails -- so the backlog cannot be
#: emptied by editing a boolean, and a landed repair cannot quietly un-land either.
RECEIPT_CHECKS = {
    "published_filters_on_cast":
        lambda: "TRY_CAST" in _txt("nflcom_column_audit.py").split("def published")[1][:600],
    "pm1_band_kept_beside_equality":
        lambda: "agree_pct_pm1" in _txt("nflcom_column_audit.py"),
    "axis_lost_declared_per_table":
        lambda: "axis_lost_tables" in _txt("nflcom_column_audit.py")
        and "REFUSED_NO_KEY_OVERLAP" in _txt("nflcom_column_audit.py"),
    "recent_games_recovery_module_exists":
        lambda: (_R / "nflcom_recent_games_recovery.py").exists(),
    # BOTH HALVES OR NEITHER. The remedy withdrew `solo` AND adjudicated `tkl` in its
    # place; receipting only the withdrawal would let the column end up mapped to nothing.
    "solo_withdrawn_and_tkl_adjudicated":
        lambda: _no_disposition("nflcom_player_career", "solo", "def_tackles_solo")
        and _has_disposition("nflcom_player_career", "tkl", "def_tackles_solo"),
    "ret_to_kickoff_returns_withdrawn":
        lambda: _no_disposition("nflcom_player_career", "ret", "kickoff_returns"),
    "ret_to_punt_return_yards_withdrawn":
        lambda: _no_disposition("nflcom_player_career", "ret", "punt_return_yards"),
    "fr_to_fumble_recovery_opp_adjudicated":
        lambda: _has_disposition("nflcom_player_season", "fr", "fumble_recovery_opp"),
    # "No change needed" is the hardest remedy to receipt, because the evidence is that the
    # code ALREADY does the thing. The predicate asserts the two clauses whose absence I
    # wrongly alleged: dedupe before aggregation, and aggregation across a player's team rows.
    # BOUND THE WINDOW BY STRUCTURE, NOT BY A MAGIC NUMBER. The first draft of this check
    # took [:2000] chars after the branch and the two clauses sit at offsets 1837 and 2089,
    # so it cut between them and reported FAIL on code that was correct -- the third
    # off-by-window false negative in one session (see `published_filters_on_cast`, which
    # used [:400] against a match at 600). Slice to the NEXT branch instead.
    "harness_nflcom_shape_dedupes_and_sums":
        lambda: (lambda blk: "SELECT DISTINCT *" in blk and "GROUP BY 1, 2" in blk)(
            _txt("witness_map.py").split('spec.shape == "nflcom"')[1]
            .split("if spec.shape ==")[0]),
    "tackle_population_scoped":
        lambda: "defensive_players" in _txt("nflcom_population_scoped_audit.py"),
    # TWO CLAUSES, NOT ONE SUBSTRING. The remedy was "divide by the non-zero base", and a
    # predicate that only checked the variable exists would still pass if the division were
    # reverted to len(out) -- which is precisely the defect, since the inflated figure and the
    # honest one differ only in the denominator.
    "team_lane_divides_by_nonzero_base":
        lambda: (lambda t: "base_nonzero=base" in t.replace(" ", "")
                 and "(base-len(flagged))/base" in t.replace(" ", ""))(
            _txt("recon_nflcom_team_conservation.py")),
    "team_lane_keymap_deterministic":
        lambda: "ORDER BY ABS(n.v - m.v), m.fid" in
        _txt("recon_nflcom_team_conservation.py"),
    # "Do not promote it" is a claim with a checkable shape: the supplementary table must
    # appear in NO floor. Guarded on a populated contract so the absence is not vacuous.
    "recent_games_not_licensed":
        lambda: bool(json.loads(_txt("witness_gate/contracts/witness_licence_floors.v1.json"))
                     .get("floors"))
        and "Recent Games" not in _txt("witness_gate/contracts/witness_licence_floors.v1.json"),
    # A remedy of "none required" is still a claim, and its receipt is that the thing it
    # declined to change is still standing.
    "unscoped_floors_stand":
        lambda: "population_scoped_floors" in _txt("witness_gate/contracts/"
                                                   "witness_licence_floors.v1.json"),
}

#: fault -> (owner class, does this imply data we must repair?)
OWNERS = {
    "SUPERTABLE_GAP":     ("supertable_backfill", True),
    "SUPERTABLE_VALUE":   ("supertable_backfill", True),
    "MAPPING_DEFECT":     ("mapping_fix", True),
    "SOURCE_DEFECT":      ("no_repair_owed", False),
    "NAME_COLLISION":     ("no_repair_owed", False),
    "DEFINITION_DIFFERS": ("no_repair_owed", False),
    "UNADJUDICATED":      ("decision_owed", False),
}


def load() -> list[dict]:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))["adjudications"]


def _v26_columns() -> set[str]:
    """v26's physical schema, read from the parquet footer (no scan)."""
    import duckdb

    from . import sources as S
    v26 = Path(S.latest_v26()).as_posix()
    con = duckdb.connect()
    try:
        return {c[0] for c in
                con.execute(f"DESCRIBE SELECT * FROM read_parquet('{v26}')").fetchall()}
    finally:
        con.close()


def _repair_class(v26_col, schema: set[str] | None) -> str:
    """ADD A COLUMN AND BACKFILL A COLUMN ARE NOT THE SAME JOB.

    Joe's question was "does this need a backfill", and answering it off a single
    SUPERTABLE_GAP count would have overstated the backfill list: some of those rows say
    the canonical DOES NOT EXIST YET (def_interception_long, punt_touchbacks), which is a
    promote, not a load. The split is derived from the SCHEMA rather than from the remedy's
    prose, because prose is not a denominator. Where the ledger names no parseable column
    the row is `unclassified` and says so -- guessing here is how a backlog acquires
    imaginary precision.

    ONE GRAIN IS ALL THIS PROVES. `latest_v26()` is the WEEKLY release and it is the only
    v26 accessor `sources` exposes, so "absent" here means absent FROM THE WEEKLY SCHEMA --
    never "does not exist". That distinction has already bitten once: games_played /
    games_started read as a missing column when they were only missing at week grain, and
    Joe had to correct it ("we track games and games started in our tables though"). The
    counter is named for what it measured.
    """
    if schema is None:
        return "unknown_schema_unavailable"
    toks = [t.strip() for t in str(v26_col).replace("/", " ").split()
            if t.strip().isidentifier()]
    if not toks:
        return "unclassified"
    return "backfill" if all(t in schema for t in toks) else "absent_from_weekly_schema"


def build(schema: set[str] | None = None) -> dict:
    rows = load()
    if schema is None:
        try:
            schema = _v26_columns()
        except Exception:
            schema = None  # classification degrades to `unknown`, it never guesses
    out, unknown_fault, no_status, applied_without_receipt = [], [], [], []
    for r in rows:
        fault = r.get("fault", "")
        owner, owes = OWNERS.get(fault, (None, False))
        key = f"{r.get('source','?')}|{r.get('table_key','?')}|{str(r.get('v26_col'))[:60]}"
        if owner is None:
            unknown_fault.append(key)
            continue
        if "remedy_applied" not in r:
            no_status.append(key)
            continue
        applied = bool(r["remedy_applied"])
        # PROOF-OR-PENDING. A backlog that can empty itself by editing a boolean is not a
        # backlog. Claiming the repair landed requires evidence it landed -- and the
        # evidence is RE-RUN here, so an unknown receipt id and a receipt whose check has
        # gone false are the same failure.
        if applied:
            rid = r.get("remedy_receipt")
            chk = RECEIPT_CHECKS.get(rid)
            if chk is None or not chk():
                applied_without_receipt.append(f"{key} [receipt={rid!r}]")
        out.append({"key": key, "fault": fault, "owner": owner, "owes_repair": owes,
                    "applied": applied, "growing": bool(r.get("live_defect")),
                    "repair_class": (_repair_class(r.get("v26_col"), schema)
                                     if owner == "supertable_backfill" else None),
                    "remedy": r.get("remedy", ""), "v26_col": r.get("v26_col")})
    outstanding = [r for r in out if not r["applied"]]
    by_owner = Counter(r["owner"] for r in outstanding)
    by_class = Counter(r["repair_class"] for r in outstanding
                       if r["owner"] == "supertable_backfill")
    return {
        "rows": out,
        "counters": {
            "adjudications": len(rows),
            "remedies_outstanding": len(outstanding),
            "repairs_we_owe": sum(1 for r in outstanding if r["owes_repair"]),
            "supertable_backfills_outstanding": by_owner.get("supertable_backfill", 0),
            "of_those_backfill_an_existing_column": by_class.get("backfill", 0),
            "of_those_absent_from_the_weekly_v26_schema":
                by_class.get("absent_from_weekly_schema", 0),
            "of_those_unclassified": by_class.get("unclassified", 0),
            "mapping_fixes_outstanding": by_owner.get("mapping_fix", 0),
            "no_repair_owed": by_owner.get("no_repair_owed", 0),
            "decisions_owed": by_owner.get("decision_owed", 0),
            "repairs_still_growing": sum(1 for r in outstanding if r["growing"]),
            # zero-gates
            "adjudications_with_unknown_fault": len(unknown_fault),
            "adjudications_missing_remedy_status": len(no_status),
            "remedy_applied_without_receipt": len(applied_without_receipt),
        },
        "detail": {"unknown_fault": unknown_fault, "missing_status": no_status,
                   "applied_without_receipt": applied_without_receipt,
                   "growing": [r["key"] for r in outstanding if r["growing"]]},
    }


def main() -> int:
    rep = build()
    c = rep["counters"]
    for k, v in c.items():
        print(f"{k:44s} {v:>5}")
    print("\n=== repairs still GROWING (current-season, cost rises weekly) ===")
    for r in rep["rows"]:
        if r["growing"] and not r["applied"]:
            print(f"  {r['fault']:18s} {str(r['v26_col'])[:70]}")
    print("\n=== supertable backfills outstanding ===")
    for r in rep["rows"]:
        if r["owner"] == "supertable_backfill" and not r["applied"]:
            print(f"  {r['fault']:18s} {str(r['v26_col'])[:70]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
