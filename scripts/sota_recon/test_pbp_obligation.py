"""The pbp-default gate: not witnessing pbp requires a receipted CANNOT or an OWED debt."""
from __future__ import annotations

from pathlib import Path

import duckdb

from . import sources as S
from .pbp_obligation import CANNOT, OWED, PBP_SOURCES
from .witness_map import WITNESS_MAP
from .witness_pivot import category

SKIP_CATS = {"scoring_derived", "rank_derived", "audit_stamp", "identifier", "bio"}


def _weekly_witnessable() -> list[str]:
    con = duckdb.connect()
    try:
        cols = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{Path(S.latest_v26()).as_posix()}')"
        ).fetchall()]
    finally:
        con.close()
    return [c for c in cols if category(c) not in SKIP_CATS]


def test_every_witnessable_column_is_pbp_witnessed_cannot_or_owed():
    """Joe's law: pbp witnesses by default. A column outside all three states is
    an unclassified arrival and fails the gate until someone writes its truth."""
    witnessed = {s.v26_col for s in WITNESS_MAP if s.source_key in PBP_SOURCES}
    unclassified = [c for c in _weekly_witnessable()
                    if c not in witnessed and c not in CANNOT and c not in OWED]
    assert not unclassified, f"unclassified for the pbp obligation: {unclassified}"


def test_owed_is_shrink_only():
    """OWED may only shrink -- with one legal exception: a LAW change that
    dissolves a CANNOT class moves its entries INTO the debt (2026-08-01: Joe
    dissolved RATE_VIA_COMPONENTS, +20 rates became owed while 5 were specced
    on the spot and gwfg left through specs; 58 -> 74 by ruling, receipted).
    From 74, only specs and ruled drops lower it. THE COMB (2026-08-01) landed
    36 specs in one pass and OWED now auto-closes against the live map -- the
    residual queue after the comb is 38 and may only fall."""
    assert len(OWED) <= 38, (
        f"OWED grew to {len(OWED)} -- new debt needs a spec or a CANNOT with a "
        "real reason, not a bigger queue")


def test_cannot_and_owed_do_not_overlap_or_shadow_witnessed():
    witnessed = {s.v26_col for s in WITNESS_MAP if s.source_key in PBP_SOURCES}
    both = set(CANNOT) & set(OWED)
    assert not both, f"in both CANNOT and OWED: {sorted(both)}"
    shadowed = (set(CANNOT) | set(OWED)) & witnessed
    assert not shadowed, (
        f"already pbp-witnessed yet still declared: {sorted(shadowed)} -- "
        "close the ledger entry")


def test_every_owed_entry_names_its_machinery():
    empty = [c for c, why in OWED.items() if not why.strip()]
    assert not empty, empty
