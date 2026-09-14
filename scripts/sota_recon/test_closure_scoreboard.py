"""The closure scoreboard IS a test: every zero-gate at zero, every queue
enumerated, every contract enrolled. This is the single loudest gate in the
program -- if any lane's counter regresses, the suite fails HERE even if that
lane's own tests were not run.

Run:  python -m pytest scripts/sota_recon/test_closure_scoreboard.py -q
"""

from __future__ import annotations

import glob
import os

from .closure_scoreboard import (CONTRACTS_DIR, ENROLLED_CONTRACTS,
                                 EXEMPT_CONTRACTS, build)


def test_all_zero_gates_pass():
    doc = build()
    assert doc["gate_pass"], doc["failing_gates"]


def test_every_contract_is_enrolled_or_exempt():
    on_disk = {os.path.basename(p)
               for p in glob.glob(os.path.join(CONTRACTS_DIR, "*.json"))}
    covered = set(ENROLLED_CONTRACTS) | set(EXEMPT_CONTRACTS)
    assert on_disk <= covered, sorted(on_disk - covered)


def test_enrollment_notes_are_real():
    for name, note in (*ENROLLED_CONTRACTS.items(), *EXEMPT_CONTRACTS.items()):
        assert len(note) > 10, f"{name}: enrollment needs a real description"


def test_queues_are_enumerated_counts():
    for q in build()["declared_queues"]:
        assert isinstance(q["count"], int) and q["count"] >= 0, q
