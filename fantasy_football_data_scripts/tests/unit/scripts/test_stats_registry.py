"""Enforce "we either know the formula or we don't" in CI.

Wraps scripts/check_stats_registry.py so every PR fails if an advanced atom ships
without a registered, cited formula, or if a BLOCKED (formula=null) metric leaks into
the build. Includes a negative test proving the guard bites.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

import scripts.check_stats_registry as chk  # noqa: E402


def test_every_advanced_atom_has_a_known_formula():
    # main() returns 0 only when all advanced STAT_COLUMNS map to a non-null-formula
    # registry entry and no blocked metric is emitted.
    assert chk.main() == 0


def test_guard_rejects_blocked_metric(monkeypatch):
    monkeypatch.setattr(chk, "STAT_COLUMNS", list(chk.STAT_COLUMNS) + ["xfp"])
    assert chk.main() == 1


def test_guard_rejects_unregistered_advanced_atom(monkeypatch):
    monkeypatch.setattr(chk, "STAT_COLUMNS", list(chk.STAT_COLUMNS) + ["passing_made_up_epa"])
    assert chk.main() == 1
