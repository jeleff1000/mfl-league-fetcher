from __future__ import annotations

from pathlib import Path

from . import closure_chain as CC
from .composite_witness_lane import InternalResolution, LaneObservation
from .closure_chain import COMPOSITE_RECEIPT, receipt_obligations


def test_failed_observations_never_enter_external_scalar_depth():
    scalar, constraints, internal = receipt_obligations(COMPOSITE_RECEIPT)

    assert scalar == ()
    assert constraints == ()
    assert internal == ()


def test_obligation_types_are_closed_lane_dataclasses():
    scalar, constraints, internal = receipt_obligations(COMPOSITE_RECEIPT)

    assert all(observation.kind == "SCALAR_OBSERVATION" for observation in scalar)
    assert all(observation.kind == "CONSTRAINT_OBSERVATION" for observation in constraints)
    assert all(resolution.targets != ("allpro",) for resolution in internal)


def test_build_excludes_pending_nflcom_scalars_but_admits_eligible_pfr(monkeypatch):
    scalar = (
        LaneObservation(
            "pfr-awards-v1", "pfr_all_pro_members", "SCALAR_OBSERVATION",
            ("allpro",), 1, 0, "PASS", 1,
        ),
        LaneObservation(
            "nflcom-l7-fg-buckets-v1", "nflcom_player_splits", "SCALAR_OBSERVATION",
            ("fg_made_0_19",), 1, 0, "PASS", 1,
        ),
    )
    constraints = (
        LaneObservation(
            "pfr-field-goals-50-plus-v1", "pfr_player_kicking",
            "CONSTRAINT_OBSERVATION", ("fg_made_50_59", "fg_made_60_"),
            1, 0, "PASS", 1,
        ),
    )
    internal = (
        InternalResolution(
            "pfr-awards-v1", "player_bio", ("hof",), 1, 0, "PASS", 1,
        ),
    )
    monkeypatch.setattr(CC, "receipt_obligations", lambda _path: (scalar, constraints, internal))

    depth = CC.build()["stage_3_depth"]

    # 2026-07-30: nflcom_player_splits now appears here, and that is the filter working
    # rather than leaking. This dict admits a scalar observation only from a source that is
    # NOT mapping-pending, and splits left that queue by acquiring 39 real table-scoped
    # MapSpecs. Its L7 field-goal BUCKET columns remain OPEN in the dossier and unmapped --
    # the specs target fgm and fg_att -- so nothing held out by the receipt's
    # does_not_license list has been admitted; the composite spec nflcom-l7-fg-buckets-v1
    # is still one of the five blocked groups and is gated separately.
    assert depth["composite_scalar_sources_per_column"] == {
        "allpro": ["pfr_all_pro_members"],
        "fg_made_0_19": ["nflcom_player_splits"],
    }
    assert depth["external_constraint_observations"] == 1
    assert depth["external_constraint_target_columns"] == 2
    assert depth["internal_resolution_observations"] == 1
    assert depth["internal_resolution_target_columns"] == 1
