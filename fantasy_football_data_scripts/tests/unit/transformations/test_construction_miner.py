"""Tests for the roster-construction miner (how a manager builds a team)."""

import pandas as pd

from multi_league.transformations.draft.construction_miner import (
    _construction_headline,
    _LABEL_META,
    mine_construction,
    score_repeat_players,
)


def draft(manager, year, positions, costs=None):
    n = len(positions)
    costs = costs or [0] * n
    return pd.DataFrame({
        "manager_key": [manager] * n,
        "manager": [manager] * n,
        "year": [year] * n,
        "pos": positions,
        "cost": costs,
        "pick": list(range(1, n + 1)),
        "round": [(i // 4) + 1 for i in range(n)],
        "NFL_player_id": [f"{manager}_{year}_{i}" for i in range(n)],
        "player": [f"P{i}" for i in range(n)],
    })


class TestHeadlineRateFraming:
    def test_strong_identity_needs_a_real_rate(self):
        meta = _LABEL_META["zero_rb"]
        # The 2-of-16 bug: above baseline but not an identity.
        assert _construction_headline("X", meta, 2 / 16, 2, 16) is None
        # A genuine Zero-RB drafter.
        assert "drafts Zero-RB" in _construction_headline("X", meta, 6 / 8, 6, 8)

    def test_mid_rate_softens_the_verb(self):
        meta = _LABEL_META["zero_rb"]
        assert "usually" in _construction_headline("X", meta, 4 / 8, 4, 8)
        assert "often" in _construction_headline("X", meta, 3 / 8, 3, 8)

    def test_opens_needs_majority_for_almost_always(self):
        meta = _LABEL_META["opens_te"]
        assert _construction_headline("X", meta, 2 / 11, 2, 11) is None


class TestConstructionScoring:
    def _fleet(self):
        # 5 managers who open RB / balanced, 4 years each. One (Z) is Zero-RB.
        frames = []
        for m in ("A", "B", "C", "D"):
            for y in range(2021, 2025):
                frames.append(draft(m, y, ["RB", "RB", "WR", "WR", "QB", "TE", "RB", "WR"]))
        for y in range(2021, 2025):  # genuinely Zero-RB: no RB in first 6
            frames.append(draft("Z", y, ["WR", "WR", "WR", "TE", "WR", "QB", "RB", "RB"]))
        return pd.concat(frames, ignore_index=True)

    def test_zero_rb_manager_surfaces_others_do_not(self):
        sigs = mine_construction(self._fleet())
        zero = [s for s in sigs if s["feature_value"] == "zero_rb"]
        assert any(s["scope_label"] == "Z" for s in zero)
        assert not any(s["scope_label"] in ("A", "B") for s in zero)

    def test_common_pattern_is_not_a_tendency(self):
        # RB-RB open is the league norm (4 of 5 managers) -> not distinctive.
        sigs = mine_construction(self._fleet())
        robust = [s for s in sigs if s["feature_value"] == "robust_rb" and s.get("surfaced")]
        # A/B/C/D all robust-RB -> base rate ~0.8, none exceeds base*1.3.
        assert robust == []


class TestRepeatPlayer:
    def test_loyalty_across_seasons(self):
        frames = []
        for y in range(2019, 2025):  # 6 seasons, same guy
            frames.append(draft("Fan", y, ["RB", "WR"], ))
        df = pd.concat(frames, ignore_index=True)
        # Force the same player id across years.
        df.loc[df["pos"] == "RB", "NFL_player_id"] = "star"
        df.loc[df["pos"] == "RB", "player"] = "Star RB"
        rows = score_repeat_players(df, min_seasons=4)
        assert len(rows) == 1
        assert "Star RB" in rows[0]["nugget_headline"]
        assert "6 different seasons" in rows[0]["nugget_headline"]

    def test_one_off_is_silent(self):
        df = draft("Fan", 2024, ["RB", "WR", "QB", "TE"])
        assert score_repeat_players(df, min_seasons=4) == []
