"""Tests for the nugget renderer + legible tier featurization.

The nugget contract: a surfaced nugget is a sentence a league-mate just
reads. No lift/z jargon, no bin edges, no config tokens; if there is no
simple sentence, the renderer refuses (returns None) rather than emitting
gibberish.
"""

import pandas as pd

from multi_league.transformations.draft.nugget_renderer import (
    grade_bucket,
    outcome_clause,
    render_nugget,
    surface_score,
)
from multi_league.transformations.draft.wide_correlation_miner import (
    _position_tier_labels,
    _score_manager_affinities,
)


def manager_row(**overrides):
    row = {
        "scope_type": "manager",
        "scope_label": "Kevin",
        "feature_type": "bio.nfl_team",
        "feature_value": "LV",
        "feature_label": "NFL Team",
        "lift": 10.2,
        "picks": 11,
        "years_seen": 11,
        "positive_years": 11,
        "negative_years": 0,
    }
    row.update(overrides)
    return row


class TestManagerNuggets:
    def test_team_affinity_reads_like_a_sentence(self):
        rendered = render_nugget(manager_row())
        assert rendered is not None
        assert rendered["nugget_headline"] == (
            "Kevin loads up on Raiders players — about 10x the league rate."
        )
        assert "11 picks across 11 drafts" in rendered["nugget_evidence"]
        assert "held in 11 of 11 seasons" in rendered["nugget_evidence"]

    def test_no_stats_jargon_in_copy(self):
        rendered = render_nugget(manager_row())
        text = rendered["nugget_headline"] + rendered["nugget_evidence"]
        for banned in ("lift", "z_score", "z=", "sigma", "_to_"):
            assert banned not in text

    def test_prior_season_stat_tier_phrasing(self):
        rendered = render_nugget(manager_row(
            feature_type="prev.target_share_tier",
            feature_value="elite",
            feature_label="Target Share",
            lift=1.9,
        ))
        assert "players coming off an elite target share season" in rendered["nugget_headline"]
        assert "about twice as often as the league" in rendered["nugget_headline"]

    def test_config_tokens_scrubbed_from_stat_labels(self):
        rendered = render_nugget(manager_row(
            feature_type="prev.weighted_ppg_tier",
            feature_value="strong",
            feature_label="Weighted PPG 4pt Half",
            lift=1.4,
        ))
        headline = rendered["nugget_headline"]
        assert "4pt" not in headline and "Half" not in headline and "half" not in headline
        assert "weighted PPG" in headline

    def test_under_direction_phrasing(self):
        rendered = render_nugget(manager_row(lift=0.2, positive_years=0, negative_years=10))
        assert "almost never drafts" in rendered["nugget_headline"]
        assert "held in 0 of 11" not in rendered["nugget_evidence"]  # uses negative years

    def test_bio_trait_extremes_use_trait_language(self):
        elite = render_nugget(manager_row(
            feature_type="bio.height_tier", feature_value="elite",
            feature_label="Ht (in)", lift=4.6,
        ))
        assert "towering players" in elite["nugget_headline"]
        assert "Ht" not in elite["nugget_headline"]

    def test_refusals_instead_of_gibberish(self):
        # Mid-tier static trait: not a nugget.
        assert render_nugget(manager_row(
            feature_type="bio.weight_tier", feature_value="middling",
            feature_label="Wt (lbs)",
        )) is None
        # Unmapped bio trait tier: no simple words -> refuse.
        assert render_nugget(manager_row(
            feature_type="bio.hand_size_tier", feature_value="elite",
            feature_label="Hand Size",
        )) is None
        # Numeric bin edges are never copy.
        assert render_nugget(manager_row(
            feature_type="draft.cost_bin", feature_value="9.99_to_13.6",
            feature_label="Cost",
        )) is None
        # Junk two-letter stat labels are never copy.
        assert render_nugget(manager_row(
            feature_type="prev.rd_tier", feature_value="strong",
            feature_label="Rd",
        )) is None
        # The generic categorical fallback refuses codey labels too
        # (16-league QA caught "pts IDP fum TD: 0" leaking through).
        assert render_nugget(manager_row(
            feature_type="prev.pts_idp_fum_td", feature_value="0",
            feature_label="Pts IDP Fum TD",
        )) is None


class TestVolumeVsInvestmentFraming:
    """Money and roster spots are different tendencies (Joe, 2026-07-06)."""

    def test_pays_up_framing_when_money_but_not_count(self):
        rendered = render_nugget(manager_row(
            lift=2.0, count_lift=1.0, market_share=0.15,
        ))
        assert "pays up for Raiders players" in rendered["nugget_headline"]
        assert "twice the league's typical money" in rendered["nugget_headline"]

    def test_bargain_hoarder_framing_when_count_but_not_money(self):
        rendered = render_nugget(manager_row(
            lift=1.0, count_lift=2.5, market_share=0.2,
            count_positive_years=10, count_negative_years=1,
        ))
        assert "far more often than the league" in rendered["nugget_headline"]
        assert "bargain prices" in rendered["nugget_headline"]
        # Evidence follows the COUNT axis for count-framed nuggets.
        assert "held in 10 of 11 seasons" in rendered["nugget_evidence"]

    def test_only_buyer_framing_owns_the_market(self):
        rendered = render_nugget(manager_row(
            feature_type="draft.position_draft_label",
            feature_value="K2",
            feature_label="Position Draft Label",
            lift=1.1, count_lift=8.0, market_share=1.0,
            picks=11, count_positive_years=11, count_negative_years=0,
        ))
        assert "the league's only buyer" in rendered["nugget_headline"]
        assert "100% of every such pick ever made" in rendered["nugget_headline"]

    def test_rows_without_count_fields_keep_old_behavior(self):
        rendered = render_nugget(manager_row())  # no count_lift/market_share
        assert "loads up on Raiders players" in rendered["nugget_headline"]


class TestDualAxisScorer:
    def test_k2_hoarder_is_visible_on_the_count_axis(self):
        """A manager who rosters a $1 K2 every year is invisible to capital
        measurement but owns 100% of the K2 market."""
        rows = []
        feature_rows = []
        idx = 0

        def add(year, manager, position, bucket, weight, slot, player):
            nonlocal idx
            rows.append((year, manager, position, bucket, weight, player))
            feature_rows.append(
                {"row_index": idx, "feature_type": "draft.slot", "feature_value": slot}
            )
            idx += 1

        for year in (2021, 2022, 2023, 2024):
            for manager in ("A", "B", "C", "D", "E", "F"):
                for j in range(9):
                    add(year, manager, "RB", "auction_16_29", 20.0, "RB", f"rb_{manager}_{year}_{j}")
                # Everyone drafts a $1 K1 — the kicker stratum has a market.
                add(year, manager, "K", "auction_1_5", 1.0, "K1", f"k1_{manager}_{year}")
                # Only manager A ever takes a second kicker — and it's a
                # DIFFERENT kicker each year, so it's a genuine multi-player
                # habit, not the same guy re-drafted.
                if manager == "A":
                    add(year, manager, "K", "auction_1_5", 1.0, "K2", f"k2_{year}")

        df = pd.DataFrame(
            rows,
            columns=["year", "manager_key", "position_group", "capital_bucket", "capital_weight", "NFL_player_id"],
        )
        df["db_name"] = "test_league"
        df["manager"] = df["manager_key"]

        signals = _score_manager_affinities(
            df, feature_rows, min_picks=3, min_abs_z=2.0, limit=50
        )
        k2 = [s for s in signals if s["feature_value"] == "K2" and s["scope_key"] == "A"]
        assert k2, "K2 hoarding must surface via the count axis"
        signal = k2[0]
        assert abs(signal["count_z_score"]) >= 2.0
        assert signal["count_lift"] > 2.0
        # The always-new information: A owns the entire K2 market — the
        # "you're the only one doing this at all" frame, regardless of how
        # little money it costs.
        assert signal["market_share"] == 1.0
        # Dollars-spent framing would call this a trivial $4 habit; expected
        # picks (~1.1) vs observed (4) is the honest measure.
        assert signal["picks"] == 4
        assert signal["n_players"] == 4
        assert signal["expected_picks"] < 2.0


class TestHabitNeedsMultiplePlayers:
    """A tendency is only a "habit" if it spans multiple distinct players.
    Drafting the SAME player every year is a loyalty to that player (surfaced
    separately by the construction miner), not a habit of drafting their
    school/team/type — so the wide affinity scorer must gate it out."""

    def _score(self, picks_by_manager):
        """picks_by_manager: {manager: [(year, college, player_id), ...]}."""
        rows, feature_rows, idx = [], [], 0
        for manager, picks in picks_by_manager.items():
            for year, college, player in picks:
                rows.append((year, manager, "WR", "snake_r3_5", 8.0, player))
                feature_rows.append(
                    {"row_index": idx, "feature_type": "draft.college", "feature_value": college}
                )
                idx += 1
        df = pd.DataFrame(
            rows,
            columns=["year", "manager_key", "position_group", "capital_bucket", "capital_weight", "NFL_player_id"],
        )
        df["db_name"] = "test_league"
        df["manager"] = df["manager_key"]
        return _score_manager_affinities(df, feature_rows, min_picks=3, min_abs_z=1.0, limit=50)

    def test_same_player_redrafted_is_not_a_school_habit(self):
        # A drafts ONE Tulane player three years running; everyone else spreads
        # across other schools. Tulane must NOT surface as A's habit.
        picks = {
            "A": [(2021, "Tulane", "p_tulane"), (2022, "Tulane", "p_tulane"), (2023, "Tulane", "p_tulane")],
            "B": [(2021, "Ohio State", "p_b1"), (2022, "Georgia", "p_b2"), (2023, "Alabama", "p_b3")],
            "C": [(2021, "LSU", "p_c1"), (2022, "USC", "p_c2"), (2023, "Miami", "p_c3")],
        }
        tulane = [s for s in self._score(picks) if s["feature_value"] == "Tulane" and s["scope_key"] == "A"]
        assert not tulane, "one player re-drafted is a player loyalty, not a school habit"

    def test_multiple_players_from_a_school_is_a_habit(self):
        # A drafts THREE different Tulane players. That is a real habit.
        picks = {
            "A": [(2021, "Tulane", "p_t1"), (2022, "Tulane", "p_t2"), (2023, "Tulane", "p_t3")],
            "B": [(2021, "Ohio State", "p_b1"), (2022, "Georgia", "p_b2"), (2023, "Alabama", "p_b3")],
            "C": [(2021, "LSU", "p_c1"), (2022, "USC", "p_c2"), (2023, "Miami", "p_c3")],
        }
        tulane = [s for s in self._score(picks) if s["feature_value"] == "Tulane" and s["scope_key"] == "A"]
        assert tulane, "three distinct Tulane players is a genuine school habit"
        assert tulane[0]["n_players"] == 3

    def test_two_players_but_one_dominant_is_not_a_habit(self):
        # A keeps one Tulane player five years and drafts a second once. Two
        # distinct players clears the count floor, but 5/6 of the signal is one
        # guy — it's really that keeper, not a Tulane habit.
        picks = {
            "A": [
                (2020, "Tulane", "keeper"), (2021, "Tulane", "keeper"), (2022, "Tulane", "keeper"),
                (2023, "Tulane", "keeper"), (2024, "Tulane", "keeper"), (2021, "Tulane", "p_other"),
            ],
            "B": [(2021, "Ohio State", "p_b1"), (2022, "Georgia", "p_b2"), (2023, "Alabama", "p_b3")],
            "C": [(2021, "LSU", "p_c1"), (2022, "USC", "p_c2"), (2023, "Miami", "p_c3")],
        }
        tulane = [s for s in self._score(picks) if s["feature_value"] == "Tulane" and s["scope_key"] == "A"]
        assert not tulane, "one dominant repeat-drafted player is not a school habit"


class TestSurfacingPolicy:
    """Confidence and importance are separate dials (Joe, 2026-07-06):
    hometown is trivia unless the signal is absolutely crazy."""

    def test_hometown_with_ordinary_evidence_is_suppressed(self):
        result = surface_score(manager_row(
            feature_type="bio.birth_place", feature_value="Toledo, OH",
            importance=1, capital_z_score=3.0, lift=2.0,
            repeatability=0.7,
        ))
        assert result["surfaced"] is False

    def test_hometown_with_a_crazy_signal_earns_airtime(self):
        result = surface_score(manager_row(
            feature_type="bio.birth_place", feature_value="Toledo, OH",
            importance=1, capital_z_score=5.5, lift=6.0,
            repeatability=1.0,
        ))
        assert result["surfaced"] is True

    def test_strategic_signal_surfaces_at_ordinary_evidence(self):
        result = surface_score(manager_row(
            feature_type="prev.target_share_tier", feature_value="elite",
            importance=3, capital_z_score=2.1, lift=1.8,
            repeatability=0.8,
        ))
        assert result["surfaced"] is True

    def test_character_bar_sits_between(self):
        low = surface_score(manager_row(
            feature_type="bio.nfl_team", importance=2,
            capital_z_score=2.1, repeatability=0.8,
        ))
        ok = surface_score(manager_row(
            feature_type="bio.nfl_team", importance=2,
            capital_z_score=2.8, repeatability=0.8,
        ))
        assert low["surfaced"] is False and ok["surfaced"] is True

    def test_importance_outranks_raw_z_in_scoring(self):
        strategic = surface_score(manager_row(importance=3, capital_z_score=3.0, repeatability=0.9))
        trivia = surface_score(manager_row(importance=1, capital_z_score=6.0, lift=8.0, repeatability=1.0))
        assert strategic["surface_score"] > trivia["surface_score"]

    def test_count_axis_evidence_counts(self):
        result = surface_score(manager_row(
            importance=3, capital_z_score=0.5, count_z_score=2.6,
            count_repeatability=0.9, repeatability=0.2,
        ))
        assert result["surfaced"] is True

    def test_one_draft_is_not_a_tendency(self):
        result = surface_score(manager_row(
            importance=3, capital_z_score=5.0, repeatability=1.0,
            years_seen=1,
        ))
        assert result["surfaced"] is False

    def test_middling_tier_is_not_a_story(self):
        assert render_nugget(manager_row(
            feature_type="prev.weighted_ppg_tier", feature_value="middling",
            feature_label="Weighted PPG", lift=2.0,
        )) is None


class TestLeagueNuggets:
    def test_bargain_direction(self):
        rendered = render_nugget({
            "scope_type": "league_inefficiency",
            "feature_type": "bio.college",
            "feature_value": "Alabama",
            "feature_label": "College",
            "excess_residual": 4.2,
            "picks": 40,
            "years_seen": 6,
            "positive_years": 5,
            "negative_years": 1,
        })
        assert rendered["nugget_headline"] == (
            "In this league, players out of Alabama have been quiet bargains."
        )
        assert "held in 5 of 6 seasons" in rendered["nugget_evidence"]

    def test_overpay_direction(self):
        rendered = render_nugget({
            "scope_type": "league_inefficiency",
            "feature_type": "prev.receptions_tier",
            "feature_value": "elite",
            "feature_label": "Receptions",
            "excess_residual": -3.0,
            "picks": 30,
            "years_seen": 5,
            "positive_years": 1,
            "negative_years": 4,
        })
        assert rendered["nugget_headline"] == (
            "This league consistently overpays for players coming off an elite receptions season."
        )


class TestDerivedBehaviorNuggets:
    def test_reach_behavior_reads_plainly(self):
        rendered = render_nugget(manager_row(
            feature_type="draft.market_reach", feature_value="big reach",
            feature_label="ADP Reach", lift=2.4,
        ))
        assert "reach way up for" in rendered["nugget_headline"]
        assert "ADP" in rendered["nugget_headline"]

    def test_neutral_market_values_are_not_nuggets(self):
        assert render_nugget(manager_row(
            feature_type="draft.market_reach", feature_value="market price",
        )) is None
        assert render_nugget(manager_row(
            feature_type="draft.market_price", feature_value="market price",
        )) is None

    def test_stack_phrase(self):
        rendered = render_nugget(manager_row(
            feature_type="draft.team_stack", feature_value="QB stack",
            feature_label="QB Stack", lift=3.2,
        ))
        assert "pass-catchers paired with their own quarterback" in rendered["nugget_headline"]

    def test_bargain_hunting_auction_phrase(self):
        rendered = render_nugget(manager_row(
            feature_type="draft.market_price", feature_value="big bargain",
            feature_label="Market Price Paid", lift=2.0,
        ))
        assert "steep discounts to market price" in rendered["nugget_headline"]


class TestFleetPersonality:
    def make_fleet(self, target_stage, pos="TE", n=200):
        rows = [{"db_name": f"lg{i}", "pos": pos, "stage": 0.5 + (i % 40) * 0.004, "yrs": 5, "picks": 60}
                for i in range(n)]
        rows.append({"db_name": "target", "pos": pos, "stage": target_stage, "yrs": 6, "picks": 70})
        return pd.DataFrame(rows)

    def test_extreme_early_league_gets_the_fleet_sentence(self):
        from multi_league.transformations.draft.wide_correlation_miner import _fleet_signals_from_frame
        signals = _fleet_signals_from_frame(self.make_fleet(0.30), "target")
        assert len(signals) == 1
        assert "drafts TEs earlier than almost any" in signals[0]["nugget_headline"]
        assert signals[0]["surfaced"] is True

    def test_copy_never_discloses_fleet_size(self):
        from multi_league.transformations.draft.wide_correlation_miner import _fleet_signals_from_frame
        signal = _fleet_signals_from_frame(self.make_fleet(0.30), "target")[0]
        for text in (signal["nugget_headline"], signal["nugget_evidence"]):
            assert "we track" not in text
            assert "%" not in text
        # No counts in the headline; the numbers stay on the row internally.
        assert not any(ch.isdigit() for ch in signal["nugget_headline"])
        assert signal["fleet_leagues"] == 200

    def test_middle_of_fleet_is_silent(self):
        from multi_league.transformations.draft.wide_correlation_miner import _fleet_signals_from_frame
        assert _fleet_signals_from_frame(self.make_fleet(0.58), "target") == []

    def test_late_extreme_uses_waits_on_phrasing(self):
        from multi_league.transformations.draft.wide_correlation_miner import _fleet_signals_from_frame
        signals = _fleet_signals_from_frame(self.make_fleet(0.95), "target")
        assert len(signals) == 1
        assert "waits on TEs" in signals[0]["nugget_headline"]
        assert "longer than almost any" in signals[0]["nugget_headline"]

    def test_similar_rules_cohort_kills_the_superflex_confounder(self):
        """A superflex league drafting QBs early is NOT a nugget when
        compared against other superflex leagues doing the same."""
        from multi_league.transformations.draft.wide_correlation_miner import _fleet_signals_from_frame
        rows = []
        flags = {}
        # 800 one-QB leagues draft QBs late.
        for i in range(800):
            rows.append({"db_name": f"oneqb{i}", "pos": "QB", "stage": 0.65 + (i % 20) * 0.003, "yrs": 5, "picks": 30})
            flags[f"oneqb{i}"] = {"superflex": False, "idp": False, "tep": False}
        # 100 superflex leagues draft QBs early — by rule; the target sits in
        # the MIDDLE of that cohort (stages span 0.26-0.34 around its 0.30).
        for i in range(100):
            rows.append({"db_name": f"sflex{i}", "pos": "QB", "stage": 0.26 + (i % 20) * 0.004, "yrs": 5, "picks": 30})
            flags[f"sflex{i}"] = {"superflex": True, "idp": False, "tep": False}
        rows.append({"db_name": "target", "pos": "QB", "stage": 0.30, "yrs": 6, "picks": 40})
        flags["target"] = {"superflex": True, "idp": False, "tep": False}
        fleet = pd.DataFrame(rows)

        # Unstratified, the target looks extreme (earlier than all 1QB leagues).
        unstratified = _fleet_signals_from_frame(fleet, "target")
        assert len(unstratified) == 1
        # Within its rule cohort it is ordinary — no nugget.
        stratified = _fleet_signals_from_frame(fleet, "target", flags)
        assert stratified == []

    def test_cohort_membership_is_reported_in_evidence(self):
        from multi_league.transformations.draft.wide_correlation_miner import _fleet_signals_from_frame
        fleet = self.make_fleet(0.30)
        flags = {row: {"superflex": False, "idp": False, "tep": False} for row in fleet["db_name"]}
        signals = _fleet_signals_from_frame(fleet, "target", flags)
        assert len(signals) == 1
        assert signals[0]["similar_rules_cohort"] is True
        assert "similar rules" in signals[0]["nugget_evidence"]


class TestOutcomeVerdict:
    """Plain 'worked / blew up' from draft grades — no LAMAR, no numbers."""

    def test_grade_buckets(self):
        assert grade_bucket("A+") == "hit"
        assert grade_bucket("B+") == "hit"
        assert grade_bucket("B") == "neutral"
        assert grade_bucket("C") == "neutral"
        assert grade_bucket("D") == "bust"
        assert grade_bucket("F") == "bust"
        assert grade_bucket(None) == "neutral"

    def test_mostly_worked(self):
        assert outcome_clause(8, 2) == "and it's mostly worked out"

    def test_mostly_blew_up(self):
        assert outcome_clause(2, 8) == "and it's mostly blown up in their face"

    def test_mixed_gets_no_verdict(self):
        assert outcome_clause(5, 5) is None

    def test_thin_sample_stays_silent(self):
        assert outcome_clause(3, 1) is None

    def test_verdict_carries_no_digits_or_lamar(self):
        for clause in (outcome_clause(9, 1), outcome_clause(1, 9)):
            assert clause is not None
            assert not any(ch.isdigit() for ch in clause)
            assert "lamar" not in clause.lower()


class TestPositionTierLabels:
    def make_df(self, values, positions=None, invert_col="prev__stat"):
        n = len(values)
        return pd.DataFrame({
            invert_col: values,
            "position_group": positions or ["RB"] * n,
            "year": [2024] * n,
        })

    def test_tiers_are_position_relative_words(self):
        df = self.make_df(list(range(1, 21)))
        labels = _position_tier_labels(df, "prev__stat", invert=False)
        assert labels[19] == "elite"      # highest value
        assert labels[0] == "weak"        # lowest value
        assert set(labels.values()) <= {"elite", "strong", "middling", "weak"}

    def test_invert_for_lower_is_better_stats(self):
        df = self.make_df(list(range(1, 21)))
        labels = _position_tier_labels(df, "prev__stat", invert=True)
        assert labels[0] == "elite"       # lowest time = fastest
        assert labels[19] == "weak"

    def test_small_position_groups_are_skipped(self):
        df = self.make_df([1, 2, 3], positions=["TE", "TE", "TE"])
        assert _position_tier_labels(df, "prev__stat", invert=False) == {}
