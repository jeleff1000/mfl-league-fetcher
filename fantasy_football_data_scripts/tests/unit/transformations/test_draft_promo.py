"""Tests for the draft promo framework — scouting blurbs on a fixed skeleton.

The contract: every manager gets a fact-carrying, actionable blurb; strategic
reads lead and trivia is demoted to an 'Also' footer; confidence is plain
language; prior-production is deduped by stat group so we never list three
flavors of 'rushing'."""

from multi_league.transformations.draft.draft_promo import build_manager_promo


def _row(**kw):
    base = {
        "scope_type": "manager", "scope_label": "Dave",
        "picks": 8, "years_seen": 8, "count_repeatability": 0.85,
        "lift": 1.0, "count_lift": 1.0, "surface_score": 3.0,
        "outcome_hits": 0, "outcome_busts": 0, "surfaced": True,
    }
    base.update(kw)
    return base


def _all_text(promo):
    return " ".join(b["text"] for b in promo["facts"]) + " " + promo["also"]


class TestProductionBlurb:
    def test_production_carries_the_multiple_and_confidence(self):
        rows = [_row(feature_type="prev.rush_yards_tier", feature_value="strong",
                     feature_label="Rushing Yards", count_lift=4.0,
                     outcome_hits=8, outcome_busts=2)]
        promo = build_manager_promo(rows, "Dave", "test_league")
        assert promo["facts"], "a production tendency must produce a lead blurb"
        text = promo["facts"][0]["text"]
        assert "4x" in text, "the magnitude fact must survive"
        assert "Dave" in text
        # plain-language outcome confidence, no interval math
        assert "%" not in text and "CI" not in text

    def test_production_dedupes_by_stat_group(self):
        # three flavors of rushing must collapse to one named stat
        rows = [
            _row(feature_type="prev.rush_yards_tier", feature_value="strong",
                 feature_label="Rushing Yards", count_lift=4.0),
            _row(feature_type="prev.rush_att_tier", feature_value="strong",
                 feature_label="Rushing Attempts", count_lift=3.5),
            _row(feature_type="prev.rush_td_tier", feature_value="strong",
                 feature_label="Rushing Touchdowns", count_lift=3.0),
        ]
        promo = build_manager_promo(rows, "Dave", "test_league")
        text = promo["facts"][0]["text"].lower()
        assert text.count("rushing") <= 1, "should not list three flavors of rushing"


class TestTiering:
    def test_trivia_is_demoted_to_the_also_footer(self):
        rows = [
            _row(feature_type="prev.rush_yards_tier", feature_value="strong",
                 feature_label="Rushing Yards", count_lift=4.0),
            _row(feature_type="bio.college_bin", feature_value="Alabama",
                 feature_label="College", count_lift=3.0, surface_score=2.0),
        ]
        promo = build_manager_promo(rows, "Dave", "test_league")
        lead_text = " ".join(b["text"] for b in promo["facts"])
        assert "Alabama" not in lead_text, "school loyalty is trivia, not a lead"
        assert "Alabama" in promo["also"]

    def test_distinctive_read_leads_over_commodity_production(self):
        rows = [
            _row(feature_type="prev.rush_yards_tier", feature_value="strong",
                 feature_label="Rushing Yards", count_lift=4.0),
            _row(feature_type="construction.zero_rb", feature_value="zero_rb",
                 feature_label="Roster Construction", count_lift=3.0, picks=6, years_seen=8),
        ]
        promo = build_manager_promo(rows, "Dave", "test_league")
        # Zero-RB (distinctive) should headline over prior-production (universal)
        assert promo["facts"][0]["kind"] == "build"


class TestEveryoneGetsABlurb:
    def test_trivia_only_manager_still_gets_a_lead(self):
        rows = [_row(feature_type="bio.nfl_team_bin", feature_value="DET",
                     feature_label="NFL Team", count_lift=3.0)]
        promo = build_manager_promo(rows, "Dave", "test_league")
        assert promo["facts"], "a manager with only trivia must still get a lead blurb"
        assert "Lions" in promo["facts"][0]["text"], "team code renders as a name"
