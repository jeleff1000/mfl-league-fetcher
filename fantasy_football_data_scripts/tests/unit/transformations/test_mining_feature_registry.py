"""Tests for the semantic-contracted mining feature registry (L0)."""

import json
from pathlib import Path

import pytest

from multi_league.transformations.draft.mining_feature_registry import (
    compile_registry,
    draft_feature_entries,
    find_repo_root,
    league_scoring_config,
    outcome_columns,
    parse_config_axis,
    select_miner_features,
)
from multi_league.transformations.draft.wide_correlation_miner import (
    TARGET_OR_LEAK_COLUMNS,
)

FIELDS = [
    "table", "column", "type", "family", "highValue", "exposure",
    "recommendation", "semanticStatus", "semanticName", "semanticCategory",
    "semanticGuidanceId", "contractIds",
]


def make_model(rows):
    columns = []
    for table, column, family in rows:
        columns.append([
            table, column, "DOUBLE", family, 1, "api", "exposed",
            "concept", column.title(), family, 0, [],
        ])
    return {"columnFields": FIELDS, "columns": columns, "relationships": []}


def entry_for(registry, table, column):
    for entry in registry["entries"]:
        if entry["table"] == table and entry["column"] == column:
            return entry
    raise AssertionError(f"missing {table}.{column}")


class TestRoleContracts:
    def test_outcome_families_are_never_draft_features(self):
        registry = compile_registry(make_model([
            ("___leagues.public.draft", "manager_lamar", "lamar"),
            ("___leagues.public.draft", "pick_score", "draft_quality"),
            ("___leagues.public.player_fantasy", "clutch_equity", "clutch"),
        ]))
        for entry in registry["entries"]:
            assert entry["role"] == "outcome"
            assert not entry["draft_time_eligible"]
            assert not entry["waiver_eligible"]

    def test_draft_pick_context_is_feature_fuel(self):
        registry = compile_registry(make_model([
            ("___leagues.public.draft", "round", "draft_quality"),
            ("___leagues.public.draft", "cost", "draft_quality"),
            ("___leagues.public.draft", "position", "draft_quality"),
        ]))
        for entry in registry["entries"]:
            assert entry["role"] == "feature"
            assert entry["temporal"] == "draft_time"
            assert entry["draft_time_eligible"]

    def test_bio_affinity_vocabulary_promoted_from_backlog(self):
        registry = compile_registry(make_model([
            ("___ops.nfl_historical.player_bio", "college", "other"),
            ("___ops.nfl_historical.player_bio", "ras_score", "other"),
        ]))
        for entry in registry["entries"]:
            assert entry["role"] == "feature"
            assert entry["temporal"] == "static"
            assert entry["legibility"] == "A"

    def test_career_aggregates_are_leaky_and_excluded(self):
        registry = compile_registry(make_model([
            ("___ops.nfl_historical.player_nfl_career", "career_targets", "base_metric"),
        ]))
        entry = registry["entries"][0]
        assert entry["temporal"] == "leaky_aggregate"
        assert not entry["draft_time_eligible"]
        assert (entry["table"], entry["column"]) in outcome_columns(registry)

    def test_super_table_stats_are_lagged_features(self):
        registry = compile_registry(make_model([
            ("___ops.nfl_historical.nfl_player_stats_all", "target_share", "base_metric"),
        ]))
        entry = registry["entries"][0]
        assert entry["role"] == "feature"
        assert entry["temporal"] == "lagged_prior_season"
        assert entry["draft_time_eligible"]
        assert entry["legibility"] == "B"

    def test_unknown_family_lands_in_backlog_not_mining(self):
        registry = compile_registry(make_model([
            ("___leagues.public.matchup", "mystery_column", "other"),
        ]))
        entry = registry["entries"][0]
        assert entry["role"] == "backlog"
        assert not entry["draft_time_eligible"]


class TestConfigAxis:
    def test_scoring_and_qb_variants_share_a_concept_key(self):
        a = parse_config_axis("rolling_3_4pt_0ppr")
        b = parse_config_axis("rolling_3_6pt_tep")
        assert a["concept_key"] == b["concept_key"] == "rolling_3"
        assert a["scoring_variant"] == "0ppr" and a["qb_variant"] == "4pt"
        assert b["scoring_variant"] == "tep" and b["qb_variant"] == "6pt"
        assert a["is_config_variant"] and b["is_config_variant"]

    def test_rank_scope_parsed(self):
        axis = parse_config_axis("rank_season_rb_0ppr")
        assert axis["rank_scope"] == "season"
        assert axis["scoring_variant"] == "0ppr"

    def test_plain_columns_are_not_variants(self):
        axis = parse_config_axis("target_share")
        assert axis["concept_key"] == "target_share"
        assert not axis["is_config_variant"]


class TestLeagueScoringConfig:
    def test_tep_beats_rec_value(self):
        config = league_scoring_config({"scoring_bonus_rec_te": 0.5, "scoring_rec": 1})
        assert config["scoring_variant"] == "tep"

    def test_rec_value_mapping(self):
        assert league_scoring_config({"scoring_rec": 1})["scoring_variant"] == "ppr"
        assert league_scoring_config({"scoring_rec": 0.5})["scoring_variant"] == "half"
        assert league_scoring_config({"scoring_rec": 0})["scoring_variant"] == "0ppr"

    def test_qb_variant_and_defaults(self):
        assert league_scoring_config({"scoring_pass_td": 6})["qb_variant"] == "6pt"
        # Missing settings fall back to the fleet-common config, not 0ppr.
        assert league_scoring_config(None) == {"scoring_variant": "ppr", "qb_variant": "4pt"}
        # A present row with explicit zero rec IS a standard-scoring league.
        assert league_scoring_config({"scoring_rec": 0})["scoring_variant"] == "0ppr"


class TestSelectMinerFeatures:
    SEASON = "___ops.nfl_historical.player_nfl_season"
    BIO = "___ops.nfl_historical.player_bio"

    def make_registry(self):
        return compile_registry(make_model([
            (self.SEASON, "rank_season_rb_ppr", "rank"),
            (self.SEASON, "rank_season_rb_tep", "rank"),
            (self.SEASON, "rank_season_rb_0ppr", "rank"),
            (self.SEASON, "target_share", "base_metric"),
            (self.BIO, "college", "other"),
        ]))

    def test_league_is_mined_in_its_own_scoring_space(self):
        registry = self.make_registry()
        tep = select_miner_features(
            registry,
            tables={self.SEASON, self.BIO},
            league_config={"scoring_variant": "tep", "qb_variant": "4pt"},
        )
        columns = {entry["column"] for entry in tep}
        assert "rank_season_rb_tep" in columns
        assert "rank_season_rb_ppr" not in columns
        assert "target_share" in columns  # non-variant passes through
        assert "college" in columns

    def test_one_entry_per_concept(self):
        registry = self.make_registry()
        chosen = select_miner_features(
            registry,
            tables={self.SEASON},
            league_config={"scoring_variant": "ppr", "qb_variant": "4pt"},
        )
        concept_keys = [entry["concept_key"] for entry in chosen]
        assert len(concept_keys) == len(set(concept_keys))

    def test_legibility_survives_the_cap(self):
        registry = self.make_registry()
        capped = select_miner_features(
            registry,
            tables={self.SEASON, self.BIO},
            league_config={"scoring_variant": "ppr", "qb_variant": "4pt"},
            max_features=2,
        )
        # Tier-A college must outrank Tier-B/C season stats under the cap.
        assert capped[0]["column"] == "college"

    def test_no_config_means_no_variant_features(self):
        registry = self.make_registry()
        chosen = select_miner_features(
            registry, tables={self.SEASON}, league_config=None,
        )
        assert all(not entry["is_config_variant"] for entry in chosen)


class TestRealArtifact:
    @pytest.fixture(scope="class")
    def artifact(self):
        path = find_repo_root(Path(__file__)) / "docs" / "mining-feature-registry.json"
        if not path.exists():
            pytest.skip("registry artifact not generated")
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)

    def test_old_hand_leak_list_is_contained_by_contracts(self, artifact):
        """Every column the wide miner hand-listed as leaky must be barred
        from feature vectors by the registry's contracts."""
        barred = outcome_columns(artifact)
        barred_columns = {column for _, column in barred}
        feature_columns = {
            entry["column"] for entry in draft_feature_entries(artifact)
        }
        for column in TARGET_OR_LEAK_COLUMNS:
            assert column not in feature_columns, f"{column} leaked into features"
            if any(entry["column"] == column for entry in artifact["entries"]):
                assert column in barred_columns

    def test_no_outcome_family_is_ever_feature_eligible(self, artifact):
        for entry in artifact["entries"]:
            if entry["family"] in ("lamar", "draft_quality", "optimal", "clutch", "percentile"):
                if entry["role"] == "feature":
                    # Only the reviewed draft-context override may do this.
                    assert entry["table"] == "___leagues.public.draft"
                    assert entry["temporal"] == "draft_time"

    def test_summary_matches_entries(self, artifact):
        entries = artifact["entries"]
        assert artifact["summary"]["columns"] == len(entries)
        assert artifact["summary"]["draft_time_features"] == sum(
            1 for entry in entries if entry["draft_time_eligible"]
        )

    def test_universe_is_materially_wider_than_the_hand_lists(self, artifact):
        assert artifact["summary"]["draft_time_features"] > 500
        tiers = artifact["summary"]["draft_time_by_tier"]
        assert tiers.get("A", 0) >= 20, "Tier-A fun vocabulary missing"
        assert tiers.get("B", 0) >= 300
