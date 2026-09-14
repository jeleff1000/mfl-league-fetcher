"""The two StatsCrew STAT families (2026-07-27) -- registered at DATASET-DIR grain.

WHY THIS FILE EXISTS. Both were harvested across TWO GitHub runs each, so neither can be
registered at run grain without witnessing a partial dataset as if it were whole:

    team_season_stats    shards 0/2/3/4 on 30232968983, shard 1 on 30238150737
    team_season_results  shards 1-14   on 30268109268, shard 0 on 30271114181

Registering the dataset DIR instead makes the glob span runs, which is correct -- and
opens a hole this file closes: a future run dir landing under the same path would join
the source silently and move every downstream denominator with nothing failing. The
composition is therefore PINNED here. A new run dir is a TEST FAILURE and a deliberate
re-pin, never a silent change.

The row counts are pinned too. `coverage_complete` is false on every individual
IMPORT_MANIFEST (each run holds only its own shards); the completeness claim lives at
this level, where the shard union is 5/5 and 15/15 respectively.

Run:  python -m pytest scripts/sota_recon/test_statscrew_stat_families.py -q
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from .sources import registry

# (source key, expected run dirs, expected shard count, expected rows, expected tags)
COMPOSITION = {
    "statscrew_team_season_stats": {
        "runs": ["30232968983", "30238150737"],
        "shards": 5,
        "rows": 168_390,
        "tags": {
            "defense_and_fumbles": 48_882, "total_scoring": 22_176, "receiving": 21_441,
            "rushing": 17_601, "interceptions": 13_264, "sacks": 12_567,
            "kick_returns": 11_310, "passing": 7_184, "punt_returns": 6_243,
            "kicking": 4_008, "punting": 3_714,
        },
        "years": (1921, 2025),
    },
    "statscrew_team_season_results": {
        "runs": ["30268109268", "30271114181"],
        "shards": 15,
        "rows": 23_235,
        "tags": {"results": 23_235},
        "years": (1920, 2023),
    },
}


def _path(key: str) -> Path:
    return Path(registry(include_subject=True)[key].path)


def _scan(key: str) -> str:
    root = _path(key).as_posix()
    return (f"read_parquet('{root}/*/shards/shard-*/records.parquet', "
            "union_by_name=true)")


@pytest.mark.parametrize("key", sorted(COMPOSITION))
def test_the_run_composition_is_exactly_what_was_pinned(key: str) -> None:
    """A new run dir under the dataset path changes the source's material. That must
    fail here rather than move a denominator silently."""
    root = _path(key)
    if not root.is_dir():
        pytest.skip(f"{key}: lake path absent in this tree")
    observed = sorted(child.name for child in root.iterdir() if child.is_dir())
    assert observed == sorted(COMPOSITION[key]["runs"]), (
        f"{key}: run dirs {observed} != pinned {sorted(COMPOSITION[key]['runs'])} -- "
        "a run joined or left this dataset; re-pin deliberately with its row counts")


@pytest.mark.parametrize("key", sorted(COMPOSITION))
def test_the_shard_union_is_complete_even_though_no_single_run_is(key: str) -> None:
    """Every individual IMPORT_MANIFEST says coverage_complete=false, because each run
    holds only its own shards. The completeness claim is the UNION, and it is checked
    against the shard ids the manifests actually assert -- not against a directory
    count, which would pass on a re-imported duplicate."""
    root = _path(key)
    if not root.is_dir():
        pytest.skip(f"{key}: lake path absent in this tree")
    expected = COMPOSITION[key]["shards"]
    observed: list[int] = []
    for manifest_path in sorted(root.glob("*/IMPORT_MANIFEST.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["expected_shards"] == expected, manifest_path
        observed.extend(manifest["observed_shards"])
    assert sorted(observed) == list(range(expected)), (
        f"{key}: shard union {sorted(observed)} != 0..{expected - 1}")


@pytest.mark.parametrize("key", sorted(COMPOSITION))
def test_the_row_and_tag_counts_are_pinned(key: str) -> None:
    root = _path(key)
    if not root.is_dir():
        pytest.skip(f"{key}: lake path absent in this tree")
    spec = COMPOSITION[key]
    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    try:
        scan = _scan(key)
        rows = connection.execute(f"SELECT COUNT(*) FROM {scan}").fetchone()[0]
        tags = dict(
            connection.execute(
                f"SELECT table_tag, COUNT(*) FROM {scan} GROUP BY 1"
            ).fetchall()
        )
        low, high = connection.execute(
            f"SELECT MIN(CAST(season AS INTEGER)), MAX(CAST(season AS INTEGER)) FROM {scan}"
        ).fetchone()
    finally:
        connection.close()
    assert rows == spec["rows"], f"{key}: {rows:,} rows != pinned {spec['rows']:,}"
    assert tags == spec["tags"], f"{key}: table tags drifted"
    assert (low, high) == spec["years"], f"{key}: span ({low}, {high}) != {spec['years']}"


def test_table_tags_are_the_dossier_discriminator_for_the_stats_family() -> None:
    """Registered flat, ELEVEN logical tables would collapse into one registry row --
    the 82-tables-in-7-rows failure. Five of the eleven share the header set
    `avg long no player tds yds`, so the tag is the only thing that separates them."""
    key = "statscrew_team_season_stats"
    if not _path(key).is_dir():
        pytest.skip("lake path absent in this tree")
    from .column_dossier import _resolve_scan, detect_regime

    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    try:
        resolved = _resolve_scan(connection, str(_path(key)))
        assert resolved is not None, f"{key} is unreadable at its registered path"
        scan, columns = resolved
        regime = detect_regime(connection, scan, columns)
    finally:
        connection.close()
    assert regime["regime"] == "MULTI_TABLE", regime
    assert "table_tag" in regime["discriminator"], regime


def test_neither_family_can_vote_yet() -> None:
    """Both key on a StatsCrew id space with no receipted crosswalk. Registration is
    not permission to vote, and their two blockers are DIFFERENT -- clearing the player
    crosswalk does not license the team-code family, and vice versa."""
    from .test_mapping_obligation import MAPPING_PENDING

    for key in COMPOSITION:
        assert key in MAPPING_PENDING, f"{key}: registered without a declared blocker"
    assert "crosswalk" in MAPPING_PENDING["statscrew_team_season_stats"]
    assert "team-code" in MAPPING_PENDING["statscrew_team_season_results"]
    assert (MAPPING_PENDING["statscrew_team_season_stats"]
            != MAPPING_PENDING["statscrew_team_season_results"]), (
        "two families sharing one blocker note is how a cleared blocker licenses a "
        "family it never applied to")


def test_statscrew_rate_denominators_follow_published_average_semantics() -> None:
    from .statscrew_column_adjudication import DERIVED_RATIOS, DERIVED_WITNESSES

    assert DERIVED_RATIOS[("interceptions", "avg")] == ("yds", "no")
    assert DERIVED_RATIOS[("kick_returns", "avg")] == ("yds", "no")
    assert DERIVED_RATIOS[("punt_returns", "avg")] == ("yds", "no")
    assert DERIVED_RATIOS[("kicking", "avg")] == ("yds", "ko")
    assert ("sacks", "avg") not in DERIVED_RATIOS
    assert ("results", "record") not in DERIVED_RATIOS
    assert DERIVED_WITNESSES[("interceptions", "avg")]["numerator"] == \
        "def_interception_yards"
    assert DERIVED_WITNESSES[("interceptions", "avg")]["denominator"] == \
        "def_interceptions"


def test_statscrew_derived_rates_remain_explicit_witnesses() -> None:
    from .statscrew_column_adjudication import build_decisions

    decisions, _ = build_decisions()
    by_key = {row["key"]: row for row in decisions}
    for key, numerator, denominator in (
        ("statscrew_team_season_stats|interceptions|avg",
         "def_interception_yards", "def_interceptions"),
        ("statscrew_team_season_stats|kick_returns|avg",
         "kickoff_return_yards", "kickoff_returns"),
        ("statscrew_team_season_stats|punt_returns|avg",
         "punt_return_yards", "punt_returns"),
    ):
        row = by_key[key]
        assert row["disposition"] == "EXCLUDED_WITH_REASON"
        assert row["witness_kind"] == "DERIVED_WITNESS"
        assert row["canonical_numerator"] == numerator
        assert row["canonical_denominator"] == denominator


def test_statscrew_points_candidates_have_distinct_scoring_lanes() -> None:
    from .statscrew_column_adjudication import build_decisions

    decisions, _ = build_decisions()
    by_key = {row["key"]: row for row in decisions}
    assert by_key["statscrew_team_season_stats|kicking|pts"]["disposition"] == \
        "NEW_SUPERTABLE_COLUMN_CANDIDATE"
    points = by_key["statscrew_team_season_stats|total_scoring|points"]
    assert points["canonical"] == "total_points_scored"
    assert points["candidate_role"] == "EXISTING_CANONICAL_VALIDATION"
    assert "2*(saf+2pt)" in points["candidate_formula"]

    for key, canonical, numerator, denominator in (
        ("statscrew_team_season_stats|punting|avg", "punt_yards_per_punt",
         "punt_yards", "punts"),
        ("statscrew_team_season_stats|receiving|avg",
         "receiving_yards_per_reception", "receiving_yards", "receptions"),
        ("statscrew_team_season_stats|rushing|avg", "rushing_yards_per_carry",
         "rushing_yards", "carries"),
    ):
        row = by_key[key]
        assert row["disposition"] == "MAPPED_TO_CANONICAL"
        assert row["witness_kind"] == "DERIVED_WITNESS"
        assert row["canonical"] == canonical
        assert row["canonical_numerator"] == numerator
        assert row["canonical_denominator"] == denominator


def test_statscrew_interception_average_matches_yards_over_returns() -> None:
    """The source's published Interception Return Average is yds / no, not no / yds."""
    key = "statscrew_team_season_stats"
    root = _path(key).as_posix()
    connection = duckdb.connect()
    try:
        scan = _scan(key)
        good, total = connection.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE TRY_CAST(avg AS DOUBLE) IS NOT NULL
                               AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
                               AND TRY_CAST(no AS DOUBLE) > 0
                               AND ABS(TRY_CAST(avg AS DOUBLE)
                                       - TRY_CAST(yds AS DOUBLE)/TRY_CAST(no AS DOUBLE)) <= 0.15),
              COUNT(*) FILTER (WHERE TRY_CAST(avg AS DOUBLE) IS NOT NULL
                               AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
                               AND TRY_CAST(no AS DOUBLE) > 0)
            FROM {scan} WHERE table_tag='interceptions'
        """).fetchone()
    finally:
        connection.close()
    assert total > 0
    assert good / total >= 0.995
