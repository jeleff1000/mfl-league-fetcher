#!/usr/bin/env python3
"""Mining Feature Registry — semantic-contracted feature universe (L0).

Compiles docs/semantic-data-model.json (4,239 columns, 29 tables, families,
labels, join contracts) into a mining registry that every draft/waiver miner
consumes instead of hand-maintained column lists.

Contracts encoded per column:
  role       outcome | feature | context | join_key | excluded | backlog
             Outcome families (lamar, draft_quality, optimal, clutch,
             percentile) can NEVER appear as features — leakage is
             impossible by construction, not maintained by hand.
  temporal   draft_time | static | lagged_prior_season | manager_prior_season
             | in_season | config | leaky_aggregate
             Draft-time mining may only consume features whose temporal
             class is draft-safe; stats join at year <= Y-1. Career
             as-of-now aggregates are leaky and excluded outright.
  config     scoring/qb-scoring variant parsed from the column name
             (_0ppr/_half/_ppr/_tep, _4pt/_5pt/_6pt) plus rank scope.
             Variants share a concept_key so mine-time selection picks the
             league-matching variant — a TEP league is mined in TEP space.
  legibility A (instantly legible to a league-mate) | B (one sentence) |
             C (composite; feeds models, rarely surfaced as copy).
             Per the nugget contract: understandable beats technically
             correct, so surfacing rank weights tier A > B >> C.

Usage:
    python -m multi_league.transformations.draft.mining_feature_registry \
        [--model docs/semantic-data-model.json] [--out docs/mining-feature-registry.json]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

REGISTRY_VERSION = "mining-feature-registry-v1"

# ---------------------------------------------------------------------------
# Family contracts
# ---------------------------------------------------------------------------

#: Families whose values are realized outcomes. Valid mining TARGETS,
#: forbidden as features — this is the leakage firewall.
OUTCOME_FAMILIES = {"lamar", "draft_quality", "optimal", "clutch", "percentile"}

FAMILY_ROLES: dict[str, str] = {
    "lamar": "outcome",
    "draft_quality": "outcome",
    "optimal": "outcome",
    "clutch": "outcome",
    "percentile": "outcome",
    "rank": "feature",
    "fantasy_scoring": "feature",
    "base_metric": "feature",
    "keeper": "context",
    "dynasty": "context",
    "context": "context",
    "standings_context": "context",
    "identity": "join_key",
    "source_internal": "excluded",
    # 1,367 columns land in "other" — kept visible as backlog, not silently
    # dropped and not silently mined. Promotion into feature/context is a
    # deliberate reviewed change to FAMILY_OVERRIDES.
    "other": "backlog",
}

#: Table-scoped role overrides — the deliberate promotion/demotion path when
#: the semantic family is too coarse for mining. Two known coarse spots:
#: (1) the draft table is family=draft_quality wholesale, but pick CONTEXT
#: (round/cost/position/...) is knowable at pick time and is feature fuel,
#: not outcome; (2) player_bio's affinity vocabulary (college, RAS, combine,
#: draft capital) sits in family=other -> backlog, yet it is the Tier-A fun
#: vocabulary the nugget contract wants most.
_DRAFT = "___leagues.public.draft"
_BIO = "___ops.nfl_historical.player_bio"
TABLE_COLUMN_ROLE_OVERRIDES: dict[tuple[str, str], str] = {
    # Draft pick context — knowable the moment the pick happens.
    **{(_DRAFT, column): "feature" for column in (
        "round", "pick", "pick_in_round", "draft_slot", "overall_pick",
        "cost", "cost_bucket", "position", "draft_type", "draft_category",
        "auto_drafted", "nfl_team", "draft_age",
        "expected_age", "draft_age_zscore", "draft_age_grade",
        "position_draft_rank", "position_draft_label",
        "starter_slots_available", "drafted_as_starter", "drafted_as_backup",
        "platform", "nominated_by", "trade_locked",
    )},
    # nfl_team_api duplicates nfl_team's values -> identical duplicate
    # nuggets (caught by the 16-league QA pass).
    (_DRAFT, "nfl_team_api"): "excluded",
    (_DRAFT, "is_keeper"): "context",
    (_DRAFT, "draft_slot_roster_id"): "excluded",
    # NFL-draft-position copies on the season stats table: 'rd'/'pick' are
    # not season performance; bio draft capital is the canonical source.
    ("___ops.nfl_historical.player_nfl_season", "rd"): "excluded",
    ("___ops.nfl_historical.player_nfl_season", "pick"): "excluded",
    ("___ops.nfl_historical.player_nfl_season", "ovr"): "excluded",
    # Bio affinity vocabulary — static, and the most legible nuggets we have.
    **{(_BIO, column): "feature" for column in (
        "nfl_position", "position_category", "position_side", "height",
        "weight", "college", "conference", "draft_year", "draft_round",
        "draft_overall", "nfl_draft_team", "age_at_draft", "is_undrafted",
        "rookie_year", "forty", "bench", "vertical", "broad_jump", "cone",
        "shuttle", "ras_score", "high_school", "birth_place",
    )},
}

#: Column-level overrides for cases the family alone gets wrong.
#: (table suffix match, column) -> role
COLUMN_ROLE_OVERRIDES: dict[str, str] = {
    # Realized usage/availability on the draft row is outcome, not feature
    # (mirrors the wide miner's old hand list — kept as contract).
    "total_fantasy_points": "outcome",
    "season_ppg": "outcome",
    "games_played": "outcome",
    "games_started": "outcome",
    "games_eligible": "outcome",
    "weeks_rostered": "outcome",
    "weeks_started": "outcome",
    "season_position_rank": "outcome",
    "position_activation_rate": "outcome",
    "position_failure_rate": "outcome",
    "failure_rate": "outcome",
    "bench_insurance_discount": "outcome",
    "kept_next_year": "outcome",
    # As-of-now fields that leak the future relative to historical drafts.
    "latest_team": "excluded",
    "status": "excluded",
    "headshot_url": "excluded",
}

# ---------------------------------------------------------------------------
# Temporal contracts by table
# ---------------------------------------------------------------------------

TABLE_TEMPORAL: dict[str, str] = {
    "___ops.nfl_historical.nfl_player_stats_all": "lagged_prior_season",
    "___ops.nfl_historical.player_nfl_season": "lagged_prior_season",
    # Career tables aggregate as-of-now: joining them to a historical draft
    # leaks the player's future. Excluded from draft-time mining until an
    # as-of-year rebuild exists.
    "___ops.nfl_historical.player_nfl_career": "leaky_aggregate",
    "___ops.nfl_historical.player_bio": "static",
    "___ops.nfl_historical.nfl_franchise_eras": "static",
    "___leagues.public.draft": "draft_time",
    "___leagues.public.draft_manager_season": "manager_prior_season",
    "___leagues.public.league_settings": "config",
    "___leagues.public.keeper_config": "config",
    "___leagues.public.matchup": "manager_prior_season",
    "___leagues.public.matchup_season": "manager_prior_season",
    "___leagues.public.standings_by_year": "manager_prior_season",
    "___leagues.public.homepage_current_standings": "leaky_aggregate",
    "___leagues.public.homepage_league_summary": "leaky_aggregate",
    "___leagues.public.homepage_manager_rankings": "leaky_aggregate",
    "___leagues.public.homepage_top_rivalries": "leaky_aggregate",
    "___leagues.public.player_fantasy": "lagged_prior_season",
    "___leagues.public.player_fantasy_season": "lagged_prior_season",
    "___leagues.public.player_fantasy_season_all": "lagged_prior_season",
    "___leagues.public.player_fantasy_career": "leaky_aggregate",
    "___leagues.public.player_fantasy_career_all": "leaky_aggregate",
    "___leagues.public.transactions": "in_season",
    "___leagues.public.transaction_manager_season": "manager_prior_season",
}

#: Temporal classes safe to feed DRAFT-TIME feature vectors.
DRAFT_SAFE_TEMPORAL = {"draft_time", "static", "lagged_prior_season", "manager_prior_season"}

# ---------------------------------------------------------------------------
# Config-variant parsing
# ---------------------------------------------------------------------------

SCORING_VARIANT_RE = re.compile(r"_(0ppr|half|ppr|tep|ppfd)(?=_|$)")
QB_VARIANT_RE = re.compile(r"_(4pt|5pt|6pt)(?=_|$)")
RANK_SCOPE_RE = re.compile(r"^rank_(season|alltime|week)?_?")
#: Point-weight recomputes of the same stat (pts_rush_att_p1/_p2/_p25...):
#: highly correlated flavors that must share one concept, or one manager
#: tendency renders as five near-identical nuggets.
POINT_WEIGHT_RE = re.compile(r"_(p\d+)(?=_|$)")
PTS_TRAILING_NUM_RE = re.compile(r"_(\d+)$")


def parse_config_axis(column: str) -> dict[str, Any]:
    scoring = SCORING_VARIANT_RE.search(column)
    qb = QB_VARIANT_RE.search(column)
    point_weight = POINT_WEIGHT_RE.search(column)
    rank_scope = None
    if column.startswith("rank_"):
        match = RANK_SCOPE_RE.match(column)
        if match and match.group(1):
            rank_scope = match.group(1)

    concept_key = SCORING_VARIANT_RE.sub("", QB_VARIANT_RE.sub("", column))
    concept_key = POINT_WEIGHT_RE.sub("", concept_key)
    if concept_key.startswith(("pts_", "fpts")):
        concept_key = PTS_TRAILING_NUM_RE.sub("", concept_key)
    # "_ret" (with-return-yards) recomputes render identically to the base
    # stat — same concept, or one tendency becomes two duplicate nuggets.
    if concept_key.endswith("_ret"):
        concept_key = concept_key[: -len("_ret")]
    return {
        "concept_key": concept_key,
        "scoring_variant": scoring.group(1) if scoring else None,
        "qb_variant": qb.group(1) if qb else None,
        "point_weight": point_weight.group(1) if point_weight else None,
        "rank_scope": rank_scope,
        "is_config_variant": bool(scoring or qb),
    }


# ---------------------------------------------------------------------------
# Legibility tiers (nugget contract: understandable beats technically correct)
# ---------------------------------------------------------------------------

#: Instantly legible to any league-mate, by column name.
TIER_A_COLUMNS = {
    "age", "age_at_draft", "draft_age", "college", "conference", "height",
    "weight", "nfl_team", "nfl_team_api", "nfl_draft_team", "position",
    "nfl_position", "round", "pick", "cost", "draft_round", "draft_overall",
    "is_undrafted", "rookie_year", "draft_year", "birth_place", "high_school",
    "auto_drafted", "is_keeper", "draft_type", "forty", "ras_score",
}

#: One-sentence-of-explanation stats, by column-name fragment.
TIER_B_FRAGMENTS = (
    "target", "carries", "touches", "receptions", "receiving", "rushing",
    "passing", "yards", "td", "attempts", "interception", "fumble", "wopr",
    "air_yards", "racr", "adot", "snap", "ppg", "points", "rolling",
    "win", "loss", "streak", "playoff", "champion", "seed", "margin",
)


# ---------------------------------------------------------------------------
# Importance tiers (Joe, 2026-07-06): confidence and importance are separate
# dials. Hometown is trivia unless the signal is absolutely crazy; a capital
# tendency is strategic at ordinary evidence. 3 = strategic (changes how you
# draft against this manager/league), 2 = character (fun, mildly strategic),
# 1 = trivia (pure fun — needs extreme evidence to earn airtime).
# ---------------------------------------------------------------------------

IMPORTANCE_TRIVIA_COLUMNS = {"birth_place", "high_school"}
IMPORTANCE_CHARACTER_COLUMNS = {
    "college", "conference", "nfl_team", "nfl_team_api", "nfl_draft_team",
    "height", "weight", "forty", "bench", "vertical", "broad_jump", "cone",
    "shuttle", "ras_score", "age_at_draft", "is_undrafted", "rookie_year",
    "draft_year", "position_side", "position_category", "high_school",
    "birth_place", "draft_age", "expected_age", "draft_age_zscore",
    "draft_age_grade",
}


def importance_tier(column: str, family: str, role: str) -> int:
    if role not in ("feature", "context"):
        return 1
    if column in IMPORTANCE_TRIVIA_COLUMNS:
        return 1
    if column in IMPORTANCE_CHARACTER_COLUMNS:
        return 2
    # Everything performance/market shaped — prior-season stats, ranks,
    # scoring components, draft/pick context — is strategic.
    return 3


def legibility_tier(column: str, family: str, role: str) -> str:
    if role not in ("feature", "context"):
        return "C"
    lower = column.lower()
    if lower in TIER_A_COLUMNS:
        return "A"
    if any(fragment in lower for fragment in TIER_B_FRAGMENTS):
        return "B"
    if family in ("base_metric", "standings_context"):
        return "B"
    return "C"


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------


def find_repo_root(start: Path) -> Path:
    node = start.resolve()
    while node != node.parent:
        if (node / "docs" / "semantic-data-model.json").exists():
            return node
        node = node.parent
    raise FileNotFoundError("Could not locate docs/semantic-data-model.json above " + str(start))


def compile_registry(model: dict[str, Any]) -> dict[str, Any]:
    fields = model["columnFields"]
    idx = {name: position for position, name in enumerate(fields)}
    entries: list[dict[str, Any]] = []

    for row in model["columns"]:
        table = row[idx["table"]]
        column = row[idx["column"]]
        family = row[idx["family"]] or "other"
        high_value = bool(row[idx["highValue"]])
        semantic_name = row[idx["semanticName"]] or column

        role = TABLE_COLUMN_ROLE_OVERRIDES.get(
            (table, column),
            COLUMN_ROLE_OVERRIDES.get(column, FAMILY_ROLES.get(family, "backlog")),
        )
        # The draft table is outcome-dense (grades, scores, realized value),
        # so feature/context roles there are ALLOWLIST-ONLY: anything not
        # explicitly reviewed is treated as an outcome (e.g. keeper_draft_score
        # is a realized same-year grade, not a keeper flag). A future column
        # landing on the draft table can never silently enter mining.
        if (
            table == _DRAFT
            and role in ("feature", "context")
            and (table, column) not in TABLE_COLUMN_ROLE_OVERRIDES
        ):
            role = "outcome"
        temporal = TABLE_TEMPORAL.get(table, "leaky_aggregate")
        config = parse_config_axis(column)
        tier = legibility_tier(column, family, role)
        draft_time_eligible = (
            role == "feature" and temporal in DRAFT_SAFE_TEMPORAL
        )
        waiver_eligible = role == "feature" and temporal in (
            DRAFT_SAFE_TEMPORAL | {"in_season"}
        )

        entries.append({
            "table": table,
            "column": column,
            "family": family,
            "high_value": high_value,
            "semantic_name": semantic_name,
            "role": role,
            "temporal": temporal,
            "legibility": tier,
            "importance": importance_tier(column, family, role),
            "draft_time_eligible": draft_time_eligible,
            "waiver_eligible": waiver_eligible,
            **config,
        })

    role_counts = Counter(entry["role"] for entry in entries)
    draft_features = [entry for entry in entries if entry["draft_time_eligible"]]
    concept_keys = {
        (entry["table"], entry["concept_key"]) for entry in draft_features
    }
    summary = {
        "version": REGISTRY_VERSION,
        "source_generated_at": model.get("generatedAt"),
        "columns": len(entries),
        "roles": dict(role_counts),
        "draft_time_features": len(draft_features),
        "draft_time_concepts": len(concept_keys),
        "draft_time_by_tier": dict(Counter(entry["legibility"] for entry in draft_features)),
        "outcome_targets": role_counts.get("outcome", 0),
    }

    relationships = [
        {
            "id": rel.get("id"),
            "label": rel.get("label"),
            "strength": rel.get("strength"),
            "columns": rel.get("columns", []),
            "tables": rel.get("tables", []),
            "guidance": rel.get("guidance"),
        }
        for rel in model.get("relationships", [])
    ]

    return {"summary": summary, "relationships": relationships, "entries": entries}


def league_scoring_config(settings_row: dict[str, Any] | None) -> dict[str, str]:
    """Map a league_settings row to registry variant tokens.

    A league is mined in ITS OWN scoring space: TE-premium leagues get the
    _tep variants, half-PPR gets _half, 6-point-passing-TD leagues get _6pt.
    Unknown settings fall back to ppr/4pt (the most common fleet config).
    """
    if settings_row is None:
        return {"scoring_variant": "ppr", "qb_variant": "4pt"}
    row = settings_row

    def numeric(key: str) -> float:
        try:
            value = row.get(key)
            return float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    if numeric("scoring_bonus_rec_te") > 0:
        scoring = "tep"
    else:
        rec = numeric("scoring_rec")
        scoring = "ppr" if rec >= 0.75 else "half" if rec >= 0.25 else "0ppr"

    pass_td = numeric("scoring_pass_td")
    qb = "6pt" if pass_td >= 5.5 else "5pt" if pass_td >= 4.5 else "4pt"
    return {"scoring_variant": scoring, "qb_variant": qb}


#: Legibility sort order for candidate ranking (nugget contract).
_TIER_ORDER = {"A": 0, "B": 1, "C": 2}


def select_miner_features(
    registry: dict[str, Any],
    *,
    tables: set[str],
    league_config: dict[str, str] | None = None,
    max_features: int = 300,
) -> list[dict[str, Any]]:
    """Choose the sanctioned feature entries for one league's mining run.

    - Only draft-time-eligible entries from the requested tables.
    - Config-variant columns are kept only when they match the league's own
      scoring space (no league_config -> variants dropped entirely).
    - One entry per (table, concept_key).
    - Ranked legibility-first (A > B > C, high-value first within a tier)
      and capped, so the fun vocabulary always survives the cap.
    """
    config = league_config or {}
    selected: dict[tuple[str, str], dict[str, Any]] = {}

    for entry in registry["entries"]:
        if not entry["draft_time_eligible"] or entry["table"] not in tables:
            continue
        if entry["is_config_variant"]:
            if not config:
                continue
            if entry["scoring_variant"] not in (None, config.get("scoring_variant")):
                continue
            if entry["qb_variant"] not in (None, config.get("qb_variant")):
                continue
        key = (entry["table"], entry["concept_key"])
        current = selected.get(key)
        if current is None or _entry_rank(entry) < _entry_rank(current):
            selected[key] = entry

    ranked = sorted(selected.values(), key=_entry_rank)
    return ranked[:max_features]


def _entry_rank(entry: dict[str, Any]) -> tuple:
    return (
        _TIER_ORDER.get(entry["legibility"], 3),
        # Prefer the plain stat over its point-weight recomputes.
        0 if entry.get("point_weight") is None else 1,
        0 if entry["high_value"] else 1,
        entry["table"],
        entry["column"],
    )


def load_registry(path: Path | None = None) -> dict[str, Any]:
    """Load the checked-in registry artifact (miners call this)."""
    if path is None:
        path = find_repo_root(Path(__file__)) / "docs" / "mining-feature-registry.json"
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def draft_feature_entries(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Draft-time-safe feature universe — the only sanctioned miner input."""
    return [entry for entry in registry["entries"] if entry["draft_time_eligible"]]


def outcome_columns(registry: dict[str, Any]) -> set[tuple[str, str]]:
    """(table, column) pairs that must never appear in a feature vector."""
    return {
        (entry["table"], entry["column"])
        for entry in registry["entries"]
        if entry["role"] in ("outcome", "excluded")
        or entry["temporal"] == "leaky_aggregate"
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compile the mining feature registry")
    parser.add_argument("--model", type=Path, default=None, help="Path to semantic-data-model.json")
    parser.add_argument("--out", type=Path, default=None, help="Output registry JSON path")
    args = parser.parse_args()

    repo_root = find_repo_root(Path(__file__))
    model_path = args.model or repo_root / "docs" / "semantic-data-model.json"
    out_path = args.out or repo_root / "docs" / "mining-feature-registry.json"

    with open(model_path, encoding="utf-8") as handle:
        model = json.load(handle)

    registry = compile_registry(model)
    with open(out_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(registry, handle, indent=1)
        handle.write("\n")

    summary = registry["summary"]
    print(f"Wrote {out_path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
