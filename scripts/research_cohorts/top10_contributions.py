"""Additive league/player facts used by the annual top-10 resampler."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from top10_rank import BoardSpec


REQUIRED_COLUMNS = {
    "dataset",
    "grain",
    "metric",
    "db_name",
    "year",
    "week",
    "NFL_player_id",
    "position",
    "numerator",
    "denominator",
    "support",
    "pool_support",
    "aggregation",
    "denominator_mode",
}


# source_metric -> (numerator column, denominator column, support column,
#                   aggregation, denominator mode)
_WIDE_FACTS: dict[tuple[str, str], tuple[str, str | None, str | None, str, str]] = {
    ("matchup", "start_rate"): ("started_active_weeks", None, None, "ratio", "sampled_league_units"),
    ("matchup", "start_rate_pct"): ("started_active_weeks", None, None, "ratio", "sampled_league_units"),
    ("matchup", "win_rate"): ("wins_started_active", None, None, "ratio", "sampled_league_units"),
    ("matchup", "won_pct"): ("wins_started_active", None, None, "ratio", "sampled_league_units"),
    ("matchup", "expected_wins"): ("wins_started_active", None, None, "ratio", "sampled_leagues"),
    ("matchup", "expected_starts"): ("started_active_weeks", None, None, "ratio", "sampled_leagues"),
    ("matchup", "total_lamar_started"): ("sum_lamar_started", None, "started_weeks", "ratio", "sampled_leagues"),
    ("matchup", "avg_clutch_started"): ("sum_clutch_started_active", None, "started_weeks", "ratio", "sampled_leagues"),
    ("matchup", "champ_total_pct"): ("n_champ_leagues", None, None, "ratio", "sampled_leagues"),
    ("matchup", "champ_as_starter_pct"): ("n_champ_start_leagues", None, None, "ratio", "sampled_leagues"),
    ("matchup", "playoff_total_pct"): ("n_final_po", None, None, "ratio", "sampled_leagues"),
    ("matchup", "playoff_as_starter_pct"): ("n_started_po", None, None, "ratio", "sampled_leagues"),
    ("draft", "adp"): ("sum_adp_pick", "n_adp_pick", "n_drafted", "ratio", "observed"),
    ("draft", "draft_rate_pct"): ("n_drafted", None, "n_drafted", "ratio", "sampled_leagues"),
    ("draft", "cost_pct"): ("sum_auction_cost_pct", "n_auction_cost_pct", "n_auction_leagues", "ratio", "observed"),
    ("transactions", "add_rate_pct"): ("n_add_leagues", None, "n_add_leagues", "ratio", "sampled_leagues"),
    ("transactions", "avg_faab_pct"): ("sum_faab_pct", "n_faab_pct", "n_add_leagues", "ratio", "observed"),
    ("transactions", "add_lamar"): ("sum_add_lamar", "n_add_lamar", "n_add_leagues", "ratio", "observed"),
    ("transactions", "avg_add_lamar"): ("sum_add_lamar", "n_add_lamar", "n_add_leagues", "ratio", "observed"),
    ("transactions", "avg_drop_regret"): ("sum_drop_regret", "n_drop_regret", "n_drop_leagues", "ratio", "observed"),
}


def contributions_from_wide(frame: pd.DataFrame, spec: BoardSpec) -> pd.DataFrame:
    """Project cached production sufficient stats into one metric's additive rows."""

    key = (spec.dataset, spec.source_metric)
    try:
        numerator_col, denominator_col, support_col, aggregation, denominator_mode = _WIDE_FACTS[key]
    except KeyError as exc:
        raise ValueError(
            f"no sufficient-stat mapping for {spec.dataset}/{spec.source_metric}"
        ) from exc
    required = {"db_name", "year", "NFL_player_id", "position", numerator_col}
    if denominator_col:
        required.add(denominator_col)
    if support_col:
        required.add(support_col)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"wide contribution frame missing columns: {', '.join(missing)}")

    out = pd.DataFrame(
        {
            "dataset": spec.dataset,
            "grain": spec.grain,
            "metric": spec.metric,
            "db_name": frame["db_name"].astype(str),
            "year": pd.to_numeric(frame["year"], errors="raise").astype(int),
            "week": frame["week"] if "week" in frame.columns else None,
            "NFL_player_id": frame["NFL_player_id"].astype(str),
            "position": frame["position"].astype(str).str.upper(),
            "numerator": pd.to_numeric(frame[numerator_col], errors="coerce"),
            "denominator": (
                pd.to_numeric(frame[denominator_col], errors="coerce")
                if denominator_col
                else 0.0
            ),
            "support": (
                pd.to_numeric(frame[support_col], errors="coerce")
                if support_col
                else 0.0
            ),
            "pool_support": 0.0,
            "aggregation": aggregation,
            "denominator_mode": denominator_mode,
        }
    )
    if spec.dataset == "draft" and "n_drafted" in frame.columns:
        out["pool_support"] = pd.to_numeric(frame["n_drafted"], errors="coerce").fillna(0)
    elif spec.dataset == "transactions" and "n_add_leagues" in frame.columns:
        out["pool_support"] = pd.to_numeric(frame["n_add_leagues"], errors="coerce").fillna(0)
    return out


@dataclass(frozen=True)
class MaterializedArtifact:
    path: str
    rows: int
    sha256: str


@dataclass(frozen=True)
class ContributionManifest:
    years: tuple[int, ...]
    datasets: tuple[str, ...]
    artifacts: dict[str, MaterializedArtifact]


def contribution_queries(
    year: int,
    *,
    datasets: Sequence[str] = ("matchup", "draft", "transactions"),
) -> dict[str, str]:
    """Return production-derived per-league sufficient-stat queries for one year."""

    requested = tuple(dict.fromkeys(str(dataset) for dataset in datasets))
    unknown = sorted(set(requested) - {"matchup", "draft", "transactions"})
    if unknown:
        raise ValueError(f"unknown contribution datasets: {', '.join(unknown)}")
    queries: dict[str, str] = {}
    if "matchup" in requested:
        from top10_extraction_sql import (
            light_matchup_sql,
            matchup_active_sql,
            matchup_week_population_sql,
        )

        queries[f"matchup_weekly_{int(year)}"] = light_matchup_sql(int(year))
        queries[f"matchup_population_weekly_{int(year)}"] = matchup_week_population_sql(
            int(year)
        )
        queries[f"matchup_active_{int(year)}"] = matchup_active_sql(int(year))
    if "draft" in requested:
        from top10_extraction_sql import light_draft_sql

        queries[f"draft_season_{int(year)}"] = light_draft_sql(int(year))
    if "transactions" in requested:
        from top10_extraction_sql import light_transaction_sql

        queries[f"transactions_weekly_{int(year)}"] = light_transaction_sql(int(year))
    return queries


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def materialize_queries(
    con: object,
    queries: Mapping[str, str],
    out_dir: Path | str,
) -> dict[str, MaterializedArtifact]:
    """Execute named read-only queries once and cache their rows as Parquet."""

    destination = Path(out_dir)
    destination.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, MaterializedArtifact] = {}
    for name, sql in queries.items():
        if not name or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for character in name.lower()):
            raise ValueError(f"invalid materialized query name: {name!r}")
        path = destination / f"{name}.parquet"
        escaped = path.as_posix().replace("'", "''")
        con.execute(f"COPY ({sql}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        rows = int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
        artifacts[name] = MaterializedArtifact(
            path=str(path),
            rows=rows,
            sha256=_file_sha256(path),
        )
    return artifacts


def materialize_contributions(
    reader: object,
    *,
    years: Sequence[int],
    out_path: Path | str,
    datasets: Sequence[str] = ("matchup", "draft", "transactions"),
    query_builder: Callable[..., Mapping[str, str]] = contribution_queries,
) -> ContributionManifest:
    """Cache every requested year's production-derived per-league facts once."""

    annual_years = tuple(dict.fromkeys(int(year) for year in years))
    selected_datasets = tuple(dict.fromkeys(str(dataset) for dataset in datasets))
    if not annual_years:
        raise ValueError("years cannot be empty")
    destination = Path(out_path)
    destination.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, MaterializedArtifact] = {}
    for year in annual_years:
        queries = query_builder(year, datasets=selected_datasets)
        overlap = sorted(set(queries) & set(artifacts))
        if overlap:
            raise ValueError(f"duplicate contribution artifact names: {', '.join(overlap)}")
        artifacts.update(materialize_queries(reader.con, queries, destination))
    manifest = ContributionManifest(
        years=annual_years,
        datasets=selected_datasets,
        artifacts=artifacts,
    )
    payload = {
        "years": list(manifest.years),
        "datasets": list(manifest.datasets),
        "artifacts": {name: asdict(artifact) for name, artifact in manifest.artifacts.items()},
    }
    (destination / "contribution_manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def aggregate_sample(
    contributions: pd.DataFrame,
    league_ids: Sequence[str],
    spec: BoardSpec,
    *,
    year: int | None = None,
    week: int | None = None,
    eligible_by_position: Mapping[str, int] | None = None,
    eligibility_units_by_player: Mapping[str, int | float] | None = None,
) -> pd.DataFrame:
    """Rebuild one metric board from additive contributions for selected leagues."""

    missing = sorted(REQUIRED_COLUMNS - set(contributions.columns))
    if missing:
        raise ValueError(f"contribution frame missing columns: {', '.join(missing)}")
    selected_ids = tuple(dict.fromkeys(str(league) for league in league_ids))
    if not selected_ids:
        raise ValueError("league sample cannot be empty")
    if len(selected_ids) != len(league_ids):
        raise ValueError("league sample contains duplicates")

    frame = contributions.loc[
        contributions["dataset"].eq(spec.dataset)
        & contributions["grain"].eq(spec.grain)
        & contributions["metric"].eq(spec.metric)
        & contributions["db_name"].isin(selected_ids)
    ].copy()
    if year is not None:
        frame = frame.loc[frame["year"].eq(int(year))].copy()
    if week is not None:
        frame = frame.loc[frame["week"].eq(int(week))].copy()
    years = set(pd.to_numeric(frame["year"], errors="coerce").dropna().astype(int))
    if len(years) > 1:
        raise ValueError("annual sample must contain a single year")
    if frame.empty:
        return pd.DataFrame(columns=["NFL_player_id", "position", spec.metric, spec.support_metric])

    keys = ["NFL_player_id", "position"]
    summary = (
        frame.groupby(keys, sort=False, dropna=False)
        .agg(
            numerator=("numerator", "sum"),
            denominator=("denominator", "sum"),
            support=("support", "sum"),
            pool_support=("pool_support", "sum"),
            aggregation=("aggregation", "first"),
            aggregation_count=("aggregation", "nunique"),
            denominator_mode=("denominator_mode", "first"),
            denominator_mode_count=("denominator_mode", "nunique"),
        )
        .reset_index()
    )
    if (summary["aggregation_count"] != 1).any():
        raise ValueError("one player cannot mix contribution aggregation modes")
    if (summary["denominator_mode_count"] != 1).any():
        raise ValueError("one player cannot mix contribution denominator modes")
    modes = set(summary["aggregation"].astype(str))
    unsupported = modes - {"ratio", "sampled_rate", "sum", "direct", "max", "min"}
    if unsupported:
        raise ValueError(f"unsupported contribution aggregation: {sorted(unsupported)}")

    values = np.full(len(summary), np.nan, dtype=float)
    ratio = summary["aggregation"].eq("ratio")
    position_counts: pd.Series | None = None
    sampled = summary["denominator_mode"].isin({"sampled_leagues", "sampled_league_units"})
    if sampled.any():
        if eligible_by_position is None:
            raise ValueError("sampled league denominators require eligible_by_position")
        normalized_counts = {
            str(position).upper(): int(count)
            for position, count in eligible_by_position.items()
        }
        position_counts = summary["position"].astype(str).str.upper().map(normalized_counts)
        if position_counts.loc[sampled].isna().any():
            missing_positions = sorted(set(summary.loc[sampled & position_counts.isna(), "position"].astype(str)))
            raise ValueError(f"missing eligible league counts for positions: {missing_positions}")
        sampled_leagues = summary["denominator_mode"].eq("sampled_leagues")
        summary.loc[sampled_leagues, "denominator"] = position_counts.loc[sampled_leagues]
        sampled_units = summary["denominator_mode"].eq("sampled_league_units")
        if sampled_units.any():
            if eligibility_units_by_player is None:
                raise ValueError(
                    "sampled league-unit denominators require eligibility_units_by_player"
                )
            units = summary["NFL_player_id"].astype(str).map(
                {str(player): float(value) for player, value in eligibility_units_by_player.items()}
            )
            if units.loc[sampled_units].isna().any():
                missing_players = sorted(set(summary.loc[sampled_units & units.isna(), "NFL_player_id"].astype(str)))
                raise ValueError(f"missing eligibility units for players: {missing_players}")
            summary.loc[sampled_units, "denominator"] = (
                position_counts.loc[sampled_units] * units.loc[sampled_units]
            )

    valid_ratio = ratio & summary["denominator"].ne(0)
    values[valid_ratio.to_numpy()] = (
        summary.loc[valid_ratio, "numerator"] / summary.loc[valid_ratio, "denominator"]
    )
    sampled_rate = summary["aggregation"].eq("sampled_rate")
    eligible_counts: pd.Series | None = None
    if sampled_rate.any():
        if eligible_by_position is None:
            raise ValueError("sampled_rate requires eligible_by_position")
        eligible_counts = summary["position"].map(
            {str(position).upper(): int(count) for position, count in eligible_by_position.items()}
        )
        missing_positions = summary.loc[sampled_rate & eligible_counts.isna(), "position"].unique()
        if len(missing_positions):
            raise ValueError(f"missing eligible counts for positions: {sorted(missing_positions)}")
        valid_sampled = sampled_rate & eligible_counts.gt(0)
        values[valid_sampled.to_numpy()] = (
            summary.loc[valid_sampled, "numerator"] / eligible_counts.loc[valid_sampled]
        )
    summed = summary["aggregation"].eq("sum")
    values[summed.to_numpy()] = summary.loc[summed, "numerator"]
    direct = summary["aggregation"].eq("direct")
    values[direct.to_numpy()] = summary.loc[direct, "numerator"]
    maximum = summary["aggregation"].eq("max")
    minimum = summary["aggregation"].eq("min")
    # Pre-materialized extrema carry their selected value in numerator. Their
    # source extractor guarantees one row per league/player before this rollup.
    if maximum.any():
        maxima = frame.groupby(keys, dropna=False)["numerator"].max()
        values[maximum.to_numpy()] = [maxima.loc[(row.NFL_player_id, row.position)] for row in summary.loc[maximum].itertuples()]
    if minimum.any():
        minima = frame.groupby(keys, dropna=False)["numerator"].min()
        values[minimum.to_numpy()] = [minima.loc[(row.NFL_player_id, row.position)] for row in summary.loc[minimum].itertuples()]

    summary[spec.metric] = values
    summary[spec.support_metric] = pd.to_numeric(summary["support"], errors="coerce")
    if position_counts is not None and spec.support_metric in {
        "eligible_leagues", "champ_elig", "playoff_elig"
    }:
        summary.loc[sampled, spec.support_metric] = position_counts.loc[sampled]

    if spec.common_pool in {"drafted_3pct", "added_3pct_or_drop"}:
        if eligible_by_position is None:
            raise ValueError(f"{spec.common_pool} requires eligible_by_position")
        denominators = eligible_counts if eligible_counts is not None else summary["position"].map(
            {str(position).upper(): int(count) for position, count in eligible_by_position.items()}
        )
        rates = summary["pool_support"] / denominators.replace(0, np.nan)
        keep = rates.ge(0.03)
        if spec.common_pool == "added_3pct_or_drop" and spec.metric == "drop_regret":
            keep = summary["support"].gt(0)
        summary = summary.loc[keep].copy()
    elif spec.common_pool != "all":
        raise ValueError(f"unsupported common-pool rule: {spec.common_pool!r}")

    return (
        summary[["NFL_player_id", "position", spec.metric, spec.support_metric]]
        .sort_values("NFL_player_id", kind="stable")
        .reset_index(drop=True)
    )
