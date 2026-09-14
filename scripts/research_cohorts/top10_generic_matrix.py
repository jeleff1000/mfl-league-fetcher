"""Sparse sample-recomputable boards for Draft and Transactions facts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Literal, Sequence

import numpy as np
import pandas as pd
from scipy import sparse


Direction = Literal["asc", "desc"]


def _divide(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    out = np.full(np.broadcast_shapes(numerator.shape, denominator.shape), np.nan)
    np.divide(numerator, denominator, out=out, where=denominator != 0)
    return out


@dataclass(frozen=True)
class SparseResearchBoard:
    leagues: tuple[str, ...]
    players: tuple[str, ...]
    positions: np.ndarray
    position_groups: np.ndarray
    facts: dict[str, sparse.csr_matrix]
    eligibility: dict[str, np.ndarray]
    direct_values: dict[str, np.ndarray]

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        fact_columns: Sequence[str],
        eligibility: pd.DataFrame | None = None,
        direct_values: pd.DataFrame | None = None,
    ) -> "SparseResearchBoard":
        required = {"db_name", "NFL_player_id", "position", *fact_columns}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"fact frame missing columns: {', '.join(missing)}")
        rows = frame.copy()
        rows["db_name"] = rows["db_name"].astype(str)
        rows["NFL_player_id"] = rows["NFL_player_id"].astype(str)
        rows["position"] = rows["position"].fillna("").astype(str).str.upper()
        leagues = set(rows["db_name"])
        if eligibility is not None:
            leagues.update(eligibility["db_name"].astype(str))
        players = set(rows["NFL_player_id"])
        if direct_values is not None:
            players.update(direct_values["NFL_player_id"].astype(str))
        league_names = tuple(sorted(leagues))
        player_names = tuple(sorted(players))
        if not league_names or not player_names:
            raise ValueError("fact frame must define leagues and players")
        li = {value: index for index, value in enumerate(league_names)}
        pi = {value: index for index, value in enumerate(player_names)}
        rows["_league"] = rows["db_name"].map(li)
        rows["_player"] = rows["NFL_player_id"].map(pi)
        shape = len(league_names), len(player_names)
        facts: dict[str, sparse.csr_matrix] = {}
        for column in fact_columns:
            values = pd.to_numeric(rows[column], errors="coerce").fillna(0.0).to_numpy(float)
            matrix = sparse.coo_matrix((values, (rows["_league"], rows["_player"])), shape=shape).tocsr()
            matrix.sum_duplicates()
            facts[column] = matrix

        player_position = rows[["NFL_player_id", "position"]].drop_duplicates()
        conflicting = player_position.groupby("NFL_player_id")["position"].nunique()
        if (conflicting > 1).any():
            raise ValueError("players have conflicting positions")
        lookup = dict(zip(player_position["NFL_player_id"], player_position["position"], strict=False))
        positions = np.asarray([lookup.get(player, "") for player in player_names])
        groups = np.where(np.isin(positions, ("K", "DEF")), positions, "SKILL")

        eligible: dict[str, np.ndarray] = {
            "SKILL": np.ones(len(league_names)),
            "K": np.ones(len(league_names)),
            "DEF": np.ones(len(league_names)),
        }
        if eligibility is not None:
            required_eligibility = {"db_name", "position_group", "eligible"}
            missing = sorted(required_eligibility - set(eligibility.columns))
            if missing:
                raise ValueError(f"eligibility frame missing columns: {', '.join(missing)}")
            eligible = {}
            e = eligibility.copy()
            e["db_name"] = e["db_name"].astype(str)
            for group, group_rows in e.groupby("position_group", sort=False):
                vector = np.zeros(len(league_names))
                for row in group_rows.itertuples():
                    vector[li[row.db_name]] = float(row.eligible)
                eligible[str(group).upper()] = vector

        direct: dict[str, np.ndarray] = {}
        if direct_values is not None:
            values = direct_values.copy()
            values["NFL_player_id"] = values["NFL_player_id"].astype(str)
            for column in values.columns:
                if column == "NFL_player_id":
                    continue
                mapping = values.set_index("NFL_player_id")[column]
                direct[column] = pd.to_numeric(
                    pd.Series(player_names).map(mapping), errors="coerce"
                ).to_numpy(float)
        return cls(league_names, player_names, positions, groups, facts, eligible, direct)

    def _rows(self, sample: Iterable[str]) -> np.ndarray:
        names = tuple(str(value) for value in sample)
        if not names:
            raise ValueError("sample cannot be empty")
        if len(names) != len(set(names)):
            raise ValueError("sample contains duplicates")
        lookup = {value: index for index, value in enumerate(self.leagues)}
        unknown = sorted(set(names) - set(lookup))
        if unknown:
            raise ValueError(f"sample contains unknown leagues: {unknown[:5]}")
        return np.asarray([lookup[name] for name in names], dtype=int)

    def subset_leagues(self, leagues: Iterable[str]) -> "SparseResearchBoard":
        """Return the board restricted to an approved annual league pool."""
        selected = tuple(str(value) for value in leagues)
        rows = self._rows(selected)
        return replace(
            self,
            leagues=selected,
            facts={field: matrix[rows].tocsr() for field, matrix in self.facts.items()},
            eligibility={
                group: np.asarray(vector)[rows]
                for group, vector in self.eligibility.items()
            },
        )

    def _sum(self, field: str, sample_rows: np.ndarray) -> np.ndarray:
        if field not in self.facts:
            raise ValueError(f"unknown fact: {field}")
        return np.asarray(self.facts[field][sample_rows].sum(axis=0)).ravel()

    def _eligible(self, sample_rows: np.ndarray) -> np.ndarray:
        counts = {
            group: float(vector[sample_rows].sum())
            for group, vector in self.eligibility.items()
        }
        return np.asarray([counts.get(str(group), 0.0) for group in self.position_groups])

    def metric_rows(
        self,
        sample: Iterable[str],
        *,
        numerator: str,
        denominator: str,
        support: str,
        position: str,
        pool_support: str | None = None,
        minimum_pool_rate: float | None = None,
    ) -> pd.DataFrame:
        sample_rows = self._rows(sample)
        eligible = self._eligible(sample_rows)
        if denominator == "direct":
            if numerator not in self.direct_values:
                raise ValueError(f"unknown direct value: {numerator}")
            values = self.direct_values[numerator].copy()
        else:
            numerators = self._sum(numerator, sample_rows)
            denominators = eligible if denominator == "eligible_leagues" else self._sum(denominator, sample_rows)
            values = _divide(numerators, denominators)
        supports = eligible if support == "eligible_leagues" else self._sum(support, sample_rows)
        candidates = np.isfinite(values) & (supports > 0)
        if position != "ALL":
            candidates &= self.positions == position.upper()
        if minimum_pool_rate is not None:
            if pool_support is None:
                raise ValueError("minimum_pool_rate requires pool_support")
            pool = self._sum(pool_support, sample_rows)
            candidates &= _divide(pool, eligible) >= float(minimum_pool_rate)
        return pd.DataFrame(
            {
                "NFL_player_id": np.asarray(self.players)[candidates],
                "position": self.positions[candidates],
                "metric": values[candidates],
                "support": supports[candidates],
            }
        )

    def ordered_top10(self, sample: Iterable[str], *, direction: Direction = "desc", **kwargs) -> tuple[str, ...]:
        rows = self.metric_rows(sample, **kwargs)
        ordered = rows.sort_values(
            ["metric", "support", "NFL_player_id"],
            ascending=[direction == "asc", False, True],
            kind="mergesort",
        )
        return tuple(ordered["NFL_player_id"].head(10))

    def batched_top10(
        self,
        samples: Sequence[Sequence[str]],
        *,
        numerator: str,
        denominator: str,
        support: str,
        position: str,
        pool_support: str | None = None,
        minimum_pool_rate: float | None = None,
        direction: Direction = "desc",
    ) -> tuple[tuple[str, ...], ...]:
        """Rebuild many sample boards with one sparse multiplication per fact."""
        sample_rows = [self._rows(sample) for sample in samples]
        row_index = np.concatenate(
            [np.full(len(indices), row, dtype=int) for row, indices in enumerate(sample_rows)]
        )
        column_index = np.concatenate(sample_rows)
        indicators = sparse.csr_matrix(
            (np.ones(len(column_index)), (row_index, column_index)),
            shape=(len(sample_rows), len(self.leagues)),
        )

        def summed(field: str) -> np.ndarray:
            if field not in self.facts:
                raise ValueError(f"unknown fact: {field}")
            return (indicators @ self.facts[field]).toarray()

        eligible = np.zeros((len(samples), len(self.players)))
        for group, vector in self.eligibility.items():
            player_mask = self.position_groups == group
            if player_mask.any():
                eligible[:, player_mask] = np.asarray(indicators @ vector).reshape(-1, 1)
        if denominator == "direct":
            if numerator not in self.direct_values:
                raise ValueError(f"unknown direct value: {numerator}")
            values = np.broadcast_to(self.direct_values[numerator], eligible.shape).copy()
        else:
            numerators = summed(numerator)
            denominators = eligible if denominator == "eligible_leagues" else summed(denominator)
            values = _divide(numerators, denominators)
        supports = eligible if support == "eligible_leagues" else summed(support)
        candidates = np.isfinite(values) & (supports > 0)
        if position != "ALL":
            candidates &= self.positions.reshape(1, -1) == position.upper()
        if minimum_pool_rate is not None:
            if pool_support is None:
                raise ValueError("minimum_pool_rate requires pool_support")
            candidates &= _divide(summed(pool_support), eligible) >= float(minimum_pool_rate)

        player_ids = np.asarray(self.players)
        boards: list[tuple[str, ...]] = []
        for row in range(len(samples)):
            indexes = np.flatnonzero(candidates[row])
            if not len(indexes):
                boards.append(())
                continue
            metric_key = values[row, indexes] if direction == "asc" else -values[row, indexes]
            order = np.lexsort((player_ids[indexes], -supports[row, indexes], metric_key))
            boards.append(tuple(player_ids[indexes[order[:10]]]))
        return tuple(boards)
