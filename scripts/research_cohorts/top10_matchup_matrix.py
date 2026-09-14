"""Sparse, sample-recomputable Matchup research boards.

The stability study repeatedly selects disjoint league samples.  Re-running the
lake SQL for every draw would be prohibitively expensive, so this module stores
the additive player/week primitives as sparse league-by-player matrices and
rebuilds the displayed metric from the selected rows.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from os import PathLike
from typing import Iterable, Sequence

import duckdb
import numpy as np
import pandas as pd
from scipy import sparse


_FACT_COLUMNS = (
    "rostered_leagues",
    "started_leagues",
    "wins_started",
    "losses_started",
    "points_started",
    "clutch_sum",
    "champ_started",
)
_START_SUPPORT_METRICS = {"ppg", "lamar", "clutch"}


def _safe_divide(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    result = np.full(np.broadcast_shapes(numerator.shape, denominator.shape), np.nan)
    np.divide(numerator, denominator, out=result, where=denominator != 0)
    return result


@dataclass(frozen=True)
class MatchupSparseBoard:
    """League-level sufficient statistics for one year and approved pool."""

    leagues: tuple[str, ...]
    players: tuple[str, ...]
    weeks: tuple[int, ...]
    positions: np.ndarray
    position_groups: np.ndarray
    facts: dict[str, dict[int, sparse.csr_matrix]]
    eligibility: dict[str, sparse.csr_matrix]
    active: sparse.csr_matrix
    direct_values: dict[str, np.ndarray]

    @classmethod
    def from_parquet_shards(
        cls,
        weekly_paths: Sequence[str | PathLike[str]],
        population_paths: Sequence[str | PathLike[str]],
        active_paths: Sequence[str | PathLike[str]],
    ) -> "MatchupSparseBoard":
        """Stream encoded parquet shards into sparse matrices one week at a time."""
        if not weekly_paths or not population_paths or not active_paths:
            raise ValueError("weekly, population, and active parquet paths are required")
        con = duckdb.connect()
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=1")
        con.from_parquet([str(path) for path in weekly_paths]).create_view("weekly")
        con.from_parquet([str(path) for path in population_paths]).create_view(
            "population"
        )
        con.from_parquet([str(path) for path in active_paths]).create_view("active_raw")

        weekly_columns = {
            str(row[0]) for row in con.execute("DESCRIBE weekly").fetchall()
        }
        required_weekly = {"db_name", "week", "NFL_player_id", "position"}
        missing = required_weekly - weekly_columns
        if missing:
            raise ValueError(f"weekly parquet missing columns: {sorted(missing)}")
        population_columns = {
            str(row[0]) for row in con.execute("DESCRIBE population").fetchall()
        }
        required_population = {
            "db_name",
            "week",
            "skill_eligible",
            "k_eligible",
            "def_eligible",
        }
        missing = required_population - population_columns
        if missing:
            raise ValueError(f"population parquet missing columns: {sorted(missing)}")
        active_columns = {
            str(row[0]) for row in con.execute("DESCRIBE active_raw").fetchall()
        }
        missing = {"NFL_player_id", "week"} - active_columns
        if missing:
            raise ValueError(f"active parquet missing columns: {sorted(missing)}")

        con.execute(
            """
            CREATE TEMP TABLE league_map AS
            SELECT db_name,ROW_NUMBER() OVER (ORDER BY db_name)-1 AS li
            FROM (
              SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name FROM weekly
              UNION
              SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name FROM population
            )
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE player_map AS
            WITH ids AS (
              SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id FROM weekly
              UNION
              SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id FROM active_raw
            ), pos AS (
              SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                     MAX(UPPER(CAST(position AS VARCHAR))) AS position,
                     COUNT(DISTINCT UPPER(CAST(position AS VARCHAR)))
                       FILTER (WHERE position IS NOT NULL) AS n_positions
              FROM weekly GROUP BY 1
            )
            SELECT ids.NFL_player_id,COALESCE(pos.position,'') AS position,
                   COALESCE(pos.n_positions,0) AS n_positions,
                   ROW_NUMBER() OVER (ORDER BY ids.NFL_player_id)-1 AS pi
            FROM ids LEFT JOIN pos USING (NFL_player_id)
            """
        )
        conflicts = con.execute(
            "SELECT NFL_player_id FROM player_map WHERE n_positions>1 LIMIT 5"
        ).fetchall()
        if conflicts:
            raise ValueError(
                f"players have conflicting positions: {[row[0] for row in conflicts]}"
            )
        con.execute(
            """
            CREATE TEMP TABLE week_map AS
            SELECT week,ROW_NUMBER() OVER (ORDER BY week)-1 AS wi
            FROM (
              SELECT DISTINCT CAST(week AS INTEGER) AS week FROM weekly
              UNION SELECT DISTINCT CAST(week AS INTEGER) AS week FROM population
              UNION SELECT DISTINCT CAST(week AS INTEGER) AS week FROM active_raw
            )
            """
        )

        leagues = tuple(
            row[0]
            for row in con.execute("SELECT db_name FROM league_map ORDER BY li").fetchall()
        )
        player_rows = con.execute(
            "SELECT NFL_player_id,position FROM player_map ORDER BY pi"
        ).fetchall()
        players = tuple(row[0] for row in player_rows)
        positions = np.asarray([row[1] for row in player_rows])
        weeks = tuple(
            int(row[0])
            for row in con.execute("SELECT week FROM week_map ORDER BY wi").fetchall()
        )
        if not leagues or not players or not weeks:
            raise ValueError("parquet shards must define leagues, players, and weeks")
        position_groups = np.where(
            np.isin(positions, ("K", "DEF")), positions, "SKILL"
        )
        fact_columns = tuple(
            column for column in _FACT_COLUMNS if column in weekly_columns
        )
        facts: dict[str, dict[int, sparse.csr_matrix]] = {
            column: {} for column in fact_columns
        }
        shape = (len(leagues), len(players))
        selected_columns = ",".join(f"w.{column}" for column in fact_columns)
        for selected_week in weeks:
            query = (
                "SELECT CAST(l.li AS INTEGER) AS li,CAST(p.pi AS INTEGER) AS pi"
                + (f",{selected_columns}" if selected_columns else "")
                + " FROM weekly w JOIN league_map l ON l.db_name=CAST(w.db_name AS VARCHAR)"
                + " JOIN player_map p ON p.NFL_player_id=CAST(w.NFL_player_id AS VARCHAR)"
                + " WHERE CAST(w.week AS INTEGER)=?"
            )
            arrays = con.execute(query, [int(selected_week)]).fetchnumpy()
            league_index = np.asarray(arrays["li"], dtype=np.int32)
            player_index = np.asarray(arrays["pi"], dtype=np.int32)
            for column in fact_columns:
                raw = arrays[column]
                if np.ma.isMaskedArray(raw):
                    raw = raw.filled(0)
                values = np.nan_to_num(np.asarray(raw, dtype=float), nan=0.0)
                nonzero = values != 0
                matrix = sparse.coo_matrix(
                    (
                        values[nonzero],
                        (league_index[nonzero], player_index[nonzero]),
                    ),
                    shape=shape,
                ).tocsr()
                matrix.sum_duplicates()
                facts[column][int(selected_week)] = matrix

        eligibility: dict[str, sparse.csr_matrix] = {}
        eligibility_shape = (len(leagues), len(weeks))
        for group, column in (
            ("SKILL", "skill_eligible"),
            ("K", "k_eligible"),
            ("DEF", "def_eligible"),
        ):
            arrays = con.execute(
                f"""
                SELECT CAST(l.li AS INTEGER) AS li,CAST(wm.wi AS INTEGER) AS wi,
                       p.{column} AS eligible
                FROM population p
                JOIN league_map l ON l.db_name=CAST(p.db_name AS VARCHAR)
                JOIN week_map wm ON wm.week=CAST(p.week AS INTEGER)
                """
            ).fetchnumpy()
            raw = arrays["eligible"]
            if np.ma.isMaskedArray(raw):
                raw = raw.filled(0)
            values = np.nan_to_num(np.asarray(raw, dtype=float), nan=0.0)
            nonzero = values != 0
            matrix = sparse.coo_matrix(
                (
                    values[nonzero],
                    (
                        np.asarray(arrays["li"], dtype=np.int32)[nonzero],
                        np.asarray(arrays["wi"], dtype=np.int32)[nonzero],
                    ),
                ),
                shape=eligibility_shape,
            ).tocsr()
            matrix.sum_duplicates()
            eligibility[group] = matrix

        arrays = con.execute(
            """
            SELECT CAST(p.pi AS INTEGER) AS pi,CAST(w.wi AS INTEGER) AS wi
            FROM (
              SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                              CAST(week AS INTEGER) AS week
              FROM active_raw
            ) a
            JOIN player_map p USING (NFL_player_id)
            JOIN week_map w USING (week)
            """
        ).fetchnumpy()
        active_matrix = sparse.coo_matrix(
            (
                np.ones(len(arrays["pi"]), dtype=float),
                (
                    np.asarray(arrays["pi"], dtype=np.int32),
                    np.asarray(arrays["wi"], dtype=np.int32),
                ),
            ),
            shape=(len(players), len(weeks)),
        ).tocsr()
        active_matrix.sum_duplicates()
        active_matrix.data[:] = 1.0
        con.close()
        return cls(
            leagues=leagues,
            players=players,
            weeks=weeks,
            positions=positions,
            position_groups=position_groups,
            facts=facts,
            eligibility=eligibility,
            active=active_matrix,
            direct_values={},
        )

    @classmethod
    def from_frames(
        cls,
        weekly: pd.DataFrame,
        eligibility: pd.DataFrame,
        active: pd.DataFrame,
    ) -> "MatchupSparseBoard":
        required_weekly = {"db_name", "week", "NFL_player_id", "position"}
        missing = required_weekly - set(weekly.columns)
        if missing:
            raise ValueError(f"weekly frame missing columns: {sorted(missing)}")
        required_eligibility = {"db_name", "week", "pos_grp", "eligible"}
        missing = required_eligibility - set(eligibility.columns)
        if missing:
            raise ValueError(f"eligibility frame missing columns: {sorted(missing)}")
        required_active = {"NFL_player_id", "week"}
        missing = required_active - set(active.columns)
        if missing:
            raise ValueError(f"active frame missing columns: {sorted(missing)}")

        weekly = weekly.copy()
        eligibility = eligibility.copy()
        active = active.copy()
        weekly["db_name"] = weekly["db_name"].astype(str)
        eligibility["db_name"] = eligibility["db_name"].astype(str)
        weekly["NFL_player_id"] = weekly["NFL_player_id"].astype(str)
        active["NFL_player_id"] = active["NFL_player_id"].astype(str)
        weekly["week"] = pd.to_numeric(weekly["week"], errors="raise").astype(int)
        eligibility["week"] = pd.to_numeric(
            eligibility["week"], errors="raise"
        ).astype(int)
        active["week"] = pd.to_numeric(active["week"], errors="raise").astype(int)

        leagues = tuple(
            sorted(set(weekly["db_name"]).union(eligibility["db_name"]))
        )
        players = tuple(
            sorted(set(weekly["NFL_player_id"]).union(active["NFL_player_id"]))
        )
        weeks = tuple(
            sorted(set(weekly["week"]).union(eligibility["week"]).union(active["week"]))
        )
        if not leagues or not players or not weeks:
            raise ValueError("weekly, eligibility, and active frames must define a board")

        league_index = {value: index for index, value in enumerate(leagues)}
        player_index = {value: index for index, value in enumerate(players)}
        week_index = {value: index for index, value in enumerate(weeks)}

        player_positions = (
            weekly[["NFL_player_id", "position"]]
            .dropna(subset=["position"])
            .drop_duplicates()
        )
        conflicting = player_positions.groupby("NFL_player_id")["position"].nunique()
        if (conflicting > 1).any():
            bad = conflicting[conflicting > 1].index.tolist()[:5]
            raise ValueError(f"players have conflicting positions: {bad}")
        position_lookup = dict(
            zip(
                player_positions["NFL_player_id"].astype(str),
                player_positions["position"].astype(str),
                strict=False,
            )
        )
        positions = np.asarray([position_lookup.get(player, "") for player in players])
        position_groups = np.where(
            np.isin(positions, ("K", "DEF")), positions, "SKILL"
        )

        weekly["_league"] = weekly["db_name"].map(league_index)
        weekly["_player"] = weekly["NFL_player_id"].map(player_index)
        facts: dict[str, dict[int, sparse.csr_matrix]] = {}
        shape = (len(leagues), len(players))
        for field in _FACT_COLUMNS:
            if field not in weekly:
                continue
            by_week: dict[int, sparse.csr_matrix] = {}
            for week, rows in weekly.groupby("week", sort=False):
                values = pd.to_numeric(rows[field], errors="coerce").fillna(0.0)
                matrix = sparse.coo_matrix(
                    (values.to_numpy(float), (rows["_league"], rows["_player"])),
                    shape=shape,
                ).tocsr()
                matrix.sum_duplicates()
                by_week[int(week)] = matrix
            facts[field] = by_week

        eligibility["_league"] = eligibility["db_name"].map(league_index)
        eligibility["_week"] = eligibility["week"].map(week_index)
        eligibility_matrices: dict[str, sparse.csr_matrix] = {}
        for group, rows in eligibility.groupby("pos_grp", sort=False):
            values = pd.to_numeric(rows["eligible"], errors="coerce").fillna(0.0)
            matrix = sparse.coo_matrix(
                (values.to_numpy(float), (rows["_league"], rows["_week"])),
                shape=(len(leagues), len(weeks)),
            ).tocsr()
            matrix.sum_duplicates()
            eligibility_matrices[str(group)] = matrix

        active["_player"] = active["NFL_player_id"].map(player_index)
        active["_week"] = active["week"].map(week_index)
        active_matrix = sparse.coo_matrix(
            (
                np.ones(len(active), dtype=float),
                (active["_player"], active["_week"]),
            ),
            shape=(len(players), len(weeks)),
        ).tocsr()
        active_matrix.sum_duplicates()
        active_matrix.data[:] = 1.0

        return cls(
            leagues=leagues,
            players=players,
            weeks=weeks,
            positions=positions,
            position_groups=position_groups,
            facts=facts,
            eligibility=eligibility_matrices,
            active=active_matrix,
            direct_values={},
        )

    def with_direct_values(self, values: pd.DataFrame) -> "MatchupSparseBoard":
        """Return a board carrying production-exact values aligned by player ID."""
        if "NFL_player_id" not in values:
            raise ValueError("direct values require NFL_player_id")
        frame = values.copy()
        frame["NFL_player_id"] = frame["NFL_player_id"].astype(str)
        if frame["NFL_player_id"].duplicated().any():
            raise ValueError("direct values contain duplicate player IDs")
        aligned: dict[str, np.ndarray] = {}
        for column in frame.columns:
            if column == "NFL_player_id":
                continue
            mapping = frame.set_index("NFL_player_id")[column]
            aligned[column] = pd.to_numeric(
                pd.Series(self.players).map(mapping), errors="coerce"
            ).to_numpy(float)
        if not aligned:
            raise ValueError("direct values require at least one metric column")
        return replace(self, direct_values=aligned)

    def subset_leagues(self, leagues: Iterable[str]) -> "MatchupSparseBoard":
        """Return the same board restricted to an approved annual league pool."""
        selected = tuple(str(value) for value in leagues)
        rows = self._sample_rows(selected)
        facts = {
            field: {
                week: matrix[rows].tocsr()
                for week, matrix in weekly.items()
            }
            for field, weekly in self.facts.items()
        }
        eligibility = {
            group: matrix[rows].tocsr()
            for group, matrix in self.eligibility.items()
        }
        return replace(
            self,
            leagues=selected,
            facts=facts,
            eligibility=eligibility,
        )

    def _sample_rows(self, sample: Iterable[str]) -> np.ndarray:
        names = tuple(str(value) for value in sample)
        if len(names) != len(set(names)):
            raise ValueError("sample contains duplicates")
        lookup = {value: index for index, value in enumerate(self.leagues)}
        unknown = sorted(set(names) - set(lookup))
        if unknown:
            raise ValueError(f"sample contains unknown leagues: {unknown[:5]}")
        if not names:
            raise ValueError("sample must contain at least one league")
        return np.asarray([lookup[value] for value in names], dtype=int)

    def _fact(self, field: str, week: int, sample_rows: np.ndarray) -> np.ndarray:
        matrix = self.facts.get(field, {}).get(week)
        if matrix is None:
            return np.zeros(len(self.players), dtype=float)
        return np.asarray(matrix[sample_rows].sum(axis=0)).ravel()

    def _eligible(
        self, week: int, sample_rows: np.ndarray, *, active_only: bool = False
    ) -> np.ndarray:
        try:
            week_column = self.weeks.index(week)
        except ValueError:
            return np.zeros(len(self.players), dtype=float)
        counts: dict[str, float] = {}
        for group in set(self.position_groups):
            matrix = self.eligibility.get(str(group))
            counts[str(group)] = (
                float(matrix[sample_rows, week_column].sum()) if matrix is not None else 0.0
            )
        result = np.asarray([counts.get(str(group), 0.0) for group in self.position_groups])
        if active_only:
            result *= np.asarray(self.active[:, week_column].toarray()).ravel()
        return result

    def metric_rows(
        self,
        sample: Iterable[str],
        *,
        grain: str,
        metric: str,
        position: str,
        week: int | None = None,
    ) -> pd.DataFrame:
        """Recompute one displayed metric and its ordering support for a sample."""
        if grain not in {"weekly", "season"}:
            raise ValueError(f"unsupported matchup grain: {grain}")
        if grain == "weekly" and week is None:
            raise ValueError("weekly boards require week")
        if position != "ALL" and position not in set(self.positions):
            return pd.DataFrame(columns=["NFL_player_id", "position", "metric", "support"])

        sample_rows = self._sample_rows(sample)
        selected_weeks = (int(week),) if grain == "weekly" else self.weeks
        n_players = len(self.players)
        rostered_total = np.zeros(n_players)
        started_total = np.zeros(n_players)
        eligible_total = np.zeros(n_players)
        points_total = np.zeros(n_players)
        clutch_total = np.zeros(n_players)
        champ_started_total = np.zeros(n_players)
        expected_wins = np.zeros(n_players)
        expected_starts = np.zeros(n_players)
        start_share_sum = np.zeros(n_players)
        active_week_count = np.zeros(n_players)
        zero_decision_share = np.zeros(n_players)
        wins_total = np.zeros(n_players)
        decisions_total = np.zeros(n_players)

        for selected_week in selected_weeks:
            rostered = self._fact("rostered_leagues", selected_week, sample_rows)
            started = self._fact("started_leagues", selected_week, sample_rows)
            wins = self._fact("wins_started", selected_week, sample_rows)
            losses = self._fact("losses_started", selected_week, sample_rows)
            points = self._fact("points_started", selected_week, sample_rows)
            clutch = self._fact("clutch_sum", selected_week, sample_rows)
            champ_started = self._fact("champ_started", selected_week, sample_rows)
            eligible = self._eligible(
                selected_week, sample_rows, active_only=(grain == "season")
            )
            start_share = _safe_divide(started, eligible)
            start_share = np.nan_to_num(start_share, nan=0.0)
            decisions = wins + losses
            decided_win_rate = _safe_divide(wins, decisions)

            rostered_total += rostered
            started_total += started
            eligible_total += eligible
            points_total += points
            clutch_total += clutch
            champ_started_total += champ_started
            expected_starts += start_share
            start_share_sum += start_share
            wins_total += wins
            decisions_total += decisions
            expected_wins += np.nan_to_num(start_share * decided_win_rate, nan=0.0)
            zero_decision_share += np.where(decisions == 0, start_share, 0.0)
            active_week_count += (eligible > 0).astype(float)

        aggregate_win_rate = _safe_divide(wins_total, decisions_total)
        expected_wins += np.nan_to_num(zero_decision_share * aggregate_win_rate, nan=0.0)

        if metric in self.direct_values:
            values = self.direct_values[metric].copy()
        elif metric == "start_rate":
            values = (
                _safe_divide(start_share_sum, active_week_count)
                if grain == "season"
                else start_share_sum
            )
        elif metric in {"win_rate", "expected_wins"}:
            values = expected_wins
            if metric == "win_rate" and grain == "season":
                values = _safe_divide(values, active_week_count)
        elif metric == "expected_starts":
            values = expected_starts
        elif metric in {"ppg"}:
            values = _safe_divide(points_total, started_total)
        elif metric == "points":
            values = points_total
        elif metric == "clutch":
            values = (
                np.sum(
                    [
                        np.nan_to_num(
                            _safe_divide(
                                self._fact("clutch_sum", selected_week, sample_rows),
                                self._eligible(
                                    selected_week,
                                    sample_rows,
                                    active_only=(grain == "season"),
                                ),
                            ),
                            nan=0.0,
                        )
                        for selected_week in selected_weeks
                    ],
                    axis=0,
                )
            )
        elif metric in {"started_leagues", "started_weeks"}:
            values = started_total
        elif metric == "eligible_leagues":
            values = eligible_total
        elif metric == "active_weeks":
            values = active_week_count
        elif metric == "inactive_weeks":
            values = len(selected_weeks) - active_week_count
        elif metric == "champ_started":
            values = _safe_divide(champ_started_total, eligible_total)
        else:
            raise ValueError(f"unsupported matchup metric: {metric}")

        direct_started_support = metric in self.direct_values and metric.endswith("ppg")
        support = (
            started_total
            if metric in _START_SUPPORT_METRICS or direct_started_support
            else eligible_total
        )
        candidates = rostered_total > 0
        if position != "ALL":
            candidates &= self.positions == position
        candidates &= np.isfinite(values)
        return pd.DataFrame(
            {
                "NFL_player_id": np.asarray(self.players)[candidates],
                "position": self.positions[candidates],
                "metric": values[candidates],
                "support": support[candidates],
            }
        )

    def ordered_top10(
        self,
        sample: Iterable[str],
        *,
        grain: str,
        metric: str,
        position: str,
        week: int | None = None,
    ) -> tuple[str, ...]:
        rows = self.metric_rows(
            sample, grain=grain, metric=metric, position=position, week=week
        )
        ordered = rows.sort_values(
            ["metric", "support", "NFL_player_id"],
            ascending=[False, False, True],
            kind="mergesort",
        )
        return tuple(ordered["NFL_player_id"].head(10))

    def batched_top10(
        self,
        samples: Sequence[Sequence[str]],
        *,
        grain: str,
        metric: str,
        position: str,
        week: int | None = None,
    ) -> tuple[tuple[str, ...], ...]:
        """Rebuild many displayed boards with batched sparse multiplication."""
        if grain not in {"weekly", "season"}:
            raise ValueError(f"unsupported matchup grain: {grain}")
        if grain == "weekly" and week is None:
            raise ValueError("weekly boards require week")
        if not samples:
            return ()

        sample_rows = [self._sample_rows(sample) for sample in samples]
        row_index = np.concatenate(
            [
                np.full(len(indices), row, dtype=int)
                for row, indices in enumerate(sample_rows)
            ]
        )
        column_index = np.concatenate(sample_rows)
        indicators = sparse.csr_matrix(
            (np.ones(len(column_index)), (row_index, column_index)),
            shape=(len(samples), len(self.leagues)),
        )
        shape = (len(samples), len(self.players))

        def fact(field: str, selected_week: int) -> np.ndarray:
            matrix = self.facts.get(field, {}).get(selected_week)
            if matrix is None:
                return np.zeros(shape)
            return (indicators @ matrix).toarray()

        def eligible(selected_week: int) -> np.ndarray:
            try:
                week_column = self.weeks.index(selected_week)
            except ValueError:
                return np.zeros(shape)
            values = np.zeros(shape)
            for group in set(self.position_groups):
                matrix = self.eligibility.get(str(group))
                if matrix is None:
                    continue
                counts = (indicators @ matrix[:, week_column]).toarray().ravel()
                mask = self.position_groups == group
                values[:, mask] = counts.reshape(-1, 1)
            if grain == "season":
                active = np.asarray(self.active[:, week_column].toarray()).ravel()
                values *= active.reshape(1, -1)
            return values

        selected_weeks = (int(week),) if grain == "weekly" else self.weeks
        supported = {
            "start_rate",
            "win_rate",
            "expected_wins",
            "expected_starts",
            "ppg",
            "points",
            "clutch",
            "started_leagues",
            "started_weeks",
            "eligible_leagues",
            "active_weeks",
            "inactive_weeks",
            "champ_started",
        } | set(self.direct_values)
        if metric not in supported:
            raise ValueError(f"unsupported matchup metric: {metric}")
        direct_metric = metric in self.direct_values
        direct_started_support = direct_metric and metric.endswith("ppg")
        need_eligible = metric != "ppg" and not direct_started_support
        need_started = metric in {
            "start_rate",
            "win_rate",
            "expected_wins",
            "expected_starts",
            "ppg",
            "clutch",
            "started_leagues",
            "started_weeks",
        } or direct_started_support
        need_outcomes = metric in {"win_rate", "expected_wins"}
        need_start_share = metric in {
            "start_rate",
            "win_rate",
            "expected_wins",
            "expected_starts",
        }
        need_active_count = grain == "season" and metric in {
            "start_rate",
            "win_rate",
            "active_weeks",
            "inactive_weeks",
        }
        rostered_total = np.zeros(shape)
        started_total = np.zeros(shape) if need_started else None
        eligible_total = np.zeros(shape) if need_eligible else None
        points_total = np.zeros(shape) if metric in {"ppg", "points"} else None
        clutch_values = np.zeros(shape) if metric == "clutch" else None
        champ_started_total = np.zeros(shape) if metric == "champ_started" else None
        expected_wins = np.zeros(shape) if need_outcomes else None
        expected_starts = np.zeros(shape) if metric == "expected_starts" else None
        start_share_sum = np.zeros(shape) if metric == "start_rate" else None
        active_week_count = np.zeros(shape) if need_active_count else None
        zero_decision_share = np.zeros(shape) if need_outcomes else None
        wins_total = np.zeros(shape) if need_outcomes else None
        decisions_total = np.zeros(shape) if need_outcomes else None

        for selected_week in selected_weeks:
            rostered = fact("rostered_leagues", selected_week)
            rostered_total += rostered
            eligible_values = eligible(selected_week) if need_eligible else None
            if eligible_total is not None and eligible_values is not None:
                eligible_total += eligible_values
            started = fact("started_leagues", selected_week) if need_started else None
            if started_total is not None and started is not None:
                started_total += started
            if points_total is not None:
                points_total += fact("points_started", selected_week)
            if clutch_values is not None:
                assert eligible_values is not None
                clutch_values += np.nan_to_num(
                    _safe_divide(
                        fact("clutch_sum", selected_week), eligible_values
                    ),
                    nan=0.0,
                )
            if champ_started_total is not None:
                champ_started_total += fact("champ_started", selected_week)
            if active_week_count is not None:
                assert eligible_values is not None
                active_week_count += (eligible_values > 0).astype(float)
            if need_start_share:
                assert started is not None and eligible_values is not None
                start_share = np.nan_to_num(
                    _safe_divide(started, eligible_values), nan=0.0
                )
                if expected_starts is not None:
                    expected_starts += start_share
                if start_share_sum is not None:
                    start_share_sum += start_share
                if need_outcomes:
                    assert expected_wins is not None
                    assert zero_decision_share is not None
                    assert wins_total is not None and decisions_total is not None
                    wins = fact("wins_started", selected_week)
                    losses = fact("losses_started", selected_week)
                    decisions = wins + losses
                    decided_win_rate = _safe_divide(wins, decisions)
                    wins_total += wins
                    decisions_total += decisions
                    expected_wins += np.nan_to_num(
                        start_share * decided_win_rate, nan=0.0
                    )
                    zero_decision_share += np.where(
                        decisions == 0, start_share, 0.0
                    )

        if need_outcomes:
            assert expected_wins is not None
            assert zero_decision_share is not None
            assert wins_total is not None and decisions_total is not None
            aggregate_win_rate = _safe_divide(wins_total, decisions_total)
            expected_wins += np.nan_to_num(
                zero_decision_share * aggregate_win_rate, nan=0.0
            )
        if direct_metric:
            values = np.broadcast_to(self.direct_values[metric], shape).copy()
        elif metric == "start_rate":
            assert start_share_sum is not None
            values = (
                _safe_divide(start_share_sum, active_week_count)
                if grain == "season"
                else start_share_sum
            )
        elif metric in {"win_rate", "expected_wins"}:
            assert expected_wins is not None
            values = expected_wins
            if metric == "win_rate" and grain == "season":
                assert active_week_count is not None
                values = _safe_divide(values, active_week_count)
        elif metric == "expected_starts":
            assert expected_starts is not None
            values = expected_starts
        elif metric == "ppg":
            assert points_total is not None and started_total is not None
            values = _safe_divide(points_total, started_total)
        elif metric == "points":
            assert points_total is not None
            values = points_total
        elif metric == "clutch":
            assert clutch_values is not None
            values = clutch_values
        elif metric in {"started_leagues", "started_weeks"}:
            assert started_total is not None
            values = started_total
        elif metric == "eligible_leagues":
            assert eligible_total is not None
            values = eligible_total
        elif metric == "active_weeks":
            assert active_week_count is not None
            values = active_week_count
        elif metric == "inactive_weeks":
            assert active_week_count is not None
            values = len(selected_weeks) - active_week_count
        elif metric == "champ_started":
            assert champ_started_total is not None and eligible_total is not None
            values = _safe_divide(champ_started_total, eligible_total)

        support = (
            started_total
            if metric in _START_SUPPORT_METRICS or direct_started_support
            else eligible_total
        )
        assert support is not None
        candidates = (rostered_total > 0) & np.isfinite(values)
        if position != "ALL":
            candidates &= self.positions.reshape(1, -1) == position

        player_ids = np.asarray(self.players)
        boards: list[tuple[str, ...]] = []
        for row in range(len(samples)):
            indexes = np.flatnonzero(candidates[row])
            if not len(indexes):
                boards.append(())
                continue
            order = np.lexsort(
                (
                    player_ids[indexes],
                    -support[row, indexes],
                    -values[row, indexes],
                )
            )
            boards.append(tuple(player_ids[indexes[order[:10]]]))
        return tuple(boards)
