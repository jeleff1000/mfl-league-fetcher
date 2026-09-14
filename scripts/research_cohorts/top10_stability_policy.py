"""Fail-closed forward policy derived from annual top-10 rank evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Literal, Sequence


PolicyState = Literal[
    "stable", "left_censored", "right_censored", "structurally_unstable", "not_applicable"
]


@dataclass(frozen=True)
class PolicyManifest:
    source_hash: str
    cluster_map_hash: str
    contract_hash: str
    git_commit: str
    seed: int
    repetitions: int
    generated_at: str


@dataclass(frozen=True)
class AnnualPolicyResult:
    dataset: str
    grain: str
    metric: str
    position: str
    base_cohort: str
    members: tuple[str, ...]
    year: int
    population: int
    state: PolicyState
    min_n: int | None
    fully_observable: bool

    @property
    def key(self) -> tuple[str, str, str, str, str]:
        return self.dataset, self.grain, self.metric, self.position, self.base_cohort


@dataclass(frozen=True)
class PolicyEntry:
    dataset: str
    grain: str
    metric: str
    position: str
    base_cohort: str
    members: tuple[str, ...]
    state: PolicyState
    forward_threshold: int | None
    evidence_years: tuple[int, ...]
    annual_evidence: tuple[AnnualPolicyResult, ...]

    @property
    def key(self) -> tuple[str, str, str, str, str]:
        return self.dataset, self.grain, self.metric, self.position, self.base_cohort


@dataclass(frozen=True)
class StabilityPolicy:
    version: int
    manifest: PolicyManifest
    entries: tuple[PolicyEntry, ...]


def build_forward_policy(
    annual_results: Sequence[AnnualPolicyResult], manifest: PolicyManifest
) -> StabilityPolicy:
    grouped: dict[tuple[str, str, str, str, str], list[AnnualPolicyResult]] = {}
    for result in annual_results:
        if not result.members or len(result.members) != len(set(result.members)):
            raise ValueError("approved pool members must be nonempty and unique")
        grouped.setdefault(result.key, []).append(result)

    entries: list[PolicyEntry] = []
    for key, raw_rows in sorted(grouped.items()):
        rows = tuple(sorted(raw_rows, key=lambda row: row.year))
        if len({row.year for row in rows}) != len(rows):
            raise ValueError(f"duplicate annual policy rows for {key}")
        member_sets = {row.members for row in rows}
        if len(member_sets) != 1:
            raise ValueError(f"approved pool changed across years for {key}")

        observed = [
            row.min_n for row in rows
            if row.fully_observable and row.state in {"stable", "left_censored"}
            and row.min_n is not None
        ]
        threshold = max(observed) if observed else None
        latest = rows[-1]
        blocking = latest.state in {"right_censored", "structurally_unstable"}
        if threshold is not None and latest.state == "right_censored":
            blocking = latest.population < 2 * threshold
        if blocking:
            threshold = None

        if threshold is not None:
            state: PolicyState = "stable"
        elif latest.state in {"right_censored", "structurally_unstable", "not_applicable"}:
            state = latest.state
        else:
            state = "right_censored"
        entries.append(
            PolicyEntry(
                dataset=key[0], grain=key[1], metric=key[2], position=key[3],
                base_cohort=key[4], members=rows[0].members, state=state,
                forward_threshold=threshold,
                evidence_years=tuple(row.year for row in rows if row.fully_observable),
                annual_evidence=rows,
            )
        )
    return StabilityPolicy(version=1, manifest=manifest, entries=tuple(entries))


def policy_threshold(policy: StabilityPolicy, *, dataset: str, grain: str, metric: str,
                     position: str, base_cohort: str) -> int | None:
    key = dataset, grain, metric, position, base_cohort
    matches = [entry for entry in policy.entries if entry.key == key]
    if len(matches) != 1 or matches[0].state != "stable":
        return None
    return matches[0].forward_threshold


def write_stability_policy(policy: StabilityPolicy, path: Path | str) -> None:
    payload = asdict(policy)
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_stability_policy(path: Path | str) -> StabilityPolicy:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    manifest = PolicyManifest(**payload["manifest"])
    entries = []
    for raw in payload["entries"]:
        annual = tuple(
            AnnualPolicyResult(**{**row, "members": tuple(row["members"])})
            for row in raw["annual_evidence"]
        )
        entries.append(
            PolicyEntry(
                **{
                    **raw,
                    "members": tuple(raw["members"]),
                    "evidence_years": tuple(raw["evidence_years"]),
                    "annual_evidence": annual,
                }
            )
        )
    return StabilityPolicy(version=int(payload["version"]), manifest=manifest, entries=tuple(entries))
