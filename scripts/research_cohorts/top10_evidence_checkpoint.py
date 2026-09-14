"""Durable per-candidate checkpoints for long annual rank studies."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Callable, Mapping

import pandas as pd

from top10_stability_runner import CandidateResult


_RESULT_FIELDS = tuple(CandidateResult.__dataclass_fields__)


class CandidateCheckpoint:
    """Cache completed candidates and fail closed if board identity changes."""

    def __init__(self, path: Path | str, identity: Mapping[str, object]) -> None:
        self.path = Path(path)
        self.identity = {
            str(key): self._normalize(value)
            for key, value in sorted(identity.items())
        }
        self._results: dict[int, CandidateResult] = {}
        if self.path.is_file():
            self._load()

    @staticmethod
    def _normalize(value: object) -> object:
        if isinstance(value, (tuple, list)):
            return [CandidateCheckpoint._normalize(item) for item in value]
        if isinstance(value, Path):
            return str(value)
        return value

    def _identity_json(self) -> str:
        return json.dumps(self.identity, sort_keys=True, separators=(",", ":"))

    def _load(self) -> None:
        frame = pd.read_csv(self.path)
        required = {"identity_json", *_RESULT_FIELDS}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"candidate checkpoint missing columns: {', '.join(missing)}")
        identities = set(frame["identity_json"].astype(str))
        if identities != {self._identity_json()}:
            raise ValueError("candidate checkpoint identity does not match requested board")
        if frame["n"].duplicated().any():
            raise ValueError("candidate checkpoint contains duplicate sample sizes")
        for raw in frame.to_dict("records"):
            payload = {field: raw[field] for field in _RESULT_FIELDS}
            payload.update(
                n=int(payload["n"]),
                total_trials=int(payload["total_trials"]),
                valid_trials=int(payload["valid_trials"]),
                invalid_trials=int(payload["invalid_trials"]),
                median_rho=float(payload["median_rho"]),
                p10_rho=float(payload["p10_rho"]),
                pass_share=float(payload["pass_share"]),
                pass_share_lower_95=float(payload["pass_share_lower_95"]),
                passes=str(payload["passes"]).strip().lower() in {"true", "1"},
            )
            result = CandidateResult(**payload)
            self._results[result.n] = result

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {"identity_json": self._identity_json(), **asdict(result)}
            for result in self.results()
        ]
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        pd.DataFrame(rows).to_csv(temp, index=False)
        temp.replace(self.path)

    def measure(
        self,
        n: int,
        evaluator: Callable[[int], CandidateResult],
    ) -> CandidateResult:
        selected = int(n)
        if selected in self._results:
            return self._results[selected]
        result = evaluator(selected)
        if int(result.n) != selected:
            raise ValueError(
                f"candidate evaluator returned n={result.n} for requested n={selected}"
            )
        self._results[selected] = result
        self._write()
        return result

    def results(self) -> tuple[CandidateResult, ...]:
        return tuple(self._results[n] for n in sorted(self._results))
