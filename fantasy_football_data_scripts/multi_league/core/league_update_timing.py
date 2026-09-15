"""Small deterministic timing receipt for the existing weekly refresh stages."""

from __future__ import annotations

from time import perf_counter
from typing import Callable


class PhaseTimer:
    def __init__(self, *, clock: Callable[[], float] = perf_counter) -> None:
        self._clock = clock
        self._start = clock()
        self._last = self._start
        self._phases: dict[str, float] = {}

    def mark(self, phase: str) -> None:
        if phase in self._phases or phase in {"total", "unmarked"}:
            raise ValueError(f"Duplicate or reserved timing phase: {phase}")
        now = self._clock()
        if now < self._last:
            raise ValueError("Timing clock moved backward")
        self._phases[phase] = round(now - self._last, 3)
        self._last = now

    def finish(self) -> dict[str, float]:
        now = self._clock()
        if now < self._last:
            raise ValueError("Timing clock moved backward")
        return {
            **self._phases,
            "unmarked": round(now - self._last, 3),
            "total": round(now - self._start, 3),
        }
