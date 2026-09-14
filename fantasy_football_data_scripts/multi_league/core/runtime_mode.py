"""Runtime-mode guards shared by database clients and corpus imports."""

from __future__ import annotations

import os


_TRUE_VALUES = {"1", "true", "yes", "on"}


def is_corpus_mode() -> bool:
    """Return whether this process must remain offline from production storage."""

    return os.environ.get("CORPUS_MODE", "").strip().lower() in _TRUE_VALUES


def assert_fly_access_allowed(client_name: str) -> None:
    """Fail closed when production Fly access is attempted by a corpus process."""

    if is_corpus_mode():
        raise RuntimeError(f"{client_name} is disabled in corpus mode")
