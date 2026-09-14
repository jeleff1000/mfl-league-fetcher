"""Helpers for platform-native fantasy player identity selection."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


PLATFORM_PLAYER_ID_PRIORITY: dict[str, tuple[str, ...]] = {
    "yahoo": (
        "yahoo_player_id",
        "player_id",
    ),
    "sleeper": (
        "sleeper_player_id_original",
        "player_id",
        "sleeper_player_id",
        "yahoo_player_id",
    ),
    "espn": (
        "espn_player_id",
        "espn_player_id_original",
        "player_id",
        "yahoo_player_id",
    ),
    "fleaflicker": (
        "fleaflicker_player_id",
        "player_id",
    ),
    "mfl": (
        "mfl_player_id",
        "player_id",
    ),
}

DEFAULT_PLAYER_ID_PRIORITY: tuple[str, ...] = (
    "yahoo_player_id",
    "sleeper_player_id_original",
    "espn_player_id",
    "espn_player_id_original",
    "fleaflicker_player_id",
    "mfl_player_id",
    "player_id",
    "sleeper_player_id",
)

CANONICAL_PLATFORM_PLAYER_ID = "_platform_player_id"
ALL_PLATFORM_PLAYER_ID_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(column for columns in PLATFORM_PLAYER_ID_PRIORITY.values() for column in columns)
)

_INVALID_ID_TOKENS = {"", "nan", "none", "<na>", "null"}


def _normalize_platform_name(platform: str | None) -> str | None:
    if platform is None:
        return None

    normalized = str(platform).strip().lower()
    return normalized or None


def platform_player_id_candidates(platform: str | None = None) -> tuple[str, ...]:
    normalized = _normalize_platform_name(platform)
    if normalized in PLATFORM_PLAYER_ID_PRIORITY:
        return PLATFORM_PLAYER_ID_PRIORITY[normalized]
    return DEFAULT_PLAYER_ID_PRIORITY


def detect_platform_from_frames(*frames: pd.DataFrame, platform_hint: str | None = None) -> str:
    """Detect the fantasy platform from dataframe values or columns."""

    normalized_hint = _normalize_platform_name(platform_hint)
    if normalized_hint in PLATFORM_PLAYER_ID_PRIORITY:
        return normalized_hint

    for frame in frames:
        if frame is None or frame.empty:
            continue

        if "platform" in frame.columns:
            platform_values = frame["platform"].dropna().astype(str).str.strip().str.lower()
            for candidate in ("sleeper", "espn", "fleaflicker", "mfl", "yahoo"):
                if (platform_values == candidate).any():
                    return candidate

        columns = set(frame.columns)
        if "fleaflicker_player_id" in columns:
            return "fleaflicker"
        if "mfl_player_id" in columns:
            return "mfl"
        if {"sleeper_player_id_original", "sleeper_player_id"} & columns:
            return "sleeper"
        if {"espn_player_id", "espn_player_id_original"} & columns:
            return "espn"
        if "yahoo_player_id" in columns:
            return "yahoo"

    return normalized_hint or "yahoo"


def select_platform_player_id_column(columns: Iterable[str], platform_hint: str | None = None) -> str | None:
    """Pick the best platform-native fantasy player ID column from a column list."""

    available_by_lower = {str(column).lower(): str(column) for column in columns}
    for column in platform_player_id_candidates(platform_hint):
        normalized_column = column.lower()
        if normalized_column in available_by_lower:
            return available_by_lower[normalized_column]
    return None


def pick_platform_player_id_column(
    df: pd.DataFrame,
    platform_hint: str | None = None,
    require_non_null: bool = True,
) -> str | None:
    """Pick the best platform-native fantasy player ID column from a dataframe."""

    platform = detect_platform_from_frames(df, platform_hint=platform_hint)
    for column in platform_player_id_candidates(platform):
        if column not in df.columns:
            continue
        if require_non_null and not df[column].notna().any():
            continue
        return column
    return None


def canonicalize_platform_player_id_series(series: pd.Series) -> pd.Series:
    """Normalize platform player IDs to a stable string form for joins."""

    normalized = series.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    invalid_mask = normalized.str.lower().isin(_INVALID_ID_TOKENS)
    return normalized.mask(invalid_mask, pd.NA)


def canonicalize_platform_player_id(value: object) -> str | None:
    """Normalize a single platform player ID to a stable string form."""

    if pd.isna(value):
        return None

    normalized = str(value).strip()
    if not normalized:
        return None

    normalized = normalized.removesuffix(".0")
    if normalized.lower() in _INVALID_ID_TOKENS:
        return None
    return normalized


def canonicalize_platform_player_ids(values: Iterable[object]) -> list[str]:
    """Normalize a collection of platform player IDs to stable strings."""

    normalized_values: list[str] = []
    for value in values:
        normalized = canonicalize_platform_player_id(value)
        if normalized is not None:
            normalized_values.append(normalized)
    return normalized_values


def prepare_platform_player_id_filter_values(
    values: Iterable[object],
    target_type: str = "string",
) -> list[object]:
    """Normalize player IDs for parquet filtering while preserving column-compatible types."""

    normalized_values = canonicalize_platform_player_ids(values)
    if target_type.lower().startswith("int"):
        typed_values: list[object] = []
        for value in normalized_values:
            try:
                typed_values.append(int(value))
            except ValueError:
                continue
        return typed_values
    return normalized_values


def assign_platform_player_id(
    df: pd.DataFrame,
    platform_hint: str | None = None,
    target_col: str = CANONICAL_PLATFORM_PLAYER_ID,
    require_non_null: bool = True,
) -> tuple[str, str]:
    """Populate a canonical in-memory join key from the platform-native ID column."""

    platform = detect_platform_from_frames(df, platform_hint=platform_hint)
    source_col = pick_platform_player_id_column(df, platform_hint=platform, require_non_null=require_non_null)
    if source_col is None:
        candidates = ", ".join(platform_player_id_candidates(platform))
        raise KeyError(f"Could not find a platform player ID column for platform='{platform}'. Tried: {candidates}")

    df[target_col] = canonicalize_platform_player_id_series(df[source_col])
    return source_col, platform
