"""Column ownership and preservation gates for active-season refreshes."""

from __future__ import annotations

from dataclasses import dataclass
import json
from numbers import Real
from time import perf_counter
from typing import Any

import pandas as pd


class OwnershipContractError(RuntimeError):
    """A publishable table or column has no declared refresh owner."""


class PreservationError(RuntimeError):
    """A refresh would erase history or user-owned configuration."""


@dataclass(frozen=True, slots=True)
class TableOwnership:
    table_name: str
    key_columns: tuple[str, ...]
    provider_owned_columns: frozenset[str]
    derived_columns: frozenset[str]
    user_owned_columns: frozenset[str]

    @property
    def classified_columns(self) -> frozenset[str]:
        return self.provider_owned_columns | self.derived_columns | self.user_owned_columns


_USER_TABLES = {
    "keeper_config",
    "league_context",
    "league_rules",
    "manager_overrides",
    "standings_config",
}

PRESERVATION_WITNESS_TABLES = (
    "draft",
    "draft_manager_career",
    "draft_player_career",
    "franchise_identity_audit",
    "franchise_identity_registry",
    "homepage_current_standings",
    "homepage_league_summary",
    "homepage_manager_profiles",
    "homepage_manager_rankings",
    "homepage_top_rivalries",
    "keeper_config",
    "league_context",
    "league_rules",
    "league_settings",
    "manager_overrides",
    "matchup",
    "matchup_career",
    "matchup_h2h_career",
    "player_fantasy",
    "player_fantasy_career",
    "player_fantasy_career_all",
    "schedule",
    "standings_config",
    "transactions",
    "transaction_manager_career",
    "transaction_player_career",
)

_KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    "player_fantasy": ("db_name", "player_week"),
    "matchup": ("db_name", "manager_week"),
    "draft": ("db_name", "year", "draft_id", "round", "pick"),
    "transactions": ("db_name", "transaction_id", "transaction_sequence"),
    "schedule": ("db_name", "manager_week"),
    "league_settings": ("db_name", "year"),
}

# Older imports can contain a manual historical finish row with no provider
# ``manager_week`` value.  That row still has a stable matchup identity and
# must remain protected by an active-season refresh.
_MATCHUP_PRESERVATION_FALLBACK_KEYS = (
    "db_name", "year", "week", "manager", "team_name", "opponent",
)

_SOURCE_SCHEMAS: dict[str, tuple[str, str]] = {
    "player_fantasy": ("multi_league.core.canonical_player", "PLAYER_FANTASY_SCHEMA"),
    "matchup": ("multi_league.core.canonical_matchup", "MATCHUP_SCHEMA"),
    "draft": ("multi_league.core.canonical_draft", "DRAFT_SCHEMA"),
    "transactions": ("multi_league.core.canonical_transaction", "TRANSACTION_SCHEMA"),
}

_SOURCE_FACT_TABLES = frozenset({
    "draft",
    "league_settings",
    "matchup",
    "player_fantasy",
    "schedule",
    "transactions",
})

_CAREER_IDENTITIES: dict[str, tuple[str, ...]] = {
    "draft_manager_career": ("franchise_id", "manager", "draft_category"),
    "draft_player_career": ("player", "position", "draft_category"),
    "matchup_career": ("franchise_id", "manager"),
    "matchup_h2h_career": ("franchise_id", "opponent_franchise_id", "manager", "opponent"),
    "player_fantasy_career": ("NFL_player_id",),
    "player_fantasy_career_all": ("NFL_player_id",),
    "transaction_manager_career": ("franchise_id", "manager"),
    "transaction_player_career": ("player", "position"),
}

_HOMEPAGE_IDENTITIES: dict[str, tuple[str, ...]] = {
    # Current standings, manager profiles, and top rivalries are deliberately
    # rebuilt from the active roster and current scores.  They can legitimately
    # add/drop rows when a manager leaves or the top-N set changes; derived
    # output-health validates the rebuilt result instead of freezing it.
    "homepage_league_summary": ("db_name",),
    "homepage_manager_rankings": ("franchise_id", "manager"),
}

_MONOTONIC_COLUMNS: dict[str, tuple[str, ...]] = {
    "draft_manager_career": ("years_active", "total_picks"),
    "draft_player_career": ("times_drafted",),
    "matchup_career": ("seasons", "games"),
    "matchup_h2h_career": ("games",),
    "player_fantasy_career": ("years_active", "games_rostered"),
    "player_fantasy_career_all": ("years_active", "games_rostered"),
    "transaction_manager_career": ("seasons", "total_moves"),
    "transaction_player_career": ("years_active",),
}


def _source_schema_owners(table_name: str) -> tuple[frozenset[str], frozenset[str]]:
    """Read raw/derived ownership from the canonical DDL declaration."""
    import importlib

    module_name, attribute = _SOURCE_SCHEMAS[table_name]
    schema = getattr(importlib.import_module(module_name), attribute)
    allowed = {"api", "enrichment", "join_key", "sim", "sql", "system"}
    unknown_tags = sorted({str(owner) for _, _, owner in schema} - allowed)
    if unknown_tags:
        raise OwnershipContractError(
            f"{table_name} canonical schema has unknown ownership tags: {unknown_tags}"
        )
    provider = frozenset(
        str(column)
        for column, _, owner in schema
        if owner in {"api", "join_key", "system"}
    )
    derived = frozenset(
        str(column)
        for column, _, owner in schema
        if owner in {"enrichment", "sim", "sql"}
    )
    return provider, derived


def table_ownership(table_name: str) -> TableOwnership:
    """Return a complete ownership contract for one canonical publish table."""
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()
    if table_name not in registry:
        raise OwnershipContractError(f"unregistered publish table: {table_name}")
    columns = frozenset(str(column) for column in registry[table_name]["columns"])
    keys = _KEY_COLUMNS.get(table_name, ("db_name",))
    missing_keys = sorted(set(keys) - columns)
    if missing_keys:
        raise OwnershipContractError(
            f"{table_name} ownership key is missing canonical columns: {missing_keys}"
        )

    if table_name in _USER_TABLES:
        provider = frozenset()
        user = columns
    elif table_name in {"league_settings", "schedule"}:
        provider = columns
        derived = frozenset()
        user = frozenset()
    elif table_name in _SOURCE_SCHEMAS:
        provider, derived = _source_schema_owners(table_name)
        provider &= columns
        derived &= columns
        if table_name == "player_fantasy":
            # Provider roster rows do not carry the canonical NFL join key.
            # It is mapped/rebuilt by enrichment and must survive an omitted
            # field on a narrow refresh, not be treated as provider-owned.
            provider -= {"player_week", "NFL_player_id"}
            derived |= {"player_week", "NFL_player_id"}
        user = frozenset()
    else:
        provider = frozenset()
        derived = columns
        user = frozenset()
    if table_name in _USER_TABLES:
        derived = frozenset()
    contract = TableOwnership(table_name, keys, provider, derived, user)
    if contract.classified_columns != columns:
        raise OwnershipContractError(f"{table_name} has unclassified publishable columns")
    return contract


def assert_publish_table_ownership(table_names: list[str]) -> dict[str, int]:
    """Fail bundle construction unless every table has a complete contract."""
    contracts = [table_ownership(table_name) for table_name in sorted(set(table_names))]
    return {
        "tables": len(contracts),
        "columns": sum(len(contract.classified_columns) for contract in contracts),
    }


def overlay_provider_columns(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    contract: TableOwnership,
) -> pd.DataFrame:
    """Apply provider fields while carrying protected values only by exact key."""
    missing_keys = sorted(set(contract.key_columns) - set(incoming.columns))
    if missing_keys:
        raise OwnershipContractError(
            f"{contract.table_name} provider payload is missing ownership keys: {missing_keys}"
        )
    unknown = sorted(set(incoming.columns) - contract.classified_columns)
    if unknown:
        raise OwnershipContractError(
            f"{contract.table_name} provider payload has unclassified columns: {unknown}"
        )
    if incoming.duplicated(list(contract.key_columns), keep=False).any():
        raise OwnershipContractError(f"{contract.table_name} provider payload has duplicate ownership keys")

    protected = sorted(
        (contract.derived_columns | contract.user_owned_columns) - set(contract.key_columns)
    )
    available = [column for column in protected if column in existing.columns]
    old = existing.loc[:, [*contract.key_columns, *available]].copy()
    # Historical provider exports can retain a few bye/placeholder rows with
    # no ownership identity at all. They cannot match an incoming canonical
    # provider row, so they must not make the preservation join ambiguous.
    # Nonblank keys remain strictly one-to-one and still fail closed below.
    complete_key = pd.Series(True, index=old.index)
    for column in contract.key_columns:
        values = old[column]
        complete_key &= values.notna() & ~values.astype(str).str.strip().str.lower().isin(
            {"", "none", "nan", "<na>"}
        )
    old = old.loc[complete_key].copy()
    if old.duplicated(list(contract.key_columns), keep=False).any():
        raise OwnershipContractError(f"{contract.table_name} existing rows have duplicate ownership keys")
    old = old.rename(columns={column: f"__preserved_{column}" for column in available})
    result = incoming.copy().merge(
        old,
        on=list(contract.key_columns),
        how="left",
        validate="one_to_one",
    )
    protected_values = pd.DataFrame(
        {
            column: (
                result[f"__preserved_{column}"]
                if f"__preserved_{column}" in result.columns
                else pd.Series(pd.NA, index=result.index, dtype="object")
            )
            for column in protected
        },
        index=result.index,
    )
    provider_result = result.drop(
        columns=[
            column
            for column in result.columns
            if column in protected or column.startswith("__preserved_")
        ]
    )
    return pd.concat([provider_result, protected_values], axis=1)


def _frame_fingerprint(frame: pd.DataFrame) -> str:
    def canonical_value(value: Any) -> str:
        if value is None or value is pd.NA:
            return "<NULL>"
        try:
            if bool(pd.isna(value)):
                return "<NULL>"
        except (TypeError, ValueError):
            pass
        # Fly's JSON frames may decode a nullable integral field as float
        # while DuckDB returns it as Int32.  That is a transport dtype change,
        # not a historical data mutation.
        if isinstance(value, Real) and not isinstance(value, bool):
            numeric = float(value)
            if numeric.is_integer():
                return str(int(numeric))
            return format(numeric, ".17g")
        return str(value)

    normalized = frame.map(canonical_value).sort_index(axis=1)
    records = normalized.to_dict("records")
    records.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")))
    return json.dumps(records, sort_keys=True, separators=(",", ":"))


def _identity_columns(
    old: pd.DataFrame,
    new: pd.DataFrame,
    candidates: tuple[str, ...],
) -> list[str]:
    """Choose the first stable identity, plus any declared disambiguators."""
    available = [column for column in candidates if column in old.columns and column in new.columns]
    if not available:
        return []
    first = available[0]
    if first.startswith("franchise_id") and old[first].notna().any():
        # Head-to-head and rivalry rollups are identified by a *pair* of
        # franchises.  Keeping only the first ID conflates every opponent of
        # the same manager and makes a legitimate career table look corrupt.
        return [column for column in available if "franchise_id" in column]
    return available


def _is_null_preservation_value(value: Any) -> bool:
    """Treat decoded JSON arrays/objects as present values, not boolean arrays."""
    if value is None or value is pd.NA:
        return True
    if isinstance(value, (dict, list, tuple, set)):
        return False
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    # ``pd.isna`` returns an array for structured JSON values.  The container
    # itself is a preserved non-null value; only a scalar missing marker means
    # this field was erased.
    if hasattr(missing, "__len__"):
        return False
    return bool(missing)


def _assert_rows_and_values_preserved(
    table_name: str,
    old: pd.DataFrame,
    new: pd.DataFrame,
    *,
    identity_candidates: tuple[str, ...],
    monotonic_columns: tuple[str, ...] = (),
) -> None:
    if old.empty:
        return
    if new.empty:
        raise PreservationError(f"preserved output disappeared in {table_name}")
    identity = _identity_columns(old, new, identity_candidates)
    if not identity:
        raise PreservationError(f"preservation identity is unavailable in {table_name}")
    old_index = old.set_index(identity, drop=False)
    new_index = new.set_index(identity, drop=False)
    if old_index.index.has_duplicates or new_index.index.has_duplicates:
        raise PreservationError(f"duplicate preservation identity in {table_name}: {identity}")
    for key, old_row in old_index.iterrows():
        if key not in new_index.index:
            raise PreservationError(f"preserved output disappeared in {table_name}: {key}")
        new_row = new_index.loc[key]
        for column in monotonic_columns:
            if column not in old.columns or column not in new.columns:
                continue
            old_value = pd.to_numeric(pd.Series([old_row[column]]), errors="coerce").iloc[0]
            new_value = pd.to_numeric(pd.Series([new_row[column]]), errors="coerce").iloc[0]
            if pd.notna(old_value) and (pd.isna(new_value) or new_value < old_value):
                raise PreservationError(
                    f"career history shrank in {table_name}.{column} for {key}"
                )
        for column, old_value in old_row.items():
            if column == "last_updated" or column not in new_row.index:
                continue
            if not _is_null_preservation_value(old_value) and _is_null_preservation_value(new_row[column]):
                raise PreservationError(f"preserved value became null in {table_name}.{column} for {key}")


def _historical_source_witness(frame: pd.DataFrame, table_name: str, active_year: int) -> pd.DataFrame:
    if frame.empty or "year" not in frame.columns:
        return frame.iloc[0:0].copy()
    years = pd.to_numeric(frame["year"], errors="coerce")
    historical = frame.loc[years < int(active_year)].copy()
    return historical.sort_index(axis=1)


def _preservation_identity_columns(
    table_name: str,
    old: pd.DataFrame,
    new: pd.DataFrame,
    default_keys: tuple[str, ...],
) -> list[str]:
    """Choose a complete, unique source identity for the preservation check."""
    keys = list(default_keys)
    old_has_duplicates = old.set_index(keys, drop=False).index.has_duplicates
    new_has_duplicates = new.set_index(keys, drop=False).index.has_duplicates
    if not old_has_duplicates and not new_has_duplicates:
        return keys
    if table_name != "matchup":
        return keys
    fallback = list(_MATCHUP_PRESERVATION_FALLBACK_KEYS)
    if any(column not in old.columns or column not in new.columns for column in fallback):
        return keys
    if (
        not old.set_index(fallback, drop=False).index.has_duplicates
        and not new.set_index(fallback, drop=False).index.has_duplicates
    ):
        return fallback
    return keys


def _assert_derived_values_not_erased(
    table_name: str,
    old: pd.DataFrame,
    new: pd.DataFrame,
) -> int:
    """Prevent an active provider refresh from turning valid enrichment into NULL."""
    if old.empty:
        return 0
    contract = table_ownership(table_name)
    default_keys = tuple(contract.key_columns)
    if any(column not in old.columns or column not in new.columns for column in default_keys):
        raise PreservationError(f"source identity is unavailable in {table_name}: {list(default_keys)}")
    keys = _preservation_identity_columns(table_name, old, new, default_keys)
    if any(column not in old.columns or column not in new.columns for column in keys):
        raise PreservationError(f"source identity is unavailable in {table_name}: {keys}")
    old_index = old.set_index(keys, drop=False)
    new_index = new.set_index(keys, drop=False)
    if old_index.index.has_duplicates or new_index.index.has_duplicates:
        raise PreservationError(f"duplicate source identity in {table_name}: {keys}")
    derived = sorted(contract.derived_columns & set(old.columns) & set(new.columns))
    optimal_deselections = 0
    if (
        table_name == "player_fantasy"
        and {"db_name", "year", "week", "league_wide_optimal_player", "league_wide_optimal_position"}
        <= set(old.columns) & set(new.columns)
    ):
        new_flags = pd.to_numeric(new["league_wide_optimal_player"], errors="coerce").eq(1)
        new_labels = new["league_wide_optimal_position"].fillna("").astype(str).str.strip().ne("")
        if (new_flags & ~new_labels).any():
            raise PreservationError("selected league-wide optimal player has no recomputed slot label")
        old_selected = old.loc[
            pd.to_numeric(old["league_wide_optimal_player"], errors="coerce").eq(1),
            ["db_name", "year", "week"],
        ].drop_duplicates()
        new_selected = set(
            map(tuple, new.loc[new_flags, ["db_name", "year", "week"]].itertuples(index=False, name=None))
        )
        if any(tuple(scope) not in new_selected for scope in old_selected.itertuples(index=False, name=None)):
            raise PreservationError("league-wide optimal recomputation removed every selected player in a week")
    for key, old_row in old_index.iterrows():
        if key not in new_index.index:
            # Active provider corrections may legitimately remove or re-key a
            # source row. Completeness validation owns row-loss decisions;
            # this gate protects enrichment only when the canonical key remains.
            continue
        new_row = new_index.loc[key]
        for column in derived:
            if pd.notna(old_row[column]) and pd.isna(new_row[column]):
                if table_name == "player_fantasy" and column == "league_wide_optimal_position":
                    old_flag = pd.to_numeric(
                        pd.Series([old_row.get("league_wide_optimal_player")]), errors="coerce"
                    ).iloc[0]
                    new_flag = pd.to_numeric(
                        pd.Series([new_row.get("league_wide_optimal_player")]), errors="coerce"
                    ).iloc[0]
                    if old_flag == 1 and new_flag == 0:
                        optimal_deselections += 1
                        continue
                raise PreservationError(
                    f"derived value became null in {table_name}.{column} for {key}"
                )
    return optimal_deselections


def assert_refresh_preservation(
    before: dict[str, pd.DataFrame],
    after: dict[str, pd.DataFrame],
    *,
    active_year: int,
) -> dict[str, Any]:
    """Fail closed when an active refresh erases history or user configuration."""
    started_at = perf_counter()
    timing: dict[str, float] = {}
    for table_name in sorted(_USER_TABLES & before.keys()):
        if table_name not in after or _frame_fingerprint(before[table_name]) != _frame_fingerprint(after[table_name]):
            raise PreservationError(f"user configuration changed in {table_name}")
    user_configuration_at = perf_counter()
    timing["user_configuration"] = round(user_configuration_at - started_at, 3)

    _assert_active_aliases_preserved(before, after, active_year=active_year)
    aliases_at = perf_counter()
    timing["active_aliases"] = round(aliases_at - user_configuration_at, 3)

    semantic_optimal_deselections = 0
    for table_name in sorted(_SOURCE_FACT_TABLES & before.keys()):
        if table_name not in after:
            raise PreservationError(f"historical source table disappeared: {table_name}")
        old_history = _historical_source_witness(before[table_name], table_name, active_year)
        new_history = _historical_source_witness(after[table_name], table_name, active_year)
        if _frame_fingerprint(old_history) != _frame_fingerprint(new_history):
            raise PreservationError(f"historical source rows changed in {table_name}")
        semantic_optimal_deselections += _assert_derived_values_not_erased(
            table_name,
            before[table_name],
            after[table_name],
        )
    source_facts_at = perf_counter()
    timing["source_facts"] = round(source_facts_at - aliases_at, 3)

    for table_name, identities in _CAREER_IDENTITIES.items():
        old = before.get(table_name)
        new = after.get(table_name)
        if old is None or old.empty:
            continue
        if new is None or new.empty:
            raise PreservationError(f"career history shrank in {table_name}")
        _assert_rows_and_values_preserved(
            table_name,
            old,
            new,
            identity_candidates=identities,
            monotonic_columns=_MONOTONIC_COLUMNS.get(table_name, ()),
        )
    career_rollups_at = perf_counter()
    timing["career_rollups"] = round(career_rollups_at - source_facts_at, 3)

    for table_name, identities in _HOMEPAGE_IDENTITIES.items():
        old = before.get(table_name)
        new = after.get(table_name)
        if old is None or old.empty:
            continue
        if new is None:
            raise PreservationError(f"homepage output disappeared in {table_name}")
        _assert_rows_and_values_preserved(
            table_name,
            old,
            new,
            identity_candidates=identities,
        )
    homepage_outputs_at = perf_counter()
    timing["homepage_outputs"] = round(homepage_outputs_at - career_rollups_at, 3)
    timing["total"] = round(homepage_outputs_at - started_at, 3)

    return {
        "historical_rows_preserved": True,
        "homepage_values_preserved": True,
        "user_configuration_preserved": True,
        "semantic_optimal_deselections": semantic_optimal_deselections,
        "validation_seconds": timing,
    }


def _assert_active_aliases_preserved(
    before: dict[str, pd.DataFrame],
    after: dict[str, pd.DataFrame],
    *,
    active_year: int,
) -> None:
    """Keep configured display names attached to their stable franchise IDs.

    A provider can legitimately change team or owner metadata, but a refresh
    cannot undo a user's explicit shared-team manager alias. Check the new
    active rows as well as previously published weeks so an added week cannot
    quietly revert to the provider's single-owner display name.
    """
    contexts = before.get("league_context")
    if contexts is None or contexts.empty or "manager_name_overrides_json" not in contexts:
        return
    configured: dict[str, set[str]] = {}
    for row in contexts.itertuples(index=False):
        db_name = str(getattr(row, "db_name", "") or "")
        raw = getattr(row, "manager_name_overrides_json", None)
        if not raw:
            continue
        try:
            overrides = json.loads(raw)
        except (TypeError, ValueError) as error:
            raise PreservationError(f"invalid manager aliases in league_context for {db_name}") from error
        if not isinstance(overrides, dict):
            raise PreservationError(f"invalid manager aliases in league_context for {db_name}")
        aliases = {str(value).strip() for value in overrides.values() if str(value).strip()}
        if aliases:
            configured[db_name] = aliases
    if not configured:
        return

    fid_to_alias: dict[tuple[str, str], str] = {}
    for table_name in ("matchup", "schedule", "player_fantasy"):
        frame = before.get(table_name)
        if frame is None or frame.empty or not {"db_name", "year", "manager", "franchise_id"}.issubset(frame):
            continue
        active = frame.loc[frame["year"].eq(active_year), ["db_name", "manager", "franchise_id"]]
        for db_name, manager, fid in active.itertuples(index=False, name=None):
            db_name, manager, fid = str(db_name), str(manager), str(fid)
            if manager not in configured.get(db_name, set()) or not fid or fid == "nan":
                continue
            key = (db_name, fid)
            if key in fid_to_alias and fid_to_alias[key] != manager:
                raise PreservationError(f"conflicting active aliases for franchise {fid} in {db_name}")
            fid_to_alias[key] = manager

    for table_name in ("matchup", "schedule", "player_fantasy", "homepage_current_standings"):
        frame = after.get(table_name)
        if frame is None or frame.empty or not {"db_name", "manager", "franchise_id"}.issubset(frame):
            continue
        checked = frame.loc[frame["year"].eq(active_year)] if "year" in frame else frame
        for db_name, manager, fid in checked[["db_name", "manager", "franchise_id"]].itertuples(index=False, name=None):
            expected = fid_to_alias.get((str(db_name), str(fid)))
            if expected is not None and str(manager) != expected:
                raise PreservationError(
                    f"active alias changed in {table_name} for franchise {fid} in {db_name}: "
                    f"expected {expected!r}"
                )


def source_preservation_snapshot(
    source_frames: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Copy bounded league-level witnesses before transformations mutate them."""
    return {
        table_name: source_frames[table_name].copy()
        for table_name in PRESERVATION_WITNESS_TABLES
        if table_name in source_frames
    }


def local_preservation_snapshot(
    local_db: Any,
    before: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Read corresponding post-transform witnesses from the local build."""
    return {
        table_name: local_db.read_table(table_name)
        for table_name in before
        if local_db.table_exists(table_name)
    }
