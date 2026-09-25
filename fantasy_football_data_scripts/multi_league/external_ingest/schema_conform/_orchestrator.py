"""Top-level orchestrator: wires normalize → score → invariants → derive → coalesce →
coverage → ledger writes → conformed_* writes.

Public entry point: run(conn, db_name, run_id).
"""

from __future__ import annotations
import hashlib
import logging
from types import SimpleNamespace

import duckdb
import duckdb as _duckdb
import pandas as pd

from ._config import (
    REQUIRED_IDENTITY_SLOTS,
    REQUIRED_DATA_SLOTS,
    OPTIONAL_SLOTS,
    CONFIDENCE_FLOOR,
    COLLISION_GAP,
)
from ._normalize import normalize_columns, _norm
from ._passes import run_passes
from ._invariants import validate_invariants
from ._derivation import apply_derivation, DERIVATION_RULES
from ._coalesce import COALESCE_INPUT_COLS, apply_coalesce
from ._coverage import (
    check_manager_guid_coverage,
    check_known_managers_resolve_canonical,
    check_new_managers_stable_synthetic,
    check_cross_table_consistency,
    apply_required_data_null_guard,
)
from ._abort import (
    AbortGate,
    SchemaConformAbort,
    SchemaConformFailure,
    SUGGESTIONS,
)
from ._ledger import (
    create_schema_decisions_table,
    log_decision,
    create_external_identity_overrides_table,
    read_overrides_for_db,
    create_import_run_summary_table,
    log_run_summary,
)

logger = logging.getLogger(__name__)

TABLES = ["matchup", "player_fantasy", "draft", "transactions"]

# Pass slot definitions per table
PASS_SLOTS_PER_TABLE = {
    "matchup": {
        1: ["year", "week"],
        2: ["manager", "opponent", "team_name"],
        3: ["manager_guid", "team_key"],
        # win/loss/tie sit alongside scores so the matcher can still bind them
        # when external data has the flags but no points (KMFFL 2013-style).
        4: ["team_points", "opponent_points", "division_id", "win", "loss", "tie"],
    },
    "player_fantasy": {
        1: ["year", "week"],
        2: ["manager", "player"],
        3: ["manager_guid", "team_key", "yahoo_player_id"],
        4: ["fantasy_position", "position", "fantasy_points"],
    },
    "draft": {
        1: ["year", "round", "pick"],
        2: ["manager", "player"],
        3: ["manager_guid", "team_key", "yahoo_player_id"],
        4: ["cost", "position", "draft_type"],
    },
    "transactions": {
        1: ["year", "week"],
        2: ["manager", "player", "transaction_type"],
        3: ["manager_guid", "transaction_id", "yahoo_player_id"],
        4: ["source_type", "destination", "faab_bid", "timestamp"],
    },
}

COOCCURRENCE_ANCHORS_PER_TABLE = {
    "matchup": {"manager_guid": "manager", "team_key": "team_name"},
    "player_fantasy": {"manager_guid": "manager", "team_key": "team_name", "yahoo_player_id": "player"},
    "draft": {"manager_guid": "manager", "team_key": "manager", "yahoo_player_id": "player"},
    # transaction_id has low MI with transaction_type (same txn_id spans add+drop rows).
    # Use manager as the co-occurrence anchor instead (MI ≈ 0.57 in KMFFL data).
    "transactions": {"manager_guid": "manager", "transaction_id": "manager", "yahoo_player_id": "player"},
}


def run(
    conn: duckdb.DuckDBPyConnection,
    db_name: str,
    run_id: str,
    franchise_merges: list[dict] | None = None,
) -> None:
    """Main entry point. No-op when no raw external data exists for db_name."""
    # Ensure staging schema exists before creating ledger tables
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")

    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    create_import_run_summary_table(conn)

    # Build name_to_guid context once from the in-process DuckDB
    ctx = _build_context(conn, db_name, franchise_merges=franchise_merges)
    overrides = read_overrides_for_db(conn, db_name)

    failures: list[SchemaConformFailure] = []
    conformed_dfs: dict[str, pd.DataFrame] = {}

    for table in TABLES:
        if not _has_raw(conn, table, db_name):
            continue
        try:
            conformed = _conform_table(conn, db_name, run_id, table, ctx, overrides)
            conformed_dfs[table] = conformed
        except _PerTableAbort as e:
            failures.extend(e.failures)

    # Cross-table consistency (only if multiple tables conformed).
    # For REQUIRED IDENTITY slots (manager_guid): hard-abort on inconsistency.
    # For OPTIONAL slots (team_key): demote — NULL the column in the offending
    # table and log a warning rather than aborting the whole import. team_key
    # mismatches are commonly caused by the matcher binding an unrelated low-
    # confidence column to team_key in one table when no real team_key exists
    # there; demoting strips the wrong value, downstream merger handles NULL.
    if len(conformed_dfs) >= 2 and not failures:
        # manager_guid is REQUIRED IDENTITY in every table — abort on mismatch.
        cf = check_cross_table_consistency(conformed_dfs, slot="manager_guid")
        if cf:
            failures.append(
                SchemaConformFailure(
                    table=cf.table or "<cross-table>",
                    gate=AbortGate.COVERAGE_BELOW_THRESHOLD,
                    ddl_slot="manager_guid",
                    source_cols_considered=[],
                    sample_source_values={},
                    best_score=None,
                    second_best_score=None,
                    second_best_slot=None,
                    suggested_action=cf.detail,
                    invariant_failures=[],
                )
            )

        # team_key is OPTIONAL — demote rather than abort.
        cf = check_cross_table_consistency(conformed_dfs, slot="team_key")
        if cf and cf.table and cf.table in conformed_dfs:
            offending = conformed_dfs[cf.table]
            if "team_key" in offending.columns:
                offending["team_key"] = None
                logger.warning(f"[cross-table-consistency] demoted team_key in {cf.table} to NULL: {cf.detail}")

    if failures:
        # Persist a failure summary so the frontend status page can render it.
        # Wrapped in try/except so a transient DuckDB write error doesn't
        # obscure the real SchemaConformAbort that follows.
        try:
            log_run_summary(
                conn,
                db_name=db_name,
                run_id=run_id,
                phase="schema_conform",
                status="failed",
                failures_json=[
                    {
                        "table": f.table,
                        "gate": f.gate.name,
                        "ddl_slot": f.ddl_slot,
                        "source_cols_considered": f.source_cols_considered,
                        "best_score": f.best_score,
                        "second_best_score": f.second_best_score,
                        "second_best_slot": f.second_best_slot,
                        "suggested_action": f.suggested_action,
                        "invariant_failures": f.invariant_failures,
                    }
                    for f in failures
                ],
            )
        except Exception as e:
            logger.warning("failed to write import_run_summary on abort: %s", e)
        raise SchemaConformAbort(failures=failures)

    # Write conformed_* tables
    for table, df in conformed_dfs.items():
        _write_conformed(conn, table, df)


# --- internals ---


class _PerTableAbort(Exception):
    def __init__(self, failures):
        self.failures = failures


def _build_context(
    conn,
    db_name: str,
    franchise_merges: list[dict] | None = None,
) -> SimpleNamespace:
    name_to_guid: dict[str, str] = {}
    team_key_to_guid: dict[str, str] = {}
    try:
        rows = conn.execute(
            "SELECT DISTINCT manager, manager_guid FROM public.matchup "
            "WHERE db_name=? AND manager_guid IS NOT NULL AND manager IS NOT NULL",
            [db_name],
        ).df()
        for _, r in rows.iterrows():
            name_to_guid[_norm(r["manager"])] = r["manager_guid"]
        try:
            rows = conn.execute(
                "SELECT DISTINCT team_key, manager_guid FROM public.matchup "
                "WHERE db_name=? AND team_key IS NOT NULL AND manager_guid IS NOT NULL",
                [db_name],
            ).df()
            for _, r in rows.iterrows():
                team_key_to_guid[r["team_key"]] = r["manager_guid"]
        except _duckdb.CatalogException:
            pass  # team_key column absent — non-fatal
        except Exception as e:
            logger.warning("_build_context: team_key query unexpected error: %s", e)
    except _duckdb.CatalogException:
        pass  # public.matchup absent — first import for league, non-fatal
    except Exception as e:
        logger.warning("_build_context: matchup query unexpected error: %s", e)
        # Re-raise so caller sees the corruption rather than silently
        # falling through to synthetic-id minting on every manager.
        raise
    guid_merges: dict[str, str] = {}
    for merge in franchise_merges or []:
        owner_ids = [str(owner_id).strip() for owner_id in merge.get("owner_ids", []) if str(owner_id).strip()]
        if len(owner_ids) < 2:
            continue
        canonical = str(merge.get("into_franchise_id") or owner_ids[0]).strip()
        for old_owner_id in owner_ids:
            if old_owner_id != canonical:
                guid_merges[old_owner_id] = canonical

    # The coverage gate compares conformed rows against these canonical
    # references. Apply the same explicit merge map to both sides first so a
    # saved historical/current owner merge is not misclassified as conflict.
    name_to_guid = {name: guid_merges.get(str(guid), guid) for name, guid in name_to_guid.items()}
    team_key_to_guid = {key: guid_merges.get(str(guid), guid) for key, guid in team_key_to_guid.items()}

    return SimpleNamespace(
        name_to_guid=name_to_guid,
        team_key_to_guid=team_key_to_guid,
        guid_merges=guid_merges,
        df=None,
    )


def _has_raw(conn, table: str, db_name: str) -> bool:
    try:
        n = conn.execute(
            f"SELECT count(*) FROM staging.staging_{table} WHERE db_name=?",
            [db_name],
        ).fetchone()[0]
        return n > 0
    except Exception:
        return False


def _conform_table(conn, db_name, run_id, table, ctx, overrides) -> pd.DataFrame:
    raw = conn.execute(f"SELECT * FROM staging.staging_{table} WHERE db_name=?", [db_name]).df()
    raw = normalize_columns(raw)

    # Strip coalesce-input columns from assignment pool
    consumed = set()
    for inputs in COALESCE_INPUT_COLS.values():
        consumed.update(inputs)
    available_src = [c for c in raw.columns if c not in consumed]
    src_df = raw[available_src].copy()
    ctx.df = src_df

    # Build reference columns from in-process canonical (public.<table>) for this league
    ref_cols = _build_ref_cols(conn, table, db_name)

    pass_slots = PASS_SLOTS_PER_TABLE[table]
    cooc_anchors = COOCCURRENCE_ANCHORS_PER_TABLE[table]

    # Only pass ref_cols for slots that exist in ref_cols
    filtered_pass_slots = {}
    for pass_num, slots in pass_slots.items():
        filtered = [s for s in slots if s in ref_cols]
        if table == "draft" and pass_num == 3:
            # Draft spreadsheets commonly have year/round/pick/manager/player/cost
            # but no platform IDs. If we let pass 3 fuzzy-bind identity slots,
            # numeric auction columns such as cost can be consumed as a bogus
            # yahoo_player_id before pass 4 maps them to cost. Draft identity
            # hints are preserved exactly below when present; manager_guid is
            # derived from manager.
            filtered = []
        filtered_pass_slots[pass_num] = filtered

    bindings, ledger_rows = run_passes(
        src_df,
        ref_cols,
        filtered_pass_slots,
        ctx,
        cooccurrence_anchors=cooc_anchors,
    )

    # Drop bindings whose score is below floor for REQUIRED_IDENTITY (will be unmapped)
    failures: list[SchemaConformFailure] = []
    final_status: dict[str, dict] = {}  # source_col → status info

    # Check for cross-source collision rows emitted by run_passes
    for row in ledger_rows:
        if not row.get("_cross_source_collision"):
            continue
        slot = row["ddl_slot"]
        if slot in REQUIRED_IDENTITY_SLOTS[table]:
            failures.append(
                _make_failure(
                    table,
                    AbortGate.SLOT_COLLISION,
                    slot,
                    [row["source_col"], row["second_best_slot"]],
                    {},
                    row["score"],
                    row["second_best_score"],
                    row["second_best_slot"],
                    [],
                )
            )

    for row in ledger_rows:
        slot, src, score = row["ddl_slot"], row["source_col"], row["score"]
        is_identity_required = slot in REQUIRED_IDENTITY_SLOTS[table]
        is_data_required = slot in REQUIRED_DATA_SLOTS[table]

        # Validate invariants — ALWAYS run, even when source column name exactly
        # matches the DDL slot. The exact-name-match bypass is only for the cosine
        # confidence floor (low score caused by string-vs-typed dtype mismatch).
        # Invariants encode data-quality contracts (e.g., guid_shape must be
        # uppercase 26-char base32) that must hold regardless of column name.
        is_exact_name_match = src == slot
        if src in src_df.columns:
            invariant_failures = validate_invariants(slot, src_df[src], ctx)
        else:
            invariant_failures = []
        if invariant_failures:
            row["status"] = "invariant_failed"
            row["invariant_failures"] = invariant_failures
            if is_identity_required:
                # Defer abort for slots with DERIVATION_RULES — derivation runs after
                # binding and may successfully populate the slot from another source col
                # (e.g., manager_guid derived via fuzzy lookup from manager name). The
                # coverage gates further down catch the case where derivation also fails.
                if slot in DERIVATION_RULES:
                    row["status"] = "pending_derivation"
                    continue
                # If score is also very low, this is really an unmapped slot, not just
                # a format mismatch — use UNMAPPED_REQUIRED so callers can distinguish.
                inv_gate = AbortGate.UNMAPPED_REQUIRED if score < 0.5 else AbortGate.LOW_CONFIDENCE_REQUIRED
                failures.append(
                    _make_failure(
                        table,
                        inv_gate,
                        slot,
                        [src],
                        {src: src_df[src].dropna().astype(str).head(5).tolist()} if src in src_df.columns else {},
                        score,
                        row.get("second_best_score"),
                        row.get("second_best_slot"),
                        invariant_failures,
                    )
                )
            continue

        # Confidence floor for required IDENTITY.
        # Skip when source column name is an exact match for the DDL slot — the user
        # has already named the column correctly, so low cosine score just reflects a
        # dtype/value mismatch between external (string) and canonical (typed) data.
        if score < CONFIDENCE_FLOOR and is_identity_required and not is_exact_name_match:
            # Defer abort for slots with DERIVATION_RULES — same reasoning as the
            # invariant-failure branch above.
            if slot in DERIVATION_RULES:
                row["status"] = "pending_derivation"
                continue
            # Very low score (< 0.5) means no plausible source column → UNMAPPED_REQUIRED.
            # Moderate score (0.5..CONFIDENCE_FLOOR) means a candidate exists but confidence
            # is insufficient → LOW_CONFIDENCE_REQUIRED.
            gate = AbortGate.UNMAPPED_REQUIRED if score < 0.5 else AbortGate.LOW_CONFIDENCE_REQUIRED
            row["status"] = "unmapped_required" if gate == AbortGate.UNMAPPED_REQUIRED else "low_confidence_required"
            failures.append(
                _make_failure(
                    table,
                    gate,
                    slot,
                    [src],
                    {src: src_df[src].dropna().astype(str).head(5).tolist()} if src in src_df.columns else {},
                    score,
                    row.get("second_best_score"),
                    row.get("second_best_slot"),
                    [],
                )
            )
            continue

        # Slot collision: runner-up too close — also skip for exact name matches.
        if (
            not is_exact_name_match
            and row.get("second_best_score") is not None
            and (score - row["second_best_score"]) < COLLISION_GAP
            and row["second_best_score"] >= CONFIDENCE_FLOOR
        ):
            row["status"] = "slot_collision"
            if is_identity_required:
                failures.append(
                    _make_failure(
                        table,
                        AbortGate.SLOT_COLLISION,
                        slot,
                        [src],
                        {src: src_df[src].dropna().astype(str).head(5).tolist()} if src in src_df.columns else {},
                        score,
                        row["second_best_score"],
                        row["second_best_slot"],
                        [],
                    )
                )
            continue

        # Confidence floor for DATA → demote
        if score < CONFIDENCE_FLOOR:
            row["status"] = "low_confidence_data" if is_data_required else "bound"
        else:
            row["status"] = "bound"

        final_status[src] = {"slot": slot, "row": row}

    if failures:
        # Persist what we have AND raise per-table abort
        _flush_ledger(conn, db_name, run_id, table, ledger_rows)
        raise _PerTableAbort(failures)

    # Build conformed dataframe — start with the source columns that bound successfully
    conformed = pd.DataFrame(index=src_df.index)
    conformed["db_name"] = db_name
    bound_pairs = [(info["slot"], src) for src, info in final_status.items()]
    for slot, src in bound_pairs:
        conformed[slot] = src_df[src].values

    if table == "draft":
        for slot in (
            "manager_guid",
            "team_key",
            "yahoo_player_id",
            "sleeper_player_id",
            "espn_player_id",
            "NFL_player_id",
        ):
            if slot in conformed.columns or slot not in src_df.columns:
                continue
            if _has_nonblank_values(src_df[slot]):
                conformed[slot] = src_df[slot].values

    # Apply derivation for required IDENTITY slots not yet bound.
    # Read from conformed (which has DDL slot names like 'manager') NOT src_df
    # (which has original source col names like 'mgr'). Derivation rules
    # reference slot names — running against src_df breaks under aliasing.
    for slot in REQUIRED_IDENTITY_SLOTS[table]:
        if slot in conformed.columns and conformed[slot].notna().mean() >= 0.95:
            continue
        if slot in DERIVATION_RULES:
            derived = apply_derivation(conformed, slot, ctx)
            if slot in conformed.columns:
                conformed[slot] = conformed[slot].fillna(derived)
            else:
                conformed[slot] = derived
            ledger_rows.append(
                {
                    "source_col": None,
                    "ddl_slot": slot,
                    "score": None,
                    "score_components": {},
                    "second_best_score": None,
                    "second_best_slot": None,
                    "pass_number": None,
                    "was_derived": True,
                    "derivation_rule": "from_manager_name",
                    "was_coalesced": False,
                    "coalesce_inputs": None,
                    "status": "derived",
                    "invariant_failures": [],
                    "sample_values": derived.dropna().astype(str).head(5).tolist(),
                }
            )

    # Archived seasons can contain a Yahoo owner ID that predates the current
    # renewal chain. Apply only explicit, user-saved franchise merges here;
    # matching by display name alone would conflate different people who share
    # a name. This must happen before the canonical-identity coverage gate.
    if ctx.guid_merges:
        for slot in ("manager_guid", "opponent_guid"):
            if slot in conformed.columns:
                conformed[slot] = conformed[slot].map(lambda value: ctx.guid_merges.get(str(value), value))

    # Apply coalesce for slots whose inputs are present
    for slot, inputs in COALESCE_INPUT_COLS.items():
        if any(c in raw.columns for c in inputs) and slot in (REQUIRED_DATA_SLOTS[table] + OPTIONAL_SLOTS[table]):
            conformed[slot] = apply_coalesce(raw, slot)
            ledger_rows.append(
                {
                    "source_col": None,
                    "ddl_slot": slot,
                    "score": None,
                    "score_components": {},
                    "second_best_score": None,
                    "second_best_slot": None,
                    "pass_number": None,
                    "was_derived": False,
                    "derivation_rule": None,
                    "was_coalesced": True,
                    "coalesce_inputs": inputs,
                    "status": "coalesced",
                    "invariant_failures": [],
                    "sample_values": [],
                }
            )

    # Synthetic ids for new managers (apply BEFORE coverage gates)
    if "manager" in src_df.columns and "manager_guid" in conformed.columns:
        for idx, row in conformed.iterrows():
            if pd.notna(row.get("manager_guid")):
                continue
            mgr = src_df.loc[idx, "manager"] if "manager" in src_df.columns else None
            if mgr is None or pd.isna(mgr):
                continue
            normed = _norm(mgr)
            if normed in overrides:
                ov = overrides[normed]
                conformed.at[idx, "manager_guid"] = ov["target_franchise_id"] or ov["external_hash"]
            elif normed in ctx.name_to_guid:
                conformed.at[idx, "manager_guid"] = ctx.name_to_guid[normed]
            else:
                # Stable synthetic
                h = hashlib.sha1((db_name + normed).encode()).hexdigest()[:12]
                conformed.at[idx, "manager_guid"] = f"external_{h}"

    # Coerce types based on DDL (year/week → int, points → float)
    conformed = _coerce_types(conformed)

    # Required-DATA null guard (post-coercion)
    conformed = apply_required_data_null_guard(conformed, table)

    # Drop rows with no identifiable manager — these are typically Yahoo's
    # "undrafted player pool" rows in draft, or placeholder rows in other
    # tables, where both manager AND manager_guid came back NULL after
    # normalization. They can't conform and shouldn't count against coverage.
    if "manager" in conformed.columns or "manager_guid" in conformed.columns:
        before = len(conformed)
        has_mgr = pd.Series(False, index=conformed.index)
        if "manager" in conformed.columns:
            has_mgr |= conformed["manager"].notna() & (conformed["manager"].astype(str).str.strip() != "")
        if "manager_guid" in conformed.columns:
            has_mgr |= conformed["manager_guid"].notna()
        conformed = conformed[has_mgr].copy()
        dropped = before - len(conformed)
        if dropped > 0:
            logger.info(f"[{table}] dropped {dropped:,} rows with no identifiable manager (of {before:,})")

    # Coverage gates
    cov_failure = check_manager_guid_coverage(conformed, table)
    if cov_failure:
        failures.append(_failure_from_coverage(cov_failure, table))

    # Build a df that has manager column merged in for known/new manager checks
    df_for_known = conformed.copy()
    if "manager" not in df_for_known.columns and "manager" in src_df.columns:
        df_for_known["manager"] = src_df["manager"].values
    cov_failure = check_known_managers_resolve_canonical(df_for_known, ctx)
    if cov_failure:
        failures.append(_failure_from_coverage(cov_failure, table))
    cov_failure = check_new_managers_stable_synthetic(df_for_known, ctx)
    if cov_failure:
        failures.append(_failure_from_coverage(cov_failure, table))

    _flush_ledger(conn, db_name, run_id, table, ledger_rows)

    if failures:
        raise _PerTableAbort(failures)

    return conformed


def _build_ref_cols(conn, table: str, db_name: str) -> dict[str, pd.Series]:
    """Pull canonical reference columns from public.<table> for this league."""
    try:
        df = conn.execute(f"SELECT * FROM public.{table} WHERE db_name=?", [db_name]).df()
    except Exception:
        df = pd.DataFrame()
    return {c: df[c] for c in df.columns}


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ("year", "week", "round", "pick"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    for c in (
        "team_points",
        "opponent_points",
        "fantasy_points",
        "team_projected_points",
        "opponent_projected_points",
        "cost",
        "faab_bid",
    ):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _has_nonblank_values(series: pd.Series) -> bool:
    text = series.astype("string").str.strip().str.lower()
    return bool((series.notna() & ~text.isin({"", "none", "nan", "<na>"})).any())


def _flush_ledger(conn, db_name, run_id, table, rows):
    for row in rows:
        # Skip internal-only cross-source collision markers — they are not
        # separate ledger rows; the orchestrator emits failures for them instead.
        if row.get("_cross_source_collision"):
            continue
        log_decision(
            conn,
            {
                "db_name": db_name,
                "run_id": run_id,
                "table_name": table,
                **{
                    k: row.get(k)
                    for k in (
                        "source_col",
                        "ddl_slot",
                        "score",
                        "score_components",
                        "second_best_score",
                        "second_best_slot",
                        "pass_number",
                        "was_derived",
                        "derivation_rule",
                        "was_coalesced",
                        "coalesce_inputs",
                        "status",
                        "invariant_failures",
                        "sample_values",
                    )
                },
            },
        )


def _write_conformed(conn, table: str, df: pd.DataFrame) -> None:
    """Idempotent write: replace previous conformed rows for this db_name."""
    target = f"staging.conformed_{table}"
    conn.register("conformed_view", df)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM conformed_view WHERE 0=1")
    db_names = df["db_name"].unique().tolist() if "db_name" in df.columns else []
    if db_names:
        ph = ",".join("?" * len(db_names))
        conn.execute(f"DELETE FROM {target} WHERE db_name IN ({ph})", db_names)
    conn.execute(f"INSERT INTO {target} SELECT * FROM conformed_view")


def _make_failure(table, gate, slot, src_cols, samples, score, sb_score, sb_slot, inv) -> SchemaConformFailure:
    return SchemaConformFailure(
        table=table,
        gate=gate,
        ddl_slot=slot,
        source_cols_considered=src_cols,
        sample_source_values=samples,
        best_score=score,
        second_best_score=sb_score,
        second_best_slot=sb_slot,
        suggested_action=SUGGESTIONS[gate],
        invariant_failures=inv,
    )


def _failure_from_coverage(cf, table) -> SchemaConformFailure:
    return SchemaConformFailure(
        table=table,
        gate=AbortGate.COVERAGE_BELOW_THRESHOLD,
        ddl_slot=None,
        source_cols_considered=[],
        sample_source_values={},
        best_score=None,
        second_best_score=None,
        second_best_slot=None,
        suggested_action=f"{cf.gate}: {cf.detail}",
        invariant_failures=[],
    )
