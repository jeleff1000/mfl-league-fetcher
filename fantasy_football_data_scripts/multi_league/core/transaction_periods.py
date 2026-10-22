"""Small, shared transaction-period checks; no provider requests or remote reads."""

from decimal import Decimal, InvalidOperation
import logging


class TransactionWeekError(ValueError):
    """A transaction's week cannot be verified from its provider data."""


YAHOO_TRANSACTION_WINDOWS = "__yahoo_transaction_windows"


def transaction_has_publishable_assets(*, platform: str, transaction_id,
                                      transaction_type, status, has_assets: bool,
                                      administrative: bool = False) -> bool:
    """Filter known non-moves before period checks; never hide incomplete moves.

    Provider parsers determine their native asset shape. Inherit the existing
    fail-closed import/refresh handling via TransactionWeekError, without adding
    requests, retries, or a separate publication path.
    """
    state = str(status or "").strip().lower()
    reason = None
    if state in {"failed", "pending", "cancelled", "canceled", "rejected", "vetoed", "unsuccessful"}:
        reason = f"inactive status={state}"
    elif not has_assets and administrative:
        reason = "administrative event without roster moves"
    elif not has_assets:
        raise TransactionWeekError(
            f"Incomplete {platform} transaction payload for {transaction_id} "
            f"(type={transaction_type}, status={state or 'unknown'}): missing player or draft-pick assets"
        )
    if reason:
        logging.getLogger(__name__).info(
            "[TRANSACTIONS] Skip %s event id=%s type=%s: %s",
            platform, transaction_id, transaction_type, reason,
        )
        return False
    return True


def retain_yahoo_transaction_windows(conn, *, db_name: str, year: int, windows) -> None:
    """Keep the already-fetched provider calendar in worker scratch, never Fly.

    Live-week transactions exist before final matchup rows do. Publication must
    use the independent calendar used by the fetcher, not the transaction's own
    derived dates, and must not pretend that a live matchup is final.
    """
    frame = dated_matchup_windows(windows)[["year", "week", "week_start", "week_end"]]
    table = f"public.{YAHOO_TRANSACTION_WINDOWS}"
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} "
                 "(db_name VARCHAR, year INTEGER, week INTEGER, week_start DATE, week_end DATE)")
    conn.execute(f"DELETE FROM {table} WHERE db_name = ? AND year = ?", [db_name, int(year)])
    conn.register("__provider_transaction_calendar", frame)
    try:
        conn.execute(f"INSERT INTO {table} SELECT ?, year, week, "
                     "try_cast(week_start AS DATE), try_cast(week_end AS DATE) "
                     "FROM __provider_transaction_calendar WHERE year = ?", [db_name, int(year)])
    finally:
        conn.unregister("__provider_transaction_calendar")


def dated_matchup_windows(frame):
    """Remove undated synthetic rows without discarding conflicting real windows."""
    import pandas as pd
    required = {"year", "week", "week_start", "week_end"}
    if not required.issubset(frame.columns):
        return pd.DataFrame(columns=sorted(required))
    start = pd.to_datetime(frame["week_start"], errors="coerce", utc=True)
    end = pd.to_datetime(frame["week_end"], errors="coerce", utc=True)
    return frame.loc[start.notna() & end.notna() & (end >= start)].drop_duplicates(
        subset=["year", "week", "week_start", "week_end"]
    ).sort_values(["year", "week"]).copy()


def local_yahoo_transaction_windows(local_db, year: int):
    """Combine retained live dates and final dates without discarding history."""
    import pandas as pd
    frames = []
    fields = ['year', 'week', 'week_start', 'week_end']
    for table in (YAHOO_TRANSACTION_WINDOWS, 'matchup'):
        try:
            frame = dated_matchup_windows(local_db.read_table(table, year=year))
            frame = frame[pd.to_numeric(frame['year'], errors='coerce') == int(year)]
            if not frame.empty:
                frames.append(frame[fields])
        except Exception:
            continue
    windows = dated_matchup_windows(pd.concat(frames, ignore_index=True)) if frames else pd.DataFrame(columns=fields)
    windows['cumulative_week'] = pd.to_numeric(windows['year']) * 100 + pd.to_numeric(windows['week'])
    return windows.reset_index(drop=True)


def native_transaction_week(value, *, platform: str, transaction_id) -> int | None:
    """Keep explicit provider periods, including preseason zero; never default."""
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value() or not 0 <= number <= 22:
            raise ValueError
        return int(number)
    except (InvalidOperation, ValueError, TypeError) as exc:
        if platform in {"espn", "sleeper"}:
            logging.getLogger(__name__).warning(
                "[TRANSACTIONS] %s transaction timing unresolved for %s: native period=%r; retaining move",
                platform, transaction_id, value,
            )
            return None
        raise TransactionWeekError(
            f"Unverified {platform} transaction week for {transaction_id}: missing or invalid native period"
        ) from exc


def native_transaction_timing_note(value, *, platform: str, week: int | None) -> str | None:
    """Keep the unmodified native period alongside a retained, undated move."""
    if week is None:
        return f"Transaction timing unresolved ({platform} native period={value!r})"
    return None


def validate_transaction_weeks(conn, db_names: list[str], *, platform: str | None = None) -> None:
    """Check only this upload's local transaction rows before building an artifact.

    Native periods do not require a timestamp (ESPN archives can omit it).
    Yahoo's cached periods must agree with one actual league date window.
    """
    def columns(table):
        return {row[0] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_catalog = current_database() "
            "AND table_schema = 'public' AND table_name = ?", [table]
        ).fetchall()}

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    cols = columns("transactions")
    if not cols:
        return
    scope = "TRUE"
    if "db_name" in cols and db_names:
        scope = "(t.db_name IN (" + ",".join(literal(name) for name in db_names) + ") " \
                "OR t.db_name IS NULL OR trim(t.db_name) = '')"
    if not conn.execute(f"SELECT 1 FROM public.transactions t WHERE {scope} LIMIT 1").fetchone():
        return
    id_expr = "t.transaction_id" if "transaction_id" in cols else "'unknown'"
    hint = literal((platform or "").lower().strip())
    provider = f"coalesce(nullif(lower(trim(t.platform)), ''), {hint})" if "platform" in cols else hint
    unresolved = conn.execute(
        f"SELECT {id_expr} FROM public.transactions t WHERE {scope} AND {provider} IN ('', 'unknown') LIMIT 5"
    ).fetchall()
    if unresolved:
        raise TransactionWeekError(f"Unverified transaction week: missing platform provenance for {unresolved}")
    # This change protects Yahoo/ESPN/Sleeper, not other providers' archived contracts.
    scope = f"({scope}) AND {provider} IN ('yahoo', 'espn', 'sleeper')"
    if not conn.execute(f"SELECT 1 FROM public.transactions t WHERE {scope} LIMIT 1").fetchone():
        return
    if not {"year", "week"}.issubset(cols):
        raise TransactionWeekError("Unverified transaction week: missing year/week columns")
    bad_week = "(try_cast(t.week AS DOUBLE) IS NULL OR NOT isfinite(try_cast(t.week AS DOUBLE)) " \
          "OR try_cast(t.week AS DOUBLE) != trunc(try_cast(t.week AS DOUBLE)) " \
          "OR try_cast(t.week AS DOUBLE) NOT BETWEEN 0 AND 22)"
    bad = f"({bad_week} OR try_cast(t.year AS INTEGER) IS NULL)"
    if "cumulative_week" in cols:
        bad = f"({bad} OR try_cast(t.cumulative_week AS BIGINT) IS DISTINCT FROM " \
              "try_cast(t.year AS BIGINT) * 100 + try_cast(t.week AS BIGINT))"
    # Yahoo derives periods from dates; unavailable timing must not block real
    # moves. ESPN/Sleeper have native periods, independent of optional dates.
    invalid = conn.execute(f"SELECT {id_expr} FROM public.transactions t WHERE {scope} "
                           f"AND try_cast(t.year AS INTEGER) IS NULL LIMIT 5").fetchall()
    if invalid:
        raise TransactionWeekError(f"Unverified transaction week: invalid period/join key for {invalid}")
    yahoo_scope = f"{scope} AND {provider} = 'yahoo'"

    def retain_without_timing(invalid_rows=None, *, native=False):
        # Worker-local rows only. Keep identities/assets; never publish guessed
        # periods or stale metrics derived from them. No Fly/network writes.
        from multi_league.core.canonical_transaction import SQL_COLUMNS
        keep = {"NFL_player_id", "nfl_team", "pickup_type", "conveyed_player", "conveyed_lamar",
                "conveyed_fantasy_points", "conveyed_year", "is_conveyed"}
        fields = ({"week", "cumulative_week", "week_start", "week_end"} | (set(SQL_COLUMNS) - keep)) & cols
        if native:
            fields |= {"manager_week", "player_week"} & cols
            if "notes" not in cols:
                conn.execute("ALTER TABLE public.transactions ADD COLUMN notes VARCHAR")
                cols.add("notes")
        changes = [f'"{field}" = NULL' for field in sorted(fields)]
        if native:
            raw_cw = "coalesce(cast(t.cumulative_week AS VARCHAR), 'NULL')" if "cumulative_week" in cols else "'NULL'"
            changes.append("notes = CASE WHEN strpos(coalesce(t.notes, ''), 'native period=') = 0 "
                           "THEN concat_ws('; ', nullif(t.notes, ''), 'Transaction timing unresolved (' || "
                           f"{provider} || ' native period=' || coalesce(cast(t.week AS VARCHAR), 'NULL') || "
                           f"', cumulative_week=' || {raw_cw} || ')') ELSE t.notes END")
        elif "notes" in cols:
            changes.append("notes = CASE WHEN strpos(coalesce(t.notes, ''), 'Transaction timing unresolved') = 0 "
                           "THEN concat_ws('; ', nullif(t.notes, ''), 'Transaction timing unresolved') ELSE t.notes END")
        where = f"t.rowid IN ({invalid_rows})" if invalid_rows else yahoo_scope
        if changes:
            conn.execute(f"UPDATE public.transactions t SET {', '.join(changes)} WHERE {where}")
    native_scope = f"{scope} AND {provider} IN ('espn', 'sleeper')"
    if "cumulative_week" in cols:
        expected_cw = "try_cast(t.year AS BIGINT) * 100 + try_cast(t.week AS BIGINT)"
        conn.execute(f"UPDATE public.transactions t SET cumulative_week = {expected_cw} "
                     f"WHERE {native_scope} AND NOT {bad_week} "
                     f"AND try_cast(t.cumulative_week AS BIGINT) IS DISTINCT FROM {expected_cw}")
    native_invalid = f"SELECT t.rowid FROM public.transactions t WHERE {native_scope} AND {bad_week}"
    if conn.execute(native_invalid + " LIMIT 1").fetchone():
        retain_without_timing(native_invalid, native=True)
        logging.getLogger(__name__).warning(
            "[TRANSACTIONS] Native transaction timing unresolved: retaining moves without timing metrics")
    if not conn.execute(f"SELECT 1 FROM public.transactions t WHERE {yahoo_scope} LIMIT 1").fetchone():
        return
    required = {"year", "week", "week_start", "week_end"}
    calendar_sql = []
    owned_name = literal(db_names[0]) if len(db_names) == 1 else "NULL"
    for table in ("matchup", YAHOO_TRANSACTION_WINDOWS):
        window_cols = columns(table)
        if required.issubset(window_cols):
            window_db = f"coalesce(nullif(db_name, ''), {owned_name})" if "db_name" in window_cols else owned_name
            calendar_sql.append(f"SELECT {window_db} AS db_name, year, week, "
                                "try_cast(week_start AS DATE) AS ws, try_cast(week_end AS DATE) AS we "
                                f"FROM public.{table}")
    if "timestamp" not in cols or not calendar_sql:
        retain_without_timing()
        logging.getLogger(__name__).warning(
            "[TRANSACTIONS] Yahoo transaction timing unresolved: missing timestamps or dated windows; retaining moves")
        return
    txn_db = f"coalesce(nullif(t.db_name, ''), {owned_name})" if "db_name" in cols else owned_name
    invalid_sql = f"""
        WITH events AS (
            SELECT DISTINCT t.rowid AS rid, {txn_db} AS db_name, {id_expr} AS transaction_id,
                   t.year, t.week, try_cast(t.timestamp AS DOUBLE) AS stamp,
                   CASE WHEN try_cast(t.timestamp AS DOUBLE) BETWEEN 1 AND 32503680000
                        THEN cast(to_timestamp(try_cast(t.timestamp AS DOUBLE)) AT TIME ZONE 'UTC' AS DATE)
                   END AS event_date
            FROM public.transactions t WHERE {yahoo_scope}
        ), calendar AS ({' UNION ALL '.join(calendar_sql)}), windows AS (
            SELECT DISTINCT db_name, year, week, ws, we FROM calendar
            WHERE ws IS NOT NULL AND we >= ws
        ), starts AS (
            SELECT db_name, year, min(ws) AS first_start FROM windows GROUP BY db_name, year
        ), verified AS (
            SELECT e.rid, e.db_name, e.transaction_id, e.year, e.week, e.stamp,
                   count(w.week) AS matches, min(w.week) AS expected_week
            FROM events e
            LEFT JOIN starts s ON s.db_name IS NOT DISTINCT FROM e.db_name AND s.year = e.year
            LEFT JOIN windows w ON w.db_name IS NOT DISTINCT FROM e.db_name AND w.year = e.year
              AND ((e.event_date BETWEEN w.ws AND w.we)
                OR (e.event_date < s.first_start AND w.ws = s.first_start))
            GROUP BY e.rid, e.db_name, e.transaction_id, e.year, e.week, e.stamp
        )
        SELECT v.rid FROM verified v JOIN public.transactions t ON t.rowid = v.rid
        WHERE matches != 1 OR v.week IS DISTINCT FROM expected_week OR {bad}
    """
    invalid = conn.execute(invalid_sql + " LIMIT 5").fetchall()
    if invalid:
        retain_without_timing(invalid_sql)
        logging.getLogger(__name__).warning(
            "[TRANSACTIONS] Yahoo transaction timing unresolved: missing, ambiguous or mismatched windows for %s; retaining moves",
            invalid,
        )
