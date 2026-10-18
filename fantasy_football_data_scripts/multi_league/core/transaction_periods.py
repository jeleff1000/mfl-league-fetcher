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


def native_transaction_week(value, *, platform: str, transaction_id) -> int:
    """Keep explicit provider periods, including preseason zero; never default."""
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value() or not 0 <= number <= 22:
            raise ValueError
        return int(number)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise TransactionWeekError(
            f"Unverified {platform} transaction week for {transaction_id}: missing or invalid native period"
        ) from exc


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
    bad = "(try_cast(t.week AS DOUBLE) IS NULL OR NOT isfinite(try_cast(t.week AS DOUBLE)) " \
          "OR try_cast(t.week AS DOUBLE) != trunc(try_cast(t.week AS DOUBLE)) " \
          "OR try_cast(t.week AS DOUBLE) NOT BETWEEN 0 AND 22 " \
          "OR try_cast(t.year AS INTEGER) IS NULL)"
    if "cumulative_week" in cols:
        bad = f"({bad} OR try_cast(t.cumulative_week AS BIGINT) IS DISTINCT FROM " \
              "try_cast(t.year AS BIGINT) * 100 + try_cast(t.week AS BIGINT))"
    invalid = conn.execute(f"SELECT {id_expr} FROM public.transactions t WHERE {scope} AND {bad} LIMIT 5").fetchall()
    if invalid:
        raise TransactionWeekError(f"Unverified transaction week: invalid period/join key for {invalid}")
    yahoo_scope = f"{scope} AND {provider} = 'yahoo'"
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
        raise TransactionWeekError("Unverified Yahoo transaction week: missing timestamps or dated windows")
    txn_db = f"coalesce(nullif(t.db_name, ''), {owned_name})" if "db_name" in cols else owned_name
    invalid = conn.execute(f"""
        WITH events AS (
            SELECT DISTINCT {txn_db} AS db_name, {id_expr} AS transaction_id,
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
            SELECT e.db_name, e.transaction_id, e.year, e.week, e.stamp,
                   count(w.week) AS matches, min(w.week) AS expected_week
            FROM events e
            LEFT JOIN starts s ON s.db_name IS NOT DISTINCT FROM e.db_name AND s.year = e.year
            LEFT JOIN windows w ON w.db_name IS NOT DISTINCT FROM e.db_name AND w.year = e.year
              AND ((e.event_date BETWEEN w.ws AND w.we)
                OR (e.event_date < s.first_start AND w.ws = s.first_start))
            GROUP BY e.db_name, e.transaction_id, e.year, e.week, e.stamp
        )
        SELECT transaction_id FROM verified
        WHERE matches != 1 OR week IS DISTINCT FROM expected_week LIMIT 5
    """).fetchall()
    if invalid:
        raise TransactionWeekError(f"Unverified Yahoo transaction week: missing, ambiguous or mismatched windows for {invalid}")
