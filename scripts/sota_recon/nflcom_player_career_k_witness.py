"""Measure NFL.com K Career mappings against the v26 career subject plane.

The K page publishes percentage rates in percent units while v26 stores the same
rates as fractions.  Counts are summed over the source's season/team rows and
long fields use MAX.  This lane is measurement-only: it changes no decision,
backfill, promotion, or license state.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry, v26_plane


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_k_witness.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def close_rate(left: float | None, right: float | None, tolerance: float = 0.11) -> bool:
    """Compare percentage-form values after rounding to the source's one decimal."""
    return left is not None and right is not None and abs(left - right) <= tolerance


def rate_from_counts(made: float | None, attempts: float | None) -> float | None:
    if made is None or attempts is None or attempts == 0:
        return None
    return 100.0 * made / attempts


def _count_measurement(con: duckdb.DuckDBPyConnection, source_field: str, canonical: str, form: str, v26: str) -> dict:
    aggregate = "MAX" if form == "MAX" else "SUM"
    sql = f"""WITH s AS (
        SELECT nflcom_slug, {aggregate}(TRY_CAST("{source_field}" AS DOUBLE)) source_value
        FROM src WHERE TRY_CAST("{source_field}" AS DOUBLE) IS NOT NULL GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug, MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v."{canonical}" IS NOT NULL GROUP BY 1
      )
      SELECT s.source_value, v.target_value FROM s JOIN v USING (nflcom_slug)"""
    pairs = con.execute(sql).fetchall()
    informative = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a != 0 or b != 0)]
    agree = sum(int(a == b) for a, b in informative)
    source_hi = sum(int(a > b) for a, b in informative)
    target_hi = sum(int(a < b) for a, b in informative)
    return {
        "source_field": source_field,
        "canonical": canonical,
        "witness_form": form,
        "paired_n": len(pairs),
        "informative_n": len(informative),
        "agree_n": agree,
        "agree_pct": round(100.0 * agree / len(informative), 2) if informative else None,
        "source_exceeds_target": source_hi,
        "target_exceeds_source": target_hi,
    }


def _rate_measurement(con: duckdb.DuckDBPyConnection, source_rate: str, source_made: str, source_att: str, canonical: str, v26_made: str, v26_att: str, v26: str, candidate_role: str) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug,
               SUM(TRY_CAST("{source_made}" AS DOUBLE)) made,
               SUM(TRY_CAST("{source_att}" AS DOUBLE)) attempts,
               MAX(TRY_CAST("{source_rate}" AS DOUBLE)) published_rate
        FROM src GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug,
               MAX(TRY_CAST(v."{v26_made}" AS DOUBLE)) made,
               MAX(TRY_CAST(v."{v26_att}" AS DOUBLE)) attempts,
               MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) stored_rate
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        GROUP BY 1
      )
      SELECT s.published_rate, s.made, s.attempts, v.made, v.attempts, v.stored_rate
      FROM s JOIN v USING (nflcom_slug)"""
    rows = con.execute(sql).fetchall()
    informative = []
    for published, made, attempts, v_made, v_attempts, stored in rows:
        source_expected = rate_from_counts(made, attempts)
        target_expected = rate_from_counts(v_made, v_attempts)
        # A rate is informative when either side has a nonzero denominator.
        if source_expected is None and target_expected is None:
            continue
        informative.append((published, source_expected, target_expected, stored))
    source_recomputed_agree = sum(int(close_rate(p, s)) for p, s, _, _ in informative if p is not None and s is not None)
    target_recomputed_agree = sum(
        int(close_rate(s, 100.0 * t) if t is not None else False)
        for _, s, t, _ in informative
        if s is not None
    )
    target_stored_agree = sum(
        int(close_rate(s, 100.0 * stored) if stored is not None else False)
        for _, s, _, stored in informative
        if s is not None
    )
    stored_pairs = [(s, 100.0 * stored) for _, s, _, stored in informative if s is not None and stored is not None]
    return {
        "source_rate": source_rate,
        "source_operands": [source_made, source_att],
        "canonical": canonical,
        "candidate_role": candidate_role,
        "v26_operands": [v26_made, v26_att],
        "source_rate_units": "percent",
        "v26_rate_units": "fraction; multiplied by 100 for comparison",
        "paired_n": len(rows),
        "informative_n": len(informative),
        "source_published_vs_source_operands_agree_n": source_recomputed_agree,
        "source_published_vs_source_operands_agree_pct": round(100.0 * source_recomputed_agree / len(informative), 2) if informative else None,
        "source_operands_vs_v26_operands_agree_n": target_recomputed_agree,
        "source_operands_vs_v26_operands_agree_pct": round(100.0 * target_recomputed_agree / len(informative), 2) if informative else None,
        "source_operands_vs_v26_stored_rate_agree_n": target_stored_agree,
        "source_operands_vs_v26_stored_rate_agree_pct": round(100.0 * target_stored_agree / len(informative), 2) if informative else None,
        "source_operands_vs_v26_stored_rate_paired_n": len(stored_pairs),
        "source_operands_exceeds_v26_stored_rate": sum(int(s > t) for s, t in stored_pairs),
        "v26_stored_rate_exceeds_source_operands": sum(int(s < t) for s, t in stored_pairs),
    }


def measure() -> dict:
    con = duckdb.connect()
    src_glob = _q(SOURCE / "**/*.parquet")
    # Preserve season/team identity while removing exact duplicate rows.
    con.execute(f"""CREATE TEMP TABLE src AS
        SELECT DISTINCT nflcom_slug, season, team, g, gs, fg_att, fgm, pct, lng, blk,
                        xp_att, xpm, xpct, xblk
        FROM read_parquet('{src_glob}', union_by_name=true)
        WHERE _table='K Career'""")
    con.execute(f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(v26_plane("career"))
    counts = [
        _count_measurement(con, "g", "games_played", "SUM", v26),
        _count_measurement(con, "gs", "games_started", "SUM", v26),
        _count_measurement(con, "fg_att", "fg_att", "SUM", v26),
        _count_measurement(con, "fgm", "fg_made", "SUM", v26),
        _count_measurement(con, "lng", "fg_long", "MAX", v26),
        _count_measurement(con, "blk", "fg_blocked", "SUM", v26),
        _count_measurement(con, "xp_att", "pat_att", "SUM", v26),
        _count_measurement(con, "xpm", "pat_made", "SUM", v26),
        _count_measurement(con, "xblk", "pat_blocked", "SUM", v26),
    ]
    rates = [
        _rate_measurement(con, "pct", "fgm", "fg_att", "fg_pct", "fg_made", "fg_att", v26, "natural"),
        _rate_measurement(con, "xpct", "xpm", "xp_att", "pat_pct", "pat_made", "pat_att", v26, "legacy-name contrast"),
        _rate_measurement(con, "xpct", "xpm", "xp_att", "xp_pct", "pat_made", "pat_att", v26, "preferred career-plane witness"),
    ]
    return {
        "measurement_only": True,
        "source_table": "K Career",
        "subject_plane": "player_nfl_career",
        "source_deduplication": "SELECT DISTINCT including season/team before aggregation",
        "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),
        "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),
        "counts": counts,
        "rates": rates,
        "rate_denominator": "player-level source and target operand totals; zero/zero omitted",
        "rate_tolerance": "0.11 percentage points",
        "no_state_change": True,
    }


def main() -> None:
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
