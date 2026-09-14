"""Idempotently apply platform-neutral playoff evidence to the lake."""

from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd


KEYS = ("db_name", "year", "week", "NFL_player_id")


def apply_overlay(con: duckdb.DuckDBPyConnection, evidence: pd.DataFrame) -> dict[str, int]:
    """Fill null backfill fields from evidence and return coverage statistics."""
    required = set(KEYS) | {"made_po_bf", "is_playoffs_bf"}
    missing = required - set(evidence.columns)
    if missing:
        raise ValueError(f"evidence is missing columns: {sorted(missing)}")
    schema = _find_player_schema(con)
    q_player = f'"{schema}"."player_fantasy"'
    have = {row[0] for row in con.execute(f"DESCRIBE {q_player}").fetchall()}
    for column in ("made_po_bf", "is_playoffs_bf"):
        if column not in have:
            con.execute(f"ALTER TABLE {q_player} ADD COLUMN {column} TINYINT")
    if evidence.empty:
        return {"evidence_rows": 0, "matched_rows": 0, "filled_rows": 0}

    has_kind = "evidence_kind" in evidence.columns
    has_champion = "champion_bf" in evidence.columns and "champion" in have
    columns = list(required) + (["evidence_kind"] if has_kind else []) + (["champion_bf"] if "champion_bf" in evidence.columns else [])
    frame = evidence.loc[:, columns].copy()
    frame["year"] = frame["year"].astype(int)
    frame["week"] = frame["week"].astype(int)
    frame["made_po_bf"] = frame["made_po_bf"].astype(int).clip(0, 1)
    frame["is_playoffs_bf"] = frame["is_playoffs_bf"].astype(int).clip(0, 1)
    con.register("_playoff_evidence_frame", frame)
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _playoff_evidence AS
            SELECT db_name, year, week, NFL_player_id,
                   MAX(made_po_bf) AS made_po_bf,
                   MAX(is_playoffs_bf) AS is_playoffs_bf
                   {", MAX(evidence_kind) AS evidence_kind" if has_kind else ""}
                   {", MAX(champion_bf) AS champion_bf" if "champion_bf" in evidence.columns else ""}
            FROM _playoff_evidence_frame
            GROUP BY 1, 2, 3, 4
            """
        )
        evidence_rows = con.execute("SELECT COUNT(*) FROM _playoff_evidence").fetchone()[0]
        matched_rows = con.execute(
            f"""
            SELECT COUNT(*) FROM {q_player} p JOIN _playoff_evidence e
              USING (db_name, year, week, NFL_player_id)
            {"WHERE e.evidence_kind <> 'unresolved'" if has_kind else ""}
            """
        ).fetchone()[0]
        filled_rows = con.execute(
            f"""
            SELECT COUNT(*) FROM {q_player} p JOIN _playoff_evidence e
              USING (db_name, year, week, NFL_player_id)
            WHERE {"e.evidence_kind <> 'unresolved' AND " if has_kind else ""}
                  (p.made_po_bf IS NULL OR p.is_playoffs_bf IS NULL)
            """
        ).fetchone()[0]
        champion_filled_rows = 0
        if has_champion:
            champion_filled_rows = con.execute(
                f"""
                SELECT COUNT(*) FROM {q_player} p JOIN _playoff_evidence e
                  USING (db_name, year, week, NFL_player_id)
                WHERE e.champion_bf = 1 AND p.champion IS NULL
                """
            ).fetchone()[0]
        con.execute(
            f"""
            UPDATE {q_player} AS p
            SET made_po_bf = COALESCE(p.made_po_bf, e.made_po_bf),
                is_playoffs_bf = COALESCE(p.is_playoffs_bf, e.is_playoffs_bf)
                {", champion = COALESCE(p.champion, CASE WHEN e.champion_bf = 1 THEN 1 END)" if has_champion else ""}
            FROM _playoff_evidence e
            WHERE p.db_name = e.db_name AND p.year = e.year
              AND p.week = e.week AND p.NFL_player_id = e.NFL_player_id
              {"AND e.evidence_kind <> 'unresolved'" if has_kind else ""}
            """
        )
    finally:
        con.unregister("_playoff_evidence_frame")
    return {
        "evidence_rows": int(evidence_rows),
        "matched_rows": int(matched_rows),
        "filled_rows": int(filled_rows),
        "champion_filled_rows": int(champion_filled_rows),
    }


def _find_player_schema(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.execute(
        """
        SELECT table_schema FROM information_schema.tables
        WHERE table_name = 'player_fantasy'
        ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END
        LIMIT 1
        """
    ).fetchall()
    if not rows:
        raise ValueError("expected player_fantasy table")
    return rows[0][0]
