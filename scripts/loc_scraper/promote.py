"""
Promote: cross-validate extracted stats and commit VERIFIED values.

Logic:
  1. For each ASSESSED game, group stat_extracts by (stat_column, team_side).
  2. If 2+ INDEPENDENT sources agree within tolerance → VERIFIED.
  3. If 1 source, A-tier OCR → HIGH confidence.
  4. If 1 source, B/C-tier → MEDIUM / LOW.
  5. Write accepted values to promotions table.
  6. Games with ≥1 stat promoted move to AUDITING; others stay ASSESSED or go LEVEL_UP.
  7. AUDITING games get a spot-check pass (are the values plausible?), then CLOSED.
"""

from __future__ import annotations
import hashlib
import json
from typing import Any

import duckdb

from .schema import open_db, set_game_state


# ── Agreement tolerance ───────────────────────────────────────────────────────

# Two sources "agree" if they're within AGREE_PCT of each other OR within AGREE_ABS
AGREE_PCT = 0.05   # 5%
AGREE_ABS = 2.0    # ±2 yards / ±2 points always counts as agreement

# Plausibility bounds per stat column (min, max per player per game)
PLAUSIBILITY: dict[str, tuple[float, float]] = {
    "passing_yards":    (0, 600),
    "passing_tds":      (0, 9),
    "passing_attempts": (0, 70),
    "passing_completions": (0, 70),
    "rushing_yards":    (-20, 300),
    "rushing_attempts": (0, 50),
    "rushing_tds":      (0, 6),
    "receiving_yards":  (-20, 300),
    "receptions":       (0, 25),
    "receiving_tds":    (0, 6),
    "fg_att":           (0, 10),
    "fg_made":          (0, 10),
    "def_interceptions":  (0, 5),
    "def_tackles_solo":   (0, 25),
    "def_sacks":          (0, 7),
    "def_tackle_assists": (0, 25),
}


def _agrees(a: float, b: float) -> bool:
    if abs(a - b) <= AGREE_ABS:
        return True
    denom = max(abs(a), abs(b), 1.0)
    return abs(a - b) / denom <= AGREE_PCT


def _plausible(stat_col: str, value: float) -> bool:
    bounds = PLAUSIBILITY.get(stat_col)
    if bounds is None:
        return True  # unknown stat — allow
    return bounds[0] <= value <= bounds[1]


# ── Promotion helpers ─────────────────────────────────────────────────────────

def _promo_key(game_key: str, stat_col: str, team_side: str | None,
               player: str | None) -> str:
    raw = f"{game_key}|{stat_col}|{team_side or ''}|{player or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def _write_promotion(
    conn: duckdb.DuckDBPyConnection,
    game_key: str, year: int | None, week: int | None,
    nfl_franchise_num: int | None, nfl_team: str | None,
    player_name: str | None, stat_column: str, value: float,
    confidence_level: str, source_keys: list[str],
) -> bool:
    """Insert or skip-if-exists a promotion row."""
    pk = _promo_key(game_key, stat_column, nfl_team, player_name)
    exists = conn.execute(
        "SELECT value FROM promotions WHERE promo_key=?", [pk]
    ).fetchone()
    if exists:
        return False  # already promoted
    if not _plausible(stat_column, value):
        return False  # out of bounds — flag for manual review
    conn.execute("""
        INSERT INTO promotions
        (promo_key, game_key, year, week,
         nfl_franchise_num, nfl_team, player_name,
         stat_column, value,
         confidence_level, source_count, source_keys)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        pk, game_key, year, week,
        nfl_franchise_num, nfl_team, player_name,
        stat_column, value,
        confidence_level, len(source_keys), ",".join(source_keys),
    ])
    return True


# ── Per-game promotion ────────────────────────────────────────────────────────

def promote_game(conn: duckdb.DuckDBPyConnection, game_key: str) -> int:
    """
    Cross-validate extracts for one ASSESSED game.
    Returns count of new promotions written.
    """
    # Load game meta
    gm = conn.execute("""
        SELECT year, week, franchise_a, franchise_b, team_a, team_b
        FROM game_manifest WHERE game_key=?
    """, [game_key]).fetchone()
    if not gm:
        return 0
    year, week, fa, fb, ta, tb = gm

    # Load all assessed extracts for this game
    extracts = conn.execute("""
        SELECT se.extract_key, se.source_key, se.stat_column, se.value,
               se.confidence, se.team_side, se.nfl_team, se.player_name_raw,
               sl.ocr_quality_tier
        FROM stat_extracts se
        JOIN source_ledger sl ON sl.source_key = se.source_key
        WHERE se.game_key=? AND sl.assess_state='ASSESSED'
          AND sl.ocr_quality_tier IN ('A','B','C')
    """, [game_key]).fetchall()

    if not extracts:
        return 0

    # Group by (stat_column, team_side, player_name_norm)
    groups: dict[tuple, list[dict]] = {}
    for row in extracts:
        ek, sk, sc, val, conf, side, team, player, qtier = row
        key = (sc, side or "", player or "")
        groups.setdefault(key, []).append(dict(
            source_key=sk, value=val, confidence=conf,
            ocr_tier=qtier, team=team,
        ))

    promoted = 0
    for (stat_col, side, player), candidates in groups.items():
        if not candidates:
            continue

        # Sort by quality (A > B > C) then confidence (HIGH > MEDIUM)
        tier_rank = {"A": 0, "B": 1, "C": 2}
        conf_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        candidates.sort(key=lambda c: (
            tier_rank.get(c["ocr_tier"], 9),
            conf_rank.get(c["confidence"], 9),
        ))

        best = candidates[0]
        agreers = [c for c in candidates[1:] if _agrees(best["value"], c["value"])]
        all_sources = [best["source_key"]] + [c["source_key"] for c in agreers]

        if len(agreers) >= 1:
            confidence_level = "VERIFIED"
        elif best["ocr_tier"] == "A":
            confidence_level = "HIGH"
        elif best["ocr_tier"] == "B":
            confidence_level = "MEDIUM"
        else:
            confidence_level = "LOW"

        # Determine franchise number from side
        fnum = fa if side == "A" else (fb if side == "B" else None)
        team = best.get("team") or (ta if side == "A" else tb)

        ok = _write_promotion(
            conn, game_key, year, week,
            fnum, team, player or None,
            stat_col, best["value"],
            confidence_level, all_sources,
        )
        if ok:
            promoted += 1

    if promoted > 0:
        conn.execute("""
            UPDATE game_manifest
            SET stats_promoted = stats_promoted + ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE game_key=?
        """, [promoted, game_key])
        set_game_state(conn, game_key, "AUDITING")
    else:
        # No promotable stats — level up or close negative
        _handle_no_promotions(conn, game_key)

    return promoted


def _handle_no_promotions(conn: duckdb.DuckDBPyConnection, game_key: str) -> None:
    """Decide whether to level-up or close as negative when no stats were promotable."""
    gm = conn.execute(
        "SELECT tier, current_level, state FROM game_manifest WHERE game_key=?",
        [game_key]
    ).fetchone()
    if not gm:
        return
    tier, current_level, state = gm
    if state not in ("ASSESSED",):
        return  # already handled

    from .config import SEARCH_LEVELS, source_budget
    levels = SEARCH_LEVELS.get(tier, [])
    budget = source_budget(tier)
    next_level = current_level + 1

    if next_level < len(levels):
        conn.execute("""
            UPDATE game_manifest SET current_level=?, last_updated=CURRENT_TIMESTAMP
            WHERE game_key=?
        """, [next_level, game_key])
        set_game_state(conn, game_key, "LEVEL_UP")
    else:
        set_game_state(conn, game_key, "CLOSED_NEGATIVE")


# ── Audit pass ────────────────────────────────────────────────────────────────

def audit_game(conn: duckdb.DuckDBPyConnection, game_key: str) -> str:
    """
    Spot-check promotions for a single AUDITING game.
    Returns final state: 'CLOSED' or 'MANUAL_REVIEW'.
    """
    promos = conn.execute("""
        SELECT promo_key, stat_column, value, confidence_level
        FROM promotions WHERE game_key=? AND audit_state='PENDING'
    """, [game_key]).fetchall()

    all_ok = True
    for pk, sc, val, conf in promos:
        if not _plausible(sc, val):
            conn.execute("""
                UPDATE promotions SET audit_state='FAILED',
                audit_note='out of plausibility bounds',
                audited_at=CURRENT_TIMESTAMP WHERE promo_key=?
            """, [pk])
            all_ok = False
        else:
            conn.execute("""
                UPDATE promotions SET audit_state='PASSED',
                audited_at=CURRENT_TIMESTAMP WHERE promo_key=?
            """, [pk])

    new_state = "CLOSED" if all_ok else "MANUAL_REVIEW"
    set_game_state(conn, game_key, new_state)
    return new_state


# ── Batch promotion ───────────────────────────────────────────────────────────

def promote_batch(
    conn: duckdb.DuckDBPyConnection,
    tier: str | None = None,
    limit: int = 500,
) -> dict[str, int]:
    """Promote extracts for up to `limit` ASSESSED games."""
    tier_clause = f"AND tier='{tier}'" if tier else ""
    rows = conn.execute(f"""
        SELECT game_key FROM game_manifest
        WHERE state='ASSESSED' {tier_clause}
        ORDER BY tier, year, week
        LIMIT {limit}
    """).fetchall()

    totals: dict[str, int] = {}
    for (gk,) in rows:
        n = promote_game(conn, gk)
        totals[gk] = n

    total_promoted = sum(totals.values())
    print(f"Promote pass: {len(totals)} games, {total_promoted} stats promoted")
    return totals


def audit_batch(
    conn: duckdb.DuckDBPyConnection,
    tier: str | None = None,
    limit: int = 500,
) -> dict[str, str]:
    """Audit AUDITING games; returns {game_key: final_state}."""
    tier_clause = f"AND tier='{tier}'" if tier else ""
    rows = conn.execute(f"""
        SELECT game_key FROM game_manifest
        WHERE state='AUDITING' {tier_clause}
        ORDER BY tier, year, week
        LIMIT {limit}
    """).fetchall()

    results = {}
    for (gk,) in rows:
        results[gk] = audit_game(conn, gk)

    closed   = sum(1 for s in results.values() if s == "CLOSED")
    manual   = sum(1 for s in results.values() if s == "MANUAL_REVIEW")
    print(f"Audit pass: {closed} CLOSED, {manual} MANUAL_REVIEW")
    return results


# ── Summary ───────────────────────────────────────────────────────────────────

def promotion_summary(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute("""
        SELECT confidence_level, stat_column, COUNT(*), AVG(value)
        FROM promotions WHERE audit_state='PASSED'
        GROUP BY confidence_level, stat_column
        ORDER BY confidence_level, stat_column
    """).fetchall()
    print(f"\nPromotion summary ({sum(r[2] for r in rows)} total passed):")
    for conf, col, cnt, avg in rows:
        print(f"  {conf:10s}  {col:30s}  n={cnt:4d}  avg={avg:.1f}")
