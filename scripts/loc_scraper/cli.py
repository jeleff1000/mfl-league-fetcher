"""
CLI runner for the LOC scraper pipeline.

Commands:
    manifest   — Build game queue from v26
    discover   — Run search strategies to find newspaper sources
    fetch      — Download tiles, OCR JSON, and HTML from CANDIDATE sources
    assess     — Score OCR quality and extract stats
    promote    — Cross-validate and promote to promotions table
    audit      — Spot-check and close promoted games
    status     — Print pipeline health summary
    classify   — Show easy/medium/hard game breakdown
    full-run   — discover + fetch + assess + promote + audit in one shot

Options:
    --tier T0..T4   — restrict to one tier (default: all)
    --limit N       — max items to process per stage
    --reset         — wipe and rebuild manifest (manifest only)
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path

from .schema import open_db, game_status_summary
from .manifest import build_manifest, print_summary


# ── Status helpers ─────────────────────────────────────────────────────────────

def cmd_status(conn, args) -> None:
    print_summary(conn)

    # Source ledger breakdown
    rows = conn.execute("""
        SELECT sl.fetch_state, sl.assess_state, COUNT(*)
        FROM source_ledger sl
        GROUP BY sl.fetch_state, sl.assess_state
        ORDER BY sl.fetch_state, sl.assess_state
    """).fetchall()
    print("\nSource ledger:")
    for fs, as_, cnt in rows:
        print(f"  fetch={fs:15s}  assess={as_:12s}  n={cnt:,}")

    # Promotion summary
    promos = conn.execute("""
        SELECT confidence_level, audit_state, COUNT(*)
        FROM promotions GROUP BY confidence_level, audit_state
        ORDER BY confidence_level, audit_state
    """).fetchall()
    if promos:
        print("\nPromotions:")
        for conf, aud, cnt in promos:
            print(f"  {conf:10s}  audit={aud:15s}  n={cnt:,}")


def cmd_classify(conn, args) -> None:
    """
    Classify incomplete games into:
      EASY   — game_date known, home city paper exists in CITY_PAPERS, T0-T2
      MEDIUM — national paper likely exists, T2-T3
      HARD   — pre-1932 or obscure market, likely T0 small-city
    Also shows which games already have useful sources vs. still blank.
    """
    from .config import FRANCHISE_CITY, CITY_PAPERS, FRANCHISE_NAMES

    rows = conn.execute("""
        SELECT game_key, year, week, tier, franchise_a, franchise_b,
               team_a, team_b, game_date, missing_csv,
               sources_useful, state
        FROM game_manifest
        WHERE state NOT IN ('CLOSED','CLOSED_NEGATIVE','SKIP')
        ORDER BY tier, year, week
    """).fetchall()

    buckets = {"EASY": [], "MEDIUM": [], "HARD": []}
    for r in rows:
        (gk, year, week, tier, fa, fb, ta, tb, gd, miss, useful, state) = r
        city_a = FRANCHISE_CITY.get(fa)
        city_b = FRANCHISE_CITY.get(fb)
        has_city_paper = (
            (city_a and city_a in CITY_PAPERS) or
            (city_b and city_b in CITY_PAPERS)
        )
        if useful and useful > 0:
            bucket = "EASY"
        elif tier in ("T3", "T4") or (tier == "T2" and has_city_paper):
            bucket = "EASY"
        elif tier in ("T1", "T2"):
            bucket = "MEDIUM" if has_city_paper else "HARD"
        else:  # T0
            bucket = "HARD"
        buckets[bucket].append((tier, year, week, ta, tb, miss, state))

    for label, items in buckets.items():
        print(f"\n{label}: {len(items)} games")
        for tier, year, week, ta, tb, miss, state in items[:10]:
            print(f"  {tier}  {year} w{week:02d}  {ta or '?'} vs {tb or '?'}  "
                  f"missing={miss}  state={state}")
        if len(items) > 10:
            print(f"  ... and {len(items)-10} more")


# ── Stage commands ─────────────────────────────────────────────────────────────

def cmd_manifest(conn, args) -> None:
    build_manifest(conn, reset=args.reset, tier_filter=args.tier)
    print_summary(conn)


def cmd_discover(conn, args) -> None:
    from .discover import discover_batch
    discover_batch(conn, tier=args.tier, limit=args.limit)
    cmd_status(conn, args)


def cmd_fetch(conn, args) -> None:
    from .fetch import fetch_batch
    fetch_batch(conn, tier=args.tier, limit=args.limit)


def cmd_assess(conn, args) -> None:
    from .assess import assess_batch
    assess_batch(conn, tier=args.tier, limit=args.limit)


def cmd_promote(conn, args) -> None:
    from .promote import promote_batch, promotion_summary
    promote_batch(conn, tier=args.tier, limit=args.limit)
    promotion_summary(conn)


def cmd_audit(conn, args) -> None:
    from .promote import audit_batch, promotion_summary
    audit_batch(conn, tier=args.tier, limit=args.limit)
    promotion_summary(conn)


def cmd_bulk_discover(conn, args) -> None:
    """
    Fan-out mode: run ALL search levels for ALL tiers up to budget.
    Goal: get every possible candidate URL into source_ledger before fetching.
    Prioritizes T0 → T1 → T2 → T3.
    """
    from .discover import discover_batch
    from .config import TIER_DEFS

    tiers = ["T0", "T1", "T2", "T3"] if not args.tier else [args.tier]
    for tier in tiers:
        tier_budget = TIER_DEFS[tier]["budget"]
        print(f"\n── {tier} discovery (budget={tier_budget}) ──")
        # Run until no more NEW/LEVEL_UP games in this tier
        for _ in range(tier_budget + 2):
            pending = conn.execute("""
                SELECT COUNT(*) FROM game_manifest
                WHERE tier=? AND state IN ('NEW','LEVEL_UP')
            """, [tier]).fetchone()[0]
            if pending == 0:
                print(f"  {tier}: no more games to discover")
                break
            discover_batch(conn, tier=tier, limit=args.limit)

    print("\nBulk discovery complete. Run 'fetch --limit N' to download everything.")


def cmd_inspect(conn, args) -> None:
    """
    Dump OCR text and extracted stats for a single game_key.
    Use this to diagnose bad OCR or ambiguous extractions.
    """
    gk = args.game_key
    if not gk:
        print("ERROR: --game-key required")
        return

    gm = conn.execute("""
        SELECT year, week, tier, team_a, team_b, game_date, missing_csv,
               state, sources_found, sources_fetched, sources_assessed, sources_useful
        FROM game_manifest WHERE game_key=?
    """, [gk]).fetchone()
    if not gm:
        print(f"Game not found: {gk}")
        return

    (year, week, tier, ta, tb, gdate, miss, state, sf, sfe, sa, su) = gm
    print(f"\n{'='*60}")
    print(f"Game: {gk}")
    print(f"  {tier}  {year} w{week:02d}  {ta} vs {tb}  date={gdate}")
    print(f"  state={state}  missing={miss}")
    print(f"  sources: found={sf} fetched={sfe} assessed={sa} useful={su}")

    sources = conn.execute("""
        SELECT source_key, source_type, newspaper_lccn, publication_date,
               fetch_state, assess_state, ocr_quality_tier,
               ocr_word_count, football_kw_count, has_score_line, contains_game_ref,
               local_json
        FROM source_ledger WHERE game_key=?
        ORDER BY ocr_quality_tier NULLS LAST, fetch_state
    """, [gk]).fetchall()

    for s in sources:
        (sk, stype, lccn, pdate, fstate, astate, qtier,
         wc, kw, score, ref, ljson) = s
        print(f"\n  source: {sk}")
        print(f"    type={stype}  lccn={lccn}  date={pdate}")
        print(f"    fetch={fstate}  assess={astate}  tier={qtier}  words={wc}  kw={kw}")
        print(f"    has_score={score}  contains_game_ref={ref}")
        if ljson and Path(ljson).exists() and args.show_ocr:
            import json as _json
            try:
                data = _json.loads(Path(ljson).read_text(encoding="utf-8", errors="replace"))
                res = data.get("resource", [{}])
                text = res[0].get("text") or res[0].get("ocr_text") or "" if res else ""
                print(f"    OCR text (first 800 chars):\n    {text[:800]}")
            except Exception as e:
                print(f"    [error reading OCR: {e}]")

    extracts = conn.execute("""
        SELECT stat_column, value, confidence, team_side, raw_snippet
        FROM stat_extracts WHERE game_key=?
        ORDER BY stat_column, team_side
    """, [gk]).fetchall()
    if extracts:
        print(f"\n  Extracts ({len(extracts)}):")
        for sc, val, conf, side, snip in extracts:
            print(f"    {sc:30s}  {val:8.1f}  {conf:6s}  side={side}")
            if snip:
                print(f"      ↳ {snip[:100]}")


def cmd_paper_stats(conn, args) -> None:
    """
    Show which newspapers produce good vs bad OCR.
    Use this to prioritize or deprioritize sources in future runs.
    """
    rows = conn.execute("""
        SELECT newspaper_lccn,
               newspaper_name,
               COUNT(*) AS total,
               SUM(CASE WHEN ocr_quality_tier='A' THEN 1 ELSE 0 END) AS tier_a,
               SUM(CASE WHEN ocr_quality_tier='B' THEN 1 ELSE 0 END) AS tier_b,
               SUM(CASE WHEN ocr_quality_tier='C' THEN 1 ELSE 0 END) AS tier_c,
               SUM(CASE WHEN ocr_quality_tier='D' THEN 1 ELSE 0 END) AS tier_d,
               SUM(CASE WHEN ocr_quality_tier IS NULL THEN 1 ELSE 0 END) AS unassessed
        FROM source_ledger
        WHERE newspaper_lccn IS NOT NULL
        GROUP BY newspaper_lccn, newspaper_name
        HAVING total > 0
        ORDER BY (tier_a + tier_b) DESC, total DESC
    """).fetchall()

    if not rows:
        print("No assessed LOC sources yet.")
        return

    print(f"\nNewspaper quality summary ({len(rows)} papers):")
    print(f"{'LCCN':15s}  {'Name':30s}  {'Total':6s}  A    B    C    D   Pend")
    print("-" * 80)
    for (lccn, name, total, a, b, c, d, pend) in rows:
        name_s = (name or "")[:28]
        pct_good = int(100 * (a + b) / max(total, 1))
        flag = "✓" if pct_good >= 50 else ("~" if pct_good >= 20 else "✗")
        print(f"{lccn or '':15s}  {name_s:30s}  {total:5d}  "
              f"{a:3d}  {b:3d}  {c:3d}  {d:3d}  {pend:4d}  {flag} {pct_good}%")


def cmd_coverage(conn, args) -> None:
    """
    Show stat family coverage progress across tiers.
    Answers: which stat types are we actually recovering and at what rate?
    """
    rows = conn.execute("""
        SELECT gm.tier, se.stat_family,
               COUNT(DISTINCT se.game_key) AS games_with_data,
               COUNT(*) AS total_extracts,
               SUM(CASE WHEN se.confidence='HIGH' THEN 1 ELSE 0 END) AS high,
               SUM(CASE WHEN se.confidence='MEDIUM' THEN 1 ELSE 0 END) AS med,
               SUM(CASE WHEN se.confidence='LOW' THEN 1 ELSE 0 END) AS low
        FROM stat_extracts se
        JOIN game_manifest gm ON gm.game_key=se.game_key
        GROUP BY gm.tier, se.stat_family
        ORDER BY gm.tier, se.stat_family
    """).fetchall()

    if not rows:
        print("No stat extracts yet.")
        return

    print(f"\nStat extraction coverage:")
    print(f"{'Tier':5s}  {'Family':15s}  {'Games':6s}  {'Total':6s}  H    M    L")
    print("-" * 65)
    for (tier, fam, games, total, h, m, lo) in rows:
        print(f"{tier:5s}  {fam:15s}  {games:6d}  {total:6d}  {h:3d}  {m:3d}  {lo:3d}")


def cmd_bulk_fetch(conn, args) -> None:
    """
    Download ALL CANDIDATE sources across all tiers.
    With D: drive space available this should all be stored locally.
    """
    from .fetch import fetch_batch
    fetched = 0
    while True:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM source_ledger WHERE fetch_state='CANDIDATE'"
        ).fetchone()[0]
        if remaining == 0:
            break
        batch_size = min(args.limit, remaining)
        results = fetch_batch(conn, tier=args.tier, limit=batch_size)
        fetched += len(results)
        print(f"  Progress: {fetched} fetched so far, {remaining - batch_size} remaining")
        if len(results) < batch_size:
            break
    print(f"\nBulk fetch done: {fetched} total sources downloaded")


def cmd_full_run(conn, args) -> None:
    """discover + fetch + assess + promote + audit in sequence."""
    from .discover import discover_batch
    from .fetch import fetch_batch
    from .assess import assess_batch
    from .promote import promote_batch, audit_batch, promotion_summary

    print("=== DISCOVER ===")
    discover_batch(conn, tier=args.tier, limit=args.limit)
    print("=== FETCH ===")
    fetch_batch(conn, tier=args.tier, limit=args.limit)
    print("=== ASSESS ===")
    assess_batch(conn, tier=args.tier, limit=args.limit)
    print("=== PROMOTE ===")
    promote_batch(conn, tier=args.tier, limit=args.limit)
    print("=== AUDIT ===")
    audit_batch(conn, tier=args.tier, limit=args.limit)
    print("=== SUMMARY ===")
    promotion_summary(conn)
    cmd_status(conn, args)


# ── Parser ────────────────────────────────────────────────────────────────────

COMMANDS = {
    "manifest":       cmd_manifest,
    "discover":       cmd_discover,
    "fetch":          cmd_fetch,
    "assess":         cmd_assess,
    "promote":        cmd_promote,
    "audit":          cmd_audit,
    "status":         cmd_status,
    "classify":       cmd_classify,
    "inspect":        cmd_inspect,
    "paper-stats":    cmd_paper_stats,
    "coverage":       cmd_coverage,
    "bulk-discover":  cmd_bulk_discover,
    "bulk-fetch":     cmd_bulk_fetch,
    "full-run":       cmd_full_run,
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m loc_scraper",
        description="LOC newspaper scraper for NFL historical stats",
    )
    parser.add_argument("command", choices=list(COMMANDS))
    parser.add_argument("--tier",   default=None,
                        help="Restrict to tier T0..T4")
    parser.add_argument("--limit",  type=int, default=200,
                        help="Max items per batch (default 200)")
    parser.add_argument("--reset",  action="store_true",
                        help="Wipe manifest before rebuilding (manifest only)")
    parser.add_argument("--game-key", default=None,
                        help="Game key for inspect command")
    parser.add_argument("--show-ocr", action="store_true",
                        help="Show raw OCR text in inspect output")
    args = parser.parse_args(argv)

    conn = open_db()
    fn = COMMANDS[args.command]
    fn(conn, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
