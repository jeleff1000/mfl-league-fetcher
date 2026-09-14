"""
Fetch: download tiles and OCR JSON for CANDIDATE sources.

Each source gets:
  - A small-res tile (6.25%) for visual spot-check
  - A medium-res tile (25%) for OCR quality assessment
  - The LOC JSON OCR text

PFA/PFR HTML sources get the raw HTML saved.
LOCAL_FILE sources are skipped (already on disk).

All downloads are idempotent — file already present → skip the HTTP call.
"""

from __future__ import annotations
import hashlib
import json
import socket
import time
import urllib.request
import urllib.error
from pathlib import Path

# Reliable timeout on Windows — urllib timeout param alone is not sufficient
socket.setdefaulttimeout(15)

import duckdb

from .config import (
    TILES_DIR, OCR_DIR,
    LOC_MIN_INTERVAL_S, LOC_TIMEOUT_S, LOC_MAX_RETRIES,
)
from .schema import open_db, set_source_fetch_state

_last_req: float = 0.0


def _get_bytes(url: str) -> tuple[bytes | None, int]:
    """Rate-limited GET; returns (body, http_status)."""
    global _last_req
    wait = LOC_MIN_INTERVAL_S - (time.time() - _last_req)
    if wait > 0:
        time.sleep(wait)
    for attempt in range(LOC_MAX_RETRIES):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": (
                    "NFLHistoricalResearch/1.0 "
                    "(academic; contact joeyeleff@gmail.com)"
                )},
            )
            with urllib.request.urlopen(req, timeout=LOC_TIMEOUT_S) as r:
                _last_req = time.time()
                return r.read(), r.status
        except urllib.error.HTTPError as e:
            _last_req = time.time()
            return None, e.code
        except Exception as e:
            wait_s = 2 ** attempt
            print(f"      [retry {attempt+1}/{LOC_MAX_RETRIES}] {type(e).__name__}: {e}  wait={wait_s}s")
            time.sleep(wait_s)
    return None, 0


def _safe_path(directory: Path, source_key: str, suffix: str) -> Path:
    """Derive a safe local file path from a source_key."""
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{source_key}{suffix}"


# ── Tile fetcher ──────────────────────────────────────────────────────────────

def fetch_loc_tile(
    conn: duckdb.DuckDBPyConnection, source_key: str, row: dict
) -> bool:
    """
    Download small + medium tiles and JSON OCR for a LOC_TILE source.
    Returns True if all downloads succeeded (or were already present).
    """
    tile_sm  = row.get("tile_url_sm",  "")
    tile_md  = row.get("tile_url_md",  "") or (
        tile_sm.replace("pct:6.25", "pct:25") if tile_sm else ""
    )
    json_url = row.get("json_url", "")

    sm_path  = _safe_path(TILES_DIR, source_key, "_sm.jpg")
    md_path  = _safe_path(TILES_DIR, source_key, "_md.jpg")
    ocr_path = _safe_path(OCR_DIR,   source_key, "_ocr.json")

    ok_sm = ok_md = ok_ocr = True

    if tile_sm and not sm_path.exists():
        body, status = _get_bytes(tile_sm)
        if body and status in (200, 0):
            sm_path.write_bytes(body)
        else:
            ok_sm = False
            print(f"      [warn] tile_sm HTTP {status}: {tile_sm[:80]}")

    if tile_md and not md_path.exists():
        body, status = _get_bytes(tile_md)
        if body and status in (200, 0):
            md_path.write_bytes(body)
        else:
            ok_md = False

    if json_url and not ocr_path.exists():
        # json_url is the tile.loc.gov word-coordinates URL (not Cloudflare-blocked)
        # It returns JSON: { "/service/ndnp/...": {"full_text": "..."} }
        body, status = _get_bytes(json_url)
        if body and status in (200, 0):
            # Verify it's valid JSON before saving
            try:
                import json as _json
                _json.loads(body)
                ocr_path.write_bytes(body)
            except Exception:
                ok_ocr = False
        else:
            ok_ocr = False
            print(f"      [warn] OCR fetch HTTP {status}: {json_url[:80]}")

    all_ok = ok_sm and ok_ocr  # md is nice-to-have; sm + ocr are required
    conn.execute("""
        UPDATE source_ledger
        SET local_tile_sm=?, local_tile_md=?, local_json=?,
            fetch_state=?, fetch_http_status=200,
            fetch_at=CURRENT_TIMESTAMP
        WHERE source_key=?
    """, [
        str(sm_path)  if sm_path.exists()  else None,
        str(md_path)  if md_path.exists()  else None,
        str(ocr_path) if ocr_path.exists() else None,
        "FETCHED" if all_ok else "FETCH_FAILED",
        source_key,
    ])
    return all_ok


# ── HTML fetcher (PFA / PFR) ──────────────────────────────────────────────────

def fetch_html_source(
    conn: duckdb.DuckDBPyConnection, source_key: str, row: dict
) -> bool:
    url = row.get("page_url", "")
    if not url:
        set_source_fetch_state(conn, source_key, "FETCH_FAILED", error="no URL")
        return False

    html_path = _safe_path(TILES_DIR, source_key, ".html")
    if html_path.exists():
        conn.execute(
            "UPDATE source_ledger SET local_tile_sm=?, fetch_state='FETCHED', "
            "fetch_at=CURRENT_TIMESTAMP WHERE source_key=?",
            [str(html_path), source_key]
        )
        return True

    body, status = _get_bytes(url)
    if body and status in (200, 0):
        html_path.write_bytes(body)
        conn.execute(
            "UPDATE source_ledger SET local_tile_sm=?, fetch_state='FETCHED', "
            "fetch_http_status=?, fetch_at=CURRENT_TIMESTAMP WHERE source_key=?",
            [str(html_path), status, source_key]
        )
        return True
    else:
        conn.execute(
            "UPDATE source_ledger SET fetch_state='FETCH_FAILED', "
            "fetch_http_status=?, fetch_at=CURRENT_TIMESTAMP WHERE source_key=?",
            [status, source_key]
        )
        return False


# ── Batch fetcher ─────────────────────────────────────────────────────────────

def fetch_batch(
    conn: duckdb.DuckDBPyConnection,
    tier: str | None = None,
    limit: int = 100,
) -> dict[str, bool]:
    """
    Fetch up to `limit` CANDIDATE sources.  Returns {source_key: success}.
    """
    tier_clause = (
        f"AND gm.tier='{tier}'" if tier else ""
    )
    rows = conn.execute(f"""
        SELECT sl.source_key, sl.source_type,
               sl.tile_url_sm, sl.tile_url_md, sl.json_url, sl.page_url,
               sl.local_tile_sm, sl.local_json
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key = sl.game_key
        WHERE sl.fetch_state = 'CANDIDATE' {tier_clause}
        ORDER BY gm.tier, gm.year, gm.week
        LIMIT {limit}
    """).fetchall()

    results = {}
    for r in rows:
        sk, stype, tile_sm, tile_md, json_url, page_url, ltile, ljson = r
        row_dict = dict(
            tile_url_sm=tile_sm, tile_url_md=tile_md,
            json_url=json_url, page_url=page_url,
        )
        print(f"  fetch [{stype}] {sk}")
        if stype == "LOC_TILE":
            ok = fetch_loc_tile(conn, sk, row_dict)
        elif stype in ("PFA_HTML", "PFA_INDEX", "PFR_BOXSCORE"):
            ok = fetch_html_source(conn, sk, row_dict)
        elif stype == "LOCAL_FILE":
            # Already on disk — just mark fetched
            conn.execute(
                "UPDATE source_ledger SET fetch_state='FETCHED', "
                "fetch_at=CURRENT_TIMESTAMP WHERE source_key=?", [sk]
            )
            ok = True
        else:
            ok = False
        results[sk] = ok
        status = "OK" if ok else "FAIL"
        print(f"    -> {status}")

    ok_count = sum(results.values())
    print(f"Fetched {len(results)} sources: {ok_count} OK, {len(results)-ok_count} failed")
    return results
