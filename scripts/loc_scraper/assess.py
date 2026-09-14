"""
Assess: score OCR quality and extract candidate stats from fetched sources.

Quality tiers:
  A — clean tabular OCR, football keywords present, score line found
  B — readable text, football keywords, no clear table but stats extractable
  C — partial OCR, some numbers legible, football context uncertain
  D — unreadable, wrong page, or clearly irrelevant

Each fetched source gets exactly one verdict — the line never blocks:
  ASSESSED    — stats extracted (or confirmed empty but football-relevant)
  IRRELEVANT  — wrong page, garbled beyond use
  NEEDS_IMAGE — OCR grade D but tile JPG exists → written to review_queue.csv
                for a human (or Claude image-read pass) to review manually

Stats written to stat_extracts table.
Image review queue: D:/...loc_scraper/review_queue.csv  (append-safe, idempotent).
"""

from __future__ import annotations
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import duckdb

from .config import (
    FOOTBALL_KEYWORDS, MIN_FOOTBALL_KEYWORDS,
    SCORE_RE, STAT_PATTERNS, SCRAPER_ROOT,
    get_search_terms, get_terms_for_team,
)
from .schema import open_db

REVIEW_CSV = SCRAPER_ROOT / "review_queue.csv"
_REVIEW_FIELDS = [
    "source_key", "game_key", "tier", "year", "week",
    "team_a", "team_b", "reason", "tile_path", "ocr_path", "page_url",
]

# Player-level stat patterns — name immediately before/after stat value
_PLAYER_YARDS_RE = re.compile(
    r"([A-Z][a-z]{2,15}(?:\s+[A-Z][a-z]{2,15})?)"
    r"[\s,]+(?:gained?|rushed?|passed?|carried?|caught?|ran?)?\s*"
    r"(\d{1,3})\s*(?:yard|yds?)",
    re.I,
)
_PLAYER_TD_RE = re.compile(
    r"([A-Z][a-z]{2,15}(?:\s+[A-Z][a-z]{2,15})?)"
    r"[\s,]+(?:scored?|plunged?|drove?|caught?|ran?|passed?)"
    r"[^.]{0,50}?touchdown",
    re.I,
)
_NOT_NAMES = frozenset([
    "The", "And", "But", "For", "With", "After", "When", "Then",
    "First", "Second", "Third", "Fourth", "Score", "Final", "Half",
    "Play", "Game", "Team", "Both", "Each", "This", "That",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "October", "November", "December",
])


# ── OCR text extraction ───────────────────────────────────────────────────────

def _reconstruct_from_coords(coords: dict) -> tuple[str, list[tuple]]:
    """
    Reconstruct page text from LOC word-coordinates dict.
    Returns (full_text, word_list) where word_list = [(line, col, word), ...].
    The word_list is used by _find_game_section() for spatial segmentation.
    """
    words: list[tuple] = []
    for word_text, occurrences in coords.items():
        for occ in occurrences:
            pos = occ.get("position", [0, 0])
            words.append((pos[0], pos[1], word_text))
    words.sort()
    lines: dict[int, list[tuple]] = {}
    for ln, col, word in words:
        lines.setdefault(ln, []).append((col, word))
    text_lines = [
        " ".join(w for _, w in sorted(lines[ln]))
        for ln in sorted(lines)
    ]
    return "\n".join(text_lines), words


def _load_ocr_json(path: str | None) -> tuple[str, dict]:
    """
    Load LOC OCR JSON; return (full_text, raw_dict).

    Handles three formats:
      1. Word-coordinates (our actual format):
           { "/service/ndnp/...": { "coords": { "word": [{position, coordinates}] } } }
         → reconstructed spatially via _reconstruct_from_coords()
      2. tile.loc.gov full_text format:
           { "/service/ndnp/...": { "full_text": "text..." } }
      3. Legacy chroniclingamerica.loc.gov:
           { "resource": [{ "text": "..." }] }
    """
    if not path or not Path(path).exists():
        return "", {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8", errors="replace"))
        text = ""

        if isinstance(data, dict):
            for key, val in data.items():
                if not isinstance(val, dict):
                    continue
                # Format 1: word-coordinates (primary — confirmed by inspection 2026-06-17)
                if "coords" in val:
                    text, _ = _reconstruct_from_coords(val["coords"])
                    break
                # Format 2: full_text present directly
                if "full_text" in val:
                    text = val["full_text"]
                    break

        # Format 3: legacy resource list
        if not text:
            res = data.get("resource", [{}])
            if res:
                text = res[0].get("text") or res[0].get("ocr_text") or ""

        return text, data
    except Exception:
        return "", {}


def _walk_strings(obj: Any):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_strings(v)


def _load_html(path: str | None) -> str:
    """Load HTML file and strip tags for text extraction."""
    if not path or not Path(path).exists():
        return ""
    raw = Path(path).read_text(encoding="utf-8", errors="replace")
    return re.sub(r"<[^>]+>", " ", raw)


def _find_game_section(
    text: str,
    raw_data: dict,
    terms_a: list[str],
    terms_b: list[str],
    context_lines: int = 60,
) -> str:
    """
    Use word-coordinate position data to spatially isolate the article on the
    page that covers our specific game — not the roundup scores, not the boxing
    column, not the college results.

    Strategy:
      1. From the coords dict, find the LINE NUMBERS where team names appear.
      2. Find the median hit-line — center of the densest team-name cluster.
      3. Return ±context_lines of reconstructed text around that center.

    Falls back to the full page text if coordinates are unavailable.
    """
    if not terms_a and not terms_b:
        return text

    all_terms = set(t.lower() for t in terms_a + terms_b)

    # Extract coords dict from raw_data
    coords: dict = {}
    if isinstance(raw_data, dict):
        for _key, val in raw_data.items():
            if isinstance(val, dict) and "coords" in val:
                coords = val["coords"]
                break

    if not coords:
        return text  # no coordinate data, use full page

    # Collect line numbers where a team name appears
    hit_lines: list[int] = []
    for word_text, occurrences in coords.items():
        if word_text.lower() in all_terms:
            for occ in occurrences:
                hit_lines.append(occ.get("position", [0, 0])[0])

    if not hit_lines:
        return text

    hit_lines.sort()
    center = hit_lines[len(hit_lines) // 2]

    # Slice the reconstructed text to that window
    lines = text.split("\n")
    lo = max(0, center - context_lines)
    hi = min(len(lines), center + context_lines)
    return "\n".join(lines[lo:hi])


def _write_review_queue(
    source_key: str, game_key: str, tier: str,
    year: int, week: int, team_a: str, team_b: str,
    reason: str, tile_path: str, ocr_path: str, page_url: str,
) -> None:
    """Append one row to review_queue.csv (idempotent: skip if source_key already present)."""
    existing_keys: set[str] = set()
    if REVIEW_CSV.exists():
        with open(REVIEW_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_keys.add(row.get("source_key", ""))
    if source_key in existing_keys:
        return
    new_file = not REVIEW_CSV.exists()
    REVIEW_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(REVIEW_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_REVIEW_FIELDS, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow({
            "source_key": source_key, "game_key": game_key,
            "tier": tier, "year": year, "week": week,
            "team_a": team_a, "team_b": team_b, "reason": reason,
            "tile_path": tile_path, "ocr_path": ocr_path, "page_url": page_url,
        })


# ── Quality scoring ───────────────────────────────────────────────────────────

def _ocr_quality_tier(text: str, terms_a: list[str], terms_b: list[str],
                       year: int | None = None) -> tuple[str, int, int, bool, bool]:
    """
    Returns: (tier, word_count, football_kw_count, has_score, contains_game)
    terms_a/terms_b are pre-computed via get_terms_for_team().
    """
    if not text or len(text) < 50:
        return "D", 0, 0, False, False

    words     = text.lower().split()
    word_count = len(words)

    kw_count   = sum(1 for kw in FOOTBALL_KEYWORDS if kw in text.lower())
    has_score  = bool(SCORE_RE.search(text))

    # Check if both team names are mentioned
    has_a    = any(t.lower() in text.lower() for t in terms_a) if terms_a else True
    has_b    = any(t.lower() in text.lower() for t in terms_b) if terms_b else True
    both_ref = has_a and has_b

    # Tier assignment
    if kw_count >= MIN_FOOTBALL_KEYWORDS and has_score and word_count >= 100:
        tier = "A" if both_ref else "B"
    elif kw_count >= MIN_FOOTBALL_KEYWORDS and word_count >= 50:
        tier = "B" if both_ref else "C"
    elif kw_count > 0 or word_count >= 30:
        tier = "C"
    else:
        tier = "D"

    return tier, word_count, kw_count, has_score, both_ref


# ── Stat extraction ───────────────────────────────────────────────────────────

def _extract_stats(text: str, game_key: str, source_key: str,
                   fran_a: int | None, fran_b: int | None,
                   team_a: str | None, team_b: str | None,
                   year: int) -> list[dict]:
    """
    Run regex patterns against section text.
    Returns list of dicts ready for stat_extracts table insertion.

    Two passes:
      Pass 1 — player-level (name + yards/touchdown): HIGH/MEDIUM confidence
      Pass 2 — config STAT_PATTERNS (no player name): LOW confidence
    """
    extracts = []
    seen_keys: set[str] = set()

    def _add(stat_col, value, snippet, player_name="", confidence="MEDIUM"):
        if value > 999:
            return
        team_side = _guess_team_side(snippet, fran_a, fran_b, year)
        raw_id = f"{source_key}:{stat_col}:{player_name}:{value}:{snippet[:30]}"
        ek = hashlib.sha256(raw_id.encode()).hexdigest()[:16]
        if ek in seen_keys:
            return
        seen_keys.add(ek)
        extracts.append(dict(
            extract_key=ek,
            source_key=source_key,
            game_key=game_key,
            team_side=team_side,
            nfl_team=team_a if team_side == "A" else team_b,
            player_name_raw=player_name,
            stat_family=_stat_family(stat_col),
            stat_column=stat_col,
            value=float(value),
            confidence=confidence,
            method="PATTERN",
            raw_snippet=snippet[:200],
        ))

    # Pass 1a: player + yardage  ("Bicks gained 8 yards", "Baugh passed 156 yards")
    for m in _PLAYER_YARDS_RE.finditer(text):
        name = m.group(1).strip().title()
        if name in _NOT_NAMES or len(name) < 4:
            continue
        yards = int(m.group(2))
        snippet = text[max(0, m.start()-40):m.end()+40].replace("\n", " ")
        ctx = snippet.lower()
        if any(kw in ctx for kw in ("pass", "threw", "aerial", "forward", "complet")):
            col = "passing_yards"
        elif any(kw in ctx for kw in ("receiv", "caught", "catch")):
            col = "receiving_yards"
        else:
            col = "rushing_yards"
        _add(col, yards, snippet, player_name=name, confidence="HIGH")

    # Pass 1b: player + touchdown  ("Bicks scored a touchdown", "Brown plunged over")
    for m in _PLAYER_TD_RE.finditer(text):
        name = m.group(1).strip().title()
        if name in _NOT_NAMES or len(name) < 4:
            continue
        snippet = text[max(0, m.start()-30):m.end()+30].replace("\n", " ")
        _add("touchdowns", 1, snippet, player_name=name, confidence="HIGH")

    # Pass 2: config STAT_PATTERNS (no player name, lower confidence)
    for stat_col, patterns in STAT_PATTERNS.items():
        for pattern in patterns:
            for m in pattern.finditer(text):
                try:
                    value = None
                    for g in m.groups():
                        if g and g.isdigit():
                            value = float(g)
                            break
                    if value is None:
                        continue
                    snippet = text[max(0, m.start()-60):m.end()+60].strip()
                    confidence = "MEDIUM" if len(m.group(0)) > 15 else "LOW"
                    _add(stat_col, value, snippet, confidence=confidence)
                except (ValueError, IndexError):
                    continue

    return extracts


def _guess_team_side(snippet: str, fran_a, fran_b, year) -> str | None:
    snl = snippet.lower()
    terms_a = get_search_terms(fran_a, year) if fran_a else []
    terms_b = get_search_terms(fran_b, year) if fran_b else []
    hit_a = any(t.lower() in snl for t in terms_a)
    hit_b = any(t.lower() in snl for t in terms_b)
    if hit_a and not hit_b: return "A"
    if hit_b and not hit_a: return "B"
    return None  # ambiguous


def _stat_family(stat_col: str) -> str:
    if "pass" in stat_col:   return "passing"
    if "rush" in stat_col:   return "rushing"
    if "receiv" in stat_col: return "receiving"
    if "fg" in stat_col or "pat" in stat_col or "kick" in stat_col: return "kicking"
    if "def_" in stat_col or "interc" in stat_col: return "idp"
    if "touchdown" in stat_col: return "scoring"
    return "misc"


# ── Assessment pipeline ───────────────────────────────────────────────────────

def assess_source(
    conn: duckdb.DuckDBPyConnection, source_key: str
) -> str:
    """
    Assess one FETCHED source.  Returns final assess_state ('ASSESSED' or 'IRRELEVANT').
    """
    row = conn.execute("""
        SELECT sl.source_type, sl.local_tile_sm, sl.local_json, sl.page_url,
               gm.franchise_a, gm.franchise_b, gm.team_a, gm.team_b,
               gm.year, gm.week, gm.game_key, gm.tier
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key = sl.game_key
        WHERE sl.source_key=?
    """, [source_key]).fetchone()
    if not row:
        return "IRRELEVANT"

    stype, ltile, ljson, page_url, fa, fb, ta, tb, year, week, game_key, game_tier = row
    # Use team codes (correct v26 mapping) — franchise IDs in game_manifest
    # use v26's PFR-derived numbering, not config.py's FRANCHISE_NAMES IDs.
    terms_a = get_terms_for_team(ta) or get_search_terms(fa or 0, year or 0)
    terms_b = get_terms_for_team(tb) or get_search_terms(fb or 0, year or 0)

    # Load text — and keep raw_data for spatial segmentation
    raw_data: dict = {}
    if stype == "LOC_TILE":
        text, raw_data = _load_ocr_json(ljson)
        # Spatially isolate the game-story section from the full sports page
        if text and raw_data:
            section = _find_game_section(text, raw_data, terms_a, terms_b)
        else:
            section = text
    elif stype in ("PFA_HTML", "PFA_INDEX", "PFR_BOXSCORE", "NA_HTML"):
        text = _load_html(ltile)
        section = text
    elif stype == "LOCAL_FILE":
        text = section = ""
    else:
        text = section = ""

    tier, wc, kw, has_score, both_ref = _ocr_quality_tier(section, terms_a, terms_b, year)

    if tier == "D":
        # If we have a tile image, flag for image review rather than silently discarding
        if ltile and Path(ltile).exists():
            conn.execute("""
                UPDATE source_ledger SET assess_state='NEEDS_IMAGE',
                ocr_quality_tier='D', ocr_word_count=?, football_kw_count=?,
                has_score_line=?, contains_game_ref=?, assess_at=CURRENT_TIMESTAMP
                WHERE source_key=?
            """, [wc, kw, has_score, both_ref, source_key])
            _write_review_queue(
                source_key=source_key, game_key=game_key,
                tier=game_tier, year=year or 0, week=week or 0,
                team_a=ta or "", team_b=tb or "",
                reason="ocr_grade_D",
                tile_path=ltile or "", ocr_path=ljson or "",
                page_url=page_url or "",
            )
            return "NEEDS_IMAGE"
        conn.execute("""
            UPDATE source_ledger SET assess_state='IRRELEVANT',
            ocr_quality_tier='D', ocr_word_count=?, football_kw_count=?,
            has_score_line=?, contains_game_ref=?, assess_at=CURRENT_TIMESTAMP
            WHERE source_key=?
        """, [wc, kw, has_score, both_ref, source_key])
        return "IRRELEVANT"

    # Extract stats from the spatially-isolated game section
    extracts = _extract_stats(section, game_key, source_key, fa, fb, ta, tb, year or 0)

    for ext in extracts:
        exists = conn.execute(
            "SELECT 1 FROM stat_extracts WHERE extract_key=?", [ext["extract_key"]]
        ).fetchone()
        if not exists:
            conn.execute("""
                INSERT INTO stat_extracts
                (extract_key, source_key, game_key,
                 team_side, nfl_team, player_name_raw,
                 stat_family, stat_column, value,
                 confidence, method, raw_snippet)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, [
                ext["extract_key"], ext["source_key"], ext["game_key"],
                ext.get("team_side"), ext.get("nfl_team"),
                ext.get("player_name_raw", ""),
                ext["stat_family"], ext["stat_column"], ext["value"],
                ext["confidence"], ext["method"], ext.get("raw_snippet"),
            ])

    conn.execute("""
        UPDATE source_ledger
        SET assess_state='ASSESSED', ocr_quality_tier=?,
            ocr_word_count=?, football_kw_count=?,
            has_score_line=?, contains_game_ref=?,
            extracted_json=?,
            assess_at=CURRENT_TIMESTAMP
        WHERE source_key=?
    """, [
        tier, wc, kw, has_score, both_ref,
        json.dumps([e["stat_column"] for e in extracts]),
        source_key,
    ])

    # Update game counters
    conn.execute("""
        UPDATE game_manifest
        SET sources_assessed = sources_assessed + 1,
            sources_useful   = sources_useful + ?,
            last_updated     = CURRENT_TIMESTAMP
        WHERE game_key = (SELECT game_key FROM source_ledger WHERE source_key=?)
    """, [1 if tier in ("A", "B") else 0, source_key])

    return "ASSESSED"


def assess_batch(
    conn: duckdb.DuckDBPyConnection,
    tier: str | None = None,
    limit: int = 200,
) -> dict[str, int]:
    """Assess up to `limit` FETCHED sources."""
    tier_clause = f"AND gm.tier='{tier}'" if tier else ""
    rows = conn.execute(f"""
        SELECT sl.source_key
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key = sl.game_key
        WHERE sl.fetch_state='FETCHED' AND sl.assess_state='PENDING'
        {tier_clause}
        ORDER BY gm.tier, gm.year, gm.week
        LIMIT {limit}
    """).fetchall()

    results = {}
    for (sk,) in rows:
        result = assess_source(conn, sk)
        results[sk] = result

    counts: dict[str, int] = {}
    for v in results.values():
        counts[v] = counts.get(v, 0) + 1
    print(f"Assessed {len(results)}: {counts}")
    return counts
