"""
DuckDB schema + state machine for the LOC scraper.

Tables:
  game_manifest   — one row per game (the work unit)
  source_ledger   — one row per newspaper page / URL candidate tried
  stat_extracts   — individual stat values found in a source
  promotions      — stats cross-validated and ready for super table insertion
  sessions        — log of scraping runs
"""

from __future__ import annotations
import duckdb
from pathlib import Path

# ── DDL ───────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """

CREATE TABLE IF NOT EXISTS game_manifest (
    game_key          VARCHAR PRIMARY KEY,   -- e.g. "1921_w04_REG_105_108" (franchise IDs)
    year              INTEGER  NOT NULL,
    week              INTEGER  NOT NULL,
    season_type       VARCHAR  NOT NULL DEFAULT 'REG',
    franchise_a       INTEGER,               -- lower franchise number
    franchise_b       INTEGER,               -- higher franchise number
    team_a            VARCHAR,               -- nfl_team code at time of game
    team_b            VARCHAR,
    game_date         DATE,

    tier              VARCHAR  NOT NULL,     -- T0..T4

    -- Coverage flags from v26 at manifest build time
    has_pass_a        BOOLEAN  DEFAULT FALSE,
    has_rush_a        BOOLEAN  DEFAULT FALSE,
    has_recv_a        BOOLEAN  DEFAULT FALSE,
    has_pass_b        BOOLEAN  DEFAULT FALSE,
    has_rush_b        BOOLEAN  DEFAULT FALSE,
    has_recv_b        BOOLEAN  DEFAULT FALSE,
    has_tackles       BOOLEAN  DEFAULT FALSE,
    missing_csv       VARCHAR,               -- CSV of missing stat families

    -- State machine (see GAME_TRANSITIONS below)
    state             VARCHAR  NOT NULL DEFAULT 'NEW',
    current_level     INTEGER  DEFAULT 0,    -- which search level we're on

    -- Running counters
    sources_found     INTEGER  DEFAULT 0,    -- candidates added to source_ledger
    sources_fetched   INTEGER  DEFAULT 0,
    sources_assessed  INTEGER  DEFAULT 0,
    sources_useful    INTEGER  DEFAULT 0,    -- quality tier A or B

    -- Resolution
    confidence_level  VARCHAR,               -- VERIFIED/HIGH/MEDIUM/LOW/NEGATIVE
    stats_promoted    INTEGER  DEFAULT 0,
    audit_notes       VARCHAR,

    -- Legacy queue pointers (if migrated from old system)
    old_queue_ids     VARCHAR,               -- CSV of prior search_task_ids

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_ledger (
    source_key        VARCHAR  PRIMARY KEY,  -- e.g. "sn86058226_1921-10-30_ed1_p11"
    game_key          VARCHAR  NOT NULL,
    source_type       VARCHAR  NOT NULL,     -- LOC_TILE / PFA_HTML / PFR_BOXSCORE / LOCAL_FILE
    search_level      INTEGER  NOT NULL DEFAULT 1,

    -- Bibliographic
    newspaper_lccn    VARCHAR,
    newspaper_name    VARCHAR,
    publication_date  DATE,
    page_seq          INTEGER,
    edition           INTEGER  DEFAULT 1,

    -- Search provenance
    search_query      VARCHAR,               -- query string that found this
    search_rank       INTEGER,               -- position in search results
    relevance_score   FLOAT,                 -- LOC API relevance score if provided

    -- URLs
    page_url          VARCHAR,               -- human-readable LOC viewer URL
    tile_url_sm       VARCHAR,               -- 6.25% tile (quick preview)
    tile_url_md       VARCHAR,               -- 25% tile (OCR quality)
    tile_url_lg       VARCHAR,               -- 100% tile (full resolution)
    json_url          VARCHAR,               -- LOC OCR JSON endpoint

    -- Local paths (populated after download)
    local_tile_sm     VARCHAR,
    local_tile_md     VARCHAR,
    local_json        VARCHAR,

    -- Fetch lifecycle
    fetch_state       VARCHAR  DEFAULT 'CANDIDATE',
    -- CANDIDATE → QUEUED → FETCHING → FETCHED → FETCH_FAILED
    fetch_http_status INTEGER,
    fetch_error       VARCHAR,
    fetch_at          TIMESTAMP,

    -- Quality assessment
    assess_state      VARCHAR  DEFAULT 'PENDING',
    -- PENDING → ASSESSING → ASSESSED / IRRELEVANT
    ocr_quality_tier  VARCHAR,               -- A / B / C / D
    ocr_word_count    INTEGER,
    football_kw_count INTEGER,
    has_score_line    BOOLEAN  DEFAULT FALSE,
    contains_game_ref BOOLEAN  DEFAULT FALSE, -- mentions both teams

    -- Extracted stats as JSON blob (before normalization)
    extracted_json    VARCHAR,
    assess_at         TIMESTAMP,

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS sl_game_key ON source_ledger (game_key);
CREATE INDEX IF NOT EXISTS sl_fetch_state ON source_ledger (fetch_state);

CREATE TABLE IF NOT EXISTS stat_extracts (
    extract_key       VARCHAR  PRIMARY KEY,  -- sha256 of (source_key+stat_column+player_name_raw)
    source_key        VARCHAR  NOT NULL,
    game_key          VARCHAR  NOT NULL,

    team_side         VARCHAR,               -- 'A' or 'B' (relative to game_manifest)
    nfl_team          VARCHAR,
    nfl_franchise_num INTEGER,
    player_name_raw   VARCHAR,               -- verbatim from OCR
    player_name_norm  VARCHAR,               -- fuzzy-matched name

    stat_family       VARCHAR,               -- passing/rushing/receiving/kicking/idp/team
    stat_column       VARCHAR,               -- e.g. passing_yards
    value             FLOAT,

    confidence        VARCHAR,               -- HIGH / MEDIUM / LOW
    method            VARCHAR,               -- PATTERN / STRUCTURED / MANUAL
    raw_snippet       VARCHAR,               -- the OCR text used for extraction

    reviewed          BOOLEAN  DEFAULT FALSE,
    review_note       VARCHAR,

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS se_game_key ON stat_extracts (game_key);

CREATE TABLE IF NOT EXISTS promotions (
    promo_key         VARCHAR  PRIMARY KEY,
    game_key          VARCHAR  NOT NULL,
    year              INTEGER,
    week              INTEGER,

    nfl_franchise_num INTEGER,
    nfl_team          VARCHAR,
    player_name       VARCHAR,

    stat_column       VARCHAR,
    value             FLOAT,

    -- Cross-validation
    confidence_level  VARCHAR,               -- VERIFIED / HIGH / MEDIUM / LOW
    source_count      INTEGER,               -- number of agreeing sources
    source_keys       VARCHAR,               -- CSV of source_key values

    -- Audit pipeline
    audit_state       VARCHAR  DEFAULT 'PENDING',
    -- PENDING → SPOT_CHECK → PASSED → FAILED → MANUAL_REVIEW
    audit_note        VARCHAR,
    audited_at        TIMESTAMP,

    -- Integration
    applied_to_super  BOOLEAN  DEFAULT FALSE,
    applied_at        TIMESTAMP,

    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id        VARCHAR  PRIMARY KEY,
    started_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at          TIMESTAMP,
    tier_filter       VARCHAR,
    level_filter      INTEGER,
    limit_games       INTEGER,
    games_attempted   INTEGER  DEFAULT 0,
    games_advanced    INTEGER  DEFAULT 0,   -- moved to next state
    games_resolved    INTEGER  DEFAULT 0,   -- CLOSED or CLOSED_NEGATIVE
    sources_fetched   INTEGER  DEFAULT 0,
    stats_extracted   INTEGER  DEFAULT 0,
    stats_promoted    INTEGER  DEFAULT 0,
    notes             VARCHAR
);

"""

# ── State machines ────────────────────────────────────────────────────────────

# game_manifest.state valid transitions
GAME_TRANSITIONS: dict[str, list[str]] = {
    "NEW":             ["DISCOVERING", "SKIP"],
    "DISCOVERING":     ["DISCOVERED", "LEVEL_UP", "CLOSED_NEGATIVE"],
    "DISCOVERED":      ["FETCHING", "LEVEL_UP"],  # LEVEL_UP when level added 0 candidates
    "FETCHING":        ["ASSESSING", "LEVEL_UP"],
    "ASSESSING":       ["ASSESSED"],
    "ASSESSED":        ["PROMOTING", "MANUAL_REVIEW", "LEVEL_UP", "CLOSED_NEGATIVE"],
    "PROMOTING":       ["AUDITING"],
    "AUDITING":        ["CLOSED", "MANUAL_REVIEW"],
    "LEVEL_UP":        ["DISCOVERING", "CLOSED_NEGATIVE"],  # exhausted this level
    "MANUAL_REVIEW":   ["PROMOTING", "CLOSED_NEGATIVE", "CLOSED"],
    "CLOSED":          [],
    "CLOSED_NEGATIVE": [],
    "SKIP":            [],
}

# source_ledger.fetch_state valid transitions
SOURCE_FETCH_TRANSITIONS: dict[str, list[str]] = {
    "CANDIDATE":   ["QUEUED", "IRRELEVANT"],
    "QUEUED":      ["FETCHING"],
    "FETCHING":    ["FETCHED", "FETCH_FAILED"],
    "FETCH_FAILED":["QUEUED"],  # can retry once
    "FETCHED":     ["ASSESSING"],
    "ASSESSING":   ["ASSESSED", "IRRELEVANT"],
    "ASSESSED":    [],
    "IRRELEVANT":  [],
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def open_db(path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open (or create) the scraper DuckDB and initialise schema."""
    from .config import SCRAPER_DB, TILES_DIR, OCR_DIR
    p = path or SCRAPER_DB
    p.parent.mkdir(parents=True, exist_ok=True)
    TILES_DIR.mkdir(parents=True, exist_ok=True)
    OCR_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(p))
    conn.execute("SET progress_bar_time=99999")
    conn.execute(SCHEMA_SQL)
    return conn


def set_game_state(conn: duckdb.DuckDBPyConnection, game_key: str, new_state: str) -> None:
    row = conn.execute(
        "SELECT state FROM game_manifest WHERE game_key=?", [game_key]
    ).fetchone()
    if row is None:
        raise KeyError(f"game_key not found: {game_key}")
    old = row[0]
    allowed = GAME_TRANSITIONS.get(old, [])
    if new_state not in allowed:
        raise ValueError(f"Invalid game transition {old!r} → {new_state!r} for {game_key}")
    conn.execute(
        "UPDATE game_manifest SET state=?, last_updated=CURRENT_TIMESTAMP WHERE game_key=?",
        [new_state, game_key],
    )


def set_source_fetch_state(
    conn: duckdb.DuckDBPyConnection, source_key: str, new_state: str,
    http_status: int | None = None, error: str | None = None,
) -> None:
    row = conn.execute(
        "SELECT fetch_state FROM source_ledger WHERE source_key=?", [source_key]
    ).fetchone()
    if row is None:
        raise KeyError(f"source_key not found: {source_key}")
    old = row[0]
    allowed = SOURCE_FETCH_TRANSITIONS.get(old, [])
    if new_state not in allowed:
        raise ValueError(f"Invalid source transition {old!r} → {new_state!r}")
    conn.execute(
        """UPDATE source_ledger
           SET fetch_state=?, fetch_http_status=?, fetch_error=?,
               fetch_at=CURRENT_TIMESTAMP
           WHERE source_key=?""",
        [new_state, http_status, error, source_key],
    )


def game_status_summary(conn: duckdb.DuckDBPyConnection) -> dict:
    rows = conn.execute(
        "SELECT state, tier, COUNT(*) FROM game_manifest GROUP BY state, tier ORDER BY tier, state"
    ).fetchall()
    out: dict = {}
    for state, tier, cnt in rows:
        out.setdefault(tier, {})[state] = cnt
    total = conn.execute("SELECT COUNT(*) FROM game_manifest").fetchone()[0]
    out["_total"] = total
    return out
