"""
image_extract.py — Claude vision pass over downloaded tile JPEGs.

For each tile (SM preferred, MD if available), calls Claude API with a
structured prompt to extract:
  - Pro football game scores found on the page
  - Player touchdowns (name + team)
  - Player yardage stats (name, yards, type)
  - Lineup / squad lists if visible

Output:
  - DB table `image_extracts` (one row per source)
  - JSONL file at SCRAPER_ROOT/image_extracts.jsonl (for easy inspection)

Idempotent: skips source_keys already in image_extracts table.

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python -m scripts.loc_scraper.image_extract [--limit N] [--model haiku]

Requirements:
    pip install anthropic
"""

from __future__ import annotations
import base64
import json
import os
import re
import time
from pathlib import Path

import duckdb

from .config import SCRAPER_ROOT
from .schema import open_db

# ── Model config ──────────────────────────────────────────────────────────────
_DEFAULT_MODEL = "claude-haiku-4-5-20251001"   # fast + cheap for image reading
_MODELS = {
    "haiku":  "claude-haiku-4-5-20251001",
    "sonnet": "claude-sonnet-4-6",
}

JSONL_OUT = SCRAPER_ROOT / "image_extracts.jsonl"

_SYSTEM = (
    "You are extracting historical NFL (National Football League) game statistics "
    "from scanned newspaper photographs taken between 1920 and 1945. "
    "Focus only on professional football. Ignore college football, baseball, boxing, "
    "and other sports unless they appear in a clearly labeled 'Pro Grid Scores' box."
)

_PROMPT_TMPL = """This is a newspaper sports page from approximately {year}.
The target game involves: {team_a} vs {team_b}.

Extract ALL of the following you can read from the image:

1. **Professional football game scores** — team name + score (e.g. "Rock Island 13, Green Bay 3")
2. **Player touchdowns** — player name + which team they played for
3. **Player yardage** — player name, yards gained, type (rushing/passing/receiving)
4. **Team lineups** — list of player names if a lineup table is visible

Return ONLY valid JSON in this exact format:
{{
  "page_description": "one sentence describing what's on the page",
  "games": [
    {{"team_a": "team name", "score_a": 0, "team_b": "team name", "score_b": 0, "source": "headline or box"}}
  ],
  "player_tds": [
    {{"name": "Player Name", "team": "team name"}}
  ],
  "player_yards": [
    {{"name": "Player Name", "yards": 0, "type": "rushing", "team": "team name"}}
  ],
  "lineups": {{
    "team name": ["Player1", "Player2"]
  }},
  "notes": "any other useful info (e.g. rain halted game, overtime, etc.)"
}}

Only include items you can actually read. Use empty arrays/objects for sections with no data.
Do not include college teams or non-football content."""


def _encode_image(path: str) -> str:
    return base64.standard_b64encode(Path(path).read_bytes()).decode()


def _call_claude(client, tile_path: str, year: int,
                 team_a: str, team_b: str, model: str) -> dict:
    """Call Claude vision API and return parsed JSON."""
    img_b64 = _encode_image(tile_path)
    prompt = _PROMPT_TMPL.format(
        year=year,
        team_a=team_a or "Unknown",
        team_b=team_b or "Unknown",
    )
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=_SYSTEM,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": img_b64,
                    },
                },
                {"type": "text", "text": prompt},
            ],
        }],
    )
    raw = response.content[0].text.strip()
    # Strip markdown code fences if present
    raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.M)
    raw = re.sub(r'\s*```$', '', raw, flags=re.M)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"parse_error": raw[:500], "games": [], "player_tds": [],
                "player_yards": [], "lineups": {}, "notes": ""}


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS image_extracts (
            source_key      VARCHAR PRIMARY KEY,
            game_key        VARCHAR,
            year            INTEGER,
            team_a          VARCHAR,
            team_b          VARCHAR,
            tile_path       VARCHAR,
            model           VARCHAR,
            games_json      VARCHAR,
            player_tds_json VARCHAR,
            player_yards_json VARCHAR,
            lineups_json    VARCHAR,
            notes           VARCHAR,
            raw_response    VARCHAR,
            extract_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def run_image_extract(
    limit: int = 200,
    model_alias: str = "haiku",
    tier_filter: str | None = None,
    min_wait_s: float = 1.0,
) -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set. "
            "Get one from console.anthropic.com and set it:\n"
            "  set ANTHROPIC_API_KEY=sk-ant-..."
        )

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    model = _MODELS.get(model_alias, _DEFAULT_MODEL)

    conn = open_db()
    _ensure_table(conn)

    tier_clause = f"AND gm.tier='{tier_filter}'" if tier_filter else ""
    rows = conn.execute(f"""
        SELECT sl.source_key, sl.local_tile_md, sl.local_tile_sm,
               gm.game_key, gm.year, gm.team_a, gm.team_b, gm.tier
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key = sl.game_key
        WHERE sl.fetch_state = 'FETCHED'
          AND sl.local_tile_sm IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM image_extracts ie WHERE ie.source_key = sl.source_key
          )
          {tier_clause}
        ORDER BY gm.tier, sl.contains_game_ref DESC, gm.year, gm.week
        LIMIT {limit}
    """).fetchall()

    print(f"Running image extract: {len(rows)} tiles  model={model}")
    JSONL_OUT.parent.mkdir(parents=True, exist_ok=True)

    done = 0
    for sk, md, sm, gk, year, ta, tb, tier in rows:
        tile = md if (md and Path(md).exists()) else sm
        if not tile or not Path(tile).exists():
            continue

        print(f"  [{done+1}/{len(rows)}] {year} {ta} vs {tb}  {tier}  {Path(tile).name}")
        t0 = time.time()
        result = _call_claude(client, tile, year or 0, ta or "", tb or "", model)
        elapsed = time.time() - t0

        games_j    = json.dumps(result.get("games", []))
        tds_j      = json.dumps(result.get("player_tds", []))
        yards_j    = json.dumps(result.get("player_yards", []))
        lineups_j  = json.dumps(result.get("lineups", {}))
        notes      = result.get("notes", "")[:500]
        raw        = json.dumps(result)[:2000]

        conn.execute("""
            INSERT OR REPLACE INTO image_extracts
            (source_key, game_key, year, team_a, team_b, tile_path, model,
             games_json, player_tds_json, player_yards_json,
             lineups_json, notes, raw_response)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [sk, gk, year, ta, tb, tile, model,
              games_j, tds_j, yards_j, lineups_j, notes, raw])

        # Append to JSONL
        with open(JSONL_OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "source_key": sk, "game_key": gk, "year": year,
                "team_a": ta, "team_b": tb, "tier": tier,
                "tile": Path(tile).name,
                **result,
            }) + "\n")

        n_games = len(result.get("games", []))
        n_tds   = len(result.get("player_tds", []))
        print(f"    games={n_games}  tds={n_tds}  ({elapsed:.1f}s)")

        done += 1
        wait = max(0, min_wait_s - elapsed)
        if wait > 0:
            time.sleep(wait)

    print(f"\nDone: {done} tiles extracted -> {JSONL_OUT}")
    _print_summary(conn)


def _print_summary(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute("""
        SELECT COUNT(*) total,
               SUM(json_array_length(games_json))      total_games,
               SUM(json_array_length(player_tds_json)) total_tds,
               SUM(json_array_length(player_yards_json)) total_yards
        FROM image_extracts
    """).fetchone()
    total, games, tds, yards = rows
    print(f"\nimage_extracts table: {total} tiles  "
          f"{games} game-scores  {tds} player-TDs  {yards} yard-lines")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit",  type=int, default=200)
    ap.add_argument("--model",  default="haiku", choices=["haiku", "sonnet"])
    ap.add_argument("--tier",   default=None)
    args = ap.parse_args()
    run_image_extract(limit=args.limit, model_alias=args.model, tier_filter=args.tier)
