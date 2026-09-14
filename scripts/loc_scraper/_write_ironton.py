import json
from pathlib import Path

JSONL = Path(r'D:\league-history-data\nfl\curated\loc_scraper\image_extracts.jsonl')

# Ironton Ohio News, September 14, 1931 (Monday)
# Covers Sunday September 13, 1931 — Week 1 of the 1931 NFL season
# Headline: "SPARTANS WON OPENING GAME SUNDAY, 14-0"
# Left column: "Tank Memoirs" — rivalry history Portsmouth vs Ironton (Portsmouth's 7th win in 9 years)
# "SCORES BY PERIOD" box: Spartans 0-0-7-7=14, Ironton 0-0-0-0=0
# NOTE: game_key in DB is "1931/2 vs CLE" (wrong — actual game is 1931 w1 PRT vs IRT)
# The newspaper date (Sept 14) and headline ("OPENING GAME") confirm this is week 1, not week 2
# Touchdown scorers not legible at this resolution (dense newspaper column text)

entry = {
    "source_key": "ee1b7f6497522e9d",
    "game_key": "1931.0_w2_REG_IRT_CLE",  # manifest key (wrong opponent — actual is PRT vs IRT w1)
    "year": 1931,
    "week": 1,  # actual week 1 (season opener)
    "team_a": "IRT",   # Ironton Tanks
    "team_b": "PRT",   # Portsmouth Spartans
    "tier": "T1",
    "tile": "ee1b7f6497522e9d_md.jpg",
    "page_description": "Ironton Ohio News sports page, Sept 14 1931. Headline: SPARTANS WON OPENING GAME SUNDAY, 14-0. Left column Tank Memoirs recounts Portsmouth-Ironton rivalry (Portsmouth's 7th win in 9 seasons). SCORES BY PERIOD box visible.",
    "games": [
        {
            "team_a": "Portsmouth Spartans",
            "score_a": 14,
            "team_b": "Ironton Tanks",
            "score_b": 0,
            "source": "headline + SCORES BY PERIOD box (0-0-7-7=14, 0-0-0-0=0)",
            "q1_a": 0, "q2_a": 0, "q3_a": 7, "q4_a": 7,
            "q1_b": 0, "q2_b": 0, "q3_b": 0, "q4_b": 0
        }
    ],
    "player_tds": [],   # touchdown scorers named in article body but not legible at this resolution
    "player_yards": [],
    "lineups": {},
    "notes": "GAME_KEY_MISMATCH: manifest has CLE as opponent but article is clearly PRT vs IRT. Opening game of 1931 NFL season Sept 13 1931. Sparta quarter breakdown: 0-0-7-7=14. Two TDs in 2nd half but scorer names not readable at this resolution."
}

existing = set(json.loads(l)['source_key'] for l in open(JSONL, encoding='utf-8'))
if entry['source_key'] in existing:
    print(f"Already in JSONL: {entry['source_key']}")
else:
    with open(JSONL, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry) + '\n')
    total = sum(1 for _ in open(JSONL, encoding='utf-8'))
    print(f"Appended Ironton entry. Total lines: {total}")
