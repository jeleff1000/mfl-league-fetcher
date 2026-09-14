"""
Constants, paths, team name maps, newspaper priorities, tier definitions.
All tunable values live here — nothing else imports from here circularly.
"""

from __future__ import annotations
import re
from pathlib import Path

# ── Storage ──────────────────────────────────────────────────────────────────

SCRAPER_ROOT = Path(r"D:\league-history-data\nfl\curated\loc_scraper")
TILES_DIR    = SCRAPER_ROOT / "tiles"
OCR_DIR      = SCRAPER_ROOT / "ocr"
SCRAPER_DB   = SCRAPER_ROOT / "loc_scraper.duckdb"

V26_GLOB = r"D:\league-history-data\nfl\releases\*_v26\tables\nfl_player_stats_all.parquet"

OLD_COMPLETENESS_DB = Path(
    r"D:\league-history-data\nfl\curated\completeness\completeness.duckdb"
)
OLD_QUEUE_CSV = Path(
    r"D:\league-history-data\nfl\curated\source_search_queues"
    r"\20260615T190000Z_final_fly_only_source_horizon\source_search_queue.csv"
)

# ── LOC API ───────────────────────────────────────────────────────────────────
# NOTE: Old chroniclingamerica.loc.gov endpoints are dead (308 → 404).
# New endpoints as of 2025:
#   Search:    www.loc.gov/collections/chronicling-america/?fo=json&q=...
#   Full text: tile.loc.gov/text-services/word-coordinates-service?segment=...
#   Tiles:     tile.loc.gov/image-services/iiif/...
# www.loc.gov resource pages are Cloudflare-blocked for bots;
# tile.loc.gov is NOT blocked and is the primary OCR/image endpoint.

# New search endpoint
LOC_SEARCH = "https://www.loc.gov/collections/chronicling-america/"

# Full-text OCR endpoint — segment path comes from search result image_url
# (the entry containing "word-coordinates-service" in the image_url array)
LOC_FULLTEXT_FMT = (
    "https://tile.loc.gov/text-services/word-coordinates-service"
    "?segment={segment}&format=alto_xml&full_text=1"
)

# Tile image endpoint — iiif_path comes from search result image_url (non-wc entries)
LOC_TILE_FMT = (
    "https://tile.loc.gov/image-services/iiif/{iiif_path}/full/pct:{pct}/0/default.jpg"
)

# Legacy — kept for reference; these now 308-redirect and should not be used
LOC_PAPERS_LEGACY   = "https://chroniclingamerica.loc.gov/newspapers.json"

# Polite rate limit — 1 request every 2 seconds; LOC asks for ~10 req/min max
LOC_MIN_INTERVAL_S   = 3.0   # LOC asks ~10 req/min max; 3s = 20/min, safe margin
LOC_TIMEOUT_S        = 12    # was 20; 12s is enough, reduces timeout stall cost
LOC_MAX_RETRIES      = 2     # was 3; 2 retries saves time on permanent failures
LOC_SEARCH_PAGE_SIZE = 20  # results per API call

# LOC is only useful through ~1945 (copyright/digitization cutoff).
# Do NOT attempt LOC for anything after this year.
LOC_MAX_YEAR = 1945

# ── Tiers ─────────────────────────────────────────────────────────────────────
# (year_min, year_max, description, primary_strategy, source_budget)
#
# LOC coverage reality (confirmed via reconnaissance 2026-06-17):
#   T0 1920-1931: LOC viable — Rock Island Argus, Milwaukee Leader
#   T1 1932-1945: LOC best — Washington Evening Star (national coverage), Milwaukee Leader
#   T2 1946-1977: LOC dead. Use PFR boxscores only. Evening Star may help 1946-1955.
#   T3 1978-1993: LOC dead. PBP replay only for tackle gaps.
#   T4 1994+:     Manual anomaly review only.

TIER_DEFS: dict[str, tuple] = {
    "T0": (1920, 1931, "APFA/early NFL",             "loc_newspaper_ocr",  15),
    "T1": (1932, 1945, "Depression/WWII-era NFL",    "loc_evening_star",   12),
    "T2": (1946, 1977, "Post-war/AFL era",            "pfr_boxscore",        8),
    "T3": (1978, 1993, "Pre-PBP IDP tackle gaps",    "local_pbp_replay",    5),
    "T4": (1994, 9999, "Anomaly investigation",       "manual_review",       3),
}

def game_tier(year: int) -> str:
    for t, (y_min, y_max, *_) in TIER_DEFS.items():
        if y_min <= year <= y_max:
            return t
    return "T4"

def source_budget(tier: str) -> int:
    return TIER_DEFS.get(tier, TIER_DEFS["T4"])[4]

# ── Search levels within each tier ───────────────────────────────────────────
# Levels are tried in order. Moving to the next level only happens when the
# current level is exhausted (all sources fetched, none resolved the game).

# LOC search strategy — ordered by what actually works (confirmed reconnaissance 2026-06-17)
#
# T0 (1920-1931): PFA first (cleanest structured data), then LOC home-city papers,
#   then Evening Star (DC, national coverage), then Milwaukee Leader sweep.
#   LOC OCR for 1920s is partially degraded — realistic yield is game scores +
#   team rosters, not player-level yardage.
#
# T1 (1932-1945): Evening Star (sn83045462) has the densest national NFL coverage
#   in the LOC corpus — try it FIRST for any T1 game, regardless of teams.
#   Then PFR for structured box scores. Then local home/away papers.
#
# T2 (1946-1977): LOC is dead (copyright cutoff ~1945 for most papers).
#   PFR boxscores are the only viable source. Some Evening Star coverage
#   may extend to ~1955 — try it as a bonus pass.
#
# T3 (1978-1993): Only source is local stathead PBP replay for tackle gaps.
#
# T4 (1994+): Manual anomaly review only.

SEARCH_LEVELS: dict[str, list[dict]] = {
    "T0": [
        {"level": 1, "strategy": "pfa_html",             "label": "PFA structured HTML box score"},
        {"level": 2, "strategy": "loc_home_city",        "label": "LOC home-team city paper"},
        {"level": 3, "strategy": "loc_evening_star",     "label": "LOC Washington Evening Star (national)"},
        {"level": 4, "strategy": "loc_milwaukee_leader", "label": "LOC Milwaukee Leader (Midwest)"},
        {"level": 5, "strategy": "loc_score_sweep",      "label": "LOC exhaustive date+teams sweep"},
        {"level": 6, "strategy": "flag_manual",          "label": "Flag for manual review"},
    ],
    "T1": [
        {"level": 1, "strategy": "loc_evening_star",     "label": "LOC Washington Evening Star (best national)"},
        {"level": 2, "strategy": "pfa_html",             "label": "PFA structured HTML box score"},
        {"level": 3, "strategy": "pfr_boxscore",         "label": "PFR boxscore lookup"},
        {"level": 4, "strategy": "loc_home_city",        "label": "LOC home-team city paper"},
        {"level": 5, "strategy": "loc_milwaukee_leader", "label": "LOC Milwaukee Leader (Midwest fallback)"},
        {"level": 6, "strategy": "flag_manual",          "label": "Flag for manual review"},
    ],
    "T2": [
        # LOC is dead for 1946-1977. PFR only.
        {"level": 1, "strategy": "pfr_boxscore",         "label": "PFR boxscore lookup"},
        {"level": 2, "strategy": "pfa_html",             "label": "PFA structured HTML box score"},
        {"level": 3, "strategy": "loc_evening_star",     "label": "LOC Evening Star (1946-1955 only)"},
        {"level": 4, "strategy": "flag_manual",          "label": "Flag for manual review"},
    ],
    "T3": [
        # LOC dead. Only local PBP parquets for tackle gaps.
        {"level": 1, "strategy": "local_pbp_replay",     "label": "Local stathead PBP replay (tackle gaps)"},
        {"level": 2, "strategy": "pfr_gamelog",          "label": "PFR gamelog individual check"},
        {"level": 3, "strategy": "flag_manual",          "label": "Flag for manual review"},
    ],
    "T4": [
        {"level": 1, "strategy": "pfr_boxscore",         "label": "PFR boxscore anomaly check"},
        {"level": 2, "strategy": "flag_manual",          "label": "Flag for manual review"},
    ],
}

# ── Historical team names ─────────────────────────────────────────────────────
# franchise_number → list of (year_start, year_end, [search_terms])
# Terms are used verbatim in LOC full-text search queries.

FRANCHISE_NAMES: dict[int, list[tuple]] = {
    # Modern franchises (1-40 range)
    2:  [(1920,1920,["Decatur Staleys","Staleys"]),
         (1921,1921,["Chicago Staleys","Staleys"]),
         (1922,2025,["Chicago Bears","Bears"])],
    4:  [(1932,1932,["Boston Braves","Braves"]),
         (1933,1936,["Boston Redskins","Redskins"]),
         (1937,2019,["Washington Redskins","Redskins"]),
         (2020,2021,["Washington Football Team","Washington"]),
         (2022,2025,["Washington Commanders","Commanders"])],
    5:  [(1925,2025,["New York Giants","Giants"])],
    6:  [(1921,2025,["Green Bay Packers","Packers"])],
    7:  [(1960,2025,["Dallas Cowboys","Cowboys"])],
    8:  [(1920,1959,["Chicago Cardinals","Cardinals"]),
         (1960,1987,["St. Louis Cardinals","Cardinals"]),
         (1988,1993,["Phoenix Cardinals","Cardinals"]),
         (1994,2025,["Arizona Cardinals","Cardinals"])],
    9:  [(1933,2025,["Philadelphia Eagles","Eagles"])],
    10: [(1933,1939,["Pittsburgh Pirates","Pittsburgh"]),
         (1940,2025,["Pittsburgh Steelers","Steelers"])],
    11: [(1967,2025,["New Orleans Saints","Saints"])],
    12: [(1976,2025,["Tampa Bay Buccaneers","Buccaneers"])],
    13: [(1966,2025,["Atlanta Falcons","Falcons"])],
    14: [(1937,1945,["Cleveland Rams","Rams"]),
         (1946,1994,["Los Angeles Rams","Rams"]),
         (1995,2015,["St. Louis Rams","Rams"]),
         (2016,2025,["Los Angeles Rams","Rams"])],
    15: [(1946,2025,["San Francisco 49ers","49ers"])],
    16: [(1930,1932,["Portsmouth Spartans","Spartans","Portsmouth"]),
         (1934,2025,["Detroit Lions","Lions"])],
    17: [(1996,2025,["Baltimore Ravens","Ravens"])],
    18: [(1950,1995,["Cleveland Browns","Browns"]),
         (1999,2025,["Cleveland Browns","Browns"])],
    19: [(1960,1970,["Boston Patriots","Patriots"]),
         (1971,2025,["New England Patriots","Patriots"])],
    20: [(1961,2025,["Minnesota Vikings","Vikings"])],
    21: [(1966,2025,["Miami Dolphins","Dolphins"])],
    22: [(1968,2025,["Cincinnati Bengals","Bengals"])],
    23: [(1967,2025,["New Orleans Saints","Saints"])],
    24: [(1976,2025,["Seattle Seahawks","Seahawks"])],
    25: [(1996,2025,["Baltimore Ravens","Ravens"])],
    26: [(1953,1983,["Baltimore Colts","Colts"]),
         (1984,2025,["Indianapolis Colts","Colts"])],
    27: [(2002,2025,["Houston Texans","Texans"])],
    28: [(1960,1996,["Houston Oilers","Oilers"]),
         (1997,1998,["Tennessee Oilers","Oilers"]),
         (1999,2025,["Tennessee Titans","Titans"])],
    29: [(1960,1962,["Dallas Texans","Texans"]),
         (1963,2025,["Kansas City Chiefs","Chiefs"])],
    30: [(1960,1981,["Oakland Raiders","Raiders"]),
         (1982,1994,["Los Angeles Raiders","Raiders"]),
         (1995,2019,["Oakland Raiders","Raiders"]),
         (2020,2025,["Las Vegas Raiders","Raiders"])],
    31: [(1960,1960,["Los Angeles Chargers","Chargers"]),
         (1961,2016,["San Diego Chargers","Chargers"]),
         (2017,2025,["Los Angeles Chargers","Chargers"])],
    32: [(1960,2025,["Denver Broncos","Broncos"])],
    33: [(1960,2025,["Buffalo Bills","Bills"])],
    34: [(1960,1962,["New York Titans","Titans"]),
         (1963,2025,["New York Jets","Jets"])],
    35: [(1995,2025,["Jacksonville Jaguars","Jaguars"])],
    36: [(1995,2025,["Carolina Panthers","Panthers"])],

    # Ancient/defunct franchises (100+ series)
    101: [(1920,1921,["Akron Pros","Akron"]),
          (1922,1925,["Akron Indians","Akron"]),
          (1926,1926,["Akron Pros","Akron"])],
    102: [(1920,1925,["Rock Island Independents","Rock Island"])],
    103: [(1920,1925,["Rochester Jeffersons","Rochester"])],
    104: [(1920,1926,["Hammond Pros","Hammond"])],
    105: [(1920,1926,["Canton Bulldogs","Canton"])],
    106: [(1920,1926,["Columbus Panhandles","Columbus Panhandles","Panhandles"])],
    107: [(1920,1929,["Dayton Triangles","Dayton"])],
    108: [(1920,1921,["Buffalo All-Americans","Buffalo"]),
          (1924,1924,["Buffalo Bisons","Buffalo"]),
          (1925,1925,["Buffalo Bisons","Buffalo"]),
          (1927,1927,["Buffalo Bisons","Buffalo"]),
          (1929,1929,["Buffalo Bisons","Buffalo"]),
          (1926,1926,["Buffalo Rangers","Buffalo"])],
    109: [(1920,1920,["Cleveland Tigers","Cleveland"]),
          (1921,1923,["Cleveland Indians","Cleveland"]),
          (1924,1925,["Cleveland Bulldogs","Cleveland"]),
          (1927,1927,["Cleveland Bulldogs","Cleveland"]),
          (1931,1931,["Cleveland Indians","Cleveland"])],
    110: [(1924,1931,["Frankford Yellow Jackets","Frankford"])],
    111: [(1922,1926,["Milwaukee Badgers","Milwaukee"])],
    112: [(1923,1925,["Duluth Kelleys","Duluth"]),
          (1926,1927,["Duluth Eskimos","Duluth"])],
    113: [(1925,1931,["Providence Steam Roller","Steam Roller","Providence"])],
    114: [(1929,1929,["Boston Bulldogs","Boston"]),
          (1944,1948,["Boston Yanks","Yanks","Boston"])],
    115: [(1925,1928,["Pottsville Maroons","Pottsville"])],
    116: [(1925,1926,["Detroit Panthers","Detroit"]),
          (1928,1928,["Detroit Wolverines","Wolverines"])],
    117: [(1926,1926,["Brooklyn Lions","Brooklyn"]),
          (1930,1943,["Brooklyn Dodgers","Brooklyn"]),
          (1944,1944,["Brooklyn Tigers","Brooklyn"])],
    118: [(1929,1932,["Staten Island Stapletons","Stapletons","Staten Island"])],
    119: [(1924,1924,["Kansas City Blues","Kansas City"]),
          (1925,1926,["Kansas City Cowboys","Kansas City"])],
    120: [(1921,1924,["Minneapolis Marines","Marines","Minneapolis"]),
          (1929,1930,["Minneapolis Red Jackets","Red Jackets","Minneapolis"])],
    121: [(1922,1924,["Racine Legion","Racine"]),
          (1926,1926,["Racine Tornadoes","Racine"])],
    122: [(1926,1926,["Hartford Blues","Hartford"])],
    123: [(1926,1926,["Louisville Colonels","Louisville"])],
    124: [(1924,1924,["Kenosha Maroons","Kenosha"])],
    125: [(1920,1920,["Muncie Flyers","Muncie"])],
    126: [(1926,1926,["Los Angeles Buccaneers","Los Angeles"])],
    127: [(1926,1928,["New York Yankees","Yankees","New York"]),
          (1936,1937,["New York Yankees","Yankees"])],
    128: [(1929,1931,["Orange Tornadoes","Orange"]),
          (1930,1930,["Newark Tornadoes","Newark"])],
    137: [(1952,1952,["Dallas Texans","Texans","Dallas"])],
    148: [(1944,1948,["Boston Yanks","Yanks"])],
}

def get_search_terms(franchise_num: int, year: int) -> list[str]:
    """Return search term variants for a franchise in a given year (config.py ID system)."""
    for y_start, y_end, terms in FRANCHISE_NAMES.get(franchise_num, []):
        if y_start <= year <= y_end:
            return terms
    return []


# ── v26 team-code → search terms (authoritative — use these in assess/discover) ─
# Keys are the nfl_team codes from the v26 super table (PFR-derived).
# Values are ordered: most specific first (full name), abbreviations last.

TEAM_CODE_NAMES: dict[str, list[str]] = {
    "AKR": ["Akron Pros", "Akron Indians", "Akron"],
    "BKN": ["Brooklyn Dodgers", "Brooklyn Tigers", "Brooklyn"],
    "BOS": ["Boston Yanks", "Boston Braves", "Boston"],
    "BUF": ["Buffalo All-Americans", "Buffalo Bisons", "Buffalo Rangers", "Buffalo"],
    "CAN": ["Canton Bulldogs", "Canton"],
    "CHI": ["Chicago Bears", "Chicago Staleys", "Staleys", "Bears"],
    "CIN": ["Cincinnati Reds", "Cincinnati"],
    "CLE": ["Cleveland Bulldogs", "Cleveland Indians", "Cleveland Tigers", "Cleveland"],
    "COL": ["Columbus Panhandles", "Columbus Tigers", "Panhandles", "Columbus"],
    "CRD": ["Chicago Cardinals", "Cardinals"],
    "DAY": ["Dayton Triangles", "Dayton"],
    "DET": ["Detroit Lions", "Detroit Panthers", "Detroit Wolverines", "Detroit"],
    "DUL": ["Duluth Kelleys", "Duluth Eskimos", "Duluth"],
    "FRN": ["Frankford Yellow Jackets", "Yellow Jackets", "Frankford"],
    "GB":  ["Green Bay Packers", "Packers", "Green Bay"],
    "GNB": ["Green Bay Packers", "Packers", "Green Bay"],
    "KEN": ["Kenosha Maroons", "Kenosha"],
    "MIL": ["Milwaukee Badgers", "Milwaukee"],
    "MIN": ["Minneapolis Red Jackets", "Red Jackets", "Minneapolis"],
    "NYG": ["New York Giants", "Giants"],
    "NYY": ["New York Yankees", "Yankees"],
    "OOR": ["Oorang Indians", "Oorang"],
    "PHI": ["Philadelphia Eagles", "Eagles", "Philadelphia"],
    "PIT": ["Pittsburgh Steelers", "Pittsburgh Pirates", "Pittsburgh"],
    "POT": ["Pottsville Maroons", "Pottsville"],
    "PRT": ["Portsmouth Spartans", "Spartans", "Portsmouth"],
    "PRV": ["Providence Steam Roller", "Steam Roller", "Providence"],
    "RAC": ["Racine Legion", "Racine Tornadoes", "Racine"],
    "RAM": ["Cleveland Rams", "Los Angeles Rams", "Rams"],
    "RCH": ["Rochester Jeffersons", "Rochester"],
    "RII": ["Rock Island Independents", "Rock Island"],
    "SIS": ["Staten Island Stapletons", "Stapletons", "Staten Island"],
    "STL": ["St. Louis Gunners", "Gunners", "St. Louis"],
    "TOL": ["Toledo Maroons", "Toledo"],
    "TOR": ["Toronto Northmen", "Toronto"],
    "WAS": ["Washington Redskins", "Redskins", "Washington"],
}


def get_terms_for_team(team_code: str | None) -> list[str]:
    """
    Look up search terms using the v26 nfl_team code.
    This is the CORRECT lookup for assess/discover — the franchise_num
    in game_manifest uses v26 IDs which differ from FRANCHISE_NAMES.
    """
    if not team_code:
        return []
    return TEAM_CODE_NAMES.get(team_code.upper(), [])

# ── City → LOC LCCN paper list ───────────────────────────────────────────────
# Maps a normalized city string to Chronicling America LCCN identifiers.
# Papers listed first are highest quality / most complete for football coverage.

CITY_PAPERS: dict[str, list[str]] = {
    "chicago":       ["sn82005696",   # Chicago Tribune
                      "sn83045487",   # Chicago Inter Ocean
                      "sn84031442"],  # Chicago Daily News
    "new_york":      ["sn83030214",   # New York Tribune
                      "sn83030431",   # New-York Tribune
                      "sn84031417"],  # New York Herald
    "green_bay":     ["sn86086235"],  # Green Bay Press-Gazette
    "buffalo":       ["sn86053085",   # Buffalo Courier-Express
                      "sn86058226",   # Buffalo Morning Express
                      "sn83032107"],  # Buffalo Evening News
    "canton":        ["sn84028645"],  # Canton Repository
    "akron":         ["sn84028436",   # Akron Beacon Journal
                      "sn84028437"],  # Akron Evening Times
    "cleveland":     ["sn83035425",   # Cleveland Plain Dealer
                      "sn83035312"],  # Cleveland Press
    "rock_island":   ["sn92053933",   # Rock Island Argus
                      "sn84038225"],  # Moline Daily Dispatch
    "dayton":        ["sn85026133",   # Dayton Daily News
                      "sn85026134"],  # Dayton Journal
    "rochester":     ["sn84035768",   # Democrat and Chronicle
                      "sn84035769"],  # Rochester Democrat
    "pittsburgh":    ["sn85054042",   # Pittsburgh Post-Gazette
                      "sn85054043"],  # Pittsburgh Press
    "philadelphia":  ["sn82000905",   # Philadelphia Inquirer
                      "sn83045357"],  # Philadelphia Record
    "boston":        ["sn83021234",   # Boston Globe (limited)
                      "sn83021235"],  # Boston Herald
    "detroit":       ["sn83045248",   # Detroit Free Press
                      "sn84038228"],  # Detroit News
    "providence":    ["sn82002637"],  # Providence Journal
    "milwaukee":     ["sn84038105",   # Milwaukee Journal
                      "sn84038106"],  # Milwaukee Sentinel
    "minneapolis":   ["sn83045030",   # Minneapolis Tribune
                      "sn83045031"],  # Minneapolis Star
    "columbus":      ["sn84028445"],  # Columbus Dispatch
    "hammond":       ["sn84028448"],  # Hammond Lake County Times
    "duluth":        ["sn84028449"],  # Duluth News-Tribune
    "pottsville":    ["sn84026916"],  # Pottsville Republican
    "frankford":     ["sn82000905"],  # Use Philadelphia Inquirer
    "staten_island": ["sn83030214"],  # Use New York Tribune
    "brooklyn":      ["sn83030214",   # New York Tribune
                      "sn83030431"],  # New-York Tribune
    "portsmouth":    ["sn84028451"],  # Portsmouth Daily Times
    "kansas_city":   ["sn86063626",   # Kansas City Star
                      "sn86063627"],  # Kansas City Times
    "oakland":       ["sn85066408"],  # Oakland Tribune
    "houston":       ["sn86088614"],  # Houston Chronicle
    "baltimore":     ["sn83009571"],  # Baltimore Sun
    "los_angeles":   ["sn86058226"],  # LA Times (limited LOC coverage)
    "san_francisco": ["sn84038226"],  # SF Chronicle (limited LOC coverage)
    # Washington Evening Star — BEST single NFL source in the LOC corpus.
    # Has national NFL coverage (not just Redskins), clean OCR, through ~1945.
    # Use this for EVERY T0/T1 game as a first-pass national source.
    "_evening_star": ["sn83045462"],  # Washington Evening Star (primary LOC NFL source)
    # National / wire coverage — checked for every T0/T1 game
    "_national":     ["sn82005696",   # Chicago Tribune (best national sports)
                      "sn83030214",   # New York Tribune
                      "sn84038227"],  # Sporting News (weekly)
}

# franchise_number → city key (for CITY_PAPERS lookup)
FRANCHISE_CITY: dict[int, str] = {
    2: "chicago", 4: "boston", 5: "new_york", 6: "green_bay",
    8: "chicago", 9: "philadelphia", 10: "pittsburgh", 14: "cleveland",
    16: "detroit", 17: "baltimore", 18: "cleveland", 19: "boston",
    20: "minneapolis", 21: "miami", 22: "cincinnati", 23: "new_orleans",
    24: "seattle", 26: "baltimore", 28: "houston", 29: "kansas_city",
    30: "oakland", 31: "los_angeles", 32: "denver", 33: "buffalo",
    34: "new_york", 35: "jacksonville",
    # Ancient
    101: "akron", 102: "rock_island", 103: "rochester", 104: "hammond",
    105: "canton", 106: "columbus", 107: "dayton", 108: "buffalo",
    109: "cleveland", 110: "frankford", 111: "milwaukee", 112: "duluth",
    113: "providence", 114: "boston", 115: "pottsville", 116: "detroit",
    117: "brooklyn", 118: "staten_island", 119: "kansas_city",
    120: "minneapolis", 121: "racine",
}

# ── OCR quality assessment ────────────────────────────────────────────────────

# Minimum distinct football keywords for a page to be considered relevant
MIN_FOOTBALL_KEYWORDS = 3

FOOTBALL_KEYWORDS = frozenset([
    "touchdown", "touchdowns", "yard", "yards", "quarterback",
    "halfback", "fullback", "linemen", "rushing", "passing",
    "interception", "fumble", "kickoff", "punt", "field goal",
    "forward pass", "drop kick", "extra point", "gridiron",
    "football", "nfl", "apfa", "national football league",
    "american professional football", "pro football",
    "backs", "tackle", "end run", "scoring",
])

# Score patterns: "Canton 14, Akron 0" / "14-0" / "14 to 0" / "14 0" (word-coord joins lose punct)
SCORE_RE = re.compile(
    r'\b(\d{1,2})\s*(?:to\s+|[-–—]\s*)(\d{1,2})\b'           # "14 to 0" / "14-0"
    r'|\b(\d{1,2})\s*,\s*(\d{1,2})\b'                         # "14, 3" (box-score column)
    r'|(\w[\w\s]{2,20}?)\s+(\d{1,2})[,\s]+(\w[\w\s]{2,20}?)\s+(\d{1,2})\b',  # "Canton 14 Akron 0"
    re.I,
)

# Individual stat patterns tried in assess.py
STAT_PATTERNS: dict[str, list[re.Pattern]] = {
    "passing_yards": [
        re.compile(r'(?:pass(?:ing)?|aerial)[:\s]+(\d+)\s*(?:yards?|yds?)', re.I),
        re.compile(r'(\d+)\s*(?:yards?|yds?)\s+(?:on|via)\s+pass', re.I),
    ],
    "rushing_yards": [
        re.compile(r'rush(?:ing)?[:\s]+(\d+)\s*(?:yards?|yds?)', re.I),
        re.compile(r'(\d+)\s*(?:yards?|yds?)\s+(?:on|via|by)\s+rush', re.I),
        re.compile(r'ground\s+gain(?:ed)?\s+(\d+)', re.I),
    ],
    "receiving_yards": [
        re.compile(r'receiv(?:ing|ed)[:\s]+(\d+)\s*(?:yards?|yds?)', re.I),
        re.compile(r'caught.*?(\d+)\s*(?:yards?|yds?)', re.I),
    ],
    "touchdowns": [
        re.compile(r'(\d+)\s*touchdown', re.I),
        re.compile(r'scored.*?touchdown', re.I),
        re.compile(r'(?:td|t\.d\.)[:\s]+(\d+)', re.I),
    ],
    "field_goals": [
        re.compile(r'field\s+goal[s]?[:\s]+(\d+)', re.I),
        re.compile(r'drop\s*kick(?:ed)?.*?(?:field\s+goal|score)', re.I),
    ],
    "def_interceptions": [
        re.compile(r'intercept(?:ed|ion)[s]?[:\s]+(\d+)', re.I),
        re.compile(r'(\d+)\s+intercept', re.I),
    ],
}

# ── PFA URL templates ─────────────────────────────────────────────────────────

PFA_BOXSCORE_URL = "https://www.profootballarchives.com/nflboxscores1/{code}.html"

# Maps year → PFA prefix (e.g. 1921 → "1921apfa", 1932 → "1932nfl")
def pfa_url_prefix(year: int) -> str:
    if year <= 1921:
        return f"{year}apfa"
    elif year <= 1932:
        return f"{year}nfl"
    else:
        return f"{year}nfl"
