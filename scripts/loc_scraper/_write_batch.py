import json
from pathlib import Path

JSONL = Path(r'D:\league-history-data\nfl\curated\loc_scraper\image_extracts.jsonl')

entries = [
    {
        'source_key': '3b8015fec130936a',
        'game_key': '1945.0_w09_REG_7_148',
        'year': 1945, 'week': 9, 'team_a': 'GB', 'team_b': None, 'tier': 'T1',
        'tile': '3b8015fec130936a_md.jpg',
        'page_description': 'DUPLICATE of cc6f583b147609b8 - Wilmington Morning Star NC Monday October 15 1945 identical page',
        'games': [
            {'team_a': 'Washington Redskins', 'score_a': None, 'team_b': 'Philadelphia Eagles', 'score_b': None, 'source': 'DUPLICATE: see cc6f583b147609b8'},
            {'team_a': 'Cleveland Rams', 'score_a': 41, 'team_b': 'Chicago Bears', 'score_b': 21, 'source': 'DUPLICATE: see cc6f583b147609b8'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'DUPLICATE: identical page content to source_key cc6f583b147609b8 (Wilmington Morning Star NC Monday October 15 1945 — Underdog Redskins Trounce Eagles; Steelers Score Upset Victory; Cleveland Rams 41 Chicago Bears 21; National Pro Loop Standings; Don Hutson Packers). Same LOC newspaper page indexed under a different game_key (w09 GB vs w08 CHI). No new data.'
    },
    {
        'source_key': 'c92b9c7f6389c8ae',
        'game_key': '1945.0_w09_REG_4_5',
        'year': 1945, 'week': 9, 'team_a': 'WAS', 'team_b': None, 'tier': 'T1',
        'tile': 'c92b9c7f6389c8ae_md.jpg',
        'page_description': 'DUPLICATE of cc6f583b147609b8 - Wilmington Morning Star NC Monday October 15 1945 identical page',
        'games': [
            {'team_a': 'Washington Redskins', 'score_a': None, 'team_b': 'Philadelphia Eagles', 'score_b': None, 'source': 'DUPLICATE: see cc6f583b147609b8'},
            {'team_a': 'Cleveland Rams', 'score_a': 41, 'team_b': 'Chicago Bears', 'score_b': 21, 'source': 'DUPLICATE: see cc6f583b147609b8'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'DUPLICATE: identical page content to source_key cc6f583b147609b8 (Wilmington Morning Star NC Monday October 15 1945 — Underdog Redskins Trounce Eagles; Steelers Score Upset Victory; Cleveland Rams 41 Chicago Bears 21; National Pro Loop Standings). Same LOC newspaper page indexed under a different game_key (w09 WAS vs w08 CHI). No new data.'
    },
    {
        'source_key': '007983123fd6a415',
        'game_key': '1945.0_w11_REG_14_148',
        'year': 1945, 'week': 11, 'team_a': 'RAM', 'team_b': None, 'tier': 'T1',
        'tile': '007983123fd6a415_md.jpg',
        'page_description': 'Detroit Times Saturday November 3 1945 - Bears Are Bears Again Beating Green Bay; Grid Results AP scores table; Pre-Grid Standings NFL standings table; Detroit Lions Bob Tales feature; Boys Town Lions extra points game',
        'games': [
            {'team_a': 'Chicago Bears', 'score_a': None, 'team_b': 'Green Bay Packers', 'score_b': None, 'source': 'BEARS ARE BEARS AGAIN BEATING GREEN BAY — Chicago Bears defeated Green Bay Packers (game played around November 4, 1945; Detroit Times Saturday preview/recap)'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'Detroit Times, Saturday November 3-4, 1945. KEY NFL: "BEARS ARE BEARS AGAIN BEATING GREEN BAY" — Chicago Bears defeated the Green Bay Packers in a 1945 NFL game (confirms Bears\' strength in 1945 even as Cleveland Rams dominated). GRID RESULTS section — AP pro football scores table for the weekend\'s games (hard to read individual scores from image but table visible). PRE-GRID STANDINGS — NFL standings table showing all teams\' W-L-T records before this weekend\'s games. Western Division shows Cleveland Rams atop, Eastern Division shows Philadelphia Eagles or Washington Redskins leading. BOB TALES column by Bob Murphy: Detroit Lions feature. "Another Banner Year... May Hatch Dazzling Season With Lions" — Detroit Lions discussed as hard-fighting team. "Dan Robinson Joins Injured" — Detroit Lions player Dan Robinson added to injury list. "M Victim of Bad Breaks" (Michigan football). "Different in War Years." "Way is Shown by Tigers" (Detroit baseball context). "Injury Jinx Hits Michigan." Boys Town Lions football: "BOYS TOWN, LIONS PROVE TOE IS MIGHTY — Extra Points Decide — By Caroline Nip Central" — Boys Town Lions school football team (Omaha NE), won a game on extra points. "They\'ll Do It Every Time" comic by Jimmy Hatlo. Rangers No Push-over (hockey). Hook and Ladder Days of Ace Gatowski Recalled (football feature). Stay on the Job column. On Night Shift feature. Confusion Marks End of Irish-Navy Battle (Notre Dame vs. Navy college football). Top Junior Circus. Lourdes, De La Salle to Clash for Title (local high school football). Hockey standings section. Boys\' Town C.C. Statistics. Final Metro Pro standings. Detroit Times, Saturday November 3-4 1945.'
    },
    {
        'source_key': '5c9d429902be4583',
        'game_key': '1945.0_w11_REG_14_148',
        'year': 1945, 'week': 11, 'team_a': 'RAM', 'team_b': None, 'tier': 'T1',
        'tile': '5c9d429902be4583_md.jpg',
        'page_description': 'TILE FILE MISSING - no image file found on disk',
        'games': [],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'TILE FILE MISSING: 5c9d429902be4583_md.jpg (and _sm.jpg) not found in tiles directory. LOC page was indexed in the scraper database as fetched but the image file is absent from disk. Cannot extract content. Game context: 1945 NFL week 11, Cleveland Rams (RAM).'
    },
    {
        'source_key': 'f56bb90031b19efb',
        'game_key': '1945.0_w11_REG_14_148',
        'year': 1945, 'week': 11, 'team_a': 'RAM', 'team_b': None, 'tier': 'T1',
        'tile': 'f56bb90031b19efb_md.jpg',
        'page_description': 'Wilmington Morning Star NC Friday November 9 1945 - RAMS NIP GIANTS IN PRO GAME THRILLER; Sensational Field Goal Wins For Skins; Big Joe Aguirre Boots Three Points and Game; New York Team Leads Early By Works of Paschal; Bob Waterfield Rams QB; 1945 NFL',
        'games': [
            {'team_a': 'Cleveland Rams', 'score_a': None, 'team_b': 'New York Giants', 'score_b': None, 'source': 'RAMS NIP GIANTS IN PRO GAME THRILLER — Cleveland Rams barely defeated the New York Giants in a thriller game (1945 NFL, game played Sunday November 4 1945); New York led early on Bill Paschal runs; Bob Waterfield was Cleveland Rams QB'},
            {'team_a': 'Washington Redskins', 'score_a': None, 'team_b': None, 'score_b': None, 'source': 'SENSATIONAL FIELD GOAL WINS FOR \'SKINS; BIG JOE AGUIRRE BOOTS FOR THREE POINTS AND GAME — Joe Aguirre (WAS kicker/end) kicked a field goal (3 points) that won the Redskins\' game (November 4 1945)'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'Wilmington Morning Star NC, Friday November 9 1945. Covers Sunday November 4, 1945 NFL game results + previews Saturday November 10 college games. KEY NFL DATA: (1) CLEVELAND RAMS NIP NEW YORK GIANTS IN PRO GAME THRILLER — Cleveland Rams barely beat the NYG Giants. "New York Team Leads Early By Works of Paschal" — BILL PASCHAL (New York Giants HB, mentioned in 1943 tiles as promising rookie) led the Giants in a controlled early lead. BOB WATERFIELD reference: "Waterfield Teams Pause For Oyster-Oakland Market" — Bob Waterfield (Cleveland Rams QB, future Hall of Famer, starred in the 1945 NFL Championship season) mentioned. Cleveland second string also played. (2) SENSATIONAL FIELD GOAL WINS FOR \'SKINS — Washington Redskins won their game on a last-second/clutch field goal. "BIG JOE AGUIRRE BOOTS FOR THREE POINTS AND GAME" — Joe Aguirre (WAS kicker/end, veteran of multiple game-winning kicks we\'ve seen in prior tiles from 1944 season) kicked the decisive 3-point field goal. BEN HOGAN TAKES RICHMOND OPEN — golf legend Ben Hogan wins the Richmond Open golf tournament. PRO STANDINGS — NFL standings table visible. Daily Crossword. Alabama\'s Bowl Bid Prospects Rise; Duke Holds Southern Conference Lead (college). Irish-Army Game Heads Grid Menu — SECOND SPOT NOW (Notre Dame vs. Army upcoming game; Army was dominant wartime dynasty). Novice-Michigan Contest to Attract Record Crowd On Saturday. Davis, Bonin Undefeated With C.C. Second. Saturday\'s Star — Crimea (horse racing). Boy\'s Bandit Johnson Haunts Fight Picture (boxing). 100 Proof Southern Comfort liquor ad. Old Thompson Bourbon Whiskey (Glenmore Distilleries). Pickards Golf Balls and Fishing Tackle. Wilmington Morning Star NC, Friday November 9 1945.'
    },
    {
        'source_key': 'aace8740e7b01d0c',
        'game_key': '1945.0_w12_REG_2_4',
        'year': 1945, 'week': 12, 'team_a': 'NYG', 'team_b': None, 'tier': 'T1',
        'tile': 'aace8740e7b01d0c_md.jpg',
        'page_description': 'The Star Wilmington NC Monday November 19 1945 - Redskins Sneak Past Chicago Bears 28-21; RAMS STOP CARDS PACK GIANTS WIN; Eagles Smash Steelers 6 to 0; NFL Standings; 1945 NFL week 12 results',
        'games': [
            {'team_a': 'Washington Redskins', 'score_a': 28, 'team_b': 'Chicago Bears', 'score_b': 21, 'source': 'Redskins Sneak Past Chicago Bears, 28-21 (November 18 1945)'},
            {'team_a': 'Philadelphia Eagles', 'score_a': 6, 'team_b': 'Pittsburgh Steelers', 'score_b': 0, 'source': 'Eagles Smash Steelers 6 To 0 (November 18 1945)'},
            {'team_a': 'Cleveland Rams', 'score_a': None, 'team_b': 'Chicago Cardinals', 'score_b': None, 'source': 'RAMS STOP CARDS (Cleveland Rams stopped the Chicago Cardinals, November 18 1945; score not in visible headline)'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'The Star Wilmington NC, Monday November 19 1945. Covers Sunday November 18 1945 NFL games. KEY NFL SCORES: (1) WASHINGTON REDSKINS 28, CHICAGO BEARS 21 — "Redskins Sneak Past Chicago Bears, 28-21." WAS won but it was close ("sneak past" implies a narrow win). (2) PHILADELPHIA EAGLES 6, PITTSBURGH STEELERS 0 — "Eagles Smash Steelers, 6 To 0." PHI Eagles shutout the Steelers (Eagles on their way to the 1945 NFL title). (3) CLEVELAND RAMS STOPPED CHICAGO CARDINALS — "RAMS STOP CARDS" — Rams beat the Cardinals (exact score not visible). PLUS: PACK WIN (Green Bay Packers won); GIANTS WIN (New York Giants won). NFL STANDINGS TABLE — visible showing Eastern and Western Division records after Week 12 of 1945 season. REPORT TWO NATS PLAYING IN CUBA (baseball — Washington Senators players). Army Gridders Called Greatest College Team (Army\'s 1945 undefeated dynasty team). Byrd, Harrison Tied in Azalea Golf Play. SHSAA Announces Rule Changes. Jock Leslie Fight With Sal Bartolo (boxing). Deacons Favored Over Gamecocks (Wake Forest vs. South Carolina). Wildcats Drill For Gray Game (Kentucky). Duke Considered for Orange Bowl (Duke football). Nine Colleges Remain Unbeaten. Army, Navy Tickets Not In Hands of Scalpers. Caps Four Gallon / Play Three Matches (hockey). Chisox Post Victory (Chicago White Sox baseball). Hiram Walker\'s Gin ad. Old Thompson Bourbon Whiskey. Pickards Golf Balls. Carolina Yacht Club. Fine Gifts at Local Jewelers. Cape Fear Loan Office. The Star Wilmington NC, Monday November 19 1945.'
    },
    {
        'source_key': 'e6cf1a064a8ac7b8',
        'game_key': '1945.0_w12_REG_3_148',
        'year': 1945, 'week': 12, 'team_a': 'PHI', 'team_b': None, 'tier': 'T1',
        'tile': 'e6cf1a064a8ac7b8_md.jpg',
        'page_description': 'Wilmington Morning Star NC Monday November 12 1945 - Washington Redskins Rout Yanks 34-0; Visitors Stopped by Baugh Passes To Steve Bagarus; PITT LIONS RAMS PRO GAME VICTORS; New York Giants Routed By Eagles; Pro Standings; 1945 NFL week results',
        'games': [
            {'team_a': 'Washington Redskins', 'score_a': 34, 'team_b': 'Boston Yanks', 'score_b': 0, 'source': 'Washington Redskins Rout Yanks 34-0; Visitors Stopped by Baugh Passes To Steve Bagarus (November 11 1945; Sammy Baugh passing to Steve Bagarus)'},
            {'team_a': 'Pittsburgh Steelers', 'score_a': None, 'team_b': None, 'score_b': None, 'source': 'PITT, LIONS, RAMS PRO GAME VICTORS — Pittsburgh Steelers won their game (November 11 1945)'},
            {'team_a': 'Detroit Lions', 'score_a': None, 'team_b': None, 'score_b': None, 'source': 'PITT, LIONS, RAMS PRO GAME VICTORS — Detroit Lions won their game (November 11 1945)'},
            {'team_a': 'Cleveland Rams', 'score_a': None, 'team_b': None, 'score_b': None, 'source': 'PITT, LIONS, RAMS PRO GAME VICTORS — Cleveland Rams won their game (November 11 1945)'},
            {'team_a': 'Philadelphia Eagles', 'score_a': None, 'team_b': 'New York Giants', 'score_b': None, 'source': 'NEW YORK GIANTS ROUTED BY EAGLES — Philadelphia Eagles routed the New York Giants by a large margin (November 11 1945)'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'Wilmington Morning Star NC, Monday November 12 1945. Covers Sunday November 11 1945 (Veterans Day/Armistice Day) NFL games. KEY NFL DATA: (1) WASHINGTON REDSKINS 34, BOSTON YANKS 0 — "Washington Redskins Rout Yanks 34-0." Complete shutout. KEY PLAYERS: "Visitors Stopped by Baugh Passes To Steve Bagarus" — SAMMY BAUGH (WAS QB, Hall of Famer) passed to STEVE BAGARUS (WAS HB/WR) repeatedly as the key offensive weapon. "First Half Sees Field Goals Put Indians Ahead Of Market" — possibly referring to a different game where field goals were key. (2) PITT, LIONS, RAMS PRO GAME VICTORS — Pittsburgh Steelers, Detroit Lions, and Cleveland Rams all won their November 11 games. (3) NEW YORK GIANTS ROUTED BY EAGLES — Philadelphia Eagles dominated and routed the New York Giants by a large margin (exact score not visible but "routed" suggests a decisive victory). PRO STANDINGS TABLE — NFL standings visible after this week (Cleveland Rams leading Western Division on way to 9-1 record and 1945 NFL Championship). SATURDAY FOOTBALL SCORES section. 12 COLLEGES UNBEATEN AFTER SATURDAY TILTS. The Sports Trail by Whitney Martin. Blue Devils, Carolina Favorites in Southern (Duke, UNC). Correct Time at Corbett Jewelry. Old Thompson Bourbon. Walker\'s Gin (Hiram Walker\'s). Pickards Golf Balls and Fishing Tackle. Southern Comfort 100 Proof. Super Standings visible. Cape Fear Loan Office 14 South Front St. Specials Monday and Tuesday — Day\'s Wrist Watches, Jane Baker Wrist Watch $39.50, General Electric Speech Guitar, Suits Jackets and Overcoats at local Wilmington store. Wilmington Morning Star NC, Monday November 12 1945.'
    },
    {
        'source_key': 'fc56c67be7a19069',
        'game_key': '1945.0_w12_REG_2_4',
        'year': 1945, 'week': 12, 'team_a': 'NYG', 'team_b': None, 'tier': 'T1',
        'tile': 'fc56c67be7a19069_md.jpg',
        'page_description': 'DUPLICATE of e6cf1a064a8ac7b8 - Wilmington Morning Star NC Monday November 12 1945 identical page',
        'games': [
            {'team_a': 'Washington Redskins', 'score_a': 34, 'team_b': 'Boston Yanks', 'score_b': 0, 'source': 'DUPLICATE: see e6cf1a064a8ac7b8'},
            {'team_a': 'Philadelphia Eagles', 'score_a': None, 'team_b': 'New York Giants', 'score_b': None, 'source': 'DUPLICATE: see e6cf1a064a8ac7b8 — Eagles routed Giants'},
        ],
        'player_tds': [],
        'player_yards': [],
        'lineups': {},
        'notes': 'DUPLICATE: identical page content to source_key e6cf1a064a8ac7b8 (Wilmington Morning Star NC Monday November 12 1945 — Washington Redskins Rout Yanks 34-0; Baugh to Bagarus; Pitt, Lions, Rams winners; Giants Routed by Eagles; Pro Standings). Same LOC newspaper page indexed under a different game_key (w12 NYG vs w12 PHI). No new data.'
    },
]

with open(JSONL, 'a', encoding='utf-8') as f:
    for entry in entries:
        f.write(json.dumps(entry) + '\n')

total = sum(1 for _ in open(JSONL, encoding='utf-8'))
print(f'Appended {len(entries)} entries. Total lines: {total}')
