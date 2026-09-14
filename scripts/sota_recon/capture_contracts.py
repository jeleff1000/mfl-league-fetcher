"""
sota_recon/capture_contracts.py  --  O.8 LAW B: the CAPTURE gate (master plan §25.1)

Law B asks: *from each source, did we take everything it offers?* Law A knows the
source exists; Law C enumerates the columns of what we captured. Neither notices that
a site publishes ten table families and we hold one.

Joe, 2026-07-26: "Law B is NOT a formality for the new sources -- our captures are
thin slices of what the sites publish." Measured on disk the same day:

  StatsCrew            we hold team_season_roster ONLY (57,808 rows). The site
                       publishes full player season stats, team season pages,
                       standings, schedules, and defunct-league records (AAFC, WFL,
                       AAFC-era AFLs, ...).
  ProFootballArchives  we hold player_game_participation ONLY (5.36M rows) -- yet the
                       capture PULLED FULL BOXSCORE PAGES and KEPT THE RAW HTML
                       (shards/*/raw/*.html.gz, e.g. nflboxscores1/2003nfl___.html).
                       Verified 2026-07-26: those pages carry quarter scores, scoring
                       plays with scorer/passer/kicker names, and full game statistics.
                       The stat content is ALREADY ON DISK, unparsed. PFA capture
                       expansion is therefore a PARSE job, not a re-crawl.

A capture contract enumerates the source's own table-of-contents and dispositions each
entry CAPTURED / EXCLUDED(reason) / QUEUED(what it takes). The counter is

    toc_entries - (captured + excluded) = 0        [capture_contract_open_toc_entries]

QUEUED entries are the open ones and keep the counter nonzero BY DESIGN -- that is Law
B doing its job. Registration does NOT close a source's census row while its capture
contract has open TOC entries: a partial capture registered as if complete is the exact
failure this law exists to prevent.

Output: docs/capture-contracts.json

Run:  python -m scripts.sota_recon.capture_contracts
"""

from __future__ import annotations

import json
import os

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "capture-contracts.json")

STATUS_VOCAB = {"CAPTURED", "EXCLUDED", "QUEUED"}


def _e(name: str, status: str, note: str, sources: list[str] | None = None,
       grain: str = "") -> dict:
    d = {"toc_entry": name, "status": status, "note": note}
    if sources:
        d["registered_sources"] = sources
    if grain:
        d["grain"] = grain
    return d


# ---------------------------------------------------------------------------
# CAPTURE CONTRACTS -- one per registered source ORIGIN (site/provider), not per
# registry row: Law B is about what the SITE publishes vs what we took.
# ---------------------------------------------------------------------------
CONTRACTS: dict[str, dict] = {

    "nflcom": {
        "toc_provenance": "PARTIALLY DERIVED (2026-07-26): entries reflect the harvest_state/player_universe config and the observed table families, not a crawl of nfl.com's own sitemap index. nfl.com publishes a /sitemap/ tree that would make this receipted -- unfetched.",
        "origin": "NFL.com (nfl.com player pages, sitemap rosters, season/team stat tables)",
        "capture_state": "raw/nflcom/harvest_state.json",
        "capture_receipt": "39,652 fetches; done_player_views logs/career/splits/"
                           "situational = 20,955 players each = 100% of "
                           "player_universe.json (20,955 slugs). VERIFIED ON DISK "
                           "2026-07-26: 398 parquet (78 MB) across 7 table families "
                           "+ 47,060 CACHED HTML PAGES (8.76 GB) retained in "
                           "raw/nflcom/cache -- like PFA, the source bytes are kept, "
                           "so any missed field is a re-parse, never a re-crawl. **GRANULARITY "
                           "(measured 2026-07-26): these 7 registry rows contain 82 "
                           "LOGICAL TABLES, keyed by the _table/_category/_side/season_type "
                           "row attributes. PFR by contrast holds 55 registry rows for 55 "
                           "logical tables. Registry-row COUNT is therefore not a measure of "
                           "onboarded material, and the O.9 column dossier MUST key on "
                           "(source, _table, column) -- keying on (source, column) would "
                           "collapse 6 split axes into one 47-column row.**",
        "toc": [
            _e("player game logs", "CAPTURED",
               "1,549,161 rows 1920-2025 across 3 season strata: Regular Season "
               "1,386,154 (1920-2025), Post Season 52,933 (1933-2025), and "
               "PRESEASON 110,074 (2008-2025). The preseason stratum is CAPTURED but "
               "UNEXPOSABLE -- v26 season_type vocabulary is REG/POST only, so 110k "
               "player-games we hold cannot be represented. Admission is a domain "
               "ruling for Joe (same class as defunct leagues), not a lane decision",
               ["nflcom_player_logs"], "player_game"),
            _e("player career pages", "CAPTURED",
               "637,134 rows 1950-2025; 9 position-scoped logical tables (QB / RBFB / "
               "WRTE / Offensive Line / K / P / Defense Career + Recent Games)",
               ["nflcom_player_career"], "player_season"),
            _e("player splits", "CAPTURED",
               "4,984,641 rows 1921-2025; 6 split axes (Opponents by Team, Stadiums, "
               "Months, Opponents by Group, Days, Outcomes) -- each a PARTITION of "
               "season totals, so the conservation check (sum over axis == season) is "
               "the witness role, not a second direct stat stream",
               ["nflcom_player_splits"], "player_season_split"),
            _e("player situational", "CAPTURED",
               "2,797,507 rows 1922-2025; 8 situational axes (Point Differential, "
               "Quarters, Field Position, Game Halves, Margin of Victory, Home vs "
               "Road, Stadium Surfaces, Attempts) -- partition/conservation role",
               ["nflcom_player_situational"], "player_season_split"),
            _e("season stat leaderboards", "CAPTURED",
               "83,630 rows 1932-2025; 22 logical tables = 11 stat categories "
               "(rushing/passing/receiving/interceptions/kickoff-returns/punt-returns/"
               "punts/field-goals/kickoffs/fumbles/tackles) x 2 season types",
               ["nflcom_player_season"], "player_season"),
            _e("team season stats", "CAPTURED",
               "71,056 rows 1932-2025; 32 logical tables = 7 categories x offense/"
               "defense/special-teams sides x season type. The DEFENSE side is a "
               "team-allowed witness -- the R5 cross-side lane's natural counterpart",
               ["nflcom_team_stats"], "team_season"),
            _e("team season rosters", "CAPTURED",
               "144,568 rows 1920-2025, 20/20 shards, coverage_complete. COLUMN-COMPLETE "
               "(verified 2026-07-26 against a retained page): the sitemap roster table "
               "publishes only 2 columns (Jersey #, Player) and we kept both -- the NULL "
               "position values are honest, the source has no position column",
               ["nflcom_team_season_roster"], "team_season"),
            _e("targeted ancient game-log re-pull", "CAPTURED",
               "8,381 rows 1932-1977 gap-fill pass (Regular Season 7,573 + Post Season "
               "808); same bloodline as player_logs, never a second vote",
               ["nflcom_player_logs_targeted"], "player_game"),
            _e("standings", "EXCLUDED",
               "team W/L/T is authoritative from nfl_team_games_all (registered "
               "pfr_team_games); a second copy adds no witness value and would need "
               "its own franchise canon"),
            _e("schedules", "EXCLUDED",
               "the game calendar is anchored by schedule_master + pfr_team_games "
               "(registered); nflcom schedule pages would be a redundant copy"),
            _e("player game stats (nflcom player pages)", "QUEUED",
               "DECLARED BUT NEVER RUN: harvest/source_catalog.yaml declares "
               "nflcom.player_game_stats (allowed_prefixes /players/) but it has no "
               "census seed range, sources/nflcom.py parse() RAISES on any dataset "
               "except team_season_roster, and it is absent from the "
               "witness-census-all.yml matrix. NOTE: this overlaps the raw/nflcom "
               "harvest we already hold via a DIFFERENT pipeline (the 47,060-page "
               "cache), so scope it against that before crawling anything"),
            # ---- SPLIT 2026-07-29. One TOC entry bundled three families under one reason,
            # "no canonical column consumes them", and that reason is false for exactly one
            # of the three: draft_round, draft_overall and age_at_draft are registered
            # canonicals in the honors_bio family. A bundled refusal is how a false clause
            # rides along on true ones -- the entry is split so each stands on its own
            # ground, which is the same argument as keeping a refusal's precondition
            # separate from its prose.
            _e("draft", "QUEUED",
               "draft_round, draft_overall and age_at_draft ARE registered canonicals "
               "(stat_contracts.v1.json, family honors_bio), sourced today from the PFR "
               "bio scrape. NFL.com's draft pages are a second publisher of the same "
               "facts and are a legitimate capture target; they were excluded by a reason "
               "that was measurably untrue"),
            _e("transactions / injuries", "EXCLUDED",
               "no canonical column consumes them -- checked against the whole contract "
               "registry (1,218 stats): no transaction_type, injury_status or "
               "injury_designation canonical exists in any family. Revisit only if the "
               "§18 spine surfaces demand"),
        ],
    },

    "statscrew": {
        "toc_provenance": "HAND-AUTHORED, UNVERIFIED (2026-07-26): StatsCrew roster pages are LEAVES -- they carry no section nav, so the site TOC cannot be derived from our retained bytes. Every StatsCrew entry below is my enumeration from general knowledge, NOT a receipt. Verify against the site index before trusting this list in either direction.",
        "origin": "StatsCrew.com (statscrew.com/football/...)",
        "capture_state": "ff_assets/statscrew/.../IMPORT_MANIFEST.json",
        "capture_receipt": "THREE datasets as of 2026-07-27. "
                           "(1) team_season_stats: 168,390 rows, 1921-2025, 5/5 shards "
                           "across runs 30232968983 + 30238150737, 11 table tags. "
                           "(2) team_season_results: 23,235 rows, 1920-2023, 15/15 "
                           "shards across runs 30271114181 + 30268109268, 1,521 "
                           "team-seasons. "
                           "(3) team_season_roster: 57,808 rows, 1920-2025, "
                           "coverage_complete; URL space crawled = "
                           "/football/roster/t-{TEAM}/y-{YEAR}. RECEIPT VERIFIED "
                           "2026-07-26 by summing the shard parquet: 57,808 actual == "
                           "57,808 claimed. Only 14 of 20 shards carry a "
                           "records.parquet -- the other 6 made zero requests (0-byte "
                           "REQUEST_LEDGER), so the work partition left them empty; a "
                           "partitioning artifact, NOT missing data, and the exact row "
                           "match proves it. "
                           "ROSTER COVERAGE CORRECTION (2026-07-27, receipt "
                           "docs/statscrew-roster-coverage.json): the rosters are "
                           "COMPLETE against the OBTAINABLE denominator, not 45% "
                           "complete -- 1,571 HELD + 845 EMPTY_AT_SOURCE + 0 UNKNOWN = "
                           "2,416 seeds. StatsCrew publishes an empty roster table for "
                           "845 team-seasons, so the obtainable denominator is 1,571. "
                           "The '1,332 missing' figure counted pages that can never be "
                           "captured -- a DEFLATED coverage number off an inflated "
                           "denominator",
        "law_b_note": "THE THIN-SLICE CASE (Joe, 2026-07-26), NOW PARTLY CLOSED "
                      "(2026-07-27): the two team-scoped stat families are captured; "
                      "the PLAYER-page family (p-{id}) and the league/franchise "
                      "indexes remain QUEUED. "
                      "NOT GREENFIELD (measured 2026-07-26 against the ff-assets "
                      "harvest repo): statscrew.player_season_stats is ALREADY DECLARED "
                      "in harvest/source_catalog.yaml (allowed_prefixes "
                      "/football/stats/) and sources/statscrew.py parse() ALREADY "
                      "accepts it as a legal dataset. It has never run because it lacks "
                      "a census seed template and a work_item_from_url branch, and "
                      "because witness-census-all.yml's matrix lists only "
                      "team_season_roster. Cost is a seed config + a matrix line, not a "
                      "new scraper. "
                      "Everything below marked QUEUED is published by the site and "
                      "NOT captured. StatsCrew is a genuinely separate compilation "
                      "origin -- its stat families are the highest-value new-root "
                      "capture available, especially for defunct leagues no other "
                      "registered source covers.",
        "toc": [
            _e("team season rosters (rows)", "CAPTURED",
               "57,808 rows with position + statscrew player ids (anderhun001 class)",
               ["statscrew_team_season_roster"], "team_season"),
            _e("team season rosters: DROPPED COLUMNS", "CAPTURED",
               "CLOSED 2026-07-26 by reparse_retained_captures.py (commit 9480aef8e) -- "
               "NO CRAWL. All 8 dropped columns recovered from the 1,692 retained pages: "
               "57,880 rows at 100% BIRTH DATE across 19,255 players, which EXCEEDS the "
               "original capture (57,808) because the old parser lost rows as well as "
               "columns. Payoff: 2,988 name-unambiguous DOB candidates for bio rows with "
               "none (pre-1933 769 / 1933-49 815 / 1950-77 1,081). QUEUED NOT APPLIED -- "
               "writing birth_date into player_bio mutates the identity spine",
               ["statscrew_team_season_roster"], "team_season"),
            _e("player season stats (ALL families on one page)", "QUEUED",
               "MEASURED 2026-07-26 (fetched /football/stats/p-brownjim001): EIGHT tables on "
               "ONE player page -- Passing (Att,Comp,Comp%,Yds,Yds/Att,TDs,TD%,Ints,Int%,"
               "Long,Rating); Rushing; Receiving; Kick Returns; DEFENSE AND FUMBLES (Tackle,"
               "Brup,Fum,F Rec,FYds,FTD,FF); TOTAL SCORING (Rush,Rec,Punt,Kick,MFG,Int,Fum,"
               "Other TDs + FG,X/C,Single,2Pt); Playing Career (Year,League,Team,GP,GS); "
               "Awards; Honors. URL /football/stats/p-{id}, and THE SEEDS ARE ALREADY ON "
               "DISK -- our retained roster pages link p-{id} for every player (1,735 links "
               "in a 50-page sample). HIGHEST VALUE IN THE PROGRAM: TACKLES for pre-1994 "
               "seasons, our thinnest stratum, on a candidate-independent root"),
            _e("team season pages", "CAPTURED",
               "CLOSED 2026-07-27 -- harvested and imported as statscrew/team_season_stats: "
               "168,390 rows, 5/5 shards across runs 30232968983 + 30238150737, 1921-2025, "
               "ELEVEN table-tagged families (the page ships eleven captions, not the ten "
               "counted here by eye in the 2026-07-26 probe of a single 1963 page -- SACKS "
               "appears on later pages and the hand count missed it). Rows are TABLE-TAGGED "
               "by the page's own <h2> caption because five tables share the header set "
               "`avg long no player tds yds` and the table INDEX moves between pages, so "
               "neither signature nor position identifies them. Column semantics come from "
               "the site's own title attributes (COLUMN_DICTIONARY.jsonl artifact), not from "
               "the short labels. "
               "ORIGINAL PROBE: MEASURED 2026-07-26 (fetched /football/stats/t-CLE/y-1963): TEN tables of "
               "per-player rows for one team-season -- Passing, Rushing, Receiving, Kicking "
               "(X/CA,X/CM,X/CP%,FGA,FGM,FG%,Pts), Punting, Punt Returns, Kick Returns, "
               "Interceptions, Defense and Fumbles, Total Scoring. URL "
               "/football/stats/t-{TEAM}/y-{year} -- harvest/team_seasons.csv ALREADY "
               "enumerates every (team, year), so this needs NO discovery crawl. This is "
               "the R4 vertical team witness",
               ["statscrew_team_season_stats"], "team_season x player x table_tag"),
            _e("standings", "QUEUED",
               "cross-check for team_games W/L/T in pre-1970 eras where the catalog "
               "is thinnest"),
            _e("schedules / game-by-game results", "CAPTURED",
               "CLOSED 2026-07-27 -- harvested and imported as "
               "statscrew/team_season_results: 23,235 rows, 15/15 shards across runs "
               "30271114181 + 30268109268, 1,521 team-seasons, 1920-2023. The 6 columns "
               "the 2026-07-26 probe counted are EIGHT stored: the page ships two "
               "trailing BLANK <th> cells, which the pre-fix parser silently dropped "
               "along with their values; they are preserved as col_6/col_7 and are "
               "OPEN dossier rows, not assumed empty. Also carries a season type "
               "(row_class reg_season 22,401 / post_season 834), the only StatsCrew "
               "family that publishes one. "
               "ORIGINAL PROBE: MEASURED 2026-07-26 (fetched /football/results/t-CLE/y-1963): "
               "game-by-game results, 6 cols (Date, Game, Res, Home, Road, Record) x 15 "
               "rows for one team-season. An independent game-calendar AND running-"
               "record witness for pre-1970 seasons where our catalog is thinnest; "
               "seeds come free from team_seasons.csv",
               ["statscrew_team_season_results"], "team_season x game"),
            _e("franchise history /football/t-{TEAM}", "QUEUED",
               "MEASURED 2026-07-26 (fetched /football/t-CLE): franchise history with "
               "ERA-SCOPED name/logo spans (1946-1958, 1959-1969, 1970-2025). A "
               "franchise-identity witness -- directly relevant to the team_fid "
               "era-alias problem (OTI/HOU, CRD/STL/PHO, CLT/BAL) that the O.7 DEF-row "
               "join had to solve by hand"),
            _e("league indexes /football/l-{LEAGUE}", "QUEUED",
               "MEASURED 2026-07-26: /football/l-NFL and /football/l-APFA each serve "
               "'<LEAGUE> by Season' + '<LEAGUE> Teams' indexes. APFA (1920-21) is the "
               "PRE-NFL league -- era coverage at our oldest floor. Also observed: l-CFL, "
               "l-AFL, l-UFL2. These indexes ARE the season/team seed lists. "
               "NARROWED 2026-07-28 by Joe's domain ruling ('we don't need the CFL "
               "stuff'): l-CFL is OUT OF SCOPE and is not to be captured. l-NFL and "
               "l-APFA remain queued -- APFA is the NFL's own first two seasons, not a "
               "foreign league. l-AFL IS IN SCOPE, measured 2026-07-28: the subject "
               "already carries all twelve AFL franchises 1960-69, so its index is a "
               "seed list for an era we admit. l-CFL and l-UFL2 are OUT under the "
               "parallel-league rule below (the subject carries neither, so neither can "
               "be a coverage gap)"),
            _e("defunct-league records: APFA / AAFC / AFL", "QUEUED",
               "THE ADMISSION QUESTION WAS ALREADY ANSWERED BY THE SUBJECT, and this "
               "entry spent two sessions asking Joe to decide something the data had "
               "settled. It read 'whether they enter the NFL supertable at all is a "
               "DOMAIN question' -- an open question asserted without querying v26. "
               "MEASURED 2026-07-28 against the live release, on Joe's correction: "
               "APFA 1920-21 (306 + 370 rows, 14 and 21 teams); the AAFC carried in "
               "full, INCLUDING all eight franchises that never played an NFL game "
               "(Miami Seahawks 283 rows 1946 only, LA Dons 1,102 rows 1946-49, "
               "Chicago Rockets 807 -> Hornets 200, Brooklyn Dodgers 796, NY Yankees "
               "973, AAFC Baltimore Colts 773, AAFC Buffalo 985); the AFL carried in "
               "full 1960-69 (Dallas Texans 1,007 rows 1960-62 -> Kansas City 1963+, "
               "NY Titans 1,109 -> Jets, Boston 3,750, Denver 3,498, Houston 3,670, "
               "Oakland 3,883, Buffalo 3,936). The FRANCHISE LIFESPANS are individually "
               "correct, which is a stronger correctness signal than the row counts. "
               "Spot-checked on Joe's own examples: George Blanda HOU 1960-66 (3,490 "
               "yds / 37 TD in 1961) then OAK 1967-69; Lance Alworth SDG 1962-70. "
               "SO THIS IS NOT AN ADMISSION QUESTION -- it is a COVERAGE question, which "
               "makes the StatsCrew capture MORE valuable, not less: it would fill eras "
               "the subject already admits rather than propose new ones. What is still "
               "genuinely unknown is whether our AAFC/AFL STAT coverage is complete or "
               "thin, and that is measurable without asking anyone"),
            _e("defunct-league records: WFL (1974-75)", "EXCLUDED",
               "RULED OUT by Joe 2026-07-28 ('i dont want WFL'), and the measurement "
               "agrees: v26 years 1974-75 carry 26 team codes, ALL NFL -- no WFL "
               "franchise appears. Unlike the AAFC and AFL the WFL had no franchise or "
               "league continuity into the NFL (it folded mid-1975), so admitting it "
               "would WIDEN the subject rather than fill it. See the parallel-league "
               "rule recorded on the league-index entry"),
            _e("parallel-league records (USFL, XFL, UFL, CFL, ...)", "EXCLUDED",
               "THE RULE, receipted rather than decided league by league. MEASURED "
               "2026-07-28 across the WHOLE v26 release: 78 distinct team codes, and "
               "every one is NFL-lineage -- the 1920s APFA clubs (AKR/CAN/DAY/DUL/FRN/"
               "HAM/POT/PRV/RII/TOL/...), the AAFC 1946-49, the AFL 1960-69, and the "
               "modern NFL. Every parallel-league era carries EXACTLY the NFL team "
               "count and nothing else: WFL 1974-75 = 26 codes, USFL 1983-85 = 29, XFL "
               "2001 = 31, XFL 2020 = 32, XFL/UFL 2023-25 = 32. "
               "SO THE SUBJECT'S BOUNDARY IS MEASURABLE, not a matter of taste: it is "
               "the NFL LINEAGE -- the league itself plus the two leagues that merged "
               "into it. A league the subject has never carried cannot be a coverage "
               "gap, only an expansion. "
               "STANDING DECISION (Joe, 2026-07-28: 'dont want expansion' -- scoped to "
               "LEAGUES when asked, explicitly NOT to the supertable column candidates "
               "or the PFA capture gap): the subject is not to be widened by admitting "
               "a league it has never carried. This is a DECLINE, not a queue item -- "
               "no future session should re-raise CFL / WFL / USFL / XFL / UFL as an "
               "open question or spend capture budget on them. His three rulings the "
               "same day (CFL out, WFL out, no league expansion) are one rule, and it "
               "is now the rule rather than three precedents. "
               "WHAT WOULD REOPEN IT is a fact, not an opinion: if the subject ever "
               "acquires a team code outside the NFL lineage, the boundary this rule is "
               "measured from has moved and the rule must be re-derived. The 78-code "
               "census above is the receipt to re-run"),
            _e("coaching records", "EXCLUDED",
               "no canonical player or team stat column consumes coaching records; they "
               "describe personnel tenure, not measured game events"),
        ],
    },

    "profootballarchives": {
        "origin": "ProFootballArchives.com (profootballarchives.com/nflboxscores*/...)",
        "capture_state": "ff_assets/profootballarchives/.../IMPORT_MANIFEST.json",
        "capture_receipt": "player_game_participation ONLY: 5,359,619 rows, 1920-2025, "
                           "coverage_complete. RECEIPT VERIFIED 2026-07-26 by summing "
                           "the shard parquet: 5,359,619 actual == 5,359,619 claimed "
                           "(13 of 20 shards carry parquet; the rest made no requests). "
                           "REQUEST_LEDGER shows the crawl hit boxscore pages "
                           "(nflboxscores1/{year}nfl{NNN}.html) and shards/*/raw/*.html.gz "
                           "retains the FULL PAGE HTML: 151,623 pages, 746 MB, COUNTED "
                           "on disk.",
        "law_b_note": "THE PARSE-NOT-CRAWL CASE (verified 2026-07-26 by decompressing a "
                      "shard-0 raw page): the retained raw HTML carries score-by-quarter, "
                      "scoring plays with player names, and full game statistics -- but "
                      "our parser extracted participation only. Expanding this capture "
                      "costs NO new crawl and NO rate-limit exposure; it is a re-parse "
                      "of raw HTML bytes already on disk -- 151,623 retained boxscore "
                      "pages, counted 2026-07-26 -- 11 data tables each, of which we "
                      "parsed ONE. That makes it the cheapest large cross-examination "
                      "gain in the program.",
        "toc_provenance": "RECEIPTED (2026-07-26): the site's OWN nav is retained in "
                          "every one of the 151,623 captured pages and reads: Home, "
                          "Leagues, Seasons, Teams, Players, Coaches, Drafts, Awards, "
                          "Leaderboards. The entries below are reconciled AGAINST that "
                          "nav -- this is the only source whose TOC is currently "
                          "site-derived rather than hand-authored.",
        "toc": [
            _e("boxscore index pages (census seeds)", "CAPTURED",
               "the per-season boxscore index ({year}nfl-boxscores.html / "
               "{year}apfa-boxscores.html) that seeds the census -- declared in "
               "harvest/source_catalog.yaml census_seed_ranges and crawled every "
               "run. Named explicitly because derive_source_toc.py observes this "
               "pattern in retained bytes, and every observed pattern must map to "
               "a REAL contract entry, never an invented one"),
            _e("per-game player participation", "CAPTURED",
               "5,359,619 rows 1920-2025 -- the widest participation witness we hold",
               ["pfa_player_game_participation"], "player_game"),
            _e("boxscore quarter scores", "QUEUED",
               "present in retained raw HTML; parse -> team-game score witness "
               "(independent of pfr_team_games) at game grain"),
            _e("boxscore scoring plays", "QUEUED",
               "present in retained raw HTML (scorer / passer / kicker names); parse -> "
               "event-grain witness for the scoring-bijection lane, an independent root "
               "against pfr_box_scoring"),
            _e("boxscore game statistics (team + player stat lines)", "QUEUED",
               "MEASURED 2026-07-26 by parsing a retained page: each boxscore page carries "
               "16 tables, of which 11 are data and we parsed exactly ONE (LINEUPS -> "
               "participation). Unparsed, per page, across ALL 151,623 retained pages: "
               "Score By Quarters, Scoring Plays, RUSHING (ATT/YDS/AVG/LG/TD), PASSING "
               "(ATT/COM/PCT/INT/YDS/AVG/LG/TD/TS/YL/RTG), RECEIVING, INTERCEPTIONS, "
               "PUNTING, PUNT RETURNS, KICKOFFS, KICKOFF RETURNS, SACKS. Two land on open "
               "program queues: the INTERCEPTIONS table is exactly what the source-hunt "
               "lane pfr_or_pfa_player_defensive_interception_gamelog_capture hunted and "
               "closed as ABANDONED-no-local-source -- the data was on our own disk the "
               "whole time; and SACKS bears on the sack_yards_lost/fum_rec burn-down "
               "class. Parse, not crawl"),
            _e("game context (venue, attendance, weather)", "QUEUED",
               "present in retained raw HTML; context-class witness vs pfr_box_game_info"),
            _e("team season rosters", "QUEUED",
               "DECLARED BUT NEVER RUN: harvest/source_catalog.yaml declares "
               "profootballarchives.team_season_roster (1919-2025) but it has no census "
               "seed range and is absent from the witness-census-all.yml matrix. An "
               "independently-compiled roster witness on the pfa_loc root, sitting "
               "unharvested behind config that already names it"),
            _e("player season pages", "QUEUED",
               "MEASURED 2026-07-26 (fetched players/p/poll01300.html): NINE tables per "
               "player -- Year/College/Participation; YEAR TEAM/NO/POS/GP/GS; "
               "Transaction/Date/Team; SCORING (TD,X1,X1A,X1%,X2,X2A,DX,FG,FGA,FG%,S,SAF,"
               "PTS); FIELD GOALS BY DISTANCE (1-19,20-29,30-39,40-49,50+,MADE,ATT,PCT,BL,"
               "LG); RUSHING (ATT,YDS,AVG,LG,TD,FD,20+,40+,FUM); PASSING (ATT,COM,COM%,INC,"
               "YDS,Y/ATT,Y/COM,LG,TD,TD%,INT,INT%,SK,YL,FD,20+); RECEIVING (TAR,REC,PCT,"
               "YDS,AVG,LG,TD,FD,20+,40+,YAC,FUM); PUNT RETURNS (NO,FC,YDS,AVG,LG,TD,20+,"
               "40+,FUM). VALUE, SCOPED BY THE SITE'S OWN COVERAGE TABLE (see the "
               "nflgamelogcoverage entry -- I initially over-claimed these as ancient): "
               "TARGETS are 1999+ so they add nothing pre-modern; FG DISTANCE BUCKETS, "
               "FIRST DOWNS and 20+/40+ explosive counts have no declared floor row and "
               "must be measured before any claim. The genuinely deep columns are "
               "SCORING (TD/X1/FG 1920+) and the per-season NO/POS/GP/GS appearance "
               "block"),
            _e("team season pages / season index", "QUEUED",
               "MEASURED 2026-07-26 (fetched 1963nfl.html): the season page carries a full "
               "STANDINGS table by conference (W,L,T,PCT,PF,PA,Home,Away) -- a team-record "
               "and points-for/against witness independent of the PFR game catalog, WITH "
               "home/away splits. nfl.html is the year index (1920-2025) that seeds it"),
            _e("transactions", "QUEUED",
               "CONFIRMED REAL 2026-07-26 by fetching a player page: PFA player pages carry "
               "a Transaction/Date/Team table (21 rows on poll01300). I had flagged this "
               "UNVERIFIED because it is not a NAV section -- that correction was itself "
               "wrong. LESSON: absence from a site nav is NOT absence from the site; "
               "families live INSIDE section pages"),
            _e("drafts", "QUEUED", "not crawled; same domain question as transactions"),
            _e("defunct leagues (AAFC, AAFL, ...) [site nav: Leagues]", "QUEUED",
               "same domain question as StatsCrew defunct leagues"),
            _e("nflgamelogcoverage.html -- THE SITE'S OWN COVERAGE DECLARATION", "QUEUED",
               "FETCHED 2026-07-26: a 50-row CATEGORY/STATISTIC/COMPLETE-COVERAGE table "
               "in which PFA states its own completeness per statistic. Verbatim rows "
               "include Touchdowns 1920-2024, One-point Conversions 1920-2024, XP "
               "Attempts 1936-2024, Two-point Conversions 1960-2024, 2PT ATTEMPTS "
               "GAPPED (1962, 1964-1966, 1968-1969, 1996-2024), Field Goals 1920-2024, "
               "FG Attempts 1960-2024, Safeties 1981-2024, RUSHING Attempts 1960-2024 "
               "but Yards 1960-2021, Long 1981-2024, PASSING all 1960-2024 with Times "
               "Sacked 1981-2021, RECEIVING Targets 1999-2024. THIS IS AN EXTERNAL "
               "SOURCE DECLARING ITS OWN AVAILABILITY FLOORS -- exactly the evidence "
               "the standing law demands before anyone states a floor. It also SCOPES "
               "every claim about PFA's value: capture it BEFORE budgeting the stat "
               "families, not after"),
            _e("nflrosterlimits.html", "QUEUED",
               "FETCHED 2026-07-26: 121 rows x 11 cols (Year, Lge, Active, Inactive, "
               "Practice Squad, Contract, Cutdown Dates, Trade Deadline, Free Agency, "
               "Injured Reserve, Notes) -- historical league ROSTER RULES by year. "
               "Feeds the §22 league-structure vector; nothing else we hold carries it"),
            _e("super-bowl.html / trainingcamps.html / officials / in-memoriam / "
               "hall-of-fame [site FOOTER, not nav]", "QUEUED",
               "FETCHED 2026-07-26: super-bowl.html = 60 rows (Game, Date, Visiting "
               "Team, Home Team, Location, Venue, Attendance); trainingcamps.html = 594 "
               "rows (Years, Facility/Location). Reached from the FOOTER, not the nav -- "
               "further proof that nav enumeration alone under-counts a site"),
            _e("statkey.html -- stat DEFINITIONS", "QUEUED",
               "FETCHED 2026-07-26: PFA's stat key page. Definition text, not a table. "
               "Bears directly on the open definition-drift questions "
               "(def_air_yards_allowed version, fum_rec own-vs-opponent semantics): an "
               "independent compiler stating what it means by each stat"),
            _e("coaches [site nav: Coaches]", "QUEUED",
               "MISSED BY THE HAND-AUTHORED TOC, caught 2026-07-26 by reading the "
               "site's own nav out of our retained pages. Likely EXCLUDED (no canonical "
               "stat column consumes coaching records, same reasoning as PFR) but that "
               "is an adjudication owed, not a silent omission"),
            _e("awards [site nav: Awards]", "QUEUED",
               "MISSED BY THE HAND-AUTHORED TOC. Likely EXCLUDED (editorial selections, "
               "not measured events) -- adjudication owed"),
            _e("leaderboards [site nav: Leaderboards]", "QUEUED",
               "MISSED BY THE HAND-AUTHORED TOC. Leaderboards are DERIVED rankings over "
               "the same season data, so likely EXCLUDED as a derived view -- but they "
               "double as a cheap totals cross-check and the call is owed"),
            _e("season index pages [site nav: Seasons]", "QUEUED",
               "MISSED BY THE HAND-AUTHORED TOC -- the per-season index that enumerates "
               "each year's games/teams; a structural completeness witness for the game "
               "catalog"),
        ],
    },

    "pfr": {
        "origin": "Pro-Football-Reference (+ Stathead, SAME root -- gap-filler only, "
                  "never a referee)",
        "capture_state": "raw/pfr/** (players, boxscores, context) + raw/stathead/generated",
        "capture_receipt": "54 registered sources spanning player-page season tables "
                           "(REG + POST), boxscore per-game tables, advanced 2018+ "
                           "tables, combine, snap counts, player index, team games, "
                           "master schedule",
        "toc": [
            _e("player-page season tables (REG)", "CAPTURED",
               "passing/rush-rec/rec-rush/kicking/punting/returns/scoring/defense/"
               "fantasy/games_played/adj_passing"),
            _e("player-page season tables (POST)", "CAPTURED",
               "7 POST authorities + games_played_playoffs registered"),
            _e("player-page advanced tables (2018+)", "CAPTURED",
               "adv_defense / adv_recrush / adv_rushrec / passing_advanced, REG + POST"),
            _e("boxscore per-game tables", "CAPTURED",
               "team_stats, scoring, kicking, returns, pbp, game_info, officials, "
               "home/vis drives+snaps+starters, offense/defense, advanced box, "
               "expected_points"),
            _e("combine", "CAPTURED", "context/combine + player-page combine"),
            _e("player index", "CAPTURED", "the scraped player universe"),
            _e("team games + master schedule", "CAPTURED",
               "nfl_team_games_all is the authoritative game catalog"),
            _e("play-by-play (Stathead-sourced merged corpus)", "CAPTURED",
               "pbp_merged_1978_2025 (2.1M plays) + rollups; PFR root <=1998, "
               "nflverse root 1999+ by the era-split contract"),
            _e("franchise / team pages", "EXCLUDED",
               "franchise identity is carried by the internal franchise registry keyed on "
               "team_fid (era-scoped aliases); PFR franchise pages restate that identity "
               "and contribute no stat atom of their own"),
            # ---- RE-DECLARED 2026-07-29. The reason below used to read "no canonical stat
            # column consumes awards or honors; they are editorial selections, not
            # measured events, so they cannot witness any atom." The first clause was
            # measurably FALSE and had been for as long as the contract registry has
            # existed: stat_contracts.v1.json registers FORTY-TWO `honors_bio` canonicals,
            # among them hof, mvp, dpoy, opoy, droy, oroy, cpoy, pro_bowl, probowls,
            # all_pro_first_team, all_pro_second_team, career_mvps, career_pro_bowls and
            # w_av. The second clause is true and does not rescue the first: an award IS an
            # editorial selection, and that is an argument about what KIND of witness it
            # can be, not about whether anything consumes it.
            #
            # Caught by refusal_preconditions.NO_CANONICAL_CONSUMER, which joins a claim
            # about the contract registry to the contract registry. Nothing had ever read
            # the two together, so a false reason held a capture out indefinitely.
            _e("awards / honors", "QUEUED",
               "42 `honors_bio` canonicals are registered and their populated source is "
               "today the PFR bio/awards scrape via player_bio; PFR's own award pages are "
               "the publisher's index of the same selections and are the natural capture "
               "for them. They are EDITORIAL selections rather than measured events, so "
               "they can never witness a measured atom -- their admissible role is "
               "identity/honors enrichment with PFR as the single root, and they must not "
               "be counted as a cross-examining witness for any stat"),
        ],
    },

    "nextgen_stats": {
        "origin": "nflverse-published NGS (2016+)",
        "capture_state": "raw/nextgen_stats/",
        "capture_receipt": "weekly raw + season published, 2016-2025",
        "toc": [
            _e("NGS weekly", "CAPTURED", "ngs_weekly_2016_2025", ["ngs_weekly_raw"]),
            _e("NGS season", "CAPTURED", "ngs_season_2016_2025", ["ngs_season_published"]),
            _e("NGS tracking primitives (raw player tracking)", "EXCLUDED",
               "not publicly published; only the derived model metrics are. Those are "
               "typed EXCLUDED:model_metric in the derivation DAG -- nothing sums, "
               "mirrors, or conserves them"),
        ],
    },

    "newspaper": {
        "origin": "Newspaper archives (LOC Chronicling America, newspapers.com, "
                  "Internet Archive) -- INDIVIDUAL PAPERS, image is the arbiter",
        "capture_state": "raw/newspaper_archives/ + curated/witnesses/newspaper/",
        "capture_receipt": "11 registered curated bundle sources + the raw archive "
                           "layer (newspaper_raw_archives, archive-of)",
        "law_b_note": "THE NEWSPAPER PROGRAM OWNS THIS CONTRACT. Capture completeness "
                      "here is unbounded by nature (every paper, every issue, every "
                      "game) and is NOT a defect counter -- entries stay QUEUED under "
                      "that program's own holds and are never arbitrated here.",
        "toc": [
            _e("player stat cells", "CAPTURED", "newspaper_player_cells (long grain)"),
            _e("team game stats + claims", "CAPTURED",
               "newspaper_team_stats / newspaper_team_claims"),
            _e("scoring events", "CAPTURED", "newspaper_scoring_events"),
            _e("narrative play-by-play", "CAPTURED", "newspaper_pbp_events"),
            _e("lineups / participation", "CAPTURED", "newspaper_lineups"),
            _e("game context + player notes", "CAPTURED",
               "newspaper_game_context / newspaper_player_notes"),
            _e("reviewer ledgers", "CAPTURED",
               "source_doc_notes / review_accepted / review_holds"),
            _e("LOC page + issue image captures", "CAPTURED",
               "raw/newspaper_archives/loc", ["newspaper_raw_archives"]),
            _e("Internet Archive captures", "CAPTURED",
               "source_horizon_internet_archive_captures (Tribune 1924)"),
            _e("remaining papers x issues x games", "QUEUED",
               "OWNED BY THE NEWSPAPER PROGRAM -- unbounded by nature; the 1920-40 "
               "coverage frontier is that program's queue, never a Law B defect here"),
        ],
    },
}


def build() -> dict:
    contracts = []
    total_toc = total_captured = total_excluded = total_queued = 0
    bad_status = []
    for source, c in sorted(CONTRACTS.items()):
        toc = c["toc"]
        cap = sum(1 for t in toc if t["status"] == "CAPTURED")
        exc = sum(1 for t in toc if t["status"] == "EXCLUDED")
        que = sum(1 for t in toc if t["status"] == "QUEUED")
        for t in toc:
            if t["status"] not in STATUS_VOCAB:
                bad_status.append(f"{source}:{t['toc_entry']}={t['status']}")
        total_toc += len(toc)
        total_captured += cap
        total_excluded += exc
        total_queued += que
        entry = {
            "source": source,
            "origin": c["origin"],
            "capture_state": c.get("capture_state"),
            "capture_receipt": c.get("capture_receipt"),
            "counters": {"toc_entries": len(toc), "captured": cap,
                         "excluded": exc, "queued": que,
                         "open": len(toc) - (cap + exc)},
            "toc": toc,
        }
        if c.get("law_b_note"):
            entry["law_b_note"] = c["law_b_note"]
        contracts.append(entry)

    return {
        "law": "Law B (CAPTURE): per source origin, the site's own table-of-contents vs "
               "what we hold. Counter = toc_entries - (captured + excluded); QUEUED "
               "entries keep it nonzero BY DESIGN. Registration does not close a "
               "source's census row while its capture contract has open TOC entries -- "
               "a partial capture registered as complete is the failure this law exists "
               "to prevent.",
        "counters": {
            "contracts": len(contracts),
            "toc_entries": total_toc,
            "captured": total_captured,
            "excluded": total_excluded,
            "capture_contract_open_toc_entries": total_queued,
            "invalid_status_rows": len(bad_status),
        },
        "invalid_status_rows": bad_status,
        "contracts": contracts,
    }


def open_entries() -> list[dict]:
    """The Law B queue: every uncaptured TOC entry, source-tagged."""
    return [{"source": c["source"], **t}
            for c in build()["contracts"] for t in c["toc"] if t["status"] == "QUEUED"]


def main() -> int:
    doc = build()
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counters"]
    print(f"contracts={c['contracts']}  toc={c['toc_entries']}  "
          f"captured={c['captured']}  excluded={c['excluded']}  "
          f"OPEN={c['capture_contract_open_toc_entries']}")
    for con in doc["contracts"]:
        k = con["counters"]
        print(f"  {con['source']:22s} toc={k['toc_entries']:3d} "
              f"captured={k['captured']:3d} excluded={k['excluded']:2d} "
              f"OPEN={k['queued']:3d}")
    print(f"contracts -> {os.path.abspath(SUMMARY_PATH)}")
    return 0 if c["invalid_status_rows"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
