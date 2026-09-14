"""
Build NFL Super Table - Unified historical + NFLverse data in MotherDuck

Combines:
- Kaggle historical data (1970-1998) - from formatted parquet or MotherDuck
- NFLverse data (1999-2025) - via combine_dst_to_nfl.py (offense + defense)

Creates a single unified table: nfl_historical.nfl_player_stats_all

Usage:
    python build_nfl_super_table.py
    python build_nfl_super_table.py --max-week 14  # Exclude incomplete weeks
    python build_nfl_super_table.py --skip-historical  # NFLverse only
    python build_nfl_super_table.py --fix-headshots  # Replace placeholder headshots with team logos
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

# Add paths for imports
sys.path.insert(0, str(SCRIPT_DIR / "multi_league" / "data_fetchers"))
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "multi_league" / "data_fetchers"))

OUTPUT_DIR = REPO_ROOT / "data" / "nflverse"
HISTORICAL_FILE = REPO_ROOT / "data" / "historical" / "historical_player_formatted.parquet"

# Import rank calculation functions
try:
    from fantasy_points_calculator import calculate_all_ranks, calculate_all_fantasy_points

    RANK_FUNCTIONS_AVAILABLE = True
except ImportError:
    RANK_FUNCTIONS_AVAILABLE = False
    calculate_all_ranks = None
    calculate_all_fantasy_points = None

try:
    from pbp_scoring_enrichment import enrich_with_pbp_scoring_rollup

    PBP_SCORING_ROLLUP_AVAILABLE = True
except ImportError:
    enrich_with_pbp_scoring_rollup = None
    PBP_SCORING_ROLLUP_AVAILABLE = False

try:
    from multi_league.data_fetchers.kicking_stat_guards import (
        KICKING_ZERO_COLUMNS,
        sanitize_non_kicker_kicking_leaks,
    )

    KICKING_STAT_GUARD_AVAILABLE = True
except ImportError:
    KICKING_ZERO_COLUMNS = ()
    sanitize_non_kicker_kicking_leaks = None
    KICKING_STAT_GUARD_AVAILABLE = False

try:
    from multi_league.data_fetchers.player_identity_guards import (
        apply_known_context_identity_rebuilds,
        apply_known_identity_repairs,
        apply_known_special_teams_identity_repairs,
        apply_known_stat_family_splits,
    )

    PLAYER_IDENTITY_GUARD_AVAILABLE = True
except ImportError:
    apply_known_context_identity_rebuilds = None
    apply_known_identity_repairs = None
    apply_known_special_teams_identity_repairs = None
    apply_known_stat_family_splits = None
    PLAYER_IDENTITY_GUARD_AVAILABLE = False

try:
    from multi_league.data_fetchers.pbp_schema_backfill import (
        apply_pbp_postseason_official_repairs,
        apply_pbp_schema_backfill,
        apply_pbp_truth_atom_overlays,
        build_pbp_safe_missing_rows,
    )

    PBP_SCHEMA_BACKFILL_AVAILABLE = True
except ImportError:
    apply_pbp_postseason_official_repairs = None
    apply_pbp_schema_backfill = None
    apply_pbp_truth_atom_overlays = None
    build_pbp_safe_missing_rows = None
    PBP_SCHEMA_BACKFILL_AVAILABLE = False

try:
    from multi_league.data_fetchers.pfr_supertable_backfill import apply_pfr_supertable_update_package

    PFR_SUPERTABLE_BACKFILL_AVAILABLE = True
except ImportError:
    apply_pfr_supertable_update_package = None
    PFR_SUPERTABLE_BACKFILL_AVAILABLE = False

# Note: JAX bug fix data is now applied in defense_stats.py

# NFL.com placeholder headshot - this exact URL is used for players without photos
NFLVERSE_PLACEHOLDER_URL = "https://static.www.nfl.com/image/private/f_auto,q_auto/league/y9boy7gxrajfspjq98cm"

# Manual headshot overrides for historical/missing players
# These are applied BEFORE team logo fallbacks
# Source: NFL.com CDN via nflverse players.parquet
HEADSHOT_OVERRIDES = {
    # Verified NFL.com CDN URLs from nflverse
    "Aaron Rodgers": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/dypvakakxhccxs67tb0y",
    "Adam Vinatieri": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/imfj1hl4kob4jof8hcwa",
    "Alvin Kamara": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/uay6uo9lonegelngihki",
    "Antonio Gates": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/kndulvysinvn4tpvt0bc",
    "Barry Sanders": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/pj2ldftirqvwh3c9pljr",
    "Ben Roethlisberger": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/vziogpnyp9d0xtpiuqhr",
    "Billy Cannon": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/billy_cannon.jpg",
    "Billy Cundiff": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/waqo64awyli6ahwmumqs",
    "Brett Favre": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/m3sggjgdqlp4jqpxktvr",
    "Cairo Santos": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/w5mfvjfuflbitwuji1yx",
    "Chris Boswell": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/rxbnhvnfjmheuryvdmuf",
    "Christian McCaffrey": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/resjdwihunsqckohcorj",
    "Clinton Portis": "https://wifebio.com/wp-content/uploads/2022/06/Clinton-Portis.jpg",
    "Colin Kaepernick": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/qgwfhbsrryni5fcxbl6a",
    "Corey Dillon": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/fkbrkwz0nztu20g3ljde",
    "Cris Carter": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/tb52fk83tmdpmbzwliuk",
    "Curtis Martin": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ikb6en50ga5g4wc8oekd",
    "Dan Marino": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/jk97vzadmthvdyu8mbr2",
    "Doug Martin": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/lhazttk4tjpi7ggf7du4",
    "Drew Brees": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/bt6nv11jgsirnuymookt",
    "Eli Manning": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/w51tf8uedjftbr4qpaex",
    "Emmitt Smith": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ub78gyoyi3x1wlaow49n",
    "Fran Tarkenton": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ihf6rynix853sp4e3sjp",
    "Frank Gore": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/gfuklcqmphmdy9ncwrdt",
    # "Gary Anderson" moved to HEADSHOT_OVERRIDES_BY_ID (K vs RB collision)
    "Greg Zuerlein": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/gzuks9rryzy90fecvi9l",
    "Harold Jackson": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/harold_jackson.jpg",
    "Isaac Bruce": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/txgl2lvmyu3m28cltnib",
    "Ja'Marr Chase": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/qya3dtjb5kgofcuj2tuw",
    "Jamal Lewis": "https://iv1.lisimg.com/image/30719967/740full-jamal-lewis.jpg",
    "Jamaal Charles": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/mdrlzgankwwjldxllgcx",
    "Jason Hanson": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/q9nslxhklckxgph3hwac",
    "Jason Myers": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/rnw5znsm0hrekqvztiog",
    "Jason Witten": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/r7uafhtwuexuaty0nhsc",
    "Jay Feely": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ncfkhjulzxlddszqfm2u",
    # "Jerry Butler" moved to HEADSHOT_OVERRIDES_BY_ID (WR vs RB collision)
    "Jerry Rice": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/g1vjcnbryx3nmg0ppsnl",
    "Jimmie Giles": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/u8xh5oxykc5bnchgrmpq",
    "Jimmy Graham": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/h7xs06oj6g03mippwtse",
    # "Jimmy Smith" moved to HEADSHOT_OVERRIDES_BY_ID (WR vs DB vs RB collision)
    "Joe Mixon": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/gwyhozfhjgedwdmmjjcf",
    "John Elway": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/qt8ysjlaemnfczfe9ffl",
    # "Josh Allen" moved to HEADSHOT_OVERRIDES_BY_ID (QB vs C collision)
    "Kellen Winslow": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/pw3hoatll5kkuc9v2gva",
    "Kyle Pitts": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/tbec213me3pszzszecqo",
    "LaDainian Tomlinson": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/fdraejwoowicuevalpne",
    # "Lamar Jackson" moved to HEADSHOT_OVERRIDES_BY_ID (QB vs DB collision)
    "Larry Fitzgerald": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/giswjsemt4f7ppwxtqgp",
    # "Marcus Allen" moved to HEADSHOT_OVERRIDES_BY_ID (RB HOF vs LB collision)
    "Marcus Robinson": "https://img.apmcdn.org/5c4af85565a07f0d0d3de32362f8caefe6c18979/uncropped/75fd64-20061224-robinsonm.jpg",
    "Mark Rypien": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ecuua46m2ktij2lyxfzd",
    "Marshall Faulk": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ker7p0exticomqhcbtjt",
    "Marvin Harrison": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/fni7z2tqhhaq5kbe5a0s",
    "Mason Crosby": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/usbwgcjsdcugjafgjzvt",
    "Matt Prater": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/pj981bi535y4jwy6skr1",
    "Matt Ryan": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/s6zam4ekrrnonsck6vak",
    "Matthew Stafford": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/svxl2pk2cuqf26zrhii4",
    "Mike Vick": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/hrg4riwhpjbijfovsjrk",
    "Morten Andersen": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/nk87h6cjuij14iofd5ix",
    "Nick Foles": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/dxuz2ir773wylutxo7zs",
    "Nick Folk": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/qcfiv2cc7ky37yq9iklp",
    "Patrick Mahomes": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/iireqbn32cpg9fn4sfy7",
    "Peyton Manning": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/xpcd6auodk3w0vda6jal",
    "Philip Rivers": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/voakyrtj34sghkwxaxxf",
    "Priest Holmes": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/dzul271lozkuowxqh9r7",
    "Qadry Ismail": "https://alchetron.com/cdn/qadry-ismail-83723fa3-6f9f-437b-9943-07e52222494-resize-750.jpeg",
    "Randy Moss": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/udotyrimbuirasxxkd8p",
    "Ray Rice": "https://d.ibtimes.com/en/full/1681330/ray-rice.jpg",
    "Rich Caster": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/g3qfwwhulljmz5xyxlz5",
    "Rob Bironas": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/rbrc5s06xlgtt1boiphv",
    "Rob Gronkowski": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/svpo7chliiymc0r7hegp",
    "Robbie Gould": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/uakpiwhnutg2rvoyrmgh",
    "Russell Wilson": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/vkstxj0evgkjiycu4glc",
    "Sebastian Janikowski": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/c5zvm3ux5uvwxdfqwd0w",
    "Shannon Sharpe": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/pl9dwxqbdarzuirkjeri",
    "Shaun Alexander": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/q6aogsngaiaq2yr1sgyv",
    "Stephen Gostkowski": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/nqi0psnge6kkglcspabo",
    "Steve Largent": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/uwi8mvpiavmova7skp7q",
    # "Steve Smith" moved to HEADSHOT_OVERRIDES_BY_ID (4 different players)
    "Terrell Owens": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/c41avnwoegxy9nnwmgbo",
    "Tim Brown": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/qhdz2r0h4zlxxmzsgemc",
    "Todd Christensen": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/ab7yhfyi0nbqfby476hm",
    "Tom Brady": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/q7dpdlxyu5rs05rgh1le",
    "Tony Dorsett": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/b6vvjgq2tpxrzhvwjt1y",
    "Tony Gonzalez": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/enbspvy0ahsyznomitre",
    "Travis Kelce": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/ebc5j60djpinc9elvlc5",
    "Trevor Lawrence": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/d75srntioly7fq0z8vip",
    "Tyreek Hill": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/gg73gnfivrn80oyqwazl",
    "Walter Payton": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/yjatyzmf5yhgs0v9qcze",
    "Warren Moon": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/nmnwarb1fy1d8xrefvfc",
    "Zach Ertz": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/xktmmeumuoglbuam75iz",
    # Pre-1970 / historical legends from GitHub-hosted assets
    "Art Powell": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/art_powell.jpg",
    "Bob Shaw": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/bob_shaw.jpg",
    "Cloyce Box": "https://2.bp.blogspot.com/--dTg8xbn1YA/VgUzrKZcJaI/AAAAAAAAKXw/vNiCCYrT2f4/s1600/Box_Cloyce1_Lions.jpg",
    "Cookie Gilchrist": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/cookie_gilchrist.jpg",
    "Dub Jones": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/dub_jones.jpg",
    "Frank Clarke": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/frank_clarke.jpg",
    "Jack Spikes": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/jack_spikes.png",
    "Jim Brown": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/jim_brown.avif",
    "Joe Kapp": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/joe_kapp.webp",
    "Josh Scobee": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/josh_scobee_espn.png",
    "Mike Ditka": "https://i.ytimg.com/vi/X_qD0sx0Vo8/maxresdefault.jpg",
    "Paul Hornung": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/paul_hornung.jpg",
    "Ray Mathews": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/ray_mathews.jpg",
    "Tom Tracy": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/tom_tracy.jpg",
    "Y.A. Tittle": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/ya_tittle.jpg",
}

# ID-based overrides for players with ambiguous names (multiple players share same name).
# Keyed by NFL_player_id (gsis_id) to guarantee correct player gets correct headshot.
# Source: nflverse players.csv + manually curated URLs.
HEADSHOT_OVERRIDES_BY_ID = {
    "BoxxCl00": "https://2.bp.blogspot.com/--dTg8xbn1YA/VgUzrKZcJaI/AAAAAAAAKXw/vNiCCYrT2f4/s1600/Box_Cloyce1_Lions.jpg",  # Cloyce Box
    # ── Josh Allen (QB BUF vs C TAM) ──
    "00-0034857": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/servs1fpsynfxep4rz2z",  # QB, BUF
    "00-0030833": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/vs1gvoadc6we63prkatk",  # C, TAM
    # ── Lamar Jackson (QB BAL vs DB NYJ/CHI/DEN) ──
    "00-0034796": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/cruqs6qpbykh7a2whd7p",  # QB, BAL
    "00-0036152": "https://static.www.nfl.com/image/upload/f_auto,q_auto/league/bybv5pnixp93qbs53zu5",  # DB, NYJ/CHI/DEN
    # ── Gary Anderson (K PIT/MIN vs RB SD/TB) ──
    "00-0000313": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/fofzz3dwrg1vr2drmoak",  # K, PIT/MIN
    "00-0000311": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/emszawiplqkjhy0rwgmi",  # RB, SD/TB
    # ── Marcus Allen (RB HOF vs LB PIT) ──
    "00-0000220": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/wy664ewj5sglsncnjakr",  # RB, RAI/KC (HOF)
    "00-0034335": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/vvpy4ouo24iqozvyp4rc",  # LB, PIT
    # ── Steve Smith (4 different players) ──
    "00-0020337": "https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/2622.png",  # WR, CAR/BAL (Sr.)
    "00-0025438": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/lpt6yae9uhke1nn69gyi",  # WR, NYG
    "00-0021346": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/kal3nvyehnm0xbm2qcfd",  # DB, JAX
    "00-0015306": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/vv0ivi8ovgf1dtq2kqhm",  # RB, OAK/SEA
    # ── Jimmy Smith (WR JAX vs DB BAL vs RB 1980s) ──
    "00-0015218": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/zgvlthadzlf0q6thnktj",  # WR, JAX
    "00-0027965": "https://static.www.nfl.com/image/private/f_auto,q_auto/league/uycb0wytmroih8dz8rag",  # DB, BAL
    # ── Jerry Butler (WR BUF 1979-86) ──
    "BUT311460": "https://raw.githubusercontent.com/jeleff1000/ff-assets/main/historical/jerry_butler.jpg",  # WR, BUF
    # BUT285505 (RB, ATL 1987) - not in nflverse, gets team logo on rebuild
}


def _csv_log(msg: str):
    """Print log message, safe for use at module load time before log() is defined."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_csv_headshot_overrides():
    """
    Load curated headshot overrides from the missing_headshots CSV.

    The CSV at assets/headshots/missing_headshots_optimal_lineup.csv contains
    manually researched headshot URLs for players missing from NFLverse.
    This function extracts unique NFL_player_id -> found_url pairs and
    merges them into HEADSHOT_OVERRIDES_BY_ID.

    Only uses rows where found_url is non-empty. For duplicate NFL_player_ids
    (same player appears on multiple teams/years), takes the first non-empty URL.
    CSV entries take precedence over existing HEADSHOT_OVERRIDES_BY_ID entries.
    """
    # CSV is at repo_root/assets/headshots/... - script is at repo_root/fantasy_football_data_scripts/nfl_data/
    csv_path = (
        Path(__file__).resolve().parent.parent.parent / "assets" / "headshots" / "missing_headshots_optimal_lineup.csv"
    )
    if not csv_path.exists():
        _csv_log(f"[HEADSHOT CSV] File not found: {csv_path}")
        return

    try:
        csv_df = pd.read_csv(csv_path)
    except Exception as e:
        _csv_log(f"[HEADSHOT CSV] Error reading CSV: {e}")
        return

    # Filter to rows with a non-empty found_url
    has_url = csv_df["found_url"].notna() & (csv_df["found_url"].str.strip() != "")
    url_rows = csv_df[has_url].copy()

    if url_rows.empty:
        _csv_log("[HEADSHOT CSV] No rows with found_url")
        return

    # Deduplicate: first non-empty found_url per NFL_player_id
    unique_overrides = url_rows.drop_duplicates(subset="NFL_player_id", keep="first")

    added = 0
    updated = 0
    for _, row in unique_overrides.iterrows():
        pid = str(row["NFL_player_id"]).strip()
        url = str(row["found_url"]).strip()
        if pid and url:
            if pid in HEADSHOT_OVERRIDES_BY_ID:
                updated += 1
            else:
                added += 1
            HEADSHOT_OVERRIDES_BY_ID[pid] = url

    _csv_log(
        f"[HEADSHOT CSV] Loaded {added + updated} overrides from CSV "
        f"({added} new, {updated} updated) from {len(unique_overrides)} unique players"
    )


# Load CSV overrides at module import time so they're available for all builds
load_csv_headshot_overrides()


def apply_dual_position_enrichments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply dual-position enrichments for players who played multiple positions.

    Uses nfl_player_id (not player name) to avoid cross-contamination between
    different players with the same name (e.g., Lamar Jackson QB vs DB,
    Gary Anderson RB vs K).

    Three-step approach:
    1. Data cleanup: Fix known stat contamination (e.g., Gary Anderson RB has
       kicking stats that belong to Gary Anderson K)
    2. Manual overrides (applied first so stat detection doesn't double-append):
       - Taysom Hill (00-0033357): TE,QB from 2019 onward
       - Travis Hunter (00-0040718): WR,DB from 2025 onward
    3. Stat-based K detection: Non-kickers with 4+ games of FG/XP attempts in a season
       get ',K' appended (e.g., Cookie Gilchrist RB,K, Paul Hornung RB,K)

    This replaces the old Cookie Gilchrist hack that overwrote position to 'K',
    which made him disappear from RB views.
    """
    id_col = "nfl_player_id" if "nfl_player_id" in df.columns else "NFL_player_id"
    if id_col not in df.columns or "position" not in df.columns:
        return df

    df = df.copy()

    # --- Step 0: Fix known data contamination ---
    # Gary Anderson RB (00-0000311) has kicking stats from Gary Anderson K (00-0000313)
    gary_rb_mask = df[id_col] == "00-0000311"
    if gary_rb_mask.any():
        kick_cols = [
            c
            for c in [
                *KICKING_ZERO_COLUMNS,
                "fg_att",
                "fg_made",
                "fg_missed",
                "fg_yards",
                "fg_long",
                "fga",
                "fgm",
                "pat_att",
                "pat_made",
                "pat_missed",
                "xpa",
                "xpm",
                "fg_a",
                "fg_m",
                "xp_a",
                "xp_m",
            ]
            if c in df.columns
        ]
        if kick_cols:
            df.loc[gary_rb_mask, kick_cols] = 0
            log(f"  Gary Anderson RB (00-0000311): zeroed kicking stats ({len(kick_cols)} cols)")

    # Determine which position columns to update (both position and nfl_position if available)
    pos_cols = ["position"]
    if "nfl_position" in df.columns:
        pos_cols.append("nfl_position")

    # --- Step 1: Manual overrides (by NFL_player_id) ---
    # Taysom Hill (00-0033357): TE,QB from 2019 onward (plays QB but listed as TE)
    mask_hill = (df[id_col] == "00-0033357") & (df["year"] >= 2019) & (df["position"] == "TE")
    if mask_hill.any():
        for col in pos_cols:
            df.loc[mask_hill, col] = "TE,QB"
        log(f"  Taysom Hill: set position=TE,QB for {mask_hill.sum()} rows (2019+)")

    # Travis Hunter (00-0040718): WR,DB from 2025 onward (two-way player)
    mask_hunter = (df[id_col] == "00-0040718") & (df["year"] >= 2025) & (df["position"] == "WR")
    if mask_hunter.any():
        for col in pos_cols:
            df.loc[mask_hunter, col] = "WR,DB"
        log(f"  Travis Hunter: set position=WR,DB for {mask_hunter.sum()} rows (2025+)")

    # --- Step 2: Stat-based K detection ---
    # Find non-K players with kicking stats (FG or XP attempts)
    fg_col = None
    for col in ["fg_att", "fga", "fg_a"]:
        if col in df.columns:
            fg_col = col
            break

    pat_col = None
    for col in ["pat_att", "xpa", "xp_a"]:
        if col in df.columns:
            pat_col = col
            break

    if fg_col or pat_col:
        # Build mask for games with any kicking attempt
        kick_mask = pd.Series(False, index=df.index)
        if fg_col:
            kick_mask = kick_mask | (df[fg_col].fillna(0) > 0)
        if pat_col:
            kick_mask = kick_mask | (df[pat_col].fillna(0) > 0)

        # Exclude players already tagged as K (single or in comma list)
        already_k = df["position"].fillna("").apply(lambda x: "K" in x.split(","))
        kick_mask = kick_mask & ~already_k

        if kick_mask.any():
            # Count qualifying games per (nfl_player_id, year) -- need 4+ games
            kick_games = df[kick_mask].groupby([id_col, "year"]).size().reset_index(name="kick_games")
            qualifying = kick_games[kick_games["kick_games"] >= 4]

            if not qualifying.empty:
                # Create a set of (nfl_player_id, year) tuples that qualify
                qual_set = set(zip(qualifying[id_col], qualifying["year"]))

                # Append ',K' to position for all rows in qualifying (nfl_player_id, year)
                qual_mask = df.apply(lambda r: (r[id_col], r["year"]) in qual_set, axis=1) & ~already_k

                if qual_mask.any():
                    for col in pos_cols:
                        df.loc[qual_mask, col] = df.loc[qual_mask, col] + ",K"
                    affected_players = df.loc[qual_mask, ["player", "year"]].drop_duplicates()
                    for _, row in affected_players.iterrows():
                        count = ((df["player"] == row["player"]) & (df["year"] == row["year"]) & qual_mask).sum()
                        log(f"  {row['player']}: appended ,K for {count} rows in {int(row['year'])}")

    return df


def merge_dual_position_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge duplicate player_week rows created by dual-position data sources.

    Some 1960s players (e.g., George Blanda, Cookie Gilchrist) have separate
    rows for their skill position and kicking stats from different Stathead
    parses (QB + K, RB + K).  After apply_dual_position_enrichments() tags
    the skill row as 'QB,K' or 'RB,K', the standalone K row is redundant.

    This merges kicking stats from K-only rows into the dual-position rows
    and drops the K-only duplicates, eliminating:
    - Duplicate weekly rows in the UI
    - Double-counted season aggregations (2× season rows for same player+year)

    Uses the same MAX-merge strategy as get_deduped_cte() in aggregate_nfl_stats.py,
    but applied at build time so all downstream consumers get clean data.
    """
    if "player_week" not in df.columns:
        return df

    # Find player_weeks that appear more than once
    pw_counts = df["player_week"].value_counts()
    dup_pws = set(pw_counts[pw_counts > 1].index)

    if not dup_pws:
        return df

    df = df.copy()

    # Pre-identify numeric columns once (avoid per-row dtype checks)
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    rows_to_drop = []
    merged_count = 0
    affected_players = {}  # {player_name: set_of_years}

    for pw in dup_pws:
        group_idx = df.index[df["player_week"] == pw]
        group = df.loc[group_idx]

        if len(group) < 2:
            continue

        # Find dual-position row (position contains comma) and K-only row(s)
        positions = group["position"].fillna("")
        dual_mask = positions.str.contains(",")
        k_only_mask = positions == "K"

        if not dual_mask.any() or not k_only_mask.any():
            continue  # Not a dual-position + K duplicate; leave for downstream dedup

        dual_idx = group_idx[dual_mask][0]

        # Merge ALL K-only rows into the dual-position row
        for ki in group_idx[k_only_mask]:
            for col in numeric_cols:
                k_val = df.at[ki, col]
                dual_val = df.at[dual_idx, col]
                # Copy K-row value if dual-row is missing/zero
                if pd.notna(k_val) and k_val != 0:
                    if pd.isna(dual_val) or dual_val == 0:
                        df.at[dual_idx, col] = k_val
            rows_to_drop.append(ki)
            merged_count += 1

        player_name = df.at[dual_idx, "player"] if "player" in df.columns else "Unknown"
        year = int(df.at[dual_idx, "year"]) if "year" in df.columns else 0
        affected_players.setdefault(player_name, set()).add(year)

    if rows_to_drop:
        df = df.drop(rows_to_drop).reset_index(drop=True)
        log(f"  Step 3: Merged {merged_count} duplicate player_week rows (K-only -> dual-position)")
        for name, years in sorted(affected_players.items()):
            year_range = f"{min(years)}-{max(years)}" if len(years) > 1 else str(min(years))
            log(f"    {name}: {len(years)} weeks merged ({year_range})")
    else:
        log("  Step 3: No duplicate player_week rows to merge")

    return df


# Team logo URLs from Wikipedia (reliable, no API blocking)
TEAM_LOGO_MAP = {
    "ARI": "https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Arizona_Cardinals_logo.svg/179px-Arizona_Cardinals_logo.svg.png",
    "ATL": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c5/Atlanta_Falcons_logo.svg/192px-Atlanta_Falcons_logo.svg.png",
    "BAL": "https://upload.wikimedia.org/wikipedia/en/thumb/1/16/Baltimore_Ravens_logo.svg/193px-Baltimore_Ravens_logo.svg.png",
    "BUF": "https://upload.wikimedia.org/wikipedia/en/thumb/7/77/Buffalo_Bills_logo.svg/189px-Buffalo_Bills_logo.svg.png",
    "CAR": "https://upload.wikimedia.org/wikipedia/en/thumb/1/1c/Carolina_Panthers_logo.svg/100px-Carolina_Panthers_logo.svg.png",
    "CHI": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Chicago_Bears_logo.svg/100px-Chicago_Bears_logo.svg.png",
    "CIN": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Cincinnati_Bengals_logo.svg/100px-Cincinnati_Bengals_logo.svg.png",
    "CLE": "https://upload.wikimedia.org/wikipedia/en/thumb/d/d9/Cleveland_Browns_logo.svg/100px-Cleveland_Browns_logo.svg.png",
    "DAL": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/15/Dallas_Cowboys.svg/100px-Dallas_Cowboys.svg.png",
    "DEN": "https://upload.wikimedia.org/wikipedia/en/thumb/4/44/Denver_Broncos_logo.svg/100px-Denver_Broncos_logo.svg.png",
    "DET": "https://upload.wikimedia.org/wikipedia/en/thumb/7/71/Detroit_Lions_logo.svg/100px-Detroit_Lions_logo.svg.png",
    "GB": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/Green_Bay_Packers_logo.svg/100px-Green_Bay_Packers_logo.svg.png",
    "HOU": "https://upload.wikimedia.org/wikipedia/en/thumb/2/28/Houston_Texans_logo.svg/100px-Houston_Texans_logo.svg.png",
    "IND": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Indianapolis_Colts_logo.svg/100px-Indianapolis_Colts_logo.svg.png",
    "JAX": "https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/100px-Jacksonville_Jaguars_logo.svg.png",
    "KC": "https://upload.wikimedia.org/wikipedia/en/thumb/e/e1/Kansas_City_Chiefs_logo.svg/100px-Kansas_City_Chiefs_logo.svg.png",
    "LV": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/100px-Las_Vegas_Raiders_logo.svg.png",
    "LAC": "https://upload.wikimedia.org/wikipedia/en/thumb/a/a6/Los_Angeles_Chargers_logo.svg/100px-Los_Angeles_Chargers_logo.svg.png",
    "LAR": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/100px-Los_Angeles_Rams_logo.svg.png",
    "MIA": "https://upload.wikimedia.org/wikipedia/en/thumb/3/37/Miami_Dolphins_logo.svg/100px-Miami_Dolphins_logo.svg.png",
    "MIN": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Minnesota_Vikings_logo.svg/98px-Minnesota_Vikings_logo.svg.png",
    "NE": "https://upload.wikimedia.org/wikipedia/en/thumb/b/b9/New_England_Patriots_logo.svg/100px-New_England_Patriots_logo.svg.png",
    "NO": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/New_Orleans_Saints_logo.svg/98px-New_Orleans_Saints_logo.svg.png",
    "NYG": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/New_York_Giants_logo.svg/100px-New_York_Giants_logo.svg.png",
    "NYJ": "https://upload.wikimedia.org/wikipedia/en/thumb/6/6b/New_York_Jets_logo.svg/100px-New_York_Jets_logo.svg.png",
    "PHI": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Philadelphia_Eagles_logo.svg/100px-Philadelphia_Eagles_logo.svg.png",
    "PIT": "https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/Pittsburgh_Steelers_logo.svg/100px-Pittsburgh_Steelers_logo.svg.png",
    "SF": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/San_Francisco_49ers_logo.svg/100px-San_Francisco_49ers_logo.svg.png",
    "SEA": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Seattle_Seahawks_logo.svg/100px-Seattle_Seahawks_logo.svg.png",
    "TB": "https://upload.wikimedia.org/wikipedia/en/thumb/a/a2/Tampa_Bay_Buccaneers_logo.svg/100px-Tampa_Bay_Buccaneers_logo.svg.png",
    "TEN": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c1/Tennessee_Titans_logo.svg/100px-Tennessee_Titans_logo.svg.png",
    "WAS": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/100px-Washington_Commanders_logo.svg.png",
    # Historical team abbreviations (relocated/renamed franchises)
    "SD": "https://upload.wikimedia.org/wikipedia/en/thumb/a/a6/Los_Angeles_Chargers_logo.svg/100px-Los_Angeles_Chargers_logo.svg.png",  # San Diego Chargers -> LAC
    "STL": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/100px-Los_Angeles_Rams_logo.svg.png",  # St. Louis Rams -> LAR
    "OAK": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/100px-Las_Vegas_Raiders_logo.svg.png",  # Oakland Raiders -> LV
    "LA": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/100px-Los_Angeles_Rams_logo.svg.png",  # LA Rams (pre-STL)
    "JAC": "https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/100px-Jacksonville_Jaguars_logo.svg.png",  # Jacksonville alt abbreviation
}

# DST logo map - maps "Team DST" player names to team logos
DST_LOGO_MAP = {
    "49ers DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/San_Francisco_49ers_logo.svg/100px-San_Francisco_49ers_logo.svg.png",
    "Bears DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Chicago_Bears_logo.svg/100px-Chicago_Bears_logo.svg.png",
    "Bengals DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Cincinnati_Bengals_logo.svg/100px-Cincinnati_Bengals_logo.svg.png",
    "Bills DST": "https://upload.wikimedia.org/wikipedia/en/thumb/7/77/Buffalo_Bills_logo.svg/189px-Buffalo_Bills_logo.svg.png",
    "Broncos DST": "https://upload.wikimedia.org/wikipedia/en/thumb/4/44/Denver_Broncos_logo.svg/100px-Denver_Broncos_logo.svg.png",
    "Browns DST": "https://upload.wikimedia.org/wikipedia/en/thumb/d/d9/Cleveland_Browns_logo.svg/100px-Cleveland_Browns_logo.svg.png",
    "Buccaneers DST": "https://upload.wikimedia.org/wikipedia/en/thumb/a/a2/Tampa_Bay_Buccaneers_logo.svg/100px-Tampa_Bay_Buccaneers_logo.svg.png",
    "Cardinals DST": "https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Arizona_Cardinals_logo.svg/179px-Arizona_Cardinals_logo.svg.png",
    "Chargers DST": "https://upload.wikimedia.org/wikipedia/en/thumb/7/72/NFL_Chargers_logo.svg/100px-NFL_Chargers_logo.svg.png",
    "Chiefs DST": "https://upload.wikimedia.org/wikipedia/en/thumb/e/e1/Kansas_City_Chiefs_logo.svg/100px-Kansas_City_Chiefs_logo.svg.png",
    "Colts DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Indianapolis_Colts_logo.svg/100px-Indianapolis_Colts_logo.svg.png",
    "Commanders DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/100px-Washington_Commanders_logo.svg.png",
    "Cowboys DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/15/Dallas_Cowboys.svg/100px-Dallas_Cowboys.svg.png",
    "Dolphins DST": "https://upload.wikimedia.org/wikipedia/en/thumb/3/37/Miami_Dolphins_logo.svg/100px-Miami_Dolphins_logo.svg.png",
    "Eagles DST": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Philadelphia_Eagles_logo.svg/100px-Philadelphia_Eagles_logo.svg.png",
    "Falcons DST": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c5/Atlanta_Falcons_logo.svg/192px-Atlanta_Falcons_logo.svg.png",
    "Giants DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/New_York_Giants_logo.svg/100px-New_York_Giants_logo.svg.png",
    "Jaguars DST": "https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/100px-Jacksonville_Jaguars_logo.svg.png",
    "Jets DST": "https://upload.wikimedia.org/wikipedia/en/thumb/6/6b/New_York_Jets_logo.svg/100px-New_York_Jets_logo.svg.png",
    "Lions DST": "https://upload.wikimedia.org/wikipedia/en/thumb/7/71/Detroit_Lions_logo.svg/100px-Detroit_Lions_logo.svg.png",
    "Packers DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/Green_Bay_Packers_logo.svg/100px-Green_Bay_Packers_logo.svg.png",
    "Panthers DST": "https://upload.wikimedia.org/wikipedia/en/thumb/1/1c/Carolina_Panthers_logo.svg/100px-Carolina_Panthers_logo.svg.png",
    "Patriots DST": "https://upload.wikimedia.org/wikipedia/en/thumb/b/b9/New_England_Patriots_logo.svg/100px-New_England_Patriots_logo.svg.png",
    "Raiders DST": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/150px-Las_Vegas_Raiders_logo.svg.png",
    "Rams DST": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/100px-Los_Angeles_Rams_logo.svg.png",
    "Ravens DST": "https://upload.wikimedia.org/wikipedia/en/thumb/1/16/Baltimore_Ravens_logo.svg/193px-Baltimore_Ravens_logo.svg.png",
    "Saints DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/New_Orleans_Saints_logo.svg/98px-New_Orleans_Saints_logo.svg.png",
    "Seahawks DST": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Seattle_Seahawks_logo.svg/100px-Seattle_Seahawks_logo.svg.png",
    "Steelers DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/Pittsburgh_Steelers_logo.svg/100px-Pittsburgh_Steelers_logo.svg.png",
    "Texans DST": "https://upload.wikimedia.org/wikipedia/en/thumb/2/28/Houston_Texans_logo.svg/100px-Houston_Texans_logo.svg.png",
    "Titans DST": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c1/Tennessee_Titans_logo.svg/100px-Tennessee_Titans_logo.svg.png",
    "Vikings DST": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Minnesota_Vikings_logo.svg/98px-Minnesota_Vikings_logo.svg.png",
    "Redskins DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/100px-Washington_Commanders_logo.svg.png",
    "Braves DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/100px-Washington_Commanders_logo.svg.png",
    "Football Team DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/100px-Washington_Commanders_logo.svg.png",
    "Washington DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/100px-Washington_Commanders_logo.svg.png",
    "Oilers DST": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c1/Tennessee_Titans_logo.svg/100px-Tennessee_Titans_logo.svg.png",
    "Staleys DST": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Chicago_Bears_logo.svg/100px-Chicago_Bears_logo.svg.png",
    "Spartans DST": "https://upload.wikimedia.org/wikipedia/en/thumb/7/71/Detroit_Lions_logo.svg/100px-Detroit_Lions_logo.svg.png",
    # Defunct NFL teams (from RetroSeasons.com)
    "Dodgers DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-AAFC-Football-Brooklyn-Dodgers.jpg",
    "Bulldogs DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-canton-bulldogs.jpg",
    "Yanks DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-york-yanks-1950-1951-nfl-photo.jpg",
    "Steam Roller DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-providence-steam-roller.png",
    "Yellow Jackets DST": "https://i1.wp.com/www.retroseasons.com/retroimages/000-logo-1929_Frankford.png",
    "Triangles DST": "https://i1.wp.com/www.retroseasons.com/retroimages/dayton-triangles-logo.png",
    "Panhandles DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-Columbus-Panhandles-2.jpg",
    "Pros DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-ol-akron-pros.png",
    "Dons DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-aafc-los-angeles-dons.jpg",
    "Badgers DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-nfl-milwaukee-badgers.png",
    "Rockets DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-aafc-chicago-rockets.jpg",
    "Tigers DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-ol-columbus-tigers.png",
    "Pottsville Maroons DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-Pottsville_Maroons.jpg",
    "Jeffersons DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-nfl-jeffs-jersey.jpg",
    "Eskimos DST": "https://i1.wp.com/www.retroseasons.com/retroimages/000-logo-duluth-eskino.jpg",
    "Stapletons DST": "https://i1.wp.com/www.retroseasons.com/retroimages/000-logo-nfl-staten-island-stapletons.png",
    "Hornets DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-aafc-chicago-hornets.jpg",
    "Oorang Indians DST": "https://i1.wp.com/www.retroseasons.com/retroimages/logo-oorang-indians.jpg",
    "Tornadoes DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-wppfc-orange-newark.png",
    "Indians DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-ClevelandIndiansLogo.jpg",
    "Maroons DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-Pottsville_Maroons.jpg",
    "Muncie Flyers DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-ol-muncie-flyers.png",
    "Tonawanda Kardex DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-Tonawanda-Kardex.jpg",
    "Brecks DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-1919-louisville-brecks.jpg",
    "Independents DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-nfl-hammond-pros.png",
    "Crimson Giants DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-nfl-evansville-crimson.png",
    "Legion DST": "https://i1.wp.com/www.retroseasons.com/retroimages/00-logo-aflg-brooklyn-horsemen.png",
}


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# Module-level cache for franchise_eras lookup: (franchise_id_int, year) -> logo_url
# Populated lazily on first call to get_def_logo_from_franchise_eras()
_franchise_eras_lookup: dict | None = None  # None = not yet loaded, {} = loaded but empty


def _load_franchise_eras_lookup() -> dict:
    """
    Load franchise_eras from MotherDuck into a nested lookup dict.

    Returns:
        dict mapping franchise_id (int) -> list of {'start_year', 'end_year', 'logo_url'}
        sorted by start_year ascending.  Returns {} on any error.
    """
    from multi_league.core.db_reader import get_reader

    try:
        reader = get_reader()
        rows = reader.query(
            "SELECT franchise_id, logo_url, start_year, end_year "
            "FROM nfl_historical.franchise_eras "
            "ORDER BY franchise_id, start_year",
            database="___ops",
        )

        lookup: dict = {}
        for row in rows:
            franchise_id = row["franchise_id"]
            logo_url = row["logo_url"]
            start_year = row["start_year"]
            end_year = row["end_year"]
            if franchise_id is None:
                continue
            fid = int(franchise_id)
            lookup.setdefault(fid, []).append(
                {
                    "logo_url": logo_url,
                    "start_year": int(start_year) if start_year is not None else None,
                    "end_year": int(end_year) if end_year is not None else None,
                }
            )

        log(f"  [franchise_eras] Loaded {len(lookup)} franchises from franchise_eras")
        return lookup

    except Exception as exc:
        log(f"  [franchise_eras] Warning: could not load franchise_eras ({exc}) — falling back to DST_LOGO_MAP")
        return {}


def get_def_logo_from_franchise_eras(nfl_player_id: str, year: int) -> "str | None":
    """
    Return the era-correct logo URL for a DEF entry using the franchise_eras table.

    Args:
        nfl_player_id: String of the form "DEF-{franchise_id}" (e.g. "DEF-15")
        year: NFL season year (int)

    Returns:
        logo_url string if found, None otherwise (caller should fall back to DST_LOGO_MAP)
    """
    global _franchise_eras_lookup

    # Lazy load on first call
    if _franchise_eras_lookup is None:
        _franchise_eras_lookup = _load_franchise_eras_lookup()

    if not nfl_player_id or not str(nfl_player_id).startswith("DEF-"):
        return None

    try:
        fid = int(str(nfl_player_id).split("-", 1)[1])
    except (ValueError, IndexError):
        return None

    eras = _franchise_eras_lookup.get(fid)
    if not eras:
        return None

    # Find the era that covers this year (end_year=None means "present")
    for era in eras:
        s = era["start_year"]
        e = era["end_year"]
        if s is None:
            continue
        if year >= s and (e is None or year <= e):
            return era["logo_url"]  # May be None if era has no logo yet

    # Year is before the first era — use the earliest era's logo as a best-effort fallback
    first_logo = eras[0]["logo_url"]
    return first_logo


def replace_placeholder_headshots(
    df: pd.DataFrame,
    headshot_col: str = "headshot_url",
    team_col: str = "nfl_team",
    player_id_col: str = "nfl_player_id",
    player_name_col: str = "player",
) -> pd.DataFrame:
    """
    Fill MISSING headshots using manual overrides, canonical photos, then team logos.

    Strategy:
    1. Apply HEADSHOT_OVERRIDES for known historical players (by name)
    2. For each player, find ANY existing headshot they have (canonical photo)
    3. Apply that headshot to ALL their records (across all teams/years)
    4. Only fall back to team logo for players with NO headshots anywhere

    This means Randy Moss keeps his real photo across Vikings, Patriots, Titans, etc.
    Only players with zero photos anywhere get team logos.

    Args:
        df: DataFrame with player data
        headshot_col: Name of the headshot URL column
        team_col: Name of the team column
        player_id_col: Column to identify unique players
        player_name_col: Column with player names (for override matching)

    Returns:
        DataFrame with headshots filled
    """
    if headshot_col not in df.columns:
        log(f"No {headshot_col} column found, skipping headshot replacement")
        return df

    if player_id_col not in df.columns:
        log(f"No {player_id_col} column found, skipping headshot replacement")
        return df

    df = df.copy()

    # Step 0: Apply manual HEADSHOT_OVERRIDES unconditionally
    # If a player is in the override dict, always use the override URL.
    # This ensures manually curated URLs survive super_table rebuilds
    # even when nflverse provides a placeholder or outdated URL.
    if player_name_col in df.columns and HEADSHOT_OVERRIDES:
        override_mask = df[player_name_col].isin(HEADSHOT_OVERRIDES.keys())
        if override_mask.any():
            df.loc[override_mask, headshot_col] = df.loc[override_mask, player_name_col].map(HEADSHOT_OVERRIDES)
            log(f"  Applied {override_mask.sum():,} manual headshot overrides (by name)")

    # Step 0b: Apply ID-based overrides (takes precedence over name-based)
    # This handles players with ambiguous names (e.g., Josh Allen QB vs Josh Allen C)
    if player_id_col in df.columns and HEADSHOT_OVERRIDES_BY_ID:
        id_override_mask = df[player_id_col].isin(HEADSHOT_OVERRIDES_BY_ID.keys())
        if id_override_mask.any():
            df.loc[id_override_mask, headshot_col] = df.loc[id_override_mask, player_id_col].map(
                HEADSHOT_OVERRIDES_BY_ID
            )
            log(f"  Applied {id_override_mask.sum():,} manual headshot overrides (by ID)")

    # Identify records with valid headshots
    has_headshot = df[headshot_col].notna() & (df[headshot_col] != "")

    # Skip DEF position (team defenses handled separately)
    is_player = df["position"] != "DEF" if "position" in df.columns else True

    # Step 1: Build canonical headshot lookup per player
    # Use the first non-null headshot found for each player
    player_headshots = df[has_headshot & is_player].groupby(player_id_col)[headshot_col].first().to_dict()

    log(f"Found canonical headshots for {len(player_headshots):,} players")

    # Step 2: Apply canonical headshot to missing records
    needs_headshot = ~has_headshot & is_player
    initial_missing = needs_headshot.sum()

    if initial_missing == 0:
        log("No headshots to replace")
        return df

    log(f"Found {initial_missing:,} records with missing headshots")

    # Apply canonical headshot where available
    df.loc[needs_headshot, headshot_col] = df.loc[needs_headshot, player_id_col].map(player_headshots)

    # Count how many got their canonical photo
    got_canonical = needs_headshot & df[headshot_col].notna() & (df[headshot_col] != "")
    log(f"  Applied canonical headshots to {got_canonical.sum():,} records")

    # Step 3: For remaining (no canonical photo anywhere), use team logo
    still_missing = needs_headshot & (df[headshot_col].isna() | (df[headshot_col] == ""))

    if still_missing.any() and team_col in df.columns:
        df.loc[still_missing, headshot_col] = df.loc[still_missing, team_col].map(TEAM_LOGO_MAP)
        got_logo = still_missing & df[headshot_col].notna() & (df[headshot_col] != "")
        log(f"  Applied team logos to {got_logo.sum():,} records (no canonical photo)")

        # Check for any we couldn't fill
        final_missing = still_missing & (df[headshot_col].isna() | (df[headshot_col] == ""))
        if final_missing.any():
            missing_teams = df.loc[final_missing, team_col].unique()
            log(f"  Warning: {final_missing.sum():,} records still missing headshots")
            log(f"    Unknown teams: {list(missing_teams)[:10]}")

    # Step 4: Handle DEF position (team defenses)
    # Primary: era-correct logo from franchise_eras (keyed by nfl_player_id + year)
    # Fallback: DST_LOGO_MAP keyed by player name (nickname-based, not era-aware)
    if "position" in df.columns:
        is_dst = df["position"] == "DEF"
        dst_needs_headshot = is_dst & (df[headshot_col].isna() | (df[headshot_col] == ""))
        if dst_needs_headshot.any():
            dst_df = df.loc[dst_needs_headshot].copy()

            # Try franchise_eras first (era-correct logos)
            if player_id_col in dst_df.columns and "year" in dst_df.columns:
                era_logos = dst_df.apply(
                    lambda row: get_def_logo_from_franchise_eras(row[player_id_col], int(row["year"]))
                    if pd.notna(row.get("year"))
                    else None,
                    axis=1,
                )
                era_filled = era_logos.notna() & (era_logos != "")
                if era_filled.any():
                    # Use index-aligned assignment to avoid positional misalignment
                    filled_idx = era_logos[era_filled].index
                    df.loc[filled_idx, headshot_col] = era_logos[era_filled]
                    log(f"  Applied franchise_eras logos to {era_filled.sum():,} DEF records")

                # Identify DEF rows still missing after franchise_eras attempt
                dst_still_missing = is_dst & (df[headshot_col].isna() | (df[headshot_col] == ""))
            else:
                dst_still_missing = dst_needs_headshot

            # Fallback: DST_LOGO_MAP by player name (handles historical teams not in franchise_eras)
            if dst_still_missing.any() and player_name_col in df.columns and DST_LOGO_MAP:
                df.loc[dst_still_missing, headshot_col] = df.loc[dst_still_missing, player_name_col].map(DST_LOGO_MAP)
                got_dst_logo = dst_still_missing & df[headshot_col].notna() & (df[headshot_col] != "")
                log(f"  Applied DST_LOGO_MAP fallback to {got_dst_logo.sum():,} DEF records")

    return df


def load_nflverse_data(start_year: int, end_year: int, max_week: int = None) -> pd.DataFrame:
    """
    Load NFLverse data using combine_dst_to_nfl module.

    This uses the existing data fetchers with caching, retry logic, and proper
    column standardization.

    Args:
        start_year: First year to fetch (e.g., 1999)
        end_year: Last year to fetch (e.g., 2025)
        max_week: Maximum week for current year (to exclude incomplete weeks)

    Returns:
        Combined DataFrame with offense + defense stats
    """
    try:
        from combine_dst_to_nfl import load_offense_year, load_defense_year
        from combine_dst_to_nfl import offense_fix_player, defense_to_player_shape
        from combine_dst_to_nfl import coerce_stringy_cols, stack_union
    except ImportError:
        log("ERROR: Could not import combine_dst_to_nfl module")
        log("Make sure the script is in fantasy_football_data_scripts directory")
        raise

    current_year = datetime.now().year
    offense_frames = []
    defense_frames = []

    for year in range(start_year, end_year + 1):
        try:
            log(f"Loading {year} offense...")
            off_df = load_offense_year(year, None, None)

            # Filter max week for current year
            if year == current_year and max_week and "week" in off_df.columns:
                off_df = off_df[off_df["week"] <= max_week]
                log(f"  Filtered offense to week <= {max_week}: {len(off_df):,} records")

            offense_frames.append(off_df)

            log(f"Loading {year} defense...")
            def_df = load_defense_year(year, None, None)

            # Filter max week for current year
            if year == current_year and max_week and "week" in def_df.columns:
                def_df = def_df[def_df["week"] <= max_week]
                log(f"  Filtered defense to week <= {max_week}: {len(def_df):,} records")

            defense_frames.append(def_df)

        except ValueError as e:
            log(f"  Skipping {year}: {e}")
        except Exception as e:
            log(f"  Error loading {year}: {e}")

    if not offense_frames:
        raise RuntimeError("No offense data loaded")
    if not defense_frames:
        raise RuntimeError("No defense data loaded")

    # Combine all years
    offense_all = pd.concat(offense_frames, ignore_index=True)
    defense_all = pd.concat(defense_frames, ignore_index=True)
    raw_offense_for_kicking_guard = offense_all.copy()

    log(f"Total offense records: {len(offense_all):,}")
    log(f"Total defense records: {len(defense_all):,}")

    # Normalize naming columns
    offense_all = offense_fix_player(offense_all)
    defense_as_players = defense_to_player_shape(defense_all)

    # Parquet safety for list-ish columns
    offense_all = coerce_stringy_cols(offense_all)
    defense_as_players = coerce_stringy_cols(defense_as_players)

    # Stack with union schema
    merged = stack_union(offense_all, defense_as_players)
    if KICKING_STAT_GUARD_AVAILABLE:
        merged = sanitize_non_kicker_kicking_leaks(
            merged,
            source_df=raw_offense_for_kicking_guard,
            min_year=1999,
            log_fn=log,
        )
    else:
        log("WARNING: Kicking stat guard unavailable; non-kicker kicking leak check skipped")

    # Add data source marker
    merged["data_source"] = "nflverse"

    # NFLverse data is considered clean (data_quality_flag='ok')
    merged["data_quality_flag"] = "ok"

    return merged


def upload_to_motherduck(df: pd.DataFrame, table_name: str, database: str = "___ops", schema: str = "nfl_historical"):
    """Stub: ___ops uploads are broken pending the Sep 2026 unified rebuild.

    See memory entry 'NFL ops rebuild plan' for the replacement design.
    """
    raise NotImplementedError(
        "___ops upload broken since MotherDuck dropped 2026-04-24. "
        "The pre-existing Fly path here would have nuked all of ___ops by calling "
        "FlyTarget.replace_database with a single-table file. Will be rebuilt as a "
        "unified build_full_ops.py before next NFL season — see memory entry "
        "'NFL ops rebuild plan'."
    )


def load_historical_data(historical_file: Path = None) -> pd.DataFrame:
    """
    Load historical data from local parquet file or MotherDuck.

    Args:
        historical_file: Path to local historical parquet file

    Returns:
        DataFrame with historical player stats (1970-1998)
    """
    historical_df = pd.DataFrame()

    # Try local file first
    if historical_file and historical_file.exists():
        log(f"Loading historical data from: {historical_file}")
        historical_df = pd.read_parquet(historical_file)
        log(f"Loaded {len(historical_df):,} historical records")
        return historical_df

    # Fallback to MotherDuck
    log("Local historical file not found, trying MotherDuck...")
    from multi_league.core.db_reader import get_reader

    try:
        reader = get_reader()
        historical_df = reader.query_df(
            "SELECT * FROM nfl_historical.historical_player_stats WHERE year < 1999",
            database="___ops",
        )
        log(f"Loaded {len(historical_df):,} historical records from MotherDuck")
    except Exception as e:
        log(f"Warning: Could not load historical data from MotherDuck: {e}")

    return historical_df


def backfill_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill columns derivable from existing data for historical rows.

    NFLverse data (1999+) already has these from defense_stats.py,
    but historical data (pre-1999) needs them derived from raw columns.

    1. pts_allow_* buckets from pts_allow (pre-1999 DEF rows)
    2. dst_points_allowed from pts_allow (pre-1999 DEF rows)
    3. pts_pick6 from PBP data (2016+ QBs, if PBP files available)
    """
    fixes = 0

    # --- pts_allow_* buckets ---
    bucket_cols = [
        "pts_allow_0",
        "pts_allow_1_6",
        "pts_allow_7_13",
        "pts_allow_14_20",
        "pts_allow_21_27",
        "pts_allow_28_34",
        "pts_allow_35_plus",
    ]
    # Ensure columns exist
    for col in bucket_cols:
        if col not in df.columns:
            df[col] = np.nan

    needs_buckets = (df["position"] == "DEF") & df["pts_allow"].notna() & df["pts_allow_0"].isna()
    n_buckets = needs_buckets.sum()
    if n_buckets > 0:
        pts = df.loc[needs_buckets, "pts_allow"]
        df.loc[needs_buckets, "pts_allow_0"] = (pts == 0).astype(int)
        df.loc[needs_buckets, "pts_allow_1_6"] = ((pts >= 1) & (pts <= 6)).astype(int)
        df.loc[needs_buckets, "pts_allow_7_13"] = ((pts >= 7) & (pts <= 13)).astype(int)
        df.loc[needs_buckets, "pts_allow_14_20"] = ((pts >= 14) & (pts <= 20)).astype(int)
        df.loc[needs_buckets, "pts_allow_21_27"] = ((pts >= 21) & (pts <= 27)).astype(int)
        df.loc[needs_buckets, "pts_allow_28_34"] = ((pts >= 28) & (pts <= 34)).astype(int)
        df.loc[needs_buckets, "pts_allow_35_plus"] = (pts >= 35).astype(int)
        log(f"  pts_allow_* buckets: {n_buckets:,} DEF rows backfilled")
        fixes += n_buckets

    # --- dst_points_allowed from pts_allow ---
    if "dst_points_allowed" not in df.columns:
        df["dst_points_allowed"] = np.nan

    needs_dst_pa = (df["position"] == "DEF") & df["pts_allow"].notna() & df["dst_points_allowed"].isna()
    n_dst = needs_dst_pa.sum()
    if n_dst > 0:
        df.loc[needs_dst_pa, "dst_points_allowed"] = df.loc[needs_dst_pa, "pts_allow"]
        log(f"  dst_points_allowed: {n_dst:,} DEF rows backfilled from pts_allow")
        fixes += n_dst

    # --- pts_pick6 from PBP (2016+) ---
    pbp_dir = Path("C:/Users/joeye/Downloads")
    needs_pick6 = (
        (df["position"] == "QB")
        & (df["year"] >= 2016)
        & df["pts_pick6"].isna()
        & df["passing_interceptions"].notna()
        & (df["passing_interceptions"] > 0)
    )
    if needs_pick6.sum() > 0:
        try:
            import duckdb as _duckdb

            available_years = [yr for yr in range(2016, 2026) if (pbp_dir / f"play_by_play_{yr}.parquet").exists()]
            if available_years:
                local = _duckdb.connect()
                all_pick6 = []
                for yr in available_years:
                    pbp_file = pbp_dir / f"play_by_play_{yr}.parquet"
                    pick6_df = local.execute(f"""
                        SELECT passer_player_id as pid, season as year, week,
                               COUNT(*) as pick6_count
                        FROM '{pbp_file}'
                        WHERE interception = 1 AND return_touchdown = 1
                          AND passer_player_id IS NOT NULL
                        GROUP BY passer_player_id, season, week
                    """).fetchdf()
                    if not pick6_df.empty:
                        all_pick6.append(pick6_df)
                local.close()

                if all_pick6:
                    pick6_all = pd.concat(all_pick6, ignore_index=True)
                    pick6_all["player_week"] = (
                        pick6_all["pid"].astype(str)
                        + "_"
                        + pick6_all["year"].astype(int).astype(str)
                        + "_"
                        + pick6_all["week"].astype(int).astype(str)
                    )
                    pick6_map = pick6_all.set_index("player_week")["pick6_count"].to_dict()

                    if "player_week" in df.columns:
                        pw_mask = needs_pick6 & df["player_week"].notna()
                        df.loc[pw_mask, "pts_pick6"] = df.loc[pw_mask, "player_week"].map(pick6_map)
                        # Set remaining QBs with INTs but no pick-6 to 0
                        still_null = (
                            (df["position"] == "QB")
                            & (df["year"] >= 2016)
                            & df["pts_pick6"].isna()
                            & df["passing_interceptions"].notna()
                            & (df["passing_interceptions"] > 0)
                        )
                        df.loc[still_null, "pts_pick6"] = 0
                        n_pick6 = len(pick6_all)
                        log(f"  pts_pick6: {n_pick6} QB-weeks with pick-6s from PBP ({len(available_years)} years)")
                        fixes += n_pick6
        except Exception as e:
            log(f"  pts_pick6: PBP backfill skipped ({e})")

    if fixes == 0:
        log("  No derived columns needed backfilling")

    return df


def populate_fg_made_60_plus_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Populate ``fg_made_60_plus_canonical`` — a stable downstream contract column.

    NFLverse uses the trailing-underscore name ``fg_made_60_`` for 60+ yard field
    goals made; ``fg_made_60_plus`` is absent.  This function creates a canonical
    alias so downstream scoring code never needs to probe for the raw NFLverse name.

    Formula: COALESCE(fg_made_60_, 0)  — trivial 1:1 copy, zero-fills NaN.

    See: docs/superpowers/findings/2026-05-02-fg-made-60-canonical.md
    L1.c Phase 0.1
    """
    # L1.c Phase 0.1: canonical 60+ FG col.
    # fg_made_60_plus is absent from super_table; only fg_made_60_ (NFLverse
    # trailing-underscore convention) exists. Canonical preserves a stable
    # downstream contract; insulates from any future NFLverse rename.
    # See: docs/superpowers/findings/2026-05-02-fg-made-60-canonical.md
    df["fg_made_60_plus_canonical"] = df.get("fg_made_60_", pd.Series(0, index=df.index)).fillna(0)
    return df


def populate_fg_yards_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Populate fg_yards_canonical via 3-stage fallback.

    Stage 1: fg_made_distance > 0, else fg_yards > 0 -> use actual made-FG distance.
    Stage 2: actual distance = 0, buckets > 0 -> bucket-midpoint sum
             (17/25/35/45/54/62 for the 6 distance buckets).
    Stage 3: all distance + buckets = 0, fg_made > 0 -> fg_made * 35.

    See: docs/superpowers/findings/2026-05-02-fg-yards-canonical-design.md
    L1.c Phase 0.3
    """
    fg_made_distance = pd.to_numeric(df.get("fg_made_distance", pd.Series(0, index=df.index)), errors="coerce").fillna(
        0
    )
    fg_yards_actual = pd.to_numeric(df.get("fg_yards", pd.Series(0, index=df.index)), errors="coerce").fillna(0)
    fg_actual_distance = fg_made_distance.where(fg_made_distance > 0, fg_yards_actual)
    fg_made = df.get("fg_made", pd.Series(0, index=df.index)).fillna(0)
    fg_0_19 = df.get("fg_made_0_19", pd.Series(0, index=df.index)).fillna(0)
    fg_20_29 = df.get("fg_made_20_29", pd.Series(0, index=df.index)).fillna(0)
    fg_30_39 = df.get("fg_made_30_39", pd.Series(0, index=df.index)).fillna(0)
    fg_40_49 = df.get("fg_made_40_49", pd.Series(0, index=df.index)).fillna(0)
    fg_50_59 = df.get("fg_made_50_59", pd.Series(0, index=df.index)).fillna(0)
    fg_60_plus = df.get("fg_made_60_plus_canonical", pd.Series(0, index=df.index)).fillna(0)

    bucket_sum_yards = fg_0_19 * 17 + fg_20_29 * 25 + fg_30_39 * 35 + fg_40_49 * 45 + fg_50_59 * 54 + fg_60_plus * 62

    has_distance = fg_actual_distance > 0
    has_buckets = bucket_sum_yards > 0

    df["fg_yards_canonical"] = fg_actual_distance.where(
        has_distance,
        bucket_sum_yards.where(has_buckets, fg_made * 35),
    )
    return df


def populate_fg_yards_over_30_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Populate fg_yards_over_30_canonical (Sleeper "yards over 30" scoring).

    Stage 1: fg_made_distance/fg_yards > 0 -> fg_yards_canonical - sub-30 bucket-midpoint estimate.
    Stage 1 simplified: if no sub-30 buckets, entire fg_yards_canonical is over 30.
    Stage 2: fg_made_distance = 0, buckets populated -> sum of 30+ buckets at midpoints.
    Stage 3: pre-bucket era -> MAX(fg_yards_canonical - fg_made * 30, 0).

    Requires fg_yards_canonical to be populated FIRST.

    See: docs/superpowers/findings/2026-05-02-fg-yards-canonical-design.md
    L1.c Phase 0.3
    """
    fg_made_distance = pd.to_numeric(df.get("fg_made_distance", pd.Series(0, index=df.index)), errors="coerce").fillna(
        0
    )
    fg_yards_actual = pd.to_numeric(df.get("fg_yards", pd.Series(0, index=df.index)), errors="coerce").fillna(0)
    fg_actual_distance = fg_made_distance.where(fg_made_distance > 0, fg_yards_actual)
    fg_made = df.get("fg_made", pd.Series(0, index=df.index)).fillna(0)
    fg_yards = df.get("fg_yards_canonical", pd.Series(0, index=df.index)).fillna(0)
    fg_0_19 = df.get("fg_made_0_19", pd.Series(0, index=df.index)).fillna(0)
    fg_20_29 = df.get("fg_made_20_29", pd.Series(0, index=df.index)).fillna(0)
    fg_30_39 = df.get("fg_made_30_39", pd.Series(0, index=df.index)).fillna(0)
    fg_40_49 = df.get("fg_made_40_49", pd.Series(0, index=df.index)).fillna(0)
    fg_50_59 = df.get("fg_made_50_59", pd.Series(0, index=df.index)).fillna(0)
    fg_60_plus = df.get("fg_made_60_plus_canonical", pd.Series(0, index=df.index)).fillna(0)

    sub_30_estimate = fg_0_19 * 17 + fg_20_29 * 25
    bucket_30_plus_yards = fg_30_39 * 35 + fg_40_49 * 45 + fg_50_59 * 54 + fg_60_plus * 62

    has_distance = fg_actual_distance > 0
    has_buckets_30_plus = bucket_30_plus_yards > 0
    # Floor at 0: in pre-modern data, sub-30 bucket midpoints can over-estimate
    # the actual sub-30 yardage and push (fg_yards - sub_30_estimate) negative.
    # Same defensive floor as Stage 3.
    stage_1_estimate = (fg_yards - sub_30_estimate).clip(lower=0)
    stage_3_estimate = (fg_yards - fg_made * 30).clip(lower=0)

    df["fg_yards_over_30_canonical"] = stage_1_estimate.where(
        has_distance,
        bucket_30_plus_yards.where(has_buckets_30_plus, stage_3_estimate),
    )
    return df


def main():
    parser = argparse.ArgumentParser(description="Build NFL Super Table in MotherDuck")
    parser.add_argument("--start-year", type=int, default=1999, help="Start year for NFLverse data")
    parser.add_argument("--end-year", type=int, default=2025, help="End year for NFLverse data")
    parser.add_argument("--max-week", type=int, default=14, help="Max week for current year (exclude partial)")
    parser.add_argument("--output-dir", type=str, help="Output directory for parquet files")
    parser.add_argument("--skip-upload", action="store_true", help="Skip MotherDuck upload")
    parser.add_argument("--skip-historical", action="store_true", help="Skip historical data (NFLverse only)")
    parser.add_argument("--historical-file", type=str, help="Path to historical parquet file")
    parser.add_argument(
        "--fix-headshots", action="store_true", help="Replace NFL.com placeholder headshots with team logos"
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    historical_file = Path(args.historical_file) if args.historical_file else HISTORICAL_FILE

    log("=" * 70)
    log("NFL Super Table Builder")
    log("=" * 70)
    log(f"NFLverse years: {args.start_year} - {args.end_year}")
    log(f"Max week for current year: {args.max_week}")
    log(f"Output: {output_dir}")
    log(f"Historical file: {historical_file}")
    log(f"Fix headshots: {args.fix_headshots}")
    log("")

    # Step 1: Fetch all NFLverse data using combine_dst_to_nfl
    log("STEP 1: Fetching NFLverse data (offense + defense)...")
    log("-" * 50)

    nflverse_combined = load_nflverse_data(start_year=args.start_year, end_year=args.end_year, max_week=args.max_week)

    log(f"\nTotal NFLverse records: {len(nflverse_combined):,}")

    # Save NFLverse data locally
    nflverse_file = output_dir / f"nflverse_player_stats_{args.start_year}_{args.end_year}.parquet"
    nflverse_combined.to_parquet(nflverse_file, index=False)
    log(f"Saved: {nflverse_file}")

    # Step 2: Load historical Kaggle data
    log("\nSTEP 2: Loading historical Kaggle data...")
    log("-" * 50)

    historical_df = pd.DataFrame()
    if not args.skip_historical:
        historical_df = load_historical_data(historical_file)
    else:
        log("Skipping historical data (--skip-historical)")

    # Step 3: Combine into super table
    log("\nSTEP 3: Building super table...")
    log("-" * 50)

    if not historical_df.empty:
        # Align columns between historical and NFLverse using reindex (faster than adding columns individually)
        all_cols = list(set(historical_df.columns) | set(nflverse_combined.columns))

        # Reindex both dataframes to have all columns (missing columns become NaN)
        historical_aligned = historical_df.reindex(columns=all_cols)
        nflverse_aligned = nflverse_combined.reindex(columns=all_cols)

        # Combine
        super_table = pd.concat([historical_aligned, nflverse_aligned], ignore_index=True)
        log(f"Combined historical ({len(historical_df):,}) + NFLverse ({len(nflverse_combined):,})")
    else:
        super_table = nflverse_combined
        log("NFLverse only (no historical data)")

    # Step 3a0: Apply confirmed identity repairs before player_week-sensitive
    # scoring/rank enrichment. This prevents same-name historical rows from
    # attaching to modern IDs and bridges confirmed duplicate historical IDs.
    if PLAYER_IDENTITY_GUARD_AVAILABLE:
        log("\nSTEP 3a0: Applying known identity repairs...")
        super_table = apply_known_identity_repairs(super_table, log_fn=log)
        super_table = apply_known_stat_family_splits(super_table, log_fn=log)
        super_table = apply_known_special_teams_identity_repairs(super_table, log_fn=log)
        super_table = apply_known_context_identity_rebuilds(super_table, log_fn=log)
    else:
        log("\nSTEP 3a0: Skipping known identity repairs (helper import unavailable)")

    # Step 3a1: Add/backfill missing PBP-derived event atoms. These are
    # additive schema columns (punting, fumbles, FG yardage, return TD splits)
    # and do not overwrite official passing/rushing/receiving box-score stats.
    if PBP_SCHEMA_BACKFILL_AVAILABLE:
        log("\nSTEP 3a0b: Repairing pre-1999 postseason official stat collisions...")
        super_table = apply_pbp_postseason_official_repairs(super_table, log_fn=log)
        log("\nSTEP 3a1: Applying PBP schema atom backfill...")
        super_table = apply_pbp_schema_backfill(super_table, log_fn=log)
        log("\nSTEP 3a1b: Applying guarded PBP truth atom overlays...")
        super_table = apply_pbp_truth_atom_overlays(super_table, log_fn=log)
        log("\nSTEP 3a1c: Adding safe pre-1999 PBP missing K/return/offense rows...")
        safe_missing_rows = build_pbp_safe_missing_rows(super_table, columns=super_table.columns, log_fn=log)
        if not safe_missing_rows.empty:
            before_rows = len(super_table)
            super_table = pd.concat([super_table, safe_missing_rows], ignore_index=True, sort=False)
            log(f"  Added {len(super_table) - before_rows:,} safe PBP missing rows")
    else:
        log("\nSTEP 3a1: Skipping PBP schema atom backfill/postseason repair (helper import unavailable)")

    if PFR_SUPERTABLE_BACKFILL_AVAILABLE:
        log("\nSTEP 3a1d: Applying PFR weekly boxscore update package...")
        super_table = apply_pfr_supertable_update_package(super_table, log_fn=log)
    else:
        log("\nSTEP 3a1d: Skipping PFR weekly boxscore update package (helper import unavailable)")

    # Sort by year, week
    super_table = super_table.sort_values(["year", "week"], ascending=[True, True])

    # Step 3a: Normalize kicking column names across data sources
    # Historical stathead K data may use xp_made/xp_attempts/fg_attempts
    # while NFLverse uses pat_made/pat_att/fg_att.  Merge into canonical names.
    kick_renames = {
        "xp_made": "pat_made",
        "xp_attempts": "pat_att",
        "fg_attempts": "fg_att",
        "xp_a": "pat_att",
        "xp_m": "pat_made",
    }
    merged_kick_cols = 0
    for old_name, new_name in kick_renames.items():
        if old_name in super_table.columns:
            if new_name in super_table.columns:
                # Both exist: merge old into new (fill NaN in new with old values)
                mask = super_table[new_name].isna() & super_table[old_name].notna()
                super_table.loc[mask, new_name] = super_table.loc[mask, old_name]
                super_table = super_table.drop(columns=[old_name])
                merged_kick_cols += 1
                log(f"  Merged {old_name} -> {new_name} ({mask.sum()} values filled)")
            else:
                # Only old exists: just rename
                super_table = super_table.rename(columns={old_name: new_name})
                merged_kick_cols += 1
                log(f"  Renamed {old_name} -> {new_name}")
    if merged_kick_cols:
        log(f"  Normalized {merged_kick_cols} kicking column names")

    # Step 3a2: Normalize fumble recovery column names across data sources
    # IDP player rows have fumble_recovery_opp/fumble_recovery_tds from raw NFLverse,
    # while DEF team rows have fum_rec/fum_ret_td from defense_stats.py.
    # Merge into canonical short-form names.
    fumble_renames = {
        "fumble_recovery_opp": "fum_rec",
        "fumble_recovery_tds": "fum_ret_td",
    }
    merged_fumble_cols = 0
    for old_name, new_name in fumble_renames.items():
        if old_name in super_table.columns:
            if new_name in super_table.columns:
                # Both exist: merge old into new (fill NaN in new with old values)
                mask = super_table[new_name].isna() & super_table[old_name].notna()
                super_table.loc[mask, new_name] = super_table.loc[mask, old_name]
                super_table = super_table.drop(columns=[old_name])
                merged_fumble_cols += 1
                log(f"  Merged {old_name} -> {new_name} ({mask.sum()} values filled)")
            else:
                # Only old exists: just rename
                super_table = super_table.rename(columns={old_name: new_name})
                merged_fumble_cols += 1
                log(f"  Renamed {old_name} -> {new_name}")
    if merged_fumble_cols:
        log(f"  Normalized {merged_fumble_cols} fumble recovery column names")

    # Step 3b: Fix headshots AFTER merge (so historical players can get NFLverse headshots)
    if args.fix_headshots and "headshot_url" in super_table.columns:
        log("\nSTEP 3b: Fixing missing headshots...")
        log("-" * 50)
        super_table = replace_placeholder_headshots(
            super_table, headshot_col="headshot_url", team_col="nfl_team", player_id_col="nfl_player_id"
        )

    # Step 3b2: Apply dual-position enrichments (e.g., Cookie Gilchrist -> RB,K for kicking seasons)
    log("\nSTEP 3b2: Applying dual-position enrichments...")
    super_table = apply_dual_position_enrichments(super_table)

    # Step 3b3: Merge duplicate player_week rows from dual-position data sources
    # (e.g., Blanda has separate QB and K rows from different Stathead parses)
    log("\nSTEP 3b3: Merging dual-position duplicate rows...")
    super_table = merge_dual_position_duplicates(super_table)

    # Step 3c: Backfill derived columns for historical data
    # NFLverse data (1999+) already has these from defense_stats.py,
    # but historical data (pre-1999) needs them derived from raw columns.
    # Must run BEFORE fantasy points/ranks so backfilled data feeds into scoring.
    log("\nSTEP 3c: Backfilling derived columns for historical data...")
    log("-" * 50)
    super_table = backfill_derived_columns(super_table)

    # Step 3c0: Apply local PBP rollups for scoring rules that need play-level
    # detail. This is a pure left-join on existing player_week rows.
    if PBP_SCORING_ROLLUP_AVAILABLE:
        log("\nSTEP 3c0: Applying PBP scoring rollups...")
        log("-" * 50)
        super_table = enrich_with_pbp_scoring_rollup(super_table, log_fn=log)
    else:
        log("\nSTEP 3c0: Skipping PBP scoring rollups (helper import unavailable)")

    # Step 3c1: Populate canonical derived kicker columns (L1.c Phase 0.1)
    log("\nSTEP 3c1: Populating canonical kicker columns...")
    super_table = populate_fg_made_60_plus_canonical(super_table)
    n_60_plus = (super_table["fg_made_60_plus_canonical"] > 0).sum()
    log(f"  fg_made_60_plus_canonical: {n_60_plus:,} non-zero rows")

    # Step 3c1b: Phase 0.3 -- fg_yards_canonical + fg_yards_over_30_canonical
    # (added 2026-05-02 per L1.c spec section 3.3). Order matters:
    # populate_fg_yards_over_30_canonical reads fg_yards_canonical, so
    # populate_fg_yards_canonical must run first.
    super_table = populate_fg_yards_canonical(super_table)
    super_table = populate_fg_yards_over_30_canonical(super_table)
    if "position" in super_table.columns:
        k_mask = super_table["position"] == "K"
        n_yards = (super_table.loc[k_mask, "fg_yards_canonical"] > 0).sum()
        n_over30 = (super_table.loc[k_mask, "fg_yards_over_30_canonical"] > 0).sum()
        log(f"  fg_yards_canonical: {n_yards:,} non-zero K rows")
        log(f"  fg_yards_over_30_canonical: {n_over30:,} non-zero K rows")

    # Step 3c2: Validate kicker week patterns
    # Kicker weeks should have bye-week gaps just like non-kicker data.
    # If kickers have sequential weeks (no gaps), it means game_num was used as week.
    log("\nSTEP 3c2: Validating kicker week patterns...")
    if "position" in super_table.columns and "week" in super_table.columns:
        k_data = super_table[(super_table["position"] == "K") & (super_table["year"] >= 1960)].copy()
        non_k_data = super_table[
            (super_table["position"].isin(["QB", "WR", "RB", "TE"])) & (super_table["year"] >= 1960)
        ]
        if len(k_data) > 0 and len(non_k_data) > 0:
            # Compare max week per player-year for kickers vs non-kickers
            k_max = k_data.groupby(["NFL_player_id", "year"])["week"].max().reset_index()
            non_k_max = non_k_data.groupby(["NFL_player_id", "year"])["week"].max().reset_index()
            k_avg_max = k_max["week"].mean()
            non_k_avg_max = non_k_max["week"].mean()
            log(f"  Kicker avg max week: {k_avg_max:.1f}, Non-kicker avg max week: {non_k_avg_max:.1f}")
            # Kickers should have similar max weeks as non-kickers if weeks are correct
            if k_avg_max < non_k_avg_max - 2:
                log("  [WARN] Kicker weeks may be sequential (game_num) instead of actual NFL weeks.")
                log("  [WARN] Consider running fix_opponent_alignment.py --step kicker-weeks")
            else:
                log("  [PASS] Kicker week patterns look consistent with non-kicker data")
        else:
            log("  [SKIP] Insufficient data for kicker week validation")

    # Step 3d: Calculate pre-computed fantasy points and ranks
    # These enable optimal lineup calculations via rank lookups instead of runtime sorting
    if RANK_FUNCTIONS_AVAILABLE:
        log("\nSTEP 3d: Calculating fantasy points and ranks...")
        log("-" * 50)
        super_table = calculate_all_ranks(super_table)
        log(f"  Added fpts_* and rank_* columns for {len(super_table):,} rows")
    else:
        log("\nSkipping rank calculation (functions not available)")

    log(f"\nSuper table total records: {len(super_table):,}")
    log(f"Year range: {int(super_table['year'].min())} - {int(super_table['year'].max())}")

    # Summary by decade
    log("\nRecords by decade:")
    super_table["decade"] = (super_table["year"] // 10) * 10
    decade_counts = super_table.groupby("decade").size()
    for decade, count in decade_counts.items():
        log(f"  {int(decade)}s: {count:,}")
    super_table = super_table.drop(columns=["decade"])

    # Summary by data source
    if "data_source" in super_table.columns:
        log("\nRecords by data source:")
        source_counts = super_table.groupby("data_source").size()
        for source, count in source_counts.items():
            log(f"  {source}: {count:,}")

    # Save super table locally
    super_file = output_dir / "nfl_player_stats_super_table.parquet"
    super_table.to_parquet(super_file, index=False)
    log(f"\nSaved: {super_file}")

    # Step 4: Upload to MotherDuck
    if not args.skip_upload:
        log("\nSTEP 4: Uploading to MotherDuck...")
        log("-" * 50)
        upload_to_motherduck(super_table, "nfl_player_stats_all")
    else:
        log("\nSkipping MotherDuck upload (--skip-upload)")

    log("\n" + "=" * 70)
    log("Done!")
    log("=" * 70)


if __name__ == "__main__":
    main()
