"""Source registry for the local v26 SOTA reconciliation harness."""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass
from pathlib import Path

DATA_LAKE = r"D:\league-history-data\nfl"


# The legal lineage vocabulary (§16 doctrine: the registry declares the adversary
# structure). Family x era ROOT assignment lives in lineage_roots.v1.json (O.5);
# this field is the raw bloodline label the contract generator consumes.
KNOWN_LINEAGES = frozenset({"pfr", "pbp_merged", "ngs", "pfa_loc", "newspaper", "internal",
                            # O.8 (2026-07-26) referee-bench onboarding:
                            "nflcom",    # NFL.com harvest + roster captures (OQ-LR-1 governs
                                         # whether its BOX families collapse with pfr)
                            "statscrew"})  # StatsCrew.com captures (candidate root, no votes
                                           # until a shared-ancestor check receipts it)


@dataclass(frozen=True)
class Source:
    key: str
    role: str
    path: str
    year_min: int
    year_max: int
    join: str
    note: str
    # lineage = the upstream BLOODLINE. Tables sharing a lineage are correlated copies of
    # one source and cast ONE vote between them (pfr pages + pfr boxscores = one PFR vote);
    # disagreement inside a lineage is an intra-source inconsistency, never a 2-1 majority.
    # MANDATORY-EXPLICIT (§16.2, O.5 2026-07-25): there is NO default -- a source registered
    # without a deliberate lineage declaration fails at construction. The old lineage="pfr"
    # default silently folded new sources into the PFR root, corrupting independence counts
    # in both directions.
    lineage: str
    # witness_class: primary (may vote) / derived (our computation over a primary -- may
    # check, never votes) / subject_history (old supertables etc. -- NEVER votes, diff
    # context only) / identity / context.
    witness_class: str = "primary"

    def __post_init__(self) -> None:
        if self.lineage not in KNOWN_LINEAGES:
            raise ValueError(
                f"source {self.key!r}: lineage {self.lineage!r} not in KNOWN_LINEAGES "
                f"{sorted(KNOWN_LINEAGES)} -- declare it deliberately (§16.2)")

    def exists(self) -> bool:
        # directory-shaped sources are legal (sharded captures / multi-file table
        # families); the physical snapshot resolves them by rglob('*.parquet')
        return os.path.isfile(self.path) or os.path.isdir(self.path)


_V26_GLOB = os.path.join(DATA_LAKE, "releases", "*_v26", "tables", "nfl_player_stats_all.parquet")


def latest_v26() -> str:
    """Return the newest local v26 release WEEKLY parquet.

    THIS IS ONE OF FOUR PLANES, NOT "THE SUPERTABLE". Use `v26_plane()` when a column's
    natural grain is a season, a career, or a season-team -- see the note there.

    PARTITIONED MODE (2026-08-04, Joe: "why isn't it instant with our
    gating?"). Measured: the gate is 8s but a pass costs ~9min of year-parts
    plus ~5min of CONCAT -- and the concat exists only to glue 106 year
    files into one monolith because consumers expect a single path. When
    `weekly_repaired_parts/PARTITIONED` exists, the parts directory IS the
    plane and every reader gets a glob (DuckDB reads it natively). A fix
    touching two years then rewrites two small files instead of 750MB.
    The monolith is rebuilt ONCE, at promote time.
    """
    files = sorted(glob.glob(_V26_GLOB), key=lambda p: Path(p).stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No v26 release parquet found at {_V26_GLOB}")
    return files[0]


def weekly_read_path() -> str:
    """READ path for the weekly plane -- a glob when partitioned.

    latest_v26() stays the MONOLITH path because 261 callers do path
    arithmetic on it (.parent, .with_name, .stem); handing them a glob
    would misdirect silently. Readers that only ever do read_parquet()
    should call THIS instead: when the parts directory carries a
    PARTITIONED marker it returns the glob, so a targeted fix rewrites two
    small year files instead of a 750MB monolith.
    """
    mono = latest_v26()
    parts = Path(mono).parent / "weekly_repaired_parts"
    if (parts / "PARTITIONED").exists() and any(parts.glob("year=*.parquet")):
        return (parts / "year=*.parquet").as_posix()
    return mono


#: The subject's grains. `weekly` is the release parquet; the other three are built LOCALLY
#: by scripts/sota_recon/build_*_v26.py into a subdirectory of the same release and then
#: uploaded, so they are always on disk beside it.
V26_PLANES = {
    "weekly": None,                          # latest_v26() itself
    "season": "player_nfl_season.parquet",
    "career": "player_nfl_career.parquet",
    "season_team": "player_nfl_season_team.parquet",
}


def v26_plane(grain: str = "weekly") -> str:
    """Path to one grain of the subject.

    WHY THIS EXISTS. For the whole programme "audit the supertable" silently meant "audit
    the WEEKLY table", because that is the only thing `latest_v26()` returns. Columns whose
    natural grain is a season -- games_played, games_started -- were then EXCLUDED with the
    reason "the v26 subject is player-WEEK grain and has no such column by construction",
    which is false: player_nfl_season carries both over 112,632 rows, 1920-2025.

    The blocker was imaginary twice over. The season/career planes are not remote: they are
    built locally and sit in a SUBDIRECTORY of the very release directory the audit already
    reads. Listing `*.parquet` in the parent shows only nfl_player_stats_all* and looks like
    proof of absence -- which is what it was mistaken for on 2026-07-31.
    """
    if grain not in V26_PLANES:
        raise KeyError(f"unknown v26 grain {grain!r}; known: {sorted(V26_PLANES)}")
    if grain == "weekly":
        return latest_v26()
    p = Path(latest_v26()).parent / "season_career_v26" / V26_PLANES[grain]
    if not p.exists():
        raise FileNotFoundError(f"v26 {grain} plane not built: {p}")
    return p.as_posix()


SCHEDULE = Source(
    key="schedule_master",
    role="anchor",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "cache", "pfr_excel", "_master_schedule_1920_2025.parquet"),
    year_min=1920,
    year_max=2025,
    join="year+week+nfl_team+opponent_nfl_team",
    note="Independent game calendar used for structural validation.",
    lineage="pfr",  # PFR-excel-derived calendar: same scrape bloodline as the box tables
)

TEAM_GAMES = Source(
    key="pfr_team_games",
    role="team",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "nfl_team_games_all.parquet"),
    year_min=1920,
    year_max=2025,
    join="year+week+team_code+opponent_code",
    note="PFR team-game rows for score symmetry and team-game existence.",
    lineage="pfr",
)

PBP_ROLLUP = Source(
    key="pbp_player_week_rollup",
    role="oracle",
    path=os.path.join(
        DATA_LAKE, "raw", "stathead", "generated", "pbp_supertable_audit_1978_2025", "pbp_player_week_rollup.parquet"
    ),
    year_min=1978,
    year_max=2025,
    join="player_week",
    note="Independent play-by-play-derived player-week stat rollup.",
    lineage="pbp_merged",  # nflverse-rooted 1999+, PFR-pbp-rooted 1978-98 (era split in votes)
)

LEGACY_SUPERTABLE = Source(
    key="legacy_motherduck_supertable",
    role="legacy",
    path=os.path.join(
        DATA_LAKE,
        "raw",
        "legacy_supertable_backup_sources",
        "motherduck_legacy_super_table",
        "nfl_super_table_backup_20251226_1825.parquet",
    ),
    year_min=1920,
    year_max=2025,
    join="player_week",
    note="Prior MotherDuck super_table snapshot for drift context only.",
    lineage="internal",
    witness_class="subject_history",  # NEVER votes
)

# --- OQ-LR-5 RESOLVED (2026-07-26): the ancient composite bundle is registered as
# PER-STREAM sources, because row-level lineage beats source-level lineage. The old
# single `ancient_ready_bundle` source declared lineage pfa_loc for ALL rows, but the
# bundle's rows carry per-row bundle_source/data_source lineage across PFR recovery
# lanes, pbp-1978 recovery, newspaper OCR, and PFA gamelogs -- root-counting them all
# as pfa_loc miscounted ancient-era root diversity in both directions. Each stream
# source scopes its rows by a MANDATORY bundle_source/data_source filter (declared in
# the note and enforced by every MapSpec/lane that consumes it).
#
# EVIDENCE FINDING (queued to Joe, deletion discipline): the upstream composite input
# curated/ancient_source_recovery/ancient_upsert_bundle/ is EMPTY on disk; the
# 2026-07-18 through_1979 regeneration therefore silently dropped the entire
# pfa_player_gamelog stream (10,320 rows, 1920-1954) that the 2026-06-06 through_1978
# bundle still carries. The pfa stream registers at the SURVIVING through_1978 file.
_ANCIENT_BUNDLES = os.path.join(DATA_LAKE, "curated", "ancient_source_recovery",
                                "ready_upsert_bundles")
_ANCIENT_1979 = os.path.join(_ANCIENT_BUNDLES, "through_1979_pfr_loc",
                             "weekly_stat_upsert_rows.parquet")
_ANCIENT_1978 = os.path.join(_ANCIENT_BUNDLES, "through_1978_pfr_loc",
                             "weekly_stat_upsert_rows.parquet")

ANCIENT_PFA_GAMELOG = Source(
    key="ancient_pfa_gamelog",
    role="oracle",
    path=_ANCIENT_1978,
    year_min=1920,
    year_max=1954,
    join="player_week",
    note="PFA player-gamelog stream of the ancient composite (rows WHERE bundle_source "
         "LIKE '%pfa_player_gamelog%' -- filter is part of the source definition). "
         "Registered at the surviving through_1978 bundle: the through_1979 regen lost "
         "this stream when its upstream input evaporated (OQ-LR-5 receipt).",
    lineage="pfa_loc",
)
ANCIENT_PFR_RECOVERY = Source(
    key="ancient_pfr_recovery",
    role="oracle",
    path=_ANCIENT_1979,
    year_min=1920,
    year_max=1978,
    join="player_week",
    note="PFR recovery-lane streams of the ancient composite (rows WHERE bundle_source "
         "LIKE '%pfr_%' -- boxscore/kicking/punting/return-long/offense-return/"
         "pre-1970-DST recovery + decade pilots; filter is part of the source "
         "definition). PFR bloodline: one vote with the pfr root, never independent.",
    lineage="pfr",
)
ANCIENT_PBP1978_RECOVERY = Source(
    key="ancient_pbp1978_recovery",
    role="oracle",
    path=_ANCIENT_1979,
    year_min=1978,
    year_max=1979,
    join="player_week",
    note="pbp-1978/79 recovery streams of the ancient composite (rows WHERE "
         "bundle_source LIKE '%pbp_1978%' -- core + punter-IDP recovery; filter is "
         "part of the source definition). pbp_merged bloodline: era-split contract "
         "resolves 1978-79 to the pfr root.",
    lineage="pbp_merged",
)
ANCIENT_NEWSPAPER_OCR = Source(
    key="ancient_newspaper_ocr",
    role="oracle",
    path=_ANCIENT_1979,
    year_min=1920,
    year_max=1939,
    join="player_week",
    note="Newspaper OCR streams of the ancient composite (rows WHERE data_source "
         "carries newspapers_com_ocr or loc_newspaper_ocr -- newspapers.com harvest + "
         "LOC newspaper-image pilots; filter is part of the source definition). "
         "Newspaper root: the image is the arbiter; non-voting until the newspaper "
         "program's holds clear (no witness_map licenses may exist for it).",
    lineage="newspaper",
)

_PLAYERS = os.path.join(DATA_LAKE, "raw", "pfr", "players", "tables")
SEASON_AUTH_PASS = Source(
    key="pfr_player_season_passing",
    role="authority",
    path=os.path.join(_PLAYERS, "passing", "_combined.parquet"),
    year_min=1932,
    year_max=2025,
    join="pfr_id+year_id",
    note="Authoritative PFR player-page passing season totals.",
    lineage="pfr",
)
SEASON_AUTH_RUSHREC = Source(
    key="pfr_player_season_rush_rec",
    role="authority",
    path=os.path.join(_PLAYERS, "rushing_and_receiving", "_combined.parquet"),
    year_min=1932,
    year_max=2025,
    join="pfr_id+year_id",
    note="Authoritative PFR player-page rush/rec season totals.",
    lineage="pfr",
)
SEASON_AUTH_RECRUSH = Source(
    key="pfr_player_season_rec_rush",
    role="authority",
    path=os.path.join(_PLAYERS, "receiving_and_rushing", "_combined.parquet"),
    year_min=1932,
    year_max=2025,
    join="pfr_id+year_id",
    note="Authoritative PFR player-page rec/rush season totals.",
    lineage="pfr",
)

PLAYER_BIO = Source(
    key="player_bio",
    role="identity",
    path=os.path.join(DATA_LAKE, "ops_data", "nfl_historical", "player_bio.parquet"),
    year_min=1920,
    year_max=2025,
    join="NFL_player_id",
    note="Identity anchor for NFL_player_id, pfr_id, and position metadata.",
    lineage="internal",
    witness_class="identity",
)

NFLCOM_SLUG_PFRID = Source(
    key="nflcom_slug_pfrid",
    role="identity",
    path=os.path.join(
        DATA_LAKE, "derived", "entity_universes", "nflcom_slug_pfrid.parquet"
    ),
    year_min=1920,
    year_max=2025,
    join="nflcom_slug -> pfr_id",
    note=(
        "Materialized exact bijection from the receipted local NFL.com roster-to-PFR "
        "identity derivation. Internal non-voting identity bridge only; never external "
        "witness depth."
    ),
    lineage="internal",
    witness_class="identity",
)

PFR_PLAYER_INDEX = Source(
    key="pfr_player_index",
    role="identity",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "players", "player_index.parquet"),
    year_min=1920,
    year_max=2025,
    join="pfr_id",
    note="PFR player index -- the player universe that was scraped.",
    lineage="pfr",
    witness_class="identity",
)

PLAYER_OFFENSE_BOX = Source(
    key="pfr_player_offense_box",
    role="oracle",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", "player_offense", "_combined.parquet"),
    year_min=1932,
    year_max=2025,
    join="boxscore_id+pfr_id",
    note="Game-level offensive box score authority.",
    lineage="pfr",
)

PLAYER_DEFENSE_BOX = Source(
    key="pfr_player_defense_box",
    role="oracle",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", "player_defense", "_combined.parquet"),
    year_min=1933,
    year_max=2025,
    join="boxscore_id+pfr_id",
    note="Game-level defensive box score authority.",
    lineage="pfr",
)

SCORING_SUMMARY = Source(
    key="scoring_summary",
    role="authority",
    path=os.path.join(DATA_LAKE, "derived", "scoring_summary", "scoring_summary.parquet"),
    year_min=1920,
    year_max=2025,
    join="franchise+year+week",
    note="Authoritative per-team-game scoring decomposition.",
    lineage="internal",
    witness_class="derived",  # our computation over PFR scoring tables
)


_PFR_PLAYER_TABLES = os.path.join(DATA_LAKE, "raw", "pfr", "players", "tables")


def _pfr_player_table(key: str, table: str, role: str, year_min: int, note: str) -> Source:
    """PFR player-page season tables: one consolidated _combined.parquet per table type."""
    return Source(
        key=key, role=role,
        path=os.path.join(_PFR_PLAYER_TABLES, table, "_combined.parquet"),
        year_min=year_min, year_max=2025, join="pfr_id+year", note=note,
        lineage="pfr",
    )


# --- Phase-2 witness expansion (2026-07-10): previously held on the lake but UNWIRED ------
PFR_GAMES_PLAYED = _pfr_player_table(
    "pfr_games_played", "games_played", "appearance", 1920,
    "Games/starts per player-season -- appearance-bijection + PPG-denominator witness.")
PFR_PLAYER_SCORING = _pfr_player_table(
    "pfr_player_scoring", "scoring", "authority", 1920,
    "Season scoring lines (TDs by type, XP, FG, safeties, points) -- scoring-family authority.")
PFR_PLAYER_KICKING = _pfr_player_table(
    "pfr_player_kicking", "kicking", "authority", 1938,
    "Kicker season authority incl. FG distance buckets + fg_long -- adjudicates fg_long=sum corruption.")
PFR_PLAYER_PUNTING = _pfr_player_table(
    "pfr_player_punting", "punting", "authority", 1939,
    "Punting season authority incl. punt_long.")
PFR_PLAYER_DEFENSE = _pfr_player_table(
    "pfr_player_defense", "defense", "authority", 1940,
    "Defense season authority (INT, sacks, tackles) -- adjudicates e.g. Krause career INTs.")
PFR_PLAYER_RETURNS = _pfr_player_table(
    "pfr_player_returns", "returns", "authority", 1941,
    "Kick/punt return season authority.")
PFR_PLAYER_FANTASY = _pfr_player_table(
    "pfr_player_fantasy", "fantasy", "oracle", 1970,
    "PFR's own computed fantasy points -- INDEPENDENT witness for our pts_* formulas.")
PFR_SNAP_COUNTS = _pfr_player_table(
    "pfr_snap_counts", "snap_counts", "negative", 2012,
    "Snap counts -- negative witness: nonzero stats require nonzero snaps (2012+).")

_PFR_BOX_TABLES = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables")


def _pfr_box_table(key: str, table: str, role: str, year_min: int, note: str) -> Source:
    """PFR boxscore per-game tables: one consolidated _combined.parquet per table type."""
    return Source(
        key=key, role=role,
        path=os.path.join(_PFR_BOX_TABLES, table, "_combined.parquet"),
        year_min=year_min, year_max=2025, join="boxscore_id+player/team", note=note,
        lineage="pfr",
    )


# game-grain witnesses (Phase-2 batch 2)
# RE-SPANNED 2026-08-02 (Joe: "do the respans"): year_min = MEASURED capture
# extent (first season holding >=10 rows), not the hand-declared 1957 box-era
# guess. Nine tables moved -- eight EARLIER (team_stats/scoring/game_info/
# starters to 1920, returns 1936, kicking 1940, pbp 1966, expected_points
# 1978) and one LATER (officials 1957->1999: the declared span was inflated;
# a span can be wrong in the flattering direction too). Density starts remain
# the per-stat truth on top of these capture extents.
PFR_BOX_HOME_STARTERS = _pfr_box_table(
    "pfr_box_home_starters", "home_starters", "appearance", 1920,
    "Game-grain starters (home) -- appearance bijection at GAME grain.")
PFR_BOX_VIS_STARTERS = _pfr_box_table(
    "pfr_box_vis_starters", "vis_starters", "appearance", 1920,
    "Game-grain starters (visitor) -- appearance bijection at GAME grain.")
PFR_BOX_HOME_SNAPS = _pfr_box_table(
    "pfr_box_home_snaps", "home_snap_counts", "negative", 2012,
    "Game-grain snap counts (home) -- negative witness per game.")
PFR_BOX_VIS_SNAPS = _pfr_box_table(
    "pfr_box_vis_snaps", "vis_snap_counts", "negative", 2012,
    "Game-grain snap counts (visitor) -- negative witness per game.")
PFR_BOX_TEAM_STATS = _pfr_box_table(
    "pfr_box_team_stats", "team_stats", "team", 1920,
    "Per-game team stat lines -- independent team-total witness (vertical contracts).")
PFR_BOX_HOME_DRIVES = _pfr_box_table(
    "pfr_box_home_drives", "home_drives", "team", 1998,
    "Per-game drive logs (home) -- drive-stat witness.")
PFR_BOX_VIS_DRIVES = _pfr_box_table(
    "pfr_box_vis_drives", "vis_drives", "team", 1998,
    "Per-game drive logs (visitor) -- drive-stat witness.")
PFR_BOX_GAME_INFO = _pfr_box_table(
    "pfr_box_game_info", "game_info", "context", 1920,
    "Game context (weather, toss, vegas line) -- schedule/venue corroboration.")
PFR_BOX_EXPECTED_POINTS = _pfr_box_table(
    "pfr_box_expected_points", "expected_points", "oracle", 1978,
    "PFR expected-points summary per game -- EP witness.")
PFR_BOX_SCORING = _pfr_box_table(
    "pfr_box_scoring", "scoring", "authority", 1920,
    "Per-play scoring log w/ running score -- the WS7b event-bijection source (raw).")
PFR_BOX_PBP = _pfr_box_table(
    "pfr_box_pbp", "pbp", "oracle", 1966,
    "PFR per-play PBP -- second PBP stream independent of nflverse.")
PFR_BOX_KICKING = _pfr_box_table(
    "pfr_box_kicking", "kicking", "authority", 1940,
    "Per-game kicking/punting lines -- game-grain kicker witness (adjudicates fg_long class).")
PFR_BOX_RETURNS = _pfr_box_table(
    "pfr_box_returns", "returns", "authority", 1936,
    "Per-game return lines -- game-grain return witness.")
PFR_BOX_DEF_ADV = _pfr_box_table(
    "pfr_box_defense_advanced", "defense_advanced", "oracle", 2018,
    "Per-game advanced defense -- witness for modern advanced def atoms.")
PFR_BOX_RECV_ADV = _pfr_box_table(
    "pfr_box_receiving_advanced", "receiving_advanced", "oracle", 2018,
    "Per-game advanced receiving -- witness for modern advanced recv atoms.")
PFR_BOX_RUSH_ADV = _pfr_box_table(
    "pfr_box_rushing_advanced", "rushing_advanced", "oracle", 2018,
    "Per-game advanced rushing.")
PFR_BOX_PASS_ADV = _pfr_box_table(
    "pfr_box_passing_advanced", "passing_advanced", "oracle", 2018,
    "Per-game advanced passing.")
PFR_BOX_OFFICIALS = _pfr_box_table(
    "pfr_box_officials", "officials", "context", 1999,
    "Game officials -- context only.")

# post-season player-page tables (playoff-grain authority) + extras
PFR_PASSING_POST = _pfr_player_table(
    "pfr_passing_post", "passing_post", "authority", 1933,
    "Playoff passing season authority -- POST-grain witness.")
PFR_RUSHREC_POST = _pfr_player_table(
    "pfr_rushrec_post", "rushing_and_receiving_post", "authority", 1933,
    "Playoff rushing/receiving authority (rush-primary).")
PFR_RECRUSH_POST = _pfr_player_table(
    "pfr_recrush_post", "receiving_and_rushing_post", "authority", 1933,
    "Playoff receiving/rushing authority (recv-primary).")
PFR_DEFENSE_POST = _pfr_player_table(
    "pfr_defense_post", "defense_post", "authority", 1940,
    "Playoff defense authority.")
PFR_KICKING_POST = _pfr_player_table(
    "pfr_kicking_post", "kicking_post", "authority", 1938,
    "Playoff kicking authority.")
PFR_PUNTING_POST = _pfr_player_table(
    "pfr_punting_post", "punting_post", "authority", 1939,
    "Playoff punting authority.")
PFR_RETURNS_POST = _pfr_player_table(
    "pfr_returns_post", "returns_post", "authority", 1941,
    "Playoff returns authority.")
PFR_SCORING_POST = _pfr_player_table(
    "pfr_scoring_post", "scoring_post", "authority", 1920,
    "Playoff scoring authority.")
PFR_GAMES_PLAYED_POST = _pfr_player_table(
    "pfr_games_played_post", "games_played_playoffs", "appearance", 1933,
    "Playoff games played -- POST appearance witness.")
PFR_ADJ_PASSING = _pfr_player_table(
    "pfr_adj_passing", "adj_passing", "oracle", 1932,
    "PFR era-adjusted passing indices -- plausibility witness.")
PFR_COMBINE = Source(
    key="pfr_combine", role="context",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "context", "tables", "combine", "_combined.parquet"),
    year_min=2000, year_max=2025, join="pfr_id",
    note="Combine measurements -- bio/identity context.", lineage="pfr")

# Dedicated PFR editorial-membership relations.  These are one PFR lineage/root,
# not independent votes; player_bio is permitted only as their identity bridge.
PFR_ALL_PRO_MEMBERS = Source(
    key="pfr_all_pro_members", role="authority",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "context", "tables", "all_pro", "_combined.parquet"),
    year_min=1920, year_max=2025, join="pfr_id+year",
    note="PFR dedicated All-Pro page; only its explicit first-team publication semantics qualify.",
    lineage="pfr", witness_class="primary")
PFR_PRO_BOWL_MEMBERS = Source(
    key="pfr_pro_bowl_members", role="authority",
    path=os.path.join(DATA_LAKE, "raw", "pfr", "context", "tables", "pro_bowl", "_combined.parquet"),
    year_min=1938, year_max=2025, join="pfr_id+year",
    note="PFR dedicated Pro Bowl membership page; one membership per player-season.",
    lineage="pfr", witness_class="primary")

# player-page advanced season tables (modern advanced-atom witnesses at season grain)
PFR_ADV_DEFENSE = _pfr_player_table(
    "pfr_adv_defense", "adv_defense", "oracle", 2018,
    "Season advanced defense (targets/cmp allowed, pressures) -- advanced-def witness.")
PFR_ADV_RECRUSH = _pfr_player_table(
    "pfr_adv_recrush", "adv_receiving_and_rushing", "oracle", 2018,
    "Season advanced receiving (recv-primary): air yards, YAC, drops witness.")
PFR_ADV_RUSHREC = _pfr_player_table(
    "pfr_adv_rushrec", "adv_rushing_and_receiving", "oracle", 2018,
    "Season advanced rushing (rush-primary): YBC/YAC, broken tackles witness.")
PFR_PASSING_ADV_SEASON = _pfr_player_table(
    "pfr_passing_adv_season", "passing_advanced", "oracle", 2018,
    "Season advanced passing: air yards, drops, pressures, RPO witness.")
PFR_ADV_DEFENSE_POST = _pfr_player_table(
    "pfr_adv_defense_post", "adv_defense_post", "oracle", 2018,
    "Playoff advanced defense witness.")
PFR_ADV_RECRUSH_POST = _pfr_player_table(
    "pfr_adv_recrush_post", "adv_receiving_and_rushing_post", "oracle", 2018,
    "Playoff advanced receiving witness.")
PFR_ADV_RUSHREC_POST = _pfr_player_table(
    "pfr_adv_rushrec_post", "adv_rushing_and_receiving_post", "oracle", 2018,
    "Playoff advanced rushing witness.")
PFR_PASSING_ADV_POST = _pfr_player_table(
    "pfr_passing_adv_post", "passing_advanced_post", "oracle", 2018,
    "Playoff advanced passing witness.")
PFR_PLAYER_COMBINE = _pfr_player_table(
    "pfr_player_combine", "combine", "context", 2000,
    "Player-page combine rows -- bio/identity context (dedup vs context/combine).")

PBP_MERGED = Source(
    key="pbp_merged_1978_2025", role="oracle",
    path=os.path.join(DATA_LAKE, "raw", "stathead", "generated",
                      "pbp_merged_1978_2025", "nfl_pbp_1978_2025_merged.parquet"),
    year_min=1978, year_max=2025, join="game_id+play",
    note="The merged play-by-play corpus itself (2.1M plays) -- ultimate re-derivation source.",
    lineage="pbp_merged",)

NGS_SEASON = Source(
    key="ngs_season_published", role="oracle",
    path=os.path.join(DATA_LAKE, "raw", "nextgen_stats", "ngs_season_2016_2025.parquet"),
    year_min=2016, year_max=2025, join="NFL_player_id+year",
    note="nflverse-published SEASON NGS rows -- external witness for WMEAN season rollups (WS2b).",
    lineage="ngs",)
NGS_WEEKLY_RAW = Source(
    key="ngs_weekly_raw", role="oracle",
    path=os.path.join(DATA_LAKE, "raw", "nextgen_stats", "ngs_weekly_2016_2025.parquet"),
    year_min=2016, year_max=2025, join="NFL_player_id+year+week",
    note="nflverse weekly NGS raw -- witness for weekly NGS atoms.",
    lineage="ngs",)
PBP_TEAM_DEFENSE = Source(
    key="pbp_team_defense", role="team",
    path=os.path.join(DATA_LAKE, "raw", "stathead", "generated",
                      "pbp_team_defense_1978_2025", "pbp_team_defense_week.parquet"),
    year_min=1978, year_max=2025, join="year+week+team",
    note="PBP-derived team-defense weekly rollup -- DST/allowed-column witness.",
    lineage="pbp_merged",
    witness_class="derived",)

# --- newspaper sidecar witness bundle (immutable, sidecar-only; 2026-07-17) -----------------
# Newspaper atoms are a WITNESS source like NFL.com logs or PFR boxscores -- never a
# supertable. The bundle contains ONLY newspaper_* sidecars (the wide 203-col overlay is
# deprecated review-only). Non-voting until identity/conflict hard-holds clear: no
# witness_map licenses exist for lineage "newspaper", so the vote engine cannot count it.
_NEWSPAPER_BUNDLE = os.path.join(DATA_LAKE, "curated", "witnesses", "newspaper",
                                 "20260717T065218Z_v1", "tables")


def _newspaper_table(key: str, table: str, role: str, join: str, note: str,
                     witness_class: str = "primary") -> Source:
    return Source(
        key=key, role=role,
        path=os.path.join(_NEWSPAPER_BUNDLE, table + ".parquet"),
        year_min=1920, year_max=1940, join=join,
        note=note, lineage="newspaper", witness_class=witness_class)


NEWSPAPER_PLAYER_CELLS = _newspaper_table(
    "newspaper_player_cells", "newspaper_weekly_player_stat_cells", "oracle",
    "player_week/NFL_player_id+boxscore_id+stat_name",
    "Newspaper player-week stat witness cells (long grain: stat_name+stat_value).")
NEWSPAPER_TEAM_STATS = _newspaper_table(
    "newspaper_team_stats", "newspaper_team_game_stats", "team",
    "boxscore_id+team_1/team_2_nfl_team+stat_name",
    "Newspaper team-game stat pairs (final_score, first_downs, yards, ...).")
NEWSPAPER_TEAM_CLAIMS = _newspaper_table(
    "newspaper_team_claims", "newspaper_team_game_stat_claims", "team",
    "boxscore_id+nfl_team+stat_name (unpivot of newspaper_team_stats via source_pair_key)",
    "Newspaper single-team stat claims -- witness path for team/DST stats.")
NEWSPAPER_SCORING_EVENTS = _newspaper_table(
    "newspaper_scoring_events", "newspaper_scoring_events", "oracle",
    "boxscore_id+scoring_team+event_order; scorer/passer/receiver NFL_player_id",
    "Newspaper scoring plays -- event-grain witness for the scoring bijection lane.")
NEWSPAPER_PBP_EVENTS = _newspaper_table(
    "newspaper_pbp_events", "newspaper_play_by_play_events", "oracle",
    "boxscore_id+event_order; primary/secondary NFL_player_id",
    "Newspaper narrative play-by-play events -- pre-PBP-era PBP witness.")
NEWSPAPER_LINEUPS = _newspaper_table(
    "newspaper_lineups", "newspaper_lineup_participation", "appearance",
    "boxscore_id+NFL_player_id (+starter_position)",
    "Newspaper lineup/starter participation -- appearance witness (cf. PFR starters).")
NEWSPAPER_GAME_CONTEXT = _newspaper_table(
    "newspaper_game_context", "newspaper_game_context", "context",
    "boxscore_id",
    "Newspaper game context (teams, scores, season_type) -- game-catalog witness.",
    witness_class="context")
NEWSPAPER_PLAYER_NOTES = _newspaper_table(
    "newspaper_player_notes", "newspaper_player_game_notes", "context",
    "player_week/NFL_player_id+boxscore_id",
    "Newspaper player-game notes (injury, playing time) -- historical-record sidecar.",
    witness_class="context")
NEWSPAPER_SOURCE_DOC_NOTES = _newspaper_table(
    "newspaper_source_doc_notes", "newspaper_reviewer_source_document_notes", "context",
    "boxscore_id+source_document_id",
    "Reviewer source-document evidence notes.", witness_class="context")
NEWSPAPER_REVIEW_ACCEPTED = _newspaper_table(
    "newspaper_review_accepted", "newspaper_general_reviewer_accepted_decisions", "context",
    "triage_id/target_entity_key",
    "Reviewer accepted-decision adjudication ledger.", witness_class="context")
NEWSPAPER_REVIEW_HOLDS = _newspaper_table(
    "newspaper_review_holds", "newspaper_general_reviewer_hold_decisions", "negative",
    "triage_id/target_entity_key",
    "Reviewer hold queue -- abstentions; never auto-promoted.", witness_class="context")


# ============================================================================
# O.8 REFEREE-BENCH ONBOARDING (2026-07-26) -- the 7 QUEUED census dirs
# ============================================================================
# Every source below walks the 9-step pipeline. Registration is NOT permission to
# vote: the nflcom stat families key on `nflcom_slug` (a name slug), which has NO
# receipted crosswalk into pfr_id/NFL_player_id space -- they register
# MAPPING_PENDING with that crosswalk named as the blocker, and cannot be licensed
# (therefore cannot vote, therefore change no root-diversity count) until it lands.
# Building it from a name join without a receipt is exactly the twins hazard the
# program forbids.
#
# ROOT DISCIPLINE (OQ-LR-1, still QUEUED -- never resolved by a blanket call here):
# nflcom registers as its OWN root, mirroring the standing pfr/nflverse_pbp
# treatment: separate roots + a DECLARED_UNRECEIPTED shared-ancestor annotation
# (elias_gsis_gamebook) recording the coordinated-lineage residual risk. Whether
# nflcom BOX families collapse into pfr for modern eras is an adjudication with a
# shared-error fingerprint receipt, not a code change.

_NFLCOM_TABLES = os.path.join(DATA_LAKE, "raw", "nflcom", "tables")


def _nflcom_table(key: str, table: str, role: str, year_min: int, year_max: int,
                  note: str, witness_class: str = "primary") -> Source:
    """NFL.com harvest table families (sharded parquet dirs, key space nflcom_slug)."""
    return Source(
        key=key, role=role, path=os.path.join(_NFLCOM_TABLES, table),
        year_min=year_min, year_max=year_max, join="nflcom_slug+season", note=note,
        lineage="nflcom", witness_class=witness_class)


NFLCOM_PLAYER_LOGS = _nflcom_table(
    "nflcom_player_logs", "player_logs", "oracle", 1920, 2025,
    "NFL.com per-game player logs (1,549,161 rows) -- game-grain stat witness across "
    "the whole history; the deepest non-PFR game-grain stream we hold.")
NFLCOM_PLAYER_CAREER = _nflcom_table(
    "nflcom_player_career", "player_career", "authority", 1950, 2025,
    "NFL.com career-page season rows (637,134 rows) -- season-grain stat authority.")
NFLCOM_PLAYER_SEASON = _nflcom_table(
    "nflcom_player_season", "player_season", "authority", 1932, 2025,
    "NFL.com season leaderboard tables by category (83,630 rows) -- season totals "
    "keyed by _player_slug/player + season_type.")
NFLCOM_PLAYER_PAGE_POSITIONS = Source(
    key="nflcom_player_page_positions",
    role="authority",
    path=os.path.join(DATA_LAKE, "derived", "validation", "sota_recon_master", "nflcom_player_page_positions.parquet"),
    year_min=1920,
    year_max=2025,
    join="nflcom_slug+season+page_kind",
    note="NFL.com player-page position meta descriptions, collapsed to one slug-season vote across logs/splits/situational; single-position witness only, never dual-eligibility authority.",
    lineage="nflcom",
)

PFR_PBP_PLAYER_WEEK_WITNESS = Source(
    key="pfr_pbp_player_week_witness",
    role="oracle",
    path=os.path.join(
        DATA_LAKE, "derived", "validation", "sota_recon_master",
        "pfr_pbp_player_week_passing.parquet",
    ),
    year_min=1978,
    year_max=2025,
    join="pfr_id+year+week",
    note="Independent player-week witness rebuilt from the local PFR play-by-play capture; "
         "derived from PFR play text/expected-points rows and never treated as a second "
         "PBP/nflverse lineage.",
    lineage="pfr",
    witness_class="derived",
)
# ---- THE COLUMN-SHIFT REPAIR, REPOINTED 2026-07-28 ----
# WHY THE REPOINT LANDS NOW, and what it does NOT do. Joe's framing: "the whole point of
# gridding them is to find and adjudicate arguments -- so if we know we mapped the columns
# correctly we're good for this stage." That draws the right line, and it is a line I had
# been blurring: ADJUDICATION needs column IDENTITY; VOTING needs a receipted crosswalk.
# Two different gates. The un-shift settles identity, so the 588 dossier rows these two
# families own become adjudicable today. Both stay MAPPING_PENDING / PENDING_CROSSWALK and
# cast NO vote until the slug->pfr_id crosswalk completes -- repointing changes what the
# columns are CALLED, never what they are allowed to do.
#
# THE REPAIR (O.9.0b, bdaf10493): the shift INVERTS from what is already stored, because
# each family's signatures collapse to exactly 8 key-SETS with no set mapping to two
# orders. 7,782,148 rows, 0 unclassified. Verified against an INDEPENDENT re-parse of the
# retained cache at 100.00000% on every recoverable column.
#
# AND THE HOLDOUT WAS CHECKED FOR SKEW (2026-07-28), because 100% on 0.7% of the rows
# proves nothing on its own -- the sample is one runner's cache, not a random draw. All 16
# layouts across both families are reached, all split kinds are reached, and no single
# season's share of the holdout differs from its share of the corpus by more than 3.12
# points. Receipt: docs/nflcom-splits-unshift-receipt.json#holdout_representativeness.
#
# WHAT IS PERMANENTLY LOST, declared per layout and left NULL rather than guessed: the
# last column of each row was truncated away AT PARSE and the retained cache covers only
# ~12% of the pages, so it cannot be recovered without a re-crawl (1st/40/fum/lng/
# net_avg/pct/rate/td). That loss predates this decision and neither approving nor
# refusing changes it -- but refusing would keep 7.78M rows carrying values under the
# WRONG names, which is strictly worse than the right names with eight NULLs.
#
# The ORIGINAL tables are untouched on disk (player_splits/, player_situational/), so the
# repoint is a source-definition change that reverses by editing this line.
_UNSHIFT_NOTE = (
    "Verified 100.00000% on every recoverable column against an independent re-parse, "
    "and the holdout was itself checked for skew (all 16 layouts and all split kinds "
    "reached; worst single-season share skew 3.12 points) -- receipt "
    "docs/nflcom-splits-unshift-receipt.json. Eight columns per family are "
    "UNRECOVERABLE (truncated at parse, cache covers ~12% of pages) and are declared per "
    "layout and left NULL, never zero-filled. Carries `_layout` as a real column, so the "
    "dossier keys it by MEASUREMENT rather than borrowing a layout census. Still "
    "MAPPING_PENDING: identity is settled, licensing is not.")

# ---- historical note: the defect this repair fixed (O.9.0, 2026-07-26) ----
# These two families are the ONLY nflcom tables whose header row begins with a BLANK
# cell (the split-label column: "Sundays", "Home Games", "Chicago Bears"). The
# harvester's parse_all_tables drops blank HEADERS but not their CELLS, then
# dict(zip(keys, cells[:len(keys)])) pairs keys[0] with cells[0] -- so EVERY value is
# stored one column to the LEFT of its true name, and the final column's value is
# truncated away entirely. Measured on disk: player_splits.g holds 'Sundays' /
# 'Wins' / 'vs AFC Teams'; the stat named `ret` is really games, `yds` is really
# returns, and so on down the row. Confirmed against the cached HTML:
#   splits      hdr=['', 'G','FUM','LOST','FF','OWN FR','OPP FR','TD']  (8 cells, 7 kept)
#   situational hdr=['', 'G','ATT','COMP','PCT','YDS', ...]             (16 cells, 15 kept)
# career / logs / season / team_stats have NO blank header and are NOT affected.
#
# 7,782,148 rows are therefore mislabelled AT REST. The fail-closed discipline held:
# both are MAPPING_PENDING / PENDING_CROSSWALK, so neither has ever voted and no
# canonical cell was touched. They may NOT be licensed until re-parsed.
# REPAIR IS A PARSE, NOT A CRAWL -- the source bytes are the retained cache pages.
NFLCOM_PLAYER_SITUATIONAL = _nflcom_table(
    "nflcom_player_situational", "player_situational_unshifted", "oracle", 1922, 2025,
    "NFL.com situational splits (2,797,507 rows) -- conditional-slice witness; the "
    "situational partition is a conservation check against season totals. "
    "REPOINTED 2026-07-28 at the O.9.0b unshifted table (was player_situational). "
    "The COLUMN-SHIFT defect is REPAIRED: 8 of 8 layouts, 8 of 8 split kinds. "
    + _UNSHIFT_NOTE)
NFLCOM_PLAYER_SPLITS = _nflcom_table(
    "nflcom_player_splits", "player_splits_unshifted", "oracle", 1921, 2025,
    "NFL.com splits tables (4,984,641 rows) -- split-partition witness; same "
    "conservation role as situational on a different partition axis. "
    "REPOINTED 2026-07-28 at the O.9.0b unshifted table (was player_splits). "
    "The COLUMN-SHIFT defect is REPAIRED: 8 of 8 layouts, 6 of 6 split kinds. "
    + _UNSHIFT_NOTE)
NFLCOM_PLAYER_LOGS_TARGETED = _nflcom_table(
    "nflcom_player_logs_targeted", "player_logs_targeted", "oracle", 1932, 1977,
    "NFL.com targeted game-log re-pull -- ancient-era gap-fill pass over player_logs; "
    "same bloodline, never a second vote. SOURCE DEFINITION REPOINTED to the derived "
    "local-cache reparse below so repeated names such as yds/att retain their exact "
    "page layout; the two original parquets are preserved unchanged and pinned in "
    "docs/nflcom-targeted-cache-reparse-receipt.json.")
# The raw targeted directory unions two purpose-built captures and discarded the table
# header/layout that disambiguates polysemous columns.  The derived file is generated only
# from retained local HTML by nflcom_targeted_cache_reparse.py (0 missing pages, 0
# unresolved signatures); it is the auditable source surface, not a manual data rewrite.
NFLCOM_PLAYER_LOGS_TARGETED = Source(
    key=NFLCOM_PLAYER_LOGS_TARGETED.key,
    role=NFLCOM_PLAYER_LOGS_TARGETED.role,
    path=os.path.join(
        DATA_LAKE, "derived", "validation", "sota_recon_master",
        "nflcom_player_logs_targeted_normalized.parquet",
    ),
    year_min=NFLCOM_PLAYER_LOGS_TARGETED.year_min,
    year_max=NFLCOM_PLAYER_LOGS_TARGETED.year_max,
    join=NFLCOM_PLAYER_LOGS_TARGETED.join + "+_layout",
    note=NFLCOM_PLAYER_LOGS_TARGETED.note,
    lineage=NFLCOM_PLAYER_LOGS_TARGETED.lineage,
    witness_class=NFLCOM_PLAYER_LOGS_TARGETED.witness_class,
)
NFLCOM_TEAM_STATS = _nflcom_table(
    "nflcom_team_stats", "team_stats", "team", 1932, 2025,
    "NFL.com team season stat tables (71,056 rows) -- team-total witness keyed by "
    "team+season+season_type (no player crosswalk needed: team key space). "
    "MANDATORY SOURCE-DEFINITION FILTER (O.9.0, 2026-07-26): rows WHERE NOT "
    "(_side='special-teams' AND _category IN ('kicking','punting')). MEASURED: those "
    "two families are byte-identical to offense_passing on all 4,456 rows each "
    "(content-hash equality over every non-partition column; 16 files -> 14 distinct "
    "contents), and their column set IS the passing set (att/cmp/pass_yds/rate/scky). "
    "NFL.com serves no /team-stats/special-teams/{kicking,punting}/ page, so the "
    "harvester cached the default passing table under a kicking/punting name. Left "
    "unfiltered this source would witness PASSING values as KICKING and PUNTING "
    "totals, and would double-count offense_passing threefold. 8,912 of 71,056 rows "
    "(12.5%) excluded; real special-teams content survives in special-teams/"
    "{field-goals,scoring,kickoff-returns,punt-returns}, which are NOT duplicates.")

# ---- ff_assets sharded captures (roster/participation = APPEARANCE witnesses) ----
# All three share one capture schema (source, dataset, season, team, game_id, player,
# source_player_id, position, source_url, retrieved_at_utc, content_sha256, ...) and
# each carries a coverage_complete IMPORT_MANIFEST pinning shard hashes. They witness
# PRESENCE, never stat values -- so they route to the appearance/presence lane, not
# to MapSpecs.
_FFASSETS = os.path.join(DATA_LAKE, "ff_assets")
_FFA_RUN = "29669388268-rosters-recovered-20260722"

NFLCOM_TEAM_SEASON_ROSTER = Source(
    key="nflcom_team_season_roster", role="appearance",
    path=os.path.join(_FFASSETS, "nflcom", "team_season_roster", _FFA_RUN),
    year_min=1920, year_max=2025, join="season+team+player(source_player_id=nflcom_slug)",
    note="NFL.com team-season rosters (144,568 rows, 20/20 shards, coverage_complete). "
         "CRITICAL: its source_player_id IS the nflcom_slug key space of the raw "
         "harvest -- this capture is the seed for the slug->pfr_id crosswalk that "
         "unblocks every nflcom stat family.",
    lineage="nflcom")
STATSCREW_TEAM_SEASON_ROSTER = Source(
    key="statscrew_team_season_roster", role="appearance",
    path=os.path.join(_FFASSETS, "statscrew", "team_season_roster", _FFA_RUN),
    year_min=1920, year_max=2025, join="season+team+player(source_player_id=statscrew id)",
    note="StatsCrew team-season rosters (57,808 rows, 20/20 shards, coverage_complete) "
         "with position + statscrew player ids (e.g. anderhun001). CANDIDATE root: "
         "genuinely separate compilation origin, but casts NO vote until a "
         "shared-ancestor check receipts its independence (§20.5).",
    lineage="statscrew")
PFA_GAME_PARTICIPATION = Source(
    key="pfa_player_game_participation", role="appearance",
    path=os.path.join(_FFASSETS, "profootballarchives", "player_game_participation",
                      "29669388268"),
    year_min=1920, year_max=2025, join="season+team+game_id+player",
    note="ProFootballArchives per-game participation (5,359,619 rows, 20/20 shards, "
         "coverage_complete) parsed from nflboxscores pages. Extends the pfa_loc root "
         "from the through-1979 ancient bundle across the ENTIRE history at game grain.",
    lineage="pfa_loc")

# ---- StatsCrew STAT families (2026-07-27): the thin-slice mandate, structurally ----
# These are the first StatsCrew captures that carry VALUES rather than presence, and they
# close two QUEUED Law B TOC entries ("team season pages", "schedules / game-by-game
# results"). Both are MULTI_TABLE by construction and are registered at DATASET-DIR grain,
# not run grain: `team_season_results` was harvested across TWO runs (shard 0 on
# 30271114181 after the wedged-shard fix, shards 1-14 on 30268109268) and `team_season_stats`
# across two as well (shards 0/2/3/4 on 30232968983, shard 1 on 30238150737 after two
# different failures). One run dir would witness a partial dataset while reading as whole.
# The composition is PINNED by test_statscrew_stat_families.py so an unexpected run dir
# joining the glob is a test failure, not a silent denominator change.
#
# TABLE TAGS ARE MANDATORY (finding 5 / O.9.3): `team_season_stats` publishes ELEVEN
# captioned tables per page and five of them share the header set `avg long no player tds
# yds`. Registered flat, eleven logical tables would collapse into one registry row -- the
# 82-tables-in-7-rows failure. `table_tag` is the parser's normalisation of the page's own
# <h2> caption and is the dossier's MULTI_TABLE discriminator.
#
# NEITHER CAN VOTE. Their key space is the StatsCrew player id (`anderhun001` class) for
# stats and the StatsCrew team code (`CAN`, `AKR`) for results; no receipted crosswalk into
# pfr_id or team_fid space exists, so both register MAPPING_PENDING / PENDING_CROSSWALK.
_STATSCREW_STATS = os.path.join(_FFASSETS, "statscrew", "team_season_stats")
_STATSCREW_RESULTS = os.path.join(_FFASSETS, "statscrew", "team_season_results")

STATSCREW_TEAM_SEASON_STATS = Source(
    key="statscrew_team_season_stats", role="authority",
    path=_STATSCREW_STATS,
    year_min=1921, year_max=2025,
    join="season+team+player(source_player_id=statscrew id)+table_tag",
    note="StatsCrew team-season stat tables (168,390 rows, 5/5 shards across runs "
         "30232968983 + 30238150737, 1921-2025) -- per-player season totals for one "
         "team-season, ELEVEN table-tagged families: defense_and_fumbles 48,882 / "
         "total_scoring 22,176 / receiving 21,441 / rushing 17,601 / interceptions "
         "13,264 / sacks 12,567 / kick_returns 11,310 / passing 7,184 / punt_returns "
         "6,243 / kicking 4,008 / punting 3,714. THE PRE-1994 IDP SURFACE this source "
         "was wanted for: the harvest's COLUMN_DICTIONARY resolves Solo Tackles, "
         "Assisted Tackles, Tackles for Loss, QB Hits and Passes Defensed from the "
         "site's own title attributes -- short labels are a measured trap (`tds` is "
         "PassingTouchdowns, `td` is Touchdown Percentage). CANDIDATE root: separate "
         "compilation origin, no vote until a shared-ancestor receipt (§20.5).",
    lineage="statscrew")
STATSCREW_TEAM_SEASON_RESULTS = Source(
    key="statscrew_team_season_results", role="team",
    path=_STATSCREW_RESULTS,
    year_min=1920, year_max=2023,
    join="season+team(statscrew code)+game date",
    note="StatsCrew game-by-game team results (23,235 rows, 15/15 shards across runs "
         "30271114181 + 30268109268, 1,521 team-seasons, 1920-2023) -- an independent "
         "game-calendar AND running-record witness. Carries both teams and both scores "
         "inside the `game` cell, the result letter, and THREE cumulative records "
         "(overall/home/road) after each game. The only StatsCrew family that publishes "
         "a season type: `row_class` reg_season 22,401 / post_season 834. `col_6` and "
         "`col_7` are the page's two trailing BLANK <th> columns, preserved rather than "
         "dropped (harvest defect 2). CANDIDATE root; no vote until a shared-ancestor "
         "receipt (§20.5).",
    lineage="statscrew")

# ---- newspaper raw archive layer (archive-of the newspaper root) ----
NEWSPAPER_RAW_ARCHIVES = Source(
    key="newspaper_raw_archives", role="context",
    path=os.path.join(DATA_LAKE, "raw", "newspaper_archives"),
    year_min=1920, year_max=1940, join="loc issue/seq keys + source_horizon candidates",
    note="ARCHIVE-OF the newspaper root: LOC page/issue captures + source-horizon "
         "acquisition and game-candidate ledgers that the curated newspaper witness "
         "bundle is built FROM. Registered as provenance context, NOT a second "
         "newspaper vote -- the curated bundle carries the witness rows and the image "
         "remains the arbiter. The newspaper program owns all holds here.",
    lineage="newspaper", witness_class="context")


import functools


@functools.lru_cache(maxsize=2)
def registry(include_subject: bool = True) -> dict[str, Source]:
    """Return all reconciliation sources keyed by stable source id.

    Cached for the process lifetime: hot loops (witness_votes/_lineages) call this per
    atom, and the uncached version re-globbed the release dir for latest_v26() each
    time -- the difference between seconds and hours on a 500K-atom stat."""
    subject = ()
    if include_subject:
        subject = (
            Source(
                key="v26_release",
                role="subject",
                path=latest_v26(),
                year_min=1920,
                year_max=2025,
                join="player_week",
                note="Current v26 release under audit -- WEEKLY grain only.",
                lineage="internal",  # the subject itself; never a root, never votes
            ),
        )
    # THE OTHER THREE SUBJECT GRAINS ARE REACHED THROUGH `v26_plane()`, NOT THROUGH THE
    # REGISTRY -- for now, and deliberately. Registering them as Sources is the right end
    # state (it makes season/career visible to every lane instead of only to callers who
    # know the accessor), but `registry()` carries two hard invariants: every source needs
    # exactly one kc_planes contract AND exactly one lineage_roots entry. Adding three
    # subjects broke 8 tests across test_kc_planes and test_lineage_roots, and
    # lineage_roots.v1.json is GENERATED -- hand-editing it has already produced
    # lineage_contract_drift once. So the registration is queued behind regenerating both
    # contracts, and the accessor ships now because it is what removes the actual blocker.
    sources = subject + (
        PLAYER_BIO, NFLCOM_SLUG_PFRID,
        PFR_PLAYER_INDEX,
        SCHEDULE,
        TEAM_GAMES,
        PBP_ROLLUP,
        PFR_PBP_PLAYER_WEEK_WITNESS,
        PLAYER_OFFENSE_BOX,
        PLAYER_DEFENSE_BOX,
        SCORING_SUMMARY,
        ANCIENT_PFA_GAMELOG,
        ANCIENT_PFR_RECOVERY,
        ANCIENT_PBP1978_RECOVERY,
        ANCIENT_NEWSPAPER_OCR,
        LEGACY_SUPERTABLE,
        SEASON_AUTH_PASS,
        SEASON_AUTH_RUSHREC,
        SEASON_AUTH_RECRUSH,
        # Phase-2 witness expansion (batch 1: player-page season tables + NGS)
        PFR_GAMES_PLAYED,
        PFR_PLAYER_SCORING,
        PFR_PLAYER_KICKING,
        PFR_PLAYER_PUNTING,
        PFR_PLAYER_DEFENSE,
        PFR_PLAYER_RETURNS,
        PFR_PLAYER_FANTASY,
        PFR_SNAP_COUNTS,
        NGS_SEASON,
        NGS_WEEKLY_RAW,
        PBP_TEAM_DEFENSE,
        # Phase-2 witness expansion (batch 2: game-grain boxscore + POST + merged PBP)
        PFR_BOX_HOME_STARTERS, PFR_BOX_VIS_STARTERS,
        PFR_BOX_HOME_SNAPS, PFR_BOX_VIS_SNAPS,
        PFR_BOX_TEAM_STATS,
        PFR_BOX_HOME_DRIVES, PFR_BOX_VIS_DRIVES,
        PFR_BOX_GAME_INFO, PFR_BOX_EXPECTED_POINTS,
        PFR_BOX_SCORING, PFR_BOX_PBP,
        PFR_BOX_KICKING, PFR_BOX_RETURNS,
        PFR_BOX_DEF_ADV, PFR_BOX_RECV_ADV, PFR_BOX_RUSH_ADV, PFR_BOX_PASS_ADV,
        PFR_BOX_OFFICIALS,
        PFR_PASSING_POST, PFR_RUSHREC_POST, PFR_RECRUSH_POST,
        PFR_DEFENSE_POST, PFR_KICKING_POST, PFR_PUNTING_POST,
        PFR_RETURNS_POST, PFR_SCORING_POST, PFR_GAMES_PLAYED_POST,
        PFR_ADJ_PASSING, PFR_COMBINE, PFR_ALL_PRO_MEMBERS, PFR_PRO_BOWL_MEMBERS,
        PFR_ADV_DEFENSE, PFR_ADV_RECRUSH, PFR_ADV_RUSHREC, PFR_PASSING_ADV_SEASON,
        PFR_ADV_DEFENSE_POST, PFR_ADV_RECRUSH_POST, PFR_ADV_RUSHREC_POST,
        PFR_PASSING_ADV_POST, PFR_PLAYER_COMBINE,
        PBP_MERGED,
        # newspaper sidecar witness bundle (non-voting lineage "newspaper")
        NEWSPAPER_PLAYER_CELLS, NEWSPAPER_TEAM_STATS, NEWSPAPER_TEAM_CLAIMS,
        NEWSPAPER_SCORING_EVENTS, NEWSPAPER_PBP_EVENTS, NEWSPAPER_LINEUPS,
        NEWSPAPER_GAME_CONTEXT, NEWSPAPER_PLAYER_NOTES, NEWSPAPER_SOURCE_DOC_NOTES,
        NEWSPAPER_REVIEW_ACCEPTED, NEWSPAPER_REVIEW_HOLDS,
        # O.8 referee-bench onboarding (2026-07-26)
        NFLCOM_PLAYER_LOGS, NFLCOM_PLAYER_CAREER, NFLCOM_PLAYER_SEASON, NFLCOM_PLAYER_PAGE_POSITIONS,
        NFLCOM_PLAYER_SITUATIONAL, NFLCOM_PLAYER_SPLITS,
        NFLCOM_PLAYER_LOGS_TARGETED, NFLCOM_TEAM_STATS,
        NFLCOM_TEAM_SEASON_ROSTER, STATSCREW_TEAM_SEASON_ROSTER,
        STATSCREW_TEAM_SEASON_STATS, STATSCREW_TEAM_SEASON_RESULTS,
        PFA_GAME_PARTICIPATION, NEWSPAPER_RAW_ARCHIVES,
    )
    return {s.key: s for s in sources}


def manifest_block() -> dict:
    """Serializable source pin for manifests."""
    return {
        k: {
            "role": s.role,
            "path": s.path,
            "exists": s.exists(),
            "year_min": s.year_min,
            "year_max": s.year_max,
            "join": s.join,
        }
        for k, s in registry().items()
    }


if __name__ == "__main__":
    print(f"{'KEY':<32} {'ROLE':<8} {'YEARS':<11} EXISTS  PATH")
    for k, s in registry().items():
        years = f"{s.year_min}-{s.year_max}"
        print(f"{k:<32} {s.role:<8} {years:<11} {'OK' if s.exists() else 'MISS':<6}  {s.path}")
