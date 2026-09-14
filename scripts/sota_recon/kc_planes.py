"""
sota_recon/kc_planes.py  --  O.4: K (key/join/cardinality) + C (expected-coverage) plane CONTRACTS

Master plan §25.4 D1/D2, addendum O.4. One typed contract per registered source declaring:

K plane (join contract):
  source_key_columns      actual columns in the source parquet (footer-resolved from the
                          committed physical-field snapshot — never guessed; unresolvable
                          sources are typed PENDING_COLUMN_MAP, proof-or-pending)
  canonical_universe      which entity universe the key lands in (entity_universes.py)
  canonical_key_columns   the universe-side key
  expected_cardinality    '1:1' | 'N:1' | 'controlled 1:N'  (1:1 -> source-key dups are
                          violations; controlled 1:N -> dup/fanout counts recorded only)
  temporal_validity       year window inside which the join is expected to land
  season_type_policy      REG | POST | ALL  (universe-side filter for season-grain joins)
  permitted_unmatched     declared reason a source row may legally miss the universe
  status                  ACTIVE            gate runs now
                          AUTHORITY         source builds the universe; gate runs as
                                            self-consistency (anti-join expected 0)
                          PENDING_CROSSWALK key spaces differ (NFL_player_id vs pfr_id;
                                            player_week vs boxscore) — inert until the
                                            crosswalk join is built and receipted
                          PENDING_COLUMN_MAP declared key not resolvable from footer
                          IDENTITY_SUBJECT  v26 itself (subject under test, never joined
                                            to its own universes as a witness)
                          NOT_APPLICABLE    workflow/triage records with no entity grain

C plane (coverage contract):
  coverage_model          DENSE | SPARSE_POSITIVE_ONLY | EVENT_ONLY | PARTICIPANT_ONLY |
                          TOTAL_ONLY | PARTIAL_KNOWN | UNKNOWN
                          (expected-observed is a MISSING finding only under DENSE; under
                          every other model it is recorded coverage, not a defect)

The contract file is GENERATED (rule-based over the sources registry + snapshot columns,
with explicit per-source overrides) and COMMITTED:
  scripts/sota_recon/witness_gate/contracts/kc_planes.v1.json
Regenerate + diff-check:  python -m scripts.sota_recon.kc_planes
Gate execution lives in recon_kc_planes.py.
"""

from __future__ import annotations

import json
import os

from .sources import registry

CONTRACTS_PATH = os.path.join(os.path.dirname(__file__), "witness_gate", "contracts", "kc_planes.v1.json")
SNAPSHOT_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "physical-field-snapshot-v0.json")

COVERAGE_MODELS = {
    "DENSE", "SPARSE_POSITIVE_ONLY", "EVENT_ONLY", "PARTICIPANT_ONLY",
    "TOTAL_ONLY", "PARTIAL_KNOWN", "UNKNOWN",
}
K_STATUSES = {
    "ACTIVE", "AUTHORITY", "PENDING_CROSSWALK", "PENDING_COLUMN_MAP",
    "IDENTITY_SUBJECT", "NOT_APPLICABLE",
}
CARDINALITIES = {"1:1", "N:1", "controlled 1:N"}

# Resolution priority for logical key components against actual source columns.
LOGICAL_COLUMN_CANDIDATES = {
    "boxscore_id": ["boxscore_id"],
    "pfr_id": ["pfr_id", "player_link_ids", "pfr_player_id"],
    "year": ["year", "season", "year_id"],
    "week": ["week"],
    "team": ["team", "nfl_team", "team_code"],
    "game_id": ["game_id"],
    "play_id": ["play_id"],
    "team_game_key": ["team_game_key"],
}

# Sources that BUILD a universe (self-consistency gates, anti-join expected 0).
AUTHORITY_SOURCES = {
    "pfr_team_games": "team_games",
    "schedule_master": "games",
    "pbp_merged_1978_2025": "plays",
    "pfr_box_scoring": "scoring_events",
    "pfr_player_offense_box": "player_game_presence",
    "pfr_player_defense_box": "player_game_presence",
    "pfr_box_kicking": "player_game_presence",
    "pfr_box_returns": "player_game_presence",
    "pfr_box_home_starters": "player_game_presence",
    "pfr_box_vis_starters": "player_game_presence",
    "pfr_box_home_snaps": "player_game_presence",
    "pfr_box_vis_snaps": "player_game_presence",
    "pfr_box_home_drives": "games",
    "pfr_box_vis_drives": "games",
}

# Team/context box tables: many rows per game, keyed by boxscore_id only.
GAME_GRAIN_BOX = {
    "pfr_box_team_stats", "pfr_box_game_info", "pfr_box_expected_points",
    "pfr_box_officials", "pfr_box_pbp",
}

# C-plane models per source (adjudicated; UNKNOWN is legal but counted by the runner).
COVERAGE_OVERRIDES = {
    "v26_release": "DENSE",
    "legacy_motherduck_supertable": "DENSE",
    "pfr_team_games": "DENSE",
    "schedule_master": "DENSE",
    "pbp_team_defense": "DENSE",
    "pbp_merged_1978_2025": "EVENT_ONLY",
    "pbp_player_week_rollup": "PARTICIPANT_ONLY",
    "pfr_box_scoring": "EVENT_ONLY",
    "pfr_box_home_drives": "EVENT_ONLY",
    "pfr_box_vis_drives": "EVENT_ONLY",
    "pfr_box_pbp": "EVENT_ONLY",
    "pfr_box_home_starters": "PARTICIPANT_ONLY",
    "pfr_box_vis_starters": "PARTICIPANT_ONLY",
    "pfr_box_home_snaps": "PARTICIPANT_ONLY",
    "pfr_box_vis_snaps": "PARTICIPANT_ONLY",
    "pfr_box_team_stats": "TOTAL_ONLY",
    "pfr_box_game_info": "PARTIAL_KNOWN",
    "pfr_box_officials": "PARTIAL_KNOWN",
    "pfr_box_expected_points": "PARTIAL_KNOWN",
    "pfr_games_played": "PARTICIPANT_ONLY",
    "pfr_games_played_post": "PARTICIPANT_ONLY",
    "pfr_snap_counts": "PARTICIPANT_ONLY",
    "player_bio": "PARTICIPANT_ONLY",
    "pfr_player_index": "PARTICIPANT_ONLY",
    "pfr_combine": "PARTICIPANT_ONLY",
    "pfr_player_combine": "PARTICIPANT_ONLY",
    "ngs_season_published": "PARTIAL_KNOWN",
    "ngs_weekly_raw": "PARTIAL_KNOWN",
    "ancient_pfa_gamelog": "PARTIAL_KNOWN",
    "ancient_pfr_recovery": "PARTIAL_KNOWN",
    "ancient_pbp1978_recovery": "PARTIAL_KNOWN",
    "ancient_newspaper_ocr": "PARTIAL_KNOWN",
    "scoring_summary": "PARTIAL_KNOWN",
}
NEWSPAPER_MODEL = "PARTIAL_KNOWN"       # only games with harvested papers
REVIEW_MODEL = "EVENT_ONLY"             # triage records
DEFAULT_STAT_TABLE_MODEL = "SPARSE_POSITIVE_ONLY"  # rows only for players with that stat family

# Explicit K overrides where rules would misbind.
K_OVERRIDES: dict[str, dict] = {
    "v26_release": {"status": "IDENTITY_SUBJECT", "reason": "subject under test"},
    "pbp_player_week_rollup": {
        "universe": "player_seasons", "logical_key": ["pfr_id", "year"],
        "source_key_exprs": ["xw_pfr_id", "year"],
        "crosswalk": {"via": "player_bio", "on": "NFL_player_id", "adds": "pfr_id",
                      "receipt": "bio_pfr_nflid"},
        "cardinality": "controlled 1:N",  # weekly rows fan into one player-season
        "unmatched": "0.33% of rollup NFL_player_ids lack a bio pfr crosswalk "
                     "(receipted; they surface as null_key_rows); POST-only seasons",
        "reason": "ACTIVE via receipted bio crosswalk "
                  "(crosswalk_receipts.v1.json#bio_pfr_nflid, O.5 §19.2); weekly grain "
                  "joins player_seasons at season fanout — game_date->boxscore crosswalk "
                  "for game-grain joins remains a separate future receipt",
    },
    "legacy_motherduck_supertable": {
        "universe": "player_seasons", "logical_key": ["pfr_id", "year"],
        "source_key_exprs": ["xw_pfr_id", "year"],
        "crosswalk": {"via": "player_bio", "on": "NFL_player_id", "adds": "pfr_id",
                      "receipt": "bio_pfr_nflid"},
        "cardinality": "controlled 1:N",  # weekly rows fan into one player-season
        "unmatched": "2.39% of legacy NFL_player_ids lack a bio pfr crosswalk "
                     "(receipted; null_key_rows); presence-universe era floors",
        "reason": "ACTIVE via receipted bio crosswalk (subject_history class still "
                  "NEVER votes — this is a join/coverage contract only)",
    },
    # OQ-LR-5 per-stream registrations share the composite bundle's join contract
    # (same physical schema; the 2026-07-25 team_game_key adjudication carries over).
    **{k: {
        "universe": "team_games", "logical_key": ["boxscore_id", "team"],
        "source_key_exprs": ["split_part(team_game_key, '_', 1)", "nfl_team"],
        "cardinality": "controlled 1:N", "canon_team": True,
        "reason": "ancient composite bundle team_game_key = boxscore_id + '_' + team (no "
                  "catalog game-num suffix; adjudicated 2026-07-25, 90/95 raw-key "
                  "unmatched receipt); player ids are NFL-space (crosswalk pending for "
                  "player grain); rows scoped by the stream's source-definition filter "
                  "(OQ-LR-5)",
    } for k in ("ancient_pfa_gamelog", "ancient_pfr_recovery",
                "ancient_pbp1978_recovery", "ancient_newspaper_ocr")},
    "pfr_box_home_starters": {
        "cardinality": "controlled 1:N",
        "reason": "two-way starters appear once per position row (2,197 dup keys receipted 2026-07-25)",
    },
    "pfr_box_vis_starters": {
        "cardinality": "controlled 1:N",
        "reason": "two-way starters appear once per position row (2,232 dup keys receipted 2026-07-25)",
    },
    "ngs_season_published": {
        "universe": "player_seasons", "logical_key": ["pfr_id", "year"],
        "source_key_exprs": ["xw_pfr_id", "year"],
        "crosswalk": {"via": "player_bio", "on": "NFL_player_id", "adds": "pfr_id",
                      "receipt": "bio_pfr_nflid"},
        "cardinality": "controlled 1:N",  # one row per NGS stat category per season
        "unmatched": "NGS covers qualifying modern players only (100% crosswalk coverage receipted)",
        "reason": "ACTIVE via receipted bio crosswalk "
                  "(crosswalk_receipts.v1.json#bio_pfr_nflid, O.5 §19.2)",
    },
    "ngs_weekly_raw": {
        "universe": "player_seasons", "logical_key": ["pfr_id", "year"],
        "source_key_exprs": ["xw_pfr_id", "year"],
        "crosswalk": {"via": "player_bio", "on": "NFL_player_id", "adds": "pfr_id",
                      "receipt": "bio_pfr_nflid"},
        "cardinality": "controlled 1:N",  # weekly rows fan into one player-season
        "unmatched": "NGS covers qualifying modern players only (100% crosswalk coverage receipted)",
        "reason": "ACTIVE via receipted bio crosswalk "
                  "(crosswalk_receipts.v1.json#bio_pfr_nflid, O.5 §19.2)",
    },
    # ---- O.8 referee-bench onboarding (2026-07-26) --------------------------------
    # Every new source lands in a DIFFERENT key space from our universes, so each is
    # typed PENDING_CROSSWALK (inert until a receipted crosswalk exists) rather than
    # guessed into ACTIVE. Name-joining these into pfr_id space without a receipt is
    # the twins hazard; §19.2 requires the crosswalk receipt first.
    # 2026-07-29: the receipt LANDED and these six were still typed PENDING by hand.
    # A hand-typed PENDING_CROSSWALK short-circuits `generate()` BEFORE the receipt gate
    # runs, so the status could not heal when the evidence arrived -- the same shape as
    # the refusal that outlived its condition. The status is no longer written here:
    # the crosswalk names its receipt and generate() derives ACTIVE-or-PENDING from
    # whether that receipt PASSES and lists the source in `licenses`. If the receipt is
    # ever withdrawn or stops licensing a family, these fall back automatically.
    # NOTE the slug column is NOT uniformly named: five families store `nflcom_slug` and
    # `player_season` stores `_player_slug`. Same slug space, different header -- writing
    # one column name for all six would have silently errored that family out of the lane.
    **{k: {
        "universe": "player_seasons", "logical_key": ["pfr_id", "year"],
        "source_key_exprs": ["xw_pfr_id", "season"],
        "cardinality": "controlled 1:N",
        "crosswalk": {"via": "nflcom_slug_pfrid",
                      "on": slug_col,
                      "adds": "pfr_id", "receipt": "nflcom_slug_pfrid"},
        "unmatched": "28.0% of slugs carry no pfr_id and are EXCLUDED, not guessed: "
                     "23,392 of 32,488 resolved (88.1% of roster rows). Coverage is "
                     "era-skewed by design of the evidence -- 1920-32 18.7%, 1933-49 "
                     "68.5%, 1950-77 83.7%, 1978-98 84.0%, 1999-2025 73.2% -- so an "
                     "ancient-era anti-join miss is expected, not a defect. Ambiguous "
                     "(name, season) keys on either side are never scored, and the 2 "
                     "slugs claiming two pfr_ids are excluded rather than resolved",
        "reason": "ACTIVE via receipted nflcom slug crosswalk "
                  "(crosswalk_receipts.v1.json#nflcom_slug_pfrid, §19.2). Licensing the "
                  "SOURCE is not licensing every column: the receipt's own "
                  "does_not_license list still withholds the L4 return block, the L7 "
                  "made-att composites and the L0 tackle total, and those rows stay OPEN "
                  "in the dossier regardless of this status",
    } for k, slug_col in (("nflcom_player_logs", "nflcom_slug"),
                          ("nflcom_player_career", "nflcom_slug"),
                          ("nflcom_player_season", "_player_slug"),
                          ("nflcom_player_situational", "nflcom_slug"),
                          ("nflcom_player_splits", "nflcom_slug"),
                          ("nflcom_player_logs_targeted", "nflcom_slug"))},
    "nflcom_team_stats": {
        "universe": "team_games", "logical_key": ["year", "week", "team"],
        "source_key_exprs": ["season", "team"],
        "cardinality": "controlled 1:N", "canon_team": True,
        "crosswalk": {"via": "stat fingerprint against v26 (year, nfl_franchise_number)",
                      "on": "undoubled nickname + season -> team_fid",
                      "adds": "team_fid", "receipt": "nflcom_team_fid"},
        "reason": "TEAM key space (season + team label), so NO player crosswalk is "
                  "needed -- only nflcom team labels canonicalized into franchise "
                  "(team_fid) space. It is SEASON grain against a team_games universe, "
                  "so the join is a rollup, not a row match. Cheapest nflcom family to "
                  "activate",
    },
    **{k: {
        "universe": "player_game_presence", "logical_key": ["boxscore_id", "pfr_id"],
        "source_key_exprs": ["game_id", "source_player_id"],
        "cardinality": "controlled 1:N",
        "crosswalk": {"via": "player_bio + game catalog",
                      "on": "source_player_id/player+team+season -> pfr_id; "
                            "game_id -> boxscore_id",
                      "adds": "pfr_id + boxscore_id", "receipt": "PENDING -- not yet built"},
        "reason": "ff_assets capture key space: source-native player ids (nflcom slug / "
                  "statscrew id) and source-native game ids. TWO crosswalks are owed "
                  "(player and game); roster captures additionally carry game_id NULL "
                  "(season grain) while PFA participation carries a real per-game id. "
                  "PRESENCE witnesses only -- they never carry stat values",
    } for k in ("nflcom_team_season_roster", "statscrew_team_season_roster",
                "pfa_player_game_participation")},
    # ---- 2026-07-27: the StatsCrew STAT families -------------------------------------
    # Unlike the three captures above these carry VALUES, so their K planes are stat
    # joins, not presence joins -- and their two blockers are DIFFERENT key spaces.
    # Typed separately so that clearing one crosswalk cannot license the other.
    "statscrew_team_season_stats": {
        "universe": "player_seasons", "logical_key": ["pfr_id", "year"],
        "source_key_exprs": ["source_player_id", "season"],
        "cardinality": "controlled 1:N",
        "crosswalk": {"via": "statscrew_team_season_roster",
                      "on": "source_player_id(statscrew id, anderhun001 class) + "
                            "player + team + season",
                      "adds": "pfr_id", "receipt": "PENDING -- not yet built"},
        "reason": "StatsCrew key space is a site-native player id ('anderhun001'); no "
                  "receipted crosswalk into pfr_id exists. The seed is the registered "
                  "statscrew_team_season_roster capture, which shares the id space AND "
                  "carries a BIRTH DATE on 100% of its re-parsed rows -- the "
                  "discriminator a (name, season) join lacks. INERT until receipted; "
                  "one player-season may hold up to 11 table-tagged rows, hence 1:N",
    },
    "statscrew_team_season_results": {
        "universe": "team_games", "logical_key": ["year", "week", "team"],
        "source_key_exprs": ["season", "team"],
        "cardinality": "controlled 1:N", "canon_team": True,
        "crosswalk": {"via": "franchise canon", "on": "statscrew team code -> team_fid",
                      "adds": "team_fid", "receipt": "PENDING -- not yet built"},
        "reason": "TEAM key space (season + statscrew code CAN/AKR/CLE), so NO player "
                  "crosswalk is owed -- only the era-aliased franchise canon "
                  "(OTI/HOU, CRD/STL/PHO, CLT/BAL) the O.7 DEF-row join had to solve "
                  "by hand. Rows are GAME grain against a team_games universe but "
                  "carry no week, so the join is by date, not by row match",
    },
    "newspaper_raw_archives": {
        "status": "NOT_APPLICABLE",
        "reason": "archive-of provenance layer (LOC page/issue image captures + "
                  "source-horizon candidate ledgers), not a tabular witness with an "
                  "entity grain; the curated newspaper bundle carries the joined rows",
    },
    "newspaper_review_accepted": {"status": "NOT_APPLICABLE", "reason": "triage workflow records, no entity grain"},
    "newspaper_review_holds": {"status": "NOT_APPLICABLE", "reason": "triage workflow records, no entity grain"},
    "player_bio": {
        "universe": "players", "logical_key": ["pfr_id"], "cardinality": "1:1",
        "unmatched": "bio spans players outside PFR box coverage (pre-1932 offense box floor, specialists)",
    },
    "pfr_player_index": {
        "universe": "players", "logical_key": ["pfr_id"], "cardinality": "1:1",
        "unmatched": "index includes players with no surviving boxscore rows",
    },
    "pfr_combine": {
        "universe": "players", "logical_key": ["pfr_id"], "cardinality": "controlled 1:N",
        "unmatched": "combine attendees who never appeared in an NFL game",
        "reason": "17 players carry multiple combine rows (re-attendees; receipted 2026-07-25)",
    },
    "pfr_player_combine": {
        "universe": "players", "logical_key": ["pfr_id"], "cardinality": "controlled 1:N",
        "unmatched": "combine attendees who never appeared in an NFL game; year is combine year, not season",
    },
    "pfr_snap_counts": {
        # season-grain snap participation; TOT/multi-team rows fan out
        "cardinality": "controlled 1:N",
    },
    "pfr_box_team_stats": {
        # O.7: the melted key-value rows keep their game-grain K contract (runner-
        # executable); the SIDE binding used by the R4 vertical lane is declared
        # here and receipted by relationship_verdicts (r4_team_witness_join_receipt)
        # in the same run that consumes it -- proof-or-pending, §19.
        "side_binding": {
            "declaration": "vis_stat/home_stat columns bind to team_games via "
                           "(boxscore_id, is_home) -> team_fid; packed labels "
                           "parsed per relationship_verdicts.Runner.BOX_LABELS",
            "receipt": "docs/relationship-verdicts.v1.json#r4_team_witness_join_receipt",
        },
    },
    "pbp_team_defense": {
        "universe": "team_games", "logical_key": ["year", "week", "team"],
        "cardinality": "1:1", "canon_team": True,
        "reason": "alt key (year, week, team) valid 1978+ (no doubleheaders in the pbp era). "
                  "KNOWN ISSUE (receipted 2026-07-25): 505 windowed keys unmatched, dominated by "
                  "BAL(Colts)/HOU(Oilers) 2-per-week 1978-95 — era-ambiguous codes ('BAL' = Colts in "
                  "catalog space but Ravens in pbp space) are UNRESOLVABLE by a shared static canon map; "
                  "fix = franchise-id (team_fid) scoped join, queued (see build_franchise_normalization).",
    },
    "scoring_summary": {
        "universe": "team_games", "logical_key": ["boxscore_id", "team"], "cardinality": "1:1",
        "reason": "derived per team-game scoring rollup; carries catalog boxscore_id + team_code directly",
    },
}


def _snapshot_columns() -> dict[str, dict[str, str]]:
    with open(os.path.abspath(SNAPSHOT_PATH), encoding="utf-8") as f:
        snap = json.load(f)
    out: dict[str, dict[str, str]] = {}
    for s in snap["sources"]:
        cols: dict[str, str] = {}
        for fentry in s.get("files") or []:
            cols.update(fentry.get("columns") or {})
        out[s["source_id"]] = cols
    return out


def _resolve(logical: str, cols: dict[str, str]) -> str | None:
    for cand in LOGICAL_COLUMN_CANDIDATES.get(logical, [logical]):
        if cand in cols:
            return cand
    return None


def _universe_key(universe: str) -> list[str]:
    from .entity_universes import UNIVERSE_KEYS
    if universe == "players":
        return ["pfr_id"]
    return UNIVERSE_KEYS[universe]


def _rule_binding(sid: str, src, cols: dict[str, str]) -> dict:
    """Default (universe, logical_key, cardinality, season_type, unmatched) from the join hint."""
    hint = src.join or ""
    if sid in AUTHORITY_SOURCES and sid not in GAME_GRAIN_BOX:
        universe = AUTHORITY_SOURCES[sid]
        if universe == "plays":
            return {"universe": "plays", "logical_key": ["game_id", "play_id"], "cardinality": "1:1"}
        if universe == "player_game_presence":
            return {"universe": universe, "logical_key": ["boxscore_id", "pfr_id"], "cardinality": "1:1"}
        if universe == "scoring_events":
            return {"universe": "games", "logical_key": ["boxscore_id"], "cardinality": "controlled 1:N"}
        if universe == "games" and sid.endswith("_drives"):
            return {"universe": "games", "logical_key": ["boxscore_id"], "cardinality": "controlled 1:N"}
        if sid == "pfr_team_games":
            return {"universe": "team_games", "logical_key": ["boxscore_id", "team"], "cardinality": "1:1"}
        if sid == "schedule_master":
            # per team-game calendar rows; (year, week, team) fans out on doubleheader weeks
            return {"universe": "team_games", "logical_key": ["year", "week", "team"],
                    "cardinality": "controlled 1:N", "canon_team": True,
                    "unmatched": "known catalog/schedule symmetric diff (~50 games, see entity-universes summary)"}
    if sid in GAME_GRAIN_BOX:
        return {"universe": "games", "logical_key": ["boxscore_id"], "cardinality": "controlled 1:N"}
    if sid.startswith("newspaper"):
        return {"universe": "games", "logical_key": ["boxscore_id"], "cardinality": "controlled 1:N",
                "unmatched": "newspaper harvest covers a curated game subset; ids minted from the catalog"}
    if hint.startswith("pfr_id+year"):
        st = "POST" if sid.endswith("_post") else "REG"
        return {"universe": "player_seasons", "logical_key": ["pfr_id", "year"],
                "cardinality": "controlled 1:N", "season_type": st,
                "unmatched": "presence universe floor is the box-table era (offense box 1932+); "
                "games_played index pages additionally list zero-game (DNP/IR) seasons with no "
                "presence evidence — era-uniform unmatched receipted 2026-07-25, adjudication queued"}
    if hint.startswith("boxscore_id"):
        if _resolve("pfr_id", cols):
            return {"universe": "player_game_presence", "logical_key": ["boxscore_id", "pfr_id"],
                    "cardinality": "1:1"}
        return {"universe": "games", "logical_key": ["boxscore_id"], "cardinality": "controlled 1:N"}
    return {"status": "PENDING_COLUMN_MAP", "reason": f"no rule for join hint '{hint}'"}


def _coverage_model(sid: str) -> str:
    if sid in COVERAGE_OVERRIDES:
        return COVERAGE_OVERRIDES[sid]
    if sid.startswith("newspaper_review"):
        return REVIEW_MODEL
    if sid.startswith("newspaper"):
        return NEWSPAPER_MODEL
    return DEFAULT_STAT_TABLE_MODEL


def generate() -> dict:
    reg = registry(include_subject=True)
    snap_cols = _snapshot_columns()
    entries = []
    for sid, src in reg.items():
        cols = snap_cols.get(sid, {})
        spec = _rule_binding(sid, src, cols)
        override = K_OVERRIDES.get(sid, {})
        if "universe" in override or "status" in override:
            # explicit override supersedes the rule outcome, including any rule-side pending status
            spec.pop("status", None)
            spec.pop("reason", None)
        spec.update(override)
        status = spec.get("status")
        entry: dict = {
            "source_id": sid,
            "k": {
                "status": status or "ACTIVE",
                "reason": spec.get("reason"),
            },
            "c": {"coverage_model": _coverage_model(sid)},
        }
        if status in {"IDENTITY_SUBJECT", "NOT_APPLICABLE", "PENDING_CROSSWALK", "PENDING_COLUMN_MAP"}:
            entries.append(entry)
            continue
        if spec.get("crosswalk"):
            # proof-or-pending (§19.2): a crosswalked contract goes ACTIVE only while its
            # alias-equivalence receipt PASSES and licenses this source; otherwise it
            # falls back to PENDING_CROSSWALK at generation time — a red regen-diff,
            # never a silently-running unproven join.
            from . import crosswalk_receipt
            try:
                rec = crosswalk_receipt.get(spec["crosswalk"]["receipt"])
                receipt_ok = rec["status"] == "PASS" and sid in rec["licenses"]
            except (FileNotFoundError, KeyError):
                receipt_ok = False
            if not receipt_ok:
                entry["k"].update({
                    "status": "PENDING_CROSSWALK",
                    "reason": f"crosswalk receipt {spec['crosswalk']['receipt']!r} "
                              "missing, failing, or not licensing this source",
                })
                entries.append(entry)
                continue
            entry["k"]["crosswalk"] = spec["crosswalk"]
        if spec.get("source_key_exprs"):
            key_fields = {"source_key_exprs": spec["source_key_exprs"], "source_key_columns": []}
        else:
            resolved, missing = {}, []
            for logical in spec["logical_key"]:
                col = _resolve(logical, cols)
                if col is None:
                    missing.append(logical)
                else:
                    resolved[logical] = col
            if missing:
                entry["k"].update({
                    "status": "PENDING_COLUMN_MAP",
                    "reason": f"unresolved logical keys {missing} against snapshot columns",
                })
                entries.append(entry)
                continue
            key_fields = {"source_key_columns": [resolved[k] for k in spec["logical_key"]]}
        entry["k"].update({
            "status": "AUTHORITY" if sid in AUTHORITY_SOURCES else "ACTIVE",
            **key_fields,
            "logical_key": spec["logical_key"],
            "canonical_universe": spec["universe"],
            "canonical_key_columns": _universe_key(spec["universe"]),
            "expected_cardinality": spec["cardinality"],
            "canon_team": bool(spec.get("canon_team")),
            "temporal_validity": {"year_min": src.year_min, "year_max": src.year_max},
            "season_type_policy": spec.get("season_type", "ALL"),
            "permitted_unmatched": spec.get("unmatched"),
        })
        if spec.get("side_binding"):
            entry["k"]["side_binding"] = spec["side_binding"]
        entries.append(entry)
    return {"version": "v1", "n_sources": len(entries), "contracts": entries}


def load() -> dict:
    with open(CONTRACTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def validate(doc: dict) -> list[str]:
    problems = []
    reg = registry(include_subject=True)
    seen = {e["source_id"] for e in doc["contracts"]}
    if seen != set(reg):
        problems.append(f"contract/registry mismatch: missing={sorted(set(reg)-seen)} extra={sorted(seen-set(reg))}")
    for e in doc["contracts"]:
        k, c = e["k"], e["c"]
        if k["status"] not in K_STATUSES:
            problems.append(f"{e['source_id']}: bad status {k['status']}")
        if c["coverage_model"] not in COVERAGE_MODELS:
            problems.append(f"{e['source_id']}: bad coverage model {c['coverage_model']}")
        if k["status"] in {"ACTIVE", "AUTHORITY"}:
            if not k.get("source_key_columns") and not k.get("source_key_exprs"):
                problems.append(f"{e['source_id']}: {k['status']} without source key columns or exprs")
            if k.get("expected_cardinality") not in CARDINALITIES:
                problems.append(f"{e['source_id']}: bad cardinality {k.get('expected_cardinality')}")
            if k.get("crosswalk") and not k["crosswalk"].get("receipt"):
                problems.append(f"{e['source_id']}: ACTIVE crosswalk without a receipt id")
        elif k["status"] != "AUTHORITY" and not k.get("reason"):
            problems.append(f"{e['source_id']}: non-runnable status without reason")
    return problems


def main() -> int:
    doc = generate()
    problems = validate(doc)
    with open(CONTRACTS_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    from collections import Counter
    statuses = Counter(e["k"]["status"] for e in doc["contracts"])
    models = Counter(e["c"]["coverage_model"] for e in doc["contracts"])
    print(f"kc_planes.v1.json: {doc['n_sources']} sources -> {CONTRACTS_PATH}")
    print("K statuses:", dict(statuses))
    print("C models:  ", dict(models))
    if problems:
        print("VALIDATION PROBLEMS:")
        for p in problems:
            print("  -", p)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
