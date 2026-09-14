"""
sota_recon/witness_contracts.py -- witness provenance & completeness matrix.

Every local witness source (PFR player-page tables, PFR game boxscore tables, PBP rollups,
NGS, ancient bundle, team games) is auto-introspected into a CONTRACT: which canonical stat
atoms it carries, at what GRAIN (game / season / career / playoff), over what ERA, and how
completely. Inverting the contracts yields the COMPLETENESS MATRIX -- for every atom, the set
of witnesses that cover it by grain and era -- which is the recon backbone:

  * atoms with a GAME-grain witness  -> weekly super table can/should be complete there
  * atoms with only a SEASON/CAREER witness in an era -> acknowledged weekly gap (e.g. Jim Brown
    fumbles 1945-77: PFR season page + PFA profile witness it, no game-grain source exists)
  * atoms a witness carries that the super table lacks -> a drift/backfill target

Pivotable by position (both the granular `nfl_position` E..LCB vocabulary and the fantasy
`position` list) so era-specific position gaps surface (does a 1950 LDH carry what a modern
LCB does?).

    python -m scripts.sota_recon.witness_contracts            # print contracts + matrix summary
    python -m scripts.sota_recon.witness_contracts --json OUT  # emit the contract registry JSON
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import duckdb

LAKE = Path(r"D:/league-history-data/nfl")


def _release_super_table() -> str:
    files = sorted(
        glob.glob(str(LAKE / "releases" / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
        key=lambda p: Path(p).stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("no v26 super table release found")
    return files[0]


# --- witness registry: (name, path, grain) ------------------------------------------------
# grain: 'game' | 'season' | 'career' | 'season_post' (playoff season). PFR player pages carry
# season rows PLUS a career-total row; game boxscores are true game grain.
def _witnesses() -> list[tuple[str, Path, str]]:
    out: list[tuple[str, Path, str]] = []
    for d in sorted((LAKE / "raw/pfr/players/tables").glob("*")):
        p = d / "_combined.parquet"
        if p.exists():
            grain = "season_post" if d.name.endswith("_post") else "season"
            out.append((f"pfr_season:{d.name}", p, grain))
    for d in sorted((LAKE / "raw/pfr/boxscores/tables").glob("*")):
        p = d / "_combined.parquet"
        if p.exists():
            out.append((f"pfr_box:{d.name}", p, "game"))
    singles = {
        ("pbp_rollup", "game"): LAKE / "raw/stathead/generated/pbp_supertable_audit_1978_2025/pbp_player_week_rollup.parquet",
        # raw merged PBP: play grain, but the DERIVATION source for fumble_lost, first downs, EPA and
        # every rate — carries per-play flags 1978+ (see DERIVED_FORMULAS). Treated as a game-grain
        # witness for coverage purposes (a season with plays can derive the player-week atom).
        ("pbp_merged", "game"): LAKE / "raw/stathead/generated/pbp_merged_1978_2025/nfl_pbp_1978_2025_merged.parquet",
        ("ancient_bundle", "game"): LAKE / "curated/ancient_source_recovery/ready_upsert_bundles/through_1979_pfr_loc/weekly_stat_upsert_rows.parquet",
        ("ngs_weekly", "game"): LAKE / "raw/nextgen_stats/ngs_weekly_2016_2025.parquet",
        ("ngs_season", "season"): LAKE / "raw/nextgen_stats/ngs_season_2016_2025.parquet",
        ("pbp_team_defense", "game"): LAKE / "raw/stathead/generated/pbp_team_defense_1978_2025/pbp_team_defense_week.parquet",
    }
    for (name, grain), p in singles.items():
        if p.exists():
            out.append((name, p, grain))
    # NFL.com harvested witnesses (see nflcom_harvest.py). Bulk category pages are position/category-
    # scoped so their generic columns (yds/td/att) canonicalize UNAMBIGUOUSLY within a category (that
    # scoping is carried by the witness name -> NFLCOM_CATEGORY_ATOMS keys off it). NFL.com uniquely
    # carries fumbles_lost / FF / OWN FR / OPP FR at season, career AND weekly grain, every position.
    nflcom = LAKE / "raw/nflcom/tables"
    for p in sorted((nflcom / "player_season").glob("*.parquet")):
        out.append((f"nflcom_season:{p.stem}", p, "season"))
    for p in sorted((nflcom / "team_stats").glob("*.parquet")):
        out.append((f"nflcom_team:{p.stem}", p, "season"))
    for name, grain in (("player_logs", "game"), ("player_career", "career")):
        p = nflcom / f"{name}.parquet"
        if p.exists():
            out.append((f"nflcom_{name.replace('player_','')}", p, grain))
    return out


# --- known-formula derivations: atoms with NO direct witness column in an era but computable ------
# from more-basic atoms/plays we DO hold. These are "back-loggable" -- we compute, not scrape.
# {canonical_atom: (source, era_from, formula-note)}
# PROVEN (2026-07-14) by rolling up RAW pbp_merged per season, NOT a rollup file:
#   fumble flag + fumbled_1_player_id populated EVERY play from 1978 (984/984 in 1978) -> fumbles 1978+.
#   first_down_rush / first_down_pass populated from 1978 -> first downs 1978+ (super ALREADY has these).
#   epa populated 1978+ EXCEPT 1993 (epa=0 that season -> a real hole to flag).
#   fumble_lost flag only 1999+; the 1978-98 recovery-team derivation is PARTIAL (fumble_recovery_1_team
#     populated for only ~29% of fumbles pre-1999 vs ~93% in 1999+) -> PBP-alone UNDERCOUNTS lost fumbles
#     1978-98; NFL.com weekly logs / PFA season are needed to complete that window.
DERIVED_FORMULAS: dict[str, tuple[str, int, str]] = {
    "fumbles": ("pbp_merged", 1978, "COUNT(fumble=1) by fumbled_1_player_id-week; fumbler known 100% of plays 1978+"),
    "fumbles_lost": ("pbp_merged", 1999, "flag 1999+ complete; 1978-98 fumble_recovery_1_team<>posteam is PARTIAL (~29% coverage) -> use NFL.com/PFA to complete"),
    "rushing_first_downs": ("pbp_merged", 1978, "SUM(first_down_rush) by rusher-week (super already ingested 1978+)"),
    "receiving_first_downs": ("pbp_merged", 1978, "SUM(first_down_pass) by receiver-week (super already ingested 1978+)"),
    "passing_first_downs": ("pbp_merged", 1978, "SUM(first_down_pass) by passer-week (super already ingested 1978+)"),
    "epa": ("pbp_merged", 1978, "player-week EPA rollups from PBP epa/qb_epa/air_epa/yac_epa; NOTE 1993 hole (epa=0)"),
    # ATTRIBUTION RATES MEASURED 2026-07-16 (share of event plays that actually name the player). A
    # PBP-derived stat can never be more complete than its id column -- this is a witness-QUALITY property
    # that was never measured, and these three entries were overclaiming.
    "def_sacks": ("pbp_merged", 1978, "COUNT(sack=1) via sack_player_id + half_sack_1/2 (BOTH required: "
                  "sack_player_id alone is only 84-91%, half-sacks close it to 93.5% in 1978-81 and "
                  "98.8-99.4% from 1999). NOTE the 1978-81 fill we shipped is therefore ~6.5% short; no "
                  "better witness exists (PFR box sacks start 1982)."),
    "def_qb_hits": ("pbp_merged", 1999, "qb_hit_1/2_player_id -- NOT 1978: qb_hit attribution is 0% before "
                    "1990 (the stat was not tracked), 100% from 1990. Era floor corrected from 1978."),
    "def_safeties": ("pbp_merged", 1990, "COUNT(safety=1) by safety_player_id, ~62-66% attributed and that "
                     "is CORRECT football, not a gap: ~35% of safeties have no creditable defender "
                     "(intentional grounding in the end zone, snap out of bounds). Do NOT 'fix' to 100%."),
    # rate atoms (comp_pct, yards_per_att, passer_rating, ...) derive from their num/den atoms any era
}

# --- pending / external witness tiers (registered, not yet contract-mapped) ----------------------
# Newspaper-derived ancient atoms live in a separate local DuckDB (schema newspaper_promoted). They
# fill the deepest pre-PBP gaps but are PENDING CONFLICT RESOLUTION -- not live-promotable yet. The
# unified "supertable witness handoff" manifest does not exist yet; this registers the source so the
# matrix can flag ancient-era gaps as "newspaper-pending" rather than "true gap".
PENDING_WITNESSES = {
    "newspaper_promoted": {
        "db": str(LAKE / "derived/newspaper_atoms/databases/newspaper_atoms.duckdb"),
        "schema": "newspaper_promoted",
        "grain": "game",
        "status": "superseded_by_sidecar_bundle",
        "tables": ["play_by_play_event", "scoring_event", "player_game_box_score",
                   "lineup_participation", "team_game_stat_claim"],
        "note": "SUPERSEDED 2026-07-17 by newspaper_sidecar_bundle (immutable parquet bundle); "
                "the mutable DuckDB stays the extraction workbench, not the registered witness",
    },
    # Finalized architecture (Joe 2026-07-17): newspaper is a sidecar witness source, never a
    # supertable. Registered from the immutable bundle; NON-VOTING until identity (1118),
    # overlay-unmatched (73), and reviewer (9) hard-holds clear. The 203-col wide overlay
    # release is deprecated review-only and must never be registered or become latest_v26.
    "newspaper_sidecar_bundle": {
        "bundle": str(LAKE / "curated/witnesses/newspaper/20260717T065218Z_v1"),
        "manifest": str(LAKE / "curated/witnesses/newspaper/20260717T065218Z_v1/BUNDLE_MANIFEST.json"),
        "grain": "game",
        "status": "registered_witness_non_voting",
        "tables": ["newspaper_weekly_player_stat_cells", "newspaper_lineup_participation",
                   "newspaper_scoring_events", "newspaper_play_by_play_events",
                   "newspaper_player_game_notes", "newspaper_team_game_stats",
                   "newspaper_team_game_stat_claims", "newspaper_game_context",
                   "newspaper_reviewer_source_document_notes",
                   "newspaper_general_reviewer_accepted_decisions",
                   "newspaper_general_reviewer_hold_decisions"],
        "note": "sidecar-only witness bundle; recon lane = recon_newspaper_sidecar; "
                "supertable writes forbidden (build_newspaper_promotion_v26 --apply fail-closes)",
    },
}


# --- canonical atom map: witness-column-name -> super_table canonical atom ------------------
# Different witnesses name the same atom differently (PFR pass_yds == super passing_yards ==
# pbp passing_yards). One entry per known variant. Extend as new witnesses are wired.
ATOM_MAP: dict[str, str] = {
    # passing
    "pass_yds": "passing_yards", "passing_yards": "passing_yards",
    "pass_td": "passing_tds", "passing_tds": "passing_tds",
    "pass_int": "passing_interceptions", "passing_interceptions": "passing_interceptions",
    "pass_cmp": "completions", "completions": "completions",
    "pass_att": "attempts", "attempts": "attempts",
    "pass_sacked": "sacks_suffered", "sacks_suffered": "sacks_suffered",
    "pass_first_down": "passing_first_downs", "passing_first_downs": "passing_first_downs",
    # rushing
    "rush_yds": "rushing_yards", "rushing_yards": "rushing_yards",
    "rush_td": "rushing_tds", "rushing_tds": "rushing_tds",
    "rush_att": "carries", "carries": "carries",
    "rush_first_down": "rushing_first_downs", "rushing_first_downs": "rushing_first_downs",
    # receiving
    "rec": "receptions", "receptions": "receptions",
    "rec_yds": "receiving_yards", "receiving_yards": "receiving_yards",
    "rec_td": "receiving_tds", "receiving_tds": "receiving_tds",
    "targets": "targets",
    "rec_first_down": "receiving_first_downs", "receiving_first_downs": "receiving_first_downs",
    # turnovers (the gap family)
    "fumbles": "fumbles",
    "fumbles_lost": "fumbles_lost",
    # defense
    "def_int": "def_interceptions", "def_interceptions": "def_interceptions",
    "def_int_yds": "def_interception_yards", "def_interception_yards": "def_interception_yards",
    "def_int_td": "def_int_ret_td", "def_int_ret_td": "def_int_ret_td",
    "sacks": "def_sacks", "def_sacks": "def_sacks",
    "fumbles_rec": "fum_rec", "fum_rec": "fum_rec",
    "fumbles_rec_yds": "fum_rec_yds", "fum_rec_yds": "fum_rec_yds",
    "fumbles_rec_td": "fum_ret_td", "fum_ret_td": "fum_ret_td",
    "fumbles_forced": "def_fumbles_forced", "def_fumbles_forced": "def_fumbles_forced",
    "tackles_solo": "def_tackles_solo", "def_tackles_solo": "def_tackles_solo",
    "tackles_assists": "def_tackle_assists", "def_tackle_assists": "def_tackle_assists",
    "tackles_combined": "def_tackles_with_assist", "def_tackles_with_assist": "def_tackles_with_assist",
    "tackles_loss": "def_tackles_for_loss", "def_tackles_for_loss": "def_tackles_for_loss",
    "pass_defended": "def_pass_defended", "def_pass_defended": "def_pass_defended",
    "qb_hits": "def_qb_hits", "def_qb_hits": "def_qb_hits",
    "safety_md": "def_safeties", "def_safeties": "def_safeties",
    # kicking
    "fgm": "fg_made", "fg_made": "fg_made",
    "fga": "fg_att", "fg_att": "fg_att",
    "xpm": "pat_made", "pat_made": "pat_made",
    "xpa": "pat_att", "pat_att": "pat_att",
    "fg_long": "fg_long",
    # returns
    "punt_ret": "punt_returns", "punt_returns": "punt_returns",
    "punt_ret_yds": "punt_return_yards", "punt_return_yards": "punt_return_yards",
    "punt_ret_td": "punt_return_tds", "punt_return_tds": "punt_return_tds",
    "kick_ret": "kickoff_returns", "kickoff_returns": "kickoff_returns",
    "kick_ret_yds": "kickoff_return_yards", "kickoff_return_yards": "kickoff_return_yards",
    "kick_ret_td": "kickoff_return_tds", "kickoff_return_tds": "kickoff_return_tds",
    # punting
    "punt": "punts", "punts": "punts",
    "punt_yds": "punt_yards", "punt_yards": "punt_yards",
    "punt_blocked": "punts_blocked", "punts_blocked": "punts_blocked",
    # composites/longs already in super under a DIFFERENT name (wave52 build_derived_columns_v26) --
    # canonicalize witness synonyms so the recon stops FALSE-flagging them as addable gaps.
    "all_purpose_yds": "all_purpose_yards", "all_purpose_yards": "all_purpose_yards",
    "yds_per_touch": "yards_per_touch", "yards_per_touch": "yards_per_touch",
    "pass_long": "passing_long", "passing_long": "passing_long",
    "rush_long": "rushing_long", "rushing_long": "rushing_long",
    "rec_long": "receiving_long", "receiving_long": "receiving_long",
    # genuinely-missing composites (kept as their own atoms = real addable gaps):
    # yds_from_scrimmage (rush+rec yds), total_tds_scored (all TDs scored), rush_receive_td, av (PFR Approximate Value)
}

# --- NFL.com per-category canonicalization -------------------------------------------------------
# NFL.com bulk category pages are position/category-scoped, so their GENERIC columns (yds/td/att/lng)
# mean different atoms per category. Map the confident ones to canonical atoms; any unmapped generic
# column is namespaced `{cat}__{col}` (see _canonical) so passing-`td` never false-merges rushing-`td`.
# Corrected against the REAL harvested column keys (1932-2025 category parquets), not a-priori guesses.
# KEY finding: the season-grain category pages do NOT carry fum/lost -- the `fumbles` category is DEFENSIVE
# (ff/fr/fr_td), offensive fumbles are `rush_fum`/`rec_fum` in rushing/receiving, and `fumbles_lost` at
# season grain appears ONLY in the per-player logs/splits (harvested separately).
NFLCOM_CATEGORY_ATOMS: dict[str, dict[str, str]] = {
    "passing": {"pass_yds": "passing_yards", "att": "attempts", "cmp": "completions",
                "td": "passing_tds", "int": "passing_interceptions", "sck": "sacks_suffered",
                "scky": "sack_yards_suffered", "rate": "passer_rating", "1st": "passing_first_downs",
                "lng": "passing_long"},
    # NOTE (2026-07-16 audit): rush_fum/rec_fum are the per-PHASE fumble SPLITS, not total fumbles --
    # mapping them onto `fumbles` conflated a split with its parent and inflated `fumbles` coverage.
    "rushing": {"rush_yds": "rushing_yards", "att": "carries", "td": "rushing_tds",
                "rush_1st": "rushing_first_downs", "lng": "rushing_long", "rush_fum": "rushing_fumbles"},
    "receiving": {"rec": "receptions", "yds": "receiving_yards", "td": "receiving_tds",
                  "rec_1st": "receiving_first_downs", "lng": "receiving_long", "tgts": "targets",
                  "rec_fum": "receiving_fumbles", "rec_yac_r": "receiving_yac"},
    "fumbles": {"ff": "def_fumbles_forced", "fr": "fum_rec", "fr_td": "fum_ret_td"},  # DEFENSIVE fumbles
    "tackles": {"comb": "def_tackles_with_assist", "solo": "def_tackles_solo",
                "asst": "def_tackle_assists", "sck": "def_sacks"},
    "interceptions": {"int": "def_interceptions", "int_yds": "def_interception_yards",
                      "int_td": "def_int_ret_td", "lng": "def_int_long"},
    "field-goals": {"fgm": "fg_made", "att": "fg_att", "lng": "fg_long", "fg_blk": "fg_blocked",
                    "1_19_a_m": "fg_19", "20_29_a_m": "fg_29", "30_39_a_m": "fg_39",
                    "40_49_a_m": "fg_49", "50_59_a_m": "fg_59", "60_a_m": "fg_60plus"},
    "kickoffs": {"ko": "kickoffs", "tb": "kickoff_touchbacks", "yds": "kickoff_yards",
                 "ret_yds": "kickoff_return_yards_allowed", "osk": "onside_kicks"},
    "kickoff-returns": {"ret": "kickoff_returns", "yds": "kickoff_return_yards",
                        "kret_td": "kickoff_return_tds", "lng": "kickoff_return_long",
                        "avg": "kickoff_return_avg", "fc": "kickoff_fair_catches", "fum": "kickoff_return_fumbles"},
    "punts": {"punts": "punts", "net_yds": "punt_net_yards", "net_avg": "punt_net_avg",
              "lng": "punt_long", "p_blk": "punts_blocked", "in_20": "punts_inside_20",
              "dn": "punts_downed", "fc": "punt_fair_catches"},
    "punt-returns": {"ret": "punt_returns", "yds": "punt_return_yards", "td": "punt_return_tds",
                     "lng": "punt_return_long", "avg": "punt_return_avg", "fc": "punt_fair_catches",
                     "fum": "punt_return_fumbles"},
}
# generic columns that MUST be namespaced by category when not explicitly mapped (else false-merge)
_NFLCOM_GENERIC = {"yds", "td", "att", "lng", "avg", "1st", "1st_2", "20", "40", "rec", "cmp",
                   "cmp_2", "int", "g", "gs", "pct", "yds_2", "td_2", "avg_2", "lng_2", "att_2"}

_YEAR_COLS = ("year_id", "season", "year")
_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _year_col(cols: set[str]) -> str | None:
    for c in _YEAR_COLS:
        if c in cols:
            return c
    return None


# metadata / identity / text columns that are NOT stats (excluded from the catalog)
_META_EXACT = {
    "pfr_id", "player", "year_id", "season", "year", "week", "boxscore_id", "team", "pos",
    "position", "nfl_position", "age", "NFL_player_id", "game_date", "boxscore_url", "source_url",
    "table_id", "table_caption", "row_index_in_table", "tr_data_row", "index_letter", "gsis_id",
    "index_position", "first_year", "last_year", "page_key", "page_kind", "page_url", "subpage_year",
    "scraped_at_utc", "team_name_abbr", "comp_name_abbr", "home_stathead_id", "player_id", "game_id",
    "play_id", "old_game_id", "desc", "recent_team", "headshot_url", "player_week", "franchise_id",
}
_META_SUFFIX = ("_links_json", "_link_texts", "_link_ids", "_urls", "_id", "_name", "_json", "_url", "_desc")


# NFL.com harvester provenance/identity columns (never stats)
_META_NFLCOM = {"nflcom_slug", "opp", "result", "game_date", "wk", "season_type", "team",
                "_player_slug", "_team_slug", "_category", "_side", "_view", "_table"}


def _is_stat_col(col: str, dtype: str) -> bool:
    """A stat column = numeric/castable, not an identity/text/metadata field."""
    if col in _META_EXACT or col in _META_NFLCOM or col.startswith("_"):
        return False
    low = col.lower()
    if any(low.endswith(s) for s in _META_SUFFIX):
        return False
    t = dtype.upper()
    numeric = any(k in t for k in ("INT", "DOUBLE", "DECIMAL", "FLOAT", "BIGINT", "HUGEINT", "REAL", "SMALLINT", "TINYINT"))
    # VARCHAR stat columns exist (PFR scrapes numbers as text) -> keep, coverage query TRY_CASTs them
    return numeric or t.startswith("VARCHAR")


def _canonical(col: str, witness: str = "") -> str:
    """Group synonym columns across witnesses onto one canonical atom. For NFL.com bulk category
    witnesses the mapping is category-scoped (generic cols namespaced `{cat}__{col}` so passing-`td`
    never merges rushing-`td`). Otherwise known aliases via ATOM_MAP; unmapped raw name is its own atom."""
    if witness.startswith("nflcom_season:"):
        cat = witness.split(":", 1)[1]
        cmap = NFLCOM_CATEGORY_ATOMS.get(cat, {})
        if col in cmap:
            return cmap[col]
        if col in _NFLCOM_GENERIC:
            return f"{cat}__{col}"
    return ATOM_MAP.get(col, col)


def build_contracts(min_rows: int = 20) -> dict:
    """Introspect EVERY witness column -> {witness: {grain, atoms: {atom: {col, era_min, era_max, seasons}}}}.

    Exhaustive: every stat column of every source is cataloged (not a hand-picked subset). Synonyms
    are grouped via _canonical; unmapped columns keep their raw name as their own atom."""
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("PRAGMA threads=4")
    contracts: dict[str, dict] = {}
    for name, path, grain in _witnesses():
        try:
            desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')").fetchall()
        except Exception:
            continue
        cols = {r[0]: r[1] for r in desc}
        yc = _year_col(set(cols))
        if not yc:
            continue
        atoms: dict[str, dict] = {}
        for col, dtype in cols.items():
            if not _is_stat_col(col, dtype):
                continue
            try:
                row = con.execute(
                    f"SELECT MIN(y), MAX(y), COUNT(*) FROM ("
                    f"  SELECT TRY_CAST({yc} AS INT) y FROM read_parquet('{path.as_posix()}')"
                    f"  WHERE regexp_matches(CAST({yc} AS VARCHAR),'^[0-9]{{4}}$') AND {_D(col)}>0"
                    f"  GROUP BY y HAVING COUNT(*)>={min_rows})"
                ).fetchone()
            except Exception:
                continue
            if row and row[0] is not None:
                atom = _canonical(col, name)
                # keep the widest-era instance if two raw cols canonicalize together within a witness
                prev = atoms.get(atom)
                if prev is None or int(row[0]) < prev["era_min"]:
                    atoms[atom] = {"col": col, "era_min": int(row[0]), "era_max": int(row[1]), "seasons": int(row[2])}
        if atoms:
            contracts[name] = {"grain": grain, "path": str(path), "atoms": atoms}
    con.close()
    return contracts


def completeness_matrix(contracts: dict) -> dict:
    """Invert contracts -> {atom: {game:[witness..], season:[witness..], era spans}}."""
    matrix: dict[str, dict] = {}
    for wname, c in contracts.items():
        grain_bucket = "game" if c["grain"] == "game" else "season"
        for atom, info in c["atoms"].items():
            m = matrix.setdefault(atom, {"game": [], "season": [], "game_era": None, "season_era": None})
            m[grain_bucket].append(f"{wname}[{info['era_min']}-{info['era_max']}]")
            key = f"{grain_bucket}_era"
            lo, hi = info["era_min"], info["era_max"]
            if m[key] is None:
                m[key] = [lo, hi]
            else:
                m[key] = [min(m[key][0], lo), max(m[key][1], hi)]
    return matrix


def position_pivot(atom: str, by: str = "nfl_position", min_rows: int = 1) -> dict:
    """Pivot an atom's coverage in the SUPER table by position × decade so per-position/era gaps
    surface (does a 1950 LDH carry what a modern LCB does?). `by` in ('nfl_position','position').
    Returns {position: {decade: nonzero_rows}} plus the era span per position."""
    st = _release_super_table()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'"); con.execute("PRAGMA threads=4")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{st}')").fetchall()}
    if atom not in cols or by not in cols:
        con.close()
        return {"error": f"{atom!r} or {by!r} not in super table"}
    rows = con.execute(
        f"SELECT {by} AS pos, ((CAST(year AS INT)/10)*10) AS decade, COUNT(*) AS n "
        f"FROM read_parquet('{st}') WHERE {_D(atom)}<>0 AND {by} IS NOT NULL "
        f"GROUP BY 1,2 HAVING COUNT(*)>={min_rows} ORDER BY 1,2"
    ).fetchall()
    con.close()
    piv: dict[str, dict] = {}
    for pos, dec, n in rows:
        piv.setdefault(str(pos), {})[int(dec)] = int(n)
    return piv


def run(json_out: str | None = None, min_rows: int = 20) -> dict:
    contracts = build_contracts(min_rows=min_rows)
    matrix = completeness_matrix(contracts)
    if json_out:
        Path(json_out).write_text(json.dumps({"contracts": contracts, "matrix": matrix}, indent=2, sort_keys=True))
    return {"contracts": contracts, "matrix": matrix}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default=None, help="write contract registry JSON to this path")
    ap.add_argument("--min-rows", type=int, default=20)
    ap.add_argument("--pivot", default=None, help="pivot this atom's super-table coverage by position × decade")
    ap.add_argument("--pivot-by", default="nfl_position", choices=["nfl_position", "position"])
    a = ap.parse_args()
    if a.pivot:
        piv = position_pivot(a.pivot, by=a.pivot_by)
        print(f"POSITION PIVOT for {a.pivot!r} by {a.pivot_by} (nonzero rows per decade):")
        decades = sorted({d for v in piv.values() if isinstance(v, dict) for d in v})
        print(f"  {'pos':10}" + "".join(f"{d:>7}" for d in decades))
        for pos in sorted(piv):
            v = piv[pos]
            if not isinstance(v, dict):
                print(f"  {pos}: {v}"); continue
            print(f"  {pos:10}" + "".join(f"{v.get(d,''):>7}" for d in decades))
        raise SystemExit
    res = run(json_out=a.json, min_rows=a.min_rows)
    contracts, matrix = res["contracts"], res["matrix"]
    # --- scale report: exhaustive catalog, not a hand-picked subset ---
    col_instances = sum(len(c["atoms"]) for c in contracts.values())
    con = duckdb.connect()
    super_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{_release_super_table()}')").fetchall()}
    con.close()
    witnessed = sorted(a for a in matrix if a in super_cols)
    super_orphans = sorted(c for c in super_cols if c not in matrix)  # in super table, no witness col named it
    witness_only = sorted(a for a in matrix if a not in super_cols)   # witnessed atom absent from super table
    print("=" * 78)
    print(f"WITNESS CATALOG (exhaustive): {len(contracts)} sources, {col_instances} column-instances, "
          f"{len(matrix)} distinct canonical atoms")
    print(f"super table has {len(super_cols)} cols | {len(witnessed)} match a witnessed atom | "
          f"{len(witness_only)} witnessed atoms NOT in super table | {len(super_orphans)} super cols with no like-named witness")
    print("=" * 78 + "\n")
    print("WITNESSED ATOMS NOT IN THE SUPER TABLE (candidate new columns to add):")
    print("  " + ", ".join(witness_only[:60]) + (" ..." if len(witness_only) > 60 else ""))
    print()
    # completeness matrix: for each atom, earliest GAME-grain witness vs earliest SEASON-grain
    print(f"{'ATOM':26}{'GAME-grain from':>16}{'SEASON-grain from':>18}   weekly-gap era")
    print("-" * 90)
    for atom in sorted(matrix):
        m = matrix[atom]
        g0 = m["game_era"][0] if m["game_era"] else None
        s0 = m["season_era"][0] if m["season_era"] else None
        # weekly gap = span where a season witness exists but no game witness
        gap = ""
        if s0 is not None and (g0 is None or g0 > s0):
            gap = f"{s0}-{(g0 - 1) if g0 else 'now'}  (season-only)"
        print(f"{atom:26}{str(g0 or '-'):>16}{str(s0 or '-'):>18}   {gap}")
    if a.json:
        print(f"\nregistry JSON -> {a.json}")
