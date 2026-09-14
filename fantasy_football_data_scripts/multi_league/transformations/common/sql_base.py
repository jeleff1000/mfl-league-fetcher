"""
SQL Enrichments Base Class

Contains constructor, connection management, shared helpers, and settings loading
for the SQL enrichments pipeline. All domain-specific mixin classes inherit from this.
"""

import logging
from typing import Any


from multi_league.core.sql_utils import validate_db_name, validate_scoped_sql
from multi_league.core.local_db import _TABLE_COLUMN_TYPES
from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row
from multi_league.core.roster_slots import (
    NON_STARTER_SLOTS,
    IDP_FAMILIES,
    eligible_positions,
    is_flex,
    resolve,
)
from multi_league.core.join_keys import manager_identity_sql
from multi_league.core.player_week_identity import player_week_repair_statements
from multi_league.transformations.player.modules.column_mapper import (
    get_position_rank_cols,
)
from multi_league.transformations.player.modules.scoring_calculator import (
    _BONUS_THRESHOLD_KEY_TO_SOURCE,
)

logger = logging.getLogger(__name__)

SUPPORTED_TD_PTS = [4, 5, 6]
_STRICT_CANONICAL_TABLES = frozenset(_TABLE_COLUMN_TYPES)

# ---------------------------------------------------------------------------
# Scoring parser maps — module-level so audit_scoring_keys.py can import them
# and verify coverage against ALL_SCORING_KEYS without running the pipeline.
#
# Keys use the "scoring_" prefix (matching league_settings DDL columns).
# Values are super_table column names.
# ---------------------------------------------------------------------------

# DEF (DST team defense) — maps scoring_* setting → pts_def_* super_table column
DEF_COL_MAP: dict[str, str] = {
    "scoring_sack": "pts_def_sack",
    "scoring_int": "pts_def_int",
    "scoring_fum_rec": "pts_def_fr",
    "scoring_def_td": "pts_def_td",
    # Split DEF TD types — now map to dedicated columns (Task 4)
    "scoring_def_fum_ret_td": "pts_def_fum_ret_td",
    "scoring_def_int_ret_td": "pts_def_int_ret_td",
    "scoring_def_kr_td": "pts_def_kr_td",
    "scoring_def_pr_td": "pts_def_pr_td",
    "scoring_def_blk_kick_td": "pts_def_blk_kick_td",
    "scoring_def_st_td": "pts_def_st_td",
    "scoring_safe": "pts_def_safety",
    "scoring_one_pt_safe": "pts_def_safety",
    "scoring_blk_kick": "pts_def_block",
    "scoring_def_fg_block": "pts_def_fg_block",
    "scoring_fg_block": "pts_def_fg_block",
    "scoring_def_punt_block": "pts_def_punt_block",
    "scoring_punt_block": "pts_def_punt_block",
    "scoring_def_pat_block": "pts_def_pat_block",
    "scoring_pat_block": "pts_def_pat_block",
    "scoring_ff": "pts_def_ff",
    "scoring_tkl_loss": "pts_def_tfl",
    "scoring_def_3_and_out": "pts_def_3out",
    "scoring_def_4_and_stop": "pts_def_4stop",
    # Points allowed brackets
    "scoring_pts_allow_0": "pts_allow_0",
    "scoring_pts_allow_1_6": "pts_allow_1_6",
    "scoring_pts_allow_7_13": "pts_allow_7_13",
    "scoring_pts_allow_14_20": "pts_allow_14_20",
    "scoring_pts_allow_21_27": "pts_allow_21_27",
    "scoring_pts_allow_28_34": "pts_allow_28_34",
    "scoring_pts_allow_35p": "pts_allow_35_plus",
    # ESPN alternate points-allowed brackets
    "scoring_pts_allow_46p": "pts_allow_35_plus",  # ESPN coarse — map to nearest
    "scoring_pts_allow_14_20_alt": "pts_allow_14_20",  # ESPN alternate — same bucket
    # Yards allowed brackets
    # Legacy Yahoo setting rows sometimes store the low-yardage bracket under
    # scoring_yds_allow_neg. Keep that alias, but let the canonical
    # scoring_yds_allow_0_100 entry win when both are present.
    "scoring_yds_allow_neg": "yds_allow_0_99",
    "scoring_yds_allow_0_100": "yds_allow_0_99",
    "scoring_yds_allow_100_199": "yds_allow_100_199",
    "scoring_yds_allow_200_299": "yds_allow_200_299",
    "scoring_yds_allow_300_349": "yds_allow_300_349",
    "scoring_yds_allow_350_399": "yds_allow_350_399",
    "scoring_yds_allow_400_449": "yds_allow_400_449",
    "scoring_yds_allow_450_499": "yds_allow_450_499",
    "scoring_yds_allow_500_549": "yds_allow_500_549",
    "scoring_yds_allow_550p": "yds_allow_550_plus",
    # Sleeper-specific DEF advanced stats (Task 4 columns)
    "scoring_def_pass_def": "pts_def_pass_def",
    "scoring_sack_yd": "pts_def_sack_yd",
    "scoring_def_2pt": "pts_def_2pt",
    "scoring_st_ff": "pts_def_st_ff",
    "scoring_def_st_ff": "pts_def_st_ff",  # alias — same column
    "scoring_st_fum_rec": "pts_def_st_fum_rec",
    "scoring_def_st_fum_rec": "pts_def_st_fum_rec",  # alias — same column
    "scoring_def_forced_punts": "pts_def_forced_punts",
    # DST misc: yards allowed (aggregate), points allowed (aggregate)
    # pts_def_ya = yards allowed (pre-existing aggregate column)
    "scoring_yds_allow": "pts_def_ya",
    # pts_allow = aggregate scalar (pre-existing — not a bracket)
    "scoring_pts_allow": "pts_allow",
    # Aliases for IDP-style stats that DST scoring also references
    # (Sleeper uses the same key name for both DST team and IDP player contexts)
    "scoring_qb_hit": "pts_idp_qb_hit",
    "scoring_tkl": "pts_idp_tackle_solo",
    "scoring_tkl_ast": "pts_idp_tackle_assist",
    "scoring_tkl_solo": "pts_idp_tackle_solo",
    "scoring_def_st_tkl_solo": "pts_idp_tackle_solo",
    # Special teams return yards (all roll into pts_def_ret_yd aggregate)
    "scoring_def_st_yd": "pts_def_ret_yd",
    "scoring_def_kr_yd": "pts_def_ret_yd",
    "scoring_def_pr_yd": "pts_def_ret_yd",
    "scoring_fg_ret_yd": "pts_def_ret_yd",
    "scoring_blk_kick_ret_yd": "pts_def_ret_yd",
    # DST team result / score / margin components (ESPN team scoring)
    "scoring_team_win": "pts_def_team_win",
    "scoring_team_loss": "pts_def_team_loss",
    "scoring_team_tie": "pts_def_team_tie",
    "scoring_team_pts": "pts_def_team_pts",
    "scoring_team_margin": "pts_def_team_margin",
    "scoring_team_win_margin_25p": "pts_def_team_win_margin_25p",
    "scoring_team_win_margin_20_24": "pts_def_team_win_margin_20_24",
    "scoring_team_win_margin_15_19": "pts_def_team_win_margin_15_19",
    "scoring_team_win_margin_10_14": "pts_def_team_win_margin_10_14",
    "scoring_team_win_margin_5_9": "pts_def_team_win_margin_5_9",
    "scoring_team_win_margin_1_4": "pts_def_team_win_margin_1_4",
    "scoring_team_loss_margin_1_4": "pts_def_team_loss_margin_1_4",
    "scoring_team_loss_margin_5_9": "pts_def_team_loss_margin_5_9",
    "scoring_team_loss_margin_10_14": "pts_def_team_loss_margin_10_14",
    "scoring_team_loss_margin_15_19": "pts_def_team_loss_margin_15_19",
    "scoring_team_loss_margin_20_24": "pts_def_team_loss_margin_20_24",
    "scoring_team_loss_margin_25p": "pts_def_team_loss_margin_25p",
}

# IDP (individual defensive players) — maps scoring_idp_* → pts_idp_* super_table column
IDP_COL_MAP: dict[str, str] = {
    "scoring_idp_tkl_solo": "pts_idp_tackle_solo",
    "scoring_idp_tkl_ast": "pts_idp_tackle_assist",
    "scoring_idp_sack": "pts_idp_sack",
    "scoring_idp_int": "pts_idp_int",
    "scoring_idp_ff": "pts_idp_ff",
    "scoring_idp_fum_rec": "pts_idp_fr",
    "scoring_idp_pass_def": "pts_idp_pd",
    "scoring_idp_qb_hit": "pts_idp_qb_hit",
    "scoring_idp_tkl_loss": "pts_idp_tfl",
    "scoring_idp_safe": "pts_idp_safety",
    "scoring_idp_def_td": "pts_idp_td",
    "scoring_idp_tkl": "pts_idp_tkl_combined",
    # New IDP columns added via 2026-04-12 migration (Task 5 audit gap fix)
    "scoring_idp_blk_kick": "pts_idp_blk_kick",
    "scoring_idp_blk_kick_td": "pts_idp_blk_kick_td",
    "scoring_idp_fum_rec_yd": "pts_idp_fum_rec_yd",
    "scoring_idp_fum_ret_td": "pts_idp_fum_ret_td",
    "scoring_idp_int_ret_yd": "pts_idp_int_ret_yd",
    "scoring_idp_xpr": "pts_idp_xpr",
    "scoring_idp_pass_def_3p": "pts_idp_pass_def_3p",
    # Shared with DST columns (same underlying data, individual vs. team attribution)
    "scoring_idp_sack_yd": "pts_def_sack_yd",
    "scoring_idp_pts_allow_0": "pts_allow_0",
    "scoring_idp_pts_allow_1_6": "pts_allow_1_6",
    "scoring_idp_pts_allow_7_13": "pts_allow_7_13",
    "scoring_idp_pts_allow_14_20": "pts_allow_14_20",
}

# BONUS (offense milestones) — maps scoring_bonus_* → bonus_* super_table column
BONUS_COL_MAP: dict[str, str] = {
    "scoring_bonus_pass_yd_300": "bonus_pass_300yd",
    "scoring_bonus_pass_yd_400": "bonus_pass_400yd",
    "scoring_bonus_rush_yd_100": "bonus_rush_100yd",
    "scoring_bonus_rush_yd_200": "bonus_rush_200yd",
    "scoring_bonus_rec_yd_100": "bonus_rec_100yd",
    "scoring_bonus_rec_yd_200": "bonus_rec_200yd",
    "scoring_bonus_pass_cmp_25": "bonus_pass_25cmp",
    "scoring_bonus_rush_att_20": "bonus_rush_20att",
    "scoring_bonus_rush_rec_yd_100": "bonus_rush_rec_100yd",
    "scoring_bonus_rush_rec_yd_200": "bonus_rush_rec_200yd",
    # DEF/K bonus milestones (Task 4 columns)
    "scoring_bonus_sack_2p": "pts_def_bonus_sack_2p",
    "scoring_bonus_tkl_10p": "pts_def_bonus_tkl_10p",
    "scoring_bonus_def_int_td_50p": "pts_def_bonus_int_td_50p",
    "scoring_bonus_def_fum_td_50p": "pts_def_bonus_fum_td_50p",
}

# KICK (kicker) — maps scoring_fgm_* / scoring_xp* → fg_made_* super_table column
KICK_COL_MAP: dict[str, str] = {
    # FG made by distance bracket
    "scoring_fgm_0_19": "fg_made_0_19",
    "scoring_fgm_0_39": "fg_made_0_19",  # coarse bracket — expanded in _load_scoring_from_settings
    "scoring_fgm_20_29": "fg_made_20_29",
    "scoring_fgm_30_39": "fg_made_30_39",
    "scoring_fgm_40_49": "fg_made_40_49",
    "scoring_fgm_50_59": "fg_made_50_59",
    "scoring_fgm_50p": "fg_made_50_59",  # coarse bracket — expanded in _load_scoring_from_settings
    "scoring_fgm_60p": "fg_made_60_",
    # FG missed by distance bracket
    "scoring_fgmiss": "fg_missed",
    "scoring_fgmiss_0_19": "fg_missed_0_19",
    "scoring_fgmiss_0_39": "fg_missed_0_19",  # coarse — expanded in _load_scoring_from_settings
    "scoring_fgmiss_20_29": "fg_missed_20_29",
    "scoring_fgmiss_30_39": "fg_missed_30_39",
    "scoring_fgmiss_40_49": "fg_missed_40_49",
    "scoring_fgmiss_50p": "fg_missed_50_59",
    "scoring_fgmiss_50_59": "fg_missed_50_59",
    "scoring_fgmiss_60p": "fg_missed_60_",
    # Yardage-based (0.1 per yard)
    "scoring_fgm_yds": "fg_made_distance",
    # PAT
    "scoring_xpm": "pat_made",
    "scoring_xpmiss": "pat_missed",
    # Flat per-FG (platform alt keys)
    "scoring_fgm": "fg_made",  # total FGs made; super_table.fgm is legacy VARCHAR text
    "scoring_fgm_yds_over_30": "fg_yds_over_30",  # existing column name
    # Kicker alt brackets (ESPN)
    "scoring_fgm_50_59_alt": "fg_made_50_59",
    "scoring_fgm_60p_alt": "fg_made_60_",
    # Kicker percentage (rare)
    "scoring_fg_pct": "fg_pct",
}

# ---------------------------------------------------------------------------
# Offense key buckets — used by audit_scoring_keys.py to verify coverage
# without needing to trace into scoring_calculator.py's precalc paths.
# ---------------------------------------------------------------------------

# Keys absorbed into fpts_* precalculated columns by scoring_calculator.py.
# These never go through *_COL_MAP — the precalc path handles them entirely.
# NOTE: st_ff / st_fum_rec are NOT here — they appear in DEF_COL_MAP (DST team
# side). The same Sleeper key name is shared between individual-player ST scoring
# and DST-team ST scoring; DEF_COL_MAP owns it so the audit doesn't double-count.
OFFENSE_PRECALC_KEYS: set[str] = {
    "rec",
    "pass_td",
    "pass_int",
    "pass_yd",
    "rush_yd",
    "rec_yd",
    "rush_td",
    "rec_td",
    "fum_lost",
    "pass_2pt",
    "rush_2pt",
    "rec_2pt",
    "rec_targets",
    "pass_inc",
    "pass_att",
    "pass_cmp",
    "rec_0_4",
    "rec_5_9",
    "rec_10_19",
    "rec_20_29",
    "rec_30_39",
    "rush_40p",
    "rush_td_40p",
    "rush_td_50p",
    "pass_td_40p",
    "pass_td_50p",
    "rec_40p",
    "rec_td_40p",
    "rec_td_50p",
    "rec_40p_alt",
    "pass_cmp_40p",
    "pass_cmp_50p",
    "pass_fd",
    "rush_fd",
    "rec_fd",
    "pass_int_td",
    "pass_sack",
    "fum",
    "fum_rec_td",
    # Individual player ST stats (not DST team — scored as offense player)
    "st_yd",
    "st_td",
    "kr_yd",
    "st_tkl_solo",
    "pr_yd",
    "int_ret_yd",
    "fum_ret_yd",
}

# Keys handled by the offense corrections list (non-default multipliers).
# NOTE: bonus_*_yd_* milestone keys are NOT here — BONUS_COL_MAP owns them.
OFFENSE_CORRECTION_KEYS: set[str] = {
    "rush_att",
    "bonus_rush_att_20",
    "bonus_pass_cmp_25",
    "bonus_rec_te",
    "bonus_rush_rec_yd_100",
    "bonus_rush_rec_yd_200",
    "bonus_rec_rb",
    "bonus_rec_wr",
}

# Keys that exist in DDL but are intentionally not scored / display-only.
EXPLICITLY_IGNORED_KEYS: set[str] = {
    "scoring_type",  # display metadata, not a scoring multiplier
    "scoring_variant",  # display metadata
    "scoring_variant_first_year",  # display metadata
    "uses_fractional_points",  # scoring SEMANTICS (floor vs decimal), not a per-stat multiplier
}


SPLIT_TD_KEYS = ("def_int_ret_td", "def_fum_ret_td", "def_blk_kick_td")


def _build_def_multipliers(scoring_settings: dict) -> dict[str, float]:
    """Build def_multipliers dict from a per-year scoring_settings dict.

    Includes mode-detect to prevent double-counting when both unified def_td
    and split TD keys are populated. The unified def_td multiplier is zeroed
    when any split TD key has a non-zero value.

    Args:
        scoring_settings: dict with bare keys (no 'scoring_' prefix)

    Returns:
        dict mapping super_table column name → multiplier
    """
    def_mults = {}
    for flat_col, super_col in DEF_COL_MAP.items():
        bare_key = flat_col.removeprefix("scoring_")
        val = scoring_settings.get(bare_key)
        if val is not None and float(val) != 0:
            if bare_key == "one_pt_safe" and super_col in def_mults:
                continue
            def_mults[super_col] = float(val)

    # NOTE: bonus_sack_2p / bonus_tkl_10p / bonus_def_int_td_50p / bonus_def_fum_td_50p
    # are Sleeper IDP bonuses (applied per individual defender, not per team defense).
    # They exist in BONUS_COL_MAP targeting pts_def_bonus_* columns that Task 11 backfilled
    # at team-week granularity, but team-level application produces wrong values (e.g.,
    # every NFL team has >=10 combined tackles, so bonus_tkl_10p would always add +5).
    # They should eventually be routed to IDP scoring with per-player aggregation.
    # For Phase 1 rollout: DST scoring skips these bonus multipliers. Follow-up PR can
    # add pts_idp_bonus_* columns + IDP scoring wiring.

    # Mode-detect: if any split TD key is populated, zero out unified def_td
    # to prevent double-counting at SQL build time.
    # NOTE: only TRUE def_td splits (int_ret_td, fum_ret_td, blk_kick_td) — NOT
    # special teams TDs (def_st_td, def_kr_td, def_pr_td) which are independent
    # scoring categories that coexist with def_td without double-counting.
    has_split_tds = any(scoring_settings.get(k) and float(scoring_settings[k]) != 0 for k in SPLIT_TD_KEYS)
    if has_split_tds and "pts_def_td" in def_mults:
        def_mults["pts_def_td"] = 0.0

    # fum_rec_td: conditionally include as DEF scoring only when no other
    # DEF TD source already covers fumble recovery TDs.
    #
    # Three cases in the fleet:
    #   1) 419 leagues: def_td=6, fum_rec_td=6 → def_td already includes fumble
    #      TDs via pts_def_td. Adding fum_rec_td would double-count. SKIP.
    #   2) 26 leagues: split TDs (def_fum_ret_td=6) + fum_rec_td=6 → already
    #      covered by pts_def_fum_ret_td in DEF_COL_MAP. SKIP.
    #   3) 26 leagues: def_td=0, no split TDs, fum_rec_td=6 → this is their
    #      only DEF fumble TD source. Every fumble recovery TD scores 0
    #      without this. INCLUDE.
    if ("pts_def_td" not in def_mults or def_mults.get("pts_def_td", 0) == 0) and "pts_def_fum_ret_td" not in def_mults:
        fum_rec_td_val = scoring_settings.get("fum_rec_td")
        if fum_rec_td_val is not None and float(fum_rec_td_val) != 0:
            def_mults["pts_def_fum_rec_td"] = float(fum_rec_td_val)

    return def_mults


def _assert_known_scoring_keys(scoring_settings: dict, league: str, year: int) -> None:
    """Hard fail if scoring_settings contains a populated key the parser doesn't know.

    This is the pre-flight key assertion (spec §4.5(a)). Catches gaps at parser-build
    time before SQL is generated. After PR 1 ships the schema-lock test in CI, this
    can be downgraded to a warning, but for Phase 1 rollout it stays hard.
    """
    from multi_league.core.scoring_config import (
        IRREDUCIBLE_SCORING_KEYS,
        yahoo_stat_modifier_bonus_source_threshold,
    )

    parser_known: set[str] = set()
    for col_map in (DEF_COL_MAP, IDP_COL_MAP, KICK_COL_MAP, BONUS_COL_MAP):
        for k in col_map:
            parser_known.add(k.removeprefix("scoring_"))
    parser_known |= OFFENSE_PRECALC_KEYS
    parser_known |= OFFENSE_CORRECTION_KEYS
    parser_known |= set(_BONUS_THRESHOLD_KEY_TO_SOURCE)
    parser_known |= {k.removeprefix("scoring_") for k in EXPLICITLY_IGNORED_KEYS}
    parser_known |= set(IRREDUCIBLE_SCORING_KEYS.keys())

    unknown = {
        k
        for k, v in scoring_settings.items()
        if v not in (None, 0) and k not in parser_known and yahoo_stat_modifier_bonus_source_threshold(k) is None
    }
    if unknown:
        raise ValueError(
            f"Unknown scoring keys for {league}/{year}: {sorted(unknown)}. "
            f"Add to a *_COL_MAP, OFFENSE_*_KEYS, or IRREDUCIBLE_SCORING_KEYS."
        )


def _normalize_td_pts(pass_td_pts) -> int:
    """Map pass_td_pts to nearest supported rank column variant (4/5/6).

    The super_table only has rank columns for 4pt, 5pt, and 6pt passing TDs.
    Leagues with non-standard values (e.g., 3pt) are mapped to the nearest
    supported variant so rank lookups don't fail with BinderError.
    """
    td_int = int(pass_td_pts)
    if td_int in SUPPORTED_TD_PTS:
        return td_int
    nearest = min(SUPPORTED_TD_PTS, key=lambda x: abs(x - td_int))
    logger.warning(f"[SCORING] {td_int}pt passing TD has no rank columns, using {nearest}pt ranks")
    return nearest


class SQLEnrichmentsBase:
    """Base class providing infrastructure for SQL-based enrichments.

    All operations use UPDATE ... FROM ... WHERE syntax to modify tables
    in place without downloading data to Python.

    NOTE: DuckDB UPDATE syntax requires unqualified column names in SET clause:
    - WRONG:  UPDATE t SET t.col = s.col FROM s WHERE t.id = s.id
    - RIGHT:  UPDATE t SET col = s.col FROM s WHERE t.id = s.id
    """

    # Non-starting roster slots — imported from core.roster_slots
    NON_STARTER_SLOTS = NON_STARTER_SLOTS

    def __init__(
        self,
        db_name: str,
        dry_run: bool = False,
        schema: str = "public",
        roster_by_year: dict[Any, dict[str, int]] | None = None,
        ppr: float = 0.5,
        pass_td_pts: int = 4,
        idp_scoring: str = "std",
        data_dir: str | None = None,
        quick: bool = False,
        conn=None,
        keeper_config_hydrated: bool = False,
        manager_name_overrides: dict[str, str] | None = None,
        franchise_merges: list[dict[str, Any]] | None = None,
    ):
        """Initialize the SQL enrichments runner.

        Args:
            db_name: Database name (e.g., 'kmffl')
            dry_run: If True, print SQL queries but don't execute
            schema: Database schema to use (default: 'public')
            roster_by_year: Dict mapping year to roster settings for league_wide_optimal.
                           Example: {2024: {"QB": 1, "RB": 2, ...}, 2023: {...}}
                           If None, uses default roster settings.
            ppr: Points per reception (0, 0.5, or 1.0) for rank column selection
            pass_td_pts: Points per passing TD (4 or 6) for rank column selection
            idp_scoring: IDP scoring variant ('std', 'premium', 'tackle_heavy', 'big_play')
            data_dir: Local data directory containing {db_name}.duckdb. When provided,
                     enrichments run on local DuckDB.
            quick: True for single-year quick-import runs. Expansion steps should
                   stay limited to the imported season instead of full NFL history.
            conn: Existing DuckDB connection to reuse. When provided, enrichments
                  run on this connection instead of opening a new one. The caller
                  retains ownership — close() becomes a no-op.
        """
        self.db_name = db_name
        self.dry_run = dry_run
        self.schema = schema
        self.data_dir = data_dir
        self.quick = quick
        self.roster_by_year = roster_by_year
        self.ppr = ppr
        self.pass_td_pts = pass_td_pts
        self.idp_scoring = idp_scoring
        self.ret_yds = 0
        self.idp_multipliers = {}
        self.def_multipliers = {}
        self.bonus_multipliers = {}
        self.kick_multipliers = {}
        self.te_premium = 0.0
        self.kick_col = "pts_k_std"
        self._scoring_params = {}
        self._conn = conn
        self._owns_conn = conn is None  # only close if we created it
        self.keeper_config_hydrated = bool(keeper_config_hydrated)
        self.manager_name_overrides = dict(manager_name_overrides or {})
        self.franchise_merges = list(franchise_merges or [])
        self._ops_attached = False
        self._table_cache: dict[str, bool] = {}
        self._column_cache: dict[str, set[str]] = {}

        # Build position rank column mapping based on scoring settings
        # These are the pre-computed rank columns in super_table
        # Single source of truth lives in column_mapper.get_position_rank_cols()
        ppr_key = {0: "0ppr", 0.5: "half", 1.0: "ppr"}.get(ppr, "half")
        td_key = f"{_normalize_td_pts(pass_td_pts)}pt"

        self.POSITION_RANK_COLS = get_position_rank_cols(ppr_key, td_key, idp_scoring)

        # IDP position families — sourced from core.roster_slots.IDP_FAMILIES
        # Convert sets to lists for backward compatibility with callers
        self.IDP_POSITION_FAMILIES = {k: list(v) for k, v in IDP_FAMILIES.items()}

        logger.info(f"[SQLEnrichments] Rank columns: {ppr} PPR, {pass_td_pts}pt TD, {idp_scoring} IDP")
        logger.info(f"[SQLEnrichments] Sample rank col: {self.POSITION_RANK_COLS['QB']}")

    @property
    def conn(self):
        """Backward-compatible property accessor for database connection."""
        return self._get_connection()

    @staticmethod
    def _primary_position_sql(column: str = "position") -> str:
        """Return SQL expression for the normalized primary position token."""
        return f"UPPER(SPLIT_PART(COALESCE({column}, ''), ',', 1))"

    @staticmethod
    def _available_settings_years(roster_by_year: dict | None) -> list[int]:
        """Return sorted integer years present in roster/scoring settings."""
        if not roster_by_year:
            return []
        return sorted(
            int(year) for year in roster_by_year.keys() if str(year).isdigit() or isinstance(year, int | float)
        )

    def _resolve_settings_year(self, year: int, roster_by_year: dict | None = None) -> tuple[int | None, dict]:
        """Resolve a year to the applicable settings year using first/last-year clamps.

        Rules:
        - exact year if present
        - before the first league year -> first league year
        - after the last league year -> last league year
        - interior missing year -> nearest available year
        """
        if roster_by_year is None:
            roster_by_year = self.roster_by_year or {}

        year_int = int(year)
        if year_int in roster_by_year:
            return year_int, roster_by_year[year_int]
        if str(year_int) in roster_by_year:
            return year_int, roster_by_year[str(year_int)]

        available_years = self._available_settings_years(roster_by_year)
        if not available_years:
            return None, {}

        if year_int < available_years[0]:
            resolved_year = available_years[0]
        elif year_int > available_years[-1]:
            resolved_year = available_years[-1]
        else:
            resolved_year = min(available_years, key=lambda existing_year: abs(existing_year - year_int))

        return resolved_year, roster_by_year.get(resolved_year) or roster_by_year.get(str(resolved_year)) or {}

    def _resolve_scoring_year(self, year: int, roster_by_year: dict | None = None) -> tuple[int | None, dict]:
        """Like _resolve_settings_year but skips years whose scoring_settings is missing rec/pass_td."""
        if roster_by_year is None:
            roster_by_year = self.roster_by_year or {}

        year_int = int(year)
        available_years = self._available_settings_years(roster_by_year)
        if not available_years:
            return None, {}

        def _scoring_populated(yr: int) -> bool:
            settings = roster_by_year.get(yr) or roster_by_year.get(str(yr)) or {}
            if not isinstance(settings, dict):
                return False
            scoring = settings.get("scoring_settings") or {}
            return "rec" in scoring and "pass_td" in scoring

        candidates = sorted(available_years, key=lambda y: (abs(y - year_int), y))
        for candidate in candidates:
            if _scoring_populated(candidate):
                year_settings = roster_by_year.get(candidate) or roster_by_year.get(str(candidate)) or {}
                return candidate, year_settings

        # No year has populated scoring — defer to roster resolver so kick_col / multipliers still come from a real year.
        return self._resolve_settings_year(year_int, roster_by_year)

    def _position_eligibility_sql(self, column: str, position: str) -> str:
        """Return SQL predicate for whether a row is eligible for a dedicated slot."""
        pos_upper = position.upper()
        token_list_sql = f"string_split(UPPER(COALESCE({column}, '')), ',')"

        if pos_upper in self.IDP_POSITION_FAMILIES:
            family = sorted({p.upper() for p in self.IDP_POSITION_FAMILIES[pos_upper]} | {pos_upper})
            family_sql = ", ".join(repr(p) for p in family)
            return f"len(list_intersect({token_list_sql}, [{family_sql}])) > 0"

        if pos_upper == "DEF":
            return f"len(list_intersect({token_list_sql}, ['DEF', 'DST', 'D/ST'])) > 0"

        return f"list_contains({token_list_sql}, '{pos_upper}')"

    def _front7_eligibility_sql(self, column: str) -> str:
        """Return SQL predicate matching any front-7 player (DL or LB family)."""
        from multi_league.core.roster_slots import FRONT_7

        family = sorted({p.upper() for p in FRONT_7})
        family_sql = ", ".join(repr(p) for p in family)
        token_list_sql = f"string_split(UPPER(COALESCE({column}, '')), ',')"
        return f"len(list_intersect({token_list_sql}, [{family_sql}])) > 0"

    def _flex_eligibility_sql(self, column: str, eligible_positions: list[str]) -> str:
        """Return SQL predicate for flex-eligible rows using normalized primary position."""
        normalized = sorted({pos.upper() for pos in eligible_positions})
        eligible_sql = ", ".join(repr(pos) for pos in normalized)
        token_list_sql = f"string_split(UPPER(COALESCE({column}, '')), ',')"
        return f"len(list_intersect({token_list_sql}, [{eligible_sql}])) > 0"

    @staticmethod
    def _preferred_flex_rank_column(eligible_positions: list[str]) -> str | None:
        """Return the generic flex-rank column when an exact precomputed one exists."""
        eligible = {pos.upper() for pos in eligible_positions}
        if eligible == {"RB", "WR", "TE"}:
            return "flex_week_rank"
        if eligible == {"QB", "RB", "WR", "TE"}:
            return "sflex_week_rank"
        return None

    def _qualified_name(self, table_name: str) -> str:
        """Get fully-qualified table name with database and schema prefix.

        Remote and attached DuckDB catalogs require qualified names to avoid
        cross-database confusion when multiple databases exist. Returns format:
            {db_name}.{schema}.{table_name}  (e.g., 'my_league.public.matchup')
        """
        if self.data_dir:
            # Local DuckDB files expose tables directly under the schema
            # (typically public.table_name), not db_name.public.table_name.
            if self.schema:
                return f"{self.schema}.{table_name}"
            return table_name

        schema = self.schema or "public"
        return f"___leagues.{schema}.{table_name}"

    def _db_filter(self, alias: str = "") -> str:
        """SQL fragment for db_name scoping. No-op in local mode."""
        if self.data_dir:
            return "1=1"

        validate_db_name(self.db_name)
        prefix = f"{alias}." if alias else ""
        return f"{prefix}db_name = '{self.db_name}'"

    def _get_connection(self):
        """Get or create a DuckDB connection — local-first when data_dir provided.

        When a connection was injected via __init__(conn=...), reuses it and
        ATTACHes ___ops once on first access.  Otherwise falls back to
        get_pipeline_connection() for standalone / CLI usage.
        """
        if self._conn is None:
            from multi_league.core.db_utils import get_pipeline_connection

            self._conn = get_pipeline_connection(self.db_name, self.data_dir, attach_ops=True)
            self._owns_conn = True
            self._ops_attached = True  # get_pipeline_connection handles it
        elif not self._ops_attached:
            self._attach_ops(self._conn)
            self._ops_attached = True
        return self._conn

    def _attach_ops(self, conn):
        """ATTACH ___ops (read-only) on an injected connection. Called once."""
        import os
        from pathlib import Path

        ops_cache = os.environ.get("OPS_CACHE_PATH", "")
        if ops_cache and Path(ops_cache).exists():
            try:
                conn.execute(f"ATTACH '{ops_cache}' AS \"___ops\" (READ_ONLY)")
                logger.info(f"[___ops] Attached local cache: {ops_cache}")
                return
            except Exception as e:
                if "already attached" in str(e).lower():
                    return
                logger.warning(f"Could not attach local ops cache: {e}")

        logger.warning(
            "[___ops] No OPS_CACHE_PATH set — ___ops tables will not be "
            "available for SQL JOINs. Set OPS_CACHE_PATH to a local "
            "___ops .duckdb file, or run build_ops_cache.py first."
        )

    def _execute(self, sql: str, description: str) -> int:
        """Execute SQL and return rows affected.

        Args:
            sql: SQL statement to execute
            description: Human-readable description for logging

        Returns:
            Number of rows affected (or -1 if dry run/unknown)
        """
        logger.info(f"[SQL] {description}")

        if self.dry_run:
            logger.info(f"[DRY RUN] Would execute:\n{sql[:500]}...")
            return -1

        conn = self._get_connection()
        try:
            if not self.data_dir:
                validate_scoped_sql(sql, self.db_name)
            result = conn.execute(sql)  # noqa: F841
            logger.info(f"[SQL] {description} - completed")
            return -1  # DuckDB doesn't return rowcount for UPDATE
        except Exception as e:
            logger.error(f"[SQL ERROR] {description}: {e}")
            raise

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        League tables use the 'public' schema by default.
        IMPORTANT: Always use explicit schema to avoid cross-database confusion.
        """
        if table_name in self._table_cache:
            return self._table_cache[table_name]

        conn = self._get_connection()

        # Use the configured schema (default: public) - always explicit
        full_name = self._qualified_name(table_name)

        try:
            conn.execute(f"SELECT 1 FROM {full_name} LIMIT 1").fetchone()
            self._table_cache[table_name] = True
            logger.info(f"[TABLE CHECK] {full_name} exists")
            return True
        except Exception as e:
            self._table_cache[table_name] = False
            logger.warning(f"[TABLE CHECK] {full_name} not found: {e}")
            return False

    def _get_table_columns(self, table_name: str) -> set[str]:
        """Get all column names for a table.

        League tables use the 'public' schema by default.
        IMPORTANT: Always use explicit schema to avoid cross-database confusion.
        """
        if table_name in self._column_cache:
            return self._column_cache[table_name]

        conn = self._get_connection()

        # Use the configured schema (default: public) - always explicit to avoid
        # cross-database confusion where unqualified names can match
        # tables from other databases
        full_name = self._qualified_name(table_name)

        try:
            result = conn.execute(f"DESCRIBE {full_name}").fetchall()
            columns = {row[0] for row in result}
            self._column_cache[table_name] = columns
            logger.info(f"[DESCRIBE] {full_name}: {len(columns)} columns")
            return columns
        except Exception as e:
            logger.warning(f"[DESCRIBE] Could not describe {full_name}: {e}")
            self._column_cache[table_name] = set()
            return set()

    def _ensure_columns(self, table: str, columns: dict[str, str], existing_cols: set[str] | None = None) -> set[str]:
        """Verify expected columns exist on a table (canonical DDL pre-defines them).

        Args:
            table: Fully qualified table name
            columns: Dict of {column_name: column_type} to verify
            existing_cols: Pre-fetched column set (avoids re-querying)

        Returns:
            Set of column names present on the table
        """
        if existing_cols is None:
            existing_cols = self._get_table_columns(table.split(".")[-1])

        missing = [c for c in columns if c not in existing_cols]
        if missing:
            table_name = table.split(".")[-1].strip('"')
            message = (
                f"[_ensure_columns] {table_name} missing {len(missing)} expected columns "
                f"(should be in canonical DDL): {', '.join(missing)}"
            )
            if table_name in _STRICT_CANONICAL_TABLES:
                raise RuntimeError(message)
            logger.warning(message)
        return existing_cols

    def _invalidate_column_cache(self, table_name: str) -> None:
        """Invalidate cached columns for a table."""
        self._column_cache.pop(table_name, None)

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        columns = self._get_table_columns(table_name)
        return column_name in columns

    def _detect_platform(self) -> str:
        """Detect platform by scanning columns across ALL tables.

        Delegates to the shared detect_platform() utility in db_utils.
        Result is cached for the lifetime of this instance.
        """
        if hasattr(self, "_platform"):
            return self._platform

        from multi_league.core.db_utils import detect_platform  # noqa: PLC0415

        self._platform = detect_platform(self._get_connection(), self.db_name, self.schema)
        logger.info(f"[platform] Detected {self._platform}")
        return self._platform

    def resolve_all_nfl_player_ids(self) -> int:
        """Resolve platform player IDs → NFL_player_id via player_bio for all tables.

        Delegates to the shared resolve_nfl_ids() utility in db_utils.
        """
        from multi_league.core.db_utils import resolve_nfl_ids  # noqa: PLC0415

        conn = self._get_connection()
        platform = self._detect_platform()
        total = 0

        for table_name in ["player_fantasy", "draft", "transactions"]:
            if not self._table_exists(table_name):
                continue
            resolved = resolve_nfl_ids(conn, self.db_name, table_name, self.schema, platform)
            logger.info(f"[resolve_nfl_ids] {table_name}: resolved via {platform}")
            total += resolved

        return total

    def _best_transaction_join_key(self, player_cols: set[str], trans_cols: set[str], trans_table: str) -> str | None:
        """Pick the best join key considering coverage in BOTH transactions and player_fantasy.

        Scores each candidate by: transaction_coverage * player_fantasy_coverage.
        This prevents picking a key that's 100% in transactions but only 10% in
        player_fantasy (e.g., yahoo_player_id in ESPN leagues stores ESPN IDs but
        player_fantasy only has them for rostered rows, missing expanded NFL data).
        """
        # Check cache first (result never changes within a run)
        cache_key = f"_txn_join_key_{trans_table}"
        if hasattr(self, "_txn_join_key_cache") and cache_key in self._txn_join_key_cache:
            cached = self._txn_join_key_cache[cache_key]
            logger.info(f"[_best_transaction_join_key] Using cached key: {cached}")
            return cached

        candidates = [
            "NFL_player_id",
            "sleeper_player_id_original",
            "yahoo_player_id",
        ]
        conn = self._get_connection()
        player_table = self._qualified_name("player_fantasy")

        # Filter to candidates that exist in both tables
        valid_candidates = [col for col in candidates if col in player_cols and col in trans_cols]
        if not valid_candidates:
            if not hasattr(self, "_txn_join_key_cache"):
                self._txn_join_key_cache = {}
            self._txn_join_key_cache[cache_key] = None
            return None

        # Batch all COUNT queries into a single UNION ALL query
        parts = []
        for col in valid_candidates:
            parts.append(f"""
                SELECT '{col}' as col_name,
                       (SELECT COUNT(*) FROM {trans_table} WHERE {col} IS NOT NULL) as trans_ct,
                       (SELECT COUNT(*) FROM {player_table} WHERE {col} IS NOT NULL) as pf_ct,
                       (SELECT COUNT(*) FROM {player_table}) as pf_total
            """)

        best_key = None
        best_score = 0
        try:
            rows = conn.execute(" UNION ALL ".join(parts)).fetchall()
            for col_name, trans_ct, pf_ct, pf_total in rows:
                pf_pct = pf_ct / max(pf_total, 1)
                score = trans_ct * pf_pct
                logger.info(
                    f"[_best_transaction_join_key] {col_name}: "
                    f"{trans_ct} trans rows, "
                    f"{pf_ct}/{pf_total} pf rows ({pf_pct:.1%}), "
                    f"score={score:.0f}"
                )
                if score > best_score:
                    best_score = score
                    best_key = col_name
        except Exception as e:
            logger.warning(f"[_best_transaction_join_key] Batch query failed: {e}")

        # Cache the result
        if not hasattr(self, "_txn_join_key_cache"):
            self._txn_join_key_cache = {}
        self._txn_join_key_cache[cache_key] = best_key
        return best_key

    def _get_super_table_columns(self) -> set[str]:
        """Get column names for the NFL super table (cached)."""
        cache_key = "_super_table_"
        if cache_key in self._column_cache:
            return self._column_cache[cache_key]
        conn = self._get_connection()
        try:
            result = conn.execute("DESCRIBE ___ops.nfl_historical.nfl_player_stats_all").fetchall()
            columns = {row[0] for row in result}
            self._column_cache[cache_key] = columns
            return columns
        except Exception as e:
            logger.warning(f"[DESCRIBE] Could not describe super_table: {e}")
            self._column_cache[cache_key] = set()
            return set()

    def _get_super_table_column_types(self) -> dict[str, str]:
        """Get column types for the NFL super table (cached)."""
        cache_key = "_super_table_types_"
        if cache_key in self._column_cache:
            return self._column_cache[cache_key]
        conn = self._get_connection()
        try:
            result = conn.execute("DESCRIBE ___ops.nfl_historical.nfl_player_stats_all").fetchall()
            column_types = {row[0]: str(row[1]).upper() for row in result}
            self._column_cache[cache_key] = column_types
            return column_types
        except Exception as e:
            logger.warning(f"[DESCRIBE] Could not describe super_table types: {e}")
            self._column_cache[cache_key] = {}
            return {}

    @staticmethod
    def _parse_def_multipliers(scoring_rules, scoring_settings=None):
        """Parse DEF (team defense) multipliers from scoring rules.

        Handles both Yahoo (scoring_rules list with stat_id) and Sleeper
        (scoring_settings dict with def_* keys) formats.

        Args:
            scoring_rules: List of Yahoo scoring rule dicts
            scoring_settings: Dict of Sleeper scoring settings (fallback)

        Returns:
            Dict mapping super_table column names to multiplier values
        """
        YAHOO_DEF_STAT_MAP = {
            "32": ("pts_def_sack", 1.0),
            "33": ("pts_def_int", 2.0),
            "34": ("pts_def_fr", 2.0),
            "35": ("pts_def_td", 6.0),
            "36": ("pts_def_safety", 2.0),
            "37": ("pts_def_block", 2.0),
            "48": ("pts_def_ret_yd", 0.0),
            "49": ("pts_def_ret_td", 0.0),
            "67": ("pts_def_4stop", 0.0),
            "68": ("pts_def_tfl", 0.0),
            "77": ("pts_def_3out", 0.0),
            # Yahoo stat 82 (XP Returned) intentionally unmapped — no precomputed
            # super_table column exists. Add a precomputed col before re-enabling.
            # Removed 2026-05-02 per L1.b Phase 0 Task 0.4 (was pts_def_xpr dead ref).
        }
        YAHOO_PA_TIER_MAP = {
            "50": ("pts_allow_0", 10.0),
            "51": ("pts_allow_1_6", 7.0),
            "52": ("pts_allow_7_13", 4.0),
            "53": ("pts_allow_14_20", 1.0),
            "54": ("pts_allow_21_27", 0.0),
            "55": ("pts_allow_28_34", -1.0),
            "56": ("pts_allow_35_plus", -4.0),
        }
        SLEEPER_DEF_MAP = {
            "def_td": ("pts_def_td", 6.0),
            "def_fum_ret_td": ("pts_def_td", 6.0),
            "def_int_ret_td": ("pts_def_td", 6.0),
            "def_st_td": ("pts_def_ret_td", 6.0),
            "def_kr_td": ("pts_def_ret_td", 6.0),
            "def_pr_td": ("pts_def_ret_td", 6.0),
            "def_blk_kick_td": ("pts_def_ret_td", 6.0),
            "sack": ("pts_def_sack", 1.0),
            "int": ("pts_def_int", 2.0),
            "fum_rec": ("pts_def_fr", 2.0),
            "ff": ("pts_def_ff", 0.0),
            "safe": ("pts_def_safety", 2.0),
            "blk_kick": ("pts_def_block", 2.0),
            "def_fg_block": ("pts_def_fg_block", 0.0),
            "fg_block": ("pts_def_fg_block", 0.0),
            "def_punt_block": ("pts_def_punt_block", 0.0),
            "punt_block": ("pts_def_punt_block", 0.0),
            "def_pat_block": ("pts_def_pat_block", 0.0),
            "pat_block": ("pts_def_pat_block", 0.0),
            "def_3_and_out": ("pts_def_3out", 0.0),
            "def_4_and_stop": ("pts_def_4stop", 0.0),
            "def_st_fum_rec": ("pts_def_fr", 2.0),
            "def_kr_yd": ("dst_return_yards", 0.0),
            "def_pr_yd": ("dst_return_yards", 0.0),
            "def_pass_def": ("def_pass_defended", 0.0),
            "def_qb_hit": ("def_qb_hits", 0.0),
            # Points allowed tiers (flag * multiplier)
            "pts_allow_0": ("pts_allow_0", 10.0),
            "pts_allow_1_6": ("pts_allow_1_6", 7.0),
            "pts_allow_7_13": ("pts_allow_7_13", 4.0),
            "pts_allow_14_20": ("pts_allow_14_20", 1.0),
            "pts_allow_21_27": ("pts_allow_21_27", 0.0),
            "pts_allow_28_34": ("pts_allow_28_34", -1.0),
            "pts_allow_35p": ("pts_allow_35_plus", -4.0),
            # Yards allowed tiers (flag * multiplier)
            "yds_allow_0_100": ("yds_allow_0_99", 0.0),
            "yds_allow_100_199": ("yds_allow_100_199", 0.0),
            "yds_allow_200_299": ("yds_allow_200_299", 0.0),
            "yds_allow_300_349": ("yds_allow_300_349", 0.0),
            "yds_allow_350_399": ("yds_allow_350_399", 0.0),
            "yds_allow_300_399": ("yds_allow_300_399", 0.0),  # coarse -> expanded to fine below
            "yds_allow_400_449": ("yds_allow_400_449", 0.0),
            "yds_allow_450_499": ("yds_allow_450_499", 0.0),
            "yds_allow_400_499": ("yds_allow_400_499", 0.0),  # coarse -> expanded to fine below
            "yds_allow_500_549": ("yds_allow_500_549", 0.0),
            "yds_allow_550p": ("yds_allow_550_plus", 0.0),
            "yds_allow_500_plus": ("yds_allow_500_plus", 0.0),  # coarse -> expanded to fine below
        }

        def_multipliers = {}
        had_explicit_ya_settings = False
        if scoring_rules:
            has_any_pos_types = any(rule.get("position_types", []) for rule in scoring_rules)
            for rule in scoring_rules:
                stat_id = str(rule.get("stat_id", ""))
                pos_types = rule.get("position_types", [])
                if has_any_pos_types and "DT" not in pos_types and stat_id in ("48", "49", "82"):
                    continue
                if stat_id in YAHOO_DEF_STAT_MAP:
                    col, default = YAHOO_DEF_STAT_MAP[stat_id]
                    def_multipliers[col] = float(rule.get("points", default))
                if stat_id in YAHOO_PA_TIER_MAP:
                    col, default = YAHOO_PA_TIER_MAP[stat_id]
                    def_multipliers[col] = float(rule.get("points", default))

            YAHOO_DEF_STAT_IDS = {
                "32",
                "33",
                "34",
                "35",
                "36",
                "37",
                "48",
                "49",
                "50",
                "51",
                "52",
                "53",
                "54",
                "55",
                "56",
                "57",
                "67",
                "74",
                "75",
                "76",
                "77",
                "82",
            }
            for rule in scoring_rules:
                name = str(rule.get("name", "")).lower()
                pos_types = rule.get("position_types", [])
                stat_id = str(rule.get("stat_id", ""))
                is_def_rule = "DT" in pos_types or stat_id in YAHOO_DEF_STAT_IDS
                if not is_def_rule:
                    continue
                if name in ("ff", "fumble forced", "fumbles forced") and "pts_def_ff" not in def_multipliers:
                    def_multipliers["pts_def_ff"] = float(rule.get("points", 1.0))
                if ("tfl" in name or "tackles for loss" in name) and "pts_def_tfl" not in def_multipliers:
                    def_multipliers["pts_def_tfl"] = float(rule.get("points", 0.0))
                if ("3" in name and "out" in name) and "pts_def_3out" not in def_multipliers:
                    def_multipliers["pts_def_3out"] = float(rule.get("points", 0.0))
                if "yds allow" in name or "yards allowed" in name:
                    ya_pts = float(rule.get("points", 0.0))
                    if "neg" in name:
                        def_multipliers["yds_allow_neg"] = ya_pts
                    # Check specific low ranges BEFORE generic digit checks
                    # to avoid "0-100" matching the '100' branch
                    elif "0-99" in name or "0_99" in name or "0-100" in name or "0_100" in name:
                        def_multipliers["yds_allow_0_99"] = ya_pts
                    elif "100-199" in name or "100_199" in name:
                        def_multipliers["yds_allow_100_199"] = ya_pts
                    elif "200-299" in name or "200_299" in name:
                        def_multipliers["yds_allow_200_299"] = ya_pts
                    # Fine brackets (check specific ranges before coarse)
                    elif "300-349" in name or "300_349" in name:
                        def_multipliers["yds_allow_300_349"] = ya_pts
                    elif "350-399" in name or "350_399" in name:
                        def_multipliers["yds_allow_350_399"] = ya_pts
                    elif "300-399" in name or "300_399" in name or "300" in name:
                        # Coarse bracket - expand to fine brackets
                        def_multipliers["yds_allow_300_349"] = ya_pts
                        def_multipliers["yds_allow_350_399"] = ya_pts
                    elif "400-449" in name or "400_449" in name:
                        def_multipliers["yds_allow_400_449"] = ya_pts
                    elif "450-499" in name or "450_499" in name:
                        def_multipliers["yds_allow_450_499"] = ya_pts
                    elif "400-499" in name or "400_499" in name or "400" in name:
                        # Coarse bracket - expand to fine brackets
                        def_multipliers["yds_allow_400_449"] = ya_pts
                        def_multipliers["yds_allow_450_499"] = ya_pts
                    elif "500-549" in name or "500_549" in name:
                        def_multipliers["yds_allow_500_549"] = ya_pts
                    elif "550" in name:
                        def_multipliers["yds_allow_550_plus"] = ya_pts
                    elif "500" in name:
                        # Coarse "500+" bracket - expand to fine brackets
                        def_multipliers["yds_allow_500_549"] = ya_pts
                        def_multipliers["yds_allow_550_plus"] = ya_pts
                # DST return yards (per-yard) — use pts_def_ret_yd (canonical precalc column)
                # Skip if stat_id 48 already set pts_def_ret_yd via YAHOO_DEF_STAT_MAP
                if ("ret yd" in name or "return yard" in name or "ret_yd" in name) and is_def_rule:
                    ret_pts = float(rule.get("points", 0.0))
                    if (
                        ret_pts != 0
                        and "pts_def_ret_yd" not in def_multipliers
                        and "dst_return_yards" not in def_multipliers
                    ):
                        def_multipliers["pts_def_ret_yd"] = ret_pts

        if scoring_settings:
            for sleeper_key, (col, default) in SLEEPER_DEF_MAP.items():  # noqa: B007
                if sleeper_key not in scoring_settings:
                    continue
                val = float(scoring_settings[sleeper_key])
                if val == 0:
                    continue
                if sleeper_key.startswith("yds_allow_"):
                    had_explicit_ya_settings = True
                if col not in def_multipliers:
                    def_multipliers[col] = val

        # Validate YA tiers are truly enabled: Yahoo API returns YA stat_ids with
        # default values even when YA scoring is disabled. When truly enabled, at least
        # one low-yardage tier (0-99 or 100-199) has POSITIVE points (rewarding good
        # defense). When disabled/default, all tiers are <= 0 (penalties only).
        ya_keys = [k for k in def_multipliers if k.startswith("yds_allow_")]
        if ya_keys and not had_explicit_ya_settings:
            ya_low = def_multipliers.get("yds_allow_0_99", 0)
            ya_low2 = def_multipliers.get("yds_allow_100_199", 0)
            if ya_low <= 0 and ya_low2 <= 0:
                # Not truly enabled — remove all YA tier entries
                for k in ya_keys:
                    del def_multipliers[k]

        # Expand any remaining coarse YA brackets to fine brackets
        COARSE_TO_FINE = {
            "yds_allow_300_399": ["yds_allow_300_349", "yds_allow_350_399"],
            "yds_allow_400_499": ["yds_allow_400_449", "yds_allow_450_499"],
            "yds_allow_500_plus": ["yds_allow_500_549", "yds_allow_550_plus"],
        }
        for coarse, fines in COARSE_TO_FINE.items():
            if coarse in def_multipliers:
                val = def_multipliers.pop(coarse)
                for fine in fines:
                    if fine not in def_multipliers:
                        def_multipliers[fine] = val

        return def_multipliers

    # =========================================================================
    # LOAD SETTINGS FROM DATABASE
    # =========================================================================

    def load_settings_from_db(self) -> tuple:
        """Load roster and scoring settings from the flat league_settings table.

        Reads canonical flat columns (roster_*, scoring_*) directly —
        no JSON parsing needed.

        Returns:
            Tuple of (roster_by_year, scoring_params) where:
            - roster_by_year: Dict mapping year (int) to roster counts dict
              Example: {2024: {"QB": 1, "RB": 2, "FLEX": 1, ...}, 2023: {...}}
            - scoring_params: Dict with 'ppr', 'pass_td_pts', 'idp_scoring'
        """
        if not self._table_exists("league_settings"):
            logger.warning("[SETTINGS] league_settings table not found")
            return {}, {}

        conn = self._get_connection()
        settings_table = self._qualified_name("league_settings")

        try:
            result = conn.execute(f"SELECT * FROM {settings_table} ORDER BY year").fetchdf()
        except Exception as e:
            logger.error(f"[SETTINGS] Failed to query league_settings: {e}")
            return {}, {}

        if result.empty:
            logger.warning("[SETTINGS] No settings found in league_settings table")
            return {}, {}

        import math

        def _safe_float(val, default=0.0):
            """Convert to float, treating None/NaN as default."""
            if val is None:
                return default
            try:
                f = float(val)
                return default if math.isnan(f) else f
            except (ValueError, TypeError):
                return default

        roster_by_year = {}
        latest_ppr = 0.0
        latest_pass_td = 4
        # DEF_COL_MAP, IDP_COL_MAP, BONUS_COL_MAP, KICK_COL_MAP are now
        # module-level constants (hoisted for audit_scoring_keys.py coverage checks).

        for _, row in result.iterrows():
            try:
                year = int(row["year"])
            except (ValueError, TypeError):
                continue

            # Pre-flight key assertion before building multipliers (spec §4.5(a)).
            # Use pd.notna (not `is not None`) because DuckDB NULLs in a DOUBLE
            # column land in the pandas row as np.nan, and np.nan is not None is
            # True. An uncaught NaN propagates into _build_def_multipliers, then
            # into def_mults[col] = float(nan), then into populate_fantasy_points'
            # f-string at build time as the literal token "nan" inside
            # `COALESCE(s.pts_def_sack, 0) * nan` — which DuckDB parses as an
            # identifier reference and rejects with "column nan not found".
            scoring_dict_for_year = extract_scoring_settings_from_flat_row(row.to_dict())
            _assert_known_scoring_keys(scoring_dict_for_year, league=str(self.db_name), year=year)

            # Extract roster counts from flat roster_* columns
            roster_counts = {}
            for col in result.columns:
                if col.startswith("roster_") and row[col] is not None:
                    try:
                        count = int(row[col])
                    except (ValueError, TypeError):
                        continue
                    if count > 0:
                        pos = col.replace("roster_", "")
                        roster_counts[pos] = count

            year_bucket = roster_by_year.setdefault(year, {})
            if roster_counts:
                year_bucket.update(roster_counts)

            # Extract scoring from flat scoring_* columns
            year_ppr = row.get("scoring_rec")
            year_pass_td = row.get("scoring_pass_td")

            ppr_val = _safe_float(year_ppr, latest_ppr)
            td_val = _safe_float(year_pass_td, latest_pass_td)
            if ppr_val != latest_ppr:
                latest_ppr = ppr_val
            if td_val != latest_pass_td:
                latest_pass_td = td_val

            # Preserve the full per-year scoring dict so downstream scoring
            # logic can recover custom offensive deltas (e.g. pass_cmp,
            # pass_yd, rush_att) instead of collapsing every year to just
            # rec/pass_td and silently under-scoring custom leagues.
            year_bucket["scoring_settings"] = dict(scoring_dict_for_year)
            year_bucket["scoring_settings"]["rec"] = ppr_val
            year_bucket["scoring_settings"]["pass_td"] = td_val

            def_mults = _build_def_multipliers(scoring_dict_for_year)
            # Yahoo exposes coarse YA tiers (300-399, 400-499, 500+). Our DDL
            # stores split buckets, so expand any populated coarse-edge value
            # across its companion bucket when that companion is absent.
            if "yds_allow_300_349" in def_mults and "yds_allow_350_399" not in def_mults:
                def_mults["yds_allow_350_399"] = def_mults["yds_allow_300_349"]
            if "yds_allow_400_449" in def_mults and "yds_allow_450_499" not in def_mults:
                def_mults["yds_allow_450_499"] = def_mults["yds_allow_400_449"]
            if "yds_allow_500_549" in def_mults and "yds_allow_550_plus" not in def_mults:
                def_mults["yds_allow_550_plus"] = def_mults["yds_allow_500_549"]
            if def_mults:
                year_bucket["def_multipliers"] = def_mults

            year_idp_mults = {}
            for flat_col, super_col in IDP_COL_MAP.items():
                val = row.get(flat_col)
                if _safe_float(val) != 0:
                    year_idp_mults[super_col] = _safe_float(val)
            if year_idp_mults:
                year_bucket["idp_multipliers"] = year_idp_mults

            if year_idp_mults:
                year_tkl = year_idp_mults.get("pts_idp_tackle_solo", 0)
                year_sack = year_idp_mults.get("pts_idp_sack", 0)
                if year_tkl >= 1.5:
                    year_bucket["idp_scoring"] = "tackle_heavy"
                elif year_sack >= 4:
                    year_bucket["idp_scoring"] = "big_play"
                elif year_tkl >= 1.0:
                    year_bucket["idp_scoring"] = "premium"
                else:
                    year_bucket["idp_scoring"] = "std"

            year_kick_col = "pts_k_std"
            if _safe_float(row.get("scoring_fgm_yds")) != 0:
                year_kick_col = "pts_k_yds"
            year_bucket["kick_col"] = year_kick_col

            year_bonus_mults = {}
            for flat_col, super_col in BONUS_COL_MAP.items():
                val = row.get(flat_col)
                if _safe_float(val) != 0:
                    year_bonus_mults[super_col] = _safe_float(val)
            if year_bonus_mults:
                year_bucket["bonus_multipliers"] = year_bonus_mults

            # Kicker multipliers — build custom formula from per-distance settings
            year_kick_mults = {}
            for flat_col, super_col in KICK_COL_MAP.items():
                val = row.get(flat_col)
                if _safe_float(val) != 0:
                    # For coarse brackets, expand to fine brackets
                    if flat_col == "scoring_fgm_0_39":
                        # 0-39 coarse → set 0-19, 20-29, 30-39 if not already set
                        for fine in ("fg_made_0_19", "fg_made_20_29", "fg_made_30_39"):
                            if fine not in year_kick_mults:
                                year_kick_mults[fine] = _safe_float(val)
                    elif flat_col == "scoring_fgm_50p":
                        # 50+ coarse → set 50-59, 60+ if not already set
                        for fine in ("fg_made_50_59", "fg_made_60_"):
                            if fine not in year_kick_mults:
                                year_kick_mults[fine] = _safe_float(val)
                    elif flat_col == "scoring_fgmiss_0_39":
                        for fine in ("fg_missed_0_19", "fg_missed_20_29", "fg_missed_30_39"):
                            if fine not in year_kick_mults:
                                year_kick_mults[fine] = _safe_float(val)
                    else:
                        if super_col not in year_kick_mults:
                            year_kick_mults[super_col] = _safe_float(val)
            if year_kick_mults:
                year_bucket["kick_multipliers"] = year_kick_mults

            year_te_premium = _safe_float(row.get("scoring_bonus_rec_te"))
            if year_te_premium != 0:
                year_bucket["te_premium"] = year_te_premium

        # Build scoring_params from latest year
        scoring_params = {
            "ppr": latest_ppr,
            "pass_td_pts": latest_pass_td,
            "ret_yds": 0,
            "idp_scoring": "std",
            "idp_multipliers": {},
            "def_multipliers": {},
            "kick_col": "pts_k_std",
            "kick_multipliers": {},
            "bonus_multipliers": {},
            "te_premium": 0.0,
        }

        # Extract IDP multipliers from flat scoring_idp_* columns (latest year)
        last_row = result.iloc[-1]
        idp_multipliers = {}
        for flat_col, super_col in IDP_COL_MAP.items():
            val = last_row.get(flat_col)
            if _safe_float(val) != 0:
                idp_multipliers[super_col] = _safe_float(val)
        scoring_params["idp_multipliers"] = idp_multipliers

        # Detect IDP scoring variant
        tkl = idp_multipliers.get("pts_idp_tackle_solo", 0)
        sack = idp_multipliers.get("pts_idp_sack", 0)
        if tkl >= 1.5:
            scoring_params["idp_scoring"] = "tackle_heavy"
        elif sack >= 4:
            scoring_params["idp_scoring"] = "big_play"
        elif tkl >= 1.0:
            scoring_params["idp_scoring"] = "premium"

        # DEF multipliers from latest year
        last_def = {}
        for flat_col, super_col in DEF_COL_MAP.items():
            val = last_row.get(flat_col)
            if _safe_float(val) != 0:
                last_def[super_col] = _safe_float(val)
        scoring_params["def_multipliers"] = last_def

        # Kicker variant: if scoring_fgm_yds is set, use yardage-based kicker
        fgm_yds = last_row.get("scoring_fgm_yds")
        if _safe_float(fgm_yds) != 0:
            scoring_params["kick_col"] = "pts_k_yds"

        # Bonus multipliers from flat scoring_bonus_* columns
        bonus_multipliers = {}
        for flat_col, super_col in BONUS_COL_MAP.items():
            val = last_row.get(flat_col)
            if _safe_float(val) != 0:
                bonus_multipliers[super_col] = _safe_float(val)
        scoring_params["bonus_multipliers"] = bonus_multipliers

        # TE premium
        te_val = last_row.get("scoring_bonus_rec_te")
        if _safe_float(te_val) != 0:
            scoring_params["te_premium"] = _safe_float(te_val)

        logger.info(f"[SETTINGS] Loaded from flat settings: {len(roster_by_year)} years")
        logger.info(
            f"[SETTINGS] Scoring: {scoring_params['ppr']} PPR, {scoring_params['pass_td_pts']}pt TD, {scoring_params['idp_scoring']} IDP"
        )
        if scoring_params["kick_col"] != "pts_k_std":
            logger.info(f"[SETTINGS] Kicker column: {scoring_params['kick_col']}")

        # Store on self so enrichments have access without double-loading
        self.roster_by_year = roster_by_year
        self._update_scoring_params(scoring_params)

        return roster_by_year, scoring_params

    def _update_scoring_params(self, scoring_params: dict[str, Any]) -> None:
        """Update POSITION_RANK_COLS based on new scoring params."""
        ppr = scoring_params.get("ppr", 0.0)
        pass_td_pts = scoring_params.get("pass_td_pts", 4)
        idp_scoring = scoring_params.get("idp_scoring", "std")

        self.ppr = ppr
        self.pass_td_pts = pass_td_pts
        self.idp_scoring = idp_scoring
        self.ret_yds = scoring_params.get("ret_yds", 0)
        self.idp_multipliers = scoring_params.get("idp_multipliers", {})
        self.def_multipliers = scoring_params.get("def_multipliers", {})
        self.kick_col = scoring_params.get("kick_col", "pts_k_std")
        self.kick_multipliers = scoring_params.get("kick_multipliers", {})
        self.bonus_multipliers = scoring_params.get("bonus_multipliers", {})
        self.te_premium = scoring_params.get("te_premium", 0.0)
        self.canonical_scoring = scoring_params.get("canonical_scoring", {})
        self._scoring_params = scoring_params

        ppr_key = {0: "0ppr", 0.5: "half", 1.0: "ppr"}.get(ppr, "half")
        td_key = f"{_normalize_td_pts(pass_td_pts)}pt"

        self.POSITION_RANK_COLS = {
            # Offense
            "QB": f"rank_qb_{td_key}",
            "RB": f"rank_rb_{ppr_key}",
            "WR": f"rank_wr_{ppr_key}",
            "TE": f"rank_te_{ppr_key}",
            "K": "rank_k",
            "DEF": "rank_def",
            # IDP - broad categories
            "LB": f"rank_lb_{idp_scoring}",
            "DL": f"rank_dl_{idp_scoring}",
            "DB": f"rank_db_{idp_scoring}",
            # IDP - specific positions (map to parent rank column)
            "ILB": f"rank_lb_{idp_scoring}",
            "OLB": f"rank_lb_{idp_scoring}",
            "MLB": f"rank_lb_{idp_scoring}",
            "DE": f"rank_dl_{idp_scoring}",
            "DT": f"rank_dl_{idp_scoring}",
            "EDGE": f"rank_dl_{idp_scoring}",
            "NT": f"rank_dl_{idp_scoring}",
            "CB": f"rank_db_{idp_scoring}",
            "S": f"rank_db_{idp_scoring}",
            "SS": f"rank_db_{idp_scoring}",
            "FS": f"rank_db_{idp_scoring}",
        }
        logger.info(f"[SETTINGS] Updated rank columns: {self.POSITION_RANK_COLS['QB']}")

    def _get_scoring_for_year(
        self,
        year: int,
        roster_by_year: dict | None = None,
    ) -> dict:
        """Get scoring config for a specific year from roster_by_year.

        Returns dict with keys: ppr, pass_td_pts, ppr_key, td_key, fpts_col,
        rolling_total_col, and rank_cols (position -> rank column name).

        Looks up roster_by_year[year]["scoring_settings"] first, falls back
        to self.ppr / self.pass_td_pts (latest year's values).

        Args:
            year: The league year to get scoring for
            roster_by_year: Dict mapping year -> settings. If None, uses self.roster_by_year.

        Returns:
            Dict with scoring config for that year
        """
        if roster_by_year is None:
            roster_by_year = self.roster_by_year or {}

        year_int = int(year)
        resolved_year, year_settings = self._resolve_scoring_year(year_int, roster_by_year)

        scoring = year_settings.get("scoring_settings", {})
        ppr = scoring.get("rec", self.ppr)
        pass_td_pts = scoring.get("pass_td", self.pass_td_pts)
        kick_col = year_settings.get("kick_col", getattr(self, "kick_col", "pts_k_std"))
        bonus_multipliers = year_settings.get("bonus_multipliers", getattr(self, "bonus_multipliers", {}))
        te_premium = float(year_settings.get("te_premium", getattr(self, "te_premium", 0.0)))
        idp_multipliers = year_settings.get("idp_multipliers", getattr(self, "idp_multipliers", {}))
        def_multipliers = year_settings.get("def_multipliers", getattr(self, "def_multipliers", {}))
        idp_scoring = year_settings.get("idp_scoring", getattr(self, "idp_scoring", "std"))

        ppr_key = {0: "0ppr", 0.5: "half", 1.0: "ppr"}.get(ppr, "half")
        td_key = f"{_normalize_td_pts(pass_td_pts)}pt"

        # fpts column for backfill
        has_ret = getattr(self, "ret_yds", 0) > 0
        ret_suffix = "_ret" if has_ret else ""
        fpts_col = f"fpts_{td_key}_{ppr_key}{ret_suffix}"

        # Tiebreaker column for optimal lineup
        rolling_total_col = f"rolling_total_{td_key}_{ppr_key}"

        # Rank columns per position
        rank_cols = {
            # Individual positions
            "QB": f"rank_qb_{td_key}",
            "RB": f"rank_rb_{ppr_key}",
            "WR": f"rank_wr_{ppr_key}",
            "TE": f"rank_te_{ppr_key}",
            "K": "rank_k",
            "DEF": "rank_def",
            "LB": f"rank_lb_{idp_scoring}",
            "DL": f"rank_dl_{idp_scoring}",
            "DB": f"rank_db_{idp_scoring}",
            # Flex (RB/WR/TE) — keyed by PPR only
            "FLEX": f"rank_flex_{ppr_key}",
            "season_FLEX": f"rank_season_flex_{ppr_key}",
            "alltime_FLEX": f"rank_alltime_flex_{ppr_key}",
            # Superflex (QB/RB/WR/TE) — keyed by pass TD + PPR
            "SUPER_FLEX": f"rank_sflex_{td_key}_{ppr_key}",
            "season_SUPER_FLEX": f"rank_season_sflex_{td_key}_{ppr_key}",
            "alltime_SUPER_FLEX": f"rank_alltime_sflex_{td_key}_{ppr_key}",
        }

        return {
            "resolved_year": resolved_year if resolved_year is not None else year_int,
            "ppr": ppr,
            "pass_td_pts": pass_td_pts,
            "ppr_key": ppr_key,
            "td_key": td_key,
            "fpts_col": fpts_col,
            "rolling_total_col": rolling_total_col,
            "kick_col": kick_col,
            "bonus_multipliers": bonus_multipliers,
            "te_premium": te_premium,
            "idp_multipliers": idp_multipliers,
            "def_multipliers": def_multipliers,
            "idp_scoring": idp_scoring,
            "rank_cols": rank_cols,
        }

    def _group_years_by_scoring(
        self,
        years: list,
        roster_by_year: dict | None = None,
    ) -> list[tuple[dict, list[int]]]:
        """Group years that share identical scoring configs.

        Returns list of (scoring_info, [years]) tuples. Each scoring_info dict
        is the result of _get_scoring_for_year() for that group.

        Leagues with constant rules -> 1 group (all years).
        Leagues that changed PPR 3x -> 3 groups.

        Args:
            years: List of years to group.
            roster_by_year: Dict mapping year -> settings. If None, uses self.roster_by_year.

        Returns:
            Sorted list of (scoring_info, sorted_year_list) tuples.
        """
        from collections import defaultdict

        if roster_by_year is None:
            roster_by_year = self.roster_by_year or {}

        groups: dict[tuple, list[int]] = defaultdict(list)
        scoring_cache: dict[tuple, dict] = {}

        for year in years:
            year_int = int(year)
            scoring = self._get_scoring_for_year(year_int, roster_by_year)

            # Build hashable key from scoring-dependent values
            # rank_cols is a dict — freeze it for hashing
            rank_cols_key = tuple(sorted(scoring["rank_cols"].items()))
            key = (
                scoring["fpts_col"],
                scoring["rolling_total_col"],
                rank_cols_key,
            )
            groups[key].append(year_int)
            if key not in scoring_cache:
                scoring_cache[key] = scoring

        # Return sorted by earliest year in each group
        result = []
        for key in sorted(groups.keys(), key=lambda k: min(groups[k])):
            result.append((scoring_cache[key], sorted(groups[key])))

        return result

    # =========================================================================
    # DETECT LEAGUE FORMAT (redraft / keeper / dynasty)
    # =========================================================================

    def detect_league_format(self) -> int:
        """Detect dynasty status and set is_dynasty on league_settings.

        Reads flat columns directly — no JSON parsing.

        Detection:
        - Sleeper: draft_type = 'dynasty' → True
        - All platforms: sleeper_taxi_slots > 0 → True
        - Draft data: keeper picks ≥ 50% of total → True
        - Keeper status (max_keepers > 0) is derivable from the flat column,
          no need to store separately.
        """
        if not self._table_exists("league_settings"):
            logger.warning("[LEAGUE_FORMAT] league_settings table not found")
            return 0

        conn = self._get_connection()
        settings_table = self._qualified_name("league_settings")
        settings_cols = self._get_table_columns("league_settings")

        if "is_dynasty" not in settings_cols:
            logger.warning("[LEAGUE_FORMAT] is_dynasty column not in league_settings")
            return 0

        # Tier 1: Sleeper draft_type = 'dynasty' or taxi_slots > 0
        sql_direct = f"""
            UPDATE {settings_table}
            SET is_dynasty = TRUE
            WHERE is_dynasty IS NULL
              AND (
                  LOWER(COALESCE(draft_type, '')) = 'dynasty'
                  OR COALESCE(sleeper_taxi_slots, 0) > 0
              )
        """
        self._execute(sql_direct, "detect_league_format: Sleeper/taxi dynasty detection")

        # Tier 2: Draft keeper density ≥ 50% → dynasty
        if self._table_exists("draft"):
            draft_table = self._qualified_name("draft")
            draft_cols = self._get_table_columns("draft")

            keeper_conditions = []
            if "is_keeper" in draft_cols:
                keeper_conditions.append("COALESCE(TRY_CAST(is_keeper AS INT), 0) = 1")
            if "is_keeper_status" in draft_cols:
                keeper_conditions.append("COALESCE(TRY_CAST(is_keeper_status AS INT), 0) = 1")
            if "is_keeper_cost" in draft_cols:
                keeper_conditions.append("COALESCE(TRY_CAST(is_keeper_cost AS INT), 0) > 0")

            if keeper_conditions:
                keeper_expr = " OR ".join(keeper_conditions)
                sql_keeper = f"""
                    UPDATE {settings_table} ls
                    SET is_dynasty = TRUE
                    FROM (
                        SELECT year,
                            SUM(CASE WHEN {keeper_expr} THEN 1 ELSE 0 END)::DOUBLE
                            / GREATEST(COUNT(*), 1) AS keeper_pct
                        FROM {draft_table}
                        WHERE year IS NOT NULL
                        GROUP BY year
                    ) d
                    WHERE ls.year = d.year
                      AND ls.is_dynasty IS NULL
                      AND d.keeper_pct >= 0.5
                """
                self._execute(sql_keeper, "detect_league_format: keeper density ≥ 50% → dynasty")

        # Tier 3: Everything else → not dynasty
        sql_default = f"""
            UPDATE {settings_table}
            SET is_dynasty = FALSE
            WHERE is_dynasty IS NULL
        """
        self._execute(sql_default, "detect_league_format: default to non-dynasty")

        # Log result
        try:
            result = conn.execute(f"SELECT year, is_dynasty FROM {settings_table} ORDER BY year").fetchall()
            for yr, dyn in result:
                logger.info(f"[LEAGUE_FORMAT] {yr}: is_dynasty={dyn}")
        except Exception:
            pass

        return len(result) if result else 0

    def ensure_player_week(self) -> int:
        """Populate player_week join key on player_fantasy and draft tables.

        player_week = {NFL_player_id}_{year}_{week}  — the primary join key
        to the super_table.  Must run after resolve_all_nfl_player_ids.

        Regenerates whenever the stored player_week doesn't match the canonical
        form for the row's NFL_player_id+year+week. Critical for the case where
        the staging merger's _backfill_nfl_player_ids set player_week to
        ``YAHOO-{yahoo_id}_{year}_{week}`` (its fallback for rows it couldn't
        resolve at that moment) and then resolve_all_nfl_player_ids later
        succeeded via player_bio. Without regeneration the stale YAHOO- prefix
        sticks, super_table joins miss on the wrong key, fantasy_points stays
        0, and fix_zero_point_starters un-stars the player. Bo Nix in KMFFL
        2025 is the canary: NFL_player_id="00-0039732" resolved correctly but
        player_week stayed "YAHOO-40875_2025_1" so all 18 weeks scored 0.

        Some platform rows are real roster/taxi placeholders that never resolve
        to the NFL stats universe (for example Sleeper "Player Invalid" or
        players without nflverse stats). Those still need a stable identity for
        delta publish, so unresolved NULL/blank player_week rows get a deterministic
        UNMAPPED key scoped by the available platform/player/manager fields.
        """
        total = 0
        for table_name in ["player_fantasy", "draft"]:
            if not self._table_exists(table_name):
                continue
            cols = self._get_table_columns(table_name)
            if "player_week" not in cols:
                continue
            t = self._qualified_name(table_name)
            # draft has year but no week — skip (player_week is weekly, not used for draft).
            if table_name == "draft" and "week" not in cols:
                continue

            for label, sql in player_week_repair_statements(t, cols, db_filter=self._db_filter()):
                total += self._execute(sql, f"ensure_player_week {label}: {table_name}")
        return total

    def ensure_manager_week(self) -> int:
        """Ensure manager_week is populated on BOTH tables.

        Canonical format: franchise_id || '_' || year || '_' || week
        Example: QAWPKGQT_2025_1

        Must match the format set by normalizers at fetch time.
        """
        total = 0

        for table_name in ["player_fantasy", "matchup"]:
            if not self._table_exists(table_name):
                continue

            table_cols = self._get_table_columns(table_name)
            qualified = self._qualified_name(table_name)

            # Need franchise_id + year + week (canonical key components)
            identity_expr = manager_identity_sql(table_cols)
            if "year" not in table_cols or "week" not in table_cols:
                logger.warning(f"[ensure_manager_week] year or week not found in {table_name}")
                continue

            mw_expr = f"{identity_expr} || '_' || CAST(year AS VARCHAR) || '_' || CAST(week AS VARCHAR)"

            sql = f"""
                UPDATE {qualified}
                SET manager_week = {mw_expr}
                WHERE {identity_expr} IS NOT NULL
                  AND year IS NOT NULL AND week IS NOT NULL
            """

            total += self._execute(sql, f"ensure_manager_week: {table_name}")

        return total

    def backfill_and_normalize_positions(self) -> int:
        """Backfill null positions from super_table and normalize comma-separated positions.

        Dual-eligible players (e.g., Taysom Hill 'TE,QB') keep their comma-
        separated positions — this is valuable data that reflects year-by-year
        eligibility evolution (e.g., Cordarrelle Patterson WR → WR,RB).

        Backfills from super_table using player_week for week-level accuracy
        (a player's position can change mid-season). Falls back to NFL_player_id
        match if player_week join misses.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_table = self._qualified_name("player_fantasy")
        total = 0

        # Step 1: Backfill null positions from super_table via player_week (most accurate)
        sql_backfill_pw = f"""
            UPDATE {player_table} p
            SET position = s.position
            FROM (
                SELECT player_week, position
                FROM ___ops.nfl_historical.nfl_player_stats_all
                WHERE player_week IS NOT NULL AND position IS NOT NULL
            ) s
            WHERE p.player_week = s.player_week
            AND p.position IS NULL
        """
        total += self._execute(sql_backfill_pw, "backfill_positions: fill null via player_week")

        # Step 2: Fallback — backfill remaining nulls via NFL_player_id (less precise, no week granularity)
        sql_backfill_id = f"""
            UPDATE {player_table} p
            SET position = s.position
            FROM (
                SELECT DISTINCT ON (NFL_player_id) NFL_player_id, position
                FROM ___ops.nfl_historical.nfl_player_stats_all
                WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL
                ORDER BY NFL_player_id, year DESC, week DESC
            ) s
            WHERE p.NFL_player_id = s.NFL_player_id
            AND p.position IS NULL
        """
        total += self._execute(sql_backfill_id, "backfill_positions: fill null via NFL_player_id fallback")

        return total

    def ensure_lineup_position(self) -> int:
        """
        Ensure lineup_position column exists and is populated.

        Some imports may have fantasy_position but not lineup_position.
        The UI expects lineup_position for started/bench filtering.
        """
        player_table = self._qualified_name("player_fantasy")

        if not self._table_exists("player_fantasy"):
            logger.warning("[ensure_lineup_position] player_fantasy table not found")
            return 0

        conn = self._get_connection()

        # Check if lineup_position column exists and has correct type
        cols = conn.execute(f"DESCRIBE {player_table}").fetchall()
        col_info = {c[0].lower(): c[1] for c in cols}  # name -> type mapping
        col_names = list(col_info.keys())

        # lineup_position column is pre-defined as VARCHAR by canonical DDL

        # Copy from fantasy_position to lineup_position
        # Always update to ensure consistency after column type changes
        if "fantasy_position" in col_names:
            # First, set ALL rows with fantasy_position (unconditional update)
            # This handles cases where column was dropped/recreated
            sql = f"""
                UPDATE {player_table}
                SET lineup_position = CAST(fantasy_position AS VARCHAR)
                WHERE fantasy_position IS NOT NULL
            """
            if not self.dry_run:
                conn.execute(sql)
                # Force a checkpoint to ensure data is persisted
                try:
                    conn.execute("CHECKPOINT")
                except Exception:
                    pass  # CHECKPOINT may not be supported in all contexts

                # Get rows affected
                updated = conn.execute(f"""
                    SELECT COUNT(*) FROM {player_table}
                    WHERE lineup_position IS NOT NULL
                """).fetchone()[0]
                logger.info(f"[ensure_lineup_position] Populated {updated} rows with lineup_position")
                return updated

        return 0

    def _identify_flex_positions(self, roster_settings: dict[str, int]):
        """Identify flex positions and their eligible positions from roster settings."""
        flex_positions = []
        for position, count in self._normalized_roster_slot_counts(roster_settings).items():
            if is_flex(position):
                eligible = list(eligible_positions(position))
                flex_positions.append((position, eligible, count))
        return flex_positions

    def _get_dedicated_slots(self, roster_settings: dict[str, int]) -> dict[str, int]:
        """Get dedicated (non-flex) starter position slots.

        Excludes flex positions (handled separately) and non-starter slots
        like BN (bench), IR (injured reserve), TAXI (taxi squad).
        """
        return {
            pos: count
            for pos, count in self._normalized_roster_slot_counts(roster_settings).items()
            if not is_flex(pos) and pos.upper() not in self.NON_STARTER_SLOTS and count > 0
        }

    @staticmethod
    def _coerce_roster_slot_count(count: Any) -> int | None:
        """Convert roster slot counts to ints while ignoring metadata values."""
        if count is None or isinstance(count, bool):
            return None
        if isinstance(count, (int, float)):  # noqa: UP038
            return int(count)
        if isinstance(count, str):
            value = count.strip()
            if not value:
                return None
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None
        return None

    def _normalized_roster_slot_counts(self, roster_settings: dict[str, Any] | None) -> dict[str, int]:
        """Return canonical roster slot counts from mixed year settings buckets."""
        normalized: dict[str, int] = {}
        if not isinstance(roster_settings, dict):
            return normalized

        for position, raw_count in roster_settings.items():
            count = self._coerce_roster_slot_count(raw_count)
            if count is None or count <= 0:
                continue
            canonical = resolve(str(position))
            normalized[canonical] = normalized.get(canonical, 0) + count

        return normalized
