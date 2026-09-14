"""
Research LAMAR precomputation for the NFL super table.

Computes LAMAR (League-Adjusted Measure Above Replacement) for 56 research
configurations using census-derived replacement percentiles from the live league population.
roster_type adds 'tep' (TE-premium); ppr adds 'ppfd' (point-per-first-down, half-PPR + 0.5/FD).
"""

# Census-derived percentiles: (size, roster_type, ppr, pass_td) -> {position: percentile}
# Percentile = fraction of NFL player pool rostered; replacement = player at CEIL(pool*pct) per
#   (year, week, season_type, position) so old years / playoff weeks scale with their actual pool.
# Re-derived 2026-07 from the live population, 2022-2025. QB/RB/WR/TE use season-level distinct
#   rostered counts. K/DEF are intentionally normalized by league size only, using eligible league-week
#   roster depth divided by the weekly K/DEF NFL pool, so unrelated offensive settings do not add noise.
#   IDP kept from existing hardcoded values (full-pool denominator wrong for fantasy IDP).
# Thin buckets fall back to the densest (size, roster, position) sibling.
# Guardrails: pool includes 0-point players (IS NOT NULL); replacement floored at 0 (GREATEST(...,0)).
# Capped at 0.95 to prevent edge cases in deep dynasty benches.

CENSUS_PERCENTILES = {
    (10, "flx", 0.0, 4): {"QB": 0.35, "RB": 0.46, "WR": 0.34, "TE": 0.22, "K": 0.28, "DEF": 0.33},
    (10, "flx", 0.5, 4): {"QB": 0.4, "RB": 0.51, "WR": 0.4, "TE": 0.25, "K": 0.29, "DEF": 0.35},
    (10, "flx", 1.0, 4): {"QB": 0.41, "RB": 0.5, "WR": 0.41, "TE": 0.27, "K": 0.3, "DEF": 0.35},
    (10, "sflx", 0.0, 4): {"QB": 0.67, "RB": 0.57, "WR": 0.49, "TE": 0.33, "K": 0.28, "DEF": 0.38},
    (10, "sflx", 0.5, 4): {"QB": 0.68, "RB": 0.58, "WR": 0.47, "TE": 0.31, "K": 0.32, "DEF": 0.37},
    (10, "sflx", 1.0, 4): {"QB": 0.67, "RB": 0.57, "WR": 0.49, "TE": 0.33, "K": 0.28, "DEF": 0.38},
    (10, "idp", 0.0, 4): {
        "QB": 0.58,
        "RB": 0.53,
        "WR": 0.44,
        "TE": 0.28,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.29,
        "LB": 0.7,
        "DB": 0.59,
    },
    (10, "idp", 0.5, 4): {
        "QB": 0.48,
        "RB": 0.6,
        "WR": 0.48,
        "TE": 0.31,
        "K": 0.35,
        "DEF": 0.43,
        "DL": 0.29,
        "LB": 0.7,
        "DB": 0.59,
    },
    (10, "idp", 1.0, 4): {
        "QB": 0.58,
        "RB": 0.53,
        "WR": 0.44,
        "TE": 0.28,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.4,
        "LB": 0.85,
        "DB": 0.68,
    },
    (10, "tep", 0.5, 4): {"QB": 0.76, "RB": 0.64, "WR": 0.55, "TE": 0.4, "K": 0.38, "DEF": 0.38},
    (10, "tep", 1.0, 4): {"QB": 0.7, "RB": 0.65, "WR": 0.56, "TE": 0.4, "K": 0.38, "DEF": 0.38},
    (10, "flx", "ppfd", 4): {"QB": 0.41, "RB": 0.5, "WR": 0.41, "TE": 0.27, "K": 0.3, "DEF": 0.35},
    (10, "sflx", "ppfd", 4): {"QB": 0.62, "RB": 0.54, "WR": 0.44, "TE": 0.29, "K": 0.26, "DEF": 0.29},
    (10, "idp", "ppfd", 4): {
        "QB": 0.58,
        "RB": 0.53,
        "WR": 0.44,
        "TE": 0.28,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.4,
        "LB": 0.85,
        "DB": 0.68,
    },
    (10, "flx", 0.0, 6): {"QB": 0.41, "RB": 0.5, "WR": 0.41, "TE": 0.27, "K": 0.3, "DEF": 0.35},
    (10, "flx", 0.5, 6): {"QB": 0.41, "RB": 0.53, "WR": 0.4, "TE": 0.25, "K": 0.29, "DEF": 0.33},
    (10, "flx", 1.0, 6): {"QB": 0.39, "RB": 0.5, "WR": 0.4, "TE": 0.26, "K": 0.29, "DEF": 0.35},
    (10, "sflx", 0.0, 6): {"QB": 0.67, "RB": 0.57, "WR": 0.49, "TE": 0.33, "K": 0.28, "DEF": 0.38},
    (10, "sflx", 0.5, 6): {"QB": 0.72, "RB": 0.64, "WR": 0.55, "TE": 0.38, "K": 0.28, "DEF": 0.38},
    (10, "sflx", 1.0, 6): {"QB": 0.7, "RB": 0.59, "WR": 0.51, "TE": 0.35, "K": 0.34, "DEF": 0.43},
    (10, "idp", 0.0, 6): {
        "QB": 0.58,
        "RB": 0.53,
        "WR": 0.44,
        "TE": 0.28,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.29,
        "LB": 0.7,
        "DB": 0.59,
    },
    (10, "idp", 0.5, 6): {
        "QB": 0.58,
        "RB": 0.53,
        "WR": 0.44,
        "TE": 0.28,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.4,
        "LB": 0.85,
        "DB": 0.68,
    },
    (10, "idp", 1.0, 6): {
        "QB": 0.6,
        "RB": 0.57,
        "WR": 0.46,
        "TE": 0.31,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.45,
        "LB": 0.95,
        "DB": 0.95,
    },
    (10, "tep", 0.5, 6): {"QB": 0.74, "RB": 0.65, "WR": 0.58, "TE": 0.42, "K": 0.38, "DEF": 0.38},
    (10, "tep", 1.0, 6): {"QB": 0.76, "RB": 0.64, "WR": 0.57, "TE": 0.41, "K": 0.38, "DEF": 0.38},
    (10, "flx", "ppfd", 6): {"QB": 0.41, "RB": 0.5, "WR": 0.41, "TE": 0.27, "K": 0.3, "DEF": 0.35},
    (10, "sflx", "ppfd", 6): {"QB": 0.67, "RB": 0.57, "WR": 0.49, "TE": 0.33, "K": 0.28, "DEF": 0.38},
    (10, "idp", "ppfd", 6): {
        "QB": 0.58,
        "RB": 0.53,
        "WR": 0.44,
        "TE": 0.28,
        "K": 0.31,
        "DEF": 0.43,
        "DL": 0.45,
        "LB": 0.95,
        "DB": 0.95,
    },
    (12, "flx", 0.0, 4): {"QB": 0.46, "RB": 0.56, "WR": 0.44, "TE": 0.29, "K": 0.36, "DEF": 0.43},
    (12, "flx", 0.5, 4): {"QB": 0.48, "RB": 0.59, "WR": 0.47, "TE": 0.31, "K": 0.35, "DEF": 0.43},
    (12, "flx", 1.0, 4): {"QB": 0.49, "RB": 0.59, "WR": 0.49, "TE": 0.33, "K": 0.37, "DEF": 0.44},
    (12, "sflx", 0.0, 4): {"QB": 0.81, "RB": 0.67, "WR": 0.58, "TE": 0.38, "K": 0.39, "DEF": 0.48},
    (12, "sflx", 0.5, 4): {"QB": 0.81, "RB": 0.67, "WR": 0.58, "TE": 0.38, "K": 0.39, "DEF": 0.48},
    (12, "sflx", 1.0, 4): {"QB": 0.81, "RB": 0.68, "WR": 0.6, "TE": 0.42, "K": 0.39, "DEF": 0.48},
    (12, "idp", 0.0, 4): {
        "QB": 0.59,
        "RB": 0.54,
        "WR": 0.41,
        "TE": 0.3,
        "K": 0.39,
        "DEF": 0.38,
        "DL": 0.55,
        "LB": 0.95,
        "DB": 0.93,
    },
    (12, "idp", 0.5, 4): {
        "QB": 0.6,
        "RB": 0.67,
        "WR": 0.56,
        "TE": 0.39,
        "K": 0.51,
        "DEF": 0.38,
        "DL": 0.55,
        "LB": 0.95,
        "DB": 0.93,
    },
    (12, "idp", 1.0, 4): {
        "QB": 0.75,
        "RB": 0.69,
        "WR": 0.6,
        "TE": 0.42,
        "K": 0.39,
        "DEF": 0.38,
        "DL": 0.42,
        "LB": 0.95,
        "DB": 0.82,
    },
    (12, "tep", 0.5, 4): {"QB": 0.85, "RB": 0.72, "WR": 0.64, "TE": 0.46, "K": 0.53, "DEF": 0.52},
    (12, "tep", 1.0, 4): {"QB": 0.85, "RB": 0.71, "WR": 0.65, "TE": 0.48, "K": 0.46, "DEF": 0.48},
    (12, "flx", "ppfd", 4): {"QB": 0.46, "RB": 0.58, "WR": 0.48, "TE": 0.31, "K": 0.34, "DEF": 0.41},
    (12, "sflx", "ppfd", 4): {"QB": 0.75, "RB": 0.62, "WR": 0.5, "TE": 0.34, "K": 0.35, "DEF": 0.41},
    (12, "idp", "ppfd", 4): {
        "QB": 0.75,
        "RB": 0.69,
        "WR": 0.6,
        "TE": 0.42,
        "K": 0.39,
        "DEF": 0.38,
        "DL": 0.42,
        "LB": 0.95,
        "DB": 0.82,
    },
    (12, "flx", 0.0, 6): {"QB": 0.42, "RB": 0.56, "WR": 0.44, "TE": 0.27, "K": 0.33, "DEF": 0.4},
    (12, "flx", 0.5, 6): {"QB": 0.5, "RB": 0.61, "WR": 0.5, "TE": 0.34, "K": 0.36, "DEF": 0.43},
    (12, "flx", 1.0, 6): {"QB": 0.49, "RB": 0.58, "WR": 0.49, "TE": 0.32, "K": 0.37, "DEF": 0.42},
    (12, "sflx", 0.0, 6): {"QB": 0.81, "RB": 0.67, "WR": 0.58, "TE": 0.38, "K": 0.39, "DEF": 0.48},
    (12, "sflx", 0.5, 6): {"QB": 0.79, "RB": 0.63, "WR": 0.55, "TE": 0.36, "K": 0.39, "DEF": 0.48},
    (12, "sflx", 1.0, 6): {"QB": 0.83, "RB": 0.71, "WR": 0.63, "TE": 0.44, "K": 0.33, "DEF": 0.54},
    (12, "idp", 0.0, 6): {
        "QB": 0.49,
        "RB": 0.62,
        "WR": 0.43,
        "TE": 0.3,
        "K": 0.38,
        "DEF": 0.38,
        "DL": 0.95,
        "LB": 0.95,
        "DB": 0.95,
    },
    (12, "idp", 0.5, 6): {
        "QB": 0.56,
        "RB": 0.61,
        "WR": 0.48,
        "TE": 0.33,
        "K": 0.36,
        "DEF": 0.38,
        "DL": 0.95,
        "LB": 0.95,
        "DB": 0.95,
    },
    (12, "idp", 1.0, 6): {
        "QB": 0.52,
        "RB": 0.59,
        "WR": 0.5,
        "TE": 0.36,
        "K": 0.38,
        "DEF": 0.38,
        "DL": 0.6,
        "LB": 0.95,
        "DB": 0.92,
    },
    (12, "tep", 0.5, 6): {"QB": 0.75, "RB": 0.73, "WR": 0.63, "TE": 0.46, "K": 0.53, "DEF": 0.66},
    (12, "tep", 1.0, 6): {"QB": 0.84, "RB": 0.73, "WR": 0.65, "TE": 0.49, "K": 0.6, "DEF": 0.66},
    (12, "flx", "ppfd", 6): {"QB": 0.51, "RB": 0.56, "WR": 0.46, "TE": 0.32, "K": 0.42, "DEF": 0.49},
    (12, "sflx", "ppfd", 6): {"QB": 0.78, "RB": 0.64, "WR": 0.51, "TE": 0.35, "K": 0.34, "DEF": 0.49},
    (12, "idp", "ppfd", 6): {
        "QB": 0.67,
        "RB": 0.69,
        "WR": 0.58,
        "TE": 0.41,
        "K": 0.39,
        "DEF": 0.38,
        "DL": 0.6,
        "LB": 0.95,
        "DB": 0.92,
    },
}

K_DEF_SIZE_PERCENTILES = {
    10: {"K": 0.30, "DEF": 0.36},
    12: {"K": 0.38, "DEF": 0.45},
}

# wave58 LAMAR fix: the thin tep/idp census samples produced structurally-impossible
# replacement percentiles (12t tep QB 0.85 > superflex 0.81, impossible for a 1-QB
# format; idp LB pinned at 0.95, DL/DB swinging 0.29->0.95). Correct them by derivation
# instead of the noisy small-sample values:
#   * tep is a 1-QB flex roster whose TE-premium VALUE flows through the fpts_*_tep
#     points (see the roster=="tep" branch in build_lamar_enrichment_sql_fast), so its
#     replacement DEPTH is identical to flx -> copy flx percentiles wholesale.
#   * idp offensive depth == flx (idp leagues are 1-QB); idp DL/LB/DB replacement ==
#     fantasy-IDP startable depth / weekly super-table pool, derived 2026-07 from Fly
#     2022-25 idp leagues (DL 23/214.5=0.11, LB 49.5/205.2=0.24, DB 36/275.8=0.13),
#     matching the season-distinct-started method used for the offensive positions.
# Runs BEFORE the K/DEF size override below so K/DEF still resolve to the size baseline.
_IDP_REPLACEMENT_PCT = {"DL": 0.11, "LB": 0.24, "DB": 0.13}
for _key in list(CENSUS_PERCENTILES):
    _ksize, _kroster, _kppr, _ktd = _key
    if _kroster in ("tep", "idp"):
        _flx = CENSUS_PERCENTILES.get((_ksize, "flx", _kppr, _ktd))
        if _flx:
            for _p in ("QB", "RB", "WR", "TE", "K", "DEF"):
                if _p in CENSUS_PERCENTILES[_key] and _p in _flx:
                    CENSUS_PERCENTILES[_key][_p] = _flx[_p]
    if _kroster == "idp":
        for _p, _v in _IDP_REPLACEMENT_PCT.items():
            if _p in CENSUS_PERCENTILES[_key]:
                CENSUS_PERCENTILES[_key][_p] = _v

for _key, _percentiles in CENSUS_PERCENTILES.items():
    _size = _key[0]
    for _position, _percentile in K_DEF_SIZE_PERCENTILES[_size].items():
        if _position in _percentiles:
            _percentiles[_position] = _percentile

# Column naming: lamar_{size}t_{roster}_{ppr}_{td}pt
# Two PPR mappings:
#   SLUG labels (for LAMAR column names): 0.0 -> "std" (readable)
#   DB labels (for fpts/rank column lookups): 0.0 -> "0ppr" (matches actual DB columns)
PPR_SLUG = {0.0: "std", 0.5: "half", 1.0: "ppr", "ppfd": "ppfd"}
PPR_DB = {0.0: "0ppr", 0.5: "half", 1.0: "ppr", "ppfd": "ppfd"}

# Points column used for LAMAR subtraction per position
# MUST match what the rank column is ordered by
POINTS_COLUMN_MAP = {
    "QB": lambda td, ppr: f"fpts_{td}pt_{PPR_DB[ppr]}",
    "RB": lambda td, ppr: f"fpts_{td}pt_{PPR_DB[ppr]}",
    "WR": lambda td, ppr: f"fpts_{td}pt_{PPR_DB[ppr]}",
    "TE": lambda td, ppr: f"fpts_{td}pt_{PPR_DB[ppr]}",
    "K": lambda td, ppr: "pts_k_yds",
    "DEF": lambda td, ppr: "pts_def_std",
    "DL": lambda td, ppr: "pts_idp_std",
    "LB": lambda td, ppr: "pts_idp_std",
    "DB": lambda td, ppr: "pts_idp_std",
}

# Rank column per position per scoring variant
RANK_COLUMN_MAP = {
    "QB": lambda td, ppr: f"rank_qb_{td}pt",
    "RB": lambda td, ppr: f"rank_rb_{PPR_DB[ppr]}",
    "WR": lambda td, ppr: f"rank_wr_{PPR_DB[ppr]}",
    "TE": lambda td, ppr: f"rank_te_{PPR_DB[ppr]}",
    "K": lambda td, ppr: "rank_k",
    "DEF": lambda td, ppr: "rank_def",
    "DL": lambda td, ppr: "rank_dl_std",
    "LB": lambda td, ppr: "rank_lb_std",
    "DB": lambda td, ppr: "rank_db_std",
}


def get_all_config_keys():
    """Return all supported (size, roster, ppr, td) tuples."""
    return list(CENSUS_PERCENTILES.keys())


def lamar_column_name(size, roster, ppr, td):
    """Generate column name like 'lamar_12t_flx_ppr_4pt'."""
    return f"lamar_{size}t_{roster}_{PPR_SLUG[ppr]}_{td}pt"


def lamar_ppg_column_name(size, roster, ppr, td):
    """Generate PPG column name like 'lamar_ppg_12t_flx_ppr_4pt'."""
    return f"lamar_ppg_{size}t_{roster}_{PPR_SLUG[ppr]}_{td}pt"


def get_all_lamar_columns():
    """Return list of all supported LAMAR column names."""
    return [lamar_column_name(*k) for k in get_all_config_keys()]


def get_all_lamar_ppg_columns():
    """Return list of all supported LAMAR PPG column names."""
    return [lamar_ppg_column_name(*k) for k in get_all_config_keys()]


def positions_list(key):
    """Get quoted position list for a config."""
    positions = list(CENSUS_PERCENTILES[key].keys())
    return ", ".join(f"'{p}'" for p in positions)


def positions_array(key):
    """Get DuckDB VARCHAR[] literal for a config's positions."""
    positions = list(CENSUS_PERCENTILES[key].keys())
    return "[" + ", ".join(f"'{p}'" for p in positions) + "]"


def position_has(pos: str, column: str = "position") -> str:
    """DuckDB predicate: comma-separated fantasy-position set contains pos."""
    return f"list_has_any(string_split(COALESCE({column}, ''), ','), ['{pos}'])"


def position_has_any(key, column: str = "position") -> str:
    return f"list_has_any(string_split(COALESCE({column}, ''), ','), {positions_array(key)})"


# ---------------------------------------------------------------------------
# SQL generation & enrichment
# ---------------------------------------------------------------------------


def build_lamar_enrichment_sql_fast(table_name="___ops.nfl_historical.nfl_player_stats_all", year=None):
    """Return list of (column_name, [sql_statements]) tuples for all supported LAMAR columns.

    Each tuple contains three SQL statements:
      1. CREATE OR REPLACE TEMP TABLE with per-position replacement points
      2. UPDATE super table: lamar = player_points - replacement_points
      3. UPDATE super table: NULL for positions not in this config
    """
    updates = []
    for key in get_all_config_keys():
        size, roster, ppr, td = key
        col = lamar_column_name(size, roster, ppr, td)
        percentiles = CENSUS_PERCENTILES[key]
        year_filter = f"AND year = {year}" if year else ""
        year_filter_s = f"AND s.year = {year}" if year else ""

        # Build UNION ALL of per-position replacement queries
        # Uses ROW_NUMBER() on fpts directly instead of pre-computed rank columns,
        # which have gaps (e.g. rank_rb_half only covers ~60 of ~80 active RBs).
        position_queries = []
        source_predicates_s = []
        for pos, pct in percentiles.items():
            pts_col = POINTS_COLUMN_MAP[pos](td, ppr)
            if roster == "tep" and pos == "TE":
                pts_col = f"fpts_{td}pt_tep"  # TE-premium basis for tep roster_type
            source_predicates_s.append(
                f"({position_has(pos, 's.position')} AND s.{pts_col} IS NOT NULL AND isfinite(s.{pts_col}))"
            )
            position_queries.append(
                f"""
                SELECT year, week, season_type, '{pos}' as position,
                    GREATEST(AVG({pts_col}), 0) as replacement_pts
                FROM (
                    SELECT year, week, season_type, {pts_col},
                        ROW_NUMBER() OVER (
                            PARTITION BY year, week, season_type
                            ORDER BY {pts_col} DESC
                        ) as rn,
                        COUNT(*) OVER (PARTITION BY year, week, season_type) as total
                    FROM {table_name}
                    WHERE {position_has(pos)} AND {pts_col} IS NOT NULL AND isfinite({pts_col}) {year_filter}
                )
                WHERE rn BETWEEN
                    LEAST(CEIL(total * {pct}), total)
                    AND LEAST(CEIL(total * {pct}) + 1, total)
                GROUP BY year, week, season_type"""
            )

        create_temp = f"CREATE OR REPLACE TEMP TABLE _repl_{col} AS " + " UNION ALL ".join(position_queries)

        # Build CASE for positions in this config
        case_parts = []
        for pos in percentiles:
            pts_col = POINTS_COLUMN_MAP[pos](td, ppr)
            if roster == "tep" and pos == "TE":
                pts_col = f"fpts_{td}pt_tep"  # TE-premium basis for tep roster_type
            case_parts.append(f"WHEN r.position = '{pos}' THEN s.{pts_col} - COALESCE(r.replacement_pts, 0)")

        update_sql = f"""
            UPDATE {table_name} s
            SET {col} = c.lamar
            FROM (
              SELECT s.rowid AS rid, MAX(CASE {" ".join(case_parts)} ELSE NULL END) AS lamar
              FROM {table_name} s
              JOIN _repl_{col} r
                ON s.year = r.year
               AND s.week = r.week
               AND s.season_type = r.season_type
               AND list_contains(string_split(COALESCE(s.position, ''), ','), r.position)
              WHERE {position_has_any(key, "s.position")}
                AND ({" OR ".join(source_predicates_s)})
                {year_filter_s}
              GROUP BY s.rowid
            ) c
            WHERE s.rowid = c.rid
        """

        # NULL for positions not in this config, or eligible rows with no finite source lane.
        null_sql = f"""
            UPDATE {table_name} s
            SET {col} = NULL
            WHERE (
                NOT {position_has_any(key, "s.position")}
                OR ({position_has_any(key, "s.position")} AND NOT ({" OR ".join(source_predicates_s)}))
            ) {year_filter_s}
        """

        updates.append((col, [create_temp, update_sql, null_sql]))
    return updates


def ensure_lamar_columns(conn, table_name="___ops.nfl_historical.nfl_player_stats_all"):
    """Add all supported LAMAR columns to the super table if they don't exist."""
    for col in get_all_lamar_columns():
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
    print(f"Ensured {len(get_all_lamar_columns())} lamar columns in {table_name}")


def enrich_research_lamar(conn, table_name="___ops.nfl_historical.nfl_player_stats_all", year=None):
    """Compute and write all supported LAMAR columns to the super table."""
    ensure_lamar_columns(conn, table_name)
    updates = build_lamar_enrichment_sql_fast(table_name, year=year)
    for i, (col, sql_stmts) in enumerate(updates):
        print(f"  [{i + 1}/{len(updates)}] Computing {col}...")
        for stmt in sql_stmts:
            conn.execute(stmt)
    print(f"Done: {len(updates)} LAMAR columns enriched")


def wrap_lamar_query_with_corrections(
    base_select_cols: list[str],
    scoring_row: dict,
    lamar_col_names: list[str],
) -> list[str]:
    """Apply per-league correction SQL to selected LAMAR columns.

    Non-LAMAR columns pass through unchanged. Corrected LAMAR expressions keep
    the original output shape by aliasing back to the input column name.
    """
    from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row
    from multi_league.core.scoring_config import compute_lamar_with_corrections

    scoring = (
        extract_scoring_settings_from_flat_row(scoring_row)
        if any(isinstance(key, str) and key.startswith("scoring_") for key in scoring_row)
        else scoring_row
    )
    lamar_set = set(lamar_col_names)
    out: list[str] = []
    for col in base_select_cols:
        if col not in lamar_set:
            out.append(col)
            continue
        corrected = compute_lamar_with_corrections(scoring, col)
        out.append(col if corrected == col else f"{corrected} AS {col}")
    return out
