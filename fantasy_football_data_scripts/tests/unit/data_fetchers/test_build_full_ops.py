"""Offline tests for build_full_ops (WS-B v1): sidecar split + compat view + bundle."""

from pathlib import Path

import duckdb
import pytest

from multi_league.data_fetchers.build_full_ops import (
    COMPAT_VIEW,
    SIDECAR_TABLES,
    apply_weekly_update,
    build_ops_promotion_bundle,
    classify_wide_columns,
    create_compatibility_view,
    maintain_primary_position,
    qident,
    rebuild_career_sidecar,
    rebuild_season_partitions,
    rebuild_weekly_rank_partition,
    recompute_base_windows,
    split_wide_to_sidecars,
    stage_new_week,
    validate_compatibility,
    weekly_rank_specs,
)
from multi_league.data_fetchers.build_full_ops import WINDOW_BASE_COLUMNS
from multi_league.data_fetchers.live_nfl_ops_refresh import rebuild_wide_rank_surface

WIDE_COLUMNS = [
    "player_week",
    "NFL_player_id",
    "year",
    "week",
    "player",
    "nfl_position",
    "fpts_4pt_0ppr",
    "rolling_3",
    "rank_qb_4pt",
    "rank_flex_0ppr",
    "rank_season_qb_4pt",
    "rank_season_flex_0ppr",
    "ppg_season_4pt_0ppr",
    "consistency_4pt_0ppr",
    "rank_alltime_qb_4pt",
    "ppg_alltime_4pt_0ppr",
]


def _make_wide(conn, *, inconsistent_row=False):
    conn.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    cols_ddl = ", ".join(
        f'"{c}" ' + ("VARCHAR" if c in ("player_week", "NFL_player_id", "player", "nfl_position") else "DOUBLE")
        for c in WIDE_COLUMNS
    )
    conn.execute(f"CREATE TABLE nfl_historical.nfl_player_stats_all ({cols_ddl})")
    rows = []
    for pid in range(6):
        for year in (2024, 2025):
            for week in (1, 2, 3):
                season_rank = float(pid + 1)
                rows.append(
                    (
                        f"nfl_{pid}_{year}_{week}",
                        f"nfl_{pid}",
                        float(year),
                        float(week),
                        f"Player {pid}",
                        "QB" if pid % 2 == 0 else "RB",
                        10.0 + pid + week,
                        9.0 + pid,
                        float((pid + week) % 6 + 1),
                        float((pid + week) % 12 + 1),
                        season_rank,
                        season_rank + 1,
                        15.0 + pid,
                        0.5 + pid / 10,
                        float(pid + 30),
                        12.0 + pid,
                    )
                )
    if inconsistent_row:
        # Mirrors the live data quirk: a duplicate-identity week with zeroed
        # "season-constant" values for the same (NFL_player_id, year).
        rows.append(
            ("nfl_0_2024_99", "nfl_0", 2024.0, 17.0, "Player 0", "QB",
             None, None, None, None, 99.0, 99.0, 0.0, 0.0, 0.0, 0.0)
        )
    placeholders = ", ".join(["?"] * len(WIDE_COLUMNS))
    conn.executemany(f"INSERT INTO nfl_historical.nfl_player_stats_all VALUES ({placeholders})", rows)


def test_classify_covers_every_column_once():
    fam = classify_wide_columns(WIDE_COLUMNS)
    assigned = fam["identity"] + fam["base"] + fam["weekly_rank"] + fam["season"] + fam["career"]
    assert sorted(assigned) == sorted(WIDE_COLUMNS)
    assert fam["identity"] == ["player_week", "NFL_player_id", "year", "week"]
    assert set(fam["weekly_rank"]) == {"rank_qb_4pt", "rank_flex_0ppr"}
    assert set(fam["season"]) == {
        "rank_season_qb_4pt",
        "rank_season_flex_0ppr",
        "ppg_season_4pt_0ppr",
        "consistency_4pt_0ppr",
    }
    assert set(fam["career"]) == {"rank_alltime_qb_4pt", "ppg_alltime_4pt_0ppr"}
    # rolling_3 varies weekly and must stay in base.
    assert "rolling_3" in fam["base"]


@pytest.mark.parametrize("messy", [False, True])
def test_vertical_split_reproduces_wide_exactly(messy):
    conn = duckdb.connect(":memory:")
    try:
        _make_wide(conn, inconsistent_row=messy)
        counts = split_wide_to_sidecars(conn, grain="vertical")
        expected_rows = 36 + (1 if messy else 0)
        assert counts == {t: expected_rows for t in SIDECAR_TABLES.values()}
        create_compatibility_view(conn, grain="vertical")
        assert validate_compatibility(conn) == 0
    finally:
        conn.close()


def test_natural_grain_on_clean_data():
    conn = duckdb.connect(":memory:")
    try:
        _make_wide(conn, inconsistent_row=False)
        counts = split_wide_to_sidecars(conn, grain="natural")
        assert counts[SIDECAR_TABLES["season"]] == 12  # 6 players x 2 years
        assert counts[SIDECAR_TABLES["career"]] == 6
        create_compatibility_view(conn, grain="natural")
        assert validate_compatibility(conn) == 0
    finally:
        conn.close()


def test_natural_grain_rejects_inconsistent_data_loudly():
    conn = duckdb.connect(":memory:")
    try:
        _make_wide(conn, inconsistent_row=True)
        with pytest.raises(RuntimeError, match="natural grain violated"):
            split_wide_to_sidecars(conn, grain="natural")
    finally:
        conn.close()


def test_promotion_bundle_roundtrip(tmp_path):
    conn = duckdb.connect(":memory:")
    try:
        _make_wide(conn)
        split_wide_to_sidecars(conn, grain="vertical")
        manifest = build_ops_promotion_bundle(
            conn, tables=list(SIDECAR_TABLES.values()), out_path=tmp_path / "ops_sidecars.duckdb"
        )
    finally:
        conn.close()

    assert set(manifest["tables"]) == set(SIDECAR_TABLES.values())
    assert all(count == 36 for count in manifest["tables"].values())
    assert (tmp_path / "ops_sidecars.manifest.json").exists()

    # The bundle is a standalone DuckDB file /merge-ops can ATTACH and read.
    check = duckdb.connect(str(tmp_path / "ops_sidecars.duckdb"), read_only=True)
    try:
        tables = {row[0] for row in check.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
        ).fetchall()}
        assert tables == set(SIDECAR_TABLES.values())
        assert check.execute("SELECT COUNT(*) FROM nfl_weekly_ranks").fetchone()[0] == 36
    finally:
        check.close()


# --------------------------------------------------------------------------
# Weekly wiring (WS-B rank wiring). A realistic-but-tiny FACT fixture drives the
# real recompute primitives; the derived surface is computed, never hand-fed.
# --------------------------------------------------------------------------

# Facts the wiring reads. Enough variants to exercise each family; classify_wide_columns
# routes each column to its sidecar by prefix, and the recompute jobs cover the subset.
_FACT_POINT_COLS = [
    "fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr", "fpts_4pt_tep", "fpts_4pt_ppfd",
    "fpts_5pt_0ppr", "fpts_5pt_half", "fpts_5pt_ppr", "fpts_5pt_tep", "fpts_5pt_ppfd",
    "fpts_6pt_0ppr", "fpts_6pt_half", "fpts_6pt_ppr", "fpts_6pt_tep", "fpts_6pt_ppfd",
    "pts_def_std", "pts_k_yds", "pts_k_std", "pts_idp_std", "pts_idp_premium",
    "pts_idp_tackle_heavy", "pts_idp_big_play",
]
_FACT_TEXT_COLS = ["player_week", "NFL_player_id", "player", "position", "nfl_position", "primary_position", "season_type"]
# A representative derived column per family (prefix drives classification).
_DERIVED_COLS = [
    # base window
    "rolling_total_4pt_half", "rolling_3_4pt_half", "rolling_5_4pt_half",
    "rolling_total_def", "rolling_total_k",
    # weekly ranks (one per shape)
    "rank_qb_4pt", "rank_flex_ppr", "rank_k", "rank_def", "rank_idp_flex_std",
    # season families
    "rank_season_qb_4pt", "rank_season_flex_ppr", "rank_season_qb_4pt_ppg",
    "rank_season_overall_4pt_half", "ppg_season_4pt_half", "consistency_4pt_half",
    "weighted_ppg_4pt_half", "avg_pts_next_year_4pt_half",
    # career families
    "rank_alltime_qb_4pt", "rank_alltime_overall_4pt_half", "ppg_alltime_4pt_half",
]
_POS_BY_MOD = {0: "QB", 1: "RB", 2: "WR", 3: "TE", 4: "K", 5: "DEF", 6: "LB", 7: "DB"}


def _make_facts_wide(conn, *, weeks_by_year):
    """Synthetic wide table carrying the fact columns + NULL derived columns.

    weeks_by_year: {year: [week, ...]}. Deterministic, varied points so ranks and
    PPG are non-trivial; season_type REG for weeks <= 14 else POST.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    all_cols = list(dict.fromkeys(_FACT_TEXT_COLS + ["year", "week"] + _FACT_POINT_COLS + _DERIVED_COLS))
    ddl = ", ".join(
        f'{qident(c)} ' + ("VARCHAR" if c in _FACT_TEXT_COLS else "DOUBLE") for c in all_cols
    )
    conn.execute(f"CREATE TABLE nfl_historical.nfl_player_stats_all ({ddl})")
    rows = []
    n_players = 10
    for pid in range(n_players):
        pos = _POS_BY_MOD[pid % 8]
        for year, weeks in weeks_by_year.items():
            for wk in weeks:
                base_pts = float((pid * 3 + wk * 2 + year % 7) % 31)
                vals = {}
                for c in all_cols:
                    if c == "player_week":
                        vals[c] = f"nfl_{pid}_{year}_{wk}"
                    elif c == "NFL_player_id":
                        vals[c] = f"nfl_{pid}"
                    elif c == "player":
                        vals[c] = f"Player {pid}"
                    elif c in ("position", "nfl_position", "primary_position"):
                        vals[c] = pos
                    elif c == "season_type":
                        vals[c] = "REG" if wk <= 14 else "POST"
                    elif c == "year":
                        vals[c] = str(year)  # exercises CAST(year AS INTEGER) paths
                    elif c == "week":
                        vals[c] = str(wk)
                    elif c in _FACT_POINT_COLS:
                        # Distinct per variant so ties are rare; None sometimes to test COALESCE.
                        salt = (hash(c) % 5) / 10.0
                        vals[c] = None if (pid == 9 and c.startswith("fpts")) else round(base_pts + salt, 2)
                    else:
                        vals[c] = None  # derived — recomputed by the wiring
                rows.append(tuple(vals[c] for c in all_cols))
    placeholders = ", ".join(["?"] * len(all_cols))
    conn.executemany(
        f"INSERT INTO nfl_historical.nfl_player_stats_all VALUES ({placeholders})", rows
    )
    return all_cols


def _full_rebuild(conn):
    """Full rebuild from facts: split, then run every recompute primitive over
    all partitions (the --full corrections path)."""
    split_wide_to_sidecars(conn, grain="vertical")
    base = f"nfl_historical.{SIDECAR_TABLES['base']}"
    weeks = conn.execute(
        f"SELECT DISTINCT CAST(year AS INTEGER) y, CAST(week AS INTEGER) w FROM {base} ORDER BY 1,2"
    ).fetchall()
    for y, w in weeks:
        rebuild_weekly_rank_partition(conn, y, w)
    recompute_base_windows(conn)
    years = [int(r[0]) for r in conn.execute(f"SELECT DISTINCT CAST(year AS INTEGER) FROM {base} ORDER BY 1").fetchall()]
    rebuild_season_partitions(conn, years)
    rebuild_career_sidecar(conn)


def _sidecar_checksums(conn):
    """Order-independent content fingerprint of every sidecar, keyed by row."""
    out = {}
    for key, table in SIDECAR_TABLES.items():
        ref = f"nfl_historical.{qident(table)}"
        cols = [r[0] for r in conn.execute(f"DESCRIBE {ref}").fetchall()]
        parts = ", ".join(f"COALESCE(CAST({qident(c)} AS VARCHAR), '<N>')" for c in cols)
        out[key] = conn.execute(
            f"SELECT COUNT(*), SUM(hash(concat_ws('|', {parts})) % 1000003) FROM {ref}"
        ).fetchone()
    return out


def test_apply_weekly_update_equals_full_rebuild():
    """The load-bearing WS-B property: applying the latest week incrementally
    yields byte-identical sidecars to a full rebuild of the complete data."""
    weeks_by_year = {2023: [1, 2, 3], 2024: [1, 2, 3, 4]}

    # Path A: full rebuild of everything.
    a = duckdb.connect(":memory:")
    b = duckdb.connect(":memory:")
    try:
        _make_facts_wide(a, weeks_by_year=weeks_by_year)
        _full_rebuild(a)
        full_ck = _sidecar_checksums(a)

        # Path B: same full fixture, but stash the new week's FACTS, delete them from
        # the wide table, full-rebuild on weeks 1..N-1, then apply the new week.
        _make_facts_wide(b, weeks_by_year=weeks_by_year)
        wide = "nfl_historical.nfl_player_stats_all"
        # Fact columns = base sidecar minus _row_uid and the derived window columns.
        _tmp = duckdb.connect(":memory:")
        _make_facts_wide(_tmp, weeks_by_year={2024: [1]})
        split_wide_to_sidecars(_tmp, grain="vertical")
        base_cols = [r[0] for r in _tmp.execute(f"DESCRIBE nfl_historical.{SIDECAR_TABLES['base']}").fetchall()]
        _tmp.close()
        window = [c for c in WINDOW_BASE_COLUMNS if c in base_cols]
        fact_cols = [c for c in base_cols if c != "_row_uid" and c not in window]
        select = ", ".join(qident(c) for c in fact_cols)
        b.execute(
            f"CREATE TABLE _newweek AS SELECT {select} FROM {wide} "
            "WHERE CAST(year AS INTEGER) = 2024 AND CAST(week AS INTEGER) = 4"
        )
        assert b.execute("SELECT COUNT(*) FROM _newweek").fetchone()[0] > 0
        b.execute(f"DELETE FROM {wide} WHERE CAST(year AS INTEGER) = 2024 AND CAST(week AS INTEGER) = 4")
        _full_rebuild(b)
        manifest = apply_weekly_update(b, "_newweek", 2024, 4)
        assert manifest["season_years_rewritten"] == [2023, 2024]

        inc_ck = _sidecar_checksums(b)
        assert inc_ck == full_ck, f"incremental != full: {inc_ck} vs {full_ck}"
    finally:
        a.close()
        b.close()


def test_live_ops_rank_surface_rebuilds_current_rows_against_full_history():
    """A week-one refresh cannot rank its only current-week QB first all-time."""
    conn = duckdb.connect(":memory:")
    try:
        _make_facts_wide(conn, weeks_by_year={2024: [1, 2, 3], 2026: [1]})
        wide = "nfl_historical.nfl_player_stats_all"
        # Mirror the live fetch plane's defect: it supplies a rank only over
        # the just-fetched week.  The production local-artifact helper must
        # recompute the historical rank family from every retained fact.
        conn.execute(
            f"UPDATE {wide} SET rank_alltime_qb_4pt = 1 "
            "WHERE year = 2026 AND nfl_position = 'QB'"
        )

        rebuild_wide_rank_surface(conn, year=2026, week=1)

        rows = conn.execute(
            f"SELECT DISTINCT rank_alltime_qb_4pt FROM {wide} "
            "WHERE NFL_player_id = 'nfl_0' ORDER BY 1"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] != 1
    finally:
        conn.close()


def test_apply_weekly_update_leaves_prior_weeks_byte_identical():
    """Prior-week base rows and other weekly-rank partitions are untouched."""
    conn = duckdb.connect(":memory:")
    try:
        _make_facts_wide(conn, weeks_by_year={2024: [1, 2, 3]})
        _full_rebuild(conn)
        base = f"nfl_historical.{SIDECAR_TABLES['base']}"
        wk = f"nfl_historical.{SIDECAR_TABLES['weekly_rank']}"
        prior_base = conn.execute(
            f"SELECT SUM(hash(concat_ws('|', player_week, CAST(rolling_total_4pt_half AS VARCHAR))) % 1000003) "
            f"FROM {base} WHERE week < 3"
        ).fetchone()[0]
        prior_wk = conn.execute(
            f"SELECT SUM(hash(concat_ws('|', player_week, CAST(rank_qb_4pt AS VARCHAR))) % 1000003) "
            f"FROM {wk} WHERE week < 3"
        ).fetchone()[0]

        cols = [r[0] for r in conn.execute(f"DESCRIBE {base}").fetchall()]
        window = [c for c in WINDOW_BASE_COLUMNS if c in cols]
        fact_cols = [c for c in cols if c != "_row_uid" and c not in window]
        select = ", ".join(qident(c) for c in fact_cols)
        conn.execute(
            f"CREATE TABLE _nw AS SELECT {select} FROM nfl_historical.nfl_player_stats_all "
            "WHERE CAST(year AS INTEGER) = 2024 AND CAST(week AS INTEGER) = 3"
        )
        # Re-apply week 3 in replace mode (idempotent path).
        apply_weekly_update(conn, "_nw", 2024, 3, mode="replace")

        assert conn.execute(
            f"SELECT SUM(hash(concat_ws('|', player_week, CAST(rolling_total_4pt_half AS VARCHAR))) % 1000003) "
            f"FROM {base} WHERE week < 3"
        ).fetchone()[0] == prior_base
        assert conn.execute(
            f"SELECT SUM(hash(concat_ws('|', player_week, CAST(rank_qb_4pt AS VARCHAR))) % 1000003) "
            f"FROM {wk} WHERE week < 3"
        ).fetchone()[0] == prior_wk
    finally:
        conn.close()


def _new_week_source_without_primary_position(conn, base):
    """A raw new-week (2024 wk4) source carrying `position` but NOT
    `primary_position` — the shape the fetch/build plane actually emits. One
    returning player (nfl_0, weekly position flipped to WR) plus two rookies
    (nfl_10 with a bio row, nfl_11 without)."""
    base_cols = [r[0] for r in conn.execute(f"DESCRIBE {base}").fetchall()]
    window = [c for c in WINDOW_BASE_COLUMNS if c in base_cols]
    fact_cols = [c for c in base_cols if c != "_row_uid" and c not in window]
    src_cols = [c for c in fact_cols if c != "primary_position"]  # builder omits the derived attr
    tmpl = f'FROM {base} WHERE "NFL_player_id" = \'nfl_0\' AND week = \'3\' LIMIT 1'

    def sel(overrides):
        # explicit list (SELECT ... REPLACE only works with *) with literal overrides
        return ", ".join(f"{overrides[c]} AS {qident(c)}" if c in overrides else qident(c) for c in src_cols)

    conn.execute(
        "CREATE TABLE _nw AS SELECT "
        + sel({"player_week": "'nfl_0_2024_4'", "week": "'4'", "position": "'WR'", "nfl_position": "'WR'"})
        + f" {tmpl}"
    )
    conn.execute(
        "INSERT INTO _nw SELECT "
        + sel({"NFL_player_id": "'nfl_10'", "player_week": "'nfl_10_2024_4'", "week": "'4'",
               "player": "'Rookie10'", "position": "'RB'", "nfl_position": "'RB'"})
        + f" {tmpl}"
    )
    conn.execute(
        "INSERT INTO _nw SELECT "
        + sel({"NFL_player_id": "'nfl_11'", "player_week": "'nfl_11_2024_4'", "week": "'4'",
               "player": "'Rookie11'", "position": "'TE'", "nfl_position": "'TE'"})
        + f" {tmpl}"
    )
    # bio: nfl_10 has an authoritative position; nfl_0 disagrees with the stored value
    # (existing must still win); nfl_11 is absent (dominant-weekly fallback).
    conn.execute('CREATE TABLE biotbl ("NFL_player_id" VARCHAR, nfl_position VARCHAR)')
    conn.execute("INSERT INTO biotbl VALUES ('nfl_10', 'WR'), ('nfl_0', 'RB')")
    return fact_cols


def test_maintain_primary_position_existing_stable_new_from_bio_and_dom():
    """Existing players keep their stored primary_position (a new week never
    reshifts it); rookies get COALESCE(bio.nfl_position, dominant weekly position)."""
    conn = duckdb.connect(":memory:")
    try:
        _make_facts_wide(conn, weeks_by_year={2024: [1, 2, 3]})
        _full_rebuild(conn)
        base = f"nfl_historical.{SIDECAR_TABLES['base']}"
        fact_cols = _new_week_source_without_primary_position(conn, base)

        stats = maintain_primary_position(conn, "_nw", bio_ref="biotbl", out_ref="_out")

        rows = dict(conn.execute('SELECT "NFL_player_id", primary_position FROM _out').fetchall())
        assert rows["nfl_0"] == "QB"   # existing: stable, ignores flipped week AND bio 'RB'
        assert rows["nfl_10"] == "WR"  # rookie: bio wins over weekly position 'RB'
        assert rows["nfl_11"] == "TE"  # rookie: dominant-weekly fallback (no bio)
        assert stats == {"players": 3, "existing": 1, "new": 2, "null_primary": 0}

        # The maintained source is now acceptable to stage_new_week (cols == fact cols).
        out_cols = sorted(r[0] for r in conn.execute("DESCRIBE _out").fetchall())
        assert out_cols == sorted(fact_cols)
        assert stage_new_week(conn, "_out", 2024, 4) == 3
    finally:
        conn.close()


def test_apply_weekly_update_bio_ref_stamps_rookie_primary_position():
    """End-to-end: a rookie with no prior row and no source primary_position flows
    through the weekly update with a correct, constant primary_position (so the
    career aggregate — which reads it via ANY_VALUE — never sees NULL)."""
    conn = duckdb.connect(":memory:")
    try:
        _make_facts_wide(conn, weeks_by_year={2024: [1, 2, 3]})
        _full_rebuild(conn)
        base = f"nfl_historical.{SIDECAR_TABLES['base']}"
        _new_week_source_without_primary_position(conn, base)

        manifest = apply_weekly_update(conn, "_nw", 2024, 4, bio_ref="biotbl")
        assert manifest["primary_position"] == {"players": 3, "existing": 1, "new": 2, "null_primary": 0}

        stamped = dict(
            conn.execute(
                f"SELECT \"NFL_player_id\", primary_position FROM {base} "
                "WHERE week = '4' ORDER BY \"NFL_player_id\""
            ).fetchall()
        )
        assert stamped == {"nfl_0": "QB", "nfl_10": "WR", "nfl_11": "TE"}

        # No row that has a weekly position is left without a primary_position.
        assert conn.execute(
            f"SELECT COUNT(*) FROM {base} WHERE position IS NOT NULL AND position <> '' "
            "AND (primary_position IS NULL OR primary_position = '')"
        ).fetchone()[0] == 0

        # The rookie reached the career sidecar with a single constant position.
        career = f"nfl_historical.{SIDECAR_TABLES['career']}"
        assert conn.execute(
            f"SELECT COUNT(*) FROM {career} WHERE player_week = 'nfl_10_2024_4'"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_stage_new_week_guards_reject_bad_input():
    conn = duckdb.connect(":memory:")
    try:
        _make_facts_wide(conn, weeks_by_year={2024: [1, 2]})
        split_wide_to_sidecars(conn, grain="vertical")
        base = f"nfl_historical.{SIDECAR_TABLES['base']}"
        cols = [r[0] for r in conn.execute(f"DESCRIBE {base}").fetchall()]
        window = [c for c in WINDOW_BASE_COLUMNS if c in cols]
        fact_cols = [c for c in cols if c != "_row_uid" and c not in window]
        select = ", ".join(qident(c) for c in fact_cols)

        # Off-scope rows in the source are rejected (target week 5 is new, source is week 1).
        conn.execute(f"CREATE TABLE _bad AS SELECT {select} FROM nfl_historical.nfl_player_stats_all WHERE week = 1")
        with pytest.raises(RuntimeError, match="outside"):
            stage_new_week(conn, "_bad", 2024, 5)  # source is week 1, not week 5

        # Appending an existing week is rejected.
        conn.execute(f"CREATE TABLE _w1 AS SELECT {select} FROM nfl_historical.nfl_player_stats_all WHERE week = 1")
        with pytest.raises(RuntimeError, match="already has"):
            stage_new_week(conn, "_w1", 2024, 1, mode="append")

        # Wrong columns are rejected.
        conn.execute("CREATE TABLE _wrong AS SELECT player_week, year, week FROM nfl_historical.nfl_player_stats_all WHERE week = 1")
        with pytest.raises(RuntimeError, match="source columns must equal"):
            stage_new_week(conn, "_wrong", 2024, 1, mode="replace")
    finally:
        conn.close()


def test_weekly_rank_specs_match_calculator_constants():
    # Guards against the wiring silently drifting from fantasy_points_calculator.
    specs = weekly_rank_specs()
    cols = {col for col, _, _ in specs}
    assert "rank_qb_4pt" in cols and "rank_idp_flex_std" in cols
    # Every spec points at a real points column name shape.
    assert all(pts.startswith(("fpts_", "pts_")) for _, pts, _ in specs)


def test_vertical_split_handles_duplicate_player_week_keys():
    """Live wide table has 12 duplicate player_week keys (1920s rows); the
    _row_uid tie-break must keep reconstruction exact instead of fanning out."""
    conn = duckdb.connect(":memory:")
    try:
        _make_wide(conn)
        # Same player_week key twice with DIFFERENT content:
        conn.execute(
            "INSERT INTO nfl_historical.nfl_player_stats_all VALUES "
            "('BerrCh20_1925_11', 'BerrCh20', 1925, 11, 'Berry', 'QB', 3.0, 1.0, 2, 4, 1, 2, 5.0, 0.1, 40, 4.0), "
            "('BerrCh20_1925_11', 'BerrCh20', 1925, 11, 'Berry', 'QB', 7.0, 2.0, 3, 5, 1, 2, 5.0, 0.1, 40, 4.0)"
        )
        split_wide_to_sidecars(conn, grain="vertical")
        create_compatibility_view(conn, grain="vertical")
        assert validate_compatibility(conn) == 0
        n = conn.execute(
            "SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all_compat "
            "WHERE player_week = 'BerrCh20_1925_11'"
        ).fetchone()[0]
        assert n == 2
    finally:
        conn.close()
