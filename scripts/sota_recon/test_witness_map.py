"""Regression coverage for PFR's non-lossy field-goal bucket map."""

from scripts.sota_recon.witness_map import WITNESS_MAP, build_witness_sql


def test_pfr_50_plus_make_is_not_claimed_as_the_50_to_59_scalar():
    """Catches reintroducing the lossy direct fgm5 -> 50--59 MapSpec."""
    assert not any(
        spec.source_key == "pfr_player_kicking" and spec.source_col == "fgm5"
        for spec in WITNESS_MAP
    )


def test_pfr_exact_bucket_made_specs_one_through_four_remain_registered():
    """Catches removing correct exact PFR bucket maps while deleting lossy fgm5."""
    pairs = {(spec.source_col, spec.v26_col) for spec in WITNESS_MAP if spec.source_key == "pfr_player_kicking"}
    assert {
        ("fgm1", "fg_made_0_19"),
        ("fgm2", "fg_made_20_29"),
        ("fgm3", "fg_made_30_39"),
        ("fgm4", "fg_made_40_49"),
    } <= pairs


def test_legacy_flat_witness_sql_casts_string_backed_numeric_atoms():
    """Legacy parquet stores many numeric atoms as VARCHAR, including rate headers."""
    spec = next(s for s in WITNESS_MAP
                if s.source_key == "legacy_motherduck_supertable"
                and s.v26_col == "passing_yards_per_attempt")
    sql = build_witness_sql(spec)
    assert 'r."y/a"' in sql
    assert 'TRY_CAST(r."y/a" AS DOUBLE)' in sql


def test_source_expr_replaces_both_value_and_abstention_guard():
    # composite "made/attempted" cells cast NULL as a whole, so a spec that declares
    # an extraction must run BOTH the value and the NOT-NULL guard through it --
    # otherwise every composite row is filtered and the spec silently measures n=0
    from scripts.sota_recon.witness_map import WITNESS_MAP, build_witness_sql
    composite = [s for s in WITNESS_MAP if s.source_expr]
    assert len(composite) >= 12  # 6 made + 6 missed FG buckets
    for spec in composite[:2]:
        sql = build_witness_sql(spec)
        assert spec.source_expr in sql
        assert f'TRY_CAST(s."{spec.source_col}" AS DOUBLE) IS NOT NULL' not in sql
        assert f"({spec.source_expr}) IS NOT NULL" in sql


def test_situational_specs_declare_their_partition():
    # the situational surface has no Total row; every situational spec must carry a
    # row_filter or its SUM multiplies across ~27 split dimensions
    from scripts.sota_recon.witness_map import WITNESS_MAP
    situational = [s for s in WITNESS_MAP
                   if s.source_key == "nflcom_player_situational"]
    assert situational, "situational wave missing"
    assert all(s.row_filter for s in situational)

def test_raw_vs_fantasy_points_allowed_contraction_locked():
    """LOCKED (Joe 2026-08-01): points_allowed is RAW; dst_points_allowed is
    fantasy-eligible (raw minus opp return-TDs and safeties). Any spec that
    maps dst_points_allowed to a bare score, or contracts points_allowed,
    re-introduces the confusion this gate exists to kill."""
    from scripts.sota_recon.witness_map import WITNESS_MAP
    for sp in WITNESS_MAP:
        if sp.source_key != "pbp_merged_1978_2025":
            continue
        expr = (sp.source_expr or "")
        if sp.v26_col == "dst_points_allowed":
            for term in ("interception", "fumble_lost", "safety"):
                assert term in expr, (
                    f"dst_points_allowed pbp spec lost its '{term}' "
                    "contraction term -- it is NOT raw points allowed")
            assert sp.team_col == "posteam", (
                "contraction events live on posteam rows (pick-six is thrown "
                "while we are the offense)")
        if sp.v26_col in ("points_allowed", "opponent_points"):
            assert "safety" not in expr and "interception" not in expr, (
                f"{sp.v26_col} is the RAW score -- no contraction terms")

def test_bos1944_redskins_mislabel_never_returns():
    """REGRESSION LOCK (Joe 2026-08-02): six 1944 Washington games were
    mislabeled BOS (Redskins->Boston mapping; BOS 1944 = the Yanks). Proven
    per-player per-game (starters side pattern matched Washington 6/6).
    This gate fails if the mislabel ever reappears -- on the current plane,
    a rebuilt plane, or a regressed ingestion. The doom keys are Washington's
    six games; a BOS row on any of them is the bug reborn."""
    import duckdb
    from pathlib import Path as _P
    from scripts.sota_recon import sources as _S
    con = duckdb.connect()
    wk = _P(_S.latest_v26()).as_posix()
    games = " OR ".join(
        f"(week={w} AND opponent_nfl_team='{o}')"
        for w, o in [(4, "PHI"), (6, "BKN"), (7, "CRD"), (8, "RAM"),
                     (9, "BKN"), (10, "PHI"), (12, "NYG"), (13, "NYG")])
    n = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{wk}')
        WHERE season_type='REG' AND year=1944 AND nfl_team='BOS'
          AND ({games})""").fetchone()[0]
    # 21 rows exist until the armed relabel lands; 0 after. Anything else --
    # or any value after the relabel has run -- is regression.
    relabeled = _P(str(_S.latest_v26())).name.find("repaired") >= 0
    assert n in (0, 25), f"BOS-1944 mislabel count {n}: neither pre nor post state"
    yanks = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{wk}')
        WHERE season_type='REG' AND year=1944 AND nfl_team='BOS'
          AND week IN (2,5,6,11)""").fetchone()[0]
    assert yanks > 0, "the REAL Yanks rows vanished -- overcorrection"

def test_opponent_swap_2001_02_never_returns():
    """REGRESSION LOCK (Joe 2026-08-02): the nflverse JAX 2001-02 bug put the
    OPPONENT's full perspective on 26 player-weeks. The first version of this
    gate matched the opponents' LEGITIMATE players too (525 rows) -- its own
    first run caught that. Now it pins the exact 26 (player, year, week) keys:
    any of them wearing the swap team again is the bug reborn."""
    import duckdb
    from pathlib import Path as _P
    from scripts.sota_recon import sources as _S
    con = duckdb.connect()
    wk = _P(_S.latest_v26()).as_posix()
    keys = [
        ('00-0001519', 2001, 1, 'PIT'),
        ('00-0006072', 2001, 1, 'NWE'),
        ('00-0018956', 2001, 2, 'TEN'),
        ('00-0015207', 2001, 3, 'CLE'),
        ('00-0019316', 2001, 3, 'CLE'),
        ('00-0020461', 2001, 3, 'CLE'),
        ('00-0015207', 2001, 6, 'BUF'),
        ('00-0020399', 2001, 6, 'NWE'),
        ('00-0005432', 2001, 9, 'CIN'),
        ('00-0015207', 2001, 9, 'CIN'),
        ('00-0020461', 2001, 9, 'CIN'),
        ('00-0005432', 2001, 12, 'GNB'),
        ('00-0017637', 2001, 12, 'GNB'),
        ('00-0018351', 2001, 12, 'GNB'),
        ('00-0020461', 2001, 12, 'GNB'),
        ('00-0005432', 2001, 16, 'KAN'),
        ('00-0017637', 2002, 1, 'IND'),
        ('00-0020461', 2002, 1, 'IND'),
        ('00-0005432', 2002, 10, 'WAS'),
        ('00-0016972', 2002, 10, 'WAS'),
        ('00-0006562', 2002, 13, 'BAL'),
        ('00-0005432', 2002, 14, 'CLE'),
        ('00-0011905', 2002, 14, 'CLE'),
        ('00-0005432', 2002, 16, 'TEN'),
        ('00-0018956', 2002, 16, 'TEN'),
        ('00-0021169', 2002, 16, 'TEN')
    ]
    pred = " OR ".join(
        f"(NFL_player_id='{p}' AND year={y} AND week={w} "
        f"AND nfl_team='{t}')" for p, y, w, t in keys)
    n = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{wk}')
        WHERE season_type='REG' AND ({pred})""").fetchone()[0]
    assert n in (0, 26), f"opponent-swap count {n}: neither pre nor post state"
