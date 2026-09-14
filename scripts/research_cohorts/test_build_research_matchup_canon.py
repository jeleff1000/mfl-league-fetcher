"""Fixture test for the canonical start-rate-weighted LAMAR SQL (Joe 2026-07-19, ledger D2):
hand-computed expectations for the exact CREATE TABLE statements main() runs."""
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_research_matchup_cohort import (
    CANON0_SQL,
    CANON_LATTICE_SQL,
    NSTL_SQL,
    PO_LATTICE_SQL,
    PSV_PRUNE_SQL,
)


@pytest.fixture()
def con():
    con = duckdb.connect()
    # sa: started-and-active league counts per concrete cell x player x week
    con.execute("""CREATE TABLE sa (teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
        league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
        year INTEGER, NFL_player_id VARCHAR, week INTEGER, pos_grp VARCHAR,
        source_teams VARCHAR, sa BIGINT)""")
    con.execute("""INSERT INTO sa VALUES
        ('12t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1',1,'SKILL','12t',2),
        ('12t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1',2,'SKILL','12t',1),
        ('10t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1',1,'SKILL','10t',1),
        ('12t','sflx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1',1,'SKILL','12t',3)""")
    # dw: eligible leagues still playing, per cell x pos_grp x week (concrete rows only needed)
    con.execute("""CREATE TABLE dw (teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
        league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
        year INTEGER, pos_grp VARCHAR, week INTEGER, n_lg BIGINT, n_champ_lg BIGINT)""")
    con.execute("""INSERT INTO dw VALUES
        ('12t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'SKILL',1,4,0),
        ('12t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'SKILL',2,2,0),
        ('10t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'SKILL',1,1,0),
        ('12t','sflx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'SKILL',1,3,0)""")
    # psv: slug canon values (no sflx slug exists -- the 12-slug space is flx-only)
    con.execute("""CREATE TABLE psv (NFL_player_id VARCHAR, year INTEGER, week INTEGER,
        slug VARCHAR, lamar DOUBLE)""")
    con.execute("""INSERT INTO psv VALUES
        ('P1',2020,1,'12t_flx_ppr_4pt',10.0),
        ('P1',2020,2,'12t_flx_ppr_4pt',6.0),
        ('P1',2020,1,'10t_flx_ppr_4pt',20.0)""")
    con.execute("""CREATE TABLE nst (teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
        league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
        year INTEGER, NFL_player_id VARCHAR, pos_grp VARCHAR, n_lg_started BIGINT)""")
    con.execute("""INSERT INTO nst VALUES
        ('12t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1','SKILL',2),
        ('10t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1','SKILL',1),
        ('12t','sflx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1','SKILL',3)""")
    con.execute(CANON0_SQL)
    con.execute(CANON_LATTICE_SQL)
    con.execute(NSTL_SQL)
    return con


def row(con, teams, roster, ppr, td):
    return con.execute(
        """SELECT num_sr, den_sr, num_abs FROM canon
        WHERE teams=? AND roster=? AND ppr=? AND td=?
          AND league_type='redraft' AND lineup_mode='managed' AND keeper_mode='non_keeper'""",
        [teams, roster, ppr, td]).fetchone()


def test_concrete_cell_matches_hand_formula(con):
    # 12t cell: sr(1)=2/4, sr(2)=1/2; canon 10, 6
    # num_sr = 10*0.5 + 6*0.5 = 8; den_sr = 1.0 -> avg = 8; num_abs = 10*2 + 6*1 = 26
    num_sr, den_sr, num_abs = row(con, '12t', 'flx', 'ppr', '4pt')
    assert num_sr == pytest.approx(8.0)
    assert den_sr == pytest.approx(1.0)
    assert num_abs == pytest.approx(26.0)


def test_nonflx_fails_closed(con):
    # the sflx cell has sa rows but no slug in the canon -> NO canon row at that cell
    assert row(con, '12t', 'sflx', 'ppr', '4pt') is None
    # and it contributes NOTHING to the coarse rollup (num stays flx-only)
    num_sr, den_sr, num_abs = con.execute(
        "SELECT num_sr, den_sr, num_abs FROM canon WHERE teams='ALL'").fetchone()
    assert num_sr == pytest.approx(8.0 + 20.0)   # 12t cell + 10t cell (sr(1)=1/1 -> 20*1)
    assert den_sr == pytest.approx(1.0 + 1.0)
    assert num_abs == pytest.approx(26.0 + 20.0)


def test_per_manager_expected_total(con):
    # 12t cell: num_abs 26 over 2 starting leagues -> expected total 13
    num_abs = row(con, '12t', 'flx', 'ppr', '4pt')[2]
    n = con.execute("""SELECT n_lg_started FROM nstl
        WHERE teams='12t' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND league_type='redraft'""").fetchone()[0]
    assert num_abs / n == pytest.approx(13.0)
    # coarse rollup covers the SAME universe as the canon numerator: sflx starting leagues
    # fail closed out of both sides, so the coarse expected-total is unbiased
    n_all = con.execute("""SELECT n_lg_started FROM nstl
        WHERE teams='ALL' AND league_type='redraft'""").fetchone()[0]
    assert n_all == 2 + 1


def test_playoff_lattice_rolls_up_counts(con):
    # two concrete cells: (2 rostered lg, 1.5 wkwt credit, 1 final, 1 started-po) and
    # (1 lg, 1.0, 1, 0) -> coarse row sums; rates derive from summed counts
    con.execute("""CREATE TABLE po0 (teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
        league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
        year INTEGER, NFL_player_id VARCHAR, pos_grp VARCHAR, n_rost_lg BIGINT,
        n_po_resolved_lg BIGINT, wkwt_credit DOUBLE,
        n_final_po BIGINT, n_started_po BIGINT)""")
    con.execute("""INSERT INTO po0 VALUES
        ('12t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1','SKILL',2,2,1.5,1,1),
        ('10t','flx','ppr','4pt','6po','redraft','managed','non_keeper',2020,'P1','SKILL',1,1,1.0,1,0)""")
    con.execute(PO_LATTICE_SQL)
    n, credit, fin, st = con.execute("""SELECT n_rost_lg, wkwt_credit, n_final_po, n_started_po
        FROM pol WHERE teams='ALL' AND league_type='redraft'""").fetchone()
    assert (n, fin, st) == (3, 2, 1)
    assert credit == pytest.approx(2.5)
    # served rates: wkwt 2.5/3, final 2/3, started 1/3
    assert 100 * credit / n == pytest.approx(83.333, abs=0.01)


def test_canonical_lookup_prunes_unneeded_slugs():
    con = duckdb.connect()
    con.execute("""CREATE TABLE sa (
        teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
        league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
        year INTEGER, NFL_player_id VARCHAR, week INTEGER, pos_grp VARCHAR,
        source_teams VARCHAR, sa BIGINT)""")
    con.execute("""INSERT INTO sa VALUES
        ('12t','flx','half','4pt','6po','redraft','managed','non_keeper',2020,'P1',1,'SKILL','12t',2),
        ('12t','flx','half','4pt','6po','redraft','managed','non_keeper',2020,'P1',1,'SKILL','12t',2),
        ('12t','sflx','half','4pt','6po','redraft','managed','non_keeper',2020,'P2',1,'SKILL','12t',3)""")
    con.execute("""CREATE TABLE _psv_all (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, slug VARCHAR, lamar DOUBLE)""")
    con.execute("""INSERT INTO _psv_all VALUES
        ('P1',2020,1,'12t_flx_half_4pt',10),
        ('P2',2020,1,'12t_flx_half_4pt',15),
        ('P2',2020,1,'10t_flx_ppr_6pt',20)""")

    con.execute(PSV_PRUNE_SQL)

    assert con.execute("SELECT slug FROM psv").fetchall() == [
        ("12t_flx_half_4pt",),
    ]
