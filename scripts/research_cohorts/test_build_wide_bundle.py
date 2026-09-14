"""Fixture-driven tests for the SQL wide-bundle builder: rung selection honors per-class
thresholds, record ships percent semantics, hollow identities are not emitted, and the
API-bound columns are padded."""
from pathlib import Path

import duckdb
import pytest

from build_wide_bundle import MIN_STABLE, SLUGS, TABLES, build_bundle, exact_matchup_sql

LATTICE = ["teams", "roster", "ppr", "td", "cohort_level"]


def _make_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Minimal rc + ops fixture covering every TABLES source with one player."""
    rc = tmp_path / "rc.duckdb"
    ops = tmp_path / "ops.duckdb"

    con = duckdb.connect(str(rc))
    for name, dataset, src, idc, laddered, stats in TABLES:
        cols = dict.fromkeys(c for c, _ in stats.values())
        id_ddl = ", ".join(f"{c} {'INTEGER' if c in ('year', 'week') else 'VARCHAR'}" for c in idc)
        stat_ddl = ", ".join(
            f"{c} {'VARCHAR' if c in ('adp_source', 'draft_rate_source') else 'DOUBLE'}"
            for c in cols if c != "n_leagues"
        )
        con.execute(f"""CREATE TABLE {src} (teams VARCHAR, roster VARCHAR, ppr VARCHAR,
            td VARCHAR, bracket VARCHAR, cohort_level INTEGER, {id_ddl}, n_leagues INTEGER,
            confidence VARCHAR{', ' + stat_ddl if stat_ddl else ''})""")

    def ins(src, lattice, idvals, n_leagues, conf, **vals):
        table_cols = [r[1] for r in con.execute(f"PRAGMA table_info('{src}')").fetchall()]
        row = dict(zip(LATTICE, lattice)) | {"bracket": "6po"} | idvals | {"n_leagues": n_leagues, "confidence": conf} | vals
        cols = [c for c in table_cols if c in row]
        con.execute(f"INSERT INTO {src} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                    [row[c] for c in cols])

    # matchup: rung 4 too thin for srate (n=40 < 50), rung 3 clears it (n=120);
    # default (ppg) answers at rung 4 (n=40 >= 35); win class never clears (2645).
    pid = {"NFL_player_id": "p1", "year": 2024}
    ins("matchup", ("12t", "flx", "ppr", "4pt", 4), pid, 40, "confident",
        ppg_when_started=20.0, start_rate_pct=90.0, won_pct=55.0, lost_pct=35.0,
        win_rate_pct=61.1, expected_wins=8.8, expected_losses=5.2,
        expected_starts=14.0, avg_lamar_started=3.0, total_lamar_started=42.0,
        avg_clutch_started=0.1, champ_week_rate_pct=25.0,
        playoff_rate_started=50.0, playoff_rate_wkwt=72.0,
        champ_total_pct=12.5, champ_as_starter_pct=7.5,
        playoff_total_pct=45.0, playoff_as_starter_pct=30.0,
        nfl_active_weeks=16.0, inactive_weeks=1.0, started_weeks=560.0,
        champ_elig_leagues=38.0, po_n_rostered_leagues=40.0)
    # A distinct bracket lane must publish to its own normal-width table rather
    # than multiplying the default table's column count.
    ins("matchup", ("12t", "flx", "ppr", "4pt", 4), pid, 40, "confident",
        ppg_when_started=17.0, start_rate_pct=81.0, won_pct=48.0, lost_pct=33.0,
        win_rate_pct=59.3, expected_wins=7.1, expected_losses=4.9,
        expected_starts=12.0, avg_lamar_started=2.0, total_lamar_started=24.0,
        avg_clutch_started=0.1, champ_week_rate_pct=20.0,
        playoff_rate_started=45.0, playoff_rate_wkwt=60.0,
        champ_total_pct=10.0, champ_as_starter_pct=6.0,
        playoff_total_pct=40.0, playoff_as_starter_pct=25.0,
        nfl_active_weeks=16.0, inactive_weeks=1.0, started_weeks=480.0,
        champ_elig_leagues=38.0, po_n_rostered_leagues=40.0,
        bracket="4po")
    ins("matchup", ("12t", "flx", "ppr", "ALL", 3), pid, 120, "confident",
        ppg_when_started=19.0, start_rate_pct=88.0, won_pct=54.0, lost_pct=34.0,
        avg_lamar_started=2.9, avg_clutch_started=0.1)
    # a non-confident row that must never serve
    ins("matchup", ("ALL", "ALL", "ALL", "ALL", 0), pid, 3000, "mushy",
        ppg_when_started=999.0, won_pct=99.0, lost_pct=1.0)
    # an identity with NO confident rows anywhere -> must not appear at all
    ins("matchup", ("12t", "flx", "ppr", "4pt", 4),
        {"NFL_player_id": "hollow", "year": 2024}, 5, "insufficient", ppg_when_started=1.0)
    # weekly: one rung-4 row
    ins("matchup_weekly", ("12t", "flx", "ppr", "4pt", 4),
        {"NFL_player_id": "p1", "year": 2024, "week": 3}, 40, "confident",
        ppg_when_started=21.0, start_rate_pct=90.0, win_rate_pct=66.7,
        expected_wins=0.6, expected_losses=0.3, expected_starts=0.9,
        won_pct=60.0, lost_pct=30.0, avg_lamar_started=4.0,
        total_lamar_started=3.6, avg_clutch_started=0.2,
        started_leagues=36.0, eligible_leagues=40.0)
    # career: rung 4 clears win (n=3000 >= 2645)
    ins("matchup_career", ("12t", "flx", "ppr", "4pt", 4), {"NFL_player_id": "p1"},
        3000, "confident", n_years=2.0, ppg_when_started=18.5,
        win_rate_pct=61.0, expected_wins=17.2, expected_losses=10.8,
        expected_starts=28.0, total_lamar_started=80.0, avg_clutch_started=0.4,
        champ_week_rate_pct=22.0, playoff_rate_started=55.0,
        playoff_rate_wkwt=68.0,
        champ_total_pct=11.0, champ_as_starter_pct=6.0,
        playoff_total_pct=42.0, playoff_as_starter_pct=28.0,
        expected_champs=0.45, expected_playoffs=1.1, active_weeks=32.0,
        inactive_weeks=3.0, started_weeks=1120.0, champ_elig_leagues=76.0,
        po_n_rostered_leagues=80.0, won_pct=53.8, lost_pct=33.7)
    # draft/txn: minimal confident rung-4 rows so their tables build
    ins("draft", ("12t", "flx", "ppr", "4pt", 4), pid, 40, "confident",
        adp=25.0, avg_manager_lamar=2.0, n_drafted=30.0)
    ins("draft", ("12t", "flx", "ppr", "4pt", 4),
        {"NFL_player_id": "thin", "year": 2002}, 1, "insufficient",
        adp=190.0, adp_source="market_blind", draft_rate_pct=100.0,
        draft_score=-5.0, draft_score_healthy=-4.0, n_drafted=1.0)
    ins("draft_career", ("12t", "flx", "ppr", "4pt", 4), {"NFL_player_id": "p1"},
        200, "confident", n_years=2.0, adp=24.0)
    for year in (2023, 2024):
        ins("draft", ("12t", "flx", "ppr", "4pt", 4),
            {"NFL_player_id": "switch", "year": year}, 40, "confident",
            adp=100.0, n_drafted=20.0)
    ins("draft_career", ("12t", "flx", "ppr", "4pt", 4),
        {"NFL_player_id": "switch"}, 200, "confident", n_years=2.0, adp=100.0)
    ins("transactions", ("12t", "flx", "ppr", "4pt", 4), pid, 70, "confident",
        add_rate_pct=12.0, avg_faab_pct=8.0)
    ins("transactions_weekly", ("12t", "flx", "ppr", "4pt", 4),
        {"NFL_player_id": "p1", "year": 2024, "week": 3}, 70, "confident", add_rate_pct=2.0)
    ins("transactions_career", ("12t", "flx", "ppr", "4pt", 4), {"NFL_player_id": "p1"},
        140, "confident", n_years=2.0, add_rate_pct=11.0)
    con.close()

    o = duckdb.connect(str(ops))
    o.execute("CREATE SCHEMA nfl_historical")
    o.execute("""CREATE TABLE nfl_historical.nfl_player_stats_all (
        NFL_player_id VARCHAR, "position" VARCHAR, player VARCHAR, year INTEGER,
        week INTEGER, season_type VARCHAR, fpts_4pt_0ppr DOUBLE, fpts_4pt_half DOUBLE,
        fpts_4pt_ppr DOUBLE, fpts_6pt_0ppr DOUBLE, fpts_6pt_half DOUBLE, fpts_6pt_ppr DOUBLE,
        lamar_10t_flx_std_4pt DOUBLE, lamar_10t_flx_std_6pt DOUBLE,
        lamar_10t_flx_half_4pt DOUBLE, lamar_10t_flx_half_6pt DOUBLE,
        lamar_10t_flx_ppr_4pt DOUBLE, lamar_10t_flx_ppr_6pt DOUBLE,
        lamar_12t_flx_std_4pt DOUBLE, lamar_12t_flx_std_6pt DOUBLE,
        lamar_12t_flx_half_4pt DOUBLE, lamar_12t_flx_half_6pt DOUBLE,
        lamar_12t_flx_ppr_4pt DOUBLE, lamar_12t_flx_ppr_6pt DOUBLE)""")
    # canonical weeklies: p1 2024 = 30 / 11 / 25 ppr-4pt -> season 66, ppg 22.0, week-3 25
    for wk, pts in ((1, 30.0), (2, 11.0), (3, 25.0)):
        o.execute("""INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('p1', 'WR', 'Test Player', 2024, ?, 'REG', ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  [wk, pts, pts, pts, pts, pts, pts, wk, wk, wk, wk, wk, wk, wk, wk, wk, wk, wk, wk])
    # a season decades before any cohort window: careers span the WHOLE career
    o.execute("""INSERT INTO nfl_historical.nfl_player_stats_all VALUES
        ('p1', 'WR', 'Test Player', 1989, 1, 'REG', 10.0, 10.0, 10.0, 10.0, 10.0, 10.0,
         4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0)""")
    o.execute("""INSERT INTO nfl_historical.nfl_player_stats_all
        (NFL_player_id, "position", player) VALUES ('hollow', 'WR', 'Hollow Player')""")
    o.execute("""INSERT INTO nfl_historical.nfl_player_stats_all
        (NFL_player_id, "position", player, year, week, season_type) VALUES
        ('switch', 'WR', 'Role Switch', 2023, 1, 'REG'),
        ('switch', 'DB', 'Role Switch', 2024, 1, 'REG')""")
    # bio: identity fallback for never-played players (ledger D7) -- 'stash' has NO stat rows
    o.execute("""CREATE TABLE nfl_historical.player_bio (
        NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR)""")
    o.execute("""INSERT INTO nfl_historical.player_bio VALUES
        ('p1', 'Test Player', 'WR'), ('stash', 'Stashed Rookie', 'QB')""")
    o.close()
    return rc, ops


@pytest.fixture(scope="module")
def bundle(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("bundle")
    rc, ops = _make_fixture(tmp)
    out = tmp / "bundle.duckdb"
    counts = build_bundle(rc, ops, out)
    con = duckdb.connect(str(out), read_only=True)
    yield con, counts
    con.close()


def test_matchup_keeps_the_exact_selected_cohort(bundle):
    con, _ = bundle
    row = con.execute("""SELECT lamar_12t_flx_ppr_4pt, level_12t_flx_ppr_4pt,
        start_rate_12t_flx_ppr_4pt, level_srate_12t_flx_ppr_4pt, n_srate_12t_flx_ppr_4pt
        FROM research_matchup WHERE NFL_player_id='p1'""").fetchone()
    assert row[0] == 6.0 and row[1] == 4           # canonical LAMAR, exact rung
    assert row[2] == 90.0 and row[3] == 4          # exact start rate, no pooled fallback
    assert row[4] == 40


def test_matchup_brackets_use_separate_normal_width_table_sql():
    """Bracket selection belongs in the source filter, never the output columns."""
    stats = next(stats for name, _, _, _, _, stats in TABLES if name == "research_matchup")
    sql = exact_matchup_sql("research_matchup_4po", "matchup", ["NFL_player_id", "year"],
                            stats, bracket="4po")
    assert "CREATE TABLE research_matchup_4po" in sql
    assert "r.bracket='4po'" in sql
    assert "start_rate_12t_flx_ppr_4pt_4po" not in sql
    assert "start_rate_12t_flx_ppr_4pt" in sql


def test_matchup_value_lanes_serve_canonical_supertable_values(bundle):
    con, _ = bundle
    # Matchup values must use the ruleset-scored supertable, not league-observed
    # aggregates.  The fixture's three weeks are 30, 11, and 25 points.
    row = con.execute("""SELECT ppg_12t_flx_ppr_4pt, points_12t_flx_ppr_4pt,
        lamar_12t_flx_ppr_4pt, active_weeks_12t_flx_ppr_4pt,
        inactive_weeks_12t_flx_ppr_4pt FROM research_matchup
        WHERE NFL_player_id='p1'""").fetchone()
    assert row == (22.0, 66.0, 6.0, 3.0, 14.0)
    # Weekly points and LAMAR are the exact supertable week values.
    row = con.execute("""SELECT ppg_12t_flx_ppr_4pt, points_12t_flx_ppr_4pt,
        lamar_12t_flx_ppr_4pt FROM research_matchup_weekly
        WHERE NFL_player_id='p1' AND week=3""").fetchone()
    assert row == (25.0, 25.0, 3.0)
    # Career values use the canonical season/supertable history, including 1989 in
    # this fixture: 76 total points over 4 games = 19 PPG.
    assert con.execute("""SELECT ppg_12t_flx_ppr_4pt, points_12t_flx_ppr_4pt,
        lamar_12t_flx_ppr_4pt FROM research_matchup_career""").fetchone() == (19.0, 76.0, 10.0)
    # draft season points = canonical season total; career = full-career total.
    assert con.execute("SELECT points_12t_flx_ppr_4pt FROM research_draft WHERE NFL_player_id='p1'").fetchone()[0] == 66.0
    assert con.execute("SELECT lamar_12t_flx_ppr_4pt FROM research_draft WHERE NFL_player_id='p1'").fetchone()[0] == 6.0
    assert con.execute("SELECT lamar_ppg_12t_flx_ppr_4pt FROM research_draft WHERE NFL_player_id='p1'").fetchone()[0] == 2.0
    assert con.execute("SELECT points_12t_flx_ppr_4pt FROM research_draft_career WHERE NFL_player_id='p1'").fetchone()[0] == 76.0
    assert con.execute("SELECT lamar_12t_flx_ppr_4pt FROM research_draft_career WHERE NFL_player_id='p1'").fetchone()[0] == 10.0


def test_draft_bundle_maps_rate_adjusted_cost_and_career_diagnostics():
    draft = next(stats for name, _, _, _, _, stats in TABLES if name == "research_draft")
    career = next(stats for name, _, _, _, _, stats in TABLES if name == "research_draft_career")
    matchup = next(stats for name, _, _, _, _, stats in TABLES if name == "research_matchup")

    assert draft["cost"] == ("cost_pct", "default")
    assert draft["lamar_ppg"] == ("avg_manager_lamar", "default")
    assert matchup["points"] == ("total_points_observed", "default")
    assert career["earliest_adp"] == ("earliest_adp", "adp")
    assert career["worst_draft_score"] == ("worst_draft_score", "dscore")
    assert career["career_auction_spend"] == ("career_auction_spend_pct", "default")


def test_draft_wide_table_keeps_one_row_for_every_player_season(bundle):
    con, _ = bundle
    row_count, unique_count = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT NFL_player_id || ':' || CAST(year AS VARCHAR))
        FROM research_draft
    """).fetchone()

    assert row_count == unique_count
    assert con.execute("""
        SELECT COUNT(*) FROM research_draft
        WHERE NFL_player_id = 'thin' AND year = 2002
    """).fetchone()[0] == 1
    assert con.execute("""
        SELECT adp_12t_flx_ppr_4pt, draft_rate_12t_flx_ppr_4pt,
               draft_score_12t_flx_ppr_4pt, draft_score_healthy_12t_flx_ppr_4pt
        FROM research_draft WHERE NFL_player_id = 'thin' AND year = 2002
    """).fetchone() == (190.0, 100.0, -5.0, -4.0)


def test_matchup_serves_exact_cohort_metrics_below_old_stability_threshold(bundle):
    con, _ = bundle
    row = con.execute("""SELECT win_rate_12t_flx_ppr_4pt,
        expected_wins_12t_flx_ppr_4pt, expected_losses_12t_flx_ppr_4pt,
        champ_wk_12t_flx_ppr_4pt, playoff_12t_flx_ppr_4pt,
        lamar_12t_flx_ppr_4pt, level_win_12t_flx_ppr_4pt
        FROM research_matchup WHERE NFL_player_id='p1'""").fetchone()
    # win_rate is now the CONDITIONAL rate (61.1): it must equal
    # expected_wins/(expected_wins+expected_losses) = 8.8/14.0 = 62.9 in shape.
    assert row == pytest.approx((61.1, 8.8, 5.2, 25.0, 72.0, 6.0, 4.0))


def test_matchup_serves_locked_denominator_lanes(bundle):
    """T6/T7/T8: the served Win % is wins in starts, champ/playoff ship as total-vs-as-starter
    pairs on the eligible-leagues denominator, and availability ships both sides."""
    con, _ = bundle
    season = con.execute("""SELECT win_rate_12t_flx_ppr_4pt, win_rate_cond_12t_flx_ppr_4pt,
        champ_total_12t_flx_ppr_4pt, champ_started_12t_flx_ppr_4pt,
        playoff_total_12t_flx_ppr_4pt, playoff_started_12t_flx_ppr_4pt,
        active_weeks_12t_flx_ppr_4pt, inactive_weeks_12t_flx_ppr_4pt
        FROM research_matchup WHERE NFL_player_id='p1'""").fetchone()
    assert season == pytest.approx((61.1, 61.1, 12.5, 7.5, 45.0, 30.0, 3.0, 14.0))
    # as-starter is always the smaller of each pair (it is a subset of the total)
    assert season[3] <= season[2] and season[5] <= season[4]

    career = con.execute("""SELECT win_rate_12t_flx_ppr_4pt,
        champ_total_12t_flx_ppr_4pt, champ_started_12t_flx_ppr_4pt,
        playoff_total_12t_flx_ppr_4pt, playoff_started_12t_flx_ppr_4pt,
        inactive_weeks_12t_flx_ppr_4pt
        FROM research_matchup_career WHERE NFL_player_id='p1'""").fetchone()
    # Career availability is canonical NFL-calendar availability, not a sum of the
    # league-source fixture's legacy inactive field.
    assert career == pytest.approx((61.0, 11.0, 6.0, 42.0, 28.0, 14.0))

    weekly = con.execute("""SELECT win_rate_12t_flx_ppr_4pt
        FROM research_matchup_weekly WHERE NFL_player_id='p1'""").fetchone()
    assert weekly == pytest.approx((66.7,))   # conditional at every grain


def test_career_serves_expected_champs_and_playoffs(bundle):
    con, _ = bundle
    row = con.execute("""SELECT expected_champs_12t_flx_ppr_4pt,
        expected_playoffs_12t_flx_ppr_4pt, playoff_12t_flx_ppr_4pt,
        lamar_12t_flx_ppr_4pt
        FROM research_matchup_career WHERE NFL_player_id='p1'""").fetchone()
    assert row == pytest.approx((0.45, 1.1, 68.0, 10.0))


def test_career_serves_percent_record(bundle):
    con, _ = bundle
    row = con.execute("""SELECT won_12t_flx_ppr_4pt, lost_12t_flx_ppr_4pt
        FROM research_matchup_career WHERE NFL_player_id='p1'""").fetchone()
    assert row == (53.8, 33.7)                     # expected components, never raw counts


def test_hollow_identities_not_emitted(bundle):
    con, _ = bundle
    n = con.execute(
        "SELECT COUNT(*) FROM research_matchup WHERE NFL_player_id='hollow'").fetchone()[0]
    assert n == 0


def test_api_columns_padded(bundle):
    con, _ = bundle
    cols = {r[1] for r in con.execute("PRAGMA table_info('research_matchup')").fetchall()}
    for slug in SLUGS:
        assert f"n_years_{slug}" in cols           # season grain pads career-only metrics
        assert f"level_champ_{slug}" in cols and f"n_champ_{slug}" in cols


def test_weekly_duplicates_class_lanes(bundle):
    con, _ = bundle
    row = con.execute("""SELECT level_12t_flx_ppr_4pt, level_srate_12t_flx_ppr_4pt,
        n_win_12t_flx_ppr_4pt, won_12t_flx_ppr_4pt FROM research_matchup_weekly
        WHERE NFL_player_id='p1' AND week=3""").fetchone()
    assert row == (4, 4, 40, 60.0)


def test_bio_fallback_names_never_played_players(bundle):
    """Ledger D7: a player with NO stat rows gets his identity from player_bio; stat-derived
    names still win for players who have stats."""
    con, _ = bundle
    stash = con.execute("SELECT player, \"position\" FROM research_player_names "
                        "WHERE NFL_player_id = 'stash'").fetchall()
    assert stash == [("Stashed Rookie", "QB")]
    p1 = con.execute("SELECT COUNT(*) FROM research_player_names "
                     "WHERE NFL_player_id = 'p1'").fetchone()[0]
    assert p1 == 1  # bio must NOT double-emit a player who has stat rows


def test_position_gate_is_year_scoped_and_career_deterministic(bundle):
    con, _ = bundle
    assert con.execute("""SELECT year FROM research_draft
        WHERE NFL_player_id='switch' ORDER BY year""").fetchall() == [(2023,)]
    assert con.execute("""SELECT COUNT(*) FROM research_draft_career
        WHERE NFL_player_id='switch'""").fetchone()[0] == 1
    assert con.execute("""SELECT "position" FROM research_player_names
        WHERE NFL_player_id='switch'""").fetchone()[0] == "DB"


def test_thresholds_are_the_12k_lake_derivation():
    assert MIN_STABLE["dscore"] == 643 and MIN_STABLE["win"] == 2645


# --- WEEKLY roster% gets its own floor (R23), and only weekly roster% -------------------
# The ladder and the floor were always here; the defect was that roster% shared srate's
# floor of 50, so a 60-league cohort was served its own number when R23 says that number
# carries about +/-11 points. These pin the fix AND its scope: start%/healthy start% and
# the season/career grains must not move until they are measured the same way.

def _stats_for(table_name):
    return next(stats for name, _, _, _, _, stats in TABLES if name == table_name)


def test_weekly_roster_rate_uses_the_measured_precision_floor():
    assert MIN_STABLE["rrate"] == 140                       # R23: +/-5 points @ 85%
    assert _stats_for("research_matchup_weekly")["roster_rate"] == ("roster_rate_pct", "rrate")


def test_season_roster_rate_uses_its_own_measured_floor():
    """75: measured 2021-2025 on 50%-rostered WRs in 300+ league cohorts, the peak of the
    required-N curve. Ja'Marr Chase needs a handful of leagues; the 50% guy needs 75; so 75
    satisfies everyone. (Was 90 while teams was mis-cut into 2 tiers instead of 4.)"""
    assert MIN_STABLE["rrates"] == 75
    assert _stats_for("research_matchup")["roster_rate"] == ("roster_rate_pct", "rrates")


def test_a_season_is_worth_a_couple_of_weeks_not_seventeen():
    """Within a league the weeks correlate at rho=0.37, so 17 weeks are 2.5 observations.
    If this ratio ever approaches 17 someone has assumed independent weeks again."""
    ratio = MIN_STABLE["rrate"] / MIN_STABLE["rrates"]
    assert 1.3 < ratio < 3.0, f"weekly/season floor ratio {ratio:.2f} implies the wrong rho"


def test_season_roster_rate_uses_its_own_measured_floor():
    """75, measured on 50% WRs in thick cohorts 2021-2025 -- the peak of the required-N curve.

    Was 90 until 2026-08-03: that run loaded a stale module which cut teams into 2 tiers
    instead of 4, so every cohort mixed two team sizes and carried variance from the cut
    rather than from the leagues. Splitting teams properly took the SD .317 -> .301.
    """
    assert MIN_STABLE["rrates"] == 75
    assert _stats_for("research_matchup")["roster_rate"] == ("roster_rate_pct", "rrates")


def test_the_season_floor_is_below_the_weekly_one_but_not_by_17x():
    """17 weeks correlate at rho=.37, so a season is worth 2.5 independent weeks, not 17."""
    assert MIN_STABLE["rrates"] < MIN_STABLE["rrate"]
    ratio = MIN_STABLE["rrate"] / MIN_STABLE["rrates"]
    assert 1.3 < ratio < 3.0, f"season/weekly ratio {ratio:.1f} implies the wrong rho"


def test_the_start_family_is_untouched_by_the_roster_floor():
    """Same class, same floor as before -- raising roster%'s floor must not drag these."""
    weekly = _stats_for("research_matchup_weekly")
    for stat in ("start_rate", "healthy_start_rate", "expected_starts"):
        assert weekly[stat][1] == "srate", f"{stat} moved off srate"
    assert MIN_STABLE["srate"] == 50


def test_career_roster_rate_still_waits_on_its_own_measurement():
    """Season is now measured (90); career is not, so it must not inherit either floor."""
    # Career alone: its required N has never been measured, so it stays on the old floor.
    assert _stats_for("research_matchup_career")["roster_rate"] == ("roster_rate_pct", "srate")


def test_a_cohort_between_the_two_floors_pools_roster_but_not_start():
    """60 leagues clears srate(50) and misses rrate(140) -- the discriminating case."""
    from build_wide_bundle import LANE_CLASSES, laddered_sql
    sql = laddered_sql("t", "research_matchup_weekly", ["NFL_player_id", "year", "week"],
                       _stats_for("research_matchup_weekly"))
    assert "('rrate',140)" in sql.replace(" ", "")     # the floor reaches the cls CTE
    assert "('srate',50)" in sql.replace(" ", "")
    # rung is picked per class, so the two stats resolve independently on the same row
    assert "PARTITION BY" in sql and "cl.cls" in sql
    assert "cls='rrate'THENroster_rate_pct" in sql.replace(" ", "")
    assert "cls='srate'THENstart_rate_pct" in sql.replace(" ", "")
    assert "rrate" in LANE_CLASSES                     # so pad_api_columns ships the lane
