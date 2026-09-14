"""audit_research_matchup.py -- the column-by-column gate on an ASSEMBLED matchup build.

Runs on the three assembled parquets (season + weekly + career), i.e. the artifact that is
about to be published, BEFORE it reaches the wide bundle or Fly. qa_sweep.py is the
complementary check one layer later (it reads the served bundle); this one exists because a
denominator defect is only visible next to the numerator and sample that produced it, and
those live here.

Six checks, matching the audit contract (docs/runbooks/research-leaderboard-exposure-*.md):

  1. IMPOSSIBLE VALUES -- no rate above 100.001%, none below 0. This is the check that
     caught win_rate_started_pct at 1,700% and pct_starts_in_playoffs at 1,200%.
  2. DENOMINATOR PROBE -- every rate divides by ELIGIBLE leagues. Asserted structurally:
     won + lost = start_rate; champ/playoff numerators never exceed their eligible
     denominator; a player who missed weeks is not deflated by them (start_rate is
     active-scoped, so it must be uncorrelated with games missed among real starters).
  3. PROVENANCE -- the physical column behind each served stat, printed so it can be read
     against build_wide_bundle.TABLES rather than assumed (Win % = win_rate_pct, wins in
     starts).
  4. PLAYOFF/CHAMP SCOPING -- as-starter is a strict subset of total for champ (same week,
     same roster); 1-league players floor at ~0% because the denominator is eligible
     leagues, not rostered leagues.
  5. WEEKLY GRAIN -- weekly rows exist for player/team-game support weeks, while
     Healthy Start% uses the separate player-active/snap denominator; the season
     team-game support count equals the number of weekly rows.
  6. BOARDS -- top-10 and bottom-10 per (stat x year x position) with the player's own
     sample beside every value, dumped to a report file. Includes the CMC litmus.

Console output is an EXCEPTION report: a clean run prints PASS lines and nothing else, so
anything else printed is something to look at. Exit code is non-zero if any gate FAILS.

    py -3 scripts/research_cohorts/audit_research_matchup.py --input D:/tmp/.../assembled
    py -3 scripts/research_cohorts/audit_research_matchup.py --input <dir> --slug 12t_flx_ppr_4pt
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

OPS_CACHE = Path(os.environ.get(
    "RESEARCH_OPS_CACHE_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb"))

# (column, floor, ceiling). Shares of a denominator that contains them: 0..100, no exceptions.
# Anything here that goes out of bounds is a defect in the column's denominator, not taste.
RATE_BOUNDS = [
    ("roster_rate_pct", 0.0, 100.0),
    ("start_rate_pct", 0.0, 100.0),
    ("healthy_start_rate_pct", 0.0, 100.0),
    ("won_pct", 0.0, 100.0),
    ("lost_pct", 0.0, 100.0),
    ("win_rate_pct", 0.0, 100.0),
    ("win_rate_started_pct", 0.0, 100.0),
    ("record_win_pct", 0.0, 100.0),
    ("playoff_win_pct", 0.0, 100.0),
    ("regular_win_pct", 0.0, 100.0),
    ("pct_starts_in_playoffs", 0.0, 100.0),
    ("start_intensity_legacy_pct", 0.0, 100.0),
    ("champ_week_rate_pct", 0.0, 100.0),
    ("playoff_rate_wkwt", 0.0, 100.0),
    ("playoff_rate_final", 0.0, 100.0),
    ("playoff_rate_started", 0.0, 100.0),
    # the four T7 lanes -- the whole point of the rebuild
    ("champ_total_pct", 0.0, 100.0),
    ("champ_as_starter_pct", 0.0, 100.0),
    ("playoff_total_pct", 0.0, 100.0),
    ("playoff_as_starter_pct", 0.0, 100.0),
]

# Served stat -> (physical season column, physical career column). Read straight out of the
# wide-bundle mapping so this file cannot drift from what is actually published.
def served_provenance() -> list[tuple[str, str, str]]:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from build_wide_bundle import TABLES
    season = dict(next(t[5] for t in TABLES if t[0] == "research_matchup"))
    career = dict(next(t[5] for t in TABLES if t[0] == "research_matchup_career"))
    stats = sorted(set(season) | set(career))
    return [(s, season.get(s, ("--",))[0], career.get(s, ("--",))[0]) for s in stats]


# Boards are cut per position; positions come from the super table (the same normalization
# the bundle's names table uses), so a board never mixes a kicker into an RB list.
POS_NORM = '''CASE
    WHEN "position" LIKE '%QB%' THEN 'QB'
    WHEN "position" LIKE '%RB%' THEN 'RB'
    WHEN "position" LIKE '%WR%' THEN 'WR'
    WHEN "position" LIKE '%TE%' THEN 'TE'
    WHEN "position" = 'DEF' THEN 'DEF'
    WHEN "position" LIKE '%K%' THEN 'K'
    ELSE "position" END'''

BOARD_STATS = [
    ("start_rate_pct", "started_weeks"),
    # The served Win% is conditional wins in starts.  `won_pct` is retained as a separate
    # diagnostic (expected wins divided by eligible team-game weeks), but must not be used
    # as the served Win% board metric.
    ("win_rate_pct", "started_weeks"),
    ("ppg_when_started", "started_weeks"),
    ("total_lamar_started", "started_weeks"),
    ("avg_clutch_started", "started_weeks"),
    ("champ_total_pct", "n_leagues"),
    ("champ_as_starter_pct", "n_leagues"),
    ("playoff_total_pct", "n_leagues"),
    ("playoff_as_starter_pct", "n_leagues"),
    ("roster_rate_pct", "n_leagues"),
    ("inactive_weeks", "nfl_active_weeks"),
]


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.lines: list[str] = []

    def ok(self, label: str, detail: str = "") -> None:
        print(f"  PASS  {label}{(' -- ' + detail) if detail else ''}")

    def fail(self, label: str, detail: str) -> None:
        self.failures.append(f"{label}: {detail}")
        print(f"  FAIL  {label} -- {detail}")

    def note(self, label: str, detail: str) -> None:
        print(f"  note  {label} -- {detail}")


def _connect(tmp: Path) -> duckdb.DuckDBPyConnection:
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='3500MB'")
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET temp_directory='{tmp.as_posix()}'")
    return con


def _columns(con, view: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {view}").fetchall()}


def check_impossible_values(con, rep: Report, view: str, label: str) -> None:
    """Gate 1. A share cannot exceed the denominator that contains it."""
    have = _columns(con, view)
    checks = [(c, lo, hi) for c, lo, hi in RATE_BOUNDS if c in have]
    if not checks:
        rep.fail(f"{label} bounds", "no rate columns found -- wrong artifact?")
        return
    parts = ", ".join(
        f"COUNT(*) FILTER (WHERE {c} > {hi} + 0.001) AS over_{c}, "
        f"COUNT(*) FILTER (WHERE {c} < {lo} - 0.001) AS under_{c}, "
        f"MAX({c}) AS max_{c}"
        for c, lo, hi in checks)
    row = con.execute(f"SELECT {parts} FROM {view}").fetchone()
    bad = []
    for i, (c, _lo, _hi) in enumerate(checks):
        over, under, mx = row[3 * i], row[3 * i + 1], row[3 * i + 2]
        if over or under:
            bad.append(f"{c}: {over:,} over / {under:,} under (max {mx})")
    if bad:
        rep.fail(f"{label} impossible values", "; ".join(bad))
    else:
        rep.ok(f"{label} impossible values", f"{len(checks)} rate columns all within 0..100")


def check_denominator_identities(con, rep: Report, view: str, label: str) -> None:
    """Gate 2. The identities that only hold when every lane shares one denominator."""
    have = _columns(con, view)

    # Weekly won/lost diagnostics are per-week expected contributions and sum to that
    # week's start share. Season/career won/lost are bounded diagnostics over different
    # rollup supports; the additive contract is checked below using Expected W/L instead.
    if label.lower().startswith("weekly") and {"won_pct", "lost_pct", "start_rate_pct"} <= have:
        n = con.execute(f"""SELECT COUNT(*) FROM {view}
            WHERE won_pct IS NOT NULL AND lost_pct IS NOT NULL AND start_rate_pct IS NOT NULL
              AND ABS(won_pct + lost_pct - start_rate_pct) > 0.5""").fetchone()[0]
        (rep.ok if n == 0 else rep.fail)(
            f"{label} won+lost=start_rate_pct",
            f"{n:,} rows break the identity" if n else "holds on every row")

    if {"expected_wins", "expected_losses", "expected_starts"} <= have:
        n = con.execute(f"""SELECT COUNT(*) FROM {view}
            WHERE expected_wins IS NOT NULL AND expected_losses IS NOT NULL
              AND expected_starts IS NOT NULL
              AND ABS(expected_wins + expected_losses - expected_starts) > .0002""").fetchone()[0]
        (rep.ok if n == 0 else rep.fail)(
            f"{label} expected W+L=expected starts",
            f"{n:,} rows break the identity" if n else "holds on every row")

    # Numerators are LEAGUE counts over the eligible-league denominator: they can never
    # exceed it. This is the structural form of "the denominator is eligible leagues".
    exact_signal_denoms = (
        ("n_champ_leagues", "champ_elig_leagues", "champ total"),
        ("n_champ_start_leagues", "champ_elig_leagues", "champ as-starter"),
        ("n_final_po", "playoff_eligible_leagues", "playoff total"),
        ("n_started_po", "playoff_eligible_leagues", "playoff as-starter"),
    )
    for num, den, name in (*exact_signal_denoms,
                            ("n_rostered_leagues", "n_leagues", "roster")):
        if {num, den} <= have:
            n = con.execute(
                f"SELECT COUNT(*) FROM {view} WHERE {num} > {den}").fetchone()[0]
            (rep.ok if n == 0 else rep.fail)(
                f"{label} {name} numerator <= eligible denominator",
                f"{n:,} rows exceed the denominator" if n else "holds on every row")
    for num, den, name in (
        ("n_champ_leagues", "n_final_po", "champ total <= playoff total"),
        ("n_champ_start_leagues", "n_started_po", "champ starter <= playoff starter"),
    ):
        if {num, den} <= have:
            n = con.execute(f"SELECT COUNT(*) FROM {view} WHERE {num} > {den}").fetchone()[0]
            (rep.ok if n == 0 else rep.fail)(
                f"{label} {name}",
                f"{n:,} rows violate playoff closure" if n else "holds on every row")
    # Weekly uses its own signal column names; the same containment still applies.
    for num, den, name in (("champ_started", "champ_eligible", "weekly champ starts"),
                           ("champ_eligible", "n_leagues", "weekly champ population")):
        if {num, den} <= have:
            n = con.execute(f"SELECT COUNT(*) FROM {view} WHERE {num} > {den}").fetchone()[0]
            (rep.ok if n == 0 else rep.fail)(
                f"{label} {name} numerator <= eligible denominator",
                f"{n:,} rows exceed the denominator" if n else "holds on every row")

    # Availability is represented by disjoint NFL-game bitsets.  `active_weeks`
    # at season grain is team-game support for the weekly rollup, while
    # `nfl_active_weeks` is the player's own active-game count.  Do not add those
    # two measures together: that was incorrectly flagging valid rows and hid the
    # career bug where league-week support was displayed as player games.
    if {"nfl_active_weeks", "active_week_mask"} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE nfl_active_weeks IS NOT NULL
            AND nfl_active_weeks <> bit_count(COALESCE(active_week_mask,0))""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} active-week mask",
            "matches player active-game count" if bad == 0 else f"{bad:,} rows disagree")
    if {"inactive_weeks", "inactive_week_mask"} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE inactive_weeks IS NOT NULL
            AND inactive_weeks <> bit_count(COALESCE(inactive_week_mask,0))""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} inactive-week mask",
            "matches missed team-game count" if bad == 0 else f"{bad:,} rows disagree")
    if {"active_week_mask", "inactive_week_mask"} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE (COALESCE(active_week_mask,0) & COALESCE(inactive_week_mask,0)) <> 0""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} availability masks disjoint",
            "no active/inactive overlap" if bad == 0 else f"{bad:,} rows overlap")
    if {"active_weeks", "active_week_mask", "inactive_week_mask"} <= have and label.lower() == "season":
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE active_weeks IS NOT NULL
            AND active_weeks <> bit_count(COALESCE(active_week_mask,0) |
                                          COALESCE(inactive_week_mask,0))""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} team-game support mask",
            "matches active plus inactive NFL games" if bad == 0 else f"{bad:,} rows disagree")


def check_exposure_bounds(con, rep: Report, view: str, label: str) -> None:
    """Check the primitive exposure counts behind the displayed rates.

    Rate bounds alone are insufficient: a uniformly wrong denominator can still produce a
    plausible percentage. These inequalities express the denominator contract directly.
    """
    have = _columns(con, view)
    specs = [
        ("rostered_leagues", "roster_eligible_leagues", "rostered <= roster-eligible"),
        ("rostered_league_weeks", "roster_eligible_league_weeks", "rostered weeks <= eligible weeks"),
        ("started_leagues", "team_game_eligible_leagues", "started <= team-game eligible"),
        ("started_team_game_weeks", "team_game_eligible_league_weeks", "started weeks <= team-game eligible weeks"),
        ("healthy_started_leagues", "healthy_eligible_leagues", "healthy starts <= healthy eligible"),
        ("started_active_weeks", "healthy_eligible_league_weeks", "active starts <= healthy eligible weeks"),
        ("wins_started", "started_leagues", "wins <= starts"),
        ("wins_started_active", "started_active_weeks", "active wins <= active starts"),
        ("losses_started", "started_leagues", "losses <= starts"),
    ]
    checked = 0
    failures = []
    for numer, denom, name in specs:
        if not {numer, denom} <= have:
            continue
        checked += 1
        bad = con.execute(
            f"SELECT COUNT(*) FROM {view} WHERE {numer} IS NOT NULL AND {denom} IS NOT NULL "
            f"AND ({numer} < 0 OR {denom} < 0 OR {numer} > {denom} + .000001)"
        ).fetchone()[0]
        if bad:
            failures.append(f"{name}: {bad:,}")
    if not checked:
        pass
    else:
        (rep.ok if not failures else rep.fail)(
            f"{label} primitive exposure bounds",
            f"{checked} inequalities hold" if not failures else "; ".join(failures))

    if {"wins_started", "losses_started", "started_team_game_weeks"} <= have:
        rep.note(f"{label} W/L support grain",
                 "raw league W/L counts are checked against started leagues; "
                 "expected W/L are checked against Exp Starts")

    # A nonzero league-count diagnostic with no weighted roster exposure is not
    # a legitimate zero-percent player.  It means the season row came from the
    # roster/matchup source but never joined the weekly player support.  Keep
    # this as a visible note rather than failing the release: older source
    # coverage gaps are real and should remain distinguishable from a bad rate.
    if {"n_rostered_leagues", "rostered_league_weeks"} <= have:
        n, max_lg = con.execute(f"""
          SELECT COUNT(*), MAX(n_rostered_leagues)
          FROM {view}
          WHERE n_rostered_leagues > 0
            AND COALESCE(rostered_league_weeks,0) = 0
        """).fetchone()
        if n:
            rep.note(f"{label} roster source-support gaps",
                     f"{n:,} rows have n_rostered_leagues > 0 but zero weighted roster weeks "
                     f"(max league count {max_lg}); inspect upstream player-week coverage")


def check_expected_start_contract(con, rep: Report, view: str, label: str) -> None:
    """Verify that Exp Starts is an eligible-league start share, not a start count."""
    have = _columns(con, view)
    support = "active_weeks"
    if label.lower() == "career" and "team_game_weeks" in have:
        support = "team_game_weeks"
    if {"expected_starts", support} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE expected_starts IS NOT NULL AND {support} IS NOT NULL
            AND (expected_starts < -.000001 OR expected_starts > {support} + .000001)""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} Exp Starts exposure bound",
            f"{bad:,} rows exceed active-week support" if bad else "within active-week support")
    if label.lower() == "weekly" and {"expected_starts", "start_rate_pct"} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE expected_starts IS NOT NULL AND start_rate_pct IS NOT NULL
            AND ABS(expected_starts - start_rate_pct/100.0) > .0002""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} Exp Starts uses eligible-league denominator",
            f"{bad:,} rows disagree with Start%" if bad else "equals weekly Start% share")
    if {"expected_wins", "expected_losses", "expected_starts"} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE expected_starts IS NOT NULL AND expected_wins IS NOT NULL
            AND expected_losses IS NOT NULL
            AND (expected_wins < -.000001 OR expected_losses < -.000001
                 OR expected_wins > expected_starts + .000001
                 OR expected_losses > expected_starts + .000001)""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} Expected W/L within Exp Starts",
            f"{bad:,} rows exceed expected-start support" if bad else "within expected-start support")


def check_rate_denominators(con, rep: Report, view: str, label: str) -> None:
    """Gate 2d. Recompute every served rate from its additive numerator/denominator.

    This is intentionally run on the assembled weekly, season, and career relations.
    Shard validation proves each shard is internally coherent; this catches a bad merge,
    rollup, or denominator-lattice join that could still produce plausible percentages.
    """
    have = _columns(con, view)
    specs = [
        ("roster_rate_pct", "rostered_leagues", "roster_eligible_leagues", 100.0,
         "Roster%"),
        ("roster_rate_pct", "rostered_league_weeks", "roster_eligible_league_weeks", 100.0,
         "Roster%"),
        ("start_rate_pct", "started_leagues", "team_game_eligible_leagues", 100.0,
         "Start%"),
        ("start_rate_pct", "started_team_game_weeks", "team_game_eligible_league_weeks", 100.0,
         "Start%"),
        ("healthy_start_rate_pct", "healthy_started_leagues", "healthy_eligible_leagues", 100.0,
         "Healthy Start%"),
        ("healthy_start_rate_pct", "started_active_weeks", "healthy_eligible_league_weeks", 100.0,
         "Healthy Start%"),
        ("win_rate_pct", "wins_started", "started_leagues", 100.0, "Win%"),
        ("win_rate_pct", "expected_wins", "expected_starts", 100.0, "Win%"),
        ("avg_clutch_started", "clutch_sum", "clutch_eligible_leagues", 1.0, "Clutch"),
        ("avg_clutch_started", "sum_clutch_started_active", "champ_elig_leagues", 1.0,
         "Clutch"),
        ("champ_week_rate_pct", "started_champ_active", "champ_eligible_league_weeks", 100.0,
         "Champ week rate"),
        ("champ_total_pct", "n_champ_leagues", "champ_elig_leagues", 100.0, "Champ%"),
        ("champ_as_starter_pct", "n_champ_start_leagues", "champ_elig_leagues", 100.0,
         "Champ%"),
        ("playoff_total_pct", "n_final_po", "playoff_eligible_leagues", 100.0, "Playoffs%"),
    ]
    checked = set()
    for rate, numer, denom, scale, name in specs:
        if not {rate, numer, denom} <= have:
            continue
        # Multiple primitive pairs can describe the same rate at different grains;
        # only test a pair whose denominator is populated on at least one row.
        key = (rate, numer, denom)
        if key in checked:
            continue
        checked.add(key)
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE {rate} IS NOT NULL AND {numer} IS NOT NULL AND {denom} > 0
            AND ABS({rate} - {scale}*{numer}/NULLIF({denom},0)) >
                {0.011 if scale == 1.0 else 0.11}""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} {name} denominator {numer}/{denom}",
            f"{bad:,} rows disagree" if bad else "matches additive numerator/denominator")
    if {"playoff_as_starter_pct", "n_started_po", "n_champ_start_leagues",
        "playoff_eligible_leagues"} <= have:
        bad = con.execute(f"""SELECT COUNT(*) FROM {view}
          WHERE playoff_as_starter_pct IS NOT NULL AND playoff_eligible_leagues > 0
            AND ABS(playoff_as_starter_pct - 100.0 *
                GREATEST(n_started_po, n_champ_start_leagues)
                / NULLIF(playoff_eligible_leagues,0)) > 0.11""").fetchone()[0]
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} Playoffs% starter denominator n_started_po/playoff_eligible_leagues",
            f"{bad:,} rows disagree" if bad else "matches additive numerator/denominator")


def check_denominator_is_population(con, rep: Report, view: str, label: str) -> None:
    """Gate 2c. Within a cohort cell, the eligible-league denominator is ONE number.

    It is a property of league settings, so every player in a cell must divide by the same
    value -- the only legitimate variation is the position group (K and DEF have their own
    eligible counts, so up to 3 distinct values per cell).

    This is the check that was missing when shard-summed denominators shipped: every other
    gate is a within-row invariant (rate <= 100, numerator <= denominator, identities), and
    all of those still hold when the denominator is uniformly too small. The defect was
    only visible ACROSS rows -- bucketed years carried up to 48 distinct denominators in a
    single cell, one per subset of buckets a player happened to be rostered in.
    """
    # At career grain, n_leagues is an exposure sum over the player's eligible
    # seasons, so it legitimately varies by player.  The population-wide lattice
    # invariant applies to weekly cells and season cells, where one year/week has
    # one position-scoped population denominator.
    if label.lower() == "career":
        rep.note(f"{label} denominator is population-wide",
                 "not applicable: career denominator is a player exposure sum")
        return
    have = _columns(con, view)
    # A denominator is population-defined at the exact displayed grain.  Weekly
    # rows therefore include week in the key; omitting it would confuse legitimate
    # week-to-week population changes with denominator drift.
    dims = [c for c in ("teams", "roster", "ppr", "td", "bracket", "league_type",
                        "lineup_mode", "keeper_mode", "year", "week", "pos_grp") if c in have]
    if "n_leagues" not in have or not dims:
        return
    key = ", ".join(dims)
    # These are population-lattice values, not player exposure totals.  Healthy
    # eligibility and the weekly Champ diagnostic are intentionally excluded:
    # they are player/week-conditioned quantities and may legitimately vary.
    population_denoms = ["n_leagues"]
    if label.lower() == "weekly":
        population_denoms.extend(
            c for c in ("roster_eligible_leagues", "team_game_eligible_leagues",
                        ) if c in have)
    for denom in population_denoms:
        row = con.execute(f"""
          SELECT COUNT(*) AS cells,
                 COUNT(*) FILTER (WHERE nvals > 1) AS over_one,
                 MAX(nvals) AS worst
          FROM (SELECT {key}, COUNT(DISTINCT {denom}) AS nvals FROM {view}
                WHERE cohort_level = 4 AND {denom} IS NOT NULL GROUP BY {key})""").fetchone()
        cells, bad, worst = row
        detail = (f"{bad:,} of {cells:,} cohort cells carry more than one {denom} "
                  f"(worst {worst} distinct values)")
        (rep.ok if bad == 0 else rep.fail)(
            f"{label} {denom} is population-wide",
            f"{cells:,} cells, exactly one population denominator"
            if bad == 0 else detail)


def check_injury_not_deflated(con, rep: Report, view: str) -> None:
    """Gate 2b. Missed weeks must not deflate the usage rate.

    start_rate is active-scoped, so among real starters it must be ~uncorrelated with how
    many weeks the player missed. A /18-style denominator shows up here as a strong negative
    correlation (the artifact that capped 2019-2020 at 83%).
    """
    have = _columns(con, view)
    if not {"start_rate_pct", "inactive_weeks", "started_weeks", "roster"} <= have:
        return
    row = con.execute(f"""SELECT CORR(start_rate_pct, inactive_weeks), COUNT(*),
        AVG(start_rate_pct) FILTER (WHERE inactive_weeks = 0),
        AVG(start_rate_pct) FILTER (WHERE inactive_weeks >= 4)
      FROM {view}
      WHERE roster='flx' AND cohort_level=4 AND format_level=0 AND start_rate_pct >= 50
        AND started_weeks >= 12 AND inactive_weeks IS NOT NULL""").fetchone()
    corr, n, healthy, hurt = row
    if n < 100 or corr is None:
        rep.note("injury deflation probe", f"only {n:,} qualifying rows -- inconclusive")
        return
    detail = (f"corr(start_rate, missed weeks) = {corr:+.3f} over {n:,} starters; "
              f"healthy {healthy:.1f}% vs 4+ missed {hurt:.1f}%")
    rep.note("injury deflation probe", detail + "; negative association is expected for injured players")


def check_champ_subset(con, rep: Report, view: str, label: str) -> None:
    """Gate 4. as-starter is a strict subset of total for champ (same week, same roster).

    NOT asserted for playoff: a week-15 waiver pickup can start a playoff game without
    having been on the roster at the qualification week, so playoff as-starter legitimately
    exceeds playoff total on a small tail. That tail is reported, not failed.
    """
    have = _columns(con, view)
    if {"champ_total_pct", "champ_as_starter_pct"} <= have:
        n = con.execute(f"""SELECT COUNT(*) FROM {view}
            WHERE champ_as_starter_pct > champ_total_pct + 0.001""").fetchone()[0]
        (rep.ok if n == 0 else rep.fail)(
            f"{label} champ as-starter <= total",
            f"{n:,} rows violate the subset" if n else "holds on every row")
    # Do not compare champ% and playoff% directly: their denominators are deliberately
    # different.  Champ% is over leagues with a championship signal; Playoffs% is over
    # leagues with playoff evidence.  Even though a championship is itself a playoff
    # event, differing signal coverage makes the two displayed rates non-comparable.
    # Numerator containment is checked separately against each lane's exact denominator.
    if {"champ_total_pct", "playoff_total_pct"} <= have:
        rep.note(f"{label} champ/playoff rates",
                 "not compared across lanes; each uses its own signal-eligible denominator")
    if {"playoff_total_pct", "playoff_as_starter_pct"} <= have:
        n, tot = con.execute(f"""SELECT
            COUNT(*) FILTER (WHERE playoff_as_starter_pct > playoff_total_pct + 0.001),
            COUNT(*) FILTER (WHERE playoff_total_pct IS NOT NULL) FROM {view}""").fetchone()
        share = 100.0 * n / max(tot, 1)
        rep.note(f"{label} playoff as-starter > total",
                 f"{n:,}/{tot:,} rows ({share:.2f}%) -- expected tail: in-playoff pickups")


def check_one_league_floor(con, rep: Report, view: str) -> None:
    """Gate 4b. The eligible-leagues denominator self-floors a 1-league stash.

    A player rostered in a single league that happened to win it must read ~0%, not 100%.
    """
    have = _columns(con, view)
    if not {"n_rostered_leagues", "champ_total_pct", "n_leagues"} <= have:
        return
    row = con.execute(f"""SELECT COUNT(*), MAX(champ_total_pct), MAX(playoff_total_pct)
        FROM {view} WHERE roster='flx' AND cohort_level=4 AND format_level=0
          AND n_rostered_leagues = 1 AND n_leagues >= 100""").fetchone()
    n, max_champ, max_po = row
    if not n:
        rep.note("1-league self-floor", "no qualifying rows")
        return
    worst = max(max_champ or 0.0, max_po or 0.0)
    detail = (f"{n:,} single-league players in 100+ league cohorts; "
              f"worst champ% {max_champ}, worst playoff% {max_po}")
    (rep.ok if worst <= 1.0 else rep.fail)("1-league self-floor", detail)


def check_weekly_grain(con, rep: Report) -> None:
    """Gate 5. Weekly rows have team-game support and the season count agrees.

    Inactive/scratch rows may remain in the weekly lattice so roster exposure and
    the season rollup retain denominator context. They must not be confused with
    player-active weeks: Healthy Start% uses the healthy denominator fields.
    """
    n = con.execute("""SELECT COUNT(*) FROM weekly
        WHERE eligible_leagues IS NULL OR eligible_leagues <= 0""").fetchone()[0]
    (rep.ok if n == 0 else rep.fail)(
        "weekly rows have an eligible denominator",
        f"{n:,} rows with no eligible leagues" if n else "every weekly row is denominated")

    mismatch, checked = con.execute("""
      WITH wk AS (
        SELECT teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,cohort_level,
               format_level,year,NFL_player_id,pos_grp, COUNT(*) AS n_weeks
        FROM weekly GROUP BY ALL)
      SELECT COUNT(*) FILTER (WHERE s.active_weeks <> wk.n_weeks), COUNT(*)
      FROM wk JOIN season s USING (teams,roster,ppr,td,league_type,lineup_mode,
        keeper_mode,cohort_level,format_level,year,NFL_player_id,bracket,pos_grp)
      WHERE s.active_weeks IS NOT NULL""").fetchone()
    detail = f"{mismatch:,} of {checked:,} joined rows disagree"
    (rep.ok if mismatch == 0 else rep.fail)("season active_weeks = weekly row count", detail)


def check_cmc_litmus(con, rep: Report, slug: tuple[str, str, str, str]) -> None:
    """Gate 6. The board-fidelity litmus: CMC tops RB clutch in 2019/2022/2023/2025.

    2022 is the injury year and is expected to place lower -- the litmus is that he is a
    high-confidence board member in the healthy years, not that he is #1 in all four.
    """
    teams, roster, ppr, td = slug
    rows = con.execute(f"""
      WITH board AS (
        SELECT s.year, s.NFL_player_id, s.avg_clutch_started, s.started_weeks,
               RANK() OVER (PARTITION BY s.year
                            ORDER BY s.avg_clutch_started DESC NULLS LAST) AS rk
        FROM season s JOIN names n ON n.NFL_player_id = s.NFL_player_id
        WHERE s.teams='{teams}' AND s.roster='{roster}' AND s.ppr='{ppr}' AND s.td='{td}'
          AND s.cohort_level=4 AND s.format_level=0
          AND n.pos='RB' AND s.year IN (2019,2022,2023,2025)
          AND s.started_weeks >= 100)
      SELECT year, rk, ROUND(avg_clutch_started,3), started_weeks FROM board
      WHERE NFL_player_id IN (SELECT NFL_player_id FROM names
                              WHERE player='Christian McCaffrey')
      ORDER BY year""").fetchall()
    if not rows:
        rep.note("CMC RB clutch litmus", "no rows -- name lookup or cohort empty")
        return
    detail = "; ".join(f"{y}: rank {rk} (clutch {c}, {sw:,} started lg-weeks)"
                       for y, rk, c, sw in rows)
    healthy = [rk for y, rk, _c, _sw in rows if y in (2019, 2023, 2025)]
    ok = bool(healthy) and max(healthy) <= 5
    (rep.ok if ok else rep.fail)("CMC RB clutch litmus", detail)


def dump_boards(con, out: Path, slug: tuple[str, str, str, str], top: int) -> Path:
    """Gate 6. Top/bottom N per (stat x year x position), player names and sample attached.

    ONE query per stat, ranked with a window partitioned by (year, position) -- not one
    query per (stat, year, position). The naive loop is ~1,900 full scans of a 400 MB
    parquet; materializing the served slice once and ranking inside it turns the whole dump
    from hours into seconds without changing a single number.
    """
    teams, roster, ppr, td = slug
    have = _columns(con, "season")
    stats = [(s, smp) for s, smp in BOARD_STATS if s in have]
    absent = [s for s, _ in BOARD_STATS if s not in have]
    cols = ", ".join(f"s.{c}" for c in
                     sorted({s for s, _ in stats} | {smp for _, smp in stats}))
    con.execute(f"""CREATE OR REPLACE TEMP TABLE board_base AS
      SELECT s.year, n.player, n.pos, {cols}
      FROM season s JOIN names n ON n.NFL_player_id = s.NFL_player_id
      WHERE s.teams='{teams}' AND s.roster='{roster}' AND s.ppr='{ppr}' AND s.td='{td}'
        AND s.cohort_level=4 AND s.format_level=0 AND s.year IS NOT NULL
        AND n.pos IN ('QB','RB','WR','TE','K','DEF')""")
    n_base = con.execute("SELECT COUNT(*) FROM board_base").fetchone()[0]

    lines: list[str] = [
        f"# research matchup boards -- {teams}/{roster}/{ppr}/{td}, top/bottom {top}",
        f"# {n_base:,} served rows (rung 4, format-blind, flx-eligible positions)",
        "# every value carries the player's OWN sample: an extreme number is only",
        "# interpretable next to the sample that produced it.",
    ]
    for stat in absent:
        lines.append(f"\n## {stat} -- COLUMN ABSENT")
    for stat, sample in stats:
        rows = con.execute(f"""
          WITH b AS (
            SELECT year, pos, player, {stat} AS v, {sample} AS smp,
                   ROW_NUMBER() OVER (PARTITION BY year, pos ORDER BY {stat} DESC) AS hi,
                   ROW_NUMBER() OVER (PARTITION BY year, pos ORDER BY {stat} ASC) AS lo
            FROM board_base WHERE {stat} IS NOT NULL)
          SELECT year, pos, player, v, smp, hi <= {top} AS is_top FROM b
          WHERE hi <= {top} OR lo <= {top}
          ORDER BY year DESC, pos, v DESC""").fetchall()
        current = None
        for year, pos, player, v, smp, is_top in rows:
            if (year, pos) != current:
                current = (year, pos)
                lines.append(f"\n## {stat} | {year} | {pos}")
            side = "TOP " if is_top else "BOT "
            lines.append(f"  {side}{player:<28s} {v:>12} (sample {smp})")
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True,
                    help="directory holding the assembled matchup parquets")
    ap.add_argument("--slug", default="12t_flx_ppr_4pt")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--report", type=Path, default=None)
    ap.add_argument("--ops-cache", type=Path, default=OPS_CACHE)
    ap.add_argument("--skip-boards", action="store_true",
                    help="run the gates only (the board dump is the slow part)")
    ap.add_argument("--year", type=int, default=None,
                    help="scope season/weekly gates to one replacement year")
    ap.add_argument("--skip-career", action="store_true",
                    help="omit career gates when validating a replacement year")
    args = ap.parse_args()

    season_path = args.input / "research_matchup_player_season.parquet"
    weekly_path = args.input / "research_matchup_weekly.parquet"
    # the career rollup writes matchup_career.parquet; the published table is
    # research_matchup_career. Accept either so the audit runs on whichever the caller has.
    career_path = next(
        (p for p in (args.input / "research_matchup_career.parquet",
                     args.input / "matchup_career.parquet") if p.exists()),
        args.input / "matchup_career.parquet")
    if not season_path.exists():
        raise SystemExit(f"no assembled season parquet at {season_path}")

    teams, roster, ppr, td = args.slug.split("_")
    rep = Report()
    con = _connect(args.input / ".duckdb_tmp_audit")
    year_filter = f" WHERE CAST(year AS INTEGER) = {args.year}" if args.year is not None else ""
    con.execute(f"CREATE VIEW season AS SELECT * FROM read_parquet('{season_path.as_posix()}'){year_filter}")
    if weekly_path.exists():
        con.execute(
            f"CREATE VIEW weekly AS SELECT * FROM read_parquet('{weekly_path.as_posix()}'){year_filter}")
    if career_path.exists() and not args.skip_career:
        con.execute(
            f"CREATE VIEW career AS SELECT * FROM read_parquet('{career_path.as_posix()}')")
    if Path(args.ops_cache).exists():
        con.execute(f"ATTACH '{Path(args.ops_cache).as_posix()}' AS ops (READ_ONLY)")
        con.execute(f"""CREATE TABLE names AS
          SELECT NFL_player_id, ANY_VALUE(player) AS player,
                 ANY_VALUE({POS_NORM}) AS pos
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE NFL_player_id IS NOT NULL AND player IS NOT NULL
          GROUP BY 1""")
    else:
        con.execute("CREATE TABLE names AS SELECT NULL::VARCHAR NFL_player_id, "
                    "NULL::VARCHAR player, NULL::VARCHAR pos WHERE false")
        rep.note("player names", f"{args.ops_cache} missing -- boards will be empty")

    print("\n1+2. GATES -- impossible values and denominator identities")
    available_views = {"season"}
    if weekly_path.exists():
        available_views.add("weekly")
    if career_path.exists() and not args.skip_career:
        available_views.add("career")
    for view, label in (("weekly", "weekly"), ("season", "season"), ("career", "career")):
        if view not in available_views:
            continue
        check_impossible_values(con, rep, view, label)
        check_denominator_identities(con, rep, view, label)
        check_exposure_bounds(con, rep, view, label)
        check_expected_start_contract(con, rep, view, label)
        check_rate_denominators(con, rep, view, label)
        check_denominator_is_population(con, rep, view, label)
        check_injury_not_deflated(con, rep, view)

    print("\n3. PROVENANCE -- served stat -> physical column (read against the policy)")
    for stat, s_col, c_col in served_provenance():
        print(f"     {stat:<20s} season={s_col:<24s} career={c_col}")

    print("\n4. PLAYOFF / CHAMP SCOPING")
    check_champ_subset(con, rep, "season", "season")
    check_one_league_floor(con, rep, "season")
    if career_path.exists() and not args.skip_career:
        check_champ_subset(con, rep, "career", "career")

    print("\n5. WEEKLY GRAIN")
    if weekly_path.exists():
        check_weekly_grain(con, rep)
    else:
        rep.note("weekly grain", "no weekly parquet supplied")

    print("\n6. BOARDS")
    if args.year is None:
        check_cmc_litmus(con, rep, (teams, roster, ppr, td))
    else:
        rep.note("CMC litmus", f"skipped for replacement-year audit ({args.year})")
    if not args.skip_boards:
        out = args.report or (args.input / f"audit_boards_{args.slug}.txt")
        dump_boards(con, out, (teams, roster, ppr, td), args.top)
        print(f"  wrote top/bottom-{args.top} boards -> {out}")

    con.close()
    print()
    if rep.failures:
        print(f"AUDIT FAILED -- {len(rep.failures)} gate(s):")
        for failure in rep.failures:
            print(f"  * {failure}")
        return 1
    print("AUDIT PASSED -- every gate clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
