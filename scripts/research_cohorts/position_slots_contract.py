"""position_slots_contract.py -- the `teams` axis, redefined as STARTING SLOTS AT A POSITION.

    THE AXIS IS TOTAL LEAGUE-WIDE STARTING SLOTS AT THE POSITION, not team count.

Team count is a *factor* of that axis, not a dimension of its own. A 12-team league that
starts 2 RB + 2 FLEX offers a different RB market than a 12-team league that starts 2 RB and
no flex, and the existing `num_teams <= 11` cut cannot see the difference. This module turns
each position's slot count into the SAME '10t'/'12t' alphabet the cohort key already uses, so
'12t' comes to mean "a 12-team-equivalent market FOR THIS POSITION".

    eff_slots(pos) = num_teams * (roster_pos + FLEX_FILL[lane][scoring][pos] * contested)
    team_equiv     = eff_slots(pos) / DIVISOR[pos][lane]
    teams          = '10t' if team_equiv <= 11 else '12t'          # today's cut, unchanged

CONSEQUENCE, stated plainly because it is the whole point: `teams` becomes a function of
(league-year, POSITION). An RB row and a WR row from the SAME league can land in different
`teams` buckets -- measured, 23,553 of 63,448 league-years (37%) split this way. The cohort
machinery already supports it: `pos_grp` is in every denominator grouping set and the `elig`
UNION ALL already fans one league-year into one row per pos_grp.

WHY THIS DIVISOR. It is the median slots-per-team for the position, which makes team_equiv
reduce to num_teams at the modal roster -- so the typical league keeps the bucket it is
already published under. Crucially QB/flx comes out at EXACTLY 1.0000, so this construction
reproduces the locked `rank_slots_contract.slots_expr("QB")` formula rather than competing
with it. An absolute slot threshold would not have that property.

FROZEN, NOT COMPUTED. These constants are measurements with a date, embedded here on purpose.
If the builder re-derived them at build time, the meaning of `12t` would drift every time the
corpus grew and a column published yesterday would silently change definition. Re-derive with
scratchpad/derive_slot_constants.py and bump MEASURED_ON deliberately.
"""
from __future__ import annotations

MEASURED_ON = "2026-07-30"
MEASURED_YEARS = (2020, 2025)
MEASURED_FILTERS = ("managed only (sleeper_best_ball=false), num_teams 8-14, "
                    "roster_QB/RB/WR > 0, roster_FLX + roster_SUPER_FLEX > 0")

# Positions whose slot count is modelled. K and DEF are NOT: roster_K is populated on 34.6%
# of league-years and roster_DEF on 38.2%, so a K/DEF slot axis would rest on a third of the
# corpus. They keep the league-level `teams` bucket and the existing pooled denominator.
# (roster_RB 97.6% / roster_WR 97.1% / roster_QB 96.6% / roster_TE 94.6% / roster_FLX 95.4%.)
# THE DECLARED-SLOT PATH IS GONE (Joe, 2026-08-03). There is now ONE way to compute a team
# number: OBSERVED CAPACITY. The old path read `roster_*` columns and did arithmetic, and it
# failed in three separate ways -- league_settings understates real rosters by 3-5 spots, its
# bench column swings 20 points between adjacent sizes, and it only ever modelled QB/RB/WR/TE
# so the other five positions had no formula at all. Keeping it alive meant two systems that
# could silently disagree. SLOT_MODELLED, teams_bucket_sql, team_equiv_sql, eff_slots_sql,
# _divisor_case and settings_teams_select are all deleted rather than deprecated.

# TIER_POSITIONS is the v4 observed-capacity set -- all nine. Crossed with both stats and the
# four tiers that is 9 x 2 x 4 = 72 rules emitted by the builder. Kept separate from
# SLOT_MODELLED so widening the tier coverage cannot break the legacy declared-slot path.
TIER_POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB")
TIER_STATS = ("rostered", "started")

# ============================ TIER CONTRACT VERSION ============================
# THIS IS AN INVISIBLE CHANGE. A caller that misses it does not crash -- it silently emits the
# OLD two-level '10t'/'12t' alphabet from DECLARED roster slots, and every number downstream is
# quietly wrong for no visible reason. So the change is made FAIL-CLOSED: `teams_bucket_sql`
# and `settings_teams_select` now REQUIRE `contract` to be passed explicitly. Any call site not
# updated raises TypeError at call time instead of returning a plausible wrong answer.
#
# v2 (2026-08-02) replaces declared roster slots with OBSERVED CAPACITY (R22):
#     wr_spots(league) = AVG over weeks of the count of position rows in that league-week
# because league_settings roster columns proved unreliable -- they understate real rosters by
# 3-5 slots and their bench column swings 20 points between adjacent sizes.
# Four buckets, not two: 08tm / 10tm / 12tm / 14tm, cut at the pooled 2021-2025 quantile match
# to the num_teams shape.
TIER_CONTRACT_VERSION = "v2-observed-capacity-2026-08-02"
TIER_LABELS = ("08tm", "10tm", "12tm", "14tm")

# EVERY POSITION AND EVERY STAT IS HANDLED THE SAME WAY AS WR (Joe, 2026-08-02). One method,
# no per-position improvisation: observed capacity, then cutoffs quantile-matched to the
# literal num_teams distribution so the population SHAPE stays recognisable while MEMBERSHIP
# differs on real capacity (R21 -- that beat equal-spacing by 22.7% on within-tier spread).
#
# ROSTERED and STARTED are different tiers because they answer different questions (R18): a
# roster% cell is denominated on total capacity, a start% cell on starting capacity alone. The
# SAME league therefore lands in different tiers for the two stats, which is correct.
#
# Derived from 2023-2025 pooled. WR rostered came out 54.4/75.2/135.8 here against the
# 54.2/79.4/134.5 derived independently from 2021-2025 -- different years, same answer.
TIER_CUTS = {
    # ==================================================================================
    # v4 (2026-08-03). QUANTILE-MATCHED to the real league-size distribution AND with the
    # mass-point boundary fixed. Earlier versions got one or the other:
    #   v2  right shape, but each cut landed exactly ON a mass point, so a 10-team league
    #       starting exactly 10 QBs fell above a 10.0 cut and read 12tm.
    #   v3  right boundary (midpoints 9r/11r/13r) but abandoned shape matching, so 14tm
    #       swelled to 44-62% of the corpus.
    #   v4  quantile-match, then nudge a cut past any value where >2% of the lane sits
    #       exactly. K started IS num_teams, so 8/10/12 are dense mass points; WR rostered
    #       is continuous and needs no nudge.
    # ACHIEVED SHAPE ERROR 0-3 POINTS on all 20 pairs -- the tiers now carry the same
    # proportions as the real population, which is what R21 established for WR.
    # ==================================================================================
    ("QB", "rostered", "flx"):  (15.5, 19.5, 31.1),
    ("QB", "started",  "flx"):  (8.5, 10.5, 12.5),
    ("QB", "rostered", "sflx"): (23.5, 34.2, 44.0),
    ("QB", "started",  "sflx"): (15.1, 19.3, 22.6),
    ("RB", "rostered"): (40.2, 51.9, 67.9),   ("RB", "started"): (21.4, 26.9, 33.2),
    ("WR", "rostered"): (48.2, 63.7, 86.2),   ("WR", "started"): (25.4, 32.6, 46.7),
    ("TE", "rostered"): (14.5, 18.9, 28.7),   ("TE", "started"): (8.4, 11.2, 14.8),
    ("K",  "rostered"): (9.9, 12.2, 15.8),    ("K",  "started"): (8.5, 10.5, 12.5),
    ("DEF", "rostered"): (10.5, 13.6, 19.6),  ("DEF", "started"): (8.5, 10.5, 12.5),
    ("DL", "rostered"): (4.0, 13.8, 28.6),    ("DL", "started"): (1.8, 7.4, 17.3),
    ("LB", "rostered"): (15.3, 34.0, 67.6),   ("LB", "started"): (9.1, 22.2, 41.0),
    ("DB", "rostered"): (4.9, 20.4, 48.5),    ("DB", "started"): (1.7, 12.2, 28.3),
}

# THE LANDING TEST BINDS ONLY WHERE STARTING SLOTS ARE FIXED. QB-non-superflex, K and DST
# start exactly one per team, so a literal N-team league MUST land in tier N -- they do, at
# 90/97/98%. RB/WR/TE are FLEX-ELIGIBLE: the manager chooses who fills the flex, so starting
# counts vary inside a team count and a lower landing rate is the flex working, not a
# misplaced cut. For ROSTERED the test never binds -- a deep 10-team league genuinely carries
# 12-team rostering demand, and demanding high landing would demand the tier reproduce
# num_teams, which is the thing it exists to improve on.
FIXED_SLOT_POSITIONS = {"QB", "K", "DEF"}     # QB only in the non-superflex lane

# IDP FLEX OCCUPANCY -- the defensive analogue of R17's offensive flex, measured the same way
# (starts beyond the dedicated slots), 2024 managed redraft IDP leagues. Self-validating:
# inferred occupants 115,208 against 120,224 actual flex slots, ratio 0.96, where R17's WR
# check was 0.98.
#
#   DL 6.4%   LB 76.9%   DB 16.6%
#
# LB dominates the IDP flex the way WR dominates the offensive flex in PPR (60%), which is why
# LB overshot its derived started by +1.10 before the combo slots were counted -- linebackers
# were filling flex slots credited to dedicated ones.
#
# Median IDP league: 0 DL, 1 LB, 2 DB dedicated plus 1 catch-all. Only 356 of 569 IDP leagues
# carry a catch-all at all.
IDP_FLEX_SHARE = {"DL": 0.064, "LB": 0.769, "DB": 0.166}

# ACCEPTED AS-IS (Joe, 2026-08-03): IDP will never be a large enough population to justify
# further splitting. DL keeps its 36/39% drift and LB/DB started keep their 18/23% landing.
# Those are recorded in PROVISIONAL_REASON and are not going to be chased further.
IDP_ACCEPTED_AS_IS = True

# TIERS ARE DERIVED ON MANAGED REDRAFT AND THAT IS FINAL (Joe, 2026-08-03).
# Format -- redraft / dynasty / best ball -- is ALREADY its own cohort axis, so a tier must
# never mix formats. Redraft is the reference population; dynasty and best ball are separated
# by the format dimension and additionally fall back to redraft when too thin to stand alone
# (the asymmetric fallback: they borrow from redraft, redraft never borrows from them).
#
# So the earlier observation that 14tm holds 31-47% of the CORPUS is not a defect to fix. It
# is dynasty and best ball genuinely carrying more capacity, measured across a boundary the
# cohort key already enforces. Inside the managed-redraft lane the cuts were derived on, the
# achieved shape error is 0-3 points.
#
# FORMAT MOVES ROSTERED, NOT STARTED. Measured 2024, 1QB non-IDP, per team:
#   QB  started 0.99 / 0.99 / 0.99 across redraft/dynasty/bestball -- IDENTICAL
#       rostered 1.83 / 3.38 / 2.75 -- a 58% spread
#   RB  started spread 10%, rostered 45%
#   WR  started spread 14%, rostered 55%
#   TE  started spread 30%, rostered 61%
# You start one quarterback whatever the format. The started spread that does exist is almost
# entirely BEST BALL, which auto-starts the optimal lineup and so lands on a second TE more
# often than a human would -- and best ball is a separate dimension, not a tier input.
# Dynasty started sits essentially on redraft, confirming A2: dynasty is a ROSTERING format,
# not a starting one.
TIERS_DERIVED_ON = "managed redraft"

# ============================ LOCKED MECHANISM CONSTANTS ============================
# The tier follows from the LINEUP RULES, not from a distribution refitted each year (Joe,
# 2026-08-03). Lineup rules do not change year to year, so a constant derived from them does
# not drift -- and a rising observed rate then shows up as LEAGUES MOVING UP A TIER, which is
# the tier working, instead of the ladder itself moving, which hides the trend.
#
# Measured 2024, clean lane (managed redraft, no superflex, no IDP; IDP positions gated to
# leagues that field them). derived_started = dedicated + flex x flex_share;
# derived_rostered = started + bench x bench_share.
#
# TWO CORRECTIONS THAT MATTERED:
#  - flex_share is measured IN THIS LANE, never borrowed. R17's pooled RB share of 0.33 came
#    from a population including superflex and IDP; in the clean lane RB wins 0.25, and the
#    pooled value put RB derived-started 0.14 high.
#  - the FLEX COLUMN differs by position. Offensive positions compete for roster_FLX; IDP
#    positions compete for roster_IDP plus the DB_LB/DL_LB combos. Reading roster_FLX for a
#    defender multiplies the IDP flex share by the offensive slot count, which is why DB
#    derived 2.33 started against 0.90 observed.
MECHANISM = {
    #        dedicated, flex slots, flex share, bench share  -> started gap / rostered gap
    "QB":  {"ded": 1.0, "flex": 2.0, "flex_share": 0.000, "bench_share": 0.126},   # 0.00 / -0.01
    "RB":  {"ded": 2.0, "flex": 2.0, "flex_share": 0.265, "bench_share": 0.330},   # -0.03 / 0.05
    "WR":  {"ded": 2.0, "flex": 2.0, "flex_share": 0.585, "bench_share": 0.390},   # 0.13 / 0.00
    "TE":  {"ded": 1.0, "flex": 2.0, "flex_share": 0.040, "bench_share": 0.118},   # 0.00 / -0.04
    "K":   {"ded": 1.0, "flex": 0.0, "flex_share": 0.000, "bench_share": 0.014},   # 0.00 / -0.01
    "DEF": {"ded": 1.0, "flex": 0.0, "flex_share": 0.000, "bench_share": 0.033},   # 0.00 / -0.01
    "DL":  {"ded": 1.0, "flex": 1.0, "flex_share": 0.000, "bench_share": 0.051},   # -0.33 / 0.02
    "LB":  {"ded": 1.0, "flex": 1.0, "flex_share": 0.770, "bench_share": 0.091},   # -0.19 / 0.07
    "DB":  {"ded": 2.0, "flex": 1.0, "flex_share": 0.100, "bench_share": 0.037},   # -1.20 / -0.15
}

# THE ROSTERED CHECK IS AN IDENTITY, NOT A TEST. derived_rostered = started + bench x
# bench_share = (started + benched)/teams = rostered/teams. It cannot fail, and it validates
# nothing. It was reported all session as if it confirmed the mechanism; it confirms only that
# rostered equals started plus benched. The ONLY real check is STARTED.
# STARTED, derived per league then medianed (never median-of-inputs), reproduces observation
# for SEVEN: QB 0.00, TE -0.00, K 0.00, DEF 0.00, RB -0.03, LB +0.05, WR +0.13.
# DL (-0.33) and DB (-1.10) do not.
#
# WHY DB FAILS -- three explanations tried, two wrong, all recorded so none is retried:
#   1. WRONG: "leagues split CB and S so our label misses them." The supertable carries
#      exactly three defensive labels -- DB (434 players), DL (328), LB (316). No split
#      exists. Asserted without checking.
#   2. WRONG: "roster_DB is corrupt." It does carry 40/41/42/200, but only in 68 leagues of
#      ~9,700; the bulk sits at 1-4 and the median of 2.0 is sound.
#   3. PARTLY: median-of-inputs. Combining MEDIAN(ded) with MEDIAN(flex) is invalid on the
#      mixed IDP lane. Fixed -- LB went -0.19 to +0.05 on it. DB only moved -1.20 to -1.10.
# What remains is a genuine mismatch: those leagues declare a median of 2 DB slots and start
# 0.90 per team, with join coverage at 99.7%. The declared value does not describe behaviour.
# Not chased further -- IDP is accepted as-is and the observed-capacity path does not read
# roster_DB.
#
# It is NOT a position-labelling problem. The supertable carries exactly three defensive
# labels -- DB (434 players), DL (328), LB (316) -- with no CB/S split, so the earlier
# explanation that leagues split those was wrong and was never checked before being written.
# Not chased further: IDP is accepted as-is, and the fix would be another league_settings
# repair on a column the observed-capacity path no longer depends on.
MECHANISM_STARTED_UNVERIFIED = {"DL", "DB"}

# STILL OPEN: these are measured on 2024 alone (4,467 clean-lane leagues of 64,991 in the
# corpus). The CUTS use 2021-2025; the constants should too before they are treated as
# permanent. Single-year is what I did, not a decision -- and TE rostership climbing
# 1.64 -> 2.03 across 2023-2025 is direct evidence a one-year snapshot moves.
MECHANISM_MEASURED_ON = "2024 only"

# Measured landing % (STARTED) and drift % (c2, both stats), 2021-2025.
TIER_LANDING = {"QB/flx": 90, "QB/sflx": 91, "K": 97, "DEF": 98,
                "TE": 74, "RB": 67, "WR": 54, "DB": 23, "DL": 17, "LB": 18}
TIER_DRIFT = {("QB", "rostered"): 10, ("QB", "started"): 10,
              ("RB", "rostered"): 8, ("RB", "started"): 5,
              ("WR", "rostered"): 3, ("WR", "started"): 5,
              ("TE", "rostered"): 19, ("TE", "started"): 13,
              ("K", "rostered"): 2, ("K", "started"): 0,
              ("DEF", "rostered"): 6, ("DEF", "started"): 0,
              ("DL", "rostered"): 49, ("DL", "started"): 50,
              ("LB", "rostered"): 21, ("LB", "started"): 14,
              ("DB", "rostered"): 23, ("DB", "started"): 22}

# PASSED EVERY GATE. Cuts for anything not in here are the best available measurement but
# failed at least one check -- named in PROVISIONAL_REASON rather than quietly shipped.
FULLY_DERIVED = {
    # drift <= 20% on both cuts, measured 2021-2025 (WR was accepted at 6-19%)
    ("QB", "rostered"), ("QB", "started"),      # 10% / 10%, both lanes
    ("RB", "rostered"), ("RB", "started"),      #  8% /  5%
    ("WR", "rostered"), ("WR", "started"),      #  3% /  5%
    ("TE", "rostered"), ("TE", "started"),      # 19% / 13%
    ("K",  "rostered"), ("K",  "started"),      #  2% /  0%
    ("DEF", "rostered"), ("DEF", "started"),    #  6% /  0%
    ("LB", "started"),                          # 14%
}
PROVISIONAL_REASON = {
    ("LB", "rostered"): "drift 21%",
    ("DB", "rostered"): "drift 23%", ("DB", "started"): "drift 22%",
    ("DL", "rostered"): "drift 49%", ("DL", "started"): "drift 50%",
}
TIER_CUT_DRIFT_PCT = {
    ("DB", "rostered"): 65, ("TE", "rostered"): 40, ("QB", "rostered"): 29,
    ("DB", "started"): 23, ("TE", "started"): 21, ("DL", "rostered"): 19,
    ("LB", "started"): 18, ("WR", "rostered"): 16,
}
UNSTABLE_CUTS = {k for k, v in TIER_CUT_DRIFT_PCT.items() if v > 20}

# DERIVED END TO END -- the full WR sequence was run for these, not just a quantile match.
# Everything NOT in here is a quantile of observed capacity with no mechanism behind it and
# no stability/dumping-ground check. Those are provisional, however confident they look.
# (the RB-era FULLY_DERIVED set is superseded by the v3 one above)

# RB mechanism constants, measured in the clean lane (managed, redraft, flex-only, 2024):
#   started/team 2.5    bench/team 6.25-7.0    RB share of bench 0.33    rostered/team 4.8
# The bench share is FLAT across scoring (0.323-0.338), unlike WR whose flex share swings
# 47%->60% between std and PPR. Scoring moves WR capacity and does not move RB capacity.
# The STARTING mechanism overpredicts by 3-15% because RB slots go unfilled (byes, injuries,
# thin waivers) where WR slots never do -- so for RB only the OBSERVED count is trustworthy,
# and the derive-from-slots route that got WR to 3.3 would put RB ~10% high.
RB_MECHANISM = {"started_per_team": 2.5, "bench_share": 0.33, "rostered_per_team": 4.8}

# The IDP bottom cuts are degenerate: DB rostered c1 = 1.0 spots is not a boundary, it is an
# artifact of 1,500-1,800 leagues with a heavily skewed capacity distribution against 11,000+
# for the skill positions. Flagged, not silently shipped.
DEGENERATE_BOTTOM_CUT = {("DB", "rostered"), ("DB", "started"), ("DL", "started")}

TIER_CUTS_WR = TIER_CUTS[("WR", "rostered")]   # back-compat for the v2 wiring already in place


def tier_cuts(pos: str, stat: str = "rostered", lane: str | None = None) -> tuple:
    """Cutoffs for this position/stat/lane. Raises rather than guessing an unmeasured one."""
    pos = pos.upper()
    for key in ((pos, stat, lane), (pos, stat)):
        if key in TIER_CUTS:
            return TIER_CUTS[key]
    raise KeyError(f"no measured tier cuts for {(pos, stat, lane)}; derive them before use")


def observed_capacity_tier_sql(capacity_expr: str, cuts: tuple,
                               labels: tuple = TIER_LABELS) -> str:
    """The tier. `capacity_expr` must be OBSERVED spots, never declared-slot arithmetic.

    `cuts` is REQUIRED and has no default. It defaulted to WR's cuts, which silently gave
    QB, RB and TE the receiver ladder -- a QB league at 20 rostered QBs fell under WR's
    52.6 c1 and read 08tm for every position. Each position has its own measured cuts and
    must pass them explicitly.
    """
    c1, c2, c3 = cuts
    a, b, c, d = labels
    return (f"CASE WHEN {capacity_expr} < {c1} THEN '{a}' "
            f"WHEN {capacity_expr} < {c2} THEN '{b}' "
            f"WHEN {capacity_expr} < {c3} THEN '{c}' ELSE '{d}' END")


def _require_contract(contract: str, fn: str) -> None:
    if contract != TIER_CONTRACT_VERSION:
        raise ValueError(
            f"{fn}: tier contract mismatch. Got {contract!r}, expected "
            f"{TIER_CONTRACT_VERSION!r}. The team-number definition changed on 2026-08-02 from "
            f"declared roster slots to observed capacity, and it is INVISIBLE -- an un-updated "
            f"caller emits the old '10t'/'12t' alphabet and is silently wrong. Update the call "
            f"site deliberately; do not paper over this by passing the constant through.")
POS_GRPS = ("QB", "RB", "WR", "TE", "K", "DEF")

# A contested seat is one a manager may fill with more than one position. In the flx lane
# that is the W/R/T FLEX only. In sflx/idp the SUPER_FLEX seat is contested too, and ignoring
# it understates every skill position's slot count -- measured, the superflex seat takes a QB
# 27% of the time and an RB ~24%. `roster` (the lane) is already a cohort dimension, so
# indexing on it costs no new key.
CONTESTED_EXTRA_SUPER_FLEX = {"flx": False, "sflx": True, "idp": True}

# FLEX_FILL[lane][scoring][pos] = share of ONE contested seat the position takes.
#
# Method: overflow_pos = started_pos - num_teams * roster_pos, as a share of the contested
# pool, renormalised to FILLED seats. Renormalising matters: 7-16% of contested seats sit
# empty in a given league-week (bye, injury, inattentive manager), and leaving that in the
# denominator biases every share DOWN by that amount.
#
# The flx-lane QB entry is a STRUCTURAL ZERO, not a measurement. A W/R/T flex cannot start a
# QB. The raw numbers show QB at 0.0002/0.0001/0.0000 there -- 0.02% of filled seats, leakage
# from leagues whose roster columns misclassify them. Carrying it put the QB/flx divisor at
# 1.0002 and quietly broke agreement with the proven QB formula, so it is forced to 0 and the
# other three renormalised.
#
# RB and WR move monotonically with receiving points, in the directions theory predicts
# (RB .384 -> .303, WR .541 -> .628 from std to full PPR). That is the WR re-derivation
# rank_slots_contract asked for: the earlier .532/.543/.518 was non-monotonic because it
# pooled lanes while dividing by a flx-sized denominator and did not renormalise to filled
# seats. TE does NOT behave: .075 std / .048 half / .069 ppr is non-monotonic on 2.4M
# contested seats, so the TE weight is carried as measured but is NOT to be leaned on until
# it is explained. RB is what this change wires.
FLEX_FILL: dict[str, dict[str, dict[str, float]]] = {
    "flx": {
        "std":  {"QB": 0.0, "RB": 0.3837, "WR": 0.5414, "TE": 0.0749},
        "half": {"QB": 0.0, "RB": 0.3459, "WR": 0.6058, "TE": 0.0483},
        "ppr":  {"QB": 0.0, "RB": 0.3025, "WR": 0.6284, "TE": 0.0691},
    },
    "sflx": {
        "std":  {"QB": 0.2464, "RB": 0.2529, "WR": 0.3972, "TE": 0.1035},
        "half": {"QB": 0.2678, "RB": 0.2524, "WR": 0.4048, "TE": 0.0749},
        "ppr":  {"QB": 0.2805, "RB": 0.2231, "WR": 0.4195, "TE": 0.0770},
    },
    "idp": {
        "std":  {"QB": 0.1235, "RB": 0.3608, "WR": 0.4543, "TE": 0.0614},
        "half": {"QB": 0.1807, "RB": 0.2879, "WR": 0.4550, "TE": 0.0764},
        "ppr":  {"QB": 0.1789, "RB": 0.2652, "WR": 0.4786, "TE": 0.0773},
    },
}

# DIVISOR[pos][lane] = median slots-per-team, over every league-year carrying roster_{pos}.
# QB/flx is exactly 1.0000 by construction (see module docstring).
DIVISOR: dict[str, dict[str, float]] = {
    "QB": {"flx": 1.0000, "sflx": 1.8415, "idp": 1.3614},
    "RB": {"flx": 2.6050, "sflx": 2.7572, "idp": 2.5304},
    "WR": {"flx": 3.2568, "sflx": 3.8390, "idp": 3.4358},
    "TE": {"flx": 1.1382, "sflx": 1.2996, "idp": 1.1546},
}

# The cut itself is unchanged from the league-level rule it replaces.
TEAM_EQUIV_CUT = 11
LANES = ("flx", "sflx", "idp")
SCORINGS = ("std", "half", "ppr")

ROSTER_COL = {"QB": "roster_QB", "RB": "roster_RB", "WR": "roster_WR", "TE": "roster_TE"}

# Columns cohort_league_settings_sql must expose for the expressions below to bind.
REQUIRED_SETTINGS_COLUMNS = (
    "num_teams", "roster_QB", "roster_RB", "roster_WR", "roster_TE",
    "roster_FLX", "roster_SUPER_FLEX",
)

# Dual-eligible players (Taysom Hill 'QB,TE') must land in exactly ONE pos_grp or they are
# double-counted against a per-position denominator. `MAX(pos_grp)` was safe while the
# alphabet was {K, DEF, SKILL} -- nobody is two of those -- but over {QB,RB,WR,TE} it picks
# alphabetically, which is not a football answer. Use the SINGULAR primary from the position
# taxonomy; the eligibility gate keeps reading the plural list, so a dual player stays
# eligible everywhere he can start while being COUNTED once, where he mostly plays.
PRIMARY_POSITION_COLUMN = "broad_position"


def _weight_case(pos: str, alias: str, lane_expr: str, scoring_expr: str) -> str:
    """Nested CASE giving FLEX_FILL[lane][scoring][pos]."""
    lanes = []
    for lane in LANES:
        arms = " ".join(
            f"WHEN {scoring_expr} = '{sc}' THEN {FLEX_FILL[lane][sc][pos]}"
            for sc in SCORINGS)
        # scoring is never NULL out of cohort_league_settings_sql (it COALESCEs to 'std'),
        # but bind a default anyway so a future caller cannot get a silent NULL weight.
        lanes.append(f"WHEN {lane_expr} = '{lane}' THEN "
                     f"CASE {arms} ELSE {FLEX_FILL[lane]['std'][pos]} END")
    return "CASE " + " ".join(lanes) + " ELSE 0 END"


def contested_expr(alias: str, lane_expr: str) -> str:
    """Seats a manager may fill with more than one position."""
    sflx_lanes = ", ".join(f"'{lane}'" for lane, extra
                           in CONTESTED_EXTRA_SUPER_FLEX.items() if extra)
    return (f"(COALESCE({alias}.roster_FLX, 0) + CASE WHEN {lane_expr} IN ({sflx_lanes}) "
            f"THEN COALESCE({alias}.roster_SUPER_FLEX, 0) ELSE 0 END)")


# IDP broad classes. These get their OWN pos_grp rather than riding the old 'SKILL' catch-all.
# Leaving them in 'SKILL' is the same defect that produced the fake "-20.3pt dynasty effect"
# for kickers: at the roster='ALL' rung a linebacker's numerator can only come from idp
# leagues (the eligibility gate says so) while his SKILL denominator counts every league in
# the cohort, so the pooled rung reads him as unstartable in most of it. Scoping both sides
# is the fix, exactly as it was for K and DEF.
IDP_BROAD = ("DL", "LB", "DB")

# pos_grp alphabet after refinement. 'SKILL' is GONE: it conflated four positions whose slot
# markets differ by 2-4x, which is the entire premise of this module.
POS_GRP_SQL_ALPHABET = ("QB", "RB", "WR", "TE", "K", "DEF", "IDP")


def pos_grp_sql(alias: str = "pos") -> str:
    """Map one player-year to exactly ONE pos_grp.

    Reads `broad_position`, the taxonomy view's SINGULAR primary, which LocalReader already
    resolves by an explicit priority order (QB, RB, WR, TE, K, DEF, DL, LB, DB, OL, P). So
    Taysom Hill's ['QB','TE'] becomes 'QB' by a football rule.

    This replaces `MAX(CASE WHEN position IN ('K','DEF') THEN position ELSE 'SKILL' END)`.
    MAX() was harmless over {K, DEF, SKILL} because no player is two of those. Over
    {QB,RB,WR,TE} it would pick alphabetically -- 'QB' over 'TE' by luck, 'RB' over 'WR' by
    accident -- and a per-position denominator cannot absorb that.

    Eligibility keeps reading `broad_positions` (the plural list) so a dual-eligible player
    stays eligible in every cohort he can start in. Only his COUNTING home is singular.
    """
    idp = ", ".join(f"'{p}'" for p in IDP_BROAD)
    # Some historical player-position rows have a NULL/incorrect broad_position
    # enrichment even though the canonical position is present. Never let those
    # players fall through to IDP; use the canonical position as the fallback.
    primary = f"COALESCE(NULLIF({alias}.broad_position, ''), {alias}.position)"
    return (f"CASE WHEN {primary} IN ('QB','RB','WR','TE','K','DEF') "
            f"THEN {primary} "
            f"WHEN {primary} IN ({idp}) THEN 'IDP' "
            f"ELSE 'IDP' END")


def teams_by_pos_grp_sql(pos_grp_expr: str, alias: str = "ls") -> str:
    """Pick the right per-position `teams` bucket for a row, given its pos_grp expression.

    ALL NINE POSITIONS now have measured cuts, so nothing falls through to the league-level
    `teams` any more. K and DEF used to, on the grounds that their roster columns are too
    sparse to model (roster_K 34.6%, roster_DEF 38.2%) -- but that was an argument against the
    DECLARED-SLOT path, which is gone. Observed capacity does not care how often a settings
    column is populated; it counts what is actually on rosters.
    """
    arms = " ".join(f"WHEN '{pos}' THEN {alias}.teams_{pos}" for pos in TIER_POSITIONS)
    return f"CASE {pos_grp_expr} {arms} ELSE {alias}.teams END"


# Blast radius, measured 2026-07-30 against the league-level rule this replaces. Recorded so
# a future reader can tell a refinement from a churn without re-running anything.
BUCKET_MOVEMENT = {          # pos -> (league_years, pct_changing_bucket, up_10_to_12, down_12_to_10)
    "QB": (63410, 18.4, 7636, 4024),
    "RB": (63448, 22.5, 5024, 9280),
    "WR": (63411, 24.7, 8134, 7532),
    "TE": (63386, 10.7, 3306, 3482),
}
# 23,553 of 63,448 league-years (37%) are '10t' for one position and '12t' for another.
LEAGUE_YEARS_SPLIT_ACROSS_POSITIONS = 23553


# --- POSITION ELIGIBILITY -------------------------------------------------------------
# A league-year is NOT a denominator for every position. A flex league cannot roster a
# linebacker, and counting it as an eligible-but-never-rostered league inflates every IDP
# denominator -- measured 2026-08-03, DL/LB/DB were being denominated on 60,986 league-years
# when only 10,982 are IDP, a 5.6x inflation. A denominator wrong by being too big is the
# same defect as one wrong by being too small.
#
# These predicates are the rule DENOM_WEEK_SQL already applied inline; lifted here so the
# census, the base extract and the pipeline cannot drift to three different answers.
ELIGIBILITY_REQUIRES = {
    "K":   "k_slots",       # a league with no K slot never rosters a kicker
    "DEF": "def_slots",
    "DL":  "idp", "LB": "idp", "DB": "idp",
    # QB/RB/WR/TE are rosterable in every league by construction -- no gate.
}


def position_eligibility_sql(pos: str, alias: str = "f") -> str:
    """SQL predicate for 'this league-year can roster `pos`'. TRUE when unrestricted."""
    req = ELIGIBILITY_REQUIRES.get(pos)
    if req is None:
        return "TRUE"
    if req == "idp":
        return f"{alias}.roster = 'idp'"
    return f"COALESCE({alias}.{req}, 0) > 0"
