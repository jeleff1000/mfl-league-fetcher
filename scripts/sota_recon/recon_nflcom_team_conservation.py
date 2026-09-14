"""LANE: (franchise, season) conservation against the NFL.com team authority.

WHAT WAS MISSING, and it is a GRAIN, not a framework. The program already reconciles team
lines against the sum of their players in three places:

    recon_vertical_team          Sigma(player atoms) == pfr_box_team_stats line, GAME grain
    recon_teamtotal              Sigma(player stats) == schedule-authority total, GAME grain
    recon_conservation           Sigma(all players)  == PFR season pages,    LEAGUE-YEAR grain
    recon_defense_conservation   DST row == Sigma(IDP), (franchise, year) -- but INTERNAL,
                                 both sides drawn from v26, so it can prove self-consistency
                                 and never external truth

NFL.com publishes team totals at (franchise, SEASON). That grain has an external authority
for the first time, and it catches a class the others cannot: a player missing from a whole
SEASON shows up here as a shortfall, while the game-grain lanes only see the games they have
rows for and the league-year lane averages the error away across 32 teams.

THE KEY IS RECEIPTED, NOT GUESSED. NFL.com names teams by a DOUBLED NICKNAME
('Bears Bears'), which cannot be matched to a franchise by label: every franchise spans the
nickname's seasons, so containment is degenerate, and renamed franchises
(Redskins -> Football Team -> Commanders) defeat similarity. The key comes from
crosswalk_receipts.v1.json#nflcom_team_fid, established by season passing-yard fingerprint
(2,146 of 2,228 team-seasons to within a yard) and keyed per (nickname, SEASON) because
Texans and Titans each span two franchises.

THE SEASON_TYPE TRAP. nflcom_team_stats declares reg and post, and they are 100.00%
identical on every joined pair in all 11 categories -- the postseason half carries no
information. Only `reg` is read; comparing the `post` half would validate regular-season
numbers as postseason.

Run::

    python -m scripts.sota_recon.recon_nflcom_team_conservation [--year 2004] [--csv out.csv]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from . import sources as S

STATS_PATH = r"D:/league-history-data/nfl/raw/nflcom/tables/team_stats/**/*.parquet"
RECEIPTS = (Path(__file__).resolve().parent / "witness_gate" / "contracts"
            / "crosswalk_receipts.v1.json")

#: (label, nflcom category, nflcom side, nflcom column, v26 expression, PLANE
#:  [, AGG [, SOURCE EXPRESSION]]).
#:
#: AGG is the seventh field and defaults to "SUM". It exists because the aggregation class is
#: a PROPERTY OF THE STAT, never of the lane: summing a maximum is exactly the defect that put
#: 4,407 impossible values into the season and career planes (`fg_long` 1,200 yards), and a
#: lane hard-coded to SUM cannot witness the nine `lng` columns at all. AGG applies to BOTH
#: sides -- MAX over a franchise's players is the team's longest play, and MAX over the single
#: authority row is that row's value.
#:
#: SOURCE EXPRESSION is the eighth field and defaults to `TRY_CAST(ts."<col>" AS DOUBLE)`. It
#: exists for the field-goal distance buckets, whose cell is COMPOSITE: NFL.com publishes
#: `30_39_a_m` as '10_8', two facts in one string. MapSpec cannot express that -- its
#: `value_path` and `transform` fields are DECLARED AND INERT, so a composite mapping there
#: emits a spec measuring zero rows while reading as mapped -- but this lane builds its own
#: SQL, so it can parse the cell and the blocker never applies here.
#:
#: PLANE is the sixth field and it is NOT cosmetic. v26 keeps defensive production on TWO
#: planes -- IDP player rows (position<>'DEF') and a single team row (position='DEF') -- and
#: which one reproduces an NFL.com team total is a PER-COLUMN fact, measured:
#:
#:      int_td   IDP 98.91%   DEF row 98.91%   -> either; IDP chosen for consistency with the
#:                                               lane's law (sum of a franchise's players)
#:      sfty     IDP 77.01%   DEF row 96.43%   -> DEF row, by 19 points. IDP safeties run
#:                                               short, which recon_defense_conservation
#:                                               already reports as 2.0-64.7% by era
#:      fr_td    IDP 91.08%   (no DST column)  -> IDP, the only option
#:
#: SUMMING ACROSS BOTH PLANES WOULD DOUBLE-COUNT, so a family that does not declare its plane
#: is a family that silently picked one.
#:
#: AND THE ORIGINAL COMMENT HERE WAS WRONG FOR ONE CATEGORY. It said "the `defense` side is
#: the OPPONENT's production". True for the volume categories -- passing/defense is passing
#: ALLOWED -- and FALSE for `scoring`, where fr_td/int_td/sfty are the team's OWN defensive
#: scores. Measured before extending rather than inherited: all three reconcile against the
#: team's own players at 91-99%, not against the opponent's.
FAMILIES = [
    ("PASS_YDS", "passing", "offense", "pass_yds", "passing_yards", "all"),
    ("PASS_TD", "passing", "offense", "td", "passing_tds", "all"),
    ("PASS_INT", "passing", "offense", "int", "passing_interceptions", "all"),
    ("PASS_CMP", "passing", "offense", "cmp", "completions", "all"),
    ("PASS_ATT", "passing", "offense", "att", "attempts", "all"),
    ("RUSH_YDS", "rushing", "offense", "rush_yds", "rushing_yards", "all"),
    ("RUSH_TD", "rushing", "offense", "td", "rushing_tds", "all"),
    ("RUSH_ATT", "rushing", "offense", "att", "carries", "all"),
    ("RECV_YDS", "receiving", "offense", "yds", "receiving_yards", "all"),
    ("RECV_TD", "receiving", "offense", "td", "receiving_tds", "all"),
    ("RECV_REC", "receiving", "offense", "rec", "receptions", "all"),
    # Tier 1: these are opponent-volume / first-down atoms.  They live only on the
    # materialized team DEF row; summing IDP players would be a different quantity.
    ("ALW_PASS_FD", "passing", "defense", "1st", "passing_first_downs_allowed", "def"),
    ("ALW_PASS_ATT", "passing", "defense", "att", "def_attempts_allowed", "def"),
    ("ALW_RUSH_FD", "rushing", "defense", "rush_1st", "rushing_first_downs_allowed", "def"),
    ("ALW_RUSH_ATT", "rushing", "defense", "att", "def_carries_allowed", "def"),
    ("ALW_RECV_FD", "receiving", "defense", "rec_1st", "receiving_first_downs_allowed", "def"),
    # ---- scoring/defense: the team's OWN defensive scores (see the plane note above) ----
    ("SC_DEF_INT_TD", "scoring", "defense", "int_td", "def_int_ret_td", "idp"),
    ("SC_DEF_SFTY", "scoring", "defense", "sfty", "def_safeties", "def"),
    ("SC_DEF_FR_TD", "scoring", "defense", "fr_td", "fum_ret_td", "idp"),
    # ---- scoring/offense ----
    # The scoring-page copies of rsh_td and rec_td are wired below as independent source
    # families. Their equality to rushing.td and receiving.td is a finding, not a reason to
    # leave a mapped source column unwitnessed.
    #
    # tot_td IS NOT HERE EITHER, because it is not identified. It is NOT offensive scrimmage
    # TDs: tot_td == rsh_td + rec_td on only 11.54% of 2,228 team-seasons. And v26 carries TWO
    # near-identical names -- `total_tds_scored` and `total_tds_accounted_for` -- of which the first is much closer
    # (63.21% vs 0.38%), so my first attempt wired the wrong one and scored 1.2% clean. 63.21%
    # is still far from a mapping. QUEUED as an identification question, not wired.
    # TWO-POINT IS AN EQUATION AND THE THIRD TERM WOULD DOUBLE-COUNT. v26 keeps passing,
    # rushing AND receiving 2pt conversions, but a two-point PASS is one team event credited
    # on both the passing and the receiving side: measured passing_2pt == receiving_2pt on
    # 447 of 447 team-seasons (100.00%), mean 1.66 each. So the team total is rush + pass.
    ("SC_OFF_2PT", "scoring", "offense", "2_pt",
     "COALESCE(rushing_2pt_conversions,0) + COALESCE(passing_2pt_conversions,0)", "idp"),
    # ---- scoring/special-teams: duplicate source columns are still measured ----
    # fgm, kret_td and pret_t are exact copies of field-goals / kickoff-returns /
    # punt-returns in the source. Their own families below verify that the duplicate pages
    # reach the same supertable canonical without silently disappearing from the audit.
    ("SC_ST_XPM", "scoring", "special-teams", "xpm", "pat_made", "idp"),
    # ---- the own-category primaries for the columns scoring duplicates ----
    ("FG_MADE", "field-goals", "special-teams", "fgm", "fg_made", "idp"),
    ("KRET_TD", "kickoff-returns", "special-teams", "kret_td", "kickoff_return_tds", "idp"),
    ("PRET_TD", "punt-returns", "special-teams", "pret_t", "punt_return_tds", "idp"),
    ("SC_RECV_TD", "scoring", "offense", "rec_td", "receiving_tds", "idp"),
    ("SC_RUSH_TD", "scoring", "offense", "rsh_td", "rushing_tds", "idp"),
    ("SC_FG_MADE", "scoring", "special-teams", "fgm", "fg_made", "idp"),
    ("SC_KRET_TD", "scoring", "special-teams", "kret_td", "kickoff_return_tds", "idp"),
    ("SC_PRET_TD", "scoring", "special-teams", "pret_t", "punt_return_tds", "idp"),
    # ---- THE DEFENSE SIDE: the largest witness gap in the whole source ----
    #
    # The lane deferred this as "a different equation and a separate queue item", and it is a
    # different equation -- the defense side is what the OPPONENT did, so it reconciles against
    # our *_allowed columns rather than against a sum of our own players.
    #
    # WHY IT MATTERS MORE THAN ANYTHING ELSE HERE: v26 carries 26 *_allowed canonicals and
    # every one of them has ZERO licence rows except def_air_yards_allowed (2). They are
    # entirely unwitnessed. They also live exclusively on the DEF plane -- 34,601 DEF rows and
    # 0 non-DEF rows for the team-level ones -- so plane="def" is forced, not chosen.
    # ALW_PASS_YDS is the GROSS allowed-passing-yards canonical. The earlier subtraction of
    # def_sack_yards was a mapping error: on the keyed 2025 authority rows,
    # passing/defense.yds == SUM(passing_yds_allowed) exactly 30/30, while subtracting sacks
    # misses by a mean 256.8 yards. Keep the expression NULL-preserving; this is a direct
    # count-family witness, not a COALESCE-to-zero repair.
    ("ALW_PASS_YDS", "passing", "defense", "yds", "passing_yds_allowed", "def"),
    ("ALW_PASS_TD", "passing", "defense", "td", "passing_tds_allowed", "def"),
    ("ALW_PASS_CMP", "passing", "defense", "cmp", "def_completions_allowed", "def"),
    ("ALW_RUSH_YDS", "rushing", "defense", "rush_yds", "rushing_yds_allowed", "def"),
    ("ALW_RUSH_TD", "rushing", "defense", "td", "rushing_tds_allowed", "def"),
    ("ALW_RECV_TD", "receiving", "defense", "td", "receiving_tds_allowed", "def"),
    # ---- downs: third/fourth down, published on BOTH sides ----
    ("ALW_3RD_MD", "downs", "defense", "3rd_md", "def_third_down_allowed", "def"),
    ("ALW_3RD_ATT", "downs", "defense", "3rd_att", "def_third_down_faced", "def"),
    ("ALW_4TH_MD", "downs", "defense", "4th_md", "def_fourth_down_allowed", "def"),
    ("ALW_4TH_ATT", "downs", "defense", "4th_att", "def_fourth_down_faced", "def"),

    # =====================================================================================
    # THE COUNT FAMILIES, wired in bulk after Joe corrected the claim that these were
    # missing or blocked. Every canonical below ALREADY EXISTS and several are already
    # licensed from other sources; the untied count measured work not done, not absent
    # material. Names were SEARCHED in the schema, not guessed -- guessing two names and
    # concluding absence produced a false "NO v26 COLUMN" three times in one session.
    #
    # THE _2 SUFFIX IS THE RATE VARIANT, not a second count: nflcom publishes `1st` beside
    # `1st_2`, `cmp` beside `cmp_2`. Confirmed by the existing equation floor on
    # nflcom_player_season|passing|reg|1st_2. Rates cannot be summed to a team total, so
    # every _2 column plus rate/yds_att/ypc/yds_rec/avg/fg/xp_pct is routed to
    # audit_capacity_runner instead of appearing here.
    # =====================================================================================

    # ---- first downs: the canonicals exist and each already carries one licence ----
    ("FD_PASS", "passing", "offense", "1st", "passing_first_downs", "idp"),
    ("FD_RUSH", "rushing", "offense", "rush_1st", "rushing_first_downs", "idp"),
    ("FD_RECV", "receiving", "offense", "rec_1st", "receiving_first_downs", "idp"),
    # The downs page republishes both sides of these count fields.  Defense is opponent
    # production and therefore uses the allowed canonicals on the DEF row; offense uses
    # the ordinary team production canonicals.  These are count witnesses, not the `_2`
    # rate forms (whose downs-page denominators are not published here).
    ("FD_RUSH_DOWNS", "downs", "defense", "rush_1st", "rushing_first_downs_allowed", "def"),
    ("FD_RECV_DOWNS", "downs", "defense", "rec_1st", "receiving_first_downs_allowed", "def"),
    ("FD_RUSH_DOWNS_OFF", "downs", "offense", "rush_1st", "rushing_first_downs", "idp"),
    ("FD_RECV_DOWNS_OFF", "downs", "offense", "rec_1st", "receiving_first_downs", "idp"),

    # ---- longest: the MAX class, now wirable because AGG is declared per family ----
    #
    # The omission was recorded here as a decision rather than an oversight, and the fix was
    # the seventh field, not a second lane. Six of the nine published `lng` columns have a
    # direct canonical and all six measure 100.00% EXACT in 2025 -- MAX(longest play across a
    # franchise's players) == the team's longest play, with no rounding to lose and no
    # population to be short of.
    #
    # THE OTHER THREE ARE NOT WIRED AND CANNOT BE: passing/defense, receiving/defense and
    # rushing/defense `lng` are the longest play ALLOWED, and v26's 26 *_allowed canonicals
    # were searched -- there is no *_long_allowed among them. Candidates, not mappings.
    ("PASS_LNG", "passing", "offense", "lng", "passing_long", "idp", "MAX"),
    ("RUSH_LNG", "rushing", "offense", "lng", "rushing_long", "idp", "MAX"),
    ("RECV_LNG", "receiving", "offense", "lng", "receiving_long", "idp", "MAX"),
    ("FG_LNG", "field-goals", "special-teams", "lng", "fg_long", "idp", "MAX"),
    ("KRET_LNG", "kickoff-returns", "special-teams", "lng", "kickoff_return_long",
     "idp", "MAX"),
    ("PRET_LNG", "punt-returns", "special-teams", "lng", "punt_return_long", "idp", "MAX"),

    # ---- field-goal distance buckets: the MADE half of a composite cell ----
    #
    # SHAPE PROVEN GLOBALLY, NOT SAMPLED. The cell is '{att}_{made}': on all 2,228 team-seasons
    # every bucket splits into exactly 2 parts and part1 < part2 NEVER happens, which is what
    # fixes the direction (a made count cannot exceed its attempts). Four eyeballed samples
    # would not have established either fact.
    #
    # THE MADE HALF IS THE MAPPING; THE ATTEMPT HALF IS A GAP. att == fg_made_X + fg_missed_X
    # measures 34-80% and runs SHORT (40-49 yards: 1,377 of 2,101 short, median authority 8 vs
    # ours 5), so `fg_missed_*` does not complete the attempt total -- filed as a supertable
    # gap rather than wired to a number that does not reconcile.
    ("FGB_0_19", "field-goals", "special-teams", "1_19_a_m", "fg_made_0_19", "idp", "SUM",
     """TRY_CAST(split_part(ts."1_19_a_m", '_', 2) AS DOUBLE)"""),
    ("FGB_20_29", "field-goals", "special-teams", "20_29_a_m", "fg_made_20_29", "idp", "SUM",
     """TRY_CAST(split_part(ts."20_29_a_m", '_', 2) AS DOUBLE)"""),
    ("FGB_30_39", "field-goals", "special-teams", "30_39_a_m", "fg_made_30_39", "idp", "SUM",
     """TRY_CAST(split_part(ts."30_39_a_m", '_', 2) AS DOUBLE)"""),
    ("FGB_40_49", "field-goals", "special-teams", "40_49_a_m", "fg_made_40_49", "idp", "SUM",
     """TRY_CAST(split_part(ts."40_49_a_m", '_', 2) AS DOUBLE)"""),
    ("FGB_50_59", "field-goals", "special-teams", "50_59_a_m", "fg_made_50_59", "idp", "SUM",
     """TRY_CAST(split_part(ts."50_59_a_m", '_', 2) AS DOUBLE)"""),
    # 60+ HAD THREE CANDIDATE NAMES and the choice is measured, not guessed. `fg_made_60_` and
    # `fg_made_60plus` are BYTE-IDENTICAL witnesses (99.18% each on 2,072 team-seasons), which
    # makes them an internal duplicate pair worth its own ruling; `fg_made_60_plus_canonical`
    # joins only 52 team-seasons and is a narrow slice, not the target.
    ("FGB_60", "field-goals", "special-teams", "60_a_m", "fg_made_60_", "idp", "SUM",
     """TRY_CAST(split_part(ts."60_a_m", '_', 2) AS DOUBLE)"""),

    # ---- 40+ yard plays: exact targets exist per family ----
    ("P40_PASS", "passing", "offense", "40", "completions_40plus", "idp"),
    ("P40_RECV", "receiving", "offense", "40", "receptions_40plus", "idp"),
    ("P40_RUSH", "rushing", "offense", "40", "rushing_40plus", "idp"),

    # ---- 20+ yard plays: pass and receive have a 20-yard column; RUSH DOES NOT.
    # v26 carries rush_explosive_10, a TEN-yard threshold, so mapping nflcom's rushing `20`
    # onto it would equate two different definitions. Joe: "explosive means something else."
    # Rushing 20+ is raised as a candidate instead.
    ("P20_PASS", "passing", "offense", "20", "pass_explosive_20", "idp"),
    ("P20_RECV", "receiving", "offense", "20", "rec_explosive_20", "idp"),

    # ---- sacks, on both sides of the ball ----
    # PLANE MEASURED, NOT ASSUMED: def_sacks on the IDP plane gives 76.7% in 2025 and on the
    # team-DEF row gives 100.0%. Sacks are credited per-player as halves, so the IDP sum loses
    # to rounding while the DEF row carries the official team figure.
    ("SACK_DEF", "passing", "defense", "sck", "def_sacks", "def"),
    # SACKY_DEF REMOVED, and the zero-row report is what found it. I declared
    # (passing, DEFENSE, scky) -> def_sack_yards. `scky` IS NOT PUBLISHED on passing/defense:
    # the inventory lists it only on passing/OFFENSE, where it is sack yards TAKEN by our own
    # offence, not sack yards gained by our defence. Flipping the plane from idp to def
    # produced NO ROWS a second time, which is the tell that the AUTHORITY side was empty
    # rather than the subject side. Offence-side scky needs a sacks-taken canonical, which is
    # a separate question.

    # ---- interceptions and passes defended, defence side ----
    ("INT_DEF", "passing", "defense", "int", "def_interceptions", "idp"),
    ("PDEF_DEF", "receiving", "defense", "pdef", "def_pass_defended", "idp"),

    # ---- fumbles by phase ----
    ("FUM_RUSH", "rushing", "offense", "rush_fum", "rushing_fumbles", "idp"),
    ("FUM_RECV", "receiving", "offense", "rec_fum", "receiving_fumbles", "idp"),

    # ---- returns: volume and yardage ----
    ("KRET", "kickoff-returns", "special-teams", "ret", "kickoff_returns", "idp"),
    ("KRET_YDS", "kickoff-returns", "special-teams", "yds", "kickoff_return_yards", "idp"),
    ("PRET", "punt-returns", "special-teams", "ret", "punt_returns", "idp"),
    ("PRET_YDS", "punt-returns", "special-teams", "yds", "punt_return_yards", "idp"),

    # ---- blocked kicks: ONE of the two survived its own measurement ----
    #
    # field-goals.fg_blk is OUR KICKER'S attempts that got blocked, and fg_blocked is that
    # subject. 49.9% clean on a base of 748 -- a population problem, not an identity one.
    ("FG_BLK", "field-goals", "special-teams", "fg_blk", "fg_blocked", "idp"),
    #
    # PUNT_BLK IS WITHDRAWN, and the reason was already written down and then ignored. An
    # earlier adjudication in this very program recorded that "p_blk is on the punt-RETURN
    # side: blocks BY this team, not punts OF this team" -- and the family stayed mapped to
    # `punts_blocked`, which is OUR punts that got blocked. Two different subjects. The
    # non-zero base is what forced the issue: it read 70.7% while the zero rows were voting,
    # and 18.4% once they stopped. A correct mapping does not measure 18.4%.
    #
    # THE REPLACEMENT IS NOT def_blk_kick EITHER, and that was measured too rather than
    # substituted: def_blk_kick beats fg_blocked on the return page (55.3% vs 12.2%) but runs
    # EXCESS against any single block type, because it is an UNDIFFERENTIATED total while
    # NFL.com splits blocks into fg_blk / xp_blk / p_blk. So the natural equation was tested --
    # fg_blk + xp_blk + p_blk == def_blk_kick -- and it fails at 45.1% exact on 1,622
    # franchise-seasons, now SHORT (median 2 against our 1). def_blk_kick is therefore both
    # under-populated AND undifferentiated, which makes all three source columns candidates for
    # three separate canonicals rather than mappings onto one.

    # ---- field-goal attempts ----
    ("FG_ATT", "field-goals", "special-teams", "att", "fg_att", "idp"),

    # =====================================================================================
    # SEVEN MORE, and every one of them was sitting in the "STILL OPEN" bucket because that
    # bucket was built by PATTERN MATCHING A COLUMN NAME instead of by searching the schema.
    # sacks_suffered, sack_yards_lost, def_plays and def_explosive_pass_allowed were all
    # there the whole time. This is the fourth repetition of the same lesson: a family is
    # absent only after the schema has been SEARCHED, never after a name has been guessed.
    # =====================================================================================

    # ---- sacks TAKEN by our own offence ----
    # SACKY_DEF was removed earlier for the right reason (scky is not published on the defence
    # side) and the OFFENCE side was then never tried. Both halves reconcile immediately.
    ("SACK_OFF", "passing", "offense", "sck", "sacks_suffered", "idp"),
    ("SACKY_OFF", "passing", "offense", "scky", "sack_yards_lost", "idp"),

    # ---- scrimmage plays: an EQUATION, and the contrast is the identification ----
    # attempts + carries alone gives 0.0% clean with 1,386 of 1,393 SHORT. Adding sacks gives
    # 94.3%. A sack is a scrimmage play, and the failed form is what proves it -- had only the
    # second been measured, 94.3% would have looked like a mediocre mapping instead of a
    # settled definition.
    ("SCRM_OFF", "downs", "offense", "scrm_plys",
     "COALESCE(attempts,0) + COALESCE(carries,0) + COALESCE(sacks_suffered,0)", "idp"),
    ("SCRM_DEF", "downs", "defense", "scrm_plys", "def_plays", "def"),

    # ---- the receiving/defense page republishes the passing page's volume, onto canonicals
    # that are semantically exact rather than merely close ----
    # receiving/defense.yds reconciles at 99.1% against BOTH def_completion_yards_allowed and
    # passing_yds_allowed (identical medians of 3,393), which is the pass/receive mirror on the
    # allowed side. The completion-yards canonical is chosen because it names this subject.
    ("ALW_RECV_REC", "receiving", "defense", "rec", "def_completions_allowed", "def"),
    ("ALW_RECV_YDS", "receiving", "defense", "yds", "def_completion_yards_allowed", "def"),

    # ---- explosive pass allowed: THE THRESHOLD IS NOW MEASURED, NOT ASSUMED ----
    # def_explosive_pass_allowed reconciles with the 20-yard column at 96.6% and with the
    # 40-yard column at 0.0% (median authority 8 against our 46, all excess). So the canonical
    # is a 20-yard threshold as a matter of measurement. The same test refuses the rushing
    # analogue: rushing/defense `20` against def_explosive_rush_allowed is 0.0%, median 9
    # against our 47, which confirms rush_explosive is the TEN-yard threshold Joe flagged.
    ("ALW_P20_PASS", "passing", "defense", "20", "def_explosive_pass_allowed", "def"),
    # The receiving/defense page republishes the same pass-allowed 20+ concept. Keep its
    # source row as an independent family so the apparent duplicate is measured rather than
    # silently disappearing behind passing/defense. Equality (or divergence) is part of the
    # receipt; it is not assumed from the column name.
    ("ALW_P20_RECV", "receiving", "defense", "20", "def_explosive_pass_allowed", "def"),

    # ---- tot_td: THE IDENTIFICATION QUESTION IS CLOSED BY THE SAME CONTRAST METHOD ----
    #
    # This was left unwired with the note "63.21% is still far from a mapping, QUEUED as an
    # identification question", and the ladder settles what the subject is:
    #
    #     rushing + receiving                          14.8% clean, 1,781 of 2,101 SHORT
    #     + kickoff and punt return TDs                22.9%, 1,602 short
    #     + interception and fumble return TDs         66.6%, 310 short / 392 excess
    #     total_tds_scored as it stands                        67.6%, 319 short / 392 excess
    #
    # The shortfall collapses only when DEFENSIVE return TDs are added, so tot_td is every
    # touchdown the franchise scored, not its offensive ones -- which is also why it equalled
    # rsh_td + rec_td on just 11.54% of team-seasons. And `total_tds_scored` already carries that
    # subject: identical median (36 against 36) with the errors BALANCED rather than one-sided.
    # A one-sided residual is a population gap; a balanced one on a matching median is a value
    # problem. So this is MAPPED with a 67.6% value defect, not an open identity.
    #
    # The 63.21% quoted earlier was measured before the non-zero base and the deterministic
    # key; the honest figure on a real base is 67.6%.
    ("SC_OFF_TOT_TD", "scoring", "offense", "tot_td", "total_tds_scored", "idp"),
]


def _norm(fam: tuple) -> tuple:
    """(label, cat, side, col, canonical, plane, agg, src_expr) with the tail defaulted.

    Kept as tuples rather than promoted to a dataclass on purpose: every entry in FAMILIES is
    a decision with its evidence in the comment above it, and a mechanical refactor of 60
    literals is exactly the kind of churn that loses a comment.
    """
    label, cat, side, col, canonical, plane = fam[:6]
    agg = fam[6] if len(fam) > 6 else "SUM"
    src = fam[7] if len(fam) > 7 else f'TRY_CAST(ts."{col}" AS DOUBLE)'
    return label, cat, side, col, canonical, plane, agg, src


#: (label, category, side, column, OURS EXPRESSION, plane) -- the RATE class.
#:
#: A RATE CANNOT BE SUMMED, so these cannot be conservation families: NFL.com's `ypc` is one
#: number per team-season and the sum of its players' ypc is meaningless. They are witnessed by
#: RECOMPUTE instead -- ratio of the summed operands, which is the same law
#: `audit_capacity_runner` applies at player grain, moved to (franchise, season).
#:
#: THE IDENTITIES ARE MEASURED, NOT ASSUMED, and they agree with the conventions already
#: receipted at player grain (docs/nflcom-splits-layout-identity.json put `1st_2 = 1st/att` at
#: 99.391% of 69,257 rows). Browns 2024 checks every form: cmp_2 59.8 == 395/661, 1st_2 27.2 ==
#: 180/661, yds_att 5.9 == 3879/661; Eagles rush_1st_2 26.4 == 164/621; Bengals yds_rec 10.7 ==
#: 4918/460; Steelers fg 93.2 == 41/44.
#:
#: The Tier 1 allowed rates are recomputed only after the DEF-row numerators and denominators
#: have been materialized from PBP. The source columns remain separate families even where two
#: NFL.com pages publish the same equation; an apparent duplicate is measured by its own row
#: and can become DUPLICATE_OF only after that comparison is receipted.
RATE_FAMILIES = [
    ("R_PASS_CMP_PCT", "passing", "offense", "cmp_2",
     "100.0 * SUM(completions) / NULLIF(SUM(attempts), 0)", "idp"),
    ("R_PASS_FD_PCT", "passing", "offense", "1st_2",
     "100.0 * SUM(passing_first_downs) / NULLIF(SUM(attempts), 0)", "idp"),
    ("R_PASS_YPA", "passing", "offense", "yds_att",
     "SUM(passing_yards) / NULLIF(SUM(attempts), 0)", "idp"),
    # PASSER RATING is the one rate that is not a ratio: it is the four-component NFL formula,
    # each term clamped to [0, 2.375]. Included deliberately -- it witnesses completions,
    # attempts, yards, TDs and interceptions JOINTLY, so a single number failing implicates a
    # set no individual ratio can. It is also era-loaded (the formula dates to 1973 and is
    # applied retroactively), which the era tolerance already handles.
    ("R_PASS_RATING", "passing", "offense", "rate",
     "100.0 / 6.0 * ("
     " LEAST(GREATEST((SUM(completions) / NULLIF(SUM(attempts), 0) - 0.3) * 5, 0), 2.375)"
     "+LEAST(GREATEST((SUM(passing_yards) / NULLIF(SUM(attempts), 0) - 3) * 0.25, 0), 2.375)"
     "+LEAST(GREATEST(SUM(passing_tds) / NULLIF(SUM(attempts), 0) * 20, 0), 2.375)"
     "+LEAST(GREATEST(2.375 - SUM(passing_interceptions) / NULLIF(SUM(attempts), 0) * 25, 0),"
     " 2.375))", "idp"),
    ("R_RUSH_YPC", "rushing", "offense", "ypc",
     "SUM(rushing_yards) / NULLIF(SUM(carries), 0)", "idp"),
    ("R_RUSH_FD_PCT", "rushing", "offense", "rush_1st_2",
     "100.0 * SUM(rushing_first_downs) / NULLIF(SUM(carries), 0)", "idp"),
    ("R_RECV_YPR", "receiving", "offense", "yds_rec",
     "SUM(receiving_yards) / NULLIF(SUM(receptions), 0)", "idp"),
    ("R_RECV_FD_PCT", "receiving", "offense", "rec_1st_2",
     "100.0 * SUM(receiving_first_downs) / NULLIF(SUM(receptions), 0)", "idp"),
    # THE DOWNS PAGE REPUBLISHES TWO OF THESE. downs/offense.rec_1st_2 and rush_1st_2 are the
    # same quantities as the receiving and rushing pages'. They are measured here rather than
    # dropped as assumed duplicates -- the four `td` duplicates on the scoring page were
    # PROVEN equal (2,228 of 2,228) before being excluded, and the same proof is owed here. If
    # they come back byte-identical they get a DUPLICATE_OF ruling; if they diverge, the
    # divergence is the finding.
    ("R_DOWNS_RECV_FD_PCT", "downs", "offense", "rec_1st_2",
     "100.0 * SUM(receiving_first_downs) / NULLIF(SUM(receptions), 0)", "idp"),
    ("R_DOWNS_RUSH_FD_PCT", "downs", "offense", "rush_1st_2",
     "100.0 * SUM(rushing_first_downs) / NULLIF(SUM(carries), 0)", "idp"),
    ("R_FG_PCT", "field-goals", "special-teams", "fg",
     "100.0 * SUM(fg_made) / NULLIF(SUM(fg_att), 0)", "idp"),
    ("R_XP_PCT", "scoring", "special-teams", "xp_pct",
     "100.0 * SUM(pat_made) / NULLIF(SUM(pat_att), 0)", "idp"),
    ("R_KRET_AVG", "kickoff-returns", "special-teams", "avg",
     "SUM(kickoff_return_yards) / NULLIF(SUM(kickoff_returns), 0)", "idp"),
    ("R_PRET_AVG", "punt-returns", "special-teams", "avg",
     "SUM(punt_return_yards) / NULLIF(SUM(punt_returns), 0)", "idp"),
    # THE ONE DEFENCE-SIDE RATE THAT IS NOT BLOCKED: yards-per-reception allowed needs
    # def_completion_yards_allowed over def_completions_allowed, and v26 has both.
    ("R_ALW_RECV_YPR", "receiving", "defense", "yds_rec",
     "SUM(def_completion_yards_allowed) / NULLIF(SUM(def_completions_allowed), 0)", "def"),
    ("R_ALW_PASS_FD_PCT", "passing", "defense", "1st_2",
     "100.0 * SUM(passing_first_downs_allowed) / NULLIF(SUM(def_attempts_allowed), 0)", "def"),
    ("R_ALW_PASS_CMP_PCT", "passing", "defense", "cmp_2",
     "100.0 * SUM(def_completions_allowed) / NULLIF(SUM(def_attempts_allowed), 0)", "def"),
    ("R_ALW_PASS_YPA", "passing", "defense", "yds_att",
     "SUM(passing_yds_allowed) / NULLIF(SUM(def_attempts_allowed), 0)", "def"),
    ("R_ALW_PASS_RATING", "passing", "defense", "rate",
     "100.0 / 6.0 * (LEAST(GREATEST((SUM(def_completions_allowed) / NULLIF(SUM(def_attempts_allowed), 0) - 0.3) * 5, 0), 2.375) + LEAST(GREATEST((SUM(passing_yds_allowed) / NULLIF(SUM(def_attempts_allowed), 0) - 3) * 0.25, 0), 2.375) + LEAST(GREATEST(SUM(passing_tds_allowed) / NULLIF(SUM(def_attempts_allowed), 0) * 20, 0), 2.375) + LEAST(GREATEST(2.375 - SUM(def_interceptions) / NULLIF(SUM(def_attempts_allowed), 0) * 25, 0), 2.375))", "def"),
    ("R_ALW_RUSH_FD_PCT", "rushing", "defense", "rush_1st_2",
     "100.0 * SUM(rushing_first_downs_allowed) / NULLIF(SUM(def_carries_allowed), 0)", "def"),
    ("R_ALW_RUSH_YPC", "rushing", "defense", "ypc",
     "SUM(rushing_yds_allowed) / NULLIF(SUM(def_carries_allowed), 0)", "def"),
    ("R_ALW_RECV_FD_PCT", "receiving", "defense", "rec_1st_2",
     "100.0 * SUM(receiving_first_downs_allowed) / NULLIF(SUM(def_completions_allowed), 0)", "def"),
    ("R_ALW_DOWNS_RECV_FD_PCT", "downs", "defense", "rec_1st_2",
     "100.0 * SUM(receiving_first_downs_allowed) / NULLIF(SUM(def_completions_allowed), 0)", "def"),
    ("R_ALW_DOWNS_RUSH_FD_PCT", "downs", "defense", "rush_1st_2",
     "100.0 * SUM(rushing_first_downs_allowed) / NULLIF(SUM(def_carries_allowed), 0)", "def"),
]

#: PLANE -> the position predicate on the v26 player side.
PLANE_PRED = {
    "all": "",                                  # no filter: preserves the original 11 exactly
    "idp": " AND position <> 'DEF'",             # a franchise's players
    "def": " AND position = 'DEF'",              # the single team-defence row
}

#: |delta| as a share of the authority total, per era -- the same shape recon_conservation
#: uses, and for the same reason: pre-1950 sourcing slack is documented, modern years must
#: reconcile tightly.
ERA_TOL = [(1920, 1945, 0.20), (1946, 1949, 0.08), (1950, 1977, 0.02), (1978, 2100, 0.005)]


def _tol(year: int) -> float:
    for lo, hi, t in ERA_TOL:
        if lo <= year <= hi:
            return t
    return 0.005


def _receipt_or_die() -> dict:
    """The lane refuses to run on an unreceipted key. Proof-or-pending applies to the JOIN
    as much as to the mapping: a fingerprint that has not been measured this build could
    silently attach a team's totals to the wrong franchise."""
    doc = json.loads(RECEIPTS.read_text(encoding="utf-8"))
    for r in doc["receipts"]:
        if r["receipt_id"] == "nflcom_team_fid":
            if r["status"] != "PASS":
                raise SystemExit(f"nflcom_team_fid receipt is {r['status']}, not PASS")
            return r
    raise SystemExit("nflcom_team_fid receipt absent -- run team_key_crosswalk first")


def run(year: int | None = None, csv: str | None = None) -> dict:
    receipt = _receipt_or_die()
    v26 = Path(S.latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET memory_limit='6GB'")
    yfilter = f"AND TRY_CAST(season AS INT) = {year}" if year else ""
    try:
        con.execute(f"""CREATE TABLE ts AS SELECT *,
          CASE WHEN length(team) % 2 = 1
                AND substr(team, 1, CAST((length(team)-1)/2 AS BIGINT))
                  = substr(team, CAST((length(team)+3)/2 AS BIGINT))
               THEN substr(team, 1, CAST((length(team)-1)/2 AS BIGINT)) ELSE team END AS nick,
          TRY_CAST(season AS INT) AS yr
          FROM read_parquet('{STATS_PATH}', union_by_name=True)
          WHERE season_type='reg' {yfilter}""")
        con.execute(f"""CREATE TABLE mine AS
          SELECT CAST(nfl_franchise_number AS INT) fid, year yr, SUM(passing_yards) v
          FROM read_parquet('{v26}') WHERE season_type='REG'
            AND nfl_franchise_number IS NOT NULL GROUP BY 1, 2""")
        # the receipted key, recomputed from the same fingerprint the receipt measured
        con.execute("""CREATE TABLE keymap AS
          SELECT n.nick, n.yr, m.fid FROM
            (SELECT nick, yr, SUM(TRY_CAST(pass_yds AS DOUBLE)) v FROM ts
             WHERE _category='passing' AND _side='offense' GROUP BY 1, 2) n
          JOIN mine m ON m.yr = n.yr
          WHERE n.v IS NOT NULL AND m.v IS NOT NULL AND ABS(n.v - m.v) <= 1
          -- fid IS THE TIE-BREAK AND IT IS NOT COSMETIC. ABS(n.v - m.v) alone leaves ties
          -- unordered, so the keyed count drifted run to run (2,105 / 2,103 / 2,101) and every
          -- family's clean rate moved with it -- PASS_CMP read 99.9% and 100.0% on identical
          -- inputs. A lane whose numbers change without its inputs changing cannot receipt
          -- anything.
          QUALIFY ROW_NUMBER() OVER (PARTITION BY n.nick, n.yr
                                     ORDER BY ABS(n.v - m.v), m.fid) = 1""")
        # AN AMBIGUOUS KEY MUST REFUSE, NOT PICK. The QUALIFY above makes the map unique per
        # (nick, yr) and enforces NOTHING per (fid, yr) -- so two DIFFERENT teams whose season
        # passing yards fall within a yard of the same franchise both attach to it, and the
        # authority CTE (which groups SUM(col) BY fid, yr) then adds their totals together.
        #
        # MEASURED: 20 of 2,125 franchise-seasons collide, and the collisions are not spelling
        # variants -- fid=7/1982 pulls in Packers, Seahawks AND Redskins (1982 was
        # strike-shortened, so season totals bunched), fid=2/2025 pulls Giants and Falcons.
        # One collided franchise-season was flagged in 44 of 49 families with an authority
        # exactly 2x ours, which is what set the 96.8% ceiling on ~24 families and read as a
        # per-column quality problem when it was one key defect reported 24 times.
        #
        # Dropped rather than disambiguated: picking the closest match would be inventing a
        # resolution the fingerprint cannot support, and 20 refusals are cheap against 2,125.
        con.execute("""DELETE FROM keymap WHERE (fid, yr) IN
          (SELECT fid, yr FROM keymap GROUP BY 1, 2 HAVING COUNT(*) > 1)""")
        ambiguous = con.execute("""SELECT COUNT(*) FROM (SELECT fid, yr FROM keymap
          GROUP BY 1, 2 HAVING COUNT(*) > 1)""").fetchone()[0]
        assert ambiguous == 0, f"{ambiguous} ambiguous (fid, yr) survived the refusal"
        keyed = con.execute("SELECT COUNT(*) FROM keymap").fetchone()[0]

        rows = []
        for fam in FAMILIES:
            label, cat, side, col, canonical, plane, agg, src = _norm(fam)
            try:
                out = con.execute(f"""
                  WITH team AS (
                    SELECT k.fid, k.yr, {agg}({src}) AS authority
                    FROM ts JOIN keymap k ON k.nick = ts.nick AND k.yr = ts.yr
                    WHERE ts._category='{cat}' AND ts._side='{side}'
                      AND ts."{col}" IS NOT NULL GROUP BY 1, 2),
                       -- CAST TO DOUBLE. Several *_allowed columns are DECIMAL in the
                       -- parquet, so SUM() returns decimal.Decimal and the tolerance test
                       -- `abs(ours - authority)` raises TypeError against a float authority.
                       -- The cast also lets `canonical` be an EXPRESSION (the 2pt equation).
                       players AS (
                    SELECT CAST(nfl_franchise_number AS INT) fid, year yr,
                           {agg}(CAST(({canonical}) AS DOUBLE)) AS ours
                    FROM read_parquet('{v26}') WHERE season_type='REG'
                      AND nfl_franchise_number IS NOT NULL
                      {PLANE_PRED[plane]} GROUP BY 1, 2)
                  SELECT t.fid, t.yr, t.authority, p.ours
                  FROM team t JOIN players p ON p.fid = t.fid AND p.yr = t.yr
                  WHERE t.authority IS NOT NULL AND p.ours IS NOT NULL""").fetchall()
            except Exception as exc:
                rows.append(dict(stat=label, verdict="BROKEN",
                                 detail=str(exc).splitlines()[0][:90]))
                continue
            # THE ZERO-AUTHORITY ROWS ARE NOT EVIDENCE, AND COUNTING THEM AS CLEAN INFLATES
            # EVERY SPARSE FAMILY. A team-season where NFL.com publishes 0 cannot be tested by
            # a relative tolerance (0.5% of 0 is 0), so it is skipped from flagging -- but it
            # was still sitting in the denominator, which made `pct_clean` a measure of how
            # RARE a stat is rather than how well it reconciles. FGB_60 read 100.0% on 2,103
            # team-seasons against 99.18% exact measured by hand, and the whole difference was
            # ~2,000 zero rows voting clean. The naive number is kept beside the real one
            # because the same inflation is latent in FG_BLK, SC_DEF_SFTY and every other
            # sparse family that was reported before this was noticed.
            flagged, base = [], 0
            for fid, yr, authority, ours in out:
                if not authority:
                    continue
                base += 1
                if abs(ours - authority) / authority > _tol(int(yr)):
                    flagged.append(dict(fid=int(fid), year=int(yr),
                                        authority=float(authority), ours=float(ours),
                                        delta=float(ours - authority)))
            short = sum(1 for f in flagged if f["delta"] < 0)
            rows.append(dict(stat=label, plane=plane, agg=agg, team_seasons=len(out),
                             base_nonzero=base, flagged=len(flagged),
                             shortfall=short, excess=len(flagged) - short,
                             pct_clean=round(100.0 * (base - len(flagged)) / base, 2)
                             if base else None,
                             pct_clean_naive=round(100.0 * (len(out) - len(flagged))
                                                   / len(out), 2) if out else None,
                             worst=sorted(flagged, key=lambda f: -abs(f["delta"]))[:5]))

        # ---------------- the RATE pass: recompute, never sum ----------------
        rate_rows = []
        for label, cat, side, col, ours_expr, plane in RATE_FAMILIES:
            try:
                out = con.execute(f"""
                  WITH team AS (
                    -- MAX, not SUM: the authority is ONE published number per franchise-season
                    -- and summing it across the source's rows would multiply a percentage.
                    SELECT k.fid, k.yr, MAX(TRY_CAST(ts."{col}" AS DOUBLE)) AS authority
                    FROM ts JOIN keymap k ON k.nick = ts.nick AND k.yr = ts.yr
                    WHERE ts._category='{cat}' AND ts._side='{side}'
                      AND ts."{col}" IS NOT NULL GROUP BY 1, 2),
                       players AS (
                    SELECT CAST(nfl_franchise_number AS INT) fid, year yr,
                           CAST(({ours_expr}) AS DOUBLE) AS ours
                    FROM read_parquet('{v26}') WHERE season_type='REG'
                      AND nfl_franchise_number IS NOT NULL
                      {PLANE_PRED[plane]} GROUP BY 1, 2)
                  SELECT t.fid, t.yr, t.authority, p.ours
                  FROM team t JOIN players p ON p.fid = t.fid AND p.yr = t.yr
                  WHERE t.authority IS NOT NULL AND p.ours IS NOT NULL""").fetchall()
            except Exception as exc:
                rate_rows.append(dict(stat=label, verdict="BROKEN",
                                      detail=str(exc).splitlines()[0][:90]))
                continue
            flagged = []
            for fid, yr, authority, ours in out:
                # PUBLISHED PRECISION IS PART OF THE TEST. Every one of these is printed to one
                # decimal, so a recompute can differ by up to 0.05 purely from the source's own
                # rounding and calling that a defect would manufacture failures. The era
                # tolerance still applies on top, because the underlying counts carry the same
                # era slack the count families do.
                if abs(ours - authority) > max(0.05, _tol(int(yr)) * abs(authority)):
                    flagged.append(dict(fid=int(fid), year=int(yr),
                                        authority=float(authority), ours=float(ours),
                                        delta=float(ours - authority)))
            short = sum(1 for f in flagged if f["delta"] < 0)
            rate_rows.append(dict(stat=label, plane=plane, team_seasons=len(out),
                                  flagged=len(flagged), shortfall=short,
                                  excess=len(flagged) - short,
                                  pct_clean=round(100.0 * (len(out) - len(flagged))
                                                  / len(out), 2) if out else None,
                                  worst=sorted(flagged, key=lambda f: -abs(f["delta"]))[:5]))
    finally:
        con.close()

    report = {
        "lane": "nflcom (franchise, season) team conservation",
        "law": "SUM over a franchise-season of its players == the NFL.com team total. A "
               "SHORTFALL means a player or a whole player-season is missing from our side; "
               "an EXCESS means a duplicated or misattributed row. The game-grain lanes "
               "cannot see a season-long absence and the league-year lane averages it away "
               "across 32 teams.",
        "key_receipt": {"id": receipt["receipt_id"], "status": receipt["status"],
                        "coverage": receipt["measured"]["coverage"]},
        "keyed_team_seasons": keyed,
        "season_type": "reg only -- the post half of this source is a 100.00% duplicate",
        "families": rows,
        "rate_law": "A RATE IS RECOMPUTED, NEVER SUMMED. ours = f(SUM(operands)) over the "
                    "franchise's players, compared against the one published number, with the "
                    "source's own one-decimal rounding allowed for.",
        "rate_families": rate_rows,
    }
    if csv:
        import csv as _csv
        with open(csv, "w", newline="", encoding="utf-8") as fh:
            w = _csv.writer(fh)
            w.writerow(["stat", "fid", "year", "authority", "ours", "delta"])
            for r in rows + rate_rows:
                for f in r.get("worst", []):
                    w.writerow([r["stat"], f["fid"], f["year"], f["authority"],
                                f["ours"], f["delta"]])
    return report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int)
    ap.add_argument("--csv")
    args = ap.parse_args()
    rep = run(args.year, args.csv)
    print(f"{rep['lane']}  |  key {rep['key_receipt']['id']} "
          f"[{rep['key_receipt']['status']}] {rep['key_receipt']['coverage']:.1%}  |  "
          f"{rep['keyed_team_seasons']:,} team-seasons keyed")
    _table("COUNT families (conservation: SUM/MAX of players == team total)", rep["families"])
    _table("RATE families (recompute: f(SUM(operands)) == published rate)",
           rep["rate_families"])
    return 0


def _table(title: str, rows: list) -> None:
    print(f"\n=== {title} ===")
    print(f"{'stat':22s} {'agg':>4s} {'joined':>8s} {'base':>8s} {'clean':>8s} {'naive':>8s} "
          f"{'flagged':>8s} {'short':>7s} {'excess':>7s}")
    for r in rows:
        if r.get("verdict") == "BROKEN":
            print(f"{r['stat']:22s} BROKEN {r['detail']}")
            continue
        # A ZERO-ROW FAMILY MUST NOT HIDE EVERY FAMILY AFTER IT. pct_clean is None when the
        # join produced nothing (a canonical that is NULL on the declared plane, say), and the
        # old `{...:>7.1f}` raised TypeError mid-report -- so 20 of 52 families silently
        # vanished from the output and looked unwired rather than empty.
        #
        # AND THERE ARE NOW TWO WAYS TO HAVE NO BASE, which the message distinguishes: no
        # joined rows at all (a plane/name defect) versus joined rows that are ALL ZERO on the
        # authority side (a stat NFL.com publishes but never non-zero in our joined span). The
        # second reads as a clean 100% under the old denominator.
        if r.get("pct_clean") is None:
            why = (f"canonical is NULL on plane={r.get('plane')!r}" if not r["team_seasons"]
                   else f"authority is ZERO on all {r['team_seasons']:,} joined rows")
            print(f"{r['stat']:22s} {r.get('agg', 'SUM'):>4s} {r['team_seasons']:>8,} "
                  f"{r.get('base_nonzero', 0):>8,}   NO BASE  ({why})")
            continue
        base = r.get("base_nonzero", r["team_seasons"])
        naive = r.get("pct_clean_naive")
        print(f"{r['stat']:22s} {r.get('agg', 'SUM'):>4s} {r['team_seasons']:>8,} "
              f"{base:>8,} {r['pct_clean']:>7.1f}% "
              f"{('%.1f%%' % naive) if naive is not None else '-':>8s} "
              f"{r['flagged']:>8,} {r['shortfall']:>7,} {r['excess']:>7,}")


if __name__ == "__main__":
    raise SystemExit(main())
