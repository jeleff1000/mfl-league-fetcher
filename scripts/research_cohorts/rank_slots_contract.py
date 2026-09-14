"""rank_slots_contract.py -- the LOCKED measurement contract for start-rate proliferation.

One place for the rules that every start-rate measurement must follow. Each was established
empirically on 2026-07-29 and each has a receipt; several were discovered by getting them
wrong first, so they are enforced here rather than left to the caller to remember.

    from rank_slots_contract import ELIGIBLE_SQL, POOLABLE, NOT_POOLABLE, slots_expr

THE MODEL
    rank   = a player's within-week start-rate rank, pooled across every league variable
             except best ball (rank corr 0.95-0.99 across scoring / teams / roster / ltype)
    slots  = total league-wide starting slots at the position
    start% = TABLE[rank][slots]                       (+-5-8 points, no per-player fit)

At gap = slots - rank = 0 the measured start rate is 51.4% -- the marginal starter is a coin
flip, which is what the model predicts and is an out-of-sample check on the whole framing.
"""
from __future__ import annotations

# --------------------------------------------------------------------------------------
# ELIGIBILITY: a REG row in the super table, and nothing else.
#
# Do NOT add a snap or touch threshold. Measured 2026-07-29 on 2025 QB weeks: filtering to
# `offense_snaps>=20 OR attempts>=10` leaves the rank x slots table IDENTICAL to one decimal
# at every rank 1-24, keeps 97.1% of ranks unchanged, and shifts none by more than 2 --
# inactives self-sort to the bottom and never reach the ranks the table reads.
#
# A threshold also costs a decade: offense_snaps is NULL for EVERY row before 2012, so
# `snaps>=20` does not filter inactive players pre-2012, it filters ALL of them. With plain
# row-presence the corpus ranks back to 2003 (28-9,560 leagues/week, 17 weeks/season).
#
# Touch-based proxies were tried and rejected for skill positions: best F1 was RB
# carries>=5 at 81.2, and the proxy finds 3,929 active player-weeks in 2012 where snaps find
# 6,599 -- a 40% undercount, because a WR who played 45 snaps and drew no targets was still
# startable. Row presence has none of that: 96-99.7% of rows carry non-zero snaps, and the
# per-week row counts (38 QB / 100 RB / 97 TE / 154 WR) are dressed-and-played numbers.
# --------------------------------------------------------------------------------------
ELIGIBLE_SQL = """
    SELECT NFL_player_id, year, week
    FROM ops.nfl_historical.nfl_player_stats_all
    WHERE season_type = 'REG' AND position = '{pos}'
    GROUP BY 1, 2, 3
"""

# Optional refinement, ONLY for absolute start% of a marginal player -- never for ranking.
# Flacco 2024 wk8 has a 1-snap relief appearance no manager could have started.
QB_ACTIVE_REFINEMENT = "(COALESCE(offense_snaps,0) >= 20 OR COALESCE(attempts,0) >= 10)"

# --------------------------------------------------------------------------------------
# POOLING: rank is invariant on every league variable except lineup mode.
# Measured 2022-2025, ~2,000 QB-weeks per contrast, within-week rank computed independently
# on each side.
# --------------------------------------------------------------------------------------
POOLABLE = {
    # dimension            rank_corr  mean|rank diff|  top-12 agreement
    "teams (10 vs 12)":       (0.9893, 0.80, 97.0),
    "scoring (ppr vs std)":   (0.9809, 1.11, 96.8),
    "ltype (keeper vs dyn)":  (0.9594, 1.68, 93.4),
    "roster (flx vs sflx)":   (0.9479, 2.02, 92.3),
}

# best ball ranks by what a QB SCORED, not by what managers believed -- retroactive
# auto-optimisation. It is a different ordering, not a correction factor, and needs its own
# rank AND its own table. Allen is 68.9% best ball vs 96.7% managed; Geno Smith is HIGHER in
# best ball (38.0 vs 34.2), i.e. the effect inverts by tier.
NOT_POOLABLE = {"lineup (managed vs best ball)": (0.4451, 7.43, 66.7)}

BEST_BALL_FILTER = "COALESCE(l.sleeper_best_ball, false) = false"


def slots_expr(pos: str, alias: str = "l") -> str:
    """Total league-wide starting slots at the position -- the axis start% scales on.

    QB is exact. Skill positions need the measured flex share, because a FLEX slot is
    contested: RB/WR/TE take .390/.532/.078 of it in standard, .355/.543/.103 in half,
    .325/.518/.157 in full PPR (measured on 264,337 of 296,968 manager-weeks that reconcile
    to the declared FLEX count, 89%).
    """
    if pos == "QB":
        return f"{alias}.num_teams * ({alias}.roster_QB + COALESCE({alias}.roster_SUPER_FLEX,0))"
    raise NotImplementedError(
        f"{pos}: slots needs the scoring-specific flex share; see FLEX_SHARE. The WR share is "
        "near-flat across scoring (.543 -> .518) and leaves an unexplained residual, so it "
        "must be re-derived before the skill positions are wired.")


FLEX_SHARE = {  # position -> scoring -> share of one FLEX slot
    "RB": {"std": 0.390, "half": 0.355, "ppr": 0.325},
    "WR": {"std": 0.532, "half": 0.543, "ppr": 0.518},
    "TE": {"std": 0.078, "half": 0.103, "ppr": 0.157},
}

# Grain. Season aggregation mixes regimes -- Drake Maye was a bench stash who became an MVP
# candidate, so his season number blends two different players and his fitted decay had the
# worst spread in the set. At week grain all 28 QB curves in 2025 wk10 are strictly monotone
# and Allen/Jackson reach exactly 100.0% at 28 slots.
GRAIN = "week"
