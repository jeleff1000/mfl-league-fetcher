"""
sota_recon/offense_recipe.py -- the ONE canonical offensive scoring recipe (SQL).

Single source of truth for the baked offensive fantasy formula, imported by BOTH the rescore builder
(build_rescore_fpts_v26) and the GATE (golden_points). Centralizing it means a scoring change (e.g. the
fumble GREATEST(total,split) fix) is edited in ONE place and the rescore + the gate move in lockstep --
the gate can then assert an EXACT match, so a fumble/return-TD double-count or a wrong weight fails loudly.

No heavy imports (pure SQL strings) so the gate stays runnable independent of the calculator.
"""
from __future__ import annotations

# Offensive-eligibility predicate: any of QB/RB/WR/TE/K among the (comma-list) position tokens.
OFF_ELIG = (
    "list_has_any("
    "list_transform(string_split(UPPER(COALESCE(position, '')), ','), x -> trim(x)), "
    "['QB','RB','WR','TE','K']"
    ")"
)

# Lost fumbles for an offensive player -- CANONICAL = GREATEST(total column, rush/sack/rec SPLIT), which
# is two-way safe: the split MISSES return/aborted-snap lost fumbles the `fumbles_lost` total carries
# (1,256 rows total>split) while the total UNDER-reports in ~5.5k rows (split>total). Taking the max never
# drops a real lost fumble, never double-counts. Applied (A3) as an ADDITIVE residual on the standard
# composites only (calculator pts_fum_residual): pts_rush/pts_rec + the per-league correction baseline are
# UNTOUCHED, so per-league totals stay invariant. `_SPLIT` kept for reference / the old baseline.
LOST_FUMBLES_SPLIT = """
(
  COALESCE(TRY_CAST(rushing_fumbles_lost AS DOUBLE), 0)
  + COALESCE(TRY_CAST(sack_fumbles_lost AS DOUBLE), 0)
  + COALESCE(TRY_CAST(receiving_fumbles_lost AS DOUBLE), 0)
)
"""

LOST_FUMBLES = f"""
GREATEST(
  COALESCE(TRY_CAST(fumbles_lost AS DOUBLE), 0),
  {LOST_FUMBLES_SPLIT}
)
"""
LOST_FUMBLES_GREATEST = LOST_FUMBLES  # back-compat alias

# Canonical fpts_4pt_half: pass(4pt) + rush + rec(half) + 2pt + own return TDs + kicker.
EXPECTED_4PT_HALF = f"""
(
  COALESCE(TRY_CAST(passing_yards AS DOUBLE), 0) * 0.04
  + COALESCE(TRY_CAST(passing_tds AS DOUBLE), 0) * 4
  - COALESCE(TRY_CAST(passing_interceptions AS DOUBLE), 0) * 2
  + COALESCE(TRY_CAST(rushing_yards AS DOUBLE), 0) * 0.1
  + COALESCE(TRY_CAST(rushing_tds AS DOUBLE), 0) * 6
  + COALESCE(TRY_CAST(receiving_yards AS DOUBLE), 0) * 0.1
  + COALESCE(TRY_CAST(receiving_tds AS DOUBLE), 0) * 6
  + COALESCE(TRY_CAST(receptions AS DOUBLE), 0) * 0.5
  - ({LOST_FUMBLES}) * 2
  + (
      COALESCE(TRY_CAST(passing_2pt_conversions AS DOUBLE), 0)
      + COALESCE(TRY_CAST(rushing_2pt_conversions AS DOUBLE), 0)
      + COALESCE(TRY_CAST(receiving_2pt_conversions AS DOUBLE), 0)
    ) * 2
  + COALESCE(TRY_CAST(special_teams_tds AS DOUBLE), 0) * 6
  + COALESCE(TRY_CAST(fum_ret_td AS DOUBLE), 0) * 6
  + COALESCE(TRY_CAST(pts_k_std AS DOUBLE), 0)
)
"""

FIRST_DOWN_BONUS = """
(
  COALESCE(TRY_CAST(rushing_first_downs AS DOUBLE), 0)
  + COALESCE(TRY_CAST(receiving_first_downs AS DOUBLE), 0)
) * 0.5
"""
