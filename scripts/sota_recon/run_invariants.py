"""
sota_recon/run_invariants.py  --  the CUMULATIVE invariant runner (WS6.1).

The lesson this exists to encode: wave43's gate passed at fix time, then later waves
reintroduced 17 bad dup groups; the Masterson row survived wave1. Invariants must be
re-checked after EVERY wave/build, not only inside the wave that first established them.

Runs the permanent invariant set (cheap count queries + golden_samples) and compares
against the recorded baseline:
  * a count ABOVE baseline = REGRESSION -> exit 1 (gate fails)
  * a count below baseline = improvement -> rerun with --rebase to ratchet the baseline down
Baselines only ratchet DOWN (toward zero); they never silently rise.

    python -m scripts.sota_recon.run_invariants            # check vs baseline
    python -m scripts.sota_recon.run_invariants --rebase   # accept improvements
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

from . import recon_bounds
from .sources import latest_v26

BASELINE = os.environ.get(
    "SOTA_INVARIANT_BASELINE",
    r"D:\league-history-data\nfl\derived\validation\sota_recon_master\INVARIANT_BASELINE.json",
)


def measure(src: str | None = None) -> dict:
    vq = Path(src or latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    dup_groups = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT player_week FROM '{vq}' WHERE player_week IS NOT NULL
        GROUP BY player_week HAVING COUNT(*) > 1)""").fetchone()[0]

    bad_dup_groups = con.execute(f"""
        WITH dups AS (SELECT player_week FROM '{vq}' WHERE player_week IS NOT NULL
                      GROUP BY player_week HAVING COUNT(*) > 1)
        SELECT COUNT(*) FROM (
          SELECT player_week, COUNT(*) AS n,
                 COUNT(DISTINCT (opponent_nfl_team, game_date)) AS dg,
                 COUNT(*) FILTER (WHERE game_date IS NULL OR nfl_position IS NULL) AS nullish
          FROM '{vq}' WHERE player_week IN (SELECT player_week FROM dups)
          GROUP BY player_week
        ) WHERE n <> dg OR nullish > 0""").fetchone()[0]

    # wave57 classes: malformed float-year keys, season_type vs schedule authority,
    # rows with no opponent (queues hold the residual; these only ratchet down)
    float_keys = con.execute(f"""
        SELECT COUNT(*) FROM '{vq}'
        WHERE regexp_matches(player_week, '_[0-9]{{4}}[.]0_')""").fetchone()[0]
    from .sources import TEAM_GAMES
    tgp = Path(TEAM_GAMES.path).as_posix() if hasattr(TEAM_GAMES, "path") else str(TEAM_GAMES)
    st_disagree = con.execute(f"""
        WITH tgd AS (SELECT team_fid, CAST(game_date AS DATE) AS gd,
                            ANY_VALUE(season_type) AS st FROM '{tgp}' GROUP BY 1, 2),
        dupkeys AS (SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*) > 1)
        SELECT COUNT(*) FROM '{vq}' w
        JOIN tgd a ON a.team_fid = w.nfl_franchise_number
                  AND a.gd = CAST(w.game_date AS DATE)
        WHERE w.season_type IS DISTINCT FROM a.st
          AND w.player_week NOT IN (SELECT player_week FROM dupkeys)""").fetchone()[0]
    no_opponent = con.execute(f"""
        SELECT COUNT(*) FROM '{vq}'
        WHERE opponent_nfl_team IS NULL OR TRIM(opponent_nfl_team) = ''
           OR opponent_nfl_franchise_number IS NULL""").fetchone()[0]
    no_name = con.execute(f"""
        SELECT COUNT(*) FROM '{vq}'
        WHERE player IS NULL OR TRIM(player) = ''""").fetchone()[0]
    # clone games, key-INDEPENDENT: a player-week whose rows are not distinguishable
    # by (opponent, game_date) is a clone regardless of key spelling (the float-key
    # twins hid from the dup-key invariant this way)
    clone_groups = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT NFL_player_id, year, week, season_type, COUNT(*) AS n,
                 COUNT(DISTINCT (opponent_nfl_team, game_date)) AS dg
          FROM '{vq}' WHERE NFL_player_id IS NOT NULL
          GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
        ) WHERE n <> dg""").fetchone()[0]
    con.close()

    bounds = recon_bounds.run(src)["total"]

    from . import (golden_samples, recon_ceilings, recon_conservation, recon_mirrors,
                   recon_row_identities, recon_stragglers)
    g = golden_samples.run()
    ceil = recon_ceilings.run(src)["total"]
    cons = recon_conservation.run(src)["flagged"]
    cells = recon_row_identities.run(src)["mismatched_cells"]
    strag = recon_stragglers.run(src)
    mirrors = recon_mirrors.run(src)
    mirror_bad = sum(c.get("mismatches") or 0 for c in mirrors if c["status"] == "CHECKED")

    return {
        "dup_player_week_groups": dup_groups,
        "bad_dup_groups": bad_dup_groups,
        "hard_bound_violations": bounds,
        "golden_samples_failed": g["failed"],
        "ceiling_violations": ceil,
        "conservation_flags": cons,
        "row_identity_cell_mismatches": cells,
        "orphan_rows_off_schedule": strag["orphan_rows"],
        "lone_player_team_games": strag["scheduled_games_with_1_2_rows"],
        "mirror_mismatches": mirror_bad,
        "float_year_keys": float_keys,
        "season_type_authority_disagreements": st_disagree,
        "missing_opponent_rows": no_opponent,
        "missing_player_name_rows": no_name,
        "clone_game_groups": clone_groups,
    }


def run(rebase: bool = False, src: str | None = None) -> tuple[bool, dict]:
    current = measure(src)
    baseline = json.load(open(BASELINE)) if os.path.exists(BASELINE) else None
    if baseline is None or rebase:
        os.makedirs(os.path.dirname(BASELINE), exist_ok=True)
        json.dump(current, open(BASELINE, "w"), indent=2)
        return True, {k: (v, v, "baseline") for k, v in current.items()}
    report, ok = {}, True
    for k, cur in current.items():
        base = baseline.get(k)
        if base is None or cur > base:
            report[k] = (cur, base, "REGRESSION")
            ok = False
        elif cur < base:
            report[k] = (cur, base, "improved (run --rebase to ratchet)")
        else:
            report[k] = (cur, base, "ok")
    return ok, report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebase", action="store_true")
    ap.add_argument("--src", default=None)
    a = ap.parse_args()
    ok, rep = run(a.rebase, a.src)
    for k, (cur, base, verdict) in rep.items():
        print(f"  {k:36s} current={cur:<6} baseline={'-' if base is None else base:<6} {verdict}")
    print("GATE", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)
