"""THE LOCK GATE v2: every vouched lane re-proves itself IN ITS VOUCHED
WINDOW on every run. Locks key on full lane fingerprints -- v1 keyed on
(source, col, shape), collided on sibling specs, and was caught as fiction
by its own first clean run (the poison protocol working early).

Born with poison_test_locks.py: the gate does not count as existing until it
has been watched refusing a flipped atom.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

LOCKFILE = Path(__file__).parent / "witness_gate" / "contracts" / "witness_locks.v1.json"
EPSILON = 0.005


def _window(lock):
    w = lock["window"]
    if w == "2024":
        return 2024, 2024
    if w == "2020-2024":
        return 2020, 2024
    lo, hi = w.split()[-1].split("-")
    return int(lo), int(hi)


def test_locked_lanes_still_vouch_in_their_windows():
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import scripts.sota_recon.witness_map as W
    from scripts.sota_recon import sources as S
    from scripts.sota_recon.vouch_2024 import lane_sql, fingerprint, compare

    if not LOCKFILE.exists():
        pytest.skip("no lockfile yet")
    doc = json.loads(LOCKFILE.read_text(encoding="utf-8"))
    if doc.get("version") != "v2":
        pytest.skip("v1 lockfile retired; rerun vouch_2024 for v2")
    locks = doc["locks"]

    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    wk = S.weekly_read_path()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    # VIEW: the gate's first v2 run crashed exit-139 materializing the full
    # plane -- the exact bug fixed in the harness an hour earlier. Same cure.
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane_post AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'POST'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    by_fp = {fingerprint(sp): sp for sp in W.WITNESS_MAP}

    # SCOPED RUNS (2026-08-04, Joe: "why do simple fixes take so long").
    # Re-proving all ~936 lanes costs ~7 minutes and runs on EVERY landing,
    # even when a batch touched three columns. SOTA_GATE_COLUMNS scopes the
    # pass to the columns a batch actually wrote; the unscoped full sweep
    # still runs on the cadence (and in CI), so nothing loses coverage --
    # it just stops re-proving untouched lanes per landing.
    import os
    scope = {c.strip() for c in os.environ.get("SOTA_GATE_COLUMNS", "").split(",") if c.strip()}
    if scope:
        locks = [l for l in locks if l["column"] in scope]
        print(f"SCOPED GATE: {len(locks)} locks over {len(scope)} columns")
    else:
        # THE ECONOMY LAW (Joe 2026-08-04: "quick updates are locked as a
        # rule; long updates only when justified"). An unscoped pass costs
        # ~7 minutes and re-proves lanes no batch touched. It is allowed
        # ONLY with a stated reason -- CI, the maintenance cadence, and
        # post-sweep certification all set one.
        reason = os.environ.get("SOTA_GATE_FULL_REASON", "").strip()
        assert reason, (
            "FULL GATE REFUSED: an unscoped 936-lane pass needs a reason. "
            "Set SOTA_GATE_COLUMNS=<touched columns> for a scoped run, or "
            "SOTA_GATE_FULL_REASON='cadence sweep|CI|post-remint' to justify "
            "the full cost.")
        print(f"FULL GATE (justified): {reason}")

    failures = []
    for lock in locks:
        sp = by_fp.get(lock["fingerprint"])
        if sp is None:
            failures.append(f"{lock['column']}<-{lock['source']}: SPEC GONE")
            continue
        if lock.get("window") == "native-validate":
            # validator-fallback locks re-prove through the same engine that
            # minted them (17 read LANE GONE before the gate knew the route)
            try:
                res = W.validate([sp])[0]
            except Exception as e:
                failures.append(f"{lock['column']}<-{lock['source']}: "
                                f"VALIDATOR BROKEN {str(e)[:50]}")
                continue
            got = res.get("agree_pct") or 0.0
            allowed = (lock["agree"] if lock["agree"] >= 1.0
                       else lock["agree"] - EPSILON)
            if (res.get("n") or 0) and got < allowed:
                failures.append(
                    f"{lock['column']}<-{lock['source']} [native]: "
                    f"vouched {lock['agree']}, now {got:.4f} -- DRIFT")
            continue
        sql, keys = lane_sql(sp)
        if not sql:
            failures.append(f"{lock['column']}<-{lock['source']}: LANE GONE")
            continue
        lo, hi = _window(lock)
        try:
            n, agree = compare(con, sp, sql, keys, lo, hi)
        except Exception as e:
            failures.append(f"{lock['column']}<-{lock['source']}: BROKEN "
                            f"{str(e).splitlines()[0][:60]}")
            continue
        got = agree / n if n else 0.0
        # a lane vouched PERFECT stays perfect: one flipped atom must fail.
        # (the poison test caught epsilon swallowing single-atom flips --
        # 999/1000 = 0.999 >= 0.995 'passed'. Fiction, fixed.)
        allowed = lock["agree"] if lock["agree"] >= 1.0 else lock["agree"] - EPSILON
        if got < allowed:
            failures.append(
                f"{lock['column']}<-{lock['source']} [{lock['window']}]: "
                f"vouched {lock['agree']}, now {got:.4f} (n={n}) -- DRIFT")
    assert not failures, (
        f"{len(failures)} locked lanes broke their vouch:\n" +
        "\n".join(failures[:20]))
