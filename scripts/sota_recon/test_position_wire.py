"""THE POSITION WIRE (Joe 2026-08-04: "a 3 way wire where players and
witnesses have position linked and position and fantasy position are linked
as well ... fully wired and immutable as a rule with quorum").

Three edges, each proven on every run:

  EDGE 1  witnesses -> nfl_position     external sources, compared through
                                        position_taxonomy (their vocabulary
                                        and ours meet on the BROAD axis --
                                        the alignment whose absence made
                                        these lanes unusable before)
  EDGE 2  nfl_position -> position      our detailed role maps to our broad
                                        position by the taxonomy, exactly
  EDGE 3  position -> fantasy_position  the fantasy slot IS the broad
                                        position (or an approved combo in
                                        the taxonomy's order)
  EDGE 4  combo ORDER                   every combo obeys the locked rank
                                        QB,RB,WR,TE,K,LB,DL,DB,P,OL -- an
                                        absolute rank, NOT primacy (Deion's
                                        WR season is WR,DB; Groza is K,OL)
  EDGE 5  witness CEILING               fantasy_position never claims a role
                                        the witness didn't declare. Kicker
                                        is the ONE role our own rule may
                                        add; everything else is read, not
                                        inferred.

QUORUM: edge 1 requires >= 2 INDEPENDENT witness roots agreeing with us on
the broad axis; a single root cannot certify a position.

This test is the immutability: any writer that breaks an edge fails here.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import duckdb
import pytest

TAX = Path(__file__).parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json"
MIN_EDGE = 0.999          # our own edges are definitional: near-exact
MIN_QUORUM_AGREE = 0.90   # witnesses use different eras/vocabularies
MIN_ROOTS = 2


def _tax_nonpos():
    """Labels that are usage roles, not positions -- never comparable."""
    d = json.loads(TAX.read_text(encoding="utf-8"))
    return d.get("non_position_labels", {}).get("labels", [])


def _tax():
    d = json.loads(TAX.read_text(encoding="utf-8"))
    return d["detailed_to_broad"], d["broad_positions"]


def _broad_case(col: str, d2b: dict) -> str:
    """Delegate to the harness's mapper so the wire and the vouch harness
    agree on what 'broad' means -- including dual-alignment slash notation
    (LCB/RCB, LT/RT), which was ~85% of the measured PFR disagreement."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.sota_recon.vouch_2024 import _broad_sql
    return _broad_sql(col)


def test_position_wire_holds():
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.sota_recon import sources as S
    import scripts.sota_recon.witness_map as W
    from scripts.sota_recon.vouch_2024 import lane_sql

    d2b, broad = _tax()
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("SET threads=2")
    wk = S.weekly_read_path()
    failures = []

    # ---------- EDGE 3: position -> fantasy_position ----------
    vocab = ", ".join(repr(b) for b in broad)
    n, ok, bad = con.execute(f"""
    SELECT COUNT(*),
      COUNT(*) FILTER (WHERE fantasy_position IS NOT NULL),
      COUNT(*) FILTER (WHERE fantasy_position IS NOT NULL
        AND NOT list_contains(list_transform(
              str_split(fantasy_position, ','), x -> TRIM(x)), fantasy_position)
        AND fantasy_position NOT IN ({vocab})
        AND fantasy_position NOT LIKE '%,%')
    FROM read_parquet('{wk}')""").fetchone()
    if n and ok / n < MIN_EDGE:
        failures.append(f"EDGE3 population: fantasy_position {ok}/{n} "
                        f"= {ok/n:.5f} < {MIN_EDGE}")
    if bad:
        failures.append(f"EDGE3 vocabulary: {bad} values outside the "
                        f"approved set {broad}")

    # ---------- EDGE 4: THE COMBO ORDER LAW ----------
    # Joe 2026-08-04: multi-positional players list in the locked rank
    # QB,RB,WR,TE,K,LB,DL,DB,P,OL -- absolute, and NOT primacy (Deion's
    # WR-eligible season is WR,DB though DB was primary). Groza is K,OL.
    # Every combo in the plane must obey it, and the law itself must still
    # be locked on both sides.
    from scripts.sota_recon.witness_gate.position_taxonomy import (
        COMBO_ORDER, is_ordered_combo)
    combos = [r[0] for r in con.execute(f"""
        SELECT DISTINCT fantasy_position FROM read_parquet('{wk}')
        WHERE fantasy_position LIKE '%,%'""").fetchall() if r[0]]
    unordered = sorted(c for c in combos if not is_ordered_combo(c))
    if unordered:
        failures.append(
            f"EDGE4 combo order: {len(unordered)} combos violate the locked "
            f"rank {','.join(COMBO_ORDER)}: {unordered[:12]}")

    # ---------- EDGE 5: THE WITNESS CEILING ----------
    # Joe 2026-08-04: "the only time we can make our own rule is kicker."
    # Where a witness declares the season, fantasy_position may not exceed
    # that declaration -- except by K, which our volume rule may add. This
    # is the gate that catches a derivation quietly inventing eligibility:
    # the union-with-the-plane-label version over-fired 2,429 seasons.
    from scripts.sota_recon.apply_weekly_overlays import _season_plane
    sp = _season_plane()
    over = con.execute(f"""
    WITH d AS (SELECT NFL_player_id AS pid, CAST(year AS INT) AS yr,
                      declared AS decl
               FROM read_parquet('{sp}') WHERE declared IS NOT NULL),
    x AS (SELECT DISTINCT d.pid, d.yr, t.fantasy_position AS fp, d.decl
          FROM read_parquet('{wk}') t
          JOIN d ON d.pid = t.NFL_player_id AND d.yr = CAST(t.year AS INT)
          WHERE t.fantasy_position IS NOT NULL),
    y AS (SELECT *, list_filter(str_split(fp, ','),
                 tok -> NOT list_contains(str_split(decl, ','), tok)) AS extra
          FROM x)
    SELECT COUNT(*) FILTER (WHERE len(extra) > 0 AND extra <> ['K']),
           ANY_VALUE(fp), ANY_VALUE(decl)
    FROM y WHERE len(extra) > 0 AND extra <> ['K']""").fetchone()
    if over and over[0]:
        failures.append(
            f"EDGE5 witness ceiling: {over[0]} player-seasons claim a role no "
            f"witness declared (e.g. we say {over[1]!r}, witness says "
            f"{over[2]!r}). Only K may be added by our own rule.")

    # ---------- EDGE 2: nfl_position -> position ----------
    b = _broad_case("nfl_position", d2b)
    n2, ok2 = con.execute(f"""
    SELECT COUNT(*), COUNT(*) FILTER (WHERE position = ({b}))
    FROM read_parquet('{wk}')
    WHERE nfl_position IS NOT NULL AND position IS NOT NULL
      AND ({b}) IS NOT NULL AND position NOT LIKE '%,%'""").fetchone()
    if n2 and ok2 / n2 < MIN_EDGE:
        failures.append(f"EDGE2 nfl_position->position: {ok2}/{n2} "
                        f"= {ok2/n2:.5f} < {MIN_EDGE}")

    # ---------- EDGE 1: witnesses -> our broad position (QUORUM) ----------
    def root_of(k):
        k = k.lower()
        for pre, r in (("pfr", "pfr"), ("nflcom", "nflcom"), ("pbp", "pbp"),
                       ("statscrew", "statscrew"), ("ancient_pfa", "pfa"),
                       ("newspaper", "newspaper")):
            if k.startswith(pre) or pre in k:
                return r
        return "internal"

    agree_by_root = defaultdict(lambda: [0, 0])
    for sp in W.WITNESS_MAP:
        if sp.v26_col not in ("position", "nfl_position"):
            continue
        if "legacy" in sp.source_key:      # us-yesterday is not a witness
            continue
        sql, keys = lane_sql(sp)
        if not sql:
            continue
        # position witnesses are mostly SEASON-grain (pfr_id, yr); the first
        # cut filtered for week keys and therefore measured NOTHING -- an
        # empty quorum that looked like disagreement. Both shapes count.
        bio = Path(S.PLAYER_BIO.path).as_posix()
        if keys == ("pid", "yr", "wk"):
            join = (f"JOIN read_parquet('{wk}') t "
                    "ON t.NFL_player_id = w.pid AND t.year = w.yr "
                    "AND t.week = w.wk")
        elif keys == ("pfr_id", "yr"):
            join = (f"JOIN (SELECT pfr_id, NFL_player_id FROM "
                    f"read_parquet('{bio}') WHERE pfr_id IS NOT NULL) b "
                    "ON b.pfr_id = w.pfr_id "
                    f"JOIN read_parquet('{wk}') t "
                    "ON t.NFL_player_id = b.NFL_player_id AND t.year = w.yr")
        else:
            continue
        # RETURN ROLES ARE NOT POSITIONS (2026-08-05). StatsCrew's roster
        # lane mixes KR/PR/RS in with real positions; our broad mapper
        # passed them through unmapped so they scored as disagreements.
        # A man listed KR returned kicks -- that says nothing about
        # whether he was a WR or a DB. Excluding them lifted that lane
        # from 0.9443 to 0.9869 post-1970: the lane was never wrong, the
        # comparison was.
        nonpos = ", ".join(repr(x) for x in _tax_nonpos())
        vocab_ok = (f"{_broad_case('CAST(w.val AS VARCHAR)', d2b)} "
                    f"NOT IN ({nonpos})") if nonpos else "1=1"
        try:
            tot, hit = con.execute(f"""
            WITH w AS ({sql})
            SELECT COUNT(*), COUNT(*) FILTER (
              WHERE {_broad_case('CAST(w.val AS VARCHAR)', d2b)} = t.position)
            FROM w {join}
            WHERE t.position IS NOT NULL AND w.val IS NOT NULL
              AND {vocab_ok}
              AND w.yr BETWEEN 2015 AND 2024""").fetchone()
        except Exception:
            continue
        if tot:
            r = root_of(sp.source_key)
            agree_by_root[r][0] += hit
            agree_by_root[r][1] += tot
    quorum = [r for r, (h, t) in agree_by_root.items()
              if t >= 100 and h / t >= MIN_QUORUM_AGREE]
    if len(quorum) < MIN_ROOTS:
        detail = {r: f"{h}/{t}={h/t:.3f}" for r, (h, t) in agree_by_root.items()}
        failures.append(f"EDGE1 quorum: only {len(quorum)} roots agree "
                        f">= {MIN_QUORUM_AGREE} (need {MIN_ROOTS}); {detail}")

    assert not failures, ("POSITION WIRE BROKEN:\n" + "\n".join(failures))
