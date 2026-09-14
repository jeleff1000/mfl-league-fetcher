"""
sota_recon/relationship_verdicts.py  --  O.6: RELATIONSHIP VERDICT RUNNER (§19.3)

Generate-and-test: every runnable candidate from relationship_candidates.py is
executed against the v26 release (and its season/career tables) PER ERA BAND under
the declared tolerance law (§17.1: EXACT float-eps via witness_votes.tolerance_of --
never a lane constant). Verdict vocabulary, typed with receipts:

  CONFIRMED     exact in every covered era (coverage window stated)
  CONDITIONAL   exact in >=1 covered era and violated in another, OR exact only
                under a declared definition scope (e.g. rate stored rounded to 1dp);
                scope = the exact eras / the definition
  REFUTED       violated in every covered era -> do-not-enforce + counterexamples
  UNDECIDABLE   no era reaches the overlap floor (deficit stated)
  PENDING_TEST  candidate whose harness is not yet runnable (R4 team-witness K-plane)
  ESCALATED     never auto-run (newspaper-root-crossing edges, by law)

Also runs the BRUTE EMPIRICAL MINER: exact linear relations (A=B, A=B+C, A<=B) over
overlapping rows, sieved on a deterministic hash-sample within same-unit column
groups, then FULL-DATA verified through the same per-era harness. Mined survivors
join the space with proposal_basis="empirical_mined".

Near-misses (violation rate < 0.1% in a covered era) are flagged for the
definition-drift queue (§19.2) -- never silently confirmed, never silently dropped.

Output: docs/relationship-verdicts.v1.json (receipt artifact; the committed edge
contract is composed from it by relationship_edges.py, which is regen-diff-tested).

Run:  python -m scripts.sota_recon.relationship_verdicts
"""

from __future__ import annotations

import argparse
import json
import os

import duckdb

from .relationship_candidates import Candidate, generate, load_contract_stats, scope_columns
from .sources import latest_v26
from .witness_votes import tolerance_of

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "relationship-verdicts.v1.json")

ERAS = [("pre1933", 1920, 1932), ("1933_49", 1933, 1949), ("1950_77", 1950, 1977),
        ("1978_98", 1978, 1998), ("1999_2025", 1999, 2025)]
ERA_SQL = ("CASE WHEN year<=1932 THEN 'pre1933' WHEN year<=1949 THEN '1933_49' "
           "WHEN year<=1977 THEN '1950_77' WHEN year<=1998 THEN '1978_98' "
           "ELSE '1999_2025' END")

MIN_OVERLAP_ROWS = 500     # per-era floor for row/team/grain tests
MIN_OVERLAP_YEARS = 3      # per-era floor for league-year conservation
NEAR_MISS_RATE = 0.001     # <0.1% violations in a covered era -> definition-drift flag

# miner sieve params (sieve is a candidate FINDER only; verdicts come from full data)
SIEVE_STAGE1_MOD = 601     # hash(player_week) % MOD == 0 -> ~2k deterministic rows
SIEVE_STAGE2_MOD = 7       # ~170k rows for survivor re-screen
SIEVE_MIN_OVERLAP = 100
SIEVE_MIN_NONZERO = 20
MINE_UNITS = ("count", "yards", "touchdowns", "epa", "wpa")


def _q(value) -> str:
    from pathlib import Path
    return Path(getattr(value, "path", value)).as_posix()


def _tol(stat: str) -> float:
    return tolerance_of(stat)


def _expr(c: Candidate, alias: str = "") -> str:
    pre = f"{alias}." if alias else ""
    weights = c.weights or tuple(1.0 for _ in c.rhs)
    return " + ".join(f"({w}) * {pre}{col}" for col, w in zip(c.rhs, weights))


def _guard(c: Candidate, alias: str = "") -> str:
    pre = f"{alias}." if alias else ""
    cols = [c.lhs, *c.rhs]
    return " AND ".join(f"{pre}{col} IS NOT NULL" for col in cols)


class Runner:
    def __init__(self, src: str | None = None):
        self.release = _q(src or latest_v26())
        base = os.path.dirname(self.release)
        self.tables = {
            "player_season": f"{base}/season_career_v26/player_nfl_season.parquet",
            "player_season_all": f"{base}/season_career_v26/player_nfl_season_all.parquet",
            "player_career": f"{base}/season_career_v26/player_nfl_career.parquet",
            "player_career_all": f"{base}/season_career_v26/player_nfl_career_all.parquet",
        }
        self.con = duckdb.connect()
        self.con.execute("SET memory_limit='6GB'")
        self._cols_cache: dict[str, set[str]] = {}
        self._team_week_built = False

    def cols(self, path: str) -> set[str]:
        if path not in self._cols_cache:
            self._cols_cache[path] = {r[0] for r in self.con.execute(
                f"DESCRIBE SELECT * FROM '{path}'").fetchall()}
        return self._cols_cache[path]

    # ---------------- per-era result -> verdict ----------------

    def _verdict(self, eras: dict[str, dict], min_overlap: int) -> dict:
        covered = {e: r for e, r in eras.items() if r["overlap"] >= min_overlap}
        exact = [e for e, r in covered.items() if r["violations"] == 0]
        near = [e for e, r in covered.items()
                if 0 < r["violations"] and r["violations"] / r["overlap"] < NEAR_MISS_RATE]
        if not covered:
            return {"verdict": "UNDECIDABLE", "era_scope": [],
                    "deficit": f"no era reaches overlap floor {min_overlap}",
                    "eras": eras, "near_miss_eras": []}
        if len(exact) == len(covered):
            return {"verdict": "CONFIRMED", "era_scope": sorted(covered),
                    "eras": eras, "near_miss_eras": []}
        if exact:
            return {"verdict": "CONDITIONAL", "era_scope": sorted(exact),
                    "condition": {"type": "era", "exact_eras": sorted(exact),
                                  "violating_eras": sorted(set(covered) - set(exact))},
                    "eras": eras, "near_miss_eras": near}
        return {"verdict": "REFUTED", "era_scope": [], "eras": eras,
                "near_miss_eras": near}

    def _era_counts(self, sql: str) -> dict[str, dict]:
        rows = self.con.execute(sql).fetchall()
        out = {e: {"overlap": 0, "violations": 0} for e, _, _ in ERAS}
        for era, overlap, violations in rows:
            out[era] = {"overlap": int(overlap), "violations": int(violations)}
        return out

    # ---------------- row-grain tests (R1/R2/R7/R9 + mined) ----------------

    def run_row(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        expr = _expr(c)
        if c.op == "eq":
            viol = f"ABS({c.lhs} - ({expr})) > {eps}"
        else:
            viol = f"{c.lhs} > ({expr}) + {eps}"
        eras = self._era_counts(f"""
            SELECT {ERA_SQL} AS era, COUNT(*),
                   COUNT(*) FILTER (WHERE {viol})
            FROM '{self.release}' WHERE {_guard(c)} GROUP BY 1""")
        v = self._verdict(eras, MIN_OVERLAP_ROWS)
        if v["verdict"] in ("REFUTED", "CONDITIONAL"):
            v["counterexamples"] = [
                {"player_week": a, "year": b, "stored": s, "derived": d}
                for a, b, s, d in self.con.execute(f"""
                    SELECT player_week, year, {c.lhs}, ({expr})
                    FROM '{self.release}' WHERE {_guard(c)} AND {viol}
                    ORDER BY ABS({c.lhs} - ({expr})) DESC LIMIT 3""").fetchall()]
        return v

    def run_rate(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        num, den = c.rhs
        computed = f"({num} * 1.0 / {den})"
        guard = f"{c.lhs} IS NOT NULL AND {num} IS NOT NULL AND {den} > 0"
        forms = {"raw": computed, "round1": f"ROUND({computed}, 1)",
                 "round2": f"ROUND({computed}, 2)",
                 "pct_round1": f"ROUND(100.0 * {computed}, 1)"}
        best = None
        for form, expr in forms.items():
            eras = self._era_counts(f"""
                SELECT {ERA_SQL} AS era, COUNT(*),
                       COUNT(*) FILTER (WHERE ABS({c.lhs} - {expr}) > {eps})
                FROM '{self.release}' WHERE {guard} GROUP BY 1""")
            v = self._verdict(eras, MIN_OVERLAP_ROWS)
            v["definition_form"] = form
            rank = {"CONFIRMED": 0, "CONDITIONAL": 1, "REFUTED": 2, "UNDECIDABLE": 3}
            if best is None or rank[v["verdict"]] < rank[best["verdict"]]:
                best = v
            if v["verdict"] == "CONFIRMED":
                break
        if best["verdict"] in ("CONFIRMED", "CONDITIONAL") \
                and best["definition_form"] != "raw":
            cond = best.get("condition", {})
            best["verdict"] = "CONDITIONAL"
            best["condition"] = {**cond, "type": "definition",
                                 "definition": f"stored form = {best['definition_form']}"}
        if best["verdict"] in ("REFUTED", "CONDITIONAL"):
            expr = forms[best["definition_form"]]
            best["counterexamples"] = [
                {"player_week": a, "year": b, "stored": s, "derived": d}
                for a, b, s, d in self.con.execute(f"""
                    SELECT player_week, year, {c.lhs}, {expr}
                    FROM '{self.release}' WHERE {guard}
                      AND ABS({c.lhs} - {expr}) > {eps}
                    ORDER BY ABS({c.lhs} - {expr}) DESC LIMIT 3""").fetchall()]
        return best

    # ---------------- team-week tests (R3/R5) ----------------

    def _build_team_week(self, cands: list[Candidate]) -> None:
        if self._team_week_built:
            return
        need_sum, need_max = set(), set()
        for c in cands:
            if c.test_kind in ("team_mirror", "team_bound"):
                (need_max if c.agg == "MAX" else need_sum).update([c.lhs, *c.rhs])
            elif c.test_kind == "team_horizontal":
                need_max.add(c.lhs)          # allowed value lives on the single DEF row
                need_sum.update(c.rhs)
        cols = self.cols(self.release)
        need_sum = sorted(x for x in need_sum if x in cols)
        need_max = sorted(x for x in need_max if x in cols)
        sel = [f"SUM({x}) AS s_{x}" for x in need_sum] + \
              [f"MAX({x}) AS m_{x}" for x in need_max]
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE team_week AS
            SELECT nfl_team, year, week, season_type,
                   MAX(opponent_nfl_team) AS opp, {', '.join(sel)}
            FROM '{self.release}'
            WHERE nfl_team IS NOT NULL AND week IS NOT NULL
            GROUP BY 1, 2, 3, 4""")
        self._team_week_built = True

    def run_team_mirror(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        p = "m_" if c.agg == "MAX" else "s_"
        lhs, rhs = f"{p}{c.lhs}", f"{p}{c.rhs[0]}"
        viol = (f"ABS({lhs} - {rhs}) > {eps}" if c.op == "eq"
                else f"{lhs} > {rhs} + {eps}")
        eras = self._era_counts(f"""
            SELECT {ERA_SQL} AS era, COUNT(*), COUNT(*) FILTER (WHERE {viol})
            FROM team_week WHERE {lhs} IS NOT NULL AND {rhs} IS NOT NULL GROUP BY 1""")
        v = self._verdict(eras, MIN_OVERLAP_ROWS)
        if v["verdict"] in ("REFUTED", "CONDITIONAL"):
            v["counterexamples"] = [
                {"nfl_team": t, "year": y, "week": w, "stored": s, "derived": d}
                for t, y, w, s, d in self.con.execute(f"""
                    SELECT nfl_team, year, week, {lhs}, {rhs} FROM team_week
                    WHERE {lhs} IS NOT NULL AND {rhs} IS NOT NULL AND {viol}
                    ORDER BY ABS({lhs} - {rhs}) DESC LIMIT 3""").fetchall()]
        return v

    def run_team_horizontal(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        weights = c.weights or tuple(1.0 for _ in c.rhs)
        expr = " + ".join(f"({w}) * b.s_{col}" for col, w in zip(c.rhs, weights))
        guard = " AND ".join([f"a.m_{c.lhs} IS NOT NULL",
                              *[f"b.s_{col} IS NOT NULL" for col in c.rhs]])
        join = ("FROM team_week a JOIN team_week b "
                "ON a.opp = b.nfl_team AND b.opp = a.nfl_team AND a.year = b.year "
                "AND a.week = b.week AND a.season_type = b.season_type")
        viol = f"ABS(a.m_{c.lhs} - ({expr})) > {eps}"
        eras = self._era_counts(f"""
            SELECT {ERA_SQL.replace('year', 'a.year')} AS era, COUNT(*),
                   COUNT(*) FILTER (WHERE {viol})
            {join} WHERE {guard} GROUP BY 1""")
        v = self._verdict(eras, MIN_OVERLAP_ROWS)
        if v["verdict"] in ("REFUTED", "CONDITIONAL"):
            v["counterexamples"] = [
                {"nfl_team": t, "year": y, "week": w, "stored": s, "derived": d}
                for t, y, w, s, d in self.con.execute(f"""
                    SELECT a.nfl_team, a.year, a.week, a.m_{c.lhs}, ({expr})
                    {join} WHERE {guard} AND {viol}
                    ORDER BY ABS(a.m_{c.lhs} - ({expr})) DESC LIMIT 3""").fetchall()]
        return v

    # ---------------- IDP <-> team-defense vertical (R4) ----------------
    # v26 keeps DUAL defensive bookkeeping: one team-DEF row per team-week plus
    # individual IDP rows. The two planes are NEVER summed together; their
    # reconciliation (team row = SUM of IDP rows) is this lane.

    def _build_idp_team_week(self, cands: list[Candidate]) -> None:
        stats = sorted({c.lhs for c in cands
                        if c.test_kind == "team_idp_vertical"} & self.cols(self.release))
        if not stats or getattr(self, "_idp_built", False):
            return
        sel = [f"MAX(CASE WHEN position = 'DEF' THEN {s} END) AS t_{s}, "
               f"SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN {s} END) AS i_{s}"
               for s in stats]
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE idp_team_week AS
            SELECT nfl_team, year, week, season_type, {', '.join(sel)}
            FROM '{self.release}'
            WHERE nfl_team IS NOT NULL AND week IS NOT NULL
            GROUP BY 1, 2, 3, 4""")
        self._idp_built = True

    def run_team_idp(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        t, i = f"t_{c.lhs}", f"i_{c.lhs}"
        viol = f"ABS({t} - {i}) > {eps}"
        eras = self._era_counts(f"""
            SELECT {ERA_SQL} AS era, COUNT(*), COUNT(*) FILTER (WHERE {viol})
            FROM idp_team_week WHERE {t} IS NOT NULL AND {i} IS NOT NULL GROUP BY 1""")
        v = self._verdict(eras, MIN_OVERLAP_ROWS)
        if v["verdict"] == "UNDECIDABLE":
            t_n, i_n = self.con.execute(f"""
                SELECT COUNT({t}), COUNT({i}) FROM idp_team_week""").fetchone()
            plane = ("team-DEF plane lacks this stat" if not t_n
                     else "IDP plane lacks this stat" if not i_n
                     else "planes overlap below floor")
            v["deficit"] = f"{plane} (team rows {t_n:,}, idp rows {i_n:,})"
        if v["verdict"] in ("REFUTED", "CONDITIONAL"):
            v["counterexamples"] = [
                {"nfl_team": a, "year": b, "week": w, "team_def_row": s, "idp_sum": d}
                for a, b, w, s, d in self.con.execute(f"""
                    SELECT nfl_team, year, week, {t}, {i} FROM idp_team_week
                    WHERE {t} IS NOT NULL AND {i} IS NOT NULL AND {viol}
                    ORDER BY ABS({t} - {i}) DESC LIMIT 3""").fetchall()]
        return v

    # ---------------- R5 cross-side: defense booking vs opponent offense (O.7) ---------
    # def-side lhs is population-scoped (team-DEF row via MAX over the single DEF
    # row, or IDP plane via SUM over non-DEF rows); rhs = offense-side sums on the
    # OPPONENT team-week. The two def planes are never combined.

    def _build_cross_tw(self, cands: list[Candidate]) -> None:
        group = [c for c in cands if c.test_kind == "team_cross_side"]
        cols = self.cols(self.release)
        stats = sorted({c.lhs for c in group} | {s for c in group for s in c.rhs})
        stats = [s for s in stats if s in cols]
        if not stats or getattr(self, "_cross_built", False):
            return
        sel = []
        for s in stats:
            sel.append(f"MAX(CASE WHEN position = 'DEF' THEN {s} END) AS t_{s}")
            sel.append(f"SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN {s} END)"
                       f" AS i_{s}")
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE cross_tw AS
            SELECT nfl_team, year, week, season_type,
                   MAX(opponent_nfl_team) AS opp, {', '.join(sel)}
            FROM '{self.release}'
            WHERE nfl_team IS NOT NULL AND week IS NOT NULL
            GROUP BY 1, 2, 3, 4""")
        self._cross_built = True

    def run_team_cross_side(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        cols = self.cols(self.release)
        if c.lhs not in cols or any(s not in cols for s in c.rhs):
            return {"verdict": "UNDECIDABLE", "era_scope": [], "eras": {},
                    "deficit": "column absent from release", "near_miss_eras": []}
        lhs = ("t_" if c.population == "team_def_row" else "i_") + c.lhs
        rhs = " + ".join(f"b.i_{s}" for s in c.rhs)
        guard = " AND ".join([f"a.{lhs} IS NOT NULL",
                              *[f"b.i_{s} IS NOT NULL" for s in c.rhs]])
        join = ("FROM cross_tw a JOIN cross_tw b "
                "ON a.opp = b.nfl_team AND b.opp = a.nfl_team AND a.year = b.year "
                "AND a.week = b.week AND a.season_type = b.season_type")
        viol = f"ABS(a.{lhs} - ({rhs})) > {eps}"
        eras = self._era_counts(f"""
            SELECT {ERA_SQL.replace('year', 'a.year')} AS era, COUNT(*),
                   COUNT(*) FILTER (WHERE {viol})
            {join} WHERE {guard} GROUP BY 1""")
        v = self._verdict(eras, MIN_OVERLAP_ROWS)
        if v["verdict"] == "UNDECIDABLE":
            n = self.con.execute(
                f"SELECT COUNT({lhs}) FROM cross_tw").fetchone()[0]
            plane = ("team-DEF plane lacks this stat"
                     if c.population == "team_def_row" and not n
                     else "IDP plane lacks this stat"
                     if c.population == "idp_rows" and not n
                     else "overlap below floor")
            v["deficit"] = f"{plane} (lhs team-weeks {n:,})"
        if v["verdict"] in ("REFUTED", "CONDITIONAL"):
            v["counterexamples"] = [
                {"nfl_team": t, "year": y, "week": w, "def_side": s, "opp_offense": d}
                for t, y, w, s, d in self.con.execute(f"""
                    SELECT a.nfl_team, a.year, a.week, a.{lhs}, ({rhs})
                    {join} WHERE {guard} AND {viol}
                    ORDER BY ABS(a.{lhs} - ({rhs})) DESC LIMIT 3""").fetchall()]
        return v

    # ---------------- R4 vertical: player-sum vs pfr_box_team_stats (O.7) ----------------
    # The team-stats box table is melted key-value with packed side values
    # ("Rush-Yds-TDs" = "31-128-1"); sides bind to team_games via
    # (boxscore_id, is_home) -> team_fid -- the kc-plane side binding declared on the
    # pfr_box_team_stats contract (kc_planes.v1.json k.side_binding). The join
    # receipt (sides parsed / bound / v26-matched) is emitted with the verdicts, so
    # the R4 lane never consumes an unreceipted join (§19).

    BOX_LABELS = {
        # label -> [(part index, canonical stat)]
        "Cmp-Att-Yd-TD-INT": [(1, "completions"), (2, "attempts"), (3, "passing_yards"),
                              (4, "passing_tds"), (5, "passing_interceptions")],
        "Rush-Yds-TDs": [(1, "carries"), (2, "rushing_yards"), (3, "rushing_tds")],
        "Sacked-Yards": [(1, "sacks_suffered"), (2, "sack_yards_lost")],
        "Fumbles-Lost": [(1, "fumbles"), (2, "fumbles_lost")],
        # era-variant labels (single plain values; measured 2026-07-26)
        "Sack Yds Lost": [(1, "sack_yards_lost")],     # 1947-63
        "Fumbles Lost": [(1, "fumbles_lost")],         # 1920-45
    }

    def _build_box_team_witness(self, stats: set[str]) -> dict:
        """Materialize team-week box witness values for `stats`; return the join
        receipt. Table: box_team(team_fid, y, w, st, stat, team_val)."""
        from .sources import TEAM_GAMES, registry
        ts = _q(registry()["pfr_box_team_stats"].path)
        tg = _q(TEAM_GAMES.path)
        labels = ", ".join(f"'{k}'" for k in self.BOX_LABELS)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE box_sides AS
            WITH kv AS (
              SELECT boxscore_id, stat, vis_stat, home_stat FROM '{ts}'
              WHERE stat IN ({labels})),
            sides AS (
              SELECT boxscore_id, stat, home_stat AS val, TRUE AS is_home FROM kv
              UNION ALL
              SELECT boxscore_id, stat, vis_stat, FALSE FROM kv)
            SELECT s.boxscore_id, s.stat, s.is_home,
                   string_split(s.val, '-') AS parts,
                   g.team_fid, CAST(g.year AS INT) AS y, CAST(g.week AS INT) AS w,
                   g.season_type AS st
            FROM sides s
            LEFT JOIN '{tg}' g ON g.boxscore_id = s.boxscore_id
              AND COALESCE(g.is_home, FALSE) = s.is_home
            WHERE s.val IS NOT NULL AND s.val != ''""")
        arms = []
        for label, parts in self.BOX_LABELS.items():
            for idx, stat in parts:
                if stat in stats:
                    arms.append(f"""
                    SELECT team_fid, y, w, st, '{stat}' AS stat,
                           TRY_CAST(parts[{idx}] AS DOUBLE) AS team_val
                    FROM box_sides WHERE stat = '{label}' AND team_fid IS NOT NULL""")
        self.con.execute("CREATE OR REPLACE TEMP TABLE box_team AS "
                         + " UNION ALL ".join(arms))
        n_sides, n_bound = self.con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE team_fid IS NOT NULL) "
            "FROM box_sides").fetchone()
        n_vals, n_parsed = self.con.execute(
            "SELECT COUNT(*), COUNT(team_val) FROM box_team").fetchone()
        v26_sums = ", ".join(f"SUM(CASE WHEN position IS DISTINCT FROM 'DEF' "
                             f"THEN {s} END) AS s_{s}" for s in sorted(stats))
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE v26_team AS
            SELECT CAST(nfl_franchise_number AS INT) AS team_fid,
                   CAST(year AS INT) AS y, CAST(week AS INT) AS w,
                   season_type AS st, {v26_sums}
            FROM '{self.release}'
            WHERE nfl_franchise_number IS NOT NULL AND week IS NOT NULL
            GROUP BY 1, 2, 3, 4""")
        n_matched = self.con.execute("""
            SELECT COUNT(*) FROM (SELECT DISTINCT team_fid, y, w, st FROM box_team) b
            JOIN v26_team v USING (team_fid, y, w, st)""").fetchone()[0]
        n_box_tw = self.con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT team_fid, y, w, st FROM box_team)"
        ).fetchone()[0]
        return {
            "kc_binding": "pfr_box_team_stats sides -> team_games(boxscore_id, is_home)"
                          " -> team_fid (kc_planes.v1.json k.side_binding)",
            "box_sides": int(n_sides),
            "sides_bound_to_team_games": int(n_bound),
            "stat_values": int(n_vals),
            "stat_values_parsed": int(n_parsed),
            "box_team_weeks": int(n_box_tw),
            "box_team_weeks_matched_to_v26": int(n_matched),
        }

    def run_team_witness_batch(self, cands: list[Candidate]) -> dict[str, dict]:
        """One box build covers every runnable pfr_box_team_stats vertical edge."""
        runnable = [c for c in cands if not c.escalation
                    and c.rhs[0].startswith("pfr_box_team_stats.")]
        results: dict[str, dict] = {}
        if not runnable:
            return results
        cols = self.cols(self.release)
        stats = {c.lhs for c in runnable if c.lhs in cols}
        for c in runnable:
            if c.lhs not in cols:
                results[c.cand_id] = {
                    "verdict": "UNDECIDABLE", "era_scope": [], "eras": {},
                    "deficit": "lhs column absent from release", "near_miss_eras": []}
        if not stats:
            return results
        self._box_receipt = self._build_box_team_witness(stats)
        for c in runnable:
            if c.lhs not in cols:
                continue
            eps = _tol(c.lhs)
            eras = self._era_counts(f"""
                SELECT {ERA_SQL.replace('year', 'b.y')} AS era, COUNT(*),
                       COUNT(*) FILTER (WHERE ABS(v.s_{c.lhs} - b.team_val) > {eps})
                FROM box_team b JOIN v26_team v USING (team_fid, y, w, st)
                WHERE b.stat = '{c.lhs}' AND b.team_val IS NOT NULL
                  AND v.s_{c.lhs} IS NOT NULL
                GROUP BY 1""")
            v = self._verdict(eras, MIN_OVERLAP_ROWS)
            if v["verdict"] in ("REFUTED", "CONDITIONAL"):
                v["counterexamples"] = [
                    {"team_fid": t, "year": y, "week": w, "player_sum": s,
                     "box_team_line": d}
                    for t, y, w, s, d in self.con.execute(f"""
                        SELECT b.team_fid, b.y, b.w, v.s_{c.lhs}, b.team_val
                        FROM box_team b JOIN v26_team v USING (team_fid, y, w, st)
                        WHERE b.stat = '{c.lhs}' AND b.team_val IS NOT NULL
                          AND v.s_{c.lhs} IS NOT NULL
                          AND ABS(v.s_{c.lhs} - b.team_val) > {eps}
                        ORDER BY ABS(v.s_{c.lhs} - b.team_val) DESC LIMIT 3""").fetchall()]
            results[c.cand_id] = v
        return results

    # ---------------- league-year conservation (R8) ----------------

    _POP_FILTER = {
        "all": "{col}",
        "team_def_row": "CASE WHEN position = 'DEF' THEN {col} END",
        "idp_rows": "CASE WHEN position IS DISTINCT FROM 'DEF' THEN {col} END",
    }

    def run_league(self, c: Candidate) -> dict:
        eps = _tol(c.lhs)
        rhs_expr = self._POP_FILTER[c.population].format(col=c.rhs[0])
        rows = self.con.execute(f"""
            SELECT year, SUM({c.lhs}), SUM({rhs_expr})
            FROM '{self.release}' GROUP BY 1
            HAVING SUM({c.lhs}) IS NOT NULL AND SUM({rhs_expr}) IS NOT NULL""").fetchall()
        eras = {e: {"overlap": 0, "violations": 0} for e, _, _ in ERAS}
        worst: list[tuple] = []
        for year, sl, sr in rows:
            era = next(e for e, lo, hi in ERAS if lo <= year <= hi)
            eras[era]["overlap"] += 1
            if abs(sl - sr) > eps:
                eras[era]["violations"] += 1
                worst.append((abs(sl - sr), year, sl, sr))
        v = self._verdict(eras, MIN_OVERLAP_YEARS)
        if v["verdict"] in ("REFUTED", "CONDITIONAL"):
            v["counterexamples"] = [{"year": y, "lhs_sum": sl, "rhs_sum": sr}
                                    for _, y, sl, sr in sorted(worst, reverse=True)[:3]]
        return v

    # ---------------- grain aggregation (R6, batched) ----------------

    def run_grain_batch(self, cands: list[Candidate]) -> dict[str, dict]:
        """One aggregate join per target grain covering every R6 candidate at once."""
        results: dict[str, dict] = {}
        by_target: dict[str, list[Candidate]] = {}
        for c in cands:
            by_target.setdefault(c.grain, []).append(c)

        for target in ("player_season", "player_season_all"):
            group = by_target.get(target, [])
            if not group:
                continue
            table = self.tables[target]
            tcols = self.cols(table)
            runnable = [c for c in group if c.lhs in tcols]
            for c in group:
                if c.lhs not in tcols:
                    results[c.cand_id] = {
                        "verdict": "UNDECIDABLE", "era_scope": [], "eras": {},
                        "deficit": f"column absent from {target} table "
                                   "(contract declares the grain -- finding)",
                        "near_miss_eras": []}
            if not runnable:
                continue
            st_filter = "WHERE season_type = 'REG'" if target == "player_season" else ""
            aggs = ", ".join(f"{c.agg}({c.lhs}) AS {c.lhs}" for c in runnable)
            self.con.execute(f"""
                CREATE OR REPLACE TEMP TABLE wk_agg AS
                SELECT NFL_player_id, year, {aggs}
                FROM '{self.release}' {st_filter} GROUP BY 1, 2""")
            sel = []
            for c in runnable:
                eps = _tol(c.lhs)
                sel.append(f"COUNT(*) FILTER (WHERE s.{c.lhs} IS NOT NULL AND "
                           f"a.{c.lhs} IS NOT NULL) AS ov_{c.lhs}")
                sel.append(f"COUNT(*) FILTER (WHERE s.{c.lhs} IS NOT NULL AND "
                           f"a.{c.lhs} IS NOT NULL AND "
                           f"ABS(s.{c.lhs} - a.{c.lhs}) > {eps}) AS vi_{c.lhs}")
            rows = self.con.execute(f"""
                SELECT {ERA_SQL.replace('year', 's.year')} AS era, {', '.join(sel)}
                FROM '{table}' s JOIN wk_agg a
                  ON s.NFL_player_id = a.NFL_player_id AND s.year = a.year
                GROUP BY 1""").fetchall()
            names = [d[0] for d in self.con.description]
            for c in runnable:
                eras = {e: {"overlap": 0, "violations": 0} for e, _, _ in ERAS}
                for row in rows:
                    rec = dict(zip(names, row))
                    eras[rec["era"]] = {"overlap": int(rec[f"ov_{c.lhs}"]),
                                        "violations": int(rec[f"vi_{c.lhs}"])}
                res = self._verdict(eras, MIN_OVERLAP_ROWS)
                if res["verdict"] in ("REFUTED", "CONDITIONAL"):
                    eps = _tol(c.lhs)
                    res["counterexamples"] = [
                        {"NFL_player_id": p, "year": y, "stored": s, "derived": d}
                        for p, y, s, d in self.con.execute(f"""
                            SELECT s.NFL_player_id, s.year, s.{c.lhs}, a.{c.lhs}
                            FROM '{table}' s JOIN wk_agg a
                              ON s.NFL_player_id = a.NFL_player_id AND s.year = a.year
                            WHERE s.{c.lhs} IS NOT NULL AND a.{c.lhs} IS NOT NULL
                              AND ABS(s.{c.lhs} - a.{c.lhs}) > {eps}
                            ORDER BY ABS(s.{c.lhs} - a.{c.lhs}) DESC
                            LIMIT 3""").fetchall()]
                results[c.cand_id] = res

        for target, source in (("player_career", "player_season"),
                               ("player_career_all", "player_season_all")):
            group = by_target.get(target, [])
            if not group:
                continue
            table, src_table = self.tables[target], self.tables[source]
            tcols, scols = self.cols(table), self.cols(src_table)
            runnable = [c for c in group if c.lhs in tcols and c.lhs in scols]
            for c in group:
                if c not in runnable:
                    results[c.cand_id] = {
                        "verdict": "UNDECIDABLE", "era_scope": [], "eras": {},
                        "deficit": f"column absent from {target} or {source} table",
                        "near_miss_eras": []}
            if not runnable:
                continue
            aggs = ", ".join(f"{c.agg}({c.lhs}) AS {c.lhs}" for c in runnable)
            self.con.execute(f"""
                CREATE OR REPLACE TEMP TABLE se_agg AS
                SELECT NFL_player_id, {aggs} FROM '{src_table}' GROUP BY 1""")
            sel = []
            for c in runnable:
                eps = _tol(c.lhs)
                sel.append(f"COUNT(*) FILTER (WHERE s.{c.lhs} IS NOT NULL AND "
                           f"a.{c.lhs} IS NOT NULL) AS ov_{c.lhs}")
                sel.append(f"COUNT(*) FILTER (WHERE s.{c.lhs} IS NOT NULL AND "
                           f"a.{c.lhs} IS NOT NULL AND "
                           f"ABS(s.{c.lhs} - a.{c.lhs}) > {eps}) AS vi_{c.lhs}")
            row = self.con.execute(f"""
                SELECT {', '.join(sel)}
                FROM '{table}' s JOIN se_agg a
                  ON s.NFL_player_id = a.NFL_player_id""").fetchone()
            names = [d[0] for d in self.con.description]
            rec = dict(zip(names, row))
            for c in runnable:
                eras = {"career_span": {"overlap": int(rec[f"ov_{c.lhs}"]),
                                        "violations": int(rec[f"vi_{c.lhs}"])}}
                res = self._verdict(eras, MIN_OVERLAP_ROWS)
                if res["verdict"] in ("REFUTED", "CONDITIONAL"):
                    eps = _tol(c.lhs)
                    res["counterexamples"] = [
                        {"NFL_player_id": p, "stored": s, "derived": d}
                        for p, s, d in self.con.execute(f"""
                            SELECT s.NFL_player_id, s.{c.lhs}, a.{c.lhs}
                            FROM '{table}' s JOIN se_agg a
                              ON s.NFL_player_id = a.NFL_player_id
                            WHERE s.{c.lhs} IS NOT NULL AND a.{c.lhs} IS NOT NULL
                              AND ABS(s.{c.lhs} - a.{c.lhs}) > {eps}
                            ORDER BY ABS(s.{c.lhs} - a.{c.lhs}) DESC
                            LIMIT 3""").fetchall()]
                results[c.cand_id] = res
        return results

    # ---------------- brute empirical miner ----------------

    def mine(self, known: set[tuple]) -> list[Candidate]:
        """Sieve exact linear relations on deterministic hash-samples; return
        survivor candidates for full-data verification. `known` = (lhs, sorted rhs,
        op) triples already proposed mechanically."""
        import numpy as np

        stats = load_contract_stats()
        scope = scope_columns(stats)
        groups: dict[str, list[str]] = {}
        cols = self.cols(self.release)
        for stat_id, s in sorted(scope.items()):
            if s.get("unit") in MINE_UNITS and stat_id in cols \
                    and s.get("aggregation_class") in ("SUM", "MAX"):
                groups.setdefault(s["unit"], []).append(stat_id)

        def sample(mod: int, columns: list[str]) -> "np.ndarray":
            sel = ", ".join(f'CAST("{c}" AS DOUBLE)' for c in columns)
            rows = self.con.execute(f"""
                SELECT {sel} FROM '{self.release}'
                WHERE hash(player_week) % {mod} = 0""").fetchall()
            arr = np.array(rows, dtype=np.float64)
            return arr if arr.size else np.empty((0, len(columns)))

        survivors: list[Candidate] = []
        unsieved: list[str] = []
        for unit, columns in sorted(groups.items()):
            a1 = sample(SIEVE_STAGE1_MOD, columns)
            a2 = None  # lazily loaded stage-2 sample
            n = len(columns)
            nn1 = ~np.isnan(a1) if a1.size else np.zeros((0, n), bool)
            dense = [i for i in range(n) if nn1[:, i].sum() >= SIEVE_MIN_OVERLAP]
            unsieved += [columns[i] for i in range(n) if i not in set(dense)]

            def check(vals_l, vals_r, need_lt=False):
                both = ~np.isnan(vals_l) & ~np.isnan(vals_r)
                if both.sum() < SIEVE_MIN_OVERLAP:
                    return False
                dl, dr = vals_l[both], vals_r[both]
                if np.count_nonzero(dl) < SIEVE_MIN_NONZERO:
                    return False
                if need_lt:
                    return bool(np.all(dl <= dr + 1e-6) and np.any(dl < dr - 1e-6))
                return bool(np.max(np.abs(dl - dr)) <= 1e-6
                            and np.unique(dl).size >= 3)

            def check_triple(vl, vb, vc):
                both = ~np.isnan(vl) & ~np.isnan(vb) & ~np.isnan(vc)
                if both.sum() < SIEVE_MIN_OVERLAP:
                    return False
                dl, db, dc = vl[both], vb[both], vc[both]
                # each component must carry signal, else the "sum" is a pair alias
                if min(np.count_nonzero(dl), np.count_nonzero(db),
                       np.count_nonzero(dc)) < SIEVE_MIN_NONZERO:
                    return False
                return bool(np.max(np.abs(dl - (db + dc))) <= 1e-6
                            and np.unique(dl).size >= 3)

            def stage2(i, j=None, k=None, op="eq"):
                nonlocal a2
                if a2 is None:
                    a2 = sample(SIEVE_STAGE2_MOD, columns)
                if k is not None:
                    return check_triple(a2[:, i], a2[:, j], a2[:, k])
                return check(a2[:, i], a2[:, j], need_lt=(op == "le"))

            for ii, i in enumerate(dense):
                for j in dense[ii + 1:]:
                    li, lj = columns[i], columns[j]
                    if check(a1[:, i], a1[:, j]) and stage2(i, j):
                        if (li, (lj,), "eq") not in known and (lj, (li,), "eq") not in known:
                            survivors.append(Candidate(
                                f"R1:player_week:{li}={lj}", "R1", "alias", "eq",
                                li, (lj,), "player_week", "row_formula",
                                "empirical_mined"))
                    elif check(a1[:, i], a1[:, j], need_lt=True) and stage2(i, j, op="le"):
                        if (li, (lj,), "le") not in known:
                            survivors.append(Candidate(
                                f"R2:player_week:{li}<={lj}", "R2", "bound", "le",
                                li, (lj,), "player_week", "row_bound",
                                "empirical_mined"))
                    elif check(a1[:, j], a1[:, i], need_lt=True) and stage2(j, i, op="le"):
                        if (lj, (li,), "le") not in known:
                            survivors.append(Candidate(
                                f"R2:player_week:{lj}<={li}", "R2", "bound", "le",
                                lj, (li,), "player_week", "row_bound",
                                "empirical_mined"))
            # triples A = B + C
            for ii, j in enumerate(dense):
                for k in dense[ii + 1:]:
                    for i in dense:
                        if i in (j, k):
                            continue
                        li = columns[i]
                        rhs = tuple(sorted((columns[j], columns[k])))
                        if (li, rhs, "eq") in known:
                            continue
                        if check_triple(a1[:, i], a1[:, j], a1[:, k]) \
                                and stage2(i, j, k):
                            survivors.append(Candidate(
                                f"R1:player_week:{li}={'+'.join(rhs)}", "R1",
                                "sum_identity", "eq", li, rhs, "player_week",
                                "row_formula", "empirical_mined"))
        self._mine_unsieved = sorted(set(unsieved))
        seen: dict[str, Candidate] = {}
        for c in survivors:
            seen.setdefault(c.cand_id, c)
        return sorted(seen.values(), key=lambda c: c.cand_id)


def run(src: str | None = None, no_mine: bool = False,
        only: set[str] | None = None) -> dict:
    r = Runner(src)
    cands = generate()
    if only:
        cands = [c for c in cands if c.r_class in only]

    known = {(c.lhs, tuple(sorted(c.rhs)), c.op) for c in cands}
    mined: list[Candidate] = []
    if not no_mine and (only is None or {"R1", "R2"} & only):
        mined = r.mine(known)
    existing = {c.cand_id for c in cands}
    mined = [c for c in mined if c.cand_id not in existing]
    all_cands = cands + mined

    r._build_team_week([c for c in all_cands
                        if c.test_kind in ("team_mirror", "team_bound",
                                           "team_horizontal")])
    r._build_idp_team_week(all_cands)
    r._build_cross_tw(all_cands)
    grain_results = r.run_grain_batch([c for c in all_cands
                                       if c.test_kind == "grain_agg"])
    witness_results = r.run_team_witness_batch(
        [c for c in all_cands if c.test_kind == "team_witness_join"])

    verdicts = []
    for c in all_cands:
        if c.escalation:
            res = {"verdict": "ESCALATED", "era_scope": [], "eras": {},
                   "escalation": c.escalation, "near_miss_eras": []}
        elif c.test_kind == "team_witness_join":
            res = witness_results.get(c.cand_id) or {
                "verdict": "PENDING_TEST", "era_scope": [], "eras": {},
                "deficit": "team witness source has no contracted K-plane join yet",
                "near_miss_eras": []}
        elif c.test_kind == "grain_agg":
            res = grain_results[c.cand_id]
        elif c.test_kind in ("row_formula", "row_bound", "partition_sum"):
            res = r.run_row(c)
        elif c.test_kind == "row_rate":
            res = r.run_rate(c)
        elif c.test_kind == "team_idp_vertical":
            res = r.run_team_idp(c)
        elif c.test_kind == "team_cross_side":
            res = r.run_team_cross_side(c)
        elif c.test_kind in ("team_mirror", "team_bound"):
            res = r.run_team_mirror(c)
        elif c.test_kind == "team_horizontal":
            res = r.run_team_horizontal(c)
        elif c.test_kind == "league_conservation":
            res = r.run_league(c)
        else:
            res = {"verdict": "PENDING_TEST", "era_scope": [], "eras": {},
                   "deficit": f"no harness for test_kind={c.test_kind}",
                   "near_miss_eras": []}
        verdicts.append({**{k: (list(v) if isinstance(v, tuple) else v)
                            for k, v in c.__dict__.items()}, **res})

    counts: dict[str, int] = {}
    for v in verdicts:
        counts[v["verdict"]] = counts.get(v["verdict"], 0) + 1
    return {
        "version": "v1",
        "r4_team_witness_join_receipt": getattr(r, "_box_receipt", None),
        "release": r.release,
        "tolerance_law": "stat_contracts tolerance_policy via witness_votes.tolerance_of "
                         "(§17.1, default EXACT float-eps)",
        "params": {"min_overlap_rows": MIN_OVERLAP_ROWS,
                   "min_overlap_years": MIN_OVERLAP_YEARS,
                   "near_miss_rate": NEAR_MISS_RATE,
                   "eras": [e[0] for e in ERAS]},
        "candidates_mechanical": len(cands),
        "candidates_mined": len(mined),
        "mined_ids": [c.cand_id for c in mined],
        "mine_unsieved_columns": getattr(r, "_mine_unsieved", []),
        "verdict_counts": dict(sorted(counts.items())),
        "near_miss_flags": [v["cand_id"] for v in verdicts if v["near_miss_eras"]],
        "verdicts": verdicts,
    }


def merge_into_receipts(partial: dict) -> dict:
    """Fold a partial (--only) run into the committed receipts doc by cand_id.

    Guardrail: a partial run must never shrink the receipt space -- every verdict
    already in the doc survives unless this run re-ran that exact candidate, and
    every candidate the generator now proposes must end up with a verdict (either
    from this run or already present). Violations raise; nothing is written."""
    with open(os.path.abspath(SUMMARY_PATH), encoding="utf-8") as f:
        doc = json.load(f)
    new_by_id = {v["cand_id"]: v for v in partial["verdicts"]}
    kept = [v for v in doc["verdicts"] if v["cand_id"] not in new_by_id]
    merged = kept + list(new_by_id.values())
    have = {v["cand_id"] for v in merged}
    expected = {c.cand_id for c in generate()} | set(doc.get("mined_ids", []))
    missing = expected - have
    if missing:
        raise RuntimeError(f"merge would leave {len(missing)} candidates without "
                           f"verdicts, e.g. {sorted(missing)[:5]} -- run them or "
                           "run the full suite")
    merged.sort(key=lambda v: (v["r_class"], v["cand_id"]))
    counts: dict[str, int] = {}
    for v in merged:
        counts[v["verdict"]] = counts.get(v["verdict"], 0) + 1
    doc["verdicts"] = merged
    doc["verdict_counts"] = dict(sorted(counts.items()))
    doc["candidates_mechanical"] = len(generate())
    doc["near_miss_flags"] = [v["cand_id"] for v in merged if v.get("near_miss_eras")]
    if partial.get("r4_team_witness_join_receipt"):
        doc["r4_team_witness_join_receipt"] = partial["r4_team_witness_join_receipt"]
    doc.pop("partial_scope", None)
    return doc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src")
    ap.add_argument("--no-mine", action="store_true")
    ap.add_argument("--only", action="append", help="restrict to R classes")
    ap.add_argument("--merge", action="store_true",
                    help="fold an --only run into the existing receipts doc by "
                         "cand_id (guardrailed; avoids a full re-run)")
    ap.add_argument("--no-write", action="store_true")
    a = ap.parse_args()
    if a.merge and not a.only:
        ap.error("--merge requires --only (a full run overwrites, never merges)")
    doc = run(a.src, no_mine=a.no_mine, only=set(a.only) if a.only else None)
    print(f"mechanical={doc['candidates_mechanical']}  mined={doc['candidates_mined']}")
    print("verdicts:", doc["verdict_counts"])
    print(f"near-miss flags (definition-drift queue): {len(doc['near_miss_flags'])}")
    if a.no_write:
        return 0
    from .recon_common import utc_stamp
    if a.merge:
        doc = merge_into_receipts(doc)
        print("merged verdicts:", doc["verdict_counts"])
    elif a.only:
        doc["partial_scope"] = sorted(a.only)
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    print(f"summary -> {os.path.abspath(SUMMARY_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
