"""
sota_recon/golden_points.py  --  GOLDEN MATRIX for the POINTS/scoring layer (all grains).

golden_grid/golden_matrix lock the STAT atoms; this locks the DERIVED POINTS columns -- the layer where
the def-TD double-count, ff-baseline, and ST-TD triple-count all hid (stats were fine, scoring wasn't).

TWO kinds of check:
  FORMULA  -- recompute each scoring column from atoms with the CANONICAL (de-duped) formula and assert an
              EXACT row-by-row match to the stored value. This is the bug-catcher: a double-count, wrong
              weight, or missing de-dup shows up as mismatched rows. Uses TD_DEDUP (int_ret_td+fum_ret_td
              when >0 else def_tds) so it CATCHES naive def_tds+fum_ret_td double-counts (DEF and IDP).
  LEADERS  -- freeze the leader (holder+value) for key points cols at week (weekly super table), season
              (player_nfl_season) and career (player_nfl_career) -> drift net covering the AGGREGATE tables
              too. First run freezes golden_points_frozen.json; later runs fail on drift.

    python -m scripts.sota_recon.golden_points            # formula checks + leader compare/freeze
    python -m scripts.sota_recon.golden_points --refreeze # rewrite the leader master
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import duckdb
from .sources import latest_v26
# Canonical offensive recipe, shared with build_rescore_fpts_v26 (one source of truth). The offense-core
# check locks fpts_4pt_half to THIS exactly -- gating, not informational (that tolerance is how the
# fumble/return-TD scoring bugs hid).
from .offense_recipe import EXPECTED_4PT_HALF, OFF_ELIG

FROZEN = Path("D:/league-history-data/nfl/derived/validation/golden_points_frozen.json")
D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"
IDP_POS = "('LB','DL','DB','DE','DT','CB','S','ILB','OLB','MLB','NT','SS','FS','EDGE','SAF')"

LEADER_COLS = ["pts_def_std", "pts_idp_std", "pts_k_std", "fpts_4pt_half", "fpts_6pt_ppr",
               "lamar_12t_flx_half_4pt", "lamar_12t_sflx_half_6pt", "lamar_12t_idp_std_4pt"]

# Simple num/den rates: each MUST equal Sigma(num)/Sigma(den) at season AND career (recompute, not avg).
# (The complex rates -- passer_rating, yards_per_touch, sack_pct, target_share/air_yards_share/wopr --
# are not plain num/den so they're verified manually + excluded from recon_aggregate's additive check.)
SIMPLE_RATE_NUMDEN = {
    "comp_pct": ("completions", "attempts"), "fg_pct": ("fg_made", "fg_att"),
    "yards_per_attempt": ("passing_yards", "attempts"), "yards_per_carry": ("rushing_yards", "carries"),
    "yards_per_reception": ("receiving_yards", "receptions"), "catch_rate": ("receptions", "targets"),
    "pacr": ("passing_yards", "passing_air_yards"), "racr": ("receiving_yards", "receiving_air_yards"),
    "yards_per_target": ("receiving_yards", "targets"), "rec_td_pct": ("receiving_tds", "targets"),
    "rush_td_pct": ("rushing_tds", "carries"), "passing_td_pct": ("passing_tds", "attempts"),
    "passing_int_pct": ("passing_interceptions", "attempts"), "adot": ("passing_air_yards", "attempts"),
    "xp_pct": ("pat_made", "pat_att"),
}


def _rate_checks(con, sea, car):
    """Lock every simple num/den rate at season + career: stored == Sigma(num)/Sigma(den). A wrong
    aggregation (avg-of-weekly-rates, or summing a rate) shows up as mismatched rows."""
    out = []
    for grain, src in (("season", sea), ("career", car)):
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()}
        for col, (num, den) in SIMPLE_RATE_NUMDEN.items():
            if not ({col, num, den} <= cols):
                continue
            # compare only where denom>0 (rate defined); tolerate the col's stored rounding
            bad = con.execute(
                f"SELECT COUNT(*) FROM '{src}' WHERE {D(den)}>0 AND {col} IS NOT NULL "
                f"AND ABS({D(col)} - ({D(num)}/{D(den)})) > 0.01"
            ).fetchone()[0]
            n = con.execute(f"SELECT COUNT(*) FROM '{src}' WHERE {D(den)}>0 AND {col} IS NOT NULL").fetchone()[0]
            out.append({"check": f"{grain}.{col}==sum/sum", "rows": n, "mismatch": bad, "ok": bad == 0})
    return out


def _formulas(con, V):
    """Build canonical scoring formulas with a schema-guarded accessor (mirrors calculator safe_col:
    a missing column -> 0). Must mirror fantasy_points_calculator / build_*_v26 exactly."""
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {V}").fetchall()}
    g = lambda c: D(c) if c in cols else "0"
    # De-duped defensive TDs: GREATEST of the disjoint return-TD component sum and box-score def_tds
    # (mirrors build_dst_scoring_v26.TD_DEDUP / fantasy_points_calculator). Taking the MAX (never adding)
    # avoids the modern double-count AND stops dropping pre-2000 box-score TDs where the PBP split is
    # incomplete. Was a CASE-WHEN(sum>0) that under-counted 72 DEF rows vs the stored GREATEST value.
    td = f"GREATEST({g('def_int_ret_td')}+{g('fum_ret_td')}, {g('def_tds')})"
    pa = (f"({g('pts_allow_0')}*10+{g('pts_allow_1_6')}*7+{g('pts_allow_7_13')}*4+{g('pts_allow_14_20')}*1"
          f"+{g('pts_allow_28_34')}*-1+{g('pts_allow_35_plus')}*-4)")
    # Return TDs (KR/PR) are baked into pts_def_std at the modal +6 (89% of league-years, median 6);
    # pts_def_ret_td is special-teams-only, disjoint from `td` (defensive INT/fum/blk returns).
    f_def = (f"{g('def_sacks')}*1+{g('def_interceptions')}*2+{g('fum_rec')}*2+{td}*6"
             f"+{g('def_safeties')}*2+{g('pts_def_block')}*2+{g('pts_def_ret_td')}*6+{pa}")
    # tackle term MUST mirror build_idp_scoring_v26.TACK: split (solo + assist*0.5) when split
    # tackles exist, else fall back to the reported combined tackles (older eras w/o splits).
    tack = (f"(CASE WHEN ({g('def_tackles_solo')}+{g('def_tackle_assists')})<>0 "
            f"THEN ({g('def_tackles_solo')}+{g('def_tackle_assists')}*0.5) ELSE {g('def_tackles_with_assist')} END)")
    f_idp = (f"{tack}*1.0+{g('def_sacks')}*4+{g('def_interceptions')}*6+{g('def_fumbles_forced')}*3+{g('fum_rec')}*2"
             f"+{g('def_tackles_for_loss')}*2+{g('def_pass_defended')}*3+{g('def_qb_hits')}*1+{g('def_safeties')}*2+{td}*6")
    fg60 = f"({g('fg_made_60_')}+{g('fg_made_60_plus')})"  # calculator 60+ union (safe_col->0 if absent)
    # MUST mirror build_kicker_scoring_v26 pts_k_std (modal league config): 60+ FG = 5 (plurality,
    # not 6), and missed XP is penalized -1 (in addition to missed FG -1).
    f_k = (f"((CASE WHEN ({g('fg_made_0_19')}+{g('fg_made_20_29')}+{g('fg_made_30_39')}+{g('fg_made_40_49')}+{g('fg_made_50_59')}+{fg60})>0 "
           f"THEN ({g('fg_made_0_19')}+{g('fg_made_20_29')}+{g('fg_made_30_39')})*3+{g('fg_made_40_49')}*4+{g('fg_made_50_59')}*5+{fg60}*5 "
           f"ELSE {g('fg_made')}*3 END) + {g('pat_made')}*1 + {g('fg_missed')}*-1 + {g('pat_missed')}*-1)")
    f_off = (f"{g('passing_yards')}*0.04+{g('passing_tds')}*4+{g('passing_interceptions')}*-2"
             f"+{g('rushing_yards')}*0.1+{g('rushing_tds')}*6+{g('receiving_yards')}*0.1+{g('receiving_tds')}*6"
             f"+{g('receptions')}*0.5+{g('fumbles_lost')}*-2")
    return {"TD": td, "F_DEF": f_def, "F_IDP": f_idp, "F_K": f_k, "F_OFF": f_off}


def _con():
    c = duckdb.connect(); c.execute("PRAGMA threads=3"); c.execute("SET memory_limit='6GB'")
    c.execute("PRAGMA disable_progress_bar"); c.execute("SET preserve_insertion_order=false")
    sp = Path(latest_v26()).parent / ".ptsspill"; sp.mkdir(exist_ok=True)
    c.execute(f"SET temp_directory='{sp.as_posix()}'")
    return c


# Exact-value anchors for the enrichment/eligibility layer (age, awards, dual-position). These columns
# are new and otherwise unlocked; freeze known-truth values so a future rebuild can't silently break them.
# (table, player, year-or-None, column, expected)
ENRICH_ANCHORS = [
    ("season", "Travis Hunter", 2025, "season_positions", "WR,DB"),
    ("season", "Devontez Walker", 2025, "season_positions", "WR"),
    ("season", "Deion Sanders", 1996, "season_positions", "WR,DB"),
    ("season", "Deion Sanders", 1993, "season_positions", "DB"),
    ("season", "Cookie Gilchrist", 1962, "season_positions", "RB,K"),
    ("season", "George Blanda", 1962, "season_positions", "QB,K"),
    ("career", "George Blanda", None, "career_positions", "QB,K"),
    ("career", "Jerry Rice", None, "career_all_pro_first", 11),
    ("season", "Peyton Manning", 2013, "age", 37),
    ("career", "Deion Sanders", None, "career_positions", "WR,DB"),
    # dual-eligibility battery (locks the crossover inference for famous two-way players)
    ("career", "Sammy Baugh", None, "career_positions", "QB,DB,P"),
    ("career", "Paul Hornung", None, "career_positions", "RB,K"),
    ("career", "Mike Vrabel", None, "career_positions", "LB"),   # goal-line TDs != TE eligibility
    ("career", "William Perry", None, "career_positions", "DL"),  # 6 career carries != RB eligibility
    ("career", "Julian Edelman", None, "career_positions", "WR"),   # 2011 CB stint was tackle-only (no coverage stat) -> not flagged
    ("career", "Matthew Slater", None, "career_positions", "WR"),    # ST gunner, NOT a DB (regression guard)
    ("career", "Chuck Bednarik", None, "career_positions", "LB,P,OL"),
    ("career", "Troy Brown", None, "career_positions", "WR,DB"),
    # award anchors (lock the voting-award ingestion)
    ("career", "Lawrence Taylor", None, "career_dpoy", 3),
    ("career", "Barry Sanders", None, "career_opoy", 2),
    ("career", "Aaron Donald", None, "career_dpoy", 3),
]


def _enrichment_checks(con, sea, car):
    """Exact-value anchors on the enrichment/eligibility columns (locks age/awards/dual-position)."""
    out = []
    for grain, player, year, col, expected in ENRICH_ANCHORS:
        src = sea if grain == "season" else car
        where = f"player='{player}'" + (f" AND year={year}" if year is not None else "")
        try:
            r = con.execute(f"SELECT {col} FROM '{src}' WHERE {where}").fetchone()
        except Exception:
            out.append({"check": f"enrich:{player}/{col}", "rows": 0, "mismatch": 1, "ok": False}); continue
        got = r[0] if r else None
        if isinstance(expected, (int, float)) and got is not None:
            ok = abs(float(got) - float(expected)) < 0.5
        else:
            ok = (str(got) == str(expected))
        out.append({"check": f"enrich:{player} {col}={expected}", "rows": 1, "mismatch": 0 if ok else 1, "ok": ok})
    return out


# Coefficients baked into the precompute recipes (offense_recipe.EXPECTED_4PT_HALF + build_dst_scoring_v26
# DEF_BASE + F_K/F_DEF here). The fleet-mode cross-check asserts these equal the frozen fleet-MODAL league
# config (___leagues MODE, captured to fleet_mode_coeffs.json). A divergence = the precompute bakes a
# non-modal recipe (a coverage/recipe error like the DST return-TD omission) OR the fleet drifted.
FLEET_MODE_FROZEN = Path("D:/league-history-data/nfl/derived/validation/fleet_mode_coeffs.json")
BAKED_COEFFS = {
    "pass_yd": 0.04, "pass_td": 4, "pass_int": 2, "rush_yd": 0.1, "rush_td": 6,
    "rec_yd": 0.1, "rec_td": 6, "fum_lost": -2, "def_st_td": 6, "def_td": 6,
    "def_int_ret_td": 6, "def_fum_ret_td": 6,
    # DST base + points-allowed tiers -- MODE-validated vs ___leagues 2026-07-15 (the recipe in F_DEF/PA
    # must equal the fleet mode; the PA tiers are the piece that has drifted before).
    "def_sack": 1, "def_fum_rec": 2, "def_safe": 2,
    "pa_0": 10, "pa_1_6": 7, "pa_7_13": 4, "pa_14_20": 1, "pa_21_27": 0, "pa_28_34": -1, "pa_35p": -4,
}


def _fleet_mode_check():
    """Cross-check: baked recipe coefficients == frozen fleet-modal league config."""
    if not FLEET_MODE_FROZEN.exists():
        return [{"check": "coeffs==fleet-mode (frozen file missing)", "rows": 0, "mismatch": 1, "ok": False}]
    fleet = json.loads(FLEET_MODE_FROZEN.read_text())
    out = []
    bad = 0
    for k, v in BAKED_COEFFS.items():
        fv = fleet.get(k)
        ok = fv is not None and abs(float(fv) - float(v)) < 1e-9
        if not ok:
            bad += 1
    out.append({"check": "coeffs==fleet-mode (recipe vs ___leagues MODE)",
                "rows": len(BAKED_COEFFS), "mismatch": bad, "ok": bad == 0})
    return out


def _pa_tier_check(con, V):
    """GATE: the flagged pts_allow_* tier MUST bucket the FANTASY PA (dst_points_allowed), never raw
    points_allowed. Deriving tiers from raw re-introduces the 2014-24 inflation (a DST charged for pick-6s
    its offense threw). Verified 100% on 2,633 divergent rows 2026-07-15; lock it so it can't regress."""
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {V}").fetchall()}
    if "dst_points_allowed" not in cols:
        return []
    tier = lambda c: (f"CASE WHEN {D(c)}=0 THEN 0 WHEN {D(c)}<=6 THEN 1 WHEN {D(c)}<=13 THEN 2 "
                      f"WHEN {D(c)}<=20 THEN 3 WHEN {D(c)}<=27 THEN 4 WHEN {D(c)}<=34 THEN 5 ELSE 6 END")
    flag = (f"CASE WHEN {D('pts_allow_0')}>0 THEN 0 WHEN {D('pts_allow_1_6')}>0 THEN 1 WHEN {D('pts_allow_7_13')}>0 THEN 2 "
            f"WHEN {D('pts_allow_14_20')}>0 THEN 3 WHEN {D('pts_allow_21_27')}>0 THEN 4 WHEN {D('pts_allow_28_34')}>0 THEN 5 "
            f"WHEN {D('pts_allow_35_plus')}>0 THEN 6 ELSE -1 END")
    where = ("position='DEF' AND year>=1994 AND (season_type='REG' OR season_type IS NULL) "
             f"AND ({D('pts_allow_0')}+{D('pts_allow_1_6')}+{D('pts_allow_7_13')}+{D('pts_allow_14_20')}"
             f"+{D('pts_allow_21_27')}+{D('pts_allow_28_34')}+{D('pts_allow_35_plus')})>0")
    n = con.execute(f"SELECT COUNT(*) FROM {V} WHERE {where}").fetchone()[0]
    bad = con.execute(f"SELECT COUNT(*) FROM {V} WHERE {where} AND ({flag})<>({tier('dst_points_allowed')})").fetchone()[0]
    return [{"check": "pts_allow tier == FANTASY PA bucket (not raw) [GATE]", "rows": n, "mismatch": bad, "ok": bad == 0}]


def _formula_checks(con, V):
    out = []
    F = _formulas(con, V)
    def chk(name, where, col, formula, tol=0.011):
        n = con.execute(f"SELECT COUNT(*) FROM {V} WHERE {where} AND {col} IS NOT NULL").fetchone()[0]
        bad = con.execute(f"SELECT COUNT(*) FROM {V} WHERE {where} AND {col} IS NOT NULL AND ABS({D(col)}-({formula}))>{tol}").fetchone()[0]
        out.append({"check": name, "rows": n, "mismatch": bad, "ok": bad == 0})
    chk("pts_def_std==DST_formula", "position='DEF'", "pts_def_std", F["F_DEF"])
    chk("pts_idp_std==IDP_formula(de-duped TD)", f"nfl_position IN {IDP_POS}", "pts_idp_std", F["F_IDP"])
    chk("pts_k_std==K_formula", "nfl_position='K'", "pts_k_std", F["F_K"])
    # de-dup guard: pts_def_td must equal the de-duped TD (never def_tds+fum_ret_td)
    chk("pts_def_td==dedup (no double-count)", "position='DEF'", "pts_def_td", F["TD"])
    # idp_td de-dup guard: pts_idp_td must equal the de-duped TD (no pick-six drop, no fum double-count)
    chk("pts_idp_td==dedup (pick-six kept)", f"nfl_position IN {IDP_POS}", "pts_idp_td", F["TD"])
    # offense core (GATING, EXACT): fpts_4pt_half MUST equal the canonical offense recipe imported from
    # offense_recipe (the same string build_rescore_fpts_v26 uses to detect stale rows). A fumble/return-TD
    # double-count, a wrong weight, or a missing de-dup now FAILS here row-for-row. Unsplit-doubleheader
    # duplicate player_weeks are excluded (build_rescore holds them for the DH wave-2 queue; a player_week
    # keyed recompute would stamp one game onto both physical rows).
    dh = f"player_week IN (SELECT player_week FROM {V} GROUP BY 1 HAVING COUNT(*)>1)"
    base = f"({OFF_ELIG}) AND fpts_4pt_half IS NOT NULL AND NOT ({dh})"
    n = con.execute(f"SELECT COUNT(*) FROM {V} WHERE {base}").fetchone()[0]
    bad = con.execute(f"SELECT COUNT(*) FROM {V} WHERE {base} AND ABS({D('fpts_4pt_half')}-({EXPECTED_4PT_HALF}))>0.011").fetchone()[0]
    out.append({"check": "fpts_4pt_half==canonical recipe (GATING/exact)", "rows": n, "mismatch": bad, "ok": bad == 0})
    return out


def _leaders(con, src, grain):
    g = {}
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()}
    for c in LEADER_COLS:
        if c not in cols:
            continue
        if grain == "week":
            row = con.execute(f"SELECT NFL_player_id, ROUND({D(c)},2) v, year FROM '{src}' WHERE {c} IS NOT NULL ORDER BY {D(c)} DESC, NFL_player_id LIMIT 1").fetchone()
            g[f"week|{c}"] = [str(row[0]), float(row[1]), int(row[2]) if row[2] else None] if row else None
        else:  # season/career tables: leader by the stored season/career value
            row = con.execute(f"SELECT NFL_player_id, ROUND({D(c)},2) v{', year' if grain=='season' else ''} FROM '{src}' WHERE {c} IS NOT NULL ORDER BY {D(c)} DESC, NFL_player_id LIMIT 1").fetchone()
            if row:
                g[f"{grain}|{c}"] = [str(row[0]), float(row[1])] + ([int(row[2])] if grain == "season" and row[2] else [])
    return g


def run(refreeze=False):
    con = _con()
    v26 = latest_v26(); V = f"read_parquet('{v26}')"
    art = Path(v26).parent / "season_career_v26"
    sea = (art / "player_nfl_season.parquet").as_posix(); car = (art / "player_nfl_career.parquet").as_posix()
    formula = (_formula_checks(con, V) + _pa_tier_check(con, V) + _rate_checks(con, sea, car)
               + _enrichment_checks(con, sea, car) + _fleet_mode_check())
    leaders = {}
    leaders.update(_leaders(con, v26, "week"))
    leaders.update(_leaders(con, sea, "season"))
    leaders.update(_leaders(con, car, "career"))
    con.close()
    f_fail = sum(1 for f in formula if not f["ok"])
    drift = []
    if refreeze or not FROZEN.exists():
        FROZEN.parent.mkdir(parents=True, exist_ok=True)
        FROZEN.write_text(json.dumps(leaders, indent=0, sort_keys=True, default=str))
        action = "frozen"
    else:
        action = "compared"
        frozen = json.loads(FROZEN.read_text())
        for k, fv in frozen.items():
            cur = leaders.get(k)
            if cur is None or cur[0] != fv[0] or abs(cur[1] - fv[1]) > 0.011:
                drift.append((k, fv, cur))
    return {"action": action, "formula": formula, "formula_fail": f_fail,
            "leaders": len(leaders), "drift": drift, "passed_overall": f_fail == 0 and not drift}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--refreeze", action="store_true"); a = ap.parse_args()
    r = run(refreeze=a.refreeze)
    print("FORMULA CONSISTENCY (recompute-from-atoms == stored):")
    for f in r["formula"]:
        print(f"  [{'PASS' if f['ok'] else 'FAIL'}] {f['check']:42} {f['mismatch']:>6} / {f['rows']:>7,} mismatch")
    print(f"\nLEADER ANCHORS: {r['leaders']} ({r['action']}); drift={len(r['drift'])}")
    for k, fv, cur in r["drift"]:
        print(f"  DRIFT {k}: frozen={fv} now={cur}")
    print(f"\nVERDICT: {'PASS' if r['passed_overall'] else 'FAIL'} (formula_fail={r['formula_fail']})")
