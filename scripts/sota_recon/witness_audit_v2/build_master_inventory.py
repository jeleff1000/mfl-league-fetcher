# -*- coding: utf-8 -*-
"""Build the Super-Table Witness & Coverage Ledger — one browsable file that maps EVERY
column of every super-table grain (player_bio / weekly / season / career) to EVERY witness,
and surfaces the gaps that are not sealed under three independent questions:

    witnessed?  a reconciliation path exists (source or witnessed formula)
    covered?    the column is actually populated across the era the witness can reach
    verified?   a live value control (golden recipe / mirror / conservation law) gates the number

Inputs (ecosystem, read-only):
  docs/witness-column-master-matrix.json   verdicts per column per grain (2026-07-17 scan)
  docs/witness-contracts-v2.json           158 witness contracts (the "witness pages")
  <workdir>/era_rescue_ledger.json         achievable-vs-actual era per column
  the current v26 release parquets          column set + live weekly era-floors (recomputed here)
  supertable-witness-master-audit-2026-07-16.md  -> the hand-encoded + live-verified defect register

Outputs:
  docs/witness-coverage-master-inventory.json   machine source of truth
  docs/witness-coverage-master-inventory.html   standalone browsable ledger (also published as an Artifact)

Nothing here mutates any table, pipeline, or prod. Re-run after any super-table promote.
Run:  python -m scripts.sota_recon.witness_audit_v2.build_master_inventory
"""
import glob, json, re, html
from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parents[3]
LAKE = Path("D:/league-history-data/nfl")
OPS = LAKE / "ops_data/nfl_historical"
WORK = LAKE / "derived/validation/witness_audit_2026_07_16"
DOCS = REPO / "docs"


def latest_v26():
    files = sorted(glob.glob(str(LAKE / "releases" / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
                   key=lambda p: Path(p).stat().st_mtime, reverse=True)
    return Path(files[0])

IDENTITY = {
    "NFL_player_id", "player_id", "gsis_id", "pfr_id", "boxscore_id", "player", "first_name", "last_name", "full_name", "player_week",
    "year", "week", "season_type", "team", "opponent", "nfl_team", "opponent_nfl_team", "home_away", "game_date", "games",
    "nfl_franchise_number", "opponent_franchise_number", "opponent_nfl_franchise_number", "position", "nfl_position", "primary_position", "fantasy_position", "starter_position", "managers", "headshot_url",
    "data_source", "height", "weight", "age", "college", "birth_date", "draft_year", "draft_round", "draft_pick", "years",
}

def classify(col):
    c = col.lower()
    if col in IDENTITY or c.endswith(("_id", "_url", "_name", "_key", "_date", "_slug")): return "identity"
    if re.search(r"(_recomputed_at_|_repaired_at_|_merged_at_|_populated_at_|_recompute_|collision_repaired)", c) \
            or c.startswith(("is_", "has_", "flag_", "qa_", "prov_", "src_", "source_")) \
            or c.endswith(("_source", "_provenance", "_flag", "_imputed", "_is_estimated")) \
            or c in {"recon_correction_log", "recon_correction"}:
        return "flag_provenance"
    if c.startswith(("pts_", "fpts_")) or c.endswith(("_fpts", "_pts_std", "_pts_ppr", "_pts_half")): return "scoring"
    if c.startswith("rank_") or c.endswith(("_rank", "_pctl", "_percentile")): return "rank"
    if "lamar" in c or c.startswith("research_") or "replacement" in c: return "research"
    if c.startswith(("ppg_", "rolling_", "weighted_ppg", "consistency_", "avg_pts_next", "avg_", "pct_")) \
            or c.endswith(("_ppg", "_per_game", "_avg", "_pct", "_rate", "_share", "_ratio", "_per_att", "_per_carry",
                           "_per_target", "_per_touch", "_per_reception", "_wmean")):
        return "rate_agg"
    return "atom"

def numeric(dt): return any(t in dt.upper() for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "HUGEINT", "REAL"))

def census_phase():
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    wk = latest_v26(); sc = wk.parent / "season_career_v26"
    tables = {"weekly": wk}
    for stem in ("player_nfl_season", "player_nfl_season_all", "player_nfl_career", "player_nfl_career_all"):
        p = sc / f"{stem}.parquet"
        if p.exists(): tables[stem] = p
    if (OPS / "player_bio.parquet").exists(): tables["player_bio"] = OPS / "player_bio.parquet"
    census = {}
    for key, p in tables.items():
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p.as_posix()}')").fetchall()
        cols = {name: {"dtype": dt, "family": classify(name)} for name, dt, *_ in desc}
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p.as_posix()}')").fetchone()[0]
        census[key] = {"path": str(p), "n_rows": n, "n_cols": len(cols), "columns": cols}
        print(f"  census {key:26} rows={n:>10,} cols={len(cols)}")

    wkcols = census["weekly"]["columns"]
    stat = [c for c, mm in wkcols.items() if mm["family"] in ("atom", "scoring", "rate_agg") and numeric(mm["dtype"])]
    floors = {}
    for i in range(0, len(stat), 110):
        chunk = stat[i:i + 110]; parts = []
        for c in chunk:
            q = '"' + c.replace('"', '""') + '"'
            parts += [f"MIN(CASE WHEN {q} IS NOT NULL AND {q}<>0 THEN year END)",
                      f"MAX(CASE WHEN {q} IS NOT NULL AND {q}<>0 THEN year END)",
                      f"SUM(CASE WHEN {q} IS NOT NULL AND {q}<>0 THEN 1 ELSE 0 END)"]
        row = con.execute(f"SELECT {', '.join(parts)} FROM read_parquet('{wk.as_posix()}')").fetchone()
        for j, c in enumerate(chunk):
            floors[c] = {"actual_floor": row[j * 3], "actual_ceiling": row[j * 3 + 1], "nonzero": row[j * 3 + 2]}
    print(f"  live weekly era-floors computed for {len(stat)} stat columns")
    return census, floors


BIO_WITNESS = {
    "height": ([*("pfr_player_page:bio", "nflverse_rosters", "newspaper_promoted:roster")], 1920, "direct", "bio_physical"),
    "weight": ([*("pfr_player_page:bio", "nflverse_rosters", "newspaper_promoted:roster")], 1920, "direct", "bio_physical"),
    "college": (["pfr_player_page:bio", "nflverse_rosters"], 1920, "direct", "bio_attr"),
    "conference": (["pfr_player_page:bio"], 1920, "text_derived", "bio_attr"),
    "high_school": (["pfr_player_page:bio"], 1936, "direct", "bio_attr"),
    "birth_date": (["pfr_player_page:bio", "nflverse_rosters"], 1920, "direct", "bio_attr"),
    "birth_place": (["pfr_player_page:bio"], 1920, "direct", "bio_attr"),
    "status": (["nflverse_rosters"], 2000, "direct", "bio_attr"),
    "latest_team": (["nflverse_rosters", "team_games"], 1920, "derived_witnessed", "bio_derived"),
    "nfl_position": ([*("pfr_player_page:bio", "nflverse_rosters", "pbp_merged")], 1920, "direct", "bio_attr"),
    "position_category": (["derived: nfl_position map"], 1920, "derived_witnessed", "bio_derived"),
    "position_side": (["derived: nfl_position map"], 1920, "derived_witnessed", "bio_derived"),
    "draft_year": (["pfr_context:draft", "nflverse_draft_picks"], 1936, "direct", "bio_draft"),
    "draft_round": (["pfr_context:draft", "nflverse_draft_picks"], 1936, "direct", "bio_draft"),
    "draft_overall": (["pfr_context:draft", "nflverse_draft_picks"], 1936, "direct", "bio_draft"),
    "nfl_draft_team": (["pfr_context:draft", "nflverse_draft_picks"], 1936, "direct", "bio_draft"),
    "age_at_draft": (["derived: draft_year - birth_date"], 1936, "derived_witnessed", "bio_derived"),
    "is_undrafted": (["derived: absence from draft witness"], 1936, "derived_witnessed", "bio_derived"),
    "forty": (["pfr_context:combine", "nflverse_combine"], 1987, "direct", "bio_combine"),
    "bench": (["pfr_context:combine", "nflverse_combine"], 1987, "direct", "bio_combine"),
    "vertical": (["pfr_context:combine", "nflverse_combine"], 1987, "direct", "bio_combine"),
    "broad_jump": (["pfr_context:combine", "nflverse_combine"], 1987, "direct", "bio_combine"),
    "cone": (["pfr_context:combine", "nflverse_combine"], 1999, "direct", "bio_combine"),
    "shuttle": (["pfr_context:combine", "nflverse_combine"], 1999, "direct", "bio_combine"),
    "w_av": (["pfr_player_page:career_av", "pfr_season:*"], 1960, "direct", "bio_value"),
    "hof": (["pfr_player_page:honors", "awards"], 1963, "text_derived", "bio_honor"),
    "allpro": (["pfr_context:all_pro", "awards"], 1920, "text_derived", "bio_honor"),
    "probowls": (["pfr_context:pro_bowl", "awards"], 1938, "text_derived", "bio_honor"),
    "seasons_started": (["derived: SUM(is_starter) weekly"], 1920, "derived_witnessed", "bio_derived"),
    "career_games": (["derived: COUNT weekly appearances"], 1920, "derived_witnessed", "bio_derived"),
    "rookie_year": (["derived: MIN(year) weekly"], 1920, "derived_witnessed", "bio_derived"),
    "first_year": (["derived: MIN(year) weekly"], 1920, "derived_witnessed", "bio_derived"),
    "last_year": (["derived: MAX(year) weekly"], 1920, "derived_witnessed", "bio_derived"),
    "years_active": (["derived: distinct years weekly"], 1920, "derived_witnessed", "bio_derived"),
    "primary_franchise_id": (["franchise_registry"], 1920, "derived_witnessed", "bio_derived"),
    "primary_team": (["franchise_registry", "team_games"], 1920, "derived_witnessed", "bio_derived"),
    "primary_team_source": (["franchise_registry"], 1920, "derived_witnessed", "bio_derived"),
    "career_history": (["franchise_registry", "team_games"], 1920, "derived_witnessed", "bio_derived"),
    "dr_av": ([], None, None, "bio_value"),
    "ras_score": ([], None, None, "bio_combine"),
}
BIO_IDENTIFIER = {"NFL_player_id", "player", "pfr_id", "espn_id", "sleeper_player_id", "yahoo_player_id", "headshot_url"}
CAREER_SCAFFOLD = {
    "games_played": ("derived_witnessed", ["derived: COUNT weekly appearances"], 1920, "bio_derived"),
    "nfl_teams": ("derived_witnessed", ["franchise_registry"], 1920, "bio_derived"),
    "nfl_team_count": ("derived_witnessed", ["franchise_registry"], 1920, "bio_derived"),
    "career_positions": ("derived_witnessed", ["derived: distinct position weekly"], 1920, "bio_derived"),
    "position_candidates": ("derived_witnessed", ["derived: position evidence"], 1920, "bio_derived"),
    "last_updated": ("identity", [], None, "flag_provenance"),
    "years_active_1": ("identity", [], None, "flag_provenance"),
}
MIRROR_PROPOSED = {
    "passing_first_downs", "passing_yards_after_catch", "passing_completed_air_yards", "passing_drops", "passing_2pt_conversions", "passing_tds_40plus", "passing_tds_50plus", "completions_40plus",
    "receiving_first_downs", "receiving_yards_after_catch", "receiving_completed_air_yards", "receiving_drops", "receiving_2pt_conversions", "receiving_tds_40plus", "receiving_tds_50plus", "receptions_40plus",
    "def_plays", "def_yards_allowed", "def_qb_hits", "def_knockdowns", "rz_pass_td", "rz_rec_td", "pass_explosive_20", "rec_explosive_20",
}
RECON_INTERNAL = {
    "completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions", "receptions", "targets",
    "receiving_yards", "receiving_tds", "def_interceptions", "fg_made", "fg_att", "pat_made", "pat_att"}
def defects_register():
    D = {
        "pick6": dict(sev="resolved_live", title="pick6 / pts_pick6* — was 100% EMPTY; NOW LIVE (resolved since audit)",
                      cols=[*("pick6", "pts_pick6", "pts_pick6_n1", "pts_pick6_n2")], era="1978+",
                      detail="Audit found pick6 100% empty in every era (2,181 real pick-sixes unrecorded); an empty column agrees with every source so no VALUE lane caught it. RE-VERIFIED 2026-07-21: now populated 1978-2025, 2,076 nonzero (confirm the ~105 delta vs 2,181 events is POST/join, not loss). The audit's 'NOT promoted' note is STALE.", ref="§9.1/§10.1"),
        "rec_target_int": dict(sev="verify", title="receiving_target_interceptions filled through the 2003-08 PBP-blind window — CONFIRM SOURCE",
                               cols=["receiving_target_interceptions"], era="1999-2017",
                               detail="Audit (§9.2/§9.7): PBP names the intended receiver on only 0.0-0.5% of INTs in 2003-08, and warned that filling 1999-2008 stamped 39,093 FALSE ZEROS. RE-VERIFIED 2026-07-21: column is populated EVERY year 1999-2017 incl. 2003-08 (~450 nonzero/yr). ~450 real target-INTs/yr cannot come from PBP in 2003-08 -> confirm the fill is a real witness (NFL.com logs / PFR charting), NOT the re-introduced false-fill the audit reverted.", ref="§9.2/§9.7"),
        "empty_components": dict(sev="empty", title="22 scoring/component columns are 100% EMPTY in the live table",
                                 cols=[], era="all",
                                 detail="pts_def_int_ret_td, pts_def_st_ff, pts_def_fg_block/pat_block/punt_block, pts_idp_blk_kick_td, pts_idp_xpr, fg_missed_0_19, etc. all-zero table-wide — same class as the pick6 empty column. Verify intent (opt-in league component computed at import) vs dead precompute; CLAUDE.md says precompute everything, so an empty precompute is a defect until proven intentional.", ref="§6/§10.1"),
        "tackles_ramp": dict(sev="incomplete_promoted", title="def_tackles_solo/assist 1978-91 ~10-20% short (event-completeness ramp)",
                             cols=[*("def_tackles_solo", "def_tackle_assists", "def_tackles_combined", "def_tackles_with_assist")], era="1978-1991",
                             detail="Conservation law (rushes - TD - kneel - fumble - OOB = tackle) proves the tackle EVENT flag is missing on 17-20% of 1978-82 plays, tapering to complete by 1992. build_pbp_idp_backfill_v26 (shipped 07-16) inherits the ramp. Closable from NFL.com logs (tackles to 1981), not from PBP.", ref="§9.8"),
        "tfl_yards": dict(sev="open_gap", title="def_tackles_for_loss_yards — 1978-98 absent + 2003-2011 INTERIOR HOLE (live-verified)",
                          cols=["def_tackles_for_loss_yards"], era="1978-1998, 2003-2011",
                          detail="RE-VERIFIED 2026-07-21 by decade: TFL count present 1978+ but yards=0 for 1978-1998; present 1999-2002 (.75-.88); ZERO again 2003-2011 (9-yr interior hole, ~18k TFL rows, no yards); present 2012+ (.88-1.00). Confirms audit §5b Tier-A. Recompute 2003-2011 same recipe; 1978-98 needs a witness; multi-tackler yardage split is a semantics ruling.", ref="§9.5/§5b"),
        "sacks_7881": dict(sev="single_witness", title="def_sacks 1978-81 ~6.5% short (attribution ceiling)",
                           cols=["def_sacks", "def_sack_yards"], era="1978-1981",
                           detail="93.5% sacker attribution even with half-sacks; PFR box sacks start 1982 so no better witness. Document as era-limit.", ref="§9.7"),
        "jax": dict(sev="resolved_live", title="JAX 2001-02 team transposition — RESOLVED live (was: 146 rows on the opponent)",
                    cols=[*("nfl_team", "opponent_nfl_team", "nfl_franchise_number")], era="2001-2002",
                    detail="Audit §9.6: whole JAX roster credited to the opponent; fix built (build_team_attribution_from_box_v26) but marked NOT applied. RE-VERIFIED 2026-07-21: Fred Taylor's 2001-02 weeks are all filed under JAX now -> the transposition fix WAS applied. Audit's 'not applied' note is STALE.", ref="§9.6/§10.2"),
        "boston44": dict(sev="open_gap", title="1944 self-play residual — 23 impossible team==opponent rows remain",
                         cols=["nfl_team", "nfl_franchise_number"], era="1944",
                         detail="Audit §9.6: Boston Yanks merged into Washington's fid4; MODE(team_code) smeared 'BOS' onto WAS players. RE-VERIFIED 2026-07-21: the BOS/WAS fix largely landed (self_play league-wide 375 -> 23) BUT the ONLY self-play rows left in the whole table are 23 impossible team==opponent rows in 1944. A team cannot play itself — finish the 1944 franchise repair.", ref="§9.6"),
        "clones": dict(sev="resolved_live", title="11 duplicate humans (same person, two ids) — MERGE APPLIED live",
                       cols=[], era="1942-2015",
                       detail="Rocket/Raghib Ismail (+14 phantom targets), Cam Cleeland, Karim Abdul-Jabbar, etc. RE-VERIFIED 2026-07-21: the loser ids (00-0008060, 00-0003103, 00-0000003, ...) are all 0 rows now -> build_clone_identity_merge_v26 was applied (the 07-20/21 '231-row clone merge'). 136 single-game pairs remain QUEUED, not merged. Bio backfill onto survivors is a separate follow-up.", ref="§10c"),
        "targets_gt_att": dict(sev="queue", title="targets > attempts — 330 team-games (detector queue)",
                               cols=["targets", "attempts"], era="1978+",
                               detail="Two real classes: duplicate identity (the clone +14) and truncated QB stat lines (Brunell 2001 wk3). Per-row adjudication; promote the bound to a gate once cleared.", ref="§9.5-Q1"),
        "season_overcount": dict(sev="queue", title="season overcount n>g — 256 player-seasons (detector queue)",
                                 cols=[], era="all",
                                 detail="games > witness games-played: dup rows / POST leak / wrong witness g. Deletion discipline — never automatic.", ref="§9.5-Q2"),
        "fumbles_lost_pbp": dict(sev="single_witness", title="fumbles_lost 1978-98 PBP recovery-team derivation partial (~29%)",
                                 cols=[*("fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost", "sack_fumbles_lost")], era="1978-1998",
                                 detail="NFL.com logs close it. (§11 correction: the NC pre-1978 lost-fumble lane is DEAD — the '2,457 games' was a 'nan'-string scanner artifact, 159 real, almost all POST.)", ref="§7/§11"),
        "adot": dict(sev="broken_compute", title="adot / receiving_adot compute is broken",
                     cols=["adot", "receiving_adot"], era="2006+",
                     detail="Stored value wrong; correct = air_yards/targets. Also 2018+ only. Recompute.", ref="§5b"),
        "dr_av": dict(sev="unwitnessed", title="dr_av — PFR draft-page Approximate Value, not harvested",
                      cols=["dr_av"], era="career/bio",
                      detail="One of only two genuinely unwitnessed columns in the whole system.", ref="§2/§3"),
        "ras_score": dict(sev="unwitnessed", title="ras_score — external (ras.football), not harvested",
                          cols=["ras_score"], era="bio",
                          detail="Second of the two genuinely unwitnessed columns.", ref="§2/§3"),
    }
    return D

def page_of(src):
    PAGE = [("pbp_merged", "Play-by-play (nflverse/PFR merged)", "pbp"), ("pbp_weekly_rollup", "PBP weekly rollup", "pbp"),
            ("pbp_rollup_audit", "PBP rollup (audit twin)", "pbp"), ("nflcom_season", "NFL.com season pages", "season"),
            ("nflcom_career", "NFL.com career pages", "career"), ("nflcom_logs_targeted", "NFL.com game logs (targeted harvest)", "weekly"),
            ("nflcom_logs", "NFL.com game logs", "weekly"), ("nflcom_splits", "NFL.com splits", "season"),
            ("nflcom_situational", "NFL.com situational", "season"), ("nflcom_team", "NFL.com team pages", "team"),
            ("pfr_box", "PFR boxscore (per-game)", "weekly"), ("pfr_season", "PFR season pages", "season"),
            ("pfr_context", "PFR context tables (adv/awards/combine)", "season"), ("newspaper_promoted", "Newspaper DuckDB (PRIMARY witness)", "weekly"),
            ("ancient_bundle", "Ancient bundle (pre-1978 compiled)", "season"), ("ngs", "Next Gen Stats", "weekly"),
            ("team_games", "Team-game catalog (nfl_team_games_all)", "team"), ("schedule", "Master schedule", "team"),
            ("scoring_summary", "Scoring summary log", "weekly"), ("awards", "Awards / honors features", "season"),
            ("combine", "Combine", "bio")]
    for pre, label, grain in PAGE:
        if src == pre or src.startswith(pre): return label, grain
    return src, "other"

def consolidate(census, wk_floors):
    matrix = json.loads((DOCS / "witness-column-master-matrix.json").read_text())
    contracts = json.loads((DOCS / "witness-contracts-v2.json").read_text())["contracts"]
    er_by_col = {e["col"]: e for e in json.loads((WORK / "era_rescue_ledger.json").read_text())}

    registry = {}
    for src, body in contracts.items():
        atoms = body.get("atoms", {})
        if not atoms: continue
        label, grain = page_of(src)
        emins = [a["era_min"] for a in atoms.values() if a.get("era_min")]
        emaxs = [a["era_max"] for a in atoms.values() if a.get("era_max")]
        reg = registry.setdefault(label, {"page_grain": grain, "sources": [], "n_atoms": 0, "era_min": None, "era_max": None})
        reg["sources"].append(src); reg["n_atoms"] += len(atoms)
        if emins: reg["era_min"] = min(x for x in (reg["era_min"], min(emins)) if x is not None)
        if emaxs: reg["era_max"] = max(x for x in (reg["era_max"], max(emaxs)) if x is not None)

    DEFECTS = defects_register()
    _empty = sorted(c for c, f in wk_floors.items() if (f.get("nonzero") or 0) == 0)
    DEFECTS["empty_components"]["cols"] = _empty
    DEFECTS["empty_components"]["detail"] += f" | live empties ({len(_empty)}): {', '.join(_empty)}"
    col_to_defect = {}
    for did, d in DEFECTS.items():
        for c in d["cols"]: col_to_defect.setdefault(c, []).append(did)

    def three_q(rec):
        fam = rec["family"]
        if fam in ("identity", "flag_provenance"): return "na", "na", "na"
        v = rec.get("verdict")
        if v in ("direct", "text_derived", "derived_witnessed"): w = "yes"
        elif rec["col"] in ("dr_av", "ras_score"): w = "no"
        elif v is None: w = "unknown"
        else: w = "no"
        fl = rec.get("actual") or {}; nz = fl.get("nonzero"); er = rec.get("era_rescue")
        if nz is not None and nz == 0: cov = "no"
        elif fam in ("rank", "research", "rate_agg", "scoring") and v == "derived_witnessed": cov = "yes" if w == "yes" else "unknown"
        elif er and er.get("gap_years", 0) and er["gap_years"] > 0: cov = "partial"
        elif w == "yes": cov = "yes"
        else: cov = "unknown"
        c = rec["col"]
        if fam == "scoring": ver = "yes"
        elif c in RECON_INTERNAL: ver = "yes"
        elif c in MIRROR_PROPOSED: ver = "partial"
        elif rec.get("defects"): ver = "no"
        else: ver = "unknown"
        return w, cov, ver

    def status_of(rec):
        fam = rec["family"]
        if fam in ("identity", "flag_provenance"): return "meta"
        if rec.get("defects"):
            sevs = [DEFECTS[d]["sev"] for d in rec["defects"]]
            if any(s in ("open_hole", "open_gap", "broken_compute", "fix_built_unapplied", "incomplete_promoted", "single_witness") for s in sevs): return "unsealed_defect"
            if any(s == "queue" for s in sevs): return "queued"
            if any(s == "empty" for s in sevs): return "empty"
            if any(s == "verify" for s in sevs): return "verify"
            if any(s == "unwitnessed" for s in sevs): return "unwitnessed"
            return "watch"
        w, cov, ver = rec["witnessed"], rec["covered"], rec["verified"]
        if w == "no": return "unwitnessed"
        if w == "unknown": return "unmapped"
        if cov == "no": return "empty"
        if cov == "partial": return "era_gap"
        return "sealed"

    GRAINS = {"weekly": "weekly", "player_nfl_season": "season", "player_nfl_season_all": "season",
              "player_nfl_career": "career", "player_nfl_career_all": "career", "player_bio": "bio"}
    MKEY = {"weekly": "weekly", "player_nfl_season": "player_nfl_season", "player_nfl_season_all": "player_nfl_season_all",
            "player_nfl_career": "player_nfl_career", "player_nfl_career_all": "player_nfl_career_all"}
    out_tables = {}; drift = {}
    for tkey, grain in GRAINS.items():
        if tkey not in census: continue
        cur = census[tkey]["columns"]; mkey = MKEY.get(tkey); mcols = matrix["tables"].get(mkey, {}) if mkey else {}
        recs = {}
        for col, meta in cur.items():
            mv = mcols.get(col, {})
            if grain == "bio":
                if col in BIO_IDENTIFIER: fam, wit, verdict, era = "identity", [], None, None
                elif col in BIO_WITNESS: wit, era, verdict, fam = BIO_WITNESS[col]
                else: fam, wit, verdict, era = meta["family"], [], None, None
                rec = dict(col=col, family=fam, dtype=meta["dtype"], verdict=verdict, rule=verdict, witnesses=wit,
                           n_witnesses=len(wit), witness_game_era=era, witness_season_era=era, inputs=None,
                           unwitnessed_inputs=None, note=None, actual=None, era_rescue=None, defects=col_to_defect.get(col, []))
            elif grain in ("season", "career") and (col in BIO_WITNESS or col in CAREER_SCAFFOLD) and \
                    mv.get("verdict") not in ("direct", "text_derived", "derived_witnessed"):
                if col in CAREER_SCAFFOLD: verdict, wit, era, fam = CAREER_SCAFFOLD[col]
                else: wit, era, verdict, fam = BIO_WITNESS[col]
                rec = dict(col=col, family=fam, dtype=meta["dtype"], verdict=verdict, rule=verdict, witnesses=wit,
                           n_witnesses=len(wit), witness_game_era=era, witness_season_era=era, inputs=None, unwitnessed_inputs=None,
                           note="career/season scaffolding — not in the stat witness matrix", actual=None,
                           era_rescue=er_by_col.get(col), defects=col_to_defect.get(col, []))
            else:
                wit = mv.get("witnesses", [])
                rec = dict(col=col, family=meta["family"], dtype=meta["dtype"], verdict=mv.get("verdict"), rule=mv.get("rule"),
                           witnesses=wit, n_witnesses=len(wit), witness_game_era=mv.get("game_era"), witness_season_era=mv.get("season_era"),
                           inputs=mv.get("inputs"), unwitnessed_inputs=mv.get("unwitnessed_inputs"), note=mv.get("note"),
                           actual=wk_floors.get(col) if grain == "weekly" else None, era_rescue=er_by_col.get(col),
                           defects=col_to_defect.get(col, []))
            rec["witnessed"], rec["covered"], rec["verified"] = three_q(rec)
            rec["status"] = status_of(rec); recs[col] = rec
        dropped = sorted(set(mcols) - set(cur)) if mcols else []
        drift[tkey] = {"matrix_cols": len(mcols), "current_cols": len(cur), "dropped_since_matrix": dropped}
        out_tables[tkey] = {"grain": grain, "n_rows": census[tkey]["n_rows"], "columns": recs}

    from collections import Counter
    summary = {tk: {"n_cols": len(t["columns"]), "status": dict(Counter(r["status"] for r in t["columns"].values()))}
               for tk, t in out_tables.items()}
    master = {"generated": "2026-07-21",
              "note": "Consolidated witness x column inventory. Verdicts from witness-column-master-matrix.json (2026-07-17); "
                      "column set + weekly actual era-floors recomputed live against the current v26 release; era-gap from "
                      "era_rescue_ledger; defect register hand-encoded + live-verified from supertable-witness-master-audit-2026-07-16.md. "
                      "Three questions per column: witnessed / covered / verified.",
              "source_release": census["weekly"]["path"], "witness_registry": registry, "defect_register": DEFECTS,
              "drift": drift, "summary": summary, "tables": out_tables}
    (DOCS / "witness-coverage-master-inventory.json").write_text(json.dumps(master, indent=1, default=str))
    print("  wrote docs/witness-coverage-master-inventory.json")
    return master


def render(master):
    from scripts.sota_recon.witness_audit_v2._ledger_template import STYLE, BODY, SCRIPT
    GRAIN_ORDER = [("player_bio", "Player Bio"), ("weekly", "Weekly"), ("player_nfl_season", "Season"), ("player_nfl_career", "Career")]
    def compact(recs):
        out = []
        for c, r in recs.items():
            fl = r.get("actual") or {}; er = r.get("era_rescue") or {}; wit = r.get("witnesses") or []
            out.append([c, r["family"], r["status"], r.get("witnessed"), r.get("covered"), r.get("verified"),
                        len(wit), r.get("witness_game_era") or r.get("witness_season_era"), fl.get("actual_floor"),
                        fl.get("nonzero"), er.get("achievable_floor") if er.get("gap_years") else None,
                        r.get("defects") or [], wit[:8], r.get("inputs") or None, r.get("note")])
        return out
    grains = {}
    for key, label in GRAIN_ORDER:
        t = master["tables"].get(key)
        if t: grains[key] = {"label": label, "rows": compact(t["columns"]), "n_rows": t["n_rows"],
                             "mirror": key + "_all" if key.startswith("player_nfl") else None}
    payload = {"meta": {"generated": master["generated"], "release": Path(master["source_release"]).parent.parent.name,
                        "source_note": master["note"]},
               "summary": master["summary"],
               "drift": {k: {"dropped": v["dropped_since_matrix"], "matrix": v["matrix_cols"], "current": v["current_cols"]}
                         for k, v in master["drift"].items()},
               "registry": master["witness_registry"], "defects": master["defect_register"], "grains": grains}
    DATA = json.dumps(payload, separators=(",", ":"), default=str)
    inner = STYLE + BODY + SCRIPT.replace("__DATA__", DATA)
    standalone = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
                  '<meta name="viewport" content="width=device-width,initial-scale=1">'
                  '<title>Super-Table Witness &amp; Coverage Ledger</title><style>*{margin:0}</style></head><body>'
                  + inner + "</body></html>")
    (DOCS / "witness-coverage-master-inventory.html").write_text(standalone, encoding="utf-8")
    (WORK / "witness-ledger-body.html").write_text(inner, encoding="utf-8")
    print(f"  wrote docs/witness-coverage-master-inventory.html ({len(standalone) / 1e6:.2f}MB)")

def main():
    print("phase 1: live census + era-floors"); census, floors = census_phase()
    print("phase 2: consolidate"); master = consolidate(census, floors)
    print("phase 3: render"); render(master)
    print("done.")

if __name__ == "__main__":
    main()
