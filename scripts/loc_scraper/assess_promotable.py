"""
assess_promotable.py — Definitive inventory of all LOC image extracts.

Classifies every game score record against both v26 (super table) and PFR
(nfl_team_games_all.parquet) to produce a clear verdict on each atom.

Classifications:
  CONFIRM_EXACT      LOC score = v26 for the exact week. Already correct in ST.
  CONFIRM_ALT_WEEK   LOC score = PFR but for a different week (manifest week error).
  FILL_PFR_VERIFIED  v26 DEF row is NULL, LOC score matches PFR. Ready to promote.
  FILL_UNVERIFIED    v26 NULL AND PFR NULL. Genuinely new, cautious.
  WRONG_EXTRACTION   LOC score doesn't match PFR. Our AI/OCR got it wrong.
  UNMAPPABLE         Team not in v26 / not an NFL team.
  PLAYER_TDS         Separate section — player-level atoms needing bio matching.

Writes NOTHING. Safe to run at any time.

Usage:
    python -m scripts.loc_scraper.assess_promotable
    python -m scripts.loc_scraper.assess_promotable --verbose
"""

from __future__ import annotations
import argparse
import glob
import json
from collections import defaultdict
from pathlib import Path

import duckdb

# ── Paths ─────────────────────────────────────────────────────────────────────

JSONL   = Path(r"D:\league-history-data\nfl\curated\loc_scraper\image_extracts.jsonl")
V26_GLOB = r"D:\league-history-data\nfl\releases\*_v26\tables\nfl_player_stats_all.parquet"
PFR_TG  = r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet"

# ── Team name normalization ───────────────────────────────────────────────────

_NORM_RAW: dict[str, str | None] = {
    "rock island independents": "RII",
    "rock island":              "RII",
    "akron pros":               "AKR",
    "akron indians":            "AKR",
    "akron":                    "AKR",
    "dayton triangles":         "DAY",
    "dayton":                   "DAY",
    "columbus panhandles":      "COL",
    "columbus tigers":          "COL",
    "panhandles":               "COL",
    "columbus":                 "COL",
    "canton bulldogs":          "CAN",
    "canton":                   "CAN",
    "hammond pros":             "HAM",
    "hammond":                  "HAM",
    "rochester jeffersons":     "RCH",
    "rochester":                "RCH",
    "milwaukee badgers":        "MIL",
    "milwaukee":                "MIL",
    "duluth eskimos":           "DUL",
    "duluth kelleys":           "DUL",
    "duluth":                   "DUL",
    "frankford yellow jackets": "FRN",
    "yellow jackets":           "FRN",
    "frankford":                "FRN",
    "pottsville maroons":       "POT",
    "pottsville":               "POT",
    "maroons":                  "POT",
    "providence steam roller":  "PRV",
    "steam roller":             "PRV",
    "providence":               "PRV",
    "staten island stapletons": "SIS",
    "stapletons":               "SIS",
    "staten island":            "SIS",
    "portsmouth spartans":      "PRT",
    "spartans":                 "PRT",
    "portsmouth":               "PRT",
    "minneapolis marines":      "MIN",
    "minneapolis red jackets":  "MIN",
    "red jackets":              "MIN",
    "minneapolis":              "MIN",
    "racine legion":            "RAC",
    "racine tornadoes":         "RAC",
    "racine":                   "RAC",
    "oorang indians":           "OOR",
    "oorang":                   "OOR",
    "cleveland bulldogs":       "CLE",
    "cleveland indians":        "CLE",
    "cleveland tigers":         "CLE",
    "cleveland rams":           "RAM",
    "los angeles rams":         "RAM",
    "rams":                     "RAM",
    "thorpe's bull-dogs (canton)": "CAN",
    "thorpe's bulldogs":        "CAN",
    "detroit wolverines":       "DET",
    "detroit panthers":         "DET",
    "detroit pros":             "DET",
    "detroit lions":            "DET",
    "lions":                    "DET",
    "detroit":                  "DET",
    "buffalo all-americans":    "BUF",
    "buffalo bisons":           "BUF",
    "buffalo rangers":          "BUF",
    "buffalo":                  "BUF",
    "green bay packers":        "GREEN_BAY",
    "packers":                  "GREEN_BAY",
    "green bay":                "GREEN_BAY",
    "new york giants":          "NYG",
    "giants":                   "NYG",
    "new york yankees":         "NYY",
    "new york yankees (afl/nfl)": "NYY",
    "new york yanks":           "NYY",
    "chicago bears":            "CHI",
    "bears":                    "CHI",
    "chicago staleys":          "CHI",
    "staleys":                  "CHI",
    "chicago cardinals":        "CRD",
    "cardinals":                "CRD",
    "washington redskins":      "WAS",
    "boston redskins":          "WAS",
    "redskins":                 "WAS",
    "washington":               "WAS",
    "brooklyn dodgers":         "BKN",
    "brooklyn tigers":          "BKN",
    "brooklyn":                 "BKN",
    "philadelphia eagles":      "PHI",
    "eagles":                   "PHI",
    "philadelphia":             "PHI",
    "pittsburgh steelers":      "PIT",
    "pittsburgh pirates":       "PIT",
    "steelers":                 "PIT",
    "pittsburgh":               "PIT",
    "phil-pitt steagles":       "PHI",
    "steagles":                 "PHI",
    "card-pitt":                "CRD",
    "carpits":                  "CRD",
    "boston yanks":             "BOS",
    "yanks":                    "BOS",
    "boston":                   "BOS",
    "new england patriots":     "NE",
    # NOT IN v26
    "ironton tanks":            None,
    "ironton":                  None,
    "unknown":                  None,
    "":                         None,
}

TEAM_NORM: dict[str, str | None] = {k.lower().strip(): v for k, v in _NORM_RAW.items()}

_NON_NFL_KEYWORDS = frozenset([
    "army", "navy", "notre dame", "college", "warsaw", "high school",
    "amateur", "semi-pro", "all-stars",
])

# PFR uses different team codes for some franchises — map to v26 codes
_PFR_TO_V26: dict[str, str] = {
    "GNB": "GNB",  # Green Bay (PFR often uses GNB)
    "GB":  "GNB",  # sometimes GB in PFR -> try GNB first
    "CHI": "CHI",
    "NYG": "NYG",
    "DET": "DET",
    "WAS": "WAS",
    "BOS": "BOS",
    "RAM": "RAM",
    "CRD": "CRD",
    "PHI": "PHI",
    "PIT": "PIT",
    "BKN": "BKN",
    "CLE": "CLE",
    "DAY": "DAY",
    "AKR": "AKR",
    "PRT": "PRT",
    "PRV": "PRV",
    "FRN": "FRN",
    "SIS": "SIS",
    "POT": "POT",
    "NYY": "NYY",
    "RII": "RII",
    "RAC": "RAC",
    "DUL": "DUL",
    "MIL": "MIL",
    "BUF": "BUF",
    "CAN": "CAN",
    "COL": "COL",
    "HAM": "HAM",
    "MIN": "MIN",
    "RCH": "RCH",
    "OOR": "OOR",
}


def normalize_team(name: str, year: int) -> str | None:
    if not name:
        return None
    key = name.lower().strip()
    for kw in _NON_NFL_KEYWORDS:
        if kw in key:
            return None
    code = TEAM_NORM.get(key)
    if code is None:
        return None
    if code == "GREEN_BAY":
        # GNB has actual data 1921-1932+; GB rows exist but are empty pre-1933
        return "GNB" if year <= 1932 else "GB"
    return code


def _gb_alts(code: str) -> list[str]:
    """Return alternate Green Bay codes to try."""
    if code == "GNB":
        return ["GB"]
    if code == "GB":
        return ["GNB"]
    return []


def _latest_v26() -> str:
    files = sorted(glob.glob(V26_GLOB), key=lambda p: Path(p).stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No v26 parquet found at {V26_GLOB}")
    return files[0]


# ── Load data ─────────────────────────────────────────────────────────────────

def load_game_scores() -> list[dict]:
    records = []
    for line in open(JSONL, encoding="utf-8"):
        e = json.loads(line)
        year = int(e.get("year") or 0)
        week = int(e.get("week") or 0)
        source_key = e["source_key"]
        for g in e.get("games", []):
            sa = g.get("score_a")
            sb = g.get("score_b")
            if sa is None or sb is None:
                continue
            ta_raw = str(g.get("team_a") or "")
            tb_raw = str(g.get("team_b") or "")
            ta = normalize_team(ta_raw, year)
            tb = normalize_team(tb_raw, year)
            records.append({
                "year": year, "week": week,
                "team_a_raw": ta_raw, "team_b_raw": tb_raw,
                "team_a": ta, "team_b": tb,
                "score_a": int(sa), "score_b": int(sb),
                "source": g.get("source", ""),
                "source_key": source_key,
            })
    return records


def deduplicate_scores(records: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        ta, tb = r["team_a"], r["team_b"]
        if ta is None and tb is None:
            r["status"] = "UNMAPPABLE"
            groups[("UNMAPPABLE", r["year"], r["week"], r["team_a_raw"][:20])].append(r)
            continue
        pair = tuple(sorted([ta or "?", tb or "?"]))
        key = (r["year"], r["week"]) + pair
        groups[key].append(r)

    deduped = []
    for key, group in groups.items():
        if key[0] == "UNMAPPABLE":
            deduped.extend(group)
            continue
        scores = set()
        for r in group:
            ta, tb = r["team_a"] or "?", r["team_b"] or "?"
            if ta <= tb:
                scores.add((r["score_a"], r["score_b"]))
            else:
                scores.add((r["score_b"], r["score_a"]))
        rep = group[0].copy()
        rep["source_count"] = len(group)
        rep["source_keys"] = [r["source_key"] for r in group]
        rep["score_conflict"] = len(scores) > 1
        if rep["score_conflict"]:
            rep["score_variants"] = list(scores)
        deduped.append(rep)
    return deduped


def load_v26_def_scores(conn: duckdb.DuckDBPyConnection, v26: str) -> dict:
    """Returns {(year, week, nfl_team): {team_pts, pa, opp}}"""
    rows = conn.execute(f"""
        SELECT DISTINCT
            CAST(year AS INTEGER),
            CAST(week AS INTEGER),
            nfl_team,
            opponent_nfl_team,
            pts_def_team_pts,
            points_allowed
        FROM read_parquet('{v26}')
        WHERE position = 'DEF'
          AND year BETWEEN 1920 AND 1945
    """).fetchall()
    result = {}
    for yr, wk, team, opp, tpts, pa in rows:
        key = (yr, wk, team)
        if key not in result:
            result[key] = {"team_pts": tpts, "pa": pa, "opp": opp}
    return result


def load_pfr_scores(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Returns {(year, week, team_code): {team_pts, opp_pts, opp_code, game_date}}
    from PFR nfl_team_games_all.parquet.
    """
    rows = conn.execute(f"""
        SELECT
            CAST(year AS INTEGER),
            CAST(week AS INTEGER),
            team_code,
            opponent_code,
            team_points,
            opponent_points,
            game_date
        FROM read_parquet('{PFR_TG}')
        WHERE year BETWEEN 1920 AND 1945
    """).fetchall()
    result = {}
    for yr, wk, tc, oc, tp, op, gd in rows:
        v26_tc = _PFR_TO_V26.get(tc, tc)
        key = (yr, wk, v26_tc)
        if key not in result:
            result[key] = {"team_pts": tp, "opp_pts": op, "opp_code": oc, "game_date": gd}
    return result


# ── Score matching helpers ────────────────────────────────────────────────────

def _match(a: int, b: float | None) -> bool:
    return b is not None and abs(a - b) < 0.5


def _find_pfr_week(pfr: dict, year: int, team: str, score: int, opp_score: int) -> int | None:
    """Search all PFR weeks for this year/team to find one matching the score."""
    alts = [team] + _gb_alts(team)
    for wk in range(1, 22):
        for tc in alts:
            row = pfr.get((year, wk, tc))
            if row and _match(score, row["team_pts"]) and _match(opp_score, row["opp_pts"]):
                return wk
            # Also try reversed (newspaper may have swapped teams)
            if row and _match(opp_score, row["team_pts"]) and _match(score, row["opp_pts"]):
                return wk
    return None


# ── Main assessment ───────────────────────────────────────────────────────────

def assess(records: list[dict], v26: dict, pfr: dict, verbose: bool = False) -> None:

    buckets: dict[str, list] = {
        "CONFIRM_EXACT":     [],
        "CONFIRM_ALT_WEEK":  [],
        "FILL_PFR_VERIFIED": [],
        "FILL_UNVERIFIED":   [],
        "WRONG_EXTRACTION":  [],
        "UNMAPPABLE":        [],
    }

    for r in records:
        year, week = r["year"], r["week"]
        ta, tb = r["team_a"], r["team_b"]
        sa, sb = r["score_a"], r["score_b"]

        # --- Unmappable ---
        if r.get("status") == "UNMAPPABLE" or (ta is None and tb is None):
            r["verdict"] = "UNMAPPABLE"
            r["note"] = f"Team not in v26: '{r['team_a_raw']}' vs '{r['team_b_raw']}'"
            buckets["UNMAPPABLE"].append(r)
            continue

        # --- Resolve v26 rows (try alternate GB codes) ---
        v26_a = v26.get((year, week, ta)) if ta else None
        if v26_a is None and ta in ("GB", "GNB"):
            for alt in _gb_alts(ta):
                v26_a = v26.get((year, week, alt))
                if v26_a: break

        v26_b = v26.get((year, week, tb)) if tb else None
        if v26_b is None and tb in ("GB", "GNB"):
            for alt in _gb_alts(tb):
                v26_b = v26.get((year, week, alt))
                if v26_b: break

        # --- Resolve PFR rows ---
        pfr_a = pfr.get((year, week, ta)) if ta else None
        if pfr_a is None and ta in ("GB", "GNB"):
            for alt in _gb_alts(ta):
                pfr_a = pfr.get((year, week, alt))
                if pfr_a: break

        pfr_b = pfr.get((year, week, tb)) if tb else None
        if pfr_b is None and tb in ("GB", "GNB"):
            for alt in _gb_alts(tb):
                pfr_b = pfr.get((year, week, alt))
                if pfr_b: break

        # --- CONFIRM_EXACT: LOC matches v26 for this exact week ---
        a_exact = v26_a and _match(sa, v26_a["team_pts"]) and _match(sb, v26_a["pa"])
        b_exact = v26_b and _match(sb, v26_b["team_pts"]) and _match(sa, v26_b["pa"])

        if a_exact or b_exact:
            r["verdict"] = "CONFIRM_EXACT"
            r["note"] = f"v26 already correct: {ta} {sa}-{sb} {tb}"
            buckets["CONFIRM_EXACT"].append(r)
            continue

        # --- Check if v26 has NULL scores (potential fill) ---
        a_null = v26_a and v26_a["team_pts"] is None and v26_a["pa"] is None
        b_null = v26_b and v26_b["team_pts"] is None and v26_b["pa"] is None
        a_missing = v26_a is None  # no DEF row at all for this team/week
        b_missing = v26_b is None

        # --- Check PFR agreement ---
        pfr_a_match = pfr_a and _match(sa, pfr_a["team_pts"]) and _match(sb, pfr_a["opp_pts"])
        pfr_b_match = pfr_b and _match(sb, pfr_b["team_pts"]) and _match(sa, pfr_b["opp_pts"])
        pfr_agrees = pfr_a_match or pfr_b_match

        # --- FILL: v26 NULL and PFR agrees ---
        if (a_null or b_null) and pfr_agrees:
            r["verdict"] = "FILL_PFR_VERIFIED"
            sides = []
            if a_null: sides.append(f"{ta} (team_pts={sa}, pa={sb})")
            if b_null: sides.append(f"{tb} (team_pts={sb}, pa={sa})")
            r["note"] = "v26 DEF row NULL; PFR confirms score. Fill: " + ", ".join(sides)
            buckets["FILL_PFR_VERIFIED"].append(r)
            continue

        # --- CONFIRM_ALT_WEEK or FILL: score matches PFR but for a different week ---
        if not pfr_agrees and ta:
            alt_wk = _find_pfr_week(pfr, year, ta, sa, sb)
            if alt_wk and alt_wk != week:
                # Check if v26 has NULL scores for the ACTUAL week
                v26_a_actual = v26.get((year, alt_wk, ta))
                if v26_a_actual is None:
                    for alt in _gb_alts(ta):
                        v26_a_actual = v26.get((year, alt_wk, alt))
                        if v26_a_actual:
                            break
                v26_b_actual = v26.get((year, alt_wk, tb)) if tb else None
                if v26_b_actual is None and tb in ("GB", "GNB"):
                    for alt in _gb_alts(tb):
                        v26_b_actual = v26.get((year, alt_wk, alt))
                        if v26_b_actual:
                            break

                a_act_null = v26_a_actual and v26_a_actual["team_pts"] is None
                b_act_null = v26_b_actual and v26_b_actual["team_pts"] is None

                if a_act_null or b_act_null:
                    # Correct week exists in v26 but scores are NULL — this is a fill
                    r["verdict"] = "FILL_PFR_VERIFIED"
                    r["actual_week"] = alt_wk
                    sides = []
                    if a_act_null:
                        sides.append(f"{ta} w{alt_wk} (team_pts={sa}, pa={sb})")
                    if b_act_null:
                        sides.append(f"{tb} w{alt_wk} (team_pts={sb}, pa={sa})")
                    r["note"] = (f"Manifest said w{week}; actual w{alt_wk} per PFR. "
                                 f"v26 NULL at correct week. Fill: " + ", ".join(sides))
                else:
                    r["verdict"] = "CONFIRM_ALT_WEEK"
                    r["actual_week"] = alt_wk
                    r["note"] = (f"Score correct but manifest week wrong "
                                 f"(said w{week}, actual w{alt_wk}). v26 already correct.")
                buckets[r["verdict"]].append(r)
                continue

        # --- WRONG_EXTRACTION: PFR has different score for this team/week ---
        if pfr_a and not pfr_a_match:
            r["verdict"] = "WRONG_EXTRACTION"
            r["note"] = (f"LOC says {ta} {sa}-{sb} but PFR says "
                         f"{ta} {pfr_a['team_pts']}-{pfr_a['opp_pts']} vs {pfr_a['opp_code']}")
            buckets["WRONG_EXTRACTION"].append(r)
            continue
        if pfr_b and not pfr_b_match:
            r["verdict"] = "WRONG_EXTRACTION"
            r["note"] = (f"LOC says {tb} {sb}-{sa} but PFR says "
                         f"{tb} {pfr_b['team_pts']}-{pfr_b['opp_pts']} vs {pfr_b['opp_code']}")
            buckets["WRONG_EXTRACTION"].append(r)
            continue

        # --- FILL_UNVERIFIED: v26 NULL, PFR also NULL ---
        if a_null or b_null:
            r["verdict"] = "FILL_UNVERIFIED"
            r["note"] = "v26 DEF row NULL; PFR also has no score for this week"
            buckets["FILL_UNVERIFIED"].append(r)
            continue

        # --- Neither v26 nor PFR has this team/week at all ---
        r["verdict"] = "WRONG_EXTRACTION"
        r["note"] = f"No DEF row in v26 and no PFR record for {ta}/{tb} in {year}/w{week}"
        buckets["WRONG_EXTRACTION"].append(r)

    # ── Print report ─────────────────────────────────────────────────────────
    total = sum(len(v) for v in buckets.values())
    print("=" * 72)
    print("LOC EXTRACT INVENTORY  —  definitive verdict per atom")
    print("=" * 72)
    print(f"  v26:  {Path(_latest_v26()).name}")
    print(f"  JSONL: {JSONL}  ({sum(1 for _ in open(JSONL, encoding='utf-8'))} tiles)")
    print(f"  Game score records: {total}")
    print()

    labels = {
        "CONFIRM_EXACT":     "CONFIRM_EXACT    — LOC score already correct in v26",
        "CONFIRM_ALT_WEEK":  "CONFIRM_ALT_WEEK — Correct score, manifest week was wrong",
        "FILL_PFR_VERIFIED": "FILL_PFR_VERIFIED — v26 NULL; PFR confirms. Promotable.",
        "FILL_UNVERIFIED":   "FILL_UNVERIFIED  — v26 NULL; PFR no data. New if correct.",
        "WRONG_EXTRACTION":  "WRONG_EXTRACTION — Our AI/OCR got it wrong vs PFR",
        "UNMAPPABLE":        "UNMAPPABLE       — Team not in v26 (non-NFL or unknown)",
    }

    for key, label in labels.items():
        items = sorted(buckets[key], key=lambda x: (x["year"], x["week"]))
        print(f"{label}  ({len(items)})")
        for r in items:
            ta, tb = r.get("team_a") or "?", r.get("team_b") or "?"
            sa, sb = r["score_a"], r["score_b"]
            wk = r["week"]
            actual = f" -> actual w{r['actual_week']}" if r.get("actual_week") else ""
            srcs = r.get("source_count", 1)
            src_tag = f"  [{srcs} tiles]" if srcs > 1 else ""
            print(f"  {r['year']}/w{wk:>2}  {ta:<5} {sa:>3}-{sb:<3} {tb:<5}{actual}{src_tag}")
            if verbose:
                print(f"           {r.get('note', '')}")
        print()

    print("=" * 72)
    print("SUMMARY")
    for key in labels:
        n = len(buckets[key])
        print(f"  {key:<22} {n}")
    print()

    # ── Player stats ──────────────────────────────────────────────────────────
    all_tds, all_yards = [], []
    for line in open(JSONL, encoding="utf-8"):
        e = json.loads(line)
        year = int(e.get("year") or 0)
        week = int(e.get("week") or 0)
        for td in e.get("player_tds", []):
            name = td.get("name", "").strip()
            if name:
                all_tds.append({
                    "year": year, "week": week,
                    "name": name, "team": td.get("team", ""),
                    "source_key": e["source_key"],
                })
        for yd in e.get("player_yards", []):
            name = yd.get("name", "").strip()
            if name:
                all_yards.append({
                    "year": year, "week": week,
                    "name": name, "yards": yd.get("yards"),
                    "type": yd.get("type", ""), "team": yd.get("team", ""),
                    "source_key": e["source_key"],
                })

    print(f"PLAYER TDs  ({len(all_tds)} atoms — require player_bio matching before promotion)")
    if all_tds:
        for td in sorted(all_tds, key=lambda x: (x["year"], x["week"], x["name"])):
            print(f"  {td['year']}/w{td['week']:>2}  {td['name']:<28}  team={td['team']}")
    else:
        print("  (none)")
    print()

    print(f"PLAYER YARDS  ({len(all_yards)} atoms)")
    if all_yards:
        for yd in sorted(all_yards, key=lambda x: (x["year"], x["week"], x["name"])):
            print(f"  {yd['year']}/w{yd['week']:>2}  {yd['name']:<28}  {yd['yards']} {yd['type']} yards  team={yd['team']}")
    else:
        print("  (none)")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    v26_path = _latest_v26()
    print(f"Loading v26:  {Path(v26_path).name}")
    conn = duckdb.connect()
    v26_scores = load_v26_def_scores(conn, v26_path)
    print(f"  {len(v26_scores)} DEF rows (1920-1945)")

    print(f"Loading PFR:  {Path(PFR_TG).name}")
    pfr_scores = load_pfr_scores(conn)
    print(f"  {len(pfr_scores)} team-game rows (1920-1945)")

    print(f"Loading JSONL: {JSONL}")
    records = load_game_scores()
    print(f"  {len(records)} game score records")
    records = deduplicate_scores(records)
    print(f"  {len(records)} after deduplication")
    print()

    assess(records, v26_scores, pfr_scores, verbose=args.verbose)


if __name__ == "__main__":
    main()
