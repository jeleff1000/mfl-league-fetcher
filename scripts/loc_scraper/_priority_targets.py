"""
Generate a ranked list of CLOSED_NEGATIVE games most worth manually searching
on Newspapers.com. Prioritizes:
  1. T1 games (1932-1945) where box scores were actually published
  2. Games involving major-city franchises with big papers
  3. Championship / late-season games (week 10+)

Output: priority_targets.csv for manual Newspapers.com searching.
"""
import sys
import csv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.config import FRANCHISE_NAMES, get_search_terms

# Franchises with major papers on Newspapers.com (not in LOC)
MAJOR_PAPER_FRANCHISES = {
    2,   # Chicago Bears — Chicago Tribune
    5,   # NY Giants — NY Times / Herald Tribune
    6,   # Green Bay Packers — Green Bay Press-Gazette
    4,   # Boston/Washington — Boston Globe / Washington Post
    9,   # Philadelphia Eagles — Philadelphia Inquirer
    10,  # Pittsburgh Steelers — Pittsburgh Post-Gazette
    16,  # Detroit Lions — Detroit Free Press
    8,   # Chicago Cardinals — Chicago Tribune
    14,  # Cleveland/LA Rams — Cleveland Plain Dealer
    18,  # Cleveland Browns — Cleveland Plain Dealer
    15,  # San Francisco 49ers
    102, # Rock Island Independents — Rock Island Argus
    105, # Canton Bulldogs — Canton Repository
    6,   # Green Bay
    113, # Providence Steam Roller
}

def franchise_label(fnum: int, year: int) -> str:
    terms = get_search_terms(fnum, year)
    return terms[0] if terms else f"Franchise#{fnum}"

def search_hint(fnum_a: int, fnum_b: int, year: int) -> str:
    ta = get_search_terms(fnum_a, year)
    tb = get_search_terms(fnum_b, year)
    name_a = ta[0] if ta else f"Franchise#{fnum_a}"
    name_b = tb[0] if tb else f"Franchise#{fnum_b}"
    return f'"{name_a}" "{name_b}" football box score'

def main():
    conn = open_db()

    rows = conn.execute("""
        SELECT game_key, year, week, season_type,
               franchise_a, franchise_b, team_a, team_b,
               tier, missing_csv,
               has_pass_a, has_rush_a, has_recv_a,
               has_pass_b, has_rush_b, has_recv_b
        FROM game_manifest
        WHERE state='CLOSED_NEGATIVE'
        ORDER BY tier, year, week
    """).fetchall()

    out = []
    for r in rows:
        (gk, year, week, stype, fa, fb, ta, tb,
         tier, missing, hpa, hra, hrca, hpb, hrb, hrcb) = r

        # Score: higher = more worth searching manually
        score = 0

        # T1 worth much more (box scores existed)
        if tier == "T1":
            score += 40
        elif tier == "T0" and year >= 1928:
            score += 15  # late APFA/early NFL, slightly better coverage
        elif tier == "T0":
            score += 5

        # Major city franchise involved
        if fa in MAJOR_PAPER_FRANCHISES: score += 20
        if fb in MAJOR_PAPER_FRANCHISES: score += 20

        # Late season / playoffs more likely to get coverage
        if week >= 12: score += 15
        elif week >= 9: score += 8

        # Missing more stats = more valuable to fill
        missing_count = len(missing.split(",")) if missing else 0
        score += missing_count * 5

        name_a = franchise_label(fa, year)
        name_b = franchise_label(fb, year)
        hint = search_hint(fa, fb, year)

        out.append({
            "priority_score": score,
            "tier": tier,
            "year": int(year),
            "week": int(week),
            "game_key": gk,
            "team_a": name_a,
            "team_b": name_b,
            "missing": missing or "",
            "newspapers_com_search": hint,
            "date_estimate": f"{int(year)}-Sep thru Dec",
        })

    # Sort by priority descending
    out.sort(key=lambda x: -x["priority_score"])

    outpath = Path(r"D:\league-history-data\nfl\curated\loc_scraper\priority_targets.csv")
    outpath.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["priority_score","tier","year","week","team_a","team_b",
                  "missing","newspapers_com_search","date_estimate","game_key"]
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out)

    print(f"Written {len(out)} targets to: {outpath}")
    print(f"\nTop 20 by priority:")
    print(f"{'Score':>5}  {'Tier':4}  {'Year':4}  {'Wk':2}  {'Matchup'}")
    print("-" * 70)
    for r in out[:20]:
        print(f"{r['priority_score']:>5}  {r['tier']:4}  {r['year']:4}  "
              f"{r['week']:2}  {r['team_a']} vs {r['team_b']}")

    # Summary by tier
    from collections import Counter
    tiers = Counter(r["tier"] for r in out)
    print(f"\nTotal CLOSED_NEGATIVE by tier: {dict(tiers)}")
    t1 = [r for r in out if r["tier"]=="T1"]
    print(f"T1 with score>=60 (best Newspapers.com targets): {sum(1 for r in t1 if r['priority_score']>=60)}")

if __name__ == "__main__":
    main()
