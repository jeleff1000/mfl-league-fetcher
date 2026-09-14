"""
research_ask_probe.py -- end-to-end stress battery for the research ask endpoint.

55 questions across the hard tail (advanced stats, rates/ratios, combos, pivots, windows,
scoring variants, splits, comparisons, critics). This is the battery behind the 2026-07-09
audit scorecard (docs/runbooks/research-mode-sota-gap-analysis-2026-07-09.md) and the seed
for the answer-level eval suite (Phase 5): each entry should eventually assert a golden top
row/value computed from Fly, not just non-error.

Usage: start the frontend dev server, then  python scripts/research_ask_probe.py
Rotates x-forwarded-for to stay under the per-IP ask rate limit (local dev only).
NOTE: several questions route through the LLM lane -- groq free tier is ~6k tokens/min,
so LLM-lane entries may 429 when run back-to-back; that is a rate limit, not a regression.
"""
import os
import json, urllib.request, time, sys

QUESTIONS = [
    # A. Advanced stats — new wave (on Fly, mostly unregistered in frontend)
    ("adv", "most total EPA in a season"),
    ("adv", "career passing WPA leaders"),
    ("adv", "most red zone targets in a season"),
    ("adv", "most explosive runs in a season"),
    ("adv", "highest rushing success rate in a season"),
    ("adv", "highest average separation in 2023"),
    ("adv", "which defenses allowed the lowest EPA per play in 2023"),
    ("adv", "most rush yards over expected in a season"),
    # A2. Advanced stats — registered in frontend registry
    ("adv-reg", "highest CPOE in a season"),
    ("adv-reg", "WOPR leaders in 2024"),
    ("adv-reg", "career passing EPA leaders"),
    ("adv-reg", "highest target share in a season"),
    ("adv-reg", "most passing air yards in a season"),
    ("adv-reg", "most drops in a season"),
    ("adv-reg", "highest aDOT in 2023"),
    ("adv-reg", "most rushing yards after contact in a season"),
    # B. Rates / per-game / ratios
    ("rate", "yards per carry leaders in 2023"),
    ("rate", "best TD to INT ratio in a season"),
    ("rate", "targets per game by Ravens RBs"),
    ("rate", "career receiving yards per target leaders"),
    ("rate", "fantasy points per touch leaders"),
    ("rate", "catch rate leaders with at least 100 targets"),
    ("rate", "EPA per play leaders in 2024"),
    ("rate", "fantasy points per target in a season"),
    ("rate", "touchdowns per carry career leaders"),
    # C. Combining stats
    ("combo", "most combined passing and rushing yards in a game"),
    ("combo", "most combined rushing and receiving touchdowns in a season"),
    ("combo", "most combined sacks and interceptions in a career"),
    ("combo", "players with 1000 rushing yards and 50 receptions in the same season"),
    ("combo", "most touches in a season"),
    # D. Pivots / group-bys
    ("pivot", "average passing yards by draft round"),
    ("pivot", "which college produced the most career fantasy points"),
    ("pivot", "fantasy points by NFL team in 2024"),
    ("pivot", "most 100 yard rushers by team all time"),
    ("pivot", "total rushing yards by decade"),
    ("pivot", "which draft class had the most fantasy points"),
    # E. Cross-grain windows / counts
    ("window", "most 30 point fantasy games in a career"),
    ("window", "most consecutive games with a touchdown"),
    ("window", "best 5 game rushing yards stretch"),
    ("window", "most 300 yard passing games in a single season"),
    # F. Scoring variants
    ("variant", "most PPR points in a game by a TE"),
    ("variant", "best 6 point passing TD season for a QB"),
    ("variant", "half PPR points per game leaders since 2015"),
    # G. Splits / context
    ("split", "Derrick Henry home vs away rushing yards"),
    ("split", "best fantasy games in December"),
    ("split", "most rushing yards on Thanksgiving"),
    ("split", "Josh Allen playoff fantasy points"),
    ("split", "most receiving yards against the Chiefs in a game"),
    # H. Comparisons / rank context
    ("compare", "compare Justin Jefferson and Ja'Marr Chase first 3 seasons"),
    ("compare", "who has more career rushing yards Barry Sanders or Emmitt Smith"),
    ("compare", "where does Puka Nacua rank in receiving yards per game all time"),
    # I. Superlatives that need critics
    ("critic", "only quarterbacks with 40 TD and under 5 INT in a season"),
    ("critic", "first player to score 25 PPR points in 10 straight games"),
    # J. Two-metric relationship / correlation-ish
    ("relation", "do players with more targets score more fantasy points"),
    ("relation", "highest fantasy points among players with under 50 targets in a season"),
]

URL = "http://localhost:3001/api/research/ask"
results = []
for i, (cat, q) in enumerate(QUESTIONS):
    ip = f"10.9.{i // 20}.{i % 250 + 1}"
    body = json.dumps({"query": q}).encode()
    req = urllib.request.Request(URL, data=body, headers={
        "Content-Type": "application/json",
        "x-forwarded-for": ip,
    })
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        results.append({"cat": cat, "q": q, "status": "EXCEPTION", "detail": str(e)[:160], "ms": int((time.time()-t0)*1000)})
        continue
    ms = int((time.time()-t0)*1000)
    if data.get("error"):
        results.append({"cat": cat, "q": q, "status": "ERROR", "detail": data["error"][:200], "ms": ms})
    else:
        rows = data.get("rows") or []
        cols = data.get("columns") or []
        chips = ((data.get("receipt") or {}).get("chips")) or []
        chipstr = "; ".join(f"{c.get('label')}={c.get('value')}" for c in chips)
        first = rows[0] if rows else []
        results.append({
            "cat": cat, "q": q, "status": f"OK({len(rows)})", "ms": ms,
            "summary": (data.get("summary") or "")[:110],
            "highlight": (data.get("highlight") or "")[:110],
            "chips": chipstr[:150],
            "cols": ",".join(str(c) for c in cols[:8]),
            "row1": str(first)[:130],
        })
    sys.stdout.write(f"[{i+1}/{len(QUESTIONS)}] {cat}: {q[:60]} -> {results[-1]['status']} ({ms}ms)\n")
    sys.stdout.flush()
    time.sleep(0.3)

with open(os.path.join(os.path.dirname(__file__), "research_ask_probe_results.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, indent=1)
print("saved research_ask_probe_results.json")
