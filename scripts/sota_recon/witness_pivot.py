"""Witness-coverage pivot: every plane column x every witness root, count + reach.

Joe's visualization contract (2026-08-01): one tab per local plane -- weekly, season
(REG), season incl. playoffs, career (REG), career incl. playoffs, player bio -- every
physical column a row grouped by category, one column per witness lineage root, each
cell = (how many specs from that root witness the column, earliest year that root's
witnessing sources reach).

COUNTING RULE (v2 -- the v1 defect made the ledger 'obviously incorrect'): coverage
comes from the WITNESS DOCUMENTATION (WITNESS_MAP), and a spec witnesses its canonical
column on EVERY plane that column lives on.  v1 partitioned each spec to the single
plane of its validation grain, which threw away 20 of passing_yards' 35 specs on the
weekly tab and nearly all of them on the career tabs -- validation grain is where a
value is CHECKED, not the only place it is witnessed.  Stratum still matters: REG tabs
count non-POST specs, the incl.-playoffs tabs count POST-stratum specs (that is their
whole question), weekly counts everything.

The full-stratum audit is an OVERLAY, not a filter: cells where at least one spec
compared >0 rows are 'measured'; documented-but-unmeasured cells stay visible (hollow)
-- including the newspaper root, excluded from the audit by the 2026-08-01 scope
decision but present in the documentation.

Reach is the DECLARED registry span of the witnessing sources clipped to 1920, not a
measured first-overlap year -- the measured version is the Phase C drill-down.

Output: witness_pivot.json beside the other receipts.  Regenerate any time with::

    python -m scripts.sota_recon.witness_pivot
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import duckdb

from . import sources as S
from .witness_map import WITNESS_MAP

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
MERGED = OUT / "MAPPING_AUDIT_FULL.json"
PIVOT = OUT / "witness_pivot.json"

ROOT_LABELS = {
    "pfr": "PFR", "nflcom": "NFL.com", "pfa_loc": "PFA", "ngs": "NGS",
    "statscrew": "StatsCrew", "nflverse_pbp": "PBP/nflverse",
    "newspaper": "Newspaper", "internal": "Legacy",
}
ROOT_ORDER = ["pfr", "nflcom", "pfa_loc", "ngs", "statscrew", "nflverse_pbp",
              "newspaper", "internal"]

CATEGORY_ORDER = [
    "passing", "rushing", "receiving", "kicking", "punting", "returns",
    "idp_defense", "team_dst", "usage_ol", "awards", "advanced_epa",
    "context", "bio", "identifier", "other",
    "scoring_derived", "rank_derived", "audit_stamp",
]


def category(c: str) -> str:
    cl = c.lower()
    if re.search(r"_(repaired|populated|recomputed|merged)_at_\d+", cl) or cl in (
            "recon_correction_log", "last_updated"):
        return "audit_stamp"
    if re.match(r"^rank_", cl) or cl.endswith("_rank"):
        return "rank_derived"
    if any(k in cl for k in ("rolling_", "consistency_", "avg_pts", "next_year",
                             "alltime", "lamar", "vs_expect", "pts_", "ppg",
                             "fantasy_pts", "bonus_")):
        return "scoring_derived"
    if any(k in cl for k in ("award", "pro_bowl", "all_pro", "mvp", "opoy", "dpoy",
                             "oroy", "droy", "cpoy")):
        return "awards"
    if any(k in cl for k in ("_id", "player_week", "slug", "headshot")) or cl in (
            "player", "espn_id", "pfr_id", "week", "year", "status"):
        return "identifier"
    if any(k in cl for k in ("birth", "death", "college", "draft", "height", "weight",
                             "hof", "forty", "bench", "vertical", "broad", "cone",
                             "shuttle", "hand_size", "arm_length", "wonderlic")) or cl in (
            "age", "age_at_draft"):
        return "bio"
    if any(k in cl for k in ("allowed", "dst_", "points_allowed", "three_out",
                             "fourth_down_stop", "shutout")):
        return "team_dst"
    if any(k in cl for k in ("def_", "tackle", "sack", "interception", "solo",
                             "assist", "qb_hit", "pressure", "hurr", "blitz",
                             "safety_md", "fumbles_forced")) and "sacks_suffered" not in cl:
        return "idp_defense"
    if any(k in cl for k in ("epa", "wpa", "cpoe", "success")):
        return "advanced_epa"
    if any(k in cl for k in ("pass", "cmp", "completion", "dropback", "dakota",
                             "sacks_suffered", "sack_yards")):
        return "passing"
    if any(k in cl for k in ("rush", "carries", "scramble", "ypc")):
        return "rushing"
    if any(k in cl for k in ("rec", "target", "catch", "adot", "air_yard", "xyac",
                             "wopr", "racr", "pacr")):
        return "receiving"
    if any(k in cl for k in ("fg_", "pat_", "xp_", "kicking")):
        return "kicking"
    if "punt" in cl and "return" not in cl:
        return "punting"
    if "return" in cl or "kick_ret" in cl:
        return "returns"
    if any(k in cl for k in ("snap", "block", "starter", "games_played",
                             "games_started", "offense_pct", "defense_pct", "st_pct")):
        return "usage_ol"
    if any(k in cl for k in ("team", "opponent", "home", "away", "game_", "margin",
                             "win", "loss", "overtime", "coach", "stadium",
                             "week_type", "season_type", "franchise", "div_")):
        return "context"
    return "other"


def plane_columns() -> dict[str, list[str]]:
    con = duckdb.connect()
    base = Path(S.v26_plane("season")).parent

    def cols(path) -> list[str]:
        p = Path(path).as_posix()
        return [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()]

    out = {
        "weekly": cols(S.latest_v26()),
        "season": cols(S.v26_plane("season")),
        "season_playoffs": cols(base / "player_nfl_season_all.parquet"),
        "career": cols(S.v26_plane("career")),
        "career_playoffs": cols(base / "player_nfl_career_all.parquet"),
        "player_bio": cols(S.PLAYER_BIO.path),
    }
    con.close()
    return out


def main() -> int:
    merged = json.loads(MERGED.read_text(encoding="utf-8"))
    reg = S.registry()
    from .lineage_roots import root_of

    # (source, source_table, v26_col) measured with >0 compared rows in the full audit
    measured = {(r["source"], r.get("source_table", "") or "", r["v26_col"])
                for r in merged["rows"] if (r.get("n") or 0) > 0}

    # MEASURED per-column density start (witness_reach.py): declared spans are source
    # facts, never column facts; a first stray nonzero is noise; START is the first
    # sustained qualifying season, and the decade density scale rides along so the
    # ledger shows when a source actually turns on.
    reach_path = OUT / "witness_reach.json"
    reach: dict[tuple, dict] = {}
    if reach_path.exists():
        for r in json.loads(reach_path.read_text(encoding="utf-8"))["measured"]:
            reach[(r["source"], r["source_table"], r["source_col"])] = r

    # DOCUMENTATION-FIRST: every spec counts on every plane its column lives on.
    cells: dict[tuple, dict] = defaultdict(
        lambda: {"count": 0, "min_year": None, "declared_year": None,
                 "week_grain": 0, "measured": 0, "sources": {}})
    for spec in WITNESS_MAP:
        if spec.season_type == "POST":
            tabs = ["weekly", "season_playoffs", "career_playoffs", "player_bio"]
        elif spec.season_type == "PRE":
            tabs = ["weekly"]
        else:
            tabs = ["weekly", "season", "career", "player_bio"]
        src = reg[spec.source_key]
        declared = max(src.year_min, 1920)
        r = reach.get((spec.source_key, spec.source_table or "", spec.source_col))
        start = r.get("start") if r else None
        # root resolved at the source's MODERN endpoint: era-split sources (pbp rollups)
        # change root at the pbp split year, and year_min resolution buried the whole
        # PBP column inside PFR. The lineage contract stays the single resolver.
        root = root_of(spec.source_key, min(src.year_max, 2025))
        is_measured = (spec.source_key, spec.source_table or "", spec.v26_col) in measured
        # the declared-span fallback is legal ONLY where measurement is impossible
        # (no year axis / unmeasured surface). A source measured IN-SPAN and found
        # EMPTY makes no reach claim at all -- '~1920' off a declared span was a
        # worse lie than a dash.
        fallback_ok = r is None or r.get("why") == "no year axis"
        for tab in tabs:
            cell = cells[(tab, spec.v26_col, root)]
            cell["count"] += 1
            if start is not None:
                cell["min_year"] = (start if cell["min_year"] is None
                                    else min(cell["min_year"], start))
            if fallback_ok:
                cell["declared_year"] = (declared if cell["declared_year"] is None
                                         else min(cell["declared_year"], declared))
            # per-source detail for the ledger tooltip: which source claims which
            # start, with its decade density scale
            skey = spec.source_key
            sdet = cell["sources"].setdefault(skey, {"start": None, "density": None})
            if start is not None and (sdet["start"] is None or start < sdet["start"]):
                sdet["start"] = start
                sdet["density"] = (r or {}).get("decade_density")
            if (spec.validation_grain or "season") == "week":
                cell["week_grain"] += 1
            if is_measured:
                cell["measured"] += 1

    tabs_out = {}
    for tab, columns in plane_columns().items():
        rows = []
        for col in sorted(set(columns)):
            entry = {"column": col, "category": category(col), "roots": {}, "total": 0}
            years = []
            for root in ROOT_ORDER:
                cell = cells.get((tab, col, root))
                if cell:
                    year = cell["min_year"]
                    if year is not None:
                        year_kind = "measured"
                    elif cell["declared_year"] is not None:
                        year, year_kind = cell["declared_year"], "declared"
                    else:
                        year_kind = "none"  # loaded specs, but no in-span data found
                    entry["roots"][root] = {
                        "count": cell["count"], "year": year, "year_kind": year_kind,
                        "year_approx": year_kind != "measured",
                        "sources": cell["sources"],
                        "week_grain": cell["week_grain"], "measured": cell["measured"]}
                    entry["total"] += cell["count"]
                    if year_kind == "measured":
                        years.append(year)
            entry["min_year"] = min(years) if years else None
            rows.append(entry)
        rows.sort(key=lambda r: (CATEGORY_ORDER.index(r["category"]), r["column"]))
        tabs_out[tab] = rows

    payload = {
        "artifact": "witness_pivot",
        "generated_from": str(MERGED),
        "root_order": ROOT_ORDER,
        "root_labels": ROOT_LABELS,
        "category_order": CATEGORY_ORDER,
        "tabs": tabs_out,
        "notes": [
            "READY-TO-VERIFY coverage: counts come from the witness documentation (WITNESS_MAP), and a spec witnesses its column on every plane the column lives on. Validation grain is where a value is checked, never a coverage partition.",
            "REG tabs count non-POST specs; the incl.-playoffs tabs count POST-stratum specs; weekly counts every stratum.",
            "measured = specs that compared >0 rows in the 2026-08-01 full-stratum audit; newspaper specs are documented but unaudited by scope decision.",
            "Year is the MEASURED first season the exact (table, column) holds non-blank values (witness_reach.py); cells with no measurable year axis fall back to the declared source span and carry year_approx=true.",
            "Counts are per-root spec counts; several tables of one source each count.",
        ],
    }
    PIVOT.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    summary = {tab: {"columns": len(rows),
                     "zero_witness": sum(1 for r in rows if r["total"] == 0)}
               for tab, rows in tabs_out.items()}
    print(json.dumps({"pivot": str(PIVOT), **summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
