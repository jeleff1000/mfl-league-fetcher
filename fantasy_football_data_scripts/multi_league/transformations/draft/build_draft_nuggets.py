#!/usr/bin/env python3
"""Draft-nugget producer — precompute the intelligence display artifact.

The miner is 60-140s/league (fleet ADP + fleet timing joins across ~1,300
leagues), far too slow for an on-demand web request, so nuggets are
precomputed. This writes one structured JSON artifact per league that the
frontend serves statically (same pattern as manager_signals.json today):

    frontend/public/draft-nuggets/<db_name>.json

Shape (consumed by the Draft Intelligence dossier). The league report and the
manager dossiers are separate. Each manager dossier is several matter-of-fact
blurbs built on the recap-style phrase skeleton, plus a compact trivia footer:
    {
      "db_name", "model_version", "generated_at",
      "league": [ { theme, items: [ { text, tone } ] } ],
      "manager_order": [ name, ... ],          # strongest first, one page each
      "managers": { name: { facts: [ { text, tone, kind } ], also: str } }
    }

Usage:
    python -m multi_league.transformations.draft.build_draft_nuggets \
        --db the_league [--db nyu_ffl ...] [--out <dir>]
    python -m multi_league.transformations.draft.build_draft_nuggets --qa
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.transformations.draft.briefing import compose_league  # noqa: E402
from multi_league.transformations.draft.draft_promo import build_manager_promo  # noqa: E402
from multi_league.transformations.draft.construction_miner import (  # noqa: E402
    run_construction_miner_fly,
)
from multi_league.transformations.draft.wide_correlation_miner import (  # noqa: E402
    run_league_fleet_personality,
    run_wide_correlation_miner_fly,
)

MODEL_VERSION = "draft-nuggets-v1"

QA_LEAGUES = [
    "the_league", "nyu_ffl", "tfl_of_extraordinary_gentleman", "keeper_league",
    "zootown_dynasty", "degenerate_gamblers_football_league", "l_14_big_booms",
    "the_pigskin_platoon", "rock_hill_fantasy_league", "champions_branch_out",
    "u_can_t_handle_this_111c", "h_town_auction", "gridiron_gurus", "wbffl",
    "get_your_piss_hot_league", "live_draft_beer_league",
]

def build_one(db_name: str) -> dict:
    wide = run_wide_correlation_miner_fly(db_name, limit=600)
    construction = run_construction_miner_fly(db_name)
    fleet = run_league_fleet_personality(db_name)

    manager_rows = [
        r for r in (wide["manager_tendencies"] + construction)
        if r.get("surfaced") and "nugget_headline" in r and r.get("scope_type") == "manager"
    ]
    by_manager: dict[str, list[dict]] = {}
    for row in manager_rows:
        by_manager.setdefault(str(row.get("scope_label", "")), []).append(row)

    managers: dict[str, dict] = {}
    strength: dict[str, float] = {}
    for name, rows in by_manager.items():
        if not name:
            continue
        promo = build_manager_promo(rows, name, db_name)
        if promo["facts"] or promo["also"]:
            managers[name] = promo
            strength[name] = max(float(r.get("surface_score", 0) or 0) for r in rows)

    manager_order = sorted(managers, key=lambda n: -strength[n])

    market_rows = [
        x for x in wide["league_inefficiencies"]
        if x.get("surfaced") and "nugget_headline" in x
    ]
    league = compose_league(fleet, market_rows)

    return {
        "db_name": db_name,
        "model_version": MODEL_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "league": league,
        "manager_order": manager_order,
        "managers": managers,
    }


def default_out_dir() -> Path:
    # frontend/public/draft-nuggets is served as a static asset.
    here = Path(__file__).resolve()
    node = here
    while node != node.parent:
        candidate = node / "frontend" / "public"
        if candidate.exists():
            return candidate / "draft-nuggets"
        node = node.parent
    return here.parent / "_artifacts" / "draft-nuggets"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build precomputed draft nugget artifacts")
    parser.add_argument("--db", action="append", default=[], help="League db_name (repeatable)")
    parser.add_argument("--qa", action="store_true", help="Build the optimizer QA-league set")
    parser.add_argument("--out", type=Path, default=None, help="Output dir (default frontend/public/draft-nuggets)")
    args = parser.parse_args()

    dbs = list(args.db) or (QA_LEAGUES if args.qa else [])
    if not dbs:
        parser.error("pass --db <name> (repeatable) or --qa")

    out_dir = args.out or default_out_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, db in enumerate(dbs, 1):
        try:
            artifact = build_one(db)
        except Exception as exc:  # noqa: BLE001
            print(f"[{i}/{len(dbs)}] FAIL {db}: {exc}", flush=True)
            continue
        path = out_dir / f"{db}.json"
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(artifact, fh, indent=1)
            fh.write("\n")
        league_ct = sum(len(g["items"]) for g in artifact["league"])
        fact_ct = sum(len(m["facts"]) for m in artifact["managers"].values())
        print(f"[{i}/{len(dbs)}] {db}: {league_ct} league lines, "
              f"{fact_ct} facts across {len(artifact['managers'])} manager pages -> {path.name}", flush=True)


if __name__ == "__main__":
    main()
