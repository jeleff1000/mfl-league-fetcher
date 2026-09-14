"""Search every v26 subject plane for NFL.com season-page demand candidates.

This is a measurement-only schema receipt.  It deliberately records both the exact
canonical lookup and a narrow alias search so a NEW_SUPERTABLE_COLUMN_CANDIDATE is
never justified by prose alone.  It does not edit the ledger or any v26 artifact.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb

from .sources import v26_plane


OUT = (
    Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
    / "nflcom_player_season_schema_search.json"
)


SEARCHES = {
    "def_interception_long": ("interception_long", "int_long", "def_int_long"),
    "punt_touchbacks": ("punt_touchback", "punt_tb", "punts_tb", "touchbacks"),
    "punts_inside_20": ("punt_in_20", "punts_inside_20", "inside_20", "in_20"),
    "punt_net_yards": ("punt_net", "net_punt", "net_yds"),
}


def _columns(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    return [row[0] for row in con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [path]
    ).fetchall()]


def measure() -> dict:
    con = duckdb.connect()
    planes = {
        "weekly": v26_plane("weekly"),
        "season": v26_plane("season"),
        "career": v26_plane("career"),
        "season_team": v26_plane("season_team"),
    }
    schema = {}
    for plane, path in planes.items():
        cols = _columns(con, path)
        schema[plane] = {
            "path": path,
            "column_count": len(cols),
            "columns": cols,
        }

    searches = []
    for canonical, aliases in SEARCHES.items():
        by_plane = {}
        for plane, details in schema.items():
            cols = details["columns"]
            lower = {col.lower(): col for col in cols}
            exact = lower.get(canonical.lower())
            matches = sorted({
                col for col in cols
                if any(
                    re.search(
                        rf"(?:^|_){re.escape(alias.lower())}(?:_|$)",
                        col.lower(),
                    )
                    for alias in aliases
                )
            })
            by_plane[plane] = {
                "exact_match": exact,
                "alias_search": list(aliases),
                "alias_matches": matches,
            }
        searches.append({"proposed_canonical": canonical, "planes": by_plane})

    result = {
        "source": "nflcom_player_season",
        "search_method": "DESCRIBE SELECT * FROM read_parquet(...) plus exact/alias column search",
        "planes": schema,
        "searches": searches,
        "all_four_exactly_absent": all(
            all(entry["planes"][plane]["exact_match"] is None for plane in planes)
            for entry in searches
        ),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    con.close()
    return result


def main() -> int:
    result = measure()
    print(f"schema receipt -> {OUT}")
    print(f"planes={len(result['planes'])} candidates={len(result['searches'])} "
          f"all_four_exactly_absent={result['all_four_exactly_absent']}")
    for entry in result["searches"]:
        print(entry["proposed_canonical"], {
            plane: details["exact_match"]
            for plane, details in entry["planes"].items()
        })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
