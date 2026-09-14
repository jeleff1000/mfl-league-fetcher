"""build_nfl_market_adp.py -- the format-split market-ADP dimension table (local parquet).

Two sources, both joined to NFL_player_id:
  * FFC  (local JSON in fantasy_leagues/external_adp/ffc/): format-split (std/half/ppr/
    2qb=superflex/dynasty/rookie), 2008-2024, with dispersion. Matched by normalized name
    to a (NFL_player_id, player, position) crosswalk pulled from the super table.
  * Yahoo (cached locally after a read-only source pull): format-BLIND market ADP 2002+, joined to
    NFL_player_id via player_bio.yahoo_player_id (validated 98.7-100% this session).

Output: D:/league-history-data/fantasy_leagues/cohort_aggregates/nfl_market_adp.parquet
    (year, source, market_format, teams, NFL_player_id, position, adp, stdev, times_drafted, pct_drafted)

Read-only against Fly. Local parquet output only.
    py -3 scripts/research_cohorts/build_nfl_market_adp.py
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from market_adp import normalize_yahoo_draft_rate

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

FFC_DIR = Path("D:/league-history-data/fantasy_leagues/external_adp/ffc")
YAHOO_CACHE = Path("D:/league-history-data/fantasy_leagues/external_adp/yahoo/yahoo_market_adp.parquet")
OUT_DIR = Path("D:/league-history-data/fantasy_leagues/cohort_aggregates")
OUT = OUT_DIR / "nfl_market_adp.parquet"

# FFC filename fmt token -> canonical market_format
FFC_FMT = {"standard": "std", "ppr": "ppr", "half-ppr": "half",
           "2qb": "sflx", "dynasty": "dynasty", "rookie": "rookie"}


def load_env() -> None:
    import os
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def norm(n: str) -> str:
    n = (n or "").lower().strip()
    n = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", n)
    n = re.sub(r"[^a-z ]", "", n)
    return re.sub(r"\s+", " ", n).strip()


def main() -> None:
    load_env()
    from multi_league.core.fly_writer import FlyWriter

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fly = FlyWriter()

    # (1) name -> NFL_player_id crosswalk from the super table (draftable era, offense+K+DEF).
    xw_rows = fly.execute(
        """
        SELECT DISTINCT NFL_player_id, player, position
        FROM nfl_historical.nfl_player_stats_all
        WHERE player IS NOT NULL AND year >= 2002 AND NFL_player_id IS NOT NULL
        """,
        database="___ops",
    )
    name_to_id: dict[str, str] = {}
    id_to_position: dict[str, str | None] = {}
    ambiguous: set[str] = set()
    for r in xw_rows:
        nm = norm(r["player"])
        if not nm:
            continue
        pid = str(r["NFL_player_id"])
        id_to_position.setdefault(pid, r.get("position"))
        if nm in name_to_id and name_to_id[nm] != pid:
            ambiguous.add(nm)  # same normalized name, different ids -> skip (rare)
        else:
            name_to_id[nm] = pid
    for nm in ambiguous:
        name_to_id.pop(nm, None)

    out: list[dict] = []

    # (2) FFC local JSON -> matched rows
    ffc_files = sorted(FFC_DIR.glob("*.json"))
    ffc_matched = ffc_total = 0
    for f in ffc_files:
        parts = f.stem[len("ffc_"):].rsplit("_", 2)  # fmt, teams(##t), year
        if len(parts) != 3:
            continue
        fmt_tok, teams_tok, year_tok = parts
        market_format = FFC_FMT.get(fmt_tok)
        if not market_format:
            continue
        teams = int(teams_tok.rstrip("t"))
        year = int(year_tok)
        d = json.loads(f.read_text())
        for p in d.get("players", []):
            ffc_total += 1
            pid = name_to_id.get(norm(p.get("name", "")))
            if not pid:
                continue
            ffc_matched += 1
            out.append({
                "year": year, "source": "ffc", "market_format": market_format, "teams": teams,
                "NFL_player_id": pid, "position": id_to_position.get(pid),
                "adp": p.get("adp"), "stdev": p.get("stdev"),
                "times_drafted": p.get("times_drafted"), "pct_drafted": None,
            })

    # (3) Yahoo (format-blind), local-cache first. The source pull is read-only and the
    # joined result is persisted so every subsequent cohort rebuild is fully local.
    if YAHOO_CACHE.exists():
        yahoo_rows = pq.read_table(YAHOO_CACHE).to_pylist()
        yahoo_basis = "local cache"
    else:
        yahoo_rows = fly.execute(
            """
            SELECT a.year, b.NFL_player_id,
                   a.avg_pick AS adp, a.percent_drafted AS pct_drafted
            FROM yahoo_historical.yahoo_draft_analysis a
            JOIN (SELECT DISTINCT CAST(CAST(yahoo_player_id AS BIGINT) AS VARCHAR) AS yid,
                                  NFL_player_id
                  FROM nfl_historical.player_bio WHERE yahoo_player_id IS NOT NULL) b
              ON TRIM(a.yahoo_player_id) = b.yid
            WHERE a.year >= 2002 AND a.avg_pick IS NOT NULL
            """,
            database="___ops",
        )
        YAHOO_CACHE.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(yahoo_rows), YAHOO_CACHE)
        yahoo_basis = "read-only source pull; cache created"
    for r in yahoo_rows:
        out.append({
            "year": int(r["year"]), "source": "yahoo", "market_format": "blind", "teams": None,
            "NFL_player_id": str(r["NFL_player_id"]),
            "position": r.get("position") or id_to_position.get(str(r["NFL_player_id"])),
            "adp": r["adp"], "stdev": None,
            "times_drafted": None,
            "pct_drafted": normalize_yahoo_draft_rate(r["pct_drafted"]),
        })

    if not out:
        raise SystemExit("no market ADP rows produced")
    pq.write_table(pa.Table.from_pylist(out), OUT)

    ffc_pct = 100.0 * ffc_matched / ffc_total if ffc_total else 0
    yahoo_n = sum(1 for r in out if r["source"] == "yahoo")
    print(f"[market-adp] {len(out):,} rows -> {OUT}")
    print(f"[market-adp] FFC: {ffc_matched:,}/{ffc_total:,} name-matched ({ffc_pct:.1f}%) across {len(ffc_files)} files")
    print(f"[market-adp] Yahoo: {yahoo_n:,} rows ({yahoo_basis}) | crosswalk names {len(name_to_id):,} (ambiguous dropped {len(ambiguous)})")
    fmts = sorted({(r["source"], r["market_format"]) for r in out})
    print(f"[market-adp] source/format pairs: {fmts}")


if __name__ == "__main__":
    main()
