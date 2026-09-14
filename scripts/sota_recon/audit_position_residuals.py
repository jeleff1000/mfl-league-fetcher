"""Audit historical StatsCrew roster-position residuals against position authorities."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
LAKE = Path(r"D:\league-history-data\nfl")
OUT = REPO_ROOT / "docs/audits/sota-recon/position"
SC_ROSTER = LAKE / "ff_assets/statscrew/team_season_roster/29669388268-rosters-recovered-20260722/**/*.parquet"
SC_XW = LAKE / "derived/validation/sota_recon_master/statscrew_player_pfr_crosswalk.parquet"
PFR_DECL = LAKE / "derived/validation/sota_recon_master/pfr_season_position_declaration.parquet"
NFLCOM_XW = LAKE / "derived/entity_universes/nflcom_slug_pfrid.parquet"
NFLCOM_POS = LAKE / "derived/validation/sota_recon_master/nflcom_player_page_positions.parquet"
WEEKLY = LAKE / "releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables/weekly_repaired_parts/year=*.parquet"

TOKEN_TO_BROAD = {
    "QB": "QB", "TB": "RB", "BB": "RB", "B": "RB", "RB": "RB", "HB": "RB", "FB": "RB", "LH": "RB", "RH": "RB", "LHB": "RB", "RHB": "RB",
    "WR": "WR", "FL": "WR", "SE": "WR", "E": "WR", "LE": "WR", "RE": "WR", "OE": "WR", "OHB": "WR",
    "TE": "TE", "K": "K", "P": "P", "LS": "DB", "RS": "DB", "S": "DB", "FS": "DB", "SS": "DB", "DB": "DB", "CB": "DB", "LCB": "DB", "RCB": "DB", "LDH": "DB", "RDH": "DB", "DH": "DB", "L": "DB", "R": "DB",
    "LB": "LB", "LLB": "LB", "RLB": "LB", "MLB": "LB", "ILB": "LB", "OLB": "LB", "RL": "LB", "LL": "LB",
    "DL": "DL", "DE": "DL", "DT": "DL", "LDE": "DL", "RDE": "DL", "LDT": "DL", "RDT": "DL", "NT": "DL", "MG": "DL", "DG": "DL", "LD": "DL", "RD": "DL",
    "OL": "OL", "C": "OL", "G": "OL", "T": "OL", "OT": "OL", "OG": "OL", "LG": "OL", "RG": "OL", "LT": "OL", "RT": "OL", "LOT": "OL", "ROT": "OL", "LOG": "OL", "ROG": "OL", "LO": "OL", "RO": "OL",
}


def parse_tokens(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [part for part in re.split(r"[-/]", str(raw).upper()) if part]


def mapped_tokens(tokens: list[str]) -> list[str]:
    return sorted({TOKEN_TO_BROAD[token] for token in tokens if token in TOKEN_TO_BROAD})


def classify_case(raw_label: str, year: int, pfr_broad: list[str], nflcom_broad: list[str], current_broad: list[str]) -> dict:
    raw = str(raw_label or "").strip().upper()
    mapped = mapped_tokens(parse_tokens(raw))
    pfr = set(pfr_broad or [])
    nfl = set(nflcom_broad or [])
    if raw == "LS" and 1950 <= year <= 1969:
        if "DB" in pfr or "DB" in nfl or "DB" in current_broad:
            return {"role_kind": "ALIGNMENT", "adjudication_class": "SAME_BROAD_ALIGNMENT", "reason": "StatsCrew LS is supported as a historical left-safety alignment token; the matched authority resolves to DB."}
        return {"role_kind": "UNKNOWN", "adjudication_class": "UNRESOLVABLE_SOURCE_CONFLICT", "reason": "Historical LS has no independent DB authority in the joined record."}
    if raw == "P":
        if pfr == {"P"} or "P" in nfl or "P" in current_broad:
            return {"role_kind": "POSITION", "adjudication_class": "EXACT_ALIAS", "reason": "StatsCrew P agrees with an authoritative punter position."}
        if pfr and pfr.isdisjoint({"P"}) and (pfr & (nfl | set(current_broad))):
            return {"role_kind": "USAGE_ROLE", "adjudication_class": "USAGE_ROLE_NOT_PRIMARY", "reason": "StatsCrew P is a rostered specialist/usage role while the authoritative season declaration is another primary position."}
        return {"role_kind": "UNKNOWN", "adjudication_class": "SOURCE_NATIVE_ONLY", "reason": "StatsCrew P is retained as a valid source-native position claim but lacks a comparable primary authority."}
    if set(mapped) & pfr:
        return {"role_kind": "POSITION", "adjudication_class": "SAME_BROAD_ALIGNMENT", "reason": "Source token set overlaps the authoritative broad position."}
    return {"role_kind": "UNKNOWN", "adjudication_class": "UNRESOLVABLE_SOURCE_CONFLICT", "reason": "Source token set does not overlap the authoritative broad position."}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _as_list(value) -> list[str]:
    if value is None:
        return []
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [str(x) for x in value if x is not None]
    text = str(value).strip().strip("[]")
    return [x.strip(" '\"") for x in text.split(",") if x.strip(" '\"")]


def build_cases() -> list[dict]:
    con = duckdb.connect()
    q = f"""
    WITH sc AS (
      SELECT r.season AS yr,r.team,r.player,r.source_player_id,r.position AS sc_raw,r.source_url,
             x.pfr_id,x.NFL_player_id
      FROM read_parquet('{SC_ROSTER.as_posix()}', union_by_name=true) r
      JOIN read_parquet('{SC_XW.as_posix()}') x USING(source_player_id)
      WHERE r.season BETWEEN 1950 AND 1969
        AND (upper(trim(r.position)) IN ('LS','P') OR upper(r.position) LIKE '%-LS-%' OR upper(r.position) LIKE 'LS-%' OR upper(r.position) LIKE '%-LS' OR upper(r.position) LIKE '%P%')
    ), n AS (
      SELECT x.nflcom_slug,x.pfr_id,p.season,p.position_raw,p.page_kind,p.source_url
      FROM read_parquet('{NFLCOM_XW.as_posix()}') x JOIN read_parquet('{NFLCOM_POS.as_posix()}') p USING(nflcom_slug)
    ), n1 AS (
      SELECT pfr_id,season,ANY_VALUE(nflcom_slug) AS nflcom_slug,
             string_agg(DISTINCT position_raw, '|') AS nflcom_raw,
             string_agg(DISTINCT source_url, '|') AS nflcom_urls
      FROM n GROUP BY 1,2
    ), d AS (
      SELECT NFL_player_id AS pfr_id,year AS yr,declared,broads
      FROM read_parquet('{PFR_DECL.as_posix()}')
    ), w AS (
      SELECT NFL_player_id,year,ANY_VALUE(nfl_position) AS current_nfl_position,
             ANY_VALUE(position) AS current_position,ANY_VALUE(fantasy_position) AS current_fantasy_position
      FROM read_parquet('{WEEKLY.as_posix()}', union_by_name=true)
      WHERE year BETWEEN 1950 AND 1969 GROUP BY 1,2
    )
    SELECT sc.*,d.declared AS pfr_raw_pos,d.broads,n1.nflcom_raw,n1.nflcom_urls,
           n1.nflcom_slug,w.current_nfl_position,w.current_position,w.current_fantasy_position
    FROM sc LEFT JOIN d USING(pfr_id,yr)
    LEFT JOIN n1 ON n1.pfr_id=sc.pfr_id AND n1.season=sc.yr
    LEFT JOIN w ON w.NFL_player_id=sc.NFL_player_id AND w.year=sc.yr
    ORDER BY yr,player,team
    """
    rows = con.execute(q).fetchdf().to_dict("records")
    cases = []
    for index, row in enumerate(rows, 1):
        pfr_broad = _as_list(row.get("broads"))
        nfl_raw = row.get("nflcom_raw")
        nfl_tokens = parse_tokens(nfl_raw)
        nfl_broad = mapped_tokens(nfl_tokens)
        current_broad = _as_list(row.get("current_position"))
        decision = classify_case(str(row.get("sc_raw") or ""), int(row["yr"]), pfr_broad, nfl_broad, current_broad)
        cases.append({
            "case_id": f"statscrew-position-{index:05d}",
            "pfr_id": row.get("pfr_id"), "NFL_player_id": row.get("NFL_player_id"), "nflcom_slug": row.get("nflcom_slug"),
            "player": row.get("player"), "year": int(row["yr"]), "team": row.get("team"), "season_type": "REGULAR_SEASON_ROSTER",
            "source_root": "statscrew", "source_dataset": "team_season_roster", "raw_label": row.get("sc_raw"),
            "source_locator": row.get("source_url"), "parsed_tokens": parse_tokens(row.get("sc_raw")),
            "mapped_broad_tokens": mapped_tokens(parse_tokens(row.get("sc_raw"))),
            "pfr_raw_per_season_pos": row.get("pfr_raw_pos"), "pfr_broad_tokens": pfr_broad,
            "pfr_raw_position_source_status": "DECLARATION_ARTIFACT_NORMALIZED_TOKEN; RAW_POS_COLUMN_NOT_PRESENT_IN_CURRENT_SOURCE_FILE",
            "nflcom_raw_position": nfl_raw, "nflcom_parsed_tokens": nfl_tokens, "nflcom_broad_tokens": nfl_broad,
            "nflcom_source_locator": row.get("nflcom_urls"),
            "current_nfl_position": row.get("current_nfl_position"), "current_position": row.get("current_position"),
            "current_fantasy_position": row.get("current_fantasy_position"),
            "role_kind": decision["role_kind"], "authoritative_for_primary": False, "authoritative_for_eligibility": False,
            "adjudication_class": decision["adjudication_class"], "reason": decision["reason"],
            "identity_confidence": "crosswalk_receipted" if row.get("pfr_id") else "missing",
            "lineage_root": "statscrew",
        })
    return cases


def main() -> None:
    cases = build_cases()
    OUT.mkdir(parents=True, exist_ok=True)
    fields = list(cases[0]) if cases else []
    with (OUT / "position-residual-cases.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for case in cases:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, list) else value for key, value in case.items()})
    counts = Counter(case["adjudication_class"] for case in cases)
    summary = {
        "schema_version": "position-residual-summary.v1", "generated_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "StatsCrew team-season roster position residuals, 1950-1969", "case_count": len(cases),
        "raw_label_counts": dict(Counter(str(case["raw_label"]) for case in cases)),
        "adjudication_counts": dict(counts), "unclassified_1950_1969": sum(case["adjudication_class"] == "UNCLASSIFIED" for case in cases),
        "terminal_classes": ["UNAVAILABLE_IN_CURRENT_SOURCE_UNIVERSE", "UNRESOLVABLE_SOURCE_CONFLICT", "SOURCE_NATIVE_ONLY", "WITHHELD_FROM_CERTIFIED_TABLE"],
        "input_hashes": {str(path): _sha256(path) for path in (PFR_DECL, NFLCOM_POS, SC_XW, NFLCOM_XW)},
        "notes": ["StatsCrew roster position is source-native and never an eligibility authority.", "Historical LS is scoped to 1950-1969 and supported as left-safety alignment by paired source vocabulary and matched DB authorities.", "P is retained as a valid position and classified per player-season; no global P demotion is applied."],
    }
    (OUT / "position-residual-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    aliases = json.loads((REPO_ROOT / "scripts/sota_recon/contracts/position_alias_decisions.v1.json").read_text(encoding="utf-8"))
    (OUT / "position-alias-decisions.json").write_text(json.dumps(aliases, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"cases": len(cases), "adjudication_counts": dict(counts), "output": str(OUT)}, indent=2))


if __name__ == "__main__":
    main()
