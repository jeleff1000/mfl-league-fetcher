"""Adjudicate the detected newspaper witness conflicts against the lake's own witnesses.

The conflict audit (witness_conflict_audit.py) enumerates cross-source scalar
disagreements INSIDE the newspaper lineage. This pass resolves them the same way the
v26 reconciliation resolves everything: bring an INDEPENDENT lineage to bear, cite it,
and never guess. Verdict vocabulary is closed; every conflict lands in exactly one:

  RESOLVED_EXTERNAL   an independent lake witness (nfl_team_games_all scores,
                      pfr game_info attendance, the per-play scoring log) matches
                      exactly ONE of the conflicting newspaper values -> that value
                      wins, citation retained
  ESCALATED_NO_MATCH  an external witness exists but matches NONE of the values ->
                      the most interesting rows on the board (either every paper is
                      wrong or OUR witness is) -> manual review with evidence
  RESOLVED_MAJORITY   no external witness covers the claim; >=2 source documents
                      agree on one value against a minority -> majority wins,
                      labeled as majority (weaker class, never silently equal to
                      external resolution)
  HELD_TIE            no external witness, no majority (1v1) -> held with evidence
  NON_ATOM            narrative claims (division race status etc.) -- excluded from
                      promotion entirely, not stat atoms

Read-only against the newspaper database and the lake. Writes resolutions +
summary into the audit run directory under adjudication_v1/.

    python -m scripts.newspaper_review.newspaper_conflict_adjudication [--audit-dir DIR]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
AUDITS_ROOT = DATA_ROOT / "witness_conflict_audits"
LAKE = Path(r"D:\league-history-data\nfl")
TEAM_GAMES = LAKE / "raw" / "pfr" / "boxscores" / "nfl_team_games_all.parquet"
GAME_INFO = LAKE / "raw" / "pfr" / "boxscores" / "tables" / "game_info" / "_combined.parquet"
SCORING = LAKE / "raw" / "pfr" / "boxscores" / "tables" / "scoring" / "_combined.parquet"
TEAM_STATS = LAKE / "raw" / "pfr" / "boxscores" / "tables" / "team_stats" / "_combined.parquet"

NON_ATOM_FIELDS = {"western_division_race_status", "division_championship_result"}


def norm(value: object) -> str:
    # NOT `value or ""` -- integer 0 is falsy and would erase every shutout side
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    if re.fullmatch(r"[-+]?\d[\d,]*(?:\.\d+)?", text):
        return text.replace(",", "")
    return text.lower()


def canonical_score(team_a: str, pts_a: object, team_b: str, pts_b: object) -> str:
    pairs = [(norm(team_a), norm(pts_a)), (norm(team_b), norm(pts_b))]
    pairs = [(t, v) for t, v in pairs if t and v]
    return "|".join(f"{t}={v}" for t, v in sorted(pairs))


def _only_unique(candidates: set):
    return next(iter(candidates)) if len(candidates) == 1 else None


def _normalized_team_values(raw_values: object) -> dict[str, str]:
    if not isinstance(raw_values, dict):
        return {}
    values: dict[str, str] = {}
    ambiguous: set[str] = set()
    for raw_team, raw_value in raw_values.items():
        team = norm(raw_team)
        value = norm(raw_value)
        if not team or not value:
            continue
        if team in values and values[team] != value:
            ambiguous.add(team)
            continue
        values[team] = value
    for team in ambiguous:
        values.pop(team, None)
    return values


def latest_audit_dir() -> Path:
    runs = sorted(p for p in AUDITS_ROOT.iterdir() if p.is_dir())
    if not runs:
        raise FileNotFoundError(f"no audit runs under {AUDITS_ROOT}")
    return runs[-1]


def load_conflicts(audit_dir: Path) -> list[dict[str, str]]:
    with (audit_dir / "detected_scalar_witness_conflicts.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def lake_witnesses(con: duckdb.DuckDBPyConnection, boxscore_ids: list[str]) -> dict:
    """Pull every lake witness that can speak to the conflicted boxscores."""
    inlist = ", ".join(f"'{b}'" for b in boxscore_ids)
    team_game_rows = con.execute(f"""
            SELECT boxscore_id, team_code, team_points,
                   opponent_code, opponent_points
            FROM '{TEAM_GAMES.as_posix()}'
            WHERE boxscore_id IN ({inlist}) AND COALESCE(is_home, FALSE)""").fetchall()
    score_candidates: dict[str, set[tuple[str, tuple[tuple[str, str], ...]]]] = {}
    side_candidates: dict[str, set[tuple[str, str]]] = {}
    for box, home_team_raw, home_points_raw, visitor_team_raw, visitor_points_raw in team_game_rows:
        home_team = norm(home_team_raw)
        visitor_team = norm(visitor_team_raw)
        if not home_team or not visitor_team or home_team == visitor_team:
            continue
        side_candidates.setdefault(box, set()).add((visitor_team, home_team))
        home_points = norm(home_points_raw)
        visitor_points = norm(visitor_points_raw)
        if not home_points or not visitor_points:
            continue
        by_team = tuple(sorted(((home_team, home_points), (visitor_team, visitor_points))))
        candidate = (canonical_score(home_team, home_points, visitor_team, visitor_points), by_team)
        score_candidates.setdefault(box, set()).add(candidate)

    scores = {}
    for box, candidates in score_candidates.items():
        candidate = _only_unique(candidates)
        if candidate is not None:
            canonical, by_team = candidate
            scores[box] = {"canonical": canonical, "by_team": dict(by_team)}

    team_sides = {
        box: candidate
        for box, candidates in side_candidates.items()
        if (candidate := _only_unique(candidates)) is not None
    }

    attendance_candidates: dict[str, set[str]] = {}
    for box, value_raw in con.execute(f"""
            SELECT boxscore_id, stat
            FROM '{GAME_INFO.as_posix()}'
            WHERE boxscore_id IN ({inlist}) AND LOWER(info) = 'attendance'
            """).fetchall():
        value = norm(value_raw)
        if value:
            attendance_candidates.setdefault(box, set()).add(value)
    attendance = {
        box: candidate
        for box, candidates in attendance_candidates.items()
        if (candidate := _only_unique(candidates)) is not None
    }

    first_down_candidates: dict[str, set[tuple[tuple[str, str], ...]]] = {}
    for b, vis, home in con.execute(f"""
            SELECT boxscore_id, vis_stat, home_stat
            FROM '{TEAM_STATS.as_posix()}'
            WHERE boxscore_id IN ({inlist}) AND stat = 'First Downs'
            """).fetchall():
        sides = team_sides.get(b)
        if sides is None:
            continue
        visitor_team, home_team = sides
        candidate = tuple(
            (team, value)
            for team, value in (
                (visitor_team, norm(vis)),
                (home_team, norm(home)),
            )
            if value
        )
        if candidate:
            first_down_candidates.setdefault(b, set()).add(candidate)
    first_downs = {
        box: dict(candidate)
        for box, candidates in first_down_candidates.items()
        if (candidate := _only_unique(candidates)) is not None
    }
    events = {}
    for b, desc, ids in con.execute(f"""
            SELECT boxscore_id, description, description_link_ids
            FROM '{SCORING.as_posix()}'
            WHERE boxscore_id IN ({inlist}) AND description IS NOT NULL""").fetchall():
        events.setdefault(b, []).append((desc, ids or ""))
    return {"scores": scores, "attendance": attendance,
            "first_downs": first_downs, "events": events}


def adjudicate_one(row: dict[str, str], wit: dict) -> dict[str, object]:
    field = row["field_name"]
    box = row["boxscore_id"]
    values = json.loads(row["values_json"])
    value_sources = json.loads(row["value_sources_json"] or "{}")
    out = dict(audit_conflict_id=row["audit_conflict_id"], surface=row["surface"],
               boxscore_id=box, field_name=field, comparison_key=row["comparison_key"],
               values_json=row["values_json"])

    def finish(verdict, resolved=None, witness=""):
        out.update(verdict=verdict, resolved_value=resolved or "", witness=witness)
        return out

    if field in NON_ATOM_FIELDS:
        return finish("NON_ATOM", witness="narrative claim, not a stat atom")

    external = None
    witness_desc = ""
    if field == "final_score":
        raw_score = wit["scores"].get(box)
        canonical = ""
        team_values: dict[str, str] = {}
        if isinstance(raw_score, dict):
            canonical = norm(raw_score.get("canonical"))
            team_values = _normalized_team_values(raw_score.get("by_team"))
        elif isinstance(raw_score, str):
            canonical = norm(raw_score)
        key_parts = row["comparison_key"].split("|")
        team = norm(key_parts[-1]) if len(key_parts) >= 3 else ""
        has_canonical_claim = any("=" in norm(value) and "|" in norm(value)
                                  for value in values)
        if has_canonical_claim:
            external = canonical or None
        elif team:
            external = team_values.get(team)
        witness_desc = (f"nfl_team_games_all {box}: canonical={canonical or None}; "
                        f"by_team={team_values}")
    elif field == "attendance":
        external = wit["attendance"].get(box)
        witness_desc = f"pfr game_info {box}: attendance {external}"
    elif field == "first_downs" and box in wit["first_downs"]:
        raw_team_values = wit["first_downs"][box]
        key_parts = row["comparison_key"].split("|")
        team = norm(key_parts[-1]) if len(key_parts) >= 3 else ""
        team_values = _normalized_team_values(raw_team_values)
        if team:
            external = team_values.get(team)
        witness_desc = f"pfr team_stats {box}: First Downs by team {team_values}"
    elif field in ("touchdowns", "fg_long", "distance_yards") and box in wit["events"]:
        # named-event adjudication: count TDs / max FG distance for the named player
        key_parts = row["comparison_key"].split("|")
        player = key_parts[1] if len(key_parts) > 2 else ""
        evs = wit["events"][box]
        if field == "touchdowns" and player:
            n = sum(1 for d, _ in evs
                    if player in d.lower() and "field goal" not in d.lower()
                    and "kick)" not in d.lower().split(player)[0])
            external, witness_desc = str(n), f"scoring log {box}: {n} TD events for '{player}'"
        elif field in ("fg_long", "distance_yards") and player:
            dists = [int(m.group(1)) for d, _ in evs if player in d.lower()
                     for m in [re.search(r"(\d+)\s+yard field goal", d)] if m]
            if dists:
                external = str(max(dists))
                witness_desc = f"scoring log {box}: FG distances {dists} for '{player}'"

    if external:
        matches = [v for v in values if norm(v) == norm(external)]
        if len(matches) == 1:
            return finish("RESOLVED_EXTERNAL", matches[0], witness_desc)
        if not matches:
            return finish("ESCALATED_NO_MATCH", external, witness_desc)

    # no external coverage -> majority of source documents
    counts = {v: len(srcs) for v, srcs in value_sources.items()}
    if counts:
        ordered = sorted(counts.items(), key=lambda kv: -kv[1])
        if len(ordered) > 1 and ordered[0][1] > ordered[1][1]:
            return finish("RESOLVED_MAJORITY", ordered[0][0],
                          f"majority {ordered[0][1]} vs {ordered[1][1]} source documents")
    return finish("HELD_TIE", witness="no external witness; no source majority")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-dir", default=None)
    args = parser.parse_args()
    audit_dir = Path(args.audit_dir) if args.audit_dir else latest_audit_dir()
    conflicts = load_conflicts(audit_dir)
    boxscores = sorted({r["boxscore_id"] for r in conflicts if r["boxscore_id"]})
    con = duckdb.connect()
    wit = lake_witnesses(con, boxscores)
    con.close()

    resolutions = [adjudicate_one(r, wit) for r in conflicts]
    out_dir = audit_dir / "adjudication_v1"
    out_dir.mkdir(exist_ok=True)
    fields = ["audit_conflict_id", "surface", "boxscore_id", "field_name",
              "comparison_key", "values_json", "verdict", "resolved_value", "witness"]
    with (out_dir / "conflict_resolutions.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(resolutions)
    verdicts = Counter(r["verdict"] for r in resolutions)
    summary = {
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "audit_dir": str(audit_dir),
        "conflicts": len(resolutions),
        "verdicts": dict(sorted(verdicts.items())),
        "resolutions_csv": str(out_dir / "conflict_resolutions.csv"),
        "doctrine": "external witness > paper majority > held; escalations are the "
                    "review queue; NON_ATOM never promotes",
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n",
                                          encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
