#!/usr/bin/env python3
"""
Classify PBP-vs-supertable identity reconciliation candidates.

This is an audit-only script. It reads the existing local PBP/supertable
comparison artifacts plus the repaired player bio parquet, then separates
"same person, duplicate IDs" from "same display name, different people."
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
DEFAULT_BIO_PATH = (
    ORGANIZED_ROOT / "nfl_historical_repair_artifacts_20260508" / "player_bio_stathead_pfr_repaired.parquet"
)
DEFAULT_OUTPUT_DIR = DEFAULT_AUDIT_DIR / "identity_truth_20260508"

ID_TOKEN_RE = re.compile(r"(?P<id>(?:[A-Z]{3}\d{6}|HIST-\d+|00-\d{7}|[a-z]{4,}[a-z0-9]{2}\d{2}))")
HIGHLIGHT_NAMES = {
    "Stanley Morgan",
    "Anthony Miller",
    "Mark Clayton",
    "Freeman McNeil",
    "J.T. Smith",
    "Kellen Winslow",
}


def clean_id(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def clean_year(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        year = int(float(value))
    except (TypeError, ValueError):
        return None
    if year <= 0:
        return None
    return year


def clean_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_date(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    return text[:10]


def norm_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def parse_id_list(value: Any) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    ids: list[str] = []
    for match in ID_TOKEN_RE.finditer(text):
        player_id = match.group("id")
        if player_id not in ids:
            ids.append(player_id)
    return ids


def year_range_text(first: Any, last: Any) -> str:
    first_year = clean_year(first)
    last_year = clean_year(last)
    if first_year is None and last_year is None:
        return ""
    if first_year == last_year:
        return str(first_year)
    return f"{first_year or '?'}-{last_year or '?'}"


def span_overlaps(a_first: int | None, a_last: int | None, b_first: int | None, b_last: int | None) -> bool:
    if a_first is None or a_last is None or b_first is None or b_last is None:
        return False
    return max(a_first, b_first) <= min(a_last, b_last)


def span_close(
    a_first: int | None, a_last: int | None, b_first: int | None, b_last: int | None, tolerance: int = 2
) -> bool:
    if a_first is None or a_last is None or b_first is None or b_last is None:
        return False
    return abs(a_first - b_first) <= tolerance and abs(a_last - b_last) <= tolerance


def observed_outside_rookie(profile: dict[str, Any], source_prefix: str) -> str:
    rookie_year = clean_year(profile.get("rookie_year"))
    first = clean_year(profile.get(f"{source_prefix}_first_year"))
    last = clean_year(profile.get(f"{source_prefix}_last_year"))
    if rookie_year is None or first is None:
        return ""
    if first < rookie_year - 1:
        return f"{source_prefix}_starts_before_rookie({first}<{rookie_year})"
    if last is not None and last < rookie_year - 1:
        return f"{source_prefix}_ends_before_rookie({last}<{rookie_year})"
    return ""


def load_bio(path: Path) -> pd.DataFrame:
    bio = pd.read_parquet(path)
    if "position" not in bio.columns and "nfl_position" in bio.columns:
        bio["position"] = bio["nfl_position"]
    keep = [
        "NFL_player_id",
        "player",
        "position",
        "latest_team",
        "birth_date",
        "college",
        "draft_year",
        "draft_round",
        "draft_overall",
        "rookie_year",
        "first_year",
        "last_year",
        "pfr_id",
        "espn_id",
    ]
    keep = [col for col in keep if col in bio.columns]
    out = bio[keep].copy()
    out["NFL_player_id"] = out["NFL_player_id"].map(clean_id)
    out["player"] = out["player"].map(clean_text)
    out["position"] = out.get("position", "").map(clean_text)
    out["birth_date"] = out.get("birth_date", "").map(clean_date)
    out["pfr_id"] = out.get("pfr_id", "").map(clean_text)
    out["college"] = out.get("college", "").map(clean_text)
    out = out[out["NFL_player_id"].ne("")]
    return out.drop_duplicates("NFL_player_id", keep="first")


def load_summary(path: Path) -> pd.DataFrame:
    summary = pd.read_csv(path)
    for col in ["player", "NFL_player_id", "position", "source"]:
        if col in summary.columns:
            summary[col] = summary[col].map(clean_text)
    stat_cols = [
        col
        for col in summary.columns
        if col
        not in {
            "player",
            "NFL_player_id",
            "position",
            "source",
            "first_year",
            "last_year",
            "weekly_rows",
            "_activity",
        }
    ]
    for col in stat_cols + ["weekly_rows", "_activity"]:
        if col in summary.columns:
            summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0.0)
    rows: list[dict[str, Any]] = []
    for (player, player_id), group in summary.groupby(["player", "NFL_player_id"], dropna=False):
        row: dict[str, Any] = {"player": player, "NFL_player_id": player_id}
        positions = [clean_text(v) for v in group["position"].dropna().tolist() if clean_text(v)]
        row["observed_position"] = "/".join(dict.fromkeys(positions))
        for source in ["pbp", "super"]:
            part = group[group["source"].eq(source)]
            if part.empty:
                continue
            row[f"{source}_first_year"] = pd.to_numeric(part["first_year"], errors="coerce").min()
            row[f"{source}_last_year"] = pd.to_numeric(part["last_year"], errors="coerce").max()
            row[f"{source}_weekly_rows"] = pd.to_numeric(part["weekly_rows"], errors="coerce").sum()
            row[f"{source}_activity"] = pd.to_numeric(part.get("_activity", 0), errors="coerce").sum()
        row["observed_first_year"] = min(
            [v for v in [clean_year(row.get("pbp_first_year")), clean_year(row.get("super_first_year"))] if v],
            default=None,
        )
        row["observed_last_year"] = max(
            [v for v in [clean_year(row.get("pbp_last_year")), clean_year(row.get("super_last_year"))] if v],
            default=None,
        )
        rows.append(row)
    return pd.DataFrame(rows)


def build_profiles(bio: pd.DataFrame, summary: pd.DataFrame) -> dict[tuple[str, str], dict[str, Any]]:
    merged = summary.merge(bio, on="NFL_player_id", how="outer", suffixes=("_summary", "_bio"))
    merged["player"] = merged["player_summary"].where(
        merged.get("player_summary", "").fillna("").astype(str).str.strip().ne(""),
        merged.get("player_bio", ""),
    )
    merged["position"] = merged["observed_position"].where(
        merged.get("observed_position", "").fillna("").astype(str).str.strip().ne(""),
        merged.get("position", ""),
    )
    profiles: dict[tuple[str, str], dict[str, Any]] = {}
    for row in merged.to_dict("records"):
        player = clean_text(row.get("player"))
        player_id = clean_id(row.get("NFL_player_id"))
        if not player or not player_id:
            continue
        profiles[(player, player_id)] = row
    return profiles


def profile_for(profiles: dict[tuple[str, str], dict[str, Any]], player: str, player_id: str) -> dict[str, Any]:
    direct = profiles.get((player, player_id))
    if direct is not None:
        return direct
    player_norm = norm_key(player)
    for (candidate_player, candidate_id), profile in profiles.items():
        if candidate_id == player_id and norm_key(candidate_player) == player_norm:
            return profile
    for (_candidate_player, candidate_id), profile in profiles.items():
        if candidate_id == player_id:
            return profile
    return {"player": player, "NFL_player_id": player_id}


def profile_text(profile: dict[str, Any], source: str | None = None) -> str:
    pieces = []
    player_id = clean_id(profile.get("NFL_player_id"))
    position = clean_text(profile.get("position") or profile.get("observed_position"))
    if player_id:
        pieces.append(player_id)
    if position:
        pieces.append(position)
    pfr_id = clean_text(profile.get("pfr_id"))
    if pfr_id:
        pieces.append(f"pfr={pfr_id}")
    birth_date = clean_date(profile.get("birth_date"))
    if birth_date:
        pieces.append(f"born={birth_date}")
    rookie_year = clean_year(profile.get("rookie_year"))
    if rookie_year:
        pieces.append(f"rookie={rookie_year}")
    college = clean_text(profile.get("college"))
    if college:
        pieces.append(f"college={college}")
    if source:
        observed = year_range_text(profile.get(f"{source}_first_year"), profile.get(f"{source}_last_year"))
        if observed:
            pieces.append(f"{source}={observed}")
    return "; ".join(pieces)


def bio_relationship(a: dict[str, Any], b: dict[str, Any]) -> tuple[str, str, str]:
    a_pfr = norm_key(a.get("pfr_id"))
    b_pfr = norm_key(b.get("pfr_id"))
    a_birth = clean_date(a.get("birth_date"))
    b_birth = clean_date(b.get("birth_date"))
    a_college = norm_key(a.get("college"))
    b_college = norm_key(b.get("college"))
    a_id = clean_id(a.get("NFL_player_id"))
    b_id = clean_id(b.get("NFL_player_id"))

    if a_pfr and b_pfr and a_pfr != b_pfr:
        return "same_name_different_person", "hard", f"different PFR IDs ({a.get('pfr_id')} vs {b.get('pfr_id')})"
    if a_birth and b_birth and a_birth != b_birth:
        return "same_name_different_person", "hard", f"different birth dates ({a_birth} vs {b_birth})"
    if a_pfr and b_pfr and a_pfr == b_pfr:
        return "same_person_duplicate_id", "hard", f"same PFR ID ({a.get('pfr_id')})"
    if a_birth and b_birth and a_birth == b_birth:
        return "same_person_duplicate_id", "hard", f"same birth date ({a_birth})"

    a_obs_first = clean_year(a.get("observed_first_year"))
    a_obs_last = clean_year(a.get("observed_last_year"))
    b_obs_first = clean_year(b.get("observed_first_year"))
    b_obs_last = clean_year(b.get("observed_last_year"))
    a_rookie = clean_year(a.get("rookie_year"))
    b_rookie = clean_year(b.get("rookie_year"))

    one_hist = a_id.startswith("HIST-") ^ b_id.startswith("HIST-")
    one_pfrish = bool(a_pfr or b_pfr)
    same_or_unknown_college = not a_college or not b_college or a_college == b_college
    if (
        one_hist
        and one_pfrish
        and same_or_unknown_college
        and (
            span_overlaps(a_obs_first, a_obs_last, b_obs_first, b_obs_last)
            or span_close(a_obs_first, a_obs_last, b_obs_first, b_obs_last)
        )
    ):
        return (
            "same_person_duplicate_id",
            "probable",
            "HIST/PFR-style duplicate with overlapping observed career window",
        )

    if a_rookie and b_rookie and abs(a_rookie - b_rookie) >= 6 and (a_pfr or b_pfr or a_birth or b_birth):
        return "same_name_different_person", "probable", f"rookie years are far apart ({a_rookie} vs {b_rookie})"

    return "uncertain_do_not_touch", "review", "insufficient hard bio evidence"


def pair_action(
    classification: str,
    confidence: str,
    super_profile: dict[str, Any],
    pbp_profile: dict[str, Any],
) -> tuple[str, str]:
    flags = [
        flag
        for flag in [
            observed_outside_rookie(super_profile, "super"),
            observed_outside_rookie(pbp_profile, "pbp"),
        ]
        if flag
    ]
    if classification == "same_name_different_person":
        action = "do_not_bridge; split/repair only rows that fall in the wrong person's era"
        if flags:
            action = "do_not_bridge; repair contaminated historical rows away from the wrong same-name ID"
        return action, "; ".join(flags)
    if classification == "same_person_duplicate_id":
        preferred = choose_canonical_id(super_profile, pbp_profile)
        return f"bridge_duplicate_ids_to={preferred}", "; ".join(flags)
    return "manual_review_before_any_bridge", "; ".join(flags)


def repair_complexity(
    classification: str,
    super_profile: dict[str, Any],
    pbp_profile: dict[str, Any],
    flags: str,
) -> str:
    if classification == "same_person_duplicate_id":
        return "duplicate_id_bridge"
    if classification != "same_name_different_person":
        return "manual_review"
    if "super_starts_before_rookie" not in flags:
        return "do_not_bridge_no_era_repair_signal"

    super_rookie = clean_year(super_profile.get("rookie_year"))
    pbp_last = clean_year(pbp_profile.get("pbp_last_year")) or clean_year(pbp_profile.get("last_year"))
    pbp_first = clean_year(pbp_profile.get("pbp_first_year")) or clean_year(pbp_profile.get("first_year"))
    if super_rookie is not None and pbp_last is not None and pbp_last < super_rookie:
        return "simple_pre_rookie_id_reassignment"
    if super_rookie is not None and pbp_first is not None and pbp_first < super_rookie:
        return "overlapping_or_partial_row_level_split"
    return "era_flag_but_insufficient_window"


def choose_canonical_id(a: dict[str, Any], b: dict[str, Any]) -> str:
    candidates = [a, b]
    scored: list[tuple[int, str]] = []
    for profile in candidates:
        player_id = clean_id(profile.get("NFL_player_id"))
        score = 0
        if clean_text(profile.get("pfr_id")):
            score += 100
        if not player_id.startswith("HIST-"):
            score += 50
        if player_id.startswith("00-"):
            score += 20
        if clean_date(profile.get("birth_date")):
            score += 10
        scored.append((score, player_id))
    scored.sort(reverse=True)
    return scored[0][1]


def build_pair_candidates(audit_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    bridge_path = audit_dir / "weekly_player_id_bridge_candidates.csv"
    if bridge_path.exists():
        bridge = pd.read_csv(bridge_path)
        for row in bridge.to_dict("records"):
            rows.append(
                {
                    "evidence_source": "weekly_bridge",
                    "player": clean_text(row.get("player")),
                    "position": clean_text(row.get("position")),
                    "super_NFL_player_id": clean_id(row.get("super_NFL_player_id")),
                    "pbp_NFL_player_id": clean_id(row.get("pbp_NFL_player_id")),
                    "paired_weeks": row.get("paired_weeks"),
                    "min_year": row.get("min_year"),
                    "max_year": row.get("max_year"),
                    "bridge_confidence": row.get("bridge_confidence"),
                    "stat_families": "",
                    "stats": "",
                    "evidence_rows": 1,
                }
            )

    collision_path = audit_dir / "same_name_stat_collision_suspects.csv"
    if collision_path.exists():
        collisions = pd.read_csv(collision_path)
        for row in collisions.to_dict("records"):
            player = clean_text(row.get("player"))
            current_id = clean_id(row.get("NFL_player_id"))
            direction = clean_text(row.get("direction"))
            if direction == "super_stat_on_id_but_pbp_stat_on_sibling":
                sibling_ids = parse_id_list(row.get("sibling_pbp_ids"))
                for sibling_id in sibling_ids:
                    rows.append(
                        {
                            "evidence_source": "stat_collision",
                            "player": player,
                            "position": clean_text(row.get("position")),
                            "super_NFL_player_id": current_id,
                            "pbp_NFL_player_id": sibling_id,
                            "paired_weeks": "",
                            "min_year": row.get("first_year"),
                            "max_year": row.get("last_year"),
                            "bridge_confidence": "",
                            "stat_families": clean_text(row.get("stat_family")),
                            "stats": clean_text(row.get("stat")),
                            "evidence_rows": 1,
                        }
                    )
            elif direction == "pbp_stat_on_id_but_super_stat_on_sibling":
                sibling_ids = parse_id_list(row.get("sibling_super_ids"))
                for sibling_id in sibling_ids:
                    rows.append(
                        {
                            "evidence_source": "stat_collision",
                            "player": player,
                            "position": clean_text(row.get("position")),
                            "super_NFL_player_id": sibling_id,
                            "pbp_NFL_player_id": current_id,
                            "paired_weeks": "",
                            "min_year": row.get("first_year"),
                            "max_year": row.get("last_year"),
                            "bridge_confidence": "",
                            "stat_families": clean_text(row.get("stat_family")),
                            "stats": clean_text(row.get("stat")),
                            "evidence_rows": 1,
                        }
                    )

    candidates = pd.DataFrame(rows)
    if candidates.empty:
        return candidates
    candidates = candidates[
        candidates["player"].ne("")
        & candidates["super_NFL_player_id"].ne("")
        & candidates["pbp_NFL_player_id"].ne("")
        & candidates["super_NFL_player_id"].ne(candidates["pbp_NFL_player_id"])
    ].copy()
    grouped = (
        candidates.groupby(["player", "super_NFL_player_id", "pbp_NFL_player_id"], dropna=False)
        .agg(
            position=("position", lambda s: "/".join(dict.fromkeys([clean_text(v) for v in s if clean_text(v)]))),
            evidence_sources=("evidence_source", lambda s: ";".join(sorted(set(map(str, s))))),
            evidence_rows=("evidence_rows", "sum"),
            paired_weeks=("paired_weeks", lambda s: max([clean_number(v) or 0 for v in s], default=0)),
            min_year=("min_year", lambda s: min([clean_year(v) for v in s if clean_year(v)], default=None)),
            max_year=("max_year", lambda s: max([clean_year(v) for v in s if clean_year(v)], default=None)),
            bridge_confidence=(
                "bridge_confidence",
                lambda s: ";".join(sorted({clean_text(v) for v in s if clean_text(v)})),
            ),
            stat_families=("stat_families", lambda s: ";".join(sorted({clean_text(v) for v in s if clean_text(v)}))),
            stats=("stats", lambda s: ";".join(sorted({clean_text(v) for v in s if clean_text(v)}))),
        )
        .reset_index()
    )
    return grouped


def classify_pairs(pairs: pd.DataFrame, profiles: dict[tuple[str, str], dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in pairs.to_dict("records"):
        player = clean_text(row.get("player"))
        super_id = clean_id(row.get("super_NFL_player_id"))
        pbp_id = clean_id(row.get("pbp_NFL_player_id"))
        super_profile = profile_for(profiles, player, super_id)
        pbp_profile = profile_for(profiles, player, pbp_id)
        classification, confidence, reason = bio_relationship(super_profile, pbp_profile)
        action, flags = pair_action(classification, confidence, super_profile, pbp_profile)
        complexity = repair_complexity(classification, super_profile, pbp_profile, flags)
        rows.append(
            {
                **row,
                "truth_classification": classification,
                "truth_confidence": confidence,
                "truth_reason": reason,
                "recommended_action": action,
                "era_flags": flags,
                "repair_complexity": complexity,
                "super_profile": profile_text(super_profile, "super"),
                "pbp_profile": profile_text(pbp_profile, "pbp"),
                "is_highlight_name": player in HIGHLIGHT_NAMES,
            }
        )
    out = pd.DataFrame(rows)
    order = {
        "same_name_different_person": 0,
        "same_person_duplicate_id": 1,
        "uncertain_do_not_touch": 2,
    }
    confidence_order = {"hard": 0, "probable": 1, "review": 2}
    out["_class_order"] = out["truth_classification"].map(order).fillna(9)
    out["_confidence_order"] = out["truth_confidence"].map(confidence_order).fillna(9)
    out["_highlight_order"] = out["is_highlight_name"].map({True: 0, False: 1})
    out = out.sort_values(
        ["_highlight_order", "_class_order", "_confidence_order", "evidence_rows", "player"],
        ascending=[True, True, True, False, True],
    ).drop(columns=["_class_order", "_confidence_order", "_highlight_order"])
    return out


def write_notes(output_dir: Path, classified: pd.DataFrame) -> None:
    counts = (
        classified.groupby(["truth_classification", "truth_confidence"], dropna=False)
        .size()
        .reset_index(name="pairs")
        .sort_values(["truth_classification", "truth_confidence"])
    )
    highlight = classified[classified["is_highlight_name"]].copy()
    do_not_bridge = classified[classified["truth_classification"].eq("same_name_different_person")].head(30)
    bridges = classified[classified["truth_classification"].eq("same_person_duplicate_id")].head(40)
    uncertain = classified[classified["truth_classification"].eq("uncertain_do_not_touch")].head(30)
    repair_candidates = classified[
        classified["truth_classification"].eq("same_name_different_person")
        & classified["truth_confidence"].eq("hard")
        & classified["era_flags"].fillna("").str.contains("super_starts_before_rookie", regex=False)
    ].head(40)

    lines = [
        "# Identity Truth Classification",
        "",
        "Audit-only classification of PBP/supertable ID reconciliation candidates.",
        "",
        "Rules used:",
        "- Different PFR IDs or different birth dates are hard same-name/different-person evidence.",
        "- Same PFR ID or same birth date is hard duplicate-ID evidence.",
        "- HIST/no-PFR IDs with overlapping observed windows against a PFR-style ID are probable duplicate IDs, not automatically live repairs.",
        "- Any pair with insufficient bio evidence remains manual review.",
        "",
        "## Counts",
        "",
        counts.to_csv(index=False),
        "",
        "## Repair Complexity Counts",
        "",
        classified.groupby("repair_complexity", dropna=False)
        .size()
        .reset_index(name="pairs")
        .sort_values("pairs", ascending=False)
        .to_csv(index=False),
        "",
        "## User-Flagged Names",
        "",
        highlight[
            [
                "player",
                "super_NFL_player_id",
                "pbp_NFL_player_id",
                "truth_classification",
                "truth_confidence",
                "truth_reason",
                "recommended_action",
                "era_flags",
                "repair_complexity",
                "super_profile",
                "pbp_profile",
            ]
        ].to_csv(index=False)
        if not highlight.empty
        else "None in current candidate set.",
        "",
        "## Do Not Bridge Examples",
        "",
        do_not_bridge[
            [
                "player",
                "super_NFL_player_id",
                "pbp_NFL_player_id",
                "truth_confidence",
                "truth_reason",
                "recommended_action",
                "era_flags",
                "repair_complexity",
                "super_profile",
                "pbp_profile",
            ]
        ].to_csv(index=False)
        if not do_not_bridge.empty
        else "None found.",
        "",
        "## High-Confidence Supertable Contamination Candidates",
        "",
        repair_candidates[
            [
                "player",
                "super_NFL_player_id",
                "pbp_NFL_player_id",
                "truth_reason",
                "era_flags",
                "repair_complexity",
                "super_profile",
                "pbp_profile",
            ]
        ].to_csv(index=False)
        if not repair_candidates.empty
        else "None found.",
        "",
        "## Duplicate-ID Bridge Examples",
        "",
        bridges[
            [
                "player",
                "super_NFL_player_id",
                "pbp_NFL_player_id",
                "truth_confidence",
                "truth_reason",
                "recommended_action",
                "repair_complexity",
                "super_profile",
                "pbp_profile",
            ]
        ].to_csv(index=False)
        if not bridges.empty
        else "None found.",
        "",
        "## Manual Review Examples",
        "",
        uncertain[
            [
                "player",
                "super_NFL_player_id",
                "pbp_NFL_player_id",
                "truth_reason",
                "recommended_action",
                "repair_complexity",
                "super_profile",
                "pbp_profile",
            ]
        ].to_csv(index=False)
        if not uncertain.empty
        else "None found.",
        "",
    ]
    (output_dir / "IDENTITY_TRUTH_NOTES.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--bio-path", type=Path, default=DEFAULT_BIO_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    bio = load_bio(args.bio_path)
    summary = load_summary(args.audit_dir / "same_name_multi_id_summary.csv")
    profiles = build_profiles(bio, summary)
    pairs = build_pair_candidates(args.audit_dir)
    classified = classify_pairs(pairs, profiles)

    pairs.to_csv(args.output_dir / "identity_pair_candidates.csv", index=False)
    classified.to_csv(args.output_dir / "identity_pair_truth_classification.csv", index=False)
    classified[
        classified["truth_classification"].eq("same_name_different_person")
        & classified["truth_confidence"].eq("hard")
        & classified["era_flags"].fillna("").str.contains("super_starts_before_rookie", regex=False)
    ].to_csv(args.output_dir / "super_identity_contamination_repair_candidates.csv", index=False)
    classified[classified["repair_complexity"].eq("simple_pre_rookie_id_reassignment")].to_csv(
        args.output_dir / "simple_identity_reassignment_candidates.csv", index=False
    )
    classified[classified["repair_complexity"].eq("overlapping_or_partial_row_level_split")].to_csv(
        args.output_dir / "overlapping_identity_split_review.csv", index=False
    )
    classified[classified["truth_classification"].eq("same_person_duplicate_id")].to_csv(
        args.output_dir / "duplicate_id_bridge_candidates.csv", index=False
    )
    classified[classified["truth_classification"].eq("uncertain_do_not_touch")].to_csv(
        args.output_dir / "manual_review_identity_pairs.csv", index=False
    )
    (
        classified.groupby(["truth_classification", "truth_confidence"], dropna=False)
        .size()
        .reset_index(name="pairs")
        .to_csv(args.output_dir / "identity_pair_truth_summary.csv", index=False)
    )
    write_notes(args.output_dir, classified)

    print(f"pair_candidates={len(pairs):,}")
    print(f"classified_pairs={len(classified):,}")
    print(
        classified.groupby(["truth_classification", "truth_confidence"], dropna=False)
        .size()
        .reset_index(name="pairs")
        .to_string(index=False)
    )
    print(args.output_dir / "IDENTITY_TRUTH_NOTES.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
