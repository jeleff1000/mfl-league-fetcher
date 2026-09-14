"""Fetch source-backed fills for missing league-season settings.

This is deliberately an artifact-only lane.  It inventories individual
``(platform, db_name, year)`` rows from the canonical snapshot, fetches only
those rows from the platform source, and writes a keyed overlay.  It never
opens the canonical snapshot for writing and never creates a lineage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.canonical_settings import flatten_settings  # noqa: E402
from multi_league.data_fetchers.fleaflicker.fleaflicker_api_client import (  # noqa: E402
    FleaflickerAPIClient,
)
from multi_league.data_fetchers.fleaflicker.fleaflicker_context import (  # noqa: E402
    FleaflickerContext,
)
from multi_league.data_fetchers.mfl.mfl_api_client import MFLAPIClient  # noqa: E402
from multi_league.data_fetchers.mfl.mfl_context import MFLContext  # noqa: E402
from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient  # noqa: E402
from multi_league.data_fetchers.sleeper.sleeper_league_settings import (  # noqa: E402
    fetch_sleeper_settings,
)
from build_source_matchup_rescue_sidecar import mfl_target_candidates  # noqa: E402

FIELDS = (
    "scoring_pass_td",
    "playoff_teams",
    "roster_FLX",
    "roster_SUPER_FLEX",
    "roster_IDP",
    "sleeper_best_ball",
)
ROSTER_FIELDS = {"roster_FLX", "roster_SUPER_FLEX", "roster_IDP"}


def shard_for(db_name: str, year: int, shards: int) -> int:
    key = f"{db_name}|{year}".encode()
    return int(hashlib.sha256(key).hexdigest()[:12], 16) % shards


def _nonnull(value):
    return value is not None and str(value).strip() not in {"", "nan", "None"}


def inventory(snapshot: Path, platform: str, out: Path) -> dict:
    con = duckdb.connect(str(snapshot), read_only=True)
    try:
        sc = {r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()}
        required = {"db_name", "year", "platform", "league_key"} | set(FIELDS)
        missing = required - sc
        if missing:
            raise SystemExit(f"canonical settings schema missing columns: {sorted(missing)}")
        rows = con.execute(
            """
            SELECT CAST(db_name AS VARCHAR) db_name, CAST(year AS INTEGER) AS season_year,
                   LOWER(TRIM(CAST(platform AS VARCHAR))) platform,
                   CAST(league_key AS VARCHAR) source_id,
                   scoring_pass_td, playoff_teams, roster_FLX, roster_SUPER_FLEX,
                   roster_IDP, sleeper_best_ball
            FROM public.league_settings
            WHERE LOWER(TRIM(CAST(platform AS VARCHAR))) = ?
              AND EXISTS (
                SELECT 1 FROM public.player_fantasy p
                WHERE p.db_name=league_settings.db_name
                  AND CAST(p.year AS INTEGER)=CAST(league_settings.year AS INTEGER)
              )
            """,
            [platform],
        ).fetchdf()
    finally:
        con.close()
    targets = []
    for row in rows.to_dict("records"):
        missing_fields = [f for f in FIELDS if not _nonnull(row.get(f))]
        if not missing_fields:
            continue
        targets.append(
            {
                "db_name": row["db_name"],
                "year": int(row["season_year"]),
                "platform": row["platform"],
                "source_id": str(row.get("source_id") or "").strip(),
                "missing_fields": missing_fields,
            }
        )
    out.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "platform": platform,
        "population": "populated canonical league-seasons with missing settings",
        "target_count": len(targets),
        "targets": targets,
    }
    (out / "source_settings_targets.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    (out / "source_settings_summary.json").write_text(
        json.dumps({k: payload[k] for k in ("schema_version", "platform", "population", "target_count")}, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload


def _mfl_settings(row: dict) -> tuple[dict | None, str | None, str | None]:
    client = MFLAPIClient()
    source_id = row["source_id"]
    candidates = mfl_target_candidates(client, source_id, row["db_name"], int(row["year"]))
    if not candidates:
        return None, None, "missing_historical_source_id"
    for candidate in candidates:
        try:
            ctx = MFLContext(
                league_id=source_id,
                league_name=row["db_name"],
                start_year=int(row["year"]),
                end_year=int(row["year"]),
                league_ids={str(row["year"]): str(candidate)},
                data_directory=Path(".source_settings_context") / f"mfl_{candidate}_{row['year']}",
            )
            league = client.fetch_league(candidate, int(row["year"])) or {}
            rules = client.fetch_rules(candidate, int(row["year"])) or {}
            brackets = client.fetch_playoff_brackets(candidate, int(row["year"])) or {}
            flat = flatten_settings(
                {"league": league, "rules": rules, "brackets": brackets},
                platform="mfl", year=int(row["year"]), league_key=str(candidate),
            )
            if any(_nonnull(flat.get(f)) for f in row["missing_fields"]):
                return flat, str(candidate), None
        except Exception:
            continue
    return None, candidates[-1], "source_returned_no_missing_setting_values"


def _fleaflicker_settings(row: dict) -> tuple[dict | None, str | None, str | None]:
    client = FleaflickerAPIClient()
    source_id = row["source_id"]
    try:
        ctx = FleaflickerContext(
            league_id=source_id,
            league_name=row["db_name"],
            start_year=int(row["year"]),
            end_year=int(row["year"]),
            league_ids={str(row["year"]): source_id},
            data_directory=Path(".source_settings_context") / f"ff_{source_id}_{row['year']}",
        )
        from multi_league.data_fetchers.fleaflicker.fleaflicker_league_settings import fetch_fleaflicker_settings
        flat = fetch_fleaflicker_settings(ctx, int(row["year"]), client=client)
        return (flat or None), source_id, None if any(_nonnull(flat.get(f)) for f in row["missing_fields"]) else "source_returned_no_missing_setting_values"
    except Exception as exc:
        return None, source_id, f"source_fetch_failed:{type(exc).__name__}"


def _sleeper_settings(row: dict) -> tuple[dict | None, str | None, str | None]:
    try:
        flat = fetch_sleeper_settings(SleeperAPIClient(), row["source_id"], int(row["year"]))
        return (flat or None), row["source_id"], None if any(_nonnull(flat.get(f)) for f in row["missing_fields"]) else "source_returned_no_missing_setting_values"
    except Exception as exc:
        return None, row["source_id"], f"source_fetch_failed:{type(exc).__name__}"


def fetch_target(row: dict) -> tuple[dict, dict | None]:
    if not row["source_id"]:
        return {**row, "status": "missing_source_id", "resolved_fields": []}, None
    if row["platform"] == "mfl":
        flat, used, error = _mfl_settings(row)
    elif row["platform"] == "fleaflicker":
        flat, used, error = _fleaflicker_settings(row)
    elif row["platform"] == "sleeper":
        flat, used, error = _sleeper_settings(row)
    else:
        return {**row, "status": "unsupported_platform", "resolved_fields": []}, None
    fills = {f: flat.get(f) for f in row["missing_fields"]} if flat else {}
    fills = {f: v for f, v in fills.items() if _nonnull(v)}
    status = "source_resolved" if fills else (error or "source_returned_no_missing_setting_values")
    sidecar = None
    if fills:
        sidecar = {
            "db_name": row["db_name"], "year": row["year"], "platform": row["platform"],
            "source_id": row["source_id"], "source_id_used": used,
            **{f + "_fill": fills.get(f) for f in FIELDS},
            "source": "platform_settings_api",
        }
    return {**row, "source_id_used": used, "status": status, "resolved_fields": sorted(fills)}, sidecar


def run(args) -> None:
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    payload = json.loads(args.targets.read_text(encoding="utf-8"))
    targets = [r for r in payload["targets"] if shard_for(r["db_name"], int(r["year"]), args.shards) == args.shard]
    statuses, sidecars = [], []
    for row in targets:
        status, sidecar = fetch_target(row)
        statuses.append(status)
        if sidecar:
            sidecars.append(sidecar)
    columns = ["db_name", "year", "platform", "source_id", "source_id_used", *[f + "_fill" for f in FIELDS], "source"]
    frame = pd.DataFrame(sidecars, columns=columns)
    # Every shard, including an empty shard, must have the same physical
    # Parquet types so the artifacts can be combined by name without relying
    # on DuckDB's first-file schema inference.
    frame["year"] = pd.array(frame["year"], dtype="int64")
    frame["scoring_pass_td_fill"] = pd.array(frame["scoring_pass_td_fill"], dtype="float64")
    for col in ("playoff_teams_fill", "roster_FLX_fill", "roster_SUPER_FLEX_fill", "roster_IDP_fill"):
        frame[col] = pd.array(frame[col], dtype="Int64")
    frame["sleeper_best_ball_fill"] = pd.array(frame["sleeper_best_ball_fill"], dtype="boolean")
    frame.to_parquet(out / f"source_settings_{args.shard}.parquet", index=False)
    (out / f"source_settings_{args.shard}.jsonl").write_text("".join(json.dumps(x, sort_keys=True) + "\n" for x in statuses), encoding="utf-8")
    print(json.dumps({"shard": args.shard, "targets": len(targets), "sidecar_rows": len(sidecars), "statuses": {s: sum(x["status"] == s for x in statuses) for s in sorted({x["status"] for x in statuses})}}, sort_keys=True))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path)
    ap.add_argument("--targets", type=Path)
    ap.add_argument("--platform", choices=("fleaflicker", "mfl", "sleeper"))
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=256)
    args = ap.parse_args()
    if args.targets is None:
        if args.snapshot is None or not args.platform:
            ap.error("--snapshot and --platform are required when --targets is omitted")
        inventory(args.snapshot, args.platform, args.out)
    else:
        run(args)


if __name__ == "__main__":
    main()
