"""Discover MFL season-scoped IDs from one deterministic numeric shard.

The workflow runs 256 of these shards with a maximum of 15 concurrent runners.
Each runner stays below MFL's per-IP throttle, writes an independent JSON artifact,
and never mutates the shared seed. A merge step combines artifacts by (year, id).

Older years are scanned first. A valid seed response also contributes its explicit
history links, so an MFL database named for one year is represented as a linked
multi-year lineage rather than as unrelated databases.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


# Older MFL databases occupy the low-ID namespace too; keep it as its own
# stratum so dense modern ranges cannot crowd it out.
DEFAULT_BANDS = "1-9999,10000-29999,30000-59999,60000-80000"
HISTORY_RE = re.compile(r"/(\d{4})/(?:home|export)/([^/?#]+)")


def _log(message: str) -> None:
    print(f"[MFL discovery] {dt.datetime.now(dt.timezone.utc).isoformat()} {message}", flush=True)


def parse_id_bands(spec: str) -> list[tuple[int, int]]:
    bands = []
    for raw in spec.split(","):
        if not raw.strip():
            continue
        lo, hi = (int(part.strip()) for part in raw.split("-", 1))
        if lo < 0 or hi < lo:
            raise ValueError(f"invalid ID band: {raw!r}")
        bands.append((lo, hi))
    return bands


def shard_ids(
    bands: list[tuple[int, int]], *, shard_index: int, shard_count: int
) -> list[int]:
    """Partition all inclusive ID bands without overlap or omission."""
    if not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must be within shard_count")
    values = []
    ordinal = 0
    for lo, hi in bands:
        for value in range(lo, hi + 1):
            if ordinal % shard_count == shard_index:
                values.append(value)
            ordinal += 1
    return values


def sampled_ids(bands: list[tuple[int, int]], *, sample_count: int) -> list[int]:
    """Select evenly spaced IDs across the configured strata."""
    if sample_count <= 0:
        return []
    widths = [hi - lo + 1 for lo, hi in bands]
    total = sum(widths)
    if not total:
        return []
    exact = [sample_count * width / total for width in widths]
    counts = [min(int(value), width) for value, width in zip(exact, widths)]
    remaining = sample_count - sum(counts)
    order = sorted(
        range(len(bands)),
        key=lambda index: (exact[index] - int(exact[index]), widths[index]),
        reverse=True,
    )
    for index in order:
        if remaining <= 0:
            break
        if counts[index] < widths[index]:
            counts[index] += 1
            remaining -= 1
    values: list[int] = []
    for (lo, hi), count in zip(bands, counts):
        width = hi - lo + 1
        for offset in range(count):
            values.append(lo + min(width - 1, ((2 * offset + 1) * width) // (2 * count)))
    return sorted(set(values))


def sampled_shard_ids(
    bands: list[tuple[int, int]], *, shard_index: int, shard_count: int, ids_per_shard: int
) -> list[int]:
    """Return a deterministic small sample for one shard."""
    values = sampled_ids(bands, sample_count=shard_count * ids_per_shard)
    return values[shard_index * ids_per_shard : (shard_index + 1) * ids_per_shard]


def contiguous_shard_ids(
    id_start: int, id_end: int, *, shard_index: int, shard_count: int
) -> list[int]:
    """Partition one inclusive numeric range into contiguous, balanced shards."""
    if id_start < 0 or id_end < id_start:
        raise ValueError("invalid contiguous ID range")
    if not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must be within shard_count")
    total = id_end - id_start + 1
    base, remainder = divmod(total, shard_count)
    offset = shard_index * base + min(shard_index, remainder)
    size = base + (1 if shard_index < remainder else 0)
    return list(range(id_start + offset, id_start + offset + size))


def task_manifest_shard_ids(path: Path, window_index: int, shard_index: int, shard_count: int) -> dict[int, list[int]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    windows = data.get("windows") or []
    if not 0 <= window_index < len(windows):
        raise ValueError(f"task window {window_index} outside 0..{len(windows) - 1}")
    by_year: dict[int, list[int]] = {}
    ordinal = 0
    for task in windows[window_index].get("tasks", []):
        year = int(task["year"])
        for league_id in range(int(task["id_start"]), int(task["id_end"]) + 1):
            if ordinal % shard_count == shard_index:
                by_year.setdefault(year, []).append(league_id)
            ordinal += 1
    return by_year


def _history_entries(payload: dict) -> list[tuple[int, str]]:
    history = ((payload or {}).get("history") or {}).get("league") or []
    if isinstance(history, dict):
        history = [history]
    result = []
    for item in history:
        if not isinstance(item, dict):
            continue
        raw_year = item.get("year")
        url = str(item.get("url") or "")
        match = HISTORY_RE.search(url)
        year = int(raw_year) if str(raw_year).isdigit() else (int(match.group(1)) if match else None)
        league_id = match.group(2) if match else str(item.get("id") or "")
        if year is not None and league_id:
            result.append((year, league_id))
    return sorted(set(result))


def history_records(seed_year: int, seed_id: str, payload: dict, name: str) -> list[dict]:
    """Return minimal lineage rows, retaining the seed and every explicit history link."""
    seed = (int(seed_year), str(seed_id))
    pairs = set(_history_entries(payload))
    pairs.add(seed)
    return [
        {
            "year": year,
            "id": league_id,
            "lineage_seed": f"{seed_year}:{seed_id}",
            "link_type": "seed" if (year, league_id) == seed else "history",
        }
        for year, league_id in sorted(pairs)
    ]


def _norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _franchise_names(payload: dict) -> list[str]:
    values = ((payload or {}).get("franchises") or {}).get("franchise") or []
    if isinstance(values, dict):
        values = [values]
    return sorted({_norm(item.get("name")) for item in values if isinstance(item, dict) and item.get("name")})


def metadata(payload: dict) -> dict:
    league = (payload or {}).get("league") or {}
    roster_names = _franchise_names(league)
    roster_blob = "|".join(roster_names).encode("utf-8")
    return {
        "name": league.get("name") or "",
        "name_key": _norm(league.get("name")),
        # MFL intentionally withholds personal owner data from unauthenticated
        # exports. Keep this explicit so downstream lineage audits do not confuse
        # a public-privacy boundary with a failed owner-graph crawl.
        "owner_data_status": "restricted_without_credentials",
        "roster_names": roster_names,
        "roster_fingerprint": hashlib.sha1(roster_blob).hexdigest() if roster_names else None,
        "size": len(roster_names),
        "draft_kind": league.get("draft_kind"),
        "auction_kind": league.get("auction_kind"),
        "history_years": [year for year, _ in _history_entries(league)],
    }


def _has_draft_payload(payload: dict) -> bool:
    draft = (payload or {}).get("draftResults") or {}
    # MFL returns draftUnit as either one object or a list of objects.  Some
    # leagues also return draftResults itself as a list-shaped wrapper.  Treat
    # both forms identically instead of assuming every unit is a dict.
    if isinstance(draft, list):
        units = draft
    elif isinstance(draft, dict):
        units = draft.get("draftUnit") or []
    else:
        units = []
    if isinstance(units, dict):
        units = [units]
    picks = []
    for unit in units:
        if not isinstance(unit, dict):
            continue
        unit_picks = unit.get("draftPick") or []
        if isinstance(unit_picks, dict):
            unit_picks = [unit_picks]
        if isinstance(unit_picks, list):
            picks.extend(unit_picks)
    auction = (payload or {}).get("auctionResults") or {}
    return bool(picks or auction)


class ShardClient:
    def __init__(self, sleep_seconds: float, timeout: float, max_errors: int):
        self.sleep_seconds = sleep_seconds
        self.timeout = timeout
        self.max_errors = max_errors
        self.errors = 0
        self.requests = 0
        self.throttles = 0
        self.consecutive_throttles = 0
        self.rate_limit_open = False

    def get(self, year: int, export_type: str, **params):
        if self.rate_limit_open:
            return None
        query = {"TYPE": export_type, "JSON": "1", **params}
        url = f"https://api.myfantasyleague.com/{year}/export?{urllib.parse.urlencode(query)}"
        for attempt in range(3):
            self.requests += 1
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "LeagueHistoryImport/1.0 (MFL shard)"})
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8-sig", "replace"))
                self.errors = 0
                self.consecutive_throttles = 0
                time.sleep(self.sleep_seconds)
                return payload
            except urllib.error.HTTPError as exc:
                if exc.code == 429:
                    self.throttles += 1
                    self.consecutive_throttles += 1
                    if self.consecutive_throttles >= 3:
                        self.rate_limit_open = True
                        _log(
                            f"429 circuit open year={year} type={export_type}; "
                            f"consecutive_429s={self.consecutive_throttles}"
                        )
                        break
                    delay = max(120.0, self.sleep_seconds * 30)
                    _log(f"429 year={year} type={export_type} attempt={attempt + 1}/3; sleeping={delay:.0f}s")
                    time.sleep(delay)
                else:
                    _log(f"HTTP {exc.code} year={year} type={export_type}; skipping")
                    break
            except Exception as exc:
                if attempt == 2:
                    _log(f"network error year={year} type={export_type}: {exc}")
                if attempt < 2:
                    time.sleep(5.0)
        self.errors += 1
        time.sleep(self.sleep_seconds)
        if self.errors >= self.max_errors:
            raise RuntimeError(f"MFL network error limit reached ({self.errors})")
        return None


def _result(
    args: argparse.Namespace,
    records: dict[tuple[int, str], dict],
    client: ShardClient,
    seeds_checked: int,
    seeds_with_draft: int,
    *,
    completed_through_year: int | None,
    complete: bool,
) -> dict:
    return {
        "platform": "mfl",
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "id_bands": args.id_bands,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "ids_per_shard": args.ids_per_shard,
        "id_start": args.id_start,
        "id_end": args.id_end,
        "completed_through_year": completed_through_year,
        "complete": complete,
        "requests": client.requests,
        "throttles": client.throttles,
        "seeds_checked": seeds_checked,
        "seeds_with_draft": seeds_with_draft,
        "records": [records[key] for key in sorted(records)],
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def _write_snapshot(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def run(args: argparse.Namespace) -> dict:
    bands = parse_id_bands(args.id_bands)
    manifest_mode = args.task_manifest is not None
    if manifest_mode:
        ids_by_year = task_manifest_shard_ids(
            args.task_manifest, args.task_window, args.shard_index, args.shard_count
        )
        ids = sorted({value for values in ids_by_year.values() for value in values})
        total_ids = sum(len(values) for values in ids_by_year.values())
        sampling_mode = "task_manifest"
    elif args.id_start is not None and args.id_end is not None:
        ids = contiguous_shard_ids(
            args.id_start,
            args.id_end,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
        )
        sampling_mode = "contiguous"
        total_ids = args.id_end - args.id_start + 1
        ids_by_year = {year: ids for year in range(args.start_year, args.end_year + 1)}
    else:
        ids = sampled_shard_ids(
            bands,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
            ids_per_shard=args.ids_per_shard,
        )
        sampling_mode = "stratified_sample"
        total_ids = args.shard_count * args.ids_per_shard
        ids_by_year = {year: ids for year in range(args.start_year, args.end_year + 1)}
    client = ShardClient(args.sleep, args.timeout, args.max_errors)
    records: dict[tuple[int, str], dict] = {}
    seeds_checked = 0
    seeds_with_draft = 0
    _write_snapshot(
        args.out,
        _result(
            args,
            records,
            client,
            seeds_checked,
            seeds_with_draft,
            completed_through_year=None,
            complete=False,
        ),
    )
    total_probes = sum(len(values) for values in ids_by_year.values())
    completed_probes = 0
    started = time.monotonic()
    _log(
        f"shard={args.shard_index}/{args.shard_count} start years={args.start_year}-{args.end_year} "
        f"mode={sampling_mode} ids={len(ids)} total_ids={total_ids} "
        f"probes={total_probes} sleep={args.sleep}s"
    )
    # Ascending years deliberately puts the scarce historical eras first.
    last_completed_year = None
    for year in range(args.start_year, args.end_year + 1):
        year_started = time.monotonic()
        year_valid_before = seeds_checked
        year_draft_before = seeds_with_draft
        year_ids = ids_by_year.get(year, [])
        for position, league_id in enumerate(year_ids, start=1):
            payload = client.get(year, "league", L=str(league_id))
            completed_probes += 1
            if client.rate_limit_open:
                _log(f"shard={args.shard_index} stopping after rate-limit circuit opened")
                break
            league = (payload or {}).get("league") if isinstance(payload, dict) else None
            if league and league.get("franchises"):
                seeds_checked += 1
                info = metadata(payload)
                draft_payload = client.get(year, "draftResults", L=str(league_id))
                if client.rate_limit_open:
                    _log(f"shard={args.shard_index} stopping after rate-limit circuit opened")
                    break
                draft_status = "available" if _has_draft_payload(draft_payload or {}) else "none_or_restricted"
                if draft_status == "available":
                    seeds_with_draft += 1
                for row in history_records(year, str(league_id), league, info["name"]):
                    row.update(info)
                    row.update({"seed_year": year, "seed_id": str(league_id), "draft_status": draft_status})
                    records[(row["year"], row["id"])] = row
            if position == 1 or position == len(year_ids) or position % 25 == 0:
                elapsed = max(time.monotonic() - started, 0.001)
                rate = completed_probes / elapsed * 60
                _log(
                    f"shard={args.shard_index} year={year} id={league_id} "
                    f"ids={position}/{len(year_ids)} probes={completed_probes}/{total_probes} "
                    f"valid={seeds_checked} drafts={seeds_with_draft} requests={client.requests} "
                    f"429s={client.throttles} errors={client.errors} rate={rate:.1f}/min"
                )
        if client.rate_limit_open:
            _write_snapshot(
                args.out,
                _result(
                    args,
                    records,
                    client,
                    seeds_checked,
                    seeds_with_draft,
                    completed_through_year=last_completed_year,
                    complete=False,
                ),
            )
            break
        _log(
            f"shard={args.shard_index} year={year} complete valid=+{seeds_checked - year_valid_before} "
            f"drafts=+{seeds_with_draft - year_draft_before} records={len(records)} "
            f"elapsed={time.monotonic() - year_started:.0f}s"
        )
        snapshot = _result(
            args,
            records,
            client,
            seeds_checked,
            seeds_with_draft,
            completed_through_year=year,
            complete=False,
        )
        _write_snapshot(args.out, snapshot)
        last_completed_year = year
        _log(
            f"shard={args.shard_index} checkpoint year={year} records={len(records)} "
            f"drafts={seeds_with_draft} requests={client.requests}"
        )
        if client.rate_limit_open:
            break
    return _result(
        args,
        records,
        client,
        seeds_checked,
        seeds_with_draft,
        completed_through_year=last_completed_year,
        complete=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, default=int(os.environ.get("MFL_SHARD_INDEX", "0")))
    parser.add_argument("--shard-count", type=int, default=int(os.environ.get("MFL_SHARD_COUNT", "256")))
    parser.add_argument("--ids-per-shard", type=int, default=int(os.environ.get("MFL_IDS_PER_SHARD", "4")))
    parser.add_argument("--id-start", type=int, default=int(os.environ.get("MFL_ID_START", "0")))
    parser.add_argument("--id-end", type=int, default=int(os.environ.get("MFL_ID_END", "10000")))
    parser.add_argument("--start-year", type=int, default=int(os.environ.get("MFL_START_YEAR", "1997")))
    parser.add_argument("--end-year", type=int, default=int(os.environ.get("MFL_END_YEAR", "2025")))
    parser.add_argument("--id-bands", default=os.environ.get("MFL_ID_BANDS", DEFAULT_BANDS))
    parser.add_argument("--sleep", type=float, default=float(os.environ.get("MFL_SLEEP", "4.2")))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("MFL_TIMEOUT", "30")))
    parser.add_argument("--max-errors", type=int, default=int(os.environ.get("MFL_MAX_ERRORS", "25")))
    parser.add_argument("--out", type=Path, default=Path(os.environ.get("MFL_SHARD_OUT", "mfl_shard.json")))
    parser.add_argument("--task-manifest", type=Path, default=None)
    parser.add_argument("--task-window", type=int, default=0)
    args = parser.parse_args()
    result = run(args)
    _write_snapshot(args.out, result)
    print(json.dumps({k: result[k] for k in ("shard_index", "requests", "seeds_checked", "seeds_with_draft")}, sort_keys=True))


if __name__ == "__main__":
    main()
