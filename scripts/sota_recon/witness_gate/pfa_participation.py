from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .ffassets_import import _resolve_manifest_payload, _verify_manifest
from .position_taxonomy import (
    POSITION_TAXONOMY_VERSION,
    UnknownPositionToken,
    normalize_position,
)


DEFAULT_TAXONOMY_PATH = Path(__file__).with_name("contracts") / "position_taxonomy.v1.json"


@dataclass(frozen=True)
class PfaMaterializationResult:
    data_path: Path
    manifest_path: Path
    input_rows: int
    output_rows: int


@dataclass(frozen=True)
class _ShardRecords:
    shard_id: int
    manifest_sha256: str
    records_path: Path


_SOURCE = "profootballarchives"
_DATASET = "player_game_participation"
_SIDE_NAMES = {"offense": "Offense", "defense": "Defense"}
_BATCH_SIZE = 65_536
_NATURAL_KEY = (
    "season",
    "game_id",
    "team",
    "lineup_side",
    "source_player_id",
    "nfl_position",
)
_SEMANTIC_FIELDS = (
    "season",
    "game_id",
    "team",
    "lineup_side",
    "player",
    "source_player_id",
    "source_position_raw",
    "position",
    "nfl_position",
    "participated",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _nonblank(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _lineage_error(message: str, row: dict[str, Any], row_index: int) -> ValueError:
    return ValueError(
        f"{message}; source={row['source']} dataset={row['dataset']} "
        f"artifact_run_id={row['artifact_run_id']} shard_id={row['shard_id']} "
        f"row_index={row_index} game_id={row.get('game_id')!r} "
        f"source_player_id={row.get('source_player_id')!r}"
    )


def _load_taxonomy_version(path: Path) -> str:
    requested = json.loads(path.read_text(encoding="utf-8"))
    canonical = json.loads(DEFAULT_TAXONOMY_PATH.read_text(encoding="utf-8"))
    if requested != canonical:
        raise ValueError("taxonomy_path must contain the canonical Task 1 taxonomy contract")
    version = str(requested.get("taxonomy_version", ""))
    if version != POSITION_TAXONOMY_VERSION:
        raise ValueError(f"position taxonomy version mismatch: {version!r}")
    return version


def _load_import_inputs(import_manifest: Path) -> tuple[list[_ShardRecords], dict[str, Any]]:
    manifest_path = import_manifest.resolve(strict=True)
    body = json.loads(manifest_path.read_text(encoding="utf-8"))
    if body.get("source") != _SOURCE or body.get("dataset") != _DATASET:
        raise ValueError(
            "expected imported dataset "
            f"{_SOURCE}:{_DATASET}, got {body.get('source')}:{body.get('dataset')}"
        )
    run_id = str(body.get("run_id", "")).strip()
    manifest_pin = str(body.get("manifest_pin", "")).strip()
    if not run_id or not manifest_pin:
        raise ValueError("import manifest requires run_id and manifest_pin")
    entries = body.get("shard_manifests")
    if not isinstance(entries, list) or not entries:
        raise ValueError("import manifest requires declared shard manifests")

    seen_ids: set[int] = set()
    inputs: list[_ShardRecords] = []
    for entry in entries:
        shard_id = int(entry["shard_id"])
        if shard_id in seen_ids:
            raise ValueError(f"duplicate shard ID in import manifest: {shard_id}")
        seen_ids.add(shard_id)
        expected_hash = str(entry.get("sha256", ""))
        named_path = Path(str(entry.get("source_path", "")))
        if not named_path.is_absolute():
            named_path = manifest_path.parent / named_path
        try:
            shard_manifest_path = named_path.resolve(strict=True)
        except (FileNotFoundError, OSError) as exc:
            raise ValueError(f"declared shard manifest missing: {named_path}") from exc
        if _sha256(shard_manifest_path) != expected_hash:
            raise ValueError(f"shard manifest checksum mismatch: shard_id={shard_id}")
        verified = _verify_manifest(shard_manifest_path, run_id)
        if (
            verified.source != _SOURCE
            or verified.dataset != _DATASET
            or verified.shard_id != shard_id
            or verified.manifest_sha256 != expected_hash
        ):
            raise ValueError(f"shard manifest lineage mismatch: shard_id={shard_id}")
        shard_body = json.loads(shard_manifest_path.read_text(encoding="utf-8"))
        records_entries = [item for item in shard_body.get("files", []) if item.get("path") == "records.parquet"]
        if len(records_entries) != 1:
            raise ValueError(f"shard manifest must name exactly one records.parquet: shard_id={shard_id}")
        records_path = _resolve_manifest_payload(verified.artifact_dir, Path("records.parquet"))
        inputs.append(
            _ShardRecords(
                shard_id=shard_id,
                manifest_sha256=expected_hash,
                records_path=records_path,
            )
        )
    return inputs, body


def _iter_input_batches(
    inputs: list[_ShardRecords], import_body: dict[str, Any]
):
    run_id = str(import_body["run_id"])
    manifest_pin = str(import_body["manifest_pin"])
    for shard in inputs:
        row_offset = 0
        parquet = pq.ParquetFile(shard.records_path)
        for batch in parquet.iter_batches(batch_size=_BATCH_SIZE):
            rows: list[dict[str, Any]] = []
            for batch_index, raw in enumerate(batch.to_pylist()):
                row_index = row_offset + batch_index
                row = dict(raw)
                try:
                    row_shard = int(row["shard_id"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        "row requires an integer shard_id: "
                        f"declared_shard_id={shard.shard_id} row_index={row_index}"
                    ) from exc
                if row_shard != shard.shard_id:
                    raise ValueError(
                        f"row shard_id mismatch: row={row_shard} "
                        f"declared={shard.shard_id} row_index={row_index}"
                    )
                row.update(
                    {
                        "source": _SOURCE,
                        "dataset": _DATASET,
                        "artifact_run_id": run_id,
                        "manifest_pin": manifest_pin,
                        "manifest_sha256": shard.manifest_sha256,
                        "_row_index": row_index,
                    }
                )
                rows.append(row)
            row_offset += len(rows)
            yield rows


def _collect_legacy_team_context(
    rows: list[dict[str, Any]],
    clubs_by_context: dict[tuple[object, object, object], set[str]],
) -> None:
    """Accumulate the real (non-side) clubs observed per game/player context.

    This is deliberately a separate streaming pass. `_repair_legacy_team` may only
    claim a club for a legacy ``Offense``/``Defense`` row when the context resolves
    to exactly one club across the WHOLE dataset -- so the map has to be complete
    before any row is repaired. Folding this into the repair pass would make the
    claim depend on where batch boundaries happen to fall: a context split across
    two batches would look unique in each half and invent a team the evidence does
    not support. The map is keyed by context, not by row, so it stays small.
    """
    for row in rows:
        team = _nonblank(row.get("team"))
        if not isinstance(team, str) or team.casefold() in _SIDE_NAMES:
            continue
        context = (row.get("season"), row.get("game_id"), row.get("source_player_id"))
        clubs_by_context.setdefault(context, set()).add(team)


def _repair_legacy_team(
    rows: list[dict[str, Any]],
    clubs_by_context: dict[tuple[object, object, object], set[str]],
) -> None:
    for row in rows:
        team = _nonblank(row.get("team"))
        if not isinstance(team, str) or team.casefold() not in _SIDE_NAMES:
            row["team"] = team
            row["lineup_side"] = _nonblank(row.get("lineup_side"))
            continue
        legacy_side = _SIDE_NAMES[team.casefold()]
        existing_side = _nonblank(row.get("lineup_side"))
        if existing_side is not None and str(existing_side).casefold() != legacy_side.casefold():
            raise _lineage_error(
                f"legacy team side conflicts with lineup_side={existing_side!r}",
                row,
                int(row["_row_index"]),
            )
        context = (row.get("season"), row.get("game_id"), row.get("source_player_id"))
        clubs = clubs_by_context.get(context, set())
        row["team"] = next(iter(clubs)) if len(clubs) == 1 else None
        row["lineup_side"] = legacy_side


def _normalize_rows(rows: list[dict[str, Any]], taxonomy_version: str) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        row_index = int(row["_row_index"])
        for required in ("season", "game_id", "lineup_side", "player", "source_player_id"):
            row[required] = _nonblank(row.get(required))
            if row[required] is None:
                raise _lineage_error(f"missing required participation field {required!r}", row, row_index)
        raw_position = _nonblank(row.get("source_position_raw"))
        if raw_position is None:
            raw_position = _nonblank(row.get("position"))
        try:
            normalized = normalize_position(raw_position)
        except UnknownPositionToken as exc:
            raise _lineage_error(
                f"unknown PFA position token {raw_position!r}", row, row_index
            ) from exc
        output = {
            "season": int(row["season"]),
            "game_id": row["game_id"],
            "team": _nonblank(row.get("team")),
            "lineup_side": row["lineup_side"],
            "player": row["player"],
            "source_player_id": row["source_player_id"],
            "source_position_raw": normalized.source_position_raw,
            "position": normalized.position,
            "nfl_position": normalized.nfl_position,
            "participated": True,
            "source": row["source"],
            "dataset": row["dataset"],
            "artifact_run_id": row["artifact_run_id"],
            "manifest_pin": row["manifest_pin"],
            "lineage_leaves": [
                {
                    "shard_id": int(row["shard_id"]),
                    "manifest_sha256": row["manifest_sha256"],
                }
            ],
            "taxonomy_version": taxonomy_version,
        }
        normalized_rows.append(output)
    return normalized_rows


def _deduplicate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row[field] for field in _NATURAL_KEY)
        existing = merged.get(key)
        if existing is None:
            merged[key] = row.copy()
            continue
        for field in _SEMANTIC_FIELDS:
            left = existing[field]
            right = row[field]
            if left is not None and right is not None and left != right:
                shard_ids = sorted(
                    {
                        leaf["shard_id"]
                        for candidate in (existing, row)
                        for leaf in candidate["lineage_leaves"]
                    }
                )
                raise ValueError(
                    f"conflicting duplicate for key={key!r}: field={field!r} "
                    f"left={left!r} right={right!r} "
                    f"leaves={','.join(f'shard_id={value}' for value in shard_ids)}"
                )
            if left is None:
                existing[field] = right
        leaves_by_shard = {
            int(leaf["shard_id"]): str(leaf["manifest_sha256"])
            for leaf in existing["lineage_leaves"]
        }
        for leaf in row["lineage_leaves"]:
            shard_id = int(leaf["shard_id"])
            manifest_sha256 = str(leaf["manifest_sha256"])
            previous = leaves_by_shard.get(shard_id)
            if previous is not None and previous != manifest_sha256:
                raise ValueError(
                    "conflicting provenance definition for "
                    f"shard_id={shard_id}: left={previous!r} right={manifest_sha256!r}"
                )
            leaves_by_shard[shard_id] = manifest_sha256
        existing["lineage_leaves"] = [
            {"shard_id": shard_id, "manifest_sha256": leaves_by_shard[shard_id]}
            for shard_id in sorted(leaves_by_shard)
        ]
    return sorted(
        merged.values(),
        key=lambda row: json.dumps(
            [row[field] for field in _NATURAL_KEY],
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def _write_materialization(
    rows: list[dict[str, Any]],
    output_dir: Path,
    *,
    input_rows: int,
    import_body: dict[str, Any],
    taxonomy_version: str,
) -> PfaMaterializationResult:
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(f"immutable materialization target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.materializing-{os.getpid()}")
    if staging.exists():
        raise FileExistsError(f"materialization staging target already exists: {staging}")
    staging.mkdir()
    try:
        data_path = staging / "PFA_PARTICIPATION.parquet"
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, data_path, compression="zstd")
        manifest = {
            "schema_version": 1,
            "source": _SOURCE,
            "dataset": _DATASET,
            "artifact_run_id": import_body["run_id"],
            "manifest_pin": import_body["manifest_pin"],
            "taxonomy_version": taxonomy_version,
            "input_rows": input_rows,
            "output_rows": len(rows),
            "natural_key": list(_NATURAL_KEY),
            "claims": [
                "participation",
                "player_team_year_association_when_team_known",
                "lineup_side",
                "raw_detailed_broad_position",
                "source_identity_alias",
            ],
            "lineage": {
                "field": "lineage_leaves",
                "type": "list<struct<manifest_sha256:string,shard_id:int64>>",
                "leaf_key": ["shard_id", "manifest_sha256"],
            },
            "data_file": {
                "path": data_path.name,
                "bytes": data_path.stat().st_size,
                "sha256": _sha256(data_path),
            },
        }
        manifest_path = staging / "MATERIALIZATION_MANIFEST.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        staging.rename(target)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return PfaMaterializationResult(
        data_path=target / data_path.name,
        manifest_path=target / manifest_path.name,
        input_rows=input_rows,
        output_rows=len(rows),
    )


def materialize_pfa_participation(
    import_manifest: Path,
    output_dir: Path,
    *,
    taxonomy_path: Path = DEFAULT_TAXONOMY_PATH,
) -> PfaMaterializationResult:
    output_path = Path(output_dir)
    if output_path.resolve().exists():
        raise FileExistsError(
            f"immutable materialization target already exists: {output_path.resolve()}"
        )
    taxonomy_version = _load_taxonomy_version(Path(taxonomy_path))
    inputs, import_body = _load_import_inputs(Path(import_manifest))

    # Pass 1: build the game/player -> clubs map over the full dataset, so the
    # legacy-side repair below never makes a team claim on partial evidence.
    clubs_by_context: dict[tuple[object, object, object], set[str]] = {}
    for batch in _iter_input_batches(inputs, import_body):
        _collect_legacy_team_context(batch, clubs_by_context)

    # Pass 2: repair and normalize a batch at a time. Only the normalized output
    # accumulates -- deduplication is inherently whole-dataset, but the raw shard
    # rows never all sit in memory at once.
    normalized: list[dict[str, Any]] = []
    input_rows = 0
    for batch in _iter_input_batches(inputs, import_body):
        input_rows += len(batch)
        _repair_legacy_team(batch, clubs_by_context)
        normalized.extend(_normalize_rows(batch, taxonomy_version))

    deduplicated = _deduplicate(normalized)
    return _write_materialization(
        deduplicated,
        output_path,
        input_rows=input_rows,
        import_body=import_body,
        taxonomy_version=taxonomy_version,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize contracted PFA participation evidence.")
    parser.add_argument("--import-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--taxonomy-path", type=Path, default=DEFAULT_TAXONOMY_PATH)
    args = parser.parse_args()
    result = materialize_pfa_participation(
        args.import_manifest,
        args.output_root,
        taxonomy_path=args.taxonomy_path,
    )
    print(
        json.dumps(
            {
                "data_path": str(result.data_path),
                "manifest_path": str(result.manifest_path),
                "input_rows": result.input_rows,
                "output_rows": result.output_rows,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
