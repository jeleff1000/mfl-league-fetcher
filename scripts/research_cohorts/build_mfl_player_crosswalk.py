"""Build a deterministic year-scoped MFL-id -> canonical NFL player crosswalk.

MFL's player database is keyed by a year-specific native id.  The MFL payload
also carries ESPN ids, while the canonical ops player bio carries the
ESPN-id-to-NFL-player mapping.  This utility materializes only one-to-one
matches; it deliberately does not guess from names.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def _esc(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _name_norm(value: object) -> str:
    text = " ".join(str(value or "").replace(".", "").split()).strip().lower()
    if "," in text:
        last, first = [part.strip() for part in text.split(",", 1)]
        text = f"{first} {last}".strip()
    for suffix in (" iii", " iv", " ii", " jr", " sr", " v"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].rstrip()
    return "".join(ch for ch in text if ch.isalnum() or ch == " ").replace("  ", " ").strip()


def _pos_family(value: object) -> str:
    pos = str(value or "").strip().upper()
    return {
        "HB": "RB", "FB": "RB", "PK": "K", "DST": "DEF", "D/ST": "DEF", "DEF": "DEF",
    }.get(pos, pos)


def build(mfl_cache_dir: Path, ops_cache: Path, out: Path) -> dict[str, int]:
    con = duckdb.connect()
    if ops_cache.suffix.lower() == ".parquet":
        bio_from = f"read_parquet('{_esc(ops_cache)}')"
    else:
        con.execute(f"ATTACH '{_esc(ops_cache)}' AS ops (READ_ONLY)")
        bio_from = "ops.nfl_historical.player_bio"
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE bio AS
        SELECT CAST(espn_id AS VARCHAR) AS espn_id,
               CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
               player,
               nfl_position
        FROM {bio_from}
        WHERE espn_id IS NOT NULL AND TRIM(CAST(espn_id AS VARCHAR)) <> ''
          AND NFL_player_id IS NOT NULL AND TRIM(CAST(NFL_player_id AS VARCHAR)) <> ''"""
    )

    files = sorted(mfl_cache_dir.glob("mfl_players_*.json"))
    if not files:
        raise SystemExit(f"no mfl_players_*.json files under {mfl_cache_dir}")
    rows: list[tuple[str, int, str, str, str, str, str, str, str, str]] = []
    real_records = 0
    for path in files:
        try:
            year = int(path.stem.rsplit("_", 1)[1])
        except ValueError:
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for native_id, record in payload.items():
            if not isinstance(record, dict):
                continue
            position = str(record.get("position") or "").strip()
            if position.upper().startswith("TM"):
                continue
            real_records += 1
            espn_id = str(record.get("espn_id") or "").strip()
            if not espn_id:
                continue
            matches = con.execute(
                "SELECT DISTINCT NFL_player_id, player, nfl_position FROM bio WHERE espn_id = ?",
                [espn_id],
            ).fetchall()
            if len(matches) != 1:
                continue
            nfl_id, player, nfl_position = matches[0]
            rows.append((
                "mfl", year, str(native_id).strip(), espn_id, str(nfl_id),
                str(player or ""), str(nfl_position or ""), str(record.get("name") or ""),
                _name_norm(record.get("name")), _pos_family(position),
            ))

    con.execute(
        """CREATE OR REPLACE TABLE crosswalk(
            platform VARCHAR, year INTEGER, native_id VARCHAR, espn_id VARCHAR,
            NFL_player_id VARCHAR, canonical_player VARCHAR,
            canonical_position VARCHAR, mfl_name VARCHAR,
            name_norm VARCHAR, pos_family VARCHAR)"""
    )
    if rows:
        con.executemany("INSERT INTO crosswalk VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute("COPY crosswalk TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(out)])
    unique_name_keys, ambiguous_name_keys = con.execute(
        """SELECT
             COUNT(*) FILTER (WHERE n = 1),
             COUNT(*) FILTER (WHERE n > 1)
           FROM (
             SELECT year, name_norm, pos_family, COUNT(DISTINCT NFL_player_id) AS n
             FROM crosswalk
             WHERE platform = 'mfl' AND name_norm <> '' AND pos_family <> ''
             GROUP BY 1, 2, 3
           )"""
    ).fetchone()
    result = {
        "cache_files": len(files),
        "real_mfl_records": real_records,
        "crosswalk_rows": len(rows),
        "years": len({r[1] for r in rows}),
        "unique_name_position_keys": unique_name_keys,
        "ambiguous_name_position_keys_excluded": ambiguous_name_keys,
    }
    out.with_suffix(".json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mfl-cache-dir", type=Path, required=True)
    ap.add_argument("--ops-cache", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print(json.dumps(build(args.mfl_cache_dir, args.ops_cache, args.out), indent=2))


if __name__ == "__main__":
    main()
