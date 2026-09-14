"""Merge processed Actions shard archives back into the local source lake."""
from __future__ import annotations

import argparse
import json
import shutil
import tarfile
from pathlib import Path

import duckdb


def player_row_key_sql(table_alias: str, available_columns: set[str]) -> str:
    """Match clutch rows even when a historical source has partial ID coverage."""
    def present(column: str) -> str:
        return f"NULLIF(TRIM(CAST({table_alias}.\"{column}\" AS VARCHAR)), '')"

    def value(column: str) -> str:
        return f"COALESCE(CAST({table_alias}.\"{column}\" AS VARCHAR), '')" if column in available_columns else "''"

    cases = []
    if "player_week" in available_columns:
        cases.append(f"WHEN {present('player_week')} IS NOT NULL THEN 'pw:' || {value('player_week')}")
    for column in ("NFL_player_id", "sleeper_player_id", "fleaflicker_player_id", "espn_player_id", "yahoo_player_id"):
        if column in available_columns:
            cases.append(
                f"WHEN {present(column)} IS NOT NULL THEN '{column}:' || {value(column)} || ':' || {value('year')} || ':' || {value('week')}"
            )
    fallback = " || ':' || ".join(value(column) for column in ("year", "week", "manager", "position", "player"))
    return "CASE " + " ".join(cases) + f" ELSE 'fallback::' || {fallback} END"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--archive-dir", type=Path, required=True)
    p.add_argument("--source-dir", type=Path, required=True)
    p.add_argument("--staging-dir", type=Path, required=True)
    args = p.parse_args()
    # gh run download nests each artifact under its artifact name; direct
    # single-artifact retries may instead land at archive-dir root.
    found = sorted(args.archive_dir.rglob("research-source-backfill-result-*.tar"))
    archives_by_name = {}
    for archive in found:
        # Prefer the canonical nested artifact path when both forms exist.
        if archive.name not in archives_by_name or archive.parent != args.archive_dir:
            archives_by_name[archive.name] = archive
    archives = [archives_by_name[name] for name in sorted(archives_by_name)]
    if not archives:
        raise SystemExit(f"no result archives under {args.archive_dir}")
    args.staging_dir.mkdir(parents=True, exist_ok=True)
    merged = []
    for archive in archives:
        with tarfile.open(archive) as tar:
            tar.extractall(args.staging_dir, filter="data")
        result = args.staging_dir / "source_playoff_clutch_backfill_results.jsonl"
        if result.exists():
            merged.extend(json.loads(line) for line in result.read_text(encoding="utf-8").splitlines() if line.strip())
            result.unlink()
    if not merged:
        raise SystemExit("result archives contained no result JSONL")
    done = [r for r in merged if r.get("status") == "done"]
    def ident(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def apply_db(processed: Path, target: Path) -> None:
        con = duckdb.connect(str(target))
        proc = processed.resolve().as_posix().replace("'", "''")
        con.execute(f"ATTACH '{proc}' AS backfill (READ_ONLY)")
        try:
            for table, keys in (("matchup", ("year", "week", "franchise_id")), ("player_fantasy", ("player_week",))):
                if not con.execute(
                    "select count(*) from duckdb_tables() where database_name='backfill' and schema_name='public' and table_name=?",
                    [table],
                ).fetchone()[0]:
                    continue
                if not con.execute("select count(*) from duckdb_tables() where schema_name='public' and table_name=?", [table]).fetchone()[0]:
                    continue
                pcols = {r[0]: str(r[1]) for r in con.execute(f"describe backfill.public.{ident(table)}").fetchall()}
                fcols = {r[0]: str(r[1]) for r in con.execute(f"describe public.{ident(table)}").fetchall()}
                actual_keys = [k for k in keys if k in pcols and k in fcols]
                if len(actual_keys) < 2 and table == "matchup":
                    continue
                if not actual_keys and table != "player_fantasy":
                    continue
                for col, dtype in pcols.items():
                    if col not in fcols:
                        con.execute(f"alter table public.{ident(table)} add column {ident(col)} {dtype}")
                update_cols = [c for c in pcols if c not in actual_keys and c != "db_name"]
                if not update_cols:
                    continue
                set_sql = ", ".join(f"{ident(c)} = b.{ident(c)}" for c in update_cols)
                if table == "player_fantasy":
                    join_sql = f"{player_row_key_sql('t', fcols)} = {player_row_key_sql('b', pcols)}"
                else:
                    join_sql = " AND ".join(f"t.{ident(k)} = b.{ident(k)}" for k in actual_keys)
                con.execute(f"update public.{ident(table)} t set {set_sql} from backfill.public.{ident(table)} b where {join_sql}")
        finally:
            con.execute("detach backfill")
            con.close()

    for r in done:
        db = r["db_name"]
        src = args.staging_dir / "leagues" / db
        dst = args.source_dir / db
        if not (src / f"{db}.duckdb").is_file():
            raise SystemExit(f"missing processed database for {db}")
        if not (dst / f"{db}.duckdb").is_file():
            raise SystemExit(f"missing local target database for {db}")
        apply_db(src / f"{db}.duckdb", dst / f"{db}.duckdb")
    out = args.staging_dir / "source_playoff_clutch_backfill_results.jsonl"
    out.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in sorted(merged, key=lambda x: x["db_name"])), encoding="utf-8")
    counts = {}
    for r in merged:
        counts[r.get("status", "unknown")] = counts.get(r.get("status", "unknown"), 0) + 1
    print(json.dumps({"archives": len(archives), "results": len(merged), "copied_done": len(done), "counts": counts}, sort_keys=True))


if __name__ == "__main__":
    main()
