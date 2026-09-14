"""Audit sparse playoff/championship player coverage by league-season.

This is an inventory-only audit against the canonical research lake.  It does
not infer missing rows from matchup coverage and does not write any lake or
cache.  A league-season is flagged when fewer than 10 distinct mapped players
have a positive ``is_playoffs`` signal or fewer than 5 distinct mapped players
have a positive ``champion`` marker.

The champion count is deliberately labelled ``champion_marked_players``: the
canonical player schema's ``champion`` field is a season/team marker, not proof
that the player started the title game.  This audit identifies sparse coverage;
it does not promote that marker into championship-game truth.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def cols(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def build(root: Path, out: Path, playoff_threshold: int = 10, champ_threshold: int = 5) -> dict:
    snapshot = root / "corpus_snapshot.duckdb"
    if not snapshot.is_file() or snapshot.stat().st_size == 0:
        raise SystemExit(f"missing canonical snapshot: {snapshot}")
    con = duckdb.connect(str(snapshot), read_only=True)
    try:
        pcols = cols(con, "public.player_fantasy")
        scols = cols(con, "public.league_settings")
        required = {"db_name", "year", "NFL_player_id", "is_playoffs", "champion"}
        missing = required - pcols
        if missing:
            raise SystemExit(f"canonical schema missing required player columns: {sorted(missing)}")
        if not {"db_name", "year", "platform"} <= scols:
            raise SystemExit("canonical schema missing league_settings db_name/year/platform")

        # Count distinct player identities, not player rows.  Unmapped rows are
        # retained under a deterministic synthetic key so the audit exposes them
        # instead of silently shrinking the sparse-player counts.
        player_key = "CASE WHEN p.NFL_player_id IS NOT NULL THEN 'id:' || CAST(p.NFL_player_id AS VARCHAR) ELSE 'unmapped:' || COALESCE(NULLIF(TRIM(CAST(p.player AS VARCHAR)), ''), 'row') END"
        player_name = "COALESCE(NULLIF(TRIM(CAST(p.player AS VARCHAR)), ''), CAST(p.NFL_player_id AS VARCHAR))" if "player" in pcols else "CAST(p.NFL_player_id AS VARCHAR)"
        sql = f"""
          WITH populated AS (
            SELECT CAST(p.db_name AS VARCHAR) AS db_name,
                   CAST(p.year AS INTEGER) AS year,
                   LOWER(TRIM(CAST(s.platform AS VARCHAR))) AS platform,
                   COUNT(*) AS player_rows,
                   COUNT(DISTINCT {player_key}) AS distinct_players,
                   COUNT(DISTINCT CASE WHEN p.NFL_player_id IS NULL THEN {player_name} END) AS unmapped_players,
                   COUNT(DISTINCT CASE WHEN CAST(p.is_playoffs AS INTEGER)=1 THEN {player_key} END) AS playoff_players,
                   COUNT(DISTINCT CASE WHEN CAST(p.champion AS INTEGER)=1 THEN {player_key} END) AS champion_marked_players,
                   COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1) AS started_rows,
                   COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND CAST(p.is_playoffs AS INTEGER)=1) AS started_playoff_rows,
                   COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND CAST(p.champion AS INTEGER)=1) AS started_champion_marked_rows
            FROM public.player_fantasy p
            JOIN public.league_settings s
              ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
            GROUP BY 1,2,3
          )
          SELECT *,
                 playoff_players < {int(playoff_threshold)} AS playoff_sparse,
                 champion_marked_players < {int(champ_threshold)} AS champion_sparse,
                 list_filter([
                   CASE WHEN playoff_players < {int(playoff_threshold)} THEN 'playoff_players_below_threshold' ELSE NULL END,
                   CASE WHEN champion_marked_players < {int(champ_threshold)} THEN 'champion_marked_players_below_threshold' ELSE NULL END,
                   CASE WHEN unmapped_players > 0 THEN 'unmapped_player_ids_present' ELSE NULL END
                 ], x -> x IS NOT NULL) AS reason_codes
          FROM populated
          ORDER BY platform, year, db_name
        """
        rows = con.execute(sql).fetchdf()
        if rows.empty:
            raise SystemExit("fail-closed: populated league-season audit returned zero rows")
        flagged = rows[(rows["playoff_sparse"] == True) | (rows["champion_sparse"] == True)].copy()
        platform_year = (
            rows.groupby(["platform", "year"], dropna=False)
            .agg(
                league_seasons=("db_name", "nunique"),
                flagged_league_seasons=("playoff_sparse", lambda s: int(((s == True) | (rows.loc[s.index, "champion_sparse"] == True)).sum())),
                playoff_sparse_league_seasons=("playoff_sparse", "sum"),
                champion_sparse_league_seasons=("champion_sparse", "sum"),
                unmapped_league_seasons=("unmapped_players", lambda s: int((s > 0).sum())),
            )
            .reset_index()
        )
        summary = {
            "schema_version": 1,
            "population": "populated league-seasons in canonical player_fantasy joined to league_settings",
            "thresholds": {"playoff_players_lt": int(playoff_threshold), "champion_marked_players_lt": int(champ_threshold)},
            "league_seasons": int(len(rows)),
            "flagged_league_seasons": int(len(flagged)),
            "by_platform": {
                str(platform): {
                    "league_seasons": int(len(group)),
                    "flagged": int(((group["playoff_sparse"] == True) | (group["champion_sparse"] == True)).sum()),
                    "playoff_sparse": int(group["playoff_sparse"].sum()),
                    "champion_sparse": int(group["champion_sparse"].sum()),
                }
                for platform, group in rows.groupby("platform", dropna=False)
            },
        }
        out.mkdir(parents=True, exist_ok=True)
        rows.to_csv(out / "playoff_championship_sparsity_all.csv", index=False)
        flagged.to_csv(out / "playoff_championship_sparsity_targets.csv", index=False)
        platform_year.to_csv(out / "playoff_championship_sparsity_by_platform_year.csv", index=False)
        (out / "playoff_championship_sparsity_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, sort_keys=True))
        return summary
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--playoff-threshold", type=int, default=10)
    ap.add_argument("--champ-threshold", type=int, default=5)
    args = ap.parse_args()
    build(args.root, args.out, args.playoff_threshold, args.champ_threshold)


if __name__ == "__main__":
    main()
