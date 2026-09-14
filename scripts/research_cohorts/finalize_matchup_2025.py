"""Replace selected year slices of a verified release with complete assemblies."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--replacement", type=Path, required=True,
                    help="Complete assembled replacement for the default year (2025).")
    ap.add_argument("--replacement-2024", type=Path, default=None,
                    help="Optional complete assembled 2024 replacement.")
    ap.add_argument("--replacement-historical", type=Path, default=None,
                    help="Optional assembled replacement containing one or more historical years.")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # The prior verified release also contains two legacy canary years (1997/1998).
    # The current research-matchup target is explicitly 2003-2025, so exclude
    # those canaries when carrying forward the base release.
    expected_years = list(range(2003, 2026))
    # Each replacement root may contain one or many complete year slices.  Resolve the
    # year coverage from the artifact itself so a historical rescue cannot silently carry
    # forward a stale year or overwrite a year twice.
    replacement_groups = []
    for root in (args.replacement, args.replacement_2024, args.replacement_historical):
        if root is None:
            continue
        replacement_groups.append(root)

    # A finalized GH artifact is the durable source of truth for any year it has
    # certified.  Carry those years forward byte-for-byte unless a future job is
    # explicitly changed to implement a new certification workflow.  In
    # particular, do not let a later pull silently reintroduce the older base
    # values after a rescue replacement has passed validation.
    prior_manifest = None
    manifests = sorted(args.base.rglob("research_matchup_golden_manifest.json"))
    if manifests:
        prior_manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    protected_years = {
        int(year) for year in (prior_manifest or {}).get("protected_years", [])
    }
    for name in ("research_matchup_player_season.parquet", "research_matchup_weekly.parquet"):
        old = next(args.base.rglob(name))
        old = old.resolve()
        new_groups = []
        replacement_years = set()
        for root in replacement_groups:
            new = next(root.rglob(name)).resolve()
            years = {int(row[0]) for row in con.execute(
                "SELECT DISTINCT CAST(year AS INTEGER) FROM read_parquet(?)", [str(new)]
            ).fetchall()}
            if not years or not years.issubset(set(expected_years)):
                raise SystemExit(f"{name}: invalid replacement years {sorted(years)}")
            overlap = replacement_years & years
            if overlap:
                raise SystemExit(f"{name}: replacement year overlap {sorted(overlap)}")
            replacement_years |= years
            new_groups.append((years, new))
        forbidden = protected_years & replacement_years
        if forbidden:
            raise SystemExit(
                f"{name}: refusing to overwrite certified GH years "
                f"{sorted(forbidden)}; use a new explicit certification workflow"
            )
        if 2025 not in replacement_years:
            raise SystemExit(f"{name}: complete 2025 replacement is required")
        target = (args.out / name).resolve().as_posix().replace("'", "''")
        old_sql = old.as_posix().replace("'", "''")
        pieces = [f"SELECT * FROM read_parquet('{old_sql}', union_by_name=true)\n"
                  "WHERE CAST(year AS INTEGER) BETWEEN 2003 AND 2025"]
        for years, new in new_groups:
            new_sql = new.as_posix().replace("'", "''")
            year_list = ", ".join(str(year) for year in sorted(years))
            pieces.append(
                f"SELECT * FROM read_parquet('{new_sql}', union_by_name=true)\n"
                f"WHERE CAST(year AS INTEGER) IN ({year_list})"
            )
        # Remove every replaced year from the carry-forward release before
        # appending the corresponding complete assembly.
        excluded = ", ".join(str(year) for year in sorted(replacement_years))
        pieces[0] = (f"SELECT * FROM read_parquet('{old_sql}', union_by_name=true)\n"
                     f"WHERE CAST(year AS INTEGER) BETWEEN 2003 AND 2025\n"
                     f"  AND CAST(year AS INTEGER) NOT IN ({excluded})")
        con.execute(f"""COPY (
          {" UNION ALL BY NAME ".join(pieces)}
        ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM read_parquet('{target}') ORDER BY year").fetchall()]
        if years != expected_years:
            raise SystemExit(f"{name}: year coverage {years} != {expected_years}")
        print(name, con.execute(f"SELECT COUNT(*) FROM read_parquet('{target}')").fetchone()[0])

    weekly = args.out / "research_matchup_weekly.parquet"
    season = args.out / "research_matchup_player_season.parquet"
    requirements = {
        weekly: {"start_rate_pct", "healthy_start_rate_pct", "win_rate_pct",
                  "expected_wins", "expected_losses", "expected_starts",
                  "clutch_sum", "champ_started", "champ_eligible"},
        season: {"start_rate_pct", "healthy_start_rate_pct", "win_rate_pct",
                 "expected_wins", "expected_losses", "expected_starts",
                 "avg_clutch_started", "champ_total_pct", "champ_as_starter_pct",
                 "playoff_total_pct", "playoff_as_starter_pct",
                 "champ_elig_leagues", "playoff_eligible_leagues"},
    }
    for path, required in requirements.items():
        cols = {r[0] for r in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
        missing = sorted(required - cols)
        if missing:
            raise SystemExit(f"{path.name}: missing required fields {missing}")
        # Apply the identity check to every replacement slice. Carried years are
        # checked by the full release gate after the wide bundle is built.
        bad = con.execute("""SELECT COUNT(*) FROM read_parquet(?)
          WHERE CAST(year AS INTEGER) IN (SELECT UNNEST(?))
            AND expected_wins IS NOT NULL AND expected_losses IS NOT NULL
            AND expected_starts IS NOT NULL
            AND ABS(expected_wins + expected_losses - expected_starts) > 0.0002""",
                          [str(path), sorted(replacement_years)]).fetchone()[0]
        if bad:
            raise SystemExit(f"{path.name}: {bad} expected-W/L identity failures")
        print(f"{path.name}: required fields and expected-W/L identity PASS")

    # Persist the release contract alongside the parquet files.  The manifest is
    # intentionally small and deterministic so it can be checked by later GH
    # pulls without reading Fly.  Replacement years become protected immediately;
    # carried years remain protected from the prior release.
    all_protected = sorted(protected_years | replacement_years)
    manifest = {
        "manifest_version": 1,
        "authority": "github-research-lake",
        "protected_years": all_protected,
        "replacement_years_this_release": sorted(replacement_years),
        "parquet": {},
    }
    for name in ("research_matchup_player_season.parquet", "research_matchup_weekly.parquet"):
        path = args.out / name
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        counts = {
            str(row[0]): int(row[1]) for row in con.execute(
                "SELECT CAST(year AS INTEGER), COUNT(*) "
                "FROM read_parquet(?) GROUP BY 1 ORDER BY 1", [str(path)]
            ).fetchall()
        }
        manifest["parquet"][name] = {
            "sha256": digest,
            "rows_by_year": counts,
        }
    (args.out / "research_matchup_golden_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"golden manifest: protected years {all_protected}")
    con.close()


if __name__ == "__main__":
    main()
