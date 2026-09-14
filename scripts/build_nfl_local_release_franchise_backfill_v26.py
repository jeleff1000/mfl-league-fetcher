"""Build v26: backfill nfl_franchise_number on all player rows.

The franchise_number column is authoritative on DEF rows (83% coverage) but nearly
empty on individual player rows (74-93% NULL). Since nfl_team+year always maps to
exactly one franchise_number (verified), this is a straightforward lookup fill.

Also fixes:
  - JAC 2001-2002 player rows -> franchise 27 (Jacksonville Jaguars)
  - SD  1999-2002 player rows -> franchise 32 (San Diego Chargers / LAC)
  - BOS 1944 franchise=4 DEF rows -> franchise=148 (Boston Yanks, WWII era)
    (The Redskins left Boston in 1937; the 1944 BOS team is the Yanks)
  - LAC franchise missing on some rows -> derive from SDG franchise 32
  - Legacy null-franchise 2014+ player rows: derive from team code + year

Stats after backfill:
  - Should bring player-row franchise coverage from ~15% to ~95%+ populated
  - ~5% residual: legacy 2014+ alternate codes (TB, GB, NE, SF, KC, NO, LA, LV)
    that match the DEF alternate codes; these get franchise from alt-code mapping.

Usage:
    python build_nfl_local_release_franchise_backfill_v26.py          # dry run
    python build_nfl_local_release_franchise_backfill_v26.py --execute
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

RELEASES = Path(r"D:\league-history-data\nfl\releases")
V25_PARQUET = sorted(RELEASES.glob("*_v25/tables/nfl_player_stats_all.parquet"))[-1]

# Alternate-code -> canonical-code mappings for teams where 2014+ rows
# use nflfastR-style abbreviations instead of PFR-style abbreviations.
# These pairs share a franchise and the alternate has franchise_number=NULL.
ALT_CODE_TO_FRANCHISE = {
    # nflfastR-style codes that differ from PFR-style codes on legacy DEF rows
    "TB":  12,   # Tampa Bay Buccaneers  (PFR: TAM)
    "GB":   7,   # Green Bay Packers      (PFR: GNB)
    "NE":  19,   # New England Patriots   (PFR: NWE)
    "SF":  15,   # San Francisco 49ers    (PFR: SFO)
    "KC":  30,   # Kansas City Chiefs     (PFR: KAN)
    "NO":  11,   # New Orleans Saints     (PFR: NOR)
    "LA":  14,   # Los Angeles Rams       (PFR: LAR / RAM)
    "LV":  31,   # Las Vegas Raiders      (PFR: LVR / OAK)
    # Modern NFL abbreviations used retroactively in stathead PBP for historical seasons
    "LAC": 32,   # Los Angeles Chargers / San Diego Chargers (same franchise)
    "TEN": 28,   # Tennessee Titans / Houston Oilers (same franchise)
    "ARI": 13,   # Arizona Cardinals / St. Louis Cardinals / Chicago Cardinals
    "IND": 26,   # Indianapolis Colts / Baltimore Colts (same franchise)
    "LAR": 14,   # Los Angeles Rams (same as LA above, PFR modern code)
    # Other codes with franchise gaps
    "JAC": 27,   # Jacksonville Jaguars   (PFR: JAX)
    "SD":  32,   # San Diego Chargers     (PFR: SDG, same franchise as LAC)
    "NYY": 114,  # New York Yankees (NFL, 1927-1928; 1926 NYY rows have NULL from source)
}

# Special one-off corrections before the general backfill
SPECIAL_CORRECTIONS = [
    # BOS franchise=4 in 1944 -- should be franchise=148 (Boston Yanks WWII team)
    # The Washington Redskins (franchise 4) left Boston in 1937.
    dict(
        description="BOS franchise=4 in 1944 -> 148 (Boston Yanks)",
        where="nfl_team='BOS' AND nfl_franchise_number=4 AND year=1944",
        set_col="nfl_franchise_number",
        set_val=148,
    ),
    # BOS franchise=NULL for years 1944-1948 (Boston Yanks era)
    # Gap-insert rows for BOS Yanks players have no franchise tag from source.
    # Safe because no other NFL team used 'BOS' between 1944-1948.
    dict(
        description="BOS NULL franchise 1944-1948 -> 148 (Boston Yanks era gap-inserts)",
        where="nfl_team='BOS' AND nfl_franchise_number IS NULL AND year BETWEEN 1944 AND 1948",
        set_col="nfl_franchise_number",
        set_val=148,
    ),
    # BOS franchise=4 in 1929 -- those rows should be franchise=148.
    # Historical note: the 1929 Boston team was the "Boston Bulldogs" (relocated Pottsville
    # Maroons), a distinct ancient franchise from both the Redskins (1932+) and the Yanks
    # (1944-1948). The pfr_team_games source incorrectly tagged them as franchise 4.
    # The enriched bundle already tags them as 148. Consolidating to 148 eliminates the
    # lookup ambiguity. A dedicated franchise ID for the Bulldogs is a future v27 fix.
    dict(
        description="BOS franchise=4 in 1929 -> 148 (consolidate 1929 Boston to Yanks bucket, pending Bulldogs ID)",
        where="nfl_team='BOS' AND nfl_franchise_number=4 AND year=1929",
        set_col="nfl_franchise_number",
        set_val=148,
    ),
]

# J.J. Jones ID collision: PFR ID 00-0034560 is the 2018 WR (Jalen Jones, LAC/NYJ).
# Six 1975 NYJ WR rows (all zero-stat roster appearances) are incorrectly tagged with
# this modern player's ID -- 43-year career span is impossible.
# Fix: rekeyed to a synthetic SYN- ID so the 2018 player keeps his correct PFR ID.
JJ_JONES_OLD_ID = "00-0034560"
JJ_JONES_SYN_ID = "SYN-JJ-JONES-NYJ-1975"
JJ_JONES_YEAR   = 1975


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main(dry_run: bool = True) -> None:
    if not V25_PARQUET.exists():
        print(f"ERROR: v25 not found at {V25_PARQUET}", file=sys.stderr)
        sys.exit(1)

    db = duckdb.connect()
    db.execute("SET progress_bar_time=99999")

    print(f"Loading v25: {V25_PARQUET}")
    db.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{V25_PARQUET}')")
    total = db.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    null_before = db.execute("SELECT COUNT(*) FROM st WHERE nfl_franchise_number IS NULL").fetchone()[0]
    print(f"  Rows: {total:,}   NULL franchise: {null_before:,} ({100*null_before/total:.1f}%)")
    print()

    # ------------------------------------------------------------------ #
    # Step 0: Apply one-off special corrections
    # ------------------------------------------------------------------ #
    print("=== Step 0: Special corrections ===")
    for sc in SPECIAL_CORRECTIONS:
        count = db.execute(f"SELECT COUNT(*) FROM st WHERE {sc['where']}").fetchone()[0]
        print(f"  {sc['description']}: {count} rows affected")
        if not dry_run and count > 0:
            db.execute(
                f"UPDATE st SET {sc['set_col']} = {sc['set_val']} WHERE {sc['where']}"
            )

    # ------------------------------------------------------------------ #
    # Step 0.5: Fix J.J. Jones ID collision
    # PFR ID 00-0034560 belongs to the 2018 WR (Jalen Jones, LAC/NYJ).
    # Six 1975 NYJ WR rows share this ID, creating a 43-year impossible career.
    # Rekeying the 1975 rows to a SYN- ID resolves the collision.
    # ------------------------------------------------------------------ #
    print("\n=== Step 0.5: J.J. Jones 1975 ID collision fix ===")
    jj_old_count = db.execute(
        f"SELECT COUNT(*) FROM st WHERE NFL_player_id='{JJ_JONES_OLD_ID}' AND year={JJ_JONES_YEAR}"
    ).fetchone()[0]
    print(f"  1975 NYJ J.J. Jones rows with colliding ID ({JJ_JONES_OLD_ID}): {jj_old_count}")
    jj_modern_count = db.execute(
        f"SELECT COUNT(*) FROM st WHERE NFL_player_id='{JJ_JONES_OLD_ID}' AND year>{JJ_JONES_YEAR}"
    ).fetchone()[0]
    print(f"  2018 J.J. Jones rows (keep existing ID): {jj_modern_count}")
    if jj_old_count > 0:
        if not dry_run:
            db.execute(f"""
                UPDATE st SET NFL_player_id = '{JJ_JONES_SYN_ID}'
                WHERE NFL_player_id = '{JJ_JONES_OLD_ID}' AND year = {JJ_JONES_YEAR}
            """)
            db.execute(f"""
                UPDATE st SET player_week = REPLACE(player_week, '{JJ_JONES_OLD_ID}', '{JJ_JONES_SYN_ID}')
                WHERE player_week LIKE '%{JJ_JONES_OLD_ID}%' AND year = {JJ_JONES_YEAR}
            """)
            print(f"  -> Rekeyed {jj_old_count} rows: NFL_player_id and player_week updated")
        else:
            print(f"  -> Would rekeye {jj_old_count} rows (dry run)")
    else:
        print("  -> No 1975 rows with this ID found (already fixed or not present)")

    # ------------------------------------------------------------------ #
    # Step 1: Build (nfl_team, year) -> franchise_number lookup from rows
    #         where it IS already populated (all sources combined).
    # ------------------------------------------------------------------ #
    print("\n=== Step 1: Build franchise lookup from populated rows ===")
    lookup = db.execute("""
        SELECT nfl_team, CAST(year AS INT) as year, nfl_franchise_number, COUNT(*) as n
        FROM st
        WHERE nfl_franchise_number IS NOT NULL
        GROUP BY nfl_team, year, nfl_franchise_number
        ORDER BY nfl_team, year
    """).fetchall()

    # Resolve to a single franchise per (team, year): use highest row count as tiebreaker.
    ambig: dict[tuple, tuple[int, int]] = {}  # key -> (franchise, count)
    for team, year, fran, n in lookup:
        key = (team, year)
        if key in ambig:
            prev_fran, prev_n = ambig[key]
            if prev_fran != fran:
                winner = fran if n >= prev_n else prev_fran
                print(f"  AMBIGUOUS (resolved by count): {team} {year} "
                      f"-> {prev_fran}(n={prev_n}) vs {fran}(n={n}) => use {winner}")
                ambig[key] = (winner, max(n, prev_n))
        else:
            ambig[key] = (fran, n)

    print(f"  Lookup entries: {len(ambig)}  (unique team+year combos with franchise)")

    # Build SQL UPDATE: for each null row, derive franchise from same (nfl_team, year)
    # We do this via a JOIN in DuckDB using the lookup as a VALUES table.
    # Build a mapping table (ambig values are (franchise, count) tuples):
    vals = ",\n".join(f"('{t}', {y}, {f})" for (t, y), (f, _n) in ambig.items())
    db.execute(f"""
        CREATE TABLE fran_lookup AS
        SELECT * FROM (VALUES {vals}) AS t(team, year, franchise)
    """)

    # Count how many rows this will fill
    can_fill_primary = db.execute("""
        SELECT COUNT(*) FROM st s
        JOIN fran_lookup f ON s.nfl_team = f.team AND CAST(s.year AS INT) = f.year
        WHERE s.nfl_franchise_number IS NULL
    """).fetchone()[0]
    print(f"  Rows that can be filled via primary lookup: {can_fill_primary:,}")

    if not dry_run:
        db.execute("""
            UPDATE st
            SET nfl_franchise_number = (
                SELECT franchise FROM fran_lookup
                WHERE fran_lookup.team = st.nfl_team
                  AND fran_lookup.year = CAST(st.year AS INT)
                LIMIT 1
            )
            WHERE st.nfl_franchise_number IS NULL
              AND EXISTS (
                SELECT 1 FROM fran_lookup
                WHERE fran_lookup.team = st.nfl_team
                  AND fran_lookup.year = CAST(st.year AS INT)
              )
        """)
        null_after_primary = db.execute("SELECT COUNT(*) FROM st WHERE nfl_franchise_number IS NULL").fetchone()[0]
        print(f"  NULL after primary fill: {null_after_primary:,}")

    # ------------------------------------------------------------------ #
    # Step 2: Fill residual nulls using alternate-code mapping
    # ------------------------------------------------------------------ #
    print("\n=== Step 2: Alternate-code mapping (TB/GB/NE/SF/KC/NO/LA/LV/JAC/SD) ===")
    for code, fran in ALT_CODE_TO_FRANCHISE.items():
        count = db.execute(
            f"SELECT COUNT(*) FROM st WHERE nfl_team='{code}' AND nfl_franchise_number IS NULL"
        ).fetchone()[0]
        if count:
            print(f"  {code} -> franchise {fran}: {count:,} rows")
            if not dry_run:
                db.execute(
                    f"UPDATE st SET nfl_franchise_number = {fran} "
                    f"WHERE nfl_team='{code}' AND nfl_franchise_number IS NULL"
                )

    # ------------------------------------------------------------------ #
    # Step 3: Same treatment for opponent_nfl_franchise_number
    # ------------------------------------------------------------------ #
    print("\n=== Step 3: opponent_nfl_franchise_number backfill ===")
    opp_null_before = db.execute(
        "SELECT COUNT(*) FROM st WHERE opponent_nfl_franchise_number IS NULL AND opponent_nfl_team IS NOT NULL"
    ).fetchone()[0]
    print(f"  NULL opponent_franchise (where opp_team known): {opp_null_before:,}")

    can_fill_opp = db.execute("""
        SELECT COUNT(*) FROM st s
        JOIN fran_lookup f ON s.opponent_nfl_team = f.team AND CAST(s.year AS INT) = f.year
        WHERE s.opponent_nfl_franchise_number IS NULL AND s.opponent_nfl_team IS NOT NULL
    """).fetchone()[0]
    print(f"  Rows fillable via primary lookup: {can_fill_opp:,}")

    if not dry_run:
        db.execute("""
            UPDATE st
            SET opponent_nfl_franchise_number = (
                SELECT franchise FROM fran_lookup
                WHERE fran_lookup.team = st.opponent_nfl_team
                  AND fran_lookup.year = CAST(st.year AS INT)
                LIMIT 1
            )
            WHERE st.opponent_nfl_franchise_number IS NULL
              AND st.opponent_nfl_team IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM fran_lookup
                WHERE fran_lookup.team = st.opponent_nfl_team
                  AND fran_lookup.year = CAST(st.year AS INT)
              )
        """)
        for code, fran in ALT_CODE_TO_FRANCHISE.items():
            db.execute(
                f"UPDATE st SET opponent_nfl_franchise_number = {fran} "
                f"WHERE opponent_nfl_team='{code}' AND opponent_nfl_franchise_number IS NULL"
            )

    # ------------------------------------------------------------------ #
    # Step 4: Summary
    # ------------------------------------------------------------------ #
    if not dry_run:
        null_after = db.execute("SELECT COUNT(*) FROM st WHERE nfl_franchise_number IS NULL").fetchone()[0]
        null_opp_after = db.execute(
            "SELECT COUNT(*) FROM st WHERE opponent_nfl_franchise_number IS NULL AND opponent_nfl_team IS NOT NULL"
        ).fetchone()[0]
        print(f"\n=== After backfill ===")
        print(f"  Franchise: {null_before:,} NULL -> {null_after:,} NULL  "
              f"(filled {null_before-null_after:,})")
        print(f"  Opp franchise: {opp_null_before:,} NULL -> {null_opp_after:,} NULL")

        # Remaining nulls: who are they?
        residual = db.execute("""
            SELECT nfl_team, COUNT(*) as n, MIN(year), MAX(year)
            FROM st WHERE nfl_franchise_number IS NULL
            GROUP BY nfl_team ORDER BY n DESC LIMIT 20
        """).fetchall()
        if residual:
            print(f"\n  Residual NULL franchise rows by team:")
            for r in residual:
                print(f"    {r[0]}: {r[1]:,} rows  {r[2]:.0f}-{r[3]:.0f}")

    # ------------------------------------------------------------------ #
    # Step 5: Write v26
    # ------------------------------------------------------------------ #
    if dry_run:
        print("\nDRY RUN -- pass --execute to apply and write v26.")
        return

    stamp = utc_stamp()
    out_dir = RELEASES / f"nfl_local_release_franchise_backfill_{stamp}_v26"
    out_dir.mkdir(parents=True)
    (out_dir / "tables").mkdir()
    out_parquet = out_dir / "tables" / "nfl_player_stats_all.parquet"

    print(f"\nWriting v26 to {out_parquet} ...")
    db.execute(f"COPY (SELECT * FROM st) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    total_v26 = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
    print(f"v26 rows: {total_v26:,}  (delta: {total_v26 - total})")
    print(f"\nv26 written: {out_dir}")


if __name__ == "__main__":
    dry = "--execute" not in sys.argv
    main(dry_run=dry)
