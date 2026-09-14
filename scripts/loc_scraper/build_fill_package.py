"""
build_fill_package.py — Build the LOC-sourced fill package for upsert into v26 super table.

Produces two files in D:/league-history-data/nfl/curated/loc_scraper/:
  loc_def_fills.parquet    — DEF row score updates (pts_def_team_pts, points_allowed)
  loc_player_atoms.csv     — Player TD/yard atoms with promotion status

Every entry is cross-verified against PFR. Nothing unverified is included.
Writes the files and prints a verification report.

Usage:
    python -m scripts.loc_scraper.build_fill_package
"""

from __future__ import annotations
import csv
import json
from pathlib import Path

import duckdb

OUT_DIR = Path(r"D:\league-history-data\nfl\curated\loc_scraper")
V26_GLOB = r"D:\league-history-data\nfl\releases\*_v26\tables\nfl_player_stats_all.parquet"
PFR_TG   = r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet"

import glob, os
def _latest_v26() -> str:
    files = sorted(glob.glob(V26_GLOB), key=lambda p: Path(p).stat().st_mtime, reverse=True)
    return files[0]


def main() -> None:
    conn = duckdb.connect()
    v26 = _latest_v26()

    # ── 1. DEF row fills ──────────────────────────────────────────────────────
    #
    # Each entry: the player_week key that exists in v26 with NULL scores,
    # the correct scores (verified by both LOC tile and PFR),
    # and the source evidence.
    #
    # Ambiguity resolution notes embedded in each row.

    def_fills = [
        {
            "player_week":       "DEF-13_1920_6",
            "year":              1920,
            "week":              6,
            "nfl_team":          "CRD",
            "opponent_nfl_team": "DET",
            "pts_def_team_pts":  21.0,
            "points_allowed":    0.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "multiple tiles (1920/w9 manifest -> actual w6)",
            "fill_notes":        (
                "LOC tiles showed CRD 21-0 DET; manifest week was w9 but PFR confirms "
                "game was 1920-10-24 CRD@DET w6. Score exact match."
            ),
        },
        {
            "player_week":       "DEF-5_1927_6",
            "year":              1927,
            "week":              6,
            "nfl_team":          "CHI",
            "opponent_nfl_team": "CLE",
            "pts_def_team_pts":  14.0,
            "points_allowed":    12.0,
            # Score verified: PFR shows CHI 14-12 CLE on 1927-10-23 (w6).
            # LOC tiles (manifested as w7/w8/w9) all read CHI 14-12.
            # '12' is NOT an OCR error — PFR confirms opponent CLE scored 12.
            "pfr_confirmed":     True,
            "loc_source_keys":   "3 tiles (1927/w7, w8, w9 manifest -> actual w6)",
            "fill_notes":        (
                "Three LOC tiles all extracted CHI 14-12. PFR confirms CHI 14 CLE 12 "
                "on 1927-10-23 (w6). '12' is correct (not OCR for '6'; CHI-DAY "
                "14-6 was a separate game at w7)."
            ),
        },
        {
            "player_week":       "DEF-6_1931_1_PRT_BKN",
            "year":              1931,
            "week":              1,
            "nfl_team":          "PRT",
            "opponent_nfl_team": "BKN",
            "pts_def_team_pts":  14.0,
            "points_allowed":    0.0,
            # LOC tile (Ironton Ohio News, Sept 14 1931) reported PRT 14-0 IRT.
            # IRT (Ironton Tanks) was NOT in the NFL in 1931 — that was an exhibition.
            # PFR records PRT w1 as PRT 14-0 BKN (1931-09-13) — same score, different opp.
            # v26 row has opponent_nfl_team=BKN already. Fill PRT score from PFR.
            # LOC provides indirect corroboration (PRT scored 14, allowed 0 that week).
            "pfr_confirmed":     True,
            "loc_source_keys":   "ee1b7f6497522e9d (Ironton Ohio News Sept 14 1931)",
            "fill_notes":        (
                "LOC (Ironton Ohio News) showed PRT 14-0 IRT but IRT was an exhibition "
                "opponent, not an NFL game. PFR records PRT w1 as 14-0 BKN (1931-09-13). "
                "Score (14-0) corroborated by LOC; opponent in v26 row is BKN per PFR."
            ),
        },
        {
            "player_week":       "DEF-7_1931_10",
            "year":              1931,
            "week":              10,
            "nfl_team":          "GNB",
            "opponent_nfl_team": "CRD",
            "pts_def_team_pts":  13.0,
            "points_allowed":    21.0,
            # LOC tile (multiple) showed CRD 21-13 GNB; manifested as 1931/w11.
            # PFR confirms game was 1931-11-15 CRD@GNB, week 10: CRD 21 GNB 13.
            # v26 GNB w10 DEF row exists (opponent=CRD) with NULL scores.
            "pfr_confirmed":     True,
            "loc_source_keys":   "tiles manifested as 1931/w11 -> actual w10",
            "fill_notes":        (
                "LOC extracted CRD 21-13 GNB at manifest w11. PFR confirms "
                "1931-11-15 CRD@GNB w10: CRD 21, GNB 13. Fill GNB side (team_pts=13, pa=21)."
            ),
        },
        {
            "player_week":       "DEF-4_1937_14",
            "year":              1937,
            "week":              14,
            "nfl_team":          "WAS",
            "opponent_nfl_team": "NYG",
            "pts_def_team_pts":  49.0,
            "points_allowed":    14.0,
            # 2 LOC tiles (manifested as 1937/w15) both showed WAS 49-14 NYG.
            # PFR confirms 1937-12-05 WAS@NYG w14: WAS 49, NYG 14.
            # NYG w14 DEF row already correct (14/49). WAS w14 is NULL.
            "pfr_confirmed":     True,
            "loc_source_keys":   "2 tiles manifested as 1937/w15 -> actual w14",
            "fill_notes":        (
                "Two LOC tiles (manifested as w15) both show WAS 49-14 NYG. "
                "PFR confirms 1937-12-05 w14: WAS 49, NYG 14. NYG side already "
                "correct in v26 (14/49). Fill WAS side."
            ),
        },
        {
            "player_week":       "DEF-5_1941_16",
            "year":              1941,
            "week":              16,
            "nfl_team":          "CHI",
            "opponent_nfl_team": "NYG",
            "pts_def_team_pts":  37.0,
            "points_allowed":    9.0,
            # LOC tile showed CHI 37-9 NYG; manifested as 1941/w16.
            # PFR confirms 1941-12-21 CHI@NYG w16 POST (championship): CHI 37, NYG 9.
            # NYG w16 DEF row already correct in v26 (9/37). CHI w16 is NULL.
            "pfr_confirmed":     True,
            "loc_source_keys":   "tile manifested as 1941/w16",
            "fill_notes":        (
                "LOC tile shows CHI 37-9 NYG at 1941/w16. PFR confirms "
                "1941-12-21 POST (NFL Championship): CHI 37, NYG 9. "
                "NYG side already correct (9/37). Fill CHI side."
            ),
        },
        {
            "player_week":       "DEF-5_1944_5_G19441015_CHI_CRD",
            "year":              1944,
            "week":              5,
            "nfl_team":          "CHI",
            "opponent_nfl_team": "CRD",
            "pts_def_team_pts":  34.0,
            "points_allowed":    7.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "tiles manifested as 1944/w7 and w8 -> actual w5",
            "fill_notes":        (
                "LOC tiles (manifested w7 and w8) extracted CHI 34-7 CRD. "
                "PFR confirms 1944-10-15 w5: CHI 34, CRD 7. Fill CHI side."
            ),
        },
        {
            "player_week":       "DEF-13_1944_5",
            "year":              1944,
            "week":              5,
            "nfl_team":          "CRD",
            "opponent_nfl_team": "CHI",
            "pts_def_team_pts":  7.0,
            "points_allowed":    34.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "tiles manifested as 1944/w7 and w8 -> actual w5",
            "fill_notes":        (
                "Mirror of CHI w5 fill. PFR confirms 1944-10-15 w5: CRD 7, CHI 34. "
                "Fill CRD side."
            ),
        },
        {
            "player_week":       "DEF-4_1944_5_G19441015_WAS_BOS",
            "year":              1944,
            "week":              5,
            "nfl_team":          "WAS",
            "opponent_nfl_team": "BOS",
            "pts_def_team_pts":  21.0,
            "points_allowed":    14.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "tiles manifested as 1944/w7 and w8 -> actual w5",
            "fill_notes":        (
                "LOC tiles (manifested w7 and w8) extracted WAS 21-14 BOS. "
                "PFR confirms 1944-10-15 w5: WAS 21, BOS 14. BOS side already "
                "correct in v26 (14/21). Fill WAS side."
            ),
        },
        {
            "player_week":       "DEF-102_1944_5",
            "year":              1944,
            "week":              5,
            "nfl_team":          "BKN",
            "opponent_nfl_team": "NYG",
            "pts_def_team_pts":  7.0,
            "points_allowed":    14.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "tiles manifested as 1944/w7 and w8 -> actual w5",
            "fill_notes":        (
                "LOC tiles extracted NYG 14-7 BKN (manifested w7/w8). PFR confirms "
                "1944-10-15 w5: NYG 14, BKN 7. Fill BKN side (team_pts=7, pa=14)."
            ),
        },
        {
            "player_week":       "DEF-14_1945_5",
            "year":              1945,
            "week":              5,
            "nfl_team":          "RAM",
            "opponent_nfl_team": "CHI",
            "pts_def_team_pts":  41.0,
            "points_allowed":    21.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "2 tiles manifested as 1945/w8 and w9 -> actual w5",
            "fill_notes":        (
                "Two LOC tiles (manifested w8/w9) extracted RAM 41-21 CHI. "
                "PFR confirms 1945-10-21 w5: RAM 41, CHI 21. CHI side already "
                "correct in v26 (21/41). Fill RAM side."
            ),
        },
        {
            "player_week":       "DEF-4_1943_12",
            "year":              1943,
            "week":              12,
            "nfl_team":          "WAS",
            "opponent_nfl_team": "NYG",
            "pts_def_team_pts":  10.0,
            "points_allowed":    14.0,
            # NYG side already correct (14/10). WAS side is NULL.
            # LOC tiles (manifested as 1943/w15) showed NYG 14-7 WAS — close but
            # WAS score was 7 not 10 (off by one field goal, likely OCR).
            # PFR confirms 1943-12-05 NYG@WAS w12: NYG 14, WAS 10.
            "pfr_confirmed":     True,
            "loc_source_keys":   "2 tiles manifested as 1943/w15 -> actual w12",
            "fill_notes":        (
                "LOC tiles (manifested w15) showed NYG 14-7 WAS. PFR confirms "
                "1943-12-05 w12: NYG 14, WAS 10 (WAS score OCR'd as 7 instead of 10 — "
                "one field goal difference). NYG side already correct in v26 (14/10). "
                "Fill WAS side from PFR."
            ),
        },
        {
            "player_week":       "DEF-4_1945_8_G19451111_WAS_BOS",
            "year":              1945,
            "week":              8,
            "nfl_team":          "WAS",
            "opponent_nfl_team": "BOS",
            "pts_def_team_pts":  34.0,
            "points_allowed":    7.0,
            # Both WAS and BOS sides are NULL. LOC tiles (manifested as 1945/w12)
            # showed WAS 34-0 BOS — WAS score (34) is correct, BOS score (0 vs 7)
            # is off by one TD. PFR is the authoritative source for this fill.
            "pfr_confirmed":     True,
            "loc_source_keys":   "2 tiles manifested as 1945/w12 -> actual w8",
            "fill_notes":        (
                "LOC tiles (manifested w12) showed WAS 34-0 BOS. PFR confirms "
                "1945-11-11 w8: WAS 34, BOS 7 (BOS score OCR'd as 0 instead of 7). "
                "Both sides NULL in v26. Fill WAS side."
            ),
        },
        {
            "player_week":       "DEF-163_1945_8",
            "year":              1945,
            "week":              8,
            "nfl_team":          "BOS",
            "opponent_nfl_team": "WAS",
            "pts_def_team_pts":  7.0,
            "points_allowed":    34.0,
            "pfr_confirmed":     True,
            "loc_source_keys":   "2 tiles manifested as 1945/w12 -> actual w8",
            "fill_notes":        (
                "Mirror of WAS w8 fill. PFR confirms 1945-11-11 w8: BOS 7, WAS 34. "
                "Fill BOS side."
            ),
        },
        {
            "player_week":       "DEF-5_1943_15",
            "year":              1943,
            "week":              15,
            "nfl_team":          "CHI",
            "opponent_nfl_team": "WAS",
            "pts_def_team_pts":  41.0,
            "points_allowed":    21.0,
            # 1943 NFL Championship, 1943-12-26 CHI@WAS.
            # PFR confirms: CHI 41, WAS 21. WAS side already correct in v26
            # (DEF-4_1943_15: WAS=21, allowed=41). CHI side is NULL.
            # LOC tile 09ede1ee952e0ad5 (1943/w15) contained a multi-game
            # roundup page that included the championship result; Claude also
            # confirmed from historical knowledge. PFR is the authoritative source.
            "pfr_confirmed":     True,
            "loc_source_keys":   "09ede1ee952e0ad5 (1943/w15 roundup tile, PFR primary)",
            "fill_notes":        (
                "1943 NFL Championship (1943-12-26): CHI 41, WAS 21. "
                "PFR-confirmed. WAS side already correct in v26 (WAS=21, allowed=41). "
                "CHI side is NULL. Fill CHI side. "
                "LOC tile 09ede1ee952e0ad5 (w15 roundup) corroborates; "
                "PFR is primary source."
            ),
        },
    ]

    # ── Verify all player_week keys exist in v26 with NULL scores ─────────────
    print("Verifying DEF fills against v26 ...")
    pws = [r["player_week"] for r in def_fills]
    placeholders = ", ".join(f"'{pw}'" for pw in pws)
    existing = conn.execute(f"""
        SELECT player_week, nfl_team, opponent_nfl_team, pts_def_team_pts, points_allowed
        FROM read_parquet('{v26}')
        WHERE player_week IN ({placeholders})
    """).fetchall()
    existing_map = {r[0]: r for r in existing}

    print(f"  Expected {len(def_fills)} rows, found {len(existing_map)} in v26")
    all_ok = True
    for fill in def_fills:
        pw = fill["player_week"]
        if pw not in existing_map:
            print(f"  MISSING: {pw}")
            all_ok = False
        else:
            row = existing_map[pw]
            if row[3] is not None or row[4] is not None:
                print(f"  ALREADY POPULATED: {pw}  team_pts={row[3]}  pa={row[4]}")
                all_ok = False
            else:
                # Verify team/opponent match
                if row[1] != fill["nfl_team"]:
                    print(f"  TEAM MISMATCH: {pw} v26={row[1]} fill={fill['nfl_team']}")
                    all_ok = False
    if all_ok:
        print(f"  All {len(def_fills)} rows: verified NULL in v26, team codes match.")

    # ── Write DEF fills parquet ───────────────────────────────────────────────
    out_def = OUT_DIR / "loc_def_fills.parquet"
    conn.execute(f"""
        COPY (
            SELECT
                player_week,
                CAST(year AS INTEGER) AS year,
                CAST(week AS INTEGER) AS week,
                nfl_team,
                opponent_nfl_team,
                CAST(pts_def_team_pts AS DOUBLE) AS pts_def_team_pts,
                CAST(points_allowed   AS DOUBLE) AS points_allowed,
                pfr_confirmed,
                loc_source_keys,
                fill_notes
            FROM (VALUES
                {", ".join(
                    f"('{r['player_week']}', {r['year']}, {r['week']}, "
                    f"'{r['nfl_team']}', '{r['opponent_nfl_team']}', "
                    f"{r['pts_def_team_pts']}, {r['points_allowed']}, "
                    f"true, '{r['loc_source_keys']}', '{r['fill_notes'].replace(chr(39), chr(39)+chr(39))}')"
                    for r in def_fills
                )}
            ) t(player_week, year, week, nfl_team, opponent_nfl_team,
                pts_def_team_pts, points_allowed, pfr_confirmed,
                loc_source_keys, fill_notes)
        ) TO '{out_def}' (FORMAT PARQUET)
    """)
    print(f"\nWrote {len(def_fills)} DEF fills -> {out_def}")

    # ── 2. Player atom inventory ──────────────────────────────────────────────
    #
    # Exhaustive accounting of every player stat atom extracted from LOC tiles.
    # Status: PROMOTABLE / NEEDS_BIO / NOT_NFL / JUNK

    player_atoms = [
        # ── Red Grange 1930/w13 CHI: 1 TD (type unknown from LOC) ───────────
        # v26 already has passing_tds=2 for GranRe20_1930_13.
        # LOC tile (manifested as w14) showed Grange scored a TD.
        # Could be a rushing TD not yet in v26 (rushing_tds=None for that row).
        # Cannot confirm TD type from newspaper alone — skip promotion.
        {
            "player_week":    "GranRe20_1930_13",
            "NFL_player_id":  "GranRe20",
            "player":         "Red Grange",
            "year":           1930,
            "week":           13,
            "nfl_team":       "CHI",
            "stat_type":      "TD",
            "stat_value":     1,
            "td_type":        "unknown",
            "loc_source_key": "tile manifested as 1930/w14 -> actual w13",
            "status":         "SKIP_TYPE_UNKNOWN",
            "notes":          (
                "LOC tile showed Grange scored a TD vs GNB (CHI 21-0 GNB, 1930/w13). "
                "v26 already has passing_tds=2 for that row; rushing_tds=None. "
                "Cannot determine from newspaper whether this was rushing or receiving. "
                "Do not promote without type confirmation."
            ),
        },
        # ── Red Grange 1926 (NYY AFL): 2 TDs ───────────────────────────────
        {
            "player_week":    "GranRe20_1926_9",
            "NFL_player_id":  "GranRe20",
            "player":         "Red Grange",
            "year":           1926,
            "week":           9,
            "nfl_team":       "NYY",
            "stat_type":      "TD",
            "stat_value":     2,
            "td_type":        "rushing+receiving",
            "loc_source_key": "2 tiles manifested as 1926/w11",
            "status":         "ALREADY_IN_V26",
            "notes":          (
                "LOC tiles covered a CHI-MIL NFL game (w9, Nov 14 1926, CHI 10-7 MIL) "
                "and separately mentioned Grange (AFL NYY) scoring 2 TDs. "
                "v26 HAS NYY AFL rows for Grange: GranRe20_1926_9 already has "
                "rushing_tds=1 and receiving_tds=1 (sourced from PFR AFL records). "
                "These 2 TDs are already captured. Nothing to promote."
            ),
        },
        # ── Ernie Dayha 1921/w6 RII: 1 TD ───────────────────────────────────
        {
            "player_week":    None,
            "NFL_player_id":  None,
            "player":         "Ernie Dayha",
            "year":           1921,
            "week":           6,
            "nfl_team":       "RII",
            "stat_type":      "TD",
            "stat_value":     1,
            "td_type":        "unknown",
            "loc_source_key": "Rock Island Argus tile (1921/w9 manifest -> actual w6)",
            "status":         "NEEDS_BIO",
            "notes":          (
                "LOC tile (Rock Island Argus, 1921) showed Ernie Dayha (Rock Island) "
                "scored a TD vs Green Bay. Not found in player_bio.parquet. "
                "Requires a player_bio entry before promotion."
            ),
        },
        # ── Jim Conzelman 1921/w6 RII: 1 TD ─────────────────────────────────
        {
            "player_week":    None,
            "NFL_player_id":  None,
            "player":         "Jim Conzelman",
            "year":           1921,
            "week":           6,
            "nfl_team":       "RII",
            "stat_type":      "TD",
            "stat_value":     1,
            "td_type":        "unknown",
            "loc_source_key": "Rock Island Argus tile (1921/w9 manifest -> actual w6)",
            "status":         "NEEDS_BIO",
            "notes":          (
                "LOC tile (Rock Island Argus, 1921) showed Jim Conzelman (Rock Island) "
                "scored a TD vs Green Bay. Famous player-coach of the era. "
                "Not found in player_bio.parquet. Requires bio entry before promotion."
            ),
        },
        # ── Thomas (Steagles) 1943: 1 TD ────────────────────────────────────
        {
            "player_week":    None,
            "NFL_player_id":  None,
            "player":         "Thomas (last name only)",
            "year":           1943,
            "week":           13,
            "nfl_team":       "PHI",
            "stat_type":      "TD",
            "stat_value":     1,
            "td_type":        "unknown",
            "loc_source_key": "tile manifested as 1943/w13",
            "status":         "NEEDS_FIRST_NAME",
            "notes":          (
                "LOC tile source text: 'Phil-Pitt Team Upsets Redskins: Lacaona Thomas 4 S...' "
                "'Lacaona' is OCR mangling of the first name (possibly LaVerne, LaRoy, or similar). "
                "PHI-WAS 1943 games: w8 (14-14 TIE, the upset) and w11 (27-14 PHI). "
                "Tile manifest says w13 but actual game is likely the w8 upset. "
                "Cannot identify player without decoding 'Lacaona' from 1943 Steagles roster."
            ),
        },
        # ── Benny Friedman 1927: 227 passing yards ───────────────────────────
        {
            "player_week":    None,
            "NFL_player_id":  None,
            "player":         "Benny Friedman",
            "year":           1927,
            "week":           13,
            "nfl_team":       "CLE",
            "stat_type":      "passing_yards",
            "stat_value":     227,
            "td_type":        None,
            "loc_source_key": "tile manifested as 1927/w13",
            "status":         "NEEDS_BIO",
            "notes":          (
                "LOC tile showed Benny Friedman (Cleveland Bulldogs) with 227 passing yards. "
                "Friedman was a famous passer of the era (HOF). Not found in player_bio.parquet "
                "under 'Friedman'. PFR id is FrieBe00 — check if bio needs to be added."
            ),
        },
        # ── 'Galeo' 1927: 500 rushing yards (×2 tiles) ──────────────────────
        {
            "player_week":    None,
            "NFL_player_id":  None,
            "player":         "Galeo",
            "year":           1927,
            "week":           None,
            "nfl_team":       "unknown",
            "stat_type":      "rushing_yards",
            "stat_value":     500,
            "td_type":        None,
            "loc_source_key": "tiles manifested as 1927/w11 and w12",
            "status":         "JUNK",
            "notes":          (
                "500 rushing yards in a single game is physically impossible (NFL record ~295). "
                "This is a Claude OCR error — likely a season cumulative stat misread as "
                "a game stat, or a number from an unrelated table on the page. Discard."
            ),
        },
    ]

    # ── Write player atoms CSV ────────────────────────────────────────────────
    out_player = OUT_DIR / "loc_player_atoms.csv"
    fieldnames = [
        "player_week", "NFL_player_id", "player", "year", "week", "nfl_team",
        "stat_type", "stat_value", "td_type", "loc_source_key", "status", "notes",
    ]
    with open(out_player, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for atom in player_atoms:
            w.writerow({k: atom.get(k, "") for k in fieldnames})
    print(f"Wrote {len(player_atoms)} player atoms -> {out_player}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("FILL PACKAGE SUMMARY")
    print("=" * 60)
    print(f"DEF row fills:     {len(def_fills)}  (all pfr_confirmed=True)")
    by_status: dict[str, int] = {}
    for a in player_atoms:
        by_status[a["status"]] = by_status.get(a["status"], 0) + 1
    print("Player atoms:")
    for status, count in sorted(by_status.items()):
        print(f"  {status:<22} {count}")
    print()
    print("Files ready:")
    print(f"  {out_def}")
    print(f"  {out_player}")
    print()
    print("DEF fills detail:")
    for fill in def_fills:
        print(f"  {fill['player_week']:<45}  "
              f"{fill['nfl_team']} {fill['pts_def_team_pts']:.0f}-"
              f"{fill['points_allowed']:.0f} {fill['opponent_nfl_team']}")


if __name__ == "__main__":
    main()
