"""Diagnostic: pull ESPN league 364315 (national_ca_az_tx_ffb_league) directly
with decrypted creds and inspect what box_scores / scoreboard returns for each
year — especially 2024 and 2025 which silently dropped during fleet import.

This bypasses the matchup builder's zero-score skip rule so we can see exactly
what ESPN serves back and whether the cookies are even being applied.
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv(".env")

# Force motherduck reads since creds live in ___ops.main.espn_leagues
os.environ.setdefault("DATABASE_BACKEND", "motherduck")
os.environ.setdefault("MOTHERDUCK_TOKEN", os.environ.get("MOTHERDUCK_TOKEN", ""))
# credential_store reads CREDENTIAL_ENCRYPTION_KEY first; .env only sets
# CREDENTIAL_ENCRYPTION_KEY_NEW so make both visible.
key_new = os.environ.get("CREDENTIAL_ENCRYPTION_KEY_NEW")
if key_new and not os.environ.get("CREDENTIAL_ENCRYPTION_KEY"):
    os.environ["CREDENTIAL_ENCRYPTION_KEY"] = key_new

sys.path.insert(0, "fantasy_football_data_scripts")

from multi_league.core.db_reader import get_reader

DB = "national_ca_az_tx_ffb_league"
reader = get_reader()
rows = reader.query(
    "SELECT espn_league_id, league_name, "
    "encrypted_espn_s2, encrypted_swid "
    "FROM main.espn_leagues WHERE database_name = '" + DB + "'",
    database="___ops",
)
if not rows:
    print(f"[FATAL] No row in ___ops.main.espn_leagues for {DB}")
    sys.exit(1)
row = rows[0]
print(f"[row] espn_league_id={row['espn_league_id']}  league_name={row['league_name']!r}")
print(
    f"[row] encrypted_espn_s2 is None? {row['encrypted_espn_s2'] is None}, "
    f"encrypted_swid is None? {row['encrypted_swid'] is None}"
)

# Try decrypting; if both are None, fall back to public (no-auth) access — same
# as the fleet import.
creds = {"league_id": int(row["espn_league_id"]), "espn_s2": None, "swid": None}
if row["encrypted_espn_s2"] and row["encrypted_swid"]:
    from multi_league.utils.credential_store import decrypt_token, get_encryption_key

    key = get_encryption_key()
    creds["espn_s2"] = decrypt_token(row["encrypted_espn_s2"], key)
    creds["swid"] = decrypt_token(row["encrypted_swid"], key)
    print(f"[creds] DECRYPTED: espn_s2 len={len(creds['espn_s2'])}  swid={creds['swid']}")
else:
    print("[creds] STORED AS PUBLIC — no cookies. Matching what fleet import does.")

from espn_api.football import League

YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

for year in YEARS:
    print(f"\n========== {year} ==========")
    kwargs = {"league_id": creds["league_id"], "year": year}
    if creds["espn_s2"]:
        kwargs["espn_s2"] = creds["espn_s2"]
    if creds["swid"]:
        kwargs["swid"] = creds["swid"]
    try:
        lg = League(**kwargs)
    except Exception as e:
        print(f"  [FAIL] League() raised: {type(e).__name__}: {e}")
        continue

    cur = getattr(lg, "current_week", None)
    fsp = getattr(lg, "finalScoringPeriod", None)
    spi = getattr(lg, "scoringPeriodId", None)
    cmp_ = getattr(lg, "currentMatchupPeriod", None)
    teams = getattr(lg, "teams", []) or []
    print(f"  current_week={cur}  finalScoringPeriod={fsp}  scoringPeriodId={spi}  currentMatchupPeriod={cmp_}")
    print(f"  teams={len(teams)}")

    # Apply the same patch as ESPNAPIClient.get_league: if current_week<1 but
    # finalScoringPeriod>0, override so box_scores() loop doesn't no-op.
    if (cur or 0) < 1 and (fsp or 0) > 0:
        print(f"  [patch] overriding current_week 0 -> {fsp}")
        lg.current_week = fsp
        if (cmp_ or 0) < 1:
            lg.currentMatchupPeriod = fsp

    # Probe weeks 1..18 for box_scores and report scores
    for wk in range(1, 19):
        try:
            bs_list = lg.box_scores(wk)
        except Exception as e:
            print(f"    week {wk:2d}: box_scores raised {type(e).__name__}: {e}")
            continue
        if not bs_list:
            print(f"    week {wk:2d}: empty box_scores list")
            continue
        # Compute score stats
        all_zero = all(
            (getattr(b, "home_score", 0) or 0) == 0 and (getattr(b, "away_score", 0) or 0) == 0 for b in bs_list
        )
        max_score = max((getattr(b, "home_score", 0) or 0, getattr(b, "away_score", 0) or 0) for b in bs_list)
        print(f"    week {wk:2d}: {len(bs_list)} matchups, " f"all_zero={all_zero}, max_score_pair={max_score}")
        # First 2 weeks — print sample
        if wk <= 2 and bs_list:
            b0 = bs_list[0]
            ht = getattr(b0, "home_team", None)
            at = getattr(b0, "away_team", None)
            print(
                f"        sample: {getattr(ht, 'team_name', '?')!r} {getattr(b0, 'home_score', '?')} vs "
                f"{getattr(at, 'team_name', '?')!r} {getattr(b0, 'away_score', '?')}  "
                f"is_playoff={getattr(b0, 'is_playoff', '?')}  matchup_type={getattr(b0, 'matchup_type', '?')!r}"
            )
        # Stop probing if we've hit the natural end
        if wk >= (lg.current_week or 18):
            break
