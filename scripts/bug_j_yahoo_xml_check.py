"""Yahoo XML reconciliation for Bug J / Cluster C (mohoney_moproblems).

Pulls Yahoo's authoritative per-week team_points + starting roster for
system_team_points_vs_player_sum failures and reports:
  * Yahoo's team_points
  * Sum of started-player points per Yahoo
  * Per-player breakdown (Yahoo's points per player_id)
  * Optional: per-stat-id team breakdown via --team-stats
  * Whether Yahoo's per-player data agrees with ours and which side has the gap.

Modes:
  --settings              dump scoring rules for mohoney 2013 league
  --cluster b             run Cluster B targets (Conklin TE-slot dropouts)
  --cluster c             run Cluster C targets (small-drift gaps)
  --team-stats            also call /team/{key}/stats;type=week to get the
                          per-stat-id team breakdown (lets us reconcile
                          team_points against individual stat contributions)

Run from project root with ../.env loaded.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure repo root + fantasy_football_data_scripts on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

import requests
from xml.etree import ElementTree as ET  # noqa: E402

from multi_league.utils.credential_store import retrieve_league_credentials  # noqa: E402

YAHOO_NS = "{http://fantasysports.yahooapis.com/fantasy/v2/base.rng}"

# Cluster B targets — Conklin TE-slot dropouts (verified shipped 2026-04-28)
TARGETS_B = [
    {"year": 2022, "week": 3, "manager_substr": "Perch", "league_key": "414.l.17571"},
    {"year": 2013, "week": 4, "manager_substr": "Ross", "league_key": "314.l.522018"},
    {"year": 2023, "week": 9, "manager_substr": "Edwin", "league_key": "423.l.6524"},
]

# Cluster C targets — small-drift gaps where started_count=9 but Yahoo
# team_points disagrees with our SUM. Picks span gap directions/magnitudes:
#   Robert 2016 wk9         gap=+12  (largest non-2013 case)
#   Matt-Rufus 2021 wk10    gap=-4.20 (non-rounded; high DEF in lineup)
#   Brendan 2025 wk4        gap=-4    (recent year; Eagles DEF 18 pts)
#   Edwin 2025 wk5          gap=-6    (modern, no DEF outlier)
TARGETS_C = [
    {"year": 2016, "week": 9, "manager_substr": "Robert", "league_key": "359.l.150914"},
    {"year": 2021, "week": 10, "manager_substr": "Rufus", "league_key": "406.l.62856"},
    {"year": 2025, "week": 4, "manager_substr": "Brendan", "league_key": "461.l.14348"},
    {"year": 2025, "week": 5, "manager_substr": "Edwin", "league_key": "461.l.14348"},
]

# Cluster C "still failing after DEF fix" targets — these don't have
# pts_def_td drift in super_table, so the bulk DEF fix doesn't recover
# them. Mix of -6/-4/-3 (we over-score) and +4 (we under-score):
#   Karen 2014 wk4          gap=-6   (49ers DEF 16; 9-starter)
#   Kyle - Bad JuJu 2018 w7 gap=-4   (Bears DEF 7; 9-starter)
#   Brian 2019 wk12         gap=-3   (Lions DEF 11; 9-starter)
#   Ross 2015 wk1           gap=+4.7 (started_count=8, no FLEX in lineup)
#   Matt-Rufus 2014 wk11    gap=+4   (started_count=8, no TE in lineup)
TARGETS_C_RESIDUAL = [
    {"year": 2014, "week": 4, "manager_substr": "Karen", "league_key": "331.l.100081"},
    {"year": 2018, "week": 7, "manager_substr": "Bad JuJu", "league_key": "380.l.249728"},
    {"year": 2019, "week": 12, "manager_substr": "Brian", "league_key": "390.l.527242"},
    {"year": 2015, "week": 1, "manager_substr": "Ross", "league_key": "348.l.8655"},
    {"year": 2014, "week": 11, "manager_substr": "Rufus", "league_key": "331.l.100081"},
]

# Default: Cluster C (Cluster B was verified 2026-04-28). Override with --cluster b.
TARGETS = TARGETS_C


def refresh_access_token(refresh_token: str) -> str:
    client_id = os.environ["YAHOO_CLIENT_ID"]
    client_secret = os.environ["YAHOO_CLIENT_SECRET"]
    resp = requests.post(
        "https://api.login.yahoo.com/oauth2/get_token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def yahoo_get(path: str, access_token: str) -> ET.Element:
    url = f"https://fantasysports.yahooapis.com/fantasy/v2{path}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    resp.raise_for_status()
    return ET.fromstring(resp.text)


def find_text(elem, tag, default=None):
    n = elem.find(f".//{YAHOO_NS}{tag}")
    return n.text if n is not None else default


def find_team_key(scoreboard_root: ET.Element, manager_substr: str) -> tuple[str, str, float] | None:
    """Walk scoreboard; return (team_key, manager_name, team_points) for the
    first team whose nickname / manager name contains manager_substr."""
    for team in scoreboard_root.iter(f"{YAHOO_NS}team"):
        team_key = find_text(team, "team_key")
        team_name = find_text(team, "name")
        # walk managers
        manager_name = None
        for nick in team.iter(f"{YAHOO_NS}nickname"):
            if nick.text and manager_substr.lower() in nick.text.lower():
                manager_name = nick.text
                break
        if manager_name is None and team_name and manager_substr.lower() in team_name.lower():
            manager_name = team_name
        if manager_name is None:
            # check guid section for fallback
            for mgr in team.iter(f"{YAHOO_NS}manager"):
                nick_el = mgr.find(f"{YAHOO_NS}nickname")
                if nick_el is not None and nick_el.text and manager_substr.lower() in nick_el.text.lower():
                    manager_name = nick_el.text
                    break
        if manager_name is None:
            continue
        # team_points lives under team/team_points/total
        tp_el = team.find(f"{YAHOO_NS}team_points/{YAHOO_NS}total")
        team_points = float(tp_el.text) if tp_el is not None and tp_el.text else float("nan")
        return team_key, manager_name, team_points
    return None


def parse_roster_with_points(roster_root: ET.Element) -> list[dict]:
    """Return list of {name, position, selected_position, points, started}."""
    out = []
    for player in roster_root.iter(f"{YAHOO_NS}player"):
        name = find_text(player, "full") or find_text(player, "name")
        # selected_position is the lineup slot Yahoo recorded for this week
        sel_pos_el = player.find(f"{YAHOO_NS}selected_position/{YAHOO_NS}position")
        selected_position = sel_pos_el.text if sel_pos_el is not None else None
        # display_position is the player's eligible NFL position
        display_position = find_text(player, "display_position")
        # player_points/total
        pp_el = player.find(f"{YAHOO_NS}player_points/{YAHOO_NS}total")
        points = float(pp_el.text) if pp_el is not None and pp_el.text else 0.0
        started = (selected_position or "").upper() not in ("BN", "IR", "")
        out.append(
            {
                "name": name,
                "position": display_position,
                "slot": selected_position,
                "points": points,
                "started": started,
            }
        )
    return out


def dump_2013_def_scoring_rules(access_token: str):
    """Dump Yahoo's actual scoring rules for mohoney 2013 to compare against
    what our pipeline captured into league_settings."""
    league_key = "314.l.522018"  # mohoney 2013
    print("=" * 70)
    print(f"  Yahoo league SETTINGS for mohoney 2013 ({league_key})")
    print("=" * 70)
    settings = yahoo_get(f"/league/{league_key}/settings", access_token)
    # walk stat_modifiers/stats/stat tree
    seen = []
    for stat in settings.iter(f"{YAHOO_NS}stat"):
        sid = find_text(stat, "stat_id")
        # the modifier value lives in stat_modifiers/stats/stat/value
        val_el = stat.find(f"{YAHOO_NS}value")
        if val_el is None:
            continue
        val = val_el.text
        if val in (None, "", "0"):
            continue
        # get stat_categories/stats/stat for human name
        seen.append((sid, val))
    # also look up names
    name_lookup = {}
    for stat in settings.iter(f"{YAHOO_NS}stat"):
        sid = find_text(stat, "stat_id")
        nm = find_text(stat, "display_name") or find_text(stat, "name")
        if sid and nm and sid not in name_lookup:
            name_lookup[sid] = nm
    for sid, val in seen:
        nm = name_lookup.get(sid, "?")
        print(f"  stat_id={sid:5}  value={val:8}  name={nm}")
    print()


def fetch_team_stats(team_key: str, week: int, access_token: str) -> list[tuple[str, str, str]]:
    """Fetch /team/{key}/stats;type=week;week=N. Returns list of
    (stat_id, value, name) tuples for stats with non-zero values.

    Yahoo's team-stat endpoint returns the per-stat-id totals that ROLL UP
    into team_points. Reconciling our SUM(starters) against this breakdown
    pinpoints which stat is off (e.g., a missing scoring rule, a player
    misclassification, or a team-level adjustment we don't track).
    """
    root = yahoo_get(f"/team/{team_key}/stats;type=week;week={week}", access_token)
    out = []
    seen_names = {}
    for stat in root.iter(f"{YAHOO_NS}stat"):
        sid = find_text(stat, "stat_id")
        nm = find_text(stat, "display_name") or find_text(stat, "name")
        if sid and nm and sid not in seen_names:
            seen_names[sid] = nm
    for stat in root.iter(f"{YAHOO_NS}stat"):
        sid = find_text(stat, "stat_id")
        val_el = stat.find(f"{YAHOO_NS}value")
        if val_el is None or val_el.text in (None, "", "0", "0.0"):
            continue
        out.append((sid, val_el.text, seen_names.get(sid, "?")))
    return out


def main():
    db_name = "mohoney_moproblems"

    # Resolve TARGETS based on --cluster flag (default: TARGETS_C set above)
    targets = TARGETS
    if "--cluster" in sys.argv:
        idx = sys.argv.index("--cluster")
        if idx + 1 < len(sys.argv):
            choice = sys.argv[idx + 1].lower()
            if choice == "b":
                targets = TARGETS_B
            elif choice == "c":
                targets = TARGETS_C
            elif choice == "c-residual":
                targets = TARGETS_C_RESIDUAL
            else:
                print(f"Unknown --cluster {choice}; expected 'b', 'c', or 'c-residual'")
                sys.exit(1)

    want_team_stats = "--team-stats" in sys.argv

    creds = retrieve_league_credentials(db_name)
    if not creds:
        print(f"FAIL: no credentials found for {db_name}")
        sys.exit(1)
    print(f"Got refresh_token for {db_name} (league_id={creds['league_id']})")

    access = refresh_access_token(creds["refresh_token"])
    print("Refreshed access token OK\n")

    if "--settings" in sys.argv:
        dump_2013_def_scoring_rules(access)
        return

    for t in targets:
        print("=" * 70)
        print(f"  Year {t['year']}  Week {t['week']}  Manager '{t['manager_substr']}'  ({t['league_key']})")
        print("=" * 70)

        # 1. /league/{key}/scoreboard;week=N — find team_key + Yahoo's team_points
        scoreboard = yahoo_get(f"/league/{t['league_key']}/scoreboard;week={t['week']}", access)
        match = find_team_key(scoreboard, t["manager_substr"])
        if match is None:
            print(f"  Could not find team for manager substring '{t['manager_substr']}'\n")
            continue
        team_key, manager_name, yahoo_team_points = match
        print(f"  Yahoo team_key={team_key}  manager='{manager_name}'  yahoo_team_points={yahoo_team_points}")

        # 2. /team/{team_key}/roster;week=N/players/stats;type=week;week=N
        roster = yahoo_get(
            f"/team/{team_key}/roster;week={t['week']}/players/stats;type=week;week={t['week']}",
            access,
        )
        players = parse_roster_with_points(roster)

        started = [p for p in players if p["started"]]
        bench = [p for p in players if not p["started"]]

        started_sum = round(sum(p["points"] for p in started), 2)
        bench_sum = round(sum(p["points"] for p in bench), 2)
        gap = round(yahoo_team_points - started_sum, 2)

        print(f"  Yahoo started-player sum  = {started_sum}  (n={len(started)} starters)")
        print(f"  Yahoo bench-player sum    = {bench_sum}  (n={len(bench)} bench/IR)")
        print(f"  Yahoo team_points - sum   = {gap}")
        print()
        print("  Yahoo starters:")
        for p in started:
            print(f"    [{p['slot']:6}] {p['name']:30} pts={p['points']:6.2f}  pos={p['position']}")
        print()
        print("  Yahoo bench / IR:")
        for p in bench:
            print(f"    [{p['slot']:6}] {p['name']:30} pts={p['points']:6.2f}  pos={p['position']}")
        print()

        if want_team_stats:
            print("  Yahoo team-stat breakdown (/team/{key}/stats;type=week):")
            try:
                stats = fetch_team_stats(team_key, t["week"], access)
                if not stats:
                    print("    (no non-zero stats returned)")
                else:
                    for sid, val, nm in stats:
                        print(f"    stat_id={sid:5}  value={val:8}  name={nm}")
            except Exception as e:
                print(f"    ERROR fetching team stats: {e}")
            print()


if __name__ == "__main__":
    # Load .env manually (don't depend on dotenv being installed)
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip("'").strip('"')
            os.environ.setdefault(k, v)
    main()
