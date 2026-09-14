"""gap_ledgers.py -- curate the unused-witness-atom ADD ledger + era-floor table."""
import json
import re
from pathlib import Path

SCRATCH = Path(r"D:\league-history-data\nfl\derived\validation\witness_audit_2026_07_16")
V2 = json.loads((SCRATCH / "witness_contracts_v2.json").read_text())
MM = json.loads((SCRATCH / "master_witness_matrix.json").read_text())

JUNK = re.compile(
    r"(__)|(_count$)|(_rows$)|(_status$)|(_bar$)|(^confidence)|(^max_confidence)|(^decision)"
    r"|(^promotion)|(^review)|(^route_to)|(^created_)|(^target_)|(^source_)|(^evidence)"
    r"|(^live_)|(^detailed_)|(^team_game_num$)|(^uniform_number$)|(^experience$)"
    r"|(^g$)|(^gs$)|(^games$)|(^games_started$)|(^event_rows$)|(^bio_lookup)|(^mapped_to)"
    r"|(^index_)|(^is_starter$)|(^draft_year$)|(^year_max$)|(^stat_value$)|(^points$)|(^yards$)")

unused = MM["unused_witness_atoms"]
groups: dict[str, list] = {}
for atom, info in unused.items():
    if JUNK.search(atom):
        continue
    wits = info["witnesses"]
    fam = sorted({w.split(":", 1)[0] for w in wits})
    key = "+".join(fam)
    era = info.get("game_era") or info.get("season_era")
    groups.setdefault(key, []).append((atom, era, info.get("game_era"), info.get("season_era"), wits[:3]))

print("=" * 100)
print("UNUSED WITNESSED ATOMS (curated), grouped by witness family")
print("=" * 100)
for key in sorted(groups, key=lambda k: -len(groups[k])):
    rows = sorted(groups[key])
    print(f"\n### {key}  ({len(rows)} atoms)")
    for atom, era, ge, se, wits in rows:
        print(f"   {atom:42} game:{str(ge or '-'):>6} season:{str(se or '-'):>6}")

# ---- era floor table for headline atoms
HEADLINE = ["passing_yards", "passing_tds", "passing_interceptions", "completions", "attempts",
            "sacks_suffered", "sack_yards_lost", "passing_long", "passing_first_downs",
            "carries", "rushing_yards", "rushing_tds", "rushing_long", "rushing_first_downs",
            "receptions", "receiving_yards", "receiving_tds", "receiving_long", "targets",
            "receiving_first_downs", "fumbles", "fumbles_lost", "def_interceptions",
            "def_interception_yards", "def_int_ret_td", "def_sacks", "def_tackles_solo",
            "def_tackle_assists", "def_tackles_with_assist", "def_tackles_for_loss",
            "def_pass_defended", "def_qb_hits", "def_fumbles_forced", "fum_rec", "fum_rec_yds",
            "fum_ret_td", "def_safeties", "fg_made", "fg_att", "fg_long", "pat_made", "pat_att",
            "punts", "punt_yards", "punt_long", "punts_blocked", "kickoff_returns",
            "kickoff_return_yards", "kickoff_return_tds", "punt_returns", "punt_return_yards",
            "punt_return_tds", "passing_epa", "rushing_epa", "receiving_epa", "passing_cpoe",
            "passing_air_yards", "receiving_air_yards", "targets"]

idx: dict[str, list] = {}
for wname, c in V2["contracts"].items():
    if c.get("error") or c.get("cls") in ("subject_history",):
        continue
    for atom, info in c.get("atoms", {}).items():
        idx.setdefault(atom, []).append((wname, c["grain"], info.get("era_min"), info.get("era_max"), c.get("cls")))

print("\n" + "=" * 100)
print("ERA FLOORS -- earliest GAME-grain vs earliest SEASON-grain witness per headline atom")
print("=" * 100)
print(f"{'atom':30} {'earliest game witness':46} {'earliest season witness':46}")
for a in dict.fromkeys(HEADLINE):
    hits = idx.get(a, [])
    game = sorted([h for h in hits if h[1] == "game" and h[2]], key=lambda h: h[2])
    season = sorted([h for h in hits if h[1] in ("season", "season_post") and h[2]], key=lambda h: h[2])
    g = f"{game[0][2]} {game[0][0]}" if game else "--"
    s = f"{season[0][2]} {season[0][0]}" if season else "--"
    print(f"{a:30} {g:46} {s:46}")

# nflcom logs harvest coverage (era of the game-grain fumbles witness so far)
print("\nNFL.com logs harvest coverage so far (game grain):")
for w in sorted(V2["contracts"]):
    if w.startswith("nflcom_logs"):
        c = V2["contracts"][w]
        if c.get("error"):
            continue
        f = c["atoms"].get("fumbles") or c["atoms"].get("fumbles_lost")
        n = c.get("n_rows")
        if f:
            print(f"   {w:44} rows={n:>9,} fumbles era {f.get('era_min')}-{f.get('era_max')} nonzero={f.get('total_nonzero')}")
        else:
            print(f"   {w:44} rows={n:>9,} (no fumbles atom)")
