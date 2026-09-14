"""THE IRONCLAD BOARD: weekly schema x witnesses, week-grain honest.

Built from WITNESS_MAP itself (never the stale pivot): for every weekly-plane
column, per lineage root, count total specs and WEEK-CAPABLE specs (declared
week grain OR a shape the week adapters can elevate). Tiers:

  QUORUM     >=2 independent roots week-capable -- the countersignable set:
             a write can demand multi-witness buy-in today
  WEEK-WIRED exactly 1 root week-capable
  SEASON     specs exist, none week-capable yet
  UNWITNESSED no specs

Output: scratchpad HTML for the artifact.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S

OUT = Path(r"C:/Users/joeye/AppData/Local/Temp/claude/d--yahoo-oauth"
           r"/fb08ab7f-c704-4f15-9531-1c67234114e7/scratchpad"
           r"/ironclad_board.html")

WEEK_CAPABLE_SHAPES = {
    "box", "pbp_rollup", "pbp_rollup_week", "team_week", "nflcom_log_week",
    "pbp_team_rollup", "pbp_team_ratio", "pbp_team_return", "pbp_drive_rollup",
    "statscrew_results", "pfr_team_stats", "pfr_drives", "pfr_box_team_ep",
    "pfr_fumble_events", "pfr_scoring_log", "pfr_pbp_detail",
    "newspaper_player_week", "newspaper_team_week", "newspaper_team_pair_week",
    # A ratio over play-grain PBP is elevated to player-week by the same
    # rollup adapter as additive PBP atoms; treating it as season-only hides
    # a valid weekly derivation path.
    "pbp_ratio",
    "pfr_pbp_player_week",
}
GATED = {"dst_points_allowed", "points_allowed", "opponent_points",
         "kickoff_return_long", "punt_return_long", "nfl_team",
         "opponent_nfl_team", "home_away"}


def root_of(source_key: str) -> str:
    k = source_key.lower()
    if k.startswith(("pfr", "ancient_pfr", "ancient_pbp1978")):
        return "pfr"
    if k.startswith("nflcom"):
        return "nflcom"
    if k.startswith(("pbp", "ancient_pbp")):
        return "pbp"
    if k.startswith("statscrew"):
        return "statscrew"
    if "pfa" in k:
        return "pfa"
    if "newspaper" in k:
        return "newspaper"
    if k.startswith("ngs"):
        return "ngs"
    return "internal"


def build() -> None:
    con = duckdb.connect()
    wk = Path(S.latest_v26()).as_posix()
    schema = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{wk}') LIMIT 0").fetchall()]
    lic = W.licensed()
    per = defaultdict(lambda: defaultdict(lambda: [0, 0, 0]))  # col->root->[wk,total,lic]
    for sp in W.WITNESS_MAP:
        if sp.v26_col not in schema:
            continue
        r = root_of(sp.source_key)
        wkcap = (sp.validation_grain == "week" or sp.grain == "week"
                 or sp.shape in WEEK_CAPABLE_SHAPES)
        cell = per[sp.v26_col][r]
        cell[1] += 1
        if wkcap:
            cell[0] += 1
        if (sp.source_key, sp.v26_col) in lic:
            cell[2] += 1
    roots = ["pfr", "nflcom", "pbp", "statscrew", "pfa", "ngs", "newspaper"]
    rows, tiers = [], defaultdict(int)
    for col in sorted(schema):
        if col.startswith(("pts_", "l4_", "fpts")) or "_recomputed_at" in col \
                or "_repaired_at" in col or "_merged_at" in col \
                or "_populated_at" in col:
            continue  # derived/audit-stamp: Stage-7 lane, not witness lanes
        cells = per.get(col, {})
        nweek = sum(1 for r in roots if cells.get(r, [0, 0, 0])[0] > 0)
        total = sum(cells.get(r, [0, 0, 0])[1] for r in roots)
        tier = ("quorum" if nweek >= 2 else
                "week" if nweek == 1 else
                "season" if total else "none")
        tiers[tier] += 1
        rows.append({"col": col, "tier": tier, "gated": col in GATED,
                     "cells": {r: cells.get(r, [0, 0, 0]) for r in roots}})
    payload = {"tiers": dict(tiers), "roots": roots, "rows": rows,
               "generated": "2026-08-02"}

    html = """<title>Ironclad Weekly Board</title>
<style>
:root{--bg:#F6F7F4;--panel:#FFFFFF;--ink:#1E2733;--mut:#66707C;--line:#DDE2DC;
--acc:#2C7A7B;--q:#2F9E63;--w:#2C7A7B;--s:#B7791F;--n:#A65959}
@media (prefers-color-scheme:dark){:root{--bg:#101720;--panel:#16202B;
--ink:#E5EAF0;--mut:#8B98A5;--line:#243140;--acc:#4FB3B3;--q:#48BB78;
--w:#4FB3B3;--s:#D69E2E;--n:#C97B7B}}
:root[data-theme="dark"]{--bg:#101720;--panel:#16202B;--ink:#E5EAF0;
--mut:#8B98A5;--line:#243140;--acc:#4FB3B3;--q:#48BB78;--w:#4FB3B3;
--s:#D69E2E;--n:#C97B7B}
:root[data-theme="light"]{--bg:#F6F7F4;--panel:#FFFFFF;--ink:#1E2733;
--mut:#66707C;--line:#DDE2DC;--acc:#2C7A7B;--q:#2F9E63;--w:#2C7A7B;
--s:#B7791F;--n:#A65959}
body{background:var(--bg);color:var(--ink);margin:0;
font:14px/1.5 system-ui,sans-serif}
header{padding:28px 24px 10px;max-width:1200px;margin:0 auto}
h1{font-family:Georgia,serif;font-weight:600;font-size:26px;margin:0}
.sub{color:var(--mut);margin-top:4px;max-width:70ch}
.tiles{display:flex;gap:10px;flex-wrap:wrap;margin:16px 0 4px}
.tile{background:var(--panel);border:1px solid var(--line);border-radius:6px;
padding:10px 16px;min-width:120px}
.tile b{font-size:22px;font-variant-numeric:tabular-nums;display:block}
.tile span{font-size:11px;letter-spacing:.06em;text-transform:uppercase;
color:var(--mut)}
.tq b{color:var(--q)}.tw b{color:var(--w)}.ts b{color:var(--s)}.tn b{color:var(--n)}
main{max-width:1200px;margin:0 auto;padding:8px 24px 60px}
.wrap{overflow-x:auto;background:var(--panel);border:1px solid var(--line);
border-radius:8px}
table{border-collapse:collapse;width:100%;min-width:860px}
th{font-size:11px;letter-spacing:.06em;text-transform:uppercase;
color:var(--mut);text-align:left;padding:8px 10px;border-bottom:1px solid var(--line);
position:sticky;top:0;background:var(--panel)}
td{padding:5px 10px;border-bottom:1px solid var(--line);
font-variant-numeric:tabular-nums;white-space:nowrap}
td.c{font-family:ui-monospace,monospace;font-size:13px}
.chip{display:inline-block;font-size:10px;letter-spacing:.05em;
padding:1px 7px;border-radius:9px;text-transform:uppercase;font-weight:600}
.quorum{background:color-mix(in srgb,var(--q) 16%,transparent);color:var(--q)}
.week{background:color-mix(in srgb,var(--w) 16%,transparent);color:var(--w)}
.season{background:color-mix(in srgb,var(--s) 16%,transparent);color:var(--s)}
.none{background:color-mix(in srgb,var(--n) 14%,transparent);color:var(--n)}
.g{color:var(--q);font-weight:700;margin-left:5px}
.wk{font-weight:700}.tt{color:var(--mut)}
.zero{color:var(--mut);opacity:.45}
tr.tier-none td.c{color:var(--mut)}
#f{margin:14px 0;padding:9px 12px;background:var(--panel);
border:1px solid var(--line);border-radius:6px;color:var(--mut);
font-size:13px;max-width:78ch}
</style>
<header>
<h1>Ironclad Weekly Board</h1>
<div class="sub">Every weekly-schema column &times; every witness root, built
live from the spec map. Cell = <b class="wk">week-capable</b>/total specs
(licensed in parens). QUORUM = two or more independent roots can countersign
a weekly atom today. &#128274; = a CI drift gate already pins this column.</div>
<div class="tiles" id="tiles"></div>
<div id="f">A season-declared spec on a box-score or play-by-play source
counts as week-capable: the source is per-game by nature and the week
adapters elevate it (2026-08-02 grain ruling). Derived/audit-stamp columns
are excluded here &mdash; they close through the Stage-7 recompute lane, not
witnesses.</div>
</header>
<main><div class="wrap"><table id="t"></table></div></main>
<script>
const D = __DATA__;
const tl = document.getElementById('tiles');
const names = {quorum:['Quorum (countersignable)','tq'],
  week:['Week-wired','tw'], season:['Season-only','ts'], none:['Unwitnessed','tn']};
for (const k of ['quorum','week','season','none']) {
  const d = document.createElement('div');
  d.className = 'tile ' + names[k][1];
  d.innerHTML = '<b>' + (D.tiers[k]||0) + '</b><span>' + names[k][0] + '</span>';
  tl.appendChild(d);
}
const t = document.getElementById('t');
let h = '<tr><th>column</th><th>tier</th>' +
  D.roots.map(r => '<th>' + r + '</th>').join('') + '</tr>';
for (const row of D.rows) {
  h += '<tr class="tier-' + row.tier + '"><td class="c">' + row.col +
    (row.gated ? '<span class="g">&#128274;</span>' : '') + '</td>' +
    '<td><span class="chip ' + row.tier + '">' + row.tier + '</span></td>';
  for (const r of D.roots) {
    const [w, tot, lic] = row.cells[r];
    h += tot ? '<td><span class="wk">' + w + '</span><span class="tt">/' +
      tot + (lic ? ' (' + lic + ')' : '') + '</span></td>'
      : '<td class="zero">&middot;</td>';
  }
  h += '</tr>';
}
t.innerHTML = h;
</script>"""
    OUT.write_text(html.replace("__DATA__", json.dumps(payload)),
                   encoding="utf-8")
    print(json.dumps({"tiers": payload["tiers"],
                      "columns": len(rows), "out": str(OUT)}))


if __name__ == "__main__":
    build()
