#!/usr/bin/env python3
"""Draft dossier facts — turns mined signals into matter-of-fact scouting
blurbs on a fixed skeleton, the way the newspaper-recap engine does.

The skeleton keeps it honest and un-sloppy: a fixed catalog of insight types,
each with ONE plain factual frame and a short pool of actionable "so-what"
clauses. There are no headline flourishes — every blurb is a plain statement:
the behavior, the number, whether it works, and what to do about it. Variety
comes from a manager having several DIFFERENT facts, not from rephrasing one
fact five ways. The real numbers (multiples, counts, seasons, player/stat
names) drop into slots; confidence is plain language derived from the real
sample (Wilson lower bound + repeatability), never invented.

Output per manager:
    {
      "facts": [ { text, tone, kind } ],  # multiple distinct facts, best first
      "also":  "compact trivia footer" | ""
    }
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from multi_league.transformations.draft.construction_miner import _LABEL_META
from multi_league.transformations.draft.nugget_renderer import (
    _PLACE_COLUMNS,
    _TEAM_COLUMNS,
    _TEAM_NAMES,
    _noun_phrase,
)


# ── measured predictability (out-of-sample backtest) ─────────────────────
# docs/draft-predictability.json is built by the fleet backtest: for each
# behavior it holds the next-year lift over base rate and a reliability tier.
# A behavior only surfaces as a confident claim if it earned a tier here; the
# lift becomes the confidence the dossier shows. Everything else is hedged.
def _load_predictability() -> dict:
    node = Path(__file__).resolve()
    while node != node.parent:
        p = node / "docs" / "draft-predictability.json"
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8")).get("behaviors", {})
            except Exception:  # pragma: no cover
                return {}
        node = node.parent
    return {}


_PREDICTABILITY = _load_predictability()
# Behaviors that cleared a real out-of-sample edge (strong/lean). Slot-driven
# noise (opens_rb/wr, robust_rb, …) scored ~1.1x and is intentionally absent.
_VALIDATED = {k: v for k, v in _PREDICTABILITY.items() if v.get("tier") in ("strong", "lean")}


def _behavior_lift_phrase(key: str) -> str:
    """Honest confidence, from the measured next-year lift."""
    info = _VALIDATED.get(key)
    if not info:
        return ""
    lift = info.get("lift") or 0
    if lift >= 3:
        return "a tendency that sticks year to year far more than the field"
    return f"a tendency that repeats about {lift:.1f}x the league's base rate"

_PLAYER_TYPE_COLUMNS = {
    "height", "weight", "ras_score", "forty", "bench", "vertical",
    "broad_jump", "age_at_draft", "is_undrafted", "cone",
}
_UP_TIERS = {"elite", "strong", "high", "big"}
_DOWN_TIERS = {"weak", "poor", "low", "middling"}


# ── numeric helpers ──────────────────────────────────────────────────────
def _num(row: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key, default))
    except (TypeError, ValueError):
        return default


def _multiple(row: dict) -> str | None:
    lift = max(_num(row, "lift", 1), _num(row, "count_lift", 1))
    if lift >= 2.6:
        return f"{round(lift)}x"
    if lift >= 1.5:
        return "twice"
    return None


def _pick(pool: list[str], seed: str) -> str:
    if not pool:
        return ""
    h = 0
    for ch in seed:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return pool[h % len(pool)]


def interpolate(template: str, data: dict) -> str:
    def repl(m: re.Match) -> str:
        v = data.get(m.group(1))
        return m.group(0) if v is None else str(v)

    out = re.sub(r"\{(\w+)\}", repl, template)
    out = re.sub(r"\s+", " ", out).strip()
    out = re.sub(r"\s+([.,;:])", r"\1", out)
    out = re.sub(r"([.,]){2,}", r"\1", out)
    return out


# ── subjects ─────────────────────────────────────────────────────────────
_MARKET_SUBJECT = {
    "big reach": "reaching a full round or more above ADP for the players they want",
    "reach": "taking their targets a little above ADP",
    "big value": "waiting for big ADP fallers and pouncing",
    "value": "drafting off the value side of the board",
    "way over market": "paying well over market price",
    "over market": "paying over market price",
    "big bargain": "hunting steep bargains in auctions",
    "bargain": "shopping below market price",
}
# Validated construction behaviors only (those that earned a tier in the
# backtest). Slot-driven "opens with a RB/WR", robust-RB, hero-RB, wr-heavy,
# te-early etc. failed out of sample and are deliberately not here. Each entry:
# (label that fits "leans {label}", honest hedged so-what).
_BUILD_LABEL = {
    "zero_rb": ("Zero-RB — no early running back",
                "The early backs are a bit likelier to reach you — but they still take one most years."),
    "early_qb": ("toward an early quarterback",
                 "A top QB may go a touch sooner than the room expects."),
    "late_qb": ("toward waiting on quarterback",
                "They'll usually leave the early QBs for everyone else."),
    "opens_qb": ("toward opening with a quarterback",
                 "Their first pick is a QB more often than most in the room."),
    "stars_and_scrubs": ("stars-and-scrubs — most of the budget on a few players",
                         "Expect a top-heavy roster; this is the stickiest trait in the data."),
}
# Hedged, observational so-whats — prior-production chasing is near-universal
# and not yet backtested, so no deterministic counter-move is claimed.
_GROUP_STAT = {
    "rushing": ("rushing", ["a tilt toward proven rushers.", "leaning on last year's lead backs."]),
    "receiving": ("receiving", ["a tilt toward high-volume receivers.", "leaning on last year's target hogs."]),
    "passing": ("passing", ["a tilt toward established passers.", "leaning on last year's passing leaders."]),
    "efficiency": ("efficiency", ["a tilt toward the per-play standouts.", "chasing last year's efficiency darlings."]),
    "scoring": ("scoring", ["a tilt toward last year's point totals.", "leaning on last season's scorers."]),
}
_STAT_GROUPS = [
    ("efficiency", ("per-touch", "per-target", "per-reception", "per touch", "efficiency", "yards per", "y/r", "y/t")),
    ("rushing", ("rush", "carr", "ground")),
    ("receiving", ("recept", "target", "receiv", "catch", "yac", "yards-after", "intermediate", "deep", "chain-moving", "first down", "first-down", "slot")),
    ("passing", ("passing", "completion", "interception")),
    ("scoring", ("ppg", "fantasy", "touchdown", "points", "scoring")),
]


def _stat_group(stat: str) -> str:
    low = stat.lower()
    for group, keys in _STAT_GROUPS:
        if any(k in low for k in keys):
            return group
    return "scoring"


def _wide_category(feature_type: str) -> str:
    base = feature_type.split(".", 1)[-1]
    for suf in ("_tier", "_bin"):
        if base.endswith(suf):
            base = base[: -len(suf)]
    if base in ("market_reach", "market_price"):
        return "market"
    if base == "team_stack":
        return "stacking"
    if base in _TEAM_COLUMNS:
        return "franchise"
    if base in _PLACE_COLUMNS or base == "conference":
        return "school"
    if base in _PLAYER_TYPE_COLUMNS:
        return "player_type"
    return "production"


def _stat_of(row: dict) -> tuple[str | None, str | None]:
    noun = _noun_phrase(str(row.get("feature_type", "")), str(row.get("feature_value", "")), row.get("feature_label"))
    if not noun:
        return None, None
    m = re.match(r"^players coming off an? (\w+) (.+?) season$", noun)
    if not m:
        return None, None
    tier, stat = m.group(1).lower(), m.group(2)
    if "/" in stat or "yds" in stat.lower() or ":" in stat:
        return None, None
    if tier in _UP_TIERS:
        return stat, "up"
    if tier in _DOWN_TIERS:
        return stat, "down"
    return None, None


def _trait_of(row: dict) -> str | None:
    noun = _noun_phrase(str(row.get("feature_type", "")), str(row.get("feature_value", "")), row.get("feature_label"))
    if not noun:
        return None
    trait = re.sub(r" players$", "", noun)
    if "/" in trait or ":" in trait:
        return None
    return trait


def _and(words: list[str]) -> str:
    words = [w for w in words if w]
    if len(words) <= 1:
        return words[0] if words else ""
    if len(words) == 2:
        return f"{words[0]} and {words[1]}"
    return ", ".join(words[:-1]) + f", and {words[-1]}"


def _count_phrase(row: dict) -> str:
    picks = int(_num(row, "picks"))
    years = int(_num(row, "years_seen"))
    unit = "auctions" if str(row.get("feature_type", "")).endswith(("stars_and_scrubs", "balanced_build")) else "drafts"
    return f"{picks} of {years} {unit}" if years else ""


# Family headline weights. VALIDATED, backtested edges (build) lead. Everything
# else is a hedged historical lean and ranks below them; universal
# prior-production ranks last.
_FAMILY_WEIGHT = {
    "build": 46, "market": 32, "tempo": 28, "stacking": 26,
    "production": 18, "production_down": 20, "player_type": 22, "tell": 8,
}


def _score(row: dict, family: str, verdict: str | None, tier: str | None = None) -> float:
    rep = max(_num(row, "count_repeatability"), _num(row, "repeatability"))
    tier_bonus = {"strong": 8, "lean": 4}.get(tier or "", 0)
    return (
        _FAMILY_WEIGHT.get(family, 20)
        + tier_bonus
        + rep * 3
        + (3 if verdict == "good" else 1 if verdict == "bad" else 0)
        + _num(row, "surface_score") * 0.3
    )


# ── fact frames ──────────────────────────────────────────────────────────
def _fact_build(row, manager, seed):
    """Only surfaces construction behaviors that earned a real out-of-sample
    edge (see _VALIDATED); the measured lift is the confidence shown."""
    sub = str(row.get("feature_type", "")).split(".", 1)[-1]
    if sub not in _VALIDATED or sub not in _BUILD_LABEL:
        return None
    label, so_what = _BUILD_LABEL[sub]
    tier = _VALIDATED[sub]["tier"]
    # Verb reflects THIS manager's own rate; the lift phrase reflects how well
    # the behavior predicts fleet-wide. (A 3-of-7 manager doesn't "clearly" do
    # anything, even if the behavior is a strong signal in general.)
    rep = max(_num(row, "count_repeatability"), _num(row, "repeatability"))
    verb = "clearly leans" if rep >= 0.6 else "leans"
    text = interpolate("{manager} {verb} {label} ({count}) — {lift}. {so_what}", {
        "manager": manager, "verb": verb, "label": label, "count": _count_phrase(row),
        "lift": _behavior_lift_phrase(sub), "so_what": so_what,
    })
    return _score(row, "build", None, tier), {"text": text, "tone": None, "kind": "build"}


def _fact_tempo(row, manager, seed):
    """Consistency (opening stability) failed the backtest, so it never
    surfaces. Run-reactivity isn't yet backtested — hedged, no confidence
    claim, lower priority."""
    ft = str(row.get("feature_type", ""))
    fv = str(row.get("feature_value", ""))
    if ft.endswith("consistency"):
        return None
    subject = {"chase": "leans into position runs — tends to jump in when a spot starts flying",
               "fade": "tends to fade position runs — hangs back while the room piles in"}.get(fv)
    if not subject:
        return None
    text = interpolate("{manager} {subject}. {so_what}", {
        "manager": manager, "subject": subject,
        "so_what": "A historical read we haven't yet validated year to year."})
    return _score(row, "tempo", None), {"text": text, "tone": None, "kind": "tempo"}


def _fact_market(row, manager, seed):
    """Market lean — plausibly a stable personality trait, but not yet
    backtested here (the merged draft table has no ADP), so it's hedged with no
    confidence claim."""
    fv = str(row.get("feature_value", ""))
    subject = _MARKET_SUBJECT.get(fv)
    if not subject:
        return None
    text = f"{manager} has a history of {subject} — a lean in past drafts we haven't yet validated forward."
    return _score(row, "market", None), {"text": text, "tone": None, "kind": "market"}


def _fact_stacking(row, manager, seed):
    text = f"{manager} has stacked a quarterback with their own pass-catchers — worth watching, not yet validated."
    return _score(row, "stacking", None), {"text": text, "tone": None, "kind": "stacking"}


def _facts_production(prod_up, prod_down, manager, seedbase):
    """Up to two up-year facts (top two stat groups) plus one buy-low fact."""
    facts = []

    by_group: dict[str, dict] = {}
    for r in prod_up:
        g = _stat_group(r["_stat"])
        if g not in by_group or _num(r, "count_lift") > _num(by_group[g], "count_lift"):
            by_group[g] = r
    # Historical over-index (true), phrased as a lean — prior-production chasing
    # is common and not yet backtested for predictiveness.
    prod_frames = [
        "{manager} has drafted last year's {label} leaders at {mult} the league rate — {so_what}",
        "{manager} historically pays up for {label} coming off big years, {mult} the field — {so_what}",
    ]
    ordered = sorted(by_group.items(), key=lambda kv: -max(_num(kv[1], "lift"), _num(kv[1], "count_lift")))
    for i, (group, r) in enumerate(ordered[:2]):
        seed = f"{seedbase}-prod-{group}"
        mult = _multiple(r) or "an elevated"
        label, sowhats = _GROUP_STAT.get(group, (group, ["a tilt toward last year's production."]))
        text = interpolate(prod_frames[i], {
            "manager": manager, "label": label, "mult": mult, "so_what": _pick(sowhats, seed)})
        facts.append((_score(r, "production", None), {"text": text, "tone": None, "kind": "production"}))

    if prod_down:
        best = {}
        for r in prod_down:
            g = _stat_group(r["_stat"])
            if g not in best or _num(r, "count_lift") > _num(best[g], "count_lift"):
                best[g] = r
        stats = _and([r["_stat"] for r in sorted(best.values(), key=lambda r: -_num(r, "count_lift"))[:2]])
        text = interpolate(
            "{manager} has leaned buy-low, targeting {stats} coming off down years — a contrarian streak worth noting.",
            {"manager": manager, "stats": stats})
        facts.append((_score(prod_down[0], "production_down", None), {"text": text, "tone": None, "kind": "production"}))
    return facts


# ── trivia (Also footer) ─────────────────────────────────────────────────
def _trivia_phrase(row: dict, cat: str) -> str | None:
    fv = str(row.get("feature_value", ""))
    if cat == "franchise":
        return f"favors {_TEAM_NAMES.get(fv.upper(), fv.upper())} players"
    if cat == "school":
        noun = _noun_phrase(str(row.get("feature_type", "")), fv, row.get("feature_label"))
        return f"leans {re.sub(r'^players out of ', '', noun or fv)}"
    if str(row.get("feature_type", "")) == "construction.repeat_player":
        return f"devoted to {fv} ({int(_num(row, 'years_seen'))} seasons)"
    return None


def build_manager_promo(rows: list[dict], manager: str, db_name: str, rotor=None) -> dict:
    """Assemble one manager's dossier: multiple matter-of-fact blurbs (best
    first) plus a compact trivia footer."""
    seedbase = f"{db_name}-{manager}"
    facts: list[tuple[float, dict]] = []
    trivia: dict[str, dict] = {}
    prod_up: list[dict] = []
    prod_down: list[dict] = []
    ptype: list[dict] = []

    for row in rows:
        ft = str(row.get("feature_type", ""))
        seed = f"{seedbase}-{ft}-{row.get('feature_value', '')}"

        if ft.startswith("construction."):
            sub = ft.split(".", 1)[-1]
            if sub == "repeat_player":
                trivia.setdefault("keeper", row)
            elif sub in ("run_reactivity", "consistency"):
                f = _fact_tempo(row, manager, seed)
                if f:
                    facts.append(f)
            else:
                f = _fact_build(row, manager, seed)
                if f:
                    facts.append(f)
            continue

        cat = _wide_category(ft)
        if cat == "market":
            f = _fact_market(row, manager, seed)
            if f:
                facts.append(f)
        elif cat == "stacking":
            facts.append(_fact_stacking(row, manager, seed))
        elif cat == "production":
            stat, direction = _stat_of(row)
            if stat:
                (prod_up if direction == "up" else prod_down).append({**row, "_stat": stat})
        elif cat == "player_type":
            ptype.append(row)
        elif cat in ("franchise", "school"):
            cur = trivia.get(cat)
            if cur is None or _num(row, "surface_score") > _num(cur, "surface_score"):
                trivia[cat] = row

    facts += _facts_production(prod_up, prod_down, manager, seedbase)

    # Player type as its own plain fact — but never list contradictory poles
    # (a manager who takes tall WRs and short RBs isn't "tall and short").
    if ptype:
        # Trait forms are post-strip (see _trait_of: drops trailing " players").
        opposites = {
            "towering": "short-for-the-position",
            "short-for-the-position": "towering",
            "big-bodied": "undersized",
            "undersized": "big-bodied",
            "true burners": "slow-forty",
            "slow-forty": "true burners",
            "elite athletes": "low-RAS athletes",
            "low-RAS athletes": "elite athletes",
        }
        traits: list[str] = []
        chosen: set[str] = set()
        for r in sorted(ptype, key=lambda r: -_num(r, "surface_score")):
            t = _trait_of(r)
            if not t or t in chosen or opposites.get(t) in chosen:
                continue
            traits.append(t)
            chosen.add(t)
            if len(traits) >= 2:
                break
        if traits:
            text = f"{manager} has skewed toward {_and(traits)} — a physical type in past picks."
            facts.append((_score(ptype[0], "player_type", None), {"text": text, "tone": None, "kind": "player_type"}))

    # Everyone gets a blurb: if nothing surfaced, promote the best trivia.
    if not facts and trivia:
        cat, row = max(trivia.items(), key=lambda kv: _num(kv[1], "surface_score"))
        phrase = _trivia_phrase(row, cat)
        if phrase:
            facts.append((1.0, {"text": f"{manager} {phrase} — their clearest draft tell.",
                                "tone": None, "kind": "tell"}))
            trivia.pop(cat, None)

    # Dedupe near-identical facts (e.g. a manager split across two franchise
    # rows both yielding "loads up on backs early"): keep the strongest.
    facts.sort(key=lambda x: -x[0])
    seen: set[str] = set()
    fact_items = []
    for _, item in facts:
        sig = re.sub(r"\d+", "", item["text"].split(".")[0]).lower()
        sig = re.sub(r"\s+", " ", sig).strip()
        if sig in seen:
            continue
        seen.add(sig)
        fact_items.append(item)
        if len(fact_items) >= 6:
            break

    also_bits = []
    for cat in ("keeper", "franchise", "school"):
        if cat in trivia:
            p = _trivia_phrase(trivia[cat], cat)
            if p:
                also_bits.append(p)
    also = ("Also: " + _and(also_bits) + ".") if also_bits else ""

    return {"facts": fact_items, "also": also}
