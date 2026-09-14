#!/usr/bin/env python3
"""Briefing composer — turns mined nugget rows into a scannable intelligence
briefing: grouped by meaning under theme headers, terse and name-free.

The nugget renderer produces standalone sentences ("Dave loads up on true
burners — about 4x the league rate"). Stacked, the repeated openers read like
a template. A briefing instead groups by theme, lets the header carry the
context, and drops the subject/verb boilerplate to a tight fragment
("True burners — 4x, mostly hits"). Same facts, reads like scouting.
"""

from __future__ import annotations

import re
from typing import Any

from multi_league.transformations.draft.nugget_renderer import (
    _PLACE_COLUMNS,
    _TEAM_COLUMNS,
    _noun_phrase,
)

# School / team / conference columns are loyalties, never production — reuse the
# renderer's authoritative sets so a new team column can't silently leak into
# the league market line.
_LOYALTY_COLUMNS = _TEAM_COLUMNS | _PLACE_COLUMNS | {"conference"}

# ── Theme buckets (order = display order within a page) ──
MANAGER_THEME_ORDER = [
    "build", "market", "targets", "player_type", "loyalty", "stacking", "tempo",
]
LEAGUE_THEME_ORDER = ["tempo", "blind_spots", "hidden_value"]

MANAGER_THEME_LABEL = {
    "build": "Draft build",
    "targets": "Targets coming off big years",
    "market": "Reads the market",
    "player_type": "Player type",
    "loyalty": "Loyalties",
    "stacking": "Stacking",
    "tempo": "Tempo & habits",
}
LEAGUE_THEME_LABEL = {
    "tempo": "How the room drafts",
    "blind_spots": "Market blind spots",
    "hidden_value": "Hidden value",
}

_CONSTRUCTION_SUBJECT = {
    "zero_rb": "Zero-RB build", "robust_rb": "Robust-RB build", "hero_rb": "Hero-RB build",
    "wr_heavy_start": "WR-heavy start", "early_qb": "grabs a QB early", "late_qb": "waits on QB",
    "te_early": "early tight end", "stars_and_scrubs": "stars & scrubs", "balanced_build": "balanced build",
    "opens_rb": "opens with a back", "opens_wr": "opens with a receiver",
    "opens_qb": "opens with a QB", "opens_te": "opens with a tight end",
    "predictable": "same plan every year", "chaotic": "reinvents it every year",
    "chase": "chases position runs", "fade": "fades position runs",
}

# Keyed by market feature_value → terse subject that follows the "Reads the
# market" header ("Reaches a round-plus above ADP", etc.).
_MARKET_SUBJECT = {
    "big reach": "Reaches a round-plus above ADP", "reach": "Reaches above ADP",
    "big value": "Scoops big ADP fallers", "value": "Takes players below ADP",
    "way over market": "Pays well over market price", "over market": "Pays over market price",
    "big bargain": "Hunts steep bargains", "bargain": "Buys below market price",
}


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _magnitude(row: dict) -> str | None:
    lift = max(_num(row.get("lift", 1)), _num(row.get("count_lift", 1)))
    if lift >= 2.6:
        return f"{round(lift)}x"
    if lift >= 1.6:
        return "2x"
    return None


def _verdict(row: dict) -> tuple[str, str] | None:
    hits = int(_num(row.get("outcome_hits", 0)))
    busts = int(_num(row.get("outcome_busts", 0)))
    decisive = hits + busts
    if decisive < 5:
        return None
    share = hits / decisive
    if share >= 0.62:
        return ("mostly hits", "good")
    if share <= 0.38:
        return ("mostly misses", "bad")
    return None


def _compact_subject(noun: str) -> str:
    text = noun.strip()
    text = re.sub(r"^players coming off an? ", "", text)
    text = re.sub(r" season$", "", text)
    text = re.sub(r"^players out of ", "", text)
    text = re.sub(r"^players (?=(taken|who|with) )", "", text)
    text = re.sub(r"^players with ", "", text)
    text = re.sub(r" players$", "", text)
    if text in ("players", ""):
        text = "roster depth"
    return text[:1].upper() + text[1:]


def _manager_theme(row: dict, category: str) -> str:
    ft = str(row.get("feature_type", ""))
    if ft.startswith("construction."):
        sub = ft.split(".", 1)[-1]
        if sub == "repeat_player":
            return "loyalty"
        if sub in ("run_reactivity", "consistency"):
            return "tempo"
        return "build"
    return {
        "production": "targets", "market": "market", "player_type": "player_type",
        "loyalty": "loyalty", "stacking": "stacking", "tempo": "tempo",
        "construction": "build",
    }.get(category, "targets")


def manager_theme_of(row: dict) -> str:
    """Theme key for one manager nugget row (used for diversified selection)."""
    ft = str(row.get("feature_type", ""))
    return _manager_theme(row, _category(ft))


def _manager_fragment(row: dict) -> tuple[str, str, str | None]:
    """(theme_key, terse_text, tone) for one manager nugget."""
    ft = str(row.get("feature_type", ""))
    tone = None
    tags: list[str] = []

    if ft.startswith("construction."):
        sub = ft.split(".", 1)[-1]
        value = str(row.get("feature_value", ""))
        if sub == "repeat_player":
            subject = f"Loyal to {value}"
            tags.append(f"{int(_num(row.get('years_seen')))} seasons")
        else:
            subject = _CONSTRUCTION_SUBJECT.get(value, value.replace("_", " ").title())
            subject = subject[:1].upper() + subject[1:]
            picks = int(_num(row.get("picks")))
            years = int(_num(row.get("years_seen")))
            if years:
                tags.append(f"{picks} of {years} drafts")
        theme = _manager_theme(row, "construction")
    else:
        category = _category(ft)
        value = str(row.get("feature_value", ""))
        if category == "market" and value in _MARKET_SUBJECT:
            subject = _MARKET_SUBJECT[value]
        else:
            noun = _noun_phrase(ft, value, row.get("feature_label"))
            subject = _compact_subject(noun or value)
        theme = _manager_theme(row, category)
        mag = _magnitude(row)
        if mag:
            tags.append(mag)
        verdict = _verdict(row)
        if verdict:
            tags.append(verdict[0])
            tone = verdict[1]

    tail = f" ({', '.join(tags)})" if tags else ""
    return theme, f"{subject}{tail}", tone


def _category(feature_type: str) -> str:
    ft = str(feature_type or "")
    base = ft.split(".", 1)[-1]
    for suffix in ("_tier", "_bin"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
    if base in ("market_reach", "market_price"):
        return "market"
    if base == "team_stack":
        return "stacking"
    if base.startswith("nfl_team") or base in _LOYALTY_COLUMNS:
        return "loyalty"
    if base in ("height", "weight", "ras_score", "forty", "bench", "vertical",
                "broad_jump", "age_at_draft", "is_undrafted"):
        return "player_type"
    return "production"


# Family caps — keep each dossier section a tight, scannable few bullets.
_MAX_PER_THEME = 3

_TIER_WORDS = {
    "elite", "strong", "high", "big", "weak", "poor", "low", "middling", "mid", "quiet",
}


def _strip_tier(subject: str) -> str:
    head, _, rest = subject.partition(" ")
    return rest if head.lower() in _TIER_WORDS and rest else subject


def _is_clean_subject(subject: str) -> bool:
    """Reject bare stat abbreviations that leak from label fallbacks
    ('Strong Y/R', 'pass yds', 'INT yds') and any raw 'label: value' leak."""
    low = subject.lower()
    return "/" not in subject and ":" not in subject and "yds" not in low


def _target_parts(row: dict) -> tuple[str, float] | None:
    """(subject, lift) for a prior-production 'targets' row, subject lowercased
    ('elite rushing volume')."""
    noun = _noun_phrase(str(row.get("feature_type", "")), str(row.get("feature_value", "")), row.get("feature_label"))
    subject = _compact_subject(noun or str(row.get("feature_value", "")))
    if not subject or subject.lower() == "roster depth" or not _is_clean_subject(subject):
        return None
    lift = max(_num(row.get("lift", 1)), _num(row.get("count_lift", 1)))
    return subject.lower(), lift


def _targets_group(rows: list[dict]) -> list[dict]:
    """Consolidate the (redundant, everyone-does-it) prior-production family
    into one distinctive bullet: name this manager's most-distinctive stats,
    deduped by concept, with an aggregate verdict."""
    best: dict[str, dict] = {}
    for row in rows:
        parts = _target_parts(row)
        if not parts:
            continue
        subject, lift = parts
        root = _strip_tier(subject)
        cur = best.get(root)
        if cur is None or lift > cur["lift"]:
            best[root] = {"subject": subject, "lift": lift, "row": row}
    if not best:
        return []
    ranked = sorted(best.values(), key=lambda x: (-x["lift"], -_num(x["row"].get("surface_score"))))
    top = ranked[:3]
    names = [x["subject"] for x in top]
    max_lift = max(x["lift"] for x in top)
    tags: list[str] = []
    if max_lift >= 2.6:
        tags.append(f"up to {round(max_lift)}x")
    elif max_lift >= 1.6:
        tags.append("about 2x")

    hits = sum(int(_num(r.get("outcome_hits", 0))) for r in rows)
    busts = sum(int(_num(r.get("outcome_busts", 0))) for r in rows)
    tone = None
    if hits + busts >= 5:
        share = hits / (hits + busts)
        if share >= 0.62:
            tags.append("mostly hits")
            tone = "good"
        elif share <= 0.38:
            tags.append("mostly misses")
            tone = "bad"

    joined = _and(names)
    text = joined[:1].upper() + joined[1:]
    if tags:
        text += f" ({', '.join(tags)})"
    return [{
        "text": text, "tone": tone,
        "full": top[0]["row"].get("nugget_headline", ""),
        "score": _num(top[0]["row"].get("surface_score")),
    }]


def compose_manager(rows: list[dict]) -> list[dict]:
    """Group one manager's nuggets into a cohesive, individual dossier: bullets
    within theme families, the redundant prior-production family consolidated to
    a single distinctive line, every family capped tight."""
    by_theme: dict[str, list[dict]] = {}
    for row in rows:
        by_theme.setdefault(manager_theme_of(row), []).append(row)

    groups = []
    for theme in MANAGER_THEME_ORDER:
        theme_rows = by_theme.get(theme)
        if not theme_rows:
            continue
        if theme == "targets":
            items = _targets_group(theme_rows)
        else:
            items = []
            for row in sorted(theme_rows, key=lambda r: -_num(r.get("surface_score"))):
                _, text, tone = _manager_fragment(row)
                items.append({
                    "text": text, "tone": tone,
                    "full": row.get("nugget_headline", ""),
                    "score": _num(row.get("surface_score")),
                })
            items = items[:_MAX_PER_THEME]
        if items:
            groups.append({"theme": MANAGER_THEME_LABEL[theme], "items": items})
    return groups


def compose_league(fleet_rows: list[dict], market_rows: list[dict]) -> list[dict]:
    """League briefing: fleet tempo (consolidated) + market blind spots /
    hidden value (subjects listed under one header, not one sentence each)."""
    groups: list[dict] = []

    early, late = [], []
    for row in fleet_rows:
        pos = str(row.get("feature_value", ""))
        word = {"K": "kickers", "DEF": "defenses"}.get(pos, f"{pos}s")
        (early if _num(row.get("fleet_percentile_earlier")) <= 0.5 else late).append(word)
    tempo_items = []
    if early:
        tempo_items.append({"text": f"Runs to {_and(early)} earlier than comparable leagues", "tone": None})
    if late:
        tempo_items.append({"text": f"Waits longer than comparable leagues on {_and(late)}", "tone": None})
    if tempo_items:
        groups.append({"theme": LEAGUE_THEME_LABEL["tempo"], "items": tempo_items})

    # Only prior-production tiers belong in "coming off a weak/elite year"
    # framing. Loyalty (schools), position-draft-label, and player-type
    # features route to their own manager families, not the league market line.
    overpays, bargains = [], []
    for row in market_rows:
        ft = str(row.get("feature_type", ""))
        if _category(ft) != "production":
            continue
        headline = str(row.get("nugget_headline", ""))
        noun = _noun_phrase(ft, str(row.get("feature_value", "")), row.get("feature_label"))
        subject = _compact_subject(noun or "")
        if not subject or not _is_clean_subject(subject):
            continue
        (bargains if "quiet bargain" in headline else overpays).append((subject, _num(row.get("surface_score"))))

    def _dedup(pairs: list[tuple[str, float]]) -> list[str]:
        seen: dict[str, float] = {}
        for sub, score in pairs:
            key = sub.lower()
            if key not in seen or score > seen[key]:
                seen[key] = score
        ordered = sorted({s.lower(): s for s, _ in pairs}.values(),
                         key=lambda s: -seen[s.lower()])
        return ordered[:3]

    def _hoist_tier(subs: list[str]) -> tuple[str | None, list[str]]:
        """Uniform tier → hoist it into the frame ('coming off weak years: …').
        Mixed tiers read as a contradiction, so strip them and list the
        production types plainly."""
        firsts = [s.split(" ", 1)[0].lower() for s in subs]
        if all(" " in s for s in subs) and all(f in _TIER_WORDS for f in firsts):
            rest = [s.split(" ", 1)[1] for s in subs]
            if len(set(firsts)) == 1:
                return firsts[0], rest
            return None, rest  # mixed tiers → plain stats, no tier frame
        return None, subs

    if overpays:
        tier, subs = _hoist_tier(_dedup(overpays))
        text = (f"Reaches for players coming off {tier} years: {_and(subs)}"
                if tier else f"Reaches past ADP for: {_and(subs)}")
        groups.append({"theme": LEAGUE_THEME_LABEL["blind_spots"],
                       "items": [{"text": text, "tone": "bad"}]})
    if bargains:
        tier, subs = _hoist_tier(_dedup(bargains))
        text = (f"Lets {tier} producers slide: {_and(subs)}"
                if tier else f"Lets these slide as quiet bargains: {_and(subs)}")
        groups.append({"theme": LEAGUE_THEME_LABEL["hidden_value"],
                       "items": [{"text": text, "tone": "good"}]})
    return groups


def _and(words: list[str]) -> str:
    words = [w for w in words if w]
    if len(words) <= 1:
        return words[0] if words else ""
    if len(words) == 2:
        return f"{words[0]} and {words[1]}"
    return ", ".join(words[:-1]) + f", and {words[-1]}"
