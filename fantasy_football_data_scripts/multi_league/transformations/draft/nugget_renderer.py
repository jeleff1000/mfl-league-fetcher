#!/usr/bin/env python3
"""Nugget renderer — mined signal -> a sentence a league-mate just reads.

The nugget contract: understandable beats technically correct. Lift/z-scores
and bin edges never reach copy; quantities become plain rate phrases
("about twice as often as the league") and confidence becomes receipts
("held in 5 of 6 drafts"). Raw numbers stay on the signal row for hovers.
"""

from __future__ import annotations

from typing import Any

#: Config tokens scrubbed from semantic labels — "Weighted PPG 4pt Half"
#: is an implementation detail; the human reads "weighted PPG".
_CONFIG_TOKENS = {"4pt", "5pt", "6pt", "0ppr", "half", "ppr", "tep", "ppfd", "ret"}

_TEAM_COLUMNS = {"nfl_team", "nfl_team_api", "nfl_draft_team"}
_PLACE_COLUMNS = {"college", "birth_place", "high_school"}

#: NFL team codes → nicknames, so a franchise loyalty reads "Steelers (6x)"
#: not "PIT (6x)". Includes relocated/renamed codes. Unknown codes fall back
#: to the raw code.
_TEAM_NAMES = {
    "ARI": "Cardinals", "ARZ": "Cardinals", "ATL": "Falcons", "BAL": "Ravens",
    "BUF": "Bills", "CAR": "Panthers", "CHI": "Bears", "CIN": "Bengals",
    "CLE": "Browns", "DAL": "Cowboys", "DEN": "Broncos", "DET": "Lions",
    "GB": "Packers", "GNB": "Packers", "HOU": "Texans", "IND": "Colts",
    "JAX": "Jaguars", "JAC": "Jaguars", "KC": "Chiefs", "KAN": "Chiefs",
    "LV": "Raiders", "LVR": "Raiders", "OAK": "Raiders", "LAC": "Chargers",
    "SD": "Chargers", "SDG": "Chargers", "LA": "Rams", "LAR": "Rams",
    "STL": "Rams", "MIA": "Dolphins", "MIN": "Vikings", "NE": "Patriots",
    "NWE": "Patriots", "NO": "Saints", "NOR": "Saints", "NYG": "Giants",
    "NYJ": "Jets", "PHI": "Eagles", "PIT": "Steelers", "SEA": "Seahawks",
    "SF": "49ers", "SFO": "49ers", "TB": "Buccaneers", "TAM": "Buccaneers",
    "TEN": "Titans", "WAS": "Commanders", "WSH": "Commanders",
}

_BOOL_PHRASES = {
    "auto_drafted": ("auto-drafted picks", "hand-picked players"),
    "is_undrafted": ("undrafted free agents", "drafted-pedigree players"),
    "drafted_as_starter": ("picks slotted straight into the lineup", "bench-slot picks"),
    "drafted_as_backup": ("bench-slot picks", "picks slotted straight into the lineup"),
    "trade_locked": ("trade-locked picks", "tradeable picks"),
}

_TIER_ARTICLES = {"elite": "an", "strong": "a", "middling": "a", "weak": "a"}

#: Derived behavioral features get bespoke phrases per value. Neutral values
#: ("market price") are not nuggets and refuse to render.
_DERIVED_VALUE_PHRASES = {
    ("market_reach", "big reach"): "players they reach way up for (a round-plus above ADP)",
    ("market_reach", "reach"): "players taken above their ADP",
    ("market_reach", "big value"): "players who slid a round-plus past their ADP",
    ("market_reach", "value"): "players taken below their ADP",
    ("market_price", "way over market"): "players at well over market price",
    ("market_price", "over market"): "players above market price",
    ("market_price", "big bargain"): "players at steep discounts to market price",
    ("market_price", "bargain"): "players below market price",
    ("team_stack", "QB stack"): "QB stacks — pass-catchers paired with their own quarterback",
}
_DERIVED_BASES = {"market_reach", "market_price", "team_stack"}

#: Static physical traits are not "seasons" — they get trait phrasing, and
#: only at the extremes (an average-height player is not a nugget).
#: column -> (elite phrase, weak phrase)
_BIO_TRAIT_PHRASES = {
    "height": ("towering players", "short-for-the-position players"),
    "weight": ("big-bodied players", "undersized players"),
    "forty": ("true burners", "slow-forty players"),  # forty is inverted upstream
    "ras_score": ("elite athletes", "low-RAS athletes"),
    "vertical": ("big-time leapers", None),
    "broad_jump": ("explosive athletes", None),
    "bench": ("weight-room standouts", None),
    "cone": ("twitchy, agile athletes", None),
    "shuttle": ("twitchy, agile athletes", None),
    "age_at_draft": ("older prospects", "very young prospects"),
}


#: Curated phrases for stat concepts whose auto-inferred labels are codes,
#: not language. Keys are concept column bases (variant tokens stripped).
_STAT_PHRASES = {
    "yards_per_touch": "yards-per-touch efficiency",
    "yards_per_target": "yards-per-target efficiency",
    "rec_td_pct": "touchdown-heavy receiving",
    "receptions": "receptions",
    "targets": "target volume",
    "carries": "rushing workload",
    "rushing_yards": "rushing yardage",
    "receiving_yards": "receiving yardage",
    "receiving_yards_after_catch": "yards-after-catch",
    "target_share": "target share",
    "wopr": "usage (WOPR)",
    "air_yards_share": "air-yards share",
    "weighted_ppg": "weighted PPG",
    "ppg_season": "PPG",
    "fpts": "fantasy scoring",
    "pts_rush_att": "rushing volume",
    "pts_rush_yd": "rushing yardage",
    "pts_rush": "rushing production",
    "pts_rec": "pass-catching volume",
    "pts_rec_yd": "receiving yardage",
    "pts_rec_fd": "receiving first downs",
    "pts_first_downs": "first-down production",
    "pts_rec_te_bonus": "TE-premium receiving",
    "receptions_0_4": "short-area receptions",
    "receptions_5_9": "intermediate receptions",
    "receptions_10_19": "chain-moving receptions",
    "receptions_20_29": "deep receptions",
}

#: Short codey tokens that mark an unphrased implementation label. If the
#: cleaned label still contains one, we refuse to render rather than print
#: "elite pts misc season" (whitelist covers legitimate short words).
_CODEY_TOKEN_WHITELIST = {"ppg", "td", "tds", "yac", "qb", "rb", "wr", "te", "def", "adot", "ras", "int", "fpts"}


def _looks_codey(phrase: str) -> bool:
    for token in phrase.replace("-", " ").split():
        bare = token.strip("().").lower()
        if bare in _CODEY_TOKEN_WHITELIST:
            continue
        if len(bare) <= 3 and not bare.isdigit() and bare.isalpha() and bare not in {"per", "off", "for", "and", "the", "of", "to", "at", "yds", "avg", "pct", "own"}:
            return True
        if bare in {"misc", "pts"} or (bare.startswith("p") and bare[1:].isdigit()):
            return True
    return False


def _clean_stat_phrase(label: str) -> str:
    words = [w for w in str(label).split() if w.lower() not in _CONFIG_TOKENS]
    # Drop a trailing "Season" — the sentence template supplies that word.
    if words and words[-1].lower() == "season":
        words = words[:-1]
    phrase = " ".join(words) or str(label)
    # Keep acronyms; lowercase ordinary words for mid-sentence use.
    return " ".join(w if w.isupper() else w.lower() for w in phrase.split())


def _noun_phrase(feature_type: str, feature_value: str, feature_label: str | None) -> str | None:
    prefix, _, raw = feature_type.partition(".")
    value = str(feature_value)
    label = feature_label or raw.replace("_", " ")

    if raw in _DERIVED_BASES:
        return _DERIVED_VALUE_PHRASES.get((raw, value))  # neutral values -> None

    if raw.endswith("_tier"):
        base = raw[: -len("_tier")]
        if prefix == "bio":
            phrases = _BIO_TRAIT_PHRASES.get(base)
            if phrases is None:
                return None  # no simple words for this trait -> not surfaced
            phrase = phrases[0] if value == "elite" else phrases[1] if value == "weak" else None
            return phrase  # mid-tier static traits are not nuggets
        # Strip variant tokens from the column base to hit the phrase map.
        concept = base
        for token in ("_0ppr", "_half", "_ppr", "_tep", "_ppfd", "_4pt", "_5pt", "_6pt", "_ret"):
            concept = concept.replace(token, "")
        import re as _re
        concept = _re.sub(r"_p\d+(?=_|$)", "", concept)
        if concept.startswith("rank_"):
            return None  # positional-rank tiers read awkwardly ("weak rank
            # season flex season"); production/efficiency tiers say it better
        stat = _STAT_PHRASES.get(concept) or _clean_stat_phrase(label)
        if len(stat) <= 2 or _looks_codey(stat):
            return None  # codey implementation label -> not surfaced as copy
        tier = value
        if tier == "middling":
            return None  # "drafts average players" is not a story
        article = _TIER_ARTICLES.get(tier, "a")
        return f"players coming off {article} {tier} {stat} season"
    base = raw[:-4] if raw.endswith("_bin") else raw
    if base in _TEAM_COLUMNS:
        return f"{_TEAM_NAMES.get(value.upper(), value.upper())} players"
    if base in _PLACE_COLUMNS:
        return f"players out of {value}"
    if base in _BOOL_PHRASES:
        positive, negative = _BOOL_PHRASES[base]
        return positive if value in ("1", "true", "True") else negative
    if base in ("position", "nfl_position", "position_group"):
        return f"{value}s"
    if base in ("conference", "position_category", "position_side"):
        return f"{value} players"
    if raw.endswith("_bin"):
        return f"players with {_clean_stat_phrase(label)} around {value.replace('_to_', '-')}"
    stat = _clean_stat_phrase(label)
    if _looks_codey(stat):
        return None  # codey implementation label -> never copy, even as fallback
    return f"{stat}: {value}"


def _rate_phrase(lift: float) -> tuple[str, str]:
    """(verb, rate clause) for a manager tendency."""
    if lift >= 2.6:
        return "loads up on", f"about {round(lift)}x the league rate"
    if lift >= 1.6:
        return "keeps drafting", "about twice as often as the league"
    if lift >= 1.0:
        return "leans toward", "noticeably more than the league"
    if lift <= 0.4:
        return "almost never drafts", "a fraction of the league rate"
    return "shies away from", "well below the league rate"


def _money_rate(lift: float) -> str:
    if lift >= 2.6:
        return f"about {round(lift)}x the league's typical money"
    if lift >= 1.6:
        return "about twice the league's typical money"
    return "well above the league's typical money"


def _confidence_phrase(row: dict[str, Any], axis: str = "capital") -> str:
    picks = int(row.get("picks", 0) or 0)
    years = int(row.get("years_seen", 0) or 0)
    prefix = "count_" if axis == "count" else ""
    if "lift" in row or "count_lift" in row:
        lift_key = f"{prefix}lift" if f"{prefix}lift" in row else "lift"
        over = float(row.get(lift_key, 1) or 1) >= 1
    else:
        over = float(row.get("excess_residual", 0) or 0) >= 0
    supporting = int(row.get(f"{prefix}positive_years" if over else f"{prefix}negative_years", 0) or 0)
    parts = [f"{picks} picks across {years} draft{'s' if years != 1 else ''}"]
    if years > 1:
        parts.append(f"pattern held in {supporting} of {years} seasons")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Surfacing policy: confidence and importance are separate dials.
# Importance comes from the registry (3 strategic / 2 character / 1 trivia).
# Trivia needs an absolutely crazy signal to earn airtime; strategic
# tendencies surface at ordinary evidence. v1 heuristic — step 4's gate
# (FDR + receipts) refines it; the QA-league eyeball pass tunes weights.
# ---------------------------------------------------------------------------

#: Minimum |dominant z| to surface, by importance tier.
_SURFACE_Z_BAR = {3: 2.0, 2: 2.5, 1: 4.0}
#: Trivia additionally needs a big lift and near-perfect repeatability.
_TRIVIA_MIN_LIFT = 3.0
_TRIVIA_MIN_REPEATABILITY = 0.85
#: Ranking weight per importance tier.
_IMPORTANCE_WEIGHT = {3: 1.0, 2: 0.6, 1: 0.25}


def _dominant_z(row: dict[str, Any]) -> float:
    z_values = [
        abs(float(row.get(key, 0) or 0))
        for key in ("capital_z_score", "count_z_score", "value_z_score")
    ]
    return max(z_values)


def _dominant_lift(row: dict[str, Any]) -> float:
    lifts = []
    for key in ("lift", "count_lift"):
        value = row.get(key)
        if value is not None:
            lift = float(value)
            lifts.append(lift if lift >= 1 else (1 / lift if lift > 0 else 99))
    return max(lifts) if lifts else 1.0


def surface_score(row: dict[str, Any]) -> dict[str, Any]:
    """Score a signal's claim to airtime and decide whether it clears the bar.

    Returns {"surface_score": float, "surfaced": bool}. Signals that fail
    stay in the discovery output for models/analysts — they just don't get
    a slot in a dossier by default.
    """
    importance = int(row.get("importance", 2) or 2)
    z = _dominant_z(row)
    repeatability = max(
        float(row.get("repeatability", 0) or 0),
        float(row.get("count_repeatability", 0) or 0),
    )

    surfaced = z >= _SURFACE_Z_BAR.get(importance, 2.5)
    # One draft is not a tendency — manager/league patterns need at least
    # two seasons of evidence to earn airtime (16-league QA: dynasty
    # startups were producing "9 picks across 1 draft" nuggets).
    if surfaced and str(row.get("scope_type", "")) != "league_fleet":
        surfaced = int(row.get("years_seen", 0) or 0) >= 2
    if importance == 1 and surfaced:
        surfaced = (
            _dominant_lift(row) >= _TRIVIA_MIN_LIFT
            and repeatability >= _TRIVIA_MIN_REPEATABILITY
        )

    score = _IMPORTANCE_WEIGHT.get(importance, 0.6) * min(z, 8.0) * (0.5 + 0.5 * repeatability)
    return {"surface_score": round(score, 3), "surfaced": bool(surfaced)}


#: Draft-grade -> plain hit/miss. The letter grade is how we already grade
#: picks to users; this reads it as "worked / blew up", never as a number.
def grade_bucket(grade: str) -> str:
    g = str(grade or "").strip().upper()
    if not g:
        return "neutral"
    if g[0] == "A" or g == "B+":
        return "hit"
    if g[0] in ("D", "F"):
        return "bust"
    return "neutral"


def outcome_clause(hits: int, busts: int, *, decisive_min: int = 5) -> str | None:
    """A plain verdict on how a tendency's picks have panned out — or None
    when it's genuinely mixed / too thin to call. No LAMAR, no numbers: just
    'worked out' vs 'blown up', earned from the picks' own draft grades."""
    decisive = int(hits) + int(busts)
    if decisive < decisive_min:
        return None
    share = hits / decisive
    if share >= 0.62:
        return "and it's mostly worked out"
    if share <= 0.38:
        return "and it's mostly blown up in their face"
    return None


def render_nugget(row: dict[str, Any]) -> dict[str, str] | None:
    """Render one mined signal into plain language, or None if we cannot say
    it simply (no simple sentence -> not surfaced; the row still exists for
    models and analysts)."""
    noun = _noun_phrase(
        str(row.get("feature_type", "")),
        str(row.get("feature_value", "")),
        row.get("feature_label"),
    )
    if noun is None:
        return None
    if noun.startswith(("players with", )) and "_to_" in str(row.get("feature_value", "")):
        return None  # numeric bin edges are not a sentence — skip copy

    scope = str(row.get("scope_type", ""))
    if scope == "manager":
        name = row.get("scope_label", "This manager")
        capital_lift = float(row.get("lift", 1) or 1)
        count_lift = float(row.get("count_lift", capital_lift) or capital_lift)
        market_share = row.get("market_share")
        picks = int(row.get("picks", 0) or 0)
        axis = "capital"
        # Volume vs investment are different tendencies (Joe, 2026-07-06):
        # money says one thing, roster spots say another, and owning the
        # whole market on something cheap is the loudest signal of all.
        if market_share is not None and float(market_share) >= 0.5 and count_lift >= 2 and picks >= 6:
            share_pct = round(float(market_share) * 100)
            if float(market_share) >= 0.75:
                headline = (
                    f"{name} is basically the league's only buyer of {noun} — "
                    f"{share_pct}% of every such pick ever made."
                )
            else:
                headline = (
                    f"{name} owns the market on {noun} — "
                    f"{share_pct}% of every such pick in league history."
                )
            axis = "count"
        elif capital_lift >= 1.5 and count_lift < 1.2:
            headline = f"{name} pays up for {noun} — {_money_rate(capital_lift)}."
        elif count_lift >= 1.5 and capital_lift < 1.2:
            headline = (
                f"{name} drafts {noun} far more often than the league — "
                f"though mostly at bargain prices."
            )
            axis = "count"
        else:
            verb, rate = _rate_phrase(capital_lift)
            headline = f"{name} {verb} {noun} — {rate}."
        return {
            "nugget_headline": headline,
            "nugget_evidence": _confidence_phrase(row, axis=axis),
        }
    elif scope == "league_inefficiency":
        excess = float(row.get("excess_residual", 0) or 0)
        if excess >= 0:
            headline = f"In this league, {noun} have been quiet bargains."
        else:
            headline = f"This league consistently overpays for {noun}."
    else:
        return None

    return {
        "nugget_headline": headline,
        "nugget_evidence": _confidence_phrase(row),
    }
