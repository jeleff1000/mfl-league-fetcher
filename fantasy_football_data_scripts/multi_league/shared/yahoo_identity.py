from __future__ import annotations

import re

_INVALID_TOKENS = {"", "--", "none", "nan", "<na>"}
_YAHOO_TEAM_KEY_RE = re.compile(r"^\d+\.l\.\d+\.t\.\d+$")
_YAHOO_TEAM_URL_RE = re.compile(r"/f1/(?P<league_id>\d+)/(?P<team_id>\d+)(?:$|[/?#])")


def _clean_identity_token(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() in _INVALID_TOKENS:
        return None
    return text


def is_valid_yahoo_team_key(value: object) -> bool:
    token = _clean_identity_token(value)
    return bool(token and _YAHOO_TEAM_KEY_RE.match(token))


def recover_yahoo_team_key(
    team_key: object,
    team_url: object,
    league_key: object | None,
) -> str | None:
    """Recover a canonical Yahoo ``team_key`` from hidden scoreboard payloads.

    Yahoo scoreboard rows occasionally redact ``team_key`` as ``--`` for hidden
    managers, but the same row still exposes the stable team slot in the team
    URL (``.../f1/<league_id>/<team_slot>``). Reconstruct the missing key from
    the league key when possible.
    """

    cleaned_team_key = _clean_identity_token(team_key)
    if cleaned_team_key and _YAHOO_TEAM_KEY_RE.match(cleaned_team_key):
        return cleaned_team_key

    cleaned_league_key = _clean_identity_token(league_key)
    cleaned_url = _clean_identity_token(team_url)
    if not cleaned_league_key or ".l." not in cleaned_league_key or not cleaned_url:
        return cleaned_team_key

    match = _YAHOO_TEAM_URL_RE.search(cleaned_url)
    if not match:
        return cleaned_team_key

    league_id_from_url = match.group("league_id")
    team_id = match.group("team_id")
    expected_league_id = cleaned_league_key.split(".l.", 1)[1].split(".t.", 1)[0]
    if league_id_from_url != expected_league_id:
        return cleaned_team_key

    return f"{cleaned_league_key}.t.{team_id}"
