"""Build an isolated canonical KMFFL model from the Yahoo web backup.

This is deliberately separate from the OAuth importer and from the source
backup database.  It consumes cached web observations plus a local read-only
ops/supertable DuckDB, then writes ``*_cookie_model.duckdb``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pandas as pd

# The pipeline package lives below the repository root, while this script is
# intentionally kept under scripts/.  Bootstrap that path for direct CLI use.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_PIPELINE_ROOT = _REPO_ROOT / "fantasy_football_data_scripts"
if str(_PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PIPELINE_ROOT))

from scripts.quick_import_kmffl_2025_web import (
    KMFFL_LEAGUE_KEYS,
    parse_settings_page,
    parse_web_timestamp_epoch,
)


def build_team_identity_maps(identities: pd.DataFrame) -> dict[tuple[int, int], dict[str, str]]:
    """Index Yahoo team identity by season and stable team slot."""
    result: dict[tuple[int, int], dict[str, str]] = {}
    if identities is None or identities.empty:
        return result
    for row in identities.to_dict("records"):
        if row.get("year") is None or row.get("team_number") is None:
            continue
        manager = str(row.get("manager") or "").strip()
        guid = str(row.get("manager_guid") or "").strip()
        if manager or guid:
            result[(int(row["year"]), int(row["team_number"]))] = {
                "manager": manager,
                "manager_guid": guid,
            }
    return result


def _team_name_map(matchups: pd.DataFrame) -> dict[tuple[int, int], str]:
    result: dict[tuple[int, int], str] = {}
    if matchups is None or matchups.empty:
        return result
    for row in matchups.to_dict("records"):
        for number_col, name_col in (("team_number", "team"), ("opponent_team_number", "opponent")):
            if row.get(number_col) is not None and row.get(name_col):
                result.setdefault((int(row["year"]), int(row[number_col])), str(row[name_col]))
    return result


def _bio_map(player_bio: pd.DataFrame) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if player_bio is None or player_bio.empty:
        return result
    for row in player_bio.to_dict("records"):
        value = row.get("yahoo_player_id")
        if pd.isna(value):
            continue
        result[str(int(float(value)))] = row
    return result


def _identity(identity_map: dict[tuple[int, int], dict[str, str]], year: int, number: Any) -> dict[str, str]:
    try:
        return identity_map.get((int(year), int(number)), {})
    except (TypeError, ValueError):
        return {}


def prepare_roster_source(
    roster: pd.DataFrame,
    identities: dict[tuple[int, int], dict[str, str]],
    team_names: dict[tuple[int, int], str],
    player_bio: pd.DataFrame,
    league_key: str,
) -> pd.DataFrame:
    """Add canonical identity and local NFL mapping fields to raw roster rows."""
    if roster is None or roster.empty:
        return pd.DataFrame()
    out = roster.copy()
    bio = _bio_map(player_bio)
    out["yahoo_player_id"] = out["yahoo_player_id"].astype(str)
    out["team_name"] = [team_names.get((int(y), int(t))) for y, t in zip(out.year, out.team_number)]
    info = [_identity(identities, y, t) for y, t in zip(out.year, out.team_number)]
    out["manager"] = [x.get("manager") for x in info]
    out["manager_guid"] = [x.get("manager_guid") for x in info]
    out["franchise_id"] = out["manager_guid"]
    out["team_key"] = [f"{league_key}.t.{int(t)}" for t in out["team_number"]]
    out["fantasy_position"] = out["lineup_position"]
    out["lineup_position"] = out["lineup_position"]
    out["is_rostered"] = 1
    out["league_id"] = league_key
    out["platform"] = "yahoo"
    out["NFL_player_id"] = [bio.get(pid, {}).get("NFL_player_id") for pid in out["yahoo_player_id"]]
    out["position"] = [bio.get(pid, {}).get("nfl_position") for pid in out["yahoo_player_id"]]
    return out


def prepare_matchup_source(
    matchups: pd.DataFrame,
    identities: dict[tuple[int, int], dict[str, str]],
    league_key: str,
) -> pd.DataFrame:
    """Convert matchup observations into canonical Yahoo raw fields."""
    if matchups is None or matchups.empty:
        return pd.DataFrame()
    out = matchups.copy()
    team_info = [_identity(identities, y, t) for y, t in zip(out.year, out.team_number)]
    opp_info = [_identity(identities, y, t) for y, t in zip(out.year, out.opponent_team_number)]
    out["manager"] = [x.get("manager") for x in team_info]
    out["manager_guid"] = [x.get("manager_guid") for x in team_info]
    out["opponent"] = [x.get("manager") or old for x, old in zip(opp_info, out["opponent"])]
    out["opponent_guid"] = [x.get("manager_guid") for x in opp_info]
    out["team_name"] = out["team"]
    out["team_key"] = [f"{league_key}.t.{int(t)}" for t in out["team_number"]]
    out["opponent_team_key"] = [f"{league_key}.t.{int(t)}" for t in out["opponent_team_number"]]
    out["league_id"] = league_key
    out["platform"] = "yahoo"
    return out


def prepare_draft_source(
    draft: pd.DataFrame,
    team_names: dict[tuple[int, int], str],
    identities: dict[tuple[int, int], dict[str, str]],
    league_key_by_year: dict[int, str],
    roster: pd.DataFrame | None = None,
    team_count: int = 10,
    team_count_by_year: Mapping[int, int] | None = None,
) -> pd.DataFrame:
    """Attach canonical Yahoo identity to draft rows.

    ``manager_guid``/``franchise_id`` are the cross-season identity keys;
    ``team_key`` is only a season-local Yahoo team slot. Some archived Yahoo
    draft tables omit those IDs and render only a shortened team label, so the
    label is used solely at this parser boundary to recover the season-local
    slot from the team identity captured by roster/matchup pages.
    """
    if draft is None or draft.empty:
        return pd.DataFrame()
    # The quick-artifact pass can receive the complete captured draft frame
    # while intentionally supplying only the years in its quick window. Keep
    # the source rows aligned with the explicit renewal-chain mapping before
    # constructing season-local team keys.
    out = draft.loc[draft["year"].isin(league_key_by_year)].copy()
    if out.empty:
        return pd.DataFrame()
    lookup: dict[tuple[int, str], dict[str, str]] = {}
    for (year, number), name in team_names.items():
        if name and int(year) in league_key_by_year:
            identity = dict(_identity(identities, year, number))
            identity["team_number"] = str(number)
            identity["team_key"] = f"{league_key_by_year[int(year)]}.t.{int(number)}"
            identity["team_name"] = name
            lookup[(year, name)] = identity

    def lookup_team(year: Any, team: Any) -> dict[str, str]:
        year_int = int(year)
        team_name = str(team)
        exact = lookup.get((year_int, team_name))
        if exact:
            return exact
        prefix = team_name.rstrip(". â€¦")
        if prefix != team_name and prefix:
            matches = [
                identity
                for (candidate_year, candidate_name), identity in lookup.items()
                if candidate_year == year_int and candidate_name.startswith(prefix)
            ]
            if len(matches) == 1:
                return matches[0]
        return {}

    info = []
    for row in out.itertuples(index=False):
        year = int(row.year)
        raw_team_key = getattr(row, "team_key", None)
        raw_team_number = getattr(row, "team_number", None)
        if raw_team_key and str(raw_team_key).strip():
            match = next(
                (
                    identity
                    for (candidate_year, _), identity in lookup.items()
                    if candidate_year == year and identity.get("team_key") == str(raw_team_key).strip()
                ),
                {},
            )
            info.append(match)
        elif raw_team_number is not None and str(raw_team_number).strip():
            number = int(raw_team_number)
            info.append(
                next(
                    (
                        identity
                        for (candidate_year, _), identity in lookup.items()
                        if candidate_year == year and identity.get("team_number") == str(number)
                    ),
                    {},
                )
            )
        else:
            info.append(lookup_team(year, row.team))
    captured_manager = out.get("manager", pd.Series([None] * len(out), index=out.index))
    captured_guid = out.get("manager_guid", pd.Series([None] * len(out), index=out.index))
    out["manager"] = [
        str(captured).strip() if pd.notna(captured) and str(captured).strip() else info_row.get("manager")
        for captured, info_row in zip(captured_manager, info)
    ]
    out["manager_guid"] = [
        str(captured).strip() if pd.notna(captured) and str(captured).strip() else info_row.get("manager_guid")
        for captured, info_row in zip(captured_guid, info)
    ]
    out["franchise_id"] = out["manager_guid"]
    out["team_key"] = [x.get("team_key") for x in info]
    out["team_name"] = [x.get("team_name") or team for x, team in zip(info, out["team"])]
    # Older Yahoo draft pages (notably KMFFL 2015) omit fantasy player IDs,
    # while the roster pages from the same cookie capture retain them. Use an
    # unambiguous year/player-name map only at this parser boundary; this is a
    # player-ID recovery, not a manager/team identity join.
    if "yahoo_player_id" not in out.columns:
        out["yahoo_player_id"] = pd.NA
    roster_ids: dict[tuple[int, str], set[str]] = {}
    if roster is not None and not roster.empty and "yahoo_player_id" in roster.columns:
        for row in roster.itertuples(index=False):
            player_id = getattr(row, "yahoo_player_id", None)
            player_name = getattr(row, "player", None)
            if pd.isna(player_id) or not str(player_name or "").strip():
                continue
            key = (int(row.year), re.sub(r"\s+", " ", str(player_name).strip()).casefold())
            roster_ids.setdefault(key, set()).add(str(player_id))
    recovered_ids = []
    for row in out.itertuples(index=False):
        current = getattr(row, "yahoo_player_id", None)
        if current is not None and not pd.isna(current) and str(current).strip():
            recovered_ids.append(current)
            continue
        key = (int(row.year), re.sub(r"\s+", " ", str(row.player).strip()).casefold())
        matches = roster_ids.get(key, set())
        recovered_ids.append(next(iter(matches)) if len(matches) == 1 else current)
    out["yahoo_player_id"] = recovered_ids
    team_count_by_year = team_count_by_year or {}
    draft_team_counts = pd.Series(
        [max(1, int(team_count_by_year.get(int(year), team_count))) for year in out["year"]],
        index=out.index,
    )
    out["round"] = ((out["pick"].astype(int) - 1) // draft_team_counts) + 1
    out["league_id"] = [league_key_by_year.get(int(year)) for year in out["year"]]
    out["platform"] = "yahoo"
    # Draft format is a per-season property.  KMFFL switched from an offline
    # snake draft in 2015 to live salary-cap drafts afterward; using one global
    # ``any(cost > 0)`` would incorrectly label 2015 as auction.
    cost_by_year = out.assign(_cost_numeric=pd.to_numeric(out["cost"], errors="coerce")).groupby("year")[
        "_cost_numeric"
    ].transform(lambda values: "auction" if values.fillna(0).gt(0).any() else "snake")
    out["draft_type"] = cost_by_year
    return out


def repair_trade_rows_from_cached_pages(transactions: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    """Recover Yahoo trade destinations and two-team trade partners.

    Yahoo's transaction HTML renders trades as a pair of ``Traded to`` rows
    without the normal action title or team class.  The two destination teams
    identify the receiving team for each asset and, for these pages, the other
    destination identifies the sending team.
    """
    if transactions is None or transactions.empty:
        return transactions

    trade_rows: dict[tuple[int, str, str], tuple[str, str, int, int]] = {}
    years = sorted(
        int(year)
        for year in pd.to_numeric(transactions.get("year"), errors="coerce").dropna().unique()
    )
    for year in years:
        transaction_dir = output_dir / str(year) / "transactions"
        # The cookie fetcher now captures per-team pages because Yahoo's
        # aggregate all-team view omits historical rows.  Keep page_* support
        # for older captures and de-duplicate the two glob families.
        pages = {
            *transaction_dir.glob("page_*.html"),
            *transaction_dir.glob("team_*_page_*.html"),
        }
        for page in sorted(pages):
            html = page.read_text(encoding="utf-8")
            trs = re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.I | re.S)
            groups: dict[str, list[tuple[str, str, int]]] = {}
            for row in trs:
                if "Traded to" not in row:
                    continue
                players = re.findall(
                    r"sports\.yahoo\.com/nfl/(?:players/(\d+)|teams/([^\"']+))[^>]*>(.*?)</a>",
                    row,
                    flags=re.I | re.S,
                )
                destination = re.search(
                    r"/f1/\d+/(\d+)[\"'][^>]*>(.*?)</a>", row, flags=re.I | re.S
                )
                timestamp = re.search(r"F-timestamp[^>]*>(.*?)</span>", row, flags=re.I | re.S)
                if not players or not destination or not timestamp:
                    continue
                clean = lambda value: re.sub(r"<[^>]+>", "", value).strip()
                destination_name = clean(destination.group(2))
                group = groups.setdefault((clean(timestamp.group(1))), [])
                group.extend(
                    (
                        player_id or f"team:{team_slug.rstrip('/')}",
                        destination_name,
                        int(destination.group(1)),
                    )
                    for player_id, team_slug, _player_html in players
                )
            for timestamp, assets in groups.items():
                destinations = {name for _, name, _ in assets}
                # Yahoo renders multi-asset trades as one row per side.  A
                # 3-for-3 trade therefore produces six assets but still only
                # two destination teams; rejecting anything except exactly
                # two assets silently drops the second side's trade metadata.
                if len(assets) < 2 or len(destinations) != 2:
                    continue
                for player_id, destination_name, destination_number in assets:
                    partner_name = next(name for name in destinations if name != destination_name)
                    partner_number = next(
                        number for _, name, number in assets if name == partner_name
                    )
                    trade_rows[(int(year), player_id, timestamp)] = (
                        destination_name,
                        partner_name,
                        destination_number,
                        partner_number,
                    )

    out = transactions.copy()
    for index, row in out.iterrows():
        key = (int(row["year"]), str(row["editorial_player_id"]), str(row["timestamp"]).strip())
        recovered = trade_rows.get(key)
        if recovered:
            out.at[index, "action"] = "Trade"
            out.at[index, "team"] = recovered[0]
            out.at[index, "trade_partner_team"] = recovered[1]
            out.at[index, "team_number"] = recovered[2]
            out.at[index, "trade_partner_team_number"] = recovered[3]
    return out


def prepare_transaction_source(
    transactions: pd.DataFrame,
    team_names: dict[tuple[int, int], str],
    identities: dict[tuple[int, int], dict[str, str]],
    league_key_by_year: dict[int, str] | None = None,
    roster: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Map the web transaction observations into canonical transaction fields."""
    if transactions is None or transactions.empty:
        return pd.DataFrame()
    out = transactions.copy().reset_index(drop=True)
    # Prefer the deterministic source ID assigned by the web normalizer.  The
    # fallback preserves compatibility with older captures that only had the
    # display/action fields.
    fallback_ids = pd.Series(
        [f"web-{int(y)}-{int(i):06d}" for i, y in enumerate(out["year"])], index=out.index
    )
    if "transaction_id" not in out.columns:
        out["transaction_id"] = fallback_ids
    else:
        out["transaction_id"] = out["transaction_id"].where(
            out["transaction_id"].notna() & out["transaction_id"].astype(str).str.strip().ne(""),
            fallback_ids,
        )
    out["transaction_sequence"] = 0
    derived_type = (
        out["action"]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"added player": "add", "dropped player": "drop", "trade": "trade", "": None})
    )
    if "transaction_type" not in out.columns:
        out["transaction_type"] = derived_type
    else:
        out["transaction_type"] = out["transaction_type"].where(
            out["transaction_type"].notna() & out["transaction_type"].astype(str).str.strip().ne(""),
            derived_type,
        )
    if "status" not in out.columns:
        out["status"] = "successful"
    else:
        out["status"] = out["status"].fillna("successful")
    # The web page exposes a human timestamp only to the minute.  The quick
    # importer supplies ``timestamp_epoch`` from that display value; retain
    # the original display text as evidence while satisfying the canonical
    # BIGINT timestamp contract.
    if "timestamp_epoch" in out.columns:
        out["timestamp"] = pd.to_numeric(out["timestamp_epoch"], errors="coerce")
    else:
        out["timestamp"] = pd.to_numeric(out.get("timestamp"), errors="coerce")
        missing_ts = out["timestamp"].isna()
        if missing_ts.any():
            fallback_ts = pd.Series(
                [
                    parse_web_timestamp_epoch(
                        out.at[idx, "transaction_datetime"]
                        if "transaction_datetime" in out.columns
                        else transactions.at[idx, "timestamp"],
                        int(out.at[idx, "year"]),
                    )
                    for idx in out.index[missing_ts]
                ],
                index=out.index[missing_ts],
                dtype="float64",
            )
            out["timestamp"] = out["timestamp"].where(~missing_ts, fallback_ts)
    if "transaction_datetime" not in out.columns:
        out["transaction_datetime"] = None
    display_ts = out.get("timestamp_display", out.get("timestamp_text", None))
    if display_ts is not None:
        out["transaction_datetime"] = out["transaction_datetime"].where(
            out["transaction_datetime"].notna() & out["transaction_datetime"].astype(str).str.strip().ne(""),
            display_ts,
        )
    elif "timestamp" in transactions.columns:
        out["transaction_datetime"] = out["transaction_datetime"].where(
            out["transaction_datetime"].notna() & out["transaction_datetime"].astype(str).str.strip().ne(""),
            transactions["timestamp"],
        )
    if "source_type" not in out.columns:
        out["source_type"] = None
    source_label = out.get("source_label", pd.Series([""] * len(out), index=out.index)).astype(str).str.lower()
    out["source_type"] = out["source_type"].where(
        out["source_type"].notna() & out["source_type"].astype(str).str.strip().ne(""),
        pd.Series(
            [
                "waivers" if "waiver" in label else ("team" if typ in {"drop", "trade"} else "freeagents")
                for label, typ in zip(source_label, out["transaction_type"])
            ],
            index=out.index,
        ),
    )
    if "destination" not in out.columns:
        out["destination"] = None
    out["destination"] = out["destination"].where(
        out["destination"].notna() & out["destination"].astype(str).str.strip().ne(""),
        out["transaction_type"].map(
            lambda typ: "freeagents" if typ == "drop" else "team"
        ),
    )
    if "faab_bid" not in out.columns:
        out["faab_bid"] = 0
    out["faab_bid"] = pd.to_numeric(out["faab_bid"], errors="coerce").fillna(0)
    out["team_name"] = out.get("team")
    if "team_number" not in out.columns:
        out["team_number"] = pd.NA
    out["team_number"] = pd.to_numeric(out["team_number"], errors="coerce").astype("Int64")
    name_lookup: dict[tuple[int, str], dict[str, str]] = {}
    number_lookup: dict[tuple[int, int], dict[str, str]] = {}
    for (year, number), name in team_names.items():
        if name:
            identity = dict(_identity(identities, year, number))
            identity["team_number"] = number
            name_lookup[(year, name)] = identity
            number_lookup[(year, number)] = identity
    info = []
    for _, row in out.iterrows():
        year = int(row["year"])
        number = row.get("team_number")
        if pd.notna(number):
            identity = number_lookup.get((year, int(number)), {})
        else:
            identity = name_lookup.get((year, str(row["team_name"])), {})
        info.append(identity)
    out["manager"] = [x.get("manager") for x in info]
    out["manager_guid"] = [x.get("manager_guid") for x in info]
    out["franchise_id"] = out["manager_guid"]
    league_key_by_year = league_key_by_year or {}
    out["team_key"] = [
        f"{league_key_by_year.get(int(y), '')}.t.{int(n)}"
        if pd.notna(n) and league_key_by_year.get(int(y))
        else None
        for y, n in zip(out["year"], out["team_number"])
    ]
    is_trade = out["transaction_type"].eq("trade")
    trade_partner_team = (
        out["trade_partner_team"]
        if "trade_partner_team" in out.columns
        else pd.Series([None] * len(out), index=out.index, dtype="object")
    )
    out["source_team_name"] = trade_partner_team.where(is_trade, None)
    if "trade_partner_team_number" not in out.columns:
        out["trade_partner_team_number"] = pd.NA
    out["trade_partner_team_number"] = pd.to_numeric(
        out["trade_partner_team_number"], errors="coerce"
    ).astype("Int64")
    source_info = []
    for _, row in out.iterrows():
        year = int(row["year"])
        number = row.get("trade_partner_team_number")
        if pd.notna(number):
            source_info.append(number_lookup.get((year, int(number)), {}))
        else:
            source_info.append(name_lookup.get((year, str(row["source_team_name"])), {}))
    out["source_manager"] = [x.get("manager") for x in source_info]
    out["source_manager_guid"] = [x.get("manager_guid") for x in source_info]
    out["source_franchise_id"] = out["source_manager_guid"]
    out["source_team_key"] = [
        f"{league_key_by_year.get(int(y), '')}.t.{int(n)}"
        if pd.notna(n) and league_key_by_year.get(int(y))
        else None
        for y, n in zip(out["year"], out["trade_partner_team_number"])
    ]
    out["destination_team_name"] = out["team_name"].where(is_trade, None)
    out["destination_manager"] = out["manager"].where(is_trade, None)
    out["destination_manager_guid"] = out["manager_guid"].where(is_trade, None)
    out["destination_franchise_id"] = out["franchise_id"].where(is_trade, None)
    out["trade_direction"] = out["transaction_type"].map(lambda value: "received" if value == "trade" else None)
    out["yahoo_player_id"] = out["editorial_player_id"].where(
        out["editorial_player_id"].astype(str).str.fullmatch(r"\d+"), None
    )
    # Yahoo renders DST transactions as team slugs (for example
    # ``team:detroit``) instead of numeric player IDs.  The roster pages from
    # the same capture contain the canonical Yahoo IDs for those DST records;
    # use an unambiguous year/player-name map at this parser boundary.
    roster_ids: dict[tuple[int, str], set[str]] = {}
    roster_ids_global: dict[str, set[str]] = {}
    if roster is not None and not roster.empty and "yahoo_player_id" in roster.columns:
        roster_for_ids = roster.copy()
        # Single-season source backups created by the quick importer may omit
        # the redundant year column; the transaction capture supplies it.
        if "year" not in roster_for_ids.columns:
            years = out["year"].dropna().unique()
            if len(years) == 1:
                roster_for_ids["year"] = int(years[0])
        if "year" not in roster_for_ids.columns:
            roster_for_ids = pd.DataFrame()
        for row in roster_for_ids.itertuples(index=False):
            player_id = getattr(row, "yahoo_player_id", None)
            player_name = getattr(row, "player", None)
            if pd.isna(player_id) or not str(player_name or "").strip():
                continue
            key = (int(row.year), re.sub(r"\s+", " ", str(player_name).strip()).casefold())
            roster_ids.setdefault(key, set()).add(str(player_id))
            roster_ids_global.setdefault(key[1], set()).add(str(player_id))
    recovered_ids = []
    for row in out.itertuples(index=False):
        current = getattr(row, "yahoo_player_id", None)
        if current is not None and not pd.isna(current) and str(current).strip():
            recovered_ids.append(current)
            continue
        key = (int(row.year), re.sub(r"\s+", " ", str(row.player).strip()).casefold())
        matches = roster_ids.get(key, set())
        if not matches:
            matches = roster_ids_global.get(key[1], set())
        recovered_ids.append(next(iter(matches)) if len(matches) == 1 else current)
    out["yahoo_player_id"] = recovered_ids
    # Fly's canonical transaction table stores both manager perspectives for
    # a trade under the same transaction_id.  Yahoo's web page exposes each
    # asset once with its receiving team; synthesize the corresponding sent
    # perspective from the captured partner fields.
    perspective_mask = is_trade & out["trade_partner_team_number"].notna()
    sent = out.loc[perspective_mask].copy()
    if not sent.empty:
        original_manager = sent["manager"].copy()
        original_manager_guid = sent["manager_guid"].copy()
        original_franchise_id = sent["franchise_id"].copy()
        original_team_name = sent["team_name"].copy()
        original_team_key = sent["team_key"].copy()
        sent["manager"] = sent["source_manager"]
        sent["manager_guid"] = sent["source_manager_guid"]
        sent["franchise_id"] = sent["source_franchise_id"]
        sent["team_name"] = sent["source_team_name"]
        sent["team_key"] = sent["source_team_key"]
        sent["source_manager"] = original_manager
        sent["source_manager_guid"] = original_manager_guid
        sent["source_franchise_id"] = original_franchise_id
        sent["source_team_name"] = original_team_name
        sent["source_team_key"] = original_team_key
        sent["trade_direction"] = "sent"
        out = pd.concat([out, sent], ignore_index=True)
    out["platform"] = "yahoo"
    return out


def prepare_schedule_source(matchups: pd.DataFrame, identities: dict[tuple[int, int], dict[str, str]], league_key: str) -> pd.DataFrame:
    """Derive the canonical schedule source from the same Yahoo scorecards."""
    if matchups is None or matchups.empty:
        return pd.DataFrame()
    out = prepare_matchup_source(matchups, identities, league_key)
    out["franchise_id"] = out["manager_guid"]
    out["opponent_franchise_id"] = out["opponent_guid"]
    out["team_name"] = out["team"]
    out["win"] = (out["team_points"] > out["opponent_points"]).astype(int)
    out["loss"] = (out["team_points"] < out["opponent_points"]).astype(int)
    return out


def settings_payload_to_flat_row(
    payload: dict[str, Any],
    year: int,
    league_key: str,
    db_name: str,
    *,
    observed_team_count: int | None = None,
) -> dict[str, Any]:
    """Convert the parser's settings payload to the canonical flat row."""
    from multi_league.core.canonical_settings import flatten_settings

    metadata = dict(payload.get("metadata") or {})
    if observed_team_count is not None and int(observed_team_count) > 0:
        # The authenticated standings table is the season's actual active
        # field. Yahoo's archived settings page can instead show the later
        # configured maximum, which corrupts historical team-count changes.
        metadata["num_teams"] = int(observed_team_count)
    scoring = {
        str(key): value for key, value in (payload.get("scoring") or {}).items() if str(key).startswith("scoring_")
    }
    positions = payload.get("roster_positions") or []
    counts: dict[str, int] = {}
    for position in positions:
        key = str(position).strip().upper().replace("W/R/T", "FLEX")
        counts[key] = counts.get(key, 0) + 1
    raw = {
        "metadata": metadata,
        "canonical_scoring": {key.removeprefix("scoring_"): value for key, value in scoring.items()},
        "roster_position_counts": counts,
    }
    row = flatten_settings(raw, platform="yahoo", year=year, league_key=league_key)
    row["db_name"] = db_name
    return row


def infer_bracket_settings(matchups: pd.DataFrame, year: int) -> dict[str, int] | None:
    """Infer Yahoo postseason settings from the observed bracket shape.

    Yahoo's archived settings page leaves the playoff row blank for older
    KMFFL seasons, but the scorecards still expose the completed bracket.  A
    six-team Yahoo bracket has two first-round games, four semifinal-team
    records in the next week, and the championship pair in the final week;
    consolation games account for the remaining records.  Require the full
    4/10/8 row signature so this cannot silently turn an arbitrary schedule
    into playoff settings.
    """
    if matchups is None or matchups.empty:
        return None
    year_rows = matchups.loc[matchups["year"].astype(int) == int(year)].copy()
    counts = (
        year_rows.groupby("week")
        .size()
        .astype(int)
        .to_dict()
    )
    print(f"[yahoo-bracket] year={int(year)} weekly_records={dict(sorted(counts.items()))}", flush=True)

    # The authenticated Yahoo web matchup module renders separate literal
    # "Championship Bracket" and "Consolation Bracket" sections.  Those
    # labels are more authoritative than a row-count shape: byes and
    # placement games can leave every week with the same number of cards.
    # Use them first whenever the cookie capture preserved the flags.
    if "is_playoffs" in year_rows.columns:
        playoff_mask = year_rows["is_playoffs"].astype(str).str.strip().str.lower().isin({"1", "true", "yes"})
        explicit_rows = year_rows.loc[playoff_mask].copy()
        if not explicit_rows.empty:
            start = int(pd.to_numeric(explicit_rows["week"], errors="coerce").dropna().min())
            final_week = int(pd.to_numeric(year_rows["week"], errors="coerce").dropna().max())
            championship_rows = explicit_rows
            if "is_consolation" in explicit_rows.columns:
                consolation_mask = explicit_rows["is_consolation"].astype(str).str.strip().str.lower().isin(
                    {"1", "true", "yes"}
                )
                championship_rows = explicit_rows.loc[~consolation_mask]
            team_numbers: set[int] = set()
            for column in ("team_number", "opponent_team_number"):
                if column not in championship_rows.columns:
                    continue
                values = pd.to_numeric(championship_rows[column], errors="coerce").dropna().astype(int)
                team_numbers.update(value for value in values if value > 0)
            result = {
                "playoff_start_week": start,
                "regular_season_weeks": start - 1,
                "end_week": final_week,
            }
            if len(team_numbers) >= 2:
                result["playoff_teams"] = len(team_numbers)
            print(
                f"[yahoo-bracket] year={int(year)} source=literal-web-brackets "
                f"playoff_start_week={start} playoff_teams={result.get('playoff_teams')}",
                flush=True,
            )
            return result

    for start in sorted(counts):
        if counts.get(start) == 4 and counts.get(start + 1) == 10 and counts.get(start + 2) == 8:
            return {
                "playoff_teams": 6,
                "playoff_start_week": int(start),
                "regular_season_weeks": int(start - 1),
                "end_week": int(start + 2),
            }
    # Older ten-team Yahoo leagues can omit the playoff row while retaining
    # the full ten-team consolation schedule in both final weeks.  In that
    # archived shape, the four-team bracket is the two-week week-15/16 format.
    # Keep this fallback deliberately narrow so an incomplete schedule cannot
    # manufacture postseason settings for other league shapes.
    final_week = max(counts) if counts else None
    if (
        final_week is not None
        and final_week == 16
        and max(counts.values()) == 10
        and counts.get(final_week - 1, 0) >= 8
        and counts.get(final_week, 0) >= 8
    ):
        return {
            "playoff_teams": 4,
            "playoff_start_week": int(final_week - 1),
            "regular_season_weeks": int(final_week - 2),
            "end_week": int(final_week),
        }
    # Some archived Yahoo settings omit the playoff row even though the
    # scorecards expose the whole completed postseason.  The only boundary
    # that schedule evidence establishes safely is the first week after an
    # uninterrupted full-league schedule where every remaining scorecard is
    # reduced.  Consolation games can either restore the full weekly count or
    # stay reduced through the championship; both forms occur in Yahoo's
    # archives.  Do not infer bracket size here: first-round byes make four-
    # and six-team fields indistinguishable from matchup counts alone.
    full_count = max(counts.values()) if counts else 0
    first_week = min(counts) if counts else None
    if first_week is not None and full_count and all(count % 2 == 0 for count in counts.values()):
        for start in sorted(counts):
            if start <= first_week or counts[start] >= full_count:
                continue
            has_later_full_schedule = any(
                counts.get(week) == full_count for week in range(start + 1, int(final_week) + 1)
            )
            if (
                has_later_full_schedule
                and all(counts.get(week) == full_count for week in range(first_week, start))
            ):
                return {
                    "playoff_start_week": int(start),
                    "regular_season_weeks": int(start - 1),
                    "end_week": int(final_week),
                }
            has_completed_reduced_suffix = all(
                counts.get(week, 0) < full_count for week in range(start, int(final_week) + 1)
            )
            if (
                has_completed_reduced_suffix
                and all(counts.get(week) == full_count for week in range(first_week, start))
            ):
                return {
                    "playoff_start_week": int(start),
                    "regular_season_weeks": int(start - 1),
                    "end_week": int(final_week),
                }
    return None


def backfill_bracket_settings(payload: dict[str, Any], matchups: pd.DataFrame, year: int) -> dict[str, Any]:
    """Fill only settings Yahoo omitted when the cached bracket proves them."""
    inferred = infer_bracket_settings(matchups, year)
    if not inferred:
        return payload
    out = dict(payload)
    metadata = dict(out.get("metadata") or {})
    for key, value in inferred.items():
        current = metadata.get(key)
        if current is None or current == 0 or current == "":
            metadata[key] = value
    out["metadata"] = metadata
    return out


def _read_source(source_db: Path) -> dict[str, pd.DataFrame]:
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        required = {"roster", "transactions", "matchups_source", "draft", "team_identity", "standings"}
        missing = sorted(required - tables)
        if missing:
            raise RuntimeError(f"source backup is missing required tables: {', '.join(missing)}")
        return {name: conn.execute(f"SELECT * FROM {name}").fetchdf() for name in required}
    finally:
        conn.close()


def map_web_transaction_windows(transactions: pd.DataFrame, matchups: pd.DataFrame) -> pd.DataFrame:
    """Attach canonical week/date fields using the same matchup windows as API data."""
    if transactions is None or transactions.empty or matchups is None or matchups.empty:
        return transactions
    out = transactions.copy()
    if "timestamp" not in out.columns:
        return out
    timestamp = pd.to_numeric(out["timestamp"], errors="coerce")
    parsed = pd.to_datetime(timestamp, unit="s", errors="coerce", utc=True).dt.tz_localize(None)
    for col in ("week", "week_start", "week_end"):
        if col not in out.columns:
            out[col] = None
    for idx in out.index:
        if pd.isna(parsed.loc[idx]):
            continue
        year = int(out.at[idx, "year"])
        windows = matchups.loc[matchups["year"].eq(year)].copy()
        if windows.empty or "week" not in windows.columns:
            continue
        max_week = int(pd.to_numeric(windows["week"], errors="coerce").max())
        if not {"week_start", "week_end"}.issubset(windows.columns):
            # Some web-only source backups contain week numbers but not the
            # derived matchup date windows.  Yahoo's football weeks begin on
            # the first Thursday of September; use that stable calendar rule
            # so the raw contract is populated before shared transforms.
            first = pd.Timestamp(year=year, month=9, day=1)
            first_thursday = first + pd.Timedelta(days=(3 - first.weekday()) % 7)
            inferred_week = max(1, min(max_week, int((parsed.loc[idx] - first_thursday).days // 7) + 1))
            out.at[idx, "week"] = inferred_week
            continue
        windows["_start"] = pd.to_datetime(windows["week_start"], errors="coerce")
        windows["_end"] = pd.to_datetime(windows["week_end"], errors="coerce") + pd.Timedelta(days=1)
        windows = windows.loc[windows["_start"].notna() & windows["_end"].notna()].sort_values("week")
        if windows.empty:
            continue
        instant = parsed.loc[idx]
        match = windows.loc[(instant >= windows["_start"]) & (instant < windows["_end"])]
        if match.empty:
            # Pre-week transactions belong to week 1; late transactions use
            # the final known window, matching Yahoo API fallback behavior.
            match = windows.iloc[[0]] if instant < windows.iloc[0]["_start"] else windows.iloc[[-1]]
        row = match.iloc[0]
        out.at[idx, "week"] = int(row["week"])
        out.at[idx, "week_start"] = row["week_start"]
        out.at[idx, "week_end"] = row["week_end"]
    out["week"] = pd.to_numeric(out["week"], errors="coerce").astype("Int64")
    return out


def build_frames(
    source_db: Path,
    output_dir: Path,
    start_year: int | None = None,
    end_year: int | None = None,
    league_keys: dict[int, str] | None = None,
    team_count: int = 10,
) -> dict[str, pd.DataFrame]:
    """Read source observations and prepare canonical raw frames."""
    source = _read_source(source_db)
    identities = build_team_identity_maps(source["team_identity"])
    matchups = source["matchups_source"]
    observed_team_counts = {
        int(year): int(count)
        for year, count in source["standings"].groupby("year").size().items()
    }
    team_names = _team_name_map(matchups)
    ops_path = Path(os.environ["OPS_CACHE_PATH"])
    ops = duckdb.connect(str(ops_path), read_only=True)
    try:
        objects = {(row[1], row[2]) for row in ops.execute("SHOW ALL TABLES").fetchall()}
        bio_table = '"nfl_historical"."player_bio"' if ("nfl_historical", "player_bio") in objects else '"public"."player_bio"'
        bio = ops.execute(
            f"SELECT NFL_player_id, yahoo_player_id, nfl_position FROM {bio_table}"
        ).fetchdf()
    finally:
        ops.close()

    rosters: list[pd.DataFrame] = []
    matchups_out: list[pd.DataFrame] = []
    schedules_out: list[pd.DataFrame] = []
    settings: list[dict[str, Any]] = []
    league_keys = league_keys or KMFFL_LEAGUE_KEYS
    selected_years = [
        year
        for year in sorted(league_keys)
        if (start_year is None or year >= int(start_year)) and (end_year is None or year <= int(end_year))
    ]
    for year in selected_years:
        league_key = league_keys[year]
        year_roster = source["roster"].loc[source["roster"]["year"] == year]
        year_matchup = matchups.loc[matchups["year"] == year]
        rosters.append(prepare_roster_source(year_roster, identities, team_names, bio, league_key))
        matchups_out.append(prepare_matchup_source(year_matchup, identities, league_key))
        schedules_out.append(prepare_schedule_source(year_matchup, identities, league_key))
        settings_file = output_dir / str(year) / "settings.html"
        payload = parse_settings_page(settings_file.read_text(encoding="utf-8"), league_key, year)
        payload = backfill_bracket_settings(payload, matchups, year)
        settings.append(
            settings_payload_to_flat_row(
                payload,
                year,
                league_key,
                "kmffl",
                observed_team_count=observed_team_counts.get(year),
            )
        )

    cookie_transactions = prepare_transaction_source(
        repair_trade_rows_from_cached_pages(source["transactions"], output_dir),
        team_names,
        identities,
        {year: league_keys[year] for year in selected_years},
        roster=source["roster"],
    )
    cookie_transactions = map_web_transaction_windows(cookie_transactions, matchups)
    frames = {
        "player_fantasy": pd.concat(rosters, ignore_index=True),
        "matchup": pd.concat(matchups_out, ignore_index=True),
        "league_settings": pd.DataFrame(settings),
        "draft": prepare_draft_source(
            source["draft"], team_names, identities,
            {year: league_keys[year] for year in selected_years},
            roster=source["roster"],
            team_count=team_count,
            team_count_by_year=observed_team_counts,
        ),
        "transactions": cookie_transactions,
        "schedule": pd.concat(schedules_out, ignore_index=True),
    }
    # Keep the canonical league identifier stable across every published table.
    # LocalLeagueDB otherwise defaults missing db_name values to the generated
    # DuckDB filename, which makes cross-table validation and joins fail.
    for frame in frames.values():
        frame["db_name"] = "kmffl"
    return frames


def write_model(
    frames: dict[str, pd.DataFrame],
    output_dir: Path,
    db_name: str = "kmffl_2015_2025_cookie_model",
    source_league_id: str = "kmffl",
) -> Path:
    """Write canonical DDL tables to a distinct model file."""
    from multi_league.core.local_db import LocalLeagueDB

    with LocalLeagueDB(output_dir, db_name) as db:
        for table in ("league_settings", "matchup", "player_fantasy", "draft", "transactions", "schedule"):
            frame = frames.get(table)
            if frame is not None and not frame.empty:
                # The shared pipeline filters every table by the active
                # database name.  Keep the source league identity in
                # ``league_id`` but align the published DDL rows with the
                # actual local database/catalog selected by the runner.
                frame = frame.copy()
                frame["db_name"] = db_name
                db.save_table(table, frame, platform="yahoo", league_id=source_league_id)
    return output_dir / f"{db_name}.duckdb"


def run_local_enrichments(
    output_dir: Path,
    db_name: str = "kmffl_2015_2025_cookie_model",
    *,
    quick: bool = False,
) -> dict[str, Any]:
    """Run the shared SQL enrichment waves against the isolated model file."""
    from multi_league.transformations.sql_enrichments import SQLEnrichments
    from multi_league.core.import_pipeline import run_local_fantasy_aggregation

    with SQLEnrichments(db_name, data_dir=str(output_dir), dry_run=False, quick=quick) as enricher:
        roster_by_year, scoring_params = enricher.load_settings_from_db()
        if roster_by_year:
            enricher.roster_by_year = roster_by_year
            enricher._update_scoring_params(scoring_params)
        results = enricher.run_all()
        fantasy_ok = run_local_fantasy_aggregation(
            db_name=db_name,
            data_dir=str(output_dir),
            dry_run=False,
            conn=enricher.conn,
        )
        results["fantasy_aggregation"] = -1 if fantasy_ok else ("error", "local fantasy aggregation failed")
        return results


def normalize_model_db_name(model_path: Path, db_name: str = "kmffl") -> None:
    """Normalize db_name after enrichments add expanded rows."""
    import duckdb

    conn = duckdb.connect(str(model_path))
    try:
        for table in ("league_settings", "matchup", "player_fantasy", "draft", "transactions", "schedule"):
            conn.execute(f'UPDATE public."{table}" SET db_name = ?', [db_name])
    finally:
        conn.close()


def restore_trade_metadata(model_path: Path, source_transactions: pd.DataFrame) -> None:
    """Restore sender/receiver fields after transaction enrichments duplicate trade rows."""
    import duckdb

    trade_source = source_transactions.loc[
        source_transactions["transaction_type"].eq("trade"),
        [
            "transaction_id",
            "player",
            "year",
            "source_manager",
            "source_manager_guid",
            "source_team_name",
            "source_franchise_id",
            "destination_manager",
            "destination_manager_guid",
            "destination_team_name",
            "destination_franchise_id",
        ],
    ].drop_duplicates("transaction_id")
    if trade_source.empty:
        return
    conn = duckdb.connect(str(model_path))
    try:
        conn.register("trade_source", trade_source)
        conn.execute(
            """
            UPDATE public.transactions AS t
            SET
                source_manager = CASE WHEN t.manager = s.destination_manager THEN s.source_manager ELSE s.destination_manager END,
                source_manager_guid = CASE WHEN t.manager = s.destination_manager THEN s.source_manager_guid ELSE s.destination_manager_guid END,
                source_team_name = CASE WHEN t.manager = s.destination_manager THEN s.source_team_name ELSE s.destination_team_name END,
                source_franchise_id = CASE WHEN t.manager = s.destination_manager THEN s.source_franchise_id ELSE s.destination_franchise_id END,
                destination_manager = CASE WHEN t.manager = s.destination_manager THEN s.destination_manager ELSE s.source_manager END,
                destination_manager_guid = CASE WHEN t.manager = s.destination_manager THEN s.destination_manager_guid ELSE s.source_manager_guid END,
                destination_team_name = CASE WHEN t.manager = s.destination_manager THEN s.destination_team_name ELSE s.source_team_name END,
                destination_franchise_id = CASE WHEN t.manager = s.destination_manager THEN s.destination_franchise_id ELSE s.source_franchise_id END,
                trade_direction = CASE WHEN t.manager = s.destination_manager THEN 'received' ELSE 'sent' END
            FROM trade_source AS s
            WHERE t.transaction_type = 'trade'
              AND t.transaction_id = s.transaction_id
            """
        )
        conn.unregister("trade_source")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ops-cache", type=Path, required=True)
    parser.add_argument("--skip-enrichments", action="store_true")
    args = parser.parse_args()
    os.environ.setdefault("CORPUS_MODE", "1")
    os.environ["OPS_CACHE_PATH"] = str(args.ops_cache)
    frames = build_frames(args.source_db, args.output_dir)
    path = write_model(frames, args.output_dir)
    enrichment_results: dict[str, Any] = {}
    if not args.skip_enrichments:
        enrichment_results = run_local_enrichments(args.output_dir)
    normalize_model_db_name(path)
    restore_trade_metadata(path, frames["transactions"])
    print(
        json.dumps(
            {
                "model": str(path),
                "rows": {key: len(value) for key, value in frames.items()},
                "enrichments": enrichment_results,
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
