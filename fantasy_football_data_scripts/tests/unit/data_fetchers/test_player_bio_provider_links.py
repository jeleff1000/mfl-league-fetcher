"""Durable provider-ID links in player_bio: fill only what is provable, never guess.

Regressions (2026-11-24): Greg Desrosiers Jr. (ESPN 4691889, no ESPN ID in any
public directory) blocked the `fantasy` ESPN update; Matt Hibner/Trey Smack/
Dominic Zvada stayed unlinked in ~40 Sleeper leagues because Sleeper links were
only ever resolved in a disposable per-update cache; Sleeper 11376/13353 were
each carried by two bio rows.
"""

import duckdb
import pandas as pd
import pytest

from multi_league.data_fetchers import live_nfl_ops_refresh as live


def _bio(rows):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute(
        "CREATE TABLE nfl_historical.player_bio (NFL_player_id VARCHAR, player VARCHAR, "
        "birth_date VARCHAR, nfl_position VARCHAR, latest_team VARCHAR, espn_id VARCHAR, "
        "sleeper_player_id DOUBLE)"
    )
    con.executemany("INSERT INTO nfl_historical.player_bio VALUES (?,?,?,?,?,?,?)", rows)
    con.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (NFL_player_id VARCHAR, year INTEGER)")
    con.executemany(
        "INSERT INTO nfl_historical.nfl_player_stats_all VALUES (?, 2026)", [(r[0],) for r in rows]
    )
    return con


ROSTER = pd.DataFrame([
    # nflverse roster: Hibner has Rotowire + ESPN; Desrosiers has only GSIS.
    {"gsis_id": "00-0040879", "full_name": "Matthew Hibner", "birth_date": "2002-03-09", "team": "BAL",
     "position": "TE", "espn_id": "4432260", "rotowire_id": "19364", "sleeper_id": None},
    {"gsis_id": "00-0041376", "full_name": "Gregory Desrosiers", "birth_date": "2001-03-27", "team": "NYG",
     "position": "RB", "espn_id": None, "rotowire_id": None, "sleeper_id": None},
    {"gsis_id": "00-0041047", "full_name": "Chris Brazzell II", "birth_date": "2003-10-23", "team": "CAR",
     "position": "WR", "espn_id": "5091739", "rotowire_id": "19239", "sleeper_id": None},
])
SLEEPER = pd.DataFrame([
    {"player_id": "13324", "full_name": "Matt Hibner", "birth_date": "2002-03-09", "team": "BAL",
     "position": "TE", "gsis_id": None, "espn_id": None, "rotowire_id": 19364},
    {"player_id": "13603", "full_name": "Gregory Desrosiers Jr.", "birth_date": "2001-03-27", "team": "NYG",
     "position": "RB", "gsis_id": None, "espn_id": None, "rotowire_id": 19655},
    {"player_id": "13353", "full_name": "Chris Brazzell II", "birth_date": "2003-09-22", "team": "CAR",
     "position": "WR", "gsis_id": None, "espn_id": None, "rotowire_id": 19239},
    {"player_id": "11376", "full_name": "Richard Gouraige", "birth_date": "1998-10-15", "team": None,
     "position": "OL", "gsis_id": None, "espn_id": None, "rotowire_id": None},
    {"player_id": "99999", "full_name": "Nobody Matches", "birth_date": "2000-01-01", "team": "DAL",
     "position": "WR", "gsis_id": None, "espn_id": None, "rotowire_id": None},
])
ESPN = pd.DataFrame([
    {"espn_id": "4691889", "full_name": "Greg Desrosiers Jr.", "birth_date": "2001-03-27", "position": "RB", "team": "NYG"},
])


def test_fill_links_sleeper_by_stable_id_and_exact_bio_and_espn_by_roster_witness():
    con = _bio([
        ("00-0040879", "Matthew Hibner", "2002-03-09", "TE", "BAL", "4432260", None),
        ("00-0041376", "Gregory Desrosiers", "2001-03-27", "RB", "NYG", None, None),
    ])
    receipt = live.fill_missing_provider_links(
        con, year=2026, sources={"roster": ROSTER, "sleeper": SLEEPER, "espn": ESPN},
    )
    rows = dict(con.execute(
        "SELECT NFL_player_id, struct_pack(s := sleeper_player_id, e := espn_id) FROM nfl_historical.player_bio"
    ).fetchall())
    assert rows["00-0040879"] == {"s": 13324.0, "e": "4432260"}   # Rotowire anchor
    assert rows["00-0041376"] == {"s": 13603.0, "e": "4691889"}   # exact bio + ESPN roster witness
    assert receipt["sleeper_linked"] == 2 and receipt["espn_linked"] == 1


def test_existing_links_are_never_overwritten():
    con = _bio([("00-0040879", "Matthew Hibner", "2002-03-09", "TE", "BAL", "1111111", 55555.0)])
    live.fill_missing_provider_links(con, year=2026, sources={"roster": ROSTER, "sleeper": SLEEPER, "espn": ESPN})
    assert con.execute("SELECT espn_id, sleeper_player_id FROM nfl_historical.player_bio").fetchall() == [
        ("1111111", 55555.0)
    ]


@pytest.mark.parametrize("change", ["birth_date", "team", "position", "surname"])
def test_espn_witness_requires_every_field_to_agree(change):
    bio = pd.DataFrame([{"NFL_player_id": "00-0041376", "player": "Gregory Desrosiers",
                         "birth_date": "2001-03-27", "nfl_position": "RB", "latest_team": "NYG"}])
    espn = ESPN.copy()
    espn[{"surname": "full_name"}.get(change, change)] = {
        "birth_date": "2001-03-28", "team": "NYJ", "position": "WR", "surname": "Greg Smith",
    }[change]
    assert live.build_espn_bio_link_rows(bio, espn, used_espn_ids=set()).empty


def test_espn_witness_rejects_ambiguous_keys_and_already_used_ids():
    bio = pd.DataFrame([
        {"NFL_player_id": "a", "player": "Greg Desrosiers", "birth_date": "2001-03-27", "nfl_position": "RB", "latest_team": "NYG"},
        {"NFL_player_id": "b", "player": "Gary Desrosiers", "birth_date": "2001-03-27", "nfl_position": "RB", "latest_team": "NYG"},
    ])
    assert live.build_espn_bio_link_rows(bio, ESPN, used_espn_ids=set()).empty
    assert live.build_espn_bio_link_rows(bio.iloc[:1], ESPN, used_espn_ids={"4691889"}).empty
    # Stathead team codes in the bio are normalized to NFLverse/ESPN codes.
    gb = pd.DataFrame([{"NFL_player_id": "k", "player": "Trey Smack", "birth_date": "2003-07-12",
                        "nfl_position": "K", "latest_team": "GNB"}])
    espn_gb = pd.DataFrame([{"espn_id": "4869461", "full_name": "Trey Smack", "birth_date": "2003-07-12",
                             "position": "K", "team": "GB"}])
    assert live.build_espn_bio_link_rows(gb, espn_gb, used_espn_ids=set()).to_dict("records") == [
        {"NFL_player_id": "k", "espn_id": "4869461"}
    ]


def test_shared_sleeper_ids_are_repaired_only_with_a_witness():
    con = _bio([
        ("00-0038676", "Richard Gouraige", "1998-10-15", "OT", None, None, 11376.0),
        ("00-0039677", "Sam Hartman", "1999-07-29", "QB", "WAS", None, 11376.0),
        ("00-0001719", "Chris Brazzell", "1976-05-22", "WR", "DAL", None, 13353.0),
        ("00-0041047", "Chris Brazzell II", "2003-10-23", "WR", "CAR", None, 13353.0),
        ("00-0000001", "Twin A", "2000-01-01", "WR", "DAL", None, 77777.0),
        ("00-0000002", "Twin B", "2000-01-01", "WR", "DAL", None, 77777.0),
    ])
    sleeper = pd.concat([SLEEPER, pd.DataFrame([{"player_id": "77777", "full_name": "Twin", "birth_date": "2000-01-01",
                                                 "team": "DAL", "position": "WR"}])])
    repairs = live.repair_contradicted_sleeper_links(con, sleeper, roster=ROSTER)
    owners = dict(con.execute(
        "SELECT NFL_player_id, sleeper_player_id FROM nfl_historical.player_bio"
    ).fetchall())
    assert owners["00-0038676"] == 11376.0 and owners["00-0039677"] is None   # birth date witness
    assert owners["00-0041047"] == 13353.0 and owners["00-0001719"] is None   # Rotowire witness beats a bad DOB
    assert owners["00-0000001"] == owners["00-0000002"] == 77777.0           # undecidable: untouched
    assert {r["witness"] for r in repairs} == {"birth_date", "stable_id"}


def test_unresolvable_directory_rows_are_skipped_not_fatal():
    rows = live.build_sleeper_bio_mapping_rows(ROSTER, SLEEPER[SLEEPER.player_id == "99999"], skip_unresolved=True)
    assert rows.empty and len(rows.attrs["skipped"]) == 1
    with pytest.raises(live.RefreshGateError):
        live.build_sleeper_bio_mapping_rows(ROSTER, SLEEPER[SLEEPER.player_id == "99999"])


def test_nflverse_sleeper_id_contradicted_by_sleeper_is_vetoed_not_recopied():
    """NFLverse lists Sam Hartman with Sleeper 11376 (Sleeper: Richard Gouraige);
    re-copying it nightly kept re-breaking the bio. Brazzell II's birth date
    differs by a month between directories but the surname agrees: kept."""
    directory = pd.DataFrame([
        {"gsis_id": "00-0039677", "full_name": "Sam Hartman", "birth_date": "1999-07-29", "sleeper_id": "11376"},
        {"gsis_id": "00-0041047", "full_name": "Chris Brazzell II", "birth_date": "2003-10-23", "sleeper_id": "13353"},
        {"gsis_id": "00-0000009", "full_name": "Unknown Guy", "birth_date": "2001-01-01", "sleeper_id": "424242"},
    ])
    vetted, vetoed = live.veto_contradicted_sleeper_ids(directory, SLEEPER)
    assert [v["sleeper_id"] for v in vetoed] == ["11376"]
    assert pd.isna(vetted.loc[0, "sleeper_id"])
    assert vetted.loc[1, "sleeper_id"] == "13353"
    assert vetted.loc[2, "sleeper_id"] == "424242"   # absent from Sleeper: no witness, unchanged
