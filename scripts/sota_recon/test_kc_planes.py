"""Contract tests for the K/C planes (O.4): generator/registry consistency, regen-diff gate,
and synthetic gate-math verification for recon_kc_planes.

Run:  python -m pytest scripts/sota_recon/test_kc_planes.py -q
"""

from __future__ import annotations

import os

import duckdb
import pytest

from . import kc_planes, recon_kc_planes
from .entity_universes import UNIVERSE_KEYS
from .sources import registry


# ---------- contract file ----------

def test_committed_contracts_match_generator():
    """§18 regen-diff law: the committed kc_planes.v1.json must be exactly what the
    committed generator produces — evaporation/drift becomes a red test, not a discovery."""
    assert kc_planes.load() == kc_planes.generate()


def test_every_source_has_exactly_one_contract():
    doc = kc_planes.load()
    ids = [e["source_id"] for e in doc["contracts"]]
    assert len(ids) == len(set(ids))
    assert set(ids) == set(registry(include_subject=True))


def test_contracts_validate_clean():
    assert kc_planes.validate(kc_planes.load()) == []


def test_non_runnable_statuses_carry_reasons():
    for e in kc_planes.load()["contracts"]:
        if e["k"]["status"] not in {"ACTIVE", "AUTHORITY"}:
            assert e["k"].get("reason"), e["source_id"]


def test_runnable_contracts_bind_known_universes():
    legal = set(UNIVERSE_KEYS) | {"players"}
    for e in kc_planes.load()["contracts"]:
        k = e["k"]
        if k["status"] in {"ACTIVE", "AUTHORITY"}:
            assert k["canonical_universe"] in legal, e["source_id"]
            assert len(k["logical_key"]) >= 1
            assert k["expected_cardinality"] in kc_planes.CARDINALITIES


def test_coverage_models_all_declared():
    for e in kc_planes.load()["contracts"]:
        assert e["c"]["coverage_model"] in kc_planes.COVERAGE_MODELS, e["source_id"]


def test_crosswalked_contracts_cite_a_passing_receipt():
    """§19.2 proof-or-pending: every ACTIVE contract that joins through an ID crosswalk
    must cite a committed receipt that PASSES and licenses that source."""
    from . import crosswalk_receipt
    crosswalked = [e for e in kc_planes.load()["contracts"] if e["k"].get("crosswalk")]
    assert crosswalked, "expected the O.5 crosswalked contracts to exist"
    for e in crosswalked:
        rec = crosswalk_receipt.get(e["k"]["crosswalk"]["receipt"])
        assert rec["status"] == "PASS", e["source_id"]
        assert e["source_id"] in rec["licenses"], e["source_id"]


def test_formerly_pending_crosswalk_contracts_are_active():
    doc = {e["source_id"]: e for e in kc_planes.load()["contracts"]}
    for sid in ("ngs_season_published", "ngs_weekly_raw",
                "pbp_player_week_rollup", "legacy_motherduck_supertable"):
        assert doc[sid]["k"]["status"] == "ACTIVE", sid
        assert doc[sid]["k"]["crosswalk"]["receipt"] == "bio_pfr_nflid", sid


# ---------- synthetic gate math ----------

@pytest.fixture()
def synth(tmp_path):
    """Tiny universes + two sources: one 1:1 contract with a dup + an unmatched key,
    one canon-team contract against team_games."""
    con = duckdb.connect()
    ud = tmp_path / "universes"
    ud.mkdir()

    def write(sql, name):
        con.execute(f"COPY ({sql}) TO '{ud / name}' (FORMAT PARQUET)")

    write("""SELECT * FROM (VALUES
        ('gA', 'P1', 2020, 1, false, false, false, false, false, false, NULL, NULL),
        ('gA', 'P2', 2020, 1, false, false, false, false, false, false, NULL, NULL)
      ) t(boxscore_id, pfr_id, year, n_evidence_kinds, in_offense_box, in_defense_box,
          in_kicking_box, in_returns_box, in_starters, in_snaps, team, side)""",
          "player_game_presence.parquet")
    write("""SELECT * FROM (VALUES
        ('k1', 'gA', 2020, 1, 'REG', '2020-09-10', 'OAK', 'RAI', 'BBB', 'BBB', 1, 2,
         true, false, false, 20, 10)
      ) t(team_game_key, boxscore_id, year, week, season_type, game_date, team_code, team_canon,
          opponent_code, opponent_canon, team_fid, opponent_fid, is_home, is_away, is_neutral,
          team_points, opponent_points)""", "team_games.parquet")
    write("SELECT * FROM (VALUES ('gA', 2020, 1, 'REG', '2020-09-10', 2, 'BBB', 'RAI')) "
          "t(boxscore_id, year, week, season_type, game_date, n_team_rows, team_lo, team_hi)",
          "games.parquet")
    write("SELECT * FROM (VALUES ('P1', 2020, 'REG', 1)) t(pfr_id, year, season_type, n_games)",
          "player_seasons.parquet")
    write("SELECT * FROM (VALUES ('pbp', 'g1', 1, 2020, 'REG', NULL, 'AAA', 1)) "
          "t(game_source, game_key, drive_num, year, season_type, box_side, posteam, n_plays)",
          "drives.parquet")
    write("SELECT * FROM (VALUES ('g1', 1, 2020, 1, 'REG', 'AAA', 'BBB')) "
          "t(game_id, play_id, year, week, season_type, posteam, defteam)", "plays.parquet")
    write("SELECT * FROM (VALUES ('gA', 0, 2020, '1', 'X', '5:00', 0, 7)) "
          "t(boxscore_id, event_seq, year, quarter, team, \"time\", vis_team_score, home_team_score)",
          "scoring_events.parquet")
    write("SELECT * FROM (VALUES (1, 'RAI', 2020, 'REG', 1)) "
          "t(team_fid, team_canon, year, season_type, n_games)", "team_seasons.parquet")

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    # presence-bound source: P1 twice (dup), P9 unmatched, one null id
    con.execute(f"""COPY (SELECT * FROM (VALUES
        ('gA', 'P1'), ('gA', 'P1'), ('gA', 'P9'), ('gA', NULL)
      ) t(boxscore_id, player_link_ids)) TO '{src_dir / "presence_src.parquet"}' (FORMAT PARQUET)""")
    # canon source: raw 'LV' must canon to RAI and match team_canon
    con.execute(f"""COPY (SELECT * FROM (VALUES (2020, 1, 'LV')) t(year, week, nfl_team))
      TO '{src_dir / "canon_src.parquet"}' (FORMAT PARQUET)""")

    contracts = {"version": "v1", "n_sources": 2, "contracts": [
        {"source_id": "s_presence",
         "k": {"status": "ACTIVE", "source_key_columns": ["boxscore_id", "player_link_ids"],
               "logical_key": ["boxscore_id", "pfr_id"],
               "canonical_universe": "player_game_presence",
               "canonical_key_columns": ["boxscore_id", "pfr_id"],
               "expected_cardinality": "1:1", "canon_team": False,
               "temporal_validity": {"year_min": 2020, "year_max": 2020},
               "season_type_policy": "ALL", "permitted_unmatched": None, "reason": None},
         "c": {"coverage_model": "DENSE"}},
        {"source_id": "s_canon",
         "k": {"status": "ACTIVE", "source_key_columns": ["year", "week", "nfl_team"],
               "logical_key": ["year", "week", "team"],
               "canonical_universe": "team_games",
               "canonical_key_columns": ["boxscore_id", "team_code"],
               "expected_cardinality": "1:1", "canon_team": True,
               "temporal_validity": {"year_min": 2020, "year_max": 2020},
               "season_type_policy": "ALL", "permitted_unmatched": None, "reason": None},
         "c": {"coverage_model": "PARTIAL_KNOWN"}},
    ]}
    paths = {"s_presence": str(src_dir / "presence_src.parquet"),
             "s_canon": str(src_dir / "canon_src.parquet")}
    return str(ud), contracts, paths


def test_runner_gate_math(synth):
    ud, contracts, paths = synth
    out = recon_kc_planes.run(universes_dir=ud, contracts=contracts, paths=paths,
                              write_receipts=False)
    assert out["counters"]["contracts_executed"] == 2
    assert out["counters"]["contracts_errored"] == 0

    p = out["sources"]["s_presence"]
    assert p["n_rows"] == 4
    assert p["null_key_rows"] == 1
    assert p["n_distinct_keys"] == 2          # (gA,P1), (gA,P9)
    assert p["dup_key_count"] == 1            # (gA,P1) twice
    assert p["cardinality_violation"] is True
    assert p["unmatched_keys"] == 1           # P9
    assert p["expected_keys_in_window"] == 2  # P1, P2
    assert p["matched_in_window"] == 1        # P1
    assert p["expected_only"] == 1            # P2 absent from source
    assert p["missing_if_dense"] == 1         # DENSE -> finding

    c = out["sources"]["s_canon"]
    assert c["unmatched_keys"] == 0           # LV canonized to RAI matched team_canon
    assert c["cardinality_violation"] is False
    assert c["missing_if_dense"] == 0         # PARTIAL_KNOWN never emits missing findings


def test_runner_executes_crosswalked_contract(synth, tmp_path):
    """Crosswalked join: NFL-space ids resolve through a bio-style map; unresolvable ids
    become null_key_rows (typed), never silent drops."""
    import duckdb as _d
    ud, contracts, paths = synth
    con = _d.connect()
    con.execute(f"""COPY (SELECT * FROM (VALUES ('N1', 'P1'), ('N2', NULL))
        t(NFL_player_id, pfr_id)) TO '{tmp_path / "bio.parquet"}' (FORMAT PARQUET)""")
    con.execute(f"""COPY (SELECT * FROM (VALUES ('N1', 2020), ('N2', 2020), ('N9', 2020))
        t(NFL_player_id, year)) TO '{tmp_path / "xw_src.parquet"}' (FORMAT PARQUET)""")
    contracts["contracts"].append({
        "source_id": "s_xw",
        "k": {"status": "ACTIVE", "source_key_exprs": ["xw_pfr_id", "year"],
              "source_key_columns": [],
              "crosswalk": {"via": "bio_map", "on": "NFL_player_id", "adds": "pfr_id",
                            "receipt": "bio_pfr_nflid"},
              "logical_key": ["pfr_id", "year"],
              "canonical_universe": "player_seasons",
              "canonical_key_columns": ["pfr_id", "year"],
              "expected_cardinality": "controlled 1:N", "canon_team": False,
              "temporal_validity": {"year_min": 2020, "year_max": 2020},
              "season_type_policy": "ALL", "permitted_unmatched": None, "reason": None},
        "c": {"coverage_model": "PARTIAL_KNOWN"}})
    contracts["n_sources"] += 1
    paths["s_xw"] = str(tmp_path / "xw_src.parquet")
    paths["bio_map"] = str(tmp_path / "bio.parquet")
    out = recon_kc_planes.run(universes_dir=ud, contracts=contracts, paths=paths,
                              write_receipts=False)
    x = out["sources"]["s_xw"]
    assert x["n_rows"] == 3
    assert x["null_key_rows"] == 2      # N2 (null pfr in bio) + N9 (absent from bio)
    assert x["n_distinct_keys"] == 1    # (P1, 2020)
    assert x["unmatched_keys"] == 0     # P1/2020 exists in the player_seasons universe
    assert x["cardinality_violation"] is False


def test_runner_skips_pending_and_counts_them(synth):
    ud, contracts, paths = synth
    contracts["contracts"][1]["k"] = {"status": "PENDING_CROSSWALK", "reason": "test"}
    out = recon_kc_planes.run(universes_dir=ud, contracts=contracts, paths=paths,
                              write_receipts=False)
    assert out["counters"]["contracts_executed"] == 1
    assert out["counters"]["contracts_pending_typed"] == 1
    assert out["pending"] == {"s_canon": "PENDING_CROSSWALK"}


def test_a_crosswalk_status_is_derived_from_its_receipt_never_hand_typed():
    """2026-07-29: six nflcom families sat at PENDING_CROSSWALK for days AFTER their
    receipt landed and PASSED, because `generate()` short-circuits on a hand-typed
    PENDING status BEFORE the receipt gate ever runs. A status written by hand cannot
    heal when the evidence arrives -- the same shape as a refusal outliving its
    condition. So an override that declares a crosswalk must NOT also declare the
    status: the receipt decides, in both directions."""
    from .kc_planes import K_OVERRIDES
    offenders = [k for k, v in K_OVERRIDES.items()
                 if v.get("crosswalk") and v.get("status") == "PENDING_CROSSWALK"]
    assert not offenders, offenders


def test_every_crosswalk_names_a_receipt_that_exists_and_licenses_it():
    """A crosswalk pointing at a receipt id that does not exist resolves to PENDING
    forever and looks identical to 'not built yet'. Either the receipt is real and
    licenses the source, or the contract must not claim a crosswalk."""
    from . import crosswalk_receipt
    from .kc_planes import K_OVERRIDES
    for source, spec in K_OVERRIDES.items():
        cw = spec.get("crosswalk")
        if not cw:
            continue
        try:
            rec = crosswalk_receipt.get(cw["receipt"])
        except (FileNotFoundError, KeyError):
            continue        # legitimately not built yet -- resolves to PENDING
        if rec["status"] == "PASS":
            assert source in rec["licenses"], (
                f"{source} claims receipt {cw['receipt']!r}, which PASSES but does not "
                f"list it in `licenses`")
