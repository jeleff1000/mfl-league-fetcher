from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from top10_stability_source import (
    build_source_manifest,
    cohort_member_from_slug,
    cohort_slug_from_member,
    eligible_leagues,
    load_approved_pools,
    pool_for_base_cohort,
)


def test_cohort_member_and_slug_round_trip() -> None:
    member = cohort_member_from_slug("12t_flx_ppr_4pt")

    assert member == "12t/flx/ppr/4pt"
    assert cohort_slug_from_member(member) == "12t_flx_ppr_4pt"


@pytest.mark.parametrize("value", ("12t/flx/ppr/4pt", "12t_flx_ppr", "../unsafe"))
def test_invalid_cohort_slug_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="cohort"):
        cohort_member_from_slug(value)


def _reader_with_settings() -> SimpleNamespace:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute(
        """
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, num_teams INTEGER, playoff_teams INTEGER,
          is_dynasty BOOLEAN,
          sleeper_best_ball BOOLEAN, roster_IDP INTEGER, roster_DL INTEGER,
          roster_LB INTEGER, roster_DB INTEGER, roster_DB_LB INTEGER,
          roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER, scoring_rec DOUBLE,
          scoring_pass_td DOUBLE
        )
        """
    )
    con.executemany(
        "INSERT INTO public.league_settings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("a", 2025, 10, 6, False, False, 0, 0, 0, 0, 0, 0, 0, 1.0, 4.0),
            ("b", 2025, 12, 6, False, False, 0, 0, 0, 0, 0, 0, 0, 0.5, 6.0),
            ("old", 2024, 10, 6, False, False, 0, 0, 0, 0, 0, 0, 0, 1.0, 4.0),
        ],
    )
    con.execute("CREATE TABLE public.draft (db_name VARCHAR, year INTEGER, is_keeper BOOLEAN)")
    return SimpleNamespace(con=con)


def _write_cluster_map(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["metric", "pos", "members", "pooled_n_2025", "display_ok", "sort_ok"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "metric": "start_rate_pct",
                "pos": "QB",
                "members": "10t/flx/ppr/4pt|12t/flx/half/6pt",
                "pooled_n_2025": "120",
                "display_ok": "True",
                "sort_ok": "True",
            }
        )


def test_pool_loader_maps_each_member_back_to_its_approved_cluster(tmp_path) -> None:
    path = tmp_path / "cluster.csv"
    _write_cluster_map(path)

    pools = load_approved_pools(path, valid_metrics={"start_rate_pct"})
    selected = pool_for_base_cohort(pools, "start_rate_pct", "QB", "10t/flx/ppr/4pt")

    assert selected.members == ("10t/flx/ppr/4pt", "12t/flx/half/6pt")
    assert selected.sort_ok is True


def test_pool_loader_rejects_unknown_metric(tmp_path) -> None:
    path = tmp_path / "cluster.csv"
    _write_cluster_map(path)

    with pytest.raises(ValueError, match="unknown metric"):
        load_approved_pools(path, valid_metrics={"won_pct"})


def test_eligible_leagues_are_unique_and_do_not_cross_years() -> None:
    reader = _reader_with_settings()

    rows = eligible_leagues(
        reader,
        year=2025,
        members=("10t/flx/ppr/4pt", "12t/flx/half/6pt"),
    )

    assert [(row.db_name, row.year) for row in rows] == [("a", 2025), ("b", 2025)]


def test_source_manifest_hashes_file_contents(tmp_path) -> None:
    source = tmp_path / "source.duckdb"
    contract = tmp_path / "contract.json"
    source.write_bytes(b"first-source")
    contract.write_text("{}", encoding="utf-8")

    before = build_source_manifest({"source": source, "contract": contract}, git_commit="abc")
    source.write_bytes(b"other-source")
    after = build_source_manifest({"source": source, "contract": contract}, git_commit="abc")

    assert before.file_sha256["source"] != after.file_sha256["source"]
    assert before.git_commit == "abc"
