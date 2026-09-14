"""O.9.2 -- the nflcom slug->pfr_id receipt must keep REFUSING what it refuses.

The value of this crosswalk is not its coverage number; it is the set of things it will
not do. A later change that raises coverage by joining on bare names, or by picking the
more frequent candidate for a twin, would look like an improvement in every printed
figure and would silently fuse two people's careers.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import duckdb
import pytest

from .nflcom_slug_crosswalk import measure
from .sources import registry

RECEIPT_PATH = Path(__file__).resolve().parent / "witness_gate" / "contracts" / "crosswalk_receipts.v1.json"
SUMMARY_PATH = Path(__file__).resolve().parents[2] / "docs" / "nflcom-slug-crosswalk.json"
RECEIPT_ID = "nflcom_slug_pfrid"


def _receipt() -> dict:
    receipts = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))["receipts"]
    found = [r for r in receipts if r["receipt_id"] == RECEIPT_ID]
    assert found, "the nflcom slug crosswalk receipt is not in the contract"
    return found[0]


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE fixture (" + ", ".join(f'\"{column}\" VARCHAR' for column in columns) + ")"
        )
        connection.executemany(
            "INSERT INTO fixture VALUES (" + ", ".join("?" for _ in columns) + ")",
            rows,
        )
        connection.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


def test_materialized_relation_is_exact_bijection_and_excludes_conflicts(tmp_path: Path) -> None:
    """Catches persisting candidate pairs instead of the already-filtered resolved set."""
    roster = tmp_path / "roster.parquet"
    _write_parquet(
        roster,
        ("source_player_id", "player", "season", "team"),
        [
            ("good-slug", "Good Player", "2020", "good-team"),
            ("conflicted-slug", "First Twin", "2020", "first-team"),
            ("conflicted-slug", "Second Twin", "2021", "second-team"),
        ],
    )
    pfr_root = tmp_path / "pfr_tables"
    _write_parquet(
        pfr_root / "passing" / "_combined.parquet",
        ("pfr_id", "player", "year_id", "team_name_abbr"),
        [
            ("GoodPl00", "Good Player", "2020", "GOD"),
            ("FirstT00", "First Twin", "2020", "FST"),
            ("SeconT00", "Second Twin", "2021", "SND"),
        ],
    )
    output = tmp_path / "derived" / "nflcom_slug_pfrid.parquet"

    measured = measure(
        resolved_output_path=output,
        roster_paths=(roster,),
        pfr_tables_root=pfr_root,
        statscrew_reparse=None,
    )

    with duckdb.connect() as connection:
        columns = [row[0] for row in connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(output)]
        ).fetchall()]
        rows = connection.execute(
            "SELECT nflcom_slug, pfr_id FROM read_parquet(?) ORDER BY 1, 2", [str(output)]
        ).fetchall()
    assert columns == ["nflcom_slug", "pfr_id"]
    assert rows == [("good-slug", "GoodPl00")]
    assert measured["counters"]["candidate_pairs"] == 3
    assert measured["counters"]["slug_conflicts_multiple_pfr_ids"] == 1
    assert measured["counters"]["resolved_slugs"] == len(rows) == 1


def test_materialized_crosswalk_is_registered_as_non_voting_identity() -> None:
    """Catches the derived identity bridge accidentally gaining external vote depth."""
    source = registry(include_subject=False)["nflcom_slug_pfrid"]
    assert source.role == "identity"
    assert source.witness_class == "identity"
    assert source.lineage == "internal"
    assert Path(source.path).name == "nflcom_slug_pfrid.parquet"


def test_help_exposes_local_materialization_without_acquisition_options() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.sota_recon.nflcom_slug_crosswalk", "--help"],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "--materialize-resolved" in completed.stdout
    for option in ("--url", "--browser", "--request", "--fetch", "--scrape", "--capture", "--recapture"):
        assert option not in completed.stdout


def test_the_join_key_is_never_a_bare_name() -> None:
    """§19.2. Bio holds five Steve Smiths, two of them born in 1979 -- a bare name join
    fuses careers and every downstream number keeps looking plausible."""
    receipt = _receipt()
    assert "season" in receipt["crosswalk"]["on"]
    assert any("bare name" in refusal for refusal in receipt["refusals"])


def test_the_licensed_mapping_is_one_to_one() -> None:
    receipt = _receipt()
    assert receipt["gates"]["licensed_mapping_is_bijective"] is True
    assert receipt["measured"]["resolved_slugs"] > 0


def test_twin_slugs_are_excluded_and_named_not_silently_picked() -> None:
    """Both conflicts are real people: `todd-collins` is a 1990s linebacker AND a
    quarterback. A crosswalk that resolved them by taking the candidate with more
    agreeing seasons would have reported 100% bijection and been wrong about a career."""
    receipt = _receipt()
    excluded = {row["slug"] for row in receipt["conflicting_slugs_excluded"]}
    assert "todd-collins" in excluded
    for row in receipt["conflicting_slugs_excluded"]:
        assert len(row["pfr_ids"]) > 1, "a conflict must name every candidate it refused"


def test_coverage_is_reported_not_rounded_up() -> None:
    """An unresolved slug stays unresolved. Coverage is recorded per era precisely so
    that the thin end -- pre-1933, where the PFR stat universe barely exists -- cannot be
    hidden inside a single headline percentage."""
    receipt = _receipt()
    measured = receipt["measured"]
    assert 0 < measured["slug_coverage"] < 1
    eras = {row["era"]: row for row in receipt["coverage_by_era"]}
    assert "1920-1932" in eras
    assert eras["1920-1932"]["coverage"] < eras["1950-1977"]["coverage"], (
        "the ancient era is the weakest and the receipt must keep saying so")


def test_the_hold_on_splits_moved_from_the_SOURCE_to_its_COLUMNS() -> None:
    """INVERTED 2026-07-29, and the inversion is the point.

    This test used to assert that player_splits / player_situational are absent from
    `licenses`, on the ground that they "still hold every value one column left of its name
    at rest". That was true when it was written and false by 2026-07-28: the un-shift
    landed, sources.py was repointed, 521 columns were adjudicated. Joe promoted both
    families on 2026-07-29.

    What must NOT be lost is the hold that is still real, and it is a different shape --
    COLUMN-level, not source-level, and structural rather than declared. An unadjudicated
    column has no MAPPED_TO_CANONICAL row, so nothing can resolve a witness through it
    however the source is licensed. This test now pins that: the families are licensed, the
    receipt still says out loud which columns are held, and the escalated ones are still
    OPEN in the ledger. If someone quietly adjudicates the L4 return block to make a number
    move, the last assertion fails."""
    receipt = _receipt()
    assert "nflcom_player_splits" in receipt["licenses"]
    assert "nflcom_player_situational" in receipt["licenses"]

    text = " ".join(receipt["does_not_license"])
    for held in ("L4", "L7", "tackle total", "not adjudicated"):
        assert held in text, (
            f"the receipt no longer names {held!r} among what it does NOT license -- the "
            "column-level hold has to stay stated, or promoting the source reads as "
            "promoting every column in it")

    from .column_dossier import load_decisions

    decided = {key for key, entry in load_decisions().items()
               if key.split("|", 1)[0] in {"nflcom_player_splits",
                                           "nflcom_player_situational"}}
    from . import nflcom_splits_column_adjudication as SPLITS

    escalated = set(SPLITS.escalated_row_keys())
    leaked = sorted(escalated & decided)
    assert not leaked, (
        f"{len(leaked)} escalated splits columns acquired a disposition while their "
        f"family was being promoted: {leaked[:5]}")


@pytest.mark.skipif(not SUMMARY_PATH.exists(), reason="summary not built in this tree")
def test_summary_and_receipt_agree() -> None:
    summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    assert summary["receipt"]["measured"] == _receipt()["measured"]
