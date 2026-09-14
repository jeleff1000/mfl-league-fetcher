from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


SCRIPT_PATH = Path(__file__).resolve().parents[5] / "scripts" / "build_newspaper_conveyor_status_report.py"
SPEC = importlib.util.spec_from_file_location("newspaper_conveyor_status_report", SCRIPT_PATH)
assert SPEC and SPEC.loader
status_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(status_report)


def test_page_visual_progress_excludes_decisions_outside_the_packet(tmp_path: Path):
    db_path = tmp_path / "newspaper_atoms.duckdb"
    run_id = "packet-v2"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA newspaper_review")
        con.execute(
            """
            CREATE TABLE newspaper_review.page_visual_review_packet_item (
              page_visual_review_packet_run_id VARCHAR,
              visual_review_item_id VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE newspaper_review.page_visual_review_decision (
              page_visual_review_packet_run_id VARCHAR,
              visual_review_item_id VARCHAR,
              decision VARCHAR,
              generated_decision_count VARCHAR
            )
            """
        )
        con.executemany(
            "INSERT INTO newspaper_review.page_visual_review_packet_item VALUES (?, ?)",
            [(run_id, "packet-item-1"), (run_id, "packet-item-2")],
        )
        con.executemany(
            "INSERT INTO newspaper_review.page_visual_review_decision VALUES (?, ?, ?, ?)",
            [
                (run_id, "packet-item-1", "extract_atoms", "1"),
                (run_id, "packet-item-2", "no_useful_stat", "0"),
                (run_id, "stale-residual-item", "extract_atoms", "99"),
            ],
        )
    finally:
        con.close()

    progress = status_report.load_page_visual_progress(db_path, run_id, packet_item_count=2)

    assert progress["reviewed_item_count"] == 2
    assert progress["remaining_item_count"] == 0
    assert progress["decision_row_count"] == 2
    assert progress["extract_decision_count"] == 1
    assert progress["generated_decision_count"] == 1
