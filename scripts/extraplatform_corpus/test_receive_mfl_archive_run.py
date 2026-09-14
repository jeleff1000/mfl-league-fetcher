from __future__ import annotations

import json

from pathlib import Path

from receive_mfl_archive_run import payload_audit_command, wait_for_successful_run


def test_wait_for_successful_run_only_returns_after_actions_reports_success() -> None:
    responses = iter(
        [
            {"status": "in_progress", "conclusion": None},
            {"status": "completed", "conclusion": "success"},
        ]
    )
    commands: list[list[str]] = []
    sleeps: list[int] = []

    def run_command(command: list[str]) -> str:
        commands.append(command)
        return json.dumps(next(responses))

    result = wait_for_successful_run(
        repo="jeleff1000/mfl-league-fetcher",
        run_id=456,
        poll_seconds=30,
        run_command=run_command,
        sleep=sleeps.append,
    )

    assert result == {"status": "completed", "conclusion": "success"}
    assert sleeps == [30]
    assert commands == [
        [
            "gh",
            "run",
            "view",
            "456",
            "--repo",
            "jeleff1000/mfl-league-fetcher",
            "--json",
            "status,conclusion",
        ],
        [
            "gh",
            "run",
            "view",
            "456",
            "--repo",
            "jeleff1000/mfl-league-fetcher",
            "--json",
            "status,conclusion",
        ],
    ]


def test_wait_for_successful_run_fails_closed_when_actions_fails() -> None:
    try:
        wait_for_successful_run(
            repo="jeleff1000/mfl-league-fetcher",
            run_id=456,
            poll_seconds=30,
            run_command=lambda _command: '{"status":"completed","conclusion":"failure"}',
            sleep=lambda _seconds: None,
        )
    except RuntimeError as exc:
        assert "failure" in str(exc)
    else:
        raise AssertionError("a failed Actions run was allowed to reach the D: receiver")


def test_payload_audit_command_keeps_all_temporary_and_report_paths_on_d_drive() -> None:
    command = payload_audit_command(
        python="python",
        receiver_directory=Path(r"D:\worker\scripts\extraplatform_corpus"),
        destination=Path(r"D:\yahoo_oauth\derived\mfl_register\pc_artifacts"),
        run_id=456,
    )

    assert command == [
        "python",
        r"D:\worker\scripts\extraplatform_corpus\audit_mfl_original_payloads.py",
        "--destination",
        r"D:\yahoo_oauth\derived\mfl_register\pc_artifacts",
        "--run-id",
        "456",
        "--scratch-directory",
        r"D:\yahoo_oauth\derived\mfl_register\audit\mfl_original_payload_scratch_456",
        "--output-directory",
        r"D:\yahoo_oauth\derived\mfl_register\audit\mfl_original_payload_audit_456",
    ]
