#!/usr/bin/env python3
"""Keep exactly three unique MFL campaigns in flight.

The planner owns league-year reservations. The watchdog only refills missing
active slots, so every replacement is selected against the canonical index and
all prior campaign plans by the planner workflow.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone


def gh_json(args: list[str]) -> dict | list:
    last_error = ""
    for attempt in range(4):
        result = subprocess.run(["gh", "api", *args], capture_output=True, text=True)
        if result.returncode == 0:
            return json.loads(result.stdout)
        last_error = (result.stderr or result.stdout).strip()
        if attempt < 3:
            time.sleep(2**attempt)
    raise RuntimeError(f"gh api failed after 4 attempts: {' '.join(args)}: {last_error}")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def workflow_runs(repo: str, workflow: str) -> list[dict]:
    pages = gh_json([
        f"repos/{repo}/actions/workflows/{workflow}/runs?per_page=100",
        "--paginate", "--slurp",
    ])
    runs: list[dict] = []
    for page in pages:
        runs.extend(page.get("workflow_runs", []))
    return runs


def dispatch(repo: str, workflow: str, values: dict[str, str]) -> None:
    command = ["gh", "workflow", "run", workflow, "--repo", repo, "--ref", "main"]
    for key, value in values.items():
        command.extend(["-f", f"{key}={value}"])
    subprocess.run(command, check=True)


def has_batch_plan_artifact(repo: str, run_id: int) -> bool:
    payload = gh_json([
        f"repos/{repo}/actions/runs/{run_id}/artifacts?"
        f"name=mfl-batch-plan-{run_id}&per_page=10",
    ])
    return bool(payload.get("artifacts")) if isinstance(payload, dict) else False


def cancel(repo: str, run_id: int) -> None:
    subprocess.run(
        ["gh", "run", "cancel", str(run_id), "--repo", repo],
        check=False,
        capture_output=True,
        text=True,
    )


def main() -> int:
    repo = os.environ.get("MFL_REPO", "jeleff1000/mfl-league-fetcher")
    campaign_workflow = os.environ.get("MFL_CAMPAIGN_WORKFLOW", "mfl_register_batch_campaign.yml")
    planner_workflow = os.environ.get("MFL_PLANNER_WORKFLOW", "mfl_plan_next_wave.yml")
    epoch = parse_time(os.environ.get("MFL_SCHEDULER_EPOCH", "2026-08-19T03:10:00Z"))
    now = datetime.now(timezone.utc)
    dry_run = os.environ.get("MFL_DRY_RUN", "false").lower() == "true"
    cooldown = timedelta(minutes=int(os.environ.get("MFL_COOLDOWN_MINUTES", "40")))
    target_active = int(os.environ.get("MFL_CAMPAIGNS_PER_WAVE", "3"))
    campaigns = [r for r in workflow_runs(repo, campaign_workflow)
                 if r.get("created_at") and parse_time(r["created_at"]) >= epoch]
    planners = [r for r in workflow_runs(repo, planner_workflow)
                if r.get("created_at") and parse_time(r["created_at"]) >= epoch]
    unusable = {"failure", "cancelled", "timed_out", "startup_failure", "action_required"}
    active_states = {"queued", "in_progress", "waiting", "requested", "pending"}
    active_campaigns = [r for r in campaigns if r.get("status") in active_states
                        and r.get("conclusion") not in unusable]
    active_planners = [r for r in planners if r.get("status") in active_states]
    queue_grace = timedelta(minutes=int(os.environ.get("MFL_QUEUE_GRACE_MINUTES", "30")))
    stale_campaigns = [
        r for r in active_campaigns
        if r.get("status") == "queued"
        and now - parse_time(r["created_at"]) >= queue_grace
    ]
    if not dry_run:
        for run in stale_campaigns:
            cancel(repo, int(run["id"]))
    stale_ids = {int(r["id"]) for r in stale_campaigns}
    active_campaigns = [r for r in active_campaigns if int(r["id"]) not in stale_ids]
    # A queued campaign cannot publish its durable batch-plan artifact until
    # GitHub assigns it a runner and the prepare job starts.  Treating queued
    # runs as "unplanned" creates a deadlock: the watchdog counts the slot,
    # refuses to launch the replacement planner, and the queued run can never
    # start.  Validate plan artifacts once a campaign has actually started;
    # queued runs still reserve an active slot and are handled by queue_grace.
    plan_grace = timedelta(minutes=int(os.environ.get("MFL_PLAN_GRACE_MINUTES", "10")))
    unplanned_campaign_ids = sorted(
        int(run["id"])
        for run in active_campaigns
        if run.get("status") in {"in_progress", "waiting", "requested"}
        and now - parse_time(run["created_at"]) >= plan_grace
        and not has_batch_plan_artifact(repo, int(run["id"]))
    )
    latest_campaign = max((parse_time(r["created_at"]) for r in campaigns), default=None)
    quiet = latest_campaign is None or now - latest_campaign >= cooldown
    needed = max(0, target_active - len(active_campaigns))
    decision = {
        "mode": "planner_backed_unique_inflight_controller",
        "repo": repo,
        "checked_at": now.isoformat().replace("+00:00", "Z"),
        "scheduler_epoch": epoch.isoformat().replace("+00:00", "Z"),
        "campaigns_seen": len(campaigns),
        "planners_seen": len(planners),
        "active_campaigns": len(active_campaigns),
        "active_campaign_ids": [int(r["id"]) for r in active_campaigns],
        "unplanned_campaign_ids": unplanned_campaign_ids,
        "stale_campaign_ids": sorted(stale_ids),
        "cancelled_stale_campaigns": [] if dry_run else sorted(stale_ids),
        "active_planners": len(active_planners),
        "target_active_campaigns": target_active,
        "quiet_period_elapsed": quiet,
        "needed_campaigns": needed,
        "dispatched": [],
    }
    if active_planners:
        decision["reason"] = "unique_reservation_planner_already_running"
    elif unplanned_campaign_ids:
        decision["reason"] = "waiting_for_active_campaign_plans"
    elif needed <= 0:
        decision["reason"] = "target_active_campaign_count_reached"
    else:
        values = {
            "campaign_count": str(needed),
            "batches_per_campaign": os.environ.get("MFL_BATCHES_PER_CAMPAIGN", "250"),
            "batch_size": os.environ.get("MFL_ORDERED_BATCH_SIZE", "2"),
            "collect_all": "true",
            # Leave runner headroom for each campaign's prepare/combine jobs.
            # Three 50-wide matrices can make the third campaign sit queued
            # even though the controller sees three campaigns in flight.
            "max_parallel": os.environ.get("MFL_MAX_PARALLEL", "250"),
            "target_years": os.environ.get(
                "MFL_TARGET_YEARS",
                "2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,"
                "2010,2011,2012,2013,2014,2015,2016,2017,2018",
            ),
            "campaign_cutoff": epoch.isoformat().replace("+00:00", "Z"),
            "mfl_rate_limit": os.environ.get("MFL_RATE_LIMIT", "6"),
        }
        if not dry_run:
            dispatch(repo, planner_workflow, values)
        decision["dispatched"].append({
            "planner_workflow": planner_workflow,
            "campaign_count": needed,
            "batches_per_campaign": int(values["batches_per_campaign"]),
            "batch_size": int(values["batch_size"]),
            "max_parallel": int(values["max_parallel"]),
        })
        decision["reason"] = "unique_planner_dispatched"
    print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
