"""
sota_recon/nflcom_merge_runner_shards.py -- fold GitHub-runner harvest shards into the local NC corpus.

The distributed crawl (ff-assets repo, workflow nflcom-harvest) uploads per-shard artifacts containing
player_{view}_shardNNN.parquet + done_shardNNN.json. This downloads a run's artifacts, drops the
parquets into the local witness dirs (tables/player_{view}/runner_shardNNN.parquet -- readers glob the
dir with union_by_name), and merges the done-lists into harvest_state.json so the LOCAL crawler skips
those players.

⚠ STOP the local nflcom_harvest process before --apply (it holds state in memory and would clobber the
merged done-lists on its next flush); restart it after.

    python -m scripts.sota_recon.nflcom_merge_runner_shards --run <run-id>            # download + report
    python -m scripts.sota_recon.nflcom_merge_runner_shards --run <run-id> --apply    # install + state merge
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

BASE = Path("D:/league-history-data/nfl/raw/nflcom")
TABLES = BASE / "tables"
REPO = "jeleff1000/ff-assets"
VIEWS = ("logs", "career", "splits", "situational")


def run(run_id: str, apply: bool) -> dict:
    dl = Path(tempfile.mkdtemp(prefix="nflcom_runner_"))
    subprocess.run(["gh", "run", "download", run_id, "-R", REPO, "-D", str(dl)], check=True)
    parquets = sorted(dl.glob("**/player_*_shard*.parquet"))
    dones = sorted(dl.glob("**/done_shard*.json"))
    res = {"artifacts_dir": str(dl), "parquet_files": len(parquets), "done_files": len(dones)}
    merged: dict[str, set] = {v: set() for v in VIEWS}
    for d in dones:
        j = json.loads(d.read_text())
        for v in VIEWS:
            merged[v].update(j.get(v, []))
    res["players_done"] = {v: len(s) for v, s in merged.items()}
    if not apply:
        return res

    installed = 0
    for p in parquets:
        view = p.name.split("_shard")[0]  # player_logs etc.
        dest = TABLES / view / f"runner_{p.stem.split('_')[-1]}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dest)
        installed += 1
    state_p = BASE / "harvest_state.json"
    st = json.loads(state_p.read_text())
    dv = st.setdefault("done_player_views", {})
    for v in VIEWS:
        dv[v] = sorted(set(dv.get(v, [])) | merged[v])
    backup = state_p.with_suffix(".json.bak_runner_merge")
    shutil.copy2(state_p, backup)
    state_p.write_text(json.dumps(st))
    res["installed_parquets"] = installed
    res["state_done_now"] = {v: len(dv[v]) for v in VIEWS}
    res["state_backup"] = str(backup)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(a.run, a.apply), indent=1))
