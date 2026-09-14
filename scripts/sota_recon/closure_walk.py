"""CLOSURE WALK: judge every locked column that has no closure dossier yet.

Runs close_column over the never-judged list (locked columns minus existing
dossiers), one at a time, resumable by construction (each dossier persists
as it lands). Prints a running scoreboard and a final tally.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
LOCKS = Path(__file__).parent / "witness_gate" / "contracts" / "witness_locks.v1.json"
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 40


def main() -> None:
    locked = {l["column"] for l in json.loads(
        LOCKS.read_text("utf-8"))["locks"]}
    judged = {p.stem.replace("closure_", "")
              for p in LAKE.glob("closure_*.json")}
    todo = sorted(locked - judged)[:LIMIT]
    print(f"{len(todo)} columns to judge this walk", flush=True)
    tally = {"CLOSED": 0, "BLOCKED": 0, "DERIVED_PENDING_S7": 0, "ERROR": 0}
    closed = []
    for i, col in enumerate(todo):
        r = subprocess.run(
            [sys.executable, "-m", "scripts.sota_recon.close_column", col],
            cwd=str(Path(__file__).resolve().parents[2]),
            capture_output=True, text=True)
        verdict = "ERROR"
        if r.returncode == 0:
            try:
                verdict = json.loads(r.stdout)["verdict"]
            except Exception:
                pass
        tally[verdict] = tally.get(verdict, 0) + 1
        if verdict == "CLOSED":
            closed.append(col)
        print(f"[{time.strftime('%H:%M:%S')}] {i+1}/{len(todo)} "
              f"{col}: {verdict}", flush=True)
    print(json.dumps({"tally": tally, "closed": closed}, indent=1),
          flush=True)


if __name__ == "__main__":
    main()
