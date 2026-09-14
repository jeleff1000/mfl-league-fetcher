"""Rollback the 3 wrong Lane B writes from fix_unjoined_player_ids_2026_04_28.py
and re-apply the correct ones.

Bug: my Lane B logic ran Phase B (set platform_id on the "correct" NFL_id) for
each disagreement, but for the "legacy clear" cases the Phase B was wrong —
it set the SLEEPER id as a YAHOO id on the modern bio row.

Wrong writes to undo:
  1. 00-0036873 (Jake Verity)         sleeper_player_id=6888 -> NULL
  2. 00-0036221 (Brandon Jones modern) yahoo_player_id=6911 -> 32740 (restore)
  3. 00-0039908 (Kris Jenkins modern)  yahoo_player_id=11688 -> NULL

Correct write to do:
  4. 00-0036411 (Antoine Winfield Jr.) sleeper_player_id=NULL -> 6888
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def main() -> None:
    load_env()
    w = FlyWriter()

    rollback = [
        (
            "00-0036873 (Jake Verity) NULL sleeper",
            "UPDATE ___ops.nfl_historical.player_bio SET sleeper_player_id = NULL WHERE NFL_player_id = '00-0036873'",
        ),
        (
            "00-0036221 (Brandon Jones modern) restore yahoo=32740",
            "UPDATE ___ops.nfl_historical.player_bio SET yahoo_player_id = 32740 WHERE NFL_player_id = '00-0036221'",
        ),
        (
            "00-0039908 (Kris Jenkins modern) NULL yahoo",
            "UPDATE ___ops.nfl_historical.player_bio SET yahoo_player_id = NULL WHERE NFL_player_id = '00-0039908'",
        ),
        (
            "00-0036411 (Antoine Winfield Jr.) set sleeper=6888",
            "UPDATE ___ops.nfl_historical.player_bio SET sleeper_player_id = 6888 WHERE NFL_player_id = '00-0036411'",
        ),
    ]
    for label, sql in rollback:
        print(f"\n[{label}]")
        print(f"  {sql}")
        res = w.execute(sql, database="___ops")
        print(f"  -> {res}")

    print("\n[verify]")
    rows = w.execute(
        """
        SELECT NFL_player_id, player,
               CAST(yahoo_player_id AS BIGINT) AS yahoo_id,
               CAST(sleeper_player_id AS BIGINT) AS sleeper_id
        FROM ___ops.nfl_historical.player_bio
        WHERE NFL_player_id IN ('00-0036873','00-0036221','00-0039908','00-0036411')
        ORDER BY NFL_player_id
        """,
        database="___ops",
    )
    for r in rows:
        print(f"  {r}")


if __name__ == "__main__":
    main()
