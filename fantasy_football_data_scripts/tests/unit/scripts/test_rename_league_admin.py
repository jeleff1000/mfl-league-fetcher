from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def _encoded(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


def test_rename_worker_calls_the_specialized_fly_operation() -> None:
    from rename_league_admin import decode_payload, run_rename

    calls = []

    class Target:
        def rename_league(self, **kwargs):
            calls.append(kwargs)
            return {"status": "COMMITTED", "target_years": [2009, 2026]}

    payload = decode_payload(
        _encoded(
            {
                "source_db": "agustafantasyleague",
                "target_db": "agusta_fantasy_league",
                "display_name": "Agusta Fantasy League",
                "operation_id": "rename-agusta",
            }
        )
    )
    result = run_rename(payload, target=Target())

    assert result["status"] == "COMMITTED"
    assert calls == [
        {
            "source_db": "agustafantasyleague",
            "target_db": "agusta_fantasy_league",
            "display_name": "Agusta Fantasy League",
            "operation_id": "rename-agusta",
        }
    ]


def test_rename_worker_rejects_invalid_or_extra_payload_fields() -> None:
    from rename_league_admin import decode_payload

    with pytest.raises(SystemExit, match="exactly"):
        decode_payload(
            _encoded(
                {
                    "source_db": "old_league",
                    "target_db": "new_league",
                    "display_name": "New League",
                    "operation_id": "rename-new",
                    "encrypted_refresh_token": "must-not-be-accepted",
                }
            )
        )
