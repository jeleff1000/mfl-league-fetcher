"""Emit the permanent position-law mutation/arming receipt."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.sota_recon.witness_gate import position_law as law


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs/audits/sota-recon/position/position-law-mutation-report.json"
TAXONOMY = ROOT / "scripts/sota_recon/witness_gate/position_taxonomy.py"
CONTRACT = ROOT / "scripts/sota_recon/witness_gate/contracts/position_taxonomy.v1.json"
DECLARATION = Path(r"D:\league-history-data\nfl\derived\validation\sota_recon_master\pfr_season_position_declaration.parquet")


def main() -> None:
    con = duckdb.connect()
    law.self_test(con)
    payload = {
        "schema_version": "position-law-mutation-report.v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "self_test": "PASS",
        "articles": {
            "VOCABULARY": "independent invalid-token poison refused",
            "ORDER": "independent out-of-order and duplicate-token poisons refused",
            "CEILING": "independent undeclared-role poison refused",
            "CONSISTENCY": "independent position/detail mismatch poison refused",
        },
        "consistency_policy": "exact_zero_violations",
        "fingerprint_inputs": {
            "law": str(Path(law.__file__)),
            "taxonomy": str(TAXONOMY),
            "contract": str(CONTRACT),
            "declaration": str(DECLARATION),
        },
        "fingerprint": law.input_fingerprint([law.__file__, TAXONOMY, CONTRACT, DECLARATION]),
        "trust_boundary": "All supported weekly writers arm self_test and assert_frame before marking year parts complete; direct filesystem replacement remains detectable by exact re-derivation and output-manifest parity, not preventable by Python alone.",
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUTPUT), "self_test": payload["self_test"]}, indent=2))


if __name__ == "__main__":
    main()
