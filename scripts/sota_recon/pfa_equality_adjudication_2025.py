"""Adjudicate captured PFA-v26 equality exceptions with the PBP rollup."""
from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb

from scripts.sota_recon.sources import PBP_ROLLUP

RECEIPT = Path(r"D:\yahoo_oauth\docs\audits\pfa-lake-audit-2025.json")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfa-equality-adjudication-2025.json")
TEAM = {
    "Los Angeles Rams": "LAR", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Jacksonville Jaguars": "JAX", "San Francisco 49ers": "SFO", "New England Patriots": "NWE",
    "Houston Texans": "HOU", "Denver Broncos": "DEN", "Seattle Seahawks": "SEA",
    "Buffalo Bills": "BUF", "Philadelphia Eagles": "PHI", "Los Angeles Chargers": "LAC",
    "Pittsburgh Steelers": "PIT", "Green Bay Packers": "GNB",
}
PBP_ALIAS = {"def_tackles_combined": "def_tackles_with_assist"}


def norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def num(value: object):
    text = "" if value is None else str(value)
    m = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*[tT]?\s*", text)
    return float(m.group(1)) if m else text


def main() -> None:
    receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    con = duckdb.connect()
    adjudications = []
    for check in receipt["equality_2025"]["column_checks"]:
        if check["status"] != "FAIL":
            continue
        canonical = PBP_ALIAS.get(check["canonical"], check["canonical"])
        cols = {x[0] for x in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [PBP_ROLLUP.path]).fetchall()}
        for example in check["examples"]:
            if canonical not in cols:
                status, pbp_value = "PBP_COLUMN_UNAVAILABLE", None
            else:
                sql = f"""SELECT \"{canonical}\" FROM read_parquet(?)
                           WHERE year=2025 AND week=? AND nfl_team=?
                             AND lower(regexp_replace(player,'[^a-zA-Z0-9]','','g'))
                                 = lower(regexp_replace(?,'[^a-zA-Z0-9]','','g'))"""
                rows = con.execute(sql, [PBP_ROLLUP.path, example["week"], TEAM.get(example["team"]), example["player"]]).fetchall()
                if not rows:
                    status, pbp_value = "PBP_ROW_UNAVAILABLE", None
                else:
                    pbp_value = rows[0][0]
                    pfa_value, v26_value = num(example["pfa"]), num(example["v26"])
                    if num(pbp_value) == v26_value:
                        status = "PBP_AGREES_V26"
                    elif num(pbp_value) == pfa_value:
                        status = "PBP_AGREES_PFA"
                    else:
                        status = "PBP_DIFFERS_FROM_BOTH"
            adjudications.append({"subtable": check["subtable"], "column": check["column"],
                                  "canonical": check["canonical"], "game_id": example["game_id"],
                                  "player": example["player"], "team": example["team"],
                                  "pfa": example["pfa"], "v26": example["v26"],
                                  "pbp": pbp_value, "status": status})
    con.close()
    OUT.write_text(json.dumps({
        "generated_at_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "source": "profootballarchives", "year": 2025,
        "basis": "PFA-v26 mismatched examples cross-checked against the independent PBP player-week rollup",
        "adjudications": adjudications,
        "summary": {"examples_checked": len(adjudications),
                    "pfa_agreements": sum(x["status"] == "PBP_AGREES_PFA" for x in adjudications),
                    "v26_agreements": sum(x["status"] == "PBP_AGREES_V26" for x in adjudications),
                    "pbp_unavailable": sum(x["status"] in {"PBP_COLUMN_UNAVAILABLE", "PBP_ROW_UNAVAILABLE"} for x in adjudications),
                    "differs_from_both": sum(x["status"] == "PBP_DIFFERS_FROM_BOTH" for x in adjudications)},
    }, indent=2), encoding="utf-8")
    print(json.dumps(json.loads(OUT.read_text(encoding="utf-8"))["summary"], indent=2))


if __name__ == "__main__":
    main()
