"""Measure the player-splits Stadiums table using the shared Outcomes witness machinery."""

import json

from .nflcom_player_splits_outcomes_witness import OUT, measure


def measure_stadiums():
    result = measure(
        source_table="Stadiums",
        split_filter="split_value IS NOT NULL AND split_value <> ''",
        partition="all nonblank stadium labels; no partition claim",
    )
    result["source_table"] = "Stadiums"
    return result


def main():
    result = measure_stadiums()
    output = OUT.with_name("nflcom_player_splits_stadiums_witness.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
