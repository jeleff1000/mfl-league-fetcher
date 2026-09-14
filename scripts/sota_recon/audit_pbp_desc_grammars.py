"""Exhaustive PBP ``desc`` grammar census.

This is an evidence audit only.  It reads the raw PBP lake and writes a JSON
receipt; it never writes a canonical/supertable parquet.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import duckdb


RAW_PBP = Path(
    r"D:\league-history-data\nfl\raw\stathead\generated\pbp_merged_1978_2025\nfl_pbp_1978_2025_merged.parquet"
)
OUT = Path(r"D:\yahoo_oauth\docs\audits\pbp-desc-grammar-audit.json")
RECLASS_OUT = Path(r"D:\yahoo_oauth\docs\audits\pbp-desc-grammar-reclassification.json")


# Player references are the principal source of fake "yardages" in free text.
# Remove them before normalizing numbers, while retaining all result numbers.
PLAYER_REF = re.compile(
    r"\b(?:\d{1,2}|NULL)-[A-Za-z][A-Za-z.'-]*(?:-[A-Za-z][A-Za-z.'-]*)*\b"
)
CLOCK = re.compile(r"\(?\d{1,2}:\d{2}(?:\.\d+)?\)?")
INJURY = re.compile(r"\*\*\s*Injury Update:.*?(?=(?:PENALTY|$))", re.I)
LOCATION = re.compile(r"\b[A-Z]{2,3}\s+(?:\d{1,2}|00)\b")
NUMBER = re.compile(r"[-+]?\d+(?:\.\d+)?")


FAMILY_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "SCORING": (
        re.compile(r"\btouchdown\b|\bfield goal\b|\bextra point\b|\bsafety\b", re.I),
        re.compile(r"\btwo-point conversion attempt\b|\bdefensive two-point attempt\b", re.I),
    ),
    "PASSING": (
        re.compile(r"\bpass (?:complete|incomplete|intended|short|deep)\b|\bpasser\b", re.I),
        re.compile(r"\bsacked\b|\bscrambles?\b|\bspike(?:d)?\b|\bthrowaway\b", re.I),
        re.compile(r"\bintended for\b|\b(?:new )?qb\b|\bin at qb\b|\bin at quarterback\b|\bas quarterback\b|\b(?:quater|quarter)back change\b|\bpass\b", re.I),
    ),
    "RUSHING": (
        re.compile(r"\b(?:left|right|middle|up the middle|quarterback)\b.*\b(?:to|for)\b", re.I),
        re.compile(r"\b(?:kneels?|knee down|takes? a knee|took a knee)\b", re.I),
        re.compile(r"\bfor\s+(?:<n>|[-+]?\d+|no gain)\s+yards?\b|\bno gain\b", re.I),
        re.compile(r"\byard run by\b", re.I),
    ),
    "RECEIVING": (re.compile(r"\bpass .*\bto\b.*\bfor\b|\bcomplete\b", re.I),),
    "PUNTING": (re.compile(r"\bpunts?\b|\bfair catch\b", re.I),),
    "KICKING": (re.compile(r"\bkicks?\b|\bfield goal\b|\bextra point\b", re.I),),
    "RETURNS": (re.compile(r"\breturn(?:ed)?\b|\btouchback\b|\bout of bounds\b", re.I),),
    "TURNOVERS": (re.compile(r"\bintercepted\b|\bfumbles?\b|\bmuff(?:ed)?\b|\brecovered\b", re.I),),
    "DEFENSE": (re.compile(r"\bsack(?:ed)?\b|\btackle\b|\bforced fumble\b|\bdefensive\b|\bblocked by\b", re.I),),
    "PENALTY": (re.compile(r"\bpenalty\b|\bno play\b|\bdeclined\b|\boffsetting\b|\benforced\b", re.I),),
    "REPLAY": (re.compile(r"\breplay official\b|\breplay\b|\bplay under review\b|\breversed\b|\bupheld\b", re.I),),
    "PLAY_STATE": (
        re.compile(r"\bplay (?:cancelled|postponed|did not occur)\b|\bblown dead\b|\bdown replayed\b|\bskycam wire\b", re.I),
        re.compile(r"\brunner ruled down\b|\bplay cannot be challenged\b|\bblank play\b", re.I),
    ),
    "ADMIN_CONTEXT": (
        re.compile(r"\btimeout\b|\binjury update\b|\btwo-minute warning\b|\bend (?:of (?:the )?)?(?:the )?(?:\w+ )?(?:quarter|half|game)\b", re.I),
        re.compile(r"\b(?:game|end game|end of regulation|captains?|captians?|coin toss|won the toss|defers?|deferred|opening kickoff|aborted snap)\b|^\(<clock>\)$|^<clock>$", re.I),
        re.compile(r"\bchange of possession\b|\byardline difference\b|\bofficial charged time out\b|\btime out\b|\bsideline warning\b", re.I),
    ),
    "EDITORIAL_CONTEXT": (re.compile(r"\b(?:career|all-time|sole possession|most|record|milestone)\b", re.I),),
    "INJURY_CONTEXT": (re.compile(r"\binjur(?:y|ed)\b|\blocker room\b|\bx-rays?\b|\bbruised kidney\b", re.I),),
    "WEATHER_CONTEXT": (re.compile(r"\b(?:temp|temperature|humidity|wind|winds|rain|lightning|weather|degrees?)\b", re.I),),
    "ROSTER_CONTEXT": (re.compile(r"\b(?:captains?|substitution infraction|reported in as eligible)\b", re.I),),
}


def normalize_desc(raw: str) -> str:
    """Remove identity/location noise but retain result wording and numbers."""
    text = str(raw)
    text = INJURY.sub(" <INJURY_ANNOTATION> ", text)
    text = PLAYER_REF.sub("<PLAYER>", text)
    text = CLOCK.sub("<CLOCK>", text)
    text = LOCATION.sub("<FIELD_LOCATION>", text)
    text = NUMBER.sub("<N>", text)
    text = re.sub(r"\b(?:NULL|NONE)\b", "<NULL>", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def families(text: str) -> list[str]:
    found = [name for name, patterns in FAMILY_PATTERNS.items() if any(p.search(text) for p in patterns)]
    return found or ["UNCLASSIFIED"]


def main() -> None:
    if "--reclassify-existing" in sys.argv:
        receipt = json.loads(OUT.read_text(encoding="utf-8"))
        class_counts = Counter()
        unmatched_examples: list[str] = []
        for item in receipt["grammar_inventory"]:
            classes = item.get("families", [])
            if "UNCLASSIFIED" in classes:
                classes = families(str(item["grammar"]))
            item["families"] = sorted(set(classes))
            class_counts.update(classes)
            if "UNCLASSIFIED" in classes and len(unmatched_examples) < 100:
                unmatched_examples.extend(item.get("examples", [])[: 100 - len(unmatched_examples)])
        receipt["family_counts"] = dict(sorted(class_counts.items()))
        receipt["unclassified_grammar_count"] = sum(
            1 for item in receipt["grammar_inventory"] if "UNCLASSIFIED" in item["families"]
        )
        receipt["unclassified_examples"] = unmatched_examples
        receipt["result_families"] = sorted(x for x in class_counts if x != "UNCLASSIFIED")
        # Keep the large immutable census receipt intact; this compact companion
        # records only the post-classification disposition changes.
        changed = [
            {"grammar": item["grammar"], "rows": item["rows"], "families": item["families"], "examples": item.get("examples", [])}
            for item in receipt["grammar_inventory"]
            if "UNCLASSIFIED" not in item.get("families", [])
        ]
        reclass = {
            "source_receipt": str(OUT),
            "grammar_count": receipt["grammar_count"],
            "unclassified_grammar_count_after_reclassification": receipt["unclassified_grammar_count"],
            "family_counts_after_reclassification": receipt["family_counts"],
            "reclassified_non_unclassified_count": len(changed),
            "unclassified_examples_after_reclassification": receipt["unclassified_examples"],
            "unclassified_grammars_after_reclassification": [
                {"grammar": item["grammar"], "rows": item["rows"], "examples": item.get("examples", [])}
                for item in receipt["grammar_inventory"]
                if "UNCLASSIFIED" in item["families"]
            ],
        }
        RECLASS_OUT.write_text(json.dumps(reclass, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps({"grammar_count": receipt["grammar_count"], "unclassified_grammar_count": receipt["unclassified_grammar_count"], "family_counts": receipt["family_counts"], "output": str(RECLASS_OUT)}, indent=2))
        return
    if not RAW_PBP.exists():
        raise FileNotFoundError(RAW_PBP)
    con = duckdb.connect()
    source = RAW_PBP.as_posix().replace("'", "''")
    # Normalize and group in DuckDB. This keeps the census exhaustive while avoiding
    # a Python regex pass over every one of the 2M+ play rows.
    normalized = f"""
      lower(trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace("desc", '{PLAYER_REF.pattern.replace("'", "''")}', '<PLAYER>', 'g'),
                '{CLOCK.pattern.replace("'", "''")}', '<CLOCK>', 'g'
              ),
              '{LOCATION.pattern.replace("'", "''")}', '<FIELD_LOCATION>', 'g'
            ),
            '{NUMBER.pattern.replace("'", "''")}', '<N>', 'g'
          ),
          '\\s+', ' ', 'g'
        )
      ))
    """
    grouped_sql = f"""
      WITH src AS (
        SELECT season, "desc" AS description, {normalized} AS grammar
        FROM read_parquet('{source}')
        WHERE "desc" IS NOT NULL AND trim("desc") <> ''
      )
      SELECT grammar, count(*) AS rows, min(season) AS first_year,
             max(season) AS last_year, list(description)[1:3] AS examples
      FROM src
      GROUP BY grammar
    """
    rows = con.execute(grouped_sql).fetchall()
    total = con.execute(f"SELECT count(*) FROM read_parquet('{source}')").fetchone()[0]
    descriptions_nonempty = con.execute(f"SELECT count(*) FROM read_parquet('{source}') WHERE \"desc\" IS NOT NULL AND trim(\"desc\") <> ''").fetchone()[0]
    templates: list[dict[str, object]] = []
    class_counts = Counter()
    unmatched_examples: list[str] = []
    for grammar, count, first_year, last_year, examples in rows:
        classes = families(str(grammar))
        class_counts.update(classes)
        item = {
            "grammar": str(grammar),
            "rows": int(count),
            "first_year": int(first_year),
            "last_year": int(last_year),
            "families": sorted(set(classes)),
            "examples": [str(x) for x in (examples or [])],
        }
        templates.append(item)
        if "UNCLASSIFIED" in classes and len(unmatched_examples) < 100:
            unmatched_examples.extend(item["examples"][: 100 - len(unmatched_examples)])
    con.close()

    inventory = sorted(templates, key=lambda x: (-int(x["rows"]), str(x["grammar"])))
    result_families = [x for x in class_counts if x != "UNCLASSIFIED"]
    receipt = {
        "source": str(RAW_PBP),
        "scope": {"first_year": min(int(x["first_year"]) for x in inventory), "last_year": max(int(x["last_year"]) for x in inventory)},
        "normalization": {
            "player_references_removed": True,
            "field_locations_removed": True,
            "clocks_removed": True,
            "result_numbers_retained_as_tokens": True,
            "raw_examples_retained_per_grammar": 3,
        },
        "row_counts": {"rows_total": total, "descriptions_nonempty": descriptions_nonempty},
        "family_counts": dict(sorted(class_counts.items())),
        "grammar_count": len(inventory),
        "unclassified_grammar_count": sum(1 for x in inventory if "UNCLASSIFIED" in x["families"]),
        "unclassified_examples": unmatched_examples,
        "result_families": sorted(result_families),
        "grammar_inventory": inventory,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(receipt, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "rows_total": total,
        "descriptions_nonempty": descriptions_nonempty,
        "grammar_count": len(inventory),
        "unclassified_grammar_count": receipt["unclassified_grammar_count"],
        "families": dict(sorted(class_counts.items())),
        "output": str(OUT),
    }, indent=2))


if __name__ == "__main__":
    main()
