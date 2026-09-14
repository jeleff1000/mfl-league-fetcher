"""The DENOMINATOR of `contracts_unenrolled_in_scoreboard`, one layer down.

THE DEFECT THIS CLOSES. `contracts_unenrolled_in_scoreboard` read 0 all session and the
claim underneath it was false. It counts `*.json` in ONE DIRECTORY -- 13 files -- while the
decisions that actually govern this program increasingly live in PYTHON: `MAPPED`,
`EXCLUDED`, `ESCALATED`, `NEW_CANDIDATES`, `MAPPING_PENDING`, `DOCUMENTED_RESIDUALS`. The
2026-07-28 session added three correspondence tables and the gate did not move, because a
correspondence table is not a `.json` file. Same shape as the other four wrong counters:
the counter measured its ARTIFACT TYPE rather than the thing the artifact is a container
for. It is the last known instance, and it is the reason this module exists.

WHY NOT JUST COUNT ALL 536 REGISTRIES. Because that denominator is inflated in the
flattering-in-reverse direction -- most module-level constants in this package are ordinary
lookup tables (`_BUCKETS`, column name lists, SQL fragments) and belong in code. A queue of
536 hand classifications would be a queue nobody burns down, which is a worse outcome than
a queue of 50 that stays honest. So the full 536 IS the reported denominator and every one
of them carries the classifier's verdict, but only the ADJUDICATION class is gated.

⚠ THIS MODULE SHIPPED WITH THE DEFECT IT WAS WRITTEN TO FIX, and the fix is the second
version. The first walked `os.listdir(HERE)` -- ONE DIRECTORY -- exactly as the .json gate
it replaced walked one directory, and it counted only assignments from container LITERALS.
Two holes, both found by turning the law on the new counter within hours of shipping it:

  SUBDIRECTORIES   61 .py files under corrections/, witness_gate/ and witness_audit_v2/
                   held 94 module-level registries the gate never opened -- including
                   `witness_gate/build_stat_contracts.py:ADJUDICATION_RULES`, which is
                   named for what it is, and `corrections/*.py:CORRECTIONS`, which are
                   applied data corrections.
  BUILT BY A CALL  a registry does not stop being one because it was written `set(...)`
                   instead of `{...}`. `column_dossier.py:QUARANTINED_SOURCES` -- a
                   decision about which sources may NOT be adjudicated, inside a SINK
                   module -- was invisible for exactly that reason, as was
                   `newspaper_witness_common.py:ATOM_TO_V26_COL`, an atom -> canonical map.

Denominator 405 -> 536, adjudication registries 37 -> 50. `Path(...)` and `os.path.join`
are NOT containers and are still declined, by an explicit constructor list rather than by
accident, and the count of call-built registries is reported so the distinction stays
visible instead of becoming a silent filter.

THE CLASSIFIER IS TWO MECHANICAL RULES, and it is deliberately over-inclusive.

  SINK   the defining module references a DECISION SINK -- it writes into the adjudication
         ledger, or it declares a scoreboard gate/queue. Then EVERY module-level registry
         in it is an adjudication registry, including short-valued ones. This is the
         load-bearing rule: a new adjudication generator CANNOT avoid it, because writing
         decisions means calling `apply_to_ledger`. `MAPPED` is caught here and nowhere
         else -- its values are canonical column names, so no prose rule would ever see it,
         and it is the single most consequential decision table in the program.

  PROSE  at least half of the registry's string values are >= 60 characters, i.e. it
         carries REASONS rather than tokens. This catches decision registries that live
         outside the sink modules (`asymmetry_registry.DOCUMENTED_RESIDUALS`,
         `test_mapping_obligation.MAPPING_PENDING`).

Over-inclusion is the correct failure direction: a lookup table swept in by SINK costs one
line saying it is a lookup and why. A decision table missed costs a false clean gate, which
is what this module exists to stop.

    python -m scripts.sota_recon.adjudication_registry_census
"""
from __future__ import annotations

import ast
import json
import os
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
RECEIPT = HERE.parents[1] / "docs" / "adjudication-registry-census.json"

# Names whose presence in a module means that module PRODUCES decisions or gate outcomes.
# `apply_to_ledger` / `DISPOSITIONS_PATH` / `load_decisions` are the adjudication ledger;
# `_declared_gates` / `_declared_queues` are the scoreboard's own declarations.
DECISION_SINKS = {"apply_to_ledger", "DISPOSITIONS_PATH", "load_decisions",
                  "_declared_gates", "_declared_queues"}

PROSE_MIN_CHARS = 60
PROSE_MIN_SHARE = 0.5

_CONTAINERS = (ast.Dict, ast.Set, ast.List, ast.Tuple)
_COMPREHENSIONS = (ast.DictComp, ast.SetComp, ast.ListComp)
# A registry does not stop being a registry because it was built by a call. `set(...)`,
# `frozenset(...)` and a dict comprehension are containers; `Path(...)` and `os.path.join`
# are not. The list is explicit so the classifier can say WHY it declined something.
_CONTAINER_CALLS = {"dict", "set", "frozenset", "list", "tuple", "defaultdict",
                    "OrderedDict", "Counter"}


def _container_call(value: ast.expr) -> str | None:
    """-> the constructor name if this expression BUILDS a container, else None."""
    if isinstance(value, _COMPREHENSIONS):
        return type(value).__name__
    if isinstance(value, ast.Call):
        function = value.func
        name = (function.id if isinstance(function, ast.Name)
                else function.attr if isinstance(function, ast.Attribute) else None)
        if name in _CONTAINER_CALLS:
            return f"{name}()"
    return None


def _registries(tree: ast.Module) -> list[tuple[str, ast.expr, str | None]]:
    out = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t for t in node.targets if isinstance(t, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target]
        else:
            continue
        for target in targets:
            if not target.id.isupper():
                continue
            if isinstance(node.value, _CONTAINERS):
                out.append((target.id, node.value, None))
                continue
            built = _container_call(node.value)
            if built is not None:
                out.append((target.id, node.value, built))
    return out


def _prose(value: ast.expr) -> tuple[bool, int, float]:
    """-> (is_prose, string_value_count, median length). Only dicts carry reasons."""
    if not isinstance(value, ast.Dict):
        return False, 0, 0.0
    strings = [v.value for v in value.values
               if isinstance(v, ast.Constant) and isinstance(v.value, str)]
    if not strings or not value.values:
        return False, 0, 0.0
    long_enough = sum(1 for s in strings if len(s) >= PROSE_MIN_CHARS)
    median = float(statistics.median(len(s) for s in strings))
    share = long_enough / len(value.values)
    return share >= PROSE_MIN_SHARE, len(strings), median


def build() -> dict:
    rows: list[dict] = []
    unparseable: list[str] = []
    # RECURSIVE, and that is the whole point. The first version of this module walked
    # `os.listdir(HERE)` -- ONE directory -- which is precisely the defect it was written to
    # replace, and it shipped that way. 61 .py files in corrections/, witness_gate/ and
    # witness_audit_v2/ held 94 module-level registries the gate never opened, including
    # `corrections/*.py:CORRECTIONS`, which are applied data corrections.
    for path in sorted(HERE.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        name = path.relative_to(HERE).as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            unparseable.append(name)
            continue
        referenced = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        referenced |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                referenced |= {alias.name for alias in node.names}
        is_sink = bool(referenced & DECISION_SINKS)
        for registry, value, built_by in _registries(tree):
            prose, strings, median = _prose(value)
            reasons = []
            if is_sink:
                reasons.append("SINK")
            if prose:
                reasons.append("PROSE")
            rows.append({
                "id": f"{name}:{registry}",
                "module": name, "registry": registry,
                # a comprehension or a constructor call has no statically countable size;
                # recorded as null rather than 0, which would read as "empty"
                "entries": (len(value.values if isinstance(value, ast.Dict) else value.elts)
                            if built_by is None else None),
                "built_by": built_by,
                "string_values": strings,
                "median_value_chars": median,
                "classification": "ADJUDICATION" if reasons else "LOOKUP",
                "matched_rules": reasons,
            })
    adjudication = [row for row in rows if row["classification"] == "ADJUDICATION"]
    return {
        "generated": "the denominator of contracts_unenrolled_in_scoreboard, one layer "
                     "down: decisions live in Python registries, not only in .json files",
        "rules": {"SINK": sorted(DECISION_SINKS),
                  "PROSE": f">= {PROSE_MIN_SHARE:.0%} of string values >= "
                           f"{PROSE_MIN_CHARS} chars"},
        "counters": {
            "module_level_registries": len(rows),
            "adjudication_registries": len(adjudication),
            "lookup_registries": len(rows) - len(adjudication),
            "sink_modules": len({row["module"] for row in rows if "SINK" in row["matched_rules"]}),
            "registries_built_by_call_or_comprehension": sum(
                1 for row in rows if row["built_by"]),
            "modules_unparseable": len(unparseable),
        },
        "modules_unparseable": unparseable,
        "adjudication_registries": sorted(row["id"] for row in adjudication),
        "registries": sorted(rows, key=lambda row: row["id"]),
    }


def write() -> dict:
    document = build()
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    document = write()
    for name, value in document["counters"].items():
        print(f"  {name:28s} {value}")
    print("\nADJUDICATION registries (must be enrolled in closure_scoreboard):")
    for row in document["registries"]:
        if row["classification"] != "ADJUDICATION":
            continue
        size = f"n={row['entries']:<4d}" if row["entries"] is not None else (
            f"{row['built_by']:<7s}")
        print(f"  {row['id']:64s} {size} {'+'.join(row['matched_rules'])}")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
