"""Extract constants + disassembly from vaulted .pyc files (salvage law, master plan §18.0).

Reads the orphaned bytecode preserved in docs/salvage/pyc-evaporated-2026-07-21-22/ and
emits human-readable, diff-able text so the lost modules' registry contents, SQL, and
structure survive independently of pyc parseability. Run from repo root:

    python -m scripts.sota_recon.extract_pyc_constants
"""

from __future__ import annotations

import dis
import io
import json
import marshal
import types
from pathlib import Path

VAULT = Path("docs/salvage/pyc-evaporated-2026-07-21-22")
OUT = VAULT / "extracted"

# vault subdir -> original source dir (for orphan detection)
ORIGIN = {
    "sota_recon": Path("scripts/sota_recon"),
    "witness_audit_v2": Path("scripts/sota_recon/witness_audit_v2"),
    "research_cohorts": Path("scripts/research_cohorts"),
    "scripts_root": Path("scripts"),
}


def module_name(pyc: Path) -> str:
    return pyc.name.split(".cpython-")[0]


def load_code(pyc: Path) -> types.CodeType:
    return marshal.loads(pyc.read_bytes()[16:])


def collect(code: types.CodeType, bag: list[dict], prefix: str = "") -> None:
    qual = f"{prefix}{code.co_name}"
    entry: dict = {
        "qualname": qual,
        "docstring": None,
        "strings": [],
        "tuples": [],
        "numbers": [],
        "names": list(code.co_names),
        "varnames": list(code.co_varnames),
    }
    children: list[types.CodeType] = []
    consts = code.co_consts
    if consts and isinstance(consts[0], str):
        entry["docstring"] = consts[0]
    for i, c in enumerate(consts):
        if isinstance(c, types.CodeType):
            children.append(c)
        elif isinstance(c, str):
            if not (i == 0 and c == entry["docstring"]):
                entry["strings"].append(c)
        elif isinstance(c, (tuple, frozenset)):
            entry["tuples"].append(repr(c))
        elif isinstance(c, (int, float)) and not isinstance(c, bool):
            entry["numbers"].append(c)
    bag.append(entry)
    for child in children:
        collect(child, bag, prefix=qual + ".")


def extract(pyc: Path, out_dir: Path) -> dict:
    mod = module_name(pyc)
    code = load_code(pyc)
    bag: list[dict] = []
    collect(code, bag, prefix=f"{mod}:")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{mod}.consts.json").write_text(
        json.dumps(bag, indent=1, default=str), encoding="utf-8"
    )
    buf = io.StringIO()
    dis.dis(code, file=buf)
    (out_dir / f"{mod}.dis.txt").write_text(buf.getvalue(), encoding="utf-8")
    return {
        "module": mod,
        "code_objects": len(bag),
        "docstring": bool(bag and bag[0]["docstring"]),
        "strings": sum(len(e["strings"]) for e in bag),
        "tuples": sum(len(e["tuples"]) for e in bag),
    }


def main() -> None:
    index: list[str] = ["# Extracted bytecode salvage index", ""]
    seen: set[tuple[str, str]] = set()
    for subdir, origin in ORIGIN.items():
        vault_dir = VAULT / subdir
        if not vault_dir.is_dir():
            continue
        # prefer plain pycs over pytest-rewritten ones for the same module
        pycs = sorted(vault_dir.glob("*.pyc"), key=lambda p: ("pytest" in p.name, p.name))
        for pyc in pycs:
            mod = module_name(pyc)
            if (subdir, mod) in seen:
                continue
            if (origin / f"{mod}.py").exists():
                continue  # not an orphan; source still lives in the repo
            seen.add((subdir, mod))
            try:
                stats = extract(pyc, OUT / subdir)
                index.append(
                    f"- `{subdir}/{mod}` — {stats['code_objects']} code objects, "
                    f"{stats['strings']} strings, {stats['tuples']} tuples"
                    f"{', docstring' if stats['docstring'] else ''}"
                )
            except Exception as exc:  # record the failure; never hide it
                index.append(f"- `{subdir}/{mod}` — EXTRACTION FAILED: {exc}")
    (OUT / "INDEX.md").write_text("\n".join(index) + "\n", encoding="utf-8")
    print("\n".join(index))


if __name__ == "__main__":
    main()
