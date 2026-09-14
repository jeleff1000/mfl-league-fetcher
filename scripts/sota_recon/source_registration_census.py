"""
sota_recon/source_registration_census.py  --  O.7: DISK-FIRST source registration census

The 2026-07-26 failure this closes: every census (physical snapshot, witness map,
lineage contract) enumerated sources.py -- so material sitting on disk WITHOUT a
registry row was invisible to every zero-counter. "All 75 sources declared" was
vacuously true because unregistered sources were never in the denominator
(nflcom full harvest, PFA expansion leads, ...). The whole point of the program
is registering EVERYTHING; the denominator must be the DISK.

This lane walks the raw-source roots, maps each candidate source directory to the
registry by path containment, and requires every directory to be exactly one of:

  REGISTERED     >=1 sources.py entry lives under it (entries listed)
  QUEUED         unregistered, with a queue note naming what registration needs
  EXCLUDED       unregistered forever, with a mandatory reason

Zero-counter (scoreboard): unregistered_without_disposition = 0. A new harvest
directory landing on disk FAILS the test until someone gives it a disposition --
dropping a source silently is now structurally impossible.

...WHICH WAS TRUE OF A PUBLISHER AND FALSE OF A DATASET (corrected 2026-07-28).
The walk above stops one level under each scan root, so `ff_assets/statscrew` read
REGISTERED on the strength of ONE dataset having a registry row, while
`ff_assets/statscrew/team_season_stats` sat on disk holding 137,864 rows -- registered
nowhere, and invisible to every gate for a full day. Registration happens per
(publisher, DATASET), so the denominator now reaches child grain too:

  Zero-counter: child_unregistered_without_disposition = 0

which raised the dispositioned denominator from 15 directories to 15 + 18 children and
immediately surfaced `ff_assets/profootballarchives/site_metadata` -- five PFA
site-metadata pages including the coverage declaration that produced
docs/pfa-declared-coverage.json, cited in two handoffs while sitting outside the census.

Output: docs/source-registration-census.json

Run:  python -m scripts.sota_recon.source_registration_census
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from . import sources as S

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "source-registration-census.json")

# Disk roots whose CHILDREN are candidate source families. curated/ and derived/
# are internal-lineage products of these raws by construction (they enter the
# census the day a registry row points at them, as ancient_ready_bundle does).
SCAN_ROOTS = [
    "D:/league-history-data/nfl/raw",
    "D:/league-history-data/nfl/ff_assets",
    "D:/sports-data/nfl",
    "D:/sports-data",
]
# witness_gate's parallel census: every dataset it declares must land inside a
# dispositioned census dir (the two key-spaces stay joined, §18)
SOURCE_CENSUS_CONTRACT = os.path.join(os.path.dirname(__file__), "witness_gate",
                                      "contracts", "source_census.v1.json")
LAKE_ROOT = "D:/league-history-data/nfl"
# scan-root children that are themselves scan roots (avoid double-listing)
_NESTED = {Path("D:/sports-data/nfl").resolve()}

# Top-level lake dirs that are NOT source-capture roots (internal machinery by
# construction). A NEW top-level dir appearing in the lake fails the census --
# a future harvest can't land outside the scan list unnoticed.
KNOWN_LAKE_TOPLEVEL = {
    "raw", "ff_assets",                       # scanned source roots
    "curated", "derived", "facts",            # internal-lineage products
    "releases", "artifacts", "staging",       # build outputs / scratch
    "ops_data", "ops_cache", "fly_downloads", # ops copies
    "fly_snapshots", "logs", "tmp", "tools",
    "test_fixtures",
}

DISPOSITION_VOCAB = {"REGISTERED", "QUEUED", "EXCLUDED"}

# Explicit dispositions for unregistered directories. Every entry is a decision
# with a receipt-note, not a default.
DISPOSITIONS: dict[str, tuple[str, str]] = {
    "raw/pfa": ("EXCLUDED",
                "MEASURED EMPTY 2026-07-26: raw/pfa/player_index_source_leads contains "
                "zero files (the O.7 census assumed expansion leads were present). The "
                "live PFA material is the registered ff_assets participation capture "
                "(pfa_player_game_participation, 5.36M rows, 20/20 shards) plus the "
                "curated ancient bundle streams. Delete-or-refill decision queued to "
                "Joe alongside raw/nflverse (deletion discipline -- an empty directory "
                "is never removed by a lane)"),
    "raw/legacy_supertable_backup_sources": (
        "EXCLUDED", "internal-lineage backups of our own supertable -- copies, "
                    "not witnesses; parent-copy parity is the only legal role"),
    "nfl/catalog": ("EXCLUDED",
                    "column-catalog metadata (supertable_columns.xlsx etc.), not "
                    "a stat source"),
    "nfl/source": ("EXCLUDED",
                   "MEASURED 2026-07-26: contains ops STAGING COPIES of already-"
                   "registered material (pfr_players/ and pfr_boxscores/ mirror "
                   "raw/pfr/*, pbp/stathead_generated/ mirrors raw/stathead/generated/, "
                   "backups/ holds supertable backups) plus supertable_columns.xlsx -- "
                   "a COLUMN CATALOG (4 sheets, ~1,900 column-metadata rows) consumed "
                   "by build_external_witness_crosswalk.py. The O.3 'external-witness "
                   "crosswalk' is therefore a COLUMN crosswalk (§18 spine input), not a "
                   "stat-value witness. Registering these copies as separate sources "
                   "would fold one bloodline into two roots and corrupt independence "
                   "counts -- the exact failure the registry exists to prevent. Same "
                   "class as raw/legacy_supertable_backup_sources and nfl/catalog. "
                   "SUPERSET RECEIPT (counted 2026-07-26, so this exclusion loses "
                   "nothing): the REGISTERED raw/pfr/boxscores holds 164,054 parquet "
                   "vs this staging copy's 89,979 -- the copy is an OLDER, SMALLER "
                   "snapshot, not extra material"),
    "sports-data/cfb": ("EXCLUDED", "college football -- out of NFL domain"),
    "raw/nflverse": ("EXCLUDED",
                     "empty directory (verified 2026-07-26, 0 bytes) -- nflverse "
                     "pbp material lives inside raw/stathead/generated as the "
                     "registered pbp_merged family; delete-or-refill decision "
                     "queued to Joe (deletion discipline)"),
    "ff_assets/runs": ("EXCLUDED",
                       "capture-run bookkeeping (import manifests/logs), not a "
                       "stat source"),
}

# ---------------------------------------------------------------------------
# CHILD-GRAIN DISPOSITIONS (2026-07-28) -- the layer beneath the publisher.
#
# The counter above walked ONE level under each scan root and asked whether ANY registry
# row lived under that directory. `ff_assets/statscrew` therefore read REGISTERED on the
# strength of a single dataset, and `unregistered_without_disposition` reported 0 while
# `ff_assets/statscrew/team_season_stats` sat on disk holding 137,864 rows with no
# registry row at all -- for a full day, through every gate run.
#
# That is this module's own docstring failing one layer down. It says "the denominator
# must be the DISK" and "a new harvest directory landing on disk FAILS the test"; both
# were true only of a new PUBLISHER. Registration happens per (publisher, DATASET), so
# the denominator has to reach dataset grain.
#
# The walk is one `iterdir` per registered directory -- no deep recursion, because
# `raw/newspaper_archives` is an archive-of root and recursing it would cost minutes to
# answer a yes/no question. A child is covered when a registry row sits AT it, UNDER it,
# or ABOVE it (a source registered at the parent directory owns its whole subtree --
# newspaper_raw_archives is registered that way and its three children are inside it).
CHILD_DISPOSITIONS: dict[str, tuple[str, str]] = {
    "raw/nflcom/cache": (
        "EXCLUDED",
        "RETAINED CAPTURE BYTES for the registered nflcom table families (47,060 pages, "
        "keyed sha1(url)), not a separate witness. O.9.0's semantic column census READS "
        "them -- the block grammar that receipted every layout signature was regenerated "
        "from exactly these pages -- so they are evidence about a registered source, not "
        "a source. Registering them would fold one bloodline into two roots. NOTE the "
        "measured limit recorded in O.9.0b: the cache holds only 12.3%/12.9% of the "
        "pages that built splits/situational (the rest came from other GH runners), so "
        "it is NOT a complete re-parse input"),
    "ff_assets/profootballarchives/site_metadata": (
        "QUEUED",
        "FIVE PFA site-metadata pages, and one of them is already load-bearing: "
        "nflgamelogcoverage.html.gz is the site's OWN per-statistic coverage declaration "
        "and is what produced docs/pfa-declared-coverage.json -- the artifact that scopes "
        "every PFA absence claim (volume stats 1960+, not 1920+). It has been cited in "
        "two handoffs while sitting outside the census. statkey.html.gz is the "
        "DEFINITIONS page, still unparsed, and bears on two named open items "
        "(def_air_yards_allowed definition-version, fum_rec own-vs-opp). "
        "nflrosterlimits.html.gz feeds the §22 league-structure vector. "
        "REGISTRATION NEEDS: these are per-site declarations, not player/team rows -- "
        "they have no entity grain, so the honest form is a `context` / "
        "NON_MAPPING_ROLE registration like newspaper_raw_archives, plus a parser for "
        "statkey. Queued rather than excluded because the material is real and already "
        "in use"),
    "raw/legacy_supertable_backup_sources/legacy_nfl_exports_review": (
        "EXCLUDED",
        "review scratch (60 entries) beside our own supertable backups. Same class as "
        "its parent: internal-lineage COPIES of material we produced, never a witness -- "
        "parent-copy parity is the only legal role and the registered "
        "legacy_motherduck_supertable already carries it"),
}

# contract datasets whose roots live outside the scanned dirs (derived/ layer):
# each needs its own disposition or the outside-census counter is nonzero
DATASET_DISPOSITIONS: dict[str, tuple[str, str]] = {
    "derived.profootballarchives.player_game_participation": (
        "EXCLUDED", "MEASURED 2026-07-26: derived/profootballarchives/ does NOT EXIST "
                    "on disk -- this derived form was never materialized. The live PFA "
                    "participation data is the REGISTERED ff_assets capture "
                    "(pfa_player_game_participation). Keeping a contract row for an "
                    "unmaterialized path would let a future re-materialization register "
                    "the same rows as a second pfa_loc witness"),
    "lake.external_witness_intake": (
        "EXCLUDED", "MEASURED 2026-07-26: EXTERNAL_WITNESS_INDEX.parquet (12,236 rows) "
                    "is a derived INDEX over the same nflcom + statscrew roster captures "
                    "that are now registered (its rows carry source/dataset = "
                    "nflcom|statscrew team_season_roster, plus intake_artifact_* "
                    "columns). It is an intake manifest of registered captures, not an "
                    "independent witness -- registering it would double-count those "
                    "rosters"),
}


def _norm(p: str | Path) -> str:
    return str(Path(p).resolve()).replace("\\", "/").lower()


def _registry_paths() -> dict[str, str]:
    out = {}
    for key, src in S.registry(include_subject=True).items():
        p = getattr(src, "path", None)
        if p:
            out[key] = _norm(p)
    return out


def _disposition_key(root: Path, child: Path) -> str:
    return f"{root.name}/{child.name}"


def _covers(registered: str, candidate: str) -> bool:
    """Is `candidate` accounted for by a source registered at `registered`?

    THREE relations, and the third is the one a first pass gets wrong. A registry row may
    sit AT the directory, UNDER it (sharded captures register at a run dir), or ABOVE it
    -- `newspaper_raw_archives` is registered at the archive root and owns its whole
    subtree, so its three children are covered by their parent. Missing that relation
    reported `loc/`, `source_horizon_acquisitions/` and `source_horizon_game_candidates/`
    as unregistered material when they are the inside of a registered source: a
    denominator inflated by the check meant to catch a deflated one.
    """
    return (registered == candidate
            or registered.startswith(candidate + "/")
            or candidate.startswith(registered + "/"))


def _child_rows(parent: Path, parent_key: str, reg_paths: dict[str, str]) -> list[dict]:
    """One row per immediate child of a REGISTERED directory.

    Registration happens per (publisher, dataset), so the publisher-grain walk above is
    one layer above the truth. One `iterdir`, never a recursive walk.
    """
    rows = []
    try:
        children = sorted(child for child in parent.iterdir() if child.is_dir())
    except OSError:
        return rows
    for child in children:
        cn = _norm(child)
        inside = sorted(k for k, p in reg_paths.items() if _covers(p, cn))
        key = f"{parent_key}/{child.name}"
        if inside:
            rows.append({"key": key, "disposition": "REGISTERED",
                         "registered_sources": inside})
        elif key in CHILD_DISPOSITIONS:
            disposition, note = CHILD_DISPOSITIONS[key]
            rows.append({"key": key, "disposition": disposition, "note": note})
        else:
            rows.append({"key": key, "disposition": "UNREGISTERED_NO_DISPOSITION"})
    return rows


def build() -> dict:
    reg_paths = _registry_paths()
    rows = []
    for root in SCAN_ROOTS:
        rp = Path(root)
        if not rp.exists():
            continue
        for child in sorted(rp.iterdir()):
            if not child.is_dir() or child.resolve() in _NESTED:
                continue
            cn = _norm(child)
            # a source may be registered AT the directory itself (sharded captures /
            # multi-file table families) or at a file beneath it -- both count as
            # REGISTERED, else a directory-shaped registration reads as undispositioned
            inside = sorted(k for k, p in reg_paths.items()
                            if p == cn or p.startswith(cn + "/"))
            key = _disposition_key(rp, child)
            if inside:
                rows.append({"dir": str(child), "key": key,
                             "disposition": "REGISTERED",
                             "registered_sources": inside,
                             "children": _child_rows(child, key, reg_paths)})
            elif key in DISPOSITIONS:
                d, note = DISPOSITIONS[key]
                rows.append({"dir": str(child), "key": key, "disposition": d,
                             "note": note})
            else:
                rows.append({"dir": str(child), "key": key,
                             "disposition": "UNREGISTERED_NO_DISPOSITION"})
    # ---- join witness_gate's parallel census: each declared dataset must land
    # inside a dispositioned dir above (else the two key-spaces have re-split)
    datasets = []
    if os.path.exists(SOURCE_CENSUS_CONTRACT):
        with open(SOURCE_CENSUS_CONTRACT, encoding="utf-8") as f:
            contract = json.load(f)
        dir_by_norm = {_norm(r["dir"]): r for r in rows}
        for d in contract.get("datasets", []):
            glob0 = (d.get("physical_globs") or [""])[0]
            root_dir = _norm(Path(LAKE_ROOT) / glob0.split("**")[0].rstrip("/*"))
            hit = None
            for dn, r in dir_by_norm.items():
                if root_dir.startswith(dn):
                    hit = r
                    break
            did = d.get("dataset_id")
            if hit:
                disp, note = hit["disposition"], None
            elif did in DATASET_DISPOSITIONS:
                disp, note = DATASET_DISPOSITIONS[did]
            else:
                disp, note = "DATASET_OUTSIDE_CENSUS", None
            entry = {"dataset_id": did, "resolved_root": root_dir,
                     "census_dir": hit["key"] if hit else None,
                     "disposition": disp}
            if note:
                entry["note"] = note
            datasets.append(entry)

    unknown_toplevel = []
    lake = Path(LAKE_ROOT)
    if lake.exists():
        unknown_toplevel = sorted(
            str(c) for c in lake.iterdir()
            if c.is_dir() and c.name not in KNOWN_LAKE_TOPLEVEL)

    counts = {}
    for r in rows:
        counts[r["disposition"]] = counts.get(r["disposition"], 0) + 1
    return {
        "scan_roots": SCAN_ROOTS,
        "law": "the census denominator is the DISK, not the registry -- every "
               "source directory carries a disposition or the counter is nonzero",
        "counters": {
            "source_dirs": len(rows),
            "registered": counts.get("REGISTERED", 0),
            "queued": counts.get("QUEUED", 0),
            "excluded": counts.get("EXCLUDED", 0),
            "unregistered_without_disposition":
                counts.get("UNREGISTERED_NO_DISPOSITION", 0),
            # THE LAYER BENEATH (2026-07-28). The counter above answers "is this
            # PUBLISHER accounted for"; registration happens per (publisher, DATASET),
            # and `ff_assets/statscrew/team_season_stats` proved the gap by sitting on
            # disk with 137,864 rows while every gate read clean.
            "registered_dirs": sum(1 for r in rows if r["disposition"] == "REGISTERED"),
            "child_dirs": sum(len(r.get("children") or []) for r in rows),
            "child_dirs_registered": sum(
                1 for r in rows for c in (r.get("children") or [])
                if c["disposition"] == "REGISTERED"),
            "child_dirs_queued": sum(
                1 for r in rows for c in (r.get("children") or [])
                if c["disposition"] == "QUEUED"),
            "child_unregistered_without_disposition": sum(
                1 for r in rows for c in (r.get("children") or [])
                if c["disposition"] == "UNREGISTERED_NO_DISPOSITION"),
            "contract_datasets": len(datasets),
            "contract_datasets_outside_census":
                sum(1 for d in datasets
                    if d["disposition"] == "DATASET_OUTSIDE_CENSUS"),
            "unknown_lake_toplevel_dirs": len(unknown_toplevel),
        },
        "unknown_lake_toplevel": unknown_toplevel,
        "rows": rows,
        "contract_datasets": datasets,
    }


def main() -> int:
    doc = build()
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counters"]
    print(f"dirs={c['source_dirs']}  registered={c['registered']}  "
          f"queued={c['queued']}  excluded={c['excluded']}  "
          f"UNDISPOSITIONED={c['unregistered_without_disposition']}")
    for r in doc["rows"]:
        if r["disposition"] == "UNREGISTERED_NO_DISPOSITION":
            print(f"  !! {r['dir']}")
    print(f"census -> {os.path.abspath(SUMMARY_PATH)}")
    return 0 if c["unregistered_without_disposition"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
