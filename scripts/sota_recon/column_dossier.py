"""
sota_recon/column_dossier.py -- O.9.3: ONE ROW PER COLUMN, across every registered source,
each ending in a terminal disposition. The column-level gate that does not exist today.

WHY THIS IS THE POINT OF O.9. `sources_without_mapping_obligation = 0` is a statement about
the 89 SOURCES -- each declares MAPPED / LANE_WITNESSED / NON_MAPPING_ROLE /
MAPPING_PENDING -- and says nothing about their ~5,600 COLUMNS. `nflcom_player_logs` has 57
columns and satisfies its obligation with a single queue note. There is no column-level
counter anywhere in `closure_scoreboard.py`. So the honest state today is that we have
gated the paperwork and not the material, which is exactly the failure
[[feedback-check-the-denominator-beneath]] names: a counter measured one layer above where
the truth lives.

THREE KEYING REGIMES, DETECTED BY MEASUREMENT, NOT DECLARED BY HAND (finding 7):

    WIDE         one logical table per row      key (source, column)
                 51 PFR-class sources; `table_id` / `page_kind` measured single-valued
    MULTI_TABLE  many logical tables per row    key (source, table, column)
                 the 7 nflcom families carry 82 logical tables in 7 registry rows; the new
                 statscrew/PFA captures carry `table_tag`
    LONG         the stat surface is ROW VALUES key (source, stat_name)
                 3 newspaper sources: `newspaper_player_cells` has 56 physical columns but
                 201 distinct `stat_name`s. A dossier keyed on physical columns sees 119
                 columns and MISSES ~585 stat concepts

Each source's regime is decided by probing its schema for a discriminator and COUNTING its
distinct values -- a source asserted MULTI_TABLE whose discriminator turns out to be
single-valued would inflate the denominator with keys that do not exist, and the reverse
would collapse tables silently.

THE O.9.0 REFINEMENT IS LOAD-BEARING: `(source, _table, column)` is sufficient for
`player_career` (its caption IS the position block, measured 1 layout per caption) but NOT
for `player_logs` / `player_logs_targeted`, where `_table` is only the season stratum and
each caption spans SEVEN layouts (DEF_log, WRTE, RBFB5, QB, K_log, P, OL). Those need
+layout, or `yds` under one key means passing, rushing, receiving, INT-return and punt
yards all at once.

Run:  python -m scripts.sota_recon.column_dossier [--source KEY] [--limit N]
"""

from __future__ import annotations

import argparse
import functools
import json
import os
from collections import Counter
from pathlib import Path

import duckdb

from .sources import registry

OUT_PATH = Path(__file__).resolve().parents[2] / "docs" / "column-dossier.json"
NFLCOM_SEMANTICS = Path(__file__).resolve().parents[2] / "docs" / "nflcom-column-semantics.json"
# The ADJUDICATION LEDGER. It lives in contracts/, not in the generated dossier, because
# the dossier is rebuilt from scratch every run: a decision written into it would be
# erased by the next build. That is not hypothetical -- two stale builds silently reverted
# this artifact today, and a hand adjudication lost that way would be unrecoverable and
# invisible.
DISPOSITIONS_PATH = (
    Path(__file__).resolve().parent / "witness_gate" / "contracts" / "column_dispositions.v1.json"
)

# TERMINAL dispositions an adjudication may assign. Deliberately small: every row must end
# up mapped to something canonical, proposed as new material, or excluded WITH a reason.
# "we looked at it" is not a disposition.
TERMINAL_DISPOSITIONS = {
    "MAPPED_TO_CANONICAL": "maps to an existing canonical stat; `canonical` names it",
    "NEW_SUPERTABLE_COLUMN_CANDIDATE": "real material we do not carry; routes to the "
                                       "census + demand join (§24.4), never ad-hoc",
    "EXCLUDED_WITH_REASON": "carries no stat content, argued case by case",
    "DUPLICATE_OF": "the same material as another dossier row; `canonical` names it",
}

# Candidate discriminator columns. Presence alone proves nothing -- each is confirmed by
# counting distinct values on the source itself, and the key is the COMPOSITE of every
# candidate that qualifies.
#
# The composite matters: nflcom `player_season` and `team_stats` carry no `_table` at all.
# They stratify on `_category` (11 and 10 values), `_side` (offense/defense/special-teams)
# and `season_type`. A single-column probe found nothing, typed both as WIDE, and
# collapsed 22 and 32 logical tables into one row each -- the exact 82-tables-in-7-rows
# failure that finding 5 warned about, reproduced by the tool built to prevent it.
DISCRIMINATOR_CANDIDATES = (
    "_table", "table_tag", "table_id", "page_kind", "_category", "_side", "dataset",
    # `_layout` joined 2026-07-28 with the un-shift repoint. It is the SAME concept the
    # NEEDS_LAYOUT machinery below reconstructs for the logs families from an artifact --
    # except the repaired splits/situational tables carry it as a REAL COLUMN (8 values
    # each), so it can be measured like any other discriminator instead of borrowed. That
    # is the better form: a census can go stale, a column cannot.
    "_layout",
)
# SECONDARY discriminators: they subdivide a table only where the source already
# partitions into tables. `season_type` names part of a TABLE on nflcom, which serves a
# separate page per category per season type -- but on v26_release, pbp_merged,
# scoring_summary and the ancient bundles it is a ROW ATTRIBUTE inside one table, and
# `pass_yds` under REG is the same column concept as `pass_yds` under POST.
#
# Treating it as primary doubled v26_release's 1,077 columns into 2,154 dossier rows of
# duplicates and inflated the open queue from 6,027 to 13,012. An inflated denominator is
# the same defect as a deflated one -- it just fails in the flattering direction, and a
# queue nobody can ever burn down is as useless as a counter that reads zero.
SECONDARY_DISCRIMINATORS = ("season_type", "_season_type")
# A discriminator names TABLES, so its cardinality is small. `_player_slug` sits in the
# same underscore namespace with 8,929 values; treating it as a stratum would mint a
# dossier row per player per column.
MAX_DISCRIMINATOR_CARDINALITY = 200
# LONG-format sources put the stat NAME in a row value. `stat_name` is the measured column
# on all three newspaper sources.
LONG_VALUE_COLUMNS = ("stat_name",)

# Sources whose stored column NAMES are known-wrong, so their rows cannot be adjudicated
# at all until a repair lands. `nflcom_player_splits` / `nflcom_player_situational` carry
# the O.9.0 COLUMN-SHIFT defect: the blank split-label header drops its name but not its
# cell, so every value sits one column LEFT of the name it is stored under and the last
# column's value is truncated away. Deciding what `yds` means there would launder a
# defect into a decision that outlives the repair.
#
# These rows are OPEN, and they are open for a reason nobody can act on -- which is a
# different state from "not looked at yet" and is counted separately.
#
# EMPTIED 2026-07-28. Both members were repointed at the O.9.0b unshifted tables, so their
# stored names are now the RIGHT names and there is nothing left to launder. The set stays
# because the state it represents is real and will recur -- a source whose column names are
# known-wrong must never be adjudicated, and an empty set here is a claim ("no source is in
# that state today") rather than a deleted concept.
#
# NOTE THE STAGE BOUNDARY this does NOT cross: emptying this set makes the two families
# ADJUDICABLE, not licensed. They remain MAPPING_PENDING behind the slug->pfr_id crosswalk
# and cast no vote. Identity and licensing are different gates and the dossier only needs
# the first.
QUARANTINED_SOURCES: set[str] = set()

# Sources whose `_table` alone under-specifies the table, so the dossier key needs
# + layout. Measured in O.9.0: each logs caption spans 7 layouts
# (docs/nflcom-column-semantics.json).
#
# `nflcom_player_career` JOINED THIS SET 2026-07-27, and the claim it used to be excluded
# by was half true. This module's docstring said "(source, _table, column) is sufficient
# for player_career (its caption IS the position block, measured 1 layout per caption)".
# Measured against the same artifact that claim cites: SEVEN of its eight captions really
# are 1:1 -- but "Recent Games" (24,428 rows) spans all SEVEN layouts
# (id_recent:{QB,RBFB5,WRTE,DEF_career,K,P,OL}). Under one key, `yds` there is passing AND
# rushing AND receiving AND punt yards at once, which is precisely the collapse this set
# exists to prevent. A rule that holds for 7 of 8 cases is not a measurement.
NEEDS_LAYOUT = {"nflcom_player_logs", "nflcom_player_career"}

# ---- BULK-CLOSABLE COHORTS -------------------------------------------------------
# Capture/provenance plumbing. These are columns we WROTE to record how a row was
# obtained; they are not stat material and can never map to a canonical cell. Closing
# them by rule is legitimate; closing anything else by rule is not.
PROVENANCE_COLUMNS = {
    "source", "dataset", "source_url", "retrieved_at_utc", "content_sha256",
    "parser_version", "artifact_run_id", "shard_id", "scraped_at_utc", "page_url",
    "page_key", "row_index_in_table", "tr_data_row", "index_letter", "index_position",
    "table_caption", "table_index", "source_path", "_file", "_shard", "ingested_at_utc",
    # ---- OUR OWN PIPELINE BOOKKEEPING, swept 2026-07-27 ----
    # 243 open rows across 63 sources were our own plumbing: run ids, staging timestamps,
    # upsert state, and the QA counters the ancient-bundle builders write beside each row.
    # None of them is a measurement of a player or a team; every one records what THIS
    # PIPELINE did to the row. They are closable by rule precisely because they carry no
    # claim a source could disagree with.
    "bundle_source", "upsert_state", "already_in_supertable", "data_source",
    "identity_resolution", "derivation_rules", "source_files",
    "source_player_url", "source_boxscore_url", "boxscore_url",
    "coverage_status", "coverage_statuses", "known_missing_context",
    "stat_completeness_class", "requires_multi_game_suffix",
    "known_stat_cols", "nonzero_known_stat_cols",
    "source_fact_cells", "nonzero_source_fact_cells",
    "bio_lookup_candidate_count", "canonical_source_witness_row_keys",
    "_view",
    # ---- PFR capture/parser emissions, swept 2026-07-27 ----
    # Verified one at a time against their holding tables, because the pfr block is where
    # plumbing-looking names are most often real: `defense`/`offense`/`special_teams` in
    # the snap-count tables are SNAP COUNTS, `stat`/`home_stat`/`vis_stat` in
    # pfr_box_team_stats are a LONG-format stat name and its two values, `reason` in
    # pfr_games_played is a DNP reason, and `name`/`ref_pos` in pfr_box_officials identify
    # the officiating crew. NONE of those are swept. These are.
    "table_id", "page_kind", "home_stathead_id", "raw_text", "play_count_tip",
    "detailed_table_count", "detailed_table_ids", "detailed_table_rows",
    "has_detailed_boxscore_tables", "live_rows", "live_player_weeks", "is_bold",
    "source_kind", "source_page_url", "source_row_index", "franchise_resolution_source",
    # NFL.com targeted-cache reparse receipt fields. These record which retained HTML
    # table produced the normalized audit row; they are not published football facts.
    "_cache_sha1", "_header_signature_sha256", "_source_artifacts",
    "_source_row_index", "_source_table_index",
}
# Provenance SUFFIXES rather than names: every builder that stamps a row invents its own
# prefix (`identity_enrichment_v3_run_id`, `local_promotion_staged_at_utc`,
# `pilot_materialization_run_id`), so enumerating them is a queue that refills itself. No
# stat in the contract registry ends in either -- and the "no stat name closed by rule"
# test is what holds that claim rather than this comment.
PROVENANCE_SUFFIXES = ("_run_id", "_at_utc", "_row_keys")

# ---- CHECKED, DELIBERATELY NOT SWEPT ----
# `source_positions` sat in the same shape as the rows above and is NOT plumbing: it holds
# 'DEF' and 'BB,DB,FB,HB,LB,LDE,LDH,LOE,LOH,QB,RDH,ROH,S,TB,WB' -- the SOURCE'S own
# published position list. `position` is a real canonical with its own taxonomy contract,
# and the multi-valued form needs an adjudicated decision about aggregation. Sweeping it
# with its neighbours would have closed real material by rule, which is the one thing a
# bulk rule may never do.
NOT_PLUMBING_LOOKS_LIKE_IT = {"source_positions"}
# Identity/key columns: they LOCATE a row rather than witness a stat. Their obligation is
# the crosswalk lane (§19.2), which is receipted separately, not the mapping lane.
KEY_COLUMNS = {
    "pfr_id", "nfl_player_id", "player", "player_name", "season", "year", "year_id",
    "week", "game_id", "boxscore_id", "team", "team_abbr", "team_name_abbr", "opponent",
    "source_player_id", "nflcom_slug", "_player_slug", "subpage_year", "stat_name",
    # composite row key minted by the ancient-bundle builders ('192010170rii_CHI_5')
    "team_game_key",
    # PFR page-scope and franchise locators. `first_year`/`last_year` ARE registered
    # stats, family `context` -- they bound a player's career on an index page rather than
    # measuring anything about a game, which is the same locator argument `season` gets.
    "first_year", "last_year", "team_id", "team_name", "team_code", "team_fid",
    "franchise_id", "opponent_code", "opponent_fid", "opponent_franchise_id",
    "pfr_opponent_code", "team_game_num",
    # PFR's own competition marker, measured constant 'NFL' on our rows. Under the settled
    # 78-code NFL-lineage boundary the subject IS this competition, so the column locates a
    # row in the league dimension exactly as `season` locates it in time.
    "comp_name_abbr",
}
# Link/markup residue the HTML parsers emit alongside a real column. Always paired with a
# real column of the same stem, so they carry no independent stat content.
MARKUP_SUFFIXES = ("_links_json", "_link_texts", "_link_ids", "_urls", "_href", "_hrefs")


def _posix(path: str) -> str:
    return path.replace("\\", "/")


def _scan_candidates(path: str) -> list[str]:
    """DuckDB scan expressions to try, cheapest first.

    Deliberately does NOT pre-check existence with a recursive glob: several source roots
    hold hundreds of thousands of files (a PFA shard dir carries ~9,900 retained
    `.html.gz`, `raw/newspaper_archives` far more), and `Path.glob('**/*.parquet')` walks
    every one of them to answer a yes/no question. Letting DuckDB resolve the pattern and
    reporting failure is both faster and more honest -- the scan is the thing that has to
    work anyway.
    """
    if os.path.isfile(path):
        return [f"read_parquet('{_posix(path)}')"]
    if os.path.isdir(path):
        root = _posix(path)
        # Known layouts BEFORE the recursive wildcard. An ff_assets capture dir holds
        # `shards/shard-N/records.parquet` next to `shards/shard-N/raw/` with ~9,900
        # retained pages per shard, so `**/*.parquet` makes DuckDB enumerate ~150,000
        # irrelevant files to find 20 relevant ones.
        # BOUNDED DEPTHS ONLY -- there is deliberately no `**/*.parquet` here.
        # `newspaper_raw_archives` is an archive-of root holding a very large page tree,
        # and the recursive wildcard makes DuckDB enumerate all of it to decide whether
        # any parquet exists. A source whose columnar surface is not at one of these
        # depths is reported UNREADABLE and counted in
        # `column_dossier_unreadable_sources`, which is the honest answer: we did not
        # enumerate its columns, and that must be a number rather than a silent zero.
        return [
            f"read_parquet('{root}/*.parquet', union_by_name=true)",
            f"read_parquet('{root}/shards/shard-*/records.parquet', union_by_name=true)",
            f"read_parquet('{root}/*/shards/shard-*/records.parquet', union_by_name=true)",
            f"read_parquet('{root}/*/*.parquet', union_by_name=true)",
            f"read_parquet('{root}/*/*/*.parquet', union_by_name=true)",
        ]
    return []


def _resolve_scan(connection: duckdb.DuckDBPyConnection, path: str) -> tuple[str, list[str]] | None:
    for scan in _scan_candidates(path):
        try:
            columns = _columns(connection, scan)
        except Exception:
            continue
        if columns:
            return scan, columns
    return None


def _columns(connection: duckdb.DuckDBPyConnection, scan: str) -> list[str]:
    return [row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()]


def detect_regime(connection: duckdb.DuckDBPyConnection, scan: str, columns: list[str]) -> dict:
    """MEASURE the regime. Presence of a discriminator is not evidence; its cardinality is."""
    lowered = {c.lower(): c for c in columns}
    for candidate in LONG_VALUE_COLUMNS:
        if candidate in lowered:
            column = lowered[candidate]
            distinct = connection.execute(
                f'SELECT COUNT(DISTINCT "{column}") FROM {scan}'
            ).fetchone()[0]
            if distinct > 1:
                return {"regime": "LONG", "discriminator": column, "distinct_values": distinct}
    qualifying: list[str] = []
    single_valued: list[str] = []
    for candidate in DISCRIMINATOR_CANDIDATES:
        if candidate not in lowered:
            continue
        column = lowered[candidate]
        distinct = connection.execute(
            f'SELECT COUNT(DISTINCT "{column}") FROM {scan}'
        ).fetchone()[0]
        if distinct > MAX_DISCRIMINATOR_CARDINALITY:
            continue  # an identity column wearing a discriminator's namespace
        if distinct > 1:
            qualifying.append(column)
        else:
            single_valued.append(column)
    if qualifying:
        # Secondary discriminators join the key only now that a primary one has proved
        # this source really does partition into tables.
        for candidate in SECONDARY_DISCRIMINATORS:
            if candidate not in lowered:
                continue
            column = lowered[candidate]
            distinct = connection.execute(
                f'SELECT COUNT(DISTINCT "{column}") FROM {scan}'
            ).fetchone()[0]
            if 1 < distinct <= MAX_DISCRIMINATOR_CARDINALITY:
                qualifying.append(column)
        return {
            "regime": "MULTI_TABLE",
            "discriminator": qualifying,
            "single_valued_candidates": single_valued,
        }
    # No qualifying discriminator: genuinely one logical table per row. Now MEASURED
    # rather than asserted -- the "55 PFR rows = 55 tables" claim lives or dies here.
    return {
        "regime": "WIDE",
        "discriminator": None,
        "single_valued_candidates": single_valued,
    }


def _table_key_expression(discriminators: list[str]) -> str:
    """The SQL that reproduces a MULTI_TABLE row's `table_key`. Shared with
    `_table_keys` so the occupancy measurement and the key enumeration cannot drift into
    two different spellings of the same key -- if they did, every pair would look
    unpublished and the queue would empty itself."""
    return " || '|' || ".join(
        f"COALESCE(CAST(\"{column}\" AS VARCHAR), '')" for column in discriminators
    )


def measure_occupancy(
    connection: duckdb.DuckDBPyConnection, scan: str, discriminators: list[str],
    columns: list[str],
) -> dict[str, set[str]]:
    """Which columns does each logical table ACTUALLY carry values in?

    One GROUP BY, one COUNT per column. `COUNT(x)` counts non-NULLs, so a header cell
    that was published but always blank still counts (it is stored as an empty string,
    not as NULL) -- the measurement errs toward keeping a column, which is the direction
    that keeps material visible.
    """
    key_expression = _table_key_expression(discriminators)
    counts = ", ".join(f'COUNT("{column}")' for column in columns)
    occupancy: dict[str, set[str]] = {}
    for row in connection.execute(
        f"SELECT {key_expression} AS table_key, {counts} FROM {scan} GROUP BY 1"
    ).fetchall():
        occupancy[row[0]] = {
            columns[index] for index, value in enumerate(row[1:]) if value
        }
    return occupancy


def _family_signature_keys(source_key: str) -> tuple[dict[str, set[str]], set[str]]:
    """Per (caption, layout) the columns the SOURCE ITSELF declares in its header, plus
    the family's non-header columns.

    For the logs families the dossier key is `caption|layout`, and `layout` is not a
    column in the data -- so occupancy cannot be measured at that grain. The source's own
    header IS the declaration, and O.9.0 receipted it by regeneration, so it is the
    better evidence anyway.
    """
    if not NFLCOM_SEMANTICS.exists():
        return {}, set()
    document = json.loads(NFLCOM_SEMANTICS.read_text(encoding="utf-8"))
    family = source_key.replace("nflcom_", "")
    signatures = (document.get("families", {}).get(family) or {}).get("signatures") or []
    per_pair: dict[str, set[str]] = {}
    every_header_key: set[str] = set()
    for entry in signatures:
        keys = {str(key).lower() for key in (entry.get("keys") or [])}
        every_header_key |= keys
        if entry.get("layout") and entry.get("caption"):
            per_pair.setdefault(f"{entry['caption']}|{entry['layout']}", set()).update(keys)
    return per_pair, every_header_key


def row_key(source: str, table_key: str, column: str) -> str:
    """The dossier's stable identity for one row. Adjudications reference this, so it must
    not drift: a changed key silently orphans every decision that pointed at it, which is
    why orphans are counted rather than ignored."""
    return f"{source}|{table_key}|{column}"


def load_decisions(path: Path | None = None) -> dict[str, dict]:
    ledger_path = path or DISPOSITIONS_PATH
    if not ledger_path.exists():
        return {}
    document = json.loads(ledger_path.read_text(encoding="utf-8"))
    return {entry["key"]: entry for entry in document.get("decisions", [])}


def validate_decision(entry: dict) -> list[str]:
    """SHAPE ONLY. A decision has to say what it claims and carry something as evidence.

    This is deliberately not the whole gate -- see `validate_decisions_against_material`.
    Shape validation cannot tell whether a claim is TRUE, only whether it was written
    down, and a gate that stops here reports zero for a ledger full of mappings to
    nowhere.
    """
    problems = []
    disposition = entry.get("disposition")
    if disposition not in TERMINAL_DISPOSITIONS:
        problems.append(f"{entry.get('key')}: disposition {disposition!r} not terminal")
    if not entry.get("reason"):
        problems.append(f"{entry.get('key')}: no reason")
    evidence = (entry.get("evidence") or "").strip()
    if not evidence:
        problems.append(f"{entry.get('key')}: no evidence -- an adjudication without "
                        "evidence is an opinion")
    elif len(evidence) < 20:
        # "an adjudication without evidence is an opinion" -- and a one-token evidence
        # string is an opinion that cleared a non-empty check. Every real receipt in
        # this ledger names an artifact and a locator; the shortest is 60 characters.
        problems.append(f"{entry.get('key')}: evidence {evidence!r} is too short to name "
                        "an artifact -- cite the receipt, do not satisfy the field")
    if disposition in {"MAPPED_TO_CANONICAL", "DUPLICATE_OF"} and not entry.get("canonical"):
        problems.append(f"{entry.get('key')}: {disposition} must name `canonical`")
    problems += _validate_audit_capacity(entry)
    return problems


#: `not a supertable column` and `cannot audit a supertable column` are DIFFERENT CLAIMS and
#: the disposition only ever answered the first. `avg` is rightly excluded from the schema --
#: a derived ratio whose operands we already store -- and wrongly excluded from the audit:
#: `avg == def_interception_yards / def_interceptions` holds on 99.98% of 15,251 player-
#: seasons and checks BOTH operands at once. Roughly 2,134 rows carry an exclusion reason and
#: an unknown number of them can still audit us.
#:
#: This is an ORTHOGONAL field, not a new disposition value. Making it a fifth disposition
#: would have forced every audit-capable column out of EXCLUDED_WITH_REASON and silently
#: moved counters that mean something else.
AUDIT_CAPACITY_KINDS = {
    "EQUATION": "an identity among published columns that checks the canonicals it names",
    "FORMULA": "a NAMED formula from a closed registry (audit_capacity_runner."
               "NAMED_FORMULAS) that recomputes the column from several of our canonicals "
               "at once -- passer_rating consumes five. Separate from EQUATION because the "
               "ratio parser takes only `a / b` and must keep refusing anything else",
    "AGGREGATE": "a coarser-grain value that checks a rollup of the canonicals it names",
    "KEY": "locates the row; its obligation is a crosswalk/canonicalization lane",
    "NONE": "argued to carry no audit capacity at all",
}
AUDIT_CAPACITY_STATUSES = {
    "RECEIPTED": "measured, with a crossed control -- may be relied on",
    "DECLARED": "claimed, NOT yet measured; a queue, never a pass",
    "BLOCKED": "the route exists and the TARGET does not; `blocker` must say why",
}


def _validate_audit_capacity(entry: dict) -> list[str]:
    """A capacity claim without a measurement is the same kind of opinion an adjudication
    without evidence is -- so RECEIPTED demands both a receipt and the control that ruled
    out the alternative reading."""
    cap = entry.get("audit_capacity")
    if cap is None:
        return []
    key, out = entry.get("key"), []
    if not isinstance(cap, dict):
        return [f"{key}: audit_capacity must be an object"]
    kind, status = cap.get("kind"), cap.get("status")
    if kind not in AUDIT_CAPACITY_KINDS:
        out.append(f"{key}: audit_capacity.kind {kind!r} not in vocabulary")
    if status not in AUDIT_CAPACITY_STATUSES:
        out.append(f"{key}: audit_capacity.status {status!r} not in vocabulary")
    if kind in {"EQUATION", "FORMULA", "AGGREGATE"}:
        audits = cap.get("audits")
        if not audits or not isinstance(audits, list):
            out.append(f"{key}: audit_capacity of kind {kind} must name what it `audits`")
        else:
            universe = canonical_universe()
            for name in audits:
                if name not in universe:
                    out.append(f"{key}: audit_capacity.audits names {name!r}, "
                               f"which is not a canonical")
        if not cap.get("expression"):
            out.append(f"{key}: audit_capacity of kind {kind} must give its `expression`")
    if status == "RECEIPTED":
        if not (cap.get("receipt") or "").strip():
            out.append(f"{key}: RECEIPTED audit_capacity must carry its measurement")
        if not (cap.get("crossed_control") or "").strip():
            out.append(f"{key}: RECEIPTED audit_capacity must carry the crossed control "
                       "that ruled out the alternative reading")
    if status == "BLOCKED" and not (cap.get("blocker") or "").strip():
        out.append(f"{key}: BLOCKED audit_capacity must say what it is blocked on")
    return out


@functools.lru_cache(maxsize=1)
def canonical_universe() -> frozenset[str]:
    """Every name a MAPPED_TO_CANONICAL decision is allowed to point at.

    NOT one file. The first version of this check asked only whether the name was a
    column of the weekly v26 release, and 42 correct `player_bio` mappings failed it --
    height, college, draft_round, birth_date are BIO-grain canonicals that the weekly
    release has no business carrying. The canonical space is the CONTRACT REGISTRY plus
    the physical tables that realise it.
    """
    contracts_path = DISPOSITIONS_PATH.parent / "stat_contracts.v1.json"
    names: set[str] = set()
    if contracts_path.exists():
        registry = json.loads(contracts_path.read_text(encoding="utf-8"))
        for contract in registry.get("stats", []):
            names.add(contract["canonical_name"])
            names.add(contract["stat_id"])
    from .sources import DATA_LAKE, latest_v26

    connection = duckdb.connect()
    try:
        for target in (latest_v26(),
                       os.path.join(DATA_LAKE, "raw", "pfr", "boxscores",
                                    "nfl_team_games_all.parquet")):
            if not os.path.isfile(target):
                continue
            names.update(
                row[0] for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{_posix(target)}')").fetchall())
    except Exception:  # a tree without the lake still gets the contract-registry check
        pass
    finally:
        connection.close()
    return frozenset(names)


def validate_decisions_against_material(
    rows: list[dict], decisions: dict[str, dict],
) -> list[str]:
    """THE HALF THAT CHECKS WHETHER THE CLAIM IS TRUE.

    Added 2026-07-28 after probing the gate that was reading zero. Every one of these
    passed `validate_decision` and would have been counted as a clean ledger:

        MAPPED_TO_CANONICAL -> 'there_is_no_such_column'      a mapping to nowhere
        DUPLICATE_OF        -> 'not|a|row'                    a duplicate of nothing
        a QUARANTINED family adjudicated                      a laundered defect

    Tests covering all three already existed. They are not the gate: they `pytest.skip`
    when the dossier artifact is absent, so in a fresh tree the tests skip and the
    counter still reads zero. The counter is what always runs, so the counter has to be
    the thing that knows.
    """
    problems: list[str] = []
    keys = {row_key(row["source"], row["table_key"], row["column"]) for row in rows}
    universe = canonical_universe()
    for key, entry in sorted(decisions.items()):
        disposition = entry.get("disposition")
        canonical = entry.get("canonical")
        if disposition == "MAPPED_TO_CANONICAL" and universe and canonical not in universe:
            problems.append(
                f"{key}: canonical {canonical!r} is in no stat contract and in no "
                "physical table -- a mapping to nowhere")
        if disposition == "DUPLICATE_OF" and canonical not in keys:
            problems.append(
                f"{key}: DUPLICATE_OF names {canonical!r}, which is not a dossier row. "
                "A duplicate must point at the row it duplicates, not at a canonical "
                "column name")
        if key.split("|", 1)[0] in QUARANTINED_SOURCES:
            problems.append(
                f"{key}: adjudicated a QUARANTINED source. Its stored column names are "
                "known-wrong (the O.9.0 COLUMN-SHIFT defect), so this decision would "
                "outlive the repair that invalidates it")
    return problems


def apply_decisions(rows: list[dict], decisions: dict[str, dict]) -> tuple[int, list[str]]:
    """Overlay the ledger onto a freshly built dossier.

    An ADJUDICATION BEATS A BULK RULE. The rules are a triage convenience; a human looking
    at the column and its evidence is the authority, and a rule that could override one
    would make the ledger advisory.
    """
    applied = 0
    seen: set[str] = set()
    for row in rows:
        key = row_key(row["source"], row["table_key"], row["column"])
        seen.add(key)
        entry = decisions.get(key)
        if entry is None:
            row["closed_by"] = "rule" if row["disposition"] != "OPEN" else None
            continue
        row["disposition"] = entry["disposition"]
        row["reason"] = entry.get("reason")
        row["evidence"] = entry.get("evidence")
        row["canonical"] = entry.get("canonical")
        row["closed_by"] = "adjudication"
        applied += 1
    # A decision pointing at a key the dossier no longer produces is STALE -- the column
    # was renamed, the source re-registered, or the keying regime changed. Dropping it
    # silently would quietly re-open an adjudicated row with nobody noticing.
    orphaned = sorted(set(decisions) - seen)
    return applied, orphaned


def bulk_disposition(column: str) -> tuple[str, str] | None:
    """Cohorts closable BY RULE. Everything else stays OPEN and is counted."""
    lowered = column.lower()
    if lowered in NOT_PLUMBING_LOOKS_LIKE_IT:
        return None
    if lowered in PROVENANCE_COLUMNS or lowered.endswith(PROVENANCE_SUFFIXES):
        return "EXCLUDED_CAPTURE_PROVENANCE", "records how the row was obtained, not a stat"
    if lowered in KEY_COLUMNS:
        return "EXCLUDED_KEY_COLUMN", "locates the row; its obligation is the crosswalk lane"
    if any(lowered.endswith(suffix) for suffix in MARKUP_SUFFIXES):
        return "EXCLUDED_MARKUP_RESIDUE", "HTML link residue paired with a real column"
    return None


def _table_keys(
    connection: duckdb.DuckDBPyConnection, scan: str, regime: dict, source_key: str
) -> list[str]:
    if regime["regime"] != "MULTI_TABLE":
        return ["*"]
    columns = regime["discriminator"]
    # DISTINCT over the composite, not the cross product: team_stats has 10 categories
    # and 3 sides but only the combinations NFL.com actually publishes, and inventing the
    # missing ones would inflate the denominator with keys that do not exist.
    selected = _table_key_expression(columns)
    values = [
        "|".join(str(part) for part in row)
        for row in connection.execute(
            f"SELECT DISTINCT {selected} FROM {scan} ORDER BY ALL"
        ).fetchall()
    ]
    if source_key in NEEDS_LAYOUT:
        # O.9.0: `_table` here under-specifies the table, and a caption can span SEVEN
        # layouts. Without + layout, `yds` under one key is passing AND rushing AND
        # receiving AND interception-return AND punt yards at once.
        pairs = _caption_layout_keys(source_key)
        if pairs:
            # A caption the census never saw would VANISH if we returned the pairs
            # alone -- `player_career` carries 48 rows under an empty caption that no
            # signature covers, and silently dropping them is the mirror image of the
            # cross-product inflation: material that stops being counted at all. Those
            # captions are kept under their own bare key and counted.
            censused = {pair.split("|", 1)[0] for pair in pairs}
            return pairs + [value for value in values if value not in censused]
        # No layout census for this family -- O.9.0 censused six, and
        # `player_logs_targeted` was not one of them. Borrowing `player_logs`' layouts
        # because the families share a bloodline would be asserting a shape rather than
        # measuring it, so the key stays under-specified and is COUNTED as such.
    return values


def _declaration_basis(source_key: str, regime: dict) -> str:
    if regime["regime"] != "MULTI_TABLE":
        return "SINGLE_TABLE"  # one logical table, so every physical column is its own
    if source_key in NEEDS_LAYOUT and _family_signature_keys(source_key)[0]:
        return "SOURCE_HEADER + MEASURED"
    return "MEASURED"


def _published_columns(
    connection: duckdb.DuckDBPyConnection, scan: str, regime: dict, source_key: str,
    columns: list[str], table_keys: list[str],
) -> dict[str, list[str]]:
    """WHICH COLUMNS DOES EACH LOGICAL TABLE ACTUALLY PUBLISH?

    THE DEFECT THIS FIXES (measured 2026-07-27). The dossier used to emit the full CROSS
    PRODUCT of every physical column against every logical table. But a MULTI_TABLE
    source's physical column list is the UNION over its tables -- `nflcom_team_stats`
    carries 63 columns across 32 tables and no single table has more than about twenty of
    them. Measured: 2,016 emitted rows against 486 (table, column) pairs that carry any
    value at all; `nflcom_player_season` 1,496 against 312; the new
    `statscrew_team_season_stats` 737 against 260. Roughly 3,000 of the 10,273 "open
    columns" were cells that do not exist -- a `rating` row filed under the PUNTING
    table, a `qbh` row filed under KICKING.

    That is the same defect the SECONDARY_DISCRIMINATORS comment above describes, in a
    second place: an INFLATED denominator, which fails in the flattering direction
    because it makes the queue look bigger and therefore more diligent, while every hour
    spent adjudicating a phantom pair is an hour not spent on material. A queue nobody
    can burn down is as useless as a counter that reads zero.

    THE DECLARATION, per regime:

      MEASURED             occupancy from the data. `COUNT(col)` over a GROUP BY of the
                           discriminator. A pair with zero non-NULLs across the entire
                           source publishes nothing -- the harvester writes
                           `dict(zip(header_keys, cells))`, so a column absent from a
                           page's header is never written for that table at all.
      SOURCE_HEADER        for the logs families the key is `caption|layout` and `layout`
      + MEASURED           is not a column, so occupancy cannot be measured at that
                           grain. O.9.0's per-signature `keys` ARE the source's own
                           header and were receipted by regeneration, so they are the
                           better evidence; UNION the family's genuinely-non-header
                           columns (`fumbles`, `nflcom_slug`, ...) measured within the
                           caption, so a real column that no signature lists is kept
                           rather than dropped.

    Both directions of error are deliberate: where the two signals disagree the UNION
    wins, because keeping a column costs one adjudication and dropping one hides
    material.
    """
    if regime["regime"] != "MULTI_TABLE":
        return {table_key: list(columns) for table_key in table_keys}

    occupancy = measure_occupancy(connection, scan, regime["discriminator"], columns)
    per_pair, header_keys = _family_signature_keys(source_key)
    lowered = {column.lower(): column for column in columns}

    published: dict[str, list[str]] = {}
    for table_key in table_keys:
        if source_key in NEEDS_LAYOUT and table_key in per_pair:
            caption = table_key.split("|", 1)[0]
            declared = {
                lowered[name] for name in per_pair[table_key] if name in lowered
            }
            # non-header columns the harvester writes alongside the table, kept only
            # where they carry values under THIS caption
            declared |= {
                column for column in occupancy.get(caption, set())
                if column.lower() not in header_keys
            }
        else:
            # Either a WIDE-keyed MULTI_TABLE source, or a caption the layout census
            # never saw. Measurement is the only declaration available; applying the
            # header filter here would strip every real stat column, because none of
            # THIS table's headers are in the map.
            declared = set(occupancy.get(table_key, set()))
        published[table_key] = [column for column in columns if column in declared]
    return published


def _caption_layout_keys(source_key: str) -> list[str]:
    """The (caption, layout) pairs O.9.0 actually OBSERVED for a logs family.

    Not a cross product of captions and layouts: the signature inventory records which
    layouts appeared under which caption, and pairing every caption with every layout
    would invent keys the source never published -- the same denominator inflation this
    module refuses everywhere else.
    """
    if not NFLCOM_SEMANTICS.exists():
        return []
    document = json.loads(NFLCOM_SEMANTICS.read_text(encoding="utf-8"))
    family = source_key.replace("nflcom_", "")
    signatures = (document.get("families", {}).get(family) or {}).get("signatures") or []
    pairs = {
        f"{entry.get('caption')}|{entry['layout']}"
        for entry in signatures
        if entry.get("layout") and entry.get("caption")
    }
    return sorted(pairs)


def build(only_source: str | None = None, limit: int | None = None) -> dict:
    sources = registry(include_subject=True)
    keys = sorted(sources)
    if only_source:
        keys = [k for k in keys if k == only_source]
    if limit:
        keys = keys[:limit]

    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    rows: list[dict] = []
    per_source: list[dict] = []
    unreadable: list[str] = []

    for index, key in enumerate(keys):
        source = sources[key]
        print(f"  [{index + 1}/{len(keys)}] {key}", flush=True)
        resolved = _resolve_scan(connection, source.path)
        if resolved is None:
            unreadable.append(key)
            continue
        scan, columns = resolved
        try:
            regime = detect_regime(connection, scan, columns)
            table_keys = _table_keys(connection, scan, regime, key)
        except Exception as exc:  # a source we cannot read is REPORTED, never skipped
            unreadable.append(f"{key}: {type(exc).__name__}")
            continue

        if regime["regime"] == "LONG":
            # The stat surface is row VALUES, so the dossier enumerates those, not the
            # physical columns -- keyed (source, stat_name).
            discriminator = regime["discriminator"]
            names = [
                str(row[0])
                for row in connection.execute(
                    f'SELECT DISTINCT "{discriminator}" FROM {scan} '
                    f'WHERE "{discriminator}" IS NOT NULL ORDER BY 1'
                ).fetchall()
            ]
            for name in names:
                rows.append(
                    {
                        "source": key,
                        "lineage": source.lineage,
                        "regime": "LONG",
                        "table_key": "stat_name",
                        "column": name,
                        "disposition": "OPEN",
                        "reason": None,
                    }
                )
            # A LONG source ALSO has physical columns -- value, unit, confidence, the
            # reviewer fields. Enumerating only the stat concepts would repeat the
            # original error in mirror image: the concepts are what a column-keyed
            # dossier misses, but the columns are still real and still need a
            # disposition. Both are enumerated, distinguished by table_key.
            for column in columns:
                bulk = bulk_disposition(column)
                rows.append(
                    {
                        "source": key,
                        "lineage": source.lineage,
                        "regime": "LONG",
                        "table_key": "physical",
                        "column": column,
                        "disposition": bulk[0] if bulk else "OPEN",
                        "reason": bulk[1] if bulk else None,
                    }
                )
            per_source.append(
                {"source": key, "regime": "LONG", "table_keys": 2,
                 "dossier_rows": len(names) + len(columns),
                 "stat_concepts": len(names), "physical_columns": len(columns),
                 # a LONG source enumerates concepts and physical columns, never a
                 # cross product of the two -- so nothing is unpublished by construction
                 "cross_product_pairs": len(names) + len(columns),
                 "unpublished_pairs": 0,
                 "column_declaration": "LONG_VALUE_SURFACE",
                 "discriminator": discriminator}
            )
            continue

        published = _published_columns(connection, scan, regime, key, columns, table_keys)
        # The source's own partition labels. `_category`, `_side` and `season_type` are
        # the very columns whose values the dossier used to BUILD this table key, so
        # `_category` under table key `rushing|offense|reg` reads 'rushing' on every row
        # -- the table's identity restated as a cell. Closing them is not a name rule
        # (which could reach a stat); it is defined relative to the discriminator this
        # source was MEASURED to partition on, so it can only ever reach the partition.
        discriminators = {
            name.lower() for name in (regime.get("discriminator") or [])
        } if regime["regime"] == "MULTI_TABLE" else set()
        emitted = 0
        for table_key in table_keys:
            for column in published[table_key]:
                bulk = bulk_disposition(column)
                if bulk is None and column.lower() in discriminators:
                    bulk = ("EXCLUDED_TABLE_DISCRIMINATOR",
                            "this source's own partition label -- the column whose value "
                            "IS this dossier row's table key, restated as a cell")
                rows.append(
                    {
                        "source": key,
                        "lineage": source.lineage,
                        "regime": regime["regime"],
                        "table_key": table_key,
                        "column": column,
                        "disposition": bulk[0] if bulk else "OPEN",
                        "reason": bulk[1] if bulk else None,
                    }
                )
                emitted += 1
        cross_product = len(table_keys) * len(columns)
        per_source.append(
            {"source": key, "regime": regime["regime"], "table_keys": len(table_keys),
             "dossier_rows": emitted, "physical_columns": len(columns),
             # CONSERVATION: cross_product = dossier_rows + unpublished_pairs, always.
             # The naive denominator is kept beside the real one so the shrink is
             # auditable from the artifact without re-measuring anything.
             "cross_product_pairs": cross_product,
             "unpublished_pairs": cross_product - emitted,
             "column_declaration": _declaration_basis(key, regime),
             "discriminator": regime.get("discriminator"),
             # captions the source publishes that O.9.0's layout census never saw. They
             # keep their material (keyed on the bare caption, declared by measurement)
             # but they are UNDER-SPECIFIED in exactly the way the layout component
             # exists to fix, so they are a number rather than an unremarked fallback.
             "table_keys_without_layout_census": (
                 sum(1 for k in table_keys if "|" not in k)
                 if key in NEEDS_LAYOUT and any("|" in k for k in table_keys) else 0),
             "layout_census": (
                 None if key not in NEEDS_LAYOUT
                 else ("PRESENT" if any("|" in k for k in table_keys) else "MISSING")
             )}
        )

    connection.close()

    decisions = load_decisions()
    # BOTH HALVES. Shape says the decision was written down; material says it is
    # true. A gate that ran only the first read zero for a ledger that could map to
    # nowhere, duplicate nothing, and adjudicate a quarantined family.
    invalid = [problem for entry in decisions.values()
               for problem in validate_decision(entry)]
    invalid += validate_decisions_against_material(rows, decisions)
    adjudicated, orphaned = apply_decisions(rows, decisions)

    dispositions = Counter(row["disposition"] for row in rows)
    regimes = Counter(entry["regime"] for entry in per_source)
    open_by_lineage = Counter(
        row["lineage"] for row in rows if row["disposition"] == "OPEN"
    )
    return {
        "law": "one row per (source, table, column) -- or (source, stat_name) for LONG "
               "sources -- each ending in a terminal disposition. Sources are the "
               "paperwork; columns are the material",
        "counters": {
            "sources_enumerated": len(per_source),
            "sources_unreadable": len(unreadable),
            "dossier_rows": len(rows),
            "column_dossier_open_rows": dispositions.get("OPEN", 0),
            "closed_by_rule": sum(1 for row in rows if row.get("closed_by") == "rule"),
            "closed_by_adjudication": adjudicated,
            # A malformed decision is a ZERO-GATE, not a queue: it is a claim about
            # material that failed to say what it claims or why.
            "column_dispositions_invalid": len(invalid),
            # AUDIT CAPACITY IS A SECOND QUESTION. `disposition` says whether a column is
            # ours; this says whether it can CHECK us, and the two have different answers.
            # These count CLAIMS. Whether a claim has been MEASURED is the equation lane's
            # counters (audit_capacity_runner), never the typed status -- naming a claim
            # counter "unmeasured" made two counters imply the same thing and disagree,
            # which is the hand-typed-status drift in miniature.
            "audit_capacity_claims_declared": sum(
                1 for e in decisions.values()
                if (e.get("audit_capacity") or {}).get("status") == "DECLARED"),
            "audit_capacity_blocked_on_a_missing_target": sum(
                1 for e in decisions.values()
                if (e.get("audit_capacity") or {}).get("status") == "BLOCKED"),
            "audit_capacity_receipted": sum(
                1 for e in decisions.values()
                if (e.get("audit_capacity") or {}).get("status") == "RECEIPTED"),
            # A decision whose key the dossier no longer produces. Silently dropping it
            # re-opens an adjudicated row with nobody noticing.
            "column_dispositions_orphaned": len(orphaned),
            # A family that NEEDS a layout component but has no layout census keys its
            # columns too coarsely, so several distinct stats share one dossier row. That
            # is an under-count of the material and must be visible as a number.
            "sources_needing_layout_without_census": sum(
                1 for entry in per_source if entry.get("layout_census") == "MISSING"),
            # THE NAIVE DENOMINATOR, kept next to the real one. Emitting the cross
            # product of columns x tables counted ~3,000 (table, column) pairs that no
            # table publishes -- `rating` under PUNTING, `qbh` under KICKING. It failed
            # in the flattering direction: a bigger queue reads as more diligence while
            # every phantom row is an hour stolen from material. Reported, never
            # silently dropped, so the shrink can be audited from the artifact.
            "cross_product_pairs": sum(
                entry.get("cross_product_pairs", entry["dossier_rows"])
                for entry in per_source),
            "column_dossier_unpublished_pairs": sum(
                entry.get("unpublished_pairs", 0) for entry in per_source),
            "table_keys_without_layout_census": sum(
                entry.get("table_keys_without_layout_census", 0) for entry in per_source),
            # OPEN because nobody may decide them, not because nobody has looked. Rolled
            # into one open-row count these read as un-worked queue; they are blocked on
            # the un-shift re-parse and no amount of adjudication effort moves them.
            "column_dossier_rows_blocked_by_quarantine": sum(
                1 for row in rows
                if row["source"] in QUARANTINED_SOURCES and row["disposition"] == "OPEN"),
        },
        "quarantined_sources_in_dossier": sorted(
            {row["source"] for row in rows if row["source"] in QUARANTINED_SOURCES}),
        "unpublished_pairs_by_source": {
            entry["source"]: entry["unpublished_pairs"]
            for entry in sorted(per_source, key=lambda e: -e.get("unpublished_pairs", 0))
            if entry.get("unpublished_pairs")
        },
        "sources_needing_layout_without_census": sorted(
            entry["source"] for entry in per_source
            if entry.get("layout_census") == "MISSING"),
        "regimes": dict(regimes),
        "dispositions": dict(dispositions.most_common()),
        "open_rows_by_lineage": dict(open_by_lineage.most_common()),
        "unreadable_sources": unreadable,
        "column_dispositions_invalid": invalid,
        "column_dispositions_orphaned": orphaned,
        "per_source": sorted(per_source, key=lambda entry: -entry["dossier_rows"]),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--force", action="store_true",
                        help="overwrite even a newer artifact")
    args = parser.parse_args()
    document = build(only_source=args.source, limit=args.limit)
    from .recon_common import utc_stamp

    document["generated_utc"] = utc_stamp()
    # NEVER let a slower run overwrite a newer artifact.
    #
    # Two builds of this module ran concurrently while its scan strategy was being fixed;
    # the older, slower one finished last and silently reverted the committed receipt to a
    # version that collapsed 54 nflcom logical tables -- 10,273 open rows back down to
    # 6,027. It failed in the flattering direction, which is the direction nobody
    # double-checks, and the only reason it would have surfaced is that a test happens to
    # read a counter the old shape lacks. A committed receipt should not depend on that.
    if OUT_PATH.exists() and not args.force:
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8")).get("generated_utc")
        except (json.JSONDecodeError, OSError):
            existing = None
        if existing and existing > document["generated_utc"]:
            raise SystemExit(
                f"refusing to overwrite a NEWER dossier ({existing} > "
                f"{document['generated_utc']}): another build finished after this one "
                "started. Re-run alone, or pass --force if this run is authoritative."
            )
    OUT_PATH.write_text(json.dumps(document, indent=2), encoding="utf-8")
    counters = document["counters"]
    print(f"sources enumerated : {counters['sources_enumerated']}  "
          f"(unreadable {counters['sources_unreadable']})")
    print(f"regimes            : {document['regimes']}")
    print(f"dossier rows       : {counters['dossier_rows']:,}")
    print(f"closed by rule     : {counters['closed_by_rule']:,}")
    print(f"adjudicated        : {counters['closed_by_adjudication']:,}  "
          f"(invalid {counters['column_dispositions_invalid']}, "
          f"orphaned {counters['column_dispositions_orphaned']})")
    print(f"OPEN               : {counters['column_dossier_open_rows']:,}")
    print(f"open by lineage    : {document['open_rows_by_lineage']}")
    if document["unreadable_sources"]:
        print(f"unreadable         : {document['unreadable_sources']}")
    print(f"\ndossier -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
