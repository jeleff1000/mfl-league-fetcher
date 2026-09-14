"""
sota_recon/closure_scoreboard.py  --  THE CLOSURE SCOREBOARD v1 (master plan §0 / §21)

One enforced roll-up of every completeness gate in the program. Rigor was
distributed across lanes and tests; this runner makes it ONE loud answer:

  ZERO-GATES     counters that must be exactly zero (a nonzero fails the suite):
                 disk census undispositioned / datasets outside census / unknown
                 lake top-level dirs / sources without mapping obligation /
                 lineage contract drift / edge-contract drift or invalidity /
                 derivation-DAG partition mismatch / stats without tolerance policy
  DECLARED QUEUES  counters allowed nonzero ONLY as enumerated, receipted queues
                 (burn-down cells, mapping-pending, near-miss drift flags, open
                 questions) -- each must be present and countable, never vague
  META-GATE      every contract file under witness_gate/contracts/ must be
                 ENROLLED here (or explicitly exempt with a reason). A new
                 contract landing unenrolled FAILS -- the scoreboard's own
                 coverage cannot rot silently ("can't leave anything out",
                 applied to the plan itself).

v2 (sequenced): Atom Certificates emitted per (stat x grain x era) from the spine
+ proof ledger; input-hash freshness pinning for the weekly-update era (§24.3).

Output: docs/closure-scoreboard.json

Run:  python -m scripts.sota_recon.closure_scoreboard
"""

from __future__ import annotations

import glob
import json
import os
from pathlib import Path

HERE = os.path.dirname(__file__)
CONTRACTS_DIR = os.path.join(HERE, "witness_gate", "contracts")
SUMMARY_PATH = os.path.join(HERE, "..", "..", "docs", "closure-scoreboard.json")

# every contract in witness_gate/contracts/ must appear here or in EXEMPT
ENROLLED_CONTRACTS = {
    "stat_contracts.v1.json": "per-stat contract registry (grains, agg class, tolerance law)",
    "lineage_roots.v1.json": "source x family x era root assignments (regen-diff-tested)",
    "composite_witnesses.v1.json": "closed local composite-witness lane contract",
    "kc_planes.v1.json": "K/C join + coverage contracts (regen-diff-tested)",
    "relationship_edges.v1.json": "R1-R9 typed edge contract (regen-diff-tested)",
    "crosswalk_receipts.v1.json": "bio crosswalk bijection receipts (gate for kc ACTIVE)",
    "equations.v1.json": "typed-IR law expressions",
    "atoms.v1.json": "witness atom registry",
    "admissibility.v1.json": "witness admissibility contracts",
    "field_mappings.v1.json": "witness-gate field mappings",
    "pbp_contracts.v1.json": "pbp terminal-state contracts",
    "position_taxonomy.v1.json": "position vocabulary",
    "source_census.v1.json": "witness-gate dataset census (joined to registration census)",
    "column_dispositions.v1.json": "O.9.3 column-dossier adjudication ledger -- decisions "
                                   "live here, not in the regenerated artifact",
    "signoff_ledger.v1.json": "RULE C: the decisions a refusal may legally wait on. "
                              "'Blocked on Joe' lived in prose in three handoffs and "
                              "nothing re-checked it; the NO_SIGNOFF predicate reads this "
                              "file every run",
    "column_demand.v1.json":
        "the evidence pack a NEW_SUPERTABLE_COLUMN_CANDIDATE must carry before it may be "
        "proposed as a column: two independent publishers, measured pairwise agreement, a "
        "crossed control, a bound the values satisfy, and who would supply it. The "
        "disposition claimed candidates route through a demand join for months while no "
        "such lane existed",
    "witness_licence_floors.v1.json":
        "the floor each measured mapping must keep clearing. A licence is a MEASUREMENT and "
        "measurements drift; nothing re-checked one once written, so the NFL.com audit had "
        "no teeth at all -- a regression from 97.7% to 40% moved no counter",
    "disagreement_adjudications.v1.json":
        "WHO is wrong when a witness and the supertable disagree. The validation verdict "
        "had five values and four of them blamed our plumbing, so a disagreement could "
        "never convict the SUPERTABLE and the repair queue could never be populated. "
        "Attribution is recorded here with its crossed control; absent means "
        "UNADJUDICATED, which is a queue and not a pass",
}
EXEMPT_CONTRACTS: dict[str, str] = {}

# ---- THE SAME META-GATE, ONE LAYER DOWN: decisions that live in PYTHON ----
#
# `contracts_unenrolled_in_scoreboard` above counts *.json in ONE DIRECTORY. It read 0 all
# of 2026-07-28 while that session added THREE correspondence tables holding hundreds of
# decisions, because a correspondence table is not a .json file. The counter was measuring
# the artifact TYPE, not the thing the artifact is a container for -- the fifth instance of
# the shape [[feedback-check-the-denominator-beneath]] names, and the one the handoff left
# open as DO NEXT item 2.
#
# `adjudication_registry_census` supplies the denominator: every module-level registry in
# this package (405), each classified ADJUDICATION or LOOKUP by two MECHANICAL rules --
# SINK (the defining module writes into the adjudication ledger or declares a scoreboard
# gate) and PROSE (at least half its string values are reasons rather than tokens). The
# SINK rule is the load-bearing one: a new adjudication generator cannot avoid it, because
# writing decisions means calling `apply_to_ledger`. So a new MAPPED table lands UNENROLLED
# and FAILS this gate until someone says, here, what it decides.
#
# Enrolment is a one-line statement of what the registry decides. An entry in
# EXEMPT_ADJUDICATION_REGISTRIES says the classifier over-reached and why -- over-reach is
# the intended failure direction, so declining costs one line and missing one costs a false
# clean gate.
ENROLLED_ADJUDICATION_REGISTRIES = {
    # -- the scoreboard's own enrolment decisions --------------------------------------
    "closure_scoreboard.py:ENROLLED_CONTRACTS":
        "which contract files this scoreboard claims to gate",
    "closure_scoreboard.py:EXEMPT_CONTRACTS":
        "contract files deliberately not gated, with the reason",
    "closure_scoreboard.py:CAPTURE_CONTRACT_EXEMPT_LINEAGES":
        "lineages with no capture obligation, each carrying its reason (own meta-gate)",
    "closure_scoreboard.py:LINEAGE_CONTRACT_ALIASES":
        "declared one-origin-two-spellings pairs, so a naming mismatch cannot present as "
        "coverage",
    "closure_scoreboard.py:ENROLLED_ADJUDICATION_REGISTRIES":
        "this registry -- the enrolment decisions themselves are decisions",
    "witness_map.py:FAULTS":
        "the fault vocabulary a measured disagreement may be attributed to. Naming "
        "SUPERTABLE_GAP and SUPERTABLE_VALUE is what lets a witness convict US; the prior "
        "vocabulary could only ever blame the mapping",
    "nflcom_column_audit.py:PLANS":
        "how each NFL.com source is read: its physical table axis, its key column, which "
        "segment of the dossier table_key names the table, the partition dimension a split "
        "source must be summed within, and whether its table axis survived capture at all",
    "nflcom_column_audit.py:AGG_FROM_CLASS":
        "the same SUM/MAX-only aggregation the generator allows; anything else is refused "
        "rather than defaulted, so the audit harness cannot aggregate what a spec may not",
    "mapspec_generator.py:TABLE_MAJOR":
        "the sources where (source_key, source_col) is NOT unique to a statistic, and how "
        "to reach the physical table for each: which column holds the table key, which "
        "segment of the dossier table_key names it, and -- for a split source -- which "
        "partition dimension a season value may be summed within. Every field is a "
        "declaration with its measurement, because none of it is inferable from the parquet",
    "mapspec_generator.py:RATE_SHAPED":
        "canonicals that denote a ratio whatever the contract claims. Needed because "
        "stat_contracts declares 22 rate-shaped columns as SUM, one of them (pat_pct) with "
        "unit='ratio' contradicting its own class and another (rushing_yards_per_carry) "
        "with unit='count' so that both its fields are wrong",
    "mapspec_generator.py:EMITTED_FIELDS":
        "which MapSpec fields a GENERATED spec is entitled to set. Everything omitted "
        "(scale, filters, v26_expr, blank_zero, validation_*) is a judgement about the "
        "source the generator has no evidence for, so it stays at its default",
    "closure_scoreboard.py:EXEMPT_ADJUDICATION_REGISTRIES":
        "registries the classifier swept in that are lookups, each with the reason",
    # -- the dossier's own closure rules ------------------------------------------------
    "column_dossier.py:KEY_COLUMNS":
        "columns closed by rule as row LOCATORS (EXCLUDED_KEY_COLUMN)",
    "column_dossier.py:PROVENANCE_COLUMNS":
        "columns closed by rule as capture provenance (EXCLUDED_PROVENANCE)",
    "column_dossier.py:DISCRIMINATOR_CANDIDATES":
        "columns eligible to act as a MULTI_TABLE table discriminator",
    "column_dossier.py:SECONDARY_DISCRIMINATORS":
        "second-axis discriminators for sources whose table key is composite",
    "column_dossier.py:TERMINAL_DISPOSITIONS":
        "the adjudication VOCABULARY -- which dispositions close a row",
    "column_dossier.py:NEEDS_LAYOUT":
        "sources whose table key requires a layout census before rows are adjudicable",
    "column_dossier.py:LONG_VALUE_COLUMNS":
        "columns whose cells are prose/composite rather than a measured value",
    "column_dossier.py:MARKUP_SUFFIXES":
        "parser-emitted markup suffixes recognised as non-material",
    "column_dossier.py:PROVENANCE_SUFFIXES":
        "suffixes (-_run_id, -_at_utc, -_row_keys) that mark a column as OUR pipeline's "
        "bookkeeping; a suffix rather than a name list because every builder invents its "
        "own prefix and an enumeration refills itself",
    "column_dossier.py:QUARANTINED_SOURCES":
        "sources whose stored column NAMES are known-wrong, so their rows may not be "
        "adjudicated at all -- a decision about what may not be decided. It was invisible "
        "to the first version of the registry gate purely because it is built with "
        "`set(...)` instead of `{...}`",
    "witness_gate/build_stat_contracts.py:ADJUDICATION_RULES":
        "the receipted rules that settled the 101 conflicts in the stat-contract registry "
        "(§25.7 polarity/unit adjudications). Named ADJUDICATION_RULES and invisible to the "
        "first version of this gate because it lives one directory down",
    "audit_capacity_runner.py:SOURCE_PLANS":
        "how to read each source the equation lane covers -- parquet glob, table "
        "discriminator, slug column, row filter. The runner was welded to player_career, so "
        "player_season's rates could not be measured at all; the plan is what makes the "
        "lane portable rather than one source's script",
    "audit_capacity_runner.py:NAMED_FORMULAS":
        "the CLOSED registry of named formulas a FORMULA capacity may claim. passer_rating "
        "is fixed by rule and consumes five of our columns at once; keeping the registry "
        "closed is what stops a capacity claim meaning anything it likes, exactly as the "
        "two-operand ratio parser does",
    "audit_capacity_runner.py:TOLERANCE_BY_DECIMALS":
        "half a unit in the last decimal the SOURCE publishes -- derived from the data, not "
        "chosen. A tighter band on a one-decimal table produced 54-58% agreement in the "
        "O.9.0 sweep that read as a label problem and was a tolerance artefact",
    "subject_level_recon.py:SKIP":
        "which aggregation classes the weekly->season->career ladder DOES NOT check, and the "
        "reason per class. This is a decision registry precisely because a skip reason is "
        "load-bearing: the first version said NON_AGGREGATABLE meant 'declared not to "
        "aggregate' -- true, useless, and it buried 287 columns whose season_derivation says "
        "'recompute', i.e. a season value EXISTS and nothing verifies it (rank 160, ppg 60, "
        "lamar 56). A skip that names the lane's choice instead of the column's behaviour "
        "functions as a licence",
    "repair_queue.py:OWNERS":
        "fault -> who owes the labour. A diagnosed repair that nothing counts never "
        "happens: every adjudication carried `remedy`/`remedy_applied` and a grep proved "
        "NOTHING read either field. `40 repairs` is also not one queue -- a SOURCE_DEFECT "
        "is not ours to backfill, and lumping them overstates the debt",
    "repair_queue.py:RECEIPT_CHECKS":
        "a receipt that is prose rots silently; each applied remedy names a PREDICATE that "
        "the gate re-runs, so the backlog cannot be emptied by editing a boolean and a "
        "landed repair cannot quietly un-land",
    "column_demand.py:REQUIRED":
        "the fields a demand record must carry to be EVIDENCED -- publishers, measured "
        "agreement, crossed control, constraint, supply. Absence is not an error; it is the "
        "difference between a candidate with a case and one without",
    "column_dossier.py:AUDIT_CAPACITY_KINDS":
        "the closed vocabulary for the SECOND question a column answers -- not `is this "
        "column ours` (that is `disposition`) but `can it CHECK us`. EQUATION / AGGREGATE / "
        "KEY / NONE",
    "column_dossier.py:AUDIT_CAPACITY_STATUSES":
        "RECEIPTED (measured, with a crossed control) / DECLARED (claimed, unmeasured -- a "
        "queue) / BLOCKED (the route exists, the target does not). A free-text status would "
        "let the queue be emptied by inventing a word",
    "column_dossier.py:NOT_PLUMBING_LOOKS_LIKE_IT":
        "columns that sit among the plumbing and are NOT it -- checked one at a time and "
        "held OPEN for adjudication, so a bulk rule can never close real material",
    # -- the correspondence tables -------------------------------------------------------
    "internal_column_adjudication.py:OWN_VOCABULARY":
        "our own dead/internal vocabulary, decided one name at a time",
    "internal_column_adjudication.py:CANONICAL_ALIASES":
        "closed source-column aliases in our assembled ancient bundles; currently the "
        "settled source_positions -> nfl_position taxonomy ruling for the PBP and PFA "
        "streams not owned by the PFR/newspaper correspondence passes",
    "nflcom_column_adjudication.py:MAPPED":
        "O.9.0 semantic concept -> canonical v26 column, for the RESOLVED nflcom layouts",
    "nflcom_column_adjudication.py:NEW_CANDIDATES":
        "nflcom material v26 does not carry, routed through the §24.4 census + demand join",
    "nflcom_column_adjudication.py:EXCLUDED":
        "nflcom columns excluded on a grain or ownership argument",
    "nflcom_column_adjudication.py:ESCALATED":
        "nflcom label questions refused rather than guessed (tackle total, bare FR)",
    "nflcom_column_adjudication.py:DERIVED_RATIOS":
        "nflcom rates whose operands the same signature publishes",
    "nflcom_column_adjudication.py:DUPLICATE_TABLE_KEYS":
        "team_stats families that are cached copies of offense_passing",
    "nflcom_column_adjudication.py:FAMILIES":
        "which nflcom families that pass is scoped to -- the scope IS a decision",
    "statscrew_column_adjudication.py:MAPPED":
        "(table_tag, column) -> canonical, argued from the site's own <th title=>",
    "statscrew_column_adjudication.py:NEW_CANDIDATES":
        "StatsCrew material v26 does not carry",
    "statscrew_column_adjudication.py:EXCLUDED":
        "StatsCrew columns excluded, including the CFL-concept domain ruling",
    "statscrew_column_adjudication.py:ESCALATED":
        "StatsCrew questions refused: tackle total, whose fumble, the composite game cell",
    "statscrew_column_adjudication.py:DERIVED_RATIOS":
        "StatsCrew rates whose operands the same table publishes",
    "statscrew_column_adjudication.py:SOURCES":
        "which StatsCrew datasets that pass is scoped to",
    "nflcom_splits_column_adjudication.py:BLOCKS":
        "layout suffix -> the stat family NFL.com's own <h3> names, checked against the "
        "block census in both families independently",
    "nflcom_splits_column_adjudication.py:MAPPED":
        "(layout, column) -> canonical for the splits/situational blocks",
    "nflcom_splits_column_adjudication.py:NEW_CANDIDATES":
        "splits material v26 does not carry (punter-side coverage, punts inside the 20)",
    "nflcom_splits_column_adjudication.py:EXCLUDED":
        "splits columns excluded on a grain, locator or provenance argument",
    "nflcom_splits_column_adjudication.py:ESCALATED":
        "the defense-block tackle total, refused for the third time from a third lineage",
    "nflcom_splits_column_adjudication.py:DERIVED_RATIOS":
        "splits rates whose operands the same block publishes, each confirmed by the "
        "arithmetic identity sweep before it may be written",
    "nflcom_splits_column_adjudication.py:_L4_PAIRS":
        "the columns L4 cannot decide, with BOTH canonicals each would take -- the "
        "escalation's own content",
    "nflcom_splits_column_adjudication.py:_BUCKETS":
        "the composite made-att field-goal cells, escalated to the split-at-admission "
        "vocabulary question",
    "nflcom_splits_column_adjudication.py:SOURCES":
        "which splits sources that pass is scoped to",
    "mapspec_generator.py:AGG_FROM_CLASS":
        "aggregation_class -> MapSpec.agg. ONLY SUM and MAX; every other class is refused "
        "rather than defaulted, because summing a stored rate produces a meaningless "
        "number and fg_long (yards/MAX) vs fg_made_distance (count/SUM) cannot be told "
        "apart by reading the name",
    "mapspec_generator.py:NEW_SOURCE_SHAPES":
        "per-source join shape for sources with no hand MapSpec yet, each carrying the "
        "evidence for the call; a wrong shape does not raise, it silently validates "
        "nothing, so an undeclared source is refused instead of defaulted",
    "identity_coverage_gate.py:ID_COLUMNS":
        "which columns hold a PFR-id-space value, in resolution order",
    "identity_coverage_gate.py:ACCEPTED_ABSENCES":
        "ids allowed to be missing from the identity spine, each with a reason. Empty is "
        "the correct state -- an entry is a standing claim that must survive re-reading",
    "capture_width_gate.py:PAGE_ROOTS":
        "retained-page root per source WITH the reason it is the right evidence; pointing "
        "a width check at the wrong directory yields a confident number about wrong pages",
    "nflcom_column_adjudication.py:ALIAS_COLUMNS":
        "long-alias physical columns (`fumbles`) -> the short column the layout census "
        "names (`fum`); licensed by 100.00% value equality against a 37.48% crossed "
        "control, not by matching non-empty counts",
    "nflcom_column_adjudication.py:ALIAS_UNPUBLISHED_LAYOUTS":
        "the four player_logs layouts whose header names no fumble column, with the "
        "occupancy denominator scanned for each; the alias is non-empty on 0 of them",
    "nflcom_column_adjudication.py:BLANK_CAPTION_OCCUPANCY":
        "per-column non-empty counts over the 48 player_career rows whose caption did "
        "not parse -- the measurement that separates the 5 columns carrying material "
        "from the 42 carrying none",
    "nflcom_column_adjudication.py:BLANK_CAPTION_SEMANTICS":
        "the census's own name for each of those 5, so a blank-caption row takes the "
        "same disposition as the identical column under a real caption",
    "legacy_column_adjudication.py:MEASURED":
        "legacy column -> canonical, each carrying the GOLDEN-SAMPLE agreement rate and n "
        "that decided it; four columns named ret* are separable no other way",
    "legacy_column_adjudication.py:DERIVED_CONFIRMED":
        "legacy rates confirmed against their OPERANDS -- including y/c, which reads as "
        "yards per carry and is yards per COMPLETION (99.6% vs 0.2%)",
    "legacy_column_adjudication.py:SCALED":
        "legacy columns matching a canonical only after a scale factor; fg% is 0-100 where "
        "the live table is 0-1",
    "legacy_column_adjudication.py:DEAD":
        "columns MEASURED all-zero over 732,587 rows -- excluded on the measurement, not "
        "on a reading of names like col_39 that have nothing to read",
    "legacy_column_adjudication.py:EXCLUDED":
        "legacy columns excluded on ownership, grain or list-cell shape",
    "legacy_column_adjudication.py:UNRESOLVED":
        "legacy columns the golden sample REFUTED every candidate for -- left OPEN with the "
        "scores that refuted them",
    "pfr_column_adjudication.py:MAPPED":
        "PFR data-stat machine id -> canonical; the pass_/def_ prefix is PFR'S OWN "
        "disambiguator between the passer's and the defender's interception",
    "pfr_column_adjudication.py:NEW_CANDIDATES":
        "pfr material v26 has no weekly column for (games started, experience, conference, "
        "per-target allowed rates, the two-point total)",
    "pfr_column_adjudication.py:EXCLUDED":
        "pfr columns excluded on grain or on OUR-OWN-RANKING ownership",
    "pfr_column_adjudication.py:ESCALATED":
        "the tackle total, the composite award/draft cells, and the LONG-format "
        "stat/home_stat/vis_stat/info columns that need a regime change first",
    "newspaper_column_adjudication.py:MAPPED":
        "1920s-newspaper stat_name -> canonical; synonyms of one measurement share a "
        "target, which is the whole point on a source whose vocabulary is era prose",
    "newspaper_column_adjudication.py:NEW_CANDIDATES":
        "newspaper material v26 has no column for (team first downs, kickoffs taken, "
        "scrimmage plays, minutes played, unrecovered fumbles)",
    "newspaper_column_adjudication.py:_EXPLICIT_EXCLUDE":
        "named newspaper stat_names excluded one at a time, each with its kind",
    "newspaper_column_adjudication.py:_PREFIX_EXCLUDE":
        "prefix rules for the standings / provenance / one-off classes",
    "newspaper_column_adjudication.py:_SUFFIX_EXCLUDE":
        "suffix rules for narrative and provenance cells",
    "newspaper_column_adjudication.py:_SUBSTRING_EXCLUDE":
        "substring rules for extraction artefacts naming one player or afternoon",
    "nflcom_category_column_adjudication.py:MAPPED":
        "(category, side, column) -> canonical for nflcom team_stats + player_season; the "
        "side axis is load-bearing (defense = allowed, except int/sck which are credits)",
    "nflcom_category_column_adjudication.py:NEW_CANDIDATES":
        "nflcom category material v26 does not carry (attempts-against, coverage-side "
        "punting/kickoffs, scrimmage plays, the downs denominators)",
    "nflcom_category_column_adjudication.py:DERIVED_RATIOS":
        "nflcom category rates whose operands the SAME table publishes, checked against the "
        "signature at validation rather than assumed",
    "nflcom_category_column_adjudication.py:ESCALATED":
        "the tackle-total and whose-fumble questions, refused here as in every other pass",
    "nflcom_category_column_adjudication.py:SOURCES":
        "which nflcom families that pass is scoped to",
    "nflcom_category_column_adjudication.py:PARTITION":
        "the harvester's own partition columns, subtracted before joining a dossier table "
        "to its header signature -- the subtraction IS the join, so it is a decision",
    "nflcom_category_column_adjudication.py:_BUCKETS":
        "the composite A-M field-goal distance cells, escalated to the split-at-admission "
        "vocabulary question",
    "newspaper_atom_column_adjudication.py:SOURCES":
        "which newspaper source the sidecar's stat-cell atom map may be applied to -- the "
        "SCOPE is the decision here, because eight team-grain stat_names spell identically "
        "to player-cell atoms and claim a different fact",
    "pbp_column_adjudication.py:GRAIN_COHORTS":
        "the play-grain argument, stated once per cohort and fanned out: which pbp_merged "
        "columns describe a PLAY rather than a player-week, and why each cohort does",
    "pbp_column_adjudication.py:NOT_PLAY_GRAIN":
        "columns inside those cohorts' name-shape that are NOT play-grain and must be "
        "adjudicated one at a time -- the def_* rollup block above all",
    "pbp_column_adjudication.py:MAPPED":
        "pbp_merged column -> canonical, for the aggregate rollup surfaces",
    "pbp_column_adjudication.py:SOURCES":
        "which pbp_merged sources that pass is scoped to",
    "pbp_column_adjudication.py:_STATE":
        "the GAME_AND_CLOCK_STATE cohort's membership -- which play columns describe the "
        "situation rather than a player. A CLOSED enumeration, deliberately: the fourth "
        "cohort is not a catch-all, so a column the corpus gains upstream falls to the "
        "residual and stays OPEN instead of being swept",
    "pbp_column_adjudication.py:_MODEL":
        "the MODEL_OUTPUT cohort's membership -- which columns are nflverse model "
        "evaluations rather than observations",
    "pbp_column_adjudication.py:_PARTICIPANT":
        "the PARTICIPANT_IDENTITY cohort's bare-name membership",
    "pbp_column_adjudication.py:_PARTICIPANT_SUFFIXES":
        "the name shapes that mark a play column as naming WHO filled a role",
    "pbp_column_adjudication.py:_PLAY_EVENT":
        "the PLAY_EVENT cohort's membership -- the per-play markers and measures whose "
        "player-week form is an aggregate carried by the rollup",
    "pbp_column_adjudication.py:_NAME_COLLISIONS":
        "the seven strings that exist in BOTH the play table and v26 and are excluded "
        "anyway; naming them is what makes that exclusion visibly deliberate",
    "newspaper_sidecar_column_adjudication.py:COHORTS":
        "the arguments the sidecar's WIDE columns are excluded on -- gate flags, row keys, "
        "three generations of identity resolution, the reviewer ledger, event grain, "
        "narrative. Stated once, cited per column",
    "newspaper_sidecar_column_adjudication.py:_BY_COHORT":
        "which column belongs to which of those arguments",
    "newspaper_sidecar_column_adjudication.py:_IDENTITY_GENERATION_MARKERS":
        "the name shapes each successive identity pass prefixed rather than overwrote; a "
        "marker rule instead of an enumeration because the enumeration refills itself",
    "newspaper_sidecar_column_adjudication.py:MAPPED":
        "the sidecar columns that ARE registered canonicals",
    "newspaper_sidecar_column_adjudication.py:ESCALATED":
        "the sidecar questions refused: the symmetric team-score pair, the raw position "
        "and side strings against a governed taxonomy, and the participation vocabulary "
        "that has no column",
    "newspaper_sidecar_column_adjudication.py:SOURCES":
        "which newspaper sidecar tables that pass is scoped to",
    "pfr_datastat_column_adjudication.py:MAPPED":
        "PFR data-stat id -> canonical, argued against the machine id the site's own "
        "generator emits. Keyed on the COLUMN alone because that is the grain PFR's "
        "vocabulary varies at; ids that do NOT mean one thing across tables are escalated "
        "instead of disambiguated by table",
    "pfr_datastat_column_adjudication.py:NEW_CANDIDATES":
        "PFR material v26 does not carry -- the charting surface above all (RPO, "
        "play-action, pocket time, on-target throws) plus the six drafted-then-corrected "
        "entries whose ids looked like an adjacent canonical",
    "pfr_datastat_column_adjudication.py:EXCLUDED":
        "PFR columns excluded, each mapped to a named argument rather than a bespoke "
        "sentence",
    "pfr_datastat_column_adjudication.py:_EXCLUSION_ARGUMENTS":
        "the arguments themselves -- stated once, cited per column, so an exclusion cannot "
        "be justified by a reason nobody else uses",
    "pfr_datastat_column_adjudication.py:DERIVED_RATIOS":
        "PFR rates whose operands the same table publishes; validated to exist in the "
        "contract registry before any of them may be written",
    "pfr_datastat_column_adjudication.py:ESCALATED":
        "the PFR questions refused rather than guessed: the tackle total from a fourth "
        "surface, the def_tgt_yds_per_att / def_yds_per_target pair, the five-way position "
        "taxonomy, the awards and draft_info composites, and the LONG-vs-WIDE regime "
        "question on box_team_stats / box_game_info",
    "composite_column_adjudication.py:BLOCKER_CODES_BY_SPEC":
        "the concrete production evidence failures rolled up at their five owning specs",
    "composite_column_adjudication.py:SETTLING_EVIDENCE_BY_SPEC":
        "the machine-gated evidence each failed composite specification must produce",
    "composite_column_adjudication.py:CANONICAL_SPLIT_PROJECTIONS":
        "executable canonical split projections eligible to settle NFL.com L7 grain",
    "closure_chain.py:OUR_OWN_COMPUTATION_FAMILIES":
        "which contract families no external publisher can witness -- the decision that "
        "separates the 1,218-stat registry from the 369-column externally-witnessable "
        "core, and therefore what every coverage percentage in the chain is measured "
        "against",
    # -- decisions living outside the sink modules, caught by PROSE ----------------------
    # -- RULE C: the refusals themselves ------------------------------------------------
    "refusal_preconditions.py:REFUSALS":
        "every enrolled refusal and the machine-checkable precondition it stands on -- a "
        "refusal IS a decision (not to map, not to license, not to capture), and it is the "
        "kind that stops anyone from re-examining the thing it refuses",
    "refusal_preconditions.py:PREDICATES":
        "the vocabulary of preconditions a refusal may be declared with -- which questions "
        "the program accepts as machine-checkable ground",
    "refusal_preconditions.py:SUPERSEDED_TEXT_MARKERS":
        "phrases whose appearance in a refusal's published text means it still cites ground "
        "that has gone; the enrolled contract copy of a receipt clause can rot independently "
        "of the generator that wrote it, and did",
    "test_mapping_obligation.py:MAPPING_PENDING":
        "sources licensed to hold a mapping obligation open, each with its blocker -- the "
        "gate between adjudicated IDENTITY and permission to VOTE",
    "asymmetry_registry.py:DOCUMENTED_RESIDUALS":
        "known asymmetries accepted with a documented reason rather than repaired",
}

EXEMPT_ADJUDICATION_REGISTRIES: dict[str, str] = {}

# ---- the DENOMINATOR of primary_lineages_without_capture_contract, made explicit ----
#
# That gate used to read zero because six lineages were subtracted from its denominator
# by a set literal inline in the expression. Two of the six (nflcom, statscrew) had since
# acquired real contracts, so their subtraction was merely redundant -- but FOUR
# (internal, ngs, pbp_merged, pfa_loc) had no contract at all and were invisible.
#
# Worse, two of those four were not missing contracts: `ngs` and `pfa_loc` are the SAME
# origins as the `nextgen_stats` and `profootballarchives` contracts under a different
# spelling. The old code patched both ends -- subtracting the source-side spelling and
# adding the contract-side spelling -- so a NAMING MISMATCH cancelled itself out and
# presented as coverage. A counter reading clean off two compensating hand-edits is the
# thing [[feedback-check-the-denominator-beneath]] is about.
#
# Now the alias is DECLARED, so the two spellings are visibly one origin...
LINEAGE_CONTRACT_ALIASES = {
    "ngs": "nextgen_stats",
    "pfa_loc": "profootballarchives",
}
# ...and an exemption must carry its reason, checked by its own meta-gate. These two are
# genuinely not capturable families: there is no external site with a table of contents
# to reconcile against, because we computed them.
CAPTURE_CONTRACT_EXEMPT_LINEAGES = {
    "internal": "our own derived/computed material (scoring_summary, player_bio, "
                "supertable rollups) -- no external publisher, so no TOC to capture "
                "against; its obligations are the derivation-DAG and equation lanes",
    "pbp_merged": "merged play-by-play, assembled by us from registered pbp sources; "
                  "the capture obligation belongs to those sources, not to the merge",
}


def _zero_gates() -> list[dict]:
    gates = []

    from .source_registration_census import build as census_build
    c = census_build()["counters"]
    gates += [
        {"gate": "disk_census_undispositioned", "value": c["unregistered_without_disposition"]},
        # THE LAYER BENEATH THAT ONE. `disk_census_undispositioned` asks whether every
        # PUBLISHER directory is accounted for; registration happens per (publisher,
        # DATASET). `ff_assets/statscrew` read REGISTERED off a single dataset while
        # `ff_assets/statscrew/team_season_stats` sat on disk with 137,864 rows and no
        # registry row, through every gate run for a day. Denominator: 15 dirs -> 15 + 18
        # children.
        {"gate": "child_dir_census_undispositioned",
         "value": c["child_unregistered_without_disposition"]},
        {"gate": "contract_datasets_outside_census", "value": c["contract_datasets_outside_census"]},
        {"gate": "unknown_lake_toplevel_dirs", "value": c["unknown_lake_toplevel_dirs"]},
    ]

    # THE SUBJECT-LEVEL LADDER. Seeded from subject_level_recon: 807 checks at or above
    # 0.995 are floored so the NEXT REBUILD cannot silently break one. Two things are gated
    # to zero here rather than queued, because neither is a deferral:
    #   * a floored check that has FALLEN -- the rebuild regressed a column
    #   * a MAX-class column the aggregator would SUM -- the drift that produced 4,407
    #     impossible "long" values, now unrepresentable but worth a standing guard
    # READ THE LEDGER, DO NOT RECOMPUTE IT. The first version of this block called
    # subject_level_recon.build() inline and the gate stopped finishing -- 841 checks over a
    # multi-GB weekly parquet is minutes, and a gate nobody can afford to run is a gate that
    # stops being run. Same contract as the licence floors: measurement goes off-repo, the
    # gate compares and checks the release pin.
    try:
        from .sources import latest_v26
        from .subject_level_recon import LEDGER as _SLL
        with open(_SLL, encoding="utf-8") as _f:
            _sl = json.load(_f)
        _rel = os.path.basename(os.path.dirname(os.path.dirname(latest_v26())))
        # A MISSING PIN IS A VIOLATION, NOT A PASS. `not in (None, _rel)` let an unpinned
        # ledger through, which is the same hole the licence floors close with "FLOORS DO NOT
        # PIN A RELEASE: cannot tell whether they are stale".
        if _sl.get("measured_against_release") != _rel:
            gates.append({"gate": "subject_level_ledger_stale", "value": 1,
                          "detail": f"pinned to {_sl.get('measured_against_release')!r}, "
                                    f"current release {_rel!r} -- re-run the lane"})
        else:
            gates.append({"gate": "subject_level_ledger_stale", "value": 0})
        _flp = os.path.join(HERE, "witness_gate", "contracts",
                             "witness_licence_floors.v1.json")
        with open(_flp, encoding="utf-8") as _f:
            _slf = {r["key"]: r for r in
                    json.load(_f).get("subject_level_floors", [])}
        _cur = {f'{r["column"]}|{r["level"]}': r.get("agree_pct")
                for r in _sl["rows"] if r.get("agree_pct") is not None}
        _fell = [f"{k}: {_cur[k]} < floor {v['agree_pct']}"
                 for k, v in _slf.items()
                 if k in _cur and _cur[k] + 1e-9 < v["agree_pct"]]
        _missing = sorted(k for k in _slf if k not in _cur)
        gates.append({"gate": "subject_level_checks_below_floor", "value": len(_fell),
                      "detail": _fell[:12]})
        # A floored check that STOPPED BEING MEASURED is not a pass. Same defect as a
        # licensed witness contributing no rows.
        gates.append({"gate": "subject_level_floored_checks_no_longer_measured",
                      "value": len(_missing), "detail": _missing[:12]})
    except Exception as _e:                                          # noqa: BLE001
        # A bare message hid a NameError once already. Name the type.
        gates.append({"gate": "subject_level_lane_unrunnable", "value": 1,
                      "detail": f"{type(_e).__name__}: {_e}"[:200]})

    # THE STRUCTURAL GUARD BEHIND IT. max_cols is now DERIVED from aggregation_class, so
    # this cannot drift -- but it drifted silently for long enough to ship 4,407 impossible
    # values, and a standing gate costs one set comparison.
    try:
        import sys as _sys
        _ffs = os.path.join(HERE, "..", "..", "fantasy_football_data_scripts")
        if _ffs not in _sys.path:
            _sys.path.insert(0, _ffs)
        from multi_league.core.stat_contracts_loader import (aggregation_sets as _aggs,
                                                             contract_classes as _ccs)
        _declared = {s for s, (a, _d) in _ccs().items() if a == "MAX"}
        gates.append({"gate": "max_class_columns_the_aggregator_would_sum",
                      "value": len(_declared - _aggs().max_cols),
                      "detail": sorted(_declared - _aggs().max_cols)})
    except Exception as _e:                                          # noqa: BLE001
        queues.append({"queue": "max_cols_guard_unrunnable", "count": 1,
                       "detail": str(_e)[:160]})

    # PROOF-OR-PENDING ON THE REPAIR LEDGER (§19). These three ARE gated, because none of
    # them is a deferral: an unknown fault means the row is unclassifiable, a missing status
    # means nobody said whether it landed, and `remedy_applied: true` without a passing
    # receipt is a backlog emptying itself while the data never changed. The receipt is a
    # PREDICATE and it is RE-RUN here, so a reverted fix fails the gate too.
    from .repair_queue import build as _repair_build
    _rqc = _repair_build()["counters"]
    gates += [
        {"gate": "adjudications_with_unknown_fault",
         "value": _rqc["adjudications_with_unknown_fault"]},
        {"gate": "adjudications_missing_remedy_status",
         "value": _rqc["adjudications_missing_remedy_status"]},
        {"gate": "remedy_applied_without_receipt",
         "value": _rqc["remedy_applied_without_receipt"]},
    ]

    from .sources import registry
    from . import witness_map as WM
    from .test_mapping_obligation import (LANE_WITNESSED, MAPPING_PENDING,
                                          NON_MAPPING_ROLES)
    mapped = {m.source_key for m in WM.WITNESS_MAP}
    undeclared = [k for k, s in registry(include_subject=True).items()
                  if k not in mapped and k not in LANE_WITNESSED
                  and k not in MAPPING_PENDING and s.role not in NON_MAPPING_ROLES]
    # THE COMPOSITION, because 0 here does NOT mean "every source is mapped" and reads
    # exactly as if it did. The gate asks whether every source has a mapping OBLIGATION
    # DECLARED -- and a declaration can be a MapSpec that actually aliases columns, or a
    # note saying why one does not exist yet. MEASURED 2026-07-27: of 91 sources, only 38
    # carry a real MapSpec; 29 are LANE_WITNESSED, 9 are MAPPING_PENDING, 17 hold a
    # non-mapping role. So 38 satisfy this gate with a NOTE. That is a legitimate
    # disposition, not a defect -- but a zero whose composition is invisible is how
    # `column_dossier_open_rows` hid 192 refusals inside plain backlog, so the split is
    # reported inline rather than left to be re-derived.
    _all_sources = registry(include_subject=True)
    _composition = {
        "sources": len(_all_sources),
        "satisfied_by_a_mapspec": len(mapped & set(_all_sources)),
        "satisfied_by_lane_witnessed_note": len(set(LANE_WITNESSED) & set(_all_sources)),
        "satisfied_by_mapping_pending_note": len(set(MAPPING_PENDING) & set(_all_sources)),
        "non_mapping_role": sum(1 for s in _all_sources.values()
                                if s.role in NON_MAPPING_ROLES),
    }
    gates.append({"gate": "sources_without_mapping_obligation", "value": len(undeclared),
                  "composition": _composition,
                  "detail": undeclared})

    from . import lineage_roots
    gates.append({"gate": "lineage_contract_drift",
                  "value": 0 if lineage_roots.load() == lineage_roots.generate() else 1})
    gates.append({"gate": "lineage_contract_invalid",
                  "value": len(lineage_roots.validate(lineage_roots.load()))})

    from . import relationship_edges as RE
    edoc = RE.load()
    gates.append({"gate": "edge_contract_drift",
                  "value": 0 if edoc == RE.generate() else 1})
    gates.append({"gate": "edge_contract_invalid", "value": len(RE.validate(edoc))})

    from .relationship_candidates import load_contract_stats
    stats = load_contract_stats()
    gates.append({"gate": "stats_without_tolerance_policy",
                  "value": sum(1 for s in stats if not s.get("tolerance_policy"))})

    from .derivation_dag import build as dag_build
    d = dag_build()["counters"]
    partition_ok = d["single_root_cells"] == sum(
        v for k, v in d.items()
        if k.startswith("single_root_cells_") and k != "single_root_cells")
    # A MapSpec on a table-major source that does not NAME its table is the NFL.com trap
    # restored: `nflcom_player_season.lng` is fg_long under field-goals, passing_long under
    # passing and punt_long under punts, so a spec silent about its table keeps ONE of seven
    # canonicals and unions every category table into it. It does not error -- it validates
    # confidently against the wrong thing. Zero-gated because the failure is silent.
    #
    # The same clause covers the partition dimension. A split source has no Total row, so a
    # season value summed across every dimension reads 5-6x high, which presents as total
    # supertable failure rather than as a modelling error.
    from . import mapspec_generator as _MG
    from . import witness_map as _WM
    _naked = sorted(
        f"{m.source_key}.{m.source_col}"
        for m in _WM.WITNESS_MAP
        if m.source_key in _MG.TABLE_MAJOR and not (m.source_table and m.table_col))
    _unpartitioned = sorted(
        f"{m.source_key}.{m.source_col}"
        for m in _WM.WITNESS_MAP
        if _MG.TABLE_MAJOR.get(m.source_key, {}).get("row_filter") and not m.row_filter)
    gates.append({"gate": "table_major_specs_without_a_table_selector",
                  "value": len(_naked) + len(_unpartitioned),
                  "detail": {"no_table_selector": _naked[:20],
                             "no_partition_dimension": _unpartitioned[:20]},
                  "denominator": {"table_major_sources": sorted(_MG.TABLE_MAJOR),
                                  "specs_on_them": sum(
                                      1 for m in _WM.WITNESS_MAP
                                      if m.source_key in _MG.TABLE_MAJOR)}})
    gates.append({"gate": "derivation_dag_partition_mismatch",
                  "value": 0 if partition_ok else 1})

    # ---- O.8 Law A (DISCOVERY): every hunt candidate terminally dispositioned ----
    from .source_hunt_census import build as hunt_build
    h = hunt_build()["counters"]
    gates.append({"gate": "hunt_candidates_without_terminal_disposition",
                  "value": h["hunt_candidates_without_terminal_disposition"]})

    # ---- O.8 Law B (CAPTURE): capture contracts must be structurally valid ----
    # NOTE the counter split: an INVALID status row is a zero-gate (the contract is
    # malformed), while OPEN toc entries are a legal enumerated QUEUE -- Law B's whole
    # point is that uncaptured families stay visible and counted, not that they are
    # forced to zero by declaring them away.
    from .capture_contracts import build as cap_build
    cp = cap_build()["counters"]
    gates.append({"gate": "capture_contract_invalid_status_rows",
                  "value": cp["invalid_status_rows"]})
    from .sources import registry as _reg
    from .capture_contracts import CONTRACTS as _CAP
    primary_lineages = {s.lineage for k, s in _reg(include_subject=True).items()
                        if s.witness_class == "primary"}
    covered = {LINEAGE_CONTRACT_ALIASES.get(lin, lin) for lin in primary_lineages} & set(_CAP)
    uncontracted_lineages = sorted(
        lin for lin in primary_lineages
        if LINEAGE_CONTRACT_ALIASES.get(lin, lin) not in _CAP
        and lin not in CAPTURE_CONTRACT_EXEMPT_LINEAGES)
    gates.append({"gate": "primary_lineages_without_capture_contract",
                  "value": len(uncontracted_lineages), "detail": uncontracted_lineages,
                  "denominator": sorted(primary_lineages),
                  "covered_by_contract": sorted(covered),
                  "exempt_with_reason": sorted(
                      lin for lin in primary_lineages
                      if lin in CAPTURE_CONTRACT_EXEMPT_LINEAGES)})
    # Meta-gate on the exemptions themselves: an exemption without a reason is just a
    # subtraction, and a subtraction is how this counter read zero while hiding four
    # lineages (see the comment on CAPTURE_CONTRACT_EXEMPT_LINEAGES).
    gates.append({"gate": "capture_contract_exemptions_without_reason",
                  "value": sum(1 for reason in CAPTURE_CONTRACT_EXEMPT_LINEAGES.values()
                               if not reason)})

    # O.9.3: a column adjudication that fails to say WHAT it claims or WHY is not a
    # decision. This is a zero-gate rather than a queue because the ledger is small and
    # hand-written -- a malformed entry is a mistake to fix, never a backlog to carry.
    import json as _j, os as _o
    _dossier_path = _o.path.join(HERE, "..", "..", "docs", "column-dossier.json")
    if _o.path.exists(_dossier_path):
        with open(_o.path.abspath(_dossier_path), encoding="utf-8") as _f:
            _dd = _j.load(_f)
        gates.append({"gate": "column_dispositions_invalid",
                      "value": _dd["counters"].get("column_dispositions_invalid", 0),
                      "detail": _dd.get("column_dispositions_invalid", [])})

    # THE AUDIT HAD NO TEETH. Until 2026-07-31 nothing anywhere asserted NFL.com agreement:
    # if def_pass_defended slid from 97.7% to 40% no test failed and no counter moved,
    # because a licence was written once and never re-checked. This compares the live
    # licence file against the floors each mapping was receipted at.
    #
    # A MISSING LICENCE IS A VIOLATION, NOT A PASS. The obvious shape -- iterate the licence
    # rows and check them -- reads 0 when the licence file is absent or a row has been
    # dropped, which is the exact failure it exists to catch. So the FLOORS are the
    # denominator and a floor with no licence row counts against us.
    _floors_path = os.path.join(CONTRACTS_DIR, "witness_licence_floors.v1.json")
    if os.path.exists(_floors_path):
        with open(_floors_path, encoding="utf-8") as _f:
            _floors_doc = json.load(_f)
        _floors = _floors_doc["floors"]
        _lic_path = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
                     r"\MAPPING_LICENSES.json")
        _eq_floors = {}
        _lic = {}
        if os.path.exists(_lic_path):
            with open(_lic_path, encoding="utf-8") as _f:
                # PRECISE key. Under (source, v26_col) a floor for QB Career's `fumbles`
                # was satisfied by whichever of QB/RBFB/WRTE validated last.
                _lic = {(r["source"], r.get("source_table", ""), r["v26_col"]): r
                        for r in json.load(_f)["rows"]}
        # THE EQUATION LANE HAD RECEIPTS AND NO FLOOR. Its 13 measurements -- passer_rating
        # at 0.9899, avg at 0.9946 -- were written to a ledger nothing compared against, so
        # they could have gone to 0.5 with no gate moving. A measurement nothing watches is
        # the exact state this gate exists to end, and building the lane without extending
        # the gate rebuilt it one layer over.
        try:
            from .audit_capacity_runner import LEDGER as _ACL2
            if os.path.exists(_ACL2):
                with open(_ACL2, encoding="utf-8") as _f:
                    for _r in json.load(_f)["rows"]:
                        if _r.get("status") == "MEASURED":
                            _eq_floors[_r["key"]] = _r["agree_pct"]
        except Exception:
            pass
        _below = []
        # THE RATCHET. Every check below compares a MEASUREMENT against a FLOOR -- which is
        # defeated completely by editing the floor. A floor lowered to 0.01 made this gate
        # read GREEN with no record of the edit, so the entire apparatus could be disarmed by
        # one number, most likely by whoever is staring at a red gate late at night.
        #
        # `high_water` is the best value a floor has ever held. A floor may RISE freely (that
        # is what repairing a defect does) and may never FALL below its high water without an
        # explicit, reasoned entry in `ratchet_exemptions`. Weakening the standard is now a
        # deliberate act that leaves a trace, instead of an invisible one.
        _exempt = {e["key"]: e for e in _floors_doc.get("ratchet_exemptions", [])}
        for _grp in ("floors", "equation_floors", "week_grain_floors",
                     "population_scoped_floors", "aggregate_floors",
                     "conservation_floors"):
            for _r in _floors_doc.get(_grp, []):
                _hw = _r.get("high_water")
                _id = _r.get("key") or f"{_r.get('source')}|{_r.get('source_table','')}|{_r.get('v26_col')}"
                if _hw is None:
                    _below.append(f"{_id}: NO high_water -- floor is not ratcheted")
                elif (_r.get("agree_pct") or 0) + 1e-9 < _hw:
                    _ex = _exempt.get(_id)
                    if not _ex or not (_ex.get("reason") or "").strip():
                        _below.append(f"{_id}: FLOOR LOWERED {_hw} -> {_r.get('agree_pct')} "
                                      f"with no reasoned ratchet_exemptions entry")
        # CONSERVATION floors, the sixth kind: players must AGGREGATE TO TEAMS and must
        # MATCH THEMSELVES. The column audit cannot see either -- its subject is one player's
        # one column, and the team-DEF plane has no pfr_id so it never reaches the crosswalk.
        # Floored at MEASURED values because several are known-broken (def_tackles_solo .17,
        # def_safeties .02-.65) and a floor is a regression guard; the ratchet is what makes
        # repairing them monotonic.
        _cons = {}
        _cp = os.path.join("D:", os.sep, "league-history-data", "nfl", "derived",
                           "validation", "sota_recon_master", "conservation_receipts.json")
        if os.path.exists(_cp):
            with open(_cp, encoding="utf-8") as _f:
                _cons = {r["key"]: r["agree_pct"] for r in json.load(_f)["rows"]}
        for _cf in _floors_doc.get("conservation_floors", []):
            _got = _cons.get(_cf["key"])
            if _got is None:
                _below.append(f"{_cf['key']}: NO MEASUREMENT (floor {_cf['agree_pct']})")
            elif _got + 1e-9 < _cf["agree_pct"]:
                _below.append(f"{_cf['key']}: {_got} < floor {_cf['agree_pct']}")
        # STALENESS IS THE FAILURE THE VALUE CHECKS CANNOT SEE. Every check below compares a
        # floor against a MEASUREMENT LEDGER, so it catches a value that moved and is blind to
        # a ledger that was never re-run. If the release changes and nobody re-measures, every
        # floor still passes -- green-means-captured, one layer over. The floors therefore pin
        # the release they were measured against, and a mismatch is a violation in itself.
        try:
            from .sources import latest_v26 as _lv
            _cur = os.path.basename(os.path.dirname(os.path.dirname(latest_v26())))
        except Exception:
            _cur = None
        _wasmeasured = _floors_doc.get("measured_against_release")
        if _cur and _wasmeasured and _cur != _wasmeasured:
            _below.append(f"STALE FLOORS: measured against {_wasmeasured}, current release "
                          f"is {_cur} -- re-run the audit before trusting any floor")
        elif _cur and not _wasmeasured:
            _below.append("FLOORS DO NOT PIN A RELEASE: cannot tell whether they are stale")
        for _fl in _floors:
            _row = _lic.get((_fl["source"], _fl.get("source_table", ""), _fl["v26_col"]))
            if _row is None:
                _below.append(f"{_fl['source']}|{_fl['v26_col']}: NO LICENCE ROW "
                              f"(floor {_fl['agree_pct']})")
                continue
            _got, _want = _row.get("agree_pct"), _fl["agree_pct"]
            if _got is None or _got + 1e-9 < _want:
                _below.append(f"{_fl['source']}|{_fl['v26_col']}: {_got} < floor {_want}")
            elif _row["verdict"].split(":")[0] != _fl["verdict"]:
                _below.append(f"{_fl['source']}|{_fl['v26_col']}: verdict "
                              f"{_row['verdict']} != receipted {_fl['verdict']}")
        # equation-lane floors live in the same contract, keyed by dossier row rather than
        # by (source, v26_col), because a rate is checked by an EXPRESSION and not by a
        # licensed mapping. A declared floor with no measurement is a violation here too.
        # THE STORED-PLANE RATE FLOORS. A published rate read against the WEEKLY release
        # verifies our weekly->season rollup; read against the STORED season plane it
        # verifies the table we actually serve. Both are needed and only the first existed.
        # Declared with no measurement is a violation here as everywhere else.
        for _sp in _floors_doc.get("season_plane_equation_floors", []):
            _got = _eq_floors.get(_sp["key"] + "@season")
            if _got is None:
                # measurement lives in the same ledger the weekly equation floors use; if the
                # lane has not been re-run for this plane the floor is UNVERIFIED, not passing
                _below.append(f"{_sp['key']} [season plane]: NO MEASUREMENT "
                              f"(floor {_sp['agree_pct']})")
            elif _got + 1e-9 < _sp["agree_pct"]:
                _below.append(f"{_sp['key']} [season plane]: {_got} < floor {_sp['agree_pct']}")
        for _ef in _floors_doc.get("equation_floors", []):
            _got = _eq_floors.get(_ef["key"])
            if _got is None:
                _below.append(f"{_ef['key']}: NO MEASUREMENT (floor {_ef['agree_pct']})")
            elif _got + 1e-9 < _ef["agree_pct"]:
                _below.append(f"{_ef['key']}: {_got} < floor {_ef['agree_pct']}")
        # WEEK-GRAIN floors, the third kind. The Recent Games recovery produced 54
        # (block, column) measurements that CANNOT be expressed as licences: the licence key
        # is (source, v26_col) and they collide with the season-grain licences the career
        # captions hold for the same canonicals. That is a real defect in the licence key and
        # it is escalated -- but LICENSING and GATING are different questions, and conflating
        # them is why these sat unwatched. A licence says who may VOUCH; a floor says what
        # must not REGRESS. These need the second, not the first.
        _wk = {}
        try:
            from .nflcom_recent_games_recovery import LEDGER as _RGL
            if os.path.exists(_RGL):
                with open(_RGL, encoding="utf-8") as _f:
                    for _r in json.load(_f)["validation"]:
                        if _r.get("agree_pct") is not None:
                            _wk[f"{_r['block']}|{_r['column']}|{_r['canonical']}"] = _r["agree_pct"]
        except Exception:
            pass
        # AGGREGATE floors, the fourth kind: g/gs are a coarser-grain value checked against a
        # SEASON-table canonical, so they are neither a licensed mapping nor a two-operand
        # ratio. They are watched at their MEASURED value (0.6295 / 0.6038) because the floor
        # is a REGRESSION guard, not a quality bar -- a mapping known to disagree still must
        # not silently disagree MORE. Their disagreement is filed as a supertable defect.
        _agg = {}
        _aggp = os.path.join("D:", os.sep, "league-history-data", "nfl", "derived",
                             "validation", "sota_recon_master",
                             "audit_capacity_aggregate_receipts.json")
        if os.path.exists(_aggp):
            with open(_aggp, encoding="utf-8") as _f:
                _agg = {r["key"]: r["agree_pct"] for r in json.load(_f)["rows"]}
        for _af in _floors_doc.get("aggregate_floors", []):
            _got = _agg.get(_af["key"])
            if _got is None:
                _below.append(f"{_af['key']}: NO MEASUREMENT (floor {_af['agree_pct']})")
            elif _got + 1e-9 < _af["agree_pct"]:
                _below.append(f"{_af['key']}: {_got} < floor {_af['agree_pct']}")
        # POPULATION-SCOPED floors, the fifth kind. A source table's name can encode a scope
        # our subject lacks -- PFR's defense table and NFL.com's Defense page carry DEFENSIVE
        # players while def_tackles_* credits whoever the PBP names. The floor records BOTH
        # numbers so narrowing can never be invisible.
        _sc = {}
        _scp = os.path.join("D:", os.sep, "league-history-data", "nfl", "derived",
                            "validation", "sota_recon_master",
                            "nflcom_population_scoped_receipts.json")
        if os.path.exists(_scp):
            with open(_scp, encoding="utf-8") as _f:
                _sc = {r["key"]: r["agree_pct"] for r in json.load(_f)["rows"]}
        for _sf in _floors_doc.get("population_scoped_floors", []):
            _got = _sc.get(_sf["key"])
            if _got is None:
                _below.append(f"{_sf['key']}: NO MEASUREMENT (floor {_sf['agree_pct']})")
            elif _got + 1e-9 < _sf["agree_pct"]:
                _below.append(f"{_sf['key']}: {_got} < floor {_sf['agree_pct']}")
        for _wf in _floors_doc.get("week_grain_floors", []):
            _got = _wk.get(_wf["key"])
            if _got is None:
                _below.append(f"{_wf['key']}: NO MEASUREMENT (floor {_wf['agree_pct']})")
            elif _got + 1e-9 < _wf["agree_pct"]:
                _below.append(f"{_wf['key']}: {_got} < floor {_wf['agree_pct']}")
        gates.append({"gate": "licensed_mappings_below_floor", "value": len(_below),
                      "detail": _below,
                      "denominator": {"floors": len(_floors),
                                      "equation_floors": len(_floors_doc.get("equation_floors", [])),
                                      "week_grain_floors": len(_floors_doc.get("week_grain_floors", [])),
                                      "aggregate_floors": len(_floors_doc.get("aggregate_floors", [])),
                                      "population_scoped_floors": len(_floors_doc.get("population_scoped_floors", [])),
                                      "conservation_floors": len(_floors_doc.get("conservation_floors", [])),
                                      "licence_rows_present": len(_lic),
                                      "measured_against_release": _wasmeasured,
                                      "current_release": _cur,
                                      "equation_measurements": len(_eq_floors)}})

    # CONSULTATION IS STRUCTURAL, BUT SILENT ABSENCE IS NOT. witness_votes._specs_for()
    # is driven off licensed() with no hardcoded source list, so a licensed spec IS in the
    # vote SQL and cannot be hand-excluded. What that does NOT guarantee is that it
    # CONTRIBUTES: a spec can sit in the union and return zero rows -- a bad join, a moved
    # parquet, an era with no overlap -- and the vote proceeds on the other witnesses with
    # no signal at all. "It was asked" and "it answered" are different facts.
    #
    # The floor carries each spec's measured `n`, and measured_against_release pins that n
    # to the CURRENT release, so a licensed pair with a floor whose n > 0 provably answers.
    # A licensed pair with NO floor, or a floor of n = 0, is a witness that may be silently
    # absent from every audit.
    try:
        from .witness_map import licensed as _licf
        _floors_path2 = os.path.join(CONTRACTS_DIR, "witness_licence_floors.v1.json")
        if os.path.exists(_floors_path2):
            with open(_floors_path2, encoding="utf-8") as _f:
                _fd2 = json.load(_f)
            # PER FLOOR, NOT PER PAIR. Aggregating with max() across a pair's floors is
            # exactly the masking this gate exists to catch: QB/RBFB/WRTE all floor
            # `fumbles`, so one of them silently contributing nothing is hidden by its
            # siblings. Each floor is one spec and each spec must answer for itself.
            _silent = sorted(
                f"{_f2['source']}|{_f2.get('source_table','')}|{_f2['v26_col']}: n={_f2.get('n')}"
                for _f2 in _fd2.get("floors", [])
                if _f2["source"].startswith("nflcom") and (_f2.get("n") or 0) <= 0)
            _have = {(_f2["source"], _f2["v26_col"]) for _f2 in _fd2.get("floors", [])}
            _silent += sorted(f"{s}|{c}: NO FLOOR" for (s, c) in _licf()
                              if s.startswith("nflcom") and (s, c) not in _have)
            _all_lic = _licf()
            gates.append({"gate": "licensed_witnesses_contributing_no_rows",
                          "value": len(_silent), "detail": _silent[:40],
                          # THE SCOPE IS PART OF THE READING. This gate checks nflcom only,
                          # because those are the pairs this programme has floors for. A `0`
                          # here means "every nflcom witness answers", NOT "every witness
                          # answers" -- and stating the excluded count is what stops the
                          # second reading. The excluded 232 are carried as a QUEUE below.
                          "denominator": {
                              "checked_nflcom_pairs":
                                  len([1 for (s, _c) in _all_lic if s.startswith("nflcom")]),
                              "licensed_pairs_OUTSIDE_this_gate":
                                  len([1 for (s, _c) in _all_lic
                                       if not s.startswith("nflcom")])}})
    except Exception:
        pass

    contracts_on_disk = {os.path.basename(p)
                         for p in glob.glob(os.path.join(CONTRACTS_DIR, "*.json"))}
    unenrolled = sorted(contracts_on_disk - set(ENROLLED_CONTRACTS) - set(EXEMPT_CONTRACTS))
    gates.append({"gate": "contracts_unenrolled_in_scoreboard", "value": len(unenrolled),
                  "detail": unenrolled})

    # ...and the same gate one layer down, over the decisions that live in Python. The
    # DENOMINATOR is reported beside the value, because that is what went wrong above.
    from .adjudication_registry_census import build as registry_build
    registries = registry_build()
    unenrolled_registries = sorted(
        set(registries["adjudication_registries"])
        - set(ENROLLED_ADJUDICATION_REGISTRIES) - set(EXEMPT_ADJUDICATION_REGISTRIES))
    gates.append({"gate": "adjudication_registries_unenrolled",
                  "value": len(unenrolled_registries),
                  "detail": unenrolled_registries,
                  "denominator": registries["counters"],
                  "rules": registries["rules"]})
    # An exemption without a reason is a subtraction, and a subtraction is how the gate
    # above read zero while hiding four lineages.
    gates.append({"gate": "adjudication_registry_exemptions_without_reason",
                  "value": sum(1 for reason in EXEMPT_ADJUDICATION_REGISTRIES.values()
                               if not reason)})

    # An adjudication pass that cannot REPORT its refusals makes them indistinguishable
    # from backlog inside column_dossier_open_rows. Zero-gated rather than queued: a
    # generator either exports its escalations or it hides them, and there is no partial
    # state worth carrying. A pass with nothing to escalate says so by returning {}.
    from .column_escalation_census import build as escalation_build
    _escalations = escalation_build()
    gates.append({"gate": "adjudication_generators_without_escalation_export",
                  "value": _escalations["counters"][
                      "generators_without_escalation_export"],
                  "detail": _escalations["generators_without_escalation_export"]})

    # ---- RULE C (2026-07-29): A REFUSAL WHOSE PRECONDITION NO LONGER HOLDS ----
    #
    # The three anti-drift rules were A (no hand-maintained numbers) and B (every counter
    # publishes its denominator). C was the one named and NOT BUILT, and it is the one that
    # cost the most: the splits/situational refusal cited a quarantine that had cleared and
    # held out 7,782,148 rows for a day; the crosswalk receipt wrote its verdict to a field
    # the consumer does not read and licensed nothing while reporting success.
    #
    # ZERO-GATED, both directions:
    #   SPENT       the ground a refusal stands on is gone and the refusal is still standing
    #   UNENROLLED  a refusal site exists on disk and NO precondition is being checked for
    #               it -- which is Rule B applied to Rule C, because a gate over only the
    #               refusals someone remembered to type in is the failure it exists to close
    #
    # The STALE-TEXT counter beside them is a QUEUE and not a gate on purpose: a refusal can
    # stand on live ground while its published prose still cites dead ground, and the fix
    # for the one instance we have (the enrolled crosswalk receipt) is to re-run
    # --write-receipt, which also LICENSES 2,278,306 rows. Correcting prose must never be a
    # side door into a licensing act.
    from .refusal_preconditions import build as refusal_build
    _refusals = refusal_build()
    _rc = _refusals["counters"]
    gates.append({"gate": "refusals_with_spent_preconditions",
                  "value": _rc["refusals_with_spent_preconditions"],
                  "detail": [row["refusal"] for row in _refusals["spent"]],
                  "denominator": _refusals["denominator"]["discovered_by_kind"]})
    gates.append({"gate": "refusal_sites_without_a_checked_precondition",
                  "value": _rc["refusal_sites_without_a_checked_precondition"],
                  "detail": [row["refusal"] for row in _refusals["unenrolled"]]})
    # An enrolment whose site has vanished is a refusal that reads as checked and refuses
    # nothing -- decoration that would keep the counter above looking healthy.
    gates.append({"gate": "refusal_enrolments_orphaned",
                  "value": _rc["orphaned_enrolments"],
                  "detail": [row["refusal"] for row in _refusals["orphaned"]]})

    # ---- 2026-07-29: two classes NO existing gate could see ------------------------
    # Both were found the same way -- something downstream tripped over them while every
    # counter above read clean -- which is the signature of a denominator measured one
    # layer too high.
    from . import capture_width_gate as CW
    from . import identity_coverage_gate as IC
    _ic = json.loads(IC.RECEIPT.read_text(encoding="utf-8")) if IC.RECEIPT.exists() else None
    if _ic is not None:
        # An id a source NAMES but our spine does not hold. The dossier counts columns and
        # the kc lane counts keys that JOIN, so a referenced-but-absent id is invisible to
        # both. Found because pfr-awards-v1 failed on 'St.CBo00'.
        # NOT a zero-gate. 221 absences that predate the gate are a BACKLOG, not a
        # regression, and the program's own vocabulary has the right slot: a declared
        # queue is "allowed nonzero ONLY as enumerated, receipted". Every id is
        # enumerated in docs/identity-coverage.json. Turning it green by blanket-
        # accepting them would be the exact defect the gate exists to catch.
        # What IS a zero-gate is the gate's own coverage: a source it could not read is
        # a source it cannot make any claim about.
        gates.append({"gate": "identity_coverage_sources_unreadable",
                      "value": max(0, _ic["counters"]["sources_unreadable"] - 1),
                      "detail": sorted(_ic["sources_unreadable"]),
                      "note": "newspaper_raw_archives is the one KNOWN-unreadable source "
                              "and is already carried by column_dossier_unreadable_sources; "
                              "it is subtracted here so the same hole is not counted twice, "
                              "and any SECOND unreadable source fails this gate"})
    _cw = json.loads(CW.RECEIPT.read_text(encoding="utf-8")) if CW.RECEIPT.exists() else None
    if _cw is not None:
        # A field the PARSER never wrote is absent from the physical snapshot, therefore
        # from the dossier, therefore from every downstream count -- so coverage reads
        # clean BECAUSE the data is missing. This gate reads the page, not the parquet.
        gates.append({"gate": "capture_width_dropped_field_suspects",
                      "value": _cw["counters"]["dropped_field_suspects"],
                      "denominator": {
                          "sources_checked": _cw["counters"]["sources_checked"],
                          "sources_without_a_page_root":
                              _cw["counters"]["sources_without_a_page_root"],
                          "sources_errored": _cw["counters"]["sources_errored"]}})
    return gates


def _declared_queues() -> list[dict]:
    queues = []
    # 2026-07-29: ids a source NAMES that our identity spine does not hold. Enumerated
    # per source and per id in docs/identity-coverage.json, so this is a burn-down with
    # names attached rather than a number. `pfr_combine` is 326 of the 387 source-level
    # hits -- combine invitees who never played, so no PFR player page exists -- and each
    # still has to be adjudicated rather than assumed.
    from . import identity_coverage_gate as IC
    if IC.RECEIPT.exists():
        _icq = json.loads(IC.RECEIPT.read_text(encoding="utf-8"))
        queues.append({"queue": "identity_spine_ids_referenced_but_absent",
                       "count": _icq["counters"]["distinct_ids_absent_from_spine"],
                       "sources_affected": len(_icq["per_source"]),
                       "denominator_hole": {
                           "sources_without_a_resolvable_id_column":
                               _icq["counters"]["sources_without_a_resolvable_id_column"],
                           "why_it_matters": "this gate can only speak for the sources it "
                                             "could read an id column from; the rest are "
                                             "counted so 0 never means 'all clear'"}})
    from .derivation_dag import build as dag_build
    d = dag_build()["counters"]
    # THE SECOND QUESTION, kept visible. `disposition` answers "is this column ours?";
    # audit capacity answers "can it CHECK us?", and ~2,134 rows carry an exclusion reason
    # that only ever addressed the first. These are QUEUES: a DECLARED capacity is a claim
    # nobody has measured, and BLOCKED is a route whose target does not exist yet. Reported
    # so the queue cannot be assumed empty merely because it is unstarted.
    _dpath = os.path.join(HERE, "..", "..", "docs", "column-dossier.json")
    if os.path.exists(_dpath):
        with open(os.path.abspath(_dpath), encoding="utf-8") as _f:
            _dc = json.load(_f)["counters"]
        for _c in ("audit_capacity_claims_declared",
                   "audit_capacity_blocked_on_a_missing_target"):
            queues.append({"queue": _c, "count": _dc.get(_c, 0)})

    # THE EQUATION LANE, and its counters are the RUNNER'S, not the typed status on the
    # decision. `audit_capacity.status` is the CLAIM; the receipts ledger is the RECEIPT.
    # Reading the typed field here would rebuild the hand-typed-status trap exactly: six
    # nflcom families sat PENDING_CROSSWALK for days after their receipt PASSED because a
    # generator short-circuited on a string before the gate ran.
    try:
        from .audit_capacity_runner import LEDGER as _ACL
        if os.path.exists(_ACL):
            with open(_ACL, encoding="utf-8") as _f:
                _ac = json.load(_f)
            for _k, _v in _ac["counters"].items():
                queues.append({"queue": _k, "count": _v})
    except Exception:
        pass

    # THE SUBJECT-LEVEL LADDER'S DEBT. 807 checks are floored at >=0.995; these are the ones
    # BELOW the bar, which are deliberately NOT floored so a ratchet cannot cement them. 3 of
    # them are the summed-long columns and will go to 0 on the next rebuild; 16 are float
    # representation (total_epa reads 2.27% at the declared 1e-06 and 100.00% at 1e-03) and
    # need a tolerance-policy ruling, not a repair.
    try:
        from .subject_level_recon import LEDGER as _SLL2
        with open(_SLL2, encoding="utf-8") as _f:
            _sl2 = json.load(_f)
        _below = [r for r in _sl2["rows"]
                  if r.get("agree_pct") is not None and r["agree_pct"] < 0.995]
        queues.append({"queue": "subject_level_checks_below_the_quality_bar",
                       "count": len(_below),
                       "detail": {"worst": [f"{r['column']}|{r['level']}={r['agree_pct']}"
                                            for r in sorted(_below,
                                                            key=lambda x: x["agree_pct"])[:8]]}})
        queues.append({"queue": "subject_level_career_columns_with_no_season_counterpart",
                       "count": _sl2["counters"].get("career_columns_absent_from_season", 0)})
    except Exception as _e:                                          # noqa: BLE001
        queues.append({"queue": "subject_level_queue_unrunnable", "count": 1,
                       "detail": f"{type(_e).__name__}: {_e}"[:200]})

    # THE REPAIR QUEUE. Joe deferred these repairs deliberately, so the COUNTS are queues,
    # not gates -- gating a deliberate deferral to zero just forces the decision to be
    # un-made. What is NOT deferrable is that a repair claimed as landed must prove it.
    from .repair_queue import build as repair_build
    _rq = repair_build()["counters"]
    for _k, _v in _rq.items():
        if _k not in ("adjudications_with_unknown_fault",
                      "adjudications_missing_remedy_status",
                      "remedy_applied_without_receipt"):
            queues.append({"queue": f"repair_{_k}", "count": _v})

    # THE DEMAND-JOIN LANE. Its denominator is the DOSSIER, not the contract -- counting
    # only candidates someone wrote a record for would read zero on an empty lane, which is
    # the exact failure mode this programme keeps re-learning.
    from .column_demand import build as demand_build
    _dm = demand_build()
    for _k, _v in _dm["counters"].items():
        queues.append({"queue": _k, "count": _v})

    # THE LAYER BENEATH `licensed_witnesses_contributing_no_rows`. That gate reads 0, and
    # its denominator is a scope I chose: nflcom only. Every other licensed pair -- pfr, the
    # ancient bundles, ngs, statscrew -- has NO floor of any kind, so nothing watches it for
    # regression and nothing verifies it answers. They are in exactly the state nflcom was in
    # before this session. Reported as a QUEUE because floors require MEASUREMENT, and
    # measuring another source's pairs is that source's audit, not a number to assert here.
    from .witness_map import licensed as _lic_all
    _fpq = os.path.join(CONTRACTS_DIR, "witness_licence_floors.v1.json")
    _floored_pairs = set()
    if os.path.exists(_fpq):
        with open(_fpq, encoding="utf-8") as _f:
            for _r in json.load(_f).get("floors", []):
                _floored_pairs.add((_r["source"], _r["v26_col"]))
    _unwatched = sorted(f"{s}|{c}" for (s, c) in _lic_all() if (s, c) not in _floored_pairs)
    queues.append({"queue": "licensed_pairs_without_any_floor",
                   "count": len(_unwatched),
                   "note": "licensed to vouch, and nothing watches them for regression or "
                           "verifies they answer -- the state nflcom was in before "
                           "2026-07-31. A floor needs a measurement, so closing these is "
                           "each source's own audit.",
                   "by_source": sorted({s.split("|")[0] for s in _unwatched})[:14]})

    # THE QUALITY GAP, reported so "gated" is never mistaken for "correct". A floor is a
    # REGRESSION guard: g/gs sit at .62 and are watched there, which stops them sliding and
    # says nothing about them being right. Without this counter a fully green board is
    # compatible with a supertable full of known defects -- which is exactly today's state,
    # nine of them diagnosed and deferred. The gap is a QUEUE, not a gate: when to close it
    # is Joe's call, and it must not be invisible while it is open.
    #
    # NO try/except HERE. The first version wrapped this in a bare handler, was placed where
    # `queues` was out of scope, and the NameError was swallowed -- so the counter silently
    # reported nothing and the board still read green. A counter that can fail quietly is
    # worse than no counter.
    _fp = os.path.join(CONTRACTS_DIR, "witness_licence_floors.v1.json")
    if os.path.exists(_fp):
        with open(_fp, encoding="utf-8") as _f:
            _fdq = json.load(_f)
        _BAR = 0.995
        _short = []
        for _grp in ("floors", "equation_floors", "week_grain_floors",
                     "population_scoped_floors", "aggregate_floors",
                     "conservation_floors"):
            for _r in _fdq.get(_grp, []):
                _a = _r.get("agree_pct")
                if _a is not None and _a < _BAR:
                    _short.append((_a, _r.get("key") or
                                   f"{_r.get('source')}|{_r.get('source_table','')}|"
                                   f"{_r.get('v26_col')}"))
        _short.sort()
        queues.append({"queue": "floors_below_the_quality_bar", "count": len(_short),
                       "bar": _BAR,
                       "note": "a floor is a REGRESSION guard, not a quality bar -- these are "
                               "watched where they are, not asserted to be right",
                       "worst": [f"{k} = {a}" for a, k in _short[:12]]})

    queues.append({"queue": "burn_down_zero_edge_single_root_cells",
                   "count": d["single_root_cells_with_zero_enforceable_edges"]})

    from .test_mapping_obligation import MAPPING_PENDING
    queues.append({"queue": "mapping_pending_sources", "count": len(MAPPING_PENDING)})

    from .composite_column_adjudication import build_decisions as composite_decisions
    _composite_receipt = os.path.join(
        HERE, "..", "..", "docs", "composite-witness-lane-receipt.json"
    )
    _, _composite_groups = composite_decisions(Path(_composite_receipt))
    queues.append({
        "queue": "composite_spec_blocker_groups",
        "count": len(_composite_groups),
        "detail": {
            "affected_rows": sum(group["affected_rows"] for group in _composite_groups),
            "groups": list(_composite_groups),
        },
    })

    from .source_registration_census import build as census_build
    c = census_build()["counters"]
    queues.append({"queue": "source_registrations_queued", "count": c["queued"]})
    # Queued at DATASET grain. This read 0 as a publisher-grain counter and it was not
    # zero underneath: ff_assets/profootballarchives/site_metadata holds the PFA coverage
    # declaration that docs/pfa-declared-coverage.json was built from -- evidence already
    # cited in two handoffs, sitting outside the census the whole time.
    queues.append({"queue": "child_registrations_queued",
                   "count": c["child_dirs_queued"],
                   "detail": {"child_dirs": c["child_dirs"],
                              "registered": c["child_dirs_registered"]}})

    from . import relationship_edges as RE
    edoc = RE.load()
    queues.append({"queue": "near_miss_drift_flags",
                   "count": edoc["counts"]["near_miss_flagged"]})
    queues.append({"queue": "do_not_enforce_registry_rows",
                   "count": edoc["counts"]["do_not_enforce"]})
    queues.append({"queue": "edges_open_pending_or_escalated",
                   "count": edoc["counts"]["open"]})

    from . import lineage_roots
    queues.append({"queue": "lineage_open_questions",
                   "count": sum(1 for q in lineage_roots.load()["open_questions"]
                                if q.get("status") != "RESOLVED")})

    # O.8 Law B: uncaptured TOC families, enumerated per source. This queue going
    # from 0 to nonzero is the law WORKING -- before it existed, "we have StatsCrew"
    # and "we have all of StatsCrew" were indistinguishable.
    from .capture_contracts import build as cap_build
    queues.append({"queue": "capture_contract_open_toc_entries",
                   "count": cap_build()["counters"]["capture_contract_open_toc_entries"]})

    # O.9.0: registered sources whose DATA is mislabelled at rest and may not be licensed
    # until repaired (today: the nflcom column-shift families). Registry-driven, so it can
    # only reach zero by lifting a quarantine on the source -- never by editing this file.
    # Before this counter, a 7.78M-row misattribution was visible only in a source note.
    from . import sources as _S
    quarantined = sorted(k for k, s in _S.registry().items() if "QUARANTINED" in s.note)
    queues.append({"queue": "quarantined_sources", "count": len(quarantined),
                   "detail": quarantined})

    # O.8e: URL-pattern families observed in RETAINED BYTES that no capture-contract
    # TOC entry claims -- the unknown-unknown queue. Nonzero means a site publishes
    # something our hand-authored contract never mentioned.
    from .derive_source_toc import build as toc_build
    queues.append({"queue": "source_toc_patterns_unaccounted",
                   "count": toc_build(sample=25)["counters"][
                       "source_toc_patterns_unaccounted"]})

    # O.8f Phase 0: identity fields recovered from retained bytes, awaiting Joe's
    # sign-off. Applying DOB to player_bio MUTATES THE IDENTITY SPINE, so it is a
    # queue, never an auto-apply.
    import json as _json, os as _os
    _rp = os.path.join(HERE, "..", "..", "docs", "reparse-retained-captures.json")
    if os.path.exists(_rp):
        with open(os.path.abspath(_rp), encoding="utf-8") as _f:
            _d = json.load(_f)
        queues.append({"queue": "reparsed_identity_rows_pending_signoff",
                       "count": _d["counters"].get("rows_with_birth_date", 0)})

    # O.8 Law A: hunt candidates still legitimately open (owned, not abandoned)
    from .source_hunt_census import build as hunt_build
    queues.append({"queue": "hunt_candidates_open",
                   "count": hunt_build()["counters"]["queued"]})

    # O.9 Phase 0b / Law A CONFIG DIMENSION (handoff finding 9). The harvest repo's own
    # source_catalog.yaml is a discovery ledger: a dataset declared there with no parser
    # branch behind it is a family we KNOW about and cannot capture. It passed every
    # counter we had, because every counter starts downstream at what landed on disk --
    # and it cost ~10 machine-hours of green-but-empty jobs in ff-assets run
    # 30226751762. The catalog now carries an explicit disposition per dataset; this
    # queue counts the ones still waiting for a parser.
    _catalog = os.path.join(HERE, "..", "..", "ff-assets", "harvest", "source_catalog.yaml")
    if os.path.exists(_catalog):
        import yaml as _yaml
        with open(os.path.abspath(_catalog), encoding="utf-8") as _f:
            _spec = _yaml.safe_load(_f)
        _queued = [
            f"{_source}/{_dataset}"
            for _source, _sspec in _spec["sources"].items()
            for _dataset, _dspec in _sspec["datasets"].items()
            if _dspec.get("harvest_disposition") == "queued_no_parser"
        ]
        queues.append({"queue": "harvest_datasets_queued_without_parser",
                       "count": len(_queued), "detail": sorted(_queued)})

    # O.9.3: THE COLUMN-LEVEL GATE THAT DID NOT EXIST.
    #
    # `sources_without_mapping_obligation = 0` is a statement about the 89 SOURCES, not
    # about their ~5,600 COLUMNS -- nflcom_player_logs has 57 columns and satisfies its
    # obligation with one queue note. Until this queue exists, the programme has gated its
    # paperwork and not its material, which is the failure mode named in
    # feedback-check-the-denominator-beneath: a counter measured a layer above the truth.
    #
    # It is a QUEUE, not a zero-gate, and deliberately so: forcing it to zero would mean
    # dispositioning thousands of columns by rule, and closing a column by rule is exactly
    # what nobody may do to a stat surface. It comes down by adjudication.
    _dossier = os.path.join(HERE, "..", "..", "docs", "column-dossier.json")
    if os.path.exists(_dossier):
        with open(os.path.abspath(_dossier), encoding="utf-8") as _f:
            _dd = json.load(_f)
        _dc = _dd["counters"]
        queues.append({"queue": "column_dossier_open_rows",
                       "count": _dc["column_dossier_open_rows"],
                       "detail": {"dossier_rows": _dc["dossier_rows"],
                                  "closed_by_rule": _dc["closed_by_rule"],
                                  "regimes": _dd.get("regimes", {}),
                                  "by_lineage": _dd.get("open_rows_by_lineage", {})}})
        # A source we cannot READ contributes zero dossier rows and would otherwise make
        # the queue look smaller than the material. Counted separately so an unreadable
        # source can never present as a clean one.
        # THE GAP BETWEEN "we measured it once" AND "it cannot rot". A spec in WITNESS_MAP
        # is a value path, never a verdict -- `test_a_generated_spec_is_not_a_license` pins
        # that. Until a spec has a MEASURED row in MAPPING_LICENSES.json nothing has checked
        # whether it agrees with us, and adding specs moves the coverage number while
        # proving nothing. Queued rather than zero-gated because measuring 1,144 specs is a
        # long D: sweep, and a gate that cannot be satisfied in one session gets ignored.
        try:
            import json as _j
            import os as _o

            from . import witness_map as _W
            _specs = {(m.source_key, m.v26_col) for m in _W.WITNESS_MAP}
            if _o.path.exists(_W.LICENSES):
                with open(_W.LICENSES, encoding="utf-8") as _fh:
                    _lic = _j.load(_fh)["rows"]
            else:
                _lic = []
            _measured = {(r["source"], r["v26_col"]) for r in _lic}
            _unmeasured = sorted(f"{a}.{b}" for a, b in (_specs - _measured))
            queues.append({"queue": "witness_specs_without_a_measured_verdict",
                           "count": len(_unmeasured),
                           "detail": {"total_specs": len(_specs),
                                      "measured": len(_specs & _measured),
                                      "sample": _unmeasured[:25]}})
            # A measured disagreement that nobody has attributed. This is the queue that
            # feeds supertable repair, and it is the one the old five-value verdict
            # vocabulary could not produce at all -- every value it had blamed the mapping.
            _adj = _W.load_adjudications()
            _dis = [r for r in _lic if r.get("verdict") not in ("VALIDATED",)
                    and not str(r.get("verdict", "")).startswith(("NO-", "BROKEN"))]
            _open = [f"{r['source']}.{r['v26_col']}" for r in _dis
                     if (_adj.get((r["source"], r["v26_col"])) or {}).get(
                         "fault", "UNADJUDICATED") == "UNADJUDICATED"]
            queues.append({"queue": "measured_disagreements_unadjudicated",
                           "count": len(_open),
                           "detail": {"measured_disagreements": len(_dis),
                                      "attributed": len(_dis) - len(_open),
                                      "sample": sorted(_open)[:25]}})
        except Exception as _exc:      # never let the scoreboard die on a queue counter
            queues.append({"queue": "witness_specs_without_a_measured_verdict",
                           "count": -1, "detail": {"error": str(_exc)[:160]}})
        queues.append({"queue": "column_dossier_unreadable_sources",
                       "count": _dc["sources_unreadable"],
                       "detail": _dd.get("unreadable_sources", [])})
        # A decision pointing at a key the dossier no longer produces. The column was
        # renamed, the source re-registered, or the keying regime changed -- and the row
        # it adjudicated has silently re-opened.
        queues.append({"queue": "column_dispositions_orphaned",
                       "count": _dc.get("column_dispositions_orphaned", 0),
                       "detail": _dd.get("column_dispositions_orphaned", [])})
        # THE SHRINK, MADE AUDITABLE (2026-07-27). A MULTI_TABLE source's physical column
        # list is the UNION over its tables, so the dossier's original cross product
        # counted (table, column) pairs no table publishes -- `rating` under PUNTING,
        # `qbh` under KICKING. 4,227 of 12,499 pairs, measured. Removing them is right;
        # removing them without a number is how a queue shrinks for reasons nobody can
        # check afterwards, so the naive denominator is carried beside the real one and
        # cross_product = dossier_rows + unpublished is asserted by test.
        # OPEN because nobody MAY decide them. Their stored column names are known-wrong
        # (the O.9.0 COLUMN-SHIFT defect), so adjudicating one would launder a defect
        # into a decision that outlives its repair. Separated from the open-row count
        # because effort does not move them -- the un-shift re-parse does.
        queues.append({"queue": "column_dossier_rows_blocked_by_quarantine",
                       "count": _dc.get("column_dossier_rows_blocked_by_quarantine", 0),
                       "detail": _dd.get("quarantined_sources_in_dossier", [])})
        queues.append({"queue": "column_dossier_unpublished_pairs",
                       "count": _dc.get("column_dossier_unpublished_pairs", 0),
                       "detail": {"cross_product_pairs": _dc.get("cross_product_pairs"),
                                  "dossier_rows": _dc["dossier_rows"],
                                  "by_source": _dd.get("unpublished_pairs_by_source", {})}})
        # ...and what the open count is MADE OF. An escalated row and an unexamined row
        # are both OPEN, and one number for both invites the next session to re-derive a
        # refusal -- or, worse, to decide it the easy way because nothing said a decision
        # had already been refused there on evidence.
        from .column_escalation_census import build as escalation_build
        _esc = escalation_build()
        queues.append({"queue": "column_escalations_open",
                       "count": _esc["counters"]["escalated_rows"],
                       "detail": {"open_rows": _esc["counters"]["open_rows"],
                                  "not_yet_reached":
                                      _esc["counters"]["open_not_yet_reached"],
                                  "by_generator": _esc["escalated_by_generator"]}})

    # RULE C, the part that is legitimately nonzero: a refusal standing on live ground whose
    # PUBLISHED REASON still cites ground that has gone. Two clauses today, both in the
    # enrolled copy of the nflcom crosswalk receipt, both naming the column-shift quarantine
    # that cleared 2026-07-28. The refusal itself is fine -- it now rests on
    # signoff_ledger:promote_splits_situational -- but a reader of the contract would take
    # the dead reason at face value, which is how the refusal outlived its condition the
    # first time.
    from .refusal_preconditions import build as _refusal_build
    _rf = _refusal_build()
    queues.append({"queue": "refusal_texts_citing_a_superseded_precondition",
                   "count": _rf["counters"][
                       "refusal_texts_citing_a_superseded_precondition"],
                   "detail": _rf["stale_refusal_texts"]})
    # The human decisions refusals are allowed to wait on, counted rather than narrated.
    with open(os.path.join(CONTRACTS_DIR, "signoff_ledger.v1.json"), encoding="utf-8") as _f:
        _signoff = json.load(_f)
    queues.append({"queue": "signoff_items_pending",
                   "count": sum(1 for i in _signoff["items"] if i["status"] == "PENDING"),
                   "detail": {i["id"]: i.get("magnitude_rows")
                              for i in _signoff["items"] if i["status"] == "PENDING"}})

    # O.9 Phase 0b: PFA boxscore tables recovered from retained bytes. PFA's OWN coverage
    # declaration scopes them (docs/pfa-declared-coverage.json): only touchdowns reach
    # 1920, volume stats start in 1960, sacks in 1981, targets in 1999. So a recovered row
    # outside its statistic's declared range is a real observation but NOT a complete
    # line, and its absence is never evidence of a zero. These rows are NOT licensed to
    # vote; they are queued behind that scoping.
    _pfa = os.path.join(HERE, "..", "..", "docs", "pfa-boxscore-reparse.json")
    if os.path.exists(_pfa):
        with open(os.path.abspath(_pfa), encoding="utf-8") as _f:
            _pd = json.load(_f)
        queues.append({"queue": "pfa_boxscore_rows_pending_licensing",
                       "count": _pd["counters"].get("rows", 0),
                       "detail": sorted(_pd.get("rows_by_table", {}))})
    return queues


def build() -> dict:
    gates = _zero_gates()
    queues = _declared_queues()
    failing = [g for g in gates if g["value"] != 0]
    return {
        "scoreboard_version": "v1",
        "law": "every zero-gate at zero or the suite fails; queues are legal only "
               "as enumerated counts; every contract enrolled or the meta-gate "
               "fails (nothing can be left out silently)",
        "gate_pass": not failing,
        "failing_gates": failing,
        "zero_gates": gates,
        "declared_queues": queues,
        "v2_pending": ["atom_certificates_per_stat_grain_era (needs §18 spine)",
                       "input_hash_freshness_pinning (§24.3 weekly-update era)",
                       "mutation_test_oracle", "backfill_holdout_proofs",
                       "drift_detection_recapture_lane"],
        "v2_landed": {
            "phase0_reparse_retained_captures": "reparse_retained_captures.py (O.8f, "
                                                "2026-07-26) -- recovers columns dropped "
                                                "at capture time; queue "
                                                "reparsed_identity_rows_pending_signoff",
            "law_B_toc_derived_from_bytes": "derive_source_toc.py (O.8e, 2026-07-26) -- "
                                            "TOC mined from ~200k retained pages instead "
                                            "of asserted; queue "
                                            "source_toc_patterns_unaccounted",
            "law_A_hunt_ledger_join": "source_hunt_census.py (O.8, 2026-07-26) -- "
                                      "gate hunt_candidates_without_terminal_disposition",
            "law_A_harvest_config_ledger": "ff-assets harvest/source_catalog.yaml read as "
                                           "a discovery ledger (O.9, 2026-07-27) -- queue "
                                           "harvest_datasets_queued_without_parser; the "
                                           "capture-side gate lives in ff-assets "
                                           "tests/test_catalog_parser_coverage.py",
            "phase0b_pfa_boxscore_reparse": "pfa_boxscore_reparse.py (O.9, 2026-07-27) -- "
                                            "recovers 12 stat tables per boxscore from "
                                            "retained bytes; scoped by "
                                            "pfa_declared_coverage.py, which turns PFA's "
                                            "own per-statistic coverage table into the "
                                            "rule that its ABSENCE claims are admissible "
                                            "only inside the declared range; queue "
                                            "pfa_boxscore_rows_pending_licensing",
            "o9_3_column_dossier": "column_dossier.py (O.9.3, 2026-07-27) -- one row per "
                                   "(source, table, column), or (source, stat_name) for "
                                   "LONG sources, across all 89 registered sources. The "
                                   "three keying regimes are DETECTED by measuring "
                                   "discriminator cardinality, not declared; queue "
                                   "column_dossier_open_rows",
            "statscrew_roster_coverage_census": "statscrew_roster_coverage.py (O.9, "
                                                "2026-07-27) -- three-state census "
                                                "(HELD / EMPTY_AT_SOURCE / UNKNOWN) "
                                                "against the seed inventory; corrects "
                                                "both the 'coverage_complete' receipt and "
                                                "the '45% complete' reading of it",
            "law_B_capture_contracts": "capture_contracts.py (O.8, 2026-07-26) -- gates "
                                       "capture_contract_invalid_status_rows + "
                                       "primary_lineages_without_capture_contract; "
                                       "queue capture_contract_open_toc_entries",
        },
    }


def main() -> int:
    doc = build()
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    print(f"GATE {'PASS' if doc['gate_pass'] else 'FAIL'}")
    for g in doc["zero_gates"]:
        mark = "ok" if g["value"] == 0 else "!!"
        print(f"  [{mark}] {g['gate']} = {g['value']}")
    for q in doc["declared_queues"]:
        print(f"  [queue] {q['queue']} = {q['count']}")
    print(f"scoreboard -> {os.path.abspath(SUMMARY_PATH)}")
    return 0 if doc["gate_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
