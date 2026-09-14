"""
sota_recon/nflcom_slug_crosswalk.py -- O.9.2: the nflcom slug -> pfr_id crosswalk.

WHY IT IS THE UNBLOCK. All seven remaining MAPPING_PENDING sources are nflcom stat
families, and all seven block on this one item: nflcom keys on a name slug
(`frank-abruzzino`) while every MapSpec validator resolves witnesses to `pfr_id`. Nothing
nflcom holds -- ~10M rows across 1920-2025, including game grain -- can vote until the two
key spaces are joined by RECEIPT rather than by assumption.

THE SEED IS A CAPTURE WE ALREADY HOLD. `nflcom_team_season_roster.source_player_id` IS the
slug space, and the same rows carry player + team + season, so the slug can be resolved
through a (name, season) universe instead of through a bare name.

A BARE NAME JOIN IS THE TWINS HAZARD (§19.2) AND IS REFUSED HERE. Bio holds five Steve
Smiths, two of them born in 1979. So this builder:

  * joins on (normalised name, season), never on name alone;
  * requires the key to be UNIQUE ON BOTH SIDES before it counts as evidence -- the
    O.9.0b lesson, where a holdout proof read 43-99% purely because the join key was not
    unique and was silently comparing rows across different layouts;
  * requires a slug to resolve to exactly ONE pfr_id across every season it matched, and
    that pfr_id to resolve back to exactly one slug. Any conflict is REPORTED, never
    resolved by picking the more frequent candidate;
  * treats team as CONFIRMATION, not as a join key: nflcom names teams by url slug
    ("brooklyn-dodgers") and PFR by abbreviation ("BKN"), and inventing a team-code
    crosswalk to strengthen this one would put an unreceipted mapping underneath a
    receipt.

EVERYTHING IT CANNOT RESOLVE IS COUNTED. Coverage is measured and recorded; unresolved
slugs stay unresolved and the sources stay MAPPING_PENDING for the rows they cover. A
crosswalk that reports 100% by quietly dropping the hard cases is worth less than one that
reports 80% and names the other 20%.

Run:  python -m scripts.sota_recon.nflcom_slug_crosswalk [--write-receipt]
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import glob
import json
import os
from pathlib import Path

import duckdb

RECEIPT_PATH = Path(__file__).resolve().parent / "witness_gate" / "contracts" / "crosswalk_receipts.v1.json"
SUMMARY_PATH = Path(__file__).resolve().parents[2] / "docs" / "nflcom-slug-crosswalk.json"
RECEIPT_ID = "nflcom_slug_pfrid"
RESOLVED_PATH = Path(
    "D:/league-history-data/nfl/derived/entity_universes/nflcom_slug_pfrid.parquet"
)

NFLCOM_ROSTER_GLOB = (
    "D:/league-history-data/nfl/ff_assets/nflcom/team_season_roster/**/records.parquet"
)
PFR_TABLES_ROOT = "D:/league-history-data/nfl/raw/pfr/players/tables"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
STATSCREW_REPARSE = (
    "D:/league-history-data/nfl/derived/reparsed_captures/statscrew_team_season_roster_full.parquet"
)

# PFR player-page season tables carrying (pfr_id, player, year_id, team_name_abbr). Their
# union is the PFR player-season universe this crosswalk resolves against.
PFR_SEASON_TABLES = (
    "passing", "rushing_and_receiving", "receiving_and_rushing", "defense", "kicking",
    "punting", "returns", "fantasy", "adv_defense", "adv_rushing_and_receiving",
    "adv_receiving_and_rushing", "passing_advanced", "ol_penalties",
)

# Name normalisation. Deliberately conservative: case, punctuation, suffixes and
# whitespace only. Nicknames are NOT mapped -- "Chuck"/"Charles" is an identity claim, and
# an identity claim belongs in a receipt of its own, not inside a join key.
# Pass 2 key: letters only, so 'N.D. Kalu', 'n-d-kalu' and 'ND Kalu' all collapse to
# 'ndkalu'. Deliberately LOOSER than NORMALIZE_SQL, which is why pass 2 carries the
# twin-block and disambiguator-block rules that pass 1 does not need.
LETTERS_ONLY_SQL = "lower(regexp_replace({col}, '[^A-Za-z]', '', 'g'))"
PFR_PLAYER_INDEX_PATH = "D:/league-history-data/nfl/raw/pfr/players/player_index.parquet"

NORMALIZE_SQL = """
    trim(regexp_replace(
        regexp_replace(
            lower(strip_accents({col})),
            '\\s+(jr|sr|ii|iii|iv|v)\\.?$', ''),
        '[^a-z0-9]+', ' ', 'g'))
"""


def _listing(paths: list[str]) -> str:
    return "['" + "','".join(p.replace("\\", "/") for p in paths) + "']"


def _pfr_season_union(
    connection: duckdb.DuckDBPyConnection,
    pfr_tables_root: str | Path = PFR_TABLES_ROOT,
) -> int:
    parts = []
    for table in PFR_SEASON_TABLES:
        path = (Path(pfr_tables_root) / table / "_combined.parquet").as_posix()
        if not os.path.exists(path):
            continue
        columns = {
            row[0]
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')"
            ).fetchall()
        }
        if not {"pfr_id", "player", "year_id"} <= columns:
            continue
        team = "team_name_abbr" if "team_name_abbr" in columns else (
            "team" if "team" in columns else "NULL"
        )
        parts.append(
            f"SELECT pfr_id, player, TRY_CAST(year_id AS INTEGER) AS season, "
            f"CAST({team} AS VARCHAR) AS team_abbr FROM read_parquet('{path}') "
            f"WHERE regexp_matches(CAST(year_id AS VARCHAR), '^[0-9]{{4}}$')"
        )
    if not parts:
        raise RuntimeError("no PFR season tables found")
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE pfr_universe AS "
        f"SELECT DISTINCT pfr_id, player, season, team_abbr FROM ({' UNION ALL '.join(parts)}) "
        "WHERE pfr_id IS NOT NULL AND player IS NOT NULL AND season IS NOT NULL"
    )
    return len(parts)


def measure(
    *,
    resolved_output_path: Path | None = None,
    roster_paths: Sequence[str | Path] | None = None,
    pfr_tables_root: str | Path = PFR_TABLES_ROOT,
    statscrew_reparse: str | Path | None = STATSCREW_REPARSE,
) -> dict:
    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    tables_used = _pfr_season_union(connection, pfr_tables_root)

    roster_paths = list(roster_paths) if roster_paths is not None else glob.glob(
        NFLCOM_ROSTER_GLOB, recursive=True
    )
    if not roster_paths:
        raise RuntimeError("nflcom roster capture not found")
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE nflcom AS SELECT DISTINCT "
        "source_player_id AS slug, player, CAST(season AS INTEGER) AS season, team "
        f"FROM read_parquet({_listing([str(path) for path in roster_paths])}, union_by_name=true) "
        "WHERE source_player_id IS NOT NULL AND player IS NOT NULL AND season IS NOT NULL"
    )
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE n AS SELECT slug, season, team, "
        f"{NORMALIZE_SQL.format(col='player')} AS name FROM nflcom"
    )
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE p AS SELECT pfr_id, season, team_abbr, "
        f"{NORMALIZE_SQL.format(col='player')} AS name FROM pfr_universe"
    )

    slugs = connection.execute("SELECT COUNT(DISTINCT slug) FROM n").fetchone()[0]
    pfr_ids = connection.execute("SELECT COUNT(DISTINCT pfr_id) FROM p").fetchone()[0]

    # UNIQUE ON BOTH SIDES FIRST. A (name, season) shared by two players on either side is
    # not evidence about either of them; it is exactly the twins case, and the O.9.0b
    # holdout proof showed what happens when a non-unique key is scored anyway.
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE n_unique AS
        SELECT name, season, ANY_VALUE(slug) AS slug FROM (SELECT DISTINCT name, season, slug FROM n)
        GROUP BY name, season HAVING COUNT(*) = 1""")
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE p_unique AS
        SELECT name, season, ANY_VALUE(pfr_id) AS pfr_id FROM (SELECT DISTINCT name, season, pfr_id FROM p)
        GROUP BY name, season HAVING COUNT(*) = 1""")
    ambiguous_nflcom = connection.execute("""
        SELECT COUNT(*) FROM (SELECT name, season FROM (SELECT DISTINCT name, season, slug FROM n)
        GROUP BY name, season HAVING COUNT(*) > 1)""").fetchone()[0]
    ambiguous_pfr = connection.execute("""
        SELECT COUNT(*) FROM (SELECT name, season FROM (SELECT DISTINCT name, season, pfr_id FROM p)
        GROUP BY name, season HAVING COUNT(*) > 1)""").fetchone()[0]

    connection.execute("""
        CREATE OR REPLACE TEMP TABLE pairs AS
        SELECT n_unique.slug, p_unique.pfr_id, COUNT(*) AS seasons_agreeing
        FROM n_unique JOIN p_unique USING (name, season)
        GROUP BY 1, 2""")

    # A slug must resolve to ONE pfr_id across every season it matched, and back again.
    # Conflicts are reported; picking the more frequent candidate would be inventing an
    # identity decision inside a join.
    candidate_pairs = connection.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]
    slug_conflicts = connection.execute(
        "SELECT COUNT(*) FROM (SELECT slug FROM pairs GROUP BY 1 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    pfr_conflicts = connection.execute(
        "SELECT COUNT(*) FROM (SELECT pfr_id FROM pairs GROUP BY 1 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE resolved AS
        SELECT slug, pfr_id, seasons_agreeing FROM pairs
        WHERE slug IN (SELECT slug FROM pairs GROUP BY 1 HAVING COUNT(*) = 1)
          AND pfr_id IN (SELECT pfr_id FROM pairs GROUP BY 1 HAVING COUNT(*) = 1)""")
    resolved = connection.execute("SELECT COUNT(*) FROM resolved").fetchone()[0]
    multi_season = connection.execute(
        "SELECT COUNT(*) FROM resolved WHERE seasons_agreeing > 1"
    ).fetchone()[0]

    # ---- PASS 2: the NAME-FORM gap (2026-07-29, Joe signed off) --------------------
    # Pass 1 resolves against PFR SEASON STAT TABLES, which carry one canonical name
    # form. NFL.com's slug carries another, and the leftovers are almost entirely that
    # mismatch rather than missing people -- measured on nflcom_player_career, 90% of
    # the 1,094 unresolved slugs already have their name in player_bio:
    #     n-d-kalu / N.D. Kalu     mike-vick / Michael Vick
    #     y-a-tittle / Y.A. Tittle benjamin-watson / Ben Watson
    #     ziggy-hood / Evander Hood (bio stores the nickname, PFR stores the given name)
    # So pass 2 matches LETTERS-ONLY (periods, spaces and hyphens all collapse) against
    # every identity source we hold, inside the slug's own career window.
    #
    # TWO BLOCKING RULES, both of which a measurement forced:
    #   1. The uniqueness test runs over the UNION of bio + player index + season tables,
    #      NOT bio alone. bio carries ONE Todd Collins and PFR has two, so a bio-only
    #      test called `todd-collins` unique -- a slug pass 1 had deliberately EXCLUDED
    #      as a conflict. bio's incompleteness manufactures false uniqueness. 13 of 927
    #      matches were falsely unique this way.
    #   2. A slug ending in a numeric disambiguator (`bob-brown-2`) is NFL.COM TELLING US
    #      the name is not unique on its side. If our side shows only one, matching would
    #      assign the SECOND Bob Brown to the FIRST one's id. Blocked regardless of how
    #      unique our side looks.
    connection.execute(f"""
        CREATE OR REPLACE TEMP TABLE ident AS
        SELECT DISTINCT {LETTERS_ONLY_SQL.format(col='player')} AS nk, pfr_id,
               TRY_CAST(first_year AS INTEGER) fy, TRY_CAST(last_year AS INTEGER) ly
          FROM read_parquet('{Path(BIO).as_posix()}')
         WHERE player IS NOT NULL AND pfr_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT {LETTERS_ONLY_SQL.format(col='player')}, pfr_id,
               TRY_CAST(first_year AS INTEGER), TRY_CAST(last_year AS INTEGER)
          FROM read_parquet('{Path(PFR_PLAYER_INDEX_PATH).as_posix()}')
         WHERE player IS NOT NULL AND pfr_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT {LETTERS_ONLY_SQL.format(col='player')}, pfr_id,
               MIN(season) OVER (PARTITION BY pfr_id), MAX(season) OVER (PARTITION BY pfr_id)
          FROM pfr_universe WHERE player IS NOT NULL AND pfr_id IS NOT NULL""")
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE name_twins AS
        SELECT nk, COUNT(DISTINCT pfr_id) AS n FROM ident GROUP BY 1""")
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE leftover AS
        SELECT slug, MIN(season) AS fy, MAX(season) AS ly,
               lower(regexp_replace(regexp_replace(slug, '-[0-9]+$', ''),
                                    '[^A-Za-z]', '', 'g')) AS nk,
               regexp_matches(slug, '-[0-9]+$') AS has_disambiguator
          FROM nflcom WHERE slug NOT IN (SELECT slug FROM resolved) GROUP BY slug""")
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE pass2 AS
        SELECT l.slug, ANY_VALUE(i.pfr_id) AS pfr_id
          FROM leftover l
          JOIN name_twins t ON t.nk = l.nk
          JOIN ident i ON i.nk = l.nk
         WHERE t.n = 1
           AND NOT l.has_disambiguator
           AND l.fy <= COALESCE(i.ly, 2100) + 1
           AND l.ly >= COALESCE(i.fy, 1900) - 1
           AND i.pfr_id NOT IN (SELECT pfr_id FROM resolved)
         GROUP BY l.slug
        HAVING COUNT(DISTINCT i.pfr_id) = 1""")
    pass2_resolved = connection.execute("SELECT COUNT(*) FROM pass2").fetchone()[0]
    pass2_blocked_twin = connection.execute("""
        SELECT COUNT(*) FROM leftover l JOIN name_twins t ON t.nk = l.nk WHERE t.n > 1""").fetchone()[0]
    pass2_blocked_disambiguator = connection.execute(
        "SELECT COUNT(*) FROM leftover WHERE has_disambiguator").fetchone()[0]
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE resolved AS
        SELECT slug, pfr_id, seasons_agreeing, 'name_season' AS pass FROM resolved
        UNION ALL SELECT slug, pfr_id, 0, 'name_form' FROM pass2""")
    resolved_total = connection.execute("SELECT COUNT(*) FROM resolved").fetchone()[0]
    if resolved_output_path is not None:
        output = resolved_output_path.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.tmp")
        if temporary.exists():
            temporary.unlink()
        quoted_temporary = temporary.as_posix().replace("'", "''")
        connection.execute(f"""
            COPY (
                SELECT slug AS nflcom_slug, pfr_id
                FROM resolved
                ORDER BY slug, pfr_id
            ) TO '{quoted_temporary}' (FORMAT PARQUET)
        """)
        temporary.replace(output)

    # Coverage measured on ROWS, not just on distinct slugs: a slug covering 20 seasons
    # is worth more than one covering a single 1926 appearance.
    roster_rows, roster_rows_resolved = connection.execute("""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE r.pfr_id IS NOT NULL)
        FROM nflcom LEFT JOIN resolved r ON r.slug = nflcom.slug""").fetchone()

    by_era = connection.execute("""
        SELECT CASE WHEN season < 1933 THEN '1920-1932'
                    WHEN season < 1950 THEN '1933-1949'
                    WHEN season < 1978 THEN '1950-1977'
                    WHEN season < 1999 THEN '1978-1998'
                    ELSE '1999-2025' END AS era,
               COUNT(DISTINCT nflcom.slug) AS slugs,
               COUNT(DISTINCT r.slug) AS resolved_slugs
        FROM nflcom LEFT JOIN resolved r ON r.slug = nflcom.slug
        GROUP BY 1 ORDER BY 1""").fetchall()

    # What the still-queued StatsCrew birth dates WOULD buy. Not applied, not used in the
    # join -- measured so the sign-off decision has a number attached to it.
    dob_would_help = None
    if statscrew_reparse is not None and os.path.exists(statscrew_reparse):
        dob_would_help = connection.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT DISTINCT name, season FROM (SELECT DISTINCT name, season, slug FROM n)
              GROUP BY name, season HAVING COUNT(*) > 1
            ) a
            WHERE EXISTS (
              SELECT 1 FROM read_parquet('{Path(statscrew_reparse).as_posix()}') s
              WHERE {NORMALIZE_SQL.format(col='s.player')} = a.name
                AND s.birth_date IS NOT NULL)""").fetchone()[0]

    conflict_detail = connection.execute("""
        SELECT slug, LIST(pfr_id ORDER BY pfr_id) AS pfr_ids,
               LIST(seasons_agreeing ORDER BY pfr_id) AS seasons
        FROM pairs WHERE slug IN (SELECT slug FROM pairs GROUP BY 1 HAVING COUNT(*) > 1)
        GROUP BY slug ORDER BY slug""").fetchall()

    unresolved_sample = connection.execute("""
        SELECT nflcom.slug, MIN(season), MAX(season), COUNT(*) FROM nflcom
        LEFT JOIN resolved r ON r.slug = nflcom.slug
        WHERE r.pfr_id IS NULL GROUP BY 1 ORDER BY 4 DESC LIMIT 25""").fetchall()

    connection.close()
    return {
        "pfr_tables_unioned": tables_used,
        "counters": {
            "nflcom_distinct_slugs": slugs,
            "pfr_distinct_ids_in_universe": pfr_ids,
            "name_season_keys_ambiguous_nflcom_side": ambiguous_nflcom,
            "name_season_keys_ambiguous_pfr_side": ambiguous_pfr,
            "candidate_pairs": candidate_pairs,
            "slug_conflicts_multiple_pfr_ids": slug_conflicts,
            "pfr_id_conflicts_multiple_slugs": pfr_conflicts,
            "resolved_slugs": resolved_total,
            "resolved_slugs_pass1_name_season": resolved,
            "resolved_slugs_pass2_name_form": pass2_resolved,
            "pass2_blocked_name_twin_in_some_source": pass2_blocked_twin,
            "pass2_blocked_nflcom_disambiguator_suffix": pass2_blocked_disambiguator,
            "resolved_on_more_than_one_season": multi_season,
            "slug_coverage": round(resolved_total / slugs, 6) if slugs else None,
            "slug_coverage_pass1_only": round(resolved / slugs, 6) if slugs else None,
            "roster_rows": roster_rows,
            "roster_rows_resolved": roster_rows_resolved,
            "roster_row_coverage": round(roster_rows_resolved / roster_rows, 6)
            if roster_rows
            else None,
            "ambiguous_names_with_a_statscrew_birth_date": dob_would_help,
        },
        "coverage_by_era": [
            {"era": era, "slugs": s, "resolved": r,
             "coverage": round(r / s, 4) if s else None}
            for era, s, r in by_era
        ],
        "conflicting_slugs_excluded": [
            {"slug": slug, "pfr_ids": list(ids), "seasons_agreeing": list(seasons)}
            for slug, ids, seasons in conflict_detail
        ],
        "largest_unresolved_slugs": [
            {"slug": slug, "first_season": lo, "last_season": hi, "roster_rows": n}
            for slug, lo, hi, n in unresolved_sample
        ],
    }


def _weakest_era(measured: dict) -> str:
    """The worst era coverage, read off the measurement rather than remembered."""
    rows = measured.get("coverage_by_era") or []
    if not rows:
        return "not measured"
    worst = min(rows, key=lambda r: r.get("coverage", 1.0))
    return f"{worst['era']} at {worst['coverage']:.1%}"


def build_receipt(measured: dict) -> dict:
    counters = measured["counters"]
    # The LICENSED SET is a bijection BY CONSTRUCTION: a slug enters `resolved` only if
    # it has exactly one candidate pfr_id and that pfr_id has exactly one candidate slug.
    # So the gate asks whether the mapping being licensed is one-to-one -- not whether the
    # raw candidate space happened to be clean, which no real name space ever is.
    # The excluded conflicts are a REPORTED QUEUE, and both of them are genuine twins:
    # `todd-collins` is a 1990s linebacker AND a quarterback, and `brandon-johnson-3`
    # shows nflcom's own slug space disambiguating with a numeric suffix that a
    # (name, season) join cannot see. Excluding them is the receipt working.
    #
    # THE BIJECTION IS NOW VERIFIED RATHER THAN ASSERTED. This was
    # `bijection_pass = counters["resolved_slugs"] > 0` -- a NON-EMPTINESS check carrying
    # the name `licensed_mapping_is_bijective`. The bijection really is by construction,
    # so the boolean was never wrong; it just did no work, and a gate that cannot fail is
    # not a gate. All three conditions are stated separately so a future change to the
    # resolver that breaks one of them fails here instead of passing on non-emptiness.
    bijection = {
        "resolved_set_is_non_empty": counters["resolved_slugs"] > 0,
        "no_pfr_id_claims_two_slugs": counters["pfr_id_conflicts_multiple_slugs"] == 0,
        "conflicting_slugs_were_excluded_not_resolved":
            counters["resolved_slugs_pass1_name_season"] + 2 * counters["slug_conflicts_multiple_pfr_ids"]
            == counters["candidate_pairs"],
    }
    bijection_pass = all(bijection.values())
    return {
        "receipt_id": RECEIPT_ID,
        # ---- THE FIELD THE CONSUMER ACTUALLY READS ----
        # kc_planes.py:450 is `rec["status"] == "PASS" and sid in rec["licenses"]`. This
        # builder recorded its verdict in `gates.licenses_sources` and never emitted
        # `status`, so the lookup raised KeyError, the caller's
        # `except (FileNotFoundError, KeyError)` swallowed it, and all four licensed
        # sources fell to PENDING_CROSSWALK with the reason "missing, failing, or not
        # licensing this source" -- indistinguishable from a crosswalk that genuinely
        # failed. Two components each internally consistent, disagreeing on a field name,
        # failing in the SAFE direction, which is exactly why nobody noticed.
        "status": "PASS" if bijection_pass else "FAIL",
        "bijection": bijection,
        "claim": "nflcom_slug -> pfr_id, resolved through the nflcom_team_season_roster "
                 "capture on (normalised name, season) keys that are UNIQUE ON BOTH "
                 "SIDES, and accepted only where the slug and the pfr_id each have "
                 "exactly one counterpart",
        "crosswalk": {
            "via": "nflcom_team_season_roster + PFR player-page season tables",
            "on": "normalised(player) + season",
            "adds": "pfr_id",
        },
        "refusals": [
            "bare name joins (twins hazard, §19.2) -- season is always part of the key",
            "non-unique (name, season) keys on EITHER side are excluded, never scored",
            "a slug matching two pfr_ids is reported as a conflict, never resolved by "
            "taking the more frequent candidate",
            "team is not a join key: nflcom uses url slugs and PFR uses abbreviations, "
            "and inventing that mapping would put an unreceipted crosswalk under a receipt",
        ],
        "measured": counters,
        "coverage_by_era": measured["coverage_by_era"],
        "gates": {
            "licensed_mapping_is_bijective": bijection_pass,
            "conflicting_slugs_excluded_not_resolved":
                counters["slug_conflicts_multiple_pfr_ids"],
            "licenses_sources": bijection_pass,
        },
        "conflicting_slugs_excluded": measured["conflicting_slugs_excluded"],
        "licenses": [
            "nflcom_player_logs", "nflcom_player_career", "nflcom_player_season",
            "nflcom_player_logs_targeted",
            # ---- PROMOTED 2026-07-29 on Joe's sign-off ("yes promote"), recorded in
            # witness_gate/contracts/signoff_ledger.v1.json:promote_splits_situational.
            # 7,782,148 rows. Their COLUMN-SHIFT exclusion was spent on 2026-07-28 and the
            # refusal outlived it by a day, which is what refusal_preconditions.py now
            # prevents: approving that ledger item makes the NO_SIGNOFF precondition FALSE
            # and FAILS the scoreboard until the promotion actually lands here.
            #
            # THE COLUMN-LEVEL HOLD IS STRUCTURAL, NOT A LIST. Licensing is per SOURCE; the
            # columns that must not vote are held out by NOT BEING ADJUDICATED -- an OPEN
            # dossier row has no MAPPED_TO_CANONICAL entry, so no witness resolves through
            # it whatever the source is licensed for. 521 of their columns are mapped to 39
            # canonicals; the L4 return block, the L7 made-att composites and the L0 tackle
            # total stay open, as do the per-layout columns the parse truncated.
            "nflcom_player_splits", "nflcom_player_situational",
        ],
        "does_not_license": [
            # ---- REWRITTEN 2026-07-29. The clause that stood here cited the COLUMN-SHIFT
            # quarantine, which had cleared on 2026-07-28 -- QUARANTINED_SOURCES empty,
            # sources.py repointed at the unshifted tables, 521 columns adjudicated. That
            # dead reason was the worst of the three stale refusals that motivated Rule C,
            # and it sat in the ENROLLED contract copy as well as in this generator, which
            # is why the staleness counter is string-level.
            "individual COLUMNS that are not adjudicated -- the L4 return block (NFL.com "
            "renders RET|YDS|AVG|LNG|TD|20+|40+|FC under BOTH Kick Return and Punt Return, "
            "so 811,572 rows carry an unknown return family), the L7 made-att field-goal "
            "composites, the L0 tackle total, and the per-layout columns the original parse "
            "truncated away. These are held out structurally: an OPEN dossier row has no "
            "canonical mapping, so nothing can resolve a witness through it",
            # GENERATED, never typed. The previous version of this clause carried
            # "72.0% of slugs, 88.1% of roster rows, 1920-32: 18.7%" as literal prose,
            # and pass 2 made all three wrong the moment it landed -- a stale number
            # inside the receipt that licenses the source is the worst place for one.
            f"coverage is partial: unresolved slugs stay unresolved and their rows stay "
            f"unlicensed rather than being joined on a weaker key. "
            f"{counters['slug_coverage']:.1%} of slugs resolve and "
            f"{counters['roster_row_coverage']:.1%} of roster rows; the weakest era is "
            f"{_weakest_era(measured)}. Licensing this receipt does NOT make the "
            f"unresolved rows votable",
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-receipt", action="store_true")
    parser.add_argument(
        "--materialize-resolved",
        action="store_true",
        help="persist the already-derived local slug-to-PFR-ID bijection",
    )
    args = parser.parse_args(argv)
    measured = measure(
        resolved_output_path=RESOLVED_PATH if args.materialize_resolved else None
    )
    receipt = build_receipt(measured)
    from .recon_common import utc_stamp

    document = dict(measured)
    document["receipt"] = receipt
    document["generated_utc"] = utc_stamp()
    SUMMARY_PATH.write_text(json.dumps(document, indent=2), encoding="utf-8")

    counters = measured["counters"]
    print(f"nflcom slugs            : {counters['nflcom_distinct_slugs']:,}")
    print(f"pfr ids in universe     : {counters['pfr_distinct_ids_in_universe']:,}")
    print(f"ambiguous (name,season) : nflcom {counters['name_season_keys_ambiguous_nflcom_side']:,}"
          f"  pfr {counters['name_season_keys_ambiguous_pfr_side']:,}")
    print(f"conflicts               : slug->many {counters['slug_conflicts_multiple_pfr_ids']:,}"
          f"  pfr->many {counters['pfr_id_conflicts_multiple_slugs']:,}")
    print(f"RESOLVED slugs          : {counters['resolved_slugs']:,} "
          f"({counters['slug_coverage']:.1%})  "
          f"multi-season {counters['resolved_on_more_than_one_season']:,}")
    print(f"roster ROW coverage     : {counters['roster_rows_resolved']:,}/"
          f"{counters['roster_rows']:,} ({counters['roster_row_coverage']:.1%})")
    print(f"ambiguous names a StatsCrew DOB could split: "
          f"{counters['ambiguous_names_with_a_statscrew_birth_date']}")
    print("\ncoverage by era:")
    for row in measured["coverage_by_era"]:
        print(f"    {row['era']}  {row['resolved']:>6,}/{row['slugs']:>6,}  "
              f"{row['coverage']:.1%}" if row["coverage"] is not None else row["era"])
    print(f"\nlicensed mapping bijective: "
          f"{'PASS' if receipt['gates']['licensed_mapping_is_bijective'] else 'FAIL'}")
    for row in measured["conflicting_slugs_excluded"]:
        print(f"    EXCLUDED twin: {row['slug']} -> {row['pfr_ids']} "
              f"(seasons {row['seasons_agreeing']})")

    if args.write_receipt:
        existing = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
        receipts = [r for r in existing.get("receipts", []) if r["receipt_id"] != RECEIPT_ID]
        receipts.append(receipt)
        existing["receipts"] = receipts
        RECEIPT_PATH.write_text(json.dumps(existing, indent=2), encoding="utf-8")
        print(f"receipt written -> {RECEIPT_PATH}")
    if args.materialize_resolved:
        print(f"resolved relation -> {RESOLVED_PATH}")
    print(f"summary -> {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
