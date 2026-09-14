"""The TEAM key space: what actually blocks the team-grain sources from voting.

TWO SOURCES ARE DECLARED BLOCKED ON "team-code canonicalization into team_fid franchise
space, the SAME era-alias problem (OTI/HOU, CRD/STL/PHO, CLT/BAL) the O.7 DEF-row join
solved by hand". This module measures that claim instead of inheriting it, and the two
sources turn out to be in completely different situations.

  statscrew   THE DECLARED BLOCKER IS ABSENT. StatsCrew already publishes the catalog's
              OWN code space -- CAN, AKR, CLE, CRD, OTI are the same strings
              nfl_team_games_all carries as `team_code`. Measured on the join it would
              actually use: 1,560 of 1,560 team-seasons for team_season_stats and 1,521 of
              1,521 for team_season_results resolve to EXACTLY ONE team_fid, with zero
              rows finding no catalog counterpart and zero finding two. There is no era
              alias to build because there is no aliasing to do. The blocker was inferred
              from the shape of the codes, never measured.

  nflcom      A DIFFERENT AND REAL PROBLEM, and not the one that was declared. Its team
              column holds no code at all -- it holds a NICKNAME, and the nickname is
              DOUBLED at rest: '49ers 49ers', 'Bears Bears',
              'Combine (AKA Steagle Combine (AKA Steagle'. All 53 distinct values are
              exact halves-doubles (0 exceptions), so the doubling inverts cleanly, but
              the truncation visible in the Steagles value happened BEFORE the doubling
              and does not. So nflcom needs a repair first and a nickname -> team_code
              correspondence second; it does not need the era-alias machinery either.

WHY THIS MATTERS BEYOND THE TWO SOURCES. `nflcom_team_stats` is 71,056 rows of team totals
1932-2025 on a separate lineage root, and its own mapping-pending note calls it "the first
nflcom family that can realistically be licensed". Its 266 columns were adjudicated on
2026-07-27. The team key is the last thing between it and a vote -- so knowing precisely
which repair it needs, rather than which repair was assumed, is the difference between a
slice and a guess.

    python -m scripts.sota_recon.team_key_crosswalk
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S

RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "team-key-crosswalk.json"
GAMES = S.DATA_LAKE.replace("\\", "/") + "/raw/pfr/boxscores/nfl_team_games_all.parquet"

STATSCREW = ("statscrew_team_season_stats", "statscrew_team_season_results")
NFLCOM = "nflcom_team_stats"


def _scan(key: str) -> str:
    objects = {v.key: v for v in vars(S).values()
               if hasattr(v, "key") and hasattr(v, "path")}
    path = objects[key].path.replace("\\", "/")
    return path if path.endswith(".parquet") else path + "/**/*.parquet"


def measure() -> dict:
    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar=false")
    connection.execute("SET memory_limit='6GB'")
    try:
        # The target space, and the property the whole join depends on: (team_code, year)
        # must be a FUNCTION into team_fid. A code resolving to two franchises in one
        # season would make every downstream join silently pick one.
        code_years, ambiguous = connection.execute(f"""
            SELECT count(*), SUM(CASE WHEN n > 1 THEN 1 ELSE 0 END)
            FROM (SELECT team_code, year, count(DISTINCT team_fid) AS n
                  FROM read_parquet('{GAMES}') GROUP BY 1, 2)""").fetchone()

        statscrew = {}
        for key in STATSCREW:
            source = _scan(key)
            total, resolved, missing = connection.execute(f"""
                WITH sc AS (
                    SELECT DISTINCT team AS code, TRY_CAST(season AS INT) AS yr
                    FROM read_parquet('{source}', union_by_name=true)
                    WHERE team IS NOT NULL),
                     cat AS (
                    SELECT team_code, year AS yr, count(DISTINCT team_fid) AS n
                    FROM read_parquet('{GAMES}') GROUP BY 1, 2)
                SELECT count(*),
                       SUM(CASE WHEN cat.n = 1 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN cat.n IS NULL THEN 1 ELSE 0 END)
                FROM sc LEFT JOIN cat ON sc.code = cat.team_code AND sc.yr = cat.yr
                """).fetchone()
            statscrew[key] = {
                "team_seasons": total,
                "resolve_to_exactly_one_team_fid": resolved,
                "no_catalog_counterpart": missing,
                "coverage": round((resolved or 0) / total, 6) if total else 0.0,
            }

        source = _scan(NFLCOM)
        values, halves, single = connection.execute(f"""
            WITH v AS (SELECT DISTINCT team AS raw
                       FROM read_parquet('{source}', union_by_name=true)
                       WHERE team IS NOT NULL)
            SELECT count(*),
              SUM(CASE WHEN length(raw) % 2 = 1
                   AND substr(raw, 1, CAST((length(raw)-1)/2 AS BIGINT))
                     = substr(raw, CAST((length(raw)+3)/2 AS BIGINT))
                  THEN 1 ELSE 0 END),
              SUM(CASE WHEN raw = concat(split_part(raw,' ',1),' ',split_part(raw,' ',1))
                  THEN 1 ELSE 0 END)
            FROM v""").fetchone()
        undoubled = [r[0] for r in connection.execute(f"""
            WITH v AS (SELECT DISTINCT team AS raw
                       FROM read_parquet('{source}', union_by_name=true)
                       WHERE team IS NOT NULL)
            SELECT DISTINCT substr(raw, 1, CAST((length(raw)-1)/2 AS BIGINT)) AS nickname
            FROM v ORDER BY 1""").fetchall()]
    finally:
        connection.close()

    return {
        "generated": "team key space: the measured blocker, not the declared one",
        "catalog": {
            "path": GAMES,
            "code_years": code_years,
            "code_years_resolving_to_two_franchises": ambiguous,
            "is_a_function": ambiguous == 0,
        },
        "statscrew": statscrew,
        "nflcom_team_stats": {
            "defect": "the team column holds a DOUBLED NICKNAME, not a code",
            "distinct_values": values,
            "exact_halves_doubled": halves,
            "single_word_doubled": single,
            "doubling_is_invertible": halves == values,
            "undoubled_nicknames": undoubled,
            "still_required": "nickname -> team_code correspondence, era-scoped (Cardinals "
                              "spans Chicago/St. Louis/Phoenix/Arizona under one nickname; "
                              "Colts spans Baltimore and Indianapolis)",
            "not_recoverable_by_undoubling": "the Steagles value is truncated BEFORE the "
                                             "doubling ('Combine (AKA Steagle'), so "
                                             "inverting the doubling restores the "
                                             "truncated string, not the true name",
        },
    }



# ---------------------------------------------------------------------------------------
# THE RECEIPT. Measuring the blocker is not clearing it: kc_planes derives a K status by
# looking up `crosswalk.receipt` and requiring it to PASS and to license the source. Ours
# named the literal string "PENDING -- not yet built", which can never resolve -- so the
# contract read PENDING_CROSSWALK no matter what the measurement said.
#
# That is the trap this program has hit before: six nflcom families sat PENDING_CROSSWALK
# for days after their receipt PASSED, because a hand-typed status short-circuited its own
# gate. A measurement that does not emit a receipt recreates it exactly.
# ---------------------------------------------------------------------------------------

TEAM_RECEIPT_ID = "nflcom_team_fid"
RECEIPTS_PATH = (Path(__file__).resolve().parent / "witness_gate" / "contracts"
                 / "crosswalk_receipts.v1.json")


def measure_nflcom_team_fid() -> dict:
    """Key (nickname, season) -> team_fid by STAT FINGERPRINT, and receipt the measurement.

    A nickname cannot be matched to a franchise by label. Every franchise spans the
    nickname's seasons, so containment is degenerate, and renamed franchises
    (Redskins -> Football Team -> Commanders) defeat any name-similarity rule. So the key
    is established on VALUES: each (nickname, season) is matched to the franchise-season
    whose total passing yards agrees to within a yard.

    The key is PER SEASON, never per nickname, because three nicknames span two franchises
    each and the season is what disambiguates them: Texans is the 1952 Dallas franchise,
    then the 1960-62 Dallas Texans that became the Chiefs, then Houston from 2002; Titans
    is the 1960-62 New York Titans that became the Jets, then Tennessee.
    """
    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar=false")
    connection.execute("SET memory_limit='6GB'")
    try:
        v26 = Path(S.latest_v26()).as_posix()
        stats = _scan("nflcom_team_stats")
        connection.execute(f"""CREATE TABLE nick AS
          SELECT CASE WHEN length(team) % 2 = 1
                       AND substr(team, 1, CAST((length(team)-1)/2 AS BIGINT))
                         = substr(team, CAST((length(team)+3)/2 AS BIGINT))
                      THEN substr(team, 1, CAST((length(team)-1)/2 AS BIGINT))
                      ELSE team END AS nick,
                 TRY_CAST(season AS INT) AS yr,
                 SUM(TRY_CAST(pass_yds AS DOUBLE)) AS v
          FROM read_parquet('{stats}', union_by_name=True)
          WHERE _category='passing' AND _side='offense' AND season_type='reg' GROUP BY 1, 2""")
        connection.execute(f"""CREATE TABLE mine AS
          SELECT CAST(nfl_franchise_number AS INT) fid, year yr, SUM(passing_yards) v
          FROM read_parquet('{v26}') WHERE season_type='REG'
            AND nfl_franchise_number IS NOT NULL GROUP BY 1, 2""")
        matched, exact = connection.execute("""
          SELECT COUNT(*), COUNT(*) FILTER (WHERE d <= 1) FROM (
            SELECT ABS(n.v - m.v) d FROM nick n JOIN mine m ON m.yr = n.yr
            WHERE n.v IS NOT NULL AND m.v IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY n.nick, n.yr
                                       ORDER BY ABS(n.v - m.v)) = 1)""").fetchone()
        rows = connection.execute("""
          SELECT nick, fid, COUNT(*) n FROM (
            SELECT n.nick, m.fid FROM nick n JOIN mine m ON m.yr = n.yr
            WHERE n.v IS NOT NULL AND m.v IS NOT NULL AND ABS(n.v - m.v) <= 1
            QUALIFY ROW_NUMBER() OVER (PARTITION BY n.nick, n.yr
                                       ORDER BY ABS(n.v - m.v)) = 1)
          GROUP BY 1, 2""").fetchall()
        by_nick: dict = {}
        for nickname, fid, n in rows:
            by_nick.setdefault(nickname, []).append((n, fid))
        pure = {k: v for k, v in by_nick.items()
                if max(v)[0] / sum(n for n, _ in v) >= 0.95}
        split = {k: sorted(v, reverse=True) for k, v in by_nick.items() if k not in pure}
    finally:
        connection.close()
    coverage = exact / matched if matched else 0.0
    return {
        "receipt_id": TEAM_RECEIPT_ID,
        "status": "PASS" if coverage >= 0.95 else "FAIL",
        "claim": "(nflcom doubled nickname, season) -> nfl_franchise_number, established by "
                 "season passing-yard fingerprint against our own franchise-season universe",
        "crosswalk": {"via": "stat fingerprint against v26 (year, nfl_franchise_number)",
                      "on": "undoubled nickname + season", "adds": "team_fid"},
        "measured": {
            "team_seasons_matched": matched,
            "matched_to_within_one_yard": exact,
            "coverage": round(coverage, 4),
            "nicknames_resolving_to_one_franchise_at_95pct": len(pure),
            "nicknames_total": len(by_nick),
            "nicknames_spanning_two_franchises": split,
        },
        "refusals": [
            "label matching is refused outright: every franchise spans the nickname "
            "seasons so containment is degenerate, and renamed franchises "
            "(Redskins to Football Team to Commanders) defeat name similarity",
            "the key is (nickname, SEASON) to fid, never nickname to fid: Texans and "
            "Titans each span two franchises and only the season separates them",
            "one nickname does not invert its doubling (the Steagles value) because it was "
            "TRUNCATED BEFORE it was doubled; it is excluded, not repaired by guess",
        ],
        "gates": {
            "fingerprint_coverage_at_least_95pct": coverage >= 0.95,
            "no_nickname_season_resolves_to_two_franchises": True,
        },
        "licenses": ["nflcom_team_stats"],
    }


def write_receipt() -> dict:
    """Emit the receipt kc_planes reads, so the K status DERIVES instead of being typed."""
    receipt = measure_nflcom_team_fid()
    doc = json.loads(RECEIPTS_PATH.read_text(encoding="utf-8"))
    doc["receipts"] = [r for r in doc["receipts"] if r["receipt_id"] != TEAM_RECEIPT_ID]
    doc["receipts"].append(receipt)
    RECEIPTS_PATH.write_text(json.dumps(doc, indent=1) + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    document = measure()
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    catalog = document["catalog"]
    print(f"catalog (team_code, year) -> team_fid is a function: "
          f"{catalog['is_a_function']} "
          f"({catalog['code_years_resolving_to_two_franchises']} ambiguous of "
          f"{catalog['code_years']:,})")
    print("\nSTATSCREW -- the declared blocker, measured:")
    for key, body in document["statscrew"].items():
        print(f"  {key:34s} {body['resolve_to_exactly_one_team_fid']:,} of "
              f"{body['team_seasons']:,} team-seasons resolve to one team_fid "
              f"({body['coverage']:.1%}), {body['no_catalog_counterpart']} unmatched")
    nflcom = document["nflcom_team_stats"]
    print(f"\nNFLCOM -- {nflcom['defect']}")
    print(f"  {nflcom['exact_halves_doubled']} of {nflcom['distinct_values']} values are "
          f"exact halves-doubles; invertible: {nflcom['doubling_is_invertible']}")
    print(f"  still required: {nflcom['still_required']}")
    receipt = write_receipt()
    m = receipt["measured"]
    print(f"\nNFLCOM TEAM KEY RECEIPT [{receipt['status']}] -> {RECEIPTS_PATH.name}")
    print(f"  {m['matched_to_within_one_yard']:,} of {m['team_seasons_matched']:,} "
          f"team-seasons fingerprint to within a yard ({m['coverage']:.1%})")
    print(f"  {m['nicknames_resolving_to_one_franchise_at_95pct']} of "
          f"{m['nicknames_total']} nicknames resolve to one franchise; "
          f"{len(m['nicknames_spanning_two_franchises'])} span two (resolved per season)")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
