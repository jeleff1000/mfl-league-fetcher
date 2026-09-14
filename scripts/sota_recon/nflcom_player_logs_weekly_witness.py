"""Measure NFL.com player-log mappings at player-game grain.

This is a measurement lane only.  It does not alter v26, the disposition ledger, or
MAPPING_LICENSES.  The captured player_logs parquet lost the position-group axis, so rows
whose numeric occupancy signature is not unique remain pending.  Season aggregation is
intentionally not used here: the source is a game log and the subject witness is weekly.
"""

from __future__ import annotations

import json
from statistics import median
from pathlib import Path

import duckdb

from .build_pre1978_fumbles_lost_v26 import NICKNAME_CODES
from .nflcom_player_logs_capture_inventory import (
    PLAYER_LOG_BLOCK_RECOVERY,
    RAW_VALUE_COLUMNS,
    mirror_layout_recovery_query,
    raw_numeric_expression,
)
from .sources import NFLCOM_SLUG_PFRID, PLAYER_BIO, latest_v26


LEDGER = Path(__file__).with_name("witness_gate") / "contracts" / "column_dispositions.v1.json"
OUT = (Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") /
       "nflcom_player_logs_weekly_witness_2025.json")
TARGETED_OUT = (Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") /
                "nflcom_player_logs_targeted_weekly_witness.json")
ALL_YEARS_OUT = (Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") /
                 "nflcom_player_logs_all_years_weekly_witness.json")
LOGS = Path(r"D:/league-history-data/nfl/raw/nflcom/tables/player_logs")
TARGETED_LOGS = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_targeted_normalized.parquet"
)

SCOPES = {
    "postseason": {"source_table": "Post Season", "season_type": "POST", "offset": 18},
    "regular": {"source_table": "Regular Season", "season_type": "REG", "offset": 0},
}
TARGETED_SCOPES = {
    "postseason": {"source_table": "Post Season", "season_type": "POST", "offset": 0},
    "regular": {"source_table": "Regular Season", "season_type": "REG", "offset": 0},
}

TOLERANCE_BY_DECIMALS = {1: 0.06, 2: 0.006, 3: 0.0006}

RATE_SPECS = {
    ("QB", "avg"): ("passing_yards", "attempts", 1.0),
    ("QB", "avg_2"): ("rushing_yards", "carries", 1.0),
    ("QB", "rate"): ("passer_rating", None, 1.0),
    ("RBFB5", "avg"): ("rushing_yards", "carries", 1.0),
    ("RBFB5", "avg_2"): ("receiving_yards", "receptions", 1.0),
    ("WRTE", "avg"): ("receiving_yards", "receptions", 1.0),
    ("WRTE", "avg_2"): ("rushing_yards", "carries", 1.0),
    ("K_log", "pct"): ("fg_made", "fg_att", 100.0),
    ("K_log", "pct_2"): ("pat_made", "pat_att", 100.0),
    ("P", "avg"): ("punt_yards", "punts", 1.0),
}


def _defense_era(year: str | int) -> str:
    """Use the audit's recording-era boundaries for defensive source fields."""
    value = int(year)
    if value < 1982:
        return "<1982"
    if value < 1994:
        return "1982-93"
    if value < 2000:
        return "1994-99"
    if value < 2010:
        return "2000-09"
    return "2010+"


def _q(path: str | Path) -> str:
    return str(path).replace("'", "''")


def _source_glob(path: str | Path) -> str:
    path = Path(path)
    return str(path) if path.suffix == ".parquet" else f"{path}/**/*.parquet"


def _target_join_predicate(
    targeted: bool, season_type: str, offset: int, date_keyed: bool = False
) -> str:
    if targeted or date_keyed:
        return (
            "v.NFL_player_id=i.NFL_player_id "
            "AND v.year=TRY_CAST(s.season AS INT) "
            "AND CAST(v.game_date AS DATE)=s.parsed_date "
            f"AND v.season_type='{season_type}' "
            "AND v.opponent_nfl_team IN ("
            "SELECT code FROM targeted_opp_codes "
            "WHERE label=regexp_replace(s.opp, '^@', ''))"
        )
    return (
        "v.NFL_player_id=i.NFL_player_id "
        f"AND v.week=TRY_CAST(s.wk AS INT)+{offset} "
        "AND v.year=2025 "
        f"AND v.season_type='{season_type}'"
    )


def _layout_expression(targeted: bool, date_keyed: bool = False) -> str:
    return "replace(_layout, 'id_gamelog:', '')" if targeted else PLAYER_LOG_BLOCK_RECOVERY


def _decimals(raw: str) -> int:
    text = str(raw).strip()
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1])


def _close_to_published(raw: str, expected: float) -> bool:
    # This is the established source-precision law used by audit_capacity_runner. NFL.com
    # has a small truncation/rounding edge around the half-unit, so a strict half-unit
    # test manufactures failures such as 8.4 versus 262/31 = 8.4516.
    tolerance = TOLERANCE_BY_DECIMALS.get(_decimals(raw), 0.06)
    return abs(float(raw) - expected) <= tolerance


def _passer_rating(comp: float, att: float, yds: float, td: float, interceptions: float) -> float | None:
    if att <= 0:
        return None
    a = (comp / att - 0.3) * 5
    b = (yds / att - 3.0) * 0.25
    c = (td / att) * 20.0
    d = 2.375 - (interceptions / att) * 25.0
    return (sum(max(0.0, min(2.375, x)) for x in (a, b, c, d)) / 6.0) * 100.0


def _mapped_columns(source_name: str = "nflcom_player_logs") -> dict[str, dict[str, str]]:
    raw = json.loads(LEDGER.read_text(encoding="utf-8"))
    rows = raw["decisions"] if isinstance(raw, dict) and "decisions" in raw else raw
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        key = row.get("key", "")
        prefix = f"{source_name}|"
        if not key.startswith(prefix) or row.get("disposition") != "MAPPED_TO_CANONICAL":
            continue
        _, table, layout_key, source_column = key.split("|", 3)
        if table not in ("Post Season", "Regular Season"):
            continue
        layout = layout_key.removeprefix("id_gamelog:")
        canonical = row.get("canonical")
        if canonical:
            out.setdefault(table + "|" + layout, {})[source_column] = canonical
    return out


def measure_fumble_candidate_matrix(
    con: duckdb.DuckDBPyConnection,
    v26: str,
    join_predicate: str,
) -> list[dict]:
    """Compare fumble candidates, including a shared target-availability denominator.

    The component columns are much sparser than the generic total.  Their raw agreement
    rate therefore cannot be compared to the generic rate directly.  The common matrix
    below scores every candidate on exactly the rows where all candidate targets exist.
    """
    rows = con.execute(f"""WITH joined AS (
          SELECT s.recovered_layout AS layout,
                 TRY_CAST(s.fum AS DOUBLE) AS source_value,
                 v.fumbles,
                 CASE WHEN v.rushing_fumbles IS NOT NULL
                           AND v.receiving_fumbles IS NOT NULL
                      THEN v.rushing_fumbles + v.receiving_fumbles END AS rush_rec,
                 CASE WHEN v.rushing_fumbles IS NOT NULL
                           AND v.receiving_fumbles IS NOT NULL
                           AND v.sack_fumbles IS NOT NULL
                      THEN v.rushing_fumbles + v.receiving_fumbles + v.sack_fumbles END AS rush_rec_sack
          FROM src_scope s
          JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
          JOIN read_parquet('{_q(v26)}') v ON v.NFL_player_id=i.NFL_player_id
           AND {join_predicate}
          WHERE s.recovered_layout IN ('QB', 'RBFB5', 'WRTE')
            AND TRY_CAST(s.fum AS DOUBLE) IS NOT NULL
        )
        SELECT layout, source_value, fumbles, rush_rec, rush_rec_sack
        FROM joined ORDER BY 1""").fetchall()
    grouped: dict[str, list[tuple[float | None, float | None, float | None, float | None]]] = {}
    for layout, source_value, fumbles, rush_rec, rush_rec_sack in rows:
        grouped.setdefault(layout, []).append(
            (source_value, fumbles, rush_rec, rush_rec_sack)
        )
    result = []
    forms = (("fumbles", 1), ("rush+rec", 2), ("rush+rec+sack", 3))
    for layout, triples in sorted(grouped.items()):
        common = [row for row in triples if all(value is not None for value in row[1:])]
        common_scores = {}
        for form, index in forms:
            pairs = [(row[0], row[index]) for row in triples]
            common_pairs = [(row[0], row[index]) for row in common]
            scored = score_candidate_pairs(pairs)
            common_scored = score_candidate_pairs(common_pairs)
            scored["agree_pct"] = (
                round(100.0 * scored["agree_n"] / scored["informative_n"], 2)
                if scored["informative_n"] else None
            )
            common_scored["agree_pct"] = (
                round(100.0 * common_scored["agree_n"] / common_scored["informative_n"], 2)
                if common_scored["informative_n"] else None
            )
            common_scores[form] = common_scored
            result.append({"layout": layout, "form": form, **scored})
        for row in result[-len(forms):]:
            row["common_target_available_n"] = len(common)
            row["common_target_score"] = common_scores[row["form"]]
            if row["form"] == "fumbles":
                row["source_nonzero_missing_target_n"] = sum(
                    int(source is not None and source > 0 and target is None)
                    for source, target, _, _ in triples
                )
    return result


def measure_layout_independent_fumble_candidate(
    con: duckdb.DuckDBPyConnection,
    v26: str,
    join_predicate: str,
) -> list[dict]:
    """Measure fumble-only rows whose source fields are shared by all offensive layouts.

    QB, RBFB5, and WRTE all expose ``fum/lost`` as the offensive fumble pair; DEF uses
    ``ff/fr`` instead.  A row with only the four duplicated fumble fields therefore does
    not need a position layout, provided the deduplicated source game key is unique.
    This lane is deliberately separate from layout-specific mappings and never assigns a
    position group to the row. It is a candidate diagnostic, not a witness licence.
    """
    other_columns = [
        column for column in RAW_VALUE_COLUMNS
        if column not in {"fum", "lost", "fumbles", "fumbles_lost"}
    ]
    only_fumbles = " AND ".join(
        f'TRY_CAST("{column}" AS DOUBLE) IS NULL' for column in other_columns
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE fumble_only AS
        SELECT DISTINCT nflcom_slug, season, parsed_date, opp,
               TRY_CAST(fum AS DOUBLE) AS fum,
               TRY_CAST(lost AS DOUBLE) AS lost,
               TRY_CAST(fumbles AS DOUBLE) AS fumbles,
               TRY_CAST(fumbles_lost AS DOUBLE) AS fumbles_lost
        FROM src_scope
        WHERE recovered_layout IS NULL
          AND TRY_CAST(fum AS DOUBLE) IS NOT NULL
          AND TRY_CAST(lost AS DOUBLE) IS NOT NULL
          AND TRY_CAST(fumbles AS DOUBLE) IS NOT NULL
          AND TRY_CAST(fumbles_lost AS DOUBLE) IS NOT NULL
          AND {only_fumbles}"""
    )
    _assert_unique(
        con,
        """SELECT nflcom_slug, season, parsed_date, opp, COUNT(*)
           FROM fumble_only
           GROUP BY 1, 2, 3, 4
           HAVING COUNT(*) > 1""",
        "layout-independent fumble source game",
    )
    values = con.execute(
        f"""SELECT s.fum, s.lost, v.fumbles, v.fumbles_lost
        FROM fumble_only s
        JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
        JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
         AND {join_predicate}"""
    ).fetchall()
    rows = []
    for source_column, target_column, index in (
        ("fum", "fumbles", 0),
        ("lost", "fumbles_lost", 1),
    ):
        target_index = index + 2
        pairs = [(pair[index], pair[target_index]) for pair in values]
        scored = score_candidate_pairs(pairs)
        scored.update(
            {
                "layout": "OFFENSE_SHARED_FUMBLE_ONLY",
                "source_column": source_column,
                "canonical": target_column,
                "status": "PROOF_PENDING_DATA_GAP",
                "source_rows": con.execute("SELECT COUNT(*) FROM fumble_only").fetchone()[0],
                "identity_joined_n": len(values),
            }
        )
        scored["agree_pct"] = (
            round(100.0 * scored["agree_n"] / scored["informative_n"], 2)
            if scored["informative_n"]
            else None
        )
        rows.append(scored)
    return rows


def _assert_unique(con: duckdb.DuckDBPyConnection, sql: str, label: str) -> None:
    bad = con.execute(sql).fetchall()
    if bad:
        raise ValueError(f"{label} key collision: {bad[:5]}")


def score_candidate_pairs(rows: list[tuple[float | None, float | None]]) -> dict[str, int]:
    """Score a candidate while excluding unknown targets from the informative base."""
    result = {
        "joined_n": len(rows),
        "target_available_n": 0,
        "informative_n": 0,
        "agree_n": 0,
        "source_exceeds_target": 0,
        "target_exceeds_source": 0,
        "source_nonzero_n": 0,
        "target_nonzero_n": 0,
    }
    for source_value, target_value in rows:
        if source_value is not None and source_value > 0:
            result["source_nonzero_n"] += 1
        if target_value is not None:
            result["target_available_n"] += 1
            if target_value > 0:
                result["target_nonzero_n"] += 1
        if target_value is None or source_value is None:
            continue
        if source_value == 0 and target_value == 0:
            continue
        result["informative_n"] += 1
        if source_value == target_value:
            result["agree_n"] += 1
        elif source_value > target_value:
            result["source_exceeds_target"] += 1
        else:
            result["target_exceeds_source"] += 1
    return result


def measure_defense_candidate_matrix(
    con: duckdb.DuckDBPyConnection,
    v26: str,
    join_predicate: str,
) -> list[dict]:
    """Measure DEF_log candidates by recording era on the weekly witness.

    ``total`` has two existing numeric candidates: the derived combined tackle field and
    the legacy ``with_assist`` field.  The source identity total=solo+assist is retained
    as a control.  The other three fields are measured against their semantic canonical;
    era stratification keeps a pre-coverage population gap from looking like remapping
    evidence.
    """
    rows = con.execute(f"""SELECT TRY_CAST(s.season AS INT) AS year,
                 TRY_CAST(s.total AS DOUBLE) AS total,
                 TRY_CAST(s.solo AS DOUBLE) AS solo,
                 TRY_CAST(s.ast AS DOUBLE) AS ast,
                 TRY_CAST(s.pdef AS DOUBLE) AS pdef,
                 v.def_tackles_combined,
                 v.def_tackles_with_assist,
                 v.def_tackles_solo,
                 v.def_tackle_assists,
                 v.def_pass_defended,
                 TRY_CAST(s.fr AS DOUBLE) AS fr,
                 v.fumble_recovery_opp,
                 v.fumble_recovery_own
          FROM src_scope s
          JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
          JOIN read_parquet('{_q(v26)}') v ON v.NFL_player_id=i.NFL_player_id
           AND {join_predicate}
          WHERE s.recovered_layout='DEF_log'
            AND (TRY_CAST(s.total AS DOUBLE) IS NOT NULL
              OR TRY_CAST(s.solo AS DOUBLE) IS NOT NULL
              OR TRY_CAST(s.ast AS DOUBLE) IS NOT NULL
              OR TRY_CAST(s.pdef AS DOUBLE) IS NOT NULL
              OR TRY_CAST(s.fr AS DOUBLE) IS NOT NULL)""").fetchall()
    candidates = {
        "total": {
            "def_tackles_combined": lambda r: r[5],
            "def_tackles_with_assist": lambda r: r[6],
            "source_solo_plus_ast": lambda r: (
                r[7] + r[8] if r[7] is not None and r[8] is not None else None
            ),
        },
        "solo": {"def_tackles_solo": lambda r: r[7]},
        "ast": {"def_tackle_assists": lambda r: r[8]},
        "pdef": {"def_pass_defended": lambda r: r[9]},
        "fr": {
            "fumble_recovery_opp": lambda r: r[11],
            "fumble_recovery_own": lambda r: r[12],
        },
    }
    source_indices = {"total": 1, "solo": 2, "ast": 3, "pdef": 4, "fr": 10}
    result = []
    for source_column, targets in candidates.items():
        source_index = source_indices[source_column]
        for form, target_fn in targets.items():
            for era in ("<1982", "1982-93", "1994-99", "2000-09", "2010+"):
                scoped = [
                    r for r in rows
                    if _defense_era(r[0]) == era and r[source_index] is not None
                ]
                pairs = [
                    (r[source_index], target_fn(r))
                    for r in scoped
                ]
                scored = score_candidate_pairs(pairs)
                scored["agree_pct"] = (
                    round(100.0 * scored["agree_n"] / scored["informative_n"], 2)
                    if scored["informative_n"] else None
                )
                result.append({
                    "source_column": source_column,
                    "candidate": form,
                    "era": era,
                    **scored,
                })
    return result


def measure_defense_source_identity(
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Record the source-layer definition of DEF ``total``.

    This is deliberately independent of v26: it tests the source's candidate combined
    tackle decomposition against its own solo and assisted components. Keeping the
    complete-row denominator and both failure directions in the receipt prevents a low
    subject agreement rate from being mistaken for evidence that ``total`` means solo
    tackles.
    """
    rows = con.execute(
        """SELECT TRY_CAST(season AS INT) AS year,
                  TRY_CAST(total AS DOUBLE) AS total,
                  TRY_CAST(solo AS DOUBLE) AS solo,
                  TRY_CAST(ast AS DOUBLE) AS ast
           FROM src_scope
           WHERE recovered_layout='DEF_log'
             AND TRY_CAST(total AS DOUBLE) IS NOT NULL
             AND TRY_CAST(solo AS DOUBLE) IS NOT NULL
             AND TRY_CAST(ast AS DOUBLE) IS NOT NULL"""
    ).fetchall()
    grouped: dict[str, list[tuple[float, float, float]]] = {}
    for year, total, solo, ast in rows:
        grouped.setdefault(_defense_era(year), []).append(
            (float(total), float(solo), float(ast))
        )
    result = []
    for era in ("<1982", "1982-93", "1994-99", "2000-09", "2010+"):
        scoped = grouped.get(era, [])
        result.append({
            "era": era,
            "complete_n": len(scoped),
            "agree_n": sum(int(total == solo + ast) for total, solo, ast in scoped),
            "source_exceeds_parts": sum(
                int(total > solo + ast) for total, solo, ast in scoped
            ),
            "parts_exceed_source": sum(
                int(total < solo + ast) for total, solo, ast in scoped
            ),
        })
    return result


def measure_scope(
    con: duckdb.DuckDBPyConnection,
    scope_name: str,
    spec: dict,
    mappings: dict[str, dict[str, str]],
    *,
    source_path: Path = LOGS,
    targeted: bool = False,
    date_keyed: bool = False,
) -> dict:
    source_table = spec["source_table"]
    offset = spec["offset"]
    season_type = spec["season_type"]
    source_filter = f"_table='{source_table}'"
    if not targeted and not date_keyed:
        source_filter = "season='2025' AND " + source_filter
    source_expr = _source_glob(source_path)
    source_select = "*"
    if targeted or date_keyed:
        source_select += ", TRY_STRPTIME(game_date, '%m/%d/%Y') AS parsed_date"
    join_predicate = _target_join_predicate(targeted, season_type, offset, date_keyed)
    if targeted or date_keyed:
        con.execute("DROP TABLE IF EXISTS targeted_opp_codes")
        con.execute("CREATE TEMP TABLE targeted_opp_codes(label VARCHAR, code VARCHAR)")
        codes = [(label, code) for label, values in NICKNAME_CODES.items() for code in values]
        codes.append(("Braves", "BOS"))
        con.executemany("INSERT INTO targeted_opp_codes VALUES (?, ?)", codes)
    con.execute("DROP TABLE IF EXISTS src_scope")
    source_sql = (
        f"SELECT {source_select} FROM read_parquet('{_q(source_expr)}', union_by_name=true) "
        f"WHERE {source_filter}"
    )
    if targeted:
        # The retained targeted cache preserves the page's exact layout axis.  Do not
        # re-infer it through the raw-capture occupancy classifier: that classifier is
        # intentionally lossy for the ordinary player_logs capture and would turn valid
        # targeted rows into false unresolved residue.
        con.execute(
            "CREATE TEMP TABLE src_scope AS SELECT *, "
            "replace(_layout, 'id_gamelog:', '') AS recovered_layout, "
            "'TARGETED_LAYOUT' AS layout_recovery_source FROM ("
            + source_sql
            + ")"
        )
    else:
        con.execute(
            "CREATE TEMP TABLE src_scope AS SELECT * FROM ("
            + mirror_layout_recovery_query(source_sql)
            + ")"
        )
    source_columns = {
        row[0] for row in con.execute("DESCRIBE src_scope").fetchall()
    }
    con.execute("DROP TABLE IF EXISTS ident_scope")
    con.execute(f"""CREATE TEMP TABLE ident_scope AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(NFLCOM_SLUG_PFRID.path)}') x
        JOIN read_parquet('{_q(PLAYER_BIO.path)}') b ON b.pfr_id=x.pfr_id""")
    source_key = (
        "nflcom_slug, season, parsed_date, opp, recovered_layout"
        if date_keyed else (
            "nflcom_slug, season, game_date, opp, recovered_layout"
            if targeted else "nflcom_slug, wk, game_date, opp, recovered_layout"
        )
    )
    _assert_unique(con, f"""SELECT {source_key}, COUNT(*) n
        FROM src_scope WHERE recovered_layout IS NOT NULL
        GROUP BY {', '.join(str(i) for i in range(1, 6))} HAVING COUNT(*) > 1""", "source player-game-block")
    _assert_unique(con, """SELECT nflcom_slug, COUNT(DISTINCT NFL_player_id) n
        FROM ident_scope GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id) > 1""", "identity")
    target_key = (
        "NFL_player_id, game_date, opponent_nfl_team"
        if targeted or date_keyed else "NFL_player_id, week"
    )
    target_filter = (
        f"year=2025 AND season_type='{season_type}' AND game_date IS NOT NULL"
        if not targeted and not date_keyed
        else f"season_type='{season_type}' AND game_date IS NOT NULL"
    )
    _assert_unique(con, f"""SELECT {target_key}, COUNT(*) n
        FROM read_parquet('{_q(latest_v26())}')
        WHERE {target_filter}
        GROUP BY {', '.join(str(i) for i in range(1, 4 if targeted or date_keyed else 3))}
        HAVING COUNT(*) > 1""", "weekly subject")

    total_rows, game_keys, classified_rows, unresolved_rows, unresolved_numeric_rows = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT nflcom_slug||'|'||wk||'|'||COALESCE(game_date,'')||'|'||COALESCE(opp,'')),
               COUNT(*) FILTER (WHERE recovered_layout IS NOT NULL),
               COUNT(*) FILTER (WHERE recovered_layout IS NULL),
               COUNT(*) FILTER (WHERE recovered_layout IS NULL AND ({raw_numeric_expression(source_columns)}))
        FROM src_scope""").fetchone()
    layout_recovery_counts = [
        {"source": source or "UNRESOLVED", "rows": int(rows)}
        for source, rows in con.execute(
            """SELECT layout_recovery_source, COUNT(*)
            FROM src_scope GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    ]
    identity_rows = con.execute("""
        SELECT COUNT(*) FROM src_scope s JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
        WHERE s.recovered_layout IS NOT NULL""").fetchone()[0]
    target_join_rows = con.execute(f"""
        SELECT COUNT(*)
        FROM src_scope s JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
        JOIN read_parquet('{_q(latest_v26())}') v ON v.NFL_player_id=i.NFL_player_id
         AND {join_predicate}
         WHERE s.recovered_layout IS NOT NULL""").fetchone()[0]
    target_unjoined_rows = int(classified_rows - target_join_rows)
    mapped = {k.split("|", 1)[1]: v for k, v in mappings.items()
              if k.startswith(source_table + "|")}
    rows = []
    v26 = _q(latest_v26())
    for layout, columns in sorted(mapped.items()):
        for source_column, canonical in sorted(columns.items()):
            if source_column == "game_date":
                continue
            rate_spec = RATE_SPECS.get((layout, source_column))
            if rate_spec:
                numerator, denominator, scale = rate_spec
                select_cols = [numerator]
                if denominator:
                    select_cols.append(denominator)
                if source_column == "rate":
                    select_cols = ["completions", "attempts", "passing_yards",
                                   "passing_tds", "passing_interceptions"]
                expr = ", ".join(f"v.\"{c}\"" for c in select_cols)
                q = f"""SELECT s.\"{source_column}\", {expr}
                    FROM src_scope s JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
                    JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
                     AND {join_predicate}
                    WHERE s.recovered_layout='{layout}'
                      AND TRY_CAST(s.\"{source_column}\" AS DOUBLE) IS NOT NULL"""
                values = con.execute(q).fetchall()
                informative = exact = 0
                source_exceeds = target_exceeds = 0
                source_values = []
                expected_values = []
                for value in values:
                    raw, *parts = value
                    if any(part is None for part in parts):
                        continue
                    if source_column == "rate":
                        expected = _passer_rating(*map(float, parts))
                    else:
                        num = float(parts[0])
                        den = float(parts[1]) if denominator else None
                        if denominator and (den is None or den == 0):
                            continue
                        expected = num / den * scale if denominator else num
                    if expected is None:
                        continue
                    informative += 1
                    exact += int(_close_to_published(raw, expected))
                    source_value = float(raw)
                    source_values.append(source_value)
                    expected_values.append(expected)
                    source_exceeds += int(source_value > expected)
                    target_exceeds += int(source_value < expected)
                rows.append({"layout": layout, "source_column": source_column,
                             "canonical": canonical, "witness": "RECOMPUTE",
                             "joined": len(values), "informative_n": informative,
                             "agree_n": exact, "source_exceeds_expected": source_exceeds,
                             "expected_exceeds_source": target_exceeds,
                             "median_source": median(source_values) if source_values else None,
                             "median_expected": median(expected_values) if expected_values else None})
            else:
                q = f"""SELECT TRY_CAST(s.\"{source_column}\" AS DOUBLE), v.\"{canonical}\"
                    FROM src_scope s JOIN ident_scope i ON i.nflcom_slug=s.nflcom_slug
                    JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
                     AND {join_predicate}
                    WHERE s.recovered_layout='{layout}'
                      AND TRY_CAST(s.\"{source_column}\" AS DOUBLE) IS NOT NULL"""
                values = con.execute(q).fetchall()
                informative = sum(int(a is not None and b is not None) for a, b in values)
                exact = sum(int(a is not None and b is not None and float(a) == float(b))
                            for a, b in values)
                source_values = [float(a) for a, b in values if a is not None and b is not None]
                target_values = [float(b) for a, b in values if a is not None and b is not None]
                rows.append({"layout": layout, "source_column": source_column,
                             "canonical": canonical, "witness": "EXACT",
                             "joined": len(values), "informative_n": informative,
                             "agree_n": exact,
                             "source_exceeds_target": sum(a > b for a, b in zip(source_values, target_values)),
                             "target_exceeds_source": sum(a < b for a, b in zip(source_values, target_values)),
                             "median_source": median(source_values) if source_values else None,
                             "median_target": median(target_values) if target_values else None})
    for row in rows:
        row["agree_pct"] = (round(100.0 * row["agree_n"] / row["informative_n"], 2)
                             if row["informative_n"] else None)
        if row["informative_n"]:
            source_side = row.get("source_exceeds_target", row.get("source_exceeds_expected", 0))
            target_side = row.get("target_exceeds_source", row.get("expected_exceeds_source", 0))
            row["one_sided"] = (source_side == 0) != (target_side == 0)
    fumble_candidates = (
        measure_fumble_candidate_matrix(con, v26, join_predicate)
        if date_keyed else []
    )
    layout_independent_fumbles = (
        measure_layout_independent_fumble_candidate(con, v26, join_predicate)
        if date_keyed else []
    )
    defense_candidates = (
        measure_defense_candidate_matrix(con, v26, join_predicate)
        if date_keyed else []
    )
    defense_source_identity = measure_defense_source_identity(con)
    return {"scope": scope_name, "source_table": source_table, "season_type": season_type,
            "source_rows": total_rows, "source_game_keys": game_keys,
            "classified_rows": classified_rows, "unresolved_rows": unresolved_rows,
            "unresolved_numeric_rows": unresolved_numeric_rows,
            "unresolved_empty_rows": int(unresolved_rows - unresolved_numeric_rows),
            "layout_recovery_counts": layout_recovery_counts,
            "fumble_candidate_matrix": fumble_candidates,
            "layout_independent_fumble_candidate": layout_independent_fumbles,
            "defense_candidate_matrix": defense_candidates,
            "defense_source_identity": defense_source_identity,
            "identity_join_rows": identity_rows, "weekly_join_rows": target_join_rows,
            "weekly_unjoined_rows": target_unjoined_rows,
            "join_key": "(nflcom_slug, parsed_game_date, opp, recovered_layout) -> "
            "(NFL_player_id, game_date, opponent_nfl_team)"
            if targeted or date_keyed else "(nflcom_slug, wk, recovered_layout) -> (NFL_player_id, week)",
            "rows": rows}


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--targeted",
        action="store_true",
        help="measure the historical targeted-log parquet using date-keyed weekly joins",
    )
    parser.add_argument(
        "--all-years",
        action="store_true",
        help="measure recoverable raw logs across all regular/postseason years using date/opponent joins",
    )
    args = parser.parse_args()
    if args.targeted and args.all_years:
        parser.error("--targeted and --all-years are mutually exclusive")
    con = duckdb.connect()
    if args.targeted:
        mappings = _mapped_columns("nflcom_player_logs_targeted")
        result = {
            "source": "nflcom_player_logs_targeted",
            "season": "all captured seasons",
            "join_key": "exact parsed source game_date to v26 game_date",
            "scopes": [
                measure_scope(
                    con, name, spec, mappings,
                    source_path=TARGETED_LOGS, targeted=True,
                )
                for name, spec in TARGETED_SCOPES.items()
            ],
        }
        output = TARGETED_OUT
    elif args.all_years:
        mappings = _mapped_columns()
        result = {
            "source": "nflcom_player_logs",
            "season": "all captured regular/postseason years",
            "join_key": "exact parsed source game_date plus opponent to v26 game_date/opponent",
            "scopes": [
                measure_scope(
                    con, name, spec, mappings, date_keyed=True,
                )
                for name, spec in SCOPES.items()
            ],
        }
        output = ALL_YEARS_OUT
    else:
        mappings = _mapped_columns()
        result = {"source": "nflcom_player_logs", "season": 2025,
                  "scopes": [measure_scope(con, name, spec, mappings)
                             for name, spec in SCOPES.items()]}
        output = OUT
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"measurement -> {output}")
    for scope in result["scopes"]:
        measured = [r for r in scope["rows"] if r["informative_n"]]
        print(scope["scope"], scope["source_rows"], scope["classified_rows"],
              scope["unresolved_rows"], len(measured),
              min((r["agree_pct"] for r in measured), default=None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
