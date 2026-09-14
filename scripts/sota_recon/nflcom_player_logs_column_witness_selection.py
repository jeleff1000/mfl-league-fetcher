"""Select NFL.com player-log canonicals from the completed weekly witness receipt.

This is a generated measurement artifact.  It does not edit the disposition ledger or
license anything.  Selection is by the existing layout-specific mapping plus the
denominator-weighted weekly witness; regular and postseason rows are combined by sums,
never by averaging percentages.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


RECEIPT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_targeted_weekly_witness.json"
)
ALL_YEARS_RECEIPT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_all_years_weekly_witness.json"
)
OUT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_column_witness_selection.json"
)


def _sum(row: dict, *names: str) -> int:
    return sum(int(row.get(name) or 0) for name in names)


def select_rows(scopes: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for scope in scopes:
        for row in scope.get("rows", []):
            grouped[(row["layout"], row["source_column"])].append(
                {"scope": scope["scope"], **row}
            )

    selections = []
    for (layout, source_column), rows in sorted(grouped.items()):
        canonicals = {row["canonical"] for row in rows}
        if len(canonicals) != 1:
            raise ValueError(
                f"mapping collision for {layout}.{source_column}: {sorted(canonicals)}"
            )
        informative = _sum(rows[0], "informative_n") if len(rows) == 1 else sum(
            int(row.get("informative_n") or 0) for row in rows
        )
        agree = sum(int(row.get("agree_n") or 0) for row in rows)
        source_excess = sum(
            int(row.get("source_exceeds_target", row.get("source_exceeds_expected", 0)) or 0)
            for row in rows
        )
        target_excess = sum(
            int(row.get("target_exceeds_source", row.get("expected_exceeds_source", 0)) or 0)
            for row in rows
        )
        selections.append(
            {
                "layout": layout,
                "source_column": source_column,
                "canonical": next(iter(canonicals)),
                "witness": sorted({row["witness"] for row in rows}),
                "informative_n": informative,
                "agree_n": agree,
                "agree_pct": round(100.0 * agree / informative, 2) if informative else None,
                "source_exceeds_target": source_excess,
                "target_exceeds_source": target_excess,
                "denominator_rule": "sum informative_n across regular and postseason",
                "status": (
                    "MAPPED_WITH_WEEKLY_WITNESS"
                    if informative
                    else "MAPPED_PENDING_INFORMATIVE_ROWS"
                ),
                "scope_breakdown": [
                    {
                        "scope": row["scope"],
                        "informative_n": int(row.get("informative_n") or 0),
                        "agree_n": int(row.get("agree_n") or 0),
                        "agree_pct": row.get("agree_pct"),
                    }
                    for row in rows
                ],
            }
        )
    return selections


def select_fumble_forms(scopes: list[dict]) -> list[dict]:
    grouped: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"informative_n": 0, "agree_n": 0})
    )
    for scope in scopes:
        for row in scope.get("fumble_candidate_matrix", []):
            score = row.get("common_target_score") or {}
            form = row["form"]
            layout = row["layout"]
            grouped[layout][form]["informative_n"] += int(score.get("informative_n") or 0)
            grouped[layout][form]["agree_n"] += int(score.get("agree_n") or 0)
    selections = []
    for layout, forms in sorted(grouped.items()):
        ranked = sorted(
            forms.items(),
            key=lambda item: (
                item[1]["agree_n"] / item[1]["informative_n"]
                if item[1]["informative_n"] else -1,
                item[1]["informative_n"],
            ),
            reverse=True,
        )
        form, values = ranked[0]
        selections.append(
            {
                "layout": layout,
                "source_column": "fum",
                "selected_form": form,
                "canonical": "fumbles" if form == "fumbles" else form,
                "informative_n": values["informative_n"],
                "agree_n": values["agree_n"],
                "agree_pct": round(
                    100.0 * values["agree_n"] / values["informative_n"], 2
                )
                if values["informative_n"]
                else None,
                "selection_rule": "maximum common-target agreement on summed denominator",
            }
        )
    return selections


def select_layout_independent_fumbles(scopes: list[dict]) -> list[dict]:
    grouped: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    source_rows_by_column: dict[str, int] = defaultdict(int)
    for scope in scopes:
        for row in scope.get("layout_independent_fumble_candidate", []):
            column = row["source_column"]
            values = grouped[column]
            for key in (
                "joined_n", "target_available_n", "informative_n", "agree_n",
                "source_exceeds_target", "target_exceeds_source", "source_nonzero_n",
                "target_nonzero_n", "identity_joined_n",
            ):
                values[key] += int(row.get(key) or 0)
            source_rows_by_column[column] += int(row.get("source_rows") or 0)
    result = []
    for source_column, values in sorted(grouped.items()):
        informative = values["informative_n"]
        result.append(
            {
                "layout": "OFFENSE_SHARED_FUMBLE_ONLY",
                "layout_required_for_mapping": False,
                "source_column": source_column,
                "canonical": "fumbles" if source_column == "fum" else "fumbles_lost",
                "source_rows": source_rows_by_column[source_column],
                **values,
                "agree_pct": round(100.0 * values["agree_n"] / informative, 2)
                if informative else None,
                "status": "MAPPED_LAYOUT_INDEPENDENT_TARGET_COVERAGE_GAP",
                "selection_rule": (
                    "slug identity plus weekly key; position layout is not required for "
                    "generic fumble columns"
                ),
            }
        )
    return result


def build() -> dict:
    source = json.loads(RECEIPT.read_text(encoding="utf-8"))
    scopes = source["scopes"]
    all_years = json.loads(ALL_YEARS_RECEIPT.read_text(encoding="utf-8"))
    result = {
        "version": "1",
        "source": source["source"],
        "source_receipt": RECEIPT.as_posix(),
        "selection_rule": (
            "retain the layout-specific canonical and sum informative weekly denominators "
            "across regular and postseason; do not pool layouts"
        ),
        "selections": select_rows(scopes),
        "fumble_form_selections": select_fumble_forms(scopes),
        "all_years_source_receipt": ALL_YEARS_RECEIPT.as_posix(),
        "all_years_selections": select_rows(all_years["scopes"]),
        "all_years_fumble_form_selections": select_fumble_forms(all_years["scopes"]),
        "all_years_layout_independent_fumble_selections": (
            select_layout_independent_fumbles(all_years["scopes"])
        ),
        "coverage_summary": {
            "targeted": {
                "mapped_fields": len(select_rows(scopes)),
                "zero_informative_fields": sum(
                    int(row["informative_n"] == 0) for row in select_rows(scopes)
                ),
            },
            "all_years": {
                "mapped_fields": len(select_rows(all_years["scopes"])),
                "zero_informative_fields": sum(
                    int(row["informative_n"] == 0)
                    for row in select_rows(all_years["scopes"])
                ),
            },
        },
        "no_disposition_or_license_change": True,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
