from scripts.sota_recon.nflcom_player_logs_column_witness_selection import select_rows


def test_selection_sums_denominators_and_refuses_mapping_collision():
    scopes = [
        {
            "scope": "regular",
            "rows": [
                {
                    "layout": "QB", "source_column": "yds", "canonical": "passing_yards",
                    "witness": "EXACT", "informative_n": 8, "agree_n": 7,
                    "source_exceeds_target": 1, "target_exceeds_source": 0,
                    "agree_pct": 87.5,
                }
            ],
        },
        {
            "scope": "postseason",
            "rows": [
                {
                    "layout": "QB", "source_column": "yds", "canonical": "passing_yards",
                    "witness": "EXACT", "informative_n": 2, "agree_n": 2,
                    "source_exceeds_target": 0, "target_exceeds_source": 0,
                    "agree_pct": 100.0,
                }
            ],
        },
    ]
    result = select_rows(scopes)[0]
    assert result["informative_n"] == 10
    assert result["agree_n"] == 9
    assert result["agree_pct"] == 90.0
