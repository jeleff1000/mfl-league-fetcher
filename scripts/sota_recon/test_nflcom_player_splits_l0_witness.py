from .nflcom_player_splits_l0_witness import (
    L0_MAPPINGS,
    compare_exact_rows,
    compare_ratio_rows,
    compare_tackle_identity,
    check_capture_recoverability,
    rank_candidate_results,
    summarize_capture_loss,
)


def test_l0_witness_matrix_covers_every_mapped_defense_column():
    assert set(L0_MAPPINGS) == {
        "ast", "int", "pdef", "sck", "sfty", "solo", "tds", "total", "yds",
    }
    assert L0_MAPPINGS["total"] == "def_tackles_combined"


def test_ratio_witness_uses_nonzero_denominator_and_published_tick():
    result = compare_ratio_rows(
        [
            (20, 4, 10, 2),   # 5.0 == 5.0
            (20.0, 4, 19.8, 4),  # 5.0 vs 4.95: within the published tick
            (30, 3, 12, 3),   # 10.0 vs 4.0: source high
            (10, 2, 30, 4),   # 5.0 vs 7.5: target high
            (10, 0, 10, 1),   # source denominator is degenerate
            (10, 2, 10, 0),   # target denominator is degenerate
        ],
        tolerance=0.05,
    )
    assert result == {
        "informative_n": 4,
        "agree_n": 2,
        "source_exceeds_target": 1,
        "target_exceeds_source": 1,
    }


def test_tackle_identity_requires_all_three_numeric_cells():
    assert compare_tackle_identity(
        [(5, 3, 2), (0, 0, 0), (None, 2, 1), (2, 1, 1)]
    ) == {"complete_n": 3, "agree_n": 3}


def test_exact_witness_reports_both_directions():
    assert compare_exact_rows([(1, 1), (2, 1), (1, 2), (None, 0)]) == {
        "informative_n": 3,
        "agree_n": 1,
        "source_exceeds_target": 1,
        "target_exceeds_source": 1,
    }


def test_candidate_matrix_ranks_best_match_first():
    ranked = rank_candidate_results(
        [
            {"canonical": "b", "agree_pct": 80.0},
            {"canonical": "a", "agree_pct": 80.0},
            {"canonical": "c", "agree_pct": 92.0},
        ]
    )
    assert [row["canonical"] for row in ranked] == ["c", "a", "b"]


def test_ratio_witness_can_scale_published_percentages():
    assert compare_ratio_rows(
        [(1, 4, 25, 100)], tolerance=0.05, scale=100.0
    ) == {
        "informative_n": 1,
        "agree_n": 1,
        "source_exceeds_target": 0,
        "target_exceeds_source": 0,
    }


def test_capture_loss_is_declared_once_per_layout():
    result = summarize_capture_loss(
        [
            ("player_splits_L0", "Months", "lng", 3),
            ("player_splits_L0", "Days", "lng", 2),
            ("player_splits_L1", "Months", "td", 4),
        ]
    )
    assert result == [
        {
            "layout": "player_splits_L0",
            "rows": 5,
            "split_tables": ["Days", "Months"],
            "lost_columns": ["lng"],
            "all_rows_declare_loss": True,
        },
        {
            "layout": "player_splits_L1",
            "rows": 4,
            "split_tables": ["Months"],
            "lost_columns": ["td"],
            "all_rows_declare_loss": True,
        },
    ]


def test_selected_witness_fields_do_not_use_the_lost_field():
    loss = [
        {"layout": "player_splits_L0", "lost_columns": ["lng"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L1", "lost_columns": ["td"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L2", "lost_columns": ["40"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L3", "lost_columns": ["1st"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L4", "lost_columns": ["fum"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L5", "lost_columns": ["rate"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L6", "lost_columns": ["net_avg"], "all_rows_declare_loss": True},
        {"layout": "player_splits_L7", "lost_columns": ["pct"], "all_rows_declare_loss": True},
    ]
    result = check_capture_recoverability(loss)
    assert {row["status"] for row in result} == {"RECOVERABLE_WITNESS_FIELDS"}
    assert all(not row["lost_witness_columns"] for row in result)
