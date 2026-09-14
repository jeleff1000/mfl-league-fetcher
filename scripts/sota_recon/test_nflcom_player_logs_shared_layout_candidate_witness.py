from scripts.sota_recon.nflcom_player_logs_shared_layout_candidate_witness import score_row


def test_shared_layout_candidate_scorer_keeps_unknown_targets_out():
    # (layout, source yds, yds_2, lng, lng_2, td, td_2, target rush/rec yards/long/td)
    row = ("RBFB5", 10, 5, None, None, 1, None, 10, 5, None, None, 1, None)
    result = score_row(row)
    assert result["RBFB5"] == {"informative_n": 3, "agree_n": 3}
    assert result["WRTE"] == {"informative_n": 2, "agree_n": 0}
    assert result["winner"] == "RBFB5"
