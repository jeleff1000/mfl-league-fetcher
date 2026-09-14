from .nflcom_player_season_post_witness import classify_capture, classify_post_witness


def test_identical_post_regular_capture_is_rejected_as_a_witness():
    assert classify_capture([{"paired_n": 10, "same_n": 10}]) == "DUPLICATES_REGULAR"


def test_mixed_post_regular_capture_is_not_classified_as_duplicate():
    assert classify_capture([{"paired_n": 10, "same_n": 9}]) == "NOT_IDENTICAL"


def test_no_pairs_remains_unresolved():
    assert classify_capture([{"paired_n": 0, "same_n": 0}]) == "NO_PAIRED_ROWS"


def test_duplicate_capture_overrides_any_post_target_agreement():
    assert classify_post_witness(
        "DUPLICATES_REGULAR",
        [{"informative_n": 10, "agree_n": 10}],
    ) == "PROOF_PENDING_DUPLICATE_REGULAR_CAPTURE"


def test_post_target_requires_every_informative_field_to_agree():
    assert classify_post_witness(
        "NOT_IDENTICAL",
        [{"informative_n": 10, "agree_n": 9}],
    ) == "PROOF_PENDING_POST_VALUE_MISMATCH"
