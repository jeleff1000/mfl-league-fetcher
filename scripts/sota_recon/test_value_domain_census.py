from scripts.sota_recon.value_domain_census import (
    extract_json_paths,
    parse_numeric,
    stable_examples,
)


def test_parse_numeric_preserves_states():
    assert parse_numeric(None) == (None, "SOURCE_NULL")
    assert parse_numeric(0) == (0.0, "OBSERVED_ZERO")
    assert parse_numeric(" 12.50 ") == (12.5, "OBSERVED_VALUE")
    assert parse_numeric("not-a-number") == (None, "PARSER_FAILED")


def test_extract_json_paths_is_recursive_and_stable():
    value = {"outer": {"inner": 3}, "items": ["a", {"x": True}]}
    assert extract_json_paths(value) == [
        ("$.items[0]", "a"),
        ("$.items[1].x", True),
        ("$.outer.inner", 3),
    ]


def test_stable_examples_are_sorted_and_deduplicated():
    assert stable_examples(["b", "a", "b", None, 2], limit=3) == ["2", "a", "b"]
