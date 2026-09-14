from .pbp_terminal_state import EXTENSIONS


def test_every_extension_has_an_explicit_terminal_classification():
    assert EXTENSIONS
    assert all(
        v["status"] in {"witnessed_direct", "witnessed_reconciliation", "excluded",
                        "unresolved_materialization", "unresolved_semantics"}
        for v in EXTENSIONS.values()
    )


def test_pressure_is_not_silently_equated_to_qb_hit():
    assert EXTENSIONS["passing_pressured"]["status"] == "excluded"
