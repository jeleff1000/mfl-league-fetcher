import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[5] / "scripts" / "extraplatform_corpus" / "batch_ingest_extraplatform.py"
SPEC = importlib.util.spec_from_file_location("batch_ingest_extraplatform", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_mfl_seed_year_does_not_truncate_history_link_expansion():
    assert MODULE._seed_year_args("mfl", "2024:10267") == []
