"""PostHog telemetry should never affect request handling."""

from unittest.mock import patch


def test_env_float_uses_default_for_invalid_values():
    with patch.dict("os.environ", {"POSTHOG_QUERY_SAMPLE_RATE": "not-a-number"}):
        import importlib
        import main as main_mod

        importlib.reload(main_mod)
        assert main_mod._POSTHOG_QUERY_SAMPLE_RATE == 0.02


def test_posthog_init_failure_disables_client():
    import main as main_mod

    class BrokenPostHog:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("posthog down")

    with patch.dict("os.environ", {"POSTHOG_API_KEY": "phc_test"}):
        with patch.object(main_mod, "_Posthog", BrokenPostHog):
            assert main_mod._init_posthog_client() is None


def test_track_event_swallows_capture_errors():
    import main as main_mod

    class BrokenClient:
        def capture(self, *args, **kwargs):
            raise RuntimeError("capture failed")

    with patch.object(main_mod, "posthog_client", BrokenClient()):
        main_mod.track_event("test_event", {"database": "___leagues"})
