from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "register_yahoo_cookie_credentials.py"
spec = importlib.util.spec_from_file_location("register_yahoo_cookie_credentials", SCRIPT)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def test_load_cookie_payload_accepts_line_wrapped_json_cookie_value(tmp_path: Path) -> None:
    path = tmp_path / "cookiejar.json"
    path.write_text(
        '{"cookies":[{"name":"T","value":"token","domain":".yahoo.com"},'
        '{"name":"Y","value":"user\n","domain":".yahoo.com"}]}',
        encoding="utf-8",
    )

    payload = module.load_cookie_payload(path)

    assert [cookie["name"] for cookie in payload["cookies"]] == ["T", "Y"]
    assert payload["cookies"][1]["value"] == "user"
