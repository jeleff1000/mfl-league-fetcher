from scripts.sota_recon.nflcom_player_logs_cache_reparse import parse_page


def test_cache_reparse_does_not_infer_layout_from_target_tables(monkeypatch, tmp_path):
    page = tmp_path / "page.html"
    page.write_text("unused", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.sota_recon.nflcom_player_logs_cache_reparse.parse_all_tables",
        lambda text: [{"caption": "Regular Season", "headers": ["unknown"], "rows": []}],
    )
    result = parse_page(
        type("Page", (), {"slug": "sample", "season": "2025", "path": str(page)})()
    )
    assert result["rows"] == []
    assert result["failures"][0]["status"] != "RESOLVED"
