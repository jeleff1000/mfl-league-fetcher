from pathlib import Path


def test_primary_snapshot_maintenance_is_explicit_and_fail_closed() -> None:
    workflow = Path(".github/workflows/fly_snapshot_inventory.yml").read_text(
        encoding="utf-8"
    )

    assert "create_primary_snapshot:" in workflow
    assert "set_primary_retention_60:" in workflow
    assert workflow.count("default: false") >= 2
    assert "if: inputs.create_primary_snapshot" in workflow
    assert "if: inputs.set_primary_retention_60" in workflow
    assert (
        'flyctl volumes snapshots create "$FLY_PRIMARY_VOLUME_ID" '
        "--app league-history-duckdb --json"
    ) in workflow
    assert (
        'flyctl volumes update "$FLY_PRIMARY_VOLUME_ID" '
        "--app league-history-duckdb --snapshot-retention 60 --json"
    ) in workflow
    assert workflow.index("- name: Set primary snapshot retention to 60 days") < workflow.index(
        "- name: Create primary volume snapshot"
    )


def test_primary_snapshot_maintenance_uses_only_the_secret_volume_id() -> None:
    workflow = Path(".github/workflows/fly_snapshot_inventory.yml").read_text(
        encoding="utf-8"
    )
    maintenance = workflow.split("- name: Create primary volume snapshot", 1)[1]

    assert "FLY_PRIMARY_VOLUME_ID: ${{ secrets.FLY_PRIMARY_VOLUME_ID }}" in maintenance
    assert "vol_" not in maintenance
