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
    maintenance = workflow.split("- name: Create primary volume snapshot", 1)[1].split(
        "- name: Create independent cross-region backup fork", 1
    )[0]

    assert "FLY_PRIMARY_VOLUME_ID: ${{ secrets.FLY_PRIMARY_VOLUME_ID }}" in maintenance
    assert "vol_" not in maintenance


def test_inventory_reports_only_bounded_runtime_configuration() -> None:
    workflow = Path(".github/workflows/fly_snapshot_inventory.yml").read_text(
        encoding="utf-8"
    )

    assert "flyctl secrets list --app league-history-duckdb --json" in workflow
    assert "DUCKDB_MEMORY_LIMIT" in workflow
    assert "DUCKDB_THREADS" in workflow
    assert "DUCKDB_CHECKPOINT_THRESHOLD" in workflow
    assert "DUCKDB_MAX_TEMP_DIRECTORY_SIZE" in workflow
    assert "cat /proc/1/environ" not in workflow


def test_cross_region_backup_fork_is_explicit_and_fail_closed() -> None:
    workflow = Path(".github/workflows/fly_snapshot_inventory.yml").read_text(
        encoding="utf-8"
    )

    assert "create_cross_region_backup:" in workflow
    assert "cross_region_backup_name:" in workflow
    assert "if: inputs.create_cross_region_backup" in workflow
    assert '[[ "$BACKUP_VOLUME_NAME" =~ ^duckdb_backup_[0-9]{8}$ ]]' in workflow
    assert 'test "$SOURCE_NAME" = "duckdb_data"' in workflow
    assert 'test "$SOURCE_ATTACHMENT" = "1781e011b69068"' in workflow
    assert 'test "$EXISTING_BACKUP_COUNT" = "0"' in workflow
    assert (
        'flyctl volumes fork "$FLY_PRIMARY_VOLUME_ID" '
        '--app league-history-duckdb --region ord '
        '--name "$BACKUP_VOLUME_NAME" --require-unique-zone --json'
    ) in workflow
