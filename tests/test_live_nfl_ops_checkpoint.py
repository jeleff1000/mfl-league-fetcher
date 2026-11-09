import hashlib
from fnmatch import fnmatchcase

import pytest

from scripts.live_nfl_ops_checkpoint import checkpoints, prune, restore


class ReleaseFixture:
    def __init__(self, assets):
        self.files = assets
        self.deleted = []

    def assets(self):
        return [{"name": name, "id": i, "state": "uploaded" if content else "starter"}
                for i, (name, content) in enumerate(self.files.items())]

    def download(self, pattern, directory):
        for name, content in self.files.items():
            if fnmatchcase(name, pattern):
                (directory / name).write_bytes(content)

    def delete(self, asset_id):
        self.deleted.append(list(self.files)[asset_id])


def test_checkpoint_restores_exact_bytes_and_rejects_partial_or_corrupt_upload(tmp_path):
    name = "ops_nfl-31-1-ops-2026-11-09"
    parts = [name + ".duckdb.part-00", name + ".duckdb.part-01"]
    digest = hashlib.sha256(b"ab").hexdigest()
    release = ReleaseFixture(dict(zip(parts, [b"a", b"b"])))
    metadata = {"prefix": name, "parts": parts, "manifest": {"sha256": digest}}
    target = tmp_path / "ops_nfl.duckdb"
    restore(release, target, metadata, expected_sha=digest)
    assert target.read_bytes() == b"ab"
    release.files[parts[-1]] = b"c"
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        restore(release, target, metadata)
    del release.files[parts[-1]]
    with pytest.raises(ValueError, match="missing release parts"):
        restore(release, target, metadata)


def test_interrupted_publish_cannot_displace_a_complete_baseline(tmp_path):
    release = ReleaseFixture({
        "ops_nfl-32-1.checkpoint.json": b"",
        "ops_nfl-32-1.duckdb.part-00": b"incomplete",
        "ops_nfl-31-1.checkpoint.json": b"committed",
        "ops_nfl-31-1.duckdb.part-00": b"current",
        "ops_nfl.duckdb.part-00": b"legacy",
        "ops_nfl.duckdb.sha256": b"legacy checksum",
    })
    assert checkpoints(release.assets()) == ["ops_nfl-31-1.checkpoint.json"]
    prune(release)
    assert release.deleted == ["ops_nfl-32-1.checkpoint.json", "ops_nfl-32-1.duckdb.part-00"]

    digest = hashlib.sha256(b"current").hexdigest()
    release.files["ops_nfl.duckdb.sha256"] = f"{digest}  output/ops_nfl.duckdb".encode()
    release.files["ops_nfl.duckdb.part-00"] = b"current"
    target = tmp_path / "new-name.duckdb"
    restore(release, target, None, expected_sha=digest)
    assert target.read_bytes() == b"current"
