"""Contract tests for the read-only PFRA page evidence worker."""

import hashlib
import importlib.util
import io
import json
import threading
from copy import deepcopy
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "pfra_page_shard.py"
spec = importlib.util.spec_from_file_location("pfra_page_shard", MODULE_PATH)


@pytest.fixture
def worker():
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def manifest_for(pdf_sha="a" * 64, pages=None):
    if pages is None:
        pages = [{
            "page_number": 1,
            "visual_id": "visual-1",
            "render_sha256": "b" * 64,
            "occurrences": [{"source_occurrence_id": "occ-1", "page_number": 1, "cohort_id": "cohort-1", "document_sha256": pdf_sha}],
            "candidate_contracts": [{"shape_id": "shape-1", "contract_sha256": "c" * 64}],
            "regions": [{"region_id": "r1", "rect": [0.1, 0.2, 0.9, 0.8], "method": "native"}],
        }]
    return {
        "schema_version": 1,
        "batch_id": "batch-1",
        "worker_sha256": "d" * 64,
        "worker_commit": "e" * 40,
        "sources": [{
            "pdf_sha256": pdf_sha,
            "byte_length": 100,
            "page_count": len(pages),
            "shard_id": int(pdf_sha, 16) % 10,
            "pages": pages,
        }],
    }


def test_manifest_digest_is_canonical_and_reports_nonempty_shards(worker):
    manifest = manifest_for()
    validated = worker.validate_manifest(manifest)
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert validated["canonical_sha256"] == hashlib.sha256(canonical).hexdigest()
    assert validated["shard_ids"] == [int("a" * 64, 16) % 10]


def test_source_shard_uses_full_pdf_digest(worker):
    manifest = manifest_for("0" * 63 + "1")
    manifest["sources"][0]["shard_id"] = 1
    assert worker.validate_manifest(manifest)["shard_ids"] == [1]


@pytest.mark.parametrize("second_source", [False, True])
def test_shape_id_has_one_contract_hash_across_manifest(worker, second_source):
    manifest = manifest_for()
    first = manifest["sources"][0]
    second_page = deepcopy(first["pages"][0])
    second_page["visual_id"] = "visual-2"
    second_page["occurrences"][0]["source_occurrence_id"] = "occ-2"
    if second_source:
        other_sha = "0" * 63 + "1"
        second_page["occurrences"][0]["document_sha256"] = other_sha
        manifest["sources"].append({
            "pdf_sha256": other_sha,
            "byte_length": 100,
            "page_count": 1,
            "shard_id": 1,
            "pages": [second_page],
        })
    else:
        first["page_count"] = 2
        second_page["page_number"] = 2
        second_page["occurrences"][0]["page_number"] = 2
        first["pages"].append(second_page)
    worker.validate_manifest(manifest)  # Reusing the same locked hash is valid.
    second_page["candidate_contracts"][0]["contract_sha256"] = "d" * 64
    with pytest.raises(ValueError, match="contract"):
        worker.validate_manifest(manifest)


@pytest.mark.parametrize("mutation", [
    lambda m: m["sources"][0].update(pdf_sha256="../escape.pdf"),
    lambda m: m["sources"][0]["pages"][0]["regions"][0].update(rect=[0, 0, 1.1, 1]),
    lambda m: m["sources"][0]["pages"][0]["regions"][0].update(rect=[0, 0, 0, 1]),
    lambda m: m["sources"][0].update(byte_length=True),
    lambda m: m["sources"][0].update(shard_id=0 if m["sources"][0]["shard_id"] != 0 else 1),
    lambda m: m["sources"].append(m["sources"][0].copy()),
    lambda m: m["sources"][0]["pages"].append(m["sources"][0]["pages"][0].copy()),
])
def test_manifest_rejects_invalid_or_duplicate_bindings(worker, mutation):
    manifest = manifest_for()
    mutation(manifest)
    with pytest.raises(ValueError):
        worker.validate_manifest(manifest)


def test_occurrence_key_allows_two_cohorts_but_rejects_duplicate_tuple(worker):
    manifest = manifest_for()
    occurrences = manifest["sources"][0]["pages"][0]["occurrences"]
    occurrences.append({"source_occurrence_id": "occ-1", "page_number": 1, "cohort_id": "cohort-2", "document_sha256": "a" * 64})
    worker.validate_manifest(manifest)
    occurrences.append(occurrences[0].copy())
    with pytest.raises(ValueError):
        worker.validate_manifest(manifest)


def test_occurrence_page_can_differ_from_representative_page(worker):
    manifest = manifest_for()
    occurrence = manifest["sources"][0]["pages"][0]["occurrences"][0]
    occurrence["page_number"] = 7
    occurrence["document_sha256"] = "f" * 64
    worker.validate_manifest(manifest)


def test_same_occurrence_page_cannot_claim_two_documents(worker):
    manifest = manifest_for()
    occurrences = manifest["sources"][0]["pages"][0]["occurrences"]
    occurrences.append({"source_occurrence_id": "occ-1", "page_number": 1, "cohort_id": "cohort-2", "document_sha256": "f" * 64})
    with pytest.raises(ValueError):
        worker.validate_manifest(manifest)


def pdf_manifest(worker, text=("ALPHA", "BRAVO"), method="native"):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    for word in text:
        page = doc.new_page(width=200, height=200)
        page.insert_text((25, 50), word)
    pdf = doc.tobytes()
    doc.close()
    pdf_sha = hashlib.sha256(pdf).hexdigest()
    pages = []
    for number in range(1, len(text) + 1):
        pages.append({
            "page_number": number,
            "visual_id": f"visual-{number}",
            "render_sha256": "b" * 64,
            "occurrences": [{"source_occurrence_id": f"occ-{number}", "page_number": number,
                             "cohort_id": "cohort-1", "document_sha256": pdf_sha}],
            "candidate_contracts": [{"shape_id": "shape-1", "contract_sha256": "c" * 64}],
            "regions": [{"region_id": "r1", "rect": [0, 0, 1, 1], "method": method,
                         **({"dpi": 100} if method == "ocr" else {})}],
        })
    manifest = manifest_for(pdf_sha, pages)
    manifest["sources"][0]["byte_length"] = len(pdf)
    manifest["worker_sha256"] = hashlib.sha256(MODULE_PATH.read_bytes()).hexdigest()
    return pdf, manifest


def test_two_pages_share_one_source_read_and_leave_no_renders(worker, tmp_path):
    pdf, manifest = pdf_manifest(worker)
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    (source_dir / f"{manifest['sources'][0]['pdf_sha256']}.pdf").write_bytes(pdf)
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_root=source_dir)
    assert result["status"] == "EVIDENCE_ONLY"
    assert result["source_read_count"] == 1
    assert result["source_hash_count"] == 1
    assert [p["page_number"] for p in result["sources"][0]["pages"]] == [1, 2]
    assert [p["regions"][0]["words"][0]["text"] for p in result["sources"][0]["pages"]] == ["ALPHA", "BRAVO"]
    assert list(source_dir.iterdir()) == [source_dir / f"{manifest['sources'][0]['pdf_sha256']}.pdf"]


def test_source_loader_still_verifies_bytes(worker):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda source: pdf)
    assert result["status"] == "EVIDENCE_ONLY"
    with pytest.raises(ValueError, match="source hash"):
        worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                         manifest["worker_commit"], source_loader=lambda source: b"x" * len(pdf))


def test_run_rejects_stale_manifest_or_worker_binding(worker):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    shard = manifest["sources"][0]["shard_id"]
    with pytest.raises(ValueError):
        worker.run_shard(manifest, "0" * 64, shard, manifest["worker_commit"], source_loader=lambda source: pdf)
    manifest["worker_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        worker.run_shard(manifest, worker.manifest_sha256(manifest), shard, manifest["worker_commit"],
                         source_loader=lambda source: pdf)


def test_ocr_timeout_is_incomplete_without_retained_render(worker, tmp_path, monkeypatch):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",), method="ocr")
    stop = threading.Event()

    class BlockingStdout:
        def read(self, size):
            stop.wait(1)
            return b""

        def close(self):
            pass

    class BlockingProcess:
        stdin = io.BytesIO()
        stdout = BlockingStdout()
        returncode = None

        def poll(self):
            return self.returncode

        def kill(self):
            self.returncode = -9
            stop.set()

        def wait(self, timeout=None):
            return self.returncode

    monkeypatch.setattr(worker.subprocess, "Popen", lambda *args, **kwargs: BlockingProcess())
    monkeypatch.setattr(worker, "OCR_TIMEOUT_SECONDS", 0.01)
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda source: pdf)
    assert result["status"] == "INCOMPLETE"
    assert result["sources"][0]["pages"][0]["regions"][0]["error"] == "OCR_TIMEOUT"
    assert list(tmp_path.iterdir()) == []


def test_ocr_output_is_capped_during_capture(worker, monkeypatch):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",), method="ocr")

    class ExcessOutputProcess:
        stdin = io.BytesIO()
        stdout = io.BytesIO(b"x" * (worker.MAX_OCR_OUTPUT + 1))

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(worker.subprocess, "Popen", lambda *args, **kwargs: ExcessOutputProcess())
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda source: pdf)
    assert result["status"] == "INCOMPLETE"
    assert result["sources"][0]["pages"][0]["regions"][0]["error"] == "OCR_OUTPUT_LIMIT"


def evidence_result(worker):
    pdf, manifest = pdf_manifest(worker)
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda source: pdf)
    return manifest, result


def test_merge_summary_is_idempotent_and_evidence_only(worker):
    manifest, result = evidence_result(worker)
    digest = worker.manifest_sha256(manifest)
    summary = worker.validate_results(manifest, digest, [result])
    assert summary == worker.validate_results(manifest, digest, [result])
    assert summary["status"] == "EVIDENCE_ONLY"
    assert summary["source_count"] == 1
    assert summary["page_count"] == 2
    assert summary["region_count"] == 2


@pytest.mark.parametrize("mutate", [
    lambda r: r.update(status="INCOMPLETE"),
    lambda r: r.update(worker_sha256="0" * 64),
    lambda r: r["sources"][0]["pages"][0]["candidate_contracts"][0].update(contract_sha256="0" * 64),
    lambda r: r["sources"][0]["pages"].pop(),
    lambda r: r["sources"][0]["pages"][0]["regions"].pop(),
    lambda r: r["sources"][0]["pages"][0]["regions"][0].update(status="ERROR", error="OCR_TIMEOUT"),
    lambda r: r.update(source_hash_count=0),
    lambda r: r.update(source_hash_count=True),
    lambda r: r["sources"][0]["pages"][0].update(page_number=True),
])
def test_merge_rejects_incomplete_or_stale_result(worker, mutate):
    manifest, result = evidence_result(worker)
    mutate(result)
    with pytest.raises(ValueError):
        worker.validate_results(manifest, worker.manifest_sha256(manifest), [result])


def test_merge_rejects_missing_or_duplicate_shard_results(worker):
    manifest, result = evidence_result(worker)
    digest = worker.manifest_sha256(manifest)
    with pytest.raises(ValueError):
        worker.validate_results(manifest, digest, [])
    with pytest.raises(ValueError):
        worker.validate_results(manifest, digest, [result, result])


def test_rotated_pdf_uses_unrotated_page_frame(worker):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=200, height=400)
    page.insert_text((25, 50), "ALPHA")
    page.set_rotation(90)
    pdf = doc.tobytes()
    doc.close()
    pdf_sha = hashlib.sha256(pdf).hexdigest()
    manifest = manifest_for(pdf_sha)
    manifest["sources"][0]["byte_length"] = len(pdf)
    manifest["worker_sha256"] = hashlib.sha256(MODULE_PATH.read_bytes()).hexdigest()
    manifest["sources"][0]["pages"][0]["occurrences"][0]["document_sha256"] = pdf_sha
    manifest["sources"][0]["pages"][0]["regions"][0]["rect"] = [0, 0, 0.5, 0.5]
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda source: pdf)
    evidence = result["sources"][0]["pages"][0]
    assert evidence["coordinate_frame"] == "UNROTATED_PAGE"
    assert evidence["source_rotation"] == 90
    assert evidence["regions"][0]["words"][0]["text"] == "ALPHA"
    assert evidence["regions"][0]["words"][0]["box"][0] < 0.5


def test_cli_preflight_reports_digest_and_exclusive_results(worker, tmp_path, capsys):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    assert worker.main(["validate-manifest", "--manifest", str(manifest_path)]) == 0
    preflight = json.loads(capsys.readouterr().out)
    assert preflight == {"canonical_sha256": digest, "shard_ids": [manifest["sources"][0]["shard_id"]]}
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    (source_dir / f"{manifest['sources'][0]['pdf_sha256']}.pdf").write_bytes(pdf)
    result_path = tmp_path / "result.json"
    flags = ["run-shard", "--manifest", str(manifest_path), "--manifest-sha256", digest,
             "--source-root", str(source_dir), "--shard-id", str(manifest["sources"][0]["shard_id"]),
             "--execution-commit", manifest["worker_commit"], "--output", str(result_path)]
    assert worker.main(flags) == 0
    assert json.loads(result_path.read_text())["status"] == "EVIDENCE_ONLY"
    with pytest.raises(FileExistsError):
        worker.main(flags)
    summary_path = tmp_path / "summary.json"
    assert worker.main(["validate-results", "--manifest", str(manifest_path), "--manifest-sha256", digest,
                        "--results", str(result_path), "--output", str(summary_path)]) == 0
    assert json.loads(summary_path.read_text())["source_count"] == 1
