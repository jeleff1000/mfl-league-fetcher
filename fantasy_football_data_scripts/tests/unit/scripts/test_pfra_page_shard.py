"""Contract tests for the read-only PFRA page evidence worker."""

import hashlib
import http.client
import importlib.util
import io
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
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


@pytest.mark.parametrize(("method", "psm"), [("ocr", True), ("ocr", 4), ("native", 6)])
def test_manifest_rejects_invalid_or_native_psm(worker, method, psm):
    manifest = manifest_for()
    region = manifest["sources"][0]["pages"][0]["regions"][0]
    region["method"] = method
    region["psm"] = psm
    with pytest.raises(ValueError):
        worker.validate_manifest(manifest)


@pytest.mark.parametrize("psm", [None, 6])
def test_ocr_psm_is_passed_and_bound_to_result(worker, monkeypatch, psm):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",), method="ocr")
    region = manifest["sources"][0]["pages"][0]["regions"][0]
    if psm is not None:
        region["psm"] = psm
    commands = []

    class TesseractProcess:
        stdin = io.BytesIO()
        stdout = io.BytesIO(
            b"level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n"
            b"5\t1\t1\t1\t1\t1\t10\t10\t20\t10\t90\tALPHA\n"
        )

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def start_process(argv, **kwargs):
        commands.append(argv)
        return TesseractProcess()

    monkeypatch.setattr(worker.subprocess, "Popen", start_process)
    result = worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda source: pdf)
    evidence = result["sources"][0]["pages"][0]["regions"][0]
    expected_psm = 3 if psm is None else 6
    assert commands == [["tesseract", "stdin", "stdout", "--psm", str(expected_psm), "tsv"]]
    assert evidence["psm"] == expected_psm
    assert evidence["words"][0]["text"] == "ALPHA"
    worker.validate_results(manifest, worker.manifest_sha256(manifest), [result])
    evidence["psm"] = 6 if expected_psm == 3 else 3
    with pytest.raises(ValueError, match="psm"):
        worker.validate_results(manifest, worker.manifest_sha256(manifest), [result])


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


RENDERED_CROPS = {
    0: [0, 0, 0.45, 0.25],
    90: [0.75, 0, 1, 0.45],
    180: [0.55, 0.75, 1, 1],
    270: [0, 0.55, 0.25, 1],
}
ALPHA_CENTERS = {0: (0.19, 0.12), 90: (0.88, 0.19), 180: (0.81, 0.88), 270: (0.12, 0.81)}
MARKER_CENTERS = {0: (0.10, 0.067), 90: (0.933, 0.10), 180: (0.90, 0.933), 270: (0.067, 0.90)}


def rendered_frame_pdf_manifest(worker, rotation, method):
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=200, height=300)
    page.insert_text((20, 40), "ALPHA")
    page.insert_text((110, 260), "BRAVO")
    page.draw_rect(fitz.Rect(10, 10, 30, 30), color=(1, 0, 0), fill=(1, 0, 0))
    page.set_rotation(rotation)
    pdf = doc.tobytes()
    doc.close()
    digest = hashlib.sha256(pdf).hexdigest()
    manifest = manifest_for(digest)
    manifest["worker_sha256"] = hashlib.sha256(MODULE_PATH.read_bytes()).hexdigest()
    manifest["sources"][0]["byte_length"] = len(pdf)
    binding = manifest["sources"][0]["pages"][0]
    binding["coordinate_frame"] = "RENDERED_PAGE"
    binding["regions"][0] = {"region_id": "r1", "rect": RENDERED_CROPS[rotation], "method": method,
                              **({"dpi": 72} if method == "ocr" else {})}
    return pdf, manifest


@pytest.mark.parametrize("rotation", [0, 90, 180, 270])
def test_rendered_native_crop_selects_same_text_and_rotates_word_box(worker, rotation):
    pdf, manifest = rendered_frame_pdf_manifest(worker, rotation, "native")
    digest = worker.manifest_sha256(manifest)
    result = worker.run_shard(manifest, digest, manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda _source: pdf)
    page = result["sources"][0]["pages"][0]
    assert page["coordinate_frame"] == "RENDERED_PAGE"
    assert page["source_rotation"] == rotation
    words = page["regions"][0]["words"]
    assert [word["text"] for word in words] == ["ALPHA"]
    box = words[0]["box"]
    center = ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2)
    assert all(abs(actual - expected) < 0.04 for actual, expected in zip(center, ALPHA_CENTERS[rotation]))
    worker.validate_results(manifest, digest, [result])


@pytest.mark.parametrize("rotation", [0, 90, 180, 270])
def test_rendered_ocr_crop_preserves_pixel_orientation_and_displayed_box(worker, monkeypatch, rotation):
    fitz = pytest.importorskip("fitz")
    pdf, manifest = rendered_frame_pdf_manifest(worker, rotation, "ocr")
    expected_quadrant = {0: (False, False), 90: (True, False),
                         180: (True, True), 270: (False, True)}[rotation]

    def synthetic_ocr(png, _psm):
        pix = fitz.Pixmap(png)
        red = []
        for y in range(pix.height):
            for x in range(pix.width):
                index = (y * pix.width + x) * pix.n
                r, g, b = pix.samples[index:index + 3]
                if r > 180 and g < 90 and b < 90:
                    red.append((x, y))
        assert red, "the rendered crop must contain the red orientation marker"
        left, right = min(x for x, _ in red), max(x for x, _ in red)
        top, bottom = min(y for _, y in red), max(y for _, y in red)
        assert ((left + right) / 2 > pix.width / 2, (top + bottom) / 2 > pix.height / 2) == expected_quadrant
        return (b"level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n" +
                f"5\t1\t1\t1\t1\t1\t{left}\t{top}\t{right - left + 1}\t{bottom - top + 1}\t90\tMARK\n".encode())

    monkeypatch.setattr(worker, "_tesseract_output", synthetic_ocr)
    digest = worker.manifest_sha256(manifest)
    result = worker.run_shard(manifest, digest, manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda _source: pdf)
    evidence = result["sources"][0]["pages"][0]["regions"][0]
    assert evidence["status"] == "OK"
    assert [word["text"] for word in evidence["words"]] == ["MARK"]
    box = evidence["words"][0]["box"]
    center = ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2)
    assert all(abs(actual - expected) < 0.04 for actual, expected in zip(center, MARKER_CENTERS[rotation]))
    worker.validate_results(manifest, digest, [result])


def test_result_validator_rejects_rendered_frame_mismatch(worker):
    pdf, manifest = rendered_frame_pdf_manifest(worker, 90, "native")
    digest = worker.manifest_sha256(manifest)
    result = worker.run_shard(manifest, digest, manifest["sources"][0]["shard_id"],
                              manifest["worker_commit"], source_loader=lambda _source: pdf)
    result["sources"][0]["pages"][0]["coordinate_frame"] = "UNROTATED_PAGE"
    with pytest.raises(ValueError, match="coordinate frame mismatch"):
        worker.validate_results(manifest, digest, [result])


@pytest.mark.parametrize("frame", ["", "PDF_PAGE", None, 90])
def test_manifest_rejects_unknown_coordinate_frame(worker, frame):
    manifest = manifest_for()
    manifest["sources"][0]["pages"][0]["coordinate_frame"] = frame
    with pytest.raises(ValueError, match="coordinate frame"):
        worker.validate_manifest(manifest)


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


class HTTPResponse(io.BytesIO):
    def __init__(self, payload, status=200, content_length=None):
        super().__init__(payload)
        self.status = status
        self.headers = {"Content-Length": str(len(payload) if content_length is None else content_length)}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def remote_fixture(worker, monkeypatch, manifest, pdf=None, *, responses=None, base="https://source.example/pfra"):
    monkeypatch.setenv("PFRA_SOURCE_BASE_URL", base)
    monkeypatch.setenv("PFRA_SOURCE_READ_TOKEN", "private-token")
    observed = []
    payloads = responses if responses is not None else {}
    if pdf is not None:
        payloads[f"/pdfs/{manifest['sources'][0]['pdf_sha256']}.pdf"] = pdf
    payloads.setdefault(f"/manifests/{worker.manifest_sha256(manifest)}.json", worker.canonical_bytes(manifest))

    class Opener:
        def open(self, request, timeout):
            observed.append((request.full_url, dict(request.header_items()), timeout))
            suffix = request.full_url.removeprefix(base)
            item = payloads[suffix]
            if isinstance(item, Exception):
                raise item
            return item if isinstance(item, HTTPResponse) else HTTPResponse(item)

    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: Opener())
    return observed


def test_fetch_manifest_binds_hash_commit_worker_and_writes_exclusively(worker, monkeypatch, tmp_path, capsys):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    observed = remote_fixture(worker, monkeypatch, manifest, pdf)
    output = tmp_path / "manifest.json"
    args = ["fetch-manifest", "--manifest-sha256", digest, "--execution-commit", manifest["worker_commit"],
            "--output", str(output)]
    assert worker.main(args) == 0
    assert json.loads(output.read_bytes()) == manifest
    assert json.loads(capsys.readouterr().out) == {"canonical_sha256": digest, "shard_ids": [manifest["sources"][0]["shard_id"]]}
    assert observed == [(f"https://source.example/pfra/manifests/{digest}.json",
                         {"Authorization": "Bearer private-token"}, observed[0][2])]
    assert 0 < observed[0][2] <= 30
    with pytest.raises(FileExistsError):
        worker.main(args)
    assert len(observed) == 1


def test_remote_shard_reads_one_pdf_for_two_pages(worker, monkeypatch, tmp_path):
    pdf, manifest = pdf_manifest(worker)
    observed = remote_fixture(worker, monkeypatch, manifest, pdf)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(worker.canonical_bytes(manifest))
    output = tmp_path / "result.json"
    assert worker.main(["run-remote-shard", "--manifest", str(manifest_path),
                        "--manifest-sha256", worker.manifest_sha256(manifest),
                        "--shard-id", str(manifest["sources"][0]["shard_id"]),
                        "--execution-commit", manifest["worker_commit"], "--output", str(output)]) == 0
    result = json.loads(output.read_bytes())
    assert result["status"] == "EVIDENCE_ONLY"
    assert result["source_read_count"] == result["source_hash_count"] == 1
    assert [p["regions"][0]["words"][0]["text"] for p in result["sources"][0]["pages"]] == ["ALPHA", "BRAVO"]
    assert [url for url, _, _ in observed] == [f"https://source.example/pfra/pdfs/{manifest['sources'][0]['pdf_sha256']}.pdf"]
    assert list(tmp_path.iterdir()) == [manifest_path, output]


@pytest.mark.parametrize("fault", ["wrong_length", "wrong_hash", "oversize", "truncated"])
def test_remote_pdf_rejects_corrupt_or_unbounded_response(worker, monkeypatch, tmp_path, fault):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    if fault == "wrong_length":
        payload = pdf[:-1]
    elif fault == "wrong_hash":
        payload = pdf[:-1] + bytes([pdf[-1] ^ 1])
    elif fault == "oversize":
        payload = pdf + b"x"
    else:
        payload = pdf[:-1]
    remote_fixture(worker, monkeypatch, manifest, responses={f"/pdfs/{manifest['sources'][0]['pdf_sha256']}.pdf": payload})
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(worker.canonical_bytes(manifest))
    output = tmp_path / "result.json"
    with pytest.raises(ValueError, match="^(HTTP_SIZE_LIMIT|HTTP_TRUNCATED|source length mismatch|source hash mismatch)$"):
        worker.main(["run-remote-shard", "--manifest", str(manifest_path),
                     "--manifest-sha256", worker.manifest_sha256(manifest),
                     "--shard-id", str(manifest["sources"][0]["shard_id"]),
                     "--execution-commit", manifest["worker_commit"], "--output", str(output)])
    assert not output.exists()


@pytest.mark.parametrize("binding", ["manifest_hash", "worker_hash", "commit"])
def test_remote_shard_rejects_stale_binding_before_network(worker, monkeypatch, tmp_path, binding):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    if binding == "worker_hash":
        manifest["worker_sha256"] = "0" * 64
    observed = remote_fixture(worker, monkeypatch, manifest)
    path = tmp_path / "manifest.json"
    path.write_bytes(worker.canonical_bytes(manifest))
    digest = "0" * 64 if binding == "manifest_hash" else worker.manifest_sha256(manifest)
    commit = "0" * 40 if binding == "commit" else manifest["worker_commit"]
    with pytest.raises(ValueError):
        worker.main(["run-remote-shard", "--manifest", str(path), "--manifest-sha256", digest,
                     "--shard-id", str(manifest["sources"][0]["shard_id"]),
                     "--execution-commit", commit, "--output", str(tmp_path / "result.json")])
    assert observed == []


@pytest.mark.parametrize("base", ["http://source.example", "https://user@source.example", "https://source.example?q=x",
                                  "https://source.example?", "https://source.example/#frag", "https://source.example#",
                                  "https://bad host", "https://source.example/\ntrap"])
def test_remote_rejects_unsafe_base_before_network(worker, monkeypatch, tmp_path, base):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = remote_fixture(worker, monkeypatch, manifest, base=base)
    with pytest.raises(ValueError, match="SOURCE_BASE_INVALID"):
        worker.main(["fetch-manifest", "--manifest-sha256", worker.manifest_sha256(manifest),
                     "--execution-commit", manifest["worker_commit"], "--output", str(tmp_path / "manifest.json")])
    assert observed == []


def test_fetch_manifest_rejects_redirect_without_exposing_token(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    observed = remote_fixture(worker, monkeypatch, manifest, responses={
        f"/manifests/{digest}.json": urllib.error.HTTPError("https://source.example", 302, "private-token", {"Location": "https://elsewhere.example"}, None)
    })
    with pytest.raises(ValueError, match="^HTTP_REDIRECT$") as error:
        worker.main(["fetch-manifest", "--manifest-sha256", digest, "--execution-commit", manifest["worker_commit"],
                     "--output", str(tmp_path / "manifest.json")])
    assert "private-token" not in str(error.value)
    assert len(observed) == 1
    assert not (tmp_path / "manifest.json").exists()


def test_redirect_handler_never_constructs_followup_request(worker):
    handler = worker._NoRedirect()
    request = urllib.request.Request("https://source.example/manifests/" + "a" * 64 + ".json",
                                     headers={"Authorization": "Bearer private-token"})
    assert handler.redirect_request(request, None, 302, "Found", {"Location": "https://elsewhere.example"},
                                    "https://elsewhere.example") is None


def test_remote_incomplete_writes_result_and_exits_nonzero(worker, monkeypatch, tmp_path):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",), method="ocr")
    observed = remote_fixture(worker, monkeypatch, manifest, pdf)
    monkeypatch.setattr(worker.subprocess, "Popen", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()))
    path = tmp_path / "manifest.json"
    path.write_bytes(worker.canonical_bytes(manifest))
    output = tmp_path / "result.json"
    assert worker.main(["run-remote-shard", "--manifest", str(path),
                        "--manifest-sha256", worker.manifest_sha256(manifest),
                        "--shard-id", str(manifest["sources"][0]["shard_id"]),
                        "--execution-commit", manifest["worker_commit"], "--output", str(output)]) == 1
    assert json.loads(output.read_bytes())["status"] == "INCOMPLETE"
    assert len(observed) == 1


def test_remote_rejects_declared_oversize_before_body_read(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)

    class UnreadableResponse(HTTPResponse):
        def read(self, size=-1):
            raise AssertionError("oversize body was read")

    remote_fixture(worker, monkeypatch, manifest, responses={
        f"/manifests/{digest}.json": UnreadableResponse(b"", content_length=32 * 1024 * 1024 + 1)
    })
    with pytest.raises(ValueError, match="^HTTP_SIZE_LIMIT$"):
        worker.main(["fetch-manifest", "--manifest-sha256", digest, "--execution-commit", manifest["worker_commit"],
                     "--output", str(tmp_path / "manifest.json")])
    assert not (tmp_path / "manifest.json").exists()


def test_output_limit_prevents_creating_file(worker, tmp_path):
    output = tmp_path / "result.json"
    with pytest.raises(ValueError, match="^JSON output limit$"):
        worker._write_json_exclusive(output, {"words": "X" * 100}, max_bytes=32)
    assert not output.exists()


@pytest.mark.parametrize("binding", ["manifest_hash", "worker_hash", "commit"])
def test_fetch_manifest_rejects_stale_binding_without_output(worker, monkeypatch, tmp_path, binding):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    if binding == "worker_hash":
        manifest["worker_sha256"] = "0" * 64
    digest = worker.manifest_sha256(manifest)
    requested_digest = "0" * 64 if binding == "manifest_hash" else digest
    remote_fixture(worker, monkeypatch, manifest, responses={
        f"/manifests/{requested_digest}.json": worker.canonical_bytes(manifest)
    })
    output = tmp_path / "manifest.json"
    with pytest.raises(ValueError):
        worker.main(["fetch-manifest", "--manifest-sha256", requested_digest,
                     "--execution-commit", "0" * 40 if binding == "commit" else manifest["worker_commit"],
                     "--output", str(output)])
    assert not output.exists()


def test_fetch_manifest_reports_generic_transport_error(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    remote_fixture(worker, monkeypatch, manifest, responses={
        f"/manifests/{digest}.json": urllib.error.URLError("private-token at https://secret.example")
    })
    with pytest.raises(ValueError, match="^HTTP_TRANSPORT$"):
        worker.main(["fetch-manifest", "--manifest-sha256", digest,
                     "--execution-commit", manifest["worker_commit"], "--output", str(tmp_path / "manifest.json")])


def test_bad_status_line_cannot_expose_server_echoed_token(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    remote_fixture(worker, monkeypatch, manifest, responses={
        f"/manifests/{digest}.json": http.client.BadStatusLine("sentinel-private-token")
    })
    output = tmp_path / "manifest.json"
    with pytest.raises(ValueError, match="^HTTP_TRANSPORT$") as error:
        worker.main(["fetch-manifest", "--manifest-sha256", digest,
                     "--execution-commit", manifest["worker_commit"], "--output", str(output)])
    assert "sentinel-private-token" not in str(error.value)
    assert error.value.__cause__ is None
    assert not output.exists()


def test_empty_optional_read_token_sends_no_authorization(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = remote_fixture(worker, monkeypatch, manifest)
    monkeypatch.setenv("PFRA_SOURCE_READ_TOKEN", "")
    digest = worker.manifest_sha256(manifest)
    assert worker.main(["fetch-manifest", "--manifest-sha256", digest,
                        "--execution-commit", manifest["worker_commit"],
                        "--output", str(tmp_path / "manifest.json")]) == 0
    assert observed[0][1] == {}


def test_ten_remote_shards_cover_manifest_and_merger_rejects_missing_one(worker, monkeypatch, tmp_path):
    manifest = None
    pdf_responses = {}
    for shard in range(10):
        for nonce in range(100):
            pdf, candidate = pdf_manifest(worker, text=(f"SHARD{shard}TRY{nonce}",))
            if candidate["sources"][0]["shard_id"] == shard:
                break
        else:
            pytest.fail(f"could not generate source for shard {shard}")
        source = candidate["sources"][0]
        source["pages"][0]["visual_id"] = f"visual-{shard}"
        source["pages"][0]["occurrences"][0]["source_occurrence_id"] = f"occ-{shard}"
        if manifest is None:
            manifest = candidate
        else:
            manifest["sources"].append(source)
        pdf_responses[f"/pdfs/{source['pdf_sha256']}.pdf"] = pdf
    digest = worker.manifest_sha256(manifest)
    observed = remote_fixture(worker, monkeypatch, manifest, responses=pdf_responses)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(worker.canonical_bytes(manifest))
    results = []
    for shard in range(10):
        output = tmp_path / f"result-{shard}.json"
        assert worker.main(["run-remote-shard", "--manifest", str(manifest_path),
                            "--manifest-sha256", digest, "--shard-id", str(shard),
                            "--execution-commit", manifest["worker_commit"],
                            "--output", str(output)]) == 0
        results.append(json.loads(output.read_bytes()))
    summary = worker.validate_results(manifest, digest, results)
    assert summary["shard_ids"] == list(range(10))
    assert (summary["source_count"], summary["page_count"], summary["region_count"]) == (10, 10, 10)
    assert summary["word_count"] == 10
    assert len(observed) == 10
    assert {url for url, _, _ in observed} == {
        f"https://source.example/pfra/pdfs/{source['pdf_sha256']}.pdf" for source in manifest["sources"]
    }
    with pytest.raises(ValueError, match="missing or extra result"):
        worker.validate_results(manifest, digest, results[:-1])


def parallel_local_sources(worker, tmp_path):
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    manifest = None
    expected_words = []
    target_shard = None
    for index in range(4):
        for attempt in range(100):
            word = f"WORD{index}TRY{attempt}"
            pdf, candidate = pdf_manifest(worker, text=(word,))
            shard = candidate["sources"][0]["shard_id"]
            if target_shard is None or shard == target_shard:
                break
        else:
            pytest.fail("could not generate four distinct sources in one shard")
        target_shard = shard
        source = candidate["sources"][0]
        source["pages"][0]["visual_id"] = f"visual-{index}"
        source["pages"][0]["occurrences"][0]["source_occurrence_id"] = f"occ-{index}"
        (source_dir / f"{source['pdf_sha256']}.pdf").write_bytes(pdf)
        if manifest is None:
            manifest = candidate
        else:
            manifest["sources"].append(source)
        expected_words.append(word)
    return source_dir, manifest, expected_words


def test_parallel_local_cli_keeps_source_order_and_exact_counters(worker, tmp_path):
    source_dir, manifest, expected_words = parallel_local_sources(worker, tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(worker.canonical_bytes(manifest))
    output = tmp_path / "result.json"
    command = [sys.executable, str(MODULE_PATH), "run-shard", "--manifest", str(manifest_path),
               "--manifest-sha256", worker.manifest_sha256(manifest), "--source-root", str(source_dir),
               "--shard-id", str(manifest["sources"][0]["shard_id"]),
               "--execution-commit", manifest["worker_commit"], "--source-workers", "4", "--output", str(output)]
    completed = subprocess.run(command, capture_output=True, text=True, check=False, env=os.environ.copy())
    assert completed.returncode == 0, completed.stderr
    result = json.loads(output.read_bytes())
    assert result["source_read_count"] == result["source_hash_count"] == 4
    assert [source["pdf_sha256"] for source in result["sources"]] == [
        source["pdf_sha256"] for source in manifest["sources"]
    ]
    assert [source["pages"][0]["regions"][0]["words"][0]["text"] for source in result["sources"]] == expected_words
    worker.validate_results(manifest, worker.manifest_sha256(manifest), [result])


def test_parallel_local_cli_fails_without_partial_result_on_bad_source(worker, tmp_path):
    source_dir, manifest, _ = parallel_local_sources(worker, tmp_path)
    bad_source = manifest["sources"][2]
    (source_dir / f"{bad_source['pdf_sha256']}.pdf").write_bytes(b"corrupt")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(worker.canonical_bytes(manifest))
    output = tmp_path / "result.json"
    completed = subprocess.run(
        [sys.executable, str(MODULE_PATH), "run-shard", "--manifest", str(manifest_path),
         "--manifest-sha256", worker.manifest_sha256(manifest), "--source-root", str(source_dir),
         "--shard-id", str(manifest["sources"][0]["shard_id"]),
         "--execution-commit", manifest["worker_commit"], "--source-workers", "4", "--output", str(output)],
        capture_output=True, text=True, check=False, env=os.environ.copy(),
    )
    assert completed.returncode != 0
    assert not output.exists()
    assert json.loads(completed.stderr)["error"] == "source length mismatch"
    assert "corrupt" not in completed.stderr


def test_parallel_rejects_injected_source_loader_before_fetch(worker):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    calls = []

    def load(_source):
        calls.append(1)
        return pdf

    with pytest.raises(ValueError, match="SOURCE_LOADER_SERIAL_ONLY"):
        worker.run_shard(manifest, worker.manifest_sha256(manifest), manifest["sources"][0]["shard_id"],
                         manifest["worker_commit"], source_loader=load, source_workers=2)
    assert calls == []


def test_tesseract_child_limits_openmp_without_changing_parent(worker, monkeypatch):
    monkeypatch.setenv("OMP_THREAD_LIMIT", "8")
    captured = []

    class Process:
        stdin = io.BytesIO()
        stdout = io.BytesIO(b"header\n")

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def start(_argv, **kwargs):
        captured.append(kwargs["env"])
        return Process()

    monkeypatch.setattr(worker.subprocess, "Popen", start)
    assert worker._tesseract_output(b"png", 3) == b"header\n"
    assert captured[0]["OMP_THREAD_LIMIT"] == "1"
    assert os.environ["OMP_THREAD_LIMIT"] == "8"


RELEASE_BASE = "https://github.com/jeleff1000/mfl-league-fetcher/releases/download/pfra-stage-1"
SIGNED_ASSET = "https://release-assets.githubusercontent.com/signed-asset?token=private-signed-url"


def release_fixture(worker, monkeypatch, payload, *, target=SIGNED_ASSET, second_target=None, token=""):
    monkeypatch.setenv("PFRA_SOURCE_BASE_URL", RELEASE_BASE)
    monkeypatch.setenv("PFRA_SOURCE_READ_TOKEN", token)
    observed = []

    class Opener:
        def __init__(self, handler):
            self.handler = handler

        def open(self, request, timeout):
            observed.append((request.full_url, dict(request.header_items()), timeout))
            redirected = self.handler.redirect_request(request, None, 302, "Found", {"Location": target}, target)
            if redirected is None:
                raise urllib.error.HTTPError(request.full_url, 302, target, {"Location": target}, None)
            observed.append((redirected.full_url, dict(redirected.header_items()), timeout))
            if second_target is not None:
                chained = self.handler.redirect_request(redirected, None, 302, "Found",
                                                        {"Location": second_target}, second_target)
                if chained is None:
                    raise urllib.error.HTTPError(redirected.full_url, 302, second_target,
                                                 {"Location": second_target}, None)
            return HTTPResponse(payload)

    def make_opener(handler):
        return Opener(handler)

    monkeypatch.setattr(urllib.request, "build_opener", make_opener)
    return observed


def test_release_manifest_allows_one_signed_asset_redirect_without_authorization(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    observed = release_fixture(worker, monkeypatch, worker.canonical_bytes(manifest))
    output = tmp_path / "manifest.json"
    assert worker.main(["fetch-manifest", "--manifest-sha256", digest,
                        "--execution-commit", manifest["worker_commit"], "--output", str(output)]) == 0
    assert json.loads(output.read_bytes()) == manifest
    assert observed[0][0] == f"{RELEASE_BASE}/manifests-{digest}.json"
    assert observed[1][0] == SIGNED_ASSET
    assert observed[0][1] == observed[1][1] == {}


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_release_redirect_closes_unbounded_body_without_reading(worker, status):
    origin = RELEASE_BASE + "/manifests-" + "a" * 64 + ".json"
    request = urllib.request.Request(origin, method="GET")
    request.timeout = 20
    handler = worker._ReleaseRedirect(origin)

    class UnboundedBody:
        closed = False

        def read(self, *_args):
            raise AssertionError("redirect body must not be drained")

        def close(self):
            self.closed = True

    body = UnboundedBody()
    followed = []

    class Parent:
        def open(self, next_request, timeout):
            followed.append((next_request.full_url, dict(next_request.header_items()), timeout))
            return HTTPResponse(b"manifest")

    handler.parent = Parent()
    response = getattr(handler, f"http_error_{status}")(
        request, body, status, "Found", {"location": SIGNED_ASSET}
    )
    assert response.read() == b"manifest"
    assert body.closed
    assert followed == [(SIGNED_ASSET, {}, 20)]


def test_release_pdf_redirect_still_checks_pdf_binding(worker, monkeypatch, tmp_path):
    pdf, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = release_fixture(worker, monkeypatch, pdf)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(worker.canonical_bytes(manifest))
    output = tmp_path / "result.json"
    assert worker.main(["run-remote-shard", "--manifest", str(manifest_path),
                        "--manifest-sha256", worker.manifest_sha256(manifest),
                        "--shard-id", str(manifest["sources"][0]["shard_id"]),
                        "--execution-commit", manifest["worker_commit"], "--output", str(output)]) == 0
    assert json.loads(output.read_bytes())["sources"][0]["pages"][0]["regions"][0]["words"][0]["text"] == "ALPHA"
    assert observed[0][0] == f"{RELEASE_BASE}/pdfs-{manifest['sources'][0]['pdf_sha256']}.pdf"
    assert observed[1][0] == SIGNED_ASSET


@pytest.mark.parametrize("base", [
    "https://github.com/other/repo/releases/download/pfra-stage-1",
    "https://evil.example/jeleff1000/mfl-league-fetcher/releases/download/pfra-stage-1",
    "https://github.com.evil.example/jeleff1000/mfl-league-fetcher/releases/download/pfra-stage-1",
    RELEASE_BASE + "?secret=1", RELEASE_BASE + "#fragment", RELEASE_BASE + "/../escape",
    RELEASE_BASE.replace("github.com", "github.com:443"),
    RELEASE_BASE.replace("pfra-stage-1", "bad%2Ftag"),
])
def test_release_rejects_unsafe_base_before_network(worker, monkeypatch, tmp_path, base):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = release_fixture(worker, monkeypatch, worker.canonical_bytes(manifest))
    monkeypatch.setenv("PFRA_SOURCE_BASE_URL", base)
    with pytest.raises(ValueError, match="^SOURCE_BASE_INVALID$"):
        worker.main(["fetch-manifest", "--manifest-sha256", worker.manifest_sha256(manifest),
                     "--execution-commit", manifest["worker_commit"], "--output", str(tmp_path / "manifest.json")])
    assert observed == []


def test_release_rejects_read_token_before_network(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = release_fixture(worker, monkeypatch, worker.canonical_bytes(manifest), token="private-token")
    with pytest.raises(ValueError, match="^SOURCE_TOKEN_INVALID$"):
        worker.main(["fetch-manifest", "--manifest-sha256", worker.manifest_sha256(manifest),
                     "--execution-commit", manifest["worker_commit"], "--output", str(tmp_path / "manifest.json")])
    assert observed == []


@pytest.mark.parametrize("target", [
    "http://release-assets.githubusercontent.com/signed-asset?token=secret",
    "https://evil.example/signed-asset?token=secret",
    "https://release-assets.githubusercontent.com.evil.example/signed-asset",
    "https://user@release-assets.githubusercontent.com/signed-asset",
    "https://release-assets.githubusercontent.com:443/signed-asset",
    "https://release-assets.githubusercontent.com/signed-asset#fragment",
    "https://release-assets.githubusercontent.com/signed-asset\nHeader: secret",
])
def test_release_rejects_bad_redirect_without_signed_url_leak(worker, monkeypatch, tmp_path, target):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = release_fixture(worker, monkeypatch, worker.canonical_bytes(manifest), target=target)
    output = tmp_path / "manifest.json"
    with pytest.raises(ValueError, match="^HTTP_REDIRECT$") as error:
        worker.main(["fetch-manifest", "--manifest-sha256", worker.manifest_sha256(manifest),
                     "--execution-commit", manifest["worker_commit"], "--output", str(output)])
    assert "secret" not in str(error.value)
    assert not output.exists()
    assert len(observed) == 1


def test_release_rejects_chained_redirect(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    observed = release_fixture(worker, monkeypatch, worker.canonical_bytes(manifest), second_target=SIGNED_ASSET)
    with pytest.raises(ValueError, match="^HTTP_REDIRECT$"):
        worker.main(["fetch-manifest", "--manifest-sha256", worker.manifest_sha256(manifest),
                     "--execution-commit", manifest["worker_commit"], "--output", str(tmp_path / "manifest.json")])
    assert len(observed) == 2


def test_fetch_manifest_rejects_oversize_and_bad_json_cleanly(worker, monkeypatch, tmp_path):
    _, manifest = pdf_manifest(worker, text=("ALPHA",))
    digest = worker.manifest_sha256(manifest)
    for payload, error in [(b"x" * (32 * 1024 * 1024 + 1), "HTTP_SIZE_LIMIT"),
                           (b"private-token is not JSON", "INPUT_ERROR")]:
        remote_fixture(worker, monkeypatch, manifest, responses={f"/manifests/{digest}.json": payload})
        with pytest.raises(ValueError, match=f"^{error}$"):
            worker.main(["fetch-manifest", "--manifest-sha256", digest,
                         "--execution-commit", manifest["worker_commit"], "--output", str(tmp_path / "manifest.json")])
        assert not (tmp_path / "manifest.json").exists()
