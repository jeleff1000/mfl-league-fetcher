"""Bounded, read-only PFRA page evidence worker.

This module collects evidence for locked candidate contracts. It never accepts a
shape, reads a registry, or writes source data.
"""

import argparse
import hashlib
import http.client
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from pathlib import Path


SHA256 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")
IDENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
RELEASE_PATH = re.compile(r"/jeleff1000/mfl-league-fetcher/releases/download/[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
MAX_PDF_BYTES = 128 * 1024 * 1024
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024
HTTP_TIMEOUT_SECONDS = 20
MAX_PAGES = 2000
MAX_REGIONS = 32
MAX_RENDER_PIXELS = 4_000_000
MAX_OCR_OUTPUT = 1_000_000
MAX_WORDS = 10_000
OCR_TIMEOUT_SECONDS = 20


def _fail(condition, message):
    if not condition:
        raise ValueError(message)


def _object(value, keys, optional=()):
    _fail(isinstance(value, dict), "expected object")
    _fail(set(keys) <= value.keys() and value.keys() <= set(keys) | set(optional), "invalid object fields")


def _id(value):
    _fail(isinstance(value, str) and IDENT.fullmatch(value), "invalid identifier")


def _sha(value):
    _fail(isinstance(value, str) and SHA256.fullmatch(value), "invalid sha256")


def _integer(value, minimum, maximum):
    _fail(type(value) is int and minimum <= value <= maximum, "invalid integer")


def _nonempty_list(value):
    _fail(isinstance(value, list) and len(value) > 0, "expected nonempty list")


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def manifest_sha256(manifest):
    return hashlib.sha256(canonical_bytes(manifest)).hexdigest()


def validate_manifest(manifest):
    """Validate an immutable v1 manifest without loading PDF/OCR dependencies."""
    _object(manifest, ("schema_version", "batch_id", "worker_sha256", "worker_commit", "sources"))
    _fail(type(manifest["schema_version"]) is int and manifest["schema_version"] == 1, "unsupported schema")
    _id(manifest["batch_id"])
    _sha(manifest["worker_sha256"])
    _fail(isinstance(manifest["worker_commit"], str) and COMMIT.fullmatch(manifest["worker_commit"]), "invalid commit")
    _nonempty_list(manifest["sources"])
    seen_sources = set()
    seen_visuals = set()
    seen_occurrences = set()
    contract_by_shape = {}
    occurrence_bindings = {}
    nonempty_shards = set()
    for source in manifest["sources"]:
        _object(source, ("pdf_sha256", "byte_length", "page_count", "shard_id", "pages"))
        pdf_sha = source["pdf_sha256"]
        _sha(pdf_sha)
        _fail(pdf_sha not in seen_sources, "duplicate source")
        seen_sources.add(pdf_sha)
        _integer(source["byte_length"], 1, MAX_PDF_BYTES)
        _integer(source["page_count"], 1, MAX_PAGES)
        _integer(source["shard_id"], 0, 9)
        _fail(source["shard_id"] == int(pdf_sha, 16) % 10, "wrong source shard")
        nonempty_shards.add(source["shard_id"])
        _nonempty_list(source["pages"])
        _fail(len(source["pages"]) <= source["page_count"], "too many pages")
        seen_pages = set()
        for page in source["pages"]:
            _object(page, ("page_number", "visual_id", "render_sha256", "occurrences", "candidate_contracts", "regions"),
                    ("coordinate_frame",))
            _fail(page.get("coordinate_frame", "UNROTATED_PAGE") in ("UNROTATED_PAGE", "RENDERED_PAGE"),
                  "invalid coordinate frame")
            _integer(page["page_number"], 1, source["page_count"])
            _fail(page["page_number"] not in seen_pages, "duplicate page")
            seen_pages.add(page["page_number"])
            _id(page["visual_id"])
            _fail(page["visual_id"] not in seen_visuals, "duplicate visual")
            seen_visuals.add(page["visual_id"])
            _sha(page["render_sha256"])
            _nonempty_list(page["occurrences"])
            for occurrence in page["occurrences"]:
                _object(occurrence, ("source_occurrence_id", "page_number", "cohort_id", "document_sha256"))
                _id(occurrence["source_occurrence_id"])
                _id(occurrence["cohort_id"])
                _sha(occurrence["document_sha256"])
                _integer(occurrence["page_number"], 1, MAX_PAGES)
                key = (occurrence["source_occurrence_id"], occurrence["page_number"], occurrence["cohort_id"])
                _fail(key not in seen_occurrences, "duplicate occurrence")
                seen_occurrences.add(key)
                page_key = key[:2]
                binding = (occurrence["document_sha256"], page["visual_id"])
                _fail(page_key not in occurrence_bindings or occurrence_bindings[page_key] == binding,
                      "contradictory occurrence binding")
                occurrence_bindings[page_key] = binding
            _nonempty_list(page["candidate_contracts"])
            seen_contracts = set()
            for contract in page["candidate_contracts"]:
                _object(contract, ("shape_id", "contract_sha256"), ("rank_hint", "feature_hints"))
                _id(contract["shape_id"])
                _sha(contract["contract_sha256"])
                _fail(contract["shape_id"] not in seen_contracts, "duplicate candidate")
                seen_contracts.add(contract["shape_id"])
                previous_hash = contract_by_shape.setdefault(contract["shape_id"], contract["contract_sha256"])
                _fail(previous_hash == contract["contract_sha256"], "conflicting candidate contract")
                if "rank_hint" in contract:
                    _integer(contract["rank_hint"], 1, 1000000)
                if "feature_hints" in contract:
                    _fail(isinstance(contract["feature_hints"], list) and len(contract["feature_hints"]) <= 32, "invalid hints")
                    for hint in contract["feature_hints"]:
                        _id(hint)
            _nonempty_list(page["regions"])
            _fail(len(page["regions"]) <= MAX_REGIONS, "too many regions")
            seen_regions = set()
            for region in page["regions"]:
                _object(region, ("region_id", "rect", "method"), ("dpi", "psm", "crop_rotation"))
                _id(region["region_id"])
                _fail(region["region_id"] not in seen_regions, "duplicate region")
                seen_regions.add(region["region_id"])
                rect = region["rect"]
                _fail(isinstance(rect, list) and len(rect) == 4 and all(type(x) in (int, float) and 0 <= x <= 1 for x in rect), "invalid crop")
                _fail(rect[0] < rect[2] and rect[1] < rect[3], "empty crop")
                _fail(region["method"] in ("native", "ocr"), "invalid method")
                if "dpi" in region:
                    _integer(region["dpi"], 36, 200)
                _fail(region["method"] == "ocr" or "dpi" not in region, "native region has dpi")
                if "psm" in region:
                    _fail(region["method"] == "ocr", "native region has psm")
                    _fail(type(region["psm"]) is int and region["psm"] in (3, 6), "invalid psm")
                if "crop_rotation" in region:
                    _fail(region["method"] == "ocr", "native region has crop rotation")
                    _fail(type(region["crop_rotation"]) is int and region["crop_rotation"] in (0, 90),
                          "invalid crop rotation")
    return {"canonical_sha256": manifest_sha256(manifest), "shard_ids": sorted(nonempty_shards)}


def _source_bytes(source, source_root, source_loader):
    if source_loader is None:
        _fail(source_root is not None, "source root required")
        path = Path(source_root) / (source["pdf_sha256"] + ".pdf")
        _fail(path.is_file() and path.stat().st_size == source["byte_length"], "source length mismatch")
        data = path.read_bytes()
    else:
        data = source_loader(source)
    _fail(isinstance(data, bytes) and len(data) == source["byte_length"], "source length mismatch")
    _fail(hashlib.sha256(data).hexdigest() == source["pdf_sha256"], "source hash mismatch")
    return data


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class _ReleaseRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, origin_url):
        self.origin_url = origin_url
        self.followed = False

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        if self.followed or request.full_url != self.origin_url or code not in (301, 302, 303, 307, 308):
            return None
        if not newurl or any(not 33 <= ord(char) <= 126 for char in newurl) or \
                "#" in newurl or "\\" in newurl:
            return None
        try:
            target = urllib.parse.urlsplit(newurl)
        except ValueError:
            return None
        if target.scheme != "https" or target.netloc != "release-assets.githubusercontent.com" or \
                not target.path.startswith("/"):
            return None
        self.followed = True
        # The signed URL is public and must receive no header from the first request.
        return urllib.request.Request(newurl, method="GET")

    def http_error_302(self, request, response, code, msg, headers):
        location = (headers.get("Location") or headers.get("location") or
                    headers.get("URI") or headers.get("uri"))
        followup = self.redirect_request(request, response, code, msg, headers, location)
        # urllib's default handler drains the entire redirect body here.
        response.close()
        if followup is None:
            return None
        return self.parent.open(followup, timeout=getattr(request, "timeout", HTTP_TIMEOUT_SECONDS))

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302


def _source_base():
    base = os.environ.get("PFRA_SOURCE_BASE_URL", "")
    _fail(base and all(33 <= ord(char) <= 126 for char in base) and
          "?" not in base and "#" not in base, "SOURCE_BASE_INVALID")
    try:
        parts = urllib.parse.urlsplit(base)
        host = parts.hostname
        port = parts.port
    except ValueError:
        raise ValueError("SOURCE_BASE_INVALID") from None
    _fail(parts.scheme == "https" and host and not parts.username and not parts.password and
          not parts.query and not parts.fragment and "@" not in parts.netloc and
          re.fullmatch(r"[A-Za-z0-9.-]+", host) and not host.startswith(".") and
          not host.endswith(".") and ".." not in host and
          (port is None or 1 <= port <= 65535), "SOURCE_BASE_INVALID")
    release_route = host == "github.com" or "/releases/download/" in parts.path
    if release_route:
        _fail(parts.netloc == "github.com" and RELEASE_PATH.fullmatch(parts.path), "SOURCE_BASE_INVALID")
    return base.rstrip("/"), release_route


def _fetch_source(kind, digest, max_bytes, *, expected_length=None):
    _sha(digest)
    base, release_route = _source_base()
    token = os.environ.get("PFRA_SOURCE_READ_TOKEN")
    _fail(not token or all(33 <= ord(char) <= 126 for char in token),
          "SOURCE_TOKEN_INVALID")
    _fail(not release_route or not token, "SOURCE_TOKEN_INVALID")
    if release_route:
        suffix = f"/manifests-{digest}.json" if kind == "manifest" else f"/pdfs-{digest}.pdf"
    else:
        suffix = f"/manifests/{digest}.json" if kind == "manifest" else f"/pdfs/{digest}.pdf"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    request = urllib.request.Request(base + suffix, headers=headers, method="GET")
    opener = urllib.request.build_opener(_ReleaseRedirect(request.full_url) if release_route else _NoRedirect())
    try:
        with opener.open(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status = response.status
            _fail(status == 200, "HTTP_REDIRECT" if 300 <= status < 400 else "HTTP_STATUS")
            declared = response.headers.get("Content-Length")
            if declared is not None:
                _fail(declared.isdecimal(), "HTTP_LENGTH_INVALID")
                _fail(int(declared) <= max_bytes, "HTTP_SIZE_LIMIT")
                if expected_length is not None:
                    _fail(int(declared) == expected_length, "HTTP_TRUNCATED")
            data = response.read(max_bytes + 1)
            _fail(len(data) <= max_bytes, "HTTP_SIZE_LIMIT")
            if declared is not None:
                _fail(len(data) == int(declared), "HTTP_TRUNCATED")
            if expected_length is not None:
                _fail(len(data) == expected_length, "HTTP_TRUNCATED")
            return data
    except urllib.error.HTTPError as exc:
        raise ValueError("HTTP_REDIRECT" if 300 <= exc.code < 400 else "HTTP_STATUS") from None
    except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError):
        raise ValueError("HTTP_TRANSPORT") from None


def _word(x0, y0, x1, y1, text, width, height):
    _fail(len(text) <= 256, "word too long")
    box = [round(max(0.0, min(1.0, x0 / width)), 6), round(max(0.0, min(1.0, y0 / height)), 6),
           round(max(0.0, min(1.0, x1 / width)), 6), round(max(0.0, min(1.0, y1 / height)), 6)]
    return {"text": text, "box": box}


def _native_words(page, clip, coordinate_frame):
    raw = page.get_text("words", clip=clip, sort=True)
    _fail(len(raw) <= MAX_WORDS, "too many words")
    if coordinate_frame == "RENDERED_PAGE":
        import fitz

        return [_word(*(fitz.Rect(*item[:4]) * page.rotation_matrix), str(item[4]),
                      page.rect.width, page.rect.height) for item in raw]
    return [_word(*item[:4], str(item[4]), page.rect.width, page.rect.height) for item in raw]


def _ocr_words(page, clip, dpi, psm, crop_rotation=0):
    # Render only the requested crop. PNG exists in memory for this call only.
    import fitz

    scale = dpi / 72
    pixels = int(clip.width * scale + 1) * int(clip.height * scale + 1)
    _fail(0 < pixels <= MAX_RENDER_PIXELS, "render pixel limit")
    matrix = fitz.Matrix(scale, scale).prerotate(crop_rotation)
    pix = page.get_pixmap(matrix=matrix, clip=clip, alpha=False)
    _fail(pix.width * pix.height <= MAX_RENDER_PIXELS, "render pixel limit")
    png = pix.tobytes("png")
    width, height = pix.width, pix.height
    origin_x, origin_y = pix.x, pix.y
    del pix
    output = _tesseract_output(png, psm)
    del png
    words = []
    for row in output.decode("utf-8", errors="replace").splitlines()[1:]:
        columns = row.split("\t", 11)
        if len(columns) != 12 or not columns[11].strip():
            continue
        _fail(len(words) < MAX_WORDS, "too many words")
        try:
            left, top, w, h = (int(columns[i]) for i in (6, 7, 8, 9))
        except ValueError:
            raise ValueError("OCR_BAD_OUTPUT") from None
        text = columns[11].strip()
        if crop_rotation:
            # OCR pixels are local to the rotated pixmap. Undo only this crop
            # transform; page orientation already belongs to the requested frame.
            x0, y0, x1, y1 = fitz.Rect(origin_x + left, origin_y + top,
                                      origin_x + left + w, origin_y + top + h) * ~matrix
        else:
            # Preserve the established mapping for old manifests/evidence.
            x0 = clip.x0 + left * clip.width / width
            y0 = clip.y0 + top * clip.height / height
            x1 = clip.x0 + (left + w) * clip.width / width
            y1 = clip.y0 + (top + h) * clip.height / height
        words.append(_word(x0, y0, x1, y1, text, page.rect.width, page.rect.height))
    return words


def _tesseract_output(png, psm):
    """Stream at most MAX_OCR_OUTPUT+1 bytes, killing slow or noisy OCR."""
    ocr_env = os.environ.copy()
    ocr_env["OMP_THREAD_LIMIT"] = "1"
    try:
        process = subprocess.Popen(["tesseract", "stdin", "stdout", "--psm", str(psm), "tsv"],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, env=ocr_env)
    except OSError:
        raise ValueError("OCR_UNAVAILABLE") from None
    output = {}

    def feed():
        try:
            process.stdin.write(png)
        except (BrokenPipeError, OSError):
            pass
        finally:
            process.stdin.close()

    def drain():
        try:
            output["bytes"] = process.stdout.read(MAX_OCR_OUTPUT + 1)
        except OSError:
            output["bytes"] = b""

    writer = threading.Thread(target=feed, daemon=True)
    reader = threading.Thread(target=drain, daemon=True)
    deadline = time.monotonic() + OCR_TIMEOUT_SECONDS
    try:
        writer.start()
        reader.start()
        reader.join(max(0, deadline - time.monotonic()))
        _fail(not reader.is_alive(), "OCR_TIMEOUT")
        data = output.get("bytes", b"")
        _fail(len(data) <= MAX_OCR_OUTPUT, "OCR_OUTPUT_LIMIT")
        try:
            return_code = process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            raise ValueError("OCR_TIMEOUT") from None
        _fail(return_code == 0, "OCR_FAILED")
        return data
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()
        reader.join(0.1)
        writer.join(0.1)


def _page_evidence(doc, binding):
    import fitz

    evidence = {key: binding[key] for key in ("page_number", "visual_id", "render_sha256", "occurrences", "candidate_contracts")}
    evidence["regions"] = []
    page = doc.load_page(binding["page_number"] - 1)
    evidence["source_rotation"] = page.rotation
    frame = binding.get("coordinate_frame", "UNROTATED_PAGE")
    evidence["coordinate_frame"] = frame
    if frame == "UNROTATED_PAGE" and page.rotation:
        page.set_rotation(0)  # In-memory document only; source bytes remain immutable.
    _fail(0 < page.rect.width <= 14400 and 0 < page.rect.height <= 14400, "page dimension limit")
    for region in binding["regions"]:
        item = {"region_id": region["region_id"], "rect": region["rect"], "method": region["method"],
                "status": "OK", "error": None, "words": []}
        if "dpi" in region:
            item["dpi"] = region["dpi"]
        if region["method"] == "ocr":
            item["psm"] = region.get("psm", 3)
        if "crop_rotation" in region:
            item["crop_rotation"] = region["crop_rotation"]
        x0, y0, x1, y1 = region["rect"]
        clip = fitz.Rect(x0 * page.rect.width, y0 * page.rect.height,
                         x1 * page.rect.width, y1 * page.rect.height)
        try:
            native_clip = clip * page.derotation_matrix if frame == "RENDERED_PAGE" else clip
            item["words"] = (_native_words(page, native_clip, frame) if region["method"] == "native"
                             else _ocr_words(page, clip, region.get("dpi", 150), item["psm"],
                                             region.get("crop_rotation", 0)))
        except (ValueError, RuntimeError) as exc:
            item["status"] = "ERROR"
            item["error"] = str(exc) if str(exc) in {
                "OCR_TIMEOUT", "OCR_UNAVAILABLE", "OCR_FAILED", "OCR_OUTPUT_LIMIT", "OCR_BAD_OUTPUT",
                "render pixel limit", "too many words", "word too long",
            } else "EXTRACTION_ERROR"
        evidence["regions"].append(item)
    return evidence


def _source_evidence(source, source_root, source_loader, remote_source):
    """Fetch, verify, and extract one source wholly within this process."""
    import fitz

    if remote_source:
        def loader(binding):
            return _fetch_source("pdf", binding["pdf_sha256"], binding["byte_length"],
                                 expected_length=binding["byte_length"])

        data = _source_bytes(source, None, loader)
    else:
        data = _source_bytes(source, source_root, source_loader)
    with fitz.open(stream=data, filetype="pdf") as doc:
        _fail(len(doc) == source["page_count"], "source page count mismatch")
        pages = [_page_evidence(doc, page) for page in source["pages"]]
    return {"pdf_sha256": source["pdf_sha256"], "byte_length": source["byte_length"],
            "page_count": source["page_count"], "pages": pages}


def run_shard(manifest, expected_manifest_sha256, shard_id, execution_commit, *, source_root=None,
              source_loader=None, remote_source=False, source_workers=1):
    """Read/hash each selected source once and return evidence without classification."""
    started = time.monotonic()
    validated = validate_manifest(manifest)
    _sha(expected_manifest_sha256)
    _fail(validated["canonical_sha256"] == expected_manifest_sha256, "manifest hash mismatch")
    _integer(shard_id, 0, 9)
    _fail(execution_commit == manifest["worker_commit"], "execution commit mismatch")
    code_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    _fail(code_sha == manifest["worker_sha256"], "worker hash mismatch")
    selected = [source for source in manifest["sources"] if source["shard_id"] == shard_id]
    _nonempty_list(selected)
    _integer(source_workers, 1, 4)
    _fail(not (source_loader is not None and source_workers > 1), "SOURCE_LOADER_SERIAL_ONLY")
    _fail(not (remote_source and source_loader is not None), "SOURCE_LOADER_CONFLICT")

    result = {"schema_version": 1, "batch_id": manifest["batch_id"],
              "manifest_sha256": expected_manifest_sha256, "worker_sha256": code_sha,
              "execution_commit": execution_commit, "shard_id": shard_id,
              "status": "EVIDENCE_ONLY", "sources": [], "source_read_count": 0,
              "source_hash_count": 0, "elapsed_ms": 0}
    if source_workers == 1:
        source_results = (_source_evidence(source, source_root, source_loader, remote_source) for source in selected)
        pool = None
    else:
        pool = ProcessPoolExecutor(max_workers=source_workers)
        source_results = pool.map(_source_evidence, selected, repeat(source_root), repeat(None), repeat(remote_source))
    try:
        for source_result in source_results:
            result["source_read_count"] += 1
            result["source_hash_count"] += 1
            result["sources"].append(source_result)
            if any(region["status"] != "OK" for page in source_result["pages"] for region in page["regions"]):
                result["status"] = "INCOMPLETE"
    except ValueError:
        raise
    except Exception:
        raise ValueError("SOURCE_WORKER_ERROR") from None
    finally:
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)
    result["elapsed_ms"] = round((time.monotonic() - started) * 1000)
    return result


def validate_results(manifest, expected_manifest_sha256, results):
    """Reject incomplete, stale, missing or unbound shard artifacts."""
    validated = validate_manifest(manifest)
    _sha(expected_manifest_sha256)
    _fail(validated["canonical_sha256"] == expected_manifest_sha256, "manifest hash mismatch")
    _fail(isinstance(results, list) and len(results) == len(validated["shard_ids"]), "missing or extra result")
    seen_shards = set()
    source_count = page_count = region_count = word_count = 0
    for result in results:
        _object(result, ("schema_version", "batch_id", "manifest_sha256", "worker_sha256",
                         "execution_commit", "shard_id", "status", "sources", "source_read_count",
                         "source_hash_count", "elapsed_ms"))
        _fail(type(result["schema_version"]) is int and result["schema_version"] == 1, "result schema mismatch")
        for key, expected in (("batch_id", manifest["batch_id"]),
                              ("manifest_sha256", expected_manifest_sha256),
                              ("worker_sha256", manifest["worker_sha256"]),
                              ("execution_commit", manifest["worker_commit"]),
                              ("status", "EVIDENCE_ONLY")):
            _fail(result[key] == expected, "result binding mismatch")
        shard = result["shard_id"]
        _integer(shard, 0, 9)
        _fail(shard in validated["shard_ids"] and shard not in seen_shards, "duplicate or outside shard")
        seen_shards.add(shard)
        expected_sources = [s for s in manifest["sources"] if s["shard_id"] == shard]
        _fail(isinstance(result["sources"], list) and len(result["sources"]) == len(expected_sources), "source coverage mismatch")
        _integer(result["source_read_count"], len(expected_sources), len(expected_sources))
        _integer(result["source_hash_count"], len(expected_sources), len(expected_sources))
        _integer(result["elapsed_ms"], 0, 86_400_000)
        for source, binding in zip(result["sources"], expected_sources):
            _object(source, ("pdf_sha256", "byte_length", "page_count", "pages"))
            _integer(source["byte_length"], binding["byte_length"], binding["byte_length"])
            _integer(source["page_count"], binding["page_count"], binding["page_count"])
            for key in ("pdf_sha256", "byte_length", "page_count"):
                _fail(source[key] == binding[key], "source binding mismatch")
            _fail(isinstance(source["pages"], list) and len(source["pages"]) == len(binding["pages"]), "page coverage mismatch")
            source_count += 1
            for page, page_binding in zip(source["pages"], binding["pages"]):
                _object(page, ("page_number", "visual_id", "render_sha256", "occurrences",
                               "candidate_contracts", "coordinate_frame", "source_rotation", "regions"))
                _integer(page["page_number"], page_binding["page_number"], page_binding["page_number"])
                for key in ("page_number", "visual_id", "render_sha256", "occurrences", "candidate_contracts"):
                    _fail(canonical_bytes(page[key]) == canonical_bytes(page_binding[key]), "page binding mismatch")
                _fail(page["coordinate_frame"] == page_binding.get("coordinate_frame", "UNROTATED_PAGE"),
                      "coordinate frame mismatch")
                _fail(type(page["source_rotation"]) is int and page["source_rotation"] in (0, 90, 180, 270), "invalid rotation")
                _fail(isinstance(page["regions"], list) and len(page["regions"]) == len(page_binding["regions"]), "region coverage mismatch")
                page_count += 1
                for region, region_binding in zip(page["regions"], page_binding["regions"]):
                    required = ("region_id", "rect", "method", "status", "error", "words")
                    _object(region, required, ("dpi", "psm", "crop_rotation"))
                    for key in ("region_id", "rect", "method"):
                        _fail(canonical_bytes(region[key]) == canonical_bytes(region_binding[key]), "region binding mismatch")
                    _fail(region.get("dpi") == region_binding.get("dpi"), "region dpi mismatch")
                    if region_binding["method"] == "ocr":
                        _fail(type(region.get("psm")) is int and region["psm"] == region_binding.get("psm", 3),
                              "region psm mismatch")
                    else:
                        _fail("psm" not in region, "native region has psm")
                    _fail(("crop_rotation" in region) == ("crop_rotation" in region_binding),
                          "region crop rotation mismatch")
                    if "crop_rotation" in region:
                        _fail(type(region["crop_rotation"]) is int and
                              region["crop_rotation"] == region_binding["crop_rotation"],
                              "region crop rotation mismatch")
                    _fail(region["status"] == "OK" and region["error"] is None, "region extraction incomplete")
                    _fail(isinstance(region["words"], list) and len(region["words"]) <= MAX_WORDS, "invalid words")
                    for word in region["words"]:
                        _object(word, ("text", "box"))
                        _fail(isinstance(word["text"], str) and 0 < len(word["text"]) <= 256, "invalid word")
                        box = word["box"]
                        _fail(isinstance(box, list) and len(box) == 4 and
                              all(type(x) in (int, float) and 0 <= x <= 1 for x in box) and
                              box[0] <= box[2] and box[1] <= box[3], "invalid word box")
                    region_count += 1
                    word_count += len(region["words"])
    _fail(seen_shards == set(validated["shard_ids"]), "missing shard")
    return {"schema_version": 1, "status": "EVIDENCE_ONLY", "batch_id": manifest["batch_id"],
            "manifest_sha256": expected_manifest_sha256, "worker_sha256": manifest["worker_sha256"],
            "execution_commit": manifest["worker_commit"], "shard_ids": validated["shard_ids"],
            "source_count": source_count, "page_count": page_count,
            "region_count": region_count, "word_count": word_count}


def _read_json(path, max_bytes):
    with Path(path).open("rb") as stream:
        data = stream.read(max_bytes + 1)
    _fail(len(data) <= max_bytes, "JSON input limit")
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("INPUT_ERROR") from None


def _write_json_exclusive(path, value, max_bytes=MAX_RESULT_BYTES):
    payload = canonical_bytes(value) + b"\n"
    _fail(len(payload) <= max_bytes, "JSON output limit")
    with Path(path).open("xb") as stream:
        stream.write(payload)


def _preflight_remote_manifest(manifest, manifest_digest, execution_commit):
    summary = validate_manifest(manifest)
    _sha(manifest_digest)
    _fail(summary["canonical_sha256"] == manifest_digest, "manifest hash mismatch")
    _fail(execution_commit == manifest["worker_commit"], "execution commit mismatch")
    _fail(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == manifest["worker_sha256"],
          "worker hash mismatch")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("validate-manifest")
    preflight.add_argument("--manifest", required=True)
    preflight.add_argument("--worker-sha256")
    run = commands.add_parser("run-shard")
    run.add_argument("--manifest", required=True)
    run.add_argument("--manifest-sha256", required=True)
    run.add_argument("--source-root", required=True)
    run.add_argument("--shard-id", required=True, type=int)
    run.add_argument("--execution-commit", required=True)
    run.add_argument("--source-workers", type=int, choices=range(1, 5), default=1)
    run.add_argument("--output", required=True)
    merge = commands.add_parser("validate-results")
    merge.add_argument("--manifest", required=True)
    merge.add_argument("--manifest-sha256", required=True)
    merge.add_argument("--results", nargs="+", required=True)
    merge.add_argument("--output", required=True)
    fetch = commands.add_parser("fetch-manifest")
    fetch.add_argument("--manifest-sha256", required=True)
    fetch.add_argument("--execution-commit", required=True)
    fetch.add_argument("--output", required=True)
    remote = commands.add_parser("run-remote-shard")
    remote.add_argument("--manifest", required=True)
    remote.add_argument("--manifest-sha256", required=True)
    remote.add_argument("--shard-id", required=True, type=int)
    remote.add_argument("--execution-commit", required=True)
    remote.add_argument("--source-workers", type=int, choices=range(1, 5), default=1)
    remote.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if args.command == "fetch-manifest":
        if Path(args.output).exists():
            raise FileExistsError("output exists")
        _sha(args.manifest_sha256)
        data = _fetch_source("manifest", args.manifest_sha256, MAX_MANIFEST_BYTES)
        try:
            manifest = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("INPUT_ERROR") from None
        summary = _preflight_remote_manifest(manifest, args.manifest_sha256, args.execution_commit)
        _write_json_exclusive(args.output, manifest, MAX_MANIFEST_BYTES)
        print(canonical_bytes(summary).decode("utf-8"))
        return 0
    manifest = _read_json(args.manifest, MAX_MANIFEST_BYTES)
    if args.command == "validate-manifest":
        summary = validate_manifest(manifest)
        if args.worker_sha256 is not None:
            _sha(args.worker_sha256)
            _fail(manifest["worker_sha256"] == args.worker_sha256, "worker hash mismatch")
        print(canonical_bytes(summary).decode("utf-8"))
    elif args.command == "run-shard":
        result = run_shard(manifest, args.manifest_sha256, args.shard_id, args.execution_commit,
                           source_root=args.source_root, source_workers=args.source_workers)
        _write_json_exclusive(args.output, result)
    elif args.command == "run-remote-shard":
        if Path(args.output).exists():
            raise FileExistsError("output exists")
        _preflight_remote_manifest(manifest, args.manifest_sha256, args.execution_commit)
        result = run_shard(manifest, args.manifest_sha256, args.shard_id, args.execution_commit,
                           remote_source=True, source_workers=args.source_workers)
        _write_json_exclusive(args.output, result)
        return 0 if result["status"] == "EVIDENCE_ONLY" else 1
    else:
        results = [_read_json(path, MAX_RESULT_BYTES) for path in args.results]
        summary = validate_results(manifest, args.manifest_sha256, results)
        _write_json_exclusive(args.output, summary)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        # Errors contain codes only: no source path, OCR text, or raw exception.
        message = str(exc) if isinstance(exc, ValueError) else "INPUT_ERROR"
        print(json.dumps({"status": "ERROR", "error": message}), file=sys.stderr)
        sys.exit(2)
