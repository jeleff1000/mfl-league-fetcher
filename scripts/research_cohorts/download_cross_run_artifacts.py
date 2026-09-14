"""Download all matching artifacts from another GitHub Actions run.

GitHub's download-artifact action is bound to the current run in some cross-run
cases and also caps matching artifacts at 200.  This uses the Actions API directly,
then removes the API bearer header when GitHub redirects to blob storage.
"""
from __future__ import annotations

import argparse
import io
import os
import time
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


class _RedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        next_headers = dict(request.headers)
        if urllib.parse.urlparse(newurl).netloc != urllib.parse.urlparse(request.full_url).netloc:
            next_headers.pop("Authorization", None)
        return urllib.request.Request(newurl, headers=next_headers, method=request.get_method())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--batches", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--repo", default="jeleff1000/league-history-workers")
    args = ap.parse_args()
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GH_TOKEN or GITHUB_TOKEN is required")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "league-history-cross-run-download",
    }
    opener = urllib.request.build_opener(_RedirectHandler)
    api = f"https://api.github.com/repos/{args.repo}/actions/runs/{args.run}/artifacts"

    def get_json(url: str):
        with opener.open(urllib.request.Request(url, headers=headers), timeout=120) as response:
            import json
            return json.load(response)

    prefix = f"research-matchup-{args.year}-batch"
    artifacts = []
    page = 1
    while True:
        payload = get_json(f"{api}?per_page=100&page={page}")
        items = payload.get("artifacts", [])
        artifacts.extend(item for item in items if item["name"].startswith(prefix))
        if len(items) < 100:
            break
        page += 1
    if len(artifacts) != args.batches:
        raise SystemExit(f"expected {args.batches} {prefix} artifacts, found {len(artifacts)}")
    args.out.mkdir(parents=True, exist_ok=True)
    print(f"found {len(artifacts)} source artifacts", flush=True)

    def download(item):
        target = args.out / item["name"]
        target.mkdir(parents=True, exist_ok=True)
        last = None
        for attempt in range(4):
            try:
                request = urllib.request.Request(item["archive_download_url"], headers=headers)
                with opener.open(request, timeout=300) as response:
                    data = response.read()
                with zipfile.ZipFile(io.BytesIO(data)) as archive:
                    for member in archive.infolist():
                        member_path = Path(member.filename)
                        if member_path.is_absolute() or ".." in member_path.parts:
                            raise RuntimeError(f"unsafe artifact member: {member.filename}")
                    archive.extractall(target)
                return item["name"]
            except Exception as exc:
                last = exc
                if attempt < 3:
                    time.sleep(2**attempt)
        raise RuntimeError(f"{item['name']}: {last}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(download, item) for item in artifacts]
        for index, future in enumerate(as_completed(futures), 1):
            print(f"downloaded {index}/{len(futures)}: {future.result()}", flush=True)


if __name__ == "__main__":
    main()
