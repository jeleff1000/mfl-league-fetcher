"""
sota_recon/pfa_metadata_fetch.py -- PHASE 1: five fetches, and only five.

These are the pages where PFA DECLARES things about itself, and each answers a question
the program currently has open:

  nflgamelogcoverage.html  the site's own PER-STATISTIC coverage declaration. It SCOPES
                           every claim we make about PFA's value -- it has already
                           corrected one (finding 10). Fetched first for that reason.
  statkey.html             stat DEFINITIONS. Bears directly on the open
                           `def_air_yards_allowed` definition-version question and the
                           `fum_rec` burn-down class, both on Joe's queue.
  nflrosterlimits.html     roster limits by era -- the denominator behind "was this
                           player active", which the presence universe needs.
  super-bowl.html          footer-only section, missed by a nav-only probe.
  trainingcamps.html       footer-only section, same.

POLITENESS. One process, one host, the catalog's own 1.0s delay, five requests. The bytes
are RETAINED so every later reading is a re-parse -- and, since two PFA shards turned out
to hold bot-challenge interstitials served as HTTP 200, every response is checked for that
signature before being called a capture.

Run:  python -m scripts.sota_recon.pfa_metadata_fetch [--apply]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

OUT_DIR = Path("D:/league-history-data/nfl/ff_assets/profootballarchives/site_metadata")
SUMMARY_PATH = Path(__file__).resolve().parents[2] / "docs" / "pfa-site-metadata-capture.json"

USER_AGENT = "ff-assets-historical-witness/1.0 (+https://github.com/jeleff1000/ff-assets)"
DELAY_SECONDS = 1.0

# Ordered deliberately: the coverage declaration scopes every later claim.
PAGES = [
    ("nflgamelogcoverage", "https://www.profootballarchives.com/nflgamelogcoverage.html",
     "the site's own per-statistic coverage declaration; scopes every claim about PFA"),
    ("statkey", "https://www.profootballarchives.com/statkey.html",
     "stat definitions; bears on def_air_yards_allowed and fum_rec"),
    ("nflrosterlimits", "https://www.profootballarchives.com/nflrosterlimits.html",
     "roster limits by era; denominator for the presence universe"),
    ("super-bowl", "https://www.profootballarchives.com/super-bowl.html",
     "footer-only section missed by a nav-only probe"),
    ("trainingcamps", "https://www.profootballarchives.com/trainingcamps.html",
     "footer-only section missed by a nav-only probe"),
]

_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
_TABLE = re.compile(r"<table.*?</table>", re.S | re.I)


def looks_like_challenge(html: str) -> bool:
    """Two of twenty PFA capture shards hold ONLY this: a 200 response carrying a
    five-second JS reload spinner instead of content. It must never be recorded as a
    capture again."""
    return "One moment, please" in html and not _TABLE.search(_SCRIPT.sub("", html))


def fetch(url: str) -> tuple[int, bytes, str | None]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=45) as reply:
            return int(reply.status), reply.read(), None
    except urllib.error.HTTPError as exc:
        return int(exc.code), b"", str(exc)
    except (urllib.error.URLError, TimeoutError) as exc:
        return 0, b"", str(exc)


def run(apply: bool) -> dict:
    if apply:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
    records = []
    for index, (name, url, why) in enumerate(PAGES):
        if index:
            time.sleep(DELAY_SECONDS)
        status, body, error = fetch(url)
        html = body.decode("utf-8", errors="replace") if body else ""
        challenge = bool(html) and looks_like_challenge(html)
        digest = hashlib.sha256(body).hexdigest() if body else None
        entry = {
            "name": name,
            "url": url,
            "why": why,
            "status_code": status,
            "error": error,
            "bytes": len(body),
            "content_sha256": digest,
            "challenge_page": challenge,
            "tables": len(_TABLE.findall(_SCRIPT.sub("", html))) if html else 0,
            "retrieved_at_utc": datetime.now(UTC).isoformat(),
        }
        if apply and body and status == 200 and not challenge:
            destination = OUT_DIR / f"{name}.html.gz"
            destination.write_bytes(gzip.compress(body))
            entry["retained_path"] = destination.as_posix()
        records.append(entry)
        print(f"  {status} {name:22s} {len(body):>8,} bytes  tables={entry['tables']}"
              f"{'  CHALLENGE PAGE' if challenge else ''}")
    captured = [r for r in records if r.get("retained_path")]
    return {
        "law": "five fetches, one host, one process, the catalog's own delay; bytes "
               "retained so every later reading is a re-parse",
        "counters": {
            "requested": len(records),
            "retained": len(captured),
            "challenge_pages": sum(1 for r in records if r["challenge_page"]),
            "failed": sum(1 for r in records if r["status_code"] != 200),
        },
        "pages": records,
        "output_dir": OUT_DIR.as_posix(),
        "applied": apply,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="retain the bytes on disk")
    args = parser.parse_args()
    doc = run(args.apply)
    from .recon_common import utc_stamp

    doc["generated_utc"] = utc_stamp()
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    print(f"\n{doc['counters']}")
    print(f"receipt -> {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
