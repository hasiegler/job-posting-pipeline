#!/usr/bin/env python3
"""
Verify candidate Greenhouse company slugs against the public job-board API,
and skip any slug already present in companies.yaml.

Greenhouse exposes a public API at:
    https://boards-api.greenhouse.io/v1/boards/{slug}/jobs

A valid, public board returns 200 with a JSON body containing a `jobs` array.
Anything else (404, non-JSON, missing `jobs` key) means the slug is unusable
for the pipeline and should not be added to companies.yaml.

Usage (run from the project root):
    python scripts/verify_greenhouse_slugs.py
    python scripts/verify_greenhouse_slugs.py --yaml companies.yaml
    python scripts/verify_greenhouse_slugs.py --out scripts/new_companies.yaml
    python scripts/verify_greenhouse_slugs.py --slugs notion canva snowflake

The script looks for companies.yaml in the current working directory by
default — invoke it from the project root so that relative path resolves.
Use --yaml to point at a different path, or --no-diff to disable the check.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_TEMPLATE = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
USER_AGENT = "TryJobPulse-SlugVerifier/1.0"
REQUEST_TIMEOUT = 10  # seconds
SLEEP_BETWEEN = 0.3   # be polite to the API

# (display_name, slug) — edit freely.
# Batch 4: 20 candidates broadened beyond pure tech — media, retail/CPG,
# finance, travel, and established industrial/enterprise brands. Targeting
# companies that have been on Greenhouse for 5+ years.
CANDIDATES: list[tuple[str, str]] = [
    # Media / entertainment
    ("The New York Times",  "nytimes"),
    ("Warner Bros Discovery","warnerbrosdiscovery"),
    ("NBCUniversal",        "nbcuniversal"),
    ("Conde Nast",          "condenast"),
    ("Spotify",             "spotifyjobs"),
    # Retail / CPG (established brands)
    ("Wayfair",             "wayfairinc"),
    ("Sweetgreen",          "sweetgreen"),
    ("Allbirds",            "allbirds"),
    ("Away",                "away"),
    ("Casper",              "casper"),
    # Finance / banking / insurance
    ("Capital One",         "capitalone"),
    ("American Express",    "americanexpress"),
    ("Goldman Sachs",       "goldmansachs"),
    ("Lemonade",            "lemonade"),
    ("Hippo Insurance",     "hippoinsurance"),
    # Travel / hospitality
    ("Hopper",              "hopper"),
    ("GetYourGuide",        "getyourguide"),
    # Enterprise / industrial
    ("Intercom",            "intercom"),
    ("Zapier",              "zapier"),
    ("Bloomberg",           "bloomberg"),
]


@dataclass
class Result:
    name: str
    slug: str
    ok: bool
    job_count: int | None
    status: str  # short human-readable reason


def load_existing_slugs(yaml_path: Path) -> set[str]:
    """Regex-parse companies.yaml to pull out Greenhouse slugs already in use.

    Uses regex instead of a YAML library so the script stays dependency-free.
    Matches the slug segment of https://boards.greenhouse.io/<slug>.
    """
    if not yaml_path.exists():
        print(f"  (no {yaml_path} found — skipping de-dup check)\n")
        return set()
    text = yaml_path.read_text(encoding="utf-8")
    slugs = set(re.findall(r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)", text))
    print(f"  Loaded {len(slugs)} existing slugs from {yaml_path}\n")
    return slugs


def check_slug(slug: str) -> tuple[bool, int | None, str]:
    """Return (ok, job_count, status_message) for a single slug."""
    url = API_TEMPLATE.format(slug=slug)
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            body = resp.read()
            if resp.status != 200:
                return False, None, f"HTTP {resp.status}"
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return False, None, "non-JSON body"
            jobs = data.get("jobs")
            if not isinstance(jobs, list):
                return False, None, "no jobs[] key"
            return True, len(jobs), "ok"
    except HTTPError as e:
        return False, None, f"HTTP {e.code}"
    except URLError as e:
        return False, None, f"network error: {e.reason}"
    except Exception as e:  # noqa: BLE001 — surface anything unexpected
        return False, None, f"error: {e.__class__.__name__}: {e}"


def verify(candidates: Iterable[tuple[str, str]]) -> list[Result]:
    results: list[Result] = []
    for name, slug in candidates:
        ok, count, status = check_slug(slug)
        results.append(Result(name=name, slug=slug, ok=ok, job_count=count, status=status))
        icon = "✓" if ok else "✗"
        count_str = f"{count:>4} jobs" if count is not None else "         "
        print(f"  {icon}  {slug:<20} {count_str}   {status}   ({name})")
        time.sleep(SLEEP_BETWEEN)
    return results


def write_yaml(results: list[Result], path: str) -> None:
    """Write valid slugs as a YAML snippet matching the companies.yaml schema."""
    valid = [r for r in results if r.ok]
    lines = ["# Verified Greenhouse slugs — generated by verify_greenhouse_slugs.py"]
    for r in valid:
        # Name key uses <slug>_greenhouse to match the existing convention.
        lines.append("")
        lines.append(f"  - name: {r.slug}_greenhouse")
        lines.append(f"    scraper_type: greenhouse")
        lines.append(f"    url: https://boards.greenhouse.io/{r.slug}")
        lines.append(f"    enabled: true")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote {len(valid)} verified entries to {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yaml",
        default="companies.yaml",
        help="Path to existing companies.yaml for de-dup. Default: companies.yaml",
    )
    parser.add_argument(
        "--no-diff",
        action="store_true",
        help="Skip the de-dup check against companies.yaml.",
    )
    parser.add_argument(
        "--out",
        help="Optional path to write a YAML snippet of verified slugs.",
    )
    parser.add_argument(
        "--slugs",
        nargs="+",
        help="Override the built-in candidate list. Pass bare slugs; names default to the slug.",
    )
    args = parser.parse_args()

    candidates = (
        [(s, s) for s in args.slugs] if args.slugs else CANDIDATES
    )

    # De-dup against existing yaml
    if not args.no_diff:
        existing = load_existing_slugs(Path(args.yaml))
        dropped = [(n, s) for n, s in candidates if s in existing]
        candidates = [(n, s) for n, s in candidates if s not in existing]
        if dropped:
            print(f"Skipping {len(dropped)} slug(s) already in {args.yaml}:")
            for n, s in dropped:
                print(f"  - {s}  ({n})")
            print()
        if not candidates:
            print("Nothing new to verify. Exiting.")
            return 0

    print(f"Verifying {len(candidates)} candidate slug(s) against Greenhouse…\n")
    results = verify(candidates)

    valid   = [r for r in results if r.ok]
    invalid = [r for r in results if not r.ok]

    print(f"\nSummary: {len(valid)} valid, {len(invalid)} invalid")
    if invalid:
        print("Invalid slugs (do NOT add to companies.yaml):")
        for r in invalid:
            print(f"  - {r.slug:<20} {r.status}   ({r.name})")

    if args.out:
        write_yaml(results, args.out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
