#!/usr/bin/env python3
"""
Verify candidate Ashby board tokens against the public posting API,
and skip any token already present in companies.yaml.

Ashby exposes a public posting API at:
    https://api.ashbyhq.com/posting-api/job-board/{token}

The board token is the path segment from the public job-board URL:
    https://jobs.ashbyhq.com/{token}

A valid, public board returns 200 with a JSON body containing a `jobs` array.
Anything else (404, non-JSON, missing `jobs` key) means the token is unusable
and should not be added to companies.yaml.

Board tokens are often mixed-case (e.g. "ElevenLabs", "PostHog").  The script
preserves the original casing in the URL and generates a lowercase name key for
companies.yaml (e.g. elevenlabs_ashby) to match the existing convention.

Usage (run from the project root):
    python tools/validators/verify_ashby_tokens.py
    python tools/validators/verify_ashby_tokens.py --yaml companies.yaml
    python tools/validators/verify_ashby_tokens.py --out tools/validators/new_ashby_companies.yaml
    python tools/validators/verify_ashby_tokens.py --tokens ElevenLabs PostHog Vercel

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

API_TEMPLATE = "https://api.ashbyhq.com/posting-api/job-board/{token}"
USER_AGENT   = "TryJobPulse-TokenVerifier/1.0"
REQUEST_TIMEOUT  = 10   # seconds
SLEEP_BETWEEN    = 0.4  # be polite to the API

# (display_name, board_token)
# Tokens are case-sensitive and come from the jobs.ashbyhq.com URL path.
# The list is intentionally wide; the script will tell you which ones are live.
CANDIDATES: list[tuple[str, str]] = [
    # AI apps / vertical AI
    ("Abridge",             "abridge"),
    ("Ambience Healthcare", "ambiencehealthcare"),
    ("Nabla",               "nabla"),
    ("OpenEvidence",        "openevidence"),
    ("Hippocratic AI",      "hippocraticai"),
    ("EvenUp",              "evenup"),
    ("Eve",                 "eve"),
    ("Luminance",           "luminance"),
    ("Clio",                "clio"),
    ("Spellbook",           "spellbook"),
    ("Norm AI",             "normai"),
    ("Greenlite",           "greenlite"),
    ("Basis",               "basis"),
    ("Numeric",             "numeric"),
    ("Rogo",                "rogo"),
    ("Hebbia",              "hebbia"),
    ("Rilla",               "rilla"),
    ("Attention",           "attention"),
    ("Clay",                "clay"),
    ("Unify",               "unifygtm"),
    # AI infra / eval / observability
    ("LangSmith",           "langsmith"),
    ("Braintrust",          "braintrustdata"),
    ("Baseten",             "baseten"),
    ("Modal Labs",          "modallabs"),
    ("Anyscale",            "anyscaleinc"),
    ("Outerbounds",         "outerbounds"),
    ("Union AI",            "unionai"),
    ("Zilliz",              "zilliz"),
    ("Qdrant",              "qdrant"),
    ("LanceDB",             "lancedb"),
    ("Marqo",               "marqo"),
    ("Nomic",               "nomic"),
    ("Contextual AI",       "contextualai"),
    ("Unstructured",        "unstructured"),
    ("Reducto",             "reducto"),
    # Robotics / hardware / defense
    ("Figure AI",           "figure"),
    ("Skild AI",            "skildai"),
    ("Physical Intelligence","physicalintelligence"),
    ("Dexterity",           "dexterity"),
    ("Path Robotics",       "pathrobotics"),
    ("Chef Robotics",       "chefrobotics"),
    ("Applied Intuition",   "appliedintuition"),
    ("Saronic",             "saronic"),
    ("Castelion",           "castelion"),
    ("Hadrian",             "hadrian"),
    ("Base Power",          "basepowercompany"),
    ("Radiant",             "radiantnuclear"),
    ("Terra Power",         "terrapower"),
    ("Zap Energy",          "zapenergy"),
    ("Commonwealth Fusion", "cfs"),
    # Climate / energy
    ("Arcadia",             "arcadia"),
    ("Crusoe",              "crusoeenergy"),
    ("Electric Hydrogen",   "electrichydrogen"),
    ("Charm Industrial",    "charmindustrial"),
    ("Heirloom",            "heirloomcarbon"),
    ("Twelve",              "twelve"),
    ("Sila",                "silananotechnologies"),
    ("Redwood Materials",   "redwoodmaterials"),
    ("Palmetto",            "palmetto"),
    ("Aurora Solar",        "aurorasolar"),
    # Fintech (new)
    ("Column",              "column"),
    ("Increase",            "increase"),
    ("Meow",                "meow"),
    ("Arc",                 "arc"),
    ("Slope",               "slope"),
    ("Settle",              "settle"),
    ("Tremendous",          "tremendous"),
    ("Astra",               "astra"),
    ("Method Financial",    "methodfi"),
    ("Stripe alternatives — Paddle", "paddle"),
    # Devtools / infra (new)
    ("Depot",               "depot"),
    ("Namespace",           "namespacelabs"),
    ("Buildkite",           "buildkite"),
    ("Earthly",             "earthly"),
    ("Chainguard Labs",     "chainguard"),
    ("Doppler",             "doppler"),
    ("Infisical",           "infisical"),
    ("Val Town",            "valtown"),
    ("Deno",                "deno"),
    ("Bun / Oven",          "oven"),
    ("Astro",               "astro"),
    ("Sanity",              "sanity"),
    ("Payload",             "payloadcms"),
    ("Storyblok",           "storyblok"),
    ("Contentful",          "contentful"),
    # Ops / vertical SaaS
    ("ServiceTitan",        "servicetitan"),
    ("Jobber",              "jobber"),
    ("Procore",             "procore"),
    ("BuildOps",            "buildops"),
    ("Shipbob",             "shipbob"),
    ("Flexport",            "flexport"),
    ("Stord",               "stord"),
    ("Nuvocargo",           "nuvocargo"),
    ("Vic.ai",              "vicai"),
    ("Ottimate",            "ottimate"),
    # Consumer / other
    ("Cash App alt — Copper","getcopper"),
    ("Whatnot",             "whatnot"),
    ("Fanatics",            "fanatics"),
    ("Curated",             "curated"),
    ("Kojo",                "kojo"),
    ("Wonderschool",        "wonderschool"),
    ("Outschool",           "outschool"),
    ("Guild",               "guildeducation"),
    ("Handshake",           "joinhandshake"),
    ("Multiverse",          "multiverse"),
]


@dataclass
class Result:
    name:      str
    token:     str
    ok:        bool
    job_count: int | None
    status:    str


def load_existing_tokens(yaml_path: Path) -> set[str]:
    """Regex-parse companies.yaml to pull out Ashby tokens already in use.

    Uses regex instead of a YAML library so the script stays dependency-free.
    Matches the token segment of https://jobs.ashbyhq.com/<token>.
    Case-insensitive comparison is used for de-dup since tokens can vary in case.
    """
    if not yaml_path.exists():
        print(f"  (no {yaml_path} found — skipping de-dup check)\n")
        return set()
    text = yaml_path.read_text(encoding="utf-8")
    tokens = set(re.findall(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)", text))
    lower_tokens = {t.lower() for t in tokens}
    print(f"  Loaded {len(lower_tokens)} existing Ashby token(s) from {yaml_path}\n")
    return lower_tokens


def check_token(token: str) -> tuple[bool, int | None, str]:
    """Return (ok, job_count, status_message) for a single board token."""
    url = API_TEMPLATE.format(token=token)
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
    except Exception as e:  # noqa: BLE001
        return False, None, f"error: {e.__class__.__name__}: {e}"


def verify(candidates: Iterable[tuple[str, str]]) -> list[Result]:
    results: list[Result] = []
    for name, token in candidates:
        ok, count, status = check_token(token)
        results.append(Result(name=name, token=token, ok=ok, job_count=count, status=status))
        icon = "✓" if ok else "✗"
        count_str = f"{count:>4} jobs" if count is not None else "          "
        print(f"  {icon}  {token:<24} {count_str}   {status}   ({name})")
        time.sleep(SLEEP_BETWEEN)
    return results


def write_yaml(results: list[Result], path: str) -> None:
    """Write live tokens as a YAML snippet matching the companies.yaml schema.

    Only boards with > 0 listed jobs are written. A board that returns a valid
    `jobs: []` is technically reachable but uninteresting for the pipeline
    (closed/draft boards, sandboxes, etc.) and would just be noise in the
    companies table.
    """
    live = [r for r in results if r.ok and (r.job_count or 0) > 0]
    skipped_empty = [r for r in results if r.ok and (r.job_count or 0) == 0]

    lines = ["# Verified Ashby tokens — generated by verify_ashby_tokens.py"]
    for r in live:
        name_key = f"{r.token.lower()}_ashby"
        lines.append("")
        lines.append(f"  - name: {name_key}")
        lines.append(f"    scraper_type: ashby")
        lines.append(f"    url: https://jobs.ashbyhq.com/{r.token}")
        lines.append(f"    enabled: true")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\nWrote {len(live)} live entries to {path}")
    if skipped_empty:
        print(f"Skipped {len(skipped_empty)} reachable board(s) with 0 jobs:")
        for r in skipped_empty:
            print(f"  - {r.token:<24} ({r.name})")


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
        help="Optional path to write a YAML snippet of verified tokens.",
    )
    parser.add_argument(
        "--tokens",
        nargs="+",
        metavar="TOKEN",
        help=(
            "Override the built-in candidate list with specific board tokens. "
            "Pass the token exactly as it appears in the jobs.ashbyhq.com URL "
            "(case-sensitive). Names default to the token value."
        ),
    )
    args = parser.parse_args()

    candidates: list[tuple[str, str]] = (
        [(t, t) for t in args.tokens] if args.tokens else CANDIDATES
    )

    if not args.no_diff:
        existing_lower = load_existing_tokens(Path(args.yaml))
        dropped   = [(n, t) for n, t in candidates if t.lower() in existing_lower]
        candidates = [(n, t) for n, t in candidates if t.lower() not in existing_lower]
        if dropped:
            print(f"Skipping {len(dropped)} token(s) already in {args.yaml}:")
            for n, t in dropped:
                print(f"  - {t}  ({n})")
            print()
        if not candidates:
            print("Nothing new to verify. Exiting.")
            return 0

    print(f"Verifying {len(candidates)} candidate token(s) against Ashby…\n")
    results = verify(candidates)

    live    = [r for r in results if r.ok and (r.job_count or 0) > 0]
    empty   = [r for r in results if r.ok and (r.job_count or 0) == 0]
    invalid = [r for r in results if not r.ok]

    print(f"\nSummary: {len(live)} live, {len(empty)} empty, {len(invalid)} invalid")
    if invalid:
        print("Invalid tokens (do NOT add to companies.yaml):")
        for r in invalid:
            print(f"  - {r.token:<24} {r.status}   ({r.name})")

    if args.out:
        write_yaml(results, args.out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
