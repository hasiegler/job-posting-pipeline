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
    python scripts/verify_ashby_tokens.py
    python scripts/verify_ashby_tokens.py --yaml companies.yaml
    python scripts/verify_ashby_tokens.py --out scripts/new_ashby_companies.yaml
    python scripts/verify_ashby_tokens.py --tokens ElevenLabs PostHog Vercel

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
    # AI / ML labs
    ("Perplexity AI",       "perplexity"),
    ("Harvey",              "harvey"),
    ("ElevenLabs",          "ElevenLabs"),
    ("Runway",              "Runway"),
    ("Character AI",        "character"),
    ("Weights & Biases",    "wandb"),
    ("Together AI",         "together"),
    ("Mistral AI",          "mistral"),
    ("Cohere",              "cohere"),
    ("Adept AI",            "adept"),
    ("Writer",              "writer"),
    ("Imbue",               "imbue"),
    ("Stability AI",        "StabilityAI"),
    ("Pika",                "pika"),
    ("Cursor",              "Cursor"),
    ("Anyscale",            "anyscale"),
    # Infrastructure / devtools
    ("Linear",              "linear"),
    ("Vercel",              "vercel"),
    ("Supabase",            "Supabase"),
    ("Neon",                "neon"),
    ("Modal",               "modal"),
    ("Temporal",            "temporal"),
    ("Grafana Labs",        "grafana"),
    ("Sentry",              "sentry"),
    ("PostHog",             "PostHog"),
    ("Retool",              "retool"),
    ("WorkOS",              "workos"),
    ("Stytch",              "stytch"),
    ("Clerk",               "clerk"),
    ("Tailscale",           "tailscale"),
    ("PlanetScale",         "planetscale"),
    ("CockroachLabs",       "cockroachlabs"),
    ("Fly.io",              "fly-io"),
    ("Railway",             "railway"),
    # Fintech / ops
    ("Brex",                "Brex"),
    ("Mercury",             "Mercury"),
    ("Rippling",            "Rippling"),
    ("Plaid",               "Plaid"),
    ("Lattice",             "Lattice"),
    ("Modern Treasury",     "ModernTreasury"),
    ("Vanta",               "vanta"),
    ("Drata",               "drata"),
    # Productivity / SaaS
    ("Loom",                "loom"),
    ("Notion",              "notion"),
    ("Coda",                "coda"),
    ("Airtable",            "airtable"),
    ("ClickUp",             "clickup"),
    ("Figma",               "figma"),
    ("Miro",                "miro"),
    # Scale AI / data
    ("Scale AI",            "ScaleAI"),
    ("Labelbox",            "labelbox"),
    ("Hugging Face",        "huggingface"),

    # --- Batch 2 -----------------------------------------------------------
    # AI / ML labs / inference
    ("OpenAI",              "openai"),
    ("Together AI",         "togetherai"),
    ("Replicate",           "replicate"),
    ("Fireworks AI",        "fireworksai"),
    ("Lambda Labs",         "lambdalabs"),
    ("Pinecone",            "pinecone"),
    ("Weaviate",            "weaviate"),
    ("Chroma",              "trychroma"),
    ("LangChain",           "langchain"),
    ("LlamaIndex",          "llamaindex"),
    ("Groq",                "groq"),
    ("Cerebras",            "cerebras"),
    ("SambaNova",           "sambanova"),
    ("Inflection AI",       "inflection"),
    ("xAI",                 "xai"),
    ("Magic",               "magic"),
    ("Suno",                "suno"),
    ("Udio",                "udio"),
    ("Luma AI",             "lumalabs"),
    ("Krea",                "krea"),
    ("Synthesia",           "synthesia"),
    ("HeyGen",              "heygen"),
    ("Tavus",               "tavus"),
    # AI agents / knowledge / meeting tools
    ("Sierra",              "sierra"),
    ("Decagon",             "decagon"),
    ("Glean",               "glean"),
    ("Mem",                 "mem"),
    ("Granola",             "granola"),
    ("Read AI",             "read"),
    ("Otter.ai",            "otterai"),
    # Devtools / coding
    ("Warp",                "warpdotdev"),
    ("Zed",                 "zed"),
    ("Tabby ML",            "tabbyml"),
    ("Codeium",             "codeium"),
    ("Continue",            "continuedev"),
    ("Sourcegraph",         "sourcegraph"),
    ("Replit",              "replit"),
    ("StackBlitz",          "stackblitz"),
    ("CodeSandbox",         "codesandbox"),
    ("Gitpod",              "gitpod"),
    ("Coder",               "coder"),
    # Cloud / hosting / app infra
    ("Render",              "render"),
    ("Fly.io (alt)",        "fly"),
    ("Northflank",          "northflank"),
    ("Porter",              "porter"),
    ("DigitalOcean",        "digitalocean"),
    ("Turso",               "turso"),
    ("Xata",                "xata"),
    ("Convex",              "convex"),
    ("Inngest",             "inngest"),
    ("Trigger.dev",         "triggerdev"),
    ("Resend",              "resend"),
    ("Knock",               "knock"),
    ("Cal.com",             "calcom"),
    # Data / analytics / orchestration
    ("Tinybird",            "tinybird"),
    ("Materialize",         "materializeinc"),
    ("Hex",                 "hex"),
    ("Hightouch",           "hightouch"),
    ("Census",              "census"),
    ("Prefect",             "prefect"),
    ("Dagster Labs",        "dagsterlabs"),
    ("Astronomer",          "astronomer"),
    ("dbt Labs",            "dbtlabs"),
    ("Airbyte",             "airbyte"),
    ("Mage AI",             "magedotai"),
    # Fintech / ops / banking-as-a-service
    ("Pilot",               "pilot"),
    ("Pulley",              "pulley"),
    ("Deel",                "deel"),
    ("Bench",               "bench"),
    ("Found",               "found"),
    ("MainStreet",          "mainstreet"),
    ("Sardine",             "sardine"),
    ("Alloy",               "alloy"),
    ("Unit",                "unit"),
    ("Lithic",              "lithic"),
    ("Highnote",            "highnote"),
    # Productivity / writing / presentation
    ("Craft",               "craftdocs"),
    ("Reflect",             "reflect"),
    ("Tana",                "tana"),
    ("Pitch",               "pitch"),
    ("Tome",                "tome"),
    ("Gamma",               "gamma"),
    ("Beautiful.ai",        "beautifulai"),
    ("Superhuman",          "superhuman"),
    ("Front",               "frontapp"),
    ("Missive",             "missive"),
    # Security / compliance
    ("Secureframe",         "secureframe"),
    ("Snyk",                "snyk"),
    ("Wiz",                 "wiz"),
    ("Orca Security",       "orcasecurity"),
    ("Lacework",            "lacework"),
    ("Aqua Security",       "aquasec"),
    ("Sysdig",              "sysdig"),
    # Health / wellness
    ("WHOOP",               "whoop"),
    ("Oura",                "ouraring"),
    ("Eight Sleep",         "eightsleep"),
    ("Levels",              "levelshealth"),
    ("Hims & Hers",         "hims"),
    ("Ro",                  "ro"),
    ("Maven Clinic",        "mavenclinic"),
    # Climate / sustainability
    ("Watershed",           "watershed"),
    ("Persefoni",           "persefoni"),
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
