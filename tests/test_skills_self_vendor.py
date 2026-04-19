"""Self-vendor exclusion tests for the skill extractor.

The skills taxonomy includes companies we also scrape (Cloudflare, Datadog,
GitLab, MongoDB, ...).  Those skill regexes fire on the company's own job
descriptions and inflate that skill at that company.  The extractor must
filter those matches per-job, but only at the matching company — Cloudflare
is still a valid skill on a SpaceX posting.

These tests don't talk to Postgres; they fake a `RealDictCursor`-style cursor
that returns canned `skills` and `companies` rows so the unit under test stays
the actual production logic in `extraction.skills`.

Run from the repo root:

    PYTHONPATH=dags venv/bin/python -m pytest tests/test_skills_self_vendor.py -v
"""

from __future__ import annotations

import os
import sys
import unittest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DAGS_DIR = os.path.join(REPO_ROOT, "dags")
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

from extraction.skills import (  # noqa: E402  (path tweak above)
    build_company_skill_exclusions,
    build_skill_matchers,
    extract_skills,
)


class _FakeCursor:
    """Minimal stand-in for psycopg2's RealDictCursor.

    `execute()` inspects the SQL string just enough to decide which canned
    payload to hand back from `fetchall()`.  Rows are dicts whose `.get()`
    semantics match what the production code relies on.
    """

    def __init__(self, *, skills, companies):
        self._skills = skills
        self._companies = companies
        self._next: list[dict] = []

    def execute(self, sql, *_args, **_kwargs):
        normalized = " ".join(sql.lower().split())
        if "from skills" in normalized:
            self._next = list(self._skills)
        elif "from companies" in normalized:
            self._next = list(self._companies)
        else:
            raise AssertionError(f"Unexpected SQL in test: {sql!r}")

    def fetchall(self):
        return list(self._next)


def _make_cursor():
    return _FakeCursor(
        skills=[
            {"skill_name": "Cloudflare", "aliases": ["cloudflare"]},
            {"skill_name": "Datadog", "aliases": ["datadog"]},
            {"skill_name": "New Relic", "aliases": ["new relic", "newrelic"]},
            {"skill_name": "Python", "aliases": ["python", "py"]},
            {
                "skill_name": "Kubernetes",
                "aliases": ["kubernetes", "k8s", "kube"],
            },
        ],
        companies=[
            {
                "company_id": 1,
                "scraper_type": "greenhouse",
                "canonical_name": "cloudflare",
            },
            {
                "company_id": 2,
                "scraper_type": "greenhouse",
                "canonical_name": "spacex",
            },
            {
                "company_id": 3,
                "scraper_type": "greenhouse",
                "canonical_name": "newrelic",
            },
        ],
    )


class SelfVendorExclusionTests(unittest.TestCase):
    def setUp(self):
        cur = _make_cursor()
        self.matchers = build_skill_matchers(cur)
        self.exclusions = build_company_skill_exclusions(cur)

    def _extract(self, text: str, company_id: int) -> list[str]:
        skills = extract_skills(text, self.matchers)
        excluded = self.exclusions.get(company_id)
        if excluded:
            skills = [s for s in skills if s not in excluded]
        return skills

    # --- The headline cases the ticket called out --------------------------

    def test_cloudflare_skill_dropped_on_cloudflare_job(self):
        text = "Join Cloudflare to build the next generation of edge networks."
        self.assertNotIn("Cloudflare", self._extract(text, company_id=1))

    def test_cloudflare_skill_kept_on_other_companys_job(self):
        text = "We use Cloudflare for our CDN."
        self.assertIn("Cloudflare", self._extract(text, company_id=2))

    # --- Sanity: real skills mentioned alongside the vendor still survive --

    def test_real_skills_survive_self_vendor_filter(self):
        text = (
            "Join Cloudflare and help us scale Python services on Kubernetes."
        )
        skills = self._extract(text, company_id=1)
        self.assertNotIn("Cloudflare", skills)
        self.assertIn("Python", skills)
        self.assertIn("Kubernetes", skills)

    # --- Two-word vendor, alias normalization (New Relic <-> newrelic) -----

    def test_new_relic_dropped_on_newrelic_company(self):
        text = "New Relic is hiring SREs to monitor New Relic."
        self.assertNotIn("New Relic", self._extract(text, company_id=3))

    def test_new_relic_kept_on_unrelated_company(self):
        text = "Experience with New Relic for observability is a plus."
        self.assertIn("New Relic", self._extract(text, company_id=2))

    # --- The exclusion map itself looks right ------------------------------

    def test_exclusion_map_contents(self):
        self.assertEqual(self.exclusions.get(1), {"Cloudflare"})
        self.assertEqual(self.exclusions.get(3), {"New Relic"})
        self.assertNotIn(2, self.exclusions)


if __name__ == "__main__":
    unittest.main()
