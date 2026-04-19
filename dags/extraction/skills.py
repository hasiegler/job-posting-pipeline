"""Extract technical skills from job text using seeded skill aliases."""

import logging
import re
from dataclasses import dataclass

from extraction import strip_html

logger = logging.getLogger(__name__)

_GO_CONTEXT_KEYWORDS = (
    "developer|engineer|programming|coding|code|experience|languages?|"
    "backend|frontend|api|microservices?|distributed|concurrency|"
    "python|java|javascript|typescript|rust|kubernetes|docker|golang"
)


@dataclass(frozen=True)
class SkillMatcher:
    skill_name: str
    patterns: tuple[re.Pattern, ...]


def _normalize_alias(alias: str) -> str:
    return " ".join((alias or "").strip().split())


def _build_pattern(skill_name: str, alias: str) -> re.Pattern | None:
    alias = _normalize_alias(alias)
    if not alias:
        return None

    # "R" is too short/noisy for case-insensitive matching; keep strict uppercase token.
    if skill_name == "R" and alias == "r":
        return re.compile(r"(?<![A-Za-z0-9])R(?![A-Za-z0-9])")

    # Drop one-character aliases (e.g., compact alias "c" from "C++"/"C#").
    if len("".join(ch for ch in alias if ch.isalnum())) <= 1:
        return None

    # "Go" is very ambiguous as a verb; require programming context nearby.
    if skill_name == "Go" and alias == "go":
        return re.compile(
            rf"(?i)(?:\b(?:{_GO_CONTEXT_KEYWORDS})\b[^\n]{{0,30}}\bGo\b|\bGo\b[^\n]{{0,30}}\b(?:{_GO_CONTEXT_KEYWORDS})\b)"
        )

    chunks = [re.escape(part) for part in alias.split()]
    body = r"\s+".join(chunks)
    return re.compile(rf"(?<![A-Za-z0-9]){body}(?![A-Za-z0-9])", re.IGNORECASE)


def build_skill_matchers(cur) -> list[SkillMatcher]:
    """Load active skills from DB and pre-compile alias matchers."""
    cur.execute("""
        SELECT skill_name, aliases
        FROM skills
        WHERE is_active = TRUE
        ORDER BY skill_name
    """)
    rows = cur.fetchall()

    if not rows:
        logger.warning("No active skills found; skills extraction will return empty lists.")
        return []

    matchers: list[SkillMatcher] = []
    for row in rows:
        skill_name = row["skill_name"]
        aliases = row.get("aliases") or []
        candidate_aliases = [skill_name, *aliases]

        seen = set()
        patterns = []
        for alias in candidate_aliases:
            normalized = _normalize_alias(str(alias)).lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)

            pattern = _build_pattern(skill_name, normalized)
            if pattern is not None:
                patterns.append(pattern)

        if patterns:
            matchers.append(SkillMatcher(skill_name=skill_name, patterns=tuple(patterns)))

    return matchers


def extract_skills(text: str, matchers: list[SkillMatcher]) -> list[str]:
    """Return canonical skill names detected in text, each at most once."""
    if not text or not matchers:
        return []

    cleaned = strip_html(text)
    if not cleaned:
        return []

    seen: set[str] = set()
    matches: list[str] = []
    for matcher in matchers:
        if matcher.skill_name not in seen and any(
            pattern.search(cleaned) for pattern in matcher.patterns
        ):
            seen.add(matcher.skill_name)
            matches.append(matcher.skill_name)

    return matches


# ---------------------------------------------------------------------------
# Self-vendor exclusions
# ---------------------------------------------------------------------------
# Some companies in our scrape list are also entries in the skills taxonomy
# (e.g. Cloudflare, Datadog, MongoDB, GitLab).  The regex matcher fires on the
# company's own job descriptions ("Join Cloudflare to build...") and inflates
# that skill's count at that company.  We filter those matches out per-job by
# comparing the job's company canonical name against the skill name + aliases
# (case- and punctuation-insensitive).  Exclusion is per-company, not global:
# `Databricks` should still be a valid skill at SpaceX.

def _normalize_company_token(value: str) -> str:
    """Lowercase + strip everything that isn't [a-z0-9] for vendor matching."""
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def build_company_skill_exclusions(cur) -> dict[int, set[str]]:
    """Map company_id -> set of skill_name strings to drop from that company's jobs.

    A skill is excluded for a company when the company's normalized canonical
    identifier matches the normalized form of the skill_name OR any alias.
    Returns only companies that have at least one excluded skill, so callers
    can cheaply check `exclusions.get(company_id)`.
    """
    cur.execute(
        "SELECT skill_name, aliases FROM skills WHERE is_active = TRUE"
    )
    skill_tokens: list[tuple[str, set[str]]] = []
    for row in cur.fetchall():
        tokens = {_normalize_company_token(row["skill_name"])}
        for alias in (row.get("aliases") or []):
            tokens.add(_normalize_company_token(str(alias)))
        tokens.discard("")
        if tokens:
            skill_tokens.append((row["skill_name"], tokens))

    cur.execute(
        """
        SELECT company_id,
               scraper_type,
               COALESCE(canonical_name, company_name) AS canonical_name
        FROM companies
        """
    )
    exclusions: dict[int, set[str]] = {}
    for row in cur.fetchall():
        canonical = row["canonical_name"] or ""
        suffix = f"_{row['scraper_type'] or ''}"
        if len(suffix) > 1 and canonical.endswith(suffix):
            canonical = canonical[: -len(suffix)]
        company_token = _normalize_company_token(canonical)
        if not company_token:
            continue
        excluded = {
            skill_name
            for skill_name, tokens in skill_tokens
            if company_token in tokens
        }
        if excluded:
            exclusions[row["company_id"]] = excluded

    if exclusions:
        logger.info(
            "Built self-vendor skill exclusions for %d companies "
            "(e.g. %s).",
            len(exclusions),
            next(iter(exclusions.values())),
        )
    return exclusions
