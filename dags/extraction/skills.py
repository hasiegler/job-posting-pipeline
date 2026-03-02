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
    """Return canonical skill names detected in text."""
    if not text or not matchers:
        return []

    cleaned = strip_html(text)
    if not cleaned:
        return []

    matches = []
    for matcher in matchers:
        if any(pattern.search(cleaned) for pattern in matcher.patterns):
            matches.append(matcher.skill_name)

    return matches
