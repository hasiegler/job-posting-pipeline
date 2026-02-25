"""Extract remote work policy from job description text and location."""

import re
import logging
from typing import Optional

from extraction import strip_html

logger = logging.getLogger(__name__)

_NUM = r"(?:\d|one|two|three|four|five|six|seven)"

_HYBRID_EXPLICIT_RE = re.compile(
    r"\bhybrid\b"
    r"|\bpartially\s+remote\b"
    r"|\bsemi[\s-]?remote\b"
    r"|\b(?:mix|combination|blend)\s+of\s+(?:remote|in[\s-]?office|on[\s-]?site|in[\s-]?person)",
    re.IGNORECASE,
)

_HYBRID_DAYS_RE = re.compile(
    rf"(?:"
    rf"(?:at\s+least\s+|minimum\s+(?:of\s+)?)?{_NUM}\+?\s*(?:[-–]\s*{_NUM}\+?\s*)?days?\s+(?:per\s+week|a\s+week|each\s+week|weekly|in[\s-]?office|in\s+the\s+office|on[\s-]?site|in[\s-]?person)"
    rf"|"
    rf"(?:in[\s-]?office|on[\s-]?site|in[\s-]?person)\s+(?:at\s+least\s+|minimum\s+(?:of\s+)?)?{_NUM}\+?\s*(?:[-–]\s*{_NUM}\+?\s*)?days?"
    rf"|"
    rf"(?:expected|required)\s+(?:to\s+be\s+)?in[\s-]?(?:office|person)\s+{_NUM}"
    rf")",
    re.IGNORECASE,
)

_REMOTE_RE = re.compile(
    r"\bfully\s+remote\b"
    r"|\b100\s*%\s*remote\b"
    r"|\bremote[\s-]+first\b"
    r"|\bremote[\s-]+only\b"
    r"|\bwork\s+from\s+(?:home|anywhere)\b"
    r"|\bwork\s+remotely\b"
    r"|\bremote\s+(?:position|role|job|opportunity|work)\b"
    r"|\b(?:position|role|job|this)\s+is\s+(?:fully\s+)?remote\b"
    r"|\bpermanently\s+remote\b"
    r"|\blocation[\s:]+remote\b"
    r"|\btelecommut(?:e|ing)\b",
    re.IGNORECASE,
)

_ONSITE_RE = re.compile(
    r"\bon[\s-]?site\b"
    r"|\bonsite\b"
    r"|\bin[\s-]?person\s+(?:position|role|job|work)\b"
    r"|\boffice[\s-]?based\b"
    r"|\b(?:required|expected)\s+to\s+(?:work|be)\s+(?:from|at|in)\s+(?:our|the)\s+office\b"
    r"|\bwork\s+(?:from|at|in)\s+(?:our|the)\s+office\b"
    r"|\bmust\s+(?:work|be)\s+on[\s-]?site\b"
    r"|\bnot\s+(?:a\s+)?remote\b"
    r"|\bnon[\s-]?remote\b"
    r"|\bin[\s-]?office\s+(?:position|role|job|work)\b",
    re.IGNORECASE,
)

_REMOTE_WEAK_RE = re.compile(r"\bremote\b", re.IGNORECASE)

_DISCLAIMER_RE = re.compile(
    r"\bnotice\s+to\s+applicants?\b[^.]*\.?"
    r"|\bequal\s+(?:opportunity|employment)\b[^.]*\.?",
    re.IGNORECASE,
)


def _strip_disclaimers(text: str) -> str:
    """Remove legal disclaimer sentences that contain misleading keywords."""
    return _DISCLAIMER_RE.sub(" ", text)


def extract_remote_policy(description: str, location: str = None) -> Optional[str]:
    """Extract remote work policy from job description and location.

    Returns "Remote", "Hybrid", or "On-Site", or None if undetermined.

    Priority order:
        1. Location field (authoritative — companies set this deliberately)
        2. Hybrid signals in description (days-per-week, explicit keywords)
        3. Strong remote signals in description
        4. On-site signals in description
        5. Weak "remote" mention in description
    """
    text = strip_html(description) if description else ""
    loc = location or ""

    # 1. Location field is authoritative
    if _HYBRID_EXPLICIT_RE.search(loc):
        return "Hybrid"
    if _REMOTE_WEAK_RE.search(loc):
        return "Remote"

    # 2. Strip legal disclaimers before checking description
    text = _strip_disclaimers(text)

    # 3. Hybrid — explicit keywords or days-per-week patterns
    if _HYBRID_EXPLICIT_RE.search(text):
        return "Hybrid"
    if _HYBRID_DAYS_RE.search(text):
        return "Hybrid"

    # 4. On-site / negation signals (check before remote so "not remote" → On-Site)
    if _ONSITE_RE.search(text):
        return "On-Site"

    # 5. Strong remote signals in description
    if _REMOTE_RE.search(text):
        return "Remote"

    # 6. Weak remote mention in description
    if _REMOTE_WEAK_RE.search(text):
        return "Remote"

    return None


if __name__ == "__main__":
    examples = [
        # Hybrid
        ("This is a hybrid role based in San Francisco.", None, "Hybrid"),
        ("We expect employees in-office at least 3 days a week.", None, "Hybrid"),
        ("This role requires two days per week on-site.", None, "Hybrid"),
        ("A mix of remote and in-office collaboration.", None, "Hybrid"),
        ("Expected in office 4 days a week.", None, "Hybrid"),
        ("", "Hybrid - New York, NY", "Hybrid"),
        # Remote
        ("This is a fully remote position.", None, "Remote"),
        ("100% remote work from anywhere.", None, "Remote"),
        ('<p>This role is <b>remote-first</b>.</p>', None, "Remote"),
        ("You can work from home permanently.", None, "Remote"),
        ("", "Remote, US", "Remote"),
        ("", "Remote - United States", "Remote"),
        ("No policy mentioned.", "Remote", "Remote"),
        # Coinbase case: location says Remote but description has "not remote-only" and "in-person"
        (
            "While many roles at Coinbase are remote-first, we are not remote-only. "
            "In-person participation is required throughout the year.",
            "Remote - USA",
            "Remote",
        ),
        # On-Site
        ("This is an on-site role in our NYC office.", None, "On-Site"),
        ("You are required to work from our office.", None, "On-Site"),
        ("This is not a remote position.", None, "On-Site"),
        ("Office-based role in London.", None, "On-Site"),
        # DoorDash case: legal disclaimer should not trigger remote
        (
            "Notice to Applicants for Jobs Located in NYC or Remote Jobs "
            "Associated With Office in NYC Only.",
            "San Francisco, CA",
            None,
        ),
        # None
        ("We build great software.", "New York, NY", None),
        ("", "", None),
    ]

    passed = 0
    failed = 0
    for desc, loc, expected in examples:
        result = extract_remote_policy(desc, loc)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            failed += 1
        else:
            passed += 1
        preview = (desc or loc or "(empty)")[:70]
        print(f"  [{status}] {preview}")
        if result != expected:
            print(f"         expected={expected}, got={result}")

    print(f"\n{passed} passed, {failed} failed")
