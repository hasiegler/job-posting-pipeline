"""Extract salary information from job description text."""

import re
import logging
from dataclasses import dataclass, asdict
from typing import Optional

from extraction import strip_html

logger = logging.getLogger(__name__)

CURRENCY_SYMBOLS = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"}

CURRENCY_CODES = frozenset({
    "USD", "EUR", "GBP", "CAD", "AUD", "CHF", "NZD", "SGD",
    "HKD", "SEK", "NOK", "DKK", "INR", "BRL", "MXN",
})

HOURLY_THRESHOLD = 500


@dataclass
class SalaryResult:
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    salary_period: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


_CODE_ALT = "|".join(CURRENCY_CODES)
_SYM_CLASS = "[$€£¥]"

_AMOUNT = r"\d{1,3}(?:[.,]\d{3})*(?:\.\d{1,2})?"

_RANGE_RE = re.compile(
    rf"(?:(?P<code_before>{_CODE_ALT})\s*)?"
    rf"(?P<sym>{_SYM_CLASS})\s*"
    rf"(?P<min>{_AMOUNT})"
    rf"\s*(?P<min_k>[Kk])?"
    rf"(?:\s*[-–—]+\s*|\s+to\s+)"
    rf"(?:{_SYM_CLASS}\s*)?"
    rf"(?P<max>{_AMOUNT})"
    rf"\s*(?P<max_k>[Kk])?"
    rf"(?:\s+(?P<code_after>{_CODE_ALT}))?"
)

_HOURLY_RE = re.compile(
    r"\b(?:per\s+hour|hourly|/\s*h(?:ou)?r|an\s+hour)\b", re.IGNORECASE
)
_YEARLY_RE = re.compile(
    r"\b(?:per\s+(?:year|annum)|annual(?:ly)?|/\s*y(?:ea)?r|yearly|salary)\b",
    re.IGNORECASE,
)


def _parse_amount(raw: str, has_k: bool) -> float:
    """Parse a salary amount, handling both US (1,000) and EU (1.000) formats."""
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", raw):
        value = float(raw.replace(".", ""))
    else:
        value = float(raw.replace(",", ""))
    if has_k:
        value *= 1000
    return value


def _resolve_currency(sym: str, code_before: str | None, code_after: str | None) -> str:
    if code_after:
        return code_after.upper()
    if code_before:
        return code_before.upper()
    return CURRENCY_SYMBOLS.get(sym, "USD")


def _detect_period(context: str, amount: float) -> str:
    if _HOURLY_RE.search(context):
        return "hourly"
    if _YEARLY_RE.search(context):
        return "yearly"
    return "hourly" if amount < HOURLY_THRESHOLD else "yearly"


def extract_salary(description: str) -> Optional[SalaryResult]:
    """Extract salary range from job description text.

    Handles patterns like:
        $93,200 - $137,000 USD
        $17.85 - $17.85 USD
        €60,000 - €80,000
        $100K - $150K
        CAD $80,000 - $100,000
        $90,000 to $120,000

    When multiple ranges appear, uses the first match (typically the
    headline/base pay range).

    Returns None if no salary information is found.
    """
    if not description:
        return None

    cleaned = strip_html(description)
    m = _RANGE_RE.search(cleaned)
    if not m:
        return None

    min_val = _parse_amount(m.group("min"), bool(m.group("min_k")))
    max_val = _parse_amount(m.group("max"), bool(m.group("max_k")))

    if min_val > max_val:
        min_val, max_val = max_val, min_val

    currency = _resolve_currency(
        m.group("sym"), m.group("code_before"), m.group("code_after")
    )

    ctx_start = max(0, m.start() - 200)
    ctx_end = min(len(cleaned), m.end() + 200)
    context = cleaned[ctx_start:ctx_end]
    period = _detect_period(context, min_val)

    return SalaryResult(
        salary_min=min_val,
        salary_max=max_val,
        salary_currency=currency,
        salary_period=period,
    )


if __name__ == "__main__":
    examples = [
        # Plain text
        "$93,200 - $137,000 USD",
        "Base Pay:\n$17.85 - $17.85 USD",
        "Compensation: $100K - $150K per year",
        "CAD $80,000 - $100,000",
        "Hourly rate: $25.00 - $35.00 per hour",
        "No salary info here.",
        # HTML with &mdash; divider spans
        (
            '<div class="title">Annual Salary:</div>'
            '<div class="pay-range"><span>$265,000</span>'
            '<span class="divider">&mdash;</span>'
            '<span>$315,000 USD</span></div>'
        ),
        # European format with dots
        (
            '<div class="title">Annual Salary:</div>'
            '<div class="pay-range"><span>€205.000</span>'
            '<span class="divider">&mdash;</span>'
            '<span>€255.000 EUR</span></div>'
        ),
        # HTML with surrounding context
        (
            '<p class="p1"><span style="font-size: 16px;">The base pay range '
            'is subject to change.</span></p>'
            '<div class="title">Pay Range</div>'
            '<div class="pay-range"><span>$204,000</span>'
            '<span class="divider">&mdash;</span>'
            '<span>$255,000 USD</span></div>'
        ),
    ]

    for text in examples:
        result = extract_salary(text)
        preview = text.replace("\n", "\\n")[:80]
        print(f"Input:  {preview}")
        print(f"Result: {result}")
        print()
