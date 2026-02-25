import html
import re

EXTRACTION_VERSION = "v1.0"


def strip_html(text: str) -> str:
    """Decode HTML entities and strip tags, collapsing whitespace."""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text)
