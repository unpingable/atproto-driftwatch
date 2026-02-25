import re
from typing import List
from .models import ClaimSignal

DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
QUANTITY_RE = re.compile(r"\b\d[\d\.,]*k?\b", re.I)
_YEAR_RE = re.compile(r"^(19|20)\d{2}$")
ENTITY_RE = re.compile(r"\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)\b")
MODAL_RE = re.compile(r"\b(definitely|confirmed|proved|certainly|sure|guaranteed|reported|reportedly|according to)\b", re.I)

# Sentence-initial function words that poison single-word entity extraction.
# Only applied to single-word matches — multi-word entities pass through.
_ENTITY_STOPWORDS = frozenset({
    "That", "This", "They", "The", "And", "But", "What", "When",
    "Where", "Why", "How", "Today", "Yesterday", "Tomorrow", "Here",
    "There", "These", "Those", "Just", "Also", "Still", "Even",
    "Very", "Really", "Only", "Some", "Many", "Most", "Every",
    "All", "Any", "Each", "Both", "Such", "Other", "Another",
    "Not", "Now", "Then", "Well", "Like", "Been", "Being",
    "Have", "Has", "Had", "Was", "Were", "Are", "Will",
    "Would", "Could", "Should", "May", "Might", "Must",
    "Can", "Did", "Does", "Got", "Get", "Let", "Yes", "Our",
    "His", "Her", "Its", "Your", "Their", "Who", "Which",
})


def _strip_url_numbers(text: str, quantities: list) -> list:
    # Remove numeric tokens that appear only as parts of URLs (query params, path IDs)
    urls = re.findall(r"https?://\S+", text)
    if not urls:
        return quantities
    url_nums = set()
    for u in urls:
        url_nums.update(re.findall(r"\d+", u))
    return [q for q in quantities if q not in url_nums]


def extract_claim_signals(text: str) -> ClaimSignal:
    # naive but deterministic heuristics
    dates = DATE_RE.findall(text)
    quantities = QUANTITY_RE.findall(text)
    # remove bare years (2024, 2026, etc.) — they're gravitational wells, not claim quantities
    quantities = [q for q in quantities if not _YEAR_RE.match(q)]
    # remove numbers that are only present in embedded URLs / params
    quantities = _strip_url_numbers(text, quantities)
    entities = []
    # capture multi-word capitalized phrases (e.g. "New York", "Jd Vance")
    # single-word matches are filtered by the sentence-initial stoplist below
    for m in ENTITY_RE.findall(text):
        if " " in m:
            entities.append(m)
        elif m not in _ENTITY_STOPWORDS:
            entities.append(m)
    modal = MODAL_RE.findall(text)

    # spans: short snippets that look like claims
    # heuristic: sentences with numbers, dates or modals
    spans = []
    for s in re.split(r"(?<=[\.\?!])\s+", text):
        if DATE_RE.search(s) or QUANTITY_RE.search(s) or MODAL_RE.search(s):
            spans.append(s.strip())

    return ClaimSignal(spans=spans, dates=dates, quantities=quantities, entities=entities, modal=modal)
