"""Small Reddit URL intent helpers shared by runtime and tests."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse


def reddit_url_points_to_comment(url: str) -> bool:
    """Return True when a Reddit URL identifies a specific comment."""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if query.get("comment"):
        return True

    fragment = (parsed.fragment or "").lower()
    if fragment.startswith("t1_"):
        return True

    parts = [part for part in parsed.path.strip("/").split("/") if part]
    try:
        comments_idx = next(idx for idx, part in enumerate(parts) if part.lower() == "comments")
    except StopIteration:
        return False

    after_comments = parts[comments_idx + 1:]
    return len(after_comments) >= 3


def has_upvote_intent(text: str) -> bool:
    """Return True for common upvote phrasings."""
    compact = re.sub(r"[^a-z0-9\s]", " ", text.strip().lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    return bool(re.search(r"\bup\s*vote\b|\bupvote\b", compact))


def has_comment_intent(text: str) -> bool:
    """Return True when the user's words point to a comment target."""
    compact = re.sub(r"[^a-z0-9\s]", " ", text.strip().lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    return bool(re.search(r"\bcomment\b", compact))
