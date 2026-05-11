"""
tools/opportunity_discovery_tool.py — Reddit SaaS promotion opportunity discovery.

Discovers Reddit posts/comments where a SaaS product can be credibly and helpfully
mentioned. Read-only — does not auto-post, auto-comment, or auto-vote.

Promotion ethics:
    Read each subreddit's rules before engaging.
    Only mention the product when it genuinely answers the question asked.
    Be transparent: disclose affiliation if asked.
    Never copy-paste the same reply across multiple threads.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, quote_plus, unquote, urlparse, urlunparse

import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SAAS_SUBREDDITS: frozenset[str] = frozenset({
    "saas", "startups", "entrepreneur", "smallbusiness", "b2bmarketing",
    "marketing", "sales", "digital_marketing", "growthhacking", "indiehackers",
    "sideproject", "productmanagement", "webdev", "devops", "sysadmin",
    "emailmarketing", "automation", "nocode", "softwareengineering",
    "programming", "technology", "business", "ecommerce", "consulting",
    "freelance", "remotework", "productivity", "saasquestions",
})

BUYING_INTENT_PHRASES: tuple[str, ...] = (
    "looking for",
    "recommend",
    "suggestion",
    "alternative to",
    "best way to",
    "how do i",
    "how to",
    "any tool",
    "any software",
    "any app",
    "need help",
    "which tool",
    "what tool",
    "best tool",
    "comparing",
    " vs ",
    " versus ",
    "switched from",
    "migrated from",
    "replacing",
    "instead of",
    "help me find",
    "can anyone",
    "anyone know",
    "what do you use",
    "what are you using",
    "tool for",
    "software for",
    "need a",
    "need an",
)

LOW_QUALITY_PATTERNS: tuple[str, ...] = (
    r"^\[deleted\]$",
    r"^\[removed\]$",
    r"^just a meme",
    r"\breddit\s*moment\b",
    r"\bcopypasta\b",
    r"\ball posts communities comments media people\b",
    r"\bback forward relevance safe search\b",
)

MIN_RELEVANCE = 70
MIN_CONFIDENCE = 75
AGENTIC_DEFAULT_CHUNK_SIZE = 10
AGENTIC_MAX_SEARCH_STEPS = 80
DEFAULT_OPPORTUNITY_TYPES: tuple[str, ...] = (
    "recent",
    "high_engagement",
    "high_google_search",
)
DEFAULT_LINK_KIND = "posts"
HIGH_ENGAGEMENT_MIN_SCORE = 50
HIGH_ENGAGEMENT_MIN_COMMENTS = 10

EMAILVERIFIER_PRODUCT_DEFAULTS: dict[str, str] = {
    "product_name": "emailverifier.io",
    "product_url": "https://emailverifier.io/",
    "product_description": (
        "Email verification SaaS that helps users verify email addresses, clean email lists, "
        "reduce bounce rates, remove invalid or disposable emails, and improve email deliverability."
    ),
    "target_customer": (
        "email marketers, sales teams, B2B SaaS founders, newsletter operators, agencies, "
        "recruiters, ecommerce businesses"
    ),
    "pain_points": (
        "high email bounce rates, invalid email lists, poor email deliverability, damaged sender "
        "reputation, fake signup emails, disposable email addresses, wasted outreach credits, "
        "cold email campaigns bouncing"
    ),
    "use_cases": (
        "bulk email list cleaning, email verification API, real-time signup email validation, "
        "lead list validation before outreach, newsletter list hygiene, CRM email cleanup"
    ),
    "keywords": (
        "email verifier, email verification, email validation, verify email, email checker, "
        "bulk email verification, email list cleaning, reduce bounce rate, disposable email detection, "
        "email deliverability"
    ),
    "competitor_names": (
        "NeverBounce, ZeroBounce, Hunter, Kickbox, Bouncer, DeBounce, Emailable, Mailfloss, Clearout"
    ),
    "product_mention_terms": (
        "emailverifier.io, www.emailverifier.io, https://emailverifier.io, "
        "emailverifier dot io, email verifier.io, emailverifier"
    ),
}

EMAILVERIFIER_REDDIT_QUERIES: tuple[str, ...] = (
    # High-intent handpicked anchors. Profile-derived queries are added around these.
    '"email list cleaning" "bounce rate"',
    '"clean email list" "bounce rate"',
    '"email list" "invalid emails"',
    '"cold email" "bounce rate"',
    '"cold email" "invalid emails"',
    '"cold outreach" "bounce rate"',
    '"email deliverability" "bounce rate"',
    '"sender reputation" "bounce rate"',
    '"email verification" "cold email"',
    '"email validation" "cold email"',
    '"bulk email verification"',
    '"email verification API"',
    '"email validation API"',
    '"disposable email" signups',
    '"fake signup" email validation',
    '"newsletter" "list hygiene"',
    '"email hygiene" newsletter',
    '"verify email list" outreach',
    '"remove invalid emails"',
    '"remove disposable emails"',
    '"ZeroBounce" alternative',
    '"NeverBounce" alternative',
    '"Hunter" "email verification"',
    '"Kickbox" alternative',
    '"Bouncer" alternative',
    '"DeBounce" alternative',
    '"Emailable" alternative',
    'email verifier alternative',
    'email verification tool bounce rate',
    'email validation tool deliverability',
    'bulk email checker list cleaning',
    'verify email list before outreach',
)

EMAILVERIFIER_GOOGLE_QUERIES: tuple[str, ...] = (
    '"email list cleaning" "bounce rate"',
    '"cold email" "bounce rate"',
    '"email deliverability" "bounce rate"',
    '"email verification tool" "cold email"',
    '"email validation API"',
    '"bulk email verification"',
    '"ZeroBounce" alternative',
    '"NeverBounce" alternative',
    '"Hunter" "email verification"',
    '"disposable email" signups',
    '"newsletter" "list hygiene"',
    '"verify email list" outreach',
)

EMAILVERIFIER_CONTEXT_TERMS: tuple[str, ...] = (
    "email verifier",
    "email verification tool",
    "email validation tool",
    "email checker",
    "bulk email verification",
    "email verification api",
    "email validation api",
    "email list cleaning",
    "clean email list",
    "email hygiene",
    "list hygiene",
    "email deliverability",
    "bounce rate",
    "email bounce",
    "bounced email",
    "bouncing",
    "invalid emails",
    "invalid email list",
    "disposable email",
    "fake signup",
    "sender reputation",
    "cold email",
    "cold outreach",
    "outreach credits",
    "newsletter list",
    "verify email list",
    "remove invalid emails",
    "remove disposable emails",
    "zerobounce",
    "neverbounce",
    "kickbox",
    "bouncer",
    "debounce",
    "emailable",
    "mailfloss",
    "clearout",
    "emaillistverify",
    "bouncify",
)

EMAILVERIFIER_WRONG_CONTEXT_PATTERNS: tuple[str, ...] = (
    r"\bverification email\b",
    r"\bemail verification code\b",
    r"\bverification code\b",
    r"\bverify my email\b",
    r"\baccount verification\b",
    r"\blogin verification\b",
    r"\b2fa\b",
    r"\botp\b",
    r"\bconfirm your email\b",
    r"\bemail confirmation\b",
    r"\bnot receiving emails?\b",
    r"\bgmail\b",
    r"\boutlook\b",
    r"\bprotonmail\b",
    r"\bemail finder\b",
    r"\bfind email address\b",
    r"\bget email from\b",
    r"\blinkedin url\b",
    r"\bscrape emails?\b",
    r"\bextract emails?\b",
)

_BLOCK_INDICATORS: tuple[str, ...] = (
    "429 rate limited",
    "rate limited",
    "too many requests",
    "please try again later",
    "error occurred",
    "search is currently unavailable",
    "captcha",
    "access denied",
    "403 forbidden",
    "unusual traffic",
    "are you a robot",
    "verify you are human",
    "robot or human",
    "whoa there",
    "you've been blocked",
    "you have been blocked",
    "temporarily blocked",
    "log in to reddit",
    "log in or sign up",
    "sign up or log in",
    "log in to continue",
    "login required",
)

_BLOCK_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bsomething went wrong\b.{0,80}\b(try again|reload|search)\b", "something went wrong"),
    (r"\bplease try again\b.{0,40}\blater\b", "please try again later"),
    (r"\bblocked\b.{0,60}\b(request|traffic|access|security)\b", "blocked"),
)

_HARD_BLOCK_INDICATORS: frozenset[str] = frozenset({
    "429 rate limited",
    "rate limited",
    "too many requests",
    "captcha",
    "access denied",
    "403 forbidden",
    "unusual traffic",
    "are you a robot",
    "verify you are human",
    "robot or human",
    "whoa there",
    "you've been blocked",
    "you have been blocked",
    "temporarily blocked",
    "blocked",
    "log in to reddit",
    "log in or sign up",
    "sign up or log in",
    "log in to continue",
    "login required",
})

_REDDIT_BROWSER_SOURCES: frozenset[str] = frozenset({
    "reddit_search",
    "reddit_authenticated_search",
    "manual_fallback_search",
})


# ---------------------------------------------------------------------------
# Pure helper functions (testable without browser)
# ---------------------------------------------------------------------------

def _dedupe_clean_strings(items: list[object]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        cleaned = re.sub(r"\s+", " ", str(item or "")).strip()
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            result.append(cleaned)
    return result


def _parse_list(text: object) -> list[str]:
    """Split comma/newline-separated text into non-empty cleaned strings."""
    if text is None:
        return []
    if isinstance(text, (list, tuple, set)):
        items: list[object] = []
        for item in text:
            items.extend(_parse_list(item))
        return _dedupe_clean_strings(items)
    items = re.split(r"[,;\n|]+", str(text))
    return _dedupe_clean_strings(list(items))


def _coerce_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not a numeric setting")
        number = int(float(str(value).strip()))
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(number, maximum))


def _parse_compact_count(value: object) -> Optional[int]:
    text = str(value or "").strip().lower().replace(",", "")
    if not text:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)\s*([km])?", text)
    if not match:
        return None
    number = float(match.group(1))
    suffix = match.group(2)
    if suffix == "k":
        number *= 1_000
    elif suffix == "m":
        number *= 1_000_000
    return int(number)


def _normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").lower()).strip()


def _normalize_choice_text(value: object) -> str:
    return re.sub(r"[^a-z0-9\s/,+&-]", " ", str(value or "").lower())


def normalize_opportunity_types(value: object) -> list[str]:
    """Normalize user-selected opportunity buckets into stable internal names."""
    text = _normalize_choice_text(value)
    compact = re.sub(r"\s+", " ", text).strip()
    if not compact:
        return []
    if re.search(r"\b(all|any|all three|everything)\b", compact):
        return list(DEFAULT_OPPORTUNITY_TYPES)

    selected: list[str] = []
    if (
        re.search(r"\b(recent|new|fresh)\b", compact)
        or re.search(r"\b(last|past|within|at most)\s+7\s+days?\b", compact)
        or re.search(r"\btype\s*1\b|\b1\b", compact)
    ):
        selected.append("recent")
    if (
        re.search(r"\b(high\s+engagement|engagement|popular|upvotes?|comments?\s+count|comment\s+count)\b", compact)
        or re.search(r"\btype\s*2\b|\b2\b", compact)
    ):
        selected.append("high_engagement")
    if (
        re.search(r"\b(google|ranked|ranking|high\s+ranked|seo)\b", compact)
        or re.search(r"\btype\s*3\b|\b3\b", compact)
    ):
        selected.append("high_google_search")

    return [item for item in DEFAULT_OPPORTUNITY_TYPES if item in set(selected)]


def normalize_link_kind(value: object) -> str:
    """Normalize whether the user asked for post URLs, comment URLs, or both."""
    text = _normalize_choice_text(value)
    compact = re.sub(r"\s+", " ", text).strip()
    if not compact:
        return ""
    if (
        "both" in compact
        or re.search(r"\bposts?\s*(and|&|\+|/)\s*comments?\b", compact)
        or re.search(r"\bcomments?\s*(and|&|\+|/)\s*posts?\b", compact)
    ):
        return "both"
    if re.search(r"\bcomments?\s*(only|urls?|links?)?\b", compact) and not re.search(r"\bpost", compact):
        return "comments"
    if re.search(r"\bposts?\s*(only|urls?|links?)?\b", compact):
        return "posts"
    return ""


def _allowed_result_types(link_kind: object) -> set[str]:
    normalized = normalize_link_kind(link_kind) or DEFAULT_LINK_KIND
    if normalized == "both":
        return {"post", "comment"}
    if normalized == "comments":
        return {"comment"}
    return {"post"}


def _term_in_text(term: str, text: str) -> bool:
    term_norm = _normalize_space(term)
    if not term_norm:
        return False
    if " " in term_norm:
        return term_norm in text
    return re.search(rf"(?<![a-z0-9]){re.escape(term_norm)}(?![a-z0-9])", text) is not None


def _domain_terms_from_url(value: object) -> list[str]:
    parsed = urlparse(str(value or "").strip())
    host = parsed.netloc.lower()
    if not host and "." in str(value or ""):
        host = str(value or "").strip().lower().split("/", 1)[0]
    if not host:
        return []
    terms = [host]
    if host.startswith("www."):
        terms.append(host[4:])
    else:
        terms.append(f"www.{host}")
    return terms


def _build_product_mention_terms(
    product_name: object,
    product_url: object = "",
    extra_terms: object = "",
) -> list[str]:
    raw: list[object] = []
    raw.extend(_parse_list(extra_terms))
    raw.extend(_domain_terms_from_url(product_url))

    name = str(product_name or "").strip()
    if name and ("." in name or " " not in name):
        raw.append(name)
    raw.extend(_domain_terms_from_url(name))

    return _dedupe_clean_strings(raw)


def _contains_product_mention(text: object, profile: dict) -> bool:
    normalized = _normalize_space(text)
    if not normalized:
        return False
    for term in profile.get("product_mention_terms", []):
        term_norm = _normalize_space(term)
        if not term_norm:
            continue
        if re.search(rf"(?<![a-z0-9]){re.escape(term_norm)}(?![a-z0-9])", normalized):
            return True
    return False


def _candidate_mentions_product(candidate: dict, profile: dict, page_text: object = "") -> bool:
    if candidate.get("mentions_product"):
        return True
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("body"),
            candidate.get("body_preview"),
            page_text,
        )
    )
    return _contains_product_mention(text, profile)


def _candidate_scoring_text(candidate: dict) -> str:
    return _normalize_space(
        " ".join(
            str(candidate.get(field, ""))
            for field in ("title", "body", "matched_query")
        )
    )


def _profile_is_emailverifier(profile: dict) -> bool:
    product_name = _normalize_space(profile.get("product_name", ""))
    product_url = _normalize_space(profile.get("product_url", ""))
    terms = " ".join(str(term) for term in profile.get("product_mention_terms", []))
    combined = f"{product_name} {product_url} {_normalize_space(terms)}"
    return "emailverifier.io" in combined or "emailverifier" in combined


def _emailverifier_candidate_context(candidate: dict) -> str:
    return _normalize_space(
        " ".join(
            str(candidate.get(field, ""))
            for field in ("title", "body", "body_preview", "matched_query", "subreddit")
        )
    )


def _has_wrong_emailverifier_context(candidate: dict) -> bool:
    text = _emailverifier_candidate_context(candidate)
    if not text:
        return False
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in EMAILVERIFIER_WRONG_CONTEXT_PATTERNS)


def _has_emailverifier_discovery_context(candidate: dict) -> bool:
    text = _emailverifier_candidate_context(candidate)
    if not text:
        return False
    return any(_term_in_text(term, text) for term in EMAILVERIFIER_CONTEXT_TERMS)


def _candidate_product_context(candidate: dict) -> str:
    parts: list[str] = []
    for field in ("title", "body", "body_preview", "post_body", "matched_query", "subreddit"):
        value = candidate.get(field)
        if value:
            parts.append(str(value))
    top_comments = candidate.get("top_comments")
    if isinstance(top_comments, list):
        for comment in top_comments:
            if isinstance(comment, dict):
                body = comment.get("body")
            else:
                body = comment
            if body:
                parts.append(str(body))
    return _normalize_space(" ".join(parts))


def _product_discovery_terms(profile: dict) -> list[str]:
    required = profile.get("required_context_terms_list", []) or []
    if required:
        return _dedupe_clean_strings(required)
    terms: list[object] = []
    terms.extend(profile.get("keywords_list", []) or [])
    terms.extend(profile.get("pain_points_list", []) or [])
    terms.extend(profile.get("use_cases_list", []) or [])
    terms.extend(profile.get("competitors_list", []) or [])
    return _dedupe_clean_strings(terms)


def _has_negative_product_context(candidate: dict, profile: dict) -> bool:
    negative_terms = profile.get("negative_keywords_list", []) or []
    if not negative_terms:
        return False
    text = _candidate_product_context(candidate)
    return any(_term_in_text(term, text) for term in negative_terms)


def _has_product_discovery_context(candidate: dict, profile: dict) -> bool:
    terms = _product_discovery_terms(profile)
    if not terms:
        return True
    text = _candidate_product_context(candidate)
    return any(_term_in_text(term, text) for term in terms)


def _clean_reddit_url(raw_url: str) -> str:
    """Return a canonical display URL for Reddit links, including Google wrappers."""
    value = str(raw_url or "").strip()
    if not value:
        return ""

    parsed = urlparse(value)
    host = parsed.netloc.lower()

    if "reddit.com" not in host:
        if "google." in host and parsed.path.startswith("/url"):
            query = parse_qs(parsed.query)
            wrapped = (query.get("q") or query.get("url") or [""])[0]
            value = wrapped or value
        else:
            match = re.search(
                r"https?://(?:old\.|www\.|new\.|np\.)?reddit\.com[^\s\"'<>&]+",
                value,
                re.IGNORECASE,
            )
            value = match.group(0) if match else value

    value = unquote(value)
    parsed = urlparse(value)
    if "reddit.com" not in parsed.netloc.lower():
        return ""

    path = re.sub(r"/+", "/", parsed.path or "")
    if not path or path == "/":
        return ""
    if not path.endswith("/"):
        path += "/"

    query = ""
    comment = (parse_qs(parsed.query).get("comment") or [""])[0]
    if comment:
        query = f"comment={quote_plus(comment)}"

    fragment = parsed.fragment if parsed.fragment.lower().startswith("t1_") else ""
    return urlunparse(("https", "www.reddit.com", path, "", query, fragment))


def _reddit_url_key(url: str) -> str:
    cleaned = _clean_reddit_url(url)
    if not cleaned:
        return str(url or "").rstrip("/").lower()

    parsed = urlparse(cleaned)
    parts = [unquote(part).lower() for part in parsed.path.strip("/").split("/") if part]
    try:
        comments_idx = parts.index("comments")
    except ValueError:
        return cleaned.rstrip("/").lower()

    if len(parts) <= comments_idx + 1:
        return cleaned.rstrip("/").lower()

    subreddit = ""
    if "r" in parts:
        r_idx = parts.index("r")
        if len(parts) > r_idx + 1:
            subreddit = parts[r_idx + 1]

    key_parts = ["reddit"]
    if subreddit:
        key_parts.extend(["r", subreddit])
    key_parts.extend(["comments", parts[comments_idx + 1]])

    comment_id = ""
    after_comments = parts[comments_idx + 1:]
    if len(after_comments) >= 3:
        comment_id = after_comments[2]
    else:
        query_comment = (parse_qs(parsed.query).get("comment") or [""])[0]
        if query_comment:
            comment_id = query_comment.lower()
        elif parsed.fragment.lower().startswith("t1_"):
            comment_id = parsed.fragment[3:].lower()
    if comment_id:
        key_parts.append(comment_id.removeprefix("t1_"))

    return "/".join(key_parts)


def _reddit_comment_id_from_url(url: str) -> str:
    """Return a Reddit comment id when the URL identifies a specific comment."""
    cleaned = _clean_reddit_url(url)
    if not cleaned:
        return ""

    parsed = urlparse(cleaned)
    parts = [unquote(part).lower() for part in parsed.path.strip("/").split("/") if part]
    try:
        comments_idx = parts.index("comments")
    except ValueError:
        return ""

    after_comments = parts[comments_idx + 1:]
    if len(after_comments) >= 3 and re.fullmatch(r"[a-z0-9_]+", after_comments[2]):
        return after_comments[2].removeprefix("t1_")

    query_comment = (parse_qs(parsed.query).get("comment") or [""])[0].strip().lower()
    if query_comment:
        return query_comment.removeprefix("t1_")

    if parsed.fragment.lower().startswith("t1_"):
        return parsed.fragment[3:].lower()

    return ""


def _is_reddit_comment_permalink(url: str) -> bool:
    return bool(_reddit_comment_id_from_url(url))


def _format_date(utc_timestamp: float) -> str:
    if not utc_timestamp:
        return "unknown"
    return datetime.fromtimestamp(utc_timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def generate_search_queries(profile: dict) -> list[str]:
    """Generate Reddit and Google search queries from the product profile.

    Returns a deduplicated list ordered from most- to least-specific.
    """
    keywords: list[str] = profile.get("keywords_list", [])
    pain_points: list[str] = profile.get("pain_points_list", [])
    use_cases: list[str] = profile.get("use_cases_list", [])
    competitors: list[str] = profile.get("competitors_list", [])
    target: str = profile.get("target_customer", "")
    custom_queries: list[str] = profile.get("search_queries_list", [])

    raw: list[str] = []

    # Caller-provided queries are highest priority. They let the API user encode
    # niche vocabulary that a generic generator may not infer.
    raw.extend(custom_queries)

    # High-intent problem/product anchors first. The API pipeline usually takes
    # only the first handful of queries per run, so these should be more specific
    # than broad "best software" searches.
    for pp in pain_points[:6]:
        for kw in keywords[:3]:
            raw.append(f'"{pp}" "{kw}"')
        raw.append(f"how to solve {pp}")
        raw.append(f"struggling with {pp}")
        raw.append(f"solution for {pp}")
        raw.append(f"tool for {pp}")

    for uc in use_cases[:6]:
        for kw in keywords[:2]:
            raw.append(f'"{uc}" "{kw}"')
        raw.append(f"tool for {uc}")
        raw.append(f"software for {uc}")
        raw.append(f"recommend tool for {uc}")

    for comp in competitors[:6]:
        raw.append(f"alternative to {comp}")
        raw.append(f"{comp} alternative")
        raw.append(f"alternatives to {comp}")
        raw.append(f"{comp} vs {profile.get('product_name', '').strip() or 'other tools'}")

    # Buying-intent + keyword combos.
    for kw in keywords[:8]:
        raw.append(f"recommend {kw} tool")
        raw.append(f"best {kw} software")
        raw.append(f"looking for {kw} app")
        raw.append(f"any tool for {kw}")
        raw.append(f'"recommend" {kw}')
        raw.append(f'"looking for" {kw}')
        raw.append(f'"any tool" {kw}')

    # Target customer angle
    if target:
        for audience in _parse_list(target)[:6]:
            raw.append(f"{audience} tools")
            raw.append(f"{audience} software recommendations")
            for kw in keywords[:2]:
                raw.append(f"best {kw} for {audience}")
        if keywords:
            raw.append(f"best {keywords[0]} for {target}")

    # Raw keyword searches are useful as a fallback, but put them after
    # intent-heavy queries.
    raw.extend(keywords[:8])

    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for q in raw:
        q_stripped = q.strip()
        if q_stripped and q_stripped.lower() not in seen:
            seen.add(q_stripped.lower())
            result.append(q_stripped)

    return result


def build_opportunity_profile(
    product_name: str,
    product_description: str,
    target_customer: str,
    pain_points: str,
    use_cases: str,
    keywords: str,
    competitor_names: str = "",
    excluded_subreddits: str = "",
    max_age_days: int = 730,
    product_url: str = "",
    product_mention_terms: str = "",
    search_queries: str = "",
    required_context_terms: str = "",
    negative_keywords: str = "",
) -> dict:
    """Build the normalized profile used by both bulk and agentic discovery."""
    return {
        "product_name": product_name,
        "product_url": product_url,
        "product_description": product_description,
        "target_customer": target_customer,
        "pain_points_list": _parse_list(pain_points),
        "use_cases_list": _parse_list(use_cases),
        "keywords_list": _parse_list(keywords),
        "competitors_list": _parse_list(competitor_names),
        "search_queries_list": _parse_list(search_queries),
        "required_context_terms_list": _parse_list(required_context_terms),
        "negative_keywords_list": _parse_list(negative_keywords),
        "excluded_subreddits": {s.strip().lower() for s in _parse_list(excluded_subreddits)},
        "max_age_days": max_age_days,
        "product_mention_terms": _build_product_mention_terms(
            product_name,
            product_url,
            product_mention_terms,
        ),
    }


def build_agentic_search_plan(
    profile: dict,
    max_steps: int = AGENTIC_MAX_SEARCH_STEPS,
    opportunity_types: object = None,
    link_kind: object = None,
) -> list[dict]:
    """
    Build a varied browser search plan for interactive discovery.

    The plan follows the user's requested buckets and result kind so the agent
    can review candidates one by one instead of doing one bulk score pass.
    """
    selected_types = normalize_opportunity_types(
        opportunity_types if opportunity_types is not None else profile.get("opportunity_types", "")
    ) or list(DEFAULT_OPPORTUNITY_TYPES)
    allowed_result_types = _allowed_result_types(
        link_kind if link_kind is not None else profile.get("link_kind", DEFAULT_LINK_KIND)
    )
    reddit_queries = generate_search_queries(profile)
    google_queries = _build_google_queries(profile)
    plan: list[dict] = []

    for idx, query in enumerate(reddit_queries):
        if "post" in allowed_result_types:
            if "recent" in selected_types:
                plan.append({
                    "mode": "reddit",
                    "query": query,
                    "result_type": "link",
                    "sort": "relevance",
                    "time_filter": "week",
                    "safe_search": False,
                    "opportunity_type": "recent",
                })
            if "high_engagement" in selected_types:
                plan.append({
                    "mode": "reddit",
                    "query": query,
                    "result_type": "link",
                    "sort": "relevance",
                    "safe_search": False,
                    "opportunity_type": "high_engagement",
                })
        if "comment" in allowed_result_types and idx < 10:
            if "recent" in selected_types:
                plan.append({
                    "mode": "reddit",
                    "query": query,
                    "result_type": "comment",
                    "sort": "relevance",
                    "time_filter": "week",
                    "safe_search": False,
                    "opportunity_type": "recent",
                })
            if "high_engagement" in selected_types:
                plan.append({
                    "mode": "reddit",
                    "query": query,
                    "result_type": "comment",
                    "sort": "relevance",
                    "safe_search": False,
                    "opportunity_type": "high_engagement",
                })
        if "high_google_search" in selected_types and "post" in allowed_result_types and idx and idx % 5 == 0 and google_queries:
            gq = google_queries.pop(0)
            plan.append({
                "mode": "google",
                "query": gq,
                "result_type": "link",
                "sort": "relevance",
                "opportunity_type": "high_google_search",
            })

    if "high_google_search" in selected_types and "post" in allowed_result_types:
        for query in google_queries:
            plan.append({
                "mode": "google",
                "query": query,
                "result_type": "link",
                "sort": "relevance",
                "opportunity_type": "high_google_search",
            })

    return plan[:max(1, max_steps)]


def _score_candidate(candidate: dict, profile: dict) -> tuple[int, int]:
    """Return (relevance_score 0-100, confidence_score 0-100) for one candidate."""
    keywords: list[str] = profile.get("keywords_list", [])
    pain_points: list[str] = profile.get("pain_points_list", [])
    use_cases: list[str] = profile.get("use_cases_list", [])
    competitors: list[str] = profile.get("competitors_list", [])

    text = _candidate_scoring_text(candidate)

    kw_hits = sum(1 for kw in keywords if _term_in_text(kw, text))
    pp_hits = sum(1 for pp in pain_points if _term_in_text(pp, text))
    use_hits = sum(1 for uc in use_cases if _term_in_text(uc, text))
    comp_hit = any(_term_in_text(c, text) for c in competitors)
    intent_hit = any(_normalize_space(phrase) in text for phrase in BUYING_INTENT_PHRASES)
    sub_bonus = 10 if candidate.get("subreddit", "").lower() in SAAS_SUBREDDITS else 0
    source = candidate.get("source", "")
    query_bonus = 10 if source == "google_search" and candidate.get("matched_query") else 0

    relevance = min(
        100,
        min(40, kw_hits * 25)
        + min(40, pp_hits * 30)
        + min(30, use_hits * 25)
        + (35 if comp_hit else 0)
        + (15 if intent_hit else 0)
        + sub_bonus
        + query_bonus,
    )

    try:
        age_days = float(candidate.get("age_days", 730) or 730)
    except (TypeError, ValueError):
        age_days = 730.0
    score_val = _parse_compact_count(candidate.get("score")) or 0
    comments = _parse_compact_count(candidate.get("comment_count")) or 0
    status: str = candidate.get("status", "unknown")

    freshness = max(0, 35 - int(age_days / 30))
    engagement = min(20, int((score_val + comments * 2) / 5))
    active_bonus = 15 if status == "active" else 5 if status == "unknown" else 0
    google_bonus = 10 if source == "google_search" else 0
    # Browser-backed Reddit search is first-party UI discovery, so trust it like prior Reddit search.
    reddit_bonus = 5 if source in _REDDIT_BROWSER_SOURCES else 0

    confidence = min(
        100,
        int(relevance * 0.55) + freshness + engagement + active_bonus + google_bonus + reddit_bonus,
    )

    return relevance, confidence


def _assign_category(candidate: dict, recent_days: int = 7) -> str:
    """Return 'recent', 'high_engagement', or 'high_google_search'."""
    if candidate.get("source") == "google_search":
        return "high_google_search"
    try:
        age_days = float(candidate.get("age_days", 999) or 999)
    except (TypeError, ValueError):
        age_days = 999.0
    if age_days <= recent_days:
        return "recent"
    score_val = _parse_compact_count(candidate.get("score")) or 0
    comments = _parse_compact_count(candidate.get("comment_count")) or 0
    if score_val >= 50 or comments >= 10:
        return "high_engagement"
    # Fallback: recency wins over arbitrary "high_engagement" label
    return "recent" if age_days <= 30 else "high_engagement"


def _candidate_age_days(candidate: dict, default: float = 9999.0) -> float:
    try:
        return float(candidate.get("age_days", default) or default)
    except (TypeError, ValueError):
        return default


def _has_high_engagement(candidate: dict) -> bool:
    score_val = _parse_compact_count(candidate.get("score")) or 0
    comments = _parse_compact_count(candidate.get("comment_count")) or 0
    return score_val >= HIGH_ENGAGEMENT_MIN_SCORE or comments >= HIGH_ENGAGEMENT_MIN_COMMENTS


def candidate_matches_opportunity_preferences(
    candidate: dict,
    opportunity_types: object,
    link_kind: object,
    recent_days: int = 7,
) -> bool:
    """Return True when a candidate matches the user's requested discovery buckets."""
    return bool(candidate_opportunity_category(candidate, opportunity_types, link_kind, recent_days))


def candidate_opportunity_category(
    candidate: dict,
    opportunity_types: object,
    link_kind: object,
    recent_days: int = 7,
) -> str:
    """Return the requested bucket matched by this candidate, or an empty string."""
    if candidate.get("type", "post") not in _allowed_result_types(link_kind):
        return ""

    selected = normalize_opportunity_types(opportunity_types) or list(DEFAULT_OPPORTUNITY_TYPES)
    if "high_google_search" in selected and candidate.get("source") == "google_search":
        return "high_google_search"
    if "recent" in selected and candidate.get("source") != "google_search":
        if _candidate_age_days(candidate) <= recent_days:
            return "recent"
    if "high_engagement" in selected and _has_high_engagement(candidate):
        return "high_engagement"
    return ""


def _is_deleted_or_removed(candidate: dict) -> bool:
    if candidate.get("removed") or candidate.get("deleted"):
        return True
    for field in ("title", "body"):
        val = _normalize_space(candidate.get(field, ""))
        if val in ("[deleted]", "[removed]"):
            return True
    return False


def _is_too_old(candidate: dict, max_age_days: int) -> bool:
    if candidate.get("source") == "google_search" and not candidate.get("created_utc"):
        return False
    try:
        return float(candidate.get("age_days", 9999)) > max_age_days
    except (TypeError, ValueError):
        return True


def _is_low_quality(candidate: dict) -> bool:
    title = candidate.get("title") or ""
    body = candidate.get("body") or ""
    if candidate.get("type") == "comment" and not _is_reddit_comment_permalink(candidate.get("url", "")):
        return True
    for pattern in LOW_QUALITY_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE) or re.search(pattern, body, re.IGNORECASE):
            return True
    if candidate.get("type") == "post" and len(title) < 10:
        return True
    return False


def deduplicate_candidates(candidates: list[dict]) -> list[dict]:
    """Remove duplicate candidates by normalized URL, preserving insertion order."""
    seen: set[str] = set()
    result: list[dict] = []
    for c in candidates:
        key = _reddit_url_key(c.get("url", ""))
        if key and key not in seen:
            seen.add(key)
            cleaned_url = _clean_reddit_url(c.get("url", ""))
            if cleaned_url:
                c["url"] = cleaned_url
            result.append(c)
    return result


def _reject_reason(
    candidate: dict,
    profile: dict,
    min_relevance: int = MIN_RELEVANCE,
    min_confidence: int = MIN_CONFIDENCE,
) -> Optional[str]:
    """Return the rejection reason string, or None if the candidate passes all filters."""
    base_reason = _basic_reject_reason(candidate, profile)
    if base_reason:
        return base_reason
    rel, conf = _score_candidate(candidate, profile)
    if rel < min_relevance:
        return f"low_relevance:{rel}"
    if conf < min_confidence:
        return f"low_confidence:{conf}"
    return None


def _basic_reject_reason(candidate: dict, profile: dict) -> Optional[str]:
    """
    Return non-score rejection reasons only.

    Agentic discovery uses this before asking the LLM to judge fit, so a candidate
    is not thrown away merely because it lacks exact keyword/competitor matches.
    """
    if candidate.get("excluded"):
        return "excluded_subreddit"
    if _is_deleted_or_removed(candidate):
        return "deleted_or_removed"
    status = str(candidate.get("status", "")).lower()
    if candidate.get("locked") or candidate.get("archived") or status in {"locked", "archived"}:
        return "locked_or_archived"
    if status == "unavailable":
        return "unavailable"
    if _candidate_mentions_product(candidate, profile):
        return "already_mentions_product"
    if _has_negative_product_context(candidate, profile):
        return "negative_product_context"
    if profile.get("required_context_terms_list") and not _has_product_discovery_context(candidate, profile):
        return "low_product_context"
    if _is_too_old(candidate, profile.get("max_age_days", 730)):
        return "too_old"
    if _is_low_quality(candidate):
        return "low_quality"
    return None


def _matched_pain_point(candidate: dict, profile: dict) -> str:
    text = _candidate_scoring_text(candidate)
    for pp in profile.get("pain_points_list", []):
        if _term_in_text(pp, text):
            return pp
    for uc in profile.get("use_cases_list", []):
        if _term_in_text(uc, text):
            return uc
    return ""


def _suggested_angle(candidate: dict, profile: dict) -> str:
    name = profile.get("product_name", "your product")
    text = _candidate_scoring_text(candidate)

    for comp in profile.get("competitors_list", []):
        if _term_in_text(comp, text):
            return (
                f"Position {name} as an alternative to {comp}: explain key differentiators "
                f"and link to a relevant comparison page or free trial."
            )

    matched = _matched_pain_point(candidate, profile)
    if matched:
        return (
            f"Directly address '{matched}' with a concrete example, "
            f"then mention how {name} solves it — keep it helpful, not promotional."
        )

    if any(phrase in text for phrase in ("looking for", "recommend", "what tool", "any tool", "any software")):
        return (
            f"Recommend {name} with a brief explanation of why it fits this exact use case, "
            f"and offer to answer follow-up questions."
        )

    keywords: list[str] = profile.get("keywords_list", [])
    for kw in keywords:
        if _term_in_text(kw, text):
            return (
                f"Add helpful context about {kw} and naturally mention {name} "
                f"only if it genuinely solves the problem being discussed."
            )

    return (
        f"Provide a genuinely helpful answer to the question, "
        f"then mention {name} briefly if it fits without forcing it."
    )


def _build_reason(candidate: dict, profile: dict) -> str:
    text = _candidate_scoring_text(candidate)
    reasons: list[str] = []

    for kw in profile.get("keywords_list", []):
        if _term_in_text(kw, text):
            reasons.append(f"mentions keyword '{kw}'")

    for pp in profile.get("pain_points_list", []):
        if _term_in_text(pp, text):
            reasons.append(f"discusses pain point '{pp}'")

    for uc in profile.get("use_cases_list", []):
        if _term_in_text(uc, text):
            reasons.append(f"matches use case '{uc}'")

    for comp in profile.get("competitors_list", []):
        if _term_in_text(comp, text):
            reasons.append(f"references competitor '{comp}' — strong 'alternatives' angle")

    if any(phrase in text for phrase in BUYING_INTENT_PHRASES):
        reasons.append("contains buying-intent language")

    if candidate.get("source") == "google_search":
        rank = candidate.get("google_rank")
        suffix = f" (Google rank #{rank})" if rank else ""
        reasons.append(f"appears in Google search results for a relevant query{suffix}")

    if not reasons:
        reasons.append("matches multiple product keywords in title/body")

    return "; ".join(reasons[:4]) + "."


def _build_result(candidate: dict, profile: dict, category: str) -> dict:
    rel, conf = _score_candidate(candidate, profile)
    return {
        "url": candidate.get("url", ""),
        "type": candidate.get("type", "post"),
        "subreddit": candidate.get("subreddit", ""),
        "title": candidate.get("title", ""),
        "created_date": candidate.get("created_date", "unknown"),
        "score": candidate.get("score"),
        "comment_count": candidate.get("comment_count"),
        "reply_count": candidate.get("reply_count"),
        "category": category,
        "relevance_score": rel,
        "confidence_score": conf,
        "reason": _build_reason(candidate, profile),
        "matched_pain_point": _matched_pain_point(candidate, profile),
        "suggested_angle": _suggested_angle(candidate, profile),
        "status": candidate.get("status", "active"),
        "source": candidate.get("source", "reddit_authenticated_search"),
    }


def _extract_subreddit_from_url(url: str) -> str:
    cleaned = _clean_reddit_url(url) or url
    m = re.search(r"reddit\.com/r/([^/?#]+)", cleaned, re.IGNORECASE)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Block detection (pure, testable)
# ---------------------------------------------------------------------------

def _detect_block_reason(body_text: str, page_title: str = "") -> Optional[str]:
    """Return a block indicator string if the page looks blocked/rate-limited, else None."""
    combined = _normalize_space(f"{body_text} {page_title}")
    if not combined:
        return None
    for indicator in _BLOCK_INDICATORS:
        if indicator in combined:
            return indicator
    for pattern, label in _BLOCK_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return label
    return None


def _is_hard_block_reason(reason: Optional[str]) -> bool:
    """Return True when another immediate authenticated query is likely to worsen blocking."""
    if not reason:
        return False
    reason_text = _normalize_space(reason)
    return any(indicator in reason_text for indicator in _HARD_BLOCK_INDICATORS)


# ---------------------------------------------------------------------------
# Browser search result normalizer (pure, testable)
# ---------------------------------------------------------------------------

def _normalize_browser_search_result(
    raw: dict,
    query: str = "",
    source: str = "reddit_authenticated_search",
) -> Optional[dict]:
    """Normalize a raw DOM-extracted Reddit search result into the internal candidate format."""
    url = _clean_reddit_url(raw.get("url", ""))
    if not url:
        return None

    raw_type = str(raw.get("type", "post")).lower()
    result_type = "comment" if raw_type == "comment" else "post"
    if result_type == "comment" and not _is_reddit_comment_permalink(url):
        return None

    subreddit = raw.get("subreddit", "") or _extract_subreddit_from_url(url)
    title = str(raw.get("title") or "").strip()
    body_preview = str(raw.get("bodyPreview") or "").strip()
    combined_preview = f"{title} {body_preview}"
    if any(re.search(pattern, combined_preview, re.IGNORECASE) for pattern in LOW_QUALITY_PATTERNS):
        return None

    score = _parse_compact_count(raw.get("scoreText"))
    comment_count = _parse_compact_count(raw.get("commentCountText"))

    created_utc = 0.0
    age_days = 365.0  # conservative default when timestamp unavailable
    time_text = str(raw.get("timeText") or "").strip()
    if time_text:
        try:
            dt = datetime.fromisoformat(time_text.replace("Z", "+00:00"))
            created_utc = dt.timestamp()
            age_days = (time.time() - created_utc) / 86400.0
        except (ValueError, TypeError):
            pass

    created_date = _format_date(created_utc) if created_utc else "unknown"

    # For comments with no title, fall back to body preview
    if result_type == "comment" and not title and body_preview:
        title = body_preview[:200]

    return {
        "url": url,
        "type": result_type,
        "subreddit": subreddit,
        "title": title[:250],
        "body": body_preview[:1000],
        "matched_query": query,
        "created_utc": created_utc,
        "created_date": created_date,
        "age_days": age_days,
        "score": score,
        "comment_count": comment_count,
        "reply_count": None,
        "removed": False,
        "deleted": False,
        "locked": False,
        "archived": False,
        "status": "unknown",
        "source": source,
        "google_rank": None,
        "excluded": False,
    }


# ---------------------------------------------------------------------------
# Browser-based functions (Playwright page required)
# ---------------------------------------------------------------------------

_SEARCH_JS_POSTS = """() => {
    const results = [];
    const seen = new Set();

    // New Reddit: shreddit-post custom elements
    for (const post of document.querySelectorAll('shreddit-post')) {
        const permalink = post.getAttribute('permalink') || '';
        if (!permalink) continue;
        const url = permalink.startsWith('http')
            ? permalink
            : 'https://www.reddit.com' + permalink;
        const key = url.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        const timeEl = post.querySelector('faceplate-timeago');
        const timeText = (timeEl && (timeEl.getAttribute('ts') || timeEl.getAttribute('datetime'))) || '';
        results.push({
            url,
            type: 'post',
            title: post.getAttribute('post-title') || '',
            subreddit: post.getAttribute('subreddit-name') || '',
            scoreText: post.getAttribute('score') || '',
            commentCountText: post.getAttribute('comment-count') || '',
            timeText,
            bodyPreview: (post.querySelector('[slot="text-body"]') || {}).innerText || '',
        });
        if (results.length >= 25) break;
    }

    // Fallback: anchor-based extraction for older/different Reddit layouts
    if (results.length === 0) {
        for (const a of document.querySelectorAll('a[href*="/comments/"]')) {
            const url = a.href || '';
            if (!url || !url.includes('reddit.com')) continue;
            const key = url.toLowerCase();
            if (seen.has(key)) continue;
            seen.add(key);
            const container = a.closest(
                'search-telemetry-tracker, article, [data-testid="post-container"], li'
            ) || a;
            const text = (container.innerText || a.innerText || '').replace(/\\s+/g, ' ').trim();
            const subMatch = url.match(/reddit\\.com\\/r\\/([^\\/\\?#]+)/i);
            results.push({
                url,
                type: 'post',
                title: (a.innerText || a.getAttribute('aria-label') || '').trim().slice(0, 300) || text.slice(0, 200),
                subreddit: subMatch ? subMatch[1] : '',
                scoreText: '',
                commentCountText: '',
                timeText: '',
                bodyPreview: text.slice(0, 500),
            });
            if (results.length >= 25) break;
        }
    }

    return results;
}"""

_SEARCH_JS_COMMENTS = """() => {
    const results = [];
    const seen = new Set();
    const isCommentPermalink = (rawUrl) => {
        try {
            const url = new URL(rawUrl);
            const parts = url.pathname.split('/').filter(Boolean).map(p => p.toLowerCase());
            const commentsIndex = parts.indexOf('comments');
            if (commentsIndex === -1) return false;
            const after = parts.slice(commentsIndex + 1);
            if (after.length >= 3) return true;
            if (url.searchParams.get('comment')) return true;
            if ((url.hash || '').toLowerCase().startsWith('#t1_')) return true;
            return false;
        } catch (_) {
            return false;
        }
    };

    for (const a of document.querySelectorAll('a[href*="/comments/"]')) {
        const url = a.href || '';
        if (!url || !url.includes('reddit.com')) continue;
        if (!isCommentPermalink(url)) continue;
        const key = url.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        const container = a.closest(
            'search-telemetry-tracker, shreddit-comment, article, [data-testid], li'
        ) || a;
        const text = (container.innerText || a.innerText || '').replace(/\\s+/g, ' ').trim();
        const subMatch = url.match(/reddit\\.com\\/r\\/([^\\/\\?#]+)/i);
        const timeEl = container.querySelector && container.querySelector('faceplate-timeago');
        const timeText = timeEl ? (timeEl.getAttribute('ts') || timeEl.getAttribute('datetime') || '') : '';
        results.push({
            url,
            type: 'comment',
            title: '',
            subreddit: subMatch ? subMatch[1] : '',
            scoreText: '',
            commentCountText: '',
            timeText,
            bodyPreview: text.slice(0, 600),
        });
        if (results.length >= 25) break;
    }

    return results;
}"""


def _build_reddit_search_url(
    query: str,
    result_type: str = "link",
    sort: str = "relevance",
    time_filter: str = "",
    safe_search: Optional[bool] = None,
) -> str:
    """Build the logged-in Reddit search URL used by the browser session."""
    params = [
        f"q={quote_plus(query)}",
        f"type={quote_plus(result_type)}",
        f"sort={quote_plus(sort)}",
    ]
    if time_filter:
        params.append(f"t={quote_plus(time_filter)}")
    if safe_search is False:
        params.append("include_over_18=on")
    return f"https://www.reddit.com/search/?{'&'.join(params)}"


async def _authenticated_reddit_search(
    page,
    query: str,
    result_type: str = "link",
    sort: str = "relevance",
    max_pages: int = 3,
    time_filter: str = "",
    safe_search: Optional[bool] = None,
) -> tuple[list[dict], Optional[str]]:
    """
    Search Reddit using the logged-in browser session.

    Returns (candidates, block_reason). block_reason is None on success or
    a string describing why results could not be fetched (block/rate-limit/error).
    """
    results: list[dict] = []
    seen_urls: set[str] = set()
    block_reason: Optional[str] = None

    search_url = _build_reddit_search_url(
        query=query,
        result_type=result_type,
        sort=sort,
        time_filter=time_filter,
        safe_search=safe_search,
    )
    js_extractor = _SEARCH_JS_COMMENTS if result_type == "comment" else _SEARCH_JS_POSTS

    for page_num in range(max(1, max_pages)):
        try:
            if page_num == 0:
                resp = await page.goto(search_url, wait_until="domcontentloaded", timeout=25_000)
                if resp and resp.status == 429:
                    block_reason = "429 rate limited"
                    logger.debug("authenticated_search_blocked", query=query, status=429)
                    return [], block_reason
                try:
                    await page.wait_for_load_state("networkidle", timeout=6_000)
                except Exception:
                    pass
                # Human-like pause after page load
                await asyncio.sleep(0.8 + page_num * 0.3)
            else:
                # Scroll to trigger infinite-scroll pagination
                await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.5)

            body_text: str = await page.evaluate("() => document.body?.innerText || ''")
            page_title: str = await page.title()

            detected = _detect_block_reason(body_text, page_title)
            if detected:
                block_reason = detected
                logger.debug(
                    "authenticated_search_blocked",
                    query=query,
                    indicator=detected,
                )
                break

            raw_items: list[dict] = await page.evaluate(js_extractor)

            new_this_page = 0
            for raw in raw_items:
                candidate = _normalize_browser_search_result(raw, query=query)
                if candidate is None:
                    continue
                url_key = _reddit_url_key(candidate["url"])
                if url_key and url_key not in seen_urls:
                    seen_urls.add(url_key)
                    results.append(candidate)
                    new_this_page += 1

            # Stop paginating if no new results appeared after scroll
            if page_num > 0 and new_this_page == 0:
                break

        except Exception as exc:
            logger.debug("authenticated_search_error", query=query, error=str(exc))
            block_reason = str(exc)
            break

    return results, block_reason


async def _manual_reddit_search(
    page,
    query: str,
    result_type: str = "link",
    sort: str = "relevance",
    max_results: int = 10,
    time_filter: str = "",
    safe_search: Optional[bool] = None,
) -> list[dict]:
    """
    Conservative fallback Reddit search via the normal UI.

    Used when authenticated search is blocked or returns nothing. Applies
    longer delays, fewer pages, and no aggressive looping.
    """
    results: list[dict] = []
    seen_urls: set[str] = set()

    search_url = _build_reddit_search_url(
        query=query,
        result_type=result_type,
        sort=sort,
        time_filter=time_filter,
        safe_search=safe_search,
    )
    js_extractor = _SEARCH_JS_COMMENTS if result_type == "comment" else _SEARCH_JS_POSTS

    try:
        resp = await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
        if resp and resp.status == 429:
            logger.debug("manual_search_rate_limited", query=query, status=429)
            return []
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass
        # Longer conservative delay
        await asyncio.sleep(2.5)

        body_text: str = await page.evaluate("() => document.body?.innerText || ''")
        page_title: str = await page.title()
        detected = _detect_block_reason(body_text, page_title)
        if detected:
            logger.debug("manual_search_also_blocked", query=query, indicator=detected)
            return []

        raw_items: list[dict] = await page.evaluate(js_extractor)

        for raw in raw_items:
            candidate = _normalize_browser_search_result(
                raw,
                query=query,
                source="manual_fallback_search",
            )
            if candidate is None:
                continue
            url_key = _reddit_url_key(candidate["url"])
            if url_key and url_key not in seen_urls:
                seen_urls.add(url_key)
                results.append(candidate)
            if len(results) >= max_results:
                break

    except Exception as exc:
        logger.debug("manual_search_error", query=query, error=str(exc))

    return results


async def _google_search(page, query: str, max_results: int = 10) -> list[dict]:
    """Navigate to Google with site:reddit.com prefix and extract Reddit URLs."""
    results: list[dict] = []
    search_url = (
        f"https://www.google.com/search"
        f"?q={quote_plus('site:reddit.com ' + query)}&num=10"
    )

    try:
        await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=5_000)
        except Exception:
            pass

        body_text: str = await page.evaluate("() => document.body?.innerText || ''")
        if "unusual traffic" in body_text.lower() or "captcha" in body_text.lower():
            logger.debug("google_blocked", query=query)
            return []

        links: list[dict] = await page.evaluate("""() => {
            const found = [];
            for (const a of document.querySelectorAll('a[href]')) {
                const href = a.href || '';
                if (href.includes('reddit.com') && href.includes('/comments/')) {
                    const container = a.closest('div[data-snc], div[data-hveid], h3, li') || a;
                    const text = (container.innerText || a.innerText || '').trim().slice(0, 300);
                    found.push({ url: href, context: text });
                    if (found.length >= 10) break;
                }
            }
            return found;
        }""")

        for rank, link in enumerate(links[:max_results], start=1):
            raw_url: str = link.get("url", "")
            context: str = link.get("context", "")
            if not raw_url:
                continue

            m = re.search(r"(https?://(?:www\.)?reddit\.com[^\s\"'&]+)", raw_url)
            clean_url = _clean_reddit_url(m.group(1) if m else raw_url)
            if not clean_url:
                continue

            results.append({
                "url": clean_url,
                "type": "post",
                "subreddit": _extract_subreddit_from_url(clean_url),
                "title": context[:200],
                "body": "",
                "matched_query": query,
                "created_utc": 0.0,
                "created_date": "unknown",
                "age_days": 365.0,
                "score": None,
                "comment_count": None,
                "reply_count": None,
                "removed": False,
                "deleted": False,
                "locked": False,
                "archived": False,
                "status": "unknown",
                "source": "google_search",
                "google_rank": rank,
                "excluded": False,
            })
    except Exception as exc:
        logger.debug("google_search_error", query=query, error=str(exc))

    return results


async def _verify_candidate(page, candidate: dict, profile: Optional[dict] = None) -> dict:
    """Load the candidate URL and update its status based on visible page content."""
    url = _clean_reddit_url(candidate.get("url", "")) or candidate.get("url", "")
    if url:
        candidate["url"] = url
    if not url:
        candidate["status"] = "unavailable"
        return candidate

    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        if not resp or resp.status in (404, 403, 410, 429, 451):
            candidate["status"] = "unavailable"
            return candidate

        try:
            await page.wait_for_load_state("networkidle", timeout=5_000)
        except Exception:
            pass

        body_text: str = await page.evaluate("() => document.body?.innerText || ''")
        page_title: str = await page.title()
        text_lower = body_text.lower()

        # Check for block/rate-limit/login-wall first
        if _detect_block_reason(body_text, page_title):
            candidate["status"] = "unavailable"
            return candidate

        # No meaningful content
        if len(body_text.strip()) < 80:
            candidate["status"] = "unavailable"
            return candidate

        if profile and _candidate_mentions_product(candidate, profile, body_text):
            candidate["mentions_product"] = True

        deleted_signals = (
            "has been deleted",
            "this post has been deleted",
            "page not found",
        )
        removed_signals = (
            "been removed",
            "this post was removed",
            "removed by moderators",
        )
        locked_signals = (
            "comments are locked",
            "locked: the community is archived",
        )
        archived_signals = (
            "this post is archived",
            "this thread is archived",
            "new comments cannot be posted",
        )

        if any(s in text_lower for s in deleted_signals):
            candidate["status"] = "deleted"
            candidate["deleted"] = True
        elif any(s in text_lower for s in removed_signals):
            candidate["status"] = "removed"
            candidate["removed"] = True
        elif any(s in text_lower for s in locked_signals):
            candidate["status"] = "locked"
            candidate["locked"] = True
        elif any(s in text_lower for s in archived_signals):
            candidate["status"] = "archived"
            candidate["archived"] = True
        else:
            candidate["status"] = "active"

        try:
            metadata = await page.evaluate("""() => {
                const post = document.querySelector(
                    'shreddit-post, [data-testid="post-container"], article'
                );
                const title =
                    post?.getAttribute?.('post-title') ||
                    document.querySelector('h1')?.innerText ||
                    '';
                const body =
                    post?.getAttribute?.('content') ||
                    post?.querySelector?.('[slot="text-body"], [data-post-click-location="text-body"]')?.innerText ||
                    '';
                const scoreText =
                    post?.getAttribute?.('score') ||
                    post?.querySelector?.('[id*="vote-arrows"], [aria-label*="upvote" i]')?.innerText ||
                    '';
                const commentCountText =
                    post?.getAttribute?.('comment-count') ||
                    post?.querySelector?.('a[href*="/comments/"]')?.innerText ||
                    '';
                return { title, body, scoreText, commentCountText };
            }""")
            meta_title = (metadata.get("title") or "").strip()
            meta_body = (metadata.get("body") or "").strip()
            if meta_title and (not candidate.get("title") or len(candidate.get("title", "")) < 15):
                candidate["title"] = meta_title[:250]
            if meta_body and not candidate.get("body"):
                candidate["body"] = meta_body[:1000]
            parsed_score = _parse_compact_count(metadata.get("scoreText"))
            if parsed_score is not None and candidate.get("score") is None:
                candidate["score"] = parsed_score
            parsed_comments = _parse_compact_count(metadata.get("commentCountText"))
            if parsed_comments is not None and candidate.get("comment_count") is None:
                candidate["comment_count"] = parsed_comments
        except Exception:
            pass

        # Enrich title from page title if still missing
        if not candidate.get("title") or len(candidate.get("title", "")) < 5:
            clean_title = (
                page_title
                .replace(" : reddit", "")
                .replace(" | Reddit", "")
                .replace(" - Reddit", "")
                .strip()
            )
            if clean_title:
                candidate["title"] = clean_title

    except Exception as exc:
        logger.debug("verify_error", url=url, error=str(exc))
        candidate["status"] = "unavailable"

    return candidate


def _build_google_queries(profile: dict) -> list[str]:
    """Queries to use with the Google site:reddit.com search (without the prefix)."""
    keywords: list[str] = profile.get("keywords_list", [])
    pain_points: list[str] = profile.get("pain_points_list", [])
    use_cases: list[str] = profile.get("use_cases_list", [])
    competitors: list[str] = profile.get("competitors_list", [])
    profile_queries = generate_search_queries(profile)

    raw: list[str] = []
    raw.extend(profile.get("search_queries_list", []) or [])

    for pp in pain_points[:4]:
        raw.append(f"how to solve {pp}")
        for kw in keywords[:2]:
            raw.append(f'"{pp}" "{kw}"')

    for uc in use_cases[:3]:
        raw.append(f"tool for {uc}")
        for kw in keywords[:2]:
            raw.append(f'"{uc}" "{kw}"')

    for comp in competitors[:4]:
        raw.append(f"alternative to {comp}")
        raw.append(f"{comp} alternative")

    for kw in keywords[:4]:
        raw.append(f"{kw} tool recommendation")
        raw.append(f"best {kw} software")

    if keywords:
        raw.append(f'"looking for" "tool" {keywords[0]}')
        raw.append(f'"recommend" "software" {keywords[0]}')

    raw.extend(q for q in profile_queries if "alternative" in q.lower())
    raw.extend(q for q in profile_queries if "recommend" in q.lower())
    raw.extend(q for q in profile_queries if "best " in q.lower())

    seen: set[str] = set()
    result: list[str] = []
    for q in raw:
        q = q.strip()
        if q and q.lower() not in seen:
            seen.add(q.lower())
            result.append(q)

    return result


# ---------------------------------------------------------------------------
# Main discovery function
# ---------------------------------------------------------------------------

async def discover_opportunities(
    page,
    product_name: str,
    product_description: str,
    target_customer: str,
    pain_points: str,
    use_cases: str,
    keywords: str,
    competitor_names: str = "",
    excluded_subreddits: str = "",
    target_link_count: int = 100,
    max_age_days: int = 730,
    recent_days: int = 7,
) -> dict:
    """
    Discover Reddit posts/comments where the SaaS can be naturally promoted.

    Returns categorized, verified, scored results plus a coverage report.
    This is a read-only discovery tool. Never auto-posts, auto-comments, or auto-votes.
    """
    target_link_count = _coerce_int(target_link_count, default=100, minimum=1, maximum=200)
    max_age_days = _coerce_int(max_age_days, default=730, minimum=1, maximum=3650)
    recent_days = _coerce_int(recent_days, default=7, minimum=1, maximum=max_age_days)

    profile: dict = build_opportunity_profile(
        product_name=product_name,
        product_description=product_description,
        target_customer=target_customer,
        pain_points=pain_points,
        use_cases=use_cases,
        keywords=keywords,
        competitor_names=competitor_names,
        excluded_subreddits=excluded_subreddits,
        max_age_days=max_age_days,
    )

    queries = generate_search_queries(profile)
    google_queries = _build_google_queries(profile)

    coverage: dict = {
        "queries_tried": [],
        "subreddits_scanned": [],
        "pages_searched": 0,
        "candidates_found": 0,
        "candidates_rejected": 0,
        "verified_results_returned": 0,
        "rejection_reasons": {},
        "search_modes_used": [],
        "blocked_indicators": [],
    }

    all_candidates: list[dict] = []

    # ---- Phase 1: Authenticated browser-backed Reddit search ----
    reddit_queries = queries[:15]
    coverage["search_modes_used"].append("authenticated_search")
    auth_blocked_count = 0
    auth_hard_blocked = False

    for idx, q in enumerate(reddit_queries):
        coverage["queries_tried"].append(f"reddit:{q}")
        posts, block_reason = await _authenticated_reddit_search(
            page, q, result_type="link", sort="relevance"
        )
        all_candidates.extend(posts)
        coverage["pages_searched"] += 1

        if block_reason:
            if block_reason not in coverage["blocked_indicators"]:
                coverage["blocked_indicators"].append(block_reason)
            auth_blocked_count += 1
            if _is_hard_block_reason(block_reason):
                auth_hard_blocked = True
                break

        # Search for comments on the first few queries
        if idx < 5:
            coverage["queries_tried"].append(f"reddit_comments:{q}")
            comments, comment_block = await _authenticated_reddit_search(
                page, q, result_type="comment", sort="relevance", max_pages=2
            )
            all_candidates.extend(comments)
            coverage["pages_searched"] += 1
            if comment_block and comment_block not in coverage["blocked_indicators"]:
                coverage["blocked_indicators"].append(comment_block)
            if comment_block:
                auth_blocked_count += 1
                if _is_hard_block_reason(comment_block):
                    auth_hard_blocked = True
                    break

        await asyncio.sleep(0.8)

    # ---- Fallback: manual search if authenticated search was blocked or produced no usable DOM results ----
    if reddit_queries and len(all_candidates) == 0:
        coverage["search_modes_used"].append("manual_fallback")
        logger.info(
            "authenticated_search_empty_or_blocked",
            blocked_count=auth_blocked_count,
            hard_blocked=auth_hard_blocked,
            switching_to="manual_fallback",
        )
        for q in reddit_queries[:5]:
            coverage["queries_tried"].append(f"manual:{q}")
            manual_results = await _manual_reddit_search(
                page, q, result_type="link", sort="relevance", max_results=10
            )
            all_candidates.extend(manual_results)
            coverage["pages_searched"] += 1
            if len(all_candidates) >= target_link_count:
                break
            await asyncio.sleep(2.0)

    # ---- Phase 2: Google site:reddit.com search (best-effort, secondary) ----
    for gq in google_queries[:8]:
        coverage["queries_tried"].append(f"google:{gq}")
        g_results = await _google_search(page, gq)
        all_candidates.extend(g_results)
        coverage["pages_searched"] += 1
        await asyncio.sleep(1.2)

    # ---- Mark excluded subreddits ----
    excluded_set: set[str] = profile["excluded_subreddits"]
    for c in all_candidates:
        if c.get("subreddit", "").lower() in excluded_set:
            c["excluded"] = True

    # ---- Deduplicate ----
    all_candidates = deduplicate_candidates(all_candidates)
    coverage["candidates_found"] = len(all_candidates)

    # ---- Pre-filter and score (no browser) ----
    accepted: list[dict] = []
    for c in all_candidates:
        reason = _reject_reason(c, profile)
        if reason:
            coverage["candidates_rejected"] += 1
            coverage["rejection_reasons"][reason] = (
                coverage["rejection_reasons"].get(reason, 0) + 1
            )
        else:
            accepted.append(c)

    # Sort by combined relevance + confidence descending
    accepted.sort(
        key=lambda c: sum(_score_candidate(c, profile)),
        reverse=True,
    )

    # ---- Phase 3: Browser verification (top candidates only) ----
    verify_limit = min(len(accepted), target_link_count + 25)
    verified: list[dict] = []

    for c in accepted[:verify_limit]:
        vc = await _verify_candidate(page, c, profile)
        reason = _reject_reason(vc, profile)
        if reason:
            r = reason
            coverage["candidates_rejected"] += 1
            coverage["rejection_reasons"][r] = coverage["rejection_reasons"].get(r, 0) + 1
        else:
            verified.append(vc)
        await asyncio.sleep(0.2)

    # ---- Categorize and build final output ----
    recent_list: list[dict] = []
    engagement_list: list[dict] = []
    google_list: list[dict] = []
    seen_urls: set[str] = set()

    for c in verified:
        url_key = c.get("url", "").rstrip("/").lower()
        if url_key in seen_urls:
            continue
        seen_urls.add(url_key)

        category = _assign_category(c, recent_days)
        result = _build_result(c, profile, category)

        if category == "recent":
            recent_list.append(result)
        elif category == "high_google_search":
            google_list.append(result)
        else:
            engagement_list.append(result)

        total_so_far = len(recent_list) + len(engagement_list) + len(google_list)
        if total_so_far >= target_link_count:
            break

    total = len(recent_list) + len(engagement_list) + len(google_list)
    coverage["verified_results_returned"] = total
    coverage["subreddits_scanned"] = sorted(
        {c.get("subreddit", "") for c in all_candidates if c.get("subreddit")}
    )

    return {
        "recent_posts_comments": recent_list,
        "high_engagement_posts_comments": engagement_list,
        "high_google_search_posts_comments": google_list,
        "coverage_report": coverage,
        "promotion_guidance": (
            "DISCOVERY ONLY — do not auto-post. "
            "Before engaging: (1) read subreddit rules, "
            "(2) only reply if the product genuinely helps, "
            "(3) disclose affiliation if asked, "
            "(4) never copy-paste the same reply across threads."
        ),
    }
