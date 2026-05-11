"""
tools/opportunity_pipeline.py — Staged backend Reddit opportunity discovery.

Pipeline stages:
    1. Initial Reddit fetch    — light-weight URL/metadata only, per opportunity type
    2. Match filter            — keyword/relevance/dedupe BEFORE expensive detail fetch
    3. Full content fetch      — body + top comments only for filtered survivors
    4. LLM evaluation          — final fit judgement per enriched post
    5. Categorize and emit     — recent / high_engagement / high_google_search buckets

This module talks to Reddit via tools.reddit_api_client (HTTP only). It does
not require a Playwright page and is meant to be invoked directly by the
agent's tool wrapper so the user does not wait on a visual UI flow.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import structlog

from tools.opportunity_discovery_tool import (
    BUYING_INTENT_PHRASES,
    DEFAULT_OPPORTUNITY_TYPES,
    HIGH_ENGAGEMENT_MIN_COMMENTS,
    HIGH_ENGAGEMENT_MIN_SCORE,
    SAAS_SUBREDDITS,
    _build_reason,
    _candidate_age_days,
    _clean_reddit_url,
    _has_negative_product_context,
    _has_product_discovery_context,
    _matched_pain_point,
    _normalize_space,
    _parse_compact_count,
    _reddit_url_key,
    _score_candidate,
    _suggested_angle,
    _term_in_text,
    build_opportunity_profile,
    generate_search_queries,
    normalize_link_kind,
    normalize_opportunity_types,
)
from tools.reddit_api_client import (
    build_async_client,
    fetch_post_detail,
    search_posts,
)
from tools.reddit_session_pool import (
    DEFAULT_GLOBAL_DETAIL_CONCURRENCY,
    DEFAULT_PER_SESSION_CONCURRENCY,
    DEFAULT_RATE_WINDOW_SECONDS,
    DEFAULT_REQUESTS_PER_WINDOW,
    SessionClientPool,
    load_session_pool_files,
    run_parallel_detail_fetch,
)

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_QUERIES_PER_TYPE = 8
DEFAULT_PAGES_PER_QUERY = 1
DEFAULT_LISTING_LIMIT = 50
DEFAULT_TOP_COMMENTS = 5
DEFAULT_MAX_DETAIL_FETCHES = 0
DEFAULT_QUERY_SLEEP = 0.6
DEFAULT_DETAIL_SLEEP = 0.2
DEFAULT_TITLE_RELEVANCE_MIN_HITS = 1
DEFAULT_RECENT_DAYS = 7
DEFAULT_GOOGLE_PROXY_MIN_SCORE = 100
DEFAULT_GOOGLE_PROXY_MIN_COMMENTS = 25
DEFAULT_STAGE2_PROBE_MIN = 20
DEFAULT_STAGE2_PROBE_MAX = 80
DEFAULT_STAGE2_PROBE_MULTIPLIER = 3
DEFAULT_REJECTED_LINK_LIMIT = 100

OPPORTUNITY_TYPE_CONFIG: dict[str, dict[str, object]] = {
    "recent": {"time_filter": "week", "sort": "new"},
    "high_engagement": {"time_filter": "year", "sort": "top"},
    "high_google_search": {"time_filter": "all", "sort": "top"},
}

REDDIT_API_SOURCE = "reddit_api_search"
HIGH_GOOGLE_PROXY_SOURCE = "high_google_search_proxy"


@dataclass
class PipelineConfig:
    queries_per_type: int = DEFAULT_QUERIES_PER_TYPE
    pages_per_query: int = DEFAULT_PAGES_PER_QUERY
    listing_limit: int = DEFAULT_LISTING_LIMIT
    top_comments: int = DEFAULT_TOP_COMMENTS
    max_detail_fetches: int = DEFAULT_MAX_DETAIL_FETCHES
    query_sleep: float = DEFAULT_QUERY_SLEEP
    detail_sleep: float = DEFAULT_DETAIL_SLEEP
    title_relevance_min_hits: int = DEFAULT_TITLE_RELEVANCE_MIN_HITS
    recent_days: int = DEFAULT_RECENT_DAYS
    target_link_count: int = 100
    max_age_days: int = 730
    use_proxy: bool = False
    extra_keep_relevant_terms: tuple[str, ...] = ()
    seen_url_keys: frozenset[str] = field(default_factory=frozenset)
    google_proxy_min_score: int = DEFAULT_GOOGLE_PROXY_MIN_SCORE
    google_proxy_min_comments: int = DEFAULT_GOOGLE_PROXY_MIN_COMMENTS
    stage2_probe_min: int = DEFAULT_STAGE2_PROBE_MIN
    stage2_probe_max: int = DEFAULT_STAGE2_PROBE_MAX
    stage2_probe_multiplier: int = DEFAULT_STAGE2_PROBE_MULTIPLIER
    rejected_link_limit: int = DEFAULT_REJECTED_LINK_LIMIT
    # Multi-session Stage 3 settings
    session_files: tuple[str, ...] = ()
    max_sessions: int = 0
    requests_per_window: int = DEFAULT_REQUESTS_PER_WINDOW
    rate_window_seconds: float = DEFAULT_RATE_WINDOW_SECONDS
    per_session_concurrency: int = DEFAULT_PER_SESSION_CONCURRENCY
    global_detail_concurrency: int = DEFAULT_GLOBAL_DETAIL_CONCURRENCY


def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return ""


def _env_int(names: tuple[str, ...], default: int, minimum: int, maximum: int) -> int:
    raw = _first_env(*names)
    if not raw:
        return default
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(value, maximum))


def _env_float(names: tuple[str, ...], default: float, minimum: float, maximum: float) -> float:
    raw = _first_env(*names)
    if not raw:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(value, maximum))


def _env_list(names: tuple[str, ...]) -> tuple[str, ...]:
    raw = _first_env(*names)
    if not raw:
        return ()
    parts = re.split(r"[,\n;]+", raw)
    return tuple(part.strip().strip('"').strip("'") for part in parts if part.strip())


def build_pipeline_config_from_env(
    target_link_count: int = 100,
    max_age_days: int = 730,
    recent_days: int = 7,
) -> PipelineConfig:
    """Build PipelineConfig from optional OPPORTUNITY/REDDIT_OPPORTUNITY env vars.

    The multi-session pool remains opt-in. Set either
    REDDIT_OPPORTUNITY_SESSION_FILES or REDDIT_OPPORTUNITY_MAX_SESSIONS to use it.
    """
    safe_target = max(1, min(int(target_link_count or 100), 200))
    safe_max_age = max(1, int(max_age_days or 730))
    safe_recent = max(1, min(int(recent_days or 7), safe_max_age))
    return PipelineConfig(
        queries_per_type=_env_int(
            ("REDDIT_OPPORTUNITY_QUERIES_PER_TYPE", "OPPORTUNITY_QUERIES_PER_TYPE"),
            DEFAULT_QUERIES_PER_TYPE,
            1,
            50,
        ),
        pages_per_query=_env_int(
            ("REDDIT_OPPORTUNITY_PAGES_PER_QUERY", "OPPORTUNITY_PAGES_PER_QUERY"),
            DEFAULT_PAGES_PER_QUERY,
            1,
            10,
        ),
        listing_limit=_env_int(
            ("REDDIT_OPPORTUNITY_LISTING_LIMIT", "OPPORTUNITY_LISTING_LIMIT"),
            DEFAULT_LISTING_LIMIT,
            1,
            100,
        ),
        top_comments=_env_int(
            ("REDDIT_OPPORTUNITY_TOP_COMMENTS", "OPPORTUNITY_TOP_COMMENTS"),
            DEFAULT_TOP_COMMENTS,
            0,
            20,
        ),
        max_detail_fetches=_env_int(
            ("REDDIT_OPPORTUNITY_MAX_DETAIL_FETCHES", "OPPORTUNITY_MAX_DETAIL_FETCHES"),
            DEFAULT_MAX_DETAIL_FETCHES,
            0,
            5000,
        ),
        query_sleep=_env_float(
            ("REDDIT_OPPORTUNITY_QUERY_SLEEP", "OPPORTUNITY_QUERY_SLEEP"),
            DEFAULT_QUERY_SLEEP,
            0.0,
            30.0,
        ),
        detail_sleep=_env_float(
            ("REDDIT_OPPORTUNITY_DETAIL_SLEEP", "OPPORTUNITY_DETAIL_SLEEP"),
            DEFAULT_DETAIL_SLEEP,
            0.0,
            30.0,
        ),
        target_link_count=safe_target,
        max_age_days=safe_max_age,
        recent_days=safe_recent,
        use_proxy=_first_env("REDDIT_OPPORTUNITY_USE_PROXY", "OPPORTUNITY_USE_PROXY").lower()
        in {"1", "true", "yes", "on"},
        google_proxy_min_score=_env_int(
            ("REDDIT_OPPORTUNITY_GOOGLE_PROXY_MIN_SCORE", "OPPORTUNITY_GOOGLE_PROXY_MIN_SCORE"),
            DEFAULT_GOOGLE_PROXY_MIN_SCORE,
            0,
            100000,
        ),
        google_proxy_min_comments=_env_int(
            (
                "REDDIT_OPPORTUNITY_GOOGLE_PROXY_MIN_COMMENTS",
                "OPPORTUNITY_GOOGLE_PROXY_MIN_COMMENTS",
            ),
            DEFAULT_GOOGLE_PROXY_MIN_COMMENTS,
            0,
            100000,
        ),
        stage2_probe_min=_env_int(
            ("REDDIT_OPPORTUNITY_STAGE2_PROBE_MIN", "OPPORTUNITY_STAGE2_PROBE_MIN"),
            DEFAULT_STAGE2_PROBE_MIN,
            0,
            500,
        ),
        stage2_probe_max=_env_int(
            ("REDDIT_OPPORTUNITY_STAGE2_PROBE_MAX", "OPPORTUNITY_STAGE2_PROBE_MAX"),
            DEFAULT_STAGE2_PROBE_MAX,
            0,
            1000,
        ),
        stage2_probe_multiplier=_env_int(
            (
                "REDDIT_OPPORTUNITY_STAGE2_PROBE_MULTIPLIER",
                "OPPORTUNITY_STAGE2_PROBE_MULTIPLIER",
            ),
            DEFAULT_STAGE2_PROBE_MULTIPLIER,
            0,
            20,
        ),
        rejected_link_limit=_env_int(
            ("REDDIT_OPPORTUNITY_REJECTED_LINK_LIMIT", "OPPORTUNITY_REJECTED_LINK_LIMIT"),
            DEFAULT_REJECTED_LINK_LIMIT,
            0,
            5000,
        ),
        session_files=_env_list(
            ("REDDIT_OPPORTUNITY_SESSION_FILES", "OPPORTUNITY_SESSION_FILES")
        ),
        max_sessions=_env_int(
            ("REDDIT_OPPORTUNITY_MAX_SESSIONS", "OPPORTUNITY_MAX_SESSIONS"),
            0,
            0,
            100,
        ),
        requests_per_window=_env_int(
            ("REDDIT_OPPORTUNITY_REQUESTS_PER_WINDOW", "OPPORTUNITY_REQUESTS_PER_WINDOW"),
            DEFAULT_REQUESTS_PER_WINDOW,
            1,
            1000,
        ),
        rate_window_seconds=_env_float(
            ("REDDIT_OPPORTUNITY_RATE_WINDOW_SECONDS", "OPPORTUNITY_RATE_WINDOW_SECONDS"),
            DEFAULT_RATE_WINDOW_SECONDS,
            1.0,
            3600.0,
        ),
        per_session_concurrency=_env_int(
            ("REDDIT_OPPORTUNITY_PER_SESSION_CONCURRENCY", "OPPORTUNITY_PER_SESSION_CONCURRENCY"),
            DEFAULT_PER_SESSION_CONCURRENCY,
            1,
            20,
        ),
        global_detail_concurrency=_env_int(
            ("REDDIT_OPPORTUNITY_GLOBAL_DETAIL_CONCURRENCY", "OPPORTUNITY_GLOBAL_DETAIL_CONCURRENCY"),
            DEFAULT_GLOBAL_DETAIL_CONCURRENCY,
            1,
            200,
        ),
    )


# ---------------------------------------------------------------------------
# Stage 2 helpers — match-filter layer (operates on title + listing meta only)
# ---------------------------------------------------------------------------

PROBE_TITLE_TERMS: tuple[str, ...] = (
    "saas",
    "startup",
    "business",
    "marketing",
    "sales",
    "outreach",
    "lead",
    "crm",
    "campaign",
    "tool",
    "software",
    "api",
    "automation",
    "integration",
    "workflow",
    "recommend",
    "alternative",
    "compare",
    "help",
    "problem",
    "issue",
    "pain",
    "stack",
    "platform",
)

PROBE_SUBREDDIT_TERMS: tuple[str, ...] = (
    "saas",
    "startup",
    "business",
    "marketing",
    "sales",
    "email",
    "coldemail",
    "ecommerce",
    "entrepreneur",
    "freelance",
    "productivity",
    "automation",
    "micro_saas",
    "b2b",
    "leadgen",
    "smallbusiness",
)

PROBE_BAD_SUBREDDIT_TERMS: tuple[str, ...] = (
    "gonewild",
    "bdsm",
    "findom",
    "sex",
    "roleplay",
    "cuck",
    "personals",
    "nsfw",
)


def _title_keyword_hits(title: str, profile: dict, extra_terms: tuple[str, ...] = ()) -> int:
    text = _normalize_space(title)
    if not text:
        return 0
    hits = 0
    seen: set[str] = set()
    sources: list[list[str]] = [
        profile.get("keywords_list", []) or [],
        profile.get("pain_points_list", []) or [],
        profile.get("use_cases_list", []) or [],
        profile.get("competitors_list", []) or [],
        list(extra_terms or ()),
    ]
    for terms in sources:
        for term in terms:
            term_norm = _normalize_space(term)
            if not term_norm or term_norm in seen:
                continue
            if _term_in_text(term, text):
                hits += 1
                seen.add(term_norm)
    return hits


def _probe_title_hits(title: str, profile: Optional[dict] = None) -> int:
    text = _normalize_space(title)
    hits = sum(1 for term in PROBE_TITLE_TERMS if term in text)
    if profile:
        dynamic_terms: list[str] = []
        dynamic_terms.extend(profile.get("keywords_list", []) or [])
        dynamic_terms.extend(profile.get("pain_points_list", []) or [])
        dynamic_terms.extend(profile.get("use_cases_list", []) or [])
        hits += sum(1 for term in dynamic_terms[:20] if _term_in_text(term, text))
    return hits


def _probe_subreddit_score(subreddit: object) -> int:
    sub = _normalize_space(subreddit)
    if not sub:
        return 0
    if any(term in sub for term in PROBE_BAD_SUBREDDIT_TERMS):
        return -3
    if sub in SAAS_SUBREDDITS:
        return 3
    if any(term in sub for term in PROBE_SUBREDDIT_TERMS):
        return 2
    if sub.startswith("u_"):
        return -1
    return 0


def _opportunity_type_matches(post: dict) -> tuple[str, ...]:
    raw = post.get("opportunity_type_matches")
    values: list[str] = []
    if isinstance(raw, (list, tuple, set)):
        values.extend(str(item) for item in raw if item)
    elif raw:
        values.extend(part.strip() for part in re.split(r"[,\s]+", str(raw)) if part.strip())
    requested = post.get("opportunity_type")
    if requested:
        values.append(str(requested))

    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in OPPORTUNITY_TYPE_CONFIG and value not in seen:
            seen.add(value)
            out.append(value)
    return tuple(out)


def _has_high_engagement_meta(post: dict) -> bool:
    score = _parse_compact_count(post.get("score") or post.get("upvotes")) or 0
    comments = _parse_compact_count(post.get("comment_count")) or 0
    return score >= HIGH_ENGAGEMENT_MIN_SCORE or comments >= HIGH_ENGAGEMENT_MIN_COMMENTS


def _has_google_proxy_match(post: dict) -> bool:
    source_detail = str(post.get("source_detail") or "")
    return (
        post.get("source") == HIGH_GOOGLE_PROXY_SOURCE
        or "high_google_search" in _opportunity_type_matches(post)
        or "reddit_top_all_proxy" in source_detail
    )


def _has_actual_google_rank(post: dict) -> bool:
    source = str(post.get("source") or "")
    return source == "google_search" or post.get("google_rank") not in (None, "")


def _has_google_proxy_authority(post: dict, config: Optional[PipelineConfig] = None) -> bool:
    if not _has_google_proxy_match(post):
        return False
    cfg = config or PipelineConfig()
    score = _parse_compact_count(post.get("score") or post.get("upvotes")) or 0
    comments = _parse_compact_count(post.get("comment_count")) or 0
    return score >= cfg.google_proxy_min_score or comments >= cfg.google_proxy_min_comments


def _stage2_probe_priority(
    post: dict,
    reason: str,
    profile: dict,
    config: PipelineConfig,
) -> Optional[tuple]:
    if reason != "title_off_topic":
        return None
    if post.get("listing_inactive_reason"):
        return None
    if str(post.get("subreddit") or "").lower() in (profile.get("excluded_subreddits") or set()):
        return None
    age_reason = _age_reject_reason(post, config)
    if age_reason:
        return None

    title = str(post.get("title") or "")
    matched_query = str(post.get("matched_query") or "")
    query_hits = _title_keyword_hits(matched_query, profile, config.extra_keep_relevant_terms)
    title_probe_hits = _probe_title_hits(title, profile)
    sub_score = _probe_subreddit_score(post.get("subreddit"))
    if sub_score < 0:
        return None

    score = _parse_compact_count(post.get("score") or post.get("upvotes")) or 0
    comments = _parse_compact_count(post.get("comment_count")) or 0
    has_probe_signal = sub_score > 0 or title_probe_hits >= 2
    if query_hits <= 0 or not has_probe_signal:
        return None

    age = _candidate_age_days(post, default=9999.0)
    return (
        query_hits,
        sub_score,
        title_probe_hits,
        min(comments, 100),
        min(score, 1000),
        -age,
    )


def _stage2_probe_limit(config: PipelineConfig, strict_count: int, rejected_count: int) -> int:
    if rejected_count <= 0:
        return 0
    desired = max(
        int(config.stage2_probe_min or 0),
        int(config.target_link_count or 0) * max(0, int(config.stage2_probe_multiplier or 0)),
    )
    cap = int(config.stage2_probe_max or 0)
    if cap > 0:
        desired = min(desired, cap)
    desired = max(0, desired - strict_count)
    return min(rejected_count, desired)


def _select_stage2_detail_probes(
    rejected: list[tuple[dict, str]],
    profile: dict,
    config: PipelineConfig,
    strict_count: int,
) -> list[dict]:
    limit = _stage2_probe_limit(config, strict_count, len(rejected))
    if limit <= 0:
        return []
    scored: list[tuple[tuple, dict]] = []
    for post, reason in rejected:
        priority = _stage2_probe_priority(post, reason, profile, config)
        if priority is not None:
            scored.append((priority, post))
    scored.sort(key=lambda item: item[0], reverse=True)
    probes: list[dict] = []
    for _, post in scored[:limit]:
        post["stage2_probe_reason"] = "query_context_probe"
        probes.append(post)
    return probes


def _detail_fetch_limit(survivor_count: int, config: PipelineConfig) -> int:
    """Return how many Stage 2 survivors may receive detail fetches.

    max_detail_fetches <= 0 means no artificial cap: keep walking the survivor
    queue until target results are found or survivors are exhausted.
    """
    if survivor_count <= 0:
        return 0
    try:
        configured = int(config.max_detail_fetches or 0)
    except (TypeError, ValueError):
        configured = 0
    if configured <= 0:
        return survivor_count
    return min(survivor_count, configured)


def _listing_priority(post: dict, profile: dict, config: PipelineConfig) -> tuple:
    title = str(post.get("title") or "")
    title_hits = _title_keyword_hits(title, profile, config.extra_keep_relevant_terms)
    score = _parse_compact_count(post.get("score") or post.get("upvotes")) or 0
    comments = _parse_compact_count(post.get("comment_count")) or 0
    age = _candidate_age_days(post, default=9999.0)
    type_rank = {
        "recent": 3,
        "high_google_search": 2,
        "high_engagement": 1,
    }.get(str(post.get("opportunity_type") or ""), 0)
    return (
        title_hits,
        _probe_title_hits(title, profile),
        type_rank,
        min(comments, 500),
        min(score, 5000),
        -age,
    )


def _age_reject_reason(post: dict, config: PipelineConfig) -> str:
    has_age = post.get("age_days") not in (None, "") or post.get("created_utc") not in (None, "")
    if not has_age:
        return ""
    age = _candidate_age_days(post, default=9999.0)
    if age > config.max_age_days:
        return "too_old"
    matches = set(_opportunity_type_matches(post))
    if matches == {"recent"} and age > config.recent_days:
        return "outside_recent_window"
    return ""


def title_passes_match_filter(
    post: dict,
    profile: dict,
    config: PipelineConfig,
) -> tuple[bool, str]:
    """Return (keep, reject_reason). Cheap title-level relevance gate."""
    if post.get("listing_inactive_reason"):
        return False, post["listing_inactive_reason"]
    title = str(post.get("title") or "").strip()
    if len(title) < 8:
        return False, "title_too_short"
    excluded: set[str] = profile.get("excluded_subreddits") or set()
    if post.get("subreddit", "").lower() in excluded:
        return False, "excluded_subreddit"
    age_reason = _age_reject_reason(post, config)
    if age_reason:
        return False, age_reason

    matches = set(_opportunity_type_matches(post))
    if "high_engagement" in matches and _has_high_engagement_meta(post):
        # High-engagement bucket allows weaker title match — keep regardless.
        return True, ""
    if "high_google_search" in matches:
        # Google-ranked proxy — Reddit "top all-time" listings, also weak title gate.
        if _title_keyword_hits(title, profile, config.extra_keep_relevant_terms):
            return True, ""
        return False, "title_off_topic"

    hits = _title_keyword_hits(title, profile, config.extra_keep_relevant_terms)
    if hits >= max(1, config.title_relevance_min_hits):
        return True, ""

    return False, "title_off_topic"


# ---------------------------------------------------------------------------
# Stage 1 — initial Reddit fetch
# ---------------------------------------------------------------------------

def _recent_time_filter(recent_days: int) -> str:
    days = max(1, int(recent_days or DEFAULT_RECENT_DAYS))
    if days <= 1:
        return "day"
    if days <= 7:
        return "week"
    if days <= 31:
        return "month"
    if days <= 365:
        return "year"
    return "all"


def _search_spec_for_type(opportunity_type: str, config: PipelineConfig) -> dict[str, object]:
    spec = dict(OPPORTUNITY_TYPE_CONFIG[opportunity_type])
    if opportunity_type == "recent":
        spec["time_filter"] = _recent_time_filter(config.recent_days)
    return spec


def _remember_search_mode(coverage: dict, mode: str) -> None:
    modes = coverage.setdefault("search_modes_used", [])
    if mode not in modes:
        modes.append(mode)


async def _fetch_listings_for_type(
    client,
    queries: list[str],
    opportunity_type: str,
    config: PipelineConfig,
    coverage: dict,
) -> list[dict]:
    spec = _search_spec_for_type(opportunity_type, config)
    out: list[dict] = []
    used_queries = queries[: config.queries_per_type]
    time_filter = str(spec.get("time_filter", "week"))
    sort = str(spec.get("sort", "new"))
    _remember_search_mode(coverage, f"reddit_api_search:{opportunity_type}:{sort}:t={time_filter}")
    for q in used_queries:
        coverage["queries_tried"].append(f"{opportunity_type}:{q}")
        posts, stats = await search_posts(
            client,
            query=q,
            time_filter=time_filter,
            sort=sort,
            limit=config.listing_limit,
            max_pages=config.pages_per_query,
            sleep_seconds=config.query_sleep,
        )
        coverage["search_stats"].append(stats)
        coverage["pages_searched"] = coverage.get("pages_searched", 0) + int(stats.get("pages") or 0)
        coverage["candidates_seen"] = coverage.get("candidates_seen", 0) + int(stats.get("children") or 0)
        if stats.get("stopped_reason") and stats["stopped_reason"] != "completed":
            coverage["blocked_indicators"].append(stats["stopped_reason"])
        for post in posts:
            post["opportunity_type"] = opportunity_type
            post["opportunity_type_matches"] = [opportunity_type]
            if opportunity_type == "high_google_search":
                post["source"] = HIGH_GOOGLE_PROXY_SOURCE
                post["source_detail"] = "reddit_top_all_proxy"
            if post.get("source_detail"):
                post["source_details"] = [str(post["source_detail"])]
            if post.get("matched_query"):
                post["matched_queries"] = [str(post["matched_query"])]
            out.append(post)
        if config.query_sleep:
            await asyncio.sleep(config.query_sleep)
    logger.info(
        "opportunity_pipeline_stage1",
        opportunity_type=opportunity_type,
        queries=len(used_queries),
        posts_fetched=len(out),
    )
    return out


def _append_unique(values: list[str], new_values: object) -> list[str]:
    out = list(values or [])
    if isinstance(new_values, (list, tuple, set)):
        candidates = [str(value) for value in new_values if value]
    elif new_values:
        candidates = [str(new_values)]
    else:
        candidates = []
    seen = set(out)
    for value in candidates:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _merge_listing(existing: dict, incoming: dict) -> None:
    existing["opportunity_type_matches"] = _append_unique(
        list(_opportunity_type_matches(existing)),
        _opportunity_type_matches(incoming),
    )
    existing["source_details"] = _append_unique(
        existing.get("source_details", []),
        incoming.get("source_details") or incoming.get("source_detail"),
    )
    if existing.get("source_details"):
        existing["source_detail"] = ",".join(existing["source_details"])
    existing["matched_queries"] = _append_unique(
        existing.get("matched_queries", []),
        incoming.get("matched_queries") or incoming.get("matched_query"),
    )
    if not existing.get("matched_query") and incoming.get("matched_query"):
        existing["matched_query"] = incoming["matched_query"]
    for key in ("score", "upvotes", "comment_count"):
        current = _parse_compact_count(existing.get(key)) or 0
        new_value = _parse_compact_count(incoming.get(key)) or 0
        if new_value > current:
            existing[key] = incoming.get(key)
    for key in ("title", "subreddit", "created_date", "created_utc", "age_days"):
        if existing.get(key) in (None, "") and incoming.get(key) not in (None, ""):
            existing[key] = incoming[key]
    if incoming.get("source") == HIGH_GOOGLE_PROXY_SOURCE and not _has_actual_google_rank(existing):
        existing["source"] = HIGH_GOOGLE_PROXY_SOURCE
    if "high_google_search" in existing["opportunity_type_matches"]:
        existing["source_details"] = _append_unique(
            existing.get("source_details", []),
            "reddit_top_all_proxy",
        )
        existing["source_detail"] = ",".join(existing["source_details"])


def _dedupe_listings(posts: list[dict], seen_keys: set[str]) -> list[dict]:
    out: list[dict] = []
    by_key: dict[str, dict] = {}
    for p in posts:
        key = _reddit_url_key(p.get("url", ""))
        if not key:
            continue
        if key in by_key:
            _merge_listing(by_key[key], p)
            continue
        if key in seen_keys:
            continue
        seen_keys.add(key)
        cleaned = _clean_reddit_url(p.get("url", ""))
        if cleaned:
            p["url"] = cleaned
        p["opportunity_type_matches"] = list(_opportunity_type_matches(p))
        by_key[key] = p
        out.append(p)
    return out


# ---------------------------------------------------------------------------
# Stage 4 — LLM evaluation
# ---------------------------------------------------------------------------

LLMReviewFn = Callable[[dict, dict], Awaitable[dict]]


def _scoring_view(candidate: dict) -> dict:
    view = dict(candidate)
    source = view.get("source")
    if (
        source == HIGH_GOOGLE_PROXY_SOURCE
        or view.get("opportunity_type") == "high_google_search"
        or "high_google_search" in _opportunity_type_matches(view)
    ):
        view["source"] = "google_search"
    elif source == REDDIT_API_SOURCE:
        view["source"] = "reddit_authenticated_search"
    return view


def _top_comments_context(top_comments: object) -> str:
    if not isinstance(top_comments, list):
        return ""
    bodies: list[str] = []
    for comment in top_comments:
        if isinstance(comment, dict):
            body = str(comment.get("body") or "").strip()
        else:
            body = str(comment or "").strip()
        if body:
            bodies.append(body)
    return " ".join(bodies)


def _candidate_content_context(candidate: dict) -> str:
    parts: list[str] = []
    for key in ("title", "post_body", "body"):
        value = str(candidate.get(key) or "").strip()
        if value and value not in parts:
            parts.append(value)
    comments = _top_comments_context(candidate.get("top_comments"))
    if comments:
        parts.append(comments)
    return " ".join(parts)


def _term_hits(terms: list[str], text: str, limit: int = 8) -> list[str]:
    hits: list[str] = []
    seen: set[str] = set()
    for term in terms or []:
        term_norm = _normalize_space(term)
        if not term_norm or term_norm in seen:
            continue
        if _term_in_text(term, text):
            hits.append(str(term))
            seen.add(term_norm)
        if len(hits) >= limit:
            break
    return hits


def _candidate_signal_text(candidate: dict) -> str:
    return _normalize_space(
        " ".join(
            part
            for part in (
                _candidate_content_context(candidate),
                str(candidate.get("matched_query") or ""),
                str(candidate.get("subreddit") or ""),
            )
            if part
        )
    )


def _matched_signal_terms(candidate: dict, profile: dict) -> dict:
    text = _candidate_signal_text(candidate)
    return {
        "keywords": _term_hits(profile.get("keywords_list", []) or [], text),
        "pain_points": _term_hits(profile.get("pain_points_list", []) or [], text),
        "use_cases": _term_hits(profile.get("use_cases_list", []) or [], text),
        "competitors": _term_hits(profile.get("competitors_list", []) or [], text),
    }


def _buying_intent_hits(candidate: dict) -> list[str]:
    text = _candidate_signal_text(candidate)
    hits: list[str] = []
    for phrase in BUYING_INTENT_PHRASES:
        phrase_norm = _normalize_space(phrase)
        if phrase_norm and phrase_norm in text:
            hits.append(phrase)
        if len(hits) >= 6:
            break
    return hits


def _engagement_level(score: int, comments: int) -> str:
    if score >= 250 or comments >= 75:
        return "very_high"
    if score >= 75 or comments >= 25:
        return "high"
    if score >= 15 or comments >= 8:
        return "moderate"
    return "low"


def _review_fit_rank(review: dict) -> int:
    return {"strong": 3, "medium": 2, "weak": 1, "reject": 0}.get(
        str((review or {}).get("fit") or "").lower(),
        0,
    )


def _review_risk_rank(review: dict) -> int:
    return {"low": 2, "medium": 1, "high": 0}.get(
        str((review or {}).get("promotion_risk") or "").lower(),
        1,
    )


def _credibility_score(candidate: dict, profile: dict, review: Optional[dict] = None) -> int:
    scoring_candidate = _scoring_view(candidate)
    relevance, confidence = _score_candidate(scoring_candidate, profile)
    score = _parse_compact_count(candidate.get("score")) or 0
    comments = _parse_compact_count(candidate.get("comment_count")) or 0
    matched = _matched_signal_terms(candidate, profile)
    matched_count = sum(len(values) for values in matched.values())
    intent_bonus = 8 if _buying_intent_hits(candidate) else 0
    comment_text = _top_comments_context(candidate.get("top_comments"))
    comment_context_bonus = 5 if comment_text and any(
        _term_in_text(term, comment_text)
        for values in matched.values()
        for term in values
    ) else 0
    engagement_bonus = min(10, int(max(0, score) / 100) + int(max(0, comments) / 10))
    review_bonus = 0
    if review:
        review_bonus = _review_fit_rank(review) * 3 + _review_risk_rank(review) * 2

    return max(
        0,
        min(
            100,
            int(relevance * 0.45)
            + int(confidence * 0.30)
            + min(14, matched_count * 2)
            + intent_bonus
            + comment_context_bonus
            + engagement_bonus
            + review_bonus,
        ),
    )


def _final_result_priority(post: dict, profile: dict) -> tuple:
    review = post.get("_review") or {}
    score = _parse_compact_count(post.get("score")) or 0
    comments = _parse_compact_count(post.get("comment_count")) or 0
    age = _candidate_age_days(post, default=9999.0)
    relevance, confidence = _score_candidate(_scoring_view(post), profile)
    return (
        _credibility_score(post, profile, review),
        _review_fit_rank(review),
        _review_risk_rank(review),
        relevance,
        confidence,
        min(comments, 500),
        min(score, 5000),
        -age,
    )


def _stage4_rejection_label(review: dict) -> str:
    if not review.get("is_opportunity"):
        return "not_opportunity"
    if review.get("fit") == "reject":
        return "fit_reject"
    if review.get("promotion_risk") == "high":
        return "high_promotion_risk"
    return "llm_rejected"


def _build_llm_review_payload(candidate: dict, profile: dict) -> dict:
    scoring_candidate = _scoring_view(candidate)
    relevance, confidence = _score_candidate(scoring_candidate, profile)
    score = _parse_compact_count(candidate.get("score")) or 0
    comments = _parse_compact_count(candidate.get("comment_count")) or 0
    matched = _matched_signal_terms(candidate, profile)
    return {
        "field_definitions": {
            "score": (
                "Reddit post score/net upvotes. Treat it as visibility/social proof, "
                "not proof of product fit."
            ),
            "comment_count": (
                "Number of Reddit comments. Higher values suggest active discussion "
                "and more chances to find intent in the thread."
            ),
            "top_comments.score": "Reddit net score/upvotes for that comment.",
            "promotion_risk": (
                "Risk that replying would feel spammy, off-topic, against subreddit "
                "norms, or unwelcome in the thread."
            ),
        },
        "review_rubric": {
            "approve_when": [
                "The post or comments ask for tools, recommendations, alternatives, or practical help.",
                "The thread discusses a pain point the product directly solves.",
                "A helpful educational answer could naturally mention the product after giving useful context.",
                "Competitors are discussed and a comparison or alternative angle would be useful.",
            ],
            "reject_when": [
                "The thread matches the product owner's negative/avoid context terms.",
                "The thread is only a meme, rant, news link, or self-promotion showcase.",
                "The product would require forcing an unrelated sales pitch.",
                "The visible context already mentions the same product.",
            ],
            "fit_scale": {
                "strong": "explicit tool/recommendation/alternative request or competitor comparison",
                "medium": "clear pain point where a product mention can be useful after practical advice",
                "weak": "relevant discussion but reply should be mostly educational and cautious",
                "reject": "not a useful or credible place to mention the product",
            },
        },
        "product": {
            "name": profile.get("product_name", ""),
            "url": profile.get("product_url", ""),
            "description": profile.get("product_description", ""),
            "target_customer": profile.get("target_customer", ""),
            "pain_points": profile.get("pain_points_list", []),
            "use_cases": profile.get("use_cases_list", []),
            "keywords": profile.get("keywords_list", []),
            "competitors": profile.get("competitors_list", []),
            "required_context_terms": profile.get("required_context_terms_list", []),
            "negative_keywords": profile.get("negative_keywords_list", []),
        },
        "candidate": {
            "url": candidate.get("url", ""),
            "subreddit": candidate.get("subreddit", ""),
            "title": candidate.get("title", ""),
            "post_body": (candidate.get("post_body") or "")[:3000],
            "combined_context_preview": _candidate_content_context(candidate)[:4500],
            "score": candidate.get("score"),
            "score_meaning": "Reddit net post score/upvotes; higher means visibility, not guaranteed fit.",
            "comment_count": candidate.get("comment_count"),
            "created_date": candidate.get("created_date", ""),
            "matched_query": candidate.get("matched_query", ""),
            "matched_queries": candidate.get("matched_queries", []),
            "opportunity_type": candidate.get("opportunity_type", ""),
            "matched_opportunity_types": list(_opportunity_type_matches(candidate)),
            "source": scoring_candidate.get("source", ""),
            "top_comments": [
                {
                    "body": (c.get("body") or "")[:1000],
                    "score": c.get("score"),
                    "score_meaning": "Reddit net comment score/upvotes.",
                }
                for c in (candidate.get("top_comments") or [])[:5]
            ],
        },
        "evidence": {
            "relevance_score": relevance,
            "confidence_score": confidence,
            "credibility_score": _credibility_score(candidate, profile),
            "matched_terms": matched,
            "buying_intent_phrases": _buying_intent_hits(candidate),
            "matched_pain_point": _matched_pain_point(scoring_candidate, profile),
            "classification_signals": {
                "is_recent_by_age": _candidate_age_days(candidate, default=9999.0)
                <= DEFAULT_RECENT_DAYS,
                "has_high_engagement": _has_high_engagement_meta(candidate),
                "has_google_top_all_proxy": _has_google_proxy_match(candidate),
                "has_actual_google_rank": _has_actual_google_rank(candidate),
            },
            "engagement": {
                "score": score,
                "comment_count": comments,
                "level": _engagement_level(score, comments),
            },
        },
    }


def _coerce_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "approved", "opportunity"}:
        return True
    if text in {"0", "false", "no", "n", "reject", "rejected", "none"}:
        return False
    return default


def _normalized_choice(value: object, default: str, allowed: set[str]) -> str:
    choice = _normalize_space(value)
    return choice if choice in allowed else default


def _heuristic_review(candidate: dict, profile: dict) -> dict:
    scoring_candidate = _scoring_view(candidate)
    rel, conf = _score_candidate(scoring_candidate, profile)
    credibility = _credibility_score(scoring_candidate, profile)
    is_op = rel >= 50 or conf >= 60 or credibility >= 62
    return {
        "is_opportunity": is_op,
        "fit": "medium" if rel >= 60 else ("weak" if is_op else "reject"),
        "promotion_risk": "medium",
        "reason": (
            "Heuristic fallback (no LLM configured). Decision based on title/body keyword "
            f"overlap and engagement evidence (relevance={rel}, confidence={conf}, "
            f"credibility={credibility})."
        ),
        "suggested_angle": _suggested_angle(scoring_candidate, profile),
        "best_action": "manual_review_before_reply",
        "needs_rule_check": True,
        "review_source": "heuristic_fallback",
    }


def _coerce_review(raw: dict, candidate: dict, profile: dict) -> dict:
    if not isinstance(raw, dict):
        return _heuristic_review(candidate, profile)
    fit = _normalized_choice(raw.get("fit"), "weak", {"strong", "medium", "weak", "reject"})
    risk = _normalized_choice(raw.get("promotion_risk"), "medium", {"low", "medium", "high"})
    return {
        "is_opportunity": _coerce_bool(raw.get("is_opportunity"), default=False),
        "fit": fit,
        "promotion_risk": risk,
        "reason": str(raw.get("reason") or ""),
        "suggested_angle": str(raw.get("suggested_angle") or _suggested_angle(_scoring_view(candidate), profile)),
        "best_action": str(raw.get("best_action") or "manual_review_before_reply"),
        "needs_rule_check": _coerce_bool(raw.get("needs_rule_check", True), default=True),
        "review_source": str(raw.get("review_source") or "llm"),
    }


# ---------------------------------------------------------------------------
# Result building
# ---------------------------------------------------------------------------

def _classification_reason(post: dict, category: str, recent_days: int) -> str:
    matches = ",".join(_opportunity_type_matches(post)) or "none"
    score = _parse_compact_count(post.get("score") or post.get("upvotes")) or 0
    comments = _parse_compact_count(post.get("comment_count")) or 0
    age = _candidate_age_days(post, default=9999.0)
    if category == "high_google_search":
        if _has_actual_google_rank(post):
            return f"actual_google_source; matches={matches}"
        return (
            "reddit_top_all_proxy_with_authority"
            f"; score={score}; comments={comments}; age_days={age:.1f}; matches={matches}"
        )
    if category == "recent":
        return f"within_recent_window; age_days={age:.1f}; recent_days={recent_days}; matches={matches}"
    return (
        "engagement_signal"
        f"; score={score}; comments={comments}; thresholds="
        f"{HIGH_ENGAGEMENT_MIN_SCORE}/{HIGH_ENGAGEMENT_MIN_COMMENTS}; matches={matches}"
    )


def _category_for_post(
    post: dict,
    recent_days: int,
    config: Optional[PipelineConfig] = None,
) -> str:
    age = _candidate_age_days(post, default=9999.0)
    if _has_actual_google_rank(post):
        return "high_google_search"
    if age <= recent_days:
        return "recent"
    if _has_google_proxy_authority(post, config):
        return "high_google_search"
    if _has_high_engagement_meta(post):
        return "high_engagement"
    return "high_engagement"


def _build_result_dict(
    post: dict,
    profile: dict,
    category: str,
    review: dict,
    recent_days: int,
) -> dict:
    scoring_post = _scoring_view(post)
    rel, conf = _score_candidate(scoring_post, profile)
    return {
        "url": post.get("url", ""),
        "type": "post",
        "subreddit": post.get("subreddit", ""),
        "title": post.get("title", ""),
        "created_date": post.get("created_date", ""),
        "score": post.get("score"),
        "comment_count": post.get("comment_count"),
        "category": category,
        "matched_opportunity_types": list(_opportunity_type_matches(post)),
        "classification_reason": _classification_reason(post, category, recent_days=recent_days),
        "relevance_score": rel,
        "confidence_score": conf,
        "credibility_score": _credibility_score(post, profile, review),
        "matched_query": post.get("matched_query", ""),
        "matched_pain_point": _matched_pain_point(scoring_post, profile),
        "reason": _build_reason(scoring_post, profile),
        "suggested_angle": review.get("suggested_angle") or _suggested_angle(scoring_post, profile),
        "fit": review.get("fit"),
        "promotion_risk": review.get("promotion_risk"),
        "llm_reason": review.get("reason"),
        "best_action": review.get("best_action"),
        "needs_rule_check": review.get("needs_rule_check"),
        "review_source": review.get("review_source"),
        "status": post.get("status", "active"),
        "source": post.get("source", "reddit_api_search"),
        "source_detail": post.get("source_detail", ""),
        "top_comments": post.get("top_comments") or [],
        "post_body_preview": (post.get("post_body") or "")[:500],
    }


def _bump_reason(counter: dict, reason: object) -> None:
    key = str(reason or "unknown")
    counter[key] = counter.get(key, 0) + 1


def _merged_reason_counts(*counters: dict) -> dict:
    merged: dict[str, int] = {}
    for counter in counters:
        for reason, count in (counter or {}).items():
            merged[str(reason)] = merged.get(str(reason), 0) + int(count or 0)
    return merged


def _record_rejected_candidate(
    coverage: dict,
    candidate: dict,
    stage: str,
    reason: object,
    config: PipelineConfig,
    review: Optional[dict] = None,
) -> None:
    coverage["rejected_candidates_total"] = int(coverage.get("rejected_candidates_total", 0) or 0) + 1
    limit = max(0, int(config.rejected_link_limit or 0))
    entries = coverage.setdefault("rejected_candidates", [])
    if limit <= 0 or len(entries) >= limit:
        coverage["rejected_candidates_truncated"] = (
            int(coverage.get("rejected_candidates_truncated", 0) or 0) + 1
        )
        return

    entry = {
        "stage": stage,
        "reason": str(reason or "unknown"),
        "url": candidate.get("url", ""),
        "title": candidate.get("title", ""),
        "subreddit": candidate.get("subreddit", ""),
        "score": candidate.get("score"),
        "comment_count": candidate.get("comment_count"),
        "created_date": candidate.get("created_date", ""),
        "age_days": candidate.get("age_days"),
        "matched_query": candidate.get("matched_query", ""),
        "matched_queries": candidate.get("matched_queries", []),
        "matched_opportunity_types": list(_opportunity_type_matches(candidate)),
        "source": candidate.get("source", ""),
        "source_detail": candidate.get("source_detail", ""),
    }
    if candidate.get("stage2_probe_reason"):
        entry["stage2_probe_reason"] = candidate.get("stage2_probe_reason")
    if review:
        entry["fit"] = review.get("fit")
        entry["promotion_risk"] = review.get("promotion_risk")
        entry["llm_reason"] = review.get("reason")
        entry["review_source"] = review.get("review_source")
    entries.append(entry)


# ---------------------------------------------------------------------------
# Stage 3 + 4 driver — runs over either a session pool or a single client.
# ---------------------------------------------------------------------------

def _detail_failure_reason(detail: dict) -> str:
    if not detail:
        return "detail_failed"
    if detail.get("detail_status") and detail["detail_status"] != "ok":
        return str(detail["detail_status"])
    if detail.get("detail_inactive_reason"):
        return str(detail["detail_inactive_reason"])
    if detail.get("locked") or detail.get("archived"):
        return "locked_or_archived"
    return "detail_failed"


async def _process_one_detail(
    post: dict,
    detail: dict,
    profile: dict,
    cfg: PipelineConfig,
    coverage: dict,
    approved: list[dict],
    review_fn,
) -> bool:
    """Apply Stage 3 success/failure accounting + Stage 4 LLM review.

    Returns True if pipeline should continue, False if target reached.
    """
    coverage["stage_counts"]["stage3_detail_attempted"] += 1
    if not detail or detail.get("detail_status") != "ok":
        coverage["stage_counts"]["stage3_detail_failed"] += 1
        reason = _detail_failure_reason(detail)
        _bump_reason(coverage["stage3_failure_reasons"], reason)
        _record_rejected_candidate(coverage, post, "stage3_detail", reason, cfg)
        return True
    if detail.get("detail_inactive_reason") or detail.get("locked") or detail.get("archived"):
        coverage["stage_counts"]["stage3_detail_failed"] += 1
        reason = _detail_failure_reason(detail)
        _bump_reason(coverage["stage3_failure_reasons"], reason)
        _record_rejected_candidate(coverage, {**post, **detail}, "stage3_detail", reason, cfg)
        return True
    merged = {**post, **{k: v for k, v in detail.items() if v not in (None, "") and not str(k).startswith("_")}}
    merged["body"] = _candidate_content_context(merged) or post.get("title", "")
    coverage["stage_counts"]["stage3_detail_fetched"] += 1

    context_post = {**merged, "body": _candidate_content_context(merged)}
    if _has_negative_product_context(context_post, profile):
        coverage["stage_counts"]["stage4_llm_rejected"] += 1
        coverage["stage_counts"]["stage4_pre_llm_rejected"] += 1
        _bump_reason(coverage["stage4_rejection_reasons"], "negative_product_context")
        _record_rejected_candidate(coverage, context_post, "stage4_pre_llm", "negative_product_context", cfg)
        return True
    if not _has_product_discovery_context(context_post, profile):
        coverage["stage_counts"]["stage4_llm_rejected"] += 1
        coverage["stage_counts"]["stage4_pre_llm_rejected"] += 1
        _bump_reason(coverage["stage4_rejection_reasons"], "low_product_context")
        _record_rejected_candidate(coverage, context_post, "stage4_pre_llm", "low_product_context", cfg)
        return True

    coverage["llm_reviewed"] += 1
    try:
        raw_review = await review_fn(merged, profile)
    except Exception as exc:
        logger.warning("opportunity_pipeline_llm_error", error=str(exc))
        raw_review = _heuristic_review(merged, profile)
    review = _coerce_review(raw_review, merged, profile)
    merged["_review"] = review
    if (
        not review.get("is_opportunity")
        or review.get("fit") == "reject"
        or review.get("promotion_risk") == "high"
    ):
        coverage["stage_counts"]["stage4_llm_rejected"] += 1
        label = _stage4_rejection_label(review)
        _bump_reason(coverage["stage4_rejection_reasons"], label)
        _record_rejected_candidate(coverage, merged, "stage4_llm", label, cfg, review)
        return True
    approved.append(merged)
    coverage["stage_counts"]["stage4_llm_approved"] += 1
    return len(approved) < cfg.target_link_count


async def _run_stage3_and_stage4(
    survivors: list[dict],
    detail_limit: int,
    cfg: PipelineConfig,
    profile: dict,
    coverage: dict,
    approved: list[dict],
    review_fn,
    single_client,
) -> None:
    """Drive Stage 3 detail fetching + Stage 4 LLM review.

    Uses a multi-session pool when more than one session file is available,
    otherwise falls back to a single-client serial loop preserving today's
    behavior.
    """
    if detail_limit <= 0 or not survivors:
        coverage["stage3_stop_reason"] = coverage.get("stage3_stop_reason") or "exhausted_survivors"
        coverage["stage_counts"]["stage3_detail_skipped"] = max(0, len(survivors) - 0)
        return

    # Multi-session pool is opt-in: caller must provide cfg.session_files OR
    # set cfg.max_sessions explicitly. Default keeps single-client behavior.
    pool_files: list = []
    if cfg.session_files or cfg.max_sessions:
        pool_files = load_session_pool_files(
            session_files=list(cfg.session_files) if cfg.session_files else None,
            max_sessions=cfg.max_sessions,
        )
    use_pool = len(pool_files) > 1

    if not use_pool:
        # Single-session fallback — preserves existing serial behavior.
        attempted = 0
        for post in survivors:
            if len(approved) >= cfg.target_link_count:
                coverage["stage3_stop_reason"] = "target_reached"
                break
            if attempted >= detail_limit:
                coverage["stage3_stop_reason"] = "detail_budget_exhausted"
                break
            attempted += 1
            detail = await fetch_post_detail(
                single_client,
                post_id=post.get("id", ""),
                post_url=post.get("url", ""),
                matched_query=post.get("matched_query", ""),
                top_comments=cfg.top_comments,
            )
            keep_going = await _process_one_detail(
                post, detail or {}, profile, cfg, coverage, approved, review_fn,
            )
            if cfg.detail_sleep:
                await asyncio.sleep(cfg.detail_sleep)
            if not keep_going:
                coverage["stage3_stop_reason"] = "target_reached"
                break
        coverage["stage_counts"]["stage3_session_count"] = 1
        coverage["stage_counts"]["stage3_detail_skipped"] = max(0, len(survivors) - attempted)
        coverage["stage_counts"]["stage3_detail_network_attempted"] = attempted
        coverage["stage_counts"]["stage3_detail_network_fetched"] = coverage["stage_counts"]["stage3_detail_fetched"]
        coverage["stage_counts"]["stage3_detail_network_failed"] = coverage["stage_counts"]["stage3_detail_failed"]
        if not coverage["stage3_stop_reason"]:
            coverage["stage3_stop_reason"] = "exhausted_survivors"
        return

    # Multi-session pool path.
    async with SessionClientPool(
        session_files=pool_files,
        requests_per_window=cfg.requests_per_window,
        rate_window_seconds=cfg.rate_window_seconds,
        use_proxy=cfg.use_proxy,
    ) as pool:
        coverage["stage_counts"]["stage3_session_count"] = pool.session_count
        logger.info(
            "opportunity_pipeline_stage3_pool",
            sessions=pool.session_count,
            survivors=len(survivors),
            detail_limit=detail_limit,
        )

        async def _process(idx: int, post: dict, detail: dict) -> bool:
            return await _process_one_detail(
                post, detail or {}, profile, cfg, coverage, approved, review_fn,
            )

        stats = await run_parallel_detail_fetch(
            survivors=survivors,
            pool=pool,
            top_comments=cfg.top_comments,
            process_in_order=_process,
            detail_limit=detail_limit,
            global_concurrency=cfg.global_detail_concurrency,
            per_session_concurrency=cfg.per_session_concurrency,
        )
        coverage["stage_counts"]["stage3_duplicate_claims_prevented"] = stats.get(
            "duplicate_claims_prevented", 0
        )
        attempted = stats.get("attempted", 0)
        network_attempted = int(stats.get("network_attempted", attempted) or 0)
        coverage["stage_counts"]["stage3_detail_network_attempted"] = network_attempted
        coverage["stage_counts"]["stage3_detail_network_fetched"] = int(stats.get("network_fetched", 0) or 0)
        coverage["stage_counts"]["stage3_detail_network_failed"] = int(stats.get("network_failed", 0) or 0)
        coverage["stage_counts"]["stage3_detail_skipped"] = max(0, len(survivors) - network_attempted)
        if len(approved) >= cfg.target_link_count:
            coverage["stage3_stop_reason"] = "target_reached"
        elif detail_limit < len(survivors) and attempted >= detail_limit:
            coverage["stage3_stop_reason"] = "detail_budget_exhausted"
        else:
            coverage["stage3_stop_reason"] = stats.get("stop_reason") or "exhausted_survivors"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def discover_opportunities_via_api(
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
    opportunity_types: object = None,
    link_kind: object = "posts",
    product_url: str = "",
    product_mention_terms: str = "",
    search_queries: str = "",
    required_context_terms: str = "",
    negative_keywords: str = "",
    llm_review: Optional[LLMReviewFn] = None,
    config: Optional[PipelineConfig] = None,
) -> dict:
    """Run the staged backend Reddit opportunity pipeline."""
    cfg = config or build_pipeline_config_from_env(
        target_link_count=target_link_count,
        max_age_days=max_age_days,
        recent_days=recent_days,
    )

    profile: dict = build_opportunity_profile(
        product_name=product_name,
        product_description=product_description,
        target_customer=target_customer,
        pain_points=pain_points,
        use_cases=use_cases,
        keywords=keywords,
        competitor_names=competitor_names,
        excluded_subreddits=excluded_subreddits,
        max_age_days=cfg.max_age_days,
        product_url=product_url,
        product_mention_terms=product_mention_terms,
        search_queries=search_queries,
        required_context_terms=required_context_terms,
        negative_keywords=negative_keywords,
    )

    selected_types = (
        normalize_opportunity_types(opportunity_types)
        if opportunity_types is not None
        else list(DEFAULT_OPPORTUNITY_TYPES)
    )
    if not selected_types:
        selected_types = list(DEFAULT_OPPORTUNITY_TYPES)
    normalize_link_kind(link_kind)  # validates input; pipeline only emits posts

    queries = generate_search_queries(profile)

    coverage: dict = {
        "selected_types": selected_types,
        "queries_tried": [],
        "search_stats": [],
        "search_modes_used": [],
        "blocked_indicators": [],
        "pages_searched": 0,
        "candidates_found": 0,
        "candidates_seen": 0,
        "candidates_rejected": 0,
        "llm_reviewed": 0,
        "llm_accepted": 0,
        "llm_rejected": 0,
        "rejection_reasons": {},
        "subreddits_scanned": [],
        "stage_counts": {
            "stage1_listings_fetched": 0,
            "stage1_unique_after_dedupe": 0,
            "stage2_strict_kept": 0,
            "stage2_kept": 0,
            "stage2_probe_kept": 0,
            "stage2_rejected": 0,
            "stage3_detail_attempted": 0,
            "stage3_detail_fetched": 0,
            "stage3_detail_failed": 0,
            "stage3_detail_skipped": 0,
            "stage3_detail_network_attempted": 0,
            "stage3_detail_network_fetched": 0,
            "stage3_detail_network_failed": 0,
            "stage3_session_count": 0,
            "stage3_duplicate_claims_prevented": 0,
            "stage4_pre_llm_rejected": 0,
            "stage4_llm_approved": 0,
            "stage4_llm_rejected": 0,
        },
        "stage2_rejection_reasons": {},
        "stage2_probe_reasons": {},
        "stage3_failure_reasons": {},
        "stage3_stop_reason": "",
        "stage4_rejection_reasons": {},
        "rejected_candidates": [],
        "rejected_candidates_total": 0,
        "rejected_candidates_truncated": 0,
    }

    seen_keys: set[str] = set(cfg.seen_url_keys or set())
    review_fn = llm_review or (lambda c, p: _async_heuristic(c, p))
    approved: list[dict] = []

    # ---- Stage 1: lightweight listing fetch per opportunity type ----
    async with build_async_client(use_proxy=cfg.use_proxy) as client:
        all_listings: list[dict] = []
        for opportunity_type in selected_types:
            if opportunity_type not in OPPORTUNITY_TYPE_CONFIG:
                continue
            posts = await _fetch_listings_for_type(client, queries, opportunity_type, cfg, coverage)
            all_listings.extend(posts)

        coverage["stage_counts"]["stage1_listings_fetched"] = len(all_listings)
        unique_listings = _dedupe_listings(all_listings, seen_keys)
        coverage["stage_counts"]["stage1_unique_after_dedupe"] = len(unique_listings)
        coverage["candidates_found"] = len(unique_listings)
        coverage["subreddits_scanned"] = sorted(
            {str(p.get("subreddit") or "") for p in unique_listings if p.get("subreddit")}
        )
        logger.info(
            "opportunity_pipeline_stage1_dedup",
            fetched=len(all_listings),
            unique=len(unique_listings),
        )

        # ---- Stage 2: match filter (no detail fetch) ----
        kept: list[dict] = []
        rejected_stage2: list[tuple[dict, str]] = []
        for post in unique_listings:
            ok, reason = title_passes_match_filter(post, profile, cfg)
            if not ok:
                rejected_stage2.append((post, reason))
                continue
            kept.append(post)
        coverage["stage_counts"]["stage2_strict_kept"] = len(kept)
        probes = _select_stage2_detail_probes(
            rejected_stage2,
            profile,
            cfg,
            strict_count=len(kept),
        )
        probe_keys = {_reddit_url_key(post.get("url", "")) for post in probes}
        if probes:
            kept.extend(probes)
            coverage["stage_counts"]["stage2_probe_kept"] = len(probes)
            _bump_reason(
                coverage["stage2_probe_reasons"],
                f"rescued_for_detail_probe:{len(probes)}",
            )
        for post, reason in rejected_stage2:
            if _reddit_url_key(post.get("url", "")) in probe_keys:
                continue
            coverage["stage_counts"]["stage2_rejected"] += 1
            _bump_reason(coverage["stage2_rejection_reasons"], reason)
            _record_rejected_candidate(coverage, post, "stage2_match_filter", reason, cfg)
        coverage["stage_counts"]["stage2_kept"] = len(kept)
        logger.info(
            "opportunity_pipeline_stage2",
            strict_kept=coverage["stage_counts"]["stage2_strict_kept"],
            kept=len(kept),
            probes=coverage["stage_counts"].get("stage2_probe_kept", 0),
            rejected=coverage["stage_counts"]["stage2_rejected"],
        )

        # Sort by cheap relevance signals first, then engagement. This keeps
        # viral but vague threads from starving more product-specific matches.
        kept.sort(key=lambda p: _listing_priority(p, profile, cfg), reverse=True)

        # ---- Stage 3 + 4: fetch details and review adaptively ----
        detail_limit = _detail_fetch_limit(len(kept), cfg)
        await _run_stage3_and_stage4(
            survivors=kept,
            detail_limit=detail_limit,
            cfg=cfg,
            profile=profile,
            coverage=coverage,
            approved=approved,
            review_fn=review_fn,
            single_client=client,
        )
        if (
            coverage["stage3_stop_reason"] == "detail_budget_exhausted"
            and coverage["stage_counts"]["stage3_detail_skipped"]
        ):
            _bump_reason(coverage["stage3_failure_reasons"], "detail_budget_exhausted")
        logger.info(
            "opportunity_pipeline_stage3",
            detail_attempted=coverage["stage_counts"]["stage3_detail_attempted"],
            detail_fetched=coverage["stage_counts"]["stage3_detail_fetched"],
            detail_failed=coverage["stage_counts"]["stage3_detail_failed"],
            detail_skipped=coverage["stage_counts"]["stage3_detail_skipped"],
            sessions=coverage["stage_counts"]["stage3_session_count"],
            duplicate_claims_prevented=coverage["stage_counts"]["stage3_duplicate_claims_prevented"],
            stop_reason=coverage["stage3_stop_reason"],
        )

    logger.info(
        "opportunity_pipeline_stage4",
        approved=coverage["stage_counts"]["stage4_llm_approved"],
        rejected=coverage["stage_counts"]["stage4_llm_rejected"],
    )

    # ---- Stage 5: categorize ----
    recent_list: list[dict] = []
    engagement_list: list[dict] = []
    google_list: list[dict] = []
    seen_emit: set[str] = set()

    for post in sorted(approved, key=lambda p: _final_result_priority(p, profile), reverse=True):
        url_key = _reddit_url_key(post.get("url", ""))
        if url_key in seen_emit:
            continue
        seen_emit.add(url_key)
        category = _category_for_post(post, cfg.recent_days, cfg)
        result = _build_result_dict(
            post,
            profile,
            category,
            post.get("_review", {}),
            cfg.recent_days,
        )
        if category == "recent":
            recent_list.append(result)
        elif category == "high_google_search":
            google_list.append(result)
        else:
            engagement_list.append(result)
        if len(recent_list) + len(engagement_list) + len(google_list) >= cfg.target_link_count:
            break

    coverage["verified_results_returned"] = (
        len(recent_list) + len(engagement_list) + len(google_list)
    )
    coverage["llm_accepted"] = coverage["stage_counts"]["stage4_llm_approved"]
    coverage["llm_rejected"] = coverage["stage_counts"]["stage4_llm_rejected"]
    skipped_rejected = (
        coverage["stage_counts"]["stage3_detail_skipped"]
        if coverage.get("stage3_stop_reason") == "detail_budget_exhausted"
        else 0
    )
    coverage["candidates_rejected"] = (
        coverage["stage_counts"]["stage2_rejected"]
        + coverage["stage_counts"]["stage3_detail_failed"]
        + skipped_rejected
        + coverage["stage_counts"]["stage4_llm_rejected"]
    )
    coverage["rejection_reasons"] = _merged_reason_counts(
        coverage.get("stage2_rejection_reasons", {}),
        coverage.get("stage3_failure_reasons", {}),
        coverage.get("stage4_rejection_reasons", {}),
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


async def _async_heuristic(candidate: dict, profile: dict) -> dict:
    return _heuristic_review(candidate, profile)


# ---------------------------------------------------------------------------
# Default LLM review adapter (LangChain ChatOpenAI / OpenRouter compatible)
# ---------------------------------------------------------------------------

LLM_REVIEW_SYSTEM = (
    "You review Reddit posts for SaaS promotion discovery. Decide whether mentioning "
    "the product would be genuinely helpful, credible, and rule-aware. Reddit score "
    "means net post upvotes/social visibility; comment_count means discussion depth. "
    "Use those as credibility/reach signals, not as proof of product fit. Approve "
    "when the post or comments show a real problem, tool/recommendation request, "
    "competitor comparison, or practical discussion where the product could be "
    "mentioned after useful advice. Do not require the title to explicitly ask for "
    "a tool if the body/comments reveal a clear pain point. Reject spammy, unrelated, "
    "hostile, meme-only, rant-only, news-only, or already self-promotional threads. "
    "Reject when the body or comments show the discussion is unrelated, off-topic, "
    "or a poor fit even if the title looked relevant. Reject if the visible context "
    "already mentions the product. Use fit=strong for explicit recommendation/tool/"
    "alternative intent, fit=medium for clear product-solvable pain, fit=weak only "
    "when a cautious educational reply could help, and promotion_risk=high when a "
    "reply would likely feel forced or unwelcome. Return only JSON with keys: "
    "is_opportunity boolean, fit strong|medium|weak|reject, promotion_risk "
    "low|medium|high, reason string, suggested_angle string, best_action string, "
    "needs_rule_check boolean."
)


def _extract_json_object(text: str) -> dict:
    if not text:
        return {}
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def make_langchain_llm_review(llm) -> LLMReviewFn:
    """Wrap a LangChain chat model into the pipeline's review callable."""
    from langchain_core.messages import HumanMessage, SystemMessage  # local import

    async def _review(candidate: dict, profile: dict) -> dict:
        payload = _build_llm_review_payload(candidate, profile)
        try:
            response = await llm.ainvoke([
                SystemMessage(content=LLM_REVIEW_SYSTEM),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
            ])
        except Exception as exc:
            logger.warning("opportunity_pipeline_llm_invoke_failed", error=str(exc))
            return _heuristic_review(candidate, profile)
        content = response.content if hasattr(response, "content") else str(response)
        parsed = _extract_json_object(str(content))
        if not parsed:
            return _heuristic_review(candidate, profile)
        parsed["review_source"] = "llm"
        return parsed

    return _review


def build_default_llm_review() -> Optional[LLMReviewFn]:
    """Build a default OpenRouter/ChatOpenAI review function if env is configured."""
    using_openrouter = bool(os.getenv("OPENROUTER_API_KEY"))
    api_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from langchain_openai import ChatOpenAI

        if using_openrouter:
            model = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
            base_url = "https://openrouter.ai/api/v1"
            default_headers = {
                "HTTP-Referer": "https://github.com/redditagent",
                "X-Title": "RedditAgent",
            }
        else:
            model = os.getenv("OPENAI_MODEL") or os.getenv("OPENROUTER_MODEL") or "gpt-4o-mini"
            if "/" in model:
                model = "gpt-4o-mini"
            base_url = "https://api.openai.com/v1"
            default_headers = None

        llm = ChatOpenAI(
            model=model,
            openai_api_key=api_key,
            openai_api_base=base_url,
            temperature=0.2,
            timeout=float(os.getenv("OPENROUTER_TIMEOUT", "60")),
            max_retries=int(os.getenv("OPENROUTER_MAX_RETRIES", "2")),
            default_headers=default_headers,
        )
    except Exception as exc:
        logger.warning("opportunity_pipeline_llm_build_failed", error=str(exc))
        return None
    return make_langchain_llm_review(llm)
