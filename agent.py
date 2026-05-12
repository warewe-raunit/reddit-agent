"""
agent.py — Reddit AI agent using LangGraph ReAct + OpenRouter.
Browser launches lazily — only when a Reddit tool is actually called.
Conversation history persists across turns in the same session.
"""

from __future__ import annotations

import os
import re
import json
import asyncio
from typing import Optional

from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from browser_manager import LazyBrowser
from confirmation import confirmation_reply
from agent_tools import (
    DEFAULT_WARMUP_SUBREDDITS,
    PERSONA_SUBREDDITS,
    comment_on_reddit_post,
    ensure_reddit_logged_in,
    is_opportunity_discovery_request,
    is_reddit_logged_in,
    make_tools,
    open_reddit_home,
    upvote_reddit_comment,
    upvote_reddit_post,
)
from reddit_url_intent import has_comment_intent, has_upvote_intent, reddit_url_points_to_comment
from tools import browse as reddit_browse_tool, reply as reddit_reply_tool
from tools.karma_growth_autopilot import (
    KarmaAutopilotSettings,
    fallback_draft as karma_fallback_draft,
    format_approval_request as format_karma_approval_request,
    is_karma_autopilot_request_text,
    is_karma_continue_command_text,
    is_karma_stop_command_text,
    parse_settings_from_text as parse_karma_settings_from_text,
    ranked_opportunities as rank_karma_opportunities,
    sanitize_public_text as sanitize_karma_public_text,
    score_candidate as score_karma_candidate,
    summarize_context as summarize_karma_context,
    validate_public_text as validate_karma_public_text,
)
from tools.observation_tool import observe_page, summarize_observation
from tools.opportunity_discovery_tool import (
    AGENTIC_DEFAULT_CHUNK_SIZE,
    DEFAULT_LINK_KIND,
    DEFAULT_OPPORTUNITY_TYPES,
    EMAILVERIFIER_PRODUCT_DEFAULTS,
    _authenticated_reddit_search,
    _basic_reject_reason,
    _google_search,
    _is_hard_block_reason,
    _manual_reddit_search,
    _reddit_url_key,
    _score_candidate,
    _verify_candidate,
    build_agentic_search_plan,
    build_opportunity_profile,
    candidate_opportunity_category,
    normalize_link_kind,
    normalize_opportunity_types,
)
from tools.opportunity_pipeline import (
    discover_opportunities_via_api,
    make_langchain_llm_review,
)
from react_loop_guard import (
    is_react_loop_error,
    react_loop_stop_message,
    react_runtime_config,
    react_timeout_message,
    react_timeout_seconds,
)

load_dotenv(override=True)


def _build_llm(model: Optional[str] = None) -> ChatOpenAI:
    return ChatOpenAI(
        model=model or os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001"),
        openai_api_key=os.getenv("OPENROUTER_API_KEY"),
        openai_api_base="https://openrouter.ai/api/v1",
        temperature=0.7,
        timeout=float(os.getenv("OPENROUTER_TIMEOUT", "60")),
        max_retries=int(os.getenv("OPENROUTER_MAX_RETRIES", "2")),
        default_headers={
            "HTTP-Referer": "https://github.com/redditagent",
            "X-Title": "RedditAgent",
        },
    )


SYSTEM_PROMPT = """**You are u/PaceNormal6940** (or the exact username of the authentic Reddit account).

You are a real, experienced Reddit user who has been active for years in tech, SaaS, marketing, startups, entrepreneurship, and productivity communities. You genuinely use and recommend **email verifier . io** — a solid email verification tool that helps clean lists and boost deliverability.

## Core Goals
- Act like a normal, authentic Reddit user at all times.
- Naturally grow karma through helpful comments, thoughtful upvotes, and occasional high-value posts.
- Mention products only when explicitly allowed, genuinely relevant, and helpful — never spammy or forced.
- Protect the long-term reputation and health of this high-karma account.

## Personality & Writing Style
- Write in a **very human, casual, short** Reddit style.
- Use **Reddit slang** naturally (e.g., fr, tbh, ngl, lowkey, big time, solid, fyi, etc.).
- Keep comments **short** (2-5 lines max).
- Sound like a real user: relaxed, helpful, slightly opinionated, and conversational.
- Examples of tone:
  “ngl this saved me so much headache”
  “been using email verifier . io for months, works great fr”
  “yeah this is the move tbh”

## Link & Promotion Rules (Critical)
- Do not use spam-filter evasion, broken domains, forced mentions, or promotional templates.
- Default to no product mentions. Only mention a product when promotion is explicitly allowed and it genuinely answers the thread.
- Do not hyperlink promotional URLs unless the user explicitly asks and the subreddit context clearly permits it.
- Value first. If the helpful answer works without a product mention, leave the product out.

## Observation-First Workflow (Mandatory)
Before **any** state-changing action (upvote, comment, reply, post, join), always call observe_current_page.
After the action, call observe_current_page again to verify.
If anything looks off (URL, DOM, accessibility), stop and report it.

## Standard Action Sequence
1. check_session → verify login.
2. If needed → login_reddit.
3. browse_reddit + warmup_reddit.
4. observe_current_page.
5. Perform action.
6. observe_current_page to confirm.
7. Report outcome.

## Opportunity Discovery
When asked to find opportunities:
- Use the backend staged **Opportunity Finder**.
- Default output: List of promising post links with short reasoning.
- Buckets: **Recent** (≤7 days), **High Engagement**, **Google-ranked**.
- Only relevant posts about email verification, bounces, list cleaning, deliverability.
- Reject anything that already mentions the tool.
- No actions during discovery. Get user approval before any comment/post.

## Karma Growth Autopilot
When asked to run `karma_growth_autopilot`, organic karma autopilot, karma builder, build karma, grow karma, or start building karma:
- This is handled by the direct Python Karma Growth Autopilot workflow before ReAct; do not route it through a separate ReAct tool.
- It may browse Reddit, open posts, read threads, filter, score, skip, rank, and draft without asking approval.
- Use the Karma Growth Autopilot workflow for these requests, not the SaaS Opportunity Finder.
- It must pause before any public text action: comment on a post, reply to a comment, or create a post.
- Before approval, show subreddit, URL, context summary, why it is a good opportunity, exact final text, and risk level.
- If approved: observe_current_page, submit through the existing comment/reply/post tool, observe_current_page again, report the result, then continue if limits allow.
- If rejected: do not submit; mark skipped and keep searching if limits allow.
- Defaults: max_comments_per_run=3, minimum_score=70, promotion_allowed=false, approval required for comments/replies/posts.
- Upvotes are normal community participation signals only; do not treat upvoting as a karma-growth trick or mass-action system.

## Action Rules (Real User Behavior)
- Upvote good content only as normal community participation; upvotes do not directly grow this account's karma.
- Comments must be **short, humanized, and use Reddit slang**.
- Always show the exact comment draft to the user for approval before posting.
- Mix helpful non-promotional comments with occasional soft promotions.
- Never mass-comment or mass-upvote.
- Keep activity natural and spread out.

## Confirmation Gating
- Request confirmation for posts, joining subreddits, or multiple actions.
- Always show final comment/post text before submitting.

## Safety & Realism Rules
- Respect subreddit rules strictly.
- Stay low-profile in strict communities.
- If something feels risky, pause and report.
- Never reply to your own comments or manipulate votes.

## Tool Usage Priorities
- observe_current_page — **mandatory** before/after every action.
- Prefer visible UI clicks.
- Report errors clearly. No excessive retries.

---

**You are not an assistant.**
**You are the Reddit user.** Protect this account’s authenticity while helping the agency grow it smartly."""


def _fallback_models() -> list[str]:
    configured = os.getenv("OPENROUTER_FALLBACK_MODELS", "")
    models = [m.strip() for m in configured.split(",") if m.strip()]
    primary = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
    defaults = ["openai/gpt-4o-mini", "anthropic/claude-3.5-haiku"]
    ordered: list[str] = []
    for model in [primary, *models, *defaults]:
        if model and model not in ordered:
            ordered.append(model)
    return ordered


def _is_transient_llm_error(exc: Exception) -> bool:
    text = str(exc).lower()
    status_code = getattr(exc, "status_code", None)
    return (
        status_code in {408, 429, 500, 502, 503, 504}
        or "504" in text
        or "timeout" in text
        or "aborted" in text
        or "rate limit" in text
    )


def _local_reply(user_message: str) -> Optional[str]:
    msg = user_message.strip().lower()
    compact = re.sub(r"[^a-z0-9\s]", "", msg)

    greetings = {"hi", "hello", "hey", "yo", "hii", "hiya", "namaste"}
    if compact in greetings:
        return "Hi! I am ready. Ask me to log in to Reddit, search for a post, comment, upvote, post, or just chat."

    if compact in {"thanks", "thank you", "ok", "okay"}:
        return "You got it."

    return None


def _is_login_status_question(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    patterns = [
        r"\bam i logged in\b",
        r"\bam i login\b",
        r"\blogin status\b",
        r"\bcheck session\b",
        r"\bcheck login\b",
        r"\bam i connected\b",
    ]
    return any(re.search(pattern, compact) for pattern in patterns)


def _extract_reddit_url(user_message: str) -> Optional[str]:
    match = re.search(r"https?://(?:www\.)?reddit\.com/\S+", user_message)
    if not match:
        return None
    return match.group(0).rstrip(").,]")


def _is_login_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    return bool(re.search(r"\b(log ?in|login)\b", compact)) and "status" not in compact


def _is_open_reddit_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    return "open reddit" in compact or compact in {"reddit", "go to reddit"}


def _is_comment_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    if re.search(r"\bup\s*vote\b|\bupvote\b", compact):
        return False
    return bool(re.search(r"\b(comment|reply)\b", compact)) and "reddit.com" in user_message.lower()


def _is_comment_upvote_request(user_message: str) -> bool:
    if not has_upvote_intent(user_message) or "reddit.com" not in user_message.lower():
        return False
    url = _extract_reddit_url(user_message)
    return has_comment_intent(user_message) or bool(url and reddit_url_points_to_comment(url))


def _is_post_upvote_request(user_message: str) -> bool:
    if not has_upvote_intent(user_message) or "reddit.com" not in user_message.lower():
        return False
    if has_comment_intent(user_message):
        return False
    url = _extract_reddit_url(user_message)
    return bool(url and not reddit_url_points_to_comment(url))


def _is_new_reddit_command_while_pending_comment(user_message: str) -> bool:
    """Return True when a user likely changed tasks instead of providing comment text."""
    if _extract_reddit_url(user_message):
        return True
    if is_opportunity_discovery_request(user_message):
        return True

    compact = re.sub(r"[^a-z0-9\s]", " ", user_message.strip().lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    command_patterns = (
        r"^(login|log in|open reddit|go to reddit|check login|check session)\b",
        r"^(upvote|up vote|join subreddit|warmup|warm up|search reddit|find reddit|browse reddit)\b",
        r"\b(upvote|up vote)\b.*\breddit\b",
    )
    return any(re.search(pattern, compact) for pattern in command_patterns)


def _is_karma_autopilot_request(user_message: str) -> bool:
    return is_karma_autopilot_request_text(user_message)


def _is_karma_continue_command(user_message: str) -> bool:
    return is_karma_continue_command_text(user_message)


def _is_karma_stop_command(user_message: str) -> bool:
    return is_karma_stop_command_text(user_message)


def _subreddit_from_reddit_url(url: str) -> str:
    match = re.search(r"reddit\.com/r/([^/]+)/", str(url or ""), flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _safe_json_preview(value: object, limit: int = 5000) -> str:
    text = json.dumps(value, ensure_ascii=False)
    return text[:limit]


OPPORTUNITY_REQUIRED_FIELDS: tuple[str, ...] = (
    "target_link_count",
    "opportunity_types",
    "link_kind",
)
OPPORTUNITY_TARGET_LINK_FALLBACK = 100

OPPORTUNITY_FIELD_LABELS: dict[str, str] = {
    "productname": "product_name",
    "product": "product_name",
    "name": "product_name",
    "productdescription": "product_description",
    "description": "product_description",
    "targetcustomer": "target_customer",
    "targetcustomers": "target_customer",
    "customer": "target_customer",
    "customers": "target_customer",
    "audience": "target_customer",
    "painpoints": "pain_points",
    "painpoint": "pain_points",
    "problems": "pain_points",
    "usecases": "use_cases",
    "usecase": "use_cases",
    "keywords": "keywords",
    "searchkeywords": "keywords",
    "competitors": "competitor_names",
    "competitor": "competitor_names",
    "competitornames": "competitor_names",
    "excludedsubreddits": "excluded_subreddits",
    "excludesubreddits": "excluded_subreddits",
    "exclude": "excluded_subreddits",
    "targetlinkcount": "target_link_count",
    "numberoflinks": "target_link_count",
    "linksneeded": "target_link_count",
    "links": "target_link_count",
    "count": "target_link_count",
    "linkcount": "target_link_count",
    "targetlinks": "target_link_count",
    "opportunitytypes": "opportunity_types",
    "opportunitytype": "opportunity_types",
    "typeoflinks": "opportunity_types",
    "linktypes": "opportunity_types",
    "linktype": "opportunity_types",
    "categories": "opportunity_types",
    "category": "opportunity_types",
    "postorcomment": "link_kind",
    "postorcomments": "link_kind",
    "postorcommentlinks": "link_kind",
    "postcomment": "link_kind",
    "postcomments": "link_kind",
    "postcommentlinks": "link_kind",
    "postscomments": "link_kind",
    "postscommentslinks": "link_kind",
    "resulttype": "link_kind",
    "resulttypes": "link_kind",
    "urltype": "link_kind",
    "urltypes": "link_kind",
    "kind": "link_kind",
    "maxagedays": "max_age_days",
    "maxage": "max_age_days",
    "recentdays": "recent_days",
    "recent": "recent_days",
    "chunksize": "chunk_size",
    "batchsize": "chunk_size",
    "linksperchunk": "chunk_size",
    "linksperbatch": "chunk_size",
}

OPPORTUNITY_DEFAULTS: dict[str, object] = {
    **EMAILVERIFIER_PRODUCT_DEFAULTS,
    "competitor_names": EMAILVERIFIER_PRODUCT_DEFAULTS["competitor_names"],
    "excluded_subreddits": "",
    "target_link_count": "",
    "opportunity_types": "",
    "link_kind": DEFAULT_LINK_KIND,
    "max_age_days": 730,
    "recent_days": 7,
    "chunk_size": AGENTIC_DEFAULT_CHUNK_SIZE,
}


def _field_label_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]", "", label.lower())


def _parse_opportunity_fields(text: str) -> dict[str, str]:
    """Parse labeled product fields from a normal or pasted CLI message."""
    fields: dict[str, str] = {}
    for line in text.splitlines():
        match = re.match(r"^\s*([A-Za-z][A-Za-z0-9 _/\-.]{1,48})\s*:\s*(.*?)\s*$", line)
        if not match:
            continue
        canonical = OPPORTUNITY_FIELD_LABELS.get(_field_label_key(match.group(1)))
        value = match.group(2).strip()
        if canonical and value:
            fields[canonical] = value
    return fields


def _infer_target_link_count(text: str) -> str:
    compact = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    patterns = (
        r"\b(?:find|get|give|return|show|need|want)\s+(?:me\s+)?(\d{1,3})(?:\s+[a-z]+){0,6}\s*(?:links?|urls?|posts?|comments?|opportunities|leads?)\b",
        r"\b(\d{1,3})(?:\s+[a-z]+){0,6}\s*(?:links?|urls?|posts?|comments?|opportunities|leads?)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, compact)
        if match:
            return match.group(1)
    return ""


def _infer_opportunity_types(text: str) -> str:
    selected = normalize_opportunity_types(text)
    return ", ".join(selected)


def _infer_link_kind(text: str) -> str:
    return normalize_link_kind(text)


def _parse_opportunity_preferences(text: str) -> dict[str, str]:
    """Parse the 3 user inputs for the hardcoded emailverifier.io discovery flow."""
    fields = _parse_opportunity_fields(text)
    if "target_link_count" not in fields:
        count = _infer_target_link_count(text)
        if count:
            fields["target_link_count"] = count
    if "opportunity_types" in fields:
        normalized_types = normalize_opportunity_types(fields["opportunity_types"])
        if normalized_types:
            fields["opportunity_types"] = ", ".join(normalized_types)
        else:
            fields.pop("opportunity_types", None)
    else:
        inferred_types = _infer_opportunity_types(text)
        if inferred_types:
            fields["opportunity_types"] = inferred_types
    if "link_kind" in fields:
        normalized_kind = normalize_link_kind(fields["link_kind"])
        if normalized_kind:
            fields["link_kind"] = normalized_kind
        else:
            fields.pop("link_kind", None)
    else:
        inferred_kind = _infer_link_kind(text)
        if inferred_kind:
            fields["link_kind"] = inferred_kind
    return fields


def _is_opportunity_field_message(text: str) -> bool:
    return bool(_parse_opportunity_preferences(text))


def _is_opportunity_run_command(text: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    return bool(
        re.search(
            r"\b(run|start|go|begin)\b.*\b(discovery|search|opportunit|leads?)\b",
            compact,
        )
        or compact in {"run discovery", "start discovery", "go", "run", "start"}
    )


def _missing_opportunity_fields(profile: dict[str, object]) -> list[str]:
    return [field for field in OPPORTUNITY_REQUIRED_FIELDS if not str(profile.get(field, "")).strip()]


def _display_opportunity_field(field: str) -> str:
    labels = {
        "target_link_count": "Number of links",
        "opportunity_types": "Type of links",
        "link_kind": "Post/comment links",
    }
    if field in labels:
        return labels[field]
    return field.replace("_", " ").title()


def _opportunity_prompt(missing: list[str], ready: bool = False) -> str:
    if ready:
        return (
            "I have the emailverifier.io discovery settings saved.\n"
            "Type `run discovery` when ready, or update Number of links or Type of links."
        )

    needed = "\n".join(f"- {_display_opportunity_field(field)}" for field in missing)
    return (
        "I can run Reddit opportunity discovery for emailverifier.io. Send these settings "
        "one by one, or use CLI `paste` mode and end with `END`:\n"
        f"{needed}\n"
        "Type of links can be: recent, high engagement, Google-ranked, or any combination/all.\n"
        "The backend Opportunity Finder returns post links and includes body + top comments for LLM review. "
        "Recent means at most 7 days old."
    )


def _coerce_discovery_int(value: object, default: int) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _clamp_discovery_int(value: object, default: int, minimum: int, maximum: int) -> int:
    number = _coerce_discovery_int(value, default)
    return max(minimum, min(number, maximum))


def _is_opportunity_continue_command(text: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    return bool(
        compact in {"next", "next 10", "continue", "more", "keep going", "next chunk"}
        or re.search(
            r"^(next|continue|more|keep going)(?:\s+\d+)?(?:\s+(discovery|opportunities|leads|links|chunk|batch))?$",
            compact,
        )
    )


def _is_opportunity_cancel_command(text: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    return compact in {
        "stop discovery",
        "cancel discovery",
        "end discovery",
        "stop opportunities",
        "cancel opportunities",
    }


def _extract_json_object(text: str) -> dict:
    """Best-effort JSON object extraction from an LLM response."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _coerce_review_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "yes", "1"}:
        return True
    if text in {"false", "no", "0"}:
        return False
    return default


def _normalize_opportunity_review(raw: dict) -> dict:
    """Normalize an LLM candidate review into stable fields for output."""
    fit = str(raw.get("fit", "weak")).strip().lower()
    if fit not in {"strong", "medium", "weak", "reject"}:
        fit = "weak"
    risk = str(raw.get("promotion_risk", "medium")).strip().lower()
    if risk not in {"low", "medium", "high"}:
        risk = "medium"
    best_action = str(raw.get("best_action", "read_rules_then_consider_reply")).strip()
    return {
        "is_opportunity": _coerce_review_bool(raw.get("is_opportunity")) and fit != "reject" and risk != "high",
        "fit": fit,
        "promotion_risk": risk,
        "reason": str(raw.get("reason", "")).strip()[:700],
        "suggested_angle": str(raw.get("suggested_angle", "")).strip()[:700],
        "best_action": best_action[:120] or "read_rules_then_consider_reply",
        "needs_rule_check": _coerce_review_bool(raw.get("needs_rule_check"), default=True),
        "review_source": str(raw.get("review_source", "llm")).strip()[:80] or "llm",
    }


def _build_agentic_result(candidate: dict, profile: dict, review: dict) -> dict:
    rel, conf = _score_candidate(candidate, profile)
    return {
        "url": candidate.get("url", ""),
        "type": candidate.get("type", "post"),
        "subreddit": candidate.get("subreddit", ""),
        "title": candidate.get("title", ""),
        "created_date": candidate.get("created_date", "unknown"),
        "score": candidate.get("score"),
        "comment_count": candidate.get("comment_count"),
        "category": candidate.get("category", ""),
        "fit": review.get("fit", "weak"),
        "promotion_risk": review.get("promotion_risk", "medium"),
        "reason": review.get("reason", ""),
        "suggested_angle": review.get("suggested_angle", ""),
        "best_action": review.get("best_action", "read_rules_then_consider_reply"),
        "needs_rule_check": review.get("needs_rule_check", True),
        "status": candidate.get("status", "active"),
        "source": candidate.get("source", "reddit_authenticated_search"),
        "review_source": review.get("review_source", "llm"),
        "relevance_score_signal": rel,
        "confidence_score_signal": conf,
    }


def _opportunity_category_label(category: str) -> str:
    labels = {
        "recent": "Recent",
        "high_engagement": "High Engagement",
        "high_google_search": "Google-Ranked",
        "matched": "Other Matches",
    }
    return labels.get(str(category or "").strip(), str(category or "Other Matches").replace("_", " ").title())


def _opportunity_category_order(session: dict, chunk: list[dict]) -> list[str]:
    selected = normalize_opportunity_types(session.get("opportunity_types", []))
    categories = selected or list(DEFAULT_OPPORTUNITY_TYPES)
    for item in chunk:
        category = item.get("category") or "matched"
        if category not in categories:
            categories.append(category)
    return categories


def _format_agentic_opportunity_chunk(chunk: list[dict], session: dict, exhausted: bool = False) -> str:
    coverage = session.get("coverage", {})
    target = session.get("target_link_count", len(chunk))
    total_found = len(session.get("accepted_results", []))
    chunk_number = session.get("chunk_number", 1)
    header = (
        f"Opportunity discovery chunk {chunk_number}: {len(chunk)} new links "
        f"(total {total_found}/{target}).\n"
        f"Product: emailverifier.io. Buckets: {session.get('opportunity_types_label', 'all')}. "
        f"URLs: {session.get('link_kind', DEFAULT_LINK_KIND)}.\n"
        "Output is grouped by type; counts per type depend on what is discoverable and approved.\n"
        "Read-only: no joins, comments, posts, or votes were performed.\n"
    )
    if not chunk:
        header = (
            "I did not find any new LLM-approved opportunities in this chunk.\n"
            "Read-only: no joins, comments, posts, or votes were performed.\n"
        )

    lines: list[str] = [header]
    category_order = _opportunity_category_order(session, chunk)
    for category in category_order:
        items = [item for item in chunk if (item.get("category") or "matched") == category]
        if not items:
            continue
        lines.append(f"{_opportunity_category_label(category)} ({len(items)})")
        for idx, item in enumerate(items, start=1):
            subreddit = f"r/{item.get('subreddit')}" if item.get("subreddit") else "unknown subreddit"
            lines.extend([
                f"{idx}. [{item.get('fit', 'weak')} fit, {item.get('promotion_risk', 'medium')} risk] {subreddit}",
                f"   Title: {item.get('title', '').strip() or '(no title)'}",
                f"   URL: {item.get('url', '')}",
                f"   Why: {item.get('reason', '').strip() or 'LLM judged this as relevant.'}",
                f"   Angle: {item.get('suggested_angle', '').strip() or 'Add a helpful, non-spammy answer if subreddit rules allow it.'}",
            ])

    lines.append(
        "\nProgress: "
        f"{coverage.get('candidates_seen', 0)} candidates seen, "
        f"{coverage.get('llm_reviewed', 0)} LLM-reviewed, "
        f"{coverage.get('llm_rejected', 0)} rejected by LLM, "
        f"{coverage.get('pages_searched', 0)} search pages opened."
    )
    blocked = coverage.get("blocked_indicators") or []
    if blocked:
        lines.append(f"Blocked/rate-limit indicators noticed: {', '.join(blocked)}.")

    if exhausted or total_found >= _clamp_discovery_int(target, OPPORTUNITY_TARGET_LINK_FALLBACK, 1, 200):
        lines.append("Discovery session complete.")
    else:
        lines.append("Send `next 10` to continue, or `stop discovery` to end this session.")
    return "\n".join(lines)


def _should_run_opportunity_discovery(
    text: str,
    profile: dict[str, object],
    parsed_fields: dict[str, str],
    started_new_request: bool,
) -> bool:
    if _missing_opportunity_fields(profile):
        return False
    if _is_opportunity_run_command(text):
        return True
    if started_new_request and all(field in parsed_fields for field in OPPORTUNITY_REQUIRED_FIELDS):
        return True
    return bool(set(parsed_fields).intersection(OPPORTUNITY_REQUIRED_FIELDS))


def _summarize_opportunity_result(result: dict) -> str:
    coverage = result.get("coverage_report", {})
    recent = result.get("recent_posts_comments", [])
    engagement = result.get("high_engagement_posts_comments", [])
    google = result.get("high_google_search_posts_comments", [])
    returned = len(recent) + len(engagement) + len(google)
    summary = (
        "Opportunity discovery complete. This was read-only: no comments, posts, votes, or joins.\n"
        f"Returned: {returned} links "
        f"({len(recent)} recent, {len(engagement)} high engagement, {len(google)} Google-ranked).\n"
        f"Coverage: {coverage.get('candidates_found', 0)} candidates found, "
        f"{coverage.get('candidates_rejected', 0)} rejected, "
        f"{coverage.get('verified_results_returned', returned)} verified returned.\n"
        f"Search modes: {', '.join(coverage.get('search_modes_used', [])) or 'none'}.\n"
    )
    blocked = coverage.get("blocked_indicators") or []
    if blocked:
        summary += f"Blocked/rate-limit indicators: {', '.join(blocked)}.\n"
    return f"{summary}\nFull JSON:\n{json.dumps(result, indent=2, ensure_ascii=False)}"


class RedditAgent:
    """Persistent agent with lazy browser and conversation history."""

    def __init__(self, account_id: str, username: str, password: str, proxy_url: Optional[str] = None, headless: bool = False):
        self.account_id = account_id
        self.username = username
        self.password = password
        self.lazy = LazyBrowser(account_id, proxy_url, headless)
        self._models = _fallback_models()
        self._model_index = 0
        self._llm = _build_llm(self._models[self._model_index])
        self._history: list[BaseMessage] = []
        self._confirmation_state: dict = {"pending": None, "approved": False}
        self._react_config = react_runtime_config()
        self._react_timeout_seconds = react_timeout_seconds()
        self._tools = make_tools(
            self.lazy,
            account_id,
            username,
            password,
            proxy_url,
            confirmation_state=self._confirmation_state,
        )
        self._agent = create_react_agent(self._llm, self._tools, prompt=SYSTEM_PROMPT)
        self._pending_comment_url: Optional[str] = None
        self._pending_direct_action: Optional[dict[str, str]] = None
        self._pending_karma_action: Optional[dict[str, object]] = None
        self._pending_opportunity_profile: Optional[dict[str, object]] = None
        self._opportunity_session: Optional[dict[str, object]] = None
        self._karma_autopilot_session: Optional[dict[str, object]] = None

    def _disarm_pending_tool_approval(self) -> Optional[dict]:
        pending = self._confirmation_state.get("pending")
        if pending and self._confirmation_state.get("approved"):
            self._confirmation_state["approved"] = False
            return dict(pending)
        return None

    def _pending_tool_still_approved(self, pending: Optional[dict]) -> bool:
        return bool(
            pending
            and self._confirmation_state.get("approved")
            and self._confirmation_state.get("pending") == pending
        )

    def _unconsumed_confirmation_message(self, pending: dict) -> str:
        self._confirmation_state["approved"] = False
        return (
            "I received your confirmation, but the agent did not execute that pending action "
            "within this turn. I reset the approval so it cannot be spent later.\n"
            f"Pending action: {pending.get('action', 'unknown')}\n"
            f"Details: {pending.get('details', '')}\n"
            "Reply 'yes' to try again or 'no' to cancel."
        )

    def _switch_model(self, model_index: int) -> None:
        self._model_index = model_index
        self._llm = _build_llm(self._models[self._model_index])
        self._agent = create_react_agent(self._llm, self._tools, prompt=SYSTEM_PROMPT)

    async def check_login_status(self) -> str:
        page = await self.lazy.get_page()
        logged_in = await is_reddit_logged_in(page, expected_username=self.username)
        if logged_in:
            await self.lazy.persist_session()
            return "Yes, you are logged in to Reddit. I saved the current browser session."
        return "No, I could not detect an active Reddit login in this browser session."

    async def login_reddit(self) -> str:
        ok, status = await ensure_reddit_logged_in(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )
        return status if ok else status

    async def open_reddit(self) -> str:
        return await open_reddit_home(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )

    async def _observe_current_page_summary(self) -> str:
        page = await self.lazy.get_page()
        try:
            obs = await observe_page(page, include_screenshot=False)
            return summarize_observation(obs, include_elements=True)
        except Exception as exc:
            return f"Observation failed: {exc}"

    async def _observe_reddit_url_summary(self, url: str) -> str:
        ok, status = await ensure_reddit_logged_in(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )
        if not ok:
            return f"Observation failed: {status}"
        page = await self.lazy.get_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await self.lazy.persist_session()
            return await self._observe_current_page_summary()
        except Exception as exc:
            return f"Observation failed: {exc}"

    async def _queue_direct_comment_confirmation(self, post_url: str, text: str) -> str:
        self._pending_direct_action = {
            "type": "comment_on_post",
            "post_url": post_url,
            "text": text,
        }
        observation = await self._observe_reddit_url_summary(post_url)
        preview = text if len(text) <= 500 else f"{text[:497]}..."
        return (
            "[CONFIRMATION REQUIRED] I am ready to submit this Reddit comment.\n"
            f"Comment: {preview}\n"
            f"Post URL: {post_url}\n\n"
            f"Current page observation:\n{observation}\n\n"
            "Reply 'yes' to post it or 'no' to cancel."
        )

    async def _execute_pending_direct_action(self) -> str:
        pending = self._pending_direct_action
        self._pending_direct_action = None
        if not pending:
            return "No pending action to confirm."
        if pending.get("type") == "comment_on_post":
            post_url = pending["post_url"]
            before = await self._observe_reddit_url_summary(post_url)
            result = await self.comment_on_post(post_url, pending["text"])
            after = await self._observe_current_page_summary()
            return (
                f"{result}\n\n"
                f"Observation before action:\n{before}\n\n"
                f"Observation after action:\n{after}"
            )
        return "Pending action type is no longer supported, so I canceled it."

    async def _handle_pending_confirmation(self, user_message: str) -> Optional[str]:
        reply = confirmation_reply(user_message)
        if self._pending_karma_action:
            if reply is True:
                return await self._execute_pending_karma_action()
            if reply is False:
                return await self._reject_pending_karma_action()
            return "Please reply 'yes' to submit this autopilot draft or 'no' to skip it."

        if self._pending_direct_action:
            if reply is True:
                return await self._execute_pending_direct_action()
            if reply is False:
                self._pending_direct_action = None
                return "Canceled the pending action."
            return "Please reply 'yes' to proceed or 'no' to cancel the pending action."

        pending_tool_action = self._confirmation_state.get("pending")
        if pending_tool_action:
            if reply is True:
                self._confirmation_state["approved"] = True
                return None
            if reply is False:
                self._confirmation_state["pending"] = None
                self._confirmation_state["approved"] = False
                return "Canceled the pending action."
            return "Please reply 'yes' to proceed or 'no' to cancel the pending action."
        return None

    async def comment_on_post(self, post_url: str, text: str) -> str:
        return await comment_on_reddit_post(
            lazy=self.lazy,
            account_id=self.account_id,
            username=self.username,
            password=self.password,
            post_url=post_url,
            text=text,
            proxy_url=self.lazy.proxy_url,
        )

    async def upvote_comment(self, comment_url: str) -> str:
        before = await self._observe_reddit_url_summary(comment_url)
        result = await upvote_reddit_comment(
            lazy=self.lazy,
            account_id=self.account_id,
            username=self.username,
            password=self.password,
            comment_url=comment_url,
            proxy_url=self.lazy.proxy_url,
        )
        after = await self._observe_current_page_summary()
        return (
            f"{result}\n\n"
            f"Observation before action:\n{before}\n\n"
            f"Observation after action:\n{after}"
        )

    async def upvote_post(self, post_url: str) -> str:
        before = await self._observe_reddit_url_summary(post_url)
        result = await upvote_reddit_post(
            lazy=self.lazy,
            account_id=self.account_id,
            username=self.username,
            password=self.password,
            post_url=post_url,
            proxy_url=self.lazy.proxy_url,
        )
        after = await self._observe_current_page_summary()
        return (
            f"{result}\n\n"
            f"Observation before action:\n{before}\n\n"
            f"Observation after action:\n{after}"
        )

    def _build_karma_autopilot_session(self, settings: KarmaAutopilotSettings) -> dict[str, object]:
        persona_allowed = {name.lower(): name for name in PERSONA_SUBREDDITS}
        blocked = {name.lower() for name in settings.subreddit_blocklist}
        requested = list(settings.subreddit_allowlist) or DEFAULT_WARMUP_SUBREDDITS.copy()
        scan_subreddits: list[str] = []
        for name in requested:
            key = str(name).lower()
            if key in blocked or key not in persona_allowed:
                continue
            canonical = persona_allowed[key]
            if canonical not in scan_subreddits:
                scan_subreddits.append(canonical)

        return {
            "settings": settings,
            "scan_subreddits": scan_subreddits,
            "subreddit_index": 0,
            "pending_candidates": [],
            "seen_listing_urls": set(),
            "seen_action_keys": set(),
            "skipped": [],
            "submitted_count": 0,
            "approval_count": 0,
            "candidates_seen": 0,
            "dry_run_items": [],
            "exhausted": False,
        }

    def _karma_action_key(self, candidate: dict[str, object]) -> str:
        action = str(candidate.get("action") or candidate.get("type") or "comment_on_post")
        post_url = str(candidate.get("post_url") or candidate.get("url") or "")
        comment_fullname = str(candidate.get("comment_fullname") or "")
        return "|".join([action, _reddit_url_key(post_url) or post_url.lower(), comment_fullname])

    def _finish_karma_autopilot(self, reason: str) -> str:
        session = self._karma_autopilot_session
        self._karma_autopilot_session = None
        self._pending_karma_action = None
        if not session:
            return f"Karma Growth Autopilot stopped: {reason}."
        settings: KarmaAutopilotSettings = session["settings"]  # type: ignore[assignment]
        dry_items: list[dict] = session.get("dry_run_items", [])  # type: ignore[assignment]
        skipped: list[str] = session.get("skipped", [])  # type: ignore[assignment]
        lines = [
            f"Karma Growth Autopilot stopped: {reason}.",
            (
                f"Submitted {session.get('submitted_count', 0)}/{settings.max_comments_per_run}; "
                f"approval opportunities shown {session.get('approval_count', 0)}/{settings.max_opportunities_per_run}; "
                f"candidates scanned {session.get('candidates_seen', 0)}."
            ),
        ]
        if settings.dry_run and dry_items:
            lines.append("Dry-run ranked candidates:")
            for idx, item in enumerate(dry_items[: settings.max_opportunities_per_run], start=1):
                lines.append(
                    f"{idx}. r/{item.get('subreddit')} score={item.get('score')} risk={item.get('risk_level')}\n"
                    f"URL: {item.get('comment_url') or item.get('post_url') or item.get('url')}\n"
                    f"Reason: {item.get('reason')}\n"
                    f"Draft:\n{item.get('text')}"
                )
        if skipped:
            lines.append(f"Skipped signals: {skipped[:8]}")
        return "\n\n".join(lines)

    def _append_karma_skip(self, session: dict[str, object], reason: str) -> None:
        skipped: list[str] = session.setdefault("skipped", [])  # type: ignore[assignment]
        if reason and reason not in skipped:
            skipped.append(reason)

    async def _fetch_next_karma_listing_candidates(self, session: dict[str, object], page) -> None:
        pending: list[dict] = session["pending_candidates"]  # type: ignore[assignment]
        if pending or session.get("exhausted"):
            return

        settings: KarmaAutopilotSettings = session["settings"]  # type: ignore[assignment]
        scan_subreddits: list[str] = session["scan_subreddits"]  # type: ignore[assignment]
        seen: set[str] = session["seen_listing_urls"]  # type: ignore[assignment]

        while not pending and int(session["subreddit_index"]) < len(scan_subreddits):
            subreddit = scan_subreddits[int(session["subreddit_index"])]
            session["subreddit_index"] = int(session["subreddit_index"]) + 1
            try:
                await page.goto(f"https://www.reddit.com/r/{subreddit}/new/", wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8_000)
                except Exception:
                    pass
                await reddit_browse_tool(page=page, account_id=self.account_id, mode="simulate_reading")
                found = await page.evaluate("""(subredditName) => {
                    const seen = new Set();
                    const out = [];
                    const posts = [...document.querySelectorAll('shreddit-post, article, [data-testid="post-container"]')];
                    for (const post of posts) {
                        const anchor = post.querySelector('a[href*="/comments/"]');
                        if (!anchor || !anchor.href || seen.has(anchor.href)) continue;
                        seen.add(anchor.href);
                        const text = (post.innerText || '').replace(/\\s+/g, ' ').trim();
                        const title =
                            post.getAttribute('post-title') ||
                            anchor.innerText ||
                            anchor.getAttribute('aria-label') ||
                            text.slice(0, 180);
                        const statusText = text.toLowerCase();
                        out.push({
                            type: 'post',
                            action: 'comment_on_post',
                            subreddit: subredditName,
                            title: (title || '').trim().slice(0, 240),
                            url: anchor.href,
                            post_url: anchor.href,
                            context: text.slice(0, 800),
                            score: post.getAttribute('score') || post.getAttribute('upvote-count') || '',
                            comment_count: post.getAttribute('comment-count') || '',
                            locked: statusText.includes('locked'),
                            removed: statusText.includes('[removed]') || statusText.includes('removed by moderator'),
                            deleted: statusText.includes('[deleted]'),
                            archived: statusText.includes('archived'),
                        });
                        if (out.length >= 10) break;
                    }
                    return out;
                }""", subreddit)
            except Exception as exc:
                self._append_karma_skip(session, f"r/{subreddit}: listing scan failed ({exc})")
                continue

            ranked = rank_karma_opportunities(found, settings)
            for item in ranked:
                key = _reddit_url_key(item.get("post_url") or item.get("url") or "")
                if not key or key in seen:
                    continue
                seen.add(key)
                if item.get("risk_level") == "high":
                    self._append_karma_skip(session, f"r/{item.get('subreddit')}: {item.get('skip_reason') or 'high risk'}")
                    continue
                if int(item.get("score") or 0) < max(40, settings.minimum_score - 25):
                    continue
                pending.append(item)

        if not pending and int(session["subreddit_index"]) >= len(scan_subreddits):
            session["exhausted"] = True

    async def _read_karma_thread_opportunities(self, page, candidate: dict[str, object]) -> list[dict[str, object]]:
        post_url = str(candidate.get("post_url") or candidate.get("url") or "")
        await page.goto(post_url, wait_until="domcontentloaded", timeout=60_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass
        await reddit_browse_tool(page=page, account_id=self.account_id, mode="simulate_reading")
        detail = await page.evaluate("""() => {
            const bodyText = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
            const post = document.querySelector('shreddit-post, article, [data-testid="post-container"]');
            const comments = [...document.querySelectorAll('shreddit-comment')].slice(0, 8).map((comment) => ({
                fullname: comment.getAttribute('thingid') || comment.getAttribute('data-fullname') || '',
                body: (comment.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 800),
                score: comment.getAttribute('score') || '',
                deleted: (comment.innerText || '').toLowerCase().includes('[deleted]'),
                removed: (comment.innerText || '').toLowerCase().includes('[removed]'),
            })).filter((comment) => comment.body);
            return {
                body: post ? (post.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 1500) : bodyText.slice(0, 1500),
                context: bodyText.slice(0, 2200),
                top_comments: comments,
                locked: bodyText.toLowerCase().includes('comments are locked'),
                archived: bodyText.toLowerCase().includes('this post is archived'),
                removed: bodyText.toLowerCase().includes('[removed]'),
                deleted: bodyText.toLowerCase().includes('[deleted]'),
            };
        }""")

        merged = {**candidate, **detail, "post_url": post_url, "url": post_url, "action": "comment_on_post"}
        opportunities: list[dict[str, object]] = [merged]
        for comment_item in detail.get("top_comments", []) or []:
            if not isinstance(comment_item, dict):
                continue
            fullname = str(comment_item.get("fullname") or "")
            body = str(comment_item.get("body") or "").strip()
            if not fullname.startswith("t1_") or len(body) < 35:
                continue
            if comment_item.get("deleted") or comment_item.get("removed"):
                continue
            comment_id = fullname.replace("t1_", "", 1)
            opportunities.append({
                **merged,
                "type": "comment",
                "action": "reply_to_reddit_comment",
                "comment_fullname": fullname,
                "comment_url": f"{post_url.rstrip('/')}/{comment_id}/",
                "parent_comment_text": body,
                "score": comment_item.get("score") or merged.get("score"),
            })
        return opportunities

    async def _draft_karma_public_text(
        self,
        candidate: dict[str, object],
        settings: KarmaAutopilotSettings,
    ) -> Optional[dict[str, object]]:
        action = str(candidate.get("action") or "comment_on_post")
        payload = {
            "action": action,
            "subreddit": candidate.get("subreddit", ""),
            "title": candidate.get("title", ""),
            "post_body_or_context": str(candidate.get("body") or candidate.get("context") or "")[:1800],
            "parent_comment_text": str(candidate.get("parent_comment_text") or "")[:900],
            "score": candidate.get("score"),
            "comment_count": candidate.get("comment_count"),
            "current_reason": candidate.get("reason", ""),
            "risk_level": candidate.get("risk_level", ""),
            "promotion_allowed": settings.promotion_allowed,
        }
        system = (
            "Draft one authentic Reddit public-text action for organic karma participation. "
            "Return only JSON with keys: action comment_on_post|reply_to_reddit_comment|skip, "
            "text string, context_summary string, reason string, risk_level low|medium|high. "
            "Rules: 2-5 lines max, casual Reddit tone, helpful first, no fake personal claims, "
            "no repeated template feel, no spam-filter evasion, no links, no product mention unless "
            "promotion_allowed is true and the product is directly relevant. Prefer no promotion."
        )
        parsed: dict = {}
        try:
            response = await self._llm.ainvoke([
                SystemMessage(content=system),
                HumanMessage(content=_safe_json_preview(payload)),
            ])
            content = response.content if hasattr(response, "content") else str(response)
            parsed = _extract_json_object(str(content))
        except Exception:
            parsed = {}

        parsed_action = str(parsed.get("action") or action).strip()
        if parsed_action == "skip":
            self._append_karma_skip(
                self._karma_autopilot_session or {},
                f"r/{candidate.get('subreddit')}: LLM skipped ({parsed.get('reason', 'no reason')})",
            )
            return None

        draft = sanitize_karma_public_text(str(parsed.get("text") or ""), promotion_allowed=settings.promotion_allowed)
        if not draft:
            draft = karma_fallback_draft(candidate, promotion_allowed=settings.promotion_allowed)

        issues = validate_karma_public_text(draft, promotion_allowed=settings.promotion_allowed)
        if issues:
            fallback = karma_fallback_draft(candidate, promotion_allowed=settings.promotion_allowed)
            fallback_issues = validate_karma_public_text(fallback, promotion_allowed=settings.promotion_allowed)
            if fallback_issues:
                self._append_karma_skip(
                    self._karma_autopilot_session or {},
                    f"r/{candidate.get('subreddit')}: draft rejected ({', '.join(issues)})",
                )
                return None
            draft = fallback

        risk = str(parsed.get("risk_level") or candidate.get("risk_level") or "medium").lower()
        if risk not in {"low", "medium", "high"}:
            risk = str(candidate.get("risk_level") or "medium")
        if risk == "high":
            self._append_karma_skip(self._karma_autopilot_session or {}, f"r/{candidate.get('subreddit')}: draft risk high")
            return None

        reason = str(parsed.get("reason") or candidate.get("reason") or "High-fit, low-risk chance to add something useful.")
        context_summary = str(parsed.get("context_summary") or summarize_karma_context(candidate))
        return {
            **candidate,
            "action": action,
            "text": draft,
            "reason": reason,
            "risk_level": risk,
            "context_summary": context_summary,
        }

    def _queue_karma_approval(self, opportunity: dict[str, object]) -> str:
        self._pending_karma_action = dict(opportunity)
        return format_karma_approval_request(opportunity)

    async def reply_to_comment(self, comment_fullname: str, post_url: str, text: str) -> str:
        ok, status = await ensure_reddit_logged_in(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )
        if not ok:
            return status
        subreddit = _subreddit_from_reddit_url(post_url)
        allowed = {name.lower() for name in PERSONA_SUBREDDITS}
        if not subreddit or subreddit.lower() not in allowed:
            return f"Reply blocked: r/{subreddit or 'unknown'} is not in the persona-matched subreddit allowlist."
        page = await self.lazy.get_page()
        result = await reddit_reply_tool(
            page=page,
            account_id=self.account_id,
            comment_fullname=comment_fullname,
            post_url=post_url,
            text=text,
        )
        if result["success"]:
            await self.lazy.persist_session()
            return f"Reply posted. Data: {result['data']}"
        return f"Reply failed: {result['error']}"

    async def _execute_pending_karma_action(self) -> str:
        pending = self._pending_karma_action
        self._pending_karma_action = None
        if not pending:
            return "No pending Karma Growth Autopilot action to confirm."

        action = str(pending.get("action") or "")
        post_url = str(pending.get("post_url") or pending.get("url") or "")
        text = str(pending.get("text") or "")
        before = await self._observe_reddit_url_summary(post_url)
        if action == "reply_to_reddit_comment":
            result = await self.reply_to_comment(
                comment_fullname=str(pending.get("comment_fullname") or ""),
                post_url=post_url,
                text=text,
            )
        else:
            result = await self.comment_on_post(post_url, text)
        after = await self._observe_current_page_summary()

        session = self._karma_autopilot_session
        success_text = result.lower()
        if session is not None and ("posted" in success_text or "submitted" in success_text) and "failed" not in success_text:
            session["submitted_count"] = int(session.get("submitted_count", 0)) + 1

        report = (
            f"{result}\n\n"
            f"Observation before action:\n{before}\n\n"
            f"Observation after action:\n{after}"
        )
        if session is None:
            return report
        settings: KarmaAutopilotSettings = session["settings"]  # type: ignore[assignment]
        if int(session.get("submitted_count", 0)) >= settings.max_comments_per_run:
            return report + "\n\n" + self._finish_karma_autopilot("target comment limit reached")
        return report + "\n\n" + await self._continue_karma_growth_autopilot()

    async def _reject_pending_karma_action(self) -> str:
        pending = self._pending_karma_action
        self._pending_karma_action = None
        if not pending:
            return "No pending Karma Growth Autopilot action to skip."
        session = self._karma_autopilot_session
        skipped_message = (
            f"Skipped r/{pending.get('subreddit', 'unknown')} "
            f"{pending.get('comment_url') or pending.get('post_url') or pending.get('url')}"
        )
        if session is None:
            return skipped_message
        self._append_karma_skip(session, skipped_message)
        return skipped_message + "\n\n" + await self._continue_karma_growth_autopilot()

    async def _continue_karma_growth_autopilot(self) -> str:
        session = self._karma_autopilot_session
        if session is None:
            return "There is no active Karma Growth Autopilot session. Start one with `karma_growth_autopilot`."

        settings: KarmaAutopilotSettings = session["settings"]  # type: ignore[assignment]
        if not session.get("scan_subreddits"):
            return self._finish_karma_autopilot(
                "no eligible persona-matched subreddits after allowlist/blocklist filtering"
            )
        if not settings.dry_run and settings.max_comments_per_run <= 0:
            return self._finish_karma_autopilot("max_comments_per_run is 0")
        if not settings.dry_run and int(session.get("submitted_count", 0)) >= settings.max_comments_per_run:
            return self._finish_karma_autopilot("target comment limit reached")
        if int(session.get("approval_count", 0)) >= settings.max_opportunities_per_run:
            return self._finish_karma_autopilot("max opportunities reached")

        ok, status = await ensure_reddit_logged_in(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )
        if not ok:
            return status

        page = await self.lazy.get_page()
        candidate_budget = max(settings.max_opportunities_per_run * 12, 20)

        while int(session.get("candidates_seen", 0)) < candidate_budget:
            await self._fetch_next_karma_listing_candidates(session, page)
            pending_candidates: list[dict] = session["pending_candidates"]  # type: ignore[assignment]
            if not pending_candidates:
                if settings.dry_run:
                    return self._finish_karma_autopilot("dry run complete")
                return self._finish_karma_autopilot("no more candidates above threshold")

            listing_candidate = pending_candidates.pop(0)
            session["candidates_seen"] = int(session.get("candidates_seen", 0)) + 1
            try:
                thread_opportunities = await self._read_karma_thread_opportunities(page, listing_candidate)
            except Exception as exc:
                self._append_karma_skip(
                    session,
                    f"r/{listing_candidate.get('subreddit', 'unknown')}: thread read failed ({exc})",
                )
                continue

            ranked = rank_karma_opportunities(thread_opportunities, settings)
            for raw in ranked:
                key = self._karma_action_key(raw)
                seen_actions: set[str] = session["seen_action_keys"]  # type: ignore[assignment]
                if key in seen_actions:
                    continue
                seen_actions.add(key)

                review = score_karma_candidate(raw, settings)
                if not review.get("is_high_fit"):
                    if review.get("skip_reason"):
                        self._append_karma_skip(session, f"r/{raw.get('subreddit')}: {review.get('skip_reason')}")
                    continue

                candidate = {**raw, **review}
                drafted = await self._draft_karma_public_text(candidate, settings)
                if drafted is None:
                    continue

                session["approval_count"] = int(session.get("approval_count", 0)) + 1
                if settings.dry_run:
                    dry_items: list[dict] = session["dry_run_items"]  # type: ignore[assignment]
                    dry_items.append(drafted)
                    if int(session.get("approval_count", 0)) >= settings.max_opportunities_per_run:
                        return self._finish_karma_autopilot("dry run complete")
                    continue

                return self._queue_karma_approval(drafted)

            if int(session.get("approval_count", 0)) >= settings.max_opportunities_per_run:
                return self._finish_karma_autopilot("max opportunities reached")

        return self._finish_karma_autopilot("candidate scan budget reached")

    async def _handle_karma_growth_autopilot(self, user_message: str) -> Optional[str]:
        has_active = self._karma_autopilot_session is not None
        if has_active and _is_karma_stop_command(user_message):
            return self._finish_karma_autopilot("stopped by user")
        if has_active and _is_karma_continue_command(user_message):
            return await self._continue_karma_growth_autopilot()
        if not _is_karma_autopilot_request(user_message):
            return None

        settings = parse_karma_settings_from_text(user_message)
        self._karma_autopilot_session = self._build_karma_autopilot_session(settings)
        return await self._continue_karma_growth_autopilot()

    def _build_agentic_opportunity_session(self, profile: dict[str, object]) -> dict[str, object]:
        profile = {**OPPORTUNITY_DEFAULTS, **profile}
        max_age_days = _clamp_discovery_int(
            profile.get("max_age_days"),
            int(OPPORTUNITY_DEFAULTS["max_age_days"]),
            1,
            3650,
        )
        target_link_count = _clamp_discovery_int(
            profile.get("target_link_count"),
            OPPORTUNITY_TARGET_LINK_FALLBACK,
            1,
            200,
        )
        chunk_size = _clamp_discovery_int(
            profile.get("chunk_size"),
            int(OPPORTUNITY_DEFAULTS["chunk_size"]),
            1,
            20,
        )
        recent_days = _clamp_discovery_int(
            profile.get("recent_days"),
            int(OPPORTUNITY_DEFAULTS["recent_days"]),
            1,
            7,
        )
        opportunity_types = normalize_opportunity_types(
            profile.get("opportunity_types")
        ) or list(DEFAULT_OPPORTUNITY_TYPES)
        link_kind = normalize_link_kind(profile.get("link_kind")) or DEFAULT_LINK_KIND
        review_profile = build_opportunity_profile(
            product_name=str(profile.get("product_name", "")),
            product_description=str(profile.get("product_description", "")),
            target_customer=str(profile.get("target_customer", "")),
            pain_points=str(profile.get("pain_points", "")),
            use_cases=str(profile.get("use_cases", "")),
            keywords=str(profile.get("keywords", "")),
            competitor_names=str(profile.get("competitor_names", "")),
            excluded_subreddits=str(profile.get("excluded_subreddits", "")),
            max_age_days=max_age_days,
            product_url=str(profile.get("product_url", "")),
            product_mention_terms=str(profile.get("product_mention_terms", "")),
        )
        review_profile["recent_days"] = recent_days
        review_profile["opportunity_types"] = ", ".join(opportunity_types)
        review_profile["link_kind"] = link_kind
        return {
            "profile": review_profile,
            "target_link_count": target_link_count,
            "chunk_size": chunk_size,
            "recent_days": recent_days,
            "opportunity_types": opportunity_types,
            "opportunity_types_label": ", ".join(opportunity_types),
            "link_kind": link_kind,
            "search_plan": build_agentic_search_plan(
                review_profile,
                opportunity_types=opportunity_types,
                link_kind=link_kind,
            ),
            "plan_index": 0,
            "pending_candidates": [],
            "accepted_results": [],
            "seen_urls": set(),
            "chunk_number": 0,
            "exhausted": False,
            "coverage": {
                "queries_tried": [],
                "subreddits_scanned": set(),
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
            },
        }

    def _track_opportunity_rejection(self, session: dict, reason: str) -> None:
        coverage = session["coverage"]
        coverage["candidates_rejected"] += 1
        reasons = coverage["rejection_reasons"]
        reasons[reason] = reasons.get(reason, 0) + 1

    def _track_search_mode(self, session: dict, mode: str) -> None:
        modes = session["coverage"]["search_modes_used"]
        if mode not in modes:
            modes.append(mode)

    def _queue_unique_opportunity_candidates(self, session: dict, candidates: list[dict]) -> None:
        pending = session["pending_candidates"]
        seen = session["seen_urls"]
        coverage = session["coverage"]
        profile = session["profile"]
        excluded = profile.get("excluded_subreddits", set())
        allowed_types = {"post", "comment"} if session.get("link_kind") == "both" else {
            "comment" if session.get("link_kind") == "comments" else "post"
        }

        for candidate in candidates:
            if candidate.get("subreddit", "").lower() in excluded:
                candidate["excluded"] = True
            if candidate.get("type", "post") not in allowed_types:
                continue
            key = _reddit_url_key(candidate.get("url", ""))
            if not key or key in seen:
                continue
            seen.add(key)
            pending.append(candidate)
            coverage["candidates_found"] += 1
            subreddit = candidate.get("subreddit")
            if subreddit:
                coverage["subreddits_scanned"].add(subreddit)

    async def _fetch_next_opportunity_candidates(self, session: dict, page) -> None:
        if session.get("exhausted"):
            return

        pending: list[dict] = session["pending_candidates"]
        plan: list[dict] = session["search_plan"]
        coverage = session["coverage"]

        while not pending and session["plan_index"] < len(plan):
            step = plan[session["plan_index"]]
            session["plan_index"] += 1
            mode = step["mode"]
            query = step["query"]

            if mode == "google":
                coverage["queries_tried"].append(f"google:{query}")
                self._track_search_mode(session, "google_search")
                results = await _google_search(page, query, max_results=10)
                coverage["pages_searched"] += 1
                self._queue_unique_opportunity_candidates(session, results)
                await asyncio.sleep(0.8)
                continue

            result_type = step.get("result_type", "link")
            prefix = "reddit_comments" if result_type == "comment" else "reddit"
            coverage["queries_tried"].append(f"{prefix}:{query}")
            self._track_search_mode(session, "authenticated_search")
            results, block_reason = await _authenticated_reddit_search(
                page,
                query,
                result_type=result_type,
                sort=step.get("sort", "relevance"),
                max_pages=1,
                time_filter=step.get("time_filter", ""),
                safe_search=step.get("safe_search"),
            )
            coverage["pages_searched"] += 1
            if block_reason:
                blocked = coverage["blocked_indicators"]
                if block_reason not in blocked:
                    blocked.append(block_reason)
                if _is_hard_block_reason(block_reason):
                    self._track_search_mode(session, "manual_fallback")
                    manual = await _manual_reddit_search(
                        page,
                        query,
                        result_type=result_type,
                        sort=step.get("sort", "relevance"),
                        max_results=8,
                        time_filter=step.get("time_filter", ""),
                        safe_search=step.get("safe_search"),
                    )
                    results.extend(manual)

            self._queue_unique_opportunity_candidates(session, results)
            await asyncio.sleep(0.8)

        if not pending and session["plan_index"] >= len(plan):
            session["exhausted"] = True

    async def _current_page_text_for_review(self, page) -> str:
        try:
            text = await page.evaluate("() => document.body?.innerText || ''")
        except Exception:
            return ""
        return re.sub(r"\s+", " ", str(text)).strip()[:6000]

    async def _review_opportunity_candidate(self, candidate: dict, profile: dict, page_text: str) -> dict:
        payload = {
            "product": {
                "name": profile.get("product_name", ""),
                "url": profile.get("product_url", ""),
                "description": profile.get("product_description", ""),
                "target_customer": profile.get("target_customer", ""),
                "pain_points": profile.get("pain_points_list", []),
                "use_cases": profile.get("use_cases_list", []),
                "keywords": profile.get("keywords_list", []),
                "competitors": profile.get("competitors_list", []),
            },
            "candidate": {
                "url": candidate.get("url", ""),
                "type": candidate.get("type", "post"),
                "subreddit": candidate.get("subreddit", ""),
                "title": candidate.get("title", ""),
                "body_preview": candidate.get("body", ""),
                "score": candidate.get("score"),
                "comment_count": candidate.get("comment_count"),
                "created_date": candidate.get("created_date", "unknown"),
            },
            "visible_page_text": page_text,
        }
        system = (
            "You review Reddit posts/comments for SaaS promotion discovery. "
            "Decide whether mentioning the product would be genuinely helpful and rule-aware. "
            "Reject spammy, unrelated, hostile, meme-only, or already self-promotional threads. "
            "Reject the candidate if the visible context already mentions emailverifier.io. "
            "Do not suggest posting automatically. Return only JSON with keys: "
            "is_opportunity boolean, fit strong|medium|weak|reject, promotion_risk low|medium|high, "
            "reason string, suggested_angle string, best_action string, needs_rule_check boolean."
        )

        try:
            response = await self._llm.ainvoke([
                SystemMessage(content=system),
                HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
            ])
            content = response.content if hasattr(response, "content") else str(response)
            parsed = _extract_json_object(str(content))
            parsed["review_source"] = "llm"
            return _normalize_opportunity_review(parsed)
        except Exception as exc:
            rel, conf = _score_candidate(candidate, profile)
            is_possible = rel >= 45 or conf >= 55
            return _normalize_opportunity_review({
                "is_opportunity": is_possible,
                "fit": "medium" if rel >= 60 else "weak",
                "promotion_risk": "medium",
                "reason": (
                    "LLM review failed, so this was kept only as a weak fallback signal "
                    f"from candidate text. Error: {exc}"
                ),
                "suggested_angle": "Manually inspect the thread and subreddit rules before considering any reply.",
                "best_action": "manual_review_before_reply",
                "needs_rule_check": True,
                "review_source": "heuristic_fallback",
            })

    async def _continue_opportunity_discovery(self) -> str:
        session = self._opportunity_session
        if session is None:
            return "There is no active opportunity discovery session. Start one with your product details first."

        ok, status = await ensure_reddit_logged_in(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )
        if not ok:
            return status

        page = await self.lazy.get_page()
        session["chunk_number"] += 1
        chunk: list[dict] = []
        profile = session["profile"]
        target_link_count = int(session["target_link_count"])
        chunk_size = int(session["chunk_size"])
        start_seen = int(session["coverage"]["candidates_seen"])
        start_reviewed = int(session["coverage"]["llm_reviewed"])
        candidate_budget = max(40, chunk_size * 8)
        review_budget = max(20, chunk_size * 4)

        while (
            len(chunk) < chunk_size
            and len(session["accepted_results"]) < target_link_count
            and not session.get("exhausted")
            and int(session["coverage"]["candidates_seen"]) - start_seen < candidate_budget
            and int(session["coverage"]["llm_reviewed"]) - start_reviewed < review_budget
        ):
            await self._fetch_next_opportunity_candidates(session, page)
            pending: list[dict] = session["pending_candidates"]
            if not pending:
                break

            candidate = pending.pop(0)
            session["coverage"]["candidates_seen"] += 1

            reason = _basic_reject_reason(candidate, profile)
            if reason:
                self._track_opportunity_rejection(session, reason)
                continue

            verified = await _verify_candidate(page, candidate, profile)
            reason = _basic_reject_reason(verified, profile)
            if reason:
                self._track_opportunity_rejection(session, reason)
                continue

            category = candidate_opportunity_category(
                verified,
                session.get("opportunity_types", DEFAULT_OPPORTUNITY_TYPES),
                session.get("link_kind", DEFAULT_LINK_KIND),
                int(session.get("recent_days", 7)),
            )
            if not category:
                self._track_opportunity_rejection(session, "outside_requested_type")
                continue
            verified["category"] = category

            page_text = await self._current_page_text_for_review(page)
            review = await self._review_opportunity_candidate(verified, profile, page_text)
            session["coverage"]["llm_reviewed"] += 1

            if not review.get("is_opportunity"):
                session["coverage"]["llm_rejected"] += 1
                self._track_opportunity_rejection(session, "llm_rejected")
                continue

            result = _build_agentic_result(verified, profile, review)
            session["accepted_results"].append(result)
            session["coverage"]["llm_accepted"] += 1
            chunk.append(result)
            await asyncio.sleep(0.4)

        await self.lazy.persist_session()
        exhausted = bool(session.get("exhausted")) or len(session["accepted_results"]) >= target_link_count
        message = _format_agentic_opportunity_chunk(chunk, session, exhausted=exhausted)
        if exhausted:
            self._opportunity_session = None
        return message

    async def _run_opportunity_discovery(self, profile: dict[str, object]) -> str:
        target_link_count = _clamp_discovery_int(
            profile.get("target_link_count"),
            OPPORTUNITY_TARGET_LINK_FALLBACK,
            1,
            200,
        )
        max_age_days = _clamp_discovery_int(
            profile.get("max_age_days"),
            int(OPPORTUNITY_DEFAULTS["max_age_days"]),
            1,
            3650,
        )
        recent_days = _clamp_discovery_int(
            profile.get("recent_days"),
            int(OPPORTUNITY_DEFAULTS["recent_days"]),
            1,
            min(365, max_age_days),
        )
        opportunity_types = normalize_opportunity_types(
            profile.get("opportunity_types")
        ) or list(DEFAULT_OPPORTUNITY_TYPES)
        link_kind = normalize_link_kind(profile.get("link_kind")) or DEFAULT_LINK_KIND

        llm_review = None
        if getattr(self, "_llm", None) is not None:
            llm_review = make_langchain_llm_review(self._llm)

        result = await discover_opportunities_via_api(
            product_name=str(profile.get("product_name", "")),
            product_description=str(profile.get("product_description", "")),
            target_customer=str(profile.get("target_customer", "")),
            pain_points=str(profile.get("pain_points", "")),
            use_cases=str(profile.get("use_cases", "")),
            keywords=str(profile.get("keywords", "")),
            competitor_names=str(profile.get("competitor_names", "")),
            excluded_subreddits=str(profile.get("excluded_subreddits", "")),
            target_link_count=target_link_count,
            max_age_days=max_age_days,
            recent_days=recent_days,
            opportunity_types=opportunity_types,
            link_kind=link_kind,
            product_url=str(profile.get("product_url", "")),
            product_mention_terms=str(profile.get("product_mention_terms", "")),
            llm_review=llm_review,
        )
        self._opportunity_session = None
        self._pending_opportunity_profile = None
        return _summarize_opportunity_result(result)

    async def _handle_opportunity_discovery(self, user_message: str) -> Optional[str]:
        parsed_fields = _parse_opportunity_preferences(user_message)
        starts_new_request = is_opportunity_discovery_request(user_message)
        has_pending = self._pending_opportunity_profile is not None
        has_active_session = getattr(self, "_opportunity_session", None) is not None

        if has_active_session and _is_opportunity_cancel_command(user_message):
            self._opportunity_session = None
            return "Stopped the active opportunity discovery session."

        if has_active_session and _is_opportunity_continue_command(user_message):
            return await self._continue_opportunity_discovery()

        if _is_opportunity_continue_command(user_message) and not has_active_session:
            return "There is no active opportunity discovery session. Start one with your product details first."

        if not (starts_new_request or parsed_fields or has_pending or _is_opportunity_run_command(user_message)):
            return None

        if starts_new_request:
            self._opportunity_session = None

        if self._pending_opportunity_profile is None:
            self._pending_opportunity_profile = dict(OPPORTUNITY_DEFAULTS)

        self._pending_opportunity_profile.update(parsed_fields)
        missing = _missing_opportunity_fields(self._pending_opportunity_profile)

        if missing:
            return _opportunity_prompt(missing)

        if _should_run_opportunity_discovery(
            user_message,
            self._pending_opportunity_profile,
            parsed_fields,
            starts_new_request,
        ):
            profile = dict(self._pending_opportunity_profile)
            return await self._run_opportunity_discovery(profile)

        return _opportunity_prompt([], ready=True)

    async def chat(self, user_message: str) -> str:
        self._history.append(HumanMessage(content=user_message))

        confirmation_result = await self._handle_pending_confirmation(user_message)
        if confirmation_result is not None:
            return confirmation_result

        if _is_login_status_question(user_message):
            return await self.check_login_status()

        if self._pending_comment_url:
            reply = confirmation_reply(user_message)
            if reply is False:
                self._pending_comment_url = None
                return "Canceled the pending comment."
            if reply is True:
                return "Please send the comment text you want me to submit."
            if _is_new_reddit_command_while_pending_comment(user_message):
                self._pending_comment_url = None
            else:
                post_url = self._pending_comment_url
                self._pending_comment_url = None
                return await self._queue_direct_comment_confirmation(post_url, user_message.strip())

        if _is_comment_upvote_request(user_message):
            comment_url = _extract_reddit_url(user_message)
            if not comment_url:
                return "Please send the Reddit comment URL you want me to upvote."
            if not reddit_url_points_to_comment(comment_url):
                return "Please send the exact Reddit comment permalink, not just the post URL."
            return await self.upvote_comment(comment_url)

        if _is_post_upvote_request(user_message):
            post_url = _extract_reddit_url(user_message)
            if not post_url:
                return "Please send the Reddit post URL you want me to upvote."
            return await self.upvote_post(post_url)

        if _is_comment_request(user_message):
            post_url = _extract_reddit_url(user_message)
            if not post_url:
                return "Please send the Reddit post URL you want me to comment on."
            self._pending_comment_url = post_url
            return "What would you like the comment to say?"

        if _is_open_reddit_request(user_message):
            return await self.open_reddit()

        if _is_login_request(user_message):
            return await self.login_reddit()

        # Canonical karma-builder route. Keep this before ReAct and Opportunity Finder.
        karma_reply = await self._handle_karma_growth_autopilot(user_message)
        if karma_reply is not None:
            return karma_reply

        opportunity_reply = await self._handle_opportunity_discovery(user_message)
        if opportunity_reply is not None:
            return opportunity_reply

        local = _local_reply(user_message)
        if local:
            return local

        last_error: Optional[Exception] = None
        start_index = self._model_index
        model_order = list(range(start_index, len(self._models))) + list(range(0, start_index))

        for model_index in model_order:
            self._switch_model(model_index)
            for attempt in range(2):
                approved_pending = None
                if self._confirmation_state.get("approved"):
                    pending = self._confirmation_state.get("pending")
                    approved_pending = dict(pending) if pending else None
                try:
                    result = await asyncio.wait_for(
                        self._agent.ainvoke(
                            {"messages": self._history},
                            config=self._react_config,
                        ),
                        timeout=self._react_timeout_seconds,
                    )

                    all_messages: list[BaseMessage] = result["messages"]
                    # Last message is the final AI response
                    final = all_messages[-1]
                    reply = final.content if hasattr(final, "content") else str(final)

                    # Keep full history for next turn
                    self._history = all_messages
                    if self._pending_tool_still_approved(approved_pending):
                        return self._unconsumed_confirmation_message(approved_pending)
                    return reply
                except asyncio.TimeoutError:
                    pending = self._disarm_pending_tool_approval()
                    return react_timeout_message(self._react_timeout_seconds, pending)
                except Exception as exc:
                    if is_react_loop_error(exc):
                        pending = self._disarm_pending_tool_approval()
                        return react_loop_stop_message(exc, pending)
                    last_error = exc
                    if not _is_transient_llm_error(exc):
                        raise
                    await asyncio.sleep(1.5 * (attempt + 1))

        return (
            "The AI provider is temporarily failing, so I could not process that message yet. "
            f"Last error: {last_error}"
        )

    async def close(self) -> None:
        await self.lazy.close()
