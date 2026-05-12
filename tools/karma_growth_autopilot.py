"""
tools/karma_growth_autopilot.py - Organic Reddit karma opportunity helpers.

This module is intentionally read-only. It normalizes settings, filters risky
threads, scores opportunities, and prepares approval prompts. Submission is
left to the existing comment/reply/post tools after explicit user approval.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Optional


DEFAULT_MAX_OPPORTUNITIES_PER_RUN = 10
DEFAULT_MAX_COMMENTS_PER_RUN = 3
DEFAULT_MINIMUM_SCORE = 70

TRUE_VALUES = {"1", "true", "yes", "y", "on", "allow", "allowed", "enable", "enabled"}
FALSE_VALUES = {"0", "false", "no", "n", "off", "deny", "disallow", "disabled"}

PUBLIC_TEXT_ACTIONS = {"comment", "reply", "post", "comment_on_post", "reply_to_comment", "create_post"}

QUESTION_TERMS = (
    "how do i",
    "how to",
    "what should",
    "what would",
    "any advice",
    "recommend",
    "suggestion",
    "which",
    "why is",
    "need help",
    "looking for",
    "best way",
    "what are you using",
)

VALUE_FIT_TERMS = (
    "saas",
    "startup",
    "marketing",
    "sales",
    "b2b",
    "email",
    "deliverability",
    "cold email",
    "outreach",
    "product",
    "automation",
    "workflow",
    "founder",
    "small business",
    "newsletter",
    "crm",
    "webdev",
    "devops",
)

POLITICAL_OR_CONTROVERSIAL_TERMS = (
    "election",
    "politics",
    "political",
    "trump",
    "biden",
    "republican",
    "democrat",
    "liberal",
    "conservative",
    "left wing",
    "right wing",
    "israel",
    "gaza",
    "ukraine",
    "russia",
    "war",
    "religion",
    "race",
    "gender debate",
)

TOXIC_OR_HOSTILE_TERMS = (
    "idiot",
    "moron",
    "stupid",
    "scam",
    "fraud",
    "hate",
    "kill",
    "dox",
    "nsfw",
    "rage bait",
    "ragebait",
)

RULE_HEAVY_TERMS = (
    "read the rules",
    "rules in the sidebar",
    "weekly thread",
    "megathread",
    "self promotion",
    "self-promotion",
    "no promotion",
    "no advertising",
    "mod removed",
    "removed by moderators",
)

PRODUCT_MENTION_PATTERNS = (
    r"\bemailverifier\b",
    r"\bemail\s*verifier\s*\.\s*io\b",
    r"\bemail\s*verifier\s+dot\s+io\b",
    r"\bhttps?://",
    r"\bwww\.",
)

SPAM_EVASION_PATTERNS = (
    r"\bdot\s+(com|io|net|org)\b",
    r"\b[a-z0-9-]+\s+\.\s+(com|io|net|org)\b",
    r"\b[a-z0-9-]+\s*\[\s*dot\s*\]\s*(com|io|net|org)\b",
)

FAKE_PERSONAL_CLAIM_PATTERNS = (
    r"\bi'?ve been using\b",
    r"\bi use(d)? this\b",
    r"\bmy team uses\b",
    r"\bat my company\b",
    r"\bfor months\b",
    r"\bsaved me\b",
    r"\bsaved us\b",
)


@dataclass(frozen=True)
class KarmaAutopilotSettings:
    max_opportunities_per_run: int = DEFAULT_MAX_OPPORTUNITIES_PER_RUN
    max_comments_per_run: int = DEFAULT_MAX_COMMENTS_PER_RUN
    minimum_score: int = DEFAULT_MINIMUM_SCORE
    subreddit_allowlist: tuple[str, ...] = ()
    subreddit_blocklist: tuple[str, ...] = ()
    promotion_allowed: bool = False
    dry_run: bool = False
    require_approval_for_comments: bool = True
    require_approval_for_replies: bool = True
    require_approval_for_posts: bool = True

    def to_public_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["subreddit_allowlist"] = list(self.subreddit_allowlist)
        data["subreddit_blocklist"] = list(self.subreddit_blocklist)
        return data


def _clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(parsed, maximum))


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return default


def normalize_subreddit_name(value: Any) -> str:
    text = str(value or "").strip()
    text = text.strip("/").removeprefix("r/").removeprefix("R/")
    text = re.sub(r"[^A-Za-z0-9_]", "", text)
    return text


def parse_subreddit_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(item) for item in value]
    else:
        raw_items = re.split(r"[,;\n]+", str(value))
    seen: set[str] = set()
    items: list[str] = []
    for raw in raw_items:
        name = normalize_subreddit_name(raw)
        key = name.lower()
        if name and key not in seen:
            seen.add(key)
            items.append(name)
    return tuple(items)


def normalize_settings(
    max_opportunities_per_run: Any = DEFAULT_MAX_OPPORTUNITIES_PER_RUN,
    max_comments_per_run: Any = DEFAULT_MAX_COMMENTS_PER_RUN,
    minimum_score: Any = DEFAULT_MINIMUM_SCORE,
    subreddit_allowlist: Any = None,
    subreddit_blocklist: Any = None,
    promotion_allowed: Any = False,
    dry_run: Any = False,
    require_approval_for_comments: Any = True,
    require_approval_for_replies: Any = True,
    require_approval_for_posts: Any = True,
) -> KarmaAutopilotSettings:
    return KarmaAutopilotSettings(
        max_opportunities_per_run=_clamp_int(max_opportunities_per_run, DEFAULT_MAX_OPPORTUNITIES_PER_RUN, 1, 50),
        max_comments_per_run=_clamp_int(max_comments_per_run, DEFAULT_MAX_COMMENTS_PER_RUN, 0, 10),
        minimum_score=_clamp_int(minimum_score, DEFAULT_MINIMUM_SCORE, 1, 100),
        subreddit_allowlist=parse_subreddit_list(subreddit_allowlist),
        subreddit_blocklist=parse_subreddit_list(subreddit_blocklist),
        promotion_allowed=coerce_bool(promotion_allowed, False),
        dry_run=coerce_bool(dry_run, False),
        require_approval_for_comments=coerce_bool(require_approval_for_comments, True),
        require_approval_for_replies=coerce_bool(require_approval_for_replies, True),
        require_approval_for_posts=coerce_bool(require_approval_for_posts, True),
    )


SETTING_LABELS = {
    "maxopportunitiesperrun": "max_opportunities_per_run",
    "maxopportunities": "max_opportunities_per_run",
    "maxcandidates": "max_opportunities_per_run",
    "maxcommentsperrun": "max_comments_per_run",
    "maxcomments": "max_comments_per_run",
    "minimumscore": "minimum_score",
    "minscore": "minimum_score",
    "threshold": "minimum_score",
    "subredditallowlist": "subreddit_allowlist",
    "allowlist": "subreddit_allowlist",
    "subreddits": "subreddit_allowlist",
    "subredditblocklist": "subreddit_blocklist",
    "blocklist": "subreddit_blocklist",
    "blockedsubreddits": "subreddit_blocklist",
    "promotionallowed": "promotion_allowed",
    "promotion": "promotion_allowed",
    "dryrun": "dry_run",
    "requireapprovalforcomments": "require_approval_for_comments",
    "requireapprovalforreplies": "require_approval_for_replies",
    "requireapprovalforposts": "require_approval_for_posts",
}


def _compact_command_text(text: str) -> str:
    compact = re.sub(r"[^a-z0-9_\s-]", " ", str(text or "").strip().lower())
    return re.sub(r"\s+", " ", compact).strip()


def is_karma_autopilot_request_text(text: str) -> bool:
    """Return True for user phrases that should start the karma builder path."""
    compact = _compact_command_text(text)
    if not compact:
        return False
    direct_phrases = {
        "karma_growth_autopilot",
        "karma growth autopilot",
        "organic karma autopilot",
        "karma autopilot",
        "karma builder",
        "start karma builder",
        "run karma builder",
        "build karma",
        "start building karma",
        "grow karma",
        "start growing karma",
        "start karma growth",
        "build my karma",
        "grow my karma",
        "start building reddit karma",
        "start growing reddit karma",
    }
    if compact in direct_phrases:
        return True
    return bool(
        re.search(r"\b(start|run|begin|use)\b.*\bkarma\b.*\b(autopilot|builder|growth)\b", compact)
        or re.search(r"\b(start|run|begin|use)\b.*\b(build|building|grow|growing)\b.*\b(?:reddit\s+)?karma\b", compact)
        or re.search(r"\b(build|building|grow|growing)\b.*\b(?:reddit\s+)?karma\b", compact)
        or re.search(r"\bfind\b.*\bkarma\b.*\b(opportunit|comments?|posts?)\b", compact)
        or re.search(r"\bkarma\b.*\b(comments?|posts?|opportunit)\b", compact)
        or re.search(r"\b(organic|authentic)\b.*\breddit\b.*\b(participation|comments?|opportunit)\b", compact)
        or re.search(r"\breddit\b.*\b(participation|comments?|opportunit)\b.*\bkarma\b", compact)
    )


def is_karma_continue_command_text(text: str) -> bool:
    compact = _compact_command_text(text)
    return bool(
        compact in {"continue karma autopilot", "continue autopilot", "keep searching", "keep going"}
        or re.search(r"^(continue|more|next|keep going)(?:\s+(karma|autopilot|searching|opportunities))?$", compact)
    )


def is_karma_stop_command_text(text: str) -> bool:
    compact = _compact_command_text(text)
    return compact in {
        "stop karma autopilot",
        "cancel karma autopilot",
        "end karma autopilot",
        "stop autopilot",
        "cancel autopilot",
        "stop searching",
        "stop karma builder",
        "cancel karma builder",
    }


def _field_key(label: str) -> str:
    return re.sub(r"[^a-z0-9]", "", label.lower())


def parse_settings_from_text(text: str, defaults: Optional[KarmaAutopilotSettings] = None) -> KarmaAutopilotSettings:
    base = (defaults or KarmaAutopilotSettings()).to_public_dict()
    overrides: dict[str, Any] = {}

    for line in str(text or "").splitlines():
        match = re.match(r"^\s*([A-Za-z][A-Za-z0-9 _/\-.]{1,56})\s*[:=]\s*(.*?)\s*$", line)
        if not match:
            continue
        key = SETTING_LABELS.get(_field_key(match.group(1)))
        if key:
            overrides[key] = match.group(2).strip()

    compact = re.sub(r"[^a-z0-9_\s,/-]", " ", str(text or "").lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    if "dry run" in compact or "dry-run" in compact:
        overrides["dry_run"] = True
    if re.search(r"\bpromotion\s+(?:allowed|on|true|yes)\b", compact):
        overrides["promotion_allowed"] = True
    if re.search(r"\b(?:no|without)\s+promotion\b", compact):
        overrides["promotion_allowed"] = False

    patterns = (
        ("max_comments_per_run", r"\bmax\s+comments(?:\s+per\s+run)?\s+(\d{1,2})\b"),
        ("max_opportunities_per_run", r"\bmax\s+(?:opportunities|candidates)(?:\s+per\s+run)?\s+(\d{1,2})\b"),
        ("minimum_score", r"\b(?:minimum|min)\s+score\s+(\d{1,3})\b"),
    )
    for key, pattern in patterns:
        match = re.search(pattern, compact)
        if match and key not in overrides:
            overrides[key] = match.group(1)

    base.update(overrides)
    return normalize_settings(**base)


def parse_compact_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = str(value).strip().lower().replace(",", "")
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*([km])?", text)
    if not match:
        return 0
    number = float(match.group(1))
    suffix = match.group(2)
    if suffix == "k":
        number *= 1000
    elif suffix == "m":
        number *= 1_000_000
    return max(0, int(number))


def _normalize_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _candidate_text(candidate: dict[str, Any]) -> str:
    parts = [
        candidate.get("title", ""),
        candidate.get("body", ""),
        candidate.get("context", ""),
        candidate.get("parent_comment_text", ""),
    ]
    comments = candidate.get("top_comments") or []
    if isinstance(comments, list):
        for comment in comments[:5]:
            if isinstance(comment, dict):
                parts.append(comment.get("body") or comment.get("text") or "")
            else:
                parts.append(str(comment))
    return _normalize_space(" ".join(str(part or "") for part in parts)).lower()


def normalize_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(candidate)
    normalized["subreddit"] = normalize_subreddit_name(normalized.get("subreddit", ""))
    normalized["type"] = str(normalized.get("type") or normalized.get("action") or "post").strip().lower()
    normalized["url"] = str(normalized.get("url") or normalized.get("post_url") or "").strip()
    normalized["post_url"] = str(normalized.get("post_url") or normalized.get("url") or "").strip()
    normalized["title"] = _normalize_space(normalized.get("title", ""))
    normalized["body"] = _normalize_space(normalized.get("body") or normalized.get("post_body") or normalized.get("context", ""))
    normalized["score"] = parse_compact_count(normalized.get("score") or normalized.get("upvotes"))
    normalized["comment_count"] = parse_compact_count(normalized.get("comment_count") or normalized.get("comments"))
    return normalized


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _regex_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def classify_risk(candidate: dict[str, Any]) -> tuple[str, list[str]]:
    text = _candidate_text(candidate)
    status = str(candidate.get("status") or "").strip().lower()
    flags: list[str] = []

    if candidate.get("locked") or candidate.get("archived") or status in {"locked", "archived"}:
        flags.append("locked_or_archived")
    if candidate.get("removed") or candidate.get("deleted") or status in {"removed", "deleted"}:
        flags.append("removed_or_deleted")
    if _contains_any(text, POLITICAL_OR_CONTROVERSIAL_TERMS):
        flags.append("political_or_controversial")
    if _contains_any(text, TOXIC_OR_HOSTILE_TERMS):
        flags.append("toxic_or_hostile")
    if _contains_any(text, RULE_HEAVY_TERMS):
        flags.append("rule_heavy")

    hard = {
        "locked_or_archived",
        "removed_or_deleted",
        "political_or_controversial",
        "toxic_or_hostile",
    }
    if any(flag in hard for flag in flags):
        return "high", flags
    if flags:
        return "medium", flags
    return "low", flags


def score_candidate(candidate: dict[str, Any], settings: KarmaAutopilotSettings) -> dict[str, Any]:
    candidate = normalize_candidate(candidate)
    subreddit = candidate.get("subreddit", "")
    subreddit_key = subreddit.lower()
    allow = {item.lower() for item in settings.subreddit_allowlist}
    block = {item.lower() for item in settings.subreddit_blocklist}
    skip_reasons: list[str] = []
    factors: list[str] = []

    if allow and subreddit_key not in allow:
        skip_reasons.append("outside_subreddit_allowlist")
    if subreddit_key in block:
        skip_reasons.append("blocked_subreddit")

    risk_level, risk_flags = classify_risk(candidate)
    if risk_level == "high":
        skip_reasons.extend(risk_flags)

    text = _candidate_text(candidate)
    score = 42

    if _contains_any(text, QUESTION_TERMS):
        score += 18
        factors.append("clear question or advice request")
    if _contains_any(text, VALUE_FIT_TERMS):
        score += 12
        factors.append("persona fit")
    if len(candidate.get("title", "")) >= 20:
        score += 6
        factors.append("enough context to answer")

    comments = int(candidate.get("comment_count") or 0)
    if comments <= 5:
        score += 12
        factors.append("low comment competition")
    elif comments <= 25:
        score += 7
        factors.append("manageable comment volume")
    elif comments > 100:
        score -= 12
        factors.append("crowded thread")

    upvotes = int(candidate.get("score") or 0)
    if 3 <= upvotes <= 200:
        score += 7
        factors.append("some traction without being saturated")
    elif upvotes > 1000:
        score -= 6
        factors.append("very saturated thread")

    age_days = candidate.get("age_days")
    try:
        age = float(age_days)
    except (TypeError, ValueError):
        age = None
    if age is not None:
        if age <= 2:
            score += 12
            factors.append("fresh thread")
        elif age <= 7:
            score += 6
            factors.append("recent thread")
        elif age > 30:
            score -= 12
            factors.append("older thread")

    if risk_level == "medium":
        score -= 18
        factors.append("medium risk signals: " + ", ".join(risk_flags))

    score = max(0, min(100, score))
    reason = "; ".join(factors) if factors else "basic relevance and engagement signals"
    if skip_reasons:
        reason = f"Skipped: {', '.join(dict.fromkeys(skip_reasons))}. {reason}"

    return {
        "score": score,
        "risk_level": risk_level,
        "risk_flags": risk_flags,
        "is_high_fit": not skip_reasons and risk_level != "high" and score >= settings.minimum_score,
        "skip_reason": ", ".join(dict.fromkeys(skip_reasons)),
        "reason": reason,
    }


def summarize_context(candidate: dict[str, Any], max_chars: int = 420) -> str:
    candidate = normalize_candidate(candidate)
    parts: list[str] = []
    if candidate.get("title"):
        parts.append(f"Title: {candidate['title']}")
    if candidate.get("body"):
        parts.append(f"Post: {candidate['body']}")
    parent = _normalize_space(candidate.get("parent_comment_text", ""))
    if parent:
        parts.append(f"Parent comment: {parent}")
    summary = _normalize_space(" ".join(parts))
    if len(summary) > max_chars:
        summary = summary[: max_chars - 3].rstrip() + "..."
    return summary or "No readable context captured."


def fallback_draft(candidate: dict[str, Any], promotion_allowed: bool = False) -> str:
    candidate = normalize_candidate(candidate)
    text = _candidate_text(candidate)
    if "cold email" in text or "bounce" in text or "deliverability" in text:
        draft = (
            "I'd start with list quality before tweaking copy or domains.\n"
            "A small validation pass usually makes the real bounce problem way clearer."
        )
    elif "recommend" in text or "looking for" in text or "which tool" in text:
        draft = (
            "I'd narrow it down by the one workflow you need most, then test 2-3 options on a tiny sample.\n"
            "Feature lists get noisy fast; the trial usually tells you the truth."
        )
    elif "startup" in text or "saas" in text or "founder" in text:
        draft = (
            "I'd keep the first pass boring and useful.\n"
            "Ship the smallest version that proves the pain is real, then let feedback decide what gets added."
        )
    else:
        draft = (
            "Solid question. I'd make the next step smaller and test it with real data first.\n"
            "That usually beats trying to solve the whole thing in one move."
        )
    return sanitize_public_text(draft, promotion_allowed=promotion_allowed)


def sanitize_public_text(text: str, promotion_allowed: bool = False) -> str:
    lines = [re.sub(r"\s+", " ", line).strip() for line in str(text or "").splitlines()]
    lines = [line for line in lines if line]
    cleaned = "\n".join(lines[:5]).strip()
    if not promotion_allowed:
        cleaned = re.sub(r"https?://\S+", "", cleaned).strip()
    return cleaned


def validate_public_text(text: str, promotion_allowed: bool = False) -> list[str]:
    issues: list[str] = []
    cleaned = str(text or "").strip()
    lines = [line for line in cleaned.splitlines() if line.strip()]
    lowered = cleaned.lower()
    if not cleaned:
        issues.append("empty_text")
    if len(lines) > 5:
        issues.append("too_many_lines")
    if len(cleaned) > 800:
        issues.append("too_long")
    if _regex_any(lowered, SPAM_EVASION_PATTERNS):
        issues.append("spam_filter_evasion_pattern")
    if _regex_any(lowered, FAKE_PERSONAL_CLAIM_PATTERNS):
        issues.append("fake_personal_claim_risk")
    if not promotion_allowed and _regex_any(lowered, PRODUCT_MENTION_PATTERNS):
        issues.append("promotion_not_allowed")
    return issues


def public_action_requires_approval(action: str, settings: KarmaAutopilotSettings) -> bool:
    normalized = str(action or "").strip().lower()
    if normalized in {"comment", "comment_on_post"}:
        return settings.require_approval_for_comments
    if normalized in {"reply", "reply_to_comment", "reply_to_reddit_comment"}:
        return settings.require_approval_for_replies
    if normalized in {"post", "create_post", "submit_text_post"}:
        return settings.require_approval_for_posts
    return normalized in PUBLIC_TEXT_ACTIONS


def format_approval_request(opportunity: dict[str, Any]) -> str:
    action = str(opportunity.get("action") or "comment").replace("_", " ")
    subreddit = normalize_subreddit_name(opportunity.get("subreddit", ""))
    url = opportunity.get("comment_url") or opportunity.get("post_url") or opportunity.get("url") or ""
    context = opportunity.get("context_summary") or summarize_context(opportunity)
    reason = opportunity.get("reason") or "High-fit opportunity based on freshness, fit, and low-risk context."
    risk = opportunity.get("risk_level") or "medium"
    score = opportunity.get("score")
    draft = str(opportunity.get("text") or "").strip()
    score_line = f"\nScore: {score}" if score is not None else ""
    return (
        "[CONFIRMATION REQUIRED] Karma Growth Autopilot found a public-text opportunity.\n"
        f"Action: {action}\n"
        f"Subreddit: r/{subreddit or 'unknown'}\n"
        f"Post/comment URL: {url}\n"
        f"Context summary: {context}\n"
        f"Why this is good: {reason}{score_line}\n"
        f"Risk level: {risk}\n"
        "Exact final text:\n"
        f"```\n{draft}\n```\n"
        "Reply 'yes' to submit it or 'no' to skip it and keep searching."
    )


def ranked_opportunities(candidates: list[dict[str, Any]], settings: KarmaAutopilotSettings) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for candidate in candidates:
        normalized = normalize_candidate(candidate)
        review = score_candidate(normalized, settings)
        ranked.append({**normalized, **review})
    ranked.sort(key=lambda item: (item.get("is_high_fit", False), int(item.get("score") or 0)), reverse=True)
    return ranked


__all__ = [
    "KarmaAutopilotSettings",
    "classify_risk",
    "coerce_bool",
    "fallback_draft",
    "format_approval_request",
    "is_karma_autopilot_request_text",
    "is_karma_continue_command_text",
    "is_karma_stop_command_text",
    "normalize_candidate",
    "normalize_settings",
    "parse_settings_from_text",
    "parse_subreddit_list",
    "public_action_requires_approval",
    "ranked_opportunities",
    "sanitize_public_text",
    "score_candidate",
    "summarize_context",
    "validate_public_text",
]
