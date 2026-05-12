"""
agent_tools.py — LangGraph-compatible tool wrappers around tools/.
Tools use LazyBrowser — browser only launches on first tool call.
"""

from __future__ import annotations

import base64
import os
import re
from datetime import datetime, time, timedelta, timezone as dt_timezone
from zoneinfo import ZoneInfo
from typing import Optional
from urllib.parse import quote_plus
from langchain_core.tools import tool

import json

from tools import login, browse, comment, upvote, comment_upvote, join_subreddit, post, reply
from tools.browse_tool import warmup_browsing_session
from tools.observation_tool import observe_page as _observe_page, summarize_observation
from tools.opportunity_pipeline import (
    build_default_llm_review as _build_default_llm_review,
    discover_opportunities_via_api as _discover_opportunities_via_api,
)
from tools.stealth.captcha import solve_login_recaptcha
from reddit_action_messages import post_upvote_result_message
from reddit_login_state import reddit_login_state
from proxy_config import captcha_proxy_config
from session_store import delete_session, session_exists
from browser_manager import LazyBrowser, active_profile_session_id


PERSONA_SUBREDDITS = [
    "SaaS",
    "marketing",
    "sales",
    "startups",
    "Entrepreneur",
    "smallbusiness",
    "B2BMarketing",
    "digital_marketing",
    "Emailmarketing",
    "GrowthHacking",
    "ProductManagement",
    "SideProject",
    "indiehackers",
    "webdev",
    "devops",
    "sysadmin",
]
DEFAULT_WARMUP_SUBREDDITS = ["SaaS", "marketing", "sales", "startups", "ProductManagement", "SideProject"]
PERSONA_DISCOVERY_QUERIES = [
    "SaaS founders",
    "B2B sales",
    "startup marketing",
    "growth marketing",
    "product management",
    "business systems",
    "automation tools",
    "tech operators",
]
PERSONA_RELEVANCE_TERMS = {
    "saas",
    "software",
    "startup",
    "founder",
    "entrepreneur",
    "sales",
    "marketing",
    "growth",
    "b2b",
    "product",
    "systems",
    "automation",
    "ops",
    "devops",
    "sysadmin",
    "webdev",
    "business",
}
WARMUP_START_LOCAL = time(9, 0)
WARMUP_END_LOCAL = time(21, 30)
ENFORCE_WARMUP_LOCAL_TIME = os.getenv("WARMUP_ENFORCE_LOCAL_TIME", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

WARMUP_PERSONA = {
    "name": "Tech, SaaS, sales, marketing, and systems enthusiast",
    "voice": (
        "short, casual operator vibe; curious about SaaS, GTM, automation, workflows, and practical systems; "
        "uses light Reddit phrasing like 'imo', 'tbh', 'ngl', 'solid point', or 'been there' only when it fits"
    ),
    "rules": [
        "Write 1-3 short sentences.",
        "No polished essay structure.",
        "No fake personal claims or made-up results.",
        "Ask a useful follow-up when the thread needs more context.",
        "Do not post automatically; user approval is required.",
    ],
}


def _proxy_config(proxy_url: Optional[str]) -> Optional[dict]:
    return captcha_proxy_config(proxy_url)


def _captcha_config_for_login() -> Optional[dict]:
    enabled = os.getenv("CAPTCHA_SOLVE_ON_LOGIN", "").strip().lower() in {"1", "true", "yes", "on"}
    api_key = os.getenv("CAPTCHA_API_KEY")
    if not enabled or not api_key:
        return None
    return {
        "api_key": api_key,
        "provider": os.getenv("CAPTCHA_PROVIDER", "2captcha"),
    }


def _normalize_subreddits(subreddits: str = "") -> list[str]:
    allowed = {name.lower(): name for name in PERSONA_SUBREDDITS}
    parsed = [s.strip().strip("/").removeprefix("r/") for s in subreddits.split(",") if s.strip()]
    selected = [allowed[s.lower()] for s in parsed if s.lower() in allowed]
    return selected or DEFAULT_WARMUP_SUBREDDITS.copy()


def _subreddit_from_reddit_url(url: str) -> Optional[str]:
    match = re.search(r"reddit\.com/r/([^/]+)/", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _allowed_comment_target(post_url: str) -> tuple[bool, str]:
    subreddit = _subreddit_from_reddit_url(post_url)
    if not subreddit:
        return False, "I can only comment when the Reddit URL includes a subreddit path like /r/SaaS/."
    allowed = {name.lower(): name for name in PERSONA_SUBREDDITS}
    if subreddit.lower() not in allowed:
        allowed_text = ", ".join(f"r/{name}" for name in DEFAULT_WARMUP_SUBREDDITS)
        return False, (
            f"I am configured to comment only in persona-matched tech/SaaS/sales/marketing/systems communities. "
            f"Core examples: {allowed_text}. This URL is for r/{subreddit}."
        )
    return True, allowed[subreddit.lower()]


async def _detect_proxy_time_context(page) -> dict:
    """Best-effort proxy-local time detection without exposing proxy credentials."""
    detected = await page.evaluate("""async () => {
        const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone || null;
        const endpoints = [
            'https://ipapi.co/json/',
            'http://ip-api.com/json/?fields=status,country,regionName,city,timezone,query'
        ];
        for (const url of endpoints) {
            try {
                const response = await fetch(url, { cache: 'no-store' });
                if (!response.ok) continue;
                const data = await response.json();
                const timezone = data.timezone || data.time_zone || null;
                if (timezone) {
                    return {
                        timezone,
                        country: data.country_name || data.country || null,
                        region: data.region || data.regionName || null,
                        city: data.city || null,
                        ip: data.ip || data.query || null,
                        source: url,
                        browserTimezone,
                    };
                }
            } catch (_) {}
        }
        return { timezone: browserTimezone, source: 'browser', browserTimezone };
    }""")

    timezone = detected.get("timezone") or "UTC"
    try:
        local_now = datetime.now(ZoneInfo(timezone))
    except Exception:
        timezone = "UTC"
        local_now = datetime.now(dt_timezone.utc)

    detected["timezone"] = timezone
    detected["local_now"] = local_now.isoformat(timespec="seconds")
    detected["local_day"] = local_now.strftime("%A")
    detected["local_time"] = local_now.strftime("%H:%M")
    return detected


def _next_warmup_start(now: datetime) -> datetime:
    candidate = datetime.combine(now.date(), WARMUP_START_LOCAL, tzinfo=now.tzinfo)
    if now.time() < WARMUP_START_LOCAL:
        return candidate
    return candidate + timedelta(days=1)


def _warmup_window_status(time_context: dict) -> dict:
    timezone = time_context.get("timezone") or "UTC"
    now = datetime.fromisoformat(time_context["local_now"])
    in_window = WARMUP_START_LOCAL <= now.time() <= WARMUP_END_LOCAL
    next_start = None if in_window else _next_warmup_start(now)
    return {
        "timezone": timezone,
        "local_now": time_context.get("local_now"),
        "local_day": time_context.get("local_day"),
        "local_time": time_context.get("local_time"),
        "in_warmup_window": in_window,
        "window": f"{WARMUP_START_LOCAL.strftime('%H:%M')}-{WARMUP_END_LOCAL.strftime('%H:%M')}",
        "next_warmup_start": next_start.isoformat(timespec="seconds") if next_start else None,
    }


async def _discover_persona_subreddit_candidates(page, max_results: int = 8, query: str = "") -> list[dict]:
    queries = [query.strip()] if query.strip() else PERSONA_DISCOVERY_QUERIES
    limit = max(1, min(int(max_results or 8), 25))
    candidates: dict[str, dict] = {}

    for search_text in queries:
        if len(candidates) >= limit:
            break
        url = f"https://www.reddit.com/search/?q={quote_plus(search_text)}&type=sr"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await page.wait_for_timeout(1500)
            found = await page.evaluate("""(searchText) => {
                const seen = new Set();
                const results = [];
                const anchors = [...document.querySelectorAll('a[href^="/r/"], a[href*="reddit.com/r/"]')];
                for (const anchor of anchors) {
                    const href = anchor.href || anchor.getAttribute('href') || '';
                    const match = href.match(/\\/r\\/([^\\/\\?#]+)/i);
                    if (!match) continue;
                    const name = decodeURIComponent(match[1]);
                    if (!name || seen.has(name.toLowerCase())) continue;
                    seen.add(name.toLowerCase());
                    const container = anchor.closest('search-telemetry-tracker, faceplate-tracker, article, div');
                    const text = ((container && container.innerText) || anchor.innerText || '').replace(/\\s+/g, ' ').trim();
                    results.push({
                        name,
                        url: `https://www.reddit.com/r/${name}/`,
                        context: text.slice(0, 260),
                        query: searchText,
                    });
                    if (results.length >= 10) break;
                }
                return results;
            }""", search_text)

            for item in found:
                name = item.get("name", "").strip()
                if not name:
                    continue
                haystack = f"{name} {item.get('context', '')}".lower()
                if any(term in haystack for term in PERSONA_RELEVANCE_TERMS):
                    candidates.setdefault(name.lower(), item)
                    if len(candidates) >= limit:
                        break
        except Exception as exc:
            candidates.setdefault(f"error-{search_text}".lower(), {
                "name": search_text,
                "error": str(exc),
            })

    return list(candidates.values())[:limit]


def is_reddit_action_request(text: str) -> bool:
    lowered = text.lower()
    if is_opportunity_discovery_request(text):
        return True
    return any(
        phrase in lowered
        for phrase in (
            "reddit",
            "login",
            "log in",
            "comment",
            "upvote",
            "comment upvote",
            "post",
            "join subreddit",
            "subreddit",
            "open reddit",
            "warmup",
            "warm up",
            "karma",
            "autonomous",
            "persona",
        )
    )


def is_opportunity_discovery_request(text: str) -> bool:
    """Detect requests to find Reddit promotion opportunities for a SaaS product."""
    lowered = text.lower()
    patterns = (
        r"\bfind\b.*\breddit\b.*\b(leads?|place|opportunit|link|post|comment|where)\b",
        r"\bwhere\b.*\bpromote\b",
        r"\breddit\b.*\bopportunit",
        r"\bfind\b.*\bopportunit\w*\b.*\b(reddit|promot\w*|saas|product|startup)\b",
        r"\bpromot\w+\b.*\breddit\b",
        r"\breddit\b.*\bpromot",
        r"\bfind\b.*\b(100|50|200|\d+)\b.*\breddit\b",
        r"\breddit\b.*\bsaas\b.*\bpromot",
        r"\bsaas\b.*\bpromot\w+\b.*\breddit\b",
        r"\bwhere can i (mention|post|comment|promote)\b",
        r"\bfind posts?\b.*\bfor my\b",
        r"\breddit leads?\b",
    )
    return any(re.search(p, lowered) for p in patterns)


async def is_reddit_logged_in(page, navigate: bool = True, expected_username: str = "") -> bool:
    """Detect whether the current Reddit browser context is logged in."""
    try:
        state = await reddit_login_state(
            page,
            expected_username=expected_username,
            navigate=navigate,
        )
        return bool(state.get("logged_in"))
    except Exception:
        return False


async def ensure_reddit_logged_in(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    proxy_url: Optional[str] = None,
) -> tuple[bool, str]:
    """Ensure Reddit is open and logged in, returning a user-facing status."""
    page = await lazy.get_page()
    session_id = active_profile_session_id(account_id)

    if await is_reddit_logged_in(page, expected_username=username):
        await lazy.persist_session()
        return True, "Already logged in to Reddit."

    result = await login(
        page=page,
        account_id=account_id,
        username=username,
        password=password,
        captcha_config=_captcha_config_for_login(),
        proxy_config=_proxy_config(proxy_url),
    )

    verified_after_login = await is_reddit_logged_in(
        page,
        navigate=not result["success"],
        expected_username=username,
    )
    if result["success"] and verified_after_login:
        await lazy.persist_session()
        return True, "Logged in to Reddit and saved the session."

    if verified_after_login:
        await lazy.persist_session()
        return True, "Logged in to Reddit and saved the session."

    delete_session(session_id)
    if result["success"]:
        return False, "Login submitted, but Reddit session could not be verified. Please try login_reddit again."
    return False, f"Login failed: {result['error'] or 'Reddit session not verified'}"


async def open_reddit_home(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    proxy_url: Optional[str] = None,
) -> str:
    """Open Reddit home, ensure login, and save the session when present."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status
    page = await lazy.get_page()
    await page.goto("https://www.reddit.com", wait_until="domcontentloaded", timeout=60_000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8_000)
    except Exception:
        pass
    await lazy.persist_session()
    return "Reddit is open and the account is logged in."


async def comment_on_reddit_post(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    post_url: str,
    text: str,
    proxy_url: Optional[str] = None,
) -> str:
    """Systematic comment workflow: open/login, warm up, navigate, comment, save."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status

    allowed, reason = _allowed_comment_target(post_url)
    if not allowed:
        return reason

    page = await lazy.get_page()
    result = await comment(page=page, account_id=account_id, post_url=post_url, text=text)
    if result["success"]:
        await lazy.persist_session()
        verified = result["data"].get("verified", False)
        return "Comment posted and verified." if verified else "Comment submitted, but Reddit did not expose a clear verification signal."
    return f"Comment failed: {result['error']}"


async def upvote_reddit_comment(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    comment_url: str = "",
    comment_fullname: str = "",
    post_url: str = "",
    proxy_url: Optional[str] = None,
) -> str:
    """Systematic comment-upvote workflow: open/login, warm up, upvote comment, save."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status

    page = await lazy.get_page()
    result = await comment_upvote(
        page=page,
        account_id=account_id,
        comment_url=comment_url,
        comment_fullname=comment_fullname,
        post_url=post_url,
    )
    if result["success"]:
        await lazy.persist_session()
        return f"Comment upvote successful. Data: {result['data']}"
    return f"Comment upvote failed: {result['error']}"


async def upvote_reddit_post(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    post_url: str,
    proxy_url: Optional[str] = None,
) -> str:
    """Systematic post-upvote workflow: open/login, upvote visible UI, save."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status

    page = await lazy.get_page()
    result = await upvote(
        page=page,
        account_id=account_id,
        post_url=post_url,
    )
    if result["success"]:
        await lazy.persist_session()
        return post_upvote_result_message(result["data"])
    return f"Post upvote failed: {result['error']}"


def make_tools(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    proxy_url: Optional[str] = None,
    confirmation_state: Optional[dict] = None,
):
    """Return list of LangGraph tools. Browser launches on first tool call."""
    confirmation_state = confirmation_state if confirmation_state is not None else {
        "pending": None,
        "approved": False,
    }

    async def _is_logged_in(page) -> bool:
        return await is_reddit_logged_in(page, expected_username=username)

    async def _ensure_logged_in(page) -> Optional[str]:
        ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
        if ok:
            return None
        delete_session(active_profile_session_id(account_id))
        return status

    def _confirmation_tool_hint(action: str, details: str = "") -> Optional[str]:
        text = f"{action} {details}".lower()
        has_upvote = "upvote" in text or "up vote" in text
        has_comment = "comment" in text
        has_post = "post" in text or "thread" in text
        if has_upvote and has_comment:
            return "upvote_comment"
        if has_upvote and has_post:
            return "upvote_post"
        if "reply" in text:
            return "reply_to_reddit_comment"
        if "join" in text and "subreddit" in text:
            return "join_subreddit_tool"
        if ("submit" in text or "create" in text or "text post" in text) and has_post:
            return "submit_text_post"
        if has_comment:
            return "comment_on_post"
        return None

    def _confirmation_gate(tool_name: str) -> Optional[str]:
        pending = confirmation_state.get("pending")
        if not pending:
            return None
        if confirmation_state.get("approved"):
            expected_tool = pending.get("tool_name")
            if expected_tool and expected_tool != tool_name:
                confirmation_state["approved"] = False
                return (
                    "[CONFIRMATION REQUIRED] The current approval is for a different action, "
                    "so I did not run this tool.\n"
                    f"Approved action: {pending.get('action', 'unknown')}\n"
                    f"Approved tool: {expected_tool}\n"
                    f"Attempted tool: {tool_name}\n"
                    "STOP: ask the user to confirm the exact action again before calling "
                    "another state-changing tool."
                )
            confirmation_state["pending"] = None
            confirmation_state["approved"] = False
            return None
        return (
            "[CONFIRMATION REQUIRED] A pending state-changing action must be confirmed "
            f"before `{tool_name}` can run.\n"
            f"Pending action: {pending.get('action', 'unknown')}\n"
            f"Details: {pending.get('details', '')}\n"
            "STOP: do not call another state-changing tool in this turn. "
            "Ask the user to reply 'yes' to proceed or 'no' to cancel."
        )

    @tool
    async def check_session() -> str:
        """Check whether a saved login session exists and is still active on Reddit."""
        page = await lazy.get_page()
        session_id = active_profile_session_id(account_id)
        try:
            logged_in = await _is_logged_in(page)
            if logged_in:
                await lazy.persist_session()
                return "Session exists: True. Already logged in on Reddit. No login needed."
            if not session_exists(session_id):
                return "Session exists: False. Must call login_reddit first."
            delete_session(session_id)
            return "Session exists: False. Session expired. Must call login_reddit."
        except Exception as e:
            return f"Session check failed: {e}. Call login_reddit to be safe."

    @tool
    async def login_reddit() -> str:
        """Login to Reddit using the account credentials. Call this if no session exists."""
        ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
        return status if ok else status

    @tool
    async def browse_reddit(mode: str = "simulate_reading", subreddit: str = "") -> str:
        """
        Simulate human browsing on Reddit to warm up the account before acting.
        mode: 'simulate_reading' (default) | 'browse_random' | 'browse_subreddit' | 'find_posts'
        subreddit: Optional subreddit name without r/ when mode='browse_subreddit'.
        Always call this after login and before commenting or upvoting.
        """
        page = await lazy.get_page()
        result = await browse(
            page=page,
            account_id=account_id,
            mode=mode,
            subreddit=subreddit or None,
        )
        if result["success"]:
            return f"Browsing complete. Data: {result['data']}"
        return f"Browse failed: {result['error']}"

    @tool
    async def warmup_reddit(
        subreddits: str = "",
        duration_minutes: float = 5.0,
        force: bool = False,
        auto_discover: bool = True,
    ) -> str:
        """
        Run a browsing-only warm-up session across business subreddits during proxy-local active hours.
        Args:
            subreddits: Comma-separated subreddit names without r/. Only persona-matched tech, SaaS, sales, marketing, systems, startup, and product communities are allowed.
            duration_minutes: Target warm-up time in minutes.
            force: If true, run even outside the proxy-local warm-up window.
            auto_discover: If true, search Reddit for adjacent persona communities and browse the relevant candidates.
        This tool only browses/reads. It does not post, comment, vote, or farm karma.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error

        parsed = _normalize_subreddits(subreddits)
        time_context = await _detect_proxy_time_context(page)
        window = _warmup_window_status(time_context)

        if ENFORCE_WARMUP_LOCAL_TIME and not force and not window["in_warmup_window"]:
            return (
                "Warm-up not started because it is outside the proxy-local active window. "
                f"Proxy/browser timezone: {window['timezone']}; local time: {window['local_day']} "
                f"{window['local_time']}; window: {window['window']}; next start: "
                f"{window['next_warmup_start']}. Subreddits: {', '.join('r/' + s for s in parsed)}."
            )

        discovered: list[dict] = []
        if auto_discover:
            discovered = [
                item
                for item in await _discover_persona_subreddit_candidates(page, max_results=6)
                if not item.get("error")
            ]
            known = {name.lower() for name in parsed}
            for item in discovered:
                name = item.get("name", "").strip()
                if name and name.lower() not in known:
                    parsed.append(name)
                    known.add(name.lower())

        safe_duration = max(1.0, min(float(duration_minutes or 5.0), 30.0))
        result = await warmup_browsing_session(
            page=page,
            account_id=account_id,
            subreddits=parsed,
            duration_minutes=safe_duration,
        )
        if result["success"]:
            await lazy.persist_session()
            data = result["data"]
            data["proxy_time"] = window
            data["persona"] = WARMUP_PERSONA
            data["auto_discovered_subreddits"] = [
                {"name": item.get("name"), "query": item.get("query"), "url": item.get("url")}
                for item in discovered
            ]
            return f"Warm-up browsing complete. Data: {data}"
        return f"Warm-up browsing failed: {result['error']}"

    @tool
    async def find_warmup_comment_opportunities(subreddits: str = "", max_posts: int = 8) -> str:
        """
        Find active posts where a human-approved helpful comment might make sense.
        Args:
            subreddits: Comma-separated subreddit names without r/. Only persona-matched tech, SaaS, sales, marketing, systems, startup, and product communities are allowed.
            max_posts: Maximum candidate posts to return.
        This tool does not submit comments. Use it to draft comments for user approval.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error

        parsed = _normalize_subreddits(subreddits)

        limit = max(1, min(int(max_posts or 8), 20))
        per_sub_limit = max(1, (limit + len(parsed) - 1) // len(parsed))
        candidates: list[dict] = []

        for subreddit_name in parsed[:8]:
            if len(candidates) >= limit:
                break
            url = f"https://www.reddit.com/r/{subreddit_name}/new/"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8_000)
                except Exception:
                    pass
                await page.wait_for_timeout(1500)
                found = await page.evaluate("""(subredditName) => {
                    const seen = new Set();
                    const results = [];
                    const posts = [...document.querySelectorAll('shreddit-post, article, [data-testid="post-container"]')];
                    for (const post of posts) {
                        const anchor = post.querySelector('a[href*="/comments/"]');
                        if (!anchor || !anchor.href || seen.has(anchor.href)) continue;
                        seen.add(anchor.href);
                        const title =
                            post.getAttribute('post-title') ||
                            anchor.innerText ||
                            anchor.getAttribute('aria-label') ||
                            '';
                        const text = (post.innerText || '').replace(/\\s+/g, ' ').trim();
                        if (!title.trim() && !text) continue;
                        results.push({
                            subreddit: subredditName,
                            title: title.trim().slice(0, 180) || text.slice(0, 180),
                            url: anchor.href,
                            context: text.slice(0, 500),
                        });
                        if (results.length >= 6) break;
                    }
                    return results;
                }""", subreddit_name)
                candidates.extend(found[:per_sub_limit])
            except Exception as exc:
                candidates.append({
                    "subreddit": subreddit_name,
                    "error": str(exc),
                })

        candidates = candidates[:limit]
        if not candidates:
            return "No warm-up comment opportunities found."

        lines = [
            "Warm-up comment opportunities. Draft comments only; do not post until the user approves.",
            f"Persona: {WARMUP_PERSONA['name']} - {WARMUP_PERSONA['voice']}",
            "Draft style: 1-3 short sentences, casual Reddit wording, no fake claims.",
        ]
        for idx, item in enumerate(candidates, start=1):
            if item.get("error"):
                lines.append(f"{idx}. r/{item['subreddit']} failed: {item['error']}")
                continue
            lines.append(
                f"{idx}. r/{item['subreddit']} - {item['title']}\n"
                f"URL: {item['url']}\n"
                f"Context: {item.get('context', '')[:300]}"
            )
        await lazy.persist_session()
        return "\n".join(lines)

    @tool
    async def search_reddit_posts(query: str, subreddit: str = "", sort: str = "relevance") -> str:
        """
        Search Reddit for posts and return candidate post URLs.
        Args:
            query: Search text, usually the title or keywords the user gave.
            subreddit: Optional subreddit name without r/ to search inside one subreddit.
            sort: relevance | hot | top | new | comments.
        Use this when the user describes a post but does not provide a full URL.
        """
        page = await lazy.get_page()
        try:
            safe_sort = sort if sort in {"relevance", "hot", "top", "new", "comments"} else "relevance"
            base = f"https://www.reddit.com/r/{subreddit}/search/" if subreddit else "https://www.reddit.com/search/"
            url = f"{base}?q={quote_plus(query)}&type=link&sort={safe_sort}"
            if subreddit:
                url += "&restrict_sr=1"

            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await page.wait_for_timeout(1500)

            posts = await page.evaluate("""() => {
                const seen = new Set();
                const results = [];
                const anchors = [...document.querySelectorAll('a[href*="/comments/"]')];
                for (const a of anchors) {
                    const href = a.href;
                    if (!href || seen.has(href)) continue;
                    seen.add(href);
                    const title = (a.innerText || a.getAttribute('aria-label') || '').trim();
                    const container = a.closest('search-telemetry-tracker, shreddit-post, article, [data-testid="post-container"]');
                    const text = container ? (container.innerText || '').trim() : title;
                    results.push({ title: title || text.slice(0, 120), url: href, context: text.slice(0, 300) });
                    if (results.length >= 8) break;
                }
                return results;
            }""")

            if not posts:
                return "No Reddit posts found for that search."

            lines = ["Search results:"]
            for i, item in enumerate(posts, start=1):
                title = (item.get("title") or "Untitled").replace("\n", " ")[:140]
                lines.append(f"{i}. {title}\nURL: {item.get('url')}")
            return "\n".join(lines)
        except Exception as e:
            return f"Search failed: {e}"

    @tool
    async def navigate_to_post(post_url: str) -> str:
        """Navigate the browser to a specific Reddit post URL."""
        page = await lazy.get_page()
        try:
            await page.goto(post_url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(2000)
            title = await page.title()
            return f"Navigated to post. Page title: {title}"
        except Exception as e:
            return f"Navigation failed: {e}"

    @tool
    async def comment_on_post(post_url: str, text: str) -> str:
        """
        Post a comment on a Reddit thread.
        Args:
            post_url: Full Reddit post URL.
            text: The comment text to post.
        """
        blocked = _confirmation_gate("comment_on_post")
        if blocked:
            return blocked
        return await comment_on_reddit_post(
            lazy=lazy,
            account_id=account_id,
            username=username,
            password=password,
            post_url=post_url,
            text=text,
            proxy_url=proxy_url,
        )

    @tool
    async def submit_text_post(subreddit: str, title: str, body: str) -> str:
        """
        Create a text post in a subreddit.
        Args:
            subreddit: Subreddit name without r/ prefix.
            title: Post title.
            body: Post body text.
        """
        blocked = _confirmation_gate("submit_text_post")
        if blocked:
            return blocked
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await post(
            page=page,
            account_id=account_id,
            subreddit=subreddit,
            title=title,
            body=body,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Post submitted. URL: {result['data'].get('post_url', '')}"
        return f"Post failed: {result['error']}"

    @tool
    async def reply_to_reddit_comment(comment_fullname: str, post_url: str, text: str) -> str:
        """
        Reply to a specific Reddit comment.
        Args:
            comment_fullname: Parent comment fullname, e.g. t1_abc123.
            post_url: Full Reddit post URL containing the comment.
            text: Reply text.
        """
        blocked = _confirmation_gate("reply_to_reddit_comment")
        if blocked:
            return blocked
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        allowed, reason = _allowed_comment_target(post_url)
        if not allowed:
            return reason
        result = await reply(
            page=page,
            account_id=account_id,
            comment_fullname=comment_fullname,
            post_url=post_url,
            text=text,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Reply posted. Data: {result['data']}"
        return f"Reply failed: {result['error']}"

    @tool
    async def upvote_post(post_url: str) -> str:
        """
        Upvote a Reddit post.
        Args:
            post_url: Full Reddit post URL.
        """
        blocked = _confirmation_gate("upvote_post")
        if blocked:
            return blocked
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await upvote(
            page=page,
            account_id=account_id,
            post_url=post_url,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Upvote successful. Data: {result['data']}"
        return f"Upvote failed: {result['error']}"

    @tool
    async def upvote_comment(comment_url: str = "", comment_fullname: str = "", post_url: str = "") -> str:
        """
        Upvote a Reddit comment.
        Args:
            comment_url: Full Reddit comment permalink, preferred when available.
            comment_fullname: Optional Reddit comment fullname, e.g. t1_abc123.
            post_url: Optional containing post URL if comment_fullname is supplied.
        """
        blocked = _confirmation_gate("upvote_comment")
        if blocked:
            return blocked
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await comment_upvote(
            page=page,
            account_id=account_id,
            comment_url=comment_url,
            comment_fullname=comment_fullname,
            post_url=post_url,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Comment upvote successful. Data: {result['data']}"
        return f"Comment upvote failed: {result['error']}"

    @tool
    async def join_subreddit_tool(subreddit: str) -> str:
        """
        Join a subreddit. Required before commenting on subreddits with Crowd Control enabled.
        Args:
            subreddit: Subreddit name without r/ prefix (e.g. 'python').
        """
        blocked = _confirmation_gate("join_subreddit_tool")
        if blocked:
            return blocked
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await join_subreddit(
            page=page,
            account_id=account_id,
            subreddit=subreddit,
        )
        if result["success"]:
            await lazy.persist_session()
            already = result["data"].get("already_joined", False)
            return f"{'Already a member.' if already else 'Joined successfully.'}"
        return f"Join failed: {result['error']}"

    @tool
    async def observe_current_page(include_screenshot: bool = False) -> str:
        """
        PRIMARY observation tool. Use this before clicking, submitting, or voting to
        ground decisions in real visible UI state, and again afterward to verify the result.
        Returns: URL, title, visible text, interactive elements (role/name/bbox/state/selector),
        detected overlays (modal/login_wall/captcha/error/rate_limit), and optional screenshot.
        Prefer this over get_accessibility_snapshot / get_page_text / take_screenshot for
        most tasks — those tools remain available for targeted follow-up queries.
        Args:
            include_screenshot: Set True only when a visual check is needed (e.g. CAPTCHA).
        """
        page = await lazy.get_page()
        try:
            obs = await _observe_page(page, include_screenshot=include_screenshot)
            summary = summarize_observation(obs, include_elements=True)
            if obs.get("screenshot_b64"):
                summary += f"\n[screenshot_b64 length={len(obs['screenshot_b64'])}]"
            return summary
        except Exception as e:
            return f"observe_page failed: {e}"

    @tool
    async def request_confirmation(action: str, details: str) -> str:
        """
        Return a confirmation question to the user before executing a state-changing action.
        Use when the action is ambiguous or the request was indirect (e.g. searching then acting).
        Do NOT use when: user gave a precise direct request like "upvote this exact URL" and
        the page observation matches unambiguously.
        Args:
            action: The action about to be performed (e.g. "upvote comment", "submit comment").
            details: What was found and what will happen (e.g. "Found upvote button for comment
                     t1_abc123, currently not upvoted, belongs to target post.").
        Returns a confirmation message for the user to answer yes/no.
        """
        confirmation_state["pending"] = {
            "action": action,
            "details": details,
            "tool_name": _confirmation_tool_hint(action, details),
        }
        confirmation_state["approved"] = False
        return (
            f"[CONFIRMATION REQUIRED] {details}\n"
            f"Ready to: {action}\n"
            "Reply 'yes' to proceed or 'no' to cancel."
        )

    @tool
    async def get_accessibility_snapshot() -> str:
        """
        Return the raw Playwright accessibility tree of the current page.
        Useful for targeted inspection of specific interactive controls.
        For a full structured snapshot including interactive elements and overlays,
        prefer observe_current_page instead.
        """
        page = await lazy.get_page()
        try:
            snapshot = await page.accessibility.snapshot()
            if snapshot is None:
                return "Accessibility snapshot unavailable."
            return str(snapshot)
        except Exception as e:
            return f"Snapshot failed: {e}"

    @tool
    async def get_page_text() -> str:
        """
        Return all visible text content from the current page as plain text.
        Useful for reading post body or comment text in full.
        For structured page state (buttons, overlays, URL), prefer observe_current_page.
        """
        page = await lazy.get_page()
        try:
            text = await page.evaluate("() => document.body.innerText")
            return text[:8000] if len(text) > 8000 else text
        except Exception as e:
            return f"get_page_text failed: {e}"

    @tool
    async def take_screenshot() -> str:
        """
        Take a screenshot of the current page and return it as a base64 PNG string.
        Use only when visual inspection is strictly necessary (e.g. visual CAPTCHA or
        layout verification). observe_current_page with include_screenshot=True is preferred
        as it also captures DOM state alongside the image.
        """
        page = await lazy.get_page()
        try:
            png_bytes = await page.screenshot(full_page=False)
            return base64.b64encode(png_bytes).decode()
        except Exception as e:
            return f"Screenshot failed: {e}"

    @tool
    async def solve_captcha() -> str:
        """
        Solve a reCAPTCHA on the current page using the configured CAPTCHA provider.
        Requires CAPTCHA_API_KEY and CAPTCHA_PROVIDER env vars.
        Call this when login or comment action is blocked by a CAPTCHA.
        """
        page = await lazy.get_page()
        captcha_config = {
            "api_key": os.getenv("CAPTCHA_API_KEY"),
            "provider": os.getenv("CAPTCHA_PROVIDER", "2captcha"),
        }
        proxy_config = _proxy_config(proxy_url)
        try:
            token = await solve_login_recaptcha(
                page=page,
                account_id=account_id,
                captcha_config=captcha_config,
                proxy_config=proxy_config,
            )
            if token:
                return f"CAPTCHA solved. Token length: {len(token)}"
            return "CAPTCHA solve returned no token. Check CAPTCHA_API_KEY."
        except Exception as e:
            return f"CAPTCHA solve failed: {e}"

    @tool
    async def discover_reddit_opportunities(
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
        opportunity_types: str = "",
        product_url: str = "",
    ) -> str:
        """
        Discover Reddit posts/comments where a SaaS product can be naturally and helpfully mentioned.

        This is a READ-ONLY discovery tool. It does not post, comment, upvote, or spam.
        Use it when the user asks to find Reddit promotion opportunities, leads, or places
        to mention their product.

        Args:
            product_name: Name of the SaaS product (e.g. "Acme CRM").
            product_description: One-paragraph description of what the product does.
            target_customer: Who the product is for (e.g. "B2B sales teams", "solo founders").
            pain_points: Comma-separated list of problems the product solves.
            use_cases: Comma-separated list of main use cases.
            keywords: Comma-separated list of search keywords related to the product/problem.
            competitor_names: Optional comma-separated competitor names for "alternatives" queries.
            excluded_subreddits: Optional comma-separated subreddits to skip.
            target_link_count: How many verified results to return (default 100).
            max_age_days: Exclude posts/comments older than this many days (default 730 = 2 years).
            recent_days: Posts within this many days go into the 'recent' category (default 7).

        Returns JSON with three categories of results plus a coverage report:
            recent_posts_comments — posts/comments from the last `recent_days` days
            high_engagement_posts_comments — high-score or high-comment-count threads
            high_google_search_posts_comments — Reddit threads ranking on Google for SaaS keywords

        Each result includes url, type, subreddit, title, scores, reason, suggested_angle, and status.

        Backend pipeline (no Playwright required):
            1. Fetch lightweight Reddit post metadata via the search JSON endpoint,
               with per-opportunity-type filters (recent uses t=week sort=new,
               high_engagement uses t=year sort=top, high_google_search uses t=all sort=top).
            2. Run a match-filter layer on titles to drop off-topic posts before any
               expensive detail fetch.
            3. Fetch full body + top comments only for the surviving URLs.
            4. Send the enriched payload to the LLM for the final fit decision.
        """
        try:
            llm_review = _build_default_llm_review()
            result = await _discover_opportunities_via_api(
                product_name=product_name,
                product_description=product_description,
                target_customer=target_customer,
                pain_points=pain_points,
                use_cases=use_cases,
                keywords=keywords,
                competitor_names=competitor_names,
                excluded_subreddits=excluded_subreddits,
                target_link_count=target_link_count,
                max_age_days=max_age_days,
                recent_days=recent_days,
                opportunity_types=opportunity_types or None,
                product_url=product_url,
                llm_review=llm_review,
            )
            return json.dumps(result, indent=2, ensure_ascii=False)
        except Exception as exc:
            return json.dumps({"error": str(exc), "result": None})

    return [
        check_session,
        login_reddit,
        browse_reddit,
        warmup_reddit,
        find_warmup_comment_opportunities,
        search_reddit_posts,
        navigate_to_post,
        comment_on_post,
        submit_text_post,
        reply_to_reddit_comment,
        upvote_post,
        upvote_comment,
        join_subreddit_tool,
        observe_current_page,
        request_confirmation,
        get_accessibility_snapshot,
        get_page_text,
        take_screenshot,
        solve_captcha,
        discover_reddit_opportunities,
    ]
