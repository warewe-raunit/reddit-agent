"""
tools/comment_upvote_tool.py - Reddit comment upvote tool for AI agents.

Uses the same Reddit vote endpoint as post upvotes:
    POST https://oauth.reddit.com/api/vote

The difference is the thing fullname:
    posts:    t3_<post_id>
    comments: t1_<comment_id>
"""

from __future__ import annotations

import asyncio
import os
import re
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, ensure_token_captured,
    safe_proxy_id, _ok, _fail, _ms,
)
from tools.upvote_tool import (
    _oauth_headers,
    _reddit_user_agent,
    _token_v2_candidate,
    _verify_oauth_token,
)

logger = structlog.get_logger(__name__)


def _comment_fullname(comment_fullname: str = "", comment_url: str = "") -> Optional[str]:
    candidate = (comment_fullname or "").strip()
    if candidate:
        return candidate if candidate.startswith("t1_") else f"t1_{candidate}"

    url = comment_url.strip()
    patterns = [
        r"/comments/[^/]+/[^/]+/([a-z0-9_]+)/?",
        r"[?&]comment=([a-z0-9_]+)",
        r"#t1_([a-z0-9_]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if match:
            comment_id = match.group(1)
            return comment_id if comment_id.startswith("t1_") else f"t1_{comment_id}"
    return None


async def _verify_comment_vote_state(page: Page, token: str, user_agent: str, comment_id: str) -> Optional[bool]:
    response = await page.request.get(
        f"https://oauth.reddit.com/api/info?id={comment_id}",
        headers=await _oauth_headers(token, user_agent),
    )
    if response.status != 200:
        return None
    payload = await response.json()
    children = payload.get("data", {}).get("children", [])
    if not children:
        return None
    return children[0].get("data", {}).get("likes") is True


async def comment_upvote(
    page: Page,
    account_id: str,
    comment_url: str = "",
    comment_fullname: str = "",
    post_url: str = "",
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Upvote a Reddit comment.

    Args:
        page: Active logged-in Reddit Playwright page.
        account_id: Unique account identifier / Reddit username.
        comment_url: Full Reddit comment permalink.
        comment_fullname: Optional comment fullname, e.g. t1_abc123.
        post_url: Optional containing post URL for browser navigation.
    """
    log = logger.bind(account_id=account_id, action="COMMENT_UPVOTE")
    start_ms = _ms()

    _early_tokens: list[str] = []

    def _early_listener(request) -> None:
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer ") and "reddit.com" in request.url:
            token = auth[len("Bearer "):]
            if len(token) > 20:
                _early_tokens.append(token)

    page.on("request", _early_listener)

    try:
        target_url = comment_url or post_url
        if target_url:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
            if _early_tokens and not getattr(page, "_reddit_bearer_token", None):
                setattr(page, "_reddit_bearer_token", _early_tokens[-1])
            await simulate_reading(page, account_id)
            await _delay(account_id, 0.8, 1.8)

        comment_id = _comment_fullname(comment_fullname=comment_fullname, comment_url=comment_url)
        if not comment_id:
            raise RuntimeError("comment id not found; pass comment_fullname like t1_abc123 or a full comment permalink")

        token = (os.getenv("REDDIT_OAUTH_ACCESS_TOKEN") or "").strip()
        if token:
            setattr(page, "_reddit_bearer_token", token)
        else:
            token = await ensure_token_captured(page, log)
        if not token:
            token = await _token_v2_candidate(page)

        if not token:
            raise RuntimeError("no Reddit OAuth-compatible token available for comment upvote")

        user_agent = _reddit_user_agent(account_id)
        if not await _verify_oauth_token(page, token, user_agent):
            raise RuntimeError("oauth token rejected by /api/v1/me")

        vote_response = await page.request.post(
            "https://oauth.reddit.com/api/vote",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {token}",
                "User-Agent": user_agent,
            },
            data=f"id={comment_id}&dir=1&rank=2",
        )
        body = (await vote_response.text())[:300]
        if vote_response.status != 200:
            raise RuntimeError(f"comment vote rejected: status={vote_response.status}, body={body}")

        await asyncio.sleep(2)
        server_verified = await _verify_comment_vote_state(page, token, user_agent, comment_id) is True
        if not server_verified:
            raise RuntimeError("comment upvote was not confirmed by Reddit server vote state")

        elapsed = _ms() - start_ms
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="COMMENT_UPVOTE",
                    result="SUCCESS", target_url=comment_url or post_url,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
                await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.comment_upvote.db_log_failed", error=str(db_exc))

        log.info("reddit.comment_upvote.success", elapsed_ms=elapsed, comment_id=comment_id)
        return _ok({"comment_id": comment_id, "api_used": True, "verified": True, "server_verified": True})

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.comment_upvote.failed", error=error_msg)
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="COMMENT_UPVOTE", result="FAILURE",
                    target_url=comment_url or post_url, error_message=error_msg,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
            except Exception:
                pass
        return _fail(error_msg)
    finally:
        try:
            page.remove_listener("request", _early_listener)
        except Exception:
            pass


async def run_tool(
    page: Page,
    account_id: str,
    comment_url: str = "",
    comment_fullname: str = "",
    post_url: str = "",
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """AI agent entry point for upvoting a Reddit comment."""
    return await comment_upvote(
        page=page,
        account_id=account_id,
        comment_url=comment_url,
        comment_fullname=comment_fullname,
        post_url=post_url,
        db=db,
        proxy_id=proxy_id,
    )
