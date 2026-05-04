"""
tools/upvote_tool.py — Reddit upvote tool for AI agents.

Stealth features:
- Bearer token captured via early network listener BEFORE page.goto()
- OAuth token can be supplied via REDDIT_OAUTH_ACCESS_TOKEN for verification
- Trusted mouse click is the primary vote action (isTrusted=true events)
- Shadow DOM traversal to find upvote button in shreddit-post-action-row
- simulate_reading() before acting

Usage:
    result = await run_tool(page, account_id, post_url="https://reddit.com/r/...")
    # result: {success: bool, data: {click_used, verified, already_upvoted?}, error: str|None}
"""

from __future__ import annotations

import asyncio
import os
import random
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, _random_scroll, ensure_token_captured,
    safe_proxy_id, _ok, _fail, _ms,
)

logger = structlog.get_logger(__name__)


def _reddit_user_agent(account_id: str) -> str:
    """Return a Reddit API user agent with Reddit's required contact suffix."""
    configured = os.getenv("REDDIT_USER_AGENT", "").strip()
    if configured:
        return configured
    username = account_id.removeprefix("u/").strip("/") or "unknown"
    return f"windows:reddit-account-manager:v1.0.0 (by /u/{username})"


async def _token_v2_candidate(page: Page) -> Optional[str]:
    cookies = await page.context.cookies(["https://www.reddit.com", "https://reddit.com"])
    for cookie in cookies:
        if cookie.get("name") == "token_v2" and cookie.get("value"):
            return cookie["value"]
    return None


async def _oauth_headers(token: str, user_agent: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "User-Agent": user_agent,
    }


async def _verify_oauth_token(page: Page, token: str, user_agent: str) -> bool:
    token_check = await page.request.get(
        "https://oauth.reddit.com/api/v1/me",
        headers=await _oauth_headers(token, user_agent),
    )
    return token_check.status == 200


async def _verify_vote_state(page: Page, token: str, user_agent: str, post_id: str) -> Optional[bool]:
    response = await page.request.get(
        f"https://oauth.reddit.com/api/info?id={post_id}",
        headers=await _oauth_headers(token, user_agent),
    )
    if response.status != 200:
        return None
    payload = await response.json()
    children = payload.get("data", {}).get("children", [])
    if not children:
        return None
    return children[0].get("data", {}).get("likes") is True


async def upvote(
    page: Page,
    account_id: str,
    post_url: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Navigate to a post and upvote it.

    Uses the visible Reddit upvote control as the primary action, matching the
    normal browser flow. OAuth/API calls are used only to verify server state.
    """
    log = logger.bind(account_id=account_id, action="UPVOTE")
    start_ms = _ms()

    # Capture token from early network events BEFORE navigation
    _early_tokens: list[str] = []
    _vote_responses: list[dict] = []

    def _early_listener(request) -> None:
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer ") and "reddit.com" in request.url:
            t = auth[len("Bearer "):]
            if len(t) > 20:
                _early_tokens.append(t)

    async def _capture_vote_response(response) -> None:
        url = response.url
        if "reddit.com" not in url.lower() or "vote" not in url.lower():
            return
        item = {"url": url, "status": response.status}
        try:
            item["body"] = (await response.text())[:300]
        except Exception:
            item["body"] = ""
        _vote_responses.append(item)

    def _vote_response_listener(response) -> None:
        asyncio.create_task(_capture_vote_response(response))

    page.on("request", _early_listener)
    page.on("response", _vote_response_listener)

    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=60_000)

        if _early_tokens and not getattr(page, "_reddit_bearer_token", None):
            setattr(page, "_reddit_bearer_token", _early_tokens[-1])

        await simulate_reading(page, account_id)
        await _delay(account_id, 1.0, 2.0)

        try:
            await page.wait_for_selector(
                '[data-testid="post-container"], shreddit-post, .Post', timeout=15_000,
            )
        except Exception:
            pass

        await _random_scroll(page, account_id)
        await _delay(account_id, 0.5, 1.5)

        token = (os.getenv("REDDIT_OAUTH_ACCESS_TOKEN") or "").strip()
        if token:
            setattr(page, "_reddit_bearer_token", token)
        else:
            token = await ensure_token_captured(page, log)
        if not token:
            token = await _token_v2_candidate(page)
        user_agent = _reddit_user_agent(account_id)

        post_data = await page.evaluate("""() => {
            const post = document.querySelector('shreddit-post');
            if (!post) return null;
            return { postId: post.getAttribute('id'), score: post.getAttribute('score') };
        }""")

        click_used = False
        server_verified = False
        ui_verified_after_reload = False
        score_after_reload = None
        post_id = None

        if post_data and post_data.get("postId"):
            raw_post_id = post_data["postId"]
            post_id = raw_post_id if raw_post_id.startswith("t3_") else f"t3_{raw_post_id}"

        # Trusted mouse click is the primary action. This keeps the vote inside
        # Reddit's normal frontend event flow instead of posting directly first.
        verified = False
        log.info("reddit.upvote.trusted_click_primary")

        btn_info = await page.evaluate("""() => {
            const post = document.querySelector('shreddit-post')
                || document.querySelector('[data-testid="post-container"]')
                || document.querySelector('.Post');
            if (!post) return { found: false, reason: 'post_not_found' };

            const roots = [post];
            if (post.shadowRoot) roots.push(post.shadowRoot);
            const actionRow = post.querySelector('shreddit-post-action-row');
            if (actionRow) {
                const voteState = (actionRow.getAttribute('vote-state') || '').toUpperCase();
                if (voteState === 'UP') return { found: true, already: true };
                roots.push(actionRow);
                if (actionRow.shadowRoot) roots.push(actionRow.shadowRoot);
            }
            const walker = document.createTreeWalker(post, NodeFilter.SHOW_ELEMENT);
            let node = walker.currentNode;
            while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }

            const selectors = [
                'button[upvote]', 'button[data-click-id="upvote"]',
                'button[data-testid="upvote-button"]', 'button[aria-label*="upvote" i]',
                'faceplate-tracker[slot="upvote"] button', '[slot="upvote"] button',
                'faceplate-vote-button[upvote] button', 'button[vote-direction="up"]',
            ];

            for (const root of roots) {
                for (const sel of selectors) {
                    const candidates = root.querySelectorAll(sel);
                    for (const btn of candidates) {
                        const rect = btn.getBoundingClientRect();
                        if (rect.width === 0 || rect.height === 0) continue;
                        const isAlready = (
                            btn.getAttribute('aria-pressed') === 'true' ||
                            (btn.className || '').toString().toLowerCase().includes('upvoted') ||
                            btn.closest('[vote-state="UP"]') !== null
                        );
                        if (isAlready) return { found: true, already: true };
                        return {
                            found: true, already: false,
                            absY: rect.top + window.scrollY,
                            absX: rect.left + window.scrollX,
                            width: rect.width, height: rect.height,
                            selector: sel, viewportHeight: window.innerHeight,
                        };
                    }
                }
            }
            return { found: false, reason: 'not_found' };
        }""")

        if not btn_info or not btn_info.get("found"):
            raise RuntimeError(f"Upvote button not found: {btn_info}")

        if btn_info.get("already"):
            log.info("reddit.upvote.already_upvoted")
            elapsed = _ms() - start_ms
            if db is not None:
                try:
                    safe_pid = await safe_proxy_id(db, proxy_id)
                    await db.log_action(
                        account_id=account_id, action_type="UPVOTE",
                        result="SUCCESS", target_url=post_url,
                        proxy_id=safe_pid, response_time_ms=elapsed,
                    )
                except Exception:
                    pass
            return _ok({"already_upvoted": True, "api_used": False, "click_used": False})

        # Scroll button into viewport center, then click with trusted event
        target_scroll_y = btn_info["absY"] - (btn_info["viewportHeight"] / 2)
        await page.evaluate(
            "(scrollTo) => window.scrollTo({ top: scrollTo, behavior: 'instant' })",
            target_scroll_y,
        )
        await asyncio.sleep(1.0)

        # Re-read fresh coordinates after scroll
        fresh_coords = await page.evaluate("""(sel) => {
            const post = document.querySelector('shreddit-post')
                || document.querySelector('[data-testid="post-container"]')
                || document.querySelector('.Post');
            if (!post) return null;
            const roots = [post];
            if (post.shadowRoot) roots.push(post.shadowRoot);
            const actionRow = post.querySelector('shreddit-post-action-row');
            if (actionRow) {
                roots.push(actionRow);
                if (actionRow.shadowRoot) roots.push(actionRow.shadowRoot);
            }
            const walker = document.createTreeWalker(post, NodeFilter.SHOW_ELEMENT);
            let node = walker.currentNode;
            while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }
            const candidates = [];
            for (const root of roots) {
                const els = root.querySelectorAll(sel);
                candidates.push(...els);
            }
            for (const btn of candidates) {
                const rect = btn.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) {
                    return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
                }
            }
            return null;
        }""", btn_info["selector"])

        click_x = int(fresh_coords["x"]) if fresh_coords else int(btn_info["absX"] + btn_info["width"] / 2)
        click_y = int(fresh_coords["y"]) if fresh_coords else int(btn_info["absY"] - target_scroll_y + btn_info["height"] / 2)

        # Human-like mouse approach before click
        approach_x = click_x + random.randint(-30, 30)
        approach_y = click_y + random.randint(-20, 20)
        await page.mouse.move(approach_x, approach_y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.4))
        await page.mouse.move(click_x, click_y, steps=random.randint(3, 8))
        await asyncio.sleep(random.uniform(0.05, 0.15))
        await page.mouse.click(click_x, click_y)
        click_used = True

        await asyncio.sleep(random.uniform(4.0, 7.0))

        # Verify vote state changed in the page first; this mirrors what a human sees.
        verified = await page.evaluate("""() => {
            const post = document.querySelector('shreddit-post')
                || document.querySelector('[data-testid="post-container"]');
            if (!post) return false;
            const actionRow = post.querySelector('shreddit-post-action-row');
            if (actionRow) {
                const voteState = (actionRow.getAttribute('vote-state') || '').toUpperCase();
                if (voteState === 'UP') return true;
            }
            const upvotedBtns = post.querySelectorAll('[aria-pressed="true"], .upvoted, [class*="upvoted"]');
            return upvotedBtns.length > 0;
        }""")
        log.info("reddit.upvote.trusted_click_done", verified=verified)

        if token and post_id:
            try:
                if not await _verify_oauth_token(page, token, user_agent):
                    raise RuntimeError("oauth token rejected by /api/v1/me")
                await asyncio.sleep(2)
                server_verified = await _verify_vote_state(page, token, user_agent, post_id) is True
                log.info("reddit.upvote.server_vote_state_after_click", server_verified=server_verified)
            except Exception as verify_exc:
                log.warning("reddit.upvote.server_verify_failed", error=str(verify_exc))
                setattr(page, "_reddit_bearer_token", None)

        try:
            await page.reload(wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await asyncio.sleep(2)
            reload_state = await page.evaluate("""() => {
                const post = document.querySelector('shreddit-post')
                    || document.querySelector('[data-testid="post-container"]');
                if (!post) return { verified: false, score: null };
                const actionRow = post.querySelector('shreddit-post-action-row');
                let verified = false;
                if (actionRow) {
                    const voteState = (actionRow.getAttribute('vote-state') || '').toUpperCase();
                    verified = voteState === 'UP';
                }
                if (!verified) {
                    const upvotedBtns = post.querySelectorAll('[aria-pressed="true"], .upvoted, [class*="upvoted"]');
                    verified = upvotedBtns.length > 0;
                }
                return { verified, score: post.getAttribute('score') };
            }""")
            ui_verified_after_reload = reload_state.get("verified") is True
            score_after_reload = reload_state.get("score")
            log.info(
                "reddit.upvote.state_after_reload",
                ui_verified_after_reload=ui_verified_after_reload,
                score_before=post_data.get("score") if post_data else None,
                score_after=score_after_reload,
                vote_responses=_vote_responses[-3:],
            )
        except Exception as reload_exc:
            log.warning("reddit.upvote.reload_verify_failed", error=str(reload_exc))

        elapsed = _ms() - start_ms

        final_verified = server_verified or ui_verified_after_reload

        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="UPVOTE",
                    result="SUCCESS", target_url=post_url,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
                await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.upvote.db_log_failed", error=str(db_exc))

        if not final_verified:
            raise RuntimeError(
                "upvote was not confirmed after reload or by Reddit server vote state; "
                f"optimistic_ui={verified}, vote_responses={_vote_responses[-3:]}"
            )

        log.info(
            "reddit.upvote.success",
            elapsed_ms=elapsed,
            click_used=click_used,
            server_verified=server_verified,
            ui_verified_after_reload=ui_verified_after_reload,
        )
        return _ok({
            "api_used": False,
            "click_used": click_used,
            "verified": final_verified,
            "ui_verified_before_reload": verified,
            "ui_verified_after_reload": ui_verified_after_reload,
            "server_verified": server_verified,
            "score_before": post_data.get("score") if post_data else None,
            "score_after_reload": score_after_reload,
            "vote_responses": _vote_responses[-3:],
        })

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.upvote.failed", error=error_msg)
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="UPVOTE", result="FAILURE",
                    target_url=post_url, error_message=error_msg,
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
        try:
            page.remove_listener("response", _vote_response_listener)
        except Exception:
            pass


async def run_tool(
    page: Page,
    account_id: str,
    post_url: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """AI agent entry point for upvoting a Reddit post.

    Args:
        page: Active Playwright page (logged in, stealth applied).
        account_id: Unique account identifier.
        post_url: Full URL of the Reddit post to upvote.
        db: Optional database instance for action logging.
        proxy_id: Optional proxy ID for logging.

    Returns:
        {success: bool, data: {api_used: bool, verified: bool, already_upvoted?: bool}, error: str|None}
    """
    return await upvote(
        page=page, account_id=account_id, post_url=post_url, db=db, proxy_id=proxy_id,
    )
