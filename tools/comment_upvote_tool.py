"""
tools/comment_upvote_tool.py — Reddit comment upvote via visible UI.

Primary action: navigate to comment URL, find the comment in visible DOM,
scroll it into view, and click its upvote button through Playwright mouse.
Read-only server verification via OAuth API is used only when a token is
already captured from network traffic; the API is never the primary vote path.
"""

from __future__ import annotations

import asyncio
import os
import random
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
        r"/comments/[^/]+/[^/]+/([a-z0-9]+)/?(?:[?#]|$)",
        r"[?&]comment=([a-z0-9]+)",
        r"#t1_([a-z0-9]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if match:
            comment_id = match.group(1)
            return comment_id if comment_id.startswith("t1_") else f"t1_{comment_id}"
    return None


def _bare_comment_id(fullname: str) -> str:
    """Strip t1_ prefix to get the bare comment ID."""
    return fullname.removeprefix("t1_")


def _fallback_viewport_coords(btn_info: dict, viewport: dict) -> Optional[dict]:
    """Convert document coordinates to safe viewport coordinates after scrolling."""
    try:
        x = int(float(btn_info["absX"]) - float(viewport.get("scrollX", 0)))
        y = int(float(btn_info["absY"]) - float(viewport.get("scrollY", 0)))
        width = int(float(viewport.get("width", 0)))
        height = int(float(viewport.get("height", 0)))
    except (KeyError, TypeError, ValueError):
        return None

    if width <= 0 or height <= 0:
        return None
    if x < 0 or y < 0 or x > width or y > height:
        return None
    return {"x": x, "y": y}


async def _verify_comment_vote_state(
    page: Page, token: str, user_agent: str, comment_id: str
) -> Optional[bool]:
    """Read-only API verification — never used as the primary vote action."""
    try:
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
    except Exception:
        return None


async def _find_and_click_comment_upvote(page: Page, bare_id: str) -> dict:
    """
    Locate the comment in the visible DOM, find its upvote button, scroll into
    view, and click through Playwright mouse. Returns a result dict.
    """
    # Find the comment element and its upvote button coordinates via JS
    btn_info = await page.evaluate(
        """(bareId) => {
        const thingId = 't1_' + bareId;

        // Selectors Reddit uses for comment containers
        const containerSelectors = [
            'shreddit-comment[thingid="' + thingId + '"]',
            '[data-fullname="' + thingId + '"]',
            '[data-thing-id="' + thingId + '"]',
            '#thing_' + thingId,
        ];

        let container = null;
        for (const sel of containerSelectors) {
            container = document.querySelector(sel);
            if (container) break;
        }

        if (!container) {
            return { found: false, reason: 'comment_container_not_found', thingId };
        }

        // Collect roots including shadow roots for traversal
        const roots = [container];
        const collectShadowRoots = (el) => {
            if (el.shadowRoot) roots.push(el.shadowRoot);
            for (const child of el.children || []) collectShadowRoots(child);
        };
        collectShadowRoots(container);

        const upvoteSelectors = [
            'button[upvote]',
            'button[data-click-id="upvote"]',
            'button[data-testid="upvote-button"]',
            'button[aria-label*="upvote" i]',
            'faceplate-tracker[slot="upvote"] button',
            '[slot="upvote"] button',
            'button[vote-direction="up"]',
        ];

        for (const root of roots) {
            for (const sel of upvoteSelectors) {
                let candidates;
                try { candidates = root.querySelectorAll(sel); } catch (_) { continue; }
                for (const btn of candidates) {
                    const rect = btn.getBoundingClientRect();
                    if (rect.width === 0 || rect.height === 0) continue;

                    const isAlready = (
                        btn.getAttribute('aria-pressed') === 'true' ||
                        (btn.className || '').toString().toLowerCase().includes('upvoted') ||
                        btn.closest('[vote-state="UP"]') !== null ||
                        btn.closest('[aria-pressed="true"]') === btn
                    );
                    if (isAlready) return { found: true, already: true, thingId };

                    return {
                        found: true,
                        already: false,
                        absX: rect.left + window.scrollX + rect.width / 2,
                        absY: rect.top + window.scrollY + rect.height / 2,
                        viewportY: rect.top + rect.height / 2,
                        viewportHeight: window.innerHeight,
                        selector: sel,
                        thingId,
                    };
                }
            }
        }
        return { found: false, reason: 'upvote_button_not_in_dom', thingId };
    }""",
        bare_id,
    )

    if not btn_info or not btn_info.get("found"):
        reason = btn_info.get("reason", "not_found") if isinstance(btn_info, dict) else "not_found"
        return {"success": False, "reason": reason, "btn_info": btn_info}

    if btn_info.get("already"):
        return {"success": True, "already": True, "btn_info": btn_info}

    # Scroll comment into viewport center
    target_scroll_y = btn_info["absY"] - btn_info["viewportHeight"] / 2
    await page.evaluate(
        "(y) => window.scrollTo({ top: y, behavior: 'smooth' })",
        target_scroll_y,
    )
    await asyncio.sleep(1.2)

    # Re-read fresh viewport coordinates after scroll
    fresh = await page.evaluate(
        """(args) => {
        const { bareId, sel } = args;
        const thingId = 't1_' + bareId;
        const containerSelectors = [
            'shreddit-comment[thingid="' + thingId + '"]',
            '[data-fullname="' + thingId + '"]',
        ];
        let container = null;
        for (const cs of containerSelectors) {
            container = document.querySelector(cs);
            if (container) break;
        }
        if (!container) return null;
        const roots = [container];
        const collectShadowRoots = (el) => {
            if (el.shadowRoot) roots.push(el.shadowRoot);
            for (const child of el.children || []) collectShadowRoots(child);
        };
        collectShadowRoots(container);
        for (const root of roots) {
            let candidates;
            try { candidates = root.querySelectorAll(sel); } catch (_) { continue; }
            for (const btn of candidates) {
                const rect = btn.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) {
                    return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
                }
            }
        }
        return null;
    }""",
        {"bareId": bare_id, "sel": btn_info["selector"]},
    )

    if fresh:
        click_x = int(fresh["x"])
        click_y = int(fresh["y"])
    else:
        viewport = await page.evaluate(
            """() => ({
                scrollX: window.scrollX,
                scrollY: window.scrollY,
                width: window.innerWidth,
                height: window.innerHeight
            })"""
        )
        fallback = _fallback_viewport_coords(btn_info, viewport)
        if not fallback:
            return {
                "success": False,
                "reason": "upvote_button_coordinates_not_visible_after_scroll",
                "btn_info": btn_info,
                "viewport": viewport,
            }
        click_x = fallback["x"]
        click_y = fallback["y"]

    # Human-like mouse approach then click
    await page.mouse.move(
        click_x + random.randint(-25, 25),
        click_y + random.randint(-15, 15),
        steps=random.randint(6, 14),
    )
    await asyncio.sleep(random.uniform(0.1, 0.35))
    await page.mouse.move(click_x, click_y, steps=random.randint(3, 7))
    await asyncio.sleep(random.uniform(0.05, 0.12))
    await page.mouse.click(click_x, click_y)

    return {
        "success": True,
        "already": False,
        "clicked": True,
        "coords": {"x": click_x, "y": click_y},
        "selector": btn_info["selector"],
        "btn_info": btn_info,
    }


async def _ui_verify_comment_upvoted(page: Page, bare_id: str) -> bool:
    """Check whether the comment's upvote button now shows pressed/upvoted state."""
    return await page.evaluate(
        """(bareId) => {
        const thingId = 't1_' + bareId;
        const containerSelectors = [
            'shreddit-comment[thingid="' + thingId + '"]',
            '[data-fullname="' + thingId + '"]',
        ];
        let container = null;
        for (const sel of containerSelectors) {
            container = document.querySelector(sel);
            if (container) break;
        }
        if (!container) return false;
        // Check vote-state attribute on comment or action row
        const voteEl = container.querySelector('[vote-state]');
        const voteState = (
            container.getAttribute('vote-state') ||
            (voteEl ? voteEl.getAttribute('vote-state') : '') ||
            ''
        ).toUpperCase();
        if (voteState === 'UP') return true;
        // Fallback: any aria-pressed="true" upvote button inside
        const roots = [container];
        const collectShadowRoots = (el) => {
            if (el.shadowRoot) roots.push(el.shadowRoot);
            for (const child of el.children || []) collectShadowRoots(child);
        };
        collectShadowRoots(container);
        const upvoteSelectors = [
            'button[upvote][aria-pressed="true"]',
            'button[data-click-id="upvote"][aria-pressed="true"]',
            'button[aria-label*="upvote" i][aria-pressed="true"]',
        ];
        for (const root of roots) {
            for (const sel of upvoteSelectors) {
                try { if (root.querySelector(sel)) return true; } catch (_) {}
            }
        }
        return false;
    }""",
        bare_id,
    )


async def comment_upvote(
    page: Page,
    account_id: str,
    comment_url: str = "",
    comment_fullname: str = "",
    post_url: str = "",
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Upvote a Reddit comment via visible UI click flow.

    Navigates to the comment URL, locates the comment in visible DOM,
    scrolls it into view, clicks the upvote button via Playwright mouse,
    then verifies via DOM state. Read-only API verification follows only
    if a bearer token was already captured from network traffic.
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
        comment_id = _comment_fullname(
            comment_fullname=comment_fullname, comment_url=comment_url
        )
        if not comment_id:
            raise RuntimeError(
                "comment id not found; pass comment_fullname like t1_abc123 or a full comment permalink"
            )

        bare_id = _bare_comment_id(comment_id)
        navigate_url = comment_url or post_url
        if not navigate_url:
            raise RuntimeError("comment_url or post_url required to navigate to the comment")

        # Navigate and let the early token listener collect any Bearer tokens
        await page.goto(navigate_url, wait_until="domcontentloaded", timeout=60_000)
        if _early_tokens and not getattr(page, "_reddit_bearer_token", None):
            setattr(page, "_reddit_bearer_token", _early_tokens[-1])

        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass

        await simulate_reading(page, account_id)
        await _delay(account_id, 0.8, 1.8)

        # Locate and click the upvote button via visible UI
        log.info("reddit.comment_upvote.ui_click_start", comment_id=comment_id)
        click_result = await _find_and_click_comment_upvote(page, bare_id)

        if not click_result.get("success"):
            raise RuntimeError(
                f"comment upvote button not found in visible UI: {click_result.get('reason')} "
                f"(thingId={comment_id})"
            )

        if click_result.get("already"):
            log.info("reddit.comment_upvote.already_upvoted", comment_id=comment_id)
            return _ok({
                "comment_id": comment_id,
                "already_upvoted": True,
                "api_used": False,
                "ui_click": False,
                "verified": True,
                "verification_source": "ui_before_click",
            })

        # Wait briefly then verify DOM state
        await asyncio.sleep(random.uniform(3.0, 5.0))

        ui_verified = await _ui_verify_comment_upvoted(page, bare_id)
        log.info("reddit.comment_upvote.ui_verified_before_reload", verified=ui_verified)

        # Reload and re-verify
        ui_verified_after_reload = False
        try:
            await page.reload(wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await asyncio.sleep(2)
            ui_verified_after_reload = await _ui_verify_comment_upvoted(page, bare_id)
            log.info(
                "reddit.comment_upvote.ui_verified_after_reload",
                verified=ui_verified_after_reload,
            )
        except Exception as reload_exc:
            log.warning("reddit.comment_upvote.reload_failed", error=str(reload_exc))

        # Optional read-only server verification using already-captured token
        server_verified: Optional[bool] = None
        token = (os.getenv("REDDIT_OAUTH_ACCESS_TOKEN") or "").strip()
        if not token:
            token = getattr(page, "_reddit_bearer_token", None) or await ensure_token_captured(page, log)
        if not token:
            token = await _token_v2_candidate(page)

        if token:
            user_agent = _reddit_user_agent(account_id)
            try:
                if await _verify_oauth_token(page, token, user_agent):
                    await asyncio.sleep(1)
                    server_verified = await _verify_comment_vote_state(
                        page, token, user_agent, comment_id
                    )
                    log.info(
                        "reddit.comment_upvote.server_verified", server_verified=server_verified
                    )
            except Exception as sv_exc:
                log.warning("reddit.comment_upvote.server_verify_failed", error=str(sv_exc))

        final_verified = ui_verified or ui_verified_after_reload or (server_verified is True)

        if not final_verified:
            raise RuntimeError(
                "comment upvote not confirmed: UI state did not change after click "
                f"(ui_before_reload={ui_verified}, ui_after_reload={ui_verified_after_reload}, "
                f"server={server_verified})"
            )

        elapsed = _ms() - start_ms
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id,
                    action_type="COMMENT_UPVOTE",
                    result="SUCCESS",
                    target_url=comment_url or post_url,
                    proxy_id=safe_pid,
                    response_time_ms=elapsed,
                )
                await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.comment_upvote.db_log_failed", error=str(db_exc))

        log.info(
            "reddit.comment_upvote.success",
            elapsed_ms=elapsed,
            comment_id=comment_id,
            ui_verified=ui_verified,
            ui_verified_after_reload=ui_verified_after_reload,
            server_verified=server_verified,
        )
        return _ok({
            "comment_id": comment_id,
            "api_used": False,
            "ui_click": True,
            "selector": click_result.get("selector"),
            "coords": click_result.get("coords"),
            "ui_verified_before_reload": ui_verified,
            "ui_verified_after_reload": ui_verified_after_reload,
            "server_verified": server_verified,
            "verified": final_verified,
            "verification_source": (
                "server" if server_verified is True
                else "ui_after_reload" if ui_verified_after_reload
                else "ui_before_reload"
            ),
        })

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.comment_upvote.failed", error=error_msg)
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id,
                    action_type="COMMENT_UPVOTE",
                    result="FAILURE",
                    target_url=comment_url or post_url,
                    error_message=error_msg,
                    proxy_id=safe_pid,
                    response_time_ms=elapsed,
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
    """AI agent entry point for upvoting a Reddit comment via visible UI."""
    return await comment_upvote(
        page=page,
        account_id=account_id,
        comment_url=comment_url,
        comment_fullname=comment_fullname,
        post_url=post_url,
        db=db,
        proxy_id=proxy_id,
    )
