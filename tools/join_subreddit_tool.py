"""
tools/join_subreddit_tool.py — Reddit subreddit join tool for AI agents.

Stealth features:
- simulate_reading() before acting
- Checks already-joined state via DOM + shadow DOM before clicking
- Multiple join strategies: Playwright locator → JS evaluate → shadow DOM
- Human-like mouse movement to the Join button (steps-based, not instant)
- Post-join verification via account's subscriptions

Context: Reddit's Crowd Control filters comments from non-members on
High/Maximum settings. Accounts must join subreddits days before commenting.

Usage:
    result = await run_tool(page, account_id, subreddit="python")
    # result: {success: bool, data: {subreddit, already_joined}, error: str|None}
"""

from __future__ import annotations

import asyncio
import random
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, safe_proxy_id, _ok, _fail, _ms,
)

logger = structlog.get_logger(__name__)


async def join_subreddit(
    page: Page,
    account_id: str,
    subreddit: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Navigate to a subreddit and click the Join button.

    Reddit's Crowd Control filters comments from non-members on High/Maximum settings.
    Accounts MUST join subreddits days before commenting there.
    """
    log = logger.bind(account_id=account_id, subreddit=subreddit, action="SUBSCRIBE")
    start_ms = _ms()
    url = f"https://www.reddit.com/r/{subreddit}/"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await simulate_reading(page, account_id)
        await _delay(account_id, 2.0, 4.0)

        # Check if already joined (DOM + shadow DOM)
        already_joined = await page.evaluate("""() => {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const text = (btn.textContent || '').trim().toLowerCase();
                if (text === 'joined' || text === 'leave') return true;
            }
            const header = document.querySelector('shreddit-subreddit-header-actions');
            if (header) {
                const sr = header.shadowRoot;
                if (sr) {
                    const btns = sr.querySelectorAll('button');
                    for (const b of btns) {
                        const t = (b.textContent || '').trim().toLowerCase();
                        if (t === 'joined' || t === 'leave') return true;
                    }
                }
            }
            return false;
        }""")

        if already_joined:
            log.info("reddit.join.already_member", subreddit=subreddit)
            elapsed = _ms() - start_ms
            if db is not None:
                try:
                    safe_pid = await safe_proxy_id(db, proxy_id)
                    await db.log_action(
                        account_id=account_id, action_type="SUBSCRIBE", result="SUCCESS",
                        subreddit=subreddit, target_url=url,
                        proxy_id=safe_pid, response_time_ms=elapsed,
                    )
                except Exception:
                    pass
            return _ok({"subreddit": subreddit, "already_joined": True})

        join_clicked = False

        # Strategy 1: Playwright locator (most reliable)
        try:
            join_btn = page.get_by_role("button", name="Join", exact=True)
            if await join_btn.count() > 0:
                first_btn = join_btn.first
                if await first_btn.is_visible():
                    box = await first_btn.bounding_box()
                    if box:
                        cx = box["x"] + box["width"] / 2
                        cy = box["y"] + box["height"] / 2
                        await page.mouse.move(cx, cy, steps=random.randint(5, 15))
                        await asyncio.sleep(random.uniform(0.1, 0.3))
                        await page.mouse.click(cx, cy)
                        join_clicked = True
                        log.info("reddit.join.clicked_via_locator")
        except Exception as loc_exc:
            log.debug("reddit.join.locator_failed", error=str(loc_exc))

        # Strategy 2: JS evaluate with bounds check
        if not join_clicked:
            js_result = await page.evaluate("""() => {
                const buttons = document.querySelectorAll('button');
                for (const btn of buttons) {
                    const text = (btn.textContent || '').trim().toLowerCase();
                    const rect = btn.getBoundingClientRect();
                    if (text === 'join' && rect.height > 0 && rect.width > 0) {
                        btn.click();
                        return { clicked: true, selector: 'button[text=join]' };
                    }
                }
                return { clicked: false };
            }""")
            if js_result and js_result.get("clicked"):
                join_clicked = True
                log.info("reddit.join.clicked_via_js")

        # Strategy 3: Shadow DOM traversal (new Reddit header component)
        if not join_clicked:
            shadow_result = await page.evaluate("""() => {
                const header = document.querySelector('shreddit-subreddit-header-actions');
                if (!header) return { clicked: false, reason: 'no_header' };
                const sr = header.shadowRoot;
                if (!sr) return { clicked: false, reason: 'no_shadow_root' };
                const btns = sr.querySelectorAll('button');
                for (const btn of btns) {
                    const text = (btn.textContent || '').trim().toLowerCase();
                    const rect = btn.getBoundingClientRect();
                    if (text === 'join' && rect.height > 0) {
                        btn.click();
                        return { clicked: true, text: btn.textContent.trim() };
                    }
                }
                return { clicked: false, reason: 'join_btn_not_in_shadow' };
            }""")
            if shadow_result and shadow_result.get("clicked"):
                join_clicked = True
                log.info("reddit.join.clicked_via_shadow_dom")

        if not join_clicked:
            raise RuntimeError(f"Could not find or click Join button for r/{subreddit}")

        await _delay(account_id, 1.5, 3.0)

        # Verify join state
        now_joined = await page.evaluate("""() => {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const text = (btn.textContent || '').trim().toLowerCase();
                if (text === 'joined' || text === 'leave') return true;
            }
            const header = document.querySelector('shreddit-subreddit-header-actions');
            if (header && header.shadowRoot) {
                const btns = header.shadowRoot.querySelectorAll('button');
                for (const b of btns) {
                    const t = (b.textContent || '').trim().toLowerCase();
                    if (t === 'joined' || t === 'leave') return true;
                }
            }
            return false;
        }""")

        elapsed = _ms() - start_ms

        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="SUBSCRIBE",
                    result="SUCCESS" if now_joined else "UNVERIFIED",
                    subreddit=subreddit, target_url=url,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
                if now_joined:
                    await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.join.db_log_failed", error=str(db_exc))

        log.info("reddit.join.done", subreddit=subreddit, verified=now_joined)
        return _ok({"subreddit": subreddit, "already_joined": False, "verified": now_joined})

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.join.failed", error=error_msg, subreddit=subreddit)
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="SUBSCRIBE", result="FAILURE",
                    subreddit=subreddit, target_url=url, error_message=error_msg,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
            except Exception:
                pass
        return _fail(error_msg)


async def run_tool(
    page: Page,
    account_id: str,
    subreddit: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """AI agent entry point for joining a Reddit subreddit.

    Args:
        page: Active Playwright page (logged in, stealth applied).
        account_id: Unique account identifier.
        subreddit: Subreddit name without r/ prefix.
        db: Optional database instance for action logging.
        proxy_id: Optional proxy ID for logging.

    Returns:
        {success: bool, data: {subreddit: str, already_joined: bool, verified: bool}, error: str|None}
    """
    return await join_subreddit(
        page=page, account_id=account_id, subreddit=subreddit, db=db, proxy_id=proxy_id,
    )
