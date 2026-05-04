"""
tools/post_tool.py — Reddit post submission tool for AI agents.

Stealth features:
- simulate_reading() called before acting (critical for anti-bot timing)
- Human-like typing for title and body
- JS-based form filling with React native setter (handles shreddit shadow DOM)
- Shadow DOM traversal for Lexical editor and custom components
- Random scroll before submit
- Warmup phase enforcement (optional, via db)

Usage:
    result = await run_tool(page, account_id, subreddit="python", title="My post", body="Content here")
    # result: {success: bool, data: {post_url: str}, error: str|None}
"""

from __future__ import annotations

import asyncio
import random
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, _random_scroll, _human_type, _ok, _fail, _ms,
)

logger = structlog.get_logger(__name__)


async def post(
    page: Page,
    account_id: str,
    subreddit: str,
    title: str,
    body: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Submit a text post to a subreddit.

    Handles both old Reddit and new Reddit (shreddit) post forms,
    including Lexical editor and shadow DOM components.
    """
    log = logger.bind(account_id=account_id, subreddit=subreddit, action="POST")
    start_ms = _ms()

    try:
        submit_url = f"https://www.reddit.com/r/{subreddit}/submit?type=self"
        log.info("reddit.post.navigating", url=submit_url)
        await page.goto(submit_url, wait_until="domcontentloaded", timeout=60_000)

        await simulate_reading(page, account_id)
        await _delay(account_id, 1.0, 2.0)

        try:
            await page.wait_for_selector(
                'textarea[name="title"], input[name="title"],'
                '[data-testid="post-composer-title"],'
                'shreddit-composer, faceplate-form',
                timeout=15_000,
            )
        except Exception:
            log.warning("reddit.post.form_not_found")

        await _random_scroll(page, account_id)
        await _delay(account_id, 0.8, 2.0)

        # --- Fill title (JS-based, handles shadow DOM + new Reddit) ---
        title_filled = await page.evaluate("""(title) => {
            let titleField = document.querySelector('textarea[name="title"], input[name="title"]');
            if (!titleField) titleField = document.querySelector('[data-testid="post-composer-title"]');
            if (!titleField) {
                const composers = document.querySelectorAll('shreddit-composer, shreddit-post-composer, faceplate-form');
                for (const comp of composers) {
                    const shadow = comp.shadowRoot;
                    if (shadow) {
                        titleField = shadow.querySelector('textarea, input[type="text"], [contenteditable="true"]');
                        if (titleField) break;
                    }
                    titleField = comp.querySelector('textarea[name="title"], input[name="title"], textarea');
                    if (titleField) break;
                }
            }
            if (!titleField) return { filled: false, error: 'title_not_found' };

            titleField.focus();
            titleField.click();
            const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set
                || Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
            if (nativeSetter) { nativeSetter.call(titleField, title); }
            else { titleField.value = title; }
            titleField.dispatchEvent(new Event('input', { bubbles: true }));
            titleField.dispatchEvent(new Event('change', { bubbles: true }));
            return { filled: true };
        }""", title)

        if not title_filled or not title_filled.get("filled"):
            title_field = await page.query_selector(
                'textarea[name="title"], input[name="title"], [data-testid="post-composer-title"]'
            )
            if not title_field:
                raise RuntimeError(f"Could not find title field: {title_filled}")
            await title_field.click()
            await _delay(account_id, 0.3, 0.8)
            await _human_type(page, title_field, title, account_id)

        log.info("reddit.post.title_filled")
        await _delay(account_id, 0.8, 1.5)

        # --- Fill body (handles Lexical editor and old Reddit textarea) ---
        body_filled = await page.evaluate("""(body) => {
            let editor = document.querySelector('div[data-lexical-editor="true"]');
            if (!editor) {
                const editors = document.querySelectorAll('div[contenteditable="true"][role="textbox"]');
                for (const e of editors) { if (e.offsetParent !== null) { editor = e; break; } }
            }
            if (!editor) {
                const editors = document.querySelectorAll('div[contenteditable="true"]');
                for (const e of editors) { if (e.offsetParent !== null) { editor = e; break; } }
            }
            if (!editor) editor = document.querySelector('textarea[name="text"], .DraftEditor-root, textarea[name="body"]');
            if (!editor) return { filled: false, error: 'body_not_found' };

            editor.focus();
            if (editor.tagName === 'TEXTAREA') {
                editor.value = body;
                editor.dispatchEvent(new Event('input', { bubbles: true }));
            } else {
                document.execCommand('selectAll', false, null);
                document.execCommand('delete', false, null);
                document.execCommand('insertText', false, body);
                editor.dispatchEvent(new Event('input', { bubbles: true }));
            }
            return { filled: true };
        }""", body)

        if not body_filled or not body_filled.get("filled"):
            body_field = await page.query_selector(
                'div[contenteditable="true"], textarea[name="text"], .DraftEditor-root'
            )
            if body_field:
                await body_field.click()
                await _delay(account_id, 0.3, 0.8)
                await _human_type(page, body_field, body, account_id)
            else:
                log.warning("reddit.post.body_field_not_found")

        log.info("reddit.post.body_filled")
        await _delay(account_id, 0.8, 1.5)

        # --- Submit ---
        await _random_scroll(page, account_id)
        await _delay(account_id, 0.5, 1.5)

        submitted = await page.evaluate("""() => {
            const selectors = ['button[type="submit"]:not([disabled])', 'button:not([disabled])'];
            const keywords = ['post', 'submit', 'create'];
            for (const sel of selectors) {
                const buttons = document.querySelectorAll(sel);
                for (const btn of buttons) {
                    const text = btn.textContent.trim().toLowerCase();
                    if (keywords.some(kw => text.includes(kw))) {
                        btn.click();
                        return { clicked: true, text: btn.textContent.trim() };
                    }
                }
            }
            const shredditComps = document.querySelectorAll('shreddit-composer, shreddit-post-composer, faceplate-form');
            for (const comp of shredditComps) {
                const shadow = comp.shadowRoot;
                if (shadow) {
                    const btns = shadow.querySelectorAll('button[type="submit"], button');
                    for (const btn of btns) {
                        const text = btn.textContent.trim().toLowerCase();
                        if (keywords.some(kw => text.includes(kw))) {
                            btn.click();
                            return { clicked: true, text: btn.textContent.trim() };
                        }
                    }
                }
            }
            const fallback = document.querySelector('button[type="submit"]');
            if (fallback) { fallback.click(); return { clicked: true }; }
            return { clicked: false };
        }""")

        if not submitted or not submitted.get("clicked"):
            await page.keyboard.press("Control+Enter")
            log.info("reddit.post.submit_via_ctrl_enter")

        await _delay(account_id, 2.0, 4.0)

        try:
            await page.wait_for_url("**/comments/**", timeout=30_000)
        except Exception:
            log.warning("reddit.post.no_redirect")

        await _delay(account_id)

        post_url = page.url
        elapsed = _ms() - start_ms

        if db is not None:
            try:
                content_text = f"{title}\n\n{body}"
                await db.store_content(account_id, content_text, subreddit, post_url)
                safe_pid = None
                if proxy_id:
                    row = await db.get_proxy(proxy_id)
                    safe_pid = proxy_id if row else None
                await db.log_action(
                    account_id=account_id, action_type="POST", result="SUCCESS",
                    subreddit=subreddit, target_url=post_url, content_text=content_text,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
                await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.post.db_log_failed", error=str(db_exc))

        log.info("reddit.post.success", post_url=post_url, elapsed_ms=elapsed)
        return _ok({"post_url": post_url})

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.post.failed", error=error_msg)
        if db is not None:
            try:
                safe_pid = None
                if proxy_id:
                    row = await db.get_proxy(proxy_id)
                    safe_pid = proxy_id if row else None
                await db.log_action(
                    account_id=account_id, action_type="POST", result="FAILURE",
                    subreddit=subreddit, error_message=error_msg,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
            except Exception:
                pass
        return _fail(error_msg)


async def run_tool(
    page: Page,
    account_id: str,
    subreddit: str,
    title: str,
    body: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """AI agent entry point for Reddit post submission.

    Args:
        page: Active Playwright page (logged in, stealth applied).
        account_id: Unique account identifier.
        subreddit: Target subreddit name (without r/).
        title: Post title.
        body: Post body text.
        db: Optional database instance for action logging.
        proxy_id: Optional proxy ID for logging.

    Returns:
        {success: bool, data: {post_url: str}, error: str|None}
    """
    return await post(
        page=page, account_id=account_id, subreddit=subreddit,
        title=title, body=body, db=db, proxy_id=proxy_id,
    )
