"""
tools/reply_tool.py — Reddit reply-to-comment tool for AI agents.

Stealth features:
- simulate_reading() before acting
- scroll_to_comment() — scrolls to target comment before clicking reply
- All DOM queries scoped to the specific shreddit-comment element (prevents
  accidentally targeting page-level composer or wrong reply button)
- Shadow DOM traversal under the specific comment tree only
- Behavior-engine typing with word/sentence boundary awareness
- 15-attempt polling for reply editor readiness

Usage:
    result = await run_tool(page, account_id, comment_fullname="t1_abc123",
                            post_url="https://...", text="Good point!")
    # result: {success: bool, data: {comment_fullname, post_url}, error: str|None}
"""

from __future__ import annotations

import asyncio
import random
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, _random_scroll, scroll_to_comment,
    safe_proxy_id, _ok, _fail, _ms, _get_behavior_engine,
)

logger = structlog.get_logger(__name__)


async def reply_to_comment(
    page: Page,
    account_id: str,
    comment_fullname: str,
    post_url: str,
    text: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Reply to a specific comment on a Reddit thread.

    Key design: every DOM query is SCOPED to the specific shreddit-comment element.
    This prevents accidentally targeting the page-level composer or a different comment's reply button.
    """
    log = logger.bind(account_id=account_id, post_url=post_url, action="REPLY")
    start_ms = _ms()

    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=60_000)
        await simulate_reading(page, account_id)
        await _delay(account_id, 1.0, 2.0)

        try:
            await page.wait_for_selector(
                '[data-testid="post-container"], shreddit-post, .Post', timeout=15_000,
            )
        except Exception:
            pass

        await _random_scroll(page, account_id)
        await _delay(account_id)

        # Scroll to target comment
        comment_id = comment_fullname.replace("t1_", "")
        found = await scroll_to_comment(page, comment_id)
        if not found:
            raise RuntimeError(f"Comment {comment_fullname} not found on page after scrolling")

        await _delay(account_id, 1.0, 2.0)

        # Step 1: Click Reply button scoped to target comment
        clicked = await page.evaluate("""(id) => {
            const actionRow = document.querySelector(
                `shreddit-comment[thingid="t1_${id}"] shreddit-comment-action-row`
            );
            if (!actionRow) return 'no_action_row';
            const replyTracker = actionRow.querySelector('faceplate-tracker[slot="comment-reply"]');
            if (!replyTracker) return 'no_tracker';
            const replyBtn = replyTracker.querySelector('button');
            if (!replyBtn) return 'no_button';
            replyBtn.click();
            return 'clicked';
        }""", comment_id)

        if clicked != "clicked":
            comment_el = await page.query_selector(
                f'[data-fullname="{comment_fullname}"],'
                f'shreddit-comment[data-fullname="{comment_fullname}"]'
            )
            if comment_el:
                reply_btn = await comment_el.query_selector('button:has-text("Reply")')
                if reply_btn:
                    await reply_btn.click()
                    clicked = "clicked"

        if clicked != "clicked":
            raise RuntimeError(f"Reply button not found: {clicked}")

        log.info("reddit.reply.button_clicked")
        await _delay(account_id, 2.0, 4.0)

        # Step 2: Find reply editor scoped to target comment only
        editor_ready = False
        for attempt in range(15):
            editor_info = await page.evaluate("""(id) => {
                const hostComment = document.querySelector(`shreddit-comment[thingid="t1_${id}"]`);
                if (!hostComment) return { found: false, reason: 'host_comment_missing' };

                const roots = [hostComment];
                if (hostComment.shadowRoot) roots.push(hostComment.shadowRoot);
                const walker = document.createTreeWalker(hostComment, NodeFilter.SHOW_ELEMENT);
                let node = walker.currentNode;
                while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }

                const editorSelectors = [
                    'div[data-lexical-editor="true"]',
                    'div[contenteditable="true"][role="textbox"]',
                    'div[contenteditable="true"]',
                    'textarea[name="comment"]',
                ];

                for (const root of roots) {
                    for (const sel of editorSelectors) {
                        const candidates = root.querySelectorAll(sel);
                        for (const el of candidates) {
                            const rect = el.getBoundingClientRect();
                            const isVisible = rect.height > 0 && rect.width > 0;
                            const isEmpty = (el.textContent || el.value || '').trim() === '';
                            if (isVisible && isEmpty) {
                                return { found: true, selector: sel, visible: true,
                                         root: root === hostComment ? 'host' : 'shadow' };
                            }
                        }
                    }
                }
                return { found: false };
            }""", comment_id)

            if editor_info and editor_info.get("found"):
                log.info("reddit.reply.editor_found", attempt=attempt)
                editor_ready = True
                break
            await asyncio.sleep(0.5)

        if not editor_ready:
            raise RuntimeError("Reply editor not found inside target comment after clicking reply button")

        # Step 3: Focus the reply editor and type
        focused = await page.evaluate("""(id) => {
            const hostComment = document.querySelector(`shreddit-comment[thingid="t1_${id}"]`);
            if (!hostComment) return { focused: false };

            const roots = [hostComment];
            if (hostComment.shadowRoot) roots.push(hostComment.shadowRoot);
            const walker = document.createTreeWalker(hostComment, NodeFilter.SHOW_ELEMENT);
            let node = walker.currentNode;
            while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }

            const editorSelectors = [
                'div[data-lexical-editor="true"]',
                'div[contenteditable="true"][role="textbox"]',
                'div[contenteditable="true"]',
                'textarea[name="comment"]',
            ];

            for (const root of roots) {
                for (const sel of editorSelectors) {
                    const candidates = root.querySelectorAll(sel);
                    for (const el of candidates) {
                        const rect = el.getBoundingClientRect();
                        if (rect.height > 0 && rect.width > 0 &&
                            (el.textContent || el.value || '').trim() === '') {
                            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            el.focus();
                            el.click();
                            return { focused: true, tag: el.tagName };
                        }
                    }
                }
            }
            return { focused: false };
        }""", comment_id)

        if not focused or not focused.get("focused"):
            raise RuntimeError("Could not focus reply editor")

        log.info("reddit.reply.editor_focused")
        await _delay(account_id, 0.5, 1.0)

        # Behavior-engine typing
        behavior = _get_behavior_engine(account_id)
        words = text.split(' ')
        prev_char = ""
        word_pos = 0
        sentence_pos = 0
        for wi, word in enumerate(words):
            if wi > 0:
                await page.keyboard.press('Space')
                word_pos = 0
                sentence_pos += 1
                if words[wi-1].rstrip().endswith(('.', '!', '?')):
                    await behavior.delay("thinking_before_reply")
                elif words[wi-1].rstrip().endswith(','):
                    await asyncio.sleep(random.uniform(0.3, 1.0))
                else:
                    await asyncio.sleep(random.uniform(0.05, 0.3))
            for ci, char in enumerate(word):
                word_pos += 1
                sentence_pos += 1
                delay_ms = behavior.human_type_delay(char, prev_char, word_pos, sentence_pos)
                await page.keyboard.type(char, delay=delay_ms)
                prev_char = char
        await _delay(account_id, 0.5, 1.0)
        log.info("reddit.reply.text_entered", preview=text[:40])

        # Submit — scoped to target comment
        await _delay(account_id, 1.0, 2.0)
        submit_result = await page.evaluate("""(id) => {
            const hostComment = document.querySelector(`shreddit-comment[thingid="t1_${id}"]`);
            if (!hostComment) return { clicked: false, reason: 'no_host_comment' };

            const roots = [hostComment];
            if (hostComment.shadowRoot) roots.push(hostComment.shadowRoot);
            const walker = document.createTreeWalker(hostComment, NodeFilter.SHOW_ELEMENT);
            let node = walker.currentNode;
            while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }

            for (const root of roots) {
                const btns = root.querySelectorAll('button:not([disabled])');
                for (const btn of btns) {
                    const rect = btn.getBoundingClientRect();
                    if (rect.width === 0 || rect.height === 0) continue;
                    const t = btn.textContent.trim().toLowerCase();
                    if (t === 'reply' || t === 'comment' || t === 'save') {
                        btn.click();
                        return { clicked: true, text: btn.textContent.trim() };
                    }
                }
            }
            return { clicked: false, reason: 'no_submit_button' };
        }""", comment_id)

        if not submit_result or not submit_result.get("clicked"):
            log.info("reddit.reply.trying_ctrl_enter")
            await page.keyboard.press("Control+Enter")

        await _delay(account_id, 3.0, 5.0)

        elapsed = _ms() - start_ms

        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.store_content(account_id, text, post_url=post_url)
                await db.log_action(
                    account_id=account_id, action_type="REPLY", result="SUCCESS",
                    target_url=post_url, content_text=text,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
                await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.reply.db_log_failed", error=str(db_exc))

        log.info("reddit.reply.success", elapsed_ms=elapsed)
        return _ok({"comment_fullname": comment_fullname, "post_url": post_url})

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.reply.failed", error=error_msg)
        if db is not None:
            try:
                safe_pid = await safe_proxy_id(db, proxy_id)
                await db.log_action(
                    account_id=account_id, action_type="REPLY", result="FAILURE",
                    target_url=post_url, error_message=error_msg,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
            except Exception:
                pass
        return _fail(error_msg)


async def run_tool(
    page: Page,
    account_id: str,
    comment_fullname: str,
    post_url: str,
    text: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """AI agent entry point for replying to a Reddit comment.

    Args:
        page: Active Playwright page (logged in, stealth applied).
        account_id: Unique account identifier.
        comment_fullname: Fullname of the parent comment, e.g. "t1_abc123".
        post_url: Full URL of the Reddit post containing the comment.
        text: Reply text.
        db: Optional database instance for action logging.
        proxy_id: Optional proxy ID for logging.

    Returns:
        {success: bool, data: {comment_fullname: str, post_url: str}, error: str|None}
    """
    return await reply_to_comment(
        page=page, account_id=account_id, comment_fullname=comment_fullname,
        post_url=post_url, text=text, db=db, proxy_id=proxy_id,
    )
