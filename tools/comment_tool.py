"""
tools/comment_tool.py — Reddit comment tool for AI agents.

Stealth features:
- simulate_reading() before acting (critical anti-bot timing signal)
- Shadow DOM traversal for shreddit comment composer
- Lexical editor detection and activation with 15-attempt polling
- Behavior-engine typing (per-account personality, sentence-aware pauses)
- Blind-mode fallback when editor is in inaccessible shadow roots
- Comment verification via DOM count before/after
- Multiple submit strategies (activeElement → scoped → proximity → Ctrl+Enter)

Usage:
    result = await run_tool(page, account_id, post_url="https://...", text="Great post!")
    # result: {success: bool, data: {comment_url, verified}, error: str|None}
"""

from __future__ import annotations

import asyncio
import random
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, _random_scroll, _ok, _fail, _ms,
    _get_behavior_engine,
)

logger = structlog.get_logger(__name__)


async def comment(
    page: Page,
    account_id: str,
    post_url: str,
    text: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """Post a comment on a Reddit thread.

    Handles Reddit's new UI with shadow DOM (shreddit-* components)
    and Lexical rich-text editor.
    """
    log = logger.bind(account_id=account_id, post_url=post_url, action="COMMENT")
    start_ms = _ms()

    try:
        log.info("reddit.comment.navigating")
        await page.goto(post_url, wait_until="domcontentloaded", timeout=60_000)

        await simulate_reading(page, account_id)
        await _delay(account_id, 1.0, 2.0)

        try:
            await page.wait_for_selector(
                '[data-testid="post-container"], shreddit-post, .Post', timeout=15_000,
            )
        except Exception:
            log.warning("reddit.comment.post_container_not_found")

        await _random_scroll(page, account_id)
        await _delay(account_id)

        # Bring comment composer into view
        await page.evaluate("""() => {
            const targets = [
                'comment-composer-host', 'shreddit-comment-composer-host',
                'shreddit-comment-composer', 'shreddit-composer[slot="comment-composer"]',
                '[data-testid="comment-composer-button"]',
                '[placeholder*="conversation" i]', '[placeholder*="thought" i]',
            ];
            for (const sel of targets) {
                const el = document.querySelector(sel);
                if (!el) continue;
                const rect = el.getBoundingClientRect();
                if (rect.height > 0 && rect.width > 0) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    return true;
                }
            }
            return false;
        }""")
        await _delay(account_id, 0.8, 1.6)

        # Step 1: Activate the comment composer (new Reddit starts collapsed)
        activated = await page.evaluate("""() => {
            const triggers = [
                'shreddit-composer[slot="comment-composer"]', 'comment-composer-host',
                'shreddit-comment-composer-host', 'shreddit-comment-composer',
                '[data-testid="comment-composer-button"]',
                '[placeholder*="thought"]', '[placeholder*="comment"]',
                '[placeholder*="Comment"]', 'div[role="textbox"]',
            ];
            for (const sel of triggers) {
                const el = document.querySelector(sel);
                if (el && el.offsetParent !== null) { el.click(); return { activated: true, selector: sel }; }
            }
            const paras = document.querySelectorAll('p[data-placeholder]');
            for (const p of paras) {
                if (p.offsetParent !== null) { p.click(); return { activated: true, selector: 'p[data-placeholder]' }; }
            }
            const editables = document.querySelectorAll('div[contenteditable="true"]');
            for (const e of editables) {
                if (e.offsetParent !== null) { e.click(); return { activated: true, selector: 'contenteditable' }; }
            }
            return { activated: false };
        }""")
        log.info("reddit.comment.composer_activate", result=activated)
        await _delay(account_id, 2.0, 3.0)

        # Step 2: Find visible Lexical editor (poll up to 15 attempts)
        editor_ready = False
        blind_mode = False
        for attempt in range(15):
            editor_info = await page.evaluate("""() => {
                const selectors = [
                    'div[data-lexical-editor="true"]',
                    'div[contenteditable="true"][role="textbox"]',
                    'div[contenteditable="true"]',
                    'textarea[name="comment"]',
                ];
                const roots = [document];
                const composerHosts = document.querySelectorAll(
                    'comment-composer-host, shreddit-comment-composer-host,' +
                    'shreddit-comment-composer, shreddit-composer, [slot="comment-composer"]'
                );
                for (const host of composerHosts) {
                    roots.push(host);
                    if (host.shadowRoot) roots.push(host.shadowRoot);
                    const walker = document.createTreeWalker(host, NodeFilter.SHOW_ELEMENT);
                    let node = walker.currentNode;
                    while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }
                }
                for (const root of roots) {
                    for (const sel of selectors) {
                        const els = root.querySelectorAll(sel);
                        for (const el of els) {
                            const rect = el.getBoundingClientRect();
                            if (rect.height === 0 || rect.width === 0) continue;
                            const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                            if (aria.includes('search')) continue;
                            return { found: true, selector: sel, visible: true };
                        }
                    }
                }
                return { found: false, visible: false };
            }""")
            if editor_info and editor_info.get("visible"):
                log.info("reddit.comment.editor_found", attempt=attempt)
                editor_ready = True
                break
            await asyncio.sleep(0.5)

        if not editor_ready:
            fallback_focus = await page.evaluate("""() => {
                const triggers = [
                    'comment-composer-host', 'shreddit-comment-composer-host',
                    'shreddit-comment-composer', 'shreddit-composer[slot="comment-composer"]',
                    '[data-testid="comment-composer-button"]',
                    '[placeholder*="conversation" i]', '[placeholder*="thought" i]',
                    '[placeholder*="comment" i]', 'p[data-placeholder]',
                ];
                for (const sel of triggers) {
                    const el = document.querySelector(sel);
                    if (!el) continue;
                    const rect = el.getBoundingClientRect();
                    if (rect.height === 0 || rect.width === 0) continue;
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.click();
                    if (typeof el.focus === 'function') el.focus();
                    return { ok: true, selector: sel };
                }
                return { ok: false };
            }""")
            if not fallback_focus or not fallback_focus.get("ok"):
                raise RuntimeError("Could not find visible comment editor after activating composer")
            log.warning("reddit.comment.editor_not_queryable_using_blind_mode")
            await _delay(account_id, 0.5, 1.0)
            await page.keyboard.type(text, delay=random.randint(50, 150))
            blind_mode = True

        # Step 3: Focus and type into the editor
        if not blind_mode:
            focused = await page.evaluate("""() => {
                const selectors = [
                    'div[data-lexical-editor="true"]',
                    'div[contenteditable="true"][role="textbox"]',
                    'div[contenteditable="true"]',
                    'textarea[name="comment"]',
                ];
                const roots = [document];
                const composerHosts = document.querySelectorAll(
                    'comment-composer-host, shreddit-comment-composer-host,' +
                    'shreddit-comment-composer, shreddit-composer, [slot="comment-composer"]'
                );
                for (const host of composerHosts) {
                    roots.push(host);
                    if (host.shadowRoot) roots.push(host.shadowRoot);
                    const walker = document.createTreeWalker(host, NodeFilter.SHOW_ELEMENT);
                    let node = walker.currentNode;
                    while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }
                }
                for (const root of roots) {
                    for (const sel of selectors) {
                        const els = root.querySelectorAll(sel);
                        for (const el of els) {
                            const rect = el.getBoundingClientRect();
                            if (rect.height === 0 || rect.width === 0) continue;
                            const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                            if (aria.includes('search')) continue;
                            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            el.focus();
                            el.click();
                            return { focused: true, tag: el.tagName };
                        }
                    }
                }
                return { focused: false };
            }""")

            if not focused or not focused.get("focused"):
                raise RuntimeError("Could not focus comment editor")

            log.info("reddit.comment.editor_focused", tag=focused.get("tag"))
            await _delay(account_id, 0.5, 1.0)

            # Behavior-engine typing with sentence-boundary awareness
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

        # Verify text was entered
        editor_text = text if blind_mode else await page.evaluate("""() => {
            const selectors = [
                'div[data-lexical-editor="true"]',
                'div[contenteditable="true"][role="textbox"]',
                'textarea[name="comment"]',
            ];
            const roots = [document];
            const composerHosts = document.querySelectorAll(
                'comment-composer-host, shreddit-comment-composer-host,' +
                'shreddit-comment-composer, shreddit-composer, [slot="comment-composer"]'
            );
            for (const host of composerHosts) {
                roots.push(host);
                if (host.shadowRoot) roots.push(host.shadowRoot);
                const walker = document.createTreeWalker(host, NodeFilter.SHOW_ELEMENT);
                let node = walker.currentNode;
                while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }
            }
            for (const root of roots) {
                for (const sel of selectors) {
                    const els = root.querySelectorAll(sel);
                    for (const el of els) {
                        const value = (el.textContent || el.value || '').trim();
                        if (value.length > 0) return value;
                    }
                }
            }
            return '';
        }""")

        if not editor_text and not blind_mode:
            log.warning("reddit.comment.keyboard_type_failed_trying_execcommand")
            await page.evaluate("""(text) => {
                const sels = [
                    'div[data-lexical-editor="true"]',
                    'div[contenteditable="true"][role="textbox"]',
                    'div[contenteditable="true"]',
                ];
                const roots = [document];
                const composerHosts = document.querySelectorAll(
                    'comment-composer-host, shreddit-comment-composer-host,' +
                    'shreddit-comment-composer, shreddit-composer, [slot="comment-composer"]'
                );
                for (const host of composerHosts) {
                    roots.push(host);
                    if (host.shadowRoot) roots.push(host.shadowRoot);
                    const walker = document.createTreeWalker(host, NodeFilter.SHOW_ELEMENT);
                    let node = walker.currentNode;
                    while (node) { if (node.shadowRoot) roots.push(node.shadowRoot); node = walker.nextNode(); }
                }
                for (const root of roots) {
                    for (const sel of sels) {
                        const els = root.querySelectorAll(sel);
                        for (const el of els) {
                            if (el.offsetParent !== null || el.getBoundingClientRect().height > 0) {
                                el.focus();
                                document.execCommand('selectAll', false, null);
                                document.execCommand('delete', false, null);
                                document.execCommand('insertText', false, text);
                                el.dispatchEvent(new Event('input', { bubbles: true }));
                                return;
                            }
                        }
                    }
                }
            }""", text)
            await _delay(account_id, 0.5, 1.0)

        log.info("reddit.comment.text_entered", preview=text[:40])
        await _delay(account_id, 1.0, 2.0)

        # Count comments before submit (for verification)
        comment_count_before = await page.evaluate("""() => {
            return document.querySelectorAll('shreddit-comment, .Comment, [data-testid="comment"]').length;
        }""")

        # Step 4: Submit (3 strategies + Ctrl+Enter fallback)
        btn_clicked = await page.evaluate("""() => {
            // Strategy 1: Use activeElement to find the scoped composer button
            const active = document.activeElement;
            if (active && (active.getAttribute('contenteditable') === 'true' ||
                           active.getAttribute('data-lexical-editor') === 'true' ||
                           active.tagName === 'TEXTAREA')) {
                let container = active.parentElement;
                while (container && container !== document.body) {
                    const tag = container.tagName.toLowerCase();
                    if (tag === 'comment-composer-host' || tag === 'shreddit-comment-composer-host' ||
                        tag === 'shreddit-comment-composer' || tag === 'shreddit-composer' ||
                        tag === 'form' || container.getAttribute('slot') === 'comment-composer') {
                        const btns = container.querySelectorAll('button:not([disabled])');
                        for (const b of btns) {
                            const t = b.textContent.trim().toLowerCase();
                            if (t === 'comment' || t === 'reply' || t === 'save') {
                                b.click();
                                return { strategy: 'activeElement_composer', clicked: true, text: b.textContent.trim() };
                            }
                        }
                    }
                    container = container.parentElement;
                }
            }
            // Strategy 2: Scope to top-level comment composer
            const composerSelectors = [
                'comment-composer-host', 'shreddit-comment-composer-host',
                'shreddit-comment-composer', '[slot="comment-composer"]',
            ];
            for (const sel of composerSelectors) {
                const composer = document.querySelector(sel);
                if (!composer) continue;
                const btns = composer.querySelectorAll('button:not([disabled])');
                for (const btn of btns) {
                    const text = btn.textContent.trim().toLowerCase();
                    if (btn.offsetParent === null && btn.getBoundingClientRect().height === 0) continue;
                    if (text === 'comment' || text === 'save' || text === 'submit') {
                        btn.click();
                        return { strategy: 'composer_scoped', clicked: true, text: btn.textContent.trim() };
                    }
                }
            }
            // Strategy 3: Find closest Comment button to active editor
            if (active) {
                const activeRect = active.getBoundingClientRect();
                const allBtns = [...document.querySelectorAll('button:not([disabled])')].filter(b => {
                    const t = b.textContent.trim().toLowerCase();
                    return (t === 'comment' || t === 'reply') && b.offsetParent !== null;
                });
                if (allBtns.length > 0) {
                    let closest = null, minDist = Infinity;
                    for (const btn of allBtns) {
                        const btnRect = btn.getBoundingClientRect();
                        const dist = Math.abs(btnRect.top - activeRect.bottom);
                        if (dist < minDist) { minDist = dist; closest = btn; }
                    }
                    if (closest) { closest.click(); return { strategy: 'closest_proximity', clicked: true }; }
                }
            }
            return { strategy: 'none', clicked: false };
        }""")

        if btn_clicked and btn_clicked.get("clicked"):
            log.info("reddit.comment.submit_button_clicked", strategy=btn_clicked.get("strategy"))
        else:
            log.info("reddit.comment.trying_ctrl_enter")
            await page.keyboard.press("Control+Enter")

        await _delay(account_id, 3.0, 5.0)

        # Verify comment posted
        comment_count_after = await page.evaluate("""() => {
            return document.querySelectorAll('shreddit-comment, .Comment, [data-testid="comment"]').length;
        }""")
        actually_posted = comment_count_after > comment_count_before

        if not actually_posted:
            log.warning("reddit.comment.submit_failed_editor_still_full")
            await page.evaluate("""() => {
                const btns = [...document.querySelectorAll('button')];
                const commentBtn = btns.find(b => b.offsetParent !== null && !b.disabled &&
                    b.textContent.trim().toLowerCase() === 'comment');
                if (commentBtn) commentBtn.click();
            }""")
            await _delay(account_id, 3.0, 5.0)
            final_count = await page.evaluate("""() => {
                return document.querySelectorAll('shreddit-comment, .Comment, [data-testid="comment"]').length;
            }""")
            actually_posted = final_count > comment_count_before

        elapsed = _ms() - start_ms

        if db is not None:
            try:
                safe_pid = None
                if proxy_id:
                    row = await db.get_proxy(proxy_id)
                    safe_pid = proxy_id if row else None
                await db.store_content(account_id, text, post_url=post_url)
                await db.log_action(
                    account_id=account_id, action_type="COMMENT",
                    result="SUCCESS" if actually_posted else "UNVERIFIED",
                    target_url=post_url, content_text=text,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
                if actually_posted:
                    await db.increment_daily_action_count(account_id)
            except Exception as db_exc:
                log.warning("reddit.comment.db_log_failed", error=str(db_exc))

        log.info("reddit.comment.success", elapsed_ms=elapsed, verified=actually_posted)
        return _ok({"comment_url": page.url, "verified": actually_posted})

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.comment.failed", error=error_msg)
        if db is not None:
            try:
                safe_pid = None
                if proxy_id:
                    row = await db.get_proxy(proxy_id)
                    safe_pid = proxy_id if row else None
                await db.log_action(
                    account_id=account_id, action_type="COMMENT", result="FAILURE",
                    target_url=post_url, error_message=error_msg,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
            except Exception:
                pass
        return _fail(error_msg)


async def run_tool(
    page: Page,
    account_id: str,
    post_url: str,
    text: str,
    db=None,
    proxy_id: Optional[str] = None,
) -> dict:
    """AI agent entry point for posting a Reddit comment.

    Args:
        page: Active Playwright page (logged in, stealth applied).
        account_id: Unique account identifier.
        post_url: Full URL of the Reddit post to comment on.
        text: Comment text.
        db: Optional database instance for action logging.
        proxy_id: Optional proxy ID for logging.

    Returns:
        {success: bool, data: {comment_url: str, verified: bool}, error: str|None}
    """
    return await comment(
        page=page, account_id=account_id, post_url=post_url,
        text=text, db=db, proxy_id=proxy_id,
    )
