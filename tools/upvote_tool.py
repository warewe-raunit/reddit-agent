"""
tools/upvote_tool.py — Reddit post upvote via visible UI click.

Primary action: trusted mouse click on the visible upvote control.
Before clicking: observation-style checks confirm the button is visible,
belongs to the intended post, and is not already upvoted.
After clicking: UI state verified before and after reload.
OAuth/API calls are read-only and used only for server-state verification.

Usage:
    result = await run_tool(page, account_id, post_url="https://reddit.com/r/...")
    # result: {success: bool, data: {click_used, verified, selector, coords,
    #          before_state, after_state, verification_source}, error: str|None}
"""

from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Optional
from urllib.parse import quote_plus, urlparse

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, _delay, _random_scroll, ensure_token_captured,
    safe_proxy_id, _ok, _fail, _ms, _human_type,
    _smooth_wheel_scroll,
)

logger = structlog.get_logger(__name__)


def _target_post_id_from_url(post_url: str) -> str:
    match = re.search(r"/comments/([a-z0-9]+)/?", post_url, flags=re.IGNORECASE)
    return match.group(1).lower() if match else ""


def _post_search_query_from_url(post_url: str) -> str:
    """Build a human-readable Reddit search query from a post permalink."""
    parsed = urlparse(post_url)
    parts = [part for part in parsed.path.split("/") if part]
    title_slug = ""
    subreddit = ""

    for idx, part in enumerate(parts):
        if part.lower() == "r" and idx + 1 < len(parts):
            subreddit = parts[idx + 1]
        if part.lower() == "comments" and idx + 2 < len(parts):
            title_slug = parts[idx + 2]
            break

    title = re.sub(r"[_-]+", " ", title_slug)
    title = re.sub(r"[^a-zA-Z0-9\s']", " ", title)
    title = re.sub(r"\s+", " ", title).strip()

    if title:
        return title[:120]
    post_id = _target_post_id_from_url(post_url)
    if subreddit and post_id:
        return f"{post_id} {subreddit}"
    return post_id or post_url


def _subreddit_from_url(post_url: str) -> str:
    parsed = urlparse(post_url)
    parts = [part for part in parsed.path.split("/") if part]
    for idx, part in enumerate(parts):
        if part.lower() == "r" and idx + 1 < len(parts):
            return parts[idx + 1]
    return ""


def _post_search_queries_from_url(post_url: str) -> list[str]:
    """Return human search attempts from broad/title-first to exact-id fallback."""
    primary = _post_search_query_from_url(post_url)
    post_id = _target_post_id_from_url(post_url)
    subreddit = _subreddit_from_url(post_url)
    queries: list[str] = []

    def add(query: str) -> None:
        normalized = re.sub(r"\s+", " ", query or "").strip()
        if normalized and normalized not in queries:
            queries.append(normalized)

    add(primary)
    if primary and subreddit and post_id not in primary:
        add(f"{primary} r/{subreddit}")
    if post_id:
        add(post_id)
    return queries or [post_url]


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


async def _smooth_scroll_to(page: Page, target_scroll_y: float) -> float:
    """Move toward a document Y position using the shared scroll gesture.

    The shared helper picks curved touch swipes on mobile and wheel scrolling on
    desktop. JavaScript scroll is kept as a final precision fallback only.
    """
    viewport = await page.evaluate("""() => ({
        scrollY: window.scrollY,
        maxY: Math.max(0, document.documentElement.scrollHeight - window.innerHeight),
    })""")
    current_y = float(viewport.get("scrollY") or 0)
    max_y = float(viewport.get("maxY") or 0)
    target_y = max(0.0, min(float(target_scroll_y), max_y))
    delta = target_y - current_y
    if abs(delta) < 12:
        return current_y

    await _smooth_wheel_scroll(page, int(delta))
    await asyncio.sleep(random.uniform(0.25, 0.55))
    current_y = float(await page.evaluate("() => window.scrollY"))

    correction = target_y - current_y
    if abs(correction) > 35:
        await _smooth_wheel_scroll(page, int(correction))
        await asyncio.sleep(random.uniform(0.25, 0.55))
        current_y = float(await page.evaluate("() => window.scrollY"))
        correction = target_y - current_y

    if abs(correction) > 45:
        await page.evaluate(
            "(top) => window.scrollTo({ top, behavior: 'smooth' })",
            target_y,
        )
        await asyncio.sleep(random.uniform(0.6, 1.1))
        current_y = float(await page.evaluate("() => window.scrollY"))

    return current_y


_POST_UPVOTE_BUTTON_SCRIPT = """() => {
    const isVisible = (el) => {
        if (!el || !el.getBoundingClientRect) return false;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return false;
        const style = window.getComputedStyle(el);
        return style.display !== 'none'
            && style.visibility !== 'hidden'
            && parseFloat(style.opacity || '1') > 0;
    };

    const pagePath = window.location.pathname;
    const idMatch = pagePath.match(/\\/comments\\/([^\\/\\?#]+)/i);
    const targetPostId = idMatch ? 't3_' + idMatch[1] : '';

    const roots = [document];
    const addShadowRoots = (root) => {
        let nodes = [];
        try { nodes = root.querySelectorAll ? root.querySelectorAll('*') : []; } catch (_) { return; }
        for (const node of nodes) {
            if (node.shadowRoot && !roots.includes(node.shadowRoot)) {
                roots.push(node.shadowRoot);
                addShadowRoots(node.shadowRoot);
            }
        }
    };
    addShadowRoots(document);

    const postSelectors = [
        'shreddit-post',
        'shreddit-feed-post',
        '[data-testid="post-container"]',
        '[data-adclicklocation="title"]',
        '.Post',
        'article',
    ];
    const commentSelectors = [
        'shreddit-comment',
        '[thingid^="t1_"]',
        '[data-fullname^="t1_"]',
        '[data-thing-id^="t1_"]',
        '[id^="t1_"]',
    ];
    const upvoteSelectors = [
        'button[upvote]',
        '[upvote]',
        'button[data-click-id="upvote"]',
        '[data-click-id="upvote"]',
        'button[data-testid="upvote-button"]',
        '[data-testid="upvote-button"]',
        'button[aria-label*="upvote" i]',
        '[aria-label*="upvote" i]',
        'button[aria-label*="up vote" i]',
        '[aria-label*="up vote" i]',
        'button[aria-label*="up-vote" i]',
        '[aria-label*="up-vote" i]',
        'faceplate-tracker[slot="upvote"] button',
        'faceplate-tracker[slot="upvote"]',
        '[slot="upvote"] button',
        '[slot="upvote"]',
        'faceplate-vote-button[upvote] button',
        'faceplate-vote-button[upvote]',
        'button[vote-direction="up"]',
        '[vote-direction="up"]',
        'button[icon-name="upvote-outline"]',
        '[icon-name="upvote-outline"]',
        '[id*="upvote" i] button',
        '[id*="vote-arrows" i] button',
    ];

    const collect = (selectorList, searchRoots = roots) => {
        const found = [];
        const seen = new Set();
        for (const root of searchRoots) {
            for (const sel of selectorList) {
                let nodes = [];
                try { nodes = root.querySelectorAll(sel); } catch (_) { continue; }
                for (const node of nodes) {
                    if (!seen.has(node)) {
                        seen.add(node);
                        found.push(node);
                    }
                }
            }
        }
        return found;
    };

    const postCandidates = collect(postSelectors);
    const postMatchesTarget = (post) => {
        if (!targetPostId) return true;
        const attrs = [
            post.getAttribute?.('id'),
            post.getAttribute?.('thingid'),
            post.getAttribute?.('data-fullname'),
            post.getAttribute?.('data-thing-id'),
            post.getAttribute?.('post-id'),
        ].filter(Boolean).map(String);
        if (attrs.some(v => v === targetPostId || v.endsWith(targetPostId.replace(/^t3_/, '')))) return true;

        const hrefAttrs = [
            post.getAttribute?.('permalink'),
            post.getAttribute?.('content-href'),
            post.getAttribute?.('href'),
        ].filter(Boolean).map(String);
        return hrefAttrs.some(href => href.includes(pagePath) || pagePath.includes(href));
    };
    const matchedPosts = postCandidates.filter(postMatchesTarget);
    const scopedHosts = matchedPosts.length ? matchedPosts : postCandidates;

    const firstCommentTop = (() => {
        let top = Infinity;
        for (const comment of collect(commentSelectors)) {
            if (!isVisible(comment)) continue;
            const rect = comment.getBoundingClientRect();
            top = Math.min(top, rect.top + window.scrollY);
        }
        return top;
    })();

    const searchEntries = [];
    const seenRoots = new Set();
    const addSearchRoot = (root, scoped) => {
        if (!root || seenRoots.has(root)) return;
        seenRoots.add(root);
        searchEntries.push({ root, scoped });
    };
    const addHostAndShadowRoots = (host) => {
        addSearchRoot(host, true);
        if (host.shadowRoot) addSearchRoot(host.shadowRoot, true);
        let nodes = [];
        try { nodes = host.querySelectorAll ? host.querySelectorAll('*') : []; } catch (_) { nodes = []; }
        for (const node of nodes) {
            if (node.shadowRoot) addSearchRoot(node.shadowRoot, true);
        }
    };
    scopedHosts.forEach(addHostAndShadowRoots);
    roots.forEach(root => addSearchRoot(root, false));

    const looksAlreadyUpvoted = (el) => {
        const label = (
            el.getAttribute?.('aria-label') ||
            el.getAttribute?.('title') ||
            el.textContent ||
            ''
        ).toLowerCase();
        const className = (el.className || '').toString().toLowerCase();
        const voteStateEl = el.closest?.('[vote-state]');
        const voteState = (voteStateEl?.getAttribute?.('vote-state') || '').toUpperCase();
        return el.getAttribute?.('aria-pressed') === 'true'
            || voteState === 'UP'
            || className.includes('upvoted')
            || label.includes('remove upvote')
            || label.includes('upvoted');
    };

    const isCommentVote = (el) => !!el.closest?.(commentSelectors.join(','));
    const beforeComments = (el) => {
        if (firstCommentTop === Infinity) return true;
        const rect = el.getBoundingClientRect();
        return rect.top + window.scrollY < firstCommentTop - 4;
    };

    for (const { root, scoped } of searchEntries) {
        for (const sel of upvoteSelectors) {
            let candidates = [];
            try { candidates = root.querySelectorAll(sel); } catch (_) { continue; }
            for (const btn of candidates) {
                if (!isVisible(btn)) continue;
                if (isCommentVote(btn)) continue;
                if (!scoped && !beforeComments(btn)) continue;

                const rect = btn.getBoundingClientRect();
                if (looksAlreadyUpvoted(btn)) {
                    return {
                        found: true,
                        already: true,
                        source: scoped ? 'post-scope-state' : 'document-state',
                        selector: sel,
                    };
                }
                return {
                    found: true,
                    already: false,
                    absY: rect.top + window.scrollY,
                    absX: rect.left + window.scrollX,
                    width: rect.width,
                    height: rect.height,
                    selector: sel,
                    source: scoped ? 'post-scope' : 'document-before-comments',
                    viewportHeight: window.innerHeight,
                    pageUrl: pagePath,
                    targetPostId,
                };
            }
        }
    }

    const openApp = [...document.querySelectorAll('a, button')]
        .some(el => isVisible(el) && /open app/i.test(el.textContent || el.getAttribute('aria-label') || ''));
    const readMore = [...document.querySelectorAll('button, [role="button"]')]
        .some(el => isVisible(el) && /read more/i.test(el.textContent || el.getAttribute('aria-label') || ''));
    return {
        found: false,
        reason: openApp ? 'mobile_web_vote_control_not_rendered' : 'not_found',
        openApp,
        readMore,
        postCandidates: postCandidates.length,
        matchedPosts: matchedPosts.length,
        firstCommentTop: firstCommentTop === Infinity ? null : firstCommentTop,
    };
}"""


_SEARCH_INPUT_SCRIPT = """() => {
    const isVisible = (el) => {
        if (!el || !el.getBoundingClientRect) return false;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return false;
        const style = window.getComputedStyle(el);
        return style.display !== 'none'
            && style.visibility !== 'hidden'
            && parseFloat(style.opacity || '1') > 0;
    };
    const roots = [document];
    const addShadowRoots = (root) => {
        let nodes = [];
        try { nodes = root.querySelectorAll ? root.querySelectorAll('*') : []; } catch (_) { return; }
        for (const node of nodes) {
            if (node.shadowRoot && !roots.includes(node.shadowRoot)) {
                roots.push(node.shadowRoot);
                addShadowRoots(node.shadowRoot);
            }
        }
    };
    addShadowRoots(document);

    const selectors = [
        'input[name="q"]',
        'input[type="search"]',
        'input[placeholder*="Search" i]',
        'textarea[placeholder*="Search" i]',
        '[role="searchbox"]',
        'search-input input',
        'reddit-search-large input',
    ];
    for (const root of roots) {
        for (const sel of selectors) {
            let candidates = [];
            try { candidates = root.querySelectorAll(sel); } catch (_) { continue; }
            for (const el of candidates) {
                if (!isVisible(el)) continue;
                const rect = el.getBoundingClientRect();
                return {
                    found: true,
                    x: rect.left + rect.width / 2,
                    y: rect.top + rect.height / 2,
                    selector: sel,
                };
            }
        }
    }
    return { found: false };
}"""


_SEARCH_TRIGGER_SCRIPT = """() => {
    const isVisible = (el) => {
        if (!el || !el.getBoundingClientRect) return false;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return false;
        const style = window.getComputedStyle(el);
        return style.display !== 'none'
            && style.visibility !== 'hidden'
            && parseFloat(style.opacity || '1') > 0;
    };
    const candidates = [
        ...document.querySelectorAll('a[href*="/search"], button[aria-label*="search" i], [role="button"][aria-label*="search" i]')
    ];
    for (const el of candidates) {
        if (!isVisible(el)) continue;
        const label = (el.textContent || el.getAttribute('aria-label') || '').trim();
        const href = el.getAttribute('href') || '';
        if (!/search/i.test(label + ' ' + href)) continue;
        const rect = el.getBoundingClientRect();
        return {
            found: true,
            x: rect.left + rect.width / 2,
            y: rect.top + rect.height / 2,
        };
    }
    return { found: false };
}"""


_SEARCH_RESULT_LINK_SCRIPT = """(targetPostId) => {
    const isVisible = (el) => {
        if (!el || !el.getBoundingClientRect) return false;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return false;
        const style = window.getComputedStyle(el);
        return style.display !== 'none'
            && style.visibility !== 'hidden'
            && parseFloat(style.opacity || '1') > 0;
    };
    const normalizedTarget = String(targetPostId || '').toLowerCase();
    const marker = `reddit-agent-result-${normalizedTarget}`;
    const roots = [document];
    const addShadowRoots = (root) => {
        let nodes = [];
        try { nodes = root.querySelectorAll ? root.querySelectorAll('*') : []; } catch (_) { return; }
        for (const node of nodes) {
            if (node.shadowRoot && !roots.includes(node.shadowRoot)) {
                roots.push(node.shadowRoot);
                addShadowRoots(node.shadowRoot);
            }
        }
    };
    addShadowRoots(document);
    for (const root of roots) {
        try {
            root.querySelectorAll('[data-agent-search-result]').forEach(el => {
                el.removeAttribute('data-agent-search-result');
            });
        } catch (_) {}
    }

    const collect = (selector) => {
        const found = [];
        const seen = new Set();
        for (const root of roots) {
            let nodes = [];
            try { nodes = root.querySelectorAll(selector); } catch (_) { continue; }
            for (const node of nodes) {
                if (seen.has(node)) continue;
                seen.add(node);
                found.push(node);
            }
        }
        return found;
    };
    const postIdFromHref = (href) => {
        const match = String(href || '').match(/\\/comments\\/([a-z0-9]+)/i);
        return match ? match[1].toLowerCase() : '';
    };
    const hrefOf = (anchor) => anchor.href || anchor.getAttribute('href') || '';
    const anchors = collect('a[href*="/comments/"]');
    const seen = new Set();
    for (const anchor of anchors) {
        const href = hrefOf(anchor);
        if (postIdFromHref(href) !== normalizedTarget) continue;
        if (seen.has(href)) continue;
        seen.add(href);
        const container = anchor.closest('search-telemetry-tracker, shreddit-post, shreddit-feed-post, article, li, [data-testid="post-container"], [data-testid*="search-result"], div') || anchor;
        const visibleTargets = [
            anchor,
            ...(container.querySelectorAll ? container.querySelectorAll('a[href*="/comments/"], a[data-testid*="post-title" i], a[slot="title"], a[role="link"], a, [role="link"]') : []),
        ].filter(el => isVisible(el));
        const clickTarget = visibleTargets.find(el => postIdFromHref(hrefOf(el)) === normalizedTarget)
            || visibleTargets.find(el => ((el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim().length > 6))
            || (isVisible(container) ? container : null);
        if (!clickTarget) continue;

        try { clickTarget.setAttribute('data-agent-search-result', marker); } catch (_) {}
        const rect = clickTarget.getBoundingClientRect();
        return {
            found: true,
            href,
            selector: `[data-agent-search-result="${marker}"]`,
            title: (clickTarget.innerText || anchor.innerText || clickTarget.getAttribute('aria-label') || '').replace(/\\s+/g, ' ').trim(),
            context: ((container && container.innerText) || '').replace(/\\s+/g, ' ').trim().slice(0, 240),
            clickTag: (clickTarget.tagName || '').toLowerCase(),
            x: rect.left + Math.min(rect.width / 2, 160),
            y: rect.top + Math.min(rect.height / 2, 24),
        };
    }
    return { found: false, resultCount: anchors.length };
}"""


async def _locate_post_upvote_button(page: Page) -> dict:
    """Find a visible post upvote control across desktop and mobile Reddit DOMs."""
    return await page.evaluate(_POST_UPVOTE_BUTTON_SCRIPT)


async def _find_search_input(page: Page) -> dict:
    return await page.evaluate(_SEARCH_INPUT_SCRIPT)


async def _click_point(page: Page, x: float, y: float) -> None:
    click_x = int(x)
    click_y = int(y)
    await page.mouse.move(
        click_x + random.randint(-18, 18),
        click_y + random.randint(-10, 10),
        steps=random.randint(5, 12),
    )
    await asyncio.sleep(random.uniform(0.08, 0.25))
    await page.mouse.move(click_x, click_y, steps=random.randint(3, 7))
    await asyncio.sleep(random.uniform(0.04, 0.12))
    await page.mouse.click(click_x, click_y)


async def _open_search_box(page: Page, account_id: str) -> dict:
    search_input = await _find_search_input(page)
    if search_input.get("found"):
        return search_input

    trigger = await page.evaluate(_SEARCH_TRIGGER_SCRIPT)
    if trigger.get("found"):
        await _click_point(page, trigger["x"], trigger["y"])
        await _delay(account_id, 0.5, 1.1)
        search_input = await _find_search_input(page)
        if search_input.get("found"):
            return search_input

    await page.goto("https://www.reddit.com/search/", wait_until="domcontentloaded", timeout=60_000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8_000)
    except Exception:
        pass
    await _delay(account_id, 0.8, 1.5)
    return await _find_search_input(page)


async def _type_reddit_search(page: Page, account_id: str, query: str) -> None:
    search_input = await _open_search_box(page, account_id)
    if not search_input.get("found"):
        raise RuntimeError("Reddit search box not found")

    await _click_point(page, search_input["x"], search_input["y"])
    focused = await page.evaluate("""() => {
        const active = document.activeElement;
        if (!active) return false;
        if (active.matches('input, textarea, [contenteditable="true"], [role="searchbox"]')) return true;
        const nested = active.querySelector?.('input, textarea, [contenteditable="true"], [role="searchbox"]');
        if (nested) {
            nested.focus();
            return true;
        }
        return false;
    }""")
    if not focused:
        raise RuntimeError("Reddit search box could not be focused")

    await page.keyboard.press("Control+A")
    await page.keyboard.press("Backspace")

    active_handle = await page.evaluate_handle("() => document.activeElement")
    active_element = active_handle.as_element()
    if not active_element:
        await active_handle.dispose()
        raise RuntimeError("Reddit search box active element was not editable")
    await _human_type(page, active_element, query, account_id)
    await active_handle.dispose()
    await _delay(account_id, 0.2, 0.6)
    await page.keyboard.press("Enter")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=12_000)
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=8_000)
    except Exception:
        pass
    await _delay(account_id, 1.0, 2.0)

    # Some mobile builds focus the field but do not submit on Enter; keep the
    # typed interaction, then use Reddit's normal search URL as a fallback.
    if "/search/" not in page.url or "q=" not in page.url:
        await page.goto(
            f"https://www.reddit.com/search/?q={quote_plus(query)}&type=link&sort=relevance",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass
        await _delay(account_id, 1.0, 2.0)


async def _wait_for_target_post_url(page: Page, target_post_id: str, timeout_ms: int = 12_000) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
    while asyncio.get_running_loop().time() < deadline:
        if _target_post_id_from_url(page.url) == target_post_id:
            return True
        await asyncio.sleep(0.25)
    return _target_post_id_from_url(page.url) == target_post_id


async def _settle_after_click(page: Page) -> None:
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=8_000)
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=5_000)
    except Exception:
        pass


async def _activate_search_result(page: Page, account_id: str, result: dict, target_post_id: str) -> bool:
    if _target_post_id_from_url(page.url) == target_post_id:
        result["activation"] = "already_on_target"
        return True

    selector = result.get("selector")
    if selector:
        locator = page.locator(selector).first

        try:
            await locator.scroll_into_view_if_needed(timeout=6_000)
            box = await locator.bounding_box()
            if box:
                await page.mouse.move(
                    int(box["x"] + min(box["width"] / 2, 160)),
                    int(box["y"] + min(box["height"] / 2, 24)),
                    steps=random.randint(6, 12),
                )
                await asyncio.sleep(random.uniform(0.12, 0.35))
            await locator.click(timeout=8_000)
            await _settle_after_click(page)
            if await _wait_for_target_post_url(page, target_post_id, timeout_ms=8_000):
                result["activation"] = "locator_click"
                return True
        except Exception as exc:
            result["locator_click_error"] = str(exc)[:180]

        try:
            await locator.focus(timeout=5_000)
            await _delay(account_id, 0.15, 0.35)
            await page.keyboard.press("Enter")
            await _settle_after_click(page)
            if await _wait_for_target_post_url(page, target_post_id, timeout_ms=8_000):
                result["activation"] = "keyboard_enter"
                return True
        except Exception as exc:
            result["keyboard_enter_error"] = str(exc)[:180]

        try:
            await locator.evaluate("(el) => el.click()")
            await _settle_after_click(page)
            if await _wait_for_target_post_url(page, target_post_id, timeout_ms=8_000):
                result["activation"] = "dom_click"
                return True
        except Exception as exc:
            result["dom_click_error"] = str(exc)[:180]

    if result.get("x") is not None and result.get("y") is not None:
        try:
            await _click_point(page, result["x"], result["y"])
            await _settle_after_click(page)
            if await _wait_for_target_post_url(page, target_post_id, timeout_ms=8_000):
                result["activation"] = "coordinate_click"
                return True
        except Exception as exc:
            result["coordinate_click_error"] = str(exc)[:180]

    # Last resort: this href was discovered from the visible search result,
    # after normal click/keyboard activation failed to make Reddit navigate.
    href = result.get("href")
    if href:
        try:
            await page.goto(href, wait_until="domcontentloaded", timeout=60_000)
            await _settle_after_click(page)
            if await _wait_for_target_post_url(page, target_post_id, timeout_ms=8_000):
                result["activation"] = "search_result_href"
                return True
        except Exception as exc:
            result["href_navigation_error"] = str(exc)[:180]

    return False


async def _click_matching_search_result(page: Page, account_id: str, target_post_id: str) -> dict:
    if _target_post_id_from_url(page.url) == target_post_id:
        return {"found": True, "targetPostId": target_post_id, "activation": "already_on_target"}

    search_url = page.url
    last_result = None
    for attempt in range(5):
        result = await page.evaluate(_SEARCH_RESULT_LINK_SCRIPT, target_post_id)
        last_result = result
        if result.get("found"):
            navigated = await _activate_search_result(page, account_id, result, target_post_id)
            if navigated:
                await _delay(account_id, 1.0, 2.0)
                result["navigated"] = True
                return result

            result["navigated"] = False
            if "/search/" not in page.url and _target_post_id_from_url(page.url) != target_post_id:
                try:
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)
                    await _settle_after_click(page)
                except Exception:
                    pass

        await _random_scroll(page, account_id, read_min=0.4, read_max=0.9)

    return {"found": False, "targetPostId": target_post_id, "lastResult": last_result}


async def _navigate_to_post_via_search(page: Page, account_id: str, post_url: str, log=None) -> dict:
    """Reach a target post through Reddit search results instead of direct URL navigation."""
    target_post_id = _target_post_id_from_url(post_url)
    if not target_post_id:
        raise RuntimeError("Could not identify Reddit post ID from URL")

    queries = _post_search_queries_from_url(post_url)
    query = queries[0]
    if log is not None:
        log.info("reddit.upvote.search_navigation_start", query=query, target_post_id=target_post_id)

    await page.goto("https://www.reddit.com/", wait_until="domcontentloaded", timeout=60_000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8_000)
    except Exception:
        pass
    await _delay(account_id, 1.0, 2.0)

    match = None
    for attempt_index, query in enumerate(queries, start=1):
        if log is not None:
            log.info(
                "reddit.upvote.search_query_attempt",
                query=query,
                attempt=attempt_index,
                total=len(queries),
                target_post_id=target_post_id,
            )
        await _type_reddit_search(page, account_id, query)
        match = await _click_matching_search_result(page, account_id, target_post_id)
        if match.get("found") and _target_post_id_from_url(page.url) == target_post_id:
            break
        if log is not None:
            log.info(
                "reddit.upvote.search_query_no_target_navigation",
                query=query,
                url=page.url,
                match=match,
            )
    else:
        attempted = " | ".join(queries)
        raise RuntimeError(f"Target post was not reached from Reddit search results. Tried: {attempted}")

    landed_post_id = _target_post_id_from_url(page.url)
    if landed_post_id != target_post_id:
        raise RuntimeError(
            f"Search result opened a different post: expected {target_post_id}, got {landed_post_id or page.url}"
        )

    if log is not None:
        log.info("reddit.upvote.search_navigation_done", url=page.url, title=match.get("title", ""))
    return {"query": query, "target_post_id": target_post_id, "result": match, "url": page.url}


async def _expand_mobile_post_if_needed(page: Page, account_id: str) -> bool:
    """Click a visible mobile 'Read more' expander if Reddit collapses the post."""
    expander = await page.evaluate("""() => {
        const isVisible = (el) => {
            if (!el || !el.getBoundingClientRect) return false;
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) return false;
            const style = window.getComputedStyle(el);
            return style.display !== 'none'
                && style.visibility !== 'hidden'
                && parseFloat(style.opacity || '1') > 0;
        };
        const candidates = [...document.querySelectorAll('button, [role="button"]')];
        for (const el of candidates) {
            const label = (el.textContent || el.getAttribute('aria-label') || '').replace(/\\s+/g, ' ').trim();
            if (!/^read more$/i.test(label) || !isVisible(el)) continue;
            const rect = el.getBoundingClientRect();
            return {
                x: rect.left + rect.width / 2,
                y: rect.top + rect.height / 2,
            };
        }
        return null;
    }""")
    if not expander:
        return False

    click_x = int(expander["x"])
    click_y = int(expander["y"])
    await page.mouse.move(
        click_x + random.randint(-18, 18),
        click_y + random.randint(-10, 10),
        steps=random.randint(5, 12),
    )
    await asyncio.sleep(random.uniform(0.08, 0.25))
    await page.mouse.click(click_x, click_y)
    await _delay(account_id, 0.8, 1.4)
    return True


async def _ui_verify_post_upvoted(page: Page) -> bool:
    """Check visible UI state for a post upvote across supported Reddit layouts."""
    btn_info = await _locate_post_upvote_button(page)
    if btn_info and btn_info.get("already"):
        return True
    return bool(await page.evaluate("""() => {
        const posts = [
            ...document.querySelectorAll('shreddit-post, shreddit-feed-post, [data-testid="post-container"], .Post, article')
        ];
        for (const post of posts) {
            const voteState = (
                post.getAttribute('vote-state') ||
                post.querySelector?.('[vote-state="UP"]')?.getAttribute('vote-state') ||
                ''
            ).toUpperCase();
            if (voteState === 'UP') return true;
            const upvoted = post.querySelector?.('[aria-pressed="true"], .upvoted, [class*="upvoted"], [aria-label*="remove upvote" i]');
            if (upvoted) return true;
        }
        return false;
    }"""))


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
        navigation = await _navigate_to_post_via_search(page, account_id, post_url, log)

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

        await _expand_mobile_post_if_needed(page, account_id)

        await _random_scroll(page, account_id)
        await _delay(account_id, 0.5, 1.5)
        await _expand_mobile_post_if_needed(page, account_id)

        token = (os.getenv("REDDIT_OAUTH_ACCESS_TOKEN") or "").strip()
        if token:
            setattr(page, "_reddit_bearer_token", token)
        else:
            token = await ensure_token_captured(page, log)
        if not token:
            token = await _token_v2_candidate(page)
        user_agent = _reddit_user_agent(account_id)

        post_data = await page.evaluate("""() => {
            const post = document.querySelector('shreddit-post, shreddit-feed-post, [data-testid="post-container"], .Post, article');
            const idMatch = window.location.pathname.match(/\\/comments\\/([^\\/\\?#]+)/i);
            const urlPostId = idMatch ? 't3_' + idMatch[1] : null;
            if (!post) return { postId: urlPostId, score: null };
            return {
                postId: post.getAttribute('id') || post.getAttribute('thingid') ||
                    post.getAttribute('data-fullname') || post.getAttribute('data-thing-id') ||
                    post.getAttribute('post-id') || urlPostId,
                score: post.getAttribute('score'),
            };
        }""")

        click_used = False
        server_verified = False
        ui_verified_after_reload = False
        score_after_reload = None
        post_id = None

        if post_data and post_data.get("postId"):
            raw_post_id = post_data["postId"]
            post_id = raw_post_id if raw_post_id.startswith("t3_") else f"t3_{raw_post_id}"

        # Trusted mouse click is the primary action — vote stays inside Reddit's
        # normal frontend event flow. API calls are read-only verification only.
        verified = False
        log.info("reddit.upvote.trusted_click_primary")

        # Observation-style pre-check: verify the upvote control is visible,
        # belongs to the intended post, and is not already upvoted.
        btn_info = await _locate_post_upvote_button(page)

        if not btn_info or not btn_info.get("found"):
            raise RuntimeError(f"Upvote button not found: {btn_info}")

        if btn_info.get("already"):
            log.info("reddit.upvote.already_upvoted", source=btn_info.get("source"))
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
            return _ok({
                "already_upvoted": True,
                "api_used": False,
                "click_used": False,
                "before_state": "upvoted",
                "verification_source": btn_info.get("source", "ui_pre_check"),
            })

        # Scroll button into viewport center, then click with trusted event
        target_scroll_y = btn_info["absY"] - (btn_info["viewportHeight"] / 2)
        await _smooth_scroll_to(page, target_scroll_y)

        # Re-read fresh coordinates after scroll with the same mobile-aware finder.
        fresh_btn_info = await _locate_post_upvote_button(page)
        if fresh_btn_info and fresh_btn_info.get("already"):
            log.info("reddit.upvote.already_upvoted_after_scroll", source=fresh_btn_info.get("source"))
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
            return _ok({
                "already_upvoted": True,
                "api_used": False,
                "click_used": False,
                "before_state": "upvoted",
                "verification_source": fresh_btn_info.get("source", "ui_after_scroll"),
            })
        elif fresh_btn_info and fresh_btn_info.get("found"):
            btn_info = fresh_btn_info

        viewport_after_scroll = await page.evaluate("""() => ({
            scrollX: window.scrollX,
            scrollY: window.scrollY,
            width: window.innerWidth,
            height: window.innerHeight,
        })""")
        click_x = int(btn_info["absX"] - viewport_after_scroll["scrollX"] + btn_info["width"] / 2)
        click_y = int(btn_info["absY"] - viewport_after_scroll["scrollY"] + btn_info["height"] / 2)
        if click_x < 0 or click_y < 0 or click_x > viewport_after_scroll["width"] or click_y > viewport_after_scroll["height"]:
            raise RuntimeError(f"Upvote button coordinates not visible after scroll: {btn_info}")

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
        verified = await _ui_verify_post_upvoted(page)
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
            await _expand_mobile_post_if_needed(page, account_id)
            ui_verified_after_reload = await _ui_verify_post_upvoted(page)
            score_after_reload = await page.evaluate("""() => {
                const post = document.querySelector('shreddit-post, shreddit-feed-post, [data-testid="post-container"], .Post, article');
                return post ? (post.getAttribute('score') || null) : null;
            }""")
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
            "selector": btn_info.get("selector") if btn_info else None,
            "coords": {
                "x": int(btn_info.get("absX", 0) + btn_info.get("width", 0) / 2) if btn_info else None,
                "y": int(btn_info.get("absY", 0)) if btn_info else None,
            },
            "before_state": "not_upvoted",
            "verified": final_verified,
            "ui_verified_before_reload": verified,
            "ui_verified_after_reload": ui_verified_after_reload,
            "server_verified": server_verified,
            "score_before": post_data.get("score") if post_data else None,
            "score_after_reload": score_after_reload,
            "vote_responses": _vote_responses[-3:],
            "navigation": navigation,
            "verification_source": (
                "server" if server_verified
                else "ui_after_reload" if ui_verified_after_reload
                else "ui_before_reload" if verified
                else "unverified"
            ),
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
