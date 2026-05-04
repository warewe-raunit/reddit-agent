"""
tools/stealth/helpers.py — Shared stealth helpers for all Reddit action tools.

Includes:
- Human-like delays (HumanBehaviorEngine-backed)
- Bezier curve mouse movement
- Human-like typing with typo simulation
- Reading simulation (scroll + mouse + idle)
- Reddit bearer token capture
- Ghost cursor click
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from typing import Optional

from playwright.async_api import Page

from tools.stealth.human_behavior import HumanBehaviorEngine, create_engine

# Per-account HumanBehaviorEngine cache
_behavior_engines: dict[str, HumanBehaviorEngine] = {}


def _ms() -> int:
    return int(time.monotonic() * 1000)


def _ok(data=None) -> dict:
    return {"success": True, "data": data, "error": None}


def _fail(error: str, data=None) -> dict:
    return {"success": False, "data": data, "error": error}


def _get_behavior_engine(account_id: str, timezone: int = 0) -> HumanBehaviorEngine:
    if account_id not in _behavior_engines:
        _behavior_engines[account_id] = create_engine(account_id, timezone=timezone)
    return _behavior_engines[account_id]


def _parse_count(text: str) -> int:
    text = text.strip().lower().replace(",", "")
    if not text or text in {"vote", "votes", "•", "-"}:
        return 0
    try:
        if text.endswith("k"):
            return int(float(text[:-1]) * 1000)
        if text.endswith("m"):
            return int(float(text[:-1]) * 1_000_000)
        return int(float(text))
    except ValueError:
        return 0


# ─────────────────────────────────────────────────────────────────
# Human-like delays
# ─────────────────────────────────────────────────────────────────

async def _delay(account_id: Optional[str] = None, min_s: Optional[float] = None,
                 max_s: Optional[float] = None, context: str = "between_pages") -> None:
    if account_id:
        behavior = _get_behavior_engine(account_id)
        await behavior.delay(context, min_s, max_s)
    else:
        default_min = min_s if min_s is not None else 2.0
        default_max = max_s if max_s is not None else 6.0
        await asyncio.sleep(random.uniform(default_min, default_max))


async def _random_scroll(
    page: Page,
    account_id: Optional[str] = None,
    read_min: Optional[float] = None,
    read_max: Optional[float] = None,
) -> None:
    if account_id:
        behavior = _get_behavior_engine(account_id)
        scroll_amount = behavior.human_scroll_distance()
    else:
        scroll_amount = random.randint(200, 900)
    await page.mouse.wheel(0, scroll_amount)
    if account_id:
        await _get_behavior_engine(account_id).delay("reading", min_s=read_min, max_s=read_max)
    else:
        lo = read_min if read_min is not None else 1.5
        hi = read_max if read_max is not None else 4.0
        await asyncio.sleep(random.uniform(lo, hi))
    if random.random() < 0.3:
        await page.mouse.wheel(0, -random.randint(50, 200))
        if account_id:
            await _delay(account_id, context="pre_click")
        else:
            await asyncio.sleep(random.uniform(0.3, 0.8))


# ─────────────────────────────────────────────────────────────────
# Human-like typing
# ─────────────────────────────────────────────────────────────────

async def _human_type(page: Page, element, text: str, account_id: Optional[str] = None) -> None:
    prev_char = ""
    word_pos = 0
    sentence_pos = 0
    for char in text:
        if char == ' ':
            word_pos = 0
            sentence_pos += 1
        else:
            word_pos += 1
            sentence_pos += 1
        if account_id:
            behavior = _get_behavior_engine(account_id)
            delay_ms = behavior.human_type_delay(char, prev_char, word_pos, sentence_pos)
        else:
            delay_ms = random.randint(45, 280)
        if char.isalpha() and random.random() < 0.03:
            nearby = 'abcdefghijklmnopqrstuvwxyz'
            wrong = random.choice(nearby.replace(char.lower(), ''))
            await element.type(wrong, delay=delay_ms)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            await element.press('Backspace')
            await asyncio.sleep(random.uniform(0.1, 0.25))
        await element.type(char, delay=delay_ms)
        if char == ' ' and random.random() < 0.15:
            await asyncio.sleep(random.uniform(0.4, 1.2))
        prev_char = char


# ─────────────────────────────────────────────────────────────────
# Bezier curve mouse movement
# ─────────────────────────────────────────────────────────────────

def _bezier_point(t: float, p0: tuple, p1: tuple, p2: tuple, p3: tuple) -> tuple[int, int]:
    t2, t3 = t * t, t * t * t
    mt, mt2, mt3 = 1 - t, (1-t)**2, (1-t)**3
    x = mt3*p0[0] + 3*mt2*t*p1[0] + 3*mt*t2*p2[0] + t3*p3[0]
    y = mt3*p0[1] + 3*mt2*t*p1[1] + 3*mt*t2*p2[1] + t3*p3[1]
    return (int(x), int(y))


def _generate_bezier_control_points(start: tuple[int, int], end: tuple[int, int], curvature: float = 0.3):
    dx, dy = end[0] - start[0], end[1] - start[1]
    distance = math.sqrt(dx*dx + dy*dy)
    if distance < 1:
        distance = 1
    dx_norm, dy_norm = dx / distance, dy / distance
    perp_x, perp_y = -dy_norm, dx_norm
    curve_magnitude = distance * curvature * random.uniform(0.5, 1.5)
    if random.random() < 0.5:
        curve_magnitude = -curve_magnitude
    cp1 = (start[0] + dx*0.25 + perp_x*curve_magnitude, start[1] + dy*0.25 + perp_y*curve_magnitude)
    cp2 = (start[0] + dx*0.75 - perp_x*curve_magnitude*0.5, start[1] + dy*0.75 - perp_y*curve_magnitude*0.5)
    return cp1, cp2


async def _bezier_mouse_move(page: Page, target_x: int, target_y: int,
                              duration_ms: float = 300.0, curvature: float = 0.3) -> None:
    current_pos = await page.evaluate("""() => ({
        x: window.__lastMouseX || window.innerWidth / 2,
        y: window.__lastMouseY || window.innerHeight / 2
    })""")
    start = (int(current_pos["x"]), int(current_pos["y"]))
    end = (target_x, target_y)
    cp1, cp2 = _generate_bezier_control_points(start, end, curvature)
    distance = math.sqrt((end[0]-start[0])**2 + (end[1]-start[1])**2)
    steps = max(10, min(30, int(distance / 20)))
    step_duration = duration_ms / steps / 1000.0
    for i in range(steps + 1):
        t = i / steps
        x, y = _bezier_point(t, start, cp1, cp2, end)
        await page.mouse.move(x, y)
        if i < steps:
            await asyncio.sleep(step_duration * random.uniform(0.8, 1.2))
    await page.evaluate(f"() => {{ window.__lastMouseX = {target_x}; window.__lastMouseY = {target_y}; }}")


async def _human_like_mouse_move(page: Page, target_x: int, target_y: int, account_id: Optional[str] = None) -> None:
    if account_id:
        behavior = _get_behavior_engine(account_id)
        current_pos = await page.evaluate("""() => ({
            x: window.__lastMouseX || window.innerWidth / 2,
            y: window.__lastMouseY || window.innerHeight / 2
        })""")
        start_x, start_y = int(current_pos["x"]), int(current_pos["y"])
        waypoints = behavior.human_mouse_move(start_x, start_y, target_x, target_y)
        for x, y, delay_ms in waypoints:
            await page.mouse.move(x, y)
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
        await page.evaluate(f"() => {{ window.__lastMouseX = {target_x}; window.__lastMouseY = {target_y}; }}")
    else:
        await _bezier_mouse_move(page, target_x, target_y)


# ─────────────────────────────────────────────────────────────────
# Ghost cursor + resolve helpers
# ─────────────────────────────────────────────────────────────────

async def _ghost_move_and_click(page: Page, element) -> None:
    ghost = getattr(page, "_ghost_cursor", None)
    if ghost:
        await ghost.move(element)
    else:
        await element.hover()
    await element.click()


async def _resolve_editable_element(page: Page, element):
    try:
        tag = await element.evaluate("el => (el.tagName || '').toLowerCase()")
        if tag in {"input", "textarea"}:
            return element
        editable = await element.query_selector('input, textarea, [contenteditable="true"], [role="textbox"]')
        if editable:
            return editable
        attrs = await element.evaluate("el => ({ id: el.id || '', name: el.getAttribute('name') || '' })")
        if attrs.get("id"):
            candidate = await page.query_selector(
                f'#{attrs["id"]} input, #{attrs["id"]} textarea, input#{attrs["id"]}, textarea#{attrs["id"]}'
            )
            if candidate:
                return candidate
        if attrs.get("name"):
            candidate = await page.query_selector(
                f'input[name="{attrs["name"]}"], textarea[name="{attrs["name"]}"]'
            )
            if candidate:
                return candidate
    except Exception:
        pass
    return element


# ─────────────────────────────────────────────────────────────────
# Reading simulation (call before every action)
# ─────────────────────────────────────────────────────────────────

async def simulate_reading(page: Page, account_id: Optional[str] = None) -> None:
    """Simulate human reading behaviour: slow scrolling, random mouse movement, idle pause.

    Call immediately after page.goto() settles, before performing any interaction.
    Reddit's anti-bot heuristics track interaction timing — pages that receive
    an action within milliseconds of load are flagged as automated.
    """
    behavior = _get_behavior_engine(account_id) if account_id else None

    if behavior:
        await behavior.delay("between_pages")
    else:
        await asyncio.sleep(random.uniform(2.0, 5.0))

    scrolls = behavior.human_scroll_count("reading_post") if behavior else random.randint(4, 9)

    for i in range(scrolls):
        distance = behavior.human_scroll_distance() if behavior else random.choice([
            random.randint(100, 250), random.randint(250, 500), random.randint(500, 800),
        ])
        await page.mouse.wheel(0, distance)
        if i < 2:
            if behavior:
                await behavior.delay("reading")
            else:
                await asyncio.sleep(random.uniform(3.0, 7.0))
        else:
            if behavior:
                await behavior.delay("pre_click")
            else:
                await asyncio.sleep(random.uniform(1.5, 4.0))

    if random.random() < 0.4:
        await page.mouse.wheel(0, random.randint(-400, -150))
        if behavior:
            await behavior.delay("pre_click")
        else:
            await asyncio.sleep(random.uniform(2.0, 5.0))

    viewport = await page.evaluate("() => ({w: window.innerWidth, h: window.innerHeight})")
    moves = random.randint(3, 7)
    for _ in range(moves):
        x = random.randint(100, max(200, viewport["w"] - 100))
        y = random.randint(100, max(200, viewport["h"] - 100))
        await _human_like_mouse_move(page, x, y, account_id)
        if behavior:
            await behavior.delay("pre_click")
        else:
            await asyncio.sleep(random.uniform(0.3, 1.5))

    if behavior:
        await behavior.delay("thinking_before_reply")
    else:
        await asyncio.sleep(random.uniform(2.0, 6.0))

    if behavior:
        await behavior.delay("reading")
    else:
        await asyncio.sleep(random.uniform(5, 12))


# ─────────────────────────────────────────────────────────────────
# Browse random posts (establishes realistic browsing trail)
# ─────────────────────────────────────────────────────────────────

async def browse_random_posts(page: Page, account_id: Optional[str] = None) -> None:
    """Click into 2-4 random posts and read them before acting.

    Call before post() or comment() when agent has just landed on a listing page.
    Establishes a realistic browsing trail before the primary action.
    """
    try:
        posts = await page.query_selector_all("a[href*='/comments/']")
        random.shuffle(posts)
        for post in posts[:random.randint(2, 4)]:
            try:
                await post.click()
                await simulate_reading(page, account_id)
                await page.go_back()
                await _delay(account_id, context="pre_click")
            except Exception:
                continue
    except Exception:
        pass


async def find_high_engagement_posts(page: Page, min_score: int = 50) -> list[dict]:
    """Find posts with high upvote scores for better comment visibility."""
    posts_data = await page.evaluate(f"""(minScore) => {{
        const results = [];
        const containers = document.querySelectorAll('[data-testid="post-container"], shreddit-post, .Post');
        containers.forEach(container => {{
            let scoreEl = container.querySelector('[data-testid="vote-buttons"]');
            if (!scoreEl) scoreEl = container.querySelector('[class*="upvote"], [class*="score"]');
            let score = 0;
            if (scoreEl) {{
                const scoreText = scoreEl.textContent || '';
                const match = scoreText.match(/([\\d.]+)([kKmM]?)/);
                if (match) {{
                    let num = parseFloat(match[1]);
                    const suffix = match[2].toLowerCase();
                    if (suffix === 'k') num *= 1000;
                    if (suffix === 'm') num *= 1000000;
                    score = Math.floor(num);
                }}
            }}
            const linkEl = container.querySelector('a[href*="/comments/"]');
            const title = linkEl ? (linkEl.textContent || '').trim() : '';
            const url = linkEl ? linkEl.href : '';
            if (score >= minScore && url) results.push({{ score, title: title.slice(0, 100), url }});
        }});
        return results.sort((a, b) => b.score - a.score);
    }}""", min_score)
    return posts_data if posts_data else []


# ─────────────────────────────────────────────────────────────────
# Reddit Bearer token capture (for API upvote calls)
# ─────────────────────────────────────────────────────────────────

async def ensure_token_captured(page: Page, log=None) -> Optional[str]:
    """Return a captured Reddit OAuth bearer token for this browser session.

    Strategy order:
    1. Return page-level cached token (no I/O)
    2. Re-check cache after any listener that may already be attached elsewhere

    Do not blindly use Reddit's token_v2/reddit_session cookies here. Some
    sessions expose token_v2 that works on oauth.reddit.com, but callers should
    validate it with /api/v1/me before making state-changing requests.
    """
    cached = getattr(page, "_reddit_bearer_token", None)
    if cached:
        return cached

    cached = getattr(page, "_reddit_bearer_token", None)
    return cached


async def scroll_to_comment(page: Page, comment_id: str, timeout_s: int = 60) -> bool:
    """Scroll until a specific shreddit-comment element is visible."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        found = await page.evaluate(f"""(id) => {{
            const el = document.querySelector(`shreddit-comment[thingid="t1_${{id}}"]`);
            if (!el) return false;
            el.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
            const rect = el.getBoundingClientRect();
            return rect.top >= 0 && rect.bottom <= window.innerHeight;
        }}""", comment_id)
        if found:
            return True
        await page.mouse.wheel(0, random.randint(300, 600))
        await asyncio.sleep(0.8)
    return False


async def safe_proxy_id(db, proxy_id: Optional[str]) -> Optional[str]:
    """Return proxy_id only if the proxy row exists in the DB."""
    if not proxy_id or not db:
        return None
    try:
        proxy_row = await db.get_proxy(proxy_id)
        return proxy_id if proxy_row is not None else None
    except Exception:
        return None
