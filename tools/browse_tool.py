"""
tools/browse_tool.py — Reddit browsing and reading simulation tool for AI agents.

Stealth features:
- simulate_reading(): slow scroll + random mouse movement + idle pause
  (call before any action — Reddit's anti-bot heuristics track this)
- browse_random_posts(): clicks into 2-4 posts and reads them
  (establishes realistic browsing trail before primary action)
- find_high_engagement_posts(): find high-score posts to target
- Per-account personality-driven scroll count, distance, and timing
- Bezier-curve mouse movement
- Reading time based on text length and engagement level

Usage:
    # Simulate reading the current page
    await run_tool(page, account_id, mode="simulate_reading")

    # Browse random posts from a subreddit listing
    await run_tool(page, account_id, mode="browse_random")

    # Find high engagement posts on current listing
    result = await run_tool(page, account_id, mode="find_posts", min_score=100)
"""

from __future__ import annotations

import asyncio
import random
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    simulate_reading, browse_random_posts, find_high_engagement_posts,
    _delay, _random_scroll, _human_like_mouse_move, _ok, _fail, _ms,
    _get_behavior_engine,
)

logger = structlog.get_logger(__name__)


async def run_tool(
    page: Page,
    account_id: str,
    mode: str = "simulate_reading",
    subreddit: Optional[str] = None,
    min_score: int = 50,
    num_posts: int = 3,
) -> dict:
    """AI agent entry point for Reddit browsing and reading simulation.

    Args:
        page: Active Playwright page.
        account_id: Unique account identifier (drives personality-based behavior).
        mode: One of:
            - "simulate_reading" — simulate reading the current page (scroll + mouse + idle)
            - "browse_random" — click into 2-4 posts and read them on current listing
            - "browse_subreddit" — navigate to a subreddit and browse (requires subreddit arg)
            - "find_posts" — find high-engagement posts on current listing
        subreddit: Required when mode="browse_subreddit".
        min_score: Minimum upvote score for find_posts mode.
        num_posts: Number of posts to browse in browse_random/browse_subreddit mode.

    Returns:
        {success: bool, data: {mode, ...}, error: str|None}
    """
    log = logger.bind(account_id=account_id, action=f"BROWSE:{mode}")
    start_ms = _ms()

    try:
        if mode == "simulate_reading":
            await simulate_reading(page, account_id)
            elapsed = _ms() - start_ms
            log.info("browse.simulate_reading.done", elapsed_ms=elapsed)
            return _ok({"mode": mode, "elapsed_ms": elapsed})

        elif mode == "browse_random":
            await browse_random_posts(page, account_id)
            elapsed = _ms() - start_ms
            log.info("browse.browse_random.done", elapsed_ms=elapsed)
            return _ok({"mode": mode, "elapsed_ms": elapsed})

        elif mode == "browse_subreddit":
            if not subreddit:
                return _fail("subreddit argument required for browse_subreddit mode")
            url = f"https://www.reddit.com/r/{subreddit}/"
            log.info("browse.browse_subreddit.navigating", url=url)
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await simulate_reading(page, account_id)
            await browse_random_posts(page, account_id)
            elapsed = _ms() - start_ms
            log.info("browse.browse_subreddit.done", elapsed_ms=elapsed)
            return _ok({"mode": mode, "subreddit": subreddit, "elapsed_ms": elapsed})

        elif mode == "find_posts":
            posts = await find_high_engagement_posts(page, min_score=min_score)
            elapsed = _ms() - start_ms
            log.info("browse.find_posts.done", count=len(posts), elapsed_ms=elapsed)
            return _ok({"mode": mode, "posts": posts[:20], "total_found": len(posts)})

        else:
            return _fail(f"Unknown mode: {mode}. Use: simulate_reading, browse_random, browse_subreddit, find_posts")

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("browse.failed", error=error_msg, mode=mode)
        return _fail(error_msg)


async def warmup_browsing_session(
    page: Page,
    account_id: str,
    subreddits: list[str],
    duration_minutes: float = 5.0,
) -> dict:
    """Simulate a realistic browsing session across multiple subreddits.

    Useful for account warmup — builds realistic activity history before
    performing target actions. Respects HumanBehaviorEngine break detection.

    Args:
        page: Active Playwright page.
        account_id: Account identifier.
        subreddits: List of subreddit names to browse.
        duration_minutes: Target session duration in minutes.
    """
    log = logger.bind(account_id=account_id, action="WARMUP_BROWSE")
    start_ms = _ms()
    deadline_ms = start_ms + int(duration_minutes * 60 * 1000)
    behavior = _get_behavior_engine(account_id)
    visited = []

    while _ms() < deadline_ms:
        sub = random.choice(subreddits)
        url = f"https://www.reddit.com/r/{sub}/"
        log.info("warmup.navigating", subreddit=sub)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await simulate_reading(page, account_id)
            visited.append(sub)

            # Optionally click into a post
            if random.random() < 0.6:
                await browse_random_posts(page, account_id)

            # Check if should take a break
            should_break, break_duration = behavior.should_take_break()
            if should_break:
                remaining_ms = deadline_ms - _ms()
                sleep_ms = min(break_duration * 1000, remaining_ms * 0.5)
                if sleep_ms > 0:
                    log.info("warmup.taking_break", duration_s=sleep_ms/1000)
                    await asyncio.sleep(sleep_ms / 1000)

            await _delay(account_id, context="between_pages")

        except Exception as exc:
            log.warning("warmup.subreddit_failed", subreddit=sub, error=str(exc))
            await asyncio.sleep(5)

    elapsed = _ms() - start_ms
    return _ok({"visited_subreddits": visited, "elapsed_ms": elapsed})
