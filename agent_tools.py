"""
agent_tools.py — LangGraph-compatible tool wrappers around tools/.
Tools use LazyBrowser — browser only launches on first tool call.
"""

from __future__ import annotations

import base64
import os
from typing import Optional
from urllib.parse import quote_plus, urlparse
from langchain_core.tools import tool

from tools import login, browse, comment, upvote, comment_upvote, join_subreddit, post, reply
from tools.browse_tool import warmup_browsing_session
from tools.stealth.captcha import solve_login_recaptcha
from session_store import delete_session, session_exists
from browser_manager import LazyBrowser


def _proxy_config(proxy_url: Optional[str]) -> Optional[dict]:
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    config: dict = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        config["username"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password
    return config


def _captcha_config_for_login() -> Optional[dict]:
    enabled = os.getenv("CAPTCHA_SOLVE_ON_LOGIN", "").strip().lower() in {"1", "true", "yes", "on"}
    api_key = os.getenv("CAPTCHA_API_KEY")
    if not enabled or not api_key:
        return None
    return {
        "api_key": api_key,
        "provider": os.getenv("CAPTCHA_PROVIDER", "2captcha"),
    }


def is_reddit_action_request(text: str) -> bool:
    lowered = text.lower()
    return any(
        phrase in lowered
        for phrase in (
            "reddit",
            "login",
            "log in",
            "comment",
            "upvote",
            "comment upvote",
            "post",
            "join subreddit",
            "open reddit",
            "warmup",
            "warm up",
            "karma",
        )
    )


async def is_reddit_logged_in(page, navigate: bool = True) -> bool:
    """Detect whether the current Reddit browser context is logged in."""
    try:
        if navigate:
            await page.goto("https://www.reddit.com", wait_until="domcontentloaded", timeout=30_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass

        ui_state = await page.evaluate("""() => {
            const visibleText = (el) => {
                const rect = el.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0 ? (el.textContent || '').trim() : '';
            };
            const buttonsAndLinks = [...document.querySelectorAll('button, a')].map(visibleText).filter(Boolean);
            const hasVisibleLogin = buttonsAndLinks.some(t => /^log in$/i.test(t));
            const hasVisibleSignup = buttonsAndLinks.some(t => /^sign up$/i.test(t));
            if (hasVisibleLogin || hasVisibleSignup) {
                return { loggedIn: false, loggedOut: true, reason: 'visible_login_button' };
            }

            const loggedInSelectors = [
                'a[href^="/user/"][data-testid="user-link"]',
                'a[href^="/user/"]',
                'faceplate-tracker[source="profile"] a',
                '#expand-user-drawer-button',
                'button[id*="USER_DROPDOWN"]',
                'header shreddit-header-action-item a[href^="/user/"]',
                'a[href="/submit"]',
                'a[href="/settings/account"]',
                'button[aria-label*="profile" i]',
                'button[aria-label*="avatar" i]',
            ];
            if (loggedInSelectors.some(sel => !!document.querySelector(sel))) {
                return { loggedIn: true, loggedOut: false, reason: 'logged_in_selector' };
            }

            const text = document.body?.innerText || '';
            const hasLoggedInUi = /\\bCreate\\b/.test(text) && /\\bHome\\b/.test(text);
            const hasLoggedOutUi = /\\bLog In\\b/.test(text) || /\\bSign Up\\b/.test(text);
            return { loggedIn: hasLoggedInUi && !hasLoggedOutUi, loggedOut: hasLoggedOutUi, reason: 'text_heuristic' };
        }""")

        if ui_state.get("loggedOut"):
            return False
        if ui_state.get("loggedIn"):
            return True

        cookies = await page.context.cookies(["https://www.reddit.com"])
        cookie_names = {cookie.get("name") for cookie in cookies}
        return "reddit_session" in cookie_names and "loid" not in cookie_names
    except Exception:
        return False


async def ensure_reddit_logged_in(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    proxy_url: Optional[str] = None,
) -> tuple[bool, str]:
    """Ensure Reddit is open and logged in, returning a user-facing status."""
    page = await lazy.get_page()
    if await is_reddit_logged_in(page):
        await lazy.persist_session()
        return True, "Already logged in to Reddit."

    result = await login(
        page=page,
        account_id=account_id,
        username=username,
        password=password,
        captcha_config=_captcha_config_for_login(),
        proxy_config=_proxy_config(proxy_url),
    )

    if result["success"] or await is_reddit_logged_in(page):
        await lazy.persist_session()
        return True, "Logged in to Reddit and saved the session."

    return False, f"Login failed: {result['error']}"


async def open_reddit_home(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    proxy_url: Optional[str] = None,
) -> str:
    """Open Reddit home, ensure login, and save the session when present."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status
    page = await lazy.get_page()
    await page.goto("https://www.reddit.com", wait_until="domcontentloaded", timeout=60_000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8_000)
    except Exception:
        pass
    await lazy.persist_session()
    return "Reddit is open and the account is logged in."


async def comment_on_reddit_post(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    post_url: str,
    text: str,
    proxy_url: Optional[str] = None,
) -> str:
    """Systematic comment workflow: open/login, warm up, navigate, comment, save."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status

    page = await lazy.get_page()
    result = await comment(page=page, account_id=account_id, post_url=post_url, text=text)
    if result["success"]:
        await lazy.persist_session()
        verified = result["data"].get("verified", False)
        return "Comment posted and verified." if verified else "Comment submitted, but Reddit did not expose a clear verification signal."
    return f"Comment failed: {result['error']}"


async def upvote_reddit_comment(
    lazy: LazyBrowser,
    account_id: str,
    username: str,
    password: str,
    comment_url: str = "",
    comment_fullname: str = "",
    post_url: str = "",
    proxy_url: Optional[str] = None,
) -> str:
    """Systematic comment-upvote workflow: open/login, warm up, upvote comment, save."""
    ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
    if not ok:
        return status

    page = await lazy.get_page()
    result = await comment_upvote(
        page=page,
        account_id=account_id,
        comment_url=comment_url,
        comment_fullname=comment_fullname,
        post_url=post_url,
    )
    if result["success"]:
        await lazy.persist_session()
        return f"Comment upvote successful. Data: {result['data']}"
    return f"Comment upvote failed: {result['error']}"


def make_tools(lazy: LazyBrowser, account_id: str, username: str, password: str, proxy_url: Optional[str] = None):
    """Return list of LangGraph tools. Browser launches on first tool call."""

    async def _is_logged_in(page) -> bool:
        return await is_reddit_logged_in(page)

    async def _ensure_logged_in(page) -> Optional[str]:
        ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
        if ok:
            return None
        delete_session(account_id)
        return status

    @tool
    async def check_session() -> str:
        """Check whether a saved login session exists and is still active on Reddit."""
        page = await lazy.get_page()
        try:
            logged_in = await _is_logged_in(page)
            if logged_in:
                await lazy.persist_session()
                return "Session exists: True. Already logged in on Reddit. No login needed."
            if not session_exists(account_id):
                return "Session exists: False. Must call login_reddit first."
            delete_session(account_id)
            return "Session exists: False. Session expired. Must call login_reddit."
        except Exception as e:
            return f"Session check failed: {e}. Call login_reddit to be safe."

    @tool
    async def login_reddit() -> str:
        """Login to Reddit using the account credentials. Call this if no session exists."""
        ok, status = await ensure_reddit_logged_in(lazy, account_id, username, password, proxy_url)
        return status if ok else status

    @tool
    async def browse_reddit(mode: str = "simulate_reading", subreddit: str = "") -> str:
        """
        Simulate human browsing on Reddit to warm up the account before acting.
        mode: 'simulate_reading' (default) | 'browse_random' | 'browse_subreddit' | 'find_posts'
        subreddit: Optional subreddit name without r/ when mode='browse_subreddit'.
        Always call this after login and before commenting or upvoting.
        """
        page = await lazy.get_page()
        result = await browse(
            page=page,
            account_id=account_id,
            mode=mode,
            subreddit=subreddit or None,
        )
        if result["success"]:
            return f"Browsing complete. Data: {result['data']}"
        return f"Browse failed: {result['error']}"

    @tool
    async def warmup_reddit(subreddits: str = "", duration_minutes: float = 5.0) -> str:
        """
        Run a browsing-only warm-up session across relevant subreddits.
        Args:
            subreddits: Comma-separated subreddit names without r/. If empty, uses safe general communities.
            duration_minutes: Target warm-up time in minutes.
        This tool only browses/reads. It does not post, comment, vote, or farm karma.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error

        parsed = [s.strip().strip("/").removeprefix("r/") for s in subreddits.split(",") if s.strip()]
        if not parsed:
            parsed = [
                "NoStupidQuestions",
                "AskReddit",
                "CasualConversation",
                "explainlikeimfive",
                "todayilearned",
            ]

        safe_duration = max(1.0, min(float(duration_minutes or 5.0), 30.0))
        result = await warmup_browsing_session(
            page=page,
            account_id=account_id,
            subreddits=parsed[:10],
            duration_minutes=safe_duration,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Warm-up browsing complete. Data: {result['data']}"
        return f"Warm-up browsing failed: {result['error']}"

    @tool
    async def find_warmup_comment_opportunities(subreddits: str = "", max_posts: int = 8) -> str:
        """
        Find active posts where a human-approved helpful comment might make sense.
        Args:
            subreddits: Comma-separated subreddit names without r/. If empty, uses general communities.
            max_posts: Maximum candidate posts to return.
        This tool does not submit comments. Use it to draft comments for user approval.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error

        parsed = [s.strip().strip("/").removeprefix("r/") for s in subreddits.split(",") if s.strip()]
        if not parsed:
            parsed = ["NoStupidQuestions", "AskReddit", "CasualConversation", "explainlikeimfive"]

        limit = max(1, min(int(max_posts or 8), 20))
        per_sub_limit = max(1, (limit + len(parsed) - 1) // len(parsed))
        candidates: list[dict] = []

        for subreddit_name in parsed[:8]:
            if len(candidates) >= limit:
                break
            url = f"https://www.reddit.com/r/{subreddit_name}/new/"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8_000)
                except Exception:
                    pass
                await page.wait_for_timeout(1500)
                found = await page.evaluate("""(subredditName) => {
                    const seen = new Set();
                    const results = [];
                    const posts = [...document.querySelectorAll('shreddit-post, article, [data-testid="post-container"]')];
                    for (const post of posts) {
                        const anchor = post.querySelector('a[href*="/comments/"]');
                        if (!anchor || !anchor.href || seen.has(anchor.href)) continue;
                        seen.add(anchor.href);
                        const title =
                            post.getAttribute('post-title') ||
                            anchor.innerText ||
                            anchor.getAttribute('aria-label') ||
                            '';
                        const text = (post.innerText || '').replace(/\\s+/g, ' ').trim();
                        if (!title.trim() && !text) continue;
                        results.push({
                            subreddit: subredditName,
                            title: title.trim().slice(0, 180) || text.slice(0, 180),
                            url: anchor.href,
                            context: text.slice(0, 500),
                        });
                        if (results.length >= 6) break;
                    }
                    return results;
                }""", subreddit_name)
                candidates.extend(found[:per_sub_limit])
            except Exception as exc:
                candidates.append({
                    "subreddit": subreddit_name,
                    "error": str(exc),
                })

        candidates = candidates[:limit]
        if not candidates:
            return "No warm-up comment opportunities found."

        lines = ["Warm-up comment opportunities. Draft comments only; do not post until the user approves:"]
        for idx, item in enumerate(candidates, start=1):
            if item.get("error"):
                lines.append(f"{idx}. r/{item['subreddit']} failed: {item['error']}")
                continue
            lines.append(
                f"{idx}. r/{item['subreddit']} - {item['title']}\n"
                f"URL: {item['url']}\n"
                f"Context: {item.get('context', '')[:300]}"
            )
        await lazy.persist_session()
        return "\n".join(lines)

    @tool
    async def search_reddit_posts(query: str, subreddit: str = "", sort: str = "relevance") -> str:
        """
        Search Reddit for posts and return candidate post URLs.
        Args:
            query: Search text, usually the title or keywords the user gave.
            subreddit: Optional subreddit name without r/ to search inside one subreddit.
            sort: relevance | hot | top | new | comments.
        Use this when the user describes a post but does not provide a full URL.
        """
        page = await lazy.get_page()
        try:
            safe_sort = sort if sort in {"relevance", "hot", "top", "new", "comments"} else "relevance"
            base = f"https://www.reddit.com/r/{subreddit}/search/" if subreddit else "https://www.reddit.com/search/"
            url = f"{base}?q={quote_plus(query)}&type=link&sort={safe_sort}"
            if subreddit:
                url += "&restrict_sr=1"

            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass
            await page.wait_for_timeout(1500)

            posts = await page.evaluate("""() => {
                const seen = new Set();
                const results = [];
                const anchors = [...document.querySelectorAll('a[href*="/comments/"]')];
                for (const a of anchors) {
                    const href = a.href;
                    if (!href || seen.has(href)) continue;
                    seen.add(href);
                    const title = (a.innerText || a.getAttribute('aria-label') || '').trim();
                    const container = a.closest('search-telemetry-tracker, shreddit-post, article, [data-testid="post-container"]');
                    const text = container ? (container.innerText || '').trim() : title;
                    results.push({ title: title || text.slice(0, 120), url: href, context: text.slice(0, 300) });
                    if (results.length >= 8) break;
                }
                return results;
            }""")

            if not posts:
                return "No Reddit posts found for that search."

            lines = ["Search results:"]
            for i, item in enumerate(posts, start=1):
                title = (item.get("title") or "Untitled").replace("\n", " ")[:140]
                lines.append(f"{i}. {title}\nURL: {item.get('url')}")
            return "\n".join(lines)
        except Exception as e:
            return f"Search failed: {e}"

    @tool
    async def navigate_to_post(post_url: str) -> str:
        """Navigate the browser to a specific Reddit post URL."""
        page = await lazy.get_page()
        try:
            await page.goto(post_url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(2000)
            title = await page.title()
            return f"Navigated to post. Page title: {title}"
        except Exception as e:
            return f"Navigation failed: {e}"

    @tool
    async def comment_on_post(post_url: str, text: str) -> str:
        """
        Post a comment on a Reddit thread.
        Args:
            post_url: Full Reddit post URL.
            text: The comment text to post.
        """
        return await comment_on_reddit_post(
            lazy=lazy,
            account_id=account_id,
            username=username,
            password=password,
            post_url=post_url,
            text=text,
            proxy_url=proxy_url,
        )

    @tool
    async def submit_text_post(subreddit: str, title: str, body: str) -> str:
        """
        Create a text post in a subreddit.
        Args:
            subreddit: Subreddit name without r/ prefix.
            title: Post title.
            body: Post body text.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await post(
            page=page,
            account_id=account_id,
            subreddit=subreddit,
            title=title,
            body=body,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Post submitted. URL: {result['data'].get('post_url', '')}"
        return f"Post failed: {result['error']}"

    @tool
    async def reply_to_reddit_comment(comment_fullname: str, post_url: str, text: str) -> str:
        """
        Reply to a specific Reddit comment.
        Args:
            comment_fullname: Parent comment fullname, e.g. t1_abc123.
            post_url: Full Reddit post URL containing the comment.
            text: Reply text.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await reply(
            page=page,
            account_id=account_id,
            comment_fullname=comment_fullname,
            post_url=post_url,
            text=text,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Reply posted. Data: {result['data']}"
        return f"Reply failed: {result['error']}"

    @tool
    async def upvote_post(post_url: str) -> str:
        """
        Upvote a Reddit post.
        Args:
            post_url: Full Reddit post URL.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await upvote(
            page=page,
            account_id=account_id,
            post_url=post_url,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Upvote successful. Data: {result['data']}"
        return f"Upvote failed: {result['error']}"

    @tool
    async def upvote_comment(comment_url: str = "", comment_fullname: str = "", post_url: str = "") -> str:
        """
        Upvote a Reddit comment.
        Args:
            comment_url: Full Reddit comment permalink, preferred when available.
            comment_fullname: Optional Reddit comment fullname, e.g. t1_abc123.
            post_url: Optional containing post URL if comment_fullname is supplied.
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await comment_upvote(
            page=page,
            account_id=account_id,
            comment_url=comment_url,
            comment_fullname=comment_fullname,
            post_url=post_url,
        )
        if result["success"]:
            await lazy.persist_session()
            return f"Comment upvote successful. Data: {result['data']}"
        return f"Comment upvote failed: {result['error']}"

    @tool
    async def join_subreddit_tool(subreddit: str) -> str:
        """
        Join a subreddit. Required before commenting on subreddits with Crowd Control enabled.
        Args:
            subreddit: Subreddit name without r/ prefix (e.g. 'python').
        """
        page = await lazy.get_page()
        login_error = await _ensure_logged_in(page)
        if login_error:
            return login_error
        result = await join_subreddit(
            page=page,
            account_id=account_id,
            subreddit=subreddit,
        )
        if result["success"]:
            await lazy.persist_session()
            already = result["data"].get("already_joined", False)
            return f"{'Already a member.' if already else 'Joined successfully.'}"
        return f"Join failed: {result['error']}"

    @tool
    async def get_accessibility_snapshot() -> str:
        """
        Return the accessibility tree of the current page as text.
        Use this to read page content, find buttons, inputs, and links without screenshots.
        Prefer this over take_screenshot for understanding page structure.
        """
        page = await lazy.get_page()
        try:
            snapshot = await page.accessibility.snapshot()
            if snapshot is None:
                return "Accessibility snapshot unavailable."
            return str(snapshot)
        except Exception as e:
            return f"Snapshot failed: {e}"

    @tool
    async def get_page_text() -> str:
        """
        Return all visible text content from the current page.
        Use this to read post content, comments, or page state as plain text.
        """
        page = await lazy.get_page()
        try:
            text = await page.evaluate("() => document.body.innerText")
            return text[:8000] if len(text) > 8000 else text
        except Exception as e:
            return f"get_page_text failed: {e}"

    @tool
    async def take_screenshot() -> str:
        """
        Take a screenshot of the current page and return it as a base64 PNG string.
        Use only when accessibility snapshot is insufficient (e.g. visual CAPTCHA).
        """
        page = await lazy.get_page()
        try:
            png_bytes = await page.screenshot(full_page=False)
            return base64.b64encode(png_bytes).decode()
        except Exception as e:
            return f"Screenshot failed: {e}"

    @tool
    async def solve_captcha() -> str:
        """
        Solve a reCAPTCHA on the current page using the configured CAPTCHA provider.
        Requires CAPTCHA_API_KEY and CAPTCHA_PROVIDER env vars.
        Call this when login or comment action is blocked by a CAPTCHA.
        """
        page = await lazy.get_page()
        captcha_config = {
            "api_key": os.getenv("CAPTCHA_API_KEY"),
            "provider": os.getenv("CAPTCHA_PROVIDER", "2captcha"),
        }
        proxy_config = {"server": proxy_url} if proxy_url else None
        try:
            token = await solve_login_recaptcha(
                page=page,
                account_id=account_id,
                captcha_config=captcha_config,
                proxy_config=proxy_config,
            )
            if token:
                return f"CAPTCHA solved. Token length: {len(token)}"
            return "CAPTCHA solve returned no token. Check CAPTCHA_API_KEY."
        except Exception as e:
            return f"CAPTCHA solve failed: {e}"

    return [
        check_session,
        login_reddit,
        browse_reddit,
        warmup_reddit,
        find_warmup_comment_opportunities,
        search_reddit_posts,
        navigate_to_post,
        comment_on_post,
        submit_text_post,
        reply_to_reddit_comment,
        upvote_post,
        upvote_comment,
        join_subreddit_tool,
        get_accessibility_snapshot,
        get_page_text,
        take_screenshot,
        solve_captcha,
    ]
