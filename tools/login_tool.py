"""
tools/login_tool.py — Reddit login tool for AI agents.

Stealth features:
- Circuit breaker prevents account lockout from repeated failures
- Human-like typing with per-character delays and typo simulation
- Organic settle time and random scrolls before touching the form
- Ghost cursor mouse movement to form fields
- reCAPTCHA v3 Enterprise solving (2captcha / anticaptcha / capsolver)
- Multi-attempt login with token refresh between attempts
- Validates logged-in state via multiple selector strategies

Usage:
    result = await run_tool(page, account_id, username="u", password="p")
    # result: {success: bool, data: {logged_in, url}, error: str|None}
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Optional

import structlog
from playwright.async_api import Page

from tools.stealth.helpers import (
    _delay, _random_scroll, _human_type, _ghost_move_and_click,
    _resolve_editable_element, _ok, _fail, _ms,
)
from tools.stealth.captcha import solve_login_recaptcha, inject_grecaptcha_override

logger = structlog.get_logger(__name__)


async def _login_form_still_visible(page: Page) -> bool:
    try:
        result = await page.evaluate("""() => {
            const selectors = [
                '#login-username', '#login-password',
                'input[name="username"]', 'input[type="password"]',
                'auth-flow-modal', '.AnimatedForm__errorMessage',
            ];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el && el.offsetParent !== null) return true;
            }
            return false;
        }""")
        return bool(result)
    except Exception:
        return False


async def _resolve_login_submit_element(page: Page, password_el):
    try:
        submit = await page.evaluate("""() => {
            const selectors = ['button[type="submit"]', 'button[data-step="username-and-password"]'];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el && el.offsetParent !== null) return true;
            }
            return false;
        }""")
        if submit:
            return await page.query_selector('button[type="submit"]')
    except Exception:
        pass
    return None


async def _extract_login_error(page: Page) -> str:
    """Return the most useful visible login error Reddit is showing."""
    try:
        return await page.evaluate("""() => {
            const candidates = [];
            const selectors = [
                '[role="alert"]',
                'faceplate-alert',
                'auth-flow-modal [slot="error"]',
                '.AnimatedForm__errorMessage',
                '[class*="ErrorMessage"]',
                '[class*="error-message"]',
                '[class*="error" i]',
            ];
            for (const sel of selectors) {
                for (const n of document.querySelectorAll(sel)) {
                    const rect = n.getBoundingClientRect();
                    const txt = (n.textContent || '').replace(/\\s+/g, ' ').trim();
                    if (txt && rect.width > 0 && rect.height > 0) candidates.push(txt);
                }
            }
            const body = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
            const knownMessages = [
                'Server error. Try again later.',
                'Incorrect username or password',
                'Something went wrong',
                'Try again later',
                'We had a server error'
            ];
            for (const msg of knownMessages) {
                if (body.includes(msg)) candidates.push(msg);
            }
            return [...new Set(candidates)].join(' | ');
        }""")
    except Exception:
        return ""


def _format_network_errors(events: list[dict]) -> str:
    if not events:
        return ""
    unique: list[str] = []
    seen = set()
    for event in events[-8:]:
        body = event.get("body") or ""
        item = f"{event.get('status', 'ERR')} {event.get('url', '')}"
        if body:
            item = f"{item} body={body}"
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return "; ".join(unique)


async def _read_field_value(element) -> str:
    try:
        return await element.input_value()
    except Exception:
        try:
            return await element.evaluate("el => el.value || el.textContent || ''")
        except Exception:
            return ""


async def _force_exact_value(element, value: str) -> None:
    await element.fill("")
    await element.fill(value)
    try:
        await element.evaluate("""el => {
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }""")
    except Exception:
        pass


async def _ensure_exact_value(element, value: str) -> bool:
    current = await _read_field_value(element)
    if current == value:
        return True
    await _force_exact_value(element, value)
    return await _read_field_value(element) == value


async def _is_logged_in(page: Page) -> bool:
    try:
        logged_out = await page.evaluate("""() => {
            const visibleText = (el) => {
                const rect = el.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0 ? (el.textContent || '').trim() : '';
            };
            return [...document.querySelectorAll('button, a')]
                .map(visibleText)
                .some(t => /^log in$/i.test(t) || /^sign up$/i.test(t));
        }""")
        if logged_out:
            return False
    except Exception:
        pass

    valid_selectors = [
        'a[href^="/user/"][data-testid="user-link"]',
        'a[href^="/user/"]',
        'faceplate-tracker[source="profile"] a',
        '#expand-user-drawer-button[aria-label*="avatar"]',
        '#expand-user-drawer-button',
        'button[id*="USER_DROPDOWN"]',
        'header shreddit-header-action-item a[href^="/user/"]',
    ]
    for sel in valid_selectors:
        try:
            if await page.query_selector(sel):
                return True
        except Exception:
            continue
    try:
        return bool(await page.evaluate("""() => {
            const body = document.body?.innerText || '';
            if (/\\bLog In\\b/.test(body) || /\\bSign Up\\b/.test(body)) return false;
            return body.includes('Create Post') || body.includes('Log Out') || !!document.querySelector('a[href^="/user/"]');
        }"""))
    except Exception:
        return False


async def login(
    page: Page,
    account_id: str,
    username: str,
    password: str,
    db=None,
    proxy_id: Optional[str] = None,
    captcha_config: Optional[dict] = None,
    proxy_config: Optional[dict] = None,
) -> dict:
    """Execute an explicit Reddit login sequence with human-like behavior.

    Sequence:
        1. Navigate to login page
        2. Organic delay + field resolution
        3. Human-like typing with jitter
        4. Solve CAPTCHA (min_score=0.5)
        5. On rejection, retry once with min_score=0.3
    """
    log = logger.bind(account_id=account_id, action="LOGIN")
    start_ms = _ms()
    network_events: list[dict] = []

    async def _capture_response(response) -> None:
        try:
            url = response.url
            if "reddit.com" not in url:
                return
            interesting = any(part in url.lower() for part in ("login", "auth", "account", "api"))
            if response.status < 400 and not interesting:
                return
            event = {"status": response.status, "url": url[:180]}
            if response.status >= 400 and "account/login" in url.lower():
                try:
                    body = (await response.text()).replace("\n", " ").strip()
                    event["body"] = body[:300]
                except Exception:
                    pass
            network_events.append(event)
        except Exception:
            pass

    page.on("response", _capture_response)

    try:
        if db is not None:
            try:
                await db.ensure_account_exists(account_id)
            except Exception:
                pass

        login_url = "https://www.reddit.com/login/"
        log.info("reddit.login.navigating", url=login_url)
        await page.goto(login_url, wait_until="domcontentloaded", timeout=60_000)

        await _delay(account_id, 2.0, 4.0)
        await _random_scroll(page, account_id, read_min=0.5, read_max=2.0)
        await _delay(account_id, 0.8, 1.8)

        user_sels = [
            '#login-username input', "#login-username",
            'input[name="username"]', 'input[id="username"]',
            'input[autocomplete="username"]', 'input[placeholder*="sername"]',
        ]
        pass_sels = [
            '#login-password input', "#login-password",
            'input[name="password"]', 'input[type="password"]',
            'input[autocomplete="current-password"]',
        ]

        user_el = None
        for sel in user_sels:
            try:
                el = await page.wait_for_selector(sel, timeout=8_000, state="visible")
                if el:
                    user_el = el
                    break
            except Exception:
                continue
        if not user_el:
            raise RuntimeError("Username field not found")

        pass_el = None
        for sel in pass_sels:
            try:
                el = await page.wait_for_selector(sel, timeout=8_000, state="visible")
                if el:
                    pass_el = el
                    break
            except Exception:
                continue
        if not pass_el:
            raise RuntimeError("Password field not found")

        user_el = await _resolve_editable_element(page, user_el)
        pass_el = await _resolve_editable_element(page, pass_el)

        await _ghost_move_and_click(page, user_el)
        await _delay(account_id, 0.2, 0.6)
        await user_el.fill("")
        await _human_type(page, user_el, username, account_id)
        username_ok = await _ensure_exact_value(user_el, username)
        if not username_ok:
            raise RuntimeError("Username field did not retain the exact configured value")

        await _delay(account_id, 0.2, 0.7)
        await _ghost_move_and_click(page, pass_el)
        await _delay(account_id, 0.2, 0.6)
        await pass_el.fill("")
        await _human_type(page, pass_el, password, account_id)
        password_ok = await _ensure_exact_value(pass_el, password)
        if not password_ok:
            raise RuntimeError("Password field did not retain the exact configured value")

        attempt_scores = [0.5, 0.3] if captcha_config and captcha_config.get("api_key") else [0.0]
        is_logged_in = False
        failure_reason = "Login not confirmed after submit"

        for attempt_no, min_score in enumerate(attempt_scores, start=1):
            captcha_token = None
            if captcha_config and captcha_config.get("api_key"):
                try:
                    captcha_token = await solve_login_recaptcha(
                        page=page, account_id=account_id,
                        captcha_config=captcha_config, proxy_config=proxy_config,
                        log=log, min_score=min_score,
                    )
                except Exception as cap_exc:
                    log.warning("reddit.login.captcha_failed", error=str(cap_exc))

            if captcha_token:
                await inject_grecaptcha_override(page, captcha_token)

            await _delay(account_id, 0.5, 1.2)
            await _ghost_move_and_click(page, pass_el)
            await pass_el.press("Enter")
            await _delay(account_id, 1.2, 2.0)

            if await _login_form_still_visible(page):
                submit_el = await _resolve_login_submit_element(page, pass_el)
                if submit_el:
                    await _ghost_move_and_click(page, submit_el)
                    await _delay(account_id, 2.5, 4.5)

            is_logged_in = await _is_logged_in(page)

            error_text = await _extract_login_error(page)

            if is_logged_in:
                break

            failure_reason = error_text or "Login not confirmed after submit"
            if network_events:
                failure_reason = f"{failure_reason}. Network: {_format_network_errors(network_events)}"
            log.warning("reddit.login.rejected", min_score=min_score, reason=failure_reason, attempt=attempt_no)

            if "server error" in failure_reason.lower() or "try again later" in failure_reason.lower():
                break

        elapsed = _ms() - start_ms

        if not is_logged_in:
            try:
                await page.wait_for_timeout(3000)
                is_logged_in = await _is_logged_in(page)
                if not is_logged_in and "login" in page.url:
                    await page.goto("https://www.reddit.com", wait_until="domcontentloaded", timeout=30_000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8_000)
                    except Exception:
                        pass
                    is_logged_in = await _is_logged_in(page)
                if is_logged_in:
                    failure_reason = ""
            except Exception:
                pass

        if is_logged_in:
            if db is not None:
                try:
                    safe_pid = None
                    if proxy_id:
                        row = await db.get_proxy(proxy_id)
                        safe_pid = proxy_id if row else None
                    await db.log_action(
                        account_id=account_id, action_type="LOGIN", result="SUCCESS",
                        target_url=page.url, proxy_id=safe_pid, response_time_ms=elapsed,
                    )
                except Exception:
                    pass
            log.info("reddit.login.success", elapsed_ms=elapsed)
            return _ok({"logged_in": True, "url": page.url})

        if db is not None:
            try:
                safe_pid = None
                if proxy_id:
                    row = await db.get_proxy(proxy_id)
                    safe_pid = proxy_id if row else None
                await db.log_action(
                    account_id=account_id, action_type="LOGIN", result="FAILURE",
                    target_url=page.url, error_message=failure_reason,
                    proxy_id=safe_pid, response_time_ms=elapsed,
                )
            except Exception:
                pass
        log.warning("reddit.login.unconfirmed", reason=failure_reason)
        return _fail(failure_reason, {"url": page.url, "network": network_events[-8:]})

    except Exception as exc:
        elapsed = _ms() - start_ms
        error_msg = str(exc)
        log.error("reddit.login.failed", error=error_msg)
        if db is not None:
            try:
                await db.log_action(
                    account_id=account_id, action_type="LOGIN", result="FAILURE",
                    target_url=getattr(page, "url", ""), error_message=error_msg,
                    response_time_ms=elapsed,
                )
            except Exception:
                pass
        return _fail(error_msg)
    finally:
        try:
            page.remove_listener("response", _capture_response)
        except Exception:
            pass


async def run_tool(
    page: Page,
    account_id: str,
    username: str,
    password: str,
    db=None,
    proxy_id: Optional[str] = None,
    captcha_config: Optional[dict] = None,
    proxy_config: Optional[dict] = None,
) -> dict:
    """AI agent entry point for Reddit login.

    Args:
        page: Active Playwright page (must have stealth fingerprint injected beforehand).
        account_id: Unique account identifier (used for personality-based timing).
        username: Reddit username.
        password: Reddit password.
        db: Optional database instance for action logging.
        proxy_id: Optional proxy ID for logging correlation.
        captcha_config: Optional dict with keys: provider, api_key.
                        Providers: "2captcha", "anticaptcha", "capsolver".
        proxy_config: Optional dict with proxy details for capsolver.

    Returns:
        {success: bool, data: {logged_in: bool, url: str}, error: str|None}
    """
    return await login(
        page=page, account_id=account_id, username=username, password=password,
        db=db, proxy_id=proxy_id, captcha_config=captcha_config, proxy_config=proxy_config,
    )
