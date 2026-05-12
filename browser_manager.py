"""
browser_manager.py — Launch Playwright browser with full stealth stack wired in.

Stealth layers applied (in order, all before any page.goto()):
  Layer 1: fingerprint.py        — 22 techniques (WebGL, canvas, audio, navigator)
  Layer 2: advanced_fingerprint  — 40 techniques (permissions, timing, matchMedia)
  Layer 3: bot_detection_evasion — 50+ techniques (PerimeterX, Kasada, Cloudflare, Reddit Sentinel)

Post-load:
  BotDetectionEvasionManager.post_load_check() fires on every page 'load' event
  to re-assert patches that anti-bot scripts try to restore on DOMContentLoaded.

Browser context settings (UA, viewport, locale, timezone) come from
BrowserProfileManager.generate(account_id) — deterministic per account.
"""

from __future__ import annotations

import asyncio
import re
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

from proxy_config import playwright_proxy_config
from session_store import load_session, save_session
from tools.stealth.fingerprint import BrowserProfileManager
from tools.stealth.bot_detection_evasion import BotDetectionEvasionManager


def profile_session_id(account_id: str, profile: dict) -> str:
    """Return the storage-state key for this account/profile pairing."""
    category = str(profile.get("device_category") or "desktop").strip().lower()
    is_mobile = bool(profile.get("is_mobile", False))
    if category == "desktop" and not is_mobile:
        return account_id
    safe_category = re.sub(r"[^A-Za-z0-9_.-]+", "_", category).strip("_") or "profile"
    return f"{account_id}__{safe_category}"


def active_profile_session_id(account_id: str) -> str:
    """Return the storage-state key for the currently configured profile."""
    profile = BrowserProfileManager().generate(account_id)
    return profile_session_id(account_id, profile)


async def launch_browser(
    account_id: str,
    proxy_url: Optional[str] = None,
    headless: bool = False,
) -> tuple[Playwright, Browser, BrowserContext, Page]:
    pw = await async_playwright().start()

    profile_mgr = BrowserProfileManager()
    profile = profile_mgr.generate(account_id)
    screen = profile["screen_resolution"]
    session_id = profile_session_id(account_id, profile)

    launch_args: dict = {
        "headless": headless,
        "args": [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            f"--window-size={screen['width']},{screen['height']}",
        ],
    }
    proxy_config = playwright_proxy_config(proxy_url)
    if proxy_config:
        launch_args["proxy"] = proxy_config

    browser = await pw.chromium.launch(**launch_args)
    saved_session = load_session(session_id)

    context_args: dict = {
        "viewport": {"width": screen["width"], "height": screen["height"]},
        "screen": {"width": screen["width"], "height": screen["height"]},
        "user_agent": profile["user_agent"],
        "locale": profile["locale"],
        "timezone_id": profile["timezone"],
        "device_scale_factor": profile["device_scale_factor"],
        "is_mobile": bool(profile.get("is_mobile", False)),
        "has_touch": bool(profile.get("has_touch", False)),
        "extra_http_headers": {
            "sec-ch-ua": profile["sec_ch_ua"],
            "sec-ch-ua-mobile": profile["sec_ch_ua_mobile"],
            "sec-ch-ua-platform": profile["sec_ch_ua_platform"],
            "Accept-Language": f"{profile['locale']},en;q=0.9",
        },
    }
    if saved_session:
        context_args["storage_state"] = saved_session

    context = await browser.new_context(**context_args)
    page = await context.new_page()

    evasion_mgr = BotDetectionEvasionManager()
    await evasion_mgr.inject_all(page, profile)

    loop = asyncio.get_running_loop()

    async def _post_load_check(p: Page) -> None:
        try:
            await evasion_mgr.post_load_check(p)
        except Exception:
            pass

    page.on("load", lambda p: loop.create_task(_post_load_check(p)))

    return pw, browser, context, page


async def persist_session(account_id: str, context: BrowserContext) -> None:
    profile = BrowserProfileManager().generate(account_id)
    session_id = profile_session_id(account_id, profile)
    state = await context.storage_state()
    save_session(session_id, state)


async def close_browser(pw: Playwright, browser: Browser) -> None:
    await browser.close()
    await pw.stop()


class LazyBrowser:
    """Browser that launches only on first tool call, stays open for the session."""

    def __init__(self, account_id: str, proxy_url: Optional[str] = None, headless: bool = False):
        self.account_id = account_id
        self.proxy_url = proxy_url
        self.headless = headless
        self._pw: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    @property
    def launched(self) -> bool:
        return self._page is not None and not self._page.is_closed()

    async def _reset_closed(self) -> None:
        browser = self._browser
        pw = self._pw
        self._pw = self._browser = self._context = self._page = None
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
        if pw is not None:
            try:
                await pw.stop()
            except Exception:
                pass

    async def get_page(self) -> Page:
        if self._page is not None:
            if self._page.is_closed() or (self._browser is not None and not self._browser.is_connected()):
                await self._reset_closed()

        if self._page is None:
            self._pw, self._browser, self._context, self._page = await launch_browser(
                self.account_id, self.proxy_url, self.headless
            )
        return self._page

    async def get_context(self) -> BrowserContext:
        await self.get_page()
        return self._context  # type: ignore[return-value]

    async def persist_session(self) -> None:
        """Persist the current browser context, if it has been launched."""
        if self._context is not None:
            await persist_session(self.account_id, self._context)

    async def close(self) -> None:
        if self._browser is not None:
            try:
                await self.persist_session()
            except Exception:
                pass
            await close_browser(self._pw, self._browser)  # type: ignore[arg-type]
            self._pw = self._browser = self._context = self._page = None
