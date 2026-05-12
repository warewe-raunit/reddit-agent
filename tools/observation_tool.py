"""
tools/observation_tool.py — Unified page observation layer.

Returns a structured snapshot grounding agent decisions in real visible UI state.
Use before and after any action to confirm context and verify outcomes.
Never rely on screenshots alone — DOM and accessibility state are always included.
"""

from __future__ import annotations

import base64
from typing import Optional

from playwright.async_api import Page

_TEXT_CAP = 6000
_TEXT_PREVIEW_CAP = 300
_ELEMENTS_CAP = 40


async def observe_page(page: Page, include_screenshot: bool = True) -> dict:
    """Return a structured snapshot of the current visible page state.

    Returns:
        url: Current page URL.
        title: Page title.
        text: Visible text, capped to _TEXT_CAP chars.
        accessibility_snapshot: Playwright accessibility tree as dict.
        screenshot_b64: Base64 PNG of viewport, or None.
        interactive_elements: Visible interactive elements with role/name/bbox/state/selector.
        overlays: Detected overlays — modal, login_wall, captcha, error, rate_limit.
    """
    url = page.url
    title = await page.title()

    try:
        raw_text = await page.evaluate("() => document.body?.innerText || ''")
        text = raw_text[:_TEXT_CAP]
    except Exception:
        text = ""

    try:
        a11y = await page.accessibility.snapshot()
    except Exception:
        a11y = None

    screenshot_b64: Optional[str] = None
    if include_screenshot:
        try:
            png = await page.screenshot(full_page=False)
            screenshot_b64 = base64.b64encode(png).decode()
        except Exception:
            pass

    elements_cap = _ELEMENTS_CAP
    try:
        js_result = await page.evaluate(
            """(cap) => {
        const isVisible = (el) => {
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) return false;
            const style = window.getComputedStyle(el);
            return style.display !== 'none'
                && style.visibility !== 'hidden'
                && parseFloat(style.opacity || '1') > 0;
        };

        const cssEscape = (window.CSS && CSS.escape)
            ? CSS.escape
            : (value) => String(value).replace(/[^a-zA-Z0-9_-]/g, '\\\\$&');
        const cssString = (value) => '"' + String(value).replace(/\\\\/g, '\\\\\\\\').replace(/"/g, '\\\\"') + '"';
        const roots = [document];
        const collectShadowRoots = (root) => {
            const nodes = root.querySelectorAll ? root.querySelectorAll('*') : [];
            for (const node of nodes) {
                if (node.shadowRoot && !roots.includes(node.shadowRoot)) {
                    roots.push(node.shadowRoot);
                    collectShadowRoots(node.shadowRoot);
                }
            }
        };
        collectShadowRoots(document);

        const interactive = [];
        const sel = 'button, a[href], input, select, textarea, [role="button"], [role="link"], [role="checkbox"], [role="radio"], [tabindex]';
        const seen = new Set();
        const els = [];
        for (const root of roots) {
            let found;
            try { found = root.querySelectorAll(sel); } catch (_) { continue; }
            for (const el of found) {
                if (seen.has(el)) continue;
                seen.add(el);
                els.push({ el, source: root === document ? 'document' : 'shadow' });
                if (els.length >= cap * 4) break;
            }
            if (els.length >= cap * 4) break;
        }
        for (const item of els) {
            const el = item.el;
            if (!isVisible(el)) continue;
            const rect = el.getBoundingClientRect();
            const role = el.getAttribute('role') || el.tagName.toLowerCase();
            const name = (
                el.getAttribute('aria-label') ||
                el.getAttribute('title') ||
                (el.innerText || '').replace(/\\s+/g, ' ') ||
                el.getAttribute('placeholder') ||
                el.getAttribute('value') || ''
            ).trim().slice(0, 80);
            const href = el.getAttribute('href') || null;
            const pressedAttr = el.getAttribute('aria-pressed');
            const disabled = !!el.disabled || el.getAttribute('aria-disabled') === 'true';

            let selector = null;
            if (el.id) {
                selector = '#' + cssEscape(el.id);
            } else if (el.getAttribute('data-testid')) {
                selector = '[data-testid=' + cssString(el.getAttribute('data-testid')) + ']';
            } else if (el.getAttribute('aria-label')) {
                selector = '[aria-label=' + cssString(el.getAttribute('aria-label')) + ']';
            } else if (el.getAttribute('name')) {
                selector = el.tagName.toLowerCase() + '[name=' + cssString(el.getAttribute('name')) + ']';
            }

            interactive.push({
                role,
                tag: el.tagName.toLowerCase(),
                name,
                bbox: {
                    x: Math.round(rect.left),
                    y: Math.round(rect.top),
                    w: Math.round(rect.width),
                    h: Math.round(rect.height),
                },
                disabled,
                pressed: pressedAttr === null ? null : pressedAttr === 'true',
                href,
                selector,
                source: item.source,
            });
            if (interactive.length >= cap) break;
        }

        const bodyText = (document.body?.innerText || '').toLowerCase();
        const overlays = [];

        for (const root of roots) {
            let dialogs;
            try { dialogs = root.querySelectorAll('[role="dialog"], [aria-modal="true"], .modal'); } catch (_) { continue; }
            for (const d of dialogs) {
                if (isVisible(d)) {
                    overlays.push({ type: 'modal', text: (d.innerText || '').slice(0, 120) });
                    break;
                }
            }
            if (overlays.some(o => o.type === 'modal')) break;
        }

        let loginLink = null;
        for (const root of roots) {
            try {
                loginLink = root.querySelector('a[href*="/login"], button[data-click-id="login"]');
            } catch (_) {}
            if (loginLink) break;
        }
        if (loginLink && isVisible(loginLink)) {
            overlays.push({ type: 'login_wall', text: (loginLink.innerText || '').trim() });
        }

        if (/captcha|recaptcha|hcaptcha|are you a robot/i.test(bodyText)) {
            overlays.push({ type: 'captcha' });
        }

        for (const root of roots) {
            let errorEls;
            try { errorEls = root.querySelectorAll('[role="alert"], .error-message, [data-error]'); } catch (_) { continue; }
            for (const err of errorEls) {
                if (isVisible(err)) {
                    overlays.push({ type: 'error', text: (err.innerText || '').slice(0, 120) });
                }
            }
        }

        if (/you are doing that too much|rate limit|try again later/i.test(bodyText)) {
            overlays.push({ type: 'rate_limit' });
        }

        return { interactive, overlays };
    }""",
            elements_cap,
        )
    except Exception:
        js_result = {"interactive": [], "overlays": []}

    return {
        "url": url,
        "title": title,
        "text": text,
        "accessibility_snapshot": a11y,
        "screenshot_b64": screenshot_b64,
        "interactive_elements": js_result.get("interactive", []),
        "overlays": js_result.get("overlays", []),
    }


def summarize_observation(obs: dict, include_elements: bool = True) -> str:
    """Return a compact human-readable summary of an observation dict."""
    lines = [
        f"URL: {obs.get('url', '?')}",
        f"Title: {obs.get('title', '?')}",
    ]
    overlays = obs.get("overlays", [])
    if overlays:
        lines.append("Overlays: " + ", ".join(o.get("type", "?") for o in overlays))
    if include_elements:
        els = obs.get("interactive_elements", [])
        if els:
            lines.append(f"Interactive elements ({len(els)}):")
            for el in els[:12]:
                state = ""
                if el.get("disabled"):
                    state = " [disabled]"
                elif el.get("pressed") is True:
                    state = " [pressed]"
                source = " [shadow]" if el.get("source") == "shadow" else ""
                lines.append(f"  {el['role']} '{el['name']}'{state}{source}")
    text = obs.get("text") or ""
    text_preview = text[:_TEXT_PREVIEW_CAP].replace("\n", " ")
    if text_preview:
        truncated = len(text) > _TEXT_PREVIEW_CAP
        suffix = "..." if truncated else ""
        label = "Text preview (truncated)" if truncated else "Text preview"
        lines.append(f"{label}: {text_preview.rstrip()}{suffix}")
    return "\n".join(lines)
