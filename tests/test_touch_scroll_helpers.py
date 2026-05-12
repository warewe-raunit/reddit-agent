from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.stealth.helpers import _smooth_wheel_scroll
from tools.upvote_tool import _smooth_scroll_to


async def _no_sleep(*args, **kwargs) -> None:
    return None


class DummyMouse:
    def __init__(self, page):
        self.page = page
        self.wheels = []

    async def wheel(self, x, y):
        self.wheels.append((x, y))
        self.page.scroll_y += y


class DummyCDP:
    def __init__(self, page):
        self.page = page
        self.events = []
        self.detached = False

    async def send(self, method, params):
        self.events.append((method, params))

    async def detach(self):
        self.detached = True


class DummyContext:
    def __init__(self, page):
        self.page = page
        self.cdp = DummyCDP(page)

    async def new_cdp_session(self, page):
        return self.cdp


class DummyPage:
    def __init__(self, *, has_touch=True, scroll_y=0, max_y=1200):
        self.has_touch = has_touch
        self.scroll_y = float(scroll_y)
        self.max_y = float(max_y)
        self.mouse = DummyMouse(self)
        self.context = DummyContext(self)
        self.js_scrolls = []

    async def evaluate(self, script, *args):
        if "navigator.maxTouchPoints" in script or "ontouchstart" in script:
            return self.has_touch
        if "document.documentElement.scrollHeight" in script:
            return {"scrollY": self.scroll_y, "maxY": self.max_y}
        if "window.scrollTo" in script:
            self.js_scrolls.append(args[0])
            self.scroll_y = float(args[0])
            return None
        if "window.scrollY" in script:
            return self.scroll_y
        if "window.innerWidth" in script:
            return {"w": 390, "h": 844}
        return None


class TestTouchScrollHelpers(unittest.IsolatedAsyncioTestCase):
    async def test_touch_scroll_falls_back_when_touch_events_do_not_move_page(self):
        page = DummyPage(has_touch=True, scroll_y=0)

        with patch("tools.stealth.helpers.asyncio.sleep", _no_sleep):
            await _smooth_wheel_scroll(page, 240)

        event_types = [params["type"] for method, params in page.context.cdp.events]
        self.assertIn("touchStart", event_types)
        self.assertIn("touchEnd", event_types)
        touch_points = [
            params["touchPoints"][0]
            for method, params in page.context.cdp.events
            if params["type"] in {"touchStart", "touchMove"} and params["touchPoints"]
        ]
        self.assertGreater(len({point["x"] for point in touch_points}), 1)
        self.assertTrue(all("radiusX" in point and "radiusY" in point and "force" in point for point in touch_points))
        self.assertTrue(page.mouse.wheels, "expected wheel fallback when touch gesture had no scroll effect")

    async def test_desktop_scroll_uses_wheel_without_touch_events(self):
        page = DummyPage(has_touch=False, scroll_y=0)

        with patch("tools.stealth.helpers.asyncio.sleep", _no_sleep):
            await _smooth_wheel_scroll(page, 180)

        self.assertTrue(page.mouse.wheels)
        self.assertGreater(len({wheel_y for _, wheel_y in page.mouse.wheels}), 1)
        self.assertEqual(page.context.cdp.events, [])

    async def test_upvote_scroll_to_does_not_force_js_when_gesture_reaches_target(self):
        page = DummyPage(has_touch=True, scroll_y=0)

        async def exact_scroll(_page, delta):
            _page.scroll_y += delta

        with patch("tools.upvote_tool._smooth_wheel_scroll", exact_scroll), \
             patch("tools.upvote_tool.asyncio.sleep", _no_sleep):
            final_y = await _smooth_scroll_to(page, 350)

        self.assertEqual(final_y, 350)
        self.assertEqual(page.js_scrolls, [])

    async def test_upvote_scroll_to_keeps_js_as_last_resort(self):
        page = DummyPage(has_touch=True, scroll_y=0)

        async def no_scroll(_page, delta):
            return None

        with patch("tools.upvote_tool._smooth_wheel_scroll", no_scroll), \
             patch("tools.upvote_tool.asyncio.sleep", _no_sleep):
            final_y = await _smooth_scroll_to(page, 350)

        self.assertEqual(final_y, 350)
        self.assertEqual(page.js_scrolls, [350.0])


if __name__ == "__main__":
    unittest.main()
