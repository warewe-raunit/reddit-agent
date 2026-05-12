"""
tests/test_browser_profile.py — Browser profile configuration tests.

Run with: python -m pytest tests/test_browser_profile.py -v
Or as a dry-run: python tests/test_browser_profile.py
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from browser_manager import profile_session_id
from tools.stealth.bot_detection_evasion import build_evasion_script
from tools.stealth.fingerprint import BrowserProfileManager


class TestBrowserProfileConfig(unittest.TestCase):
    def test_mobile_env_profile_overrides_browser_identity(self):
        env = {
            "BROWSER_PROFILE_IS_ACTIVE": "true",
            "BROWSER_DEVICE_CATEGORY": "mobile",
            "BROWSER_USER_AGENT": (
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
            ),
            "BROWSER_SEC_CH_UA": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "BROWSER_SEC_CH_UA_MOBILE": "?1",
            "BROWSER_SEC_CH_UA_PLATFORM": '"Android"',
            "BROWSER_PLATFORM": "Linux armv8l",
            "BROWSER_WIDTH": "412",
            "BROWSER_HEIGHT": "915",
        }

        with patch.dict(os.environ, env, clear=False):
            profile = BrowserProfileManager().generate("mobile-account")

        self.assertTrue(profile["is_mobile"])
        self.assertTrue(profile["has_touch"])
        self.assertEqual(profile["device_category"], "mobile")
        self.assertEqual(profile["screen_resolution"], {"width": 412, "height": 915})
        self.assertEqual(profile["platform"], "Linux armv8l")
        self.assertEqual(profile["sec_ch_ua_mobile"], "?1")
        self.assertEqual(profile["sec_ch_ua_platform"], '"Android"')
        self.assertIn("Android 13; Pixel 7", profile["user_agent"])
        self.assertEqual(profile["max_touch_points"], 5)

    def test_user_agent_data_script_uses_mobile_client_hints(self):
        env = {
            "BROWSER_PROFILE_IS_ACTIVE": "true",
            "BROWSER_DEVICE_CATEGORY": "mobile",
            "BROWSER_SEC_CH_UA_MOBILE": "?1",
            "BROWSER_SEC_CH_UA_PLATFORM": '"Android"',
        }

        with patch.dict(os.environ, env, clear=False):
            profile = BrowserProfileManager().generate("mobile-account")

        script = build_evasion_script(profile)
        self.assertIn("const _CH_MOBILE = true;", script)
        self.assertIn('const _CH_PLATFORM = "Android";', script)
        self.assertIn('model: _CH_MODEL', script)

    def test_mobile_session_does_not_reuse_desktop_storage(self):
        env = {
            "BROWSER_PROFILE_IS_ACTIVE": "true",
            "BROWSER_DEVICE_CATEGORY": "mobile",
        }

        with patch.dict(os.environ, env, clear=False):
            profile = BrowserProfileManager().generate("PaceNormal6940")

        self.assertEqual(profile_session_id("PaceNormal6940", profile), "PaceNormal6940__mobile")
        self.assertEqual(
            profile_session_id("PaceNormal6940", {"device_category": "desktop", "is_mobile": False}),
            "PaceNormal6940",
        )


if __name__ == "__main__":
    unittest.main()
