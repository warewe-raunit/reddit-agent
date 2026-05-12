from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy_config import captcha_proxy_config, capsolver_proxy_url, playwright_proxy_config


class TestProxyConfig(unittest.TestCase):
    def test_playwright_proxy_config_uses_server_and_credentials(self):
        proxy = "http://user:pass@example.proxy:10090"
        self.assertEqual(
            playwright_proxy_config(proxy),
            {
                "server": "http://example.proxy:10090",
                "username": "user",
                "password": "pass",
            },
        )

    def test_captcha_proxy_config_keeps_capsolver_fields(self):
        proxy = "http://user:pass@example.proxy:10090"
        config = captcha_proxy_config(proxy)
        self.assertEqual(config["server"], "http://example.proxy:10090")
        self.assertEqual(config["host"], "example.proxy")
        self.assertEqual(config["port"], 10090)
        self.assertEqual(config["auth_user"], "user")
        self.assertEqual(config["auth_pass"], "pass")
        self.assertEqual(capsolver_proxy_url(config), proxy)

    def test_capsolver_proxy_url_accepts_legacy_server_only_shape(self):
        config = {"server": "http://user:pass@example.proxy:10090"}
        self.assertEqual(
            capsolver_proxy_url(config),
            "http://user:pass@example.proxy:10090",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
