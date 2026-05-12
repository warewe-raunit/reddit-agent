from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reddit_login_state import classify_reddit_login_state


class TestRedditLoginStateClassification(unittest.TestCase):
    def test_author_user_links_do_not_count_as_logged_in(self):
        state = classify_reddit_login_state(
            {
                "loggedOut": False,
                "profileMenuVisible": False,
                "settingsVisible": False,
                "expectedUserVisible": False,
                "logoutVisible": False,
                "userLinkCount": 3,
            },
            has_session_cookie=False,
            expected_username="PaceNormal6940",
        )
        self.assertFalse(state["logged_in"])
        self.assertEqual(state["reason"], "no_session_cookie_or_account_menu")

    def test_session_cookie_counts_as_logged_in_even_with_loid_cookie_present(self):
        state = classify_reddit_login_state(
            {"loggedOut": False, "userLinkCount": 0},
            has_session_cookie=True,
            expected_username="PaceNormal6940",
        )
        self.assertTrue(state["logged_in"])
        self.assertEqual(state["reason"], "reddit_session_cookie")

    def test_visible_login_button_wins_over_weak_logged_in_clues(self):
        state = classify_reddit_login_state(
            {
                "loggedOut": True,
                "reason": "visible_login_or_signup",
                "profileMenuVisible": True,
                "expectedUserVisible": True,
            },
            has_session_cookie=False,
            expected_username="PaceNormal6940",
        )
        self.assertFalse(state["logged_in"])
        self.assertEqual(state["reason"], "visible_login_or_signup")


if __name__ == "__main__":
    unittest.main(verbosity=2)
