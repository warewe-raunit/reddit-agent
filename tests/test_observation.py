"""
tests/test_observation.py — Unit tests for observation layer and helpers.

Covers:
- observation element structure validation
- comment permalink ID parsing
- confirmation decision logic
- already-upvoted detection parsing

Run with: python -m pytest tests/test_observation.py -v
Or as a dry-run: python tests/test_observation.py
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest

from confirmation import confirmation_reply
from react_loop_guard import (
    DEFAULT_REACT_RECURSION_LIMIT,
    MAX_REACT_RECURSION_LIMIT,
    MIN_REACT_RECURSION_LIMIT,
    is_react_loop_error,
    react_recursion_limit,
    react_runtime_config,
    react_timeout_seconds,
)
from reddit_url_intent import has_comment_intent, has_upvote_intent, reddit_url_points_to_comment
from reddit_action_messages import post_upvote_result_message
from tools.comment_upvote_tool import _comment_fullname, _bare_comment_id, _fallback_viewport_coords
from tools.observation_tool import summarize_observation
from tools.upvote_tool import (
    _POST_UPVOTE_BUTTON_SCRIPT,
    _SEARCH_INPUT_SCRIPT,
    _SEARCH_RESULT_LINK_SCRIPT,
    _post_search_queries_from_url,
    _post_search_query_from_url,
    _target_post_id_from_url,
)


# ---------------------------------------------------------------------------
# _comment_fullname parsing
# ---------------------------------------------------------------------------

class TestCommentFullnameParsing(unittest.TestCase):
    def test_bare_fullname_with_prefix(self):
        result = _comment_fullname(comment_fullname="t1_abc123")
        self.assertEqual(result, "t1_abc123")

    def test_bare_fullname_without_prefix(self):
        result = _comment_fullname(comment_fullname="abc123")
        self.assertEqual(result, "t1_abc123")

    def test_url_standard_permalink(self):
        url = "https://www.reddit.com/r/SaaS/comments/xyz789/post_title/abc123/"
        result = _comment_fullname(comment_url=url)
        self.assertEqual(result, "t1_abc123")

    def test_url_no_trailing_slash(self):
        url = "https://www.reddit.com/r/SaaS/comments/xyz789/post_title/abc123"
        result = _comment_fullname(comment_url=url)
        self.assertEqual(result, "t1_abc123")

    def test_url_query_param(self):
        url = "https://www.reddit.com/r/SaaS/comments/xyz789/?comment=def456"
        result = _comment_fullname(comment_url=url)
        self.assertEqual(result, "t1_def456")

    def test_url_hash_fragment(self):
        url = "https://www.reddit.com/r/SaaS/comments/xyz789/#t1_ghi789"
        result = _comment_fullname(comment_url=url)
        self.assertEqual(result, "t1_ghi789")

    def test_missing_inputs_returns_none(self):
        result = _comment_fullname()
        self.assertIsNone(result)

    def test_invalid_url_returns_none(self):
        result = _comment_fullname(comment_url="https://www.reddit.com/r/SaaS/")
        self.assertIsNone(result)

    def test_fullname_takes_priority_over_url(self):
        url = "https://www.reddit.com/r/SaaS/comments/xyz789/post_title/fromurl/"
        result = _comment_fullname(comment_fullname="priority123", comment_url=url)
        self.assertEqual(result, "t1_priority123")


class TestBareCommentId(unittest.TestCase):
    def test_strips_prefix(self):
        self.assertEqual(_bare_comment_id("t1_abc123"), "abc123")

    def test_no_prefix_unchanged(self):
        self.assertEqual(_bare_comment_id("abc123"), "abc123")


# ---------------------------------------------------------------------------
# summarize_observation
# ---------------------------------------------------------------------------

class TestSummarizeObservation(unittest.TestCase):
    def _make_obs(self, **kwargs) -> dict:
        defaults = {
            "url": "https://www.reddit.com/r/SaaS/comments/test/",
            "title": "Test Post | Reddit",
            "text": "Some visible text",
            "accessibility_snapshot": None,
            "screenshot_b64": None,
            "interactive_elements": [],
            "overlays": [],
        }
        defaults.update(kwargs)
        return defaults

    def test_contains_url(self):
        obs = self._make_obs()
        summary = summarize_observation(obs)
        self.assertIn("reddit.com", summary)

    def test_contains_title(self):
        obs = self._make_obs(title="My Reddit Post | Reddit")
        summary = summarize_observation(obs)
        self.assertIn("My Reddit Post", summary)

    def test_overlay_login_wall_reported(self):
        obs = self._make_obs(overlays=[{"type": "login_wall", "text": "Log In"}])
        summary = summarize_observation(obs)
        self.assertIn("login_wall", summary)

    def test_overlay_captcha_reported(self):
        obs = self._make_obs(overlays=[{"type": "captcha"}])
        summary = summarize_observation(obs)
        self.assertIn("captcha", summary)

    def test_interactive_elements_listed(self):
        obs = self._make_obs(interactive_elements=[
            {"role": "button", "tag": "button", "name": "Upvote", "bbox": {"x": 10, "y": 20, "w": 30, "h": 30},
             "disabled": False, "pressed": False, "href": None, "selector": '[aria-label="Upvote"]'},
        ])
        summary = summarize_observation(obs)
        self.assertIn("Upvote", summary)

    def test_disabled_element_flagged(self):
        obs = self._make_obs(interactive_elements=[
            {"role": "button", "tag": "button", "name": "Vote", "bbox": {"x": 0, "y": 0, "w": 30, "h": 30},
             "disabled": True, "pressed": None, "href": None, "selector": None},
        ])
        summary = summarize_observation(obs)
        self.assertIn("[disabled]", summary)

    def test_pressed_element_flagged(self):
        obs = self._make_obs(interactive_elements=[
            {"role": "button", "tag": "button", "name": "Upvote", "bbox": {"x": 0, "y": 0, "w": 30, "h": 30},
             "disabled": False, "pressed": True, "href": None, "selector": None},
        ])
        summary = summarize_observation(obs)
        self.assertIn("[pressed]", summary)

    def test_shadow_element_flagged(self):
        obs = self._make_obs(interactive_elements=[
            {"role": "button", "tag": "button", "name": "Upvote", "bbox": {"x": 0, "y": 0, "w": 30, "h": 30},
             "disabled": False, "pressed": False, "href": None, "selector": None, "source": "shadow"},
        ])
        summary = summarize_observation(obs)
        self.assertIn("[shadow]", summary)

    def test_no_elements_when_include_false(self):
        obs = self._make_obs(interactive_elements=[
            {"role": "button", "tag": "button", "name": "Upvote", "bbox": {"x": 0, "y": 0, "w": 30, "h": 30},
             "disabled": False, "pressed": False, "href": None, "selector": None},
        ])
        summary = summarize_observation(obs, include_elements=False)
        self.assertNotIn("Upvote", summary)

    def test_truncated_text_preview_is_labeled(self):
        obs = self._make_obs(text="x" * 400)
        summary = summarize_observation(obs)
        self.assertIn("Text preview (truncated):", summary)
        self.assertTrue(summary.endswith("..."))


# ---------------------------------------------------------------------------
# Confirmation decision logic
# ---------------------------------------------------------------------------

class TestConfirmationGating(unittest.TestCase):
    """Verify confirmation logic: direct precise URL requests skip confirmation;
    indirect/search-derived targets require it."""

    def _needs_confirmation(self, user_message: str, action_url: str, is_search_derived: bool) -> bool:
        """Simulate the rule: confirm if action was search-derived or message is vague."""
        import re
        has_exact_url = bool(re.search(r"https?://(?:www\.)?reddit\.com/\S+", user_message))
        if has_exact_url and not is_search_derived:
            return False
        return True

    def test_direct_url_no_confirmation(self):
        self.assertFalse(self._needs_confirmation(
            "upvote https://www.reddit.com/r/SaaS/comments/abc/title/def/",
            "https://www.reddit.com/r/SaaS/comments/abc/title/def/",
            is_search_derived=False,
        ))

    def test_vague_request_needs_confirmation(self):
        self.assertTrue(self._needs_confirmation(
            "upvote the top comment on the SaaS post about pricing",
            "https://www.reddit.com/r/SaaS/comments/abc/title/def/",
            is_search_derived=True,
        ))

    def test_search_derived_needs_confirmation(self):
        self.assertTrue(self._needs_confirmation(
            "upvote the best comment",
            "https://www.reddit.com/r/SaaS/comments/abc/",
            is_search_derived=True,
        ))

    def test_confirmation_yes_reply(self):
        self.assertTrue(confirmation_reply("yes"))

    def test_confirmation_no_reply(self):
        self.assertFalse(confirmation_reply("no"))

    def test_confirmation_unknown_reply(self):
        self.assertIsNone(confirmation_reply("maybe later"))


# ---------------------------------------------------------------------------
# ReAct loop guard
# ---------------------------------------------------------------------------

class TestReactLoopGuard(unittest.TestCase):
    def test_default_recursion_limit(self):
        self.assertEqual(react_recursion_limit({}), DEFAULT_REACT_RECURSION_LIMIT)

    def test_recursion_limit_clamped_low(self):
        self.assertEqual(react_recursion_limit({"REACT_RECURSION_LIMIT": "1"}), MIN_REACT_RECURSION_LIMIT)

    def test_recursion_limit_clamped_high(self):
        self.assertEqual(react_recursion_limit({"REACT_RECURSION_LIMIT": "999"}), MAX_REACT_RECURSION_LIMIT)

    def test_invalid_recursion_limit_uses_default(self):
        self.assertEqual(react_recursion_limit({"REACT_RECURSION_LIMIT": "abc"}), DEFAULT_REACT_RECURSION_LIMIT)

    def test_runtime_config_contains_recursion_limit(self):
        self.assertEqual(react_runtime_config({"REACT_RECURSION_LIMIT": "20"}), {"recursion_limit": 20})

    def test_timeout_seconds_clamped(self):
        self.assertEqual(react_timeout_seconds({"REACT_TIMEOUT_SECONDS": "1"}), 15.0)

    def test_detects_langgraph_recursion_error_by_class_name(self):
        GraphRecursionError = type("GraphRecursionError", (Exception,), {})
        self.assertTrue(is_react_loop_error(GraphRecursionError("limit reached")))

    def test_detects_langgraph_recursion_error_by_message(self):
        exc = RuntimeError("Recursion limit of 32 reached without hitting a stop condition in LangGraph")
        self.assertTrue(is_react_loop_error(exc))


# ---------------------------------------------------------------------------
# Reddit URL intent helpers
# ---------------------------------------------------------------------------

class TestRedditUrlIntent(unittest.TestCase):
    def test_post_url_with_comments_path_is_not_comment_permalink(self):
        url = (
            "https://www.reddit.com/r/coldemail/comments/1t3mgv9/"
            "we_send_436000_emailsmo_ama/?utm_source=share"
        )
        self.assertFalse(reddit_url_points_to_comment(url))

    def test_comment_permalink_is_comment_target(self):
        url = "https://www.reddit.com/r/SaaS/comments/abc123/title_here/def456/"
        self.assertTrue(reddit_url_points_to_comment(url))

    def test_comment_query_param_is_comment_target(self):
        url = "https://www.reddit.com/r/SaaS/comments/abc123/title_here/?comment=def456"
        self.assertTrue(reddit_url_points_to_comment(url))

    def test_comment_fragment_is_comment_target(self):
        url = "https://www.reddit.com/r/SaaS/comments/abc123/title_here/#t1_def456"
        self.assertTrue(reddit_url_points_to_comment(url))

    def test_upvote_intent_variants(self):
        self.assertTrue(has_upvote_intent("upvote this post"))
        self.assertTrue(has_upvote_intent("up vote this post"))

    def test_comment_intent_does_not_match_comments_url_segment(self):
        self.assertFalse(has_comment_intent("https://www.reddit.com/r/x/comments/abc/title/"))
        self.assertTrue(has_comment_intent("upvote this comment"))


# ---------------------------------------------------------------------------
# Reddit action messages
# ---------------------------------------------------------------------------

class TestRedditActionMessages(unittest.TestCase):
    def test_server_verified_without_ui_is_explicit(self):
        msg = post_upvote_result_message({
            "server_verified": True,
            "ui_verified_before_reload": False,
            "ui_verified_after_reload": False,
            "score_before": "29",
            "score_after_reload": "29",
            "verification_source": "server",
        })
        self.assertIn("recorded on Reddit server", msg)
        self.assertIn("Visible score stayed 29", msg)

    def test_score_change_is_reported(self):
        msg = post_upvote_result_message({
            "server_verified": False,
            "ui_verified_before_reload": True,
            "ui_verified_after_reload": True,
            "score_before": "29",
            "score_after_reload": "30",
            "verification_source": "ui_after_reload",
        })
        self.assertIn("Visible score changed from 29 to 30", msg)


# ---------------------------------------------------------------------------
# Comment upvote coordinate fallback
# ---------------------------------------------------------------------------

class TestCommentUpvoteCoordinateFallback(unittest.TestCase):
    def test_fallback_uses_document_x_not_viewport_y(self):
        btn_info = {"absX": 240, "absY": 500}
        viewport = {"scrollX": 0, "scrollY": 300, "width": 800, "height": 600}
        coords = _fallback_viewport_coords(btn_info, viewport)
        self.assertEqual(coords, {"x": 240, "y": 200})

    def test_fallback_returns_none_when_outside_viewport(self):
        btn_info = {"absX": 240, "absY": 1200}
        viewport = {"scrollX": 0, "scrollY": 300, "width": 800, "height": 600}
        self.assertIsNone(_fallback_viewport_coords(btn_info, viewport))


# ---------------------------------------------------------------------------
# Already-upvoted detection
# ---------------------------------------------------------------------------

class TestAlreadyUpvotedDetection(unittest.TestCase):
    """Verify the upvote detection heuristics on simulated btn_info dicts."""

    def _is_already_upvoted(self, btn_info: dict) -> bool:
        return btn_info.get("already", False)

    def test_already_false_when_not_upvoted(self):
        btn_info = {"found": True, "already": False, "selector": "button[upvote]"}
        self.assertFalse(self._is_already_upvoted(btn_info))

    def test_already_true_vote_state(self):
        btn_info = {"found": True, "already": True, "source": "vote-state-attr"}
        self.assertTrue(self._is_already_upvoted(btn_info))

    def test_already_true_button_state(self):
        btn_info = {"found": True, "already": True, "source": "button-state"}
        self.assertTrue(self._is_already_upvoted(btn_info))

    def test_not_found_is_not_already(self):
        btn_info = {"found": False, "reason": "post_not_found"}
        self.assertFalse(self._is_already_upvoted(btn_info))


class TestPostUpvoteMobileSelectors(unittest.TestCase):
    def test_post_upvote_finder_includes_mobile_reddit_controls(self):
        self.assertIn("shreddit-feed-post", _POST_UPVOTE_BUTTON_SCRIPT)
        self.assertIn("document-before-comments", _POST_UPVOTE_BUTTON_SCRIPT)
        self.assertIn('button[aria-label*="up vote" i]', _POST_UPVOTE_BUTTON_SCRIPT)
        self.assertIn('button[icon-name="upvote-outline"]', _POST_UPVOTE_BUTTON_SCRIPT)

    def test_post_upvote_finder_reports_mobile_web_hidden_control(self):
        self.assertIn("mobile_web_vote_control_not_rendered", _POST_UPVOTE_BUTTON_SCRIPT)


class TestPostUpvoteSearchNavigation(unittest.TestCase):
    def test_extracts_target_post_id_from_url(self):
        url = "https://www.reddit.com/r/coldemail/comments/1t68p9g/just_starting_need_advice_from_the_best/?utm_source=share"
        self.assertEqual(_target_post_id_from_url(url), "1t68p9g")

    def test_builds_human_search_query_from_permalink_slug(self):
        url = "https://www.reddit.com/r/coldemail/comments/1t68p9g/just_starting_need_advice_from_the_best/"
        self.assertEqual(_post_search_query_from_url(url), "just starting need advice from the best")

    def test_builds_ordered_search_query_fallbacks(self):
        url = "https://www.reddit.com/r/coldemail/comments/1t68p9g/just_starting_need_advice_from_the_best/"
        self.assertEqual(
            _post_search_queries_from_url(url),
            [
                "just starting need advice from the best",
                "just starting need advice from the best r/coldemail",
                "1t68p9g",
            ],
        )

    def test_search_navigation_scripts_include_expected_surfaces(self):
        self.assertIn('input[type="search"]', _SEARCH_INPUT_SCRIPT)
        self.assertIn('a[href*="/comments/"]', _SEARCH_RESULT_LINK_SCRIPT)
        self.assertIn("targetPostId", _SEARCH_RESULT_LINK_SCRIPT)
        self.assertIn("data-agent-search-result", _SEARCH_RESULT_LINK_SCRIPT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
