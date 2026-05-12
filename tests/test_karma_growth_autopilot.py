"""
Tests for Organic Karma Growth Autopilot helper logic.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.karma_growth_autopilot import (
    KarmaAutopilotSettings,
    fallback_draft,
    format_approval_request,
    is_karma_autopilot_request_text,
    is_karma_continue_command_text,
    is_karma_stop_command_text,
    normalize_settings,
    parse_settings_from_text,
    public_action_requires_approval,
    ranked_opportunities,
    score_candidate,
    validate_public_text,
)


class TestKarmaAutopilotSettings(unittest.TestCase):
    def test_defaults_match_requested_safety_posture(self) -> None:
        settings = normalize_settings()

        self.assertEqual(settings.max_comments_per_run, 3)
        self.assertEqual(settings.minimum_score, 70)
        self.assertFalse(settings.promotion_allowed)
        self.assertTrue(settings.require_approval_for_comments)
        self.assertTrue(settings.require_approval_for_replies)
        self.assertTrue(settings.require_approval_for_posts)

    def test_parse_controls_from_text(self) -> None:
        settings = parse_settings_from_text(
            """
            max comments per run: 2
            minimum score: 82
            allowlist: SaaS, startups
            blocklist: politics
            promotion allowed: true
            dry run
            """
        )

        self.assertEqual(settings.max_comments_per_run, 2)
        self.assertEqual(settings.minimum_score, 82)
        self.assertEqual(settings.subreddit_allowlist, ("SaaS", "startups"))
        self.assertEqual(settings.subreddit_blocklist, ("politics",))
        self.assertTrue(settings.promotion_allowed)
        self.assertTrue(settings.dry_run)


class TestKarmaAutopilotIntent(unittest.TestCase):
    def test_build_karma_phrases_start_autopilot(self) -> None:
        phrases = [
            "start building karma",
            "build karma",
            "grow karma",
            "karma builder",
            "start building reddit karma",
            "find karma comment opportunities",
            "find organic Reddit participation opportunities",
            "find Reddit comment opportunities for karma",
        ]

        for phrase in phrases:
            with self.subTest(phrase=phrase):
                self.assertTrue(is_karma_autopilot_request_text(phrase))

    def test_continue_and_stop_phrases(self) -> None:
        self.assertTrue(is_karma_continue_command_text("keep searching"))
        self.assertTrue(is_karma_stop_command_text("stop karma builder"))

    def test_karma_builder_is_not_exposed_as_react_tool(self) -> None:
        from agent_tools import make_tools

        tools = make_tools(
            lazy=object(),
            account_id="test-account",
            username="test-user",
            password="test-pass",
            confirmation_state={},
        )

        self.assertNotIn("karma_growth_autopilot", {tool.name for tool in tools})


class TestKarmaCandidateScoring(unittest.TestCase):
    def test_scores_recent_helpful_question_above_default_threshold(self) -> None:
        candidate = {
            "subreddit": "SaaS",
            "title": "How do I reduce bounce rate for cold email outreach?",
            "body": "Looking for advice before I send to a larger list.",
            "url": "https://www.reddit.com/r/SaaS/comments/abc/test/",
            "score": 12,
            "comment_count": 3,
            "age_days": 1,
        }

        result = score_candidate(candidate, KarmaAutopilotSettings())

        self.assertTrue(result["is_high_fit"])
        self.assertGreaterEqual(result["score"], 70)
        self.assertEqual(result["risk_level"], "low")

    def test_filters_locked_and_political_threads(self) -> None:
        settings = KarmaAutopilotSettings()
        locked = {
            "subreddit": "SaaS",
            "title": "How do I pick a CRM?",
            "url": "https://www.reddit.com/r/SaaS/comments/abc/test/",
            "locked": True,
        }
        political = {
            "subreddit": "SaaS",
            "title": "Election politics and startup marketing",
            "url": "https://www.reddit.com/r/SaaS/comments/def/test/",
        }

        self.assertFalse(score_candidate(locked, settings)["is_high_fit"])
        self.assertEqual(score_candidate(locked, settings)["risk_level"], "high")
        self.assertFalse(score_candidate(political, settings)["is_high_fit"])
        self.assertEqual(score_candidate(political, settings)["risk_level"], "high")

    def test_allowlist_and_blocklist_are_enforced(self) -> None:
        settings = normalize_settings(
            subreddit_allowlist="SaaS",
            subreddit_blocklist="startups",
        )
        outside = {
            "subreddit": "marketing",
            "title": "How to improve newsletter deliverability?",
            "url": "https://www.reddit.com/r/marketing/comments/abc/test/",
        }
        blocked = {
            "subreddit": "startups",
            "title": "How to improve newsletter deliverability?",
            "url": "https://www.reddit.com/r/startups/comments/abc/test/",
        }

        self.assertIn("outside_subreddit_allowlist", score_candidate(outside, settings)["skip_reason"])
        self.assertIn("blocked_subreddit", score_candidate(blocked, settings)["skip_reason"])

    def test_ranked_opportunities_puts_best_candidate_first(self) -> None:
        settings = KarmaAutopilotSettings()
        weak = {
            "subreddit": "SaaS",
            "title": "Random update",
            "url": "https://www.reddit.com/r/SaaS/comments/weak/test/",
        }
        strong = {
            "subreddit": "SaaS",
            "title": "Looking for email deliverability advice for cold email",
            "body": "Any advice on reducing bounce rate?",
            "url": "https://www.reddit.com/r/SaaS/comments/strong/test/",
            "score": 10,
            "comment_count": 2,
            "age_days": 1,
        }

        ranked = ranked_opportunities([weak, strong], settings)

        self.assertEqual(ranked[0]["url"], strong["url"])
        self.assertTrue(ranked[0]["is_high_fit"])


class TestKarmaDraftSafety(unittest.TestCase):
    def test_fallback_draft_is_short_and_non_promotional_by_default(self) -> None:
        draft = fallback_draft({
            "title": "How do I reduce bounce rate for cold email?",
            "body": "Looking for advice.",
        })

        self.assertLessEqual(len([line for line in draft.splitlines() if line.strip()]), 5)
        self.assertEqual(validate_public_text(draft, promotion_allowed=False), [])

    def test_validation_blocks_promotion_evasion_and_fake_claims(self) -> None:
        text = "I've been using email verifier . io for months, saved me fr"

        issues = validate_public_text(text, promotion_allowed=False)

        self.assertIn("spam_filter_evasion_pattern", issues)
        self.assertIn("fake_personal_claim_risk", issues)
        self.assertIn("promotion_not_allowed", issues)

    def test_public_text_actions_require_approval_by_default(self) -> None:
        settings = KarmaAutopilotSettings()

        self.assertTrue(public_action_requires_approval("comment_on_post", settings))
        self.assertTrue(public_action_requires_approval("reply_to_reddit_comment", settings))
        self.assertTrue(public_action_requires_approval("submit_text_post", settings))

    def test_approval_request_contains_required_fields(self) -> None:
        message = format_approval_request({
            "action": "comment_on_post",
            "subreddit": "SaaS",
            "post_url": "https://www.reddit.com/r/SaaS/comments/abc/test/",
            "context_summary": "A founder asks how to reduce cold email bounce rate.",
            "reason": "Fresh, specific, low-risk advice request.",
            "risk_level": "low",
            "score": 88,
            "text": "I'd start with list quality before changing copy.\nA small validation pass usually shows the real issue fast.",
        })

        self.assertIn("Subreddit: r/SaaS", message)
        self.assertIn("Post/comment URL: https://www.reddit.com/r/SaaS/comments/abc/test/", message)
        self.assertIn("Context summary:", message)
        self.assertIn("Why this is good:", message)
        self.assertIn("Exact final text:", message)
        self.assertIn("Risk level: low", message)


if __name__ == "__main__":
    unittest.main()
