"""
tools/ — AI agent-callable Reddit action tools with full stealth and anti-bot features.

Each tool exposes a single async `run_tool()` function that returns:
    {success: bool, data: dict, error: str | None}

Stealth features (baked into every tool):
- BrowserProfileManager: deterministic fingerprint per account (WebGL, canvas, audio, plugins, UA)
- HumanBehaviorEngine: log-normal timing, fatigue, distraction spikes, circadian rhythm, Perlin jitter
- simulate_reading(): scroll + mouse + idle pause before every action (critical Reddit anti-bot signal)
- _human_type(): per-character delays, bigram speedup, typo simulation
- Bezier-curve mouse movement with overshoot and micro-corrections
- Bearer token capture from cookies + network interception
- Shadow DOM traversal for shreddit-* web components
- CAPTCHA solving (2captcha / anticaptcha / capsolver)

Available tools:
    login_tool.run_tool(page, account_id, username, password, ...)
    post_tool.run_tool(page, account_id, subreddit, title, body, ...)
    comment_tool.run_tool(page, account_id, post_url, text, ...)
    reply_tool.run_tool(page, account_id, comment_fullname, post_url, text, ...)
    upvote_tool.run_tool(page, account_id, post_url, ...)
    comment_upvote_tool.run_tool(page, account_id, comment_url, comment_fullname, ...)
    browse_tool.run_tool(page, account_id, mode, ...)
    join_subreddit_tool.run_tool(page, account_id, subreddit, ...)

Stealth submodules:
    tools.stealth.fingerprint.BrowserProfileManager           (22 techniques)
    tools.stealth.advanced_fingerprint.AdvancedFingerprintManager (40 techniques)
    tools.stealth.bot_detection_evasion.BotDetectionEvasionManager (50+ techniques)
    tools.stealth.human_behavior.HumanBehaviorEngine, create_engine
    tools.stealth.helpers.simulate_reading, browse_random_posts, ...
    tools.stealth.captcha.solve_login_recaptcha, inject_grecaptcha_override

Full-stack injection (all 112+ techniques, 11 vendors):
    mgr = BotDetectionEvasionManager()
    await mgr.inject_all(page, profile)   # call before page.goto()
    await mgr.post_load_check(page)       # call after page.goto()
"""

from tools.login_tool import run_tool as login
from tools.post_tool import run_tool as post
from tools.comment_tool import run_tool as comment
from tools.reply_tool import run_tool as reply
from tools.upvote_tool import run_tool as upvote
from tools.comment_upvote_tool import run_tool as comment_upvote
from tools.browse_tool import run_tool as browse
from tools.join_subreddit_tool import run_tool as join_subreddit

__all__ = [
    "login",
    "post",
    "comment",
    "reply",
    "upvote",
    "comment_upvote",
    "browse",
    "join_subreddit",
]
