"""
agent.py — Reddit AI agent using LangGraph ReAct + OpenRouter.
Browser launches lazily — only when a Reddit tool is actually called.
Conversation history persists across turns in the same session.
"""

from __future__ import annotations

import os
import re
import asyncio
from typing import Optional

from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from browser_manager import LazyBrowser
from agent_tools import (
    comment_on_reddit_post,
    ensure_reddit_logged_in,
    is_reddit_action_request,
    is_reddit_logged_in,
    make_tools,
    open_reddit_home,
    upvote_reddit_comment,
)

load_dotenv(override=True)


def _build_llm(model: Optional[str] = None) -> ChatOpenAI:
    return ChatOpenAI(
        model=model or os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001"),
        openai_api_key=os.getenv("OPENROUTER_API_KEY"),
        openai_api_base="https://openrouter.ai/api/v1",
        temperature=0.7,
        timeout=float(os.getenv("OPENROUTER_TIMEOUT", "60")),
        max_retries=int(os.getenv("OPENROUTER_MAX_RETRIES", "2")),
        default_headers={
            "HTTP-Referer": "https://github.com/redditagent",
            "X-Title": "RedditAgent",
        },
    )


SYSTEM_PROMPT = """You are a helpful Reddit automation assistant. You can chat normally AND control a real browser to perform Reddit actions.

Behavior:
- For casual conversation, questions, or anything unrelated to Reddit actions — respond directly without using any tools.
- Use browser tools ONLY when the user explicitly asks you to perform a Reddit action (login, browse, post, comment, upvote, join subreddit, etc.).
- If the user asks to "upvote this comment" or provides a comment permalink with an upvote request, call upvote_comment. Do not ask what the comment should say.
- If the user asks to "upvote this post" or provides a post URL with an upvote request, call upvote_post.
- If the user asks to comment on a post by description/title instead of giving a URL, call search_reddit_posts first, pick the best matching result, then comment_on_post.
- If search results are ambiguous, ask the user which result to use before posting.
- If the user asks for warm-up mode, account warmup, or karma-building help, use warmup_reddit for browsing-only warmup and find_warmup_comment_opportunities to surface candidate posts. Draft helpful comments for the user to approve, but do not submit warm-up comments automatically.

When performing Reddit actions, follow this sequence:
1. Call check_session to verify login status.
2. If not logged in → call login_reddit.
3. After session verified/restored → call browse_reddit to warm up the account.
4. Then perform the requested action.
5. If a tool fails, report the error clearly. Do not retry more than once.

Keep responses concise and friendly."""


def _fallback_models() -> list[str]:
    configured = os.getenv("OPENROUTER_FALLBACK_MODELS", "")
    models = [m.strip() for m in configured.split(",") if m.strip()]
    primary = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
    defaults = ["openai/gpt-4o-mini", "anthropic/claude-3.5-haiku"]
    ordered: list[str] = []
    for model in [primary, *models, *defaults]:
        if model and model not in ordered:
            ordered.append(model)
    return ordered


def _is_transient_llm_error(exc: Exception) -> bool:
    text = str(exc).lower()
    status_code = getattr(exc, "status_code", None)
    return (
        status_code in {408, 429, 500, 502, 503, 504}
        or "504" in text
        or "timeout" in text
        or "aborted" in text
        or "rate limit" in text
    )


def _local_reply(user_message: str) -> Optional[str]:
    msg = user_message.strip().lower()
    compact = re.sub(r"[^a-z0-9\s]", "", msg)

    greetings = {"hi", "hello", "hey", "yo", "hii", "hiya", "namaste"}
    if compact in greetings:
        return "Hi! I am ready. Ask me to log in to Reddit, search for a post, comment, upvote, post, or just chat."

    if compact in {"thanks", "thank you", "ok", "okay"}:
        return "You got it."

    return None


def _is_login_status_question(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    patterns = [
        r"\bam i logged in\b",
        r"\bam i login\b",
        r"\blogin status\b",
        r"\bcheck session\b",
        r"\bcheck login\b",
        r"\bam i connected\b",
    ]
    return any(re.search(pattern, compact) for pattern in patterns)


def _extract_reddit_url(user_message: str) -> Optional[str]:
    match = re.search(r"https?://(?:www\.)?reddit\.com/\S+", user_message)
    if not match:
        return None
    return match.group(0).rstrip(").,]")


def _is_login_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    return bool(re.search(r"\b(log ?in|login)\b", compact)) and "status" not in compact


def _is_open_reddit_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    return "open reddit" in compact or compact in {"reddit", "go to reddit"}


def _is_comment_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    if re.search(r"\bup\s*vote\b|\bupvote\b", compact):
        return False
    return bool(re.search(r"\b(comment|reply)\b", compact)) and "reddit.com" in user_message.lower()


def _is_comment_upvote_request(user_message: str) -> bool:
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    has_upvote = bool(re.search(r"\bup\s*vote\b|\bupvote\b", compact))
    has_comment = bool(re.search(r"\bcomment\b", compact)) or "/comment/" in user_message.lower()
    return has_upvote and has_comment and "reddit.com" in user_message.lower()


class RedditAgent:
    """Persistent agent with lazy browser and conversation history."""

    def __init__(self, account_id: str, username: str, password: str, proxy_url: Optional[str] = None, headless: bool = False):
        self.account_id = account_id
        self.username = username
        self.password = password
        self.lazy = LazyBrowser(account_id, proxy_url, headless)
        self._models = _fallback_models()
        self._model_index = 0
        self._llm = _build_llm(self._models[self._model_index])
        self._history: list[BaseMessage] = []
        self._tools = make_tools(self.lazy, account_id, username, password, proxy_url)
        self._agent = create_react_agent(self._llm, self._tools, prompt=SYSTEM_PROMPT)
        self._pending_comment_url: Optional[str] = None

    def _switch_model(self, model_index: int) -> None:
        self._model_index = model_index
        self._llm = _build_llm(self._models[self._model_index])
        self._agent = create_react_agent(self._llm, self._tools, prompt=SYSTEM_PROMPT)

    async def check_login_status(self) -> str:
        page = await self.lazy.get_page()
        logged_in = await is_reddit_logged_in(page)
        if logged_in:
            await self.lazy.persist_session()
            return "Yes, you are logged in to Reddit. I saved the current browser session."
        return "No, I could not detect an active Reddit login in this browser session."

    async def login_reddit(self) -> str:
        ok, status = await ensure_reddit_logged_in(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )
        return status if ok else status

    async def open_reddit(self) -> str:
        return await open_reddit_home(
            self.lazy, self.account_id, self.username, self.password, self.lazy.proxy_url
        )

    async def comment_on_post(self, post_url: str, text: str) -> str:
        return await comment_on_reddit_post(
            lazy=self.lazy,
            account_id=self.account_id,
            username=self.username,
            password=self.password,
            post_url=post_url,
            text=text,
            proxy_url=self.lazy.proxy_url,
        )

    async def upvote_comment(self, comment_url: str) -> str:
        return await upvote_reddit_comment(
            lazy=self.lazy,
            account_id=self.account_id,
            username=self.username,
            password=self.password,
            comment_url=comment_url,
            proxy_url=self.lazy.proxy_url,
        )

    async def chat(self, user_message: str) -> str:
        self._history.append(HumanMessage(content=user_message))

        if _is_login_status_question(user_message):
            return await self.check_login_status()

        if self._pending_comment_url and not is_reddit_action_request(user_message):
            post_url = self._pending_comment_url
            self._pending_comment_url = None
            return await self.comment_on_post(post_url, user_message.strip())

        if _is_comment_upvote_request(user_message):
            comment_url = _extract_reddit_url(user_message)
            if not comment_url:
                return "Please send the Reddit comment URL you want me to upvote."
            return await self.upvote_comment(comment_url)

        if _is_comment_request(user_message):
            post_url = _extract_reddit_url(user_message)
            if not post_url:
                return "Please send the Reddit post URL you want me to comment on."
            self._pending_comment_url = post_url
            return "What would you like the comment to say?"

        if _is_open_reddit_request(user_message):
            return await self.open_reddit()

        if _is_login_request(user_message):
            return await self.login_reddit()

        local = _local_reply(user_message)
        if local:
            return local

        last_error: Optional[Exception] = None
        start_index = self._model_index
        model_order = list(range(start_index, len(self._models))) + list(range(0, start_index))

        for model_index in model_order:
            self._switch_model(model_index)
            for attempt in range(2):
                try:
                    result = await self._agent.ainvoke({"messages": self._history})

                    all_messages: list[BaseMessage] = result["messages"]
                    # Last message is the final AI response
                    final = all_messages[-1]
                    reply = final.content if hasattr(final, "content") else str(final)

                    # Keep full history for next turn
                    self._history = all_messages
                    return reply
                except Exception as exc:
                    last_error = exc
                    if not _is_transient_llm_error(exc):
                        raise
                    await asyncio.sleep(1.5 * (attempt + 1))

        return (
            "The AI provider is temporarily failing, so I could not process that message yet. "
            f"Last error: {last_error}"
        )

    async def close(self) -> None:
        await self.lazy.close()
