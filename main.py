"""
main.py — Entry point. Conversational Reddit agent CLI.

Usage:
    python main.py

The agent chats normally and opens the browser only when a Reddit
action (login, browse, comment, upvote) is explicitly requested.
"""

import asyncio
import logging
import os
import sys
import traceback

import structlog
from dotenv import load_dotenv

from agent import RedditAgent

load_dotenv(override=True)

# Route stdlib logging to stdout — INFO suppresses httpx/httpcore wire noise
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=logging.INFO,
)
for _noisy in ("httpx", "httpcore", "openai", "langchain", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# Human-readable structlog output for tool events
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)


async def main():
    account_id = os.getenv("REDDIT_USERNAME", "account_1")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")
    proxy_url = os.getenv("PROXY_URL") or None

    if not username or not password:
        print("Set REDDIT_USERNAME and REDDIT_PASSWORD in .env")
        return

    agent = RedditAgent(
        account_id=account_id,
        username=username,
        password=password,
        proxy_url=proxy_url,
        headless=False,
    )

    print("Reddit Agent ready. Chat normally or ask it to perform Reddit actions.")
    print("Type 'quit' or press Ctrl+C to exit.\n")

    try:
        while True:
            try:
                user_input = input("You: ").strip()
            except EOFError:
                break

            if not user_input:
                continue
            if user_input.lower() in ("quit", "exit"):
                break

            try:
                reply = await agent.chat(user_input)
                print(f"\nAgent: {reply}\n")
            except Exception as e:
                print(f"\nAgent error: {e}\n")
                if os.getenv("DEBUG_TRACEBACK") == "1":
                    traceback.print_exc()
    except KeyboardInterrupt:
        pass
    finally:
        print("\nClosing browser session...")
        await agent.close()
        print("Bye.")


if __name__ == "__main__":
    asyncio.run(main())
