"""Small confirmation helpers shared by agent runtime and tests."""

from __future__ import annotations

import re
from typing import Optional


def confirmation_reply(user_message: str) -> Optional[bool]:
    """Return True/False for explicit yes/no confirmation replies, else None."""
    compact = re.sub(r"[^a-z0-9\s]", "", user_message.strip().lower())
    compact = re.sub(r"\s+", " ", compact).strip()
    yes_replies = {
        "yes",
        "y",
        "confirm",
        "confirmed",
        "proceed",
        "go ahead",
        "do it",
        "submit it",
        "post it",
        "looks good",
    }
    no_replies = {
        "no",
        "n",
        "cancel",
        "stop",
        "abort",
        "do not",
        "dont",
        "do not proceed",
        "dont proceed",
    }
    if compact in yes_replies:
        return True
    if compact in no_replies:
        return False
    return None
