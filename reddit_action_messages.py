"""User-facing status messages for Reddit actions."""

from __future__ import annotations

from typing import Any


def _score_changed(before: Any, after: Any) -> bool:
    if before is None or after is None:
        return False
    return str(before) != str(after)


def post_upvote_result_message(data: dict) -> str:
    """Return a clear status line for post upvote verification results."""
    if data.get("already_upvoted"):
        return f"Post was already upvoted. Data: {data}"

    source = data.get("verification_source", "unknown")
    before = data.get("score_before")
    after = data.get("score_after_reload")
    score_changed = _score_changed(before, after)
    ui_verified = bool(data.get("ui_verified_before_reload") or data.get("ui_verified_after_reload"))
    server_verified = bool(data.get("server_verified"))

    if server_verified and not ui_verified:
        visible_note = (
            f"Visible score stayed {after} after reload."
            if before is not None and after is not None and not score_changed
            else "Visible UI did not confirm the vote after reload."
        )
        return (
            "Post upvote recorded on Reddit server, but the visible page did not update after reload. "
            f"{visible_note} Verification source: {source}. Data: {data}"
        )

    if score_changed:
        return f"Post upvote successful. Visible score changed from {before} to {after}. Data: {data}"

    return f"Post upvote successful. Verification source: {source}. Data: {data}"
