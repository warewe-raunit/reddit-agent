"""Runtime guardrails for LangGraph ReAct execution."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Optional


DEFAULT_REACT_RECURSION_LIMIT = 32
MIN_REACT_RECURSION_LIMIT = 12
MAX_REACT_RECURSION_LIMIT = 80

DEFAULT_REACT_TIMEOUT_SECONDS = 180.0
MIN_REACT_TIMEOUT_SECONDS = 15.0
MAX_REACT_TIMEOUT_SECONDS = 600.0


def _env(env: Optional[Mapping[str, str]] = None) -> Mapping[str, str]:
    return env if env is not None else os.environ


def _clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def _clamp_float(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def react_recursion_limit(env: Optional[Mapping[str, str]] = None) -> int:
    """Return a bounded LangGraph recursion limit for one ReAct turn."""
    raw = _env(env).get("REACT_RECURSION_LIMIT", "").strip()
    if not raw:
        return DEFAULT_REACT_RECURSION_LIMIT
    try:
        return _clamp_int(int(raw), MIN_REACT_RECURSION_LIMIT, MAX_REACT_RECURSION_LIMIT)
    except ValueError:
        return DEFAULT_REACT_RECURSION_LIMIT


def react_timeout_seconds(env: Optional[Mapping[str, str]] = None) -> float:
    """Return a bounded wall-clock timeout for one ReAct turn."""
    raw = _env(env).get("REACT_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return DEFAULT_REACT_TIMEOUT_SECONDS
    try:
        return _clamp_float(float(raw), MIN_REACT_TIMEOUT_SECONDS, MAX_REACT_TIMEOUT_SECONDS)
    except ValueError:
        return DEFAULT_REACT_TIMEOUT_SECONDS


def react_runtime_config(env: Optional[Mapping[str, str]] = None) -> dict:
    """LangGraph config passed to graph.ainvoke for hard step limiting."""
    return {"recursion_limit": react_recursion_limit(env)}


def is_react_loop_error(exc: Exception) -> bool:
    """Detect LangGraph recursion-limit failures without importing LangGraph in tests."""
    cls_name = exc.__class__.__name__
    text = str(exc).lower()
    return cls_name == "GraphRecursionError" or (
        "recursion limit" in text and ("stop condition" in text or "langgraph" in text)
    )


def react_loop_stop_message(exc: Exception, pending_action: Optional[dict] = None) -> str:
    """User-facing message when a ReAct turn is stopped by the loop guard."""
    action_text = ""
    if pending_action:
        action = pending_action.get("action", "unknown action")
        details = pending_action.get("details", "")
        action_text = (
            "\n\nA confirmed action was still pending, so I reset that approval. "
            f"Pending action: {action}. {details}".rstrip()
        )
    return (
        "I stopped the agent because it hit the ReAct step limit before reaching a final answer. "
        "I did not keep retrying the same tool loop. Please give me the exact Reddit URL/action "
        "or tell me to cancel and I will take the shorter direct path."
        f"{action_text}\n\nLoop guard detail: {exc}"
    )


def react_timeout_message(timeout_seconds: float, pending_action: Optional[dict] = None) -> str:
    """User-facing message when a ReAct turn exceeds the wall-clock limit."""
    action_text = ""
    if pending_action:
        action = pending_action.get("action", "unknown action")
        details = pending_action.get("details", "")
        action_text = (
            "\n\nA confirmed action was still pending, so I reset that approval. "
            f"Pending action: {action}. {details}".rstrip()
        )
    return (
        "I stopped the agent because this ReAct turn took too long "
        f"({timeout_seconds:.0f}s timeout). I did not leave it running in the background. "
        "Please give me the exact Reddit URL/action or tell me to cancel and I will use the direct path."
        f"{action_text}"
    )
