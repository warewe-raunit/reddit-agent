"""
tools/reddit_api_client.py — Backend-only async Reddit API client.

Provides lightweight HTTP access to Reddit's search and post-detail JSON
endpoints. Used by the staged opportunity-discovery pipeline so that the
agent does not need a live Playwright browser to enumerate candidate posts.

Reuses the same request shape that experiments/reddit_api_fetch_probe.py
validated against Reddit's logged-in JSON endpoints.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus

import httpx
import structlog

logger = structlog.get_logger(__name__)

ROOT = Path(__file__).resolve().parents[1]
SEARCH_ENDPOINT = "https://www.reddit.com/search.json"
MAX_LISTING_LIMIT = 100
REMOVED_OR_DELETED_VALUES = {"[removed]", "[deleted]", "removed", "deleted"}


def _active_session_file(account_id: str = "") -> Path:
    aid = account_id or os.getenv("REDDIT_USERNAME") or "account_1"
    if os.getenv("BROWSER_PROFILE_IS_ACTIVE"):
        category = (os.getenv("BROWSER_DEVICE_CATEGORY") or "desktop").strip().lower()
        if category and category != "desktop":
            aid = f"{aid}__{category}"
    return ROOT / "sessions" / f"{aid}.json"


def load_reddit_cookies(session_file: Optional[Path] = None) -> httpx.Cookies:
    cookies = httpx.Cookies()
    path = session_file or _active_session_file()
    if not path.exists():
        return cookies
    try:
        storage_state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return cookies
    for cookie in storage_state.get("cookies", []):
        domain = str(cookie.get("domain") or "")
        if "reddit.com" not in domain:
            continue
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if not name:
            continue
        cookies.set(name, value, domain=domain, path=str(cookie.get("path") or "/"))
    return cookies


def discover_session_files(
    explicit: Optional[list] = None,
    sessions_dir: Optional[Path] = None,
    max_sessions: int = 0,
) -> list[Path]:
    """Find Reddit session JSON files. Returns deterministic ordering by name."""
    if explicit:
        out: list[Path] = []
        for entry in explicit:
            p = Path(entry)
            if p.exists():
                out.append(p)
        return out[: max_sessions or len(out)]
    base = sessions_dir or (ROOT / "sessions")
    if not base.exists():
        return []
    files = sorted(p for p in base.glob("*.json") if p.is_file())
    out = []
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict) or not data.get("cookies"):
            continue
        # require at least one reddit cookie
        if any("reddit.com" in str(c.get("domain") or "") for c in data["cookies"]):
            out.append(f)
    if max_sessions and max_sessions > 0:
        out = out[:max_sessions]
    return out


def read_session_proxy(session_file: Path) -> Optional[str]:
    """Per-session proxy lookup: sidecar `<name>.proxy` text file, JSON `proxy_url`
    field, or env override `PROXY_URL_<stem>`. None means use default."""
    try:
        env_key = f"PROXY_URL_{session_file.stem.upper()}"
        if os.getenv(env_key):
            return os.getenv(env_key)
        sidecar = session_file.with_suffix(".proxy")
        if sidecar.exists():
            text = sidecar.read_text(encoding="utf-8").strip()
            if text:
                return text
        data = json.loads(session_file.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            url = str(data.get("proxy_url") or "").strip()
            if url:
                return url
    except (OSError, json.JSONDecodeError):
        pass
    return None


def parse_rate_headers(response: httpx.Response) -> dict:
    """Extract Reddit's rate-limit headers. All values may be missing."""
    h = response.headers
    def _num(name: str) -> Optional[float]:
        raw = h.get(name)
        if not raw:
            return None
        m = re.findall(r"\d+(?:\.\d+)?", raw)
        if not m:
            return None
        return float(m[0])
    return {
        "used": _num("x-ratelimit-used"),
        "remaining": _num("x-ratelimit-remaining"),
        "reset": _num("x-ratelimit-reset"),
        "status": response.status_code,
    }


def _user_agent() -> str:
    return (
        os.getenv("BROWSER_USER_AGENT")
        or os.getenv("USER_AGENT")
        or f"windows:redditagent-opportunity:v0.1 (by /u/{os.getenv('REDDIT_USERNAME') or 'unknown'})"
    )


def _headers(query: str, time_filter: str = "week") -> dict[str, str]:
    return {
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": (
            f"https://www.reddit.com/search/?q={quote_plus(query)}"
            f"&type=link&sort=new&t={quote_plus(time_filter)}"
        ),
        "User-Agent": _user_agent(),
    }


def _post_created_utc(data: dict[str, Any]) -> Optional[float]:
    try:
        value = float(data.get("created_utc") or data.get("created") or 0)
    except (TypeError, ValueError):
        return None
    return value or None


def _format_utc(epoch: Optional[float]) -> str:
    if not epoch:
        return ""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_post_url(data: dict[str, Any]) -> str:
    permalink = str(data.get("permalink") or "")
    if permalink.startswith("http"):
        return permalink
    if permalink.startswith("/"):
        return f"https://www.reddit.com{permalink}"
    return str(data.get("url") or "")


def _compact(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def inactive_reason(data: dict[str, Any]) -> str:
    removed_by = str(data.get("removed_by_category") or "").strip()
    if removed_by:
        return f"removed_by_category:{removed_by}"
    if data.get("banned_by") or data.get("banned_at_utc"):
        return "removed_or_banned"
    if _compact(data.get("author")) in REMOVED_OR_DELETED_VALUES:
        return "deleted_author"
    for field in ("title", "selftext", "body"):
        value = _compact(data.get(field))
        if value in REMOVED_OR_DELETED_VALUES:
            return f"{value.strip('[]')}_{field}"
    if data.get("spam"):
        return "spam"
    return ""


def _post_id(data: dict[str, Any]) -> str:
    pid = str(data.get("id") or "").strip()
    if pid:
        return pid
    match = re.search(r"/comments/([^/]+)/", str(data.get("permalink") or ""))
    return match.group(1) if match else ""


def normalize_listing_post(data: dict[str, Any], matched_query: str = "") -> dict[str, Any]:
    """Lightweight metadata only — no body, no comments."""
    created_utc = _post_created_utc(data)
    age_days = None
    if created_utc:
        age_days = max(0.0, (datetime.now(timezone.utc).timestamp() - created_utc) / 86400.0)
    return {
        "id": _post_id(data),
        "url": _canonical_post_url(data),
        "title": str(data.get("title") or "").strip(),
        "subreddit": str(data.get("subreddit") or "").strip(),
        "score": data.get("score"),
        "upvotes": data.get("ups", data.get("score")),
        "comment_count": data.get("num_comments"),
        "created_utc": created_utc,
        "created_date": _format_utc(created_utc),
        "age_days": age_days,
        "matched_query": matched_query,
        "listing_inactive_reason": inactive_reason(data),
        "type": "post",
        "source": "reddit_api_search",
    }


async def search_posts(
    client: httpx.AsyncClient,
    query: str,
    time_filter: str = "week",
    sort: str = "new",
    limit: int = 100,
    max_pages: int = 2,
    sleep_seconds: float = 0.7,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch lightweight post metadata for one search query.

    Returns (posts, stats). Posts contain only listing-level metadata.
    """
    limit = max(1, min(limit, MAX_LISTING_LIMIT))
    posts: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "query": query,
        "time_filter": time_filter,
        "sort": sort,
        "pages": 0,
        "children": 0,
        "stopped_reason": "completed",
    }
    after = ""
    count = 0
    for _ in range(max(1, max_pages)):
        params: dict[str, Any] = {
            "q": query,
            "type": "link",
            "sort": sort,
            "t": time_filter,
            "limit": limit,
            "raw_json": 1,
        }
        if after:
            params["after"] = after
        if count:
            params["count"] = count
        try:
            response = await client.get(
                SEARCH_ENDPOINT,
                params=params,
                headers=_headers(query, time_filter),
            )
        except httpx.HTTPError as exc:
            stats["stopped_reason"] = f"http_error:{exc.__class__.__name__}"
            logger.warning("reddit_api_search_http_error", query=query, error=str(exc))
            break
        stats["pages"] += 1
        if response.status_code == 429:
            stats["stopped_reason"] = "reddit_429_rate_limited"
            break
        if response.status_code in {403, 404, 410, 451}:
            stats["stopped_reason"] = f"http_{response.status_code}"
            break
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPStatusError, ValueError):
            stats["stopped_reason"] = f"bad_payload_{response.status_code}"
            break
        listing = payload.get("data", {}) if isinstance(payload, dict) else {}
        children = listing.get("children", []) if isinstance(listing, dict) else []
        stats["children"] += len(children)
        for child in children:
            if not isinstance(child, dict):
                continue
            data = child.get("data", {})
            if not isinstance(data, dict):
                continue
            post = normalize_listing_post(data, matched_query=query)
            if not post.get("title") or not post.get("url"):
                continue
            posts.append(post)
        after = str(listing.get("after") or "") if isinstance(listing, dict) else ""
        count += len(children)
        if not after:
            break
        if sleep_seconds:
            await asyncio.sleep(sleep_seconds)
    return posts, stats


def _detail_post_data(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, list) or not payload:
        return None
    first = payload[0] if isinstance(payload[0], dict) else {}
    children = first.get("data", {}).get("children", [])
    if not children:
        return None
    data = children[0].get("data", {})
    return data if isinstance(data, dict) else None


def _detail_comment_children(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    listing = payload[1] if isinstance(payload[1], dict) else {}
    children = listing.get("data", {}).get("children", [])
    return [child for child in children if isinstance(child, dict)]


def _normalize_comment(data: dict[str, Any], post_url: str, max_chars: int) -> dict[str, Any]:
    body = str(data.get("body") or "").strip()
    if max_chars > 0 and len(body) > max_chars:
        body = body[:max_chars].rstrip() + "..."
    permalink = str(data.get("permalink") or "")
    comment_url = f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink
    if not comment_url and data.get("id"):
        comment_url = f"{post_url.rstrip('/')}/{data.get('id')}/"
    return {
        "author": str(data.get("author") or "").strip(),
        "score": data.get("score"),
        "body": body,
        "url": comment_url,
    }


def _top_comments(payload: Any, post_url: str, limit: int, max_chars: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    out: list[dict[str, Any]] = []
    for child in _detail_comment_children(payload):
        if child.get("kind") != "t1":
            continue
        data = child.get("data", {}) or {}
        if inactive_reason(data):
            continue
        body = _compact(data.get("body"))
        if not body or body in REMOVED_OR_DELETED_VALUES:
            continue
        out.append(_normalize_comment(data, post_url, max_chars))
        if len(out) >= limit:
            break
    return out


async def fetch_post_detail(
    client: httpx.AsyncClient,
    post_id: str,
    post_url: str = "",
    matched_query: str = "",
    top_comments: int = 5,
    max_body_chars: int = 3000,
    max_comment_chars: int = 1500,
) -> dict[str, Any]:
    """Fetch full post body + top comments. Returns enriched dict or {} on failure."""
    pid = str(post_id or "").strip()
    if not pid:
        return {}
    endpoint = f"https://www.reddit.com/comments/{pid}.json"
    try:
        response = await client.get(
            endpoint,
            params={"raw_json": 1, "limit": max(0, top_comments), "sort": "top"},
            headers=_headers(matched_query or pid),
        )
    except httpx.HTTPError as exc:
        logger.warning("reddit_api_detail_http_error", post_id=pid, error=str(exc))
        return {"detail_status": "http_error", "detail_error": str(exc)}
    rate = parse_rate_headers(response)
    if response.status_code in {403, 404, 410, 451, 429}:
        return {"detail_status": f"http_{response.status_code}", "_rate": rate, "_status": response.status_code}
    try:
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPStatusError, ValueError):
        return {"detail_status": f"bad_payload_{response.status_code}", "_rate": rate, "_status": response.status_code}
    data = _detail_post_data(payload) or {}
    if not data:
        return {"detail_status": "missing_post_data", "_rate": rate, "_status": response.status_code}
    reason = inactive_reason(data)
    body = str(data.get("selftext") or "").strip()
    if max_body_chars > 0 and len(body) > max_body_chars:
        body = body[:max_body_chars].rstrip() + "..."
    return {
        "detail_status": "ok",
        "detail_inactive_reason": reason,
        "post_body": body,
        "score": data.get("score", data.get("ups")),
        "upvotes": data.get("ups", data.get("score")),
        "comment_count": data.get("num_comments"),
        "subreddit": str(data.get("subreddit") or "").strip(),
        "title": str(data.get("title") or "").strip(),
        "top_comments": _top_comments(payload, post_url, top_comments, max_comment_chars),
        "locked": bool(data.get("locked")),
        "archived": bool(data.get("archived")),
        "_rate": rate,
        "_status": response.status_code,
    }


def build_async_client(
    timeout: float = 30.0,
    use_proxy: bool = False,
    session_file: Optional[Path] = None,
    proxy_url: Optional[str] = None,
) -> httpx.AsyncClient:
    cookies = load_reddit_cookies(session_file)
    kwargs: dict[str, Any] = {
        "cookies": cookies,
        "follow_redirects": True,
        "timeout": timeout,
    }
    chosen_proxy = proxy_url
    if chosen_proxy is None and use_proxy:
        chosen_proxy = os.getenv("PROXY_URL")
    if chosen_proxy:
        kwargs["proxy"] = chosen_proxy
    return httpx.AsyncClient(**kwargs)
