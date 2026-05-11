"""
tools/reddit_session_pool.py — Multi-session Reddit detail-fetch pool.

Stage 3 of the opportunity pipeline can require hundreds of post-detail
requests. Reddit's logged-in JSON endpoint caps a single session at roughly
100 requests / 10 minutes per account. To stay safe and fast we shard work
across multiple saved Playwright sessions, each with its own cookies (and
optionally its own proxy), and cap each session with a live header-aware
rate limiter.

Public surface:
    SessionRateLimiter — sliding-window + Reddit-header-aware throttle
    SessionWorker      — one async client + one limiter
    SessionClientPool  — async-context-managed bundle of workers
    run_parallel_detail_fetch — orchestrator: queue, claim lock, early stop
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import httpx
import structlog

from tools.opportunity_discovery_tool import _reddit_url_key
from tools.reddit_api_client import (
    build_async_client,
    discover_session_files,
    fetch_post_detail,
    read_session_proxy,
)

logger = structlog.get_logger(__name__)


DEFAULT_REQUESTS_PER_WINDOW = 100
DEFAULT_RATE_WINDOW_SECONDS = 600.0
DEFAULT_PER_SESSION_CONCURRENCY = 1
DEFAULT_GLOBAL_DETAIL_CONCURRENCY = 8


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

@dataclass
class SessionRateLimiter:
    """Per-session throttle.

    Two layers:
      1. Sliding window of `requests_per_window` requests in `window_seconds`.
         Default 100 / 600s ≈ 1 request every 6s.
      2. Live Reddit response headers can pause the session further:
         `x-ratelimit-remaining` low → wait until reset.
         429 status → pause for full reset window.
    """

    name: str
    requests_per_window: int = DEFAULT_REQUESTS_PER_WINDOW
    window_seconds: float = DEFAULT_RATE_WINDOW_SECONDS
    min_interval: float = 0.0  # auto-derived if 0
    _times: deque = field(default_factory=deque)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _paused_until: float = 0.0

    def __post_init__(self) -> None:
        try:
            self.requests_per_window = max(1, int(self.requests_per_window or DEFAULT_REQUESTS_PER_WINDOW))
        except (TypeError, ValueError):
            self.requests_per_window = DEFAULT_REQUESTS_PER_WINDOW
        try:
            self.window_seconds = max(0.001, float(self.window_seconds or DEFAULT_RATE_WINDOW_SECONDS))
        except (TypeError, ValueError):
            self.window_seconds = DEFAULT_RATE_WINDOW_SECONDS
        if self.min_interval <= 0:
            self.min_interval = self.window_seconds / max(1, self.requests_per_window)

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                wait = self._compute_wait(now)
                if wait <= 0:
                    self._times.append(now)
                    return
            await asyncio.sleep(wait)

    async def wait_until_ready(self) -> None:
        """Wait until this session can make a request without consuming a slot.

        Workers use this before claiming a URL, so a paused/rate-limited session
        does not hold the next priority item while other sessions are available.
        """
        while True:
            async with self._lock:
                wait = self._compute_wait(time.monotonic())
                if wait <= 0:
                    return
            await asyncio.sleep(wait)

    def _compute_wait(self, now: float) -> float:
        if now < self._paused_until:
            return self._paused_until - now
        cutoff = now - self.window_seconds
        while self._times and self._times[0] < cutoff:
            self._times.popleft()
        if len(self._times) >= self.requests_per_window:
            return max(0.0, self._times[0] + self.window_seconds - now)
        if self._times and self.min_interval > 0:
            since_last = now - self._times[-1]
            if since_last < self.min_interval:
                return self.min_interval - since_last
        return 0.0

    def update_from_headers(self, rate: Optional[dict]) -> None:
        if not rate:
            return
        remaining = rate.get("remaining")
        reset = rate.get("reset")
        status = rate.get("status")

        def _seconds(value: object, default: float = 0.0) -> float:
            try:
                seconds = float(value)
            except (TypeError, ValueError):
                seconds = default
            return max(0.0, seconds)

        if status == 429:
            reset_seconds = _seconds(reset, self.window_seconds)
            self.pause_until(time.monotonic() + reset_seconds)
            logger.warning(
                "reddit_session_pool_rate_limited",
                session=self.name,
                reset_seconds=reset_seconds,
            )
            return
        try:
            remaining_count = float(remaining) if remaining is not None else None
        except (TypeError, ValueError):
            remaining_count = None
        reset_seconds = _seconds(reset)
        if remaining_count is not None and reset_seconds > 0 and remaining_count <= 1:
            self.pause_until(time.monotonic() + reset_seconds)
            logger.info(
                "reddit_session_pool_remaining_low",
                session=self.name,
                remaining=remaining_count,
                reset_seconds=reset_seconds,
            )

    def pause_until(self, mono_ts: float) -> None:
        self._paused_until = max(self._paused_until, mono_ts)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

@dataclass
class SessionWorker:
    name: str
    session_file: Path
    client: httpx.AsyncClient
    limiter: SessionRateLimiter
    proxy_url: Optional[str] = None
    success_count: int = 0
    failure_count: int = 0
    rate_pauses: int = 0

    async def fetch(
        self,
        post: dict,
        top_comments: int,
        max_body_chars: int = 3000,
        max_comment_chars: int = 1500,
    ) -> dict:
        await self.limiter.acquire()
        detail = await fetch_post_detail(
            self.client,
            post_id=post.get("id", ""),
            post_url=post.get("url", ""),
            matched_query=post.get("matched_query", ""),
            top_comments=top_comments,
            max_body_chars=max_body_chars,
            max_comment_chars=max_comment_chars,
        )
        rate = detail.get("_rate") if isinstance(detail, dict) else None
        status = detail.get("_status") if isinstance(detail, dict) else None
        if rate:
            self.limiter.update_from_headers(rate)
        if status == 429:
            self.rate_pauses += 1
        if isinstance(detail, dict) and detail.get("detail_status") == "ok":
            self.success_count += 1
        else:
            self.failure_count += 1
        return detail


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

class SessionClientPool:
    """Async-context bundle of SessionWorkers, one per session file."""

    def __init__(
        self,
        session_files: list[Path],
        requests_per_window: int = DEFAULT_REQUESTS_PER_WINDOW,
        rate_window_seconds: float = DEFAULT_RATE_WINDOW_SECONDS,
        timeout: float = 30.0,
        use_proxy: bool = False,
        proxy_resolver: Optional[Callable[[Path], Optional[str]]] = None,
    ) -> None:
        self.session_files = list(session_files)
        self.requests_per_window = requests_per_window
        self.rate_window_seconds = rate_window_seconds
        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_resolver = proxy_resolver or read_session_proxy
        self.workers: list[SessionWorker] = []

    async def __aenter__(self) -> "SessionClientPool":
        for path in self.session_files:
            proxy = self.proxy_resolver(path)
            if not proxy and self.use_proxy:
                proxy = os.getenv("PROXY_URL")
            client = build_async_client(
                timeout=self.timeout,
                session_file=path,
                proxy_url=proxy,
            )
            limiter = SessionRateLimiter(
                name=path.stem,
                requests_per_window=self.requests_per_window,
                window_seconds=self.rate_window_seconds,
            )
            worker = SessionWorker(
                name=path.stem,
                session_file=path,
                client=client,
                limiter=limiter,
                proxy_url=proxy,
            )
            self.workers.append(worker)
            logger.info(
                "reddit_session_pool_worker_loaded",
                session=worker.name,
                proxy=bool(proxy),
            )
        logger.info("reddit_session_pool_loaded", session_count=len(self.workers))
        return self

    async def __aexit__(self, *exc: object) -> None:
        for worker in self.workers:
            try:
                await worker.client.aclose()
            except Exception:
                pass
        self.workers = []

    @property
    def session_count(self) -> int:
        return len(self.workers)


# ---------------------------------------------------------------------------
# Parallel detail dispatcher
# ---------------------------------------------------------------------------

ProcessFn = Callable[[int, dict, dict], Awaitable[bool]]
"""Process callback. Receives (priority_index, post, detail_or_error_dict).
Returns True if processing should continue, False to stop dispatcher."""


async def run_parallel_detail_fetch(
    survivors: list[dict],
    pool: SessionClientPool,
    top_comments: int,
    process_in_order: ProcessFn,
    detail_limit: int,
    global_concurrency: int = DEFAULT_GLOBAL_DETAIL_CONCURRENCY,
    per_session_concurrency: int = DEFAULT_PER_SESSION_CONCURRENCY,
) -> dict:
    """Drive Stage 3 detail fetches across the pool.

    Workers fetch in arbitrary completion order, but `process_in_order` is
    called sequentially in the priority order of `survivors` so the LLM
    evaluation and the final output stay deterministic.

    Returns a stats dict.
    """
    n = min(len(survivors), max(0, detail_limit))
    stats = {
        "session_count": pool.session_count,
        "duplicate_claims_prevented": 0,
        "stop_reason": "",
        "attempted": 0,
        "fetched": 0,
        "failed": 0,
        "approved_at_stop": 0,
    }
    if n == 0 or not pool.workers:
        stats["stop_reason"] = "no_work"
        return stats

    results: dict[int, dict] = {}
    events: dict[int, asyncio.Event] = {i: asyncio.Event() for i in range(n)}
    claim_lock = asyncio.Lock()
    next_index = 0
    claimed: set[int] = set()
    stop_event = asyncio.Event()
    global_sem = asyncio.Semaphore(max(1, global_concurrency))

    stats["network_attempted"] = 0
    stats["network_fetched"] = 0
    stats["network_failed"] = 0
    claimed_keys: set[str] = set()

    def _claim_key(post: dict, idx: int) -> str:
        key = _reddit_url_key(str(post.get("url") or ""))
        if key:
            return f"url:{key}"
        pid = str(post.get("id") or "").strip()
        if pid:
            return f"id:{pid}"
        return f"idx:{idx}"

    async def _claim() -> Optional[int]:
        nonlocal next_index
        async with claim_lock:
            if stop_event.is_set():
                return None
            while next_index < n:
                idx = next_index
                next_index += 1
                if idx in claimed:
                    stats["duplicate_claims_prevented"] += 1
                    events[idx].set()
                    continue
                key = _claim_key(survivors[idx], idx)
                if key in claimed_keys:
                    stats["duplicate_claims_prevented"] += 1
                    events[idx].set()
                    continue
                claimed.add(idx)
                claimed_keys.add(key)
                return idx
            return None

    async def _worker_loop(worker: SessionWorker) -> None:
        per_sess_sem = asyncio.Semaphore(max(1, per_session_concurrency))
        in_flight: list[asyncio.Task] = []
        try:
            while not stop_event.is_set():
                await worker.limiter.wait_until_ready()
                if stop_event.is_set():
                    break
                idx = await _claim()
                if idx is None:
                    break
                async def _do(i: int = idx) -> None:
                    async with global_sem, per_sess_sem:
                        if stop_event.is_set():
                            events[i].set()
                            return
                        post = survivors[i]
                        logger.debug(
                            "reddit_session_pool_claim",
                            session=worker.name,
                            idx=i,
                            url=post.get("url"),
                        )
                        try:
                            detail = await worker.fetch(post, top_comments)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            detail = {"detail_status": "worker_exception", "detail_error": str(exc)}
                        stats["network_attempted"] += 1
                        if isinstance(detail, dict) and detail.get("detail_status") == "ok":
                            stats["network_fetched"] += 1
                        else:
                            stats["network_failed"] += 1
                        results[i] = detail
                        events[i].set()
                task = asyncio.create_task(_do())
                in_flight.append(task)
                # cap per-worker fan-out to per_session_concurrency
                if len(in_flight) >= per_session_concurrency:
                    done, pending = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                    in_flight = list(pending)
        finally:
            if in_flight:
                if stop_event.is_set():
                    for task in in_flight:
                        if not task.done():
                            task.cancel()
                await asyncio.gather(*in_flight, return_exceptions=True)
            logger.info(
                "reddit_session_pool_worker_done",
                session=worker.name,
                success=worker.success_count,
                failure=worker.failure_count,
                rate_pauses=worker.rate_pauses,
            )

    worker_tasks = [asyncio.create_task(_worker_loop(w)) for w in pool.workers]

    async def _consume_in_order() -> None:
        for i in range(n):
            await events[i].wait()
            if i not in results:
                continue
            stats["attempted"] += 1
            detail = results[i]
            if isinstance(detail, dict) and detail.get("detail_status") == "ok":
                stats["fetched"] += 1
            else:
                stats["failed"] += 1
            keep_going = await process_in_order(i, survivors[i], detail)
            if not keep_going:
                stats["stop_reason"] = "target_reached"
                stop_event.set()
                # release any waiting events so workers can exit
                for j in range(i + 1, n):
                    events[j].set()
                return
        stats["stop_reason"] = stats["stop_reason"] or "exhausted_survivors"

    try:
        await _consume_in_order()
    finally:
        stop_event.set()
        for task in worker_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    return stats


def load_session_pool_files(
    session_files: Optional[list] = None,
    max_sessions: int = 0,
) -> list[Path]:
    return discover_session_files(explicit=session_files, max_sessions=max_sessions)
