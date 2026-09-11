import asyncio
import logging
import os
import time
from urllib.parse import urlencode

import aiohttp
from core.config import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

logger = logging.getLogger(__name__)

# ─── Async token bucket (per-tenant rate limiter) ─────────────────────────────
# MarianaTek enforces 200 req/min per tenant. We cap at 50% headroom (100 req/min)
# because the bucket is in-process; multiple Lambda containers cannot coordinate.
# With MaxConcurrency=1 in Step Functions, a single container handles all shards
# sequentially, making this bucket fully effective for the entire pipeline run.

_MAX_REQUESTS_PER_MIN: int = int(os.environ.get("CRM_MAX_REQUESTS_PER_MIN", "100"))
_RATE_WINDOW_SEC: float = 60.0
# Initial burst allowance. Small on purpose: a large capacity lets the first N requests
# fire instantly, and with sequential shards each starting a fresh full bucket those
# bursts stack up and trip MarianaTek's server-side limit. Defaults to 10% of the rate.
# A full minute's budget, matching revi-dlk-bronze. This was _MAX_REQUESTS_PER_MIN // 10,
# which throttled far harder than intended: absorbing a burst and then settling to the rate
# is the entire point of a token bucket, and a tenth of the budget meant a cold container
# spaced its first requests 1/rate apart instead of letting them go. Measured at 100
# requests, capacity 10 took 57s where capacity 100 takes 22s — the burst is most of the
# difference on short jobs, and costs nothing on long ones because it drains once.
#
# It does NOT let us exceed the budget: capacity bounds only the idle refill, and acquire()
# below still spaces every request past the burst at exactly 1/rate.
_BURST_CAPACITY: int = int(os.environ.get("CRM_BURST_CAPACITY", str(_MAX_REQUESTS_PER_MIN)))

_buckets: dict = {}


class _AsyncTokenBucket:
    """
    Per-tenant token bucket, refilling continuously at `rate` tokens/sec.

    acquire() ALWAYS consumes a token, letting the count go negative — this is what
    makes it correct under asyncio.gather: concurrent waiters each see a lower token
    count and compute a progressively longer wait, so they fire spaced 1/rate apart
    instead of all at once. `capacity` bounds only the idle-refill burst.

    Deliberately different from revi-dlk-bronze's version, which clamps the count at 0
    and hands every concurrent waiter the same short wait — they then fire together.
    Measured over 1900 requests that reaches ~278 req/min against MarianaTek's 200/min
    hard ceiling; this one holds ~105 against a 100 budget. Bronze looks faster partly
    because it is overshooting, so do not "fix" this to match it.
    """

    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                self._capacity,
                self._tokens + (now - self._last) * self._rate,
            )
            self._last = now
            self._tokens -= 1.0  # consume unconditionally; may go negative so that
            #                      concurrent waiters queue instead of bursting together
            wait = 0.0 if self._tokens >= 0 else (-self._tokens) / self._rate
        if wait > 0:
            await asyncio.sleep(wait)


async def _throttle(api_base_url: str) -> None:
    if api_base_url not in _buckets:
        rate = _MAX_REQUESTS_PER_MIN / _RATE_WINDOW_SEC
        _buckets[api_base_url] = _AsyncTokenBucket(rate=rate, capacity=_BURST_CAPACITY)
    await _buckets[api_base_url].acquire()


# ─── Session ──────────────────────────────────────────────────────────────────
# Module-level session reused across all page fetches within a Lambda invocation.
# Avoids opening a new TCP connection per request when many pages are fetched
# concurrently via asyncio.gather. The handler resets `_session = None` at the
# start of each invocation — the session binds to the event loop of the
# asyncio.run() that created it, which is closed on the next (warm-start) call.
_session = None


async def _get_session():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def close_session() -> None:
    """
    Close the shared session in the CURRENT event loop. Call once at the end of each
    Lambda invocation (inside asyncio.run) — closing here, rather than orphaning the
    session for the next invocation to GC, avoids the "Unclosed client session" warning
    and leaves no sockets dangling on the closed loop.
    """
    global _session
    if _session is not None and not _session.closed:
        try:
            await _session.close()
        except Exception:
            pass
    _session = None


# ─── Fetch ────────────────────────────────────────────────────────────────────

def _should_retry(exc: BaseException) -> bool:
    """
    Retry on server errors, rate limits and timeouts, but not other 4xx (e.g. 404).

    asyncio.TimeoutError, not aiohttp.ServerTimeoutError, is what the ClientTimeout(total=...)
    below actually raises — aiohttp ends at `raise asyncio.TimeoutError` in helpers.py. And
    ServerTimeoutError SUBCLASSES asyncio.TimeoutError, not the other way round, so a predicate
    naming only ServerTimeoutError returns False for every total-timeout and tenacity never
    retries the one failure the retry policy exists for.

    That is not hypothetical: it took out 10 of 82 accounts on the 2026-09-11 notes/tags
    backfill. Each died on a single slow /users?filter[id] response while the accounts that
    succeeded fetched 4840 ids in 7 requests in 8 seconds — transient server slowness, retried
    away by every other path in this codebase, fatal only here because id_sync has no DLQ.

    asyncio.TimeoutError covers ServerTimeoutError by inheritance, so it is the only timeout
    class worth naming.
    """
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status >= 500 or exc.status == 429
    return isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError))


_DEFAULT_RETRY_AFTER = 10  # seconds to wait on 429 when Retry-After header is absent
_MAX_RETRY_AFTER = 30      # cap the honored Retry-After so one 429 can't stall a shard
_REQUEST_TIMEOUT = 30      # per-request HTTP timeout (a page fetch should be fast)


# Retry budget is bounded so a single bad call can't eat a shard's time budget:
#   up to 3 tries, each request <= 30s, backoff waits <= 10s (2s, 4s).
#   Worst case ~ 3*30 + 2*10 = ~110s (only if a call keeps failing); normal case ~0s.
# The token bucket keeps us under the rate limit, so 429s should be rare; when one
# slips through we honor Retry-After (capped) instead of tenacity's generic backoff.
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception(_should_retry),
)
async def api_get(path: str, api_base_url: str, params=None):
    """
    GET {api_base_url}/{path}?{params} with per-tenant rate limiting and retry.

    params may be a dict (regular endpoints) or a list of (key, value) tuples
    (user_batch endpoints where the same key repeats, e.g. &user=1&user=2).
    """
    await _throttle(api_base_url)

    url = f"{api_base_url.rstrip('/')}/{path.lstrip('/')}"
    # Log the exact request line (query string included) — the bearer token lives in the
    # header, not the URL, so this is safe. Shows the repeated &user= params for debugging.
    qs = urlencode(params, doseq=True) if params else ""
    logger.info(f"[api_get] GET {url}{('?' + qs) if qs else ''}")

    headers = {
        "Authorization": f"Bearer {settings.API_KEY}",
        "Accept": "application/vnd.api+json",
    }
    session = await _get_session()
    async with session.get(
        url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT)
    ) as resp:
        if resp.status == 429:
            retry_after = min(int(resp.headers.get("Retry-After", _DEFAULT_RETRY_AFTER)), _MAX_RETRY_AFTER)
            logger.warning(f"[api_get] 429 (token bucket should prevent this) → sleeping {retry_after}s")
            await asyncio.sleep(retry_after)
        resp.raise_for_status()
        data = await resp.json()
        n = len(data.get("data", [])) if isinstance(data, dict) and isinstance(data.get("data"), list) else "?"
        # `or {}` rather than a .get default: the default only applies when the key is
        # ABSENT, and MarianaTek sends an explicit "meta": null on responses that do not
        # paginate — filter[id] queries among them. .get("meta", {}) returns None there and
        # the next .get() raises. This is only a log line; it must never fail the request.
        meta = (data.get("meta") or {}) if isinstance(data, dict) else {}
        total = (meta.get("pagination") or {}).get("count")
        logger.info(f"[api_get] → {resp.status}, {n} records on page (meta.count={total})")
        return data
