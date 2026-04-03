import aiohttp
from core.config import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def _should_retry(exc: BaseException) -> bool:
    """Retry on network/server errors but not 4xx client errors (e.g. 404)."""
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status >= 500 or exc.status == 429
    return isinstance(exc, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError))


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    retry=retry_if_exception(_should_retry),
)
async def api_get(path: str, api_base_url: str, params: dict = None):
    url = f"{(api_base_url).rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {settings.API_KEY}",
        "Accept": "application/vnd.api+json"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()