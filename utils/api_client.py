import aiohttp
from core.config import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp

# Retry on network errors and timeouts, up to 5 attempts, exponential backoff
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    retry=retry_if_exception_type((aiohttp.ClientError, aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError))
)
async def api_get(path: str, api_base_url: str , params: dict = None):
    # Use provided api_base_url or fallback to settings
    url = f"{(api_base_url).rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {settings.API_KEY}",
        "Accept": "application/vnd.api+json"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()