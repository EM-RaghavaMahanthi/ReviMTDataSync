import aiohttp
from core.config import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp
import csv
import os
import datetime

# Retry on network errors and timeouts, up to 5 attempts, exponential backoff
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    retry=retry_if_exception_type((aiohttp.ClientError, aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError))
)
async def api_get(path: str, api_base_url, params: dict = None):
    try:
        url = f"{api_base_url.rstrip('/')}/{path.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {settings.API_KEY}",
            "Accept": "application/vnd.api+json"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=30) as resp:
                resp.raise_for_status()
                return await resp.json()
    except Exception as e:
        # Log failed request to DLQ CSV
        dlq_file = "data/dlq.csv"
        os.makedirs("data", exist_ok=True)
        with open(dlq_file, "a", newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:  # Write header if file is empty
                writer.writerow(["timestamp", "path", "api_base_url", "params", "error"])
            writer.writerow([datetime.datetime.now().isoformat(), path, api_base_url, str(params), str(e)])
        
        # Re-raise the exception so retry logic still works
        raise e
