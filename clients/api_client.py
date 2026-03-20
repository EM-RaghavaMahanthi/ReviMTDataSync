import aiohttp
import asyncio
import logging
from typing import Dict, Any, Optional
from urllib.parse import urlencode
from tenacity import retry, stop_after_attempt, wait_exponential


from core.stg_db_config import settings


logger = logging.getLogger(__name__)


class AnalyticsAPIClient:
    """
    HTTP client for calling analytics endpoints
    Handles API requests to your backend server with proper error handling and retries
    """
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        timeout_total = getattr(settings, 'CONNECTION_TIMEOUT', 300)  # 5 min default
        timeout_read = getattr(settings, 'READ_TIMEOUT', 120)  # 2 min default
        
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=timeout_total,
                sock_read=timeout_read
            ),
            connector=aiohttp.TCPConnector(limit=10)  # Connection pool
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._session:
            await self._session.close()
    
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def call_endpoint(self, endpoint: str, account_id: str, access_token: str) -> dict:
        """
        Call analytics endpoint with account_id parameter
        
        Args:
            endpoint: Analytics endpoint (e.g., 'post-process' or 'analytics/sync')
            account_id: Account ID to send as parameter
            access_token: Auth0 bearer token
        
        Returns:
            JSON response from your backend API
        """
        if not self._session:
            timeout_total = getattr(settings, 'CONNECTION_TIMEOUT', 300)
            timeout_read = getattr(settings, 'READ_TIMEOUT', 120)
            
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=timeout_total,
                    sock_read=timeout_read
                )
            )
        
        # Build query parameters with just account_id
        params = {
            'accountId': account_id
        }
        
        # Build full URL
        base_url = getattr(settings, 'ENV_API_BASE_URL', 'https://api.example.com').rstrip('/')
        full_url = f"{base_url}/{endpoint}?{urlencode(params)}"
        
        # Headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'ReviSync-Lambda-Pipeline/1.0'
        }
        
        logger.info(f"📤 Calling API: {endpoint} - Account: {account_id}")
        logger.debug(f"📍 Full URL: {full_url}")
        
        async with self._session.get(full_url, headers=headers) as response:
            logger.debug(f"📥 API Response: {response.status} for {endpoint}")
            
            if response.status == 200:
                data = await response.json()
                logger.info(f"✅ API success: {endpoint} - {account_id} - Response size: {len(str(data))}")
                return data
            
            elif response.status == 504:
                logger.info(f"✅ API success (504 expected): {endpoint} - {account_id} - Gateway timeout is expected behavior")
                return {"status": "success", "message": "Request processed (504 timeout expected)"}
            
            elif response.status == 401:
                error_text = await response.text()
                raise Exception(f"🔐 Authentication failed for {endpoint}: {error_text}")
            
            elif response.status == 403:
                error_text = await response.text()
                raise Exception(f"🚫 Access forbidden for {endpoint} - Account {account_id}: {error_text}")
            
            elif response.status == 404:
                raise Exception(f"🔍 Endpoint not found: {endpoint}")
            
            elif response.status == 429:
                retry_after = response.headers.get('Retry-After', '60')
                raise Exception(f"⏳ Rate limited for {endpoint}. Retry after {retry_after}s")
            
            elif response.status >= 500:
                error_text = await response.text()
                raise Exception(f"🔥 Server error for {endpoint}: {response.status} - {error_text}")
            
            else:
                error_text = await response.text()
                raise Exception(f"❌ API error for {endpoint}: {response.status} - {error_text}")
    
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def post_endpoint(
        self, 
        endpoint: str, 
        account_id: str, 
        access_token: str
    ) -> dict:
        """
        POST to analytics endpoint with account_id in URL path
        
        Args:
            endpoint: Analytics endpoint (e.g., 'eztexting/sync-with-eztexting')
            account_id: Account ID to append to URL path
            access_token: Auth0 bearer token
        
        Returns:
            JSON response from your backend API
        """
        if not self._session:
            timeout_total = getattr(settings, 'CONNECTION_TIMEOUT', 300)
            timeout_read = getattr(settings, 'READ_TIMEOUT', 120)
            
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=timeout_total,
                    sock_read=timeout_read
                )
            )
        
        # Build full URL with account_id in path
        base_url = getattr(settings, 'ENV_API_BASE_URL', 'https://api.example.com').rstrip('/')
        full_url = f"{base_url}/{endpoint}/{account_id}"
        
        # Headers
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'User-Agent': 'ReviSync-Lambda-Pipeline/1.0'
        }
        
        logger.info(f"📤 POST to API: {endpoint} - Account: {account_id}")
        logger.debug(f"📍 Full URL: {full_url}")
        
        # Empty payload
        async with self._session.post(full_url, data='', headers=headers) as response:
            logger.debug(f"📥 API Response: {response.status} for {endpoint}")
            
            if response.status == 200:
                data = await response.json()
                logger.info(f"✅ POST success: {endpoint} - {account_id} - Response size: {len(str(data))}")
                return data
            
            elif response.status == 504:
                logger.info(f"✅ POST success (504 expected): {endpoint} - {account_id} - Gateway timeout is expected behavior")
                return {"status": "success", "message": "Request processed (504 timeout expected)"}
            
            elif response.status == 401:
                error_text = await response.text()
                raise Exception(f"🔐 Authentication failed for {endpoint}: {error_text}")
            
            elif response.status == 403:
                error_text = await response.text()
                raise Exception(f"🚫 Access forbidden for {endpoint} - Account {account_id}: {error_text}")
            
            elif response.status == 404:
                raise Exception(f"🔍 Endpoint not found: {endpoint}")
            
            elif response.status == 429:
                retry_after = response.headers.get('Retry-After', '60')
                raise Exception(f"⏳ Rate limited for {endpoint}. Retry after {retry_after}s")
            
            elif response.status >= 500:
                error_text = await response.text()
                raise Exception(f"🔥 Server error for {endpoint}: {response.status} - {error_text}")
            
            else:
                error_text = await response.text()
                raise Exception(f"❌ POST error for {endpoint}: {response.status} - {error_text}")
    
    async def close(self):
        """Close the session"""
        if self._session:
            await self._session.close()
