import aiohttp
import json
from typing import Optional
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
from core.stg_db_config import settings
from core.logger import setup_logging
import logging

logger = logging.getLogger(__name__)

class Auth0Client:
    """
    Simplified Auth0 client for analytics API access
    Only handles primary Auth0 token - no account tokens
    """
    
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._session:
            await self._session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_access_token(self) -> str:
        """
        Get valid access token for analytics API
        Environment-aware: uses env var for dev, Auth0 for prod
        """
        now = datetime.now()
        
        # Check if cached token is still valid
        if (self._token and 
            self._token_expiry and 
            self._token_expiry > now):
            logger.debug("Using cached Auth0 token")
            return self._token
        
        # Fetch new token based on environment
        if hasattr(settings, 'ENVIRONMENT') and settings.ENVIRONMENT == 'dev':
            token = await self._get_dev_token()
        else:
            token = await self._get_prod_token()
        
        # Cache the token
        self._token = token
        
        # Set expiry based on environment
        if hasattr(settings, 'ENVIRONMENT') and settings.ENVIRONMENT == 'dev':
            # Dev tokens don't expire (from env var)
            self._token_expiry = now + timedelta(days=1)
        else:
            # Prod tokens from Auth0 typically expire in 1 hour
            self._token_expiry = now + timedelta(hours=1)
        
        logger.info(f"Auth0 token obtained for production environment")
        return self._token
    
    async def _get_dev_token(self) -> str:
        """Get development token from environment variable"""
        if not hasattr(settings, 'DEV_AUTH_TOKEN') or not settings.DEV_AUTH_TOKEN:
            raise ValueError("DEV_AUTH_TOKEN environment variable not set for development")
        
        logger.info("Using dev Auth0 token from environment variable")
        return settings.DEV_AUTH_TOKEN
    
    async def _get_prod_token(self) -> str:
        """Get production token from Auth0"""
        if not hasattr(settings, 'AUTH0_CLIENT_ID') or not settings.AUTH0_CLIENT_ID:
            raise ValueError("AUTH0_CLIENT_ID must be set for production")
        if not hasattr(settings, 'AUTH0_CLIENT_SECRET') or not settings.AUTH0_CLIENT_SECRET:
            raise ValueError("AUTH0_CLIENT_SECRET must be set for production")
        
        logger.info("Fetching prod Auth0 token from Auth0")
        
        if not self._session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        
        payload = {
            "client_id": settings.AUTH0_CLIENT_ID,
            "client_secret": settings.AUTH0_CLIENT_SECRET,
            "audience": settings.AUTH0_AUDIENCE,
            "grant_type": "client_credentials"
        }
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        try:
            async with self._session.post(
                settings.AUTH0_TOKEN_URL,
                json=payload,
                headers=headers
            ) as response:
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Auth0 token request failed: {response.status} - {error_text}")
                    raise Exception(f"Auth0 token request failed: {response.status}")
                
                data = await response.json()
                access_token = data.get('access_token')
                
                if not access_token:
                    raise ValueError("No access_token in Auth0 response")
                
                logger.info("Successfully obtained Auth0 access token")
                return access_token
                
        except aiohttp.ClientError as e:
            logger.error(f"Auth0 request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch Auth0 token: {str(e)}")
            raise
    
    def clear_cache(self):
        """Clear cached token"""
        self._token = None
        self._token_expiry = None
        logger.info("Auth0 token cache cleared")
    
    async def close(self):
        """Close the HTTP session"""
        if self._session:
            await self._session.close()
            self._session = None
