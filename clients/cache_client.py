import asyncio
import json
from typing import Any, Optional, Dict, Union, List
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
import redis.asyncio as redis  # Use the async version

from core.logger import get_logger
from core.config import settings

logger = get_logger(__name__)

class CacheClient:
    """
    Redis cache client - EXACTLY replicates your Node.js caching strategy
    Updated to use redis-py for Python 3.13 compatibility
    """
    
    def __init__(self, redis_host: Optional[str] = None, redis_port: Optional[int] = None, 
                 redis_password: Optional[str] = None, redis_username: Optional[str] = None,
                 redis_db: Optional[int] = None):
        self._redis: Optional[redis.Redis] = None
        # Global flag to enable or disable Cache (replicate Node.js)
        self.cache_enabled = getattr(settings, 'CACHE_ENABLED', True)
        
        # Allow override of Redis connection parameters
        self.redis_host = redis_host or settings.REDIS_HOST
        self.redis_port = redis_port or settings.REDIS_PORT
        self.redis_password = redis_password or settings.REDIS_PASSWORD
        self.redis_username = redis_username or getattr(settings, 'REDIS_USERNAME', '')
        self.redis_db = redis_db if redis_db is not None else getattr(settings, 'REDIS_DB', 0)
    
    async def connect_redis(self) -> None:
        """
        Establishes connection to Redis client and logs connection status.
        Replicates Node.js connectRedis function
        """
        if not self.cache_enabled:
            return
        
        try:
            # Build Redis URL (replicate Node.js URL format exactly)
            auth_part = f"{self.redis_username}:{self.redis_password}@" if self.redis_password else ""
            redis_url = f"redis://{auth_part}{self.redis_host}:{self.redis_port}"

            self._redis = redis.from_url(
                redis_url,
                db=self.redis_db,
                decode_responses=True,
                retry_on_timeout=True,
                socket_keepalive=True,
                health_check_interval=30
            )
            
            logger.info("Connected to Redis via utils")
            
        except Exception as error:
            logger.error(f"Error connecting to Redis: {error}")
            raise
    
    async def ensure_connection(self) -> None:
        """
        Utility function to ensure Redis connection is ready
        Replicates Node.js ensureConnection function
        """
        if not self.cache_enabled:
            return
        
        if not self._redis:
            await self.connect_redis()
    
    async def generate_cache_key(
        self, 
        module_name: str, 
        req: Dict[str, Any], 
        logged_in_user_id: Optional[Union[str, int]] = None
    ) -> str:
        """
        Generates unique cache key based on module name, request parameters, and user ID.
        EXACTLY replicates your Node.js generateCacheKey function
        
        Args:
            module_name: The service/module calling cache (e.g., "Analytics", "Auth", "Users")
            req: Request object containing params, query and path
            logged_in_user_id: Optional ID of the logged in user
        
        Returns:
            Generated cache key string, empty string if caching disabled
        """
        if not self.cache_enabled:
            return ""
        
        await self.ensure_connection()
        
        # Get function name dynamically from path (replicate Node.js)
        function_name = req.get('path', '').replace('/', '') if req.get('path') else ""
        
        params = req.get('params', {}) or {}
        query = req.get('query', {}) or {}
        all_params = {**params, **query}
        
        # Sort parameters and filter out undefined/null values (replicate Node.js)
        query_params = []
        for key in sorted(all_params.keys()):
            value = all_params[key]
            if value is not None and value != "":
                query_params.append(f"{key}:{value}")
        
        normalized_query_params = ":".join(query_params)
        
        # Include logged_in_user_id in key if available (replicate Node.js)
        user_part = f"user:{logged_in_user_id}" if logged_in_user_id else ""
        
        # Build cache key parts and filter out empty strings (replicate Node.js)
        key_parts = [module_name, function_name, user_part, normalized_query_params]
        cache_key = ":".join([part for part in key_parts if part])
        
        return cache_key
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def get_cache(self, key: str) -> Any:
        """
        Retrieves and parses data from Redis using provided key.
        EXACTLY replicates your Node.js getCache function
        """
        if not self.cache_enabled:
            return None
        
        try:
            await self.ensure_connection()
            data = await self._redis.get(key)
            return json.loads(data) if data else None
            
        except Exception as error:
            logger.error(f"Error getting data from Redis: {error}")
            return None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def set_cache(
        self, 
        key: str, 
        data: Any, 
        expiration_time: Union[int, Dict[str, int]]
    ) -> None:
        """
        Stores data in Redis with optional expiration time.
        EXACTLY replicates your Node.js setCache function
        
        Examples:
            await cache_client.set_cache('myKey', data, 60)  # 60 seconds
            await cache_client.set_cache('myKey', data, {'hours': 1})  # 1 hour
            await cache_client.set_cache('myKey', data, {'days': 1})  # 1 day - your default
        """
        if not self.cache_enabled:
            return
        
        try:
            await self.ensure_connection()
            
            # Calculate expiration in seconds (replicate Node.js logic exactly)
            if isinstance(expiration_time, int):
                expiration_in_seconds = int(expiration_time)
            else:
                # Calculate total seconds from hours and days
                hours = expiration_time.get('hours', 0)
                days = expiration_time.get('days', 0)
                total_hours = hours + (days * 24)
                total_minutes = total_hours * 60
                total_seconds = total_minutes * 60
                expiration_in_seconds = int(total_seconds)
            
            # Set with expiration (replicate Node.js EX option)
            await self._redis.set(key, json.dumps(data, default=str), ex=expiration_in_seconds)
            logger.info(f"Cache set for key: {key}")
            
        except Exception as error:
            logger.error(f"Error setting data to Redis: {error}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def update_cache(
        self, 
        key: str, 
        data: Any, 
        expiration_time: Union[int, Dict[str, int]] = {'days': 1}
    ) -> bool:
        """
        Force update cache with fresh data - for data engineering pipelines
        Always overwrites existing cache data
        
        Args:
            key: Cache key to update
            data: Fresh data to store
            expiration_time: Cache expiration (default 1 day)
            
        Returns:
            bool: True if successful, False if failed
        """
        if not self.cache_enabled:
            logger.warning("Cache disabled - skipping cache update")
            return False
        
        try:
            await self.ensure_connection()
            
            # Calculate expiration in seconds
            if isinstance(expiration_time, int):
                expiration_in_seconds = int(expiration_time)
            else:
                hours = expiration_time.get('hours', 0)
                days = expiration_time.get('days', 0)
                total_hours = hours + (days * 24)
                expiration_in_seconds = int(total_hours * 3600)
            
            # Force update cache (SET always overwrites)
            await self._redis.set(key, json.dumps(data, default=str), ex=expiration_in_seconds)
            logger.info(f"Cache UPDATED for key: {key}")
            return True
            
        except Exception as error:
            logger.error(f"Error updating cache: {error}")
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def refresh_cache_key(
        self, 
        module_name: str, 
        req: Dict[str, Any], 
        fresh_data: Any,
        logged_in_user_id: Optional[Union[str, int]] = None,
        expiration_time: Union[int, Dict[str, int]] = {'days': 1}
    ) -> tuple[str, bool]:
        """
        Generate cache key and refresh it with fresh data
        Convenience method for data engineering pipelines
        
        Returns:
            tuple: (cache_key, update_success)
        """
        # Generate the cache key
        cache_key = await self.generate_cache_key(module_name, req, logged_in_user_id)
        
        # Update cache with fresh data
        success = await self.update_cache(cache_key, fresh_data, expiration_time)
        
        return cache_key, success

    async def bulk_update_cache(
        self, 
        updates: List[Dict[str, Any]],
        expiration_time: Union[int, Dict[str, int]] = {'days': 1}
    ) -> Dict[str, int]:
        """
        Bulk update multiple cache keys efficiently
        
        Args:
            updates: List of {'key': cache_key, 'data': fresh_data}
            expiration_time: Cache expiration
            
        Returns:
            Dict with success/failure counts
        """
        if not self.cache_enabled:
            return {'successful': 0, 'failed': len(updates)}
        
        await self.ensure_connection()
        
        # Calculate expiration
        if isinstance(expiration_time, int):
            expiration_in_seconds = int(expiration_time)
        else:
            hours = expiration_time.get('hours', 0)
            days = expiration_time.get('days', 0)
            expiration_in_seconds = int((hours + days * 24) * 3600)
        
        successful = 0
        failed = 0
        
        # Use pipeline for efficiency
        pipe = self._redis.pipeline()
        
        try:
            for update in updates:
                key = update['key']
                data = update['data']
                pipe.set(key, json.dumps(data, default=str), ex=expiration_in_seconds)
            
            # Execute all updates at once
            results = await pipe.execute()
            
            for i, result in enumerate(results):
                if result:
                    successful += 1
                else:
                    failed += 1
                    logger.warning(f"Failed to update cache key: {updates[i]['key']}")
            
            logger.info(f"Bulk cache update: {successful} successful, {failed} failed")
            
        except Exception as error:
            logger.error(f"Bulk cache update error: {error}")
            failed = len(updates)
        
        return {'successful': successful, 'failed': failed}

    
    async def remove_cache(self, key: str) -> None:
        """Removes specified key from Redis cache"""
        if not self.cache_enabled:
            return
        
        try:
            await self.ensure_connection()
            await self._redis.delete(key)
            logger.info(f"Cache removed for key: {key}")
        except Exception as error:
            logger.error(f"Error removing data from Redis: {error}")
    
    async def remove_all_cache_keys_starting_with(
        self, 
        module_name: str, 
        logged_in_user_id: Optional[Union[str, int]] = None
    ) -> None:
        """
        Clears all cached data matching specific pattern.
        EXACTLY replicates your Node.js removeAllCacheKeysStartingWith function
        """
        if not self.cache_enabled:
            return
        
        try:
            await self.ensure_connection()
            
            # Build key pattern (replicate Node.js logic)
            if logged_in_user_id:
                user_part = f":user:{logged_in_user_id}:"
                key_pattern = f"{module_name}:*{user_part}*"
            else:
                key_pattern = f"{module_name}:*"
            
            # Get matching keys using scan_iter
            keys = []
            async for key in self._redis.scan_iter(match=key_pattern):
                keys.append(key)
            
            if keys:
                await self._redis.delete(*keys)
                user_msg = f" for user '{logged_in_user_id}'" if logged_in_user_id else ""
                logger.info(f"Cleared {len(keys)} cache key(s) starting with '{module_name}'{user_msg}")
            else:
                user_msg = f" for user '{logged_in_user_id}'" if logged_in_user_id else ""
                logger.info(f"No cache keys found starting with '{module_name}'{user_msg}")
                
        except Exception as error:
            user_msg = f" for user '{logged_in_user_id}'" if logged_in_user_id else ""
            logger.error(f"Error clearing cache keys starting with '{module_name}'{user_msg}: {error}")
    
    async def close(self):
        """Close Redis connection"""
        if self._redis:
            await self._redis.aclose()  # Use aclose() for redis-py async
