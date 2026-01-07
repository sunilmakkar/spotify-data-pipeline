"""
Redis cache management for recommendation API.
"""

import redis
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class RecommendationCache:
    """Redis cache for storing and retrieving recommendations."""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, ttl: int = 3600):
        """
        Initialize Redis cache connection.
        
        Args:
            host: Redis host (default: localhost)
            port: Redis port (default: 6379)
            ttl: Time-to-live for cache entries in seconds (default: 3600 = 1 hour)
        """
        self.ttl = ttl
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                decode_responses=True,
                socket_connect_timeout=5
            )
            # Test connection
            self.client.ping()
            logger.info("✓ Redis connection established")
        except Exception as e:
            logger.warning(f"⚠ Redis connection failed: {e}. Will operate without cache.")
            self.client = None
    
    def _generate_key(self, track_id: str, user_id: str) -> str:
        """Generate cache key for a track recommendation."""
        return f"rec:{user_id}:{track_id}"
    
    def get(self, track_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve recommendations from cache.
        
        Args:
            track_id: Spotify track ID
            user_id: User identifier
            
        Returns:
            Cached recommendations dict or None if not found
        """
        if not self.client:
            return None
        
        try:
            key = self._generate_key(track_id, user_id)
            cached_data = self.client.get(key)
            
            if cached_data:
                logger.info(f"✓ Cache HIT for track {track_id}")
                return json.loads(cached_data)
            else:
                logger.info(f"✗ Cache MISS for track {track_id}")
                return None
                
        except Exception as e:
            logger.warning(f"⚠ Cache GET failed: {e}")
            return None
    
    def set(self, track_id: str, user_id: str, data: Dict[str, Any]) -> bool:
        """
        Store recommendations in cache.
        
        Args:
            track_id: Spotify track ID
            user_id: User identifier
            data: Recommendations data to cache
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            key = self._generate_key(track_id, user_id)
            serialized_data = json.dumps(data, default=str)
            self.client.setex(key, self.ttl, serialized_data)
            logger.info(f"✓ Cached recommendations for track {track_id} (TTL: {self.ttl}s)")
            return True
            
        except Exception as e:
            logger.warning(f"⚠ Cache SET failed: {e}")
            return False
    
    def delete(self, track_id: str, user_id: str) -> bool:
        """
        Delete recommendations from cache.
        
        Args:
            track_id: Spotify track ID
            user_id: User identifier
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            key = self._generate_key(track_id, user_id)
            self.client.delete(key)
            logger.info(f"✓ Deleted cache for track {track_id}")
            return True
            
        except Exception as e:
            logger.warning(f"⚠ Cache DELETE failed: {e}")
            return False
    
    def health_check(self) -> bool:
        """
        Check if Redis is healthy.
        
        Returns:
            True if Redis is accessible, False otherwise
        """
        if not self.client:
            return False
        
        try:
            self.client.ping()
            return True
        except Exception:
            return False