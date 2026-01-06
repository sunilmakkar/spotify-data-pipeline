"""
Spotify Recommendation API
FastAPI service that serves personalized track recommendations with Redis caching.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import logging
import time
from datetime import datetime
from typing import Optional

from api.models import RecommendationResponse, HealthResponse, Recommendation
from api.cache import RecommendationCache
from api.database import RecommendationDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Spotify Recommendation API",
    description="Real-time personalized music recommendations powered by collaborative filtering",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Initialize cache and database (singleton pattern)
cache = RecommendationCache()
database = RecommendationDatabase()


@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup."""
    logger.info("=" * 80)
    logger.info("Spotify Recommendation API Starting Up")
    logger.info("=" * 80)
    
    # Test database connection
    if database.connect():
        logger.info("✓ Database connection successful")
    else:
        logger.warning("⚠ Database connection failed - will retry on first request")
    
    # Test cache connection
    if cache.health_check():
        logger.info("✓ Cache connection successful")
    else:
        logger.warning("⚠ Cache connection failed - will operate without cache")
    
    logger.info("=" * 80)


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    logger.info("Shutting down API...")
    database.close()


@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint redirect to docs."""
    return {
        "message": "Spotify Recommendation API",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns connection status for Redis and Snowflake.
    """
    redis_status = "connected" if cache.health_check() else "disconnected"
    snowflake_status = "connected" if database.health_check() else "disconnected"
    
    overall_status = "healthy" if (redis_status == "connected" or snowflake_status == "connected") else "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        redis=redis_status,
        snowflake=snowflake_status
    )


@app.get("/recommendations/{track_id}", response_model=RecommendationResponse)
async def get_recommendations(
    track_id: str,
    user_id: str = Query(default="sunilmakkar97", description="User identifier"),
    limit: int = Query(default=10, ge=1, le=50, description="Maximum number of recommendations")
):
    """
    Get personalized recommendations for a track.
    
    Args:
        track_id: Spotify track ID (from URL path)
        user_id: User identifier (query parameter, default: sunilmakkar97)
        limit: Maximum recommendations to return (query parameter, 1-50, default: 10)
    
    Returns:
        RecommendationResponse with track recommendations and metadata
    
    Raises:
        HTTPException: 404 if track not found, 500 if database error
    """
    start_time = time.time()
    cache_hit = False
    
    logger.info(f"📥 Recommendation request: track_id={track_id}, user_id={user_id}, limit={limit}")
    
    try:
        # Try cache first
        cached_data = cache.get(track_id, user_id)
        
        if cached_data:
            cache_hit = True
            query_time_ms = (time.time() - start_time) * 1000
            
            logger.info(f"✓ Served from cache in {query_time_ms:.2f}ms")
            
            # Update response with current metadata
            cached_data['cache_hit'] = True
            cached_data['query_time_ms'] = round(query_time_ms, 2)
            cached_data['generated_at'] = datetime.utcnow()
            
            # Apply limit if different from cached
            if len(cached_data.get('recommendations', [])) > limit:
                cached_data['recommendations'] = cached_data['recommendations'][:limit]
                cached_data['total_recommendations'] = limit
            
            return RecommendationResponse(**cached_data)
        
        # Cache miss - query database
        logger.info("Cache miss - querying Snowflake...")
        
        db_result = database.get_recommendations(track_id, user_id, limit)
        
        query_time_ms = (time.time() - start_time) * 1000
        
        # Check if we got results
        if db_result['total_recommendations'] == 0:
            # No recommendations found
            response_data = {
                'track_id': track_id,
                'track_name': db_result.get('track_name', 'Unknown'),
                'artist_name': db_result.get('artist_name', 'Unknown'),
                'recommendations': [],
                'total_recommendations': 0,
                'message': 'No recommendations available yet. This track needs more listening history.',
                'cache_hit': False,
                'query_time_ms': round(query_time_ms, 2),
                'generated_at': datetime.utcnow()
            }
            
            logger.info(f"✓ No recommendations found for track {track_id}")
            return RecommendationResponse(**response_data)
        
        # Build response with recommendations
        recommendations = [
            Recommendation(**rec) for rec in db_result['recommendations']
        ]
        
        response_data = {
            'track_id': track_id,
            'track_name': db_result['track_name'],
            'artist_name': db_result['artist_name'],
            'recommendations': recommendations,
            'total_recommendations': len(recommendations),
            'cache_hit': False,
            'query_time_ms': round(query_time_ms, 2),
            'generated_at': datetime.utcnow()
        }
        
        # Cache the result (convert to dict for caching)
        cache_data = {
            'track_id': track_id,
            'track_name': db_result['track_name'],
            'artist_name': db_result['artist_name'],
            'recommendations': db_result['recommendations'],
            'total_recommendations': len(recommendations)
        }
        cache.set(track_id, user_id, cache_data)
        
        logger.info(f"✓ Served {len(recommendations)} recommendations from database in {query_time_ms:.2f}ms")
        
        return RecommendationResponse(**response_data)
    
    except Exception as e:
        logger.error(f"✗ Error processing request: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve recommendations: {str(e)}"
        )


@app.get("/cache/clear/{track_id}")
async def clear_cache(
    track_id: str,
    user_id: str = Query(default="sunilmakkar97", description="User identifier")
):
    """
    Clear cache for a specific track (admin/debugging endpoint).
    
    Args:
        track_id: Spotify track ID to clear from cache
        user_id: User identifier
    
    Returns:
        Success message
    """
    success = cache.delete(track_id, user_id)
    
    if success:
        return {"message": f"Cache cleared for track {track_id}", "success": True}
    else:
        return {"message": "Cache clear failed or cache unavailable", "success": False}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)