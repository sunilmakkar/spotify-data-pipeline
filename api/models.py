"""
Pydantic models for Spotify Recommendation API responses.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class Recommendation(BaseModel):
    """Single track recommendation with scoring details."""
    track_id: str = Field(..., description="Spotify track ID")
    track_name: str = Field(..., description="Track name")
    artist_name: str = Field(..., description="Artist name")
    recommendation_score: float = Field(..., description="Combined recommendation score (0-100)")
    cooccurrence_score: float = Field(..., description="Co-occurrence score (0-1)")
    artist_affinity_score: float = Field(..., description="Artist preference score (0-100)")
    sessions_together: int = Field(..., description="Number of sessions both tracks appeared together")
    reasoning: str = Field(..., description="Human-readable explanation")


class RecommendationResponse(BaseModel):
    """Complete API response with recommendations and metadata."""
    track_id: str = Field(..., description="Source track Spotify ID")
    track_name: str = Field(..., description="Source track name")
    artist_name: str = Field(..., description="Source track artist")
    recommendations: List[Recommendation] = Field(default_factory=list, description="List of recommended tracks")
    total_recommendations: int = Field(..., description="Total number of recommendations returned")
    message: Optional[str] = Field(None, description="Optional message (e.g., for empty results)")
    cache_hit: bool = Field(..., description="Whether result came from cache")
    query_time_ms: float = Field(..., description="Time taken to generate response (milliseconds)")
    generated_at: datetime = Field(default_factory=datetime.utcnow, description="Timestamp when response was generated")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Overall health status")
    redis: str = Field(..., description="Redis connection status")
    snowflake: str = Field(..., description="Snowflake connection status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Health check timestamp")