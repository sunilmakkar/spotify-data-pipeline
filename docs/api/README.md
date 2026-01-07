# Spotify Recommendation API Documentation

## Overview
Real-time personalized music recommendations powered by collaborative filtering, with Redis caching for sub-millisecond response times.

## Base URL
```
http://3.137.218.31:8000
```

## Endpoints

### 1. Health Check
```bash
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "redis": "connected",
  "snowflake": "connected",
  "timestamp": "2026-01-07T00:00:00Z"
}
```

### 2. Get Recommendations
```bash
GET /recommendations/{track_id}?user_id=sunilmakkar97&limit=10
```

**Parameters:**
- `track_id` (path, required): Spotify track ID
- `user_id` (query, optional): User identifier (default: "sunilmakkar97")
- `limit` (query, optional): Max recommendations (1-50, default: 10)

**Response:**
```json
{
  "track_id": "6YV2AI87l1n2fzqU8Dyo05",
  "track_name": "Virginia Beach",
  "artist_name": "Drake",
  "recommendations": [
    {
      "track_id": "7rC5Pl8rQSX4myONQHYPBK",
      "track_name": "Mob Ties",
      "artist_name": "Drake",
      "recommendation_score": 100.0,
      "cooccurrence_score": 1.0,
      "artist_affinity_score": 100.0,
      "sessions_together": 1,
      "reasoning": "Frequently played together (1 sessions), artist is highly preferred"
    }
  ],
  "total_recommendations": 4,
  "cache_hit": true,
  "query_time_ms": 1.08,
  "generated_at": "2026-01-07T00:00:00Z"
}
```

### 3. Clear Cache (Admin)
```bash
GET /cache/clear/{track_id}?user_id=sunilmakkar97
```

**Response:**
```json
{
  "message": "Cache cleared for track {track_id}",
  "success": true
}
```

## Performance
- **Cache hit:** <100ms
- **Cache miss:** <700ms
- **Cache TTL:** 3600 seconds (1 hour)

## Interactive Documentation
Visit http://3.137.218.31:8000/docs for Swagger UI with live testing.

## Service Management

**Start:**
```bash
sudo systemctl start recommendation-api
```

**Stop:**
```bash
sudo systemctl stop recommendation-api
```

**Restart:**
```bash
sudo systemctl restart recommendation-api
```

**Status:**
```bash
sudo systemctl status recommendation-api
```

**View Logs:**
```bash
sudo journalctl -u recommendation-api -f
```

## Architecture
```
Client Request → FastAPI (/recommendations/{track_id})
                      ↓
                Check Redis Cache
                      ↓
           Cache Hit? → Return (<100ms)
                      ↓ No
            Query Snowflake:
              - GOLD.TRACK_COOCCURRENCE
              - GOLD.ARTIST_AFFINITY
                      ↓
         Calculate Combined Score:
           (cooccurrence * 0.7) + (affinity/100 * 0.3)
                      ↓
         Store in Redis (TTL: 1 hour)
                      ↓
              Return (<700ms)
```
