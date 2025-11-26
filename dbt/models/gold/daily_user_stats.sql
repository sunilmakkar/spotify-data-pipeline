{{
    config(
        materialized='table',
        tags=['gold', 'metrics', 'daily']
    )
}}

-- ============================================================================
-- GOLD LAYER: DAILY USER LISTENING STATISTICS
-- ============================================================================
-- Purpose: Aggregate user listening behavior by day
-- 
-- Grain: One row per user per day
-- Primary Key: (date, user_id)
--
-- Metrics:
--   - Total plays per day
--   - Total listening time
--   - Unique tracks/artists played
--   - Average track duration
--
-- Source: SILVER.silver_plays
-- Target: GOLD.daily_user_stats
-- ============================================================================

WITH daily_aggregates AS (
    SELECT 
        -- Date dimension (truncate timestamp to date)
        DATE(played_at) AS date,
        
        -- User identifier
        user_id,
        
        -- Aggregate metrics
        COUNT(*) AS total_plays,
        SUM(duration_ms) AS total_listening_time_ms,
        COUNT(DISTINCT track_id) AS unique_tracks,
        COUNT(DISTINCT artist_name) AS unique_artists,
        AVG(duration_ms) AS avg_track_duration_ms
        
    FROM {{ ref('silver_plays') }}
    GROUP BY 
        DATE(played_at),
        user_id
)

SELECT 
    -- Dimensions
    date,
    user_id,
    
    -- Metrics (cast for clarity)
    total_plays::NUMBER AS total_plays,
    total_listening_time_ms::NUMBER AS total_listening_time_ms,
    unique_tracks::NUMBER AS unique_tracks,
    unique_artists::NUMBER AS unique_artists,
    ROUND(avg_track_duration_ms, 2)::NUMBER AS avg_track_duration_ms
    
FROM daily_aggregates
ORDER BY date DESC, user_id