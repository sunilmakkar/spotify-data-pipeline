
  
    

        create or replace transient table spotify_data.gold.top_tracks
         as
        (

-- ============================================================================
-- GOLD LAYER: TOP TRACKS BY PLAY COUNT
-- ============================================================================
-- Purpose: Aggregate and rank tracks by popularity
-- 
-- Grain: One row per track (all-time aggregation)
-- Primary Key: track_id
--
-- Metrics:
--   - Total plays per track
--   - Total listening time
--   - First and last play timestamps
--   - Popularity rank
--
-- Source: SILVER.silver_plays
-- Target: GOLD.top_tracks
-- ============================================================================

WITH track_aggregates AS (
    SELECT 
        -- Track identifiers and attributes
        track_id,
        track_name,
        artist_name,
        album_name,
        
        -- Aggregate metrics
        COUNT(*) AS total_plays,
        SUM(duration_ms) AS total_listening_time_ms,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at
        
    FROM spotify_data.silver.silver_plays
    GROUP BY 
        track_id,
        track_name,
        artist_name,
        album_name
),

ranked_tracks AS (
    SELECT 
        *,
        -- Add ranking based on play count (1 = most played)
        ROW_NUMBER() OVER (ORDER BY total_plays DESC, total_listening_time_ms DESC) AS rank
    FROM track_aggregates
)

SELECT 
    -- Identifiers
    track_id::VARCHAR AS track_id,
    track_name::VARCHAR AS track_name,
    artist_name::VARCHAR AS artist_name,
    album_name::VARCHAR AS album_name,
    
    -- Metrics
    total_plays::NUMBER AS total_plays,
    total_listening_time_ms::NUMBER AS total_listening_time_ms,
    
    -- Timestamps
    first_played_at::TIMESTAMP AS first_played_at,
    last_played_at::TIMESTAMP AS last_played_at,
    
    -- Ranking
    rank::NUMBER AS rank
    
FROM ranked_tracks
ORDER BY rank
        );
      
  