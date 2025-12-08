
  
    

        create or replace transient table spotify_data.gold.top_artists
         as
        (

-- ============================================================================
-- GOLD LAYER: TOP ARTISTS BY PLAY COUNT
-- ============================================================================
-- Purpose: Aggregate and rank artists by popularity
-- 
-- Grain: One row per artist (all-time aggregation)
-- Primary Key: artist_name
--
-- Metrics:
--   - Total plays per artist
--   - Total listening time
--   - Number of unique tracks played
--   - First and last play timestamps
--   - Popularity rank
--
-- Source: SILVER.silver_plays
-- Target: GOLD.top_artists
-- ============================================================================

WITH artist_aggregates AS (
    SELECT 
        -- Artist identifier
        artist_name,
        
        -- Aggregate metrics
        COUNT(*) AS total_plays,
        SUM(duration_ms) AS total_listening_time_ms,
        COUNT(DISTINCT track_id) AS unique_tracks,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at
        
    FROM spotify_data.silver.silver_plays
    GROUP BY artist_name
),

ranked_artists AS (
    SELECT 
        *,
        -- Add ranking based on play count (1 = most played artist)
        ROW_NUMBER() OVER (ORDER BY total_plays DESC, total_listening_time_ms DESC) AS rank
    FROM artist_aggregates
)

SELECT 
    -- Identifier
    artist_name::VARCHAR AS artist_name,
    
    -- Metrics
    total_plays::NUMBER AS total_plays,
    total_listening_time_ms::NUMBER AS total_listening_time_ms,
    unique_tracks::NUMBER AS unique_tracks,
    
    -- Timestamps
    first_played_at::TIMESTAMP AS first_played_at,
    last_played_at::TIMESTAMP AS last_played_at,
    
    -- Ranking
    rank::NUMBER AS rank
    
FROM ranked_artists
ORDER BY rank
        );
      
  