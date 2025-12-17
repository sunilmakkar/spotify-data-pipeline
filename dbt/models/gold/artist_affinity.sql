{{
    config(
        materialized='table',
        tags=['recommendations', 'gold']
    )
}}

-- ============================================================================
-- ARTIST AFFINITY MODEL
-- ============================================================================
-- Purpose: Rank artists by listening preference with recency bias
-- Logic: Score = (weighted_plays * 0.6) + (unique_tracks * 0.4), normalized to 0-100
-- Recency: Last 7 days weighted 2x
-- ============================================================================

WITH artist_plays AS (
    SELECT 
        user_id,
        artist_name,
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks,
        SUM(CASE 
            WHEN played_at >= DATEADD(day, -7, CURRENT_TIMESTAMP()) 
            THEN 2 
            ELSE 1 
        END) as weighted_plays
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
    GROUP BY user_id, artist_name
),

artist_scores AS (
    SELECT 
        user_id,
        artist_name,
        total_plays,
        unique_tracks,
        weighted_plays,
        (weighted_plays * 0.6) + (unique_tracks * 0.4) as raw_score
    FROM artist_plays
),

normalized_scores AS (
    SELECT 
        user_id,
        artist_name,
        total_plays,
        unique_tracks,
        weighted_plays,
        raw_score,
        ROUND(
            ((raw_score - MIN(raw_score) OVER ()) / 
             NULLIF((MAX(raw_score) OVER () - MIN(raw_score) OVER ()), 0)) * 100,
            2
        ) as affinity_score
    FROM artist_scores
),

ranked_artists AS (
    SELECT 
        user_id,
        artist_name,
        total_plays,
        unique_tracks,
        weighted_plays,
        affinity_score,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY affinity_score DESC) as rank
    FROM normalized_scores
)

SELECT 
    user_id,
    artist_name,
    total_plays,
    unique_tracks,
    weighted_plays,
    affinity_score,
    rank,
    CURRENT_TIMESTAMP() as calculated_at
FROM ranked_artists
WHERE rank <= 50
ORDER BY user_id, rank
