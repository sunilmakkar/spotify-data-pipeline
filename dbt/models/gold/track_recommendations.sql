{{
    config(
        materialized='table',
        tags=['recommendations', 'gold']
    )
}}

-- ============================================================================
-- TRACK RECOMMENDATIONS MODEL
-- ============================================================================
-- Purpose: Generate personalized track recommendations
-- Logic: Combines co-occurrence (70%) + artist affinity (30%)
-- ============================================================================

WITH played_tracks AS (
    SELECT DISTINCT 
        user_id,
        track_id
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
),

recommendations AS (
    SELECT 
        co.user_id,
        co.track_b_id as recommended_track_id,
        co.track_b_name as recommended_track_name,
        co.track_b_artist as recommended_artist,
        co.cooccurrence_score,
        COALESCE(aa.affinity_score, 0) as artist_affinity_score,
        -- Final score: 70% co-occurrence + 30% artist affinity
        ROUND(
            (co.cooccurrence_score * 0.7) + 
            (COALESCE(aa.affinity_score, 0) / 100 * 0.3),
            3
        ) * 100 as recommendation_score,
        co.track_a_name as based_on_track,
        co.track_a_artist as based_on_artist
    FROM {{ ref('track_cooccurrence') }} co
    LEFT JOIN {{ ref('artist_affinity') }} aa 
        ON co.user_id = aa.user_id 
        AND co.track_b_artist = aa.artist_name
    WHERE co.user_id = 'sunilmakkar97'
),

filtered_recommendations AS (
    SELECT 
        r.user_id,
        r.recommended_track_id,
        r.recommended_track_name,
        r.recommended_artist,
        r.cooccurrence_score,
        r.artist_affinity_score,
        r.recommendation_score,
        r.based_on_track,
        r.based_on_artist
    FROM recommendations r
),

ranked_recommendations AS (
    SELECT 
        user_id,
        recommended_track_id,
        recommended_track_name,
        recommended_artist,
        cooccurrence_score,
        artist_affinity_score,
        recommendation_score,
        based_on_track,
        based_on_artist,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY recommendation_score DESC) as rank
    FROM filtered_recommendations
)

SELECT 
    user_id,
    recommended_track_id,
    recommended_track_name,
    recommended_artist,
    cooccurrence_score,
    artist_affinity_score,
    recommendation_score,
    based_on_track,
    based_on_artist,
    rank,
    CURRENT_TIMESTAMP() as calculated_at
FROM ranked_recommendations
WHERE rank <= 50
ORDER BY user_id, rank
