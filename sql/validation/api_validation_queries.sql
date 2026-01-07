-- ============================================================================
-- PHASE 6.2: RECOMMENDATION API VALIDATION QUERIES
-- ============================================================================
-- Purpose: Validate recommendation API serves correct data
-- API Endpoint: http://3.137.218.31:8000/recommendations/{track_id}
-- ============================================================================

-- Query 1: Get valid track_ids for API testing
SELECT DISTINCT
    track_a_id,
    track_a_name,
    track_a_artist,
    COUNT(*) as recommendation_count
FROM GOLD.TRACK_COOCCURRENCE
WHERE user_id = 'sunilmakkar97'
GROUP BY track_a_id, track_a_name, track_a_artist
ORDER BY recommendation_count DESC
LIMIT 10;

-- Query 2: Verify recommendations match API response for a specific track
-- (Replace TRACK_ID with actual track_id to test)
WITH recs AS (
    SELECT 
        co.track_b_id,
        co.track_b_name,
        co.track_b_artist,
        co.cooccurrence_score,
        co.sessions_together,
        COALESCE(aa.affinity_score, 0) as artist_affinity_score,
        ROUND(
            (co.cooccurrence_score * 0.7) + 
            (COALESCE(aa.affinity_score, 0) / 100 * 0.3),
            3
        ) * 100 as recommendation_score
    FROM GOLD.TRACK_COOCCURRENCE co
    LEFT JOIN GOLD.ARTIST_AFFINITY aa 
        ON co.user_id = aa.user_id 
        AND co.track_b_artist = aa.artist_name
    WHERE co.user_id = 'sunilmakkar97'
      AND co.track_a_id = '6YV2AI87l1n2fzqU8Dyo05'  -- Example: Virginia Beach
)
SELECT * FROM recs
ORDER BY recommendation_score DESC
LIMIT 10;

-- Query 3: Check cache performance (run from EC2)
-- curl http://3.137.218.31:8000/recommendations/6YV2AI87l1n2fzqU8Dyo05
-- First call: cache_hit=false, query_time_ms ~600-700ms
-- Second call: cache_hit=true, query_time_ms <100ms

-- Query 4: Test with tracks from recent gym session
SELECT DISTINCT
    track_id,
    track_name,
    artist_name,
    MIN(detected_at) as first_played
FROM BRONZE.CURRENTLY_PLAYING
WHERE DATE(detected_at) = CURRENT_DATE()
ORDER BY first_played DESC
LIMIT 10;
