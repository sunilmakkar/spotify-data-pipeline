-- ============================================================================
-- PHASE 6 VALIDATION QUERIES - SPOTIFY DATA PIPELINE
-- ============================================================================
-- Purpose: Validate real-time recommendation system components
-- Project: Spotify Data Pipeline - Phase 6
-- Components: Currently Playing Poller, Recommendation API, Live Dashboard
-- Last Updated: December 2025
-- ============================================================================

-- Set default context
USE DATABASE SPOTIFY_DATA;
USE WAREHOUSE SPOTIFY_WH;
USE SCHEMA BRONZE;

-- ============================================================================
-- PHASE 6.1: CURRENTLY_PLAYING TABLE VALIDATION
-- ============================================================================
-- Purpose: Verify currently_playing table works before building poller
-- Table: BRONZE.currently_playing
-- Status: Testing table creation and basic operations
-- ============================================================================

-- Test 1: Insert a sample row
INSERT INTO currently_playing VALUES (
    CURRENT_TIMESTAMP(),
    'test_track_123',
    'Test Song',
    'Test Artist',
    'Test Album',
    TRUE,
    120000,
    240000
);

-- Test 2: Verify the insert worked
SELECT * 
FROM currently_playing 
ORDER BY detected_at DESC 
LIMIT 5;

-- Test 3: Clean up the test row
DELETE FROM currently_playing 
WHERE track_id = 'test_track_123';

-- Test 4: Verify table is now empty (ready for poller)
SELECT COUNT(*) as row_count 
FROM currently_playing;

-- ============================================================================
-- PHASE 6.1: POLLER VALIDATION (Run after poller is deployed)
-- ============================================================================
-- Purpose: Validate poller is detecting track changes and storing data
-- Run these queries while playing Spotify to verify real-time updates
-- ============================================================================

-- Query 1: Check if poller is writing data
-- Expected: Should see rows appearing as you play Spotify
SELECT 
    detected_at,
    track_name,
    artist_name,
    is_playing,
    progress_ms,
    duration_ms
FROM currently_playing
ORDER BY detected_at DESC
LIMIT 20;

-- Query 2: Verify track change detection
-- Expected: Should see different track_ids when you skip songs
SELECT 
    detected_at,
    track_id,
    track_name,
    artist_name,
    LAG(track_id) OVER (ORDER BY detected_at) as previous_track_id,
    LAG(track_name) OVER (ORDER BY detected_at) as previous_track_name,
    CASE 
        WHEN track_id != LAG(track_id) OVER (ORDER BY detected_at) 
        THEN 'TRACK CHANGE' 
        ELSE 'SAME TRACK' 
    END as change_status
FROM currently_playing
ORDER BY detected_at DESC
LIMIT 20;

-- Query 3: Check poller latency (time between detections)
-- Expected: Should be ~5 seconds between rows
SELECT 
    detected_at,
    track_name,
    LAG(detected_at) OVER (ORDER BY detected_at) as previous_detection,
    DATEDIFF(second, LAG(detected_at) OVER (ORDER BY detected_at), detected_at) as seconds_since_last
FROM currently_playing
ORDER BY detected_at DESC
LIMIT 20;

-- Query 4: Verify playback state tracking
-- Expected: is_playing should be TRUE when Spotify is active
SELECT 
    is_playing,
    COUNT(*) as detection_count,
    MIN(detected_at) as first_detection,
    MAX(detected_at) as last_detection
FROM currently_playing
GROUP BY is_playing
ORDER BY is_playing DESC;

-- ============================================================================
-- PHASE 6.2: RECOMMENDATION API VALIDATION (Run after API is deployed)
-- ============================================================================
-- Purpose: Validate recommendation API is serving correct data
-- Note: API queries will be tested via curl/browser, not Snowflake
-- These queries help verify the data API is reading from
-- ============================================================================

-- Query 5: Sample track IDs for API testing
-- Use these track_ids to test API endpoint: GET /recommendations/{track_id}
SELECT DISTINCT
    track_id,
    track_name,
    artist_name
FROM SILVER.SILVER_PLAYS
WHERE user_id = 'sunilmakkar97'
ORDER BY played_at DESC
LIMIT 10;

-- Query 6: Verify co-occurrence data availability for API
-- Expected: Should return track pairs that API will use
SELECT 
    track_a_id,
    track_a_name,
    track_b_id,
    track_b_name,
    cooccurrence_score
FROM GOLD.TRACK_COOCCURRENCE
WHERE user_id = 'sunilmakkar97'
ORDER BY cooccurrence_score DESC
LIMIT 10;

-- Query 7: Verify artist affinity data availability for API
-- Expected: Should return artist scores that API will use
SELECT 
    artist_name,
    affinity_score,
    rank
FROM GOLD.ARTIST_AFFINITY
WHERE user_id = 'sunilmakkar97'
ORDER BY rank
LIMIT 10;

-- ============================================================================
-- PHASE 6.3: END-TO-END SYSTEM VALIDATION
-- ============================================================================
-- Purpose: Validate complete real-time recommendation flow
-- Flow: Poller detects → Snowflake updated → API serves → Dashboard displays
-- ============================================================================

-- Query 8: Get most recent currently playing track
-- This simulates what the dashboard will query
SELECT 
    detected_at,
    track_id,
    track_name,
    artist_name,
    album_name,
    is_playing
FROM currently_playing
ORDER BY detected_at DESC
LIMIT 1;

-- Query 9: Simulate recommendation retrieval for current track
-- This mimics what the API will do when dashboard requests recommendations
WITH current_track AS (
    SELECT track_id
    FROM currently_playing
    ORDER BY detected_at DESC
    LIMIT 1
)
SELECT 
    co.track_b_id as recommended_track_id,
    co.track_b_name as recommended_track_name,
    co.track_b_artist as recommended_artist,
    co.cooccurrence_score,
    COALESCE(aa.affinity_score, 0) as artist_affinity_score,
    ROUND(
        (co.cooccurrence_score * 0.7) + 
        (COALESCE(aa.affinity_score, 0) / 100 * 0.3),
        3
    ) * 100 as recommendation_score
FROM current_track ct
JOIN GOLD.TRACK_COOCCURRENCE co 
    ON ct.track_id = co.track_a_id
LEFT JOIN GOLD.ARTIST_AFFINITY aa 
    ON co.user_id = aa.user_id 
    AND co.track_b_artist = aa.artist_name
WHERE co.user_id = 'sunilmakkar97'
ORDER BY recommendation_score DESC
LIMIT 10;

-- Query 10: System health check
-- Verify all components have data
SELECT 
    'Currently Playing' as COMPONENT,
    COUNT(*) as ROW_COUNT,
    MAX(detected_at) as LAST_UPDATED
FROM currently_playing
UNION ALL
SELECT 
    'Track Co-occurrence',
    COUNT(*),
    NULL
FROM GOLD.TRACK_COOCCURRENCE
UNION ALL
SELECT 
    'Artist Affinity',
    COUNT(*),
    NULL
FROM GOLD.ARTIST_AFFINITY;

-- ============================================================================
-- CLEANUP QUERIES (Use when needed)
-- ============================================================================
-- Purpose: Clear test data or reset tables during development
-- WARNING: These queries delete data - use with caution
-- ============================================================================

-- Clear all currently_playing data (if needed during testing)
-- TRUNCATE TABLE currently_playing;

-- Clear old currently_playing data (keep last 24 hours only)
-- DELETE FROM currently_playing 
-- WHERE detected_at < DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- ============================================================================
-- END OF PHASE 6 VALIDATION QUERIES
-- ============================================================================