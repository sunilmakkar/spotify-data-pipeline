-- ============================================================================
-- DATA VALIDATION QUERIES - SPOTIFY DATA PIPELINE
-- ============================================================================
-- Purpose: Validate data quality across Bronze, Silver, and Gold layers
-- Project: Spotify Data Pipeline Recreation
-- Phases: 2.3 (Silver Layer) and 2.4 (Gold Layer)
-- ============================================================================

-- Set default context
USE DATABASE SPOTIFY_DATA;
USE WAREHOUSE SPOTIFY_WH;


-- ============================================================================
-- BRONZE LAYER VALIDATION
-- ============================================================================
USE SCHEMA BRONZE;

-- 1. Row Count Check
-- Expected: ~400 rows
SELECT COUNT(*) as total_rows
FROM BRONZE.plays;

-- 2. Sample Records Examination
-- Note: Using VALUE: syntax because this is an external table
SELECT
    VALUE:event_id::VARCHAR as event_id,
    VALUE:user_id::VARCHAR as user_id,
    VALUE:event_type::VARCHAR as event_type,
    VALUE:track_id::VARCHAR as track_id,
    VALUE:track_name::VARCHAR as track_name,
    VALUE:artist_name::VARCHAR as artist_name,
    VALUE:album_name::VARCHAR as album_name,
    VALUE:duration_ms::NUMBER as duration_ms,
    VALUE:played_at::VARCHAR as played_at,
    VALUE:device_type::VARCHAR as device_type
FROM BRONZE.plays
LIMIT 10;

-- 3. Duplicate Check
-- Expected: No duplicates (event_id should be unique)
SELECT
    VALUE:event_id::VARCHAR as event_id,
    COUNT(*) as occurrences
FROM BRONZE.plays
GROUP BY VALUE:event_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- 4. Data Quality Assessment
-- Checks for: null user_ids, invalid durations, suspiciously long tracks
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN VALUE:user_id IS NULL THEN 1 ELSE 0 END) as null_user_ids,
    SUM(CASE WHEN VALUE:duration_ms::NUMBER <= 0 THEN 1 ELSE 0 END) as invalid_durations,
    SUM(CASE WHEN VALUE:duration_ms::NUMBER > 600000 THEN 1 ELSE 0 END) as suspiciously_long_tracks
FROM BRONZE.plays;


-- ============================================================================
-- SILVER LAYER VALIDATION
-- ============================================================================
USE SCHEMA SILVER;

-- 1. Row Count Check
-- Expected: Same as bronze (~400 rows)
SELECT COUNT(*) as total_rows
FROM SILVER.silver_plays;

-- 2. Sample Records
-- Purpose: Verify proper data types (no longer VARIANT)
SELECT
    event_id,
    user_id,
    track_name,
    artist_name,
    duration_ms,
    played_at,
    processed_at
FROM SILVER.silver_plays
LIMIT 5;

-- 3. Bronze vs Silver Comparison
-- Row counts should match (no filtering occurred)
SELECT 
    'BRONZE' as layer,
    COUNT(*) as row_count
FROM BRONZE.plays
UNION ALL
SELECT 
    'SILVER' as layer,
    COUNT(*) as row_count
FROM SILVER.silver_plays;

-- 4. Data Type Verification
-- Check column metadata (should see proper types, not VARIANT)
SELECT 
    column_name,
    data_type,
    is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'SILVER'
  AND table_name = 'SILVER_PLAYS'
ORDER BY ordinal_position;

-- 5. Layer Comparison Sample
-- Verify data matches between bronze and silver
SELECT * FROM (
    SELECT 
        'BRONZE' as source,
        VALUE:event_id::VARCHAR as event_id,
        VALUE:track_name::VARCHAR as track_name,
        VALUE:duration_ms::NUMBER as duration_ms
    FROM BRONZE.plays
    LIMIT 3
)
UNION ALL
SELECT * FROM (
    SELECT 
        'SILVER' as source,
        event_id,
        track_name,
        duration_ms
    FROM SILVER.silver_plays
    LIMIT 3
);

-- 6. Processed Timestamp Verification
-- Confirm processed_at was added during transformation
SELECT 
    MIN(processed_at) as earliest_processed,
    MAX(processed_at) as latest_processed,
    COUNT(DISTINCT processed_at) as unique_timestamps
FROM SILVER.silver_plays;

-- 7. Track ID Validation
-- Verify deterministic track_ids (same song = same ID)
SELECT 
    track_name,
    COUNT(DISTINCT track_id) as unique_ids,
    COUNT(*) as total_plays
FROM SILVER.silver_plays
GROUP BY track_name
ORDER BY total_plays DESC;


-- ============================================================================
-- GOLD LAYER VALIDATION
-- ============================================================================
USE SCHEMA GOLD;

-- 1. Daily User Stats - Check Aggregations
SELECT 
    date,
    user_id,
    total_plays,
    total_listening_time_ms,
    unique_tracks,
    unique_artists,
    avg_track_duration_ms
FROM daily_user_stats
ORDER BY date DESC, total_plays DESC
LIMIT 10;

-- 2. Top Tracks - Most Popular Tracks
-- Expected: 5 unique tracks with proper rankings
SELECT 
    rank,
    track_id,
    track_name,
    artist_name,
    total_plays,
    total_listening_time_ms
FROM top_tracks
ORDER BY rank;

-- 3. Top Artists - Most Popular Artists
SELECT 
    rank,
    artist_name,
    total_plays,
    unique_tracks,
    total_listening_time_ms
FROM top_artists
ORDER BY rank;

-- 4. Device Usage - Device Preferences
-- Percentages should sum to ~100%
SELECT 
    device_type,
    total_plays,
    play_percentage,
    total_listening_time_ms
FROM device_usage
ORDER BY total_plays DESC;

-- 5. Cross-Layer Totals Verification
-- Gold totals should match silver record count
SELECT 
    (SELECT COUNT(*) FROM SILVER.silver_plays) as silver_count,
    (SELECT SUM(total_plays) FROM GOLD.device_usage) as gold_total_plays;

-- 6. Top Tracks Uniqueness Check
-- Should show 5 unique tracks (not duplicates)
SELECT COUNT(DISTINCT track_id) as unique_tracks
FROM top_tracks;

-- 7. Duplicate Detection
-- Should return no results
SELECT 
    track_id,
    COUNT(*) as occurrences
FROM top_tracks
GROUP BY track_id
HAVING COUNT(*) > 1;


-- ============================================================================
-- MAINTENANCE QUERIES (Run as needed)
-- ============================================================================

-- Refresh external table to pick up new S3 files
-- ALTER EXTERNAL TABLE BRONZE.plays REFRESH;

-- Drop and recreate tables (for debugging/regeneration)
-- DROP TABLE IF EXISTS SILVER.silver_plays;
-- DROP TABLE IF EXISTS GOLD.daily_user_stats;
-- DROP TABLE IF EXISTS GOLD.top_tracks;
-- DROP TABLE IF EXISTS GOLD.top_artists;
-- DROP TABLE IF EXISTS GOLD.device_usage;


-- ============================================================================
-- END OF VALIDATION QUERIES
-- ============================================================================