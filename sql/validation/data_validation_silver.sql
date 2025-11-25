-- ============================================================================
-- SILVER LAYER EXPLORATION - PHASE 2.3
-- Purpose: Examine bronze data structure before building transformations
-- ============================================================================

-- Set context 
USE SPOTIFY_DATA;
USE SCHEMA BRONZE;
USE WAREHOUSE SPOTIFY_WH;


-- ============================================================================
-- 1. ROW COUNT CHECK
-- ============================================================================
-- Expected: ~200 rows (from Phase 1.3 Kafka consumer)
SELECT COUNT(*) as total_rows
FROM BRONZE.plays;


-- ============================================================================
-- 2. SAMPLE RECORDS EXAMINATION
-- ============================================================================
-- Purpose: Understand schema and verify data looks correct
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


-- ============================================================================
-- 3. DUPLICATE CHECK
-- ============================================================================
-- Purpose: Identify if any event_ids appear multiple times
-- Expected: No duplicates (event_id should be unique)
SELECT
    VALUE:event_id::VARCHAR as event_id,
    COUNT(*) as occurrences
FROM BRONZE.plays
GROUP BY VALUE:event_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;


-- ============================================================================
-- 4. DATA QUALITY ASSESSMENT
-- ============================================================================
-- Purpose: Identify records that need filtering in silver layer
-- Checks:
--   - Null user_ids (should be filtered out)
--   - Invalid durations (≤ 0ms means not played)
--   - Suspiciously long tracks (> 10 minutes suggests data error)
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN VALUE:user_id IS NULL THEN 1 ELSE 0 END) as null_user_ids,
    SUM(CASE WHEN VALUE:duration_is::NUMBER <= 0 THEN 1 ELSE 0 END) as invalid_durations,
    SUM(CASE WHEN VALUE:duration_ms::NUMBER > 600000 THEN 1 ELSE 0 END) as suspiciously_long_tracks
FROM BRONZE.plays;


-- ============================================================================
-- 5. SILVER LAYER VALIDATION
-- ============================================================================
-- Purpose: Verify silver layer table was created correctly
-- Expected: 200 rows with proper data types (no longer VARIANT)

USE DATABASE SPOTIFY_DATA;
USE SCHEMA SILVER;

-- Row count check
SELECT COUNT(*) as total_rows
FROM SILVER.silver_plays;

-- Sample some records (none with proper types, not VARIANT!)
SELECT
    event_id,
    user_id,
    track_name,
    artist_name,
    duration_ms,
    played_at,
    processed_at
FROM silver_plays
LIMIT 5;


-- ============================================================================
-- 6. SILVER VS BRONZE COMPARISON
-- ============================================================================
-- Purpose: Verify silver layer transformations worked correctly

-- Compare row counts (should be equal - no records filtered out)
SELECT 
    'BRONZE' as layer,
    COUNT(*) as row_count
FROM BRONZE.plays
UNION ALL
SELECT 
    'SILVER' as layer,
    COUNT(*) as row_count
FROM SILVER.silver_plays;

-- Verify data types are proper (no more VARIANT)
-- Check column metadata
SELECT 
    column_name,
    data_type,
    is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'SILVER'
  AND table_name = 'SILVER_PLAYS'
ORDER BY ordinal_position;

-- Sample comparison: Same event in both layers
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

-- Verify processed_at timestamp was added
SELECT 
    MIN(processed_at) as earliest_processed,
    MAX(processed_at) as latest_processed,
    COUNT(DISTINCT processed_at) as unique_timestamps
FROM SILVER.silver_plays;


-- ============================================================================
-- END OF VALIDATION QUERIES
-- ============================================================================