{{
    config(
        materialized='table',
        tags=['silver', 'plays']
    )
}}

-- ============================================================================
-- SILVER LAYER: CLEANED PLAYS DATA
-- ============================================================================
-- Purpose: Transform bronze plays data into clean, analytics-ready format
-- 
-- Transformations:
--   1. Deduplicate records (DISTINCT on event_id)
--   2. Cast all data types from VARIANT to proper types
--   3. Filter invalid records (defensive WHERE clauses)
--   4. Add processing metadata (processed_at timestamp)
--
-- Deduplication Strategy:
--   Uses ROW_NUMBER() to handle cases where the same track was played at
--   the exact same timestamp (duplicates from backfill script running
--   multiple times with different event_ids). Keeps first occurrence based
--   on event_id sort order.
--
-- Source: bronze.plays (external table reading from S3 Parquet files)
-- Target: SILVER.silver_plays (materialized table)
-- ============================================================================

WITH source_data AS (
    SELECT
        VALUE:event_id::VARCHAR AS event_id,
        VALUE:user_id::VARCHAR AS user_id,
        VALUE:event_type::VARCHAR AS event_type,
        VALUE:track_id::VARCHAR AS track_id,
        VALUE:track_name::VARCHAR AS track_name,
        VALUE:artist_name::VARCHAR AS artist_name,
        VALUE:album_name::VARCHAR AS album_name,
        VALUE:duration_ms::NUMBER AS duration_ms,
        VALUE:played_at::VARCHAR AS played_at,
        VALUE:device_type::VARCHAR AS device_type
    FROM {{ source('bronze', 'plays') }}
    WHERE
      -- Data quality filters
        VALUE:user_id IS NOT NULL
        AND VALUE:duration_ms::NUMBER > 0
        AND VALUE:duration_ms::NUMBER < 600000  -- Less than 10 minutes (catches data errors)
        AND VALUE:event_id IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                user_id,
                track_id,
                played_at
            ORDER BY event_id
        ) as row_num
    FROM source_data
)

SELECT
    -- Identifiers
    event_id::VARCHAR AS event_id,
    user_id::VARCHAR AS user_id,
    event_type::VARCHAR AS event_type,
    
    -- Track information
    track_id::VARCHAR AS track_id,
    track_name::VARCHAR AS track_name,
    artist_name::VARCHAR AS artist_name,
    album_name::VARCHAR AS album_name,
    
    -- Metrics
    duration_ms::NUMBER AS duration_ms,
    
    -- Timestamps
    TRY_CAST(played_at AS TIMESTAMP) AS played_at,
    
    -- Device context
    device_type::VARCHAR AS device_type,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS processed_at
    
FROM deduplicated
WHERE row_num = 1 -- Keep only first occurrence of each unique play
