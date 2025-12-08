
  
    

        create or replace transient table spotify_data.gold.device_usage
         as
        (

-- ============================================================================
-- GOLD LAYER: DEVICE USAGE STATISTICS
-- ============================================================================
-- Purpose: Aggregate listening behavior by device type
-- 
-- Grain: One row per device type (all-time aggregation)
-- Primary Key: device_type
--
-- Metrics:
--   - Total plays per device
--   - Total listening time
--   - Percentage of total plays
--   - Average track duration
--
-- Source: SILVER.silver_plays
-- Target: GOLD.device_usage
-- ============================================================================

WITH device_aggregates AS (
    SELECT 
        -- Device identifier
        device_type,
        
        -- Aggregate metrics
        COUNT(*) AS total_plays,
        SUM(duration_ms) AS total_listening_time_ms,
        AVG(duration_ms) AS avg_track_duration_ms
        
    FROM spotify_data.silver.silver_plays
    GROUP BY device_type
),

total_plays_calc AS (
    -- Calculate overall total for percentage calculation
    SELECT SUM(total_plays) AS overall_total_plays
    FROM device_aggregates
)

SELECT 
    -- Identifier
    device_type::VARCHAR AS device_type,
    
    -- Metrics
    total_plays::NUMBER AS total_plays,
    total_listening_time_ms::NUMBER AS total_listening_time_ms,
    
    -- Percentage of total plays
    ROUND((total_plays::FLOAT / overall_total_plays::FLOAT) * 100, 2)::FLOAT AS play_percentage,
    
    -- Average duration
    ROUND(avg_track_duration_ms, 2)::NUMBER AS avg_track_duration_ms
    
FROM device_aggregates
CROSS JOIN total_plays_calc
ORDER BY total_plays DESC
        );
      
  