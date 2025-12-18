-- ============================================================================
-- SPOTIFY DATA PIPELINE - SNOWFLAKE DDL (DATA DEFINITION LANGUAGE)
-- ============================================================================
-- Purpose: Define database infrastructure for Spotify data pipeline
-- Project: Spotify Data Pipeline - Phases 1-6
-- Components: Database, Warehouse, Schemas, External Stage, Tables
-- Last Updated: December 2025
-- ============================================================================

-- ============================================================================
-- SECTION 1: DATABASE & WAREHOUSE SETUP
-- ============================================================================
-- Purpose: Create core Snowflake infrastructure
-- Components: SPOTIFY_DATA database, SPOTIFY_WH warehouse (X-SMALL)
-- Cost Control: Auto-suspend 60s, auto-resume enabled
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS SPOTIFY_DATA;

-- Use the database
USE DATABASE SPOTIFY_DATA;

-- Create warehouse (compute resource)
CREATE WAREHOUSE IF NOT EXISTS SPOTIFY_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Use the warehouse
USE WAREHOUSE SPOTIFY_WH;

-- ============================================================================
-- SECTION 2: SCHEMA STRUCTURE (MEDALLION ARCHITECTURE)
-- ============================================================================
-- Purpose: Implement medallion data architecture
-- Layers:
--   - RAW: Landing zone for raw Kafka messages (unused currently)
--   - BRONZE: External tables pointing to S3 Parquet files
--   - SILVER: Cleaned, deduplicated, and validated data
--   - GOLD: Analytics-ready aggregations and recommendations
-- ============================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- ============================================================================
-- SECTION 3: EXTERNAL STAGE (S3 CONNECTION)
-- ============================================================================
-- Purpose: Connect Snowflake to S3 data lake for external table access
-- S3 Bucket: s3://spotify-data-lake-sunil-2025/bronze/
-- AWS Role: arn:aws:iam::528757821825:role/snowflake-s3-role
-- File Format: Parquet
-- ============================================================================

-- Use BRONZE schema for external resources
USE SCHEMA BRONZE;

-- Create storage integration for S3 access
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::528757821825:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-data-lake-sunil-2025/bronze/');

-- Verify storage integration (reveals Snowflake AWS User and External ID for trust policy)
DESC STORAGE INTEGRATION s3_integration;

-- Create external stage pointing to S3 bronze layer
CREATE OR REPLACE STAGE bronze_stage
    URL = 's3://spotify-data-lake-sunil-2025/bronze/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = (TYPE = PARQUET);

-- ============================================================================
-- SECTION 4: EXTERNAL TABLES (BRONZE LAYER)
-- ============================================================================
-- Purpose: Create external tables that read Parquet files directly from S3
-- Table: plays - Listening history from Spotify API (via Kafka)
-- Partitioning: Hive-style (year/month/day/hour)
-- Refresh: Manual (ALTER EXTERNAL TABLE plays REFRESH)
-- ============================================================================

-- Create external table for plays
CREATE OR REPLACE EXTERNAL TABLE plays
WITH LOCATION = @bronze_stage/plays/
PATTERN = '.*year=.*/month=.*/day=.*/hour=.*/.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

-- ============================================================================
-- SECTION 5: REGULAR TABLES (BRONZE LAYER)
-- ============================================================================
-- Purpose: Create standard tables for real-time/operational data
-- Note: Unlike external tables, these are stored in Snowflake's internal storage
-- ============================================================================

-- Table: currently_playing
-- Purpose: Track real-time playback state (written by Spotify Poller - Phase 6)
-- Updated: Every 5 seconds by background poller service
-- Usage: Powers real-time recommendation dashboard
CREATE TABLE IF NOT EXISTS currently_playing (
    detected_at TIMESTAMP_NTZ NOT NULL,
    track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(500),
    artist_name VARCHAR(500),
    album_name VARCHAR(500),
    is_playing BOOLEAN,
    progress_ms INTEGER,
    duration_ms INTEGER,
    PRIMARY KEY (detected_at, track_id)
);

-- Add additional playback state columns for Phase 6
ALTER TABLE currently_playing 
ADD device_type VARCHAR(50),
    device_name VARCHAR(200),
    shuffle_state BOOLEAN,
    repeat_state VARCHAR(20),
    context_type VARCHAR(50),
    context_uri VARCHAR(200);

-- ============================================================================
-- SECTION 6: VERIFICATION QUERIES
-- ============================================================================
-- Purpose: Validate infrastructure setup and data availability
-- Run these queries after initial setup to confirm everything works
-- ============================================================================

-- Verify database and warehouse creation
SHOW DATABASES;
SHOW WAREHOUSES;
SHOW SCHEMAS IN DATABASE SPOTIFY_DATA;

-- Verify external stage connectivity
LIST @bronze_stage/plays/;

-- Refresh external table metadata to detect Parquet files
ALTER EXTERNAL TABLE plays REFRESH;

-- Verify external table file detection
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(TABLE_NAME => 'PLAYS'));

-- Test query: Sample plays data
SELECT * 
FROM BRONZE.plays 
LIMIT 10;

-- Test query: Count total plays
SELECT COUNT(*) as total_plays 
FROM BRONZE.plays;

-- Test query: Top tracks by play count
SELECT 
    VALUE:track_name::VARCHAR as track_name,
    VALUE:artist_name::VARCHAR as artist_name,
    COUNT(*) as play_count
FROM BRONZE.plays
GROUP BY VALUE:track_name, VALUE:artist_name
ORDER BY play_count DESC
LIMIT 10;

-- ============================================================================
-- SECTION 7: COST CONTROL & CLEANUP
-- ============================================================================
-- Purpose: Monitor Snowflake credit usage and remove unused resources
-- Best Practice: Run weekly to ensure no surprise costs
-- ============================================================================

-- Check warehouse configuration
SHOW WAREHOUSES LIKE 'SPOTIFY_WH';

-- Monitor credit usage (last 7 days)
SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) as total_credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME;

-- List all warehouses
SHOW WAREHOUSES;

-- Remove unused warehouses (if any exist)
DROP WAREHOUSE IF EXISTS SNOWFLAKE_LEARNING_WH;
DROP WAREHOUSE IF EXISTS COMPUTE_WH;

-- ============================================================================
-- END OF DDL SCRIPT
-- ============================================================================
-- Next Steps:
-- 1. Run DBT models to populate SILVER and GOLD layers
-- 2. Set up Airflow DAG for automated pipeline orchestration
-- 3. Verify data quality with validation queries
-- ============================================================================