-- ====================================================
-- SPOTIFY DATA PIPELINE - SNOWFLAKE DDL
-- ====================================================

-- ====================================================
-- DATABASE & WAREHOUSE SETUP
-- ====================================================


-- Create database
CREATE DATABASE IF NOT EXISTS SPOTIFY_DATA;

-- Use the database
USE DATABASE SPOTIFY_DATA;

-- Create warehouse (compute resource)
CREATE WAREHOUSE SPOTIFY_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Use the warehouse
USE WAREHOUSE SPOTIFY_WH;

-- ====================================================
-- SCHEMA STRUCTURE
-- ====================================================


-- RAW schema 
CREATE SCHEMA IF NOT EXISTS RAW;

-- BRONZE schema (external tables pointing to S3)
CREATE SCHEMA IF NOT EXISTS BRONZE;

-- SILVER schema (cleaned and deduplicated data)
CREATE SCHEMA IF NOT EXISTS SILVER;

-- GOLD schema (analytics and metrics)
CREATE SCHEMA IF NOT EXISTS GOLD;

-- ====================================================
-- VERIFICATION
-- ====================================================


SHOW DATABASES;
SHOW WAREHOUSES;
SHOW SCHEMAS IN DATABASE SPOTIFY_DATA;

-- ====================================================
-- EXTERNAL STAGE (S3 CONNECTION)
-- ====================================================

-- Use the BRONZE schema
USE SCHEMA BRONZE;

-- Create storage integration
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::528757821825:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-data-lake-sunil-2025/bronze/');

-- Get Snowflake's AWS user and External ID
DESC STORAGE INTEGRATION s3_integration;

-- Now create BRONZE stage
CREATE OR REPLACE STAGE bronze_stage
    URL = 's3://spotify-data-lake-sunil-2025/bronze/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = (TYPE = PARQUET);

-- Test it
LIST @bronze_stage/plays/;


-- ====================================================
-- EXTERNAL TABLES
-- ====================================================

-- Use BRONZE schema
USE SCHEMA BRONZE;

-- Create external tables for plays
CREATE OR REPLACE EXTERNAL TABLE plays
WITH LOCATION = @bronze_stage/plays/
PATTERN = '.*year=.*/month=.*/day=.*/hour=.*/.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;


-- ====================================================
-- VERIFICATION - EXTERNAL TABLES
-- ====================================================


-- Refresh metadata to detect existing files
ALTER EXTERNAL TABLE plays REFRESH;

-- Check metadata (to see partition columns detected)
SELECT * FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(TABLE_NAME => 'PLAYS'));


-- ====================================================
-- VERIFICATION - QUERY EXTERNAL TABLE
-- ====================================================


-- Test query: Get first 10 events
SELECT * FROM BRONZE.plays LIMIT 10;

-- Count total events (should be 200)
SELECT COUNT(*) as total_events FROM BRONZE.plays;

-- Check unique tracks
SELECT 
    VALUE:track_name::VARCHAR as track_name,
    VALUE:artist_name::VARCHAR as artist_name,
    COUNT(*) as play_count
FROM BRONZE.plays
GROUP BY VALUE:track_name, VALUE:artist_name
ORDER BY play_count DESC
LIMIT 10;

-- Verify partition detection
SELECT 
    registered_on,
    file_name,
    file_size
FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(TABLE_NAME => 'PLAYS'));


-- ====================================================
-- COST CONTROL VERIFICATION
-- ====================================================


-- Check warehouse configuration
SHOW WAREHOUSES LIKE 'SPOTIFY_WH';

-- Verify auto-suspend is 60 seconds and size is X-SMALL
-- Expected: auto_suspend = 60, size = X-SMALL

-- Check current credit usage
SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) as total_credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME;

-- Check what warehouses are there
SHOW WAREHOUSES;

-- Delete unused warehouses
DROP WAREHOUSE IF EXISTS SNOWFLAKE_LEARNING_WH;
DROP WAREHOUSE IF EXISTS COMPUTE_WH;

-- Verify warehouses
SHOW WAREHOUSES;