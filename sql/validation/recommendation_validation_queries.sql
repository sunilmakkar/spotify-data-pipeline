-- ============================================================================
-- RECOMMENDATION ENGINE VALIDATION QUERIES - SPOTIFY DATA PIPELINE
-- ============================================================================
-- Purpose: Validate collaborative filtering recommendation models
-- Project: Spotify Data Pipeline - Phase 5.3
-- Models: track_cooccurrence, artist_affinity, track_recommendations
-- Last Updated: December 2025
-- ============================================================================
-- NOTE: Duplicate investigation queries moved to data_validation_queries_silver.sql
--       See that file for context on duplicate issue discovered/resolved Dec 13, 2025
-- ============================================================================

-- Set default context
USE DATABASE SPOTIFY_DATA;
USE WAREHOUSE SPOTIFY_WH;
USE SCHEMA SILVER;

-- ============================================================================
-- PHASE 5.3.1: DATA EXPLORATION FOR CO-OCCURRENCE MODEL
-- ============================================================================
-- Purpose: Understand listening patterns before building track_cooccurrence
-- Run Date: December 16, 2025
-- ============================================================================

-- Query 1: Data Volume and Date Range
-- Shows how much data we have to work with for recommendations
SELECT 
    COUNT(*) as total_plays,
    COUNT(DISTINCT track_id) as unique_tracks,
    MIN(played_at) as earliest_play,
    MAX(played_at) as latest_play,
    DATEDIFF(day, MIN(played_at), MAX(played_at)) as days_of_data
FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
WHERE user_id = 'sunilmakkar97';

-- Query 2: Sample Recent Plays
-- Shows what recent listening looks like
SELECT 
    played_at,
    track_name,
    artist_name,
    track_id
FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
WHERE user_id = 'sunilmakkar97'
ORDER BY played_at DESC
LIMIT 10;

-- Query 3: Time Gaps Between Plays
-- Checks if plays are clustered (sessions) or spread out
-- Important for determining if 1-hour session window makes sense
SELECT 
    track_name,
    played_at,
    LAG(played_at) OVER (ORDER BY played_at) as previous_play,
    DATEDIFF(minute, LAG(played_at) OVER (ORDER BY played_at), played_at) as minutes_since_last
FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
WHERE user_id = 'sunilmakkar97'
ORDER BY played_at DESC
LIMIT 15;

-- ============================================================================
-- PHASE 5.3.1: TRACK CO-OCCURRENCE MODEL VALIDATION
-- ============================================================================
-- Purpose: Validate track_cooccurrence model after creation
-- Model Location: dbt/models/gold/track_cooccurrence.sql
-- Status: COMPLETE - Model created successfully on Dec 16, 2025
-- ============================================================================

-- Query 4: Session Grouping Test
-- Groups plays into listening sessions (1-hour window)
WITH plays_with_lag AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        LAG(played_at) OVER (ORDER BY played_at) as previous_play
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
    WHERE user_id = 'sunilmakkar97'
),
session_assignment AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        SUM(CASE 
            WHEN DATEDIFF(minute, previous_play, played_at) > 60 OR previous_play IS NULL
            THEN 1 
            ELSE 0 
        END) OVER (ORDER BY played_at) as session_id
    FROM plays_with_lag
)
SELECT 
    session_id,
    COUNT(*) as tracks_in_session,
    MIN(played_at) as session_start,
    MAX(played_at) as session_end,
    DATEDIFF(minute, MIN(played_at), MAX(played_at)) as session_duration_minutes,
    LISTAGG(DISTINCT LEFT(track_name, 30), ' | ') as tracks_played
FROM session_assignment
GROUP BY session_id
ORDER BY session_id;

-- Query 5: Track Pair Generation Test
-- Creates all track pairs within sessions
WITH plays_with_lag AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        LAG(played_at) OVER (ORDER BY played_at) as previous_play
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
    WHERE user_id = 'sunilmakkar97'
),
session_assignment AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        SUM(CASE 
            WHEN DATEDIFF(minute, previous_play, played_at) > 60 OR previous_play IS NULL
            THEN 1 
            ELSE 0 
        END) OVER (ORDER BY played_at) as session_id
    FROM plays_with_lag
),
track_pairs AS (
    SELECT DISTINCT
        a.session_id,
        a.track_id as track_a_id,
        a.track_name as track_a_name,
        b.track_id as track_b_id,
        b.track_name as track_b_name
    FROM session_assignment a
    JOIN session_assignment b 
        ON a.session_id = b.session_id 
        AND a.track_id < b.track_id
)
SELECT 
    COUNT(*) as total_pairs,
    COUNT(DISTINCT session_id) as sessions_with_pairs
FROM track_pairs;

-- Query 6: Co-Occurrence Score Calculation Test
-- Calculates final co-occurrence scores for track pairs
WITH plays_with_lag AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        LAG(played_at) OVER (ORDER BY played_at) as previous_play
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
    WHERE user_id = 'sunilmakkar97'
),
session_assignment AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        SUM(CASE 
            WHEN DATEDIFF(minute, previous_play, played_at) > 60 OR previous_play IS NULL
            THEN 1 
            ELSE 0 
        END) OVER (ORDER BY played_at) as session_id
    FROM plays_with_lag
),
track_pairs AS (
    SELECT DISTINCT
        a.session_id,
        a.track_id as track_a_id,
        a.track_name as track_a_name,
        b.track_id as track_b_id,
        b.track_name as track_b_name
    FROM session_assignment a
    JOIN session_assignment b 
        ON a.session_id = b.session_id 
        AND a.track_id < b.track_id
),
pair_counts AS (
    SELECT 
        track_a_id,
        track_a_name,
        track_b_id,
        track_b_name,
        COUNT(DISTINCT session_id) as sessions_together
    FROM track_pairs
    GROUP BY track_a_id, track_a_name, track_b_id, track_b_name
),
track_session_counts AS (
    SELECT 
        track_id,
        COUNT(DISTINCT session_id) as total_sessions
    FROM session_assignment
    GROUP BY track_id
)
SELECT 
    pc.track_a_name,
    pc.track_b_name,
    pc.sessions_together,
    tsc.total_sessions as track_a_total_sessions,
    ROUND(pc.sessions_together::FLOAT / tsc.total_sessions, 3) as cooccurrence_score
FROM pair_counts pc
JOIN track_session_counts tsc ON pc.track_a_id = tsc.track_id
WHERE pc.sessions_together >= 1
ORDER BY cooccurrence_score DESC, pc.sessions_together DESC
LIMIT 20;

-- Verify track_cooccurrence table created
SELECT 
    COUNT(*) as total_pairs,
    COUNT(DISTINCT user_id) as unique_users,
    MAX(cooccurrence_score) as max_score,
    MIN(cooccurrence_score) as min_score
FROM SPOTIFY_DATA.GOLD.TRACK_COOCCURRENCE;

-- View top 10 co-occurring track pairs
SELECT 
    track_a_name,
    track_a_artist,
    track_b_name,
    track_b_artist,
    sessions_together,
    track_a_total_sessions,
    cooccurrence_score
FROM SPOTIFY_DATA.GOLD.TRACK_COOCCURRENCE
ORDER BY cooccurrence_score DESC, sessions_together DESC
LIMIT 10;

-- ============================================================================
-- PHASE 5.3.2: ARTIST AFFINITY MODEL VALIDATION
-- ============================================================================
-- Purpose: Validate artist_affinity model after creation
-- Model Location: dbt/models/gold/artist_affinity.sql
-- Status: COMPLETE - Model created successfully on Dec 16, 2025
-- ============================================================================

-- Query 9: Artist Affinity Score Calculation Test
-- Calculates preference scores for each artist
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
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
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
        -- Normalized score: (weighted_plays * 0.6) + (unique_tracks * 0.4)
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
        -- Normalize to 0-100 scale
        ROUND(
            ((raw_score - MIN(raw_score) OVER ()) / 
             NULLIF((MAX(raw_score) OVER () - MIN(raw_score) OVER ()), 0)) * 100,
            2
        ) as affinity_score
    FROM artist_scores
)
SELECT 
    artist_name,
    total_plays,
    unique_tracks,
    weighted_plays,
    affinity_score
FROM normalized_scores
ORDER BY affinity_score DESC
LIMIT 20;

-- Query 10: Verify Table Stats
-- Confirms artist_affinity table created with expected data
SELECT 
    COUNT(*) as total_artists,
    COUNT(DISTINCT user_id) as unique_users,
    MAX(affinity_score) as max_score,
    MIN(affinity_score) as min_score,
    MAX(rank) as max_rank
FROM SPOTIFY_DATA.GOLD.ARTIST_AFFINITY;

-- Query 11: View Top 20 Artists by Affinity
-- Shows your strongest artist preferences
SELECT 
    rank,
    artist_name,
    total_plays,
    unique_tracks,
    weighted_plays,
    affinity_score
FROM SPOTIFY_DATA.GOLD.ARTIST_AFFINITY
ORDER BY rank
LIMIT 20;

-- ============================================================================
-- PHASE 5.3.3: TRACK RECOMMENDATIONS MODEL VALIDATION
-- ============================================================================
-- Purpose: Validate track_recommendations model after creation
-- Model Location: dbt/models/gold/track_recommendations.sql
-- Status: COMPLETE - Model created successfully on Dec 16, 2025
-- Note: May contain 0 rows due to cold-start (all co-occurrence tracks already played)
-- ============================================================================

-- Query 12: Track Recommendations Calculation Test
-- Combines co-occurrence + artist affinity for final recommendations
WITH played_tracks AS (
    SELECT DISTINCT 
        user_id,
        track_id
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
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
        ) * 100 as recommendation_score
    FROM SPOTIFY_DATA.GOLD.TRACK_COOCCURRENCE co
    LEFT JOIN SPOTIFY_DATA.GOLD.ARTIST_AFFINITY aa 
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
        r.recommendation_score
    FROM recommendations r
)
SELECT 
    recommended_track_name,
    recommended_artist,
    cooccurrence_score,
    artist_affinity_score,
    recommendation_score
FROM filtered_recommendations
ORDER BY recommendation_score DESC
LIMIT 20;

-- Check: Are all co-occurrence tracks already played?
WITH all_cooccurrence_tracks AS (
    SELECT DISTINCT track_b_id, track_b_name, track_b_artist
    FROM SPOTIFY_DATA.GOLD.TRACK_COOCCURRENCE
    WHERE user_id = 'sunilmakkar97'
),
played_tracks AS (
    SELECT DISTINCT track_id
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
    WHERE user_id = 'sunilmakkar97'
)
SELECT 
    COUNT(DISTINCT act.track_b_id) as total_cooccurrence_tracks,
    COUNT(DISTINCT pt.track_id) as tracks_already_played,
    COUNT(DISTINCT act.track_b_id) - COUNT(DISTINCT pt.track_id) as new_recommendations
FROM all_cooccurrence_tracks act
LEFT JOIN played_tracks pt ON act.track_b_id = pt.track_id;

-- Query 13: Verify Table Stats
-- Confirms track_recommendations table created (may be empty with limited data)
SELECT 
    COUNT(*) as total_recommendations,
    COUNT(DISTINCT user_id) as unique_users,
    MAX(recommendation_score) as max_score,
    MIN(recommendation_score) as min_score
FROM SPOTIFY_DATA.GOLD.TRACK_RECOMMENDATIONS;

-- Query 14: View Top 20 Recommendations (if any exist)
-- Shows personalized track recommendations
SELECT 
    rank,
    recommended_track_name,
    recommended_artist,
    cooccurrence_score,
    artist_affinity_score,
    recommendation_score,
    based_on_track,
    based_on_artist
FROM SPOTIFY_DATA.GOLD.TRACK_RECOMMENDATIONS
ORDER BY rank
LIMIT 20;

-- ============================================================================
-- END-TO-END RECOMMENDATION FLOW TESTING
-- ============================================================================
-- Purpose: Test complete recommendation pipeline
-- Tests: Co-occurrence → Artist Affinity → Final Recommendations
-- ============================================================================

-- Check track co-occurrence
SELECT COUNT(*) as cooccurrence_count FROM GOLD.TRACK_COOCCURRENCE;

-- Check artist affinity  
SELECT COUNT(*) as artist_count FROM GOLD.ARTIST_AFFINITY;

-- Check recommendations
SELECT COUNT(*) as recommendation_count FROM GOLD.TRACK_RECOMMENDATIONS;

-- 1. TRACK CO-OCCURRENCE: Verify track pairs identified
SELECT 
    TRACK_A_NAME,
    TRACK_B_NAME,
    TRACK_A_ARTIST,
    TRACK_B_ARTIST,
    SESSIONS_TOGETHER,
    COOCCURRENCE_SCORE
FROM GOLD.TRACK_COOCCURRENCE
ORDER BY COOCCURRENCE_SCORE DESC
LIMIT 10;

-- 2. ARTIST AFFINITY: Verify artist rankings with recency bias
SELECT 
    ARTIST_NAME,
    TOTAL_PLAYS,
    UNIQUE_TRACKS,
    WEIGHTED_PLAYS,
    AFFINITY_SCORE,
    RANK
FROM GOLD.ARTIST_AFFINITY
ORDER BY RANK
LIMIT 10;

-- 3. TRACK RECOMMENDATIONS: Verify personalized recommendations
SELECT 
    RECOMMENDED_TRACK_NAME,
    RECOMMENDED_ARTIST,
    RECOMMENDATION_SCORE,
    COOCCURRENCE_SCORE,
    ARTIST_AFFINITY_SCORE,
    BASED_ON_TRACK,
    BASED_ON_ARTIST
FROM GOLD.TRACK_RECOMMENDATIONS
ORDER BY RECOMMENDATION_SCORE DESC
LIMIT 10;

-- 4. PIPELINE VALIDATION: Verify data freshness and counts
SELECT 
    'Track Co-occurrence' as MODEL,
    COUNT(*) as ROW_COUNT,
    MAX(COOCCURRENCE_SCORE) as MAX_SCORE
FROM GOLD.TRACK_COOCCURRENCE
UNION ALL
SELECT 
    'Artist Affinity',
    COUNT(*),
    MAX(AFFINITY_SCORE)
FROM GOLD.ARTIST_AFFINITY
UNION ALL
SELECT 
    'Track Recommendations',
    COUNT(*),
    MAX(RECOMMENDATION_SCORE)
FROM GOLD.TRACK_RECOMMENDATIONS;

-- ============================================================================
-- END OF RECOMMENDATION VALIDATION QUERIES
-- ============================================================================
