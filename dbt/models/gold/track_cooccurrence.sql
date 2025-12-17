{{
    config(
        materialized='table',
        tags=['recommendations', 'gold']
    )
}}

-- ============================================================================
-- TRACK CO-OCCURRENCE MODEL
-- ============================================================================
-- Purpose: Identify tracks that are frequently played together in sessions
-- Logic: 1-hour session window, co-occurrence score = sessions_together / track_a_total_sessions
-- ============================================================================

WITH plays_with_lag AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        user_id,
        LAG(played_at) OVER (PARTITION BY user_id ORDER BY played_at) as previous_play
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
),

session_assignment AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        track_id,
        user_id,
        SUM(CASE 
            WHEN DATEDIFF(minute, previous_play, played_at) > 60 OR previous_play IS NULL
            THEN 1 
            ELSE 0 
        END) OVER (PARTITION BY user_id ORDER BY played_at) as session_id
    FROM plays_with_lag
),

track_pairs AS (
    SELECT DISTINCT
        a.user_id,
        a.session_id,
        a.track_id as track_a_id,
        a.track_name as track_a_name,
        a.artist_name as track_a_artist,
        b.track_id as track_b_id,
        b.track_name as track_b_name,
        b.artist_name as track_b_artist
    FROM session_assignment a
    JOIN session_assignment b 
        ON a.user_id = b.user_id
        AND a.session_id = b.session_id 
        AND a.track_id < b.track_id
),

pair_counts AS (
    SELECT 
        user_id,
        track_a_id,
        track_a_name,
        track_a_artist,
        track_b_id,
        track_b_name,
        track_b_artist,
        COUNT(DISTINCT session_id) as sessions_together
    FROM track_pairs
    GROUP BY user_id, track_a_id, track_a_name, track_a_artist, track_b_id, track_b_name, track_b_artist
),

track_session_counts AS (
    SELECT 
        user_id,
        track_id,
        COUNT(DISTINCT session_id) as total_sessions
    FROM session_assignment
    GROUP BY user_id, track_id
)

SELECT 
    pc.user_id,
    pc.track_a_id,
    pc.track_a_name,
    pc.track_a_artist,
    pc.track_b_id,
    pc.track_b_name,
    pc.track_b_artist,
    pc.sessions_together,
    tsc.total_sessions as track_a_total_sessions,
    ROUND(pc.sessions_together::FLOAT / tsc.total_sessions, 3) as cooccurrence_score,
    CURRENT_TIMESTAMP() as calculated_at
FROM pair_counts pc
JOIN track_session_counts tsc 
    ON pc.user_id = tsc.user_id 
    AND pc.track_a_id = tsc.track_id
WHERE pc.sessions_together >= 1
ORDER BY pc.user_id, cooccurrence_score DESC, pc.sessions_together DESC
