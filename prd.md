# Spotify Data Platform Recreation

**Owner:** Sunil  
**Timeline:** 4 weeks (Nov 18 - Dec 15)  
**Target:** Complete before Christmas

---

## PHASE 1: INFRASTRUCTURE & DATA INGESTION

**Duration:** Week 1 (40-50 hours)  
**Goal:** Events flowing from Spotify API → Kafka → S3

### Phase 1.1: Environment Setup ✅ COMPLETED
**Duration:** Days 1-2

#### Tasks:
- ✅ Create AWS account / use existing
- ✅ Set up S3 bucket with folder structure:
  - `spotify-data-lake/bronze/plays/`
  - `spotify-data-lake/bronze/skips/`
  - `spotify-data-lake/bronze/likes/`
- ✅ Create IAM user with S3 access, save credentials
- ✅ Sign up for Snowflake trial (30-day, $400 credits)
- ✅ Sign up for Confluent Cloud (Kafka), use free tier
- ✅ Set up local Python environment (venv)
- ✅ Install: `pip install confluent-kafka boto3 spotipy pandas pyarrow`

#### Deliverables:
- ✅ AWS S3 bucket accessible
- ✅ Snowflake account active
- ✅ Confluent Kafka cluster running
- ✅ Python environment ready

#### Definition of Done:
- ✅ Can upload test file to S3 from Python
- ✅ Can connect to Snowflake from Python
- ✅ Can see Kafka cluster in Confluent dashboard

---

### Phase 1.2: Spotify Event Generation (Days 3-4)

#### Tasks:
- ✅ Get Spotify API credentials (create app in Spotify Developer Dashboard)
- ✅ Write Python script to fetch your listening history
- ✅ Transform API response into standardized event format:
```python
{
  "event_id": "uuid",
  "user_id": "your_spotify_id",
  "event_type": "play",
  "track_id": "spotify_track_id",
  "track_name": "Blinding Lights",
  "artist_name": "The Weeknd",
  "album_name": "After Hours",
  "duration_ms": 200040,
  "played_at": "2025-11-18T14:23:15Z",
  "device_type": "desktop"
}
```
- ✅ Alternative: Create event simulator if API limits hit
- ✅  Produce events to Kafka topic `user_plays`

#### Deliverables:
- ✅ Python Kafka producer script
- ✅ Events visible in Confluent Cloud UI

#### Definition of Done:
- ✅ Can see 100+ events in Kafka topic
- ✅ Events have correct schema
- ✅ Can produce events on demand

---

### Phase 1.3: Kafka → S3 Consumer (Days 5-7)

#### Tasks:
- ✅ Write Kafka consumer in Python
- ✅ Consume events from `user_plays` topic
- ✅ Batch events (e.g., every 100 events or 60 seconds)
- ✅ Convert to Parquet format
- ✅ Write to S3 with partitioning:
  - `s3://spotify-data-lake/bronze/plays/year=2025/month=11/day=18/hour=14/part-001.parquet`
- ✅ Add error handling (retry logic, dead letter queue concept)
- ✅ Test: Run producer and consumer together

#### Deliverables:
- ✅ Kafka consumer script
- ✅ Parquet files in S3

#### Definition of Done:
- ✅ Events flow automatically: Kafka → S3
- ✅ Parquet files readable (verify with pandas)
- ✅ Files properly partitioned by date/hour
- ✅ Consumer can run for 1+ hours without crashing

**Phase 1 Milestone:** ✅ End-to-end data ingestion pipeline working

---

## PHASE 2: DATA WAREHOUSE & TRANSFORMATIONS

**Duration:** Week 2 (40-50 hours)  
**Goal:** Raw events transformed into analytics-ready tables

### Phase 2.1: Snowflake Setup (Days 8-9)

#### Tasks:
- ✅ Create Snowflake database: `SPOTIFY_DATA`
- ✅ Create schemas: `BRONZE`, `SILVER`, `GOLD`
- ✅ Create external stage pointing to S3:
```sql
CREATE STAGE bronze_stage
  URL = 's3://spotify-data-lake/bronze/'
  CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');
```
- ✅ Create external table for plays:
```sql
CREATE EXTERNAL TABLE bronze.plays
  WITH LOCATION = @bronze_stage/plays/
  FILE_FORMAT = (TYPE = PARQUET);
```
- ✅ Test query: `SELECT * FROM bronze.plays LIMIT 10;`
- ✅ Set up auto-suspend (1 min idle) to control costs
- ✅ Set warehouse size to X-Small

#### Deliverables:
- ✅ Snowflake database structure
- ✅ External tables querying S3 data

#### Definition of Done:
- ✅ Can query S3 Parquet files from Snowflake
- ✅ Warehouse auto-suspends (verify in usage dashboard)
- ✅ Costs tracking < $1/day

---

### Phase 2.2: DBT Project Setup (Days 10-11)

#### Tasks:
- ✅ Install DBT: `pip install dbt-snowflake`
- ✅  Initialize DBT project: `dbt init spotify_analytics`
- ✅  Configure `profiles.yml` with Snowflake connection
- ✅  Create folder structure:
  - `models/bronze/` (sources)
  - `models/silver/` (cleaned)
  - `models/gold/` (metrics)
- ✅  Define sources in `models/bronze/sources.yml`:
```yaml
sources:
  - name: bronze
    tables:
      - name: plays
```
- ✅  Test connection: `dbt debug`

#### Deliverables:
- ✅  DBT project initialized
- ✅  Connected to Snowflake

#### Definition of Done:
- ✅  `dbt debug` passes
- ✅  Can see bronze tables in DBT

---

### Phase 2.3: Silver Layer Transformations (Days 12-13)

#### Tasks:
- ✅ Create `models/silver/silver_plays.sql`:
  - Remove duplicates (same event_id)
  - Cast data types properly
  - Filter out invalid records (null user_id, negative duration)
  - Add data quality flags
```sql
SELECT DISTINCT
  event_id,
  user_id,
  event_type,
  track_id,
  track_name,
  artist_name,
  album_name,
  duration_ms,
  CAST(played_at AS TIMESTAMP) as played_at,
  device_type,
  CURRENT_TIMESTAMP() as processed_at
FROM {{ source('bronze', 'plays') }}
WHERE user_id IS NOT NULL
  AND duration_ms > 0
  AND duration_ms < 600000  -- less than 10 min (catches errors)
```

- ✅ Add DBT tests:
```yaml
models:
  - name: silver_plays
    columns:
      - name: event_id
        tests:
          - unique
          - not_null
      - name: user_id
        tests:
          - not_null
```
- ✅ Run: `dbt run --models silver_plays`
- ✅ Run: `dbt test --models silver_plays`

#### Deliverables:
- ✅ Silver layer model
- ✅ Passing DBT tests

#### Definition of Done:
- ✅ Silver table exists in Snowflake
- ✅ All DBT tests pass
- ✅ Can query clean data

---

### Phase 2.4: Gold Layer Analytics (Day 14)

#### Tasks:
- ✅ Create `models/gold/user_daily_listening.sql`:
```sql
SELECT
  user_id,
  DATE(played_at) as date,
  COUNT(*) as total_plays,
  COUNT(DISTINCT track_id) as unique_tracks,
  COUNT(DISTINCT artist_name) as unique_artists,
  SUM(duration_ms) / 60000.0 as total_minutes_listened,
  AVG(duration_ms) as avg_track_duration_ms
FROM {{ ref('silver_plays') }}
GROUP BY user_id, DATE(played_at)
```

- ✅ Create `models/gold/track_popularity.sql`:
```sql
SELECT
  track_id,
  track_name,
  artist_name,
  COUNT(*) as play_count,
  COUNT(DISTINCT user_id) as unique_listeners,
  AVG(duration_ms) as avg_listen_duration
FROM {{ ref('silver_plays') }}
GROUP BY track_id, track_name, artist_name
ORDER BY play_count DESC
```

- ✅ Create `models/gold/artist_stats.sql`
- ✅ Create `models/gold/device_usage.sql`
- ✅ Run all models: `dbt run`
- ✅ Generate docs: `dbt docs generate` and `dbt docs serve`

#### Deliverables:
- ✅ Gold layer metrics tables
- ✅ DBT documentation

#### Definition of Done:
- ✅ 3+ gold tables in Snowflake
- ✅ Can query your daily listening stats
- ✅ DBT docs viewable in browser

**Phase 2 Milestone:** ✅ Clean, analytics-ready data in Snowflake

---

## PHASE 3: ORCHESTRATION & AUTOMATION

**Duration:** Week 3 (40-50 hours)  
**Goal:** Pipeline runs automatically on schedule

### Phase 3.1: Airflow Setup (Days 15-17)

#### Tasks:
- ✅ Launch EC2 instance (t2.medium for Airflow, use free tier if available)
- ✅ Install Docker and Docker Compose
- ✅ Set up Airflow using Docker:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml'
docker-compose up -d
```
- ✅ Access Airflow UI: `http://ec2-ip:8080`
- ✅ Create connections:
  - AWS (for S3)
  - Snowflake
  - Kafka (Confluent)
- ✅ Test connections in UI

#### Deliverables:
- ✅ Airflow running on EC2
- ✅ Connections configured

#### Definition of Done:
- ✅ Can access Airflow UI
- ✅ Test connections successful

---

### Phase 3.2: Create Ingestion DAG (Days 18-19)

#### Tasks:
- ✅ Create `dags/spotify_ingestion_dag.py`
- ✅ Tasks:
  - Check Kafka topic has new messages
  - Run consumer (batch consume for 5 minutes)
  - Verify Parquet files landed in S3
  - Run data quality checks
  - Send success/failure alert
- ✅ Schedule: Every hour
- ✅ Add retry logic (3 retries, 5 min delay)
- ✅ Test run manually in Airflow UI

#### Deliverables:
- ✅ Ingestion DAG file
- ✅ DAG visible in Airflow

#### Definition of Done:
- ✅ DAG runs successfully
- ✅ Can see task logs
- ✅ S3 files updated after DAG run

---

### Phase 3.3: Create Transformation DAG (Day 20)

#### Tasks:
- ✅ Create `dags/dbt_transformation_dag.py`
- ✅ Tasks:
  - Check new data in bronze layer
  - Run `dbt run --models silver.*`
  - Run `dbt test --models silver.*`
  - Run `dbt run --models gold.*`
  - Run `dbt test --models gold.*`
  - Update dashboard metrics
- ✅ Schedule: Every 6 hours (or after ingestion DAG)
- ✅ Add task dependencies
- ✅ Test end-to-end

#### Deliverables:
- ✅ DBT transformation DAG
- ✅ Automated pipeline

#### Definition of Done:
- ✅ DBT runs via Airflow
- ✅ Gold tables update automatically
- ✅ Both DAGs run without manual intervention for 24 hours

**Phase 3 Milestone:** ✅ Fully automated data pipeline

---

## PHASE 4: ANALYTICS & POLISH

**Duration:** Week 4 (40-50 hours)  
**Goal:** Dashboard, documentation, demo-ready

### Phase 4.1: Dashboard (Days 24-25)

#### Tasks:
- ✅ Create Streamlit app: `dashboard.py`
- ✅ Connect to Snowflake
- ✅ Build pages:
  - **Overview:** Total plays, unique tracks, listening time
  - **Trends:** Line chart of plays over time
  - **Top Content:** Top 10 tracks, artists, albums
  - **Recommendations:** Your personalized suggestions
  - **Real-time:** Current activity (if streaming view)
- ✅ Add filters: Date range, device type
- ✅ Style with Streamlit themes
- ✅ Deploy to Streamlit Cloud (free)

#### Deliverables:
- ✅ Streamlit dashboard
- ✅ Live URL

#### Definition of Done:
- ✅ Dashboard loads in < 3 seconds
- ✅ All visualizations render correctly
- ✅ Shareable link works

---

Phase 4.2: Documentation (REVISED) - What's Left
Tasks:

 Create high-level architecture diagram (use draw.io or Lucidchart):

✅ Show all components: Event Generator → Kafka → S3 → Snowflake → DBT → Airflow → Streamlit
✅ Data flow arrows
✅ Label technologies used
✅ Save as: docs/architecture/system_architecture.png


 Write comprehensive README.md (root level):

✅ Project overview & purpose
✅ Architecture explanation (embed diagram)
✅ Complete tech stack with justifications
✅ Data flow walkthrough
✅ All 4 phase summaries (paste from your notes)
✅ How to run locally (setup instructions)
✅ Screenshots/demo links
✅ Lessons learned
Future improvements


 Create SETUP.md (deployment guide):

AWS setup (EC2, S3, IAM)
Snowflake setup (warehouse, database, schemas)
Kafka installation
DBT configuration
Airflow deployment
Streamlit deployment


Deliverables:

✅ System architecture diagram
✅ Complete README.md
 SETUP.md deployment guide
 Well-commented codebase
 (Optional) Blog post draft

Definition of Done:

 Someone can clone the repo and understand the entire project
 Setup instructions are clear enough for someone to replicate
 All documentation is professional and portfolio-ready


**Phase 4 Milestone:** ✅ PROJECT COMPLETE - INTERVIEW READY

---

### Phase 5: Real Spotify API Integration

---

### Phase 5.1: Spotify API Setup & Authentication

#### Tasks:
- ✅ Create Spotify Developer App:
  - Go to https://developer.spotify.com/dashboard
  - Create new app
  - Get Client ID and Client Secret
  - Add redirect URI: `http://localhost:8888/callback`
- ✅ Create `src/spotify_auth.py`:
  - Implement authorization code flow
  - Method: `get_authorization_url()` - generates Spotify login URL
  - Method: `get_access_token()` - exchanges code for token
  - Method: `refresh_access_token()` - handles token refresh
- ✅ Create token storage:
  - Store tokens in `~/.spotify_tokens.json` or database
  - Include: access_token, refresh_token, expires_at
- ✅ Build authorization flow:
  - User opens browser to authorize
  - Callback receives authorization code
  - Exchange code for tokens
  - Store tokens securely
- ✅ Test OAuth flow:
  - Authorize your Spotify account
  - Verify tokens are stored
  - Verify tokens refresh automatically
  - Test API calls work with stored tokens

---

### Phase 5.2: Historical Data Pipeline

#### Tasks:
- ✅ Create `src/spotify_api_client.py`:
  - Implement SpotifyAPIClient class
  - Method: `get_recently_played()` - fetches last 50 tracks
  - Method: `get_track_details()` - enriches with artist/album info
  - Handle rate limiting and retries
  - Load tokens from storage and auto-refresh
- ✅ Replace EventSimulator in pipeline:
  - Update `dags/spotify_pipeline_basic.py`
  - Change `generate_events` task to call `spotify_api_client.py`
  - Remove synthetic data generation logic
  - Keep existing Kafka → S3 → Snowflake flow
- ✅ Update `.env` file:
  - Add `SPOTIFY_CLIENT_ID`
  - Add `SPOTIFY_CLIENT_SECRET`
  - Add `SPOTIFY_REDIRECT_URI`
- ✅ Schedule hourly runs:
  - Enable DAG schedule in Airflow (every hour)
  - Fetch recently played tracks hourly
  - Build historical listening dataset
- ✅ Test historical pipeline:
  - Run DAG manually, verify real Spotify data appears
  - Check Bronze → Silver → Gold layers update correctly
  - Verify dashboard displays real listening history

---

---

## Pre-Phase 5.3: Cleanup Unused Audio Features Files

**Goal:** Remove audio features scripts and models that were built but cannot be used due to Spotify API restrictions.

**Why:** Switching from audio features API (blocked) to collaborative filtering (behavioral patterns).

**Estimated Time:** 10 minutes

---

### Tasks:
- [ ] Remove unused Python scripts:
```bash
  rm ~/spotify-data-pipeline/src/fetch_audio_features.py
  rm ~/spotify-data-pipeline/src/generate_recommendations.py
```
- [ ] Remove unused DBT models:
```bash
  rm ~/spotify-data-pipeline/dbt/models/silver/track_audio_features.sql
  rm ~/spotify-data-pipeline/dbt/models/gold/user_listening_profile.sql
  rm ~/spotify-data-pipeline/dbt/models/gold/track_recommendations.sql
```
- [ ] Remove audio features source from DBT sources file:
  - Edit `~/spotify-data-pipeline/dbt/models/bronze/sources.yml`
  - Delete the `track_audio_features` source entry
- [ ] Commit cleanup to Git:
```bash
  git add -A
  git commit -m "Remove unused audio features files - switching to collaborative filtering"
  git push origin main
```

---

## Phase 5.3: Collaborative Filtering Recommendation Engine

**Goal:** Build a recommendation system based on YOUR listening patterns without requiring external APIs.

**Approach:** Analyze which tracks you play together (co-occurrence), identify preferred artists (affinity), and recommend unplayed tracks based on these behavioral patterns.

**Why This Approach:**
- Uses real production data already in Snowflake
- No dependency on Spotify API Extended Quota approval
- Demonstrates stronger data science skills (feature engineering vs API calls)
- Industry-standard approach (Netflix, Amazon, YouTube use collaborative filtering)
- Self-contained and production-ready

**Estimated Time:** 2.5 hours

---

### Phase 5.3.1: Track Co-Occurrence Analysis

**What it does:** Identify which tracks you frequently play together in the same listening sessions.

**Concept:** If you play Track A and Track B in the same hour 70% of the time, they have high co-occurrence (0.70 score).

#### Tasks:
- [ ] Create `dbt/models/gold/track_cooccurrence.sql`:
  - Define listening session as tracks played within 1 hour of each other
  - Calculate co-occurrence scores for track pairs
  - Formula: `cooccurrence_score = count(sessions with both tracks) / count(sessions with track A)`
  - Store pairs: (track_id_a, track_id_b, cooccurrence_score)
  - Filter to track pairs that appeared together at least 2 times
  - Return top 20 co-occurring tracks per source track

#### SQL Logic:
```sql
{{
    config(
        materialized='table',
        tags=['gold', 'recommendations']
    )
}}

/*
    Track Co-Occurrence Analysis
    
    Identifies tracks that are frequently played together in the same listening sessions.
    Used as foundation for collaborative filtering recommendations.
*/

WITH user_plays AS (
    SELECT 
        user_id,
        track_id,
        track_name,
        artist_name,
        played_at,
        DATE_TRUNC('hour', played_at) as session_hour
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
),

session_pairs AS (
    -- Find all pairs of tracks played in same session
    SELECT 
        p1.track_id as track_a,
        p1.track_name as track_a_name,
        p1.artist_name as artist_a,
        p2.track_id as track_b,
        p2.track_name as track_b_name,
        p2.artist_name as artist_b,
        p1.session_hour
    FROM user_plays p1
    JOIN user_plays p2 
        ON p1.session_hour = p2.session_hour
        AND p1.track_id < p2.track_id  -- Avoid duplicates and self-pairs
),

cooccurrence_counts AS (
    -- Count how many sessions each pair appeared in together
    SELECT 
        track_a,
        track_a_name,
        artist_a,
        track_b,
        track_b_name,
        artist_b,
        COUNT(DISTINCT session_hour) as sessions_together
    FROM session_pairs
    GROUP BY track_a, track_a_name, artist_a, track_b, track_b_name, artist_b
),

track_session_counts AS (
    -- Count total sessions for each track
    SELECT 
        track_id,
        COUNT(DISTINCT session_hour) as total_sessions
    FROM user_plays
    GROUP BY track_id
)

-- Calculate co-occurrence scores
SELECT 
    cc.track_a,
    cc.track_a_name,
    cc.artist_a,
    cc.track_b,
    cc.track_b_name,
    cc.artist_b,
    cc.sessions_together,
    tsc.total_sessions as track_a_total_sessions,
    -- Co-occurrence score: what % of track_a sessions also included track_b
    cc.sessions_together::FLOAT / tsc.total_sessions as cooccurrence_score,
    RANK() OVER (PARTITION BY cc.track_a ORDER BY cc.sessions_together DESC) as rank
FROM cooccurrence_counts cc
JOIN track_session_counts tsc ON cc.track_a = tsc.track_id
WHERE cc.sessions_together >= 2  -- Must appear together at least twice
QUALIFY rank <= 20  -- Top 20 co-occurring tracks per source track
ORDER BY cc.track_a, cooccurrence_score DESC
```

**Estimated Time:** 45 minutes

---

### Phase 5.3.2: Artist Affinity Scores

**What it does:** Calculate preference scores for each artist based on play frequency, recency, and track diversity.

**Concept:** Artists you play more often, more recently, and with more variety of tracks get higher affinity scores.

#### Tasks:
- [ ] Create `dbt/models/gold/artist_affinity.sql`:
  - Calculate artist scores based on:
    - Total plays (quantity)
    - Recent plays weighted 2x (recency - last 7 days)
    - Unique tracks per artist (diversity)
  - Normalize scores to 0-100 scale
  - Rank top 50 artists by affinity

#### SQL Logic:
```sql
{{
    config(
        materialized='table',
        tags=['gold', 'recommendations']
    )
}}

/*
    Artist Affinity Scores
    
    Calculates user preference scores for artists based on:
    - Play frequency (more plays = higher score)
    - Recency (recent plays weighted higher)
    - Track diversity (more unique tracks = higher engagement)
*/

WITH artist_metrics AS (
    SELECT 
        user_id,
        artist_name,
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks,
        MAX(played_at) as last_played_at,
        
        -- Weighted plays: recent plays count for more
        SUM(CASE 
            WHEN played_at >= CURRENT_DATE - 7 THEN 2.0  -- Last week: 2x weight
            WHEN played_at >= CURRENT_DATE - 30 THEN 1.5 -- Last month: 1.5x weight
            ELSE 1.0  -- Older: 1x weight
        END) as weighted_plays
        
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
    GROUP BY user_id, artist_name
),

scored_artists AS (
    SELECT 
        user_id,
        artist_name,
        total_plays,
        unique_tracks,
        last_played_at,
        weighted_plays,
        
        -- Normalize weighted plays to 0-100 scale
        (weighted_plays / MAX(weighted_plays) OVER ()) * 100 as affinity_score,
        
        RANK() OVER (ORDER BY weighted_plays DESC) as affinity_rank
        
    FROM artist_metrics
)

SELECT 
    user_id,
    artist_name,
    total_plays,
    unique_tracks,
    last_played_at,
    ROUND(affinity_score, 2) as affinity_score,
    affinity_rank
FROM scored_artists
WHERE affinity_rank <= 50  -- Top 50 artists
ORDER BY affinity_score DESC
```

**Estimated Time:** 30 minutes

---

### Phase 5.3.3: Generate Track Recommendations

**What it does:** Combine co-occurrence patterns and artist affinity to recommend unplayed tracks.

**Algorithm:**
1. Start with tracks you've played frequently (>3 times)
2. Find tracks that co-occur with your frequent tracks
3. Boost score if co-occurring track is by high-affinity artist
4. Filter out tracks you've already played
5. Calculate final recommendation score: `(cooccurrence_score * 0.7) + (artist_affinity_score/100 * 0.3)`
6. Return top 50 recommendations

#### Tasks:
- [ ] Create `dbt/models/gold/track_recommendations.sql`:
  - Query top tracks from your listening history (>3 plays)
  - Join with `track_cooccurrence` to find candidate recommendations
  - Join with `artist_affinity` to boost scores for preferred artists
  - Filter out already-played tracks
  - Calculate weighted recommendation scores
  - Include recommendation reasoning
  - Return top 50 with metadata

#### SQL Logic:
```sql
{{
    config(
        materialized='table',
        tags=['gold', 'recommendations']
    )
}}

/*
    Track Recommendations - Collaborative Filtering
    
    Recommends unplayed tracks based on:
    - Co-occurrence with user's frequently played tracks
    - Artist affinity (preference for certain artists)
    
    Recommendation Score = (co-occurrence * 0.7) + (artist affinity * 0.3)
*/

WITH user_frequent_tracks AS (
    -- Tracks user plays frequently (seed tracks for recommendations)
    SELECT 
        track_id,
        track_name,
        artist_name,
        COUNT(*) as play_count
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
    GROUP BY track_id, track_name, artist_name
    HAVING play_count >= 3  -- Played at least 3 times
),

user_played_tracks AS (
    -- All tracks user has played (to exclude from recommendations)
    SELECT DISTINCT track_id
    FROM {{ ref('silver_plays') }}
    WHERE user_id = 'sunilmakkar97'
),

candidate_recommendations AS (
    -- Find tracks that co-occur with user's frequent tracks
    SELECT 
        co.track_b as recommended_track_id,
        co.track_b_name as track_name,
        co.artist_b as artist_name,
        MAX(co.cooccurrence_score) as max_cooccurrence_score,
        COUNT(DISTINCT co.track_a) as num_source_tracks,
        AVG(co.cooccurrence_score) as avg_cooccurrence_score
    FROM {{ ref('track_cooccurrence') }} co
    INNER JOIN user_frequent_tracks uft 
        ON co.track_a = uft.track_id
    WHERE co.track_b NOT IN (SELECT track_id FROM user_played_tracks)
    GROUP BY co.track_b, co.track_b_name, co.artist_b
),

recommendations_with_affinity AS (
    -- Add artist affinity scores
    SELECT 
        cr.recommended_track_id as track_id,
        cr.track_name,
        cr.artist_name,
        cr.max_cooccurrence_score,
        cr.avg_cooccurrence_score,
        cr.num_source_tracks,
        COALESCE(aa.affinity_score, 0) as artist_affinity_score,
        
        -- Final recommendation score (weighted combination)
        (cr.avg_cooccurrence_score * 0.7 + COALESCE(aa.affinity_score, 0)/100 * 0.3) * 100 
            as recommendation_score,
        
        -- Recommendation reasoning
        CASE 
            WHEN aa.affinity_score >= 70 THEN 'High affinity artist + Often played together'
            WHEN aa.affinity_score >= 40 THEN 'Moderate affinity artist + Often played together'
            WHEN cr.num_source_tracks >= 3 THEN 'Frequently co-occurs with multiple tracks you love'
            ELSE 'Often played together with tracks you enjoy'
        END as recommendation_reason
        
    FROM candidate_recommendations cr
    LEFT JOIN {{ ref('artist_affinity') }} aa 
        ON cr.artist_name = aa.artist_name
)

-- Final recommendations
SELECT 
    'sunilmakkar97' as user_id,
    track_id,
    track_name,
    artist_name,
    ROUND(recommendation_score, 2) as recommendation_score,
    recommendation_reason,
    ROUND(max_cooccurrence_score, 2) as cooccurrence_score,
    ROUND(artist_affinity_score, 2) as artist_score,
    num_source_tracks,
    RANK() OVER (ORDER BY recommendation_score DESC) as recommendation_rank,
    CURRENT_TIMESTAMP() as generated_at
FROM recommendations_with_affinity
WHERE recommendation_score > 0
QUALIFY recommendation_rank <= 50  -- Top 50 recommendations
ORDER BY recommendation_score DESC
```

**Estimated Time:** 1 hour

---

### Phase 5.3.4: Update Airflow DAG

**What it does:** Integrate recommendation models into production pipeline.

#### Tasks:
- [ ] Update `dags/spotify_pipeline_basic.py`:
  - Add new DBT task: `dbt_run_recommendations`
  - Task runs models: `track_cooccurrence`, `artist_affinity`, `track_recommendations`
  - Place after existing `dbt_run_gold` task
  - Update task dependencies:
```python
    dbt_run_gold >> dbt_run_recommendations >> dbt_test >> log_success
```

#### Code Changes:
```python
# Add new task after dbt_run_gold
dbt_run_recommendations = BashOperator(
    task_id='dbt_run_recommendations',
    bash_command='cd /opt/airflow && dbt run --project-dir dbt --profiles-dir dbt --models track_cooccurrence artist_affinity track_recommendations',
    dag=dag
)

# Update dependencies
dbt_run_gold >> dbt_run_recommendations >> dbt_test >> log_success
```

- [ ] Test in Airflow:
  - Trigger manual DAG run
  - Verify all 3 new models compile and run successfully
  - Check logs for any errors
  - Verify execution time (should be <2 minutes for recommendation models)

**Estimated Time:** 20 minutes

---

### Phase 5.3.5: Verify Recommendations in Snowflake

**What it does:** Validate that recommendations are generated correctly and make sense.

#### Tasks:
- [ ] Query Snowflake to view your recommendations:
```sql
  SELECT 
      track_name,
      artist_name,
      recommendation_score,
      recommendation_reason,
      cooccurrence_score,
      artist_score,
      recommendation_rank
  FROM SPOTIFY_DATA.GOLD.TRACK_RECOMMENDATIONS
  ORDER BY recommendation_rank
  LIMIT 20;
```

- [ ] Sanity check recommendations:
  - Do recommended artists match your listening preferences?
  - Are recommended tracks ones you haven't played before?
  - Do the scores make sense (higher for more relevant)?
  - Does the recommendation reasoning make sense?

- [ ] Verify co-occurrence table:
```sql
  SELECT 
      track_a_name,
      artist_a,
      track_b_name,
      artist_b,
      cooccurrence_score
  FROM SPOTIFY_DATA.GOLD.TRACK_COOCCURRENCE
  WHERE track_a = 'YOUR_FAVORITE_TRACK_ID'
  ORDER BY cooccurrence_score DESC
  LIMIT 10;
```

- [ ] Verify artist affinity:
```sql
  SELECT 
      artist_name,
      total_plays,
      unique_tracks,
      affinity_score,
      affinity_rank
  FROM SPOTIFY_DATA.GOLD.ARTIST_AFFINITY
  ORDER BY affinity_rank
  LIMIT 20;
```

- [ ] Document findings:
  - Take screenshot of top 10 recommendations
  - Note: "Recommendations based on collaborative filtering using behavioral listening patterns"
  - Save example output for portfolio/interviews

**Estimated Time:** 15 minutes

---

## Phase 6: Near Real-Time Recommendations

**Goal:** Build real-time recommendation system that updates recommendations as you play new songs.

**Architecture:**
1. **Poller:** Detects what song is currently playing (polls every 5 seconds)
2. **API:** Serves recommendations for currently playing track (FastAPI + Redis cache)
3. **Dashboard:** Displays current song + live recommendations (auto-refreshes every 5 seconds)

**Estimated Time:** 5 hours total

---

### Phase 6.1: Currently Playing Poller

**What it does:** Continuously poll Spotify to detect when you start playing a new song.

**How it works:**
- Polls `/v1/me/player/currently-playing` endpoint every 5 seconds
- Compares current track_id with previous track_id
- When track changes, logs the event and triggers recommendation fetch
- Runs as background daemon process

#### Tasks:
- [ ] Create `src/spotify_poller.py`:
  - Import SpotifyClient for authenticated API calls
  - Poll currently-playing endpoint every 5 seconds
  - Detect track changes (new track_id different from last)
  - Log each track change with timestamp
  - Store current playback state in database
  - Handle errors gracefully (connection issues, rate limits)
  - Use asyncio or threading for non-blocking execution

#### Python Implementation Structure:
```python
import time
import logging
from datetime import datetime
from src.spotify_client import SpotifyClient
from config import SNOWFLAKE_* # Import Snowflake config

class SpotifyPoller:
    def __init__(self):
        self.spotify = SpotifyClient()
        self.current_track_id = None
        self.last_check = None
        
    def get_currently_playing(self):
        """Fetch currently playing track"""
        # Call Spotify API
        # Return track info or None
        
    def detect_track_change(self, current_track):
        """Check if track has changed"""
        # Compare with self.current_track_id
        # Return True if changed
        
    def store_current_track(self, track_info):
        """Store in Snowflake BRONZE.currently_playing table"""
        # Insert/update current playback state
        
    def poll_loop(self):
        """Main polling loop"""
        while True:
            try:
                current = self.get_currently_playing()
                if self.detect_track_change(current):
                    self.store_current_track(current)
                    logging.info(f"Track changed: {current['track_name']}")
                time.sleep(5)  # Poll every 5 seconds
            except Exception as e:
                logging.error(f"Polling error: {e}")
                time.sleep(10)  # Back off on error
```

- [ ] Create background process:
  - Create systemd service file (Linux) or launchd (Mac)
  - Configure to start on boot
  - Add restart on failure
  - Alternative: Use `nohup` or `screen` for simpler deployment

- [ ] Create Snowflake table for current playback:
```sql
  CREATE TABLE IF NOT EXISTS SPOTIFY_DATA.BRONZE.currently_playing (
      user_id VARCHAR(255),
      track_id VARCHAR(255),
      track_name VARCHAR(500),
      artist_name VARCHAR(500),
      album_name VARCHAR(500),
      started_at TIMESTAMP_NTZ,
      detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY (user_id, detected_at)
  );
```

- [ ] Test poller:
  - Run poller script: `python src/spotify_poller.py`
  - Play Spotify on your phone/computer
  - Skip between songs
  - Verify log shows track changes within 5-10 seconds
  - Check Snowflake table has current playback data
  - Test error handling (disconnect internet, stop Spotify)

**Estimated Time:** 1.5 hours

---

### Phase 6.2: Real-Time Recommendation API

**What it does:** FastAPI service that returns recommendations for a given track_id with sub-second response times.

**Changes from original PRD:**
- Queries `GOLD.track_cooccurrence` instead of `track_similarity`
- Also queries `GOLD.artist_affinity` to boost recommendation scores
- Same caching strategy (Redis with 1-hour TTL)

#### Tasks:
- [ ] Build FastAPI application - Create `src/recommendation_api.py`:
  - Import FastAPI, Snowflake connector, Redis client
  - Endpoint: `GET /recommendations/{track_id}`
  - Query `SPOTIFY_DATA.GOLD.track_cooccurrence` for co-occurring tracks
  - Query `SPOTIFY_DATA.GOLD.artist_affinity` for artist scores
  - Calculate combined recommendation scores
  - Return JSON with top 10 recommendations
  
#### API Response Format:
```json
{
  "track_id": "currently_playing_track_id",
  "track_name": "Current Track Name",
  "artist_name": "Current Artist",
  "recommendations": [
    {
      "track_id": "recommended_track_123",
      "track_name": "Recommended Song",
      "artist_name": "Artist Name",
      "recommendation_score": 85.5,
      "reason": "Often played together",
      "cooccurrence_score": 0.72,
      "artist_affinity": 88.0
    },
    // ... 9 more recommendations
  ],
  "generated_at": "2025-12-11T20:30:00Z",
  "cache_hit": true
}
```

#### FastAPI Implementation Structure:
```python
from fastapi import FastAPI, HTTPException
import snowflake.connector
import redis
import json
from typing import List, Dict
from config import SNOWFLAKE_*, REDIS_HOST

app = FastAPI()
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_recommendations_from_db(track_id: str) -> List[Dict]:
    """Query Snowflake for recommendations"""
    conn = snowflake.connector.connect(...)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        co.track_b as track_id,
        co.track_b_name as track_name,
        co.artist_b as artist_name,
        co.cooccurrence_score,
        aa.affinity_score as artist_affinity,
        (co.cooccurrence_score * 0.7 + COALESCE(aa.affinity_score, 0)/100 * 0.3) * 100 
            as recommendation_score
    FROM GOLD.track_cooccurrence co
    LEFT JOIN GOLD.artist_affinity aa ON co.artist_b = aa.artist_name
    WHERE co.track_a = %s
    ORDER BY recommendation_score DESC
    LIMIT 10
    """
    
    cursor.execute(query, (track_id,))
    # Return formatted results
    
@app.get("/recommendations/{track_id}")
async def get_recommendations(track_id: str):
    # Check cache first
    cached = redis_client.get(f"rec:{track_id}")
    if cached:
        return json.loads(cached)
    
    # Cache miss - query database
    recommendations = get_recommendations_from_db(track_id)
    
    # Store in cache (1 hour TTL)
    redis_client.setex(
        f"rec:{track_id}", 
        3600,  # 1 hour
        json.dumps(recommendations)
    )
    
    return recommendations

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
```

- [ ] Add caching layer:
  - Install Redis on EC2: `sudo apt install redis-server`
  - Install Redis Python client: `pip install redis`
  - Cache key format: `rec:{track_id}`
  - TTL: 1 hour (3600 seconds)
  - Cache hit should return in <100ms
  - Cache miss queries Snowflake (~300-500ms)

- [ ] Deploy API on EC2:
  - Install FastAPI: `pip install fastapi uvicorn`
  - Run with Uvicorn: `uvicorn recommendation_api:app --host 0.0.0.0 --port 8000`
  - Configure as systemd service for auto-restart
  - Expose port 8000 in EC2 security group
  - Access at: `http://YOUR_EC2_IP:8000`

- [ ] Test API:
  - Test health endpoint: `curl http://YOUR_EC2_IP:8000/health`
  - Test recommendations with real track ID:
```bash
    curl http://YOUR_EC2_IP:8000/recommendations/2aTf0R0TQCJJKcb0ipszD2
```
  - Verify response time <500ms (with cold cache)
  - Verify response time <100ms (with warm cache)
  - Test cache expiration (wait 1 hour, verify re-query)
  - Verify recommendations match Snowflake data

**Estimated Time:** 2 hours

---

### Phase 6.3: Live Dashboard Integration

**What it does:** Real-time Streamlit dashboard that shows what you're currently playing and updates recommendations every 5 seconds.

**No changes from original PRD** - Dashboard logic remains the same, just consumes recommendations from new collaborative filtering source.

#### Tasks:
- [ ] Update Streamlit dashboard - Add new page: "Now Playing":
  - Create `pages/3_Now_Playing.py` in Streamlit app
  - Poll Snowflake `BRONZE.currently_playing` table every 5 seconds
  - Display currently playing track with album art
  - Call recommendation API for current track
  - Display top 10 recommendations below current track
  - Show recommendation scores and reasons

#### Streamlit Page Structure:
```python
import streamlit as st
import requests
import time
from datetime import datetime

st.set_page_config(page_title="Now Playing", layout="wide")
st.title("🎵 Now Playing - Live Recommendations")

# Auto-refresh every 5 seconds
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# Fetch currently playing
def get_currently_playing():
    # Query BRONZE.currently_playing from Snowflake
    # Return latest track
    
# Fetch recommendations from API
def get_recommendations(track_id):
    response = requests.get(f"http://YOUR_EC2_IP:8000/recommendations/{track_id}")
    return response.json()

current_track = get_currently_playing()

if current_track:
    st.subheader("Currently Playing")
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Album art (if available)
        st.image(current_track['album_art_url'])
    
    with col2:
        st.markdown(f"### {current_track['track_name']}")
        st.markdown(f"**{current_track['artist_name']}**")
        st.markdown(f"*{current_track['album_name']}*")
    
    # Get recommendations
    recommendations = get_recommendations(current_track['track_id'])
    
    st.subheader("Recommended Based on This Track")
    
    for idx, rec in enumerate(recommendations['recommendations'], 1):
        with st.expander(f"{idx}. {rec['track_name']} - {rec['artist_name']} ({rec['recommendation_score']:.1f})"):
            st.write(f"**Reason:** {rec['reason']}")
            st.progress(rec['recommendation_score'] / 100)
            st.write(f"Co-occurrence Score: {rec['cooccurrence_score']:.2f}")
            st.write(f"Artist Affinity: {rec['artist_affinity']:.1f}")
            
            # Spotify link
            spotify_url = f"https://open.spotify.com/track/{rec['track_id']}"
            st.markdown(f"[▶ Play on Spotify]({spotify_url})")

# Auto-refresh
time.sleep(5)
st.rerun()
```

- [ ] Add auto-refresh functionality:
  - Use `st.rerun()` to refresh page every 5 seconds
  - Display "Last updated: X seconds ago" timestamp
  - Add manual "Refresh Now" button for immediate update
  - Handle errors gracefully (API down, no current playback)

- [ ] Enhance UI:
  - Fetch album art from Spotify API using track_id
  - Display as large image next to current track
  - Add "Play on Spotify" links that open Spotify app/web
  - Show recommendation scores as progress bars (0-100%)
  - Display recommendation reasoning (e.g., "Often played together")
  - Highlight when recommendations change (flash/color change)
  - Add metrics: Total recommendations generated, Cache hit rate

- [ ] Optional: Audio previews:
  - Fetch `preview_url` from Spotify API for each recommendation
  - Embed 30-second preview using `st.audio()`
  - Add play/pause controls for each recommendation
  - Note: Not all tracks have previews available

- [ ] Test live dashboard:
  - Run dashboard: `streamlit run app.py`
  - Play Spotify on phone/computer
  - Skip to new song
  - Verify dashboard updates within 5-10 seconds
  - Check recommendations refresh when song changes
  - Verify recommendation scores and reasons display correctly
  - Test "Play on Spotify" links open correctly
  - Ensure dashboard doesn't lag or freeze during auto-refresh

**Estimated Time:** 1.5 hours

---

## Summary of Changes from Original PRD

### What Stayed the Same:
- **Phase 6.1 (Poller):** Identical - detecting currently playing track
- **Phase 6.3 (Dashboard):** Identical UI/UX - just displays different recommendation source
- Overall real-time architecture and data flow

### What Changed:
- **Phase 5.3:** Switched from audio features API to collaborative filtering
  - Removed: Spotify audio features fetch, user listening profile based on audio characteristics
  - Added: Track co-occurrence analysis, artist affinity scores, behavioral recommendations
- **Phase 6.2 (API):** Query different Snowflake tables
  - Old: `track_similarity` (audio features based)
  - New: `track_cooccurrence` + `artist_affinity` (behavioral based)

### Why These Changes:
- **Spotify API Restriction:** Audio features endpoint blocked for Development Mode apps
- **Extended Quota Requirements:** Must be registered business with 250K+ MAU (not feasible for portfolio project)
- **Better Approach:** Collaborative filtering demonstrates stronger data science skills
- **Production Ready:** Self-contained system with no external API dependencies
- **Industry Standard:** Same approach used by Netflix, Amazon, YouTube

### Total Time Estimates:
- **Pre-Phase 5.3:** 10 minutes
- **Phase 5.3:** 2.5 hours
- **Phase 6.1:** 1.5 hours
- **Phase 6.2:** 2 hours
- **Phase 6.3:** 1.5 hours
- **Total:** ~7.5 hours

---

## WEEKLY CHECKPOINTS

### End of Week 1:
- ✅ Can produce 1000+ events to Kafka
- ✅ Events automatically land in S3 as Parquet
- ✅ Can query files from Snowflake

### End of Week 2:
- ✅ DBT models transform bronze → silver → gold
- ✅ All tests passing
- ✅ Gold tables have meaningful metrics

### End of Week 3:
- ✅ Airflow running both DAGs
- ✅ Pipeline runs automatically every hour
- ✅ No manual intervention needed for 24 hours

### End of Week 4:
- ✅ Dashboard live and functional
- ✅ README complete

### End of Week 5:
- [ ] Spotify API connected and OAuth implemented
- [ ] Historical data pipeline running hourly with real Spotify data
- [ ] Simple recommendation engine built and deployed
- [ ] Dashboard shows recommendations based on listening history

### End of Week 6:
- [ ] Currently playing poller running continuously
- [ ] Real-time recommendation API deployed and cached
- [ ] Live dashboard showing "Now Playing" with recommendations
- [ ] Recommendations update within 5-10 seconds of playing new song
- [ ] Full near real-time user experience implemented


## LAST THING TO DO:

AFTER ALL PROJECT IS DONE. ALL DOCUMENTATION IS DONE. WE NEED:
TESTS TO BE WRITTEN

---

## RISK MITIGATION

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Kafka learning curve | High | Medium | Watch 2-3 hours tutorials first, use Confluent docs |
| AWS costs spike | Medium | High | Set billing alerts at $10, $20, $50 |
| Snowflake trial expires | Medium | Medium | Start Week 2 to maximize trial period |
| Airflow setup issues | Medium | Medium | Use Docker Compose (easier than manual install) |
| Scope creep | High | High | Stick to PRD, save "nice-to-haves" for post-Christmas |

---

## OPTIONAL ENHANCEMENTS (Post-Christmas)

If you finish early or want to add later:
- [ ] Add more event types (skips, likes, playlist adds)
- [ ] Stream processing with Kafka Streams/Flink
- [ ] More sophisticated ML recommendations
- [ ] Data quality monitoring dashboard
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Infrastructure as Code (Terraform)

---

## ADDITIONAL RESOURCES

### Learning Resources

**Kafka:**
- [Confluent Kafka Python docs](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Kafka Quickstart](https://kafka.apache.org/quickstart)

**Snowflake:**
- [Snowflake for Data Engineers course (free)](https://learn.snowflake.com)
- [External tables guide](https://docs.snowflake.com/en/user-guide/tables-external-intro)

**DBT:**
- [DBT Tutorial](https://docs.getdbt.com/tutorial/learning-more/getting-started)
- [Best practices](https://docs.getdbt.com/guides/best-practices)

**Airflow:**
- [Official tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
- [Astronomer guides](https://www.astronomer.io/guides/)

**Spotify API:**
- [API Reference](https://developer.spotify.com/documentation/web-api)

---

## TROUBLESHOOTING GUIDE

### "Kafka consumer not receiving messages"
- Check topic name matches
- Verify consumer group ID
- Check Confluent Cloud dashboard for errors
- Ensure `auto.offset.reset='earliest'` for testing

### "Snowflake costs running up"
- Check warehouse is auto-suspending (1 min idle)
- Use X-Small warehouse size
- Query the `QUERY_HISTORY` view to see expensive queries
- Suspend manually when not using: `ALTER WAREHOUSE my_wh SUSPEND;`

### "DBT tests failing"
- Run in debug mode: `dbt run --debug`
- Check source data has records
- Verify Snowflake connection in `profiles.yml`
- Look at compiled SQL in `target/` folder

### "Airflow DAG not running"
- Check DAG is unpaused (toggle in UI)
- Verify schedule interval syntax
- Check task logs for errors
- Ensure connections are configured

### "S3 access denied"
- Check IAM permissions include `s3:PutObject`, `s3:GetObject`
- Verify AWS credentials are correct
- Check bucket policy allows your IAM user

---

## CODE SNIPPETS TO GET STARTED

### Kafka Producer (Basic)
```python
from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': 'your-cluster.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'YOUR_KEY',
    'sasl.password': 'YOUR_SECRET'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')

# Send event
event = {
    'event_id': 'uuid-here',
    'user_id': 'user123',
    'track_name': 'Blinding Lights',
    'played_at': '2025-11-18T14:00:00Z'
}

producer.produce(
    'user_plays',
    key='user123',
    value=json.dumps(event),
    callback=delivery_report
)

producer.flush()
```

### Kafka Consumer to S3
```python
from confluent_kafka import Consumer
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

consumer = Consumer({
    'bootstrap.servers': 'your-cluster.confluent.cloud:9092',
    'group.id': 'spotify-s3-writer',
    'auto.offset.reset': 'earliest',
    # ... auth config
})

consumer.subscribe(['user_plays'])

s3 = boto3.client('s3')
events = []

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    
    event = json.loads(msg.value().decode('utf-8'))
    events.append(event)
    
    # Batch write every 100 events
    if len(events) >= 100:
        df = pd.DataFrame(events)
        table = pa.Table.from_pandas(df)
        
        # Partition path
        now = datetime.now()
        path = f"bronze/plays/year={now.year}/month={now.month}/day={now.day}/part-{now.timestamp()}.parquet"
        
        # Write to S3
        pq.write_table(table, f's3://your-bucket/{path}')
        
        events = []
        print(f"Wrote batch to {path}")
```

### DBT Model Template
```sql
-- models/silver/silver_plays.sql

{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

SELECT
    event_id,
    user_id,
    track_id,
    track_name,
    artist_name,
    duration_ms,
    played_at::timestamp as played_at,
    current_timestamp() as processed_at
FROM {{ source('bronze', 'plays') }}

{% if is_incremental() %}
    WHERE played_at > (SELECT MAX(played_at) FROM {{ this }})
{% endif %}
```

### Airflow DAG Template
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sunil',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_ingestion',
    default_args=default_args,
    description='Ingest Spotify events',
    schedule_interval='@hourly',
    start_date=datetime(2025, 11, 18),
    catchup=False
) as dag:
    
    consume_events = BashOperator(
        task_id='consume_kafka',
        bash_command='python /path/to/consumer.py',
    )
    
    run_dbt = BashOperator(
        task_id='transform_data',
        bash_command='cd /path/to/dbt && dbt run --models silver.*',
    )
    
    consume_events >> run_dbt
```

---

## COST TRACKING SPREADSHEET

Track weekly to stay on budget:

| Week | AWS S3 | Snowflake | EC2 | Total | Notes |
|------|--------|-----------|-----|-------|-------|
| 1 | $1 | $0 | $0 | $1 | Trial credits |
| 2 | $2 | $0 | $0 | $2 | Trial credits |
| 3 | $2 | $0 | $0 | $2 | Trial credits |
| 4 | $2 | $5 | $0 | $7 | Trial ending |

**Set AWS billing alert at $10/month.**

---

## INTERVIEW PREP QUESTIONS

Practice answering these about your project:

1. "Walk me through your data pipeline" (2 min answer)
2. "Why did you choose Kafka over SQS/RabbitMQ?"
3. "How did you handle data quality issues?"
4. "What would you do differently with more time/budget?"
5. "How did you optimize Snowflake costs?"
6. "Explain your partitioning strategy"
7. "How would you scale this to 1M events/second?"
8. "What monitoring/alerting would you add?"

---

## SUCCESS CHECKLIST

Before calling it "done":

### Technical:
- [ ] Pipeline runs for 48 hours without intervention
- [ ] All DBT tests passing
- [ ] Dashboard loads in < 3 seconds
- [ ] 10,000+ events processed
- [ ] Costs under $5/week

### Documentation:
- [ ] README explains setup clearly
- [ ] Architecture diagram included
- [ ] Code comments throughout
- [ ] Blog post written

### Demo:
- [ ] Can explain in 5 minutes
- [ ] Can answer technical questions
- [ ] Video recorded and uploaded
- [ ] GitHub repo public