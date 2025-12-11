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

### Phase 5.3: Simple Recommendation Engine

#### Tasks:
- [ ] Create `dbt/models/gold/track_similarity.sql`:
  - Collaborative filtering: Users who liked track X also liked track Y
  - Based on play patterns (co-occurrence analysis)
  - Calculate similarity scores between tracks
  - Rank top 10 similar tracks per track
- [ ] Create `dbt/models/gold/user_recommendations.sql`:
  - Top 10 recommended tracks per user
  - Based on similar users' listening habits
  - Exclude tracks user has already played
  - Include recommendation scores
- [ ] Update `dags/spotify_pipeline_basic.py`:
  - Add DBT task to run recommendation models
  - Runs after `dbt_run_gold` task completes
- [ ] Store recommendations in Snowflake:
  - Table: `SPOTIFY_DATA.GOLD.track_similarity`
  - Table: `SPOTIFY_DATA.GOLD.user_recommendations`
  - Columns: user_id, track_id, recommendation_score, rank
- [ ] Add recommendations page to Streamlit dashboard:
  - New page: "Recommendations"
  - Display top 10 recommended tracks for current user
  - Show: track name, artist, recommendation score
  - Optional: Add "play on Spotify" links
- [ ] Test recommendations:
  - Query your personalized recommendations from Snowflake
  - Verify they make sense based on your listening history
  - Check dashboard displays recommendations correctly

---
### Phase 6: Near Real-Time Recommendations

---

### Phase 6.1: Currently Playing Poller

#### Tasks:
- [ ] Create `src/spotify_poller.py`:
  - Poll `/v1/me/player/currently-playing` endpoint every 5 seconds
  - Detect when a new song starts playing
  - Compare current track_id with previous track_id
  - Log track changes with timestamps
- [ ] Create background process:
  - Run poller as daemon/background service
  - Use threading or asyncio for non-blocking polls
  - Handle errors and rate limits gracefully
- [ ] Store current playback state:
  - Table: `SPOTIFY_DATA.RAW.currently_playing`
  - Columns: user_id, track_id, track_name, artist, started_at, detected_at
  - Update table every time new song detected
- [ ] Trigger recommendation generation:
  - When new song detected, call recommendation API
  - Store recommendations in cache (Redis or in-memory)
  - Log recommendation generation events
- [ ] Test poller:
  - Run poller while playing Spotify
  - Verify new songs are detected within 5-10 seconds
  - Check logs show track changes accurately

---

### Phase 6.2: Real-Time Recommendation API

#### Tasks:
- [ ] Build FastAPI application:
  - Create `src/recommendation_api.py`
  - Endpoint: `GET /recommendations/{track_id}`
  - Queries `SPOTIFY_DATA.GOLD.track_similarity` for similar tracks
  - Returns JSON with top 10 recommendations
- [ ] Add caching layer:
  - Install Redis (locally or on EC2)
  - Cache recommendations per track_id (TTL: 1 hour)
  - Check cache first, query Snowflake only on cache miss
  - Reduces latency to <100ms for cached results
- [ ] Deploy API:
  - Run FastAPI on EC2 alongside Airflow
  - Expose on port 8000: `http://ec2-ip:8000`
  - Add health check endpoint: `/health`
- [ ] Test API:
  - Call `/recommendations/{track_id}` with real track IDs
  - Verify response time <500ms (with cache)
  - Test cache invalidation after 1 hour
  - Verify recommendations are accurate

---

### Phase 6.3: Live Dashboard Integration

#### Tasks:
- [ ] Update Streamlit dashboard:
  - Add new page: "Now Playing"
  - Poll recommendation API every 5 seconds
  - Display currently playing track (from poller)
  - Display real-time recommendations below current track
- [ ] Add auto-refresh functionality:
  - Use `st.experimental_rerun()` to refresh every 5 seconds
  - Show timestamp of last update
  - Add manual "Refresh Now" button
- [ ] Enhance UI:
  - Display album art for current track and recommendations
  - Add "Play on Spotify" links (opens Spotify app/web)
  - Show recommendation scores as progress bars
  - Highlight when recommendations change
- [ ] Optional: Audio previews:
  - Fetch preview URLs from Spotify API
  - Embed 30-second preview clips in dashboard
  - Add play/pause buttons for each recommendation
- [ ] Test live dashboard:
  - Play songs on Spotify
  - Verify dashboard updates within 5-10 seconds
  - Check recommendations refresh for each new song
  - Ensure UI is responsive and doesn't lag

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