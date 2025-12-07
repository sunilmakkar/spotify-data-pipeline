# Airflow Architecture Documentation

## 1. System Overview

### High-Level Architecture

This Spotify data pipeline demonstrates a production-grade data engineering workflow using industry-standard tools orchestrated by Apache Airflow.

**Data Flow:**
```
Event Generation (Python) 
    → Kafka (Message Queue) 
    → S3 (Data Lake - Parquet files) 
    → Snowflake Bronze (External Table) 
    → DBT Silver (Cleaned data) 
    → DBT Gold (Business metrics)
```

**Component Purpose:**
- **Event Simulator:** Generates realistic Spotify listening events (200 events per run)
- **Kafka:** Handles real-time event streaming with producer/consumer pattern
- **S3:** Acts as data lake with Parquet format and date/hour partitioning
- **Snowflake Bronze Layer:** External table reading directly from S3
- **DBT Silver Layer:** Data cleaning, type casting, and standardization
- **DBT Gold Layer:** Business-ready aggregations (top tracks, artists, user stats, device usage)
- **Airflow:** Orchestrates the entire workflow with scheduling, monitoring, and error handling

### Medallion Architecture

The pipeline follows the medallion architecture pattern:

- **Bronze (Raw):** Exact copy of source data from S3, minimal transformation
- **Silver (Cleaned):** Data quality improvements, standardized formats, deduplication
- **Gold (Business):** Aggregated metrics ready for analytics and dashboards

**Why this pattern?**
- Separates concerns: raw data preservation vs business logic
- Enables reprocessing: can rebuild Silver/Gold without re-ingesting from source
- Follows best practices used at Databricks, Snowflake, and other data platforms

### Key Design Principles

1. **Separation of Concerns:** Each component has a single responsibility
2. **Idempotency:** Pipeline can be re-run without duplicating data
3. **Observability:** Comprehensive logging, monitoring, and alerting
4. **Fault Tolerance:** Automatic retries and graceful degradation
5. **Scalability:** Architecture supports 10x-100x growth with infrastructure changes

## 2. DAG Design Philosophy

### Two-DAG Architecture

The pipeline uses two independent DAGs rather than a monolithic approach:

**1. Production Pipeline (`spotify_data_basic`)**
- **Purpose:** Execute the core data pipeline end-to-end
- **Schedule:** Hourly (`'0 * * * *'`)
- **Tasks:** 10 tasks covering event generation → Kafka → S3 → Snowflake → DBT
- **Focus:** Data processing and transformation

**2. Monitoring Pipeline (`data_quality_monitoring`)**
- **Purpose:** Validate data quality and pipeline health
- **Schedule:** Every 30 minutes (`'*/30 * * * *'`)
- **Tasks:** 6 tasks (5 parallel validation checks + start/success tasks)
- **Focus:** Observability and alerting

**Why separate DAGs?**
- **Different concerns:** Production focuses on data movement, monitoring focuses on validation
- **Independent scheduling:** Monitoring runs 2x more frequently to catch issues faster
- **Failure isolation:** A monitoring failure doesn't block production data processing
- **Clear responsibilities:** Each DAG has a single, well-defined purpose
- **Production best practice:** Separating data pipelines from monitoring is standard at companies like Airbnb, Uber, Netflix

### Task Granularity: Why 10 Tasks Instead of 3?

The production DAG evolved from a simple 3-task pipeline to a granular 10-task workflow:

**Original (3 tasks):**
```
generate_events → run_dbt → log_success
```

**Current (10 tasks):**
```
start_consumer ──┐
                 ├──→ wait_for_s3_files → refresh_snowflake_table → dbt_compile → dbt_run_silver → dbt_run_gold → dbt_test → log_success
generate_events ─┘                                                                                                           ↓
                                                                                                                      stop_consumer
```

**Design decisions:**

1. **Parallel Event Generation & Kafka Consumer**
   - Why: Kafka consumer must be running *before* events are generated
   - Pattern: Background process management with PID tracking
   - Benefit: Ensures no events are lost during streaming

2. **Explicit S3 Validation (`wait_for_s3_files`)**
   - Why: S3 writes are eventually consistent; can't assume immediate availability
   - Operator: `S3KeySensor` with 5-minute timeout
   - Benefit: Prevents downstream failures from missing files

3. **Snowflake External Table Refresh**
   - Why: Snowflake doesn't auto-detect new S3 files without explicit refresh
   - Command: `ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;`
   - Benefit: Ensures Bronze layer sees latest data before DBT runs

4. **Granular DBT Tasks (compile → run_silver → run_gold → test)**
   - Why: Monolithic `dbt run` hides which layer failed
   - Benefit: Better observability and faster debugging
   - Example: If Gold layer fails, we know Silver succeeded (don't need to re-run everything)
   - Pattern: Follows DBT best practices for production deployments

5. **Graceful Consumer Shutdown (`stop_consumer`)**
   - Why: Ensures Kafka consumer releases resources properly
   - Dependency: Runs after pipeline success/failure
   - Benefit: No zombie processes consuming EC2 memory

### Task Dependencies Rationale

**Critical path:**
```
start_consumer + generate_events → wait_for_s3_files → refresh_snowflake_table → dbt_compile → dbt_run_silver → dbt_run_gold → dbt_test
```

**Why this order?**
- Can't wait for S3 files until events are generated
- Can't refresh Snowflake until S3 files exist
- Can't run Silver transformations until Bronze is refreshed
- Can't run Gold until Silver exists
- Tests must run after all transformations complete

**Parallel execution:**
- `start_consumer` and `generate_events` run in parallel (reduces total runtime by ~30 seconds)
- All 5 monitoring checks run in parallel (reduces monitoring DAG runtime from 5 minutes to 1 minute)

### Idempotency Strategy

Every task is designed to be idempotent (can be safely re-run):

- **Event generation:** Creates new events with unique timestamps (no duplicates)
- **Kafka:** Consumer uses offset management (processes each event exactly once)
- **S3:** Parquet files partitioned by date/hour (re-runs don't overwrite existing data)
- **DBT:** Uses `incremental` models where appropriate (only processes new data)
- **Monitoring:** Checks are time-windowed (re-running doesn't affect results)

**Why this matters:** Allows safe pipeline re-runs after failures without data corruption or duplication.

## 3. Operator Selection & Technical Trade-offs

### BashOperator for Event Generation & Kafka

**Choice:** `BashOperator` to run Python scripts and Kafka consumer

**Alternatives considered:**
- Custom PythonOperator with event generation logic embedded in DAG
- KafkaProducer/KafkaConsumer operators from `airflow-providers-apache-kafka`

**Why BashOperator?**
- **Simplicity:** Existing Python scripts work without modification
- **Process isolation:** Each run gets a clean process (no state pollution)
- **Debugging:** Easier to test scripts independently outside Airflow
- **Flexibility:** Can pass environment variables and capture output/logs
- **Standard pattern:** Most production pipelines run external processes this way

**Trade-off:** Less type safety than PythonOperator, but gained operational simplicity

### SnowflakeOperator vs SnowflakeHook

**Choice:** `SnowflakeOperator` for SQL execution

**Why SnowflakeOperator?**
- **Declarative:** SQL is visible directly in DAG code
- **Connection management:** Automatically handles Snowflake connection pooling
- **Retry logic:** Inherits Airflow's retry mechanism
- **Logging:** Query execution and row counts automatically logged
- **Audit trail:** Each task execution stored in Airflow metadata

**When SnowflakeHook would be better:**
- Complex logic requiring multiple queries with conditional execution
- Need to process query results in Python (e.g., dynamic branching)
- Building custom operators with Snowflake interactions

**Our use case:** Simple `ALTER TABLE` and `SELECT` statements → Operator is perfect fit

### S3KeySensor for File Validation

**Choice:** `S3KeySensor` to wait for Parquet files

**Alternatives considered:**
- Poll S3 in a while loop within BashOperator
- Skip validation and assume files exist
- Use `ExternalTaskSensor` if generation was a separate DAG

**Why S3KeySensor?**
- **Built-in retry logic:** Polls every 60 seconds with exponential backoff
- **Timeout handling:** Fails gracefully after 5 minutes if files don't appear
- **Resource efficient:** Uses Airflow's sensor mode (doesn't block worker slots)
- **Explicit dependency:** Makes S3 requirement visible in DAG graph
- **Battle-tested:** Standard pattern in production data pipelines

**Configuration:**
```python
wait_for_s3_files = S3KeySensor(
    task_id='wait_for_s3_files',
    bucket_name='spotify-data-pipeline-sunil-2025',
    bucket_key='events/year={{ ... }}/hour={{ ... }}/*.parquet',
    wildcard_match=True,
    timeout=300,  # 5 minutes
    poke_interval=60,  # Check every 60 seconds
    aws_conn_id='aws_default'
)
```

### Email Alerts via SMTP

**Choice:** Gmail SMTP for failure notifications

**Alternatives considered:**
- AWS SES (Simple Email Service)
- Slack webhooks
- PagerDuty integration
- AWS SNS (Simple Notification Service)

**Why Gmail SMTP?**
- **Zero cost:** Gmail provides free SMTP for low-volume usage
- **Simple setup:** Just username/password and app-specific password
- **Universal:** Email works everywhere (no additional accounts needed)
- **Testing:** Easy to verify alerts during development

**Production consideration:** For a real production system, I would use:
- **AWS SES** for higher volume and better deliverability
- **Slack** for team collaboration and faster response times
- **PagerDuty** for on-call rotation and escalation policies

### LocalExecutor vs CeleryExecutor

**Choice:** `LocalExecutor` for task execution

**Why LocalExecutor?**
- **Simplicity:** Single EC2 instance, no distributed worker management
- **Cost:** No need for separate worker nodes or message broker (Redis/RabbitMQ)
- **Sufficient scale:** Handles 10 concurrent tasks easily on t3.small
- **Development-friendly:** Easier debugging (all tasks on same machine)

**Current limitations:**
- Can't scale horizontally (limited to single machine's resources)
- Task parallelism capped at 4 (memory constraint on 2GB instance)
- No worker node redundancy (single point of failure)

**When to upgrade to CeleryExecutor:**
- Processing >1,000 events per hour
- Need task parallelism >10
- Require high availability (worker node redundancy)
- Multiple teams sharing Airflow infrastructure

**Scaling path:** LocalExecutor → CeleryExecutor on multiple EC2 instances → Kubernetes with KubernetesExecutor

### Memory Optimization Choices

**Configuration:**
```python
# airflow.cfg
[core]
parallelism = 4              # Max tasks across all DAGs
dag_concurrency = 2          # Max tasks per DAG
max_active_runs_per_dag = 1  # One DAG run at a time
```

**Why these limits?**
- **EC2 instance:** t3.small with 2GB RAM + 2GB swap
- **Memory pressure:** Docker containers + Postgres + Airflow scheduler/webserver
- **Observed behavior:** parallelism=8 caused OOM errors, parallelism=4 is stable
- **Trade-off:** Longer task execution time vs system stability

**If I had t3.medium (4GB RAM):**
- parallelism = 8
- dag_concurrency = 4
- Could run both DAGs simultaneously without memory issues

## 4. Error Handling Strategy

### Retry Configuration

Every task in the production pipeline is configured with automatic retry logic:
```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10)
}
```

**Retry Logic Explained:**

- **retries=3:** Each task attempts up to 3 times before marking as failed
- **retry_delay=2 minutes:** Initial wait before first retry
- **exponential_backoff=True:** Subsequent retries wait longer (2min → 4min → 8min)
- **max_retry_delay=10 minutes:** Cap on retry delay (prevents waiting hours)

**Why exponential backoff?**
- Transient failures (network hiccup, temporary S3 throttling) often resolve quickly
- Persistent failures (code bug, credential issue) won't be fixed by immediate retry
- Exponential backoff reduces load on downstream systems during outages
- Industry standard pattern (used by AWS SDK, Kubernetes, etc.)

### Task-Specific Error Handling

**1. Kafka Consumer Management**

The `start_consumer` task includes PID tracking to prevent zombie processes:
```bash
# Start consumer in background, save PID
nohup python src/consume_and_write.py > /tmp/consumer.log 2>&1 & echo $! > /tmp/kafka_consumer.pid
```

**Failure scenarios:**
- If consumer crashes: PID file exists but process is dead → `stop_consumer` handles cleanup
- If pipeline fails: `stop_consumer` still runs (trigger rule: `all_done`) → no orphaned processes
- If EC2 restarts: `/tmp/` is cleared → fresh start, no stale PIDs

**2. S3 File Validation**

The `S3KeySensor` has built-in timeout protection:
```python
wait_for_s3_files = S3KeySensor(
    timeout=300,  # 5 minutes max
    poke_interval=60,  # Check every 60 seconds
    mode='poke'  # Blocks worker slot (for short waits)
)
```

**Failure scenarios:**
- Files don't appear in 5 minutes → Task fails, triggers retry
- S3 is temporarily unavailable → Sensor retries automatically
- Wrong bucket/path configuration → Fails fast (within 60 seconds)

**3. Snowflake Connection Failures**

SnowflakeOperator includes built-in retry logic for connection issues:

**Transient failures handled automatically:**
- Network timeouts
- Temporary credential issues
- Query queue limits

**Permanent failures fail fast:**
- Invalid SQL syntax (no point retrying)
- Permission errors (REFRESH privilege missing)
- Table doesn't exist

**4. DBT Task Failures**

Each DBT layer (compile, silver, gold, test) fails independently:

**Example failure scenario:**
```
dbt_compile ✓ → dbt_run_silver ✓ → dbt_run_gold ✗ → dbt_test (skipped)
```

**Recovery strategy:**
- Fix the Gold model SQL
- Clear the failed task in Airflow UI
- Re-run from `dbt_run_gold` (don't need to re-run Silver)
- Silver data is already in Snowflake (idempotent)

### Email Alert System

**Configuration:**
```python
# In docker-compose.yml
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT: 587
AIRFLOW__SMTP__SMTP_USER: your-email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD: your-app-password
AIRFLOW__SMTP__SMTP_MAIL_FROM: your-email@gmail.com
```

**Alert triggers:**

1. **Task failure after all retries:** Critical tasks send email via `on_failure_callback`
2. **Monitoring check failures:** Data quality issues trigger immediate alerts
3. **Email content includes:**
   - Task name and DAG ID
   - Execution date
   - Error message
   - Link to Airflow UI for logs

**Example alert function:**
```python
def task_failure_alert(context):
    """Send email alert when critical task fails"""
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    
    subject = f"Airflow Task Failed: {dag_id}.{task_instance.task_id}"
    body = f"""
    Task {task_instance.task_id} failed after all retries.
    
    DAG: {dag_id}
    Execution Date: {context.get('execution_date')}
    Log URL: {task_instance.log_url}
    
    Please check Airflow UI for details.
    """
    
    send_email(to='your-email@gmail.com', subject=subject, html_content=body)
```

**Applied to critical tasks:**
```python
refresh_snowflake_table = SnowflakeOperator(
    task_id='refresh_snowflake_table',
    sql="ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;",
    snowflake_conn_id='snowflake_default',
    on_failure_callback=task_failure_alert  # Email if this fails
)
```

### Graceful Degradation

**Philosophy:** The pipeline should fail safely without corrupting data or leaving resources in bad states.

**1. Kafka Consumer Cleanup**
```python
stop_consumer = BashOperator(
    task_id='stop_consumer',
    bash_command='kill $(cat /tmp/kafka_consumer.pid) || true',
    trigger_rule='all_done'  # Runs even if upstream tasks fail
)
```

**Key:** `trigger_rule='all_done'` ensures cleanup happens regardless of pipeline success/failure.

**2. No Partial Writes**

- **S3:** Parquet files written atomically (all-or-nothing)
- **Snowflake:** Transactions ensure partial updates don't corrupt data
- **DBT:** Each model runs in transaction (rollback on failure)

**3. Monitoring Independence**

The monitoring DAG runs separately from production:
- Production failure doesn't prevent monitoring alerts
- Monitoring failure doesn't block data processing
- Each system can fail independently without cascading

### Failure Recovery Workflow

**Step-by-step recovery process:**

1. **Receive email alert** with task name and error
2. **Check Airflow UI logs** for detailed error message
3. **Identify root cause:**
   - Transient (network, timeout) → Manual retry usually succeeds
   - Configuration (wrong path, credentials) → Fix config, clear task, retry
   - Code bug (bad SQL, Python error) → Fix code, redeploy, retry
4. **Clear failed task** in Airflow UI (only that task, not entire DAG)
5. **Trigger retry** (task picks up where it left off)
6. **Verify success** via dashboard or Snowflake query

**Example:**
```bash
# If dbt_run_gold fails, manually retry just that task
docker-compose exec airflow-scheduler airflow tasks clear spotify_data_basic dbt_run_gold -y
```

## 5. Monitoring & Observability

### Why Independent Monitoring?

The `data_quality_monitoring` DAG runs separately from production for several critical reasons:

**1. Early Detection**
- Monitoring runs every 30 minutes (production runs hourly)
- Can detect issues 30 minutes faster than waiting for next production run
- Example: If Bronze table stops receiving data at 2:15pm, monitoring detects it by 2:30pm instead of 3:00pm

**2. Failure Isolation**
- Production pipeline failure doesn't prevent monitoring from running
- Monitoring failure doesn't block data processing
- Can diagnose production issues using monitoring data

**3. Different Metrics**
- Production measures: "Did tasks complete?"
- Monitoring measures: "Is the data correct?"
- These are fundamentally different questions requiring different tools

**4. Independent Alerting**
- Monitoring alerts indicate data quality issues (missing data, anomalies)
- Production alerts indicate infrastructure issues (connection failures, timeouts)
- Different on-call response procedures for each

### Five Data Quality Checks

The monitoring DAG runs 5 parallel validation checks on every execution:

#### 1. Row Count Validation (Bronze vs Silver)

**What it checks:** Data retention between Bronze and Silver layers
```sql
SELECT 
    (SELECT COUNT(*) FROM SPOTIFY_DATA.BRONZE.plays) as bronze_count,
    (SELECT COUNT(*) FROM SPOTIFY_DATA.SILVER.silver_plays) as silver_count,
    (silver_count::FLOAT / NULLIF(bronze_count, 0)) as retention_rate
```

**Pass condition:** `retention_rate >= 0.95` (at least 95% of Bronze data makes it to Silver)

**Why 95% threshold?**
- Allows for legitimate data quality filtering (malformed records, duplicates)
- Detects major issues (DBT transformation bug, Snowflake connection problem)
- Too strict (99%) would cause false alarms; too loose (80%) would miss real issues

**What triggers failure:**
- DBT Silver transformation has a bug and drops records
- Snowflake external table refresh failed (Bronze is stale)
- S3 files corrupted or incomplete

#### 2. Data Freshness Check

**What it checks:** Most recent event is within the last 24 hours
```sql
SELECT MAX(played_at) as latest_event
FROM SPOTIFY_DATA.SILVER.silver_plays
WHERE played_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
```

**Pass condition:** Query returns at least one row (data exists in last 24 hours)

**Why this matters:**
- Detects pipeline stalls (nothing generated/processed recently)
- Catches scheduling issues (DAG disabled accidentally)
- Identifies upstream data source problems

**What triggers failure:**
- Production pipeline hasn't run in 24+ hours
- Event generation is failing silently
- Kafka consumer crashed and isn't processing events

#### 3. DBT Test Results

**What it checks:** All DBT tests passed in last run
```sql
SELECT status, COUNT(*) as test_count
FROM SPOTIFY_DATA.SILVER.dbt_test_results
WHERE executed_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
GROUP BY status
```

**Pass condition:** No rows with `status = 'fail'`

**Why leverage DBT tests?**
- DBT already validates data contracts (not null, unique, relationships)
- Avoids duplicating test logic in Airflow and DBT
- Single source of truth for data quality rules

**What triggers failure:**
- Primary key violations (duplicate user_ids)
- Foreign key violations (track_id references artist that doesn't exist)
- Custom business logic violations (play_duration > song_duration)

#### 4. Event Volume Anomaly Detection

**What it checks:** Today's event volume compared to 7-day average
```sql
WITH daily_counts AS (
    SELECT 
        DATE(played_at) as event_date,
        COUNT(*) as event_count
    FROM SPOTIFY_DATA.SILVER.silver_plays
    WHERE played_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    GROUP BY DATE(played_at)
),
averages AS (
    SELECT AVG(event_count) as avg_count
    FROM daily_counts
    WHERE event_date < CURRENT_DATE()  -- Exclude today
)
SELECT 
    dc.event_count as today_count,
    a.avg_count as seven_day_avg,
    (dc.event_count / NULLIF(a.avg_count, 0)) as ratio
FROM daily_counts dc, averages a
WHERE dc.event_date = CURRENT_DATE()
```

**Pass condition:** `ratio >= 0.5` (today's volume is at least 50% of 7-day average)

**Why 50% threshold?**
- Detects major drops (event generation stopped, Kafka consumer hung)
- Allows for natural variance (weekends vs weekdays, holidays)
- Avoids false alarms from expected low-volume periods

**What triggers failure:**
- Event generator is producing far fewer events than normal
- Kafka consumer is processing slowly (backlog building up)
- S3 writes are failing intermittently

#### 5. Gold Layer Completeness

**What it checks:** All 4 Gold tables have data
```sql
-- Check each table separately
SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.top_tracks;
SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.top_artists;
SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.daily_user_stats;
SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.device_usage;
```

**Pass condition:** Each query returns `count > 0`

**Why check all 4 tables?**
- Gold layer is what downstream consumers use (dashboards, APIs)
- Empty Gold table means business metrics are unavailable
- Catches DBT Gold transformations that silently fail

**What triggers failure:**
- DBT Gold model has syntax error but didn't raise exception
- Business logic filters out all rows (overly restrictive WHERE clause)
- Upstream Silver table is empty (cascading failure)

### Alert Mechanisms

**Email alerts trigger on any check failure:**
```python
def send_monitoring_alert(context):
    """Alert when data quality check fails"""
    task_id = context['task_instance'].task_id
    
    subject = f"Data Quality Alert: {task_id}"
    body = f"""
    Data quality check '{task_id}' failed.
    
    Execution Time: {context['execution_date']}
    
    Investigate immediately:
    1. Check Airflow logs for detailed error
    2. Query Snowflake to verify data
    3. Review recent pipeline runs for failures
    
    Dashboard: docker-compose exec airflow-scheduler python /opt/airflow/scripts/dashboard.py
    """
    
    send_email(to='your-email@gmail.com', subject=subject, html_content=body)
```

**Alert includes:**
- Which specific check failed
- Timestamp of failure
- Troubleshooting steps
- Link to dashboard for current state

### Operational Dashboard

Created `scripts/dashboard.py` for quick health visibility:

**What it shows:**
```
PIPELINE HEALTH
- Total runs (last 10)
- Success rate
- Last successful run (time ago)
- Average duration

DATA METRICS
- Bronze layer row count
- Silver layer row count  
- Events in last 24 hours
- Gold layer table counts

MONITORING STATUS
- Total checks (last 5 runs)
- Passed vs failed
- Last check time
```

**Why this dashboard?**
- Single command gives complete system health: `python scripts/dashboard.py`
- No need to open Airflow UI for routine checks
- Color-coded output (green = good, red = issues)
- Useful for daily standups or incident response

**Usage:**
```bash
docker-compose exec airflow-scheduler python /opt/airflow/scripts/dashboard.py
```

### Logging Strategy

**Task-level logging:**
- Every task outputs to Airflow logs (accessible via UI)
- BashOperator commands log stdout/stderr
- SnowflakeOperator logs query text and row counts
- Sensors log each polling attempt

**External logs:**
- Kafka consumer writes to `/tmp/consumer.log` (inspectable if issues arise)
- DBT logs stored in `dbt_spotify/logs/` (detailed transformation info)
- Docker container logs: `docker-compose logs airflow-scheduler`

**Log retention:**
- Airflow keeps logs for 30 days (configurable)
- Allows post-mortem analysis of historical failures
- Helps identify patterns (does Gold fail every Sunday?)

### Metrics That Matter

**Primary metrics tracked:**

1. **Pipeline success rate:** Percentage of runs that complete without failure
2. **Data freshness:** Hours since most recent event processed
3. **Data retention:** Bronze → Silver → Gold row count ratios
4. **Event volume:** Events processed per hour (trend over time)
5. **Task duration:** How long each task takes (identify bottlenecks)

**Why these specific metrics?**
- Success rate: Overall system health indicator
- Freshness: Detects stalled pipelines
- Retention: Detects data loss bugs
- Volume: Detects upstream issues
- Duration: Identifies performance regressions

### Observability Best Practices Demonstrated

**1. Separation of concerns:** Monitoring is independent from production
**2. Fail fast:** Each check has clear pass/fail criteria (no ambiguity)
**3. Actionable alerts:** Email includes troubleshooting steps, not just "something broke"
**4. Historical context:** Dashboard shows trends (not just current state)
**5. Multi-layer validation:** Check Bronze, Silver, Gold, and business logic
**6. Anomaly detection:** Compare current state to historical baselines

## 6. Scalability Considerations

### Current Architecture Limitations

**Infrastructure constraints:**
- **Single EC2 instance (t3.small):** 2GB RAM, 2 vCPUs
- **LocalExecutor:** All tasks run on same machine
- **Parallelism cap:** 4 concurrent tasks maximum
- **No redundancy:** Single point of failure

**Data volume constraints:**
- **Current scale:** 200 events per hour = 4,800 events per day
- **Storage:** ~5MB per day in S3 (Parquet compressed)
- **Processing time:** Full pipeline takes ~6 minutes end-to-end
  - Kafka consumer processing: ~5 minutes (largest bottleneck)
  - DBT transformations: ~40 seconds total (compile + silver + gold + test)
  - Other tasks: ~20 seconds combined
- **Snowflake:** Single warehouse (X-Small) handles queries easily

**What breaks at scale:**
1. **At 2,000 events/hour (10x):** Kafka consumer takes ~50 minutes → exceeds hourly schedule window
2. **At 10,000 events/hour (50x):** Would need 4+ hours for Kafka processing alone → pipeline stalls
3. **At 100,000 events/hour (500x):** Need distributed Kafka consumers and parallel stream processing

**Key bottleneck:** Kafka consumer processes ~40 events/minute sequentially. This is the limiting factor, not DBT or Snowflake performance.

### Horizontal Scaling Strategy

### Phase 1: Optimize Current Architecture (10x scale)

**Target:** 2,000 events/hour without infrastructure changes

**Challenge:** At 10x volume, Kafka consumer would take 50 minutes to process 2,000 events, which doesn't fit in the hourly schedule.

**Solution 1: Parallel Kafka Consumers**
```python
# Current: Single consumer thread
# Optimized: Multiple consumer threads with topic partitions

# In docker-compose.yml
start_consumer = BashOperator(
    task_id='start_consumer',
    bash_command='''
        # Start 4 parallel consumers
        for i in {0..3}; do
            nohup python src/consume_and_write.py --partition $i > /tmp/consumer_$i.log 2>&1 &
            echo $! >> /tmp/kafka_consumer.pids
        done
    '''
)
```

**Benefit:** 4 parallel consumers = 4x throughput = ~160 events/minute
- 2,000 events processed in ~12 minutes (fits in hourly window)

**Trade-off:** More complex process management (track multiple PIDs)

**Solution 2: Batch Event Generation**
```python
# Current: Generate 200 events per hour
# Optimized: Generate 500 events every 3 hours

generate_events = BashOperator(
    bash_command='python src/generate_and_produce.py --num-events 500',
    schedule='0 */3 * * *'  # Every 3 hours instead of hourly
)
```

**Benefit:** Fewer pipeline runs, same total volume (2,000 events/day vs 4,800/day if needed)
**Trade-off:** Less granular data (3-hour batches vs 1-hour batches)

**Solution 3: Incremental DBT Models**
```sql
-- Current: Full table refresh on every run
{{ config(materialized='table') }}

-- Optimized: Process only new data
{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

SELECT * FROM {{ ref('bronze_plays') }}
{% if is_incremental() %}
WHERE played_at > (SELECT MAX(played_at) FROM {{ this }})
{% endif %}
```

**Benefit:** DBT only processes new rows, runtime stays constant as data grows
**Current impact:** Minimal (40s → 30s), but crucial at higher scale

**Solution 4: Snowflake Table Clustering**
```sql
-- Add clustering keys for query performance
ALTER TABLE SPOTIFY_DATA.SILVER.silver_plays 
CLUSTER BY (DATE(played_at), user_id);

ALTER TABLE SPOTIFY_DATA.GOLD.top_tracks
CLUSTER BY (play_count DESC);
```

**Benefit:** Query performance stays constant as table size grows from MB to GB
**Cost:** Slight increase in Snowflake compute for automatic clustering maintenance

**Phase 1 Result:**
- Cost: Same (~$36/month)
- Capacity: 2,000 events/hour sustained
- Changes: Code optimizations only, no infrastructure upgrades

---

### Phase 2: Upgrade Infrastructure (100x scale)

**Target:** 20,000 events/hour

**Challenge:** Even with parallel consumers, single EC2 instance can't handle 20,000 events/hour due to CPU and network I/O limits.

**Solution 1: Migrate to CeleryExecutor**
```yaml
# docker-compose.yml additions
redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
  
airflow-worker-1:
  <<: *airflow-common
  command: celery worker
  deploy:
    resources:
      limits:
        memory: 2G
  
airflow-worker-2:
  <<: *airflow-common
  command: celery worker
  deploy:
    resources:
      limits:
        memory: 2G
```

**Architecture changes:**
- **3 EC2 instances:** 
  - 1x t3.medium (scheduler + webserver)
  - 2x t3.medium (Celery workers)
- **Redis** as message broker for task distribution
- **Parallelism:** 16 concurrent tasks (8 per worker)

**Benefits:**
- Horizontal scaling: Add workers dynamically as load increases
- Fault tolerance: If worker crashes, tasks redistributed to healthy workers
- Independent scaling: Scale workers without touching scheduler

**Cost increase:** $15/month → $90/month (3x t3.medium instances)

**Solution 2: Upgrade Snowflake Warehouse**
```sql
-- Current: X-SMALL (1 credit/hour, ~$2/hour)
-- Upgraded: SMALL (2 credits/hour, ~$4/hour)
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'SMALL';
```

**Benefit:** 
- 2x faster DBT transformations
- Handles larger joins and aggregations efficiently
- Still auto-suspends after 1 minute idle (minimal cost increase)

**Actual cost impact:** ~$20/month → ~$40/month (only runs ~10 hours/month)

**Solution 3: Snowflake Auto-Scaling**
```sql
ALTER WAREHOUSE COMPUTE_WH SET 
  AUTO_SUSPEND = 60,              -- Suspend after 1 min idle
  AUTO_RESUME = TRUE,              -- Auto-start when query submitted
  MIN_CLUSTER_COUNT = 1,
  MAX_CLUSTER_COUNT = 3,           -- Scale to 3 clusters under load
  SCALING_POLICY = 'STANDARD';
```

**Benefit:** Automatically handles traffic spikes without manual intervention
**Cost model:** Pay only for clusters actually used

**Solution 4: Distributed Kafka Consumers**
```python
# Deploy Kafka consumers on worker nodes
# Each worker runs independent consumer group

# Worker 1: Partitions 0-4
# Worker 2: Partitions 5-9
# Total: 10 partitions for parallel processing

# Kafka topic configuration
kafka-topics --create \
  --topic spotify-events \
  --partitions 10 \
  --replication-factor 2
```

**Benefit:** 10 parallel consumers across 2 workers = 10x throughput
**Capacity:** ~400 events/minute = 24,000 events/hour

**Phase 2 Result:**
- Cost: $36/month → $305/month
- Capacity: 20,000 events/hour sustained
- Latency: Pipeline completes in <15 minutes even at peak load

---

### Phase 3: Distributed Processing (1,000x scale)

**Target:** 200,000+ events/hour (millions per day)

**Challenge:** Single-region, VM-based architecture can't handle this scale. Need cloud-native distributed systems.

**Solution 1: Managed Kafka (AWS MSK or Confluent Cloud)**
```yaml
# Replace local Kafka with AWS MSK
kafka:
  type: AWS MSK
  cluster:
    brokers: 3                      # Multi-AZ for high availability
    broker_type: kafka.m5.large     # 2 vCPU, 8GB RAM per broker
  topic_config:
    partitions: 50                  # Massive parallelism
    replication_factor: 3           # Data durability
    retention_hours: 168            # 7 days retention
```

**Benefits:**
- Handles millions of events/second
- Automatic broker scaling and replacement
- Multi-AZ deployment (survives datacenter failure)
- Managed upgrades and patching

**Cost:** ~$300/month for 3-broker cluster

**Solution 2: Apache Spark for Stream Processing**
```python
# Replace Python consumer with Spark Structured Streaming
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SpotifyStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "msk-cluster:9092") \
    .option("subscribe", "spotify-events") \
    .option("startingOffsets", "latest") \
    .load()

# Transform and write to S3 (Parquet)
query = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("parquet") \
    .option("path", "s3://bucket/events/") \
    .option("checkpointLocation", "s3://bucket/checkpoints/") \
    .trigger(processingTime="1 minute") \
    .start()
```

**Why Spark?**
- Processes 200,000+ events/hour easily across distributed cluster
- Fault-tolerant: Automatic retry on node failures
- Exactly-once semantics: No duplicate or lost events
- Industry standard at Netflix, Uber, Airbnb for streaming data

**Deployment:** AWS EMR (Elastic MapReduce) with auto-scaling
- Min: 2 nodes (1 master + 1 worker)
- Max: 20 nodes during peak hours
- Cost: ~$200-500/month depending on utilization

**Solution 3: Kubernetes for Airflow (AWS EKS)**
```yaml
# Helm chart values for production Airflow on Kubernetes
airflow:
  executor: KubernetesExecutor
  
  workers:
    replicas: 10                    # 10 worker pods
    autoscaling:
      enabled: true
      minReplicas: 5                # Scale down during off-hours
      maxReplicas: 50               # Scale up during peak
      targetCPUUtilizationPercentage: 70
    
  scheduler:
    replicas: 2                     # HA scheduler
    
  webserver:
    replicas: 2                     # HA web UI
    
  postgresql:
    primary:
      persistence:
        size: 100Gi
    readReplicas:
      replicaCount: 2               # Read replicas for scalability
```

**Benefits:**
- Auto-scaling: Adds workers during peak, removes during off-hours
- High availability: Scheduler/webserver redundancy
- Resource efficiency: Pack multiple tasks per node
- Rolling updates: Zero-downtime deployments

**Cost:** 
- EKS cluster: $75/month
- EC2 nodes (spot instances): $300-600/month
- Total: ~$500-1000/month

**Solution 4: Data Lake Architecture with Delta Lake**
```python
# Replace Snowflake external tables with Delta Lake on S3
from delta import *

# Write to Delta Lake (ACID transactions, time travel)
df_events.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("event_date") \
    .option("mergeSchema", "true") \
    .save("s3://bucket/delta/bronze/plays")

# Read in Snowflake via external tables (still works)
# But also queryable directly via Spark/Athena/Presto
spark.read.format("delta").load("s3://bucket/delta/bronze/plays")
```

**Benefits:**
- ACID transactions on S3 (no partial writes)
- Time travel: Query data as it existed yesterday/last week
- Schema evolution: Add columns without breaking queries
- Query from multiple tools: Snowflake, Spark, Athena

**Phase 3 Result:**
- Cost: $305/month → $1,900/month
- Capacity: 200,000+ events/hour (4.8M events/day)
- Latency: Sub-minute end-to-end (streaming real-time)
- Reliability: 99.9% uptime SLA

---

### Database Scaling Path

**Current: Postgres for Airflow metadata (Docker container)**
- Sufficient for: <100 DAGs, <10,000 task executions/day
- Single instance, no replication
- Local disk storage

**At Phase 2 (100x scale):**

Migrate to **AWS RDS Postgres with Multi-AZ**
```sql
-- RDS configuration
Instance: db.t3.medium (2 vCPU, 4GB RAM)
Storage: 100GB SSD with auto-scaling to 500GB
Multi-AZ: Yes (automatic failover)
Backup: 7-day retention, automated snapshots
```

**Add read replicas:**
```
Primary (writes): Scheduler writes task status
Read Replica 1: Webserver reads for UI
Read Replica 2: Monitoring/reporting queries
```

**Add connection pooling (PgBouncer):**
```
Max connections: 1000 (pooled to 100 DB connections)
Benefits: Handles bursts without overwhelming database
```

**Cost:** ~$100/month

**At Phase 3 (1,000x scale):**

Migrate to **AWS Aurora Postgres**
```sql
-- Aurora configuration
Instance class: db.r5.2xlarge (8 vCPU, 64GB RAM)
Read replicas: 5 (auto-scaling)
Global database: Optional multi-region support
Performance: 5x better than standard RDS
```

**Additional optimizations:**
- Partition task_instance table by execution_date (faster queries)
- Archive DAG runs older than 30 days to S3
- Separate database for application data vs Airflow metadata

**Cost:** ~$400/month

---

### Storage Optimization

**Current: S3 Standard for all data**
- $0.023 per GB/month
- Immediate access, high durability

**Optimization: Intelligent Tiering with Lifecycle Policies**
```json
{
    "Rules": [
        {
            "Id": "tier-old-events",
            "Status": "Enabled",
            "Transitions": [
                {
                    "Days": 90,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 365,
                    "StorageClass": "GLACIER_IR"
                },
                {
                    "Days": 730,
                    "StorageClass": "DEEP_ARCHIVE"
                }
            ],
            "Filter": {
                "Prefix": "events/"
            }
        }
    ]
}
```

**Storage tiers explained:**
- **Standard (0-90 days):** $0.023/GB - Frequent access (recent data)
- **Standard-IA (90-365 days):** $0.0125/GB - Infrequent access (quarterly reports)
- **Glacier Instant Retrieval (1-2 years):** $0.004/GB - Annual audits
- **Deep Archive (2+ years):** $0.00099/GB - Compliance retention

**Cost savings at 1,000x scale:**
- Without tiering: 100TB × $0.023 = $2,300/month
- With tiering: 10TB (Standard) + 30TB (IA) + 40TB (Glacier) + 20TB (Deep) = $650/month
- **Savings: 72%** ($1,650/month)

---

### Monitoring at Scale

**Current: Email alerts + basic dashboard**
- Works for 1-2 people
- No historical trends or anomaly detection

**At Phase 2 (100x scale):**

**Add Datadog or New Relic**
```python
# Instrument Airflow with Datadog APM
from ddtrace import tracer

@tracer.wrap()
def process_events():
    # Automatically tracked: duration, errors, throughput
    pass
```

**Metrics tracked:**
- Pipeline latency: P50, P95, P99, P999
- Task failure rate by DAG and task
- Snowflake query performance and cost
- Kafka consumer lag (events waiting to be processed)
- Cost per 1,000 events processed

**Dashboards:**
- Real-time pipeline health
- Cost tracking (per-pipeline, per-component)
- Capacity planning (predict when to scale)

**Cost:** ~$100/month for monitoring

**At Phase 3 (1,000x scale):**

**Add PagerDuty for on-call rotation**
```python
# Escalation policy
Level 1: On-call engineer (5 min response)
Level 2: Team lead (15 min response)
Level 3: Engineering manager (30 min response)
```

**Add Slack integration for team visibility**
```python
# Airflow alerts to Slack channels
#data-engineering: All pipeline runs
#data-alerts: Failures only
#data-costs: Cost anomalies
```

**Add Grafana for custom dashboards**
```sql
-- Example: Events processed per hour over last 7 days
SELECT 
    DATE_TRUNC('hour', execution_date) as hour,
    COUNT(*) as events_processed
FROM airflow.task_instance
WHERE dag_id = 'spotify_data_basic'
  AND task_id = 'generate_events'
  AND state = 'success'
  AND execution_date >= NOW() - INTERVAL '7 days'
GROUP BY hour
ORDER BY hour;
```

**Total monitoring cost at Phase 3:** ~$200/month

---

### Cost Analysis

**Current (200 events/hour):**
- EC2 t3.small: $15/month
- Snowflake X-Small: ~$20/month (auto-suspend minimizes usage)
- S3 Standard: <$1/month
- **Total: ~$36/month**
- **Cost per 1,000 events: $0.075**

**Phase 2: 100x scale (20,000 events/hour):**
- 3x EC2 t3.medium: $90/month
- Redis: $15/month
- Snowflake Small: ~$150/month
- S3: ~$50/month
- RDS Postgres Multi-AZ: $100/month
- Monitoring (Datadog): $100/month
- **Total: ~$505/month**
- **Cost per 1,000 events: $0.010** (87% cheaper per event!)

**Phase 3: 1,000x scale (200,000 events/hour):**
- EKS cluster: $500/month
- MSK (Managed Kafka): $300/month
- EMR (Spark cluster): $400/month
- Snowflake Medium + auto-scaling: $800/month
- S3 with lifecycle policies: $650/month
- Aurora Postgres: $400/month
- Monitoring (Datadog + PagerDuty): $200/month
- **Total: ~$3,250/month**
- **Cost per 1,000 events: $0.0054** (93% cheaper per event than current!)

**Key insight: Economies of scale**
- Per-event costs decrease dramatically as volume increases
- Fixed costs (infrastructure) amortize across more events
- Optimization efforts have bigger ROI at scale
