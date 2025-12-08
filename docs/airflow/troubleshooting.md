# Airflow Troubleshooting Guide

This document covers runtime issues encountered during development and their solutions. For installation and setup issues, see [airflow_setup.md](airflow_setup.md).

---

## Table of Contents
1. [DBT Permission Error After EC2 Restart](#dbt-permission-error-after-ec2-restart)
2. [Safari Browser Freezing on Graph View](#safari-browser-freezing-on-graph-view)
3. [Snowflake External Table Not Refreshing](#snowflake-external-table-not-refreshing)
4. [Memory Optimization Journey](#memory-optimization-journey)
5. [Dashboard Docker Limitations](#dashboard-docker-limitations)
6. [Kafka Consumer Hanging (Potential Issue)](#kafka-consumer-hanging-potential-issue)

---

## DBT Permission Error After EC2 Restart

### Symptom
DBT compile task fails with:
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/dbt/target/manifest.json'
```

Task shows as "up for retry" and eventually fails after exhausting all retry attempts.

### Root Cause
When EC2 instance is stopped and restarted, file permissions on the `dbt/target/` directory can become restrictive, preventing Airflow from writing DBT's compiled artifacts (manifest.json, compiled SQL files).

### Solution
From the project root on EC2 (`/home/ubuntu/spotify-data-pipeline`), run:
```bash
chmod -R 777 dbt/target/
```

This grants full read/write/execute permissions to the target directory.

### Alternative (if chmod doesn't work)
Delete and recreate the target directory:
```bash
rm -rf dbt/target/
mkdir dbt/target/
chmod -R 777 dbt/target/
```

### Prevention
Consider adding this to your EC2 startup routine:
```bash
cd ~/spotify-data-pipeline
chmod -R 777 dbt/target/
docker-compose up -d
```

---

## Safari Browser Freezing on Graph View

### Symptom
When viewing the Airflow DAG Graph view in Safari browser, the page becomes unresponsive and freezes. The browser tab hangs and may eventually crash.

### Root Cause
This was **not** a Safari compatibility issue - it was a **server-side memory problem**. When the EC2 instance was running low on memory (before optimization), the Airflow webserver would struggle to render the Graph view, causing the browser to hang while waiting for a response that never came.

Safari appeared to be the problem, but it was actually a symptom of the underlying memory constraints on the t3.small instance.

### Solution
The Safari freezing was resolved by the memory optimization described in [Memory Optimization Journey](#memory-optimization-journey):
- Reduced Airflow parallelism from 8 to 4
- Reduced DAG concurrency from 4 to 2
- Added 2GB swap space

After these changes, the Graph view loads smoothly in Safari without any freezing.

### Key Takeaway
**Always investigate server-side issues first** before assuming client-side browser problems. What appeared to be a Safari rendering bug was actually the Airflow webserver struggling under memory pressure.

---

## Snowflake External Table Not Refreshing

### Symptom
- New Parquet files appear in S3 bucket
- `wait_for_s3_files` task succeeds (confirms S3 files exist)
- Bronze layer query in Snowflake shows old row count (doesn't include new data)
- DBT transformations run but process stale data

### Root Cause
Snowflake external tables **do not automatically detect new files** added to the S3 location. The external table metadata must be explicitly refreshed to scan for new files.

From Snowflake documentation:
> "External tables do not automatically refresh their metadata when new files are added to the stage. You must manually refresh the metadata."

### Solution
This is why the `refresh_snowflake_table` task was added to the pipeline:
```python
refresh_snowflake_table = SnowflakeOperator(
    task_id='refresh_snowflake_table',
    snowflake_conn_id='snowflake_default',
    sql="ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;",
    dag=dag
)
```

This task runs after `wait_for_s3_files` and before DBT transformations, ensuring Snowflake sees all new S3 files.

### Why This Matters
Without explicit refresh:
- Bronze layer appears empty or stale
- Silver/Gold layers process incomplete data
- Data quality checks may fail incorrectly
- Pipeline appears successful but data is missing

### Verification
Check if refresh is working:
```sql
-- Before refresh
SELECT COUNT(*) FROM SPOTIFY_DATA.BRONZE.plays;  -- Old count

-- After refresh task runs
SELECT COUNT(*) FROM SPOTIFY_DATA.BRONZE.plays;  -- Should include new events
```

---

## Memory Optimization Journey

### Original Problem
With initial Airflow configuration:
- `parallelism=8` (max tasks across all DAGs)
- `dag_concurrency=4` (max tasks per DAG)

The EC2 instance (t3.small with 2GB RAM) experienced:
- Out of Memory (OOM) errors
- Docker containers crashing
- Tasks failing with exit code 137 (killed by OOM killer)
- Scheduler becoming unresponsive

### Investigation
- t3.small has only 2GB of physical RAM
- Running 8 parallel tasks (each consuming ~200-300MB) exceeded available memory
- No swap space configured initially
- Docker containers competing for limited memory resources

### Solution (Two-Part)

**Part 1: Reduce Airflow Parallelism**

Updated `docker-compose.yml`:
```yaml
environment:
  AIRFLOW__CORE__PARALLELISM: 4      # Down from 8
  AIRFLOW__CORE__DAG_CONCURRENCY: 2   # Down from 4
```

**Part 2: Add Swap Space**

Added 2GB of virtual memory on EC2:
```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### Results
- **Stable operation:** No OOM errors or container crashes since changes
- **Trade-off accepted:** Pipeline runtime increased from ~4 minutes to ~6 minutes
- **System health:** Memory usage stays within safe limits

### Memory Usage Patterns
With optimized settings:
- Idle: ~800MB used
- During pipeline run: ~1.5-1.8GB used (physical + swap)
- Peak during parallel tasks: ~2GB total (stays under 2GB physical + 2GB swap limit)

### Key Takeaway
**Prioritized stability over speed.** A 6-minute pipeline that runs reliably is better than a 4-minute pipeline that crashes randomly. For a portfolio project, demonstrating production-ready reliability is more important than raw performance.

---

## Dashboard Docker Limitations

### Issue
The operational dashboard (`scripts/dashboard.py`) cannot check Docker container status from inside the Airflow scheduler container.

### Symptom
Dashboard shows all metrics (pipeline health, data metrics, monitoring status) but cannot report on Docker container health (running/stopped).

### Root Cause
The `docker ps` command requires access to the Docker socket (`/var/run/docker.sock`). The Airflow containers do not have this socket mounted for security reasons.

### Why This Is Expected Behavior
This is **not a bug** - it's a deliberate design decision:
- Giving containers access to Docker socket is a security risk
- Container should not need to manage its own host infrastructure
- Dashboard runs inside the scheduler container, isolated from host Docker daemon

### Workaround
Check Docker container status from the EC2 host instead:
```bash
# On EC2 host (not inside container)
docker-compose ps

# Or for detailed status
docker-compose ps --all
```

### Dashboard Still Useful
The dashboard provides valuable metrics even without container status:
- Pipeline execution status (success/failure counts)
- Data volume metrics (Bronze/Silver/Gold row counts)
- Monitoring DAG health
- Latest pipeline run information

Container health is best monitored from the host level, which is where infrastructure monitoring typically happens in production environments.

---

## Kafka Consumer Hanging (Potential Issue)

### Symptom (If It Occurs)
- `start_consumer` task succeeds
- Consumer process appears to start
- Events are generated but not appearing in S3
- `wait_for_s3_files` task times out
- Consumer logs show no activity

### Potential Root Causes
1. **Stale PID file:** Previous consumer didn't clean up `/tmp/kafka_consumer.pid`
2. **Zombie process:** Consumer process exists but isn't actually consuming messages
3. **Kafka connection issue:** Consumer can't connect to Kafka broker

### Diagnostic Steps

**Check if PID file exists:**
```bash
docker-compose exec airflow-scheduler ls -la /tmp/kafka_consumer.pid
```

**Check if process is actually running:**
```bash
docker-compose exec airflow-scheduler ps aux | grep kafka
```

**Check consumer logs:**
```bash
docker-compose exec airflow-scheduler cat /tmp/kafka_consumer.log
```

### Solution Steps

**Step 1: Kill any stale processes**
```bash
docker-compose exec airflow-scheduler pkill -f "python.*kafka_consumer"
```

**Step 2: Remove stale PID file**
```bash
docker-compose exec airflow-scheduler rm -f /tmp/kafka_consumer.pid
```

**Step 3: Restart the consumer task**
Clear the task in Airflow UI and trigger a fresh run.

### Prevention
The `stop_consumer` task is designed to handle cleanup gracefully:
- Reads PID from `/tmp/kafka_consumer.pid`
- Sends SIGTERM to process
- Removes PID file
- Logs shutdown status

Always let the pipeline complete normally (including `stop_consumer`) rather than manually killing tasks.

### Note
This issue has **not been encountered** in this project but is documented as a potential scenario to watch for, given the complexity of background process management in Airflow.

---

## General Troubleshooting Tips

### Check Airflow Logs
```bash
# View scheduler logs
docker-compose logs airflow-scheduler

# View webserver logs
docker-compose logs airflow-webserver

# Follow logs in real-time
docker-compose logs -f airflow-scheduler
```

### Check Task Logs in UI
1. Navigate to DAG → Grid view
2. Click on task instance
3. Click "Log" button
4. Look for error messages and stack traces

### Restart Airflow Services
```bash
# Stop all services
docker-compose down

# Start fresh
docker-compose up -d

# Verify all containers running
docker-compose ps
```

### Check Disk Space
```bash
# Airflow logs can fill up disk
df -h

# Clean up old logs if needed
docker-compose exec airflow-scheduler find /opt/airflow/logs -name "*.log" -mtime +7 -delete
```

### Check Snowflake Connection
```bash
# Test connection from inside scheduler
docker-compose exec airflow-scheduler python -c "
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
conn = hook.get_conn()
print('Connection successful!')
"
```

---

## When to Get Help

If you encounter issues not covered here:

1. **Check Airflow logs first** - Most issues have clear error messages in logs
2. **Search Airflow documentation** - https://airflow.apache.org/docs/
3. **Check provider documentation** - Snowflake, AWS providers have specific gotchas
4. **Review DAG code** - Double-check task dependencies and configurations
5. **Verify connections** - Ensure all Airflow connections are properly configured

Most runtime issues fall into these categories:
- **Permissions** (like DBT target directory)
- **Memory** (optimize settings for your instance size)
- **Connections** (check credentials and network access)
- **Dependencies** (ensure task ordering is correct)
