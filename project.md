# CANONICAL PROJECT DOCUMENTATION

**Project:** Spotify Data Platform Recreation

**Objective:**
Build a production-grade data pipeline that mimics Spotify's data infrastructure - from event generation through transformations to analytics and recommendations. Demonstrate end-to-end data engineering skills using industry-standard tools.

---

## Why This Project Matters

### For Hiring Managers:
- Proves you understand event-driven architectures (most modern systems)
- Shows full data lifecycle: streaming → storage → transformation → analytics
- Uses exact tools they use in production (Kafka, S3, Snowflake, DBT, Airflow)
- Demonstrates you can build, not just talk about data pipelines

### For You:
- Portfolio piece that gets interviews
- Hands-on experience with 6+ core data engineering tools
- Real problem solving (you'll hit actual production issues)
- Blog post + video content for visibility

---

## Architecture Overview
```
┌─────────────────┐
│  Spotify API    │ (Event Source)
│  or Simulator   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Topics   │ (Streaming Layer)
│  - plays        │
│  - skips        │
│  - likes        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AWS S3        │ (Data Lake - Bronze Layer)
│  Parquet Files  │
│  Partitioned    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowflake     │ (Data Warehouse)
│  External       │
│  Tables         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DBT Models    │ (Transformation Layer)
│  Bronze→Silver  │
│  Silver→Gold    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Airflow       │ (Orchestration)
│   DAGs          │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Dashboard      │ (Analytics Layer)
│  Streamlit      │
└─────────────────┘
```

---

## Tech Stack & Why

| Tool | Purpose | Why This Tool | Cost |
|------|---------|---------------|------|
| Kafka (Confluent) | Message streaming | Industry standard for real-time data, 80%+ market share | $0 (free tier) |
| AWS S3 | Data lake storage | Universal standard, every company uses it | $1-3/month |
| Snowflake | Cloud data warehouse | Most common in job postings (70%+), best for analytics | $0 for 3 months (trial), then $10-20/month |
| DBT Core | Data transformation | Standard for analytics engineering, SQL-based | $0 (open source) |
| Apache Airflow | Workflow orchestration | 70% market share, Airbnb/Reddit/Adobe use it | $0 (EC2 free tier) |
| Python | Scripting/automation | Universal language for data engineering | $0 |
| Streamlit | Dashboard/visualization | Fast prototyping, free hosting | $0 |

**Total Monthly Cost:** $1-3 during trial, $15-25 after

---

## Data Flow Explained

### 1. Event Generation (What Happens When You Listen):
- User plays "Blinding Lights" by The Weeknd
- Event generated: `{user_id, track_id, artist, timestamp, duration, skipped, device}`
- Event sent to Kafka topic `user_plays`

### 2. Streaming (Kafka):
- Kafka receives millions of events
- Topics organize events by type (plays, skips, likes)
- Consumers read from topics and process

### 3. Data Lake (S3):
- Consumer writes events to S3 as Parquet files
- Organized: `s3://bucket/plays/year=2025/month=11/day=18/hour=14/data.parquet`
- Partitioning enables efficient querying (only scan relevant dates)

### 4. Data Warehouse (Snowflake):
- External tables point to S3 Parquet files
- Can query S3 data directly from Snowflake
- Separation of storage (S3) and compute (Snowflake) = cost efficiency

### 5. Transformations (DBT):
- **Bronze:** Raw events exactly as received (no changes)
- **Silver:** Cleaned, deduplicated, validated, type-cast
- **Gold:** Business metrics (daily listening stats, artist popularity, user preferences)

### 6. Orchestration (Airflow):
- Schedules pipeline runs (hourly/daily)
- Dependencies: ingest → validate → transform → quality checks
- Monitoring, alerting, retry logic

### 7. Analytics (Dashboard):
- **Real-time:** What's playing now across all users
- **Historical:** Your listening trends over time
- **Recommendations:** Songs you might like based on patterns

---

## Key Technical Concepts You'll Learn

### Event-Driven Architecture:
- Decoupling producers from consumers
- Multiple systems reading same events (fan-out pattern)
- At-least-once delivery guarantees

### Data Lake Patterns:
- Bronze/Silver/Gold medallion architecture
- Partitioning strategies for performance
- Parquet columnar format advantages

### Data Warehouse Optimization:
- External tables vs loaded tables
- Query performance tuning
- Cost management (auto-suspend, warehouse sizing)

### Data Quality:
- Schema validation
- Uniqueness/not-null constraints
- Anomaly detection (sudden drops in volume)

### Orchestration:
- DAG design (directed acyclic graphs)
- Idempotency (safe to rerun)
- Backfilling historical data

---

## What Makes This Production-Grade

### ❌ Toy Project:
- CSV files
- Single Python script
- No error handling
- Manual execution
- No monitoring

### ✅ Your Project:
- Streaming data pipeline
- Separate concerns (ingest/transform/serve)
- Data quality checks
- Automated orchestration
- Observability (logs, metrics)
- Proper partitioning
- Cost optimization
- Documentation

---

## Success Metrics

### Technical:
- ✅ 10,000+ events flowing through pipeline
- ✅ End-to-end latency < 5 minutes
- ✅ DBT tests all passing
- ✅ Airflow DAG runs without failures
- ✅ Dashboard loads in < 3 seconds

### Interview Ready:
- ✅ Can explain every component in 2 minutes
- ✅ Can answer "why Kafka over SQS?"
- ✅ Can explain partitioning strategy
- ✅ Can demo live during interview
- ✅ GitHub README explains setup clearly