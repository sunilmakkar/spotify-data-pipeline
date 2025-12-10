# Spotify Data Pipeline - Complete Setup Guide

This guide provides step-by-step instructions to deploy the entire Spotify Data Pipeline from scratch.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [AWS Setup](#aws-setup)
3. [Snowflake Setup](#snowflake-setup)
4. [EC2 Instance Setup](#ec2-instance-setup)
5. [Kafka Installation](#kafka-installation)
6. [Project Deployment](#project-deployment)
7. [Airflow Configuration](#airflow-configuration)
8. [DBT Setup](#dbt-setup)
9. [Verification](#verification)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before starting, ensure you have:

- AWS Account with billing enabled
- Snowflake Account (free trial available)
- GitHub Account
- Local machine with:
  - Python 3.13.2+
  - Git
  - SSH client
  - AWS CLI 2.32.12+

---

## AWS Setup

### Step 1: Create IAM User

1. **Login to AWS Console** → Navigate to IAM

2. **Create User:**
   ```
   User name: spotify-pipeline-user
   Access type: Programmatic access
   ```

3. **Attach Policies:**
   - `AmazonS3FullAccess`
   - Create custom policy: `snowflake-s3-access-policy`
   
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": [
                   "s3:GetObject",
                   "s3:GetObjectVersion",
                   "s3:ListBucket",
                   "s3:GetBucketLocation"
               ],
               "Resource": [
                   "arn:aws:s3:::spotify-data-lake-sunil-2025",
                   "arn:aws:s3:::spotify-data-lake-sunil-2025/*"
               ]
           }
       ]
   }
   ```

4. **Save Credentials:**
   - Access Key ID
   - Secret Access Key
   - Store securely (needed for `.env` file)

### Step 2: Create S3 Bucket

```bash
# Configure AWS CLI
aws configure
# Enter: Access Key ID, Secret Access Key, Region (us-east-2), Output format (json)

# Create S3 bucket
aws s3 mb s3://spotify-data-lake-sunil-2025 --region us-east-2

# Verify creation
aws s3 ls
```

### Step 3: Create EC2 Key Pair

```bash
# Create key pair
aws ec2 create-key-pair \
    --key-name spotify-pipeline-key \
    --region us-east-2 \
    --query 'KeyMaterial' \
    --output text > ~/spotify-pipeline-key.pem

# Set permissions
chmod 400 ~/spotify-pipeline-key.pem
```

### Step 4: Launch EC2 Instance

1. **Navigate to EC2 Dashboard** → Launch Instance

2. **Configuration:**
   - **Name:** `spotify-airflow-orchestrator`
   - **AMI:** Ubuntu Server 24.04 LTS
   - **Instance Type:** t3.small (2 vCPU, 2GB RAM)
   - **Key Pair:** spotify-pipeline-key
   - **Storage:** 20GB gp3 SSD
   - **Region:** us-east-2

3. **Security Group Configuration:**
   
   **Inbound Rules:**
   ```
   Type: SSH          | Port: 22   | Source: Your IP
   Type: Custom TCP   | Port: 8082 | Source: Your IP  (Airflow UI)
   Type: Custom TCP   | Port: 9092 | Source: 0.0.0.0/0 (Kafka)
   ```

4. **Launch Instance** and note the Public IP address

---

## Snowflake Setup

### Step 1: Create Snowflake Account

1. Go to https://signup.snowflake.com/
2. Select region: **us-east-2** (Ohio)
3. Complete registration
4. Note your account identifier (format: `uxojbrg-rm52997`)

### Step 2: Create Database & Warehouse

Login to Snowflake and run:

```sql
-- Create warehouse (compute resource)
CREATE WAREHOUSE SPOTIFY_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Use the warehouse
USE WAREHOUSE SPOTIFY_WH;

-- Create database
CREATE DATABASE IF NOT EXISTS SPOTIFY_DATA;
USE DATABASE SPOTIFY_DATA;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Verify
SHOW DATABASES;
SHOW WAREHOUSES;
SHOW SCHEMAS IN DATABASE SPOTIFY_DATA;
```

### Step 3: Create Storage Integration

```sql
USE SCHEMA BRONZE;

-- Create storage integration for S3 access
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-data-lake-sunil-2025/raw_events/');

-- Get Snowflake's AWS user ARN (needed for IAM trust relationship)
DESC STORAGE INTEGRATION s3_integration;
```

**Note the values:**
- `STORAGE_AWS_IAM_USER_ARN`
- `STORAGE_AWS_EXTERNAL_ID`

### Step 4: Configure AWS IAM Role for Snowflake

**In AWS Console:**

1. **Create IAM Role:**
   - Service: EC2 (temporarily, will modify trust policy)
   - Name: `snowflake-s3-role`

2. **Attach Policy:** Same policy as earlier
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": [
                   "s3:GetObject",
                   "s3:GetObjectVersion",
                   "s3:ListBucket",
                   "s3:GetBucketLocation"
               ],
               "Resource": [
                   "arn:aws:s3:::spotify-data-lake-sunil-2025",
                   "arn:aws:s3:::spotify-data-lake-sunil-2025/*"
               ]
           }
       ]
   }
   ```

3. **Edit Trust Relationship:**
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Principal": {
                   "AWS": "YOUR_STORAGE_AWS_IAM_USER_ARN"
               },
               "Action": "sts:AssumeRole",
               "Condition": {
                   "StringEquals": {
                       "sts:ExternalId": "YOUR_STORAGE_AWS_EXTERNAL_ID"
                   }
               }
           }
       ]
   }
   ```

### Step 5: Create External Stage & Table

**Back in Snowflake:**

```sql
USE SCHEMA BRONZE;

-- Create stage pointing to S3
CREATE OR REPLACE STAGE bronze_stage
    URL = 's3://spotify-data-lake-sunil-2025/raw_events/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = (TYPE = PARQUET);

-- Test stage
LIST @bronze_stage;

-- Create external table
CREATE OR REPLACE EXTERNAL TABLE plays
WITH LOCATION = @bronze_stage
PATTERN = '.*year=.*/month=.*/day=.*/hour=.*/.*[.]parquet'
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = FALSE;

-- Refresh to detect existing files (run after first pipeline run)
ALTER EXTERNAL TABLE plays REFRESH;
```

---

## EC2 Instance Setup

### Step 1: SSH into EC2

```bash
ssh -i ~/spotify-pipeline-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

### Step 2: Update System & Install Dependencies

```bash
# Update package list
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y \
    python3.13 \
    python3-pip \
    python3-venv \
    docker.io \
    docker-compose \
    git \
    openjdk-11-jdk

# Add user to docker group
sudo usermod -aG docker ubuntu
newgrp docker

# Verify installations
python3 --version
docker --version
docker-compose --version
java -version
```

### Step 3: Configure Swap Space (for t3.small)

```bash
# Create 2GB swap file
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Make swap permanent
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Verify swap is active
free -h
```

---

## Kafka Installation

### Step 1: Download & Install Kafka

```bash
# Create directory
cd ~
mkdir kafka && cd kafka

# Download Kafka
wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1
```

### Step 2: Start Kafka Services

```bash
# Start Zookeeper (in background)
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Wait 10 seconds for Zookeeper to start
sleep 10

# Start Kafka broker (in background)
bin/kafka-server-start.sh config/server.properties &

# Wait 10 seconds for Kafka to start
sleep 10
```

### Step 3: Create Kafka Topics

```bash
# Create spotify-plays topic
bin/kafka-topics.sh --create \
    --topic spotify-plays \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Create other topics
bin/kafka-topics.sh --create --topic spotify-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic spotify-likes --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic spotify-skips --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verify topics created
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 4: Make Kafka Services Persistent (Optional)

Create systemd services so Kafka starts automatically:

```bash
# Create Zookeeper service
sudo nano /etc/systemd/system/zookeeper.service
```

```ini
[Unit]
Description=Apache Zookeeper Server
After=network.target

[Service]
Type=simple
User=ubuntu
ExecStart=/home/ubuntu/kafka/kafka_2.13-3.5.1/bin/zookeeper-server-start.sh /home/ubuntu/kafka/kafka_2.13-3.5.1/config/zookeeper.properties
ExecStop=/home/ubuntu/kafka/kafka_2.13-3.5.1/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

```bash
# Create Kafka service
sudo nano /etc/systemd/system/kafka.service
```

```ini
[Unit]
Description=Apache Kafka Server
After=network.target zookeeper.service

[Service]
Type=simple
User=ubuntu
ExecStart=/home/ubuntu/kafka/kafka_2.13-3.5.1/bin/kafka-server-start.sh /home/ubuntu/kafka/kafka_2.13-3.5.1/config/server.properties
ExecStop=/home/ubuntu/kafka/kafka_2.13-3.5.1/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

```bash
# Enable services
sudo systemctl daemon-reload
sudo systemctl enable zookeeper
sudo systemctl enable kafka

# Start services
sudo systemctl start zookeeper
sudo systemctl start kafka

# Check status
sudo systemctl status zookeeper
sudo systemctl status kafka
```

---

## Project Deployment

### Step 1: Clone Repository

```bash
cd ~
git clone https://github.com/sunilmakkar/spotify-data-pipeline.git
cd spotify-data-pipeline
```

### Step 2: Create Environment File

```bash
nano .env
```

Add the following (replace with your actual values):

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
S3_BUCKET=spotify-data-lake-sunil-2025
AWS_REGION=us-east-2

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_API_KEY=
KAFKA_API_SECRET=

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_WAREHOUSE=SPOTIFY_WH
SNOWFLAKE_DATABASE=SPOTIFY_DATA
SNOWFLAKE_SCHEMA=RAW

# Spotify Configuration (for Phase 5)
SPOTIFY_CLIENT_ID=
SPOTIFY_CLIENT_SECRET=
SPOTIFY_REDIRECT_URI=http://localhost:8888/callback

# Gmail SMTP (for Airflow alerts)
GMAIL_USER=your_email@gmail.com
GMAIL_APP_PASSWORD=your_16_char_app_password

# Airflow
AIRFLOW_UID=50000
```

**Save and exit** (Ctrl+X, Y, Enter)

### Step 3: Install Python Dependencies

```bash
pip install -r requirements.txt --break-system-packages
```

---

## Airflow Configuration

### Step 1: Build Airflow Docker Image

```bash
# Build custom Airflow image
docker-compose build

# Verify image was built
docker images | grep spotify-airflow
```

### Step 2: Initialize Airflow Database

```bash
# Start containers
docker-compose up -d

# Wait 2 minutes for initialization

# Verify containers are running
docker-compose ps
```

**Expected output:**
```
NAME                              STATUS
spotify-airflow-scheduler         Up (healthy)
spotify-airflow-webserver         Up (healthy)
postgres                          Up (healthy)
```

### Step 3: Access Airflow UI

1. **Open browser:** `http://YOUR_EC2_PUBLIC_IP:8082`
2. **Login:**
   - Username: `airflow`
   - Password: `airflow`

3. **Verify DAGs loaded:**
   - `spotify_data_basic` (production pipeline)
   - `data_quality_monitoring` (monitoring pipeline)

---

## DBT Setup

### Step 1: Configure DBT Profile

DBT profile is already configured and mounted into Docker. Verify:

```bash
cat dbt/profiles.yml
```

Should contain:

```yaml
spotify_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ACCOUNTADMIN
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: SILVER
      threads: 4
      client_session_keep_alive: False
```

### Step 2: Test DBT Connection

```bash
# Test DBT inside Airflow container
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt debug"
```

**Expected output:** `All checks passed!`

### Step 3: Run DBT Models (First Time)

```bash
# Compile DBT
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt compile"

# Run Silver layer
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --select silver"

# Run Gold layer
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --select gold"

# Run tests
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test"
```

### Step 4: Understanding DBT Tests

DBT tests validate data quality across all layers. Tests are defined in `dbt/models/schema.yml`.

**Configured Tests:**

**Silver Layer (`silver_plays`):**
- `unique` - Ensures event_id has no duplicates
- `not_null` - Validates required fields (event_id, user_id, track_id, played_at)
- `accepted_values` - Checks event_type is valid ('play', 'skip', 'like')
- `relationships` - Validates foreign keys (not applicable in this schema)

**Gold Layer (`daily_user_stats`):**
- `unique` - Ensures date + user_id combination is unique
- `not_null` - Validates aggregated metrics exist
- Custom tests - Checks `total_plays > 0` and `unique_tracks > 0`

**Gold Layer (`top_tracks`):**
- `unique` - Ensures track_id has no duplicates
- `not_null` - Validates track metrics
- Custom test - Checks `rank` is between 1 and 5

**Gold Layer (`top_artists`):**
- `unique` - Ensures artist_name has no duplicates
- `not_null` - Validates artist metrics
- Custom test - Checks `rank` is between 1 and 5

**Gold Layer (`device_usage`):**
- `unique` - Ensures device_type has no duplicates
- `not_null` - Validates device metrics
- Custom test - Checks `play_percentage` sums to ~100%

**Running Specific Tests:**

```bash
# Test only Silver layer
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test --select silver"

# Test only Gold layer
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test --select gold"

# Test specific model
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test --select daily_user_stats"
```

**Interpreting Test Results:**

- **PASS** (green) - Data meets quality standards
- **FAIL** (red) - Data quality issue detected
- **ERROR** (red) - Test configuration or SQL error

**Test failures indicate:**
- Duplicate records (uniqueness test)
- Missing critical data (not_null test)
- Invalid values (accepted_values test)
- Data consistency issues (custom tests)

**When tests fail in production:**
1. Check Airflow logs for specific failure details
2. Query the failing table in Snowflake to inspect data
3. Review upstream tasks (Bronze → Silver transformation)
4. Fix data generation logic if systematic issue found

---

## Verification

### Step 1: Generate Test Data

Manually trigger the pipeline from Airflow UI:

1. Go to `http://YOUR_EC2_PUBLIC_IP:8082`
2. Find `spotify_data_basic` DAG
3. Click play button (▶) → Trigger DAG
4. Wait ~6 minutes for completion
5. Verify all 10 tasks are green

### Step 2: Verify Data in S3

```bash
# List S3 files
aws s3 ls s3://spotify-data-lake-sunil-2025/raw_events/ --recursive
```

**Expected output:** Parquet files in date/hour partitions

### Step 3: Verify Data in Snowflake

```sql
-- Refresh external table
ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;

-- Check Bronze layer
SELECT COUNT(*) as total_events
FROM SPOTIFY_DATA.BRONZE.plays;
-- Expected: 200 events

-- Check Silver layer
SELECT COUNT(*) as total_events
FROM SPOTIFY_DATA.SILVER.silver_plays;
-- Expected: 200 events

-- Check Gold layer
SELECT * FROM SPOTIFY_DATA.GOLD.daily_user_stats;
-- Expected: 1 row with stats for today
```

### Step 4: Verify Monitoring DAG

1. In Airflow UI, find `data_quality_monitoring` DAG
2. Toggle ON (enable)
3. Wait 30 minutes for first run
4. Verify 6 tasks complete successfully

---

## Troubleshooting

### Kafka Issues

**Problem:** Kafka topics not created
```bash
# Check if Kafka is running
ps aux | grep kafka

# Restart Kafka services
sudo systemctl restart zookeeper
sudo systemctl restart kafka
```

**Problem:** Consumer can't connect to Kafka
```bash
# Check Kafka logs
journalctl -u kafka -f
```

### Airflow Issues

**Problem:** Containers crash with OOM (Out of Memory)
```bash
# Verify swap is active
free -h

# Restart Docker
docker-compose down
docker-compose up -d
```

**Problem:** DBT tasks fail with permission errors
```bash
# Fix DBT target permissions
chmod -R 777 dbt/target/
```

**Problem:** Tasks stuck in "queued" state
```bash
# Check Airflow scheduler logs
docker-compose logs airflow-scheduler

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Snowflake Issues

**Problem:** External table not refreshing
```sql
-- Manually refresh
ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;

-- Check what files Snowflake sees
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(TABLE_NAME => 'PLAYS'));
```

**Problem:** Storage integration failing
```bash
# Verify IAM role trust relationship in AWS
# Verify STORAGE_AWS_IAM_USER_ARN matches Snowflake's ARN
# Verify STORAGE_AWS_EXTERNAL_ID is correct
```

### EC2 Issues

**Problem:** Can't SSH into EC2
```bash
# Verify security group allows SSH from your IP
# Verify key permissions
chmod 400 ~/spotify-pipeline-key.pem
```

**Problem:** Airflow UI not accessible
```bash
# Verify security group allows port 8082
# Check if containers are running
docker-compose ps
```

---

## Next Steps

After successful deployment:

1. **Run pipeline multiple times** to generate historical data
2. **Enable monitoring DAG** for continuous data quality checks
3. **Deploy Streamlit dashboard** (see README.md)
4. **Phase 5:** Integrate real Spotify API (future enhancement)

---

## Useful Commands

```bash
# SSH into EC2
ssh -i ~/spotify-pipeline-key.pem ubuntu@YOUR_EC2_IP

# Check Docker containers
docker-compose ps

# View Airflow logs
docker-compose logs airflow-scheduler --tail=100

# Restart Airflow
docker-compose restart

# Stop all services
docker-compose down

# Rebuild and restart
docker-compose up -d --build

# Check Kafka topics
~/kafka/kafka_2.13-3.5.1/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Support

For issues or questions:
- Check `docs/airflow/troubleshooting.md`
- Review Airflow logs: `docker-compose logs`
- Check GitHub Issues: https://github.com/sunilmakkar/spotify-data-pipeline/issues

---

**Project Complete!** You now have a fully functional data pipeline.