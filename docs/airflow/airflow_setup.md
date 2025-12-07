# Airflow Setup Guide

This guide walks through setting up Apache Airflow on AWS EC2 for the Spotify data pipeline. Follow these steps to recreate the orchestration environment from scratch.

---

## Prerequisites

Before starting the Airflow setup, ensure you have the following:

### AWS Account & Resources

**EC2 Instance:**
- Instance type: `t3.small` or larger (minimum 2GB RAM)
- Operating system: Ubuntu 24.04 LTS
- Storage: 20GB EBS volume minimum
- **Swap space:** 2GB swap file configured (required for t3.small to prevent OOM errors)
- Security group rules:
  - Port 22 (SSH) - Your IP only
  - Port 8082 (Airflow webserver) - Your IP only
  - Port 9092 (Kafka) - Internal only (if running Kafka on same instance)

**S3 Bucket:**
- Bucket name: `spotify-data-lake-sunil-2025`
- Region: Same as EC2 instance
- Permissions: EC2 instance needs read/write access via IAM role

**IAM Role for EC2:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::spotify-data-pipeline-sunil-2025",
                "arn:aws:s3:::spotify-data-pipeline-sunil-2025/*"
            ]
        }
    ]
}
```

### Snowflake Account

**Required setup:**
- Active Snowflake account (free trial works)
- Database created: `SPOTIFY_DATA`
- Schemas created: `BRONZE`, `SILVER`, `GOLD`
- Warehouse created: `SPOTIFY_WH` (X-Small minimum)
- User with permissions:
  - `USAGE` on database and schemas
  - `CREATE TABLE` on Silver and Gold schemas
  - `SELECT` on Bronze external table
  - `OPERATE` on warehouse

**Connection details you'll need:**
- Account identifier (e.g., `abc12345.us-east-1`)
- Username
- Password
- Role (e.g., `ACCOUNTADMIN` or custom role)
- Database: `SPOTIFY_DATA`
- Schema: `SILVER` (default for connections)
- Warehouse: `COMPUTE_WH`

### Email Account for Alerts

**Gmail SMTP (recommended for development):**
- Gmail account
- App-specific password (not your regular Gmail password)
  - Generate at: https://myaccount.google.com/apppasswords
  - Requires 2-factor authentication enabled

**Alternative options:**
- AWS SES (for production)
- Custom SMTP server

### Local Development Machine

**Required software:**
- SSH client (terminal on Mac/Linux, PuTTY on Windows)
- Text editor (VS Code, Sublime, etc.) for editing files
- Git (for cloning repository and version control)
- Web browser (Chrome/Firefox recommended, Safari has known issues with Airflow UI)

### Project Files

**Clone the repository:**
```bash
git clone https://github.com/sunilmakkar/spotify-data-pipeline.git
cd spotify-data-pipeline
```

**Required files/directories:**
- `dags/` - Airflow DAG definitions
- `src/` - Python scripts for event generation and Kafka
- `dbt_spotify/` - DBT project with transformations
- `scripts/` - Utility scripts (dashboard, etc.)
- `sql/`- DDL and data validation scripts
- `docker-compose.yml` - Airflow container orchestration
- `Dockerfile` - Custom Airflow image with dependencies

### Knowledge Prerequisites

**Helpful but not required:**
- Basic Docker concepts (containers, images, volumes)
- Linux command line navigation
- Understanding of environment variables
- SSH key pair authentication

**If you're new to Docker:**
- Containers are isolated processes that package application + dependencies
- Docker Compose orchestrates multiple containers (Airflow scheduler, webserver, postgres)
- Volumes mount local directories into containers (so Airflow can see your DAG files)

---

## Verification Checklist

Before proceeding, confirm:
- [ ] EC2 instance running and accessible via SSH
- [ ] S3 bucket created and IAM role attached to EC2
- [ ] Snowflake account active with database/schemas created
- [ ] Gmail app password generated (if using Gmail SMTP)
- [ ] Repository cloned to local machine
- [ ] SSH key pair downloaded for EC2 access

## Docker Installation on EC2

### Step 1: Connect to EC2 Instance
```bash
# Replace with your EC2 public IP and key file path
ssh -i ~/path/to/your-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

**Verify connection:**
```bash
# Should show Ubuntu 24.04
lsb_release -a

# Check available disk space (should have at least 10GB free)
df -h
```

### Step 2: Configure Swap Space

The t3.small instance has only 2GB RAM. Add 2GB swap to prevent out-of-memory errors when running Airflow.
```bash
# Create 2GB swap file
sudo fallocate -l 2G /swapfile

# Set correct permissions (required for security)
sudo chmod 600 /swapfile

# Format as swap
sudo mkswap /swapfile

# Enable swap immediately
sudo swapon /swapfile

# Make swap persistent across reboots
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Verify swap is active
free -h
```

**Expected output from `free -h`:**
```
              total        used        free      shared  buff/cache   available
Mem:           1.9Gi       400Mi       1.2Gi       1.0Mi       300Mi       1.4Gi
Swap:          2.0Gi          0B       2.0Gi
```

You should see 2.0Gi in the Swap row.

### Step 3: Install Docker
```bash
# Update package index
sudo apt-get update

# Install prerequisites
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine and Docker Compose
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose

# Add current user to docker group (avoid using sudo for docker commands)
sudo usermod -aG docker $USER

# Apply group membership (or logout/login)
newgrp docker
```

**Verify Docker installation:**
```bash
# Check Docker version
docker --version
# Expected: Docker version 24.0.x or higher

# Check Docker Compose version
docker-compose --version
# Expected: docker-compose version 1.29.x or higher

# Test Docker works
docker run hello-world
# Should download and run a test container
```

### Step 4: Configure Docker for Production Use

**Increase Docker storage and configure logging:**
```bash
# Check current disk usage
docker system df

# Configure Docker daemon settings
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json > /dev/null <<EOF
{
  "data-root": "/var/lib/docker",
  "storage-driver": "overlay2",
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF

# Restart Docker to apply changes
sudo systemctl restart docker

# Verify Docker is running
sudo systemctl status docker
```

**Why these settings?**
- `data-root`: Where Docker stores images/containers
- `storage-driver`: overlay2 is fastest and most stable for Ubuntu
- `log-opts`: Limits container logs to 30MB total (10MB × 3 files) to prevent disk fill

### Step 5: Clone Project Repository
```bash
# Navigate to home directory
cd ~

# Clone the repository
git clone https://github.com/sunilmakkar/spotify-data-pipeline.git

# Enter project directory
cd spotify-data-pipeline

# Verify all required files exist
ls -la
# Should see: dags/, src/, dbt_spotify/, scripts/, sql/, docker-compose.yml, Dockerfile
```

### Step 6: Verify Setup
```bash
# Check Docker is accessible without sudo
docker ps
# Should show empty list (no containers running yet)

# Verify Docker Compose works
docker-compose --version

# Check swap is active
free -h | grep Swap
# Should show 2.0Gi total

# Check disk space
df -h /
# Should have at least 10GB free
```

### Troubleshooting

**Issue: "Permission denied" when running docker commands**

Solution:
```bash
# Verify you're in the docker group
groups
# Should include 'docker'

# If not, log out and back in, or run:
newgrp docker

# Test again
docker ps
```

**Issue: "Cannot connect to the Docker daemon"**

Solution:
```bash
# Check if Docker service is running
sudo systemctl status docker

# If not running, start it
sudo systemctl start docker

# Enable Docker to start on boot
sudo systemctl enable docker

# Verify it's running
docker ps
```

**Issue: docker-compose command not found**

Solution:
```bash
# Install docker-compose separately if missing
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Make it executable
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker-compose --version
```

**Issue: Swap not persisting after reboot**

Solution:
```bash
# Check if swap entry is in fstab
cat /etc/fstab | grep swap

# If missing, add it again
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Verify swap is active
swapon --show
```

**Issue: Not enough disk space**

Solution:
```bash
# Clean up unused Docker resources
docker system prune -a --volumes

# Check disk usage again
df -h

# If still low, consider upgrading EBS volume in AWS Console
```

**Issue: Docker daemon fails to start after configuration change**

Solution:
```bash
# Check Docker daemon logs
sudo journalctl -u docker.service --no-pager | tail -50

# If daemon.json has syntax errors, validate it
cat /etc/docker/daemon.json | python3 -m json.tool

# If invalid, recreate the file with correct syntax
sudo nano /etc/docker/daemon.json

# Restart Docker
sudo systemctl restart docker
```
## Airflow Installation

### Step 1: Understanding the Custom Dockerfile

The project uses a custom Airflow image to include additional Python packages needed for the pipeline.

**View the Dockerfile:**
```bash
cd ~/spotify-data-pipeline
cat Dockerfile
```

**Complete Dockerfile:**
```dockerfile
FROM apache/airflow:2.8.1-python3.11

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

**Why a custom image?**
- Default Airflow 2.8.1 image doesn't include Snowflake, AWS, or DBT packages
- Need dbt-snowflake, boto3, and kafka connectors for the pipeline
- Easier to version control all dependencies via requirements.txt

**Key packages in requirements.txt:**
- `dbt-core==1.10.15` and `dbt-snowflake==1.8.3` - Data transformations
- `snowflake-connector-python==3.18.0` - Snowflake connectivity
- `boto3==1.41.0` - AWS S3 operations
- `confluent-kafka==2.12.2` - Kafka event streaming
- `pandas==2.3.3` and `pyarrow==22.0.0` - Data processing
- `spotipy==2.25.1` - Spotify API integration (if needed)

### Step 2: Understanding docker-compose.yml

The docker-compose.yml orchestrates three main services plus an initialization service.

**Key sections explained:**

**1. Common Configuration (x-airflow-common):**
```yaml
x-airflow-common:
  &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  image: spotify-airflow-custom:latest
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__PARALLELISM: 4
    AIRFLOW__CORE__DAG_CONCURRENCY: 2
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
    AIRFLOW__SCHEDULER__MAX_THREADS: 1
```

**Executor choice:**
- `LocalExecutor` runs tasks on the same machine as scheduler
- Simpler than CeleryExecutor (no Redis/RabbitMQ required)
- Sufficient for current scale (200 events/hour, ~6 minute runtime)

**Memory optimization settings:**
- `PARALLELISM: 4` - Maximum 4 tasks running across all DAGs simultaneously
- `DAG_CONCURRENCY: 2` - Maximum 2 tasks running per DAG
- `MAX_ACTIVE_RUNS_PER_DAG: 1` - Only 1 DAG run instance at a time
- `MAX_THREADS: 1` - Scheduler uses single thread (reduces memory usage)

**Why these limits?**
- t3.small has 2GB RAM + 2GB swap = 4GB total
- Without limits: parallelism=8 caused OOM (out of memory) errors
- With limits: Stable operation, longer runtime but no crashes

**2. Volume Mounts:**
```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/src:/opt/airflow/src
  - ${AIRFLOW_PROJ_DIR:-.}/.env:/opt/airflow/.env
  - ${AIRFLOW_PROJ_DIR:-.}/config.py:/opt/airflow/config.py:ro
  - ${AIRFLOW_PROJ_DIR:-.}/dbt:/opt/airflow/dbt
  - ${AIRFLOW_PROJ_DIR:-.}/dbt/profiles.yml:/home/airflow/.dbt/profiles.yml:ro
```

**What each mount does:**
- `dags/` - DAG definitions (changes appear immediately, no rebuild needed)
- `logs/` - Task execution logs (persist across container restarts)
- `config/` - Airflow configuration overrides
- `plugins/` - Custom Airflow plugins
- `src/` - Python scripts for event generation and Kafka consumer
- `.env` - Environment variables (credentials, config)
- `config.py` - Application configuration
- `dbt/` - DBT project directory
- `dbt/profiles.yml` - DBT connection profiles (mounted read-only with `:ro`)

**Why read-only (`:ro`) for some files?**
- Prevents accidental modification from inside container
- profiles.yml contains Snowflake credentials (should only be edited on host)

**3. SMTP Configuration (Email Alerts):**
```yaml
AIRFLOW__SMTP__SMTP_HOST: 'smtp.gmail.com'
AIRFLOW__SMTP__SMTP_STARTTLS: 'True'
AIRFLOW__SMTP__SMTP_SSL: 'False'
AIRFLOW__SMTP__SMTP_USER: ${GMAIL_USER}
AIRFLOW__SMTP__SMTP_PASSWORD: ${GMAIL_APP_PASSWORD}
AIRFLOW__SMTP__SMTP_PORT: 587
AIRFLOW__SMTP__SMTP_MAIL_FROM: ${GMAIL_USER}
```

**Environment variable approach:**
- Credentials stored in `.env` file (not committed to git)
- `${GMAIL_USER}` and `${GMAIL_APP_PASSWORD}` are loaded from `.env` at runtime
- Keeps sensitive information out of version control

**SMTP settings explained:**
- `SMTP_HOST`: Gmail's SMTP server
- `SMTP_STARTTLS: 'True'`: Use TLS encryption (required by Gmail)
- `SMTP_SSL: 'False'`: Don't use SSL (TLS is used instead)
- `SMTP_PORT: 587`: Standard port for STARTTLS
- `SMTP_MAIL_FROM`: Email address that appears in "From" field

**4. Services:**

**postgres service:**
- Stores Airflow metadata (DAG runs, task states, connections)
- Uses persistent volume `postgres-db-volume` (survives container restarts)
- Health check ensures database is ready before Airflow starts

**airflow-webserver service:**
- Web UI accessible at `http://EC2_IP:8082`
- Port mapping: `8082:8080` (external:internal)
- Depends on `airflow-init` completing successfully

**airflow-scheduler service:**
- Orchestrates DAG execution and task scheduling
- Health check on port 8974 (scheduler health endpoint)
- Depends on `airflow-init` completing successfully

**airflow-init service:**
- One-time initialization service
- Creates admin user (username: `airflow`, password: `airflow`)
- Sets up directory permissions
- Migrates database schema

### Step 3: Create Environment Variables File

Create a `.env` file to store sensitive credentials. This file should never be committed to git.
```bash
cd ~/spotify-data-pipeline

# Create .env file
cat > .env << 'EOF'
# Airflow configuration
AIRFLOW_UID=50000

# Gmail SMTP (for email alerts)
GMAIL_USER=your-email@gmail.com
GMAIL_APP_PASSWORD=your-16-char-app-password

# Project directory
AIRFLOW_PROJ_DIR=.
EOF

# Edit the file with your actual credentials
nano .env
```

**Replace these values:**
- `GMAIL_USER`: Your Gmail address (e.g., `sunil.makkar97@gmail.com`)
- `GMAIL_APP_PASSWORD`: The 16-character app password from Google

**To generate Gmail app password:**
1. Visit: https://myaccount.google.com/apppasswords
2. Requires 2-factor authentication enabled on your Google account
3. Create app password for "Mail"
4. Copy the 16-character password (format: `xxxx xxxx xxxx xxxx`)

**Verify .env file:**
```bash
# Check contents (credentials will be visible)
cat .env

# Verify variables are set correctly
# Lines should look like:
# GMAIL_USER=sunil.makkar97@gmail.com
# GMAIL_APP_PASSWORD=abcd efgh ijkl mnop
```

**Secure the .env file:**
```bash
# Add to .gitignore (prevents committing to git)
echo ".env" >> .gitignore

# Verify it's ignored
git status
# .env should NOT appear in untracked files
```

**⚠️ IMPORTANT SECURITY NOTES:**
- Never commit `.env` file to git (contains credentials)
- Each team member needs their own `.env` file on their machine
- If `.env` is accidentally committed, rotate your Gmail app password immediately
- For production, use secrets management (AWS Secrets Manager, HashiCorp Vault)

### Step 4: Build Custom Airflow Image
```bash
# Make sure you're in the project directory
cd ~/spotify-data-pipeline

# Build the custom Airflow image (takes 5-10 minutes first time)
docker-compose build

# You'll see output showing package installations
# Look for successful installation of dbt-snowflake, boto3, etc.
```

**What happens during build:**
1. Downloads base `apache/airflow:2.8.1-python3.11` image (~1GB)
2. Copies requirements.txt into image
3. Installs all packages from requirements.txt
4. Creates image tagged as `spotify-airflow-custom:latest`

**Monitor the build:**
```bash
# If build seems stuck, check Docker logs in another terminal
docker-compose logs -f
```

**Verify the image was created:**
```bash
docker images | grep spotify-airflow-custom

# Expected output:
# spotify-airflow-custom   latest   abc123def456   2 minutes ago   2.5GB
```

### Step 5: Initialize Airflow Database

Airflow needs to initialize its metadata database before first use.
```bash
# Initialize database and create admin user
docker-compose up airflow-init

# Wait for completion (30-60 seconds)
# Look for this output:
# airflow-init_1  | Admin user airflow created
# airflow-init_1 exited with code 0
```

**What `airflow-init` does:**
1. Creates Postgres database schema (tables for DAGs, tasks, connections, etc.)
2. Runs database migrations
3. Creates admin user with credentials:
   - Username: `airflow`
   - Password: `airflow`
4. Sets correct file permissions on mounted volumes
5. Exits cleanly (code 0 = success)

**If initialization fails:**
```bash
# Check logs for errors
docker-compose logs airflow-init

# Common issues:
# - Postgres not ready (wait 30s and retry)
# - Permission errors (check AIRFLOW_UID in .env)
# - Port conflicts (ensure 8082 is available)
```

### Step 6: Start Airflow Services
```bash
# Start all services in detached mode (runs in background)
docker-compose up -d

# Verify all containers are running and healthy
docker-compose ps
```

**Expected output:**
```
NAME                                        STATUS
spotify-data-pipeline-postgres-1            Up (healthy)
spotify-data-pipeline-airflow-webserver-1   Up (healthy)
spotify-data-pipeline-airflow-scheduler-1   Up (healthy)
```

**All three containers must show "Up (healthy)"**

**Wait for health checks (30-60 seconds):**
```bash
# Watch containers become healthy
watch docker-compose ps

# Stop watching with Ctrl+C when all show (healthy)
```

**Verify environment variables loaded correctly:**
```bash
# Check SMTP credentials are available in container
docker-compose exec airflow-scheduler env | grep GMAIL

# Expected output:
# GMAIL_USER=sunil.makkar97@gmail.com
# GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
```

If these are empty, `.env` file was not loaded properly.

**Check logs if any container fails:**
```bash
# Scheduler logs (most important)
docker-compose logs airflow-scheduler | tail -50

# Webserver logs
docker-compose logs airflow-webserver | tail -50

# Follow logs in real-time
docker-compose logs -f
```

### Step 7: Access Airflow Web UI

**Open your browser and navigate to:**
```
http://YOUR_EC2_PUBLIC_IP:8082
```

Replace `YOUR_EC2_PUBLIC_IP` with your actual EC2 instance public IP.

**Login credentials:**
- Username: `airflow`
- Password: `airflow`

**What you should see after login:**
- Airflow dashboard with "DAGs" list
- Your DAGs: `spotify_data_basic` and `data_quality_monitoring`
- Navigation: DAGs, Browse, Admin tabs

**If you can't access the UI:**

1. **Check security group:**
```bash
# In AWS Console: EC2 → Security Groups → Your SG
# Ensure inbound rule exists:
# Type: Custom TCP
# Port: 8082
# Source: Your IP address
```

2. **Verify webserver is running:**
```bash
docker-compose ps | grep webserver
# Should show "Up (healthy)"
```

3. **Check webserver logs:**
```bash
docker-compose logs airflow-webserver | grep ERROR
```

4. **Test from EC2 instance:**
```bash
curl http://localhost:8082/health
# Should return: {"metadatabase":{"status":"healthy"},...}
```

### Step 8: Verify Installation

**1. Check Airflow version:**
```bash
docker-compose exec airflow-scheduler airflow version

# Expected output:
# 2.8.1
```

**2. Check installed packages:**
```bash
# Verify dbt is installed
docker-compose exec airflow-scheduler dbt --version
# Expected: installed version 1.10.15

# Verify Snowflake connector
docker-compose exec airflow-scheduler python -c "import snowflake.connector; print(snowflake.connector.__version__)"
# Expected: 3.18.0

# Verify boto3 (AWS SDK)
docker-compose exec airflow-scheduler python -c "import boto3; print(boto3.__version__)"
# Expected: 1.41.0
```

**3. Check DAG files are visible:**
```bash
docker-compose exec airflow-scheduler ls -la /opt/airflow/dags

# Expected output:
# spotify_data_basic.py
# data_quality_monitoring.py
```

**4. Check DBT project is mounted:**
```bash
docker-compose exec airflow-scheduler ls -la /opt/airflow/dbt

# Expected output:
# dbt_project.yml
# models/
# profiles.yml (symlink to /home/airflow/.dbt/profiles.yml)
```

**5. Verify source code is accessible:**
```bash
docker-compose exec airflow-scheduler ls -la /opt/airflow/src

# Expected output:
# event_simulator.py
# generate_and_produce.py
# consume_and_write.py
```

**6. Test email configuration (optional):**
```bash
docker-compose exec airflow-scheduler python -c "
from airflow.utils.email import send_email
send_email(
    to='your-email@gmail.com',
    subject='Airflow Test Email',
    html_content='<p>Test email from Airflow</p>'
)
"

# Check your inbox for test email
# If it fails, check SMTP settings in docker-compose.yml and .env file
```

### Step 9: Stop and Restart Airflow

**To stop all services:**
```bash
docker-compose down

# Containers stop but data persists (Postgres volume remains)
```

**To restart after changes:**
```bash
# If you modified docker-compose.yml, Dockerfile, or .env:
docker-compose down
docker-compose build  # Rebuild image if Dockerfile/requirements changed
docker-compose up -d

# If you only changed DAG files or Python code:
docker-compose restart  # No rebuild needed
```

**To completely reset (⚠️ deletes all data):**
```bash
docker-compose down -v  # -v removes volumes (deletes Postgres data)
docker-compose up airflow-init
docker-compose up -d
```

### Troubleshooting

**Issue: Build fails with "no space left on device"**

Solution:
```bash
# Clean up unused Docker resources
docker system prune -a --volumes

# Check available space
df -h

# If still insufficient, resize EBS volume in AWS Console
```

**Issue: airflow-init exits with code 1**

Solution:
```bash
# Check initialization logs
docker-compose logs airflow-init

# Common causes:
# 1. Postgres not ready (wait 30s, retry)
# 2. Permission errors - fix with:
sudo chown -R 50000:0 logs/ dags/ plugins/

# 3. Database already initialized - this is fine, continue
```

**Issue: Containers start but immediately exit**

Solution:
```bash
# Check which container is failing
docker-compose ps

# View logs for failing container
docker-compose logs airflow-scheduler  # or airflow-webserver

# Common causes:
# 1. Port 8082 already in use:
sudo lsof -i :8082
# Kill process or change port in docker-compose.yml

# 2. Memory exhaustion (OOM):
free -h  # Check swap is active
# If swap is 0, reconfigure it (see Step 2 in Docker Installation)

# 3. Missing environment variables:
docker-compose exec airflow-scheduler env | grep AIRFLOW
docker-compose exec airflow-scheduler env | grep GMAIL
```

**Issue: "Unable to load the config, contains a configuration error"**

Solution:
```bash
# Validate docker-compose.yml syntax
docker-compose config

# If errors shown, fix YAML indentation
# Common: mixing tabs and spaces

# Rebuild and restart
docker-compose down
docker-compose up -d
```

**Issue: DAGs not appearing in UI**

Solution:
```bash
# 1. Check DAG files exist
docker-compose exec airflow-scheduler ls /opt/airflow/dags

# 2. Check for Python syntax errors
docker-compose exec airflow-scheduler python /opt/airflow/dags/spotify_data_basic.py
# Should execute without errors

# 3. Check scheduler is parsing DAGs
docker-compose logs airflow-scheduler | grep "spotify_data_basic"

# 4. Force DAG refresh in UI
# Go to Admin → Connections → Refresh button
```

**Issue: Import errors for packages**

Solution:
```bash
# Verify package is in requirements.txt
cat requirements.txt | grep snowflake

# Rebuild image to install missing packages
docker-compose down
docker-compose build --no-cache
docker-compose up -d

# Verify package installed
docker-compose exec airflow-scheduler pip list | grep snowflake
```

**Issue: "Webserver shutting down" or "Scheduler heartbeat timeout"**

Solution:
```bash
# This indicates memory pressure
# 1. Verify swap is active and being used
free -h

# 2. Check if swap is filling up (bad sign)
# If swap "used" is >1.5GB, reduce parallelism

# 3. Edit docker-compose.yml:
# AIRFLOW__CORE__PARALLELISM: 2  # Reduce from 4 to 2
# AIRFLOW__CORE__DAG_CONCURRENCY: 1  # Reduce from 2 to 1

# 4. Restart
docker-compose down
docker-compose up -d
```

**Issue: Email alerts not working**

Solution:
```bash
# 1. Verify .env file has correct credentials
cat .env | grep GMAIL

# 2. Check environment variables loaded in container
docker-compose exec airflow-scheduler env | grep GMAIL

# 3. Test SMTP connection manually
docker-compose exec airflow-scheduler python -c "
import smtplib
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('your-email@gmail.com', 'your-app-password')
print('SMTP connection successful')
server.quit()
"

# If login fails:
# - Regenerate Gmail app password
# - Ensure 2FA is enabled on Gmail account
# - Check for typos in .env file
```

**Issue: Can't connect to Snowflake from Airflow**

Solution:
```bash
# Test Snowflake connection from container
docker-compose exec airflow-scheduler python -c "
import snowflake.connector
conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='COMPUTE_WH',
    database='SPOTIFY_DATA',
    schema='SILVER'
)
print('Connected successfully')
conn.close()
"

# If this fails, check:
# 1. Snowflake credentials are correct
# 2. Snowflake account is active (not suspended)
# 3. EC2 IP is not blocked by Snowflake network policy
```
## Connection Configuration

Airflow needs connections configured to interact with Snowflake and AWS S3. These connections store credentials and connection details that your DAGs reference.

### Step 1: Access Airflow Connections

**Navigate to the Connections page:**

1. Open Airflow UI in browser: `http://YOUR_EC2_IP:8082`
2. Login with credentials: `airflow` / `airflow`
3. Click **Admin** in top menu
4. Click **Connections** in dropdown

You should see a list of connections (some default ones may already exist).

### Step 2: Create Snowflake Connection

**Click the `+` button to add a new connection.**

**Fill in the following fields:**

- **Connection Id:** `snowflake_default`
- **Connection Type:** `Snowflake` (select from dropdown)
- **Description:** `Snowflake connection for Spotify pipeline`
- **Host:** Your Snowflake account identifier
  - Format: `abc12345.us-east-2` or `abc12345.us-east-2.snowflakecomputing.com`
  - Find this in Snowflake UI (bottom-left corner shows account)
- **Schema:** `SILVER`
- **Login:** Your Snowflake username
- **Password:** Your Snowflake password
- **Account:** Your Snowflake account identifier (same as Host, without `.snowflakecomputing.com`)
- **Warehouse:** `SPOTIFY_WH`
- **Database:** `SPOTIFY_DATA`
- **Region:** `us-east-2`
- **Role:** Your Snowflake role (e.g., `ACCOUNTADMIN`)

**Example values:**
```
Connection Id: snowflake_default
Connection Type: Snowflake
Host: abc12345.us-east-2
Schema: SILVER
Login: SUNNI
Password: YourPassword123
Account: abc12345
Warehouse: SPOTIFY_WH
Database: SPOTIFY_DATA
Region: us-east-2
Role: ACCOUNTADMIN
```

**Click Save.**

### Step 3: Test Snowflake Connection

**Test the connection before proceeding:**
```bash
# SSH into EC2
ssh -i ~/path/to/key.pem ubuntu@YOUR_EC2_IP

# Test Snowflake connection from Airflow container
docker-compose exec airflow-scheduler python -c "
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute('SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();')
result = cursor.fetchone()
print(f'Connected successfully!')
print(f'Warehouse: {result[0]}')
print(f'Database: {result[1]}')
print(f'Schema: {result[2]}')
cursor.close()
conn.close()
"
```

**Expected output:**
```
Connected successfully!
Warehouse: SPOTIFY_WH
Database: SPOTIFY_DATA
Schema: SILVER
```

**If connection fails:**
- Check Snowflake credentials are correct
- Verify Snowflake account is not suspended
- Ensure user has proper permissions (USAGE, SELECT, CREATE)
- Check network connectivity (Snowflake may block EC2 IP ranges)

### Step 4: Create AWS Connection

**Click the `+` button again to add AWS connection.**

**Important:** If your EC2 instance has an IAM role attached (recommended), you only need minimal configuration. If not, you'll need AWS access keys.

#### Option A: Using IAM Role (Recommended)

**Fill in these fields:**

- **Connection Id:** `aws_default`
- **Connection Type:** `Amazon Web Services`
- **Description:** `AWS connection for S3 access in us-east-2`
- **Extra:** `{"region_name": "us-east-2"}`

**That's it!** When the connection has no AWS keys specified, Airflow automatically uses the EC2 instance's IAM role.

#### Option B: Using AWS Access Keys (If No IAM Role)

**Fill in these fields:**

- **Connection Id:** `aws_default`
- **Connection Type:** `Amazon Web Services`
- **Description:** `AWS connection for S3 access in us-east-2`
- **AWS Access Key ID:** Your AWS access key
- **AWS Secret Access Key:** Your AWS secret key
- **Extra:** `{"region_name": "us-east-2"}`

**⚠️ Security Note:** Using IAM roles is more secure than hardcoding access keys. If you use access keys, never commit them to git.

**Click Save.**

### Step 5: Test AWS Connection

**Test S3 access from Airflow:**
```bash
docker-compose exec airflow-scheduler python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id='aws_default')
bucket_name = 'spotify-data-lake-sunil-2025'

# Test bucket access
exists = hook.check_for_bucket(bucket_name)
print(f'Bucket {bucket_name} exists: {exists}')

# List files in bronze prefix
keys = hook.list_keys(bucket_name=bucket_name, prefix='bronze/plays/')
print(f'Files found in bronze/plays/: {len(keys) if keys else 0}')
if keys:
    print('Sample files:')
    for key in keys[:3]:
        print(f'  - {key}')
"
```

**Expected output:**
```
Bucket spotify-data-lake-sunil-2025 exists: True
Files found in bronze/plays/: 15
Sample files:
  - bronze/plays/2024-12-03/events_001.parquet
  - bronze/plays/2024-12-03/events_002.parquet
  - bronze/plays/2024-12-04/events_001.parquet
```

**If connection fails:**
- Verify IAM role is attached to EC2 instance
- Check IAM role has S3 read/write permissions for your bucket
- Verify bucket name is correct: `spotify-data-lake-sunil-2025`
- Ensure bucket is in `us-east-2` region
- Confirm Extra field includes: `{"region_name": "us-east-2"}`

### Step 6: Verify Connections in DAG

**Check that your DAGs can see the connections:**
```bash
# List all configured connections
docker-compose exec airflow-scheduler airflow connections list

# Should show:
# snowflake_default | snowflake | ...
# aws_default       | aws        | ...
```

**Test import DAGs without errors:**
```bash
# Parse production DAG
docker-compose exec airflow-scheduler python /opt/airflow/dags/spotify_data_basic.py
# Should complete without errors

# Parse monitoring DAG
docker-compose exec airflow-scheduler python /opt/airflow/dags/data_quality_monitoring.py
# Should complete without errors
```

**Check Airflow UI:**
- Go to **DAGs** page
- Both `spotify_data_basic` and `data_quality_monitoring` should appear
- If you see import errors, check the connection IDs match exactly

### Troubleshooting

**Issue: Snowflake connection test fails with "Account must be specified"**

Solution:
```bash
# The "Account" field is required and must match your Snowflake account identifier
# Format: abc12345 (without region or .snowflakecomputing.com)
# Find it in Snowflake UI → bottom-left → shows full account URL
# Extract the first part: https://abc12345.us-east-2.snowflakecomputing.com → use "abc12345"
```

**Issue: "Role 'PUBLIC' does not exist or not authorized"**

Solution:
```sql
-- In Snowflake, grant necessary permissions to your user:
GRANT ROLE ACCOUNTADMIN TO USER SUNNI;

-- Or create a custom role with specific permissions:
CREATE ROLE SPOTIFY_PIPELINE_ROLE;
GRANT USAGE ON DATABASE SPOTIFY_DATA TO ROLE SPOTIFY_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE SPOTIFY_WH TO ROLE SPOTIFY_PIPELINE_ROLE;
GRANT ALL ON SCHEMA SPOTIFY_DATA.BRONZE TO ROLE SPOTIFY_PIPELINE_ROLE;
GRANT ALL ON SCHEMA SPOTIFY_DATA.SILVER TO ROLE SPOTIFY_PIPELINE_ROLE;
GRANT ALL ON SCHEMA SPOTIFY_DATA.GOLD TO ROLE SPOTIFY_PIPELINE_ROLE;
GRANT ROLE SPOTIFY_PIPELINE_ROLE TO USER SUNNI;

-- Update Airflow connection to use SPOTIFY_PIPELINE_ROLE
```

**Issue: "Warehouse 'SPOTIFY_WH' does not exist"**

Solution:
```sql
-- Create the warehouse:
CREATE WAREHOUSE SPOTIFY_WH WITH 
    WAREHOUSE_SIZE='X-SMALL' 
    AUTO_SUSPEND=60 
    AUTO_RESUME=TRUE 
    INITIALLY_SUSPENDED=TRUE;

-- Verify it was created:
SHOW WAREHOUSES LIKE 'SPOTIFY_WH';

-- Grant usage to your user/role:
GRANT USAGE ON WAREHOUSE SPOTIFY_WH TO ROLE ACCOUNTADMIN;
```

**Issue: AWS connection fails with "Access Denied" to S3 bucket**

Solution:
```bash
# Check IAM role permissions
# In AWS Console: EC2 → Instance → Security tab → IAM Role → View attached policies

# The role needs this policy:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::spotify-data-lake-sunil-2025",
                "arn:aws:s3:::spotify-data-lake-sunil-2025/*"
            ]
        }
    ]
}
```

**Issue: S3KeySensor times out waiting for files**

Solution:
```bash
# Check if files exist in S3
aws s3 ls s3://spotify-data-lake-sunil-2025/bronze/plays/ --recursive

# If no files:
# 1. Kafka consumer may not have written anything yet
# 2. Bucket path in DAG doesn't match actual S3 structure
# 3. Check consumer logs: tail /tmp/consumer.log

# Verify S3 bucket region matches connection configuration
aws s3api get-bucket-location --bucket spotify-data-lake-sunil-2025
# Should return: "LocationConstraint": "us-east-2"
```

**Issue: Connection appears in list but DAG still shows import error**

Solution:
```bash
# Restart Airflow scheduler to reload connections
docker-compose restart airflow-scheduler

# Wait 30 seconds for scheduler to restart
docker-compose ps

# Check if DAGs now import successfully
docker-compose logs airflow-scheduler | grep "spotify_data_basic"
```

**Issue: "Snowflake connector not installed"**

Solution:
```bash
# Verify snowflake-connector-python is installed
docker-compose exec airflow-scheduler pip list | grep snowflake

# If missing, rebuild image
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

**Issue: "Could not connect to Snowflake backend after 1 attempt(s)"**

Solution:
```bash
# Test network connectivity to Snowflake
docker-compose exec airflow-scheduler ping abc12345.us-east-2.snowflakecomputing.com

# Common causes:
# 1. Wrong account identifier
# 2. Firewall blocking outbound HTTPS (port 443)
# 3. Snowflake account suspended
# 4. Region mismatch

# Verify Snowflake account URL format:
# Correct: abc12345.us-east-2.snowflakecomputing.com
# Incorrect: abc12345.snowflakecomputing.com (missing region)
```

**Issue: AWS region mismatch error**

Solution:
```bash
# Ensure aws_default connection has correct region in Extra field
# Edit connection in Airflow UI:
# Extra: {"region_name": "us-east-2"}

# Verify S3 bucket is in us-east-2:
aws s3api get-bucket-location --bucket spotify-data-lake-sunil-2025

# If bucket is in different region, either:
# 1. Update connection Extra to match bucket region
# 2. Or create new bucket in us-east-2 and update DAG
```

## DAG Deployment

DAG files are Python scripts that define your workflows. Airflow automatically detects DAG files in the mounted `dags/` directory.

### Step 1: Understanding DAG Directory Structure

**Your current DAG location:**
```bash
# On EC2 host
~/spotify-data-pipeline/dags/

# Inside Airflow containers (mounted via docker-compose)
/opt/airflow/dags/
```

**Volume mount in docker-compose.yml:**
```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
```

**How it works:**
- Files in `~/spotify-data-pipeline/dags/` on EC2 are visible inside containers at `/opt/airflow/dags/`
- Changes to DAG files on EC2 are immediately visible to Airflow
- Airflow scheduler scans this directory every 30 seconds for new/modified DAGs
- No container rebuild or restart needed when editing DAG files

### Step 2: Verify DAG Files Are Present

**Check DAG files exist on EC2:**
```bash
cd ~/spotify-data-pipeline/dags
ls -la

# Should show:
# data_quality_monitoring.py
# spotify_pipeline_basic.py
# __pycache__/ (auto-generated by Python)
```

**Verify files are visible inside Airflow container:**
```bash
docker-compose exec airflow-scheduler ls -la /opt/airflow/dags

# Should show the same files:
# data_quality_monitoring.py
# spotify_pipeline_basic.py
# __pycache__/
```

### Step 3: Check DAG Parsing

**Airflow automatically parses DAG files to detect workflows.**

**Check for parsing errors:**
```bash
# View scheduler logs for parsing errors
docker-compose logs airflow-scheduler | grep -i error | tail -20

# Check if DAGs parsed successfully
docker-compose exec airflow-scheduler airflow dags list

# Should show:
# dag_id                      | filepath                              | owner   | paused
# data_quality_monitoring     | data_quality_monitoring.py            | sunil   | True/False
# spotify_pipeline_basic      | spotify_pipeline_basic.py             | airflow | True/False
```

**If DAGs don't appear:**
- Check for Python syntax errors in DAG files
- Verify file permissions (should be readable)
- Check scheduler logs for detailed error messages

### Step 4: Edit DAG Files

**To modify a DAG:**
```bash
# SSH into EC2
ssh -i ~/path/to/key.pem ubuntu@YOUR_EC2_IP

# Navigate to dags directory
cd ~/spotify-data-pipeline/dags

# Edit the DAG file
nano spotify_pipeline_basic.py
# Make your changes, save (Ctrl+X, Y, Enter)
```

**After editing:**
- Airflow automatically detects changes within 30-60 seconds
- No need to restart containers
- Refresh Airflow UI to see updates
- If DAG structure changed significantly, you may need to unpause/re-pause the DAG

**Example workflow:**
```bash
# 1. Edit DAG
nano spotify_pipeline_basic.py

# 2. Test for syntax errors
python spotify_pipeline_basic.py
# Should complete without errors

# 3. Check Airflow picked up changes
docker-compose logs airflow-scheduler | grep "spotify_pipeline_basic" | tail -5

# 4. Refresh Airflow UI in browser (Ctrl+R or F5)
```

### Step 5: Verify DAGs in Airflow UI

**Check DAGs appear in the UI:**

1. Open Airflow UI: `http://YOUR_EC2_IP:8082`
2. Login: `airflow` / `airflow`
3. Navigate to **DAGs** page (default landing page)

**You should see:**
- `spotify_pipeline_basic` - Production data pipeline
- `data_quality_monitoring` - Data quality checks

**DAG status indicators:**
- **Green toggle** = DAG is unpaused (can run)
- **Red toggle** = DAG is paused (won't run)
- **Red circle** = DAG has import errors
- **Clock icon** = Next scheduled run time

**Click on a DAG name to see:**
- Graph view (task dependencies)
- Code (DAG file contents)
- Recent runs
- Task duration trends

### Step 6: Manual DAG Execution

**To manually trigger a DAG run:**

1. Go to **DAGs** page in Airflow UI
2. Find `spotify_pipeline_basic` in the list
3. Click the **Play button** (▶) on the right side
4. Click **Trigger DAG** in the popup
5. DAG run starts immediately

**Monitor execution:**
- Click on DAG name to see task progress
- Click on task squares to view logs
- Green = success, Red = failed, Yellow = running

**Alternative: Trigger from command line:**
```bash
docker-compose exec airflow-scheduler airflow dags trigger spotify_pipeline_basic

# Check run status
docker-compose exec airflow-scheduler airflow dags list-runs -d spotify_pipeline_basic --state running
```

### Step 7: Deployment Best Practices

**Version control workflow:**
```bash
# After editing DAG files
cd ~/spotify-data-pipeline

# Check what changed
git diff dags/

# Stage changes
git add dags/spotify_pipeline_basic.py

# Commit with descriptive message
git commit -m "Update spotify pipeline: increase event count to 300"

# Push to GitHub
git push origin main
```

**Testing new DAGs before deployment:**
```bash
# Test Python syntax
python dags/spotify_pipeline_basic.py

# Test imports work in Airflow environment
docker-compose exec airflow-scheduler python /opt/airflow/dags/spotify_pipeline_basic.py

# Check for Airflow-specific issues
docker-compose exec airflow-scheduler airflow dags test spotify_pipeline_basic 2024-12-01
```

**Common DAG file locations:**
- Production DAGs: `~/spotify-data-pipeline/dags/`
- Backups: `~/spotify-data-pipeline/dags/archive/` (create if needed)
- Development/testing: Edit in separate branch, test locally

### Troubleshooting

**Issue: DAG doesn't appear in UI after adding new file**

Solution:
```bash
# 1. Verify file exists
ls -la ~/spotify-data-pipeline/dags/

# 2. Check file is visible in container
docker-compose exec airflow-scheduler ls /opt/airflow/dags/

# 3. Check for Python syntax errors
python ~/spotify-data-pipeline/dags/new_dag.py

# 4. Check scheduler logs
docker-compose logs airflow-scheduler | grep "new_dag"

# 5. Wait 30-60 seconds for scheduler to detect it
# 6. Refresh Airflow UI (hard refresh: Ctrl+Shift+R)
```

**Issue: DAG shows red circle (import error)**

Solution:
```bash
# Click on the DAG name in UI to see error details

# Common causes:
# 1. Python syntax error
python ~/spotify-data-pipeline/dags/your_dag.py

# 2. Missing imports
docker-compose exec airflow-scheduler python -c "from airflow.operators.bash import BashOperator"

# 3. Missing connections
docker-compose exec airflow-scheduler airflow connections list

# 4. Check detailed error in scheduler logs
docker-compose logs airflow-scheduler | grep ERROR | grep your_dag
```

**Issue: Changes to DAG file not reflected in UI**

Solution:
```bash
# 1. Verify you edited the correct file
cat ~/spotify-data-pipeline/dags/spotify_pipeline_basic.py | grep "your_change"

# 2. Check file timestamp updated
ls -lt ~/spotify-data-pipeline/dags/

# 3. Force scheduler to re-parse
docker-compose restart airflow-scheduler

# 4. Hard refresh browser (Ctrl+Shift+R)

# 5. Check scheduler picked up changes
docker-compose logs airflow-scheduler | tail -50 | grep spotify_pipeline_basic
```

**Issue: DAG runs but tasks fail immediately**

Solution:
```bash
# Check task logs in Airflow UI:
# 1. Click on DAG name
# 2. Click on failed task (red square)
# 3. Click "Log" button
# Read error message

# Common causes:
# - Missing connections (snowflake_default, aws_default)
# - Wrong file paths in BashOperator commands
# - Missing Python dependencies
# - Insufficient permissions (Snowflake, S3)
```

**Issue: `__pycache__` directory causing issues**

Solution:
```bash
# Python auto-generates this for performance
# Safe to delete if needed
rm -rf ~/spotify-data-pipeline/dags/__pycache__

# Add to .gitignore to avoid committing
echo "__pycache__/" >> ~/spotify-data-pipeline/.gitignore
```

**Issue: File permissions prevent Airflow from reading DAG**

Solution:
```bash
# Check current permissions
ls -la ~/spotify-data-pipeline/dags/

# DAG files should be readable (644 or 664)
chmod 664 ~/spotify-data-pipeline/dags/*.py

# If container runs as different user, may need broader permissions
chmod 775 ~/spotify-data-pipeline/dags/
```

## Verification Steps

This section provides a comprehensive checklist to verify your entire Airflow setup is working correctly.

### Step 1: System Health Checks

**Verify Docker containers are running:**
```bash
docker-compose ps

# Expected output - all containers show "Up (healthy)":
# spotify-data-pipeline-postgres-1            Up (healthy)
# spotify-data-pipeline-airflow-webserver-1   Up (healthy)
# spotify-data-pipeline-airflow-scheduler-1   Up (healthy)
```

**Check system resources:**
```bash
# Verify swap is active
free -h
# Should show 2.0Gi in Swap row

# Check disk space
df -h
# Should have at least 5GB free on root partition

# Check Docker disk usage
docker system df
# Images, containers, volumes should have reasonable sizes
```

**Monitor container logs:**
```bash
# Check for errors in scheduler
docker-compose logs airflow-scheduler | grep ERROR | tail -20
# Should show no recent critical errors

# Check webserver is serving requests
docker-compose logs airflow-webserver | grep "Listening at" | tail -1
# Should show: Listening at: http://0.0.0.0:8080
```

### Step 2: Airflow Service Verification

**Access Airflow UI:**
```bash
# From your browser
http://YOUR_EC2_IP:8082

# Login credentials:
# Username: airflow
# Password: airflow
```

**Verify UI loads successfully:**
- Dashboard shows with DAGs list
- No error messages or connection failures
- Navigation menu (DAGs, Browse, Admin) is accessible

**Check Airflow version:**
```bash
docker-compose exec airflow-scheduler airflow version

# Expected output:
# 2.8.1
```

**Verify scheduler is processing DAGs:**
```bash
# Check scheduler heartbeat
docker-compose exec airflow-scheduler airflow jobs check --job-type SchedulerJob

# Should show: Job "SchedulerJob" (latest: ...) is alive
```

### Step 3: Connection Verification

**Test Snowflake connection:**
```bash
docker-compose exec airflow-scheduler python -c "
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute('SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();')
result = cursor.fetchone()
print(f'Warehouse: {result[0]}')
print(f'Database: {result[1]}')
print(f'Schema: {result[2]}')
cursor.close()
conn.close()
"

# Expected output:
# Warehouse: SPOTIFY_WH
# Database: SPOTIFY_DATA
# Schema: SILVER
```

**Test AWS S3 connection:**
```bash
docker-compose exec airflow-scheduler python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id='aws_default')
bucket = 'spotify-data-lake-sunil-2025'
exists = hook.check_for_bucket(bucket)
print(f'Bucket {bucket} exists: {exists}')
"

# Expected output:
# Bucket spotify-data-lake-sunil-2025 exists: True
```

**Verify connections in Airflow UI:**
- Navigate to **Admin** → **Connections**
- Confirm `snowflake_default` exists
- Confirm `aws_default` exists

### Step 4: DAG Import Verification

**List all DAGs:**
```bash
docker-compose exec airflow-scheduler airflow dags list

# Should show:
# data_quality_monitoring
# spotify_pipeline_basic
```

**Check DAG files for syntax errors:**
```bash
# Test production DAG
docker-compose exec airflow-scheduler python /opt/airflow/dags/spotify_pipeline_basic.py
# Should complete with no output (no errors)

# Test monitoring DAG
docker-compose exec airflow-scheduler python /opt/airflow/dags/data_quality_monitoring.py
# Should complete with no output (no errors)
```

**Verify DAGs appear in UI:**
- Go to **DAGs** page in Airflow UI
- Both DAGs should be visible with no red circles
- Click on each DAG name to verify no import errors

**Check DAG details:**
```bash
# Get production DAG info
docker-compose exec airflow-scheduler airflow dags show spotify_pipeline_basic

# Should display task dependency graph
```

### Step 5: Test Pipeline Run

**Manually trigger the production pipeline:**

1. In Airflow UI, go to **DAGs** page
2. Find `spotify_pipeline_basic`
3. Click the **Play button** (▶) on the right
4. Click **Trigger DAG** in popup
5. Click on DAG name to watch execution

**Monitor the run:**
```bash
# Watch run progress from command line
docker-compose exec airflow-scheduler airflow dags list-runs -d spotify_pipeline_basic

# Follow scheduler logs in real-time
docker-compose logs -f airflow-scheduler
```

**Expected behavior:**
- All 10 tasks should complete successfully
- Total runtime: approximately 6 minutes
- Task execution order:
  1. `start_consumer` and `generate_events` (parallel)
  2. `wait_for_s3_files`
  3. `stop_consumer`
  4. `refresh_snowflake_table`
  5. `dbt_compile`
  6. `dbt_run_silver`
  7. `dbt_run_gold`
  8. `dbt_test`
  9. `log_success`

**Verify successful completion:**
```bash
# Check latest run status
docker-compose exec airflow-scheduler airflow dags list-runs -d spotify_pipeline_basic

# Should show state: success
```

**In Airflow UI:**
- All task squares should be green
- No red (failed) or yellow (running) tasks
- Click on tasks to view logs and confirm success

### Step 6: Data Verification

**Check data was written to S3:**
```bash
# List files in S3 bucket
docker-compose exec airflow-scheduler python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id='aws_default')
keys = hook.list_keys(bucket_name='spotify-data-lake-sunil-2025', prefix='bronze/plays/')
print(f'Total files in S3: {len(keys) if keys else 0}')
if keys:
    for key in keys[-3:]:
        print(f'  {key}')
"

# Should show files in bronze/plays/ directory
```

**Verify data in Snowflake:**
```bash
docker-compose exec airflow-scheduler python -c "
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
conn = hook.get_conn()
cursor = conn.cursor()

# Check Bronze layer
cursor.execute('SELECT COUNT(*) FROM SPOTIFY_DATA.BRONZE.plays;')
bronze_count = cursor.fetchone()[0]
print(f'Bronze layer: {bronze_count} rows')

# Check Silver layer
cursor.execute('SELECT COUNT(*) FROM SPOTIFY_DATA.SILVER.silver_plays;')
silver_count = cursor.fetchone()[0]
print(f'Silver layer: {silver_count} rows')

# Check Gold layer
cursor.execute('SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.top_tracks;')
gold_count = cursor.fetchone()[0]
print(f'Gold layer (top_tracks): {gold_count} rows')

cursor.close()
conn.close()
"

# Expected output:
# Bronze layer: 1800 rows (or more, accumulates over time)
# Silver layer: 1800 rows
# Gold layer (top_tracks): 5 rows
```

### Step 7: Monitoring DAG Verification

**Trigger the monitoring DAG:**

1. In Airflow UI, go to **DAGs** page
2. Find `data_quality_monitoring`
3. Click the **Play button** (▶)
4. Click **Trigger DAG**

**Expected behavior:**
- 6 tasks total (start → 5 parallel checks → success)
- Runtime: approximately 1-2 minutes
- All checks should pass if production pipeline ran successfully

**Verify monitoring checks:**
```bash
# Check monitoring DAG run status
docker-compose exec airflow-scheduler airflow dags list-runs -d data_quality_monitoring

# Should show state: success
```

**In Airflow UI:**
- All 6 tasks should be green
- Parallel tasks (`check_row_counts`, `check_freshness`, etc.) all succeed
- `all_checks_passed` task completes

### Step 8: Email Alert Verification

**Test email configuration:**
```bash
docker-compose exec airflow-scheduler python -c "
from airflow.utils.email import send_email
send_email(
    to='sunil.makkar97@gmail.com',
    subject='Airflow Setup Verification',
    html_content='<p>Email alerts are working correctly!</p>'
)
"

# Check your email inbox
# Should receive test email within 1-2 minutes
```

**If email fails:**
- Verify `.env` file has correct `GMAIL_USER` and `GMAIL_APP_PASSWORD`
- Check environment variables loaded: `docker-compose exec airflow-scheduler env | grep GMAIL`
- Ensure Gmail app password is valid (not regular password)
- Check SMTP settings in docker-compose.yml

### Step 9: Operational Dashboard

**Run the dashboard script:**
```bash
docker-compose exec airflow-scheduler python /opt/airflow/scripts/dashboard.py
```

**Expected output:**
```
************************************************************
SPOTIFY DATA PIPELINE - OPERATIONAL DASHBOARD
Generated: 2024-12-07 20:00:00
************************************************************

============================================================
                      PIPELINE HEALTH                       
============================================================

Total Runs (last 10): 1
Success Rate: 100.0%
Last Successful Run: 2024-12-07 19:50:00 (10 min ago)
Average Duration: 6.2 minutes

============================================================
                        DATA METRICS                        
============================================================

Total Events Processed: 1,800
Events (Last 24 Hours): 200
Bronze Layer Rows: 1,800
Silver Layer Rows: 1,800

Gold Layer Tables:
  top_tracks: 5
  top_artists: 5
  daily_user_stats: 6
  device_usage: 3

============================================================
                     MONITORING STATUS                      
============================================================

Total Checks (last 5): 5
Passed: 5
Failed: 0
Last Check: 2024-12-07 19:55:00 (5 min ago)
```

**Dashboard should show:**
- 100% success rate (if pipeline just ran successfully)
- Recent successful run timestamp
- Data counts in all layers
- All monitoring checks passed

### Step 10: Schedule Verification

**Check production pipeline schedule:**
```bash
# View DAG schedule
docker-compose exec airflow-scheduler airflow dags details spotify_pipeline_basic | grep schedule

# Should show: schedule_interval: None (manual trigger only)
# Or if scheduled: schedule_interval: 0 * * * * (hourly)
```

**Check monitoring DAG schedule:**
```bash
docker-compose exec airflow-scheduler airflow dags details data_quality_monitoring | grep schedule

# Should show: schedule_interval: */30 * * * * (every 30 minutes)
```

**Verify next scheduled run:**
- In Airflow UI, DAGs page shows "Next Run" column
- Monitoring DAG should show next run in ~30 minutes
- Production DAG shows "None" if manually triggered only

### Verification Checklist

**System Level:**
- [ ] All 3 Docker containers running and healthy
- [ ] Swap space active (2GB)
- [ ] Sufficient disk space (>5GB free)
- [ ] No critical errors in container logs

**Airflow Level:**
- [ ] Airflow UI accessible at port 8082
- [ ] Scheduler job is alive
- [ ] Both DAGs visible without import errors
- [ ] Connections configured (snowflake_default, aws_default)

**Pipeline Level:**
- [ ] Production DAG runs successfully (all 10 tasks green)
- [ ] Runtime approximately 6 minutes
- [ ] Data written to S3 (bronze/plays/)
- [ ] Data in Snowflake (Bronze, Silver, Gold layers)

**Monitoring Level:**
- [ ] Monitoring DAG runs successfully (all 6 tasks green)
- [ ] All 5 data quality checks pass
- [ ] Email alerts working (test email received)
- [ ] Dashboard shows correct metrics

**If all items are checked:** Your Airflow setup is complete and fully operational!

**If any items fail:** Refer to Section 7 (Common Setup Issues) for troubleshooting guidance.

## Common Setup Issues

This section consolidates troubleshooting guidance for issues encountered during Airflow setup and operation.

### Docker and Container Issues

**Issue: "Cannot connect to the Docker daemon"**

Symptoms:
- `docker` commands fail with connection error
- `docker-compose ps` shows nothing

Solution:
```bash
# Check if Docker service is running
sudo systemctl status docker

# If stopped, start it
sudo systemctl start docker

# Enable Docker to start on boot
sudo systemctl enable docker

# Verify Docker is accessible
docker ps
```

---

**Issue: "Permission denied while trying to connect to Docker daemon"**

Symptoms:
- Docker commands require `sudo`
- `docker-compose` fails without sudo

Solution:
```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Apply group membership
newgrp docker

# Test without sudo
docker ps

# If still fails, log out and log back in
exit
# Then SSH back in
```

---

**Issue: Containers start but immediately exit**

Symptoms:
- `docker-compose ps` shows containers with "Exit 1" or "Restarting"
- Containers don't stay in "Up" state

Solution:
```bash
# Check which container is failing
docker-compose ps

# View logs for the failing container
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver

# Common causes and fixes:

# 1. Port conflict (8082 already in use)
sudo lsof -i :8082
# Kill the process or change port in docker-compose.yml

# 2. Memory exhaustion
free -h
# If swap is 0, reconfigure swap (see Docker Installation section)

# 3. Missing environment variables
docker-compose exec airflow-scheduler env | grep AIRFLOW
# Verify all required variables are set

# 4. Database not ready
docker-compose logs postgres
# Wait 30 seconds and retry: docker-compose up -d
```

---

**Issue: "no space left on device" during build**

Symptoms:
- `docker-compose build` fails midway
- Error mentions disk space

Solution:
```bash
# Clean up unused Docker resources
docker system prune -a --volumes

# Check disk space
df -h

# If root partition is full:
# 1. Remove old container images
docker images
docker rmi <image-id>

# 2. Clear Docker build cache
docker builder prune -a

# 3. Increase EBS volume size in AWS Console if needed
```

---

**Issue: Containers are "unhealthy" after starting**

Symptoms:
- `docker-compose ps` shows "Up (unhealthy)"
- Containers running but health checks fail

Solution:
```bash
# Check health check logs
docker inspect spotify-data-pipeline-airflow-webserver-1 | grep -A 10 Health

# Common causes:

# 1. Webserver not responding on port 8080
docker-compose exec airflow-webserver curl http://localhost:8080/health
# If fails, check webserver logs for errors

# 2. Scheduler heartbeat failing
docker-compose logs airflow-scheduler | grep heartbeat

# 3. Postgres not accepting connections
docker-compose exec postgres pg_isready -U airflow
# Should return: accepting connections

# Wait 2-3 minutes for health checks to pass
# If still unhealthy after 5 minutes, restart:
docker-compose restart
```

### Memory and Resource Issues

**Issue: "Scheduler heartbeat timeout" or "Webserver shutting down"**

Symptoms:
- Containers crash with OOM (out of memory) errors
- Scheduler stops processing tasks
- System becomes unresponsive

Solution:
```bash
# 1. Verify swap is active
free -h
# Swap should show 2.0Gi total

# If swap is 0, reconfigure:
sudo swapon /swapfile
# Make permanent: see Docker Installation section

# 2. Check current memory usage
free -h
# If swap "used" is >1.5GB, reduce parallelism

# 3. Edit docker-compose.yml to reduce parallelism:
nano docker-compose.yml
# Change:
# AIRFLOW__CORE__PARALLELISM: 2  (from 4)
# AIRFLOW__CORE__DAG_CONCURRENCY: 1  (from 2)

# 4. Restart containers
docker-compose down
docker-compose up -d

# 5. Monitor memory usage
watch free -h
```

---

**Issue: System freezes or becomes very slow**

Symptoms:
- SSH connection lags
- Commands take long time to execute
- EC2 instance unresponsive

Solution:
```bash
# 1. Check if swap is thrashing (heavily used)
free -h
# If swap used is >1.8GB, system is overloaded

# 2. Stop Airflow temporarily
docker-compose down

# 3. Check for runaway processes
top
# Look for processes using >50% CPU or memory

# 4. Upgrade EC2 instance type:
# AWS Console: Stop instance → Change type to t3.medium → Start
# This gives 4GB RAM (2x current)

# 5. After upgrade, restart Airflow
docker-compose up -d
```

### Connection and Network Issues

**Issue: Cannot access Airflow UI from browser**

Symptoms:
- Browser shows "Connection refused" or "Timeout"
- Cannot reach `http://EC2_IP:8082`

Solution:
```bash
# 1. Verify webserver is running
docker-compose ps | grep webserver
# Should show "Up (healthy)"

# 2. Test from EC2 instance itself
curl http://localhost:8082/health
# Should return JSON with "healthy" status

# 3. Check EC2 security group
# AWS Console → EC2 → Security Groups
# Ensure inbound rule exists:
# Type: Custom TCP, Port: 8082, Source: Your IP

# 4. Verify correct EC2 public IP
# AWS Console → EC2 → Instances → Check Public IPv4

# 5. Check if port is actually listening
sudo netstat -tlnp | grep 8082
# Should show docker-proxy listening on 8082
```

---

**Issue: Snowflake connection fails**

Symptoms:
- `snowflake_default` test fails
- Tasks using SnowflakeOperator fail
- Error: "Could not connect to Snowflake"

Solution:
```bash
# 1. Test connection manually
docker-compose exec airflow-scheduler python -c "
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
conn = hook.get_conn()
print('Connected successfully')
conn.close()
"

# If fails, check:

# 2. Verify connection exists in Airflow
docker-compose exec airflow-scheduler airflow connections list | grep snowflake

# 3. Check connection details in Airflow UI
# Admin → Connections → snowflake_default
# Verify: Host, Account, Warehouse, Database, Login, Password

# 4. Test Snowflake credentials directly
# Login to Snowflake UI with same credentials
# Verify account is not suspended

# 5. Check network connectivity
docker-compose exec airflow-scheduler ping YOUR_ACCOUNT.us-east-2.snowflakecomputing.com
# Should get responses

# 6. Verify Snowflake warehouse exists
# In Snowflake UI: SHOW WAREHOUSES;
# Ensure SPOTIFY_WH exists and is not suspended
```

---

**Issue: AWS S3 connection fails**

Symptoms:
- `aws_default` test fails
- S3KeySensor tasks timeout
- Error: "Access Denied" or "NoSuchBucket"

Solution:
```bash
# 1. Test S3 connection
docker-compose exec airflow-scheduler python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id='aws_default')
exists = hook.check_for_bucket('spotify-data-lake-sunil-2025')
print(f'Bucket exists: {exists}')
"

# If fails, check:

# 2. Verify IAM role attached to EC2
# AWS Console → EC2 → Instance → Security tab
# Should show IAM role name

# 3. Check IAM role permissions
# AWS Console → IAM → Roles → Your role → Permissions
# Ensure S3 read/write permissions for your bucket

# 4. Verify bucket name is correct
aws s3 ls s3://spotify-data-lake-sunil-2025/
# Should list bucket contents

# 5. Check bucket region matches connection
aws s3api get-bucket-location --bucket spotify-data-lake-sunil-2025
# Should return: "LocationConstraint": "us-east-2"

# 6. Verify aws_default connection has correct region
# In Airflow UI: Admin → Connections → aws_default
# Extra: {"region_name": "us-east-2"}
```

### DAG and Import Issues

**Issue: DAG doesn't appear in Airflow UI**

Symptoms:
- DAG file exists but not visible in UI
- DAGs list doesn't show your DAG

Solution:
```bash
# 1. Verify file exists and is in correct location
ls -la ~/spotify-data-pipeline/dags/
# Should show your DAG file

# 2. Check file is visible in container
docker-compose exec airflow-scheduler ls /opt/airflow/dags/
# Should show same files

# 3. Test for Python syntax errors
python ~/spotify-data-pipeline/dags/your_dag.py
# Should complete without errors

# 4. Check scheduler logs for parsing errors
docker-compose logs airflow-scheduler | grep your_dag | grep ERROR

# 5. Wait 30-60 seconds for scheduler to detect new DAG
# Then refresh browser (Ctrl+Shift+R)

# 6. Force scheduler restart if needed
docker-compose restart airflow-scheduler
```

---

**Issue: DAG shows red circle (import error)**

Symptoms:
- DAG appears but has red circle icon
- Clicking DAG shows import error message

Solution:
```bash
# 1. Click on DAG name in UI to see error details

# 2. Test DAG file directly
docker-compose exec airflow-scheduler python /opt/airflow/dags/your_dag.py

# Common errors and fixes:

# Missing import:
# Error: "No module named 'snowflake'"
# Fix: Verify package in requirements.txt, rebuild image
docker-compose exec airflow-scheduler pip list | grep snowflake

# Missing connection:
# Error: "Connection 'snowflake_default' doesn't exist"
# Fix: Create connection in Airflow UI (see Connection Configuration)

# Syntax error:
# Error: "SyntaxError: invalid syntax"
# Fix: Check DAG file for Python errors
python ~/spotify-data-pipeline/dags/your_dag.py

# Import path error:
# Error: "ModuleNotFoundError: No module named 'src'"
# Fix: Check sys.path in DAG or use absolute paths
```

---

**Issue: DAG changes not reflected in UI**

Symptoms:
- Edited DAG file but UI shows old version
- New tasks don't appear in graph view

Solution:
```bash
# 1. Verify file was actually saved
cat ~/spotify-data-pipeline/dags/your_dag.py | grep "your_change"

# 2. Check file timestamp
ls -lt ~/spotify-data-pipeline/dags/

# 3. Wait 30-60 seconds for scheduler to re-parse
# Airflow scans DAG directory every 30 seconds

# 4. Hard refresh browser
# Chrome/Firefox: Ctrl+Shift+R
# Safari: Cmd+Shift+R

# 5. Check scheduler detected changes
docker-compose logs airflow-scheduler | tail -50 | grep your_dag

# 6. If still not updating, restart scheduler
docker-compose restart airflow-scheduler
```

### Email and Alert Issues

**Issue: Email alerts not working**

Symptoms:
- Tasks fail but no email received
- Test email command doesn't send email

Solution:
```bash
# 1. Verify .env file has correct credentials
cat .env | grep GMAIL
# Should show GMAIL_USER and GMAIL_APP_PASSWORD

# 2. Check environment variables loaded in container
docker-compose exec airflow-scheduler env | grep GMAIL
# Should show both variables

# 3. Test SMTP connection manually
docker-compose exec airflow-scheduler python -c "
import smtplib
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('sunil.makkar97@gmail.com', 'your-app-password')
print('SMTP connection successful')
server.quit()
"

# If login fails:

# 4. Regenerate Gmail app password
# Visit: https://myaccount.google.com/apppasswords
# Generate new password and update .env file

# 5. Verify 2FA is enabled on Gmail account
# Required for app passwords

# 6. Restart containers to reload environment variables
docker-compose down
docker-compose up -d

# 7. Check for typos in .env file
# Common: extra spaces, wrong quotes, missing characters
```

---

**Issue: Emails sent but not received**

Symptoms:
- No errors in logs but inbox empty
- Test email command succeeds

Solution:
```bash
# 1. Check spam/junk folder
# Gmail may filter automated emails

# 2. Verify correct recipient email
# Check DAG default_args:
cat ~/spotify-data-pipeline/dags/spotify_pipeline_basic.py | grep email

# 3. Check Gmail sent folder
# Login to Gmail, check if emails were actually sent

# 4. Try different recipient email
# Test with another email address to rule out delivery issues
```

### Database and Data Issues

**Issue: "Database is locked" or Postgres errors**

Symptoms:
- Tasks fail with database connection errors
- Postgres container unhealthy

Solution:
```bash
# 1. Check Postgres container is running
docker-compose ps | grep postgres
# Should show "Up (healthy)"

# 2. Test database connection
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT 1;"
# Should return: 1

# 3. Check for too many connections
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT count(*) FROM pg_stat_activity;"
# If >100, database may be overloaded

# 4. Restart Postgres (will briefly interrupt Airflow)
docker-compose restart postgres

# 5. If persistent, reset database (DELETES ALL DATA)
docker-compose down -v
docker-compose up airflow-init
docker-compose up -d
```

---

**Issue: Snowflake tables empty after pipeline runs**

Symptoms:
- Pipeline completes successfully
- Snowflake queries return 0 rows

Solution:
```bash
# 1. Check S3 has data files
aws s3 ls s3://spotify-data-lake-sunil-2025/bronze/plays/ --recursive

# 2. Verify Snowflake external table definition
# In Snowflake UI:
DESCRIBE TABLE SPOTIFY_DATA.BRONZE.plays;
# Check LOCATION points to correct S3 path

# 3. Manually refresh external table
ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;

# 4. Query Bronze layer
SELECT COUNT(*) FROM SPOTIFY_DATA.BRONZE.plays;
# If 0, external table configuration is wrong

# 5. Check DBT transformations ran
# In Airflow: Check dbt_run_silver and dbt_run_gold tasks succeeded

# 6. Query DBT logs
docker-compose exec airflow-scheduler cat /opt/airflow/dbt/logs/dbt.log | tail -50
```

### Performance Issues

**Issue: DAG runs take much longer than expected**

Symptoms:
- Production pipeline takes >30 minutes (should be ~6 minutes)
- Tasks queue instead of running immediately

Solution:
```bash
# 1. Check parallelism settings
docker-compose exec airflow-scheduler airflow config get-value core parallelism
# Should be 4

# 2. Check if other DAGs are running
docker-compose exec airflow-scheduler airflow dags list-runs --state running
# If multiple DAGs running, they share limited parallelism

# 3. Check task queue
docker-compose exec airflow-scheduler airflow tasks states-for-dag-run spotify_pipeline_basic <run-id>
# Look for tasks in "queued" state

# 4. Check system resources during run
free -h
top
# If memory/CPU maxed out, consider upgrading instance

# 5. Review task logs for slow operations
# In Airflow UI, click on slow task → View log
# Look for network timeouts or slow queries
```

---

**Issue: Scheduler consuming too much CPU**

Symptoms:
- `top` shows airflow-scheduler using >80% CPU
- System becomes sluggish

Solution:
```bash
# 1. Check number of DAGs
docker-compose exec airflow-scheduler airflow dags list | wc -l
# If >50 DAGs, scheduler has to parse many files

# 2. Reduce DAG scan frequency (not recommended for development)
# Edit docker-compose.yml:
# AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 60  # Scan every 60 seconds

# 3. Check for errors causing constant re-parsing
docker-compose logs airflow-scheduler | grep "Failed to import"

# 4. Restart scheduler
docker-compose restart airflow-scheduler
```

### General Troubleshooting Steps

**When something doesn't work:**

1. **Check the logs**
```bash
docker-compose logs airflow-scheduler | tail -100
docker-compose logs airflow-webserver | tail -100
docker-compose logs postgres | tail -100
```

2. **Verify connections**
```bash
docker-compose exec airflow-scheduler airflow connections list
```

3. **Check system resources**
```bash
free -h
df -h
docker system df
```

4. **Restart affected service**
```bash
docker-compose restart airflow-scheduler
# or
docker-compose down && docker-compose up -d
```

5. **Review Airflow UI**
- Check task logs for detailed errors
- Look at DAG graph for dependency issues
- Review recent runs for patterns

6. **Test connections manually**
```bash
# Snowflake
docker-compose exec airflow-scheduler python -c "from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook; ..."

# AWS S3
docker-compose exec airflow-scheduler python -c "from airflow.providers.amazon.aws.hooks.s3 import S3Hook; ..."
```

7. **Check documentation**
- Airflow docs: https://airflow.apache.org/docs/
- This setup guide (previous sections)
- GitHub repository README

**Still stuck?**
- Check scheduler logs for detailed error messages
- Search error message in Airflow documentation
- Verify all prerequisites from Section 1 are met
- Consider posting issue to GitHub repository with:
  - Error message
  - Relevant logs
  - Steps to reproduce
