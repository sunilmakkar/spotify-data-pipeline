#!/usr/bin/env python3
"""
Operational Dashboard for Spotify Data Pipeline
Displays pipeline health, data metrics, and system status
"""

import sys
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow.models import DagRun, TaskInstance, DagModel
from airflow.utils.db import provide_session
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import subprocess

# ANSI color codes for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
BOLD = '\033[1m'
RESET = '\033[0m'


def print_header(title):
    """Print formatted section header"""
    print(f"\n{BOLD}{BLUE}{'=' * 60}{RESET}")
    print(f"{BOLD}{BLUE}{title.center(60)}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 60}{RESET}\n")


def print_metric(label, value, status=None):
    """Print formatted metric with optional status color"""
    if status == 'success':
        color = GREEN
    elif status == 'failed':
        color = RED
    elif status == 'warning':
        color = YELLOW
    else:
        color = RESET
    
    print(f"{BOLD}{label}:{RESET} {color}{value}{RESET}")


@provide_session
def get_pipeline_health(session=None):
    """Get pipeline run statistics"""
    print_header("PIPELINE HEALTH")
    
    dag_id = 'spotify_data_basic'
    
    # Get last 10 runs
    runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.run_type != 'backfill'
    ).order_by(DagRun.execution_date.desc()).limit(10).all()
    
    if not runs:
        print_metric("Status", "No runs found", "warning")
        return
    
    # Calculate success rate
    successful_runs = [r for r in runs if r.state == 'success']
    failed_runs = [r for r in runs if r.state == 'failed']
    success_rate = (len(successful_runs) / len(runs)) * 100
    
    # Last successful run
    last_success = successful_runs[0] if successful_runs else None
    last_failure = failed_runs[0] if failed_runs else None
    
    # Print metrics
    print_metric("Total Runs (last 10)", len(runs))
    print_metric("Success Rate", f"{success_rate:.1f}%", 
                 "success" if success_rate >= 90 else "warning")
    
    if last_success:
        time_since = datetime.now(timezone.utc) - last_success.execution_date
        hours_ago = time_since.total_seconds() / 3600
        print_metric("Last Successful Run", 
                     f"{last_success.execution_date} ({hours_ago:.1f} hours ago)",
                     "success")
    
    if last_failure:
        print_metric("Last Failed Run", 
                     f"{last_failure.execution_date}",
                     "failed")
    else:
        print_metric("Last Failed Run", "None", "success")
    
    # Calculate average duration
    completed_runs = [r for r in runs if r.end_date and r.start_date]
    if completed_runs:
        avg_duration = sum([(r.end_date - r.start_date).total_seconds() 
                           for r in completed_runs]) / len(completed_runs)
        print_metric("Average Duration", f"{avg_duration / 60:.1f} minutes")


def get_data_metrics():
    """Get data volume metrics from Snowflake"""
    print_header("DATA METRICS")
    
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Total events (all-time)
        total_query = "SELECT COUNT(*) FROM SPOTIFY_DATA.BRONZE.plays;"
        total_result = hook.get_first(total_query)
        total_events = total_result[0] if total_result else 0
        
        # Events in last 24 hours
        recent_query = """
            SELECT COUNT(*) FROM SPOTIFY_DATA.SILVER.silver_plays
            WHERE played_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());
        """
        recent_result = hook.get_first(recent_query)
        recent_events = recent_result[0] if recent_result else 0
        
        # Layer row counts
        silver_query = "SELECT COUNT(*) FROM SPOTIFY_DATA.SILVER.silver_plays;"
        silver_result = hook.get_first(silver_query)
        silver_count = silver_result[0] if silver_result else 0
        
        # Gold tables
        gold_tables = ['top_tracks', 'top_artists', 'daily_user_stats', 'device_usage']
        gold_counts = {}
        for table in gold_tables:
            query = f"SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.{table};"
            result = hook.get_first(query)
            gold_counts[table] = result[0] if result else 0
        
        # Print metrics
        print_metric("Total Events Processed", f"{total_events:,}", "success")
        print_metric("Events (Last 24 Hours)", f"{recent_events:,}")
        print_metric("Bronze Layer Rows", f"{total_events:,}")
        print_metric("Silver Layer Rows", f"{silver_count:,}")
        
        print(f"\n{BOLD}Gold Layer Tables:{RESET}")
        for table, count in gold_counts.items():
            status = "success" if count > 0 else "warning"
            print_metric(f"  {table}", f"{count:,}", status)
    
    except Exception as e:
        print_metric("Error", f"Could not fetch data metrics: {str(e)}", "failed")


def get_system_health():
    """Get system resource usage"""
    print_header("SYSTEM HEALTH")
    
    try:
        # Docker container status
        docker_output = subprocess.check_output(
            ['docker', 'ps', '--filter', 'name=airflow', '--format', '{{.Names}}: {{.Status}}']
        ).decode('utf-8')
        
        print(f"{BOLD}Airflow Containers:{RESET}")
        for line in docker_output.strip().split('\n'):
            if line:
                name, status = line.split(': ', 1)
                container_status = "success" if "healthy" in status.lower() or "up" in status.lower() else "warning"
                print_metric(f"  {name}", status, container_status)
    
    except Exception as e:
        print_metric("Status", "Container status check unavailable in Docker environment", "warning")


@provide_session
def get_monitoring_status(session=None):
    """Get monitoring DAG status"""
    print_header("MONITORING STATUS")
    
    dag_id = 'data_quality_monitoring'
    
    # Get last 5 monitoring runs
    runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id
    ).order_by(DagRun.execution_date.desc()).limit(5).all()
    
    if not runs:
        print_metric("Status", "No monitoring runs found", "warning")
        return
    
    successful_runs = [r for r in runs if r.state == 'success']
    failed_runs = [r for r in runs if r.state == 'failed']
    
    print_metric("Total Checks (last 5)", len(runs))
    print_metric("Passed", len(successful_runs), "success")
    print_metric("Failed", len(failed_runs), "failed" if failed_runs else "success")
    
    if runs[0]:
        time_since = datetime.now(timezone.utc) - runs[0].execution_date
        minutes_ago = time_since.total_seconds() / 60
        print_metric("Last Check", 
                     f"{runs[0].execution_date} ({minutes_ago:.0f} min ago)",
                     "success" if runs[0].state == 'success' else "failed")


def main():
    """Main dashboard execution"""
    print(f"\n{BOLD}{GREEN}{'*' * 60}{RESET}")
    print(f"{BOLD}{GREEN}SPOTIFY DATA PIPELINE - OPERATIONAL DASHBOARD{RESET}")
    print(f"{BOLD}{GREEN}Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{GREEN}{'*' * 60}{RESET}")
    
    get_pipeline_health()
    get_data_metrics()
    get_monitoring_status()
    get_system_health()
    
    print(f"\n{BOLD}{GREEN}{'*' * 60}{RESET}\n")


if __name__ == "__main__":
    main()
