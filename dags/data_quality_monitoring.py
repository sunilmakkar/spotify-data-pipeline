from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

# Email alert function (reused from main pipeline)
def send_monitoring_alert(context):
    """
    Send email alert when a monitoring check fails.
    Context includes task instance, execution date, etc.
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    
    # Log the failure
    logging.error(f"MONITORING ALERT: {task_id} failed at {execution_date}")
    logging.error(f"View logs: {log_url}")
    
    # Email is automatically sent via Airflow's email_on_failure setting
    # This function adds context logging

# Default arguments for monitoring DAG
default_args = {
    'owner': 'sunil',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),  # Historical start date
    'email': ['sunil.makkar97@gmail.com'],
    'email_on_failure': True,  # Send email if ANY check fails
    'email_on_retry': False,   # Don't spam on retries
    'retries': 0,              # NO RETRIES for monitoring (want immediate visibility)
    'on_failure_callback': send_monitoring_alert,  # Custom logging + email
}

# Create the monitoring DAG
dag = DAG(
    dag_id='data_quality_monitoring',
    default_args=default_args,
    description='Independent monitoring DAG for data quality validation',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2024, 12, 1),
    catchup=False,  # Don't backfill historical runs
    max_active_runs=1,  # Only one monitoring run at a time
    tags=['monitoring', 'data-quality', 'alerts'],
)

# Start task (dummy operator for clean DAG visualization)
start_monitoring = DummyOperator(
    task_id='start_monitoring',
    dag=dag,
)

# Success task (all checks passed)
all_checks_passed = DummyOperator(
    task_id='all_checks_passed',
    dag=dag,
)

# Placeholder for actual checks (we'll implement these next)
# For now, just set up the structure
logging.info("Data Quality Monitoring DAG initialized")

# Task dependencies (we'll add the actual checks between start and success)
start_monitoring >> all_checks_passed
