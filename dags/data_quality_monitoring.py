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

def validate_row_counts(**context):
    """
    Compare Bronze vs Silver layer row counts.
    Alert if Silver has significantly fewer rows than expected (>5% loss).
    
    Bronze layer: Raw events from S3
    Silver layer: Cleaned/transformed events (some filtering expected)
    """
    logging.info("Starting row count validation...")
    
    # Connect to Snowflake using the Hook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Query Bronze layer count
    bronze_query = "SELECT COUNT(*) as row_count FROM SPOTIFY_DATA.BRONZE.plays;"
    bronze_result = hook.get_first(bronze_query)
    bronze_count = bronze_result[0] if bronze_result else 0
    
    # Query Silver layer count
    silver_query = "SELECT COUNT(*) as row_count FROM SPOTIFY_DATA.SILVER.silver_plays;"
    silver_result = hook.get_first(silver_query)
    silver_count = silver_result[0] if silver_result else 0
    
    # Calculate retention rate
    if bronze_count == 0:
        logging.warning("Bronze layer is empty - cannot validate retention rate")
        raise ValueError("Bronze layer has 0 rows - pipeline may not have run yet")
    
    retention_rate = (silver_count / bronze_count) * 100
    threshold = bronze_count * 0.95  # 95% retention expected
    
    # Log the results
    logging.info(f"Bronze layer: {bronze_count:,} rows")
    logging.info(f"Silver layer: {silver_count:,} rows")
    logging.info(f"Retention rate: {retention_rate:.2f}%")
    logging.info(f"Threshold: {threshold:,.0f} rows (95% of Bronze)")
    
    # Validate retention
    if silver_count >= threshold:
        logging.info(f"Row count validation PASSED - {silver_count:,} >= {threshold:,.0f}")
        logging.info(f"Data loss is within acceptable range ({100-retention_rate:.2f}% filtered)")
    else:
        loss_pct = ((bronze_count - silver_count) / bronze_count) * 100
        error_msg = (
            f"Row count validation FAILED!\n"
            f"Bronze: {bronze_count:,} rows\n"
            f"Silver: {silver_count:,} rows\n"
            f"Retention: {retention_rate:.2f}%\n"
            f"Expected: >=95% retention\n"
            f"Actual loss: {loss_pct:.2f}% (more than 5% threshold)\n"
            f"Missing rows: {bronze_count - silver_count:,}\n"
            f"This indicates excessive data loss during transformation!"
        )
        logging.error(error_msg)
        raise ValueError(error_msg)

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

# Check 1: Row Count Validation
check_row_counts = PythonOperator(
    task_id='check_row_counts',
    python_callable=validate_row_counts,
    provide_context=True,
    dag=dag,
)

# Success task (all checks passed)
all_checks_passed = DummyOperator(
    task_id='all_checks_passed',
    dag=dag,
)

# Placeholder for actual checks
# For now, just set up the structure
logging.info("Data Quality Monitoring DAG initialized")

# Task dependencies
start_monitoring >> check_row_counts >> all_checks_passed
