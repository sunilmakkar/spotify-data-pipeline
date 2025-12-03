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

def validate_data_freshness(**context):
    """
    Check if the most recent event is within the last 24 hours.
    Detects if pipeline has stopped generating/ingesting new data.
    """
    logging.info("Starting data freshness validation...")
    
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Query most recent event timestamp
    freshness_query = "SELECT MAX(played_at) as latest_event FROM SPOTIFY_DATA.SILVER.silver_plays;"
    result = hook.get_first(freshness_query)
    
    if not result or result[0] is None:
        logging.warning("Silver layer has no data - cannot validate freshness")
        raise ValueError("Silver layer is empty - pipeline may not have run yet")
    
    latest_event = result[0]
    current_time = datetime.now()
    
    # Calculate age of most recent event
    data_age = current_time - latest_event
    age_hours = data_age.total_seconds() / 3600
    
    # Log the results
    logging.info(f"Most recent event: {latest_event}")
    logging.info(f"Current time: {current_time}")
    logging.info(f"Data age: {age_hours:.2f} hours")
    
    # Validate freshness (24 hour threshold)
    threshold_hours = 24
    if age_hours < threshold_hours:
        logging.info(f"Data freshness validation PASSED - data is {age_hours:.2f} hours old (< {threshold_hours} hours)")
    else:
        error_msg = (
            f"Data freshness validation FAILED!\n"
            f"Most recent event: {latest_event}\n"
            f"Current time: {current_time}\n"
            f"Data age: {age_hours:.2f} hours\n"
            f"Threshold: {threshold_hours} hours\n"
            f"Data is stale - pipeline may not be running or upstream source has issues!"
        )
        logging.error(error_msg)
        raise ValueError(error_msg)

def validate_dbt_tests(**context):
    """
    Check DBT test results for failures.
    DBT stores test results in Snowflake after each test run.
    """
    logging.info("Starting DBT test validation...")
    
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Query recent DBT test results
    # Note: This assumes DBT is configured to store test results
    # If table doesn't exist, this check will be skipped
    test_query = """
        SELECT 
            test_name,
            status,
            failures,
            executed_at
        FROM SPOTIFY_DATA.SILVER.dbt_test_results
        ORDER BY executed_at DESC
        LIMIT 10;
    """
    
    try:
        results = hook.get_records(test_query)
    except Exception as e:
        # Table might not exist if DBT tests haven't run yet
        logging.warning(f"Could not query DBT test results: {str(e)}")
        logging.warning("Skipping DBT test validation - table may not exist yet")
        return
    
    if not results:
        logging.warning("No DBT test results found - tests may not have run yet")
        return
    
    # Check for failures
    failed_tests = [test for test in results if test[1] == 'fail']
    
    # Log summary
    logging.info(f"Total tests checked: {len(results)}")
    logging.info(f"Failed tests: {len(failed_tests)}")
    
    # Validate
    if len(failed_tests) == 0:
        logging.info("DBT test validation PASSED - all recent tests passed")
    else:
        # Build detailed error message
        error_msg = f"DBT test validation FAILED!\n{len(failed_tests)} test(s) failed:\n\n"
        for test in failed_tests:
            test_name = test[0]
            status = test[1]
            failures = test[2]
            executed_at = test[3]
            error_msg += f"- {test_name}: {failures} failure(s) at {executed_at}\n"
        
        error_msg += "\nCheck DBT logs for details on failed tests."
        logging.error(error_msg)
        raise ValueError(error_msg)

def validate_event_volume(**context):
    """
    Detect abnormal drops in event volume by comparing to 7-day baseline.
    Alerts if today's volume is significantly lower than historical average.
    """
    logging.info("Starting event volume anomaly detection...")
    
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Query daily event counts for last 7 days
    volume_query = """
        SELECT 
            DATE(played_at) as event_date,
            COUNT(*) as event_count
        FROM SPOTIFY_DATA.SILVER.silver_plays
        WHERE played_at >= DATEADD(day, -7, CURRENT_DATE())
        GROUP BY DATE(played_at)
        ORDER BY event_date DESC;
    """
    
    results = hook.get_records(volume_query)
    
    if not results or len(results) < 2:
        logging.warning("Insufficient data for volume anomaly detection (need at least 2 days)")
        return
    
    # Extract counts
    daily_counts = [row[1] for row in results]
    dates = [row[0] for row in results]
    
    # Most recent day is today (or latest data)
    today_count = daily_counts[0]
    today_date = dates[0]
    
    # Historical baseline (exclude today, use last 6 days)
    historical_counts = daily_counts[1:]
    
    if len(historical_counts) == 0:
        logging.warning("No historical data available for comparison")
        return
    
    # Calculate baseline average
    baseline_average = sum(historical_counts) / len(historical_counts)
    threshold = baseline_average * 0.5  # 50% of average
    
    # Log the analysis
    logging.info(f"Today's date: {today_date}")
    logging.info(f"Today's event count: {today_count:,}")
    logging.info(f"Historical average (last {len(historical_counts)} days): {baseline_average:,.0f}")
    logging.info(f"Threshold (50% of average): {threshold:,.0f}")
    
    # Validate volume
    if today_count >= threshold:
        variance_pct = ((today_count - baseline_average) / baseline_average) * 100
        logging.info(f"Event volume validation PASSED - {today_count:,} >= {threshold:,.0f}")
        logging.info(f"Variance from baseline: {variance_pct:+.1f}%")
    else:
        drop_pct = ((baseline_average - today_count) / baseline_average) * 100
        error_msg = (
            f"Event volume anomaly detected!\n"
            f"Today ({today_date}): {today_count:,} events\n"
            f"Baseline average: {baseline_average:,.0f} events\n"
            f"Threshold (50%): {threshold:,.0f} events\n"
            f"Drop: {drop_pct:.1f}% below baseline\n"
            f"Historical counts: {historical_counts}\n"
            f"Possible causes: Kafka consumer down, upstream API issues, pipeline not running"
        )
        logging.error(error_msg)
        raise ValueError(error_msg)

def validate_gold_tables(**context):
    """
    Verify all Gold layer tables have data.
    Ensures downstream analytics and dashboards aren't broken.
    """
    logging.info("Starting Gold layer completeness validation...")
    
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Define Gold tables to check
    gold_tables = [
        'top_tracks',
        'top_artists',
        'daily_user_stats',
        'device_usage'
    ]
    
    empty_tables = []
    table_counts = {}
    
    # Check each table
    for table in gold_tables:
        query = f"SELECT COUNT(*) FROM SPOTIFY_DATA.GOLD.{table};"
        try:
            result = hook.get_first(query)
            count = result[0] if result else 0
            table_counts[table] = count
            
            if count == 0:
                empty_tables.append(table)
                logging.warning(f"Table {table} is EMPTY (0 rows)")
            else:
                logging.info(f"Table {table}: {count:,} rows")
        
        except Exception as e:
            logging.error(f"Error querying {table}: {str(e)}")
            empty_tables.append(f"{table} (query failed)")
    
    # Log summary
    total_rows = sum(table_counts.values())
    logging.info(f"Total Gold layer rows: {total_rows:,}")
    logging.info(f"Tables checked: {len(gold_tables)}")
    logging.info(f"Empty tables: {len(empty_tables)}")
    
    # Validate
    if len(empty_tables) == 0:
        logging.info("Gold layer validation PASSED - all tables have data")
    else:
        error_msg = (
            f"Gold layer validation FAILED!\n"
            f"Empty tables: {empty_tables}\n"
            f"Table counts: {table_counts}\n"
            f"These tables are missing data - downstream analytics may be broken!\n"
            f"Check DBT logs to see if Gold layer transformations failed."
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

# Check 2: Data Freshness Validation
check_freshness = PythonOperator(
    task_id='check_freshness',
    python_callable=validate_data_freshness,
    provide_context=True,
    dag=dag,
)

# Check 3: DBT Test Results Validation
check_dbt_tests = PythonOperator(
    task_id='check_dbt_tests',
    python_callable=validate_dbt_tests,
    provide_context=True,
    dag=dag,
)

# Check 4: Event Volume Anomaly Detection
check_volume = PythonOperator(
    task_id='check_volume',
    python_callable=validate_event_volume,
    provide_context=True,
    dag=dag,
)

# Check 5: Gold Layer Completeness
check_gold_tables = PythonOperator(
    task_id='check_gold_tables',
    python_callable=validate_gold_tables,
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
start_monitoring >> [check_row_counts, check_freshness, check_dbt_tests, check_volume, check_gold_tables] >> all_checks_passed
