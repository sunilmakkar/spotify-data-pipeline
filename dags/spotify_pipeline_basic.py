from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('/opt/airflow/.env')

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 26),
    'depends_on_past': False,
    'retries': 3,                      # Retry failed tasks 3 times
    'retry_delay': timedelta(minutes=2),  # Wait 2 minutes between retries
    'retry_exponential_backoff': True,    # Exponential backoff: 2min → 4min → 8min
    'max_retry_delay': timedelta(minutes=10),  # Cap max wait at 10 minutes
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['sunil.makkar97@gmail.com'],
}

def task_failure_alert(context):
    """
    Callback function that runs when a task fails after all retries.
    In production, this would send alerts to Slack/PagerDuty/email.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    
    error_message = f"""
    TASK FAILURE ALERT
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {execution_date}
    
    Task failed after {task_instance.max_tries} attempts.
    Check logs for details.
    """
    
    print(error_message)
    # In production, you'd send this to Slack/email/PagerDuty here

# Define the DAG
with DAG(
    dag_id='spotify_data_basic',
    default_args=default_args,
    description='Basic Spotify data pipeline: Events -> DBT transformations',
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['spotify', 'pipeline', 'basic'],
) as dag:

    # Task 1: Start Kafka consumer in background
    start_consumer_task = BashOperator(
        task_id='start_consumer',
        bash_command='''
        cd /opt/airflow && \
        nohup python -m src.kafka_consumer_background > /tmp/consumer.log 2>&1 &
        sleep 3
        echo "Consumer started, PID: $(cat /tmp/kafka_consumer.pid)"
        ''',
    )

    # Task 2: Generate Spotify events
    def generate_spotify_events():
        """Generate simulated Spotify events and send to Kafka"""
        import sys
        sys.path.insert(0, '/opt/airflow')  # Add project root to path
        
        from src.event_simulator import EventSimulator
        
        print("Starting event generation...")
        simulator = EventSimulator()
        simulator.simulate_events(count=200)  # Generate 200 events
        print("Event generation completed!")
        
    generate_events_task = PythonOperator(
        task_id='generate_events',
        python_callable=generate_spotify_events,
        on_failure_callback=task_failure_alert,
    )

    # Task 3: Wait for consumer to write files to S3
    wait_for_s3_task = S3KeySensor(
        task_id='wait_for_s3_files',
        bucket_name='spotify-data-lake-sunil-2025',
        bucket_key='bronze/plays/*',  # Wildcard - any file in this prefix
        wildcard_match=True,
        aws_conn_id='aws_default',
        timeout=180,  # 3 minutes max wait
        poke_interval=10,  # Check every 10 seconds
        mode='poke',
        on_failure_callback=task_failure_alert,
    )

    # Task 4: Stop Kafka consumer
    stop_consumer_task = BashOperator(
        task_id='stop_consumer',
        bash_command='''
        if [ -f /tmp/kafka_consumer.pid ]; then
            echo "Stopping consumer (PID: $(cat /tmp/kafka_consumer.pid))..."
            kill $(cat /tmp/kafka_consumer.pid) 2>/dev/null || true
            sleep 2
            echo "Consumer stopped"
            # Show final consumer logs
            echo "=== Consumer Logs (last 20 lines) ==="
            tail -20 /tmp/consumer.log || true
        else
            echo "No PID file found, consumer may have already stopped"
        fi
        ''',
    )

    # Task 5: Refresh Snowflake external table
    refresh_snowflake_task = SnowflakeOperator(
        task_id='refresh_snowflake_table',
        snowflake_conn_id='snowflake_default',
        sql='ALTER EXTERNAL TABLE SPOTIFY_DATA.BRONZE.plays REFRESH;',
        warehouse='SPOTIFY_WH',
        database='SPOTIFY_DATA',
        schema='BRONZE',
        on_failure_callback=task_failure_alert,
    )

    # Task 6: Run DBT transformations

    # Task 6a: Compile DBT models (validate SQL)
    dbt_compile_task = BashOperator(
        task_id='dbt_compile',
        bash_command='cd /opt/airflow/dbt && dbt compile',
        on_failure_callback=task_failure_alert,
    )

    # Task 6b: Run Silver layer transformations
    dbt_run_silver_task = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/dbt && dbt run --select silver.*',
        on_failure_callback=task_failure_alert,
    )
    
    # Task 6c: Run Gold layer transformations
    dbt_run_gold_task = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt && dbt run --select gold.*',
        on_failure_callback=task_failure_alert,
    )
    
    # Task 6d: Run DBT tests
    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test',
        on_failure_callback=task_failure_alert,
    )

    # Task 7: Log pipeline success
    def log_pipeline_success():
        """Log successful pipeline completion"""
        print("=" * 50)
        print("Spotify Pipeline Completed Successfully!")
        print("=" * 50)
        print("Events generated and sent to Kafka")
        print("DBT transformations executed")
        print("Data available in Snowflake Gold Layer")
        print("=" * 50)

    log_success_task = PythonOperator(
        task_id='log_success',
        python_callable=log_pipeline_success,
    )

    # Define task dependencies (execution order)
    # Start consumer and generate events in parallel
    start_consumer_task >> wait_for_s3_task
    generate_events_task >> wait_for_s3_task
    
    # Sequential flow after both complete
    wait_for_s3_task >> stop_consumer_task >> refresh_snowflake_task >> dbt_compile_task >> dbt_run_silver_task >> dbt_run_gold_task >> dbt_test_task >> log_success_task
