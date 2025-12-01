from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 27),
}

def test_s3_connection():
    """Test AWS S3 connection"""
    hook = S3Hook(aws_conn_id='aws_default')
    # Just check if bucket exists instead of listing all buckets
    exists = hook.check_for_bucket('spotify-data-lake-sunil-2025')
    print(f"S3 Connection Success! Bucket exists: {exists}")
    return True

def test_snowflake_connection():
    """Test Snowflake connection"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    result = hook.get_first("SELECT CURRENT_VERSION() as version")
    print(f"Snowflake Connection Success! Version: {result[0]}")
    return True

with DAG(
    dag_id='test_connections',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:
    
    test_s3 = PythonOperator(
        task_id='test_s3',
        python_callable=test_s3_connection
    )
    
    test_snowflake = PythonOperator(
        task_id='test_snowflake',
        python_callable=test_snowflake_connection
    )
    
    test_s3 >> test_snowflake
