"""
Validation tests for Phase 1.3 Kafka → S3 Parquet files.
Tests that Parquet files are properly formatted and contain expected data.

Usage: python -m tests.test_parquet_validation
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET,
    AWS_REGION
)


def test_parquet_files_exist():
    """Test that Parquet files exist in the expected S3 location."""
    print("\n" + "="*50)
    print("TEST 1: Checking Parquet files exist in S3")
    print("="*50)
    
    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # List files in bronze/plays/ prefix
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix='bronze/plays/'
    )
    
    if 'Contents' not in response:
        print("No files found in bronze/plays/")
        return False
    
    parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    
    print(f"Found {len(parquet_files)} Parquet file(s)")
    for file in parquet_files:
        print(f"   - {file}")
    
    return len(parquet_files) > 0


def test_parquet_file_readable():
    """Test that a Parquet file can be read with pandas."""
    print("\n" + "="*50)
    print("TEST 2: Reading Parquet file with pandas")
    print("="*50)
    
    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # List all files in prefix
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix='bronze/plays/'
    )
    
    if 'Contents' not in response:
        print("No files found to test")
        return False
    
    # Find first .parquet file
    parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    if not parquet_files:
        print("No Parquet files found")
        return False
    
    test_file_key = parquet_files[0]
    print(f"Testing file: {test_file_key}")
    
    try:
        # Download and read Parquet file
        obj = s3.get_object(Bucket=S3_BUCKET, Key=test_file_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        
        print(f"Parquet file successfully read!")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        return True
        
    except Exception as e:
        print(f"Failed to read Parquet file: {e}")
        return False


def test_parquet_schema():
    """Test that Parquet file has expected schema."""
    print("\n" + "="*50)
    print("TEST 3: Validating Parquet schema")
    print("="*50)
    
    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # List all files in prefix
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix='bronze/plays/'
    )
    
    if 'Contents' not in response:
        print("No files found")
        return False
    
    parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    if not parquet_files:
        print("No Parquet files found")
        return False
    
    test_file_key = parquet_files[0]
    
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=test_file_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        
        # Expected columns from Phase 1.2 schema
        expected_columns = [
            'event_id',
            'user_id',
            'event_type',
            'track_id',
            'track_name',
            'artist_name',
            'album_name',
            'duration_ms',
            'played_at',
            'device_type'
        ]
        
        actual_columns = list(df.columns)
        
        # Check if all expected columns are present
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)
        
        if missing_columns:
            print(f"Missing columns: {missing_columns}")
            return False
        
        if extra_columns:
            print(f"Extra columns (unexpected): {extra_columns}")
        
        print("Schema validation passed!")
        print(f"   All {len(expected_columns)} expected columns present")
        
        # Print sample data
        print("\n Sample data (first 3 rows):")
        print(df.head(3).to_string())
        
        return True
        
    except Exception as e:
        print(f"Schema validation failed: {e}")
        return False


def test_partitioning_structure():
    """Test that files follow the expected partitioning structure."""
    print("\n" + "="*50)
    print("TEST 4: Validating partitioning structure")
    print("="*50)
    
    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix='bronze/plays/'
    )
    
    if 'Contents' not in response:
        print("No files found")
        return False
    
    parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    
    # Check if files follow partitioning pattern: year=YYYY/month=MM/day=DD/hour=HH/
    all_valid = True
    for file_path in parquet_files:
        # Expected: bronze/plays/year=2025/month=11/day=20/hour=21/part-*.parquet
        parts = file_path.split('/')
        
        if len(parts) < 6:
            print(f"Invalid path structure: {file_path}")
            all_valid = False
            continue
        
        # Check for year=, month=, day=, hour= pattern
        has_year = any('year=' in part for part in parts)
        has_month = any('month=' in part for part in parts)
        has_day = any('day=' in part for part in parts)
        has_hour = any('hour=' in part for part in parts)
        
        if not (has_year and has_month and has_day and has_hour):
            print(f"Missing partition keys in: {file_path}")
            all_valid = False
        
    if all_valid:
        print(f"All {len(parquet_files)} file(s) follow correct partitioning structure")
        print("   Pattern: bronze/plays/year=YYYY/month=MM/day=DD/hour=HH/part-*.parquet")
        return True
    else:
        return False


def run_all_tests():
    """Run all validation tests and print summary."""
    print("\n" + "="*48)
    print("Phase 1.3 Parquet Validation Test Suite")
    print("="*50 + "\n")
    
    tests = [
        ("Files Exist", test_parquet_files_exist),
        ("File Readable", test_parquet_file_readable),
        ("Schema Valid", test_parquet_schema),
        ("Partitioning Valid", test_partitioning_structure),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\nTest '{test_name}' crashed: {e}")
            results[test_name] = False
    
    # Print summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"{status} - {test_name}")
    
    print("="*50)
    print(f"Results: {passed}/{total} tests passed")
    print("="*50)
    
    if passed == total:
        print("\nAll validation tests passed! Phase 1.3 is working correctly.")
        return True
    else:
        print(f"\n{total - passed} test(s) failed. Please review the output above.")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)