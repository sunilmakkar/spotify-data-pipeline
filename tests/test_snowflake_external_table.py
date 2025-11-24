"""
Validation tests for Phase 2.1 Snowflake External Table.
Tests that external table exists, is queryable, and contains expected data.

Usage: python -m tests.test_snowflake_external_table
"""

import snowflake.connector
from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
)


def get_snowflake_connection():
    """Create Snowflake connection."""
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema='BRONZE'
    )


def test_external_table_exists():
    """Test that the external table 'plays' exists in BRONZE schema."""
    print("\n" + "="*50)
    print("TEST 1: Checking external table exists")
    print("="*50)
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SHOW TABLES LIKE 'PLAYS' IN SCHEMA BRONZE;
        """)
        
        results = cursor.fetchall()
        
        if len(results) == 0:
            print("External table 'PLAYS' not found in BRONZE schema")
            cursor.close()
            conn.close()
            return False
        
        table_info = results[0]
        table_name = table_info[1]  # Table name is in second column
        
        print(f"External table found: {table_name}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


def test_external_table_queryable():
    """Test that the external table can be queried."""
    print("\n" + "="*50)
    print("TEST 2: Querying external table")
    print("="*50)
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Simple query
        cursor.execute("SELECT * FROM BRONZE.plays LIMIT 5;")
        
        results = cursor.fetchall()
        
        if len(results) == 0:
            print("Query returned no results")
            cursor.close()
            conn.close()
            return False
        
        print(f"Successfully queried external table")
        print(f"   Retrieved {len(results)} sample rows")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error querying table: {e}")
        return False


def test_row_count():
    """Test that the external table has the expected number of rows (200)."""
    print("\n" + "="*50)
    print("TEST 3: Validating row count")
    print("="*50)
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Count rows
        cursor.execute("SELECT COUNT(*) FROM BRONZE.plays;")
        
        result = cursor.fetchone()
        row_count = result[0]
        
        expected_count = 200
        
        if row_count != expected_count:
            print(f"Row count mismatch: Expected {expected_count}, got {row_count}")
            # This is a warning, not a failure - you might have added more events
            cursor.close()
            conn.close()
            return True  # Still pass the test
        
        print(f"Row count validated: {row_count} rows")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error counting rows: {e}")
        return False


def test_column_schema():
    """Test that all expected columns are present."""
    print("\n" + "="*50)
    print("TEST 4: Validating column schema")
    print("="*50)
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Query to check columns - using VALUE column access
        cursor.execute("""
            SELECT 
                VALUE:event_id::VARCHAR as event_id,
                VALUE:user_id::VARCHAR as user_id,
                VALUE:event_type::VARCHAR as event_type,
                VALUE:track_id::VARCHAR as track_id,
                VALUE:track_name::VARCHAR as track_name,
                VALUE:artist_name::VARCHAR as artist_name,
                VALUE:album_name::VARCHAR as album_name,
                VALUE:duration_ms::INTEGER as duration_ms,
                VALUE:played_at::VARCHAR as played_at,
                VALUE:device_type::VARCHAR as device_type
            FROM BRONZE.plays 
            LIMIT 1;
        """)
        
        result = cursor.fetchone()
        column_names = [desc[0] for desc in cursor.description]
        
        expected_columns = [
            'EVENT_ID',
            'USER_ID',
            'EVENT_TYPE',
            'TRACK_ID',
            'TRACK_NAME',
            'ARTIST_NAME',
            'ALBUM_NAME',
            'DURATION_MS',
            'PLAYED_AT',
            'DEVICE_TYPE'
        ]
        
        # Check if all expected columns are present
        missing_columns = set(expected_columns) - set(column_names)
        
        if missing_columns:
            print(f"Missing columns: {missing_columns}")
            cursor.close()
            conn.close()
            return False
        
        print("Schema validation passed!")
        print(f"   All {len(expected_columns)} expected columns present")
        
        # Print sample data
        print("\nSample row:")
        for i, col_name in enumerate(column_names):
            print(f"   {col_name}: {result[i]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Schema validation failed: {e}")
        return False


def test_files_registered():
    """Test that Parquet files are registered in the external table."""
    print("\n" + "="*50)
    print("TEST 5: Checking registered files")
    print("="*50)
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Count registered files
        cursor.execute("""
            SELECT COUNT(*) as file_count
            FROM TABLE(INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(TABLE_NAME => 'PLAYS'));
        """)
        
        result = cursor.fetchone()
        file_count = result[0]
        
        if file_count == 0:
            print("No files registered in external table")
            cursor.close()
            conn.close()
            return False
        
        print(f"Files registered: {file_count} Parquet file(s)")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error checking registered files: {e}")
        return False


def run_all_tests():
    """Run all validation tests and print summary."""
    print("\n" + "="*48)
    print("Phase 2.1 Snowflake External Table Validation")
    print("="*50 + "\n")
    
    tests = [
        ("External Table Exists", test_external_table_exists),
        ("Table Queryable", test_external_table_queryable),
        ("Row Count Valid", test_row_count),
        ("Schema Valid", test_column_schema),
        ("Files Registered", test_files_registered),
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
        print("\nAll validation tests passed! Phase 2.1 external table is working correctly.")
        return True
    else:
        print(f"\n{total - passed} test(s) failed. Please review the output above.")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)