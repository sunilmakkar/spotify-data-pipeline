# test_snowflake.py
import snowflake.connector
from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA
)

def test_snowflake_connection():
    """Test Snowflake connection and database access"""
    try:
        # Create connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        cursor = conn.cursor()
        
        # Test 1: Check Snowflake version
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"✓ Connected to Snowflake! Version: {version[0]}")
        
        # Test 2: Verify database exists
        cursor.execute("SELECT CURRENT_DATABASE()")
        db = cursor.fetchone()
        print(f"✓ Using database: {db[0]}")
        
        # Test 3: Verify warehouse exists
        cursor.execute("SELECT CURRENT_WAREHOUSE()")
        wh = cursor.fetchone()
        print(f"✓ Using warehouse: {wh[0]}")
        
        # Test 4: Verify schema exists
        cursor.execute("SELECT CURRENT_SCHEMA()")
        schema = cursor.fetchone()
        print(f"✓ Using schema: {schema[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n Snowflake Connection Test PASSED!")
        return True
        
    except Exception as e:
        print(f"\n Snowflake Connection Test FAILED!")
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    test_snowflake_connection()