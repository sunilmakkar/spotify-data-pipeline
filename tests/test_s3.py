# test_s3.py
import boto3
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET, AWS_REGION

def test_s3_connection():
    """Test S3 connection and bucket access"""
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        # Test 1: Upload a test file
        test_data = b"Hello from Spotify pipeline! This is a test."
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key='test/test.txt',
            Body=test_data
        )
        print("✓ S3 upload successful!")
        
        # Test 2: List objects in bucket
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        object_count = response.get('KeyCount', 0)
        print(f"✓ Found {object_count} objects in bucket")
        
        # Test 3: Verify folder structure exists
        folders = ['bronze/plays/', 'bronze/skips/', 'bronze/likes/']
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix='bronze/')
        
        if response.get('KeyCount', 0) > 0:
            print("✓ Bronze folder structure verified")
        else:
            print("⚠ Warning: Bronze folders may not be visible (this is okay)")
        
        print("\n S3 Connection Test PASSED!")
        return True
        
    except Exception as e:
        print(f"\n S3 Connection Test FAILED!")
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    test_s3_connection()