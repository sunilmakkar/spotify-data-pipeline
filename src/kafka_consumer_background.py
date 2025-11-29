"""
Kafka to S3 Consumer - Background Mode (Airflow Optimized)
Consumes events from Kafka topic 'spotify-plays' and writes them to S3 as Parquet files.
Implements batching, graceful shutdown, and PID file management for orchestration.

Differences from kafka_consumer.py:
- Writes PID file on startup for process management
- Checks for stop signal file to gracefully exit
- Runs for max duration then exits (doesn't run forever)
- Enhanced logging for Airflow task monitoring

Usage in Airflow:
- Start: nohup python -m src.kafka_consumer_background > /tmp/consumer.log 2>&1 &
- Stop: kill $(cat /tmp/kafka_consumer.pid)
"""
import json
import time
import signal
import sys
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
import boto3
from io import BytesIO
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_API_KEY,
    KAFKA_API_SECRET,
    S3_BUCKET,
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
)

# Configuration
TOPIC = 'spotify-plays'
CONSUMER_GROUP = 'spotify-s3-writer'
BATCH_SIZE = 100
BATCH_TIMEOUT = 60  # seconds
MAX_RUNTIME = 300   # 5 minutes max (safety limit)
PID_FILE = '/tmp/kafka_consumer.pid'
STOP_SIGNAL_FILE = '/tmp/kafka_consumer_stop'

class BackgroundKafkaConsumer:
    """Kafka consumer optimized for background orchestration"""
    
    def __init__(self):
        print("=" * 60)
        print("BACKGROUND KAFKA CONSUMER - AIRFLOW MODE")
        print("=" * 60)
        
        # Write PID file for process management
        self.write_pid_file()
        
        # Initialize Kafka consumer
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        self.consumer = Consumer(conf)
        self.consumer.subscribe([TOPIC])
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        # State tracking
        self.batch = []
        self.batch_start_time = time.time()
        self.start_time = time.time()
        self.total_consumed = 0
        self.total_written = 0
        self.running = True
        
        print(f"Consumer initialized successfully")
        print(f"Topic: {TOPIC}")
        print(f"Consumer Group: {CONSUMER_GROUP}")
        print(f"Batch Size: {BATCH_SIZE}")
        print(f"Batch Timeout: {BATCH_TIMEOUT}s")
        print(f"Max Runtime: {MAX_RUNTIME}s")
        print(f"PID: {os.getpid()}")
        print(f"PID File: {PID_FILE}")
        print("=" * 60)
    
    def write_pid_file(self):
        """Write process ID to file for external process management"""
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        print(f"PID file written: {PID_FILE}")
    
    def remove_pid_file(self):
        """Remove PID file on shutdown"""
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
            print(f"PID file removed: {PID_FILE}")
    
    def should_stop(self) -> bool:
        """Check if consumer should stop gracefully"""
        # Check for stop signal file
        if os.path.exists(STOP_SIGNAL_FILE):
            print(f"Stop signal detected: {STOP_SIGNAL_FILE}")
            return True
        
        # Check max runtime
        runtime = time.time() - self.start_time
        if runtime > MAX_RUNTIME:
            print(f"Max runtime reached: {runtime:.1f}s > {MAX_RUNTIME}s")
            return True
        
        return False
    
    def write_batch_to_s3(self) -> bool:
        """Write current batch to S3 as Parquet file"""
        if not self.batch:
            return False
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.batch)
            
            # Extract date/hour from first event for partitioning
            first_event = self.batch[0]
            played_at = datetime.fromisoformat(first_event['played_at'].replace('Z', '+00:00'))
            
            year = played_at.strftime('%Y')
            month = played_at.strftime('%m')
            day = played_at.strftime('%d')
            hour = played_at.strftime('%H')
            
            # Generate S3 key with partitioning
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
            s3_key = f"bronze/plays/year={year}/month={month}/day={day}/hour={hour}/part-{timestamp}.parquet"
            
            # Write to Parquet in memory
            table = pa.Table.from_pandas(df)
            parquet_buffer = BytesIO()
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)
            
            # Upload to S3
            print(f"\n{'='*60}")
            print(f"Writing batch to S3...")
            print(f"Events in batch: {len(self.batch)}")
            print(f"S3 Key: {s3_key}")
            
            self.s3_client.upload_fileobj(parquet_buffer, S3_BUCKET, s3_key)
            
            self.total_written += len(self.batch)
            print(f"Batch written successfully!")
            print(f"Total events written to S3: {self.total_written}")
            print(f"{'='*60}\n")
            
            # Clear batch
            self.batch = []
            self.batch_start_time = time.time()
            
            return True
            
        except Exception as e:
            print(f"Error writing batch to S3: {e}")
            return False
    
    def process_message(self, msg):
        """Process a single Kafka message"""
        try:
            event = json.loads(msg.value().decode('utf-8'))
            self.batch.append(event)
            self.total_consumed += 1
            
            if self.total_consumed % 10 == 0:
                print(f"Consumed {self.total_consumed} events (batch: {len(self.batch)}/{BATCH_SIZE})")
            
            # Check if batch is full
            if len(self.batch) >= BATCH_SIZE:
                print(f"\n✓ Batch full ({len(self.batch)} events)")
                self.write_batch_to_s3()
                self.consumer.commit()
                
        except json.JSONDecodeError as e:
            print(f"❌ Failed to decode message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def run(self):
        """Main consumer loop"""
        print("\nConsumer starting...\n")
        
        try:
            while self.running:
                # Check stop conditions
                if self.should_stop():
                    print("Stop condition met, shutting down...")
                    break
                
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Check batch timeout
                    if self.batch and (time.time() - self.batch_start_time) > BATCH_TIMEOUT:
                        print(f"\n✓ Batch timeout ({BATCH_TIMEOUT}s) - writing partial batch")
                        self.write_batch_to_s3()
                        self.consumer.commit()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                        continue
                
                # Process message
                self.process_message(msg)
            
            # Write any remaining events
            if self.batch:
                print(f"\n✓ Writing final batch ({len(self.batch)} events)")
                self.write_batch_to_s3()
                self.consumer.commit()
            
        except KeyboardInterrupt:
            print("\n Interrupted by user")
        except Exception as e:
            print(f"\nFatal error: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources on shutdown"""
        print("\n" + "=" * 60)
        print("CONSUMER SHUTDOWN")
        print("=" * 60)
        print(f"Total events consumed: {self.total_consumed}")
        print(f"Total events written: {self.total_written}")
        print(f"Runtime: {time.time() - self.start_time:.1f}s")
        
        # Close consumer
        if self.consumer:
            self.consumer.close()
            print("✓ Kafka consumer closed")
        
        # Remove PID file
        self.remove_pid_file()
        
        # Remove stop signal file if exists
        if os.path.exists(STOP_SIGNAL_FILE):
            os.remove(STOP_SIGNAL_FILE)
            print(f"✓ Stop signal file removed")
        
        print("=" * 60)
        print("CONSUMER STOPPED GRACEFULLY")
        print("=" * 60)

if __name__ == "__main__":
    consumer = BackgroundKafkaConsumer()
    consumer.run()
