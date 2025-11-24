"""
Kafka to S3 Consumer
Consumes events from Kafka topic 'spotify-plays' and writes them to S3 as Parquet files.
Implements batching (100 events or 60 seconds) and date/hour partitioning.

Usage: python -m src.kafka_consumer
"""

import json
import time
import signal
import sys
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


class KafkaToS3Consumer:
    """Consumer that reads from Kafka and writes batches to S3 as Parquet files."""
    
    def __init__(
        self,
        topic: str = "spotify-plays",
        batch_size: int = 100,
        batch_timeout: int = 60,
        consumer_group: str = "spotify-s3-writer"
    ):
        """
        Initialize the Kafka to S3 consumer.
        
        Args:
            topic: Kafka topic to consume from
            batch_size: Number of events before writing to S3
            batch_timeout: Seconds to wait before writing partial batch
            consumer_group: Kafka consumer group ID
        """
        self.topic = topic
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.consumer_group = consumer_group
        
        # Initialize Kafka consumer
        self.consumer = self._create_consumer()
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        # Event batch storage
        self.event_batch: List[Dict] = []
        self.batch_start_time = time.time()
        
        # Statistics
        self.total_events_processed = 0
        self.total_batches_written = 0
        
        # Graceful shutdown flag
        self.running = True
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self._print_startup_banner()
    
    def _print_startup_banner(self):
        """Print startup configuration."""
        print("=" * 50)
        print("Kafka → S3 Consumer Starting")
        print("=" * 50)
        print(f"Topic: {self.topic}")
        print(f"Batch Size: {self.batch_size} events")
        print(f"Batch Timeout: {self.batch_timeout} seconds")
        print(f"S3 Bucket: {S3_BUCKET}")
        print(f"Consumer Group: {self.consumer_group}")
        print("=" * 50)
        print()
    
    def _create_consumer(self) -> Consumer:
        """
        Create and configure Kafka consumer.
        
        Returns:
            Configured Kafka Consumer instance
        """
        config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET,
            'group.id': self.consumer_group,
            'auto.offset.reset': 'earliest',  # Read from beginning for existing messages
            'enable.auto.commit': False,  # Manual commit after successful S3 write
        }
        
        consumer = Consumer(config)
        consumer.subscribe([self.topic])
        
        print(f"Kafka consumer initialized and subscribed to '{self.topic}'")
        return consumer
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print("\n Shutdown signal received. Finishing current batch...")
        self.running = False
    
    def _parse_event(self, message_value: str) -> Optional[Dict]:
        """
        Parse JSON event from Kafka message.
        Handles malformed JSON gracefully.
        
        Args:
            message_value: Raw message value as string
            
        Returns:
            Parsed event dict or None if parsing fails
        """
        try:
            event = json.loads(message_value)
            return event
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
            return None
    
    def _build_s3_path(self, played_at_str: str) -> str:
        """
        Build S3 path with date/hour partitioning based on event time.
        
        Args:
            played_at_str: ISO 8601 timestamp string from event
            
        Returns:
            S3 key path like: bronze/plays/year=2025/month=11/day=20/hour=14/part-<timestamp>.parquet
        """
        try:
            # Parse the played_at timestamp
            played_at = datetime.fromisoformat(played_at_str.replace('Z', '+00:00'))
            
            # Generate filename with processing time for uniqueness
            processing_time = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            filename = f"part-{processing_time}.parquet"
            
            # Build partitioned path using event time
            s3_key = (
                f"bronze/plays/"
                f"year={played_at.year}/"
                f"month={played_at.month:02d}/"
                f"day={played_at.day:02d}/"
                f"hour={played_at.hour:02d}/"
                f"{filename}"
            )
            
            return s3_key
            
        except (ValueError, AttributeError) as e:
            # Fallback if timestamp parsing fails
            print(f"Failed to parse timestamp '{played_at_str}': {e}")
            print("Using current time for partitioning")
            now = datetime.now(timezone.utc)
            processing_time = now.strftime("%Y%m%d-%H%M%S")
            filename = f"part-{processing_time}.parquet"
            
            s3_key = (
                f"bronze/plays/"
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
                f"hour={now.hour:02d}/"
                f"{filename}"
            )
            return s3_key
    
    def _convert_to_parquet(self, events: List[Dict]) -> bytes:
        """
        Convert list of events to Parquet format with explicit schema.
        
        Args:
            events: List of event dictionaries
            
        Returns:
            Parquet file as bytes
        """
        # Create DataFrame from events
        df = pd.DataFrame(events)
        
        # Define explicit schema with proper types
        # Handle missing fields by filling with None
        schema_columns = {
            'event_id': 'string',
            'user_id': 'string',
            'event_type': 'string',
            'track_id': 'string',
            'track_name': 'string',
            'artist_name': 'string',
            'album_name': 'string',
            'duration_ms': 'Int64',  # Nullable integer
            'played_at': 'string',  # Keep as string for now (will be parsed in Snowflake)
            'device_type': 'string'
        }
        
        # Ensure all columns exist (fill missing with None)
        for col, dtype in schema_columns.items():
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns to match schema
        df = df[list(schema_columns.keys())]
        
        # Apply explicit types
        for col, dtype in schema_columns.items():
            if dtype == 'Int64':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:
                df[col] = df[col].astype(dtype)
        
        # Convert to Parquet bytes
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
        buffer.seek(0)
        
        return buffer.getvalue()
    
    def _write_batch_to_s3(self, events: List[Dict], max_retries: int = 3) -> bool:
        """
        Write batch of events to S3 as Parquet file with retry logic.
        
        Args:
            events: List of event dictionaries
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if write successful, False otherwise
        """
        if not events:
            print("Empty batch, skipping write")
            return True
        
        # Use first event's timestamp for partitioning
        first_event = events[0]
        played_at = first_event.get('played_at', datetime.now(timezone.utc).isoformat())
        
        # Build S3 path with partitioning
        s3_key = self._build_s3_path(played_at)
        
        # Convert events to Parquet
        try:
            parquet_bytes = self._convert_to_parquet(events)
        except Exception as e:
            print(f"Failed to convert batch to Parquet: {e}")
            return False
        
        # Retry logic for S3 upload
        for attempt in range(1, max_retries + 1):
            try:
                self.s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=parquet_bytes
                )
                
                self.total_batches_written += 1
                print(f"Batch {self.total_batches_written} written to S3: s3://{S3_BUCKET}/{s3_key}")
                print(f"   Events in batch: {len(events)}")
                print(f"   Total events processed: {self.total_events_processed}")
                print()
                
                return True
                
            except Exception as e:
                wait_time = 2 ** (attempt - 1)  # Exponential backoff: 1s, 2s, 4s
                print(f"S3 write attempt {attempt}/{max_retries} failed: {e}")
                
                if attempt < max_retries:
                    print(f"   Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed to write batch after {max_retries} attempts. Skipping batch.")
                    return False
        
        return False
    
    def _should_flush_batch(self) -> bool:
        """
        Check if batch should be flushed based on size or timeout.
        
        Returns:
            True if batch should be written to S3
        """
        # Trigger 1: Batch size reached
        if len(self.event_batch) >= self.batch_size:
            return True
        
        # Trigger 2: Timeout reached (and batch is not empty)
        time_elapsed = time.time() - self.batch_start_time
        if len(self.event_batch) > 0 and time_elapsed >= self.batch_timeout:
            return True
        
        return False
    
    def _flush_batch(self):
        """Write current batch to S3 and reset batch state."""
        if not self.event_batch:
            return
        
        success = self._write_batch_to_s3(self.event_batch)
        
        if success:
            # Commit offsets only after successful write
            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as e:
                print(f"Failed to commit offsets: {e}")
        
        # Reset batch state regardless of success (skip and continue)
        self.event_batch = []
        self.batch_start_time = time.time()
    
    def run(self):
        """
        Main event loop: consume from Kafka, batch events, write to S3.
        Runs continuously until shutdown signal received.
        """
        print("Consumer running. Press Ctrl+C to stop.\n")
        
        try:
            while self.running:
                # Poll for messages (1 second timeout)
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message received, check if we should flush due to timeout
                    if self._should_flush_batch():
                        print(f"Batch timeout reached ({self.batch_timeout}s). Flushing batch...")
                        self._flush_batch()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                        continue
                
                # Parse event
                event = self._parse_event(msg.value().decode('utf-8'))
                
                if event is None:
                    # Skip malformed events
                    continue
                
                # Add to batch
                self.event_batch.append(event)
                self.total_events_processed += 1
                
                # Check if batch should be flushed
                if self._should_flush_batch():
                    self._flush_batch()
        
        except KeyboardInterrupt:
            print("\n KeyboardInterrupt received")
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown consumer: flush final batch and close connections."""
        print("\n Shutting down consumer...")
        
        # Flush any remaining events
        if self.event_batch:
            print(f"Flushing final batch ({len(self.event_batch)} events)...")
            self._flush_batch()
        
        # Close Kafka consumer
        print("Closing Kafka consumer...")
        self.consumer.close()
        
        # Print final statistics
        print("\n" + "=" * 50)
        print("Final Statistics")
        print("=" * 50)
        print(f"Total events processed: {self.total_events_processed}")
        print(f"Total batches written: {self.total_batches_written}")
        print("=" * 50)
        print("\n Consumer shutdown complete")


def main():
    """Entry point for the consumer."""
    consumer = KafkaToS3Consumer(
        topic="spotify-plays",
        batch_size=100,
        batch_timeout=60,
        consumer_group="spotify-s3-writer"
    )
    
    consumer.run()


if __name__ == "__main__":
    main()