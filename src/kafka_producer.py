# kafka_producer.py
from confluent_kafka import Producer
import json
import time
from src.spotify_client import SpotifyClient
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_API_KEY,
    KAFKA_API_SECRET,
    KAFKA_TOPICS
)

class SpotifyKafkaProducer:
    """Produces Spotify events to Kafka"""
    
    def __init__(self):
        self.spotify_client = SpotifyClient()
        
        # Kafka producer configuration
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET
        }
        
        self.producer = Producer(conf)
        self.topic = KAFKA_TOPICS['plays']  # or 'events'
        
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            print(f'✗ Message delivery failed: {err}')
        else:
            print(f'✓ Message delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}')
    
    def produce_event(self, event):
        """
        Produce a single event to Kafka
        
        Args:
            event: Event dictionary
        """
        try:
            self.producer.produce(
                self.topic,
                key=event['user_id'],
                value=json.dumps(event),
                callback=self.delivery_report
            )
            # Trigger delivery report callbacks
            self.producer.poll(0)
            
        except Exception as e:
            print(f"✗ Error producing event: {e}")
    
    def produce_recently_played(self, limit=50):
        """
        Fetch recently played tracks and produce to Kafka
        
        Args:
            limit: Number of tracks to fetch
        """
        print(f"\nFetching {limit} recently played tracks from Spotify...")
        events = self.spotify_client.get_recently_played(limit=limit)
        
        print(f"✓ Fetched {len(events)} events")
        print(f"Producing to Kafka topic: {self.topic}...\n")
        
        for event in events:
            self.produce_event(event)
            time.sleep(0.1)  # Small delay to avoid overwhelming Kafka
        
        # Wait for all messages to be delivered
        print("\nFlushing remaining messages...")
        self.producer.flush()
        
        print(f"\nSuccessfully produced {len(events)} events to Kafka!")
        return len(events)
    
    def produce_continuous(self, interval_seconds=60):
        """
        Continuously produce events at regular intervals
        
        Args:
            interval_seconds: How often to fetch new events
        """
        print(f"Starting continuous production (every {interval_seconds} seconds)")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                self.produce_recently_played(limit=50)
                print(f"\nWaiting {interval_seconds} seconds before next batch...\n")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n\nStopping producer...")
            self.producer.flush()
            print("✓ Producer stopped cleanly")

if __name__ == "__main__":
    import sys
    
    producer = SpotifyKafkaProducer()
    
    # Check if user wants continuous mode
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        producer.produce_continuous(interval_seconds=300)  # Every 5 minutes
    else:
        # Single batch mode
        producer.produce_recently_played(limit=50)