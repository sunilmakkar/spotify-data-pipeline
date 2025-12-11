# spotify_historical_backfill.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import json
from confluent_kafka import Producer
from src.spotify_client import SpotifyClient
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_TOPICS

class SpotifyHistoricalBackfill:
    """Fetch real Spotify listening history and send to Kafka"""
    
    def __init__(self):
        # Set up Kafka producer
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET
        }
        self.producer = Producer(conf)
        self.topic = KAFKA_TOPICS['plays']
        
        # Set up Spotify client
        self.spotify_client = SpotifyClient()
        
    def backfill_recent_plays(self, limit=50):
        """
        Fetch recently played tracks from Spotify and send to Kafka
        
        Args:
            limit: Number of tracks to fetch (max 50 per Spotify API)
        """
        print(f"\nFetching {limit} recently played tracks from Spotify...")
        
        # Fetch real Spotify data
        events = self.spotify_client.get_recently_played(limit=limit)
        
        print(f"✓ Fetched {len(events)} tracks from Spotify API")
        
        if len(events) == 0:
            print("⚠ No listening history found. Play some songs on Spotify and try again!")
            return
        
        # Send each event to Kafka
        print(f"\nSending {len(events)} events to Kafka topic '{self.topic}'...")
        
        for i, event in enumerate(events):
            self.producer.produce(
                self.topic,
                key=event['user_id'],
                value=json.dumps(event)
            )
            
            if (i + 1) % 10 == 0:
                print(f"  Sent {i + 1}/{len(events)} events")
                self.producer.poll(0)
        
        # Flush remaining messages
        print("\nFlushing remaining messages...")
        self.producer.flush()
        
        print(f"✓ Successfully sent {len(events)} real Spotify events to Kafka!")
        print(f"  User: {events[0]['user_id']}")
        print(f"  Date range: {events[-1]['played_at']} to {events[0]['played_at']}")

if __name__ == "__main__":
    backfill = SpotifyHistoricalBackfill()
    backfill.backfill_recent_plays(limit=50)
