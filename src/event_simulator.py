# event_simulator.py
import uuid
import random
import hashlib
from datetime import datetime, timezone, timedelta
import json
import os
from confluent_kafka import Producer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_TOPICS

class EventSimulator:
    """Simulates Spotify events when API limits are hit"""
    
    def __init__(self):
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_API_KEY,
            'sasl.password': KAFKA_API_SECRET
        }
        self.producer = Producer(conf)
        self.topic = KAFKA_TOPICS['plays']
        
        # Track which date to use for next run
        self.date_file = '/tmp/simulator_last_date.txt'
        self.current_date = self._get_next_date()
        
        # Sample data
        self.tracks = [
            {"track_name": "Blinding Lights", "artist": "The Weeknd", "album": "After Hours", "duration": 200040},
            {"track_name": "Levitating", "artist": "Dua Lipa", "album": "Future Nostalgia", "duration": 203807},
            {"track_name": "Circles", "artist": "Post Malone", "album": "Hollywood's Bleeding", "duration": 215280},
            {"track_name": "Watermelon Sugar", "artist": "Harry Styles", "album": "Fine Line", "duration": 174000},
            {"track_name": "drivers license", "artist": "Olivia Rodrigo", "album": "SOUR", "duration": 242014},
        ]
    
    def _get_next_date(self):
        """Get the next date to use for event generation"""
        if os.path.exists(self.date_file):
            with open(self.date_file, 'r') as f:
                last_date_str = f.read().strip()
                last_date = datetime.fromisoformat(last_date_str)
                # Use next day
                next_date = last_date + timedelta(days=1)
        else:
            # First run 
            next_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Save this date for next run
        with open(self.date_file, 'w') as f:
            f.write(next_date.isoformat())
        
        return next_date
    
    def generate_track_id(self, track_name, artist_name):
        """Generate deterministic track_id based on track name and artist"""
        unique_string = f"{track_name}_{artist_name}".lower()
        hash_digest = hashlib.md5(unique_string.encode()).hexdigest()
        return f"track_{hash_digest[:12]}"
        
    def generate_event(self):
        """Generate a random simulated event"""
        track = random.choice(self.tracks)
        
        # Generate deterministic track_id
        track_id = self.generate_track_id(track["track_name"], track["artist"])
        
        # Generate random time within the current_date (spread across 24 hours)
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        
        event_timestamp = self.current_date.replace(
            hour=random_hour,
            minute=random_minute,
            second=random_second
        )
        
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": "simulated_user_123",
            "event_type": "play",
            "track_id": track_id,
            "track_name": track["track_name"],
            "artist_name": track["artist"],
            "album_name": track["album"],
            "duration_ms": track["duration"],
            "played_at": event_timestamp.isoformat().replace("+00:00", "Z"),
            "device_type": random.choice(["desktop", "mobile", "tablet"])
        }
    
    def simulate_events(self, count=100):
        """Generate and produce simulated events"""
        print(f"Generating {count} simulated events for {self.current_date.date()}...\n")
        
        for i in range(count):
            event = self.generate_event()
            self.producer.produce(
                self.topic,
                key=event['user_id'],
                value=json.dumps(event)
            )
            
            if (i + 1) % 10 == 0:
                print(f"Produced {i + 1}/{count} events")
                self.producer.poll(0)
        
        print("\nFlushing...")
        self.producer.flush()
        print(f"Produced {count} simulated events for {self.current_date.date()}!")

if __name__ == "__main__":
    simulator = EventSimulator()