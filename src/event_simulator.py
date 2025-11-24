# event_simulator.py
import uuid
import random
from datetime import datetime, timezone, timedelta
import json
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
        
        # Sample data
        self.tracks = [
            {"track_name": "Blinding Lights", "artist": "The Weeknd", "album": "After Hours", "duration": 200040},
            {"track_name": "Levitating", "artist": "Dua Lipa", "album": "Future Nostalgia", "duration": 203807},
            {"track_name": "Circles", "artist": "Post Malone", "album": "Hollywood's Bleeding", "duration": 215280},
            {"track_name": "Watermelon Sugar", "artist": "Harry Styles", "album": "Fine Line", "duration": 174000},
            {"track_name": "drivers license", "artist": "Olivia Rodrigo", "album": "SOUR", "duration": 242014},
        ]
        
    def generate_event(self):
        """Generate a random simulated event"""
        track = random.choice(self.tracks)
        
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": "simulated_user_123",
            "event_type": "play",
            "track_id": f"sim_{uuid.uuid4().hex[:10]}",
            "track_name": track["track_name"],
            "artist_name": track["artist"],
            "album_name": track["album"],
            "duration_ms": track["duration"],
            "played_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "device_type": random.choice(["desktop", "mobile", "tablet"])
        }
    
    def simulate_events(self, count=100):
        """Generate and produce simulated events"""
        print(f"Generating {count} simulated events...\n")
        
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
        print(f"Produced {count} simulated events!")

if __name__ == "__main__":
    simulator = EventSimulator()
    simulator.simulate_events(count=100)