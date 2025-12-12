# spotify_client.py
import sys
from pathlib import Path
import spotipy
from spotipy.oauth2 import SpotifyOAuth

sys.path.append(str(Path(__file__).parent.parent))
from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI

import uuid
from datetime import datetime

class SpotifyClient:
    """Client to interact with Spotify API"""
    
    def __init__(self):
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=SPOTIFY_REDIRECT_URI,
            scope="user-read-recently-played user-library-read user-top-read user-read-private user-read-email",
            cache_path=".cache"
        ))
        
    def get_user_id(self):
        """Get the current user's Spotify ID"""
        user_info = self.sp.current_user()
        return user_info['id']
    
    def get_recently_played(self, limit=50):
        """
        Fetch recently played tracks
        
        Args:
            limit: Number of tracks to fetch (max 50)
            
        Returns:
            List of standardized event dictionaries
        """
        results = self.sp.current_user_recently_played(limit=limit)
        events = []
        
        for item in results['items']:
            track = item['track']
            event = self._transform_to_event(track, item)
            events.append(event)
            
        return events
    
    def _transform_to_event(self, track, play_item):
        """
        Transform Spotify API response to standardized event format
        
        Args:
            track: Track object from Spotify API
            play_item: Play history item containing played_at timestamp
            
        Returns:
            Standardized event dictionary
        """
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": self.get_user_id(),
            "event_type": "play",
            "track_id": track['id'],
            "track_name": track['name'],
            "artist_name": track['artists'][0]['name'] if track['artists'] else "Unknown",
            "album_name": track['album']['name'],
            "duration_ms": track['duration_ms'],
            "played_at": play_item['played_at'],
            "device_type": (play_item.get('context') or {}).get('type', 'unknown')
        }
    
    def test_connection(self):
        """Test Spotify API connection"""
        try:
            user = self.sp.current_user()
            print(f"✓ Connected to Spotify as: {user['display_name']} ({user['id']})")
            return True
        except Exception as e:
            print(f"✗ Spotify connection failed: {e}")
            return False

if __name__ == "__main__":
    # Test the client
    client = SpotifyClient()
    
    if client.test_connection():
        print("\nFetching recently played tracks...")
        events = client.get_recently_played(limit=5)
    
        print(f"\n✓ Fetched {len(events)} events")
    
        if events:
            print("\nSample event:")
            import json
            print(json.dumps(events[0], indent=2))
        else:
            print("\nNo recent listening history found. Play some music on Spotify and try again!")
