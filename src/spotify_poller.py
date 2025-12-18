# src/spotify_poller.py
"""
Spotify Currently Playing Poller - Phase 6.1
Continuously monitors Spotify playback and stores current track state in Snowflake.
Polls every 5 seconds, detects track changes, logs to file.
"""

import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

import snowflake.connector
from snowflake.connector import DictCursor

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from src.spotify_client import SpotifyClient
from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE
)


class SpotifyPoller:
    """Polls Spotify API for currently playing track and stores in Snowflake"""
    
    def __init__(self, poll_interval: int = 5):
        """
        Initialize the Spotify poller.
        
        Args:
            poll_interval: Seconds between polls (default: 5)
        """
        self.poll_interval = poll_interval
        self.last_track_id = None
        self.spotify_client = SpotifyClient()
        self.snowflake_conn = None
        
        # Setup logging
        self.setup_logging()
        self.logger.info("=" * 80)
        self.logger.info("Spotify Poller Initialized")
        self.logger.info(f"Poll interval: {poll_interval} seconds")
        self.logger.info("=" * 80)
        
    def setup_logging(self):
        """Configure logging to file and console"""
        # Create logs directory
        log_dir = Path.home() / "logs"
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / "spotify_poller.log"
        
        # Configure logger
        self.logger = logging.getLogger("SpotifyPoller")
        self.logger.setLevel(logging.INFO)
        
        # File handler (detailed logs)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Console handler (summary logs)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Logging initialized. Log file: {log_file}")
    
    def get_snowflake_connection(self):
        """Create Snowflake connection"""
        try:
            conn = snowflake.connector.connect(
                account=SNOWFLAKE_ACCOUNT,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema='BRONZE'
            )
            self.logger.info("✓ Snowflake connection established")
            return conn
        except Exception as e:
            self.logger.error(f"✗ Snowflake connection failed: {e}")
            raise
    
    async def get_currently_playing(self) -> Optional[Dict[str, Any]]:
        """
        Fetch currently playing track from Spotify API.
        
        Returns:
            Dictionary with track data, or None if nothing playing
        """
        try:
            # Get full playback state
            playback = self.spotify_client.sp.current_playback()
            
            if playback is None or not playback.get('item'):
                self.logger.debug("No track currently playing")
                return None
            
            track = playback['item']
            device = playback.get('device', {})
            context = playback.get('context', {})
            
            track_data = {
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_name': track['artists'][0]['name'] if track.get('artists') else 'Unknown',
                'album_name': track['album']['name'] if track.get('album') else 'Unknown',
                'is_playing': playback.get('is_playing', False),
                'progress_ms': playback.get('progress_ms', 0),
                'duration_ms': track.get('duration_ms', 0),
                'device_type': device.get('type', 'Unknown'),
                'device_name': device.get('name', 'Unknown'),
                'shuffle_state': playback.get('shuffle_state', False),
                'repeat_state': playback.get('repeat_state', 'off'),
                'context_type': context.get('type') if context else None,
                'context_uri': context.get('uri') if context else None
            }
            
            return track_data
            
        except Exception as e:
            self.logger.error(f"Error fetching currently playing track: {e}")
            return None
    
    def detect_track_change(self, current_track_id: str) -> bool:
        """
        Detect if track has changed since last poll.
        
        Args:
            current_track_id: Track ID from current poll
            
        Returns:
            True if track changed, False otherwise
        """
        if self.last_track_id is None:
            # First poll
            return True
        
        return current_track_id != self.last_track_id
    
    async def store_current_track(self, track_data: Dict[str, Any]):
        """
        Store current track data in Snowflake.
        
        Args:
            track_data: Dictionary containing track information
        """
        try:
            # Ensure connection is alive
            if self.snowflake_conn is None or self.snowflake_conn.is_closed():
                self.logger.info("Reconnecting to Snowflake...")
                self.snowflake_conn = self.get_snowflake_connection()
            
            cursor = self.snowflake_conn.cursor()
            
            insert_query = """
            INSERT INTO currently_playing (
                detected_at,
                track_id,
                track_name,
                artist_name,
                album_name,
                is_playing,
                progress_ms,
                duration_ms,
                device_type,
                device_name,
                shuffle_state,
                repeat_state,
                context_type,
                context_uri
            ) VALUES (
                CURRENT_TIMESTAMP(),
                %(track_id)s,
                %(track_name)s,
                %(artist_name)s,
                %(album_name)s,
                %(is_playing)s,
                %(progress_ms)s,
                %(duration_ms)s,
                %(device_type)s,
                %(device_name)s,
                %(shuffle_state)s,
                %(repeat_state)s,
                %(context_type)s,
                %(context_uri)s
            )
            """
            
            cursor.execute(insert_query, track_data)
            self.snowflake_conn.commit()
            cursor.close()
            
            self.logger.info(
                f"✓ Stored: {track_data['track_name']} - {track_data['artist_name']} "
                f"[{track_data['device_type']}]"
            )
            
        except Exception as e:
            self.logger.error(f"✗ Failed to store track data: {e}")
            # Reset connection on error
            self.snowflake_conn = None
    
    async def poll_loop(self):
        """Main polling loop - runs continuously"""
        self.logger.info("Starting poll loop...")
        
        # Initial Snowflake connection
        try:
            self.snowflake_conn = self.get_snowflake_connection()
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake on startup: {e}")
            self.logger.error("Will retry on first track detection...")
        
        poll_count = 0
        
        while True:
            try:
                poll_count += 1
                
                # Get currently playing track
                track_data = await self.get_currently_playing()
                
                if track_data is None:
                    self.logger.debug(f"Poll #{poll_count}: No playback detected")
                else:
                    current_track_id = track_data['track_id']
                    
                    # Check if track changed
                    if self.detect_track_change(current_track_id):
                        self.logger.info(f"🎵 TRACK CHANGE DETECTED: {track_data['track_name']} - {track_data['artist_name']}")
                        
                        # Store in Snowflake
                        await self.store_current_track(track_data)
                        
                        # Update last track
                        self.last_track_id = current_track_id
                    else:
                        self.logger.debug(f"Poll #{poll_count}: Same track playing ({track_data['track_name']})")
                
                # Wait before next poll
                await asyncio.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                self.logger.info("Poller stopped by user (Ctrl+C)")
                break
            except Exception as e:
                self.logger.error(f"Error in poll loop: {e}")
                self.logger.info(f"Waiting 30 seconds before retry...")
                await asyncio.sleep(30)
    
    async def run(self):
        """Entry point - starts the poller with error recovery"""
        try:
            await self.poll_loop()
        except Exception as e:
            self.logger.critical(f"Critical error in poller: {e}")
            raise
        finally:
            # Cleanup
            if self.snowflake_conn and not self.snowflake_conn.is_closed():
                self.snowflake_conn.close()
                self.logger.info("Snowflake connection closed")


async def main():
    """Main entry point"""
    poller = SpotifyPoller(poll_interval=5)
    await poller.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✓ Poller stopped gracefully")
    except Exception as e:
        print(f"\n✗ Poller crashed: {e}")
        raise