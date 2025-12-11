"""
Fetch audio features for tracks from Spotify API and store in Snowflake
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.spotify_client import SpotifyClient
from config import SNOWFLAKE_CONFIG
from typing import List, Dict

import snowflake.connector
import json
import time

def get_unique_track_ids() -> List[str]:
    """Get all unique track IDs from SILVER.SILVER_PLAYS"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT track_id 
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
    WHERE track_id IS NOT NULL
    """
    
    cursor.execute(query)
    track_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return track_ids

def fetch_audio_features_batch(spotify_client: SpotifyClient, track_ids: List[str]) -> List[Dict]:
    """Fetch audio features for a batch of tracks (max 100 per API call)"""
    if not track_ids:
        return []
    
    # Spotify API allows max 100 tracks per request
    batch_size = 100
    all_features = []
    
    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i + batch_size]
        batch_str = ','.join(batch)
        
        try:
            response = spotify_client.sp.audio_features(tracks=batch)
            # Filter out None responses (tracks without features)
            valid_features = [f for f in response if f is not None]
            all_features.extend(valid_features)
            
            print(f"Fetched features for batch {i//batch_size + 1}: {len(valid_features)} tracks")
            
            # Rate limiting - be nice to Spotify API
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error fetching batch {i//batch_size + 1}: {str(e)}")
            continue
    
    return all_features

def store_audio_features(features: List[Dict]):
    """Store audio features in Snowflake staging table"""
    if not features:
        print("No features to store")
        return
    
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    # Create staging table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS SPOTIFY_DATA.BRONZE.TRACK_AUDIO_FEATURES (
        track_id VARCHAR(255) PRIMARY KEY,
        danceability FLOAT,
        energy FLOAT,
        key INTEGER,
        loudness FLOAT,
        mode INTEGER,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        duration_ms INTEGER,
        time_signature INTEGER,
        fetched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    cursor.execute(create_table_sql)
    
    # Insert or update features
    insert_sql = """
    MERGE INTO SPOTIFY_DATA.BRONZE.TRACK_AUDIO_FEATURES AS target
    USING (
        SELECT 
            %s as track_id,
            %s as danceability,
            %s as energy,
            %s as key,
            %s as loudness,
            %s as mode,
            %s as speechiness,
            %s as acousticness,
            %s as instrumentalness,
            %s as liveness,
            %s as valence,
            %s as tempo,
            %s as duration_ms,
            %s as time_signature
    ) AS source
    ON target.track_id = source.track_id
    WHEN MATCHED THEN 
        UPDATE SET
            danceability = source.danceability,
            energy = source.energy,
            key = source.key,
            loudness = source.loudness,
            mode = source.mode,
            speechiness = source.speechiness,
            acousticness = source.acousticness,
            instrumentalness = source.instrumentalness,
            liveness = source.liveness,
            valence = source.valence,
            tempo = source.tempo,
            duration_ms = source.duration_ms,
            time_signature = source.time_signature,
            fetched_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (track_id, danceability, energy, key, loudness, mode, speechiness, 
                acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature)
        VALUES (source.track_id, source.danceability, source.energy, source.key, 
                source.loudness, source.mode, source.speechiness, source.acousticness,
                source.instrumentalness, source.liveness, source.valence, source.tempo,
                source.duration_ms, source.time_signature)
    """
    
    # Batch insert for efficiency
    records = []
    for feature in features:
        records.append((
            feature['id'],
            feature['danceability'],
            feature['energy'],
            feature['key'],
            feature['loudness'],
            feature['mode'],
            feature['speechiness'],
            feature['acousticness'],
            feature['instrumentalness'],
            feature['liveness'],
            feature['valence'],
            feature['tempo'],
            feature['duration_ms'],
            feature['time_signature']
        ))
    
    cursor.executemany(insert_sql, records)
    conn.commit()
    
    print(f"Stored {len(records)} audio features in Snowflake")
    
    cursor.close()
    conn.close()

def main():
    """Main execution function"""
    print("Starting audio features fetch...")
    
    # Initialize Spotify client
    spotify_client = SpotifyClient()
    
    # Get unique track IDs from listening history
    print("Fetching unique track IDs from Snowflake...")
    track_ids = get_unique_track_ids()
    print(f"Found {len(track_ids)} unique tracks")
    
    # Fetch audio features
    print("Fetching audio features from Spotify API...")
    audio_features = fetch_audio_features_batch(spotify_client, track_ids)
    print(f"Successfully fetched features for {len(audio_features)} tracks")
    
    # Store in Snowflake
    print("Storing audio features in Snowflake...")
    store_audio_features(audio_features)
    
    print("Audio features fetch complete!")

if __name__ == "__main__":
    main()