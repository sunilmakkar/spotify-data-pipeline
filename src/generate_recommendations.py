"""
Generate personalized track recommendations based on user listening profile
"""
import sys
import snowflake.connector
import math

from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.spotify_client import SpotifyClient
from config import SNOWFLAKE_CONFIG
from typing import Dict, List


def get_user_profile() -> Dict:
    """Get user's listening profile (weighted average audio features)"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        avg_danceability,
        avg_energy,
        avg_valence,
        avg_tempo,
        avg_acousticness,
        avg_instrumentalness,
        avg_speechiness,
        avg_loudness
    FROM SPOTIFY_DATA.GOLD.USER_LISTENING_PROFILE
    LIMIT 1
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if not result:
        raise Exception("No user listening profile found. Run DBT models first.")
    
    profile = {
        'danceability': result[0],
        'energy': result[1],
        'valence': result[2],
        'tempo': result[3],
        'acousticness': result[4],
        'instrumentalness': result[5],
        'speechiness': result[6],
        'loudness': result[7]
    }
    
    return profile

def get_played_track_ids() -> List[str]:
    """Get list of tracks already played (to exclude from recommendations)"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT track_id 
    FROM SPOTIFY_DATA.SILVER.SILVER_PLAYS
    """
    
    cursor.execute(query)
    track_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return track_ids

def get_top_artists() -> List[str]:
    """Get user's top 5 artists to use as recommendation seeds"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT artist_name
    FROM SPOTIFY_DATA.GOLD.TOP_ARTISTS
    ORDER BY total_plays DESC
    LIMIT 5
    """
    
    cursor.execute(query)
    artists = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return artists

def fetch_recommendations(spotify_client: SpotifyClient, profile: Dict, artists: List[str]) -> List[Dict]:
    """Fetch recommendations from Spotify API based on user profile"""
    
    # Search for artist IDs (Spotify needs artist IDs, not names)
    seed_artists = []
    for artist_name in artists[:2]:  # Use top 2 artists as seeds
        try:
            results = spotify_client.sp.search(q=f'artist:{artist_name}', type='artist', limit=1)
            if results['artists']['items']:
                seed_artists.append(results['artists']['items'][0]['id'])
        except Exception as e:
            print(f"Error searching for artist {artist_name}: {str(e)}")
    
    if not seed_artists:
        print("Warning: No seed artists found, using profile features only")
    
    # Call Spotify recommendations endpoint
    try:
        recommendations = spotify_client.sp.recommendations(
            seed_artists=seed_artists[:2] if seed_artists else None,
            limit=100,  # Get 100 candidates to filter down
            target_danceability=profile['danceability'],
            target_energy=profile['energy'],
            target_valence=profile['valence'],
            target_acousticness=profile['acousticness'],
            target_instrumentalness=profile['instrumentalness'],
            target_speechiness=profile['speechiness']
        )
        
        tracks = recommendations['tracks']
        print(f"Fetched {len(tracks)} recommendation candidates from Spotify")
        
        return tracks
        
    except Exception as e:
        print(f"Error fetching recommendations: {str(e)}")
        return []

def calculate_similarity_score(track_features: Dict, profile: Dict) -> float:
    """
    Calculate similarity between track and user profile using Euclidean distance
    Lower distance = higher similarity
    Convert to 0-100 score where 100 = perfect match
    """
    
    # Features to compare (normalized 0-1 scale)
    features = ['danceability', 'energy', 'valence', 'acousticness', 
                'instrumentalness', 'speechiness']
    
    # Calculate Euclidean distance
    distance_squared = 0
    for feature in features:
        track_val = track_features.get(feature, 0.5)  # Default to middle if missing
        profile_val = profile.get(feature, 0.5)
        distance_squared += (track_val - profile_val) ** 2
    
    distance = math.sqrt(distance_squared)
    
    # Convert distance to similarity score (0-100)
    # Max possible distance for 6 features = sqrt(6) ≈ 2.45
    # Normalize and invert: closer = higher score
    max_distance = math.sqrt(len(features))
    similarity = (1 - (distance / max_distance)) * 100
    
    return round(similarity, 2)

def score_and_filter_recommendations(
    tracks: List[Dict], 
    spotify_client: SpotifyClient,
    profile: Dict, 
    played_tracks: List[str]
) -> List[Dict]:
    """Score recommendations and filter out already-played tracks"""
    
    # Get audio features for all recommended tracks
    track_ids = [track['id'] for track in tracks]
    
    try:
        audio_features = spotify_client.sp.audio_features(tracks=track_ids)
    except Exception as e:
        print(f"Error fetching audio features for recommendations: {str(e)}")
        return []
    
    scored_recommendations = []
    
    for track, features in zip(tracks, audio_features):
        # Skip if already played
        if track['id'] in played_tracks:
            continue
        
        # Skip if no audio features available
        if not features:
            continue
        
        # Calculate similarity score
        score = calculate_similarity_score(features, profile)
        
        recommendation = {
            'track_id': track['id'],
            'track_name': track['name'],
            'artist_name': track['artists'][0]['name'],
            'album_name': track['album']['name'],
            'similarity_score': score,
            'danceability': features['danceability'],
            'energy': features['energy'],
            'valence': features['valence'],
            'tempo': features['tempo'],
            'acousticness': features['acousticness'],
            'instrumentalness': features['instrumentalness'],
            'speechiness': features['speechiness'],
            'loudness': features['loudness']
        }
        
        scored_recommendations.append(recommendation)
    
    # Sort by similarity score (highest first)
    scored_recommendations.sort(key=lambda x: x['similarity_score'], reverse=True)
    
    # Return top 50
    return scored_recommendations[:50]

def store_recommendations(recommendations: List[Dict]):
    """Store recommendations in Snowflake"""
    
    if not recommendations:
        print("No recommendations to store")
        return
    
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    # Clear existing recommendations
    cursor.execute("TRUNCATE TABLE IF EXISTS SPOTIFY_DATA.GOLD.TRACK_RECOMMENDATIONS")
    
    # Create table if doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS SPOTIFY_DATA.GOLD.TRACK_RECOMMENDATIONS (
        track_id VARCHAR(255),
        track_name VARCHAR(500),
        artist_name VARCHAR(500),
        album_name VARCHAR(500),
        similarity_score FLOAT,
        danceability FLOAT,
        energy FLOAT,
        valence FLOAT,
        tempo FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        speechiness FLOAT,
        loudness FLOAT,
        recommended_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        rank INTEGER
    )
    """
    cursor.execute(create_table_sql)
    
    # Insert recommendations
    insert_sql = """
    INSERT INTO SPOTIFY_DATA.GOLD.TRACK_RECOMMENDATIONS 
    (track_id, track_name, artist_name, album_name, similarity_score,
     danceability, energy, valence, tempo, acousticness, instrumentalness,
     speechiness, loudness, rank)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    records = []
    for rank, rec in enumerate(recommendations, 1):
        records.append((
            rec['track_id'],
            rec['track_name'],
            rec['artist_name'],
            rec['album_name'],
            rec['similarity_score'],
            rec['danceability'],
            rec['energy'],
            rec['valence'],
            rec['tempo'],
            rec['acousticness'],
            rec['instrumentalness'],
            rec['speechiness'],
            rec['loudness'],
            rank
        ))
    
    cursor.executemany(insert_sql, records)
    conn.commit()
    
    print(f"Stored {len(records)} recommendations in Snowflake")
    
    cursor.close()
    conn.close()

def main():
    """Main execution function"""
    print("Starting recommendation generation...")
    
    # Initialize Spotify client
    spotify_client = SpotifyClient()
    
    # Get user listening profile
    print("Fetching user listening profile...")
    profile = get_user_profile()
    print(f"Profile: danceability={profile['danceability']:.2f}, "
          f"energy={profile['energy']:.2f}, valence={profile['valence']:.2f}")
    
    # Get tracks already played (to exclude)
    print("Fetching played tracks...")
    played_tracks = get_played_track_ids()
    print(f"Found {len(played_tracks)} tracks already played")
    
    # Get top artists for seeding
    print("Fetching top artists...")
    top_artists = get_top_artists()
    print(f"Top artists: {', '.join(top_artists[:3])}")
    
    # Fetch recommendations from Spotify
    print("Fetching recommendations from Spotify API...")
    candidate_tracks = fetch_recommendations(spotify_client, profile, top_artists)
    
    # Score and filter recommendations
    print("Scoring and filtering recommendations...")
    final_recommendations = score_and_filter_recommendations(
        candidate_tracks, 
        spotify_client,
        profile, 
        played_tracks
    )
    print(f"Generated {len(final_recommendations)} final recommendations")
    
    # Store in Snowflake
    print("Storing recommendations in Snowflake...")
    store_recommendations(final_recommendations)
    
    print("Recommendation generation complete!")
    print(f"Top 5 recommendations:")
    for i, rec in enumerate(final_recommendations[:5], 1):
        print(f"  {i}. {rec['track_name']} by {rec['artist_name']} "
              f"(score: {rec['similarity_score']}/100)")

if __name__ == "__main__":
    main()