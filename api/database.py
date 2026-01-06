"""
Snowflake database operations for recommendation API.
"""

import snowflake.connector
import logging
from typing import Dict, Any, List, Optional
import sys
from pathlib import Path

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE
)

logger = logging.getLogger(__name__)


class RecommendationDatabase:
    """Snowflake database connection and query handler."""
    
    def __init__(self, private_key_path: str = '/home/ubuntu/snowflake-keys/rsa_key.p8'):
        """
        Initialize Snowflake database connection.
        
        Args:
            private_key_path: Path to RSA private key for authentication
        """
        self.private_key_path = private_key_path
        self.conn = None
    
    def _get_private_key(self):
        """Load private key for Snowflake authentication."""
        try:
            with open(self.private_key_path, 'rb') as key_file:
                private_key = key_file.read()
            
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            
            p_key = serialization.load_pem_private_key(
                private_key,
                password=None,
                backend=default_backend()
            )
            
            return p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except FileNotFoundError:
            # For local development on Mac (fallback - won't work with MFA)
            logger.warning("Private key not found - using password fallback for local dev")
            return None
        except Exception as e:
            logger.error(f"Failed to load private key: {e}")
            raise
    
    def connect(self) -> bool:
        """
        Establish connection to Snowflake.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            private_key = self._get_private_key()
            
            if private_key:
                # Use key-pair authentication (for EC2)
                self.conn = snowflake.connector.connect(
                    account=SNOWFLAKE_ACCOUNT,
                    user=SNOWFLAKE_USER,
                    private_key=private_key,
                    warehouse=SNOWFLAKE_WAREHOUSE,
                    database=SNOWFLAKE_DATABASE,
                    schema='GOLD'
                )
            else:
                # Fallback for local dev (requires password in config)
                from config import SNOWFLAKE_PASSWORD
                self.conn = snowflake.connector.connect(
                    account=SNOWFLAKE_ACCOUNT,
                    user=SNOWFLAKE_USER,
                    password=SNOWFLAKE_PASSWORD,
                    warehouse=SNOWFLAKE_WAREHOUSE,
                    database=SNOWFLAKE_DATABASE,
                    schema='GOLD'
                )
            
            logger.info("✓ Snowflake connection established")
            return True
            
        except Exception as e:
            logger.error(f"✗ Snowflake connection failed: {e}")
            return False
    
    def get_recommendations(
        self, 
        track_id: str, 
        user_id: str = "sunilmakkar97", 
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Query Snowflake for track recommendations.
        
        Args:
            track_id: Source track Spotify ID
            user_id: User identifier
            limit: Maximum number of recommendations to return
            
        Returns:
            Dict containing recommendations and metadata
        """
        if not self.conn:
            if not self.connect():
                raise Exception("Failed to connect to Snowflake")
        
        try:
            cursor = self.conn.cursor()
            
            # Combined query to get recommendations with scores
            query = """
            SELECT 
                co.track_b_id,
                co.track_b_name,
                co.track_b_artist,
                co.cooccurrence_score,
                co.sessions_together,
                COALESCE(aa.affinity_score, 0) as artist_affinity_score,
                ROUND(
                    (co.cooccurrence_score * 0.7) + 
                    (COALESCE(aa.affinity_score, 0) / 100 * 0.3),
                    3
                ) * 100 as recommendation_score
            FROM GOLD.TRACK_COOCCURRENCE co
            LEFT JOIN GOLD.ARTIST_AFFINITY aa 
                ON co.user_id = aa.user_id 
                AND co.track_b_artist = aa.artist_name
            WHERE co.user_id = %s
              AND co.track_a_id = %s
            ORDER BY recommendation_score DESC
            LIMIT %s
            """
            
            cursor.execute(query, (user_id, track_id, limit))
            results = cursor.fetchall()
            
            # Get source track info (from first result or separate query)
            track_info_query = """
            SELECT DISTINCT track_a_name, track_a_artist
            FROM GOLD.TRACK_COOCCURRENCE
            WHERE user_id = %s AND track_a_id = %s
            LIMIT 1
            """
            cursor.execute(track_info_query, (user_id, track_id))
            track_info = cursor.fetchone()
            
            cursor.close()
            
            # Format results
            recommendations = []
            for row in results:
                recommendations.append({
                    'track_id': row[0],
                    'track_name': row[1],
                    'artist_name': row[2],
                    'cooccurrence_score': float(row[3]),
                    'sessions_together': int(row[4]),
                    'artist_affinity_score': float(row[5]),
                    'recommendation_score': float(row[6]),
                    'reasoning': self._generate_reasoning(
                        float(row[3]),  # cooccurrence_score
                        float(row[5]),  # artist_affinity_score
                        int(row[4])     # sessions_together
                    )
                })
            
            return {
                'track_id': track_id,
                'track_name': track_info[0] if track_info else 'Unknown',
                'artist_name': track_info[1] if track_info else 'Unknown',
                'recommendations': recommendations,
                'total_recommendations': len(recommendations)
            }
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def _generate_reasoning(self, cooccurrence_score: float, artist_affinity: float, sessions: int) -> str:
        """Generate human-readable explanation for recommendation."""
        reasons = []
        
        if cooccurrence_score >= 0.8:
            reasons.append(f"Frequently played together ({sessions} sessions)")
        elif cooccurrence_score >= 0.5:
            reasons.append(f"Often played together ({sessions} sessions)")
        else:
            reasons.append(f"Sometimes played together ({sessions} sessions)")
        
        if artist_affinity >= 80:
            reasons.append("artist is highly preferred")
        elif artist_affinity >= 50:
            reasons.append("artist is well-liked")
        
        return ", ".join(reasons)
    
    def health_check(self) -> bool:
        """
        Check if Snowflake connection is healthy.
        
        Returns:
            True if connection is working, False otherwise
        """
        try:
            if not self.conn:
                return self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP()")
            cursor.fetchone()
            cursor.close()
            return True
            
        except Exception:
            # Try to reconnect
            return self.connect()
    
    def close(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("✓ Snowflake connection closed")