#!/usr/bin/env python3

import os
import json
import time
import logging
import re
from typing import Dict, Any, Optional, List

# Import Google API libraries
try:
    import googleapiclient.discovery
    import googleapiclient.errors
except ImportError:
    logging.error("Google API client not installed. Run: pip install google-api-python-client")
    raise

# Set up logging
logger = logging.getLogger("youtube_api")

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
MIN_DURATION = 5 * 60  # Minimum video duration in seconds (5 minutes)


class YouTubeAPI:
    """Handles interactions with the YouTube API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the YouTube API client.
        
        Args:
            api_key (str, optional): YouTube API key. If not provided, will try to load from environment.
        """
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        self.youtube = None
        
        logger.info("YouTube API handler initialized")
    
    def check_credentials(self) -> bool:
        """Check if API credentials are valid.
        
        Returns:
            bool: True if credentials are valid, False otherwise
        """
        if not self.api_key:
            logger.error("No YouTube API key found")
            return False
        
        try:
            # Try to initialize client
            youtube = googleapiclient.discovery.build(
                "youtube", "v3", developerKey=self.api_key
            )
            
            # Make a simple API call to verify credentials
            youtube.channels().list(part="snippet", id="UC_x5XG1OV2P6uZZ5FSM9Ttw").execute()
            
            # Store the client if successful
            self.youtube = youtube
            
            logger.info("YouTube API credentials validated successfully")
            return True
            
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 403:
                logger.error(f"API key is invalid or has insufficient permissions: {e}")
            else:
                logger.error(f"HTTP error when validating credentials: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error validating YouTube credentials: {e}")
            return False
    
    def _ensure_client(self) -> bool:
        """Ensure the YouTube client is initialized.
        
        Returns:
            bool: True if client is ready, False otherwise
        """
        if self.youtube is not None:
            return True
        
        if not self.api_key:
            logger.error("No YouTube API key found")
            return False
        
        try:
            self.youtube = googleapiclient.discovery.build(
                "youtube", "v3", developerKey=self.api_key
            )
            return True
        except Exception as e:
            logger.error(f"Failed to initialize YouTube client: {e}")
            return False
    
    def parse_duration(self, duration: str) -> int:
        """Parse ISO 8601 duration format to seconds.
        
        Args:
            duration (str): ISO 8601 duration string (e.g., "PT1H30M15S")
            
        Returns:
            int: Duration in seconds
        """
        match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
        if not match:
            logger.warning(f"Could not parse duration: {duration}")
            return 0
        
        hours, minutes, seconds = (int(x) if x else 0 for x in match.groups())
        total_seconds = hours * 3600 + minutes * 60 + seconds
        
        return total_seconds
    
    def get_latest_video(self, playlist_id: str, retry_count: int = 0) -> Optional[Dict[str, Any]]:
        """Get the latest video from a playlist with retry logic.
        
        Args:
            playlist_id (str): YouTube playlist ID
            retry_count (int, optional): Current retry attempt
            
        Returns:
            Optional[Dict[str, Any]]: Video metadata or None if no suitable video found
        """
        if not self._ensure_client():
            return None
        
        logger.info(f"Fetching latest video from playlist {playlist_id}")
        
        try:
            # Get the latest item from the playlist
            playlist_response = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=1
            ).execute()
            
            items = playlist_response.get("items", [])
            if not items:
                logger.info(f"No videos found in playlist {playlist_id}")
                return None
            
            video_id = items[0]["contentDetails"]["videoId"]
            
            # Get video details to check duration and other metadata
            video_response = self.youtube.videos().list(
                part="contentDetails,snippet,statistics",
                id=video_id
            ).execute()
            
            video_items = video_response.get("items", [])
            if not video_items:
                logger.warning(f"Video {video_id} details not found")
                return None
            
            video_info = video_items[0]
            
            # Parse duration
            duration_str = video_info["contentDetails"]["duration"]
            duration_seconds = self.parse_duration(duration_str)
            
            # Skip if too short
            if duration_seconds < MIN_DURATION:
                logger.info(f"Video {video_id} is too short ({duration_seconds}s < {MIN_DURATION}s), skipping")
                return None
            
            # Extract relevant metadata
            metadata = {
                "id": video_id,
                "title": video_info["snippet"]["title"],
                "channel": video_info["snippet"]["channelTitle"],
                "playlist_id": playlist_id,
                "published_at": video_info["snippet"]["publishedAt"],
                "duration_seconds": duration_seconds,
                "view_count": int(video_info["statistics"].get("viewCount", 0)),
                "duration_formatted": self.format_duration(duration_seconds)
            }
            
            logger.info(f"Found video: {metadata['title']} ({metadata['id']})")
            return metadata
            
        except googleapiclient.errors.HttpError as e:
            error_reason = "Unknown"
            
            if e.resp.status == 403:
                error_reason = "API key issues or quota exceeded"
            elif e.resp.status == 404:
                error_reason = "Playlist not found"
            
            logger.error(f"YouTube API error fetching playlist {playlist_id}: {error_reason} - {e}")
            
            if retry_count < MAX_RETRIES - 1:
                retry_count += 1
                logger.info(f"Retrying ({retry_count}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
                return self.get_latest_video(playlist_id, retry_count)
            
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error fetching playlist {playlist_id}: {e}")
            
            if retry_count < MAX_RETRIES - 1:
                retry_count += 1
                logger.info(f"Retrying ({retry_count}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
                return self.get_latest_video(playlist_id, retry_count)
            
            return None
    
    def format_duration(self, seconds: int) -> str:
        """Format duration in seconds to a human-readable string.
        
        Args:
            seconds (int): Duration in seconds
            
        Returns:
            str: Formatted duration string (e.g., "1h 30m")
        """
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"


# Example usage
if __name__ == "__main__":
    # Set up logging for standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create API client
    api = YouTubeAPI()
    
    if api.check_credentials():
        # Test with a sample playlist ID
        test_playlist = "PLOGi5-fAu8bHAZDlFuohcjjOU7DlJ8bEZ"
        video = api.get_latest_video(test_playlist)
        
        if video:
            print(f"Latest video: {video['title']}")
            print(f"Channel: {video['channel']}")
            print(f"Duration: {video['duration_formatted']}")
            print(f"Published: {video['published_at']}")
        else:
            print(f"No suitable video found in playlist {test_playlist}")
    else:
        print("Failed to validate YouTube API credentials")
