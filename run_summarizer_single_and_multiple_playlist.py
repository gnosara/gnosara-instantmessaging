#!/usr/bin/env python3

import os
import json
import sys
import logging
from pathlib import Path
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/summarizer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("summarizer")

# Constants
CONFIG_FILE = "one_off_summary_config.json"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

class PlaylistSummarizer:
    """Summarizes YouTube playlist videos based on configuration."""
    
    def __init__(self, config_file=CONFIG_FILE):
        """Initialize the summarizer."""
        # Load configuration
        self.config = self._load_config(config_file)
        if not self.config:
            raise ValueError(f"Could not load configuration from {config_file}")
        
        # Ensure directories exist
        Path("logs").mkdir(exist_ok=True)
        
        # Import required modules
        self._import_modules()
        
        logger.info(f"Playlist summarizer initialized with config: {self.config}")
    
    def _load_config(self, config_file):
        """Load configuration from JSON file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return None
    
    def _import_modules(self):
        """Import required modules."""
        # Import environment variables
        try:
            from dotenv import load_dotenv
            load_dotenv()
            logger.info("Loaded environment variables")
        except ImportError:
            logger.warning("python-dotenv not installed, skipping .env loading")
        
        # Import Google API client
        try:
            import googleapiclient.discovery
            self.googleapiclient = googleapiclient
        except ImportError:
            logger.error("Google API client not installed. Run: pip install google-api-python-client")
            raise
        
        # Import YouTube Transcript API
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            self.transcript_api = YouTubeTranscriptApi
        except ImportError:
            logger.error("YouTube Transcript API not installed. Run: pip install youtube-transcript-api")
            raise
        
        # Import summary queue
        try:
            from summary_queue import SummaryQueue
            self.summary_queue = SummaryQueue()
        except ImportError:
            logger.error("summary_queue module not found")
            raise
        
        # Initialize YouTube API
        self._init_youtube_api()
    
    def _init_youtube_api(self):
        """Initialize the YouTube API client."""
        # Get API key from environment
        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key:
            logger.error("YOUTUBE_API_KEY not found in environment variables")
            raise ValueError("YOUTUBE_API_KEY not found in environment variables")
        
        try:
            # Initialize client
            self.youtube = self.googleapiclient.discovery.build(
                "youtube", "v3", developerKey=api_key
            )
            logger.info("YouTube API client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize YouTube client: {e}")
            raise
    
    def parse_duration(self, duration: str) -> int:
        """Parse ISO 8601 duration format to seconds."""
        match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
        if not match:
            logger.warning(f"Could not parse duration: {duration}")
            return 0
        
        hours, minutes, seconds = (int(x) if x else 0 for x in match.groups())
        total_seconds = hours * 3600 + minutes * 60 + seconds
        
        return total_seconds
    
    def fetch_playlist_videos_from_single_playlist(self, playlist_id: str, target_remaining: int, videos_checked_so_far: int, max_to_check: int) -> List[Dict[str, Any]]:
        """
        Fetch videos from a single playlist that match criteria.
        
        Args:
            playlist_id: The YouTube playlist ID
            target_remaining: How many more matching videos we need
            videos_checked_so_far: How many videos we've checked across all playlists
            max_to_check: Maximum videos to check across all playlists
            
        Returns:
            List of matching videos from this playlist
        """
        # Match criteria
        title_filter = self.config["settings"]["match_criteria"].get("title_contains", "")
        tag_filter = self.config["settings"]["match_criteria"].get("tags_contain", "")
        min_duration = self.config["settings"]["match_criteria"].get("min_duration_seconds", 600)
        max_age_days = self.config["settings"]["match_criteria"].get("max_age_days", 365)
        
        logger.info(f"Searching playlist {playlist_id} for {target_remaining} more matching videos")
        
        # Calculate cutoff date if max_age_days is specified
        cutoff_date = None
        if max_age_days:
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        matching_videos = []
        videos_checked_in_playlist = 0
        next_page_token = None
        
        # Calculate how many more videos we can check
        videos_remaining_to_check = max_to_check - videos_checked_so_far
        
        while (len(matching_videos) < target_remaining and 
               videos_checked_in_playlist < videos_remaining_to_check):
            try:
                # Get playlist items
                playlist_response = self.youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,  # API limit is 50
                    pageToken=next_page_token
                ).execute()
                
                items = playlist_response.get("items", [])
                if not items:
                    logger.info(f"No more videos found in playlist {playlist_id}")
                    break
                
                # Process this batch of videos
                for item in items:
                    # Check if we've reached our limits
                    if videos_checked_in_playlist >= videos_remaining_to_check:
                        logger.info(f"Reached maximum number of videos to check in this playlist")
                        break
                    
                    videos_checked_in_playlist += 1
                    
                    video_id = item["contentDetails"]["videoId"]
                    title = item["snippet"]["title"]
                    channel = item["snippet"]["channelTitle"]
                    published_at = item["snippet"]["publishedAt"]
                    published_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    
                    # Check if video is too old
                    if cutoff_date and published_date < cutoff_date:
                        logger.info(f"Skipping video {video_id} (too old: {published_date.isoformat()})")
                        continue
                    
                    # Check title filter
                    if title_filter and title_filter.lower() not in title.lower():
                        logger.info(f"Skipping video {video_id} (title doesn't match filter)")
                        continue
                    
                    # Get detailed video info (for duration and tags)
                    try:
                        video_response = self.youtube.videos().list(
                            part="contentDetails,snippet,statistics,topicDetails",
                            id=video_id
                        ).execute()
                        
                        video_items = video_response.get("items", [])
                        if not video_items:
                            logger.warning(f"No details found for video {video_id}")
                            continue
                        
                        video_info = video_items[0]
                        duration_str = video_info["contentDetails"]["duration"]
                        duration_seconds = self.parse_duration(duration_str)
                        view_count = int(video_info["statistics"].get("viewCount", 0))
                        
                        # Get tags if available
                        tags = video_info["snippet"].get("tags", [])
                        
                        # Check duration filter
                        if duration_seconds < min_duration:
                            logger.info(f"Skipping video {video_id} (too short: {duration_seconds}s < {min_duration}s)")
                            continue
                        
                        # Check tag filter
                        if tag_filter and not any(tag_filter.lower() in tag.lower() for tag in tags):
                            logger.info(f"Skipping video {video_id} (tags don't match filter)")
                            continue
                        
                        # This video passes all filters
                        logger.info(f"✅ Video {video_id} matches all criteria")
                        
                        matching_video = {
                            "id": video_id,
                            "title": title,
                            "channel": channel,
                            "published_at": published_at,
                            "published_date": published_date,
                            "duration_seconds": duration_seconds,
                            "view_count": view_count,
                            "tags": tags
                        }
                        
                        matching_videos.append(matching_video)
                        
                        # Check if we've found enough matching videos
                        if len(matching_videos) >= target_remaining:
                            logger.info(f"Found {len(matching_videos)} matching videos in this playlist")
                            break
                        
                    except Exception as e:
                        logger.error(f"Error getting details for video {video_id}: {e}")
                        continue
                
                # Get the next page token for pagination
                next_page_token = playlist_response.get("nextPageToken")
                if not next_page_token:
                    logger.info("No more pages in playlist")
                    break
                
            except Exception as e:
                logger.error(f"Error fetching playlist: {e}")
                break
        
        logger.info(f"Checked {videos_checked_in_playlist} videos in playlist {playlist_id}, found {len(matching_videos)} matching")
        return matching_videos, videos_checked_in_playlist
    
    def fetch_playlist_videos(self) -> List[Dict[str, Any]]:
        """
        Fetch videos from all playlists until we have enough matching videos
        or reach the maximum limit.
        """
        playlist_ids = self.config["playlists"]
        target_matching = self.config["settings"]["target_matching_videos"]
        max_to_check = self.config["settings"]["max_videos_to_check"]
        
        logger.info(f"Searching {len(playlist_ids)} playlists for {target_matching} total matching videos")
        logger.info(f"Will check maximum of {max_to_check} videos across all playlists")
        
        all_matching_videos = []
        total_videos_checked = 0
        
        # Check each playlist until we have enough videos or hit our limit
        for i, playlist_id in enumerate(playlist_ids):
            logger.info(f"Checking playlist {i+1}/{len(playlist_ids)}: {playlist_id}")
            
            # Skip if we already have enough videos
            if len(all_matching_videos) >= target_matching:
                logger.info(f"Already found {len(all_matching_videos)} videos (target: {target_matching}), skipping remaining playlists")
                break
                
            # Skip if we've checked too many videos
            if total_videos_checked >= max_to_check:
                logger.info(f"Already checked {total_videos_checked} videos (max: {max_to_check}), skipping remaining playlists")
                break
            
            # Calculate how many more videos we need
            target_remaining = target_matching - len(all_matching_videos)
            
            # Check this playlist
            matching_videos, videos_checked = self.fetch_playlist_videos_from_single_playlist(
                playlist_id, 
                target_remaining,
                total_videos_checked,
                max_to_check
            )
            
            # Update our counters
            all_matching_videos.extend(matching_videos)
            total_videos_checked += videos_checked
        
        logger.info(f"Total: Checked {total_videos_checked} videos across {len(playlist_ids)} playlists, found {len(all_matching_videos)} matching")
        return all_matching_videos
    
    def fetch_transcripts(self, videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch transcripts for videos."""
        if not videos:
            return []
        
        logger.info(f"Fetching transcripts for {len(videos)} videos")
        
        processed_videos = []
        for video in videos:
            video_id = video["id"]
            
            try:
                # Fetch transcript
                transcript = self.transcript_api.get_transcript(video_id)
                
                # Format for processing
                formatted = {
                    "id": video_id,
                    "title": video["title"],
                    "channel": video["channel"],
                    "podcaster": video["channel"],  # For compatibility with summarizer
                    "text": " ".join(t["text"] for t in transcript),
                    "transcript": " ".join(t["text"] for t in transcript)  # For compatibility with summarizer
                }
                
                logger.info(f"Fetched transcript for {video_id} ({len(formatted['text'])} chars)")
                processed_videos.append(formatted)
                
            except Exception as e:
                logger.warning(f"Failed to fetch transcript for {video_id}: {e}")
        
        logger.info(f"Fetched {len(processed_videos)} transcripts")
        return processed_videos
    
    def process_videos(self, videos: List[Dict[str, Any]]) -> List[str]:
        """Process videos in batches."""
        if not videos:
            return []
        
        batch_size = self.config["settings"]["processing"].get("batch_size", 2)
        batch_delay = self.config["settings"]["processing"].get("batch_delay_seconds", 65)
        writing_style = self.config["settings"]["processing"].get("writing_style")
        
        logger.info(f"Processing {len(videos)} videos in batches of {batch_size}")
        logger.info(f"Using writing style: {writing_style or 'default'}")
        
        successful_ids = []
        
        # Process in batches
        for i in range(0, len(videos), batch_size):
            batch = videos[i:i+batch_size]
            
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} videos")
            
            # Use the summary queue to process the batch
            success_ids, failed_ids = self.summary_queue.process_batch(batch, writing_style=writing_style)
            
            successful_ids.extend(success_ids)
            
            logger.info(f"Batch {i//batch_size + 1} complete: {len(success_ids)} success, {len(failed_ids)} failed")
            
            # Add a delay between batches
            if i + batch_size < len(videos):
                logger.info(f"Waiting {batch_delay} seconds before next batch...")
                time.sleep(batch_delay)
        
        logger.info(f"Processing complete: {len(successful_ids)} videos summarized successfully")
        return successful_ids
    
    def run(self) -> List[str]:
        """Run the playlist summarization process."""
        # Fetch matching videos from playlists
        videos = self.fetch_playlist_videos()
        if not videos:
            logger.error("No matching videos found in any playlist")
            return []
        
        # Fetch transcripts
        videos_with_transcripts = self.fetch_transcripts(videos)
        if not videos_with_transcripts:
            logger.error("No transcripts found for videos")
            return []
        
        # Process videos
        successful_ids = self.process_videos(videos_with_transcripts)
        
        return successful_ids


def main():
    """Main entry point for the script."""
    print("\n=== 🎬 YouTube Playlist Summarizer ===\n")
    print(f"Using configuration from: {CONFIG_FILE}\n")
    
    try:
        # Create and run summarizer
        summarizer = PlaylistSummarizer()
        successful_ids = summarizer.run()
        
        if successful_ids:
            print(f"\n✅ Successfully summarized {len(successful_ids)} videos:")
            for video_id in successful_ids:
                print(f"- https://youtube.com/watch?v={video_id}")
            print(f"\nSummaries saved to the 'summaries' directory\n")
        else:
            print("\n❌ No videos were successfully summarized\n")
            
    except Exception as e:
        logger.error(f"Error in summarization process: {e}", exc_info=True)
        print(f"\n❌ Error: {str(e)}\n")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
