#!/usr/bin/env python3

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Set, Tuple, Optional

# Load .env file first
from dotenv import load_dotenv
load_dotenv()

# Now import modular components
from youtube_api_module import YouTubeAPI
from queue_manager_module import QueueManager
from tag_selector_module import select_tags

# Rest of your code...

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/populate_queue.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("populate_queue")

# Constants
PLAYLIST_CONFIG_FILE = Path("playlist_config.json")
MIN_VIDEOS_FOR_PROCESSING = 2
TEST_DELAY = 15  # seconds (for testing)
PROD_DELAY = 65  # seconds (for production)

class PlaylistMonitor:
    """Monitors YouTube playlists and adds new videos to the processing queue."""
    
    def __init__(self, config_file: Path = PLAYLIST_CONFIG_FILE, testing: bool = False):
        """Initialize the playlist monitor.
        
        Args:
            config_file (Path): Path to the playlist configuration file
            testing (bool): Whether to use test delay instead of production delay
        """
        self.config_file = config_file
        self.testing = testing
        self.delay = TEST_DELAY if testing else PROD_DELAY
        
        # Initialize components
        self.api = YouTubeAPI()
        self.queue = QueueManager()
        
        # Ensure required directories exist
        Path("logs").mkdir(exist_ok=True)
        
        logger.info(f"Playlist monitor initialized with {'testing' if testing else 'production'} settings")
    
    def load_playlist_config(self) -> List[str]:
        """Load playlist IDs from configuration file.
        
        Returns:
            List[str]: List of playlist IDs
        """
        try:
            if not self.config_file.exists():
                logger.error(f"Config file {self.config_file} not found")
                return []
            
            config = json.loads(self.config_file.read_text(encoding="utf-8"))
            playlists = [p["id"] for p in config.get("playlists", [])]
            
            logger.info(f"Loaded {len(playlists)} playlists from config")
            return playlists
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config file: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error loading config: {e}")
            return []
    
    def process_playlists(self) -> List[Dict[str, Any]]:
        """Process all playlists and find new videos.
        
        Returns:
            List[Dict[str, Any]]: List of new videos found
        """
        # Load playlists
        playlists = self.load_playlist_config()
        if not playlists:
            logger.warning("No playlists to process")
            return []
        
        # Load existing queue and seen videos
        pending_videos = self.queue.get_pending_videos()
        seen_ids = self.queue.get_all_seen_video_ids()
        
        # Track new videos
        new_videos = []
        errors = 0
        
        # Process each playlist
        for i, playlist_id in enumerate(playlists):
            try:
                logger.info(f"Processing playlist {i+1}/{len(playlists)}: {playlist_id}")
                
                # Get latest video from playlist
                video = self.api.get_latest_video(playlist_id)
                
                if not video:
                    logger.info(f"No suitable video found in playlist {playlist_id}")
                    continue
                
                video_id = video["id"]
                
                # Check if video is already seen or pending
                if video_id in seen_ids:
                    logger.info(f"Video {video_id} already processed or in queue")
                    continue
                
                # Add to new videos list
                logger.info(f"✅ Playlist {i+1}: Found new video {video_id} - {video['title']}")
                new_videos.append(video)
                
                # Add tags to the video metadata
                video["tags"] = select_tags(video["title"], video["channel"])
                
                # Add metadata about when it was found
                video["found_at"] = datetime.now().isoformat()
                
            except Exception as e:
                logger.error(f"Error processing playlist {playlist_id}: {e}")
                errors += 1
                continue
        
        logger.info(f"Found {len(new_videos)} new videos across {len(playlists)} playlists")
        logger.info(f"Encountered {errors} errors during processing")
        
        return new_videos
    
    def update_queue(self, new_videos: List[Dict[str, Any]]) -> bool:
        """Update the processing queue with new videos if enough are found.
        
        Args:
            new_videos (List[Dict[str, Any]]): List of new videos to add
            
        Returns:
            bool: True if queue was updated, False otherwise
        """
        if not new_videos:
            logger.info("No new videos to add to queue")
            return False
        
        # Add all new videos to the queue
        for video in new_videos:
            self.queue.add_to_pending(video)
        
        # Check if we now have enough for processing
        pending_count = len(self.queue.get_pending_videos())
        
        if pending_count >= MIN_VIDEOS_FOR_PROCESSING:
            logger.info(f"✅ New candidates: {len(new_videos)} → Added to processing queue")
            logger.info(f"📝 Queue updated. Summarization will trigger.")
            return True
        else:
            logger.info(f"Queue now has {pending_count} videos (need {MIN_VIDEOS_FOR_PROCESSING} to trigger processing)")
            return False
    
    def run(self) -> int:
        """Run the playlist monitoring process.
        
        Returns:
            int: Exit code (0 for success, 1 for error)
        """
        try:
            # Check YouTube API credentials
            if not self.api.check_credentials():
                logger.error("YouTube API credentials missing or invalid")
                return 1
            
            # Process playlists
            new_videos = self.process_playlists()
            
            # Update queue
            updated = self.update_queue(new_videos)
            
            return 0
            
        except Exception as e:
            logger.error(f"Unexpected error in playlist monitor: {e}")
            return 1


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="YouTube Playlist Monitor")
    parser.add_argument("--testing", action="store_true", help="Run in testing mode with shorter delays")
    parser.add_argument("--config", type=str, default=str(PLAYLIST_CONFIG_FILE), help="Path to playlist config file")
    args = parser.parse_args()
    
    # Create and run monitor
    monitor = PlaylistMonitor(
        config_file=Path(args.config),
        testing=args.testing
    )
    
    exit_code = monitor.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
