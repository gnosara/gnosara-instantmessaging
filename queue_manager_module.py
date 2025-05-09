#!/usr/bin/env python3

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Set, Optional

# Set up logging
logger = logging.getLogger("queue_manager")

# Constants
PROCESSING_QUEUE_FILE = Path("processing_queue.json")
SEEN_VIDEOS_FILE = Path("seen_videos.json")


class QueueManager:
    """Manages the processing queue and seen videos tracking."""
    
    def __init__(self, queue_file: Path = PROCESSING_QUEUE_FILE, seen_file: Path = SEEN_VIDEOS_FILE):
        """Initialize the queue manager.
        
        Args:
            queue_file (Path): Path to processing queue file
            seen_file (Path): Path to seen videos file
        """
        self.queue_file = queue_file
        self.seen_file = seen_file
        
        # Ensure files exist
        self._ensure_files_exist()
        
        logger.info("Queue manager initialized")
    
    def _ensure_files_exist(self) -> None:
        """Ensure queue and seen files exist with valid JSON."""
        # Check processing queue file
        if not self.queue_file.exists():
            logger.info(f"Creating new processing queue file: {self.queue_file}")
            self.queue_file.write_text('{"pending": []}', encoding="utf-8")
        
        # Check seen videos file
        if not self.seen_file.exists():
            logger.info(f"Creating new seen videos file: {self.seen_file}")
            self.seen_file.write_text('{"done": {}}', encoding="utf-8")
    
    def get_pending_videos(self) -> List[Dict[str, Any]]:
        """Get list of pending videos.
        
        Returns:
            List[Dict[str, Any]]: List of pending videos
        """
        try:
            queue_data = json.loads(self.queue_file.read_text(encoding="utf-8"))
            pending = queue_data.get("pending", [])
            
            # Handle the case where pending might be a list of strings instead of objects
            # (for backward compatibility)
            normalized_pending = []
            for item in pending:
                if isinstance(item, str):
                    # Convert string ID to object format
                    normalized_pending.append({"id": item, "title": f"Video {item}"})
                else:
                    normalized_pending.append(item)
            
            logger.info(f"Loaded {len(normalized_pending)} pending videos")
            return normalized_pending
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing processing queue file: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error loading processing queue: {e}")
            return []
    
    def get_seen_videos(self) -> Dict[str, Dict[str, Any]]:
        """Get dictionary of seen videos.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping video IDs to metadata
        """
        try:
            seen_data = json.loads(self.seen_file.read_text(encoding="utf-8"))
            done = seen_data.get("done", {})
            
            # Handle the case where done might be a list instead of a dict
            # (for backward compatibility)
            if isinstance(done, list):
                # Convert list to dictionary format
                normalized_done = {}
                for video_id in done:
                    normalized_done[video_id] = {
                        "processed_at": "unknown", 
                        "title": f"Video {video_id}"
                    }
                return normalized_done
            
            logger.info(f"Loaded {len(done)} seen videos")
            return done
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing seen videos file: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error loading seen videos: {e}")
            return {}
    
    def get_all_seen_video_ids(self) -> Set[str]:
        """Get set of all seen video IDs (both pending and done).
        
        Returns:
            Set[str]: Set of all video IDs
        """
        # Get pending video IDs
        pending_ids = {v["id"] for v in self.get_pending_videos()}
        
        # Get done video IDs
        done_ids = set(self.get_seen_videos().keys())
        
        # Combine and return
        all_ids = pending_ids.union(done_ids)
        logger.info(f"Found {len(all_ids)} total seen video IDs")
        
        return all_ids
    
    def add_to_pending(self, video: Dict[str, Any]) -> bool:
        """Add a video to the pending list.
        
        Args:
            video (Dict[str, Any]): Video metadata to add
            
        Returns:
            bool: True if added successfully, False otherwise
        """
        try:
            # Load current queue
            queue_data = json.loads(self.queue_file.read_text(encoding="utf-8"))
            pending = queue_data.get("pending", [])
            
            # Check if video is already in pending
            video_id = video["id"]
            if any(v.get("id") == video_id for v in pending):
                logger.info(f"Video {video_id} is already in pending list")
                return False
            
            # Add to pending
            pending.append(video)
            queue_data["pending"] = pending
            
            # Save updated queue
            self.queue_file.write_text(json.dumps(queue_data, indent=2), encoding="utf-8")
            
            logger.info(f"Added video {video_id} to pending list")
            return True
            
        except Exception as e:
            logger.error(f"Error adding video to pending list: {e}")
            return False
    
    def mark_as_done(self, video_ids: List[str], titles: Optional[List[str]] = None) -> bool:
        """Mark videos as done (processed).
        
        Args:
            video_ids (List[str]): List of video IDs to mark as done
            titles (Optional[List[str]]): Optional list of video titles
            
        Returns:
            bool: True if updated successfully, False otherwise
        """
        if not video_ids:
            return True
        
        try:
            # Load current queue and seen data
            queue_data = json.loads(self.queue_file.read_text(encoding="utf-8"))
            seen_data = json.loads(self.seen_file.read_text(encoding="utf-8"))
            
            # Get pending videos and done videos
            pending = queue_data.get("pending", [])
            done = seen_data.get("done", {})
            
            # Ensure done is a dictionary
            if isinstance(done, list):
                done = {id: {"processed_at": "unknown", "title": f"Video {id}"} for id in done}
            
            # Update each video
            timestamp = datetime.now().isoformat()
            for i, video_id in enumerate(video_ids):
                # Add to done list with timestamp and title
                title = titles[i] if titles and i < len(titles) else None
                
                # If title not provided, try to find from pending list
                if title is None:
                    for pending_video in pending:
                        if isinstance(pending_video, dict) and pending_video.get("id") == video_id:
                            title = pending_video.get("title", f"Video {video_id}")
                            break
                    
                    # If still not found, use default
                    if title is None:
                        title = f"Video {video_id}"
                
                # Add to done dictionary
                done[video_id] = {
                    "processed_at": timestamp,
                    "title": title
                }
                
                # Remove from pending list
                if isinstance(pending, list):
                    # Handle both object format and string format
                    new_pending = []
                    for item in pending:
                        if isinstance(item, str) and item == video_id:
                            continue
                        elif isinstance(item, dict) and item.get("id") == video_id:
                            continue
                        new_pending.append(item)
                    pending = new_pending
            
            # Update files
            queue_data["pending"] = pending
            seen_data["done"] = done
            
            self.queue_file.write_text(json.dumps(queue_data, indent=2), encoding="utf-8")
            self.seen_file.write_text(json.dumps(seen_data, indent=2), encoding="utf-8")
            
            logger.info(f"Marked {len(video_ids)} videos as done")
            return True
            
        except Exception as e:
            logger.error(f"Error marking videos as done: {e}")
            return False
    
    def get_queue_counts(self) -> Dict[str, int]:
        """Get counts of videos in queue and seen lists.
        
        Returns:
            Dict[str, int]: Dictionary with counts
        """
        pending_count = len(self.get_pending_videos())
        seen_count = len(self.get_seen_videos())
        
        return {
            "pending": pending_count,
            "done": seen_count,
            "total": pending_count + seen_count
        }


# Example usage
if __name__ == "__main__":
    # Set up logging for standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create queue manager
    queue = QueueManager()
    
    # Print current counts
    counts = queue.get_queue_counts()
    print(f"Current queue status:")
    print(f"  Pending: {counts['pending']}")
    print(f"  Done: {counts['done']}")
    print(f"  Total: {counts['total']}")
    
    # Test adding a video
    test_video = {
        "id": "test123",
        "title": "Test Video",
        "channel": "Test Channel",
        "duration_seconds": 600
    }
    
    if queue.add_to_pending(test_video):
        print(f"Added test video to pending list")
    
    # Test marking as done
    if queue.mark_as_done(["test123"]):
        print(f"Marked test video as done")
    
    # Print updated counts
    counts = queue.get_queue_counts()
    print(f"Updated queue status:")
    print(f"  Pending: {counts['pending']}")
    print(f"  Done: {counts['done']}")
    print(f"  Total: {counts['total']}")
