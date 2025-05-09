#!/usr/bin/env python3

import os
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import importlib.util
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/summary_queue.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("summary_queue")

# Constants
PROCESSING_QUEUE_FILE = Path("processing_queue.json")
SUMMARY_STATUS_FILE = Path("logs/summary_status.json")
SEEN_VIDEOS_FILE = Path("seen_videos.json")  # New file to track processed videos
SUMMARIES_DIR = Path("summaries")
MIN_BATCH_SIZE = 2  # Minimum number of videos to process in a batch
MAX_BATCH_SIZE = 5  # Maximum number of videos to process in a batch

# Default tags for different categories - used for tag generation
DEFAULT_TAGS = {
    "ai": ["#AI", "#Technology", "#Innovation", "#Podcast", "#Future"],
    "tech": ["#Technology", "#Innovation", "#Digital", "#Podcast", "#Science"],
    "health": ["#Health", "#Wellness", "#Science", "#Podcast", "#Lifestyle"],
    "business": ["#Business", "#Entrepreneurship", "#Success", "#Podcast", "#Leadership"],
    "mindfulness": ["#Mindfulness", "#Meditation", "#Wellness", "#Podcast", "#Health"],
    "finance": ["#Finance", "#Investing", "#Wealth", "#Podcast", "#Business"],
    "default": ["#Podcast", "#Interview", "#Learning", "#Knowledge", "#Insights"]
}

class SummaryQueue:
    """Handler for summary queue management and batch processing."""
    
    def __init__(self):
        """Initialize the summary queue manager."""
        # Ensure directories exist
        Path("logs").mkdir(exist_ok=True)
        SUMMARIES_DIR.mkdir(exist_ok=True)
        
        # Create processing queue file if it doesn't exist
        if not PROCESSING_QUEUE_FILE.exists():
            logger.info("Processing queue file not found, creating new one")
            PROCESSING_QUEUE_FILE.write_text('{"pending": []}', encoding="utf-8")
        
        # Create summary status file if it doesn't exist
        if not SUMMARY_STATUS_FILE.exists():
            logger.info("Summary status file not found, creating new one")
            self._init_summary_status()
            
        # Create seen videos file if it doesn't exist
        if not SEEN_VIDEOS_FILE.exists():
            logger.info("Seen videos file not found, creating new one")
            SEEN_VIDEOS_FILE.write_text('{"done": {}}', encoding="utf-8")
        
        logger.info("Summary queue manager initialized")
    
    def _init_summary_status(self) -> None:
        """Initialize the summary status file with default structure."""
        today = datetime.now().strftime("%Y-%m-%d")
        status = {
            "date": today,
            "pending": [],
            "batched": [],
            "completed": [],
            "failed": [],
            "posted": []
        }
        SUMMARY_STATUS_FILE.write_text(json.dumps(status, indent=2), encoding="utf-8")
        logger.info("Initialized summary status file")
    
    def load_processing_queue(self) -> List[Dict[str, Any]]:
        """Load the queue of videos waiting to be processed.
        
        Handles both the old format (flat array of video IDs) and the new format
        (object with 'pending' array of video objects) for backward compatibility.
        
        Returns:
            List[Dict[str, Any]]: List of video objects in the queue
        """
        try:
            data = json.loads(PROCESSING_QUEUE_FILE.read_text(encoding="utf-8"))
            
            # Handle the new format with "pending" key
            if isinstance(data, dict) and "pending" in data:
                logger.info(f"Loaded processing queue with new format: {len(data['pending'])} videos")
                return data["pending"]
            
            # Handle the old format (flat array)
            elif isinstance(data, list):
                logger.info(f"Loaded processing queue with old format: {len(data)} videos")
                # Convert string IDs to dictionary format
                normalized_data = []
                for item in data:
                    if isinstance(item, str):
                        # Convert string ID to dictionary format
                        normalized_data.append({
                            "id": item,
                            "title": f"Video {item}",
                            "channel": "Unknown",
                            "found_at": datetime.now().isoformat()
                        })
                    else:
                        normalized_data.append(item)
                return normalized_data
            
            # Handle empty or unexpected format
            else:
                logger.warning(f"Unexpected processing queue format: {type(data)}, returning empty list")
                return []
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing processing queue file: {e}")
            return []
    
    def save_processing_queue(self, queue: List[Dict[str, Any]]) -> None:
        """Save the queue of videos waiting to be processed.
        
        Always saves in the new format with a 'pending' array of video objects.
        
        Args:
            queue (List[Dict[str, Any]]): List of video objects to save
        """
        try:
            # Always use new format with "pending" key
            data = {"pending": queue}
            PROCESSING_QUEUE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.info(f"Updated processing queue saved with {len(queue)} videos")
        except Exception as e:
            logger.error(f"Failed to save processing queue: {e}")
    
    def load_summary_status(self) -> Dict[str, Any]:
        """Load the summary status tracking data.
        
        Returns:
            Dict[str, Any]: Summary status data
        """
        try:
            data = json.loads(SUMMARY_STATUS_FILE.read_text(encoding="utf-8"))
            
            # Check if it's a new day and reset if needed
            today = datetime.now().strftime("%Y-%m-%d")
            if data.get("date") != today:
                logger.info(f"New day detected ({today}), resetting summary status")
                self._init_summary_status()
                data = json.loads(SUMMARY_STATUS_FILE.read_text(encoding="utf-8"))
            
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing summary status file: {e}")
            self._init_summary_status()
            return json.loads(SUMMARY_STATUS_FILE.read_text(encoding="utf-8"))
    
    def save_summary_status(self, status: Dict[str, Any]) -> None:
        """Save the summary status tracking data.
        
        Args:
            status (Dict[str, Any]): Summary status data to save
        """
        try:
            SUMMARY_STATUS_FILE.write_text(json.dumps(status, indent=2), encoding="utf-8")
            logger.info("Updated summary status saved")
        except Exception as e:
            logger.error(f"Failed to save summary status: {e}")
    
    def load_seen_videos(self) -> Dict[str, Dict[str, Any]]:
        """Load the seen videos tracking data.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of processed videos
        """
        try:
            data = json.loads(SEEN_VIDEOS_FILE.read_text(encoding="utf-8"))
            return data.get("done", {})
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing seen videos file: {e}")
            return {}
            
    def save_seen_videos(self, videos: Dict[str, Dict[str, Any]]) -> None:
        """Save the seen videos tracking data.
        
        Args:
            videos (Dict[str, Dict[str, Any]]): Dictionary of processed videos
        """
        try:
            data = {"done": videos}
            SEEN_VIDEOS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
            logger.info("Updated seen videos saved")
        except Exception as e:
            logger.error(f"Failed to save seen videos: {e}")
    
    def update_pending_items(self) -> int:
        """Update the list of pending items by checking the processing queue.
        
        Returns:
            int: Number of pending items
        """
        # Load current queue and status
        queue = self.load_processing_queue()
        status = self.load_summary_status()
        
        # Update pending list with video IDs
        # Handle both string IDs and dictionary items with 'id' field
        pending_ids = []
        for item in queue:
            if isinstance(item, dict) and 'id' in item:
                pending_ids.append(item['id'])
            elif isinstance(item, str):
                pending_ids.append(item)
            else:
                logger.warning(f"Unrecognized queue item format: {item}")
        
        status["pending"] = pending_ids
        
        # Save updated status
        self.save_summary_status(status)
        
        return len(status["pending"])
    
    def ready_for_batch(self) -> bool:
        """Check if there are enough videos in the queue to process as a batch.
        
        Returns:
            bool: True if there are enough videos to batch, False otherwise
        """
        queue = self.load_processing_queue()
        return len(queue) >= MIN_BATCH_SIZE
    
    def get_next_batch(self) -> List[Dict[str, Any]]:
        """Get the next batch of videos to process.
        
        Returns:
            List[Dict[str, Any]]: Batch of videos to process
        """
        queue = self.load_processing_queue()
        
        if len(queue) < MIN_BATCH_SIZE:
            logger.info(f"Not enough videos to batch (need {MIN_BATCH_SIZE}, have {len(queue)})")
            return []
        
        # Take up to MAX_BATCH_SIZE videos from the queue
        batch_size = min(MAX_BATCH_SIZE, len(queue))
        raw_batch = queue[:batch_size]
        
        # Normalize the batch items to ensure they're in the correct format
        batch = []
        for item in raw_batch:
            if isinstance(item, dict) and 'id' in item:
                # Check for required fields and add defaults if missing
                normalized_item = {
                    "id": item["id"],
                    "channel": item.get("channel", "Unknown"),
                    "title": item.get("title", f"Video {item['id']}"),
                    "text": item.get("text", ""),
                    "found_at": item.get("found_at", datetime.now().isoformat())
                }
                
                # Include any additional metadata that might be present
                for key, value in item.items():
                    if key not in normalized_item:
                        normalized_item[key] = value
                
                batch.append(normalized_item)
            elif isinstance(item, str):
                # Convert string ID to dictionary format
                batch.append({
                    "id": item,
                    "channel": "Unknown",
                    "title": f"Video {item}",
                    "text": "",
                    "found_at": datetime.now().isoformat()
                })
            else:
                logger.warning(f"Skipping unrecognized batch item format: {item}")
        
        logger.info(f"Selected batch of {len(batch)} videos for processing")
        return batch
    
    def mark_as_batched(self, batch: List[Dict[str, Any]]) -> None:
        """Mark videos as being processed in a batch.
        
        Args:
            batch (List[Dict[str, Any]]): The batch of videos being processed
        """
        status = self.load_summary_status()
        
        # Add video IDs to batched list
        batch_ids = [item["id"] for item in batch]
        status["batched"].extend(batch_ids)
        
        # Remove duplicates
        status["batched"] = list(set(status["batched"]))
        
        # Save updated status
        self.save_summary_status(status)
        logger.info(f"Marked {len(batch_ids)} videos as batched")
    
    def mark_as_completed(self, video_ids: List[str]) -> None:
        """Mark videos as successfully completed.
        
        Args:
            video_ids (List[str]): List of video IDs that were completed
        """
        status = self.load_summary_status()
        
        # Move from batched to completed
        for vid_id in video_ids:
            if vid_id in status["batched"]:
                status["batched"].remove(vid_id)
            
            # Add to completed if not already there
            if vid_id not in status["completed"]:
                status["completed"].append(vid_id)
        
        # Save updated status
        self.save_summary_status(status)
        logger.info(f"Marked {len(video_ids)} videos as completed")
        
        # Also update seen videos
        self._update_seen_videos(video_ids, "completed")
    
    def mark_as_failed(self, video_ids: List[str]) -> None:
        """Mark videos as failed processing.
        
        Args:
            video_ids (List[str]): List of video IDs that failed processing
        """
        status = self.load_summary_status()
        
        # Move from batched to failed
        for vid_id in video_ids:
            if vid_id in status["batched"]:
                status["batched"].remove(vid_id)
            
            # Add to failed if not already there
            if vid_id not in status["failed"]:
                status["failed"].append(vid_id)
        
        # Save updated status
        self.save_summary_status(status)
        logger.info(f"Marked {len(video_ids)} videos as failed")
        
        # Also update seen videos
        self._update_seen_videos(video_ids, "failed")
    
    def mark_as_posted(self, video_ids: List[str]) -> None:
        """Mark videos as posted to social media.
        
        Args:
            video_ids (List[str]): List of video IDs that were posted
        """
        status = self.load_summary_status()
        
        # Add to posted list
        for vid_id in video_ids:
            if vid_id not in status["posted"]:
                status["posted"].append(vid_id)
        
        # Save updated status
        self.save_summary_status(status)
        logger.info(f"Marked {len(video_ids)} videos as posted")
        
        # Also update seen videos
        self._update_seen_videos(video_ids, "posted")
    
    def _update_seen_videos(self, video_ids: List[str], status_type: str) -> None:
        """Update seen videos file with newly processed videos.
        
        Args:
            video_ids (List[str]): List of video IDs to update
            status_type (str): The status to set for these videos
        """
        seen_videos = self.load_seen_videos()
        
        # Find video metadata in the summaries
        for video_id in video_ids:
            # Look for a summary file to get tags and other metadata
            summary_files = list(SUMMARIES_DIR.glob(f"{video_id}_*.json"))
            
            if summary_files:
                try:
                    # Load summary to get metadata including tags
                    with open(summary_files[0], "r", encoding="utf-8") as f:
                        summary = json.load(f)
                    
                    # Extract metadata from summary
                    title = summary.get("title", f"Video {video_id}")
                    channel = summary.get("podcaster", "Unknown")
                    
                    # Create entry with basic info
                    video_data = {
                        "title": title,
                        "channel": channel,
                        "processed_at": datetime.now().isoformat(),
                        "status": status_type
                    }
                    
                    # Add tags if present in summary
                    if "tags" in summary and isinstance(summary["tags"], list) and summary["tags"]:
                        video_data["tags"] = summary["tags"]
                    
                    # Add to seen videos
                    seen_videos[video_id] = video_data
                    
                except Exception as e:
                    logger.error(f"Error loading summary for {video_id}: {e}")
                    # Add basic entry if summary loading fails
                    seen_videos[video_id] = {
                        "title": f"Video {video_id}",
                        "processed_at": datetime.now().isoformat(),
                        "status": status_type
                    }
            else:
                # No summary file found, add basic entry
                seen_videos[video_id] = {
                    "title": f"Video {video_id}",
                    "processed_at": datetime.now().isoformat(),
                    "status": status_type
                }
        
        # Save updated seen videos
        self.save_seen_videos(seen_videos)
        logger.info(f"Updated seen videos with {len(video_ids)} entries")
    
    def remove_from_queue(self, video_ids: List[str]) -> None:
        """Remove processed videos from the queue.
        
        Args:
            video_ids (List[str]): List of video IDs to remove
        """
        queue = self.load_processing_queue()
        
        # Filter out processed videos
        new_queue = []
        for item in queue:
            item_id = item["id"] if isinstance(item, dict) and "id" in item else item
            if item_id not in video_ids:
                new_queue.append(item)
        
        # Save updated queue
        self.save_processing_queue(new_queue)
        logger.info(f"Removed {len(queue) - len(new_queue)} videos from queue")
    
    def get_summary_files(self) -> List[Tuple[str, Path]]:
        """Get list of summary files in the summaries directory.
        
        Returns:
            List[Tuple[str, Path]]: List of tuples with (video_id, file_path)
        """
        if not SUMMARIES_DIR.exists():
            logger.warning(f"Summaries directory {SUMMARIES_DIR} does not exist")
            return []
        
        summary_files = []
        
        try:
            # Get all JSON files
            files = list(SUMMARIES_DIR.glob("*.json"))
            
            for file_path in files:
                # Extract video ID from filename
                filename = file_path.name
                video_id = filename.split("_")[0]  # Assume format: VIDEO_ID_rest_of_filename.json
                
                if video_id:
                    summary_files.append((video_id, file_path))
            
            logger.info(f"Found {len(summary_files)} summary files")
            return summary_files
        
        except Exception as e:
            logger.error(f"Error listing summary files: {e}")
            return []
    
    def get_unposted_summaries(self) -> List[Tuple[str, Path]]:
        """Get list of summaries that have not been posted yet.
        
        Returns:
            List[Tuple[str, Path]]: List of tuples with (video_id, file_path)
        """
        # Get all summary files
        all_summaries = self.get_summary_files()
        
        # Load status to get list of posted videos
        status = self.load_summary_status()
        posted_ids = status.get("posted", [])
        
        # Filter out already posted summaries
        unposted = [(vid_id, path) for vid_id, path in all_summaries if vid_id not in posted_ids]
        
        logger.info(f"Found {len(unposted)} unposted summaries")
        return unposted
    
    def generate_tags_for_summary(self, video: Dict[str, Any], summary_content: Dict[str, Any]) -> List[str]:
        """Generate appropriate tags for a summary based on its content.
        
        Args:
            video (Dict[str, Any]): Original video metadata
            summary_content (Dict[str, Any]): Generated summary content
            
        Returns:
            List[str]: List of 5 relevant tags
        """
        title = summary_content.get("title", video.get("title", "")).lower()
        
        # Default to generic podcast tags
        tags = DEFAULT_TAGS["default"]
        
        # Simple keyword matching for category
        if any(kw in title for kw in ["ai", "artificial intelligence", "machine learning"]):
            tags = DEFAULT_TAGS["ai"]
        elif any(kw in title for kw in ["tech", "technology", "digital", "software"]):
            tags = DEFAULT_TAGS["tech"]
        elif any(kw in title for kw in ["health", "medical", "wellness", "fitness"]):
            tags = DEFAULT_TAGS["health"]
        elif any(kw in title for kw in ["business", "entrepreneur", "startup", "company"]):
            tags = DEFAULT_TAGS["business"]
        elif any(kw in title for kw in ["mindful", "meditation", "spiritual", "consciousness"]):
            tags = DEFAULT_TAGS["mindfulness"]
        elif any(kw in title for kw in ["finance", "money", "invest", "market", "stock"]):
            tags = DEFAULT_TAGS["finance"]
            
        logger.info(f"Generated {len(tags)} tags for video: {video.get('id')}")
        return tags
    
    def process_batch(self, batch: List[Dict[str, Any]], writing_style: str = None) -> Tuple[List[str], List[str]]:
        """Process a batch of videos using the summarize_batch function.
        
        Args:
            batch (List[Dict[str, Any]]): The batch of videos to process
            writing_style (str, optional): Writing style to use
            
        Returns:
            Tuple[List[str], List[str]]: Lists of successful and failed video IDs
        """
        logger.info(f"Processing batch of {len(batch)} videos")
        
        # Mark these videos as being batched
        self.mark_as_batched(batch)
        
        # Check if improved_summarize_two_video_batch module is available
        try:
            # Try to import the module dynamically
            if not hasattr(sys.modules, 'improved_summarize_two_video_batch'):
                module_path = 'improved_summarize_two_video_batch.py'
                spec = importlib.util.spec_from_file_location('improved_summarize_two_video_batch', module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                sys.modules['improved_summarize_two_video_batch'] = module
            else:
                module = sys.modules['improved_summarize_two_video_batch']
            
            # Use the module's summarize_batch function
            logger.info(f"Calling summarize_batch with writing_style={writing_style}")
            results = module.summarize_batch(batch, writing_style=writing_style)
            
            # Process results
            success_ids = []
            failed_ids = []
            
            if len(results) == len(batch):
                for i, (video, summary) in enumerate(zip(batch, results)):
                    video_id = video["id"]
                    
                    # Validate summary
                    errors = module.validate_summary(summary, video_id)
                    
                    if not errors:
                        # Generate tags for the summary based on content
                        summary_tags = self.generate_tags_for_summary(video, summary)
                        summary["tags"] = summary_tags
                        
                        # Save valid summary
                        filename = f"{video_id}_{''.join(c if c.isalnum() else '_' for c in video.get('channel', 'unknown'))}.json"
                        path = SUMMARIES_DIR / filename
                        
                        try:
                            # Ensure YouTube link is in the summary
                            if "video_url" not in summary:
                                summary["video_url"] = f"https://www.youtube.com/watch?v={video_id}"
                                
                            path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
                            logger.info(f"Summary saved to {path}")
                            success_ids.append(video_id)
                        except Exception as e:
                            logger.error(f"Failed to save summary for {video_id}: {e}")
                            failed_ids.append(video_id)
                    else:
                        # Try to fix invalid summary
                        logger.warning(f"Validation failed for {video_id}, attempting to fix")
                        fixed = module.call_claude_fix(json.dumps(summary))
                        
                        if fixed and not module.validate_summary(fixed, video_id):
                            # Generate tags for the fixed summary
                            summary_tags = self.generate_tags_for_summary(video, fixed)
                            fixed["tags"] = summary_tags
                            
                            # Ensure YouTube link is preserved in fixed summary
                            if "video_url" not in fixed:
                                fixed["video_url"] = f"https://www.youtube.com/watch?v={video_id}"
                                
                            # Save fixed summary
                            filename = f"{video_id}_{''.join(c if c.isalnum() else '_' for c in video.get('channel', 'unknown'))}.json"
                            path = SUMMARIES_DIR / filename
                            
                            try:
                                path.write_text(json.dumps(fixed, indent=2), encoding="utf-8")
                                logger.info(f"Fixed summary saved to {path}")
                                success_ids.append(video_id)
                            except Exception as e:
                                logger.error(f"Failed to save fixed summary for {video_id}: {e}")
                                failed_ids.append(video_id)
                        else:
                            # Could not fix
                            logger.error(f"Could not fix summary for {video_id}")
                            failed_ids.append(video_id)
            else:
                logger.error(f"Expected {len(batch)} results but got {len(results)}")
                # All videos in this batch are considered failed
                failed_ids = [video["id"] for video in batch]
            
            # Update status
            if success_ids:
                self.mark_as_completed(success_ids)
            
            if failed_ids:
                self.mark_as_failed(failed_ids)
            
            # Remove all processed videos from queue
            self.remove_from_queue(success_ids + failed_ids)
            
            return success_ids, failed_ids
        
        except ImportError:
            logger.error("Failed to import improved_summarize_two_video_batch module")
            # Mark all as failed
            failed_ids = [video["id"] for video in batch]
            self.mark_as_failed(failed_ids)
            return [], failed_ids
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Mark all as failed
            failed_ids = [video["id"] for video in batch]
            self.mark_as_failed(failed_ids)
            return [], failed_ids


# Example usage
if __name__ == "__main__":
    # Create queue manager
    queue_manager = SummaryQueue()
    
    # Update pending items
    pending_count = queue_manager.update_pending_items()
    print(f"Found {pending_count} pending items")
    
    # Check if ready for batch
    if queue_manager.ready_for_batch():
        print("Ready for batch processing")
        
        # Get next batch
        batch = queue_manager.get_next_batch()
        print(f"Got batch of {len(batch)} videos")
        
        # Process batch
        success_ids, failed_ids = queue_manager.process_batch(batch)
        print(f"Processed batch: {len(success_ids)} successful, {len(failed_ids)} failed")
    else:
        print("Not enough videos for batch processing")
    
    # Get unposted summaries
    unposted = queue_manager.get_unposted_summaries()
    print(f"Found {len(unposted)} unposted summaries")