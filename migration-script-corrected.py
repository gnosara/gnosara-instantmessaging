#!/usr/bin/env python3

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/migration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("migration")

# Constants
PROCESSING_QUEUE_FILE = Path("processing_queue.json")
BACKUP_QUEUE_FILE = Path("processing_queue.json.backup")
SUMMARY_STATUS_FILE = Path("logs/summary_status.json")
SEEN_VIDEOS_FILE = Path("seen_videos.json")
SUMMARIES_DIR = Path("summaries")

def backup_files():
    """Create backups of important files before migration."""
    logger.info("Creating backups of important files")
    
    # Backup processing queue
    if PROCESSING_QUEUE_FILE.exists():
        with open(PROCESSING_QUEUE_FILE, "r", encoding="utf-8") as f:
            with open(BACKUP_QUEUE_FILE, "w", encoding="utf-8") as f2:
                f2.write(f.read())
        logger.info(f"Backed up processing queue to {BACKUP_QUEUE_FILE}")
    
    # Backup summary status
    if SUMMARY_STATUS_FILE.exists():
        backup_path = SUMMARY_STATUS_FILE.with_suffix(".json.backup")
        with open(SUMMARY_STATUS_FILE, "r", encoding="utf-8") as f:
            with open(backup_path, "w", encoding="utf-8") as f2:
                f2.write(f.read())
        logger.info(f"Backed up summary status to {backup_path}")
        
    logger.info("Backups completed")

def migrate_processing_queue():
    """Migrate processing queue from old format to new format."""
    logger.info("Migrating processing queue")
    
    if not PROCESSING_QUEUE_FILE.exists():
        logger.warning(f"Processing queue file not found: {PROCESSING_QUEUE_FILE}")
        # Create empty queue in new format
        new_queue = {"pending": []}
        with open(PROCESSING_QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(new_queue, f, indent=2)
        logger.info("Created new empty processing queue")
        return
    
    try:
        # Load existing queue
        with open(PROCESSING_QUEUE_FILE, "r", encoding="utf-8") as f:
            old_queue = json.load(f)
        
        # Check if it's already in the new format
        if isinstance(old_queue, dict) and "pending" in old_queue:
            logger.info("Processing queue already in new format, no migration needed")
            return
        
        # Convert from old format to new format
        new_queue = {"pending": []}
        
        if isinstance(old_queue, list):
            timestamp = datetime.now().isoformat()
            for item in old_queue:
                if isinstance(item, str):
                    # Convert string ID to object with basic metadata only
                    # Note: No tags at this stage - tags will be added during/after summarization
                    new_item = {
                        "id": item,
                        "title": f"Video {item}",
                        "channel": "Unknown",
                        "found_at": timestamp
                    }
                    new_queue["pending"].append(new_item)
                elif isinstance(item, dict) and "id" in item:
                    # Make sure required fields are present
                    if "title" not in item:
                        item["title"] = f"Video {item['id']}"
                    if "channel" not in item:
                        item["channel"] = "Unknown"
                    if "found_at" not in item:
                        item["found_at"] = timestamp
                    
                    # Explicitly remove tags if present in the queue item
                    # (Tags should not be in the queue, only in summaries and seen_videos)
                    if "tags" in item:
                        del item["tags"]
                    
                    new_queue["pending"].append(item)
                else:
                    logger.warning(f"Skipping unrecognized queue item: {item}")
        
        # Save new queue
        with open(PROCESSING_QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(new_queue, f, indent=2)
        
        logger.info(f"Migrated processing queue: {len(new_queue['pending'])} items")
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing processing queue: {e}")
    except Exception as e:
        logger.error(f"Unexpected error migrating processing queue: {e}")

def create_seen_videos():
    """Create seen videos tracking file based on summary status."""
    logger.info("Creating seen videos tracking file")
    
    if SEEN_VIDEOS_FILE.exists():
        logger.info(f"Seen videos file already exists: {SEEN_VIDEOS_FILE}")
        return
    
    if not SUMMARY_STATUS_FILE.exists():
        logger.warning(f"Summary status file not found: {SUMMARY_STATUS_FILE}")
        # Create empty seen videos file
        seen_videos = {"done": {}}
        with open(SEEN_VIDEOS_FILE, "w", encoding="utf-8") as f:
            json.dump(seen_videos, f, indent=2)
        logger.info("Created new empty seen videos file")
        return
    
    try:
        # Load summary status
        with open(SUMMARY_STATUS_FILE, "r", encoding="utf-8") as f:
            status = json.load(f)
        
        # Create seen videos dictionary
        seen_videos = {"done": {}}
        
        # Process completed videos
        timestamp = datetime.now().isoformat()
        
        # Add completed videos
        for video_id in status.get("completed", []):
            # Try to find title from summary file
            title = f"Video {video_id}"
            channel = "Unknown"
            
            # Look for matching summary file
            summary_files = list(SUMMARIES_DIR.glob(f"{video_id}_*.json"))
            if summary_files:
                try:
                    with open(summary_files[0], "r", encoding="utf-8") as f:
                        summary = json.load(f)
                    title = summary.get("title", title)
                    channel = summary.get("podcaster", channel)
                    
                    # Get tags from summary if present
                    if "tags" in summary and isinstance(summary["tags"], list) and summary["tags"]:
                        seen_videos["done"][video_id] = {
                            "title": title,
                            "channel": channel,
                            "processed_at": timestamp,
                            "status": "completed",
                            "tags": summary["tags"]
                        }
                    else:
                        seen_videos["done"][video_id] = {
                            "title": title,
                            "channel": channel,
                            "processed_at": timestamp,
                            "status": "completed"
                        }
                except Exception as e:
                    logger.warning(f"Error loading summary for {video_id}: {e}")
                    seen_videos["done"][video_id] = {
                        "title": title,
                        "channel": channel,
                        "processed_at": timestamp,
                        "status": "completed"
                    }
            else:
                seen_videos["done"][video_id] = {
                    "title": title,
                    "channel": channel,
                    "processed_at": timestamp,
                    "status": "completed"
                }
        
        # Add failed videos
        for video_id in status.get("failed", []):
            seen_videos["done"][video_id] = {
                "title": f"Video {video_id}",
                "channel": "Unknown",
                "processed_at": timestamp,
                "status": "failed",
                "error": "Unknown error during processing"
            }
        
        # Add posted videos
        for video_id in status.get("posted", []):
            if video_id in seen_videos["done"]:
                seen_videos["done"][video_id]["status"] = "posted"
                seen_videos["done"][video_id]["posted_at"] = timestamp
            else:
                # Try to find summary with tags
                summary_files = list(SUMMARIES_DIR.glob(f"{video_id}_*.json"))
                if summary_files:
                    try:
                        with open(summary_files[0], "r", encoding="utf-8") as f:
                            summary = json.load(f)
                        title = summary.get("title", f"Video {video_id}")
                        channel = summary.get("podcaster", "Unknown")
                        
                        # Include tags if present in summary
                        if "tags" in summary and isinstance(summary["tags"], list) and summary["tags"]:
                            seen_videos["done"][video_id] = {
                                "title": title,
                                "channel": channel,
                                "processed_at": timestamp,
                                "status": "posted",
                                "posted_at": timestamp,
                                "tags": summary["tags"]
                            }
                        else:
                            seen_videos["done"][video_id] = {
                                "title": title,
                                "channel": channel,
                                "processed_at": timestamp,
                                "status": "posted",
                                "posted_at": timestamp
                            }
                    except Exception as e:
                        logger.warning(f"Error loading summary for {video_id}: {e}")
                        seen_videos["done"][video_id] = {
                            "title": f"Video {video_id}",
                            "channel": "Unknown",
                            "processed_at": timestamp,
                            "status": "posted",
                            "posted_at": timestamp
                        }
                else:
                    seen_videos["done"][video_id] = {
                        "title": f"Video {video_id}",
                        "channel": "Unknown",
                        "processed_at": timestamp,
                        "status": "posted",
                        "posted_at": timestamp
                    }
        
        # Save seen videos file
        with open(SEEN_VIDEOS_FILE, "w", encoding="utf-8") as f:
            json.dump(seen_videos, f, indent=2)
        
        logger.info(f"Created seen videos file with {len(seen_videos['done'])} entries")
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing summary status: {e}")
    except Exception as e:
        logger.error(f"Unexpected error creating seen videos: {e}")

def update_summary_files():
    """Update existing summary files to include tags if missing."""
    logger.info("Updating summary files to include tags")
    
    if not SUMMARIES_DIR.exists():
        logger.warning(f"Summaries directory not found: {SUMMARIES_DIR}")
        return
    
    # Get all summary files
    summary_files = list(SUMMARIES_DIR.glob("*.json"))
    logger.info(f"Found {len(summary_files)} summary files")
    
    updated_count = 0
    
    # Default tags for different categories
    default_tags = {
        "ai": ["#AI", "#Technology", "#Innovation", "#Podcast", "#Future"],
        "tech": ["#Technology", "#Innovation", "#Digital", "#Podcast", "#Science"],
        "health": ["#Health", "#Wellness", "#Science", "#Podcast", "#Lifestyle"],
        "business": ["#Business", "#Entrepreneurship", "#Success", "#Podcast", "#Leadership"],
        "mindfulness": ["#Mindfulness", "#Meditation", "#Wellness", "#Podcast", "#Health"],
        "finance": ["#Finance", "#Investing", "#Wealth", "#Podcast", "#Business"],
        "default": ["#Podcast", "#Interview", "#Learning", "#Knowledge", "#Insights"]
    }
    
    # Process each summary file
    for file_path in summary_files:
        try:
            # Load summary
            with open(file_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
            
            # Skip if already has tags
            if "tags" in summary and isinstance(summary["tags"], list) and summary["tags"]:
                continue
            
            # Determine appropriate tags based on title and content
            title = summary.get("title", "").lower()
            tags_to_use = default_tags["default"]
            
            # Simple keyword matching for category
            if any(kw in title for kw in ["ai", "artificial intelligence", "machine learning"]):
                tags_to_use = default_tags["ai"]
            elif any(kw in title for kw in ["tech", "technology", "digital", "software"]):
                tags_to_use = default_tags["tech"]
            elif any(kw in title for kw in ["health", "medical", "wellness", "fitness"]):
                tags_to_use = default_tags["health"]
            elif any(kw in title for kw in ["business", "entrepreneur", "startup", "company"]):
                tags_to_use = default_tags["business"]
            elif any(kw in title for kw in ["mindful", "meditation", "spiritual", "consciousness"]):
                tags_to_use = default_tags["mindfulness"]
            elif any(kw in title for kw in ["finance", "money", "invest", "market", "stock"]):
                tags_to_use = default_tags["finance"]
            
            # Add tags to summary
            summary["tags"] = tags_to_use
            
            # Save updated summary
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
            
            updated_count += 1
            
        except Exception as e:
            logger.error(f"Error updating summary file {file_path}: {e}")
    
    logger.info(f"Updated {updated_count} summary files with tags")

def main():
    """Run the full migration process."""
    logger.info("Starting migration to Gnosara V6 format")
    
    # Ensure directories exist
    Path("logs").mkdir(exist_ok=True)
    
    # Create backups
    backup_files()
    
    # Migrate processing queue
    migrate_processing_queue()
    
    # Create seen videos file
    create_seen_videos()
    
    # Update summary files
    update_summary_files()
    
    logger.info("Migration completed successfully")
    print("✅ Migration to Gnosara V6 format completed successfully!")
    print("  📋 Processing queue migrated to new format")
    print("  📋 Seen videos tracking file created")
    print("  📋 Summary files updated with tags")
    print("  💾 Backups created for important files")

if __name__ == "__main__":
    main()
