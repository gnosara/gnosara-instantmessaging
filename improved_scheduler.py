#!/usr/bin/env python3

import os
import json
import csv
import sys
import argparse
import datetime
import time
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

import googleapiclient.discovery
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv

from improved_summarize_two_video_batch import (
    summarize_batch, 
    call_claude_fix, 
    validate_summary, 
    process_single_video
)

# === CONFIG ===
MIN_DURATION = 5 * 60  # Minimum video duration in seconds
MAX_RETRIES = 3  # Maximum number of retries for API calls
BATCH_SIZE = 2  # Number of videos to process in a batch
BATCH_DELAY = 65  # Delay between batches in seconds

# Playlists to monitor
PLAYLISTS = {
    "Playlist A": "PLOGi5-fAu8bGACL3TvvVRCdqCMF4-GwSy",
    "Playlist B": "PLOGi5-fAu8bH_aqRjkNHe_m6zVARmkynC",
    "Playlist E": "PLOGi5-fAu8bHAZDlFuohcjjOU7DlJ8bEZ",
    "Playlist F": "PLOGi5-fAu8bFlc82P2cNj8hjY3MNZaQUw"
}

# Directory structure
SUMMARIES_DIR = Path("summaries")
LOGS_DIR = Path("logs")
SALVAGE_DIR = Path("salvage")
LAST_SEEN_FILE = Path("last_seen.json")
DAILY_LOG_FILE = LOGS_DIR / "daily_summary.csv"
PROCESSING_QUEUE_FILE = Path("processing_queue.json")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scheduler")

def setup_directories():
    """Ensure all required directories exist."""
    for d in [SUMMARIES_DIR, LOGS_DIR, SALVAGE_DIR, LOGS_DIR / "raw_responses", LOGS_DIR / "fixed_json"]:
        d.mkdir(exist_ok=True, parents=True)
    logger.info("Directory structure verified")

def load_youtube_api():
    """Initialize and return the YouTube API client."""
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error("YOUTUBE_API_KEY not found in environment variables")
        raise ValueError("YOUTUBE_API_KEY not found in environment variables")
        
    try:
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
        logger.info("YouTube API client initialized")
        return youtube
    except Exception as e:
        logger.error(f"Failed to initialize YouTube API client: {e}")
        raise

def load_last_seen():
    """Load the record of last seen videos for each playlist."""
    if not LAST_SEEN_FILE.exists():
        logger.info("Last seen file not found, creating new one")
        LAST_SEEN_FILE.write_text("{}", encoding="utf-8")
    try:
        data = json.loads(LAST_SEEN_FILE.read_text(encoding="utf-8"))
        logger.info(f"Loaded last seen data for {len(data)} playlists")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing last seen file: {e}")
        return {}

def save_last_seen(last_seen: dict):
    """Save the record of last seen videos."""
    try:
        LAST_SEEN_FILE.write_text(json.dumps(last_seen, indent=2), encoding="utf-8")
        logger.info("Updated last seen data saved")
    except Exception as e:
        logger.error(f"Failed to save last seen data: {e}")

def load_processing_queue():
    """Load the queue of videos waiting to be processed."""
    if not PROCESSING_QUEUE_FILE.exists():
        logger.info("Processing queue file not found, creating new one")
        PROCESSING_QUEUE_FILE.write_text("[]", encoding="utf-8")
    try:
        data = json.loads(PROCESSING_QUEUE_FILE.read_text(encoding="utf-8"))
        logger.info(f"Loaded processing queue with {len(data)} videos")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing processing queue file: {e}")
        return []

def save_processing_queue(queue: list):
    """Save the queue of videos waiting to be processed."""
    try:
        PROCESSING_QUEUE_FILE.write_text(json.dumps(queue, indent=2), encoding="utf-8")
        logger.info(f"Updated processing queue saved with {len(queue)} videos")
    except Exception as e:
        logger.error(f"Failed to save processing queue: {e}")

def get_latest_video(youtube, playlist_id: str, retry_count=0):
    """Get the latest video from a playlist with retry logic."""
    logger.info(f"Fetching latest video from playlist {playlist_id}")
    
    try:
        items = youtube.playlistItems().list(
            part="snippet,contentDetails", 
            playlistId=playlist_id, 
            maxResults=1
        ).execute().get("items")
        
        if not items:
            logger.info(f"No videos found in playlist {playlist_id}")
            return None

        video_id = items[0]["contentDetails"]["videoId"]
        
        # Get video details
        snippet = youtube.videos().list(
            part="contentDetails,snippet", 
            id=video_id
        ).execute().get("items")[0]
        
        duration = snippet["contentDetails"]["duration"]
        dur = parse_duration(duration)
        
        if dur < MIN_DURATION:
            logger.info(f"Video {video_id} is too short ({dur}s), skipping")
            return None

        video_info = {
            "id": video_id,
            "title": snippet["snippet"]["title"],
            "channel": snippet["snippet"]["channelTitle"],
            "published_at": items[0]["snippet"]["publishedAt"],
            "duration": dur
        }
        
        logger.info(f"Found video: {video_info['title']} ({video_info['id']})")
        return video_info
        
    except Exception as e:
        logger.error(f"Error fetching latest video: {e}")
        if retry_count < MAX_RETRIES - 1:
            retry_count += 1
            logger.info(f"Retrying ({retry_count}/{MAX_RETRIES})...")
            time.sleep(2)  # Short delay before retry
            return get_latest_video(youtube, playlist_id, retry_count)
        return None

def parse_duration(duration: str) -> int:
    """Parse ISO 8601 duration format to seconds."""
    import re
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        logger.warning(f"Could not parse duration: {duration}")
        return 0
    h, m, s = (int(x) if x else 0 for x in match.groups())
    total_seconds = h * 3600 + m * 60 + s
    return total_seconds

def fetch_transcript(video_id: str, retry_count=0):
    """Fetch transcript for a video with retry logic."""
    logger.info(f"Fetching transcript for video {video_id}")
    
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        logger.info(f"Successfully fetched transcript for {video_id}")
        return transcript
    except Exception as e:
        logger.warning(f"Transcript error for {video_id}: {e}")
        if retry_count < MAX_RETRIES - 1:
            retry_count += 1
            logger.info(f"Retrying transcript fetch ({retry_count}/{MAX_RETRIES})...")
            time.sleep(2)  # Short delay before retry
            return fetch_transcript(video_id, retry_count)
        return []

def format_transcript(transcript: list, meta: dict):
    """Format transcript and metadata for processing."""
    if not transcript:
        logger.warning(f"Empty transcript for {meta['id']}")
        return None
        
    formatted = {
        "id": meta["id"],
        "title": meta["title"],
        "channel": meta["channel"],
        "podcaster": meta["channel"],  # For compatibility with summarizer
        "content_type": "podcast",
        "text": " ".join(t["text"] for t in transcript),
        "transcript": " ".join(t["text"] for t in transcript),  # For compatibility with summarizer
        "metadata": {
            "source": "youtube",
            "duration_seconds": meta["duration"],
            "published_at": meta["published_at"]
        }
    }
    
    logger.info(f"Formatted transcript for {meta['id']} ({len(formatted['text'])} chars)")
    return formatted

def save_summary(summary, meta):
    """Save a summary to the summaries directory."""
    filename = f"{meta['id']}_{''.join(c if c.isalnum() else '_' for c in meta['channel'])}.json"
    path = SUMMARIES_DIR / filename
    
    try:
        path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logger.info(f"Summary saved to {path}")
        return str(path)
    except Exception as e:
        logger.error(f"Failed to save summary: {e}")
        return ""

def log_result(meta, success: bool, filepath: str = "", error: str = ""):
    """Log processing results to CSV file."""
    is_new = not DAILY_LOG_FILE.exists()
    
    try:
        with DAILY_LOG_FILE.open('a', newline='') as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow(["timestamp", "video_id", "channel", "title", "success", "filepath", "error"])
            writer.writerow([
                datetime.datetime.now().isoformat(),
                meta["id"], meta["channel"], meta["title"],
                success, filepath, error
            ])
        logger.info(f"Logged result for {meta['id']}: success={success}")
    except Exception as e:
        logger.error(f"Failed to log result: {e}")

def process_playlist(youtube, name, pid, last_seen, force=False):
    """Check a playlist for new videos and add them to the processing queue."""
    logger.info(f"Checking playlist: {name} ({pid})")
    
    # Get the latest video
    video = get_latest_video(youtube, pid)
    if not video:
        logger.info(f"No suitable video found in playlist {name}")
        return False

    # Check if we've already seen this video
    if not force and video["id"] == last_seen.get(pid):
        logger.info(f"No new video in playlist {name}")
        return False

    logger.info(f"New video found in {name}: {video['title']} ({video['id']})")
    
    # Fetch transcript
    transcript = fetch_transcript(video["id"])
    if not transcript:
        logger.warning(f"Could not fetch transcript for {video['id']}")
        log_result(video, False, error="Transcript fetch failed")
        return False

    # Format for processing
    formatted = format_transcript(transcript, video)
    if not formatted:
        logger.warning(f"Could not format transcript for {video['id']}")
        log_result(video, False, error="Transcript formatting failed")
        return False
        
    # Add to processing queue
    queue = load_processing_queue()
    
    # Check if video is already in queue
    if any(item["id"] == video["id"] for item in queue):
        logger.info(f"Video {video['id']} is already in processing queue")
        return False
        
    queue.append(formatted)
    save_processing_queue(queue)
    
    # Update last seen
    last_seen[pid] = video["id"]
    save_last_seen(last_seen)
    
    logger.info(f"Added {video['id']} to processing queue")
    return True

def process_queue(writing_style=None):
    """
    Process videos in the queue in batches, but only if there are at least 2 videos.
    
    Args:
        writing_style (str, optional): Writing style to use. Defaults to None.
    """
    queue = load_processing_queue()
    if not queue:
        logger.info("Processing queue is empty")
        return
        
    logger.info(f"Processing queue has {len(queue)} videos")
    
    # Only process if we have at least 2 videos (for batch efficiency)
    if len(queue) < 2:
        logger.info(f"Only {len(queue)} video(s). Waiting for more to batch.")
        return  # Exit without processing any videos
    
    logger.info(f"Enough videos found ({len(queue)}). Starting batch summarization.")
    
    # Process in batches of BATCH_SIZE
    processed_ids = []
    
    for i in range(0, len(queue), BATCH_SIZE):
        batch = queue[i:i+BATCH_SIZE]
        
        # Skip processing if this batch is smaller than BATCH_SIZE
        # (except for the first batch which was already verified to be at least BATCH_SIZE)
        if len(batch) < BATCH_SIZE and i > 0:
            logger.info(f"Remaining batch size ({len(batch)}) is smaller than minimum batch size ({BATCH_SIZE}). Waiting for more videos.")
            break  # Stop processing and keep remaining videos in queue
        
        # Process batch
        logger.info(f"Processing batch of {len(batch)} videos")
        results = summarize_batch(batch, writing_style=writing_style)
        
        # Match results to videos
        if len(results) == len(batch):
            for video, summary in zip(batch, results):
                # Validate summary
                errors = validate_summary(summary, video["id"])
                
                if not errors:
                    # Save valid summary
                    path = save_summary(summary, video)
                    log_result(video, True, path)
                    processed_ids.append(video["id"])
                else:
                    # Try to fix invalid summary
                    logger.warning(f"Validation failed for {video['id']}, attempting to fix")
                    fixed = call_claude_fix(json.dumps(summary))
                    
                    if fixed and not validate_summary(fixed, video["id"]):
                        # Ensure YouTube link is preserved in fixed summary
                        if "video_url" not in fixed:
                            fixed["video_url"] = f"https://www.youtube.com/watch?v={video['id']}"
                            
                        # Save fixed summary
                        path = save_summary(fixed, video)
                        log_result(video, True, path)
                        processed_ids.append(video["id"])
                    else:
                        # Log failure
                        logger.error(f"Could not fix summary for {video['id']}")
                        salvage_path = SALVAGE_DIR / f"{video['id']}_raw.json"
                        salvage_path.write_text(json.dumps(summary, indent=2))
                        log_result(video, False, error="Validation failed and could not fix")
        else:
            logger.error(f"Expected {len(batch)} results but got {len(results)}")
            
            # Try to match results to videos by title or content
            for video in batch:
                matched = False
                for summary in results:
                    if (video["title"].lower() in summary.get("title", "").lower() or 
                        summary.get("title", "").lower() in video["title"].lower()):
                        
                        # Ensure YouTube link is in the summary
                        if "video_url" not in summary:
                            summary["video_url"] = f"https://www.youtube.com/watch?v={video['id']}"
                        
                        # Validate summary
                        errors = validate_summary(summary, video["id"])
                        
                        if not errors:
                            # Save valid summary
                            path = save_summary(summary, video)
                            log_result(video, True, path)
                            processed_ids.append(video["id"])
                            matched = True
                            break
                
                if not matched:
                    # Log failure - we won't process individually to maintain batch efficiency
                    logger.error(f"Could not match video {video['id']} to any summary")
                    log_result(video, False, error="No matching summary found")
        
        # Wait between batches
        if i + BATCH_SIZE < len(queue):
            logger.info(f"Waiting {BATCH_DELAY} seconds before next batch...")
            time.sleep(BATCH_DELAY)
    
    # Remove processed videos from queue
    new_queue = [item for item in queue if item["id"] not in processed_ids]
    save_processing_queue(new_queue)
    
    logger.info(f"Processed {len(processed_ids)} videos, {len(new_queue)} remaining in queue")

def main():
    """Main function to run the scheduler."""
    parser = argparse.ArgumentParser(description="YouTube Podcast Summarizer")
    parser.add_argument("--force", action="store_true", help="Force processing of all videos")
    parser.add_argument("--max", type=int, default=None, help="Maximum number of playlists to check")
    parser.add_argument("--process-only", action="store_true", help="Only process queue, don't check playlists")
    parser.add_argument("--style", type=str, default=None, help="Writing style to use (e.g., casual, professional, gnosara)")
    parser.add_argument("--sample", type=str, default=None, help="Path to a custom writing sample file")
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    if not os.getenv("YOUTUBE_API_KEY") or not os.getenv("CLAUDE_API_KEY"):
        logger.error("Missing API keys in .env")
        print("❌ Missing API keys in .env")
        return

    # Setup
    setup_directories()
    
    # Handle custom writing sample if provided
    writing_style = args.style
    if args.sample:
        try:
            from writing_samples import load_custom_sample
            if load_custom_sample(args.sample):
                logger.info(f"Loaded custom writing sample from {args.sample}")
                writing_style = "gnosara"  # Use the custom sample
            else:
                logger.error(f"Could not load custom writing sample from {args.sample}")
        except ImportError:
            logger.warning("writing_samples module not found, continuing with default style")
    
    # Process existing queue
    if args.process_only:
        logger.info("Processing queue only")
        process_queue(writing_style=writing_style)
        return
    
    # Check playlists for new videos
    last_seen = load_last_seen()
    youtube = load_youtube_api()

    count = 0
    for name, pid in PLAYLISTS.items():
        if args.max and count >= args.max:
            logger.info(f"Reached maximum number of playlists ({args.max})")
            break
            
        if process_playlist(youtube, name, pid, last_seen, args.force):
            count += 1

    logger.info(f"Added {count} new videos to processing queue")
    
    # Process queue
    process_queue(writing_style=writing_style)
    
    logger.info("Scheduler run completed")

if __name__ == "__main__":
    main()
