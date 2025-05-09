#!/usr/bin/env python3

"""
Modified post_scheduler.py with Telegram integration
Includes fixes to ensure all platforms receive posts properly
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import importlib.util
import sys

# Import our modules
from socialbu_api import SocialBuAPI
from post_formatter import PostFormatter
from summary_queue import SummaryQueue

# Import Telegram modules
from telegram_api import TelegramAPI
from telegram_formatter import TelegramFormatter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/post_scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("post_scheduler")

# Create Telegram-specific logger
telegram_logger = logging.getLogger("telegram_integration")
telegram_logger.setLevel(logging.INFO)
telegram_handler = logging.FileHandler("logs/telegram_integration.log")
telegram_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
telegram_logger.addHandler(telegram_handler)
telegram_logger.addHandler(logging.StreamHandler())

# Constants
DAILY_LOG_FILE = Path("logs/daily_log.json")
MIN_POSTING_INTERVAL = 20 * 60  # 20 minutes in seconds
PLATFORMS = ["twitter", "facebook", "telegram"]  # Added telegram to platforms
MAX_RETRIES = 3  # Number of retries for failed posts


class PostScheduler:
    """Main controller for the Gnosara posting system."""
    
    def __init__(self, dry_run: bool = False):
        """Initialize the post scheduler.
        
        Args:
            dry_run (bool, optional): If True, don't actually post to social media
        """
        # Ensure directories exist
        Path("logs").mkdir(exist_ok=True)
        
        # Load environment variables
        self._load_env_vars()
        
        # Store dry run setting
        self.dry_run = dry_run
        
        # Initialize components
        self.api = SocialBuAPI(dry_run=dry_run)
        self.formatter = PostFormatter()
        self.queue_manager = SummaryQueue()
        
        # Initialize Telegram components
        self.telegram_api = TelegramAPI(dry_run=dry_run)
        self.telegram_formatter = TelegramFormatter()
        
        # Create daily log file if it doesn't exist
        self._init_daily_log()
        
        if dry_run:
            logger.info("Post scheduler initialized in DRY RUN mode - no posts will be made")
            telegram_logger.info("Telegram integration initialized in DRY RUN mode")
        else:
            logger.info("Post scheduler initialized")
            telegram_logger.info("Telegram integration initialized")
    
    def _load_env_vars(self):
        """Load environment variables."""
        try:
            # Try to import dotenv
            from dotenv import load_dotenv
            load_dotenv()
            logger.info("Loaded environment variables from .env file")
        except ImportError:
            logger.warning("python-dotenv not installed, skipping .env loading")
        
        # Check for required environment variables
        required_vars = [
            "SOCIALBU_API_KEY", "SOCIALBU_EMAIL", "SOCIALBU_PASSWORD", 
            "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_IDS"  # Added Telegram vars
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    def _init_daily_log(self):
        """Initialize the daily log file with default structure."""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Check if we need to create a new daily log
        if DAILY_LOG_FILE.exists():
            try:
                data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
                if data.get("date") == today:
                    # Today's log already exists
                    return
            except json.JSONDecodeError:
                # File exists but is not valid JSON, will be overwritten
                pass
        
        # Create new daily log
        log_data = {
            "date": today,
            "summarized": [],
            "posted": {},
            "errors": [],
            "pending": []
        }
        
        # Create log file
        try:
            DAILY_LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
            logger.info(f"Created new daily log for {today}")
        except Exception as e:
            logger.error(f"Failed to create daily log: {e}")
    
    def _update_daily_log(self, key: str, items: List[Any]) -> None:
        """Update the daily log with new items.
        
        Args:
            key (str): The log section to update ('summarized', 'posted', 'errors', 'pending')
            items (List[Any]): The items to add
        """
        try:
            # Load current log
            log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            
            # Check if we need to create a new daily log
            today = datetime.now().strftime("%Y-%m-%d")
            if log_data.get("date") != today:
                self._init_daily_log()
                log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            
            # Special handling for 'posted' which is a dict by platform
            if key == "posted":
                for platform, ids in items:
                    if platform not in log_data["posted"]:
                        log_data["posted"][platform] = []
                    log_data["posted"][platform].extend(ids)
                    # Remove duplicates
                    log_data["posted"][platform] = list(set(log_data["posted"][platform]))
            else:
                # Regular list updates
                log_data[key].extend(items)
                # Remove duplicates
                log_data[key] = list(set(log_data[key]))
            
            # Save updated log
            DAILY_LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
            logger.info(f"Updated daily log: added {len(items)} items to {key}")
            
        except Exception as e:
            logger.error(f"Failed to update daily log: {e}")
    
    def _log_error(self, message: str) -> None:
        """Log an error to the daily log file.
        
        Args:
            message (str): The error message to log
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error = f"{timestamp} - {message}"
        self._update_daily_log("errors", [error])
    
    def _get_posted_videos(self) -> Dict[str, List[str]]:
        """Get dictionary of videos posted to each platform.
        
        Returns:
            Dict[str, List[str]]: Dictionary mapping platforms to lists of posted video IDs
        """
        try:
            log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            return log_data.get("posted", {})
        except Exception as e:
            logger.error(f"Failed to get posted videos: {e}")
            return {}
    
    def check_and_process_queue(self, writing_style: str = None) -> bool:
        """Check if enough videos are in the queue and process them if so.
        
        Args:
            writing_style (str, optional): Writing style to use for summaries
            
        Returns:
            bool: True if batch was processed, False otherwise
        """
        logger.info("Checking processing queue")
        
        # Update pending items
        self.queue_manager.update_pending_items()
        
        # Check if we have enough videos for a batch
        if not self.queue_manager.ready_for_batch():
            logger.info("Not enough videos in queue for batch processing")
            return False
        
        # Get next batch
        batch = self.queue_manager.get_next_batch()
        if not batch:
            logger.warning("Failed to get next batch")
            return False
        
        # Process the batch
        success_ids, failed_ids = self.queue_manager.process_batch(batch, writing_style)
        
        # Update daily log
        if success_ids:
            self._update_daily_log("summarized", success_ids)
        
        if failed_ids:
            for vid_id in failed_ids:
                self._log_error(f"Failed to summarize video: {vid_id}")
        
        logger.info(f"Batch processing complete: {len(success_ids)} successful, {len(failed_ids)} failed")
        return True
    
    def _should_post_to_platform(self, video_id: str, platform: str, force_all: bool = False) -> bool:
        """Check if a video should be posted to a specific platform.
        
        Args:
            video_id (str): The video ID
            platform (str): The platform to check
            force_all (bool, optional): If True, ignore previous posting status
            
        Returns:
            bool: True if the video should be posted to the platform, False otherwise
        """
        # If force_all is True, always return True
        if force_all:
            return True
            
        # Get dictionary of videos already posted to each platform
        posted = self._get_posted_videos()
        
        # Check if this video has already been posted to this platform
        platform_posts = posted.get(platform, [])
        return video_id not in platform_posts
    
    def post_unposted_summaries(self, force_all: bool = False) -> Dict[str, List[str]]:
        """Post unposted summaries to social media platforms.
        
        Args:
            force_all (bool, optional): If True, force delivery of all summaries regardless 
                                        of previous posting status
                                        
        Returns:
            Dict[str, List[str]]: Dictionary mapping platforms to lists of posted video IDs
        """
        if force_all:
            logger.info("Force-delivering all summaries, ignoring previous posting status")
            telegram_logger.info("Force-delivering all summaries to Telegram")
        else:
            logger.info("Checking for unposted summaries")
        
        # Get unposted summaries
        unposted = self.queue_manager.get_unposted_summaries()
        if not unposted:
            logger.info("No unposted summaries found")
            return {}
        
        # Authenticate with SocialBu (for Twitter & Facebook)
        socialbu_authenticated = self.api.authenticate()
        if not socialbu_authenticated:
            logger.error("Failed to authenticate with SocialBu API")
            self._log_error("Failed to authenticate with SocialBu API")
            # We'll continue for Telegram even if SocialBu fails
        
        # Check Telegram bot status
        telegram_operational = self.telegram_api.check_bot_status()
        if not telegram_operational:
            telegram_logger.error("Telegram bot is not operational")
            self._log_error("Telegram bot is not operational")
            # We'll continue for SocialBu even if Telegram fails
        
        # Track posted summaries by platform
        posted_by_platform = {platform: [] for platform in PLATFORMS}
        
        # For tracking videos that need to be marked as fully posted
        # A video is fully posted when it has been posted to all required platforms
        # or when it has been attempted to be posted to all platforms
        attempts_by_video = {}
        
        # Post each summary to each platform
        for video_id, file_path in unposted:
            attempts_by_video[video_id] = set()
            
            try:
                # Load summary
                summary_data = json.loads(file_path.read_text(encoding="utf-8"))
                
                # Post to each platform
                for platform in PLATFORMS:
                    # Skip if already posted to this platform (unless force_all is True)
                    if not self._should_post_to_platform(video_id, platform, force_all):
                        logger.info(f"Skipping {video_id} for {platform} - already posted")
                        # Consider this platform as attempted
                        attempts_by_video[video_id].add(platform)
                        continue
                    
                    # Record that we've attempted this platform
                    attempts_by_video[video_id].add(platform)
                        
                    if platform in ["twitter", "facebook"]:
                        # Skip if SocialBu auth failed
                        if not socialbu_authenticated:
                            logger.warning(f"Skipping {platform} posting due to authentication failure")
                            continue
                            
                        # Get account IDs for this platform
                        account_ids = self.api.get_account_ids_by_platform(platform)
                        if not account_ids:
                            logger.warning(f"No accounts found for platform: {platform}")
                            continue
                        
                        # Format content for this platform
                        content = self.formatter.format_summary(summary_data, platform, file_path.name)
                        if not content:
                            logger.error(f"Failed to format content for {video_id} on {platform}")
                            self._log_error(f"Failed to format content for {video_id} on {platform}")
                            continue
                        
                        # Post to platform via SocialBu
                        response = self.api.create_post(content, account_ids, platform)
                        if response:
                            logger.info(f"Successfully posted {video_id} to {platform}")
                            posted_by_platform[platform].append(video_id)
                        else:
                            logger.error(f"Failed to post {video_id} to {platform}")
                            self._log_error(f"Failed to post {video_id} to {platform}")
                    
                    elif platform == "telegram":
                        # Skip if Telegram bot is not operational
                        if not telegram_operational:
                            telegram_logger.warning(f"Skipping Telegram posting due to bot being non-operational")
                            continue
                            
                        # Format content for Telegram
                        content = self.telegram_formatter.format_summary(summary_data, file_path.name)
                        if not content:
                            telegram_logger.error(f"Failed to format content for {video_id} on Telegram")
                            self._log_error(f"Failed to format content for {video_id} on Telegram")
                            continue
                        
                        # Broadcast to all configured Telegram chats
                        telegram_logger.info(f"Sending {video_id} to Telegram channels")
                        response = self.telegram_api.broadcast_message(content)
                        
                        # Check if any broadcasts were successful
                        success_count = sum(1 for result in response.get("results", []) if result.get("success", False))
                        total_count = len(response.get("results", []))
                        
                        if success_count > 0:
                            telegram_logger.info(f"Successfully posted {video_id} to {success_count}/{total_count} Telegram channels")
                            posted_by_platform[platform].append(video_id)
                            # Update also in daily log
                            self._update_daily_log("posted", [(platform, [video_id])])
                        else:
                            telegram_logger.error(f"Failed to post {video_id} to any Telegram channels")
                            self._log_error(f"Failed to post {video_id} to Telegram")
                    
                    # Wait between platforms to avoid rate limiting
                    time.sleep(2)
                
                # Wait between summaries
                if len(unposted) > 1:
                    time.sleep(MIN_POSTING_INTERVAL / len(unposted))
                
            except Exception as e:
                logger.error(f"Error posting summary {video_id}: {e}")
                self._log_error(f"Error posting summary {video_id}: {str(e)}")
        
        # Update the queue manager based on our posting results
        self._update_queue_manager_status(posted_by_platform, attempts_by_video)
        
        # Update daily log
        posted_items = [(platform, ids) for platform, ids in posted_by_platform.items() if ids]
        if posted_items:
            self._update_daily_log("posted", posted_items)
        
        # Logout from SocialBu
        if socialbu_authenticated:
            self.api.logout()
        
        logger.info(f"Posting complete: {sum(len(ids) for ids in posted_by_platform.values())} summaries posted")
        return posted_by_platform
    
    def _update_queue_manager_status(self, posted_by_platform: Dict[str, List[str]], 
                                    attempts_by_video: Dict[str, Set[str]]) -> None:
        """Update queue manager status based on posting results.
        
        Args:
            posted_by_platform (Dict[str, List[str]]): Dictionary mapping platforms to posted video IDs
            attempts_by_video (Dict[str, Set[str]]): Dictionary mapping video IDs to sets of attempted platforms
        """
        # Load existing seen videos to update
        seen_videos = self.queue_manager.load_seen_videos()
        
        # Update seen videos with posting info
        for video_id, attempted_platforms in attempts_by_video.items():
            # Get platforms this video was successfully posted to
            successful_platforms = [platform for platform, ids in posted_by_platform.items() 
                                 if video_id in ids]
            
            # Update seen videos entry if it exists
            if video_id in seen_videos:
                # Ensure the video has a "posted_to" field that's a list
                if "posted_to" not in seen_videos[video_id]:
                    seen_videos[video_id]["posted_to"] = []
                
                # Add successful platforms
                seen_videos[video_id]["posted_to"].extend(successful_platforms)
                
                # Remove duplicates
                seen_videos[video_id]["posted_to"] = list(set(seen_videos[video_id]["posted_to"]))
                
                # Update status to "posted" if posted to at least one platform
                if successful_platforms:
                    seen_videos[video_id]["status"] = "posted"
                    seen_videos[video_id]["posted_at"] = datetime.now().isoformat()
        
        # Save the updated seen videos
        self.queue_manager.save_seen_videos(seen_videos)
        
        # Determine which videos should be marked as completed in the queue manager
        # A video is considered fully processed if:
        # 1. It was successfully posted to at least one platform, AND
        # 2. It was attempted to be posted to all enabled platforms
        fully_processed = []
        for video_id, attempted_platforms in attempts_by_video.items():
            # Check if all required platforms were attempted
            all_platforms_attempted = all(platform in attempted_platforms for platform in PLATFORMS)
            
            # Check if at least one platform was successful
            any_platform_success = any(video_id in ids for ids in posted_by_platform.values())
            
            if all_platforms_attempted and any_platform_success:
                fully_processed.append(video_id)
        
        # Mark fully processed videos as posted in the queue manager
        if fully_processed:
            logger.info(f"Marking {len(fully_processed)} videos as fully processed")
            self.queue_manager.mark_as_posted(fully_processed)
    
    def run_full_cycle(self, writing_style: str = None, force_all: bool = False) -> Dict[str, Any]:
        """Run a full processing and posting cycle.
        
        Args:
            writing_style (str, optional): Writing style to use for summaries
            force_all (bool, optional): If True, force delivery of all summaries
            
        Returns:
            Dict[str, Any]: Results of the processing cycle
        """
        logger.info("Starting full processing cycle")
        results = {
            "timestamp": datetime.now().isoformat(),
            "summarized": [],
            "posted": {},
            "errors": []
        }
        
        # Process queue if needed
        processed = self.check_and_process_queue(writing_style)
        
        # Post unposted summaries
        posted = self.post_unposted_summaries(force_all=force_all)
        
        # Collect results
        if processed:
            status = self.queue_manager.load_summary_status()
            results["summarized"] = status.get("completed", [])
        
        results["posted"] = posted
        
        logger.info("Full processing cycle complete")
        return results
    
    def generate_daily_report(self) -> Dict[str, Any]:
        """Generate a report of today's activities.
        
        Returns:
            Dict[str, Any]: Report data
        """
        try:
            # Load daily log
            log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            
            # Calculate some stats
            summarized_count = len(log_data.get("summarized", []))
            
            posted_counts = {}
            for platform, ids in log_data.get("posted", {}).items():
                posted_counts[platform] = len(ids)
            
            total_posted = sum(posted_counts.values())
            error_count = len(log_data.get("errors", []))
            
            # Compile report
            report = {
                "date": log_data.get("date"),
                "stats": {
                    "summarized": summarized_count,
                    "posted": posted_counts,
                    "total_posted": total_posted,
                    "errors": error_count
                },
                "details": log_data
            }
            
            logger.info(f"Generated daily report for {log_data.get('date')}")
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate daily report: {e}")
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "error": f"Failed to generate report: {str(e)}"
            }


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Gnosara Post Scheduler")
    parser.add_argument("--process-only", action="store_true", help="Only process queue, don't post")
    parser.add_argument("--post-only", action="store_true", help="Only post summaries, don't process queue")
    parser.add_argument("--style", type=str, default=None, help="Writing style to use")
    parser.add_argument("--report", action="store_true", help="Generate and print daily report")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry run mode - no actual posts will be made")
    parser.add_argument("--platforms", type=str, default="all", 
                       help="Comma-separated list of platforms to post to (twitter,facebook,telegram)")
    parser.add_argument("--telegram-only", action="store_true", help="Only post to Telegram")
    parser.add_argument("--force-all", action="store_true", help="Force delivery of all summaries, ignoring previous posting status")
    args = parser.parse_args()
    
    # Create scheduler with dry run setting
    scheduler = PostScheduler(dry_run=args.dry_run)
    
    if args.dry_run:
        print("🔍 RUNNING IN DRY RUN MODE - No actual posts will be made")
    
    # Filter platforms if specified
    global PLATFORMS
    if args.telegram_only:
        PLATFORMS = ["telegram"]
        print("Posting to Telegram only")
    elif args.platforms != "all":
        platforms = [p.strip() for p in args.platforms.split(",")]
        PLATFORMS = [p for p in PLATFORMS if p in platforms]
        print(f"Posting to platforms: {', '.join(PLATFORMS)}")
    
    if args.report:
        # Generate daily report
        report = scheduler.generate_daily_report()
        print(json.dumps(report, indent=2))
    elif args.process_only:
        # Only process queue
        scheduler.check_and_process_queue(args.style)
    elif args.post_only:
        # Only post summaries
        scheduler.post_unposted_summaries(force_all=args.force_all)
    else:
        # Run full cycle
        scheduler.run_full_cycle(args.style, force_all=args.force_all)


if __name__ == "__main__":
    main()