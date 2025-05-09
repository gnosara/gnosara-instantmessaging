#!/usr/bin/env python3

import os
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import importlib.util
import sys

# Import our modules
from socialbu_api import SocialBuAPI
from post_formatter import PostFormatter
from summary_queue import SummaryQueue

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

# Constants
DAILY_LOG_FILE = Path("logs/daily_log.json")
MIN_POSTING_INTERVAL = 20 * 60  # 20 minutes in seconds
PLATFORMS = ["twitter", "facebook"]  # Platforms to post to


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
        
        # Create daily log file if it doesn't exist
        self._init_daily_log()
        
        if dry_run:
            logger.info("Post scheduler initialized in DRY RUN mode - no posts will be made")
        else:
            logger.info("Post scheduler initialized")
    
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
        required_vars = ["SOCIALBU_API_KEY", "SOCIALBU_EMAIL", "SOCIALBU_PASSWORD"]
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
    
    def post_unposted_summaries(self) -> Dict[str, List[str]]:
        """Post unposted summaries to social media platforms.
        
        Returns:
            Dict[str, List[str]]: Dictionary mapping platforms to lists of posted video IDs
        """
        logger.info("Checking for unposted summaries")
        
        # Get unposted summaries
        unposted = self.queue_manager.get_unposted_summaries()
        if not unposted:
            logger.info("No unposted summaries found")
            return {}
        
        # Authenticate with SocialBu
        if not self.api.authenticate():
            logger.error("Failed to authenticate with SocialBu API")
            self._log_error("Failed to authenticate with SocialBu API")
            return {}
        
        # Track posted summaries by platform
        posted_by_platform = {platform: [] for platform in PLATFORMS}
        
        # Post each summary to each platform
        for video_id, file_path in unposted:
            try:
                # Load summary
                summary_data = json.loads(file_path.read_text(encoding="utf-8"))
                
                # Post to each platform
                for platform in PLATFORMS:
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
                    
                    # Post to platform
                    response = self.api.create_post(content, account_ids, platform)
                    if response:
                        logger.info(f"Successfully posted {video_id} to {platform}")
                        posted_by_platform[platform].append(video_id)
                    else:
                        logger.error(f"Failed to post {video_id} to {platform}")
                        self._log_error(f"Failed to post {video_id} to {platform}")
                    
                    # Wait between posts to avoid rate limiting
                    time.sleep(5)
                
                # Wait between summaries
                time.sleep(MIN_POSTING_INTERVAL / len(unposted))
                
            except Exception as e:
                logger.error(f"Error posting summary {video_id}: {e}")
                self._log_error(f"Error posting summary {video_id}: {str(e)}")
        
        # Mark as posted in queue manager
        all_posted_ids = []
        for platform, ids in posted_by_platform.items():
            all_posted_ids.extend(ids)
        
        # Remove duplicates
        all_posted_ids = list(set(all_posted_ids))
        
        if all_posted_ids:
            self.queue_manager.mark_as_posted(all_posted_ids)
        
        # Update daily log
        posted_items = [(platform, ids) for platform, ids in posted_by_platform.items() if ids]
        if posted_items:
            self._update_daily_log("posted", posted_items)
        
        # Logout from SocialBu
        self.api.logout()
        
        logger.info(f"Posting complete: {sum(len(ids) for ids in posted_by_platform.values())} summaries posted")
        return posted_by_platform
    
    def run_full_cycle(self, writing_style: str = None) -> Dict[str, Any]:
        """Run a full processing and posting cycle.
        
        Args:
            writing_style (str, optional): Writing style to use for summaries
            
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
        posted = self.post_unposted_summaries()
        
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
    args = parser.parse_args()
    
    # Create scheduler with dry run setting
    scheduler = PostScheduler(dry_run=args.dry_run)
    
    if args.dry_run:
        print("🔍 RUNNING IN DRY RUN MODE - No actual posts will be made")
    
    if args.report:
        # Generate daily report
        report = scheduler.generate_daily_report()
        print(json.dumps(report, indent=2))
    elif args.process_only:
        # Only process queue
        scheduler.check_and_process_queue(args.style)
    elif args.post_only:
        # Only post summaries
        scheduler.post_unposted_summaries()
    else:
        # Run full cycle
        scheduler.run_full_cycle(args.style)


if __name__ == "__main__":
    main()