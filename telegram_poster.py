#!/usr/bin/env python3

import os
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Set, Tuple, Optional

# Add main section for CLI usage
"""
Telegram poster for Gnosara podcast summary system.

This module handles posting podcast summaries to Telegram channels.
It can be used as a standalone script or imported as a module.
"""

# Import our modules
from telegram_api import TelegramAPI
from telegram_formatter import TelegramFormatter
from summary_queue import SummaryQueue

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/telegram_poster.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telegram_poster")

# Constants
DAILY_LOG_FILE = Path("logs/daily_log.json")
MIN_POSTING_INTERVAL = 20 * 60  # 20 minutes in seconds


class TelegramPoster:
    """Handler for posting summaries to Telegram channels."""
    
    def __init__(self, dry_run: bool = False):
        """Initialize the Telegram poster.
        
        Args:
            dry_run (bool, optional): If True, don't actually post to Telegram
        """
        # Ensure directories exist
        Path("logs").mkdir(exist_ok=True)
        
        # Load environment variables
        self._load_env_vars()
        
        # Store dry run setting
        self.dry_run = dry_run
        
        # Initialize components
        self.api = TelegramAPI(dry_run=dry_run)
        self.formatter = TelegramFormatter()
        self.queue_manager = SummaryQueue()
        
        if dry_run:
            logger.info("Telegram poster initialized in DRY RUN mode - no posts will be made")
        else:
            logger.info("Telegram poster initialized")
    
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
        required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_IDS"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _update_daily_log(self, video_ids: List[str]) -> None:
        """Update the daily log with Telegram posted items.
        
        Args:
            video_ids (List[str]): List of video IDs posted to Telegram
        """
        try:
            # Load current log
            log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            
            # Check if we need to create a new daily log
            today = datetime.now().strftime("%Y-%m-%d")
            if log_data.get("date") != today:
                # Initialize new daily log
                log_data = {
                    "date": today,
                    "summarized": [],
                    "posted": {},
                    "errors": [],
                    "pending": []
                }
            
            # Make sure 'telegram' is in the posted dict
            if "posted" not in log_data:
                log_data["posted"] = {}
                
            if "telegram" not in log_data["posted"]:
                log_data["posted"]["telegram"] = []
            
            # Add the video IDs to the telegram list
            log_data["posted"]["telegram"].extend(video_ids)
            
            # Remove duplicates
            log_data["posted"]["telegram"] = list(set(log_data["posted"]["telegram"]))
            
            # Save updated log
            DAILY_LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
            logger.info(f"Updated daily log: added {len(video_ids)} items to telegram posted list")
            
        except Exception as e:
            logger.error(f"Failed to update daily log: {e}")
    
    def _log_error(self, message: str) -> None:
        """Log an error to the daily log file.
        
        Args:
            message (str): The error message to log
        """
        try:
            # Load current log
            log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            
            # Check if we need to create a new daily log
            today = datetime.now().strftime("%Y-%m-%d")
            if log_data.get("date") != today:
                # Initialize new daily log
                log_data = {
                    "date": today,
                    "summarized": [],
                    "posted": {},
                    "errors": [],
                    "pending": []
                }
            
            # Add the error message with timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error = f"{timestamp} - {message}"
            
            if "errors" not in log_data:
                log_data["errors"] = []
                
            log_data["errors"].append(error)
            
            # Save updated log
            DAILY_LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
            logger.info(f"Added error to daily log: {message}")
            
        except Exception as e:
            logger.error(f"Failed to log error: {e}")

    def post_unposted_summaries(self) -> List[str]:
        """Post unposted summaries to Telegram.
        
        Returns:
            List[str]: List of posted video IDs
        """
        logger.info("Checking for unposted summaries for Telegram")
        
        # Get unposted summaries
        unposted = self.queue_manager.get_unposted_summaries()
        if not unposted:
            logger.info("No unposted summaries found")
            return []
        
        # Check bot status
        if not self.api.check_bot_status():
            logger.error("Telegram bot is not operational")
            self._log_error("Telegram bot is not operational")
            return []
        
        # Track posted summaries
        posted_ids = []
        
        # Post each summary
        for video_id, file_path in unposted:
            try:
                # Load summary
                summary_data = json.loads(file_path.read_text(encoding="utf-8"))
                
                # Format content for Telegram
                content = self.formatter.format_summary(summary_data, file_path.name)
                if not content:
                    logger.error(f"Failed to format content for {video_id} on Telegram")
                    self._log_error(f"Failed to format content for {video_id} on Telegram")
                    continue
                
                # Broadcast to all configured Telegram chats
                response = self.api.broadcast_message(content)
                
                # Check if all broadcasts were successful
                all_success = all(result.get("success", False) for result in response.get("results", []))
                
                if all_success:
                    logger.info(f"Successfully posted {video_id} to Telegram")
                    posted_ids.append(video_id)
                else:
                    # Some broadcasts failed
                    success_count = sum(1 for result in response.get("results", []) if result.get("success", False))
                    total_count = len(response.get("results", []))
                    logger.warning(f"Partially posted {video_id} to Telegram ({success_count}/{total_count} succeeded)")
                    
                    # Still count it as posted if at least one broadcast succeeded
                    if success_count > 0:
                        posted_ids.append(video_id)
                    else:
                        logger.error(f"Failed to post {video_id} to Telegram (all broadcasts failed)")
                        self._log_error(f"Failed to post {video_id} to Telegram (all broadcasts failed)")
                
                # Wait between summaries to avoid rate limiting
                time.sleep(MIN_POSTING_INTERVAL / len(unposted))
                
            except Exception as e:
                logger.error(f"Error posting summary {video_id} to Telegram: {e}")
                self._log_error(f"Error posting summary {video_id} to Telegram: {str(e)}")
        
        # Mark as posted in queue manager
        if posted_ids:
            self.queue_manager.mark_as_posted(posted_ids)
            
            # Update daily log
            self._update_daily_log(posted_ids)
        
        logger.info(f"Telegram posting complete: {len(posted_ids)} summaries posted")
        return posted_ids
    
    def run(self) -> Dict[str, Any]:
        """Run the Telegram posting process.
        
        Returns:
            Dict[str, Any]: Results of the posting process
        """
        logger.info("Starting Telegram posting process")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "posted": [],
            "errors": []
        }
        
        # Post unposted summaries
        posted_ids = self.post_unposted_summaries()
        
        # Collect results
        results["posted"] = posted_ids
        
        logger.info("Telegram posting process complete")
        return results


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Gnosara Telegram Poster")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry run mode - no actual posts will be made")
    parser.add_argument("--report", action="store_true", help="Generate and print posting report")
    args = parser.parse_args()
    
    # Create poster with dry run setting
    poster = TelegramPoster(dry_run=args.dry_run)
    
    if args.dry_run:
        print("🔍 RUNNING IN DRY RUN MODE - No actual posts will be made")
    
    if args.report:
        # Load daily log to get Telegram posting stats
        try:
            log_data = json.loads(DAILY_LOG_FILE.read_text(encoding="utf-8"))
            telegram_posts = log_data.get("posted", {}).get("telegram", [])
            print(f"Telegram posts today: {len(telegram_posts)}")
            print(f"Post IDs: {', '.join(telegram_posts)}")
        except Exception as e:
            print(f"Error generating report: {e}")
    else:
        # Run the posting process
        results = poster.run()
        
        # Print summary
        print(f"Posting complete. Posted {len(results['posted'])} summaries to Telegram.")
        if results["posted"]:
            print(f"Posted IDs: {', '.join(results['posted'])}")


if __name__ == "__main__":
    main()
