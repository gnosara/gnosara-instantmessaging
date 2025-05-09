#!/usr/bin/env python3

import json
import logging
import re
from typing import Dict, Any, List, Optional
from pathlib import Path
from post_formatter import PostFormatter  # Import the base formatter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/telegram_formatter.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telegram_formatter")

# Constants
CTA = "✨ Start your day 1% smarter. Follow for daily breakthroughs."


class TelegramFormatter:
    """Formats podcast summaries for Telegram."""
    
    @staticmethod
    def format_for_telegram(summary_json: Dict[str, Any], filename: str = None) -> str:
        """Format a summary into Telegram content.
        
        Args:
            summary_json (Dict[str, Any]): The summary JSON object
            filename (str, optional): Original filename for video ID extraction
            
        Returns:
            str: Formatted content for Telegram
        """
        try:
            logger.info("Formatting summary for Telegram")
            s = summary_json["summary"]
            title = summary_json.get("title", "")
            podcaster = summary_json.get("podcaster", "Unknown Podcast")
            guest = summary_json.get("guest", "")
            
            # First try to get video_url directly from the summary JSON
            youtube_link = summary_json.get("video_url", "")
            
            # If video_url is not in JSON, try to get video_id
            if not youtube_link:
                # Try to get video_id from the JSON data
                video_id = summary_json.get("video_id", "")
                
                # If video_id is not in JSON, try to extract it from the filename
                if not video_id and filename:
                    extracted_id = PostFormatter.extract_video_id_from_filename(filename)
                    if extracted_id:
                        video_id = extracted_id
                        logger.info(f"Extracted video_id {video_id} from filename {filename}")
                
                # Create YouTube link if video_id is available
                if video_id:
                    youtube_link = f"https://www.youtube.com/watch?v={video_id}"
                else:
                    # Fallback to a generic YouTube link if no video_id is found
                    youtube_link = "https://www.youtube.com"
                    logger.warning(f"No video_id found for {filename}, using generic YouTube link")
            
            # Handle missing guest field - fallback to podcaster if guest is missing
            if not guest or guest.lower() == "unknown":
                featuring = podcaster
            else:
                featuring = guest

            # Format the content according to the template
            # Using Telegram HTML formatting: <b>bold</b>, <i>italic</i>, <a href="link">text</a>
            content_parts = []
            
            # Top Section (Header) - formatted for Telegram with HTML
            content_parts.append(f"🌀 <b>{title}</b>")
            content_parts.append(f"🎧 <b>{podcaster}</b>")
            content_parts.append(f"🧑 <b>Featuring:</b> {featuring}")
            content_parts.append("")
            
            # Essence paragraph
            content_parts.append(f"<i>{s.get('essence', '')}</i>")
            content_parts.append("")
            
            # Top Takeaways with dashes
            content_parts.append("<b>Top Takeaways:</b>")
            for point in s.get("top_takeaways", []):
                content_parts.append(f"• {point}")
            content_parts.append("")
            
            # Game-Changing Ideas with bullet points
            content_parts.append("<b>Game-Changing Ideas:</b>")
            for idea in s.get("game_changing_ideas", []):
                content_parts.append(f"• {idea}")
            content_parts.append("")
            
            # Things You Can Do with bullet points
            content_parts.append("<b>Things You Can Do:</b>")
            for action in s.get("things_you_can_do", []):
                content_parts.append(f"• {action}")
            content_parts.append("")
            
            # Why This Matters
            content_parts.append(f"<b>Why This Matters:</b>")
            content_parts.append(s.get("why_this_matters", ""))
            content_parts.append("")
            
            # Add tags if available
            if "tags" in summary_json and isinstance(summary_json["tags"], list) and summary_json["tags"]:
                formatted_tags = " ".join(tag for tag in summary_json["tags"])
                content_parts.append("")
                content_parts.append(formatted_tags)
            
            # Ending with the YouTube link and CTA - with extra validation
            if youtube_link and youtube_link.startswith("http"):
                # Use a simpler format for the link to avoid HTML parsing issues
                content_parts.append(f"👉 Watch the full episode: {youtube_link}")
            else:
                content_parts.append("👉 Watch the full episode on YouTube")
                
            content_parts.append(CTA)
            
            # Join all parts with newlines
            content = "\n".join(content_parts)
            
            logger.info(f"Formatted content for Telegram with {len(content)} characters")
            return content
        
        except KeyError as e:
            logger.error(f"Missing required field in summary: {e}")
            return ""
        except Exception as e:
            logger.error(f"Error formatting content: {e}")
            return ""
    
    @staticmethod
    def format_summary(summary_json: Dict[str, Any], filename: str = None) -> str:
        """Format a summary for Telegram.
        
        Args:
            summary_json (Dict[str, Any]): The summary JSON object
            filename (str, optional): Original filename for video ID extraction
            
        Returns:
            str: Formatted content for Telegram
        """
        return TelegramFormatter.format_for_telegram(summary_json, filename)


# Example usage
if __name__ == "__main__":
    # Test with a sample summary
    sample_path = Path("summaries/dQw4w9WgXcQ_sample_ai_podcast.json")
    
    if sample_path.exists():
        with open(sample_path, "r") as f:
            sample_summary = json.load(f)
        
        # Format for Telegram
        telegram_content = TelegramFormatter.format_summary(sample_summary, sample_path.name)
        print("Telegram Content Sample:")
        print("-" * 80)
        print(telegram_content)
        print("-" * 80)
        print(f"Character count: {len(telegram_content)}")
    else:
        print(f"Sample file not found: {sample_path}")