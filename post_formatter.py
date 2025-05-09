#!/usr/bin/env python3

import json
import logging
import re
from typing import Dict, Any, List, Tuple
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/post_formatter.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("post_formatter")

# Constants
MAX_TWITTER_CHARS = 25000  # SocialBu handles Twitter's actual limit internally
MAX_FACEBOOK_CHARS = 63206  # Facebook's character limit
CTA = "✨ Start your day 1% smarter. Follow for daily breakthroughs."

class PostFormatter:
    """Formats podcast summaries for different social media platforms."""
    
    @staticmethod
    def extract_video_id_from_filename(filename: str) -> str:
        """Extract video ID from filename if it follows the pattern VIDEO_ID_rest_of_filename.json.
        
        Args:
            filename (str): The filename to extract from
            
        Returns:
            str: The video ID or empty string if not found
        """
        # Try to match VIDEO_ID_rest_of_filename.json pattern
        match = re.match(r'^([a-zA-Z0-9_-]+)_.*\.json$', filename)
        if match:
            return match.group(1)
        return ""
    
    @staticmethod
    def format_for_twitter(summary_json: Dict[str, Any], filename: str = None) -> str:
        """Format a summary into Twitter content using the Gnosara format.
        
        Args:
            summary_json (Dict[str, Any]): The summary JSON object
            filename (str, optional): Original filename for video ID extraction
            
        Returns:
            str: Formatted content for Twitter
        """
        try:
            logger.info("Formatting summary for Twitter")
            s = summary_json["summary"]
            title = summary_json.get("title", "")
            podcaster = summary_json.get("podcaster", "Unknown Podcast")
            guest = summary_json.get("guest", "")
            
            # First try to get video_url directly from the summary JSON (new approach)
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
            content_parts = []
            
            # Top Section (Header) - always structured exactly like this
            content_parts.append(f"🌀 {title}")
            content_parts.append(f"🎧 {podcaster}")
            content_parts.append(f"🧑 Featuring: {featuring}")
            content_parts.append("")
            
            # Essence paragraph
            content_parts.append(s.get("essence", ""))
            content_parts.append("")
            
            # Top Takeaways with dashes instead of bullets
            content_parts.append("Top Takeaways:")
            for point in s.get("top_takeaways", []):
                content_parts.append(f"- {point}")
            content_parts.append("")
            
            # Game-Changing Ideas with dashes
            content_parts.append("Game-Changing Ideas:")
            for idea in s.get("game_changing_ideas", []):
                content_parts.append(f"- {idea}")
            content_parts.append("")
            
            # Things You Can Do with dashes
            content_parts.append("Things You Can Do:")
            for action in s.get("things_you_can_do", []):
                content_parts.append(f"- {action}")
            content_parts.append("")
            
            # Why This Matters
            content_parts.append(f"Why This Matters:")
            content_parts.append(s.get("why_this_matters", ""))
            content_parts.append("")
            
            # Ending (CTA) - exactly as specified
            # Make the YouTube link very prominent
            content_parts.append(f"👉 Watch the full episode: {youtube_link}")
            content_parts.append(CTA)
            
            # Join all parts with newlines
            content = "\n".join(content_parts)
            
            # Check character limit
            if len(content) > MAX_TWITTER_CHARS:
                logger.warning(f"Content exceeds {MAX_TWITTER_CHARS} characters ({len(content)}), truncating")
                content = content[:MAX_TWITTER_CHARS-3] + "..."
            
            logger.info(f"Formatted content for Twitter with {len(content)} characters")
            return content
        
        except KeyError as e:
            logger.error(f"Missing required field in summary: {e}")
            return ""
        except Exception as e:
            logger.error(f"Error formatting content: {e}")
            return ""
    
    @staticmethod
    def format_for_facebook(summary_json: Dict[str, Any], filename: str = None) -> str:
        """Format a summary into Facebook content using the Gnosara format.
        
        Args:
            summary_json (Dict[str, Any]): The summary JSON object
            filename (str, optional): Original filename for video ID extraction
            
        Returns:
            str: Formatted content for Facebook
        """
        # For now, Facebook formatting is the same as Twitter
        # In the future, you might want to customize this further
        return PostFormatter.format_for_twitter(summary_json, filename)
    
    @staticmethod
    def format_summary(summary_json: Dict[str, Any], platform: str, filename: str = None) -> str:
        """Format a summary for a specific platform.
        
        Args:
            summary_json (Dict[str, Any]): The summary JSON object
            platform (str): The platform to format for ('twitter', 'facebook', etc.)
            filename (str, optional): Original filename for video ID extraction
            
        Returns:
            str: Formatted content for the specified platform
        """
        platform = platform.lower()
        
        if platform == "twitter" or platform == "x":
            return PostFormatter.format_for_twitter(summary_json, filename)
        elif platform == "facebook":
            return PostFormatter.format_for_facebook(summary_json, filename)
        else:
            logger.warning(f"Unsupported platform: {platform}, using Twitter format")
            return PostFormatter.format_for_twitter(summary_json, filename)


# Example usage
if __name__ == "__main__":
    # Test with a sample summary
    sample_path = Path("summaries/dQw4w9WgXcQ_sample_ai_podcast.json")
    
    if sample_path.exists():
        with open(sample_path, "r") as f:
            sample_summary = json.load(f)
        
        # Format for Twitter
        twitter_content = PostFormatter.format_summary(sample_summary, "twitter", sample_path.name)
        print("Twitter Content Sample:")
        print("-" * 80)
        print(twitter_content)
        print("-" * 80)
        print(f"Character count: {len(twitter_content)}")
    else:
        print(f"Sample file not found: {sample_path}")
