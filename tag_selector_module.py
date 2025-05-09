#!/usr/bin/env python3

import re
import logging
from typing import List, Dict, Any, Set

# Set up logging
logger = logging.getLogger("tag_selector")

# Tag library
TAG_LIBRARY = {
    # General Topics
    "general": [
        "#AI", "#Crypto", "#Web3", "#Bitcoin", "#Blockchain", "#Technology",
        "#Leadership", "#Motivation", "#Mindset", "#Success", "#Habits",
        "#Discipline", "#Wealth", "#Finance", "#Investing", "#Risk",
        "#Innovation", "#Startups"
    ],
    
    # Conscious/Spiritual Topics
    "spiritual": [
        "#Spirituality", "#Presence", "#Healing", "#Awareness", "#Consciousness",
        "#Energy", "#Wisdom", "#Meditation", "#InnerWork"
    ],
    
    # Life Topics
    "life": [
        "#Love", "#Relationships", "#Parenting", "#Emotions", "#Resilience",
        "#Trauma", "#Purpose"
    ],
    
    # Format Types
    "format": [
        "#Podcast", "#Interview", "#Shorts", "#Lecture"
    ]
}

# Flatten tag library for easy lookup
ALL_TAGS = set()
for category in TAG_LIBRARY.values():
    ALL_TAGS.update(category)

# Playlist category mapping (example - customize based on your playlists)
PLAYLIST_CATEGORY_MAP = {
    # Example mappings - replace with actual playlist IDs
    "PLOGi5-fAu8bGACL3TvvVRCdqCMF4-GwSy": ["#AI", "#Technology", "#Innovation"],
    "PLOGi5-fAu8bH_aqRjkNHe_m6zVARmkynC": ["#Mindset", "#Success", "#Motivation"],
    "PLOGi5-fAu8bHAZDlFuohcjjOU7DlJ8bEZ": ["#Spirituality", "#Consciousness", "#Wisdom"],
    "PLOGi5-fAu8bFlc82P2cNj8hjY3MNZaQUw": ["#Finance", "#Wealth", "#Investing"]
}

# Channel category mapping (example - customize based on your channels)
CHANNEL_CATEGORY_MAP = {
    # Example mappings - replace with actual channel names
    "Lex Fridman": ["#AI", "#Technology", "#Podcast", "#Interview"],
    "Huberman Lab": ["#Science", "#Health", "#Podcast"],
    "Tim Ferriss": ["#Success", "#Habits", "#Interview"],
    "Jay Shetty": ["#Mindset", "#Spirituality", "#Purpose"],
    "Rich Roll": ["#Health", "#Spirituality", "#Podcast"]
}

# Keyword mapping to tags
KEYWORD_TO_TAG_MAP = {
    # AI and Technology
    "ai": "#AI",
    "artificial intelligence": "#AI",
    "machine learning": "#AI",
    "deep learning": "#AI",
    "tech": "#Technology",
    "technology": "#Technology",
    "software": "#Technology",
    "hardware": "#Technology",
    "computer": "#Technology",
    "digital": "#Technology",
    
    # Cryptocurrency and Finance
    "crypto": "#Crypto",
    "cryptocurrency": "#Crypto",
    "bitcoin": "#Bitcoin",
    "ethereum": "#Crypto",
    "blockchain": "#Blockchain",
    "web3": "#Web3",
    "finance": "#Finance",
    "money": "#Finance",
    "investing": "#Investing",
    "investment": "#Investing",
    "wealth": "#Wealth",
    "financial": "#Finance",
    "economy": "#Finance",
    
    # Personal Development
    "leadership": "#Leadership",
    "motivation": "#Motivation",
    "motivational": "#Motivation",
    "mindset": "#Mindset",
    "discipline": "#Discipline",
    "habits": "#Habits",
    "success": "#Success",
    "goals": "#Success",
    "achievement": "#Success",
    
    # Entrepreneurship and Business
    "startup": "#Startups",
    "startups": "#Startups",
    "entrepreneurship": "#Startups",
    "entrepreneur": "#Startups",
    "business": "#Startups",
    "innovation": "#Innovation",
    "innovate": "#Innovation",
    "risk": "#Risk",
    
    # Spiritual and Conscious
    "spiritual": "#Spirituality",
    "spirituality": "#Spirituality",
    "meditation": "#Meditation",
    "mindfulness": "#Meditation",
    "presence": "#Presence",
    "awareness": "#Awareness",
    "consciousness": "#Consciousness",
    "healing": "#Healing",
    "energy": "#Energy",
    "wisdom": "#Wisdom",
    "inner work": "#InnerWork",
    
    # Life and Relationships
    "love": "#Love",
    "relationship": "#Relationships",
    "relationships": "#Relationships",
    "parenting": "#Parenting",
    "family": "#Parenting",
    "children": "#Parenting",
    "emotion": "#Emotions",
    "emotions": "#Emotions",
    "emotional": "#Emotions",
    "resilience": "#Resilience",
    "trauma": "#Trauma",
    "purpose": "#Purpose",
    "meaning": "#Purpose",
    
    # Content Formats
    "podcast": "#Podcast",
    "interview": "#Interview",
    "conversation": "#Interview",
    "shorts": "#Shorts",
    "lecture": "#Lecture",
    "talk": "#Lecture"
}

def select_tags(title: str, channel_name: str, playlist_id: str = None, summary: str = None) -> List[str]:
    """Select 5 relevant tags for a video based on title, channel, and optionally playlist and summary.
    
    Args:
        title (str): Video title
        channel_name (str): Channel name
        playlist_id (str, optional): Playlist ID
        summary (str, optional): Video summary text
        
    Returns:
        List[str]: List of exactly 5 relevant tags
    """
    # Track potential tags and their scores
    tag_scores = {}
    
    # Initialize all potential tags with zero score
    for tag in ALL_TAGS:
        tag_scores[tag] = 0
    
    # 1. Add score for tags from the channel mapping
    channel_tags = CHANNEL_CATEGORY_MAP.get(channel_name, [])
    for tag in channel_tags:
        if tag in tag_scores:
            tag_scores[tag] += 3  # Higher weight for channel tags
    
    # 2. Add score for tags from the playlist mapping
    if playlist_id:
        playlist_tags = PLAYLIST_CATEGORY_MAP.get(playlist_id, [])
        for tag in playlist_tags:
            if tag in tag_scores:
                tag_scores[tag] += 4  # Higher weight for playlist tags
    
    # 3. Analyze title for keywords
    title_lower = title.lower()
    for keyword, tag in KEYWORD_TO_TAG_MAP.items():
        if keyword in title_lower:
            tag_scores[tag] = tag_scores.get(tag, 0) + 2
    
    # 4. Analyze summary text if provided
    if summary:
        summary_lower = summary.lower()
        for keyword, tag in KEYWORD_TO_TAG_MAP.items():
            if keyword in summary_lower:
                tag_scores[tag] = tag_scores.get(tag, 0) + 1
    
    # Always include #Podcast tag if no format tag has a high score
    format_tags = TAG_LIBRARY["format"]
    max_format_score = max([tag_scores.get(tag, 0) for tag in format_tags])
    if max_format_score < 2:
        tag_scores["#Podcast"] = tag_scores.get("#Podcast", 0) + 2
    
    # Sort tags by score (descending)
    sorted_tags = sorted(tag_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Get the top 5 tags
    top_tags = [tag for tag, score in sorted_tags[:5] if score > 0]
    
    # If we don't have 5 tags yet, add some defaults to reach exactly 5
    while len(top_tags) < 5:
        for tag in ALL_TAGS:
            if tag not in top_tags:
                top_tags.append(tag)
                break
    
    # Ensure we return exactly 5 tags
    return top_tags[:5]


# Example usage
if __name__ == "__main__":
    # Set up logging for standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Test with some sample inputs
    test_videos = [
        {
            "title": "How AI is Transforming the Future of Work",
            "channel": "Lex Fridman",
            "playlist_id": "PLOGi5-fAu8bGACL3TvvVRCdqCMF4-GwSy"
        },
        {
            "title": "The Science of Building Better Habits",
            "channel": "Huberman Lab",
            "playlist_id": None
        },
        {
            "title": "Finding Purpose Through Meditation",
            "channel": "Jay Shetty",
            "playlist_id": "PLOGi5-fAu8bHAZDlFuohcjjOU7DlJ8bEZ"
        }
    ]
    
    for i, video in enumerate(test_videos):
        tags = select_tags(video["title"], video["channel"], video["playlist_id"])
        print(f"Test {i+1}: {video['title']}")
        print(f"Selected tags: {', '.join(tags)}")
        print()
