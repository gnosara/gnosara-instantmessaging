import os
import json
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import logging

# === CONFIG ===
CLAUDE_SONNET_MODEL = "claude-3-7-sonnet-20250219"
CLAUDE_BASE = "https://api.anthropic.com"
MAX_TOKENS = 4000
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/summarizer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("summarizer")

load_dotenv()
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")

if not CLAUDE_API_KEY:
    logger.error("CLAUDE_API_KEY not found in environment variables")
    raise ValueError("CLAUDE_API_KEY not found in environment variables")

STATS = {
    "fixer_attempts": 0,
    "fixer_successes": 0,
}

REQUIRED_FIELDS = [
    "essence",
    "top_takeaways",
    "game_changing_ideas",
    "things_you_can_do",
    "why_this_matters"
]

# Ensure directories exist
Path("salvage").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# --- JSON EXTRACTION ---
def extract_json_objects(text):
    """Extract JSON objects from text using multiple strategies."""
    logger.info("Attempting to extract JSON objects from Claude response")
    results = []
    
    # Check for explicit delimiter
    if "--- END OF SUMMARY ---" in text:
        logger.info("Found explicit delimiters in response")
        parts = text.split("--- END OF SUMMARY ---")
        for part in parts:
            cleaned = part.strip()
            if cleaned:
                try:
                    obj = json.loads(cleaned)
                    results.append(obj)
                    logger.info(f"Successfully extracted JSON object with delimiter method")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON with delimiter method: {e}")
        if results:
            return results

    # Fallback: brace-count based extraction
    logger.info("Trying brace-count based extraction")
    current = ""
    brace_count = 0
    in_object = False
    for char in text:
        if char == '{' and not in_object:
            in_object = True
            brace_count = 1
            current = '{'
        elif in_object:
            current += char
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    try:
                        obj = json.loads(current)
                        results.append(obj)
                        logger.info(f"Successfully extracted JSON object with brace-count method")
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON with brace-count method: {e}")
                    current = ""
                    in_object = False
    
    if not results:
        logger.warning("Standard extraction methods failed, trying robust fallback parser")
        results = parse_robust_json(text)
    
    logger.info(f"Extracted {len(results)} JSON objects in total")
    return results

# --- Robust fallback parser ---
def parse_robust_json(text):
    """More aggressive JSON extraction for malformed responses."""
    logger.info("Using robust JSON parser")
    results = []
    current_obj = ""
    brace_count = 0
    capturing = False
    for char in text:
        if char == '{':
            if brace_count == 0:
                current_obj = ""  # Reset if we're starting a new object
                capturing = True
            brace_count += 1
        if capturing:
            current_obj += char
        if char == '}':
            brace_count -= 1
            if brace_count == 0 and capturing:
                try:
                    # Clean up common JSON issues
                    fixed_json = fix_common_json_errors(current_obj)
                    obj = json.loads(fixed_json)
                    results.append(obj)
                    logger.info(f"Successfully extracted JSON object with robust parser")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON with robust parser: {e}")
                current_obj = ""
                capturing = False
    return results

def fix_common_json_errors(json_str):
    """Fix common JSON formatting errors."""
    # Remove trailing commas in arrays
    import re
    json_str = re.sub(r',\s*]', ']', json_str)
    json_str = re.sub(r',\s*}', '}', json_str)
    
    # Fix missing quotes around keys
    json_str = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', json_str)
    
    return json_str

# --- Claude fallback fixer ---
def call_claude_fix(raw_text, retry_count=0):
    """Use Claude to fix malformed JSON, with retry logic."""
    STATS["fixer_attempts"] += 1
    logger.info(f"Attempting to fix malformed JSON with Claude (attempt {retry_count + 1}/{MAX_RETRIES})")

    FIX_PROMPT = """
You are a JSON repair assistant. The following is a malformed or inconsistently formatted JSON summary. Fix all formatting issues, ensure it's valid JSON, and return only the JSON. No extra explanation, no markdown.

If you see any fields missing from this structure, add them with sensible placeholder values:
{
  "title": "...",
  "podcaster": "...",
  "guest": "...",
  "summary": {
    "essence": "...",
    "top_takeaways": [...],
    "game_changing_ideas": [...],
    "things_you_can_do": [...],
    "why_this_matters": "..."
  }
}
    """.strip()

    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    payload = {
        "model": CLAUDE_SONNET_MODEL,
        "messages": [
            {"role": "user", "content": FIX_PROMPT + "\n\n" + raw_text}
        ],
        "temperature": 0.5,
        "max_tokens": MAX_TOKENS
    }

    try:
        logger.info("Sending fix request to Claude API")
        response = requests.post(f"{CLAUDE_BASE}/v1/messages", headers=headers, json=payload)
        
        if response.status_code != 200:
            logger.error(f"Claude API returned error: {response.status_code} - {response.text}")
            if retry_count < MAX_RETRIES - 1:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                return call_claude_fix(raw_text, retry_count + 1)
            return None
            
        data = response.json()
        fixed_text = data["content"][0]["text"]
        
        # Save the fixed text for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path("logs/fixed_json").mkdir(exist_ok=True)
        with open(f"logs/fixed_json/fixed_{timestamp}.json", "w") as f:
            f.write(fixed_text)
            
        try:
            fixed_json = json.loads(fixed_text)
            STATS["fixer_successes"] += 1
            logger.info("Successfully fixed and parsed JSON")
            return fixed_json
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse fixed JSON: {e}")
            # Try to extract JSON from the fixed text
            extracted = extract_json_objects(fixed_text)
            if extracted:
                logger.info("Extracted JSON from fixed text")
                return extracted[0]
            
            if retry_count < MAX_RETRIES - 1:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                return call_claude_fix(raw_text, retry_count + 1)
            return None
            
    except Exception as e:
        logger.error(f"Error calling Claude API: {e}")
        if retry_count < MAX_RETRIES - 1:
            logger.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return call_claude_fix(raw_text, retry_count + 1)
        return None

# --- JSON Validation ---
def validate_summary(summary, video_id):
    """Validate the structure and content of a summary."""
    logger.info(f"Validating summary for {video_id}")
    errors = []
    
    # Check for required top-level fields
    for field in ["title", "podcaster"]:
        if field not in summary:
            errors.append(f"Missing field: {field}")
    
    # Check for summary section
    if "summary" not in summary:
        errors.append("Missing summary section")
        logger.warning(f"Summary for {video_id} is missing the summary section")
        return errors
    
    # Check for required fields in summary section
    summary_section = summary["summary"]
    for field in REQUIRED_FIELDS:
        if field not in summary_section:
            errors.append(f"Missing field: {field}")
    
    # Check that list fields are actually lists
    for field in ["top_takeaways", "game_changing_ideas", "things_you_can_do"]:
        if field in summary_section and not isinstance(summary_section[field], list):
            errors.append(f"Field {field} is not a list")
    
    # Log validation results
    if errors:
        logger.warning(f"Validation errors in {video_id}: {errors}")
    else:
        logger.info(f"{video_id} passed validation.")
    
    return errors

# --- Main Summarizer ---
def summarize_batch(batch, retry_count=0, writing_style=None):
    """
    Process a batch of videos for summarization with retry logic.
    
    Args:
        batch (list): List of video data to summarize
        retry_count (int, optional): Current retry attempt. Defaults to 0.
        writing_style (str, optional): Writing style to use for summaries. Defaults to None.
        
    Returns:
        list: JSON summary objects
    """
    logger.info(f"Starting batch summarization for {len(batch)} videos using style: {writing_style}")
    
    # Default writing guidance
    writing_guidance = ""
    
    # Try to load writing sample if specified
    if writing_style:
        try:
            # Try to import writing_samples module
            from writing_samples import get_writing_sample
            
            # Get the writing sample for the specified style
            writing_sample = get_writing_sample(writing_style)
            if writing_sample and "example" in writing_sample:
                writing_example = writing_sample["example"]
                writing_description = writing_sample.get("description", "Custom writing style")
                
                # Add writing sample reference to the prompt
                writing_guidance = f"""
WRITING STYLE REFERENCE:
The summary should be written in a {writing_style} style as described below:
- {writing_description}

Here's an example of the writing style to emulate:
---
{writing_example}
---

Please follow this style while creating all summaries.
"""
                logger.info(f"Using '{writing_style}' writing style: {writing_description}")
            else:
                logger.warning(f"No example found for writing style: {writing_style}")
        except ImportError:
            logger.warning("Writing samples module not found, using default style")
    
    prompt = f"""
You are Gnosara, an expert podcast summarizer.

I'm sending you {len(batch)} transcripts to summarize. The original podcast could be lengthy and technical, but I need this summary to be extremely accessible to laypeople.

{writing_guidance}

For EACH transcript, create a summary in exactly this format:

{{
  "title": "The exact title of the podcast episode",
  "podcaster": "Name of the podcast host or show",
  "guest": "Name of the guest (if applicable)",
  "summary": {{
    "essence": "Start with one strong, punchy sentence that grabs attention. Then add 1–2 more to explain the core message of the episode. Avoid academic tone — keep it engaging and clear.",
    
    "top_takeaways": [
      "4-5 bullet points of the most important insights, written in casual, direct language",
      "Use active voice and concrete examples where possible",
      "Focus on surprising or counterintuitive points",
      "Make each point self-contained and valuable on its own",
      "Include a direct quote from the transcript as the final bullet, formatted with dashes like this: -- Quote from the transcript -- Speaker Name" 
    ],
    
    "game_changing_ideas": [
      "4-5 bullet points highlighting paradigm-shifting concepts from the episode",
      "Focus on ideas that challenge conventional wisdom",
      "Explain complex ideas in simple, relatable terms",
      "Emphasize practical implications of these ideas",
      "Include a direct quote from the transcript as the final bullet, formatted with dashes like this: -- Quote from the transcript -- Speaker Name"
    ],
    
    "things_you_can_do": [
      "4-5 bullet points of specific, actionable steps listeners can take",
      "Be concrete and specific rather than general",
      "Include approximate costs, time commitments, or resources needed when relevant",
      "Focus on accessible actions that don't require special expertise"
    ],
    
    "why_this_matters": "A single SHORT paragraph explaining the broader significance of this topic. Connect it to current trends, personal development, or societal issues. Write in a conversational tone that conveys genuine enthusiasm."
  }}
}}

IMPORTANT GUIDELINES:
1. The first sentence of the essence must hook the reader instantly. Use curiosity, bold claims, or surprising facts. Make it clear why this matters.
2. Write like you're talking to a friend — use plain, casual and conversational everyday language (aim for an 8th-grade reading level or lower).
3. Break down complex ideas into simple, bite-sized chunks. Use analogies to explain technical concepts when helpful.
4. Keep it punchy — each sentence should be short (ideally under 15 words) and easy to scan.
5. Make sure quotes are EXACT excerpts from the transcript — no paraphrasing or summarizing. Keep them jargon-free and relatable.
6. Focus on practical insights over theoretical details — show the real-world impact and implications and applications, when possible.
7. Make it personal — use "you" language to help the reader see why this matters to them.
8. The total word count for each summary should be between 400–600 words. Aim for that range across all sections combined.
9. Return ONLY valid JSON in the exact structure requested. DO NOT use markdown formatting. No extra commentary, formatting, or markdown.

AFTER EACH JSON OBJECT, include this delimiter exactly on its own line:
--- END OF SUMMARY ---

Return only the JSON objects and delimiters. No extra commentary or formatting.
"""

    formatted_batch = [
        {
            "index": i + 1,
            "podcaster": item.get("podcaster", item.get("channel", "Unknown")),
            "title": item.get("title", "Unknown"),
            "transcript": item.get("transcript", item.get("text", ""))
        }
        for i, item in enumerate(batch)
    ]

    headers = {
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    payload = {
        "model": CLAUDE_SONNET_MODEL,
        "messages": [
            {"role": "user", "content": prompt + json.dumps(formatted_batch)}
        ],
        "temperature": 0.7,
        "max_tokens": MAX_TOKENS
    }

    try:
        logger.info("Sending batch to Claude API")
        response = requests.post(f"{CLAUDE_BASE}/v1/messages", headers=headers, json=payload)
        
        if response.status_code != 200:
            logger.error(f"Claude API returned error: {response.status_code} - {response.text}")
            if retry_count < MAX_RETRIES - 1:
                logger.info(f"Retrying batch in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                return summarize_batch(batch, retry_count + 1, writing_style)
            return []
            
        data = response.json()
        
        # Save raw response for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path("logs/raw_responses").mkdir(exist_ok=True)
        with open(f"logs/raw_responses/response_{timestamp}.json", "w") as f:
            f.write(json.dumps(data, indent=2))
        
        logger.info("Received response from Claude API")
        
        if "content" in data and isinstance(data["content"], list):
            raw_text = data["content"][0]["text"]
            
            # Save raw text for debugging
            with open(f"logs/raw_responses/text_{timestamp}.txt", "w") as f:
                f.write(raw_text)
            
            json_objects = extract_json_objects(raw_text)
            
            # ✅ Add YouTube links to extracted objects
            for i, obj in enumerate(json_objects):
                if i < len(batch):
                    video_id = batch[i].get("id")
                    if video_id:
                        obj["video_url"] = f"https://www.youtube.com/watch?v={video_id}"
            
            if len(json_objects) != len(batch):
                logger.warning(f"Expected {len(batch)} objects but found {len(json_objects)}")
                
                # Try fallback extraction
                fallback = parse_robust_json(raw_text)
                if len(fallback) > len(json_objects):
                    logger.info(f"Fallback extraction found {len(fallback)} objects")
                    json_objects = fallback
                    
                    # ✅ Add YouTube links to fallback objects
                    for i, obj in enumerate(json_objects):
                        if i < len(batch):
                            video_id = batch[i].get("id")
                            if video_id:
                                obj["video_url"] = f"https://www.youtube.com/watch?v={video_id}"
                
                # If still not enough, try to fix with Claude
                if len(json_objects) < len(batch):
                    logger.info("Still missing objects, attempting to fix with Claude")
                    fixed = call_claude_fix(raw_text)
                    if fixed:
                        if isinstance(fixed, list):
                            logger.info(f"Claude fix returned {len(fixed)} objects")
                            
                            # ✅ Add YouTube links to fixed objects
                            for i, obj in enumerate(fixed):
                                if i < len(batch):
                                    video_id = batch[i].get("id")
                                    if video_id:
                                        obj["video_url"] = f"https://www.youtube.com/watch?v={video_id}"
                            
                            return fixed
                        else:
                            logger.info("Claude fix returned a single object")
                            
                            # ✅ Add YouTube link to single fixed object if we can
                            if len(batch) > 0:
                                video_id = batch[0].get("id")
                                if video_id:
                                    fixed["video_url"] = f"https://www.youtube.com/watch?v={video_id}"
                            
                            return [fixed]
            
            # Ensure we have the right number of objects
            if len(json_objects) < len(batch) and retry_count < MAX_RETRIES - 1:
                logger.warning(f"Still missing objects after all extraction attempts. Retrying batch...")
                time.sleep(RETRY_DELAY)
                return summarize_batch(batch, retry_count + 1, writing_style)
                
            return json_objects
        else:
            logger.error("Claude returned no usable content")
            if retry_count < MAX_RETRIES - 1:
                logger.info(f"Retrying batch in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                return summarize_batch(batch, retry_count + 1, writing_style)
            return []
            
    except Exception as e:
        logger.error(f"Claude summarization failed: {e}")
        if retry_count < MAX_RETRIES - 1:
            logger.info(f"Retrying batch in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return summarize_batch(batch, retry_count + 1, writing_style)
        return []

# Function to process a single video
def process_single_video(video_data, writing_style=None):
    """
    Process a single video and return its summary.
    
    Args:
        video_data (dict): Video data to summarize
        writing_style (str, optional): Writing style to use. Defaults to None.
        
    Returns:
        dict: Summary object with video URL
    """
    logger.info(f"Processing single video: {video_data.get('id', 'unknown')} with style: {writing_style}")
    
    # Create a batch of one
    batch = [video_data]
    results = summarize_batch(batch, writing_style=writing_style)
    
    if results and len(results) > 0:
        logger.info("Successfully processed single video")
        return results[0]
    else:
        logger.error("Failed to process single video")
        return None
