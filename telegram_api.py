#!/usr/bin/env python3

import os
import json
import time
import logging
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/telegram_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telegram_api")

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
TELEGRAM_LOG_FILE = Path("logs/telegram_log.json")
TELEGRAM_MESSAGE_CHUNK_SIZE = 4096  # Maximum message size for Telegram


class TelegramAPI:
    """Handler for Telegram API interactions."""
    
    def __init__(self, bot_token: str = None, chat_ids: List[str] = None, dry_run: bool = False):
        """Initialize the Telegram API client.
        
        Args:
            bot_token (str, optional): Telegram Bot token. If not provided, 
                                     will try to load from environment.
            chat_ids (List[str], optional): List of chat IDs to send messages to.
                                     If not provided, will try to load from environment.
            dry_run (bool, optional): If True, don't actually send to Telegram
        """
        self.api_base_url = "https://api.telegram.org/bot"
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.dry_run = dry_run
        
        # Get chat IDs from environment if not provided
        if chat_ids is None:
            chat_ids_env = os.getenv("TELEGRAM_CHAT_IDS", "")
            # Split by comma and remove any empty strings
            self.chat_ids = [chat_id.strip() for chat_id in chat_ids_env.split(",") if chat_id.strip()]
        else:
            self.chat_ids = chat_ids
        
        if not self.bot_token and not self.dry_run:
            logger.error("TELEGRAM_BOT_TOKEN not found in environment variables")
            raise ValueError("TELEGRAM_BOT_TOKEN not found in environment variables")
        
        if not self.chat_ids and not self.dry_run:
            logger.warning("No Telegram chat IDs found. Messages will have nowhere to send.")
        
        # Ensure the log directory exists
        Path("logs").mkdir(exist_ok=True)
        
        # Create Telegram log file if it doesn't exist
        if not TELEGRAM_LOG_FILE.exists():
            TELEGRAM_LOG_FILE.write_text(json.dumps({}), encoding="utf-8")
            
        if self.dry_run:
            logger.info("Telegram API initialized in DRY RUN mode - no messages will be sent")
        else:
            logger.info(f"Telegram API handler initialized with {len(self.chat_ids)} chat IDs")
    
    def check_bot_status(self) -> bool:
        """Check if the bot token is valid and the bot is operational.
        
        Returns:
            bool: True if bot is operational, False otherwise
        """
        # If in dry run mode, return mock success without checking
        if self.dry_run:
            logger.info("[DRY RUN] Mock bot status check successful")
            return True
        
        if not self.bot_token:
            logger.error("No bot token provided")
            return False
        
        url = f"{self.api_base_url}{self.bot_token}/getMe"
        
        try:
            logger.info("Checking Telegram bot status")
            response = requests.get(url)
            
            if response.status_code != 200:
                logger.error(f"Bot status check failed: {response.status_code} - {response.text}")
                return False
            
            data = response.json()
            if not data.get("ok"):
                logger.error(f"Bot status check failed: {data.get('description', 'Unknown error')}")
                return False
            
            bot_info = data.get("result", {})
            bot_name = bot_info.get("username")
            logger.info(f"Bot status check successful. Bot name: @{bot_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error checking bot status: {e}")
            return False
    
    def send_message(self, content: str, chat_id: str, 
                    retry_count: int = 0) -> Dict[str, Any]:
        """Send a message to a specific Telegram chat.
        
        Args:
            content (str): The message content
            chat_id (str): The chat ID to send to
            retry_count (int, optional): Current retry attempt
            
        Returns:
            Dict[str, Any]: Response data or empty dict if failed
        """
        # If in dry run mode, return a mock success response
        if self.dry_run:
            mock_response = {
                "ok": True,
                "mock_message": True,
                "chat_id": chat_id,
                "content_preview": content[:100] + "..." if len(content) > 100 else content,
                "timestamp": datetime.utcnow().isoformat(),
                "message": "Dry run mode - no actual message was sent"
            }
            logger.info(f"[DRY RUN] Mock message sent to chat ID: {chat_id}")
            
            # Log the mock message
            self._log_message(mock_response, chat_id, content)
            
            return mock_response
        
        # Check if content exceeds maximum message size
        if len(content) > TELEGRAM_MESSAGE_CHUNK_SIZE:
            logger.info(f"Message exceeds Telegram limit ({len(content)} chars), splitting into chunks")
            return self._send_chunked_message(content, chat_id)
        
        # Real message sending logic
        url = f"{self.api_base_url}{self.bot_token}/sendMessage"
        
        # First try to send with HTML formatting
        try_html = True
        
        if try_html:
            # Try with HTML formatting first
            payload = {
                "chat_id": chat_id,
                "text": content,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
        else:
            # Fallback to plain text if HTML failed previously
            payload = {
                "chat_id": chat_id,
                "text": content,
                "disable_web_page_preview": True
            }
        
        try:
            logger.info(f"Sending message to chat ID: {chat_id}")
            response = requests.post(url, json=payload)
            
            if response.status_code != 200:
                error_text = response.text
                logger.error(f"Failed to send message: {response.status_code} - {error_text}")
                
                # If it's an HTML parsing error and we were using HTML mode, try again without HTML
                if try_html and "can't parse entities" in error_text and retry_count == 0:
                    logger.info("HTML parsing error detected, retrying without HTML formatting")
                    # Try again without HTML formatting
                    plain_payload = {
                        "chat_id": chat_id,
                        "text": content,
                        "disable_web_page_preview": True
                    }
                    plain_response = requests.post(url, json=plain_payload)
                    
                    if plain_response.status_code == 200:
                        data = plain_response.json()
                        self._log_message(data, chat_id, content)
                        logger.info(f"Successfully sent message without HTML to chat ID: {chat_id}")
                        return data
                
                # Retry if we haven't reached max retries
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds (attempt {retry_count + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    return self.send_message(content, chat_id, retry_count + 1)
                
                return {}
            
            data = response.json()
            
            # Log the successful message
            self._log_message(data, chat_id, content)
            
            logger.info(f"Successfully sent message to chat ID: {chat_id}")
            return data
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            
            # Retry if we haven't reached max retries
            if retry_count < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds (attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                return self.send_message(content, chat_id, retry_count + 1)
            
            return {}
    
    def _send_chunked_message(self, content: str, chat_id: str) -> Dict[str, Any]:
        """Send a large message as multiple chunks.
        
        Args:
            content (str): The full message content
            chat_id (str): The chat ID to send to
            
        Returns:
            Dict[str, Any]: Response data from the last chunk or empty dict if failed
        """
        # Split content into chunks of TELEGRAM_MESSAGE_CHUNK_SIZE
        chunks = [content[i:i + TELEGRAM_MESSAGE_CHUNK_SIZE] 
                 for i in range(0, len(content), TELEGRAM_MESSAGE_CHUNK_SIZE)]
        
        logger.info(f"Splitting message into {len(chunks)} chunks")
        
        last_response = {}
        for i, chunk in enumerate(chunks):
            # Add part indicator if multiple chunks
            if len(chunks) > 1:
                chunk_header = f"Part {i+1}/{len(chunks)}\n\n"
                if i > 0:  # Add separator line for all but the first chunk
                    chunk_header = f"\n{'='*20}\n\n{chunk_header}"
                chunk = chunk_header + chunk
            
            # Send chunk
            response = self.send_message(chunk, chat_id)
            
            if not response:
                logger.error(f"Failed to send chunk {i+1}/{len(chunks)}")
                return {}
            
            last_response = response
            
            # Wait between chunks to avoid rate limiting
            if i < len(chunks) - 1:
                time.sleep(1)
        
        return last_response
    
    def broadcast_message(self, content: str) -> Dict[str, List[Dict[str, Any]]]:
        """Send the same message to all configured chat IDs.
        
        Args:
            content (str): The message content
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: Results by chat ID
        """
        if not self.chat_ids:
            logger.warning("No chat IDs configured, cannot broadcast message")
            return {"results": []}
        
        results = []
        
        for chat_id in self.chat_ids:
            response = self.send_message(content, chat_id)
            results.append({
                "chat_id": chat_id,
                "success": bool(response),
                "response": response
            })
            
            # Wait between messages to avoid rate limiting
            if chat_id != self.chat_ids[-1]:
                time.sleep(1)
        
        logger.info(f"Broadcast message to {len(self.chat_ids)} chats")
        return {"results": results}
    
    def _log_message(self, response_data: Dict[str, Any], chat_id: str, content: str) -> None:
        """Log successful message to telegram_log.json.
        
        Args:
            response_data (Dict[str, Any]): The API response data
            chat_id (str): The chat ID sent to
            content (str): The content that was sent
        """
        try:
            # Load existing log
            log_data = json.loads(TELEGRAM_LOG_FILE.read_text(encoding="utf-8"))
            
            # Generate a unique key for this message based on timestamp
            timestamp = datetime.now().isoformat()
            message_key = f"message_{timestamp}"
            
            # Create log entry
            log_entry = {
                "timestamp": timestamp,
                "chat_id": chat_id,
                "content_preview": content[:100] + "..." if len(content) > 100 else content,
                "content_length": len(content),
                "response": response_data
            }
            
            # Add to log
            log_data[message_key] = log_entry
            
            # Save log
            TELEGRAM_LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
            logger.info(f"Message logged successfully with key: {message_key}")
            
        except Exception as e:
            logger.error(f"Error logging message: {e}")


# Example usage
if __name__ == "__main__":
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("dotenv not installed, skipping .env loading")
    
    # Create API client
    api = TelegramAPI()
    
    # Test bot status
    if api.check_bot_status():
        print("Bot is operational!")
        
        # Send a test message if we have chat IDs
        if api.chat_ids:
            test_content = "This is a test message from the Gnosara automation system."
            response = api.broadcast_message(test_content)
            print(f"Message broadcast results: {json.dumps(response, indent=2)}")
        else:
            print("No chat IDs configured, cannot send test message")
    else:
        print("Bot is not operational!")