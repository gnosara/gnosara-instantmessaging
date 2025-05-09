#!/usr/bin/env python3

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/socialbu_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("socialbu_api")

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
POST_LOG_FILE = Path("logs/post_log.json")


class SocialBuAPI:
    """Handler for SocialBu API interactions."""
    
    def __init__(self, api_key: str = None, dry_run: bool = False):
        """Initialize the SocialBu API client.
        
        Args:
            api_key (str, optional): API key for SocialBu. If not provided, 
                                     will try to load from environment.
            dry_run (bool, optional): If True, don't actually post to social media
        """
        self.api_base_url = "https://socialbu.com/api/v1"
        self.api_key = api_key or os.getenv("SOCIALBU_API_KEY")
        self.dry_run = dry_run
        
        if not self.api_key and not self.dry_run:
            logger.error("SOCIALBU_API_KEY not found in environment variables")
            raise ValueError("SOCIALBU_API_KEY not found in environment variables")
        
        self.auth_token = None
        self.headers = {
            "Content-Type": "application/json"
        }
        
        # Ensure the log directory exists
        Path("logs").mkdir(exist_ok=True)
        
        # Create post log file if it doesn't exist
        if not POST_LOG_FILE.exists():
            POST_LOG_FILE.write_text(json.dumps({}), encoding="utf-8")
            
        if self.dry_run:
            logger.info("SocialBu API initialized in DRY RUN mode - no posts will be made")
        else:
            logger.info("SocialBu API handler initialized")
    
    def authenticate(self) -> bool:
        """Authenticate with SocialBu API to get auth token.
        
        Returns:
            bool: True if authentication was successful, False otherwise.
        """
        # If in dry run mode, return mock success without attempting real authentication
        if self.dry_run:
            logger.info("[DRY RUN] Mock authentication successful")
            self.auth_token = "mock_auth_token"
            self.headers["Authorization"] = f"Bearer {self.auth_token}"
            return True
            
        # Real authentication logic
        email = os.getenv("SOCIALBU_EMAIL")
        password = os.getenv("SOCIALBU_PASSWORD")
        
        if not email or not password:
            logger.error("Missing SOCIALBU_EMAIL or SOCIALBU_PASSWORD in environment")
            return False
        
        auth_url = f"{self.api_base_url}/auth/get_token"
        payload = {
            "email": email,
            "password": password
        }
        
        try:
            logger.info("Authenticating with SocialBu API")
            response = requests.post(auth_url, json=payload)
            
            if response.status_code != 200:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
            
            data = response.json()
            self.auth_token = data.get("authToken")
            
            if not self.auth_token:
                logger.error("No auth token received in response")
                return False
            
            # Update headers with the token
            self.headers["Authorization"] = f"Bearer {self.auth_token}"
            logger.info("Successfully authenticated with SocialBu API")
            return True
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        """Get list of connected social media accounts.
        
        Returns:
            List[Dict[str, Any]]: List of account objects or empty list if failed.
        """
        # If in dry run mode, return mock accounts
        if self.dry_run:
            logger.info("[DRY RUN] Returning mock accounts")
            mock_accounts = [
                {
                    "id": 1001,
                    "name": "Mock Twitter Account",
                    "provider": "twitter",
                    "provider_id": "mock_twitter_id",
                    "status": "active"
                },
                {
                    "id": 1002,
                    "name": "Mock Facebook Account",
                    "provider": "facebook",
                    "provider_id": "mock_facebook_id",
                    "status": "active"
                }
            ]
            return mock_accounts
            
        # Real account fetching logic
        if not self.auth_token and not self.authenticate():
            logger.error("Cannot get accounts without authentication")
            return []
        
        url = f"{self.api_base_url}/accounts"
        
        try:
            logger.info("Fetching social media accounts")
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to get accounts: {response.status_code} - {response.text}")
                return []
            
            # Log the raw response for debugging
            logger.info(f"Raw API response: {response.text}")
            
            data = response.json()
            
            # Log the parsed JSON
            logger.info(f"Parsed JSON: {json.dumps(data, indent=2)}")
            
            # Handle if data is a list
            if isinstance(data, list):
                accounts = data
                logger.info("Data is a list, using as is")
            else:
                accounts = data.get("items", [])
                logger.info(f"Data is an object, extracted 'items' field: {len(accounts)} accounts")
            
            # Log the accounts being returned
            logger.info(f"Returning accounts: {json.dumps(accounts, indent=2)}")
            
            logger.info(f"Retrieved {len(accounts)} accounts")
            return accounts
            
        except Exception as e:
            logger.error(f"Error fetching accounts: {e}")
            return []
    
    def get_account_ids_by_platform(self, platform: str) -> List[int]:
        """Get account IDs for a specific platform (facebook, twitter, etc.).
        
        Args:
            platform (str): The platform name to filter by
            
        Returns:
            List[int]: List of account IDs for the platform or empty list if none found
        """
        accounts = self.get_accounts()
        platform_accounts = []
        
        logger.info(f"Looking for accounts with platform: {platform}")
        
        for account in accounts:
            # Check the 'type' field which contains the platform information
            account_type = account.get("type", "")
            logger.info(f"Account type: {account_type}, looking for: {platform}")
            
            # Use startswith to match platform names like 'twitter.profile', 'facebook.page', etc.
            if account_type.startswith(platform.lower()):
                platform_accounts.append(account.get("id"))
                logger.info(f"Match found! Added account ID: {account.get('id')}")
            # Also check alternative field '_type' which might contain platform names
            elif platform.lower() in account.get("_type", "").lower():
                platform_accounts.append(account.get("id"))
                logger.info(f"Match found via _type field! Added account ID: {account.get('id')}")
        
        logger.info(f"Found {len(platform_accounts)} accounts for platform: {platform}")
        return platform_accounts
    
    def create_post(self, content: str, account_ids: List[int], 
                   platform: str = "twitter", options: Dict[str, Any] = None,
                   retry_count: int = 0) -> Dict[str, Any]:
        """Create a post on specified accounts.
        
        Args:
            content (str): The post content
            account_ids (List[int]): List of account IDs to post to
            platform (str, optional): The platform type (twitter, facebook, etc.)
            options (Dict[str, Any], optional): Platform-specific options
            retry_count (int, optional): Current retry attempt
            
        Returns:
            Dict[str, Any]: Response data or empty dict if failed
        """
        # If in dry run mode, return a mock success response
        if self.dry_run:
            mock_response = {
                "success": True,
                "mock_post": True,
                "platform": platform,
                "account_ids": account_ids,
                "content_preview": content[:100] + "..." if len(content) > 100 else content,
                "timestamp": datetime.utcnow().isoformat(),
                "message": "Dry run mode - no actual post was made"
            }
            logger.info(f"[DRY RUN] Mock post created for platform: {platform}")
            
            # Log the mock post
            self._log_post(mock_response, platform, account_ids, content)
            
            return mock_response
        
        # Real posting logic
        if not self.auth_token and not self.authenticate():
            logger.error("Cannot create post without authentication")
            return {}
        
        # Default options based on platform
        if options is None:
            options = {}
        
        # Prepare the payload
        publish_time = datetime.utcnow() + timedelta(minutes=10)
        publish_at = publish_time.strftime("%Y-%m-%d %H:%M:%S")
        
        payload = {
            "accounts": account_ids,
            "publish_at": publish_at,
            "content": content,
            "draft": False
        }
        
        # Add platform-specific options
        if options:
            payload["options"] = options
        
        url = f"{self.api_base_url}/posts"
        
        try:
            logger.info(f"Creating post for platform: {platform}")
            logger.info(f"Post payload: {json.dumps(payload, indent=2)}")
            response = requests.post(url, headers=self.headers, json=payload)
            
            if response.status_code != 200:
                logger.error(f"Failed to create post: {response.status_code} - {response.text}")
                
                # Retry if we haven't reached max retries
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds (attempt {retry_count + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    return self.create_post(content, account_ids, platform, options, retry_count + 1)
                
                return {}
            
            data = response.json()
            
            # Log the successful post
            self._log_post(data, platform, account_ids, content)
            
            logger.info(f"Successfully created post for platform: {platform}")
            return data
            
        except Exception as e:
            logger.error(f"Error creating post: {e}")
            
            # Retry if we haven't reached max retries
            if retry_count < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds (attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                return self.create_post(content, account_ids, platform, options, retry_count + 1)
            
            return {}
    
    def _log_post(self, response_data: Dict[str, Any], platform: str, 
                 account_ids: List[int], content: str) -> None:
        """Log successful post to post_log.json.
        
        Args:
            response_data (Dict[str, Any]): The API response data
            platform (str): The platform posted to
            account_ids (List[int]): The account IDs posted to
            content (str): The content that was posted
        """
        try:
            # Load existing log
            log_data = json.loads(POST_LOG_FILE.read_text(encoding="utf-8"))
            
            # Generate a unique key for this post based on timestamp
            timestamp = datetime.now().isoformat()
            post_key = f"post_{timestamp}"
            
            # Create log entry
            log_entry = {
                "timestamp": timestamp,
                "platform": platform,
                "account_ids": account_ids,
                "content_preview": content[:100] + "..." if len(content) > 100 else content,
                "response": response_data
            }
            
            # Add to log
            log_data[post_key] = log_entry
            
            # Save log
            POST_LOG_FILE.write_text(json.dumps(log_data, indent=2), encoding="utf-8")
            logger.info(f"Post logged successfully with key: {post_key}")
            
        except Exception as e:
            logger.error(f"Error logging post: {e}")
    
    def logout(self) -> bool:
        """Logout and invalidate the auth token.
        
        Returns:
            bool: True if logout was successful, False otherwise
        """
        if not self.auth_token:
            logger.info("No active session to logout from")
            return True
        
        url = f"{self.api_base_url}/auth/logout"
        
        try:
            logger.info("Logging out from SocialBu API")
            response = requests.post(url, headers=self.headers)
            
            if response.status_code != 200:
                logger.error(f"Logout failed: {response.status_code} - {response.text}")
                return False
            
            # Clear auth token
            self.auth_token = None
            self.headers.pop("Authorization", None)
            
            logger.info("Successfully logged out from SocialBu API")
            return True
            
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Create API client
    api = SocialBuAPI()
    
    # Test authentication
    if api.authenticate():
        print("Authentication successful!")
        
        # Get accounts
        accounts = api.get_accounts()
        print(f"Found {len(accounts)} accounts")
        
        # Get Twitter accounts
        twitter_accounts = api.get_account_ids_by_platform("twitter")
        print(f"Found {len(twitter_accounts)} Twitter accounts")
        
        # Create a test post if we have Twitter accounts
        if twitter_accounts:
            test_content = "This is a test post from the Gnosara automation system."
            response = api.create_post(test_content, twitter_accounts)
            print(f"Post creation result: {response}")
        
        # Logout
        api.logout()
    else:
        print("Authentication failed!")