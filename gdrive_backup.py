#!/usr/bin/env python3

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/gdrive_backup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gdrive_backup")

# Constants
SUMMARIES_DIR = Path("summaries")
GDRIVE_BACKUP_LOG = Path("logs/gdrive_backup_log.json")

class GDriveBackup:
    """Handles backing up summaries to Google Drive."""
    
    def __init__(self, creds_path: Optional[str] = None):
        """Initialize Google Drive backup utility.
        
        Args:
            creds_path (str, optional): Path to credentials JSON file
        """
        self.creds_path = creds_path or os.getenv("GDRIVE_CREDS_JSON")
        self.drive = None
        
        # Ensure logs directory exists
        Path("logs").mkdir(exist_ok=True)
        
        # Initialize backup log if it doesn't exist
        if not GDRIVE_BACKUP_LOG.exists():
            GDRIVE_BACKUP_LOG.write_text(json.dumps({}), encoding="utf-8")
        
        logger.info("GDrive backup utility initialized")
    
    def authenticate(self) -> bool:
        """Authenticate with Google Drive API.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        try:
            from pydrive.auth import GoogleAuth
            from pydrive.drive import GoogleDrive
            
            if not self.creds_path:
                logger.error("No credentials path provided")
                return False
            
            logger.info("Authenticating with Google Drive API")
            gauth = GoogleAuth()
            
            # Try to load credentials from file
            if os.path.exists(self.creds_path):
                gauth.LoadCredentialsFile(self.creds_path)
            
            if gauth.credentials is None:
                logger.error("No valid credentials found")
                return False
            elif gauth.access_token_expired:
                logger.info("Access token expired, refreshing")
                gauth.Refresh()
                gauth.SaveCredentialsFile(self.creds_path)
            
            # Create Drive client
            self.drive = GoogleDrive(gauth)
            logger.info("Successfully authenticated with Google Drive API")
            return True
            
        except ImportError:
            logger.error("PyDrive not installed, run: pip install pydrive")
            return False
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    def get_or_create_folder(self, folder_name: str, parent_id: Optional[str] = 'root') -> Optional[str]:
        """Get or create a folder in Google Drive.
        
        Args:
            folder_name (str): Name of the folder to get or create
            parent_id (str, optional): ID of the parent folder
            
        Returns:
            Optional[str]: Folder ID if found or created, None otherwise
        """
        if not self.drive:
            if not self.authenticate():
                return None
        
        try:
            # Check if folder exists
            query = f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed=false"
            file_list = self.drive.ListFile({'q': query}).GetList()
            
            if file_list:
                logger.info(f"Found existing folder: {folder_name}")
                return file_list[0]['id']
            
            # Create folder if it doesn't exist
            folder = self.drive.CreateFile({
                'title': folder_name,
                'parents': [{'id': parent_id}],
                'mimeType': 'application/vnd.google-apps.folder'
            })
            folder.Upload()
            
            logger.info(f"Created new folder: {folder_name}")
            return folder['id']
            
        except Exception as e:
            logger.error(f"Error getting/creating folder {folder_name}: {e}")
            return None
    
    def create_backup_folder_structure(self) -> Optional[str]:
        """Create the folder structure for backups.
        
        Returns:
            Optional[str]: ID of the deepest folder for today, None if failed
        """
        if not self.drive:
            if not self.authenticate():
                return None
        
        try:
            # Get or create Gnosara root folder
            gnosara_id = self.get_or_create_folder("Gnosara-Backups")
            if not gnosara_id:
                return None
            
            # Get or create summaries folder
            summaries_id = self.get_or_create_folder("summaries", gnosara_id)
            if not summaries_id:
                return None
            
            # Get or create folder for current year
            year = datetime.now().strftime("%Y")
            year_id = self.get_or_create_folder(year, summaries_id)
            if not year_id:
                return None
            
            # Get or create folder for current month
            month = datetime.now().strftime("%m")
            month_id = self.get_or_create_folder(month, year_id)
            if not month_id:
                return None
            
            # Get or create folder for current day
            day = datetime.now().strftime("%d")
            day_id = self.get_or_create_folder(day, month_id)
            if not day_id:
                return None
            
            logger.info(f"Created/verified backup folder structure: Gnosara-Backups/summaries/{year}/{month}/{day}")
            return day_id
            
        except Exception as e:
            logger.error(f"Error creating backup folder structure: {e}")
            return None
    
    def get_files_to_backup(self) -> List[Path]:
        """Get list of files that need to be backed up.
        
        Returns:
            List[Path]: List of file paths to backup
        """
        if not SUMMARIES_DIR.exists():
            logger.warning(f"Summaries directory {SUMMARIES_DIR} does not exist")
            return []
        
        try:
            # Load backup log
            backup_log = json.loads(GDRIVE_BACKUP_LOG.read_text(encoding="utf-8"))
            
            # Get all JSON files in the summaries directory
            files = list(SUMMARIES_DIR.glob("*.json"))
            
            # Filter out already backed up files
            to_backup = []
            for file_path in files:
                file_key = str(file_path)
                if file_key not in backup_log or not backup_log[file_key].get("backed_up", False):
                    to_backup.append(file_path)
            
            logger.info(f"Found {len(to_backup)} files to backup")
            return to_backup
            
        except Exception as e:
            logger.error(f"Error getting files to backup: {e}")
            return []
    
    def backup_files(self, files: List[Path]) -> Dict[str, bool]:
        """Backup files to Google Drive.
        
        Args:
            files (List[Path]): List of file paths to backup
            
        Returns:
            Dict[str, bool]: Dictionary mapping file paths to backup success
        """
        if not files:
            logger.info("No files to backup")
            return {}
        
        # Create backup folder structure
        folder_id = self.create_backup_folder_structure()
        if not folder_id:
            logger.error("Failed to create backup folder structure")
            return {str(f): False for f in files}
        
        # Load existing backup log
        try:
            backup_log = json.loads(GDRIVE_BACKUP_LOG.read_text(encoding="utf-8"))
        except Exception:
            backup_log = {}
        
        # Track results
        results = {}
        
        # Upload each file
        for file_path in files:
            try:
                if not self.drive:
                    if not self.authenticate():
                        results[str(file_path)] = False
                        continue
                
                # Create Drive file
                drive_file = self.drive.CreateFile({
                    'title': file_path.name,
                    'parents': [{'id': folder_id}]
                })
                
                # Set content
                drive_file.SetContentFile(str(file_path))
                
                # Upload
                drive_file.Upload()
                
                # Record success
                results[str(file_path)] = True
                
                # Update backup log
                backup_log[str(file_path)] = {
                    "backed_up": True,
                    "backup_time": datetime.now().isoformat(),
                    "drive_file_id": drive_file['id']
                }
                
                logger.info(f"Successfully backed up {file_path.name}")
                
            except Exception as e:
                logger.error(f"Error backing up {file_path.name}: {e}")
                results[str(file_path)] = False
        
        # Save updated backup log
        try:
            GDRIVE_BACKUP_LOG.write_text(json.dumps(backup_log, indent=2), encoding="utf-8")
            logger.info(f"Updated backup log with {len(results)} entries")
        except Exception as e:
            logger.error(f"Error saving backup log: {e}")
        
        return results
    
    def backup_all_summaries(self) -> Dict[str, Any]:
        """Backup all summaries to Google Drive.
        
        Returns:
            Dict[str, Any]: Backup report
        """
        logger.info("Starting backup of all summaries")
        
        # Get files to backup
        files = self.get_files_to_backup()
        
        if not files:
            logger.info("No files to backup")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "no_files",
                "message": "No files to backup",
                "backed_up": 0,
                "failed": 0
            }
        
        # Backup files
        results = self.backup_files(files)
        
        # Compile report
        success_count = sum(1 for success in results.values() if success)
        failed_count = len(results) - success_count
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "status": "complete",
            "message": f"Backed up {success_count} files, {failed_count} failed",
            "backed_up": success_count,
            "failed": failed_count,
            "details": results
        }
        
        logger.info(f"Backup complete: {success_count} succeeded, {failed_count} failed")
        return report


# Example usage
if __name__ == "__main__":
    import argparse
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Google Drive Backup Utility")
    parser.add_argument("--creds", type=str, default=None, help="Path to Google Drive credentials JSON file")
    args = parser.parse_args()
    
    # Create backup utility
    backup = GDriveBackup(args.creds)
    
    # Backup all summaries
    report = backup.backup_all_summaries()
    
    # Print report
    print(json.dumps(report, indent=2))