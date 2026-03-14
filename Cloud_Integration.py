# Cloud_Integration.py - Modified for local storage
import os
import shutil
import logging
from datetime import datetime
import pandas as pd

class LocalStorageManager:
    def __init__(self, base_path="local_storage"):
        """
        Initialize local storage manager
        """
        self.base_path = base_path
        self.backups_path = os.path.join(base_path, "backups")
        self.reports_path = os.path.join(base_path, "reports")
        self.summaries_path = os.path.join(base_path, "summaries")
        
        # Create directories if they don't exist
        self._create_directories()
        
    def _create_directories(self):
        """Create necessary local directories"""
        for path in [self.backups_path, self.reports_path, self.summaries_path]:
            os.makedirs(path, exist_ok=True)
            logging.info(f"Ensured directory exists: {path}")
    
    def upload_file(self, local_file_path, destination_subfolder="backups"):
        """
        Copy a file to local storage
        """
        try:
            # Determine destination path
            if destination_subfolder == "backups":
                dest_dir = self.backups_path
            elif destination_subfolder == "reports":
                dest_dir = self.reports_path
            elif destination_subfolder == "summaries":
                dest_dir = self.summaries_path
            else:
                dest_dir = os.path.join(self.base_path, destination_subfolder)
                os.makedirs(dest_dir, exist_ok=True)
            
            # Generate destination filename with timestamp
            filename = os.path.basename(local_file_path)
            name, ext = os.path.splitext(filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_filename = f"{name}_{timestamp}{ext}"
            dest_path = os.path.join(dest_dir, dest_filename)
            
            # Copy file
            shutil.copy2(local_file_path, dest_path)
            logging.info(f"File saved locally: {dest_path}")
            
            return {
                "status": "success",
                "local_path": dest_path,
                "size": os.path.getsize(dest_path)
            }
            
        except Exception as e:
            logging.error(f"Failed to save file locally: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def list_files(self, subfolder="backups"):
        """List files in a local storage folder"""
        try:
            if subfolder == "backups":
                target_dir = self.backups_path
            elif subfolder == "reports":
                target_dir = self.reports_path
            elif subfolder == "summaries":
                target_dir = self.summaries_path
            else:
                target_dir = os.path.join(self.base_path, subfolder)
            
            if not os.path.exists(target_dir):
                return []
            
            files = []
            for filename in os.listdir(target_dir):
                file_path = os.path.join(target_dir, filename)
                if os.path.isfile(file_path):
                    files.append({
                        "name": filename,
                        "size": os.path.getsize(file_path),
                        "modified": datetime.fromtimestamp(os.path.getmtime(file_path))
                    })
            
            return files
            
        except Exception as e:
            logging.error(f"Failed to list files: {str(e)}")
            return []
    
    def get_file(self, filename, subfolder="backups"):
        """Get path to a local file"""
        try:
            if subfolder == "backups":
                file_path = os.path.join(self.backups_path, filename)
            elif subfolder == "reports":
                file_path = os.path.join(self.reports_path, filename)
            elif subfolder == "summaries":
                file_path = os.path.join(self.summaries_path, filename)
            else:
                file_path = os.path.join(self.base_path, subfolder, filename)
            
            if os.path.exists(file_path):
                return file_path
            else:
                logging.warning(f"File not found: {file_path}")
                return None
                
        except Exception as e:
            logging.error(f"Error accessing file: {str(e)}")
            return None