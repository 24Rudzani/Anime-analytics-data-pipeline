# Cloud_Monitor.py - Modified for local storage monitoring
import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

class LocalStorageMonitor:
    def __init__(self, storage_manager):
        """
        Initialize monitor with local storage manager
        """
        self.storage = storage_manager
        self.monitoring_log = os.path.join(self.storage.base_path, "monitoring_log.json")
        
    def check_storage_health(self):
        """Check health of local storage"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "status": "healthy",
            "directories": {},
            "issues": []
        }
        
        # Check each directory
        for dir_name in ["backups", "reports", "summaries"]:
            dir_path = os.path.join(self.storage.base_path, dir_name)
            
            if os.path.exists(dir_path):
                # Count files and calculate size
                files = os.listdir(dir_path)
                total_size = sum(os.path.getsize(os.path.join(dir_path, f)) 
                               for f in files if os.path.isfile(os.path.join(dir_path, f)))
                
                report["directories"][dir_name] = {
                    "exists": True,
                    "file_count": len(files),
                    "total_size_mb": round(total_size / (1024 * 1024), 2),
                    "writable": os.access(dir_path, os.W_OK)
                }
            else:
                report["directories"][dir_name] = {
                    "exists": False,
                    "file_count": 0,
                    "total_size_mb": 0,
                    "writable": False
                }
                report["issues"].append(f"Directory missing: {dir_name}")
        
        # Log the report
        self._log_monitoring_report(report)
        
        return report
    
    def _log_monitoring_report(self, report):
        """Save monitoring report"""
        try:
            # Load existing log
            if os.path.exists(self.monitoring_log):
                with open(self.monitoring_log, 'r') as f:
                    logs = json.load(f)
            else:
                logs = []
            
            # Add new report (keep last 100)
            logs.append(report)
            if len(logs) > 100:
                logs = logs[-100:]
            
            # Save back
            with open(self.monitoring_log, 'w') as f:
                json.dump(logs, f, indent=2, default=str)
                
        except Exception as e:
            logging.error(f"Failed to save monitoring log: {str(e)}")
    
    def get_storage_stats(self):
        """Get storage statistics"""
        stats = {
            "total_files": 0,
            "total_size_mb": 0,
            "by_folder": {},
            "oldest_file": None,
            "newest_file": None
        }
        
        all_files = []
        
        for folder in ["backups", "reports", "summaries"]:
            folder_path = os.path.join(self.storage.base_path, folder)
            if os.path.exists(folder_path):
                folder_files = []
                for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    if os.path.isfile(file_path):
                        file_stat = os.stat(file_path)
                        file_info = {
                            "name": filename,
                            "size": file_stat.st_size,
                            "modified": datetime.fromtimestamp(file_stat.st_mtime)
                        }
                        folder_files.append(file_info)
                        all_files.append(file_info)
                
                stats["by_folder"][folder] = {
                    "count": len(folder_files),
                    "size_mb": round(sum(f["size"] for f in folder_files) / (1024 * 1024), 2)
                }
                stats["total_files"] += len(folder_files)
                stats["total_size_mb"] += stats["by_folder"][folder]["size_mb"]
        
        if all_files:
            stats["oldest_file"] = min(all_files, key=lambda x: x["modified"])
            stats["newest_file"] = max(all_files, key=lambda x: x["modified"])
        
        return stats