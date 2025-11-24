# Cloud_Monitor.py
import json
import os
from datetime import datetime
import pandas as pd

def monitor_cloud_storage():
    """Monitor cloud storage simulation"""
    print("CLOUD STORAGE MONITORING DASHBOARD")
    print("=" * 60)
    
    # Check simulated storage
    storage_dir = "cloud_simulated_storage"
    
    if not os.path.exists(storage_dir):
        print("Cloud storage not initialized")
        return
    
    # Storage statistics
    total_size = 0
    file_count = 0
    backup_count = 0
    summary_count = 0
    report_count = 0
    
    for root, dirs, files in os.walk(storage_dir):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            total_size += file_size
            file_count += 1
            
            # Categorize files
            if 'backups' in file_path:
                backup_count += 1
            elif 'summaries' in file_path:
                summary_count += 1
            elif 'reports' in file_path:
                report_count += 1
    
    print(f"STORAGE STATISTICS:")
    print(f"   Total Files: {file_count}")
    print(f"   Total Size: {total_size / (1024*1024):.2f} MB")
    print(f"   Backup Files: {backup_count}")
    print(f"   Summary Files: {summary_count}")
    print(f"   Report Files: {report_count}")
    
    # Check backup operations log
    log_file = os.path.join(storage_dir, "logs", "s3_operations.log")
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            operations = [json.loads(line) for line in f.readlines()]
        
        successful_ops = len([op for op in operations if op['status'] == 'SUCCESS'])
        failed_ops = len([op for op in operations if op['status'] == 'FAILED'])
        
        print(f"\nOPERATION METRICS:")
        print(f"   Total Operations: {len(operations)}")
        print(f"   Successful: {successful_ops}")
        print(f"   Failed: {failed_ops}")
        if operations:
            print(f"   Success Rate: {(successful_ops/len(operations))*100:.1f}%")
    
    # List recent backup sessions
    buckets_dir = os.path.join(storage_dir, "buckets", "anime-data-pipeline-prod", "backups")
    if os.path.exists(buckets_dir):
        backup_sessions = [f for f in os.listdir(buckets_dir) if os.path.isdir(os.path.join(buckets_dir, f))]
        backup_sessions.sort(reverse=True)
        
        print(f"\nRECENT BACKUP SESSIONS ({len(backup_sessions)} total):")
        for session in backup_sessions[:5]:  # Show last 5
            session_path = os.path.join(buckets_dir, session)
            files = os.listdir(session_path)
            session_size = sum(os.path.getsize(os.path.join(session_path, f)) for f in files)
            print(f"   {session}: {len(files)} files, {session_size/(1024*1024):.2f} MB")
    
    # Check metadata files
    metadata_dir = os.path.join(storage_dir, "metadata")
    if os.path.exists(metadata_dir):
        metadata_files = [f for f in os.listdir(metadata_dir) if f.endswith('.json')]
        print(f"\nMETADATA:")
        print(f"   Metadata Files: {len(metadata_files)}")
    
    print("\nCLOUD SIMULATION STATUS: ACTIVE")
    print("READY FOR PRODUCTION AWS MIGRATION")
    print("=" * 60)

if __name__ == "__main__":
    monitor_cloud_storage()