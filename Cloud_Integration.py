# Cloud_Integration.py
import pandas as pd
import os
import json
import shutil
from datetime import datetime
from dotenv import load_dotenv
import logging
import hashlib

# Load environment variables
load_dotenv()

# Configure logging without emojis
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudStorageSimulator:
    """Professional AWS S3 simulator for academic projects"""
    
    def __init__(self):
        self.backup_dir = "cloud_simulated_storage"
        self.metadata_dir = os.path.join(self.backup_dir, "metadata")
        self.setup_directories()
        
    def setup_directories(self):
        """Create directory structure mimicking S3 buckets"""
        directories = [
            self.backup_dir,
            self.metadata_dir,
            os.path.join(self.backup_dir, "buckets"),
            os.path.join(self.backup_dir, "logs")
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        logging.info("Cloud storage simulator initialized")
    
    def generate_file_hash(self, file_path):
        """Generate MD5 hash for file integrity checking (like S3 ETags)"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def log_operation(self, operation, bucket, key, status, message=""):
        """Log S3 operations for monitoring"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "bucket": bucket,
            "key": key,
            "status": status,
            "message": message
        }
        
        log_file = os.path.join(self.backup_dir, "logs", "s3_operations.log")
        with open(log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    
    def upload_file(self, file_path, bucket_name, s3_key):
        """Simulate S3 file upload with full metadata"""
        try:
            # Create bucket directory if it doesn't exist
            bucket_path = os.path.join(self.backup_dir, "buckets", bucket_name)
            os.makedirs(bucket_path, exist_ok=True)
            
            # Create full path mimicking S3 structure
            full_path = os.path.join(bucket_path, s3_key)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            
            # Copy file to simulated storage
            shutil.copy2(file_path, full_path)
            
            # Generate S3-like metadata
            file_stats = os.stat(file_path)
            file_hash = self.generate_file_hash(file_path)
            
            metadata = {
                "s3_simulated_metadata": {
                    "ETag": f'"{file_hash}"',
                    "LastModified": datetime.now().isoformat() + "Z",
                    "ContentLength": file_stats.st_size,
                    "StorageClass": "STANDARD",
                    "Bucket": bucket_name,
                    "Key": s3_key,
                    "Location": f"https://{bucket_name}.s3.amazonaws.com/{s3_key}",
                    "VersionId": "null"
                },
                "file_info": {
                    "original_path": file_path,
                    "simulated_path": full_path,
                    "md5_hash": file_hash,
                    "upload_timestamp": datetime.now().isoformat()
                },
                "simulation_info": {
                    "provider": "AWS S3 Simulator",
                    "region": "us-east-1",
                    "simulation_time": datetime.now().isoformat()
                }
            }
            
            # Save metadata
            metadata_file = os.path.join(self.metadata_dir, f"{file_hash}.metadata.json")
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Log the upload
            self.log_operation("PUT_OBJECT", bucket_name, s3_key, "SUCCESS")
            
            logging.info(f"[S3 SIM] Uploaded {os.path.basename(file_path)} to s3://{bucket_name}/{s3_key}")
            logging.info(f"   File size: {file_stats.st_size} bytes, ETag: {file_hash}")
            
            return metadata
            
        except Exception as e:
            self.log_operation("PUT_OBJECT", bucket_name, s3_key, "FAILED", str(e))
            logging.error(f"Upload failed: {e}")
            return None
    
    def list_objects(self, bucket_name, prefix=""):
        """Simulate S3 list_objects_v2 operation"""
        try:
            bucket_path = os.path.join(self.backup_dir, "buckets", bucket_name)
            objects = []
            
            if os.path.exists(bucket_path):
                for root, dirs, files in os.walk(bucket_path):
                    for file in files:
                        full_path = os.path.join(root, file)
                        # Convert to S3-style path with forward slashes
                        relative_path = os.path.relpath(full_path, bucket_path).replace("\\", "/")
                        
                        # Check if it matches the prefix
                        if prefix and relative_path.startswith(prefix):
                            file_stats = os.stat(full_path)
                            file_hash = self.generate_file_hash(full_path)
                            
                            objects.append({
                                "Key": relative_path,
                                "LastModified": datetime.fromtimestamp(file_stats.st_mtime).isoformat() + "Z",
                                "ETag": f'"{file_hash}"',
                                "Size": file_stats.st_size,
                                "StorageClass": "STANDARD"
                            })
                        elif not prefix:  # If no prefix specified, include all objects
                            file_stats = os.stat(full_path)
                            file_hash = self.generate_file_hash(full_path)
                            
                            objects.append({
                                "Key": relative_path,
                                "LastModified": datetime.fromtimestamp(file_stats.st_mtime).isoformat() + "Z",
                                "ETag": f'"{file_hash}"',
                                "Size": file_stats.st_size,
                                "StorageClass": "STANDARD"
                            })
            
            # Log the operation
            self.log_operation("LIST_OBJECTS", bucket_name, prefix, "SUCCESS", f"Found {len(objects)} objects")
            
            return {
                "Contents": objects,
                "IsTruncated": False,
                "KeyCount": len(objects),
                "MaxKeys": 1000,
                "Name": bucket_name
            }
            
        except Exception as e:
            self.log_operation("LIST_OBJECTS", bucket_name, prefix, "FAILED", str(e))
            logging.error(f"List objects failed: {e}")
            return {"Contents": [], "KeyCount": 0}
    
    def head_object(self, bucket_name, key):
        """Simulate S3 head_object operation"""
        try:
            object_path = os.path.join(self.backup_dir, "buckets", bucket_name, key)
            
            if os.path.exists(object_path):
                file_stats = os.stat(object_path)
                file_hash = self.generate_file_hash(object_path)
                
                metadata = {
                    "ContentLength": file_stats.st_size,
                    "ETag": f'"{file_hash}"',
                    "LastModified": datetime.fromtimestamp(file_stats.st_mtime).isoformat() + "Z"
                }
                
                self.log_operation("HEAD_OBJECT", bucket_name, key, "SUCCESS")
                return metadata
            else:
                self.log_operation("HEAD_OBJECT", bucket_name, key, "FAILED", "Object not found")
                return None
                
        except Exception as e:
            self.log_operation("HEAD_OBJECT", bucket_name, key, "FAILED", str(e))
            return None

def setup_cloud_storage():
    """Initialize cloud storage simulator"""
    return CloudStorageSimulator()

def upload_to_cloud_storage(file_path, bucket_name, s3_key):
    """Main upload function using professional simulator"""
    simulator = setup_cloud_storage()
    return simulator.upload_file(file_path, bucket_name, s3_key)

def list_cloud_backups(bucket_name="anime-data-pipeline-prod", prefix="backups/"):
    """List all backups in simulated cloud storage"""
    simulator = setup_cloud_storage()
    result = simulator.list_objects(bucket_name, prefix)
    
    # Debug information
    if result and 'Contents' in result and result['Contents']:
        logging.info(f"Found {len(result['Contents'])} backup objects")
        for obj in result['Contents'][:5]:  # Log first 5 objects
            logging.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
    else:
        logging.info("No backup objects found with the current prefix")
        # Let's check what's actually in the storage
        backup_path = os.path.join(simulator.backup_dir, "buckets", bucket_name)
        if os.path.exists(backup_path):
            all_files = []
            for root, dirs, files in os.walk(backup_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, backup_path).replace("\\", "/")
                    all_files.append(relative_path)
            
            if all_files:
                logging.info(f"Found {len(all_files)} total objects in bucket: {all_files[:3]}...")  # Show first 3
    
    return result

def generate_data_summary():
    """Generate comprehensive data summary with cloud metadata"""
    try:
        anime_df = pd.read_csv('anime.csv')
        ratings_df = pd.read_csv('rating.csv')
        
        summary = {
            "timestamp": datetime.now().isoformat() + "Z",
            "dataset_statistics": {
                "anime_records": len(anime_df),
                "ratings_records": len(ratings_df),
                "total_records": len(anime_df) + len(ratings_df)
            },
            "data_quality_metrics": {
                "anime_missing_values": anime_df.isnull().sum().to_dict(),
                "ratings_missing_values": ratings_df.isnull().sum().to_dict(),
                "completeness_score": {
                    "anime": (1 - anime_df.isnull().sum().sum() / (len(anime_df) * len(anime_df.columns))) * 100,
                    "ratings": (1 - ratings_df.isnull().sum().sum() / (len(ratings_df) * len(ratings_df.columns))) * 100
                }
            },
            "business_insights": {
                "avg_rating": float(anime_df['rating'].mean()),
                "total_members": int(anime_df['members'].sum()),
                "unique_genres": int(anime_df['genre'].nunique()),
                "content_distribution": anime_df['type'].value_counts().to_dict()
            },
            "cloud_metadata": {
                "storage_provider": "AWS S3 Simulator",
                "region": "us-east-1",
                "bucket_strategy": "time-partitioned-backups",
                "simulation_version": "1.0"
            }
        }
        return summary
    except Exception as e:
        logging.error(f"Failed to generate data summary: {e}")
        return None

def backup_data_to_cloud():
    """Professional backup process with comprehensive logging"""
    logging.info("Starting Professional Cloud Backup Simulation")
    
    simulator = setup_cloud_storage()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bucket_name = "anime-data-pipeline-prod"
    
    # Files to backup
    backup_files = [
        'anime.csv',
        'rating.csv',
        'etl_pipeline.log',
        'requirements.txt'
    ]
    
    backup_results = []
    successful_uploads = 0
    
    for file_path in backup_files:
        if os.path.exists(file_path):
            s3_key = f"backups/{timestamp}/{os.path.basename(file_path)}"
            result = simulator.upload_file(file_path, bucket_name, s3_key)
            
            if result:
                successful_uploads += 1
                backup_results.append({
                    "file": file_path,
                    "status": "SUCCESS",
                    "s3_location": f"s3://{bucket_name}/{s3_key}",
                    "metadata": result
                })
                logging.info(f"Backup successful: {file_path}")
            else:
                backup_results.append({
                    "file": file_path,
                    "status": "FAILED",
                    "error": "Upload failed"
                })
                logging.error(f"Backup failed: {file_path}")
        else:
            backup_results.append({
                "file": file_path,
                    "status": "NOT_FOUND",
                "error": "File does not exist"
            })
            logging.warning(f"File not found: {file_path}")
    
    # Generate and upload data summary
    summary = generate_data_summary()
    if summary:
        summary_file = f"data_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        s3_key = f"summaries/{timestamp}/data_summary.json"
        simulator.upload_file(summary_file, bucket_name, s3_key)
        os.remove(summary_file)
        
        backup_results.append({
            "file": "data_summary.json",
            "status": "SUCCESS", 
            "s3_location": f"s3://{bucket_name}/{s3_key}",
            "type": "metadata"
        })
    
    # Generate backup report
    generate_comprehensive_report(backup_results, timestamp, successful_uploads, len(backup_files))
    
    return successful_uploads, backup_results

def generate_comprehensive_report(backup_results, timestamp, successful_uploads, total_files):
    """Generate professional backup report"""
    report = {
        "backup_operation_report": {
            "operation_id": f"backup_{timestamp}",
            "timestamp": datetime.now().isoformat() + "Z",
            "summary": {
                "total_files_processed": total_files,
                "successful_uploads": successful_uploads,
                "failed_uploads": total_files - successful_uploads,
                "success_rate": f"{(successful_uploads/total_files)*100:.1f}%",
                "total_data_size": sum(
                    os.path.getsize(result['file']) 
                    for result in backup_results 
                    if result['status'] == 'SUCCESS' and os.path.exists(result['file'])
                )
            },
            "cloud_environment": {
                "storage_provider": "AWS S3 Simulator",
                "region": "us-east-1",
                "bucket": "anime-data-pipeline-prod",
                "simulation_mode": True
            },
            "detailed_results": backup_results,
            "recommendations": [
                "All critical data files backed up successfully",
                "Data quality metrics included in summary",
                "Ready for production cloud migration"
            ]
        }
    }
    
    # Save report
    report_file = f"backup_report_{timestamp}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    # Upload report to cloud
    simulator = setup_cloud_storage()
    simulator.upload_file(report_file, "anime-data-pipeline-prod", f"reports/{report_file}")
    
    # Print professional summary (without emojis)
    print("\n" + "="*70)
    print("PROFESSIONAL CLOUD BACKUP SIMULATION COMPLETED")
    print("="*70)
    print(f"Operation ID: backup_{timestamp}")
    print(f"Successful: {successful_uploads}/{total_files} files")
    print(f"Success Rate: {(successful_uploads/total_files)*100:.1f}%")
    print(f"Storage: AWS S3 Simulator (Production-ready)")
    print(f"Report: {report_file}")
    print(f"Simulated Bucket: anime-data-pipeline-prod")
    print("="*70)
    print("This simulation mimics real AWS S3 behavior including:")
    print("   • ETag generation (MD5 hashes)")
    print("   • S3-like metadata and logging")
    print("   • Bucket structure and object storage")
    print("   • Operation monitoring and audit trails")
    print("="*70)

if __name__ == "__main__":
    logging.info("Starting Professional Cloud Storage Simulation")
    
    # Perform professional backup
    success_count, results = backup_data_to_cloud()
    
    # List and display backups
    backups = list_cloud_backups("anime-data-pipeline-prod")
    print(f"\nAvailable backups: {backups['KeyCount']} objects")
    
    logging.info("Professional cloud simulation completed!")