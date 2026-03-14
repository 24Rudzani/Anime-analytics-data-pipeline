import pandas as pd
import logging
from datetime import datetime
import os
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

class LocalStorageManager:
    """Simple local storage manager for ETL outputs"""
    
    def __init__(self, base_path="local_storage"):
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
    
    def save_dataframe(self, df, filename, subfolder="backups", format="parquet"):
        """Save dataframe to local storage"""
        try:
            # Determine destination
            if subfolder == "backups":
                dest_dir = self.backups_path
            elif subfolder == "reports":
                dest_dir = self.reports_path
            elif subfolder == "summaries":
                dest_dir = self.summaries_path
            else:
                dest_dir = os.path.join(self.base_path, subfolder)
                os.makedirs(dest_dir, exist_ok=True)
            
            # Add timestamp to filename
            name, ext = os.path.splitext(filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format == "parquet":
                dest_filename = f"{name}_{timestamp}.parquet"
                dest_path = os.path.join(dest_dir, dest_filename)
                df.to_parquet(dest_path, index=False)
            else:  # csv
                dest_filename = f"{name}_{timestamp}.csv"
                dest_path = os.path.join(dest_dir, dest_filename)
                df.to_csv(dest_path, index=False)
            
            logging.info(f"Saved {len(df)} records to {dest_path}")
            
            return {
                "status": "success",
                "path": dest_path,
                "records": len(df),
                "size": os.path.getsize(dest_path)
            }
            
        except Exception as e:
            logging.error(f"Failed to save file: {str(e)}")
            return {"status": "failed", "error": str(e)}
    
    def save_json(self, data, filename, subfolder="summaries"):
        """Save JSON data to local storage"""
        try:
            if subfolder == "summaries":
                dest_dir = self.summaries_path
            else:
                dest_dir = os.path.join(self.base_path, subfolder)
                os.makedirs(dest_dir, exist_ok=True)
            
            # Add timestamp to filename
            name, ext = os.path.splitext(filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_filename = f"{name}_{timestamp}.json"
            dest_path = os.path.join(dest_dir, dest_filename)
            
            with open(dest_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logging.info(f"Saved JSON to {dest_path}")
            
            return {
                "status": "success",
                "path": dest_path,
                "size": os.path.getsize(dest_path)
            }
            
        except Exception as e:
            logging.error(f"Failed to save JSON: {str(e)}")
            return {"status": "failed", "error": str(e)}

def extract():
    """Extract data from source CSV files"""
    logging.info("EXTRACT: Reading source CSV files...")
    
    try:
        # Check if files exist
        if not os.path.exists('anime.csv'):
            logging.error("anime.csv not found in current directory")
            raise FileNotFoundError("anime.csv is missing")
        
        if not os.path.exists('rating.csv'):
            logging.error("rating.csv not found in current directory")
            raise FileNotFoundError("rating.csv is missing")
        
        # Read anime data
        anime_df = pd.read_csv('anime.csv')
        logging.info(f"Extracted {len(anime_df)} anime records")
        
        # Read ratings data
        ratings_df = pd.read_csv('rating.csv')
        
        # Take a sample for ETL demonstration (optional - remove if you want all data)
        ratings_sample = ratings_df.sample(n=min(100000, len(ratings_df)), random_state=42)
        logging.info(f"Extracted {len(ratings_sample)} ratings records (sampled from {len(ratings_df)} total)")
        
        return anime_df, ratings_sample
        
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

def transform(anime_df, ratings_df):
    """Transform and clean the data"""
    logging.info("TRANSFORM: Cleaning and transforming data...")
    
    # Anime data transformations
    anime_clean = anime_df.copy()
    
    # Handle missing values
    anime_clean['genre'] = anime_clean['genre'].fillna('Unknown')
    anime_clean['type'] = anime_clean['type'].fillna('Unknown')
    
    # Clean numeric columns
    def clean_numeric(value):
        try:
            if pd.notna(value) and str(value).replace('.', '').replace('-', '').isdigit():
                return float(value)
            return None
        except:
            return None
    
    anime_clean['episodes'] = anime_clean['episodes'].apply(clean_numeric)
    anime_clean['rating'] = anime_clean['rating'].apply(clean_numeric)
    anime_clean['members'] = anime_clean['members'].apply(clean_numeric)
    
    # Create new calculated columns
    anime_clean['popularity_score'] = anime_clean['rating'] * (anime_clean['members'] / 100000)
    anime_clean['popularity_score'] = anime_clean['popularity_score'].fillna(0)
    
    # Add transformation timestamp
    anime_clean['etl_processed_date'] = datetime.now().date()
    
    # Ratings data transformations
    ratings_clean = ratings_df.copy()
    
    # Filter out invalid ratings (-1 typically means "no rating")
    ratings_clean = ratings_clean[ratings_clean['rating'] != -1]
    
    # Add data quality flags
    ratings_clean['is_high_rating'] = ratings_clean['rating'] >= 8
    ratings_clean['rating_date'] = datetime.now().date()  # Simulate rating date
    
    logging.info(f"Transformed {len(anime_clean)} anime records")
    logging.info(f"Transformed {len(ratings_clean)} ratings records")
    
    return anime_clean, ratings_clean

def generate_quality_report(anime_df, ratings_df):
    """Generate data quality report"""
    report = {
        "etl_timestamp": datetime.now().isoformat(),
        "data_summary": {
            "anime": {
                "total_records": len(anime_df),
                "columns": list(anime_df.columns),
                "missing_values": anime_df.isnull().sum().to_dict(),
                "data_types": anime_df.dtypes.astype(str).to_dict()
            },
            "ratings": {
                "total_records": len(ratings_df),
                "unique_users": ratings_df['user_id'].nunique(),
                "unique_anime": ratings_df['anime_id'].nunique(),
                "rating_distribution": ratings_df['rating'].value_counts().head(10).to_dict()
            }
        },
        "key_metrics": {
            "avg_anime_rating": float(anime_df['rating'].mean()),
            "avg_user_rating": float(ratings_df['rating'].mean()),
            "total_members_sum": int(anime_df['members'].sum()),
            "high_rated_anime": int((anime_df['rating'] >= 8).sum())
        }
    }
    return report

def load_local(anime_df, ratings_df, storage_manager):
    """Load transformed data to local storage"""
    logging.info("LOAD: Saving data to local storage...")
    
    try:
        # 1. Save transformed data as backups
        logging.info("Saving transformed data to backups...")
        anime_result = storage_manager.save_dataframe(
            anime_df, 
            "anime_transformed.parquet", 
            "backups",
            format="parquet"
        )
        
        ratings_result = storage_manager.save_dataframe(
            ratings_df, 
            "ratings_transformed.parquet", 
            "backups",
            format="parquet"
        )
        
        # 2. Generate and save quality report
        logging.info("Generating quality report...")
        quality_report = generate_quality_report(anime_df, ratings_df)
        
        report_result = storage_manager.save_json(
            quality_report,
            "quality_report.json",
            "reports"
        )
        
        # 3. Create summary statistics
        logging.info("Creating summary statistics...")
        summary = {
            "extraction": {
                "timestamp": datetime.now().isoformat(),
                "anime_records": len(anime_df),
                "ratings_records": len(ratings_df),
                "unique_anime": anime_df['anime_id'].nunique(),
                "unique_users": ratings_df['user_id'].nunique()
            },
            "transformations": {
                "anime_columns_added": ["popularity_score", "etl_processed_date"],
                "ratings_columns_added": ["is_high_rating", "rating_date"],
                "invalid_ratings_removed": len(ratings_df) - len(ratings_df[ratings_df['rating'] != -1])
            },
            "storage": {
                "anime_backup": anime_result,
                "ratings_backup": ratings_result,
                "quality_report": report_result
            }
        }
        
        summary_result = storage_manager.save_json(
            summary,
            "etl_summary.json",
            "summaries"
        )
        
        logging.info("All data successfully saved to local storage")
        
        return {
            "success": True,
            "backups": {
                "anime": anime_result,
                "ratings": ratings_result
            },
            "reports": {
                "quality": report_result,
                "summary": summary_result
            }
        }
        
    except Exception as e:
        logging.error(f"Load to local storage failed: {e}")
        return {"success": False, "error": str(e)}

def main():
    """Main ETL pipeline function"""
    logging.info("=" * 50)
    logging.info("Starting ETL Pipeline (Local Storage Mode)")
    logging.info("=" * 50)
    
    # Initialize local storage
    storage = LocalStorageManager(base_path="local_storage")
    
    try:
        # EXTRACT
        anime_data, ratings_data = extract()
        
        # TRANSFORM
        anime_clean, ratings_clean = transform(anime_data, ratings_data)
        
        # LOAD (to local storage)
        result = load_local(anime_clean, ratings_clean, storage)
        
        if result["success"]:
            logging.info("=" * 50)
            logging.info("ETL Pipeline completed successfully!")
            logging.info("=" * 50)
            
            # Print summary to console
            print("\n" + "=" * 60)
            print("📊 ETL PIPELINE EXECUTION SUMMARY")
            print("=" * 60)
            print(f"✅ EXTRACT:")
            print(f"   • Anime: {len(anime_data):,} records")
            print(f"   • Ratings: {len(ratings_data):,} records")
            print(f"\n✅ TRANSFORM:")
            print(f"   • Cleaned anime: {len(anime_clean):,} records")
            print(f"   • Cleaned ratings: {len(ratings_clean):,} records")
            print(f"   • Added popularity scores & quality flags")
            print(f"\n✅ LOAD (Local Storage):")
            print(f"   • Backups: local_storage/backups/")
            print(f"   • Reports: local_storage/reports/")
            print(f"   • Summaries: local_storage/summaries/")
            print("=" * 60)
            print("\n📁 To view your data:")
            print("   1. In the Codespace file explorer, navigate to 'local_storage'")
            print("   2. Check subfolders: backups/, reports/, summaries/")
            print("   3. View logs: etl_pipeline.log")
            print("=" * 60)
            
        else:
            logging.error(f"ETL Pipeline failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")
        print(f"\n❌ Error: {e}")
        print("Check etl_pipeline.log for details")

if __name__ == "__main__":
    main()