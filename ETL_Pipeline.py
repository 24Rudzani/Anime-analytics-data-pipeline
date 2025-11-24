import oracledb
import pandas as pd
import logging
from datetime import datetime
import os

# Set up logging without emojis
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

def get_connection():
    """Create connection to Oracle database"""
    return oracledb.connect(
        user='YOUR_USERNAME_HERE',
        password='YOUR_PASSWORD_HERE', 
        dsn='YOUR_DSN_HERE'
    )

def extract():
    """Extract data from source CSV files"""
    logging.info("EXTRACT: Reading source CSV files...")
    
    try:
        # Read anime data - using correct path
        anime_df = pd.read_csv('anime.csv')
        logging.info(f"Extracted {len(anime_df)} anime records")
        
        # Read ratings data (sample for development)
        ratings_df = pd.read_csv('rating.csv')
        # Take a larger sample for ETL demonstration
        ratings_sample = ratings_df.sample(n=100000, random_state=42)
        logging.info(f"Extracted {len(ratings_sample)} ratings records")
        
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
            return float(value) if pd.notna(value) and str(value).replace('.', '').isdigit() else None
        except:
            return None
    
    anime_clean['episodes'] = anime_clean['episodes'].apply(clean_numeric)
    anime_clean['rating'] = anime_clean['rating'].apply(clean_numeric)
    anime_clean['members'] = anime_clean['members'].apply(clean_numeric)
    
    # Create new calculated columns
    anime_clean['popularity_score'] = anime_clean['rating'] * (anime_clean['members'] / 100000)
    anime_clean['popularity_score'] = anime_clean['popularity_score'].fillna(0)
    
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

def load(anime_df, ratings_df):
    """Load transformed data into database"""
    logging.info("LOAD: Loading data into Oracle database...")
    
    connection = get_connection()
    cursor = connection.cursor()
    
    try:
        # Drop staging tables if they exist
        try:
            cursor.execute("DROP TABLE anime_staging")
        except:
            pass
        try:
            cursor.execute("DROP TABLE ratings_staging")
        except:
            pass
        
        # Create staging tables for ETL
        cursor.execute("""
            CREATE TABLE anime_staging (
                anime_id NUMBER PRIMARY KEY,
                name VARCHAR2(255),
                genre VARCHAR2(500),
                type VARCHAR2(50),
                episodes NUMBER,
                rating NUMBER,
                members NUMBER,
                popularity_score NUMBER,
                load_timestamp DATE DEFAULT SYSDATE
            )
        """)
        
        cursor.execute("""
            CREATE TABLE ratings_staging (
                user_id NUMBER,
                anime_id NUMBER,
                rating NUMBER,
                is_high_rating NUMBER(1),
                rating_date DATE,
                load_timestamp DATE DEFAULT SYSDATE
            )
        """)
        
        logging.info("Created staging tables")
        
        # Load anime data
        anime_tuples = []
        for row in anime_df.itertuples(index=False):
            anime_tuples.append((
                int(row.anime_id),
                str(row.name)[:255],
                str(row.genre)[:500],
                str(row.type)[:50],
                float(row.episodes) if pd.notna(row.episodes) else None,
                float(row.rating) if pd.notna(row.rating) else None,
                float(row.members) if pd.notna(row.members) else None,
                float(row.popularity_score) if pd.notna(row.popularity_score) else 0
            ))
        
        cursor.executemany("""
            INSERT INTO anime_staging (anime_id, name, genre, type, episodes, rating, members, popularity_score)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
        """, anime_tuples)
        
        # Load ratings data
        ratings_tuples = []
        for row in ratings_df.itertuples(index=False):
            ratings_tuples.append((
                int(row.user_id),
                int(row.anime_id),
                float(row.rating) if pd.notna(row.rating) else None,
                1 if row.is_high_rating else 0,
                row.rating_date
            ))
        
        cursor.executemany("""
            INSERT INTO ratings_staging (user_id, anime_id, rating, is_high_rating, rating_date)
            VALUES (:1, :2, :3, :4, :5)
        """, ratings_tuples)
        
        connection.commit()
        logging.info("Data loaded into staging tables successfully")
        
        # Data quality checks
        cursor.execute("SELECT COUNT(*) FROM anime_staging")
        anime_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM ratings_staging")
        ratings_count = cursor.fetchone()[0]
        
        logging.info(f"Data quality verification:")
        logging.info(f"  - Anime staging: {anime_count} records")
        logging.info(f"  - Ratings staging: {ratings_count} records")
        
        # Calculate data quality metrics
        cursor.execute("SELECT AVG(rating) FROM ratings_staging WHERE rating IS NOT NULL")
        avg_rating = cursor.fetchone()[0]
        logging.info(f"  - Average rating: {avg_rating:.2f}")
        
        return True
        
    except Exception as e:
        logging.error(f"Load failed: {e}")
        connection.rollback()
        return False
    finally:
        connection.close()

def main():
    """Main ETL pipeline function"""
    logging.info("Starting ETL Pipeline...")
    
    try:
        # Extract
        anime_data, ratings_data = extract()
        
        # Transform
        anime_clean, ratings_clean = transform(anime_data, ratings_data)
        
        # Load
        success = load(anime_clean, ratings_clean)
        
        if success:
            logging.info("ETL Pipeline completed successfully!")
            print("\n" + "="*50)
            print("ETL PIPELINE SUMMARY:")
            print(f"• Extracted: {len(anime_data)} anime, {len(ratings_data)} ratings")
            print(f"• Transformed: {len(anime_clean)} anime, {len(ratings_clean)} ratings")
            print(f"• Loaded into staging tables")
            print(f"• Check etl_pipeline.log for detailed logs")
            print("="*50)
        else:
            logging.error("ETL Pipeline failed")
            
    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")

if __name__ == "__main__":
    main()