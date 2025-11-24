import oracledb
import pandas as pd
import numpy as np

def get_connection():
    """Create connection to Oracle database"""
    try:
        connection = oracledb.connect(
            user='YOUR_USERNAME_HERE',
            password='YOUR_PASSWORD_HERE', 
            dsn='YOUR_DSN_HERE'
        )
        print("Connected to Oracle Database")
        return connection
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

def clean_numeric_value(value):
    """Clean numeric values for Oracle insertion"""
    if pd.isna(value) or value == '' or value == 'Unknown':
        return None
    try:
        # Handle string representations of numbers
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
        return float(value)
    except (ValueError, TypeError):
        return None

def load_anime_data(connection):
    """Load anime CSV data into Oracle"""
    try:
        df = pd.read_csv('anime.csv')
        print(f"Loaded anime data: {len(df)} rows")
        
        cursor = connection.cursor()
        
        # Prepare INSERT statement
        insert_sql = """
        INSERT INTO anime (anime_id, name, genre, type, episodes, rating, members) 
        VALUES (:1, :2, :3, :4, :5, :6, :7)
        """
        
        # Process data row by row with proper cleaning
        data_tuples = []
        success_count = 0
        error_count = 0
        
        for row in df.itertuples(index=False):
            try:
                # Clean numeric values
                cleaned_episodes = clean_numeric_value(row.episodes)
                cleaned_rating = clean_numeric_value(row.rating)
                cleaned_members = clean_numeric_value(row.members)
                
                # Handle text fields
                name = row.name if pd.notna(row.name) else 'Unknown'
                genre = row.genre if pd.notna(row.genre) else 'Unknown'
                anime_type = row.type if pd.notna(row.type) else 'Unknown'
                
                data_tuples.append((
                    int(row.anime_id),
                    str(name)[:255],  # Limit to 255 chars for VARCHAR2
                    str(genre)[:500],
                    str(anime_type)[:50],
                    cleaned_episodes,
                    cleaned_rating,
                    cleaned_members
                ))
                success_count += 1
                
            except Exception as e:
                print(f"Error processing row {row.anime_id}: {e}")
                error_count += 1
                continue
        
        print(f"Processed {success_count} rows successfully, {error_count} errors")
        
        if data_tuples:
            cursor.executemany(insert_sql, data_tuples)
            connection.commit()
            print("Anime data loaded successfully")
            return True
        else:
            print("No valid data to insert")
            return False
        
    except Exception as e:
        print(f"Failed to load anime data: {e}")
        connection.rollback()
        return False

def load_ratings_data(connection):
    """Load ratings CSV data into Oracle"""
    try:
        df = pd.read_csv('rating.csv')
        print(f"Loaded ratings data: {len(df)} rows")
        
        # Take a smaller sample for development
        sample_size = 50000
        if len(df) > sample_size:
            print(f"Taking sample of {sample_size} rows")
            df = df.sample(n=sample_size, random_state=42)
        
        cursor = connection.cursor()
        insert_sql = "INSERT INTO ratings (user_id, anime_id, rating) VALUES (:1, :2, :3)"
        
        # Process ratings data
        data_tuples = []
        for row in df.itertuples(index=False):
            try:
                # Clean rating value (handle -1 ratings which might mean "no rating")
                rating = float(row.rating) if row.rating != -1 else None
                
                data_tuples.append((
                    int(row.user_id),
                    int(row.anime_id),
                    rating
                ))
            except Exception as e:
                continue  # Skip problematic rows
        
        cursor.executemany(insert_sql, data_tuples)
        connection.commit()
        
        print("Ratings data loaded successfully")
        return True
        
    except Exception as e:
        print(f"Failed to load ratings data: {e}")
        connection.rollback()
        return False

def main():
    print("Starting data loading process...")
    connection = get_connection()
    
    if connection:
        try:
            # Clear existing data
            cursor = connection.cursor()
            cursor.execute("DELETE FROM ratings")
            cursor.execute("DELETE FROM anime")
            connection.commit()
            print("Cleared existing data from tables")
            
            # Load data
            load_anime_data(connection)
            load_ratings_data(connection)
            
            # Verify the data was loaded
            cursor.execute("SELECT COUNT(*) FROM anime")
            anime_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM ratings")
            ratings_count = cursor.fetchone()[0]
            
            print("VERIFICATION:")
            print(f"Anime table: {anime_count} rows")
            print(f"Ratings table: {ratings_count} rows")
            
            if anime_count > 0 and ratings_count > 0:
                print("SUCCESS! Data loading completed!")
                print("\n READY FOR SQL ANALYSIS!")
            else:
                print("WARNING: Tables appear to be empty")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            connection.close()
    else:
        print("Cannot proceed without connection")

if __name__ == "__main__":
    main()