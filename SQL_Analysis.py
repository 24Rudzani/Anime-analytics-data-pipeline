import oracledb

def get_connection():
    """Create connection to Oracle database"""
    return oracledb.connect(
        user='YOUR_USERNAME_HERE',
        password='YOUR_PASSWORD_HERE', 
        dsn='YOUR_DSN_HERE'
     )

def run_sql_analysis():
    connection = get_connection()
    cursor = connection.cursor()
    
    print("=== SQL ANALYSIS FOR ANIME DATASET ===\n")
    
    # 1. BASIC TABLE OVERVIEW
    print("1. TABLE OVERVIEW")
    cursor.execute("SELECT COUNT(*) as total_anime FROM anime")
    anime_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) as total_ratings FROM ratings")
    ratings_count = cursor.fetchone()[0]
    print(f"   Total Anime: {anime_count:,}")
    print(f"   Total Ratings: {ratings_count:,}\n")
    
    # 2. JOIN OPERATIONS (INNER JOIN)
    print("2. JOIN OPERATION: Anime with their Average Ratings")
    cursor.execute("""
        SELECT a.anime_id, a.name, a.type, ROUND(AVG(r.rating), 2) as avg_rating
        FROM anime a
        INNER JOIN ratings r ON a.anime_id = r.anime_id
        WHERE r.rating IS NOT NULL
        GROUP BY a.anime_id, a.name, a.type
        HAVING COUNT(r.rating) > 10
        ORDER BY avg_rating DESC
        FETCH FIRST 10 ROWS ONLY
    """)
    print("   Top 10 Highest Rated Anime (with >10 ratings):")
    for row in cursor:
        print(f"   {row[1]} ({row[2]}) - Rating: {row[3]}")
    print()
    
    # 3. AGGREGATE FUNCTIONS WITH GROUP BY
    print("3. AGGREGATES: Anime Count by Type")
    cursor.execute("""
        SELECT type, COUNT(*) as count, ROUND(AVG(rating), 2) as avg_rating
        FROM anime 
        WHERE rating IS NOT NULL
        GROUP BY type
        ORDER BY count DESC
    """)
    print("   Anime Distribution by Type:")
    for row in cursor:
        print(f"   {row[0]}: {row[1]} anime, Avg Rating: {row[2]}")
    print()
    
    # 4. CRUD OPERATIONS DEMONSTRATION
    print("4. CRUD OPERATIONS")
    
    # CREATE (INSERT) - Add a test record
    cursor.execute("""
        INSERT INTO anime (anime_id, name, genre, type, episodes, rating, members) 
        VALUES (99999, 'Test Anime', 'Adventure', 'TV', 12, 8.5, 1000)
    """)
    print("   [INSERT] Added test anime record")
    
    # READ (SELECT) - Verify the insert
    cursor.execute("SELECT name, rating FROM anime WHERE anime_id = 99999")
    test_anime = cursor.fetchone()
    print(f"   [READ] Found {test_anime[0]} with rating {test_anime[1]}")
    
    # UPDATE - Modify the test record
    cursor.execute("UPDATE anime SET rating = 9.0 WHERE anime_id = 99999")
    cursor.execute("SELECT rating FROM anime WHERE anime_id = 99999")
    updated_rating = cursor.fetchone()[0]
    print(f"   [UPDATE] Changed rating to {updated_rating}")
    
    # DELETE - Clean up
    cursor.execute("DELETE FROM anime WHERE anime_id = 99999")
    print("   [DELETE] Removed test anime record")
    print()
    
    # 5. CTEs (COMMON TABLE EXPRESSIONS)
    print("5. CTE: Popular Genres Analysis")
    cursor.execute("""
        WITH genre_analysis AS (
            SELECT 
                genre,
                COUNT(*) as anime_count,
                ROUND(AVG(rating), 2) as avg_rating,
                SUM(members) as total_members
            FROM anime
            WHERE rating IS NOT NULL AND genre IS NOT NULL
            GROUP BY genre
        )
        SELECT genre, anime_count, avg_rating, total_members
        FROM genre_analysis
        WHERE anime_count >= 10
        ORDER BY avg_rating DESC
        FETCH FIRST 5 ROWS ONLY
    """)
    print("   Top 5 Genres by Average Rating (with >=10 anime):")
    for row in cursor:
        print(f"   {row[0]}: {row[1]} anime, Rating: {row[2]}, Members: {row[3]:,}")
    print()
    
    # 6. FILTERING AND SORTING
    print("6. FILTERING: High-Rated Movies")
    cursor.execute("""
        SELECT name, genre, rating, members
        FROM anime
        WHERE type = 'Movie' 
          AND rating >= 8.5 
          AND members >= 100000
        ORDER BY rating DESC, members DESC
        FETCH FIRST 5 ROWS ONLY
    """)
    print("   Top 5 High-Rated Popular Movies:")
    for row in cursor:
        print(f"   {row[0]} - {row[1]} (Rating: {row[2]}, Members: {row[3]:,})")
    print()
    
    # 7. LEFT JOIN OPERATION
    print("7. LEFT JOIN: All Anime with Their Ratings (Including Unrated)")
    cursor.execute("""
        SELECT a.name, a.type, COUNT(r.rating) as rating_count
        FROM anime a
        LEFT JOIN ratings r ON a.anime_id = r.anime_id
        GROUP BY a.name, a.type
        ORDER BY rating_count DESC
        FETCH FIRST 5 ROWS ONLY
    """)
    print("   Top 5 Most Rated Anime (Including Unrated):")
    for row in cursor:
        print(f"   {row[0]} ({row[1]}) - Ratings: {row[2]:,}")
    
    connection.commit()
    connection.close()
    print("\n[COMPLETED] SQL ANALYSIS COMPLETED!")

if __name__ == "__main__":
    run_sql_analysis()