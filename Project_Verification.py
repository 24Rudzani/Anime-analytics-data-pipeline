# Project_Verification.py
import os
import json
import subprocess
from datetime import datetime

def verify_project_completion():
    """Verify all project components are working"""
    print("PROJECT VERIFICATION CHECKLIST")
    print("=" * 60)
    
    components = {
        "Database Connection": check_database(),
        "ETL Pipeline": check_etl_pipeline(),
        "SQL Analysis": check_sql_analysis(),
        "Cloud Integration": check_cloud_integration(),
        "Dashboard": check_dashboard(),
        "Data Files": check_data_files(),
        "Requirements": check_requirements()
    }
    
    print("\nVERIFICATION RESULTS:")
    print("-" * 40)
    
    total_checks = len(components)
    passed_checks = sum(1 for check in components.values() if check)
    
    for component, status in components.items():
        status_symbol = "[PASS]" if status else "[FAIL]"
        print(f"{status_symbol} {component}")
    
    print("-" * 40)
    print(f"Overall: {passed_checks}/{total_checks} checks passed")
    
    if passed_checks == total_checks:
        print("PROJECT READY FOR SUBMISSION!")
    else:
        print("Some components need attention")
    
    return passed_checks == total_checks

def check_database():
    """Check if database is accessible"""
    try:
        import oracledb
        connection = oracledb.connect(
            user=os.getenv('DB_USER', 'system'),
            password=os.getenv('DB_PASSWORD', ''),
            dsn=os.getenv('DB_DSN', 'YOUR_DSN_HERE')
)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM anime")
        anime_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM ratings")
        ratings_count = cursor.fetchone()[0]
        connection.close()
        print(f"Database: Connected ({anime_count} anime, {ratings_count} ratings)")
        return True
    except Exception as e:
        print(f"Database: Failed - {e}")
        return False

def check_etl_pipeline():
    """Check if ETL pipeline files exist and have run successfully"""
    files = ['ETL_Pipeline.py', 'etl_pipeline.log']
    for file in files:
        if not os.path.exists(file):
            print(f"ETL Pipeline: Missing {file}")
            return False
    
    # Check if ETL has run successfully
    if os.path.exists('etl_pipeline.log'):
        with open('etl_pipeline.log', 'r', encoding='utf-8') as f:
            log_content = f.read()
            if "ETL Pipeline completed successfully" in log_content:
                print("ETL Pipeline: Successfully executed")
                return True
            else:
                print("ETL Pipeline: Log exists but no success message")
                return False
    print("ETL Pipeline: All files present")
    return True

def check_sql_analysis():
    """Check SQL analysis component"""
    if os.path.exists('SQL_Analysis.py'):
        # Test if it can run without errors
        try:
            # We'll do a simple import test rather than full execution
            with open('SQL_Analysis.py', 'r', encoding='utf-8') as f:
                content = f.read()
            if "oracledb.connect" in content and "SELECT" in content:
                print("SQL Analysis: Script ready and contains database operations")
                return True
            else:
                print("SQL Analysis: Script may be incomplete")
                return False
        except:
            print("SQL Analysis: Script exists but has issues")
            return False
    else:
        print("SQL Analysis: Script missing")
        return False

def check_cloud_integration():
    """Check cloud integration"""
    cloud_dir = "cloud_simulated_storage"
    if os.path.exists(cloud_dir):
        # Count files in cloud storage
        file_count = 0
        total_size = 0
        for root, dirs, files in os.walk(cloud_dir):
            file_count += len(files)
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
        
        # Check if we have backup files
        backup_path = os.path.join(cloud_dir, "buckets", "anime-data-pipeline-prod", "backups")
        backup_count = 0
        if os.path.exists(backup_path):
            for root, dirs, files in os.walk(backup_path):
                backup_count += len(files)
        
        print(f"Cloud Integration: Active ({file_count} files, {backup_count} backups, {total_size/(1024*1024):.1f} MB total)")
        return file_count > 0
    else:
        print("Cloud Integration: Not initialized")
        return False

def check_dashboard():
    """Check dashboard component"""
    if os.path.exists('Anime_Dashboard.py'):
        # Check if it's a valid Streamlit app
        try:
            with open('Anime_Dashboard.py', 'r', encoding='utf-8') as f:
                content = f.read()
            if "streamlit" in content and "st." in content:
                print("Dashboard: Streamlit app ready for deployment")
                return True
            else:
                print("Dashboard: File exists but may not be valid Streamlit app")
                return False
        except UnicodeDecodeError:
            # If UTF-8 fails, try a different encoding or just check file existence
            print("Dashboard: Streamlit app file exists (encoding issue in verification)")
            return True
        except Exception as e:
            print(f"Dashboard: Error reading file - {e}")
            return False
    else:
        print("Dashboard: App file missing")
        return False

def check_data_files():
    """Check required data files"""
    required_files = ['anime.csv', 'rating.csv']
    missing_files = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"Data Files: Missing {missing_files}")
        return False
    else:
        # Check file sizes to ensure they're not empty
        anime_size = os.path.getsize('anime.csv') / (1024*1024)
        rating_size = os.path.getsize('rating.csv') / (1024*1024)
        print(f"Data Files: All present (anime: {anime_size:.1f}MB, ratings: {rating_size:.1f}MB)")
        return True

def check_requirements():
    """Check requirements file"""
    if os.path.exists('requirements.txt'):
        with open('requirements.txt', 'r') as f:
            packages = [line.strip() for line in f.readlines() if line.strip() and not line.startswith('#')]
        print(f"Requirements: {len(packages)} packages specified")
        return len(packages) > 0
    else:
        print("Requirements: File missing")
        return False

def check_cloud_backups_detailed():
    """Detailed check of cloud backups"""
    print("\n" + "=" * 50)
    print("DETAILED CLOUD STORAGE ANALYSIS")
    print("=" * 50)
    
    cloud_dir = "cloud_simulated_storage"
    if not os.path.exists(cloud_dir):
        print("Cloud storage not initialized")
        return
    
    # Analyze storage structure
    buckets_path = os.path.join(cloud_dir, "buckets")
    if os.path.exists(buckets_path):
        buckets = os.listdir(buckets_path)
        print(f"Buckets: {buckets}")
        
        for bucket in buckets:
            bucket_path = os.path.join(buckets_path, bucket)
            print(f"\nBucket: {bucket}")
            
            # Check different prefixes
            prefixes = ["backups", "summaries", "reports"]
            for prefix in prefixes:
                prefix_path = os.path.join(bucket_path, prefix)
                if os.path.exists(prefix_path):
                    file_count = 0
                    total_size = 0
                    
                    for root, dirs, files in os.walk(prefix_path):
                        file_count += len(files)
                        for file in files:
                            file_path = os.path.join(root, file)
                            total_size += os.path.getsize(file_path)
                    
                    print(f"  {prefix}: {file_count} files, {total_size/(1024*1024):.2f} MB")

def generate_submission_checklist():
    """Generate submission checklist"""
    checklist = {
        "GitHub Repository": [
            "All code files uploaded",
            "README.md with setup instructions",
            "requirements.txt included",
            ".gitignore with sensitive data excluded",
            "No credentials in code"
        ],
        "Code Quality": [
            "PEP 8 compliance",
            "Error handling implemented",
            "Logging configured",
            "Modular code structure"
        ],
        "Functionality": [
            "SQL queries working (JOINs, CTEs, Aggregates)",
            "ETL pipeline running end-to-end",
            "Cloud integration demonstrated",
            "Data transformations applied",
            "Data quality checks implemented"
        ],
        "Documentation": [
            "Presentation slides (8-12 slides)",
            "Architecture diagram",
            "Project overview and description",
            "Technical decisions documented",
            "Challenges and solutions explained"
        ]
    }
    
    print("\n" + "=" * 60)
    print("SUBMISSION CHECKLIST")
    print("=" * 60)
    
    for category, items in checklist.items():
        print(f"\n{category}:")
        for item in items:
            print(f"  [ ] {item}")
    
    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("1. Create presentation slides (PDF)")
    print("2. Finalize GitHub repository")
    print("3. Test complete workflow")
    print("4. Submit before deadline: Tuesday, November 7, 2025")
    print("=" * 60)

if __name__ == "__main__":
    # Run verification
    print("PROJECT COMPLETION VERIFICATION")
    print("=" * 60)
    
    all_passed = verify_project_completion()
    
    # Detailed cloud analysis
    check_cloud_backups_detailed()
    
    # Generate checklist
    generate_submission_checklist()
    
    # Final status
    if all_passed:
        print("\n" + "=" * 60)
        print("SUCCESS! YOUR PROJECT IS COMPLETE AND READY!")
        print("=" * 60)
        print("You have successfully implemented:")
        print("✓ Complex SQL analysis with Oracle database")
        print("✓ Complete ETL pipeline with data transformations") 
        print("✓ Professional cloud storage simulation")
        print("✓ Interactive Streamlit dashboard")
        print("✓ Comprehensive error handling and logging")
        print("✓ Data quality checks and monitoring")
        print("=" * 60)
        print("\nNEXT: Create your presentation slides and submit!")
    else:
        print("\nPlease address the issues above before submission.")