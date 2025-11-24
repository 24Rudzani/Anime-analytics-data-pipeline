# Project_Runner.py - Unified controller
import os
import sys
import importlib

def safe_import(module_name, function_name=None):
    """Safely import modules and handle errors"""
    try:
        module = importlib.import_module(module_name)
        if function_name:
            return getattr(module, function_name)
        return module
    except ImportError as e:
        print(f"❌ Could not import {module_name}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error with {module_name}: {e}")
        return None

def run_load_data():
    """Run data loading component"""
    print("\n" + "="*50)
    print("📥 LOADING DATA INTO DATABASE")
    print("="*50)
    
    load_data = safe_import("Load_Data", "main")
    if load_data:
        load_data()
    else:
        print("Load_Data module not available")

def run_etl_standard():
    """Run standard ETL pipeline"""
    print("\n" + "="*50)
    print("🔄 RUNNING STANDARD ETL PIPELINE")
    print("="*50)
    
    etl = safe_import("ETL_Pipeline", "main")
    if etl:
        etl()
    else:
        print("ETL_Pipeline module not available")

def run_etl_enhanced():
    """Run enhanced ETL pipeline"""
    print("\n" + "="*50)
    print("🚀 RUNNING ENHANCED ETL PIPELINE")
    print("="*50)
    
    etl_enhanced = safe_import("ETL_Pipeline_Enhanced", "main")
    if etl_enhanced:
        etl_enhanced()
    else:
        print("ETL_Pipeline_Enhanced module not available")

def run_sql_analysis():
    """Run SQL analysis"""
    print("\n" + "="*50)
    print("📊 RUNNING SQL ANALYSIS")
    print("="*50)
    
    sql_analysis = safe_import("SQL_Analysis", "run_sql_analysis")
    if sql_analysis:
        sql_analysis()
    else:
        print("SQL_Analysis module not available")

def run_cloud_integration():
    """Run cloud integration"""
    print("\n" + "="*50)
    print("☁️ RUNNING CLOUD INTEGRATION")
    print("="*50)
    
    cloud = safe_import("Cloud_Integration", "backup_data_to_cloud")
    if cloud:
        success_count, results = cloud()
        print(f"Cloud backup completed: {success_count} files processed")
    else:
        print("Cloud_Integration module not available")

def run_backup_check():
    """Run backup verification"""
    print("\n" + "="*50)
    print("🔍 CHECKING BACKUP INTEGRITY")
    print("="*50)
    
    backup_check = safe_import("Check_Backup", "check_cloud_backups")
    if backup_check:
        backup_check()
    else:
        print("Check_Backup module not available")

def run_dashboard():
    """Launch Streamlit dashboard"""
    print("\n" + "="*50)
    print("📈 LAUNCHING STREAMLIT DASHBOARD")
    print("="*50)
    print("To launch the dashboard, run in a separate terminal:")
    print("streamlit run Anime_Dashboard.py")
    print("="*50)

def run_complete_pipeline():
    """Run the complete data engineering pipeline"""
    print("\n" + "="*60)
    print("🚀 COMPLETE DATA ENGINEERING PIPELINE")
    print("="*60)
    
    # 1. Data Loading
    print("1. 📥 Loading data into database...")
    run_load_data()
    
    # 2. ETL Processing
    print("\n2. 🔄 Running enhanced ETL pipeline...")
    run_etl_enhanced()
    
    # 3. SQL Analysis
    print("\n3. 📊 Performing SQL analysis...")
    run_sql_analysis()
    
    # 4. Cloud Backup
    print("\n4. ☁️ Backing up to cloud storage...")
    run_cloud_integration()
    
    # 5. Backup Verification
    print("\n5. 🔍 Verifying backup integrity...")
    run_backup_check()
    
    # 6. Dashboard info
    print("\n6. 📈 Visualization dashboard ready!")
    run_dashboard()
    
    print("\n" + "="*60)
    print("✅ COMPLETE PIPELINE FINISHED!")
    print("="*60)

def show_menu():
    """Display the main menu"""
    print("\n" + "="*50)
    print("🎯 DATA ENGINEERING PROJECT RUNNER")
    print("="*50)
    print("Choose component to run:")
    print("1. 📥 Load Data")
    print("2. 🔄 ETL Pipeline (Standard)")
    print("3. 🚀 ETL Pipeline (Enhanced)")
    print("4. 📊 SQL Analysis")
    print("5. ☁️ Cloud Integration")
    print("6. 🔍 Backup Check")
    print("7. 📈 Launch Dashboard")
    print("8. 🚀 Complete Pipeline")
    print("9. ❌ Exit")
    print("="*50)

def main():
    """Main controller function"""
    while True:
        show_menu()
        
        try:
            choice = input("Enter choice (1-9): ").strip()
            
            if choice == '1':
                run_load_data()
            elif choice == '2':
                run_etl_standard()
            elif choice == '3':
                run_etl_enhanced()
            elif choice == '4':
                run_sql_analysis()
            elif choice == '5':
                run_cloud_integration()
            elif choice == '6':
                run_backup_check()
            elif choice == '7':
                run_dashboard()
            elif choice == '8':
                run_complete_pipeline()
            elif choice == '9':
                print("👋 Exiting Project Runner. Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please enter 1-9.")
                
            # Pause between operations
            if choice != '9':
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            print("\n👋 Exiting Project Runner. Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()