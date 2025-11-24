# Project: Anime Data Pipeline Builder

## 📊 Project Overview

A complete data engineering solution demonstrating SQL proficiency and ETL pipeline development using an anime dataset. This project processes 12,294 anime records with 50,000 ratings through a robust ETL pipeline, performs advanced SQL analysis, and provides interactive visualizations.

## 🎯 Project Goals

- Write complex SQL queries for data analysis.
- Build automated ETL pipeline using Python.
- Integrate cloud storage solutions.
- Apply data transformation and quality checks.
- Present technical decisions and problem-solving approaches.

## 📁 Project Structure

Project-Y/
├── ETL_Pipeline.py          # Main ETL pipeline
├── SQL_Analysis.py          # SQL queries and analysis  
├── Anime_Dashboard.py       # Interactive Streamlit dashboard
├── Cloud_Integration.py     # Cloud storage handling
├── Cloud_Monitor.py         # Monitoring capabilities
├── Load_Data.py             # Data loading functionality
├── Project_Runner.py        # Execution coordinator
├── Project_Verification.py  # Validation system
├── requirements.txt         # Dependencies
├── anime.csv               # Source anime dataset
├── rating.csv              # Ratings dataset
├── etl_pipeline.log        # Pipeline execution logs
└── cloud_simulated_storage/ # Simulated cloud storage
    ├── backups/
    ├── reports/
    └── summaries/

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- Oracle Database
- Required packages: `pip install -r requirements.txt`

### Environment Setup
1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up Oracle database connection
4. Configure environment variables in `.env`

### Running the Project
1. **ETL Pipeline**: `python ETL_Pipeline.py`
2. **SQL Analysis**: `python SQL_Analysis.py`
3. **Dashboard**: `streamlit run Anime_Dashboard.py`

## 📊 Dataset
- **Source**: Anime Recommendation Database
- **Size**: 12,294 anime, 50,000 ratings
- **Minimum Requirements**: ✓ 1,000+ rows ✓ 5+ columns ✓ Suitable for joins
- **Columns**: Anime ID, name, genre, type, episodes, rating, members

## 🔧 Technologies Used
- **Database**: Oracle Database
- **Programming**: Python 3.8+
- **Libraries**: pandas, oracledb, streamlit, sqlalchemy
- **Cloud**: Simulated AWS S3/Azure Blob Storage
- **ETL Framework**: Custom Python pipeline

## 📈 Key Features

### ✅ SQL Analysis
- Complex queries with JOINs (INNER, LEFT), CTEs, Aggregations
- CRUD operations (CREATE, READ, UPDATE, DELETE)
- Advanced filtering, sorting, and data transformations
- Group By with HAVING clauses

### ✅ ETL Pipeline
- **Extract**: Data ingestion from CSV files with error handling
- **Transform**: Data cleaning, validation, business logic
- **Load**: Cloud database storage and file uploads
- **Quality**: Data integrity verification and logging

### ✅ Cloud Integration
- Simulated cloud storage with backup management
- Automated file versioning and reporting
- Storage monitoring and metadata tracking

### ✅ Dashboard
- Interactive Streamlit application
- Real-time data visualization
- Pipeline metrics and status monitoring

## 🎯 SQL Analysis Highlights

### Top Rated Anime
- **Highest Rated**: "Kimi no Na wa." (9.82/10)
- **Most Common Type**: TV shows (3,671 anime)
- **Genre Distribution**: Clear analysis across multiple categories

### Key Insights
- Rating patterns across different anime types
- Popular genres with highest engagement
- Data quality suitable for recommendation systems

## 🔄 ETL Pipeline Design

### Extract Phase
- Source: CSV files (anime.csv, rating.csv)
- Error handling for file access and data retrieval
- Data validation at ingestion

### Transform Phase
- Handle null values and duplicates
- Validate data types and ranges
- Apply business logic transformations
- Comprehensive logging

### Load Phase
- Store in Oracle database
- Upload to cloud storage
- Verify data integrity
- Generate backup reports

## 🚀 Results & Insights
The analysis revealed:
- Clear rating patterns across anime types
- Popular genres with highest engagement
- Quality data suitable for recommendation systems
- Robust pipeline capable of handling large datasets

## 📋 Project Requirements Met

### MVP Features
- ✅ Complex SQL queries with multiple JOIN types
- ✅ Complete ETL pipeline with error handling
- ✅ Cloud storage integration
- ✅ Data transformation and quality checks
- ✅ Technical documentation

### Bonus Features
- ✅ Interactive Streamlit dashboard
- ✅ Automated pipeline execution
- ✅ Data quality monitoring
- ✅ Comprehensive logging system

## 🎓 Learning Outcomes
- Advanced SQL query writing and optimization
- ETL pipeline design and implementation
- Cloud storage integration patterns
- Data quality assurance techniques
- Project documentation and presentation

## 📞 Support
For questions or issues, please open an issue in the GitHub repository.

## 📄 License
This project uses publicly available anime data for educational purposes.
