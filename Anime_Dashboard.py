import streamlit as st
import pandas as pd
import plotly.express as px

# Page configuration
st.set_page_config(
    page_title="Anime Analytics Dashboard",
    page_icon="🎬",
    layout="wide"
)

def main():
    # Logo before title
    col1, col2 = st.columns([1, 4])
    
    with col1:
        try:
            st.image(r"C:\Users\mothi\OneDrive\Documents\Data Science - Project Y\anime_header.png.png", 
                    use_container_width=True,
                    caption='')
        except:
            st.info("Logo")
    
    with col2:
        st.title("Anime Analytics Dashboard")
        st.markdown("### Data Engineering Project - SQL & ETL Pipeline")
    
    st.markdown("---")
    
    # Project completion status
    st.header("✅ Project Completion Status")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("SQL Analysis", "100%", "Completed")
    
    with col2:
        st.metric("ETL Pipeline", "100%", "Implemented")
    
    with col3:
        st.metric("Data Records", "12,294", "Anime")
    
    with col4:
        st.metric("Ratings", "50,000+", "Processed")
    
    # Project achievements
    st.header("🏆 Project Achievements")
    
    achievements = [
        "✅ Oracle Database configured and populated successfully",
        "✅ 12,294 anime records loaded with proper schema design",
        "✅ 50,000+ ratings processed through ETL pipeline", 
        "✅ Complex SQL queries implemented (JOINs, CTEs, Aggregates)",
        "✅ Complete ETL pipeline with data transformation",
        "✅ Data quality checks and error handling implemented",
        "✅ Interactive visualization dashboard created",
        "✅ All project requirements completed in 3-day sprint"
    ]
    
    for achievement in achievements:
        st.write(achievement)
    
    # Demo data visualization
    st.header("📊 Sample Data Visualizations")
    
    # Create sample data for demonstration
    sample_data = pd.DataFrame({
        'Type': ['TV', 'OVA', 'Movie', 'Special', 'ONA', 'Music'],
        'Count': [3671, 3285, 2297, 1671, 652, 488],
        'Avg_Rating': [6.90, 6.38, 6.32, 6.52, 5.64, 5.59]
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Anime Distribution by Type")
        fig1 = px.pie(sample_data, values='Count', names='Type', hole=0.3)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.subheader("Average Ratings by Type")
        fig2 = px.bar(sample_data, x='Type', y='Avg_Rating', 
                     title="Average Ratings by Anime Type")
        st.plotly_chart(fig2, use_container_width=True)
    
    # Key insights
    st.header("💡 Key Insights Discovered")
    
    insights = [
        "🎯 **Highest Rated Anime**: 'Kimi no Na wa.' (9.37/10)",
        "📺 **Most Common Type**: TV shows (3,671 records)", 
        "⭐ **Genre Analysis**: Hybrid genres have highest ratings",
        "🚀 **Performance**: Processed 7.8M+ ratings dataset",
        "🔧 **Engineering**: Robust ETL pipeline implemented"
    ]
    
    for insight in insights:
        st.write(insight)
    
    # Technical stack
    st.header("🛠️ Technical Stack")
    
    tech_col1, tech_col2 = st.columns(2)
    
    with tech_col1:
        st.subheader("Technologies Used")
        st.write("""
        - **Database**: Oracle 21c
        - **Programming**: Python 3.8+
        - **Libraries**: pandas, oracledb, streamlit, plotly
        - **ETL**: Custom pipeline
        - **Analysis**: Advanced SQL
        """)
    
    with tech_col2:
        st.subheader("Project Architecture")
        st.write("""
        📥 Extract: CSV files  
        🔄 Transform: Data cleaning  
        📤 Load: Oracle database  
        🔍 Analyze: SQL queries  
        📊 Visualize: Dashboard
        """)
    
    # Final completion
    st.markdown("---")
    st.success("🎉 **PROJECT COMPLETED SUCCESSFULLY!** All data engineering requirements met and demonstrated.")

if __name__ == "__main__":
    main()