"""
dashboard.py — Live Streamlit Dashboard
=========================================
Reads from the jobs_clean table in jobs.db and displays:
  - KPI metrics (total jobs, companies, median salary, categories)
  - Sidebar filters (source, category, job level)
  - 5 interactive charts
  - Searchable job listings table
  - Download button for filtered data

Run with:
  streamlit run dashboard.py

Opens at: http://localhost:8501
Auto-refreshes every 5 minutes (cache TTL).
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
from collections import Counter
from datetime import datetime
import os
import numpy as np
from scipy import stats
import seaborn as sns
import matplotlib.pyplot as plt

# ══════════════════════════════════════════════════════════════
#  PAGE CONFIGURATION  (must be first Streamlit call)
# ══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Nepal Job Market Dashboard",
    page_icon="🇳🇵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ══════════════════════════════════════════════════════════════
#  LOAD DATA  (cached for 5 minutes so dashboard stays fast)
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_data():
    """Loads the clean jobs table from the database."""
    try:
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "jobs.db")
        
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM jobs_clean", conn)
        conn.close()
        df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
        df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return pd.DataFrame()

df = load_data()

# ══════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════

st.title("🇳🇵 Nepal Job Market Analytics Dashboard")
st.caption(
    f"📅 Dashboard loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  "
    f"Data sources: **MeroJob** + **KumariJob**  |  "
    f"Auto-refreshes every 5 minutes"
)

if df.empty:
    st.error("⚠️ No data found in database.")
    st.info(
        "**To get started:**\n"
        "1. Open a terminal in this folder\n"
        "2. Run: `python scheduler.py`\n"
        "3. Wait for scraping to complete\n"
        "4. Come back and refresh this page"
    )
    st.stop()

# ══════════════════════════════════════════════════════════════
#  SIDEBAR FILTERS
# ══════════════════════════════════════════════════════════════

st.sidebar.header("🔍 Filter Jobs")
st.sidebar.markdown("---")

# Source filter
all_sources = sorted(df['source'].dropna().unique().tolist())
source_filter = st.sidebar.multiselect(
    "📡 Data Source",
    options=all_sources,
    default=all_sources,
    help="Choose which website(s) to include"
)

# Category filter (show top 30 most common)
top_cats = df['category'].value_counts().head(30).index.tolist()
category_filter = st.sidebar.multiselect(
    "💼 Job Category",
    options=top_cats,
    default=[],
    help="Leave empty to show all categories"
)

# Job Level filter
all_levels = sorted(df['job_level'].dropna().unique().tolist())
level_filter = st.sidebar.multiselect(
    "📊 Job Level",
    options=all_levels,
    default=[],
    help="Leave empty to show all levels"
)

# Location filter
top_locs = df[df['location'] != 'Unknown']['location'].value_counts().head(15).index.tolist()
loc_filter = st.sidebar.multiselect(
    "📍 Location",
    options=top_locs,
    default=[],
    help="Leave empty to show all cities"
)

st.sidebar.markdown("---")
st.sidebar.markdown("**About this project:**")
st.sidebar.markdown(
    "Real-time job market analytics for Nepal, "
    "built by scraping MeroJob and KumariJob. "
    "Data updates every 7 days automatically."
)

# ══════════════════════════════════════════════════════════════
#  APPLY FILTERS
# ══════════════════════════════════════════════════════════════

filtered = df.copy()

if source_filter:
    filtered = filtered[filtered['source'].isin(source_filter)]
if category_filter:
    filtered = filtered[filtered['category'].isin(category_filter)]
if level_filter:
    filtered = filtered[filtered['job_level'].isin(level_filter)]
if loc_filter:
    filtered = filtered[filtered['location'].isin(loc_filter)]

# ══════════════════════════════════════════════════════════════
#  KPI METRICS  (top row of numbers)
# ══════════════════════════════════════════════════════════════

st.markdown("### 📊 Key Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "📋 Total Jobs",
        f"{len(filtered):,}",
        delta=f"{len(filtered) - len(df):+,}" if len(filtered) != len(df) else None
    )
with col2:
    st.metric("🏢 Companies Hiring", f"{filtered['company'].nunique():,}")
with col3:
    median_sal = filtered['salary_min'].median()
    st.metric(
        "💰 Median Salary",
        f"NPR {median_sal:,.0f}" if pd.notna(median_sal) else "N/A"
    )
with col4:
    st.metric("🗂 Categories", filtered['category'].nunique())
with col5:
    st.metric("🌆 Cities", filtered[filtered['location'] != 'Unknown']['location'].nunique())

st.markdown("---")

# ══════════════════════════════════════════════════════════════
#  ADVANCED EDA NAVIGATION TABS
# ══════════════════════════════════════════════════════════════

st.markdown("### 📊 Advanced Statistical Analysis & EDA")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Statistical Summary", 
    "🔗 Correlation Analysis", 
    "📊 Distribution Analysis", 
    "🎯 Scatter Plots", 
    "📅 Time Series"
])

# Tab 1: Statistical Summary
with tab1:
    st.markdown("#### Numerical Variables Summary")
    
    # Prepare numerical data
    numerical_cols = ['salary_min', 'salary_max']
    numerical_data = filtered[numerical_cols].copy()
    
    # Clean salary data
    for col in numerical_cols:
        numerical_data[col] = pd.to_numeric(numerical_data[col], errors='coerce')
        numerical_data = numerical_data[(numerical_data[col] >= 0) & (numerical_data[col] <= 1000000)]
    
    if not numerical_data.empty:
        # Custom statistics table
        stats_data = []
        for col in numerical_cols:
            col_data = numerical_data[col].dropna()
            if len(col_data) > 0:
                stats_data.append({
                    'Variable': col.replace('_', ' ').title(),
                    'Count': len(col_data),
                    'Mean': f"NPR {col_data.mean():,.0f}",
                    'Median': f"NPR {col_data.median():,.0f}",
                    'Std Dev': f"NPR {col_data.std():,.0f}",
                    'Min': f"NPR {col_data.min():,.0f}",
                    'Max': f"NPR {col_data.max():,.0f}",
                    'Q1': f"NPR {col_data.quantile(0.25):,.0f}",
                    'Q3': f"NPR {col_data.quantile(0.75):,.0f}",
                    'Skewness': f"{stats.skew(col_data):.2f}",
                    'Kurtosis': f"{stats.kurtosis(col_data):.2f}"
                })
        
        stats_df = pd.DataFrame(stats_data)
        st.dataframe(stats_df, use_container_width=True)
        
        # Categorical variables summary
        st.markdown("#### Categorical Variables Summary")
        cat_cols = ['category', 'job_level', 'location', 'source']
        cat_stats = []
        
        for col in cat_cols:
            if col in filtered.columns:
                col_data = filtered[col].dropna()
                cat_stats.append({
                    'Variable': col.replace('_', ' ').title(),
                    'Unique Values': col_data.nunique(),
                    'Most Frequent': col_data.mode()[0] if len(col_data.mode()) > 0 else 'N/A',
                    'Frequency': col_data.value_counts().iloc[0] if len(col_data) > 0 else 0,
                    'Missing Values': filtered[col].isna().sum(),
                    'Missing %': f"{(filtered[col].isna().sum() / len(filtered) * 100):.1f}%"
                })
        
        cat_stats_df = pd.DataFrame(cat_stats)
        st.dataframe(cat_stats_df, use_container_width=True)
    else:
        st.info("No sufficient numerical data available for statistical summary.")

# Tab 2: Correlation Analysis
with tab2:
    st.markdown("#### Salary Correlation Matrix")
    
    # Prepare correlation data
    corr_data = filtered[['salary_min', 'salary_max']].copy()
    for col in corr_data.columns:
        corr_data[col] = pd.to_numeric(corr_data[col], errors='coerce')
    
    # Add encoded categorical variables
    if len(filtered) > 0:
        # Encode categorical variables for correlation
        cat_encoded = pd.get_dummies(filtered[['job_level', 'source']], prefix=['level', 'src'])
        corr_data = pd.concat([corr_data, cat_encoded], axis=1)
        
        # Calculate correlation
        corr_matrix = corr_data.corr()
        
        # Create correlation heatmap
        if not corr_matrix.empty:
            fig = px.imshow(
                corr_matrix,
                color_continuous_scale='RdBu',
                aspect='auto',
                title='Correlation Matrix Heatmap',
                color_continuous_midpoint=0
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show strong correlations
            strong_corr = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    corr_val = corr_matrix.iloc[i, j]
                    if abs(corr_val) > 0.3:  # Only show correlations > 0.3
                        strong_corr.append({
                            'Variable 1': corr_matrix.columns[i],
                            'Variable 2': corr_matrix.columns[j],
                            'Correlation': f"{corr_val:.3f}",
                            'Strength': 'Strong' if abs(corr_val) > 0.7 else 'Moderate'
                        })
            
            if strong_corr:
                st.markdown("#### Notable Correlations (|r| > 0.3)")
                st.dataframe(pd.DataFrame(strong_corr), use_container_width=True)
            else:
                st.info("No strong correlations found between variables.")

# Tab 3: Distribution Analysis
with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        # Salary distribution with statistics
        salary_clean = filtered[filtered['salary_min'].notna() & (filtered['salary_min'] > 0) & (filtered['salary_min'] < 500000)]
        if len(salary_clean) > 5:
            st.markdown("#### Salary Distribution with Density Curve")
            
            # Create distribution plot with density
            hist_data = [salary_clean['salary_min'].values]
            group_labels = ['Salary Distribution']
            
            fig = ff.create_distplot(
                hist_data, 
                group_labels,
                bin_size=10000,
                show_rug=False,
                colors=['#3498db']
            )
            
            # Add statistical lines
            mean_sal = salary_clean['salary_min'].mean()
            median_sal = salary_clean['salary_min'].median()
            
            fig.add_vline(x=mean_sal, line_dash="dash", line_color="red", 
                         annotation_text=f"Mean: NPR {mean_sal:,.0f}")
            fig.add_vline(x=median_sal, line_dash="dot", line_color="green",
                         annotation_text=f"Median: NPR {median_sal:,.0f}")
            
            fig.update_layout(
                title="Salary Distribution with Density & Statistics",
                xaxis_title="Salary (NPR)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Box plots by category
        if len(salary_clean) > 10:
            st.markdown("#### Salary by Job Level (Box Plot)")
            
            # Group salary by job level
            salary_by_level = salary_clean.groupby('job_level')['salary_min'].apply(list).to_dict()
            
            fig = go.Figure()
            
            for level, salaries in salary_by_level.items():
                if len(salaries) > 2:  # Only show levels with enough data
                    fig.add_trace(go.Box(
                        y=salaries,
                        name=level,
                        boxpoints='outliers'
                    ))
            
            fig.update_layout(
                title="Salary Distribution by Job Level",
                yaxis_title="Salary (NPR)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Violin plots
    st.markdown("#### Salary Distribution Shapes by Category")
    
    # Get top categories with enough salary data
    top_cats_salary = salary_clean['category'].value_counts().head(8)
    salary_violin_data = salary_clean[salary_clean['category'].isin(top_cats_salary.index)]
    
    if len(salary_violin_data) > 20:
        fig = go.Figure()
        
        for cat in top_cats_salary.index:
            cat_salaries = salary_violin_data[salary_violin_data['category'] == cat]['salary_min']
            if len(cat_salaries) > 3:
                fig.add_trace(go.Violin(
                    y=cat_salaries,
                    name=cat[:30],  # Truncate long category names
                    box_visible=True,
                    meanline_visible=True
                ))
        
        fig.update_layout(
            title="Salary Distribution Shapes by Top Categories",
            yaxis_title="Salary (NPR)",
            height=500,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

# Tab 4: Scatter Plot Analysis
with tab4:
    st.markdown("#### Salary Range Analysis")
    
    # Salary min vs max scatter (ensure valid salary ranges)
    salary_scatter = filtered[
        filtered['salary_min'].notna() & 
        filtered['salary_max'].notna() &
        (filtered['salary_min'] > 0) &
        (filtered['salary_max'] > 0) &
        (filtered['salary_min'] < 500000) &
        (filtered['salary_max'] < 500000) &
        (filtered['salary_max'] >= filtered['salary_min'])  # Ensure max >= min
    ]
    
    if len(salary_scatter) > 10:
        col1, col2 = st.columns(2)
        
        with col1:
            # Basic scatter plot
            fig = px.scatter(
                salary_scatter,
                x='salary_min',
                y='salary_max',
                color='job_level',
                title='Salary Min vs Max by Job Level',
                hover_data=['title', 'company', 'category'],
                labels={'salary_min': 'Min Salary (NPR)', 'salary_max': 'Max Salary (NPR)'}
            )
            
            # Add diagonal line (perfect correlation)
            max_val = max(salary_scatter['salary_max'].max(), salary_scatter['salary_min'].max())
            fig.add_shape(
                type="line",
                x0=0, y0=0, x1=max_val, y1=max_val,
                line=dict(color="red", width=2, dash="dash")
            )
            
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Salary gap analysis (filter out invalid data)
            valid_salary = salary_scatter[
                (salary_scatter['salary_max'] > salary_scatter['salary_min']) &
                (salary_scatter['salary_min'] > 0)
            ].copy()
            
            if len(valid_salary) > 5:
                valid_salary['salary_gap'] = valid_salary['salary_max'] - valid_salary['salary_min']
                valid_salary['salary_gap_pct'] = (valid_salary['salary_gap'] / valid_salary['salary_min']) * 100
                
                # Ensure all size values are positive and reasonable
                valid_salary['size_value'] = np.clip(valid_salary['salary_gap_pct'], 5, 100)
                
                fig = px.scatter(
                    valid_salary,
                    x='salary_min',
                    y='salary_gap',
                    color='job_level',
                    size='size_value',
                    title='Salary Gap Analysis (Valid Ranges Only)',
                    hover_data=['title', 'company'],
                    labels={'salary_min': 'Min Salary (NPR)', 'salary_gap': 'Salary Gap (NPR)'}
                )
                
                fig.update_layout(height=450)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough valid salary range data for gap analysis.")
    
    else:
        st.info("Not enough salary data for scatter plot analysis.")

# Tab 5: Time Series Analysis
with tab5:
    if 'scraped_at' in filtered.columns:
        st.markdown("#### Job Posting Trends Over Time")
        
        # Prepare time series data
        time_data = filtered.copy()
        time_data['scraped_at'] = pd.to_datetime(time_data['scraped_at'], errors='coerce')
        time_data = time_data.dropna(subset=['scraped_at'])
        
        if len(time_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Jobs over time
                daily_jobs = time_data.groupby(time_data['scraped_at'].dt.date).size().reset_index()
                daily_jobs.columns = ['Date', 'Job_Count']
                
                fig = px.line(
                    daily_jobs,
                    x='Date',
                    y='Job_Count',
                    title='Daily Job Postings Trend',
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Average salary trend
                if len(salary_clean) > 0:
                    salary_time = salary_clean.copy()
                    salary_time['scraped_at'] = pd.to_datetime(salary_time['scraped_at'], errors='coerce')
                    daily_salary = salary_time.groupby(salary_time['scraped_at'].dt.date)['salary_min'].mean().reset_index()
                    daily_salary.columns = ['Date', 'Avg_Salary']
                    
                    fig = px.line(
                        daily_salary,
                        x='Date',
                        y='Avg_Salary',
                        title='Average Salary Trend Over Time',
                        markers=True
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Category trends
            st.markdown("#### Job Category Trends")
            category_time = time_data.groupby([time_data['scraped_at'].dt.date, 'category']).size().reset_index()
            category_time.columns = ['Date', 'Category', 'Count']
            
            # Show top 5 categories over time
            top5_cats = time_data['category'].value_counts().head(5).index
            category_time_top = category_time[category_time['Category'].isin(top5_cats)]
            
            if len(category_time_top) > 0:
                fig = px.line(
                    category_time_top,
                    x='Date',
                    y='Count',
                    color='Category',
                    title='Top 5 Job Categories Trends Over Time',
                    markers=True
                )
                fig.update_layout(height=450)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No valid timestamp data available for time series analysis.")
    else:
        st.info("Timestamp data not available in the dataset.")

st.markdown("---")

# ══════════════════════════════════════════════════════════════
#  ROW 1: Categories + Location
# ══════════════════════════════════════════════════════════════

st.markdown("### 📈 Job Distribution")
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    top_cats_chart = filtered['category'].value_counts().head(12)
    if len(top_cats_chart) > 0:
        fig = px.bar(
            x=top_cats_chart.values,
            y=top_cats_chart.index,
            orientation='h',
            title="Top Job Categories",
            color=top_cats_chart.values,
            color_continuous_scale='Viridis',
            labels={'x': 'Number of Jobs', 'y': 'Category'}
        )
        fig.update_layout(showlegend=False, height=420, margin=dict(l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    loc_data = filtered[filtered['location'] != 'Unknown']['location'].value_counts().head(10)
    if len(loc_data) > 0:
        fig = px.pie(
            values=loc_data.values,
            names=loc_data.index,
            title="Jobs by City",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════
#  ROW 2: Job Level + Salary
# ══════════════════════════════════════════════════════════════

chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    level_data = filtered['job_level'].value_counts()
    if len(level_data) > 0:
        fig = px.bar(
            x=level_data.index,
            y=level_data.values,
            title="Jobs by Experience Level",
            color=level_data.index,
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={'x': 'Level', 'y': 'Number of Jobs'},
            text=level_data.values
        )
        fig.update_traces(showlegend=False, textposition='outside')
        fig.update_layout(height=400, margin=dict(l=0, r=0), plot_bgcolor='white')
        st.plotly_chart(fig, use_container_width=True)

with chart_col4:
    salary_data = filtered[
        filtered['salary_min'].notna() &
        (filtered['salary_min'] > 0) &
        (filtered['salary_min'] < 500_000)
    ]
    if len(salary_data) > 5:
        fig = px.histogram(
            salary_data,
            x='salary_min',
            nbins=25,
            title="Salary Distribution (NPR/month)",
            color_discrete_sequence=['#f0a500'],
            labels={'salary_min': 'Minimum Salary (NPR)'}
        )
        median = salary_data['salary_min'].median()
        fig.add_vline(
            x=median,
            line_dash="dash",
            line_color="#e74c3c",
            annotation_text=f"Median: NPR {median:,.0f}"
        )
        fig.update_layout(height=400, plot_bgcolor='white', margin=dict(l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough salary data to display distribution chart.")

# ══════════════════════════════════════════════════════════════
#  SKILLS CHART (full width)
# ══════════════════════════════════════════════════════════════

st.markdown("### 🛠 In-Demand Skills")
all_skills = []
for s in filtered['skills'].dropna():
    if s and s.strip():
        for skill in s.split(','):
            skill = skill.strip()
            if skill and skill.lower() not in ('n/a', 'na', ''):
                all_skills.append(skill)

if len(all_skills) > 5:
    skill_series = pd.Series(Counter(all_skills)).sort_values(ascending=False).head(20)
    fig = px.bar(
        x=skill_series.index,
        y=skill_series.values,
        title="Top 20 Skills Required by Employers",
        color=skill_series.values,
        color_continuous_scale='Plasma',
        labels={'x': 'Skill', 'y': 'Number of Jobs'},
        text=skill_series.values
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(
        height=420,
        showlegend=False,
        xaxis_tickangle=-35,
        plot_bgcolor='white',
        margin=dict(l=0, r=0)
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(
        "Skills data comes from MeroJob only. "
        "Make sure MeroJob scraper has run and returned results with skills."
    )

# ══════════════════════════════════════════════════════════════
#  JOB LISTINGS TABLE  (searchable)
# ══════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("### 📋 Browse All Job Listings")

search_term = st.text_input(
    "🔎 Search jobs (by title or company name)",
    placeholder="e.g. Software Engineer, Bajaj, IT Manager..."
)

display_df = filtered.copy()
if search_term:
    mask = (
        display_df['title'].str.contains(search_term, case=False, na=False) |
        display_df['company'].str.contains(search_term, case=False, na=False)
    )
    display_df = display_df[mask]
    st.caption(f"Found **{len(display_df)}** results for '{search_term}'")

# Select columns to show in the table
show_cols = ['title', 'company', 'location', 'category', 'job_level',
             'salary_min', 'salary_max', 'experience', 'source', 'job_url']
show_cols = [c for c in show_cols if c in display_df.columns]  # safety check

st.dataframe(
    display_df[show_cols].rename(columns={
        'title': 'Job Title',
        'company': 'Company',
        'location': 'Location',
        'category': 'Category',
        'job_level': 'Level',
        'salary_min': 'Min Salary (NPR)',
        'salary_max': 'Max Salary (NPR)',
        'experience': 'Experience',
        'source': 'Source',
        'job_url': 'Link'
    }),
    use_container_width=True,
    height=500
)

# ── Download Button ────────────────────────────────────────
csv_data = display_df[show_cols].to_csv(index=False, encoding='utf-8-sig')
st.download_button(
    label=f"⬇️ Download {len(display_df)} jobs as CSV",
    data=csv_data,
    file_name=f"nepal_jobs_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv"
)

# ══════════════════════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════════════════════

st.markdown("---")
st.caption(
    "Nepal Job Market Analytics Dashboard | "
    "Data scraped from MeroJob.com and KumariJob.com | "
    f"Showing {len(filtered):,} of {len(df):,} total jobs"
)
