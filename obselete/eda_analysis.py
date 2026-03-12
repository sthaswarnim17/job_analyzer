"""
eda_analysis.py — Exploratory Data Analysis
=============================================
Loads cleaned jobs data and produces 6 analysis charts.
Charts are saved to the /charts folder as interactive HTML files.

Run this after the pipeline has collected some data:
  python eda_analysis.py
"""

import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from collections import Counter
import os

# ══════════════════════════════════════════════════════════════
#  LOAD DATA
# ══════════════════════════════════════════════════════════════

DB_PATH = "jobs.db"
CHARTS_DIR = "charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)
try:
    df = pd.read_sql("SELECT * FROM jobs_clean", conn)
    conn.close()
except Exception as e:
    conn.close()
    print(f"❌ Could not load data: {e}")
    print("   Make sure you've run scheduler.py at least once first.")
    exit(1)

print(f"✅ Loaded {len(df)} jobs from database")
print(f"   Sources: {df['source'].value_counts().to_dict()}")
print(f"   Date range: {df['scraped_at'].min()} to {df['scraped_at'].max()}")
print()

# ══════════════════════════════════════════════════════════════
#  BASIC STATISTICS (print to terminal — for your report)
# ══════════════════════════════════════════════════════════════

print("═" * 50)
print("BASIC STATISTICS")
print("═" * 50)
print(f"Total job listings:      {len(df)}")
print(f"Unique companies:        {df['company'].nunique()}")
print(f"Unique job categories:   {df['category'].nunique()}")
print(f"Unique locations:        {df['location'].nunique()}")
print(f"Jobs with salary info:   {df['salary_min'].notna().sum()}")
print(f"Average salary (min):    NPR {df['salary_min'].mean():,.0f}" if df['salary_min'].notna().any() else "N/A")
print(f"Median salary (min):     NPR {df['salary_min'].median():,.0f}" if df['salary_min'].notna().any() else "N/A")
print()
print("Missing values per column:")
print(df.isnull().sum().to_string())
print()


# ══════════════════════════════════════════════════════════════
#  CHART 1: Top 15 Job Categories (Horizontal Bar)
# ══════════════════════════════════════════════════════════════

print("Creating Chart 1: Top Job Categories...")

cat_counts = df['category'].value_counts().head(15)
fig1 = px.bar(
    x=cat_counts.values,
    y=cat_counts.index,
    orientation='h',
    title="<b>Top 15 Job Categories in Nepal</b>",
    labels={'x': 'Number of Job Listings', 'y': 'Category'},
    color=cat_counts.values,
    color_continuous_scale='Viridis',
    text=cat_counts.values
)
fig1.update_traces(textposition='outside')
fig1.update_layout(
    height=550,
    showlegend=False,
    plot_bgcolor='white',
    font=dict(size=13)
)
fig1.write_html(f"{CHARTS_DIR}/01_top_categories.html")
print(f"  ✅ Saved: {CHARTS_DIR}/01_top_categories.html")


# ══════════════════════════════════════════════════════════════
#  CHART 2: Jobs by City (Donut Chart)
# ══════════════════════════════════════════════════════════════

print("Creating Chart 2: Jobs by City...")

loc_counts = df[df['location'] != 'Unknown']['location'].value_counts().head(12)
fig2 = px.pie(
    values=loc_counts.values,
    names=loc_counts.index,
    title="<b>Job Distribution by City</b>",
    hole=0.45,
    color_discrete_sequence=px.colors.qualitative.Set3
)
fig2.update_traces(textposition='inside', textinfo='percent+label')
fig2.update_layout(height=500, font=dict(size=13))
fig2.write_html(f"{CHARTS_DIR}/02_location_distribution.html")
print(f"  ✅ Saved: {CHARTS_DIR}/02_location_distribution.html")


# ══════════════════════════════════════════════════════════════
#  CHART 3: Job Level Distribution (Bar Chart)
# ══════════════════════════════════════════════════════════════

print("Creating Chart 3: Job Level Distribution...")

level_order = ['Entry Level', 'Mid Level', 'Senior Level', 'Management', 'Not Specified']
level_counts = df['job_level'].value_counts()
# Reorder to our standard order (only include levels that exist)
level_counts = level_counts.reindex([l for l in level_order if l in level_counts.index])

fig3 = px.bar(
    x=level_counts.index,
    y=level_counts.values,
    title="<b>Jobs by Experience Level</b>",
    labels={'x': 'Job Level', 'y': 'Number of Jobs'},
    color=level_counts.index,
    color_discrete_map={
        'Entry Level': '#2ecc71',
        'Mid Level': '#3498db',
        'Senior Level': '#9b59b6',
        'Management': '#e74c3c',
        'Not Specified': '#95a5a6'
    },
    text=level_counts.values
)
fig3.update_traces(textposition='outside', showlegend=False)
fig3.update_layout(height=450, plot_bgcolor='white', font=dict(size=13))
fig3.write_html(f"{CHARTS_DIR}/03_job_levels.html")
print(f"  ✅ Saved: {CHARTS_DIR}/03_job_levels.html")


# ══════════════════════════════════════════════════════════════
#  CHART 4: Salary Distribution (Histogram)
# ══════════════════════════════════════════════════════════════

print("Creating Chart 4: Salary Distribution...")

salary_df = df[df['salary_min'].notna() & (df['salary_min'] > 0) & (df['salary_min'] < 500_000)]

if len(salary_df) > 5:
    fig4 = px.histogram(
        salary_df,
        x='salary_min',
        nbins=30,
        title="<b>Salary Distribution (Minimum Monthly Salary, NPR)</b>",
        labels={'salary_min': 'Minimum Salary (NPR)', 'count': 'Number of Jobs'},
        color_discrete_sequence=['#f0a500']
    )
    fig4.update_layout(
        height=450,
        plot_bgcolor='white',
        bargap=0.05,
        font=dict(size=13)
    )
    # Add a vertical median line
    median_sal = salary_df['salary_min'].median()
    fig4.add_vline(
        x=median_sal,
        line_dash="dash",
        line_color="#e74c3c",
        annotation_text=f"Median: NPR {median_sal:,.0f}",
        annotation_position="top right"
    )
    fig4.write_html(f"{CHARTS_DIR}/04_salary_distribution.html")
    print(f"  ✅ Saved: {CHARTS_DIR}/04_salary_distribution.html")
else:
    print("  ⚠️  Not enough salary data for chart (less than 5 entries with salary)")


# ══════════════════════════════════════════════════════════════
#  CHART 5: Top 20 In-Demand Skills
# ══════════════════════════════════════════════════════════════

print("Creating Chart 5: Top Skills...")

all_skills = []
for skills_str in df['skills'].dropna():
    if skills_str and skills_str.strip():
        for skill in skills_str.split(','):
            skill = skill.strip()
            if skill and skill.lower() not in ('n/a', 'na', ''):
                all_skills.append(skill)

if len(all_skills) > 10:
    skill_counts = pd.Series(Counter(all_skills)).sort_values(ascending=False).head(20)
    fig5 = px.bar(
        x=skill_counts.index,
        y=skill_counts.values,
        title="<b>Top 20 In-Demand Skills (from MeroJob)</b>",
        labels={'x': 'Skill', 'y': 'Frequency (Number of Jobs Mentioning It)'},
        color=skill_counts.values,
        color_continuous_scale='Plasma',
        text=skill_counts.values
    )
    fig5.update_traces(textposition='outside')
    fig5.update_layout(
        height=500,
        showlegend=False,
        xaxis_tickangle=-35,
        plot_bgcolor='white',
        font=dict(size=12)
    )
    fig5.write_html(f"{CHARTS_DIR}/05_top_skills.html")
    print(f"  ✅ Saved: {CHARTS_DIR}/05_top_skills.html")
else:
    print("  ⚠️  Not enough skills data. Skills data comes from MeroJob API.")


# ══════════════════════════════════════════════════════════════
#  CHART 6: Source Comparison (MeroJob vs KumariJob)
# ══════════════════════════════════════════════════════════════

print("Creating Chart 6: Source Breakdown...")

source_counts = df['source'].value_counts()
fig6 = px.pie(
    values=source_counts.values,
    names=source_counts.index,
    title="<b>Job Listings by Source Website</b>",
    color_discrete_map={'merojob': '#3498db', 'kumarijob': '#e74c3c'},
    hole=0.4
)
fig6.update_traces(textinfo='percent+value+label')
fig6.update_layout(height=450, font=dict(size=14))
fig6.write_html(f"{CHARTS_DIR}/06_source_breakdown.html")
print(f"  ✅ Saved: {CHARTS_DIR}/06_source_breakdown.html")


# ══════════════════════════════════════════════════════════════
#  CHART 7: Salary by Category (Box Plot)
# ══════════════════════════════════════════════════════════════

print("Creating Chart 7: Salary by Category...")

sal_cat = df[df['salary_min'].notna() & (df['salary_min'] > 0) & (df['salary_min'] < 300_000)]

# Only keep categories with at least 3 salary entries
valid_cats = sal_cat['category'].value_counts()
valid_cats = valid_cats[valid_cats >= 3].index.tolist()
sal_cat = sal_cat[sal_cat['category'].isin(valid_cats)]

if len(sal_cat) > 10:
    fig7 = px.box(
        sal_cat,
        x='category',
        y='salary_min',
        title="<b>Salary Range by Job Category (NPR)</b>",
        labels={'category': 'Job Category', 'salary_min': 'Minimum Salary (NPR)'},
        color='category',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig7.update_layout(
        height=550,
        showlegend=False,
        xaxis_tickangle=-35,
        plot_bgcolor='white',
        font=dict(size=12)
    )
    fig7.write_html(f"{CHARTS_DIR}/07_salary_by_category.html")
    print(f"  ✅ Saved: {CHARTS_DIR}/07_salary_by_category.html")
else:
    print("  ⚠️  Not enough salary+category data for box plot")


# ══════════════════════════════════════════════════════════════
#  DONE
# ══════════════════════════════════════════════════════════════

print()
print("═" * 50)
print("📊 EDA COMPLETE")
print(f"   All charts saved to /{CHARTS_DIR}/")
print("   Open any .html file in your browser to view it.")
print("   These charts can also be embedded in your report.")
print("═" * 50)
