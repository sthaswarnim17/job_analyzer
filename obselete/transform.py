with open("report_generator.py", "r") as f:
    content = f.read()

# Replace Imports
imports_target = """import pandas as pd

from logger import get_logger"""

imports_replacement = """from collections import Counter
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from logger import get_logger"""
content = content.replace(imports_target, imports_replacement)


# Add Plot Generation
func_target = """# ══════════════════════════════════════════════════════════════
#  HTML REPORT GENERATOR
# ══════════════════════════════════════════════════════════════"""

func_replacement = """# ══════════════════════════════════════════════════════════════
#  PLOT GENERATION
# ══════════════════════════════════════════════════════════════

def _generate_plots(df: pd.DataFrame) -> dict:
    \"\"\"Generates EDA plots and returns a dict mapping plot name to base64 or file paths. Using file paths relative to report.\"\"\"
    plots = {}
    sns.set_style("whitegrid")
    
    # 1. Top 15 Categories
    cat_counts = df['category'].value_counts().head(15)
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(cat_counts.index[::-1], cat_counts.values[::-1], color=sns.color_palette("viridis", len(cat_counts)))
    ax.bar_label(bars, padding=3)
    ax.set_xlabel("Number of Job Listings")
    ax.set_title("Top 15 Job Categories in Nepal", fontsize=14, fontweight='bold')
    plt.tight_layout()
    plot_path = os.path.join("reports", "plot_categories.png")
    plt.savefig(plot_path, dpi=120)
    plt.close(fig)
    plots['categories'] = "plot_categories.png"
    
    # 2. Top 12 Cities Donut
    loc_counts = df[df['location'] != 'Unknown']['location'].value_counts().head(12)
    fig, ax = plt.subplots(figsize=(8, 8))
    colors = sns.color_palette("Set3", len(loc_counts))
    wedges, texts, autotexts = ax.pie(
        loc_counts.values, labels=loc_counts.index, autopct='%1.1f%%',
        colors=colors, pctdistance=0.8, startangle=90,
        wedgeprops=dict(width=0.55)
    )
    ax.set_title("Job Distribution by City", fontsize=14, fontweight='bold')
    plt.tight_layout()
    plot_path = os.path.join("reports", "plot_locations.png")
    plt.savefig(plot_path, dpi=120)
    plt.close(fig)
    plots['locations'] = "plot_locations.png"

    # 3. Job Levels Bar
    level_order = ['Entry Level', 'Mid Level', 'Senior Level', 'Management', 'Not Specified']
    level_counts = df['job_level'].value_counts()
    level_counts = level_counts.reindex([l for l in level_order if l in level_counts.index])
    color_map = {
        'Entry Level': '#2ecc71', 'Mid Level': '#3498db',
        'Senior Level': '#9b59b6', 'Management': '#e74c3c', 'Not Specified': '#95a5a6'
    }
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(level_counts.index, level_counts.values,
                  color=[color_map.get(l, '#aaa') for l in level_counts.index])
    ax.bar_label(bars, padding=3)
    ax.set_xlabel("Job Level")
    ax.set_ylabel("Number of Jobs")
    ax.set_title("Jobs by Experience Level", fontsize=14, fontweight='bold')
    plt.tight_layout()
    plot_path = os.path.join("reports", "plot_levels.png")
    plt.savefig(plot_path, dpi=120)
    plt.close(fig)
    plots['levels'] = "plot_levels.png"

    # 4. Salary Distribution Hist
    salary_df = df[(df['salary_min'].notna()) & (df['salary_min'] > 0) & (df['salary_min'] < 500_000)]
    if len(salary_df) > 5:
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.histplot(salary_df['salary_min'], bins=30, kde=True, color='#f0a500', ax=ax)
        median_sal = salary_df['salary_min'].median()
        mean_sal = salary_df['salary_min'].mean()
        ax.axvline(median_sal, color='#e74c3c', linestyle='--', linewidth=1.5, label=f'Median: NPR {median_sal:,.0f}')
        ax.axvline(mean_sal, color='#2980b9', linestyle='--', linewidth=1.5, label=f'Mean: NPR {mean_sal:,.0f}')
        ax.set_xlabel("Minimum Salary (NPR)")
        ax.set_ylabel("Number of Jobs")
        ax.set_title("Salary Distribution (Minimum Monthly Salary, NPR)", fontsize=14, fontweight='bold')
        ax.legend()
        plt.tight_layout()
        plot_path = os.path.join("reports", "plot_salary_dist.png")
        plt.savefig(plot_path, dpi=120)
        plt.close(fig)
        plots['salary_dist'] = "plot_salary_dist.png"

    # 5. Top 20 Skills
    all_skills = []
    for skills_str in df['skills'].dropna():
        if skills_str and skills_str.strip():
            for skill in skills_str.split(','):
                skill = skill.strip()
                if skill and skill.lower() not in ('n/a', 'na', ''):
                    all_skills.append(skill)
    if len(all_skills) > 10:
        skill_counts = pd.Series(Counter(all_skills)).sort_values(ascending=False).head(20)
        fig, ax = plt.subplots(figsize=(12, 5))
        bars = ax.bar(skill_counts.index, skill_counts.values, color=sns.color_palette("plasma", len(skill_counts)))
        ax.bar_label(bars, padding=3, fontsize=8)
        ax.set_xlabel("Skill")
        ax.set_ylabel("Frequency")
        ax.set_title("Top 20 In-Demand Skills", fontsize=14, fontweight='bold')
        plt.xticks(rotation=40, ha='right')
        plt.tight_layout()
        plot_path = os.path.join("reports", "plot_skills.png")
        plt.savefig(plot_path, dpi=120)
        plt.close(fig)
        plots['skills'] = "plot_skills.png"

    # 6. Source Distribution Pie
    source_counts = df['source'].value_counts()
    fig, ax = plt.subplots(figsize=(6, 6))
    colors_dict = {'merojob': '#3498db', 'kumarijob': '#e74c3c'}
    ax.pie(
        source_counts.values, labels=source_counts.index,
        autopct=lambda p: f'{p:.1f}%\\n({int(p*sum(source_counts.values)/100)})',
        colors=[colors_dict.get(s, '#aaa') for s in source_counts.index],
        startangle=90, wedgeprops=dict(width=0.6)
    )
    ax.set_title("Job Listings by Source Website", fontsize=14, fontweight='bold')
    plt.tight_layout()
    plot_path = os.path.join("reports", "plot_sources.png")
    plt.savefig(plot_path, dpi=120)
    plt.close(fig)
    plots['sources'] = "plot_sources.png"

    # 7. Salary box plot
    sal_cat = df[(df['salary_min'].notna()) & (df['salary_min'] > 0) & (df['salary_min'] < 300_000)]
    valid_cats = sal_cat['category'].value_counts()
    valid_cats = valid_cats[valid_cats >= 3].index.tolist()
    sal_cat = sal_cat[sal_cat['category'].isin(valid_cats)]
    if len(sal_cat) > 10:
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.boxplot(data=sal_cat, x='category', y='salary_min', palette='Pastel1', ax=ax)
        ax.set_xlabel("Job Category")
        ax.set_ylabel("Minimum Salary (NPR)")
        ax.set_title("Salary Range by Job Category (NPR)", fontsize=14, fontweight='bold')
        plt.xticks(rotation=40, ha='right')
        plt.tight_layout()
        plot_path = os.path.join("reports", "plot_salary_box.png")
        plt.savefig(plot_path, dpi=120)
        plt.close(fig)
        plots['salary_box'] = "plot_salary_box.png"
        
    return plots

# ══════════════════════════════════════════════════════════════
#  HTML REPORT GENERATOR
# ══════════════════════════════════════════════════════════════"""
content = content.replace(func_target, func_replacement)


func_call_target = """    s = _build_summary(df)
    date_str  = datetime.now().strftime("%Y%m%d")"""
func_call_replacement = """    s = _build_summary(df)
    plots = _generate_plots(df)
    date_str  = datetime.now().strftime("%Y%m%d")"""
content = content.replace(func_call_target, func_call_replacement)

html_target = """<!-- Recent jobs -->
<div class="section">"""

html_replacement = """<!-- EDA Visualizations -->
<div class="section">
  <h2>EDA Visualizations</h2>
  <div style="display:flex; flex-wrap:wrap; gap: 20px; justify-content: center;">
    {f'<img src="{plots["categories"]}" style="max-width: 48%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'categories' in plots else ''}
    {f'<img src="{plots["locations"]}" style="max-width: 48%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'locations' in plots else ''}
    {f'<img src="{plots["levels"]}" style="max-width: 48%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'levels' in plots else ''}
    {f'<img src="{plots["salary_dist"]}" style="max-width: 48%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'salary_dist' in plots else ''}
    {f'<img src="{plots["skills"]}" style="max-width: 98%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'skills' in plots else ''}
    {f'<img src="{plots["sources"]}" style="max-width: 48%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'sources' in plots else ''}
    {f'<img src="{plots["salary_box"]}" style="max-width: 98%; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">' if 'salary_box' in plots else ''}
  </div>
</div>

<!-- Recent jobs -->
<div class="section">"""
content = content.replace(html_target, html_replacement)

excel_docs_target = """      Sheet 3 — Location Summary
      Sheet 4 — Salary Statistics"""

excel_docs_replacement = """      Sheet 3 — Location Summary
      Sheet 4 — Salary Statistics
      Sheet 5 — Top Skills"""
content = content.replace(excel_docs_target, excel_docs_replacement)

excel_logic_target = """    # Export columns for Sheet 1
    export_cols = ["title", "company", "location", "category","""

excel_logic_replacement = """    # Sheet 5 — Top Skills (Skills extraction)
    all_skills = []
    for skills_str in df['skills'].dropna():
        if skills_str and skills_str.strip():
            for skill in skills_str.split(','):
                skill = skill.strip()
                if skill and skill.lower() not in ('n/a', 'na', ''):
                    all_skills.append(skill)
    if len(all_skills) > 0:
        skill_summary = pd.Series(Counter(all_skills)).sort_values(ascending=False).head(50).reset_index()
        skill_summary.columns = ["Skill", "Frequency"]
    else:
        skill_summary = pd.DataFrame(columns=["Skill", "Frequency"])

    # Export columns for Sheet 1
    export_cols = ["title", "company", "location", "category","""
content = content.replace(excel_logic_target, excel_logic_replacement)

excel_sheet_target = """            loc_summary.to_excel(writer, sheet_name="By Location",      index=False)
            sal_stats.to_excel(  writer, sheet_name="Salary by Level",  index=False)"""

excel_sheet_replacement = """            loc_summary.to_excel(writer, sheet_name="By Location",      index=False)
            sal_stats.to_excel(  writer, sheet_name="Salary by Level",  index=False)
            skill_summary.to_excel(writer, sheet_name="Top Skills",     index=False)"""
content = content.replace(excel_sheet_target, excel_sheet_replacement)

with open("report_generator.py", "w") as f:
    f.write(content)
