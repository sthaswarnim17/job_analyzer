"""
report_generator.py — Automated HTML & Excel Report Generation
===============================================================
Covers Chapter 4 of the syllabus:
  4.1 Automated report generation (Excel, HTML)
  4.5 Data storage and retrieval strategies for pipelines
  4.6 Case study: End-to-end automated analytics pipeline

After each pipeline run, this module generates:
  1. reports/report_YYYYMMDD.html  — styled HTML summary report
  2. reports/report_YYYYMMDD.xlsx  — Excel workbook with multiple sheets

Run standalone: python report_generator.py
Called from  :  scheduler.py (after clean_and_merge step)
"""

import os
import sqlite3
from datetime import datetime

import pandas as pd

from logger import get_logger

logger = get_logger("job_analyzer.report")

DB_PATH     = "jobs.db"
REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  LOAD DATA
# ══════════════════════════════════════════════════════════════

def _load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM jobs_clean", conn)
    except Exception as exc:
        logger.error(f"Could not load data: {exc}")
        conn.close()
        raise
    conn.close()
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    return df


# ══════════════════════════════════════════════════════════════
#  COMPUTE SUMMARY STATS
# ══════════════════════════════════════════════════════════════

def _build_summary(df: pd.DataFrame) -> dict:
    """Returns a dict of key metrics used in the HTML report."""
    sal = df[df["salary_min"].notna() & (df["salary_min"] > 0)]["salary_min"]

    top_cats  = df["category"].value_counts().head(5)
    top_locs  = (df[df["location"] != "Unknown"]["location"]
                 .value_counts().head(5))
    top_levels = df["job_level"].value_counts()

    return {
        "generated_at"      : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_jobs"        : len(df),
        "total_companies"   : df["company"].nunique(),
        "total_categories"  : df["category"].nunique(),
        "total_cities"      : df["location"].nunique(),
        "merojob_count"     : int((df["source"] == "merojob").sum()),
        "kumari_count"      : int((df["source"] == "kumarijob").sum()),
        "jobs_with_salary"  : int(sal.shape[0]),
        "salary_mean"       : sal.mean()   if len(sal) > 0 else 0,
        "salary_median"     : sal.median() if len(sal) > 0 else 0,
        "salary_min"        : sal.min()    if len(sal) > 0 else 0,
        "salary_max"        : sal.max()    if len(sal) > 0 else 0,
        "top_cats"          : top_cats,
        "top_locs"          : top_locs,
        "top_levels"        : top_levels,
        "df"                : df,
    }


# ══════════════════════════════════════════════════════════════
#  HTML REPORT GENERATOR
# ══════════════════════════════════════════════════════════════

def generate_html_report(df: pd.DataFrame = None) -> str:
    """
    Generates a self-contained styled HTML analytics report and
    saves it to the /reports directory.

    Args:
        df (pd.DataFrame): Optional pre-loaded dataframe.

    Returns:
        str: Absolute path to the generated HTML file.
    """
    if df is None:
        df = _load_data()

    s = _build_summary(df)
    date_str  = datetime.now().strftime("%Y%m%d")
    file_name = f"report_{date_str}.html"
    file_path = os.path.join(REPORTS_DIR, file_name)

    # Build category rows
    cat_rows = "".join(
        f"<tr><td>{cat}</td><td>{count}</td></tr>"
        for cat, count in s["top_cats"].items()
    )
    loc_rows = "".join(
        f"<tr><td>{loc}</td><td>{count}</td></tr>"
        for loc, count in s["top_locs"].items()
    )
    level_rows = "".join(
        f"<tr><td>{level}</td><td>{count}</td></tr>"
        for level, count in s["top_levels"].items()
    )

    # Recent 20 jobs table
    recent = df.sort_values("scraped_at", ascending=False).head(20)
    job_rows = "".join(
        f"<tr>"
        f"<td>{row['title']}</td>"
        f"<td>{row['company']}</td>"
        f"<td>{row['location']}</td>"
        f"<td>{row['job_level']}</td>"
        f"<td>{'NPR {:,.0f}'.format(row['salary_min']) if pd.notna(row.get('salary_min')) and row.get('salary_min', 0) > 0 else 'N/A'}</td>"
        f"<td>{row['source']}</td>"
        f"</tr>"
        for _, row in recent.iterrows()
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Nepal Job Market Report — {s['generated_at'][:10]}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Segoe UI', Arial, sans-serif;
      background: #f0f4f8;
      color: #1e293b;
      padding: 24px;
    }}
    header {{
      background: linear-gradient(135deg, #1d4ed8 0%, #0f172a 100%);
      color: white;
      padding: 28px 32px;
      border-radius: 12px;
      margin-bottom: 28px;
    }}
    header h1 {{ font-size: 1.8rem; margin-bottom: 6px; }}
    header p  {{ opacity: 0.78; font-size: 0.92rem; }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }}
    .kpi-card {{
      background: white;
      border-radius: 10px;
      padding: 18px 20px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.08);
      border-left: 4px solid #2563eb;
    }}
    .kpi-card .value {{ font-size: 1.7rem; font-weight: 700; color: #1d4ed8; }}
    .kpi-card .label {{ font-size: 0.82rem; color: #64748b; margin-top: 4px; }}
    .section {{
      background: white;
      border-radius: 10px;
      padding: 22px 24px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.08);
      margin-bottom: 24px;
    }}
    .section h2 {{
      font-size: 1.08rem;
      font-weight: 700;
      color: #1e293b;
      margin-bottom: 16px;
      padding-bottom: 10px;
      border-bottom: 2px solid #e2e8f0;
    }}
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
    th {{
      background: #f1f5f9;
      text-align: left;
      padding: 9px 12px;
      font-weight: 600;
      color: #475569;
      border-bottom: 2px solid #e2e8f0;
    }}
    td {{ padding: 8px 12px; border-bottom: 1px solid #f1f5f9; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #f8fafc; }}
    .badge {{
      display: inline-block;
      padding: 2px 9px;
      border-radius: 99px;
      font-size: 0.78rem;
      font-weight: 600;
    }}
    .badge-blue   {{ background:#dbeafe; color:#1d4ed8; }}
    .badge-red    {{ background:#fee2e2; color:#dc2626; }}
    .salary-highlight {{ color: #16a34a; font-weight: 600; }}
    footer {{
      text-align: center;
      color: #94a3b8;
      font-size: 0.82rem;
      margin-top: 28px;
    }}
  </style>
</head>
<body>

<header>
  <h1>Nepal Job Market — Analytics Report</h1>
  <p>Generated on {s['generated_at']} &nbsp;|&nbsp; Data from MeroJob and KumariJob</p>
</header>

<!-- KPI Cards -->
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="value">{s['total_jobs']:,}</div>
    <div class="label">Total Job Listings</div>
  </div>
  <div class="kpi-card">
    <div class="value">{s['total_companies']:,}</div>
    <div class="label">Companies Hiring</div>
  </div>
  <div class="kpi-card">
    <div class="value">{s['total_categories']:,}</div>
    <div class="label">Job Categories</div>
  </div>
  <div class="kpi-card">
    <div class="value">{s['total_cities']:,}</div>
    <div class="label">Cities Covered</div>
  </div>
  <div class="kpi-card" style="border-left-color:#16a34a;">
    <div class="value" style="color:#16a34a;">NPR {s['salary_median']:,.0f}</div>
    <div class="label">Median Salary (NPR)</div>
  </div>
  <div class="kpi-card" style="border-left-color:#d97706;">
    <div class="value" style="color:#d97706;">{s['jobs_with_salary']:,}</div>
    <div class="label">Jobs with Salary Data</div>
  </div>
</div>

<!-- Source breakdown + salary stats -->
<div class="grid-2">
  <div class="section">
    <h2>Data Sources</h2>
    <table>
      <tr><th>Portal</th><th>Listings</th></tr>
      <tr>
        <td><span class="badge badge-blue">MeroJob</span></td>
        <td><strong>{s['merojob_count']:,}</strong></td>
      </tr>
      <tr>
        <td><span class="badge badge-red">KumariJob</span></td>
        <td><strong>{s['kumari_count']:,}</strong></td>
      </tr>
    </table>
  </div>
  <div class="section">
    <h2>Salary Statistics (NPR)</h2>
    <table>
      <tr><th>Metric</th><th>Value</th></tr>
      <tr><td>Mean</td>   <td class="salary-highlight">NPR {s['salary_mean']:,.0f}</td></tr>
      <tr><td>Median</td> <td class="salary-highlight">NPR {s['salary_median']:,.0f}</td></tr>
      <tr><td>Minimum</td><td>NPR {s['salary_min']:,.0f}</td></tr>
      <tr><td>Maximum</td><td>NPR {s['salary_max']:,.0f}</td></tr>
    </table>
  </div>
</div>

<!-- Top categories + locations -->
<div class="grid-2">
  <div class="section">
    <h2>Top 5 Job Categories</h2>
    <table>
      <tr><th>Category</th><th>Listings</th></tr>
      {cat_rows}
    </table>
  </div>
  <div class="section">
    <h2>Top 5 Locations</h2>
    <table>
      <tr><th>City</th><th>Listings</th></tr>
      {loc_rows}
    </table>
  </div>
</div>

<!-- Job levels -->
<div class="section">
  <h2>Job Level Distribution</h2>
  <table>
    <tr><th>Level</th><th>Count</th></tr>
    {level_rows}
  </table>
</div>

<!-- Recent jobs -->
<div class="section">
  <h2>Most Recently Scraped Listings (Top 20)</h2>
  <table>
    <tr>
      <th>Title</th><th>Company</th><th>Location</th>
      <th>Level</th><th>Salary (Min)</th><th>Source</th>
    </tr>
    {job_rows}
  </table>
</div>

<footer>
  Nepal Job Market Trend Analyzer — Advanced Python Programming for Data Science Project<br>
  Report auto-generated by report_generator.py
</footer>
</body>
</html>
"""

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"HTML report saved: {os.path.abspath(file_path)}")
    return os.path.abspath(file_path)


# ══════════════════════════════════════════════════════════════
#  EXCEL REPORT GENERATOR
# ══════════════════════════════════════════════════════════════

def generate_excel_report(df: pd.DataFrame = None) -> str:
    """
    Generates a multi-sheet Excel workbook:
      Sheet 1 — All Jobs (full data)
      Sheet 2 — Category Summary
      Sheet 3 — Location Summary
      Sheet 4 — Salary Statistics

    Args:
        df (pd.DataFrame): Optional pre-loaded dataframe.

    Returns:
        str: Absolute path to the generated .xlsx file.
    """
    if df is None:
        df = _load_data()

    date_str  = datetime.now().strftime("%Y%m%d")
    file_path = os.path.join(REPORTS_DIR, f"report_{date_str}.xlsx")

    # Sheet 2 — Category summary
    cat_summary = (
        df.groupby("category")
        .agg(
            Total_Jobs  =("title",      "count"),
            Avg_Salary  =("salary_min", "mean"),
            Median_Sal  =("salary_min", "median"),
            Companies   =("company",    "nunique"),
        )
        .sort_values("Total_Jobs", ascending=False)
        .reset_index()
    )

    # Sheet 3 — Location summary
    loc_summary = (
        df[df["location"] != "Unknown"]
        .groupby("location")
        .agg(
            Total_Jobs  =("title",      "count"),
            Avg_Salary  =("salary_min", "mean"),
            Companies   =("company",    "nunique"),
        )
        .sort_values("Total_Jobs", ascending=False)
        .reset_index()
    )

    # Sheet 4 — Salary stats per level
    sal_stats = (
        df[df["salary_min"].notna() & (df["salary_min"] > 0)]
        .groupby("job_level")["salary_min"]
        .agg(["count", "mean", "median", "std", "min", "max"])
        .rename(columns={"count":"Count","mean":"Mean","median":"Median",
                         "std":"Std_Dev","min":"Min","max":"Max"})
        .sort_values("Median", ascending=False)
        .reset_index()
    )

    # Export columns for Sheet 1
    export_cols = ["title", "company", "location", "category",
                   "job_level", "salary_min", "salary_max",
                   "source", "scraped_at", "job_url"]
    sheet1 = df[[c for c in export_cols if c in df.columns]].copy()
    sheet1.columns = [c.replace("_", " ").title() for c in sheet1.columns]

    try:
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            sheet1.to_excel(     writer, sheet_name="All Jobs",         index=False)
            cat_summary.to_excel(writer, sheet_name="By Category",      index=False)
            loc_summary.to_excel(writer, sheet_name="By Location",      index=False)
            sal_stats.to_excel(  writer, sheet_name="Salary by Level",  index=False)

        logger.info(f"Excel report saved: {os.path.abspath(file_path)}")
        return os.path.abspath(file_path)

    except ImportError:
        logger.warning("openpyxl not installed — skipping Excel report. Run: pip install openpyxl")
        return ""


# ══════════════════════════════════════════════════════════════
#  MAIN — generate both reports in one call
# ══════════════════════════════════════════════════════════════

def generate_all_reports() -> tuple:
    """
    Convenience function: loads data once, generates both
    HTML and Excel reports, returns their file paths.

    Returns:
        tuple: (html_path, excel_path)
    """
    logger.info("=== Report generation started ===")
    df        = _load_data()
    html_path = generate_html_report(df)
    xlsx_path = generate_excel_report(df)
    logger.info("=== Report generation complete ===")
    return html_path, xlsx_path


if __name__ == "__main__":
    html_p, xlsx_p = generate_all_reports()
    print(f"\nReports generated:")
    print(f"  HTML  : {html_p}")
    if xlsx_p:
        print(f"  Excel : {xlsx_p}")
    print(f"\nOpen the HTML file in your browser to view the report.")
