"""
visualizations.py — Static Charts with Matplotlib and Seaborn
==============================================================
Covers Chapter 5 of the syllabus:
  5.2 Matplotlib : line, bar, histogram, scatter, subplots
  5.3 Seaborn    : box plot, heatmap, distribution, violin, pair plot

All charts are saved as high-resolution PNG files to /charts.

Run: python visualizations.py
"""

import os
import sqlite3
import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from collections import Counter

from logger import get_logger

warnings.filterwarnings("ignore")
logger = get_logger("job_analyzer.visualizations")

# ── Config ─────────────────────────────────────────────────────
DB_PATH    = "jobs.db"
CHARTS_DIR = "charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

# Global Seaborn theme (applies to all seaborn + matplotlib plots)
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams.update({
    "figure.dpi":       130,
    "savefig.dpi":      150,
    "savefig.bbox":     "tight",
    "axes.spines.top":  False,
    "axes.spines.right":False,
})

ACCENT  = "#2563EB"   # Blue
DANGER  = "#DC2626"   # Red
SUCCESS = "#16A34A"   # Green
WARN    = "#D97706"   # Amber


# ══════════════════════════════════════════════════════════════
#  LOAD DATA
# ══════════════════════════════════════════════════════════════

def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM jobs_clean", conn)
    except Exception as exc:
        logger.error(f"Could not load jobs_clean table: {exc}")
        conn.close()
        raise
    conn.close()
    # Numeric coercion
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    logger.info(f"Loaded {len(df)} job records from database")
    return df


# ══════════════════════════════════════════════════════════════
#  CHART 1 — Matplotlib Horizontal Bar: Top Job Categories
# ══════════════════════════════════════════════════════════════

def plot_top_categories(df: pd.DataFrame):
    """
    Matplotlib horizontal bar chart showing the 12 most common
    job categories. Bars are colour-coded by count via a colormap.
    """
    logger.info("Creating Chart 1: Top Categories (Matplotlib bar)")

    cat_counts = df["category"].value_counts().head(12)
    colours    = plt.cm.Blues(np.linspace(0.4, 0.9, len(cat_counts)))

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(cat_counts.index[::-1],
                   cat_counts.values[::-1],
                   color=colours[::-1],
                   edgecolor="white",
                   linewidth=0.6)

    # Annotate each bar with its count
    for bar, val in zip(bars, cat_counts.values[::-1]):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                f"  {val}", va="center", fontsize=10, color="#333333")

    ax.set_title("Top 12 Job Categories — Nepal Job Market",
                 fontsize=14, fontweight="bold", pad=14)
    ax.set_xlabel("Number of Job Listings")
    ax.set_ylabel("")
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    path = f"{CHARTS_DIR}/mpl_01_top_categories.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 2 — Seaborn histplot + KDE: Salary Distribution
# ══════════════════════════════════════════════════════════════

def plot_salary_distribution(df: pd.DataFrame):
    """
    Seaborn histplot with a KDE overlay for minimum salary.
    Vertical lines mark the mean and median for easy comparison.
    """
    logger.info("Creating Chart 2: Salary Distribution (Seaborn histplot + KDE)")

    sal = df[(df["salary_min"].notna()) &
             (df["salary_min"] > 0) &
             (df["salary_min"] < 300_000)]["salary_min"]

    if len(sal) < 10:
        logger.warning("  Not enough salary data — skipping chart 2")
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.histplot(sal, bins=30, kde=True, color=ACCENT, ax=ax,
                 edgecolor="white", linewidth=0.5, alpha=0.75)

    mean_val   = sal.mean()
    median_val = sal.median()
    ax.axvline(mean_val,   color=DANGER,  linestyle="--", linewidth=1.6,
               label=f"Mean   NPR {mean_val:,.0f}")
    ax.axvline(median_val, color=SUCCESS, linestyle="--", linewidth=1.6,
               label=f"Median NPR {median_val:,.0f}")

    ax.set_title("Monthly Salary Distribution (Minimum, NPR)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Minimum Salary (NPR)")
    ax.set_ylabel("Number of Jobs")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"NPR {x/1000:.0f}K"))
    ax.legend(framealpha=0.85)

    path = f"{CHARTS_DIR}/sns_02_salary_distribution.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 3 — Seaborn Box Plot: Salary by Job Level
# ══════════════════════════════════════════════════════════════

def plot_salary_by_level(df: pd.DataFrame):
    """
    Seaborn box plot comparing salary distributions across
    the five standardized job levels. Swarm points overlay
    shows the actual data density.
    """
    logger.info("Creating Chart 3: Salary by Job Level (Seaborn boxplot)")

    order  = ["Entry Level", "Mid Level", "Senior Level",
              "Management", "Not Specified"]
    sal_df = df[(df["salary_min"].notna()) &
                (df["salary_min"] > 0) &
                (df["salary_min"] < 300_000)].copy()
    sal_df = sal_df[sal_df["job_level"].isin(order)]

    if len(sal_df) < 10:
        logger.warning("  Not enough data — skipping chart 3")
        return

    palette = {
        "Entry Level":   "#86efac",
        "Mid Level":     "#60a5fa",
        "Senior Level":  "#c084fc",
        "Management":    "#f87171",
        "Not Specified": "#cbd5e1",
    }

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.boxplot(data=sal_df, x="job_level", y="salary_min",
                order=[o for o in order if o in sal_df["job_level"].unique()],
                palette=palette, ax=ax, linewidth=1.2, fliersize=3)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda y, _: f"NPR {y/1000:.0f}K"))
    ax.set_title("Salary Distribution by Experience Level",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Job Level")
    ax.set_ylabel("Minimum Monthly Salary (NPR)")
    ax.tick_params(axis="x", rotation=15)

    path = f"{CHARTS_DIR}/sns_03_salary_by_level.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 4 — Seaborn Heatmap: Correlation Matrix
# ══════════════════════════════════════════════════════════════

def plot_correlation_heatmap(df: pd.DataFrame):
    """
    Seaborn heatmap of correlations between numeric features.
    Job level and source are one-hot encoded so they appear
    in the correlation matrix alongside salary variables.
    """
    logger.info("Creating Chart 4: Correlation Heatmap (Seaborn heatmap)")

    numeric = df[["salary_min", "salary_max"]].copy()

    # One-hot encode job_level and source (Chapter 3 — feature encoding)
    level_dummies  = pd.get_dummies(df["job_level"],  prefix="lvl",  dtype=int)
    source_dummies = pd.get_dummies(df["source"],     prefix="src",  dtype=int)
    combined       = pd.concat([numeric, level_dummies, source_dummies], axis=1)

    corr = combined.corr()

    fig, ax = plt.subplots(figsize=(11, 8))
    mask    = np.triu(np.ones_like(corr, dtype=bool), k=1)   # Hide upper triangle

    sns.heatmap(
        corr,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        center=0,
        vmin=-1, vmax=1,
        linewidths=0.5,
        linecolor="white",
        annot_kws={"size": 8},
        ax=ax,
        square=True,
    )
    ax.set_title("Correlation Matrix — Job Market Features",
                 fontsize=14, fontweight="bold", pad=14)
    plt.xticks(rotation=40, ha="right", fontsize=9)
    plt.yticks(rotation=0,              fontsize=9)

    path = f"{CHARTS_DIR}/sns_04_correlation_heatmap.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 5 — Matplotlib Scatter: Salary Min vs Max
# ══════════════════════════════════════════════════════════════

def plot_salary_scatter(df: pd.DataFrame):
    """
    Matplotlib scatter plot of salary_min vs salary_max,
    colour-coded by job level. A diagonal line shows where
    min == max (single fixed salary).
    """
    logger.info("Creating Chart 5: Salary Min vs Max Scatter (Matplotlib scatter)")

    sal_df = df[(df["salary_min"].notna()) &
                (df["salary_max"].notna()) &
                (df["salary_min"] > 0) &
                (df["salary_max"] > 0) &
                (df["salary_min"] < 300_000) &
                (df["salary_max"] < 500_000)].copy()

    if len(sal_df) < 5:
        logger.warning("  Not enough paired salary data — skipping chart 5")
        return

    level_colours = {
        "Entry Level":   "#22c55e",
        "Mid Level":     "#3b82f6",
        "Senior Level":  "#a855f7",
        "Management":    "#ef4444",
        "Not Specified": "#94a3b8",
    }

    fig, ax = plt.subplots(figsize=(9, 7))

    for level, grp in sal_df.groupby("job_level"):
        ax.scatter(grp["salary_min"], grp["salary_max"],
                   label=level,
                   s=35, alpha=0.70,
                   color=level_colours.get(level, "#888888"),
                   edgecolors="white", linewidths=0.4)

    # Diagonal: min == max
    lim = max(sal_df["salary_max"].max(), sal_df["salary_min"].max()) * 1.05
    ax.plot([0, lim], [0, lim], color="#6b7280", linestyle="--",
            linewidth=1.2, label="Min = Max")

    ax.set_title("Salary Range: Minimum vs Maximum Offered (NPR)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Minimum Salary (NPR)")
    ax.set_ylabel("Maximum Salary (NPR)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}K"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda y, _: f"{y/1000:.0f}K"))
    ax.legend(title="Job Level", fontsize=9)

    path = f"{CHARTS_DIR}/mpl_05_salary_scatter.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 6 — Seaborn Violin Plot: Salary by Top Categories
# ══════════════════════════════════════════════════════════════

def plot_violin_salary_by_category(df: pd.DataFrame):
    """
    Seaborn violin plot showing the salary distribution shape
    for the 8 categories with the most salary data.
    """
    logger.info("Creating Chart 6: Violin Plot — Salary by Category (Seaborn)")

    sal_df = df[(df["salary_min"].notna()) &
                (df["salary_min"] > 0) &
                (df["salary_min"] < 300_000)].copy()

    top_cats = (sal_df.groupby("category")["salary_min"]
                .count()
                .sort_values(ascending=False)
                .head(8)
                .index.tolist())
    sal_df = sal_df[sal_df["category"].isin(top_cats)]

    if len(sal_df) < 10:
        logger.warning("  Not enough data — skipping chart 6")
        return

    fig, ax = plt.subplots(figsize=(13, 6))
    sns.violinplot(data=sal_df, x="category", y="salary_min",
                   order=top_cats,
                   palette="pastel",
                   inner="box",        # Show IQR inside violin
                   linewidth=1.0,
                   ax=ax)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda y, _: f"NPR {y/1000:.0f}K"))
    ax.set_title("Salary Distribution by Job Category (Top 8)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Job Category")
    ax.set_ylabel("Minimum Salary (NPR)")
    ax.tick_params(axis="x", rotation=35)

    path = f"{CHARTS_DIR}/sns_06_violin_salary_category.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 7 — Matplotlib Subplots: Dashboard Overview Grid
#  Demonstrates plt.subplots() with multiple axes in one figure
# ══════════════════════════════════════════════════════════════

def plot_dashboard_overview(df: pd.DataFrame):
    """
    Multi-panel Matplotlib figure combining 4 mini-charts into
    a single summary dashboard image.

    Panels:
      [0,0] Bar   — Top 8 categories
      [0,1] Bar   — Jobs by job level
      [1,0] Bar   — Top 8 locations
      [1,1] Hist  — Salary distribution
    """
    logger.info("Creating Chart 7: Dashboard Overview (Matplotlib subplots)")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Nepal Job Market — Overview Dashboard",
                 fontsize=16, fontweight="bold", y=1.01)
    plt.subplots_adjust(hspace=0.45, wspace=0.35)

    # ── Panel [0,0]: Top categories ────────────────────────
    ax = axes[0, 0]
    cat_data = df["category"].value_counts().head(8)
    colours  = sns.color_palette("Blues_d", n_colors=len(cat_data))
    ax.barh(cat_data.index[::-1], cat_data.values[::-1], color=colours)
    ax.set_title("Top 8 Categories")
    ax.set_xlabel("Listings")
    ax.tick_params(axis="y", labelsize=8)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=5))

    # ── Panel [0,1]: Job level ──────────────────────────────
    ax = axes[0, 1]
    level_data = df["job_level"].value_counts()
    level_pal  = ["#22c55e", "#3b82f6", "#a855f7", "#ef4444", "#94a3b8"]
    ax.bar(level_data.index, level_data.values,
           color=level_pal[:len(level_data)], edgecolor="white")
    ax.set_title("Jobs by Experience Level")
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=20, labelsize=8)
    for i, v in enumerate(level_data.values):
        ax.text(i, v + 1, str(v), ha="center", fontsize=8)

    # ── Panel [1,0]: Top locations ──────────────────────────
    ax = axes[1, 0]
    loc_data = (df[df["location"] != "Unknown"]["location"]
                .value_counts().head(8))
    ax.barh(loc_data.index[::-1], loc_data.values[::-1],
            color=sns.color_palette("Greens_d", n_colors=len(loc_data)))
    ax.set_title("Top 8 Locations")
    ax.set_xlabel("Listings")
    ax.tick_params(axis="y", labelsize=8)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=5))

    # ── Panel [1,1]: Salary histogram ──────────────────────
    ax = axes[1, 1]
    sal = df[(df["salary_min"].notna()) &
             (df["salary_min"] > 0) &
             (df["salary_min"] < 200_000)]["salary_min"]
    if len(sal) > 5:
        ax.hist(sal, bins=25, color=ACCENT, edgecolor="white",
                linewidth=0.4, alpha=0.80)
        ax.axvline(sal.median(), color=DANGER, linestyle="--",
                   linewidth=1.4, label=f"Median {sal.median()/1000:.0f}K")
        ax.legend(fontsize=8)
    ax.set_title("Salary Distribution (NPR)")
    ax.set_xlabel("Min Salary (NPR)")
    ax.set_ylabel("Jobs")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}K"))

    path = f"{CHARTS_DIR}/mpl_07_dashboard_overview.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  CHART 8 — Seaborn Count Plot: Source × Job Level Comparison
# ══════════════════════════════════════════════════════════════

def plot_source_vs_level(df: pd.DataFrame):
    """
    Seaborn grouped bar chart comparing the job level distribution
    between MeroJob and KumariJob sources.
    """
    logger.info("Creating Chart 8: Source vs Job Level (Seaborn countplot)")

    order = ["Entry Level", "Mid Level", "Senior Level",
             "Management", "Not Specified"]
    sub   = df[df["job_level"].isin(order)].copy()

    fig, ax = plt.subplots(figsize=(11, 5))
    sns.countplot(data=sub, x="job_level", hue="source",
                  order=[o for o in order if o in sub["job_level"].unique()],
                  palette={"merojob": ACCENT, "kumarijob": DANGER},
                  ax=ax, edgecolor="white", linewidth=0.6)

    ax.set_title("Job Level Distribution by Source Website",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Job Level")
    ax.set_ylabel("Number of Listings")
    ax.tick_params(axis="x", rotation=15)
    ax.legend(title="Source", labels=["MeroJob", "KumariJob"])

    path = f"{CHARTS_DIR}/sns_08_source_vs_level.png"
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"  Saved: {path}")


# ══════════════════════════════════════════════════════════════
#  MAIN — run all charts
# ══════════════════════════════════════════════════════════════

def generate_all_charts():
    """Loads data, runs all 8 chart functions, and logs a summary."""
    logger.info("=== visualizations.py — Starting static chart generation ===")
    df = load_data()

    plot_top_categories(df)
    plot_salary_distribution(df)
    plot_salary_by_level(df)
    plot_correlation_heatmap(df)
    plot_salary_scatter(df)
    plot_violin_salary_by_category(df)
    plot_dashboard_overview(df)
    plot_source_vs_level(df)

    logger.info(f"=== All charts saved to /{CHARTS_DIR}/ ===")


if __name__ == "__main__":
    generate_all_charts()
    print(f"\nAll charts saved to: {os.path.abspath(CHARTS_DIR)}/")
    print("Open any .png file to view it.")
