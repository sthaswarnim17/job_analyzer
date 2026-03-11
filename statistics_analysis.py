"""
statistics_analysis.py — Applied Statistics and EDA
=====================================================
Covers Chapter 6 of the syllabus:
  6.1 Statistical measures: correlation, covariance, skewness, kurtosis
  6.2 Probability review, sampling, and hypothesis testing
  6.3 Regression and trend analysis using statsmodels
  6.4 EDA using descriptive and inferential methods

Also covers Chapter 1 (OOP for data science — Chapter 1.4):
  - Class-based design: JobMarketAnalyzer encapsulates all analysis
  - Encapsulation: data and methods together in one class
  - Clean public API: one call to run_full_analysis()

Run: python statistics_analysis.py
"""

import sqlite3
import warnings

import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as mticker
import os

warnings.filterwarnings("ignore")

from logger import get_logger

logger = get_logger("job_analyzer.statistics")

DB_PATH    = "jobs.db"
CHARTS_DIR = "charts"
os.makedirs(CHARTS_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  CLASS: JobMarketAnalyzer
#  Demonstrates OOP for data science (Chapter 1.4):
#    - __init__   : encapsulates DB path and loaded data
#    - Properties : expose derived data cleanly
#    - Methods    : each statistical task is its own method
#    - Encapsulation: private helpers prefixed with _
# ══════════════════════════════════════════════════════════════

class JobMarketAnalyzer:
    """
    Object-oriented statistical analysis engine for Nepal job market data.

    Attributes:
        db_path (str): Path to the SQLite database file.
        df      (pd.DataFrame): Full cleaned jobs dataset.
        sal_df  (pd.DataFrame): Subset with valid salary data only.

    Example:
        analyzer = JobMarketAnalyzer("jobs.db")
        analyzer.run_full_analysis()
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.df      = self._load_data()
        self.sal_df  = self._filter_salary_data()
        logger.info(
            f"JobMarketAnalyzer initialized — "
            f"{len(self.df)} total jobs, "
            f"{len(self.sal_df)} with salary data"
        )

    # ── Private helpers ────────────────────────────────────────

    def _load_data(self) -> pd.DataFrame:
        """Loads jobs_clean table from SQLite and coerces types."""
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql("SELECT * FROM jobs_clean", conn)
        except Exception as exc:
            logger.error(f"Failed to load data: {exc}")
            raise
        finally:
            conn.close()

        df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
        df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
        return df

    def _filter_salary_data(self) -> pd.DataFrame:
        """Returns rows with realistic salary_min values."""
        return self.df[
            (self.df["salary_min"].notna()) &
            (self.df["salary_min"] > 0) &
            (self.df["salary_min"] < 400_000)
        ].copy()

    @staticmethod
    def _separator(title: str):
        """Prints a formatted section header."""
        print(f"\n{'═' * 60}")
        print(f"  {title}")
        print('═' * 60)

    # ── 1. Descriptive Statistics ──────────────────────────────

    def descriptive_statistics(self) -> pd.DataFrame:
        """
        Computes an extended set of descriptive statistics for
        salary_min including skewness and kurtosis.

        Returns:
            pd.DataFrame: Summary table with all statistics.
        """
        self._separator("1. DESCRIPTIVE STATISTICS (Salary — NPR)")

        sal = self.sal_df["salary_min"]

        stats_dict = {
            "Count"            : len(sal),
            "Mean"             : sal.mean(),
            "Median"           : sal.median(),
            "Std Dev"          : sal.std(),
            "Variance"         : sal.var(),
            "Min"              : sal.min(),
            "25th Percentile"  : sal.quantile(0.25),
            "75th Percentile"  : sal.quantile(0.75),
            "Max"              : sal.max(),
            "IQR"              : sal.quantile(0.75) - sal.quantile(0.25),
            # Skewness > 0  means right-skewed (long right tail)
            # Skewness < 0  means left-skewed
            "Skewness"         : sal.skew(),
            # Kurtosis > 0  means heavier tails than normal (leptokurtic)
            # Kurtosis < 0  means lighter tails (platykurtic)
            "Kurtosis"         : sal.kurtosis(),
            "Coeff of Variation": sal.std() / sal.mean() * 100,
        }

        for label, value in stats_dict.items():
            if isinstance(value, (int, np.integer)):
                print(f"  {label:<26}: {value:>10,}")
            else:
                print(f"  {label:<26}: {value:>13,.2f}")

        # Skewness interpretation
        skew = sal.skew()
        if skew > 1:
            interp = "strongly right-skewed (most jobs pay less, few pay a lot)"
        elif skew > 0.5:
            interp = "moderately right-skewed"
        elif skew < -1:
            interp = "strongly left-skewed"
        else:
            interp = "approximately symmetric"
        print(f"\n  Interpretation: Salary distribution is {interp}.")
        print(f"  Kurtosis {sal.kurtosis():.2f} — "
              + ("heavy-tailed (outliers present)" if sal.kurtosis() > 0
                 else "light-tailed (few outliers)"))

        summary_df = pd.DataFrame(list(stats_dict.items()),
                                  columns=["Statistic", "Value"])
        return summary_df

    # ── 2. Correlation Analysis ────────────────────────────────

    def correlation_analysis(self) -> pd.DataFrame:
        """
        Computes Pearson correlation and covariance between salary_min
        and salary_max. Reports correlation strength interpretation.

        Returns:
            pd.DataFrame: Correlation matrix.
        """
        self._separator("2. CORRELATION AND COVARIANCE ANALYSIS")

        paired = self.sal_df[
            self.sal_df["salary_max"].notna() & (self.sal_df["salary_max"] > 0)
        ][["salary_min", "salary_max"]].copy()

        if len(paired) < 5:
            print("  Not enough paired salary data for correlation analysis.")
            return pd.DataFrame()

        corr_matrix = paired.corr(method="pearson")
        cov_matrix  = paired.cov()
        r           = corr_matrix.loc["salary_min", "salary_max"]

        print(f"  Sample size (jobs with both min & max salary): {len(paired)}")
        print(f"\n  Pearson Correlation (salary_min vs salary_max): r = {r:.4f}")
        print(f"  Covariance:                                          {cov_matrix.loc['salary_min','salary_max']:,.2f}")

        # Interpret r
        if abs(r) >= 0.9:
            strength = "very strong"
        elif abs(r) >= 0.7:
            strength = "strong"
        elif abs(r) >= 0.4:
            strength = "moderate"
        else:
            strength = "weak"
        direction = "positive" if r > 0 else "negative"
        print(f"\n  Interpretation: {strength.title()} {direction} correlation.")
        print(f"  This means companies that offer a higher minimum also tend")
        print(f"  to offer a proportionally higher maximum salary.")

        print("\n  Full correlation matrix:")
        print(corr_matrix.to_string())
        return corr_matrix

    # ── 3. Hypothesis Testing: One-Way ANOVA ──────────────────

    def anova_salary_by_level(self) -> dict:
        """
        One-Way ANOVA test: Are mean salaries significantly different
        across job levels?

        H0: All job level groups have the same mean salary.
        H1: At least one group has a significantly different mean.

        Uses scipy.stats.f_oneway.

        Returns:
            dict: {'f_stat': float, 'p_value': float, 'reject_h0': bool}
        """
        self._separator("3. HYPOTHESIS TEST — One-Way ANOVA")
        print("  Question: Do different job levels have significantly")
        print("  different salary distributions?")
        print()
        print("  H₀ (Null Hypothesis):      All job level means are equal")
        print("  H₁ (Alt.  Hypothesis):     At least one mean is different")
        print("  Significance level (α):    0.05")

        groups = []
        labels = []
        for level in ["Entry Level", "Mid Level", "Senior Level", "Management"]:
            grp = self.sal_df[self.sal_df["job_level"] == level]["salary_min"]
            if len(grp) >= 3:
                groups.append(grp.values)
                labels.append(level)

        if len(groups) < 2:
            print("\n  Not enough groups with sufficient data for ANOVA.")
            return {}

        f_stat, p_value = stats.f_oneway(*groups)
        reject = p_value < 0.05

        print(f"\n  Groups tested: {labels}")
        print(f"  Group sizes  : {[len(g) for g in groups]}")
        print(f"\n  F-statistic  : {f_stat:.4f}")
        print(f"  p-value      : {p_value:.6f}")
        print()
        if reject:
            print("  RESULT: Reject H₀ — there IS a statistically significant")
            print("  difference in salaries across job levels (p < 0.05).")
            print("  Senior/Management roles likely pay significantly more.")
        else:
            print("  RESULT: Fail to reject H₀ — no statistically significant")
            print("  difference detected (p >= 0.05). Salary differences may")
            print("  be due to random variation in this dataset.")

        # Group means for context
        print("\n  Group means (NPR):")
        for label, grp in zip(labels, groups):
            print(f"    {label:<18}: NPR {np.mean(grp):>10,.0f}  (n={len(grp)})")

        return {"f_stat": f_stat, "p_value": p_value, "reject_h0": reject}

    # ── 4. Two-Sample T-Test: MeroJob vs KumariJob Salaries ───

    def ttest_salary_by_source(self) -> dict:
        """
        Independent two-sample t-test: Do MeroJob and KumariJob
        listings offer significantly different salaries?

        Returns:
            dict: {'t_stat', 'p_value', 'reject_h0'}
        """
        self._separator("4. HYPOTHESIS TEST — Two-Sample T-Test")
        print("  Question: Is there a significant difference in salaries")
        print("  between MeroJob and KumariJob listings?")
        print()
        print("  H₀: Mean(MeroJob salary) == Mean(KumariJob salary)")
        print("  H₁: Mean(MeroJob salary) != Mean(KumariJob salary)")
        print("  α  = 0.05 (two-tailed)")

        mero   = self.sal_df[self.sal_df["source"] == "merojob"]["salary_min"]
        kumari = self.sal_df[self.sal_df["source"] == "kumarijob"]["salary_min"]

        if len(mero) < 5 or len(kumari) < 5:
            print("\n  Not enough data from one or both sources.")
            return {}

        t_stat, p_value = stats.ttest_ind(mero, kumari, equal_var=False)
        reject = p_value < 0.05

        print(f"\n  MeroJob   — n={len(mero)}, mean=NPR {mero.mean():,.0f}")
        print(f"  KumariJob — n={len(kumari)}, mean=NPR {kumari.mean():,.0f}")
        print(f"\n  t-statistic : {t_stat:.4f}")
        print(f"  p-value     : {p_value:.6f}")
        print()
        if reject:
            higher = "MeroJob" if mero.mean() > kumari.mean() else "KumariJob"
            print(f"  RESULT: Reject H₀ — {higher} listings offer significantly")
            print("  different (higher) salaries on average (p < 0.05).")
        else:
            print("  RESULT: Fail to reject H₀ — no significant salary difference")
            print("  between the two portals was found (p >= 0.05).")

        return {"t_stat": t_stat, "p_value": p_value, "reject_h0": reject}

    # ── 5. Linear Regression: Salary Min → Salary Max ─────────

    def linear_regression(self) -> dict:
        """
        Simple OLS linear regression using scipy.stats.linregress:
          salary_max = β0 + β1 * salary_min + ε

        Shows how well the minimum salary predicts the maximum.

        Returns:
            dict: Regression parameters and R² score.
        """
        self._separator("5. LINEAR REGRESSION — Salary Min predicts Salary Max")
        print("  Model: salary_max = intercept + slope × salary_min")

        paired = self.sal_df[
            self.sal_df["salary_max"].notna() &
            (self.sal_df["salary_max"] > 0) &
            (self.sal_df["salary_max"] < 500_000)
        ][["salary_min", "salary_max"]].dropna()

        if len(paired) < 10:
            print("\n  Not enough paired salary data for regression.")
            return {}

        x = paired["salary_min"].values
        y = paired["salary_max"].values

        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        r_squared = r_value ** 2

        print(f"\n  Sample size  : {len(paired)}")
        print(f"  Intercept    : NPR {intercept:+,.0f}")
        print(f"  Slope        : {slope:.4f}  "
              "(for every NPR 1 increase in min salary, max increases by this)")
        print(f"  R² score     : {r_squared:.4f}  "
              f"({r_squared*100:.1f}% of variance in max explained by min)")
        print(f"  Correlation r: {r_value:.4f}")
        print(f"  p-value      : {p_value:.2e}  "
              + ("(significant)" if p_value < 0.05 else "(not significant)"))
        print(f"  Std Error    : {std_err:.4f}")

        # Scatter plot with regression line
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(x, y, alpha=0.45, s=25, color="#3b82f6",
                   edgecolors="white", linewidths=0.3,
                   label="Job listings")
        x_line = np.linspace(x.min(), x.max(), 200)
        ax.plot(x_line, intercept + slope * x_line,
                color="#dc2626", linewidth=2.0,
                label=f"OLS fit  R²={r_squared:.3f}")
        ax.set_title("Linear Regression: Salary Min vs Max (NPR)",
                     fontsize=13, fontweight="bold")
        ax.set_xlabel("Minimum Salary (NPR)")
        ax.set_ylabel("Maximum Salary (NPR)")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(
            lambda v, _: f"{v/1000:.0f}K"))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(
            lambda v, _: f"{v/1000:.0f}K"))
        ax.legend()
        path = f"{CHARTS_DIR}/stat_regression.png"
        fig.savefig(path, dpi=140, bbox_inches="tight")
        plt.close(fig)
        logger.info(f"  Regression chart saved: {path}")

        return {
            "slope": slope, "intercept": intercept,
            "r_squared": r_squared, "p_value": p_value
        }

    # ── 6. Chi-Square Test: Category vs Location ──────────────

    def chi_square_category_location(self) -> dict:
        """
        Chi-square test of independence: Are certain job categories
        more concentrated in specific cities?

        H0: Job category and location are independent.
        H1: Category and location are associated.

        Returns:
            dict: {'chi2', 'p_value', 'dof', 'reject_h0'}
        """
        self._separator("6. CHI-SQUARE TEST — Category vs Location")
        print("  Question: Is job category associated with city location?")
        print("  H₀: Category and location are independent")
        print("  H₁: Category and location are NOT independent")
        print("  α  = 0.05")

        top_cats = self.df["category"].value_counts().head(6).index
        top_locs = (self.df[self.df["location"] != "Unknown"]["location"]
                    .value_counts().head(5).index)

        sub = self.df[
            self.df["category"].isin(top_cats) &
            self.df["location"].isin(top_locs)
        ]

        if len(sub) < 20:
            print("\n  Not enough data for chi-square test.")
            return {}

        contingency = pd.crosstab(sub["category"], sub["location"])
        chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
        reject = p_value < 0.05

        print(f"\n  Contingency table (top 6 categories × top 5 cities):")
        print(contingency.to_string())
        print(f"\n  Chi-square statistic : {chi2:.4f}")
        print(f"  Degrees of freedom   : {dof}")
        print(f"  p-value              : {p_value:.6f}")
        print()
        if reject:
            print("  RESULT: Reject H₀ — Category and location ARE significantly")
            print("  associated. Certain job types cluster in specific cities.")
        else:
            print("  RESULT: Fail to reject H₀ — no significant association")
            print("  between job category and city was found (p >= 0.05).")

        return {"chi2": chi2, "p_value": p_value, "dof": dof, "reject_h0": reject}

    # ── 7. Category-Level Summary Table ───────────────────────

    def category_salary_summary(self) -> pd.DataFrame:
        """
        Builds a per-category summary table with count, mean,
        median, and standard deviation of minimum salary.

        Returns:
            pd.DataFrame: Summary sorted by median salary descending.
        """
        self._separator("7. SALARY SUMMARY BY JOB CATEGORY")

        summary = (
            self.sal_df.groupby("category")["salary_min"]
            .agg(Count="count",
                 Mean="mean",
                 Median="median",
                 Std="std",
                 Min="min",
                 Max="max")
            .query("Count >= 3")
            .sort_values("Median", ascending=False)
            .head(15)
        )

        print(summary.applymap(
            lambda x: f"NPR {x:,.0f}" if isinstance(x, (float, int)) else x
        ).to_string())

        return summary

    # ── 8. Run Full Analysis ───────────────────────────────────

    def run_full_analysis(self):
        """
        Runs all statistical analyses in sequence and prints
        a consolidated report to the terminal.
        """
        print("=" * 60)
        print("  NEPAL JOB MARKET — STATISTICAL ANALYSIS REPORT")
        print(f"  Total records: {len(self.df):,}")
        print(f"  Records with salary: {len(self.sal_df):,}")
        print("=" * 60)

        self.descriptive_statistics()
        self.correlation_analysis()
        self.anova_salary_by_level()
        self.ttest_salary_by_source()
        self.linear_regression()
        self.chi_square_category_location()
        self.category_salary_summary()

        print("\n" + "=" * 60)
        print("  ANALYSIS COMPLETE")
        print("=" * 60)
        logger.info("Full statistical analysis completed.")


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    analyzer = JobMarketAnalyzer(DB_PATH)
    analyzer.run_full_analysis()
