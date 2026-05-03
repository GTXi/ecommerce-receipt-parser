# serialize_and_ship.py
"""
Saves all pipeline results locally as Parquet files and auto-generates
PNG charts so anyone running the project can see results immediately
without needing Streamlit, a browser, or any remote server.

Output structure (all inside pipeline_output/):
  receipts_processed.parquet   ← full 100k row detail table
  tax_by_region.parquet
  revenue_by_month.parquet
  revenue_by_tier.parquet
  timings.parquet              ← benchmark times for all methods
  benchmark_report.txt         ← human-readable text summary
  charts/
    01_benchmark_times.png     ← bar chart: sync vs concurrent vs parallel
    02_tax_by_region.png       ← horizontal bar: tax collected per region
    03_revenue_by_month.png    ← line chart: monthly revenue trend
    04_revenue_by_tier.png     ← donut chart: revenue share by tier
    05_processing_paradigms.png← grouped bar: GIL demonstration
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")           # Non-interactive backend — no display needed
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path


# ── COLOUR PALETTE ────────────────────────────────────────────────────────────
# Consistent colours used across all charts for professional appearance
COLORS = {
    "sync":            "#E24B4A",   # Red   → slowest
    "threading":       "#EF9F27",   # Amber → middle
    "asyncio":         "#1D9E75",   # Teal  → fast (I/O)
    "multiprocessing": "#378ADD",   # Blue  → fastest (CPU)
    "gil":             "#D4537E",   # Pink  → GIL limited
}
REGION_COLORS = ["#378ADD", "#1D9E75", "#EF9F27", "#E24B4A", "#7F77DD", "#D4537E", "#639922"]


# ── PARQUET SAVE ──────────────────────────────────────────────────────────────
def save_results(summary: dict, output_dir: Path):
    """
    Saves all summary DataFrames as Parquet files.
    Parquet chosen over CSV because:
      - 5-8x smaller file size (columnar + Snappy compression)
      - Preserves dtypes (datetime64, float64) — no re-parsing needed
      - 3-10x faster to read back than CSV
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "receipts_processed": summary["detail_df"],
        "tax_by_region":      summary["tax_by_region"],
        "revenue_by_month":   summary["revenue_by_month"],
        "revenue_by_tier":    summary["revenue_by_tier"],
    }
    if "timings" in summary:
        tables["timings"] = summary["timings"]

    for name, df in tables.items():
        df.to_parquet(
            output_dir / f"{name}.parquet",
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

    print(f"  ✓ Parquet files saved → {output_dir.resolve()}")


# ── TEXT REPORT ───────────────────────────────────────────────────────────────
def save_text_report(rows: list, summary: dict, output_dir: Path, n_workers: int):
    """
    Writes a plain-text benchmark_report.txt alongside the Parquet files.
    Readable without any software — useful for GitHub README screenshots.
    """
    lines = [
        "=" * 64,
        "  E-COMMERCE RECEIPT PARSER — BENCHMARK REPORT",
        "=" * 64,
        "",
        f"  Receipts processed : {summary['total_receipts']:,}",
        f"  Total Revenue (MYR): {summary['total_revenue_myr']:,.2f}",
        f"  Total Tax (MYR)    : {summary['total_tax_myr']:,.2f}",
        f"  CPU cores used     : {n_workers}",
        "",
        f"  {'Paradigm':<28} {'Method':<18} {'Time':>8}  {'vs Sync':>10}",
        "  " + "-" * 62,
    ]
    for phase, method, secs, speedup in rows:
        marker = "◀ baseline" if speedup == 1.0 else f"{speedup:.1f}x faster"
        lines.append(f"  {phase:<28} {method:<18} {secs:>7.2f}s  {marker}")
    lines += ["", "=" * 64]

    report_path = output_dir / "benchmark_report.txt"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  ✓ Text report saved  → {report_path.resolve()}")


# ── CHART GENERATION ──────────────────────────────────────────────────────────
def save_charts(summary: dict, output_dir: Path, dpi: int = 150):
    """
    Generates and saves 5 PNG charts automatically after each pipeline run.
    Uses matplotlib only — no browser, no Streamlit required.
    All charts saved to pipeline_output/charts/
    """
    charts_dir = output_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    _chart_benchmark(summary["timings"], charts_dir, dpi)
    _chart_tax_by_region(summary["tax_by_region"], charts_dir, dpi)
    _chart_revenue_by_month(summary["revenue_by_month"], charts_dir, dpi)
    _chart_revenue_by_tier(summary["revenue_by_tier"], charts_dir, dpi)
    _chart_paradigm_comparison(summary["timings"], charts_dir, dpi)

    print(f"  ✓ Charts saved       → {charts_dir.resolve()}")
    print(f"    01_benchmark_times.png")
    print(f"    02_tax_by_region.png")
    print(f"    03_revenue_by_month.png")
    print(f"    04_revenue_by_tier.png")
    print(f"    05_processing_paradigms.png")


def _chart_benchmark(timings: pd.DataFrame, charts_dir: Path, dpi: int):
    """
    Chart 1 — Grouped bar chart: execution time for every method.
    This is the primary academic deliverable — proves performance differences
    between sync, concurrent, and parallel approaches at a glance.
    """
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor("#F8F8F8")
    ax.set_facecolor("#F8F8F8")

    phases  = timings["phase"].unique()
    methods = timings["method"].unique()
    x       = np.arange(len(phases))
    width   = 0.18
    method_colors = {
        "Sync":            COLORS["sync"],
        "Threading":       COLORS["threading"],
        "Asyncio":         COLORS["asyncio"],
        "Multiprocessing": COLORS["multiprocessing"],
        "Threading (GIL)": COLORS["gil"],
    }

    for i, method in enumerate(methods):
        subset = timings[timings["method"] == method]
        vals   = [
            subset[subset["phase"] == p]["seconds"].values[0]
            if p in subset["phase"].values else 0
            for p in phases
        ]
        offset = (i - len(methods) / 2) * width + width / 2
        bars   = ax.bar(x + offset, vals, width,
                        label=method,
                        color=method_colors.get(method, "#888780"),
                        edgecolor="white", linewidth=0.5)
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.3,
                        f"{val:.1f}s", ha="center", va="bottom",
                        fontsize=8, color="#444441")

    ax.set_xticks(x)
    ax.set_xticklabels(phases, fontsize=11)
    ax.set_ylabel("Execution time (seconds)", fontsize=11)
    ax.set_title("Benchmark: Sync vs Concurrent vs Parallel\n"
                 "Lower is better", fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=9, framealpha=0.7)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_ylim(0, timings["seconds"].max() * 1.2)
    ax.yaxis.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(charts_dir / "01_benchmark_times.png", dpi=dpi, bbox_inches="tight")
    plt.close()


def _chart_tax_by_region(tax_df: pd.DataFrame, charts_dir: Path, dpi: int):
    """
    Chart 2 — Horizontal bar: total tax collected per region (MYR).
    Horizontal layout chosen because region name strings are long.
    """
    tax_sorted = tax_df.sort_values("total_tax_myr", ascending=True).copy()
    region_labels = {
        "MY-KUL": "Malaysia — KL",  "MY-PNG": "Malaysia — Penang",
        "SG-01":  "Singapore",      "TH-BKK": "Thailand — Bangkok",
        "ID-JKT": "Indonesia — Jakarta", "PH-MNL": "Philippines — Manila",
        "VN-HCM": "Vietnam — Ho Chi Minh",
    }
    tax_sorted["label"] = tax_sorted["region_code"].map(region_labels).fillna(tax_sorted["region_code"])

    fig, ax = plt.subplots(figsize=(9, 5))
    fig.patch.set_facecolor("#F8F8F8")
    ax.set_facecolor("#F8F8F8")

    bars = ax.barh(tax_sorted["label"], tax_sorted["total_tax_myr"],
                   color=REGION_COLORS[:len(tax_sorted)], edgecolor="white")
    for bar in bars:
        w = bar.get_width()
        ax.text(w + tax_sorted["total_tax_myr"].max() * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"RM {w:,.0f}", va="center", fontsize=9, color="#444441")

    ax.set_xlabel("Total Tax Collected (MYR)", fontsize=11)
    ax.set_title("Total Tax Revenue by Region\n(converted to MYR)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.spines[["top", "right"]].set_visible(False)
    ax.xaxis.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(charts_dir / "02_tax_by_region.png", dpi=dpi, bbox_inches="tight")
    plt.close()


def _chart_revenue_by_month(monthly_df: pd.DataFrame, charts_dir: Path, dpi: int):
    """
    Chart 3 — Line chart: monthly revenue trend over 2 years.
    Line chart is correct for time-series — encodes continuous time flow.
    """
    fig, ax = plt.subplots(figsize=(12, 4))
    fig.patch.set_facecolor("#F8F8F8")
    ax.set_facecolor("#F8F8F8")

    ax.plot(monthly_df["month"], monthly_df["final_total_myr"],
            color=COLORS["asyncio"], linewidth=2, marker="o", markersize=3)
    ax.fill_between(monthly_df["month"], monthly_df["final_total_myr"],
                    alpha=0.15, color=COLORS["asyncio"])

    # Show every 3rd label to prevent crowding
    labels = monthly_df["month"].tolist()
    tick_positions = list(range(0, len(labels), 3))
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([labels[i] for i in tick_positions], rotation=45, ha="right", fontsize=8)

    ax.set_ylabel("Revenue (MYR)", fontsize=11)
    ax.set_title("Monthly Revenue Trend (all regions combined)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"RM {x/1e6:.1f}M"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(charts_dir / "03_revenue_by_month.png", dpi=dpi, bbox_inches="tight")
    plt.close()


def _chart_revenue_by_tier(tier_df: pd.DataFrame, charts_dir: Path, dpi: int):
    """
    Chart 4 — Donut chart: revenue share by customer tier.
    Part-of-whole relationships suit donut/pie charts.
    Shows how tier discount rates affect revenue composition.
    """
    tier_order  = ["bronze", "silver", "gold", "platinum"]
    tier_colors = [COLORS["sync"], COLORS["threading"],
                   COLORS["asyncio"], COLORS["multiprocessing"]]

    df = tier_df.set_index("customer_tier").reindex(tier_order).dropna()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
    fig.patch.set_facecolor("#F8F8F8")

    # Donut chart
    wedges, texts, autotexts = ax1.pie(
        df["sum"], labels=df.index, autopct="%1.1f%%",
        colors=tier_colors, startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "white", "linewidth": 1.5},
        pctdistance=0.75,
    )
    for at in autotexts:
        at.set_fontsize(9)
        at.set_color("white")
        at.set_fontweight("bold")
    ax1.set_title("Revenue Share by Tier", fontsize=12, fontweight="bold")

    # Bar chart: average receipt value per tier
    ax2.set_facecolor("#F8F8F8")
    bars = ax2.bar(df.index, df["mean"], color=tier_colors, edgecolor="white")
    for bar in bars:
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + df["mean"].max() * 0.01,
                 f"RM {bar.get_height():,.0f}",
                 ha="center", va="bottom", fontsize=9)
    ax2.set_ylabel("Avg Receipt Value (MYR)", fontsize=11)
    ax2.set_title("Avg Value per Tier", fontsize=12, fontweight="bold")
    ax2.spines[["top", "right"]].set_visible(False)
    ax2.yaxis.grid(True, alpha=0.3, linestyle="--")
    ax2.set_axisbelow(True)

    plt.suptitle("Customer Tier Analysis", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(charts_dir / "04_revenue_by_tier.png", dpi=dpi, bbox_inches="tight")
    plt.close()


def _chart_paradigm_comparison(timings: pd.DataFrame, charts_dir: Path, dpi: int):
    """
    Chart 5 — Side-by-side bar: focuses specifically on the CPU processing phase.
    Highlights the GIL limitation: threading ≈ sync, multiprocessing >> both.
    This is the most academically important chart — it explains WHY multiprocessing
    exists and what problem the GIL creates for CPU-bound work.
    """
    cpu_rows = timings[timings["phase"] == "CPU processing"].copy()
    if cpu_rows.empty:
        return

    method_order  = ["Sync", "Threading (GIL)", "Multiprocessing"]
    method_colors_list = [COLORS["sync"], COLORS["gil"], COLORS["multiprocessing"]]
    cpu_rows = cpu_rows.set_index("method").reindex(method_order).dropna().reset_index()

    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor("#F8F8F8")
    ax.set_facecolor("#F8F8F8")

    bars = ax.bar(cpu_rows["method"], cpu_rows["seconds"],
                  color=method_colors_list[:len(cpu_rows)],
                  edgecolor="white", linewidth=0.8, width=0.5)

    for bar, row in zip(bars, cpu_rows.itertuples()):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + cpu_rows["seconds"].max() * 0.01,
                f"{row.seconds:.1f}s", ha="center", va="bottom",
                fontsize=11, fontweight="bold", color="#444441")

    # Annotation explaining the GIL
    sync_time = cpu_rows[cpu_rows["method"] == "Sync"]["seconds"].values
    gil_time  = cpu_rows[cpu_rows["method"] == "Threading (GIL)"]["seconds"].values
    if len(sync_time) and len(gil_time):
        ax.annotate("GIL: threads queue\nfor CPU — no speedup",
                    xy=(1, gil_time[0]), xytext=(1.35, gil_time[0] * 0.6),
                    arrowprops={"arrowstyle": "->", "color": COLORS["gil"]},
                    fontsize=9, color=COLORS["gil"])

    ax.set_ylabel("Execution time (seconds)", fontsize=11)
    ax.set_title("CPU Processing: GIL Limitation vs Multiprocessing\n"
                 "Threading ≈ Sync (GIL blocks CPU threads)  |  "
                 "Multiprocessing bypasses GIL",
                 fontsize=11, fontweight="bold", pad=12)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)
    ax.set_ylim(0, cpu_rows["seconds"].max() * 1.25)

    plt.tight_layout()
    plt.savefig(charts_dir / "05_processing_paradigms.png", dpi=dpi, bbox_inches="tight")
    plt.close()