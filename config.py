# config.py
"""
Central configuration for the E-Commerce Receipt Parser pipeline.
Edit ONLY this file to change scale or output paths.

SCALING GUIDE:
  Quick test    : BENCH_N=1_000,  N_RECEIPTS=5_000
  Assignment    : BENCH_N=10_000, N_RECEIPTS=100_000, SYNC_PROC_N=10_000 ← default
  Stress test   : BENCH_N=10_000, N_RECEIPTS=500_000, SYNC_PROC_N=10_000 (needs ~4GB free disk)
  Shahadan test : BENCH_N=10_000, N_RECEIPTS=1_000_000, SYNC_PROC_N=10_000
"""

# ── SCALE ──────────────────────────────────────────────────────────────────────
N_RECEIPTS         = 100_000   # Total receipts for the full async + parallel run
BENCH_N            = 10_000    # Receipts used for sync/threaded baseline
                               # (kept small so sync doesn't take 30+ min)
SYNC_PROC_N        = 5_000     # Receipts used for sync/threaded processing baseline
                               # (extrapolated to N_RECEIPTS in the results table)
SEMAPHORE          = 500       # Max concurrent open file handles for asyncio
                               # Reduce to 200 on machines with low ulimit -n
MAX_THREAD_WORKERS = 64        # ThreadPoolExecutor ceiling for I/O writes

# ── OUTPUT PATHS ───────────────────────────────────────────────────────────────
OUTPUT_DIR  = "receipts_data"    # Where generated JSON receipt files are stored
RESULTS_DIR = "pipeline_output"  # Where Parquet + PNG chart files are saved

# ── CHART OUTPUT ───────────────────────────────────────────────────────────────
# No Streamlit or browser required to view results.
SAVE_CHARTS = True             # Set False to skip chart generation
CHART_DPI   = 150              # PNG resolution (150 = good screen quality)gen