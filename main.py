# main.py — Orchestrator
import time
import asyncio
import multiprocessing as mp
from pathlib import Path
import pandas as pd
import json

from generate_receipts import generate_all, generate_receipt
from process_receipts import process_chunk, run_parallel, aggregate_results
from serialize_and_ship import save_results, ship_to_proxmox

OUTPUT_DIR = Path("./receipts_data")
RESULTS_DIR = Path("./pipeline_output")
N_RECEIPTS = 100_000

# ── PROXMOX CONFIG (edit these) ────────────────────────────────────────────────
PROXMOX_USER = "root"
PROXMOX_HOST = "192.168.1.100"  # Your LXC container IP
PROXMOX_PATH = "/opt/dashboard-data/"


def run_sync_generation(n: int, base_path: Path) -> float:
    """Baseline: generate receipts synchronously, one at a time."""
    t = time.perf_counter()
    base_path.mkdir(exist_ok=True)
    # NOTE: Deliberately slow — single-threaded, sequential writes.
    # This is the baseline you are trying to beat.
    for i in range(n):
        receipt = generate_receipt(i)
        shard = i // 100
        dir_path = base_path / f"sync_{shard:05d}"
        dir_path.mkdir(parents=True, exist_ok=True)
        fp = dir_path / f"{receipt['receipt_id']}.json"
        with open(fp, 'w') as f:
            json.dump(receipt, f)
    return time.perf_counter() - t


def run_sync_processing(all_paths: list) -> tuple[pd.DataFrame, float]:
    """Baseline: process all receipts on a single core, no parallelism."""
    t = time.perf_counter()
    df = process_chunk(all_paths)  # All 100k paths in one sequential call
    elapsed = time.perf_counter() - t
    return df, elapsed


if __name__ == "__main__":
    import numpy as np

    print("=" * 60)
    print("RECEIPT PARSER — CONCURRENCY BENCHMARK")
    print("=" * 60)

    # ── PHASE 1A: SYNC GENERATION (baseline) ──────────────────────────────────
    # Run on a smaller N (10k) to keep the demo reasonable — sync on 100k takes hours
    SYNC_N = 10_000
    print(f"\n[1/5] Sync generation ({SYNC_N} receipts)...")
    sync_gen_time = run_sync_generation(SYNC_N, OUTPUT_DIR / "sync")
    print(f"  ✓ SYNC generation:       {sync_gen_time:.3f}s")

    # ── PHASE 1B: ASYNC GENERATION (concurrent) ───────────────────────────────
    print(f"\n[2/5] Async generation ({N_RECEIPTS} receipts)...")
    t = time.perf_counter()
    asyncio.run(generate_all(N_RECEIPTS, OUTPUT_DIR / "async"))
    async_gen_time = time.perf_counter() - t
    print(f"  ✓ CONCURRENT generation: {async_gen_time:.3f}s")

    # ── COLLECT ALL FILE PATHS ─────────────────────────────────────────────────
    all_paths = sorted((OUTPUT_DIR / "async").rglob("*.json"))
    print(f"\n  Found {len(all_paths)} receipt files to process.")

    # ── PHASE 2A: SYNC PROCESSING (baseline) ──────────────────────────────────
    # Run on subset for time (full 100k sync takes many minutes)
    SYNC_PROC_N = 5_000
    sync_sample = all_paths[:SYNC_PROC_N]
    print(f"\n[3/5] Sync processing ({SYNC_PROC_N} receipts)...")
    _, sync_proc_time = run_sync_processing(list(sync_sample))
    print(f"  ✓ SYNC processing:       {sync_proc_time:.3f}s")

    # ── PHASE 2B: PARALLEL PROCESSING ─────────────────────────────────────────
    print(f"\n[4/5] Parallel processing ({N_RECEIPTS} receipts)...")
    t = time.perf_counter()
    final_df = run_parallel(all_paths)
    parallel_proc_time = time.perf_counter() - t
    print(f"  ✓ PARALLEL processing:   {parallel_proc_time:.3f}s")

    # ── AGGREGATE + PRINT RESULTS TABLE ───────────────────────────────────────
    summary = aggregate_results(final_df)

    # Extrapolate sync time to full 100k for fair comparison display
    sync_proc_extrapolated = (sync_proc_time / SYNC_PROC_N) * N_RECEIPTS

    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    print(f"{'Phase':<30} {'Sync':>10} {'Concurrent/Parallel':>20} {'Speedup':>10}")
    print("-" * 60)
    print(
        f"{'Generation (10k/100k files)':<30} {sync_gen_time:>9.2f}s {async_gen_time:>19.2f}s {sync_gen_time / async_gen_time:>9.1f}x")
    print(
        f"{'Processing (5k→100k extrap.)':<30} {sync_proc_extrapolated:>9.2f}s {parallel_proc_time:>19.2f}s {sync_proc_extrapolated / parallel_proc_time:>9.1f}x")
    print("-" * 60)
    print(f"Total Revenue (MYR): {summary['total_revenue_myr']:,.2f}")
    print(f"Receipts processed:  {len(final_df):,}")

    # Save timing metadata as Parquet too (for the dashboard benchmark chart)
    timings_df = pd.DataFrame([
        {"phase": "Generation", "method": "Sync", "seconds": sync_gen_time, "n": SYNC_N},
        {"phase": "Generation", "method": "Concurrent", "seconds": async_gen_time, "n": N_RECEIPTS},
        {"phase": "Processing", "method": "Sync", "seconds": sync_proc_extrapolated, "n": N_RECEIPTS},
        {"phase": "Processing", "method": "Parallel", "seconds": parallel_proc_time, "n": N_RECEIPTS},
    ])

    # ── PHASE 4: SAVE + SHIP ───────────────────────────────────────────────────
    print(f"\n[5/5] Serializing and shipping to Proxmox...")
    summary["timings"] = timings_df
    save_results(summary, RESULTS_DIR)
    ship_to_proxmox(RESULTS_DIR, PROXMOX_USER, PROXMOX_HOST, PROXMOX_PATH)
    print("\nPipeline complete.")