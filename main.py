# main.py
"""
Build for the E-Commerce Receipt Parser & Tax Calculator.
This code will benchmarks 3 Python programming paradigms on 100,000 receipts:
  - Synchronous     : sequential baseline, single OS thread
  - Concurrent      : threading (ThreadPoolExecutor) + asyncio
  - Parallel        : multiprocessing.Pool with pandas vectorisation

To configure scale, paths, and Proxmox settings: edit config.py
To run locally without Proxmox:  set SHIP_TO_PROXMOX = False in config.py
"""

import time
import asyncio
import multiprocessing as mp
import json
import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Local modules
from generate_receipts import generate_receipt, generate_all
from process_receipts   import process_chunk, run_parallel, aggregate_results
from serialize_and_ship import save_results, ship_to_proxmox
import config


# ── HELPERS ────────────────────────────────────────────────────────────────────

def _prepare_receipts(n: int) -> list[dict]:
    """
    Pre-generate all receipt dicts in memory BEFORE benchmarking writes.
    Reason: Faker is CPU-bound — generating fake data takes real CPU time.
    By separating generation from writing, the I/O benchmark reflects only
    disk throughput, not Faker overhead. This gives asyncio/threading a fair
    chance to show their true I/O concurrency benefit.
    """
    print(f"  Pre-generating {n:,} receipt dicts in memory...")
    return [generate_receipt(i) for i in range(n)]


def _write_receipt_from_dict(args: tuple) -> None:
    """Write a pre-generated receipt dict to disk — used by all three methods."""
    receipt, base_path = args
    receipt_id = int(receipt["receipt_id"].split("-")[1])
    shard      = receipt_id // 100
    dir_path   = Path(base_path) / f"{shard:05d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    fp = dir_path / f"{receipt['receipt_id']}.json"
    with open(fp, 'w', encoding='utf-8') as f:
        json.dump(receipt, f)


# ── SYNC WRITE ─────────────────────────────────────────────────────────────────
def run_sync_writes(receipts: list[dict], base_path: Path) -> float:
    """
    Sync baseline: write pre-generated receipts one at a time.
    Single OS thread — each write blocks until the OS confirms it.
    This is the slowest possible approach, used as the comparison floor.
    """
    base_path.mkdir(parents=True, exist_ok=True)
    t = time.perf_counter()
    for receipt in receipts:
        _write_receipt_from_dict((receipt, base_path))
    return time.perf_counter() - t


# ── THREADED WRITE ─────────────────────────────────────────────────────────────
def run_threaded_writes(receipts: list[dict], base_path: Path) -> float:
    """
    Concurrent approach 1: ThreadPoolExecutor.
    The GIL is released during file write syscalls, so multiple threads
    genuinely overlap their disk waits — true I/O concurrency.
    max_workers=64: empirical ceiling before context-switch overhead
    cancels out the I/O parallelism benefit on consumer SSDs.
    """
    base_path.mkdir(parents=True, exist_ok=True)
    args = [(r, base_path) for r in receipts]
    t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=config.MAX_THREAD_WORKERS) as ex:
        ex.map(_write_receipt_from_dict, args)
    return time.perf_counter() - t


# ── ASYNC WRITE ────────────────────────────────────────────────────────────────
async def _write_receipt_async(receipt: dict, base_path: Path, sem: asyncio.Semaphore):
    """Single async coroutine — writes one receipt under a shared semaphore."""
    import aiofiles
    receipt_id = int(receipt["receipt_id"].split("-")[1])
    shard      = receipt_id // 100
    dir_path   = base_path / f"{shard:05d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    fp = dir_path / f"{receipt['receipt_id']}.json"
    async with sem:
        async with aiofiles.open(fp, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(receipt))


async def _async_write_all(receipts: list[dict], base_path: Path):
    """Fire all write coroutines concurrently via asyncio.gather."""
    sem   = asyncio.Semaphore(config.SEMAPHORE)
    tasks = [_write_receipt_async(r, base_path, sem) for r in receipts]
    await asyncio.gather(*tasks)


def run_async_writes(receipts: list[dict], base_path: Path) -> float:
    """
    Concurrent approach 2: asyncio + aiofiles.
    Single-threaded cooperative multitasking — coroutines yield at every
    'await', letting the event loop schedule the next write immediately.
    Zero thread-spawning overhead vs ThreadPoolExecutor.
    """
    base_path.mkdir(parents=True, exist_ok=True)
    t = time.perf_counter()
    asyncio.run(_async_write_all(receipts, base_path))
    return time.perf_counter() - t


# ── SYNC PROCESSING ────────────────────────────────────────────────────────────
def run_sync_processing(paths: list) -> tuple[pd.DataFrame, float]:
    """Single-core processing baseline — no parallelism."""
    t  = time.perf_counter()
    df = process_chunk(paths)
    return df, time.perf_counter() - t


# ── ENTRY POINT ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    OUTPUT_DIR  = Path(config.OUTPUT_DIR)
    RESULTS_DIR = Path(config.RESULTS_DIR)

    print("=" * 64)
    print("  E-COMMERCE RECEIPT PARSER — CONCURRENCY BENCHMARK")
    print("=" * 64)
    print(f"  Scale : {config.BENCH_N:,} receipts (generation benchmark)")
    print(f"         {config.N_RECEIPTS:,} receipts (async + parallel full run)")
    print(f"  Cores : {mp.cpu_count()} logical CPUs detected")
    print("=" * 64)

    # ── PRE-GENERATE IN MEMORY (removes Faker overhead from I/O timings) ───────
    bench_receipts = _prepare_receipts(config.BENCH_N)

    # ── [1/6] SYNC WRITES ─────────────────────────────────────────────────────
    print(f"\n[1/6] Sync writes ({config.BENCH_N:,} receipts)...")
    sync_write_time = run_sync_writes(bench_receipts, OUTPUT_DIR / "sync")
    print(f"  ✓ SYNC:      {sync_write_time:.3f}s")

    # ── [2/6] THREADED WRITES ─────────────────────────────────────────────────
    print(f"\n[2/6] Threaded writes ({config.BENCH_N:,} receipts, "
          f"{config.MAX_THREAD_WORKERS} workers)...")
    thread_write_time = run_threaded_writes(bench_receipts, OUTPUT_DIR / "threaded")
    print(f"  ✓ THREADING: {thread_write_time:.3f}s")

    # ── [3/6] ASYNC WRITES (full 100k) ────────────────────────────────────────
    print(f"\n[3/6] Async writes ({config.N_RECEIPTS:,} receipts, "
          f"semaphore={config.SEMAPHORE})...")
    # For the full async run, generate the extra receipts
    full_receipts = bench_receipts + [
        generate_receipt(i) for i in range(config.BENCH_N, config.N_RECEIPTS)
    ]
    async_write_time = run_async_writes(full_receipts, OUTPUT_DIR / "async")
    # Normalise async time to BENCH_N for a fair per-receipt comparison
    async_write_norm = (async_write_time / config.N_RECEIPTS) * config.BENCH_N
    print(f"  ✓ ASYNCIO:   {async_write_time:.3f}s total  "
          f"({async_write_norm:.3f}s normalised to {config.BENCH_N:,})")

    # ── COLLECT PATHS ─────────────────────────────────────────────────────────
    all_paths = sorted((OUTPUT_DIR / "async").rglob("*.json"))
    print(f"\n  Found {len(all_paths):,} receipt files to process.")

    # ── [4/6] SYNC PROCESSING BASELINE ────────────────────────────────────────
    sync_sample = list(all_paths[:config.SYNC_PROC_N])
    print(f"\n[4/6] Sync processing ({config.SYNC_PROC_N:,} receipts)...")
    _, sync_proc_time = run_sync_processing(sync_sample)
    print(f"  ✓ SYNC processing:      {sync_proc_time:.3f}s")

    # ── [4b/6] THREADED PROCESSING (GIL demonstration) ────────────────────────────
    from process_receipts import run_threaded_processing

    print(f"\n[4b/6] Threaded processing ({config.SYNC_PROC_N:,} receipts, 8 workers)...")
    print(f"  Note: Expected to show minimal speedup — GIL limits CPU-bound threading")
    threaded_sample = list(all_paths[:config.SYNC_PROC_N])
    _, threaded_proc_time = run_threaded_processing(threaded_sample, max_workers=8)
    # Extrapolate to 100k for fair comparison
    threaded_proc_extrap = (threaded_proc_time / config.SYNC_PROC_N) * config.N_RECEIPTS
    print(f"  ✓ THREADED processing:  {threaded_proc_time:.3f}s  "
          f"(extrap: {threaded_proc_extrap:.1f}s)")

    # ── [5/6] PARALLEL PROCESSING ─────────────────────────────────────────────
    n_workers = max(1, mp.cpu_count() - 1)
    print(f"\n[5/6] Parallel processing ({config.N_RECEIPTS:,} receipts, "
          f"{n_workers} workers)...")
    t = time.perf_counter()
    final_df = run_parallel(all_paths)
    parallel_proc_time = time.perf_counter() - t
    print(f"  ✓ PARALLEL processing:  {parallel_proc_time:.3f}s")

    # ── AGGREGATE ─────────────────────────────────────────────────────────────
    summary          = aggregate_results(final_df)
    sync_proc_extrap = (sync_proc_time / config.SYNC_PROC_N) * config.N_RECEIPTS

    # ── BENCHMARK TABLE ───────────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print("  BENCHMARK RESULTS")
    print(f"  (generation rows normalised to {config.BENCH_N:,} receipts for fair comparison)")
    print("=" * 64)
    print(f"  {'Paradigm':<30} {'Method':<16} {'Time':>8}  {'vs Sync':>8}")
    print("  " + "-" * 60)

    rows = [
        ("I/O — file writes", "Sync (baseline)", sync_write_time, 1.0),
        ("I/O — file writes", "Threading", thread_write_time, sync_write_time / thread_write_time),
        ("I/O — file writes", "Asyncio", async_write_norm, sync_write_time / async_write_norm),
        ("CPU — tax math", "Sync (baseline)", sync_proc_extrap, 1.0),
        ("CPU — tax math", "Threading (GIL!)", threaded_proc_extrap, sync_proc_extrap / threaded_proc_extrap),
        ("CPU — tax math", "Multiprocessing", parallel_proc_time, sync_proc_extrap / parallel_proc_time),
    ]
    for phase, method, secs, speedup in rows:
        marker = "◀ baseline" if speedup == 1.0 else f"{speedup:.1f}x faster"
        print(f"  {phase:<30} {method:<16} {secs:>7.2f}s  {marker}")

    print("  " + "-" * 60)
    print(f"  Total Revenue (MYR):  {summary['total_revenue_myr']:>,.2f}")
    print(f"  Receipts processed:   {len(final_df):>,}")
    print(f"  CPU cores (parallel): {n_workers} / {mp.cpu_count()}")
    print("=" * 64)

    # ── SAVE TIMINGS ──────────────────────────────────────────────────────────
    timings_df = pd.DataFrame([
        {"phase": "I/O writes",  "method": "Sync",           "seconds": sync_write_time,    "n": config.BENCH_N},
        {"phase": "I/O writes",  "method": "Threading",      "seconds": thread_write_time,  "n": config.BENCH_N},
        {"phase": "I/O writes",  "method": "Asyncio",        "seconds": async_write_norm,   "n": config.BENCH_N},
        {"phase": "CPU processing","method": "Sync",         "seconds": sync_proc_extrap,   "n": config.N_RECEIPTS},
        {"phase": "CPU processing","method": "Multiprocessing","seconds": parallel_proc_time,"n": config.N_RECEIPTS},
    ])
    summary["timings"] = timings_df

    # ── [6/6] SAVE + OPTIONAL SHIP ────────────────────────────────────────────
    print(f"\n[6/6] Saving results to {RESULTS_DIR}/...")
    save_results(summary, RESULTS_DIR)
    print(f"  ✓ Parquet files saved locally to: {RESULTS_DIR.resolve()}")

    if config.SHIP_TO_PROXMOX:
        print(f"\n  Shipping to Proxmox at {config.PROXMOX_HOST}...")
        ship_to_proxmox(RESULTS_DIR, config.PROXMOX_USER,
                        config.PROXMOX_HOST, config.PROXMOX_PATH)
    else:
        print("\n  [Proxmox skipped — SHIP_TO_PROXMOX = False in config.py]")
        print(f"  To view dashboard locally, run:")
        print(f"    streamlit run dashboard.py --server.port 8501")
        print(f"  Then open:  http://localhost:8501")

    print("\nPipeline complete.")