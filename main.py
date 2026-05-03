# main.py

import time
import asyncio
import multiprocessing as mp
import json
import os
import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# ── LOCAL MODULES ─────────────────────────────────────────────────────────────
from generate_receipts import generate_receipt, generate_all
from process_receipts  import (process_chunk, run_parallel,
                                aggregate_results, run_threaded_processing)
from serialize_and_ship import save_results, save_text_report, save_charts
import config


# ── MEMORY HELPER ─────────────────────────────────────────────────────────────
def _get_memory_mb() -> float:
    """
    Returns current process Resident Set Size in MB.
    Used to measure the memory cost of each processing paradigm
    multiprocessing is faster but uses more RAM (each worker copies memory).
    """
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0   # psutil optional — silently skip if not installed


# ── I/O BENCHMARK HELPERS ─────────────────────────────────────────────────────
def _prepare_receipts(n: int) -> list:
    """
    Pre-generates receipt dicts in memory before timing any writes.
    names/timestamps/SKUs takes real CPU time. By separating this step,
    the I/O benchmark only measures disk throughput, giving threading and
    asyncio a fair comparison against sync.
    """
    print(f"  Pre-generating {n:,} receipt dicts in memory...")
    return [generate_receipt(i) for i in range(n)]


def _write_receipt_from_dict(args: tuple) -> None:
    """
    Writes one pre-generated receipt dict to disk.
    Shared by sync, threaded, and async write paths with same operation,
    different concurrency model wrapping it.
    """
    receipt, base_path = args
    receipt_id = int(receipt["receipt_id"].split("-")[1])
    shard      = receipt_id // 100          # max 100 files per subdirectory
    dir_path   = Path(base_path) / f"{shard:05d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    fp = dir_path / f"{receipt['receipt_id']}.json"
    with open(fp, 'w', encoding='utf-8') as f:
        json.dump(receipt, f)


def run_sync_writes(receipts: list, base_path: Path) -> float:
    """
    SYNC BASELINE for I/O: writes one file at a time, blocking on each.
    The OS must confirm each write before the next begins.
    Slowest possible approach — used as the comparison floor.
    """
    base_path.mkdir(parents=True, exist_ok=True)
    t = time.perf_counter()
    for receipt in receipts:
        _write_receipt_from_dict((receipt, base_path))
    return time.perf_counter() - t


def run_threaded_writes(receipts: list, base_path: Path) -> float:
    """
    CONCURRENT I/O approach 1: ThreadPoolExecutor.
    File write syscalls release the GIL — threads genuinely overlap their
    disk waits even in CPython. Multiple OS-level file operations proceed
    simultaneously without Python bytecode contention.
    max_workers=64: empirical sweet spot before context-switch overhead
    cancels out the I/O concurrency benefit on consumer SSDs.
    """
    base_path.mkdir(parents=True, exist_ok=True)
    args = [(r, base_path) for r in receipts]
    t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=config.MAX_THREAD_WORKERS) as ex:
        ex.map(_write_receipt_from_dict, args)
    return time.perf_counter() - t


async def _write_one_async(receipt: dict, base_path: Path, sem: asyncio.Semaphore):
    """Single async coroutine — writes one receipt under a shared semaphore."""
    import aiofiles
    receipt_id = int(receipt["receipt_id"].split("-")[1])
    shard      = receipt_id // 100
    dir_path   = base_path / f"{shard:05d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    fp = dir_path / f"{receipt['receipt_id']}.json"
    async with sem:   # Releases slot when write completes
        async with aiofiles.open(fp, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(receipt))


async def _async_write_all(receipts: list, base_path: Path):
    """Fires all write coroutines concurrently via asyncio.gather."""
    sem   = asyncio.Semaphore(config.SEMAPHORE)
    tasks = [_write_one_async(r, base_path, sem) for r in receipts]
    await asyncio.gather(*tasks)


def run_async_writes(receipts: list, base_path: Path) -> float:
    """
    CONCURRENT I/O approach 2: asyncio + aiofiles.
    Single OS thread. The event loop schedules coroutines cooperatively —
    each coroutine yields at 'await', immediately allowing the next to start.
    Zero thread-spawning overhead. Semaphore prevents hitting OS file
    descriptor limits (default 1024 on Linux, similar on Windows).
    """
    base_path.mkdir(parents=True, exist_ok=True)
    t = time.perf_counter()
    asyncio.run(_async_write_all(receipts, base_path))
    return time.perf_counter() - t


def run_sync_processing(paths: list) -> tuple:
    """SYNC BASELINE for CPU processing: single core, no parallelism."""
    t  = time.perf_counter()
    df = process_chunk(paths)
    return df, time.perf_counter() - t


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# All execution lives inside if __name__ == "__main__" to prevent
# multiprocessing workers from re-running the orchestration code on spawn.
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":

    OUTPUT_DIR  = Path(config.OUTPUT_DIR)
    RESULTS_DIR = Path(config.RESULTS_DIR)
    n_workers   = max(1, mp.cpu_count() - 1)

    print("=" * 64)
    print("  E-COMMERCE RECEIPT PARSER — CONCURRENCY BENCHMARK")
    print("=" * 64)
    print(f"  Scale        : {config.BENCH_N:>10,} receipts  (I/O benchmark)")
    print(f"                 {config.N_RECEIPTS:>10,} receipts  (full async + parallel run)")
    print(f"  CPU cores    : {mp.cpu_count()} logical  ({n_workers} used for parallel)")
    print(f"  Baseline RAM : {_get_memory_mb():.0f} MB")
    print("=" * 64)

    # ── PRE-GENERATE RECEIPTS IN MEMORY ───────────────────────────────────────
    bench_receipts = _prepare_receipts(config.BENCH_N)

    # ── [1/7] SYNC WRITES ─────────────────────────────────────────────────────
    print(f"\n[1/7] Sync writes ({config.BENCH_N:,} receipts — sequential baseline)...")
    sync_write_time = run_sync_writes(bench_receipts, OUTPUT_DIR / "sync")
    print(f"  ✓ SYNC:              {sync_write_time:.3f}s")

    # ── [2/7] THREADED WRITES ─────────────────────────────────────────────────
    print(f"\n[2/7] Threaded writes ({config.BENCH_N:,} receipts, "
          f"{config.MAX_THREAD_WORKERS} workers)...")
    thread_write_time = run_threaded_writes(bench_receipts, OUTPUT_DIR / "threaded")
    print(f"  ✓ THREADING:         {thread_write_time:.3f}s  "
          f"({sync_write_time/max(thread_write_time,0.001):.1f}x vs sync)")

    # ── [3/7] ASYNC WRITES ────────────────────────────────────────────────────
    print(f"\n[3/7] Async writes ({config.N_RECEIPTS:,} receipts, "
          f"semaphore={config.SEMAPHORE})...")
    full_receipts    = bench_receipts + [
        generate_receipt(i) for i in range(config.BENCH_N, config.N_RECEIPTS)
    ]
    async_write_time = run_async_writes(full_receipts, OUTPUT_DIR / "async")
    # Normalise to BENCH_N so all three I/O methods are on the same scale
    async_write_norm = (async_write_time / config.N_RECEIPTS) * config.BENCH_N
    print(f"  ✓ ASYNCIO:           {async_write_time:.3f}s total  "
          f"({async_write_norm:.3f}s normalised to {config.BENCH_N:,} | "
          f"{sync_write_time/max(async_write_norm,0.001):.1f}x vs sync)")

    # ── COLLECT FILE PATHS ────────────────────────────────────────────────────
    all_paths = sorted((OUTPUT_DIR / "async").rglob("*.json"))
    print(f"\n  Found {len(all_paths):,} receipt files ready for processing.")

    # ── [4/7] SYNC PROCESSING ─────────────────────────────────────────────────
    sync_sample = list(all_paths[:config.SYNC_PROC_N])
    print(f"\n[4/7] Sync processing ({config.SYNC_PROC_N:,} receipts — CPU baseline)...")
    mem_before        = _get_memory_mb()
    _, sync_proc_time = run_sync_processing(sync_sample)
    mem_sync_delta    = _get_memory_mb() - mem_before
    sync_proc_extrap  = (sync_proc_time / config.SYNC_PROC_N) * config.N_RECEIPTS
    print(f"  ✓ SYNC processing:   {sync_proc_time:.3f}s  "
          f"(extrap {config.N_RECEIPTS//1000}k: {sync_proc_extrap:.1f}s)  "
          f"| +{mem_sync_delta:.0f} MB RAM")

    # ── [5/7] THREADED PROCESSING (GIL demonstration) ─────────────────────────
    threaded_sample = list(all_paths[:config.SYNC_PROC_N])
    print(f"\n[5/7] Threaded processing ({config.SYNC_PROC_N:,} receipts, 8 workers)...")
    print(f"  Note: CPU-bound work — GIL prevents true thread parallelism.")
    print(f"        Expected: similar time to sync. This proves why multiprocessing exists.")
    mem_before            = _get_memory_mb()
    _, threaded_proc_time = run_threaded_processing(threaded_sample, max_workers=8)
    mem_threaded_delta    = _get_memory_mb() - mem_before
    threaded_proc_extrap  = (threaded_proc_time / config.SYNC_PROC_N) * config.N_RECEIPTS
    print(f"  ✓ THREADED:          {threaded_proc_time:.3f}s  "
          f"(extrap {config.N_RECEIPTS//1000}k: {threaded_proc_extrap:.1f}s)  "
          f"| +{mem_threaded_delta:.0f} MB RAM")

    # ── [6/7] PARALLEL PROCESSING ─────────────────────────────────────────────
    print(f"\n[6/7] Parallel processing ({config.N_RECEIPTS:,} receipts, "
          f"{n_workers} workers)...")
    mem_before         = _get_memory_mb()
    t_par              = time.perf_counter()
    final_df           = run_parallel(all_paths)
    parallel_proc_time = time.perf_counter() - t_par
    mem_parallel_delta = _get_memory_mb() - mem_before
    print(f"  ✓ MULTIPROCESSING:   {parallel_proc_time:.3f}s  "
          f"| +{mem_parallel_delta:.0f} MB RAM  "
          f"(higher RAM = each worker gets its own memory copy)")

    # ── AGGREGATE RESULTS ─────────────────────────────────────────────────────
    summary = aggregate_results(final_df)

    # ── BENCHMARK TABLE ───────────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print("  BENCHMARK RESULTS")
    print(f"  I/O rows normalised to {config.BENCH_N:,} | "
          f"CPU rows extrapolated to {config.N_RECEIPTS:,}")
    print("=" * 64)
    print(f"  {'Paradigm':<26} {'Method':<20} {'Time':>8}  {'vs Sync':>10}")
    print("  " + "-" * 62)

    rows = [
        ("I/O — file writes",  "Sync (baseline)",    sync_write_time,       1.0),
        ("I/O — file writes",  "Threading",          thread_write_time,     sync_write_time / max(thread_write_time, 0.001)),
        ("I/O — file writes",  "Asyncio",            async_write_norm,      sync_write_time / max(async_write_norm,  0.001)),
        ("CPU — tax math",     "Sync (baseline)",    sync_proc_extrap,      1.0),
        ("CPU — tax math",     "Threading (GIL)",    threaded_proc_extrap,  sync_proc_extrap / max(threaded_proc_extrap, 0.001)),
        ("CPU — tax math",     "Multiprocessing",    parallel_proc_time,    sync_proc_extrap / max(parallel_proc_time,   0.001)),
    ]
    for phase, method, secs, speedup in rows:
        marker = "◀ baseline" if speedup == 1.0 else f"{speedup:.1f}x faster"
        print(f"  {phase:<26} {method:<20} {secs:>7.2f}s  {marker}")

    print("  " + "-" * 62)
    # NEW — fixed label width + fixed number column width
    W = 22  # label column width
    N = 20  # number column width
    print(f"  {'Total Revenue (MYR):':<{W}} {summary['total_revenue_myr']:>{N},.2f}")
    print(f"  {'Total Tax (MYR):':<{W}} {summary['total_tax_myr']:>{N},.2f}")
    print(f"  {'Receipts processed:':<{W}} {summary['total_receipts']:>{N},}")
    print(f"  {'CPU cores used:':<{W}} {f'{n_workers} / {mp.cpu_count()}':>{N}}")
    print("=" * 64)

    # ── SAVE TIMINGS TO SUMMARY ───────────────────────────────────────────────
    timings_df = pd.DataFrame([
        {"phase": "I/O writes",     "method": "Sync",            "seconds": sync_write_time,      "n": config.BENCH_N},
        {"phase": "I/O writes",     "method": "Threading",       "seconds": thread_write_time,    "n": config.BENCH_N},
        {"phase": "I/O writes",     "method": "Asyncio",         "seconds": async_write_norm,     "n": config.BENCH_N},
        {"phase": "CPU processing", "method": "Sync",            "seconds": sync_proc_extrap,     "n": config.N_RECEIPTS},
        {"phase": "CPU processing", "method": "Threading (GIL)", "seconds": threaded_proc_extrap, "n": config.N_RECEIPTS},
        {"phase": "CPU processing", "method": "Multiprocessing", "seconds": parallel_proc_time,   "n": config.N_RECEIPTS},
    ])
    summary["timings"] = timings_df

    # ── [7/7] SAVE ALL RESULTS LOCALLY ────────────────────────────────────────
    print(f"\n[7/7] Saving results to {RESULTS_DIR}/...")
    save_results(summary, RESULTS_DIR)
    save_text_report(rows, summary, RESULTS_DIR, n_workers)

    if config.SAVE_CHARTS:
        print()
        print(f"  Generating charts...")
        save_charts(summary, RESULTS_DIR, dpi=config.CHART_DPI)

    print(f"\n{'=' * 64}")
    print(f"  Pipeline complete.")
    print(f"  Results saved to: {RESULTS_DIR.resolve()}")
    print(f"{'=' * 64}")