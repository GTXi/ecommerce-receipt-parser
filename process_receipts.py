# process_receipts.py
"""
Phase 2 of the Receipt Parser pipeline.

Demonstrates three approaches to CPU-bound processing:
  - process_chunk()          : single-core worker (used by both sync and parallel)
  - run_threaded_processing(): threading — shows GIL limitation on CPU work
  - run_parallel()           : multiprocessing.Pool — bypasses GIL, true parallelism

Key concept: pandas vectorisation avoids Python for-loops entirely.
All tax math runs as NumPy C operations across entire columns at once.
"""

import multiprocessing as mp
import json
import time
import math
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List
from concurrent.futures import ThreadPoolExecutor

# ── REGIONAL TAX RATES ────────────────────────────────────────────────────────
# Stored as plain dicts — pandas .map() turns these into vectorised column ops
REGIONAL_TAX = {
    "MY-KUL": 0.06,   # Malaysia SST 6%
    "MY-PNG": 0.06,
    "SG-01":  0.09,   # Singapore GST 9%
    "TH-BKK": 0.07,   # Thailand VAT 7%
    "ID-JKT": 0.11,   # Indonesia PPN 11%
    "PH-MNL": 0.12,   # Philippines VAT 12%
    "VN-HCM": 0.10,   # Vietnam VAT 10%
}

# ── CURRENCY CONVERSION TO MYR ────────────────────────────────────────────────
CURRENCY_TO_MYR = {
    "MYR": 1.0,
    "SGD": 3.47,
    "THB": 0.13,
    "IDR": 0.000293,
    "PHP": 0.079,
    "VND": 0.000183,
}

# ── CUSTOMER TIER DISCOUNTS ───────────────────────────────────────────────────
TIER_DISCOUNT = {
    "bronze":   0.00,
    "silver":   0.05,
    "gold":     0.10,
    "platinum": 0.15,
}


# ── SINGLE-CORE WORKER ────────────────────────────────────────────────────────
def process_chunk(file_paths: List[Path]) -> pd.DataFrame:
    """
    Reads a list of JSON receipt files, builds a DataFrame, and applies
    all tax/discount/currency math using pandas vectorisation.

    Used by:
      - run_sync_processing()  → called once with a small sample
      - run_parallel()         → called by each multiprocessing worker
      - run_threaded_processing() → called by each ThreadPoolExecutor worker

    Returns a partial DataFrame — NO Python for-loops on rows.
    """
    records = []
    for fp in file_paths:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                receipt = json.load(f)
            items = receipt.get("items", [])
            if not items:
                continue
            # List comprehension for loading items is acceptable —
            # it's data ingestion, not the math we're vectorising
            raw_subtotal = sum(
                item["quantity"] * item["unit_price"] * (1 - item["discount_pct"])
                for item in items
            )
            records.append({
                "receipt_id":    receipt["receipt_id"],
                "timestamp":     receipt["timestamp"],
                "region_code":   receipt["region_code"],
                "currency":      receipt["currency"],
                "customer_tier": receipt["customer_tier"],
                "raw_subtotal":  raw_subtotal,
            })
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            continue   # Skip corrupted files gracefully

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── VECTORISED MATH — all operations run in NumPy C code ──────────────────

    # .map(dict) applies a dict lookup to every row of a Series simultaneously.
    # No Python loop — pandas delegates to C extensions internally.
    df["tax_rate"]          = df["region_code"].map(REGIONAL_TAX).fillna(0.0)
    df["tier_discount_rate"]= df["customer_tier"].map(TIER_DISCOUNT).fillna(0.0)
    df["fx_rate"]           = df["currency"].map(CURRENCY_TO_MYR).fillna(1.0)

    # Series arithmetic broadcasts across all rows at once (NumPy vectorisation)
    # Formula: final = raw × (1 - tier_discount) × (1 + tax_rate) × fx_rate
    df["discounted_subtotal"] = df["raw_subtotal"] * (1 - df["tier_discount_rate"])
    df["tax_amount_myr"]      = df["discounted_subtotal"] * df["tax_rate"] * df["fx_rate"]
    df["final_total_myr"]     = df["discounted_subtotal"] * (1 + df["tax_rate"]) * df["fx_rate"]

    # pd.to_datetime parses all timestamps in one C-level pass
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["month"]     = df["timestamp"].dt.to_period("M").astype(str)

    return df[["receipt_id", "region_code", "currency", "customer_tier",
               "raw_subtotal", "tax_amount_myr", "final_total_myr", "month"]]


# ── PARALLEL PROCESSING (multiprocessing) ─────────────────────────────────────
def run_parallel(all_paths: List[Path], n_workers: int = None) -> pd.DataFrame:
    """
    Splits file paths into N chunks and processes each chunk in a separate
    OS process via multiprocessing.Pool.

    Why no race conditions: workers return independent DataFrames via IPC queue.
    The main process only concatenates AFTER all workers finish — no shared
    mutable state exists at any point.

    Why imap_unordered: returns results as soon as any worker finishes.
    Faster than imap (which waits for ordered results) when order doesn't matter.
    """
    if n_workers is None:
        n_workers = max(1, mp.cpu_count() - 1)  # Leave 1 core for OS

    # np.array_split handles uneven N gracefully (last chunk may differ by 1)
    chunks = [list(c) for c in np.array_split(all_paths, n_workers) if len(c) > 0]

    partial_dfs = []
    with mp.Pool(processes=n_workers) as pool:
        for partial_df in pool.imap_unordered(process_chunk, chunks):
            if not partial_df.empty:
                partial_dfs.append(partial_df)
                print(f"  Chunk done. Collected {len(partial_dfs)}/{len(chunks)} chunks...")

    print("  Concatenating all partial DataFrames...")
    return pd.concat(partial_dfs, ignore_index=True)


# ── THREADED PROCESSING (GIL demonstration) ───────────────────────────────────
def run_threaded_processing(all_paths: list, max_workers: int = 8) -> tuple:
    """
    Runs process_chunk() across multiple threads using ThreadPoolExecutor.

    EXPECTED RESULT: Similar time to sync, NOT faster.
    REASON: process_chunk() is CPU-bound (pandas math, JSON parsing).
    Python's GIL (Global Interpreter Lock) allows only ONE thread to execute
    Python bytecode at a time. Threads queue up at the GIL instead of running
    in parallel — giving you thread overhead with none of the speed benefit.

    This function exists to DEMONSTRATE WHY multiprocessing is needed for
    CPU-bound work. The GIL is released for I/O syscalls (which is why
    threading works for file writes) but NOT for CPU-bound Python code.
    """
    chunk_size = math.ceil(len(all_paths) / max_workers)
    chunks     = [all_paths[i: i + chunk_size]
                  for i in range(0, len(all_paths), chunk_size)]

    partial_dfs = []
    t = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in futures:
            df = future.result()
            if not df.empty:
                partial_dfs.append(df)

    elapsed  = time.perf_counter() - t
    final_df = pd.concat(partial_dfs, ignore_index=True) if partial_dfs else pd.DataFrame()
    return final_df, elapsed


# ── AGGREGATION ───────────────────────────────────────────────────────────────
def aggregate_results(df: pd.DataFrame) -> dict:
    """
    Produces summary statistics from the fully processed DataFrame.
    All groupby/agg operations are vectorised — no Python loops.
    """
    return {
        "total_revenue_myr": df["final_total_myr"].sum(),
        "total_tax_myr":     df["tax_amount_myr"].sum(),
        "total_receipts":    len(df),

        "tax_by_region": (
            df.groupby("region_code")["tax_amount_myr"]
            .sum().reset_index()
            .rename(columns={"tax_amount_myr": "total_tax_myr"})
        ),
        "revenue_by_month": (
            df.groupby("month")["final_total_myr"]
            .sum().reset_index().sort_values("month")
        ),
        "revenue_by_tier": (
            df.groupby("customer_tier")["final_total_myr"]
            .agg(["sum", "mean", "count"]).reset_index()
        ),
        "detail_df": df,
    }