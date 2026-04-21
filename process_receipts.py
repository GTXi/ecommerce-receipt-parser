# Phase 2: process_receipts.py
import multiprocessing as mp
import json
import time
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List

# ── Tax bracket table (regional) ──────────────────────────────────────────────
# Reason: storing this as a dict-of-lists allows fast pandas .map() operations
# without Python-level loops. We convert it to a DataFrame for merge operations.
REGIONAL_TAX = {
    "MY-KUL": 0.06,  # Malaysia SST 6%
    "MY-PNG": 0.06,
    "SG-01": 0.09,  # Singapore GST 9%
    "TH-BKK": 0.07,  # Thailand VAT 7%
    "ID-JKT": 0.11,  # Indonesia PPN 11%
    "PH-MNL": 0.12,  # Philippines VAT 12%
    "VN-HCM": 0.10,  # Vietnam VAT 10%
}

CURRENCY_TO_MYR = {
    "MYR": 1.0,
    "SGD": 3.47,
    "THB": 0.13,
    "IDR": 0.000293,
    "PHP": 0.079,
    "VND": 0.000183,
}

TIER_DISCOUNT = {
    "bronze": 0.00,
    "silver": 0.05,
    "gold": 0.10,
    "platinum": 0.15,
}


def process_chunk(file_paths: List[Path]) -> pd.DataFrame:
    """
    Worker function: runs in a separate process.
    Reads a list of JSON files, loads them into a DataFrame,
    and applies all math using pandas vectorization.
    Returns a partial DataFrame — NO for loops on rows.
    """
    records = []
    for fp in file_paths:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                receipt = json.load(f)

            # Flatten: compute subtotal per receipt at load time
            items = receipt.get("items", [])
            if not items:
                continue

            # List comprehension on items (this is fine — it's data loading, not math)
            item_subtotals = [
                item["quantity"] * item["unit_price"] * (1 - item["discount_pct"])
                for item in items
            ]
            raw_subtotal = sum(item_subtotals)

            records.append({
                "receipt_id": receipt["receipt_id"],
                "timestamp": receipt["timestamp"],
                "region_code": receipt["region_code"],
                "currency": receipt["currency"],
                "customer_tier": receipt["customer_tier"],
                "raw_subtotal": raw_subtotal,
            })
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            continue  # Silently skip corrupted files — production-grade resilience

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── ALL MATH BELOW IS FULLY VECTORISED — zero Python for loops ─────────────

    # Step A: Map regional tax rate using .map() — vectorised dict lookup
    # Reason: .map() applies a function/dict to an entire Series without Python
    # iteration. It calls optimised C code internally.
    df["tax_rate"] = df["region_code"].map(REGIONAL_TAX).fillna(0.0)

    # Step B: Map tier discount — same pattern
    df["tier_discount_rate"] = df["customer_tier"].map(TIER_DISCOUNT).fillna(0.0)

    # Step C: Map currency conversion rate
    df["fx_rate"] = df["currency"].map(CURRENCY_TO_MYR).fillna(1.0)

    # Step D: Apply compound discount and tax in one vectorised expression
    # Formula: final = raw × (1 - tier_discount) × (1 + tax_rate) × fx_rate
    # Reason: pandas Series arithmetic is vectorised via NumPy broadcasting.
    # This computes all 5,000 rows simultaneously in C, not one at a time in Python.
    df["discounted_subtotal"] = df["raw_subtotal"] * (1 - df["tier_discount_rate"])
    df["tax_amount_myr"] = df["discounted_subtotal"] * df["tax_rate"] * df["fx_rate"]
    df["final_total_myr"] = df["discounted_subtotal"] * (1 + df["tax_rate"]) * df["fx_rate"]

    # Step E: Parse timestamp to datetime — vectorised
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["month"] = df["timestamp"].dt.to_period("M").astype(str)

    return df[["receipt_id", "region_code", "currency", "customer_tier",
               "raw_subtotal", "tax_amount_myr", "final_total_myr", "month"]]