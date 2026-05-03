# generate_receipts.py
"""
Phase 1 of the Receipt Parser pipeline.

Demonstrates CONCURRENT I/O using asyncio + aiofiles to generate and write
100,000 JSON receipt files to disk. A Semaphore caps concurrent open file
handles at 500, preventing OSError from file descriptor exhaustion.

Key concept: asyncio uses a single OS thread. Coroutines yield control at
every 'await', allowing hundreds of file writes to overlap without spawning
hundreds of OS threads.
"""

import asyncio
import aiofiles
import json
import random
import time
from faker import Faker
from pathlib import Path

fake = Faker()

# Southeast Asian regions with their local currencies
REGIONS = ["MY-KUL", "MY-PNG", "SG-01", "TH-BKK", "ID-JKT", "PH-MNL", "VN-HCM"]
CURRENCIES = {
    "MY-KUL": "MYR", "MY-PNG": "MYR",
    "SG-01":  "SGD", "TH-BKK": "THB",
    "ID-JKT": "IDR", "PH-MNL": "PHP", "VN-HCM": "VND",
}


def generate_receipt(receipt_id: int) -> dict:
    """
    Pure CPU function and this will generate one receipt dict in memory.
    No file I/O here. Called by both async and sync/threaded paths.
    Faker is CPU-bound (string generation), which is why we pre-generate
    receipts before benchmarking the I/O writes.
    """
    region    = random.choice(REGIONS)
    num_items = random.randint(1, 15)
    items = [
        {
            "sku":          fake.bothify(text="SKU-????-####"),
            "name":         fake.word(),
            "quantity":     random.randint(1, 10),
            "unit_price":   round(random.uniform(1.5, 500.0), 2),
            "discount_pct": round(random.uniform(0, 0.40), 4),
        }
        for _ in range(num_items)
    ]
    return {
        "receipt_id":    f"RCP-{receipt_id:07d}",
        "timestamp":     fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
        "region_code":   region,
        "currency":      CURRENCIES[region],
        "customer_tier": random.choice(["bronze", "silver", "gold", "platinum"]),
        "shipping_code": fake.bothify(text="SHP-??-####"),
        "items":         items,
    }


async def _write_one_async(receipt: dict, base_path: Path, sem: asyncio.Semaphore):
    """
    Single async coroutine: writes one pre-generated receipt to disk.
    'async with sem' blocks if 500 handles are already open, then releases
    automatically — prevents OSError: Too many open files.
    """
    receipt_id = int(receipt["receipt_id"].split("-")[1])
    shard      = receipt_id // 100          # 100 files per subdirectory
    dir_path   = base_path / f"{shard:05d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    fp = dir_path / f"{receipt['receipt_id']}.json"

    async with sem:
        async with aiofiles.open(fp, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(receipt, ensure_ascii=False))


async def generate_all(n_receipts: int, base_path: Path, semaphore_limit: int = 500):
    """
    Async entry point: generates all receipt dicts then fires all write
    coroutines concurrently via asyncio.gather.
    Single OS thread — the event loop schedules coroutines cooperatively.
    """
    base_path.mkdir(parents=True, exist_ok=True)
    sem      = asyncio.Semaphore(semaphore_limit)
    receipts = [generate_receipt(i) for i in range(n_receipts)]
    tasks    = [_write_one_async(r, base_path, sem) for r in receipts]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Standalone test: run just the async generation phase
    from pathlib import Path
    OUTPUT_PATH = Path("./receipts_data/async")
    N = 10_000
    t_start = time.perf_counter()
    asyncio.run(generate_all(N, OUTPUT_PATH))
    print(f"[ASYNC] Generated {N} receipts in {time.perf_counter() - t_start:.2f}s")