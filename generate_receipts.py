# Phase 1: generate_receipts.py
import asyncio
import aiofiles
import json
import random
import time
from faker import Faker
from pathlib import Path

fake = Faker()

REGIONS = ["MY-KUL", "MY-PNG", "SG-01", "TH-BKK", "ID-JKT", "PH-MNL", "VN-HCM"]
CURRENCIES = {"MY-KUL": "MYR", "MY-PNG": "MYR", "SG-01": "SGD",
              "TH-BKK": "THB", "ID-JKT": "IDR", "PH-MNL": "PHP", "VN-HCM": "VND"}


def generate_receipt(receipt_id: int) -> dict:
    """Pure CPU function — generates a single receipt dict. No I/O here."""
    region = random.choice(REGIONS)
    num_items = random.randint(1, 15)
    items = [
        {
            "sku": fake.bothify(text="SKU-????-####"),
            "name": fake.ecommerce_name() if hasattr(fake, 'ecommerce_name') else fake.word(),
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(1.5, 500.0), 2),
            "discount_pct": round(random.uniform(0, 0.40), 4),
        }
        for _ in range(num_items)
    ]
    return {
        "receipt_id": f"RCP-{receipt_id:07d}",
        "timestamp": fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
        "region_code": region,
        "currency": CURRENCIES[region],
        "customer_tier": random.choice(["bronze", "silver", "gold", "platinum"]),
        "shipping_code": fake.bothify(text="SHP-??-####"),
        "items": items,
    }


async def write_receipt(receipt_id: int, base_path: Path, sem: asyncio.Semaphore):
    """Coroutine: generates JSON and writes to disk. Semaphore caps open handles."""
    receipt = generate_receipt(receipt_id)

    # Shard into subdirectories: receipts/00042/RCP-0042001.json
    # This prevents OS directory inode exhaustion (explained in Step 1.2)
    shard = receipt_id // 100  # 100 files per directory
    dir_path = base_path / f"{shard:05d}"
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path / f"{receipt['receipt_id']}.json"

    async with sem:  # Blocks here if 500 file handles are already open
        async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(receipt, ensure_ascii=False))


async def generate_all(n_receipts: int, base_path: Path):
    """Entry point: fires off all coroutines and lets the event loop manage them."""
    sem = asyncio.Semaphore(500)  # Hard cap: never exceed 500 concurrent open files

    tasks = [
        write_receipt(i, base_path, sem)
        for i in range(n_receipts)
    ]

    # asyncio.gather fires all tasks concurrently but uses only one OS thread
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    OUTPUT_PATH = Path("./receipts_data")
    N = 100_000

    t_start = time.perf_counter()
    asyncio.run(generate_all(N, OUTPUT_PATH))
    t_end = time.perf_counter()

    print(f"[CONCURRENT] Generated {N} receipts in {t_end - t_start:.2f}s")