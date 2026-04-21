import pandas as pd
import subprocess
from pathlib import Path


def save_results(summary: dict, output_dir: Path):
    """Save all summary tables as Parquet files."""
    output_dir.mkdir(exist_ok=True)

    # Main detail table
    summary["detail_df"].to_parquet(
        output_dir / "receipts_processed.parquet",
        engine="pyarrow",
        compression="snappy",  # Snappy: fast decompression, ~5x smaller than raw
        index=False,
    )

    # Each summary table saved separately for easy dashboard loading
    summary["tax_by_region"].to_parquet(
        output_dir / "tax_by_region.parquet",
        engine="pyarrow", compression="snappy", index=False
    )
    summary["revenue_by_month"].to_parquet(
        output_dir / "revenue_by_month.parquet",
        engine="pyarrow", compression="snappy", index=False
    )
    summary["revenue_by_tier"].to_parquet(
        output_dir / "revenue_by_tier.parquet",
        engine="pyarrow", compression="snappy", index=False
    )

    # Benchmark timings (Correctly placed inside save_results!)
    if "timings" in summary:
        summary["timings"].to_parquet(
            output_dir / "timings.parquet", engine="pyarrow", compression="snappy", index=False
        )

    print(f"Saved all Parquet files to {output_dir}")


def ship_to_proxmox(local_dir: Path, remote_user: str, remote_host: str, remote_path: str):
    """
    Uses rsync to ship the output to Proxmox.
    Note: Windows doesn't have rsync natively, so we are bypassing the actual
    execution here for now until the Proxmox server is ready to receive it!
    """
    print(f"Shipping to {remote_host}:{remote_path}...")
    print("-> (Automated transfer bypassed on Windows. Files are safely saved locally!)")