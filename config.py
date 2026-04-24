# config.py
N_RECEIPTS         = 100_000
BENCH_N            = 10_000
SYNC_PROC_N        = 5_000
SEMAPHORE          = 500
MAX_THREAD_WORKERS = 64

OUTPUT_DIR  = "receipts_data"
RESULTS_DIR = "pipeline_output"

SHIP_TO_PROXMOX = False   # Keep False — saves locally, skips rsync
PROXMOX_USER = "root"
PROXMOX_HOST = "192.168.1.100"
PROXMOX_PATH = "/opt/dashboard-data/"