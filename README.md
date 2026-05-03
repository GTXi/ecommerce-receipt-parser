# E-Commerce Receipt Parser & Tax Calculator (Regional)

> Individual Assignment: Parallel Programming

> Name: NOR ZAFRAN AQIL | MatrixNo: 2025231608

> Lecturer: SHAHADAN BIN SAAD | CourseCode: ITT440 Network Programming

### [Demo Video](https://youtu.be/3GOp7A1b1kA)

## Project Overview
A Python pipeline that will generate 100,000 dummy e-commerce receipts across 7 Southeast Asian regions and will benchmark 3 programming paradigms against each other on the same workload

| Paradigm | Technique | Use Case in This Project |
| :--- | :--- | :--- |
| **Synchronous** | Sequential baseline | Single-threaded file writes + single-core tax math |
| **Concurrent** | threading + asyncio | I/O-bound file generation (disk writes) |
| **Parallel** | multiprocessing.Pool | CPU-bound tax calculation + currency conversion |

The primary goal is to prove with real numbers why each paradigm exists and when to use it, specifically demonstrating Python's Global Interpreter Lock (GIL) limitations.

## Problem statement
<div align="justify">
Processing 100,000+ e-commerce receipts with compound discounts, 
regional SEA tax brackets, and currency conversion. Benchmarks 
synchronous, concurrent (asyncio/threading), and parallel 
(multiprocessing) approaches to quantify performance differences.
</div>

### System requirements

<div align="justify">

- OS: Windows 10/11, macOS 12+, or Ubuntu 20.04+
- Python 3.11+
- RAM: 8GB minimum 
- SSD recommended (HDD will bottleneck Phase 1, but doable in shaa Allah)
- Free Space: 5GB minimum
- Cpu Cores: 2 cores minimum (4+ recommended for multiprocessing benchmark)

</div>

### Installation

```
git clone https://github.com/GTXi/ecommerce-receipt-parser.git
```
```
cd ecommerce-receipt-parser
```

**Windows(powershell/pycharm):**
```
py -3.11 -m venv .venv
```
```
.venv\Scripts\Activate.ps1
```
**Mac/Linux:**
```
python3.11 -m venv .venv
```
```
source .venv/bin/activate
```

### Install dependencies

```
pip install -r requirements.txt
```

# How to run this project on your machine
Full pipeline : generates files (do this first) + processes
```
python main.py
```

<div align="justify">
  
> Before running the program. Open `config.py` and change the values for `N_RECEIPTS` and `BENCH_N` to you desired or follows the premade setup.

</div>

### Configuration

Edit `config.py` only. No other file needs changing.

| Variable | Default | What it controls |
|---|---|---|
| `N_RECEIPTS` | `100_000` | Total receipts for async + parallel phases |
| `BENCH_N` | `10_000` | Receipts for sync/threaded baseline |
| `SYNC_PROC_N` | `5_000` | Receipts for sync processing baseline |
| `SAVE_CHARTS` | `True` | Set `False` to skip PNG generation |

**Quick test (2 min):** Set `N_RECEIPTS = 5_000` and `BENCH_N = 1_000`

## What happen during the run

|Phase|Process|Action|
|---|---|---|
|Phase [1/7]|Sync writes|sequential baseline (BENCH_N files)|
|Phase [2/7]  |Threaded writes   |ThreadPoolExecutor concurrent I/O|
|Phase [3/7]  |Async writes      |asyncio + aiofiles concurrent I/O (full N_RECEIPTS)|
|Phase [4/7]  |Sync processing   |single-core tax math baseline|
|Phase [5/7]  |Threaded proc.    |GIL demonstration (CPU-bound threading)|
|Phase [6/7]  |Parallel proc.    |multiprocessing.Pool full N_RECEIPTS|
|Phase [7/7]  |Save results      |Parquet files + benchmark_report.txt + 5 PNG charts|

### Generate receipt only
```
python generate_receipts.py
```

### Processing only (requires receipts_data/ to exist)
```
python process_receipts.py
```

<details>
<summary><h2>Troubleshooting</h2></summary>

**`ModuleNotFoundError: No module named 'pandas'`**
Virtual environment is not activated. Run the activate command first.

**`No suitable Python runtime found`**
Python 3.11 is not installed. Download from python.org/downloads/release/python-3119

**`.venv\Scripts\Activate.ps1` cannot be loaded**
Run: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

**Parallel processing hangs**
Watch for "Chunk done" messages. Reduce `N_RECEIPTS` in config.py if needed.

</details>


# Sample output

<img width="583" height="317" alt="Screenshot 2026-05-01 215721" src="https://github.com/user-attachments/assets/a0af852e-f05a-4e4a-a8ae-7a1c62fd608a" />

> results will differs based on your hardware. The most important thing is the speedup ratios

# Benchmark Results 

<img width="1485" height="733" alt="01_benchmark_times" src="https://github.com/user-attachments/assets/af7d4b6c-61c9-49d3-8f47-27b1eb49faa2" />

<div align="middle">
Chart 1: Benchmark: All Methods Compared
</div>
<br>
<img width="1185" height="734" alt="05_processing_paradigms" src="https://github.com/user-attachments/assets/4d9e1c37-2a9a-4ff1-bb4b-2cb84d380ec2" />

<div align="middle">
Chart 2: GIL Limitation: Threading vs Multiprocessing
</div>

### Key findings from the benchmark:
<div align="justify">

- Threading vs Sync (I/O): Threading is faster because the GIL is released during file write syscalls, allowing threads to genuinely overlap disk waits.
- Asyncio vs Sync (I/O): asyncio achieves similar speedup to threading with zero thread-spawning overhead, cooperative multitasking via the event loop.
- Threading vs Sync (CPU): Threading shows minimal improvement on CPU-bound work. The GIL prevents two threads from executing Python bytecode simultaneously and they queue, not parallelise.
- Multiprocessing vs Sync (CPU): Multiprocessing bypasses the GIL by spawning separate OS processes, each with their own interpreter. Near-linear speedup proportional to core count.

> Check the pipeline_output/charts/ folder after running the script to see these results visualised on your own hardware.

</div>

# Technical Reasoning Behind It
<div align="justify">
File I/O is I/O-bound meaning the CPU sits idle while the OS and disk controller do the real work.

`asyncio` uses a single OS thread with a cooperative event loop. Each coroutine `awaits` the file write and immediately takes control, allowing the next coroutine to start. Hundreds of writes overlap with zero thread-spawning overhead.

The `asyncio.Semaphore(500)` cap prevents hitting the OS file descriptor limit (default 1,024 on most systems). Without it, you get `OSError: Too many open files` around file 1,000.
</div>

### Why threading works for I/O but not CPU
<div align="justify">
Python's GIL (Global Interpreter Lock) ensures only one thread executes Python bytecode at a time. The GIL is released during I/O syscalls while threads genuinely overlap disk waits. For CPU-bound work like pandas math, the GIL stays held and threads take turns, giving no parallelism benefit.
</div>

### Why multiprocessing for CPU math

<div align="justify">
multiprocessing spawns separate OS processes, each with their own Python interpreter and their own GIL. True parallelism on all available cores.
The aggregation uses a return-and-concat pattern with each worker returns an independent DataFrame, the main process concatenates after all workers finish. No shared mutable state = no race conditions by construction.
</div>

### Why pandas vectorisation

<div align="justify">
All tax and currency math runs as NumPy C operations across entire columns simultaneously. No Python `for` loop iterates over rows.
</div>

# Conclusion

<div align="justify">
The E-Commerce Receipt Parser experiment clearly demonstrates that selecting the correct programming paradigm depends entirely on the nature of the task and the volume of the data. For simple, small-scale operations, sequential execution remains the most efficient choice as it avoids the unnecessary overhead of managing multiple workers or threads. However, for heavily I/O-bound tasks such as generating and writing 100,000 receipt files to a drive, concurrent execution using `asyncio` or threading drastically reduces wait times by overlapping disk operations. Conversely, for large-scale, CPU-bound tasks like calculating regional taxes and currency conversions across massive data frames, the Python Global Interpreter Lock (GIL) renders threading ineffective. In these cases, true parallel execution via multiprocessing is essential to fully utilize multi-core processors and achieve significant performance gains.

The Receipt Parser pipeline successfully demonstrates:

*   **True parallelism** for CPU-bound mathematical operations using Python's `multiprocessing` module.
*   **The explicit limitations of the GIL**, proving that threads queue rather than parallelize during intensive Python calculations.
*   **Massive I/O efficiency gains** by using `asyncio` and `aiofiles` to write thousands of JSON files concurrently without hitting OS file limits.
*   **Practical performance scaling**, proving that intensive data pipelines can be processed efficiently and yield massive speedups on standard multi-core hardware, such as a Ryzen 5 laptop.
*   **Real-world transferability:** These concepts are directly applicable to actual data engineering, FinTech processing, and mass data migration tasks.

</div>
