# High-Performance Resilient File Replication Engine (RoboKopi.py)

An industrial-grade, single-file data replication engine written in Python. This utility bridges the gap between raw low-level unbuffered block I/O operations and transactional state safety, providing a robust synchronization framework optimized for high-bandwidth local mounts, cloud storage interfaces, and network transport boundaries.

Unlike traditional replication utilities that maintain processing states entirely inside volatile system memory maps, this script records stream milestones dynamically down to physical storage surfaces. If interrupted by unexpected connection timeouts, storage fabric panics, or power disruptions, execution resume routines restore state parameters instantly without losing pre-buffered byte layouts.

---

## Technical Architecture & Core Design Primitives

The engine is engineered around three distinct operational planes combined into a single, cohesive namespace module:

1. **Primary Byte Streaming Plane**: Manages low-level file descriptor handles, executes unbuffered sequential block I/O reads/writes, calculates intermediate incremental SHA-256 cryptographic checksums, and performs single-instruction atomic directory tree transformations.
2. **Secondary Safeguard & Cache Management Plane**: Controls multi-threaded mutex gates, manages global thread processing barriers, validates disk fragment alignment integrity, and maintains the persistent json progress state database file (`.sync_progress.json`) equipped with append-only checksum verification verification blocks.
3. **Tertiary Telemetry & System Utilities Plane**: Decodes command-line arguments, translates operational final states into standard industrial bitmask exit status metrics, logs diagnostics across persistent files, and handles runtime performance auditing summary emissions.

---

## Functional Architecture Directory Mapping

The following catalog outlines the internal functional layers organizing the execution footprint of `RoboKopi.py`:

### Core Data Streams & Block I/O Primitives
* `read_file_metadata_quick(file_path)`: Pulls size, modification timestamps, and hardware inode allocation markers using filesystem kernel shortcuts.
* `compute_partial_hash(file_path, block_size)`: Evaluates a deterministic SHA-256 fingerprint scanning exclusively across head and tail storage blocks to safely accelerate resume matches.
* `compute_unique_file_id(inode, size, mtime, partial_hash)`: Generates an MD5 asset verification signature identifying mutations across source paths.
* `scan_source_build_file_list(source_dir)`: Recursively scans source trees using low-overhead directory walk iterators.
* `classify_files_against_cache(file_list, cache_data)`: Distributes items across concrete strategy execution arrays (`skip`, `resume`, or `new`).
* `stream_data_incremental(...)`: Executes the sequential block-by-block data copy loop, enforcing global suspend evaluation gates at every step.
* `atomic_rename_part_to_final(part_path, final_path, atomic_fs_flag)`: Invokes single-instruction kernel pointer swaps (`os.replace`) to switch completed items into active production space.

### Resiliency Barriers & Cache Mechanics
* `acquire_cache_mutex()` / `release_cache_mutex()`: Controls concurrency access around active progress data parameters.
* `set_global_suspend_event()` / `clear_global_suspend_event()`: Manipulates internal system flags to pause or release worker loops mid-stream.
* `write_progress_tmp_atomic(progress_path, cache_data, atomic_fs_flag)`: Writes memory-mapped state tracking details to disk, sealing the payload with a trailing verification signature.
* `classify_failure_transient_vs_persistent(exc)`: Analyzes hardware errors to determine if a connection issue can be safely bypassed or if it represents a hard limits failure.

### Command Line Interfacing & Metrics Reporting
* `parse_cli_arguments()`: Parses incoming execution parameters, wrapping Windows slash parameters onto standard long options.
* `orchestrate_production_sync(config)`: Coordinates multi-tier module tasks, setting up logging directories and executing tasks.
* `classify_failures_and_emit_exitcode(summary)`: Maps operation outcomes to standard bitmask parameters for integration into continuous pipeline workflows.

---

## Execution Heuristics Strategy Workflow

The engine evaluates file assets using an integrated identity match system rather than relying on shallow file naming passes. This diagram tracks how files are analyzed, processed, and validated from discovery down to atomic final commit:

```mermaid
graph TD
    A[Scan Source Asset Directory] --> B[Compute Asset Unique ID Token]
    B --> C{Token Present in Cache?}
    
    C -- Yes --> D{Is p100 Milestone Verified?}
    D -- Yes --> E[Classify as SKIP - Target Is Invariant]
    D -- No --> F[Classify as RESUME - Partial Target Identified]
    
    C -- No --> G[Classify as NEW / MUTATED TASK]
    
    F --> H[Verify Size and Part Alignment]
    G --> I[Truncate Target to Byte 0]
    
    H --> J[Seek Handles to Last Milestone Offset]
    I --> K[Open Block Stream Loop Descriptor]
    J --> K
    
    K --> L{Suspend Gate Set?}
    L -- Yes --> M[Yield Loop - Wait for Connection Recovery]
    M --> L
    L -- No --> N[Read Unbuffered Data Chunk]
    
    N --> O[Write Chunk and Update Checksum Hasher]
    O --> P{Crossed 25-50-75 Threshold?}
    P -- Yes --> Q[Flush Progress Database State safely to Disk]
    P -- No --> R{EOF Reached?}
    Q --> R
    
    R -- No --> L
    R -- Yes --> S[Verify Full-Stream Cryptographic SHA-256 Match]
    
    S -- Pass --> T[Execute OS Kernel Atomic Rename Swap]
    S -- Fail --> U[Flag Asset Corrupted - Write Local Diagnostic Logs]
    
    T --> V[Mark p100 True - Commit Summary Report to Storage]
    V --> W[Emit Industrial Bitmask Status Code and Exit Cleanly]
    U --> W

---

## Operating Instructions & Command Line Interface

Invoke the engine using either standard POSIX parameters or traditional RoboKopi option variables.

### General Syntax Structure

```bash
python3 RoboKopi.py <source_path> <destination_path> [options]

```

### Supported Performance Parameter Configurations

* `source`: Path pointing to the source data folder boundary.
* `destination`: Path pointing to the target migration or backup folder location.
* `--MT`, `/MT <int>`: Allocates multi-threaded background processing pools (Default: `1`).
* `--BUFFER`, `/BUFFER <bytes>`: Adjusts unbuffered data loop transfer block chunk capacity limits (Default: `2097152` for 2MB blocks).
* `--NOATOMIC`, `/NOATOMIC`: Forces standard destructive replacement loops, bypassing single-instruction file system rename operations.

### Deployment Examples

```bash
# Basic Local Synchronization File Migration Run
python3 ./RoboKopi.py /Users/loundsv/Downloads /Volumes/media

# Aggressive Optimization Pass Using custom 4MB Block Arrays
python3 ./RoboKopi.py /Users/loundsv/Downloads /Volumes/media --MT 4 --buffer-size 4194304

# Resilient Processing Run Applying traditional Option Slashes
python3 ./RoboKopi.py /Users/loundsv/Downloads /Volumes/media /MT 8 /BUFFER 2097152

```

---

## Infrastructure Bitmask Exit Code Table Reference

The engine terminates using a bitmask mapping model, letting automation environments evaluate the exact health of completed tasks:

* `0` (0x00): Invariant Run. Source matched destination perfectly. No data transformations required.
* `1` (0x01): Replication Success. Data blocks streamed, passed cryptographic verification, and committed cleanly with zero errors.
* `8` (0x08): Severe Systemic Outage. Found unrecoverable operating system permission blocks or fatal storage disk hardware faults. Process halted to safeguard your file layout state.

```
