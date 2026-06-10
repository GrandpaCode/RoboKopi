import os
import sys
import json
import time
import socket
import hashlib
import logging
import traceback
import subprocess
import threading
import argparse
import queue
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, asdict, field
from typing import Dict, Tuple, List, Any, Optional

# --- Mutex Synchronization Layer & Global Volatile States ---
_cache_lock = threading.Lock()
_log_lock = threading.Lock()
_registry_lock = threading.Lock()
_throttler_lock = threading.Lock()
_telemetry_lock = threading.Lock()

_global_suspend_event = threading.Event()
_global_suspend_event.set()

# Track active directory footprints globally across worker threads
_active_directory_registry = set()

# Global runtime telemetry store for the observer thread
_thread_telemetry_registry = {}
_global_bytes_transferred = 0

logger = logging.getLogger("SyncEngine")

class StorageThrottler:
    """
    Monitors engine health and adapts thread allocation ceilings in real-time.
    Implements an aggressive safety drop if metadata errors hit small/empty files.
    """
    def __init__(self, requested_max: int):
        self.max_allowed_workers = requested_max
        self.current_ceiling = requested_max
        self.consecutive_successes = 0

    def get_ceiling(self) -> int:
        with _throttler_lock:
            return self.current_ceiling

    def handle_success(self):
        with _throttler_lock:
            self.consecutive_successes += 1
            if self.consecutive_successes >= 100 and self.current_ceiling < self.max_allowed_workers:
                self.current_ceiling += 1
                self.consecutive_successes = 0
                _log_lock.acquire()
                print(f"\n[THROTTLE-RAMP] Mount point stable. Raising thread ceiling to {self.current_ceiling}.\n", flush=True)
                _log_lock.release()

    def handle_failure(self, file_size: int, error_code: int) -> float:
        with _throttler_lock:
            self.consecutive_successes = 0
            old_ceiling = self.current_ceiling
            
            if file_size <= 4096 or error_code in (16, 35):
                self.current_ceiling = 1
                cool_off = 3.5
                _log_lock.acquire()
                print(f"\n[CIRCUIT-BREAKER] Metadata saturation signature caught on file size {file_size}b (Errno {error_code})!", flush=True)
                print(f"                  Slamming execution ceiling from {old_ceiling} down to 1 thread for a deep cool-off.\n", flush=True)
                _log_lock.release()
            else:
                self.current_ceiling = max(1, int(self.current_ceiling / 2))
                cool_off = 1.5
                _log_lock.acquire()
                print(f"\n[THROTTLE-BACKOFF] Disk write saturation. Halving ceiling from {old_ceiling} to {self.current_ceiling}.\n", flush=True)
                _log_lock.release()
            
            return cool_off

class TransferResult:
    def __init__(self, bytes_transferred: int, final_hash: str, exception: Optional[Exception] = None):
        self.bytes_transferred = bytes_transferred
        self.final_hash = final_hash
        self.exception = exception

class FsType(Enum):
    LOCAL = 1
    NFS = 2
    CIFS = 3
    UNKNOWN = 4

@dataclass
class JobSummary:
    source_directory: str
    destination_directory: str
    total_files_scanned: int = 0
    files_skipped: int = 0
    files_copied: int = 0
    files_failed: int = 0
    total_bytes_transferred: int = 0
    total_acl_warnings: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    elapsed_seconds: float = 0.0
    execution_success: bool = True
    failure_reasons: List[str] = field(default_factory=list)

# --- Low-Level File Descriptor and Block Stream Primitives ---

def read_file_metadata_quick(file_path: str) -> Tuple[int, float, int]:
    stat = os.stat(file_path)
    return int(stat.st_size), float(stat.st_mtime), int(stat.st_ino)

def compute_partial_hash(file_path: str, block_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        if file_size <= block_size * 2:
            hasher.update(f.read())
        else:
            hasher.update(f.read(block_size))
            f.seek(file_size - block_size)
            hasher.update(f.read(block_size))
    return hasher.hexdigest()

def compute_unique_file_id(inode: int, size: int, mtime: float, partial_hash: str) -> str:
    payload = f"inode:{inode}|size:{size}|mtime:{mtime}|phash:{partial_hash}"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def scan_source_build_file_list(source_dir: str) -> List[Dict[str, Any]]:
    file_list = []
    source_path = Path(source_dir)
    if not source_path.is_dir():
        raise OSError(f"Source target '{source_dir}' is unreachable or malformed.")
    for p in source_path.rglob("*"):
        if p.is_file():
            try:
                abs_str = str(p.resolve())
                rel_path = str(p.relative_to(source_path))
                size, mtime, inode = read_file_metadata_quick(abs_str)
                phash = compute_partial_hash(abs_str)
                uid = compute_unique_file_id(inode, size, mtime, phash)
                file_list.append({
                    "unique_id": uid,
                    "absolute_path": abs_str,
                    "relative_path": rel_path,
                    "size": size,
                    "mtime": mtime,
                    "inode": inode,
                    "partial_hash": phash
                })
            except OSError:
                continue
    return file_list

def classify_files_against_cache(file_list: List[Dict[str, Any]], cache_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    classified = {"skip": [], "resume": [], "new": []}
    for file_entry in file_list:
        uid = file_entry["unique_id"]
        if uid in cache_data:
            if cache_data[uid].get("p100", False):
                classified["skip"].append(file_entry)
            else:
                classified["resume"].append(file_entry)
        else:
            classified["new"].append(file_entry)
    return classified

def init_file_milestone_booleans() -> Dict[str, bool]:
    return {"p25": False, "p50": False, "p75": False, "p100": False}

def validate_resume_dest_part(part_path: str, expected_offset: int) -> Tuple[bool, int, str]:
    if not os.path.exists(part_path):
        return False, 0, "Target execution block fragment missing on disk."
    actual_size = os.path.getsize(part_path)
    if actual_size < expected_offset:
        return False, actual_size, f"Allocation disparity (Found {actual_size}, Expected >= {expected_offset})"
    return True, expected_offset, "Block boundary match verified."

def validate_and_maybe_create_parent_dirs(dest_file_path: str) -> bool:
    parent_dir = os.path.dirname(dest_file_path)
    if not parent_dir:
        return False
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
        return True
    return False

def open_source_and_dest_part_buffered(src_path: str, dest_part_path: str, is_resume: bool) -> Tuple[Any, Any]:
    validate_and_maybe_create_parent_dirs(dest_part_path)
    src_stream = open(src_path, "rb")
    try:
        if is_resume:
            dest_stream = open(dest_part_path, "rb+")
        else:
            dest_stream = open(dest_part_path, "wb")
    except Exception as e:
        src_stream.close()
        raise e
    return src_stream, dest_stream

def stream_data_incremental_buffered(
    src_stream: Any, dest_stream: Any, total_size: int, start_offset: int, 
    buffer_size: int, progress_callback: Any, file_uid: str, worker_id: int
) -> TransferResult:
    global _global_bytes_transferred
    bytes_copied = start_offset
    
    src_hasher = hashlib.sha256()
    dest_hasher = hashlib.sha256()
    
    milestone_idx = 0
    thresholds = [0.25, 0.50, 0.75]
    
    is_large_file = total_size > (250 * 1024 * 1024)
    chunk_counter = 0

    try:
        if start_offset > 0:
            resume_seed = f"resumed_at:{start_offset}".encode("utf-8")
            src_hasher.update(resume_seed)
            dest_hasher.update(resume_seed)
            
            src_stream.seek(start_offset, os.SEEK_SET)
            dest_stream.seek(start_offset, os.SEEK_SET)

        while milestone_idx < len(thresholds) and bytes_copied >= int(total_size * thresholds[milestone_idx]):
            milestone_idx += 1

        while True:
            _global_suspend_event.wait()
            chunk = src_stream.read(buffer_size)
            if not chunk:
                break
                
            src_hasher.update(chunk)
            dest_stream.write(chunk)
            dest_hasher.update(chunk)
            
            chunk_len = len(chunk)
            bytes_copied += chunk_len
            chunk_counter += 1
            
            with _telemetry_lock:
                _global_bytes_transferred += chunk_len
                if worker_id in _thread_telemetry_registry:
                    _thread_telemetry_registry[worker_id]["offset"] = bytes_copied

            if not is_large_file or (chunk_counter % 8 == 0):
                dest_stream.flush()
                try:
                    os.fsync(dest_stream.fileno())
                except OSError as e:
                    if e.errno not in (16, 35):
                        raise e
            
            if milestone_idx < len(thresholds) and bytes_copied >= int(total_size * thresholds[milestone_idx]):
                milestone_key = f"p{int(thresholds[milestone_idx] * 100)}"
                progress_callback(file_uid, milestone_key, True, bytes_copied)
                milestone_idx += 1
        
        dest_stream.flush()
        try:
            os.fsync(dest_stream.fileno())
        except OSError:
            pass

        if src_hasher.hexdigest() != dest_hasher.hexdigest():
            raise ValueError("Inline streaming parity check failed. Payload compromised.")
            
        return TransferResult(bytes_copied, dest_hasher.hexdigest())
    except Exception as e:
        return TransferResult(bytes_copied, "", e)

def atomic_rename_part_to_final(part_path: str, final_path: str, atomic_fs_flag: bool) -> bool:
    try:
        if atomic_fs_flag:
            os.replace(part_path, final_path)
            return True
        else:
            if os.path.exists(final_path):
                os.remove(final_path)
            import shutil
            shutil.copy2(part_path, final_path)
            os.remove(part_path)
            return True
    except (OSError, IOError):
        return False

def detect_filesystem_type(path: str) -> Tuple[FsType, bool]:
    if sys.platform == "win32":
        return FsType.LOCAL, True
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[1] in path:
                    fs_str = parts[2].lower()
                    if "nfs" in fs_str:
                        return FsType.NFS, False
                    if "cifs" in fs_str or "smb" in fs_str:
                        return FsType.CIFS, False
    except Exception:
        pass
    return FsType.LOCAL, True

def check_fd_limits_and_estimate_peak(max_workers: int) -> int:
    try:
        import resource
        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        return soft_limit
    except ImportError:
        return 512

def append_failed_log(log_path: str, file_path: str, failure_type: str, details: str) -> None:
    _log_lock.acquire()
    try:
        payload = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            "target_asset": file_path,
            "failure_classification": failure_type,
            "exception_details": details
        }
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except (IOError, OSError):
            pass
    finally:
        _log_lock.release()

def write_progress_tmp_atomic(progress_path: str, cache_data: Dict[str, Any], atomic_fs_flag: bool) -> None:
    try:
        tmp_path = progress_path + ".tmp"
        serialized_bytes = json.dumps(cache_data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        checksum = hashlib.sha256(serialized_bytes).hexdigest().encode("utf-8")
        
        with open(tmp_path, "wb") as f:
            f.write(serialized_bytes + b"|CHECKSUM:" + checksum)
            f.flush()
            os.fsync(f.fileno())
            
        if atomic_fs_flag:
            os.replace(tmp_path, progress_path)
        else:
            bak_path = progress_path + ".bak"
            if os.path.exists(progress_path):
                os.rename(progress_path, bak_path)
            with open(progress_path, "wb") as f:
                f.write(serialized_bytes + b"|CHECKSUM:" + checksum)
    except (OSError, IOError):
        pass

def bootstrap_progress_and_scan(progress_path: str, backup_path: str) -> Dict[str, Any]:
    target = progress_path if os.path.exists(progress_path) else (backup_path if os.path.exists(backup_path) else None)
    if not target:
        return {}
    try:
        with open(target, "rb") as f:
            payload = f.read()
        if b"|CHECKSUM:" not in payload:
            raise ValueError()
        data_part, cs_part = payload.rsplit(b"|CHECKSUM:", 1)
        if hashlib.sha256(data_part).hexdigest() == cs_part.decode("utf-8").strip():
            return json.loads(data_part.decode("utf-8"))
    except Exception:
        pass
    return {}

def calculate_dynamic_max_workers(requested_threads: int, dest_path: str) -> int:
    cpu_limit = os.cpu_count() or 1
    safe_cpu_max = cpu_limit * 2
    soft_fd = check_fd_limits_and_estimate_peak(requested_threads)
    safe_fd_max = max(1, int((soft_fd - 32) / 2))
    _, is_local = detect_filesystem_type(dest_path)
    storage_max = requested_threads if is_local else 4
    
    final_cap = min(requested_threads, safe_cpu_max, safe_fd_max, storage_max)
    return max(1, final_cap)

def build_job_summary(config: Dict[str, Any], total_scanned: int, skipped: int, copied: int, failed: int, total_bytes: int, acl_warnings: int, start_time: float, errors: List[str]) -> JobSummary:
    return JobSummary(
        source_directory=config["source_directory"],
        destination_directory=config["destination_directory"],
        total_files_scanned=total_scanned,
        files_skipped=skipped,
        files_copied=copied,
        files_failed=failed,
        total_bytes_transferred=total_bytes,
        total_acl_warnings=acl_warnings,
        start_time=start_time,
        end_time=time.perf_counter(),
        elapsed_seconds=round(time.perf_counter() - start_time, 4),
        execution_success=(failed == 0),
        failure_reasons=errors
    )

def write_job_summary(summary_path: str, job_summary: JobSummary) -> None:
    try:
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(asdict(job_summary), f, indent=2)
    except (IOError, OSError):
        pass

def classify_failures_and_emit_exitcode(job_summary: JobSummary) -> int:
    if job_summary.files_failed > 0:
        return 8
    if job_summary.files_copied > 0:
        return 1
    return 0

# --- Production Master Orchestrator ---

def orchestrate_production_sync(config: Dict[str, Any]) -> int:
    start_time_marker = time.perf_counter()
    
    logger.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setLevel(logging.INFO)
    c_handler.setFormatter(log_formatter)
    logger.addHandler(c_handler)

    tuned_workers = calculate_dynamic_max_workers(config["max_workers"], config["destination_directory"])
    
    cache_path = os.path.join(config["destination_directory"], ".sync_progress.json")
    backup_path = cache_path + ".bak"
    summary_path = os.path.join(config["destination_directory"], "sync_run_summary.json")
    
    os.makedirs(config["destination_directory"], exist_ok=True)
    global_cache = bootstrap_progress_and_scan(cache_path, backup_path)
    
    logger.info("Scanning file structural layout arrays...")
    source_assets = scan_source_build_file_list(config["source_directory"])
    workload = classify_files_against_cache(source_assets, global_cache)
    
    skipped_count = len(workload["skip"])
    active_payload = workload["new"] + workload["resume"]
    
    logger.info(f"Scan complete. Total: {len(source_assets)} | Action Tasks: {len(active_payload)} | Bypassed: {skipped_count}")
    
    if not active_payload:
        logger.info("Destination targets fully synchronized with source profiles.")
        return 0

    # Unified Synchronized Queue Core
    master_work_queue = queue.Queue()
    for task in active_payload:
        master_work_queue.put(task)

    # Initialize the Smart Hardware Performance Throttler
    throttler = StorageThrottler(tuned_workers)

    copied_count = 0
    failed_count = 0
    transferred_bytes = 0
    acl_warnings_count = 0
    errors_registry = []
    
    stats_lock = threading.Lock()
    shutdown_telemetry_event = threading.Event()

    def progress_callback_hook(file_uid: str, milestone_key: str, state: bool, current_offset: int):
        _cache_lock.acquire()
        try:
            if file_uid not in global_cache:
                global_cache[file_uid] = init_file_milestone_booleans()
            global_cache[file_uid][milestone_key] = state
            global_cache[file_uid]["last_milestone_offset"] = current_offset
            write_progress_tmp_atomic(cache_path, global_cache, config["atomic_fs"])
        finally:
            _cache_lock.release()

    # --- TELEMETRY OBSERVER THREAD (ANSI IN-PLACE INTEGRATION) ---
    def telemetry_dashboard_observer(poll_interval=1.0):
        import sys
        last_time = time.perf_counter()
        last_bytes = 0

        # Fixed line boundaries: 3 header + workers + 1 footer line
        total_render_lines = 4 + tuned_workers

        try:
            sys.stdout.write("\n" * total_render_lines)
            sys.stdout.flush()
        except Exception:
            pass

        def _clear_and_print_line(s: str):
            sys.stdout.write("\x1b[2K" + s + "\n")

        while not shutdown_telemetry_event.wait(poll_interval):
            current_time = time.perf_counter()

            with _telemetry_lock:
                current_bytes = _global_bytes_transferred
                current_registry = dict(_thread_telemetry_registry)

            elapsed_interval = current_time - last_time
            if elapsed_interval <= 0:
                continue

            bytes_interval = current_bytes - last_bytes
            speed_mbs = (bytes_interval / (1024 * 1024)) / elapsed_interval

            try:
                active_ceiling = throttler.get_ceiling()
            except Exception:
                active_ceiling = tuned_workers

            last_time = current_time
            last_bytes = current_bytes

            total_elapsed_str = time.strftime("%H:%M:%S", time.gmtime(current_time - start_time_marker))

            _log_lock.acquire()
            try:
                sys.stdout.write(f"\x1b[{total_render_lines}A")

                _clear_and_print_line("=" * 80)
                _clear_and_print_line(f"[PROGRESS MONITOR] Run Duration: {total_elapsed_str} | Active Pool Ceiling: {active_ceiling}/{tuned_workers} threads | Speed: {speed_mbs:.2f} MB/s")
                _clear_and_print_line("-" * 80)

                for wid in range(tuned_workers):
                    if wid >= active_ceiling:
                        _clear_and_print_line(f" -> Thread {wid}: [THROTTLED] Paused by health engine rules.")
                        continue

                    reg = current_registry.get(wid)
                    if reg and reg.get("path"):
                        total = reg.get("total_size", 0) or 0
                        offset = reg.get("offset", 0) or 0
                        pct = (offset / total * 100) if total > 0 else 0.0
                        
                        raw_path = reg["path"]
                        display_path = raw_path if len(raw_path) <= 45 else f".../{Path(raw_path).name[-42:]}"
                        
                        _clear_and_print_line(f" -> Thread {wid}: [{pct:6.2f}%] {display_path:<45} ({offset}/{total} bytes)")
                    else:
                        _clear_and_print_line(f" -> Thread {wid}: [IDLE] Checking path lock allocations...")

                _clear_and_print_line("=" * 80)
                sys.stdout.flush()
            finally:
                _log_lock.release()

        # Post-run cleanup spacing
        _log_lock.acquire()
        try:
            sys.stdout.write("\n")
            sys.stdout.flush()
        finally:
            _log_lock.release()

    for idx in range(tuned_workers):
        _thread_telemetry_registry[idx] = {"path": "", "offset": 0, "total_size": 0}

    telemetry_thread = threading.Thread(target=telemetry_dashboard_observer, daemon=True)
    telemetry_thread.start()

    # Worker Consumer Execution Core
    def concurrent_worker_consumer(worker_id: int):
        nonlocal copied_count, failed_count, transferred_bytes, acl_warnings_count
        deferred_buffer = []
        
        while True:
            if worker_id >= throttler.get_ceiling():
                with _telemetry_lock:
                    _thread_telemetry_registry[worker_id]["path"] = ""
                time.sleep(0.25)
                continue

            try:
                task_item = master_work_queue.get_nowait()
            except queue.Empty:
                if deferred_buffer:
                    for item in deferred_buffer:
                        master_work_queue.put(item)
                    deferred_buffer.clear()
                    time.sleep(0.2)
                    continue
                break
                
            relative_path_str = task_item["relative_path"]
            parts = Path(relative_path_str).parts
            root_dir = parts[0] if parts else ""

            _registry_lock.acquire()
            if root_dir in _active_directory_registry:
                _registry_lock.release()
                deferred_buffer.append(task_item)
                continue
            else:
                _active_directory_registry.add(root_dir)
                _registry_lock.release()
                
            src_file = task_item["absolute_path"]
            dest_file = os.path.join(config["destination_directory"], relative_path_str)
            dest_part = dest_file + ".part"
            f_uid = task_item["unique_id"]
            f_size = task_item["size"]
            
            with _telemetry_lock:
                _thread_telemetry_registry[worker_id]["path"] = relative_path_str
                _thread_telemetry_registry[worker_id]["total_size"] = f_size
                _thread_telemetry_registry[worker_id]["offset"] = 0

            try:
                if f_size == 0:
                    validate_and_maybe_create_parent_dirs(dest_file)
                    with open(dest_file, "wb"): pass
                    progress_callback_hook(f_uid, "p100", True, 0)
                    with stats_lock: 
                        copied_count += 1
                    with _telemetry_lock:
                        _thread_telemetry_registry[worker_id]["offset"] = 0
                    throttler.handle_success()
                    continue

                is_resume = f_uid in global_cache and not global_cache[f_uid].get("p100", False)
                start_byte = global_cache[f_uid].get("last_milestone_offset", 0) if is_resume else 0
                
                if is_resume:
                    res_ok, _, _ = validate_resume_dest_part(dest_part, start_byte)
                    if not res_ok:
                        start_byte = 0
                        is_resume = False

                with _telemetry_lock:
                    _thread_telemetry_registry[worker_id]["offset"] = start_byte

                src_stream, dest_stream = open_source_and_dest_part_buffered(src_file, dest_part, is_resume)
                result = stream_data_incremental_buffered(
                    src_stream, dest_stream, f_size, start_byte, 
                    config["buffer_size"], progress_callback_hook, f_uid, worker_id
                )
                src_stream.close()
                dest_stream.close()

                if result.exception is not None:
                    raise result.exception

                _, swap_capable = detect_filesystem_type(dest_part)
                if not atomic_rename_part_to_final(dest_part, dest_file, swap_capable & config["atomic_fs"]):
                    raise IOError("Target execution block allocation rename rejected by kernel.")

                _cache_lock.acquire()
                try:
                    global_cache[f_uid]["p100"] = True
                    write_progress_tmp_atomic(cache_path, global_cache, config["atomic_fs"])
                finally:
                    _cache_lock.release()

                with stats_lock:
                    copied_count += 1
                    transferred_bytes += result.bytes_transferred
                
                throttler.handle_success()

            except Exception as file_fault:
                errno_val = getattr(file_fault, "errno", 0)
                
                _global_suspend_event.clear()
                cool_off_period = throttler.handle_failure(f_size, errno_val)
                
                err_text = f"Fault encountered on thread {worker_id} for path '{relative_path_str}': {str(file_fault)}"
                with stats_lock:
                    failed_count += 1
                    errors_registry.append(err_text)
                
                append_failed_log(os.path.join(config["destination_directory"], "failed_transfers.log"), src_file, type(file_fault).__name__, str(file_fault))
                
                master_work_queue.put(task_item)
                
                time.sleep(cool_off_period)
                _global_suspend_event.set()

            finally:
                with _telemetry_lock:
                    _thread_telemetry_registry[worker_id]["path"] = ""
                _registry_lock.acquire()
                _active_directory_registry.discard(root_dir)
                _registry_lock.release()

    threads_pool = []
    for i in range(tuned_workers):
        t = threading.Thread(target=concurrent_worker_consumer, args=(i,), daemon=True)
        threads_pool.append(t)
        t.start()

    for t in threads_pool:
        t.join()

    shutdown_telemetry_event.set()
    telemetry_thread.join()

    try:
        if os.path.exists(cache_path):
            import shutil
            shutil.copy2(cache_path, backup_path)
    except IOError:
        pass

    job_summary = build_job_summary(
        config, len(source_assets), skipped_count, copied_count, 
        failed_count, transferred_bytes, acl_warnings_count, 
        start_time_marker, errors_registry
    )
    
    write_job_summary(summary_path, job_summary)
    return classify_failures_and_emit_exitcode(job_summary)

def parse_cli_arguments():
    parser = argparse.ArgumentParser(description="ANSI Dashboard Engine", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("source", type=str)
    parser.add_argument("destination", type=str)
    parser.add_argument("--MT", type=int, default=1, dest="max_workers")
    parser.add_argument("--BUFFER", type=int, default=2*1024*1024, dest="buffer_size")
    parser.add_argument("--NOATOMIC", action="store_false", dest="atomic_fs")
    
    processed_args = []
    for arg in sys.argv[1:]:
        u = arg.upper()
        if u.startswith("/MT") or u.startswith("/BUFFER") or u.startswith("/NOATOMIC"):
            processed_args.append(arg.replace("/", "--", 1))
        else:
            processed_args.append(arg)
    return parser.parse_args(processed_args)

if __name__ == "__main__":
    args = parse_cli_arguments()
    job_config = {
        "source_directory": os.path.abspath(args.source),
        "destination_directory": os.path.abspath(args.destination),
        "max_workers": args.max_workers,
        "buffer_size": args.buffer_size,
        "atomic_fs": args.atomic_fs
    }
    exit_status = orchestrate_production_sync(job_config)
    logger.info(f"Shutting down engine context block. Exit Code: {exit_status}")
    logging.shutdown()
    sys.exit(exit_status)