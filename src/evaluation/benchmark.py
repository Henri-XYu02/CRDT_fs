import time
import os
import shutil
import statistics
from typing import List, Dict, Any, Callable
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilesystemBenchmark:
    def __init__(self, mount_point: str, filesystem_type: str):
        self.mount_point = Path(mount_point)
        self.filesystem_type = filesystem_type
        self.results: Dict[str, List[float]] = {}

    def measure_operation(self, operation: Callable, name: str, iterations: int = 100) -> float:
        """Measure the time taken for a filesystem operation."""
        times = []
        for _ in range(iterations):
            start_time = time.time()
            operation()
            end_time = time.time()
            times.append(end_time - start_time)
        
        avg_time = statistics.mean(times)
        self.results[name] = times
        return avg_time

    def single_file_io_test(self, file_size_mb: int = 100):
        """Test read/write performance on a single file."""
        test_file = self.mount_point / "test_file"
        
        # Write test
        def write_test():
            with open(test_file, 'wb') as f:
                f.write(os.urandom(file_size_mb * 1024 * 1024))
        
        # Read test
        def read_test():
            with open(test_file, 'rb') as f:
                f.read()
        
        write_time = self.measure_operation(write_test, f"write_{file_size_mb}mb")
        read_time = self.measure_operation(read_test, f"read_{file_size_mb}mb")
        
        logger.info(f"{self.filesystem_type} - Write {file_size_mb}MB: {write_time:.2f}s")
        logger.info(f"{self.filesystem_type} - Read {file_size_mb}MB: {read_time:.2f}s")

    def filesystem_operations_test(self, num_files: int = 1000):
        """Test filesystem operations (create, move, delete)."""
        test_dir = self.mount_point / "test_dir"
        test_dir.mkdir(exist_ok=True)
        
        # Create files
        def create_files():
            for i in range(num_files):
                (test_dir / f"file_{i}").touch()
        
        # Move files
        def move_files():
            for i in range(num_files):
                src = test_dir / f"file_{i}"
                dst = test_dir / f"moved_file_{i}"
                src.rename(dst)
        
        # Delete files
        def delete_files():
            for i in range(num_files):
                (test_dir / f"moved_file_{i}").unlink()
        
        create_time = self.measure_operation(create_files, "create_files")
        move_time = self.measure_operation(move_files, "move_files")
        delete_time = self.measure_operation(delete_files, "delete_files")
        
        logger.info(f"{self.filesystem_type} - Create {num_files} files: {create_time:.2f}s")
        logger.info(f"{self.filesystem_type} - Move {num_files} files: {move_time:.2f}s")
        logger.info(f"{self.filesystem_type} - Delete {num_files} files: {delete_time:.2f}s")

    def git_history_replay(self, repo_path: str):
        """Replay git history and measure filesystem performance."""
        repo_path = Path(repo_path)
        if not (repo_path / ".git").exists():
            raise ValueError("Not a git repository")
        
        def replay_commit(commit_hash: str):
            subprocess.run(["git", "checkout", commit_hash], cwd=repo_path)
        
        # Get all commit hashes
        result = subprocess.run(
            ["git", "log", "--pretty=format:%H"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        commits = result.stdout.splitlines()
        
        total_time = 0
        for commit in commits:
            start_time = time.time()
            replay_commit(commit)
            end_time = time.time()
            total_time += (end_time - start_time)
        
        logger.info(f"{self.filesystem_type} - Git history replay: {total_time:.2f}s")

    def get_results(self) -> Dict[str, Any]:
        """Get benchmark results with statistics."""
        return {
            "filesystem_type": self.filesystem_type,
            "operations": {
                name: {
                    "mean": statistics.mean(times),
                    "median": statistics.median(times),
                    "stddev": statistics.stdev(times) if len(times) > 1 else 0,
                    "min": min(times),
                    "max": max(times)
                }
                for name, times in self.results.items()
            }
        } 