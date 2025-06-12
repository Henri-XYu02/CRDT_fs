import os
import time
import psutil
import asyncio
import random
import string
import statistics
from pathlib import Path
from typing import List, Tuple
import pyfuse3
from serde import serde
from serde.json import from_json
from filesystem.fuse_binding import FuseOps
from filesystem.inode_store import LWWInodeStore
from merkle_crdt.merkle_ktree import MerkleKTree

# Define Config class to match the config.json structure
@serde
class Config:
    replica: int
    peers: list[str]
    basepath: str
    mountpoint: str
    host: str
    port: int
    
class FilesystemEvaluator:
    def __init__(self, mount_point: str):
        self.mount_point = Path(mount_point)
        self.process = psutil.Process()
        self.results = {}

    def measure_resource_usage(self) -> Tuple[float, float]:
        """Measure CPU and memory usage"""
        cpu_percent = self.process.cpu_percent(interval=1)
        memory_info = self.process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
        return cpu_percent, memory_mb

    async def measure_io_speed(self, file_size_mb: int = 1) -> float:
        """Measure raw I/O speed for a single file"""
        test_file = self.mount_point / "io_test_file"
        data = os.urandom(file_size_mb * 1024 * 1024)
        
        # Write test
        start_time = time.time()
        with open(test_file, 'wb') as f:
            f.write(data)
        write_time = time.time() - start_time
        
        # Read test
        start_time = time.time()
        with open(test_file, 'rb') as f:
            read_data = f.read()
        read_time = time.time() - start_time
        
        # Cleanup
        os.remove(test_file)
        
        write_speed = file_size_mb / write_time  # MB/s
        read_speed = file_size_mb / read_time    # MB/s
        return (write_speed + read_speed) / 2

    async def measure_fs_operations(self, num_operations: int = 1000) -> float:
        """Measure filesystem operations per second"""
        operations = []
        
        # Create test files
        for i in range(num_operations):
            filename = f"test_file_{i}.txt"
            filepath = self.mount_point / filename
            start_time = time.time()
            with open(filepath, 'w') as f:
                f.write("test")
            operations.append(time.time() - start_time)
        
        # Delete test files
        for i in range(num_operations):
            filename = f"test_file_{i}.txt"
            filepath = self.mount_point / filename
            start_time = time.time()
            os.remove(filepath)
            operations.append(time.time() - start_time)
        
        return num_operations * 2 / sum(operations)  # Operations per second

    async def measure_directory_operations(self, num_operations: int = 100) -> float:
        """Measure directory operations (create, move, delete) per second"""
        operations = []
        
        # Create directories
        for i in range(num_operations):
            dirname = f"test_dir_{i}"
            dirpath = self.mount_point / dirname
            start_time = time.time()
            os.makedirs(dirpath)
            operations.append(time.time() - start_time)
        
        # Move directories
        for i in range(num_operations):
            old_path = self.mount_point / f"test_dir_{i}"
            new_path = self.mount_point / f"moved_dir_{i}"
            start_time = time.time()
            os.rename(old_path, new_path)
            operations.append(time.time() - start_time)
        
        # Delete directories
        for i in range(num_operations):
            dirpath = self.mount_point / f"moved_dir_{i}"
            start_time = time.time()
            os.rmdir(dirpath)
            operations.append(time.time() - start_time)
        
        return num_operations * 3 / sum(operations)  # Operations per second

    async def run_evaluation(self):
        """Run all evaluations and collect results"""
        print("Starting filesystem evaluation...")
        
        # Resource Usage
        print("Measuring resource usage...")
        cpu_usage, memory_usage = self.measure_resource_usage()
        self.results['resource_usage'] = {
            'cpu_percent': cpu_usage,
            'memory_mb': memory_usage
        }
        
        # I/O Speed
        print("Measuring I/O speed...")
        io_speed = await self.measure_io_speed()
        self.results['io_speed'] = {
            'mb_per_second': io_speed
        }
        
        # Filesystem Operations
        print("Measuring filesystem operations...")
        fs_ops = await self.measure_fs_operations()
        self.results['fs_operations'] = {
            'ops_per_second': fs_ops
        }
        
        # Directory Operations
        print("Measuring directory operations...")
        dir_ops = await self.measure_directory_operations()
        self.results['directory_operations'] = {
            'ops_per_second': dir_ops
        }
        
        return self.results

async def main():
    # Read config file
    config_path = "./config.json"
    with open(config_path, "r") as f:
        config = from_json(Config, f.read())
    
    print(config.mountpoint)
    evaluator = FilesystemEvaluator(config.mountpoint)
    
    try:
        results = await evaluator.run_evaluation()
        print("\nEvaluation Results:")
        print("------------------")
        print(f"Resource Usage:")
        print(f"  CPU: {results['resource_usage']['cpu_percent']:.2f}%")
        print(f"  Memory: {results['resource_usage']['memory_mb']:.2f} MB")
        print(f"\nI/O Speed: {results['io_speed']['mb_per_second']:.2f} MB/s")
        print(f"Filesystem Operations: {results['fs_operations']['ops_per_second']:.2f} ops/s")
        print(f"Directory Operations: {results['directory_operations']['ops_per_second']:.2f} ops/s")
    except Exception as e:
        print(f"Error during evaluation: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 