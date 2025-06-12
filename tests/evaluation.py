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
from serde.json import from_json, to_json
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

    async def test_networked_volumes(self, duration_seconds: int = 180):
        """Test networked volumes with continuous file operations"""
        import csv
        import subprocess
        from datetime import datetime
        import matplotlib.pyplot as plt
        import pandas as pd
        
        # Create CSV file for logging
        csv_file = "networked_volume_test.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'operation_type', 'file_name', 'data_written', 'peer_count', 'cumulative_files', 'cumulative_data'])
        
        
        for peer_count in range(4, 5):
            print(f"Starting test with {peer_count} peer{'s' if peer_count > 1 else ''}...")
            # Start 4 volumes (1 primary + 3 peers)
            volumes = []
            base_ports = [8000 + i for i in range(peer_count)]
            base_mounts = [f"/tmp/mount{i}" for i in range(peer_count)]
            base_paths = [f"/tmp/basepath{i}" for i in range(peer_count)]
            
            for mount in base_mounts:
                if os.path.exists(mount):
                    # clean up dir first
                    os.system(f"rm -rf {mount}")
                os.makedirs(mount, exist_ok=True)
            
            for base_path in base_paths:
                if os.path.exists(base_path):
                    # clean up dir first
                    os.system(f"rm -rf {base_path}")
                os.makedirs(base_path, exist_ok=True)
            
            # Create config files and start volumes
            for i in range(peer_count):
                config = {
                    "replica": i + 1,
                    "peers": [f"localhost:{p}" for p in base_ports if p != base_ports[i]],
                    "basepath": base_paths[i],
                    "mountpoint": base_mounts[i],
                    "host": "localhost",
                    "port": base_ports[i]
                }
                
                # Create config file
                config_path = f"config{i}.json"
                with open(config_path, 'w') as f:
                    f.write(to_json(config))
                
                # Start volume process
                process = subprocess.Popen(['python', 'src/main.py', config_path])
                volumes.append(process)
                
                # Wait for volume to mount
                time.sleep(10)
            
            try:
                # Test each operation type separately
                for operation in ['create', 'create_with_data', 'append_data']:
                    print(f"\nTesting {operation} operations for {duration_seconds} seconds...")
                    start_time = time.time()
                    file_counter = 0
                    cumulative_files = 0
                    cumulative_data = 0
                    
                    while time.time() - start_time < duration_seconds:
                        timestamp = time.time() - start_time
                        
                        # Get resource usage
                        cpu_usage, memory_mb = self.measure_resource_usage()
                        
                        # Perform operation and track metrics
                        data_written = 0
                        if operation == 'create':
                            filename = f"test_file_{file_counter}.txt"
                            filepath = Path(base_mounts[0]) / filename
                            with open(filepath, 'w') as f:
                                pass
                            cumulative_files += 1
                        elif operation == 'create_with_data':
                            filename = f"test_file_{file_counter}.txt"
                            filepath = Path(base_mounts[0]) / filename
                            data_written = 4096
                            with open(filepath, 'w') as f:
                                f.write('x' * data_written)
                            cumulative_files += 1
                            cumulative_data += data_written
                        else:  # append_data
                            filename = f"test_file_0.txt"
                            filepath = Path(base_mounts[0]) / filename
                            data_written = 4096
                            with open(filepath, 'a') as f:
                                f.write('x' * data_written)
                            cumulative_data += data_written
                        
                        # Log operation with metrics for each peer count
                        with open(csv_file, 'a', newline='') as f:
                            writer = csv.writer(f)
                            writer.writerow([
                                timestamp,
                                operation,
                                filename,
                                data_written,
                                peer_count,
                                cumulative_files,
                                cumulative_data
                            ])
                        
                        file_counter += 1
                        await asyncio.sleep(0.1)  # Small delay between operations
                    
                    print(f"Completed {operation} operations. Created {cumulative_files} files, wrote {cumulative_data} bytes")
                    
            finally:
                # Cleanup
                for process in volumes:
                    process.terminate()
                    process.wait()
                
                # Clean up config files
                for i in range(peer_count):
                    os.remove(f"config{i}.json")
                    # Clean up mount points
                    subprocess.run(['fusermount', '-u', base_mounts[i]], check=False)
        
        # Generate graphs
        self.generate_networked_volume_graphs(csv_file)
        return csv_file

    def generate_networked_volume_graphs(self, csv_file: str):
        """Generate graphs from the networked volume test data"""
        import pandas as pd
        import matplotlib.pyplot as plt
        
        # Read the CSV data
        df = pd.read_csv(csv_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Create figure with 3 subplots
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
        
        # Plot 1: File Count over Time (for create operations)
        create_data = df[df['operation_type'] == 'create']
        for peer_count in range(2, 5):
            peer_data = create_data[create_data['peer_count'] == peer_count]
            ax1.plot(peer_data['timestamp'], peer_data['cumulative_files'], 
                    label=f'{peer_count} peer{"s" if peer_count > 1 else ""}')
        ax1.set_title('Cumulative File Count Over Time (Create Empty Files)')
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Number of Files')
        ax1.legend()
        ax1.grid(True)
        
        # Plot 2: Data Written over Time (for create_with_data operations)
        create_with_data = df[df['operation_type'] == 'create_with_data']
        for peer_count in range(2, 5):
            peer_data = create_with_data[create_with_data['peer_count'] == peer_count]
            ax2.plot(peer_data['timestamp'], peer_data['cumulative_data'], 
                    label=f'{peer_count} peer{"s" if peer_count > 1 else ""}')
        ax2.set_title('Cumulative Data Written (Create with 4KB Data)')
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Data Written (bytes)')
        ax2.legend()
        ax2.grid(True)
        
        # Plot 3: Data Written over Time (for append operations)
        append_data = df[df['operation_type'] == 'append_data']
        for peer_count in range(2, 5):
            peer_data = append_data[append_data['peer_count'] == peer_count]
            ax3.plot(peer_data['timestamp'], peer_data['cumulative_data'], 
                    label=f'{peer_count} peer{"s" if peer_count > 1 else ""}')
        ax3.set_title('Cumulative Data Written (Append 4KB Data)')
        ax3.set_xlabel('Time')
        ax3.set_ylabel('Data Written (bytes)')
        ax3.legend()
        ax3.grid(True)
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig('networked_volume_graphs.png')
        plt.close()

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
        # Run original evaluations
        # results = await evaluator.run_evaluation()
        # print("\nEvaluation Results:")
        # print("------------------")
        # print(f"Resource Usage:")
        # print(f"  CPU: {results['resource_usage']['cpu_percent']:.2f}%")
        # print(f"  Memory: {results['resource_usage']['memory_mb']:.2f} MB")
        # print(f"\nI/O Speed: {results['io_speed']['mb_per_second']:.2f} MB/s")
        # print(f"Filesystem Operations: {results['fs_operations']['ops_per_second']:.2f} ops/s")
        # print(f"Directory Operations: {results['directory_operations']['ops_per_second']:.2f} ops/s")
        
        # Run networked volume test
        print("\nRunning networked volume test...")
        csv_file = await evaluator.test_networked_volumes()
        print(f"Networked volume test completed. Results saved to {csv_file}")
        
    except Exception as e:
        print(f"Error during evaluation: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 