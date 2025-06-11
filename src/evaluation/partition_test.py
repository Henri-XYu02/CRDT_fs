import time
import logging
from pathlib import Path
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import threading
from .network_sim import NetworkSimulator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PartitionTest:
    def __init__(self, mount_points: List[str], network_interfaces: List[str]):
        self.mount_points = [Path(mp) for mp in mount_points]
        self.network_sims = [NetworkSimulator(iface) for iface in network_interfaces]
        self.results: Dict[str, Any] = {}

    def _create_test_structure(self, node_idx: int):
        """Create a test directory structure on a specific node."""
        base_dir = self.mount_points[node_idx] / f"test_node_{node_idx}"
        base_dir.mkdir(exist_ok=True)
        
        # Create some test files
        for i in range(10):
            (base_dir / f"file_{i}").write_text(f"test content {i}")
        
        # Create nested directories
        nested_dir = base_dir / "nested"
        nested_dir.mkdir(exist_ok=True)
        for i in range(5):
            (nested_dir / f"nested_file_{i}").write_text(f"nested content {i}")

    def _perform_operations_during_partition(self, node_idx: int, duration: float):
        """Perform filesystem operations while a node is partitioned."""
        base_dir = self.mount_points[node_idx] / f"test_node_{node_idx}"
        
        def operation_loop():
            start_time = time.time()
            while time.time() - start_time < duration:
                # Create new files
                new_file = base_dir / f"partition_file_{int(time.time())}"
                new_file.write_text("created during partition")
                
                # Modify existing files
                for i in range(10):
                    file_path = base_dir / f"file_{i}"
                    if file_path.exists():
                        file_path.write_text(f"modified during partition {i}")
                
                # Move files
                nested_dir = base_dir / "nested"
                if nested_dir.exists():
                    for i in range(5):
                        src = nested_dir / f"nested_file_{i}"
                        dst = base_dir / f"moved_file_{i}"
                        if src.exists():
                            src.rename(dst)
                
                time.sleep(0.1)  # Small delay between operations
        
        return operation_loop

    def test_partition_recovery(self, partition_duration: float = 30.0):
        """Test how long it takes for nodes to recover after a partition."""
        # Initialize test structure on all nodes
        for i in range(len(self.mount_points)):
            self._create_test_structure(i)
        
        # Start operations on node 0
        operation_thread = threading.Thread(
            target=self._perform_operations_during_partition(0, partition_duration)
        )
        operation_thread.start()
        
        # Simulate partition for all other nodes
        for sim in self.network_sims[1:]:
            sim.simulate_network_partition(partition_duration)
        
        # Wait for operations to complete
        operation_thread.join()
        
        # Measure recovery time
        recovery_times = []
        for i in range(1, len(self.mount_points)):
            start_time = time.time()
            while True:
                # Check if all files created during partition are visible
                base_dir = self.mount_points[i] / "test_node_0"
                if all((base_dir / f"partition_file_{j}").exists() 
                      for j in range(int(partition_duration * 10))):
                    break
                time.sleep(0.1)
            recovery_time = time.time() - start_time
            recovery_times.append(recovery_time)
        
        self.results["partition_recovery"] = {
            "partition_duration": partition_duration,
            "recovery_times": recovery_times,
            "average_recovery_time": sum(recovery_times) / len(recovery_times)
        }
        
        logger.info(f"Partition recovery test completed. Average recovery time: "
                   f"{self.results['partition_recovery']['average_recovery_time']:.2f}s")

    def test_concurrent_operations(self, duration: float = 30.0):
        """Test concurrent operations during partition."""
        # Initialize test structure
        for i in range(len(self.mount_points)):
            self._create_test_structure(i)
        
        # Create conflicting operations
        def node0_operations():
            base_dir = self.mount_points[0]
            dir_a = base_dir / "dir_a"
            dir_b = base_dir / "dir_b"
            
            dir_a.mkdir(exist_ok=True)
            dir_b.mkdir(exist_ok=True)
            
            # Move dir_a into dir_b
            (dir_a / "test_file").write_text("test")
            dir_a.rename(dir_b / "dir_a")
        
        def node1_operations():
            base_dir = self.mount_points[1]
            dir_a = base_dir / "dir_a"
            dir_b = base_dir / "dir_b"
            
            dir_a.mkdir(exist_ok=True)
            dir_b.mkdir(exist_ok=True)
            
            # Move dir_b into dir_a
            (dir_b / "test_file").write_text("test")
            dir_b.rename(dir_a / "dir_b")
        
        # Start operations on both nodes
        with ThreadPoolExecutor(max_workers=2) as executor:
            future0 = executor.submit(node0_operations)
            future1 = executor.submit(node1_operations)
            
            # Simulate partition
            for sim in self.network_sims:
                sim.simulate_network_partition(duration)
            
            # Wait for operations to complete
            future0.result()
            future1.result()
        
        # Check final state
        final_states = []
        for i in range(len(self.mount_points)):
            base_dir = self.mount_points[i]
            state = {
                "dir_a_exists": (base_dir / "dir_a").exists(),
                "dir_b_exists": (base_dir / "dir_b").exists(),
                "dir_a_in_b": (base_dir / "dir_b" / "dir_a").exists(),
                "dir_b_in_a": (base_dir / "dir_a" / "dir_b").exists()
            }
            final_states.append(state)
        
        self.results["concurrent_operations"] = {
            "duration": duration,
            "final_states": final_states
        }
        
        logger.info("Concurrent operations test completed")
        for i, state in enumerate(final_states):
            logger.info(f"Node {i} final state: {state}")

    def get_results(self) -> Dict[str, Any]:
        """Get all test results."""
        return self.results 