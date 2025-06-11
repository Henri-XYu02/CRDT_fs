import argparse
import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from .benchmark import FilesystemBenchmark
from .partition_test import PartitionTest
from .network_sim import NetworkSimulator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_benchmarks(mount_points: List[str], filesystem_types: List[str], 
                  git_repo_path: str = None) -> Dict[str, Any]:
    """Run filesystem benchmarks on all mount points."""
    results = {}
    
    for mount_point, fs_type in zip(mount_points, filesystem_types):
        logger.info(f"Running benchmarks for {fs_type} at {mount_point}")
        benchmark = FilesystemBenchmark(mount_point, fs_type)
        
        # Run single file I/O tests
        benchmark.single_file_io_test(file_size_mb=100)
        benchmark.single_file_io_test(file_size_mb=1000)
        
        # Run filesystem operations test
        benchmark.filesystem_operations_test(num_files=1000)
        
        # Run git history replay if repo path provided
        if git_repo_path:
            benchmark.git_history_replay(git_repo_path)
        
        results[fs_type] = benchmark.get_results()
    
    return results

def run_network_tests(mount_points: List[str], network_interfaces: List[str],
                     filesystem_types: List[str]) -> Dict[str, Any]:
    """Run network-related tests."""
    results = {}
    
    # Test different network conditions
    network_conditions = [
        (5, 1000),    # Low latency, high bandwidth
        (100, 100),   # High latency, low bandwidth
        (50, 500),    # Medium latency, medium bandwidth
    ]
    
    for fs_type in filesystem_types:
        results[fs_type] = {}
        
        for latency_ms, bandwidth_mbps in network_conditions:
            logger.info(f"Testing {fs_type} with {latency_ms}ms latency, "
                       f"{bandwidth_mbps}Mbps bandwidth")
            
            # Set network conditions
            with NetworkSimulator(network_interfaces[0]) as net_sim:
                net_sim.set_network_conditions(latency_ms, bandwidth_mbps)
                
                # Run benchmarks under these conditions
                benchmark = FilesystemBenchmark(mount_points[0], fs_type)
                benchmark.single_file_io_test(file_size_mb=100)
                benchmark.filesystem_operations_test(num_files=100)
                
                results[fs_type][f"latency_{latency_ms}ms_bw_{bandwidth_mbps}mbps"] = \
                    benchmark.get_results()
    
    # Run partition tests
    partition_test = PartitionTest(mount_points, network_interfaces)
    partition_test.test_partition_recovery()
    partition_test.test_concurrent_operations()
    
    results["partition_tests"] = partition_test.get_results()
    
    return results

def save_results(results: Dict[str, Any], output_dir: str):
    """Save test results to JSON files."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save overall results
    with open(output_path / f"evaluation_results_{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2)
    
    # Save individual test results
    for test_type, test_results in results.items():
        with open(output_path / f"{test_type}_results_{timestamp}.json", "w") as f:
            json.dump(test_results, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Run CRDT_FS evaluation tests")
    parser.add_argument("--mount-points", nargs="+", required=True,
                       help="List of mount points to test")
    parser.add_argument("--filesystem-types", nargs="+", required=True,
                       help="List of filesystem types being tested")
    parser.add_argument("--network-interfaces", nargs="+", required=True,
                       help="List of network interfaces to use for testing")
    parser.add_argument("--git-repo", help="Path to git repository for history replay")
    parser.add_argument("--output-dir", default="evaluation_results",
                       help="Directory to save test results")
    
    args = parser.parse_args()
    
    # Run benchmarks
    benchmark_results = run_benchmarks(
        args.mount_points,
        args.filesystem_types,
        args.git_repo
    )
    
    # Run network tests
    network_results = run_network_tests(
        args.mount_points,
        args.network_interfaces,
        args.filesystem_types
    )
    
    # Combine results
    all_results = {
        "benchmarks": benchmark_results,
        "network_tests": network_results,
        "test_configuration": {
            "mount_points": args.mount_points,
            "filesystem_types": args.filesystem_types,
            "network_interfaces": args.network_interfaces,
            "git_repo": args.git_repo
        }
    }
    
    # Save results
    save_results(all_results, args.output_dir)
    logger.info(f"Evaluation completed. Results saved to {args.output_dir}")

if __name__ == "__main__":
    main() 