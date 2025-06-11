import subprocess
import logging
from typing import Optional, Tuple
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NetworkSimulator:
    def __init__(self, interface: str):
        self.interface = interface
        self._current_config: Optional[Tuple[float, float]] = None

    def set_network_conditions(self, latency_ms: float, bandwidth_mbps: float):
        """Set network conditions using tc (traffic control)."""
        # Remove existing qdisc
        subprocess.run(["tc", "qdisc", "del", "dev", self.interface, "root"], 
                      stderr=subprocess.DEVNULL)
        
        # Add new qdisc with specified conditions
        cmd = [
            "tc", "qdisc", "add", "dev", self.interface, "root",
            "netem", "delay", f"{latency_ms}ms",
            "rate", f"{bandwidth_mbps}mbit"
        ]
        
        try:
            subprocess.run(cmd, check=True)
            self._current_config = (latency_ms, bandwidth_mbps)
            logger.info(f"Set network conditions: {latency_ms}ms latency, {bandwidth_mbps}Mbps bandwidth")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to set network conditions: {e}")
            raise

    def reset_network_conditions(self):
        """Reset network conditions to normal."""
        try:
            subprocess.run(["tc", "qdisc", "del", "dev", self.interface, "root"],
                         stderr=subprocess.DEVNULL)
            self._current_config = None
            logger.info("Reset network conditions to normal")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to reset network conditions: {e}")
            raise

    def get_current_config(self) -> Optional[Tuple[float, float]]:
        """Get current network configuration."""
        return self._current_config

    def simulate_network_partition(self, duration_seconds: float):
        """Simulate a network partition by blocking all traffic."""
        try:
            # Block all traffic
            subprocess.run(["tc", "qdisc", "add", "dev", self.interface, "root",
                          "netem", "loss", "100%"])
            logger.info(f"Network partition started for {duration_seconds} seconds")
            
            time.sleep(duration_seconds)
            
            # Restore normal conditions
            self.reset_network_conditions()
            logger.info("Network partition ended")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to simulate network partition: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset_network_conditions() 