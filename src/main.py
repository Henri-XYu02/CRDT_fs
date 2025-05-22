"""
Main entry point for the distributed filesystem.
"""
import os
import sys
import asyncio
import argparse
import logging
import uuid
from typing import List

from .filesystem import FSOperations, mount
from .networking import NetworkManager

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Start a peer-to-peer distributed filesystem node"
    )
    parser.add_argument(
        "mountpoint",
        help="Directory to mount the filesystem at"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to listen on (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to listen on (default: 8000)"
    )
    parser.add_argument(
        "--peers",
        nargs="*",
        default=[],
        help="List of peers to connect to in the format host:port"
    )
    parser.add_argument(
        "--replica-id",
        default=None,
        help="Unique identifier for this replica (default: randomly generated)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)"
    )
    return parser.parse_args()

async def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    # Generate replica ID if not provided
    replica_id = args.replica_id or str(uuid.uuid4())
    logger.info(f"Starting replica {replica_id}")
    
    # Create filesystem operations
    fs = FSOperations(replica_id)
    
    # Create and start network manager
    network = NetworkManager(fs, replica_id, args.host, args.port)
    await network.start()
    logger.info(f"Listening on {args.host}:{args.port}")
    
    # Connect to peers
    for peer in args.peers:
        try:
            host, port = peer.split(":")
            peer_id = f"{host}:{port}"
            if await network.add_peer(peer_id, host, int(port)):
                logger.info(f"Connected to peer {peer}")
            else:
                logger.warning(f"Failed to connect to peer {peer}")
        except Exception as e:
            logger.error(f"Error connecting to peer {peer}: {e}")
    
    # Mount filesystem
    logger.info(f"Mounting filesystem at {args.mountpoint}")
    mount(args.mountpoint, replica_id)
    
    # Wait for interrupt
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await network.stop()

if __name__ == "__main__":
    asyncio.run(main()) 