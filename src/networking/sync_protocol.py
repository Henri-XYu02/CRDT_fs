"""
Implementation of the synchronization protocol for CRDT operations.
This module handles the propagation of changes between replicas using Merkle trees
to efficiently detect and resolve differences.
"""
from typing import Dict, List, Optional, Set
import asyncio
import logging
from ..merkle_crdt import MerkleCRDT
from ..filesystem import FSOperations
from .peer import Peer

logger = logging.getLogger(__name__)

class SyncProtocol:
    """
    Protocol for synchronizing CRDT operations between replicas.
    Uses Merkle trees to efficiently detect differences and ensure causal delivery.
    """
    def __init__(self, fs: FSOperations, replica_id: str):
        self.fs = fs
        self.replica_id = replica_id
        self.peers: Dict[str, Peer] = {}  # peer_id -> Peer
        self.sync_interval = 5.0  # seconds
        self._sync_task: Optional[asyncio.Task] = None

    async def add_peer(self, peer_id: str, host: str, port: int) -> bool:
        """
        Add a new peer to sync with.
        
        Args:
            peer_id: Unique identifier for the peer
            host: Peer's hostname
            port: Peer's port number
            
        Returns:
            True if peer was added successfully
        """
        if peer_id in self.peers:
            return True
            
        peer = Peer(host, port)
        if await peer.connect():
            self.peers[peer_id] = peer
            return True
            
        return False

    async def remove_peer(self, peer_id: str) -> None:
        """
        Remove a peer.
        
        Args:
            peer_id: ID of the peer to remove
        """
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            await peer.disconnect()
            del self.peers[peer_id]

    async def start(self) -> None:
        """Start periodic synchronization with peers."""
        if self._sync_task is None:
            self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop(self) -> None:
        """Stop synchronization with peers."""
        if self._sync_task is not None:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None

    async def _sync_loop(self) -> None:
        """Main synchronization loop."""
        while True:
            try:
                await self._sync_with_all_peers()
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
            await asyncio.sleep(self.sync_interval)

    async def _sync_with_all_peers(self) -> None:
        """Synchronize with all connected peers."""
        for peer_id, peer in list(self.peers.items()):
            try:
                await self._sync_with_peer(peer)
            except Exception as e:
                logger.error(f"Error syncing with peer {peer_id}: {e}")
                # Remove peer if we can't connect
                if not peer.connected:
                    await self.remove_peer(peer_id)

    async def _sync_with_peer(self, peer: Peer) -> None:
        """
        Synchronize with a specific peer.
        
        Args:
            peer: The peer to sync with
        """
        # Get peer's Merkle root
        peer_root = await peer.get_merkle_root()
        if not peer_root:
            return

        # Get our Merkle root
        our_root = self.fs.tree.crdt.merkle_tree.get_root_hash()
        
        # If roots are same, we're in sync
        if peer_root == our_root:
            return
            
        # Get differences between our trees
        diff = await peer.get_merkle_diff(our_root)
        if not diff:
            return
            
        # Get missing operations from peer
        missing_ops = diff.get("missing_ops", [])
        if missing_ops:
            ops = await peer.get_operations(missing_ops)
            if ops:
                await self._apply_operations(ops)

    async def _apply_operations(self, ops: Dict[str, dict]) -> None:
        """
        Apply operations received from a peer.
        
        Args:
            ops: Dict mapping operation hashes to operation data
        """
        # Sort operations by timestamp to maintain rough causal order
        sorted_ops = sorted(
            ops.items(),
            key=lambda x: x[1].get("timestamp", 0)
        )
        
        for op_hash, op_data in sorted_ops:
            # Skip operations we already have
            if op_hash in self.fs.tree.crdt.operations:
                continue
                
            # Create operation in our CRDT
            self.fs.tree.crdt.add_operation(
                op_type=op_data["operation_type"],
                path=op_data["path"],
                payload=op_data["payload"],
                dependencies=set(op_data.get("dependencies", []))
            )
            
            # If this is a file content operation, fetch the chunks
            if op_data["operation_type"] == "write":
                await self._sync_file_content(op_data)

    async def _sync_file_content(self, op_data: dict) -> None:
        """
        Sync file content for a write operation.
        
        Args:
            op_data: The write operation data
        """
        data_hash = op_data["payload"].get("data_hash")
        if not data_hash or self.fs.data.has_chunk(data_hash):
            return
            
        # Try to get the chunk from each peer until successful
        for peer in self.peers.values():
            chunk = await peer.get_chunk(data_hash)
            if chunk:
                self.fs.data.chunks[data_hash] = chunk
                break 