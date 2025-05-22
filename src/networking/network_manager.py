"""
Implementation of the network manager that handles peer-to-peer communication.
"""
from typing import Dict, Any, Optional
import asyncio
from aiohttp import web
import json

from ..filesystem import FSOperations
from .sync_protocol import SyncProtocol

class NetworkManager:
    """
    Manages the network interface for peer-to-peer communication.
    Provides an HTTP server that other peers can connect to.
    """
    def __init__(self, fs: FSOperations, replica_id: str, host: str = "0.0.0.0", port: int = 8000):
        self.fs = fs
        self.replica_id = replica_id
        self.host = host
        self.port = port
        self.sync = SyncProtocol(fs, replica_id)
        self.app = web.Application()
        self._setup_routes()
        self._runner: Optional[web.AppRunner] = None

    def _setup_routes(self) -> None:
        """Set up HTTP routes."""
        self.app.router.add_get("/ping", self._handle_ping)
        self.app.router.add_get("/merkle/root", self._handle_get_merkle_root)
        self.app.router.add_post("/merkle/diff", self._handle_get_merkle_diff)
        self.app.router.add_post("/operations", self._handle_get_operations)
        self.app.router.add_get("/chunk/{hash}", self._handle_get_chunk)

    async def start(self) -> None:
        """Start the network manager."""
        # Start HTTP server
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        
        # Start sync protocol
        await self.sync.start()

    async def stop(self) -> None:
        """Stop the network manager."""
        # Stop sync protocol
        await self.sync.stop()
        
        # Stop HTTP server
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    async def add_peer(self, peer_id: str, host: str, port: int) -> bool:
        """
        Add a peer to sync with.
        
        Args:
            peer_id: Unique identifier for the peer
            host: Peer's hostname
            port: Peer's port number
            
        Returns:
            True if peer was added successfully
        """
        return await self.sync.add_peer(peer_id, host, port)

    async def remove_peer(self, peer_id: str) -> None:
        """
        Remove a peer.
        
        Args:
            peer_id: ID of the peer to remove
        """
        await self.sync.remove_peer(peer_id)

    async def _handle_ping(self, request: web.Request) -> web.Response:
        """Handle ping requests."""
        return web.Response(text="pong")

    async def _handle_get_merkle_root(self, request: web.Request) -> web.Response:
        """Handle requests for our Merkle tree root hash."""
        root_hash = self.fs.tree.crdt.merkle_tree.get_root_hash()
        return web.json_response({"root_hash": root_hash})

    async def _handle_get_merkle_diff(self, request: web.Request) -> web.Response:
        """Handle requests for Merkle tree differences."""
        data = await request.json()
        local_hash = data.get("local_hash")
        if not local_hash:
            raise web.HTTPBadRequest(text="local_hash required")
            
        # Create a temporary tree with just the root hash for comparison
        temp_tree = self.fs.tree.crdt.merkle_tree.__class__()
        temp_tree.root.hash_value = local_hash
        
        # Get differences
        missing_ops = self.fs.tree.crdt.merkle_tree.diff(temp_tree)
        
        return web.json_response({
            "missing_ops": missing_ops
        })

    async def _handle_get_operations(self, request: web.Request) -> web.Response:
        """Handle requests for specific operations."""
        data = await request.json()
        hashes = data.get("hashes", [])
        if not hashes:
            raise web.HTTPBadRequest(text="hashes required")
            
        # Get requested operations
        ops = {}
        for op_hash in hashes:
            op = self.fs.tree.crdt.operations.get(op_hash)
            if op:
                ops[op_hash] = {
                    "timestamp": op.timestamp,
                    "replica_id": op.replica_id,
                    "operation_type": op.operation_type,
                    "path": op.path,
                    "payload": op.payload,
                    "dependencies": list(op.dependencies)
                }
                
        return web.json_response(ops)

    async def _handle_get_chunk(self, request: web.Request) -> web.Response:
        """Handle requests for file chunks."""
        chunk_hash = request.match_info["hash"]
        chunk = self.fs.data.get_chunk(chunk_hash)
        
        if chunk is None:
            raise web.HTTPNotFound(text=f"Chunk {chunk_hash} not found")
            
        return web.Response(body=chunk) 