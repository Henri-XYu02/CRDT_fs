"""
Implementation of a peer in the distributed filesystem network.
"""
from typing import Optional, Dict, Any
import asyncio
import aiohttp
import json
import msgpack

class Peer:
    """
    Represents a peer in the distributed filesystem network.
    Handles communication with a single remote peer.
    """
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.session: Optional[aiohttp.ClientSession] = None
        self.connected = False

    async def connect(self) -> bool:
        """
        Establish connection with the peer.
        
        Returns:
            True if connection was successful, False otherwise
        """
        if self.connected:
            return True
            
        try:
            self.session = aiohttp.ClientSession()
            async with self.session.get(f"http://{self.host}:{self.port}/ping") as resp:
                if resp.status == 200:
                    self.connected = True
                    return True
        except Exception:
            self.connected = False
            if self.session:
                await self.session.close()
                self.session = None
        return False

    async def disconnect(self) -> None:
        """Close connection with the peer."""
        self.connected = False
        if self.session:
            await self.session.close()
            self.session = None

    async def get_merkle_root(self) -> Optional[str]:
        """
        Get the Merkle tree root hash from the peer.
        
        Returns:
            Root hash if successful, None otherwise
        """
        if not self.connected or not self.session:
            return None
            
        try:
            async with self.session.get(f"http://{self.host}:{self.port}/merkle/root") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("root_hash")
        except Exception:
            pass
        return None

    async def get_merkle_diff(self, local_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get the difference between our Merkle tree and the peer's.
        
        Args:
            local_hash: Our local root hash
            
        Returns:
            Dict of differences if successful, None otherwise
        """
        if not self.connected or not self.session:
            return None
            
        try:
            async with self.session.post(
                f"http://{self.host}:{self.port}/merkle/diff",
                json={"local_hash": local_hash}
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
        return None

    async def get_operations(self, hashes: list) -> Optional[Dict[str, Any]]:
        """
        Get specific operations from the peer.
        
        Args:
            hashes: List of operation hashes to fetch
            
        Returns:
            Dict of operations if successful, None otherwise
        """
        if not self.connected or not self.session:
            return None
            
        try:
            async with self.session.post(
                f"http://{self.host}:{self.port}/operations",
                json={"hashes": hashes}
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
        return None

    async def get_chunk(self, chunk_hash: str) -> Optional[bytes]:
        """
        Get a specific data chunk from the peer.
        
        Args:
            chunk_hash: Hash of the chunk to fetch
            
        Returns:
            Chunk data if successful, None otherwise
        """
        if not self.connected or not self.session:
            return None
            
        try:
            async with self.session.get(
                f"http://{self.host}:{self.port}/chunk/{chunk_hash}"
            ) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception:
            pass
        return None

    def __str__(self) -> str:
        return f"Peer({self.host}:{self.port})"

    def __repr__(self) -> str:
        return self.__str__() 