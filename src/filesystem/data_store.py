"""
Implementation of the data store for file contents.
This uses content-addressed storage with Merkle trees for integrity verification.
"""
from typing import Dict, Optional
import hashlib

class DataStore:
    """
    Store for file contents using content-addressed storage.
    Each file's content is stored as chunks, addressed by their cryptographic hashes.
    """
    def __init__(self):
        self.chunks: Dict[str, bytes] = {}  # Hash -> Content mapping
        self.chunk_size = 1024 * 1024  # 1MB chunks

    def write(self, data: bytes) -> str:
        """
        Write data to the store.
        
        Args:
            data: The data to write
            
        Returns:
            The root hash of the data's Merkle tree
        """
        # Split data into chunks
        chunks = []
        for i in range(0, len(data), self.chunk_size):
            chunk = data[i:i + self.chunk_size]
            chunk_hash = hashlib.sha256(chunk).hexdigest()
            self.chunks[chunk_hash] = chunk
            chunks.append(chunk_hash)
            
        # Build Merkle tree of chunks
        while len(chunks) > 1:
            new_level = []
            for i in range(0, len(chunks), 2):
                left = chunks[i]
                right = chunks[i + 1] if i + 1 < len(chunks) else left
                combined = left + right
                parent_hash = hashlib.sha256(combined.encode()).hexdigest()
                new_level.append(parent_hash)
            chunks = new_level
            
        return chunks[0] if chunks else hashlib.sha256(b"").hexdigest()

    def read(self, root_hash: str) -> Optional[bytes]:
        """
        Read data from the store.
        
        Args:
            root_hash: The root hash of the data's Merkle tree
            
        Returns:
            The data if found, None otherwise
        """
        def verify_chunk(chunk_hash: str) -> Optional[bytes]:
            chunk = self.chunks.get(chunk_hash)
            if chunk is None:
                return None
            computed_hash = hashlib.sha256(chunk).hexdigest()
            return chunk if computed_hash == chunk_hash else None

        # For now, we'll implement a simple version that assumes
        # the root_hash is a direct chunk hash
        return verify_chunk(root_hash)

    def delete(self, root_hash: str) -> bool:
        """
        Delete data from the store.
        
        Args:
            root_hash: The root hash of the data to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        if root_hash in self.chunks:
            del self.chunks[root_hash]
            return True
        return False

    def has_chunk(self, chunk_hash: str) -> bool:
        """Check if a chunk exists in the store."""
        return chunk_hash in self.chunks

    def get_chunk(self, chunk_hash: str) -> Optional[bytes]:
        """Get a specific chunk by its hash."""
        return self.chunks.get(chunk_hash) 