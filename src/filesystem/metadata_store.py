"""
Implementation of the metadata store using a CRDT map.
"""
from typing import Dict, Optional, Any
from dataclasses import dataclass
import time

from ..merkle_crdt import MerkleCRDT

@dataclass
class Metadata:
    """File or directory metadata."""
    mode: int = 0o644  # Default file permissions
    uid: int = 0
    gid: int = 0
    size: int = 0
    atime: float = 0.0  # Access time
    mtime: float = 0.0  # Modification time
    ctime: float = 0.0  # Creation time
    data_hash: str = ""  # Hash of file content in DataStore

class MetadataStore:
    """
    Store for file and directory metadata using a CRDT map.
    Each inode maps to a metadata object containing standard filesystem metadata.
    """
    def __init__(self, replica_id: str):
        self.crdt = MerkleCRDT(replica_id)
        self.metadata: Dict[int, Metadata] = {}

    def get(self, inode: int) -> Optional[Metadata]:
        """Get metadata for an inode."""
        return self.metadata.get(inode)

    def create(self, inode: int, is_directory: bool = False) -> Metadata:
        """
        Create metadata for a new inode.
        
        Args:
            inode: Inode number
            is_directory: Whether this is a directory
            
        Returns:
            The created metadata object
        """
        now = time.time()
        metadata = Metadata(
            mode=0o755 if is_directory else 0o644,
            uid=0,
            gid=0,
            size=0,
            atime=now,
            mtime=now,
            ctime=now
        )
        
        self.metadata[inode] = metadata
        
        # Create CRDT operation
        self.crdt.add_operation(
            op_type="create_metadata",
            path=str(inode),
            payload={
                "mode": metadata.mode,
                "uid": metadata.uid,
                "gid": metadata.gid,
                "size": metadata.size,
                "atime": metadata.atime,
                "mtime": metadata.mtime,
                "ctime": metadata.ctime
            }
        )
        
        return metadata

    def update(self, inode: int, **kwargs: Any) -> bool:
        """
        Update metadata fields for an inode.
        
        Args:
            inode: Inode number
            **kwargs: Metadata fields to update
            
        Returns:
            True if update was successful, False otherwise
        """
        metadata = self.metadata.get(inode)
        if not metadata:
            return False
            
        # Update fields
        for key, value in kwargs.items():
            if hasattr(metadata, key):
                setattr(metadata, key, value)
                
        # Create CRDT operation
        self.crdt.add_operation(
            op_type="update_metadata",
            path=str(inode),
            payload=kwargs
        )
        
        return True

    def delete(self, inode: int) -> bool:
        """
        Delete metadata for an inode.
        
        Args:
            inode: Inode number
            
        Returns:
            True if deletion was successful, False otherwise
        """
        if inode not in self.metadata:
            return False
            
        # Create CRDT operation
        self.crdt.add_operation(
            op_type="delete_metadata",
            path=str(inode),
            payload=None
        )
        
        del self.metadata[inode]
        return True

    def update_times(self, inode: int, access: bool = False, modify: bool = False) -> bool:
        """
        Update access and/or modification times for an inode.
        
        Args:
            inode: Inode number
            access: Whether to update access time
            modify: Whether to update modification time
            
        Returns:
            True if update was successful, False otherwise
        """
        metadata = self.metadata.get(inode)
        if not metadata:
            return False
            
        now = time.time()
        updates = {}
        
        if access:
            updates["atime"] = now
        if modify:
            updates["mtime"] = now
            
        return self.update(inode, **updates) if updates else True 