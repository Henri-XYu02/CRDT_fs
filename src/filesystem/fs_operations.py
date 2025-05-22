"""
Implementation of high-level filesystem operations.
This module ties together the tree structure, metadata store, and data store.
"""
from typing import Optional, List, Tuple
import os
import time

from .fs_tree import FSTree
from .metadata_store import MetadataStore
from .data_store import DataStore

class FSOperations:
    """
    High-level filesystem operations that coordinate between the different stores.
    This provides a clean interface for the FUSE layer to use.
    """
    def __init__(self, replica_id: str):
        self.tree = FSTree(replica_id)
        self.metadata = MetadataStore(replica_id)
        self.data = DataStore()

    def getattr(self, path: str) -> Optional[dict]:
        """
        Get attributes of a file or directory.
        
        Args:
            path: Path to the file or directory
            
        Returns:
            Dict of attributes if found, None otherwise
        """
        inode = self._path_to_inode(path)
        if inode is None:
            return None
            
        metadata = self.metadata.get(inode)
        if metadata is None:
            return None
            
        return {
            'st_mode': metadata.mode,
            'st_uid': metadata.uid,
            'st_gid': metadata.gid,
            'st_size': metadata.size,
            'st_atime': metadata.atime,
            'st_mtime': metadata.mtime,
            'st_ctime': metadata.ctime
        }

    def readdir(self, path: str) -> List[str]:
        """
        List contents of a directory.
        
        Args:
            path: Path to the directory
            
        Returns:
            List of entry names in the directory
        """
        inode = self._path_to_inode(path)
        if inode is None:
            return []
            
        entries = self.tree.list_directory(inode)
        return ['.', '..'] + [entry.name for entry in entries]

    def mkdir(self, path: str, mode: int) -> bool:
        """
        Create a directory.
        
        Args:
            path: Path where to create the directory
            mode: Directory permissions
            
        Returns:
            True if successful, False otherwise
        """
        parent_inode = self._path_to_inode(os.path.dirname(path))
        if parent_inode is None:
            return False
            
        name = os.path.basename(path)
        inode = self.tree.create_node(parent_inode, name, is_directory=True)
        if inode is None:
            return False
            
        metadata = self.metadata.create(inode, is_directory=True)
        metadata.mode = mode
        return True

    def create(self, path: str, mode: int) -> bool:
        """
        Create a file.
        
        Args:
            path: Path where to create the file
            mode: File permissions
            
        Returns:
            True if successful, False otherwise
        """
        parent_inode = self._path_to_inode(os.path.dirname(path))
        if parent_inode is None:
            return False
            
        name = os.path.basename(path)
        inode = self.tree.create_node(parent_inode, name, is_directory=False)
        if inode is None:
            return False
            
        metadata = self.metadata.create(inode, is_directory=False)
        metadata.mode = mode
        return True

    def write(self, path: str, data: bytes, offset: int) -> int:
        """
        Write data to a file.
        
        Args:
            path: Path to the file
            data: Data to write
            offset: Offset at which to write
            
        Returns:
            Number of bytes written
        """
        inode = self._path_to_inode(path)
        if inode is None:
            return 0
            
        metadata = self.metadata.get(inode)
        if metadata is None:
            return 0
            
        # For now, we'll implement a simple version that always writes the whole file
        data_hash = self.data.write(data)
        self.metadata.update(inode, size=len(data), data_hash=data_hash, mtime=time.time())
        return len(data)

    def read(self, path: str, size: int, offset: int) -> bytes:
        """
        Read data from a file.
        
        Args:
            path: Path to the file
            size: Number of bytes to read
            offset: Offset from which to read
            
        Returns:
            The read data
        """
        inode = self._path_to_inode(path)
        if inode is None:
            return b""
            
        metadata = self.metadata.get(inode)
        if metadata is None or not metadata.data_hash:
            return b""
            
        data = self.data.read(metadata.data_hash)
        if data is None:
            return b""
            
        # Update access time
        self.metadata.update_times(inode, access=True)
        
        # Return requested portion of data
        return data[offset:offset + size]

    def unlink(self, path: str) -> bool:
        """
        Delete a file.
        
        Args:
            path: Path to the file
            
        Returns:
            True if successful, False otherwise
        """
        inode = self._path_to_inode(path)
        if inode is None:
            return False
            
        metadata = self.metadata.get(inode)
        if metadata is None:
            return False
            
        # Delete data if it exists
        if metadata.data_hash:
            self.data.delete(metadata.data_hash)
            
        # Delete metadata and tree node
        self.metadata.delete(inode)
        return self.tree.delete_node(inode)

    def rmdir(self, path: str) -> bool:
        """
        Delete a directory.
        
        Args:
            path: Path to the directory
            
        Returns:
            True if successful, False otherwise
        """
        inode = self._path_to_inode(path)
        if inode is None:
            return False
            
        # Directory must be empty
        if self.tree.list_directory(inode):
            return False
            
        # Delete metadata and tree node
        self.metadata.delete(inode)
        return self.tree.delete_node(inode)

    def rename(self, old_path: str, new_path: str) -> bool:
        """
        Rename/move a file or directory.
        
        Args:
            old_path: Current path
            new_path: New path
            
        Returns:
            True if successful, False otherwise
        """
        inode = self._path_to_inode(old_path)
        if inode is None:
            return False
            
        new_parent_inode = self._path_to_inode(os.path.dirname(new_path))
        if new_parent_inode is None:
            return False
            
        return self.tree.move_node(inode, new_parent_inode, os.path.basename(new_path))

    def _path_to_inode(self, path: str) -> Optional[int]:
        """
        Convert a path to an inode number.
        
        Args:
            path: Path to convert
            
        Returns:
            Inode number if found, None otherwise
        """
        if not path or path == '/':
            return 0
            
        parts = [p for p in path.split('/') if p]
        current = 0  # Start at root
        
        for part in parts:
            current = self.tree.lookup(current, part)
            if current is None:
                return None
                
        return current 