"""
FUSE interface implementation for the distributed filesystem.
"""
import os
from typing import Any, Dict
import errno
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from .fs_operations import FSOperations

class FuseInterface(LoggingMixIn, Operations):
    """
    FUSE interface that translates FUSE operations to our filesystem operations.
    """
    def __init__(self, replica_id: str):
        self.fs = FSOperations(replica_id)

    def getattr(self, path: str, fh: Any = None) -> Dict[str, Any]:
        """Get file attributes."""
        attrs = self.fs.getattr(path)
        if attrs is None:
            raise FuseOSError(errno.ENOENT)
        return attrs

    def readdir(self, path: str, fh: Any) -> list:
        """Read directory entries."""
        return self.fs.readdir(path)

    def mkdir(self, path: str, mode: int) -> None:
        """Create a directory."""
        if not self.fs.mkdir(path, mode):
            raise FuseOSError(errno.EACCES)

    def rmdir(self, path: str) -> None:
        """Remove a directory."""
        if not self.fs.rmdir(path):
            raise FuseOSError(errno.EACCES)

    def create(self, path: str, mode: int, fi: Any = None) -> None:
        """Create a file."""
        if not self.fs.create(path, mode):
            raise FuseOSError(errno.EACCES)

    def unlink(self, path: str) -> None:
        """Remove a file."""
        if not self.fs.unlink(path):
            raise FuseOSError(errno.EACCES)

    def read(self, path: str, size: int, offset: int, fh: Any) -> bytes:
        """Read from a file."""
        return self.fs.read(path, size, offset)

    def write(self, path: str, data: bytes, offset: int, fh: Any) -> int:
        """Write to a file."""
        return self.fs.write(path, data, offset)

    def rename(self, old: str, new: str) -> None:
        """Rename a file or directory."""
        if not self.fs.rename(old, new):
            raise FuseOSError(errno.EACCES)

    def truncate(self, path: str, length: int, fh: Any = None) -> None:
        """Truncate a file."""
        # For now, we'll implement this as a write of empty bytes
        if length == 0:
            self.fs.write(path, b"", 0)

    def flush(self, path: str, fh: Any) -> None:
        """Flush cached data."""
        # No-op for now as we don't implement caching
        pass

    def release(self, path: str, fh: Any) -> None:
        """Release an open file."""
        # No-op for now as we don't track open files
        pass

    def fsync(self, path: str, datasync: bool, fh: Any) -> None:
        """Synchronize file contents."""
        # No-op for now as we don't implement caching
        pass

def mount(mountpoint: str, replica_id: str, **kwargs: Any) -> None:
    """
    Mount the filesystem at the specified mountpoint.
    
    Args:
        mountpoint: Directory to mount the filesystem at
        replica_id: Unique identifier for this replica
        **kwargs: Additional arguments to pass to FUSE
    """
    FUSE(
        FuseInterface(replica_id),
        mountpoint,
        foreground=True,
        allow_other=True,
        **kwargs
    ) 