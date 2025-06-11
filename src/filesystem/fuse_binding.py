import pyfuse3
from typing import Tuple, Optional
import errno
import os


class FuseOps(pyfuse3.Operations):
    def init(self) -> None:
        """Initialize operations."""
        pass

    async def lookup(self, parent_inode: int, name: bytes, ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        """Look up a directory entry by name and get its attributes."""
        # Return EntryAttributes with zero st_ino to indicate not found
        attr = pyfuse3.EntryAttributes()
        attr.st_ino = 0
        return attr

    async def open(self, inode: int, flags: int, ctx: pyfuse3.RequestContext) -> pyfuse3.FileInfo:
        """Open a inode with flags."""
        fi = pyfuse3.FileInfo()
        fi.fh = 0  # Dummy file handle
        return fi

    async def create(self, parent_inode: int, name: bytes, mode: int, flags: int, 
                     ctx: pyfuse3.RequestContext) -> Tuple[pyfuse3.FileInfo, pyfuse3.EntryAttributes]:
        """Create a file with permissions mode and open it with flags."""
        fi = pyfuse3.FileInfo()
        fi.fh = 0  # Dummy file handle
        
        attr = pyfuse3.EntryAttributes()
        attr.st_ino = 0  # Dummy inode
        attr.st_mode = mode
        
        return (fi, attr)

    async def read(self, fh: int, off: int, size: int) -> bytes:
        """Read size bytes from fh at position off."""
        return b''  # Return empty bytes

    async def write(self, fh: int, off: int, buf: bytes) -> int:
        """Write buf into fh at off."""
        return len(buf)  # Pretend we wrote all bytes

    async def flush(self, fh: int) -> None:
        """Handle close() syscall."""
        pass

    async def fsync(self, fh: int, datasync: bool) -> None:
        """Flush buffers for open file fh."""
        pass

    async def release(self, fh: int) -> None:
        """Release open file."""
        pass

    async def opendir(self, inode: int, ctx: pyfuse3.RequestContext) -> int:
        """Open the directory with inode."""
        return 0  # Dummy directory handle

    async def readdir(self, fh: int, start_id: int, token: pyfuse3.ReaddirToken) -> None:
        """Read entries in open directory fh."""
        # Don't call readdir_reply - just return
        pass

    async def releasedir(self, fh: int) -> None:
        """Release open directory."""
        pass

    async def mkdir(self, parent_inode: int, name: bytes, mode: int, 
                    ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        """Create a directory."""
        attr = pyfuse3.EntryAttributes()
        attr.st_ino = 0  # Dummy inode
        attr.st_mode = mode | os.stat.S_IFDIR
        return attr

    async def rmdir(self, parent_inode: int, name: bytes, ctx: pyfuse3.RequestContext) -> None:
        """Remove directory name."""
        pass

    async def unlink(self, parent_inode: int, name: bytes, ctx: pyfuse3.RequestContext) -> None:
        """Remove a (possibly special) file."""
        pass

    async def rename(self, parent_inode_old: int, name_old: str, parent_inode_new: int, 
                     name_new: str, flags: int, ctx: pyfuse3.RequestContext) -> None:
        """Rename a directory entry."""
        pass
