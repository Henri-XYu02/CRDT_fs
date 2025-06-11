import stat
import pyfuse3
from typing import Tuple, Optional
import errno
import os

from filesystem.inode_store import InodeStore
from merkle_crdt.merkle_ktree import MerkleKTree


class FuseOps(pyfuse3.Operations):
    fs_structure: MerkleKTree
    inode_store: InodeStore
    fhtable: dict[int, int]
    itable: dict[int, int]
    fhind: int

    def __init__(self, fs_structure: MerkleKTree, inode_store: InodeStore, *args):
        super().__init__(*args)
        self.fs_structure = fs_structure
        self.inode_store = inode_store
        self.fhind = 1
        self.fhtable = {}
        self.itable = {}

    def fh(self, inode: int):
        if inode not in self.fhtable:
            self.fhtable[inode] = self.fhind
            self.itable[self.fhind] = inode
            self.fhind += 1
        return self.fhtable[inode]

    def hf(self, handle: int):
        return self.itable[handle]

    def init(self, ) -> None:
        self.fh(1)
        pass

    async def lookup(self, parent_inode: int, name: bytes, ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        for child in self.fs_structure.child[self.hf(parent_inode)]:
            if child[0] == name.decode():
                return await self.getattr(self.fh(child[1]))
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def open(self, inode: int, flags: int, ctx: pyfuse3.RequestContext) -> pyfuse3.FileInfo:
        return pyfuse3.FileInfo(fh=inode)

    async def create(self, parent_inode: int, name: bytes, mode: int, flags: int, 
                     ctx: pyfuse3.RequestContext) -> Tuple[pyfuse3.FileInfo, pyfuse3.EntryAttributes]:
        """Create a file with permissions mode and open it with flags."""
        new_inode = await self.fs_structure.mkf(parent_inode, name.decode())
        new_inode = self.fh(new_inode)

        return (await self.open(new_inode, flags, ctx), await self.getattr(new_inode, ctx))

    async def read(self, fh: int, off: int, size: int) -> bytes:
        """Read size bytes from fh at position off."""
        return await self.inode_store.read(self.hf(fh), off, size)

    async def write(self, fh: int, off: int, buf: bytes) -> int:
        """Write buf into fh at off."""
        return await self.inode_store.write(self.hf(fh), off, buf)

    async def fsync(self, fh: int, datasync: bool) -> None:
        """Flush buffers for open file fh."""
        print("fsync")
        await self.fs_structure.fsync()
        await self.inode_store.fsync()

    async def opendir(self, inode: int, ctx: pyfuse3.RequestContext) -> int:
        """Open the directory with inode."""
        return inode

    async def readdir(self, fh: int, start_id: int, token: pyfuse3.ReaddirToken) -> None:
        """Read entries in open directory fh."""
        vals = list(sorted((self.fs_structure.child[self.hf(fh)])))
        # Don't call readdir_reply - just return
        for i in range(start_id, len(self.fs_structure.child[self.hf(fh)])):
            child = vals[i]
            if not pyfuse3.readdir_reply(token, child[0].encode(), await self.getattr(self.fh(child[1])), i + 1):
                return
        pass


    async def getattr(self, inode, ctx=None):
        attr = pyfuse3.EntryAttributes()
        attr.st_ino = inode
        if self.hf(inode) & (1 << 63) != 0:
             attr.st_mode = 0o777 | stat.S_IFREG
             attr.st_size = await self.inode_store.size(self.hf(inode))
        else:
             attr.st_mode = 0o777 | stat.S_IFDIR

        stamp = int(1438467123.985654 * 1e9)
        attr.st_atime_ns = stamp
        attr.st_ctime_ns = stamp
        attr.st_mtime_ns = stamp
        attr.st_gid = os.getgid()
        attr.st_uid = os.getuid()
        return attr

    async def mkdir(self, parent_inode: int, name: bytes, mode: int, 
                    ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
        """Create a directory."""
        new_inode = await self.fs_structure.mkdir(parent_inode, name.decode())
        new_inode = self.fh(new_inode)
        return await self.getattr(new_inode)

    async def rmdir(self, parent_inode: int, name: bytes, ctx: pyfuse3.RequestContext) -> None:
        """Remove directory name."""
        for child in self.fs_structure.child[self.hf(parent_inode)]:
            if child[0] == name.decode():
                await self.fs_structure.remove(child[1])

    async def unlink(self, parent_inode: int, name: bytes, ctx: pyfuse3.RequestContext) -> None:
        print(f"{parent_inode}, {name}")
        """Remove a (possibly special) file."""
        for child in self.fs_structure.child[self.hf(parent_inode)].copy():
            if child[0] == name.decode():
                await self.fs_structure.remove(child[1])

    async def rename(self, parent_inode_old: int, name_old: str, parent_inode_new: int, 
                     name_new: str, flags: int, ctx: pyfuse3.RequestContext) -> None:
        """Rename a directory entry."""
        for child in self.fs_structure.child[self.hf(parent_inode_old)]:
            if child[0] == name_old:
                await self.fs_structure.rename(child[1], self.hf(parent_inode_new), name_new)
