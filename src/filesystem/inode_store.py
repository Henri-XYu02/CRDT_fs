


import base64
import os
from typing import Set
from merkle_crdt.merkle_lww import MerkleLWWRegister
class InodeStore:
    inodes: dict[int, MerkleLWWRegister]
    path: str
    dirty: Set[int]
    def __init__(self, path: str):
        pass

    async def read(self, inode: int, off: int, size: int) -> bytes:
        return bytes()

    async def write(self, inode: int, off: int, buf: bytes) -> int:
        return len(buf)

    async def open(self, inode: int):
        pass

    async def fsync(self):
        pass

    async def size(self, inode: int):
        pass

class LWWInodeStore(InodeStore):
    inodes: dict[int, MerkleLWWRegister]
    path: str
    dirty: Set[int]
    def __init__(self, path: str):
        # make dir at path
        self.path = path
        self.dirty = set()
        self.inodes = {}
        pass

    async def read(self, inode: int, off: int, size: int) -> bytes:
        # Horribly inefficient right now
        if inode not in self.inodes:
            await self.open(inode)
        contents = self.inodes[inode].read()
        contents_decoded = base64.b64decode(contents)
        return contents_decoded[off:size]

    async def write(self, inode: int, off: int, buf: bytes) -> int:
        if inode not in self.inodes:
            await self.open(inode)
        contents = self.inodes[inode].read()
        contents_decoded = base64.b64decode(contents)
        result = contents_decoded[:off] + buf + contents_decoded[off + len(buf):]
        contents = base64.b64encode(result).decode()
        self.dirty.add(inode)
        await self.inodes[inode].write(contents)
        self.dirty.add(inode)
        return len(buf)

    async def size(self, inode: int):
        if inode not in self.inodes:
            await self.open(inode)
        contents = self.inodes[inode].read()
        contents_decoded = base64.b64decode(contents)
        return len(contents_decoded)

    async def open(self, inode: int):
        # TODO: replica
        # TODO: locking around dirty add, fsync, this
        reg = MerkleLWWRegister(os.path.join(self.path, str(inode)), 0)
        await reg.fload()
        self.inodes[inode] = reg

    async def fsync(self):
        for inode in self.dirty:
            await self.inodes[inode].fsync()
