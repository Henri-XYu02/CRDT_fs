


import asyncio
import base64
import datetime
import os
from typing import Set

from sortedcontainers import SortedDict, SortedSet
import trio
from merkle_crdt.merkle_crdt import MerkleCRDT
from merkle_crdt.merkle_lww import MerkleLWWRegister
class InodeStore:
    inodes: dict[int, MerkleLWWRegister]
    path: str
    dirty: Set[int]
    timed_ops: SortedSet
    times: dict[int, int]
    lock: asyncio.Lock
    replica: int

    def __init__(self, path: str, replica: int):
        self.timed_ops = SortedSet()
        self.lock = asyncio.Lock()
        self.path = path
        self.replica = replica
        self.times = {}

    async def read(self, inode: int, off: int, size: int) -> bytes:
        return bytes()

    async def write(self, inode: int, off: int, buf: bytes) -> int:
        if inode in self.times:
            time = self.times[inode]
            self.timed_ops.remove((time, inode))
        now = datetime.datetime.now()
        now_secs = int(now.timestamp())
        self.timed_ops.add((now_secs, inode))
        self.times[inode] = now_secs
        return len(buf)

    async def open(self, inode: int) -> MerkleCRDT:
        raise Exception("UNIMPLEMENTED")

    async def fsync(self):
        pass

    async def size(self, inode: int):
        pass

    async def signal_write(self, inode: int):
        async with self.lock:
            if inode in self.times:
                time = self.times[inode]
                self.timed_ops.remove((time, inode))
            now = datetime.datetime.now()
            now_secs = int(now.timestamp())
            self.timed_ops.add((now_secs, inode))
            self.times[inode] = now_secs

    async def changes_since(self, time: int) -> tuple[list[int], int]:
        async with self.lock:
            now = datetime.datetime.now()
            now_secs = int(now.timestamp())
            return list(self.timed_ops.irange((time,0))), now_secs

class LWWInodeStore(InodeStore):
    inodes: dict[int, MerkleLWWRegister]
    path: str
    dirty: Set[int]
    def __init__(self, path: str, replica: int):
        super().__init__(path, replica)
        # make dir at path
        self.dirty = set()
        self.inodes = {}

    async def read(self, inode: int, off: int, size: int) -> bytes:
        # Horribly inefficient right now
        async with self.lock:
            if inode not in self.inodes:
                await self._open(inode)
            contents = self.inodes[inode].read()
            contents_decoded = base64.b64decode(contents)
            return contents_decoded[off:off+size]

    async def write(self, inode: int, off: int, buf: bytes) -> int:
        async with self.lock:
            await super().write(inode, off, buf)
            if inode not in self.inodes:
                await self._open(inode)
            contents = self.inodes[inode].read()
            contents_decoded = base64.b64decode(contents)
            result = contents_decoded[:off] + buf + contents_decoded[off + len(buf):]
            contents = base64.b64encode(result).decode()
            self.dirty.add(inode)
            await self.inodes[inode].write(contents)
            self.dirty.add(inode)
            return len(buf)

    async def size(self, inode: int):
        async with self.lock:
            if inode not in self.inodes:
                await self._open(inode)
            contents = self.inodes[inode].read()
            contents_decoded = base64.b64decode(contents)
            return len(contents_decoded)

    async def _open(self, inode: int):
        # TODO: replica
        if inode in self.inodes:
            return self.inodes[inode]
        reg = MerkleLWWRegister(os.path.join(self.path, str(inode)), self.replica)
        await reg.fload()
        self.inodes[inode] = reg
        return reg

    async def open(self, inode: int):
        async with self.lock:
            return await self._open(inode)

    async def fsync(self):
        for inode in self.dirty:
            await self.inodes[inode].fsync()
        self.dirty.clear()
