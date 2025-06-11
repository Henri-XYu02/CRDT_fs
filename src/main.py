"""
Main entry point for the distributed filesystem.
"""
import os
import sys
import asyncio
import argparse
import logging
import uuid
from typing import List
import trio

import pyfuse3

from filesystem.fuse_binding import FuseOps
from filesystem.inode_store import LWWInodeStore
from merkle_crdt.merkle_ktree import MerkleKTree
from merkle_crdt.merkle_lww import MerkleLWWRegister

async def main() -> None:
    """Main entry point."""
    register = MerkleLWWRegister("merklelww.json", 0)
    await register.fload()
    print(register.value)

    inode_store = LWWInodeStore("inodes")
    fs_structure = MerkleKTree("ftree.json", 0)

    await fs_structure.fload()

    pyfuse3.init(FuseOps(fs_structure, inode_store), "fs")
    await pyfuse3.main()


if __name__ == "__main__":
    trio.run(main)
