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

from merkle_crdt.merkle_lww import MerkleLWWRegister

async def main() -> None:
    """Main entry point."""
    register = MerkleLWWRegister("merklelww.json", 0)
    await register.write("Test")
    await register.write("1")
    await register.write("2")
    await register.write("3")
    await register.fsync()


    register = MerkleLWWRegister("merklelww.json", 0)
    await register.fload()
    print(register.value)


if __name__ == "__main__":
    asyncio.run(main()) 
