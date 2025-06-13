"""
Main entry point for the distributed filesystem.
"""
import os
from random import randint
import sys
import asyncio
import argparse
import logging
import traceback
import uuid
from typing import List
import fastapi
#from serde import serde
import serde
from serde.json import from_json, to_json
import trio
import trio_asyncio
import uvicorn
# import pyfuse3
# import pyfuse3.asyncio

from filesystem.fuse_binding import FuseOps
from filesystem.inode_store import InodeStore, LWWInodeStore
from merkle_crdt.merkle_ktree import MerkleKTree
from merkle_crdt.merkle_lww import MerkleLWWRegister
from networking.api_server import APIHandler
from networking.peer import Peer

@serde
class Config:
    replica: int
    peers: list[str]
    basepath: str
    mountpoint: str
    host: str
    port: int

def fsync_loop(inode_store: InodeStore, fs_structure: MerkleKTree, done: list[bool], finished: list[int]):
    async def inner():
        while True:

            await inode_store.fsync()
            await fs_structure.fsync()
            if done[0]:
                finished[0] += 1
                return
            await asyncio.sleep(10)
    return inner

def peer_loop(peer: Peer, done: list[bool], finished: list[int]):
    async def inner():
        while True:
            try:
                await peer.push_all()
            except Exception as e:
                print(f"Peer {peer.host} failed before connecting: {e}")
                traceback.print_exception(e)
            await asyncio.sleep(60)
        while True:
            try:
                await peer.push_changed()
            except Exception as e:
                print(f"Peer {peer.host} failed while connected")
                traceback.print_exception(e)
            if done[0]:
                # finished[0] += 1
                return
            await asyncio.sleep(60)
    return inner

async def main() -> None:
    if len(sys.argv) != 2:
        print("usage: python3 main.py configname")
    sys.setrecursionlimit(10**6)
    try:
        with open(sys.argv[1], "r") as f:
            config = from_json(Config, f.read())
    except:
        config = Config(0, [], input("Enter a base path: "), input("Enter mount path: "), "0.0.0.0", int(input("Enter a port: ")))
        os.mkdir(config.basepath)
        os.mkdir(config.mountpoint)
    if config.replica == 0:
        config.replica = randint(1, 2**31 - 1)
    with open(sys.argv[1], "w") as f:
        f.write(to_json(config))

    replica = config.replica
    try:
        os.mkdir(os.path.join(config.basepath, "inodes"))
    except:
        pass

    inode_store = LWWInodeStore(os.path.join(config.basepath, "inodes"), replica)
    fs_structure = MerkleKTree(os.path.join(config.basepath, "ftree.json"), replica)

    await fs_structure.fload()
    pyfuse3.asyncio.enable()
    pyfuse3.init(FuseOps(fs_structure, inode_store), config.mountpoint)
    app = fastapi.FastAPI()
    hello = APIHandler(inode_store, fs_structure, replica)
    app.include_router(hello.router)
    uconfig = uvicorn.Config(app=app)
    uconfig.host = config.host
    uconfig.port = config.port
    server = uvicorn.Server(config=uconfig)


    done = [False]
    finished = [0]
    pcount = 0

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        print("Finished fastapi. Please wait for about one minute for everything else to stop")
        done[0] = True
        pyfuse3.close()
        while finished[0] != 1: # Don't wait for peers
            await asyncio.sleep(1)
        # sys.exit(0)
        os._exit(0)

    async def pyfuse_main():
        await pyfuse3.main()
        print("Finished fuse. Please wait for everything else to stop")
        done[0] = True
        server.should_exit = True

    async def fastapi_main():
        await server.serve()
        print("Finished fastapi. Please wait for everything else to stop")
        done[0] = True
        pyfuse3.close()



    async with asyncio.TaskGroup() as tg:
        pcount += 1
        tg.create_task(pyfuse_main())
        tg.create_task(fastapi_main())
        tg.create_task(fsync_loop(inode_store, fs_structure, done, finished)())
        for peer in config.peers:
            pcount += 1
            r = peer.split(":")
            host = r[0]
            port = int(r[1])
            peer = Peer(host, port, inode_store, fs_structure, replica)
            tg.create_task(peer_loop(peer, done, finished)())


    done[0] = True
    pyfuse3.close()

if __name__ == "__main__":
    asyncio.run(main())
