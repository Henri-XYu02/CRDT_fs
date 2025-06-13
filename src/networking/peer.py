from typing import Optional, Dict, Any
import asyncio
import json
import msgpack
import httpx
from serde.json import to_json
import trio

from filesystem.inode_store import InodeStore
from merkle_crdt.merkle_crdt import MerkleCRDT
from merkle_crdt.merkle_ktree import MerkleKTree

class Peer:
    lock: asyncio.Lock
    host: str
    port: int
    last_time: int
    inode_store: InodeStore
    ktree: MerkleKTree
    replica: int

    def __init__(self, host: str, port: int, inode_store: InodeStore, ktree: MerkleKTree, replica: int):
        self.host = host
        self.port = port
        self.lock = asyncio.Lock()
        self.last_time = 0
        self.inode_store = inode_store
        self.ktree = ktree
        self.replica = replica

    async def healthcheck(self):
        async with httpx.AsyncClient() as client:
            await client.get(f'http://{self.host}:{self.port}/healthcheck')



    async def push_all(self):
        await self.healthcheck()
        async with self.lock:
            changes = {}
            changes["root"] = self.ktree
            # Open all things in path
            new_changes = self.inode_store.inodes.copy()
            for k, v in new_changes.items():
                changes[str(k)] = v
            await self.push_changelist(changes)

    async def push_changed(self):
        await self.healthcheck()
        async with self.lock:
            # Get changed - assume fstree has changes
            changes = {}
            changes["root"] = self.ktree
            new_changes, ts = await self.inode_store.changes_since(self.last_time)
            self.last_time = ts
            for change in new_changes:
                changes[str(change)] = await self.inode_store.open(change)
            await self.push_changelist(changes)

    async def push_changelist(self, changelist: dict[str, MerkleCRDT]):
        # Add nodes until all data transferred
        root = {}
        new_nodes = {}
        nodes_to_add = {}
        for k, v in changelist.items():
            v.cut_root()
            root[k] = v.tree.root
            new_nodes[k] = [to_json(v.tree.nodes[v.tree.root])]
            nodes_to_add[k] = set()
        depth = 1
        while len(new_nodes) != 0:
            async with httpx.AsyncClient() as client:
                response = await client.post(f'http://{self.host}:{self.port}/bulk_get_nodes_to_add', json=new_nodes)
                newer_nodes = response.json()
                new_nodes = {}
                for k, v in newer_nodes.items():
                    if len(v) != 0:
                        new_nodes[k] = []
                        for node in v:
                            add = to_json(changelist[k].tree.nodes[node])
                            if add not in nodes_to_add[k]:
                                new_nodes[k].append(add)
                                nodes_to_add[k].add(add)
                                # Recursively add children up to depth
                                new = {node}
                                for i in range(depth - 1):
                                    to_check_new = set()
                                    for to_check in new:
                                        for child in changelist[k].tree.nodes[to_check].children:
                                            if child not in nodes_to_add[k]:
                                                new_nodes[k].append(to_json(changelist[k].tree.nodes[child]))
                                                to_check_new.add(child)
                                    new = to_check_new

                        if len(new_nodes[k]) == 0:
                            new_nodes.pop(k)
            # TODO: cut this out to benchmark depth scaling
            depth *= 2
        for k in nodes_to_add.keys():
            nodes_to_add[k] = list(nodes_to_add[k])
        #print("deciding add ", nodes_to_add)
        #prinv("adding root ", root)
        print("Pushing changelist")
        async with httpx.AsyncClient() as client:
            response = await client.post(f'http://{self.host}:{self.port}/bulk_add', json=nodes_to_add)
            response = await client.post(f'http://{self.host}:{self.port}/bulk_root', json=root)


