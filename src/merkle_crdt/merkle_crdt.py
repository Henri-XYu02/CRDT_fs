"""
Core implementation of the Merkle-CRDT that combines CRDT operations with a Merkle tree structure.
"""
import asyncio
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import hashlib
import time
from serde import serde
from serde.json import to_json, from_json
import trio


@serde
class MerkleNode:
    hash_value: str # str of hex; serialization needs this to avoid collisions since we're using 128+bit hashes
    replica: int
    height: int
    value: list[str] # 0th item: operation, 1+th item: args; don't want to fight generics
    children: Set[str]

@serde
class MerkleTree:
    root: str
    nodes: dict[str, MerkleNode]

class MerkleCRDT:
    """
    Requires subclassing to form any given crdt.
    """
    tree: MerkleTree
    applied_ops: Set[str]
    lock: asyncio.Lock
    fname: str
    replica: int # Randomly generated, i64 (or hash of hostname or smth)

    def __init__(self, path: str, replica: int):
        self.applied_ops = set()  # Set of applied operation hashes
        self.lock = asyncio.Lock()
        self.fname = path
        self.replica = replica
        self.tree = MerkleTree("", {})
        new_node = self.new_node([], set())
        self.tree.nodes[new_node.hash_value] = new_node
        self.tree.root = new_node.hash_value

    async def fsync(self):
        # Serializes the CRDT to disk; takes a lock so no operations happen concurrently
        async with self.lock:
            with open(self.fname, "w") as f:
                f.write(to_json(self.tree))
                f.flush()


    async def fload(self):
        # Loads the crdt from disk
        async with self.lock:
            try:
                with open(self.fname, "r") as f:
                    self.tree = from_json(MerkleTree, f.read())

                    l: list[MerkleNode] = []

                    self.topo(self.tree.nodes[self.tree.root], l)

                    l.sort(key=lambda x: (x.height, x.replica))

                    self.apply_operations([i.value for i in l])
            except FileNotFoundError:
                pass # Do nothing if IO error
    def get_node(self, hash: str) -> MerkleNode | None:
        return self.tree.nodes.get(hash, None)

    def put_node(self, node: MerkleNode):
        self.tree.nodes[node.hash_value] = node

    def new_node(self, value: list[str], children: set[str]) -> MerkleNode:
        hasher = hashlib.sha1()
        for item in value:
            hasher.update(item.encode('utf-8'))
        for item in sorted(children):
            hasher.update(item.encode('utf-8'))
        val = hasher.hexdigest()
        height = max([self.tree.nodes[child].height for child in children] or [0]) + 1
        new_node = MerkleNode(val, self.replica, height, value, children)
        return new_node

    def topo(self, node: MerkleNode, l: list[MerkleNode]):
        if node.hash_value in self.applied_ops:
            return
        for child in node.children:
            self.topo(self.tree.nodes[child], l)
        self.applied_ops.add(node.hash_value)
        l.append(node)
        # TODO: sort by height so things that rely on height locality are more efficient
        # TODO: also add batching support since that suits our use case very nicely


    async def add_root(self, root: str):
        # IMPORTANT PRECONDITION: ALL CHILDREN OF THE ROOT MUST BE ADDED
        async with self.lock:
            # If new root is a subtree of us
            if root in self.applied_ops:
                #print("DECIDED OLD ROOT", root)
                return
            # If we are a subtree of new root
            def rec(h):
                if h == self.tree.root:
                    return True
                if h in self.applied_ops:
                    return False
                if h not in self.tree.nodes:
                    #print("IMPOSSIBLE: ", h, self.tree.nodes, self.fname)
                    pass
                for c in self.tree.nodes[h].children:
                    if rec(c):
                        return True
                return False
            should_use_old_root = rec(root)
            # Otherwise, merge both
            if root not in self.tree.nodes:
                #print("IMPOSSIBLE: ", root, self.tree.nodes, self.fname)
                pass
            root_obj  = self.tree.nodes[root]

            l: list[MerkleNode] = []

            self.topo(root_obj, l)

            l.sort(key=lambda x: (x.height, x.replica))

            self.apply_operations([i.value for i in l])

            if should_use_old_root:
                self.tree.root = root
                #print("DECIDED OLD PEER ROOT", root)
                return

            new_node = self.new_node([], {root, self.tree.root})
            self.put_node(new_node)
            self.tree.root = new_node.hash_value
            #print("DECIDED NEW ROOT", new_node.hash_value)


    def apply_operation(self, op: list[str]):
        # Meant to be implemented in a subclass
        pass

    def apply_operations(self, ops: list[list[str]]):
        # Meant to be implemented in a subclass
        for op in ops:
            self.apply_operation(op)

