from typing import Set
import fastapi
from serde.json import from_json, to_json
import trio
import trio_asyncio
import uvicorn

from filesystem.inode_store import InodeStore
from merkle_crdt.merkle_crdt import MerkleCRDT, MerkleNode
from merkle_crdt.merkle_ktree import MerkleKTree

app = fastapi.FastAPI()

# Two ways to sync: Pull and push nodes

from fastapi import FastAPI, APIRouter

FS_TREE = "root"

class APIHandler:
    inode_store: InodeStore
    ktree: MerkleKTree
    replica: int

    def __init__(self, inode_store: InodeStore, ktree: MerkleKTree, replica: int):
        self.router = APIRouter()
        self.inode_store = inode_store
        self.ktree = ktree
        self.replica = replica
        self.router.add_api_route("/bulk_get_nodes_to_add", self.bulk_get_nodes_to_add, methods=["POST"])
        self.router.add_api_route("/bulk_add", self.bulk_add, methods=["POST"])
        self.router.add_api_route("/bulk_root", self.bulk_inform_root, methods=["POST"])
        self.router.add_api_route("/healthcheck", self.healthcheck, methods=["GET"])

    async def bulk_add(self, pairs: dict[str, list[str]]):
        for (k, v) in pairs.items():
            await self.add_nodes(k, v)

    async def bulk_get_nodes_to_add(self, pairs: dict[str, list[str]]) -> dict[str, list[str]]:
        r = {}
        for (k, v) in pairs.items():
            r[k] = await self.get_nodes_to_add(k, v)
        return r

    async def bulk_inform_root(self, pairs: dict[str, str]):
        for (k, v) in pairs.items():
            await self.inform_of_root(k, v)

    async def get_crdt(self, name: str) -> MerkleCRDT:
        if name == FS_TREE:
            return self.ktree
        else:
            return await self.inode_store.open(int(name))

    async def signal_write_if_needed(self, name: str, hash_val: str):
        if name == FS_TREE:
            return
        else:
            crdt = await self.get_crdt(name)
            if hash_val not in crdt.applied_ops:
                await self.inode_store.signal_write(int(name))

    def recursively_check_missing(self, crdt: MerkleCRDT, node: str, missing: Set[str], visited: Set[str]):
        #print("RECURSIVELY CHECKED", node)
        if node not in crdt.tree.nodes:
            missing.add(node)
            return
        node_value = crdt.tree.nodes[node]
        for child in node_value.children:
            if child not in visited:
                visited.add(child)
                self.recursively_check_missing(crdt, child, missing, visited)

    # Necessary for push replication
    async def get_nodes_to_add(self, tree: str, nodes: list[str]) -> list[str]:
        # Get specified merkle crdt
        crdt = await self.get_crdt(tree)
        # Unmarshal nodes
        new_nodes: list[MerkleNode] = []
        for node in nodes:
            new_nodes.append(from_json(MerkleNode, node))

        missing: Set[str] = set()
        visited: Set[str] = set()
        # Identify missing children
        for node in new_nodes:
            self.recursively_check_missing(crdt, node.hash_value, missing, visited)
            for child in node.children:
                if child not in crdt.tree.nodes:
                    missing.add(child)

        #print("getting for ", tree, " SEP ", missing, " SEP ", nodes)

        # Return missing children
        return list(missing)

    # Necessary for push replication
    async def add_nodes(self, tree: str, nodes: list[str]):
        # Get specified merkle crdt
        crdt = await self.get_crdt(tree)
        # Unmarshal nodes
        new_nodes: list[MerkleNode] = []
        for node in nodes:
            new_nodes.append(from_json(MerkleNode, node))


        # Put nodes in tree
        for node in new_nodes:
            #print("adding for ", tree, " SEP ", node)
            crdt.put_node(node)
            #print(crdt.tree.nodes, crdt.fname)
            #print("ADDED for ", tree, " SEP ", node)

    async def inform_of_root(self, tree: str, root: str):
        # Get crdt
        crdt = await self.get_crdt(tree)
        # Call the add root method
        await self.signal_write_if_needed(tree, root)
        #print("adding root ", tree, " SEP ", root)
        #print(crdt.tree.nodes, crdt.fname)
        await crdt.add_root(root)
        await crdt.fsync()

    # Necessary for pull replication - TODO: implement
    async def get_nodes(self, tree: str, hashes: list[str]) -> list[str]:
        # Get tree
        crdt = await self.get_crdt(tree)

        result = []
        for hash_val in set(hashes):
            result.append(to_json(crdt.tree.nodes[hash_val]))
        return result

    async def get_root(self, tree: str) -> str:
        crdt = await self.get_crdt(tree)
        # Returns the current root
        return to_json(crdt.tree.nodes[crdt.tree.root])

    async def changes_since(self, time: int) -> list[str]:
        # Get list of changes since some time
        return []

    async def healthcheck(self) -> str:
        return str(self.replica)
